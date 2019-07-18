package com.esgyn.kafkaCDC.server.kafkaConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.log4j.Logger;

import com.esgyn.kafkaCDC.server.esgynDB.EsgynDB;
import com.esgyn.kafkaCDC.server.esgynDB.TableInfo;
import com.esgyn.kafkaCDC.server.esgynDB.TableState;
import com.esgyn.kafkaCDC.server.esgynDB.MessageTypePara;
import com.esgyn.kafkaCDC.server.kafkaConsumer.messageType.RowMessage;

import com.esgyn.kafkaCDC.server.utils.Utils;
import com.esgyn.kafkaCDC.server.utils.KafkaParams;

import java.io.UnsupportedEncodingException;
import kafka.consumer.ConsumerTimeoutException;

@SuppressWarnings("deprecation")

/*
 * This represents a typical Kafka consumer that uses EsgynDB for data storage. It is a
 * single-threaded server that can be replicated to scale out - each copy handling a partition of a
 * topic.
 * 
 * Execution of each server in the group handled by... TBD a. pdsh script per node b. zookeeper?
 */
public class ConsumerThread<T> extends Thread {
    EsgynDB                     esgyndb;
    KafkaParams                 kafkaParams;
    // execution settings
    private boolean             skip;
    private boolean             bigEndian;
    private String              delimiter;
    private String              format;
    private String              encoding;
    private int                 partitionID;
    private String              messageClass;
    private String              outPutPath;
    private boolean             aconn   = false;
    private boolean             batchUpdate = false;

    long                        cacheNum;
    long                        kafkaPollNum;
    long                        latestTime;

    Map<String, TableState>     tables  = null;
    KafkaConsumer<?, ?>         kafkaconsumer;
    private final AtomicBoolean running = new AtomicBoolean(true);
    Connection                  dbConn  = null;
    String                      keepQuery= "values(1);";
    RowMessage<T>               urm     = null;
    // e.g.:com.esgyn.kafkaCDC.server.kafkaConsumer.messageType.UnicomRowMessage

    private static Logger       log     = Logger.getLogger(ConsumerThread.class);

    public ConsumerThread(EsgynDB esgyndb_, KafkaParams kafkaParams_,
			  boolean skip_, boolean bigEndian_,
			  String delimiter_, String format_, 
			  String encoding_, int partitionID_,
			  String messageClass_,String outPutPath_,
			  boolean aconn_,boolean batchUpdate_) {
        if (log.isTraceEnabled()) {
            log.trace("enter function");
        }
        esgyndb     = esgyndb_;
	kafkaParams = kafkaParams_;

        skip = skip_;
        bigEndian = bigEndian_;

        delimiter = delimiter_;
        format = format_;

        encoding = encoding_;
        partitionID = partitionID_;

        messageClass = messageClass_;
        outPutPath = outPutPath_;

        aconn = aconn_;
        batchUpdate = batchUpdate_;

        cacheNum = 0;
        kafkaPollNum = 0;

        tables = new HashMap<String, TableState>(0);
        Properties props = new Properties();

        if (kafkaParams.getZookeeper() != null) {
            props.put("zookeeper.connect", kafkaParams.getZookeeper());
        } else {
            props.put("bootstrap.servers", kafkaParams.getBroker());
        }
        props.put("group.id", kafkaParams.getGroup());
        props.put("enable.auto.commit", "false");
        props.put("max.partition.fetch.bytes", 10485760);
        props.put("heartbeat.interval.ms", kafkaParams.getHbTO());
        props.put("session.timeout.ms", kafkaParams.getSeTO());
        props.put("request.timeout.ms", kafkaParams.getReqTO());
        props.put("key.deserializer", kafkaParams.getKey());
        props.put("value.deserializer", kafkaParams.getValue());
        props.put("max.poll.records", (int) kafkaParams.getCommitCount());
        
        if (kafkaParams.getKafkaUser() !="") {
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put("sasl.jaas.config", 
		  "org.apache.kafka.common.security.plain.PlainLoginModule required username=" 
		  + kafkaParams.getKafkaUser() + " password=" + kafkaParams.getKafkaPW() +";");
        }
 
        kafkaconsumer = new KafkaConsumer(props);
        Utils utils = new Utils();

        TopicPartition partition = new TopicPartition(kafkaParams.getTopic(), partitionID);
        kafkaconsumer.assign(Arrays.asList(partition));

	String full = kafkaParams.getFull();
        switch (full) {
            case "START":
                kafkaconsumer.seekToBeginning(Arrays.asList(partition));
                break;
            case "END":
                kafkaconsumer.seekToEnd(Arrays.asList(partition));
                break;
            case "":
                break;
            default:
                if (utils.isDateStr(full)) {
                    seekToTime(kafkaconsumer, partition, utils);
                }else {
                    kafkaconsumer.seek(partition, Long.parseLong(full));
                }
                break;
        }
        // building RowMessage
        try {
            urm = (RowMessage<T>) Class.forName(messageClass).newInstance();
        } catch (InstantiationException ine) {
            log.error("when forName messageClass,there is a error: [" + ine.getMessage() + "]",ine);
        } catch (IllegalAccessException iae) {
            log.error("when forName messageClass,there is a error: [" + iae.getMessage() + "]",iae);
        } catch (ClassNotFoundException cnfe) {
            log.error("when forName messageClass,there is a error: [" + cnfe.getMessage()
                    + "]make sure the full-qualified name is right",cnfe);
        }

        if (log.isTraceEnabled()) {
            log.trace("exit function");
        }
    }

    public void run() {
        if (log.isTraceEnabled()) {
            log.trace("enter function");
        }

        try {
            if (aconn) {
                dbConn=esgyndb.getSharedConn();
            }else {
                dbConn = esgyndb.CreateConnection(false);
            }
            log.info("consumer server started.");
            while (running.get()) {
                if (!ProcessRecord())
                    break;
            } // while true

            log.info("consumer server stoped.");
        } catch (ConsumerTimeoutException cte) {
            log.error("consumer time out: " + cte.getMessage());
        } catch (WakeupException we) {
            log.warn("wakeup exception");
            // Ignore exception if closing
            if (running.get()) {
                log.error("wakeup: " + we.getMessage());
            }
        } finally {
            log.info("commit the cached record");
            commit_tables();
            if (!aconn) {
                log.info("close connection");
                esgyndb.CloseConnection(dbConn);
            }
            kafkaconsumer.close();
            running.set(false);
        }
        if (log.isTraceEnabled()) {
            log.trace("exit function");
        }
    }
    public boolean commit_tables() {
        if (aconn) {
            synchronized (ConsumerThread.class) {
                boolean commit_table_success=false;
                try {
                  commit_table_success= commit_tables_();
                } catch (SQLException e) {
                    // Connection does not exist(-29002) || Timeout expired(-29154)||
                    //Statement must be recompiled to allow privileges to be re-evaluated(-8734)||ERROR[8738] Statement must be recompiled due to redefinition of the object(s) accessed.
                    if ((e.getErrorCode()==-29002)||(e.getErrorCode()==-29154)||(e.getErrorCode()==-8734)||(e.getErrorCode()==-8738)) {
                        synchronized (esgyndb) {
                            //just create 1 dbConn
                            if (esgyndb.getSharedConn() == dbConn) {
                                log.info("single dbconnection is disconnect or Statement must be recompiled , retry commit table !");
                                if (log.isDebugEnabled()) {
                                    log.debug("current Thread dbconn ["+esgyndb.getSharedConn()+"] equal"
                                            + " old dbconn current dbconn["+dbConn+"], create a new dbconn");
                                }
                                try {
                                    esgyndb.CloseConnection(dbConn);
                                } catch (Exception e1) {
                                }
                                dbConn = esgyndb.CreateConnection(false);
                            } else {
                                if (log.isDebugEnabled()) {
                                    log.debug("current Thread dbconnection ["+dbConn+"] not equal new dbconn ["+
                                             esgyndb.getSharedConn()+"],set new dbconn to current dbconn");
                                }
                                dbConn = esgyndb.getSharedConn();
                            }
                            //reInitStmt
                            for (TableState tableState : tables.values()) {
                                tableState.InitStmt(dbConn, skip);
                            }
                        }
                        commit_table_success = commit_tables();
                        log.info("single database connections is disconnect or Statement must be recompiled, retry commit table success:"+commit_table_success);
                    }
                }
                return commit_table_success;
            }
        }else {
            boolean commit_table_success=false;
            try {
                commit_table_success=commit_tables_();
            } catch (SQLException e) {
                // Connection does not exist(-29002) || Timeout expired(-29154)||
                //Statement must be recompiled to allow privileges to be re-evaluated(-8734)||ERROR[8738] Statement must be recompiled due to redefinition of the object(s) accessed.
                if (e.getErrorCode()==-29002||e.getErrorCode()==-29154||(e.getErrorCode()==-8734)||(e.getErrorCode()==-8738)) {
                    log.info("multi database connections is disconnect or Statement must be recompiled.retry commit table!");
                    try {
                        esgyndb.CloseConnection(dbConn);
                    } catch (Exception e1) {
                    }
                    dbConn=esgyndb.CreateConnection(false);
                    //reInitStmt
                    for (TableState tableState : tables.values()) {
                        tableState.InitStmt(dbConn, skip);
                    }

                    commit_table_success = commit_tables();
                    log.info("multi database connections is disconnect or Statement must be recompiled.retry commit table success:"+commit_table_success);
                }
            }
            return commit_table_success;
        }
    }

    public boolean commit_tables_() throws SQLException {
        for (TableState tableState : tables.values()) {
    		if (!tableState.CommitTable(outPutPath,format)) {
                esgyndb.AddErrInsertNum(tableState.GetErrInsertRows());
                esgyndb.AddErrUpdateNum(tableState.GetErrUpdateRows());
                esgyndb.AddErrDeleteNum(tableState.GetErrDeleteRows());
                esgyndb.AddKafkaPollNum(kafkaPollNum);
                kafkaPollNum = 0;
                esgyndb.setLatestTime(latestTime);
                latestTime = 0;
                esgyndb.AddTransTotal(tableState.GetTransTotal());
                esgyndb.AddTransFails(tableState.GetTransFails());
                tableState.ClearCache();
                if (!skip) 
                return false;
            }
        }
        if (log.isDebugEnabled()) {
            log.trace("kafka commit.tables:[" + tables.size() + "]");
        }
        kafkaconsumer.commitSync();
        for (TableState tableState : tables.values()) {
            esgyndb.AddInsMsgNum(tableState.GetCacheInsert());
            esgyndb.AddUpdMsgNum(tableState.GetCacheUpdate());
            esgyndb.AddKeyMsgNum(tableState.GetCacheUpdkey());
            esgyndb.AddDelMsgNum(tableState.GetCacheDelete());

            esgyndb.AddInsertNum(tableState.GetInsertRows());
            esgyndb.AddUpdateNum(tableState.GetUpdateRows());
            esgyndb.AddDeleteNum(tableState.GetDeleteRows());

            esgyndb.AddTotalNum(cacheNum);
            cacheNum = 0;
            esgyndb.AddKafkaPollNum(kafkaPollNum);
            kafkaPollNum = 0;
            esgyndb.setLatestTime(latestTime);
            latestTime = 0;
            esgyndb.AddTransTotal(tableState.GetTransTotal());
            esgyndb.AddTransFails(tableState.GetTransFails());
            tableState.ClearCache();
        }
        if (tables.size()==0) {
            esgyndb.AddKafkaPollNum(kafkaPollNum);
            kafkaPollNum = 0;
        }
	return true;
    }

    public boolean ProcessRecord() {
        // note that we don't commitSync to kafka - tho we should
        ConsumerRecords<?, ?> records = kafkaconsumer.poll(kafkaParams.getStreamTO());
        if (log.isDebugEnabled()) {
            log.debug("poll messages: " + records.count());
        }
        if (records.isEmpty())
            return false; // timed out

        cacheNum += records.count();
        kafkaPollNum +=records.count();
        ProcessMessages(records);

        if(!commit_tables())
        return false ;//commit tables faild And not --skip

        return true;
    }

    public void ProcessMessages(ConsumerRecords<?, ?> records) {
        if (log.isTraceEnabled()) {
            log.trace("enter function");
        }

        for (ConsumerRecord<?, ?> record : records) {

            try {
                ProcessMessage(record);
            } catch (ArrayIndexOutOfBoundsException aiooe) {
                log.error("table schema is not matched with data, raw data: [" + record + "]",aiooe);
            } catch (UnsupportedEncodingException uee) {
                log.error("the encoding is not supported in java, raw data: [" + record + "]",uee);
            }
        } // for each msg
        if (log.isTraceEnabled()) {
            log.trace("exit function");
        }
    }

    public <T> void ProcessMessage(ConsumerRecord<?, ?> record)
            throws UnsupportedEncodingException {
        if (log.isTraceEnabled()) {
            log.trace("enter function");
        }
        // position info for this message
        long partition = record.partition();
        long offset = record.offset();
        String topic = record.topic();
        latestTime = record.timestamp();
        long num = 0;

        if (partition != partitionID) {
            log.error("message info [topic: " + topic + ", partition: " + partition + ", off: "
                    + offset + "], current partition #" + partitionID);
            if (log.isTraceEnabled()) {
                log.trace("exit function");
            }
            return;
        }

        T msg = (T) record.value();
        String tableName = esgyndb.GetDefaultSchema() + "." + esgyndb.GetDefaultTable();
        TableState tableState = tables.get(tableName);
        if (log.isTraceEnabled()) {
            log.trace("1 tableNameFull[" + tableName + "],\ntableState if null ["
                    + (tableState == null) + "]");
        }
        // if tableName is null,should found it from message
        if (esgyndb.GetDefaultTable() != null) {
            if (tableState == null) {
                TableInfo tableInfo = esgyndb.GetTableInfo(tableName);
                if (tableInfo == null) {
                    if (log.isDebugEnabled()) {
                        log.warn("the table [" + tableName + "] is not exists!");
                    }

                    return;
                }

                tableState = new TableState(tableInfo,format, batchUpdate);

                if (!isInitStmt(tableState)) {
                    if (log.isDebugEnabled()) {
                        log.warn("init the table [" + tableName + "] fail!");
                    }
                    return;
                }
            } else {
                if (log.isTraceEnabled()) {
                    log.debug(" tableInfo if null [" + (tableState.GetTableInfo() == null) + "]");
                }
            }
        }

        MessageTypePara typeMessage = new MessageTypePara(esgyndb, tables, tableState, dbConn,
                delimiter, partitionID, msg, encoding, bigEndian,offset);

        if (!urm.init(typeMessage))
            return;

        if (!urm.AnalyzeMessage())
            return;

        if (log.isDebugEnabled()) {
            log.debug("operatorType[" + urm.GetOperatorType() + "]\n" + "cacheNum [" + cacheNum
		      + "]\n" + "commitCount [" + kafkaParams.getCommitCount() + "]");
        }
        if (urm.GetOperatorType().equals("K")&&(!batchUpdate)) {
            commit_tables();
            if (log.isDebugEnabled()) {
                log.debug(" before the table [" + tableName + "] message has commit"
                        + " due to there is \"K\" operate");
            }
        }

        if (esgyndb.GetDefaultTable() == null) {
            tableName = urm.GetSchemaName() + "." + urm.GetTableName();
            tableState = tables.get(tableName);
            if (tableState == null) {
                TableInfo tableInfo = esgyndb.GetTableInfo(tableName);

                if (tableInfo == null) {
                    if (log.isDebugEnabled()) {
                        log.warn("the table [" + tableName + "] is not exists!");
                    }
                    return;
                }

                tableState = new TableState(tableInfo,format, batchUpdate);

                if (!isInitStmt(tableState)) {
                    if (log.isDebugEnabled()) {
                        log.warn("init the table [" + tableName + "] fail!");
                    }
                    return;
                }
            }
        }
	RowMessage<T> urmClone = null;
	try {
            urmClone = (RowMessage)urm.clone();
        } catch (CloneNotSupportedException e) {
            log.error("clone the RowMessage failed.",e);
        }

        if (log.isDebugEnabled()) {
            log.debug("start insert message to table , urm [" + urm.toString() + "],"
                    + "tableState if null [" + (tableState == null) + "]");
        }

        tableState.InsertMessageToTable(urmClone);
        if (log.isDebugEnabled()) {
            log.debug("put table state in map :" + tableName + "  " + tableState.toString());
        }
        tables.put(tableName, tableState);

        if (log.isDebugEnabled()) {
            log.debug("operatorType[" + urm.GetOperatorType() + "]\n" + "cacheNum [" + cacheNum
		      + "]\n" + "commitCount [" + kafkaParams.getCommitCount() + "]");
        }
        if (urm.GetOperatorType().equals("K")&&(!batchUpdate)) {
            commit_tables();
            if (log.isDebugEnabled()) {
                log.debug(" before the table [" + tableName + "] message has commit"
                        + " due to there is \"K\" operate");
            }
        }

        if (log.isTraceEnabled()) {
            log.trace("exit function");
        }
    }

    public void seekToTime(KafkaConsumer<?, ?> kafkaconsumer,TopicPartition partition,Utils utils) {
        long offset=0;
        long beginOffset=0;
        boolean offsetGreaterThanEnd=false;

        //get offset by startTime
        String dateFormat = utils.whichDateFormat(kafkaParams.getFull());
        long startDateStamp = utils.dateToStamp(kafkaParams.getFull(), dateFormat);

        Map<TopicPartition, Long> startMap = new HashMap<>();
        startMap.put(partition, startDateStamp);
        Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = kafkaconsumer.offsetsForTimes(startMap);
        OffsetAndTimestamp offsetAndTimestamp = offsetsForTimes.get(partition);
        //if offsetAndTimestamp is null means offsetGreaterThanEnd
        if (offsetAndTimestamp!=null) {
            offset = offsetAndTimestamp.offset();
        }else {
            offsetGreaterThanEnd=true;
        }
        //get begin offset
        ArrayList<TopicPartition> partitions = new ArrayList<>();
        partitions.add(partition);
        Map<TopicPartition, Long> beginningOffsets = kafkaconsumer.beginningOffsets(partitions);
        if (beginningOffsets!=null) {
            beginOffset = beginningOffsets.get(partition);
        }

        if (offset <beginOffset) {
            log.info("poll the data from beginning:"+beginOffset);
            kafkaconsumer.seekToBeginning(partitions);;
        }else if (offsetGreaterThanEnd) {
            log.info("poll the data from latest");
            kafkaconsumer.seekToEnd(partitions);
        }else {
            log.info("poll the data from offset:"+offset);
            kafkaconsumer.seek(partition, offset);
        }
    }

    public boolean KeepAliveEveryConn() {
        if (dbConn!=null) {
            if (aconn) {
                synchronized (ConsumerThread.class) {
                    execKeepAliveStmt();
                }
            }else {
                execKeepAliveStmt();
            }
        }
        return true;
    }
    public boolean execKeepAliveStmt() {
        ResultSet columnRS = null;
        PreparedStatement keepStmt =null;
        try {
            log.info("prepare the keepaliveEveryConn stmt, query:" + keepQuery);
            keepStmt = dbConn.prepareStatement(keepQuery);
            columnRS = keepStmt.executeQuery();
            while (columnRS.next()) {
                columnRS.getString("(EXPR)");
            }
            dbConn.commit();
        } catch (SQLException se) {
            log.error("execKeepAliveStmt is faild.",se);
            return false;
        } finally {
            if (columnRS != null && keepStmt != null) {
                try {
                    keepStmt.close();
                    columnRS.close();
                } catch (SQLException e) {
                    log.error("keepalivestmt colse faild in method execKeepAliveStmt",e);
                    return false;
                }
            }
        }
        return true;
    }

    public boolean isInitStmt(TableState tableState) {
        if (aconn) {
            synchronized (ConsumerThread.class) {
            if (!tableState.InitStmt(dbConn,skip))
                return false;
            }
        }else {
            if (!tableState.InitStmt(dbConn,skip))
                return false;
        }
        return true;
    }

    public int GetConsumerID() {
        return partitionID;
    }

    public synchronized boolean GetState() {
        return running.get();
    }

    public synchronized void Close() {
        running.set(false);
        kafkaconsumer.wakeup();
    }
}
