package com.esgyn.kafkaCDC.server.kafkaConsumer;

import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;
import java.sql.Connection;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.log4j.Logger;

import com.esgyn.kafkaCDC.server.esgynDB.EsgynDB;
import com.esgyn.kafkaCDC.server.esgynDB.TableInfo;
import com.esgyn.kafkaCDC.server.esgynDB.TableState;
import com.esgyn.kafkaCDC.server.esgynDB.MessageTypePara;
import com.esgyn.kafkaCDC.server.kafkaConsumer.messageType.RowMessage;

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
    // execution settings
    String                      zookeeper;
    String                      broker;
    String                      topic;
    String                      groupid;
    long                        streamTO;
    long                        zkTO;
    int                         partitionID;
    long                        commitCount;
    long                        cacheNum;
    long                        kafkaPollNum;

    String                      encoding;
    String                      key;
    String                      value;
    String                      kafkauser;
    String                      kafkapasswd;

    String                      full;
    boolean                     skip;
    boolean                     bigEndian;
    String                      delimiter;
    String                      format;

    Map<String, TableState>     tables  = null;
    KafkaConsumer<?, ?>         kafkaconsumer;
    private final AtomicBoolean running = new AtomicBoolean(true);
    Connection                  dbConn  = null;
    boolean                     aconn   = false;
    RowMessage<T>               urm     = null;
    // e.g.:com.esgyn.kafkaCDC.server.kafkaConsumer.messageType.UnicomRowMessage
    private String              messageClass;
    private String              outPutPath;

    private static Logger       log     = Logger.getLogger(ConsumerThread.class);

    public ConsumerThread(EsgynDB esgyndb_, String full_, boolean skip_, boolean bigEndian_,
            String delimiter_, String format_, String zookeeper_, String broker_, String topic_,
            String groupid_, String encoding_, String key_, String value_,String kafkauser_ ,
            String kafkapasswd_, int partitionID_,long streamTO_, long zkTO_, long commitCount_, 
            String messageClass_,String outPutPath_,Connection dbConn_,boolean aconn_) {
        if (log.isTraceEnabled()) {
            log.trace("enter function");
        }
        esgyndb   = esgyndb_;
        zookeeper = zookeeper_;
        broker = broker_;
        topic = topic_;
        groupid = groupid_;
        encoding = encoding_;
        partitionID = partitionID_;
        streamTO = streamTO_;
        zkTO = zkTO_;
        commitCount = commitCount_;
        cacheNum = 0;
        kafkaPollNum = 0;
        bigEndian = bigEndian_;

        format = format_;
        delimiter = delimiter_;
        key = key_;
        value = value_;
        kafkauser = kafkauser_;
        kafkapasswd = kafkapasswd_;
        full = full_;
        skip = skip_;
        messageClass = messageClass_;
        outPutPath = outPutPath_;
        aconn = aconn_;

        tables = new HashMap<String, TableState>(0);
        Properties props = new Properties();

        if (aconn) {
            dbConn=dbConn_;
        }
        if (zookeeper != null) {
            props.put("zookeeper.connect", zookeeper);
        } else {
            props.put("bootstrap.servers", broker);
        }
        props.put("group.id", groupid);
        props.put("enable.auto.commit", "false");
        props.put("max.partition.fetch.bytes", 10485760);
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", key);
        props.put("value.deserializer", value);
        props.put("max.poll.records", (int) commitCount);
        
        if (kafkauser !="") {
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=" + kafkauser + " password="+kafkapasswd+";");
        }
      


        kafkaconsumer = new KafkaConsumer(props);


        TopicPartition partition = new TopicPartition(topic, partitionID);
        kafkaconsumer.assign(Arrays.asList(partition));

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
                kafkaconsumer.seek(partition, Long.parseLong(full));
                break;
        }
        // building RowMessage
        try {
            urm = (RowMessage<T>) Class.forName(messageClass).newInstance();
        } catch (InstantiationException ine) {
            log.error("when forName messageClass,there is a error: [" + ine.getMessage() + "]");
            ine.printStackTrace();
        } catch (IllegalAccessException ine) {
            log.error("when forName messageClass,there is a error: [" + ine.getMessage() + "]");
            ine.printStackTrace();
        } catch (ClassNotFoundException cnfe) {
            log.error("when forName messageClass,there is a error: [" + cnfe.getMessage()
                    + "]make sure the full-qualified name is right");
            cnfe.printStackTrace();
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
            if (!aconn)
            dbConn = esgyndb.CreateConnection(false);
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
            esgyndb.DisplayDatabase();
            kafkaconsumer.close();
            running.set(false);
        }
        if (log.isTraceEnabled()) {
            log.trace("exit function");
        }
    }

    public boolean commit_tables() {
        for (TableState tableState : tables.values()) {
    		if (!tableState.CommitTable(outPutPath,format)) {
                esgyndb.AddErrInsertNum(tableState.GetErrInsertRows());
                esgyndb.AddErrUpdateNum(tableState.GetErrUpdateRows());
                esgyndb.AddErrDeleteNum(tableState.GetErrDeleteRows());
                esgyndb.AddKafkaPollNum(kafkaPollNum);
                kafkaPollNum = 0;
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
        ConsumerRecords<?, ?> records = kafkaconsumer.poll(streamTO);
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
                log.error("table schema is not matched with data, raw data: [" + record + "]");
                aiooe.printStackTrace();
            } catch (UnsupportedEncodingException uee) {
                log.error("the encoding is not supported in java, raw data: [" + record + "]");
                uee.printStackTrace();
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

                tableState = new TableState(tableInfo);
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
                    + "]\n" + "commitCount [" + commitCount + "]");
        }
        if (urm.GetOperatorType().equals("K")) {
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

                tableState = new TableState(tableInfo);
            }
        }

        if (!tableState.InitStmt(dbConn,skip)) {
            if (log.isDebugEnabled()) {
                log.warn("init the table [" + tableName + "] fail!");
            }
            return;
        }

	    RowMessage<T> urmClone = null;
	    try {
            urmClone = (RowMessage)urm.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
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
                    + "]\n" + "commitCount [" + commitCount + "]");
        }
        if (urm.GetOperatorType().equals("K")) {
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
