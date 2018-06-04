import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;
import java.sql.Connection;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger; 

import java.lang.ArrayIndexOutOfBoundsException;
import java.io.UnsupportedEncodingException;
import kafka.consumer.ConsumerTimeoutException;

@SuppressWarnings("deprecation") 		

/*
 * This represents a typical Kafka consumer that uses EsgynDB for data storage.
 * It is a single-threaded server that can be replicated to scale out - each copy 
 * handling a partition of a topic.
 * 
 * Execution of each server in the group handled by...
 *   TBD
 *   a.  pdsh script per node
 *   b.  zookeeper?
 */
public class ConsumerThread extends Thread
{
    EsgynDB esgyndb;
    // execution settings
    String  zookeeper; 
    String  broker; 
    String  topic;
    String  groupid;
    long    streamTO;
    long    zkTO;
    int     partitionID;
    long    commitCount;
    long    cacheNum;

    String  encoding;
    String  key;
    String  value;

    boolean full;
    boolean skip;
    String  delimiter;
    String  format;

    Map<String, TableInfo>  tables = null;
    KafkaConsumer<String, String> kafka;
    private final AtomicBoolean running = new AtomicBoolean(true);
    Connection  dbConn = null;

    private static Logger log = Logger.getLogger(ConsumerThread.class);

    ConsumerThread(EsgynDB esgyndb_,
		   boolean full_,
		   boolean skip_,
		   String  delimiter_,
		   String  format_,
		   String  zookeeper_,
		   String  broker_,
		   String  topic_,
		   String  groupid_,
		   String  encoding_,
		   String  key_,
		   String  value_,
		   int     partitionID_,
		   long    streamTO_,
		   long    zkTO_,
		   long    commitCount_) 
    {
	if (log.isTraceEnabled()){
	    log.trace("enter function");
	}
	esgyndb     = esgyndb_;
	zookeeper   = zookeeper_; 
	broker      = broker_;
	topic       = topic_;
	groupid     = groupid_;
	encoding    = encoding_;
	partitionID = partitionID_;
	streamTO    = streamTO_;
	zkTO        = zkTO_;
	commitCount = commitCount_;
	cacheNum    = 0;

	format      = format_;
	delimiter   = delimiter_;
	key         = key_;
	value       = value_;
	full        = full_;
	skip        = skip_;

	tables = new HashMap<String, TableInfo>(0);
	Properties props = new Properties();

	if (zookeeper != null){
	    props.put("zookeeper.connect", zookeeper);
	} else {
	    props.put("bootstrap.servers", broker);
	}
	props.put("group.id", groupid);
	props.put("enable.auto.commit", "true");
	props.put("auto.commit.interval.ms", "1000");
	props.put("session.timeout.ms", "30000");
	props.put("key.deserializer", key);
	props.put("value.deserializer", value);

	kafka = new KafkaConsumer<String, String>(props);

	TopicPartition partition = new TopicPartition(topic, partitionID);
	kafka.assign(Arrays.asList(partition));

	if (full) {
	    // priming poll
	    kafka.poll(100);
	    // always start from beginning
	    kafka.seekToBeginning(Arrays.asList(partition));
	}

	if (log.isTraceEnabled()){
	    log.trace("exit function");
	}
    }
	
    public void run() 
    {
	if (log.isTraceEnabled()){
	    log.trace("enter function");
	}

	try {
	    dbConn = esgyndb.CreateConnection(false);
	    log.info("consumer server started.");
	    while(running.get()) {
		// note that we don't commitSync to kafka - tho we should
		ConsumerRecords<String, String> records = kafka.poll(streamTO);
		if(log.isDebugEnabled()){
		    log.debug("poll messages: " + records.count());
		}
		if (records.isEmpty())
		    break;               // timed out

		cacheNum += records.count();
		ProcessRecords(records);
		
		if (cacheNum > commitCount) {
		    commit_tables();
		}
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
	    log.info("close connection");
	    esgyndb.CloseConnection(dbConn);
	    esgyndb.DisplayDatabase();
	    kafka.close();
	    running.set(false);
	}
	if (log.isTraceEnabled()){
	    log.trace("exit function");
	}
    }

    public void commit_tables() {
	for (TableInfo tableinfo : tables.values()) {
	    if (!tableinfo.CommitTable()){
		return;
	    }
	}

	kafka.commitSync();
	for (TableInfo tableinfo : tables.values()) {
	    esgyndb.AddInsMsgNum(tableinfo.GetCacheInsert());
	    esgyndb.AddUpdMsgNum(tableinfo.GetCacheUpdate());
	    esgyndb.AddKeyMsgNum(tableinfo.GetCacheUpdkey());
	    esgyndb.AddDelMsgNum(tableinfo.GetCacheDelete());

	    esgyndb.AddInsertNum(tableinfo.GetInsertRows());
	    esgyndb.AddUpdateNum(tableinfo.GetUpdateRows());
	    esgyndb.AddDeleteNum(tableinfo.GetDeleteRows());

	    esgyndb.AddTotalNum(cacheNum);
	    cacheNum = 0;

	    tableinfo.ClearCache();
	}
    }

    public void ProcessRecords(ConsumerRecords<String, String> records) 
    {
	if (log.isTraceEnabled()){
	    log.trace("enter function");
	}
	for (ConsumerRecord<String, String> message : records) {
	    try {
		ProcessMessage(message);
	    } catch (ArrayIndexOutOfBoundsException aiooe) {
		log.error ("table schema is not matched with data, raw data: [" 
			   + message + "]");
		aiooe.printStackTrace();
	    } catch (UnsupportedEncodingException uee) {
		log.error ("the encoding is not supported in java, raw data: [" 
			   + message + "]");
		uee.printStackTrace();
	    }
	} // for each msg
	if (log.isTraceEnabled()){
	    log.trace("exit function");
	}
    }

    public void ProcessMessage(ConsumerRecord<String, String> message) 
	throws UnsupportedEncodingException
    {
	if (log.isTraceEnabled()){
	    log.trace("enter function");
	}
	// position info for this message
	long    partition = message.partition();
	long    offset    = message.offset();
	String  topic     = message.topic();
	long    num       = 0;

	if (partition != partitionID) {
	    log.error("message info [topic: " + topic + ", partition: " 
		      + partition + ", off: " + offset + "], current partition #" 
		      + partitionID);
	    if (log.isTraceEnabled()){
		log.trace("exit function");
	    }
	    return;
	}

	RowMessage urm = null;
	String     msgStr = message.value();

	if (format.equals("Unicom")) {
	    String     dbMsg = new String(msgStr.getBytes(encoding), "UTF-8");
	    urm = new UnicomRowMessage(esgyndb.GetDefaultSchema(),
				       esgyndb.GetDefaultTable(),
				       delimiter, partitionID, dbMsg);
	} else if (format.equals("HongQuan")) {
	    StringBuffer strBuffer = new StringBuffer();
	    strBuffer.append("message [" + msgStr + "] length: " + msgStr.length() 
			     + "\nraw data [");
	    for (int i = 0; i < msgStr.length(); i++) {
		String temp = Integer.toHexString(msgStr.charAt(i) & 0xFF);  
		if(temp.length() == 1){  
		    temp = "0" + temp;  
		}  
		strBuffer.append(" " + temp);
	    }
	    strBuffer.append("]");
	    log.debug(strBuffer);
	    urm = new HongQuanRowMessage(esgyndb.GetDefaultSchema(),
					 esgyndb.GetDefaultTable(),
					 delimiter, partitionID, msgStr);
	} else {
	    String     dbMsg = new String(msgStr.getBytes(encoding), "UTF-8");
	    urm = new RowMessage(esgyndb.GetDefaultSchema(), 
				 esgyndb.GetDefaultTable(),
				 delimiter, partitionID, dbMsg);
	}

	urm.AnalyzeMessage();

	String    tableName = urm.GetSchemaName() + "." + urm.GetTableName();
	TableInfo tableInfo = esgyndb.GetTableInfo(tableName);

	if (tableInfo == null || !tableInfo.InitStmt(dbConn)) {
	    if (log.isDebugEnabled()) {
		log.warn("the table [" + tableName + "] is not exists!");
	    }
	    return;
	}

	tableInfo.InsertMessageToTable(urm);

	tables.put(tableName, tableInfo);
	if (log.isTraceEnabled()){
	    log.trace("exit function");
	}
    }

    public int GetConsumerID() 
    {
	return partitionID;
    }

    public synchronized boolean GetState() 
    {
	return 	running.get();
    } 

    public synchronized void Close() 
    {
	running.set(false);
	kafka.wakeup();
    }
}
