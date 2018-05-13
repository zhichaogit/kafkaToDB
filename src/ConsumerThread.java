import java.util.Arrays;
import java.util.Map;
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

    boolean full;
    boolean skip;
    String  delimiter;
    String  format;

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
		   int     partitionID_,
		   long    streamTO_,
		   long    zkTO_,
		   long    commitCount_) 
    {
	log.trace("enter function");
	esgyndb     = esgyndb_;
	zookeeper   = zookeeper_; 
	broker      = broker_;
	topic       = topic_;
	groupid     = groupid_;
	partitionID = partitionID_;
	streamTO    = streamTO_;
	zkTO        = zkTO_;
	commitCount = commitCount_;

	format      = format_;
	delimiter   = delimiter_;
	full        = full_;
	skip        = skip_;

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
	props.put("key.deserializer", 
		  "org.apache.kafka.common.serialization.StringDeserializer");
	props.put("value.deserializer", 
		  "org.apache.kafka.common.serialization.StringDeserializer");

	kafka = new KafkaConsumer<>(props);

	//kafka.subscribe(Arrays.asList(topic));
	TopicPartition partition = new TopicPartition(topic, partitionID);
	kafka.assign(Arrays.asList(partition));

	if (full) {
	    // priming poll
	    kafka.poll(100);
	    // always start from beginning
	    kafka.seekToBeginning(Arrays.asList(partition));
	}
	log.trace("exit function");
    }
	
    public void run() 
    {
	log.trace("enter function");

	try {
	    dbConn = esgyndb.CreateConnection(false);
	    log.info("consumer server started.");
	    while(running.get()) {
		// note that we don't commitSync to kafka - tho we should
		ConsumerRecords<String, String> records = kafka.poll(streamTO);

		log.debug("poll messages: " + records.count());
		if (records.isEmpty())
		    break;               // timed out

		ProcessRecords(records);
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
	    esgyndb.CommitAllDatabase(dbConn);
	    log.info("close connection");
	    esgyndb.CloseConnection(dbConn);
	    esgyndb.DisplayDatabase();
	    kafka.close();
	    running.set(false);
	}
	log.trace("exit function");
    }

    public void ProcessRecords(ConsumerRecords<String, String> records) 
    {
	log.trace("enter function");
	for (ConsumerRecord<String, String> message : records) {
	    try {
		ProcessMessage(message);
	    } catch (ArrayIndexOutOfBoundsException aiooe) {
		log.error ("table schema is not matched with data, raw data: [" 
			   + message + "]");
		aiooe.printStackTrace();
	    }
	} // for each msg
	log.trace("exit function");
    }

    public void ProcessMessage(ConsumerRecord<String, String> message) 
    {
	log.trace("enter function");
	// position info for this message
	long    partition = message.partition();
	long    offset    = message.offset();
	String  topic     = message.topic();
	long    num       = 0;

	if (partition != partitionID) {
	    log.error("message info [topic: " + topic + ", partition: " 
		      + partition + ", off: " + offset + "], current partition #" 
		      + partitionID);
	    log.trace("exit function");
	    return;
	}

	RowMessage urm = null;
	if (format.equals("unicom"))
	    urm = new UnicomRowMessage(esgyndb.GetDefaultSchema(),
				       esgyndb.GetDefaultTable(),
				       delimiter, partitionID, message.value());
	else
	    urm = new RowMessage(esgyndb.GetDefaultSchema(), 
				 esgyndb.GetDefaultTable(),
				 delimiter, partitionID, message.value());

	urm.AnalyzeMessage();

	esgyndb.InsertMessageToDatabase(dbConn, urm);

	log.trace("exit function");
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
