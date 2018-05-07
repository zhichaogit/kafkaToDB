import java.util.Arrays;
import java.util.ArrayList;
import java.util.Properties;
import java.sql.Connection;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger; 

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
public class ConsumerServer 
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
    boolean running = true;

    boolean full;
    boolean skip;
    String  delimiter;
    String  format;

    KafkaConsumer<String, String> kafka;
    Connection  dbconn = null;

    private long    messagenum = 0;
    private long    insertnum = 0;
    private long    updatenum = 0;
    private long    deletenum = 0;
    private long    updkeynum = 0;

    private static Logger log = Logger.getLogger(ConsumerServer.class);

    ConsumerServer(EsgynDB esgyndb_,
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
	esgyndb     = esgyndb_;
	zookeeper   = zookeeper_; 
	broker      = broker_;
	topic       = topic_;
	groupid     = groupid_;
	partitionID = partitionID_;
	streamTO    = streamTO_;
	zkTO        = zkTO_;
	commitCount = commitCount_;

	running     = true;

	format      = format_;
	delimiter   = delimiter_;
	full        = full_;
	skip        = skip_;

	messagenum  = 0;
	updatenum   = 0;
	deletenum   = 0;
	updkeynum   = 0;

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
    }	
	
    public void ProcessMessages() 
    {
	try {
	    long startTime = System.currentTimeMillis();
	    log.info("ProcessMessages start ...");

	    dbconn = esgyndb.CreateConnection();
	    while(running) {
		// note that we don't commitSync to kafka - tho we should
		ConsumerRecords<String, String> records = kafka.poll(streamTO);

		if (records.isEmpty())
		    break;               // timed out

		ProcessRecords(records);
	    } // while true
	    esgyndb.CloseConnection(dbconn);
	    long diff = (System.currentTimeMillis() - startTime );

	    log.info("ProcessMessages messages infomation in " + (diff - streamTO) 
		     + " ms, batch size = " + commitCount
		     + "\n\t Total Messages: " + messagenum 
		     + "\n\tInsert Messages: " + insertnum
		     + "\n\tUpdate Messages: " + updatenum
		     + "\n\tDelete Messages: " + deletenum
		     + "\n\tUpdkey Messages: " + updkeynum);
	} catch (ConsumerTimeoutException cte) {
	    if (!skip)
		log.error("ProcessMessages consumer time out; " + cte.getMessage());
	}
	/*
	catch (BatchUpdateException bx) {
	    int[] insertCounts = bx.getUpdateCounts();
	    int count = 1;
	    for (int i : insertCounts) {
		if ( i == Statement.EXECUTE_FAILED ) 
		    log.error("Error on request #" + count +": Execute failed");
		else 
		    count++;
	    }
	    log.error(bx.getMessage());
	    } 
	catch (SQLException sx) {
	    log.error ("SQL error: " + sx.getMessage());
	}*/
    }

    public void ProcessRecords(ConsumerRecords<String, String> records) 
    {
	for (ConsumerRecord<String, String> message : records) {
	    ProcessMessage(message);
	} // for each msg
    }

    public void ProcessMessage(ConsumerRecord<String, String> message) 
    {
	// position info for this message
	long    partition = message.partition();
	long    offset    = message.offset();
	String  topic     = message.topic();
	long    num       = 0;

	if (partition != partitionID) {
	    log.error("ProcessMessage message info [topic: " + topic 
		      + ", partition: " + partition + ", offset: " + offset 
		      + "], current partition #" + partitionID);
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

	ArrayList<ColumnValue> columns = urm.GetColumns();
	log.debug("Database schema: " + urm.GetSchemaName() + ", table name: "
		  + urm.GetTableName() + ", partition: " + partitionID );
	switch(urm.GetOperatorType()) {
	case "I":
	    num = esgyndb.InsertData(dbconn, urm.GetSchemaName(), 
				     urm.GetTableName(), partitionID, columns);
	    insertnum += num;
	    break;
	case "U":
	    num = esgyndb.UpdateData(dbconn, urm.GetSchemaName(), 
				     urm.GetTableName(), partitionID, columns);
	    updkeynum += num;
	    break;
	case "K":
	    num = esgyndb.UpdateData(dbconn, urm.GetSchemaName(), 
				     urm.GetTableName(), partitionID, columns);
	    updatenum += num;
	    break;
	case "D":
	    num = esgyndb.DeleteData(dbconn, urm.GetSchemaName(), 
				     urm.GetTableName(), partitionID, columns);
	    deletenum += num;
	    break;

	default:
	    log.error("ProcessMessage operator [" + urm.GetOperatorType() + "]");
	}
	messagenum += num;
    }

    public int GetConsumerID() 
    {
	return partitionID;
    }

    public void DropConsumer() 
    {
	running = false;
	kafka.close();
    }
}
