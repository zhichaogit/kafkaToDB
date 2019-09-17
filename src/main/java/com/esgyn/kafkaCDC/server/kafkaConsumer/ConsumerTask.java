package com.esgyn.kafkaCDC.server.kafkaConsumer;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;

import kafka.consumer.ConsumerTimeoutException;

import com.esgyn.kafkaCDC.server.utils.Utils;
import com.esgyn.kafkaCDC.server.utils.FileUtils;
import com.esgyn.kafkaCDC.server.utils.Constants;
import com.esgyn.kafkaCDC.server.utils.Parameters;
import com.esgyn.kafkaCDC.server.utils.KafkaParams;

import com.esgyn.kafkaCDC.server.databaseLoader.LoaderHandle;
import com.esgyn.kafkaCDC.server.kafkaConsumer.messageType.RowMessage;

import lombok.Getter;
import lombok.Setter;

public class ConsumerTask<T> {
    // the states
    public final static int          STATE_INIT_FAIL         = 1;
    public final static int          STATE_ARRAY_OVERRUN     = 2;
    public final static int          STATE_CLONE_NOT_SUPPORT = 3;
    public final static int          STATE_NORMAL_EXCEPTION  = 4;
    public final static int          STATE_DUMP_DATA_FAIL    = 5;
    public final static int          STATE_PARTITION_ERROR   = 6;
    public final static int          STATE_TOPIC_ERROR       = 7;

    @Setter
    @Getter
    private String              topic                        = null;
    @Setter
    @Getter
    private String              group                        = null;
    @Setter
    @Getter
    private int                 partitionID                  = -1;
    @Setter
    @Getter
    private LoaderHandle        loaderHandle                 = null;
    private ConsumeStates       consumeStates                = null;
    /*
     * state value mean:
     * 0: work as design
     * !0: there are exception, cann't poll data from Kafka
     */
    @Getter
    private int                 state                        = 0;
    @Getter
    private long                consumeNumber                = 0;
    @Getter
    private long                curTime                      = 0;
    @Getter
    private long                curOffset                    = -1;

    private Parameters          params                       = null;
    private KafkaParams         kafkaParams                  = null;
    private String              messageClass                 = null;

    private RowMessage<T>       urm                          = null;
    private KafkaConsumer<?, ?> kafkaConsumer                = null;

    private static Logger       log     = Logger.getLogger(ConsumerTask.class);

    public ConsumerTask(ConsumeStates consumeStates_, String topic_, String group_,
			int partitionID_, LoaderHandle loaderHandle_) {
        if (log.isTraceEnabled()) { log.trace("enter"); }

	consumeStates  = consumeStates_;
	topic          = topic_;
	group          = group_;
        partitionID    = partitionID_;

	state          = 0;
	consumeNumber  = 0;

	params         = consumeStates.getParams();;
	kafkaParams    = params.getKafka();
	messageClass   = params.getKafkaCDC().getMsgClass();

	loaderHandle   = loaderHandle_; 

        if (log.isTraceEnabled()) { log.trace("exit"); }
    }

    public boolean init() {
	Properties props = createProperties();
        kafkaConsumer = new KafkaConsumer(props);

        TopicPartition partition = new TopicPartition(topic, partitionID);
        kafkaConsumer.assign(Arrays.asList(partition));

	String mode = kafkaParams.getMode();
        switch (mode) {
            case "START":
                kafkaConsumer.seekToBeginning(Arrays.asList(partition));
                break;
            case "END":
                kafkaConsumer.seekToEnd(Arrays.asList(partition));
                break;
            case "":
                break;
            default:
                if (Utils.isDateStr(mode)) {
                    seekToTime(kafkaConsumer, partition);
                }else {
                    kafkaConsumer.seek(partition, Long.parseLong(mode));
                }
                break;
        }

	if (!buildURM())
	    return false;

        if (log.isTraceEnabled()) { log.trace("enter"); }

	return true;
    }

    private boolean buildURM() {
	boolean retValue = true;

        if (log.isTraceEnabled()) { log.trace("enter"); }

        // building RowMessage
        try {
            urm = (RowMessage<T>) Class.forName(messageClass).newInstance();
        } catch (InstantiationException ine) {
            log.error("when forName messageClass, there is a error: [" 
		      + ine.getMessage() + "]", ine);
	    retValue = false;
        } catch (IllegalAccessException iae) {
            log.error("when forName messageClass, there is a error: [" 
		      + iae.getMessage() + "]", iae);
	    retValue = false;
        } catch (ClassNotFoundException cnfe) {
            log.error("when forName messageClass, there is a error: [" 
		      + cnfe.getMessage() + "] make sure the mode is right", cnfe);
	    retValue = false;
        } finally {

	    if (log.isTraceEnabled()) { log.trace("exit"); }
	    return retValue;
	}
    }

    private Properties createProperties() {
        if (log.isTraceEnabled()) { log.trace("enter"); }

        Properties props = new Properties();

        if (kafkaParams.getZookeeper() != null) {
            props.put("zookeeper.connect", kafkaParams.getZookeeper());
        } else {
            props.put("bootstrap.servers", kafkaParams.getBroker());
        }

        props.put("group.id", group);
        props.put("enable.auto.commit", "false");
        props.put("max.partition.fetch.bytes", (int)kafkaParams.getFetchBytes());
        props.put("heartbeat.interval.ms",     kafkaParams.getHbTO());
        props.put("session.timeout.ms",        kafkaParams.getSeTO());
        props.put("request.timeout.ms",        kafkaParams.getReqTO());
        props.put("key.deserializer",          kafkaParams.getKey());
        props.put("value.deserializer",        kafkaParams.getValue());
        props.put("max.poll.records",          (int) kafkaParams.getFetchSize());
        
        if (kafkaParams.getKafkaUser() != null) {
	    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, 
		      "SASL_PLAINTEXT");
	    props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
	    props.put("sasl.jaas.config", Constants.SEC_PLAIN_STRING
		      + kafkaParams.getKafkaUser() + " password=" 
		      + kafkaParams.getKafkaPW() + ";");
        }

        if (log.isTraceEnabled()) { log.trace("exit"); }

	return props;
    }

    public long work(int consumerID) {
        if (log.isTraceEnabled()) { log.trace("enter"); }

	ConsumerRecords<?, ?> records = null;
	    
        try {
	    records = kafkaConsumer.poll(kafkaParams.getWaitTO());

	    if (log.isDebugEnabled()) {
		log.debug("poll messages from topic [" + topic + "] partition [" 
			  + partitionID + "] count [" + records.count() + "]");
	    }

	    if (!checkRecords(records))
		return 0;

	    List<RowMessage> rows = new ArrayList<RowMessage>();
	    if (!ProcessRecords(records, rows, consumerID))
		return 0;
	 
	    consumeNumber += records.count();

	    // the data had dump to disk, commit the kafka
	    if (log.isDebugEnabled()) {
		log.debug("commit the topic [" + topic + "], partition [" 
			  + partitionID + "], offset [" + curOffset + "]");
	    }

	    kafkaConsumer.commitSync();

	    loaderHandle.offer(consumerID, topic, partitionID, rows);
        } catch (ConsumerTimeoutException cte) {
            log.error("consumer time out: " + cte.getMessage());
        } catch (WakeupException we) {
            log.warn("wakeup exception");
	} catch (ArrayIndexOutOfBoundsException aiooe) {
	    state = STATE_ARRAY_OVERRUN;
	    log.error("table schema is not matched with data", aiooe);
	} catch (CloneNotSupportedException cnse) {
	    state = STATE_CLONE_NOT_SUPPORT;
	    log.error("clone the RowMessage failed.", cnse);
	} catch (Exception e) {
	    state = STATE_NORMAL_EXCEPTION;
	    log.error("consume RowMessage failed.", e);
        } finally {
	    if (log.isDebugEnabled()) {
		log.debug("commit the cached messages: " + records.count());
	    }

	    if (log.isTraceEnabled()) { log.trace("exit"); }

	    return records.count();
        }
    }

    public boolean ProcessRecords(ConsumerRecords<?, ?> records,
				  List<RowMessage>      rows,
				  int                   consumerID) 
	throws CloneNotSupportedException {
	long   insMsgs       = 0;
	long   updMsgs       = 0;
	long   keyMsgs       = 0;
	long   delMsgs       = 0;

	long   insErrs       = 0;
	long   updErrs       = 0;
	long   keyErrs       = 0;
	long   delErrs       = 0;

	if (log.isTraceEnabled()) { log.trace("enter"); }

	for (ConsumerRecord<?, ?> record : records) {
	    T msg     = (T)record.value();
	    curOffset = record.offset();
	    curTime   = record.timestamp();
	    
	    if (!checkRecord(record))
		return false;

	    if (!urm.init(params, consumerID, curOffset, curTime, 
			  partitionID, topic, msg)) {
		log.error("message from Kafka initialize RowMessage error, "
			  + "we can continue to consume except you fix the issue. "
			  + "thread [" + consumerID + "], offset [" + curOffset
			  + "], timestamp [" + curTime + "], partition ["
			  + "], topic [" + topic + "], message [" 
			  + urm.getMsgString() + "]");
		state = STATE_INIT_FAIL;
		return false;
	    }

	    RowMessage row = (RowMessage)urm.clone();
	    if (row.AnalyzeMessage()) {
		switch(row.getOperatorType()) {
		case "I":
		    insMsgs++;
		    break;
	    
		case "U":
		    updMsgs++;
		    break;

		case "K":
		    keyMsgs++;
		    break;

		case "D":
		    delMsgs++;
		    break;

		default:
		    log.error("the row message type [" + row.getOperatorType() 
			      + "] error");
		    return false;
		}
	    } else if (params.getKafkaCDC().isSkip()) {
		switch(row.getOperatorType()) {
		case "I":
		    insErrs++;
		    break;
	    
		case "U":
		    updErrs++;
		    break;

		case "K":
		    keyErrs++;
		    break;

		case "D":
		    delErrs++;
		    break;

		default:
		    log.error("the row message type [" + row.getOperatorType() 
			      + "] error");
		    return false;
		}
	    } else {
		log.error("message format error, if you want to continue, "
			  + "add skip parameter please!");
		return false;
	    }

	    rows.add(row);
	}

	if (!dumpDataToFile(rows))
	    return false;

	consumeStates.addInsMsgNum(insMsgs);
	consumeStates.addUpdMsgNum(updMsgs);
	consumeStates.addKeyMsgNum(keyMsgs);
	consumeStates.addDelMsgNum(delMsgs);

	consumeStates.addInsErrNum(insErrs);
	consumeStates.addUpdErrNum(updErrs);
	consumeStates.addKeyErrNum(keyErrs);
	consumeStates.addDelErrNum(delErrs);

	consumeStates.addKafkaMsgNum(records.count());
	consumeStates.addKafkaErrNum(insErrs + updErrs + keyErrs + delErrs);

	if (log.isTraceEnabled()) { log.trace("exit"); }

	return true;
    }

    public boolean dumpDataToFile(List<RowMessage> rows) {
	if (log.isTraceEnabled()) { log.trace("enter"); }

	String rootPath = params.getKafkaCDC().getKafkaDir();
	if (rootPath != null) {
	    String fileName = topic + "_" + partitionID + "_" + curOffset;
	    String filePath = rootPath + fileName;
	    if (params.getKafkaCDC().isDumpBinary() &&
		!FileUtils.dumpDataToFile(rows, filePath, FileUtils.BYTE_STRING)) {
		state = STATE_DUMP_DATA_FAIL;
		log.error("dump banirt data to file fail.");
		return false;
	    }

	    filePath = rootPath + fileName + ".sql";
	    if (!FileUtils.dumpDataToFile(rows, filePath, FileUtils.SQL_STRING)) {
		state = STATE_DUMP_DATA_FAIL;
		log.error("dump sql data to file fail.");
		return false;
	    }
	}

	if (log.isTraceEnabled()) { log.trace("exit"); }

	return true;
    }

    public boolean checkRecords(ConsumerRecords<?, ?> records) {
	if (log.isTraceEnabled()) { log.trace("enter"); }

	if (records.isEmpty()) {
	    log.warn("there are no data in the topic [" + topic 
		     + "], partition [" + partitionID + "]"); 
	    return false;
	}

	// there are some exception about kafka data, need to fix it by manual.
	if (state > 0) {
	    log.error("the data of topic [" + topic + "], partition [" 
		      + partitionID + "] had some issues need to fix, "
		      + "the state [" + state + "]");
	    return false;
	}

	if (log.isTraceEnabled()) { log.trace("exit"); }

	return true;
    }

    public boolean checkRecord(ConsumerRecord<?, ?> record) {
	if (log.isTraceEnabled()) { log.trace("enter"); }

	if (partitionID != record.partition()) {
	    log.error("topic [" + topic + "] partition [" 
		      + partitionID + "] recv the error partition ["
		      + record.partition() 
		      + "], check the kafka server please!");
	    state = STATE_PARTITION_ERROR;
	    return false;
	}

	if (!topic.equals(record.topic())) {
	    log.error("topic [" + topic + "] partition [" 
		      + partitionID + "] recv the error topic ["
		      + record.topic() 
		      + "], check the kafka server please!");
	    state = STATE_TOPIC_ERROR;
	    return false;
	}

	if (log.isTraceEnabled()) { log.trace("exit"); }

	return true;
    }

    public void seekToTime(KafkaConsumer<?, ?> kafkaConsumer,
			   TopicPartition partition) {
        long offset=0;
        long beginOffset=0;
        boolean offsetGreaterThanEnd=false;

        //get offset by start time
        String dateFormat = Utils.whichDateFormat(kafkaParams.getMode());
        long startDateStamp 
	    = Utils.dateToStamp(kafkaParams.getMode(), dateFormat);

        Map<TopicPartition, Long> startMap = new HashMap<>();
        startMap.put(partition, startDateStamp);
        Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes 
	    = kafkaConsumer.offsetsForTimes(startMap);
        OffsetAndTimestamp offsetAndTimestamp = offsetsForTimes.get(partition);
        //if offsetAndTimestamp is null means offsetGreaterThanEnd
        if (offsetAndTimestamp != null) {
            offset = offsetAndTimestamp.offset();
        }else {
            offsetGreaterThanEnd=true;
        }
        //get begin offset
        List<TopicPartition> partitions = new ArrayList<>();
        partitions.add(partition);
        Map<TopicPartition, Long> beginningOffsets 
	    = kafkaConsumer.beginningOffsets(partitions);
        if (beginningOffsets != null) {
            beginOffset = beginningOffsets.get(partition);
        }

        if (offset < beginOffset) {
            log.info("poll the data from beginning: " + beginOffset);
            kafkaConsumer.seekToBeginning(partitions);;
        }else if (offsetGreaterThanEnd) {
            log.info("poll the data from latest");
            kafkaConsumer.seekToEnd(partitions);
        }else {
            log.info("poll the data from offset: " + offset);
            kafkaConsumer.seek(partition, offset);
        }
    }

    public void show(StringBuffer strBuffer) {
	String consumerTaskStr =
	    String.format("  -> msg task [loader:%3d, topic:%16s, partition:%3d"
			  + ", group:%16s, number:%12d, time:%15d, offset:%12d]\n",
			  loaderHandle.getLoaderID(), topic, partitionID, group, 
			  consumeNumber, curTime, curOffset);

	strBuffer.append(consumerTaskStr);
    }

    public void close() {
        if (log.isTraceEnabled()) { log.trace("enter"); }

        kafkaConsumer.close();
        kafkaConsumer.wakeup();
	log.warn("Topic [" + topic + "] partition [" + partitionID 
		 + "] is closed.");

        if (log.isTraceEnabled()) { log.trace("exit"); }
    }
}
