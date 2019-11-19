package com.esgyn.kafkaCDC.server.kafkaConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;

import com.esgyn.kafkaCDC.server.databaseLoader.LoaderHandle;
import com.esgyn.kafkaCDC.server.databaseLoader.LoaderTasks;
import com.esgyn.kafkaCDC.server.logCleaner.LogCleaner;
import com.esgyn.kafkaCDC.server.clientServer.KCServer;
import com.esgyn.kafkaCDC.server.utils.Constants;
import com.esgyn.kafkaCDC.server.utils.Parameters;
import com.esgyn.kafkaCDC.server.utils.TopicParams;
import com.esgyn.kafkaCDC.server.utils.Utils;

import lombok.Getter;

public class ConsumerTasks<T> {
    private static Logger log = Logger.getLogger(ConsumerTasks.class);
    private ConcurrentLinkedQueue<ConsumerTask> tasks = null;
    private List<ConsumerTask>          taskArray     = null;
    @Getter
    private List<ConsumerThread>        consumers     = null;
    @Getter
    private LogCleaner                  logCleaner    = null;
    @Getter
    private KCServer                    kcServer      = null;

    @Getter
    private Parameters                  params        = null;
    @Getter
    private LoaderTasks                 loaderTasks   = null;
    @Getter
    private ConsumeStates               consumeStates = null;
    @Getter
    private volatile long               running       = 0;
    @Getter
    private long  maxWaitTime = Constants.DEFAULT_STREAM_TO_S * 1000;
    @Getter
    private long  waitTO      = Constants.DEFAULT_WAIT_TO_S * 1000;
    @Getter
    private long  sleepTime   = Constants.DEFAULT_SLEEP_TIME;

    private long  oldestTime  = -1;
    private long  newestTime  = -1;


    public ConsumerTasks(Parameters params_) {
        if (log.isTraceEnabled()) { log.trace("enter"); }

	params      = params_;
	maxWaitTime = params.getKafka().getStreamTO();
	waitTO      = params.getKafka().getWaitTO();
	sleepTime   = params.getKafkaCDC().getSleepTime();

        if (log.isTraceEnabled()) { log.trace("exit"); }
    }

    public boolean init() {
        if (log.isTraceEnabled()) { log.trace("enter"); }

	List<TopicParams> topics = params.getKafka().getTopics();
	tasks     = new ConcurrentLinkedQueue<ConsumerTask>();
        taskArray = new ArrayList<ConsumerTask>(0);

	if (log.isDebugEnabled()) {
	    log.debug("there are [" + topics.size() + "] topics"); 
	}

	loaderTasks   = new LoaderTasks(params);
	if (!loaderTasks.init())
	    return false;

	consumeStates = new ConsumeStates(this, loaderTasks.getLoadStates());

	if (!init_topics(topics))
	    return false;

	if (!init_consumers())
	    return false;

	init_log_cleaner();

	init_client_server();

	if (log.isTraceEnabled()) { log.trace("exit"); }

	return true;
    }

    private boolean init_topics(List<TopicParams> topics){
        if (log.isTraceEnabled()) { log.trace("enter"); }
	int          loaderID     = 0;
	LoaderHandle loaderHandle = null;
	List<LoaderHandle> loaderHandles = loaderTasks.getLoaderHandles();
	
	for(TopicParams topic : topics){
	    String topicName  = topic.getTopic();
	    String desSchema  = topic.getDesSchema();

	    if (log.isDebugEnabled()) {
		log.debug("there are [" + topic.getPartitions().length 
			  + "] partitions in topic [" + topicName + "]"); 
	    }

	    for (int partitionID : topic.getPartitions()) {
		if (log.isDebugEnabled()) {
		    log.debug("consumer task topic [" + topicName + "], partition ["
			      + partitionID + "]"); 
		}

		if (loaderID >= loaderHandles.size()) 
		    loaderID = 0;
		loaderHandle = loaderHandles.get(loaderID++);
		ConsumerTask consumerTask = 
		    new ConsumerTask(taskArray.size(), consumeStates, topicName, 
				     desSchema, topic.getGroup(),partitionID, 
				     loaderHandle);
		if (!consumerTask.init())
		    return false;

		tasks.offer(consumerTask);
		taskArray.add(consumerTask);
	    }
	}

	if (log.isTraceEnabled()) { log.trace("exit"); }

	return true;
    }

    private boolean init_consumers(){
	if (log.isTraceEnabled()) { log.trace("enter"); }

	running = params.getKafkaCDC().getConsumers();
        consumers = new ArrayList<ConsumerThread>(0);

        //start consumer theads
        for (int i = 0; i < running; i++) {
            // connect to kafka w/ either zook setting
	    ConsumerThread consumer = new ConsumerThread(this, i);
            consumer.setName("ConsumerThread-" + i);
            consumers.add(consumer);
            consumer.start();
        }

	if (log.isTraceEnabled()) { log.trace("exit"); }

	return true;
    }

    private void init_log_cleaner() {
        logCleaner= new LogCleaner(params);
        logCleaner.setName("LogCleaner");
        logCleaner.start();
    }

    private void init_client_server() {
	kcServer = new KCServer(this, params.getKafkaCDC().getPort());
        kcServer.setName("KafkaCDC Server");
	kcServer.start();
    }

    public ConsumerTask poll() { 
	if (log.isTraceEnabled()) { log.trace("enter"); }

	ConsumerTask task = tasks.poll(); 

	if (log.isTraceEnabled()) { log.trace("exit"); }
	
	return task;
    }

    public void offer(ConsumerTask consumerTask) { 
	if (log.isTraceEnabled()) { log.trace("enter"); }
	
	tasks.offer(consumerTask); 

	if (log.isTraceEnabled()) { log.trace("exit"); }
    }

    public void showConsumers(StringBuffer strBuffer, int format) {
	boolean first = true;

	strBuffer.append(Constants.getFormatStart("Consumers", format));
	for (ConsumerThread consumer : consumers) {
	    if (!first)
		strBuffer.append(Constants.getFormatEntry(format));
	    first = false;
	    consumer.show(strBuffer, format);
	}
	strBuffer.append(Constants.getFormatEnd(format));
    }

    public void showTasks(StringBuffer strBuffer, int format) { 
	boolean first = true;

	strBuffer.append(Constants.getFormatStart("Tasks", format));
	for (ConsumerTask task : taskArray) {
	    if (oldestTime == -1)
		task.getCurTime();
	    else if (oldestTime > task.getCurTime())
		oldestTime = task.getCurTime();

	    if (newestTime == -1)
		task.getCurTime();
	    else if (newestTime < task.getCurTime())
		newestTime = task.getCurTime();
		
	    if (!first)
		strBuffer.append(Constants.getFormatEntry(format));
	    first = false;
	    task.show(strBuffer, format);
	}
	strBuffer.append(Constants.getFormatEnd(format));
    }

    public void show(StringBuffer strBuffer) { 
	int format = Constants.KAFKA_STRING_FORMAT;

	strBuffer.append("\n  KafkaCDC states:\n")
	    .append("  There are [" + getRunning())
	    .append("] consumers and [" + loaderTasks.getRunning())
	    .append("] loaders running,");

	consumeStates.show(strBuffer);

	if (params.getKafkaCDC().isShowConsumers()) {
	    strBuffer.append("  The detail of consumer threads:\n");
	    showConsumers(strBuffer, format);
	}

	if (params.getKafkaCDC().isShowTasks()) {
	    StringBuffer tempBuffer = new StringBuffer();
	    showTasks(tempBuffer, format);
	    strBuffer.append("  The detail of consumer tasks(oldest: "
			     + Utils.stampToDateStr(oldestTime) + ", newest: "
			     + Utils.stampToDateStr(newestTime) + "):\n");
	    strBuffer.append(tempBuffer.toString());
	}

	loaderTasks.show(strBuffer);
    }

    public synchronized void decrease() { running--; }

    public void close(int signal_) {
        if (log.isTraceEnabled()) { log.trace("enter"); }

	for (ConsumerThread consumer : consumers) {
	    try {
		if (consumer.getRunning()) {
		    log.info("waiting for [" + consumer.getName() + "] stop ...");
		    consumer.stopConsumer();
		    // don't need to join, via the running number to handle
		    // consumer.join();
		    log.info(consumer.getName() + " stoped success.");
		}
	    } catch (Exception e) {
		log.error("wait " + consumer.getName() + " stoped fail!",e);
	    }
	}
 
	kcServer.stopServer();
        logCleaner.interrupt();
	loaderTasks.close(signal_);

	log.info("consumers exited, waiting for loader finish the tasks,running:"
		 + loaderTasks.getRunning());
	while (loaderTasks.getRunning() > 0) {
	    for (int i = 0; i < params.getKafkaCDC().getInterval()*100; i++) {
	        if (loaderTasks.getRunning() == 0) {
	            break;
	        }
	        Utils.waitMillisecond(10);
	    }

	    log.info("consumers exited show state");
	    StringBuffer strBuffer = new StringBuffer();
	    show(strBuffer);
	    log.info(strBuffer.toString());
	}

        if (log.isTraceEnabled()) { log.trace("exit"); }
    }
}
