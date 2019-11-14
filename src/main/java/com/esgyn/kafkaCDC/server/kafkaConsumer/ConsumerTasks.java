package com.esgyn.kafkaCDC.server.kafkaConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;

import com.esgyn.kafkaCDC.server.databaseLoader.LoaderHandle;
import com.esgyn.kafkaCDC.server.databaseLoader.LoaderTasks;
import com.esgyn.kafkaCDC.server.utils.Constants;
import com.esgyn.kafkaCDC.server.utils.Parameters;
import com.esgyn.kafkaCDC.server.utils.TopicParams;
import com.esgyn.kafkaCDC.server.utils.Utils;

import lombok.Getter;

public class ConsumerTasks<T> {
    private static Logger log = Logger.getLogger(ConsumerTasks.class);
    private ConcurrentLinkedQueue<ConsumerTask> tasks = null;
    private List<ConsumerTask>          taskArray     = null;
    private List<ConsumerThread>        consumers     = null;

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

	if (!initTopics(topics))
	    return false;

	if (!initConsumers())
	    return false;

	if (log.isTraceEnabled()) { log.trace("exit"); }

	return true;
    }

    private boolean initTopics(List<TopicParams> topics){
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
		    new ConsumerTask(consumeStates, topicName, desSchema, topic.getGroup(),
				     partitionID, loaderHandle);
		if (!consumerTask.init())
		    return false;

		tasks.offer(consumerTask);
		taskArray.add(consumerTask);
	    }
	}

	if (log.isTraceEnabled()) { log.trace("exit"); }

	return true;
    }

    private boolean initConsumers(){
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

    public void show(StringBuffer strBuffer) { 
	consumeStates.show(strBuffer);

	if (params.getKafkaCDC().isShowConsumers()) {
	    strBuffer.append("  The detail of consumer threads:\n");
	    for (ConsumerThread consumer : consumers) {
		consumer.show(strBuffer);
	    }
	}

	if (params.getKafkaCDC().isShowTasks()) {
	    StringBuffer tempBuffer = new StringBuffer();
	    long oldestTime = -1;
	    long newestTime = -1;

	    for (ConsumerTask task : taskArray) {
		if (oldestTime == -1)
		    task.getCurTime();
		else if (oldestTime > task.getCurTime())
		    oldestTime = task.getCurTime();

		if (newestTime == -1)
		    task.getCurTime();
		else if (newestTime < task.getCurTime())
		    newestTime = task.getCurTime();
		
		task.show(tempBuffer);
	    }

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
 
	loaderTasks.close(signal_);

        if (log.isTraceEnabled()) { log.trace("exit"); }
   }
}
