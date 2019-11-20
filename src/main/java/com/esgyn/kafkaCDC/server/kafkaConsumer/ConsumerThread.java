package com.esgyn.kafkaCDC.server.kafkaConsumer;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import com.esgyn.kafkaCDC.server.utils.Utils;
import com.esgyn.kafkaCDC.server.utils.Constants;

import lombok.Getter;

public class ConsumerThread extends Thread {
    private ConsumerTasks       consumerTasks  = null;

    @Getter
    private int                 consumerID     = -1;
    @Getter
    private long                consumedNumber = 0;
    @Getter
    private long                waitTime       = 0;
    @Getter
    private long                preLoaderTime  = 0;
    private long                startTime      = 0;

    private boolean             looping        = true;
    @Getter
    private final AtomicBoolean running = new AtomicBoolean(true);

    private static Logger log = Logger.getLogger(ConsumerThread.class);

    public ConsumerThread(ConsumerTasks consumerTasks_, int consumerID_) {
        if (log.isTraceEnabled()) { log.trace("enter"); }

	consumerTasks  = consumerTasks_;

	consumerID     = consumerID_;

        if (log.isTraceEnabled()) { log.trace("exit"); }
    }

    public void run() {
        if (log.isTraceEnabled()) { log.trace("enter"); }

	log.info("consumer server started.");
	ConsumerTask consumerTask = null;
	startTime = Utils.getTime();

	while (getRunning()) {
	    // remove the task from the queue
	    preLoaderTime = Utils.getTime();
	    consumerTask = consumerTasks.poll();
	    if (consumerTask != null){
		// poll data from kafka
		long pollNumber = consumerTask.work(consumerID);
		if (log.isDebugEnabled()) { 
		    log.debug("ConsumeThread pulled " + pollNumber + " msgs");
		}

		if (pollNumber > 0) {
		    waitTime = 0;
		    // update the consumer tasks statistics
		    consumedNumber += pollNumber;
		    // return the task to the queue
		    consumerTasks.offer(consumerTask);
		} else {
		    // check the timeout and set running false to exit loop
		    if (!checkTimeOut()) {
			// if not timeout, need to return the task to the queue
			consumerTasks.offer(consumerTask);
		    }
		    waitTime += consumerTasks.getSleepTime() + consumerTasks.getWaitTO();
		    Utils.waitMillisecond(consumerTasks.getSleepTime());
		}
		// reset the task null
		consumerTask = null;
	    } else {
		// there are no work to do, go to sleep a while
		if (log.isDebugEnabled()) { 
		    log.debug("ConsumeThread haven't tasks to do, consumer goto sleep "
			      + consumerTasks.getSleepTime() + "ms");
		}
		checkTimeOut();
		waitTime += consumerTasks.getSleepTime();
		Utils.waitMillisecond(consumerTasks.getSleepTime());
	    }
	} // while true

	consumerTasks.decrease();
	looping = false;

	log.info("consumer thread stoped.");

        if (log.isTraceEnabled()) { log.trace("exit");}
    }

    public boolean checkTimeOut() { 
	if ((consumerTasks.getMaxWaitTime() >=0) && (waitTime > consumerTasks.getMaxWaitTime())) {
	    log.warn("ConsumeThread free time [" + waitTime
		     + "ms] had more than the max free time [" 
		     + consumerTasks.getMaxWaitTime() + "ms]");
	    stopConsumer();
	    return true;
	}

	return false;
    }

    public void show(StringBuffer strBuffer, int format) { 
	Long  waitLoaderTime = Utils.getTime() - preLoaderTime;
	Long  consumeTime    = Utils.getTime() - startTime;
	Long  maxWaitTime    = consumerTasks.getMaxWaitTime();
	maxWaitTime          = maxWaitTime < 0 ? -1 : maxWaitTime;
	if (consumeTime <= 0)
	    consumeTime = (long)1;

	switch(format){
	case Constants.KAFKA_STRING_FORMAT:
	    String consumerThreadStr = 
		String.format("  -> Consumer {ID:%3d, Msgs:%12d, Speed: %6d, Wait:%12dms,"
			      + " Wait Loader: %12dms, Max Free: %8dms, Looping:%5s, Running:%5s}",
			      consumerID, consumedNumber, consumedNumber/consumeTime, 
			      waitTime, waitLoaderTime, maxWaitTime,
			      String.valueOf(looping), String.valueOf(getRunning()));
	    strBuffer.append(consumerThreadStr);
	    break;
	    
	case Constants.KAFKA_JSON_FORMAT:
	    strBuffer.append("{\"Consumer ID\": " + consumerID)
		.append(", \"Msgs\": " + consumedNumber)
		.append(", \"Speed(n/s)\": " + consumedNumber/consumeTime)
		.append(", \"Wait\": \"" + waitTime + "ms\"")
		.append(", \"Wait Loader\": \"" + waitLoaderTime + "ms\"")
		.append(", \"Max Free\": \"" + consumerTasks.getMaxWaitTime() + "ms\"")
		.append(", \"Looping\": \"" + looping + "\"")
		.append(", \"Running\": \"" + getRunning() + "\"}");
	    break;
	}

    }

    public synchronized boolean getRunning() { return running.get(); }
    public synchronized void stopConsumer() { running.set(false); }
}
