package com.esgyn.kafkaCDC.server.kafkaConsumer;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import com.esgyn.kafkaCDC.server.utils.Utils;

import lombok.Getter;

public class ConsumerThread extends Thread {
    private ConsumerTasks       consumerTasks  = null;

    @Getter
    private int                 consumerID     = -1;
    @Getter
    private long                consumedNumber = 0;
    @Getter
    private long                freeTime       = 0;
    @Getter
    private long                preLoaderTime  = 0;

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
		    freeTime = 0;
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
		    freeTime += consumerTasks.getSleepTime();
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
		freeTime += consumerTasks.getSleepTime();
		Utils.waitMillisecond(consumerTasks.getSleepTime());
	    }
	} // while true

	consumerTasks.decrease();
	looping = false;

	log.info("consumer thread stoped.");

        if (log.isTraceEnabled()) { log.trace("exit");}
    }

    public boolean checkTimeOut() { 
	if ((consumerTasks.getMaxFreeTime() >=0) && (freeTime > consumerTasks.getMaxFreeTime())) {
	    log.warn("ConsumeThread free time [" + freeTime
		     + "ms] had more than the max free time [" 
		     + consumerTasks.getMaxFreeTime() + "ms]");
	    stopConsumer();
	    return true;
	}

	return false;
    }

    public void show(StringBuffer strBuffer) { 
	Long  waitTime = Utils.getTime() - preLoaderTime;
	String consumerThreadStr =
	    String.format("  -> consumer [id:%3d, msgs:%12d, free:%12dms, wait: %12dms,"
			  + " max free: %8dms, looping:%-5s, running:%-5s]\n",
			  consumerID, consumedNumber, freeTime, waitTime, 
			  consumerTasks.getMaxFreeTime(),
			  String.valueOf(looping), String.valueOf(getRunning()));

	strBuffer.append(consumerThreadStr);
    }

    public synchronized boolean getRunning() { return running.get(); }
    public synchronized void stopConsumer() { running.set(false); }
}
