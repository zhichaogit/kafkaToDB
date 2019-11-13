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
    private long                preConsumeTime = 0;

    private boolean             looping        = true;
    @Getter
    private final AtomicBoolean running = new AtomicBoolean(true);

    private static Logger log = Logger.getLogger(ConsumerThread.class);

    public ConsumerThread(ConsumerTasks consumerTasks_, int consumerID_) {
        if (log.isTraceEnabled()) { log.trace("enter"); }

	consumerTasks  = consumerTasks_;

	consumerID     = consumerID_;
        consumedNumber = 0;
	preConsumeTime = Utils.getTime();

        if (log.isTraceEnabled()) { log.trace("exit"); }
    }

    public void run() {
        if (log.isTraceEnabled()) { log.trace("enter"); }

	log.info("consumer server started.");
	ConsumerTask consumerTask = null;
	while (getRunning()) {
	    // remove the task from the queue
	    consumerTask = consumerTasks.poll();
	    if (consumerTask != null){
		// poll data from kafka
		long pollNumber = consumerTask.work(consumerID);
		if (log.isDebugEnabled()) { 
		    log.debug("ConsumeThread pulled " + pollNumber + " msgs");
		}

		if (pollNumber > 0) {
		    preConsumeTime = Utils.getTime();
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
		Utils.waitMillisecond(consumerTasks.getSleepTime());
	    }
	} // while true

	consumerTasks.decrease();
	looping = false;

	log.info("consumer thread stoped.");

        if (log.isTraceEnabled()) { log.trace("exit");}
    }

    public boolean checkTimeOut() { 
	Long  freeTime = Utils.getTime() - preConsumeTime;

	if ((consumerTasks.getMaxFreeTime() >=0) && (freeTime > consumerTasks.getMaxFreeTime())) {
	    log.info("ConsumeThread free time [" + freeTime/1000 
		     + "s] had more than the max free time [" 
		     + consumerTasks.getMaxFreeTime()/1000 + "s]");
	    stopConsumer();
	    return true;
	}

	return false;
    }

    public void show(StringBuffer strBuffer) { 
	Long  freeTime = Utils.getTime() - preConsumeTime;
	String consumerThreadStr =
	    String.format("  -> consumer [id:%3d, msgs:%12d, free:%12dms, max free: %8dms, looping:%s, running:%s]\n",
			  consumerID, consumedNumber, freeTime, consumerTasks.getMaxFreeTime(),
			  String.valueOf(looping), String.valueOf(getRunning()));

	strBuffer.append(consumerThreadStr);
    }

    public synchronized boolean getRunning() { return running.get(); }
    public synchronized void stopConsumer() { running.set(false); }
}
