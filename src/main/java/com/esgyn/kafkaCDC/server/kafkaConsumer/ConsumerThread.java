package com.esgyn.kafkaCDC.server.kafkaConsumer;

import org.apache.log4j.Logger;

import com.esgyn.kafkaCDC.server.utils.Utils;

import java.util.concurrent.atomic.AtomicBoolean;

import lombok.Getter;

public class ConsumerThread extends Thread {
    private ConsumerTasks       consumerTasks  = null;

    @Getter
    private int                 consumerID     = -1;
    @Getter
    private long                consumedNumber = 0;
    @Getter
    private long                preConsumeTime = 0;

    private boolean             running        = true;
    @Getter
    private final AtomicBoolean looping = new AtomicBoolean(true);

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
	while (looping.get()) {
	    // remove the task from the queue
	    consumerTask = consumerTasks.poll();
	    if (consumerTask != null){
		// poll data from kafka
		long pollNumber = consumerTask.work(consumerID);
		if (pollNumber > 0) {
		    preConsumeTime = Utils.getTime();
		    // update the consumer tasks statistics
		    consumedNumber += pollNumber;
		    // return the task to the queue
		    consumerTasks.offer(consumerTask);
		} else {
		    // check the timeout and set looping false to exit loop
		    checkTimeOut();
		}
		// reset the task null
		consumerTask = null;
	    } else {
		// there are no work to do, go to sleep a while
		if (log.isDebugEnabled()) { 
		    log.debug("ConsumeThread haven't tasks to do, consumer goto sleep 1s");
		}
		try {
		    checkTimeOut();
		    Thread.sleep(1000);
		} catch (Exception e) {
		    log.error("throw exception when call Thread.sleep");
		}
	    }
	} // while true

	consumerTasks.decrease();
	running = false;

	log.info("consumer thread stoped.");

        if (log.isTraceEnabled()) { log.trace("exit");}
    }

    public void checkTimeOut() { 
	Long  freeTime = Utils.getTime() - preConsumeTime;

	if (freeTime > consumerTasks.getMaxFreeTime()) {
	    log.info("ConsumeThread free time [" + freeTime/1000 
		     + "s] had more than the max free time [" 
		     + consumerTasks.getMaxFreeTime()/1000 + "s]");
	    looping.set(false);
	}
    }

    public void show(StringBuffer strBuffer) { 
	Long  freeTime = Utils.getTime() - preConsumeTime;
	String consumerThreadStr =
	    String.format("  -> consumer [id:%3d, msgs:%12d, free:%8ds"
			  + ", state:%s]\n", consumerID, consumedNumber, 
			  freeTime/1000, running ? "running" : "stoped");

	strBuffer.append(consumerThreadStr);
    }

    public synchronized boolean getLooping() { return looping.get(); }
    public synchronized void Close() { looping.set(false); }
}
