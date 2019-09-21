package com.esgyn.kafkaCDC.server.databaseLoader;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;

import com.esgyn.kafkaCDC.server.kafkaConsumer.messageType.RowMessage;
import com.esgyn.kafkaCDC.server.utils.Parameters;

import lombok.Getter;

public class LoaderHandle {
    private ConcurrentLinkedQueue<LoaderTask> tasks   = null;
    private LoadStates                 loadStates     = null;

    @Getter
    private Parameters                 params         = null;
    @Getter
    private LoaderTasks                loaderTasks    = null;
    @Getter
    private int                        loaderID       = -1;

    private static Logger log = Logger.getLogger(LoaderHandle.class);

    public LoaderHandle(LoaderTasks loaderTasks_, int loaderID_) {
        if (log.isTraceEnabled()) { log.trace("enter"); }

	loaderTasks = loaderTasks_;
	loaderID    = loaderID_;
	loadStates  = loaderTasks.getLoadStates();
	params      = loadStates.getParams();
	tasks       = new ConcurrentLinkedQueue<LoaderTask>();

        if (log.isTraceEnabled()) { log.trace("exit"); }
    }

    // called by database loader
    public LoaderTask poll() { 
	if (log.isTraceEnabled()) { log.trace("enter"); }

	LoaderTask task = tasks.poll(); 

	if (log.isTraceEnabled()) { log.trace("exit"); }
	
	return task;
    }

    // called by kafka consumers
    public void offer(int consumerID_, String topic_, int partitionID_, List<RowMessage> rows) { 
	if (log.isTraceEnabled()) { log.trace("enter"); }
	
	LoaderTask loaderTask = new LoaderTask(loadStates, topic_, partitionID_, consumerID_, rows);
	if (log.isDebugEnabled()) { 
	    log.debug("generate the database load task [" + loaderTask + "]"); 
	}

	tasks.offer(loaderTask);
	loadStates.addTotalTasks(1);
	
	if (log.isTraceEnabled()) { log.trace("exit"); }
    }
}
