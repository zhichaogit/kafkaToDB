package com.esgyn.kafkaCDC.server.databaseLoader;

import org.apache.log4j.Logger;

import java.util.Map;
import java.util.HashMap;

import java.sql.Connection;
import java.sql.SQLException;

import com.esgyn.kafkaCDC.server.database.Database;
import com.esgyn.kafkaCDC.server.database.TableState;

import com.esgyn.kafkaCDC.server.databaseLoader.LoaderTask;
import com.esgyn.kafkaCDC.server.databaseLoader.LoaderTasks;
import com.esgyn.kafkaCDC.server.databaseLoader.LoaderHandle;

import java.util.concurrent.atomic.AtomicBoolean;

import lombok.Getter;

public class LoaderThread extends Thread {
    @Getter
    private long                loadedNumber   = 0;

    private LoaderHandle        loaderHandle   = null;
    private LoaderTasks         loaderTasks    = null;
    private Database            database       = null;
    private Map<String, TableState> tables     = null;

    private Connection          dbConn         = null;
    private boolean             running        = true;

    private final AtomicBoolean looping = new AtomicBoolean(true);

    private static Logger log = Logger.getLogger(LoaderThread.class);

    public LoaderThread(LoaderHandle loaderHandle_) {
        if (log.isTraceEnabled()) { log.trace("enter"); }

	loaderHandle   = loaderHandle_;

	loaderTasks    = loaderHandle.getLoaderTasks();
	database       = loaderTasks.getDatabase();
        loadedNumber   = 0;

	tables         = new HashMap<String, TableState>(0);

        if (log.isTraceEnabled()) { log.trace("exit"); }
    }

    public void run() {
        if (log.isTraceEnabled()) { log.trace("enter"); }

	log.info("loader thread started.");
	LoaderTask loaderTask = null;

	// exit when finished the tasks
	while (true) {
	    // remove the task from the queue
	    loaderTask = loaderHandle.poll();
	    if (loaderTask != null){
		try {
		    if (dbConn == null) {
			dbConn = database.CreateConnection(false);
		    }

		    long loadNumber = loaderTask.work(loaderHandle.getLoaderID(), dbConn, tables);
		    // update the statistics

		    // TODO add statistics
		    // loaderTasks.incTotalLoaded(loadNumber);
		    // TODO try catch the SQLException and try again.
		    loadedNumber += loadNumber;
		} catch (SQLException e) {
		    // TODO handle retry
		} finally {
		    // reset the task null
		    loaderTask = null;
		}
	    } else if (looping.get()) {
		// there are no work to do, go to sleep a while
		if (log.isDebugEnabled()) {
		    log.info("loader thread haven't task to do, loader goto sleep 1s");
		}
		try {
		    Thread.sleep(1000);
		} catch (Exception e) {
		    log.error("throw exception when call Thread.sleep");
		}
	    } else {
		log.info("loader thread stoped via close.");
		break;
	    }
	} // while true

	if (dbConn != null) {
	    database.CloseConnection(dbConn);
	    dbConn = null;
	}
	loaderTasks.decrease();

        if (log.isTraceEnabled()) { log.trace("exit");}
    }

    public void show(StringBuffer strBuffer) {
	String loaderThreadStr =
	    String.format("  -> loader   [id:%3d, loaded:%12d, state:%5b]\n", 
			  loaderHandle.getLoaderID(), loadedNumber, running);

	strBuffer.append(loaderThreadStr);
    }

    public synchronized boolean getLooping() { return looping.get(); }
    public synchronized void Close() {
        if (log.isTraceEnabled()) { log.trace("enter");}

	log.info("close the loader thread [" + loaderHandle.getLoaderID() +  "].");
	looping.set(false); 

        if (log.isTraceEnabled()) { log.trace("exit");}
    }
}
