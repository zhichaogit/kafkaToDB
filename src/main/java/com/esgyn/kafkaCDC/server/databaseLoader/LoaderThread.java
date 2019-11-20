package com.esgyn.kafkaCDC.server.databaseLoader;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import com.esgyn.kafkaCDC.server.database.Database;
import com.esgyn.kafkaCDC.server.database.TableState;
import com.esgyn.kafkaCDC.server.utils.Utils;
import com.esgyn.kafkaCDC.server.utils.Constants;

public class LoaderThread extends Thread {
    private long                loadedNumber   = 0;
    private long                startTime      = 0;
    private long                waitTime       = 0;
    private long                sleepTime      = 0;
    private boolean             looping        = true;

    private LoaderHandle        loaderHandle   = null;
    private LoaderTasks         loaderTasks    = null;
    private Map<String, TableState> tables     = null;

    private Connection          dbConn         = null;

    private static int    state = Constants.KAFKA_CDC_RUNNING;

    private static Logger log = Logger.getLogger(LoaderThread.class);

    public LoaderThread(LoaderHandle loaderHandle_) {
        if (log.isTraceEnabled()) { log.trace("enter"); }

	loaderHandle   = loaderHandle_;

	loaderTasks    = loaderHandle.getLoaderTasks();
	sleepTime      = loaderHandle.getParams().getKafkaCDC().getSleepTime();
        loadedNumber   = 0;

	tables         = new HashMap<String, TableState>(0);

        if (log.isTraceEnabled()) { log.trace("exit"); }
    }

    public void run() {
	int waitLoops = 0;
        if (log.isTraceEnabled()) { log.trace("enter"); }

	log.info("loader thread started.");
	LoaderTask loaderTask = null;

	startTime = Utils.getTime();

	// exit when finished the tasks
	while (loaderTask == null) {
	    // remove the task from the queue
	    loaderTask = loaderHandle.poll();
	    if (loaderTask != null){
		waitTime = 0;
		while (loaderTask != null) {
		    if (getLoaderState() == Constants.KAFKA_CDC_IMMEDIATE
			|| getLoaderState() == Constants.KAFKA_CDC_ABORT) {
			break;
		    }
		    try {
			if (dbConn == null) {
			    dbConn = Database.CreateConnection(loaderHandle.getParams().getDatabase());
			}

			if (dbConn != null) {
			    long loadNumber = loaderTask.work(loaderHandle.getLoaderID(), dbConn, 
							      tables, Constants.STATE_RUNNING);
			    if (loadNumber < 0) {
				log.error("loader thread load data to database fail! "
					  + "fix the database error as soon as possable please, "
					  + "loader thread will wait " + sleepTime + "ms and continue");
				// we try it again, it's not fail
				// loaderTask.getLoadStates().addTransFails(1);
				Utils.waitMillisecond(sleepTime);

				Database.CloseConnection(dbConn);
				dbConn = null;
				loaderTask.clean();
				continue;
			    }
				
			    loadedNumber += loadNumber;
			    // reset the task null
			    loaderTask = null;
			} else {
			    log.error("loader thread create connection fail! "
				      + "fix the database error as soon as possable please, "
				      + "loader thread will wait 1000ms and continue");
			    loaderTask.clean();
			    waitTime += sleepTime;
			    Utils.waitMillisecond(sleepTime);
			}
		    } catch (SQLException se) {
			log.error("loader thread throw exception when execute work:", se);
			try {
			    // we are try again, it's not fail
			    // loaderTask.getLoadStates().addTransFails(1);
			    dbConn.rollback();
			} catch (Exception e) {
			}
			// if the disconnect, reconnect in next loop
			Database.CloseConnection(dbConn);
			dbConn = null;

			log.error("throw unhandled exception! "
				  + "fix the database error as soon as possable please, "
				  + "loader thread will wait 1000ms and continue");
			loaderTask.clean();
			waitTime += sleepTime;
			Utils.waitMillisecond(sleepTime);
		    }
		}
	    } else if (getLoaderState() != Constants.KAFKA_CDC_RUNNING) {
		log.info("loader thread exit via state [" 
			 + Constants.getState(getLoaderState()) + "]");
		break;
	    } else {
		// there are no work to do, go to sleep a while
		if (log.isDebugEnabled()) {
		    log.debug("loader thread haven't task to do, loader goto sleep "
			      + sleepTime + "ms");
		}

		if (sleepTime * waitLoops++ > 60000) {
		    waitLoops = 0;
		    try {
			dbConn.commit();
		    } catch (Exception e) {
		    }
		}

		waitTime += sleepTime;
		Utils.waitMillisecond(sleepTime);
	    }
	} // while true

	while (loaderTask != null) {
	    switch (getLoaderState()) {
	    case Constants.KAFKA_CDC_RUNNING:
	    case Constants.KAFKA_CDC_NORMAL:
		log.error("loader cann't exit with tasks in RUNNING and NORMAL state.");
	    case Constants.KAFKA_CDC_IMMEDIATE:
		try {
		    // dump data to file
		    loaderTask.work(loaderHandle.getLoaderID(), dbConn,
				    tables, Constants.STATE_EXITING);
		} catch (Exception e) {
		    log.error("throw exception when dump data to file:", e);
		}
		loaderTask.getLoadStates().addTransFails(1);
		break;

	    case Constants.KAFKA_CDC_ABORT:
		// do nothing when abort, remove the task
		break;
	    }
	    loaderTask = loaderHandle.poll();	    
	}

	if (dbConn != null) {
	    Database.CloseConnection(dbConn);
	    dbConn = null;
	}

	loaderTasks.decrease();
	looping = false;

        if (log.isTraceEnabled()) { log.trace("exit");}
    }

    public void show(StringBuffer strBuffer, int format) {
	Long  loadedTime    = Utils.getTime() - startTime;
	if (loadedTime <= 0)
	    loadedTime = (long)1;

	switch(format){
	case Constants.KAFKA_STRING_FORMAT:
	    String loaderThreadStr = 
		String.format("  -> Loader   [ID: %3d, Loaded: %12d, Speed(n/s): %6d, Wait: %12dms, "
			      + "Looping: %5s, State: %s]", 
			      loaderHandle.getLoaderID(), loadedNumber, loadedNumber/loadedTime, 
			      waitTime, String.valueOf(looping), Constants.getState(getLoaderState()));
	    strBuffer.append(loaderThreadStr);
	    break;
	    
	case Constants.KAFKA_JSON_FORMAT:
	    strBuffer.append("{\"Loader ID\": " + loaderHandle.getLoaderID())
		.append(", \"Loaded\": " + loadedNumber)
		.append(", \"Speed(n/s)\": " + loadedNumber/loadedTime)
		.append(", \"Wait\": \"" + waitTime + "ms\"")
		.append(", \"Looping\": \"" + looping + "\"")
		.append(", \"state\": \"" + Constants.getState(getLoaderState()) + "\"}");
	    break;
	}
    }

    public synchronized int getLoaderState() { return state; }
    public synchronized void stopLoader(int signal_) {
        if (log.isTraceEnabled()) { log.trace("enter");}

	log.info("close the loader thread [" + loaderHandle.getLoaderID() +  "].");
	state = signal_; 

        if (log.isTraceEnabled()) { log.trace("exit");}
    }
}
