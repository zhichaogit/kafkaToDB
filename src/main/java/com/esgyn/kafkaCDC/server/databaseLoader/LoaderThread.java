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
    private long                preLoadTime    = 0;
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

	// exit when finished the tasks
	while (true) {
	    // remove the task from the queue
	    loaderTask = loaderHandle.poll();
	    if (loaderTask != null){
		preLoadTime = Utils.getTime();
		while (loaderTask != null) {
		    try {
			if (dbConn == null) {
			    dbConn = Database.CreateConnection(loaderHandle.getParams().getDatabase());
			}

			if (dbConn != null) {
			    long loadNumber = loaderTask.work(loaderHandle.getLoaderID(), 
							      dbConn, tables, state);
			    if (loadNumber < 0) {
				log.error("loader thread load data to database fail! "
					  + "fix the database error as soon as possable please, "
					  + "loader thread will wait 1000ms and continue");
				loaderTask.getLoadStates().addTransFails(1);
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
			    Utils.waitMillisecond(sleepTime);
			}
		    } catch (SQLException se) {
			log.error("loader thread throw exception when execute work:", se);
			try {
			    loaderTask.getLoadStates().addTransFails(1);
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
			Utils.waitMillisecond(sleepTime);
		    }
		}
	    } else if (getLoaderState() == Constants.KAFKA_CDC_RUNNING) {
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

		Utils.waitMillisecond(sleepTime);
	    } else {
		log.info("loader thread stoped via close.");
		break;
	    }
	} // while true

	if (dbConn != null) {
	    Database.CloseConnection(dbConn);
	    dbConn = null;
	}

	loaderTasks.decrease();
	looping = false;

        if (log.isTraceEnabled()) { log.trace("exit");}
    }

    public void show(StringBuffer strBuffer) {
	Long  freeTime = Utils.getTime() - preLoadTime;
	String loaderThreadStr =
	    String.format("  -> loader   [id:%3d, loaded:%12d, free:%12dms, looping:%s, state:%s]\n", 
			  loaderHandle.getLoaderID(), loadedNumber, freeTime,
			  String.valueOf(looping), Constants.getState(getLoaderState()));

	strBuffer.append(loaderThreadStr);
    }

    public synchronized int getLoaderState() { return state; }
    public synchronized void stopLoader(int signal_) {
        if (log.isTraceEnabled()) { log.trace("enter");}

	log.info("close the loader thread [" + loaderHandle.getLoaderID() +  "].");
	state = signal_; 

        if (log.isTraceEnabled()) { log.trace("exit");}
    }
}
