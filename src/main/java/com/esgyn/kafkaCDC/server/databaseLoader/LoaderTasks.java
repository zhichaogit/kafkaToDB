package com.esgyn.kafkaCDC.server.databaseLoader;

import org.apache.log4j.Logger;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;

import java.lang.StringBuffer;

import java.util.Map;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.esgyn.kafkaCDC.server.utils.TableInfo;
import com.esgyn.kafkaCDC.server.utils.Parameters;

import com.esgyn.kafkaCDC.server.databaseLoader.LoadStates;
import com.esgyn.kafkaCDC.server.databaseLoader.LoaderTask;
import com.esgyn.kafkaCDC.server.databaseLoader.LoaderHandle;
import com.esgyn.kafkaCDC.server.databaseLoader.LoaderThread;

import lombok.Getter;
import lombok.Setter;

public class LoaderTasks<T> {
    private static Logger log = Logger.getLogger(LoaderTasks.class);
    private List<LoaderThread>          loaders       = null;

    @Getter
    private long                        running       = 0;
    @Getter
    private Parameters                  params        = null;
    @Getter
    private LoadStates                  loadStates    = null;
    @Getter
    private List<LoaderHandle>          loaderHandles = null;

    public LoaderTasks(Parameters params_) {
        if (log.isTraceEnabled()) { log.trace("enter"); }

	params      = params_;

        if (log.isTraceEnabled()) { log.trace("exit"); }
    }

    public boolean init() {
        if (log.isTraceEnabled()) { log.trace("enter"); }

	loadStates = new LoadStates(params);

	loaders = new ArrayList<LoaderThread>(0);
        loaderHandles = new ArrayList<LoaderHandle>(0);
        // start database load theads
	running = params.getKafkaCDC().getLoaders();
        for (int i = 0; i < running; i++) {
	    LoaderHandle loaderHandle = new LoaderHandle(this, i);
	    LoaderThread loader = new LoaderThread(loaderHandle);
            loader.setName("LoaderThread-" + i);
	    loaderHandles.add(loaderHandle);
            loaders.add(loader);
            loader.start();
        }

	if (log.isTraceEnabled()) { log.trace("exit"); }

	return true;
    }

    public void show(StringBuffer strBuffer) {
	if (params.getKafkaCDC().isShowLoaders()) {
	    strBuffer.append("  The detail of loader threads:\n");
	    for (LoaderThread loader : loaders) {
		loader.show(strBuffer);
	    }
	}
	loadStates.show(strBuffer);
    }

    public synchronized void decrease() { running--; }

    public void close() {
        if (log.isTraceEnabled()) { log.trace("enter"); }

	// close the load threads
	log.info("start to stop the all loader threads ...");
        for (LoaderThread loader : loaders) {
            try {
		if (loader.getLooping()) {
		    log.info("waiting for [" + loader.getName() + "] close ...");
		    loader.Close();
		    // don't need to join, same as ConsumerTasks
		    // loader.join();
		    log.info("loader [" + loader.getName() + "] stop succeed!");
		}
            } catch (Exception e) {
                log.error("load thread join exception", e);
            }
        }

        if (log.isTraceEnabled()) { log.trace("exit"); }
    }
}