package com.esgyn.kafkaCDC.server.databaseLoader;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.esgyn.kafkaCDC.server.utils.Parameters;
import com.esgyn.kafkaCDC.server.utils.Constants;

import lombok.Getter;

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

    public void showLoaders(StringBuffer strBuffer, int format) {
	boolean first = true;

	strBuffer.append(Constants.getFormatStart(format));
	for (LoaderThread loader : loaders) {
	    if (!first)
		strBuffer.append(Constants.getFormatEntry(format));
	    first = false;
	    loader.show(strBuffer, format);
	}
	strBuffer.append(Constants.getFormatEnd(format));
    }

    public void show(StringBuffer strBuffer) {
	if (params.getKafkaCDC().isShowLoaders()) {
	    strBuffer.append("  The detail of loader threads:\n");
	    showLoaders(strBuffer, Constants.KAFKA_STRING_FORMAT);
	}
	loadStates.show(strBuffer);
    }

    public synchronized void decrease() { running--; }

    public void close(int signal_) {
        if (log.isTraceEnabled()) { log.trace("enter"); }

	// close the load threads
	log.info("start to stop the all loader threads ...");
        for (LoaderThread loader : loaders) {
            try {
		if (loader.getLoaderState() < signal_) {
		    log.info("waiting for [" + loader.getName() + "] close ...");
		    loader.stopLoader(signal_);
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
