package com.esgyn.kafkaCDC.server;

import org.apache.log4j.Logger;

import com.esgyn.kafkaCDC.server.databaseLoader.LoaderTasks;
import com.esgyn.kafkaCDC.server.kafkaConsumer.ConsumerTasks;
import com.esgyn.kafkaCDC.server.utils.CleanDataLogs;
import com.esgyn.kafkaCDC.server.utils.Parameters;
import com.esgyn.kafkaCDC.server.utils.Utils;

public class KafkaCDC {
    private static Logger log = Logger.getLogger(KafkaCDC.class);

    public static void show(ConsumerTasks consumerTasks) {
	StringBuffer strBuffer = new StringBuffer();
	LoaderTasks  loaderTasks = consumerTasks.getLoaderTasks();
	strBuffer.append("\n  KafkaCDC states:\n")
	    .append("  There are [" + consumerTasks.getRunning())
	    .append("] consumers and [" + loaderTasks.getRunning())
	    .append("] loaders running, ");
	consumerTasks.show(strBuffer);
	log.info(strBuffer.toString());
    }

    public static void wait_loader_stop(ConsumerTasks consumerTasks) {
	log.info("stop all process threads ...");
	consumerTasks.close();

	log.info("consumers exited, waiting for loader finish the tasks");
	while (consumerTasks.getLoaderTasks().getRunning() > 0) {
	    try {
		Thread.sleep(consumerTasks.getParams().getKafkaCDC().getInterval());

		show(consumerTasks);
	    } catch (Exception e) {
		log.error("show statistics throw Exception:", e);
		break;
	    }
	}

	// show the latest statistics
	show(consumerTasks);
    }

    public static void main(String[] args) {
	Parameters params = new Parameters(args);
	params.init();

	ConsumerTasks consumerTasks = new ConsumerTasks(params);
	
	if (!consumerTasks.init()) {
	    log.error("consumer task init error");
	    return;
	}
	//cleanLogs
	CleanDataLogs  cleanDataLogs= new CleanDataLogs(params);
	cleanDataLogs.setName("cleanDataLogs");
        cleanDataLogs.start();

	Runtime.getRuntime().addShutdownHook(new Thread() {
		public void run() {
		    // show help or version information
		    setName("CtrlCThread");
		    log.warn("exiting via Ctrl+C, show the lastest states:");
		    wait_loader_stop(consumerTasks);
 
                    log.warn("stop cleanDataLogs process thread.");
                    cleanDataLogs.interrupt();
		}
	    });

	// show statistics
        while (consumerTasks.getRunning() > 0) {
            try {
                Thread.sleep(params.getKafkaCDC().getInterval());

		show(consumerTasks);
            } catch (Exception e) {
                log.error("show statistics throw Exception:", e);
                break;
            }
        }

	wait_loader_stop(consumerTasks);
        log.warn("stop cleanDataLogs process thread.");
        cleanDataLogs.interrupt();

        log.info("exit time: " + Utils.getCurrentTime());
    }
}
