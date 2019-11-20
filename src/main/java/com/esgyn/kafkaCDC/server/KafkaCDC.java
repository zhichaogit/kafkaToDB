package com.esgyn.kafkaCDC.server;

import org.apache.log4j.Logger;

import com.esgyn.kafkaCDC.server.kafkaConsumer.ConsumerTasks;
import com.esgyn.kafkaCDC.server.utils.Parameters;
import com.esgyn.kafkaCDC.server.utils.Constants;
import com.esgyn.kafkaCDC.server.utils.Utils;

public class KafkaCDC {
    private static Logger log = Logger.getLogger(KafkaCDC.class);

    public static void main(String[] args) {
	Parameters params = new Parameters(args);
	params.init();

	ConsumerTasks consumerTasks = new ConsumerTasks(params);
	if (!consumerTasks.init()) {
	    log.error("consumer task init error");
	    return;
	}

	//add shutdownhook
	shutdownHook(consumerTasks);

	// show statistics
	showStatistics(consumerTasks);

        //wait all stop
        waitLoaderStop(consumerTasks, Constants.KAFKA_CDC_NORMAL);

        log.info("exit time: " + Utils.getCurrentTime());
    }

    public static void show(ConsumerTasks consumerTasks) {
	StringBuffer strBuffer = new StringBuffer();
	consumerTasks.show(strBuffer);
	log.info(strBuffer.toString());
    }

    public static void showStatistics(ConsumerTasks consumerTasks) {
        while (consumerTasks.getLoaderTasks().getRunning() > 0) {
	    Utils.waitMillisecond(consumerTasks.getParams().getKafkaCDC().getInterval());

	    show(consumerTasks);
        }
    }

    public static void waitLoaderStop(ConsumerTasks consumerTasks, int signal_) {
	log.info("stop all process threads ...");
	consumerTasks.close(signal_);

	// show the latest statistics
	log.info("all process threads stoped" );
	show(consumerTasks);
    }

    public static void shutdownHook(ConsumerTasks consumerTasks) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                // show help or version information
                setName("CtrlCThread");
                log.warn("exiting via Ctrl+C, show the lastest states:");
		//wait all stop
		waitLoaderStop(consumerTasks, Constants.KAFKA_CDC_NORMAL);
            }
        });
    }
}
