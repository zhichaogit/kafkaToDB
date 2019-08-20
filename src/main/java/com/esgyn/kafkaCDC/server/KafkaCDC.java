package com.esgyn.kafkaCDC.server;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;

import com.esgyn.kafkaCDC.server.utils.Constants;
import com.esgyn.kafkaCDC.server.utils.Parameters;
import com.esgyn.kafkaCDC.server.esgynDB.EsgynDB;
import com.esgyn.kafkaCDC.server.kafkaConsumer.ConsumerThread;

public class KafkaCDC implements Runnable {
    private static Logger log = Logger.getLogger(KafkaCDC.class);

    private volatile int              running     = 0;
    private EsgynDB                   esgyndb     = null;
    private Parameters                params      = null;
    private ArrayList<ConsumerThread> consumers   = null;

    public static void main(String[] args) {
        // load configure log4j.xml
        DOMConfigurator.configure(Constants.DEFAULT_LOGCONFPATH);

	KafkaCDC me  = new KafkaCDC();
        me.params    = new Parameters(args);
	me.params.init();

        me.esgyndb   = new EsgynDB(me.params.getEsgynDB());
        me.params.checkKafkaPartitions();

        me.consumers = new ArrayList<ConsumerThread>(0);

        //start consumer theads
        for (int partition : me.params.getKafkaCDC().getPartitions()) {
            // connect to kafka w/ either zook setting
	    ConsumerThread consumer = 
		new ConsumerThread(me.esgyndb, me.params.getKafka(), 
				   me.params.getKafkaCDC().isSkip(), me.params.getKafkaCDC().isBigEndian(),
				   me.params.getKafkaCDC().getDelimiter(), me.params.getKafkaCDC().getFormat(),
				   me.params.getKafkaCDC().getEncoding(), partition,
				   me.params.getKafkaCDC().getMsgClass(), me.params.getKafkaCDC().getOutPath(), 
				   me.params.getKafkaCDC().isAConn(), me.params.getKafkaCDC().isBatchUpdate());
            consumer.setName("ConsumerThread-" + partition);
            me.consumers.add(consumer);
            consumer.start();
        }

        me.running = me.params.getKafkaCDC().getPartitions().length;
        Thread ctrltrhead = new Thread(me);
        ctrltrhead.setName("CtrlCThread");

        log.info("start up CtrlCThread");
        ctrltrhead.start();

        for (ConsumerThread consumer : me.consumers) {
            try {
                log.info("waiting consumer [" + consumer.getName() + "] stop");
                consumer.join();
            } catch (Exception e) {
                log.error("consumerthread join exception",e);
            }
        }

        if (me.params.getKafkaCDC().isAConn()) {
            log.info("close connection");
            me.esgyndb.CloseConnection(me.esgyndb.getSharedConn());
        }
        log.info("all of sub threads were stoped");

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        Date endtime = new Date();
        log.info("exit time: " + sdf.format(endtime));
    }

    private class KafkaCDCCtrlCThread extends Thread {
        public KafkaCDCCtrlCThread() {
            super("CtrlCThread");
        }

        public void run() {
            // show help or version information
            if (consumers == null)
                return;

            log.warn("exiting via Ctrl+C!");

            for (ConsumerThread consumer : consumers) {
                try {
                    log.info("waiting for [" + consumer.getName() + "] stop.");
                    consumer.Close();
                    consumer.join();
                    log.info(consumer.getName() + " stoped success.");
                } catch (Exception e) {
                    log.error("wait " + consumer.getName() + " stoped fail!",e);
                }
            }

            if (esgyndb != null) {
                log.info("show the last results:");
                esgyndb.DisplayDatabase(params.getKafkaCDC().getInterval(), params.getKafkaCDC().isTableSpeed());
            } else {
                log.warn("didn't connect to database!");
            }
            running = 0;
        }
    }

    public KafkaCDC() {
        Runtime.getRuntime().addShutdownHook(new KafkaCDCCtrlCThread());
    }

    public void run() {
        log.info("keepalive thread start to run");
        boolean alreadydisconnected=false;
        while (running != 0) {
            try {
                Thread.sleep(params.getKafkaCDC().getInterval());
                if (alreadydisconnected) {
                    break;
                }
                esgyndb.DisplayDatabase(params.getKafkaCDC().getInterval(), params.getKafkaCDC().isTableSpeed());
                boolean firstAliveConsumer=true;
                for (int i = 0; i < consumers.size(); i++) {
                    ConsumerThread consumer = consumers.get(i);
                    if (!consumer.GetState()) {
                        running--;
                    }else {
                        if (params.getKafkaCDC().isAConn() && firstAliveConsumer) {
                            // single conn
                            if (params.getKafkaCDC().isKeepalive() && !consumer.KeepAliveEveryConn()) {
                                log.error("All Thread is disconnected from EsgynDB!");
                                alreadydisconnected=true;
                                break;
                            }
                            firstAliveConsumer=false;
                        }else if(!params.getKafkaCDC().isAConn()){
                            //multiple conn
                            if (params.getKafkaCDC().isKeepalive() && !consumer.KeepAliveEveryConn()) {
                                log.error(consumer.getName()+" Thread is disconnected from EsgynDB!"
                                        + "\n this thread will stop");
                                try {
                                    consumer.Close();
                                    consumer.join();
                                    consumers.remove(i);
                                    running--;
                                    log.info(consumer.getName() + " stoped success.");
                                } catch (Exception e) {
                                    log.error("wait " + consumer.getName() + " stoped fail!",e);
                                }
                            }
                        }
                    }
                }
            } catch (InterruptedException ie) {
                log.error("keepalive throw InterruptedException " ,ie);
                break;
            } catch (Exception e) {
                log.error("keepalive throw Exception " ,e);
                break;
            }
        }
        log.warn("keepalive thread exited!");
    }
}
