package com.esgyn.kafkaCDC.server;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.config.SaslConfigs;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;

import com.esgyn.kafkaCDC.server.utils.Utils;
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

        me.esgyndb   = new EsgynDB(me.params.getDBParams());
        me.consumers = new ArrayList<ConsumerThread>(0);

        if (me.params.getParams().isAConn()) {
            log.info("create a dbconn for shard connection");
            me.esgyndb.setSharedConn(me.esgyndb.CreateConnection(false));
        }

        //start consumer theads
        for (int partition : me.params.getParams().getPartitions()) {
            // connect to kafka w/ either zook setting
	    ConsumerThread consumer = 
		new ConsumerThread(me.esgyndb, me.params.getKafkaParams(), 
				   me.params.getParams().isSkip(), me.params.getParams().isBigEndian(),
				   me.params.getParams().getDelimiter(), me.params.getParams().getFormat(),
				   me.params.getParams().getEncoding(), partition,
				   me.params.getParams().getMessageClass(), me.params.getParams().getOutPath(), 
				   me.params.getParams().isAConn(), me.params.getParams().isBatchUpdate());
            consumer.setName("ConsumerThread-" + partition);
            me.consumers.add(consumer);
            consumer.start();
        }

        me.running = me.params.getParams().getPartitions().length;
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

        if (me.params.getParams().isAConn()) {
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
                esgyndb.DisplayDatabase(params.getParams().getInterval(), params.getParams().isTableSpeed());
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
                Thread.sleep(params.getParams().getInterval());
                if (alreadydisconnected) {
                    break;
                }
                esgyndb.DisplayDatabase(params.getParams().getInterval(), params.getParams().isTableSpeed());
                boolean firstAliveConsumer=true;
                for (int i = 0; i < consumers.size(); i++) {
                    ConsumerThread consumer = consumers.get(i);
                    if (!consumer.GetState()) {
                        running--;
                    }else {
                        if (params.getParams().isAConn() && firstAliveConsumer) {
                            // single conn
                            if (params.getParams().isKeepalive() && !consumer.KeepAliveEveryConn()) {
                                log.error("All Thread is disconnected from EsgynDB!");
                                alreadydisconnected=true;
                                break;
                            }
                            firstAliveConsumer=false;
                        }else if(!params.getParams().isAConn()){
                            //multiple conn
                            if (params.getParams().isKeepalive() && !consumer.KeepAliveEveryConn()) {
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
