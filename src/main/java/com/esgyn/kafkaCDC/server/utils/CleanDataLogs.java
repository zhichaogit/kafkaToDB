package com.esgyn.kafkaCDC.server.utils;

import java.util.Date;

import org.apache.log4j.Logger;

public class CleanDataLogs extends Thread {
    private static Logger  log        = Logger.getLogger(CleanDataLogs.class);
    
    private KafkaCDCParams kafkaCDC         = null;
    private long           cleanLogInterval = 0L;

    public CleanDataLogs(Parameters params_) {
        kafkaCDC = params_.getKafkaCDC();
    }

    @Override
    public void run() {
        if (log.isTraceEnabled()) {
            log.trace("enter");
        }
        log.info("start cleanDataLogs thread");
        long cleanDelayTime     = kafkaCDC.getCleanDelayTime();
        cleanLogInterval = kafkaCDC.getCleanInterval();
        while (cleanLogInterval >= 0) {
            long curTime   = System.currentTimeMillis();
            Date startTime = new Date(curTime);
            Date endTime   = new Date(curTime - cleanDelayTime);
            
            long cleanKafkaLogNum = FileUtils.saveTimeRegionFiles(kafkaCDC.getKafkaDir(), startTime, endTime);
            long cleanLoadLogNum = FileUtils.saveTimeRegionFiles(kafkaCDC.getLoadDir(), startTime, endTime);
            
            log.info("clean [" + kafkaCDC.getKafkaDir() + "] dir files num [" + cleanKafkaLogNum 
                    +"] , ["+kafkaCDC.getLoadDir()+"] dir files num [ " + cleanLoadLogNum + "]");
            try {
                Thread.sleep(cleanLogInterval);
            } catch (InterruptedException e) {
                break;
            }
        }
        if (log.isTraceEnabled()) {
            log.trace("exit");
        }
        log.info("stop cleanDataLogs thread");
    }
}
