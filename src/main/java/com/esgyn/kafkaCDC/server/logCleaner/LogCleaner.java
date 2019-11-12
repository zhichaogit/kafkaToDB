package com.esgyn.kafkaCDC.server.logCleaner;

import java.util.Date;

import com.esgyn.kafkaCDC.server.utils.Utils;
import com.esgyn.kafkaCDC.server.utils.FileUtils;
import com.esgyn.kafkaCDC.server.utils.Parameters;
import com.esgyn.kafkaCDC.server.utils.KafkaCDCParams;

import org.apache.log4j.Logger;

public class LogCleaner extends Thread {
    private static Logger  log        = Logger.getLogger(LogCleaner.class);
    
    private KafkaCDCParams kafkaCDC         = null;
    private long           cleanLogInterval = 0L;

    public LogCleaner(Parameters params_) {
        kafkaCDC = params_.getKafkaCDC();
    }

    @Override
    public void run() {
        if (log.isTraceEnabled()) {
            log.trace("enter");
        }
        log.info("start LogCleaner thread");
        long cleanDelayTime     = kafkaCDC.getCleanDelayTime();
        cleanLogInterval = kafkaCDC.getCleanInterval();
        while (cleanLogInterval >= 0) {
            long curTime   = System.currentTimeMillis();
            Date startTime = new Date(curTime);
            Date endTime   = new Date(curTime - cleanDelayTime);
            
            long cleanKafkaLogNum = 0;
            long cleanLoadLogNum  = 0;

            if (kafkaCDC.getKafkaDir() !=null) {
                cleanKafkaLogNum = FileUtils.saveTimeRegionFiles(kafkaCDC.getKafkaDir(), startTime, endTime);
            }
            if (kafkaCDC.getLoadDir() !=null) {
                cleanLoadLogNum = FileUtils.saveTimeRegionFiles(kafkaCDC.getLoadDir(), startTime, endTime);
            }

            log.info("clean [" + kafkaCDC.getKafkaDir() + "] dir files num [" + cleanKafkaLogNum 
                    +"] , ["+kafkaCDC.getLoadDir()+"] dir files num [ " + cleanLoadLogNum + "]");

	    if (!Utils.waitMillisecond(cleanLogInterval))
		break;
        }
        if (log.isTraceEnabled()) {
            log.trace("exit");
        }
        log.info("stop LogCleaner thread");
    }
}
