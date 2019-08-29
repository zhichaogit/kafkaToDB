package com.esgyn.kafkaCDC.server.utils;

import org.apache.log4j.Logger;

import com.esgyn.kafkaCDC.server.utils.Constants;

import lombok.Getter;
import lombok.Setter;

public class KafkaCDCParams {
    @Setter 
    @Getter 
    private long            consumers   = Constants.DEFAULT_CONSUMERS;
    @Setter 
    @Getter
    private boolean         bigEndian   = false;
    @Setter 
    @Getter
    private String          delimiter   = Constants.DEFAULT_DELIMITER;
    @Setter 
    @Getter
    private boolean         dumpBinary  = false;
    @Setter 
    @Getter
    private String          encoding    = Constants.DEFAULT_ENCODING;
    @Setter 
    @Getter
    private String          format      = null;
    @Setter 
    @Getter
    private long            interval    = Constants.DEFAULT_INTERVAL_S;
    @Setter 
    @Getter
    private boolean         skip        = false;
    @Setter 
    @Getter 
    private long            loaders     = Constants.DEFAULT_LEADER;
    @Setter 
    @Getter
    private String          loadDir     = null;
    @Setter
    @Getter
    private String          kafkaDir    = null;
    @Setter 
    @Getter
    private boolean         showConsumers = true;
    @Setter 
    @Getter
    private boolean         showLoaders = true;
    @Setter 
    @Getter
    private boolean         showTasks   = false;
    @Setter 
    @Getter
    private boolean         showTables  = true;
    @Setter 
    @Getter
    private boolean         showSpeed   = false;
    @Setter 
    @Getter
    private String          msgClass    = null;

    public void init(String startTime) {
	loadDir = getFullPath(loadDir, startTime);
	kafkaDir = getFullPath(kafkaDir, startTime);

	interval *= 1000;
	msgClass = "com.esgyn.kafkaCDC.server.kafkaConsumer.messageType." 
	    + format + "RowMessage";
    }

    private String getFullPath(String curDir, String startTime) {
	if (curDir == null)
	    return null;

	return Constants.DEFAULT_LOG_PATH + "/" + startTime + "/" + curDir + "/";
    }

    public String toString() {
        StringBuffer strBuffer = new StringBuffer();

	strBuffer.append("\n\nKafkaCDC options:")
	    .append("\n\tconsumers     = "    + consumers)
	    .append("\n\tbigendian     = "    + bigEndian)
	    .append("\n\tdelimiter     = \""  + delimiter + "\"")
	    .append("\n\tdumpbinary    = "    + dumpBinary)
	    .append("\n\tencoding      = "    + encoding)
	    .append("\n\tformat        = "    + format)
	    .append("\n\tinterval      = "    + interval/1000 + "s")
	    .append("\n\tskip          = "    + skip)
	    .append("\n\tloaders       = "    + loaders)
	    .append("\n\tloadDir       = "    + loadDir)
	    .append("\n\tkafkaDir      = "    + kafkaDir)
	    .append("\n\tshowConsumers = "    + showConsumers)
	    .append("\n\tshowLoaders   = "    + showLoaders)
	    .append("\n\tshowTasks     = "    + showTasks)
	    .append("\n\tshowTables    = "    + showTables)
	    .append("\n\tshowSpeed     = "    + showSpeed)
	    .append("\n\tmsgClass      = "    + msgClass);

	return strBuffer.toString();
    } 
}
