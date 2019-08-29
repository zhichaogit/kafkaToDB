package com.esgyn.kafkaCDC.server.utils;


import java.util.List;

import com.esgyn.kafkaCDC.server.utils.Constants;
import com.esgyn.kafkaCDC.server.utils.TopicParams;

import lombok.Getter;
import lombok.Setter;

public class KafkaParams {
    @Setter
    @Getter
    private String          broker      = Constants.DEFAULT_BROKER;
    @Setter
    @Getter
    private long            fetchBytes  = Constants.DEFAULT_FETCH_BYTES;
    @Setter 
    @Getter
    private long            fetchSize   = Constants.DEFAULT_FETCH_SIZE;
    @Setter
    @Getter
    private String          mode        = null;
    @Setter
    @Getter
    private List<TopicParams> topics = null;
    @Setter
    @Getter
    private String          kafkaUser   = null;
    @Setter
    @Getter
    private String          kafkaPW     = null;
    @Setter
    @Getter
    private String          key         = Constants.DEFAULT_KEY;
    @Setter
    @Getter
    private String          value       = Constants.DEFAULT_VALUE;
    @Setter
    @Getter
    private long            streamTO    = Constants.DEFAULT_STREAM_TO_S;
    @Setter
    @Getter
    private long            waitTO      = Constants.DEFAULT_WAIT_TO_S;
    @Setter
    @Getter
    private long            zkTO        = Constants.DEFAULT_ZOOK_TO_S;
    @Setter
    @Getter
    private int             hbTO        = Constants.DEFAULT_HEATBEAT_TO_S;
    @Setter
    @Getter
    private int             seTO        = Constants.DEFAULT_SESSION_TO_S;
    @Setter
    @Getter
    private int             reqTO       = Constants.DEFAULT_REQUEST_TO_S;
    @Setter
    @Getter
    private String          zookeeper   = null;

    // only for conf
    public void init() {
	mode = mode.toUpperCase();

	streamTO    *= 1000;
	waitTO      *= 1000;
	zkTO        *= 1000;
	hbTO        *= 1000;
	seTO        *= 1000;
	reqTO       *= 1000;
    }

    public String toString() {
        StringBuffer strBuffer = new StringBuffer();
	String topicsStr = null;

	// show the topics
	for (TopicParams topic : topics) {
	    if (topicsStr == null) {
		topicsStr = "[" + topic.toString();
	    } else {
		topicsStr += ", " + topic.toString();
	    }
	}
	topicsStr += "]";

	strBuffer.append("\n\nKafka options:")
	    .append("\n\tbroker        = "    + broker)
	    .append("\n\tfetchBytes    = "    + fetchBytes)
	    .append("\n\tfetchSize     = "    + fetchSize)
	    .append("\n\tmode          = "    + mode)
	    .append("\n\ttopics        = "    + topicsStr)
	    .append("\n\tkafkauser     = "    + kafkaUser)
	    .append("\n\tkafkapasswd   = "    + kafkaPW)
	    .append("\n\tkey           = "    + key)
	    .append("\n\tvalue         = "    + value)
	    .append("\n\tstreamTO      = "    + streamTO/1000 + "s")
	    .append("\n\tzkTO          = "    + zkTO/1000 + "s")
	    .append("\n\tbhTO          = "    + hbTO/1000 + "s")
	    .append("\n\tseTO          = "    + seTO/1000 + "s")
	    .append("\n\treqTO         = "    + reqTO/1000 + "s")
	    .append("\n\tzookeeper     = "    + zookeeper);

	return strBuffer.toString();
    }
}
