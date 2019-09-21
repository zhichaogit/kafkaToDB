package com.esgyn.kafkaCDC.server.utils;


import lombok.Getter;
import lombok.Setter;

public class TopicParams {
    @Setter
    @Getter
    private String          topic       = null;
    @Setter 
    @Getter
    private String          partition   = "-1";
    @Setter
    @Getter
    private String          group       = null;
    @Setter 
    @Getter
    private int[]           partitions  = null;

    public String toString() {
        StringBuffer strBuffer = new StringBuffer();
	String       partStr   = "[";

	partStr += partitions[0];
	for (int i = 1; i < partitions.length; i++) {
	    partStr += "," + partitions[i];
	}
	partStr += "]";
	strBuffer.append(topic + " : {")
	    .append("partition : " + partStr + ", group : " + group)
	    .append("}");

	return strBuffer.toString();
    }
}
