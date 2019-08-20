package com.esgyn.kafkaCDC.server.utils;

import org.apache.log4j.Logger;

import com.esgyn.kafkaCDC.server.utils.Constants;

import lombok.Getter;
import lombok.Setter;

public class KafkaCDCParams {
    @Setter 
    @Getter 
    private boolean aConn       = false;
    @Setter 
    @Getter
    private boolean batchUpdate = false;
    @Setter 
    @Getter
    private boolean bigEndian   = false;
    @Setter 
    @Getter
    private long    commitCount = Constants.DEFAULT_COMMIT_COUNT;
    @Setter 
    @Getter
    private String  delimiter   = ",";
    @Setter 
    @Getter
    private String  encoding    = Constants.DEFAULT_ENCODING;
    @Setter 
    @Getter
    private String  format      = null;
    @Setter 
    @Getter
    private long    interval    = Constants.DEFAULT_INTERVAL_S;
    @Setter 
    @Getter
    private boolean keepalive   = false;
    @Setter 
    @Getter
    private String  msgClass    = Constants.DEFAULT_MESSAGECLASS;
    @Setter 
    @Getter
    private String  outPath     = null;
    @Setter 
    @Getter
    private String  partition   = null;
    @Setter 
    @Getter
    private int[]   partitions  = null;
    @Setter 
    @Getter
    private boolean skip        = false;
    @Setter 
    @Getter
    private boolean tableSpeed  = false;
}
