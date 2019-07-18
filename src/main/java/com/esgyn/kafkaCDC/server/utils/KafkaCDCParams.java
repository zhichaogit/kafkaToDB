package com.esgyn.kafkaCDC.server.utils;

import org.apache.log4j.Logger;

import com.esgyn.kafkaCDC.server.utils.Constants;

import lombok.Getter;
import lombok.Setter;

public class KafkaCDCParams {
    @Setter 
    @Getter 
    boolean aConn       = false;
    @Setter 
    @Getter
    boolean batchUpdate = false;
    @Setter 
    @Getter
    boolean bigEndian   = false;
    @Setter 
    @Getter
    long    commitCount = Constants.DEFAULT_COMMIT_COUNT;
    @Setter 
    @Getter
    String  delimiter   = null;
    @Setter 
    @Getter
    String  encoding    = Constants.DEFAULT_ENCODING;
    @Setter 
    @Getter
    String  format      = null;
    @Setter 
    @Getter
    long    interval    = Constants.DEFAULT_INTERVAL_S;
    @Setter 
    @Getter
    boolean keepalive   = false;
    @Setter 
    @Getter
    String  messageClass= Constants.DEFAULT_MESSAGECLASS;
    @Setter 
    @Getter
    String  outPath     = null;
    @Setter 
    @Getter
    int[]   partitions  = null;
    @Setter 
    @Getter
    String  partString  = null;
    @Setter 
    @Getter
    boolean skip        = false;
    @Setter 
    @Getter
    boolean tableSpeed  = false;
}
