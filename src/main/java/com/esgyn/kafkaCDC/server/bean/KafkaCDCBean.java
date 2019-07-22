package com.esgyn.kafkaCDC.server.bean;

import lombok.Getter;
import lombok.Setter;

/**
 * config for KafkaCDC
 */
public class KafkaCDCBean {
    @Setter
    @Getter
    private boolean aconn;
    @Setter
    @Getter
    private boolean batchUpdate = false;
    @Setter
    @Getter
    private boolean bigendian;
    @Setter
    @Getter
    private String  delim       = null;
    @Setter
    @Getter
    private String  encode      = null;
    @Setter
    @Getter
    private String  format      = null;
    @Setter
    @Getter
    private int     interval    = 0;
    @Setter
    @Getter
    private boolean keepalive;
    @Setter
    @Getter
    private String  outpath     = null;
    @Setter
    @Getter
    private String  partition   = null;
    @Setter
    @Getter
    private boolean skip;
    @Setter
    @Getter
    private boolean tablespeed;
}
