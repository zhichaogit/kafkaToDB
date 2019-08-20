package com.esgyn.kafkaCDC.server.utils;


import java.util.List;

import com.esgyn.kafkaCDC.server.utils.Constants;

import lombok.Getter;
import lombok.Setter;

public class KafkaParams {
    @Setter
    @Getter
    private String broker      = Constants.DEFAULT_BROKER;
    @Setter
    @Getter 
    private long   commitCount = Constants.DEFAULT_COMMIT_COUNT;
    @Setter
    @Getter
    private String full        = null;
    @Setter
    @Getter
    private String group       = null;
    @Setter
    @Getter
    private String topic       = null;  // TODO support topics
    @Setter
    @Getter
    private String kafkaUser   = null;
    @Setter
    @Getter
    private String kafkaPW     = null;
    @Setter
    @Getter
    private String key         = Constants.DEFAULT_KEY;
    @Setter
    @Getter
    private String value       = Constants.DEFAULT_VALUE;
    @Setter
    @Getter
    private long   streamTO    = Constants.DEFAULT_STREAM_TO_S;
    @Setter
    @Getter
    private long   zkTO        = Constants.DEFAULT_ZOOK_TO_S;
    @Setter
    @Getter
    private int    hbTO        = Constants.DEFAULT_HEATBEAT_TO_S;
    @Setter
    @Getter
    private int    seTO        = Constants.DEFAULT_SESSION_TO_S;
    @Setter
    @Getter
    private int    reqTO       = Constants.DEFAULT_REQUEST_TO_S;
    @Setter
    @Getter
    private String zookeeper   = null;
}
