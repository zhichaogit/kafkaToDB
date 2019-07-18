package com.esgyn.kafkaCDC.server.utils;

import com.esgyn.kafkaCDC.server.utils.Constants;

import lombok.Getter;
import lombok.Setter;

public class KafkaParams {
    @Setter
    @Getter
    String  broker      = Constants.DEFAULT_BROKER;
    @Setter
    @Getter 
    long    commitCount = Constants.DEFAULT_COMMIT_COUNT;
    @Setter
    @Getter
    String  full        = null;
    @Setter
    @Getter
    String  group       = null;
    @Setter
    @Getter
    String  topic       = null;
    @Setter
    @Getter
    String  kafkaUser   = null;
    @Setter
    @Getter
    String  kafkaPW     = null;
    @Setter
    @Getter
    String  key         = Constants.DEFAULT_KEY;
    @Setter
    @Getter
    String  value       = Constants.DEFAULT_VALUE;
    @Setter
    @Getter
    long    streamTO    = Constants.DEFAULT_STREAM_TO_S;
    @Setter
    @Getter
    long    zkTO        = Constants.DEFAULT_ZOOK_TO_S;
    @Setter
    @Getter
    int     hbTO        = Constants.DEFAULT_HEATBEAT_TO_S;
    @Setter
    @Getter
    int     seTO        = Constants.DEFAULT_SESSION_TO_S;
    @Setter
    @Getter
    int     reqTO       = Constants.DEFAULT_REQUEST_TO_S;
    @Setter
    @Getter
    String  zookeeper   = null;
}
