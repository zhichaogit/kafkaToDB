package com.esgyn.kafkaCDC.server.bean;

import java.util.ArrayList;
import java.util.List;

import lombok.Getter;
import lombok.Setter;

/**
 * config for kafka
 */
public class KafkaBean {
    @Setter
    @Getter
    private String       broker    = null;
    @Setter
    @Getter
    private int          commit    = 0;
    @Setter
    @Getter
    private String       full      = null;
    @Setter
    @Getter
    private String       group     = null;
    @Setter
    @Getter
    private List<String> topic     = null;
    @Setter
    @Getter
    private String       kafkauser = null;
    @Setter
    @Getter
    private String       kafkapw   = null;
    @Setter
    @Getter
    private String       key       = null;
    @Setter
    @Getter
    private String       value     = null;
    @Setter
    @Getter
    private int          sto       = 0;
    @Setter
    @Getter
    private int          zkto      = 0;
    @Setter
    @Getter
    private int          hbto      = 0;
    @Setter
    @Getter
    private int          seto      = 0;
    @Setter
    @Getter
    private int          reqto     = 0;
    @Setter
    @Getter
    private String       zook      = null;
}
