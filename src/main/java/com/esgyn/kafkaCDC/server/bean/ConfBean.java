package com.esgyn.kafkaCDC.server.bean;

import java.util.List;

import lombok.Getter;
import lombok.Setter;

/**
 * 
 * KafkaCDCJson config
 *
 */
public class ConfBean {
    @Setter
    @Getter
    private EsgynDBBean       esgynDB  = null;
    @Setter
    @Getter
    private KafkaBean         kafka    = null;
    @Setter
    @Getter
    private KafkaCDCBean      kafkaCDC = null;
    @Setter
    @Getter
    private List<MappingBean> mapping  = null;
}
