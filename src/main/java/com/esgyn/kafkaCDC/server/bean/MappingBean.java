package com.esgyn.kafkaCDC.server.bean;

import java.util.List;

import lombok.Getter;
import lombok.Setter;

/**
 * mapping config
 */
public class MappingBean {
    @Setter
    @Getter
    private String           schema     = null;
    @Setter
    @Getter
    private String           table      = null;
    @Setter
    @Getter
    private String           old_schema = null;
    @Setter
    @Getter
    private String           old_table  = null;
    @Setter
    @Getter
    private List<ColumnBean> columns    = null;
    @Setter
    @Getter
    private List<Integer>    keys       = null;
}
