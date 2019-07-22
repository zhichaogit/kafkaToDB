package com.esgyn.kafkaCDC.server.bean;

import lombok.Getter;
import lombok.Setter;

/**
 * EsgynDB object for json conf
 */
public class EsgynDBBean {
    @Setter
    @Getter
    private String  dbip        = null;
    @Setter
    @Getter
    private String  dbport      = "";
    @Setter
    @Getter
    private String  dbuser      = null;
    @Setter
    @Getter
    private String  dbpw        = null;
    @Setter
    @Getter
    private String  tenant      = null;
}
