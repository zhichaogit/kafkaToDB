package com.esgyn.kafkaCDC.server.utils;

import com.esgyn.kafkaCDC.server.utils.Constants;

import lombok.Getter;
import lombok.Setter;

public class EsgynDBParams {
    @Setter
    @Getter 
    String                 DBUrl       = null;
    @Setter
    @Getter 
    String                 DBDriver    = Constants.DEFAULT_DRIVER;
    @Setter
    @Getter 
    String                 DBUser      = Constants.DEFAULT_USER;
    @Setter
    @Getter 
    String                 DBPassword  = Constants.DEFAULT_PASSWORD;
    @Setter
    @Getter 
    String                 defSchema   = null;
    @Setter
    @Getter 
    String                 defTable    = null;
}
