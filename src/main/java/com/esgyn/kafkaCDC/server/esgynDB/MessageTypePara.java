package com.esgyn.kafkaCDC.server.esgynDB;

import java.util.Map;
import java.sql.Connection;

import com.esgyn.kafkaCDC.server.utils.EsgynDBParams;

import lombok.Getter;
import lombok.Setter;

public class MessageTypePara<T> {
    @Setter
    @Getter
    private T                       message    = null;
    @Setter
    @Getter
    private int                     thread     = 0;
    @Setter
    @Getter
    private Long                    offset     = 0L ;
    @Setter
    @Getter
    private String                  delimiter  = null;
    @Setter
    @Getter
    private String                  encoding   = null;
    @Setter
    @Getter
    private boolean                 bigEndian  = false;
    @Setter
    @Getter
    private EsgynDBParams           esgynDB    = null;
    @Setter
    @Getter
    private TableState              tableState = null;
    @Setter
    @Getter
    private Connection              DBConn     = null;
    @Setter
    @Getter
    private Map<String, TableState> tables     = null;

    public MessageTypePara(EsgynDBParams esgynDB_, Map<String, TableState> tables_,
			   TableState tableState_, Connection dbConn_, 
			   String delimiter_, int thread_, T message_, 
			   String encoding_, boolean bigEndian_,long offset_) {
        offset     = offset_;
        thread     = thread_;
        DBConn     = dbConn_;
        tables     = tables_;
        message    = message_;
        esgynDB    = esgynDB_;
        encoding   = encoding_;
        delimiter  = delimiter_;
        bigEndian  = bigEndian_;
        tableState = tableState_;
    }
}
