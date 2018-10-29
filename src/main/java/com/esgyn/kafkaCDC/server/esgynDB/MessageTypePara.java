package com.esgyn.kafkaCDC.server.esgynDB;

import java.sql.Connection;
import java.util.Map;

public class MessageTypePara<T> {
    private EsgynDB                 esgynDB    = null;
    private Map<String, TableState> tables     = null;
    private TableState              tableState = null;
    private Connection              dbConn     = null;
    private String                  delimiter  = null;
    private int                     thread     = 0;
    private T                       message    = null;
    private String                  encoding   = null;
    private boolean                 bigEndian  = false;

    public MessageTypePara(EsgynDB esgynDB_, Map<String, TableState> tables_,
            TableState tableState_, Connection dbConn_, String delimiter_, int thread_, T message_,
            String encoding_, boolean bigEndian_) {
        esgynDB = esgynDB_;
        tables = tables_;
        tableState = tableState_;
        dbConn = dbConn_;
        delimiter = delimiter_;
        thread = thread_;
        message = message_;
        encoding = encoding_;
        bigEndian = bigEndian_;
    }

    public EsgynDB getEsgynDB() {
        return esgynDB;
    }

    public void setEsgynDB(EsgynDB esgynDB) {
        this.esgynDB = esgynDB;
    }

    public Map<String, TableState> getTables() {
        return tables;
    }

    public void setTables(Map<String, TableState> tables) {
        this.tables = tables;
    }

    public TableState getTableState() {
        return tableState;
    }

    public void setTableState(TableState tableState) {
        this.tableState = tableState;
    }

    public Connection getDbConn() {
        return dbConn;
    }

    public void setDbConn(Connection dbConn) {
        this.dbConn = dbConn;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public int getThread() {
        return thread;
    }

    public void setThread(int thread) {
        this.thread = thread;
    }

    public T getMessage() {
        return message;
    }

    public void setMessage(T message) {
        this.message = message;
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public void setBigEndian(boolean bigEndian) {
        this.bigEndian = bigEndian;
    }

    public boolean getBigEndian() {
        return bigEndian;
    }

    public void setBigEndian(Boolean bigEndian) {
        this.bigEndian = bigEndian;
    }

}
