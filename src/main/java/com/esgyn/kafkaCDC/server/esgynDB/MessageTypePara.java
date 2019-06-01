package com.esgyn.kafkaCDC.server.esgynDB;

import java.util.Map;
import java.sql.Connection;

public class MessageTypePara<T> {
    private T                       message    = null;
    private int                     thread     = 0;
    private Long                    offset     = 0L ;
    private String                  delimiter  = null;
    private String                  encoding   = null;
    private boolean                 bigEndian  = false;
    private EsgynDB                 esgynDB    = null;
    private TableState              tableState = null;
    private Connection              dbConn     = null;
    private Map<String, TableState> tables     = null;

    public MessageTypePara(EsgynDB esgynDB_, Map<String, TableState> tables_,
			   TableState tableState_, Connection dbConn_, 
			   String delimiter_, int thread_, T message_, 
			   String encoding_, boolean bigEndian_,long offset_) {
        offset     = offset_;
        thread     = thread_;
        dbConn     = dbConn_;
        tables     = tables_;
        message    = message_;
        esgynDB    = esgynDB_;
        encoding   = encoding_;
        delimiter  = delimiter_;
        bigEndian  = bigEndian_;
        tableState = tableState_;
    }

    public EsgynDB getEsgynDB() { return esgynDB; }
    public void setEsgynDB(EsgynDB esgynDB) { this.esgynDB = esgynDB; }

    public Map<String, TableState> getTables() { return tables; }
    public void setTables(Map<String, TableState> tables) { this.tables = tables; }

    public TableState getTableState() { return tableState; }
    public void setTableState(TableState tableState) { this.tableState = tableState; }

    public Connection getDbConn() { return dbConn; }
    public void setDbConn(Connection dbConn) { this.dbConn = dbConn; }

    public String getDelimiter() { return delimiter; }
    public void setDelimiter(String delimiter) { this.delimiter = delimiter; }

    public int getThread() { return thread; }
    public void setThread(int thread) { this.thread = thread; }

    public T getMessage() { return message; }
    public void setMessage(T message) { this.message = message; }

    public String getEncoding() { return encoding; }
    public void setEncoding(String encoding) { this.encoding = encoding; }

    public boolean getBigEndian() { return bigEndian; }
    public void setBigEndian(boolean bigEndian) { this.bigEndian = bigEndian; }

    public Long getOffset() { return offset; }
    public void setOffset(Long offset) { this.offset = offset;}
}
