package com.esgyn.kafkaCDC.server.databaseLoader;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.esgyn.kafkaCDC.server.database.TableState;
import com.esgyn.kafkaCDC.server.kafkaConsumer.messageType.RowMessage;
import com.esgyn.kafkaCDC.server.utils.Parameters;
import com.esgyn.kafkaCDC.server.utils.TableInfo;
import com.esgyn.kafkaCDC.server.utils.Utils;

import lombok.Getter;

public class LoaderTask {
    private static Logger log = Logger.getLogger(LoaderTask.class);
    @Getter
    private LoadStates               loadStates  = null;
    private String                   topic       = null;
    private int                      partitionID = -1;
    private int                      consumerID  = -1;
    private List<RowMessage>         rows        = null;
    private Parameters               params      = null;

    // input via work parameters
    private int                      loaderID    = -1;
    private Connection               dbConn      = null;
    private Map<String, TableState>  tables      = null;
    private int                      state       = 0;

    // execute states
    private Long                     curOffset   = null;

    public LoaderTask(LoadStates       loadStates_,
		      String           topic_,
		      int              partitionID_,
		      int              consumerID_,
		      List<RowMessage> rows_) {
	if (log.isTraceEnabled()) { log.trace("enter"); }

	loadStates     = loadStates_;
	topic          = topic_;
	partitionID    = partitionID_;
	consumerID     = consumerID_;
	rows           = rows_;

	params         = loadStates.getParams();

        if (log.isTraceEnabled()) { log.trace("exit"); }
    }

    private boolean process_records() throws SQLException {
	if (log.isTraceEnabled()) { log.trace("enter"); }
	long existTable = 0;

	for (RowMessage row : rows) {
	    if (row.getOperatorType().equals("K")) {
	    	if (!flush_data())
	    	    return false;
	    } 

	    existTable += process_record(row);
	}

	if (!flush_data())
	    return false;

	loadStates.addExistTable(existTable);

	if (log.isTraceEnabled()) { log.trace("exit"); }

	return true;
    }

    private long process_record(RowMessage row) {
	if (log.isTraceEnabled()) { log.trace("enter"); }

	String     tableName  = row.getSchemaName() + "." + row.getTableName();
	TableState tableState = tables.get(tableName);

        // generate the TableStates for database loader
	if (tableState == null) {
	    TableInfo tableInfo = params.getDatabase().getTableInfo(tableName);
	    // Ignore this data when table not exist.
	    if (tableInfo == null) {
		if (log.isDebugEnabled()) {
		    log.warn("the table [" + tableName + "] is not exists!");
		}
		return 0;
	    }
		
	    tableState = new TableState(tableInfo);
	    tableState.init();
	    if (log.isDebugEnabled()) {
		log.debug("put table state [" + tableState + "] into map [" + tableName + "]");
	    }

	    tables.put(tableName, tableState);
	}
	
	curOffset = row.getOffset();
        tableState.insertMessageToTable(row);

        if (log.isTraceEnabled()) { log.trace("exit"); }

	return 1;
    }

    public long work(int loaderID_, Connection dbConn_, Map<String, TableState> tables_, int state_)
	throws SQLException {
        if (log.isTraceEnabled()) { 
	    log.trace("enter, loader [" + loaderID_ + ", conn [" + dbConn_ 
		      + "], tables [" + tables_ + "]");
	}

	loaderID  = loaderID_;
	dbConn    = dbConn_;
	tables    = tables_;
	state     = state_;

	long startTime = Utils.getTime();

	if (!process_records())
	    return -1;
	
	long endProcessTime = Utils.getTime();
        dbConn.commit();
	long endCommitTime = Utils.getTime();
	loadStates.addTransTotal(1);
	loadStates.addDoneTasks(1);

	log.info("loader task [totle time: " + (endCommitTime - startTime)
		 + "ms, process time: " + (endProcessTime - startTime) 
		 + "ms, commit time: " + (endCommitTime - endProcessTime)
		 + "ms, count: " + rows.size() 
		 + "speed: " + (rows.size() * 1000/(endCommitTime - startTime)) 
		 + "(n/s)]"); 

        if (log.isTraceEnabled()) { log.trace("exit"); }
	
	return rows.size();
    }


    private boolean flush_data() throws SQLException {
        if (log.isTraceEnabled()) { log.trace("enter"); }

        for (TableState tableState : tables.values()) {
            if (tableState.getCacheTotal() == 0) {
                continue;
            }

	    if (log.isDebugEnabled()) { 
		log.debug("there are [" + tableState.getCacheTotal() + "] rows in cache"); 
	    }

	    if (!tableState.flushData(dbConn, state))
	        return false;

	    loadStates.addErrInsertNum(tableState.getErrInsert());
	    loadStates.addErrUpdateNum(tableState.getErrUpdate());
	    loadStates.addErrDeleteNum(tableState.getErrDelete());

            loadStates.addInsertNum(tableState.getInsertRows());
            loadStates.addUpdateNum(tableState.getUpdateRows());
            loadStates.addDeleteNum(tableState.getDeleteRows());

            tableState.clearCache();
        }

        if (log.isTraceEnabled()) { log.trace("exit"); }

        return true;
    }

    public void clean() {
        if (log.isTraceEnabled()) { log.trace("enter"); }

        for (TableState tableState : tables.values()) {
	    tableState.clean();
	}
    }
}
