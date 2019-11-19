package com.esgyn.kafkaCDC.server.databaseLoader;

import java.util.Map;

import org.apache.log4j.Logger;

import com.esgyn.kafkaCDC.server.utils.Parameters;
import com.esgyn.kafkaCDC.server.utils.TableInfo;
import com.esgyn.kafkaCDC.server.utils.Constants;

import lombok.Getter;

public class LoadStates {
    @Getter
    private Parameters     params         = null;

    private long           totalTasks     = 0;
    private long           doneTasks      = 0;
    private long           existTable     = 0;

    private long           totalLoaded    = 0;
    private long           transTotal     = 0;
    private long           transFails     = 0;

    private long           insertNum      = 0;
    private long           updateNum      = 0;
    private long           deleteNum      = 0;

    private long           errInsertNum   = 0;
    private long           errUpdateNum   = 0;
    private long           errDeleteNum   = 0;

    private static Logger log = Logger.getLogger(LoadStates.class);

    public LoadStates(Parameters params_) {
        if (log.isTraceEnabled()) { log.trace("enter"); }

	params    = params_;

        if (log.isTraceEnabled()) { log.trace("exit"); }
    }

    public synchronized void incTotalLoaded(long inc) { totalLoaded += inc; }
    public synchronized long getTotalLoaded() { return totalLoaded; }

    public synchronized void addTotalTasks(long totalTasks_) {
        totalTasks += totalTasks_;
    }

    public synchronized void addDoneTasks(long doneTasks_) {
        doneTasks += doneTasks_;
    }

    public synchronized void addExistTable(long existTable_) {
        existTable += existTable_;
    }

    public synchronized void addInsertNum(long insertNum_) {
        insertNum += insertNum_;
    }

    public synchronized void addUpdateNum(long updateNum_) {
        updateNum += updateNum_;
    }

    public synchronized void addDeleteNum(long deleteNum_) {
        deleteNum += deleteNum_;
    }

    public synchronized void addErrInsertNum(long errInsertNum_) {
        errInsertNum += errInsertNum_;
    }

    public synchronized void addErrUpdateNum(long errUpdateNum_) {
        errUpdateNum += errUpdateNum_;
    }

    public synchronized void addErrDeleteNum(long errDeleteNum_) {
        errDeleteNum += errDeleteNum_;
    }

    public synchronized void addTransTotal(long transTotal_) {
        transTotal+=transTotal_;
    }

    public synchronized void addTransFails(long transFails_) {
        transFails += transFails_;
    }

    public void showTables(StringBuffer strBuffer, int format) {
	boolean first = true;
	Map<String, TableInfo> tableHashMap = params.getDatabase().getTableHashMap();

	strBuffer.append(Constants.getFormatStart("Tables", format));
	for (String tableName : tableHashMap.keySet()) {
	    TableInfo tableInfo = tableHashMap.get(tableName);
	    String mapTableName = tableInfo.getSchemaName() + "." + tableInfo.getTableName();
	    if (tableName.equals(mapTableName)) {
		if (!first)
		    strBuffer.append(Constants.getFormatEntry(format));
		first = false;
		tableInfo.show(strBuffer, format);
	    }
	}
	strBuffer.append(Constants.getFormatEnd(format));
    }

    public void showDatabase(StringBuffer strBuffer, int format) {
	switch(format){
	case Constants.KAFKA_STRING_FORMAT:
	    strBuffer.append("Transactions: {Total: " + transTotal)
		.append(", Fails: " + transFails + "}")
		.append(", Tasks {Total: " + totalTasks)
		.append(", Done: " + doneTasks + "}")
		.append(", DMLs {Insert: " + insertNum)
		.append(", Update: " + updateNum)
		.append(", Delete: " + deleteNum + "}")
		.append(", Fails {Insert: " + errInsertNum)
		.append(", Update: " + errUpdateNum)
		.append(", Delete: " + errDeleteNum + "}");
	    break;
	   
	case Constants.KAFKA_JSON_FORMAT:
	    strBuffer.append("{\"Transactions\": {\"Total\": " + transTotal)
		.append(", \"Fails\": " + transFails + "}")
		.append(", \"Tasks\": {\"Total\": " + totalTasks)
		.append(", \"Done\": " + doneTasks + "}")
		.append(", \"DMLs\": {\"Insert\": " + insertNum)
		.append(", \"Update\": " + updateNum)
		.append(", \"Delete\": " + deleteNum + "}")
		.append(", \"Fails\": {\"Insert\": " + errInsertNum)
		.append(", \"update\": " + errUpdateNum)
		.append(", \"delete\": " + errDeleteNum + "}}");
	    break;
	}
    }

    public void show(StringBuffer strBuffer) {
        strBuffer.append("  Database loader states, ");
	showDatabase(strBuffer, Constants.KAFKA_STRING_FORMAT);

	if (params.getKafkaCDC().isShowTables()) {
	    strBuffer.append("  The detail of table loaded:\n");
	    showTables(strBuffer, Constants.KAFKA_STRING_FORMAT);
	}
    }
}
