package com.esgyn.kafkaCDC.server.databaseLoader;

import java.util.Map;

import org.apache.log4j.Logger;

import com.esgyn.kafkaCDC.server.utils.Parameters;
import com.esgyn.kafkaCDC.server.utils.TableInfo;

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

    public void show(StringBuffer strBuffer) {	
        strBuffer.append("  Database loader states")
	    .append(", Transactions [total: " + transTotal)
	    .append(", Fails: " + transFails + "]")
	    .append(", Tasks [total: " + totalTasks + ", done: " + doneTasks + "]")
	    .append(", DMLs [insert: " + insertNum + ", update: " + updateNum + ", delete: " + deleteNum + "]")
	    .append(", Fails [insert: " + errInsertNum + ", update: " + errUpdateNum + ", delete: " + errDeleteNum + "]\n");

	if (params.getKafkaCDC().isShowTables()) {
	    strBuffer.append("  The detail of table loaded:\n");
	    Map<String, TableInfo> tableHashMap = params.getDatabase().getTableHashMap();
	    for (String tableName : tableHashMap.keySet()) {
		TableInfo tableInfo = tableHashMap.get(tableName);
		String mapTableName = tableInfo.getSchemaName() + "." + tableInfo.getTableName();
		if (tableName.equals(mapTableName))
		    tableInfo.show(strBuffer);
	    }
	}
    }
}
