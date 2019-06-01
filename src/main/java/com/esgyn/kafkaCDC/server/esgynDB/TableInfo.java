package com.esgyn.kafkaCDC.server.esgynDB;

import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import org.apache.log4j.Logger;

public class TableInfo {
    private String           schemaName    = null;
    private String           tableName     = null;

    private boolean          multiable     = false;;

    private long             insMsgNum     = 0;
    private long             updMsgNum     = 0;
    private long             keyMsgNum     = 0;
    private long             delMsgNum     = 0;

    private long             errInsNum     = 0;
    private long             errUpdNum     = 0;
    private long             errDelNum     = 0;

    private long             insertNum     = 0;
    private long             updateNum     = 0;
    private long             deleteNum     = 0;

    ArrayList<ColumnInfo>    keyColumns    = null;
    ArrayList<ColumnInfo>    columns       = null;
    Map<Integer, ColumnInfo> columnMap     = null;
    Map<String, ColumnInfo>  columnNameMap = null;
    private static Logger    log = Logger.getLogger(TableInfo.class);

    public TableInfo(String schemaName_, String tableName_, boolean multiable_) {
        tableName  = tableName_;
        multiable  = multiable_;
        schemaName = schemaName_;

        columns    = new ArrayList<ColumnInfo>(0);
        columnMap  = new HashMap<Integer, ColumnInfo>(0);
        keyColumns = new ArrayList<ColumnInfo>(0);
        columnNameMap = new HashMap<String, ColumnInfo>(0);
    }

    public void AddColumn(ColumnInfo column) {
        columns.add(column);
        columnMap.put(column.GetColumnID(), column);
        columnNameMap.put(column.GetColumnName(), column);
    }

    public String GetTableName() { return tableName; }
    public String GetSchemaName() { return schemaName; }

    public ColumnInfo GetColumn(int index) { return columns.get(index); }
    public ColumnInfo GetColumn(String colName) { return columnNameMap.get(colName);}
    public ColumnInfo GetColumnFromMap(int colid) { return columnMap.get(colid); }

    public long GetColumnCount() { return columns.size(); }
    public void AddKey(ColumnInfo column) { keyColumns.add(column); }
    public ColumnInfo GetKey(int index) { return keyColumns.get(index); }
    public long GetKeyCount() { return keyColumns.size(); }

    public ArrayList<ColumnInfo> GetKeyColumns() { return keyColumns; }
    public ArrayList<ColumnInfo> GetColumns() { return columns; }
    public Map<Integer, ColumnInfo> GetColumnMap() { return columnMap; }

    public boolean IsMultiable() { return multiable; }

    public synchronized void IncInsertRows(long rows) { insertNum += rows; }
    public synchronized void IncUpdateRows(long rows) { updateNum += rows; }
    public synchronized void IncDeleteRows(long rows) { deleteNum += rows; }

    public synchronized void IncInsMsgNum(long rows) { insMsgNum += rows; }
    public synchronized void IncUpdMsgNum(long rows) { updMsgNum += rows; }
    public synchronized void IncKeyMsgNum(long rows) { keyMsgNum += rows; }

    public synchronized void IncDelMsgNum(long rows) { delMsgNum += rows; }
    public synchronized void IncErrInsNum(long rows) { errInsNum += rows; }
    public synchronized void IncErrUpdNum(long rows) { errUpdNum += rows; }
    public synchronized void IncErrDelNum(long rows) { errDelNum += rows;}

    public void DisplayStat(StringBuffer strBuffer) {
        String tableString = 
	    String.format("  %s\tMsgs [ %d, %d, %d, %d] DMLs [ %d, %d, %d]"
			  + " Fails [ %d, %d, %d]\n",
			  schemaName + "." + tableName, insMsgNum, updMsgNum, 
			  keyMsgNum, delMsgNum, insertNum, updateNum, deleteNum,
			  errInsNum, errUpdNum, errDelNum);
        strBuffer.append(tableString);
    }
}
