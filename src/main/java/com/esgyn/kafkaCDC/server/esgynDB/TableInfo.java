package com.esgyn.kafkaCDC.server.esgynDB;

import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Date;
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

    private long           interval        = 0;
    private boolean        tablespeed      = false;
    private long           begin;
    private Date           startTime;
    private long           oldTableMsgNum  = 0;
    private long           oldIMsgNum      = 0;
    private long           oldUMsgNum      = 0;
    private long           oldKMsgNum      = 0;
    private long           oldDMsgNum      = 0;
    private long           maxTableSpeed   = 0;
    private long           maxISpeed       = 0;
    private long           maxUSpeed       = 0;
    private long           maxKSpeed       = 0;
    private long           maxDSpeed       = 0;
    private static Logger    log = Logger.getLogger(TableInfo.class);

    public TableInfo(String schemaName_, String tableName_, boolean multiable_,long interval_,boolean tablespeed_) {
        tableName  = tableName_;
        multiable  = multiable_;
        schemaName = schemaName_;

        columns    = new ArrayList<ColumnInfo>(0);
        columnMap  = new HashMap<Integer, ColumnInfo>(0);
        keyColumns = new ArrayList<ColumnInfo>(0);
        columnNameMap = new HashMap<String, ColumnInfo>(0);

        interval   = interval_;
        tablespeed = tablespeed_;
        begin      = new Date().getTime();
        startTime  = new Date();
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
    public synchronized void IncErrDelNum(long rows) { errDelNum += rows; }

    public void DisplayStat(StringBuffer strBuffer) {

        String tableString = 
	    String.format("  %-35s Msgs [%12d,%12d,%12d,%12d] DMLs [%12d,%12d,%12d]"
			  + " Fails [ %d, %d, %d]\n",
			  schemaName + "." + tableName, insMsgNum, updMsgNum, 
			  keyMsgNum, delMsgNum, insertNum, updateNum, deleteNum,
			  errInsNum, errUpdNum, errDelNum);
        strBuffer.append(tableString);

        if (tablespeed) {
            Long end = new Date().getTime();
            Float useTime = ((float) (end - begin)) / 1000;
            //tables run Speed
            long tableMsgNum = insMsgNum + updMsgNum + keyMsgNum + delMsgNum;
            long avgTableSpeed = (long) (tableMsgNum / useTime);
            long incTableMessage = (tableMsgNum - oldTableMsgNum);
            long curTableSpeed = (long) (incTableMessage / (interval / 1000));
            if (curTableSpeed > maxTableSpeed)
                maxTableSpeed = curTableSpeed;
            //insert speed
            long avgISpeed = (long) (insMsgNum / useTime);
            long incIMessage = (insMsgNum - oldIMsgNum);
            long curISpeed = (long) (incIMessage / (interval / 1000));
            if (curISpeed > maxISpeed)
                maxISpeed = curISpeed;
            //update speed
            long avgUSpeed = (long) (updMsgNum / useTime);
            long incUMessage = (updMsgNum - oldUMsgNum);
            long curUSpeed = (long) (incUMessage / (interval / 1000));
            if (curUSpeed > maxUSpeed)
                maxUSpeed = curUSpeed;
            //updatekey speed
            long avgKSpeed = (long) (keyMsgNum / useTime);
            long incKMessage = (keyMsgNum - oldKMsgNum);
            long curKSpeed = (long) (incKMessage / (interval / 1000));
            if (curKSpeed > maxKSpeed)
                maxKSpeed = curKSpeed;
            //Delete speed
            long avgDSpeed = (long) (delMsgNum / useTime);
            long incDMessage = (delMsgNum - oldDMsgNum);
            long curDSpeed = (long) (incDMessage / (interval / 1000));
            if (curDSpeed > maxDSpeed)
                maxDSpeed = curDSpeed;
            String tableSpeedStr =
                    String.format("                                      "
                            + "tableSpeed(n/s) [max: %d, avg: %d, cur: %d] "
                            + "IS(n/s) [ %d, %d, %d] "
                            + "US(n/s) [ %d, %d, %d] "
                            + "KS(n/s) [ %d, %d, %d] "
                            + "DS(n/s) [ %d, %d, %d]\n",
                            maxTableSpeed,avgTableSpeed,curTableSpeed,
                            maxISpeed,avgISpeed,curISpeed,
                            maxUSpeed,avgUSpeed,curUSpeed,
                            maxKSpeed,avgKSpeed,curKSpeed,
                            maxDSpeed,avgDSpeed,curDSpeed);
            strBuffer.append(tableSpeedStr);
            oldTableMsgNum = tableMsgNum;
            oldIMsgNum     = insMsgNum;
            oldUMsgNum     = updMsgNum;
            oldKMsgNum     = keyMsgNum;
            oldDMsgNum     = delMsgNum;
        }
    }
}
