package com.esgyn.kafkaCDC.server.utils;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import lombok.Getter;
import lombok.Setter;

public class TableInfo {
    @Setter
    @Getter
    private String           schemaName    = null;
    @Setter
    @Getter
    private String           tableName     = null;
    @Setter
    @Getter
    private String           srcSchemaName = null;
    @Setter
    @Getter
    private String           srcTableName  = null;
    @Setter
    @Getter
    private boolean          multiable     = false;;
    @Setter
    @Getter
    private ArrayList<ColumnInfo> columns  = null;
    @Setter
    @Getter
    private ArrayList<Integer>    keys     = null;
    @Getter
    private Parameters       params        = null;

    private long             columnSize    = 0;

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

    private long             interval      = 0;
    private Date             startTime     = null;
    private long             oldTableMsgNum= 0;
    private long             oldIMsgNum    = 0;
    private long             oldUMsgNum    = 0;
    private long             oldKMsgNum    = 0;
    private long             oldDMsgNum    = 0;
    private long             maxTableSpeed = 0;
    private long             maxISpeed     = 0;
    private long             maxUSpeed     = 0;
    private long             maxKSpeed     = 0;
    private long             maxDSpeed     = 0;
    private static Logger log = Logger.getLogger(TableInfo.class);

    ArrayList<ColumnInfo>    keyColumns    = null;
    Map<Integer, ColumnInfo> columnMap     = null;
    Map<String, ColumnInfo>  columnNameMap = null;

    public TableInfo(Parameters params_, String schemaName_, String tableName_) {
	params     = params_;
        tableName  = tableName_;
        schemaName = schemaName_;

        columns    = new ArrayList<ColumnInfo>(0);
        columnMap  = new HashMap<Integer, ColumnInfo>(0);
        keyColumns = new ArrayList<ColumnInfo>(0);
        columnNameMap = new HashMap<String, ColumnInfo>(0);

        startTime  = new Date();
    }

    public void addColumn(ColumnInfo columnInfo) {
        columns.add(columnInfo);
        columnMap.put(columnInfo.getColumnID(), columnInfo);
        columnNameMap.put(columnInfo.getColumnName(), columnInfo);
	columnSize += columnInfo.getColumnSize();
    }

    public ColumnInfo getColumn(int index) { return columns.get(index); }
    public ColumnInfo getColumn(String colName) { return columnNameMap.get(colName);}
    public void putColumn(String colName, ColumnInfo colomnInfo) { 
	columnNameMap.put(colName, colomnInfo);
    }
    public ColumnInfo getColumnFromMap(int colid) { return columnMap.get(colid); }
    public long getBatchSize() {
	long tableBatchSize = Constants.DEFAULT_COMMITSIZE / columnSize;

	if (params.getDatabase().getBatchSize() > tableBatchSize)
	    return tableBatchSize;
	else 
	    return params.getDatabase().getBatchSize();
    }


    public long getColumnCount() { return columns.size(); }
    public void addKey(ColumnInfo column) { keyColumns.add(column); }
    public ColumnInfo getKey(int index) { return keyColumns.get(index); }
    public long getKeyCount() { return keyColumns.size(); }

    public ArrayList<ColumnInfo> getKeyColumns() { return keyColumns; }
    public ArrayList<ColumnInfo> getColumns() { return columns; }
    public Map<Integer, ColumnInfo> getColumnMap() { return columnMap; }

    // there are no primary key when colId is 0, the data can been repeatable
    public boolean isRepeatable() { return (keyColumns.get(0).getColumnID() != 0); }

    public synchronized void incInsertRows(long rows) { insertNum += rows; }
    public synchronized void incUpdateRows(long rows) { updateNum += rows; }
    public synchronized void incDeleteRows(long rows) { deleteNum += rows; }

    public synchronized void incInsMsgNum(long rows) { insMsgNum += rows; }
    public synchronized void incUpdMsgNum(long rows) { updMsgNum += rows; }
    public synchronized void incKeyMsgNum(long rows) { keyMsgNum += rows; }
    public synchronized void incDelMsgNum(long rows) { delMsgNum += rows; }

    public synchronized void incErrInsNum(long rows) { errInsNum += rows; }
    public synchronized void incErrUpdNum(long rows) { errUpdNum += rows; }
    public synchronized void incErrDelNum(long rows) { errDelNum += rows; }

    public void show(StringBuffer strBuffer) {
	long interval = params.getKafkaCDC().getInterval();

        String tableString = 
	    String.format("  -> %-60s Msgs [%12d,%12d,%12d,%12d] DMLs [%12d,%12d,%12d]"
			  + " Fails [ %d, %d, %d]\n",
			  schemaName + "." + tableName, insMsgNum, updMsgNum, 
			  keyMsgNum, delMsgNum, insertNum, updateNum, deleteNum,
			  errInsNum, errUpdNum, errDelNum);
        strBuffer.append(tableString);

        if (params.getKafkaCDC().isShowSpeed()) {
            Long end = new Date().getTime();
            Float useTime = ((float) (end - startTime.getTime())) / 1000;
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
                    String.format("%-66s"
                            + "tableSpee(dn/s) [max: %d, avg: %d, cur: %d] "
                            + "IS(n/s) [ %d, %d, %d] "
                            + "US(n/s) [ %d, %d, %d] "
                            + "KS(n/s) [ %d, %d, %d] "
			    + "DS(n/s) [ %d, %d, %d]\n", "",
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
