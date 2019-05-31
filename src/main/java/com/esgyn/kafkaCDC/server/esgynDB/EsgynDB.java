package com.esgyn.kafkaCDC.server.esgynDB;

import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;

import java.util.Map;
import java.util.HashMap;
import java.lang.StringBuffer;
import org.apache.log4j.Logger;

import java.util.Date;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;

public class EsgynDB {
    String                 dburl       = null;
    String                 dbdriver    = null;
    String                 dbuser      = null;
    String                 dbpassword  = null;
    String                 defschema   = null;
    String                 deftable    = null;
    String                 NOTINITT1   = "SB_HISTOGRAMS";
    String                 NOTINITT2   = "SB_HISTOGRAM_INTERVALS";
    String                 NOTINITT3   = "SB_PERSISTENT_SAMPLES";
    Connection             sharedConn  = null;
    long                   commitCount = 500;
    Map<String, TableInfo> tables      = null;
    private static Logger  log         = Logger.getLogger(EsgynDB.class);

    private long           totalMsgNum = 0;
    private long           kafkaMsgNum = 0;

    private long           messageNum  = 0;
    private long           insMsgNum   = 0;
    private long           updMsgNum   = 0;
    private long           keyMsgNum   = 0;
    private long           delMsgNum   = 0;

    private long           oldMsgNum   = 0;

    private long           insertNum   = 0;
    private long           updateNum   = 0;
    private long           deleteNum   = 0;

    private long           errInsertNum   = 0;
    private long           errUpdateNum   = 0;
    private long           errDeleteNum   = 0;

    private long           maxSpeed    = 0;
    private long           interval    = 0;

    private boolean        multiable   = false;

    private long           begin;
    private Date           startTime;

    public EsgynDB(String defschema_, String deftable_, String dburl_, String dbdriver_,
            String dbuser_, String dbpassword_, long interval_, long commitCount_,
            boolean multiable_) {
        if (log.isTraceEnabled()) {
            log.trace("enter function [table: " + defschema_ + "." + deftable_ + ", db url: "
                    + dburl_ + ", db driver:" + dbdriver_ + ", db user: " + dbuser_
                    + ", commit count:" + commitCount_ + "]");
        }

        dburl = dburl_;
        dbdriver = dbdriver_;
        dbuser = dbuser_;
        dbpassword = dbpassword_;
        defschema = defschema_;
        deftable = deftable_;
        interval = interval_;
        commitCount = commitCount_;
        multiable = multiable_;

        begin = new Date().getTime();
        startTime = new Date();

        tables = new HashMap<String, TableInfo>();

        log.info("start to init schemas");
        init_schemas();

        if (log.isTraceEnabled()) {
            log.trace("exit function");
        }
    }

    private void init_schemas() {
        ResultSet schemaRS = null;
        Connection dbconn_Meta = CreateConnection(true);

        if (log.isTraceEnabled()) {
            log.trace("enter function");
        }

        try {
            DatabaseMetaData dbmd = dbconn_Meta.getMetaData();
            String schemaName = null;

            if (defschema == null) {
                schemaRS = dbmd.getSchemas();
                while (schemaRS.next()) {
                    schemaName = schemaRS.getString("TABLE_SCHEM");
                    log.info("start to init schema [" + schemaName + "]");
                    init_schema(dbconn_Meta, schemaName);
                }
            } else {
                log.info("start to init default schema [" + defschema + "]");
                init_schema(dbconn_Meta, defschema);

                if (tables.size() <= 0) {
                    log.error("init schema [" + defschema + "] fail, cann't find any table!");
                    System.exit(0);
                }
            }
        } catch (SQLException sqle) {
            log.error("SQLException has occurred when init_schemas.",sqle);
        } catch (Exception e) {
            log.error("Exception has occurred when init_schemas.",e);
        } finally {
            CloseConnection(dbconn_Meta);
        }

        if (log.isTraceEnabled()) {
            log.trace("exit function");
        }
    }

    public TableInfo init_schema(Connection dbconn, String schemaName) {
        TableInfo tableInfo = null;

        if (log.isTraceEnabled()) {
            log.trace("enter function [schema: " + schemaName + "]");
        }

        try {
            if (deftable == null) {
                ResultSet tableRS = null;
                DatabaseMetaData dbmd = dbconn.getMetaData();

                tableRS = dbmd.getTables("Trafodion", schemaName, "%", null);
                while (tableRS.next()) {
                    String tableNameStr = tableRS.getString("TABLE_NAME");
                    if (!tableNameStr.equals(NOTINITT1)&&!tableNameStr.equals(NOTINITT2)&&!tableNameStr.equals(NOTINITT3)) {
                        init_tables(tableInfo,dbconn,schemaName,tableNameStr);
                    }
                }
            } else {
		String[] tables= deftable.split(",");
                for (String table : tables) {
                    if (!table.equals(NOTINITT1)&&!table.equals(NOTINITT2)&&!table.equals(NOTINITT3)) {
                        init_tables(tableInfo,dbconn,defschema,table);
                    }
                }
                if (tables.length>1) 
                    deftable=null;
            }
        } catch (SQLException sqle) {
            log.error("SQLException has occurred when init_schema.",sqle);
        } catch (Exception e) {
            log.error("Exception has occurred when init_schema.",e);
        }

        if (log.isTraceEnabled()) {
            log.trace("exit function [table info: " + tableInfo + "]");
        }

        return tableInfo;
    }
	
    public void init_tables(TableInfo tableInfo,Connection dbconn,String schema,String table) {
        if (log.isTraceEnabled()) {
            log.trace("enter function");
        }
        String tableName = schema + "." + table;
        tableInfo = new TableInfo(schema, table, multiable);

        log.info("start to init table [" + tableName + "]");
        if (init_culumns(dbconn, tableInfo) <= 0) {
            log.error("init table [" + tableName + "] is not exist!");
        } else {
            init_keys(dbconn, tableInfo);
            tables.put(tableName, tableInfo);
        }
        if (log.isTraceEnabled()) {
            log.trace("exit function");
        }
    }
    public long init_culumns(Connection dbconn, TableInfo table) {
        if (log.isTraceEnabled()) {
            log.trace("enter function [table info: " + table + "]");
        }

        try {
            String getTableColumns ="SELECT ? TABLE_NAME, c.COLUMN_NAME COLUMN_NAME, "
                    + "c.FS_DATA_TYPE DATA_TYPE, c.SQL_DATA_TYPE TYPE_NAME, "
                    + "c.COLUMN_SIZE COLUMN_SIZE, c.NULLABLE NULLABLE, c.CHARACTER_SET CHARACTER_SET, "
                    + "c.COLUMN_NUMBER ORDINAL_POSITION FROM \"_MD_\".COLUMNS c WHERE "
                    + "c.object_uid=(select object_uid from \"_MD_\".OBJECTS o where o.CATALOG_NAME= "
                    + "'TRAFODION'  AND o.SCHEMA_NAME=? AND o.object_name=?) AND "
                    + "c.COLUMN_CLASS != 'S'  ORDER BY c.COLUMN_NUMBER;" ;
            PreparedStatement psmt = (PreparedStatement) dbconn.prepareStatement(getTableColumns);

            psmt.setString(1, table.GetTableName());
            psmt.setString(2, table.GetSchemaName());
            psmt.setString(3, table.GetTableName());

            ResultSet columnRS = psmt.executeQuery();
            StringBuffer strBuffer = new StringBuffer();
            strBuffer.append("get table \"" + table.GetSchemaName() + "." + table.GetTableName()
                    + "\" column sql \"" + getTableColumns + "\"\n columns [\n");

            int colOff = 0;
            int colId = 0;
            while (columnRS.next()) {
                String colName = columnRS.getString("COLUMN_NAME");
                String typeName = columnRS.getString("TYPE_NAME");
                String colType = columnRS.getString("DATA_TYPE");
                String colSet = columnRS.getString("CHARACTER_SET");
                int colSize = Integer.parseInt(columnRS.getString("COLUMN_SIZE"));

                colId = Integer.parseInt(columnRS.getString("ORDINAL_POSITION"));
                ColumnInfo column =
                        new ColumnInfo(colId, colOff, colSize, colSet, colType, typeName, colName);

                strBuffer.append("\t" + colName + " [id: " + colId + ", off: " + colOff + ", Type: "
                        + typeName.trim() + ", Type ID: " + colType + ", Size: "
                        + column.GetColumnSize() + "]\n");

                table.AddColumn(column);
                colOff++;
            }
            strBuffer.append("]");
            log.debug(strBuffer.toString());
            psmt.close();
        } catch (SQLException sqle) {
            log.error("SQLException has occurred when init_culumns.",sqle);
        } catch (Exception e) {
            log.error("Exception has occurred when init_culumns",e);
        }

        if (log.isTraceEnabled()) {
            log.trace("exit function [column number:" + table.GetColumnCount() + "]");
        }

        return table.GetColumnCount();
    }

    public long init_keys(Connection dbconn, TableInfo table) {
        if (log.isTraceEnabled()) {
            log.trace("enter function [table info: " + table + "]");
        }

        ColumnInfo firstColumn = table.GetColumn(0);
        if (firstColumn.GetColumnID() != 0) {
            log.warn("no primary key on table [" + table.GetSchemaName() + "."
                    + table.GetTableName() + "], use all of columns.");

            for (int i = 0; i < table.GetColumnCount(); i++) {
                table.AddKey(table.GetColumn(i));
            }

            return table.GetKeyCount();
        }

        try {
            String getTableKeys ="SELECT c.FS_DATA_TYPE DATA_TYPE, "
                    + "c.SQL_DATA_TYPE TYPE_NAME, k.KEYSEQ_NUMBER, c.NULLABLE NULLABLE, "
                    + "c.COLUMN_PRECISION DECIMAL_DIGITS,  c.COLUMN_NUMBER KEY_COLUMN_ID, "
                    + "c.COLUMN_NUMBER ORDINAL_POSITION  FROM \"_MD_\".COLUMNS c, \"_MD_\".KEYS k  "
                    + "WHERE c.OBJECT_UID=(select object_uid from \"_MD_\".OBJECTS o where "
                    + "o.CATALOG_NAME= 'TRAFODION'  AND o.SCHEMA_NAME=? AND o.object_name=?) "
                    + "AND c.OBJECT_UID=k.OBJECT_UID  AND c.COLUMN_NUMBER = k.COLUMN_NUMBER AND "
                    + "c.COLUMN_CLASS != 'S' ORDER BY KEYSEQ_NUMBER;";
            PreparedStatement psmt = (PreparedStatement) dbconn.prepareStatement(getTableKeys);

            psmt.setString(1, table.GetSchemaName());
            psmt.setString(2, table.GetTableName());
            ResultSet keysRS = psmt.executeQuery();
            StringBuffer strBuffer = null;
            if (log.isDebugEnabled()) {
                strBuffer = new StringBuffer();
                strBuffer.append("get primakey of \"" + table.GetSchemaName() + "\".\""
                        + table.GetTableName() + "\" key columns\n[\n");
            }
            int colId = 0;
            while (keysRS.next()) {
                colId = Integer.parseInt(keysRS.getString("KEY_COLUMN_ID"));
                ColumnInfo column = table.GetColumnFromMap(colId);
                if (log.isDebugEnabled()) {
                    strBuffer.append("\t" + column.GetColumnName() + " [id: " + column.GetColumnID()
                            + ", Off: " + column.GetColumnOff() + ", Type: " + column.GetTypeName()
                            + ", Type ID: " + column.GetColumnType() + "]\n");
                }

                table.AddKey(column);
            }
            if (log.isDebugEnabled()) {
                strBuffer.append("]\n");
                log.debug(strBuffer.toString());
            }
            psmt.close();
        } catch (SQLException sqle) {
            log.error("SQLException has occurred when init_keys",sqle);
        } catch (Exception e) {
            log.error("Exception has occurred when init_keys",e);
        }

        if (log.isTraceEnabled()) {
            log.trace("exit function [key column number: " + table.GetKeyCount() + "]");
        }

        return table.GetKeyCount();
    }

    public long GetBatchSize() {
        return commitCount;
    }

    public Connection CreateConnection(boolean autocommit) {
        Connection dbConn = null;
        if (log.isTraceEnabled()) {
            log.trace("enter function [autocommit: " + autocommit + "]");
        }

        try {
            Class.forName(dbdriver);
            dbConn = DriverManager.getConnection(dburl, dbuser, dbpassword);
            dbConn.setAutoCommit(autocommit);
        } catch (SQLException se) {
            log.error("SQLException has occurred when CreateConnection:",se);
        } catch (ClassNotFoundException ce) {
            log.error("driver class not found when CreateConnection:: " ,ce);
            System.exit(1);
        } catch (Exception e) {
            log.error("create connect error when CreateConnection:: " , e);
            System.exit(1);
        }

        if (log.isTraceEnabled()) {
            log.trace("exit function");
        }

        return dbConn;
    }

    public void CloseConnection(Connection dbConn_) {
        if (dbConn_ == null)
            return;

        if (log.isTraceEnabled()) {
            log.trace("enter function [db conn: " + dbConn_ + "]");
        }

        try {
            dbConn_.close();
        } catch (SQLException e) {
            log.error("connection close error.",e);
        }

        if (log.isTraceEnabled()) {
            log.trace("exit function");
        }
    }

    public String GetDefaultSchema() {
        return defschema;
    }

    public String GetDefaultTable() {
        return deftable;
    }

    public Connection getSharedConn() {
        return sharedConn;
    }

    public void setSharedConn(Connection sharedConn) {
        this.sharedConn = sharedConn;
    }

    public TableInfo GetTableInfo(String tableName_) {
        return tables.get(tableName_);
    }

    public synchronized void AddInsMsgNum(long insMsgNum_) {
        insMsgNum += insMsgNum_;
        messageNum += insMsgNum_;
    }

    public synchronized void AddUpdMsgNum(long updMsgNum_) {
        updMsgNum += updMsgNum_;
        messageNum += updMsgNum_;
    }

    public synchronized void AddKeyMsgNum(long keyMsgNum_) {
        keyMsgNum += keyMsgNum_;
        messageNum += keyMsgNum_;
    }

    public synchronized void AddDelMsgNum(long delMsgNum_) {
        delMsgNum += delMsgNum_;
        messageNum += delMsgNum_;
    }

    public synchronized void AddInsertNum(long insertNum_) {
        insertNum += insertNum_;
    }

    public synchronized void AddUpdateNum(long updateNum_) {
        updateNum += updateNum_;
    }

    public synchronized void AddDeleteNum(long deleteNum_) {
        deleteNum += deleteNum_;
    }

    public synchronized void AddErrInsertNum(long errInsertNum_) {
        errInsertNum += errInsertNum_;
    }

    public synchronized void AddErrUpdateNum(long errUpdateNum_) {
        errUpdateNum += errUpdateNum_;
    }

    public synchronized void AddErrDeleteNum(long errDeleteNum_) {
        errDeleteNum += errDeleteNum_;
    }

    public synchronized void AddTotalNum(long totalMsgNum_) {
        totalMsgNum += totalMsgNum_;
    }
    public synchronized void AddKafkaPollNum(long kafkaMsgNum_) {
        kafkaMsgNum += kafkaMsgNum_;
    }

    public void DisplayDatabase() {
        Long end = new Date().getTime();
        Date endTime = new Date();
        Float useTime = ((float) (end - begin)) / 1000;
        long avgSpeed = (long) (messageNum / useTime);
        long incMessage = (messageNum - oldMsgNum);
        long curSpeed = (long) (incMessage / (interval / 1000));
        if (curSpeed > maxSpeed)
            maxSpeed = curSpeed;
        DecimalFormat df = new DecimalFormat("####0.000");
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        StringBuffer strBuffer = new StringBuffer();
        strBuffer.append("kafkaPoll states:\n  kafkaPollNum ["+kafkaMsgNum+"]\n");
        strBuffer.append("Consumer messages [ " + totalMsgNum
                + " processed: " + messageNum + " increased: " + incMessage
                + "], speed(n/s) [max: " + maxSpeed + ", avg: " + avgSpeed + ", cur: " + curSpeed
                + "]\n  Run time        [ " + df.format(useTime) + "s, start: " + sdf.format(startTime)
                + ", cur: " + sdf.format(endTime) + "]\n  KafkaTotalMsgs  [I: " + insMsgNum + ", U: " + updMsgNum
                + ", K: " + keyMsgNum + ", D: " + delMsgNum + "] DMLs [insert: " + insertNum + ", update: " + updateNum
                + ", delete: " + deleteNum + "] Fails [insert: " + errInsertNum + ", update: " + errUpdateNum
                + ", delete: " + errDeleteNum + "]\n");
        for (TableInfo tableInfo : tables.values()) {
            tableInfo.DisplayStat(strBuffer);
        }
        log.info(strBuffer.toString());

        oldMsgNum = messageNum;
    }
}
