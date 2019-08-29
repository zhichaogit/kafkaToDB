package com.esgyn.kafkaCDC.server.utils;

import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.lang.StringBuffer;

import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;

import org.apache.log4j.Logger;
import com.esgyn.kafkaCDC.server.utils.Constants;
import com.esgyn.kafkaCDC.server.utils.Parameters;
import com.esgyn.kafkaCDC.server.database.Database;

import lombok.Getter;
import lombok.Setter;

public class DatabaseParams {
    @Setter
    @Getter 
    private long            batchSize   = Constants.DEFAULT_BATCH_SIZE;
    @Setter
    @Getter 
    private boolean         batchUpdate = false;
    @Setter 
    @Getter 
    private long            DBConns     = Constants.DEFAULT_CONNECTIONS;
    @Setter
    @Getter
    private String          DBIP        = "localhost";
    @Setter
    @Getter
    private String          DBPort      = "23400";
    @Setter
    @Getter
    private String          DBType      = "EsgynDB";
    @Setter
    @Getter 
    private String          DBDriver    = Constants.DEFAULT_DRIVER;
    @Setter
    @Getter 
    private String          DBUser      = Constants.DEFAULT_USER;
    @Setter
    @Getter
    private String          DBPassword  = Constants.DEFAULT_PASSWORD;
    @Setter
    @Getter
    private String          DBTenant    = null;
    @Setter
    @Getter 
    private String          defSchema   = null;
    @Setter
    @Getter 
    private String          defTable    = null;
    @Setter 
    @Getter
    private boolean         keepalive   = false;
    @Setter
    @Getter 
    private String          DBUrl       = null;
    @Setter
    @Getter 
    private ArrayList<TableInfo> tables = null;
    @Setter
    @Getter 
    private Map<String, TableInfo> tableHashMap = null;
    @Getter 
    private Parameters      params      = null;

    String                  NOTINITT1   = "SB_HISTOGRAMS";
    String                  NOTINITT2   = "SB_HISTOGRAM_INTERVALS";
    String                  NOTINITT3   = "SB_PERSISTENT_SAMPLES";

    private static Logger   log         = Logger.getLogger(DatabaseParams.class);

    public void init(Parameters params_) {
        DBUrl = "jdbc:t4jdbc://" + DBIP + ":" + DBPort 
	    + "/catelog=Trafodion;applicationName=KafkaCDC;connectionTimeout=0";
        if (DBTenant != null)
            DBUrl += ";tenantName=" + DBTenant;

	params    = params_;
	defSchema = Utils.getTrueName(defSchema);
	defTable  = Utils.getTrueName(defTable);

        tableHashMap = new HashMap<String, TableInfo>();
	log.info("start to init schemas");
	init_schemas();
    }

    public TableInfo getTableInfo(String tableName_) {
        return tableHashMap.get(tableName_);
    }

    private void init_schemas() {
	Database   database  = new Database(this);
        ResultSet schemaRS = null;
        Connection dbConnMD = database.CreateConnection(true);

        if (log.isTraceEnabled()) {
            log.trace("enter");
        }

        try {
            DatabaseMetaData dbmd = dbConnMD.getMetaData();
            String schemaName = null;

            if (defSchema == null) {
                schemaRS = dbmd.getSchemas();
                while (schemaRS.next()) {
                    schemaName = schemaRS.getString("TABLE_SCHEM");
                    log.info("start to init schema [" + schemaName + "]");
                    init_schema(dbConnMD, schemaName);
                }
            } else {
                log.info("start to init default schema [" + defSchema + "]");
                init_schema(dbConnMD, defSchema);

                if (tableHashMap.size() <= 0) {
                    log.error("init schema [" + defSchema + "] fail, cann't find any table!");
                    System.exit(0);
                }
            }
        } catch (SQLException sqle) {
            log.error("SQLException has occurred when init_schemas.",sqle);
        } catch (Exception e) {
            log.error("Exception has occurred when init_schemas.",e);
        } finally {
            database.CloseConnection(dbConnMD);
        }

        if (log.isTraceEnabled()) {
            log.trace("exit");
        }
    }

    public TableInfo init_schema(Connection dbconn, String schemaName) {
        TableInfo tableInfo = null;

        if (log.isTraceEnabled()) {
            log.trace("enter [schema: " + schemaName + "]");
        }

        try {
            if (defTable == null) {
                String getTables ="SELECT OBJECT_NAME TABLE_NAME FROM "
                        + "TRAFODION.\"_MD_\".OBJECTS ob WHERE ob.CATALOG_NAME='TRAFODION' "
                        + "AND ob.SCHEMA_NAME = ? AND OBJECT_TYPE='BT' "
                        + "AND OBJECT_NAME NOT in('"
                        + NOTINITT1 + "','" + NOTINITT2 +"','" + NOTINITT3 + "');";
                PreparedStatement psmt = (PreparedStatement) dbconn.prepareStatement(getTables);

                psmt.setString(1, schemaName);
                ResultSet tableRS = psmt.executeQuery();
                while (tableRS.next()) {
                    String tableNameStr = tableRS.getString("TABLE_NAME");
                    init_table(tableInfo, dbconn, schemaName, tableNameStr);
                }
            } else {
                String[] tableNames= defTable.split(",");
                for (String tableName : tableNames) {
                    init_table(tableInfo, dbconn, defSchema, tableName);
                }
                if (tableNames.length > 1) 
                    defTable = null;
            }
        } catch (SQLException sqle) {
            log.error("SQLException has occurred when init_schema.",sqle);
        } catch (Exception e) {
            log.error("Exception has occurred when init_schema.",e);
        }

        if (log.isTraceEnabled()) {
            log.trace("exit [table info: " + tableInfo + "]");
        }

        return tableInfo;
    }
	
    public void init_table(TableInfo tableInfo, Connection dbconn, String schema, String table) {
        if (log.isTraceEnabled()) { log.trace("enter"); }

        String tableName = schema + "." + table;
        tableInfo = new TableInfo(params, schema, table);

        log.info("start to init table [" + tableName + "]");
        if (init_culumns(dbconn, tableInfo) <= 0) {
            log.error("init table [" + tableName + "] is not exist!");
        } else {
            init_keys(dbconn, tableInfo);
            tableHashMap.put(tableName, tableInfo);
        }

        if (log.isTraceEnabled()) { log.trace("exit"); }
    }

    public long init_culumns(Connection dbconn, TableInfo table) {
        if (log.isTraceEnabled()) { log.trace("enter [table info: " + table + "]"); }

        try {
            String getTableColumns ="SELECT ? TABLE_NAME, c.COLUMN_NAME COLUMN_NAME, "
                    + "c.FS_DATA_TYPE DATA_TYPE, c.SQL_DATA_TYPE TYPE_NAME, "
                    + "c.COLUMN_SIZE COLUMN_SIZE, c.NULLABLE NULLABLE, c.CHARACTER_SET CHARACTER_SET, "
                    + "c.COLUMN_NUMBER ORDINAL_POSITION FROM \"_MD_\".COLUMNS c WHERE "
                    + "c.object_uid=(select object_uid from \"_MD_\".OBJECTS o where o.CATALOG_NAME= "
                    + "'TRAFODION'  AND o.SCHEMA_NAME=? AND o.object_name=?) AND "
                    + "c.COLUMN_CLASS != 'S'  ORDER BY c.COLUMN_NUMBER;" ;
            PreparedStatement psmt = (PreparedStatement) dbconn.prepareStatement(getTableColumns);

            psmt.setString(1, table.getTableName());
            psmt.setString(2, table.getSchemaName());
            psmt.setString(3, table.getTableName());

            ResultSet columnRS = psmt.executeQuery();
            StringBuffer strBuffer = new StringBuffer();
            strBuffer.append("get table \"" + table.getSchemaName() + "." + table.getTableName()
                    + "\" column sql \"" + getTableColumns + "\"\n columns [\n");

            int colOff = 0;
            int colId = 0;
            while (columnRS.next()) {
                String colName = columnRS.getString("COLUMN_NAME");
                String typeName = columnRS.getString("TYPE_NAME");
                int colType = Integer.parseInt(columnRS.getString("DATA_TYPE"));
                String colSet = columnRS.getString("CHARACTER_SET");
                int colSize = Integer.parseInt(columnRS.getString("COLUMN_SIZE"));

                colId = Integer.parseInt(columnRS.getString("ORDINAL_POSITION"));
                ColumnInfo column =
		    new ColumnInfo(colId, colOff, colSize, colSet, colType, typeName, colName);

                strBuffer.append("\t" + colName + " [id: " + colId + ", off: " + colOff + ", Type: "
                        + typeName.trim() + ", Type ID: " + colType + ", Size: "
                        + column.getColumnSize() + "]\n");

                table.addColumn(column);
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
            log.trace("exit [column number:" + table.getColumnCount() + "]");
        }

        return table.getColumnCount();
    }

    public long init_keys(Connection dbconn, TableInfo table) {
        if (log.isTraceEnabled()) { log.trace("enter [table info: " + table + "]"); }

        ColumnInfo firstColumn = table.getColumn(0);
        if (firstColumn.getColumnID() != 0) {
            log.warn("no primary key on table [" + table.getSchemaName() + "."
                    + table.getTableName() + "], use all of columns.");

            for (int i = 0; i < table.getColumnCount(); i++) {
                table.addKey(table.getColumn(i));
            }

            return table.getKeyCount();
        }

        try {
            String getTableKeys ="SELECT c.object_uid,c.FS_DATA_TYPE DATA_TYPE, c.SQL_DATA_TYPE "
                    + "TYPE_NAME, k.KEYSEQ_NUMBER, c.NULLABLE NULLABLE, c.COLUMN_PRECISION "
                    + "DECIMAL_DIGITS,  c.COLUMN_NUMBER KEY_COLUMN_ID, c.COLUMN_NUMBER "
                    + "ORDINAL_POSITION  FROM (select c.*  from \"_MD_\".COLUMNS c  WHERE "
                    + "c.OBJECT_UID=(select object_uid from \"_MD_\".OBJECTS o where "
                    + "o.CATALOG_NAME= 'TRAFODION'  AND o.SCHEMA_NAME=? AND o.object_name=?)) c," 
                    + "(select * from  \"_MD_\".KEYS k where k.OBJECT_UID=(select object_uid from "
                    + "\"_MD_\".OBJECTS o where o.CATALOG_NAME= 'TRAFODION'  AND o.SCHEMA_NAME=? AND "
                    + "o.object_name=?)) k where  c.OBJECT_UID=k.OBJECT_UID  AND c.COLUMN_NUMBER = "
                    + "k.COLUMN_NUMBER AND c.COLUMN_CLASS != 'S' ORDER BY k.KEYSEQ_NUMBER; ";
            PreparedStatement psmt = (PreparedStatement) dbconn.prepareStatement(getTableKeys);

            psmt.setString(1, table.getSchemaName());
            psmt.setString(2, table.getTableName());
            psmt.setString(3, table.getSchemaName());
            psmt.setString(4, table.getTableName());
            ResultSet keysRS = psmt.executeQuery();
            StringBuffer strBuffer = null;
            if (log.isDebugEnabled()) {
                strBuffer = new StringBuffer();
                strBuffer.append("get primakey of \"" + table.getSchemaName() + "\".\""
                        + table.getTableName() + "\" key columns\n[\n");
            }
            int colId = 0;
            while (keysRS.next()) {
                colId = Integer.parseInt(keysRS.getString("KEY_COLUMN_ID"));
                ColumnInfo column = table.getColumnFromMap(colId);
                if (log.isDebugEnabled()) {
                    strBuffer.append("\t" + column.getColumnName() + " [id: " + column.getColumnID()
                            + ", Off: " + column.getColumnOff() + ", Type: " + column.getTypeName()
                            + ", Type ID: " + column.getColumnType() + "]\n");
                }

                table.addKey(column);
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
            log.trace("exit [key column number: " + table.getKeyCount() + "]");
        }

        return table.getKeyCount();
    }

    public void init_json_tables(){
        if (log.isTraceEnabled()) { log.trace("enter"); }

        for (TableInfo table : tables) {
            init_json_table(table);
        }
        if (tables.size() <= 0) {
            log.error("init schema [" + defSchema + "] fail, cann't find any table!");
            System.exit(0);
        }
        if (log.isTraceEnabled()) { log.trace("exit"); }
    }
    public void init_json_table(TableInfo table){
        if (log.isTraceEnabled()) { log.trace("enter"); }

        String tableName = table.getSrcSchemaName() + "." + table.getSrcTableName();
        ArrayList<ColumnInfo> columns = table.getColumns();
        ArrayList<Integer>    keys    = table.getKeys();

        TableInfo tableInfo = new TableInfo(params, table.getSrcSchemaName(), table.getSrcTableName());
        log.info("start to init table [" + tableName + "]");

        if (columns.size()<=0) {
            log.error("init table [" + tableName + "] is not exist!");
        }else {
            init_json_columns(tableInfo,columns);
            init_json_keys(tableInfo,keys);
            tableHashMap.put(tableName, tableInfo);
        }

        if (log.isTraceEnabled()) { log.trace("exit"); }
    }
    public long init_json_columns(TableInfo tableInfo, ArrayList<ColumnInfo> columns){
        if (log.isTraceEnabled()) { log.trace("enter"); }

        StringBuffer strBuffer = null;
        if (log.isDebugEnabled()) {
            strBuffer = new StringBuffer();
            strBuffer.append("\n\tget table [" + tableInfo.getSchemaName() 
			     + "." + tableInfo.getTableName() + "],");
        }
        for (int i = 0; i < columns.size(); i++) {
            String colName   = columns.get(i).getColumnName();
            String typeName  = columns.get(i).getTypeName();
            int    colType   = columns.get(i).getColumnType();
            String colSet    = columns.get(i).getColumnSet();
            int    colSize   = columns.get(i).getColumnSize();
            int    colId     = columns.get(i).getColumnID();
            ColumnInfo column = new ColumnInfo(colId, i, colSize, colSet, 
					       colType, typeName, colName);
            strBuffer.append("\n\tcolName [ " + colName + "], id [" + colId 
			     + "], off [" + i + "], Type [" + typeName.trim()
			     + "], Type ID [" + colType + "], Size [" 
			     + column.getColumnSize() + "]");
            tableInfo.addColumn(column);
        }
        if (log.isDebugEnabled()) {
            log.debug(strBuffer.toString());
        }
        if (log.isTraceEnabled()) {
            log.trace("exit [column number:" + tableInfo.getColumnCount() + "]");
        }
        return tableInfo.getColumnCount();
    }

    public long init_json_keys(TableInfo tableInfo, ArrayList<Integer> keys) {
        if (log.isTraceEnabled()) { log.trace("enter [table info: " + tableInfo + "]"); }
        String tableName=tableInfo.getSchemaName() + "."+ tableInfo.getTableName();
        ColumnInfo firstColumn = tableInfo.getColumn(0);
        if (firstColumn.getColumnID() != 0) {
            log.warn("no primary key on table [" + tableName + "], use all of columns.");

            for (int i = 0; i < tableInfo.getColumnCount(); i++) {
                tableInfo.addKey(tableInfo.getColumn(i));
            }

            return tableInfo.getKeyCount();
        }

        StringBuffer strBuffer = null;
        if (log.isDebugEnabled()) {
            strBuffer = new StringBuffer();
            strBuffer.append("\n\tget primakey of [" + tableName + "],");
        }
        int colId = 0;
        for (int i = 0; i < keys.size(); i++) {
            colId = keys.get(i);
            ColumnInfo column = tableInfo.getColumnFromMap(colId);
            if (log.isDebugEnabled()) {
                strBuffer.append("\n\t key columns [" + column.getColumnName() + "], id [" + column.getColumnID()
                        + "], Off [" + column.getColumnOff() + "], Type [" + column.getTypeName()
                        + "], Type ID [" + column.getColumnType() + "]");
            }
            tableInfo.addKey(column);
        }

        if (log.isDebugEnabled()) {
            log.debug(strBuffer.toString());
        }

        if (log.isTraceEnabled()) {
            log.trace("exit [key column number: " + tableInfo.getKeyCount() + "]");
        }

        return tableInfo.getKeyCount();
    }

    public String toString() {
        StringBuffer strBuffer = new StringBuffer();

        strBuffer.append("\nDatabase options:")
	    .append("\n\tbatchSize     = "    + batchSize)
	    .append("\n\tDBConns       = "    + DBConns)
	    .append("\n\tDBIP          = "    + DBIP)
	    .append("\n\tDBPort        = "    + DBPort)
	    .append("\n\tDBDriver      = "    + DBDriver)
	    .append("\n\tDBUser        = "    + DBUser)
	    .append("\n\tDBPassword    = "    + DBPassword)
	    .append("\n\tDBTenant      = "    + DBTenant)
	    .append("\n\tdefSchema     = "    + defSchema)
	    .append("\n\tdefTable      = "    + defTable)
	    .append("\n\tkeepalive     = "    + keepalive)
	    .append("\n\tdburl         = "    + DBUrl);

	return strBuffer.toString();
    }
}
