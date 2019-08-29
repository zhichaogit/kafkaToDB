package com.esgyn.kafkaCDC.server.database;

import java.sql.Statement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.BatchUpdateException;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.IndexOutOfBoundsException;

import java.util.Map;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.ArrayList;
import org.apache.log4j.Logger;

import com.esgyn.kafkaCDC.server.utils.TableInfo;
import com.esgyn.kafkaCDC.server.utils.ColumnInfo;
import com.esgyn.kafkaCDC.server.kafkaConsumer.messageType.RowMessage;

import lombok.Getter;
import lombok.Setter;

public class TableState {
    @Getter
    private boolean                        commited    = false;
    @Getter
    private String                         schemaName  = null;
    @Getter
    private String                         tableName   = null;
    private int                            retry       = 0;

    // must get it after commited
    @Getter
    private long                           cacheInsert = 0;
    @Getter
    private long                           cacheUpdate = 0;
    @Getter
    private long                           cacheUpdkey = 0;
    @Getter
    private long                           cacheDelete = 0;

    @Getter
    private long                           errInsert   = 0;
    @Getter
    private long                           errUpdate   = 0;
    @Getter
    private long                           errDelete   = 0;
    private boolean                        havePK      = false;

    @Getter
    private TableInfo                      tableInfo   = null;

    private String                         insertSql   = null;
    private String                         updateSql   = null;
    private String                         deleteSql   = null;
    private Statement                      stmt        = null;
    private PreparedStatement              insertStmt  = null;
    private PreparedStatement              deleteStmt  = null;
    private PreparedStatement              updateStmt  = null;
    private final String                   I_OPERATE   = "insert_operate";
    private final String                   U_OPERATE   = "update_operate";
    private final String                   D_OPERATE   = "delete_operate";
    private List<RowMessage>               msgs        = null;
    Map<String, RowMessage>                insertRows  = null;
    Map<String, RowMessage>                updateRows  = null;
    Map<String, RowMessage>                deleteRows  = null;

    ArrayList<ColumnInfo>                  keyColumns  = null;
    ArrayList<ColumnInfo>                  columns     = null;
    Map<Integer, ColumnInfo>               columnMap   = null;

    private static Logger                  log         = Logger.getLogger(TableState.class);

    public long getCacheTotal () {
	return cacheInsert + cacheUpdate + cacheDelete + cacheUpdkey;
    }

    public TableState(TableInfo tableInfo_) {
        tableInfo   = tableInfo_;
        tableName   = tableInfo.getTableName();
        schemaName  = tableInfo.getSchemaName();

        columns     = tableInfo.getColumns();
        columnMap   = tableInfo.getColumnMap();
        keyColumns  = tableInfo.getKeyColumns();
    }

    public boolean init(){
	// TODO, make sure can remove the HongQuan
        if (tableInfo.isRepeatable()) {
            insertRows = new IdentityHashMap<String, RowMessage>(0);
        } else {
            insertRows = new HashMap<String, RowMessage>(0);
        }
        updateRows = new HashMap<String, RowMessage>(0);
        deleteRows = new HashMap<String, RowMessage>(0);
        msgs       = new ArrayList<RowMessage>();

        if (columns.get(0).getColumnID() == 0)
            havePK = true;

	init_insert_stmt();
	init_delete_stmt();
	init_update_stmt();

	return true;
    }

    public void init_insert_stmt() {
        ColumnInfo column = columns.get(0);
        String valueSql = ") VALUES(?";
        
	insertSql = "UPSERT INTO \"" + schemaName + "\"." + "\"" + tableName
	    + "\"" + "(" + column.getColumnName();

        for (int i = 1; i < columns.size(); i++) {
            column = columns.get(i);
            valueSql += ", ?";
            insertSql += ", " + column.getColumnName();
        }

        insertSql += valueSql + ");";

        if (log.isDebugEnabled()) {
	    log.debug("insert prepare statement [" + insertSql + "]");
	}
    }

    public String where_condition() {
        ColumnInfo keyInfo = keyColumns.get(0);
        String whereSql = " WHERE " + keyInfo.getColumnName() + " = ?";

        for (int i = 1; i < keyColumns.size(); i++) {
            keyInfo = keyColumns.get(i);
            whereSql += " AND " + keyInfo.getColumnName() + " = ?";
        }
        return whereSql;
    }

    public void init_update_stmt() {
        ColumnInfo column = columns.get(0);

        updateSql = "update \"" + schemaName + "\"." + "\"" + tableName + "\"" + " SET "+
	    column.getColumnName() + "= ?";

        for (int i = 1; i < columns.size(); i++) {
            column = columns.get(i);
            updateSql += ", " + column.getColumnName() + " = ? ";
        }
        updateSql += where_condition() + ";";

        if (log.isDebugEnabled()) {
            log.debug("update prepare statement [" + updateSql + "]");
        }
    }

    public void init_delete_stmt() {
        ColumnInfo keyInfo = keyColumns.get(0);
        
	deleteSql = "DELETE FROM \"" + schemaName + "\"." + "\"" + tableName + "\""
	    + where_condition() + ";";

        if (log.isDebugEnabled()) {
            log.debug("delete prepare statement [" + deleteSql + "]");
        }
    }

    private String get_key_value(String message, Map<Integer, ColumnValue> rowValues, boolean cur) {
        String key = null;

        if (rowValues == null)
            return key;

        for (int i = 0; i < keyColumns.size(); i++) {
            ColumnInfo keyInfo = keyColumns.get(i);
            ColumnValue column = rowValues.get(keyInfo.getColumnOff());

            if (column == null)
                continue;

            if (log.isDebugEnabled()) {
                log.debug("key id: " + i + ", type: " + cur + " column [id: " + column.getColumnID()
                        + ", cur value: " + column.getCurValue() + ", old value: "
                        + column.getOldValue() + "] cur is null: [" + column.curValueIsNull()
                        + "] old is null: [" + column.oldValueIsNull() + "]");
            }

            if (cur) {
                if (column.curValueIsNull()) {
                    if (havePK) {
                        log.error("the cur primary key value is null. column name ["
                                + keyInfo.getColumnName() + "] message [" + message + "]");
                        return null;
                    } else {
                        if (key == null) {
                            key = "";
                        } else {
                            key += "";
                        }
                    }
                } else {
                    /*
                     * column is splited by "", in order to support key1:["aa", "a"] is different
                     * with key2:["a", "aa"];
                     */
                    if (key == null) {
                        key = column.getCurValue();
                    } else {
                        key += "" + column.getCurValue();
                    }
                }
            } else {
                if (column.oldValueIsNull()) {
                    if (havePK) {
                        log.error("the old primary key value is null. column name ["
                                + keyInfo.getColumnName() + "] message [" + message + "]");
                        return null;
                    } else {
                        if (key == null) {
                            key = "";
                        } else {
                            key += "";
                        }
                    }
                } else {
                    /*
                     * column is splited by "", in order to support key1:["aa", "a"] is different
                     * with key2:["a", "aa"];
                     */
                    if (key == null) {
                        key = column.getOldValue();
                    } else {
                        key += "" + column.getOldValue();
                    }
                }
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("key string: [" + key + "]");
        }

        return key;
    }

    public long insertRow(RowMessage rowMessage) {
        if (log.isTraceEnabled()) { log.trace("enter"); }

        Map<Integer, ColumnValue> rowValues = rowMessage.getColumns();

        String message = rowMessage.getMessage();
        String key = get_key_value(message, rowValues, true);

        if (log.isDebugEnabled()) {
            log.debug("insert row key [" + key + "], message [" + message + "]");
        }

        if (key == null)
            return 0;

        // new row is inserted
        insertRows.put(key, rowMessage);

        // remove the update messages
        updateRows.remove(key);

        // remove the delete messages
        deleteRows.remove(key);

        cacheInsert++;

        if (log.isTraceEnabled()) {
            log.trace("exit cache insert [rows: " + cacheInsert + ", insert: "
                    + insertRows.size() + ", update: " + updateRows.size() + ", delete: "
                    + deleteRows.size() + "]");
        }

        return 1;
    }

    public long check_update_key(Map<Integer, ColumnValue> rowValues, String message) {
        ColumnValue cacheValue = null;

        for (int i = 0; i < keyColumns.size(); i++) {
            ColumnInfo keyInfo = keyColumns.get(i);
            cacheValue = rowValues.get(keyInfo.getColumnOff());
            if (cacheValue == null)
                continue;

            String oldValue = cacheValue.getOldValue();
            String curValue = cacheValue.getCurValue();
            if (log.isDebugEnabled()) {
                log.debug("update the keys [" + oldValue + "] to [" + curValue + "]");
            }

            if (havePK && !curValue.equals(oldValue)) {
                log.error("U message cann't update the keys,"
                        + "tablename ["+schemaName+"."+tableName+"],curValue ["+curValue+"],oldValue "
                        + "["+oldValue+"] ,keyCol:"+keyInfo.getColumnName()+"message [" + message + "]\n");
                return 0;
            }
        }

        return 1;
    }

    public long updateRow(RowMessage rowMessage) {
        if (log.isTraceEnabled()) { log.trace("enter"); }

        Map<Integer, ColumnValue> rowValues = rowMessage.getColumns();

        String message = rowMessage.getMessage();
	// TODO if target changed the primary key, we cann't check the update keys
        if ((check_update_key(rowValues, message) == 0))
            return 0;

        String key = get_key_value(message, rowValues, false);

        if (log.isDebugEnabled()) {
            log.debug("update row key [" + key + "], message [" + message + "]");
        }

        if (key == null)
            return 0;

        RowMessage insertRM = insertRows.get(key);

        if (insertRM != null) {
            Map<Integer, ColumnValue> insertRow = insertRM.getColumns();
            if (insertRow != null) {
            /*
             * exist in insert map, must be not exist in update and delete update the insert row in
             * memory
             */
                for (ColumnValue value : rowValues.values()) {
                    insertRow.put(value.getColumnID(), value);
                }
                if (log.isDebugEnabled()) {
                    log.debug("update row key [" + key + "] is exist in insert cache");
                }
                insertRM.setColumns(insertRow);
                insertRows.put(key, insertRM);
            }
        } else {
	    if (log.isDebugEnabled()) {
                log.trace("the key ["+key+"] not exist in insertRows");
            }
            RowMessage deleteRM = deleteRows.get(key);
            
            if (deleteRM != null) {
                Map<Integer, ColumnValue> deleterow = deleteRM.getColumns();
                if (deleterow != null) {
                    if (log.isDebugEnabled()) {
                        log.error("update row key is exist in delete cache [" + key + "]," 
                            +"the message ["+ message + "]");
                    }
                    return 0;
              }
            } else {
                if (log.isDebugEnabled()) {
                    log.trace("update row key is not exist in delete cache ");
                }
                RowMessage updateRM = updateRows.get(key);
                if (updateRM != null) {
                    if (log.isDebugEnabled()) {
                        log.trace("the key ["+key+"] exist in updateRows");
                    }
                    Map<Integer, ColumnValue> updateRow = updateRM.getColumns();
                    if (updateRow != null) {
                        if (log.isDebugEnabled()) {
                             log.debug("update row key is exist in update cache [" + key + "],"
                                    +" the message ["+ message + "]");
                        }

                        ColumnValue cacheValue;

                        for (ColumnValue value : updateRow.values()) {
                            cacheValue = rowValues.get(value.getColumnID());
                            if (cacheValue != null) {
                                value = new ColumnValue(cacheValue.getColumnID(),
                                        cacheValue.getCurValue(), value.getOldValue(),
                                        tableInfo.getColumn(cacheValue.getColumnID()).getTypeName());
                            }

                            rowValues.put(value.getColumnID(), value);
                        }
			rowMessage.setColumns(rowValues);
                    }
                }

                updateRows.put(key, rowMessage);
            }
        }

        cacheUpdate++;

        if (log.isTraceEnabled()) {
            log.trace("exit cache update [rows: " + cacheUpdate + ", insert: "
                    + insertRows.size() + ", update: " + updateRows.size() + ", delete: "
                    + deleteRows.size() + "]");
        }

        return 1;
    }

    public long updateRowWithKey(RowMessage rowMessage) {
        if (log.isTraceEnabled()) { log.trace("enter"); }

        Map<Integer, ColumnValue> rowValues = rowMessage.getColumns();
        String message = rowMessage.getMessage();
        String oldkey = get_key_value(message, rowValues, false);
        String newkey = get_key_value(message, rowValues, true);

        if (log.isDebugEnabled()) {
            log.debug("updkey row key [old key: " + oldkey + ", new key: " + newkey + "], "
                    + "message [" + message + "]");
        }

        if (oldkey == null || newkey == null)
            return 0;

        RowMessage insertRM = insertRows.get(newkey);

        if (insertRM != null) {
            Map<Integer, ColumnValue> insertRow = insertRM.getColumns();
            /*
             * exist in insert map, must be not exist in update and delete update the insert row in
             * memory
             */
            if (insertRow != null) {
                if (log.isDebugEnabled()) {
                    log.error("updkey row key is exist in insert cache,newkey [" + newkey + "],"
                        +"the message ["+ message + "]");
                }
                return 0;
            }
        } else {
	    if (log.isTraceEnabled()) {
                log.trace("the newkey ["+newkey+"] not exist in insertRows");
            }
            RowMessage deleteRM = deleteRows.get(oldkey);

            if (deleteRM != null) {
                Map<Integer, ColumnValue> deleterow = deleteRM.getColumns();
                if (deleterow != null) {
                    if (log.isDebugEnabled()) {
                        log.error("update row key is exist in delete cache [" + oldkey + "]," 
                            +"the message ["+ message + "]");
                    }
                    return 0;
                }
            } else {
                if (log.isTraceEnabled()) {
                    log.trace("update row key is not exist in delete cache ");
                }
                RowMessage updateRM = updateRows.get(oldkey);
                Map<Integer, ColumnValue> updateRow = null;
                if (updateRM != null) {
                    updateRow = updateRM.getColumns();
                    if (log.isDebugEnabled()) {
                        log.debug("updkey row [key: " + oldkey + "] in update cache [" + updateRow + "]");
                    }
                    if (updateRow != null) {
                        ColumnValue cacheValue;

                        for (ColumnValue value : rowValues.values()) {
                            cacheValue = updateRow.get(value.getColumnID());
                            if (cacheValue != null) {
                                value = new ColumnValue(value.getColumnID(), value.getCurValue(),
                                        cacheValue.getOldValue(),
                                        tableInfo.getColumn(cacheValue.getColumnID()).getTypeName());
                                updateRow.put(value.getColumnID(), value);
                            } else {
                                updateRow.put(value.getColumnID(), new ColumnValue(value));
                            }
                        }
                        updateRM.setColumns(updateRow);
                        // delete the old update message
                        updateRows.remove(oldkey);
                    }
                }else {
                    updateRow = new HashMap<Integer, ColumnValue>(0);
                    for (ColumnValue value : rowValues.values()) {
                        updateRow.put(value.getColumnID(), new ColumnValue(value));
                    }
                    updateRM = rowMessage;
                    updateRM.setColumns(updateRow);
                }
                // add new insert message
                updateRows.put(newkey, updateRM);
            }
        }
        cacheUpdkey++;

        if (log.isTraceEnabled()) {
            log.trace("exit cache updateKey [rows: " + cacheUpdkey + ", insert: "
                    + insertRows.size() + ", update: " + updateRows.size() + ", delete: "
                    + deleteRows.size() + "]");
        }
        return 1;
    }

    public long updateRowWithKeySplit(RowMessage rowMessage) {
        if (log.isTraceEnabled()) { log.trace("enter"); }

        Map<Integer, ColumnValue> rowValues = rowMessage.getColumns();
        String message = rowMessage.getMessage();
        String oldkey = get_key_value(message, rowValues, false);
        String newkey = get_key_value(message, rowValues, true);

        if (log.isDebugEnabled()) {
            log.debug("updkey row key [old key: " + oldkey + ", new key: " + newkey + "], "
                    + "message [" + message + "]");
        }

        if (oldkey == null || newkey == null)
            return 0;

        RowMessage insertRM = insertRows.get(oldkey);
        if (insertRM !=null) {
            Map<Integer, ColumnValue> insertRow = insertRM.getColumns();
            if (insertRow != null) {
                /*
                 * exist in insert map, must be not exist in update and delete update the 
                 * insert row in memory
                 */
                for (ColumnValue value : rowValues.values()) {
                    insertRow.put(value.getColumnID(), new ColumnValue(value));
                }

                if (log.isDebugEnabled()) {
                    log.debug("updkey row key [" + oldkey + "] exist in insert cache");
                }

                // remove old key
                insertRows.remove(oldkey);

                // insert the new key
                insertRM.setColumns(insertRow);
                insertRows.put(newkey, insertRM);

                // delete the old key on disk
                if (!oldkey.equals(newkey))
                deleteRows.put(oldkey, rowMessage);
            }
        } else {
            RowMessage deleteRM = deleteRows.get(oldkey);
            RowMessage updateRM = updateRows.get(oldkey);
            if (deleteRM != null && updateRM==null) {
            Map<Integer, ColumnValue> deleterow = deleteRM.getColumns();
                if (deleterow != null) {
                    if (log.isDebugEnabled()) {
                        log.error("update row key is exist in delete cache [" + oldkey
                                + "], the message [" + message + "]");
                    }
                    return 0;
                }
            } else {
                Map<Integer, ColumnValue> updateRow = null;
                if (updateRM != null) {
                    updateRow = updateRM.getColumns();
                    if (log.isDebugEnabled()) {
                        log.debug(
                            "updkey row [key: " + oldkey + "] in update cache [" + updateRow + "]");
                    }
                    if (updateRow != null) {
                        ColumnValue cacheValue;

                        for (ColumnValue value : rowValues.values()) {
                            cacheValue = updateRow.get(value.getColumnID());
                            if (cacheValue != null) {
                                value = new ColumnValue(value.getColumnID(), value.getCurValue(),
                                        cacheValue.getOldValue(),
                                        tableInfo.getColumn(cacheValue.getColumnID()).getTypeName());
                                updateRow.put(value.getColumnID(), value);
                            } else {
                                updateRow.put(value.getColumnID(), new ColumnValue(value));
                            }
                        }
                        // delete the old update message
                        updateRows.remove(oldkey);
                    }
                } else {
                    updateRow = new HashMap<Integer, ColumnValue>(0);
                    for (ColumnValue value : rowValues.values()) {
                        updateRow.put(value.getColumnID(), new ColumnValue(value));
                    }
                    updateRM = rowMessage;
                    updateRM.setColumns(updateRow);
                }

                // add new insert message
                updateRows.put(newkey, updateRM);

                // delete the data on the disk
                if (!oldkey.equals(newkey)){
                    rowMessage.setColumns(rowValues);
                    deleteRows.put(oldkey, rowMessage);
                }
            }
        }

        cacheUpdkey++;
        if (log.isTraceEnabled()) {
            log.trace("exit cache updkey [rows: " + cacheUpdkey + ", insert: "
                    + insertRows.size() + ", update: " + updateRows.size() + ", delete: "
                    + deleteRows.size() + "]");
        }

        return 1;
    }

    public long deleteRow(RowMessage rowMessage) {
        if (log.isTraceEnabled()) {
            log.trace("exit");
        }

        Map<Integer, ColumnValue> rowValues = rowMessage.getColumns();
        String message = rowMessage.getMessage();
        String key = get_key_value(message, rowValues, false);

        // delete cur row
        if (log.isDebugEnabled()) {
            log.debug("delete row key [" + key + "], message [" + message + "]");
        }

        if (key == null)
            return 0;

        deleteRows.put(key, rowMessage);

        // remove the insert messages
        insertRows.remove(key);

        // remove the update messages
        updateRows.remove(key);

        cacheDelete++;

        if (log.isTraceEnabled()) {
            log.trace("exit cache delete [rows: " + cacheDelete + ", insert: "
                    + insertRows.size() + ", update: " + updateRows.size() + ", delete: "
                    + deleteRows.size() + "]");
        }

        return 1;
    }

    private void prepareStmts(Connection dbConn_) throws SQLException {
        if (log.isTraceEnabled()) {
            log.trace("enter, conn: " + dbConn_);
	}

	if (insertStmt == null) {
	    insertStmt = dbConn_.prepareStatement(insertSql);
	}

	if (updateStmt == null) {
	    updateStmt = dbConn_.prepareStatement(updateSql);
	}

	if (deleteStmt == null) {
	    deleteStmt = dbConn_.prepareStatement(deleteSql);
	}

	if (stmt == null) {
	    stmt = dbConn_.createStatement();
	}
    }

    private void closeStmts() {
	try {
	    if (insertStmt != null) {
		insertStmt.close();
	    }

	    if (updateStmt != null) {
		updateStmt.close();
	    }

	    if (deleteStmt != null) {
		deleteStmt.close();
	    }

	    if (stmt != null) {
		stmt.close();
	    }
	} catch (Exception e) {
	    log.error("there are exception generated when close stmt, the details:" + e.getMessage());
	} finally {
	    insertStmt = null;
	    updateStmt = null;
	    deleteStmt = null;
	    stmt       = null;
	}
    }

    // TODO support output error message
    public void commitTable(Connection dbConn_) throws SQLException {
	String outPutPath_= null;
        if (log.isTraceEnabled()) {
            log.trace("commit table [" + schemaName + "." + tableName + ", insert: "
		      + insertRows.size() + ", update: " + updateRows.size() + ", delete: "
		      + deleteRows.size() + "] ");
        }

	// if reconnect to database, we must prepare the stmt again
	prepareStmts(dbConn_);

	if (insertRows.size() > 0) {
	    insert_data();
	}

        if (updateRows.size() > 0) {
	    update_data();
	}

        if (deleteRows.size() <= 0) {
	    delete_data();
	}

	dbConn_.commit();
	commited = true;
    }


    public long getInsertRows() {
        return commited ? insertRows.size() : 0;
    }

    public long getUpdateRows() {
        return commited ? updateRows.size() : 0;
    }

    public long getDeleteRows() {
        return commited ? deleteRows.size() : 0;
    }

    public void clearCache() {
        if (commited) {
            tableInfo.incInsertRows(insertRows.size());
            tableInfo.incUpdateRows(updateRows.size());
            tableInfo.incDeleteRows(deleteRows.size());

            tableInfo.incInsMsgNum(cacheInsert);
            tableInfo.incUpdMsgNum(cacheUpdate);
            tableInfo.incKeyMsgNum(cacheUpdkey);
            tableInfo.incDelMsgNum(cacheDelete);
        }

	tableInfo.incErrInsNum(errInsert);
	tableInfo.incErrUpdNum(errUpdate);
	tableInfo.incDelMsgNum(errDelete);

        insertRows.clear();
        updateRows.clear();
        deleteRows.clear();

        cacheInsert = 0;
        cacheUpdate = 0;
        cacheUpdkey = 0;
        cacheDelete = 0;

        errInsert = 0;
        errUpdate = 0;
        errDelete = 0;
    }

    private long insert_row_data(Map<Integer, ColumnValue> row,int offset) throws SQLException {
        long result = 1;
        StringBuffer strBuffer=null;

        if (log.isTraceEnabled()) {
            log.trace("enter, offset [" + offset + "]");
        }

        if (log.isDebugEnabled()) {
            strBuffer = new StringBuffer();
        }

        for (int i = 0; i < columns.size(); i++) {
            ColumnInfo columnInfo = columns.get(i);
            ColumnValue columnValue = row.get(columnInfo.getColumnOff());
            if (columnValue == null || columnValue.curValueIsNull()) {
                if (log.isDebugEnabled()) {
                    strBuffer.append("\tcolumn: " + i + " [null]\n");
                }
                insertStmt.setNull(i + 1, columnInfo.getColumnType());
            } else {
                if (log.isDebugEnabled()) {
                    strBuffer.append("\tcolumn: " + i + ", value [" + columnValue.getCurValue() 
                                     + "]\n");
                }
                try {
                    insertStmt.setString(i + 1, columnValue.getCurValue());
                } catch (SQLException se) {
                    if (isNotDisConnAndTOExpection(se)) {
                        log.error("\nThere is a error when set value to insertStmt, the paramInt,["
                                +(i+1)+","+columnValue.getCurValue()+"]");
                    }
                    throw se;
                }catch (Exception e) {
                    log.error("\nThere is a error when set value to insertStmt, the paramInt,["
                            +(i+1)+","+columnValue.getCurValue()+"]");
                }
            }
        }

        if (result == 1) {
            insertStmt.addBatch();
        }

        if (log.isDebugEnabled()) {
            log.debug(strBuffer.toString());
        }
        if (log.isTraceEnabled()) {
            log.trace("exit, offset [" + offset + "], insert row: [" + result + "]");
        }

        return result;
    }

    private void insert_data() throws SQLException {
        if (log.isTraceEnabled()) { log.trace("enter"); }

        if (log.isDebugEnabled()) {
            log.debug("insert rows [cache row: " + cacheInsert + ", cache: " +
                    + insertRows.size() + "]\n");
        }

        int offset = 0;
        Map<Integer, RowMessage> errRows = new HashMap<Integer, RowMessage>(0);
        for (RowMessage insertRM : insertRows.values()) {
            Map<Integer, ColumnValue> cols = insertRM.getColumns();
            if (log.isDebugEnabled()) {
                log.debug("insert row offset: " + offset+ ",rowmessage:"
                        +cols.get(0).getCurValue()+"\n");
            }
            try {
                insert_row_data(cols, offset);
            }catch (SQLException se) {
                if (isNotDisConnAndTOExpection(se)) {
                    matchErr(insertRM);
                    errInsert++;
                }
                throw se;
            }
            errRows.put(offset, insertRM);
            offset++;
        }

        try {
            insertStmt.executeBatch();
        } catch (BatchUpdateException bue) {
            //throw the ERROR[8734] Statement must be recompiled to allow privileges to be re-evaluated
            //throw the ERROR[8738] Statement must be recompiled due to redefinition of the object(s) accessed
            SQLException se = bue;
            do {
                if (se !=null && (se.getErrorCode()==-8734 ||se.getErrorCode()==-8738)) {
                    throw bue;
                }
                se = se.getNextException();
            } while (se != null);
            // print the error data 
            int[] insertCounts = bue.getUpdateCounts();
            printBatchErrorMsg(insertCounts,errRows,I_OPERATE);
            throw bue;
        } catch (IndexOutOfBoundsException iobe) {
           errInsert++;
           throw iobe;
        }catch (SQLException se) {
            if (isNotDisConnAndTOExpection(se)) {
                errInsert++;
            }
            throw se;
        }finally {
            errRows.clear();
        }

        if (log.isTraceEnabled()) { log.trace("exit"); }
    }

    private long update_row_data(Map<Integer, ColumnValue> row,int offset) throws SQLException  {
        long result = 1;
        StringBuffer strBuffer = null;

        if (log.isTraceEnabled()) {
            log.trace("enter, offset [" + offset + "]");
        }
        if (log.isDebugEnabled()) {
            strBuffer = new StringBuffer();
        }

        ColumnInfo columnInfo = null;
        String updateSql = "UPDATE \"" + schemaName + "\"." + "\"" + tableName + "\"" + " SET ";
        ColumnInfo keyInfo = null;
        ColumnValue keyValue = null;
        String whereSql = null;

        if (tableInfo.getParams().getDatabase().isBatchUpdate()) {
            //set col values
            for (int i = 0; i < columns.size(); i++) {
                columnInfo = columns.get(i);
                ColumnValue columnValue = row.get(columnInfo.getColumnOff());
                if (columnValue == null || columnValue.curValueIsNull()) {
                    if (log.isDebugEnabled()) {
                        strBuffer.append("\tcolumn: " + i + " [null]\n");
                    }
		    updateStmt.setNull(i + 1, columnInfo.getColumnType());
                } else {
                    if (log.isDebugEnabled()) {
                        strBuffer.append("\tcolumn: " + i + ", value [" + columnValue.getCurValue()
                                         + "]\n");
                    }
                    try {
                        updateStmt.setString(i + 1, columnValue.getCurValue());
                    } catch (SQLException se) {
                        if (isNotDisConnAndTOExpection(se)) {
                            log.error("\nThere is a error when set value to updateStmt, the paramInt,["
                                    +(i+1)+","+columnValue.getCurValue()+"]");
                        }
                        throw se;
                    }catch (Exception e) {
                        log.error("\nThere is a error when set value to updateStmt, the paramInt,["
                                +(i+1)+","+columnValue.getCurValue()+"]");
                        throw e;
                    }
                }
            }
            //set key values
            for (int i = 0; i < keyColumns.size(); i++) {
                keyInfo = keyColumns.get(i);
                keyValue = row.get(keyInfo.getColumnOff());

                if (keyValue == null || keyValue.oldValueIsNull()) {
                    if (havePK) {
                        String key = get_key_value(null, row, false);
                        log.error("the primary key value is null [table:" + schemaName + "." + tableName
                                + ", column:" + keyInfo.getColumnName() + "]");
                        result = 0;
                        break;
                    }
                    updateStmt.setNull(columns.size()+ (i + 1), keyInfo.getColumnType());
                } else {
                    if (log.isDebugEnabled()) {
                        strBuffer.append("\tkey id:" + i + ", column id:" + keyInfo.getColumnOff()
                                + ", key [" + keyValue.getOldValue() + "]");
                    }
                    try {
                        updateStmt.setString(columns.size()+ (i + 1), keyValue.getOldValue());
                    } catch (SQLException se) {
                        if (isNotDisConnAndTOExpection(se)) {
                            log.error("\nThere is a error when set value to updateStmt,the errorCode["
                                    +se.getErrorCode()+"],the paramInt,["+(i+1)+","+keyValue.getOldValue()+"]");
                        }
                        throw se;
                    }
                }
            }

            if (result == 1) {
                updateStmt.addBatch();
            }
            if (log.isDebugEnabled()) {
                log.debug(strBuffer.toString());
            }
            if (log.isTraceEnabled()) {
                log.trace("exit,offset:[" + offset + "], insert row: [" + result + "]");
            }

            return result;
        }

	//one by one update if  format is not protobuf
        for (int i = 0; i < keyColumns.size(); i++) {
            keyInfo = keyColumns.get(i);
            keyValue = row.get(keyInfo.getColumnOff());
            if (keyValue == null)
                continue;

            if (whereSql == null) {
                whereSql = " WHERE ";
            } else {
                whereSql += " AND ";
            }

            whereSql += keyInfo.getColumnName() + keyValue.getOldCondStr();
        }

        for (ColumnValue columnValue : row.values()) {
            if (columnInfo != null)
                updateSql += ", ";

            columnInfo = columns.get(columnValue.getColumnID());
            updateSql += columnInfo.getColumnName() + " = " + columnValue.getCurValueStr();
            if (log.isDebugEnabled()) {
                strBuffer.append("\tcolumn: " + columnInfo.getColumnOff() + ", value ["
                        + columnValue.getCurValue() + "]\n");
            }
        }

        updateSql += whereSql + ";";

        if (log.isDebugEnabled()) {
            strBuffer.append("\tupdate sql [" + updateSql + "]\n");
            log.debug(strBuffer.toString());
        }

        try {
            stmt.executeUpdate(updateSql);
        } catch (SQLException se) {
            log.error("\nthere is a error when execute the update sql,the execute sql:["+updateSql+"]"
                    + ",the coltype is ["+columnInfo.getTypeName()+"]");
            throw se;
        }

        result = 1;

        if (log.isTraceEnabled()) {
            log.trace("exit, offset [" + offset + "], insert row: [" + result + "]");
        }

        return result;
    }

    public void update_data() throws SQLException  {
        if (log.isTraceEnabled()) { log.trace("enter"); }

        if (log.isDebugEnabled()) {
            log.debug("update rows [cache update row: " + cacheUpdate + ", cache updkey row: "
                    + cacheUpdkey + ", cache: " + updateRows.size() + "]");
        }

        int offset = 0;
        Map<Integer, RowMessage> errRows = new HashMap<Integer, RowMessage>(0);
        for (RowMessage updateRow : updateRows.values()) {
            if (log.isDebugEnabled()) {
                log.debug("update row offset: " + offset + "\n");
            }
            errRows.put(offset, updateRow);
            if (updateRow != null) {
                try {
                    update_row_data(updateRow.getColumns(),offset);
                } catch (SQLException se) {
                    if (isNotDisConnAndTOExpection(se)) {
                        matchErr(updateRow);
                        errUpdate++;
                    }
                    throw se;
                }
                offset++;
            }
        }

	try {
	    updateStmt.executeBatch();
	} catch (BatchUpdateException bue) {
	    //throw the ERROR[8734] Statement must be recompiled to allow privileges to be re-evaluated
	    //throw the ERROR[8738] Statement must be recompiled due to redefinition of the object(s) accessed.
	    SQLException se = bue;
	    do {
		if (se !=null && (se.getErrorCode()==-8734 || se.getErrorCode()==-8738)) {
		    throw bue;
		}
		se = se.getNextException();
	    } while (se != null);
	    // print the error data
	    int[] updateCounts = bue.getUpdateCounts();
	    printBatchErrorMsg(updateCounts, errRows, U_OPERATE);
	    throw bue;
	} catch (IndexOutOfBoundsException iobe) {
	    errUpdate++;
	    throw iobe;
	}catch (SQLException se) {
	    if (isNotDisConnAndTOExpection(se)) {
		errUpdate++;
	    }
	    throw se;
	}finally {
	    errRows.clear();
	}

        if (log.isTraceEnabled()) { log.trace("exit"); }
    }

    private long delete_row_data(Map<Integer, ColumnValue> row,int offset) throws SQLException {
        long result = 1;
        StringBuffer strBuffer = null;

        if (log.isTraceEnabled()) { log.trace("enter, offset:[" + offset +"]"); }

        if (log.isDebugEnabled()) {
            strBuffer = new StringBuffer();
        }
        for (int i = 0; i < keyColumns.size(); i++) {
            ColumnInfo keyInfo = keyColumns.get(i);
            ColumnValue keyValue = row.get(keyInfo.getColumnOff());

            if (keyValue == null || keyValue.oldValueIsNull()) {
                if (havePK) {
                    String key = get_key_value(null, row, false);
                    log.error("the primary key value is null [table:" + schemaName + "." + tableName
                            + ", column:" + keyInfo.getColumnName() + "]");
                    result = 0;
                    break;
                }
                deleteStmt.setNull(i + 1, keyInfo.getColumnType());
            } else {
                if (log.isDebugEnabled()) {
                    strBuffer.append("\tkey id:" + i + ", column id:" + keyInfo.getColumnOff() 
                            + ", key [" + keyValue.getOldValue() + "]");
                }
                try {
                    deleteStmt.setString(i + 1, keyValue.getOldValue());
                } catch (SQLException se) {
                    if (isNotDisConnAndTOExpection(se)) {
                        log.error("\nThere is a error when set value to deleteStmt,the errorCode["
                                +se.getErrorCode()+"],the paramInt,["+(i+1)+","+keyValue.getOldValue()+"]");
                    }
                    throw se;
                }
            }
        }

        if (result == 1) {
            deleteStmt.addBatch();
        }

        if (log.isDebugEnabled()) {
            log.debug(strBuffer.toString());
        }
        if (log.isTraceEnabled()) {
            log.trace("enter, offset:["+ offset +"], delete row [" + result + "]");
        }

        return result;
    }

    private void delete_data() throws SQLException {
        if (log.isTraceEnabled()) { log.trace("enter"); }

        if (log.isDebugEnabled()) {
            log.debug("delete rows [cache row: " + cacheDelete + ", cache: " + deleteRows.size()
                    + "]");
        }

        int offset = 0;
        Map<Integer, RowMessage> errRows = new HashMap<Integer, RowMessage>(0);
        for (RowMessage deleteRow : deleteRows.values()) {
            if (log.isDebugEnabled()) {
                log.debug("delete row offset: " + offset + "\n");
            }
            errRows.put(offset, deleteRow);
            try {
                delete_row_data(deleteRow.getColumns(),offset);
            }catch (SQLException se) {
                if (isNotDisConnAndTOExpection(se)) {
                    matchErr(deleteRow);
                    errDelete++;
                }
            }
            offset++;
        }

        try {
            int[] batchResult = deleteStmt.executeBatch();
        } catch (BatchUpdateException bue) {
          //throw the ERROR[8734] Statement must be recompiled to allow privileges to be re-evaluated
          //throw the ERROR[8738] Statement must be recompiled due to redefinition of the object(s) accessed.
            SQLException se = bue;
            do {
                if (se !=null && (se.getErrorCode()==-8734 || se.getErrorCode()==-8738)) {
                    throw bue;
                }
                se = se.getNextException();
            } while (se != null);
            // print the error data 
            int[] deleteCounts = bue.getUpdateCounts();
            printBatchErrorMsg(deleteCounts,errRows,D_OPERATE);
            throw bue;
        }catch (IndexOutOfBoundsException iobe) {
             errDelete++;
           throw iobe;
        }catch (SQLException se) {
            if (isNotDisConnAndTOExpection(se)) {
                errDelete++;
            }
            throw se;
        }finally {
            errRows.clear();
        }

        if (log.isTraceEnabled()) { log.trace("exit"); }
    }

    public void matchErr(RowMessage operateRM) {
        if (operateRM != null) {
	    log.error(operateRM.getErrorMsg());
	}
    }

    public void AddColumn(ColumnInfo column) {
        columns.add(column);
        columnMap.put(column.getColumnID(), column);
    }

    public ColumnInfo getColumn(int index) { return columns.get(index); }
    public ColumnInfo getColumnFromMap(int colid) { return columnMap.get(colid); }
    public long getColumnCount() { return columns.size(); }

    public void addKey(ColumnInfo column) { keyColumns.add(column); }
    public ColumnInfo getKey(int index) { return keyColumns.get(index); }
    public long getKeyCount() { return keyColumns.size(); }

    public long insertMessageToTable(RowMessage urm) {
        if (log.isTraceEnabled()) { log.trace("enter"); }

        msgs.add(urm);
        switch (urm.getOperatorType()) {
            case "I":
                insertRow(urm);
                break;
            case "U":
                updateRow(urm);
                break;
            case "K":
                if (tableInfo.getParams().getDatabase().isBatchUpdate()) {
                    updateRowWithKeySplit(urm);
                }else {
                    updateRowWithKey(urm);
                }
                break;
            case "D":
                deleteRow(urm);
                break;

            default:
                log.error("operator [" + urm.getOperatorType() + "]");
                return 0;
        }

        if (log.isTraceEnabled()) { log.trace("exit"); }

        return 1;
    }

    public void werrToFile(String outPutPath_) {
        if (log.isDebugEnabled()) {
            log.debug("commit faild,write message to file.msgs count:["
                       +msgs.size()+"]");
        }
        // write to file
        BufferedOutputStream bufferOutput = init_bufferOutput(outPutPath_, schemaName, tableName);
        for (int i = 0; i < msgs.size(); i++) {
            RowMessage rowMessage = msgs.get(i);
            Object message = rowMessage.getMessage();
            try {
                bufferOutput.write((String.valueOf(message)+"\n").getBytes());
            } catch (IOException e) {
                log.error("throw IOException when write err message to file ",e);
            }
        }
        flushAndClose_bufferOutput(bufferOutput);
    }

    public void printBatchErrorMsg(int[] insertCounts, Map<Integer, 
				    RowMessage> errRows, String operateType) {
        if (log.isDebugEnabled()) { log.trace("enter"); }
        for (int i = 0; i < insertCounts.length; i++) {
             if ( insertCounts[i] == Statement.EXECUTE_FAILED ) {
                 if (log.isDebugEnabled()) {
                     log.debug("Error on request #" + i +": Execute failed");
                 }

                 RowMessage rowMessage = errRows.get(i);
                 if (rowMessage != null) {
		     log.error(rowMessage.getErrorMsg(i, operateType));
                 }
                 // add the errMess count
                 switch (operateType) {
                    case I_OPERATE:
                        errInsert ++;
                        break;
                    case U_OPERATE:
                        errUpdate ++;
                        break;
                    case D_OPERATE:
                        errDelete ++;
                        break;
                    default:
                        log.error("not match any operate type when add the error message count");
                        break;
		 }
             }
	}

        if (log.isDebugEnabled()) { log.trace("exit"); }
     }

    public BufferedOutputStream init_bufferOutput(String filepath,String schemaName,String tableName) {
        BufferedOutputStream bufferedOutput =null;
        if (log.isDebugEnabled()) {
            log.trace("enter, filepath:\"" + filepath 
		      + "\", schemaName: " + schemaName + ", tableName: "
		      + tableName);
        }
        if (filepath!=null) {
            
            // create parent path
            if (!filepath.substring(filepath.length() - 1).equals("/")) 
                filepath = filepath + "/";
            String fullOutPutPath = filepath + schemaName + "/" + tableName;
            if (log.isDebugEnabled()) {
                log.trace("filepath: \"" + fullOutPutPath + "\"");
            }
            File file = new File(fullOutPutPath);
            if (!file.getParentFile().exists()) {
                if (!file.getParentFile().mkdirs()) {
                   log.error("create kafka error message output path ["
			     + schemaName +"] dir faild");
                }
            }
           // new output buff
            try {
               bufferedOutput = new BufferedOutputStream(new FileOutputStream(fullOutPutPath,true));
            } catch (FileNotFoundException e) {
                log.error("file notfound when init Outputpath buffer.",e);
            } 
        }

        if (log.isDebugEnabled()) { log.trace("exit"); }

        return bufferedOutput;
    }
    
    public void flushAndClose_bufferOutput(BufferedOutputStream bufferedOutput) {
        if (bufferedOutput!=null) {
            try {
                bufferedOutput.flush();
                bufferedOutput.close();
            } catch (IOException e) {
                log.error("throw IOException when flush or close output buffer.",e);
            }
        }
    }

    //make sure it's not Connection does not exist(-29002) Exception && Timeout expired(-29154) Exception
    public boolean isNotDisConnAndTOExpection(SQLException se) {
        if (!(se.getErrorCode()==-29002) && !(se.getErrorCode()==-29154)) {
           return true;
        }
        return false;
    }
}
