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

import com.esgyn.kafkaCDC.server.utils.FileUtils;
import com.esgyn.kafkaCDC.server.utils.TableInfo;
import com.esgyn.kafkaCDC.server.utils.ColumnInfo;
import com.esgyn.kafkaCDC.server.kafkaConsumer.messageType.RowMessage;

import lombok.Getter;
import lombok.Setter;

public class TableState {
    public final static int                ERROR       = -1;
    public final static int                EMPTY       = 0;
    
    @Getter
    private int                            state       = EMPTY;
    @Getter
    private String                         schemaName  = null;
    @Getter
    private String                         tableName   = null;

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
    private RowMessage                     lastMsg     = null;
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

        updateSql = "UPDATE \"" + schemaName + "\"." + "\"" + tableName + "\"" + " SET "+
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

    public boolean is_update_key(RowMessage rowMessage) {
        if (log.isTraceEnabled()) { log.trace("enter"); }

        Map<Integer, ColumnValue> rowValues = rowMessage.getColumns();

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
		if (log.isDebugEnabled()) {
		    log.error("U message cann't update the keys," + "tablename [" + schemaName 
			      + "." + tableName + "],curValue [" + curValue + "],oldValue "
			      + "[" + oldValue + "] ,keyCol:" + keyInfo.getColumnName()
			      + "message [" + rowMessage.getMessage() + "]\n");
		}
                return true;
            }
        }

        return false;
    }

    public long updateRow(RowMessage rowMessage) {
        if (log.isTraceEnabled()) { log.trace("enter"); }

        Map<Integer, ColumnValue> rowValues = rowMessage.getColumns();

        String message = rowMessage.getMessage();

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
                if (!tableInfo.getParams().getKafkaCDC().isSkip()) {
                    log.error("updkey row key is exist in insert cache, newkey [" + newkey + "],"
                        +"the message ["+ message + "]");
                }
                return 0;
            }
        } else {
	    if (log.isTraceEnabled()) {
                log.trace("the newkey ["+newkey+"] not exist in insertRows");
            }
	    deleteRows.remove(newkey);
            RowMessage deleteRM = deleteRows.get(oldkey);

            if (deleteRM != null) {
                Map<Integer, ColumnValue> deleterow = deleteRM.getColumns();
                if (deleterow != null) {
		    if (!tableInfo.getParams().getKafkaCDC().isSkip()) {
                        log.error("update row key is exist in delete cache [" + oldkey + "]," 
				  + "the message ["+ message + "]");
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
        if (insertRM != null) {
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
	    deleteRows.remove(newkey);

            RowMessage deleteRM = deleteRows.get(oldkey);
            RowMessage updateRM = updateRows.get(oldkey);
            if (deleteRM != null && updateRM == null) {
		Map<Integer, ColumnValue> deleterow = deleteRM.getColumns();
                if (deleterow != null) {
		    if (!tableInfo.getParams().getKafkaCDC().isSkip()) {
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

    public void closeStmts() {
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

    public boolean commitTable(Connection dbConn_) throws SQLException {
        if (log.isTraceEnabled()) {
            log.trace("commit table [" + schemaName + "." + tableName + ", insert: "
		      + insertRows.size() + ", update: " + updateRows.size() + ", delete: "
		      + deleteRows.size() + "] ");
        }

	if (state == ERROR) {
	    return dump_data_to_file(true);
	}

	// if reconnect to database, we must prepare the stmt again
	prepareStmts(dbConn_);

	if (insertRows.size() > 0 && !insert_data()) {
	    return false;
	}

        if (updateRows.size() > 0 && !update_data()) {
	    return false;
	}

        if (deleteRows.size() > 0 && !delete_data()) {
	    return false;
	}

	dbConn_.commit();
	if (!dump_data_to_file(false)) {
	    return false;
	}
	state = EMPTY;

	return true;
    }


    public long getInsertRows() {
        return insertRows.size();
    }

    public long getUpdateRows() {
        return updateRows.size();
    }

    public long getDeleteRows() {
        return deleteRows.size();
    }

    public void clearCache() {
        if (state == EMPTY) {
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

    private boolean insert_row_data(Map<Integer, ColumnValue> row, int offset) throws SQLException {
        boolean result = true;
        StringBuffer strBuffer=null;

        if (log.isTraceEnabled()) {
            log.trace("enter, offset [" + offset + "]");
        }

        if (log.isDebugEnabled()) {
            strBuffer = new StringBuffer();
        }

	int         i           = 0;
	ColumnInfo  columnInfo  = null;
	ColumnValue columnValue = null;
	try {
	    for (i = 0; i < columns.size(); i++) {
		columnInfo = columns.get(i);
		columnValue = row.get(columnInfo.getColumnOff());
		if (columnValue == null || columnValue.curValueIsNull()) {
		    if (log.isDebugEnabled()) {
			strBuffer.append("\tcolumn: " + i + " [null]\n");
		    }
		    insertStmt.setNull(i + 1, columnInfo.getColumnType());
		} else {
		    if (log.isDebugEnabled()) {
			strBuffer.append("\tcolumn: " + i + ", value [" 
					 + columnValue.getCurValue() + "]\n");
		    }

		    insertStmt.setString(i + 1, columnValue.getCurValue());
		}
	    }

            insertStmt.addBatch();
	} catch (SQLException se) {
	    if (Database.isAccepableSQLExpection(se))
		throw se;

	    log.error("the row [" + offset + "] has an error in column [" + (i+1) + "]"
		      + ", the value [" + columnValue.getCurValue() + "]:", se);
	    result = false;
	}catch (Exception e) {
	    log.error("the row [" + offset + "] has an error in column [" + (i+1) + "]"
		      + ", the value [" + columnValue.getCurValue() + "]:", e);
	    result = false;
	}

        if (log.isDebugEnabled()) {
            log.debug(strBuffer.toString());
        }

        if (log.isTraceEnabled()) {
            log.trace("exit, offset [" + offset + "], insert row: [" + result + "]");
        }

        return result;
    }

    private boolean insert_data() throws SQLException {
	boolean result = true;

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
                log.debug("insert row offset: " + offset + ",rowmessage:"
			  + cols.get(0).getCurValue() + "\n");
            }
	    if (!insert_row_data(cols, offset)) {
		log.error("the RowMessage is [" + insertRM.getErrorMsg() + "]");

		return false;
	    }
            errRows.put(offset, insertRM);
            offset++;
        }

        try {
            insertStmt.executeBatch();
        } catch (BatchUpdateException bue) {
	    if (Database.isAccepableSQLExpection(bue))
		throw bue;

	    log.error("throw exception when execute the insert sql:", bue);

            // print the error data 
            int[] insertCounts = bue.getUpdateCounts();
            printBatchErrorMsg(insertCounts, errRows, I_OPERATE);
            result = false;
        } catch (IndexOutOfBoundsException iobe) {
	    log.error("throw exception when execute the insert sql:", iobe);
            result = false;
        }catch (SQLException se) {
	    if (Database.isAccepableSQLExpection(se))
		throw se;
	    log.error("throw exception when execute the insert sql:", se);
            result = false;
        }finally {
            errRows.clear();
        }

        if (log.isTraceEnabled()) { log.trace("exit"); }

	return result;
    }

    private boolean update_row_data(Map<Integer, ColumnValue> row,int offset) throws SQLException  {
        boolean      result    = true;
        StringBuffer strBuffer = null;

        if (log.isTraceEnabled()) {
            log.trace("enter, offset [" + offset + "]");
        }
        if (log.isDebugEnabled()) {
            strBuffer = new StringBuffer();
        }

        String      whereSql   = null;
        String      updateSql  = "UPDATE \"" + schemaName + "\"." + "\"" + tableName + "\"" + " SET ";
        ColumnInfo  keyInfo    = null;
        ColumnInfo  columnInfo = null;
        ColumnValue keyValue   = null;

        if (tableInfo.getParams().getDatabase().isBatchUpdate()) {
	    int         i           = 0;
	    ColumnValue columnValue = null;
            //set col values
	    try {
		for (i = 0; i < columns.size(); i++) {
		    columnInfo = columns.get(i);
		    columnValue = row.get(columnInfo.getColumnOff());
		    if (columnValue == null || columnValue.curValueIsNull()) {
			if (log.isDebugEnabled()) {
			    strBuffer.append("\tcolumn: " + i + " [null]\n");
			}
			updateStmt.setNull(i + 1, columnInfo.getColumnType());
		    } else {
			if (log.isDebugEnabled()) {
			    strBuffer.append("\tcolumn: " + i + ", value [" 
					     + columnValue.getCurValue() + "]\n");
			}
                        updateStmt.setString(i + 1, columnValue.getCurValue());
		    }
		}
		columnValue = null;

		//set key values
		for (i = 0; i < keyColumns.size(); i++) {
		    keyInfo = keyColumns.get(i);
		    keyValue = row.get(keyInfo.getColumnOff());

		    if (keyValue == null || keyValue.oldValueIsNull()) {
			if (havePK) {
			    String key = get_key_value(null, row, false);
			    log.error("the primary key value is null [table:" + schemaName + "." + tableName
				      + ", column:" + keyInfo.getColumnName() + "]");
			    result = false;
			    break;
			}
			updateStmt.setNull(columns.size()+ (i + 1), keyInfo.getColumnType());
		    } else {
			if (log.isDebugEnabled()) {
			    strBuffer.append("\tkey id:" + i + ", column id:" + keyInfo.getColumnOff()
					     + ", key [" + keyValue.getOldValue() + "]");
			}
			updateStmt.setString(columns.size()+ (i + 1), keyValue.getOldValue());
		    }
		}
	    } catch (SQLException se) {
		if (Database.isAccepableSQLExpection(se))
		    throw se;

		if (columnValue != null) {
		    log.error("the row [" + offset + "] has an error in column [" + (i+1) + "]"
			      + ", the value [" + columnValue.getCurValue() + "]:", se);
		} else {
		    log.error("the row [" + offset + "] has an error in key column [" + (i+1) + "]"
			      + ", the value [" + keyValue.getOldValue() + "]:", se);
		}
		result = false;
	    }catch (Exception e) {
		if (columnValue != null) {
		    log.error("the row [" + offset + "] has an error in column [" + (i+1) + "]"
			      + ", the value [" + columnValue.getCurValue() + "]:", e);
		} else {
		    log.error("the row [" + offset + "] has an error in key column [" + (i+1) + "]"
			      + ", the value [" + keyValue.getOldValue() + "]:", e);
		}
		result = false;
	    }

            if (result) {
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

	// one by one update if  format is not protobuf
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
	    if (Database.isAccepableSQLExpection(se))
		throw se;

	    log.error("throw exception when execute the update sql, the sql:["
		      + updateSql + "]");
	    result = false;
	}

        if (log.isTraceEnabled()) {
            log.trace("exit, offset [" + offset + "], insert row: [" + result + "]");
        }

        return result;
    }

    public boolean update_data() throws SQLException  {
	boolean result = true;
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
		if (!update_row_data(updateRow.getColumns(), offset)) {
		    log.error("the RowMessage is [" + updateRow.getErrorMsg() + "]");
		    return false;
		}
                offset++;
            }
        }

        if (tableInfo.getParams().getDatabase().isBatchUpdate()) {
	    try {
		updateStmt.executeBatch();
	    } catch (BatchUpdateException bue) {
		if (Database.isAccepableSQLExpection(bue))
		    throw bue;

		// print the error data 
		int[] updateCounts = bue.getUpdateCounts();
		printBatchErrorMsg(updateCounts, errRows, U_OPERATE);
		result = false;
	    } catch (IndexOutOfBoundsException iobe) {
		log.error("throw exception when execute the update sql:", iobe);
		result = false;
	    }catch (SQLException se) {
		if (Database.isAccepableSQLExpection(se))
		    throw se;

		log.error("throw exception when execute the update sql", se);
		result = false;
	    }finally {
		errRows.clear();
	    }
	}

        if (log.isTraceEnabled()) { log.trace("exit"); }

	return result;
    }

    private boolean delete_row_data(Map<Integer, ColumnValue> row, int offset) throws SQLException {
        boolean      result    = true;
        StringBuffer strBuffer = null;

        if (log.isTraceEnabled()) { log.trace("enter, offset:[" + offset +"]"); }

        if (log.isDebugEnabled()) {
            strBuffer = new StringBuffer();
        }

	int         i        = 0;
	ColumnInfo  keyInfo  = null;
	ColumnValue keyValue = null;
	try {
	    for (i = 0; i < keyColumns.size(); i++) {
		keyInfo = keyColumns.get(i);
		keyValue = row.get(keyInfo.getColumnOff());

		if (keyValue == null || keyValue.oldValueIsNull()) {
		    if (havePK) {
			String key = get_key_value(null, row, false);
			log.error("the primary key value is null [table:" + schemaName + "." + tableName
				  + ", column:" + keyInfo.getColumnName() + "]");
			result = false;
			break;
		    }
		    deleteStmt.setNull(i + 1, keyInfo.getColumnType());
		} else {
		    if (log.isDebugEnabled()) {
			strBuffer.append("\tkey id:" + i + ", column id:" + keyInfo.getColumnOff() 
					 + ", key [" + keyValue.getOldValue() + "]");
		    }
		    deleteStmt.setString(i + 1, keyValue.getOldValue());
                }
            }
	} catch (SQLException se) {
	    if (Database.isAccepableSQLExpection(se)) {
		throw se;
	    }
	    log.error("the row [" + offset + "] has an error in key column [" + (i+1) + "]"
		      + ", the value [" + keyValue.getOldValue() + "]:", se);
	    result = false;
        }

        if (result) {
            deleteStmt.addBatch();
        }

        if (log.isDebugEnabled()) {
            log.debug(strBuffer.toString());
        }
        if (log.isTraceEnabled()) {
            log.trace("enter, offset:[" + offset + "], delete row [" + result + "]");
        }

        return result;
    }

    private boolean delete_data() throws SQLException {
	boolean result = true;
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
	    if (!delete_row_data(deleteRow.getColumns(), offset)) {
		log.error("the RowMessage is [" + deleteRow.getErrorMsg() + "]");
		return false;
	    }

            offset++;
        }

        try {
            int[] batchResult = deleteStmt.executeBatch();
        } catch (BatchUpdateException bue) {
	    if (Database.isAccepableSQLExpection(bue))
		throw bue;

	    log.error("throw exception when execute the insert sql:", bue);

            // print the error data 
            int[] deleteCounts = bue.getUpdateCounts();
            printBatchErrorMsg(deleteCounts, errRows, D_OPERATE);
            result = false;
        } catch (IndexOutOfBoundsException iobe) {
	    log.error("throw exception when execute the insert sql:", iobe);
            result = false;
        }catch (SQLException se) {
	    if (Database.isAccepableSQLExpection(se))
		throw se;
	    log.error("throw exception when execute the insert sql:", se);
            result = false;
        }finally {
            errRows.clear();
        }

        if (log.isTraceEnabled()) { log.trace("exit"); }
	return result;
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
	lastMsg = urm;

        switch (urm.getOperatorType()) {
            case "I":
                insertRow(urm);
                break;
            case "U":
		if (!is_update_key(urm)) {
		    updateRow(urm);
		    break;
		} else {
		    urm.setOperatorType("K");
		}
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

	if (state >= EMPTY) {
	    state++;	
	}

        if (log.isTraceEnabled()) { log.trace("exit"); }

        return 1;
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

    private boolean dump_data_to_file(boolean withError) {
	if (log.isTraceEnabled()) { log.trace("enter"); }

	List<RowMessage> allList    = new ArrayList<RowMessage>();
	List<RowMessage> insertList = new ArrayList<RowMessage>(insertRows.values());
	List<RowMessage> updateList = new ArrayList<RowMessage>(updateRows.values());
	List<RowMessage> deleteList = new ArrayList<RowMessage>(deleteRows.values());

	allList.addAll(insertList);
	allList.addAll(updateList);
	allList.addAll(deleteList);

	String rootPath = tableInfo.getParams().getKafkaCDC().getLoadDir();
	if (rootPath != null) {
	    // file name: schema_table_topic_partitionID_offset.sql
	    String errorPath = withError ? "_error" : "";
            String filePath = String.format("%s_%s_%s_%d_%d%s.sql", 
					    tableInfo.getSchemaName(), 
					    tableInfo.getTableName(), 
					    lastMsg.getTopic(), 
					    lastMsg.getPartitionID(), 
					    lastMsg.getOffset(),
					    errorPath);
	    filePath = rootPath + filePath;
	    if (!FileUtils.dumpDataToFile(allList, filePath, FileUtils.SQL_STRING)) {
		log.error("dump sql data to file fail.");
		return false;
	    }
	}

	if (log.isTraceEnabled()) { log.trace("exit"); }

	return true;
    }
}
