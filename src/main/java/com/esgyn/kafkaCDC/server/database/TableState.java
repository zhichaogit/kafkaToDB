package com.esgyn.kafkaCDC.server.database;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.esgyn.kafkaCDC.server.kafkaConsumer.messageType.RowMessage;
import com.esgyn.kafkaCDC.server.utils.ColumnInfo;
import com.esgyn.kafkaCDC.server.utils.FileUtils;
import com.esgyn.kafkaCDC.server.utils.TableInfo;

import lombok.Getter;

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
        if (!tableInfo.isTherePK()) {
            insertRows = new IdentityHashMap<String, RowMessage>(0);
        } else {
            insertRows = new HashMap<String, RowMessage>(0);
        }
        updateRows = new HashMap<String, RowMessage>(0);
        deleteRows = new HashMap<String, RowMessage>(0);
        msgs       = new ArrayList<RowMessage>();

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
                    if (tableInfo.isTherePK()) {
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
                    if (tableInfo.isTherePK()) {
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

    private long insert_row(RowMessage rowMessage) {
        if (log.isTraceEnabled()) { log.trace("enter"); }

        Map<Integer, ColumnValue> rowColumns = rowMessage.getColumns();

        String message = rowMessage.getMessage();
        String key = get_key_value(message, rowColumns, true);

        if (log.isDebugEnabled()) {
            log.debug("insert row key [" + key + "], message [" + message + "]");
        }

        if (key == null)
            return 0;

	merge_offsets(key, rowMessage);
        // new row is inserted
        insertRows.put(key, rowMessage);

        if (log.isTraceEnabled()) {
            log.trace("exit cache insert [rows: " + cacheInsert + ", insert: "
		      + insertRows.size() + ", update: " + updateRows.size() + ", delete: "
		      + deleteRows.size() + "]");
        }

        return 1;
    }

    public boolean is_update_key(RowMessage rowMessage) {
        if (log.isTraceEnabled()) { log.trace("enter"); }

        Map<Integer, ColumnValue> rowColumns = rowMessage.getColumns();

        ColumnValue cacheValue = null;

        for (int i = 0; i < keyColumns.size(); i++) {
            ColumnInfo keyInfo = keyColumns.get(i);
            cacheValue = rowColumns.get(keyInfo.getColumnOff());
            if (cacheValue == null)
                continue;

            String oldValue = cacheValue.getOldValue();
            String curValue = cacheValue.getCurValue();
            if (log.isDebugEnabled()) {
                log.debug("update the keys [" + oldValue + "] to [" + curValue + "]");
            }

            if (tableInfo.isTherePK() && !curValue.equals(oldValue)) {
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

    private long update_row(RowMessage rowMessage) {
        if (log.isTraceEnabled()) { log.trace("enter"); }

        Map<Integer, ColumnValue> rowColumns = rowMessage.getColumns();

        String message = rowMessage.getMessage();

        String key = get_key_value(message, rowColumns, false);

        if (log.isDebugEnabled()) {
            log.debug("update row key [" + key + "], message [" + message + "]");
        }

        if (key == null)
            return 0;

        RowMessage insertRM = insertRows.get(key);

        if (insertRM != null) {
            Map<Integer, ColumnValue> insertedColumns = insertRM.getColumns();
            if (insertedColumns != null) {
            /*
             * exist in insert map, must be not exist in update and delete update the insert row in
             * memory
             */
                for (ColumnValue value : rowColumns.values()) {
                    insertedColumns.put(value.getColumnID(), value);
                }
                if (log.isDebugEnabled()) {
                    log.debug("update row key [" + key + "] is exist in insert cache");
                }
                insertRM.setColumns(insertedColumns);
		insertRM.add(rowMessage.getOffset());
                insertRows.put(key, insertRM);
            }
        } else {
	    if (log.isDebugEnabled()) {
                log.trace("the key ["+key+"] not exist in insertRows");
            }
            RowMessage deleteRM = deleteRows.get(key);
            
            if (deleteRM != null) {
                Map<Integer, ColumnValue> deleteColumns = deleteRM.getColumns();
                if (deleteColumns != null) {
		    if (!tableInfo.getParams().getKafkaCDC().isSkip()) {
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
                    Map<Integer, ColumnValue> updateColumns = updateRM.getColumns();
                    if (updateColumns != null) {
                        if (log.isDebugEnabled()) {
                             log.debug("update row key is exist in update cache [" + key + "],"
                                    +" the message ["+ message + "]");
                        }

                        ColumnValue cacheValue;

                        for (ColumnValue value : updateColumns.values()) {
                            cacheValue = rowColumns.get(value.getColumnID());
			    /* 
			     * multi updates must merge the old value and cur value 
			     * to a new ColumnValue:
			     * update t1 set c1 = 2 where c1 = 1;
			     * update t1 set c1 = 3 where c1 = 2;
			     * the merge result should be:
			     * update t1 set c1 = 3 where c1 = 1;
			     */
                            if (cacheValue != null) {
                                value = new ColumnValue(cacheValue.getColumnID(),
                                        cacheValue.getCurValue(), value.getOldValue(),
                                        tableInfo.getColumn(cacheValue.getColumnID()).getTypeName());
                            }

                            rowColumns.put(value.getColumnID(), value);
                        }
			rowMessage.merge(updateRM.getOffsets());
			rowMessage.setColumns(rowColumns);
                    }
                }

                updateRows.put(key, rowMessage);
            }
        }

        if (log.isTraceEnabled()) {
            log.trace("exit cache update [rows: " + cacheUpdate + ", insert: "
                    + insertRows.size() + ", update: " + updateRows.size() 
		      + ", delete: " + deleteRows.size() + "]");
        }

        return 1;
    }

    private long update_row_with_key(RowMessage rowMessage) {
        if (log.isTraceEnabled()) { log.trace("enter"); }

        Map<Integer, ColumnValue> rowColumns = rowMessage.getColumns();
        String message = rowMessage.getMessage();
        String oldkey = get_key_value(message, rowColumns, false);
        String newkey = get_key_value(message, rowColumns, true);

        if (log.isDebugEnabled()) {
            log.debug("updkey row key [old key: " + oldkey + ", new key: "
		      + newkey + "], " + "message [" + message + "]");
        }

        if (oldkey == null || newkey == null)
            return 0;

	if (oldkey.equals(newkey)){
            log.error("updkey row key are same [old key: " + oldkey
		      + ", new key: " + newkey + "]");
	    return 0;
	}

        RowMessage insertOldRM = insertRows.get(oldkey);
        if (insertOldRM != null) {
	    RowMessage insertNewRM = insertRows.get(newkey);
	    if (insertNewRM != null) {
		// new value have exist
		if (!tableInfo.getParams().getKafkaCDC().isSkip()) {
		    log.error("update row key is exist in insert cache [" + newkey + "],"
			      + "the message [" + rowMessage.getMessage() + "]");
		}
		return 0;
	    }

            Map<Integer, ColumnValue> insertColumns = insertOldRM.getColumns();
            if (insertColumns != null) {
                /*
                 * exist in insert map, must be not exist in update and delete update the 
                 * insert row in memory
                 */
                for (ColumnValue value : rowColumns.values()) {
                    insertColumns.put(value.getColumnID(), new ColumnValue(value));
                }

                if (log.isDebugEnabled()) {
                    log.debug("updkey row key [" + oldkey + "] exist in insert cache");
                }

                // remove old key
                insertRows.remove(oldkey);

                // insert the new key
                insertOldRM.setColumns(insertColumns);
		insertOldRM.add(rowMessage.getOffset());
                insertRows.put(newkey, insertOldRM);

                // delete the old key on disk
		deleteRows.put(oldkey, rowMessage);
            }
        } else {
            RowMessage deleteOldRM = deleteRows.get(oldkey);
            if (deleteOldRM != null) {
		Map<Integer, ColumnValue> deleteColumns = deleteOldRM.getColumns();
                if (deleteColumns != null) {
		    if (!tableInfo.getParams().getKafkaCDC().isSkip()) {
                        log.error("update row key is exist in delete cache [" + oldkey
                                + "], the message [" + message + "]");
                    }
                    return 0;
                }
            } else {
		RowMessage updateOldRM = updateRows.get(oldkey);
                Map<Integer, ColumnValue> updateColumns = null;
                if (updateOldRM != null) {
                    updateColumns = updateOldRM.getColumns();
                    if (log.isDebugEnabled()) {
                        log.debug(
                            "updkey row [key: " + oldkey + "] in update cache [" + updateColumns + "]");
                    }

                    if (updateColumns != null) {
                        ColumnValue cacheValue;

			/* 
			 * multi updates must merge the old value and cur value 
			 * to a new ColumnValue:
			 * update t1 set c1 = 2 where c1 = 1;
			 * update t1 set c1 = 3 where c1 = 2;
			 * the merge result should be:
			 * update t1 set c1 = 3 where c1 = 1;
			 */
                        for (ColumnValue value : rowColumns.values()) {
                            cacheValue = updateColumns.get(value.getColumnID());
                            if (cacheValue != null) {
                                value = new ColumnValue(value.getColumnID(), value.getCurValue(),
                                        cacheValue.getOldValue(),
                                        tableInfo.getColumn(cacheValue.getColumnID()).getTypeName());
                                updateColumns.put(value.getColumnID(), value);
                            } else {
                                updateColumns.put(value.getColumnID(), new ColumnValue(value));
                            }
                        }

                        // delete the old update message
                        updateRows.remove(oldkey);
			updateOldRM.add(rowMessage.getOffset());
                    }
                } else {
                    updateColumns = new HashMap<Integer, ColumnValue>(0);
                    for (ColumnValue value : rowColumns.values()) {
                        updateColumns.put(value.getColumnID(), new ColumnValue(value));
                    }
                    updateOldRM = rowMessage;
                    updateOldRM.setColumns(updateColumns);
                }

		RowMessage deleteNewRM = deleteRows.get(newkey);
		if (deleteNewRM != null) {
		    // remove the delete operator and merge the offset to cur row
		    updateOldRM.merge(deleteNewRM.getOffsets());
		    deleteRows.remove(newkey);
		}
		
                // add new insert message
                updateRows.put(newkey, updateOldRM);

                // delete the data on the disk
		rowMessage.setColumns(rowColumns);
		deleteRows.put(oldkey, rowMessage);
            }
        }

        if (log.isTraceEnabled()) {
            log.trace("exit cache updkey [rows: " + cacheUpdkey + ", insert: "
                    + insertRows.size() + ", update: " + updateRows.size() + ", delete: "
                    + deleteRows.size() + "]");
        }

        return 1;
    }

    private long delete_row(RowMessage rowMessage) {
        if (log.isTraceEnabled()) {
            log.trace("exit");
        }

        Map<Integer, ColumnValue> rowColumns = rowMessage.getColumns();
        String message = rowMessage.getMessage();
        String key = get_key_value(message, rowColumns, false);

        if (log.isDebugEnabled()) {
            log.debug("delete row key [" + key + "], message [" + message + "]");
        }

        if (key == null)
            return 0;

	merge_offsets(key, rowMessage);
        // delete cur row
        deleteRows.put(key, rowMessage);

        if (log.isTraceEnabled()) {
            log.trace("exit cache delete [rows: " + cacheDelete + ", insert: "
                    + insertRows.size() + ", update: " + updateRows.size() + ", delete: "
                    + deleteRows.size() + "]");
        }

        return 1;
    }

    private void merge_offsets(String key, RowMessage rowMessage) {
        RowMessage insertRM = insertRows.get(key);
	RowMessage updateRM = updateRows.get(key);
	RowMessage deleteRM = deleteRows.get(key);

	if (insertRM != null) {
	    // merge history to current message
	    rowMessage.merge(insertRM.getOffsets());

	    // remove the old insert messages
	    insertRows.remove(key);
	} else if (updateRM != null) {
	    // record the merge history
	    rowMessage.merge(updateRM.getOffsets());

	    // remove the update messages
	    updateRows.remove(key);
	} else if  (deleteRM != null) {
	    // record the merge history
	    rowMessage.merge(deleteRM.getOffsets());
	    
	    // remove the delete messages
	    deleteRows.remove(key);
	}
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

    public void clean() {
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

        insertRows.clear();
        updateRows.clear();
        deleteRows.clear();
	msgs.clear();

        cacheInsert = 0;
        cacheUpdate = 0;
        cacheUpdkey = 0;
        cacheDelete = 0;

        errInsert = 0;
        errUpdate = 0;
        errDelete = 0;
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
	msgs.clear();

        cacheInsert = 0;
        cacheUpdate = 0;
        cacheUpdkey = 0;
        cacheDelete = 0;

        errInsert = 0;
        errUpdate = 0;
        errDelete = 0;
    }

    boolean executeBatch(PreparedStatement stmt, Map<Integer, RowMessage> errRows,
			 String operate)  throws SQLException {
	boolean result = true;

        try {
            stmt.executeBatch();
        } catch (BatchUpdateException bue) {
	    if (Database.isAccepableSQLExpection(bue))
		throw bue;
	    SQLException se = bue;
	    do {
	        log.error("throw exception when batch execute the sql:", se);
	        se = se.getNextException();
            } while (se != null);

            // print the error data 
            int[] counts = bue.getUpdateCounts();
            printBatchErrorMsg(counts, errRows, operate);
            result = false;
        } catch (IndexOutOfBoundsException iobe) {
	    log.error("throw exception when batch execute the sql:", iobe);
            result = false;
        }catch (SQLException se) {
	    if (Database.isAccepableSQLExpection(se))
		throw se;
	    log.error("throw exception when batch execute the sql:", se);
            result = false;
        }finally {
            errRows.clear();
        }

	return result;
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

	    if ((offset % tableInfo.getBatchSize()) == 0) {
		result = executeBatch(insertStmt, errRows, I_OPERATE);

		if (!result)
		    return result;
	    }
        }

	if ((offset % tableInfo.getBatchSize()) != 0) {
	    result = executeBatch(insertStmt, errRows, I_OPERATE);
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
	    strBuffer.append("update one row, key count [" + keyColumns.size() + "]");
        }

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
		    if (log.isDebugEnabled()) {
			strBuffer.append("\n\tkey id:" + i + ", column id:" 
					 + keyInfo.getColumnOff());
		    }

		    if (keyValue == null || keyValue.oldValueIsNull()) {
			if (tableInfo.isTherePK()) {
			    String key = get_key_value(null, row, false);
			    log.error("the primary key value is null [table:" 
				      + schemaName + "." + tableName + ", column:"
				      + keyInfo.getColumnName() + "]");
			    result = false;
			    break;
			}
			
			if (log.isDebugEnabled()) {
			    strBuffer.append(", key is [null]");
			}

			updateStmt.setNull(columns.size()+ (i + 1), keyInfo.getColumnType());
		    } else {
			if (log.isDebugEnabled()) {
			    strBuffer.append(", key is [" + keyValue.getOldValue() + "]");
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

        String      whereSql   = null;
        String      updateSql  = "UPDATE \"" + schemaName + "\"." + "\"" + tableName + "\"" + " SET ";

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
            if (columnValue.getCurValue()!=null && columnValue.getCurValue().equals(columnValue.getOldValue()))
                continue;
            if (columnInfo != null)
                updateSql += ", ";

            columnInfo = columns.get(columnValue.getColumnID());
            updateSql += columnInfo.getColumnName() + " = " + columnValue.getCurValueStr();
            if (log.isDebugEnabled()) {
                strBuffer.append("\tcolumn: " + columnInfo.getColumnOff() + ", curValue ["
                        + columnValue.getCurValue() + "],oldValue["+columnValue.getOldValue()+"]\n");
            }
        }
        //not executeUpdate if all columns not change
        if (columnInfo==null) {
            if (log.isDebugEnabled()) {
                strBuffer.append("all columns not change,not execute this udpdate sql.\n");
            }
            return true;
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
		      + updateSql + "]",se);
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

		if ((offset % tableInfo.getBatchSize()) == 0) {
		    result = executeBatch(updateStmt, errRows, U_OPERATE);

		    if (!result)
			return result;
		}
            }
        }

        if (tableInfo.getParams().getDatabase().isBatchUpdate()
	    && (offset % tableInfo.getBatchSize()) != 0) {
	    result = executeBatch(updateStmt, errRows, U_OPERATE);
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
	    strBuffer.append("delete one row, key count [" + keyColumns.size() + "]");
        }

	int         i        = 0;
	ColumnInfo  keyInfo  = null;
	ColumnValue keyValue = null;
	try {
	    for (i = 0; i < keyColumns.size(); i++) {
		keyInfo = keyColumns.get(i);
		keyValue = row.get(keyInfo.getColumnOff());

		if (log.isDebugEnabled()) {
		    strBuffer.append("\n\tkey id:" + i + ", column id:" 
				     + keyInfo.getColumnOff());
		}

		if (keyValue == null || keyValue.oldValueIsNull()) {
		    if (tableInfo.isTherePK()) {
			String key = get_key_value(null, row, false);
			log.error("the primary key value is null [table:" 
				  + schemaName + "." + tableName + ", column:"
				  + keyInfo.getColumnName() + "]");
			result = false;
			break;
		    }

		    if (log.isDebugEnabled()) {
			strBuffer.append(", key is [null]");
		    }
		    
		    deleteStmt.setNull(i + 1, keyInfo.getColumnType());
		} else {
		    if (log.isDebugEnabled()) {
			strBuffer.append(", key is [" + keyValue.getOldValue() + "]");
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

	    if ((offset % tableInfo.getBatchSize()) == 0) {
		result = executeBatch(deleteStmt, errRows, D_OPERATE);

		if (!result)
		    return result;
	    }
        }

	if ((offset % tableInfo.getBatchSize()) != 0) {
	    result = executeBatch(deleteStmt, errRows, D_OPERATE);
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
	String operatorType = urm.getOperatorType();
	 long update_rows ;
        switch (operatorType) {
            case "I":
                cacheInsert += insert_row(urm);
                break;
            case "U":
            case "K":
		if (is_update_key(urm)) {
		    urm.setOperatorType("K");
		    update_rows = update_row_with_key(urm);
		} else if (tableInfo.isTherePK()&&tableInfo.getParams().getDatabase().isBatchUpdate()) {
		    urm.setOperatorType("I");
		    update_rows = insert_row(urm);
                } else {
		    urm.setOperatorType("U");
		    update_rows = update_row(urm);
		}
		if (operatorType.equals("U")) {
		    cacheUpdate += update_rows;
                }else {
                    cacheUpdkey += update_rows;
                }
                break;
            case "D":
                cacheDelete += delete_row(urm);
                break;

            default:
                log.error("operator [" + operatorType + "]");
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
