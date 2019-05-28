package com.esgyn.kafkaCDC.server.esgynDB;

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

import com.esgyn.kafkaCDC.server.kafkaConsumer.messageType.RowMessage;

public class TableState {
    private String                         schemaName  = null;
    private String                         tableName   = null;
    private String                         format      = null;
    private int                            retry       = 0;

    private long                           cacheInsert = 0;
    private long                           cacheUpdate = 0;
    private long                           cacheUpdkey = 0;
    private long                           cacheDelete = 0;

    private long                           errInsert   = 0;
    private long                           errUpdate   = 0;
    private long                           errDelete   = 0;

    private boolean                        commited    = true;
    private boolean                        havePK      = false;
    private boolean                        multiable   = false;

    private Connection                     dbConn      = null;
    private TableInfo                      tableInfo   = null;

    private PreparedStatement              insertStmt  = null;
    private PreparedStatement              deleteStmt  = null;
    private List<RowMessage>               msgs        = null;
    Map<String, RowMessage> insertRows  = null;
    Map<String, RowMessage> updateRows  = null;
    Map<String, RowMessage> deleteRows  = null;

    ArrayList<ColumnInfo>                  keyColumns  = null;
    ArrayList<ColumnInfo>                  columns     = null;
    Map<Integer, ColumnInfo>               columnMap   = null;

    private static Logger                  log         = Logger.getLogger(TableState.class);

    public TableState(TableInfo tableInfo_,String format_) {
        tableInfo = tableInfo_;
        schemaName = tableInfo.GetSchemaName();
        tableName = tableInfo.GetTableName();
        multiable = tableInfo.IsMultiable();

        keyColumns = tableInfo.GetKeyColumns();
        columns = tableInfo.GetColumns();
        columnMap = tableInfo.GetColumnMap();
        format = format_;

        if (multiable) {
            insertRows = new IdentityHashMap<String, RowMessage>(0);
        } else {
            insertRows = new HashMap<String, RowMessage>(0);
        }
        updateRows = new HashMap<String, RowMessage>(0);
        deleteRows = new HashMap<String, RowMessage>(0);
        msgs       = new ArrayList<RowMessage>();
    }

    public boolean InitStmt(Connection dbConn_,boolean skip) {
        if (columns.get(0).GetColumnID() == 0)
            havePK = true;

          dbConn = dbConn_;
          init_insert_stmt(skip);
          init_delete_stmt();

        return true;
    }

    public void init_insert_stmt(boolean skip) {
        ColumnInfo column = columns.get(0);
        String valueSql = ") VALUES(?";
        String insertSql = "UPSERT USING LOAD INTO \"" + schemaName + "\"." + "\"" + tableName
                + "\"" + "(" + column.GetColumnName();
	if (skip) {
            insertSql = "UPSERT INTO \"" + schemaName + "\"." + "\"" + tableName
                    + "\"" + "(" + column.GetColumnName();
        }

        for (int i = 1; i < columns.size(); i++) {
            column = columns.get(i);
            valueSql += ", ?";
            insertSql += ", " + column.GetColumnName();
        }

        insertSql += valueSql + ");";

        log.debug("insert prepare statement [" + insertSql + "]");
        try {
            insertStmt = dbConn.prepareStatement(insertSql);
        } catch (SQLException se) {
            if (!(se.getErrorCode()==-29002) && !(se.getErrorCode()==-29154)) {
                log.error("errCode["+se.getErrorCode()+"],Prepare insert stmt exception, SQL:[" + insertSql + "]");
                do {
                    log.error("errCode["+se.getErrorCode()+"],catch SQLException in method init_insert_stmt",se);
                    se = se.getNextException();
                } while (se != null);
            }
        }
    }

    public String where_condition() {
        ColumnInfo keyInfo = keyColumns.get(0);
        String whereSql = " WHERE " + keyInfo.GetColumnName() + " = ?";

        for (int i = 1; i < keyColumns.size(); i++) {
            keyInfo = keyColumns.get(i);
            whereSql += " AND " + keyInfo.GetColumnName() + " = ?";
        }
        return whereSql;
    }

    public void init_delete_stmt() {
        ColumnInfo keyInfo = keyColumns.get(0);
        String deleteSql = "DELETE FROM \"" + schemaName + "\"." + "\"" + tableName + "\"";

        deleteSql += where_condition() + ";";

        if (log.isDebugEnabled()) {
            log.debug("delete prepare statement [" + deleteSql + "]");
        }

        try {
            deleteStmt = dbConn.prepareStatement(deleteSql);
        } catch (SQLException e) {
            if (!(e.getErrorCode()==-29002) && !(e.getErrorCode()==-29154)) {
                log.error("Prepare delete stmt exception, SQL [" + deleteSql + "]");
                do {
                    log.error("catch SQLException in method init_insert_stmt",e);
                    e = e.getNextException();
                } while (e != null);
            }
        }
    }

    private String get_key_value(String message, Map<Integer, ColumnValue> rowValues, boolean cur) {
        String key = null;

        if (rowValues == null)
            return key;

        for (int i = 0; i < keyColumns.size(); i++) {
            ColumnInfo keyInfo = keyColumns.get(i);
            ColumnValue column = rowValues.get(keyInfo.GetColumnOff());

            if (column == null)
                continue;

            if (log.isDebugEnabled()) {
                log.debug("key id: " + i + ", type: " + cur + " column [id: " + column.GetColumnID()
                        + ", cur value: " + column.GetCurValue() + ", old value: "
                        + column.GetOldValue() + "] cur is null: [" + column.CurValueIsNull()
                        + "] old is null: [" + column.OldValueIsNull() + "]");
            }

            if (cur) {
                if (column.CurValueIsNull()) {
                    if (havePK) {
                        log.error("the cur primary key value is null. column name ["
                                + keyInfo.GetColumnName() + "] message [" + message + "]");
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
                        key = column.GetCurValue();
                    } else {
                        key += "" + column.GetCurValue();
                    }
                }
            } else {
                if (column.OldValueIsNull()) {
                    if (havePK) {
                        log.error("the old primary key value is null. column name ["
                                + keyInfo.GetColumnName() + "] message [" + message + "]");
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
                        key = column.GetOldValue();
                    } else {
                        key += "" + column.GetOldValue();
                    }
                }
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("key string: [" + key + "]");
        }

        return key;
    }

    public long InsertRow(RowMessage rowMessage) {
        if (log.isTraceEnabled()) {
            log.trace("enter function");
        }

        Map<Integer, ColumnValue> rowValues = rowMessage.GetColumns();

        String message = rowMessage.GetMessage();
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
            log.trace("exit function cache insert [rows: " + cacheInsert + ", insert: "
                    + insertRows.size() + ", update: " + updateRows.size() + ", delete: "
                    + deleteRows.size() + "]");
        }

        return 1;
    }

    public long check_update_key(Map<Integer, ColumnValue> rowValues, String message) {
        ColumnValue cacheValue = null;

        for (int i = 0; i < keyColumns.size(); i++) {
            ColumnInfo keyInfo = keyColumns.get(i);
            cacheValue = rowValues.get(keyInfo.GetColumnOff());
            if (cacheValue == null)
                continue;

            String oldValue = cacheValue.GetOldValue();
            String curValue = cacheValue.GetCurValue();
            if (log.isDebugEnabled()) {
                log.debug("update the keys [" + oldValue + "] to [" + curValue + "]");
            }

            if (havePK && !curValue.equals(oldValue)) {
                log.error("U message cann't update the keys,"
                        + "tablename ["+schemaName+"."+tableName+"],curValue ["+curValue+"],oldValue "
                        + "["+oldValue+"] ,keyCol:"+keyInfo.GetColumnName()+"message [" + message + "]\n");
                return 0;
            }
        }

        return 1;
    }

    public long UpdateRow(RowMessage rowMessage) {
        if (log.isTraceEnabled()) {
            log.trace("enter function");
        }

        Map<Integer, ColumnValue> rowValues = rowMessage.GetColumns();

        String message = rowMessage.GetMessage();
        if (!format.equals("Protobuf") && (check_update_key(rowValues, message) == 0))
            return 0;

        String key = get_key_value(message, rowValues, false);

        if (log.isDebugEnabled()) {
            log.debug("update row key [" + key + "], message [" + message + "]");
        }

        if (key == null)
            return 0;

        RowMessage insertRM = insertRows.get(key);

        if (insertRM != null) {
            Map<Integer, ColumnValue> insertRow = insertRM.GetColumns();
            if (insertRow != null) {
            /*
             * exist in insert map, must be not exist in update and delete update the insert row in
             * memory
             */
                for (ColumnValue value : rowValues.values()) {
                    insertRow.put(value.GetColumnID(), value);
                }
                if (log.isDebugEnabled()) {
                    log.debug("update row key [" + key + "] is exist in insert cache");
                }
                insertRM.columns = insertRow;
                insertRows.put(key, insertRM);
            }
        } else {
	    if (log.isDebugEnabled()) {
                log.trace("the key ["+key+"] not exist in insertRows");
            }
            RowMessage deleteRM = deleteRows.get(key);
            
            if (deleteRM != null) {
                Map<Integer, ColumnValue> deleterow = deleteRM.GetColumns();
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
                    Map<Integer, ColumnValue> updateRow = updateRM.GetColumns();
                    if (updateRow != null) {
                        if (log.isDebugEnabled()) {
                             log.debug("update row key is exist in update cache [" + key + "],"
                                    +" the message ["+ message + "]");
                        }

                        ColumnValue cacheValue;

                        for (ColumnValue value : updateRow.values()) {
                            cacheValue = rowValues.get(value.GetColumnID());
                            if (cacheValue != null) {
                                value = new ColumnValue(cacheValue.GetColumnID(),
                                        cacheValue.GetCurValue(), value.GetOldValue());
                            }

                            rowValues.put(value.GetColumnID(), value);
                        }
                    rowMessage.columns = rowValues;
                    }
                }

                updateRows.put(key, rowMessage);
            }
        }

        cacheUpdate++;

        if (log.isTraceEnabled()) {
            log.trace("exit function cache update [rows: " + cacheUpdate + ", insert: "
                    + insertRows.size() + ", update: " + updateRows.size() + ", delete: "
                    + deleteRows.size() + "]");
        }

        return 1;
    }

    public long UpdateRowWithKey(RowMessage rowMessage) {
        if (log.isTraceEnabled()) {
            log.trace("enter function");
        }
        Map<Integer, ColumnValue> rowValues = rowMessage.GetColumns();
        String message = rowMessage.GetMessage();
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
            Map<Integer, ColumnValue> insertRow = insertRM.GetColumns();
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
                Map<Integer, ColumnValue> deleterow = deleteRM.GetColumns();
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
                    updateRow = updateRM.GetColumns();
                    if (log.isDebugEnabled()) {
                        log.debug("updkey row [key: " + oldkey + "] in update cache [" + updateRow + "]");
                    }
                    if (updateRow != null) {
                        ColumnValue cacheValue;

                        for (ColumnValue value : rowValues.values()) {
                            cacheValue = updateRow.get(value.GetColumnID());
                            if (cacheValue != null) {
                                value = new ColumnValue(value.GetColumnID(), value.GetCurValue(),
                                        cacheValue.GetOldValue());
                                updateRow.put(value.GetColumnID(), value);
                            } else {
                                updateRow.put(value.GetColumnID(), new ColumnValue(value));
                            }
                        }
                        updateRM.columns=updateRow;
                        // delete the old update message
                        updateRows.remove(oldkey);
                    }
                }else {
                    updateRow = new HashMap<Integer, ColumnValue>(0);
                    for (ColumnValue value : rowValues.values()) {
                        updateRow.put(value.GetColumnID(), new ColumnValue(value));
                    }
                    updateRM = rowMessage;
                    updateRM.columns = updateRow;
                }
                // add new insert message
                updateRows.put(newkey, updateRM);
            }
        }
        cacheUpdkey++;

        if (log.isTraceEnabled()) {
            log.trace("exit function cache updateKey [rows: " + cacheUpdkey + ", insert: "
                    + insertRows.size() + ", update: " + updateRows.size() + ", delete: "
                    + deleteRows.size() + "]");
        }
        return 1;
    }

    public long DeleteRow(RowMessage rowMessage) {
        if (log.isTraceEnabled()) {
            log.trace("exit function");
        }

        Map<Integer, ColumnValue> rowValues = rowMessage.GetColumns();
        String message = rowMessage.GetMessage();
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
            log.trace("exit function cache delete [rows: " + cacheDelete + ", insert: "
                    + insertRows.size() + ", update: " + updateRows.size() + ", delete: "
                    + deleteRows.size() + "]");
        }

        return 1;
    }

    public boolean CommitTable(String outPutPath,String format_ ) throws SQLException {
        if (log.isDebugEnabled()) {
            log.info("commit table [" + schemaName + "." + tableName + ", insert: "
                    + insertRows.size() + ", update: " + updateRows.size() + ", delete: "
                    + deleteRows.size() + "] ");
        }
        format=format_;
        try {
            insert_data();
            update_data();
            delete_data();
        } catch (BatchUpdateException bue) {
            commited = false;
            if (!(bue.getErrorCode()==0)&&!(bue.getMessage().equals("Connection does not exist"))
                    &&!(bue.getMessage().equals("Timeout expired"))) {
                log.error("batch update table [" + schemaName + "." + tableName
                        + "] throw BatchUpdateException,errorCode["+bue.getErrorCode()+"]," + bue.getMessage());
                SQLException se = bue;
 
                if (log.isDebugEnabled()) {
                    do {
                        log.error("catch BatchUpdateException in method CommitTable ",se);
                        se = se.getNextException();
                    } while (se != null);
                } else {
                    log.error("catch BatchUpdateException in method CommitTable ",se);
                    se = se.getNextException();
                    if (se != null) {
                        log.error("catch BatchUpdateException in method CommitTable ",se);
                    }
                }
            }

            return false;
        } catch (IndexOutOfBoundsException iobe) {
            commited = false;
            log.error("batch update table [" + schemaName + "." + tableName
                    + "] throw IndexOutOfBoundsException: " , iobe);

            return false;
        } catch (SQLException se) {
            commited = false;
            if (!(se.getErrorCode()==-29002) && !(se.getErrorCode()==-29154)) {
                log.error("batch update table [" + schemaName + "." + tableName
                        + "] throw SQLException: " + se.getMessage());
                if (log.isDebugEnabled()) {
                    do {
                        log.error("catch SQLException in method CommitTable",se);
                        se = se.getNextException();
                    } while (se != null);
                } else {
                    log.error("catch SQLException in method CommitTable",se);
                    se = se.getNextException();
                    if (se != null) {
                        log.error("catch SQLException in method CommitTable",se);
                    }
                }
            }

            return false;
        } catch (Exception e) {
            commited = false;
            log.error("batch update table [" + schemaName + "." + tableName + "] throw Exception: "
                    , e);

            return false;
        } finally {
            int disconnErrorCode=0;
            try {
                if (commited) {
                    try {
                      dbConn.commit();
                    }catch(SQLException se){
                      int errorCode = se.getErrorCode();
                      // retry commit table 3 times when commit table conflict
                      if (retry<3 && (errorCode==8616 || errorCode==-8616)) {
                        retry++;
                        log.warn("commit table conflict, retry["+retry+"] time");
                        dbConn.rollback();
                        CommitTable(outPutPath,format);
                      }
                      throw se;
                    }
                } else {
                    dbConn.rollback();
                    werrToFile(outPutPath);
                }
            } catch (SQLException disconnse) {
                disconnErrorCode = disconnse.getErrorCode();
                // retry to connection database 3 times when disconnection.
                if (retry<3 && (disconnErrorCode==-29002 || disconnErrorCode==-29154)) {
                    retry++;
                    commited=true;
                    log.warn("commit table Connection does not exist or Timeout expired when operate"
                            + "table["+schemaName+"."+tableName+"], retry create connect ["+retry+"] time");
                    throw disconnse;
                }
                // Connection does not exist || Timeout expired
                if ((disconnErrorCode==-29002) || (disconnErrorCode==-29154)) {
                    log.error("batch update table [" + schemaName + "." + tableName
                            + "],errorcode["+disconnse.getErrorCode()+"], rollback throw SQLException after reConnect database 3 times,"
                            + "make sure database server is alive pls." ,disconnse);
                }else {
                    log.error("batch update table [" + schemaName + "." + tableName
                            + "],errorcode["+disconnse.getErrorCode()+"], rollback throw SQLException: " ,disconnse);
                }
            }finally{
                msgs.clear();
	    }
        }

        return true;
    }
    public void werrToFile(String outPutPath) {
        if (log.isDebugEnabled()) {
            log.debug("commit faild,write message to file.msgs count:["
                       +msgs.size()+"]");
        }
        // write to file
        BufferedOutputStream bufferOutput = init_bufferOutput(outPutPath, schemaName, tableName);
        for (int i = 0; i < msgs.size(); i++) {
            RowMessage rowMessage = msgs.get(i);
            Object message = rowMessage.mtpara.getMessage();
            try {
                bufferOutput.write((String.valueOf(message)+"\n").getBytes());
            } catch (IOException e) {
                log.error("throw IOException when write err message to file ",e);
            }
        }
        flushAndClose_bufferOutput(bufferOutput);
    }

     public void printBatchErrMess(int[] insertCounts, Map<Integer, RowMessage> errRows) {
        if (log.isDebugEnabled()) {
            log.trace("enter function");
        }
        for (int i = 0; i < insertCounts.length; i++) {
             if ( insertCounts[i] == Statement.EXECUTE_FAILED ) {
                 if (log.isDebugEnabled()) {
                     log.debug("Error on request #" + i +": Execute failed");
                 }
                 Object messagesource =null;
                 RowMessage rowMessage = errRows.get(i);
                 MessageTypePara mtpara=null;
                 if (rowMessage!=null) {
                     mtpara = rowMessage.mtpara;
                     if (mtpara!=null) {
   
                     switch (format) {
                        case "Protobuf":
                            log.error("Error on request #" + i +": Execute failed,\n"
                                    + "throw BatchUpdateException when deal whith the kafka message ."
                                    + "offset:["+mtpara.getOffset()+"],"
                                    + "table:["+rowMessage.schemaName+"."+rowMessage.tableName+"],"
                                    + "operate type:["+rowMessage.GetOperatorType()+"],"
                                    + "source message:["+mtpara.getMessage() +"]\n"
                                    + "parsed message:["+rowMessage.messagePro+"]");
                            break;
                        case "Json":
                        case "Unicom":
                        case "UnicomJson":
                            log.error("Error on request #" + i +": Execute failed,\n"
                                    + "throw BatchUpdateException when deal whith the kafka message ."
                                    + "offset:["+mtpara.getOffset()+"],"
                                    + "table:["+rowMessage.schemaName+"."+rowMessage.tableName+"],"
                                    + "operate type:["+rowMessage.GetOperatorType()+"],"
                                    + "source message:["+mtpara.getMessage() +"]");
                            break;
                        case "HongQuan":
                            log.error("Error on request #" + i +": Execute failed,\n"
                                    + "throw BatchUpdateException when deal whith the kafka message ."
                                    + "table:["+rowMessage.schemaName+"."+rowMessage.tableName+"],"
                                    + "offset:["+mtpara.getOffset()+"],"
                                    + "operate type:["+rowMessage.GetOperatorType()+"],"
                                    + "source message:["+mtpara.getMessage() +"]\n"
                                    + "parsed message:["+new String((rowMessage.data))+"]");
                            break;
                        default:
                            log.error("Error on request #" + i +": Execute failed,\n"
                                    + "throw BatchUpdateException when deal whith the kafka message ."
                                    + "table:["+rowMessage.schemaName+"."+rowMessage.tableName+"],"
                                    + "offset:["+mtpara.getOffset()+"],"
                                    + "operate type:["+rowMessage.GetOperatorType()+"],"
                                    + "source message:["+mtpara.getMessage() +"]");
                            break;
                     }
                    }
                 }
                 errInsert ++;
             }
         }
        if (log.isDebugEnabled()) {
            log.trace("exit function");
        }
     }

    public BufferedOutputStream init_bufferOutput(String filepath,String schemaName,String tableName) {
        BufferedOutputStream bufferedOutput =null;
        if (log.isDebugEnabled()) {
            log.trace("enter function,filepath:"+filepath+",schemaName:"+schemaName+","
                    + "tableName:"+tableName);
        }
        if (filepath!=null) {
            
            // create parent path
            if (!filepath.substring(filepath.length()-1).equals("/")) 
                filepath=filepath+"/";
            String fullOutPutPath=filepath+schemaName +"/"+tableName;
            if (log.isDebugEnabled()) {
                log.trace("filepath:"+fullOutPutPath);
            }
            File file = new File(fullOutPutPath);
            if (!file.getParentFile().exists()) {
                if (!file.getParentFile().mkdirs()) {
                   log.error("create kafka error message output path [" + schemaName +"] dir faild");
                }
            }
           // new output buff
            try {
               bufferedOutput = new BufferedOutputStream(new FileOutputStream(fullOutPutPath,true));
            } catch (FileNotFoundException e) {
                log.error("file notfound when init Outputpath buffer.",e);
            } 
        }
        if (log.isDebugEnabled()) {
            log.trace("exit function");
        }
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

    public void ClearCache() {
        if (commited) {
            tableInfo.IncInsertRows(insertRows.size());
            tableInfo.IncUpdateRows(updateRows.size());
            tableInfo.IncDeleteRows(deleteRows.size());

            tableInfo.IncInsMsgNum(cacheInsert);
            tableInfo.IncUpdMsgNum(cacheUpdate);
            tableInfo.IncKeyMsgNum(cacheUpdkey);
            tableInfo.IncDelMsgNum(cacheDelete);
        }

            tableInfo.IncErrInsNum(errInsert);
            tableInfo.IncErrUpdNum(errUpdate);
            tableInfo.IncDelMsgNum(errDelete);

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

    private long insert_row_data(Map<Integer, ColumnValue> row,int offset) throws Exception {
        long result = 1;
        StringBuffer strBuffer=null;

        if (log.isTraceEnabled()) {
            log.trace("enter function,offset:["+offset+ "]");
        }

        if (log.isDebugEnabled()) {
            strBuffer = new StringBuffer();
        }

        for (int i = 0; i < columns.size(); i++) {
            ColumnInfo columnInfo = columns.get(i);
            ColumnValue columnValue = row.get(columnInfo.GetColumnOff());
            if (columnValue == null || columnValue.CurValueIsNull()) {
                if (log.isDebugEnabled()) {
                    strBuffer.append("\tcolumn: " + i + " [null]\n");
                }
                insertStmt.setNull(i + 1, columnInfo.GetColumnType());
            } else {
                if (log.isDebugEnabled()) {
                    strBuffer.append("\tcolumn: " + i + ", value [" + columnValue.GetCurValue() 
                                     + "]\n");
                }
                try {
                    insertStmt.setString(i + 1, columnValue.GetCurValue());
                } catch (SQLException se) {
                    if (!(se.getErrorCode()==-29002) && !(se.getErrorCode()==-29154)) {
                        log.error("\nThere is a error when set value to insertStmt, the paramInt,["
                                +(i+1)+","+columnValue.GetCurValue()+"]");
                    }
                    throw se;
                }catch (Exception e) {
                    log.error("\nThere is a error when set value to insertStmt, the paramInt,["
                            +(i+1)+","+columnValue.GetCurValue()+"]");
                    throw e;
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
            log.trace("exit function,offset:[" + offset + "], insert row: [" + result + "]");
        }

        return result;
    }

    private void insert_data() throws Exception {
        if (insertRows.size() <= 0)
            return;

        if (log.isTraceEnabled()) {
            log.trace("enter function");
        }
        if (log.isDebugEnabled()) {
            log.debug("insert rows [cache row: " + cacheInsert + ", cache: " +
                    + insertRows.size() + "]\n");
        }

        int offset = 0;
        Map<Integer, RowMessage> errRows = new HashMap<Integer, RowMessage>(0);
        for (RowMessage insertRM : insertRows.values()) {
            Map<Integer, ColumnValue> cols = insertRM.GetColumns();
            if (log.isDebugEnabled()) {
                log.debug("insert row offset: " + offset+ ",rowmessage:"
                        +cols.get(0).GetCurValue()+"\n");
            }
            try {
                insert_row_data(cols,offset);
            }catch (SQLException se) {
                if (!(se.getErrorCode()==-29002) && !(se.getErrorCode()==-29154)) {
                    matchErr(insertRM);
                    errInsert++;
                }
                throw se;
            } catch (Exception e) {
                matchErr(insertRM);
                errInsert++;
                throw e;
            }
            errRows.put(offset, insertRM);
            offset++;
        }

        try {
            insertStmt.executeBatch();
        } catch (BatchUpdateException bue) {
            // print the error data 
            int[] insertCounts = bue.getUpdateCounts();
            printBatchErrMess(insertCounts,errRows);
            throw bue;
        } catch (IndexOutOfBoundsException iobe) {
           errInsert++;
           throw iobe;
        }catch (SQLException se) {
            if (!(se.getErrorCode()==-29002) && !(se.getErrorCode()==-29154)) {
                errInsert++;
            }
            throw se;
        }finally {
            errRows.clear();
        }

        if (log.isTraceEnabled()) {
            log.trace("exit function");
        }
    }

    private long update_row_data(Map<Integer, ColumnValue> row,int offset) throws SQLException  {
        long result = 0;
        StringBuffer strBuffer = null;

        if (log.isTraceEnabled()) {
            log.trace("enter function,offset:["+offset+"]");
        }
        if (log.isDebugEnabled()) {
            strBuffer = new StringBuffer();
        }

        ColumnInfo columnInfo = null;
        String updateSql = "UPDATE  \"" + schemaName + "\"." + "\"" + tableName + "\"" + " SET ";
        ColumnInfo keyInfo = null;
        ColumnValue keyValue = null;
        String whereSql = null;

        for (int i = 0; i < keyColumns.size(); i++) {
            keyInfo = keyColumns.get(i);
            keyValue = row.get(keyInfo.GetColumnOff());
            if (keyValue == null)
                continue;

            if (whereSql == null) {
                whereSql = " WHERE ";
            } else {
                whereSql += " AND ";
            }

            whereSql += keyInfo.GetColumnName() + keyValue.GetOldCondStr();
        }

        for (ColumnValue columnValue : row.values()) {
            if (columnInfo != null)
                updateSql += ", ";

            columnInfo = columns.get(columnValue.GetColumnID());
            updateSql += columnInfo.GetColumnName() + " = " + columnValue.GetCurValueStr();
            if (log.isDebugEnabled()) {
                strBuffer.append("\tcolumn: " + columnInfo.GetColumnOff() + ", value ["
                        + columnValue.GetCurValue() + "]\n");
            }
        }

        updateSql += whereSql + ";";

        if (log.isDebugEnabled()) {
            strBuffer.append("\tupdate sql [" + updateSql + "]\n");
            log.debug(strBuffer.toString());
        }

        Statement st = dbConn.createStatement();
        try {
            st.executeUpdate(updateSql);
        } catch (SQLException se) {
            log.error("\nthere is a error when execute the update sql,the execute sql:["+updateSql+"]");
            throw se;
        }
        st.close();

        result = 1;

        if (log.isTraceEnabled()) {
            log.trace("exit function,["+offset+"], insert row: [" + result + "]");
        }

        return result;
    }

    public void update_data() throws SQLException  {
        if (log.isTraceEnabled()) {
            log.trace("enter function");
        }

        if (log.isDebugEnabled()) {
            log.debug("update rows [cache update row: " + cacheUpdate + ", cache updkey row: "
                    + cacheUpdkey + ", cache: " + updateRows.size() + "]");
        }

        if (updateRows.size() <= 0)
            return;

        int offset = 0;
        Map<Integer, RowMessage> errRows = new HashMap<Integer, RowMessage>(0);
        for (RowMessage updateRow : updateRows.values()) {
            if (log.isDebugEnabled()) {
                log.debug("update row offset: " + offset + "\n");
            }
            errRows.put(offset, updateRow);
            if (updateRow != null) {
                try {
                    update_row_data(updateRow.GetColumns(),offset);
                } catch (SQLException se) {
                    if (!(se.getErrorCode()==-29002) && !(se.getErrorCode()==-29154)) {
                        matchErr(updateRow);
                        errUpdate++;
                    }
                    throw se;
                }
                offset++;
            }
        }

        if (log.isTraceEnabled()) {
            log.trace("exit function");
        }
    }

    private long delete_row_data(Map<Integer, ColumnValue> row,int offset) throws SQLException {
        long result = 1;
        StringBuffer strBuffer = null;

        if (log.isTraceEnabled()) {
            log.trace("enter function ,offset:["+offset +"]");
        }

        if (log.isDebugEnabled()) {
            strBuffer = new StringBuffer();
        }
        for (int i = 0; i < keyColumns.size(); i++) {
            ColumnInfo keyInfo = keyColumns.get(i);
            ColumnValue keyValue = row.get(keyInfo.GetColumnOff());

            if (keyValue == null || keyValue.OldValueIsNull()) {
                if (havePK) {
                    String key = get_key_value(null, row, false);
                    log.error("the primary key value is null [table:" + schemaName + "." + tableName
                            + ", column:" + keyInfo.GetColumnName() + "]");
                    result = 0;
                    break;
                }
                deleteStmt.setNull(i + 1, keyInfo.GetColumnType());
            } else {
                if (log.isDebugEnabled()) {
                    strBuffer.append("\tkey id:" + i + ", column id:" + keyInfo.GetColumnOff() 
                            + ", key [" + keyValue.GetOldValue() + "]");
                }
                try {
                    deleteStmt.setString(i + 1, keyValue.GetOldValue());
                } catch (SQLException se) {
                    if (!(se.getErrorCode()==-29002) && !(se.getErrorCode()==-29154)) {
                        log.error("\nThere is a error when set value to deleteStmt,the errorCode["
                                +se.getErrorCode()+"],the paramInt,["+(i+1)+","+keyValue.GetOldValue()+"]");
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
            log.trace("enter function,offset:["+ offset +"], delete row [" + result + "]");
        }

        return result;
    }

    private void delete_data() throws Exception {
        if (log.isTraceEnabled()) {
            log.trace("enter function");
        }

        if (log.isDebugEnabled()) {
            log.debug("delete rows [cache row: " + cacheDelete + ", cache: " + deleteRows.size()
                    + "]");
        }

        if (deleteRows.size() <= 0)
            return;

        int offset = 0;
        Map<Integer, RowMessage> errRows = new HashMap<Integer, RowMessage>(0);
        for (RowMessage deleteRow : deleteRows.values()) {
            if (log.isDebugEnabled()) {
                log.debug("delete row offset: " + offset + "\n");
            }
            errRows.put(offset, deleteRow);
            try {
                delete_row_data(deleteRow.GetColumns(),offset);
            }catch (SQLException se) {
                if (!(se.getErrorCode()==-29002) && !(se.getErrorCode()==-29154)) {
                    matchErr(deleteRow);
                    errDelete++;
                }
            } catch (Exception e) {
                matchErr(deleteRow);
                errDelete++;
                throw e;
            }
            offset++;
        }

        try {
            int[] batchResult = deleteStmt.executeBatch();
        } catch (BatchUpdateException bue) {
            // print the error data 
            int[] deleteCounts = bue.getUpdateCounts();
            printBatchErrMess(deleteCounts,errRows);
            errDelete+=deleteCounts.length;
            throw bue;
        }catch (IndexOutOfBoundsException iobe) {
             errDelete++;
           throw iobe;
        }catch (SQLException se) {
            if (!(se.getErrorCode()==-29002) && !(se.getErrorCode()==-29154)) {
                errDelete++;
            }
            throw se;
        }finally {
            errRows.clear();
        }

        if (log.isTraceEnabled()) {
            log.trace("exit function");
        }
    }
    public void matchErr(RowMessage operateRM) {
        if (operateRM.mtpara!=null) {
            switch (format) {
                case "Protobuf":
                    log.error("kafka offset:["+operateRM.mtpara.getOffset()+"],"
                            + "table:["+operateRM.schemaName+"."+operateRM.tableName+"],"
                            + "operate type:["+operateRM.GetOperatorType()+"],"
                            + "source message:["+operateRM.mtpara.getMessage() +"]\n"
                            + "parsed message:["+operateRM.messagePro+"]");
                    break;
                case "Json":
                case "Unicom":
                case "UnicomJson":
                    log.error("kafka offset:["+operateRM.mtpara.getOffset()+"],"
                            + "table:["+operateRM.schemaName+"."+operateRM.tableName+"],"
                            + "operate type:["+operateRM.GetOperatorType()+"],"
                            + "source message:["+operateRM.mtpara.getMessage() +"]\n");
                    break;
                case "HongQuan":
                    log.error("kafka offset:["+operateRM.mtpara.getOffset()+"],"
                            + "table:["+operateRM.schemaName+"."+operateRM.tableName+"],"
                            + "operate type:["+operateRM.GetOperatorType()+"],"
                            + "source message:["+operateRM.mtpara.getMessage() +"]\n"
                            + "parsed message:["+new String((operateRM.data)) +"]");
                    break;

                default:
                    log.error("kafka offset:["+operateRM.mtpara.getOffset()+"],"
                            + "table:["+operateRM.schemaName+"."+operateRM.tableName+"],"
                            + "operate type:["+operateRM.GetOperatorType()+"],"
                            + "source message:["+operateRM.mtpara.getMessage() +"]");
                    break;
            }
            }
    }

    public String GetTableName() {
        return tableName;
    }

    public String GetSchemaName() {
        return schemaName;
    }

    public void AddColumn(ColumnInfo column) {
        columns.add(column);
        columnMap.put(column.GetColumnID(), column);
    }

    public ColumnInfo GetColumn(int index) {
        return columns.get(index);
    }

    public ColumnInfo GetColumnFromMap(int colid) {
        return columnMap.get(colid);
    }

    public long GetColumnCount() {
        return columns.size();
    }

    public void AddKey(ColumnInfo column) {
        keyColumns.add(column);
    }

    public ColumnInfo GetKey(int index) {
        return keyColumns.get(index);
    }

    public long GetKeyCount() {
        return keyColumns.size();
    }

    public long GetCacheInsert() {
        return commited ? cacheInsert : 0;
    }

    public long GetCacheUpdate() {
        return commited ? cacheUpdate : 0;
    }

    public long GetCacheUpdkey() {
        return commited ? cacheUpdkey : 0;
    }

    public long GetCacheDelete() {
        return commited ? cacheDelete : 0;
    }

    public long GetInsertRows() {
        return commited ? insertRows.size() : 0;
    }

    public long GetUpdateRows() {
        return commited ? updateRows.size() : 0;
    }

    public long GetDeleteRows() {
        return commited ? deleteRows.size() : 0;
    }

    public long GetErrInsertRows() {
        return errInsert;
    }

    public long GetErrUpdateRows() {
        return errUpdate;
    }

    public long GetErrDeleteRows() {
        return errDelete;
    }

    public long InsertMessageToTable(RowMessage urm) {
        if (log.isTraceEnabled()) {
            log.trace("enter function");
        }
        msgs.add(urm);

        switch (urm.GetOperatorType()) {
            case "I":
                InsertRow(urm);
                break;
            case "U":
                UpdateRow(urm);
                break;
            case "K":
                UpdateRowWithKey(urm);
                break;
            case "D":
                DeleteRow(urm);
                break;

            default:
                log.error("operator [" + urm.GetOperatorType() + "]");
                return 0;
        }

        if (log.isTraceEnabled()) {
            log.trace("exit function");
        }
        return 1;
    }

    public TableInfo GetTableInfo() {
        return tableInfo;
    }
}
