import java.sql.Statement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.BatchUpdateException;
import java.lang.IndexOutOfBoundsException;

import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import org.apache.log4j.Logger; 
 
public class TableInfo
{
    private String            schemaName = null;
    private String            tableName  = null;

    private long              insMsgNum  = 0;
    private long              updMsgNum  = 0;
    private long              keyMsgNum  = 0;
    private long              delMsgNum  = 0;

    private long              insertNum  = 0;
    private long              updateNum  = 0;
    private long              deleteNum  = 0;

    private long              cacheInsert= 0;
    private long              cacheUpdate= 0;
    private long              cacheUpdkey= 0;
    private long              cacheDelete= 0;

    private Connection        dbConn     = null;

    private PreparedStatement insertStmt = null;
    private PreparedStatement deleteStmt = null;

    ArrayList<ColumnInfo>     keyColumns = null;
    ArrayList<ColumnInfo>     columns    = null;

    Map<String, Map<Integer, ColumnValue>> insertRows = null;
    Map<String, Map<Integer, ColumnValue>> updateRows = null;
    Map<String, Map<Integer, ColumnValue>> deleteRows = null;

    private static Logger     log = Logger.getLogger(TableInfo.class);

    public TableInfo(String schemaName_, String tableName_)
    {
	schemaName = schemaName_;
	tableName  = tableName_;

	columns    = new ArrayList<ColumnInfo>(0);
	keyColumns = new ArrayList<ColumnInfo>(0);

	insertRows = new HashMap<String, Map<Integer, ColumnValue>>(0);
	updateRows = new HashMap<String, Map<Integer, ColumnValue>>(0);
	deleteRows = new HashMap<String, Map<Integer, ColumnValue>>(0);
    }

    public boolean InitStmt(Connection dbConn_)
    {
	if (dbConn == null) {
	    dbConn = dbConn_;
	    init_insert_stmt();
	    init_delete_stmt();
	} else if (dbConn != dbConn_) {
	    log.error ("table: " + schemaName + "." + tableName + 
		       " is reinited, dbConn: " + dbConn + ", the new dbConn: "
		       + dbConn_);
	    return false;
	}

	return true;
    }

    public void init_insert_stmt() {
	ColumnInfo  column    = columns.get(0);
	String      valueSql  = ") VALUES(?";
	String      insertSql = "UPSERT USING LOAD INTO " + schemaName + "."
	    + tableName + "(" + column.GetColumnName();

	for(int i = 1; i < columns.size(); i++) {
	    column     = columns.get(i);
	    valueSql  += ", ?";
	    insertSql += ", " + column.GetColumnName();
	}
	
	insertSql += valueSql + ");";

	log.debug ("insert prepare statement [" + insertSql + "]");
	try {
	    insertStmt = dbConn.prepareStatement(insertSql);
	} catch (SQLException e) {
	    log.error ("Prepare insert stmt exception");
	    do{
		e.printStackTrace();
		e = e.getNextException();
	    }while(e!=null); 
	}
    }

    public String where_condition () {
	ColumnInfo keyInfo  = keyColumns.get(0);
	String     whereSql = " WHERE "+ keyInfo.GetColumnName() + " = ?";

	for(int i = 1; i < keyColumns.size(); i++) {
	    keyInfo = keyColumns.get(i);
	    whereSql += " AND " + keyInfo.GetColumnName() + " = ?";
	}
	return whereSql;
    }

    public void init_delete_stmt() {
	ColumnInfo keyInfo   = keyColumns.get(0);
	String     deleteSql = "DELETE FROM " + schemaName + "." 
	    + tableName;

	deleteSql += where_condition() + ";";

	log.debug ("delete prepare statement [" + deleteSql + "]");
	try {
	    deleteStmt = dbConn.prepareStatement(deleteSql);
	} catch (SQLException e) {
	    log.error ("Prepare delete stmt exception");
	    do{
		e.printStackTrace();
		e = e.getNextException();
	    }while(e!=null); 
	}
    }

    private String get_key_value(String message, 
				 Map<Integer, ColumnValue> rowValues, boolean cur)
    {
	String key = null;

	for(int i = 0; i < keyColumns.size(); i++) {
	    ColumnInfo  keyInfo = keyColumns.get(i);
	    ColumnValue column  = rowValues.get(keyInfo.GetColumnID());

	    if (cur) {
		if (column.CurValueIsNull()){
		    log.error("the cur primarykey value is null. column name [" 
			      + keyInfo.GetColumnName() + "] message [" 
			      + message + "]");
		    return null;
		} else {
		    /* 
		     * column is splited by "", in order to support
		     * key1:["aa", "a"] is different with key2:["a", "aa"];
		     */
		    if (key == null){
			key = column.GetCurValue();
		    } else {
			key += "" + column.GetCurValue();
		    }
		}
	    } else {
		if (column.OldValueIsNull()){
		    log.error("the old primarykey value is null. column name [" 
			      + keyInfo.GetColumnName() + "] message [" 
			      + message + "]");
		    return null;
		} else {
		    /* 
		     * column is splited by "", in order to support
		     * key1:["aa", "a"] is different with key2:["a", "aa"];
		     */
		    if (key == null){
			key = column.GetOldValue();
		    } else {
			key += "" + column.GetOldValue();
		    }
		}
	    }
	}

	return key;
    }

    public long InsertRow(RowMessage rowMessage)
    {
	log.trace ("enter function");

	Map<Integer, ColumnValue> rowValues = rowMessage.GetColumns();

	String message = rowMessage.GetMessage();
	String key = get_key_value(message, rowValues, true);

	log.debug ("insert row key [" + key + "], message [" + message + "]");
	// new row is inserted
	insertRows.put(key, rowValues);

	// remove the update messages
	updateRows.remove(key);

	// remove the delete messages
	deleteRows.remove(key);

	cacheInsert++;

	log.trace ("exit function cache insert [rows: " + cacheInsert 
		   + ", insert: " + insertRows.size()
		   + ", update: " + updateRows.size()
		   + ", delete: " + deleteRows.size() + "]");

	return 1;
    }

    public long check_update_key(Map<Integer, ColumnValue> rowValues,
				 String message)
    {
	ColumnValue cacheValue = null;

	for(int i = 0; i < keyColumns.size(); i++) {
	    ColumnInfo keyInfo = keyColumns.get(i);
	    cacheValue = rowValues.get(keyInfo.GetColumnID());
	    String oldValue = cacheValue.GetOldValue();
	    String curValue = cacheValue.GetCurValue();
	    log.debug("update the keys [" + oldValue + "] to [" + curValue 
		      + "]");
	    if (!curValue.equals(oldValue)) {
		log.error("U message cann't update the keys," 
			  + " message [" + message + "]");
		return 0;
	    }
	}

	return 1;
    }

    public long UpdateRow(RowMessage rowMessage)
    {
	log.trace ("enter function");

	Map<Integer, ColumnValue> rowValues = rowMessage.GetColumns();

	String message = rowMessage.GetMessage();
	if (check_update_key(rowValues, message) == 0)
	    return 0;

	String key = get_key_value(message, rowValues, false);

	log.debug ("update row key [" + key + "], message [" + message + "]");
	Map<Integer, ColumnValue> insertRow = insertRows.get(key);

	if (insertRow != null){
	    /*
	     * exist in insert map, must be not exist in update and delete 
	     * update the insert row in memory
	     */
	    for (ColumnValue value : rowValues.values()) {
		insertRow.put(value.GetColumnID(), value);
	    }

	    log.debug("update row key [" + key + "] is exist in insert cache");

	    insertRows.put(key, insertRow);
	} else {
	    Map<Integer, ColumnValue> deleterow = deleteRows.get(key);
	    if (deleterow != null){
		log.error("update row key is exist in delete cache [" + key 
			  + "], the message [" + message + "]");
		return 0;
	    } else {
		Map<Integer, ColumnValue> updateRow = updateRows.get(key);

		if (updateRow != null) {
		    ColumnValue cacheValue;

		    for (ColumnValue value : updateRow.values()) {
			cacheValue = rowValues.get(value.GetColumnID());
			if (cacheValue != null) {
			    value = new ColumnValue(cacheValue.GetColumnID(),
						    cacheValue.GetCurValue(),
						    value.GetOldValue());
			}

			rowValues.put(value.GetColumnID(), value);
		    }
		}

		updateRows.put(key, rowValues);
	    }
	}

	cacheUpdate++;

	log.trace ("exit function cache update [rows: " + cacheUpdate 
		   + ", insert: " + insertRows.size()
		   + ", update: " + updateRows.size()
		   + ", delete: " + deleteRows.size() + "]");

	return 1;
    }

    public long UpdateRowWithKey(RowMessage rowMessage)
    {
	log.trace ("exit function");

	Map<Integer, ColumnValue> rowValues = rowMessage.GetColumns();
	String message = rowMessage.GetMessage();
	String oldkey = get_key_value(message, rowValues, false);
	String newkey = get_key_value(message, rowValues, true);

	log.debug ("updkey row key [old key: " + oldkey + ", new key: " 
		   + newkey + "], message [" + message + "]");
	Map<Integer, ColumnValue> insertRow = insertRows.get(oldkey);

	if (insertRow != null){
	    /*
	     * exist in insert map, must be not exist in update and delete 
	     * update the insert row in memory
	     */
	    for (ColumnValue value : rowValues.values()) {
		insertRow.put(value.GetColumnID(), value);
	    }

	    log.debug("updkey row key [" + oldkey + "] exist in insert cache");
	    // remove old key
	    insertRows.remove(oldkey);

	    // insert the new key
	    insertRows.put(newkey, insertRow);
	} else {
	    Map<Integer, ColumnValue> deleterow = deleteRows.get(oldkey);

	    if (deleterow != null){
		log.error("update row key is exist in delete cache [" + oldkey 
			  + "], the message [" + message + "]");
		return 0;
	    } else {
		Map<Integer, ColumnValue> updateRow = updateRows.get(oldkey);

		log.debug ("updkey row [key: " + oldkey + "] in update cache ["
			   + updateRow + "]");
		if (updateRow != null) {
		    ColumnValue cacheValue;

		    for (ColumnValue value : rowValues.values()) {
			cacheValue = updateRow.get(value.GetColumnID());
			if (cacheValue != null) {
			    value = new ColumnValue(value.GetColumnID(),
						    value.GetCurValue(),
						    cacheValue.GetOldValue());
			}

			updateRow.put(value.GetColumnID(), value);
		    }
		} else {
		    updateRow = rowValues;
		}

		updateRows.remove(oldkey);

		updateRows.put(newkey, updateRow);
	    }
	}

	cacheUpdkey++;
	log.trace ("exit function cache updkey [rows: " + cacheUpdkey
		   + ", insert: " + insertRows.size()
		   + ", update: " + updateRows.size()
		   + ", delete: " + deleteRows.size() + "]");

	return 1;
    }

    public long DeleteRow(RowMessage rowMessage)
    {
	log.trace ("exit function");

	Map<Integer, ColumnValue> rowValues = rowMessage.GetColumns();
	String message = rowMessage.GetMessage();
	String key = get_key_value(message, rowValues, false);

	// delete cur row
	log.debug ("delete row key [" + key + "], message [" + message + "]");
	deleteRows.put(key, rowValues);

	// remove the insert messages
	insertRows.remove(key);

	// remove the update messages
	updateRows.remove(key);

	cacheDelete++;

	log.trace ("exit function cache delete [rows: " + cacheDelete 
		   + ", insert: " + insertRows.size() 
		   + ", update: " + updateRows.size()
		   + ", delete: " + deleteRows.size() + "]");

	return 1;
    }

    public boolean CommitTable() {
	try {
	    insert_data();
	    update_data();
	    delete_data();

	    dbConn.commit();
	} catch (BatchUpdateException bue) {
	    log.error ("batch update execute exception: " + bue.getMessage());
	    SQLException se = bue;

	    do{
		se.printStackTrace();
		se = se.getNextException();
	    }while(se != null); 

	    return false;
	} catch (IndexOutOfBoundsException iobe) {
	    log.error ("Index Out Of Bounds exception: " + iobe.getMessage());
	    iobe.printStackTrace();

	    return false;
	} catch (SQLException se) {
	    log.error ("batch update SQL exception: " + se.getMessage());
	    do{
		se.printStackTrace();
		se = se.getNextException();
	    }while(se != null); 

	    return false;
	} catch (Exception e) {
	    log.error ("batch update exception: " + e.getMessage());
	    e.printStackTrace();

	    return false;
	}

	return true;
    }

    public void ClearCache() {
	insertNum += insertRows.size();
	updateNum += updateRows.size();
	deleteNum += deleteRows.size();

	insertRows.clear();
	updateRows.clear();
	deleteRows.clear();

	insMsgNum += cacheInsert;
	updMsgNum += cacheUpdate;
	keyMsgNum += cacheUpdkey;
	delMsgNum += cacheDelete;

	cacheInsert = 0;
	cacheUpdate = 0;
	cacheUpdkey = 0;
	cacheDelete = 0;
    }

    private long insert_row_data(Map<Integer, ColumnValue> row) throws Exception
    {
	long result = 1;

	log.trace("enter function");

	for (int i = 0; i < columns.size(); i++) {
	    ColumnInfo columnInfo = columns.get(i);
	    ColumnValue columnValue = row.get(columnInfo.GetColumnID());
	    if (columnValue == null || columnValue.CurValueIsNull() ) {
		log.debug("\tcolumn: " + i + " [null]");
		insertStmt.setNull(i+1, columnInfo.GetColumnType()); 
	    } else {
		log.debug("\tcolumn: " + i + ", value [" 
			  + columnValue.GetCurValue() + "]");
		insertStmt.setString(i+1, columnValue.GetCurValue());
	    }
	}

	if (result == 1) {
	    insertStmt.addBatch();
	}

	log.trace("enter function insert row: [" + result + "]");

	return result;
    }

    private void insert_data() throws Exception
    {
	log.trace("enter function");

	log.debug("insert rows [cache row: " + cacheInsert + ", cache: " 
		  + insertRows.size() + "]");

	int offset = 0;
	for (Map<Integer, ColumnValue> insertRow : insertRows.values()){
	    log.debug("insert row offset: " + offset);
	    offset++;
	    insert_row_data(insertRow);
	}

	int [] batchResult = insertStmt.executeBatch();

	log.trace("exit function");
    }

    private long update_row_data(Map<Integer, ColumnValue> row) throws Exception
    {
	long        result = 0;

	log.trace("enter function");

	ColumnInfo  columnInfo  = null;
	String      updateSql   = "UPDATE  " + schemaName + "." + tableName 
	    + " SET ";
	ColumnInfo  keyInfo     = null;
	ColumnValue keyValue    = null;
	String      whereSql    = " WHERE ";

	for(int i = 0; i < keyColumns.size(); i++) {
	    keyInfo = keyColumns.get(i);
	    if (keyValue != null)
		whereSql += " AND ";
	    keyValue  = row.get(keyInfo.GetColumnID());
	    whereSql += keyInfo.GetColumnName() + keyValue.GetOldCondStr();
	}

	for(ColumnValue columnValue : row.values()) {
	    if (columnInfo != null)
		updateSql += ", ";
	    
	    columnInfo = columns.get(columnValue.GetColumnID());
	    updateSql += columnInfo.GetColumnName() + " = " 
		+ columnValue.GetCurValueStr();
	    log.debug("\tcolumn: " + columnInfo.GetColumnID() + ", value [" 
		      + columnValue.GetCurValue() + "]");
	}

	updateSql += whereSql + ";";

	log.debug ("update sql [" + updateSql + "]");

	Statement st = dbConn.createStatement();
	st.executeUpdate(updateSql);
	st.cancel();
	result = 1;

	log.trace("enter function insert row: [" + result + "]");

	return result;
    }

    public void update_data()  throws Exception
    {
	log.trace("enter function");

	log.debug("update rows [cache update row: " + cacheUpdate
		  + ", cache updkey row: " + cacheUpdkey + ", cache: "
		  + updateRows.size() + "]");

	int  offset = 0;
	for (Map<Integer, ColumnValue> updateRow : updateRows.values()){
	    log.debug("update row offset: " + offset);
	    if (updateRow != null) {
		offset++;
		update_row_data(updateRow);
	    }
	}

	log.trace("exit function");
    }

    private long delete_row_data(Map<Integer, ColumnValue> row)  throws Exception
    {
	long result = 1;

	log.trace("enter function");

	for (int i = 0; i < keyColumns.size(); i++) {
	    ColumnInfo keyInfo = keyColumns.get(i);
	    ColumnValue keyValue = row.get(keyInfo.GetColumnID());
	    if (keyValue.OldValueIsNull()) {
		String key = get_key_value(null, row, false);
		log.error("the primary key value is null [table:" 
			  + schemaName + "." + tableName + ", column:"
			  + keyInfo.GetColumnName() + "]");
		result = 0;
		break;
	    } else {
		log.debug("\tkey id:" + i + ", column id:" 
			  + keyInfo.GetColumnID() + ", key ["
			  + keyValue.GetOldValue() + "]");
		deleteStmt.setString(i+1, keyValue.GetOldValue());
	    }
	}

	if (result == 1) {
	    deleteStmt.addBatch();
	}

	log.trace("enter function delete row [" + result + "]");

	return result;
    }

    private void delete_data() throws Exception
    {
	log.trace("enter function");

	log.debug("delete rows [cache row: " + cacheDelete + ", cache: " 
		  + deleteRows.size() + "]");

	int offset = 0;
	for (Map<Integer, ColumnValue> deleteRow : deleteRows.values()){
	    log.debug("delete row offset: " + offset);
	    offset++;
	    delete_row_data(deleteRow);
	}

	int [] batchResult = deleteStmt.executeBatch();

	log.trace("exit function");
    }

    public String GetTableName()
    {
	return tableName;
    }

    public String GetSchemaName()
    {
	return schemaName;
    }

    public void AddColumn(ColumnInfo column)
    {
	columns.add(column);
    }

    public ColumnInfo GetColumn(int index)
    {
	return columns.get(index);
    }

    public long GetColumnCount()
    {
	return columns.size();
    }

    public void AddKey(ColumnInfo column)
    {
	keyColumns.add(column);
    }

    public ColumnInfo GetKey(int index)
    {
	return keyColumns.get(index);
    }

    public long GetKeyCount()
    {
	return keyColumns.size();
    }

    public long GetCacheInsert()
    {
	return cacheInsert;
    }

    public long GetCacheUpdate()
    {
	return cacheUpdate;
    }

    public long GetCacheUpdkey()
    {
	return cacheUpdkey;
    }

    public long GetCacheDelete()
    {
	return cacheDelete;
    }

    public long GetInsertRows() {
	return insertRows.size();
    }

    public long GetUpdateRows() {
	return updateRows.size();
    }

    public long GetDeleteRows() {
	return deleteRows.size();
    }

    public long InsertMessageToTable(RowMessage urm)
    {
	log.trace("enter function");

	switch(urm.GetOperatorType()) {
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

	log.trace("exit function");
	return 1;
    }

    public void DisplayStat(StringBuffer strBuffer)
    {
	strBuffer.append("\t" + schemaName + "." + tableName + " messages [I: " 
		 + insMsgNum + ", U: " + updMsgNum + ", K: " + keyMsgNum
		 + ", D: " + delMsgNum + "], table operator [insert: "
		 + insertNum + ", update: " + updateNum + ", delete: "
		 + deleteNum + "]\n");
    }    
}
