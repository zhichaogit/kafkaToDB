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

    private long              insertNum  = 0;
    private long              updateNum  = 0;
    private long              deleteNum  = 0;

    private long              cacheInsert= 0;
    private long              cacheUpdate= 0;
    private long              cacheDelete= 0;
    private long              cacheRows  = 0;

    private long              commitCount= 0;

    private Connection        dbConn     = null;

    private PreparedStatement insertStmt = null;
    private PreparedStatement deleteStmt = null;

    ArrayList<ColumnInfo>     keyColumns = null;
    ArrayList<ColumnInfo>     columns    = null;

    Map<String, Map<Integer, ColumnValue>> insertRows = null;
    Map<String, Map<Integer, ColumnValue>> updateRows = null;
    Map<String, Map<Integer, ColumnValue>> deleteRows = null;

    private static Logger     log = Logger.getLogger(TableInfo.class);

    public TableInfo(String schemaName_, String tableName_, long commitCount_)
    {
	schemaName = schemaName_;
	tableName  = tableName_;
	commitCount= commitCount_;

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

	log.debug ("[" + insertSql + "]");
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

	log.debug ("[" + deleteSql + "]");
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

    private String get_key_value(Map<Integer, ColumnValue> rowValues, boolean cur)
    {
	String key = null;

	for(int i = 0; i < keyColumns.size(); i++) {
	    ColumnInfo  keyInfo = keyColumns.get(i);
	    ColumnValue column  = rowValues.get(keyInfo.GetColumnID());

	    if (cur) {
		if (column.CurValueIsNull()){
		    // TODO add message
		    log.error("the cur primarykey value is null. column name [" 
			      + keyInfo.GetColumnName() + "]");
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
		    // TODO add message
		    log.error("the old primarykey value is null. column name [" 
			      + keyInfo.GetColumnName() + "]");
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
	log.debug ("enter function");

	Map<Integer, ColumnValue> rowValues = rowMessage.GetColumns();

	String key = get_key_value(rowValues, true);

	log.debug ("insert row key [" + key + "], message [" 
		   + rowMessage.GetMessage() + "]");
	// new row is inserted
	insertRows.put(key, rowValues);

	// remove the update messages
	updateRows.remove(key);

	// remove the delete messages
	deleteRows.remove(key);

	cacheInsert++;

	log.debug ("exit function cache insert [" + cacheInsert + "]");

	return 1;
    }

    public long UpdateRow(RowMessage rowMessage)
    {
	log.debug ("enter function");

	Map<Integer, ColumnValue> rowValues = rowMessage.GetColumns();
	String key = get_key_value(rowValues, false);

	log.debug ("update row key [" + key + "], message [" 
		   + rowMessage.GetMessage() + "]");
	Map<Integer, ColumnValue> insertRow = insertRows.get(key);

	if (insertRow != null){
	    /*
	     * exist in insert map, must be not exist in update and delete 
	     * update the insert row in memory
	     */
	    for (ColumnValue value : rowValues.values()) {
		insertRow.put(value.GetColumnID(), value);
	    }

	    log.debug ("row key [" + key + "] in insert cache");
	    for (ColumnValue value : insertRow.values()) {
		log.debug ("\tColumn [" + value.GetColumnID()
			   + ":" + value.GetCurValue() 
			   + ":" + value.GetOldValue() +  "]");
	    }

	    insertRows.put(key, insertRow);
	} else {
	    Map<Integer, ColumnValue> deleterow = deleteRows.get(key);
	    if (deleterow != null){
		log.error("update row is exist in delete map [" + key 
			  + "], the message [" + rowMessage.GetMessage() + "]");
		return 0;
	    } else {
		Map<Integer, ColumnValue> updateRow = updateRows.get(key);

		if (updateRow != null) {
		    for (ColumnValue value : rowValues.values()) {
			updateRow.put(value.GetColumnID(), value);
		    }
		} else {
		    updateRow = rowValues;
		}

		updateRows.put(key, updateRow);
		log.debug("update row key [" + key + "], row: " + updateRow 
			  + ", size: " + updateRows.size());

		for (Map<Integer, ColumnValue> row : updateRows.values()){
		    log.debug("row: " + row);
		    for (ColumnValue rowvalue : row.values()){
			log.debug("\tColumn [" + rowvalue.GetColumnID() 
				  + ", " + rowvalue.GetCurValue() 
				  + "," + rowvalue.GetOldValue() + "]");
		    }
		}
	    }
	}

	cacheUpdate++;

	log.debug ("exit function cache update [" + cacheUpdate + "]");

	return 1;
    }

    public long UpdateRowWithKey(RowMessage rowMessage)
    {
	log.debug ("exit function");

	Map<Integer, ColumnValue> rowValues = rowMessage.GetColumns();
	String oldkey = get_key_value(rowValues, false);
	String newkey = get_key_value(rowValues, true);

	Map<Integer, ColumnValue> insertRow = insertRows.get(oldkey);

	if (insertRow != null){
	    /*
	     * exist in insert map, must be not exist in update and delete 
	     * update the insert row in memory
	     */
	    for (ColumnValue value : rowValues.values()) {
		insertRow.put(value.GetColumnID(), value);
	    }

	    log.debug ("row key [" + oldkey + ":" + newkey + "] in insert cache");
	    for (ColumnValue value : insertRow.values()) {
		log.debug ("\tColumn [" + value.GetColumnID()
			   + ":" + value.GetCurValue() 
			   + ":" + value.GetOldValue() +  "]");
	    }
	    // remove old key
	    insertRows.remove(oldkey);

	    // insert the new key
	    insertRows.put(newkey, insertRow);
	} else {
	    Map<Integer, ColumnValue> deleterow = deleteRows.get(oldkey);

	    if (deleterow != null){
		log.error("update row is exist in delete map [" + oldkey 
			  + "], the message [" + rowMessage.GetMessage() + "]");
		return 0;
	    } else {
		Map<Integer, ColumnValue> updateRow = updateRows.get(oldkey);

		if (updateRow != null) {
		    for (ColumnValue value : rowValues.values()) {
			updateRow.put(value.GetColumnID(), value);
		    }
		} else {
		    updateRow = rowValues;
		}
		// remove old key
		updateRows.remove(oldkey);

		log.debug ("updkey row key [" + oldkey + ":" + newkey 
			   + "], message [" + rowMessage.GetMessage() + "]");
		updateRows.put(newkey, updateRow);
	    }
	}

	cacheUpdate++;
	log.debug ("exit function cache update key [" + cacheUpdate + "]");

	return 1;
    }

    public long DeleteRow(RowMessage rowMessage)
    {
	log.debug ("exit function");

	Map<Integer, ColumnValue> rowValues = rowMessage.GetColumns();
	String key = get_key_value(rowValues, false);

	// delete cur row
	log.debug ("delete row key [" + key + "], message [" 
		   + rowMessage.GetMessage() + "]");
	deleteRows.put(key, rowValues);

	// remove the insert messages
	insertRows.remove(key);

	// remove the update messages
	updateRows.remove(key);

	cacheDelete++;

	log.debug ("exit function cache delete [" + cacheDelete + "]");

	return 1;
    }

    private void check_flush_cache() {
	if (cacheRows % commitCount == 0) {
	    flush_cache();
	}
    }

    private void flush_cache() {
	insert_data();
	update_data();
	delete_data();
    }

    private long insert_row_data(Map<Integer, ColumnValue> row)
    {
	long result = 1;

	log.trace("enter function");

	try {
	    for (int i = 0; i < columns.size(); i++) {
		ColumnInfo columnInfo = columns.get(i);
		ColumnValue columnValue = row.get(columnInfo.GetColumnID());
		if (columnValue == null || columnValue.CurValueIsNull() ) {
		    log.debug("\tcolumn: " + i + " [null]");
		    insertStmt.setNull(i+1, columnInfo.GetColumnType()); 
		} else {
		    log.debug("\tcolumn: " + i + " ["+ columnValue.GetCurValue() 
			      + "]");
		    insertStmt.setString(i+1, columnValue.GetCurValue());
		}
	    }

	    if (result == 1) {
		insertStmt.addBatch();
	    }
	} catch (SQLException e) {
	    log.error ("Prepare insert stmt exception");
	    do{
		e.printStackTrace();
		e = e.getNextException();
	    }while(e!=null); 
	}

	log.trace("enter function [" + result + "]");

	return result;
    }

    private long insert_data()
    {
	long result = 0;

	log.debug("enter function, insert rows: " + insertRows.size());

	try {
	    int offset = 0;
	    for (Map<Integer, ColumnValue> insertRow : insertRows.values()){
		log.debug("insert row: " + cacheInsert + ", offset: " + offset
			  + ", inserted: " + result);
		offset++;
		result += insert_row_data(insertRow);
	    }

	    int [] batchResult = insertStmt.executeBatch();
	    dbConn.commit();
	    insertRows.clear();
	    insertNum += cacheInsert;
	    cacheInsert = 0;
	} catch (BatchUpdateException bue) {
	    int[] insertCounts = bue.getUpdateCounts();
	    int count = 1;

	    for (int i : insertCounts) {
		if ( i == Statement.EXECUTE_FAILED ) 
		    log.error("error on request #" + count +" execute failed");
		else 
		    count++;
	    }
	    log.error(bue.getMessage());
	    bue.printStackTrace();
	} catch (IndexOutOfBoundsException iobe) {
	    log.error ("IndexOutOfBounds exception");
	    iobe.printStackTrace();
	} catch (SQLException e) {
	    log.error ("Execute batch insert stmt exception");
	    do{
		e.printStackTrace();
		e = e.getNextException();
	    }while(e!=null); 
	}

	log.debug("exit function [" + result + "]");
	return result;
    }

    private long update_row_data(Map<Integer, ColumnValue> row) {
	ColumnInfo  columnInfo  = null;
	String      updateSql   = "UPDATE  " + schemaName + "." 
	    + tableName + " SET ";
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
	}

	updateSql += whereSql + ";";

	log.debug ("update sql: [" + updateSql + "]");

	try {
	    Statement st = dbConn.createStatement();
	    st.executeUpdate(updateSql);
	    st.cancel();
	} catch (SQLException e) {
	    log.error ("Prepare insert stmt exception");
	    do{
		e.printStackTrace();
		e = e.getNextException();
	    }while(e!=null); 
	}

	return 1;
    }

    public long update_data()
    {
	long             result = 0;

	log.debug("enter function");

	try {
	    int offset = 0;
	    for (Map<Integer, ColumnValue> updateRow : updateRows.values()){
		log.debug("update row: " + cacheUpdate + ", offset: " + offset
			  + ", updated: " + result + ", row: " + updateRow);
		if (updateRow != null) {
		    offset++;
		    result += update_row_data(updateRow);
		}
	    }

	    dbConn.commit();
	    updateRows.clear();
	    updateNum += cacheUpdate;
	    cacheUpdate = 0;
	} catch (BatchUpdateException bue) {
	    int[] updateCounts = bue.getUpdateCounts();
	    int count = 1;

	    for (int i : updateCounts) {
		if ( i == Statement.EXECUTE_FAILED ) 
		    log.error("error on request #" + count +" execute failed");
		else 
		    count++;
	    }
	    log.error(bue.getMessage());
	    bue.printStackTrace();
	} catch (IndexOutOfBoundsException iobe) {
	    log.error ("IndexOutOfBounds exception");
	    iobe.printStackTrace();
	} catch (SQLException e) {
	    log.error ("Execute update stmt exception");
	    do{
		e.printStackTrace();
		e = e.getNextException();
	    }while(e!=null); 
	}

	log.debug("exit function [" + result + "]");
	return result;
    }

    private long delete_row_data(Map<Integer, ColumnValue> row)
    {
	long result = 1;

	log.trace("enter function");

	try {
	    for (int i = 0; i < keyColumns.size(); i++) {
		ColumnInfo keyInfo = keyColumns.get(i);
		ColumnValue keyValue = row.get(keyInfo.GetColumnID());
		if (keyValue.OldValueIsNull()) {
		    String key = get_key_value(row, false);
		    log.error("the primary key value is null [table:" 
			      + schemaName + "." + tableName + ", column:"
			      + keyInfo.GetColumnName() + "]");
		    result = 0;
		    break;
		} else {
		    log.debug("\tkey " + i + ":" + keyInfo.GetColumnID()
			      + " ["+ keyValue.GetOldValue() + "]");
		    deleteStmt.setString(i+1, keyValue.GetOldValue());
		}
	    }

	    if (result == 1) {
		deleteStmt.addBatch();
	    }
	} catch (SQLException e) {
	    log.error ("Prepare insert stmt exception");
	    do{
		e.printStackTrace();
		e = e.getNextException();
	    }while(e!=null); 
	}

	log.trace("enter function [" + result + "]");

	return result;
    }

    private long delete_data()
    {
	long result = 0;

	log.debug("enter function");

	try {
	    int offset = 0;
	    for (Map<Integer, ColumnValue> deleteRow : deleteRows.values()){
		log.debug("delete row: " + cacheDelete + ", offset: " + offset
			  + ", result: " + result);
		offset++;
		result += delete_row_data(deleteRow);
	    }

	    int [] batchResult = deleteStmt.executeBatch();
	    dbConn.commit();
	    deleteRows.clear();
	    deleteNum += cacheDelete;
	    cacheDelete = 0;
	} catch (BatchUpdateException bue) {
	    int[] deleteCounts = bue.getUpdateCounts();
	    int count = 1;

	    for (int i : deleteCounts) {
		if ( i == Statement.EXECUTE_FAILED ) 
		    log.error("error on request #" + count +" execute failed");
		else 
		    count++;
	    }
	    log.error(bue.getMessage());
	    bue.printStackTrace();
	} catch (IndexOutOfBoundsException iobe) {
	    log.error ("IndexOutOfBounds exception");
	    iobe.printStackTrace();
	} catch (SQLException e) {
	    log.error ("Execute batch delete stmt exception");
	    do{
		e.printStackTrace();
		e = e.getNextException();
	    }while(e!=null); 
	}

	log.debug("exit function [" + result + "]");
	return result;
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

    public long GetCacheRows()
    {
	return cacheRows;
    }

    public long InsertMessageToTable(RowMessage urm)
    {
	long        num = 0;
	log.trace("enter function");

	switch(urm.GetOperatorType()) {
	case "I":
	    num = InsertRow(urm);
	    break;
	case "U":
	    num = UpdateRow(urm);
	    break;
	case "K":
	    num = UpdateRow(urm);
	    break;
	case "D":
	    num = DeleteRow(urm);
	    break;

	default:
	    log.error("operator [" + urm.GetOperatorType() + "]");
	    return num;
	}

	cacheRows++;

	check_flush_cache();

	log.trace("exit function");
	return num;
    }

    public void CommitAllTable(Connection dbConn_)
    {
	log.debug("enter function [" + dbConn + ":" + dbConn_ + "]");

	flush_cache();

	log.debug("exit function");
    }

    public void DisplayStat()
    {
	log.info("Table " + schemaName + "." + tableName + " state [insert: "
		 + insertNum + ", update: " + updateNum + ", delete: " 
		 + deleteNum + "]");
    }    
}

