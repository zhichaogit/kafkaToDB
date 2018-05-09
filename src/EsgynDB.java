import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.DatabaseMetaData;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.BatchUpdateException;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.lang.StringBuffer;
import org.apache.log4j.Logger; 
import java.lang.IndexOutOfBoundsException;

import java.util.Date;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
 
public class EsgynDB
{
    String      dburl       = null;
    String      dbdriver    = null;
    String      dbuser      = null;
    String      dbpassword  = null;
    String      defschema   = null;
    String      deftable    = null;
    long        commitCount = 500;
    Connection  dbkeepconn  = null;
    String      keepQuery   = "values(1);";
    Map<String, SchemaInfo> schemas  = null;
    PreparedStatement       keepstmt = null;
    private static Logger   log = Logger.getLogger(EsgynDB.class);

    private long    messagenum = 0;
    private long    insertnum = 0;
    private long    updatenum = 0;
    private long    deletenum = 0;

    private long    begin;
    private Date    starttime;

    public EsgynDB(String  defschema_,
		   String  deftable_,
		   String  dburl_, 
		   String  dbdriver_, 
		   String  dbuser_, 
		   String  dbpassword_,
		   long    commitCount_) 
    {
	log.trace("enter function [" + defschema_ + "." + deftable_ 
		  + ", " + dburl_ + ", " + dbdriver_ + ", " + dbuser_ 
		  + ", " + commitCount_ + "]");

	dburl       = dburl_;
	dbdriver    = dbdriver_;
	dbuser      = dbuser_;
	dbpassword  = dbpassword_;
	defschema   = defschema_;
	deftable    = deftable_;
	commitCount = commitCount_;

	begin = new Date().getTime();
	starttime = new Date();
	
	schemas = new HashMap<String, SchemaInfo>(); 
	dbkeepconn = CreateConnection(true);
	try {
	    log.info("prepare the keepalive stmt, query:" + keepQuery); 
	    keepstmt = dbkeepconn.prepareStatement(keepQuery);
	} catch (SQLException e) {
	    log.error("prepare the keepalive stmt error.");
            e.printStackTrace();
	}
	log.info("start to init schemas:"); 
	init_schemas();

	log.trace("exit function");
    }

    private void init_schemas()
    {
	ResultSet            schemaRS = null;

	log.trace("enter function");
	try {
	    DatabaseMetaData dbmd = dbkeepconn.getMetaData();
	    String           schemaName = null;
	    SchemaInfo       schema = null;
	    
	    if (defschema == null) {
		schemaRS = dbmd.getSchemas();
		while (schemaRS.next()) {
		    schemaName = schemaRS.getString("TABLE_SCHEM");
		    log.info("start to init schema [" + schemaName + "]");
		    schema = init_schema(schemaName);

		    schemas.put(schemaName, schema);
		}
	    } else {
		log.info("start to init default schema [" + defschema + "]");
		schema = init_schema(defschema);

		if (schema != null)
		    schemas.put(defschema, schema);
		else 
		    log.error("init schema [" + defschema + "] fail, cann't find!");
	    }
	} catch (SQLException sqle) {
            sqle.printStackTrace();
	} catch (Exception e) {
            e.printStackTrace();
        }

	log.trace("exit function");
    }

    public SchemaInfo init_schema(String schemaName)
    {
	SchemaInfo          schema = null;

	log.trace("enter function [" + schemaName + "]");
	try {
	    if (deftable == null) {
		ResultSet         tableRS = null;
		DatabaseMetaData  dbmd = dbkeepconn.getMetaData();

		schema = new SchemaInfo(schemaName);
		tableRS = dbmd.getTables("Trafodion", schemaName, "%", null);
		while (tableRS.next()) {
		    String     tableName = tableRS.getString("TABLE_NAME");
		    TableInfo  table = new TableInfo(schemaName, tableName);
		    
		    log.info("start to init table [" + schemaName + "."  + 
			     tableName + "]");
		    init_culumns(table);
		    schema.AddTable(tableName, table);
		}
	    } else {
		TableInfo  table = new TableInfo(defschema, deftable);

		log.info("start to init table [" + defschema + "." 
			 + deftable + "]");
		if (init_culumns(table) > 0) {
		    schema = new SchemaInfo(schemaName);
		    schema.AddTable(deftable, table);
		} else {
		    log.error("init table [" + defschema + "." + deftable 
			      + "] is not exist!");
		}
	    }
	} catch (SQLException sqle) {
            sqle.printStackTrace();
	} catch (Exception e) {
            e.printStackTrace();
        }

	log.trace("exit function [" + schema + "]");
	return schema;
    }

    public long init_culumns(TableInfo table)
    {
	log.trace("enter function [" + table + "]");

	try {
	    String getTableColumns = "SELECT o.object_name TABLE_NAME, "
		+ "c.COLUMN_NAME COLUMN_NAME, c.FS_DATA_TYPE DATA_TYPE, "
		+ "c.FS_DATA_TYPE SOURCE_DATA_TYPE, c.SQL_DATA_TYPE TYPE_NAME, "
		+ "c.COLUMN_SIZE COLUMN_SIZE, c.NULLABLE NULLABLE,"
		+ "c.COLUMN_PRECISION DECIMAL_DIGITS, "
		+ "c.COLUMN_SCALE NUM_PREC_RADIX, c.DEFAULT_VALUE COLUMN_DEF, "
		+ "c.COLUMN_NUMBER ORDINAL_POSITION "
		+ "FROM \"_MD_\".OBJECTS o, \"_MD_\".COLUMNS c "
		+ "WHERE o.object_uid=c.object_uid AND c.COLUMN_CLASS != 'S' "
		+ "AND o.SCHEMA_NAME=? AND o.object_name=? "
		+ "ORDER BY c.COLUMN_NUMBER";
	    PreparedStatement   psmt = 
		(PreparedStatement) dbkeepconn.prepareStatement(getTableColumns);

	    psmt.setString(1, table.GetSchemaName());
	    psmt.setString(2, table.GetTableName());

            ResultSet       columnRS = psmt.executeQuery();
	    StringBuffer    strBuffer = new StringBuffer();
	    strBuffer.append("Get table \"" + table.GetSchemaName() + "." 
			     + table.GetTableName() + "\" columns [\n");

	    while (columnRS.next()) {
		String      colname = columnRS.getString("COLUMN_NAME");
		String      typename = columnRS.getString("TYPE_NAME");
		String      coltype = columnRS.getString("DATA_TYPE");
		ColumnInfo  column = new ColumnInfo(colname, typename, coltype);

		strBuffer.append("\tName: " + colname + ", Type: " + typename.trim() 
				 + ", Type ID: " + coltype + "\n");

		table.AddColumn(column);
	    }
	    log.debug(strBuffer.toString());
	    psmt.close();
	} catch (SQLException sqle) {
	    sqle.printStackTrace();
	} catch (Exception e) {
	    e.printStackTrace();
	}

	log.trace("exit function [" + table.GetColumnCount() + "");
	return table.GetColumnCount();
    }

    public long GetBatchSize()
    {
	return commitCount;
    }

    public Connection CreateConnection(boolean autocommit)
    {
	Connection          dbconn = null;
	log.trace("enter function [" + autocommit + "]");

	try {
	    Class.forName(dbdriver);
	    dbconn = DriverManager.getConnection(dburl, dbuser, dbpassword);
	    dbconn.setAutoCommit(autocommit);
	} catch (SQLException se) {
	    log.error ("SQL error: " + se.getMessage());
	    se.printStackTrace();
	} catch (ClassNotFoundException ce) {
	    log.error ("driver class not found: " + ce.getMessage());
	    ce.printStackTrace();
	} catch (Exception e) {
	    log.error ("create connect error: " + e.getMessage());
	    e.printStackTrace();
	}
	
	log.trace("exit function");
	return dbconn;
    }

    public void CloseConnection(Connection  dbconn)
    {
	log.trace("enter function [" + dbconn + "]");

	try {
	    dbconn.close();
	} catch (SQLException e) {
	    e.printStackTrace();
	}

	log.trace("exit function");
    }

    public String GetDefaultSchema()
    {
	return defschema;
    }

    public String GetDefaultTable()
    {
	return deftable;
    }

    public SchemaInfo GetSchema(String schemaName)
    {
	if (defschema != null)
	    return schemas.get(defschema);

	return schemas.get(schemaName);
    }

    public boolean KeepAlive()
    {
	try {
            ResultSet columnRS = keepstmt.executeQuery();
	    while (columnRS.next()) {
                columnRS.getString("(EXPR)");
            }

	} catch (SQLException e) {
	    return false;
	}

	return true;
    }

    public long InsertData(Connection  conn,
			   String      message,
			   String      schemaName,
			   String      tableName, 
			   int         thread,
			   ArrayList<ColumnValue> columns)
    {
	SchemaInfo        schema = schemas.get(schemaName);
	TableInfo         table = schema.GetTable(tableName);
	long              result = 0;

	if (table == null)
	    return result;

	log.trace("enter function [" + conn + ", " + message + ","
		  + schemaName + "." + tableName + "," + thread + "]");

	try {
	    ColumnValue columnValue = columns.get(0);
	    ColumnInfo  column      = table.GetColumn(columnValue.GetColumnID());
	    PreparedStatement stmt  = table.GetInsertStmt(conn, thread);

	    if (stmt == null) {
		String      insertSql   = "UPSERT USING LOAD INTO " + schemaName + "." 
		    + tableName + "(" + column.GetColunmName();
		String      valueSql    = ") VALUES(?";

		for(int i = 1; i < columns.size(); i++) {
		    columnValue = columns.get(i);
		    column = table.GetColumn(columnValue.GetColumnID());
		    insertSql += ", " + column.GetColunmName();
		
		    valueSql += ", ?";
		}

		insertSql += valueSql + ");";

		log.debug ("Thread [" + thread + "] InsertData: [" + insertSql + "]");
		stmt = conn.prepareStatement(insertSql);
		table.SetInsertStmt(conn, thread, stmt);
	    }

	    
	    for(int i = 0; i < table.GetColumnCount(); i++) {
		column      = table.GetColumn(i);
		stmt.setNull(i+1, column.GetColunmType());
	    }

	    for(int i = 0; i < columns.size(); i++) {
		columnValue = columns.get(i);
		column      = table.GetColumn(columnValue.GetColumnID());
		if (!columnValue.CurValueIsNull())
		    stmt.setString(i+1, columnValue.GetCurValue());
	    }

	    stmt.addBatch();
	    table.IncCacheRows();
	    if ((table.GetCacheRows() % commitCount) == 0) {
		int [] batchResult = stmt.executeBatch();
		conn.commit();
		long rows = table.GetCacheRows();
		synchronized (this) {
		    insertnum += rows;
		    messagenum += rows;
		}

		table.SetCacheRows(0);
		table.IncreaseInsert(rows);
	    }
	} catch (BatchUpdateException bue) {
	    int[] insertCounts = bue.getUpdateCounts();
	    int count = 1;

	    for (int i : insertCounts) {
		if ( i == Statement.EXECUTE_FAILED ) 
		    log.error("error on request #" + count +" execute failed");
		else 
		    count++;
	    }
	    log.error (bue.getMessage() + "\nBatchUpdate exception raw data: ["
		       + message + "]");
	    bue.printStackTrace();
	} catch (IndexOutOfBoundsException iobe) {
	    log.error ("IndexOutOfBounds exception raw data: [" + message + "]");
	    iobe.printStackTrace();
	} catch (SQLException e) {
	    log.error ("SQL exception raw data: [" + message + "]");
	    e.printStackTrace();
	}
	
	log.trace("exit function [" + result + "]");
	return result;
    }

    public long UpdateData(Connection  conn,
			   String      message,
			   String      schemaName,
			   String      tableName, 
			   int         thread,
			   ArrayList<ColumnValue> columns)
    {
	SchemaInfo       schema = schemas.get(schemaName);
	TableInfo        table = schema.GetTable(tableName);
	long             result = 0;

	if (table == null)
	    return result;

	log.trace("enter function [" + conn + ", " + message + "," + schemaName
		  + "." + tableName + "," + thread + "]");

	try {
	    if (table.GetCacheRows() != 0) {
		PreparedStatement stmt = table.GetInsertStmt(conn, thread);
		int [] batchResult = stmt.executeBatch();
		conn.commit();
		long rows = table.GetCacheRows();
		synchronized (this) {
		    insertnum += rows;
		    messagenum += rows;
		}

		table.SetCacheRows(0);
		table.IncreaseInsert(rows);
	    }

	    ColumnValue columnValue = columns.get(0);
	    ColumnInfo  column      = 
		table.GetColumn(columnValue.GetColumnID());
	    String      updateSql   = "UPDATE  " + schemaName + "." 
		+ tableName + " SET " + column.GetColunmName() + " = "
		+ columnValue.GetCurValueStr();
	    String      whereSql    = " WHERE " + column.GetColunmName()
		+ columnValue.GetOldCondStr();


	    for(int i = 1; i < columns.size(); i++) {
		columnValue = columns.get(i);
		column = table.GetColumn(columnValue.GetColumnID());
		
		updateSql += ", " + column.GetColunmName() + " = "
		    + columnValue.GetCurValueStr();
		whereSql += " AND " + column.GetColunmName() 
		    + columnValue.GetOldCondStr();
	    }

	    updateSql += whereSql + ";";

	    log.debug ("update sql: [" + updateSql + "]");
	    Statement st = conn.createStatement();

	    st.executeUpdate(updateSql);
	    st.cancel();
	    conn.commit();	    
	    table.IncreaseUpdate();
	    synchronized (this){
		updatenum++;
		messagenum++;
	    }
	    result = 1;
	} catch (BatchUpdateException bue) {
	    int[] insertCounts = bue.getUpdateCounts();
	    int count = 1;

	    for (int i : insertCounts) {
		if ( i == Statement.EXECUTE_FAILED ) 
		    log.error("error on request #" + count +" execute failed");
		else 
		    count++;
	    }
	    log.error (bue.getMessage() + "\nBatchUpdate exception raw data: ["
		       + message + "]");
	    bue.printStackTrace();
	} catch (IndexOutOfBoundsException iobe) {
	    log.error ("IndexOutOfBounds exception raw data: [" + message + "]");
	    iobe.printStackTrace();
	} catch (SQLException e) {
	    log.error ("SQL exception raw data: [" + message + "]");
	    e.printStackTrace();
	}

	log.trace("exit function [" + result + "]");
	return result;
    }

    public long DeleteData(Connection  conn,
			   String      message,
			   String      schemaName,
			   String      tableName, 
			   int         thread,
			   ArrayList<ColumnValue> columns)
    {
	SchemaInfo       schema = schemas.get(schemaName);
	TableInfo        table = schema.GetTable(tableName);
	long             result = 0;

	if (table == null)
	    return result;

	log.trace("enter function [" + conn + ", " + message + "," + schemaName
		  + "." + tableName + "," + thread + "]");

	try {
	    if (table.GetCacheRows() != 0) {
		PreparedStatement stmt = table.GetInsertStmt(conn, thread);
		int [] batchResult = stmt.executeBatch();
		conn.commit();
		long rows = table.GetCacheRows();
		synchronized (this) {
		    insertnum += rows;
		    messagenum += rows;
		}

		table.SetCacheRows(0);
		table.IncreaseInsert(rows);
	    }

	    ColumnValue columnValue = columns.get(0);
	    ColumnInfo  column      = 
		table.GetColumn(columnValue.GetColumnID());
	    String      deleteSql   = "DELETE FROM " + schemaName + "." 
		+ tableName + " WHERE "+ column.GetColunmName() 
		+ columnValue.GetOldCondStr();

	    for(int i = 1; i < columns.size(); i++) {
		columnValue = columns.get(i);
		column = table.GetColumn(columnValue.GetColumnID());
		deleteSql += " AND " + column.GetColunmName()
		    + columnValue.GetOldCondStr();;
	    }
	    deleteSql += ";";

	    log.debug ("delete sql: " + deleteSql);
	    Statement st = conn.createStatement();

	    st.executeUpdate(deleteSql);
	    st.cancel();
	    conn.commit();
	    result = 1;
	    table.IncreaseDelete();
	    synchronized (this){
		messagenum++;
		deletenum++;
	    }
	} catch (BatchUpdateException bue) {
	    int[] insertCounts = bue.getUpdateCounts();
	    int count = 1;

	    for (int i : insertCounts) {
		if ( i == Statement.EXECUTE_FAILED ) 
		    log.error("error on request #" + count +" execute failed");
		else 
		    count++;
	    }
	    log.error (bue.getMessage() + "\nBatchUpdate exception raw data: ["
		       + message + "]");
	    bue.printStackTrace();
	} catch (IndexOutOfBoundsException iobe) {
	    log.error ("IndexOutOfBounds exception raw data: [" + message + "]");
	    iobe.printStackTrace();
	} catch (SQLException e) {
	    log.error ("SQL exception raw data: [" + message + "]");
	    e.printStackTrace();
	}

	log.trace("exit function [" + result + "]");
	return result;
    }

    public void  CommitAll(Connection  conn,
			   int         thread)
    {
	log.trace("enter function");

	try {
 	    PreparedStatement stmt;
	    SchemaInfo        schemainfo;
	    TableInfo         tableinfo;

	    for (Map.Entry<String, SchemaInfo> schema : schemas.entrySet()) {
		schemainfo = schema.getValue();
		Map<String, TableInfo> tables = schemainfo.GetTables();
		for (Map.Entry<String, TableInfo> table : tables.entrySet()) {
		    tableinfo = table.getValue();
		    stmt = tableinfo.GetInsertStmt(conn, thread);
		    if (stmt != null && tableinfo.GetCacheRows() > 0){
			log.info(tableinfo.GetTableName() + ", stmt:" + stmt
				 + ", cache row:" + tableinfo.GetCacheRows());
			int [] batchResult = stmt.executeBatch();
			conn.commit();
			long rows = tableinfo.GetCacheRows();
			synchronized (this) {
			    insertnum += rows;
			    messagenum += rows;
			}

			tableinfo.SetCacheRows(0);
			tableinfo.IncreaseInsert(rows);
		    }
		}
	    }
	} catch (SQLException e) {
	    do{
		e.printStackTrace();
		e = e.getNextException();
	    }while(e!=null); 
	}

	log.trace("exit function");
    }

    public void DisplayDatabase()
    {
	Long end = new Date().getTime();
	Date endtime = new Date();
	Float use_time = ((float) (end - begin))/1000;
	long speed = (long)(messagenum/use_time);
	DecimalFormat df = new DecimalFormat("####0.000");
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

	log.info("consumer states, total message: " + messagenum + ", run: " 
		 + df.format(use_time) + "s, speed: " + speed + "/s\n\t[start: "
		 + sdf.format(starttime) + ", cur: " + sdf.format(endtime)
		 + ", insert: " + insertnum + ", update: " + updatenum 
		 + ", delete: " + deletenum + "]");
	for (Map.Entry<String, SchemaInfo> schema : schemas.entrySet()) {
	    schema.getValue().DisplaySchema();
	}
    }
}
