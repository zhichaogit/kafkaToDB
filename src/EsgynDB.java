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
    PreparedStatement       keeppsmt = null;
    private static Logger   log = Logger.getLogger(EsgynDB.class);

    private long    messagenum = 0;
    private long    insertnum = 0;
    private long    updatenum = 0;
    private long    deletenum = 0;

    public EsgynDB(String  defschema_,
		   String  deftable_,
		   String  dburl_, 
		   String  dbdriver_, 
		   String  dbuser_, 
		   String  dbpassword_,
		   long    commitCount_) 
    {
	dburl       = dburl_;
	dbdriver    = dbdriver_;
	dbuser      = dbuser_;
	dbpassword  = dbpassword_;
	defschema   = defschema_;
	deftable    = deftable_;
	commitCount = commitCount_;
	
	schemas = new HashMap<String, SchemaInfo>(); 
	dbkeepconn = CreateConnection(true);
	try {
	    keeppsmt = (PreparedStatement) dbkeepconn.prepareStatement(keepQuery);
	} catch (SQLException e) {
            e.printStackTrace();
	}
	init_schemas();
    }

    private void init_schemas()
    {
	ResultSet            schemaRS = null;
	try {
	    DatabaseMetaData dbmd = dbkeepconn.getMetaData();
	    String           schemaName = null;
	    SchemaInfo       schema = null;
	    
	    if (defschema == null) {
		schemaRS = dbmd.getSchemas();
		while (schemaRS.next()) {
		    schemaName = schemaRS.getString("TABLE_SCHEM");
		    log.info("Get schema [" + schemaName + "]");
		    schema = init_schema(schemaName);

		    schemas.put(schemaName, schema);
		}
	    } else {
		log.info("Get schema [" + defschema + "]");
		schema = init_schema(defschema);

		schemas.put(defschema, schema);
	    }
	} catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public SchemaInfo init_schema(String schemaName)
    {
	SchemaInfo          schema = new SchemaInfo(schemaName);

	try {
	    if (deftable == null) {
		ResultSet         tableRS = null;
		DatabaseMetaData  dbmd = dbkeepconn.getMetaData();

		tableRS = dbmd.getTables("Trafodion", schemaName, "%", null);
		while (tableRS.next()) {
		    String     tableName = tableRS.getString("TABLE_NAME");
		    TableInfo  table = new TableInfo(schemaName, tableName);
		    
		    init_culumns(table);
		    schema.AddTable(tableName, table);
		}
	    } else {
		TableInfo  table = new TableInfo(defschema, deftable);
		if (init_culumns(table) > 0) {
		    schema.AddTable(deftable, table);
		} else {
		    log.error("The table [" + deftable + "] is not exists in "
			      + "schema [" + defschema+ "]");
		}
		
	    }
	} catch (SQLException e) {
            e.printStackTrace();
        }

	return schema;
    }

    public int init_culumns(TableInfo table)
    {
	int    columnnum = 0;
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
		columnnum++;
	    }
	    log.debug(strBuffer.toString());
	    psmt.close();
	} catch (SQLException e) {
	    e.printStackTrace();
	}

	return columnnum;
    }

    public long GetBatchSize()
    {
	return commitCount;
    }

    public Connection CreateConnection(boolean autocommit)
    {
	Connection          dbconn = null;

	try {
	    Class.forName(dbdriver);
	    dbconn = DriverManager.getConnection(dburl, dbuser, dbpassword);
	    dbconn.setAutoCommit(autocommit);
	} catch (SQLException sx) {
	    log.error ("SQL error: " + sx.getMessage());
	    sx.printStackTrace();
	} catch (ClassNotFoundException cx) {
	    log.error ("Driver class not found: " + cx.getMessage());
	    cx.printStackTrace();
	}
	
	return dbconn;
    }

    public void CloseConnection(Connection  dbconn)
    {
	try {
	    dbconn.close();
	} catch (SQLException e) {
	    e.printStackTrace();
	}
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
            ResultSet columnRS = keeppsmt.executeQuery();
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

	if (table == null)
	    return 0;

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

	    
	    for(int i = 0; i < columns.size(); i++) {
		columnValue = columns.get(i);
		column      = table.GetColumn(columnValue.GetColumnID());
		if (columnValue.CurValueIsNull())
		    stmt.setNull(i+1, column.GetColunmType());
		else
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
	} 
	catch (BatchUpdateException bx) {
	    int[] insertCounts = bx.getUpdateCounts();
	    int count = 1;
	    for (int i : insertCounts) {
		if ( i == Statement.EXECUTE_FAILED ) 
		    log.error("Error on request #" + count +": Execute failed");
		else 
		    count++;
	    }
	    log.error(bx.getMessage());
	} catch (SQLException e) {
	    log.error ("Thread [" + thread + "] InsertData raw data: [" + message + "]");
	    e.printStackTrace();
	    return 0;
	}
	
	return 1;
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

	if (table == null)
	    return 0;

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

	    log.debug ("Thread [" + thread + "] UpdateData: [" + updateSql + "}");
	    Statement st = conn.createStatement();

	    st.executeUpdate(updateSql);
	    st.cancel();
	    conn.commit();	    
	} catch (BatchUpdateException bx) {
	    int[] insertCounts = bx.getUpdateCounts();
	    int count = 1;
	    for (int i : insertCounts) {
		if ( i == Statement.EXECUTE_FAILED ) 
		    log.error("Error on request #" + count +": Execute failed");
		else 
		    count++;
	    }
	    log.error(bx.getMessage());
	} catch (SQLException e) {
	    log.error ("Thread [" + thread + "] UpdateData raw data: [" + message + "]");
	    e.printStackTrace();
	    return 0;
	}

	table.IncreaseUpdate();
	synchronized (this){
	    updatenum++;
	    messagenum++;
	}

	return 1;
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

	if (table == null)
	    return 0;

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
	    String      deleteSql   = "DELETE WITH NO ROLLBACK FROM " + schemaName + "." 
		+ tableName + " WHERE "+ column.GetColunmName() 
		+ columnValue.GetOldCondStr();

	    for(int i = 1; i < columns.size(); i++) {
		columnValue = columns.get(i);
		column = table.GetColumn(columnValue.GetColumnID());
		deleteSql += " AND " + column.GetColunmName()
		    + columnValue.GetOldCondStr();;
	    }
	    deleteSql += ";";

	    log.debug ("Thread [" + thread + "] DeleteData: " + deleteSql);
	    Statement st = conn.createStatement();

	    st.executeUpdate(deleteSql);
	    st.cancel();
	    conn.commit();
	} catch (BatchUpdateException bx) {
	    int[] insertCounts = bx.getUpdateCounts();
	    int count = 1;
	    for (int i : insertCounts) {
		if ( i == Statement.EXECUTE_FAILED ) 
		    log.error("Error on request #" + count +": Execute failed");
		else 
		    count++;
	    }
	    log.error(bx.getMessage());
	} catch (SQLException e) {
	    log.error ("Thread [" + thread + "] DeleteData raw data: [" + message + "]");
	    e.printStackTrace();
	    return 0;
	}

	table.IncreaseDelete();
	synchronized (this){
	    messagenum++;
	    deletenum++;
	}

	return 1;
    }

    public void  CommitAll(Connection  conn,
			   int         thread)
    {
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
			int [] batchResult = stmt.executeBatch();
			conn.commit();
			tableinfo.SetCacheRows(0);
		    }
		}
	    }
	} catch (SQLException e) {
	    do{
		e.printStackTrace();
		e = e.getNextException();
	    }while(e!=null); 
	}
    }

    public void DisplayDatabase()
    {
	log.info("Show the state of consumer [Total Messages: " + messagenum
		 + ", Insert Messages: " + insertnum 
		 + ", Update Messages: " + updatenum
		 + ", Delete Messages: " + deletenum + "]");
	for (Map.Entry<String, SchemaInfo> schema : schemas.entrySet()) {
	    schema.getValue().DisplaySchema();
	}
    }
}
