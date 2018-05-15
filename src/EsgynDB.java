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
    Map<String, TableInfo>  tables   = null;
    PreparedStatement       keepStmt = null;
    private static Logger   log = Logger.getLogger(EsgynDB.class);

    private long messageNum = 0;
    private long insertNum  = 0;
    private long updateNum  = 0;
    private long updkeyNum  = 0;
    private long deleteNum  = 0;

    private long begin;
    private Date starttime;

    public EsgynDB(String  defschema_,
		   String  deftable_,
		   String  dburl_, 
		   String  dbdriver_, 
		   String  dbuser_, 
		   String  dbpassword_,
		   long    commitCount_) 
    {
	log.trace("enter function [table: " + defschema_ + "." + deftable_ 
		  + ", db url: " + dburl_ + ", db driver:" + dbdriver_ 
		  + ", db user: " + dbuser_  + ", commit count:" 
		  + commitCount_ + "]");

	dburl       = dburl_;
	dbdriver    = dbdriver_;
	dbuser      = dbuser_;
	dbpassword  = dbpassword_;
	defschema   = defschema_;
	deftable    = deftable_;
	commitCount = commitCount_;

	begin = new Date().getTime();
	starttime = new Date();
	
	tables = new HashMap<String, TableInfo>(); 
	dbkeepconn = CreateConnection(true);
	try {
	    log.info("prepare the keepalive stmt, query:" + keepQuery); 
	    keepStmt = dbkeepconn.prepareStatement(keepQuery);
	} catch (SQLException e) {
	    log.error("prepare the keepalive stmt error.");
            e.printStackTrace();
	}

	log.info("start to init schemas"); 
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
	    
	    if (defschema == null) {
		schemaRS = dbmd.getSchemas();
		while (schemaRS.next()) {
		    schemaName = schemaRS.getString("TABLE_SCHEM");
		    log.info("start to init schema [" + schemaName + "]");
		    init_schema(schemaName);
		}
	    } else {
		log.info("start to init default schema [" + defschema + "]");
		init_schema(defschema);
		
		if (tables.size() <= 0)
		    log.error("init schema [" + defschema + 
			      "] fail, cann't find!");
	    }
	} catch (SQLException sqle) {
            sqle.printStackTrace();
	} catch (Exception e) {
            e.printStackTrace();
        }

	log.trace("exit function");
    }

    public TableInfo init_schema(String schemaName)
    {
	TableInfo tableInfo = null;

	log.trace("enter function [schema: " + schemaName + "]");
	try {
	    if (deftable == null) {
		ResultSet         tableRS = null;
		DatabaseMetaData  dbmd = dbkeepconn.getMetaData();

		tableRS = dbmd.getTables("Trafodion", schemaName, "%", null);
		while (tableRS.next()) {
		    String  tableName = schemaName + "." 
			+ tableRS.getString("TABLE_NAME");
		    tableInfo = new TableInfo(schemaName, tableName);
		    
		    log.info("start to init table [" + tableName + "]");
		    if (init_culumns(tableInfo) <= 0) {
			log.error("init table [" + tableName 
				  + "] is not exist!");
		    }
		    if (init_keys(tableInfo) <= 0) {
			log.error("no primary key on table [" + tableName + "]");
		    }
		    tables.put(tableName, tableInfo);
		}
	    } else {
		String tableName = defschema + "." + deftable;
		tableInfo = new TableInfo(defschema, deftable);

		log.info("start to init table [" + tableName + "]");
		if (init_culumns(tableInfo) <= 0) {
		    log.error("init table [" + tableName + "] is not exist!");
		}
		if (init_keys(tableInfo) <= 0) {
		    log.error("no primary key on table [" + tableName + "]");
		}

		tables.put(tableName, tableInfo);
	    }
	} catch (SQLException sqle) {
            sqle.printStackTrace();
	} catch (Exception e) {
            e.printStackTrace();
        }

	log.trace("exit function [table info: " + tableInfo + "]");
	return tableInfo;
    }

    public long init_culumns(TableInfo table)
    {
	log.trace("enter function [table info: " + table + "]");

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
			     + table.GetTableName() + "\" columns \n[\n");

	    while (columnRS.next()) {
		String      colname = columnRS.getString("COLUMN_NAME");
		String      typename = columnRS.getString("TYPE_NAME");
		String      coltype = columnRS.getString("DATA_TYPE");
		String      colid = columnRS.getString("ORDINAL_POSITION");
		ColumnInfo  column = new ColumnInfo(colid, coltype, typename, colname);

		strBuffer.append("\tName: " + colname + ", Type: " + typename.trim() 
				 + ", Type ID: " + coltype + "\n");

		table.AddColumn(column);
	    }
	    strBuffer.append("]"); 
	    log.debug(strBuffer.toString());
	    psmt.close();
	} catch (SQLException sqle) {
	    sqle.printStackTrace();
	} catch (Exception e) {
	    e.printStackTrace();
	}

	log.trace("exit function [column number:" + table.GetColumnCount() + "]");
	return table.GetColumnCount();
    }

    public long init_keys(TableInfo table)
    {
	log.trace("enter function [table info: " + table + "]");

	try {
	    String getTableKeys = "SELECT k.COLUMN_NAME COLUMN_NAME, "
		+ "c.FS_DATA_TYPE DATA_TYPE, c.SQL_DATA_TYPE TYPE_NAME, "
		+ "c.NULLABLE NULLABLE, c.COLUMN_PRECISION DECIMAL_DIGITS, "
		+ "c.COLUMN_NUMBER KEY_COLUMN_ID, c.COLUMN_NUMBER ORDINAL_POSITION "
		+ "FROM \"_MD_\".OBJECTS o, \"_MD_\".COLUMNS c, \"_MD_\".KEYS k "
		+ "WHERE o.OBJECT_UID=c.OBJECT_UID AND o.OBJECT_UID=k.OBJECT_UID "
		+ "AND c.COLUMN_NUMBER = k.COLUMN_NUMBER AND c.COLUMN_CLASS != 'S' "
		+ "AND o.SCHEMA_NAME=? AND o.object_name=? "
		+ "ORDER BY k.KEYSEQ_NUMBER;";
	    PreparedStatement   psmt = 
		(PreparedStatement) dbkeepconn.prepareStatement(getTableKeys);

	    psmt.setString(1, table.GetSchemaName());
	    psmt.setString(2, table.GetTableName());

            ResultSet       keysRS = psmt.executeQuery();
	    StringBuffer    strBuffer = new StringBuffer();
	    strBuffer.append("Get primakey of \"" + table.GetSchemaName() 
			     + "\".\"" + table.GetTableName() 
			     + "\" key columns\n[\n");

	    while (keysRS.next()) {
		String      colname  = keysRS.getString("COLUMN_NAME");
		String      typename = keysRS.getString("TYPE_NAME");
		String      coltype  = keysRS.getString("DATA_TYPE");
		String      colid    = keysRS.getString("KEY_COLUMN_ID");
		ColumnInfo  column   = new ColumnInfo(colid, coltype, typename, colname);

		strBuffer.append("\tName: " + colname + ", Type: " + typename.trim() 
				 + ", Type ID: " + coltype + "\n");

		table.AddKey(column);
	    }
	    strBuffer.append("]\n"); 
	    log.debug(strBuffer.toString());
	    psmt.close();
	} catch (SQLException sqle) {
	    sqle.printStackTrace();
	} catch (Exception e) {
	    e.printStackTrace();
	}

	log.trace("exit function [key column number: " + table.GetKeyCount() + "]");
	return table.GetKeyCount();
    }

    public long GetBatchSize()
    {
	return commitCount;
    }

    public Connection CreateConnection(boolean autocommit)
    {
	Connection          dbConn = null;
	log.trace("enter function [autocommit: " + autocommit + "]");

	try {
	    Class.forName(dbdriver);
	    dbConn = DriverManager.getConnection(dburl, dbuser, dbpassword);
	    dbConn.setAutoCommit(autocommit);
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
	return dbConn;
    }

    public void CloseConnection(Connection  dbConn_)
    {
	log.trace("enter function [db conn: " + dbConn_ + "]");

	try {
	    dbConn_.close();
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

    public TableInfo GetTableInfo(String tableName_)
    {
	return tables.get(tableName_);
    }

    public boolean KeepAlive()
    {
	try {
            ResultSet columnRS = keepStmt.executeQuery();
	    while (columnRS.next()) {
                columnRS.getString("(EXPR)");
            }

	} catch (SQLException e) {
	    return false;
	}

	return true;
    }

    public synchronized void AddInsertNum(long insertNum_){
	insertNum += insertNum_;
	messageNum += insertNum_;
    }

    public synchronized void AddUpdateNum(long updateNum_){
	updateNum += updateNum_;
	messageNum += updateNum_;
    }

    public synchronized void AddUpdkeyNum(long updkeyNum_){
	updkeyNum += updkeyNum_;
	messageNum += updkeyNum_;
    }

    public synchronized void AddDeleteNum(long deleteNum_){
	deleteNum += deleteNum_;
	messageNum += deleteNum_;
    }

    public void DisplayDatabase()
    {
	Long end = new Date().getTime();
	Date endtime = new Date();
	Float use_time = ((float) (end - begin))/1000;
	long speed = (long)(messageNum/use_time);
	DecimalFormat df = new DecimalFormat("####0.000");
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

	log.info("consumer states, total message: " + messageNum + ", run: " 
		 + df.format(use_time) + "s, speed: " + speed + "/s\n\t[start: "
		 + sdf.format(starttime) + ", cur: " + sdf.format(endtime)
		 + ", insert: " + insertNum + ", update: " + updateNum 
		 + ", updkey: " + updkeyNum  + ", delete: " + deleteNum + "]");
	for (TableInfo tableInfo : tables.values()) {
	    tableInfo.DisplayStat();
	}
    }
}
