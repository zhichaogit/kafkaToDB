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
    Map<String, SchemaInfo> schemas  = null;
    PreparedStatement       keepstmt = null;
    private static Logger   log = Logger.getLogger(EsgynDB.class);

    private long    messagenum = 0;
    private long    insertnum = 0;
    private long    updatenum = 0;
    private long    updkeynum = 0;
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
		    TableInfo  table = new TableInfo(schemaName, tableName, 
						     commitCount);
		    
		    log.info("start to init table [" + schemaName + "."  + 
			     tableName + "]");
		    init_culumns(table);
		    init_keys(table);
		    schema.AddTable(tableName, table);
		}
	    } else {
		TableInfo  table = new TableInfo(defschema, deftable, commitCount);

		log.info("start to init table [" + defschema + "." 
			 + deftable + "]");
		if (init_culumns(table) > 0) {
		    init_keys(table);
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

	log.trace("exit function [" + table.GetColumnCount() + "");
	return table.GetColumnCount();
    }

    public long init_keys(TableInfo table)
    {
	log.trace("enter function [" + table + "]");

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
	    if (table.GetKeyCount() == 0) {
		log.error("No primary key on \"" + table.GetSchemaName()
			  + "\".\"" + table.GetTableName() + "\"");
	    } else {
		log.debug(strBuffer.toString());
	    }
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
	Connection          dbConn = null;
	log.trace("enter function [" + autocommit + "]");

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
	log.trace("enter function [" + dbConn_ + "]");

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

    public long InsertMessageToDatabase(Connection dbConn_, RowMessage urm)
    {
	SchemaInfo  schema = schemas.get(urm.GetSchemaName());
	TableInfo   table = schema.GetTable(urm.GetTableName());

	if (!table.InitStmt(dbConn_))
	    return 0;

	switch(urm.GetOperatorType()) {
	case "I":
	    insertnum++;
	    break;
	case "U":
	    updatenum++;
	    break;
	case "K":
	    updkeynum++;
	    break;
	case "D":
	    deletenum++;
	    break;

	default:
	    log.error("operator [" + urm.GetOperatorType() + "]");
	    return 0;
	}

	messagenum++;

	return table.InsertMessageToTable(urm);
    }

    public void CommitAllDatabase(Connection dbConn_)
    {
	log.trace("enter function");

	for (SchemaInfo schemaInfo : schemas.values()) {
	    Map<String, TableInfo> tables = schemaInfo.GetTables();
	    for (TableInfo tableinfo : tables.values()) {
		tableinfo.CommitAllTable(dbConn_);
	    }
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
		 + ", updkey: " + updkeynum  + ", delete: " + deletenum + "]");
	for (SchemaInfo schemaInfo : schemas.values()) {
	    schemaInfo.DisplaySchema();
	}
    }
}
