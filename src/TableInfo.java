import java.util.ArrayList;
import org.apache.log4j.Logger; 
import java.sql.PreparedStatement;
import java.sql.Connection;
 
public class TableInfo
{
    String                  schemaName = null;
    String                  tableName = null;

    private long            insertnum = 0;
    private long            updatenum = 0;
    private long            deletenum = 0;
    private long            cachenum = 0;

    private int             threadid = -1;
    private Connection      dbconn = null;
    private PreparedStatement insertstmt = null;

    ArrayList<ColumnInfo>   columns = null;
    private static Logger   log = Logger.getLogger(TableInfo.class);

    public TableInfo(String schemaName_, String tableName_) 
    {
	schemaName = schemaName_;
	tableName = tableName_;
	columns = new ArrayList<ColumnInfo>(0);
    }

    public String GetTableName()
    {
	return tableName;
    }

    public String GetSchemaName()
    {
	return schemaName;
    }

    public PreparedStatement GetInsertStmt(Connection        dbconn_,
					   int               threadid_)
    {
	if (dbconn_ == dbconn && threadid_ == threadid)
	    return insertstmt;
	else 
	    return null;
    }

    public void SetInsertStmt(Connection        dbconn_,
			      int               threadid_,
			      PreparedStatement stmt)
    {
	dbconn = dbconn_;
	threadid = threadid_;
	insertstmt = stmt;
    }

    public void AddColumn(ColumnInfo column)
    {
	columns.add(column);
    }

    public ColumnInfo GetColumn(int index)
    {
	return (ColumnInfo)columns.get(index);
    }

    public synchronized void IncreaseInsert(long num)
    {
	insertnum += num;
    }

    public synchronized void IncCacheRows()
    {
	cachenum++;
    }

    public synchronized long GetCacheRows()
    {
	return cachenum;
    }

    public synchronized void SetCacheRows(long cachenum_)
    {
	cachenum = cachenum_;
    }

    public synchronized void IncreaseUpdate()
    {
	updatenum++;
    }

    public synchronized void IncreaseDelete()
    {
	deletenum++;
    }

    public void DisplayStat()
    {
	log.info("Table " + schemaName + "." + tableName + " stat [insert: "
		 + insertnum + ", update: " + updatenum + ", delete: " 
		 + deletenum + "]");
    }    
}

