import java.util.ArrayList;
import org.apache.log4j.Logger; 
 
public class TableInfo
{
    String                  schemaName = null;
    String                  tableName = null;

    private long            insertnum = 0;
    private long            updatenum = 0;
    private long            deletenum = 0;

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

    public void AddColumn(ColumnInfo column)
    {
	columns.add(column);
    }

    public ColumnInfo GetColumn(int index)
    {
	return (ColumnInfo)columns.get(index);
    }

    public void IncreaseInsert()
    {
	insertnum++;
    }

    public void IncreaseUpdate()
    {
	updatenum++;
    }

    public void IncreaseDelete()
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

