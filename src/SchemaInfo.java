import java.util.Map;
import java.util.HashMap;
import org.apache.log4j.Logger; 

public class SchemaInfo
{
    String                      schemaName = null;
    Map<String, TableInfo>      tables = null;
    private static Logger       log = Logger.getLogger(TableInfo.class);

    public SchemaInfo(String schemaName_) 
    {
	schemaName = schemaName_;
	tables = new HashMap<String, TableInfo>();
    }

    public String GetSchemaName()
    {
	return schemaName;
    }

    public void AddTable(String tableName, TableInfo table)
    {
	tables.put(tableName, table);
    }

    public TableInfo GetTable(String tablename){
	return tables.get(tablename);
    }

    public Map<String, TableInfo> GetTables(){
	return tables;
    }

    public void DisplaySchema()
    {
	log.info("Show the state of schema [" + schemaName + "]");
	for (Map.Entry<String, TableInfo> table : tables.entrySet()) {
	    table.getValue().DisplayStat();
	}
    }
}

