import java.util.ArrayList;
import org.apache.log4j.Logger; 
 
public class RowMessage
{
    String                  message         = null;
    String                  schemaName      = null;
    String                  tableName       = null;
    String                  delimiter       = ",";
    String                  operatorType    = "I";
    int                     thread          = -1;

    protected static Logger log = Logger.getLogger(RowMessage.class); 

    ArrayList<ColumnValue>  columns = null;

    public RowMessage(String defschema_, String deftable_, String delimiter_,
		      int thread_, String message_)
    {
	log.trace("enter function [" + defschema_ + ", " + deftable_
		  + ", \"" + delimiter_ + "\", " + thread_ + ", " + message_);

	schemaName = defschema_;
	tableName = deftable_;
	if (delimiter_ != null)
	    delimiter = delimiter_;
	thread = thread_;
	message = message_;

	log.trace("exit function");
    }

    public void AnalyzeMessage()
    {
	log.trace("enter function");

	String[] formats = message.split("\\" + delimiter);

	StringBuffer strBuffer = new StringBuffer();

	strBuffer.append("RowMessage thread [" + thread + "]\n");
	strBuffer.append("Raw message:[" + message + "]\n");
	strBuffer.append("Operator Info: [Table Name: " + tableName 
			 + ", Type: " + operatorType + "]");

	columns = new ArrayList<ColumnValue>(0);
	for (int i = 0; i < formats.length; i++) {
	    strBuffer.append("\n\tColumn: " + formats[i]);
	    ColumnValue columnValue = new ColumnValue(i, formats[i], null);
	    columns.add(columnValue);
	}
	strBuffer.append("\nRowMessage end");
	log.debug(strBuffer);

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

    public String GetOperatorType()
    {
	return operatorType;
    }

    public ArrayList<ColumnValue> GetColumns()
    {
	return columns;
    }
}

