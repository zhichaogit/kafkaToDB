import java.util.Map;
import java.util.HashMap;
import org.apache.log4j.Logger; 
 
public class HongQuanRowMessage extends RowMessage
{
    private int             length          = 0;
    private byte[]          data            = null;
    private int []          fieldSizes      = null;

    public HongQuanRowMessage(TableInfo tableInfo_, int thread_, byte[] message_)
    {
	super(tableInfo_.GetSchemaName(), tableInfo_.GetTableName(), null, thread_, null);

	data = message_;

	fieldSizes = new int[(int)tableInfo_.GetColumnCount()];
	for (int i = 0; i < tableInfo_.GetColumnCount(); i++) {
	    ColumnInfo column = tableInfo_.GetColumn(i);
	    fieldSizes[i] = column.GetColumnSize();
	    length += fieldSizes[i];
	}

	if (length > message_.length) {
	    log.error("message error [" + message_ + "] message length: " 
		      + message_.length + ", length: " + length);
	    return;
	}

	if (log.isDebugEnabled()){
	    log.info("the table mode [" + fieldSizes + "] total size [" 
		     + length + "]");
	}
    }

    @Override
    public void AnalyzeMessage()
    {
	int          offset    = 0;
	StringBuffer strBuffer = null;

	if (length > data.length) {
	    return;
	}

	if(log.isDebugEnabled()){
	    strBuffer = new StringBuffer();

	    strBuffer.append("RowMessage thread [" + thread + "]\n");
	    strBuffer.append("Raw message:[" + data + "]\n");
	    strBuffer.append("Operator Info: [Table Name: " + tableName 
			     + ", Type: " + operatorType + "]");
	}

	columns = new HashMap<Integer, ColumnValue>(0);
	for (int i = 0; i < fieldSizes.length; i++) {
	    log.debug("i: " + i + ", offset: " + offset + ", field: " 
		      + fieldSizes[i]);
	    ColumnValue columnValue = new ColumnValue(i, get_column(data, offset, fieldSizes[i]), null);

	    if(log.isDebugEnabled()){
		strBuffer.append("\n\tColumn: " + columnValue.GetCurValue());
	    }

	    columns.put(i, columnValue);
	    offset += fieldSizes[i];
	}

	if(log.isDebugEnabled()){
	    strBuffer.append("\nRowMessage end");
	    log.debug(strBuffer.toString());
	}

	if (log.isTraceEnabled()){
	    log.trace("exit function");
	}
    }

    private String get_column(byte [] data, int offset, int size)
    {
	long   value = 0;

	for (int i = offset + size - 1; i >= offset; i--){
	    value *= 256;
	    long b = data[i] & 0xFF;
	    
	    value += b;
	}

	return Long.toString(value);
    }
}