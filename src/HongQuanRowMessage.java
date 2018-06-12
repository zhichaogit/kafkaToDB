import java.util.Map;
import java.util.HashMap;
import org.apache.log4j.Logger; 
 
public class HongQuanRowMessage extends RowMessage
{
    private int             length          = 0;
    private byte[]          data            = null;
    private int []          fieldSizes      = null;
    private int []          fieldTypes      = null;
    private boolean          allFixTypes    = true;

    public HongQuanRowMessage(TableInfo tableInfo_, int thread_, byte[] message_)
    {
	super(tableInfo_.GetSchemaName(), tableInfo_.GetTableName(), null, thread_, null);

	data = message_;
	fieldSizes = new int[(int)tableInfo_.GetColumnCount()];
	fieldTypes = new int[(int)tableInfo_.GetColumnCount()];
	for (int i = 0; i < tableInfo_.GetColumnCount(); i++) {
	    ColumnInfo column = tableInfo_.GetColumn(i);
	    fieldSizes[i] = column.GetColumnSize();
	    fieldTypes[i] = column.GetColumnType();
	    switch(fieldTypes[i]){
	    case 136:  		// TINYINT
	    case 137:  		// UNSIGNED TINYINT
	    case 130:		// SIGNED SMALLINT
	    case 131:		// UNSIGNED SMALLINT
	    case 132:		// SIGNED INTEGER
	    case 133:		// UNSIGNED INTEGER
	    case 134:		// SIGNED LARGEINT
	    case 138:		// UNSIGNED LARGEINT
		break;

	    case 64:		// VARCHAR
	    case 2:		// NCHAR
	    case 0:		// CHAR
		allFixTypes = false;
		break;

	    default:
	    }

	    length += fieldSizes[i];
	}
	
	if (log.isDebugEnabled()){
	    log.info("the table mode [" + fieldSizes + "] total size [" 
		     + length + "]");
	}
    }

    @Override
    public void AnalyzeMessage()
    {
	if (allFixTypes && length != data.length){
	    log.error("message error [" + data + "] message length: "
		      + data.length + ", length: " + length);
	    return;
	}

	int          offset    = 0;
	StringBuffer strBuffer = null;

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
	    ColumnValue columnValue = null;
	    switch(fieldTypes[i]){
	    case 136:  		// TINYINT
	    case 137:  		// UNSIGNED TINYINT
	    case 130:		// SIGNED SMALLINT
	    case 131:		// UNSIGNED SMALLINT
	    case 132:		// SIGNED INTEGER
	    case 133:		// UNSIGNED INTEGER
	    case 134:		// SIGNED LARGEINT
	    case 138:		// UNSIGNED LARGEINT
		columnValue = new ColumnValue(i, get_column(data, offset, fieldSizes[i]), null);
		break;

	    case 64:		// VARCHAR
	    case 2:		// NCHAR
	    case 0:		// CHAR
		columnValue = new ColumnValue(i, bytes2HexString(data, offset, fieldSizes[i]), null);
		break;

	    default:
		columns = null;
		log.error("don't support data type [" + fieldTypes[i] 
			  + "], column id [" + i + "] message [" 
			  + bytes2HexString(data, 0, data.length)
			  + "] message length: " + data.length);
		return;
	    }

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

    public static String bytes2HexString(byte[] b, int offset, int size) {  
	int start = offset;

	if (start < 0){
	    start = 0;
	} else if (start > b.length) {
	    start = b.length;
	}

	if (start + size > b.length){
	    size = b.length - start;
	}

	return new String(b, start, size);  
    }  
}
