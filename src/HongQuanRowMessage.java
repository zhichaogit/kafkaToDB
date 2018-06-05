import java.util.Map;
import java.util.HashMap;
import org.apache.log4j.Logger; 
 
public class HongQuanRowMessage extends RowMessage
{
    private int             length          = 0;
    private byte[]          data            = null;
    private int []          fieldSizes      = null;

    public HongQuanRowMessage(String defschema_, String deftable_, String delimiter_,
			      int thread_, byte [] message_)
    {
	super(defschema_, deftable_, delimiter_, thread_, null);

	data = message_;
	if (delimiter_ == null && delimiter_.length() == 0){
	    log.error("the delimiter is error [" + delimiter_ + "]");
	    return;
	}

	String[] fieldDelims = delimiter_.split("[|]");
	fieldSizes = new int[fieldDelims.length];
	for (int i = 0; i < fieldDelims.length; i++) {
	    fieldSizes[i] = Integer.parseInt(fieldDelims[i]);
	    length += fieldSizes[i];
	}

	if (length > message.length()) {
	    log.error("message error [" + message + "] message length: " 
		      + message.length() + ", length: " + length);
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

	if (length > message.length()) {
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

	for (int i = offset; i < offset + size; i++){
	    value *= 10;
	    value += (data[i] & 0xFF);
	}

	return Long.toString(value);
    }
}