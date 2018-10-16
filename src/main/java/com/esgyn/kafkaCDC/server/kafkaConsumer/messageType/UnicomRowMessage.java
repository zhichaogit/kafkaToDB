package com.esgyn.kafkaCDC.server.kafkaConsumer.messageType;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import org.apache.log4j.Logger; 
 
import com.esgyn.kafkaCDC.server.esgynDB.ColumnValue;
import com.esgyn.kafkaCDC.server.esgynDB.MessageTypePara;

public class UnicomRowMessage extends RowMessage<String> {
    private static Logger   log          = Logger.getLogger(UnicomRowMessage.class);
    private final byte SEPARATOR_LEVEL_1    = 0x1;
    private final byte SEPARATOR_DATA       = 0x2;
    private final byte SEPARATOR_NULL       = 0x3;
    private final byte SEPARATOR_NO_DATA    = 0x4;
    private int             offset          = 0;

    String                  processID       = null;
    String                  scnSign         = null;
    String                  transactionID   = null;
    String                  localTID        = null;
    String                  transactionSign = null;
    String                  rebuildTID      = null;
    String                  transactionOff  = null;
    String                  catlogName      = null;
    String                  timestamp       = null;
    String                  emptystr        = "";
    String                  message         = null;
    public UnicomRowMessage() {}
    
    public UnicomRowMessage(MessageTypePara<String> mtpara) throws UnsupportedEncodingException
    {
	super(mtpara);
    }

    @Override
    public boolean init(MessageTypePara<String> mtpara_) throws UnsupportedEncodingException {
         super.init(mtpara_);
        message = new String(((String) mtpara.getMessage()).getBytes(mtpara.getEncoding()), "UTF-8");
        return true;
    }

    @Override
    public Boolean AnalyzeMessage()
    {
	if (log.isTraceEnabled()){
	    log.trace("exit function");
	}

	String[] formats = message.split("");

	// transaction information
	String[] tranFormats = formats[0].split("");
	processID = tranFormats[0];
	scnSign = tranFormats[1];
	transactionID = tranFormats[2];
	localTID = tranFormats[3];
	transactionSign = tranFormats[4];
	rebuildTID = tranFormats[5];
	transactionOff = tranFormats[6];

	String[] names = formats[1].split("[.]");
	if (names.length == 3){
	    catlogName = names[0];
	    if (schemaName == null)
		schemaName = names[1];
	    tableName = names[2];
	} else if(names.length == 2) {
	    if (schemaName == null)
		schemaName = names[0];
	    tableName = names[1];
	} else {
	    tableName = names[0];
	}
	operatorType = formats[2];
	timestamp = formats[3];

	StringBuffer strBuffer = null;
	if(log.isDebugEnabled()){
	    strBuffer = new StringBuffer();

	    strBuffer.append("Raw message:[" + message + "]\n");
	    strBuffer.append("Operator message: [pid: " + processID 
			     + ", scn sign:" + scnSign + ", transaction id:" 
			     + transactionID + ", local transaction id:" + localTID
			     + ", transaction sign" + transactionSign
			     + ", rebuild transaction id" + rebuildTID
			     + ", transaction offset:"  + transactionOff + "]\n");
	    strBuffer.append("Operator Info: [Table Name: " + tableName + ", Type: "
			     + operatorType + ", Timestamp: " + timestamp + "]");
	}

	columns = new HashMap<Integer, ColumnValue>(0);
	for (int i = 4; i < formats.length; i++) {
	    if(log.isDebugEnabled()){
		strBuffer.append("\n\tColumn: " + formats[i]);
	    }
	    offset = 0;
	    ColumnValue column = get_column(formats[i].getBytes());
	    columns.put(column.GetColumnID(), column);
	}
	if(log.isDebugEnabled()){
	    log.debug(strBuffer.toString());
	}

	if (log.isTraceEnabled()){
	    log.trace("exit function");
	}

	return true;
    }

    private int byte_array_to_long(byte[] data, int start, int length) {
	int result = 0;

	for (int i=0; i < length; i++) {
	    result *= 10;
	    result += (data[start+i] - '0');
	}

	return result;
    }

    private int get_value_length(byte[] coldata) 
    {
	int     start = offset;
	boolean stop = false;

	for (; offset < coldata.length; offset++) {
	    switch(coldata[offset]){
	    case SEPARATOR_LEVEL_1:
	    case SEPARATOR_DATA:
	    case SEPARATOR_NULL:
	    case SEPARATOR_NO_DATA:
	    	stop = true;
		break;
	    }
	    
	    if (stop)
	    	break;
	}
	return offset - start;
    }

    private String get_column_value(byte[] coldata) 
    {
	String value = null;

	switch(coldata[offset++]) {
	case SEPARATOR_DATA: {
	    int valueStart = offset;
	    int length = get_value_length(coldata);
	    value = new String (coldata, valueStart, length);
	    break;
	} 

	case SEPARATOR_NULL: {
	    value = null;
	    break;
	}

	case SEPARATOR_NO_DATA: {
	    value = emptystr;
	    break;
	}

	default:
	    log.error("unknown operator: " + coldata[offset-1] + " data [" 
		      + coldata + "]");
	}

	return value;
    }

    private ColumnValue get_column(byte [] coldata)
    {
	// analyze column id
	int offsetStart = offset;
	int length = get_value_length(coldata);
	int cid = byte_array_to_long(coldata, offsetStart, length);

	// analyze current value
	String currValue = get_column_value(coldata);
	// analyze old value
	String oldValue = get_column_value(coldata);
	    
	if (log.isDebugEnabled()) {
	    log.debug("cur value [" + currValue + "] old value [" + oldValue + "]");
	}
	return new ColumnValue(cid, currValue, oldValue);
    }
}
