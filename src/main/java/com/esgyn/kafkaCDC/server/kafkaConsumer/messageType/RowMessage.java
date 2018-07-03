package com.esgyn.kafkaCDC.server.kafkaConsumer.messageType;

import java.util.Map;
import java.util.HashMap;
import org.apache.log4j.Logger; 
 
import com.esgyn.kafkaCDC.server.esgynDB.ColumnValue; 
 
public class RowMessage
{
    String                  message         = null;
    String                  schemaName      = null;
    String                  tableName       = null;
    String                  delimiter       = "\\,";
    String                  operatorType    = "I";
    int                     thread          = -1;

    protected static Logger log = Logger.getLogger(RowMessage.class); 

    Map<Integer, ColumnValue>   columns = null;

    public RowMessage(String defschema_, String deftable_, String delimiter_,
		      int thread_, String message_)
    {
	if (log.isTraceEnabled()){
	    log.trace("enter function [schema: " + defschema_ + ", table: "
		      + deftable_ + ", delimiter: \"" + delimiter_ 
		      + "\", thread id: " + thread_ + ", message [" + message_ 
		      + "]");
	}

	schemaName = defschema_;
	tableName = deftable_;
	if (delimiter_ != null) {
	    delimiter = "[" + delimiter_ + "]";

	    if (log.isDebugEnabled()){
		log.debug("delimiter is [" + delimiter + "]");
	    }
	} 
	thread = thread_;
	message = message_;

	if (log.isTraceEnabled()){
	    log.trace("exit function");
	}
    }

    public void AnalyzeMessage()
    {
	if (log.isTraceEnabled()){
	    log.trace("enter function");
	}

	String[] formats = message.split(delimiter);
	StringBuffer strBuffer = null;

	if(log.isDebugEnabled()){
	    strBuffer = new StringBuffer();

	    strBuffer.append("RowMessage thread [" + thread + "]\n");
	    strBuffer.append("Raw message:[" + message + "]\n");
	    strBuffer.append("Operator Info: [Table Name: " + tableName 
			     + ", Type: " + operatorType + "]");
	}

	columns = new HashMap<Integer, ColumnValue>(0);
	for (int i = 0; i < formats.length; i++) {
	    if(log.isDebugEnabled()){
		strBuffer.append("\n\tColumn: " + formats[i]);
	    }
	    ColumnValue columnValue = new ColumnValue(i, formats[i], null);
	    columns.put(i, columnValue);
	}
	if(log.isDebugEnabled()){
	    strBuffer.append("\nRowMessage end");
	    log.debug(strBuffer.toString());
	}

	if (log.isTraceEnabled()){
	    log.trace("exit function");
	}
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

    public String GetMessage()
    {
	return message;
    }

    public Map<Integer, ColumnValue> GetColumns()
    {
	return columns;
    }
}
