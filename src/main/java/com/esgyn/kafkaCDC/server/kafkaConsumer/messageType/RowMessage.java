package com.esgyn.kafkaCDC.server.kafkaConsumer.messageType;

import java.util.Map;
import java.util.HashMap;
import java.io.UnsupportedEncodingException;

import com.esgyn.kafkaCDC.server.utils.ColumnInfo;
import com.esgyn.kafkaCDC.server.utils.TableInfo;
import com.esgyn.kafkaCDC.server.utils.Parameters;
import com.esgyn.kafkaCDC.server.utils.KafkaCDCParams;
import com.esgyn.kafkaCDC.server.database.ColumnValue;
import com.esgyn.kafkaCDC.server.kafkaConsumer.messageType.protobufSerializtion.MessageDb.Record;

import org.apache.log4j.Logger;

import lombok.Getter;
import lombok.Setter;

public class RowMessage<T> implements Cloneable{
    private static Logger          log          = Logger.getLogger(RowMessage.class);

    @Getter
    protected Parameters           params       = null;  
    @Getter
    protected Long                 offset       = 0L ;
    @Getter
    protected String               msgString    = null;
    @Getter
    protected T                    message      = null;
    @Getter
    protected String               schemaName   = null;
    @Getter
    protected String               tableName    = null;
    @Getter
    protected TableInfo            tableInfo    = null;
    @Getter
    protected String               delimiter    = null;
    @Getter
    protected String               encoding     = null;
    @Setter
    @Getter
    protected String               operatorType = "I";
    @Setter
    @Getter
    protected Map<Integer, ColumnValue> columns = null;

    @Getter
    protected int                  thread       = -1;
    @Getter
    protected long                 latestTime   = 0;
    @Getter
    protected long                 partitionID  = -1;
    @Getter
    protected String               topic        = null;


    public TableInfo getTableInfo(String tableName) {
	return params.getDatabase().getTableInfo(tableName);
    }

    public RowMessage() {}

    public boolean init(Parameters params_, int thread_, Long offset_,
			long latestTime_, long partitionID_, String topic_, 
			T message_) {
	boolean retValue = true;

        if (log.isTraceEnabled()) { log.trace("enter"); }

	params     = params_;
	thread     = thread_;
	offset     = offset_;
	latestTime = latestTime_;
	topic      = topic_;
	partitionID= partitionID_;
	message    = message_;
	
        schemaName = params.getDatabase().getDefSchema();
        tableName  = params.getDatabase().getDefTable();
        tableInfo  = params.getDatabase().getTableInfo(schemaName + "." + tableName);

        KafkaCDCParams kafkaCDC = params.getKafkaCDC();
	delimiter = "[" + kafkaCDC.getDelimiter() + "]";
	encoding   = kafkaCDC.getEncoding();

        if (log.isTraceEnabled()) {
            log.trace("thread id [" + thread + "], offset [" + offset 
		      + "], table name [" + schemaName + "."+ tableName
		      + "], delimiter \"" + delimiter + "\", encoding ["
		      + encoding + "], message [" + message + "]");
        }

        columns = new HashMap<Integer, ColumnValue>(0);

	retValue = init_();
        if (log.isTraceEnabled()) { log.trace("exit"); }

        return retValue;
    }

    protected boolean init_() {
	try {
            msgString = new String(((String) message).getBytes(encoding), "UTF-8");
        } catch (UnsupportedEncodingException usee) {
            log.error("the encoding [" + encoding + "] is not supported in java ["
		      + usee.getMessage() + "]", usee);
	    return false;
        }

	return true;
    }

    public Boolean AnalyzeMessage() {
        if (log.isTraceEnabled()) { log.trace("enter"); }

        String[]     cols = msgString.split(delimiter);
        StringBuffer strBuffer = null;

        if (log.isDebugEnabled()) {
            strBuffer = new StringBuffer();

            strBuffer.append("RowMessage thread [" + thread + "]\n");
            strBuffer.append("Raw message:[" + msgString + "]\n");
            strBuffer.append("Operator Info: [Table Name: " + tableName 
			     + ", Type: " + operatorType + "]");
        }

        for (int i = 0; i < cols.length; i++) {
            if (log.isDebugEnabled()) {
                strBuffer.append("\n\tColumn: " + cols[i]);
            }

	    String typeName = tableInfo.getColumn(i).getTypeName();
            ColumnValue columnValue = new ColumnValue(i, cols[i], null, typeName);
            columns.put(i, columnValue);
        }
        if (log.isDebugEnabled()) {
            strBuffer.append("\nRowMessage end");
            log.debug(strBuffer.toString());
        }

        if (log.isTraceEnabled()) { log.trace("exit"); }

        return true;
    }

    public String getMessage() { return msgString; }

    private String getInsertString() {
        ColumnInfo  columnInfo  = tableInfo.getColumn(0);
	ColumnValue columnValue = columns.get(columnInfo.getColumnOff());
	String      value = ") VALUES(" + columnValue.getCurValueStr();
	String      sql   = "INSERT INTO \"" + schemaName + "\"." 
	    + "\"" + tableName + "\"" + "(" + columnInfo.getColumnName();

        for (int i = 1; i < columns.size(); i++) {
	    columnInfo  = tableInfo.getColumn(i);
	    columnValue = columns.get(columnInfo.getColumnOff());

	    value += ", " + columnValue.getCurValueStr();
            sql += ", " + columnInfo.getColumnName();
        }

        sql += value + ");";

	return sql;
    } 

    private String getWhereCondition() {
        ColumnInfo  keyInfo  = tableInfo.getKey(0);
	ColumnValue keyValue = columns.get(keyInfo.getColumnOff());
        String      where = " WHERE " + keyInfo.getColumnName() 
	    + keyValue.getOldCondStr();

        for (int i = 1; i < tableInfo.getKeyCount(); i++) {
            keyInfo = tableInfo.getKey(i);
	    keyValue = columns.get(keyInfo.getColumnOff());

            where += " AND " + keyInfo.getColumnName() + keyValue.getOldCondStr();
        }

        return where;
    }

    private String getUpdateString() {
        ColumnInfo  columnInfo  = tableInfo.getColumn(0);
	ColumnValue columnValue = columns.get(0);

        String sql = "UPDATE \"" + schemaName + "\"." + "\"" + tableName + "\""
	    + " SET " + columnInfo.getColumnName() + " = " 
	    + columnValue.getCurValueStr();

        for (int i = 1; i < columns.size(); i++) {
	    columnInfo  = tableInfo.getColumn(i);
	    columnValue = columns.get(i);
            sql += ", " + columnInfo.getColumnName() + " = " 
		+ columnValue.getCurValueStr();
        }

        sql += getWhereCondition() + ";";

	return sql;
    } 

    private String getDeleteString() {
        String sql = "DELETE FROM \"" + schemaName + "\"." + "\"" + tableName + "\"";

        sql += getWhereCondition() + ";";

	return sql;
    } 

    public String toString() { 
	String sql;

	if (columns.size() > 0) {
	    switch(operatorType) {
	    case "I":
		sql = getInsertString();
		break;
	    
	    case "U":
	    case "K":
		sql = getUpdateString();
	    break;


	    case "D":
		sql = getDeleteString();
		break;

	    default:
		log.error("the row message type [" + operatorType + "] error");
		return null;
	    }
	} else {
	    sql = "-- data format error, check the kafka data please!";
	}

	sql += "--" + offset + "\r\n";

	return sql; 
    }

    public String getErrorMsg(int batchOff, String type) {
	return getErrorMsg_(batchOff, type, null);
    }

    protected String getErrorMsg_(int batchOff, String type, String msg) {
	String errorMsg = "Error on request #" + batchOff + ": Execute failed "
	    + "when operate the [" + type + "]\nthrow BatchUpdateException "
	    + "when deal whith the kafka message. table:[" + schemaName + "."
	    + tableName + "], offset:[" + offset + "], message type:["
	    + operatorType + "], source message:[" + message + "]";

	if (msg != null) {
	    errorMsg = "\nparsed message [" + msg + "]";
	}

	return errorMsg;
    }

    public String getErrorMsg() { return getErrorMsg_(null); }

    protected String getErrorMsg_(String msg) {
	String errorMsg = "kafka offset:[" + offset + "], table:[" 
	    + schemaName + "." + tableName + "], message type:["
	    + operatorType + "], source message:[" + message +"]";

	if (msg != null) {
	    errorMsg = "\nparsed message [" + msg + "]";
	}

	return errorMsg;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
