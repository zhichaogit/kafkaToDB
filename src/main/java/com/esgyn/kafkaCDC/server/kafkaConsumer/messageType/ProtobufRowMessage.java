package com.esgyn.kafkaCDC.server.kafkaConsumer.messageType;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.HashMap;

import com.esgyn.kafkaCDC.server.esgynDB.ColumnInfo;
import com.esgyn.kafkaCDC.server.esgynDB.ColumnValue;
import com.esgyn.kafkaCDC.server.esgynDB.EsgynDB;
import com.esgyn.kafkaCDC.server.kafkaConsumer.messageType.RowMessage;
import com.esgyn.kafkaCDC.server.kafkaConsumer.messageType.protobufSerializtion.MessageDb;
import com.esgyn.kafkaCDC.server.kafkaConsumer.messageType.protobufSerializtion.MessageDb.Column;
import com.esgyn.kafkaCDC.server.kafkaConsumer.messageType.protobufSerializtion.MessageDb.Record;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.esgyn.kafkaCDC.server.esgynDB.MessageTypePara;
import com.esgyn.kafkaCDC.server.esgynDB.TableInfo;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
/**
 * 
 * @author xzc
 *  
 */
public class ProtobufRowMessage extends RowMessage<byte[]> {
    private static Logger log                      = Logger.getLogger(ProtobufRowMessage.class);

    private int           offset                   = 0;

    String                catlogName               = null;
    String                emptystr                 = "";
    EsgynDB               esgynDB                  = null;
    private final int     INSERT_DRDS              = 0;
    private final int     UPDATE_DRDS              = 1;
    private final int     DELETE_DRDS              = 2;
    private final int     DELETE_VAL               = 3;
    private final int     INSERT_VAL               = 5;
    private final int     UPDATE_VAL               = 10;
    private final int     UPDATE_COMP_ENSCRIBE_VAL = 11;
    private final int     UPDATE_COMP_SQL_VAL      = 15;
    private final int     TRUNCATE_TABLE_VAL       = 100;
    private final int     UPDATE_COMP_PK_SQL_VAL   = 115;
    private final int     SQL_DDL_VAL              = 160;
    private final int     SOURCEORACLE             = 1;
    private final int     SOURCEDRDS               = 2;
    public ProtobufRowMessage() {}

    public ProtobufRowMessage(MessageTypePara<byte[]> mtpara) throws UnsupportedEncodingException {
        super(mtpara);
    }

    @Override
    public boolean init(MessageTypePara<byte[]> mtpara_) throws UnsupportedEncodingException {
        super.init(mtpara_);
	 try {
	    byte[] rec = mtpara_.getMessage();
            messagePro = MessageDb.Record.parseFrom(rec);
        } catch (InvalidProtocolBufferException e) {
            log.error("parseFrom Record has error ,make sure you data pls",e);
            return false;
        }
        esgynDB = mtpara.getEsgynDB();
        return true;
    }

    @Override
    public Boolean AnalyzeMessage() {
        if (log.isTraceEnabled()) {
            log.trace("enter function");
        }
        
        TableInfo tableInfo = null;
        // transaction information
        String tableNamePro = messagePro.getTableName();
        int keyColNum = messagePro.getKeyColumnList().size(); // keycol size
        int colNum = messagePro.getColumnList().size(); //col size

        String[] names = tableNamePro.split("[.]");
        if (names.length == 3) {
            catlogName = names[0];
            if (schemaName == null)
                schemaName = names[1];
            tableName = names[2];
        } else if (names.length == 2) {
            if (schemaName == null)
                schemaName = names[0];
            tableName = names[1];
        } else {
            tableName = names[0];
        }

	if (schemaName==null) 
          schemaName = esgynDB.GetDefaultSchema();

        if (tableName==null) 
          tableName = esgynDB.GetDefaultTable();
          tableInfo = esgynDB.GetTableInfo(schemaName + "." + tableName);        
          int operationType = messagePro.getOperationType();

	if (tableInfo == null) {
            if (log.isDebugEnabled()) {
                log.error("Table [" + schemaName + "." + tableName
                        + "] is not exist in EsgynDB.");
            }

                return false;
        }
        StringBuffer strBuffer = null;
        if (log.isDebugEnabled()) {
            strBuffer = new StringBuffer();

            strBuffer.append("Raw message:[" + messagePro.toString() + "]\n");
            strBuffer.append(
                    "Operator Info: [Table Name: " + tableName + ", Type: " + operationType + "]\n");
        }

        columns = new HashMap<Integer, ColumnValue>(0);
        // get keycolumn
        for (int i = 0; i < keyColNum; i++) {
            Column column = messagePro.getKeyColumnList().get(i);
            if (log.isDebugEnabled()) {
                strBuffer.append("\tColumn: " + column);
            }
            // "K" or "U"
            if (operationType==UPDATE_DRDS) {
                String oldValue = bytesToString(column.getOldValue(), mtpara.getEncoding());
                String newValue = bytesToString(column.getNewValue(), mtpara.getEncoding());
                if (!newValue.equals(oldValue)) {
                    operationType=UPDATE_COMP_PK_SQL_VAL;
                }
            }

            ColumnValue columnvalue = get_column(messagePro,column,tableInfo);

            columns.put(columnvalue.GetColumnID(), columnvalue);
        }
        // get column
        for (int i = 0; i < colNum; i++) {
            Column column = messagePro.getColumnList().get(i);
            if (log.isDebugEnabled()) {
                strBuffer.append("\tColumn: " + column);
            }
            ColumnValue columnvalue = get_column(messagePro,column,tableInfo);

            columns.put(columnvalue.GetColumnID(), columnvalue);
        }
        //operationType
        switch (operationType) {
            case INSERT_DRDS:
            case INSERT_VAL: {
                operatorType = "I";
                break;
            }

            case DELETE_DRDS: 
            case DELETE_VAL: {
                operatorType = "D";
                break;
            }

            case UPDATE_DRDS: 
            case UPDATE_COMP_SQL_VAL: {
                operatorType = "U";
                break;
            }

            case UPDATE_COMP_PK_SQL_VAL: {
                operatorType = "K";
                break;
            }
            default:
                log.error("current operatorType is [" + operationType
                        + "] ,please make sure your operatortype");
        }

        if (log.isDebugEnabled()) {
            strBuffer.append("operatorType:[" + operatorType + "]\n");
            log.debug(strBuffer.toString());
        }

        if (log.isTraceEnabled()) {
            log.trace("exit function");
        }

        return true;
    }


    private ColumnValue get_column(Record message,Column column,TableInfo tableInfo) {
        String colTypeName = null;
        int sourceType = message.getSourceType();//1.oracle 2.mysql(DRDS)
        offset = 0;
        int index = column.getIndex(); // column index
        String colname = column.getName(); // column name
        boolean oldHave = column.getOldHave();
        boolean newHave = column.getNewHave();
        boolean oldNull = column.getOldNull();
        boolean newNull = column.getNewNull();
        ByteString oldValuebs = column.getOldValue();
        ByteString newValuebs = column.getNewValue();
        String newValue=null;
        String oldValue=null;
        
        //need found col index form tableInfo when mysql(DRDS)
        switch (sourceType) {
            case SOURCEORACLE:
                if (log.isDebugEnabled()) {
                    log.debug("the data maybe come form oracle,source col index:"+index);
                }
                ColumnInfo columnInfo = tableInfo.GetColumn(index);
                colTypeName = columnInfo.GetTypeName().trim();
                newValue = covertValue1(newNull,newValuebs,colTypeName);
                oldValue = covertValue1(oldNull,oldValuebs,colTypeName);
                break;
            case SOURCEDRDS:
                if (log.isDebugEnabled()) {
                    log.debug("the data maybe come form mysql(DRDS),source col index:"+index);
                }
                ColumnInfo colInfo = tableInfo.GetColumn("\"" + colname.toString() + "\"");
                colTypeName = colInfo.GetTypeName().trim();
                index    = colInfo.GetColumnID();
                newValue = covertValue2(newNull,newValuebs,colTypeName);
                oldValue = covertValue2(oldNull,oldValuebs,colTypeName);
                break;

            default:
                
                log.error("sourceType is :["+sourceType +"],it doesn't match any type!");

                break;
        }

    	ColumnValue columnvalue = new ColumnValue(index, newValue, oldValue);

        if (log.isDebugEnabled()) {
            log.debug("colindex [" + index + "] ,colname [" + colname + "]"
                      + "cur value [" + newValue + "] old value [" + oldValue + "]"
                      + "columnTypeName ["+ colTypeName + "]");
        }
        return columnvalue;
    }
    
    //make sure the value is null or "" (oracle)
    private String covertValue1(boolean valueNull,ByteString Valuebs,String colTypeName) {
        String value=null;
        String encode="GBK";
        if (valueNull && Valuebs.size()!=0 ) {
            value = null;
        }else if(valueNull && Valuebs.size()==0){
	    if(insertEmptyStr(colTypeName.toUpperCase())){
              value = "";
            }else if (insert0(colTypeName.toUpperCase())){
              value = "0";
            }
        }else {
	    if(!mtpara.getEncoding().equals("UTF8")){
                encode = mtpara.getEncoding();
                if(!mtpara.getEncoding().equals("GBK"))
                log.warn("the data from oracle  default encode is GBK,but you set: " +encode);
            }
            value = bytesToString(Valuebs, encode);
        }
        return value;
    }
    //make sure the value is "" or not (drds)
    private String covertValue2(boolean valueNull,ByteString Valuebs,String colTypeName) {
        String value=null;
	String encode="UTF8";
        if (valueNull) {
            value = null;
        }else if(Valuebs.size()==0){
            if(insertEmptyStr(colTypeName.toUpperCase())){
              value = "";
            }else if (insert0(colTypeName.toUpperCase())){
              value = "0";
            }
        }else{
            if(!mtpara.getEncoding().equals("UTF8")){
                encode = mtpara.getEncoding();
                log.warn("the data from drds(mysql)  default encode is UTF8,but you set: " +encode);
            }
            value = bytesToString(Valuebs, encode);
            if(value.equals("0000-00-00 00:00:00"))
            value = "0001-01-01 00:00:00"; 
        }
        return value;
    }

    public static String bytesToString(ByteString src, String charSet) {
        if (StringUtils.isEmpty(charSet)) {
            charSet = charSet;
        }
        return bytesToString(src.toByteArray(), charSet);
    }

    public static String bytesToString(byte[] input, String charSet) {
        if (ArrayUtils.isEmpty(input)) {
            return StringUtils.EMPTY;
        }
        ByteBuffer buffer = ByteBuffer.allocate(input.length);
        buffer.put(input);
        buffer.flip();
        Charset charset = null;
        CharsetDecoder decoder = null;
        CharBuffer charBuffer = null;
        try {
            charset = Charset.forName(charSet);
            decoder = charset.newDecoder();
            charBuffer = decoder.decode(buffer.asReadOnlyBuffer());
        } catch (Exception ex) {
            log.error("",ex);
        }
        return charBuffer.toString();
    }
    //if string type
    public static boolean insertEmptyStr(String colTypeName) {
        switch (colTypeName) {
            case "NCHAR":
            case "NCHAR VARYING":
            case "LONG VARCHAR":
            case "CHARACTER":
            case "VARCHAR":
                return true;
            default:
                return false;
        }
        
    }
    // if num type
    private static boolean insert0(String colTypeName) {
        switch (colTypeName) {
            case "SIGNED SMALLINT":
            case "SIGNED INTEGER":
            case "SIGNED NUMERIC":
            case "UNSIGNED NUMERIC":
            case "SIGNED DECIMAL":
            case "UNSIGNED DECIMAL":
            case "DOUBLE":
            case "UNSIGNED SMALLINT":
            case "SIGNED TINYINT":
            case "UNSIGNED TINYINT":
                return true;
            default:
                return false;
        }
    }

}
