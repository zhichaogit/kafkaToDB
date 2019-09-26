package com.esgyn.kafkaCDC.server.kafkaConsumer.messageType;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.esgyn.kafkaCDC.server.database.ColumnValue;
import com.esgyn.kafkaCDC.server.kafkaConsumer.messageType.protobufSerializtion.MessageDb;
import com.esgyn.kafkaCDC.server.kafkaConsumer.messageType.protobufSerializtion.MessageDb.Column;
import com.esgyn.kafkaCDC.server.kafkaConsumer.messageType.protobufSerializtion.MessageDb.Record;
import com.esgyn.kafkaCDC.server.utils.ColumnInfo;
import com.esgyn.kafkaCDC.server.utils.TableInfo;
import com.esgyn.kafkaCDC.server.utils.Utils;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
/**
 * 
 * @author xzc
 *  
 */
public class ProtobufRowMessage extends RowMessage<byte[]> {
    private static Logger log                      = Logger.getLogger(ProtobufRowMessage.class);

    private int           offset                   = 0;
    private Record        messagePro               = null;


    String                catlogName               = null;
    String                emptystr                 = "";
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

    @Override
    protected boolean init_() {
	 try {
	    byte[] rec = message;
            messagePro = MessageDb.Record.parseFrom(rec);
	    msgString = new String(message);
	 } catch (InvalidProtocolBufferException e) {
	     log.error("parseFrom Record has error ,make sure you data pls", e);
	     return false;
	 }

	 return true;
    }

    @Override
    public Boolean analyzeMessage() {
        if (log.isTraceEnabled()) { log.trace("enter"); }
        
        // transaction information
        String tableNamePro = messagePro.getTableName();
        int sourceType      = messagePro.getSourceType(); //message source oracle/DRDS
        int keyColNum       = messagePro.getKeyColumnList().size(); // keycol size
        int colNum          = messagePro.getColumnList().size(); //col size

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
	    schemaName = params.getDatabase().getDefSchema();

        if (tableName==null) 
	    tableName = params.getDatabase().getDefTable();
	tableInfo = getTableInfo(schemaName + "." + tableName);   
	int operationType = messagePro.getOperationType();

        if (tableInfo == null ) {
            if (log.isDebugEnabled()) {
                log.error("Table [" + schemaName + "." + tableName
                        + "] is not exist in Database.");
            }

	    return false;
        }
        if (colNum != tableInfo.getColumnCount() && (sourceType != SOURCEORACLE)) {
            log.error("Table [" + schemaName + "." + tableName
                    + "] column count in EsgynDB is not equal column count in messages."
                    + "the kafka offset[" + getOffset() + "],the message:"+messagePro);
            return false;
        }
        StringBuffer strBuffer = null;
        if (log.isDebugEnabled()) {
            strBuffer = new StringBuffer();

            strBuffer.append("Raw message:[" + messagePro.toString() + "]\n");
            strBuffer.append(
                    "Operator Info: [Table Name: " + tableName + ", Type: " + operationType + "]\n");
        }

        // get keycolumn
        for (int i = 0; i < keyColNum; i++) {
            Column column = messagePro.getKeyColumnList().get(i);
            if (log.isDebugEnabled()) {
                strBuffer.append("\tColumn: " + column);
            }
            // "K" or "U"
            if (operationType==UPDATE_DRDS) {
                String oldValue = null;
                String newValue = null;
                try {
                    oldValue = bytesToString(column.getOldValue(), getEncoding(),
                            messagePro,column.getIndex(),column.getName());
                    newValue = bytesToString(column.getNewValue(), getEncoding(),
                            messagePro,column.getIndex(),column.getName());
                } catch (Exception e) {
                    return false;
                }
                if (!newValue.equals(oldValue)) {
                    operationType=UPDATE_COMP_PK_SQL_VAL;
                }
            }

            ColumnValue columnvalue = null;
            try {
                columnvalue = get_column(messagePro,column,tableInfo);
            } catch (Exception e) {
                return false;
            }

            columns.put(columnvalue.getColumnID(), columnvalue);
        }
        // get column
        for (int i = 0; i < colNum; i++) {
            Column column = messagePro.getColumnList().get(i);
            if (log.isDebugEnabled()) {
                strBuffer.append("\tColumn: " + column);
            }
            ColumnValue columnvalue= null;
            try {
                columnvalue = get_column(messagePro,column,tableInfo);
            } catch (Exception e) {
                return false;
            }

            columns.put(columnvalue.getColumnID(), columnvalue);
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

        if (log.isTraceEnabled()) { log.trace("exit"); }

        return true;
    }


    private ColumnValue get_column(Record messagePro,Column column,TableInfo tableInfo) throws Exception {
        String colTypeName = null;
        int sourceType = messagePro.getSourceType();//1.oracle 2.mysql(DRDS)
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
                ColumnInfo columnInfo = tableInfo.getColumn(index);
                colTypeName = columnInfo.getTypeName().trim();
                newValue = covertValue1(newNull,newValuebs,colTypeName,messagePro,index,colname);
                oldValue = covertValue1(oldNull,oldValuebs,colTypeName,messagePro,index,colname);
                break;
            case SOURCEDRDS:
                if (log.isDebugEnabled()) {
                    log.debug("the data maybe come form mysql(DRDS),source col name:"+colname);
                }
                ColumnInfo colInfo = tableInfo.getColumn("\"" + colname.toString() + "\"");
                if (colInfo==null) {
                    log.error("columnname from kafka is["+colname+"],"
                            + "it not exist database ["+schemaName+"."+tableName+"]\n"
                            + "the kafka message:"+messagePro);
                }else {
                    colTypeName = colInfo.getTypeName().trim();
                }
                index    = colInfo.getColumnID();
                newValue = covertValue2(newNull,newValuebs,colTypeName,messagePro,index,colname);
                oldValue = covertValue2(oldNull,oldValuebs,colTypeName,messagePro,index,colname);
                break;

            default:
                
                log.error("sourceType is :["+sourceType +"],it doesn't match any type!");

                break;
        }
        ColumnValue columnvalue = new ColumnValue(index, newValue, oldValue,colTypeName);

        if (log.isDebugEnabled()) {
            log.debug("colindex [" + index + "] ,colname [" + colname + "]"
                      + "cur value [" + newValue + "] old value [" + oldValue + "]"
                      + "columnTypeName ["+ colTypeName + "]");
        }
        return columnvalue;
    }
    
    //make sure the value is null or "" (oracle)
    private String covertValue1(boolean valueNull,ByteString Valuebs,String colTypeName,
            Record messagePro,int index,String colname) throws Exception {
        String value=null;
        String encode="GBK";
        if (valueNull && Valuebs.size()!=0 ) {
            value = null;
        }else if(valueNull && Valuebs.size()==0){
	    if(insertEmptyStr(colTypeName.toUpperCase())){
              value = "";
            }else if (Utils.isNumType(colTypeName)){
              value = "0";
            }
        }else {
	        if(!getEncoding().equals("UTF8")){
                encode = getEncoding();
                if(!getEncoding().equals("GBK"))
                log.warn("the data from oracle  default encode is GBK,but you set: " +encode);
            }
            value = bytesToString(Valuebs, encode,messagePro,index,colname);
            if(value.equals("0000-00-00 00:00:00")) {
                value = "0001-01-01 00:00:00"; 
            }else if (value.equals("0000-00-00")){
                value = "0001-01-01"; 
            }
        }
        return value;
    }
    //make sure the value is "" or not (drds)
    private String covertValue2(boolean valueNull,ByteString Valuebs,String colTypeName,
            Record messagePro,int index,String colname) throws Exception {
        String value=null;
	String encode="UTF8";
        if (valueNull) {
            value = null;
        }else if(Valuebs.size()==0){
            if(colTypeName!=null && insertEmptyStr(colTypeName.toUpperCase())){
              value = "";
            }else if (colTypeName!=null && Utils.isNumType(colTypeName)){
              value = "0";
            }
        }else{
            if(!getEncoding().equals("UTF8")){
                encode = getEncoding();
                log.warn("the data from drds(mysql)  default encode is UTF8,but you set: " +encode);
            }
            value = bytesToString(Valuebs, encode,messagePro ,index,colname);
            if(value.equals("0000-00-00 00:00:00")) {
                value = "0001-01-01 00:00:00"; 
            }else if (value.equals("0000-00-00")){
                value = "0001-01-01"; 
            }
        }
        return value;
    }

    public String bytesToString(ByteString src, String charSet,Record messagePro,int index,
            String colname) throws Exception {
        if (StringUtils.isEmpty(charSet)) {
            charSet = charSet;
        }
        return bytesToString(src.toByteArray(), charSet,messagePro,index,colname);
    }

    public String bytesToString(byte[] input, String charSet,Record messagePro,int index,
            String colname) throws Exception {
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
            // print err message when oracle data is utf8,else catch the error 
            if (charSet.equals("GBK")) {
                ByteBuffer buffer_Utf8 = ByteBuffer.allocate(input.length);
                buffer_Utf8.put(input);
                buffer_Utf8.flip();
                Charset charset_Utf8 = null;
                CharsetDecoder decoder_Utf8 = null;
                CharBuffer charBuffer_Utf8 = null;
                try {
                    charset_Utf8 = Charset.forName("UTF8");
                    decoder_Utf8 = charset_Utf8.newDecoder();
                    charBuffer_Utf8 = decoder_Utf8.decode(buffer_Utf8.asReadOnlyBuffer());
                    if (log.isDebugEnabled()) {
                        log.debug("data from oracle decode by UTF8 is success:"+charBuffer_Utf8.toString()+
                                ",the kafka offset["+getOffset()+"],the colindex ["+index+"],"
                                + "the colname["+colname+"],charset["+"UTF8"+"],the source message:"
                                +messagePro.toString());
                    }
                    return charBuffer_Utf8.toString();
                }catch (Exception e) {
                    log.error("data from oracle decode by UTF8 is faild when bytesToString.the kafka"
                            + " offset["+getOffset()+"],the colindex"+index+"],the colname["
                            +colname+"],charset["+charSet+"],the source message:"
                            +messagePro.toString(),e);
                    throw e;
                }
            }
            log.error("an exception has occured when bytesToString.the kafka offset["
                    +getOffset()+"],the colindex ["+index+"],the colname["+colname
                    +getOffset()+"],the colindex ["+index+"],the colname["+colname
                    +"],charset["+charSet+"],the source message:"+messagePro.toString(),ex);
            throw ex;
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

    @Override
    public String getErrorMsg(int batchOff, String type) {
	return getErrorMsg_(batchOff, type, messagePro.toString());
    }

    @Override
    public String getErrorMsg() { return getErrorMsg_(messagePro.toString()); }
}
