package com.esgyn.kafkaCDC.server.kafkaConsumer.messageType;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.HashMap;

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
    Record                message                  = null;
    EsgynDB               esgynDB                  = null;

    private final int     DELETE_VAL               = 3;
    private final int     INSERT_VAL               = 5;
    private final int     UPDATE_VAL               = 10;
    private final int     UPDATE_COMP_ENSCRIBE_VAL = 11;
    private final int     UPDATE_COMP_SQL_VAL      = 15;
    private final int     TRUNCATE_TABLE_VAL       = 100;
    private final int     UPDATE_COMP_PK_SQL_VAL   = 115;
    private final int     SQL_DDL_VAL              = 160;

    public ProtobufRowMessage() {}

    public ProtobufRowMessage(MessageTypePara<byte[]> mtpara) throws UnsupportedEncodingException {
        super(mtpara);
    }

    @Override
    public boolean init(MessageTypePara<byte[]> mtpara_) throws UnsupportedEncodingException {
        super.init(mtpara_);
	 try {
	    byte[] rec = mtpara_.getMessage();
            message = MessageDb.Record.parseFrom(rec);
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return true;
    }

    @Override
    public Boolean AnalyzeMessage() {
        if (log.isTraceEnabled()) {
            log.trace("enter function");
        }
        
        TableInfo tableInfo = null;
        // transaction information
        String tableNamePro = message.getTableName();
        int keyColNum = message.getKeyColumnList().size(); // keycol size
        int colNum = message.getColumnList().size(); //col size


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

        int operationType = message.getOperationType();
        switch (operationType) {
            case INSERT_VAL: {
                operatorType = "I";
                break;
            }

            case DELETE_VAL: {
                operatorType = "D";
                break;
            }

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

        StringBuffer strBuffer = null;
        if (log.isDebugEnabled()) {
            strBuffer = new StringBuffer();

            strBuffer.append("Raw message:[" + message.toString() + "]\n");
            strBuffer.append(
                    "Operator Info: [Table Name: " + tableName + ", Type: " + operatorType + "]\n");
        }

        columns = new HashMap<Integer, ColumnValue>(0);
        // get keycolumn
        for (int i = 0; i < keyColNum; i++) {
            Column column = message.getKeyColumnList().get(i);
            if (log.isDebugEnabled()) {
                strBuffer.append("\tColumn: " + column);
            }

            ColumnValue columnvalue = get_column(column);

            columns.put(columnvalue.GetColumnID(), columnvalue);
        }
        // get column
        for (int i = 0; i < colNum; i++) {
            Column column = message.getColumnList().get(i);
            if (log.isDebugEnabled()) {
                strBuffer.append("\tColumn: " + column);
            }
            ColumnValue columnvalue = get_column(column);

            columns.put(columnvalue.GetColumnID(), columnvalue);
        }

        if (log.isDebugEnabled()) {
            log.debug(strBuffer.toString());
        }

        if (log.isTraceEnabled()) {
            log.trace("exit function");
        }

        return true;
    }


    private ColumnValue get_column(Column column) {
        offset = 0;
        int index = column.getIndex(); // column index
        boolean oldHave = column.getOldHave();
        boolean newHave = column.getNewHave();
        boolean oldNull = column.getOldNull();
        boolean newNull = column.getNewNull();
        String oldValue = oldNull ? null : bytesToString(column.getOldValue(), mtpara.getEncoding());
        String newValue = newNull ? null : bytesToString(column.getNewValue(), mtpara.getEncoding());
        
        //colID
        
        ColumnValue columnvalue = new ColumnValue(index, newValue, oldValue);

        if (log.isDebugEnabled()) {
            log.debug("cur value [" + newValue + "] old value [" + oldValue + "]");
        }
        return columnvalue;
    }

    private static String bytesToString(ByteString src, String charSet) {
        if (StringUtils.isEmpty(charSet)) {
            charSet = charSet;
        }
        return bytesToString(src.toByteArray(), charSet);
    }

    private static String bytesToString(byte[] input, String charSet) {
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
            ex.printStackTrace();
        }
        return charBuffer.toString();
    }


}
