package com.esgyn.kafkaCDC.server.kafkaConsumer.messageType;

import java.util.Map;
import java.util.HashMap;
import java.io.UnsupportedEncodingException;

import com.esgyn.kafkaCDC.server.utils.TableInfo;
import com.esgyn.kafkaCDC.server.utils.EsgynDBParams;
import com.esgyn.kafkaCDC.server.esgynDB.ColumnValue;
import com.esgyn.kafkaCDC.server.esgynDB.MessageTypePara;
import com.esgyn.kafkaCDC.server.kafkaConsumer.messageType.protobufSerializtion.MessageDb.Record;

import org.apache.log4j.Logger;

import lombok.Getter;
import lombok.Setter;

public class RowMessage<T> implements Cloneable{
    protected static Logger          log          = Logger.getLogger(RowMessage.class);
    public MessageTypePara<T>        mtpara       = null;
    public T                         message_     = null;
    public int                       thread       = -1;
    public TableInfo                 tableInfo    = null;
    public Record                    messagePro   = null;
    public byte[]                    data         = null;
    public String                    delimiter    = "\\,";

    @Getter
    protected String                 message      = null;
    @Getter
    protected String                 schemaName   = null;
    @Getter
    protected String                 tableName    = null;
    @Getter
    protected String                 operatorType = "I";
    @Setter
    @Getter
    protected Map<Integer, ColumnValue> columns     = null;

    public RowMessage() {}

    public RowMessage(MessageTypePara<T> mtpara_) throws UnsupportedEncodingException {
        init(mtpara_);
    }

    public boolean init(MessageTypePara<T> mtpara_) throws UnsupportedEncodingException {
        mtpara = mtpara_;
        EsgynDBParams esgynDB_ = mtpara.getEsgynDB();
        String delimiter_ = mtpara.getDelimiter();
        schemaName = esgynDB_.getDefSchema();
        tableName = esgynDB_.getDefTable();
        tableInfo = esgynDB_.getTableInfo(schemaName+"."+tableName);
        if (log.isTraceEnabled()) {
            log.trace("enter function [schema: " + schemaName + ", table: " + tableName
                    + ", delimiter: \"" + delimiter_ + "\", thread id: " + mtpara.getThread()
                    + ", message [" + mtpara.getMessage() + "]");
        }

        if (delimiter_ != null) {
            delimiter = "[" + delimiter_ + "]";

            if (log.isDebugEnabled()) {
                log.debug("delimiter is [" + delimiter + "]");
            }
        }
        thread = mtpara.getThread();
        message_ = mtpara.getMessage();

        if (log.isTraceEnabled()) {
            log.trace("exit function");
        }
        return true;
    }

    public Boolean AnalyzeMessage() {
        if (log.isTraceEnabled()) {
            log.trace("enter function");
        }
        try {
            message = new String(((String) message_).getBytes(mtpara.getEncoding()), "UTF-8");
        } catch (UnsupportedEncodingException usee) {
            log.error("the encoding is not supported in java [" + usee.getMessage() + "]",usee);
        }

        String[] formats = message.split(delimiter);
        StringBuffer strBuffer = null;

        if (log.isDebugEnabled()) {
            strBuffer = new StringBuffer();

            strBuffer.append("RowMessage thread [" + thread + "]\n");
            strBuffer.append("Raw message:[" + message + "]\n");
            strBuffer.append(
                    "Operator Info: [Table Name: " + tableName + ", Type: " + operatorType + "]");
        }

        columns = new HashMap<Integer, ColumnValue>(0);
        for (int i = 0; i < formats.length; i++) {
            if (log.isDebugEnabled()) {
                strBuffer.append("\n\tColumn: " + formats[i]);
            }
            ColumnValue columnValue = new ColumnValue(i, formats[i], null,tableInfo.getColumn(i).getTypeName());
            columns.put(i, columnValue);
        }
        if (log.isDebugEnabled()) {
            strBuffer.append("\nRowMessage end");
            log.debug(strBuffer.toString());
        }

        if (log.isTraceEnabled()) {
            log.trace("exit function");
        }

        return true;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
