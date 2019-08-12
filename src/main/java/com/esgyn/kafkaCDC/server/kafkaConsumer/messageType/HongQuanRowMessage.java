package com.esgyn.kafkaCDC.server.kafkaConsumer.messageType;

import java.util.Map;

import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;

import com.esgyn.kafkaCDC.server.esgynDB.ColumnInfo;
import com.esgyn.kafkaCDC.server.esgynDB.ColumnValue;
import com.esgyn.kafkaCDC.server.esgynDB.EsgynDB;
import com.esgyn.kafkaCDC.server.esgynDB.TableState;
import com.esgyn.kafkaCDC.server.esgynDB.MessageTypePara;

public class HongQuanRowMessage extends RowMessage<byte[]> {
    private static Logger           log         = Logger.getLogger(HongQuanRowMessage.class);

    private int                     length      = 0;
    private EsgynDB                 esgyndb     = null;
    private Map<String, TableState> tables      = null;
    private int[]                   fieldSizes  = null;
    private int[]                   fieldTypes  = null;
    private boolean                 allFixTypes = true;
    private boolean                 bigEndian   = true;


    public HongQuanRowMessage() {}

    public HongQuanRowMessage(MessageTypePara<byte[]> mtpara_) throws UnsupportedEncodingException {
        init(mtpara_);
    }

    @Override
    public boolean init(MessageTypePara<byte[]> mtpara_) throws UnsupportedEncodingException {
        super.init(mtpara_);
        if (log.isTraceEnabled()) {
            log.trace("enter function");
        }
        data = (byte[]) mtpara.getMessage();
        bigEndian = mtpara.getBigEndian();
        esgyndb = mtpara.getEsgynDB();
        tables = mtpara.getTables();
        tableInfo = mtpara.getTableState().GetTableInfo();

        if (log.isDebugEnabled()) {
            StringBuffer strBuffer = new StringBuffer();
            strBuffer.append("message [" + data + "] length: " + data.length + "\nraw data [");

            for (int i = 0; i < data.length; i++) {
                String temp = Integer.toHexString(data[i] & 0xFF);
                if (temp.length() == 1) {
                    temp = "0" + temp;
                }
                strBuffer.append(" " + temp);
            }

            strBuffer.append("]");
            log.debug(strBuffer);
        }

        fieldSizes = new int[(int) tableInfo.GetColumnCount()];
        fieldTypes = new int[(int) tableInfo.GetColumnCount()];
        for (int i = 0; i < tableInfo.GetColumnCount(); i++) {
            ColumnInfo column = tableInfo.GetColumn(i);
            fieldSizes[i] = column.GetColumnSize();
            fieldTypes[i] = column.GetColumnType();
            switch (fieldTypes[i]) {
                case 136: // TINYINT
                case 137: // UNSIGNED TINYINT
                case 130: // SIGNED SMALLINT
                case 131: // UNSIGNED SMALLINT
                case 132: // SIGNED INTEGER
                case 133: // UNSIGNED INTEGER
                case 134: // SIGNED LARGEINT
                case 138: // UNSIGNED LARGEINT
                    break;

                case 64: // VARCHAR
                case 2: // NCHAR
                case 0: // CHAR
                    allFixTypes = false;
                    break;

                default:
            }

            length += fieldSizes[i];
        }

        if (log.isDebugEnabled()) {
            log.debug("the table mode [" + fieldSizes + "] total size [" + length + "]");
        }
        return true;
    }


    @Override
    public Boolean AnalyzeMessage() {
        if (allFixTypes && length != data.length) {
            log.error("message error [" + data + "] message length: " + data.length + ", length: "
                    + length);
            return false;
        }

        int offset = 0;
        StringBuffer strBuffer = null;

        if (log.isDebugEnabled()) {
            strBuffer = new StringBuffer();
            strBuffer.append("RowMessage thread [" + thread + "]\n");
            strBuffer.append("Raw message:[" + data + "]\n");
            strBuffer.append(
                    "Operator Info: [Table Name: " + tableName + ", Type: " + operatorType + "]");
        }

        columns = new HashMap<Integer, ColumnValue>(0);
        for (int i = 0; i < fieldSizes.length; i++) {
            log.debug("i: " + i + ", offset: " + offset + ", field: " + fieldSizes[i]);
            ColumnValue columnValue = null;
            switch (fieldTypes[i]) {
                case 136: // TINYINT
                case 137: // UNSIGNED TINYINT
                case 130: // SIGNED SMALLINT
                case 131: // UNSIGNED SMALLINT
                case 132: // SIGNED INTEGER
                case 133: // UNSIGNED INTEGER
                case 134: // SIGNED LARGEINT
                case 138: // UNSIGNED LARGEINT
                    columnValue = new ColumnValue(i, get_column(data, offset, fieldSizes[i]), null,
                            tableInfo.GetColumn(i).GetTypeName());
                    break;

                case 64: // VARCHAR
                case 2: // NCHAR
                case 0: // CHAR
                    columnValue =
                            new ColumnValue(i, bytes2HexString(data, offset, fieldSizes[i]), null,
                                    tableInfo.GetColumn(i).GetTypeName());
                    break;

                default:
                    columns = null;
                    log.error("don't support data type [" + fieldTypes[i] + "], column id [" + i
                            + "] message [" + bytes2HexString(data, 0, data.length)
                            + "] message length: " + data.length);
                    return false;
            }

            if (log.isDebugEnabled()) {
                strBuffer.append(
                        "\n\tColumn: [" + columnValue.GetCurValue() + "], size: " + fieldSizes[i]);
            }

            columns.put(i, columnValue);
            offset += fieldSizes[i];
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

    private String get_column(byte[] data, int offset, int size) {
        long value = 0;
        if (bigEndian) {
            for (int i = offset; i < offset + size; i++) {
                value *= 256;
                long b = data[i] & 0xFF;

                value += b;
            }
        } else {
            for (int i = offset + size - 1; i >= offset; i--) {
                value *= 256;
                long b = data[i] & 0xFF;

                value += b;
            }
        }

        return Long.toString(value);
    }

    public static String bytes2HexString(byte[] b, int offset, int size) {
        int start = offset;

        if (start < 0) {
            start = 0;
        } else if (start > b.length) {
            start = b.length;
        }

        if (start + size > b.length) {
            size = b.length - start;
        }

        return new String(b, start, size);
    }

}
