package com.esgyn.kafkaCDC.server.kafkaConsumer.messageType;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Iterator;

import com.esgyn.kafkaCDC.server.esgynDB.ColumnInfo;
import com.esgyn.kafkaCDC.server.esgynDB.ColumnValue;
import com.esgyn.kafkaCDC.server.esgynDB.EsgynDB;
import com.esgyn.kafkaCDC.server.esgynDB.TableInfo;
import com.esgyn.kafkaCDC.server.esgynDB.MessageTypePara;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map.Entry;
import org.apache.log4j.Logger;

/**
 * 
 * @author xzc support Unicom new json format
 *
 */
public class UnicomJsonRowMessage extends RowMessage<String> {
    private static Logger log                = Logger.getLogger(UnicomJsonRowMessage.class);

    String                operatorTypeSource = null;
    JsonNode              columnJsonNode     = null;
    JsonNode              keyColJsonNode     = null;
    EsgynDB               esgynDB            = null;
    String                message            = null;
    String                colNewData         = null;
    String                colOldData         = null;

    public UnicomJsonRowMessage() {}

    public UnicomJsonRowMessage(MessageTypePara<String> mtpara)
            throws UnsupportedEncodingException {
        init(mtpara);

    }

    @Override
    public boolean init(MessageTypePara<String> mtpara) throws UnsupportedEncodingException {
        super.init(mtpara);
        message =
                new String(((String) mtpara.getMessage()).getBytes(mtpara.getEncoding()), "UTF-8");
        esgynDB = mtpara.getEsgynDB();
        return true;
    }

    @Override
    public Boolean AnalyzeMessage() {
        ObjectMapper mapper = new ObjectMapper();
        TableInfo tableInfo = null;

        StringBuffer strBuffer = null;
        if (log.isDebugEnabled()) {
            strBuffer = new StringBuffer();

            strBuffer.append("start analyzeMessage...\n");

        }
        try {
            JsonNode nodes = mapper.readTree(message);
            for (JsonNode node : nodes) {
                tableName = node.get("table_name").toString().replace("\"", "").toUpperCase();
                columnJsonNode = node.get("column");
                keyColJsonNode = node.get("key_column");

                if (node.get("operate") != null) {
                    operatorTypeSource =
                            node.get("operate").toString().replace("\"", "").toLowerCase();
                } else {
                    log.warn("\"type\"  not exist in json data");
                }

                if (log.isDebugEnabled()) {
                    strBuffer.append(
                            "schema: [" + schemaName + "], tablename: [" + tableName + "]\n");
                    if (columnJsonNode != null)
                        strBuffer.append("columnJsonNode: [" + columnJsonNode.toString() + "]\n");
                    if (keyColJsonNode != null)
                        strBuffer.append("keyColJsonNode: [" + keyColJsonNode.toString() + "]\n");
                    if (operatorTypeSource != null)
                        strBuffer.append("operatorTypeSource: [" + operatorTypeSource + "]\n");
                    // log.debug(strBuffer);
                }
		tableInfo = esgynDB.GetTableInfo(schemaName + "." + tableName);
                if (tableInfo == null) {
                    if (log.isDebugEnabled()) {
                        log.error("Table [" + schemaName + "." + tableName
                                + "] is not exist in EsgynDB.");
                    }

                    return false;
                }

                columns = new HashMap<Integer, ColumnValue>(0);
                // analysis columnjsondata
                if (columnJsonNode != null) {
                    if (log.isDebugEnabled()) {
                        strBuffer.append("analyze columnjsondata ...\n");
                    }
                    boolean colInfoIsNull = analyJsonData(columnJsonNode, tableInfo, false);
                    if (!colInfoIsNull)
                        return false;



                }
                // analysis key_columnjsondata
                if (keyColJsonNode != null) {
                    if (log.isDebugEnabled()) {
                        strBuffer.append("analyze key_columnjsondata ...\n");
                    }
                    boolean colInfoIsNull = analyJsonData(keyColJsonNode, tableInfo, true);
                    if (!colInfoIsNull)
                        return false;

                }

                // make sure operatorType
                switch (operatorTypeSource) {
                    case "insert":
                        operatorType = "I";
                        break;
                    case "update":
                        operatorType = "U";
                        break;
                    case "updatePK":
                        operatorType = "K";
                        break;
                    case "delete":
                        operatorType = "D";
                        break;
                    default:
                        log.error(operatorTypeSource
                                + " not match any type,type e.g. (insert,update,delete)");
                        return false;
                }


            }
        } catch (IOException e) {
            e.printStackTrace();
        }


        if (log.isDebugEnabled()) {
            strBuffer.append("\noperatorType:[" + operatorType + "]\n");
            log.debug(strBuffer.toString());
        }
        if (log.isTraceEnabled()) {
            log.trace("exit function");
        }

        return true;
    }

    // analy json data
    private boolean analyJsonData(JsonNode jsonNodes, TableInfo tableInfo, boolean ifKeyCol) {
        StringBuffer strBuffer = new StringBuffer();
        int i = 0;

        for (JsonNode jsonNode : jsonNodes) {
            if (log.isTraceEnabled()) {
                log.trace("JsonNode:[" + jsonNode + "]");
            }
            Iterator<Entry<String, JsonNode>> it = jsonNode.fields();

            if (operatorTypeSource.equals("update")) {
                while (it.hasNext()) {
                    Entry<String, JsonNode> entry = it.next();
                    boolean colInfoIsNull =
                            get_update_column(entry, tableInfo, i, ifKeyCol, strBuffer);
                    i++;
                    if (!colInfoIsNull)
                        return false;
                }
            } else {
                if (log.isTraceEnabled()) {
                    log.trace("JsonNode:[" + jsonNode + "]");
                }
                Iterator<Entry<String, JsonNode>> itK = jsonNode.fields();
                while (itK.hasNext()) {
                    Entry<String, JsonNode> entry = itK.next();
                    boolean colInfoIsNull = get_column(entry, tableInfo, strBuffer);
                    if (!colInfoIsNull)
                        return false;
                }
            }
        }
        if (log.isDebugEnabled()) {
            log.debug(strBuffer.toString());
        }
        return true;

    }


    private boolean get_column(Entry<String, JsonNode> entry, TableInfo tableInfo,
            StringBuffer strBuffer) {
        if (log.isTraceEnabled()) {
            log.trace("enter function");
            log.trace("get column json:[" + entry + "]");
        }
        String colName = "\"" + entry.getKey().toString() + "\"";
        String colNewData = null;
        String colOldData = null;
        if (operatorTypeSource.equals("insert")) {
            colNewData = entry.getValue().toString().replace("\"", "");
        } else if (operatorTypeSource.equals("delete")) {
            colOldData = entry.getValue().toString().replace("\"", "");
        }

        ColumnInfo colInfo = tableInfo.GetColumn(colName);

        if (colInfo == null) {
            log.error("Table [" + schemaName + "." + tableName + "] have not column [" + colName
                    + "]");
            return false;
        }
        int colId = colInfo.GetColumnID();


        ColumnValue columnValue = new ColumnValue(colId, colNewData, colOldData);

        columns.put(colId, columnValue);

        if (log.isDebugEnabled()) {

            strBuffer.append("column name :[" + colName + "], colID:[" + colId
                    + "], columnNewdate :[" + colNewData + "],  colOldData [" + colOldData + "]\n");
        }
        if (log.isTraceEnabled()) {
            log.trace("exit function");
        }

        return true;
    }

    // get the column when operate is update
    private boolean get_update_column(Entry<String, JsonNode> entry, TableInfo tableInfo, int i,
            boolean ifKeyCol, StringBuffer strBuffer) {

        String colName = "\"" + entry.getKey().toString() + "\"";
        // Odd num is columnnewdata, Even num is columnOlddata
        if (isOdd(i)) {
            colNewData = entry.getValue().toString().replace("\"", "");
        } else {
            colOldData = entry.getValue().toString().replace("\"", "");
            if (log.isTraceEnabled()) {
                log.trace("colOldData:[" + colOldData + "]");
            }
        }
        ColumnInfo colInfo = tableInfo.GetColumn(colName);

        if (colInfo == null) {
            log.error("Table [" + schemaName + "." + tableName + "] have not column [" + colName
                    + "]");

            return false;
        }
        if (isOdd(i)) {
            int colId = colInfo.GetColumnID();
            ColumnValue columnValue = new ColumnValue(colId, colNewData, colOldData);

            if (ifKeyCol && operatorTypeSource.equals("update")
                    && !columnValue.GetCurValue().equals(columnValue.GetOldValue())) {
                operatorTypeSource = "updatePK";
            }

            columns.put(colId, columnValue);
            if (log.isDebugEnabled()) {
                strBuffer.append(
                        "column name :[" + colName + "], colID:[" + colId + "], columnNewdate :["
                                + colNewData + "],  colOldData [" + colOldData + "]\n");
            }

        }

        return true;

    }

    // if odd
    private static boolean isOdd(int a) {
        if (a % 2 != 0) {
            // odd num
            return true;
        } else {
            // Even num
            return false;
        }
    }

}