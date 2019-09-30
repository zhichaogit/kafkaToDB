package com.esgyn.kafkaCDC.server.kafkaConsumer.messageType;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.esgyn.kafkaCDC.server.database.ColumnValue;
import com.esgyn.kafkaCDC.server.utils.ColumnInfo;
import com.esgyn.kafkaCDC.server.utils.TableInfo;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

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
    String                colNewData         = null;
    String                colOldData         = null;

    public UnicomJsonRowMessage() {}

    @Override
    public Boolean analyzeMessage() {
        ObjectMapper mapper = new ObjectMapper();

        StringBuffer strBuffer = null;
        if (log.isDebugEnabled()) {
            strBuffer = new StringBuffer();

            strBuffer.append("start analyzeMessage...\n");

        }

        if (schemaName==null && desSchema != null) {
            schemaName = desSchema;
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
		tableInfo = getTableInfo(schemaName + "." + tableName);
                if (tableInfo == null) {
                    if (log.isDebugEnabled()) {
                        log.error("Table [" + schemaName + "." + tableName
                                + "] is not exist in database.");
                    }

                    return false;
                }

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
            log.error("an exception has occured when analyzeMessage ", e);
        }


        if (log.isDebugEnabled()) {
            strBuffer.append("\noperatorType:[" + operatorType + "]\n");
            log.debug(strBuffer.toString());
        }

        if (log.isTraceEnabled()) { log.trace("exit"); }

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
            log.trace("enter get column json:[" + entry + "]");
        }
        String colName = "\"" + entry.getKey().toString() + "\"";
        String colNewData = null;
        String colOldData = null;
        if (operatorTypeSource.equals("insert")) {
            colNewData = entry.getValue().toString().replace("\"", "");
        } else if (operatorTypeSource.equals("delete")) {
            colOldData = entry.getValue().toString().replace("\"", "");
        }

        ColumnInfo colInfo = tableInfo.getColumn(colName);

        if (colInfo == null) {
            log.error("Table [" + schemaName + "." + tableName + "] have not column [" + colName
                    + "]");
            return false;
        }
        int colId = colInfo.getColumnID();


        ColumnValue columnValue = new ColumnValue(colId, colNewData, colOldData,colInfo.getTypeName());

        columns.put(colId, columnValue);

        if (log.isDebugEnabled()) {

            strBuffer.append("column name :[" + colName + "], colID:[" + colId
                    + "], columnNewdate :[" + colNewData + "],  colOldData [" + colOldData + "]\n");
        }

        if (log.isTraceEnabled()) { log.trace("exit"); }

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
        ColumnInfo colInfo = tableInfo.getColumn(colName);

        if (colInfo == null) {
            log.error("Table [" + schemaName + "." + tableName + "] have not column [" + colName
                    + "]");

            return false;
        }
        if (isOdd(i)) {
            int colId = colInfo.getColumnID();
            ColumnValue columnValue = new ColumnValue(colId, colNewData, colOldData,colInfo.getTypeName());

            if (ifKeyCol && operatorTypeSource.equals("update")
                    && !columnValue.getCurValue().equals(columnValue.getOldValue())) {
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
