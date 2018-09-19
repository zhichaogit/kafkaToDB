package com.esgyn.kafkaCDC.server.kafkaConsumer.messageType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import com.esgyn.kafkaCDC.server.esgynDB.ColumnInfo;
import com.esgyn.kafkaCDC.server.esgynDB.ColumnValue;
import com.esgyn.kafkaCDC.server.esgynDB.EsgynDB;
import com.esgyn.kafkaCDC.server.esgynDB.TableInfo;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map.Entry;
import org.apache.log4j.Logger;

public class JsonRowMessage extends RowMessage {
    private static Logger log = Logger.getLogger(JsonRowMessage.class);
    
    int ts = 0;
    int xid = 0;
    int xoffset = 0;
    String commit = null;
    String operatorTypeSource = null;
    String position = null;
    JsonNode dataJsonNode = null;
    JsonNode oldJsonNode = null;
    EsgynDB  esgynDB = null;


    public JsonRowMessage(EsgynDB esgynDB_, String delimiter_, int thread_, String message_) {
        super(esgynDB_.GetDefaultSchema(), esgynDB_.GetDefaultTable(), delimiter_, thread_, message_);
        esgynDB = esgynDB_;
    }

    @Override
    public Boolean AnalyzeMessage() {
        ObjectMapper mapper = new ObjectMapper();
        TableInfo    tableInfo = null;

        try {
	    JsonNode     node = mapper.readTree(message);

            schemaName = node.get("database").toString().replace("\"", "");
            tableName = node.get("table").toString().replace("\"", "");
            dataJsonNode = node.get("data");
            oldJsonNode = node.get("old");
	    
	    if (log.isDebugEnabled()) {	    
		log.debug("datajasonNode: ["+ dataJsonNode.toString() 
			  + "]\n schemam: [" + schemaName + ", tablename: [" 
			  + tableName + "}");
	    }

	    tableInfo = esgynDB.GetTableInfo( schemaName + "." + tableName);

	    if(tableInfo == null){
		if (log.isDebugEnabled()) {
		    log.error("Table [" + schemaName + "." + tableName 
			      + "] is not exist in EsgynDB.");
		}
	       
	       return false;
	    }

	    // get json data

	    if(node.get("type") != null){ 
	        operatorTypeSource = node.get("type").toString().replace("\"", "").toLowerCase();
	    }else{
	        log.warn("\"type\"  not exist in json data");
	    }

	    if(node.get("ts") != null){
                ts = Integer.valueOf(node.get("ts").toString());
	    }else{
	        log.warn("\"ts\" not exist in json data");
	    }

	    if(node.get("xid") != null){
	        xid = Integer.valueOf(node.get("xid").toString());
	    }else{
	        log.warn("\"xid\" not exist in json data");
	    }
	   if(node.get("commit") == null && node.get("xoffset") == null)
	    {
                log.warn("\"commit\" and \"xoffset\" can't not exist"
                    + " at the same time ,check your datas pls");
            }else if(node.get("commit") != null && node.get("xoffset") != null) {
                log.warn("\"commit\" and \"xoffset\" can't exist"
                    + " at the same time ,check your datas pls");
            }else {
                if(node.get("commit") != null)
            	commit = node.get("commit").toString().replace("\"", "");
            	if(node.get("xoffset") != null)
            	xoffset = Integer.valueOf(node.get("xoffset").toString()); 
            } 

	    if(node.get("position") !=null){
	        position = node.get("position").toString().replace("\"", "");
            }else{
	        log.warn("\"position\" not exist in json data");
	    }
	} catch (IOException e) {
            e.printStackTrace();
        }

        StringBuffer strBuffer = null;
        if (log.isDebugEnabled()) {
            strBuffer = new StringBuffer();

            strBuffer.append("Raw message:[" + message + "]\n");
            strBuffer.append("Operator message: [xid: " + xid + ", position:" 
			     + position);
            strBuffer.append("Operator Info: [Table Name: " + schemaName + "." 
			     + tableName + ", Type: " + operatorType 
			     + ", Timestamp: " + ts + "]\n"); 
	}
        
        columns = new HashMap<Integer, ColumnValue>(0);

	// analysis jsondata
        if (dataJsonNode.isObject()) {
            Iterator<Entry<String, JsonNode>> it = dataJsonNode.fields();
            while (it.hasNext()) {
                Entry<String, JsonNode> entry = it.next();
                String colNewData = entry.getValue().toString().replace("\"", "");
                String colName = "\"" + entry.getKey().toString() + "\"";

		ColumnInfo colInfo = tableInfo.GetColumn(colName);
		if(colInfo == null){
		    log.error("Table [" + schemaName + "." + tableName + "] have not column [" + colName + "]");
	       
		    return false;
	        }
                int    colId = colInfo.GetColumnID();
                ColumnValue columnValue = new ColumnValue(colId, colNewData, null);
                columns.put(colId, columnValue);
               
		if (log.isDebugEnabled()) {
		    strBuffer.append("\tcolumn name [" + colName + ":" + colId 
				      + "], column data [" + colNewData + "]");
		}
            }
        }
        
        //get old data
        if (oldJsonNode !=null && oldJsonNode.isObject()) {
            Iterator<Entry<String, JsonNode>> it = oldJsonNode.fields();
            while (it.hasNext()) {
                Entry<String, JsonNode> entry = it.next();
                String colNewData = entry.getValue().toString().replace("\"", "");
                String colName ="\"" +  entry.getKey().toString() + "\"";
                ColumnInfo colInfo = tableInfo.GetColumn(colName);
                int    colId = colInfo.GetColumnID();
                ColumnValue columnValue = columns.get(colId);
                if (columnValue != null) {
                    columnValue = new ColumnValue(colId, columnValue.GetCurValue(),colNewData );
                    
                } else {
                    columnValue = new ColumnValue(colId, colNewData, null);
                }

                if (operatorTypeSource.equals( "update") 
		    && !columnValue.GetCurValue().equals(columnValue.GetOldValue())){
		    operatorTypeSource = "updatePK";
		}
		columns.put(colId, columnValue);
                
		if (log.isDebugEnabled()) {
		    strBuffer.append("\tcolumn name [" + colName + ":" + colId 
				      + "], column old data [" + colNewData + "]");
		}
            }
        }
        
	switch (operatorTypeSource) {
            case "insert":
                operatorType="I";
                break;
            case "update":
                operatorType="U";
                break;
            case "updatePK":
                operatorType="K";
                break;
            case "delete":
                operatorType="D";
                break;
            default:
		log.error(operatorTypeSource + " not match any type,type e.g. (insert,update,delete)");
		return false;
            }

	if(log.isDebugEnabled()){
	    strBuffer.append("\noperatorType:[" + operatorType + "]\n");
	    log.debug(strBuffer);
	}

	return true;
    }
}
