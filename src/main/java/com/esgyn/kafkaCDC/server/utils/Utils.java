package com.esgyn.kafkaCDC.server.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import com.esgyn.kafkaCDC.server.bean.ConfBean;
import com.esgyn.kafkaCDC.server.bean.MappingBean;
import com.google.gson.Gson;

public class Utils {
    private static Logger      log     =  Logger.getLogger(Utils.class);
    ArrayList<String> dateFormats = null;

    public Utils() {
        dateFormats = new ArrayList<>();
        dateFormats.add("yyyy-MM-dd HH:mm:ss");
        dateFormats.add("yyyy-MM-dd");
        dateFormats.add("yyyy/MM/dd HH:mm:ss");
        dateFormats.add("yyyy/MM/dd");
    }
    
    /**
     * 
     * @param input Date string
     * @param dateFormat
     * @return
     */

    public long dateToStamp(String str, String dateFormat) {

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
        Date date = null;
        try {
            date = simpleDateFormat.parse(str);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        long ts = date.getTime();
        return ts;
    }
    /**
     * Determine whether it is a time type
     * @param datevalue
     * @return 
     */
    public boolean isDateStr(String datevalue) {
        datevalue = datevalue.trim();
        for (String dateFormat : dateFormats) {
            try {
                SimpleDateFormat fmt = new SimpleDateFormat(dateFormat);
                Date dd = fmt.parse(datevalue);
                if (datevalue.equals(fmt.format(dd))) {
                    return true;
                }
            } catch (Exception e) {
            }
        }
        return false;

    }
    /**
     * @param datevalue
     * @return string 
     */

    public String whichDateFormat(String datevalue) {
        for (String dateFormat : dateFormats) {
            try {
                SimpleDateFormat fmt = new SimpleDateFormat(dateFormat);
                Date dd = fmt.parse(datevalue);
                if (datevalue.equals(fmt.format(dd))) {
                    return dateFormat;
                }
            } catch (Exception e) {
            }
        }
        return null;
    }
    /**
     * if num type
     * @param colTypeName
     * @return
     */
    public boolean isNumType(String colTypeName) {
        colTypeName=colTypeName.trim().toUpperCase();
        switch (colTypeName) {
            case "SIGNED SMALLINT":
            case "SIGNED INTEGER":
            case "SIGNED BIGINT":
            case "SIGNED LARGEINT":
            case "SIGNED NUMERIC":
            case "UNSIGNED NUMERIC":
            case "SIGNED DECIMAL":
            case "UNSIGNED DECIMAL":
            case "DOUBLE":
            case "UNSIGNED SMALLINT":
            case "SIGNED TINYINT":
            case "UNSIGNED TINYINT":
            case "UNSIGNED INTEGER":
                return true;
            default:
                return false;
        }
    }
    /**
     *  read paras from json file
     * @param inputPath
     * @return jsonString
     */
    public String readJsonConf(String inputPath) {
        File jsonFile = new File(inputPath);
        if (jsonFile.exists() && jsonFile.isFile()) {
            FileInputStream fileInputStream;
            try {
                fileInputStream = new FileInputStream(jsonFile);
                InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
                int ch=0;
                StringBuffer sb = new StringBuffer();
                while ((ch=inputStreamReader.read())!=-1) {
                    sb.append((char)ch);
                }
                inputStreamReader.close();
                return sb.toString();
            } catch (IOException e) {
                log.error("read jsonFile from conf/  cause an error",e);
            }
        }else{
            log.error("["+inputPath+"],not exists or not a file");
        }
        return null;
    }
    /**
     * Parse json formatted data via Gson
     * @param jsonString
     * @return  jsonconfBean
     */

    public ConfBean jsonParse(String jsonString) throws Exception{
        ConfBean jsonConf=null;
        try {
            Gson gson = new Gson();
             jsonConf = gson.fromJson(jsonString,ConfBean.class);
        } catch (Exception e) {
           throw e;
        }
        return jsonConf;
    }
}
