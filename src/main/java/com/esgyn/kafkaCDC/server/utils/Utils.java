package com.esgyn.kafkaCDC.server.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class Utils {
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
		e.printStackTrace();
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
		e.printStackTrace();
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
}
