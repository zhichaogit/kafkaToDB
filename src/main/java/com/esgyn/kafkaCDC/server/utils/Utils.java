package com.esgyn.kafkaCDC.server.utils;

import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;

import org.apache.log4j.Logger;

public class Utils {
    private static Logger      log     =  Logger.getLogger(Utils.class);
    private static ArrayList<String> dateFormats 
	= new ArrayList<String>(Arrays.asList("yyyy-MM-dd HH:mm:ss",
					      "yyyy-MM-dd",
					      "yyyy/MM/dd HH:mm:ss",
					      "yyyy/MM/dd"));

    /**
     *
     * @param input Date string
     * @param dateFormat
     * @return
     */

    public static long dateToStamp(String str, String dateFormat) {
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
     * Long format stamp  to String format date
     * @param stamp
     * @return dateStr
     */
    public static String stampToDateStr(long stamp) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String dateStr = sdf.format(stamp);
        return  dateStr;
    }
    /**
     *  date format to string
     * @param date
     * @return date string
     */
    public static String dateToStr(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String dateStr = sdf.format(date);
        return dateStr;
    }

    /**
     * Determine whether it is a time type
     * @param datevalue
     * @return 
     */
    public static boolean isDateStr(String datevalue) {
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
     * @param intStr
     * @return if input string is integer, otherwise false
     */
    public static boolean isInteger(String intStr) {
        try{
            int i = Integer.parseInt(intStr);
            return true;
        }catch(NumberFormatException e){
          return false;
        }
    }
    /**
     * @param datevalue
     * @return string 
     */

    public static String whichDateFormat(String datevalue) {
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
    public static boolean isNumType(String colTypeName) {
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

    public static String getCurrentTime(){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date curTime = new Date();
        return sdf.format(curTime);
    }

    public static long getTime(){
	return new Date().getTime();
    }

    public static String getTrueName(String name)
    {
        if (name != null) {
            if (name.startsWith("[") && name.endsWith("]")) {
                name = name.substring(1, name.length() - 1);
                log.warn("The schema name is lowercase");
            } else {
                name = name.toUpperCase();
            }
        }

	return name;
    }

    public static boolean waitMillisecond(long milliseconds) {
	try {
	    Thread.sleep(milliseconds);
	} catch (Exception e) {
	    log.error("throw exception when call Thread.sleep.", e);
	    return false;
	}

	return true;
    }
    /**
     * @param passWord
     * @return decode passWord
     */
    public static String getDecodePW(String passWord) {
        if (passWord != null && passWord.startsWith("[") && passWord.endsWith("]")) {
            passWord = passWord.substring(1, passWord.length() - 1);
            passWord = decodeBase64(passWord, "utf-8");
        }
        return passWord;
    }
    /**
     * Base64 decode.
     * @param input
     * @param encoding
     * @return decodeBase64
     */
    public static String decodeBase64(String input,String encoding) {
        String decodeBase64 = null;
        try {
            decodeBase64 =
                    new String(Base64.getDecoder().decode(input.getBytes()), encoding);
        } catch (UnsupportedEncodingException e) {
            log.error("decode passWD has error"+e);
        }
        return decodeBase64;
    }
    /**
     * 
     * @param passWord
     * @return encodePW
     */
    public static String getEncodePW(String passWord) {
        String encodePW = null;
        if (passWord !=null) {
            encodePW = encodeBase64(passWord,"utf-8");
        }
        return ("[" + encodePW + "]");
    }
    /**
     * Base64 encode.
     * @param input
     * @return
     */
    public static String encodeBase64(String input,String encoding) {

        String asB64 = null;
        try {
            asB64 = Base64.getEncoder().encodeToString(input.getBytes(encoding));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return asB64;
    }
}
