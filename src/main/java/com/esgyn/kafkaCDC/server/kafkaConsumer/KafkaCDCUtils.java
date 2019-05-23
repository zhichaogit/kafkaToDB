package com.esgyn.kafkaCDC.server.kafkaConsumer;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class KafkaCDCUtils {
    ArrayList<String> dateFormats = null;

    public KafkaCDCUtils() {
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
}
