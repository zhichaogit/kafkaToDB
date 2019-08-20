package com.esgyn.kafkaCDC.server.esgynDB;

import com.esgyn.kafkaCDC.server.utils.Utils;

import lombok.Getter;
import lombok.Setter;

public class ColumnValue {
    @Setter
    @Getter
    private int    columnID     = 0;
    @Setter
    @Getter
    private String curValue     = null;
    @Setter
    @Getter
    private String curValueStr  = null;
    @Setter
    @Getter
    private String oldValue     = null;
    @Setter
    @Getter
    private String oldValueStr  = null;
    @Setter
    @Getter
    private String nullValueStr = "null";
    @Setter
    @Getter
    private String oldCondStr   = " is null";


    public boolean curValueIsNull() { return curValue == null; }
    public boolean oldValueIsNull() { return oldValue == null; }

    public ColumnValue(int columnID_, String curValue_, String oldValue_,String colTypeName_) {
        Utils utils = new Utils();
        columnID = columnID_;

        if (curValue_ != null) {
            curValue = curValue_.replace("\"", "").replace("\'", "");
            if (utils.isNumType(colTypeName_)) {
                curValueStr = curValue;
            }else {
                curValueStr = "\'" + curValue + "\'";
            }
        } else {
            curValueStr = nullValueStr;
        }

        if (oldValue_ != null) {
            oldValue = oldValue_.replace("\"", "").replace("\'", "");
            if (utils.isNumType(colTypeName_)) {
                oldValueStr = oldValue;
            }else {
                oldValueStr = "\'" + oldValue + "\'";
            }
            oldCondStr = " = " + oldValueStr;
        } else {
            oldValueStr = nullValueStr;
        }
    }

    public ColumnValue(ColumnValue value) {
        columnID = value.getColumnID();
        curValue = value.getCurValue();
        oldValue = value.getOldValue();
        curValueStr = value.getCurValueStr();
        oldValueStr = value.getOldValueStr();
        oldCondStr = value.getOldCondStr();
    }
}
