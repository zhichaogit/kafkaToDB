package com.esgyn.kafkaCDC.server.esgynDB;

public class ColumnValue {
    int    columnID     = 0;
    String curValue     = null;
    String curValueStr  = null;
    String oldValue     = null;
    String oldValueStr  = null;
    String nullValueStr = "null";
    String oldCondStr   = " is null";

    public ColumnValue(int columnID_, String curValue_, String oldValue_) {
        columnID = columnID_;

        if (curValue_ != null) {
            curValue = curValue_.replace("\"", "").replace("\'", "");
            curValueStr = "\'" + curValue + "\'";
        } else {
            curValueStr = nullValueStr;
        }

        if (oldValue_ != null) {
            oldValue = oldValue_.replace("\"", "").replace("\'", "");
            oldValueStr = "\'" + oldValue + "\'";
            oldCondStr = " = " + oldValueStr;
        } else {
            oldValueStr = nullValueStr;
        }
    }

    public ColumnValue(ColumnValue value) {
        columnID = value.GetColumnID();
        curValue = value.GetCurValue();
        oldValue = value.GetOldValue();
        curValueStr = value.GetCurValueStr();
        oldValueStr = value.GetOldValueStr();
        oldCondStr = value.GetOldCondStr();
    }

    public int GetColumnID() {
        return columnID;
    }

    public boolean CurValueIsNull() {
        return curValue == null;
    }

    public boolean OldValueIsNull() {
        return oldValue == null;
    }

    public String GetCurValue() {
        return curValue;
    }

    public String GetOldValue() {
        return oldValue;
    }

    public String GetCurValueStr() {
        return curValueStr;
    }

    public String GetOldValueStr() {
        return oldValueStr;
    }

    public String GetOldCondStr() {
        return oldCondStr;
    }
}
