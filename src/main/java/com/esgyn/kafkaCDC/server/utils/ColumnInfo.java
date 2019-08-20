package com.esgyn.kafkaCDC.server.utils;

import lombok.Getter;
import lombok.Setter;

public class ColumnInfo {
    @Setter
    @Getter
    private int    columnID   = -1;
    @Setter
    @Getter
    private int    columnOff  = -1;
    @Setter
    @Getter
    private int    columnType = -1;
    @Setter
    @Getter
    private String columnSet  = null;
    @Setter
    @Getter
    private String typeName   = null;
    @Setter
    @Getter
    private String columnName = null;

    private int    columnSize = -1;

    public ColumnInfo() { }

    public ColumnInfo(int columnID_, int columnOff_, int columnSize_, 
		      String columnSet_, int columnType_, String typeName_,
		      String columnName_) {
        columnID = columnID_;
        columnOff = columnOff_;
        columnSize = columnSize_;
        columnSet = columnSet_;
        columnType = columnType_;
        typeName = typeName_;
        columnName = "\"" + columnName_ + "\"";
    }

    public int getColumnSize() {
        if (columnSet.indexOf("UCS2") >= 0) {
            return columnSize / 2;
        } else if (columnSet.indexOf("UTF8") >= 0) {
            return columnSize / 4;
        } else {
            return columnSize;
        }
    }
}
