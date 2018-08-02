package com.esgyn.kafkaCDC.server.esgynDB;

public class ColumnInfo {
    int         columnID   = -1;
    int         columnOff  = -1;
    int         columnType = -1;
    int         columnSize = -1;
    String      columnSet  = null;
    String      typeName   = null;
    String      columnName = null;

    public ColumnInfo(int    columnID_,
		      int    columnOff_,
		      int    columnSize_,
		      String columnSet_,
		      String columnType_,
		      String typeName_,
		      String columnName_) 
    {
	columnID   = columnID_;
	columnOff  = columnOff_;
	columnSize = columnSize_;
	columnSet  = columnSet_;
	columnType = Integer.parseInt(columnType_);
	typeName   = typeName_;
	columnName = "\"" + columnName_ + "\"";
    }

    public String GetColumnName()
    {
	return columnName;
    }

    public String GetTypeName()
    {
	return typeName;
    }

    public int GetColumnType()
    {
	return columnType;
    }

    public int GetColumnSize()
    {
	if (columnSet.indexOf("UCS2") >= 0) {
	    return columnSize/2;
	} else if (columnSet.indexOf("UTF8") >= 0) {
	    return columnSize/4;
	} else {
	    return columnSize;
	}
    }

    public int GetColumnID()
    {
	return columnID;
    }

    public int GetColumnOff()
    {
	return columnOff;
    }
}
