public class ColumnInfo
{
    int         columnID   = -1;
    int         columnOff  = -1;
    int         columnType = -1;
    String      typeName   = null;
    String      columnName = null;

    public ColumnInfo(int    columnID_,
		      int    columnOff_,
		      String columnType_,
		      String typeName_,
		      String columnName_) 
    {
	columnID   = columnID_;
	columnOff  = columnOff_;
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

    public int GetColumnID()
    {
	return columnID;
    }

    public int GetColumnOff()
    {
	return columnOff;
    }
}

