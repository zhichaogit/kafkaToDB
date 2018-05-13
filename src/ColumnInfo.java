public class ColumnInfo
{
    int         columnID   = -1;
    int         columnType = -1;
    String      typeName   = null;
    String      columnName = null;

    public ColumnInfo(String columnID_,
		      String columnType_,
		      String typeName_,
		      String columnName_) 
    {
	columnID   = Integer.parseInt(columnID_);
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
}

