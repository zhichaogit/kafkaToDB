public class ColumnInfo
{
    String      columnName = null;
    String      typeName   = null;
    int         columnType = -1;

    public ColumnInfo(String columnName_,
		      String typeName_,
		      String columnType_) 
    {
	columnName = columnName_;
	typeName   = typeName_;
	columnType = Integer.parseInt(columnType_);
    }

    public String GetColunmName()
    {
	return columnName;
    }

    public String GetTypeName()
    {
	return typeName;
    }

    public int GetColunmType()
    {
	return columnType;
    }
}

