public class ColumnValue
{
    int         columnID  = 0;
    String      curValue  = null;
    String      oldValue  = null;

    public ColumnValue(int columnID_, String curValue_, String oldValue_)
    {
	columnID = columnID_;

	if (curValue_ != null)
	    curValue = curValue_.replace("\"","").replace("\'","");
	if (oldValue_ != null)
	    oldValue = oldValue_.replace("\"","").replace("\'","");
    }

    public int GetColumnID()
    {
	return columnID;
    }

    public String GetCurValue()
    {
	return curValue;
    }

    public String GetOldValue()
    {
	return oldValue;
    }
}

