public class ColumnValue
{
    int         columnID  = 0;
    String      curValue  = null;
    String      oldValue  = null;
    String      nullValue = "null";
    String      oldCondValue = " is null";

    public ColumnValue(int columnID_, String curValue_, String oldValue_)
    {
	columnID = columnID_;

	if (curValue_ != null) {
	    curValue = "\'" + curValue_.replace("\"","").replace("\'","") + "\'";
	} else {
	    curValue = nullValue;
	}

	if (oldValue_ != null) {
	    oldValue = "\'" + oldValue_.replace("\"","").replace("\'","") + "\'";
	    oldCondValue = " = " + oldValue;
	} else {
	    oldValue = nullValue;
	}
    }

    public int GetColumnID()
    {
	return columnID;
    }

    public boolean CurValueIsNull()
    {
	return curValue == null;
    }

    public boolean OldValueIsNull()
    {
	return oldValue == null;
    }

    public String GetCurValue()
    {
	return curValue;
    }

    public String GetOldValue()
    {
	return curValue;
    }
    
    public String GetOldCondValue()
    {
	return oldCondValue;
    }
}

