package com.esgyn.kafkaCDC.server.esgynDB;

import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import java.util.Map;
import java.lang.StringBuffer;

import org.apache.log4j.Logger;

import java.util.Date;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;

import com.esgyn.kafkaCDC.server.utils.TableInfo;
import com.esgyn.kafkaCDC.server.utils.EsgynDBParams;

import lombok.Getter;
import lombok.Setter;

public class EsgynDB {
    @Setter
    @Getter
    private EsgynDBParams  DBParams    = null;
    Connection             sharedConn  = null;

    private static Logger  log         = Logger.getLogger(EsgynDB.class);

    private long           totalMsgNum = 0;
    private long           kafkaMsgNum = 0;
    private long           latestTime  = 0;
    private long           transTotal  = 0;
    private long           transFails  = 0;

    private long           messageNum  = 0;
    private long           insMsgNum   = 0;
    private long           updMsgNum   = 0;
    private long           keyMsgNum   = 0;
    private long           delMsgNum   = 0;

    private long           oldMsgNum   = 0;

    private long           insertNum   = 0;
    private long           updateNum   = 0;
    private long           deleteNum   = 0;

    private long           errInsertNum   = 0;
    private long           errUpdateNum   = 0;
    private long           errDeleteNum   = 0;

    private long           maxSpeed    = 0;

    private long           begin;
    private Date           startTime;

    public EsgynDB(EsgynDBParams DBParams_) {
        if (log.isTraceEnabled()) {
            log.trace("enter function [table: " 
		      + DBParams_.getDefSchema() + "." + DBParams_.getDefTable() 
		      + ", db url: " + DBParams_.getDBUrl()
		      + ", db driver:" + DBParams_.getDBDriver() 
		      + ", db user: " + DBParams_.getDBUser() + "]");
        }

	DBParams  = DBParams_;

        begin     = new Date().getTime();
        startTime = new Date();

        if (log.isTraceEnabled()) {
            log.trace("exit function");
        }
    }

    public Connection CreateConnection(boolean autocommit) {
        Connection dbConn = null;
        if (log.isTraceEnabled()) {
            log.trace("enter function [autocommit: " + autocommit + "]");
        }

        try {
            Class.forName(DBParams.getDBDriver());
            dbConn = DriverManager.getConnection(DBParams.getDBUrl(), DBParams.getDBUser(), 
						 DBParams.getDBPassword());
            dbConn.setAutoCommit(autocommit);
        } catch (SQLException se) {
            log.error("SQLException has occurred when CreateConnection:",se);
        } catch (ClassNotFoundException ce) {
            log.error("driver class not found when CreateConnection:: " ,ce);
            System.exit(1);
        } catch (Exception e) {
            log.error("create connect error when CreateConnection:: " , e);
            System.exit(1);
        }

        if (log.isTraceEnabled()) {
            log.trace("exit function");
        }

        return dbConn;
    }

    public void CloseConnection(Connection dbConn_) {
        if (dbConn_ == null)
            return;

        if (log.isTraceEnabled()) {
            log.trace("enter function [db conn: " + dbConn_ + "]");
        }

        try {
            dbConn_.close();
        } catch (SQLException e) {
            log.error("connection close error.",e);
        }

        if (log.isTraceEnabled()) {
            log.trace("exit function");
        }
    }

    public Connection getSharedConn() {
        return sharedConn;
    }

    public void setSharedConn(Connection sharedConn) {
        this.sharedConn = sharedConn;
    }

    public synchronized void AddInsMsgNum(long insMsgNum_) {
        insMsgNum += insMsgNum_;
        messageNum += insMsgNum_;
    }

    public synchronized void AddUpdMsgNum(long updMsgNum_) {
        updMsgNum += updMsgNum_;
        messageNum += updMsgNum_;
    }

    public synchronized void AddKeyMsgNum(long keyMsgNum_) {
        keyMsgNum += keyMsgNum_;
        messageNum += keyMsgNum_;
    }

    public synchronized void AddDelMsgNum(long delMsgNum_) {
        delMsgNum += delMsgNum_;
        messageNum += delMsgNum_;
    }

    public synchronized void AddInsertNum(long insertNum_) {
        insertNum += insertNum_;
    }

    public synchronized void AddUpdateNum(long updateNum_) {
        updateNum += updateNum_;
    }

    public synchronized void AddDeleteNum(long deleteNum_) {
        deleteNum += deleteNum_;
    }

    public synchronized void AddErrInsertNum(long errInsertNum_) {
        errInsertNum += errInsertNum_;
    }

    public synchronized void AddErrUpdateNum(long errUpdateNum_) {
        errUpdateNum += errUpdateNum_;
    }

    public synchronized void AddErrDeleteNum(long errDeleteNum_) {
        errDeleteNum += errDeleteNum_;
    }

    public synchronized void AddTotalNum(long totalMsgNum_) {
        totalMsgNum += totalMsgNum_;
    }
    public synchronized void AddKafkaPollNum(long kafkaMsgNum_) {
        kafkaMsgNum += kafkaMsgNum_;
    }

    public synchronized void setLatestTime(long latestTime_) {
        if (latestTime_ > latestTime) {
            latestTime = latestTime_;
        }
    }
    public synchronized void AddTransTotal(long transTotal_) {
        transTotal+=transTotal_;
    }
    public synchronized void AddTransFails(long transFails_) {
        transFails += transFails_;
    }

    public void DisplayDatabase(long interval, boolean tablespeed) {
        Long end = new Date().getTime();
        Date endTime = new Date();
        Float useTime = ((float) (end - begin)) / 1000;
        long avgSpeed = (long) (messageNum / useTime);
        long incMessage = (messageNum - oldMsgNum);
        long curSpeed = (long) (incMessage / (interval / 1000));
        if (curSpeed > maxSpeed)
            maxSpeed = curSpeed;
        DecimalFormat df = new DecimalFormat("####0.000");
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        StringBuffer strBuffer = new StringBuffer();
        strBuffer.append("kafkaPoll states:\n  kafkaPollNum ["+kafkaMsgNum+"] latestDataTime ["+sdf.format(latestTime)+"] "
                + "TransactionNum [Total: " + transTotal + ", Success: " + (transTotal-transFails)
                + ", Fails: " + transFails + " ]\n");
        strBuffer.append("Consumer messages [ " + totalMsgNum
                + " processed: " + messageNum + " increased: " + incMessage
                + "], speed(n/s) [max: " + maxSpeed + ", avg: " + avgSpeed + ", cur: " + curSpeed
                + "]\n  Run time        [ " + df.format(useTime) + "s, start: " + sdf.format(startTime)
                + ", cur: " + sdf.format(endTime) + "]\n  KafkaTotalMsgs  [I: " + insMsgNum + ", U: " + updMsgNum
                + ", K: " + keyMsgNum + ", D: " + delMsgNum + "] DMLs [insert: " + insertNum + ", update: " + updateNum
                + ", delete: " + deleteNum + "] Fails [insert: " + errInsertNum + ", update: " + errUpdateNum
                + ", delete: " + errDeleteNum + "]\n");

	Map<String, TableInfo> tableHashMap = DBParams.getTableHashMap();
        for (TableInfo tableInfo : tableHashMap.values()) {
            tableInfo.DisplayStat(strBuffer, interval, tablespeed);
        }
        log.info(strBuffer.toString());

        oldMsgNum = messageNum;
    }
}
