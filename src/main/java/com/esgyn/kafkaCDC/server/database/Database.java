package com.esgyn.kafkaCDC.server.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

import com.esgyn.kafkaCDC.server.utils.DatabaseParams;
import com.esgyn.kafkaCDC.server.utils.Utils;

public class Database {
    private static Logger log    = Logger.getLogger(Database.class);
    private static String CQDSQL = "cqd pcode_opt_level 'OFF';";

    public static Connection CreateConnection(DatabaseParams database_) {
        if (log.isTraceEnabled()) { log.trace("enter"); }
        if (log.isDebugEnabled()) {
            log.debug("dburl[" + database_.getDBUrl() + "],dbuser[" + database_.getDBUser()
            + "],DBPassword[" + database_.getDBPW() + "]");
        }

        Connection dbConn     = null;
        String     DBPW       = null;
        try {
            Class.forName(database_.getDBDriver());
            DBPW = Utils.getDecodePW(database_.getDBPW());
            dbConn = DriverManager.getConnection(database_.getDBUrl(), database_.getDBUser(), 
                                                 DBPW);
            dbConn.setAutoCommit(false);
            Statement cqdStmt = dbConn.createStatement();
            cqdStmt.execute(CQDSQL);
            dbConn.commit();
            cqdStmt.close();
        } catch (SQLException se) {
            log.error("SQLException has occurred when CreateConnection:", se);
	    dbConn = null;
        } catch (ClassNotFoundException ce) {
            log.error("driver class not found when CreateConnection:", ce);
            System.exit(1);
        } catch (Exception e) {
            log.error("create connect error when CreateConnection:", e);
            System.exit(1);
        }

        if (log.isTraceEnabled()) { log.trace("exit"); }

        return dbConn;
    }

    public static void CloseConnection(Connection dbConn_) {
        if (dbConn_ == null)
            return;

        if (log.isTraceEnabled()) {
            log.trace("enter [db conn: " + dbConn_ + "]");
        }

        try {
            dbConn_.close();
        } catch (SQLException e) {
            log.error("connection close error.",e);
        }

        if (log.isTraceEnabled()) { log.trace("exit"); }
    }

    public static boolean isAccepableSQLExpection(SQLException se) {
	/*
	 * make sure it's not Connection does not exist(-29002) Exception 
	 * && Timeout expired(-29154) Exception
	 * ERROR[8734] Statement must be recompiled to allow privileges to be re-evaluated
	 * ERROR[8738] Statement must be recompiled due to redefinition of the object(s) accessed
	 * ERROR[8448] UNABLE TO ACCESS Hbase interface. HBASE_ACCESS_ERROR(-706)
	 * ERROR[2105] this query could not be compiled because of incompatible control query
	 * shape(CQS)specifications.
	 */
	switch (se.getErrorCode()) {
	case -19002:
	case -29154:
	case -8734:
	case -8738:
	case -8448:
	case -2105:
	    return true;

	default:
	    return false;
	}
    }
}
