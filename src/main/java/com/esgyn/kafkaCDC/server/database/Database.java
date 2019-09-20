package com.esgyn.kafkaCDC.server.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.esgyn.kafkaCDC.server.utils.DatabaseParams;

import lombok.Getter;
import lombok.Setter;

public class Database {
    private static Logger log = Logger.getLogger(Database.class);

    public static Connection CreateConnection(DatabaseParams database_) {
        if (log.isTraceEnabled()) { log.trace("enter"); }

        Connection dbConn = null;
        try {
            Class.forName(database_.getDBDriver());
            dbConn = DriverManager.getConnection(database_.getDBUrl(), database_.getDBUser(), 
						 database_.getDBPassword());
            dbConn.setAutoCommit(false);
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
	 */
	switch (se.getErrorCode()) {
	case -19002:
	case -29154:
	case -8734:
	case -8738:
	    return true;

	default:
	    return false;
	}
    }
}