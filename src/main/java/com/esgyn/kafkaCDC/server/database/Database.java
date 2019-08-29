package com.esgyn.kafkaCDC.server.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.esgyn.kafkaCDC.server.utils.DatabaseParams;

import lombok.Getter;
import lombok.Setter;

public class Database {
    @Setter
    @Getter
    private DatabaseParams database    = null;

    private static Logger  log         = Logger.getLogger(Database.class);

    public Database(DatabaseParams database_) {
        if (log.isTraceEnabled()) {
            log.trace("enter [table: " 
		      + database_.getDefSchema() + "." + database_.getDefTable() 
		      + ", db url: " + database_.getDBUrl()
		      + ", db driver:" + database_.getDBDriver() 
		      + ", db user: " + database_.getDBUser() + "]");
        }

	database  = database_;

        if (log.isTraceEnabled()) { log.trace("exit"); }
    }

    public Connection CreateConnection(boolean autocommit) {
        Connection dbConn = null;
        if (log.isTraceEnabled()) {
            log.trace("enter [autocommit: " + autocommit + "]");
        }

        try {
            Class.forName(database.getDBDriver());
            dbConn = DriverManager.getConnection(database.getDBUrl(), database.getDBUser(), 
						 database.getDBPassword());
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

        if (log.isTraceEnabled()) { log.trace("exit"); }

        return dbConn;
    }

    public void CloseConnection(Connection dbConn_) {
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
}
