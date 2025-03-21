package com.taos.example.highvolume;

import java.sql.*;

/**
 * Prepare target database.
 * Count total records in database periodically so that we can estimate the writing speed.
 */
public class DataBaseMonitor {
    private Connection conn;
    private Statement stmt;
    private String dbName;

    public DataBaseMonitor init(String dbName) throws SQLException {
        if (conn == null) {
            conn = Util.getConnection();
            stmt = conn.createStatement();
        }
        this.dbName = dbName;
        return this;
    }

    public void close() {
        try {
            stmt.close();
        } catch (SQLException e) {
        }
        try {
            conn.close();
        } catch (SQLException e) {
        }
    }

    public void prepareDatabase() throws SQLException {
        stmt.execute("DROP DATABASE IF EXISTS " + dbName);
        stmt.execute("CREATE DATABASE IF NOT EXISTS " + dbName + " vgroups 20");
        stmt.execute("use " + dbName);
        stmt.execute("CREATE STABLE " + dbName + ".meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(64))");
    }

    public long count() throws SQLException {
        try (ResultSet result = stmt.executeQuery("SELECT count(*) from " + dbName +".meters")) {
            result.next();
            return result.getLong(1);
        }
    }
}