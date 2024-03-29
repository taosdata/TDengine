package com.taos.example.highvolume;

import java.sql.*;

/**
 * Prepare target database.
 * Count total records in database periodically so that we can estimate the writing speed.
 */
public class DataBaseMonitor {
    private Connection conn;
    private Statement stmt;

    public DataBaseMonitor init() throws SQLException {
        if (conn == null) {
            String jdbcURL = System.getenv("TDENGINE_JDBC_URL");
            conn = DriverManager.getConnection(jdbcURL);
            stmt = conn.createStatement();
        }
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
        stmt.execute("DROP DATABASE IF EXISTS test");
        stmt.execute("CREATE DATABASE test");
        stmt.execute("CREATE STABLE test.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)");
    }

    public long count() throws SQLException {
        try (ResultSet result = stmt.executeQuery("SELECT count(*) from test.meters")) {
            result.next();
            return result.getLong(1);
        }
    }

    public long getTableCount() throws SQLException {
        try (ResultSet result = stmt.executeQuery("select count(*) from information_schema.ins_tables where db_name = 'test';")) {
            result.next();
            return result.getLong(1);
        }
    }
}