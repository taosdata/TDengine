package com.zddt.common;

import java.sql.*;


public class TDConnection {
    private String user;
    private String password;
    private String host;
    private Connection connection;
    private Statement statement;
    private int affectRows;
    private String errMsg = "";
    private int errCode = 0;

    public TDConnection(String host, String user, String password) {
        this.host = host;
        this.user = user;
        this.password = password;
    }

    public boolean connect() {
        this.connection = TDConnectionFactory.getConnection(host, user, password);
        if (this.connection == null) {
            return false;
        }

        try {
            this.statement = this.connection.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
            this.errCode = e.getErrorCode();
            this.errMsg = e.getMessage();
            TDLog.error(String.format("failed to create statement, code:%d, error:%s", e.getErrorCode(), e.getMessage()));
            return false;
        }

        return true;
    }

    public void close() {
        try {
            this.statement.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            this.connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public boolean executeUpdate(String sql) {
        this.affectRows = 0;
        for (int i = 0; i < 5; ++i) {
            try {
                this.affectRows = statement.executeUpdate(sql);
            } catch (SQLException e) {
                e.printStackTrace();
                this.errCode = e.getErrorCode();
                this.errMsg = e.getMessage();
                continue;
            }
            return true;
        }
        return false;
    }

    public int executeQueryCount(String sql) {
        Statement stmt;
        ResultSet resSet = null;
        try {
            stmt = connection.createStatement();
            resSet = stmt.executeQuery(sql);
            if (resSet == null) {
                TDLog.error(String.format("failed to execute sql:%s", sql));
                return 0;
            }

            ResultSetMetaData metaData = resSet.getMetaData();
            if (metaData.getColumnCount() != 1) {
                TDLog.error(String.format("invalid resultset, sql:%s", sql));
                return 0;
            }

            while (resSet.next()) {
                return 1;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            this.errCode = e.getErrorCode();
            this.errMsg = e.getMessage();
            TDLog.error(String.format("query failed, sql:%s, code:%d, error:%s", sql, e.getErrorCode(), e.getErrorCode()));
        } finally {
            try {
                resSet.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return 0;
    }

    public int getErrorCode() {
        return this.errCode;
    }

    public String getErrorStr() {
        return this.errMsg;
    }

    public int getAffectrows() {
        return this.affectRows;
    }
}
