package com.taosdata.jdbc;

import java.sql.*;

public class AbstractStatementWrapper extends AbstractStatement{
    protected Statement statement;

    public AbstractStatementWrapper(Statement statement) {
        this.statement = statement;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        return statement.executeQuery(sql);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        return statement.executeUpdate(sql);
    }

    @Override
    public void close() throws SQLException {
        statement.close();
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        return statement.execute(sql);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return statement.getResultSet();
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return statement.getUpdateCount();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return statement.getConnection();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return statement.isClosed();
    }
}
