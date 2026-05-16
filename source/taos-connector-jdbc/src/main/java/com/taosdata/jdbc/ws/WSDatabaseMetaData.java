package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.AbstractDatabaseMetaData;

import java.sql.Connection;
import java.sql.SQLException;

public class WSDatabaseMetaData extends AbstractDatabaseMetaData {

    private final String url;
    private final String userName;
    private final Connection connection;

    public WSDatabaseMetaData(String url, String userName, Connection connection) {
        this.url = url;
        this.userName = userName;
        this.connection = connection;
    }

    @Override
    public String getURL() throws SQLException {
        return this.url;
    }

    @Override
    public String getUserName() throws SQLException {
        return this.userName;
    }

    @Override
    public String getDriverName() throws SQLException {
        return WebSocketDriver.class.getName();
    }
    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return true;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.connection;
    }
}
