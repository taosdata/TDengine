package com.taosdata.jdbc.rs;

import com.taosdata.jdbc.AbstractDatabaseMetaData;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @deprecated Use WebSocket connection instead.
 */
@Deprecated
public class RestfulDatabaseMetaData extends AbstractDatabaseMetaData {

    private final String url;
    private final String userName;
    private final Connection connection;

    public RestfulDatabaseMetaData(String url, String userName, Connection connection) {
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
        return RestfulDriver.class.getName();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.connection;
    }
}
