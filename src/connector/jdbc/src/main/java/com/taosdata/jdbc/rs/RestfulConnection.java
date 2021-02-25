package com.taosdata.jdbc.rs;

import com.taosdata.jdbc.*;

import java.sql.*;
import java.util.Properties;

public class RestfulConnection extends AbstractConnection {

    private final String host;
    private final int port;
    private final String url;
    private volatile String database;
    /******************************************************/
    private boolean isClosed;
    private final DatabaseMetaData metadata;

    public RestfulConnection(String host, String port, Properties props, String database, String url) {
        this.host = host;
        this.port = Integer.parseInt(port);
        this.database = database;
        this.url = url;
        this.metadata = new RestfulDatabaseMetaData(url, props.getProperty(TSDBDriver.PROPERTY_KEY_USER), this);
    }

    @Override
    public Statement createStatement() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);;

        return new RestfulStatement(this, database);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);;
        //TODO: prepareStatement
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void close() throws SQLException {
        if (isClosed)
            return;
        //TODO: release all resources
        isClosed = true;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);;

        return this.metadata;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getDatabase() {
        return database;
    }

    public String getUrl() {
        return url;
    }
}