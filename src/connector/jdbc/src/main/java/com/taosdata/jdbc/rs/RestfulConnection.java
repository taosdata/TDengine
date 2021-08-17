package com.taosdata.jdbc.rs;

import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class RestfulConnection extends AbstractConnection {

    private final String host;
    private final int port;
    private final String url;
    private final String database;
    private final String token;
    /******************************************************/
    private boolean isClosed;
    private final DatabaseMetaData metadata;

    public RestfulConnection(String host, String port, Properties props, String database, String url, String token) {
        super(props);
        this.host = host;
        this.port = Integer.parseInt(port);
        this.database = database;
        this.url = url;
        this.token = token;
        this.metadata = new RestfulDatabaseMetaData(url, props.getProperty(TSDBDriver.PROPERTY_KEY_USER), this);
    }

    @Override
    public Statement createStatement() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        return new RestfulStatement(this, database);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        return new RestfulPreparedStatement(this, database, sql);
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
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        return this.metadata;
    }

    // getters
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

    public String getToken() {
        return token;
    }
}