package com.taosdata.jdbc;

import java.sql.*;
import java.util.Properties;

public class TSDBConnection extends AbstractConnection {

    private TSDBJNIConnector connector;
    private final TSDBDatabaseMetaData databaseMetaData;
    private boolean batchFetch;

    public Boolean getBatchFetch() {
        return this.batchFetch;
    }

    public TSDBConnection(Properties info, TSDBDatabaseMetaData meta) throws SQLException {
        super(info);
        this.databaseMetaData = meta;
        connect(info.getProperty(TSDBDriver.PROPERTY_KEY_HOST),
                Integer.parseInt(info.getProperty(TSDBDriver.PROPERTY_KEY_PORT, "0")),
                info.getProperty(TSDBDriver.PROPERTY_KEY_DBNAME),
                info.getProperty(TSDBDriver.PROPERTY_KEY_USER),
                info.getProperty(TSDBDriver.PROPERTY_KEY_PASSWORD));

        String batchLoad = info.getProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD);
        if (batchLoad != null) {
            this.batchFetch = Boolean.parseBoolean(batchLoad);
        }
    }

    private void connect(String host, int port, String dbName, String user, String password) throws SQLException {
        this.connector = new TSDBJNIConnector();
        this.connector.connect(host, port, dbName, user, password);
        this.catalog = dbName;
        this.databaseMetaData.setConnection(this);
    }

    public TSDBJNIConnector getConnector() {
        return this.connector;
    }

    public Statement createStatement() throws SQLException {
        if (isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        }

        return new TSDBStatement(this);
    }

    public TSDBSubscribe subscribe(String topic, String sql, boolean restart) throws SQLException {
        if (isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        }

        long id = this.connector.subscribe(topic, sql, restart);
        if (id == 0) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_SUBSCRIBE_FAILED);
        }
        return new TSDBSubscribe(this.connector, id);
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        if (isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        }
        
        return new TSDBPreparedStatement(this, sql);
    }

    public void close() throws SQLException {
        if (isClosed) {
            return;
        }
        
        this.connector.closeConnection();
        this.isClosed = true;
    }

    public boolean isClosed() throws SQLException {
        return this.connector != null && this.connector.isClosed();
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        if (isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        }
        return this.databaseMetaData;
    }

}