/***************************************************************************
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 *****************************************************************************/
package com.taosdata.jdbc;

import java.sql.*;
import java.util.Properties;

public class TSDBConnection extends AbstractConnection {

    private TSDBJNIConnector connector;
    private TSDBDatabaseMetaData databaseMetaData;
    private boolean batchFetch;

    public Boolean getBatchFetch() {
        return this.batchFetch;
    }

    public void setBatchFetch(Boolean batchFetch) {
        this.batchFetch = batchFetch;
    }

    public TSDBConnection(Properties info, TSDBDatabaseMetaData meta) throws SQLException {
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

    public TSDBJNIConnector getConnection() {
        return this.connector;
    }

    public Statement createStatement() throws SQLException {
        if (isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        }

        TSDBStatement statement = new TSDBStatement(this, this.connector);
        statement.setConnection(this);
        return statement;
    }

    public TSDBSubscribe subscribe(String topic, String sql, boolean restart) throws SQLException {
        if (isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        }

        long id = this.connector.subscribe(topic, sql, restart, 0);
        if (id == 0) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_SUBSCRIBE_FAILED);
        }
        return new TSDBSubscribe(this.connector, id);
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        if (isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        }

        return new TSDBPreparedStatement(this, this.connector, sql);
    }

    public void close() throws SQLException {
        if (isClosed)
            return;
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

    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        if (isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        }
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

}
