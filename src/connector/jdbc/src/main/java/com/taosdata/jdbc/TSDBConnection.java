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

import java.io.*;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.*;
import java.util.concurrent.Executor;

public class TSDBConnection implements Connection {

    private TSDBJNIConnector connector = null;

    protected Properties props = null;

    private String catalog = null;

    private TSDBDatabaseMetaData dbMetaData = null;

    private Properties clientInfoProps = new Properties();

    private int timeoutMilliseconds = 0;

    private String tsCharSet = "";

    public TSDBConnection(Properties info, TSDBDatabaseMetaData meta) throws SQLException {
        this.dbMetaData = meta;
        connect(info.getProperty(TSDBDriver.PROPERTY_KEY_HOST),
                Integer.parseInt(info.getProperty(TSDBDriver.PROPERTY_KEY_PORT, "0")),
                info.getProperty(TSDBDriver.PROPERTY_KEY_DBNAME), info.getProperty(TSDBDriver.PROPERTY_KEY_USER),
                info.getProperty(TSDBDriver.PROPERTY_KEY_PASSWORD));
    }

    private void connect(String host, int port, String dbName, String user, String password) throws SQLException {
        this.connector = new TSDBJNIConnector();
        this.connector.connect(host, port, dbName, user, password);

        try {
            this.setCatalog(dbName);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        this.dbMetaData.setConnection(this);
    }

    public TSDBJNIConnector getConnection() {
        return this.connector;
    }

    public Statement createStatement() throws SQLException {
        if (!this.connector.isClosed()) {
            TSDBStatement statement = new TSDBStatement(this, this.connector);
            statement.setConnection(this);
            return statement;
        } else {
            throw new SQLException(TSDBConstants.FixErrMsg(TSDBConstants.JNI_CONNECTION_NULL));
        }
    }

    public TSDBSubscribe subscribe(String topic, String sql, boolean restart) throws SQLException {
        if (this.connector.isClosed()) {
            throw new SQLException(TSDBConstants.FixErrMsg(TSDBConstants.JNI_CONNECTION_NULL));
        }

        long id = this.connector.subscribe(topic, sql, restart, 0);
        if (id == 0) {
            throw new SQLException(TSDBConstants.WrapErrMsg("failed to create subscription"));
        }

        return new TSDBSubscribe(this.connector, id);
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        if (!this.connector.isClosed()) {
            return new TSDBPreparedStatement(this, this.connector, sql);
        } else {
            throw new SQLException(TSDBConstants.FixErrMsg(TSDBConstants.JNI_CONNECTION_NULL));
        }
    }

    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public String nativeSQL(String sql) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {
    }

    public boolean getAutoCommit() throws SQLException {
        return true;
    }

    public void commit() throws SQLException {
    }

    public void rollback() throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void close() throws SQLException {
        if (this.connector != null && !this.connector.isClosed()) {
            this.connector.closeConnection();
        } else {
            throw new SQLException(TSDBConstants.WrapErrMsg("connection is already closed!"));
        }
    }

    public boolean isClosed() throws SQLException {
        return this.connector.isClosed();
    }

    /**
     * A connection's database is able to provide information describing its tables,
     * its supported SQL grammar, its stored procedures, the capabilities of this
     * connection, etc. This information is made available through a
     * DatabaseMetaData object.
     *
     * @return a DatabaseMetaData object for this connection
     * @throws SQLException if a database access error occurs
     */
    public DatabaseMetaData getMetaData() throws SQLException {
        return this.dbMetaData;
    }

    /**
     * This readOnly option is not supported by TDengine. However, the method is intentionally left blank here to
     * support HikariCP connection.
     *
     * @param readOnly
     * @throws SQLException
     */
    public void setReadOnly(boolean readOnly) throws SQLException {
    }

    public boolean isReadOnly() throws SQLException {
        return true;
    }

    public void setCatalog(String catalog) throws SQLException {
        this.catalog = catalog;
    }

    public String getCatalog() throws SQLException {
        return this.catalog;
    }

    /**
     * The transaction isolation level option is not supported by TDengine.
     * This method is intentionally left empty to support HikariCP connection.
     *
     * @param level
     * @throws SQLException
     */
    public void setTransactionIsolation(int level) throws SQLException {
    }

    /**
     * The transaction isolation level option is not supported by TDengine.
     *
     * @return
     * @throws SQLException
     */
    public int getTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_NONE;
    }

    public SQLWarning getWarnings() throws SQLException {
        //todo: implement getWarnings according to the warning messages returned from TDengine
        return null;
//        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void clearWarnings() throws SQLException {
        // left blank to support HikariCP connection
        //todo: implement clearWarnings according to the warning messages returned from TDengine
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        // This method is implemented in the current way to support Spark
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY) {
            throw new SQLException(TSDBConstants.INVALID_VARIABLES);
        }

        if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
            throw new SQLException(TSDBConstants.INVALID_VARIABLES);
        }

        return this.prepareStatement(sql);
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void setHoldability(int holdability) throws SQLException {
        // intentionally left empty to support druid connection pool.
    }

    /**
     * the transaction is not supported by TDengine, so the opened ResultSet Objects will remain open
     *
     * @return
     * @throws SQLException
     */
    public int getHoldability() throws SQLException {
        //intentionally left empty to support HikariCP connection.
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    public Savepoint setSavepoint() throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        return this.prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public Clob createClob() throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public Blob createBlob() throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public NClob createNClob() throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public SQLXML createSQLXML() throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public boolean isValid(int timeout) throws SQLException {
        return !this.isClosed();
    }

    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        clientInfoProps.setProperty(name, value);
    }

    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        for (Enumeration<Object> enumer = properties.keys(); enumer.hasMoreElements(); ) {
            String name = (String) enumer.nextElement();
            clientInfoProps.put(name, properties.getProperty(name));
        }
    }

    public String getClientInfo(String name) throws SQLException {
        return clientInfoProps.getProperty(name);
    }

    public Properties getClientInfo() throws SQLException {
        return clientInfoProps;
    }

    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void setSchema(String schema) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public String getSchema() throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void abort(Executor executor) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        this.timeoutMilliseconds = milliseconds;
    }

    public int getNetworkTimeout() throws SQLException {
        return this.timeoutMilliseconds;
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }
}
