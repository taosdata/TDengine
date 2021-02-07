package com.taosdata.jdbc.rs;

import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.TSDBDriver;

import java.sql.*;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class RestfulConnection implements Connection {

    private static final String CONNECTION_IS_CLOSED = "connection is closed.";
    private static final String AUTO_COMMIT_IS_TRUE = "auto commit is true";
    private final String host;
    private final int port;
    private final Properties props;
    private volatile String database;
    private final String url;
    /******************************************************/
    private boolean isClosed;
    private DatabaseMetaData metadata;
    private Map<String, Class<?>> typeMap;
    private Properties clientInfoProps = new Properties();

    public RestfulConnection(String host, String port, Properties props, String database, String url) {
        this.host = host;
        this.port = Integer.parseInt(port);
        this.props = props;
        this.database = database;
        this.url = url;
        this.metadata = new RestfulDatabaseMetaData(url, props.getProperty(TSDBDriver.PROPERTY_KEY_USER), this);
    }

    @Override
    public Statement createStatement() throws SQLException {
        if (isClosed())
            throw new SQLException(CONNECTION_IS_CLOSED);

        return new RestfulStatement(this, database);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        if (isClosed())
            throw new SQLException(CONNECTION_IS_CLOSED);
        //TODO: prepareStatement
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        if (isClosed())
            throw new SQLException(CONNECTION_IS_CLOSED);

        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        if (isClosed())
            throw new SQLException(CONNECTION_IS_CLOSED);

        //nothing did
        return sql;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if (isClosed())
            throw new SQLException(CONNECTION_IS_CLOSED);
        if (!autoCommit)
            throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        if (isClosed())
            throw new SQLException(CONNECTION_IS_CLOSED);
        return true;
    }

    @Override
    public void commit() throws SQLException {
        if (isClosed())
            throw new SQLException(CONNECTION_IS_CLOSED);
        if (getAutoCommit())
            throw new SQLException(AUTO_COMMIT_IS_TRUE);
        //nothing to do
    }

    @Override
    public void rollback() throws SQLException {
        if (isClosed())
            throw new SQLException(CONNECTION_IS_CLOSED);
        if (getAutoCommit())
            throw new SQLException(AUTO_COMMIT_IS_TRUE);
        //nothing to do
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
            throw new SQLException(CONNECTION_IS_CLOSED);

        return this.metadata;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        if (isClosed())
            throw new SQLException(CONNECTION_IS_CLOSED);
        // nothing to do
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        if (isClosed())
            throw new SQLException(CONNECTION_IS_CLOSED);
        return true;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        if (isClosed())
            throw new SQLException(CONNECTION_IS_CLOSED);
        synchronized (RestfulConnection.class) {
            this.database = catalog;
        }
    }

    @Override
    public String getCatalog() throws SQLException {
        if (isClosed())
            throw new SQLException(CONNECTION_IS_CLOSED);
        return this.database;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        if (isClosed())
            throw new SQLException(CONNECTION_IS_CLOSED);
        switch (level) {
            case Connection.TRANSACTION_NONE:
                break;
            case Connection.TRANSACTION_READ_UNCOMMITTED:
            case Connection.TRANSACTION_READ_COMMITTED:
            case Connection.TRANSACTION_REPEATABLE_READ:
            case Connection.TRANSACTION_SERIALIZABLE:
                throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
            default:
                throw new SQLException(TSDBConstants.INVALID_VARIABLES);
        }
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        if (isClosed())
            throw new SQLException(CONNECTION_IS_CLOSED);
        //Connection.TRANSACTION_NONE specifies that transactions are not supported.
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        if (isClosed())
            throw new SQLException(CONNECTION_IS_CLOSED);

        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        if (isClosed())
            throw new SQLException(CONNECTION_IS_CLOSED);
        //nothing to do
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        if (isClosed())
            throw new SQLException(CONNECTION_IS_CLOSED);

        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY) {
            throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
        }
        if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY)
            throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        if (isClosed())
            throw new SQLException(CONNECTION_IS_CLOSED);
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY || resultSetConcurrency != ResultSet.CONCUR_READ_ONLY)
            throw new SQLFeatureNotSupportedException(TSDBConstants.INVALID_VARIABLES);

        return this.prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        if (isClosed())
            throw new SQLException(CONNECTION_IS_CLOSED);
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY || resultSetConcurrency != ResultSet.CONCUR_READ_ONLY)
            throw new SQLFeatureNotSupportedException(TSDBConstants.INVALID_VARIABLES);

        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        if (isClosed())
            throw new SQLException(CONNECTION_IS_CLOSED);

        synchronized (RestfulConnection.class) {
            if (this.typeMap == null) {
                this.typeMap = new HashMap<>();
            }
            return this.typeMap;
        }
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        if (isClosed())
            throw new SQLException(CONNECTION_IS_CLOSED);

        synchronized (RestfulConnection.class) {
            this.typeMap = map;
        }
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        if (isClosed())
            throw new SQLException(CONNECTION_IS_CLOSED);
        if (holdability != ResultSet.HOLD_CURSORS_OVER_COMMIT)
            throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public int getHoldability() throws SQLException {
        if (isClosed())
            throw new SQLException(CONNECTION_IS_CLOSED);
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        if (isClosed())
            throw new SQLException(CONNECTION_IS_CLOSED);
        if (getAutoCommit())
            throw new SQLException(TSDBConstants.INVALID_VARIABLES);
        //nothing to do
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        if (isClosed())
            throw new SQLException(CONNECTION_IS_CLOSED);
        if (getAutoCommit())
            throw new SQLException(TSDBConstants.INVALID_VARIABLES);
        //nothing to do
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        if (isClosed())
            throw new SQLException(CONNECTION_IS_CLOSED);
        if (getAutoCommit())
            throw new SQLException(TSDBConstants.INVALID_VARIABLES);
        //nothing to do
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        if (isClosed())
            throw new SQLException(CONNECTION_IS_CLOSED);
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        if (resultSetHoldability != ResultSet.HOLD_CURSORS_OVER_COMMIT)
            throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
        return createStatement(resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        if (resultSetHoldability != ResultSet.HOLD_CURSORS_OVER_COMMIT)
            throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
        return prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (timeout < 0)
            throw new SQLException(TSDBConstants.INVALID_VARIABLES);
        // TODO:
        /* The driver shall submit a query on the connection or use some other mechanism that positively verifies
         the connection is still valid when this method is called.*/
        return !isClosed();
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        if (isClosed)
            throw new SQLClientInfoException();
        clientInfoProps.setProperty(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        if (isClosed)
            throw new SQLClientInfoException();

        for (Enumeration<Object> enumer = properties.keys(); enumer.hasMoreElements(); ) {
            String name = (String) enumer.nextElement();
            clientInfoProps.put(name, properties.getProperty(name));
        }
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        if (isClosed)
            throw new SQLClientInfoException();

        return clientInfoProps.getProperty(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        if (isClosed)
            throw new SQLClientInfoException();

        return clientInfoProps;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        if (isClosed())
            throw new SQLException(CONNECTION_IS_CLOSED);
        synchronized (RestfulConnection.class) {
            this.database = schema;
        }
    }

    @Override
    public String getSchema() throws SQLException {
        if (isClosed())
            throw new SQLException(CONNECTION_IS_CLOSED);
        return this.database;
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        if (executor == null) {
            throw new SQLException("Executor can not be null");
        }

        executor.execute(() -> {
            try {
                close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        if (isClosed())
            throw new SQLException(CONNECTION_IS_CLOSED);
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        if (isClosed())
            throw new SQLException(CONNECTION_IS_CLOSED);
        return 0;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            return iface.cast(this);
        } catch (ClassCastException cce) {
            throw new SQLException("Unable to unwrap to " + iface.toString());
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public Properties getProps() {
        return props;
    }

    public String getDatabase() {
        return database;
    }

    public String getUrl() {
        return url;
    }
}
