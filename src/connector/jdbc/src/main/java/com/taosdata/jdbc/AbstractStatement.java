package com.taosdata.jdbc;

import java.sql.*;

public abstract class AbstractStatement implements Statement {

    private volatile boolean closeOnCompletion;
    private int fetchSize;

    @Override
    public abstract ResultSet executeQuery(String sql) throws SQLException;

    @Override
    public abstract int executeUpdate(String sql) throws SQLException;

    @Override
    public abstract void close() throws SQLException;

    @Override
    public int getMaxFieldSize() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        return TSDBConstants.maxFieldSize;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        if (max < 0)
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE);
        // nothing to do
    }

    @Override
    public int getMaxRows() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        return 0;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        if (max < 0)
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE);
        // nothing to do
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        return 0;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        if (seconds < 0)
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE);
    }

    @Override
    public void cancel() throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        // nothing to do
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public abstract boolean execute(String sql) throws SQLException;

    @Override
    public abstract ResultSet getResultSet() throws SQLException;

    @Override
    public abstract int getUpdateCount() throws SQLException;

    @Override
    public boolean getMoreResults() throws SQLException {
        return getMoreResults(CLOSE_CURRENT_RESULT);
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        //nothing to do
    }

    @Override
    public int getFetchDirection() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        if (rows < 0)
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE);
        //nothing to do
        this.fetchSize = rows;
    }

    @Override
    public int getFetchSize() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        return this.fetchSize;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public abstract void addBatch(String sql) throws SQLException;

    @Override
    public abstract void clearBatch() throws SQLException;

    @Override
    public abstract int[] executeBatch() throws SQLException;

    @Override
    public abstract Connection getConnection() throws SQLException;

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORTED_METHOD_EXCEPTION_MSG);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public abstract boolean isClosed() throws SQLException;

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        //nothing to do
    }

    @Override
    public boolean isPoolable() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        this.closeOnCompletion = true;
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        return this.closeOnCompletion;
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
}
