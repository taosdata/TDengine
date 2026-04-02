package com.taosdata.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.concurrent.*;

public class TSDBStatement extends AbstractStatement {
    /**
     * Status of current statement
     */
    private boolean isClosed;
    private TSDBConnection connection;
    private TSDBResultSet resultSet;

    private int queryTimeout;

    TSDBStatement(TSDBConnection connection, Long instanceId) {
        this.connection = connection;
        this.instanceId = instanceId;
        connection.registerStatement(this.instanceId, this);
    }

    public ResultSet executeQuery(String sql) throws SQLException {
        return executeQuery(sql, null);
    }

    public ResultSet executeQuery(String sql, Long reqId) throws SQLException {
        if (queryTimeout > 0) {
            ExecutorService executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<>(1), Executors.defaultThreadFactory(), new ThreadPoolExecutor.CallerRunsPolicy());
            Future<ResultSet> f = executor.submit(() -> executeQueryImpl(sql, reqId));

            try {
                return f.get(this.queryTimeout, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SQLException("failed to execute sql: " + sql + ", cause: " + e.getMessage(), e);
            } catch (TimeoutException | ExecutionException e) {
                f.cancel(true);
                throw new SQLTimeoutException("failed to execute sql: " + sql + ", cause: the execution time exceeds timeout: " + this.queryTimeout + " seconds");
            } finally {
                executor.shutdownNow();
            }
        } else {
            return executeQueryImpl(sql, reqId);
        }
    }

    private ResultSet executeQueryImpl(String sql, Long reqId) throws SQLException {
        synchronized (this) {
            if (isClosed()) {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
            }
            if (this.resultSet != null && !this.resultSet.isClosed())
                this.resultSet.close();
            //TODO:
            // this is an unreasonable implementation, if the paratemer is a insert statement,
            // the JNI connector will execute the sql at first and return a pointer: pSql,
            // we use this pSql and invoke the isUpdateQuery(long pSql) method to decide .
            // but the insert sql is already executed in database.
            //execute query
            long pSql = this.connection.getConnector().executeQuery(sql, reqId);
            // if pSql is create/insert/update/delete/alter SQL
            if (this.connection.getConnector().isUpdateQuery(pSql)) {
                this.connection.getConnector().freeResultSet(pSql);
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_WITH_EXECUTEQUERY);
            }
            int timestampPrecision = this.connection.getConnector().getResultTimePrecision(pSql);
            resultSet = new TSDBResultSet(this, this.connection.getConnector(), pSql, timestampPrecision);
            return resultSet;
        }
    }

    public int executeUpdate(String sql) throws SQLException {
        return executeUpdate(sql, (Long) null);
    }

    public int executeUpdate(String sql, Long reqId) throws SQLException {
        if (queryTimeout > 0) {
            ExecutorService executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<>(1), Executors.defaultThreadFactory(), new ThreadPoolExecutor.CallerRunsPolicy());

            Future<Integer> f = executor.submit(() -> executeUpdateImpl(sql, reqId));

            try {
                return f.get(this.queryTimeout, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SQLException("failed to execute sql: " + sql + ", cause: " + e.getMessage(), e);
            } catch (TimeoutException | ExecutionException e) {
                f.cancel(true);
                throw new SQLTimeoutException("failed to execute sql: " + sql + ", cause: the execution time exceeds timeout: " + this.queryTimeout + " seconds");
            } finally {
                executor.shutdownNow();
            }
        } else {
            return executeUpdateImpl(sql, reqId);
        }
    }

    private int executeUpdateImpl(String sql, Long reqId) throws SQLException {

        synchronized (this) {
            if (isClosed())
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
            if (this.resultSet != null && !this.resultSet.isClosed())
                this.resultSet.close();

            long pSql = this.connection.getConnector().executeQuery(sql, reqId);
            // if pSql is create/insert/update/delete/alter SQL
            if (!this.connection.getConnector().isUpdateQuery(pSql)) {
                this.connection.getConnector().freeResultSet(pSql);
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_WITH_EXECUTEUPDATE);
            }
            int affectedRows = this.connection.getConnector().getAffectedRows(pSql);
            this.connection.getConnector().freeResultSet(pSql);
            return affectedRows;
        }
    }

    @Override
    public void setQueryTimeout(int queryTimeout) throws SQLException {
        super.setQueryTimeout(queryTimeout);
        this.queryTimeout = queryTimeout;
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        super.getQueryTimeout();
        return this.queryTimeout;
    }

    public void close() throws SQLException {
        if (isClosed)
            return;
        connection.unregisterStatement(this.instanceId);
        if (this.resultSet != null && !this.resultSet.isClosed())
            this.resultSet.close();
        isClosed = true;
    }

    public boolean execute(String sql) throws SQLException {
        return execute(sql, (Long) null);
    }

    public boolean execute(String sql, Long reqId) throws SQLException {
        if (queryTimeout > 0) {
            ExecutorService executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<>(1), Executors.defaultThreadFactory(), new ThreadPoolExecutor.CallerRunsPolicy());

            Future<Boolean> f = executor.submit(() -> executeImpl(sql, reqId));

            try {
                return f.get(this.queryTimeout, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SQLException("failed to execute sql: " + sql + ", cause: " + e.getMessage(), e);
            } catch (TimeoutException | ExecutionException e) {
                f.cancel(true);
                throw new SQLTimeoutException("failed to execute sql: " + sql + ", cause: the execution time exceeds timeout: " + this.queryTimeout + " seconds");
            } finally {
                executor.shutdownNow();
            }
        } else {
            return executeImpl(sql, reqId);
        }
    }

    public boolean executeImpl(String sql, Long reqId) throws SQLException {
        synchronized (this) {
            // check if closed
            if (isClosed()) {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
            }
            if (this.resultSet != null && !this.resultSet.isClosed())
                this.resultSet.close();

            // execute query
            long pSql = this.connection.getConnector().executeQuery(sql, reqId);
            // if pSql is create/insert/update/delete/alter SQL
            if (this.connection.getConnector().isUpdateQuery(pSql)) {
                this.affectedRows = this.connection.getConnector().getAffectedRows(pSql);
                this.connection.getConnector().freeResultSet(pSql);
                return false;
            }

            int timestampPrecision = this.connection.getConnector().getResultTimePrecision(pSql);
            this.resultSet = new TSDBResultSet(this, this.connection.getConnector(), pSql, timestampPrecision);
            return true;
        }
    }

    public ResultSet getResultSet() throws SQLException {
        if (isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        }

        return this.resultSet;
    }

    public int getUpdateCount() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        return this.affectedRows;
    }

    public Connection getConnection() throws SQLException {
        if (isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        }

        if (this.connection.getConnector() == null) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_JNI_CONNECTION_NULL);
        }

        return this.connection;
    }

    public void setConnection(TSDBConnection connection) {
        this.connection = connection;
    }

    public boolean isClosed() throws SQLException {
        return isClosed;
    }


}
