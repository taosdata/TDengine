package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.AbstractStatement;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.utils.SqlSyntaxValidator;
import com.taosdata.jdbc.ws.entity.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class WSStatement extends AbstractStatement {
    private boolean closed;
    private final Transport transport;
    private WSResultSet resultSet;
    private final String database;
    private final FetchType fetchType;
    private final Connection connection;
    private final RequestFactory factory;

    public WSStatement(Transport transport, String database, FetchType fetchType, Connection connection, RequestFactory factory) {
        this.transport = transport;
        this.database = database;
        this.fetchType = fetchType;
        this.connection = connection;
        this.factory = factory;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        if (!SqlSyntaxValidator.isValidForExecuteQuery(sql))
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_FOR_EXECUTE_QUERY, "not a valid sql for executeQuery: " + sql);

        this.execute(sql);
        return this.resultSet;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        if (!SqlSyntaxValidator.isValidForExecuteUpdate(sql))
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_FOR_EXECUTE_UPDATE, "not a valid sql for executeUpdate: " + sql);

        this.execute(sql);
        return affectedRows;
    }

    @Override
    public void close() throws SQLException {
        synchronized (WSStatement.class) {
            if (!isClosed())
                this.closed = true;
        }
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);

        Request request = factory.generateQuery(sql);
        CompletableFuture<Response> send = transport.send(request);

        Response response;
        try {
            response = send.get();
            QueryResp queryResp = (QueryResp) response;
            if (Code.SUCCESS.getCode() != queryResp.getCode()) {
                throw TSDBError.createSQLException(queryResp.getCode(), queryResp.getMessage());
            }
            if (queryResp.isUpdate()) {
                this.resultSet = null;
                this.affectedRows = queryResp.getAffectedRows();
                return false;
            } else {
                this.resultSet = new WSResultSet(this, queryResp, this.database, this.transport, this.fetchType, factory);
                this.affectedRows = -1;
                return true;
            }
        } catch (InterruptedException | ExecutionException e) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_WITH_EXECUTEQUERY, e.getMessage());
        }
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        return this.resultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);

        return affectedRows;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.connection;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }
}
