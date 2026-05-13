package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.*;
import com.taosdata.jdbc.ws.loadbalance.RebalanceManager;
import com.taosdata.jdbc.utils.ReqId;
import com.taosdata.jdbc.utils.SqlSyntaxValidator;
import com.taosdata.jdbc.ws.entity.*;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.taosdata.jdbc.utils.SqlSyntaxValidator.getDatabaseName;

public class WSStatement extends AbstractStatement {
    protected final Transport transport;
    protected String database;
    private final AbstractConnection connection;


    protected final AtomicBoolean closed = new AtomicBoolean(false);
    protected ResultSet resultSet;
    private volatile int queryTimeoutInSeconds;
    private volatile long queryTimeoutInMs;
    protected final ZoneId zoneId;

    public WSStatement(Transport transport, String database, AbstractConnection connection, Long instanceId, ZoneId zoneId) {
        this.transport = transport;
        this.database = database;
        this.connection = connection;
        this.instanceId = instanceId;

        // check if need to rebalance
        RebalanceManager rebalanceManager = RebalanceManager.getInstance();
        if (rebalanceManager.isRebalancing()
                && this.transport.isConnected()
                && this.connection.canRebalanced()
                && rebalanceManager.isRebalancing(this.transport.getCurrentEndpoint())
                && rebalanceManager.handleRebalancing(this.transport.getConnectionParam(), this.transport.getCurrentEndpoint())) {
            this.transport.balanceConnection();
        }
        this.connection.registerStatement(this.instanceId, this);
        this.zoneId = zoneId;
        this.queryTimeoutInSeconds = (transport.getConnectionParam().getRequestTimeout() + (1000 - 1)) / 1000;
        this.queryTimeoutInMs = transport.getConnectionParam().getRequestTimeout();
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        return executeQuery(sql, null);
    }

    public ResultSet executeQuery(String sql, Long reqId) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);

        this.execute(sql, reqId);
        return this.resultSet;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        return this.executeUpdate(sql, (Long) null);
    }

    public int executeUpdate(String sql, Long reqId) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);

        this.execute(sql, reqId);
        return affectedRows;
    }

    @Override
    public void close() throws SQLException {
        if (!isClosed()) {
            this.connection.unregisterStatement(this.instanceId);
            this.closed.set(true);
            if (resultSet != null && !resultSet.isClosed()) {
                resultSet.close();
            }
        }
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        return this.execute(sql, (Long) null);
    }

    public boolean execute(String sql, Long reqId) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);

        if (null == reqId)
            reqId = ReqId.getReqID();

        byte[] sqlBytes = sql.getBytes();

        // write version and sqlLen in little endian byte sequence
        byte[] result = ByteBuffer.allocate(6).order(ByteOrder.LITTLE_ENDIAN).putShort((short)1).putInt(sqlBytes.length).array();
        Response response = transport.send(Action.BINARY_QUERY.getAction(),
                reqId, 0, 6, result, sqlBytes, this.getQueryTimeoutInMs());

        QueryResp queryResp = (QueryResp) response;
        if (Code.SUCCESS.getCode() != queryResp.getCode()) {
            throw TSDBError.createSQLException(queryResp.getCode(), queryResp.getMessage());
        }
        if (SqlSyntaxValidator.isUseSql(sql)) {
            this.database = getDatabaseName(sql);
            this.connection.setCatalog(this.database);
            this.connection.setClientInfo(TSDBDriver.PROPERTY_KEY_DBNAME, this.database);
        }

        if (this.resultSet != null) {
            this.resultSet.close();
        }

        if (queryResp.isUpdate()) {
            this.resultSet = null;
            this.affectedRows = queryResp.getAffectedRows();
            return false;
        } else {
            this.resultSet = new BlockResultSet(this, this.transport, queryResp, this.database, this.zoneId);
            this.affectedRows = -1;
            return true;
        }
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return queryTimeoutInSeconds;
    }

    public long getQueryTimeoutInMs() {
        return queryTimeoutInMs;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        if (seconds < 0)
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE);

        if (seconds == 0){
            this.queryTimeoutInSeconds = Integer.MAX_VALUE;
            this.queryTimeoutInMs = Integer.MAX_VALUE * 1000L;
            return;
        }
        this.queryTimeoutInSeconds = seconds;
        this.queryTimeoutInMs = seconds * 1000L;
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
        return closed.get();
    }

    protected void checkBlobSupport() throws SQLException{
        AbstractConnection con = (AbstractConnection) getConnection();
        if (!con.isSupportBlob()){
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_BLOB_UNSUPPORTED_IN_SERVER);
        }
    }
}
