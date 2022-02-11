package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.ws.entity.*;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class BlockResultSet extends AbstractWSResultSet {

    public BlockResultSet(Statement statement, Transport transport, RequestFactory factory, QueryResp response, String database) throws SQLException {
        super(statement, transport, factory, response, database);
    }

    @Override
    public List<List<Object>> fetchJsonData() throws SQLException, ExecutionException, InterruptedException {
        FetchReq fetchReq = new FetchReq(queryId, queryId);
        Request blockRequest = new Request(Action.FETCH_BLOCK.getAction(), fetchReq);
        CompletableFuture<Response> fetchFuture = transport.send(blockRequest);
        Response resp = fetchFuture.get();
        FetchBlockResp blockResp = (FetchBlockResp) resp;
        return null;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return false;
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return 0;
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return 0;
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return 0;
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return 0;
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return 0;
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return 0;
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        return new byte[0];
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return 0;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return false;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return false;
    }

    @Override
    public boolean isFirst() throws SQLException {
        return false;
    }

    @Override
    public boolean isLast() throws SQLException {
        return false;
    }

    @Override
    public void beforeFirst() throws SQLException {

    }

    @Override
    public void afterLast() throws SQLException {

    }

    @Override
    public boolean first() throws SQLException {
        return false;
    }

    @Override
    public boolean last() throws SQLException {
        return false;
    }

    @Override
    public int getRow() throws SQLException {
        return 0;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        return false;
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        return false;
    }

    @Override
    public boolean previous() throws SQLException {
        return false;
    }

    @Override
    public Statement getStatement() throws SQLException {
        return null;
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return null;
    }
}
