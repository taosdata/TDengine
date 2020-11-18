package com.taosdata.jdbc.rs;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.rs.util.HttpClientPoolUtil;

import java.sql.*;
import java.util.Arrays;
import java.util.List;

public class TaosRestfulStatement implements Statement {

    private final String catalog;
    private final TaosRestfulConnection conn;

    public TaosRestfulStatement(TaosRestfulConnection c, String catalog) {
        this.conn = c;
        this.catalog = catalog;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {

        final String url = "http://" + conn.getHost() + ":"+conn.getPort()+"/rest/sql";

        String result = HttpClientPoolUtil.execute(url, sql);
        String fields = "";
        List<String> words = Arrays.asList(sql.split(" "));
        if (words.get(0).equalsIgnoreCase("select")) {
            int index = 0;
            if (words.contains("from")) {
                index = words.indexOf("from");
            }
            if (words.contains("FROM")) {
                index = words.indexOf("FROM");
            }
            fields = HttpClientPoolUtil.execute(url, "DESCRIBE " + words.get(index + 1));
        }

        JSONObject jsonObject = JSON.parseObject(result);
        if (jsonObject.getString("status").equals("error")) {
            throw new SQLException(TSDBConstants.WrapErrMsg("SQL execution error: " +
                    jsonObject.getString("desc") + "\n" +
                    "error code: " + jsonObject.getString("code")));
        }
        String dataStr = jsonObject.getString("data");
        if ("use".equalsIgnoreCase(fields.split(" ")[0])) {
            return new TaosRestfulResultSet(dataStr, "");
        }

        JSONObject jsonField = JSON.parseObject(fields);
        if (jsonField == null) {
            return new TaosRestfulResultSet(dataStr, "");
        }
        if (jsonField.getString("status").equals("error")) {
            throw new SQLException(TSDBConstants.WrapErrMsg("SQL execution error: " +
                    jsonField.getString("desc") + "\n" +
                    "error code: " + jsonField.getString("code")));
        }
        String fieldData = jsonField.getString("data");

        return new TaosRestfulResultSet(dataStr, fieldData);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        return 0;
    }

    @Override
    public void close() throws SQLException {

    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {

    }

    @Override
    public int getMaxRows() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {

    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {

    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return 0;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {

    }

    @Override
    public void cancel() throws SQLException {

    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public void setCursorName(String name) throws SQLException {

    }

    @Override
    public boolean execute(String sql) throws SQLException {
        return false;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return null;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return 0;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {

    }

    @Override
    public int getFetchDirection() throws SQLException {
        return 0;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {

    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return 0;
    }

    @Override
    public void addBatch(String sql) throws SQLException {

    }

    @Override
    public void clearBatch() throws SQLException {

    }

    @Override
    public int[] executeBatch() throws SQLException {
        return new int[0];
    }

    @Override
    public Connection getConnection() throws SQLException {
        return null;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return null;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return 0;
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return false;
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return false;
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {

    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {

    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
