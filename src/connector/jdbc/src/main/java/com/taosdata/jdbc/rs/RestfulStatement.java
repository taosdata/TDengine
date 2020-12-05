package com.taosdata.jdbc.rs;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.rs.util.HttpClientPoolUtil;
import com.taosdata.jdbc.utils.SqlSyntaxValidator;

import java.sql.*;
import java.util.Arrays;
import java.util.List;

public class RestfulStatement implements Statement {

    private boolean closed;
    private String database;
    private final RestfulConnection conn;

    public RestfulStatement(RestfulConnection c, String database) {
        this.conn = c;
        this.database = database;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        if (isClosed())
            throw new SQLException("statement already closed");
        if (!SqlSyntaxValidator.isSelectSql(sql))
            throw new SQLException("not a select sql for executeQuery: " + sql);

        final String url = "http://" + conn.getHost() + ":" + conn.getPort() + "/rest/sql";
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
            return new RestfulResultSet(dataStr, "");
        }

        JSONObject jsonField = JSON.parseObject(fields);
        if (jsonField == null) {
            return new RestfulResultSet(dataStr, "");
        }
        if (jsonField.getString("status").equals("error")) {
            throw new SQLException(TSDBConstants.WrapErrMsg("SQL execution error: " +
                    jsonField.getString("desc") + "\n" +
                    "error code: " + jsonField.getString("code")));
        }
        String fieldData = jsonField.getString("data");

        return new RestfulResultSet(dataStr, fieldData);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        if (isClosed())
            throw new SQLException("statement already closed");
        if (!SqlSyntaxValidator.isValidForExecuteUpdate(sql))
            throw new SQLException("not a valid sql for executeUpdate: " + sql);

        if (this.database == null)
            throw new SQLException("Database not specified or available");

        final String url = "http://" + conn.getHost() + ":" + conn.getPort() + "/rest/sql";
        HttpClientPoolUtil.execute(url, "use " + conn.getDatabase());
        String result = HttpClientPoolUtil.execute(url, sql);
        JSONObject jsonObject = JSON.parseObject(result);
        if (jsonObject.getString("status").equals("error")) {
            throw new SQLException(TSDBConstants.WrapErrMsg("SQL execution error: " +
                    jsonObject.getString("desc") + "\n" +
                    "error code: " + jsonObject.getString("code")));
        }
        return Integer.parseInt(jsonObject.getString("rows"));
    }

    @Override
    public void close() throws SQLException {
        this.closed = true;
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
        //TODO: getWarnings not Implemented
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
        if (isClosed()) {
            throw new SQLException("Invalid method call on a closed statement.");
        }
        //如果执行了use操作应该将当前Statement的catalog设置为新的database
        if (SqlSyntaxValidator.isUseSql(sql)) {
            this.database = sql.trim().replace("use", "").trim();
        }
        if (this.database == null)
            throw new SQLException("Database not specified or available");

        final String url = "http://" + conn.getHost() + ":" + conn.getPort() + "/rest/sql";
        // use database
        HttpClientPoolUtil.execute(url, "use " + conn.getDatabase());
        // execute sql
        String result = HttpClientPoolUtil.execute(url, sql);
        // parse result
        JSONObject jsonObject = JSON.parseObject(result);
        if (jsonObject.getString("status").equals("error")) {
            throw new SQLException(TSDBConstants.WrapErrMsg("SQL execution error: " +
                    jsonObject.getString("desc") + "\n" +
                    "error code: " + jsonObject.getString("code")));
        }
        return true;
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
        return closed;
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
