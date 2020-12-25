package com.taosdata.jdbc.rs;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.rs.util.HttpClientPoolUtil;
import com.taosdata.jdbc.utils.SqlSyntaxValidator;

import java.sql.*;
import java.util.Arrays;
import java.util.List;

public class RestfulStatement implements Statement {

    private static final String STATEMENT_CLOSED = "Statement already closed.";
    private boolean closed;
    private String database;
    private final RestfulConnection conn;

    private volatile RestfulResultSet resultSet;
    private volatile int affectedRows;
    private volatile boolean closeOnCompletion;

    public RestfulStatement(RestfulConnection conn, String database) {
        this.conn = conn;
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
            throw new SQLException(TSDBConstants.WrapErrMsg("SQL execution error: " + jsonObject.getString("desc") + "\n" + "error code: " + jsonObject.getString("code")));
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
            throw new SQLException(TSDBConstants.WrapErrMsg("SQL execution error: " + jsonField.getString("desc") + "\n" + "error code: " + jsonField.getString("code")));
        }
        String fieldData = jsonField.getString("data");

        this.resultSet = new RestfulResultSet(dataStr, fieldData);
        this.affectedRows = 0;
        return resultSet;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        if (isClosed())
            throw new SQLException("statement already closed");
        if (!SqlSyntaxValidator.isValidForExecuteUpdate(sql))
            throw new SQLException("not a valid sql for executeUpdate: " + sql);

        if (this.database == null)
            throw new SQLException("Database not specified or available");

        final String url = "http://" + conn.getHost().trim() + ":" + conn.getPort() + "/rest/sql";
        HttpClientPoolUtil.execute(url, "use " + conn.getDatabase());
        String result = HttpClientPoolUtil.execute(url, sql);
        JSONObject jsonObject = JSON.parseObject(result);
        if (jsonObject.getString("status").equals("error")) {
            throw new SQLException(TSDBConstants.WrapErrMsg("SQL execution error: " + jsonObject.getString("desc") + "\n" + "error code: " + jsonObject.getString("code")));
        }
        this.resultSet = null;
        this.affectedRows = Integer.parseInt(jsonObject.getString("rows"));
        return this.affectedRows;
    }

    @Override
    public void close() throws SQLException {
        synchronized (RestfulStatement.class) {
            if (!isClosed())
                this.closed = true;
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        if (isClosed())
            throw new SQLException(STATEMENT_CLOSED);
        return TSDBConstants.maxFieldSize;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        if (isClosed())
            throw new SQLException(STATEMENT_CLOSED);
        if (max < 0)
            throw new SQLException(TSDBConstants.INVALID_VARIABLES);
        // nothing to do
    }

    @Override
    public int getMaxRows() throws SQLException {
        if (isClosed())
            throw new SQLException(STATEMENT_CLOSED);
        return 0;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        if (isClosed())
            throw new SQLException(STATEMENT_CLOSED);
        if (max < 0)
            throw new SQLException(TSDBConstants.INVALID_VARIABLES);
        // nothing to do
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        if (isClosed())
            throw new SQLException(RestfulStatement.STATEMENT_CLOSED);
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        if (isClosed())
            throw new SQLException(STATEMENT_CLOSED);
        return 0;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        if (isClosed())
            throw new SQLException(STATEMENT_CLOSED);
        if (seconds < 0)
            throw new SQLException(TSDBConstants.INVALID_VARIABLES);
    }

    @Override
    public void cancel() throws SQLException {
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        if (isClosed())
            throw new SQLException(STATEMENT_CLOSED);
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        // nothing to do
        if (isClosed())
            throw new SQLException(STATEMENT_CLOSED);
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        if (isClosed())
            throw new SQLException(RestfulStatement.STATEMENT_CLOSED);
        throw new SQLException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
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

        if (SqlSyntaxValidator.isSelectSql(sql)) {
            executeQuery(sql);
        } else if (SqlSyntaxValidator.isInsertSql(sql)) {
            executeUpdate(sql);
        } else {
            final String url = "http://" + conn.getHost() + ":" + conn.getPort() + "/rest/sql";
            HttpClientPoolUtil.execute(url, "use " + this.database);
            String result = HttpClientPoolUtil.execute(url, sql);
            JSONObject jsonObject = JSON.parseObject(result);
            if (jsonObject.getString("status").equals("error")) {
                throw new SQLException(TSDBConstants.WrapErrMsg("SQL execution error: " + jsonObject.getString("desc") + "\n" + "error code: " + jsonObject.getString("code")));
            }
            this.resultSet = new RestfulResultSet(jsonObject.getJSONArray("data"), jsonObject.getJSONArray("head"));
        }

        return true;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        if (isClosed())
            throw new SQLException(STATEMENT_CLOSED);
        return resultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Invalid method call on a closed statement.");
        }
        return this.affectedRows;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return getMoreResults(CLOSE_CURRENT_RESULT);
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (direction != ResultSet.FETCH_FORWARD && direction != ResultSet.FETCH_REVERSE && direction != ResultSet.FETCH_UNKNOWN)
            throw new SQLException(TSDBConstants.INVALID_VARIABLES);
        this.resultSet.setFetchDirection(direction);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return this.resultSet.getFetchDirection();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        if (isClosed())
            throw new SQLException(STATEMENT_CLOSED);
        if (rows < 0)
            throw new SQLException(TSDBConstants.INVALID_VARIABLES);
        //nothing to do
    }

    @Override
    public int getFetchSize() throws SQLException {
        if (isClosed())
            throw new SQLException(STATEMENT_CLOSED);
        return 0;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        if (isClosed())
            throw new SQLException(STATEMENT_CLOSED);
        return this.resultSet.getConcurrency();
    }

    @Override
    public int getResultSetType() throws SQLException {
        if (isClosed())
            throw new SQLException(STATEMENT_CLOSED);
        return this.resultSet.getType();
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        if (isClosed())
            throw new SQLException(STATEMENT_CLOSED);
        //TODO:
    }

    @Override
    public void clearBatch() throws SQLException {
        //TODO:
    }

    @Override
    public int[] executeBatch() throws SQLException {
        //TODO:
        return new int[0];
    }

    @Override
    public Connection getConnection() throws SQLException {
        if (isClosed())
            throw new SQLException(STATEMENT_CLOSED);
        return this.conn;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        if (isClosed())
            throw new SQLException(STATEMENT_CLOSED);
        if (resultSet == null)
            return false;

//        switch (current) {
//            case CLOSE_CURRENT_RESULT:
//                resultSet.close();
//                break;
//            case KEEP_CURRENT_RESULT:
//                break;
//            case CLOSE_ALL_RESULTS:
//                resultSet.close();
//                break;
//            default:
//                throw new SQLException(TSDBConstants.INVALID_VARIABLES);
//        }
//        return next;
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException(TSDBConstants.UNSUPPORT_METHOD_EXCEPTIONZ_MSG);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        if (isClosed())
            throw new SQLException(STATEMENT_CLOSED);
        return this.resultSet.getHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        if (isClosed())
            throw new SQLException(STATEMENT_CLOSED);
        //nothing to do
    }

    @Override
    public boolean isPoolable() throws SQLException {
        if (isClosed())
            throw new SQLException(STATEMENT_CLOSED);
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        if (isClosed())
            throw new SQLException(STATEMENT_CLOSED);
        this.closeOnCompletion = true;
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        if (isClosed())
            throw new SQLException(STATEMENT_CLOSED);
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
