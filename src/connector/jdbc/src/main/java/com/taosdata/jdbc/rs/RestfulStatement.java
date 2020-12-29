package com.taosdata.jdbc.rs;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.rs.util.HttpClientPoolUtil;
import com.taosdata.jdbc.utils.SqlSyntaxValidator;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

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

    private String[] parseTableIdentifier(String sql) {
        sql = sql.trim().toLowerCase();
        String[] ret = null;
        if (sql.contains("where"))
            sql = sql.substring(0, sql.indexOf("where"));
        if (sql.contains("interval"))
            sql = sql.substring(0, sql.indexOf("interval"));
        if (sql.contains("fill"))
            sql = sql.substring(0, sql.indexOf("fill"));
        if (sql.contains("sliding"))
            sql = sql.substring(0, sql.indexOf("sliding"));
        if (sql.contains("group by"))
            sql = sql.substring(0, sql.indexOf("group by"));
        if (sql.contains("order by"))
            sql = sql.substring(0, sql.indexOf("order by"));
        if (sql.contains("slimit"))
            sql = sql.substring(0, sql.indexOf("slimit"));
        if (sql.contains("limit"))
            sql = sql.substring(0, sql.indexOf("limit"));
        // parse
        if (sql.contains("from")) {
            sql = sql.substring(sql.indexOf("from") + 4).trim();
            return Arrays.asList(sql.split(",")).stream()
                    .map(tableIdentifier -> {
                        tableIdentifier = tableIdentifier.trim();
                        if (tableIdentifier.contains(" "))
                            tableIdentifier = tableIdentifier.substring(0, tableIdentifier.indexOf(" "));
                        return tableIdentifier;
                    }).collect(Collectors.joining(",")).split(",");
        }
        return ret;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        if (isClosed())
            throw new SQLException("statement already closed");
        if (!SqlSyntaxValidator.isSelectSql(sql))
            throw new SQLException("not a select sql for executeQuery: " + sql);

        final String url = "http://" + conn.getHost() + ":" + conn.getPort() + "/rest/sql";
        // row data
        String result = HttpClientPoolUtil.execute(url, sql);
        JSONObject resultJson = JSON.parseObject(result);
        if (resultJson.getString("status").equals("error")) {
            throw new SQLException(TSDBConstants.WrapErrMsg("SQL execution error: " + resultJson.getString("desc") + "\n" + "error code: " + resultJson.getString("code")));
        }

        // parse table name from sql
        String[] tableIdentifiers = parseTableIdentifier(sql);
        if (tableIdentifiers != null) {
            List<JSONObject> fieldJsonList = new ArrayList<>();
            for (String tableIdentifier : tableIdentifiers) {
                // field meta
                String fields = HttpClientPoolUtil.execute(url, "DESCRIBE " + tableIdentifier);
                JSONObject fieldJson = JSON.parseObject(fields);
                if (fieldJson.getString("status").equals("error")) {
                    throw new SQLException(TSDBConstants.WrapErrMsg("SQL execution error: " + fieldJson.getString("desc") + "\n" + "error code: " + fieldJson.getString("code")));
                }
                fieldJsonList.add(fieldJson);
            }
            this.resultSet = new RestfulResultSet(database, this, resultJson, fieldJsonList);
        } else {
            this.resultSet = new RestfulResultSet(database, this, resultJson);
        }

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
        } else if (SqlSyntaxValidator.isShowSql(sql) || SqlSyntaxValidator.isDescribeSql(sql)) {
            final String url = "http://" + conn.getHost().trim() + ":" + conn.getPort() + "/rest/sql";
            if (!SqlSyntaxValidator.isShowDatabaseSql(sql)) {
                HttpClientPoolUtil.execute(url, "use " + conn.getDatabase());
            }
            String result = HttpClientPoolUtil.execute(url, sql);
            JSONObject resultJson = JSON.parseObject(result);
            if (resultJson.getString("status").equals("error")) {
                throw new SQLException(TSDBConstants.WrapErrMsg("SQL execution error: " + resultJson.getString("desc") + "\n" + "error code: " + resultJson.getString("code")));
            }
            this.resultSet = new RestfulResultSet(database, this, resultJson);
        } else {
            executeUpdate(sql);
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
