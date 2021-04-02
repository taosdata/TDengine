package com.taosdata.jdbc.rs;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.taosdata.jdbc.AbstractStatement;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.utils.HttpClientPoolUtil;
import com.taosdata.jdbc.utils.SqlSyntaxValidator;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class RestfulStatement extends AbstractStatement {

    private boolean closed;
    private String database;
    private final RestfulConnection conn;

    private volatile RestfulResultSet resultSet;
    private volatile int affectedRows;

    public RestfulStatement(RestfulConnection conn, String database) {
        this.conn = conn;
        this.database = database;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        if (!SqlSyntaxValidator.isValidForExecuteQuery(sql))
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_FOR_EXECUTE_QUERY, "not a valid sql for executeQuery: " + sql);

        final String url = "http://" + conn.getHost() + ":" + conn.getPort() + "/rest/sql";
        if (SqlSyntaxValidator.isDatabaseUnspecifiedQuery(sql)) {
            return executeOneQuery(url, sql);
        }

//        if (this.database != null && !this.database.trim().replaceAll("\\s","").isEmpty())
//            HttpClientPoolUtil.execute(url, "use " + this.database);
        return executeOneQuery(url, sql);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        if (!SqlSyntaxValidator.isValidForExecuteUpdate(sql))
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_FOR_EXECUTE_UPDATE, "not a valid sql for executeUpdate: " + sql);

        final String url = "http://" + conn.getHost() + ":" + conn.getPort() + "/rest/sql";
        if (SqlSyntaxValidator.isDatabaseUnspecifiedUpdate(sql)) {
            return executeOneUpdate(url, sql);
        }

//        if (this.database != null && !this.database.trim().replaceAll("\\s", "").isEmpty())
//            HttpClientPoolUtil.execute(url, "use " + this.database);
        return executeOneUpdate(url, sql);
    }

    @Override
    public void close() throws SQLException {
        synchronized (RestfulStatement.class) {
            if (!isClosed())
                this.closed = true;
        }
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        if (!SqlSyntaxValidator.isValidForExecute(sql))
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_FOR_EXECUTE, "not a valid sql for execute: " + sql);

        //如果执行了use操作应该将当前Statement的catalog设置为新的database
        boolean result = true;
        final String url = "http://" + conn.getHost() + ":" + conn.getPort() + "/rest/sql";
        if (SqlSyntaxValidator.isUseSql(sql)) {
            HttpClientPoolUtil.execute(url, sql);
            this.database = sql.trim().replace("use", "").trim();
            this.conn.setCatalog(this.database);
            result = false;
        } else if (SqlSyntaxValidator.isDatabaseUnspecifiedQuery(sql)) {
            executeOneQuery(url, sql);
        } else if (SqlSyntaxValidator.isDatabaseUnspecifiedUpdate(sql)) {
            executeOneUpdate(url, sql);
            result = false;
        } else {
            if (SqlSyntaxValidator.isValidForExecuteQuery(sql)) {
                executeQuery(sql);
            } else {
                executeUpdate(sql);
                result = false;
            }
        }

        return result;
    }

    private ResultSet executeOneQuery(String url, String sql) throws SQLException {
        if (!SqlSyntaxValidator.isValidForExecuteQuery(sql))
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_FOR_EXECUTE_QUERY, "not a valid sql for executeQuery: " + sql);

        // row data
        String result = HttpClientPoolUtil.execute(url, sql);
        JSONObject resultJson = JSON.parseObject(result);
        if (resultJson.getString("status").equals("error")) {
            throw TSDBError.createSQLException(resultJson.getInteger("code"), resultJson.getString("desc"));
        }
        this.resultSet = new RestfulResultSet(database, this, resultJson);
        this.affectedRows = 0;
        return resultSet;
    }

    private int executeOneUpdate(String url, String sql) throws SQLException {
        if (!SqlSyntaxValidator.isValidForExecuteUpdate(sql))
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_FOR_EXECUTE_UPDATE, "not a valid sql for executeUpdate: " + sql);

        String result = HttpClientPoolUtil.execute(url, sql);
        JSONObject jsonObject = JSON.parseObject(result);
        if (jsonObject.getString("status").equals("error")) {
            throw TSDBError.createSQLException(jsonObject.getInteger("code"), jsonObject.getString("desc"));
        }
        this.resultSet = null;
        this.affectedRows = checkJsonResultSet(jsonObject);
        return this.affectedRows;
    }

    private int checkJsonResultSet(JSONObject jsonObject) {
        // create ... SQLs should return 0 , and Restful result is this:
        // {"status": "succ", "head": ["affected_rows"], "data": [[0]], "rows": 1}
        JSONArray head = jsonObject.getJSONArray("head");
        JSONArray data = jsonObject.getJSONArray("data");
        int rows = Integer.parseInt(jsonObject.getString("rows"));
        if (head.size() == 1 && "affected_rows".equals(head.getString(0))
                && data.size() == 1 && data.getJSONArray(0).getInteger(0) == 0 && rows == 1) {
            return 0;
        }
        return rows;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        return resultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);

        return this.affectedRows;
    }

    @Override
    public Connection getConnection() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        return this.conn;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }


}
