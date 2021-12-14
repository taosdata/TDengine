package com.taosdata.jdbc.rs;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.taosdata.jdbc.AbstractStatement;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.enums.TimestampFormat;
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

        return executeOneQuery(sql);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);
        if (!SqlSyntaxValidator.isValidForExecuteUpdate(sql))
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_FOR_EXECUTE_UPDATE, "not a valid sql for executeUpdate: " + sql);

        return executeOneUpdate(sql);
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

        //如果执行了use操作应该将当前Statement的catalog设置为新的database
        boolean result = true;

        if (SqlSyntaxValidator.isUseSql(sql)) {
            String ret = HttpClientPoolUtil.execute(getUrl(), sql, this.conn.getToken());
            JSONObject resultJson = JSON.parseObject(ret);
            if (resultJson.getString("status").equals("error")) {
                throw TSDBError.createSQLException(resultJson.getInteger("code"), "sql: " + sql + ", desc: " + resultJson.getString("desc"));
            }
            this.database = sql.trim().replace("use", "").trim();
            this.conn.setCatalog(this.database);
            result = false;
        } else if (SqlSyntaxValidator.isDatabaseUnspecifiedQuery(sql)) {
            executeOneQuery(sql);
        } else if (SqlSyntaxValidator.isDatabaseUnspecifiedUpdate(sql)) {
            executeOneUpdate(sql);
            result = false;
        } else {
            if (SqlSyntaxValidator.isValidForExecuteQuery(sql)) {
                executeOneQuery(sql);
            } else {
                executeOneUpdate(sql);
                result = false;
            }
        }

        return result;
    }

    private String getUrl() throws SQLException {
        String dbname = conn.getClientInfo(TSDBDriver.PROPERTY_KEY_DBNAME);
        if (dbname == null || dbname.trim().isEmpty()) {
            dbname = "";
        } else {
            dbname = "/" + dbname.toLowerCase();
        }
        TimestampFormat timestampFormat = TimestampFormat.valueOf(conn.getClientInfo(TSDBDriver.PROPERTY_KEY_TIMESTAMP_FORMAT).trim().toUpperCase());
        String url;

        switch (timestampFormat) {
            case TIMESTAMP:
                url = "http://" + conn.getHost() + ":" + conn.getPort() + "/rest/sqlt" + dbname;
                break;
            case UTC:
                url = "http://" + conn.getHost() + ":" + conn.getPort() + "/rest/sqlutc" + dbname;
                break;
            default:
                url = "http://" + conn.getHost() + ":" + conn.getPort() + "/rest/sql" + dbname;
        }
        return url;
    }

    private ResultSet executeOneQuery(String sql) throws SQLException {
        // row data
        String result = HttpClientPoolUtil.execute(getUrl(), sql, this.conn.getToken());
        JSONObject resultJson = JSON.parseObject(result);
        if (resultJson.getString("status").equals("error")) {
            throw TSDBError.createSQLException(resultJson.getInteger("code"), "sql: " + sql + ", desc: " + resultJson.getString("desc"));
        }
        this.resultSet = new RestfulResultSet(database, this, resultJson);
        this.affectedRows = -1;
        return resultSet;
    }

    private int executeOneUpdate(String sql) throws SQLException {
        String result = HttpClientPoolUtil.execute(getUrl(), sql, this.conn.getToken());
        JSONObject jsonObject = JSON.parseObject(result);
        if (jsonObject.getString("status").equals("error")) {
            throw TSDBError.createSQLException(jsonObject.getInteger("code"), "sql: " + sql + ", desc: " + jsonObject.getString("desc"));
        }
        this.resultSet = null;
        this.affectedRows = getAffectedRows(jsonObject);
        return this.affectedRows;
    }

    private int getAffectedRows(JSONObject jsonObject) throws SQLException {
        JSONArray head = jsonObject.getJSONArray("head");
        if (head.size() != 1 || !"affected_rows".equals(head.getString(0)))
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "invalid variable: [" + head.toJSONString() + "]");
        JSONArray data = jsonObject.getJSONArray("data");
        if (data != null) {
            return data.getJSONArray(0).getInteger(0);
        }
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "invalid variable: [" + jsonObject.toJSONString() + "]");
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
