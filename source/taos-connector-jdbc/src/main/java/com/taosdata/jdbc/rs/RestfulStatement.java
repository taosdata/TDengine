package com.taosdata.jdbc.rs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.taosdata.jdbc.AbstractStatement;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.utils.HttpClientPoolUtil;
import com.taosdata.jdbc.utils.JsonUtil;
import com.taosdata.jdbc.utils.SqlSyntaxValidator;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.taosdata.jdbc.utils.SqlSyntaxValidator.getDatabaseName;

/**
 * @deprecated Use WebSocket connection instead.
 */
@Deprecated
public class RestfulStatement extends AbstractStatement {

    private boolean closed;
    private String database;
    private final RestfulConnection conn;
    private static final String ROW_NAME = "affected_rows";

    private volatile RestfulResultSet resultSet; //NOSONAR

    public RestfulStatement(RestfulConnection conn, String database) {
        this.conn = conn;
        this.database = database;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);

        execute(sql);
        return resultSet;
    }

    public ResultSet executeQuery(String sql, Long reqId) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);

        execute(sql, reqId);
        return resultSet;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);

        execute(sql);
        return affectedRows;
    }

    public int executeUpdate(String sql, Long reqId) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);

        execute(sql, reqId);
        return affectedRows;
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
        return execute(sql, (Long) null);
    }

    @Override
    public boolean execute(String sql, Long reqId) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED);

        // 如果执行了use操作应该将当前Statement的catalog设置为新的database
        boolean result = true;

        String response = HttpClientPoolUtil.execute(getUrl(), sql, this.conn.getAuth(), reqId);
        ObjectMapper objectMapper = JsonUtil.getObjectMapper();
        JsonNode jsonObject = null;
        try {
            jsonObject = objectMapper.readTree(response);
        } catch (JsonProcessingException e) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, String.format("execute sql: %s, response: %s, can not cast to JsonNode.", sql, response));
        }
        if (jsonObject.get("code").asInt() != 0) {
            throw TSDBError.createSQLException(jsonObject.get("code").asInt(), "sql: " + sql + ", desc: " + jsonObject.get("desc").asText());
        }

        if (SqlSyntaxValidator.isUseSql(sql)) {
            this.database = getDatabaseName(sql);
            this.conn.setCatalog(this.database);
            this.conn.setClientInfo(TSDBDriver.PROPERTY_KEY_DBNAME, this.database);
            result = false;
        } else {
            JsonNode head = jsonObject.get("column_meta");
            if (head == null) {
                throw TSDBError.createSQLException(jsonObject.get("code").asInt(), "sql: " + sql + ", desc: " + jsonObject.get("desc").asText() + ", head meta is null.");
            }
            int rows = jsonObject.get("rows").asInt();
            if (head.size() == 1 && ROW_NAME.equals(head.get(0).get(0).asText()) && rows == 1) {
                this.resultSet = null;
                this.affectedRows = getAffectedRows(jsonObject);
                return false;
            } else {
                this.resultSet = new RestfulResultSet(database, this, jsonObject);
                this.affectedRows = -1;
            }
        }
        return result;
    }


    private String getUrl() throws SQLException {
        String dbname = conn.getClientInfo(TSDBDriver.PROPERTY_KEY_DBNAME);
        String protocol = "http";
        if (conn.isUseSsl()) {
            protocol = "https";
        }
        if (dbname == null || dbname.trim().isEmpty()) {
            dbname = "";
        } else {
            dbname = "/" + dbname.toLowerCase();
        }

        String url;
        String port = null != conn.getPort() ? ":" + conn.getPort() : "";
        if (this.conn.getToken() != null && !"".equals(this.conn.getToken().trim())) {
            url = protocol + "://" + conn.getHost() + port + "/rest/sql" + dbname + "?token=" + this.conn.getToken();
        } else {
            url = protocol + "://" + conn.getHost() + port + "/rest/sql" + dbname;
        }
        return url;
    }
    private int getAffectedRows(JsonNode jsonObject) throws SQLException {
        JsonNode head = jsonObject.get("column_meta");
        if (head.size() != 1 || !"affected_rows".equals(head.get(0).get(0).asText())) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "invalid variable: [" + head.toString() + "]");
        }
        JsonNode data = jsonObject.get("data");
        if (data != null) {
            return data.get(0).get(0).asInt();
        }
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "invalid variable: [" + jsonObject.toString() + "]");
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
