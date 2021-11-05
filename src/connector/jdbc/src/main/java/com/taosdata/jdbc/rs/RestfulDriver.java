package com.taosdata.jdbc.rs;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.taosdata.jdbc.*;
import com.taosdata.jdbc.utils.HttpClientPoolUtil;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

public class RestfulDriver extends AbstractDriver {

    private static final String URL_PREFIX = "jdbc:TAOS-RS://";

    static {
        try {
            DriverManager.registerDriver(new RestfulDriver());
        } catch (SQLException e) {
            throw TSDBError.createRuntimeException(TSDBErrorNumbers.ERROR_URL_NOT_SET, e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        // throw SQLException if url is null
        if (url == null || url.trim().isEmpty() || url.trim().replaceAll("\\s", "").isEmpty())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_URL_NOT_SET);

        // return null if url is not be accepted
        if (!acceptsURL(url))
            return null;

        Properties props = parseURL(url, info);
        String host = props.getProperty(TSDBDriver.PROPERTY_KEY_HOST);
        String port = props.getProperty(TSDBDriver.PROPERTY_KEY_PORT, "6041");
        String database = props.containsKey(TSDBDriver.PROPERTY_KEY_DBNAME) ? props.getProperty(TSDBDriver.PROPERTY_KEY_DBNAME) : null;

        String loginUrl = "http://" + host + ":" + port + "/rest/login/" + props.getProperty(TSDBDriver.PROPERTY_KEY_USER) + "/" + props.getProperty(TSDBDriver.PROPERTY_KEY_PASSWORD) + "";
        try {
            if (!props.containsKey(TSDBDriver.PROPERTY_KEY_USER))
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_USER_IS_REQUIRED);
            if (!props.containsKey(TSDBDriver.PROPERTY_KEY_PASSWORD))
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PASSWORD_IS_REQUIRED);

            String user = URLEncoder.encode(props.getProperty(TSDBDriver.PROPERTY_KEY_USER), StandardCharsets.UTF_8.displayName());
            String password = URLEncoder.encode(props.getProperty(TSDBDriver.PROPERTY_KEY_PASSWORD), StandardCharsets.UTF_8.displayName());
            loginUrl = "http://" + props.getProperty(TSDBDriver.PROPERTY_KEY_HOST) + ":" + props.getProperty(TSDBDriver.PROPERTY_KEY_PORT) + "/rest/login/" + user + "/" + password + "";
        } catch (UnsupportedEncodingException e) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "unsupported UTF-8 concoding, user: " + props.getProperty(TSDBDriver.PROPERTY_KEY_USER) + ", password: " + props.getProperty(TSDBDriver.PROPERTY_KEY_PASSWORD));
        }

        int poolSize = Integer.valueOf(props.getProperty("httpPoolSize", HttpClientPoolUtil.DEFAULT_MAX_PER_ROUTE));
        boolean keepAlive = Boolean.valueOf(props.getProperty("httpKeepAlive", HttpClientPoolUtil.DEFAULT_HTTP_KEEP_ALIVE));

        HttpClientPoolUtil.init(poolSize, keepAlive);
        String result = HttpClientPoolUtil.execute(loginUrl);
        JSONObject jsonResult = JSON.parseObject(result);
        String status = jsonResult.getString("status");
        String token = jsonResult.getString("desc");

        if (!status.equals("succ")) {
            throw new SQLException(jsonResult.getString("desc"));
        }

        RestfulConnection conn = new RestfulConnection(host, port, props, database, url, token);
        if (database != null && !database.trim().replaceAll("\\s", "").isEmpty()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("use " + database);
            }
        }
        return conn;
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        if (url == null)
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_URL_NOT_SET);
        return url.length() > 0 && url.trim().length() > 0 && url.startsWith(URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        if (info == null) {
            info = new Properties();
        }
        if (acceptsURL(url)) {
            info = parseURL(url, info);
        }
        return getPropertyInfo(info);
    }

    @Override
    public int getMajorVersion() {
        return 2;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        //TODO SQLFeatureNotSupportedException
        throw new SQLFeatureNotSupportedException();
    }
}
