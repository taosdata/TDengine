package com.taosdata.jdbc.rs;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.taosdata.jdbc.*;
import com.taosdata.jdbc.enums.TimestampFormat;
import com.taosdata.jdbc.utils.HttpClientPoolUtil;
import com.taosdata.jdbc.ws.InFlightRequest;
import com.taosdata.jdbc.ws.Transport;
import com.taosdata.jdbc.ws.WSClient;
import com.taosdata.jdbc.ws.WSConnection;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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

        String user;
        String password;
        try {
            if (!props.containsKey(TSDBDriver.PROPERTY_KEY_USER))
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_USER_IS_REQUIRED);
            if (!props.containsKey(TSDBDriver.PROPERTY_KEY_PASSWORD))
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PASSWORD_IS_REQUIRED);

            user = URLEncoder.encode(props.getProperty(TSDBDriver.PROPERTY_KEY_USER), StandardCharsets.UTF_8.displayName());
            password = URLEncoder.encode(props.getProperty(TSDBDriver.PROPERTY_KEY_PASSWORD), StandardCharsets.UTF_8.displayName());
        } catch (UnsupportedEncodingException e) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "unsupported UTF-8 concoding, user: " + props.getProperty(TSDBDriver.PROPERTY_KEY_USER) + ", password: " + props.getProperty(TSDBDriver.PROPERTY_KEY_PASSWORD));
        }
        String loginUrl;
        String batchLoad = info.getProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD);
//        if (Boolean.parseBoolean(batchLoad)) {
        if (false) {
            loginUrl = "ws://" + props.getProperty(TSDBDriver.PROPERTY_KEY_HOST)
                    + ":" + props.getProperty(TSDBDriver.PROPERTY_KEY_PORT) + "/rest/ws";
            WSClient client;
            Transport transport;
            try {
                int timeout = props.containsKey(TSDBDriver.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT)
                        ? Integer.parseInt(props.getProperty(TSDBDriver.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT))
                        : Transport.DEFAULT_MESSAGE_WAIT_TIMEOUT;
                int maxRequest = props.containsKey(TSDBDriver.PROPERTY_KEY_MAX_CONCURRENT_REQUEST)
                        ? Integer.parseInt(props.getProperty(TSDBDriver.PROPERTY_KEY_MAX_CONCURRENT_REQUEST))
                        : Transport.DEFAULT_MAX_REQUEST;

                InFlightRequest inFlightRequest = new InFlightRequest(timeout, maxRequest);
                CountDownLatch latch = new CountDownLatch(1);
                Map<String, String> httpHeaders = new HashMap<>();
                client = new WSClient(new URI(loginUrl), user, password, database,
                        inFlightRequest, httpHeaders, latch, maxRequest);
                transport = new Transport(client, inFlightRequest);
                if (!client.connectBlocking(timeout, TimeUnit.MILLISECONDS)) {
                    throw new SQLException("can't create connection with server");
                }
                if (!latch.await(timeout, TimeUnit.MILLISECONDS)) {
                    throw new SQLException("auth timeout");
                }
                if (!client.isAuth()) {
                    throw new SQLException("auth failure");
                }
            } catch (URISyntaxException e) {
                throw new SQLException("Websocket url parse error: " + loginUrl, e);
            } catch (InterruptedException e) {
                throw new SQLException("creat websocket connection has been Interrupted ", e);
            }
            // TODO fetch Type from config
            props.setProperty(TSDBDriver.PROPERTY_KEY_TIMESTAMP_FORMAT, String.valueOf(TimestampFormat.TIMESTAMP));
            return new WSConnection(url, props, transport, database);
        }
        loginUrl = "http://" + props.getProperty(TSDBDriver.PROPERTY_KEY_HOST) + ":" + props.getProperty(TSDBDriver.PROPERTY_KEY_PORT) + "/rest/login/" + user + "/" + password + "";
        int poolSize = Integer.parseInt(props.getProperty("httpPoolSize", HttpClientPoolUtil.DEFAULT_MAX_PER_ROUTE));
        boolean keepAlive = Boolean.parseBoolean(props.getProperty("httpKeepAlive", HttpClientPoolUtil.DEFAULT_HTTP_KEEP_ALIVE));

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
        return url.trim().length() > 0 && url.startsWith(URL_PREFIX);
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
