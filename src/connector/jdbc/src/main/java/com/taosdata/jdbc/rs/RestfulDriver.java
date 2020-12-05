package com.taosdata.jdbc.rs;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.taosdata.jdbc.AbstractTaosDriver;
import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.rs.util.HttpClientPoolUtil;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

public class RestfulDriver extends AbstractTaosDriver {

    private static final String URL_PREFIX = "jdbc:TAOS-RS://";

    static {
        try {
            DriverManager.registerDriver(new RestfulDriver());
        } catch (SQLException e) {
            throw new RuntimeException(TSDBConstants.WrapErrMsg("can not register Restful JDBC driver"), e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        // throw SQLException if url is null
        if (url == null)
            throw new SQLException(TSDBConstants.WrapErrMsg("url is not set!"));
        // return null if url is not be accepted
        if (!acceptsURL(url))
            return null;

        Properties props = parseURL(url, info);
        String host = props.getProperty(TSDBDriver.PROPERTY_KEY_HOST, "localhost");
        String port = props.getProperty(TSDBDriver.PROPERTY_KEY_PORT, "6041");
        String database = props.containsKey(TSDBDriver.PROPERTY_KEY_DBNAME) ? props.getProperty(TSDBDriver.PROPERTY_KEY_DBNAME) : null;

        String loginUrl = "http://" + props.getProperty(TSDBDriver.PROPERTY_KEY_HOST) + ":"
                + props.getProperty(TSDBDriver.PROPERTY_KEY_PORT) + "/rest/login/"
                + props.getProperty(TSDBDriver.PROPERTY_KEY_USER) + "/"
                + props.getProperty(TSDBDriver.PROPERTY_KEY_PASSWORD) + "";
        String result = HttpClientPoolUtil.execute(loginUrl);
        JSONObject jsonResult = JSON.parseObject(result);
        String status = jsonResult.getString("status");
        if (!status.equals("succ")) {
            throw new SQLException(jsonResult.getString("desc"));
        }

        return new RestfulConnection(host, port, props, database, url);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        if (url == null)
            throw new SQLException(TSDBConstants.WrapErrMsg("url is null"));
        return (url != null && url.length() > 0 && url.trim().length() > 0) && url.startsWith(URL_PREFIX);
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
