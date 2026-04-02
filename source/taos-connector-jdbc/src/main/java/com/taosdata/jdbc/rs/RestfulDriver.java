package com.taosdata.jdbc.rs;


import com.taosdata.jdbc.AbstractDriver;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.utils.HttpClientPoolUtil;
import com.taosdata.jdbc.utils.StringUtils;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Base64;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * @deprecated Use WebSocket connection instead.
 */
@Deprecated
public class RestfulDriver extends AbstractDriver {
    public static final String URL_PREFIX = "jdbc:TAOS-RS://";

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

        Properties props = StringUtils.parseUrl(url, info);
        String batchLoad = info.getProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD);
        if (Boolean.parseBoolean(batchLoad)) {
            ConnectionParam param = ConnectionParam.getParamWs(props);
            return getWSConnection(url, param, props);
        }
        HttpClientPoolUtil.init(props);
        ConnectionParam param = ConnectionParam.getParam(props);

        String auth = null;

        if (param.getUser() != null && param.getPassword() != null) {
            auth = Base64.getEncoder().encodeToString(
                    (param.getUser() + ":" + param.getPassword()).getBytes(StandardCharsets.UTF_8));
        }

        RestfulConnection conn = new RestfulConnection(param.getEndpoints().get(0).getHost(), String.valueOf(param.getEndpoints().get(0).getPort()), props, param.getDatabase(),
                url, auth, param.isUseSsl(), param.getCloudToken(), param.getTz());
        if (param.getDatabase() != null && !param.getDatabase().trim().replaceAll("\\s", "").isEmpty()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("use " + param.getDatabase()); // NOSONAR
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
            info = StringUtils.parseUrl(url, info);
        }
        return getPropertyInfo(info);
    }

    @Override
    public int getMajorVersion() {
        return 3;
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
        throw new SQLFeatureNotSupportedException();
    }
}
