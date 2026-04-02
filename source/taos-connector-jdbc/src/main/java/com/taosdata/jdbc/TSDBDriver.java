package com.taosdata.jdbc;

import com.taosdata.jdbc.utils.StringUtils;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * The Java SQL framework allows for multiple database drivers. Each driver
 * should supply a class that implements the Driver interface
 *
 * <p>
 * The DriverManager will try to load as many drivers as it can find and then
 * for any given connection request, it will ask each driver in turn to try to
 * connect to the target URL.
 *
 * <p>
 * It is strongly recommended that each Driver class should be small and stand
 * alone so that the Driver class can be loaded and queried without bringing in
 * vast quantities of supporting code.
 *
 * <p>
 * When a Driver class is loaded, it should create an instance of itself and
 * register it with the DriverManager. This means that a user can load and
 * register a driver by doing Class.forName("foo.bah.Driver")
 */
public class TSDBDriver extends AbstractDriver {

    /**
     * @deprecated Use {@link #URL_PREFIX} instead.
     */
    @Deprecated
    private static final String URL_PREFIX1 = "jdbc:TSDB://";

    public static final String URL_PREFIX = "jdbc:TAOS://";

    /**
     * PRODUCT_NAME
     */
    public static final String PROPERTY_KEY_PRODUCT_NAME = "productName";
    /**
     * Key used to retrieve the host value from the properties instance passed to
     * the driver.
     */
    public static final String PROPERTY_KEY_HOST = "host";
    /**
     * Key used to retrieve the port number value from the properties instance
     * passed to the driver.
     */
    public static final String PROPERTY_KEY_PORT = "port";
    /**
     * Key used to retrieve the endpoints value from the properties instance passed
     * to the driver.
     */
    public static final String PROPERTY_KEY_ENDPOINTS = "endpoints";
    /**
     * Key used to retrieve the database value from the properties instance passed
     * to the driver.
     */
    public static final String PROPERTY_KEY_DBNAME = "dbname";
    /**
     * Key used to retrieve the user value from the properties instance passed to
     * the driver.
     */
    public static final String PROPERTY_KEY_USER = "user";
    /**
     * Key used to retrieve the password value from the properties instance passed
     * to the driver.
     */
    public static final String PROPERTY_KEY_PASSWORD = "password";
    /**
     * Key used to retrieve the bearer_token value from the properties instance passed
     * to the driver.
     */
    public static final String PROPERTY_KEY_BEARER_TOKEN = "bearerToken";

    /**
     * Key used to retrieve the token value from the properties instance passed to
     * the driver.
     * Just for Cloud Service
     */
    public static final String PROPERTY_KEY_TOKEN = "token";
    /**
     * Use SSL (true/false) to communicate with the server. The default value is false.
     * Just for Cloud Service
     */
    public static final String PROPERTY_KEY_USE_SSL = "useSSL";
    /**
     * Key for the configuration file directory of TSDB client in properties instance
     */
    public static final String PROPERTY_KEY_CONFIG_DIR = "cfgdir";
    /**
     * Key for the timezone used by the TSDB client in properties instance
     */
    public static final String PROPERTY_KEY_TIME_ZONE = "timezone";
    /**
     * Key for the locale used by the TSDB client in properties instance
     */
    public static final String PROPERTY_KEY_LOCALE = "locale";
    /**
     * Key for the char encoding used by the TSDB client in properties instance
     */
    public static final String PROPERTY_KEY_CHARSET = "charset";

    /**
     * fetch data from native function in a batch model
     */
    public static final String PROPERTY_KEY_BATCH_LOAD = "batchfetch";

    /**
     * continue process commands in executeBatch
     */
    public static final String PROPERTY_KEY_BATCH_ERROR_IGNORE = "batchErrorIgnore";

    /**
     * message receive from server timeout. ms
     */
    public static final String PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT = "messageWaitTimeout";

    public static final String PROPERTY_KEY_WS_KEEP_ALIVE_SECONDS = "wsKeepAlive";
    /**
     * connect mode
     */
    public static final String PROPERTY_KEY_CONNECT_MODE = "conmode";
    /**
     * Specifies whether to treat VARCHAR as STRING type. Only effected in WebSocket connection mode.
     * This property can be used to control how VARCHAR data is handled.
     */
    public static final String PROPERTY_KEY_VARCHAR_AS_STRING = "varcharAsString";


    public static final String PROPERTY_KEY_ENABLE_COMPRESSION = "enableCompression";

    public static final String PROPERTY_KEY_ENABLE_AUTO_RECONNECT = "enableAutoReconnect";

    public static final String PROPERTY_KEY_SLAVE_CLUSTER_HOST = "slaveClusterHost";
    public static final String PROPERTY_KEY_SLAVE_CLUSTER_PORT = "slaveClusterPort";
    public static final String PROPERTY_KEY_RECONNECT_INTERVAL_MS = "reconnectIntervalMs";
    public static final String PROPERTY_KEY_RECONNECT_RETRY_COUNT = "reconnectRetryCount";

    public static final String PROPERTY_KEY_DISABLE_SSL_CERT_VALIDATION = "disableSSLCertValidation";
    public static final String PROPERTY_KEY_APP_IP = "app_ip";
    public static final String PROPERTY_KEY_APP_NAME = "app_name";

    /**
     * for efficient writing
     */
    public static final String PROPERTY_KEY_COPY_DATA = "copyData";
    public static final String PROPERTY_KEY_BATCH_SIZE_BY_ROW = "batchSizeByRow";
    public static final String PROPERTY_KEY_CACHE_SIZE_BY_ROW = "cacheSizeByRow";
    public static final String PROPERTY_KEY_BACKEND_WRITE_THREAD_NUM = "backendWriteThreadNum";
    public static final String PROPERTY_KEY_STRICT_CHECK = "strictCheck";
    public static final String PROPERTY_KEY_RETRY_TIMES = "retryTimes";
    public static final String PROPERTY_KEY_ASYNC_WRITE = "asyncWrite";

    /**
     * for health check
     */
    public static final String PROPERTY_KEY_HEALTH_CHECK_INIT_INTERVAL = "healthCheckInitInterval";
    public static final String PROPERTY_KEY_HEALTH_CHECK_MAX_INTERVAL = "healthCheckMaxInterval";
    public static final String PROPERTY_KEY_HEALTH_CHECK_CON_TIMEOUT = "healthCheckConTimeout";
    public static final String PROPERTY_KEY_HEALTH_CHECK_CMD_TIMEOUT = "healthCheckCmdTimeout";
    public static final String PROPERTY_KEY_HEALTH_CHECK_RECOVERY_COUNT = "healthCheckRecoveryCount";
    public static final String PROPERTY_KEY_HEALTH_CHECK_RECOVERY_INTERVAL = "healthCheckRecoveryInterval";
    public static final String PROPERTY_KEY_REBALANCE_THRESHOLD = "rebalanceThreshold";
    public static final String PROPERTY_KEY_REBALANCE_CON_BASE_COUNT = "rebalanceConBaseCount";

    /**
     * for stmt bind mode
     */
    public static final String PROPERTY_KEY_PBS_MODE = "pbsMode";

    /**
     * max message number send to server concurrently
     */
    public static final String PROPERTY_KEY_MAX_CONCURRENT_REQUEST = "maxConcurrentRequest";
    /**
     * max pool size
     */
    public static final String HTTP_POOL_SIZE = "httpPoolSize";

    public static final String HTTP_KEEP_ALIVE = "httpKeepAlive";

    /**
     * the timeout in milliseconds until a connection is established.
     * zero is interpreted as an infinite timeout.
     */
    public static final String HTTP_CONNECT_TIMEOUT = "httpConnectTimeout";

    /**
     * Defines the socket timeout in milliseconds,
     * which is the timeout for waiting for data or, put differently,
     * a maximum period inactivity between two consecutive data packets.
     * zero is interpreted as an infinite timeout.
     */
    public static final String HTTP_SOCKET_TIMEOUT = "httpSocketTimeout";

    private TSDBDatabaseMetaData dbMetaData = null;

    static {
        try {
            DriverManager.registerDriver(new TSDBDriver());
        } catch (SQLException e) {
            throw TSDBError.createRuntimeException(TSDBErrorNumbers.ERROR_CANNOT_REGISTER_JNI_DRIVER, e);
        }
    }

    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url))
            return null;

        Properties props = parseURL(url, info);
        if (props == null) {
            return null;
        }

        if (!props.containsKey(TSDBDriver.PROPERTY_KEY_USER))
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_USER_IS_REQUIRED);
        if (!props.containsKey(TSDBDriver.PROPERTY_KEY_PASSWORD))
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PASSWORD_IS_REQUIRED);

        try {
            TSDBJNIConnector.init(props);
            return new TSDBConnection(props, this.dbMetaData);
        } catch (SQLWarning sqlWarning) {
            return new TSDBConnection(props, this.dbMetaData);
        } catch (SQLException sqlEx) {
            throw sqlEx;
        } catch (Exception ex) {
            throw new SQLException("SQLException:" + ex, ex);
        }
    }

    /**
     * @param url the URL of the database
     * @return <code>true</code> if this driver understands the given URL;
     * <code>false</code> otherwise
     * @throws SQLException if a database access error occurs or the url is {@code null}
     */
    public boolean acceptsURL(String url) throws SQLException {
        if (url == null)
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_URL_NOT_SET);
        return url.trim().length() > 0 && (url.startsWith(URL_PREFIX) || url.startsWith(URL_PREFIX1));
    }

    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        if (info == null) {
            info = new Properties();
        }

        if (acceptsURL(url)) {
            info = parseURL(url, info);
        }

        return getPropertyInfo(info);
    }

    /**
     * example: jdbc:TAOS://127.0.0.1:0/db?user=user_abc&password=your_password
     */
    public Properties parseURL(String url, Properties defaults) throws SQLException {
        if (url == null || url.length() <= 0 || url.trim().length() <= 0)
            return new Properties();
        if (!url.startsWith(URL_PREFIX) && !url.startsWith(URL_PREFIX1))
            return new Properties();

        // parse properties
        Properties urlProps = StringUtils.parseUrl(url, defaults);
        this.dbMetaData = new TSDBDatabaseMetaData(url, urlProps.getProperty(TSDBDriver.PROPERTY_KEY_USER));
        return urlProps;
    }

    public int getMajorVersion() {
        return 3;
    }

    public int getMinorVersion() {
        return 0;
    }

    public boolean jdbcCompliant() {
        return false;
    }

    public Logger getParentLogger() {
        return null;
    }

}
