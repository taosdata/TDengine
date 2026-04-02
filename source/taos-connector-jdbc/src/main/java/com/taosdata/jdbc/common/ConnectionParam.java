package com.taosdata.jdbc.common;

import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.utils.HttpClientPoolUtil;
import com.taosdata.jdbc.utils.StringUtils;
import com.taosdata.jdbc.utils.Utils;
import io.netty.buffer.ByteBuf;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

public class ConnectionParam {
    private List<Endpoint> endpoints;
    private String database;
    private String cloudToken;
    private String user;
    private String password;
    private String bearerToken;
    private String tz;
    private ZoneId zoneId;
    private boolean useSsl;
    private int maxRequest;
    private int connectTimeout;
    private int requestTimeout;
    private int connectMode;
    private final boolean varcharAsString;
    private boolean enableCompression;
    private boolean enableAutoConnect;

    private String slaveClusterHost;
    private int slaveClusterPort;
    private int reconnectIntervalMs;
    private int reconnectRetryCount;
    private boolean disableSslCertValidation;
    private String appName;
    private String appIp;
    private boolean copyData;
    private int batchSizeByRow;
    private int cacheSizeByRow;
    private int backendWriteThreadNum;
    private boolean strictCheck;
    private int retryTimes;
    private String asyncWrite;
    private String pbsMode;
    private int wsKeepAlive;
    private int healthCheckInitInterval;
    private int healthCheckMaxInterval;
    private int healthCheckConTimeout;
    private int healthCheckCmdTimeout;
    private int healthCheckRecoveryCount;
    private int healthCheckRecoveryInterval;
    private int rebalanceThreshold;
    private int rebalanceConBaseCount;
    private Consumer<String> textMessageHandler;
    private Consumer<ByteBuf> binaryMessageHandler;
    static public final int CONNECT_MODE_BI = 1;

    private ConnectionParam(Builder builder) {
        this.endpoints = builder.endpoints;
        this.database = builder.database;
        this.cloudToken = builder.cloudToken;
        this.user = builder.user;
        this.password = builder.password;
        this.bearerToken = builder.bearerToken;
        this.tz = builder.tz;
        this.useSsl = builder.useSsl;
        this.maxRequest = builder.maxRequest;
        this.connectTimeout = builder.connectTimeout;
        this.requestTimeout = builder.requestTimeout;
        this.connectMode = builder.connectMode;
        this.varcharAsString = builder.varcharAsString;
        this.enableCompression = builder.enableCompression;
        this.slaveClusterHost = builder.slaveClusterHost;
        this.slaveClusterPort = builder.slaveClusterPort;
        this.reconnectIntervalMs = builder.reconnectIntervalMs;
        this.reconnectRetryCount = builder.reconnectRetryCount;
        this.enableAutoConnect = builder.enableAutoReconnect;
        this.disableSslCertValidation = builder.disableSslCertValidation;
        this.appName = builder.appName;
        this.appIp = builder.appIp;
        this.copyData = builder.copyData;
        this.batchSizeByRow = builder.batchSizeByRow;
        this.cacheSizeByRow = builder.cacheSizeByRow;
        this.backendWriteThreadNum = builder.backendWriteThreadNum;
        this.strictCheck = builder.strictCheck;
        this.retryTimes = builder.retryTimes;
        this.asyncWrite = builder.asyncWrite;
        this.textMessageHandler = builder.textMessageHandler;
        this.binaryMessageHandler = builder.binaryMessageHandler;
        this.pbsMode = builder.pbsMode;
        this.wsKeepAlive = builder.wsKeepAlive;
        this.healthCheckInitInterval = builder.healthCheckInitInterval;
        this.healthCheckMaxInterval = builder.healthCheckMaxInterval;
        this.healthCheckConTimeout = builder.healthCheckConTimeout;
        this.healthCheckCmdTimeout = builder.healthCheckCmdTimeout;
        this.healthCheckRecoveryCount = builder.healthCheckRecoveryCount;
        this.healthCheckRecoveryInterval = builder.healthCheckRecoveryInterval;
        this.rebalanceThreshold = builder.rebalanceThreshold;
        this.rebalanceConBaseCount = builder.rebalanceConBaseCount;
    }

    public void setEndpoints(List<Endpoint> endpoints) {
        this.endpoints = endpoints;
    }
    public void setDatabase(String database) {
        this.database = database;
    }

    public void setCloudToken(String cloudToken) {
        this.cloudToken = cloudToken;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setPassword(String password) {
        this.password = password;
    }
    public void setBearerToken(String bearerToken) {
        this.bearerToken = bearerToken;
    }

    public void setTz(String tz) {
        this.tz = tz;
    }

    public void setZoneId(ZoneId zoneId) {
        this.zoneId = zoneId;
    }

    public void setUseSsl(boolean useSsl) {
        this.useSsl = useSsl;
    }

    public void setMaxRequest(int maxRequest) {
        this.maxRequest = maxRequest;
    }

    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public void setRequestTimeout(int requestTimeout) {
        this.requestTimeout = requestTimeout;
    }

    public void setConnectMode(int connectMode) {
        this.connectMode = connectMode;
    }

    public void setEnableCompression(boolean enableCompression) {
        this.enableCompression = enableCompression;
    }

    public void setEnableAutoConnect(boolean enableAutoConnect) {
        this.enableAutoConnect = enableAutoConnect;
    }

    public void setSlaveClusterHost(String slaveClusterHost) {
        this.slaveClusterHost = slaveClusterHost;
    }

    public void setSlaveClusterPort(int slaveClusterPort) {
        this.slaveClusterPort = slaveClusterPort;
    }

    public void setReconnectIntervalMs(int reconnectIntervalMs) {
        this.reconnectIntervalMs = reconnectIntervalMs;
    }

    public void setReconnectRetryCount(int reconnectRetryCount) {
        this.reconnectRetryCount = reconnectRetryCount;
    }

    public void setDisableSslCertValidation(boolean disableSslCertValidation) {
        this.disableSslCertValidation = disableSslCertValidation;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public void setAppIp(String appIp) {
        this.appIp = appIp;
    }

    public void setCopyData(boolean copyData) {
        this.copyData = copyData;
    }

    public void setBatchSizeByRow(int batchSizeByRow) {
        this.batchSizeByRow = batchSizeByRow;
    }

    public void setCacheSizeByRow(int cacheSizeByRow) {
        this.cacheSizeByRow = cacheSizeByRow;
    }

    public void setBackendWriteThreadNum(int backendWriteThreadNum) {
        this.backendWriteThreadNum = backendWriteThreadNum;
    }

    public void setStrictCheck(boolean strictCheck) {
        this.strictCheck = strictCheck;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }

    public void setAsyncWrite(String asyncWrite) {
        this.asyncWrite = asyncWrite;
    }

    public void setPbsMode(String pbsMode) {
        this.pbsMode = pbsMode;
    }
    public void setWsKeepAlive(int wsKeepAlive) {
        this.wsKeepAlive = wsKeepAlive;
    }

    public void setHealthCheckInitInterval(int healthCheckInitInterval) {
        this.healthCheckInitInterval = healthCheckInitInterval;
    }

    public void setHealthCheckMaxInterval(int healthCheckMaxInterval) {
        this.healthCheckMaxInterval = healthCheckMaxInterval;
    }

    public void setHealthCheckConTimeout(int healthCheckConTimeout) {
        this.healthCheckConTimeout = healthCheckConTimeout;
    }

    public void setHealthCheckCmdTimeout(int healthCheckCmdTimeout) {
        this.healthCheckCmdTimeout = healthCheckCmdTimeout;
    }

    public void setHealthCheckRecoveryCount(int healthCheckRecoveryCount) {
        this.healthCheckRecoveryCount = healthCheckRecoveryCount;
    }

    public void setHealthCheckRecoveryInterval(int healthCheckRecoveryInterval) {
        this.healthCheckRecoveryInterval = healthCheckRecoveryInterval;
    }

    public void setRebalanceThreshold(int rebalanceThreshold) {
        this.rebalanceThreshold = rebalanceThreshold;
    }

    public void setRebalanceConBaseCount(int rebalanceConBaseCount) {
        this.rebalanceConBaseCount = rebalanceConBaseCount;
    }
    public List<Endpoint> getEndpoints() {
        return endpoints;
    }
    public String getDatabase() {
        return database;
    }
    public String getCloudToken() {
        return cloudToken;
    }

    public String getUser() {
        return user;
    }
    public String getPassword() {
        return password;
    }
    public String getBearerToken() {
        return bearerToken;
    }
    public String getTz() {
        return tz;
    }
    public ZoneId getZoneId() {
        return zoneId;
    }
    public boolean isUseSsl() {
        return useSsl;
    }
    public int getMaxRequest() {
        return maxRequest;
    }
    public int getConnectTimeout() {
        return connectTimeout;
    }
    public int getRequestTimeout() {
        return requestTimeout;
    }
    public int getConnectMode() {
        return connectMode;
    }
    public boolean isVarcharAsString() {
        return varcharAsString;
    }
    public boolean isEnableCompression() {
        return enableCompression;
    }
    public String getSlaveClusterHost() {
        return slaveClusterHost;
    }
    public int getSlaveClusterPort() {
        return slaveClusterPort;
    }
    public int getReconnectIntervalMs() {
        return reconnectIntervalMs;
    }
    public int getReconnectRetryCount() {
        return reconnectRetryCount;
    }
    public boolean isEnableAutoConnect() {
        return enableAutoConnect;
    }

    public boolean isDisableSslCertValidation() {
        return disableSslCertValidation;
    }

    public String getAppName() {
        return appName;
    }
    public String getAppIp() {
        return appIp;
    }
    public boolean isCopyData() {
        return copyData;
    }

    public int getBatchSizeByRow() {
        return batchSizeByRow;
    }

    public int getCacheSizeByRow() {
        return cacheSizeByRow;
    }

    public int getBackendWriteThreadNum() {
        return backendWriteThreadNum;
    }

    public boolean isStrictCheck() {
        return strictCheck;
    }

    public int getRetryTimes() {
        return retryTimes;
    }

    public String getAsyncWrite() {
        return asyncWrite;
    }

    public String getPbsMode() {
        return pbsMode;
    }
    public int getWsKeepAlive() {
        return wsKeepAlive;
    }
    public int getHealthCheckInitInterval() {
        return healthCheckInitInterval;
    }

    public int getHealthCheckMaxInterval() {
        return healthCheckMaxInterval;
    }

    public int getHealthCheckConTimeout() {
        return healthCheckConTimeout;
    }

    public int getHealthCheckCmdTimeout() {
        return healthCheckCmdTimeout;
    }

    public int getHealthCheckRecoveryCount() {
        return healthCheckRecoveryCount;
    }

    public int getHealthCheckRecoveryInterval() {
        return healthCheckRecoveryInterval;
    }

    public int getRebalanceThreshold() {
        return rebalanceThreshold;
    }

    public int getRebalanceConBaseCount() {
        return rebalanceConBaseCount;
    }
    public Consumer<String> getTextMessageHandler() {
        return textMessageHandler;
    }

    public void setTextMessageHandler(Consumer<String> textMessageHandler) {
        this.textMessageHandler = textMessageHandler;
    }

    public Consumer<ByteBuf> getBinaryMessageHandler() {
        return binaryMessageHandler;
    }

    public void setBinaryMessageHandler(Consumer<ByteBuf> binaryMessageHandler) {
        this.binaryMessageHandler = binaryMessageHandler;
    }

    public static ConnectionParam getParamWs(Properties perperties) throws SQLException {
        ConnectionParam connectionParam = getParam(perperties);
        if (connectionParam.getTz() == null
                || connectionParam.getTz().contains("+")
                || connectionParam.getTz().contains("-")
                || !connectionParam.getTz().contains("/")){
            // for history reason, we will not support time zone with offset in websocket connection
            connectionParam.setTz("");
            return connectionParam;
        }

        try {
            ZoneId zoneId = ZoneId.of(connectionParam.getTz());
            ZoneId defaultZoneId = ZoneId.systemDefault();
            if (!defaultZoneId.equals(zoneId)){
                //  Only set the time zone if it differs from the system default
                connectionParam.setZoneId(zoneId);
            }
        } catch (DateTimeException e) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "invalid time zone");
        }
        return connectionParam;
    }

    public static ConnectionParam getParam(Properties properties) throws SQLException {
        String host = properties.getProperty(TSDBDriver.PROPERTY_KEY_HOST);
        String port = properties.getProperty(TSDBDriver.PROPERTY_KEY_PORT);
        String endpointStr = properties.getProperty(TSDBDriver.PROPERTY_KEY_ENDPOINTS);
        List<Endpoint> endpoints;

        if (host != null || port != null) {
            endpoints = new ArrayList<>();
            boolean isIpv6 = host != null && host.startsWith("[") && host.endsWith("]");
            endpoints.add(new Endpoint(host, port == null ? 0 : Integer.parseInt(port), isIpv6));
        } else {
            endpoints = StringUtils.parseEndpoints(endpointStr);
        }

        String database = properties.containsKey(TSDBDriver.PROPERTY_KEY_DBNAME)
                ? properties.getProperty(TSDBDriver.PROPERTY_KEY_DBNAME)
                : null;

        String cloudToken = null;
        if (properties.containsKey(TSDBDriver.PROPERTY_KEY_TOKEN)) {
            cloudToken = properties.getProperty(TSDBDriver.PROPERTY_KEY_TOKEN);
        }

        String bearerToken = null;
        if (properties.containsKey(TSDBDriver.PROPERTY_KEY_BEARER_TOKEN)) {
            bearerToken = properties.getProperty(TSDBDriver.PROPERTY_KEY_BEARER_TOKEN);
        }

        String user = properties.getProperty(TSDBDriver.PROPERTY_KEY_USER);
        String password = properties.getProperty(TSDBDriver.PROPERTY_KEY_PASSWORD);

        if (cloudToken == null && bearerToken == null) {
            if (user == null) {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_USER_IS_REQUIRED);
            }
            if (password == null) {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_PASSWORD_IS_REQUIRED);
            }

            try {
                user = URLDecoder.decode(user, StandardCharsets.UTF_8.displayName());
                password = URLDecoder.decode(password, StandardCharsets.UTF_8.displayName());
            } catch (UnsupportedEncodingException e) {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE,
                        "unsupported UTF-8 concoding, user: " + properties.getProperty(TSDBDriver.PROPERTY_KEY_USER)
                                + ", password: " + properties.getProperty(TSDBDriver.PROPERTY_KEY_PASSWORD));
            }
        }

        String tz = properties.getProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE);

        boolean useSsl = Boolean.parseBoolean(properties.getProperty(TSDBDriver.PROPERTY_KEY_USE_SSL, "false"));

        int maxRequest = Integer
                .parseInt(properties.getProperty(TSDBDriver.HTTP_POOL_SIZE, HttpClientPoolUtil.DEFAULT_MAX_PER_ROUTE));
        int connectTimeout = Integer.parseInt(
                properties.getProperty(TSDBDriver.HTTP_CONNECT_TIMEOUT, HttpClientPoolUtil.DEFAULT_CONNECT_TIMEOUT));

        int requestTimeout = Integer.parseInt(properties.getProperty(TSDBDriver.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT,
                String.valueOf(TSDBConstants.DEFAULT_MESSAGE_WAIT_TIMEOUT)));

        int connectMode = Integer.parseInt(properties.getProperty(TSDBDriver.PROPERTY_KEY_CONNECT_MODE,"0"));
        if (connectMode < 0 || connectMode > 1){
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "unsupported connect mode");
        }

        boolean varcharAsString = Boolean.parseBoolean(properties.getProperty(TSDBDriver.PROPERTY_KEY_VARCHAR_AS_STRING, "false"));

        String slaveClusterHost = properties.getProperty(TSDBDriver.PROPERTY_KEY_SLAVE_CLUSTER_HOST, "");
        int slaveClusterPort = Integer.parseInt(properties.getProperty(TSDBDriver.PROPERTY_KEY_SLAVE_CLUSTER_PORT, "6041"));
        if (slaveClusterPort <= 0){
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "slave port must be positive integer");
        }

        if (!StringUtils.isEmpty(slaveClusterHost) && endpoints.size() != 1){
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "load balancing and slave cluster can not be set at the same time");
        }

        if (!StringUtils.isEmpty(slaveClusterHost)) {
            endpoints.add(new Endpoint(slaveClusterHost, slaveClusterPort, slaveClusterHost.startsWith("[") && slaveClusterHost.endsWith("]")));
        }

        int reconnectIntervalMs  = Integer
                .parseInt(properties.getProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_INTERVAL_MS, "2000"));
        if (reconnectIntervalMs < 0){
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "invalid para PROPERTY_KEY_RECONNECT_INTERVAL_MS");
        }

        int reconnectRetryCount = Integer
                .parseInt(properties.getProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_RETRY_COUNT, "3"));
        if (reconnectRetryCount < 0){
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "invalid para PROPERTY_KEY_RECONNECT_RETRY_COUNT");
        }

        boolean enableCompression = Boolean.parseBoolean(properties.getProperty(TSDBDriver.PROPERTY_KEY_ENABLE_COMPRESSION,"false"));
        boolean enableAutoReconnect = Boolean.parseBoolean(properties.getProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT,"false"));
        boolean disableSslCertValidation = Boolean.parseBoolean(properties.getProperty(TSDBDriver.PROPERTY_KEY_DISABLE_SSL_CERT_VALIDATION,"false"));

        String appName = properties.getProperty(TSDBDriver.PROPERTY_KEY_APP_NAME, "java");
        if (appName.getBytes().length > 23){
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "invalid app name, max length is 23");
        }

        String appIp = properties.getProperty(TSDBDriver.PROPERTY_KEY_APP_IP, "");
        if (!Utils.isValidIP(appIp)){
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "invalid app ip address");
        }

        boolean copyData = Boolean.parseBoolean(properties.getProperty(TSDBDriver.PROPERTY_KEY_COPY_DATA, "false"));

        int batchSizeByRow = Integer.parseInt(properties.getProperty(TSDBDriver.PROPERTY_KEY_BATCH_SIZE_BY_ROW, "1000"));
        if (batchSizeByRow < 0){
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "invalid para PROPERTY_KEY_BATCH_SIZE_BY_ROW");
        }

        int cacheSizeByRow = Integer.parseInt(properties.getProperty(TSDBDriver.PROPERTY_KEY_CACHE_SIZE_BY_ROW, "10000"));
        if (cacheSizeByRow < 0){
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "invalid para PROPERTY_KEY_CACHE_SIZE_BY_ROW");
        }

        if (batchSizeByRow > cacheSizeByRow || cacheSizeByRow % batchSizeByRow != 0){
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "PROPERTY_KEY_CACHE_SIZE_BY_ROW must be an integer multiple of PROPERTY_KEY_BATCH_SIZE_BY_ROW");
        }

        int backendWriteThreadNum = Integer.parseInt(properties.getProperty(TSDBDriver.PROPERTY_KEY_BACKEND_WRITE_THREAD_NUM, "10"));
        if (backendWriteThreadNum < 0){
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "invalid para PROPERTY_KEY_BACKEND_WRITE_THREAD_NUM");
        }

        boolean strictCheck = Boolean.parseBoolean(properties.getProperty(TSDBDriver.PROPERTY_KEY_STRICT_CHECK, "false"));
        int retryTimes = Integer.parseInt(properties.getProperty(TSDBDriver.PROPERTY_KEY_RETRY_TIMES, "3"));
        if (retryTimes < 0){
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "invalid para PROPERTY_KEY_RETRY_TIMES");
        }

        String asyncWrite = properties.getProperty(TSDBDriver.PROPERTY_KEY_ASYNC_WRITE, "");
        if (!asyncWrite.equals("") && !asyncWrite.equalsIgnoreCase("STMT")){
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "PROPERTY_KEY_ASYNC_WRITE only support STMT");
        }

        String pbsMode = properties.getProperty(TSDBDriver.PROPERTY_KEY_PBS_MODE, "");
        if (!pbsMode.equals("") && !pbsMode.equalsIgnoreCase("line")){
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "PROPERTY_KEY_PBS_MODE only support line");
        }

        int wsKeepAlive = Integer.parseInt(properties.getProperty(TSDBDriver.PROPERTY_KEY_WS_KEEP_ALIVE_SECONDS, "300"));
        if (wsKeepAlive <= 0){
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "invalid para PROPERTY_KEY_WS_KEEP_ALIVE_SECONDS");
        }

        int healthCheckInitInterval = Integer.parseInt(properties.getProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_INIT_INTERVAL, "10"));
        if (healthCheckInitInterval <= 0){
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "invalid para PROPERTY_KEY_HEALTH_CHECK_INIT_INTERVAL, must be positive integer");
        }

        int healthCheckMaxInterval = Integer.parseInt(properties.getProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_MAX_INTERVAL, "300"));
        if (healthCheckMaxInterval < healthCheckInitInterval){
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "invalid para PROPERTY_KEY_HEALTH_CHECK_MAX_INTERVAL, must be positive integer and greater than PROPERTY_KEY_HEALTH_CHECK_INIT_INTERVAL");
        }
        int healthCheckConTimeout = Integer.parseInt(properties.getProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_CON_TIMEOUT, "1"));
        if (healthCheckConTimeout <= 0){
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "invalid para PROPERTY_KEY_HEALTH_CHECK_CON_TIMEOUT, must be positive integer");
        }

        int healthCheckCmdTimeout = Integer.parseInt(properties.getProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_CMD_TIMEOUT, "5"));
        if (healthCheckCmdTimeout <= 0){
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "invalid para PROPERTY_KEY_HEALTH_CHECK_CMD_TIMEOUT, must be positive integer");
        }

        int healthCheckRecoveryCount = Integer.parseInt(properties.getProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_RECOVERY_COUNT, "3"));
        if (healthCheckRecoveryCount <= 0){
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "invalid para PROPERTY_KEY_HEALTH_CHECK_RECOVERY_COUNT, must be positive integer");
        }

        int healthCheckRecoveryInterval = Integer.parseInt(properties.getProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_RECOVERY_INTERVAL, "60"));
        if (healthCheckRecoveryInterval <= 0){
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "invalid para PROPERTY_KEY_HEALTH_CHECK_RECOVERY_INTERVAL, must be positive integer");
        }

        int rebalanceThreshold = Integer.parseInt(properties.getProperty(TSDBDriver.PROPERTY_KEY_REBALANCE_THRESHOLD, "20"));
        if (rebalanceThreshold < 10 || rebalanceThreshold > 50){
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "invalid para PROPERTY_KEY_REBALANCE_THRESHOLD, must be between 10 and 50");
        }
        int rebalanceConBaseCount = Integer.parseInt(properties.getProperty(TSDBDriver.PROPERTY_KEY_REBALANCE_CON_BASE_COUNT, "30"));
        if (rebalanceConBaseCount <= 0){
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, "invalid para PROPERTY_KEY_REBALANCE_CON_BASE_COUNT, must be positive integer");
        }

        return new Builder(endpoints)
                .setDatabase(database)
                .setCloudToken(cloudToken)
                .setUserAndPassword(user, password)
                .setBearerToken(bearerToken)
                .setTimeZone(tz)
                .setUseSsl(useSsl)
                .setMaxRequest(maxRequest)
                .setConnectionTimeout(connectTimeout)
                .setRequestTimeout(requestTimeout)
                .setConnectMode(connectMode)
                .setVarcharAsString(varcharAsString)
                .setEnableCompression(enableCompression)
                .setSlaveClusterHost(slaveClusterHost)
                .setSlaveClusterPort(slaveClusterPort)
                .setReconnectIntervalMs(reconnectIntervalMs)
                .setReconnectRetryCount(reconnectRetryCount)
                .setEnableAutoReconnect(enableAutoReconnect)
                .setDisableSslCertValidation(disableSslCertValidation)
                .setAppIp(appIp)
                .setAppName(appName)
                .setCopyData(copyData)
                .setBatchSizeByRow(batchSizeByRow)
                .setCacheSizeByRow(cacheSizeByRow)
                .setBackendWriteThreadNum(backendWriteThreadNum)
                .setStrictCheck(strictCheck)
                .setRetryTimes(retryTimes)
                .setAsyncWrite(asyncWrite)
                .setPbsMode(pbsMode)
                .setWsKeepAlive(wsKeepAlive)
                .setHealthCheckInitInterval(healthCheckInitInterval)
                .setHealthCheckMaxInterval(healthCheckMaxInterval)
                .setHealthCheckConTimeout(healthCheckConTimeout)
                .setHealthCheckCmdTimeout(healthCheckCmdTimeout)
                .setHealthCheckRecoveryCount(healthCheckRecoveryCount)
                .setHealthCheckRecoveryInterval(healthCheckRecoveryInterval)
                .setRebalanceThreshold(rebalanceThreshold)
                .setRebalanceConBaseCount(rebalanceConBaseCount)
                .build();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("ConnectionParam{");
        sb.append("endpoints=").append(endpoints);
        sb.append(", database='").append(database).append('\'');
        sb.append(", user='").append(user).append('\'');
        sb.append(", password='******'");
        if (cloudToken != null && !cloudToken.isEmpty()) {
            sb.append(", cloudToken='******'");
        }
        if (bearerToken != null && !bearerToken.isEmpty()) {
            sb.append(", bearerToken='******'");
        }
        sb.append(", tz='").append(tz).append('\'');
        sb.append(", useSsl=").append(useSsl);
        sb.append(", maxRequest=").append(maxRequest);
        sb.append(", connectTimeout=").append(connectTimeout);
        sb.append(", requestTimeout=").append(requestTimeout);
        sb.append(", connectMode=").append(connectMode);
        sb.append(", varcharAsString=").append(varcharAsString);
        sb.append(", enableCompression=").append(enableCompression);
        sb.append(", enableAutoConnect=").append(enableAutoConnect);
        sb.append(", slaveClusterHost='").append(slaveClusterHost).append('\'');
        sb.append(", slaveClusterPort=").append(slaveClusterPort);
        sb.append(", reconnectIntervalMs=").append(reconnectIntervalMs);
        sb.append(", reconnectRetryCount=").append(reconnectRetryCount);
        sb.append(", disableSslCertValidation=").append(disableSslCertValidation);
        sb.append(", appName='").append(appName).append('\'');
        sb.append(", appIp='").append(appIp).append('\'');
        sb.append(", copyData=").append(copyData);
        sb.append(", batchSizeByRow=").append(batchSizeByRow);
        sb.append(", cacheSizeByRow=").append(cacheSizeByRow);
        sb.append(", backendWriteThreadNum=").append(backendWriteThreadNum);
        sb.append(", strictCheck=").append(strictCheck);
        sb.append(", retryTimes=").append(retryTimes);
        sb.append(", asyncWrite='").append(asyncWrite).append('\'');
        sb.append(", pbsMode='").append(pbsMode).append('\'');
        sb.append(", wsKeepAlive=").append(wsKeepAlive);
        sb.append(", healthCheckInitInterval=").append(healthCheckInitInterval);
        sb.append(", healthCheckMaxInterval=").append(healthCheckMaxInterval);
        sb.append(", healthCheckConTimeout=").append(healthCheckConTimeout);
        sb.append(", healthCheckCmdTimeout=").append(healthCheckCmdTimeout);
        sb.append(", healthCheckRecoveryCount=").append(healthCheckRecoveryCount);
        sb.append(", healthCheckRecoveryInterval=").append(healthCheckRecoveryInterval);
        sb.append(", rebalanceThreshold=").append(rebalanceThreshold);
        sb.append(", rebalanceConBaseCount=").append(rebalanceConBaseCount);
        sb.append('}');
        return sb.toString();
    }

    public static class Builder {
        private final List<Endpoint> endpoints;
        private String database;
        private String cloudToken;
        private String user;
        private String password;
        private String bearerToken;
        private String tz;
        private boolean useSsl;
        private int maxRequest;
        private int connectTimeout;
        private int requestTimeout;
        private int connectMode;
        private boolean varcharAsString;

        private boolean enableCompression;
        private boolean enableAutoReconnect;
        private String slaveClusterHost;
        private int slaveClusterPort;
        private int reconnectIntervalMs;
        private int reconnectRetryCount;
        private boolean disableSslCertValidation;

        private String appName;
        private String appIp;
        private boolean copyData;
        private int batchSizeByRow;
        private int cacheSizeByRow;
        private int backendWriteThreadNum;
        private boolean strictCheck;
        private int retryTimes;
        private String asyncWrite;
        private String pbsMode;
        private int wsKeepAlive;
        private int healthCheckInitInterval;
        private int healthCheckMaxInterval;
        private int healthCheckConTimeout;
        private int healthCheckCmdTimeout;
        private int healthCheckRecoveryCount;
        private int healthCheckRecoveryInterval;
        private int rebalanceThreshold;
        private int rebalanceConBaseCount;
        private Consumer<String> textMessageHandler;
        private Consumer<ByteBuf> binaryMessageHandler;

        public Builder(List<Endpoint> endpoints) {
            this.endpoints = endpoints;
        }

        public Builder setDatabase(String database) {
            this.database = database;
            return this;
        }

        public Builder setCloudToken(String cloudToken) {
            this.cloudToken = cloudToken;
            return this;
        }

        public Builder setUserAndPassword(String user, String password) {
            this.user = user;
            this.password = password;
            return this;
        }

        public Builder setBearerToken(String bearerToken) {
            this.bearerToken = bearerToken;
            return this;
        }
        public Builder setTimeZone(String timeZone) {
            this.tz = timeZone;
            return this;
        }

        public Builder setUseSsl(boolean useSsl) {
            this.useSsl = useSsl;
            return this;
        }

        public Builder setMaxRequest(int maxRequest) {
            this.maxRequest = maxRequest;
            return this;
        }

        public Builder setConnectionTimeout(int connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        public Builder setRequestTimeout(int requestTimeout) {
            this.requestTimeout = requestTimeout;
            return this;
        }
        public Builder setConnectMode(int connectMode) {
            this.connectMode = connectMode;
            return this;
        }

        public Builder setVarcharAsString(boolean varcharAsString) {
            this.varcharAsString = varcharAsString;
            return this;
        }
        public Builder setEnableCompression(boolean enableCompression) {
            this.enableCompression = enableCompression;
            return this;
        }
        public Builder setEnableAutoReconnect(boolean enableAutoReconnect) {
            this.enableAutoReconnect = enableAutoReconnect;
            return this;
        }

        public Builder setSlaveClusterHost(String slaveClusterHost) {
            this.slaveClusterHost = slaveClusterHost;
            return this;
        }

        public Builder setSlaveClusterPort(int slaveClusterPort) {
            this.slaveClusterPort = slaveClusterPort;
            return this;
        }

        public Builder setReconnectIntervalMs(int reconnectIntervalMs) {
            this.reconnectIntervalMs = reconnectIntervalMs;
            return this;
        }

        public Builder setReconnectRetryCount(int reconnectRetryCount) {
            this.reconnectRetryCount = reconnectRetryCount;
            return this;
        }

        public Builder setDisableSslCertValidation(boolean disableSslCertValidation) {
            this.disableSslCertValidation = disableSslCertValidation;
            return this;
        }
        public Builder setAppName(String appName) {
            this.appName = appName;
            return this;
        }

        public Builder setAppIp(String appIp) {
            this.appIp = appIp;
            return this;
        }

        public Builder setCopyData(boolean copyData) {
            this.copyData = copyData;
            return this;
        }

        public Builder setBatchSizeByRow(int batchSizeByRow) {
            this.batchSizeByRow = batchSizeByRow;
            return this;
        }

        public Builder setCacheSizeByRow(int cacheSizeByRow) {
            this.cacheSizeByRow = cacheSizeByRow;
            return this;
        }

        public Builder setBackendWriteThreadNum(int backendWriteThreadNum) {
            this.backendWriteThreadNum = backendWriteThreadNum;
            return this;
        }

        public Builder setStrictCheck(boolean strictCheck) {
            this.strictCheck = strictCheck;
            return this;
        }

        public Builder setRetryTimes(int retryTimes) {
            this.retryTimes = retryTimes;
            return this;
        }

        public Builder setAsyncWrite(String asyncWrite) {
            this.asyncWrite = asyncWrite;
            return this;
        }

        public Builder setPbsMode(String pbsMode) {
            this.pbsMode = pbsMode;
            return this;
        }

        public Builder setWsKeepAlive(int wsKeepAlive) {
            this.wsKeepAlive = wsKeepAlive;
            return this;
        }

        public Builder setHealthCheckInitInterval(int healthCheckInitInterval) {
            this.healthCheckInitInterval = healthCheckInitInterval;
            return this;
        }

        public Builder setHealthCheckMaxInterval(int healthCheckMaxInterval) {
            this.healthCheckMaxInterval = healthCheckMaxInterval;
            return this;
        }

        public Builder setHealthCheckConTimeout(int healthCheckConTimeout) {
            this.healthCheckConTimeout = healthCheckConTimeout;
            return this;
        }

        public Builder setHealthCheckCmdTimeout(int healthCheckCmdTimeout) {
            this.healthCheckCmdTimeout = healthCheckCmdTimeout;
            return this;
        }

        public Builder setHealthCheckRecoveryCount(int healthCheckRecoveryCount) {
            this.healthCheckRecoveryCount = healthCheckRecoveryCount;
            return this;
        }

        public Builder setHealthCheckRecoveryInterval(int healthCheckRecoveryInterval) {
            this.healthCheckRecoveryInterval = healthCheckRecoveryInterval;
            return this;
        }

        public Builder setRebalanceThreshold(int rebalanceThreshold) {
            this.rebalanceThreshold = rebalanceThreshold;
            return this;
        }
        public Builder setRebalanceConBaseCount(int rebalanceConBaseCount) {
            this.rebalanceConBaseCount = rebalanceConBaseCount;
            return this;
        }
        public Builder setTextMessageHandler(Consumer<String> textMessageHandler) {
            this.textMessageHandler = textMessageHandler;
            return this;
        }

        public Builder setBinaryMessageHandler(Consumer<ByteBuf> binaryMessageHandler) {
            this.binaryMessageHandler = binaryMessageHandler;
            return this;
        }
        public ConnectionParam build() {
            return new ConnectionParam(this);
        }
    }

    public static ConnectionParam.Builder copyToBuilder(ConnectionParam original) {
        if (original == null) {
            return null;
        }

        return new ConnectionParam.Builder(original.getEndpoints()) // we can make sure endpoints will not be modified
                .setDatabase(original.getDatabase())
                .setCloudToken(original.getCloudToken())
                .setUserAndPassword(original.getUser(), original.getPassword())
                .setTimeZone(original.getTz())
                .setUseSsl(original.isUseSsl())
                .setMaxRequest(original.getMaxRequest())
                .setConnectionTimeout(original.getConnectTimeout())
                .setRequestTimeout(original.getRequestTimeout())
                .setConnectMode(original.getConnectMode())
                .setVarcharAsString(original.isVarcharAsString())
                .setEnableCompression(original.isEnableCompression())
                .setSlaveClusterHost(original.getSlaveClusterHost())
                .setSlaveClusterPort(original.getSlaveClusterPort())
                .setReconnectIntervalMs(original.getReconnectIntervalMs())
                .setReconnectRetryCount(original.getReconnectRetryCount())
                .setEnableAutoReconnect(original.isEnableAutoConnect())
                .setDisableSslCertValidation(original.isDisableSslCertValidation())
                .setAppName(original.getAppName())
                .setAppIp(original.getAppIp())
                .setCopyData(original.isCopyData())
                .setBatchSizeByRow(original.getBatchSizeByRow())
                .setCacheSizeByRow(original.getCacheSizeByRow())
                .setBackendWriteThreadNum(original.getBackendWriteThreadNum())
                .setStrictCheck(original.isStrictCheck())
                .setRetryTimes(original.getRetryTimes())
                .setAsyncWrite(original.getAsyncWrite())
                .setPbsMode(original.getPbsMode())
                .setWsKeepAlive(original.getWsKeepAlive())
                .setHealthCheckInitInterval(original.getHealthCheckInitInterval())
                .setHealthCheckMaxInterval(original.getHealthCheckMaxInterval())
                .setHealthCheckConTimeout(original.getHealthCheckConTimeout())
                .setHealthCheckCmdTimeout(original.getHealthCheckCmdTimeout())
                .setHealthCheckRecoveryCount(original.getHealthCheckRecoveryCount())
                .setHealthCheckRecoveryInterval(original.getHealthCheckRecoveryInterval())
                .setRebalanceThreshold(original.getRebalanceThreshold())
                .setRebalanceConBaseCount(original.getRebalanceConBaseCount())
                .setTextMessageHandler(original.getTextMessageHandler())
                .setBinaryMessageHandler(original.getBinaryMessageHandler());
    }
}
