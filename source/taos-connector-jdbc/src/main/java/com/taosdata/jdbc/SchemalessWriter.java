package com.taosdata.jdbc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.taosdata.jdbc.enums.ConnectionType;
import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;
import com.taosdata.jdbc.enums.WSFunction;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.rs.RestfulDriver;
import com.taosdata.jdbc.utils.HttpClientPoolUtil;
import com.taosdata.jdbc.utils.JsonUtil;
import com.taosdata.jdbc.utils.StringUtils;
import com.taosdata.jdbc.utils.Utils;
import com.taosdata.jdbc.ws.FutureResponse;
import com.taosdata.jdbc.ws.InFlightRequest;
import com.taosdata.jdbc.ws.Transport;
import com.taosdata.jdbc.ws.entity.Code;
import com.taosdata.jdbc.ws.entity.CommonResp;
import com.taosdata.jdbc.ws.entity.Request;
import com.taosdata.jdbc.ws.entity.Response;
import com.taosdata.jdbc.ws.schemaless.ConnReq;
import com.taosdata.jdbc.ws.schemaless.InsertReq;
import com.taosdata.jdbc.ws.schemaless.SchemalessAction;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @deprecated This class will be removed in future. Please use writer functions in connection object instead.
 */
@Deprecated
// will be removed in future, please use writer functions in connection object.
public class SchemalessWriter implements AutoCloseable {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(SchemalessWriter.class);

    // jni
    private TSDBJNIConnector connector;
    private boolean shouldClose = true;
    // websocket
    private Transport transport;
    private final AtomicLong insertId = new AtomicLong(0);

    // meta
    private String dbName;
    private ConnectionType type;

    public SchemalessWriter(Connection connection) throws SQLException {
        if (connection instanceof TSDBConnection) {
            this.type = ConnectionType.JNI;
            this.connector = ((TSDBConnection) connection).getConnector();
            this.shouldClose = false;
        } else {
            // use websocket schemaless insert through existing connection, url mast contain username password or cloudToken
            DatabaseMetaData metaData = connection.getMetaData();
            String url = metaData.getURL();
            String db = connection.getCatalog();
            init(url, null, null, null, db, null);
        }
    }

    public SchemalessWriter(Connection connection, String dbName) throws SQLException {
        if (connection instanceof TSDBConnection) {
            this.type = ConnectionType.JNI;
            this.connector = ((TSDBConnection) connection).getConnector();
            selectDB(connector, dbName);
            this.shouldClose = false;
        } else {
            // use websocket schemaless insert through existing connection, url mast contain username password or cloudToken
            DatabaseMetaData metaData = connection.getMetaData();
            String url = metaData.getURL();
            init(url, null, null, null, dbName, null);
        }
    }

    public SchemalessWriter(String url) throws SQLException {
        init(url, null, null, null, null, null);
    }

    public SchemalessWriter(String url, String cloudToken) throws SQLException {
        init(url, null, null, cloudToken, null, null);
    }

    public SchemalessWriter(String url, String username, String password) throws SQLException {
        init(url, username, password, null, null, null);
    }

    public SchemalessWriter(String url, String username, String password, String dbName) throws SQLException {
        init(url, username, password, null, dbName, null);
    }

    public SchemalessWriter(String url, String user, String password, String cloudToken, String dbName, Boolean useSSL) throws SQLException {
        init(url, user, password, cloudToken, dbName, useSSL);
    }

    private void init(String url, String user, String password, String cloudToken, String dbName, Boolean useSSL) throws SQLException {
        String t;
        Properties properties;

        if (url.startsWith(TSDBDriver.URL_PREFIX)) {
            t = "jni";
            properties = StringUtils.parseUrl(url, null);
        } else if (url.startsWith(RestfulDriver.URL_PREFIX)) {
            t = "ws";
            properties = StringUtils.parseUrl(url, null);
        } else {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "unknown url：" + url);
        }

        if (user != null)
            properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, user);
        if (password != null)
            properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, password);
        if (cloudToken != null)
            properties.setProperty(TSDBDriver.PROPERTY_KEY_TOKEN, cloudToken);
        if (dbName != null)
            properties.setProperty(TSDBDriver.PROPERTY_KEY_DBNAME, dbName);
        if (useSSL != null)
            properties.setProperty(TSDBDriver.PROPERTY_KEY_USE_SSL, String.valueOf(useSSL));
        ConnectionParam param = ConnectionParam.getParam(properties);

        this.init(param.getEndpoints().get(0).getHost(), String.valueOf(param.getEndpoints().get(0).getPort()), param.getUser(), param.getPassword(), param.getDatabase(), param.getCloudToken(), t, param.isUseSsl());
    }

    public SchemalessWriter(String host, String port, String cloudToken, String dbName, Boolean useSSL) throws SQLException {
        init(host, port, null, null, dbName, cloudToken, "ws", useSSL);
    }

    public SchemalessWriter(String host, String port, String user, String password, String dbName, String type) throws SQLException {
        init(host, port, user, password, dbName, null, type, false);
    }

    public SchemalessWriter(String host, String port, String user, String password, String dbName, String type, Boolean useSSL) throws SQLException {
        init(host, port, user, password, dbName, null, type, useSSL);
    }

    private void init(String host, String port, String user, String password, String dbName, String cloudToken, String type, boolean useSSL) throws SQLException {
        this.dbName = dbName;
        if (null == type || ConnectionType.JNI.getType().equalsIgnoreCase(type)) {
            this.type = ConnectionType.JNI;
            int p = Integer.parseInt(port);
            this.connector = new TSDBJNIConnector();
            this.connector.connect(host, p, dbName, user, password);
        } else if (ConnectionType.WEBSOCKET.getType().equalsIgnoreCase(type) || ConnectionType.WS.getType().equalsIgnoreCase(type)) {
            this.type = ConnectionType.WS;
            int timeout = Integer.parseInt(HttpClientPoolUtil.DEFAULT_SOCKET_TIMEOUT);
            int connectTime = Integer.parseInt(HttpClientPoolUtil.DEFAULT_CONNECT_TIMEOUT);
            ConnectionParam param = new ConnectionParam.Builder(Utils.getEndpoints(host, port))
                    .setUserAndPassword(user, password)
                    .setDatabase(dbName)
                    .setCloudToken(cloudToken)
                    .setConnectionTimeout(connectTime)
                    .setRequestTimeout(timeout)
                    .setUseSsl(useSSL)
                    .build();
            InFlightRequest inFlightRequest = new InFlightRequest(20);
            param.setTextMessageHandler(message -> {
                try {
                    JsonNode jsonObject = JsonUtil.getObjectReader().readTree(message);
                    ObjectReader actionReader = JsonUtil.getObjectReader(CommonResp.class);
                    Response response = actionReader.treeToValue(jsonObject, CommonResp.class);
                    FutureResponse remove = inFlightRequest.remove(response.getAction(), response.getReqId());
                    if (null != remove) {
                        remove.getFuture().complete(response);
                    }
                } catch (JsonProcessingException e) {
                    log.error("Error processing message", e);
                }
            });

            this.transport = new Transport(WSFunction.SCHEMALESS, param, inFlightRequest);



            transport.checkConnection(connectTime);

            ConnReq connectReq = new ConnReq();
            connectReq.setReqId(1);
            connectReq.setUser(param.getUser());
            connectReq.setPassword(param.getPassword());
            connectReq.setDb(dbName);
            CommonResp auth = (CommonResp) transport.send(new Request(SchemalessAction.CONN.getAction(), connectReq));

            if (Code.SUCCESS.getCode() != auth.getCode()) {
                transport.close();
                throw new SQLException("(0x" + Integer.toHexString(auth.getCode()) + "):" + "auth failure: " + auth.getMessage());
            }
        }
    }

    /**
     * batch schemaless lines write to db
     *
     * @param lines         schemaless lines
     * @param protocolType  schemaless type {@link SchemalessProtocolType}
     * @param timestampType Time precision {@link SchemalessTimestampType}
     * @throws SQLException execute exception
     */
    public void write(String[] lines, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType) throws SQLException {
        write(lines, protocolType, timestampType, null, null, null);
    }

    @Deprecated
    public void write(String[] lines, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType, String dbName, Integer ttl, Long reqId) throws SQLException {
        switch (type) {
            case JNI: {
                if (null != dbName && (this.dbName == null || !this.dbName.endsWith(dbName))) {
                    selectDB(connector, dbName);
                }
                if (null == ttl && null == reqId) {
                    connector.insertLines(lines, protocolType, timestampType);
                } else if (null == reqId) {
                    connector.insertLinesWithTtl(lines, protocolType, timestampType, ttl);
                } else if (null == ttl) {
                    connector.insertLinesWithReqId(lines, protocolType, timestampType, reqId);
                } else {
                    connector.insertLinesWithTtlAndReqId(lines, protocolType, timestampType, ttl, reqId);
                }
                break;
            }
            case WS: {
                for (String line : lines) {
                    InsertReq insertReq = new InsertReq();
                    insertReq.setReqId(insertId.getAndIncrement());
                    insertReq.setProtocol(protocolType.ordinal());
                    insertReq.setPrecision(timestampType.getType());
                    insertReq.setData(line);
                    if (ttl != null)
                        insertReq.setTtl(ttl);
                    if (reqId != null)
                        insertReq.setReqId(reqId);
                    CommonResp response = (CommonResp) transport.send(new Request(SchemalessAction.INSERT.getAction(), insertReq));
                    if (Code.SUCCESS.getCode() != response.getCode()) {
                        throw new SQLException("(0x" + Integer.toHexString(response.getCode()) + "):" + response.getMessage());
                    }
                }
                break;
            }
            default:
                // nothing
        }
    }

    public void write(String[] lines, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType, Integer ttl, Long reqId) throws SQLException {
        switch (type) {
            case JNI: {
                if (null == ttl && null == reqId) {
                    connector.insertLines(lines, protocolType, timestampType);
                } else if (null == reqId) {
                    connector.insertLinesWithTtl(lines, protocolType, timestampType, ttl);
                } else if (null == ttl) {
                    connector.insertLinesWithReqId(lines, protocolType, timestampType, reqId);
                } else {
                    connector.insertLinesWithTtlAndReqId(lines, protocolType, timestampType, ttl, reqId);
                }
                break;
            }
            case WS: {
                for (String line : lines) {
                    InsertReq insertReq = new InsertReq();
                    insertReq.setReqId(insertId.getAndIncrement());
                    insertReq.setProtocol(protocolType.ordinal());
                    insertReq.setPrecision(timestampType.getType());
                    insertReq.setData(line);
                    if (ttl != null)
                        insertReq.setTtl(ttl);
                    if (reqId != null)
                        insertReq.setReqId(reqId);
                    CommonResp response = (CommonResp) transport.send(new Request(SchemalessAction.INSERT.getAction(), insertReq));
                    if (Code.SUCCESS.getCode() != response.getCode()) {
                        throw new SQLException("0x" + Integer.toHexString(response.getCode()) + ":" + response.getMessage());
                    }
                }
                break;
            }
            default:
                // nothing
        }
    }

    /**
     * only one line writes to db
     *
     * @param line          schemaless line
     * @param protocolType  schemaless type {@link SchemalessProtocolType}
     * @param timestampType Time precision {@link SchemalessTimestampType}
     * @throws SQLException execute exception
     */
    public void write(String line, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType) throws SQLException {
        write(new String[]{line}, protocolType, timestampType);
    }

    /**
     * batch schemaless lines write to db with list
     *
     * @param lines         schemaless list
     * @param protocolType  schemaless type {@link SchemalessProtocolType}
     * @param timestampType Time precision {@link SchemalessTimestampType}
     * @throws SQLException execute exception
     */
    public void write(List<String> lines, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType) throws SQLException {
        String[] strings = lines.toArray(new String[0]);
        write(strings, protocolType, timestampType);
    }

    public int writeRaw(String line, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType) throws SQLException {
        return this.writeRaw(line, protocolType, timestampType, null, null, null);
    }

    @Deprecated
    public int writeRaw(String line, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType, String dbName, Integer ttl, Long reqId) throws SQLException {
        switch (type) {
            case JNI: {
                if (null != dbName && (this.dbName == null || !this.dbName.endsWith(dbName))) {
                    selectDB(connector, dbName);
                }
                if (null == ttl && null == reqId) {
                    return connector.insertRaw(line, protocolType, timestampType);
                } else if (null == reqId) {
                    return connector.insertRawWithTtl(line, protocolType, timestampType, ttl);
                } else if (null == ttl) {
                    return connector.insertRawWithReqId(line, protocolType, timestampType, reqId);
                } else {
                    return connector.insertRawWithTtlAndReqId(line, protocolType, timestampType, ttl, reqId);
                }
            }
            case WS: {
                InsertReq insertReq = new InsertReq();
                insertReq.setReqId(insertId.getAndIncrement());
                insertReq.setProtocol(protocolType.ordinal());
                insertReq.setPrecision(timestampType.getType());
                insertReq.setData(line);
                if (ttl != null)
                    insertReq.setTtl(ttl);
                if (reqId != null)
                    insertReq.setReqId(reqId);
                CommonResp response = (CommonResp) transport.send(new Request(SchemalessAction.INSERT.getAction(), insertReq));
                if (Code.SUCCESS.getCode() != response.getCode()) {
                    throw new SQLException("(0x" + Integer.toHexString(response.getCode()) + "):" + response.getMessage());
                }
                // websocket don't return the num of schemaless insert
                return 0;
            }
            default:
                // nothing
                return 0;
        }
    }

    public int writeRaw(String line, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType, Integer ttl, Long reqId) throws SQLException {
        switch (type) {
            case JNI: {
                if (null == ttl && null == reqId) {
                    return connector.insertRaw(line, protocolType, timestampType);
                } else if (null == reqId) {
                    return connector.insertRawWithTtl(line, protocolType, timestampType, ttl);
                } else if (null == ttl) {
                    return connector.insertRawWithReqId(line, protocolType, timestampType, reqId);
                } else {
                    return connector.insertRawWithTtlAndReqId(line, protocolType, timestampType, ttl, reqId);
                }
            }
            case WS: {
                InsertReq insertReq = new InsertReq();
                insertReq.setReqId(insertId.getAndIncrement());
                insertReq.setProtocol(protocolType.ordinal());
                insertReq.setPrecision(timestampType.getType());
                insertReq.setData(line);
                if (ttl != null)
                    insertReq.setTtl(ttl);
                if (reqId != null)
                    insertReq.setReqId(reqId);
                CommonResp response = (CommonResp) transport.send(new Request(SchemalessAction.INSERT.getAction(), insertReq));
                if (Code.SUCCESS.getCode() != response.getCode()) {
                    throw new SQLException("(0x" + Integer.toHexString(response.getCode()) + "):" + response.getMessage());
                }
                // websocket don't return the num of schemaless insert
                return 0;
            }
            default:
                // nothing
                return 0;
        }
    }

    private void selectDB(TSDBJNIConnector connector, String dbName) throws SQLException {
        long pSql = connector.executeQuery("use " + dbName);
        connector.freeResultSet(pSql);
    }

    public void close() throws SQLException {
        switch (type) {
            case JNI: {
                if (this.shouldClose && !connector.isClosed())
                    connector.closeConnection();
                break;
            }
            case WS: {
                transport.close();
                break;
            }
            default:
                // nothing
        }
    }
}
