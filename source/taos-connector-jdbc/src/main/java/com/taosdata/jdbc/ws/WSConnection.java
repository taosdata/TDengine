package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.enums.FieldBindType;
import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.utils.StmtUtils;
import com.taosdata.jdbc.utils.StringUtils;
import com.taosdata.jdbc.ws.entity.*;
import com.taosdata.jdbc.ws.schemaless.InsertReq;
import com.taosdata.jdbc.ws.schemaless.SchemalessAction;
import com.taosdata.jdbc.ws.stmt2.entity.Field;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2PrepareResp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class WSConnection extends AbstractConnection {
    private static final Logger log = LoggerFactory.getLogger(WSConnection.class);
    public static final AtomicBoolean g_FirstConnection = new AtomicBoolean(true);
    private final Transport transport;
    private final DatabaseMetaData metaData;
    private String database;
    private final ConnectionParam param;
    private final AtomicLong insertId = new AtomicLong(0);
    private final String jdbcUrl;

    private static final ConcurrentHashMap<String, ConCheckInfo> conCheckInfoMap = new ConcurrentHashMap<>();
    private static final Map<String, Object> jdbcUrlLocks = new ConcurrentHashMap<>();

    public WSConnection(String url, Properties properties, Transport transport, ConnectionParam param, String serverVersion) {
        super(properties, serverVersion);
        this.transport = transport;
        this.database = param.getDatabase();
        this.param = param;
        this.jdbcUrl = StringUtils.retainHostPortPart(url);
        this.metaData = new WSDatabaseMetaData(url, properties.getProperty(TSDBDriver.PROPERTY_KEY_USER), this);
    }

    @Override
    public Statement createStatement() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        if (this.getClientInfo(TSDBDriver.PROPERTY_KEY_DBNAME) != null)
            database = this.getClientInfo(TSDBDriver.PROPERTY_KEY_DBNAME);
        WSStatement statement = new WSStatement(transport, database, this, idGenerator.getAndIncrement(), param.getZoneId());

        statementsMap.put(statement.getInstanceId(), statement);
        return statement;
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);

        if (this.getClientInfo(TSDBDriver.PROPERTY_KEY_DBNAME) != null) {
            database = this.getClientInfo(TSDBDriver.PROPERTY_KEY_DBNAME);
        }

        boolean efficientWritingSql = false;
        if (sql.startsWith("ASYNC_INSERT")){
            sql = sql.substring("ASYNC_".length());
            efficientWritingSql = true;
        }

        if (!sql.contains("?")){
            return new TSWSPreparedStatement(transport,
                    param,
                    database,
                    this,
                    sql,
                    idGenerator.getAndIncrement());
        }

        if (transport != null && !transport.isClosed()) {
            Stmt2PrepareResp prepareResp = StmtUtils.initStmtWithRetry(transport, sql, param.getRetryTimes());

            boolean isInsert = prepareResp.isInsert();
            boolean isSuperTable = false;
            if (isInsert){
                for (Field field : prepareResp.getFields()){
                    if (field.getBindType() == FieldBindType.TAOS_FIELD_TBNAME.getValue()){
                        isSuperTable = true;
                        break;
                    }
                }
            }

            if ((efficientWritingSql || "STMT".equalsIgnoreCase(param.getAsyncWrite())) && isInsert && isSuperTable) {
                return new WSEWPreparedStatement(transport,
                        param,
                        database,
                        this,
                        sql,
                        idGenerator.getAndIncrement(),
                        prepareResp);
            } else {
                if ("line".equalsIgnoreCase(param.getPbsMode())){
                    if (super.supportLineBind) {
                        return new WSRowPreparedStatement(transport,
                                param,
                                database,
                                this,
                                sql,
                                idGenerator.getAndIncrement(),
                                prepareResp);
                    } else {
                        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_LINE_BIND_MODE_UNSUPPORTED_IN_SERVER);
                    }
                }
                return new TSWSPreparedStatement(transport,
                        param,
                        database,
                        this,
                        sql,
                        idGenerator.getAndIncrement(),
                        prepareResp);
            }
        } else {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        }
    }

    @Override
    public void close() throws SQLException {
        for (Map.Entry<Long, Statement> entry : statementsMap.entrySet()) {
            Statement value = entry.getValue();
            value.close();
        }
        statementsMap.clear();
        transport.close();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return transport.isClosed();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        if (isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
        }
        return this.metaData;
    }
    public ConnectionParam getParam() {
        return param;
    }

    @Override
    public void write(String[] lines, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType, Integer ttl, Long reqId) throws SQLException {
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
            CommonResp response = (CommonResp) transport.send(new Request(SchemalessAction.INSERT.getAction(), insertReq), param.getRequestTimeout());
            if (Code.SUCCESS.getCode() != response.getCode()) {
                throw new SQLException("0x" + Integer.toHexString(response.getCode()) + ":" + response.getMessage());
            }
        }
    }

    @Override
    public int writeRaw(String line, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType, Integer ttl, Long reqId) throws SQLException {
        InsertReq insertReq = new InsertReq();
        insertReq.setReqId(insertId.getAndIncrement());
        insertReq.setProtocol(protocolType.ordinal());
        insertReq.setPrecision(timestampType.getType());
        insertReq.setData(line);
        if (ttl != null)
            insertReq.setTtl(ttl);
        if (reqId != null)
            insertReq.setReqId(reqId);
        CommonResp response = (CommonResp) transport.send(new Request(SchemalessAction.INSERT.getAction(), insertReq), param.getRequestTimeout());
        if (Code.SUCCESS.getCode() != response.getCode()) {
            throw new SQLException("(0x" + Integer.toHexString(response.getCode()) + "):" + response.getMessage());
        }
        // websocket don't return the num of schemaless insert
        return 0;
    }

    private boolean noNeedCheck(){
        ConCheckInfo conCheckInfo = conCheckInfoMap.get(jdbcUrl);
        return conCheckInfo != null
                && conCheckInfo.isValid()
                && !transport.isConnectionLost()
                && (!conCheckInfo.isExpired(param.getWsKeepAlive()));
    }
    @Override
    public boolean isValid(int timeout) throws SQLException {
        //true if the connection is valid, false otherwise
        if (isClosed())
            return false;
        if (timeout < 0)    //SQLException - if the value supplied for timeout is less than 0
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_INVALID_VARIABLE);

        if (noNeedCheck()){
            return true;
        }

        Object lock = jdbcUrlLocks.computeIfAbsent(jdbcUrl, k -> new Object());
        synchronized (lock) {
            if (noNeedCheck()){
                return true;
            }

            int status;
            Statement stmt = null;
            ResultSet resultSet = null;
            try {
                stmt = createStatement();
                stmt.setQueryTimeout(timeout);
                resultSet = stmt.executeQuery("SHOW CLUSTER ALIVE");
                resultSet.next();
                status = resultSet.getInt(1);
                conCheckInfoMap.put(jdbcUrl, new ConCheckInfo(System.currentTimeMillis(), status != 0));
                return status != 0;
            } catch (SQLException e) {
                conCheckInfoMap.put(jdbcUrl, new ConCheckInfo(System.currentTimeMillis(), false));
                log.error("check connection failed", e);
                return false;
            } finally {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
            }
        }
    }
}
