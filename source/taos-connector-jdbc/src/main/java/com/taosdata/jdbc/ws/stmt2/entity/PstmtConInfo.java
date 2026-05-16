package com.taosdata.jdbc.ws.stmt2.entity;

import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.ws.Transport;

public class PstmtConInfo {
    private final Transport transport;
    private final ConnectionParam param;
    private final String database;
    private final AbstractConnection connection;
    private final Long instanceId;

    public PstmtConInfo(Transport transport, ConnectionParam param, String database, AbstractConnection connection, Long instanceId) {
        this.transport = transport;
        this.param = param;
        this.database = database;
        this.connection = connection;
        this.instanceId = instanceId;
    }

    public Transport getTransport() {
        return transport;
    }

    public ConnectionParam getParam() {
        return param;
    }

    public String getDatabase() {
        return database;
    }

    public AbstractConnection getConnection() {
        return connection;
    }

    public Long getInstanceId() {
        return instanceId;
    }
}
