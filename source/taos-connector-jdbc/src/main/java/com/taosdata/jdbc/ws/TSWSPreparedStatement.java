package com.taosdata.jdbc.ws;


import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.ws.stmt2.entity.Stmt2PrepareResp;

import java.sql.SQLException;
public class TSWSPreparedStatement extends AbsWSPreparedStatement {
    public TSWSPreparedStatement(Transport transport,
                                 ConnectionParam param,
                                 String database,
                                 AbstractConnection connection,
                                 String sql,
                                 Long instanceId,
                                 Stmt2PrepareResp prepareResp) throws SQLException {
        super(transport, param, database, connection, sql, instanceId, prepareResp);
    }

    public TSWSPreparedStatement(Transport transport,
                                 ConnectionParam param,
                                 String database,
                                 AbstractConnection connection,
                                 String sql,
                                 Long instanceId) throws SQLException {
        super(transport, param, database, connection, sql, instanceId);
    }
}
