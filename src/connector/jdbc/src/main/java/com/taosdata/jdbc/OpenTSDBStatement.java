package com.taosdata.jdbc;

import com.taosdata.jdbc.rs.RestfulConnection;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class OpenTSDBStatement extends AbstractStatementWrapper {

    public OpenTSDBStatement(Statement statement) {
        super(statement);
    }

    public void executeTelnetPut(String[] strings) throws SQLException {
        Connection connection = this.getConnection();
        if (connection instanceof TSDBConnection) {
            TSDBConnection tsdbConnection = (TSDBConnection) connection;
            tsdbConnection.getConnector().insertTelnetLines(strings);
        } else if (connection instanceof RestfulConnection) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD, "restful connection is not supported currently");
        } else {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN_CONNECTION, "unknown connection：" + connection.getCatalog());
        }
    }

    public void executeTelnetPut(String sql) throws SQLException {
        executeTelnetPut(new String[]{sql});
    }

    public void executeInsertJsonPayload(String json) throws SQLException {
        Connection connection = this.getConnection();
        if (connection instanceof TSDBConnection) {
            TSDBConnection tsdbConnection = (TSDBConnection) connection;
            tsdbConnection.getConnector().insertJsonPayload(json);
        } else if (connection instanceof RestfulConnection) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD, "restful connection is not supported currently");
        } else {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN_CONNECTION, "unknown connection：" + connection.getCatalog());
        }
    }
}
