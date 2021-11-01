package com.taosdata.jdbc;

import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;
import com.taosdata.jdbc.rs.RestfulConnection;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class SchemalessStatement extends AbstractStatementWrapper {
    public SchemalessStatement(Statement statement) {
        super(statement);
    }

    public void executeSchemaless(String[] strings, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType) throws SQLException {
        Connection connection = this.getConnection();
        if (connection instanceof TSDBConnection) {
            TSDBConnection tsdbConnection = (TSDBConnection) connection;
            tsdbConnection.getConnector().insertLines(strings, protocolType, timestampType);
        } else if (connection instanceof RestfulConnection) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD, "restful connection is not supported currently");
        } else {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "unknown connection：" + connection.getMetaData().getURL());
        }
    }

    public void executeSchemaless(String sql, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType) throws SQLException {
        executeSchemaless(new String[]{sql}, protocolType, timestampType);
    }
}
