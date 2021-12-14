package com.taosdata.jdbc;

import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;
import com.taosdata.jdbc.rs.RestfulConnection;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * this class is an extension of {@link Statement}. e.g.:
 * Statement statement = conn.createStatement();
 * SchemalessStatement schemalessStatement = new SchemalessStatement(statement);
 * schemalessStatement.execute(sql);
 * schemalessStatement.insert(lines, SchemalessProtocolType, SchemalessTimestampType);
 */
public class SchemalessStatement extends AbstractStatementWrapper {
    public SchemalessStatement(Statement statement) {
        super(statement);
    }

    /**
     * batch insert schemaless lines
     *
     * @param lines         schemaless lines
     * @param protocolType  schemaless type {@link SchemalessProtocolType}
     * @param timestampType Time precision {@link SchemalessTimestampType}
     * @throws SQLException execute insert exception
     */
    public void insert(String[] lines, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType) throws SQLException {
        Connection connection = this.getConnection();
        if (connection instanceof TSDBConnection) {
            TSDBConnection tsdbConnection = (TSDBConnection) connection;
            tsdbConnection.getConnector().insertLines(lines, protocolType, timestampType);
        } else if (connection instanceof RestfulConnection) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD, "restful connection is not supported currently");
        } else {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "unknown connection：" + connection.getMetaData().getURL());
        }
    }

    /**
     * only one insert
     *
     * @param line          schemaless line
     * @param protocolType  schemaless type {@link SchemalessProtocolType}
     * @param timestampType Time precision {@link SchemalessTimestampType}
     * @throws SQLException execute insert exception
     */
    public void insert(String line, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType) throws SQLException {
        insert(new String[]{line}, protocolType, timestampType);
    }
}
