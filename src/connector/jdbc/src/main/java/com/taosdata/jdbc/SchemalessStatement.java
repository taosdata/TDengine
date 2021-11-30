package com.taosdata.jdbc;

import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;
import com.taosdata.jdbc.rs.RestfulConnection;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author huolibo@qq.com
 * @version v1.0.0
 * @JDK: 1.8
 * @description: this class is an extension of {@link Statement}. use like:
 * Statement statement = conn.createStatement();
 * SchemalessStatement schemalessStatement = new SchemalessStatement(statement);
 * schemalessStatement.execute(sql);
 * schemalessStatement.executeSchemaless(lines, SchemalessProtocolType, SchemalessTimestampType);
 * @since 2021-11-03 17:10
 */
public class SchemalessStatement extends AbstractStatementWrapper {
    public SchemalessStatement(Statement statement) {
        super(statement);
    }

    /**
     * batch insert schemaless lines
     *
     * @param lines         schemaless data
     * @param protocolType  schemaless type {@link SchemalessProtocolType}
     * @param timestampType Time precision {@link SchemalessTimestampType}
     * @throws SQLException execute insert exception
     */
    public void executeSchemaless(String[] lines, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType) throws SQLException {
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
    public void executeSchemaless(String line, SchemalessProtocolType protocolType, SchemalessTimestampType timestampType) throws SQLException {
        executeSchemaless(new String[]{line}, protocolType, timestampType);
    }
}
