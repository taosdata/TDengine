package com.taosdata.jdbc;

import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;
import com.taosdata.jdbc.rs.RestfulConnection;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * This class is for schemaless lines(line/telnet/json) write to tdengine.
 * e.g.:
 * SchemalessWriter writer = new SchemalessWriter(connection);
 * writer.write(lines, SchemalessProtocolType, SchemalessTimestampType);
 */
public class SchemalessWriter {
    protected Connection connection;

    public SchemalessWriter(Connection connection) {
        this.connection = connection;
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
}
