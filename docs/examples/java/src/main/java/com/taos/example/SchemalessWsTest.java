package com.taos.example;

import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

// ANCHOR: schemaless
public class SchemalessWsTest {
    private static final String host = "127.0.0.1";
    private static final String lineDemo = "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1626006833639";
    private static final String telnetDemo = "metric_telnet 1707095283260 4 host=host0 interface=eth0";
    private static final String jsonDemo = "{\"metric\": \"metric_json\",\"timestamp\": 1626846400,\"value\": 10.3, \"tags\": {\"groupid\": 2, \"location\": \"California.SanFrancisco\", \"id\": \"d1001\"}}";

    public static void main(String[] args) throws SQLException {
        final String url = "jdbc:TAOS-WS://" + host + ":6041?user=root&password=taosdata";
        try (Connection connection = DriverManager.getConnection(url)) {
            init(connection);
            AbstractConnection conn = connection.unwrap(AbstractConnection.class);

            conn.write(lineDemo, SchemalessProtocolType.LINE, SchemalessTimestampType.MILLI_SECONDS);
            conn.write(telnetDemo, SchemalessProtocolType.TELNET, SchemalessTimestampType.MILLI_SECONDS);
            conn.write(jsonDemo, SchemalessProtocolType.JSON, SchemalessTimestampType.SECONDS);
            System.out.println("Inserted data with schemaless successfully.");
        } catch (Exception ex) {
            // please refer to the JDBC specifications for detailed exceptions info
            System.out.printf("Failed to insert data with schemaless, %sErrMessage: %s%n",
                    ex instanceof SQLException ? "ErrCode: " + ((SQLException) ex).getErrorCode() + ", " : "",
                    ex.getMessage());
            // Print stack trace for context in examples. Use logging in production.
            ex.printStackTrace();
            throw ex;
        }
    }

    private static void init(Connection connection) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE DATABASE IF NOT EXISTS power");
            stmt.execute("USE power");
        }
    }
}
// ANCHOR_END: schemaless
