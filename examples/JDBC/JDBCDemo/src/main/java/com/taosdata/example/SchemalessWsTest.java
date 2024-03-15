package com.taosdata.example;

import com.taosdata.jdbc.SchemalessWriter;
import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

// ANCHOR: schemaless
public class SchemalessWsTest {
    private static final String host = "127.0.0.1";
    private static final String lineDemo = "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1626006833639000000";
    private static final String telnetDemo = "stb0_0 1707095283260 4 host=host0 interface=eth0";
    private static final String jsonDemo = "{\"metric\": \"meter_current\",\"timestamp\": 1626846400,\"value\": 10.3, \"tags\": {\"groupid\": 2, \"location\": \"California.SanFrancisco\", \"id\": \"d1001\"}}";

    public static void main(String[] args) throws SQLException {
        final String url = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata&batchfetch=true";
        try(Connection connection = DriverManager.getConnection(url)){
            init(connection);

            try(SchemalessWriter writer = new SchemalessWriter(connection, "power")){
                writer.write(lineDemo, SchemalessProtocolType.LINE, SchemalessTimestampType.NANO_SECONDS);
                writer.write(telnetDemo, SchemalessProtocolType.TELNET, SchemalessTimestampType.MILLI_SECONDS);
                writer.write(jsonDemo, SchemalessProtocolType.JSON, SchemalessTimestampType.SECONDS);
            }
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
