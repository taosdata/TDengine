package com.taos.example;

import com.taosdata.jdbc.SchemalessWriter;
import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class LineProtocolExample {
    // format: measurement,tag_set field_set timestamp
    private static String[] lines = {
            "meters,location=California.LosAngeles,groupid=2 current=11.8,voltage=221,phase=0.28 1648432611249000", // micro
                                                                                                              // seconds
            "meters,location=California.LosAngeles,groupid=2 current=13.4,voltage=223,phase=0.29 1648432611249500",
            "meters,location=California.LosAngeles,groupid=3 current=10.8,voltage=223,phase=0.29 1648432611249300",
            "meters,location=California.LosAngeles,groupid=3 current=11.3,voltage=221,phase=0.35 1648432611249800",
    };

    private static Connection getConnection() throws SQLException {
        String jdbcUrl = "jdbc:TAOS://localhost:6030?user=root&password=taosdata";
        return DriverManager.getConnection(jdbcUrl);
    }

    private static void createDatabase(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            // the default precision is ms (millisecond), but we use us(microsecond) here.
            stmt.execute("CREATE DATABASE IF NOT EXISTS test PRECISION 'us'");
            stmt.execute("USE test");
        }
    }

    public static void main(String[] args) throws SQLException {
        try (Connection conn = getConnection()) {
            createDatabase(conn);
            SchemalessWriter writer = new SchemalessWriter(conn);
            writer.write(lines, SchemalessProtocolType.LINE, SchemalessTimestampType.MICRO_SECONDS);
        }
    }
}
