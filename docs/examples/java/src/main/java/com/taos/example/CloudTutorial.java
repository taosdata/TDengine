package com.taos.example;

import java.sql.*;

public class CloudTutorial {
    public static void main(String[] args) throws SQLException {
        String jdbcUrl = System.getenv("TDENGINE_JDBC_URL");
        try(Connection conn = DriverManager.getConnection(jdbcUrl)) {
            try (Statement stmt = conn.createStatement()) {
// ANCHOR: insert
stmt.execute("DROP DATABASE IF EXISTS power");
stmt.execute("CREATE DATABASE power");
stmt.execute("CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)");
stmt.execute("INSERT INTO power.d1001 USING power.meters TAGS(California.SanFrancisco, 2) VALUES ('2018-10-03 14:38:05.000', 10.30000, 219, 0.31000) ('2018-10-03 14:38:15.000', 12.60000, 218, 0.33000) ('2018-10-03 14:38:16.800', 12.30000, 221, 0.31000) power.d1002 USING power.meters TAGS(California.SanFrancisco, 3) VALUES ('2018-10-03 14:38:16.650', 10.30000, 218, 0.25000)");
// ANCHOR_END: insert
// ANCHOR: query
ResultSet result = stmt.executeQuery("SELECT ts, current FROM power.meters LIMIT 2");
// ANCHOR_END: query
// ANCHOR: meta
// print column names
ResultSetMetaData meta = result.getMetaData();
System.out.println(meta.getColumnLabel(1) + "\t" + meta.getColumnLabel(2));
// output: ts	current
// ANCHOR_END: meta
// ANCHOR: iter
while(result.next()) {
    System.out.println(result.getTimestamp(1) + "\t" + result.getFloat(2));
}
// output:
//2018-10-03 14:38:05.0	10.3
//2018-10-03 14:38:15.0	12.6
// ANCHOR_END: iter
            }
        }
    }
}
