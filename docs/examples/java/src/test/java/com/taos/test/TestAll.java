package com.taos.test;

import com.taos.example.*;
import org.junit.FixMethodOrder;
import org.junit.Test;

import java.sql.*;

@FixMethodOrder
public class TestAll {
    private String[] args = new String[]{};

    public void dropDB(String dbName) throws SQLException {
        String jdbcUrl = "jdbc:TAOS://localhost:6030?user=root&password=taosdata";
        try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("drop database if exists " + dbName);
            }
        }
    }

    public void insertData() throws SQLException {
        String jdbcUrl = "jdbc:TAOS://localhost:6030?user=root&password=taosdata";
        try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
            try (Statement stmt = conn.createStatement()) {
                String sql = "INSERT INTO power.d1001 USING power.meters TAGS('California.SanFrancisco', 2) VALUES('2018-10-03 14:38:05.000',10.30000,219,0.31000)\n" +
                        "                        power.d1001 USING power.meters TAGS('California.SanFrancisco', 2) VALUES('2018-10-03 15:38:15.000',12.60000,218,0.33000)\n" +
                        "                        power.d1001 USING power.meters TAGS('California.SanFrancisco', 2) VALUES('2018-10-03 15:38:16.800',12.30000,221,0.31000)\n" +
                        "                        power.d1002 USING power.meters TAGS('California.SanFrancisco', 3) VALUES('2018-10-03 15:38:16.650',10.30000,218,0.25000)\n" +
                        "                        power.d1003 USING power.meters TAGS('California.LosAngeles', 2) VALUES('2018-10-03 15:38:05.500',11.80000,221,0.28000)\n" +
                        "                        power.d1003 USING power.meters TAGS('California.LosAngeles', 2) VALUES('2018-10-03 15:38:16.600',13.40000,223,0.29000)\n" +
                        "                        power.d1004 USING power.meters TAGS('California.LosAngeles', 3) VALUES('2018-10-03 15:38:05.000',10.80000,223,0.29000)\n" +
                        "                        power.d1004 USING power.meters TAGS('California.LosAngeles', 3) VALUES('2018-10-03 15:38:06.000',10.80000,223,0.29000)\n" +
                        "                        power.d1004 USING power.meters TAGS('California.LosAngeles', 3) VALUES('2018-10-03 15:38:07.000',10.80000,223,0.29000)\n" +
                        "                        power.d1004 USING power.meters TAGS('California.LosAngeles', 3) VALUES('2018-10-03 15:38:08.500',11.50000,221,0.35000)";

                stmt.execute(sql);
            }
        }
    }

    @Test
    public void testJNIConnect() throws SQLException {
        JNIConnectExample.main(args);
    }

    @Test
    public void testRestConnect() throws SQLException {
        RESTConnectExample.main(args);
    }

    @Test
    public void testRestInsert() throws SQLException {
        dropDB("power");
        RestInsertExample.main(args);
        RestQueryExample.main(args);
    }

    @Test
    public void testStmtInsert() throws SQLException {
        dropDB("power");
        StmtInsertExample.main(args);
    }

    @Test
    public void testSubscribe() {
        SubscribeDemo.main(args);
    }


    @Test
    public void testSubscribeOverWebsocket() {
        WebsocketSubscribeDemo.main(args);
    }

    @Test
    public void testSchemaless() throws SQLException {
        LineProtocolExample.main(args);
        TelnetLineProtocolExample.main(args);
        // for json protocol, tags may be double type. but for telnet protocol tag must be nchar type.
        // To avoid type mismatch, we delete database test.
        dropDB("test");
        JSONProtocolExample.main(args);
    }
}
