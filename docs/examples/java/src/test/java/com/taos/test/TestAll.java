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
        waitTransaction();
    }

    public void dropTopic(String topicName) throws SQLException {
        String jdbcUrl = "jdbc:TAOS://localhost:6030?user=root&password=taosdata";
        try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("drop topic if exists " + topicName);
            }
        }
        waitTransaction();
    }

    public void waitTransaction() throws SQLException {

        String jdbcUrl = "jdbc:TAOS://localhost:6030?user=root&password=taosdata";
        try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
            try (Statement stmt = conn.createStatement()) {
                for (int i = 0; i < 10; i++) {
                    stmt.execute("show transactions");
                    try (ResultSet resultSet = stmt.getResultSet()) {
                        if (resultSet.next()) {
                            int count = resultSet.getInt(1);
                            if (count == 0) {
                                break;
                            }
                        }
                    }
                }
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
    public void testJNIConnect() throws Exception {
        JNIConnectExample.main(args);
    }

    @Test
    public void testRestConnect() throws Exception {
        RESTConnectExample.main(args);
    }

    @Test
    public void testWsConnect() throws Exception {
        WSConnectExample.main(args);
    }

    @Test
    public void testBase() throws Exception {
        JdbcCreatDBDemo.main(args);
        JdbcInsertDataDemo.main(args);
        JdbcQueryDemo.main(args);

        dropDB("power");
    }

    @Test
    public void testWsSchemaless() throws Exception {
        dropDB("power");
        SchemalessWsTest.main(args);
    }
    @Test
    public void testJniSchemaless() throws Exception {
        dropDB("power");
        SchemalessJniTest.main(args);
    }

    @Test
    public void testJniStmtBasic() throws Exception {
        dropDB("power");
        ParameterBindingBasicDemo.main(args);
    }

    @Test
    public void testJniStmtFull() throws Exception {
        dropDB("power");
        ParameterBindingFullDemo.main(args);
    }

    @Test
    public void testWsStmtBasic() throws Exception {
        dropDB("power");
        WSParameterBindingBasicDemo.main(args);
    }

    @Test
    public void testWsStmtFull() throws Exception {
        dropDB("power");
        WSParameterBindingFullDemo.main(args);
    }

    @Test
    public void testConsumer() throws Exception {
        dropDB("power");
        SubscribeDemo.main(args);
    }

    @Test
    public void testSubscribeJni() throws SQLException, InterruptedException {
        dropTopic("topic_meters");
        dropDB("power");
        ConsumerLoopFull.main(args);
        dropTopic("topic_meters");
        dropDB("power");
    }
    @Test
    public void testSubscribeWs() throws SQLException, InterruptedException {
        dropTopic("topic_meters");
        dropDB("power");
        WsConsumerLoopFull.main(args);
        dropTopic("topic_meters");
        dropDB("power");
    }
}
