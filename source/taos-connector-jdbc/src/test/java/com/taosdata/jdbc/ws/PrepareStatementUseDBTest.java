package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

public class PrepareStatementUseDBTest {
    private static final String HOST = "127.0.0.1";
    private static Connection conn;
    private static final String SQL_INSERT = "insert into t1 values (?, ?)";
    private static final String DB_NAME = TestUtils.camelToSnake(PrepareStatementUseDBTest.class);
    private static final String USE_DB = "stmt_ws_use_db2";

    @Test
    public void testUseDB() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + HOST + ":6041/" + DB_NAME + "?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword() + "&batchfetch=true";
        }
        conn = DriverManager.getConnection(url);
        try (PreparedStatement stmt = conn.prepareStatement(SQL_INSERT)) {
            stmt.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
            stmt.setInt(2, 1);
            stmt.addBatch();
            stmt.executeBatch();
            stmt.execute("use " + USE_DB);
            stmt.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
            stmt.setInt(2, 2);
            stmt.addBatch();
            stmt.executeBatch();
        }
    }

    @Before
    public void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + HOST + ":6041/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword() + "&batchfetch=true";
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "C");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
        conn = DriverManager.getConnection(url, properties);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists " + DB_NAME);
            stmt.execute("create database if not exists " + DB_NAME);
            stmt.execute("use " + DB_NAME);
            stmt.execute("create table t1 (ts timestamp, c1 int)");

            stmt.execute("drop database if exists " + USE_DB);
            stmt.execute("create database if not exists " + USE_DB);
            stmt.execute("use " + USE_DB);
            stmt.execute("create table t1 (ts timestamp, c1 int)");
        }
    }

    @AfterClass
    public static void after() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists " + DB_NAME);
            stmt.execute("drop database if exists " + USE_DB);
        }
        conn.close();
    }
}

