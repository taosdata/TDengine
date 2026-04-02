package com.taosdata.jdbc.rs;

import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DatabaseSpecifiedTest {

    static final String HOST = TestEnvUtil.getHost();
            private final String dbname = TestUtils.camelToSnake(DatabaseSpecifiedTest.class);

    private Connection connection;
    private long ts;

    @Test
    public void test() throws SQLException {
        // when
        String url = SpecifyAddress.getInstance().getRestWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + HOST + ":" + TestEnvUtil.getRsPort() + "/" + dbname + "?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        } else {
            url = url + dbname + "?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        connection = DriverManager.getConnection(url);
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from weather");

            //then
            assertNotNull(rs);
            rs.next();
            long now = rs.getTimestamp("ts").getTime();
            assertEquals(ts, now);
            int f1 = rs.getInt(2);
            assertEquals(1, f1);
            String loc = rs.getString("loc");
            assertEquals("beijing", loc);
        }
    }

    @Before
    public void before() {
        ts = System.currentTimeMillis();
        try {
            String url = SpecifyAddress.getInstance().getRestUrl();
            if (url == null) {
                url = "jdbc:TAOS-RS://" + HOST + ":" + TestEnvUtil.getRsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
            }
            Connection connection = DriverManager.getConnection(url);
            Statement stmt = connection.createStatement();

            stmt.execute("drop database if exists " + dbname);
            stmt.execute("create database if not exists " + dbname);
            stmt.execute("use " + dbname);
            stmt.execute("create table weather(ts timestamp, f1 int) tags(loc nchar(10))");
            stmt.execute("insert into t1 using weather tags('beijing') values( " + ts + ", 1)");

            stmt.close();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @After
    public void after() {
        try {
            if (connection != null) {
                Statement statement = connection.createStatement();
                statement.execute("drop database if exists " + dbname);
                statement.close();
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}

