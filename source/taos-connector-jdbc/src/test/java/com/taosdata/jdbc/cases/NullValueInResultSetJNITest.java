package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

public class NullValueInResultSetJNITest {
    private static final String DB_NAME = TestUtils.camelToSnake(NullValueInResultSetJNITest.class);

                    Connection conn;

    @Test
    public void test() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from weather");
            ResultSetMetaData meta = rs.getMetaData();
            while (rs.next()) {
            }

        }
    }

    @Before
    public void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + TestEnvUtil.getHost() + ":" + TestEnvUtil.getJniPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "C");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        conn = DriverManager.getConnection(url, properties);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists " + DB_NAME);
            stmt.execute("create database if not exists " + DB_NAME);
            stmt.execute("use " + DB_NAME);
            stmt.execute("create table weather(ts timestamp, f1 int, f2 bigint, f3 float, f4 double, f5 smallint, f6 tinyint, f7 bool, f8 binary(64), f9 nchar(64))");
            stmt.executeUpdate("insert into weather(ts, f1) values(now+1s, 1)");
            stmt.executeUpdate("insert into weather(ts, f2) values(now+2s, 2)");
            stmt.executeUpdate("insert into weather(ts, f3) values(now+3s, 3.0)");
            stmt.executeUpdate("insert into weather(ts, f4) values(now+4s, 4.0)");
            stmt.executeUpdate("insert into weather(ts, f5) values(now+5s, 5)");
            stmt.executeUpdate("insert into weather(ts, f6) values(now+6s, 6)");
            stmt.executeUpdate("insert into weather(ts, f7) values(now+7s, true)");
            stmt.executeUpdate("insert into weather(ts, f8) values(now+8s, 'hello')");
            stmt.executeUpdate("insert into weather(ts, f9) values(now+9s, '涛思数据')");
        }
    }

    @After
    public void after() throws SQLException {
        if (conn != null) {
            Statement statement = conn.createStatement();
            statement.execute("drop database if exists " + DB_NAME);
            statement.close();
            conn.close();
        }
    }
}

