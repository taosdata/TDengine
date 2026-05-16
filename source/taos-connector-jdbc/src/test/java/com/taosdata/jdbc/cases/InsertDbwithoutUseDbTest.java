package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.sql.*;
import java.util.Properties;
import java.util.Random;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class InsertDbwithoutUseDbTest {

        static final String HOST = TestEnvUtil.getHost();


    private static Properties properties;
    private static final Random random = new Random(System.currentTimeMillis());
    private static final String DB_NAME = TestUtils.camelToSnake(InsertDbwithoutUseDbTest.class);

    @Test
    public void case001() throws SQLException {
        // prepare schema
        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + HOST + ":" + TestEnvUtil.getJniPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        Connection conn = DriverManager.getConnection(url, properties);
        Statement stmt = conn.createStatement();
        stmt.execute("drop database if exists " + DB_NAME);
        stmt.execute("create database if not exists " + DB_NAME);
        stmt.execute("create table " + DB_NAME + ".weather(ts timestamp, f1 int)");

        conn.close();

        // execute insert
        url = SpecifyAddress.getInstance().getJniWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + HOST + ":" + TestEnvUtil.getJniPort() + "/" + DB_NAME + "?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        } else {
            url = url + DB_NAME + "?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        conn = DriverManager.getConnection(url, properties);
        stmt = conn.createStatement();
        int affectedRow = stmt.executeUpdate("insert into weather(ts, f1) values(now," + random.nextInt(100) + ")");
        Assert.assertEquals(1, affectedRow);
        boolean flag = stmt.execute("insert into weather(ts, f1) values(now + 10s," + random.nextInt(100) + ")");
        Assert.assertEquals(false, flag);
        ResultSet rs = stmt.executeQuery("select count(*) from weather");
        rs.next();
        int count = rs.getInt("count(*)");
        Assert.assertEquals(2, count);
        conn.close();
    }

    @Test
    public void case002() throws SQLException {
        // prepare the schema
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + HOST + ":" + TestEnvUtil.getRsPort() + "/" + DB_NAME + "?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        Connection conn = DriverManager.getConnection(url, properties);
        Statement stmt = conn.createStatement();
        stmt.execute("drop database if exists " + DB_NAME);
        stmt.execute("create database if not exists " + DB_NAME);
        stmt.execute("create table " + DB_NAME + ".weather(ts timestamp, f1 int)");
        stmt.close();

        // execute
        stmt = conn.createStatement();
        int affectedRow = stmt.executeUpdate("insert into weather(ts, f1) values(now," + random.nextInt(100) + ")");
        Assert.assertEquals(1, affectedRow);
        boolean flag = stmt.execute("insert into weather(ts, f1) values(now + 10s," + random.nextInt(100) + ")");
        Assert.assertEquals(false, flag);
        ResultSet rs = stmt.executeQuery("select count(*) from weather");
        rs.next();
        int count = rs.getInt("count(*)");
        Assert.assertEquals(2, count);
        stmt.execute("drop database if exists " + DB_NAME);
        stmt.close();
        conn.close();
    }

    @BeforeClass
    public static void beforeClass() {
        properties = new Properties();
        properties.setProperty("charset", "UTF-8");
        properties.setProperty("locale", "en_US.UTF-8");
        properties.setProperty("timezone", "UTC-8");
    }

}

