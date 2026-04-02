package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.sql.*;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class StableTest {

    private static Connection connection;

    static final String HOST = TestEnvUtil.getHost();
    private static final String DB_NAME = TestUtils.camelToSnake(StableTest.class);
    private static final String STB_NAME = "st";

    @BeforeClass
    public static void createDatabase() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        String url = SpecifyAddress.getInstance().getJniWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + HOST + ":0/";
        }
        connection = DriverManager.getConnection(url, properties);
        Statement statement = connection.createStatement();
        statement.execute("drop database if exists " + DB_NAME);
        statement.execute("create database if not exists " + DB_NAME);
        statement.execute("use " + DB_NAME);
        statement.close();

    }

    @Test
    public void case001_createSuperTable() {
        try (Statement stmt = connection.createStatement()) {
            final String sql = "create table " + STB_NAME + " (ts timestamp, v1 int, v2 int) tags (tg nchar(20)) ";
            stmt.execute(sql);
        } catch (SQLException e) {
            assert false : "error create stable" + e.getMessage();
        }
    }

    @Test
    public void case002_createTable() {
        try (Statement stmt = connection.createStatement()) {
            final String sql = "create table t1 using " + STB_NAME + " tags (\"beijing\")";
            stmt.execute(sql);
        } catch (SQLException e) {
            assert false : "error create table" + e.getMessage();
        }
    }

    @Test
    public void case003_describeSTable() {
        int num = 0;
        try (Statement stmt = connection.createStatement()) {
            String sql = "describe " + STB_NAME;
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                num++;
            }
            rs.close();
            assertEquals(4, num);
        } catch (SQLException e) {
            assert false : "error describe stable" + e.getMessage();
        }
    }

    @Test
    public void case004_describeTable() {
        int num = 0;
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery("describe t1");
            while (rs.next()) {
                num++;
            }
            rs.close();
            assertEquals(4, num);
        } catch (SQLException e) {
            assert false : "error describe stable" + e.getMessage();
        }
    }

    @AfterClass
    public static void close() {
        try {
            if (connection != null){
                Statement statement = connection.createStatement();
                statement.execute("drop database if exists " + DB_NAME);
                statement.close();
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}

