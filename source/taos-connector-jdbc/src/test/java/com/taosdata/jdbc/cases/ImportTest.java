package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.sql.*;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ImportTest {
    private static Connection connection;
    static final String DB_NAME = TestUtils.camelToSnake(ImportTest.class);
    static final String T_NAME = "t0";
    static final String HOST = TestEnvUtil.getHost();
    private static long ts;

    @Test
    public void case001_insertData() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            for (int i = 0; i < 50; i++) {
                ts++;
                int row = stmt.executeUpdate("import into " + DB_NAME + "." + T_NAME + " values (" + ts + ", " + (100 + i) + ", " + i + ")");
                assertEquals(1, row);
            }
        }
    }

    @Test
    public void case002_checkSum() throws SQLException {
        Assert.assertEquals(50, select());
    }

    private int select() throws SQLException {
        int count = 0;
        try (Statement stmt = connection.createStatement()) {
            String sql = "select * from " + DB_NAME + "." + T_NAME;
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                count++;
            }
            rs.close();
        }
        return count;
    }

    @Test
    public void case003_importData() throws SQLException {
        // 避免时间重复
        try (Statement stmt = connection.createStatement()) {
            StringBuilder sqlBuilder = new StringBuilder("import into ").append(DB_NAME).append(".").append(T_NAME).append(" values ");
            for (int i = 0; i < 50; i++) {
                int a = i / 5;
                long t = (++ts) + a;
                sqlBuilder.append("(").append(t).append(",").append((100 + i)).append(",").append(i).append(") ");
            }
            int rows = stmt.executeUpdate(sqlBuilder.toString());
            assertEquals(50, rows);
        }
    }

    @Test
    public void case004_checkSum() throws SQLException {
        Assert.assertEquals(100, select());
    }

    @BeforeClass
    public static void before() throws SQLException {
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

        Statement stmt = connection.createStatement();
        stmt.execute("create database if not exists " + DB_NAME);
        stmt.execute("create table if not exists " + DB_NAME + "." + T_NAME + " (ts timestamp, k int, v int)");
        stmt.close();

        ts = System.currentTimeMillis();
    }

    @AfterClass
    public static void close() {
        if (connection != null) {
            try {
                Statement statement = connection.createStatement();
                statement.executeUpdate("drop database " + DB_NAME);
                statement.close();
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }
    }
}

