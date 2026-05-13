package com.taosdata.jdbc;

import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.StringUtils;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.*;

import java.sql.*;
import java.util.Properties;

public class BlobTest {
    static final String HOST = TestEnvUtil.getHost();
    static final String DB_NAME = TestUtils.camelToSnake(BlobTest.class);
    static final String TABLE_NATIVE = "blob_noraml";
    static Connection connection;
    static Statement statement;

    static final String TEST_STR = "20160601";
    static final byte[] expectedArray = StringUtils.hexToBytes(TEST_STR);

    @BeforeClass
    public static void checkEnvironment() {
        TestUtils.runInMain();
    }
    @Test
    public void testInsert() throws Exception {
        statement.executeUpdate("insert into subt_a using " + DB_NAME + "." + TABLE_NATIVE + "  tags( \"123456abcdef\")  values(now, \"\\x" + TEST_STR + "\")");
        ResultSet resultSet = statement.executeQuery("select c1, t1 from " + DB_NAME + "." + TABLE_NATIVE);
        resultSet.next();
        Assert.assertArrayEquals(expectedArray, resultSet.getBytes(1));
        resultSet.close();
    }

    @Test
    public void testInsertNull() throws Exception {
        statement.executeUpdate("insert into subt_a using " + DB_NAME + "." + TABLE_NATIVE + "  tags( \"123456abcdef\")  values(now, NULL)");
        ResultSet resultSet = statement.executeQuery("select c1, t1 from " + DB_NAME + "." + TABLE_NATIVE);
        resultSet.next();
        Assert.assertArrayEquals(null, resultSet.getBytes(1));
        resultSet.close();
    }

    @Before
    public void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + HOST + ":" + TestEnvUtil.getJniPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "C");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connection = DriverManager.getConnection(url, properties);
        statement = connection.createStatement();
        statement.executeUpdate("drop database if exists " + DB_NAME);
        statement.executeUpdate("create database if not exists " + DB_NAME);
        statement.executeUpdate("use " + DB_NAME);
        statement.executeUpdate("create table " + TABLE_NATIVE + " (ts timestamp, c1 blob)  tags(t1 varchar(20))");
    }

    @After
    public void after() {
        try {
            if (statement != null && !statement.isClosed()) {
                statement.executeUpdate("drop database if exists " + DB_NAME);
                statement.close();
            }
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            // ignore
        }
    }
}

