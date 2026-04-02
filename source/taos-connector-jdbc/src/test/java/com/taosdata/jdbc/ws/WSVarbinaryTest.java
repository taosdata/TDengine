package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.StringUtils;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

public class WSVarbinaryTest {
    static final String HOST = TestEnvUtil.getHost();
    static final String DB_NAME = TestUtils.camelToSnake(WSVarbinaryTest.class);
    static final String TABLE_NATIVE = "varbinary_noraml";
    static final String TABLE_STMT = "varbinary_stmt";
    static Connection connection;
    static Statement statement;

    static final String TEST_STR = "20160601";
    static final byte[] expectedArray = StringUtils.hexToBytes(TEST_STR);

    @Test
    public void testInsert() throws Exception {
        statement.executeUpdate("insert into " + DB_NAME + "." + TABLE_NATIVE + " values(now, \"\\x" + TEST_STR + "\")");
        ResultSet resultSet = statement.executeQuery("select c1 from " + DB_NAME + "." + TABLE_NATIVE);

        resultSet.next();
        byte[] result1 = resultSet.getBytes(1);
        Assert.assertArrayEquals(expectedArray, result1);
    }

    @Test
    public void testPrepare() throws SQLException {
        TSWSPreparedStatement preparedStatement = (TSWSPreparedStatement) connection.prepareStatement("insert into " + DB_NAME + "." + TABLE_STMT + " values (?, ?)");
        preparedStatement.setTimestamp(1, new Timestamp(System.currentTimeMillis()));

        preparedStatement.setBytes(2, expectedArray);
        preparedStatement.addBatch();
        preparedStatement.executeBatch();
        ResultSet resultSet = statement.executeQuery("select c1 from " + DB_NAME + "." + TABLE_STMT);
        while (resultSet.next()) {
            Assert.assertArrayEquals(expectedArray, resultSet.getBytes(1));
        }
    }

    @Test
    public void testPrepareOld() throws SQLException {
        TSWSPreparedStatement preparedStatement = (TSWSPreparedStatement) connection.prepareStatement("insert into " + DB_NAME + "." + TABLE_STMT + " values (?, ?)");

        long current = System.currentTimeMillis();
        ArrayList<Long> tsList = new ArrayList<>();
        tsList.add(current);
        preparedStatement.setTimestamp(0, tsList);

        ArrayList<byte[]> list1 = new ArrayList<>();
        list1.add(expectedArray);
        preparedStatement.setVarbinary(1, list1, 20);

        preparedStatement.columnDataAddBatch();
        preparedStatement.columnDataExecuteBatch();
        ResultSet resultSet = statement.executeQuery("select c1 from " + DB_NAME + "." + TABLE_STMT);
        while (resultSet.next()) {
            Assert.assertArrayEquals(expectedArray, resultSet.getBytes(1));
        }
    }

    @BeforeClass
    public static void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + HOST + ":" + TestEnvUtil.getWsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword() + "&batchfetch=true";
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "C");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connection = DriverManager.getConnection(url, properties);
        statement = connection.createStatement();
        statement.executeUpdate("drop database if exists " + DB_NAME);
        statement.executeUpdate("create database if not exists " + DB_NAME);
        statement.executeUpdate("use " + DB_NAME);
        statement.executeUpdate("create table " + TABLE_NATIVE + " (ts timestamp, c1 varbinary(20))");
        statement.executeUpdate("create table " + TABLE_STMT + " (ts timestamp, c1 varbinary(20))");
    }

    @AfterClass
    public static void after() {
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

