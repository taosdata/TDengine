package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.common.TDBlob;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.StringUtils;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.*;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

public class WSBlobTest {
    static final String HOST = TestEnvUtil.getHost();
    static final String DB_NAME = TestUtils.camelToSnake(WSBlobTest.class);
    static final String TABLE_NORMAL = "blob_normal";
    static final String TABLE_STMT = "blob_stmt";
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
        statement.executeUpdate("insert into subt_a using " + DB_NAME + "." + TABLE_NORMAL + "  tags( \"123456abcdef\")  values(now, \"\\x" + TEST_STR + "\")");
        try (ResultSet resultSet = statement.executeQuery("select c1, t1 from " + DB_NAME + "." + TABLE_NORMAL)) {
            resultSet.next();
            Assert.assertArrayEquals(expectedArray, resultSet.getBytes(1));
        }
    }
    @Test
    public void testInsertNull() throws Exception {
        statement.executeUpdate("insert into subt_a using " + DB_NAME + "." + TABLE_NORMAL + "  tags( \"123456abcdef\")  values(now, NULL)");
        try (ResultSet resultSet = statement.executeQuery("select c1, t1 from " + DB_NAME + "." + TABLE_NORMAL)) {
            resultSet.next();
            Assert.assertArrayEquals(null, resultSet.getBytes(1));
        }
    }
    @Test
    public void testPrepareExt() throws SQLException {
        try (TSWSPreparedStatement preparedStatement = (TSWSPreparedStatement) connection.prepareStatement("insert into ? using " + DB_NAME + "." + TABLE_STMT + "   tags(?)  values (?, ?)")) {
            preparedStatement.setTableName("subt_b");
            preparedStatement.setTagString(0, TEST_STR);

            long current = System.currentTimeMillis();
            ArrayList<Long> tsList = new ArrayList<>();
            tsList.add(current);
            tsList.add(current + 1);
            preparedStatement.setTimestamp(0, tsList);

            ArrayList<Blob> list = new ArrayList<>();
            list.add(new TDBlob(TEST_STR.getBytes(StandardCharsets.UTF_8), true));
            list.add(null);
            preparedStatement.setBlob(1, list, 200);

            preparedStatement.columnDataAddBatch();
            preparedStatement.columnDataExecuteBatch();
        }

        try (ResultSet resultSet = statement.executeQuery("select c1 from " + DB_NAME + "." + TABLE_STMT + " order by ts asc")) {
            resultSet.next();
            Assert.assertArrayEquals(TEST_STR.getBytes(StandardCharsets.UTF_8), resultSet.getBytes(1));
            resultSet.next();
            Assert.assertArrayEquals(null, resultSet.getBytes(1));
        }
    }

    @Test
    public void testPrepareStd() throws SQLException {
        long current = System.currentTimeMillis();
        try (PreparedStatement preparedStatement = connection.prepareStatement("insert into " + DB_NAME + "." + TABLE_STMT + " (tbname, t1, ts, c1) values (?, ?, ?, ?)")) {
            preparedStatement.setString(1, "subt_b");
            preparedStatement.setString(2, TEST_STR);

            preparedStatement.setTimestamp(3, new Timestamp(current));
            preparedStatement.setBlob(4, new TDBlob(TEST_STR.getBytes(StandardCharsets.UTF_8), true));

            preparedStatement.addBatch();
            preparedStatement.executeBatch();
        }

        try (ResultSet resultSet = statement.executeQuery("select c1 from " + DB_NAME + "." + TABLE_STMT)) {
            resultSet.next();
            Assert.assertArrayEquals(TEST_STR.getBytes(StandardCharsets.UTF_8), resultSet.getBytes(1));
        }
    }

    @Before
    public void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS-WS://" + HOST + ":" + TestEnvUtil.getWsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "C");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connection = DriverManager.getConnection(url, properties);
        statement = connection.createStatement();
        statement.executeUpdate("drop database if exists " + DB_NAME);
        statement.executeUpdate("create database if not exists " + DB_NAME);
        statement.executeUpdate("use " + DB_NAME);
        statement.executeUpdate("create table " + TABLE_NORMAL + " (ts timestamp, c1 blob) tags(t1 varchar(20))");
        statement.executeUpdate("create table " + TABLE_STMT + " (ts timestamp, c1 blob) tags(t1 varchar(20))");
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
            // Log and ignore cleanup failures to avoid hiding resource issues completely
            e.printStackTrace();
        }
    }
}

