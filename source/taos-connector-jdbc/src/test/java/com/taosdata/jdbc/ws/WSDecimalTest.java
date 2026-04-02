package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.StringUtils;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.*;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class WSDecimalTest {
    static final String HOST = TestEnvUtil.getHost();
    static final String DB_NAME = TestUtils.camelToSnake(WSDecimalTest.class);
    static final String TABLE_NORMAL = "decimal_normal";
    static final String TABLE_STMT = "decimal_stmt";
    static Connection connection;
    static Statement statement;

    static final String TEST_STR = "20160601";
    static final byte[] expectedArray = StringUtils.hexToBytes(TEST_STR);

    static final String DECIMAL_VALUE_1 = "12.32";
    static final String DECIMAL_VALUE_2 = "1234567890111.12345678";

    @BeforeClass
    public static void checkEnvironment() {
        TestUtils.runInMain();
    }

    @Test
    public void testInsert() throws Exception {
        statement.executeUpdate("insert into subt_a using " + DB_NAME + "." + TABLE_NORMAL + "  tags(1)  values(now, \"12.32\", \"1234567890111.12345678\")");
        try (ResultSet resultSet = statement.executeQuery("select d1, d2 from " + DB_NAME + "." + TABLE_NORMAL)) {
            resultSet.next();
            Assert.assertEquals(DECIMAL_VALUE_1, resultSet.getString(1));
            Assert.assertEquals(0, new BigDecimal(DECIMAL_VALUE_2).compareTo(resultSet.getBigDecimal(2)));
        }
    }
    @Test
    public void testInsertNull() throws Exception {
        statement.executeUpdate("insert into subt_a using " + DB_NAME + "." + TABLE_NORMAL + "  tags(1)  values(now, NULL, NULL)");
        try ( ResultSet resultSet = statement.executeQuery("select d1, d2 from " + DB_NAME + "." + TABLE_NORMAL)) {
            resultSet.next();
            Assert.assertNull(resultSet.getBigDecimal(1));
            Assert.assertNull(resultSet.getBigDecimal(2));
        }
    }
    @Test
    public void testPrepareExt() throws SQLException {
        try (TSWSPreparedStatement preparedStatement = (TSWSPreparedStatement) connection.prepareStatement("insert into ? using " + DB_NAME + "." + TABLE_STMT + "   tags(?)  values (?, ?, ?)")) {
            preparedStatement.setTableName("subt_b");
            preparedStatement.setTagInt(0, 1);

            long current = System.currentTimeMillis();
            List<Long> tsList = new ArrayList<>();
            tsList.add(current);
            tsList.add(current + 1);
            preparedStatement.setTimestamp(0, tsList);

            List<BigDecimal> list = new ArrayList<>();
            list.add(new BigDecimal(DECIMAL_VALUE_1));
            list.add(null);
            preparedStatement.setBigDecimal(1, list);

            List<BigDecimal> list2 = new ArrayList<>();
            list2.add(new BigDecimal(DECIMAL_VALUE_2));
            list2.add(null);
            preparedStatement.setBigDecimal(2, list2);

            preparedStatement.columnDataAddBatch();
            preparedStatement.columnDataExecuteBatch();

            try (PreparedStatement preparedStatement2 = connection.prepareStatement("select d1, d2 from " + DB_NAME + "." + TABLE_STMT);
                 ResultSet resultSet = preparedStatement2.executeQuery()) {
                resultSet.next();
                Assert.assertEquals(0, resultSet.getBigDecimal(1).compareTo(new BigDecimal(DECIMAL_VALUE_1)));
                Assert.assertEquals(0, resultSet.getBigDecimal(2).compareTo(new BigDecimal(DECIMAL_VALUE_2)));
            }
        }
    }

    @Test
    public void testPrepareStd() throws SQLException {
        long current = System.currentTimeMillis();
        try (PreparedStatement preparedStatement = connection.prepareStatement("insert into " + DB_NAME + "." + TABLE_STMT + " (tbname, t1, ts, d1, d2) values (?, ?, ?, ?, ?)")) {
            preparedStatement.setString(1, "subt_b");
            preparedStatement.setInt(2, 1);

            preparedStatement.setTimestamp(3, new Timestamp(current));
            preparedStatement.setBigDecimal(4, new BigDecimal(DECIMAL_VALUE_1));
            preparedStatement.setBigDecimal(5, new BigDecimal(DECIMAL_VALUE_2));

            preparedStatement.addBatch();
            preparedStatement.executeBatch();
        }

        try (PreparedStatement preparedStatement2 = connection.prepareStatement("select d1, d2 from " + DB_NAME + "." + TABLE_STMT);
             ResultSet resultSet = preparedStatement2.executeQuery()) {
            resultSet.next();
            Assert.assertEquals(0, resultSet.getBigDecimal(1).compareTo(new BigDecimal(DECIMAL_VALUE_1)));
            Assert.assertEquals(0, resultSet.getBigDecimal(2).compareTo(new BigDecimal(DECIMAL_VALUE_2)));
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
        statement.executeUpdate("create table " + TABLE_NORMAL + " (ts timestamp, d1 decimal(4,2), d2 decimal(30,10)) tags(t1 int)");
        statement.executeUpdate("create table " + TABLE_STMT + " (ts timestamp, d1 decimal(4,2), d2 decimal(30,10)) tags(t1 int)");
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

