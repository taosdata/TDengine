package com.taosdata.jdbc.ws.stmt;

import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.common.TDBlob;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.StringUtils;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import com.taosdata.jdbc.ws.TSWSPreparedStatement;
import io.netty.util.ResourceLeakDetector;
import org.junit.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.Collections;
import java.util.Properties;

public class WsPstmtAllTypeTest {
            final String db_name = TestUtils.camelToSnake(WsPstmtAllTypeTest.class);
    final String tableName = "wpt";
    final String stableName = "swpt";

    final String tableName2 = "unsigned_stable";
    Connection connection;
    static final String TEST_STR = "20160601";
    static final byte[] expectedVarBinary = StringUtils.hexToBytes(TEST_STR);
    static final byte[] expectedGeometry = StringUtils.hexToBytes("0101000000000000000000F03F0000000000000040");
    static final String DECIMAL_VALUE_1 = "12.32";
    static final String DECIMAL_VALUE_2 = "1234567890111.12345678";

    @Test
    public void testExecuteUpdate() throws SQLException {
        String sql = "insert into " + db_name + "." + tableName + " values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            long current = System.currentTimeMillis();
            statement.setTimestamp(1, new Timestamp(current));

            statement.setByte(2, (byte) 2);
            statement.setShort(3, (short) 3);
            statement.setInt(4, 4);
            statement.setLong(5, 5L);
            statement.setFloat(6, 6.6f);
            statement.setDouble(7, 7.7);
            statement.setBoolean(8, true);
            statement.setString(9, "你好");
            statement.setNString(10, "世界");
            statement.setString(11, "hello world");
            statement.setBytes(12, expectedVarBinary);
            statement.setBytes(13, expectedGeometry);

            statement.setShort(14, TSDBConstants.MAX_UNSIGNED_BYTE);
            statement.setInt(15, TSDBConstants.MAX_UNSIGNED_SHORT);
            statement.setLong(16, TSDBConstants.MAX_UNSIGNED_INT);
            statement.setObject(17, new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG));
            statement.setBlob(18, new TDBlob("blob".getBytes(), true));

            statement.setBigDecimal(19, new BigDecimal(DECIMAL_VALUE_1));
            statement.setBigDecimal(20, new BigDecimal(DECIMAL_VALUE_2));

            statement.executeUpdate();

            try (ResultSet resultSet = statement.executeQuery("select * from " + db_name + "." + tableName)) {
                resultSet.next();
                Assert.assertEquals(new Timestamp(current), resultSet.getTimestamp(1));
                Assert.assertEquals((byte) 2, resultSet.getByte(2));
                Assert.assertEquals((short) 3, resultSet.getShort(3));
                Assert.assertEquals(4, resultSet.getInt(4));
                Assert.assertEquals(5L, resultSet.getLong(5));
                Assert.assertEquals(6.6f, resultSet.getFloat(6), 0.0001);
                Assert.assertEquals(7.7, resultSet.getDouble(7), 0.0001);
                Assert.assertTrue(resultSet.getBoolean(8));
                Assert.assertEquals("你好", resultSet.getString(9));
                Assert.assertEquals("世界", resultSet.getString(10));
                Assert.assertEquals("hello world", resultSet.getString(11));
                Assert.assertArrayEquals(expectedVarBinary, resultSet.getBytes(12));
                Assert.assertArrayEquals(expectedGeometry, resultSet.getBytes(13));

                Assert.assertEquals(TSDBConstants.MAX_UNSIGNED_BYTE, resultSet.getShort(14));
                Assert.assertEquals(TSDBConstants.MAX_UNSIGNED_SHORT, resultSet.getInt(15));
                Assert.assertEquals(TSDBConstants.MAX_UNSIGNED_INT, resultSet.getLong(16));
                Assert.assertEquals(new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG), resultSet.getObject(17));
                Assert.assertArrayEquals("blob".getBytes(), resultSet.getBlob(18).getBytes(1, 4));

                Assert.assertEquals(0, resultSet.getBigDecimal(19).compareTo(new BigDecimal(DECIMAL_VALUE_1)));
                Assert.assertEquals(0, resultSet.getBigDecimal(20).compareTo(new BigDecimal(DECIMAL_VALUE_2)));

                Date date = new Date(current);
                Assert.assertEquals(new Date(current), resultSet.getDate(1));
                Assert.assertEquals(new Time(current), resultSet.getTime(1));
                Assert.assertEquals(new Timestamp(current), resultSet.getTimestamp(1));
                Assert.assertEquals(7.7, resultSet.getBigDecimal(7).doubleValue(), 0.000001);
            }
        }
    }

    @Test
    public void testExecuteUpdate2() throws SQLException {
        String sql = "insert into stb_1 using " + db_name + "." + stableName + " tags (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) values (?, ?, ?)";
        try (TSWSPreparedStatement statement = connection.prepareStatement(sql).unwrap(TSWSPreparedStatement.class)) {
            long current = System.currentTimeMillis();
            statement.setTagTimestamp(0, new Timestamp(current));
            statement.setTagByte(1, (byte) 2);
            statement.setTagShort(2, (short) 3);
            statement.setTagInt(3, 4);
            statement.setTagLong(4, 5L);
            statement.setTagFloat(5, 6.6f);
            statement.setTagDouble(6, 7.7);
            statement.setTagBoolean(7, true);
            statement.setTagString(8, "你好");
            statement.setTagNString(9, "世界");
            statement.setTagString(10, "hello world");
            statement.setTagVarbinary(11, expectedVarBinary);
            statement.setTagGeometry(12, expectedGeometry);
            statement.setTagShort(13, TSDBConstants.MAX_UNSIGNED_BYTE);
            statement.setTagInt(14, TSDBConstants.MAX_UNSIGNED_SHORT);
            statement.setTagLong(15, TSDBConstants.MAX_UNSIGNED_INT);
            statement.setTagBigInteger(16, new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG));

            statement.setTimestamp(0, Collections.singletonList(current));
            statement.setByte(1, Collections.singletonList((byte) 2));
            statement.setBlob(2, Collections.singletonList(new TDBlob("blob".getBytes(), true)), 100);
            statement.columnDataAddBatch();
            statement.columnDataExecuteBatch();

            try (ResultSet resultSet = statement.executeQuery("select * from " + db_name + "." + stableName)) {
                resultSet.next();
                Assert.assertEquals(new Timestamp(current), resultSet.getTimestamp(1));
                Assert.assertEquals((byte) 2, resultSet.getByte(2));
                Assert.assertArrayEquals("blob".getBytes(), resultSet.getBlob(3).getBytes(1, 4));

                Assert.assertEquals(new Timestamp(current), resultSet.getTimestamp(4));
                Assert.assertEquals((byte) 2, resultSet.getByte(5));
                Assert.assertEquals((short) 3, resultSet.getShort(6));
                Assert.assertEquals(4, resultSet.getInt(7));
                Assert.assertEquals(5L, resultSet.getLong(8));
                Assert.assertEquals(6.6f, resultSet.getFloat(9), 0.0001);
                Assert.assertEquals(7.7, resultSet.getDouble(10), 0.0001);
                Assert.assertTrue(resultSet.getBoolean(11));
                Assert.assertEquals("你好", resultSet.getString(12));
                Assert.assertEquals("世界", resultSet.getString(13));
                Assert.assertEquals("hello world", resultSet.getString(14));

                Assert.assertArrayEquals(expectedVarBinary, resultSet.getBytes(15));
                Assert.assertArrayEquals(expectedGeometry, resultSet.getBytes(16));

                Assert.assertEquals(TSDBConstants.MAX_UNSIGNED_BYTE, resultSet.getShort(17));
                Assert.assertEquals(TSDBConstants.MAX_UNSIGNED_SHORT, resultSet.getInt(18));
                Assert.assertEquals(TSDBConstants.MAX_UNSIGNED_INT, resultSet.getLong(19));
                Assert.assertEquals(new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG), resultSet.getObject(20));
            }
        }
    }

    @Test
    public void testExecuteUpdateWithSetObject() throws SQLException {
        String sql = "insert into " + db_name + "." + tableName + " values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            long current = System.currentTimeMillis();
            statement.setObject(1, new Timestamp(current));

            statement.setObject(2, (byte) 2);
            statement.setObject(3, (short) 3);
            statement.setObject(4, 4);
            statement.setObject(5, 5L);
            statement.setObject(6, 6.6f);
            statement.setObject(7, 7.7);
            statement.setObject(8, true);
            statement.setObject(9, "你好");
            statement.setObject(10, "世界");
            statement.setObject(11, "hello world");
            statement.setObject(12, expectedVarBinary);
            statement.setObject(13, expectedGeometry);

            statement.setObject(14, TSDBConstants.MAX_UNSIGNED_BYTE);
            statement.setObject(15, TSDBConstants.MAX_UNSIGNED_SHORT);
            statement.setObject(16, TSDBConstants.MAX_UNSIGNED_INT);
            statement.setObject(17, new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG));
            statement.setObject(18, new TDBlob("blob".getBytes(), true));

            statement.setObject(19, new BigDecimal(DECIMAL_VALUE_1));
            statement.setObject(20, new BigDecimal(DECIMAL_VALUE_2));

            statement.executeUpdate();

            try (ResultSet resultSet = statement.executeQuery("select * from " + db_name + "." + tableName)) {
                resultSet.next();
                Assert.assertEquals(new Timestamp(current), resultSet.getTimestamp(1));
                Assert.assertEquals((byte) 2, resultSet.getByte(2));
                Assert.assertEquals((short) 3, resultSet.getShort(3));
                Assert.assertEquals(4, resultSet.getInt(4));
                Assert.assertEquals(5L, resultSet.getLong(5));
                Assert.assertEquals(6.6f, resultSet.getFloat(6), 0.0001);
                Assert.assertEquals(7.7, resultSet.getDouble(7), 0.0001);
                Assert.assertTrue(resultSet.getBoolean(8));
                Assert.assertEquals("你好", resultSet.getString(9));
                Assert.assertEquals("世界", resultSet.getString(10));
                Assert.assertEquals("hello world", resultSet.getString(11));
                Assert.assertArrayEquals(expectedVarBinary, resultSet.getBytes(12));
                Assert.assertArrayEquals(expectedGeometry, resultSet.getBytes(13));

                Assert.assertEquals(TSDBConstants.MAX_UNSIGNED_BYTE, resultSet.getShort(14));
                Assert.assertEquals(TSDBConstants.MAX_UNSIGNED_SHORT, resultSet.getInt(15));
                Assert.assertEquals(TSDBConstants.MAX_UNSIGNED_INT, resultSet.getLong(16));
                Assert.assertEquals(new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG), resultSet.getObject(17));
                Assert.assertArrayEquals("blob".getBytes(), resultSet.getBlob(18).getBytes(1, 4));

                Assert.assertEquals(0, resultSet.getBigDecimal(19).compareTo(new BigDecimal(DECIMAL_VALUE_1)));
                Assert.assertEquals(0, resultSet.getBigDecimal(20).compareTo(new BigDecimal(DECIMAL_VALUE_2)));

                Assert.assertEquals(new Date(current), resultSet.getDate(1));
                Assert.assertEquals(new Time(current), resultSet.getTime(1));
                Assert.assertEquals(new Timestamp(current), resultSet.getTimestamp(1));
                Assert.assertEquals(7.7, resultSet.getBigDecimal(7).doubleValue(), 0.000001);
            }
        }
    }

    @Test
    public void testExecuteCriticalValue() throws SQLException {
        String sql = "insert into " + db_name + "." + tableName + " values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (TSWSPreparedStatement statement = connection.prepareStatement(sql).unwrap(TSWSPreparedStatement.class)) {
            statement.setTimestamp(1, new Timestamp(0));
            statement.setByte(2, (byte) 127);
            statement.setShort(3, (short) 32767);
            statement.setInt(4, 2147483647);
            statement.setLong(5, 9223372036854775807L);
            statement.setFloat(6, Float.MAX_VALUE);
            statement.setDouble(7, Double.MAX_VALUE);
            statement.setBoolean(8, true);
            statement.setString(9, "ABC");
            statement.setNString(10, "涛思数据");
            statement.setString(11, "陶");
            statement.setVarbinary(12, expectedVarBinary);
            statement.setGeometry(13, expectedGeometry);
            statement.setShort(14, TSDBConstants.MAX_UNSIGNED_BYTE);
            statement.setInt(15, TSDBConstants.MAX_UNSIGNED_SHORT);
            statement.setLong(16, TSDBConstants.MAX_UNSIGNED_INT);
            statement.setObject(17, new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG));
            statement.setBlob(18, new TDBlob("blob".getBytes(), true));

            statement.setBigDecimal(19, new BigDecimal(DECIMAL_VALUE_1));
            statement.setBigDecimal(20, new BigDecimal(DECIMAL_VALUE_2));

            statement.executeUpdate();
        }
    }

    @Test(expected = SQLException.class)
    public void testUtinyIntOutOfRange() throws SQLException {
        String sql = "insert into " + db_name + "." + tableName2 + " values(?, ?, ?, ?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setTimestamp(1, new Timestamp(0));
            statement.setShort(2, (short) (TSDBConstants.MAX_UNSIGNED_BYTE + 1));
            statement.setInt(3, TSDBConstants.MAX_UNSIGNED_SHORT);
            statement.setLong(4, TSDBConstants.MAX_UNSIGNED_INT);
            statement.setObject(5, new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG));

            statement.executeUpdate();
        }
    }

    @Test(expected = SQLException.class)
    public void testUtinyIntOutOfRange2() throws SQLException {
        String sql = "insert into " + db_name + "." + tableName2 + " values(?, ?, ?, ?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setTimestamp(1, new Timestamp(0));
            statement.setShort(2, (short) -1);
            statement.setInt(3, TSDBConstants.MAX_UNSIGNED_SHORT);
            statement.setLong(4, TSDBConstants.MAX_UNSIGNED_INT);
            statement.setObject(5, new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG));

            statement.executeUpdate();
        }
    }

    @Test(expected = SQLException.class)
    public void testUShortOutOfRange() throws SQLException {
        String sql = "insert into " + db_name + "." + tableName2 + " values(?, ?, ?, ?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setTimestamp(1, new Timestamp(0));
            statement.setShort(2, (short) 0);
            statement.setInt(3, TSDBConstants.MAX_UNSIGNED_SHORT + 1);
            statement.setLong(4, TSDBConstants.MAX_UNSIGNED_INT);
            statement.setObject(5, new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG));

            statement.executeUpdate();
        }
    }

    @Test(expected = SQLException.class)
    public void testUShortOutOfRange2() throws SQLException {
        String sql = "insert into " + db_name + "." + tableName2 + " values(?, ?, ?, ?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setTimestamp(1, new Timestamp(0));
            statement.setShort(2, (short) 0);
            statement.setInt(3, -1);
            statement.setLong(4, TSDBConstants.MAX_UNSIGNED_INT);
            statement.setObject(5, new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG));

            statement.executeUpdate();
        }
    }

    @Test(expected = SQLException.class)
    public void testUIntOutOfRange() throws SQLException {
        String sql = "insert into " + db_name + "." + tableName2 + " values(?, ?, ?, ?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setTimestamp(1, new Timestamp(0));
            statement.setShort(2, (short) 0);
            statement.setInt(3, 0);
            statement.setLong(4, TSDBConstants.MAX_UNSIGNED_INT + 1);
            statement.setObject(5, new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG));

            statement.executeUpdate();
        }
    }

    @Test(expected = SQLException.class)
    public void testUIntOutOfRange2() throws SQLException {
        String sql = "insert into " + db_name + "." + tableName2 + " values(?, ?, ?, ?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setTimestamp(1, new Timestamp(0));
            statement.setShort(2, (short) 0);
            statement.setInt(3, 0);
            statement.setLong(4, -1L);
            statement.setObject(5, new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG));

            statement.executeUpdate();
        }
    }

    @Test(expected = SQLException.class)
    public void testULongOutOfRange() throws SQLException {
        String sql = "insert into " + db_name + "." + tableName2 + " values(?, ?, ?, ?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setTimestamp(1, new Timestamp(0));
            statement.setShort(2, (short) 0);
            statement.setInt(3, 0);
            statement.setLong(4, 0);
            statement.setObject(5, new BigInteger("18446744073709551616"));

            statement.executeUpdate();
        }
    }

    @Test(expected = SQLException.class)
    public void testULongOutOfRange2() throws SQLException {
        String sql = "insert into " + db_name + "." + tableName2 + " values(?, ?, ?, ?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setTimestamp(1, new Timestamp(0));
            statement.setShort(2, (short) 0);
            statement.setInt(3, 0);
            statement.setLong(4, 0);
            statement.setObject(5, new BigInteger("-1"));

            statement.executeUpdate();
        }
    }

    @Before
    public void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + TestEnvUtil.getHost() + ":" + TestEnvUtil.getRsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword() + "&batchfetch=true";
        } else {
            url += "?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword() + "&batchfetch=true";
        }
        Properties properties = new Properties();
        connection = DriverManager.getConnection(url, properties);
        try (Statement statement = connection.createStatement()) {
            statement.execute("drop database if exists " + db_name);
            statement.execute("create database " + db_name + " keep 36500");
            statement.execute("use " + db_name);
            statement.execute("create table if not exists " + db_name + "." + tableName +
                    "(ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, " +
                    "c5 float, c6 double, c7 bool, c8 binary(10), c9 nchar(10), c10 varchar(20), c11 varbinary(100), c12 geometry(100)," +
                    "c13 tinyint unsigned, c14 smallint unsigned, c15 int unsigned, c16 bigint unsigned, c17 blob, " +
                    "c18 decimal(4,2), c19 decimal(30,10))");

            statement.execute("create stable if not exists " + db_name + "." + stableName +
                    "(ts timestamp, c1 tinyint, c2 blob) tags (t1 timestamp, t2 tinyint, t3 smallint, t4 int, t5 bigint, " +
                    "t6 float, t7 double, t8 bool, t9 binary(10), t10 nchar(10), t11 varchar(20), t12 varbinary(100), t13 geometry(100)," +
                    "t14 tinyint unsigned, t15 smallint unsigned, t16 int unsigned, t17 bigint unsigned)");

            statement.execute("create table if not exists " + db_name + "." + tableName2 +
                    "(ts timestamp, " +
                    "c1 tinyint unsigned, c2 smallint unsigned, c3 int unsigned, c4 bigint unsigned)");
        }
    }

    @After
    public void after() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("drop database if exists " + db_name);
        }
        connection.close();
    }

    @BeforeClass
    public static void setUp() {
        TestUtils.runInMain();
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    @AfterClass
    public static void tearDown() {
        System.gc();
    }
}