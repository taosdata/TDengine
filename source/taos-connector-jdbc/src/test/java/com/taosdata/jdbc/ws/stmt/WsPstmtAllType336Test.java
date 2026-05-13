package com.taosdata.jdbc.ws.stmt;

import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.StringUtils;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import com.taosdata.jdbc.ws.TSWSPreparedStatement;
import io.netty.util.ResourceLeakDetector;
import org.junit.*;

import java.math.BigInteger;
import java.sql.*;
import java.util.Collections;
import java.util.Properties;

public class WsPstmtAllType336Test {
            final String dbName = TestUtils.camelToSnake(WsPstmtAllTypeTest.class);
    final String tableName = "wpt";
    final String stableName = "swpt";

    final String tableName2 = "unsigned_stable";
    Connection connection;
    static final String TEST_STR = "20160601";
    static final byte[] expectedVarBinary = StringUtils.hexToBytes(TEST_STR);
    static final byte[] expectedGeometry = StringUtils.hexToBytes("0101000000000000000000F03F0000000000000040");

    @Test
    public void testExecuteUpdate() throws SQLException {
        String sql = "insert into " + dbName + "." + tableName + " values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        PreparedStatement statement = connection.prepareStatement(sql);
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

        statement.executeUpdate();

        ResultSet resultSet = statement.executeQuery("select * from " + dbName + "." + tableName);
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

        Assert.assertEquals(new Date(current), resultSet.getDate(1));
        Assert.assertEquals(new Time(current), resultSet.getTime(1));
        Assert.assertEquals(new Timestamp(current), resultSet.getTimestamp(1));
        Assert.assertEquals(7.7, resultSet.getBigDecimal(7).doubleValue(), 0.000001);

        resultSet.close();
        statement.close();
    }

    @Test
    public void testExecuteUpdate2() throws SQLException {
        String sql = "insert into stb_1 using " + dbName + "." + stableName + " tags (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) values (?, ?)";
        TSWSPreparedStatement statement = connection.prepareStatement(sql).unwrap(TSWSPreparedStatement.class);
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
        statement.setTagInt(14,  TSDBConstants.MAX_UNSIGNED_SHORT);
        statement.setTagLong(15,  TSDBConstants.MAX_UNSIGNED_INT);
        statement.setTagBigInteger(16, new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG));

        statement.setTimestamp(0, Collections.singletonList(current));
        statement.setByte(1, Collections.singletonList((byte) 2));
        statement.columnDataAddBatch();
        statement.columnDataExecuteBatch();

        ResultSet resultSet = statement.executeQuery("select * from " + dbName + "." + stableName);
        resultSet.next();
        Assert.assertEquals(new Timestamp(current), resultSet.getTimestamp(1));
        Assert.assertEquals((byte) 2, resultSet.getByte(2));

        Assert.assertEquals(new Timestamp(current), resultSet.getTimestamp(3));
        Assert.assertEquals((byte) 2, resultSet.getByte(4));
        Assert.assertEquals((short) 3, resultSet.getShort(5));
        Assert.assertEquals(4, resultSet.getInt(6));
        Assert.assertEquals(5L, resultSet.getLong(7));
        Assert.assertEquals(6.6f, resultSet.getFloat(8), 0.0001);
        Assert.assertEquals(7.7, resultSet.getDouble(9), 0.0001);
        Assert.assertTrue(resultSet.getBoolean(10));
        Assert.assertEquals("你好", resultSet.getString(11));
        Assert.assertEquals("世界", resultSet.getString(12));
        Assert.assertEquals("hello world", resultSet.getString(13));

        Assert.assertArrayEquals(expectedVarBinary, resultSet.getBytes(14));
        Assert.assertArrayEquals(expectedGeometry, resultSet.getBytes(15));

        Assert.assertEquals(TSDBConstants.MAX_UNSIGNED_BYTE, resultSet.getShort(16));
        Assert.assertEquals(TSDBConstants.MAX_UNSIGNED_SHORT, resultSet.getInt(17));
        Assert.assertEquals(TSDBConstants.MAX_UNSIGNED_INT, resultSet.getLong(18));
        Assert.assertEquals(new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG), resultSet.getObject(19));

        resultSet.close();
        statement.close();
    }

    @Test
    public void testExecuteCriticalValue() throws SQLException {
        String sql = "insert into " + dbName + "." + tableName + " values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        TSWSPreparedStatement statement = connection.prepareStatement(sql).unwrap(TSWSPreparedStatement.class);
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

        statement.executeUpdate();
        statement.close();
    }

    @Test(expected = SQLException.class)
    public void testUtinyIntOutOfRange() throws SQLException {
        String sql = "insert into " + dbName + "." + tableName2 + " values(?, ?, ?, ?, ?)";
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setTimestamp(1, new Timestamp(0));
        statement.setShort(2, (short)(TSDBConstants.MAX_UNSIGNED_BYTE + 1));
        statement.setInt(3, TSDBConstants.MAX_UNSIGNED_SHORT);
        statement.setLong(4, TSDBConstants.MAX_UNSIGNED_INT);
        statement.setObject(5, new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG));

        statement.executeUpdate();
        statement.close();
    }
    @Test(expected = SQLException.class)
    public void testUtinyIntOutOfRange2() throws SQLException {
        String sql = "insert into " + dbName + "." + tableName2 + " values(?, ?, ?, ?, ?)";
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setTimestamp(1, new Timestamp(0));
        statement.setShort(2, (short) -1);
        statement.setInt(3, TSDBConstants.MAX_UNSIGNED_SHORT);
        statement.setLong(4, TSDBConstants.MAX_UNSIGNED_INT);
        statement.setObject(5, new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG));

        statement.executeUpdate();
        statement.close();
    }

    @Test(expected = SQLException.class)
    public void testUShortOutOfRange() throws SQLException {
        String sql = "insert into " + dbName + "." + tableName2 + " values(?, ?, ?, ?, ?)";
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setTimestamp(1, new Timestamp(0));
        statement.setShort(2, (short) 0);
        statement.setInt(3, TSDBConstants.MAX_UNSIGNED_SHORT + 1);
        statement.setLong(4, TSDBConstants.MAX_UNSIGNED_INT);
        statement.setObject(5, new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG));

        statement.executeUpdate();
        statement.close();
    }

    @Test(expected = SQLException.class)
    public void testUShortOutOfRange2() throws SQLException {
        String sql = "insert into " + dbName + "." + tableName2 + " values(?, ?, ?, ?, ?)";
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setTimestamp(1, new Timestamp(0));
        statement.setShort(2, (short) 0);
        statement.setInt(3, -1);
        statement.setLong(4, TSDBConstants.MAX_UNSIGNED_INT);
        statement.setObject(5, new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG));

        statement.executeUpdate();
        statement.close();
    }

    @Test(expected = SQLException.class)
    public void testUIntOutOfRange() throws SQLException {
        String sql = "insert into " + dbName + "." + tableName2 + " values(?, ?, ?, ?, ?)";
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setTimestamp(1, new Timestamp(0));
        statement.setShort(2, (short) 0);
        statement.setInt(3, 0);
        statement.setLong(4, TSDBConstants.MAX_UNSIGNED_INT + 1);
        statement.setObject(5, new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG));

        statement.executeUpdate();
        statement.close();
    }

    @Test(expected = SQLException.class)
    public void testUIntOutOfRange2() throws SQLException {
        String sql = "insert into " + dbName + "." + tableName2 + " values(?, ?, ?, ?, ?)";
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setTimestamp(1, new Timestamp(0));
        statement.setShort(2, (short) 0);
        statement.setInt(3, 0);
        statement.setLong(4, -1L);
        statement.setObject(5, new BigInteger(TSDBConstants.MAX_UNSIGNED_LONG));

        statement.executeUpdate();
        statement.close();
    }

    @Test(expected = SQLException.class)
    public void testULongOutOfRange() throws SQLException {
        String sql = "insert into " + dbName + "." + tableName2 + " values(?, ?, ?, ?, ?)";
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setTimestamp(1, new Timestamp(0));
        statement.setShort(2, (short) 0);
        statement.setInt(3, 0);
        statement.setLong(4, 0);
        statement.setObject(5, new BigInteger("18446744073709551616"));

        statement.executeUpdate();
        statement.close();
    }
    @Test(expected = SQLException.class)
    public void testULongOutOfRange2() throws SQLException {
        String sql = "insert into " + dbName + "." + tableName2 + " values(?, ?, ?, ?, ?)";
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setTimestamp(1, new Timestamp(0));
        statement.setShort(2, (short) 0);
        statement.setInt(3, 0);
        statement.setLong(4, 0);
        statement.setObject(5, new BigInteger("-1"));

        statement.executeUpdate();
        statement.close();
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
        Statement statement = connection.createStatement();
        statement.execute("drop database if exists " + dbName);
        statement.execute("create database " + dbName + " keep 36500");
        statement.execute("use " + dbName);
        statement.execute("create table if not exists " + dbName + "." + tableName +
                "(ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, " +
                "c5 float, c6 double, c7 bool, c8 binary(10), c9 nchar(10), c10 varchar(20), c11 varbinary(100), c12 geometry(100)," +
                "c13 tinyint unsigned, c14 smallint unsigned, c15 int unsigned, c16 bigint unsigned)");

        statement.execute("create stable if not exists " + dbName + "." + stableName +
                "(ts timestamp, c1 tinyint) tags (t1 timestamp, t2 tinyint, t3 smallint, t4 int, t5 bigint, " +
                "t6 float, t7 double, t8 bool, t9 binary(10), t10 nchar(10), t11 varchar(20), t12 varbinary(100), t13 geometry(100)," +
                "t14 tinyint unsigned, t15 smallint unsigned, t16 int unsigned, t17 bigint unsigned)");

        statement.execute("create table if not exists " + dbName + "." + tableName2 +
                "(ts timestamp, " +
                "c1 tinyint unsigned, c2 smallint unsigned, c3 int unsigned, c4 bigint unsigned)");

        statement.close();
    }

    @After
    public void after() throws SQLException {

        try (Statement statement = connection.createStatement()){
            statement.execute("drop database if exists " + dbName);
        }
        connection.close();
    }

    @BeforeClass
    public static void setUp() {
        TestUtils.runIn336();
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    @AfterClass
    public static void tearDown() {
        System.gc();
    }
}