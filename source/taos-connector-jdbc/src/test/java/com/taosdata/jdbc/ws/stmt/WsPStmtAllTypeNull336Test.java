package com.taosdata.jdbc.ws.stmt;

import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import com.taosdata.jdbc.ws.TSWSPreparedStatement;
import io.netty.util.ResourceLeakDetector;
import org.junit.*;

import java.sql.*;
import java.util.Collections;
import java.util.Properties;

import static com.taosdata.jdbc.TSDBConstants.*;

public class WsPStmtAllTypeNull336Test {
            final String dbName = TestUtils.camelToSnake(WsPStmtAllTypeNull336Test.class);
    final String tableName = "wpt";
    final String stableName = "swpt";
    Connection connection;

    @Test
    public void testExecuteUpdate() throws SQLException {
        String sql = "insert into " + dbName + "." + tableName + " values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            long current = System.currentTimeMillis();
            statement.setTimestamp(1, new Timestamp(current));
            statement.setNull(2, Types.TINYINT);
            statement.setNull(3, Types.SMALLINT);
            statement.setNull(4, Types.INTEGER);
            statement.setNull(5, Types.BIGINT);
            statement.setNull(6, Types.FLOAT);
            statement.setNull(7, Types.DOUBLE);
            statement.setNull(8, Types.BOOLEAN);

            statement.setString(9, null);
            statement.setNString(10, null);
            statement.setString(11, null);
            statement.setNull(12, Types.VARBINARY);
            statement.setNull(13, Types.VARBINARY);

            // Unsigned types
            statement.setNull(14, Types.SMALLINT);
            statement.setNull(15, Types.INTEGER);
            statement.setNull(16, Types.BIGINT);
            statement.setNull(17, Types.BIGINT);

            statement.executeUpdate();

            try (ResultSet resultSet = statement.executeQuery("select * from " + dbName + "." + tableName)) {
                resultSet.next();
                Assert.assertEquals(resultSet.getTimestamp(1), new Timestamp(current));

                resultSet.getByte(2);
                Assert.assertTrue(resultSet.wasNull());

                resultSet.getShort(3);
                Assert.assertTrue(resultSet.wasNull());

                resultSet.getInt(4);
                Assert.assertTrue(resultSet.wasNull());

                resultSet.getLong(5);
                Assert.assertTrue(resultSet.wasNull());

                resultSet.getFloat(6);
                Assert.assertTrue(resultSet.wasNull());

                resultSet.getDouble(7);
                Assert.assertTrue(resultSet.wasNull());

                resultSet.getBoolean(8);
                Assert.assertTrue(resultSet.wasNull());

                Assert.assertNull(resultSet.getString(9));
                Assert.assertNull(resultSet.getString(10));
                Assert.assertNull(resultSet.getString(11));

                Assert.assertNull(resultSet.getBytes(12));
                Assert.assertNull(resultSet.getBytes(13));

                // Unsigned types
                resultSet.getShort(14);
                Assert.assertTrue(resultSet.wasNull());

                resultSet.getInt(15);
                Assert.assertTrue(resultSet.wasNull());

                resultSet.getLong(16);
                Assert.assertTrue(resultSet.wasNull());

                resultSet.getObject(17);
                Assert.assertTrue(resultSet.wasNull());
            }
        }
    }

    @Test
    public void testExecuteUpdate2() throws SQLException {
        String sql = "insert into stb_1 using " + dbName + "." + stableName + " tags (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) values (?, ?)";
        try (TSWSPreparedStatement statement = connection.prepareStatement(sql).unwrap(TSWSPreparedStatement.class)) {
            long current = System.currentTimeMillis();

            statement.setTagNull(0, TSDB_DATA_TYPE_TIMESTAMP);
            statement.setTagNull(1, TSDB_DATA_TYPE_TINYINT);
            statement.setTagNull(2, TSDB_DATA_TYPE_SMALLINT);
            statement.setTagNull(3, TSDB_DATA_TYPE_INT);
            statement.setTagNull(4, TSDB_DATA_TYPE_BIGINT);
            statement.setTagNull(5, TSDB_DATA_TYPE_FLOAT);
            statement.setTagNull(6, TSDB_DATA_TYPE_DOUBLE);
            statement.setTagNull(7, TSDB_DATA_TYPE_BOOL);

            statement.setTagNull(8, TSDB_DATA_TYPE_BINARY);
            statement.setTagNull(9, TSDB_DATA_TYPE_NCHAR);
            statement.setTagNull(10, TSDB_DATA_TYPE_BINARY);

            statement.setTagNull(11, TSDB_DATA_TYPE_VARBINARY);
            statement.setTagNull(12, TSDB_DATA_TYPE_GEOMETRY);

            statement.setTagNull(13, TSDB_DATA_TYPE_UTINYINT);
            statement.setTagNull(14, TSDB_DATA_TYPE_USMALLINT);
            statement.setTagNull(15, TSDB_DATA_TYPE_UINT);
            statement.setTagNull(16, TSDB_DATA_TYPE_UBIGINT);

            statement.setTimestamp(0, Collections.singletonList(current));
            statement.setByte(1, Collections.singletonList(null));
            statement.columnDataAddBatch();
            statement.columnDataExecuteBatch();

            try (ResultSet resultSet = statement.executeQuery("select * from " + dbName + "." + stableName)) {
                resultSet.next();

                Assert.assertEquals(resultSet.getTimestamp(1), new Timestamp(current));

                resultSet.getByte(2);
                Assert.assertTrue(resultSet.wasNull());

                resultSet.getTimestamp(3);
                Assert.assertTrue(resultSet.wasNull());

                resultSet.getByte(4);
                Assert.assertTrue(resultSet.wasNull());

                resultSet.getShort(5);
                Assert.assertTrue(resultSet.wasNull());

                resultSet.getInt(6);
                Assert.assertTrue(resultSet.wasNull());

                resultSet.getLong(7);
                Assert.assertTrue(resultSet.wasNull());

                resultSet.getFloat(8);
                Assert.assertTrue(resultSet.wasNull());

                resultSet.getDouble(9);
                Assert.assertTrue(resultSet.wasNull());

                resultSet.getBoolean(10);
                Assert.assertTrue(resultSet.wasNull());

                Assert.assertNull(resultSet.getString(11));
                Assert.assertNull(resultSet.getString(12));
                Assert.assertNull(resultSet.getString(13));

                Assert.assertNull(resultSet.getBytes(14));
                Assert.assertNull(resultSet.getBytes(15));

                resultSet.getShort(16);
                Assert.assertTrue(resultSet.wasNull());

                resultSet.getInt(17);
                Assert.assertTrue(resultSet.wasNull());

                resultSet.getLong(18);
                Assert.assertTrue(resultSet.wasNull());

                Object obj = resultSet.getObject(19);
                Assert.assertNull(obj);
            }
        }
    }

    @Test
    public void testColumnDataNullValues() throws SQLException {
        String sql = "insert into ? using " + dbName + "." + stableName + " tags (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) values (?, ?)";
        try (TSWSPreparedStatement statement = connection.prepareStatement(sql).unwrap(TSWSPreparedStatement.class)) {
            long current = System.currentTimeMillis();

            // Set all tags to null
            statement.setTagNull(0, TSDB_DATA_TYPE_TIMESTAMP);
            statement.setTagNull(1, TSDB_DATA_TYPE_TINYINT);
            statement.setTagNull(2, TSDB_DATA_TYPE_SMALLINT);
            statement.setTagNull(3, TSDB_DATA_TYPE_INT);
            statement.setTagNull(4, TSDB_DATA_TYPE_BIGINT);
            statement.setTagNull(5, TSDB_DATA_TYPE_FLOAT);
            statement.setTagNull(6, TSDB_DATA_TYPE_DOUBLE);
            statement.setTagNull(7, TSDB_DATA_TYPE_BOOL);
            statement.setTagNull(8, TSDB_DATA_TYPE_BINARY);
            statement.setTagNull(9, TSDB_DATA_TYPE_NCHAR);
            statement.setTagNull(10, TSDB_DATA_TYPE_BINARY);
            statement.setTagNull(11, TSDB_DATA_TYPE_VARBINARY);
            statement.setTagNull(12, TSDB_DATA_TYPE_GEOMETRY);
            statement.setTagNull(13, TSDB_DATA_TYPE_UTINYINT);
            statement.setTagNull(14, TSDB_DATA_TYPE_USMALLINT);
            statement.setTagNull(15, TSDB_DATA_TYPE_UINT);
            statement.setTagNull(16, TSDB_DATA_TYPE_UBIGINT);

            statement.setTableName("test_null_values");

            // Set column data with null values for Decimal columns
            statement.setTimestamp(0, Collections.singletonList(current));
            statement.setByte(1, Collections.singletonList(null));
            statement.columnDataAddBatch();
            statement.columnDataExecuteBatch();

            // Verify the inserted null values
            try (ResultSet resultSet = statement.executeQuery("select * from " + dbName + ".test_null_values")) {
                Assert.assertTrue(resultSet.next());
                Assert.assertEquals(new Timestamp(current), resultSet.getTimestamp(1));

                // Check that the byte column is null
                resultSet.getByte(2);
                Assert.assertTrue(resultSet.wasNull());
            }
        }
    }

    @Before
    public void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getWebSocketWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-WS://" + TestEnvUtil.getHost() + ":" + TestEnvUtil.getRsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        } else {
            url += "?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        Properties properties = new Properties();
        connection = DriverManager.getConnection(url, properties);
        try (Statement statement = connection.createStatement()) {
            statement.execute("drop database if exists " + dbName);
            statement.execute("create database " + dbName + " keep 36500");
            statement.execute("use " + dbName);
            statement.execute("create table if not exists " + dbName + "." + tableName +
                    "(ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, " +
                    "c5 float, c6 double, c7 bool, c8 binary(10), c9 nchar(10), c10 varchar(20), c11 varbinary(100), c12 geometry(100), " +
                    "c13 tinyint unsigned, c14 smallint unsigned, c15 int unsigned, c16 bigint unsigned)");

            statement.execute("create stable if not exists " + dbName + "." + stableName +
                    "(ts timestamp, c1 tinyint) tags (t1 timestamp, t2 tinyint, t3 smallint, t4 int, t5 bigint, " +
                    "t6 float, t7 double, t8 bool, t9 binary(10), t10 nchar(10), t11 varchar(20), t12 varbinary(100), t13 geometry(100)," +
                    " t14 tinyint unsigned, t15 smallint unsigned, t16 int unsigned, t17 bigint unsigned)");
        }
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

