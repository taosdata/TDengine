package com.taosdata.jdbc.ws.stmt;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.ResourceLeakDetector;
import org.junit.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Properties;
import java.util.TimeZone;

public class WsPStmtLineModeNullTest {
            final String db_name = TestUtils.camelToSnake(WsPStmtLineModeNullTest.class);
    final String tableName = "wpt";
    String stableName = "swpt";
    Connection connection;
    PreparedStatement preparedStatement;

    @Test
    public void testExecuteUpdate() throws SQLException {
        String sql = "insert into " + db_name + "." + tableName + " values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
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

            // Decimal
            statement.setNull(18, Types.DECIMAL);
            statement.setNull(19, Types.DECIMAL);

            statement.executeUpdate();

            try (ResultSet resultSet = statement.executeQuery("select * from " + db_name + "." + tableName)) {
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

                // Decimal
                Assert.assertNull(resultSet.getBigDecimal(18));
                Assert.assertNull(resultSet.getBigDecimal(19));
            }
        }
    }

    @Test
    public void testSetObject() throws SQLException {
        String sql = "insert into " + db_name + "." + tableName + " values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            long current = System.currentTimeMillis();
            statement.setTimestamp(1, new Timestamp(current));
            statement.setObject(2, null, Types.TINYINT);
            statement.setObject(3, null, Types.SMALLINT);
            statement.setObject(4, null, Types.INTEGER);
            statement.setObject(5, null, Types.BIGINT);
            statement.setObject(6, null, Types.FLOAT);
            statement.setObject(7, null, Types.DOUBLE);
            statement.setObject(8, null, Types.BOOLEAN);

            statement.setObject(9, null, Types.VARCHAR);
            statement.setObject(10, null, Types.NCHAR);
            statement.setObject(11, null, Types.VARCHAR);
            statement.setObject(12, null, Types.VARBINARY);
            statement.setObject(13, null, Types.VARBINARY);

            // Unsigned types
            statement.setObject(14, null, Types.SMALLINT);
            statement.setObject(15, null, Types.INTEGER);
            statement.setObject(16, null, Types.BIGINT);
            statement.setObject(17, null, Types.BIGINT);

            // Decimal
            statement.setObject(18, null, Types.DECIMAL);
            statement.setObject(19, null, Types.DECIMAL);

            statement.executeUpdate();

            try (ResultSet resultSet = statement.executeQuery("select * from " + db_name + "." + tableName)) {
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

                // Decimal
                Assert.assertNull(resultSet.getBigDecimal(18));
                Assert.assertNull(resultSet.getBigDecimal(19));
            }
        }
    }
    @Test
    public void testSetObject2() throws SQLException {
        String sql = "insert into " + db_name + "." + tableName + " values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            long current = System.currentTimeMillis();
            statement.setTimestamp(1, new Timestamp(current));
            statement.setObject(2, null);
            statement.setObject(3, null);
            statement.setObject(4, null);
            statement.setObject(5, null);
            statement.setObject(6, null);
            statement.setObject(7, null);
            statement.setObject(8, null);

            statement.setObject(9, null);
            statement.setObject(10, null);
            statement.setObject(11, null);
            statement.setObject(12, null);
            statement.setObject(13, null);

            // Unsigned types
            statement.setObject(14, null);
            statement.setObject(15, null);
            statement.setObject(16, null);
            statement.setObject(17, null);

            // Decimal
            statement.setObject(18, null);
            statement.setObject(19, null);

            statement.executeUpdate();

            try (ResultSet resultSet = statement.executeQuery("select * from " + db_name + "." + tableName)) {
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

                // Decimal
                Assert.assertNull(resultSet.getBigDecimal(18));
                Assert.assertNull(resultSet.getBigDecimal(19));
            }
        }
    }

    @Test(expected = SQLException.class)
    public void testSetCharacterStream_int_Reader_int() throws SQLException {
        Reader reader = new StringReader("test");
        preparedStatement.setCharacterStream(1, reader, 4);
    }

    @Test(expected = SQLException.class)
    public void testSetRef() throws SQLException {
        Ref ref = null;
        preparedStatement.setRef(1, ref);
    }
    @Test
    public void testSetBlob_int_Blob() throws SQLException {
        Blob blob = null;
        preparedStatement.setBlob(1, blob);
    }

    @Test(expected = SQLException.class)
    public void testSetClob_int_Clob() throws SQLException {
        Clob clob = null; // 不需要实际实现
        preparedStatement.setClob(1, clob);
    }

    @Test(expected = SQLException.class)
    public void testSetArray() throws SQLException {
        Array array = null; // 不需要实际实现
        preparedStatement.setArray(1, array);
    }

    @Test(expected = SQLException.class)
    public void testSetDate() throws SQLException {
        Date date = new Date(0);
        Calendar cal = Calendar.getInstance();
        preparedStatement.setDate(1, date, cal);
    }

    @Test(expected = SQLException.class)
    public void testSetTime() throws SQLException {
        Time time = new Time(0);
        Calendar cal = Calendar.getInstance();
        preparedStatement.setTime(1, time, cal);
    }

    @Test
    public void testSetTimestamp_WithCalendar() throws SQLException {
        // 这个方法有特殊实现，需要单独测试
        Timestamp timestamp = new Timestamp(1000);
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        cal.set(Calendar.YEAR, 1970);
        cal.set(Calendar.MONTH, Calendar.JANUARY);
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 1);
        cal.set(Calendar.MILLISECOND, 0);

        preparedStatement.setTimestamp(1, timestamp, cal);
    }

    @Test(expected = SQLException.class)
    public void testSetNull_WithTypeName() throws SQLException {
        preparedStatement.setNull(1, java.sql.Types.VARCHAR, "VARCHAR");
    }

    @Test(expected = SQLException.class)
    public void testSetURL() throws SQLException, MalformedURLException {
        URL url = new URL("http://example.com");
        preparedStatement.setURL(1, url);
    }

    @Test(expected = SQLException.class)
    public void testSetRowId() throws SQLException {
        RowId rowId = null; // 不需要实际实现
        preparedStatement.setRowId(1, rowId);
    }

    @Test(expected = SQLException.class)
    public void testSetNCharacterStream_long() throws SQLException {
        Reader reader = new StringReader("test");
        preparedStatement.setNCharacterStream(1, reader, 4L);
    }

    @Test(expected = SQLException.class)
    public void testSetNClob_int_NClob() throws SQLException {
        NClob nclob = null; // 不需要实际实现
        preparedStatement.setNClob(1, nclob);
    }

    @Test(expected = SQLException.class)
    public void testSetClob_int_Reader_long() throws SQLException {
        Reader reader = new StringReader("test");
        preparedStatement.setClob(1, reader, 4L);
    }
    @Test
    public void testSetBlob_int_InputStream_long() throws SQLException {
        InputStream inputStream = new ByteArrayInputStream("test".getBytes());
        preparedStatement.setBlob(1, inputStream, 4L);
    }

    @Test(expected = SQLException.class)
    public void testSetNClob_int_Reader_long() throws SQLException {
        Reader reader = new StringReader("test");
        preparedStatement.setNClob(1, reader, 4L);
    }

    @Test(expected = SQLException.class)
    public void testSetSQLXML() throws SQLException {
        SQLXML sqlxml = null; // 不需要实际实现
        preparedStatement.setSQLXML(1, sqlxml);
    }

    @Test(expected = SQLException.class)
    public void testSetObject_withScale() throws SQLException {
        Object obj = new Object();
        preparedStatement.setObject(1, obj, java.sql.Types.VARCHAR, 10);
    }

    @Test(expected = SQLException.class)
    public void testSetAsciiStream_long() throws SQLException {
        InputStream inputStream = new ByteArrayInputStream("test".getBytes());
        preparedStatement.setAsciiStream(1, inputStream, 4L);
    }

    @Test(expected = SQLException.class)
    public void testSetBinaryStream_long() throws SQLException {
        InputStream inputStream = new ByteArrayInputStream("test".getBytes());
        preparedStatement.setBinaryStream(1, inputStream, 4L);
    }

    @Test(expected = SQLException.class)
    public void testSetCharacterStream_long() throws SQLException {
        Reader reader = new StringReader("test");
        preparedStatement.setCharacterStream(1, reader, 4L);
    }

    @Test(expected = SQLException.class)
    public void testSetAsciiStream() throws SQLException {
        InputStream inputStream = new ByteArrayInputStream("test".getBytes());
        preparedStatement.setAsciiStream(1, inputStream);
    }

    @Test(expected = SQLException.class)
    public void testSetBinaryStream() throws SQLException {
        InputStream inputStream = new ByteArrayInputStream("test".getBytes());
        preparedStatement.setBinaryStream(1, inputStream);
    }

    @Test(expected = SQLException.class)
    public void testSetCharacterStream() throws SQLException {
        Reader reader = new StringReader("test");
        preparedStatement.setCharacterStream(1, reader);
    }

    @Test(expected = SQLException.class)
    public void testSetNCharacterStream() throws SQLException {
        Reader reader = new StringReader("test");
        preparedStatement.setNCharacterStream(1, reader);
    }

    @Test(expected = SQLException.class)
    public void testSetClob_int_Reader() throws SQLException {
        Reader reader = new StringReader("test");
        preparedStatement.setClob(1, reader);
    }

    @Test
    public void testSetBlob_int_InputStream() throws SQLException {
        InputStream inputStream = new ByteArrayInputStream("test".getBytes());
        preparedStatement.setBlob(1, inputStream);
    }

    @Test(expected = SQLException.class)
    public void testSetNClob_int_Reader() throws SQLException {
        Reader reader = new StringReader("test");
        preparedStatement.setNClob(1, reader);
    }

    @Test
    @Ignore
    public void testCompositeBufferMemLeak(){
        PooledByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;
        for (int i = 0; i < 100000000; i++) {
            CompositeByteBuf compositeBuffer = allocator.compositeBuffer(100);
            compositeBuffer.release();
        }
        System.out.println("CompositeByteBuf memory leak test completed successfully.");
    }

    @Before
    public void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-WS://" + TestEnvUtil.getHost() + ":" + TestEnvUtil.getWsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        } else {
            url += "?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword() + "&batchfetch=true";
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PBS_MODE, "line");
        connection = DriverManager.getConnection(url, properties);
        try (Statement statement = connection.createStatement()) {
            statement.execute("drop database if exists " + db_name);
            statement.execute("create database " + db_name + " keep 36500");
            statement.execute("use " + db_name);
            statement.execute("create table if not exists " + db_name + "." + tableName +
                    "(ts timestamp, c1 tinyint, c2 smallint, c3 int, c4 bigint, " +
                    "c5 float, c6 double, c7 bool, c8 binary(10), c9 nchar(10), c10 varchar(20), c11 varbinary(100), c12 geometry(100), " +
                    "c13 tinyint unsigned, c14 smallint unsigned, c15 int unsigned, c16 bigint unsigned, " +
                    "c17 decimal(4,2), c18 decimal(30,10))");
        }
        preparedStatement = connection.prepareStatement("insert into " + db_name + "." + tableName + " (ts, c1) values (?, ?)");
    }

    @After
    public void after() throws SQLException {
        try (Statement statement = connection.createStatement()){
            statement.execute("drop database if exists " + db_name);
        }
        preparedStatement.close();
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

