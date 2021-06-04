package com.taosdata.jdbc;

import org.junit.*;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

public class TSDBPreparedStatementTest {

    private static final String host = "127.0.0.1";
    private static Connection conn;
    private static final String sql_insert = "insert into t1 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String sql_select = "select * from t1 where ts >= ? and ts < ? and f1 >= ?";

    private PreparedStatement pstmt_insert;
    private PreparedStatement pstmt_select;

    //create table weather(ts timestamp, f1 int, f2 bigint, f3 float, f4 double, f5 smallint, f6 tinyint, f7 bool, f8 binary(64), f9 nchar(64)) tags(loc nchar(64))

    @Test
    public void executeQuery() throws SQLException {
        // given
        long ts = System.currentTimeMillis();
        pstmt_insert.setTimestamp(1, new Timestamp(ts));
        pstmt_insert.setInt(2, 2);
        pstmt_insert.setLong(3, 3l);
        pstmt_insert.setFloat(4, 3.14f);
        pstmt_insert.setDouble(5, 3.1415);
        pstmt_insert.setShort(6, (short) 6);
        pstmt_insert.setByte(7, (byte) 7);
        pstmt_insert.setBoolean(8, true);
        pstmt_insert.setBytes(9, "abc".getBytes());
        pstmt_insert.setString(10, "涛思数据");
        pstmt_insert.executeUpdate();
        long start = ts - 1000 * 60 * 60;
        long end = ts + 1000 * 60 * 60;
        pstmt_select.setTimestamp(1, new Timestamp(start));
        pstmt_select.setTimestamp(2, new Timestamp(end));
        pstmt_select.setInt(3, 0);

        // when
        ResultSet rs = pstmt_select.executeQuery();
        ResultSetMetaData meta = rs.getMetaData();

        // then
        assertResultSetMetaData(meta);
        {
            Assert.assertNotNull(rs);
            Assert.assertEquals(ts, rs.getTimestamp(1).getTime());
            Assert.assertEquals(2, rs.getInt(2));
            Assert.assertEquals(2, rs.getInt("f1"));
            Assert.assertEquals(3l, rs.getLong(3));
            Assert.assertEquals(3l, rs.getLong("f2"));
            Assert.assertEquals(3.14f, rs.getFloat(4), 0.0);
            Assert.assertEquals(3.14f, rs.getFloat("f3"), 0.0);
            Assert.assertEquals(3.1415, rs.getDouble(5), 0.0);
            Assert.assertEquals(3.1415, rs.getDouble("f4"), 0.0);
            Assert.assertEquals((short) 6, rs.getShort(6));
            Assert.assertEquals((short) 6, rs.getShort("f5"));
            Assert.assertEquals((byte) 7, rs.getByte(7));
            Assert.assertEquals((byte) 7, rs.getByte("f6"));
            Assert.assertTrue(rs.getBoolean(8));
            Assert.assertTrue(rs.getBoolean("f7"));
            Assert.assertTrue(Arrays.equals("abc".getBytes(), rs.getBytes(9)));
            Assert.assertTrue(Arrays.equals("abc".getBytes(), rs.getBytes("f8")));
            Assert.assertEquals("涛思数据", rs.getString(10));
            Assert.assertEquals("涛思数据", rs.getString("f9"));
        }
    }

    @Test
    public void executeUpdate() throws SQLException {
        pstmt_insert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmt_insert.setFloat(4, 3.14f);
        int result = pstmt_insert.executeUpdate();
        Assert.assertEquals(1, result);
    }

    @Test
    public void setNull() throws SQLException {
        pstmt_insert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmt_insert.setNull(2, Types.INTEGER);
        int result = pstmt_insert.executeUpdate();
        Assert.assertEquals(1, result);

        pstmt_insert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmt_insert.setNull(3, Types.BIGINT);
        result = pstmt_insert.executeUpdate();
        Assert.assertEquals(1, result);

        pstmt_insert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmt_insert.setNull(4, Types.FLOAT);
        result = pstmt_insert.executeUpdate();
        Assert.assertEquals(1, result);

        pstmt_insert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmt_insert.setNull(5, Types.DOUBLE);
        result = pstmt_insert.executeUpdate();
        Assert.assertEquals(1, result);

        pstmt_insert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmt_insert.setNull(6, Types.SMALLINT);
        result = pstmt_insert.executeUpdate();
        Assert.assertEquals(1, result);

        pstmt_insert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmt_insert.setNull(7, Types.TINYINT);
        result = pstmt_insert.executeUpdate();
        Assert.assertEquals(1, result);

        pstmt_insert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmt_insert.setNull(8, Types.BOOLEAN);
        result = pstmt_insert.executeUpdate();
        Assert.assertEquals(1, result);

        pstmt_insert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmt_insert.setNull(9, Types.BINARY);
        result = pstmt_insert.executeUpdate();
        Assert.assertEquals(1, result);

        pstmt_insert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmt_insert.setNull(10, Types.NCHAR);
        result = pstmt_insert.executeUpdate();
        Assert.assertEquals(1, result);

        pstmt_insert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmt_insert.setNull(10, Types.OTHER);
        result = pstmt_insert.executeUpdate();
        Assert.assertEquals(1, result);
    }

    @Test
    public void executeTest() throws SQLException {
        Statement stmt = conn.createStatement();

        int numOfRows = 1000;

        for (int loop = 0; loop < 10; loop++) {
            stmt.execute("drop table if exists weather_test");
            stmt.execute("create table weather_test(ts timestamp, f1 nchar(4), f2 float, f3 double, f4 timestamp, f5 int, f6 bool, f7 binary(10))");

            TSDBPreparedStatement s = (TSDBPreparedStatement) conn.prepareStatement("insert into ? values(?, ?, ?, ?, ?, ?, ?, ?)");
            Random r = new Random();
            s.setTableName("weather_test");

            ArrayList<Long> ts = new ArrayList<Long>();
            for (int i = 0; i < numOfRows; i++) {
                ts.add(System.currentTimeMillis() + i);
            }
            s.setTimestamp(0, ts);

            int random = 10 + r.nextInt(5);
            ArrayList<String> s2 = new ArrayList<String>();
            for (int i = 0; i < numOfRows; i++) {
                if (i % random == 0) {
                    s2.add(null);
                } else {
                    s2.add("分支" + i % 4);
                }
            }
            s.setNString(1, s2, 4);

            random = 10 + r.nextInt(5);
            ArrayList<Float> s3 = new ArrayList<Float>();
            for (int i = 0; i < numOfRows; i++) {
                if (i % random == 0) {
                    s3.add(null);
                } else {
                    s3.add(r.nextFloat());
                }
            }
            s.setFloat(2, s3);

            random = 10 + r.nextInt(5);
            ArrayList<Double> s4 = new ArrayList<Double>();
            for (int i = 0; i < numOfRows; i++) {
                if (i % random == 0) {
                    s4.add(null);
                } else {
                    s4.add(r.nextDouble());
                }
            }
            s.setDouble(3, s4);

            random = 10 + r.nextInt(5);
            ArrayList<Long> ts2 = new ArrayList<Long>();
            for (int i = 0; i < numOfRows; i++) {
                if (i % random == 0) {
                    ts2.add(null);
                } else {
                    ts2.add(System.currentTimeMillis() + i);
                }
            }
            s.setTimestamp(4, ts2);

            random = 10 + r.nextInt(5);
            ArrayList<Integer> vals = new ArrayList<>();
            for (int i = 0; i < numOfRows; i++) {
                if (i % random == 0) {
                    vals.add(null);
                } else {
                    vals.add(r.nextInt());
                }
            }
            s.setInt(5, vals);

            random = 10 + r.nextInt(5);
            ArrayList<Boolean> sb = new ArrayList<>();
            for (int i = 0; i < numOfRows; i++) {
                if (i % random == 0) {
                    sb.add(null);
                } else {
                    sb.add(i % 2 == 0 ? true : false);
                }
            }
            s.setBoolean(6, sb);

            random = 10 + r.nextInt(5);
            ArrayList<String> s5 = new ArrayList<String>();
            for (int i = 0; i < numOfRows; i++) {
                if (i % random == 0) {
                    s5.add(null);
                } else {
                    s5.add("test" + i % 10);
                }
            }
            s.setString(7, s5, 10);

            s.columnDataAddBatch();
            s.columnDataExecuteBatch();
            s.columnDataCloseBatch();

            String sql = "select * from weather_test";
            PreparedStatement statement = conn.prepareStatement(sql);
            ResultSet rs = statement.executeQuery();
            int rows = 0;
            while (rs.next()) {
                rows++;
            }
            Assert.assertEquals(numOfRows, rows);
        }
    }

    @Test
    public void bindDataSelectColumnTest() throws SQLException {
        Statement stmt = conn.createStatement();

        int numOfRows = 1000;

        for (int loop = 0; loop < 10; loop++) {
            stmt.execute("drop table if exists weather_test");
            stmt.execute("create table weather_test(ts timestamp, f1 nchar(4), f2 float, f3 double, f4 timestamp, f5 int, f6 bool, f7 binary(10))");

            TSDBPreparedStatement s = (TSDBPreparedStatement) conn.prepareStatement("insert into ? (ts, f1, f7) values(?, ?, ?)");
            Random r = new Random();
            s.setTableName("weather_test");

            ArrayList<Long> ts = new ArrayList<Long>();
            for (int i = 0; i < numOfRows; i++) {
                ts.add(System.currentTimeMillis() + i);
            }
            s.setTimestamp(0, ts);

            int random = 10 + r.nextInt(5);
            ArrayList<String> s2 = new ArrayList<String>();
            for (int i = 0; i < numOfRows; i++) {
                if (i % random == 0) {
                    s2.add(null);
                } else {
                    s2.add("分支" + i % 4);
                }
            }
            s.setNString(1, s2, 4);

            random = 10 + r.nextInt(5);
            ArrayList<String> s5 = new ArrayList<String>();
            for (int i = 0; i < numOfRows; i++) {
                if (i % random == 0) {
                    s5.add(null);
                } else {
                    s5.add("test" + i % 10);
                }
            }
            s.setString(2, s5, 10);

            s.columnDataAddBatch();
            s.columnDataExecuteBatch();
            s.columnDataCloseBatch();

            String sql = "select * from weather_test";
            PreparedStatement statement = conn.prepareStatement(sql);
            ResultSet rs = statement.executeQuery();
            int rows = 0;
            while (rs.next()) {
                rows++;
            }
            Assert.assertEquals(numOfRows, rows);
        }
    }


    @Test
    public void setBoolean() throws SQLException {
        pstmt_insert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmt_insert.setBoolean(8, true);
        int ret = pstmt_insert.executeUpdate();
        Assert.assertEquals(1, ret);
    }

    @Test
    public void setByte() throws SQLException {
        pstmt_insert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmt_insert.setByte(7, (byte) 0x001);
        int ret = pstmt_insert.executeUpdate();
        Assert.assertEquals(1, ret);
    }

    @Test
    public void setShort() throws SQLException {
        pstmt_insert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmt_insert.setShort(6, (short) 2);
        int ret = pstmt_insert.executeUpdate();
        Assert.assertEquals(1, ret);
    }

    @Test
    public void setInt() throws SQLException {
        pstmt_insert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmt_insert.setInt(2, 10086);
        int ret = pstmt_insert.executeUpdate();
        Assert.assertEquals(1, ret);
    }

    @Test
    public void setLong() throws SQLException {
        pstmt_insert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmt_insert.setLong(3, Long.MAX_VALUE);
        int ret = pstmt_insert.executeUpdate();
        Assert.assertEquals(1, ret);
    }

    @Test
    public void setFloat() throws SQLException {
        pstmt_insert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmt_insert.setFloat(4, 3.14f);
        int ret = pstmt_insert.executeUpdate();
        Assert.assertEquals(1, ret);
    }

    @Test
    public void setDouble() throws SQLException {
        pstmt_insert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmt_insert.setDouble(5, 3.14444);
        int ret = pstmt_insert.executeUpdate();
        Assert.assertEquals(1, ret);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setBigDecimal() throws SQLException {
        pstmt_insert.setBigDecimal(1, null);
    }

    @Test
    public void setString() throws SQLException {
        pstmt_insert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmt_insert.setString(10, "aaaa");
        boolean execute = pstmt_insert.execute();
        Assert.assertFalse(execute);

        pstmt_insert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmt_insert.setString(10, new Person("john", 33, true).toString());
        Assert.assertFalse(pstmt_insert.execute());

        pstmt_insert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmt_insert.setString(10, new Person("john", 33, true).toString().replaceAll("'", "\""));
        Assert.assertFalse(pstmt_insert.execute());
    }

    class Person {
        String name;
        int age;
        boolean sex;

        public Person(String name, int age, boolean sex) {
            this.name = name;
            this.age = age;
            this.sex = sex;
        }

        @Override
        public String toString() {
            return "Person{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    ", sex=" + sex +
                    '}';
        }
    }

    @Test
    public void setBytes() throws SQLException, IOException {
        pstmt_insert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));

//        ByteArrayOutputStream baos = new ByteArrayOutputStream();
//        ObjectOutputStream oos = new ObjectOutputStream(baos);
//        oos.writeObject(new Person("john", 33, true));
//        oos.flush();
//        byte[] bytes = baos.toByteArray();
//        pstmt_insert.setBytes(9, bytes);

        pstmt_insert.setBytes(9, new Person("john", 33, true).toString().getBytes());
        int ret = pstmt_insert.executeUpdate();
        Assert.assertEquals(1, ret);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setDate() throws SQLException {
        pstmt_insert.setDate(1, new Date(System.currentTimeMillis()));
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setTime() throws SQLException {
        pstmt_insert.setTime(1, new Time(System.currentTimeMillis()));
    }

    @Test
    public void setTimestamp() throws SQLException {
        pstmt_insert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        int ret = pstmt_insert.executeUpdate();
        Assert.assertEquals(1, ret);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setAsciiStream() throws SQLException {
        pstmt_insert.setAsciiStream(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setBinaryStream() throws SQLException {
        pstmt_insert.setBinaryStream(1, null);
    }

    @Test
    public void clearParameters() throws SQLException {
        pstmt_insert.clearParameters();
    }

    @Test
    public void setObject() throws SQLException {
        pstmt_insert.setObject(1, new Timestamp(System.currentTimeMillis()));
        int ret = pstmt_insert.executeUpdate();
        Assert.assertEquals(1, ret);

        pstmt_insert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmt_insert.setObject(2, 111);
        ret = pstmt_insert.executeUpdate();
        Assert.assertEquals(1, ret);

        pstmt_insert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmt_insert.setObject(3, Long.MAX_VALUE);
        ret = pstmt_insert.executeUpdate();
        Assert.assertEquals(1, ret);

        pstmt_insert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmt_insert.setObject(4, 3.14159265354f);
        ret = pstmt_insert.executeUpdate();
        Assert.assertEquals(1, ret);

        pstmt_insert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmt_insert.setObject(5, Double.MAX_VALUE);
        ret = pstmt_insert.executeUpdate();
        Assert.assertEquals(1, ret);

        pstmt_insert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmt_insert.setObject(6, Short.MAX_VALUE);
        ret = pstmt_insert.executeUpdate();
        Assert.assertEquals(1, ret);

        pstmt_insert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmt_insert.setObject(7, Byte.MAX_VALUE);
        ret = pstmt_insert.executeUpdate();
        Assert.assertEquals(1, ret);

        pstmt_insert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmt_insert.setObject(8, true);
        ret = pstmt_insert.executeUpdate();
        Assert.assertEquals(1, ret);

        pstmt_insert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmt_insert.setObject(9, "hello".getBytes());
        ret = pstmt_insert.executeUpdate();
        Assert.assertEquals(1, ret);

        pstmt_insert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmt_insert.setObject(10, "Hello");
        ret = pstmt_insert.executeUpdate();
        Assert.assertEquals(1, ret);
    }

    @Test
    public void execute() throws SQLException {
        pstmt_insert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        int ret = pstmt_insert.executeUpdate();
        Assert.assertEquals(1, ret);

        executeQuery();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setCharacterStream() throws SQLException {
        pstmt_insert.setCharacterStream(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setRef() throws SQLException {
        pstmt_insert.setRef(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setBlob() throws SQLException {
        pstmt_insert.setBlob(1, (Blob) null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setClob() throws SQLException {
        pstmt_insert.setClob(1, (Clob) null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setArray() throws SQLException {
        pstmt_insert.setArray(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getMetaData() throws SQLException {
        pstmt_insert.getMetaData();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setURL() throws SQLException {
        pstmt_insert.setURL(1, null);
    }

    @Test
    public void getParameterMetaData() throws SQLException {
        ParameterMetaData parameterMetaData = pstmt_insert.getParameterMetaData();
        Assert.assertNotNull(parameterMetaData);
        //TODO: modify the test case
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setRowId() throws SQLException {
        pstmt_insert.setRowId(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setNString() throws SQLException {
        pstmt_insert.setNString(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setNCharacterStream() throws SQLException {
        pstmt_insert.setNCharacterStream(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setNClob() throws SQLException {
        pstmt_insert.setNClob(1, (NClob) null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setSQLXML() throws SQLException {
        pstmt_insert.setSQLXML(1, null);
    }

    private void assertResultSetMetaData(ResultSetMetaData meta) throws SQLException {
        Assert.assertEquals(10, meta.getColumnCount());
        Assert.assertEquals("ts", meta.getColumnLabel(1));
        Assert.assertEquals("f1", meta.getColumnLabel(2));
        Assert.assertEquals("f2", meta.getColumnLabel(3));
        Assert.assertEquals("f3", meta.getColumnLabel(4));
        Assert.assertEquals("f4", meta.getColumnLabel(5));
        Assert.assertEquals("f5", meta.getColumnLabel(6));
        Assert.assertEquals("f6", meta.getColumnLabel(7));
        Assert.assertEquals("f7", meta.getColumnLabel(8));
        Assert.assertEquals("f8", meta.getColumnLabel(9));
        Assert.assertEquals("f9", meta.getColumnLabel(10));
    }

    @Before
    public void before() {
        try {
            Statement stmt = conn.createStatement();
            stmt.execute("drop table if exists weather");
            stmt.execute("create table if not exists weather(ts timestamp, f1 int, f2 bigint, f3 float, f4 double, f5 smallint, f6 tinyint, f7 bool, f8 binary(64), f9 nchar(64)) tags(loc nchar(64))");
            stmt.execute("create table if not exists t1 using weather tags('beijing')");
            stmt.close();

            pstmt_insert = conn.prepareStatement(sql_insert);
            pstmt_select = conn.prepareStatement(sql_select);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @After
    public void after() {
        try {
            if (pstmt_insert != null)
                pstmt_insert.close();
            if (pstmt_select != null)
                pstmt_select.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    @BeforeClass
    public static void beforeClass() {
        try {
            conn = DriverManager.getConnection("jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata");
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("drop database if exists test_pstmt_jni");
                stmt.execute("create database if not exists test_pstmt_jni");
                stmt.execute("use test_pstmt_jni");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void afterClass() {
        try {
            if (conn != null)
                conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}