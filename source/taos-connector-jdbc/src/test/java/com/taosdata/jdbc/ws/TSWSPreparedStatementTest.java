package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.*;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Random;

public class TSWSPreparedStatementTest {
        private static Connection conn;

    static final String HOST = TestEnvUtil.getHost();
    private static final String SQL_INSERT = "insert into t1 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String SQL_SELECT = "select * from t1 where ts >= ? and ts < ? and f1 >= ?";
    private static final String DB_NAME = TestUtils.camelToSnake(TSWSPreparedStatementTest.class);

    private PreparedStatement pstmtInsert;
    private PreparedStatement pstmtSelect;
    //create table weather(ts timestamp, f1 int, f2 bigint, f3 float, f4 double, f5 smallint, f6 tinyint, f7 bool, f8 binary(64), f9 nchar(64)) tags(loc nchar(64))

    @Test
    public void executeQuery() throws SQLException {
        // given
        long ts = System.currentTimeMillis();
        pstmtInsert.setTimestamp(1, new Timestamp(ts));
        pstmtInsert.setInt(2, 2);
        pstmtInsert.setLong(3, 3L);
        pstmtInsert.setFloat(4, 3.14f);
        pstmtInsert.setDouble(5, 3.1415);
        pstmtInsert.setShort(6, (short) 6);
        pstmtInsert.setByte(7, (byte) 7);
        pstmtInsert.setBoolean(8, true);
        pstmtInsert.setBytes(9, "abc".getBytes());
        pstmtInsert.setNString(10, "涛思数据");
        pstmtInsert.executeUpdate();
        long start = ts - 1000 * 60 * 60;
        long end = ts + 1000 * 60 * 60;
        pstmtSelect.setTimestamp(1, new Timestamp(start));
        pstmtSelect.setTimestamp(2, new Timestamp(end));
        pstmtSelect.setInt(3, 0);

        // when
        ResultSet rs = pstmtSelect.executeQuery();
        ResultSetMetaData meta = rs.getMetaData();
        rs.next();

        // then
        assertMetaData(meta);
        {
            Assert.assertNotNull(rs);
            Assert.assertEquals(ts, rs.getTimestamp(1).getTime());
            Assert.assertEquals(2, rs.getInt(2));
            Assert.assertEquals(2, rs.getInt("f1"));
            Assert.assertEquals(3L, rs.getLong(3));
            Assert.assertEquals(3L, rs.getLong("f2"));
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
            Assert.assertArrayEquals("abc".getBytes(), rs.getBytes(9));
            Assert.assertArrayEquals("abc".getBytes(), rs.getBytes("f8"));
            Assert.assertEquals("涛思数据", rs.getString(10));
            Assert.assertEquals("涛思数据", rs.getString("f9"));
        }
    }

    private void assertMetaData(ResultSetMetaData meta) throws SQLException {
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

    @Test
    public void setNullForTimestamp() throws SQLException {
        // given
        long ts = System.currentTimeMillis();

        // when
        pstmtInsert.setTimestamp(1, new Timestamp(ts));
        pstmtInsert.setNull(2, Types.INTEGER);
        pstmtInsert.setNull(3, Types.BIGINT);
        pstmtInsert.setNull(4, Types.FLOAT);
        pstmtInsert.setNull(5, Types.DOUBLE);
        pstmtInsert.setNull(6, Types.SMALLINT);
        pstmtInsert.setNull(7, Types.TINYINT);
        pstmtInsert.setNull(8, Types.BOOLEAN);
        pstmtInsert.setNull(9, Types.BINARY);
        pstmtInsert.setNull(10, Types.NCHAR);
        int result = pstmtInsert.executeUpdate();

        // then
        Assert.assertEquals(1, result);
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from t1");
            ResultSetMetaData meta = rs.getMetaData();
            assertMetaData(meta);
            rs.next();
            assertAllNullExceptTimestamp(rs, ts);
        }
    }

    private void assertAllNullExceptTimestamp(ResultSet rs, long ts) throws SQLException {
        Assert.assertNotNull(rs);
        Assert.assertEquals(ts, rs.getTimestamp(1).getTime());
        Assert.assertEquals(0, rs.getInt(2));
        Assert.assertEquals(0, rs.getInt("f1"));
        Assert.assertEquals(0, rs.getLong(3));
        Assert.assertEquals(0, rs.getLong("f2"));
        Assert.assertEquals(0, rs.getFloat(4), 0.0);
        Assert.assertEquals(0, rs.getFloat("f3"), 0.0);
        Assert.assertEquals(0, rs.getDouble(5), 0.0);
        Assert.assertEquals(0, rs.getDouble("f4"), 0.0);
        Assert.assertEquals(0, rs.getShort(6));
        Assert.assertEquals(0, rs.getShort("f5"));
        Assert.assertEquals(0, rs.getByte(7));
        Assert.assertEquals(0, rs.getByte("f6"));
        Assert.assertFalse(rs.getBoolean(8));
        Assert.assertFalse(rs.getBoolean("f7"));
        Assert.assertNull(rs.getBytes(9));
        Assert.assertNull(rs.getBytes("f8"));
        Assert.assertNull(rs.getString(10));
        Assert.assertNull(rs.getString("f9"));
    }

    @Test
    public void executeTest() throws SQLException {
        Statement stmt = conn.createStatement();

        int numOfRows = 1000;

        for (int loop = 0; loop < 10; loop++) {
            stmt.execute("drop table if exists weather_test");
            stmt.execute("create table weather_test(ts timestamp, f1 nchar(4), f2 float, f3 double, f4 timestamp, f5 int, f6 bool, f7 binary(10))");

            TSWSPreparedStatement s = (TSWSPreparedStatement) conn.prepareStatement("insert into weather_test values(?, ?, ?, ?, ?, ?, ?, ?)");
            Random r = new Random();

            ArrayList<Long> ts = new ArrayList<>();
            for (int i = 0; i < numOfRows; i++) {
                ts.add(System.currentTimeMillis() + i);
            }
            s.setTimestamp(0, ts);

            int random = 10 + r.nextInt(5);
            ArrayList<String> s2 = new ArrayList<>();
            for (int i = 0; i < numOfRows; i++) {
                if (i % random == 0) {
                    s2.add(null);
                } else {
                    s2.add("分支" + i % 4);
                }
            }
            s.setNString(1, s2, 4);

            random = 10 + r.nextInt(5);
            ArrayList<Float> s3 = new ArrayList<>();
            for (int i = 0; i < numOfRows; i++) {
                if (i % random == 0) {
                    s3.add(null);
                } else {
                    s3.add(r.nextFloat());
                }
            }
            s.setFloat(2, s3);

            random = 10 + r.nextInt(5);
            ArrayList<Double> s4 = new ArrayList<>();
            for (int i = 0; i < numOfRows; i++) {
                if (i % random == 0) {
                    s4.add(null);
                } else {
                    s4.add(r.nextDouble());
                }
            }
            s.setDouble(3, s4);

            random = 10 + r.nextInt(5);
            ArrayList<Long> ts2 = new ArrayList<>();
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
                    sb.add(i % 2 == 0);
                }
            }
            s.setBoolean(6, sb);

            random = 10 + r.nextInt(5);
            ArrayList<String> s5 = new ArrayList<>();
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

            TSWSPreparedStatement s = (TSWSPreparedStatement) conn.prepareStatement("insert into weather_test (ts, f1, f7) values(?, ?, ?)");
            Random r = new Random();

            ArrayList<Long> ts = new ArrayList<>();
            for (int i = 0; i < numOfRows; i++) {
                ts.add(System.currentTimeMillis() + i);
            }
            s.setTimestamp(0, ts);

            int random = 10 + r.nextInt(5);
            ArrayList<String> s2 = new ArrayList<>();
            for (int i = 0; i < numOfRows; i++) {
                if (i % random == 0) {
                    s2.add(null);
                } else {
                    s2.add("分支" + i % 4);
                }
            }
            s.setNString(1, s2, 4);

            random = 10 + r.nextInt(5);
            ArrayList<String> s3 = new ArrayList<>();
            for (int i = 0; i < numOfRows; i++) {
                if (i % random == 0) {
                    s3.add(null);
                } else {
                    s3.add("test" + i % 10);
                }
            }
            s.setString(2, s3, 10);

            s.columnDataAddBatch();
            s.columnDataExecuteBatch();
            s.columnDataCloseBatch();
            s.close();

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
    public void bindDataWithSingleTagTest() throws SQLException {
        Statement stmt = conn.createStatement();

        String[] types = new String[]{"tinyint", "smallint", "int", "bigint", "bool", "float", "double", "binary(10)", "nchar(10)"};

        for (String type : types) {
            stmt.execute("drop table if exists weather_test");
            stmt.execute("create table weather_test(ts timestamp, f1 nchar(10), f2 binary(10)) tags (t " + type + ")");

            int numOfRows = 1;

            TSWSPreparedStatement s = (TSWSPreparedStatement) conn.prepareStatement("insert into ? using weather_test tags(?) values(?, ?, ?)");
            Random r = new Random();
            s.setTableName("w1");

            switch (type) {
                case "tinyint":
                    s.setTagByte(0, (byte) 1);
                    break;
                case "smallint":
                    s.setTagShort(0, (short) 1);
                    break;
                case "int":
                    s.setTagInt(0, 1);
                    break;
                case "bigint":
                    s.setTagLong(0, 1L);
                    break;
                case "float":
                    s.setTagFloat(0, 1.23f);
                    break;
                case "double":
                    s.setTagDouble(0, 3.14159265);
                    break;
                case "bool":
                    s.setTagBoolean(0, true);
                    break;
                case "binary(10)":
                    s.setTagString(0, "test");
                    break;
                case "nchar(10)":
                    s.setTagNString(0, "test");
                    break;
                default:
                    break;
            }

            ArrayList<Long> ts = new ArrayList<>();
            for (int i = 0; i < numOfRows; i++) {
                ts.add(System.currentTimeMillis() + i);
            }
            s.setTimestamp(0, ts);

            int random = 10 + r.nextInt(5);
            ArrayList<String> s2 = new ArrayList<>();
            for (int i = 0; i < numOfRows; i++) {
                s2.add("分支" + i % 4);
            }
            s.setNString(1, s2, 10);

            random = 10 + r.nextInt(5);
            ArrayList<String> s3 = new ArrayList<>();
            for (int i = 0; i < numOfRows; i++) {
                s3.add("test" + i % 4);
            }
            s.setString(2, s3, 10);

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
    public void bindDataWithMultipleTagsTest() throws SQLException {
        Statement stmt = conn.createStatement();

        stmt.execute("drop table if exists weather_test");
        stmt.execute("create table weather_test(ts timestamp, f1 nchar(10), f2 binary(10)) tags (t1 int, t2 binary(10))");

        int numOfRows = 1;

        TSWSPreparedStatement s = (TSWSPreparedStatement) conn.prepareStatement("insert into ? using weather_test tags(?,?) (ts, f2) values(?, ?)");
        s.setTableName("w2");
        s.setTagInt(0, 1);
        s.setTagString(1, "test");

        ArrayList<Long> ts = new ArrayList<>();
        for (int i = 0; i < numOfRows; i++) {
            ts.add(System.currentTimeMillis() + i);
        }
        s.setTimestamp(0, ts);

        ArrayList<String> s2 = new ArrayList<>();
        for (int i = 0; i < numOfRows; i++) {
            s2.add("test" + i % 4);
        }
        s.setString(1, s2, 10);

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

    @Test
    public void bindDataQueryTest() throws SQLException {
        Statement stmt = conn.createStatement();

        stmt.execute("drop table if exists weather_test");
        stmt.execute("create table weather_test(ts timestamp, f1 nchar(10), f2 binary(10)) tags (t1 int, t2 binary(10))");

        int numOfRows = 1;

        TSWSPreparedStatement s = (TSWSPreparedStatement) conn.prepareStatement("insert into ? using weather_test tags(?,?) (ts, f2) values(?, ?)");
        s.setTableName("w2");
        s.setTagInt(0, 1);
        s.setTagString(1, "test");

        ArrayList<Long> ts = new ArrayList<>();
        for (int i = 0; i < numOfRows; i++) {
            ts.add(System.currentTimeMillis() + i);
        }
        s.setTimestamp(0, ts);

        ArrayList<String> s2 = new ArrayList<>();
        for (int i = 0; i < numOfRows; i++) {
            s2.add("test" + i % 4);
        }
        s.setString(1, s2, 10);

        s.columnDataAddBatch();
        s.columnDataExecuteBatch();
        s.columnDataCloseBatch();

        String sql = "select * from weather_test where t1 >= ? and t1 <= ?";
        TSWSPreparedStatement s1 = (TSWSPreparedStatement) conn.prepareStatement(sql);
        s1.setInt(1, 0);
        s1.setInt(2, 10);

        ResultSet rs = s1.executeQuery();
        int rows = 0;
        while (rs.next()) {
            rows++;
        }
        Assert.assertEquals(numOfRows, rows);
    }

    @Test
    public void setTagNullTest() throws SQLException {
        Statement stmt = conn.createStatement();

        stmt.execute("drop table if exists weather_test");
        stmt.execute("create table weather_test(ts timestamp, c1 int) tags (t1 tinyint, t2 smallint, t3 int, t4 bigint, t5 float, t6 double, t7 bool, t8 binary(10), t9 nchar(10))");

        int numOfRows = 1;

        TSWSPreparedStatement s = (TSWSPreparedStatement) conn.prepareStatement("insert into ? using weather_test tags(?,?,?,?,?,?,?,?,?) values(?, ?)");
        s.setTableName("w3");
        s.setTagNull(0, TSDBConstants.TSDB_DATA_TYPE_TINYINT);
        s.setTagNull(1, TSDBConstants.TSDB_DATA_TYPE_SMALLINT);
        s.setTagNull(2, TSDBConstants.TSDB_DATA_TYPE_INT);
        s.setTagNull(3, TSDBConstants.TSDB_DATA_TYPE_BIGINT);
        s.setTagNull(4, TSDBConstants.TSDB_DATA_TYPE_FLOAT);
        s.setTagNull(5, TSDBConstants.TSDB_DATA_TYPE_DOUBLE);
        s.setTagNull(6, TSDBConstants.TSDB_DATA_TYPE_BOOL);
        s.setTagNull(7, TSDBConstants.TSDB_DATA_TYPE_BINARY);
        s.setTagNull(8, TSDBConstants.TSDB_DATA_TYPE_NCHAR);

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < numOfRows; i++) {
            s.setTimestamp(1, new Timestamp(startTime + i));
            s.setInt(2, i);
            s.addBatch();
        }
        s.executeBatch();
    }

    private String stringGenerator(int length) {
        String source = "abcdefghijklmnopqrstuvwxyz";
        StringBuilder sb = new StringBuilder();
        Random rand = new Random();
        for (int i = 0; i < length; i++) {
            sb.append(source.charAt(rand.nextInt(26)));
        }
        return sb.toString();
    }

    @Test(expected = SQLException.class)
    public void setMaxTableNameTest() throws SQLException {
        Statement stmt = conn.createStatement();

        stmt.execute("drop table if exists weather_test");
        stmt.execute("create table weather_test(ts timestamp, c1 int) tags (t1 int)");

        TSWSPreparedStatement s = (TSWSPreparedStatement) conn.prepareStatement("insert into ? using weather_test tags(?) values(?, ?)");
        String tbname = stringGenerator(193);
        s.setTableName(tbname);
        s.setTagInt(0, 1);

        int numOfRows = 1;

        ArrayList<Long> ts = new ArrayList<>();
        for (int i = 0; i < numOfRows; i++) {
            ts.add(System.currentTimeMillis() + i);
        }
        s.setTimestamp(0, ts);

        ArrayList<Integer> s2 = new ArrayList<>();
        for (int i = 0; i < numOfRows; i++) {
            s2.add(i);
        }
        s.setInt(1, s2);

        s.columnDataAddBatch();
        s.columnDataExecuteBatch();
        s.columnDataCloseBatch();
    }

    @Test(expected = SQLException.class)
    public void createTwoSameDbTest() throws SQLException {
        // when
        Statement stmt = conn.createStatement();
        stmt.execute("create database dbtest");
        stmt.execute("create database dbtest");
    }

    @Test
    public void setBoolean() throws SQLException {
        // given
        long ts = System.currentTimeMillis();

        // when
        pstmtInsert.setTimestamp(1, new Timestamp(ts));
        pstmtInsert.setNull(2, Types.INTEGER);
        pstmtInsert.setNull(3, Types.BIGINT);
        pstmtInsert.setNull(4, Types.FLOAT);
        pstmtInsert.setNull(5, Types.DOUBLE);
        pstmtInsert.setNull(6, Types.SMALLINT);
        pstmtInsert.setNull(7, Types.TINYINT);
        pstmtInsert.setBoolean(8, true);
        pstmtInsert.setBytes(9, null);
        pstmtInsert.setNString(10, null);
        int result = pstmtInsert.executeUpdate();

        // then
        Assert.assertEquals(1, result);
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from t1");
            ResultSetMetaData meta = rs.getMetaData();
            assertMetaData(meta);
            rs.next();
            {
                Assert.assertNotNull(rs);
                Assert.assertEquals(ts, rs.getTimestamp(1).getTime());
                Assert.assertEquals(ts, rs.getTimestamp("ts").getTime());
                Assert.assertTrue(rs.getBoolean(8));
                Assert.assertTrue(rs.getBoolean("f7"));
            }
        }
    }

    @Test
    public void setByte() throws SQLException {
        // given
        long ts = System.currentTimeMillis();

        // when
        pstmtInsert.setTimestamp(1, new Timestamp(ts));
        pstmtInsert.setNull(2, Types.INTEGER);
        pstmtInsert.setNull(3, Types.BIGINT);
        pstmtInsert.setNull(4, Types.FLOAT);
        pstmtInsert.setNull(5, Types.DOUBLE);
        pstmtInsert.setNull(6, Types.SMALLINT);
        pstmtInsert.setByte(7, (byte) 0x001);
        pstmtInsert.setNull(8, Types.BOOLEAN);
        pstmtInsert.setBytes(9, null);
        pstmtInsert.setNString(10, null);

        int result = pstmtInsert.executeUpdate();

        // then
        Assert.assertEquals(1, result);
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from t1");
            ResultSetMetaData meta = rs.getMetaData();
            assertMetaData(meta);
            rs.next();
            {
                Assert.assertNotNull(rs);
                Assert.assertEquals(ts, rs.getTimestamp(1).getTime());
                Assert.assertEquals(ts, rs.getTimestamp("ts").getTime());
                Assert.assertEquals((byte) 0x001, rs.getByte(7));
                Assert.assertEquals((byte) 0x001, rs.getByte("f6"));
            }
        }
    }

    @Test
    public void setShort() throws SQLException {
        // given
        long ts = System.currentTimeMillis();

        // when
        pstmtInsert.setTimestamp(1, new Timestamp(ts));
        pstmtInsert.setNull(2, Types.INTEGER);
        pstmtInsert.setNull(3, Types.BIGINT);
        pstmtInsert.setNull(4, Types.FLOAT);
        pstmtInsert.setNull(5, Types.DOUBLE);
        pstmtInsert.setNull(7, Types.TINYINT);
        pstmtInsert.setNull(8, Types.BOOLEAN);
        pstmtInsert.setNull(9, Types.BINARY);
        pstmtInsert.setNull(10, Types.NCHAR);
        pstmtInsert.setShort(6, (short) 2);
        int result = pstmtInsert.executeUpdate();

        // then
        Assert.assertEquals(1, result);
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from t1");
            ResultSetMetaData meta = rs.getMetaData();
            assertMetaData(meta);
            rs.next();
            {
                Assert.assertNotNull(rs);
                Assert.assertEquals(ts, rs.getTimestamp(1).getTime());
                Assert.assertEquals(ts, rs.getTimestamp("ts").getTime());
                Assert.assertEquals((short) 2, rs.getByte(6));
                Assert.assertEquals((short) 2, rs.getByte("f5"));
            }
        }
    }

    @Test
    public void setInt() throws SQLException {
        // given
        long ts = System.currentTimeMillis();

        // when
        pstmtInsert.setTimestamp(1, new Timestamp(ts));
        pstmtInsert.setInt(2, 10086);
        pstmtInsert.setNull(3, Types.BIGINT);
        pstmtInsert.setNull(4, Types.FLOAT);
        pstmtInsert.setNull(5, Types.DOUBLE);
        pstmtInsert.setNull(6, Types.SMALLINT);
        pstmtInsert.setNull(7, Types.TINYINT);
        pstmtInsert.setNull(8, Types.BOOLEAN);
        pstmtInsert.setNull(9, Types.BINARY);
        pstmtInsert.setNull(10, Types.NCHAR);
        int result = pstmtInsert.executeUpdate();

        // then
        Assert.assertEquals(1, result);
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from t1");
            ResultSetMetaData meta = rs.getMetaData();
            assertMetaData(meta);
            rs.next();
            {
                Assert.assertNotNull(rs);
                Assert.assertEquals(ts, rs.getTimestamp(1).getTime());
                Assert.assertEquals(ts, rs.getTimestamp("ts").getTime());
                Assert.assertEquals(10086, rs.getInt(2));
                Assert.assertEquals(10086, rs.getInt("f1"));
            }
        }
    }

    @Test
    public void setLong() throws SQLException {
        // given
        long ts = System.currentTimeMillis();

        // when
        pstmtInsert.setTimestamp(1, new Timestamp(ts));
        pstmtInsert.setLong(3, Long.MAX_VALUE);
        pstmtInsert.setNull(2, Types.INTEGER);
        pstmtInsert.setNull(4, Types.FLOAT);
        pstmtInsert.setNull(5, Types.DOUBLE);
        pstmtInsert.setNull(6, Types.SMALLINT);
        pstmtInsert.setNull(7, Types.TINYINT);
        pstmtInsert.setNull(8, Types.BOOLEAN);
        pstmtInsert.setNull(9, Types.BINARY);
        pstmtInsert.setNull(10, Types.NCHAR);
        int result = pstmtInsert.executeUpdate();

        // then
        Assert.assertEquals(1, result);
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from t1");
            ResultSetMetaData meta = rs.getMetaData();
            assertMetaData(meta);
            rs.next();
            {
                Assert.assertNotNull(rs);
                Assert.assertEquals(ts, rs.getTimestamp(1).getTime());
                Assert.assertEquals(ts, rs.getTimestamp("ts").getTime());
                Assert.assertEquals(Long.MAX_VALUE, rs.getLong(3));
                Assert.assertEquals(Long.MAX_VALUE, rs.getLong("f2"));
            }
        }
    }

    @Test
    public void setFloat() throws SQLException {
        // given
        long ts = System.currentTimeMillis();

        // when
        pstmtInsert.setTimestamp(1, new Timestamp(ts));
        pstmtInsert.setFloat(4, 3.14f);
        pstmtInsert.setNull(2, Types.INTEGER);
        pstmtInsert.setNull(3, Types.BIGINT);
        pstmtInsert.setNull(5, Types.DOUBLE);
        pstmtInsert.setNull(6, Types.SMALLINT);
        pstmtInsert.setNull(7, Types.TINYINT);
        pstmtInsert.setNull(8, Types.BOOLEAN);
        pstmtInsert.setNull(9, Types.BINARY);
        pstmtInsert.setNull(10, Types.NCHAR);
        int result = pstmtInsert.executeUpdate();

        // then
        Assert.assertEquals(1, result);
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from t1");
            ResultSetMetaData meta = rs.getMetaData();
            assertMetaData(meta);
            rs.next();
            {
                Assert.assertNotNull(rs);
                Assert.assertEquals(ts, rs.getTimestamp(1).getTime());
                Assert.assertEquals(ts, rs.getTimestamp("ts").getTime());
                Assert.assertEquals(3.14f, rs.getFloat(4), 0.0f);
                Assert.assertEquals(3.14f, rs.getFloat("f3"), 0.0f);
            }
        }
    }

    @Test
    public void setDouble() throws SQLException {
        // given
        long ts = System.currentTimeMillis();

        // when
        pstmtInsert.setTimestamp(1, new Timestamp(ts));

        pstmtInsert.setDouble(5, 3.14444);
        pstmtInsert.setNull(2, Types.INTEGER);
        pstmtInsert.setNull(3, Types.BIGINT);
        pstmtInsert.setNull(4, Types.FLOAT);
        pstmtInsert.setNull(6, Types.SMALLINT);
        pstmtInsert.setNull(7, Types.TINYINT);
        pstmtInsert.setNull(8, Types.BOOLEAN);
        pstmtInsert.setNull(9, Types.BINARY);
        pstmtInsert.setNull(10, Types.NCHAR);
        int result = pstmtInsert.executeUpdate();

        // then
        Assert.assertEquals(1, result);
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from t1");
            ResultSetMetaData meta = rs.getMetaData();
            assertMetaData(meta);
            rs.next();
            {
                Assert.assertNotNull(rs);
                Assert.assertEquals(ts, rs.getTimestamp(1).getTime());
                Assert.assertEquals(ts, rs.getTimestamp("ts").getTime());
                Assert.assertEquals(3.14444, rs.getDouble(5), 0.0);
                Assert.assertEquals(3.14444, rs.getDouble("f4"), 0.0);
            }
        }
    }

    @Test
    public void setString() throws SQLException {
        // given
        long ts = System.currentTimeMillis();
        String f9 = "{\"name\": \"john\", \"age\": 10, \"address\": \"192.168.1.100\"}";

        // when
        pstmtInsert.setTimestamp(1, new Timestamp(ts));
        pstmtInsert.setNull(2, Types.INTEGER);
        pstmtInsert.setNull(3, Types.BIGINT);
        pstmtInsert.setNull(4, Types.FLOAT);
        pstmtInsert.setNull(5, Types.DOUBLE);
        pstmtInsert.setNull(6, Types.SMALLINT);
        pstmtInsert.setNull(7, Types.TINYINT);
        pstmtInsert.setNull(8, Types.BOOLEAN);
        pstmtInsert.setNull(9, Types.VARCHAR);
        pstmtInsert.setNString(10, f9);

        int result = pstmtInsert.executeUpdate();

        // then
        Assert.assertEquals(1, result);
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from t1");
            ResultSetMetaData meta = rs.getMetaData();
            assertMetaData(meta);
            rs.next();
            {
                Assert.assertNotNull(rs);
                Assert.assertEquals(ts, rs.getTimestamp(1).getTime());
                Assert.assertEquals(ts, rs.getTimestamp("ts").getTime());
                Assert.assertEquals(f9, rs.getString(10));
                Assert.assertEquals(f9, rs.getString("f9"));
            }
        }
    }

    @Test
    public void setBytes() throws SQLException {
        // given
        long ts = System.currentTimeMillis();
        byte[] f8 = "{\"name\": \"john\", \"age\": 10, \"address\": \"192.168.1.100\"}".getBytes();

        // when
        pstmtInsert.setTimestamp(1, new Timestamp(ts));
        pstmtInsert.setNull(2, Types.INTEGER);
        pstmtInsert.setNull(3, Types.BIGINT);
        pstmtInsert.setNull(4, Types.FLOAT);
        pstmtInsert.setNull(5, Types.DOUBLE);
        pstmtInsert.setNull(6, Types.SMALLINT);
        pstmtInsert.setNull(7, Types.TINYINT);
        pstmtInsert.setNull(8, Types.BOOLEAN);
        pstmtInsert.setBytes(9, f8);
        pstmtInsert.setNull(10, Types.NCHAR);
        int result = pstmtInsert.executeUpdate();

        // then
        Assert.assertEquals(1, result);
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from t1");
            ResultSetMetaData meta = rs.getMetaData();
            assertMetaData(meta);
            rs.next();
            {
                Assert.assertNotNull(rs);
                Assert.assertEquals(ts, rs.getTimestamp(1).getTime());
                Assert.assertEquals(ts, rs.getTimestamp("ts").getTime());
                Assert.assertArrayEquals(f8, rs.getBytes(9));
                Assert.assertArrayEquals(f8, rs.getBytes("f8"));
            }
        }
    }

    @Test
    public void setDate() throws SQLException {
        // given
        long ts = new java.util.Date().getTime();

        // when
        pstmtInsert.setDate(1, new Date(ts));
        pstmtInsert.setNull(2, Types.INTEGER);
        pstmtInsert.setNull(3, Types.BIGINT);
        pstmtInsert.setNull(4, Types.FLOAT);
        pstmtInsert.setNull(5, Types.DOUBLE);
        pstmtInsert.setNull(6, Types.SMALLINT);
        pstmtInsert.setNull(7, Types.TINYINT);
        pstmtInsert.setNull(8, Types.BOOLEAN);
        pstmtInsert.setNull(9, Types.BINARY);
        pstmtInsert.setNull(10, Types.NCHAR);
        int result = pstmtInsert.executeUpdate();

        // then
        Assert.assertEquals(1, result);
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from t1");
            ResultSetMetaData meta = rs.getMetaData();
            assertMetaData(meta);
            rs.next();
            {
                Assert.assertNotNull(rs);
                Assert.assertEquals(ts, rs.getTimestamp(1).getTime());
                Assert.assertEquals(ts, rs.getTimestamp("ts").getTime());
            }
        }
    }

    @Test
    public void setTime() throws SQLException {
        // given
        long ts = System.currentTimeMillis();

        // when
        pstmtInsert.setTime(1, new Time(ts));
        pstmtInsert.setNull(2, Types.INTEGER);
        pstmtInsert.setNull(3, Types.BIGINT);
        pstmtInsert.setNull(4, Types.FLOAT);
        pstmtInsert.setNull(5, Types.DOUBLE);
        pstmtInsert.setNull(6, Types.SMALLINT);
        pstmtInsert.setNull(7, Types.TINYINT);
        pstmtInsert.setNull(8, Types.BOOLEAN);
        pstmtInsert.setNull(9, Types.BINARY);
        pstmtInsert.setNull(10, Types.NCHAR);
        int result = pstmtInsert.executeUpdate();

        // then
        Assert.assertEquals(1, result);
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from t1");
            ResultSetMetaData meta = rs.getMetaData();
            assertMetaData(meta);
            rs.next();
            {
                Assert.assertNotNull(rs);
                Assert.assertEquals(ts, rs.getTimestamp(1).getTime());
                Assert.assertEquals(ts, rs.getTimestamp("ts").getTime());
            }
        }
    }

    @Test
    public void setTimestamp() throws SQLException {
        // given
        long ts = System.currentTimeMillis();

        // when
        pstmtInsert.setTimestamp(1, new Timestamp(ts));
        pstmtInsert.setNull(2, Types.INTEGER);
        pstmtInsert.setNull(3, Types.BIGINT);
        pstmtInsert.setNull(4, Types.FLOAT);
        pstmtInsert.setNull(5, Types.DOUBLE);
        pstmtInsert.setNull(6, Types.SMALLINT);
        pstmtInsert.setNull(7, Types.TINYINT);
        pstmtInsert.setNull(8, Types.BOOLEAN);
        pstmtInsert.setNull(9, Types.BINARY);
        pstmtInsert.setNull(10, Types.NCHAR);
        int result = pstmtInsert.executeUpdate();

        // then
        Assert.assertEquals(1, result);
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from t1");
            ResultSetMetaData meta = rs.getMetaData();
            assertMetaData(meta);
            rs.next();
            {
                Assert.assertNotNull(rs);
                Assert.assertEquals(ts, rs.getTimestamp(1).getTime());
                Assert.assertEquals(ts, rs.getTimestamp("ts").getTime());
            }
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setAsciiStream() throws SQLException {
        pstmtInsert.setAsciiStream(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setBinaryStream() throws SQLException {
        pstmtInsert.setBinaryStream(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setCharacterStream() throws SQLException {
        pstmtInsert.setCharacterStream(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setRef() throws SQLException {
        pstmtInsert.setRef(1, null);
    }

    @Test(expected = SQLException.class)
    public void setBlobErr() throws SQLException {
        TestUtils.runIn336();
        pstmtInsert.setBlob(1, (Blob) null);
    }

    @Test
    public void setBlob() throws SQLException {
        TestUtils.runInMain();
        pstmtInsert.setBlob(1, (Blob) null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setClob() throws SQLException {
        pstmtInsert.setClob(1, (Clob) null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setArray() throws SQLException {
        pstmtInsert.setArray(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setURL() throws SQLException {
        pstmtInsert.setURL(1, null);
    }

    @Test
    public void getParameterMetaData() throws SQLException {
        // given
        long ts = System.currentTimeMillis();
        pstmtInsert.setTimestamp(1, new Timestamp(ts));
        pstmtInsert.setInt(2, 2);
        pstmtInsert.setLong(3, 3L);
        pstmtInsert.setFloat(4, 3.14f);
        pstmtInsert.setDouble(5, 3.1415);
        pstmtInsert.setShort(6, (short) 6);
        pstmtInsert.setByte(7, (byte) 7);
        pstmtInsert.setBoolean(8, true);
        pstmtInsert.setBytes(9, "abc".getBytes());
        pstmtInsert.setNString(10, "涛思数据");

        // when
        ParameterMetaData parameterMetaData = pstmtInsert.getParameterMetaData();

        // then
        Assert.assertNotNull(parameterMetaData);
        Assert.assertEquals(10, parameterMetaData.getParameterCount());
        Assert.assertEquals(Types.TIMESTAMP, parameterMetaData.getParameterType(1));
        Assert.assertEquals(Types.INTEGER, parameterMetaData.getParameterType(2));
        Assert.assertEquals(Types.BIGINT, parameterMetaData.getParameterType(3));
        Assert.assertEquals(Types.FLOAT, parameterMetaData.getParameterType(4));
        Assert.assertEquals(Types.DOUBLE, parameterMetaData.getParameterType(5));
        Assert.assertEquals(Types.SMALLINT, parameterMetaData.getParameterType(6));
        Assert.assertEquals(Types.TINYINT, parameterMetaData.getParameterType(7));
        Assert.assertEquals(Types.BOOLEAN, parameterMetaData.getParameterType(8));
        Assert.assertEquals(Types.VARCHAR, parameterMetaData.getParameterType(9));
        Assert.assertEquals(Types.NCHAR, parameterMetaData.getParameterType(10));

        Assert.assertEquals("TIMESTAMP", parameterMetaData.getParameterTypeName(1));
        Assert.assertEquals("INT", parameterMetaData.getParameterTypeName(2));
        Assert.assertEquals("BIGINT", parameterMetaData.getParameterTypeName(3));
        Assert.assertEquals("FLOAT", parameterMetaData.getParameterTypeName(4));
        Assert.assertEquals("DOUBLE", parameterMetaData.getParameterTypeName(5));
        Assert.assertEquals("SMALLINT", parameterMetaData.getParameterTypeName(6));
        Assert.assertEquals("TINYINT", parameterMetaData.getParameterTypeName(7));
        Assert.assertEquals("BOOL", parameterMetaData.getParameterTypeName(8));
        Assert.assertEquals("BINARY", parameterMetaData.getParameterTypeName(9));
        Assert.assertEquals("NCHAR", parameterMetaData.getParameterTypeName(10));
    }

    @Test
    public void setObjectTest() throws SQLException {

        long ts = System.currentTimeMillis();
        pstmtSelect.setObject(1, new Timestamp(ts - 10000));
        pstmtSelect.setObject(2, new Timestamp(ts + 10000));
        pstmtSelect.setObject(3, 10);
        pstmtSelect.execute();
    }
    @Test
    public void setObjectFullTest() throws SQLException {

        String sql = "INSERT INTO weather_001 USING weather TAGS ('tag_001') VALUES ('2023-10-01 12:00:00', 25, 1234567890123, 23.45, 67.89, 10, 1, true, 'example_binary_data', 'example_nchar_data');";
        Statement stmt = conn.createStatement();
        stmt.execute(sql);
        stmt.close();

        boolean haveResult = false;
        String query_sql = "SELECT * FROM weather_001 WHERE ts = ?  AND f1 = ? AND f2 = ?  AND f3 > ? AND f4 > ? AND f5 = ? AND f6 = ? AND f7 = ? AND f8 = ? AND f9 = ? and loc = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(query_sql)) {
            pstmt.setObject(1, LocalDateTime.now());
            pstmt.setObject(1, Timestamp.valueOf("2023-10-01 12:00:00"));
            pstmt.setObject(2, 25);
            pstmt.setObject(3, 1234567890123L);
            pstmt.setObject(4, 23.44f);
            pstmt.setObject(5, 67.889);
            pstmt.setObject(6, (short) 10);
            pstmt.setObject(7, (byte) 1);
            pstmt.setObject(8, true);
            pstmt.setObject(9, "example_binary_data".getBytes());
            pstmt.setObject(10, "example_nchar_data");
            pstmt.setObject(11, "tag_001");

            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    haveResult = true;
                }
            }
        }

        assert(haveResult);

    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setRowId() throws SQLException {
        pstmtInsert.setRowId(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setNCharacterStream() throws SQLException {
        pstmtInsert.setNCharacterStream(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setNClob() throws SQLException {
        pstmtInsert.setNClob(1, (NClob) null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setSQLXML() throws SQLException {
        pstmtInsert.setSQLXML(1, null);
    }

    @Before
    public void before() throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute("drop table if exists weather");
        stmt.execute("create table if not exists weather(ts timestamp, f1 int, f2 bigint, f3 float, f4 double, f5 smallint, f6 tinyint, f7 bool, f8 binary(64), f9 nchar(64)) tags(loc nchar(64))");
        stmt.execute("create table if not exists t1 using weather tags('beijing')");
        stmt.close();

        pstmtInsert = conn.prepareStatement(SQL_INSERT);
        pstmtSelect = conn.prepareStatement(SQL_SELECT);
    }

    @After
    public void after() {
        try {
            if (pstmtInsert != null)
                pstmtInsert.close();
            if (pstmtSelect != null)
                pstmtSelect.close();
        } catch (SQLException e) {
            // nothing
        }

    }

    @BeforeClass
    public static void beforeClass() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + HOST + ":" + TestEnvUtil.getWsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword() + "&batchfetch=true";
        }
        conn = DriverManager.getConnection(url);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists " + DB_NAME);
            stmt.execute("create database if not exists " + DB_NAME);
            stmt.execute("use " + DB_NAME);
        }
    }

    @AfterClass
    public static void afterClass() {
        try {
            Statement statement = conn.createStatement();
            statement.execute("drop database if exists " + DB_NAME);
            statement.execute("drop database if exists dbtest");
            statement.close();
            if (conn != null)
                conn.close();
        } catch (SQLException e) {
            // nothing
        }
    }
}

