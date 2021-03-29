package com.taosdata.jdbc.rs;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.sql.*;

public class RestfulPreparedStatementTest {
    private static final String host = "127.0.0.1";
    private static Connection conn;
    private static final String sql_insert = "insert into t1 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static PreparedStatement pstmt_insert;
    private static final String sql_select = "select * from t1 where ts > ? and ts <= ? and f1 >= ?";
    private static PreparedStatement pstmt_select;

    @Test
    public void executeQuery() throws SQLException {
        long end = System.currentTimeMillis();
        long start = end - 1000 * 60 * 60;
        pstmt_select.setTimestamp(1, new Timestamp(start));
        pstmt_select.setTimestamp(2, new Timestamp(end));
        pstmt_select.setInt(3, 0);

        ResultSet rs = pstmt_select.executeQuery();
        Assert.assertNotNull(rs);
        ResultSetMetaData meta = rs.getMetaData();
        while (rs.next()) {
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                System.out.print(meta.getColumnLabel(i) + ": " + rs.getString(i) + "\t");
            }
            System.out.println();
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

    class Person implements Serializable {
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


    @BeforeClass
    public static void beforeClass() {
        try {
            Class.forName("com.taosdata.jdbc.rs.RestfulDriver");
            conn = DriverManager.getConnection("jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata");
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("drop database if exists test_pstmt");
                stmt.execute("create database if not exists test_pstmt");
                stmt.execute("use test_pstmt");
                stmt.execute("create table weather(ts timestamp, f1 int, f2 bigint, f3 float, f4 double, f5 smallint, f6 tinyint, f7 bool, f8 binary(64), f9 nchar(64)) tags(loc nchar(64))");
                stmt.execute("create table t1 using weather tags('beijing')");
            }
            pstmt_insert = conn.prepareStatement(sql_insert);
            pstmt_select = conn.prepareStatement(sql_select);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void afterClass() {
        try {
            if (pstmt_insert != null)
                pstmt_insert.close();
            if (pstmt_select != null)
                pstmt_select.close();
            if (conn != null)
                conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}