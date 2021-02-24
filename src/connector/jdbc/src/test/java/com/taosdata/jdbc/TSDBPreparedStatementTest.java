package com.taosdata.jdbc;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;

public class TSDBPreparedStatementTest {
    private static final String host = "127.0.0.1";
    private static Connection conn;
    private static final String sql_insert = "insert into t1 values(?, ?)";
    private static PreparedStatement pstmt_insert;
    private static final String sql_select = "select * from t1 where ts > ? and ts <= ? and temperature >= ?";
    private static PreparedStatement pstmt_select;

    @Test
    public void executeQuery() throws SQLException {
        long end = System.currentTimeMillis();
        long start = end - 1000 * 60 * 60;
        pstmt_select.setTimestamp(1, new Timestamp(start));
        pstmt_select.setTimestamp(2, new Timestamp(end));
        pstmt_select.setFloat(3, 0);

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
        pstmt_insert.setFloat(2, 3.14f);
        int result = pstmt_insert.executeUpdate();
        Assert.assertEquals(1, result);
    }

    @Test
    public void setNull() throws SQLException {
        pstmt_insert.setNull(2, Types.FLOAT);
    }

    @Test
    public void setBoolean() throws SQLException {
        pstmt_insert.setBoolean(2, true);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setByte() throws SQLException {
        pstmt_insert.setByte(1, (byte) 0x001);
    }

    @Test
    public void setShort() {
    }

    @Test
    public void setInt() {
    }

    @Test
    public void setLong() {
    }

    @Test
    public void setFloat() {
    }

    @Test
    public void setDouble() {
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setBigDecimal() throws SQLException {
        pstmt_insert.setBigDecimal(1, null);
    }

    @Test
    public void setString() {
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setBytes() throws SQLException {
        pstmt_insert.setBytes(1, new byte[]{});
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
    public void setTimestamp() {
        //TODO
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
    public void clearParameters() {
        //TODO
    }

    @Test
    public void setObject() throws SQLException {
        pstmt_insert.setObject(1, System.currentTimeMillis());
        //TODO
    }

    @Test
    public void execute() {
        //TODO
    }

    @Test
    public void addBatch() {
        //TODO:
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

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getParameterMetaData() throws SQLException {
        ParameterMetaData parameterMetaData = pstmt_insert.getParameterMetaData();
//        Assert.assertNotNull(parameterMetaData);
        //TODO:
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
            Class.forName("com.taosdata.jdbc.TSDBDriver");
            conn = DriverManager.getConnection("jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata");
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("drop database if exists test_pstmt");
                stmt.execute("create database if not exists test_pstmt");
                stmt.execute("use test_pstmt");
                stmt.execute("create table weather(ts timestamp, temperature float) tags(loc nchar(64))");
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

            if (conn != null)
                conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}