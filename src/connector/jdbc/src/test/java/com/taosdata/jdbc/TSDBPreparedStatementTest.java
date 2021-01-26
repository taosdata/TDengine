package com.taosdata.jdbc;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class TSDBPreparedStatementTest {
    private static final String host = "127.0.0.1";
    private static Connection conn;

    @Test
    public void executeQuery() {

    }

    @Test
    public void executeUpdate() {

    }

    @Test
    public void setNull() {
    }

    @Test
    public void setBoolean() {
    }

    @Test
    public void setByte() {
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

    @Test
    public void setBigDecimal() {
    }

    @Test
    public void setString() {
    }

    @Test
    public void setBytes() {
    }

    @Test
    public void setDate() {
    }

    @Test
    public void setTime() {
    }

    @Test
    public void setTimestamp() {
    }

    @Test
    public void setAsciiStream() {
    }

    @Test
    public void setUnicodeStream() {
    }

    @Test
    public void setBinaryStream() {
    }

    @Test
    public void clearParameters() {
    }

    @Test
    public void setObject() {

    }

    @Test
    public void execute() {
    }

    @Test
    public void addBatch() {

    }

    @Test
    public void setCharacterStream() {
    }

    @Test
    public void setRef() {
    }

    @Test
    public void setBlob() {
    }

    @Test
    public void setClob() {
    }

    @Test
    public void setArray() {
    }

    @Test
    public void getMetaData() {
    }

    @Test
    public void setURL() {
    }

    @Test
    public void getParameterMetaData() {
    }

    @Test
    public void setRowId() {
    }

    @Test
    public void setNString() {
    }

    @Test
    public void setNCharacterStream() {
    }

    @Test
    public void setNClob() {

    }

    @Test
    public void setSQLXML() {

    }


    @BeforeClass
    public static void beforeClass() {
        try {
            Class.forName("com.taosdata.jdbc.rs.RestfulDriver");
            conn = DriverManager.getConnection("jdbc:TAOS://" + host + ":6030/jdbc_test?user=root&password=taosdata");
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