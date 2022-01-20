package com.taosdata.jdbc.cases;

import org.junit.Test;

import java.sql.*;

public class JDBCTypeAndTypeCompareTest {

    @Test
    public void test() throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:TAOS://192.168.17.156:6030/", "root", "taosdata");
        Statement stmt = conn.createStatement();

        stmt.execute("drop database if exists test");
        stmt.execute("create database if not exists test");
        stmt.execute("use test");
        stmt.execute("create table weather(ts timestamp, f1 int, f2 bigint, f3 float, f4 double, f5 smallint, f6 tinyint, f7 bool, f8 binary(10), f9 nchar(10) )");
        stmt.execute("insert into weather values(now, 1, 2, 3.0, 4.0, 5, 6, true, 'test','test')");

        ResultSet rs = stmt.executeQuery("select * from weather");
        ResultSetMetaData meta = rs.getMetaData();
        while (rs.next()) {
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                String columnName = meta.getColumnName(i);
                String columnTypeName = meta.getColumnTypeName(i);
                Object value = rs.getObject(i);
                System.out.printf("columnName : %s, columnTypeName: %s, JDBCType: %s\n", columnName, columnTypeName, value.getClass().getName());
            }
        }

        stmt.close();
        conn.close();
    }
}
