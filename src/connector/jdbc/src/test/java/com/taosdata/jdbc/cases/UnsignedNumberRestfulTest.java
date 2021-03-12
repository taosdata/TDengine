package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.TSDBDriver;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.sql.*;
import java.util.Properties;


@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class UnsignedNumberRestfulTest {
    //    private static final String host = "127.0.0.1";
    private static final String host = "master";
    private static Connection restfulConn;

    @Test
    public void testCase001() {
        try (Statement stmt = restfulConn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from us_table");
            ResultSetMetaData meta = rs.getMetaData();
            while (rs.next()) {
                for (int i = 1; i <= meta.getColumnCount(); i++) {
                    System.out.print(meta.getColumnLabel(i) + ": " + rs.getString(i) + "\t");
                }
                System.out.println();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @BeforeClass
    public static void beforeClass() {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");

        try {
            Class.forName("com.taosdata.jdbc.rs.RestfulDriver");
            final String url = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata";
            restfulConn = DriverManager.getConnection(url, properties);

            Statement stmt = restfulConn.createStatement();
            stmt.execute("drop database if exists unsign_restful");
            stmt.execute("create database if not exists unsign_restful");
            stmt.execute("use unsign_restful");
            stmt.execute("create table us_table(ts timestamp, f1 tinyint unsigned, f2 smallint unsigned, f3 int unsigned, f4 bigint unsigned)");
            stmt.executeUpdate("insert into us_table(ts,f1,f2,f3,f4) values(now, 254, 65534,4294967294, 18446744073709551614)");
            stmt.close();
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void afterClass() {
        try {
            if (restfulConn != null)
                restfulConn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
