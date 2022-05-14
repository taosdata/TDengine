package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.enums.TimestampFormat;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Ignore
public class WSSelectTest {
    //    private static final String host = "192.168.1.98";
    private static final String host = "127.0.0.1";
    private static final int port = 6041;
    private static Connection connection;
    private static final String databaseName = "driver";

    private static void testInsert() throws SQLException {
        Statement statement = connection.createStatement();
        long cur = System.currentTimeMillis();
        List<String> timeList = new ArrayList<>();
        for (long i = 0L; i < 3000; i++) {
            long t = cur + i;
            timeList.add("insert into " + databaseName + ".alltype_query values(" + t + ",1,1,1,1,1,1,1,1,1,1,1,'test_binary','test_nchar')");
        }
        for (int i = 0; i < 3000; i++) {
            statement.execute(timeList.get(i));
        }
        statement.close();
    }

    @Test
    public void testWSSelect() throws SQLException {
        Statement statement = connection.createStatement();
        int count = 0;
        long start = System.nanoTime();
        for (int i = 0; i < 1000; i++) {
            ResultSet resultSet = statement.executeQuery("select ts,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13 from " + databaseName + ".alltype_query limit 3000");
            while (resultSet.next()) {
                count++;
                resultSet.getTimestamp(1);
                resultSet.getBoolean(2);
                resultSet.getInt(3);
                resultSet.getInt(4);
                resultSet.getInt(5);
                resultSet.getLong(6);
                resultSet.getInt(7);
                resultSet.getInt(8);
                resultSet.getLong(9);
                resultSet.getLong(10);
                resultSet.getFloat(11);
                resultSet.getDouble(12);
                resultSet.getString(13);
                resultSet.getString(14);
            }
        }
        long d = System.nanoTime() - start;
        System.out.println(d / 1000);
        System.out.println(count);
        statement.close();
    }

    @BeforeClass
    public static void beforeClass() throws SQLException {
        String url = "jdbc:TAOS-RS://" + host + ":" + port + "/?user=root&password=taosdata";
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIMESTAMP_FORMAT, String.valueOf(TimestampFormat.UTC));
        properties.setProperty(TSDBDriver.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT, "100000");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
        connection = DriverManager.getConnection(url, properties);
        Statement statement = connection.createStatement();
        statement.execute("drop database if exists " + databaseName);
        statement.execute("create database " + databaseName);
        statement.execute("create table " + databaseName + ".alltype_query(ts timestamp, c1 bool,c2 tinyint, c3 smallint, c4 int, c5 bigint, c6 tinyint unsigned, c7 smallint unsigned, c8 int unsigned, c9 bigint unsigned, c10 float, c11 double, c12 binary(20), c13 nchar(30) )");
        statement.close();
        testInsert();
    }
}
