package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.TSDBConnection;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.TSDBResultSet;
import com.taosdata.jdbc.TSDBSubscribe;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class TD4144Test {

    private static TSDBConnection connection;
    private static final String host = "127.0.0.1";

    private static final String topic = "topic-meter-current-bg-10";
    private static final String sql = "select * from meters where current > 10";
    private static final String sql2 = "select * from meters where ts >= '2020-08-15 12:20:00.000'";


    @Test
    public void test() throws SQLException {
        TSDBSubscribe subscribe = null;
        TSDBResultSet res = null;
        boolean hasNext = false;

        try {
            subscribe = connection.subscribe(topic, sql, false);
            int count = 0;
            while (true) {
                // 等待1秒，避免频繁调用 consume，给服务端造成压力
                TimeUnit.SECONDS.sleep(1);
                if (res == null) {
                    // 消费数据
                    res = subscribe.consume();
                    hasNext = res.next();
                }

                if (res == null) {
                    continue;
                }
                ResultSetMetaData metaData = res.getMetaData();
                int number = 0;
                while (hasNext) {
                    int columnCount = metaData.getColumnCount();
                    for (int i = 1; i <= columnCount; i++) {
                        System.out.print(metaData.getColumnLabel(i) + ": " + res.getString(i) + "\t");
                    }
                    System.out.println();
                    count++;
                    number++;
                    hasNext = res.next();
                    if (!hasNext) {
                        res.close();
                        res = null;
                        System.out.println("rows： " + count);
                    }
                    if (hasNext == true && number >= 10) {
                        System.out.println("batch" + number);
                        break;
                    }
                }

            }

        } catch (SQLException | InterruptedException throwables) {
            throwables.printStackTrace();
        } finally {
            if (subscribe != null)
                subscribe.close(true);
        }
    }

    @BeforeClass
    public static void beforeClass() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        String url = "jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata";
        connection = (DriverManager.getConnection(url, properties)).unwrap(TSDBConnection.class);
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("drop database if exists power");
            stmt.execute("create database if not exists power");
            stmt.execute("use power");
            stmt.execute("create table meters(ts timestamp, current float, voltage int, phase int) tags(location binary(64), groupId int)");
            stmt.execute("create table d1001 using meters tags(\"Beijing.Chaoyang\", 2)");
            stmt.execute("create table d1002 using meters tags(\"Beijing.Haidian\", 2)");
            stmt.execute("insert into d1001 values(\"2020-08-15 12:00:00.000\", 12, 220, 1),(\"2020-08-15 12:10:00.000\", 12.3, 220, 2),(\"2020-08-15 12:20:00.000\", 12.2, 220, 1)");
            stmt.execute("insert into d1002 values(\"2020-08-15 12:00:00.000\", 9.9, 220, 1),(\"2020-08-15 12:10:00.000\", 10.3, 220, 1),(\"2020-08-15 12:20:00.000\", 11.2, 220, 1)");
        }
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        if (connection != null)
            connection.close();
    }
}
