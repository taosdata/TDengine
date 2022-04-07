package com.taos.example;

import com.taosdata.jdbc.TSDBConnection;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.TSDBResultSet;
import com.taosdata.jdbc.TSDBSubscribe;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class SubscribeDemo {
    private static final String topic = "topic-meter-current-bg-10";
    private static final String sql = "select * from meters where current > 10";

    public static void main(String[] args) {
        Connection connection = null;
        TSDBSubscribe subscribe = null;

        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");
            Properties properties = new Properties();
            properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
            properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
            String jdbcUrl = "jdbc:TAOS://127.0.0.1:6030/power?user=root&password=taosdata";
            connection = DriverManager.getConnection(jdbcUrl, properties);
            subscribe = ((TSDBConnection) connection).subscribe(topic, sql, true); // 创建订阅
            int count = 0;
            while (count < 10) {
                TimeUnit.SECONDS.sleep(1); // 等待1秒，避免频繁调用 consume，给服务端造成压力
                TSDBResultSet resultSet = subscribe.consume(); // 消费数据
                if (resultSet == null) {
                    continue;
                }
                ResultSetMetaData metaData = resultSet.getMetaData();
                while (resultSet.next()) {
                    int columnCount = metaData.getColumnCount();
                    for (int i = 1; i <= columnCount; i++) {
                        System.out.print(metaData.getColumnLabel(i) + ": " + resultSet.getString(i) + "\t");
                    }
                    System.out.println();
                    count++;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (null != subscribe)
                    subscribe.close(true); // 关闭订阅
                if (connection != null)
                    connection.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }
}