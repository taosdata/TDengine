package com.taosdata.example;

import com.alibaba.fastjson.JSON;
import com.taosdata.jdbc.TSDBDriver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Scanner;


public class ConsumerLoopImp {

    public static void main(String[] args) throws SQLException, InterruptedException {
        String url = "jdbc:TAOS://localhost:6030/power?user=root&password=taosdata";
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "C");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");

// ANCHOR: create_topic
Connection connection = DriverManager.getConnection(url, properties);
Statement statement = connection.createStatement();
statement.executeUpdate("CREATE TOPIC IF NOT EXISTS topic_meters AS SELECT ts, current, voltage, phase, groupid, location FROM meters");
// ANCHOR_END: create_topic

        statement.close();
        connection.close();

        final AbsConsumerLoop consumerLoop = new AbsConsumerLoop() {
            @Override
            public void process(ResultBean result) {
                System.out.println("data: " + JSON.toJSONString(result));
            }
        };


        // 创建并启动第1个线程
        Thread thread1 = new Thread(() -> {
            // 在这里执行你的代码
            try {
                consumerLoop.pollData();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        thread1.start();

        // 创建并启动第2个线程
        Thread thread2 = new Thread(() -> {
            Scanner scanner = new Scanner(System.in);
            System.out.println("输入任何字符结束程序：");
            String input = scanner.nextLine();
            System.out.println("准备结束程序......");
            try {
                consumerLoop.shutdown();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            scanner.close();
        });
        thread2.start();


        // 等待两个线程结束
        thread1.join();
        thread2.join();

        System.out.println("程序结束");
    }
}