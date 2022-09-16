package com.taos.example;

import com.taosdata.jdbc.tmq.ConsumerRecords;
import com.taosdata.jdbc.tmq.TMQConstants;
import com.taosdata.jdbc.tmq.TaosConsumer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

public class SubscribeDemo {
    private static final String TOPIC = "tmq_topic";
    private static final String DB_NAME = "meters";
    private static final AtomicBoolean shutdown = new AtomicBoolean(false);

    public static void main(String[] args) {
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            public void run() {
                shutdown.set(true);
            }
        }, 3_000);
        try {
            // prepare
            Class.forName("com.taosdata.jdbc.TSDBDriver");
            String jdbcUrl = "jdbc:TAOS://127.0.0.1:6030/?user=root&password=taosdata";
            Connection connection = DriverManager.getConnection(jdbcUrl);
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("drop topic if exists " + TOPIC);
                statement.executeUpdate("drop database if exists " + DB_NAME);
                statement.executeUpdate("create database " + DB_NAME);
                statement.executeUpdate("use " + DB_NAME);
                statement.executeUpdate(
                        "CREATE TABLE `meters` (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT) TAGS (`groupid` INT, `location` BINARY(24))");
                statement.executeUpdate("CREATE TABLE `d0` USING `meters` TAGS(0, 'California.LosAngles')");
                statement.executeUpdate("INSERT INTO `d0` values(now - 10s, 0.32, 116)");
                statement.executeUpdate("INSERT INTO `d0` values(now - 8s, NULL, NULL)");
                statement.executeUpdate(
                        "INSERT INTO `d1` USING `meters` TAGS(1, 'California.SanFrancisco') values(now - 9s, 10.1, 119)");
                statement.executeUpdate(
                        "INSERT INTO `d1` values (now-8s, 10, 120) (now - 6s, 10, 119) (now - 4s, 11.2, 118)");
                // create topic
                statement.executeUpdate("create topic " + TOPIC + " as select * from meters");
            }

            // create consumer
            Properties properties = new Properties();
            properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, "127.0.0.1:6030");
            properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
            properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
            properties.setProperty(TMQConstants.GROUP_ID, "test");
            properties.setProperty(TMQConstants.VALUE_DESERIALIZER,
                    "com.taos.example.MetersDeserializer");

            // poll data
            try (TaosConsumer<Meters> consumer = new TaosConsumer<>(properties)) {
                consumer.subscribe(Collections.singletonList(TOPIC));
                while (!shutdown.get()) {
                    ConsumerRecords<Meters> meters = consumer.poll(Duration.ofMillis(100));
                    for (Meters meter : meters) {
                        System.out.println(meter);
                    }
                }
                consumer.unsubscribe();
            }
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        timer.cancel();
    }
}
