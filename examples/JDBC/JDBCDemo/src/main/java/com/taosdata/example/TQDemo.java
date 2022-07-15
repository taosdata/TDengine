package com.taosdata.example;

import com.taosdata.jdbc.TSDBConnection;
import com.taosdata.jdbc.TSDBResultSet;
import com.taosdata.jdbc.TSDBSubscribe;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class TQDemo {
    private static final String host = "192.168.56.105";
    private static final String topic = "test_tq";

    public static void main(String[] args) {
        try (Connection conn = getConnection()) {
            createTopic(conn);

            Thread producer = new Thread(new Producer(conn, topic, 1));
            producer.start();

            Thread consumer = new Thread(new Consumer(conn, topic, 1), "Consumer");
            consumer.start();

            producer.join();
            consumer.join();

        } catch (SQLException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static Connection getConnection() throws SQLException {
        final String jdbcUrl = "jdbc:TAOS://" + host + ":6030/";
        return DriverManager.getConnection(jdbcUrl, "root", "taosdata");
    }

    private static void createTopic(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop topic if exists " + topic);
            stmt.execute("create topic if not exists " + topic + " partitions 1");
        }
    }

    private static class Producer implements Runnable {
        private final Connection conn;
        private final String topic;
        private final int partitionIndex;

        private Producer(Connection conn, String topic, int partitionIndex) {
            this.conn = conn;
            this.topic = topic;
            this.partitionIndex = partitionIndex;
        }

        @Override
        public void run() {
            try (Statement stmt = conn.createStatement()) {
                for (int i = 0; i < 10; i++) {
                    stmt.execute("insert into " + topic + ".p" + partitionIndex + " (off, ts, content) values(0, now, 'abcdefg')");
                    TimeUnit.SECONDS.sleep(1);
                }
            } catch (SQLException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static class Consumer implements Runnable {
        private static final SimpleDateFormat formator = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        private final Connection conn;
        private final String topic;
        private final int partitionIndex;

        private Consumer(Connection conn, String topic, int partitionIndex) {
            this.conn = conn;
            this.topic = topic;
            this.partitionIndex = partitionIndex;
        }

        @Override
        public void run() {
            try {
                System.out.println(Thread.currentThread().getName() + " started");
                TSDBConnection tsdbConn = conn.unwrap(TSDBConnection.class);
                final String sql = "select * from " + topic + ".p" + partitionIndex;
                TSDBSubscribe subscribe = tsdbConn.subscribe(topic, sql, false);


                for (int count = 0; true; ) {
                    TSDBResultSet rs = subscribe.consume();
                    while (rs.next()) {
                        long offset = rs.getLong("off");
                        Timestamp ts = rs.getTimestamp("ts");
                        String content = rs.getString("content");

                        System.out.println(Thread.currentThread().getName()
                                + " >>> offset: " + offset
                                + ", ts: " + formator.format(new Date(ts.getTime()))
                                + ", content: " + content);
                        count++;
                    }
                    if (count == 10)
                        break;
                }
                System.out.println(Thread.currentThread().getName() + " stopped");

            } catch (SQLException e) {
                e.printStackTrace();
            }

        }
    }

}
