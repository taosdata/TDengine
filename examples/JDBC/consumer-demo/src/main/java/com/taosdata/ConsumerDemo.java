package com.taosdata;

import com.taosdata.jdbc.tmq.TMQConstants;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.taosdata.Config.*;

public class ConsumerDemo {
    public static void main(String[] args) throws SQLException {
        // Config
        Config config = Config.getFromENV();
        // Generated data
        mockData();

        Properties prop = new Properties();
        prop.setProperty(TMQConstants.CONNECT_TYPE, config.getType());
        prop.setProperty(TMQConstants.BOOTSTRAP_SERVERS, config.getHost() + ":" + config.getPort());
        prop.setProperty(TMQConstants.CONNECT_USER, "root");
        prop.setProperty(TMQConstants.CONNECT_PASS, "taosdata");
        prop.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        prop.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        prop.setProperty(TMQConstants.GROUP_ID, "gId");
        prop.setProperty(TMQConstants.VALUE_DESERIALIZER, "com.taosdata.BeanDeserializer");
        for (int i = 0; i < config.getConsumerNum() - 1; i++) {
            new Thread(new Worker(prop, config)).start();
        }
        new Worker(prop, config).run();
    }

    public static void mockData() throws SQLException {
        String dbName = "test_consumer";
        String tableName = "st";
        String url = "jdbc:TAOS-RS://" + TAOS_HOST + ":" + TAOS_PORT + "/?user=root&password=taosdata&batchfetch=true";
        Connection connection = DriverManager.getConnection(url);
        Statement statement = connection.createStatement();
        statement.executeUpdate("create database if not exists " + dbName + " WAL_RETENTION_PERIOD 3650");
        statement.executeUpdate("use " + dbName);
        statement.executeUpdate("create table if not exists " + tableName + " (ts timestamp, c1 int, c2 nchar(100)) ");
        statement.executeUpdate("create topic if not exists " + TOPIC + " as select ts, c1, c2 from " + tableName);

        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setName("mock-data-thread-" + t.getId());
            return t;
        });
        AtomicInteger atomic = new AtomicInteger();
        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            int i = atomic.getAndIncrement();
            try {
                statement.executeUpdate("insert into " + tableName + " values(now, " + i + ",'" + i + "')");
            } catch (SQLException e) {
                // ignore
            }
        }, 0, 10, TimeUnit.MILLISECONDS);
    }
}
