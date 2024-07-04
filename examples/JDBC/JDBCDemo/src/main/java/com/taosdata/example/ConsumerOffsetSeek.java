package com.taosdata.example;

import com.alibaba.fastjson.JSON;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.tmq.ConsumerRecords;
import com.taosdata.jdbc.tmq.TaosConsumer;
import com.taosdata.jdbc.tmq.TopicPartition;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;


public class ConsumerOffsetSeek {

    public static void main(String[] args) throws SQLException, InterruptedException {
        Properties config = new Properties();
        config.setProperty("td.connect.type", "jni");
        config.setProperty("bootstrap.servers", "localhost:6030");
        config.setProperty("td.connect.user", "root");
        config.setProperty("td.connect.pass", "taosdata");
        config.setProperty("auto.offset.reset", "latest");
        config.setProperty("msg.with.table.name", "true");
        config.setProperty("enable.auto.commit", "true");
        config.setProperty("auto.commit.interval.ms", "1000");
        config.setProperty("group.id", "group1");
        config.setProperty("client.id", "1");
        config.setProperty("value.deserializer", "com.taosdata.example.AbsConsumerLoop$ResultDeserializer");
        config.setProperty("value.deserializer.encoding", "UTF-8");

// ANCHOR: consumer_seek
String topic = "topic_meters";
Map<TopicPartition, Long> offset = null;
try (TaosConsumer<AbsConsumerLoop.ResultBean> consumer = new TaosConsumer<>(config)) {
    consumer.subscribe(Collections.singletonList(topic));
    for (int i = 0; i < 10; i++) {
        if (i == 3) {
            // Saving consumption position
            offset = consumer.position(topic);
        }
        if (i == 5) {
            // reset consumption to the previously saved position
            for (Map.Entry<TopicPartition, Long> entry : offset.entrySet()) {
                consumer.seek(entry.getKey(), entry.getValue());
            }
        }
        ConsumerRecords<AbsConsumerLoop.ResultBean> records = consumer.poll(Duration.ofMillis(500));
    }
}
// ANCHOR_END: consumer_seek
    }
}