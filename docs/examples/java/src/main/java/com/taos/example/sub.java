package com.taos.example;

import com.taosdata.jdbc.tmq.ConsumerRecord;
import com.taosdata.jdbc.tmq.ConsumerRecords;
import com.taosdata.jdbc.tmq.TMQConstants;
import com.taosdata.jdbc.tmq.TaosConsumer;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class Consumer {
  public static void main(String[] args) throws SQLException {

    String url = System.getenv("TDENGINE_JDBC_URL");

    Properties properties = new Properties();
    properties.setProperty(TMQConstants.CONNECT_TYPE, "websocket");
    properties.setProperty(TMQConstants.CONNECT_URL, url);
    properties.setProperty(TMQConstants.CONNECT_TIMEOUT, "10000");
    properties.setProperty(TMQConstants.CONNECT_MESSAGE_TIMEOUT, "10000");
    properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
    properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
    properties.setProperty(TMQConstants.AUTO_OFFSET_RESET, "earliest");
    properties.setProperty(TMQConstants.GROUP_ID, "gId");
    properties.setProperty(TMQConstants.VALUE_DESERIALIZER, "com.taosdata.jdbc.tmq.MapDeserializer");

    try (TaosConsumer<Map<String, Object>> consumer = new TaosConsumer<>(properties)) {
      consumer.subscribe(Collections.singletonList("test"));
      for (int i = 0; i < 100; i++) {
        ConsumerRecords<Map<String, Object>> consumerRecords = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<Map<String, Object>> r : consumerRecords) {
          Map<String, Object> bean = r.value();
          bean.forEach((k, v) -> {
            System.out.print(k + " : " + v + " ");
          });
          System.out.println();
        }
      }
      consumer.unsubscribe();
    }
  }
}