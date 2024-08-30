package com.taosdata;

import com.alibaba.fastjson.JSON;
import com.google.common.util.concurrent.RateLimiter;
import com.taosdata.jdbc.tmq.ConsumerRecord;
import com.taosdata.jdbc.tmq.ConsumerRecords;
import com.taosdata.jdbc.tmq.TaosConsumer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Semaphore;

public class Worker implements Runnable {

    int sleepTime;


    ForkJoinPool pool = new ForkJoinPool();


    TaosConsumer<Map<String, Object>> consumer;
    AlertConfig alertConfig;

    Map<String, Map<String, Boolean>> bigBoolMap = new HashMap<>();

    public Worker(Properties prop, AlertConfig alertConfig) throws SQLException {
        consumer = new TaosConsumer<Map<String, Object>>(prop);
        consumer.subscribe(Collections.singletonList(alertConfig.getTopic()));
         sleepTime = 100;
        this.alertConfig = alertConfig;
    }

    public static void logAlert(AlertConfig alertConfig, String message) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(alertConfig.getAlertFile(), true))) {
            writer.write(message);
            writer.newLine();
            System.out.println(message);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void logAlertMsg(AlertConfig alertConfig, String tbName, Timestamp ts, String column, String value, String alertLevel) {
        String alertInfo = "[" + LocalDateTime.now() + "] alert: table_name = " + tbName + ", ts = " + ts;
        alertInfo += ", column = " + column + ", value = " + value +  ", alert_level = " + alertLevel;
        logAlert(alertConfig, alertInfo);
    }

    public static void logAlertBoolMsg(AlertConfig alertConfig, String tbName, Timestamp ts, String column, Boolean value, String alertLevel) {
        String alertInfo = "[" + LocalDateTime.now() + "] alert: table_name = " + tbName + ", ts = " + ts;
        alertInfo += ", column = " + column + ", value changed to " + value +  ", alert_level = " + alertLevel;
        logAlert(alertConfig, alertInfo);
    }

    @Override
    public void run() {
        while (!Thread.interrupted()) {
            try {
               // 控制请求频率
                ConsumerRecords<Map<String, Object>> records = consumer.poll(Duration.ofMillis(sleepTime));
                pool.submit(() -> {
                    try {
                        for (ConsumerRecord<Map<String, Object>> record : records) {
                            // 流量控制
                            Map<String, Object> colMap = record.value();

                            Object tableName = colMap.get("tbname");


                            String tbName = new String((byte[]) tableName, StandardCharsets.UTF_8);

                            if (!bigBoolMap.containsKey(tbName)){
                                bigBoolMap.put(tbName, new HashMap<>());
                            }

                            for (BoolAlert boolAlert: alertConfig.getBoolAlertList()) {
                                String column = boolAlert.getColName();

                                if (!colMap.containsKey(column)) {
                                    System.out.println("column not found: " + column);
                                    System.exit(-1);
                                }

                                if (!(colMap.get(column) instanceof Boolean)) {
                                    System.out.println("column is not boolean: " + column);
                                    System.exit(-1);
                                }

                                Boolean value = (Boolean) colMap.get(column);

                                if (bigBoolMap.get(tbName).containsKey(column)) {
                                    Boolean oldValue = bigBoolMap.get(tbName).get(column);

                                    // false -> true
                                    if (Boolean.FALSE.equals(oldValue) && Boolean.TRUE.equals(value) && boolAlert.getToTrueLevel() != null) {
                                        logAlertBoolMsg(alertConfig, tbName, (Timestamp) colMap.get("ts"), column, value, boolAlert.getToTrueLevel());
                                    }

                                    if (Boolean.TRUE.equals(oldValue) && Boolean.FALSE.equals(value) && boolAlert.getToFalseLevel() != null) {
                                        logAlertBoolMsg(alertConfig, tbName, (Timestamp) colMap.get("ts"), column, value, boolAlert.getToFalseLevel());
                                    }
                                }
                            }

                            for (NumericAlert numericAlert: alertConfig.getNumericAlertList()){
                                String column = numericAlert.getColName();
                                if (!colMap.containsKey(column)) {
                                    System.out.println("column not found: " + column);
                                    System.exit(-1);
                                }

                                if (!(colMap.get(column) instanceof Number)) {
                                    System.out.println("column is not number: " + column);
                                    System.exit(-1);
                                }

                                Object value = colMap.get(column);
                                Double doubleValue = null;

                                if (value instanceof Short) {
                                    doubleValue = (Short) value * 1.0;
                                }
                                if (value instanceof Long) {
                                    doubleValue = (Long) value * 1.0;
                                }
                                if (value instanceof Integer) {
                                    doubleValue = (Integer) value * 1.0;
                                }
                                if (value instanceof Float) {
                                    doubleValue = ((Float) value).doubleValue();
                                }
                                if (value instanceof Double) {
                                    doubleValue = (Double) value;
                                }

                                if (doubleValue == null){
                                    System.out.println("column is not number: " + column);
                                    System.exit(-1);
                                }

                                if (doubleValue >= numericAlert.getMin() && doubleValue < numericAlert.getMax()) {
                                    logAlertMsg(alertConfig, tbName, (Timestamp) colMap.get("ts"), column, value.toString(), numericAlert.getLevel());
                                }
                            }

                            colMap.forEach((column, value) -> {
                              if (value instanceof Boolean) {
                                  bigBoolMap.get(tbName).put(column, (Boolean) value);
                              }
                            });
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.exit(-1);
                    }
                });
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
