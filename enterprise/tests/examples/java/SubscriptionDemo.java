package com.taosdata.iot.clients;

import com.taosdata.iot.clients.topics.STableTopic;
import com.taosdata.iot.clients.topics.TableTopic;
import com.taosdata.iot.clients.topics.Topic;
import com.taosdata.jdbc.TSDBResultSetMetaData;

import java.util.Properties;

/**
 * @author Jiangyi Hou
 * @since 18-12-3
 */
public class SubscriptionDemo {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("host", "127.0.0.1");
        props.setProperty("database", "db1");
        props.setProperty("user", "root");
        props.setProperty("password", "taosdata");
        props.setProperty("cfgdir", "/home/jyhou/sim/dnode1/cfg");
//        props.setProperty("charset", "UTF-8");
//        props.setProperty("locale", "en_US.utf-8");
//        props.setProperty("timezone", "UTC-8");
        Consumer consumer = new TaosConsumer(props);
        Topic tableTopic = new TableTopic("tb2");
        Topic stableTopic = new STableTopic("select * from stb where devid > 2 and ts < '2019-09-17 00:00:00.000'");
        try {
            consumer.subscribe(tableTopic);
            consumer.subscribe(stableTopic);
            Long stime = 0L;
            long loop = 0l;
            TSDBResultSetMetaData metaData;
            ConsumerResultSet consumerResultSet;
            while (loop < 100000000) {
                loop++;
                stime = System.currentTimeMillis();
                System.out.printf("Polling start time: %d\n", stime);
                consumerResultSet = consumer.poll(5000, 500, 1000);
                System.out.printf("Polling finished, time used: %d\n", System.currentTimeMillis() - stime);
                metaData = consumerResultSet.getResultSetMetaData();
                System.out.printf("ConsumerResultSet.size(): %d\n", consumerResultSet.size());
//                if (consumerResultSet != null && consumerResultSet.size() > 0) {
//                    for (TSDBResultSetRowData rowData : consumerResultSet.getRows()) {
//                        for (int col = 0; col < metaData.getColumnCount(); col++) {
//                            System.out.printf("%s  |  ", rowData.get(col).toString());
//                        }
//                        System.out.println("\n");
//                    }
//                    continue;
//                } else {
//                    System.out.println("sleep for 1 s");
//                    Thread.currentThread().sleep(1000);
//                }
                while (consumerResultSet.next()) {
                        for (int col = 1; col <= metaData.getColumnCount(); col++) {
                            System.out.printf("%s  |  ", consumerResultSet.getString(col));
                        }
                        System.out.println("\n");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
