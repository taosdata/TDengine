package com.taosdata.example;

import com.taosdata.jdbc.TSDBConnection;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.TSDBResultSet;
import com.taosdata.jdbc.TSDBSubscribe;

import java.sql.DriverManager;
import java.util.Properties;

public class SubscribeDemo {

    public static TSDBConnection getConnection(String host, String database) throws Exception {
        Class.forName("com.taosdata.jdbc.TSDBDriver");
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HOST, host);
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");

        String cs = String.format("jdbc:TAOS://%s:0/%s", host, database);
        return (TSDBConnection) DriverManager.getConnection(cs, properties);
    }

    public static void main(String[] args) throws Exception {
        String usage = "java -Djava.ext.dirs=../ TestTSDBSubscribe [-host host] <-db database> <-topic topic> <-sql sql>";
        if (args.length < 2) {
            System.err.println(usage);
            return;
        }

        String host = "localhost", database = "", topic = "", sql = "";
        for (int i = 0; i < args.length; i++) {
            if ("-db".equalsIgnoreCase(args[i]) && i < args.length - 1) {
                database = args[++i];
            }
            if ("-topic".equalsIgnoreCase(args[i]) && i < args.length - 1) {
                topic = args[++i];
            }
            if ("-host".equalsIgnoreCase(args[i]) && i < args.length - 1) {
                host = args[++i];
            }
            if ("-sql".equalsIgnoreCase(args[i]) && i < args.length - 1) {
                sql = args[++i];
            }
        }
        if (database.isEmpty() || topic.isEmpty() || sql.isEmpty()) {
            System.err.println(usage);
            return;
        }

        TSDBConnection connection = null;
        TSDBSubscribe sub = null;
        try {
            connection = getConnection(host, database);
            sub = ((TSDBConnection) connection).subscribe(topic, sql, false);

            int total = 0;
            while (true) {
                TSDBResultSet rs = sub.consume();
                int count = 0;
                while (rs.next()) {
                    count++;
                }
                total += count;
                System.out.printf("%d rows consumed, total %d\n", count, total);
                Thread.sleep(900);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != sub) {
                sub.close(true);
            }
            if (null != connection) {
                connection.close();
            }
        }
    }
}
