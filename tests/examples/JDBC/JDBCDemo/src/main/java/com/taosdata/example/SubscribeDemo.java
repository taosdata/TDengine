package com.taosdata.example;

import com.taosdata.jdbc.TSDBConnection;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.TSDBResultSet;
import com.taosdata.jdbc.TSDBSubscribe;

import java.sql.DriverManager;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class SubscribeDemo {
    private static final String usage = "java -jar SubscribeDemo.jar -host <hostname> -database <database name> -topic <topic> -sql <sql>";

    public static void main(String[] args) {
        // parse args from command line
        String host = "", database = "", topic = "", sql = "";
        for (int i = 0; i < args.length; i++) {
            if ("-host".equalsIgnoreCase(args[i]) && i < args.length - 1) {
                host = args[++i];
            }
            if ("-database".equalsIgnoreCase(args[i]) && i < args.length - 1) {
                database = args[++i];
            }
            if ("-topic".equalsIgnoreCase(args[i]) && i < args.length - 1) {
                topic = args[++i];
            }
            if ("-sql".equalsIgnoreCase(args[i]) && i < args.length - 1) {
                sql = args[++i];
            }
        }
        if (host.isEmpty() || database.isEmpty() || topic.isEmpty() || sql.isEmpty()) {
            System.out.println(usage);
            return;
        }
        /*********************************************************************************************/
        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");
            Properties properties = new Properties();
            properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
            properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
            properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
            final String url = "jdbc:TAOS://" + host + ":6030/" + database + "?user=root&password=taosdata";
            // get TSDBConnection
            TSDBConnection connection = (TSDBConnection) DriverManager.getConnection(url, properties);
            // create TSDBSubscribe
            TSDBSubscribe sub = connection.subscribe(topic, sql, false);

            int total = 0;
            while (true) {
                TSDBResultSet rs = sub.consume();
                int count = 0;
                while (rs.next()) {
                    count++;
                }
                total += count;
                System.out.printf("%d rows consumed, total %d\n", count, total);
                if (total >= 10)
                    break;
                TimeUnit.SECONDS.sleep(1);
            }
            sub.close(false);
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}