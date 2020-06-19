import com.taosdata.jdbc.TSDBConnection;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.TSDBResultSet;
import com.taosdata.jdbc.TSDBSubscribe;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class TestTSDBSubscribe {
    public static void main(String[] args) throws Exception {
        String usage = "java -cp taos-jdbcdriver-1.0.3_dev-dist.jar com.taosdata.jdbc.TSDBSubscribe -db dbName " +
                "-topic topicName -tname tableName -h host";
        if (args.length < 2) {
            System.err.println(usage);
            return;
        }

        String dbName = "";
        String tName = "";
        String host = "localhost";
        String topic = "";
        for (int i = 0; i < args.length; i++) {
            if ("-db".equalsIgnoreCase(args[i]) && i < args.length - 1) {
                dbName = args[++i];
            }
            if ("-tname".equalsIgnoreCase(args[i]) && i < args.length - 1) {
                tName = args[++i];
            }
            if ("-h".equalsIgnoreCase(args[i]) && i < args.length - 1) {
                host = args[++i];
            }
            if ("-topic".equalsIgnoreCase(args[i]) && i < args.length - 1) {
                topic = args[++i];
            }
        }
        if (StringUtils.isEmpty(dbName) || StringUtils.isEmpty(tName) || StringUtils.isEmpty(topic)) {
            System.err.println(usage);
            return;
        }

        Connection connection = null;
        TSDBSubscribe subscribe = null;
        long subscribId = 0;
        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");
            Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HOST, host);
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
            connection = DriverManager.getConnection("jdbc:TAOS://" + host + ":0/" + dbName + "?user=root&password=taosdata"
                    , properties);
            String rawSql = "select * from " + tName + ";";
            subscribe = ((TSDBConnection) connection).createSubscribe();
            subscribId = subscribe.subscribe(topic, rawSql, false, 1000);
            int a = 0;
            TSDBResultSet resSet = null;
            while (true) {
                Thread.sleep(900);
                resSet = subscribe.consume(subscribId);

                while (resSet.next()) {
                    for (int i = 1; i <= resSet.getMetaData().getColumnCount(); i++) {
                        System.out.printf(i + ": " + resSet.getString(i) + "\t");
                    }
                    System.out.println("\n======" + a + "==========");
                }

                a++;
                if (a >= 10) {
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != subscribe && 0 != subscribId) {
                subscribe.unsubscribe(subscribId, true);
            }
            if (null != connection) {
                connection.close();
            }
        }
    }
}
