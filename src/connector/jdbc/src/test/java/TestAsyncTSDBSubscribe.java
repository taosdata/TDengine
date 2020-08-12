import com.taosdata.jdbc.*;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class TestAsyncTSDBSubscribe {
    public static void main(String[] args) throws SQLException {
        String usage = "java -cp taos-jdbcdriver-2.0.0_dev-dist.jar com.taosdata.jdbc.TSDBSubscribe -db dbName -topic topicName " +
                "-tname tableName -h host";
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
        long subscribId = 0;
        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");

            Properties properties = new Properties();
            properties.setProperty(TSDBDriver.PROPERTY_KEY_HOST, host);
            properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
            properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
            properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
            properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");

            connection = DriverManager.getConnection("jdbc:TAOS://" + host + ":0/" + dbName + "?user=root&password=taosdata", properties);
            String rawSql = "select * from " + tName + ";";
            TSDBSubscribe subscribe = ((TSDBConnection) connection).createSubscribe();
            subscribId = subscribe.subscribe(topic, rawSql, false, 1000, new CallBack("first"));
            long subscribId2 = subscribe.subscribe("test", rawSql, false, 1000, new CallBack("second"));
            int a = 0;
            Thread.sleep(2000);
            subscribe.unsubscribe(subscribId, true);
            System.err.println("cancel subscribe");
        } catch (Exception e) {
            e.printStackTrace();
            if (null != connection && !connection.isClosed()) {
                connection.close();
            }
        }
    }

    private static class CallBack implements TSDBSubscribeCallBack {
        private String name = "";

        public CallBack(String name) {
            this.name = name;
        }

        @Override
        public void invoke(TSDBResultSet resultSet) {
            try {
                while (null !=resultSet && resultSet.next()) {
                    System.out.print("callback_" + name + ": ");
                    for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                        System.out.printf(i + ": " + resultSet.getString(i) + "\t");
                    }
                    System.out.println();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
