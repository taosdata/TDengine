import com.taosdata.jdbc.*;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class TestAsyncTSDBSubscribeSample {
    public static void main(String[] args) {
        String dbName = "log";
        String tName = "dn_10_211_55_3";
        String host = "10.211.55.3";
        String topic = "test";

        Connection connection = null;
        TSDBSubscribe subscribe = null;
        long subscribId = 0;
        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");
            Properties properties = new Properties();
            properties.setProperty(TSDBDriver.PROPERTY_KEY_HOST, host);
            connection = DriverManager.getConnection("jdbc:TAOS://" + host + ":0/" + dbName + "?user=root&password=taosdata", properties);
            String rawSql = "select * from " + tName + ";";
            subscribe = ((TSDBConnection) connection).createSubscribe();
            subscribId = subscribe.subscribe(topic, rawSql, false, 1000, new CallBack("first"));
            long subscribId2 = subscribe.subscribe("test", rawSql, false, 1000, new CallBack("second"));
            int a = 0;
            Thread.sleep(2000);
            subscribe.unsubscribe(subscribId, true);
            System.err.println("cancel subscribe");
        } catch (Exception e) {
            e.printStackTrace();
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
