import com.taosdata.jdbc.TSDBConnection;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.TSDBResultSet;
import com.taosdata.jdbc.TSDBSubscribe;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class TestTSDBSubscribeSample {
    public static void main(String[] args) throws Exception {
        // use log db
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
            connection = DriverManager.getConnection("jdbc:TAOS://" + host + ":0/" + dbName + "?user=root&password=taosdata"
                    , properties);
            String rawSql = "select * from " + tName + ";";
            subscribe = ((TSDBConnection) connection).createSubscribe();
            subscribId = subscribe.subscribe(topic, rawSql, false, 1000);
            int a = 0;
            while (true) {
                Thread.sleep(1000);
                TSDBResultSet resSet = subscribe.consume(subscribId);

                while (resSet.next()) {
                    for (int i = 1; i <= resSet.getMetaData().getColumnCount(); i++) {
                        System.out.printf(i + ": " + resSet.getString(i) + "\t");
                    }
                    System.out.println("\n================");
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
