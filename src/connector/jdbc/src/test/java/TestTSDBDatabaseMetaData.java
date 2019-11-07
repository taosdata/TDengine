import com.taosdata.jdbc.TSDBDriver;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Properties;

public class TestTSDBDatabaseMetaData {

    public static void main(String[] args) {
        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");
            Properties properties = new Properties();
            properties.setProperty(TSDBDriver.PROPERTY_KEY_HOST, "192.168.1.114");
            Connection connection = DriverManager.getConnection("jdbc:TAOS://192.168.1.114:0/?user=root&password=taosdata", properties);
            DatabaseMetaData dbMetaData = connection.getMetaData();
            ResultSet resSet = dbMetaData.getCatalogs();
            while(resSet.next()) {
                for (int i = 1; i <= resSet.getMetaData().getColumnCount(); i++) {
                    System.out.printf("dbMetaData.getCatalogs(%d) = %s\n", i, resSet.getString(i));
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
