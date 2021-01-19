import com.taosdata.jdbc.TSDBDriver;

import java.sql.*;
import java.util.Properties;

public class TestTSDBDatabaseMetaData {

    public static void main(String[] args) throws SQLException {
        Connection connection = null;
        DatabaseMetaData dbMetaData;
        ResultSet resSet;
        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");
            Properties properties = new Properties();
            properties.setProperty(TSDBDriver.PROPERTY_KEY_HOST, "localhost");
            connection = DriverManager.getConnection("jdbc:TAOS://localhost:0/", properties);
            dbMetaData = connection.getMetaData();
            resSet = dbMetaData.getCatalogs();
            while(resSet.next()) {
                for (int i = 1; i <= resSet.getMetaData().getColumnCount(); i++) {
                    System.out.printf("dbMetaData.getCatalogs(%d) = %s\n", i, resSet.getString(i));
                }
            }
            resSet.close();

        } catch (Exception e) {
            e.printStackTrace();
            if (null != connection) {
                connection.close();
            }
        }
    }
}
