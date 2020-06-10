import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.TSDBPreparedStatement;

import java.sql.*;
import java.util.Properties;

public class TestPreparedStatement {

    public static void main(String[] args) {
        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");
            Properties properties = new Properties();
            properties.setProperty(TSDBDriver.PROPERTY_KEY_HOST, "192.168.1.117");
            Connection connection = DriverManager.getConnection("jdbc:TAOS://10.211.55.3:0/log?user=root&password=taosdata", properties);
            String createSql = "create table t (ts timestamp, speed int);";
            Statement statement = connection.createStatement();
            statement.executeQuery(createSql);
            String rawSql = "SELECT ts, c1 FROM (select c1, ts from db.tb1) SUB_QRY";
            if (1 < 2) {
                return;
            }
//            String[] params = new String[]{"ts", "c1"};
            PreparedStatement pstmt = (TSDBPreparedStatement) connection.prepareStatement(rawSql);
            ResultSet resSet = pstmt.executeQuery();
            while(resSet.next()) {
                for (int i = 1; i <= resSet.getMetaData().getColumnCount(); i++) {
                    System.out.printf("%d: %s\n", i, resSet.getString(i));
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
