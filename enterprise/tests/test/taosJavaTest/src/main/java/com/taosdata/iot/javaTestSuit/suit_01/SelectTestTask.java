package com.taosdata.iot.javaTestSuit.suit_01;

import com.taosdata.iot.javaTestSuit.utils.ConnectionFactory;
import com.taosdata.iot.javaTestSuit.utils.SqlGenerator;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

public class SelectTestTask extends TestTask {

    @Override
    public void run() {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        Connection connection = connectionFactory.getConnection();
        try {

            Statement stmt = connection.createStatement();

            String db = "db";
            String createDb = SqlGenerator.getCreateDbSql(db);
            stmt.executeUpdate(createDb);
            stmt.executeUpdate("use " + db);
            String[] schema = new String[] {"ts", "timestamp", "c1", "int"};
            
            for (int i = 0; i < 10; i++) {
                stmt.executeUpdate(SqlGenerator.getCreateTableSql("tb" + i, schema));
            }
            
            ResultSet resSet = stmt.executeQuery("show tables");
            ResultSetMetaData metaData = resSet.getMetaData();
            
            while (resSet.next()) {
                StringBuffer strBuff = new StringBuffer();
                for (int col = 1; col <= metaData.getColumnCount(); col++) {
                    strBuff.append(metaData.getColumnName(col) + "=" + resSet.getObject(col) + " ");
                }
                System.out.println(strBuff);
            }
            resSet.close();
            stmt.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
    }
}
