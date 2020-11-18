package com.taosdata.jdbc.cases;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class FailOverTest {

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    @Test
    public void testFailOver() throws ClassNotFoundException {
        Class.forName("com.taosdata.jdbc.TSDBDriver");
        final String url = "jdbc:TAOS://:/?user=root&password=taosdata";

        while (true) {
            try (Connection conn = DriverManager.getConnection(url)) {
                Statement stmt = conn.createStatement();
                ResultSet resultSet = stmt.executeQuery("select server_status()");
                resultSet.next();
                int status = resultSet.getInt("server_status()");
                System.out.println(">>>>>>>>>" + sdf.format(new Date()) + " status : " + status);
                stmt.close();
                TimeUnit.SECONDS.sleep(5);
            } catch (SQLException | InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

}
