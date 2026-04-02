package com.taosdata.iot.javaTestSuit.suit_01;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @author Jiangyi Hou
 * @since 19-5-7
 */
public class TimezoneTester {
    public static void main(String[] args) {
        TimezoneTester tester = new TimezoneTester();
        tester.getConnection();
    }

    public void getConnection() {
        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");
            String url = "jdbc:TSDB://192.168.0.1:0/?user=root&password=taosdata&timezone=Asia/Shanghai";
            Connection connection = DriverManager.getConnection(url);
            Statement stmt = connection.createStatement();
            ResultSet resultSet = stmt.executeQuery("show databases");
            resultSet.next();
            resultSet.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
