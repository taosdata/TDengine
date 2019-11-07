package com.taosdata.iot.javaTestSuit.suit_01;

import com.taosdata.iot.javaTestSuit.utils.ConnectionFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class NestedQueryTester extends TaosTester {

    ConnectionFactory connectionFactory = new ConnectionFactory();

    public static void main(String[] args) {
        NestedQueryTester innerQueryTester = new NestedQueryTester();
        innerQueryTester.test();

    }

    private void test() {
        Properties properties = new Properties();
        Connection connection = connectionFactory.getConnection(properties);
        try {

            long ts = 15000000000l;
            Statement stmt = connection.createStatement();
            stmt.executeUpdate("drop database if exists nestedQueryTestDb");
            stmt.executeUpdate("create database nestedQueryTestDb");
            stmt.executeUpdate("use nestedQueryTestDb");
            stmt.executeUpdate("create table tb (ts timestamp, c1 int)");
            for (int i = 0; i < 100; i++) {
                ts = ts + 1000;
                StringBuilder sql = new StringBuilder("insert into tb values (");
                sql.append(ts).append(", ").append(i).append(")");
                stmt.executeUpdate(sql.toString());
            }

            ResultSet resSet = stmt.executeQuery("select * from tb");
            ResultSet resSet1;

            //  case 1: same stmt, executeQuery before closing current resSet
            try {
                resSet1 = stmt.executeQuery("select * from tb");
            } catch (RuntimeException re) {
                if (!re.getMessage().equals("Connection already has an open result set")) {
                    re.printStackTrace();
                    return;
                } else {
                    System.out.printf("case 1 passed: same statement calling executeQuery() before closing current resultSet is successfully denied\n");
                }
            }

            // case 2: same stmt, executeUpdate before closing current resSet
            try {
                stmt.executeUpdate("insert into tb values (now, -1)");
            } catch (RuntimeException re) {
                if (!re.getMessage().equals("Connection already has an open result set")) {
                    re.printStackTrace();
                    return;
                } else {
                    System.out.printf("case 2 passed: same statement calling executeUpdate() before closing current resultSet is successfully denied\n");
                }
            }

            // case 3: different stmt but in same connection, executeQuery before closing current resSet
            try {
                Statement stmt1 = connection.createStatement();
                stmt1.executeQuery("select * from tb");
            } catch (RuntimeException re) {
                if (!re.getMessage().equals("Connection already has an open result set")) {
                    re.printStackTrace();
                    return;
                } else {
                    System.out.printf("case 3 passed: different statement created by same connection calling executeQuery() before closing current resultSet is successfully denied\n");
                }
            }


            // case 4: different stmt but in same connection, executeUpdate before closing current resSet
            try {
                Statement stmt1 = connection.createStatement();
                stmt1.executeUpdate("insert into tb values (now, -1)");
            } catch (RuntimeException re) {
                if (!re.getMessage().equals("Connection already has an open result set")) {
                    re.printStackTrace();
                    return;
                } else {
                    System.out.printf("case 4 passed: different statement created by same connection calling executeUpdate() before closing current resultSet is successfully denied\n");
                }
            }

            // case 5: different connection, executeQuery and executeUpdate before closing the other connection's resSet
            Connection connection1 = connectionFactory.getConnection(properties);
            Statement stmt1 = connection1.createStatement();
            stmt1.executeUpdate("use nestedQueryTestDb");
            stmt1.executeQuery("select * from tb");
            System.out.printf("case 5 passed: statement in different connection calling executeQuery and executeUpdate before closing the other connection's resultSet succeeded");


            int c = 0;
            int c1 = 0;
//            while (resSet.next()) {
//                System.out.printf("A\t%d\n", c);
//                if (c > 10) {
//                    while (resSet.next()) {
//                        System.out.printf("B\t%d\n", c1);
//                        c1++;
//                    }
//                }
//                c++;
//            }

            resSet.close();
            stmt.executeUpdate("drop database nestedQueryTestDb");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
