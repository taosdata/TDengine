package com.taosdata.iot.javaTestSuit.utils;

import com.google.common.base.Strings;

import java.math.BigDecimal;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.Scanner;

/**
 *
 * Utility class for executing select queries and printing the results in console
 * Usage: java -cp taosJavaTest-1.0-SNAPSHOT-jar-with-dependencies.jar com.taosdata.iot.javaTestSuit.utils.SqlExecutor "use db0" "insert into tb values (7300000, 3)"
 *
 *
 */

public class SqlExecutor {

    private Connection connection;
    static String useDb;

    public SqlExecutor() {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        this.connection = connectionFactory.getConnection();
    }

    public SqlExecutor(String host) {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        this.connection = connectionFactory.getConnection(host, new Properties());
    }

    public static void main(String[] args) {

        String host = "";

        if (args.length >= 2) {
            if (args[0] != null && "-h".equals(args[0])) {
                host = args[1];
            }
        }
        SqlExecutor sqlExecutor;
        if (host.length() < 1) {
            sqlExecutor = new SqlExecutor();
        } else {
            sqlExecutor = new SqlExecutor(host);
        }
        Scanner scanner = new Scanner(System.in);

        String sql = "";
        System.out.println("Please select database first:");
        sql = scanner.nextLine().trim();
        while (!sql.toLowerCase().startsWith("use ")) {
            System.out.println("Please select database first:");
            sql = scanner.nextLine().trim();
        }
        sqlExecutor.executeUpdate(sql);

        while (true) {
            sql = scanner.nextLine().trim().toLowerCase();

            if ("quit".equals(sql)) {
                break;
            } else if (sql.startsWith("select") || sql.startsWith("show") || sql.startsWith("describe")) {
                sqlExecutor.executeSql(sql);
            } else {
                sqlExecutor.executeUpdate(sql);
            }
        }
        return;
    }


    private void executeSql(String sql) {

        Statement stmt;
        ResultSet resSet = null;
        try {
            stmt = connection.createStatement();
            resSet = stmt.executeQuery(sql);
            if (resSet == null) {
                System.out.println(sql + " failed");
                System.exit(4);
            }

            ResultSetMetaData metaData = resSet.getMetaData();
            int lineLength = 0;
            int cellLength = 0;
            String display = "";
            for (int col = 1; col <= metaData.getColumnCount(); ++col) {
                cellLength = metaData.getColumnDisplaySize(col);
                if ("TIMESTAMP".equalsIgnoreCase(metaData.getColumnTypeName(col))) {
                    cellLength = 24;
                }
                System.out.printf("%s|", Strings.padEnd(metaData.getColumnName(col),
                        cellLength, ' '));
                lineLength += metaData.getColumnDisplaySize(col) + 1;
            }
            System.out.printf("\n");
            System.out.printf("%s\n", Strings.padEnd("", lineLength, '='));

            SimpleDateFormat sdf = new SimpleDateFormat("yy-MM-dd HH:mm:ss.SSS");
            while (resSet.next()) {
                StringBuffer strBuff = new StringBuffer();
                for (int col = 1; col <= metaData.getColumnCount(); col++) {
                    display = String.valueOf(resSet.getObject(col));
                    cellLength = metaData.getColumnDisplaySize(col);
                    if ("TIMESTAMP".equalsIgnoreCase(metaData.getColumnTypeName(col))) {
                        display = sdf.format(new Timestamp(Long.valueOf(display)));
                        cellLength = 24;
                    }
                    System.out.printf("%s|", Strings.padEnd(display,
                            cellLength, ' '));
                }
                System.out.printf("\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.printf("Failed to execute query: %s\n", sql);
        } finally {
            try {
                resSet.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void executeUpdate(String sql) {

        System.out.printf("sql: %s\n", sql);
        Statement stmt;
        try {
            stmt = connection.createStatement();
            long start = System.nanoTime();
            int res = stmt.executeUpdate(sql);
            long end = System.nanoTime();
            BigDecimal time = BigDecimal.valueOf(end - start).divide(BigDecimal.valueOf(1e9));
            System.out.printf("Query OK, (%d) row(s) in set (%fs)\n", res, time);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.printf("Failed to execute query: %s\n", sql);
        }
    }
}
