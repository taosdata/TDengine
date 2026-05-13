package com.taos.example;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class Util {
    public static void printResult(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            String columnLabel = metaData.getColumnLabel(i);
            System.out.printf(" %s |", columnLabel);
        }
        System.out.println();
        System.out.println("-------------------------------------------------------------");
        while (resultSet.next()) {
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String value = resultSet.getString(i);
                System.out.printf("%s, ", value);
            }
            System.out.println();
        }
    }

}
