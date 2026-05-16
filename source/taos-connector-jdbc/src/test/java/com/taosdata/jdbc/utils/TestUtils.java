package com.taosdata.jdbc.utils;

import org.junit.Assume;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;

public class TestUtils {
    private static final AtomicInteger counter = new AtomicInteger(0);
    public static String camelToSnake(Class<?> clazz) {
        String className = clazz.getSimpleName();
        String temp = className.replaceAll("(?<=[a-z])([A-Z])", "_$1").toLowerCase();
        return temp +  counter.incrementAndGet();
    }

    public static void waitTransactionDone(Connection connection) {
        while (true) {
            try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery("show transactions")){
                if (resultSet.next()) {
                    continue;
                }
                break;
            } catch (SQLException e) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                }
            }
        }
    }

    public static void runIn336(){
        String env = System.getenv("TD_3360_TEST");
        Assume.assumeTrue("true".equals(env));
    }

    public static void runInMain(){
        String env = System.getenv("TD_3360_TEST");
        Assume.assumeFalse("true".equals(env));
    }

    public static void runInEnterprise(){
        String env = System.getenv("TD_ENTERPRISE_TEST");
        Assume.assumeTrue("true".equals(env));
    }
}