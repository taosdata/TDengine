package com.taosdata.iot.javaTestSuit.suit_01;

import com.taosdata.iot.javaTestSuit.Exceptions.TestFailureException;
import com.taosdata.iot.javaTestSuit.utils.ConnectionFactory;

import java.sql.Connection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class TaosTester {

//    public boolean runMultiThreadTest(int threadNum, Properties properties, Class taskClass) {
    public static <T extends Runnable> boolean runMultiThreadTest(List<T> tasks) {

        boolean success = true;
        int threadNum = tasks.size();
        ExecutorService executorService = Executors.newFixedThreadPool(threadNum);
        try {
            for (int i = 0; i < threadNum; i++) {
                executorService.execute(tasks.get(i));
            }
        } catch (TestFailureException testFailureException) {
            testFailureException.printStackTrace();
            success = false;
        } catch (Exception exception) {
            exception.printStackTrace();
            success = false;
        } catch (Error error) {
            error.printStackTrace();
            success = false;
        }

        executorService.shutdown();
        while (!executorService.isTerminated()) {
            // wait till all threads complete their tasks
        }

        return success;
    }

    public Connection getConnection(Properties properties) {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        Connection connection = connectionFactory.getConnection(properties);
        return connection;
    }
}
