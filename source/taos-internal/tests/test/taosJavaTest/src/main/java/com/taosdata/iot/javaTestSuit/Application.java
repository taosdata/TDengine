//package com.taosdata.iot.javaTestSuit;
//
////import com.taosdata.iot.javaTestSuit.suit_01.AggregateTester;
//import com.taosdata.iot.javaTestSuit.suit_01.ImportTestTask;
//import com.taosdata.iot.javaTestSuit.suit_01.ImportTester;
//import com.taosdata.iot.javaTestSuit.suit_01.SelectTestTask;
//import com.taosdata.iot.javaTestSuit.utils.ConnectionFactory;
//import com.taosdata.iot.javaTestSuit.utils.ConnectionProperties;
//import org.apache.log4j.BasicConfigurator;
//import org.slf4j.LoggerFactory;
//
//import java.sql.Connection;
//import java.sql.SQLException;
//import java.util.Properties;
//
//public class Application {
//
//    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(Application.class);
//
//    public static void main(String[] args) {
//
//        BasicConfigurator.configure();
//
//        System.out.println("args format: host configDir threadNum replica");
//        if (logger.isDebugEnabled()) {
//            System.out.println("Logger.DEBUG is enabled.");
//        } else {
//            System.out.println("Logger.DEBUG is NOT enabled.");
//        }
//        logger.debug("Thread-{}, In Application.main(): ", Thread.currentThread().getName());
//
//        Application application = new Application();
//        ConnectionProperties properties = new ConnectionProperties();
//        int threadNum = 1;
//
////        if (args != null && args.length > 0) {
////            if (!Strings.isNullOrEmpty(args[0])) {
////                properties.put("HOST", args[0]);
////            }
////            if (!Strings.isNullOrEmpty(args[1])) {
////                properties.put("CONFIG_DIR", args[1]);
////            }
////            if (!Strings.isNullOrEmpty(args[2])) {
////                threadNum = Integer.getInteger(args[2]);
////            }
////
////            if (!Strings.isNullOrEmpty(args[0])) {
////                properties.put("replica", args[3]);
////            }
////        }
//
//        // multi-thread import test
////        ImportTester importTest = new ImportTester();
////        importTest.runMultiThreadTest(threadNum);
//
//        // sum test
////        AggregateTester aggregateTester = new AggregateTester();
////        aggregateTester.runTest(threadNum);
//
//        // select test
//        Thread thread = new Thread(new SelectTestTask());
//        thread.start();
//
//    }
//
//    public boolean runAllTests() {
//        boolean success = true;
//        int count = 0;
//        int passCount = 0;
//        int failCount = 0;
//        int threadNum = 1;
//
//        logger.info("Start running all tests:");
//        System.out.println("Start running all tests:\n");
//        ConnectionFactory connectionFactory = new ConnectionFactory();
//        Connection connection = connectionFactory.getConnection();
//
//        AggregateTester aggregateTester = new AggregateTester();
//        if (!aggregateTester.runTest(threadNum)) {
//            count++;
//            failCount++;
//            success = false;
//            System.out.println("Error: ComputeTest");
//            return success;
//        } else {
//            count++;
//            passCount++;
//            System.out.println("Success: ComputeTest");
//        }
//
//        // single thread test
//        ImportTester importTester = new ImportTester();
//            threadNum = 1;
//        if (!importTester.runMultiThreadTest(threadNum, new Properties(), ImportTestTask.class)) {
//            count++;
//            failCount++;
//            success = false;
//            System.out.println("Error: ImportTest");
//            return success;
//        } else {
//            count++;
//            passCount++;
//            System.out.println("Success: ImportTest");
//        }
//
//        try {
//            connection.close();
//        } catch (SQLException sqlException) {
//            sqlException.printStackTrace();
//            success = false;
//            System.out.println("Error: Can not close connection!");
//        }
//
//        // multithread test
//        threadNum = 3;
//        ImportTester importTest = new ImportTester();
//        count++;
//        if (importTest.runMultiThreadTest(threadNum, new Properties(), ImportTestTask.class)) {
//            passCount++;
//        } else {
//            failCount++;
//        }
//
//        System.out.printf("Total tests: %d\n", count);
//        System.out.printf("Passed tests: %d\n", passCount);
//        System.out.printf("Failed tests: %d\n", failCount);
//        return success;
//    }
//}
