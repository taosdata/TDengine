package com.taos.example.highvolume;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;


public class FastWriteExample {
    static final Logger logger = LoggerFactory.getLogger(FastWriteExample.class);
    static ThreadPoolExecutor writerThreads;
    static ThreadPoolExecutor producerThreads;
    static final ThreadPoolExecutor statThread = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
    private static final List<Stoppable> allTasks = new ArrayList<>();

    private static int readThreadCount = 5;
    private static int writeThreadPerReadThread = 5;
    private static int batchSizeByRow = 1000;
    private static int cacheSizeByRow = 10000;
    private static int subTableNum = 1000000;
    private static int rowsPerSubTable = 100;
    private static String dbName = "test";


    public static void forceStopAll() {
        logger.info("shutting down");

        for (Stoppable task : allTasks) {
            task.stop();
        }

        if (producerThreads != null) {
            producerThreads.shutdown();
        }

        if (writerThreads != null) {
            writerThreads.shutdown();
        }

        statThread.shutdown();
    }

    private static void createSubTables(){
        writerThreads = (ThreadPoolExecutor) Executors.newFixedThreadPool(readThreadCount, getNamedThreadFactory("FW-CreateSubTable-thread-"));

        int range = (subTableNum + readThreadCount - 1) / readThreadCount;

        for (int i = 0; i < readThreadCount; i++) {
            int startIndex = i * range;
            int endIndex;
            if (i == readThreadCount - 1) {
                endIndex = subTableNum - 1;
            } else {
                endIndex = startIndex + range - 1;
            }

            logger.debug("create sub table task {} {} {}", i, startIndex, endIndex);

            CreateSubTableTask createSubTableTask = new CreateSubTableTask(i,
                    startIndex,
                    endIndex,
                    dbName);
            writerThreads.submit(createSubTableTask);
        }

        logger.info("create sub table task started.");

        while (writerThreads.getActiveCount() != 0) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
        logger.info("create sub table task finished.");

    }

    public static void startStatTask() throws SQLException {
        StatTask statTask = new StatTask(dbName, subTableNum);
        allTasks.add(statTask);
        statThread.submit(statTask);
    }
    public static ThreadFactory getNamedThreadFactory(String namePrefix) {
        return new ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger(1);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, namePrefix + threadNumber.getAndIncrement());
            }
        };
    }

    private static void invokeKafkaDemo() throws SQLException {
        producerThreads = (ThreadPoolExecutor) Executors.newFixedThreadPool(readThreadCount, getNamedThreadFactory("FW-kafka-producer-thread-"));
        writerThreads = (ThreadPoolExecutor) Executors.newFixedThreadPool(readThreadCount, getNamedThreadFactory("FW-kafka-consumer-thread-"));

        int range = (subTableNum + readThreadCount - 1) / readThreadCount;

        for (int i = 0; i < readThreadCount; i++) {
            int startIndex = i * range;
            int endIndex;
            if (i == readThreadCount - 1) {
                endIndex = subTableNum - 1;
            } else {
                endIndex = startIndex + range - 1;
            }

            ProducerTask producerTask = new ProducerTask(i,
                    rowsPerSubTable,
                    startIndex,
                    endIndex);
            allTasks.add(producerTask);
            producerThreads.submit(producerTask);

            ConsumerTask consumerTask = new ConsumerTask(i,
                    writeThreadPerReadThread,
                    batchSizeByRow,
                    cacheSizeByRow,
                    dbName);
            allTasks.add(consumerTask);
            writerThreads.submit(consumerTask);
        }

        startStatTask();
        Runtime.getRuntime().addShutdownHook(new Thread(FastWriteExample::forceStopAll));

        while (writerThreads.getActiveCount() != 0) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }
    private static void invokeMockDataDemo() throws SQLException {
        ThreadFactory namedThreadFactory = new ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger(1);
            private final String namePrefix = "FW-work-thread-";

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, namePrefix + threadNumber.getAndIncrement());
            }
        };

        writerThreads = (ThreadPoolExecutor) Executors.newFixedThreadPool(readThreadCount, namedThreadFactory);

        int range = (subTableNum + readThreadCount - 1) / readThreadCount;

        for (int i = 0; i < readThreadCount; i++) {
            int startIndex = i * range;
            int endIndex;
            if (i == readThreadCount - 1) {
                endIndex = subTableNum - 1;
            } else {
                endIndex = startIndex + range - 1;
            }

            WorkTask task = new WorkTask(i,
                    writeThreadPerReadThread,
                    batchSizeByRow,
                    cacheSizeByRow,
                    rowsPerSubTable,
                    startIndex,
                    endIndex,
                    dbName);
            allTasks.add(task);
            writerThreads.submit(task);
        }

        startStatTask();
        Runtime.getRuntime().addShutdownHook(new Thread(FastWriteExample::forceStopAll));

        while (writerThreads.getActiveCount() != 0) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // print help
    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("java -jar highVolume.jar", options);
        System.out.println();
    }

    public static void main(String[] args) throws SQLException, InterruptedException {
        Options options = new Options();

        Option readThdcountOption = new Option("r", "readThreadCount", true, "Specify the readThreadCount, default is 5");
        readThdcountOption.setRequired(false);
        options.addOption(readThdcountOption);

        Option writeThdcountOption = new Option("w", "writeThreadPerReadThread", true, "Specify the writeThreadPerReadThread, default is 5");
        writeThdcountOption.setRequired(false);
        options.addOption(writeThdcountOption);

        Option batchSizeOption = new Option("b", "batchSizeByRow", true, "Specify the batchSizeByRow, default is 1000");
        batchSizeOption.setRequired(false);
        options.addOption(batchSizeOption);

        Option cacheSizeOption = new Option("c", "cacheSizeByRow", true, "Specify the cacheSizeByRow, default is 10000");
        cacheSizeOption.setRequired(false);
        options.addOption(cacheSizeOption);

        Option subTablesOption = new Option("s", "subTableNum", true, "Specify the subTableNum, default is 1000000");
        subTablesOption.setRequired(false);
        options.addOption(subTablesOption);

        Option rowsPerTableOption = new Option("R", "rowsPerSubTable", true, "Specify the rowsPerSubTable, default is 100");
        rowsPerTableOption.setRequired(false);
        options.addOption(rowsPerTableOption);

        Option dbNameOption = new Option("d", "dbName", true, "Specify the database name, default is test");
        dbNameOption.setRequired(false);
        options.addOption(dbNameOption);

        Option kafkaOption = new Option("K", "useKafka", false, "use kafka demo to test");
        kafkaOption.setRequired(false);
        options.addOption(kafkaOption);


        Option helpOption = new Option(null, "help", false, "print help information");
        helpOption.setRequired(false);
        options.addOption(helpOption);

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            printHelp(options);
            System.exit(1);
            return;
        }

        if (cmd.hasOption("help")) {
            printHelp(options);
            return;
        }

        if (cmd.getOptionValue("readThreadCount") != null) {
            readThreadCount = Integer.parseInt(cmd.getOptionValue("readThreadCount"));
            if (readThreadCount <= 0){
                logger.error("readThreadCount must be greater than 0");
                return;
            }
        }

        if (cmd.getOptionValue("writeThreadPerReadThread") != null) {
            writeThreadPerReadThread = Integer.parseInt(cmd.getOptionValue("writeThreadPerReadThread"));
            if (writeThreadPerReadThread <= 0){
                logger.error("writeThreadPerReadThread must be greater than 0");
                return;
            }
        }

        if (cmd.getOptionValue("batchSizeByRow") != null) {
            batchSizeByRow = Integer.parseInt(cmd.getOptionValue("batchSizeByRow"));
            if (batchSizeByRow <= 0){
                logger.error("batchSizeByRow must be greater than 0");
                return;
            }
        }

        if (cmd.getOptionValue("cacheSizeByRow") != null) {
            cacheSizeByRow = Integer.parseInt(cmd.getOptionValue("cacheSizeByRow"));
            if (cacheSizeByRow <= 0){
                logger.error("cacheSizeByRow must be greater than 0");
                return;
            }
        }

        if (cmd.getOptionValue("subTableNum") != null) {
            subTableNum = Integer.parseInt(cmd.getOptionValue("subTableNum"));
            if (subTableNum <= 0){
                logger.error("subTableNum must be greater than 0");
                return;
            }
        }

        if (cmd.getOptionValue("rowsPerSubTable") != null) {
            rowsPerSubTable = Integer.parseInt(cmd.getOptionValue("rowsPerSubTable"));
            if (rowsPerSubTable <= 0){
                logger.error("rowsPerSubTable must be greater than 0");
                return;
            }
        }

        if (cmd.getOptionValue("dbName") != null) {
            dbName = cmd.getOptionValue("dbName");
        }

        logger.info("readThreadCount={}, writeThreadPerReadThread={} batchSizeByRow={} cacheSizeByRow={}, subTableNum={}, rowsPerSubTable={}",
                readThreadCount, writeThreadPerReadThread, batchSizeByRow, cacheSizeByRow, subTableNum, rowsPerSubTable);

        logger.info("create database begin.");
        Util.prepareDatabase(dbName);

        logger.info("create database end.");

        logger.info("create sub tables start.");
        createSubTables();
        logger.info("create sub tables end.");


        if (cmd.hasOption("K")) {
            Util.createKafkaTopic();
            // use kafka demo
            invokeKafkaDemo();

        } else {
            // use mock data source demo
            invokeMockDataDemo();
        }

    }
}