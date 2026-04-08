package com.taosdata.iot.javaTestSuit.suit_01;

import com.taosdata.jdbc.TSDBDriver;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class InsertTester extends TaosTester {

    public static void main(String[] args) {
        InsertTester insertTester = new InsertTester();

        // configuration properties
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CONFIG_DIR, "/etc/taos");

        // run tests
//        insertTester.runInsertPerfTestTask(properties);
        insertTester.runSimpleInsertTest(properties);
    }

    public void runInsertPerfTestTask(Properties properties) {

        // test params for db and tb
        int threadNum = 1;
        int replica = 1;
        int cache = 16384;
        int ablocks = 4000;
        int tables = 1000;
        int tableNum = 10;
        int rowSize = 9;
        int columns  = 2;
        int batchSize = 50;
        int batches = 20;

        // make runnables
        List<InsertPerfTestTask> tasks = new ArrayList<InsertPerfTestTask>(threadNum);
        for (int i = 0; i < threadNum; i++) {
            InsertPerfTestTask task = new InsertPerfTestTask();
            task.setReplica(replica);
            task.setCache(cache);
            task.setAblocks(ablocks);
            task.setTables(tables);
            task.setTableNum(tableNum);
            task.setRowSize(rowSize);
            task.setColumns(columns);
            task.setProperties(properties);
            task.setBatchSize(batchSize);
            task.setBatches(batches);
            tasks.add(task);
        }

        runMultiThreadTest(tasks);
    }

    public void runSimpleInsertTest(Properties properties) {
        int threadNum = 1;
        List<SimpleInsertTestTask> tasks = new ArrayList<>(threadNum);
        for (int i = 0; i < threadNum; i++) {
            SimpleInsertTestTask task = new SimpleInsertTestTask();
            tasks.add(task);
        }

        runMultiThreadTest(tasks);
    }
}
