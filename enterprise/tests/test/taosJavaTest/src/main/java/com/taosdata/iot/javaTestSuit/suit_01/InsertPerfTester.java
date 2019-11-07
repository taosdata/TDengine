package com.taosdata.iot.javaTestSuit.suit_01;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class InsertPerfTester extends TaosTester {

    private int threadNum = 1;
    private int replica = 1;
    private int cache = 16384;
    private int ablocks = 4000;
    private int tables = 1000;
    private int tableNum = 1000;
    private int rowSize = 10;
    private int columns  = 2;
    private int batchSize = 200;
    private int batches = 50;
    private int insertMethod = 1;
    
    public static void main(String[] args) {

        InsertPerfTester tester = new InsertPerfTester();
//        tester.getArgs();

        List<InsertPerfTestTask> tasks = new ArrayList<>(tester.threadNum);
        for (int i = 0; i < tester.threadNum; i++) {
            InsertPerfTestTask task = new InsertPerfTestTask();
            task.setReplica(tester.replica);
            task.setCache(tester.cache);
            task.setAblocks(tester.ablocks);
            task.setTables(tester.tables);
            task.setTableNum(tester.tableNum);
            task.setRowSize(tester.rowSize);
            task.setColumns(tester.columns);
            task.setBatchSize(tester.batchSize);
            task.setBatches(tester.batches);
            task.setInsertMethod(tester.insertMethod);
            tasks.add(task);
        }
        runMultiThreadTest(tasks);

    }

    private void getArgs() {

        System.out.println("=====Insert Performance Test=====\nPlease enter the test parameters:");
        Scanner scanner = new Scanner(System.in);
        Integer arg = null;
        System.out.printf("Please enter threads: (default %d)\n", this.threadNum);

        if ((arg = scanner.nextInt())!= null) {
            this.threadNum = arg;
        }
        System.out.printf("Please enter database replica: (default %d)\n", this.replica);
        if ((arg = scanner.nextInt())!= null) {
            this.replica = arg;
        }
        System.out.printf("Please enter cache size in bytes: (default %d)\n", this.cache);
        if ((arg = scanner.nextInt())!= null) {
            this.cache = arg;
        }
        System.out.printf("Please enter numOfCacheBlocks: (default %d)\n", this.ablocks);
        if ((arg = scanner.nextInt())!= null) {
            this.ablocks = arg;
        }
        System.out.printf("Please enter sessionsPerVnode: (default %d)\n", this.tables);
        if ((arg = scanner.nextInt())!= null) {
            this.tables = arg;
        }
        System.out.printf("Please enter number of tables each thread should create: (default %d)\n", this.tableNum);
        if ((arg = scanner.nextInt())!= null) {
            this.tableNum = arg;
        }
        System.out.printf("Please enter table row size in bytes: (default %d)\n", this.rowSize);
        if ((arg = scanner.nextInt())!= null) {
            this.rowSize = arg;
        }
        System.out.printf("Please enter number of columns in table: (default %d)\n", this.columns);
        if ((arg = scanner.nextInt())!= null) {
            this.columns = arg;
        }
        System.out.printf("Please enter insert batch size: (default %d)\n", this.batchSize);
        if ((arg = scanner.nextInt())!= null) {
            this.batchSize = arg;
        }
        System.out.printf("Please enter number of batches: (default %d)\n", this.batches);
        if ((arg = scanner.nextInt())!= null) {
            this.batches = arg;
        }
        System.out.printf("Please enter insert method: \n(option: \n\t1.sameTableBatchInsert \n\t2.oneByOneInsert \n\t3.blendedInsert\n\tdefault %d)\n", this.insertMethod);
        if ((arg = scanner.nextInt())!= null) {
            this.insertMethod = arg;
        }
    }
}
