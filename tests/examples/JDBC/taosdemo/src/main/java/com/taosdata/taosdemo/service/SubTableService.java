package com.taosdata.taosdemo.service;

import com.taosdata.taosdemo.components.JdbcTaosdemoConfig;
import com.taosdata.taosdemo.dao.SubTableMapper;
import com.taosdata.taosdemo.dao.SubTableMapperImpl;
import com.taosdata.taosdemo.domain.SubTableMeta;
import com.taosdata.taosdemo.domain.SubTableValue;
import com.taosdata.taosdemo.domain.SuperTableMeta;
import com.taosdata.taosdemo.service.data.SubTableMetaGenerator;
import com.taosdata.taosdemo.service.data.SubTableValueGenerator;
import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SubTableService extends AbstractService {

    private SubTableMapper mapper;
    private static final Logger logger = Logger.getLogger(SubTableService.class);

    public SubTableService(DataSource datasource) {
        this.mapper = new SubTableMapperImpl(datasource);
    }

    public void createSubTable(SuperTableMeta superTableMeta, long numOfTables, String prefixOfTable, int numOfThreadsForCreate) {
        ExecutorService executor = Executors.newFixedThreadPool(numOfThreadsForCreate);
        for (long i = 0; i < numOfTables; i++) {
            long tableIndex = i;
            executor.execute(() -> createSubTable(superTableMeta, prefixOfTable + (tableIndex + 1)));
        }
        executor.shutdown();
    }

    public void createSubTable(SuperTableMeta superTableMeta, String tableName) {
        // 构造数据
        SubTableMeta meta = SubTableMetaGenerator.generate(superTableMeta, tableName);
        createSubTable(meta);
    }

    // 创建一张子表，可以指定database，supertable，tablename，tag值
    public void createSubTable(SubTableMeta subTableMeta) {
        mapper.createUsingSuperTable(subTableMeta);
    }

    /*************************************************************************************************************************/
    // 插入：多线程，多表
    public int insert(List<SubTableValue> subTableValues, int threadSize, int frequency) {
        ExecutorService executor = Executors.newFixedThreadPool(threadSize);
        Future<Integer> future = executor.submit(() -> insert(subTableValues));
        executor.shutdown();
        //TODO：frequency
        return getAffectRows(future);
    }

    // 插入：单表，insert into xxx values(),()...
    public int insert(SubTableValue subTableValue) {
        return mapper.insertOneTableMultiValues(subTableValue);
    }

    // 插入: 多表，insert into xxx values(),()... xxx values(),()...
    public int insert(List<SubTableValue> subTableValues) {
        return mapper.insertMultiTableMultiValues(subTableValues);
    }

    // 插入：单表，自动建表, insert into xxx using xxx tags(...) values(),()...
    public int insertAutoCreateTable(SubTableValue subTableValue) {
        return mapper.insertOneTableMultiValuesUsingSuperTable(subTableValue);
    }

    // 插入：多表，自动建表, insert into xxx using XXX tags(...) values(),()... xxx using XXX tags(...) values(),()...
    public int insertAutoCreateTable(List<SubTableValue> subTableValues) {
        return mapper.insertMultiTableMultiValuesUsingSuperTable(subTableValues);
    }

    public int insertMultiThreads(SuperTableMeta superTableMeta, int threadSize, long tableSize, long startTime, long gap, JdbcTaosdemoConfig config) {
        long start = System.currentTimeMillis();

        List<FutureTask> taskList = new ArrayList<>();
        List<Thread> threads = IntStream.range(0, threadSize)
                .mapToObj(i -> {
                    long startInd = i * gap;
                    long endInd = (i + 1) * gap < tableSize ? (i + 1) * gap : tableSize;
                    FutureTask<Integer> task = new FutureTask<>(
                            new InsertTask(superTableMeta,
                                    startInd, endInd,
                                    startTime, config.timeGap,
                                    config.numOfRowsPerTable, config.numOfTablesPerSQL, config.numOfValuesPerSQL,
                                    config.order, config.rate, config.range,
                                    config.prefixOfTable, config.autoCreateTable)
                    );
                    taskList.add(task);
                    return new Thread(task, "InsertThread-" + i);
                }).collect(Collectors.toList());

        threads.stream().forEach(Thread::start);
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        int affectedRows = 0;
        for (FutureTask<Integer> task : taskList) {
            try {
                affectedRows += task.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

        long end = System.currentTimeMillis();
        logger.info("insert " + affectedRows + " rows, time cost: " + (end - start) + " ms");
        return affectedRows;
    }

    private class InsertTask implements Callable<Integer> {

        private final long startTableInd; // included
        private final long endTableInd;   // excluded
        private final long startTime;
        private final long timeGap;
        private final long numOfRowsPerTable;
        private long numOfTablesPerSQL;
        private long numOfValuesPerSQL;
        private final SuperTableMeta superTableMeta;
        private final int order;
        private final int rate;
        private final long range;
        private final String prefixOfTable;
        private final boolean autoCreateTable;

        public InsertTask(SuperTableMeta superTableMeta, long startTableInd, long endTableInd,
                          long startTime, long timeGap,
                          long numOfRowsPerTable, long numOfTablesPerSQL, long numOfValuesPerSQL,
                          int order, int rate, long range,
                          String prefixOfTable, boolean autoCreateTable) {
            this.superTableMeta = superTableMeta;
            this.startTableInd = startTableInd;
            this.endTableInd = endTableInd;
            this.startTime = startTime;
            this.timeGap = timeGap;
            this.numOfRowsPerTable = numOfRowsPerTable;
            this.numOfTablesPerSQL = numOfTablesPerSQL;
            this.numOfValuesPerSQL = numOfValuesPerSQL;
            this.order = order;
            this.rate = rate;
            this.range = range;
            this.prefixOfTable = prefixOfTable;
            this.autoCreateTable = autoCreateTable;
        }


        @Override
        public Integer call() {

            long numOfTables = endTableInd - startTableInd;
            if (numOfRowsPerTable < numOfValuesPerSQL)
                numOfValuesPerSQL = (int) numOfRowsPerTable;
            if (numOfTables < numOfTablesPerSQL)
                numOfTablesPerSQL = (int) numOfTables;

            int affectRows = 0;
            // row
            for (long rowCnt = 0; rowCnt < numOfRowsPerTable; ) {
                long rowSize = numOfValuesPerSQL;
                if (rowCnt + rowSize > numOfRowsPerTable) {
                    rowSize = numOfRowsPerTable - rowCnt;
                }
                //table
                for (long tableCnt = startTableInd; tableCnt < endTableInd; ) {
                    long tableSize = numOfTablesPerSQL;
                    if (tableCnt + tableSize > endTableInd) {
                        tableSize = endTableInd - tableCnt;
                    }
                    long startTime = this.startTime + rowCnt * timeGap;
//                    System.out.println(Thread.currentThread().getName() + " >>> " + "rowCnt: " + rowCnt + ", rowSize: " + rowSize + ", " + "tableCnt: " + tableCnt + ",tableSize: " + tableSize + ", " + "startTime: " + startTime + ",timeGap: " + timeGap + "");
                    /***********************************************/
                    // 生成数据
                    List<SubTableValue> data = SubTableValueGenerator.generate(superTableMeta, prefixOfTable, tableCnt, tableSize, rowSize, startTime, timeGap);
                    // 乱序
                    if (order != 0)
                        SubTableValueGenerator.disrupt(data, rate, range);
                    // insert
                    if (autoCreateTable)
                        affectRows += insertAutoCreateTable(data);
                    else
                        affectRows += insert(data);
                    /***********************************************/
                    tableCnt += tableSize;
                }
                rowCnt += rowSize;
            }

            return affectRows;
        }
    }


}
