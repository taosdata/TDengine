package com.taosdata.taosdemo.components;

import com.taosdata.taosdemo.domain.FieldMeta;
import com.taosdata.taosdemo.domain.SuperTableMeta;
import com.taosdata.taosdemo.domain.TagMeta;
import com.taosdata.taosdemo.service.DatabaseService;
import com.taosdata.taosdemo.service.InsertTask;
import com.taosdata.taosdemo.service.SubTableService;
import com.taosdata.taosdemo.service.SuperTableService;
import com.taosdata.taosdemo.service.data.SuperTableMetaGenerator;
import com.taosdata.taosdemo.utils.JdbcTaosdemoConfig;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


@Component
public class TaosDemoCommandLineRunner implements CommandLineRunner {

    private static Logger logger = Logger.getLogger(TaosDemoCommandLineRunner.class);
    private DatabaseService databaseService;
    private SuperTableService superTableService;
    private DataSource dataSource;

    private SuperTableMeta superTableMeta;

    @Override
    public void run(String... args) throws Exception {
        // 读配置参数
        JdbcTaosdemoConfig config = new JdbcTaosdemoConfig(args);
        boolean isHelp = Arrays.asList(args).contains("--help");
        if (isHelp || config.host == null || config.host.isEmpty()) {
            JdbcTaosdemoConfig.printHelp();
            System.exit(0);
        }

        dataSource = DataSourceFactory.getInstance(config.host, config.port, config.user, config.password);
        databaseService = new DatabaseService(dataSource);
        superTableService = new SuperTableService(dataSource);

        // 创建数据库
//        createDatabaseTask(config);
        // 超级表的meta
        superTableMeta = buildSuperTableMeta(config);
        // 建表
//        createTableTask(config);
        // 插入
        insertTask(config);
        // 删除表
        if (config.dropTable) {
            superTableService.drop(config.database, config.superTable);
        }
        System.exit(0);
    }

    private void createDatabaseTask(JdbcTaosdemoConfig config) {
        long start = System.currentTimeMillis();
        Map<String, String> databaseParam = new HashMap<>();
        databaseParam.put("database", config.database);
        databaseParam.put("keep", Integer.toString(config.keep));
        databaseParam.put("days", Integer.toString(config.days));
        databaseParam.put("replica", Integer.toString(config.replica));
        //TODO: other database parameters
        databaseService.dropDatabase(config.database);
        databaseService.createDatabase(databaseParam);
        databaseService.useDatabase(config.database);
        long end = System.currentTimeMillis();
        logger.info(">>> create database time cost : " + (end - start) + " ms.");
    }

    // 建超级表，三种方式：1. 指定SQL，2. 指定field和tags的个数，3. 默认
    private void createTableTask(JdbcTaosdemoConfig config) {
        long start = System.currentTimeMillis();
        if (config.doCreateTable) {
            superTableService.create(superTableMeta);
            if (config.autoCreateTable)
                return;
            // 批量建子表
//            subTableService.createSubTable(superTableMeta, config.numOfTables, config.prefixOfTable, config.numOfThreadsForCreate);
        }
        long end = System.currentTimeMillis();
        logger.info(">>> create table time cost : " + (end - start) + " ms.");
    }

    private long getProperStartTime(JdbcTaosdemoConfig config) {
        Instant now = Instant.now();
        long earliest = now.minus(Duration.ofDays(config.keep)).toEpochMilli();
        long startTime = config.startTime;
        if (startTime == 0 || startTime < earliest) {
            startTime = earliest;
        }
        return startTime;
    }

    private void insertTask(JdbcTaosdemoConfig config) {
        long tableSize = config.numOfTables;
        int threadSize = config.numOfThreadsForInsert;
        long startTime = getProperStartTime(config);

        if (tableSize < threadSize)
            threadSize = (int) tableSize;
        long gap = (long) Math.ceil((0.0d + tableSize) / threadSize);

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
                                    config.prefixOfTable, config.autoCreateTable, dataSource)
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
//        long numOfTables = config.numOfTables;
//        int numOfTablesPerSQL = config.numOfTablesPerSQL;
//        long numOfRowsPerTable = config.numOfRowsPerTable;
//        int numOfValuesPerSQL = config.numOfValuesPerSQL;

//        long start = System.currentTimeMillis();
//        long affectRows = 0;
//        long end = System.currentTimeMillis();
//        logger.info(">>> insert " + affectRows + " rows with time cost: " + (end - start) + "ms");
    }

    private SuperTableMeta buildSuperTableMeta(JdbcTaosdemoConfig config) {
        SuperTableMeta tableMeta;
        // create super table
        if (config.superTableSQL != null) {
            // use a sql to create super table
            tableMeta = SuperTableMetaGenerator.generate(config.superTableSQL);
            if (config.database != null && !config.database.isEmpty())
                tableMeta.setDatabase(config.database);
        } else if (config.numOfFields == 0) {
            // default sql = "create table test.weather (ts timestamp, temperature float, humidity int) tags(location nchar(64), groupId int)";
            SuperTableMeta superTableMeta = new SuperTableMeta();
            superTableMeta.setDatabase(config.database);
            superTableMeta.setName(config.superTable);
            List<FieldMeta> fields = new ArrayList<>();
            fields.add(new FieldMeta("ts", "timestamp"));
            fields.add(new FieldMeta("temperature", "float"));
            fields.add(new FieldMeta("humidity", "int"));
            superTableMeta.setFields(fields);
            List<TagMeta> tags = new ArrayList<>();
            tags.add(new TagMeta("location", "nchar(64)"));
            tags.add(new TagMeta("groupId", "int"));
            superTableMeta.setTags(tags);
            return superTableMeta;
        } else {
            // create super table with specified field size and tag size
            tableMeta = SuperTableMetaGenerator.generate(config.database, config.superTable, config.numOfFields, config.prefixOfFields, config.numOfTags, config.prefixOfTags);
        }
        return tableMeta;
    }


}
