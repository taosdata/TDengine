package com.taosdata.taosdemo;

import com.taosdata.taosdemo.components.DataSourceFactory;
import com.taosdata.taosdemo.domain.FieldMeta;
import com.taosdata.taosdemo.domain.SuperTableMeta;
import com.taosdata.taosdemo.domain.TagMeta;
import com.taosdata.taosdemo.service.DatabaseService;
import com.taosdata.taosdemo.service.InsertTask;
import com.taosdata.taosdemo.service.SuperTableService;
import com.taosdata.taosdemo.service.data.SuperTableMetaGenerator;
import com.taosdata.taosdemo.utils.JdbcTaosdemoConfig;
import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TaosDemoApplication {
    private static Logger logger = Logger.getLogger(TaosDemoApplication.class);

    public static void main(String[] args) {


        // 读配置参数
        JdbcTaosdemoConfig config = new JdbcTaosdemoConfig(args);
        boolean isHelp = Arrays.asList(args).contains("--help");
        if (isHelp || config.host == null || config.host.isEmpty()) {
            JdbcTaosdemoConfig.printHelp();
            System.exit(0);
        }

        DataSource dataSource = DataSourceFactory.getInstance(config.host, config.port, config.user, config.password);
        DatabaseService databaseService = new DatabaseService(dataSource);
        SuperTableService superTableService = new SuperTableService(dataSource);

        // 创建数据库
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
        /**********************************************************************************/
        // 超级表的meta
        SuperTableMeta superTableMeta;
        // create super table
        if (config.superTableSQL != null) {
            // use a sql to create super table
            superTableMeta = SuperTableMetaGenerator.generate(config.superTableSQL);
            if (config.database != null && !config.database.isEmpty())
                superTableMeta.setDatabase(config.database);
        } else if (config.numOfFields == 0) {
            // default sql = "create table test.weather (ts timestamp, temperature float, humidity int) tags(location nchar(64), groupId int)";
            superTableMeta = new SuperTableMeta();
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
        } else {
            // create super table with specified field size and tag size
            superTableMeta = SuperTableMetaGenerator.generate(config.database, config.superTable, config.numOfFields, config.prefixOfFields, config.numOfTags, config.prefixOfTags);
        }

        /**********************************************************************************/
        // 建表
        start = System.currentTimeMillis();
        if (config.doCreateTable) {
            superTableService.create(superTableMeta);
            if (config.autoCreateTable)
                return;
            // 批量建子表
//            subTableService.createSubTable(superTableMeta, config.numOfTables, config.prefixOfTable, config.numOfThreadsForCreate);
        }
        end = System.currentTimeMillis();
        logger.info(">>> create table time cost : " + (end - start) + " ms.");
        /**********************************************************************************/
        // 插入
        long tableSize = config.numOfTables;
        int threadSize = config.numOfThreadsForInsert;
        long startTime = getProperStartTime(config);

        if (tableSize < threadSize)
            threadSize = (int) tableSize;
        long gap = (long) Math.ceil((0.0d + tableSize) / threadSize);

        start = System.currentTimeMillis();

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

        end = System.currentTimeMillis();
        logger.info("insert " + affectedRows + " rows, time cost: " + (end - start) + " ms");
        /**********************************************************************************/
        // 删除表
        if (config.dropTable) {
            superTableService.drop(config.database, config.superTable);
        }
        System.exit(0);
    }

    private static long getProperStartTime(JdbcTaosdemoConfig config) {
        Instant now = Instant.now();
        long earliest = now.minus(Duration.ofDays(config.keep)).toEpochMilli();
        long startTime = config.startTime;
        if (startTime == 0 || startTime < earliest) {
            startTime = earliest;
        }
        return startTime;
    }


}
