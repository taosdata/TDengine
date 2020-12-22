package com.taosdata.taosdemo;

import com.taosdata.taosdemo.components.DataSourceFactory;
import com.taosdata.taosdemo.components.JdbcTaosdemoConfig;
import com.taosdata.taosdemo.domain.SuperTableMeta;
import com.taosdata.taosdemo.service.DatabaseService;
import com.taosdata.taosdemo.service.SubTableService;
import com.taosdata.taosdemo.service.SuperTableService;
import com.taosdata.taosdemo.service.data.SuperTableMetaGenerator;
import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class TaosDemoApplication {

    private static Logger logger = Logger.getLogger(TaosDemoApplication.class);

    public static void main(String[] args) throws IOException {
        // 读配置参数
        JdbcTaosdemoConfig config = new JdbcTaosdemoConfig(args);
        boolean isHelp = Arrays.asList(args).contains("--help");
        if (isHelp || config.host == null || config.host.isEmpty()) {
            JdbcTaosdemoConfig.printHelp();
            System.exit(0);
        }
        // 初始化
        final DataSource dataSource = DataSourceFactory.getInstance(config.host, config.port, config.user, config.password);
        final DatabaseService databaseService = new DatabaseService(dataSource);
        final SuperTableService superTableService = new SuperTableService(dataSource);
        final SubTableService subTableService = new SubTableService(dataSource);
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
        // 构造超级表的meta
        SuperTableMeta superTableMeta;
        // create super table
        if (config.superTableSQL != null) {
            // use a sql to create super table
            superTableMeta = SuperTableMetaGenerator.generate(config.superTableSQL);
            if (config.database != null && !config.database.isEmpty())
                superTableMeta.setDatabase(config.database);
        } else if (config.numOfFields == 0) {
            String sql = "create table " + config.database + "." + config.superTable + " (ts timestamp, temperature float, humidity int) tags(location nchar(64), groupId int)";
            superTableMeta = SuperTableMetaGenerator.generate(sql);
        } else {
            // create super table with specified field size and tag size
            superTableMeta = SuperTableMetaGenerator.generate(config.database, config.superTable, config.numOfFields, config.prefixOfFields, config.numOfTags, config.prefixOfTags);
        }
        /**********************************************************************************/
        // 建表
        start = System.currentTimeMillis();
        if (config.doCreateTable) {
            superTableService.create(superTableMeta);
            if (!config.autoCreateTable) {
                // 批量建子表
                subTableService.createSubTable(superTableMeta, config.numOfTables, config.prefixOfTable, config.numOfThreadsForCreate);
            }
        }
        end = System.currentTimeMillis();
        logger.info(">>> create table time cost : " + (end - start) + " ms.");
        /**********************************************************************************/
        // 插入
        long tableSize = config.numOfTables;
        int threadSize = config.numOfThreadsForInsert;
        long startTime = getProperStartTime(config.startTime, config.keep);

        if (tableSize < threadSize)
            threadSize = (int) tableSize;
        long gap = (long) Math.ceil((0.0d + tableSize) / threadSize);

        start = System.currentTimeMillis();
        // multi threads to insert
        int affectedRows = subTableService.insertAutoCreateTable(superTableMeta, threadSize, tableSize, startTime, gap, config);
        end = System.currentTimeMillis();
        logger.info("insert " + affectedRows + " rows, time cost: " + (end - start) + " ms");
        /**********************************************************************************/
        // 删除表
        if (config.dropTable) {
            superTableService.drop(config.database, config.superTable);
        }
        System.exit(0);
    }

    private static long getProperStartTime(long startTime, int keep) {
        Instant now = Instant.now();
        long earliest = now.minus(Duration.ofDays(keep)).toEpochMilli();
        if (startTime == 0 || startTime < earliest) {
            startTime = earliest;
        }
        return startTime;
    }


}
