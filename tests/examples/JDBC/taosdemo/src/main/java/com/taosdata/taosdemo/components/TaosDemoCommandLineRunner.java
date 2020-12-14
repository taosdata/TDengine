package com.taosdata.taosdemo.components;

import com.taosdata.taosdemo.domain.FieldMeta;
import com.taosdata.taosdemo.domain.SubTableValue;
import com.taosdata.taosdemo.domain.SuperTableMeta;
import com.taosdata.taosdemo.domain.TagMeta;
import com.taosdata.taosdemo.service.DatabaseService;
import com.taosdata.taosdemo.service.SubTableService;
import com.taosdata.taosdemo.service.SuperTableService;
import com.taosdata.taosdemo.service.data.SubTableValueGenerator;
import com.taosdata.taosdemo.service.data.SuperTableMetaGenerator;
import com.taosdata.taosdemo.utils.JdbcTaosdemoConfig;
import com.taosdata.taosdemo.utils.TimeStampUtil;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.*;


@Component
public class TaosDemoCommandLineRunner implements CommandLineRunner {

    private static Logger logger = Logger.getLogger(TaosDemoCommandLineRunner.class);
    @Autowired
    private DatabaseService databaseService;
    @Autowired
    private SuperTableService superTableService;
    @Autowired
    private SubTableService subTableService;

    private SuperTableMeta superTableMeta;
//    private List<SubTableMeta> subTableMetaList;
//    private List<SubTableValue> subTableValueList;
//    private List<List<SubTableValue>> dataList;

    @Override
    public void run(String... args) throws Exception {
        // 读配置参数
        JdbcTaosdemoConfig config = new JdbcTaosdemoConfig(args);
        boolean isHelp = Arrays.asList(args).contains("--help");
        if (isHelp) {
            JdbcTaosdemoConfig.printHelp();
            System.exit(0);
        }
        // 准备数据
        prepareMetaData(config);
        // 超级表的meta
        superTableMeta = createSupertable(config);
        // 子表的meta
//        subTableMetaList = SubTableMetaGenerator.generate(superTableMeta, config.numOfTables, config.tablePrefix);
        // 创建数据库
        createDatabaseTask(config);
        // 建表
        createTableTask(config);
        // 插入
        insertTask(config);
        // 查询: 1. 生成查询语句, 2. 执行查询

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
            subTableService.createSubTable(superTableMeta, config.numOfTables, config.prefixOfTable, config.numOfThreadsForCreate);
        }
        long end = System.currentTimeMillis();
        logger.info(">>> create table time cost : " + (end - start) + " ms.");
    }

    private void insertTask(JdbcTaosdemoConfig config) {
        long start = System.currentTimeMillis();

        int numOfTables = config.numOfTables;
        int numOfTablesPerSQL = config.numOfTablesPerSQL;
        int numOfRowsPerTable = config.numOfRowsPerTable;
        int numOfValuesPerSQL = config.numOfValuesPerSQL;

        Instant now = Instant.now();
        long earliest = now.minus(Duration.ofDays(config.keep)).toEpochMilli();
        if (config.startTime == 0 || config.startTime < earliest) {
            config.startTime = earliest;
        }

        if (numOfRowsPerTable < numOfValuesPerSQL)
            numOfValuesPerSQL = numOfRowsPerTable;
        if (numOfTables < numOfTablesPerSQL)
            numOfTablesPerSQL = numOfTables;

        // row
        for (int rowCnt = 0; rowCnt < numOfRowsPerTable; ) {
            int rowSize = numOfValuesPerSQL;
            if (rowCnt + rowSize > numOfRowsPerTable) {
                rowSize = numOfRowsPerTable - rowCnt;
            }

            //table
            for (int tableCnt = 0; tableCnt < numOfTables; ) {
                int tableSize = numOfTablesPerSQL;
                if (tableCnt + tableSize > numOfTables) {
                    tableSize = numOfTables - tableCnt;
                }
                /***********************************************/
                long startTime = config.startTime + rowCnt * config.timeGap;

//                for (int i = 0; i < tableSize; i++) {
//                    System.out.print(config.prefixOfTable + (tableCnt + i + 1) + ", tableSize: " + tableSize + ", rowSize: " + rowSize);
//                    System.out.println(", startTime: " + TimeStampUtil.longToDatetime(startTime) + ",timeGap: " + config.timeGap);
//                }

                // 生成数据
                List<SubTableValue> data = SubTableValueGenerator.generate(superTableMeta, config.prefixOfTable, tableCnt, tableSize, rowSize, startTime, config.timeGap);
//                List<SubTableValue> data = SubTableValueGenerator.generate(subTableMetaList, tableCnt, tableSize, rowSize, startTime, config.timeGap);
                // 乱序
                if (config.order != 0) {
                    SubTableValueGenerator.disrupt(data, config.rate, config.range);
                }
                // insert
                if (config.autoCreateTable) {
                    subTableService.insertAutoCreateTable(data, config.numOfThreadsForInsert, config.frequency);
                } else {
                    subTableService.insert(data, config.numOfThreadsForInsert, config.frequency);
                }
                /***********************************************/
                tableCnt += tableSize;
            }
            rowCnt += rowSize;
        }


        /*********************************************************************************/
        // 批量插入，自动建表
//            dataList.stream().forEach(subTableValues -> {
//                subTableService.insertAutoCreateTable(subTableValues, config.numOfThreadsForInsert, config.frequency);
//            });

//            subTableService.insertAutoCreateTable(subTableMetaList, config.numOfTables, config.tablePrefix, config.numOfThreadsForInsert, config.frequency);
//        } else {
//            dataList.stream().forEach(subTableValues -> {
//                subTableService.insert(subTableValues, config.numOfThreadsForInsert, config.frequency);
//            });

//            subTableService.insert(subTableMetaList, config.numOfTables, config.tablePrefix, config.numOfThreadsForInsert, config.frequency);
//        }
        long end = System.currentTimeMillis();
        logger.info(">>> insert time cost : " + (end - start) + " ms.");
    }

    private void prepareMetaData(JdbcTaosdemoConfig config) {
        long start = System.currentTimeMillis();
        // 超级表的meta
        superTableMeta = createSupertable(config);
        // 子表的meta
//        subTableMetaList = SubTableMetaGenerator.generate(superTableMeta, config.numOfTables, config.prefixOfTable);

        /*
        // 子表的data
        subTableValueList = SubTableValueGenerator.generate(subTableMetaList, config.numOfRowsPerTable, config.startTime, config.timeGap);
        // 如果有乱序，给数据搞乱
        if (config.order != 0) {
            SubTableValueGenerator.disrupt(subTableValueList, config.rate, config.range);
        }
        // 分割数据
        int numOfTables = config.numOfTables;
        int numOfTablesPerSQL = config.numOfTablesPerSQL;
        int numOfRowsPerTable = config.numOfRowsPerTable;
        int numOfValuesPerSQL = config.numOfValuesPerSQL;
        dataList = SubTableValueGenerator.split(subTableValueList, numOfTables, numOfTablesPerSQL, numOfRowsPerTable, numOfValuesPerSQL);
        */
        long end = System.currentTimeMillis();
        logger.info(">>> prepare meta data time cost : " + (end - start) + " ms.");
    }

    private SuperTableMeta createSupertable(JdbcTaosdemoConfig config) {
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
