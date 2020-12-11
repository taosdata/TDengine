package com.taosdata.taosdemo.components;

import com.taosdata.taosdemo.domain.*;
import com.taosdata.taosdemo.service.DatabaseService;
import com.taosdata.taosdemo.service.SubTableService;
import com.taosdata.taosdemo.service.SuperTableService;
import com.taosdata.taosdemo.service.data.SubTableMetaGenerator;
import com.taosdata.taosdemo.service.data.SubTableValueGenerator;
import com.taosdata.taosdemo.service.data.SuperTableMetaGenerator;
import com.taosdata.taosdemo.utils.JdbcTaosdemoConfig;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.TimeUnit;


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
    private List<SubTableMeta> subTableMetaList;
    private List<SubTableValue> subTableValueList;
    private List<List<SubTableValue>> dataList;


    @Override
    public void run(String... args) throws Exception {
        // 读配置参数
        JdbcTaosdemoConfig config = new JdbcTaosdemoConfig(args);
        boolean isHelp = Arrays.asList(args).contains("--help");
        if (isHelp) {
            JdbcTaosdemoConfig.printHelp();
        }
        // 准备数据
        prepareData(config);
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
        Map<String, String> databaseParam = new HashMap<>();
        databaseParam.put("database", config.database);
        databaseParam.put("keep", Integer.toString(config.keep));
        databaseParam.put("days", Integer.toString(config.days));
        databaseParam.put("replica", Integer.toString(config.replica));
        //TODO: other database parameters
        databaseService.dropDatabase(config.database);
        databaseService.createDatabase(databaseParam);
        databaseService.useDatabase(config.database);
    }

    // 建超级表，三种方式：1. 指定SQL，2. 指定field和tags的个数，3. 默认
    private void createTableTask(JdbcTaosdemoConfig config) {
        if (config.doCreateTable) {
            superTableService.create(superTableMeta);
            // 批量建子表
            subTableService.createSubTable(subTableMetaList, config.numOfThreadsForCreate);
        }
    }

    private void insertTask(JdbcTaosdemoConfig config) {
        int numOfThreadsForInsert = config.numOfThreadsForInsert;
        int sleep = config.sleep;
        if (config.autoCreateTable) {
            // 批量插入，自动建表
            dataList.stream().forEach(subTableValues -> {
                subTableService.insertAutoCreateTable(subTableValues, numOfThreadsForInsert);
                sleep(sleep);
            });
        } else {
            dataList.stream().forEach(subTableValues -> {
                subTableService.insert(subTableValues, numOfThreadsForInsert);
                sleep(sleep);
            });
        }
    }

    private void prepareData(JdbcTaosdemoConfig config) {
        // 超级表的meta
        superTableMeta = createSupertable(config);
        // 子表的meta
        subTableMetaList = SubTableMetaGenerator.generate(superTableMeta, config.numOfTables, config.tablePrefix);
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
    }

    private SuperTableMeta createSupertable(JdbcTaosdemoConfig config) {
        SuperTableMeta tableMeta;
        // create super table
        logger.info(">>> create super table <<<");
        if (config.superTableSQL != null) {
            // use a sql to create super table
            tableMeta = SuperTableMetaGenerator.generate(config.superTableSQL);
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

    private static void sleep(int sleep) {
        if (sleep <= 0)
            return;
        try {
            TimeUnit.MILLISECONDS.sleep(sleep);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
