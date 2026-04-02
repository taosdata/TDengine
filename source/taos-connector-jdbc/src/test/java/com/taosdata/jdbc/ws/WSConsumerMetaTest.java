package com.taosdata.jdbc.ws;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.tmq.*;
import com.taosdata.jdbc.utils.JsonUtil;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import com.taosdata.jdbc.ws.tmq.meta.*;
import org.junit.*;

import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class WSConsumerMetaTest {
    private static final String HOST = "127.0.0.1";
    private static final String DB_NAME = TestUtils.camelToSnake(WSConsumerMetaTest.class);
    private static final String SUPER_TABLE = "st";
    private static final String SUPER_TABLE_JSON = "st_json";
    private static Connection connection;
    private static Statement statement;
    private static final String[] topics = {"topic_ws_map" + DB_NAME, "topic_db" + DB_NAME, "topic_json" + DB_NAME};

    @Test
    public void testCreateChildTable() throws Exception {
        String topic = topics[0];
        // create topic
        statement.executeUpdate("create topic if not exists " + topic + " only meta as STABLE " + SUPER_TABLE);

        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_USER, TestEnvUtil.getUser());
        properties.setProperty(TMQConstants.CONNECT_PASS, TestEnvUtil.getPassword());
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, TestEnvUtil.getHost() + ":" + TestEnvUtil.getWsPort());
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        properties.setProperty(TMQConstants.AUTO_OFFSET_RESET, "latest");
        properties.setProperty(TMQConstants.GROUP_ID, "ws_map");
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");
        properties.setProperty(TMQConstants.MSG_ENABLE_BATCH_META, "1");

        try (TaosConsumer<Map<String, Object>> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            consumer.poll(Duration.ofMillis(100));
            {
                int idx = 1;
                String sql = String.format("create table if not exists %s using %s.%s tags(%s, '%s', %s)", "ct" + idx, DB_NAME, SUPER_TABLE, idx, "t" + idx, "true");
                statement.execute(sql);
            }

            MetaCreateChildTable dstMeta = new MetaCreateChildTable();
            dstMeta.setType(MetaType.CREATE);
            dstMeta.setTableName("ct1");
            dstMeta.setTableType(TableType.CHILD);
            dstMeta.setUsing("st");
            dstMeta.setTagNum(3);
            List<Tag> tags = new ArrayList<>();
            tags.add(new Tag("t1", 4, JsonUtil.getObjectMapper().getNodeFactory().numberNode(1)));
            tags.add(new Tag("t2", 8, JsonUtil.getObjectMapper().getNodeFactory().textNode("\"t1\"")));
            tags.add(new Tag("t3", 1, JsonUtil.getObjectMapper().getNodeFactory().numberNode(1)));
            dstMeta.setTags(tags);
            dstMeta.setCreateList(new ArrayList<>());

            boolean getCreate = false;
            int loopTime = 10;
            while (!getCreate && loopTime > 0) {
                ConsumerRecords<Map<String, Object>> consumerRecords = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<Map<String, Object>> r : consumerRecords) {
                    MetaCreateChildTable meta = (MetaCreateChildTable) r.getMeta();
                    Assert.assertEquals(dstMeta, meta);
                }
                if (!consumerRecords.isEmpty()){
                    getCreate = true;
                }
                loopTime--;
            }
            Assert.assertTrue(getCreate);
            consumer.unsubscribe();
        }
    }

    @Test
    public void testAutoCreateChildTable() throws Exception {
        String topic = topics[0];
        // create topic
        statement.executeUpdate("create topic if not exists " + topic + " only meta as STABLE " + SUPER_TABLE);

        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_USER, TestEnvUtil.getUser());
        properties.setProperty(TMQConstants.CONNECT_PASS, TestEnvUtil.getPassword());
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, TestEnvUtil.getHost() + ":" + TestEnvUtil.getWsPort());
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        properties.setProperty(TMQConstants.AUTO_OFFSET_RESET, "latest");
        properties.setProperty(TMQConstants.GROUP_ID, "ws_map");
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");
        properties.setProperty(TMQConstants.MSG_ENABLE_BATCH_META, "1");

        try (TaosConsumer<Map<String, Object>> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            consumer.poll(Duration.ofMillis(100));
            {
                int idx = 1;
                String sql = String.format("insert into %s using %s.%s tags(%s, '%s', %s) values (now, 1)", "act" + idx, DB_NAME, SUPER_TABLE, idx, "t" + idx, "true");
                statement.execute(sql);
            }

            MetaCreateChildTable dstMeta = new MetaCreateChildTable();
            dstMeta.setType(MetaType.CREATE);
            dstMeta.setTableName("act1");
            dstMeta.setTableType(TableType.CHILD);
            dstMeta.setUsing("st");
            dstMeta.setTagNum(3);
            List<Tag> tags = new ArrayList<>();
            tags.add(new Tag("t1", 4, JsonUtil.getObjectMapper().getNodeFactory().numberNode(1)));
            tags.add(new Tag("t2", 8, JsonUtil.getObjectMapper().getNodeFactory().textNode("\"t1\"")));
            tags.add(new Tag("t3", 1, JsonUtil.getObjectMapper().getNodeFactory().numberNode(1)));
            dstMeta.setTags(tags);
            dstMeta.setCreateList(new ArrayList<>());

            int looptime = 10;
            boolean getCreate = false;
            while (!getCreate && looptime > 0) {
                ConsumerRecords<Map<String, Object>> consumerRecords = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<Map<String, Object>> r : consumerRecords) {
                    MetaCreateChildTable meta = (MetaCreateChildTable) r.getMeta();
                    Assert.assertEquals(dstMeta, meta);
                }
                if (!consumerRecords.isEmpty()){
                    getCreate = true;
                }
                looptime--;
            }
            Assert.assertTrue(getCreate);
            consumer.unsubscribe();
        }
    }

    @Test
    public void testCreateChildTableJson() throws Exception {
        String topic = topics[2];
        // create topic
        statement.executeUpdate("create topic if not exists " + topic + " only meta as STABLE " + SUPER_TABLE_JSON);

        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_USER, TestEnvUtil.getUser());
        properties.setProperty(TMQConstants.CONNECT_PASS, TestEnvUtil.getPassword());
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, TestEnvUtil.getHost() + ":" + TestEnvUtil.getWsPort());
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        properties.setProperty(TMQConstants.AUTO_OFFSET_RESET, "latest");
        properties.setProperty(TMQConstants.GROUP_ID, "ws_map");
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");
        properties.setProperty(TMQConstants.MSG_ENABLE_BATCH_META, "1");

        try (TaosConsumer<Map<String, Object>> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            consumer.poll(Duration.ofMillis(100));
            {
                int idx = 1;
                String sql = String.format("create table if not exists %s using %s.%s tags('%s')", "et" + idx, DB_NAME, SUPER_TABLE_JSON, "{\"a\":1}");
                statement.execute(sql);
            }

            MetaCreateChildTable dstMeta = new MetaCreateChildTable();
            dstMeta.setType(MetaType.CREATE);
            dstMeta.setTableName("et1");
            dstMeta.setTableType(TableType.CHILD);
            dstMeta.setUsing("st_json");
            dstMeta.setTagNum(1);
            List<Tag> tags = new ArrayList<>();
            tags.add(new Tag("t1", 15, JsonUtil.getObjectMapper().getNodeFactory().textNode("{\"a\":1}")));
            dstMeta.setTags(tags);
            dstMeta.setCreateList(new ArrayList<>());

            int loopTime = 10;
            boolean getCreate = false;
            while (!getCreate && loopTime > 0) {
                ConsumerRecords<Map<String, Object>> consumerRecords = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<Map<String, Object>> r : consumerRecords) {
                    MetaCreateChildTable meta = (MetaCreateChildTable) r.getMeta();
                    Assert.assertEquals(dstMeta, meta);
                }
                if (!consumerRecords.isEmpty()){
                    getCreate = true;
                }
                loopTime--;
            }

            Assert.assertTrue(getCreate);
            consumer.unsubscribe();
        }
    }
    @Test
    public void testCreateMultiChildTable() throws Exception {
        String topic = topics[0];
        // create topic
        statement.executeUpdate("create topic if not exists " + topic + " only meta as STABLE " + SUPER_TABLE);

        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_USER, TestEnvUtil.getUser());
        properties.setProperty(TMQConstants.CONNECT_PASS, TestEnvUtil.getPassword());
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, TestEnvUtil.getHost() + ":" + TestEnvUtil.getWsPort());
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "false");
        properties.setProperty(TMQConstants.AUTO_OFFSET_RESET, "latest");
        properties.setProperty(TMQConstants.GROUP_ID, "ws_map");
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");
        properties.setProperty(TMQConstants.MSG_ENABLE_BATCH_META, "1");

        int subTableNum = 9;
        try (TaosConsumer<Map<String, Object>> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            consumer.poll(Duration.ofMillis(100));
            {
                String sql = "create table if not exists ";
                for (int idx = 1; idx <= subTableNum; idx++) {
                    sql += String.format("%s using %s.%s tags(%s, '%s', %s)", "dt" + idx, DB_NAME, SUPER_TABLE, idx, "t" + idx, "true");
                }
                statement.execute(sql);
            }

            int changeNum = subTableNum;
            int loopTime = 10;

            List<MetaCreateChildTable> metaList = new ArrayList<>();
            while (changeNum > 0 && loopTime > 0) {
                ConsumerRecords<Map<String, Object>> consumerRecords = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<Map<String, Object>> r : consumerRecords) {
                    MetaCreateChildTable meta = (MetaCreateChildTable) r.getMeta();
                    Assert.assertEquals(MetaType.CREATE, meta.getType());
                    metaList.add(meta);
                    if (!meta.getCreateList().isEmpty()){
                        changeNum -= meta.getCreateList().size();
                    } else {
                        changeNum--;
                    }
                }
                loopTime--;
            }
            if(changeNum > 0){
                throw new Exception("not all child table created");
            }

            List<Integer> tags = new ArrayList<>();
            for (MetaCreateChildTable metaCreateChildTable : metaList){
                if (!metaCreateChildTable.getCreateList().isEmpty()){
                    for (ChildTableInfo childTableInfo : metaCreateChildTable.getCreateList()){
                        tags.add(childTableInfo.getTags().get(0).getValue().asInt());
                    }
                } else {
                    tags.add(metaCreateChildTable.getTags().get(0).getValue().asInt());
                }
            }

            tags.sort(Integer::compareTo);
            for (int i = 1; i <= subTableNum; i++){
                Assert.assertTrue(i == tags.get(i - 1));
            }
            consumer.commitSync();
            consumer.unsubscribe();
        }
    }
    @Test
    public void testCreateSuperTable() throws Exception {
        String topic = topics[1];
        // create topic
        statement.executeUpdate("create topic if not exists " + topic + " only meta as database " + DB_NAME);

        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_USER, TestEnvUtil.getUser());
        properties.setProperty(TMQConstants.CONNECT_PASS, TestEnvUtil.getPassword());
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, TestEnvUtil.getHost() + ":" + TestEnvUtil.getWsPort());
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "false");
        properties.setProperty(TMQConstants.AUTO_OFFSET_RESET, "latest");
        properties.setProperty(TMQConstants.GROUP_ID, "ws_map");
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");
        properties.setProperty(TMQConstants.MSG_ENABLE_BATCH_META, "1");

        try (TaosConsumer<Map<String, Object>> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            consumer.poll(Duration.ofMillis(100));
            {
                int idx = 1;
                statement.execute("create stable if not exists " + SUPER_TABLE + idx
                        + " (ts timestamp, c1 int) tags(t1 int, t2 bool)");
            }

            MetaCreateSuperTable dstMeta = new MetaCreateSuperTable();
            dstMeta.setType(MetaType.CREATE);
            dstMeta.setTableName("st1");
            dstMeta.setTableType(TableType.SUPER);
            List<Column> columns = new ArrayList<>();
            columns.add(new Column("ts", 9, null, false, "delta-i", "lz4", "medium"));
            columns.add(new Column("c1", 4, null, false, "simple8b", "lz4", "medium"));
            dstMeta.setColumns(columns);
            List<Column> tags = new ArrayList<>();
            tags.add(new Column("t1", 4, null, null, null, null, null));
            tags.add(new Column("t2", 1, null, null, null, null, null));
            dstMeta.setTags(tags);

            boolean getCreate = false;
            int loopTime = 10;
            while (!getCreate && loopTime > 0) {
                ConsumerRecords<Map<String, Object>> consumerRecords = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<Map<String, Object>> r : consumerRecords) {
                    MetaCreateSuperTable meta = (MetaCreateSuperTable) r.getMeta();
                    Assert.assertEquals(dstMeta, meta);
                }
                if (!consumerRecords.isEmpty()){
                    getCreate = true;
                }
                loopTime--;
            }

            Assert.assertTrue(getCreate);
            consumer.commitSync();
            consumer.unsubscribe();
        }
    }
    @Test
    public void testCreateNormalTable() throws Exception {
        String topic = topics[1];
        // create topic
        statement.executeUpdate("create topic if not exists " + topic + " only meta as database " + DB_NAME);

        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_USER, TestEnvUtil.getUser());
        properties.setProperty(TMQConstants.CONNECT_PASS, TestEnvUtil.getPassword());
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, TestEnvUtil.getHost() + ":" + TestEnvUtil.getWsPort());
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "false");
        properties.setProperty(TMQConstants.AUTO_OFFSET_RESET, "latest");
        properties.setProperty(TMQConstants.GROUP_ID, "ws_map");
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");
        properties.setProperty(TMQConstants.MSG_ENABLE_BATCH_META, "1");

        String normalTable = "nn";
        try (TaosConsumer<Map<String, Object>> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            consumer.poll(Duration.ofMillis(100));
            {
                int idx = 1;
                statement.execute("create table if not exists " + normalTable + idx
                        + " (ts timestamp, c1 int)");
            }

            MetaCreateNormalTable dstMeta = new MetaCreateNormalTable();
            dstMeta.setType(MetaType.CREATE);
            dstMeta.setTableName("nn1");
            dstMeta.setTableType(TableType.NORMAL);
            List<Column> columns = new ArrayList<>();
            columns.add(new Column("ts", 9, null, false, "delta-i", "lz4", "medium"));
            columns.add(new Column("c1", 4, null, false, "simple8b", "lz4", "medium"));
            dstMeta.setColumns(columns);
            List<Column> tags = new ArrayList<>();
            dstMeta.setTags(tags);

            boolean getCreate = false;
            int loopTime = 10;
            while (!getCreate && loopTime > 0) {
                ConsumerRecords<Map<String, Object>> consumerRecords = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<Map<String, Object>> r : consumerRecords) {
                    MetaCreateNormalTable meta = (MetaCreateNormalTable) r.getMeta();
                    Assert.assertEquals(dstMeta, meta);
                }
                if (!consumerRecords.isEmpty()){
                    getCreate = true;
                }
                loopTime--;
            }

            Assert.assertTrue(getCreate);
            consumer.commitSync();
            consumer.unsubscribe();
        }
    }
    @Test
    public void testDropSuperTable() throws Exception {
        String topic = topics[1];
        // create topic
        statement.executeUpdate("create topic if not exists " + topic + " only meta as database " + DB_NAME);

        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_USER, TestEnvUtil.getUser());
        properties.setProperty(TMQConstants.CONNECT_PASS, TestEnvUtil.getPassword());
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, TestEnvUtil.getHost() + ":" + TestEnvUtil.getWsPort());
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        properties.setProperty(TMQConstants.AUTO_OFFSET_RESET, "latest");
        properties.setProperty(TMQConstants.GROUP_ID, "ws_map");
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");
        properties.setProperty(TMQConstants.MSG_ENABLE_BATCH_META, "1");

        try (TaosConsumer<Map<String, Object>> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            consumer.poll(Duration.ofMillis(100));
            {
                int idx = 2;
                statement.execute("create stable if not exists " + SUPER_TABLE + idx
                        + " (ts timestamp, c1 int) tags(t1 int, t2 bool)");

                statement.execute("drop stable if exists " + SUPER_TABLE + idx);
            }

            MetaDropSuperTable dstMeta = new MetaDropSuperTable();
            dstMeta.setType(MetaType.DROP);
            dstMeta.setTableName("st2");
            dstMeta.setTableType(TableType.SUPER);

            boolean getDrop = false;
            int maxLoop = 10;
            while (!getDrop && maxLoop > 0) {
                ConsumerRecords<Map<String, Object>> consumerRecords = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<Map<String, Object>> r : consumerRecords) {
                    if (r.getMeta().getType() == MetaType.DROP) {
                        MetaDropSuperTable meta = (MetaDropSuperTable) r.getMeta();
                        Assert.assertEquals(dstMeta, meta);
                        getDrop = true;
                        break;
                    }
                }
                maxLoop--;
            }

            Assert.assertTrue(getDrop);
            consumer.commitSync();
            consumer.unsubscribe();
        }
    }

    @Test
    @Ignore // tsdb bug
    public void testDropChildTable() throws Exception {
        String topic = topics[0];
        // create topic
        statement.executeUpdate("create topic if not exists " + topic + " only meta as STABLE " + SUPER_TABLE);

        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_USER, TestEnvUtil.getUser());
        properties.setProperty(TMQConstants.CONNECT_PASS, TestEnvUtil.getPassword());
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, TestEnvUtil.getHost() + ":" + TestEnvUtil.getWsPort());
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        properties.setProperty(TMQConstants.AUTO_OFFSET_RESET, "latest");
        properties.setProperty(TMQConstants.GROUP_ID, "ws_map");
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");
        properties.setProperty(TMQConstants.MSG_ENABLE_BATCH_META, "1");

        int subTableNum = 5;
        try (TaosConsumer<Map<String, Object>> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            consumer.poll(Duration.ofMillis(100));
            {
                String sql = "create table if not exists ";
                for (int idx = 1; idx <= subTableNum; idx++) {
                    sql += String.format(" %s using %s.%s tags(%s, '%s', %s)", "cht" + idx, DB_NAME, SUPER_TABLE, idx, "t" + idx, "true");
                }
                statement.execute(sql);

                String dropSql = "drop table if exists ";
                for (int idx = 1; idx <= subTableNum; idx++) {
                    dropSql += String.format(" cht" + idx + ", ");
                }

                dropSql = dropSql.substring(0, dropSql.length() - 2);
                statement.execute(dropSql);
            }

            boolean getDrop = false;
            int loopTime = 10;

            List<String> dropChildTableNameList = new ArrayList<>();
            while(!getDrop && loopTime > 0) {
                ConsumerRecords<Map<String, Object>> consumerRecords = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<Map<String, Object>> r : consumerRecords) {
                    if (r.getMeta().getType() == MetaType.DROP) {
                        MetaDropTable meta = (MetaDropTable) r.getMeta();
                        dropChildTableNameList.addAll(meta.getTableNameList());

                        if (dropChildTableNameList.size() == subTableNum) {
                            getDrop = true;
                            break;
                        }
                    }
                }
                loopTime--;
            }
            Assert.assertTrue(getDrop);
            consumer.unsubscribe();
        }
    }

    @Test
    @Ignore // tsdb compatible bug, fixed next version
    public void testAlterTable() throws Exception {
        String topic = topics[0];
        // create topic
        statement.executeUpdate("create topic if not exists " + topic + " only meta as STABLE " + SUPER_TABLE);

        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_USER, TestEnvUtil.getUser());
        properties.setProperty(TMQConstants.CONNECT_PASS, TestEnvUtil.getPassword());
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, TestEnvUtil.getHost() + ":" + TestEnvUtil.getWsPort());
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        properties.setProperty(TMQConstants.AUTO_OFFSET_RESET, "latest");
        properties.setProperty(TMQConstants.GROUP_ID, "ws_map");
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");
        properties.setProperty(TMQConstants.MSG_ENABLE_BATCH_META, "1");

        int subTableNum = 5;
        try (TaosConsumer<Map<String, Object>> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            consumer.poll(Duration.ofMillis(100));
            {
                int idx = 1001;
                String sql = String.format("create table if not exists %s using %s.%s tags(%s, '%s', %s)", "ct" + idx, DB_NAME, SUPER_TABLE, idx, "t" + idx, "true");
                statement.execute(sql);

                String alterSql = String.format("alter table ct" + idx + " set tag t1=1001, t2='tt1001', t3=false");
                statement.execute(alterSql);
            }

            MetaAlterTable metaDst = new MetaAlterTable();
            metaDst.setType(MetaType.ALTER);
            metaDst.setTableName("ct1001");
            metaDst.setTableType(TableType.CHILD);
            metaDst.setAlterType(15);
            metaDst.setColType(0);
            metaDst.setColLength(0);
            metaDst.setColValueNull(false);
            List<TagAlter> tags = new ArrayList<>();
            tags.add(new TagAlter("t1", "1001", false));
            tags.add(new TagAlter("t2", "\"tt1001\"", false));
            tags.add(new TagAlter("t3", "fal", false));
            metaDst.setTags(tags);
            ObjectMapper mapper = JsonUtil.getObjectMapper();
            mapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

            boolean getAlter = false;
            int loopTime = 10;
            while (!getAlter && loopTime > 0) {
                ConsumerRecords<Map<String, Object>> consumerRecords = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<Map<String, Object>> r : consumerRecords) {
                    if (r.getMeta().getType() == MetaType.ALTER) {
                        MetaAlterTable meta = (MetaAlterTable) r.getMeta();
                        Assert.assertEquals(metaDst, meta);
                        getAlter = true;
                    }
                }
                loopTime--;
            }
            Assert.assertTrue(getAlter);
            consumer.unsubscribe();
        }
    }

    @BeforeClass
    public static void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-WS://" + HOST + ":" + TestEnvUtil.getWsPort() +"/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "C");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connection = DriverManager.getConnection(url, properties);
        statement = connection.createStatement();
        for (String topic : topics) {
            statement.executeUpdate("drop topic if exists " + topic);
        }
        statement.execute("drop database if exists " + DB_NAME);
        statement.execute("create database if not exists " + DB_NAME + " WAL_RETENTION_PERIOD 3650");
        statement.execute("use " + DB_NAME);
        statement.execute("create stable if not exists " + SUPER_TABLE
                + " (ts timestamp, c1 int) tags(t1 int, t2 varchar(10), t3 bool)");
        statement.execute("create stable if not exists " + SUPER_TABLE_JSON
                + " (ts timestamp, c1 int) tags(t1 json)");
    }

    @AfterClass
    public static void after() throws InterruptedException {
        try {
            if (connection != null) {
                if (statement != null) {
                    for (String topic : topics) {
                        TimeUnit.SECONDS.sleep(3);
                        statement.executeUpdate("drop topic if exists " + topic);
                    }
                    statement.executeUpdate("drop database if exists " + DB_NAME);
                    statement.close();
                }
                connection.close();
            }
        } catch (SQLException e) {
            // ignore
        }
    }
}

