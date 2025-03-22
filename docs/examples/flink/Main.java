package com.taosdata.flink.example;

import com.taosdata.flink.cdc.TDengineCdcSource;
import com.taosdata.flink.common.TDengineCdcParams;
import com.taosdata.flink.common.TDengineConfigParams;
import com.taosdata.flink.sink.TDengineSink;
import com.taosdata.flink.source.TDengineSource;
import com.taosdata.flink.source.entity.SourceSplitSql;
import com.taosdata.flink.source.entity.SplitType;
import com.taosdata.flink.source.entity.TimestampSplitInfo;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.tmq.ConsumerRecords;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.shaded.curator5.com.google.common.base.Strings;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.junit.Assert;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.time.Duration;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import javax.xml.transform.Source;

import org.apache.flink.streaming.api.CheckpointingMode;



public class Main {
    static String jdbcUrl = "jdbc:TAOS-WS://localhost:6041?user=root&password=taosdata";
    static void prepare() throws ClassNotFoundException, SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        String insertQuery = "INSERT INTO " +
                "power.d1001 USING power.meters TAGS('California.SanFrancisco', 1) " +
                "VALUES " +
                "('2024-12-19 19:12:45.642', 50.30000, 201, 0.31000) " +
                "('2024-12-19 19:12:46.642', 82.60000, 202, 0.33000) " +
                "('2024-12-19 19:12:47.642', 92.30000, 203, 0.31000) " +
                "('2024-12-19 18:12:45.642', 50.30000, 201, 0.31000) " +
                "('2024-12-19 18:12:46.642', 82.60000, 202, 0.33000) " +
                "('2024-12-19 18:12:47.642', 92.30000, 203, 0.31000) " +
                "('2024-12-19 17:12:45.642', 50.30000, 201, 0.31000) " +
                "('2024-12-19 17:12:46.642', 82.60000, 202, 0.33000) " +
                "('2024-12-19 17:12:47.642', 92.30000, 203, 0.31000) " +
                "power.d1002 USING power.meters TAGS('Alabama.Montgomery', 2) " +
                "VALUES " +
                "('2024-12-19 19:12:45.642', 50.30000, 204, 0.25000) " +
                "('2024-12-19 19:12:46.642', 62.60000, 205, 0.33000) " +
                "('2024-12-19 19:12:47.642', 72.30000, 206, 0.31000) " +
                "('2024-12-19 18:12:45.642', 50.30000, 204, 0.25000) " +
                "('2024-12-19 18:12:46.642', 62.60000, 205, 0.33000) " +
                "('2024-12-19 18:12:47.642', 72.30000, 206, 0.31000) " +
                "('2024-12-19 17:12:45.642', 50.30000, 204, 0.25000) " +
                "('2024-12-19 17:12:46.642', 62.60000, 205, 0.33000) " +
                "('2024-12-19 17:12:47.642', 72.30000, 206, 0.31000) ";

        Class.forName("com.taosdata.jdbc.ws.WebSocketDriver");
        try (Connection connection = DriverManager.getConnection(jdbcUrl, properties);
             Statement stmt = connection.createStatement()) {

            stmt.executeUpdate("DROP TOPIC IF EXISTS topic_meters");

            stmt.executeUpdate("DROP database IF EXISTS power");
            // create database
            int rowsAffected = stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS power vgroups 5");

            stmt.executeUpdate("use power");
            // you can check rowsAffected here
            System.out.println("Create database power successfully, rowsAffected: " + rowsAffected);
            // create table
            rowsAffected = stmt.executeUpdate("CREATE STABLE IF NOT EXISTS meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);");
            // you can check rowsAffected here
            System.out.println("Create stable power.meters successfully, rowsAffected: " + rowsAffected);

            stmt.executeUpdate("CREATE TOPIC topic_meters as SELECT ts, `current`, voltage, phase, location, groupid, tbname FROM meters");

            int affectedRows = stmt.executeUpdate(insertQuery);
            // you can check affectedRows here
            System.out.println("Successfully inserted " + affectedRows + " rows to power.meters.");

            stmt.executeUpdate("DROP database IF EXISTS power_sink");
            // create database
            stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS power_sink vgroups 5");

            stmt.executeUpdate("use power_sink");
            // you can check rowsAffected here
            System.out.println("Create database power successfully, rowsAffected: " + rowsAffected);
            // create table
            stmt.executeUpdate("CREATE STABLE IF NOT EXISTS sink_meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);");
            // you can check rowsAffected here

            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS sink_normal (ts timestamp, current float, voltage int, phase float);");
            // you can check rowsAffected here


        } catch (Exception ex) {
            // please refer to the JDBC specifications for detailed exceptions info
            System.out.printf("Failed to create database power or stable meters, %sErrMessage: %s%n",
                    ex instanceof SQLException ? "ErrCode: " + ((SQLException) ex).getErrorCode() + ", " : "",
                    ex.getMessage());
            // Print stack trace for context in examples. Use logging in production.
            throw ex;
        }
    }

    public static void main(String[] args) throws Exception {
        prepare();
        if (args != null && args.length > 0 &&  args[0].equals("source")) {
            testSource();
        } else if (args != null && args.length > 0 && args[0].equals("table")) {
            testTableToSink();
        } else if (args != null && args.length > 0 && args[0].equals("cdc")) {
            testCustomTypeCdc();
        }else if (args != null && args.length > 0 && args[0].equals("table-cdc")) {
            testCdcTableToSink();
        }
    }

    static SourceSplitSql getTimeSplit() {
// ANCHOR: time_interval
SourceSplitSql splitSql = new SourceSplitSql();
splitSql.setSql("select  ts, `current`, voltage, phase, groupid, location, tbname from meters")
.setSplitType(SplitType.SPLIT_TYPE_TIMESTAMP)
.setTimestampSplitInfo(new TimestampSplitInfo(
        "2024-12-19 16:12:48.000",
        "2024-12-19 19:12:48.000",
        "ts",
        Duration.ofHours(1),
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"),
        ZoneId.of("Asia/Shanghai")));
// ANCHOR_END: time_interval  
return splitSql;   
    }

    static SourceSplitSql getTagSplit() throws Exception {
// ANCHOR: tag_split        
SourceSplitSql splitSql = new SourceSplitSql();
splitSql.setSql("select  ts, current, voltage, phase, groupid, location from meters where voltage > 100")
        .setTagList(Arrays.asList("groupid >100 and location = 'Shanghai'", 
            "groupid >50 and groupid < 100 and location = 'Guangzhou'", 
            "groupid >0 and groupid < 50 and location = 'Beijing'"))
        .setSplitType(SplitType.SPLIT_TYPE_TAG);                     
// ANCHOR_END: tag_split 
return splitSql;
    }

    static SourceSplitSql getTableSqlit() {
// ANCHOR: table_split          
SourceSplitSql splitSql = new SourceSplitSql();
splitSql.setSelect("ts, current, voltage, phase, groupid, location")
        .setTableList(Arrays.asList("d1001", "d1002"))
        .setOther("order by ts limit 100")
        .setSplitType(SplitType.SPLIT_TYPE_TABLE);
// ANCHOR_END: table_split  
    }

    //ANCHOR: source_test
    static void testSource() throws Exception {
        Properties connProps = new Properties();
        connProps.setProperty(TDengineConfigParams.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        connProps.setProperty(TDengineConfigParams.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        connProps.setProperty(TDengineConfigParams.VALUE_DESERIALIZER, "RowData");
        connProps.setProperty(TDengineConfigParams.TD_JDBC_URL, "jdbc:TAOS-WS://localhost:6041/power?user=root&password=taosdata");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        splitSql.setSql("select  ts, `current`, voltage, phase, groupid, location, tbname from meters")
            .setSplitType(SplitType.SPLIT_TYPE_TIMESTAMP)
            .setTimestampSplitInfo(new TimestampSplitInfo(
            "2024-12-19 16:12:48.000",
            "2024-12-19 19:12:48.000",
            "ts",
            Duration.ofHours(1),
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"),
            ZoneId.of("Asia/Shanghai")));

        TDengineSource<RowData> source = new TDengineSource<>(connProps, sql, RowData.class);
        DataStreamSource<RowData> input = env.fromSource(source, WatermarkStrategy.noWatermarks(), "tdengine-source");
        DataStream<String> resultStream = input.map((MapFunction<RowData, String>) rowData -> {
            StringBuilder sb = new StringBuilder();
            sb.append("ts: " + rowData.getTimestamp(0, 0) +
                    ", current: " + rowData.getFloat(1) +
                    ", voltage: " + rowData.getInt(2) +
                    ", phase: " + rowData.getFloat(3) +
                    ", location: " + rowData.getString(4).toString());
            sb.append("\n");
            return sb.toString();
        });
        resultStream.print();
        env.execute("tdengine flink source");
       
    }
    //ANCHOR_END: source_test
    
    //ANCHOR: source_custom_type_test
    void testCustomTypeSource() throws Exception {
        System.out.println("testTDengineSourceByTimeSplit start！");
        Properties connProps = new Properties();
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        connProps.setProperty(TDengineConfigParams.VALUE_DESERIALIZER, "com.taosdata.flink.entity.ResultSoureDeserialization");
        connProps.setProperty(TDengineConfigParams.TD_JDBC_URL, "jdbc:TAOS-WS://localhost:6041/power?user=root&password=taosdata");
        SourceSplitSql splitSql = new SourceSplitSql();
        splitSql.setSql("select  ts, `current`, voltage, phase, groupid, location, tbname from meters")
                .setSplitType(SplitType.SPLIT_TYPE_TIMESTAMP)
                //按照时间分片
                .setTimestampSplitInfo(new TimestampSplitInfo(
                        "2024-12-19 16:12:48.000",
                        "2024-12-19 19:12:48.000",
                        "ts",
                        Duration.ofHours(1),
                        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"),
                        ZoneId.of("Asia/Shanghai")));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        TDengineSource<ResultBean> source = new TDengineSource<>(connProps, splitSql, ResultBean.class);
        DataStreamSource<ResultBean> input = env.fromSource(source, WatermarkStrategy.noWatermarks(), "tdengine-source");
        DataStream<String> resultStream = input.map((MapFunction<ResultBean, String>) rowData -> {
            StringBuilder sb = new StringBuilder();
            sb.append("ts: " + rowData.getTs() +
                    ", current: " + rowData.getCurrent() +
                    ", voltage: " + rowData.getVoltage() +
                    ", phase: " + rowData.getPhase() +
                    ", groupid: " + rowData.getGroupid() +
                    ", location" + rowData.getLocation() +
                    ", tbname: " + rowData.getTbname());
            sb.append("\n");
            totalVoltage.addAndGet(rowData.getVoltage());
            return sb.toString();
        });
        resultStream.print();
        env.execute("flink tdengine source");
    }
    //ANCHOR_END: source_custom_type_test

    //ANCHOR: source_batch_test
    void testBatchSource() throws Exception {
        Properties connProps = new Properties();
        connProps.setProperty(TDengineConfigParams.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        connProps.setProperty(TDengineConfigParams.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        connProps.setProperty(TDengineConfigParams.VALUE_DESERIALIZER, "RowData");
        connProps.setProperty(TDengineConfigParams.TD_BATCH_MODE, "true");
        connProps.setProperty(TDengineConfigParams.TD_JDBC_URL, "jdbc:TAOS-WS://localhost:6041/power?user=root&password=taosdata");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        Class<SourceRecords<RowData>> typeClass = (Class<SourceRecords<RowData>>) (Class<?>) SourceRecords.class;
        SourceSplitSql sql = new SourceSplitSql("select ts, `current`, voltage, phase, tbname from meters");
        TDengineSource<SourceRecords<RowData>> source = new TDengineSource<>(connProps, sql, typeClass);
        DataStreamSource<SourceRecords<RowData>> input = env.fromSource(source, WatermarkStrategy.noWatermarks(), "tdengine-source");
        DataStream<String> resultStream = input.map((MapFunction<SourceRecords<RowData>, String>) records -> {
            StringBuilder sb = new StringBuilder();
            Iterator<RowData> iterator = records.iterator();
            while (iterator.hasNext()) {
                GenericRowData row = (GenericRowData) iterator.next();
                sb.append("ts: " + row.getTimestamp(0, 0) +
                        ", current: " + row.getFloat(1) +
                        ", voltage: " + row.getInt(2) +
                        ", phase: " + row.getFloat(3) +
                        ", location: " + rowData.getString(4).toString());
                sb.append("\n");
                totalVoltage.addAndGet(row.getInt(2));
            }
            return sb.toString();
        });
        resultStream.print();
        env.execute("flink tdengine source");

    }
    //ANCHOR_END: source_batch_test

    //ANCHOR: cdc_source
    void testTDengineCdc() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.enableCheckpointing(100, AT_LEAST_ONCE);
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        Properties config = new Properties();
        config.setProperty(TDengineCdcParams.CONNECT_TYPE, "ws");
        config.setProperty(TDengineCdcParams.BOOTSTRAP_SERVERS, "localhost:6041");
        config.setProperty(TDengineCdcParams.AUTO_OFFSET_RESET, "earliest");
        config.setProperty(TDengineCdcParams.MSG_WITH_TABLE_NAME, "true");
        config.setProperty(TDengineCdcParams.AUTO_COMMIT_INTERVAL_MS, "1000");
        config.setProperty(TDengineCdcParams.GROUP_ID, "group_1");
        config.setProperty(TDengineCdcParams.ENABLE_AUTO_COMMIT, "true");
        config.setProperty(TDengineCdcParams.CONNECT_USER, "root");
        config.setProperty(TDengineCdcParams.CONNECT_PASS, "taosdata");
        config.setProperty(TDengineCdcParams.VALUE_DESERIALIZER, "RowData");
        config.setProperty(TDengineCdcParams.VALUE_DESERIALIZER_ENCODING, "UTF-8");
        TDengineCdcSource<RowData> tdengineSource = new TDengineCdcSource<>("topic_meters", config, RowData.class);
        DataStreamSource<RowData> input = env.fromSource(tdengineSource, WatermarkStrategy.noWatermarks(), "tdengine-source");
        DataStream<String> resultStream = input.map((MapFunction<RowData, String>) rowData -> {
            StringBuilder sb = new StringBuilder();
            sb.append("tsxx: " + rowData.getTimestamp(0, 0) +
                    ", current: " + rowData.getFloat(1) +
                    ", voltage: " + rowData.getInt(2) +
                    ", phase: " + rowData.getFloat(3) +
                    ", location: " + rowData.getString(4).toString());
            sb.append("\n");
            totalVoltage.addAndGet(rowData.getInt(2));
            return sb.toString();
        });
        resultStream.print();
        JobClient jobClient = env.executeAsync("Flink test cdc Example");
        Thread.sleep(5000L);
        // The task submitted by Flink UI cannot be cancle and needs to be stopped on the UI page.
        jobClient.cancel().get();
    }
    //ANCHOR_END: cdc_source

    //ANCHOR: cdc_batch_source
    void testTDengineCdcBatch() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        Properties config = new Properties();
        config.setProperty(TDengineCdcParams.CONNECT_TYPE, "ws");
        config.setProperty(TDengineCdcParams.BOOTSTRAP_SERVERS, "localhost:6041");
        config.setProperty(TDengineCdcParams.AUTO_OFFSET_RESET, "earliest");
        config.setProperty(TDengineCdcParams.MSG_WITH_TABLE_NAME, "true");
        config.setProperty(TDengineCdcParams.AUTO_COMMIT_INTERVAL_MS, "1000");
        config.setProperty(TDengineCdcParams.GROUP_ID, "group_1");
        config.setProperty(TDengineCdcParams.CONNECT_USER, "root");
        config.setProperty(TDengineCdcParams.CONNECT_PASS, "taosdata");
        config.setProperty(TDengineCdcParams.VALUE_DESERIALIZER, "RowData");
        config.setProperty(TDengineCdcParams.VALUE_DESERIALIZER_ENCODING, "UTF-8");
        config.setProperty(TDengineCdcParams.TMQ_BATCH_MODE, "true");

        Class<ConsumerRecords<RowData>> typeClass = (Class<ConsumerRecords<RowData>>) (Class<?>) ConsumerRecords.class;
        TDengineCdcSource<ConsumerRecords<RowData>> tdengineSource = new TDengineCdcSource<>("topic_meters", config, typeClass);
        DataStreamSource<ConsumerRecords<RowData>> input = env.fromSource(tdengineSource, WatermarkStrategy.noWatermarks(), "tdengine-source");
        DataStream<String> resultStream = input.map((MapFunction<ConsumerRecords<RowData>, String>) records -> {
            Iterator<ConsumerRecord<RowData>> iterator = records.iterator();
            StringBuilder sb = new StringBuilder();
            while (iterator.hasNext()) {
                GenericRowData row = (GenericRowData) iterator.next().value();
                sb.append("tsxx: " + row.getTimestamp(0, 0) +
                        ", current: " + row.getFloat(1) +
                        ", voltage: " + row.getInt(2) +
                        ", phase: " + row.getFloat(3) +
                        ", location: " + rowData.getString(4).toString());
                sb.append("\n");
                totalVoltage.addAndGet(row.getInt(2));
            }
            return sb.toString();

        });

        resultStream.print();
        JobClient jobClient = env.executeAsync("Flink test cdc Example");
        Thread.sleep(5000L);
        jobClient.cancel().get();
    }
    //ANCHOR_END: cdc_batch_source

    //ANCHOR: cdc_custom_type_test
    static void testCustomTypeCdc() throws Exception {
        System.out.println("testCustomTypeTDengineCdc start！");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.enableCheckpointing(100, AT_LEAST_ONCE);
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(4);
        Properties config = new Properties();
        config.setProperty(TDengineCdcParams.CONNECT_TYPE, "ws");
        config.setProperty(TDengineCdcParams.BOOTSTRAP_SERVERS, "localhost:6041");
        config.setProperty(TDengineCdcParams.AUTO_OFFSET_RESET, "earliest");
        config.setProperty(TDengineCdcParams.MSG_WITH_TABLE_NAME, "true");
        config.setProperty(TDengineCdcParams.AUTO_COMMIT_INTERVAL_MS, "1000");
        config.setProperty(TDengineCdcParams.GROUP_ID, "group_1");
        config.setProperty(TDengineCdcParams.CONNECT_USER, "root");
        config.setProperty(TDengineCdcParams.CONNECT_PASS, "taosdata");
        config.setProperty(TDengineCdcParams.VALUE_DESERIALIZER, "com.taosdata.flink.entity.ResultDeserializer");
        config.setProperty(TDengineCdcParams.VALUE_DESERIALIZER_ENCODING, "UTF-8");
        TDengineCdcSource<ResultBean> tdengineSource = new TDengineCdcSource<>("topic_meters", config, ResultBean.class);
        DataStreamSource<ResultBean> input = env.fromSource(tdengineSource, WatermarkStrategy.noWatermarks(), "tdengine-source");
        DataStream<String> resultStream = input.map((MapFunction<ResultBean, String>) rowData -> {
            StringBuilder sb = new StringBuilder();
            sb.append("ts: " + rowData.getTs() +
                    ", current: " + rowData.getCurrent() +
                    ", voltage: " + rowData.getVoltage() +
                    ", phase: " + rowData.getPhase() +
                    ", groupid: " + rowData.getGroupid() +
                    ", location" + rowData.getLocation() +
                    ", tbname: " + rowData.getTbname());
            sb.append("\n");
            totalVoltage.addAndGet(rowData.getVoltage());
            return sb.toString();
        });
        resultStream.print();
        JobClient jobClient = env.executeAsync("Flink test cdc Example");
        Thread.sleep(5000L);
        jobClient.cancel().get();
    }
    //ANCHOR_END: cdc_custom_type_test

    //ANCHOR: RowDataToSink
    static void testRowDataToSink() throws Exception {
        Properties connProps = new Properties();
        connProps.setProperty(TDengineConfigParams.VALUE_DESERIALIZER, "RowData");
        connProps.setProperty(TDengineConfigParams.TD_JDBC_URL, "jdbc:TAOS-WS://localhost:6041/power?user=root&password=taosdata");
        SourceSplitSql sql = new SourceSplitSql("select ts, `current`, voltage, phase, tbname from meters");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000, CheckpointingMode.AT_LEAST_ONCE);
        TDengineSource<RowData> source = new TDengineSource<>(connProps, sql, RowData.class);
        DataStreamSource<RowData> input = env.fromSource(source, WatermarkStrategy.noWatermarks(), "tdengine-source");
        Properties sinkProps = new Properties();
        sinkProps.setProperty(TDengineConfigParams.VALUE_DESERIALIZER, "RowData");
        sinkProps.setProperty(TDengineConfigParams.TD_SOURCE_TYPE, "tdengine_source");
        sinkProps.setProperty(TDengineConfigParams.TD_DATABASE_NAME, "power_sink");
        sinkProps.setProperty(TDengineConfigParams.TD_SUPERTABLE_NAME, "sink_meters");
        sinkProps.setProperty(TDengineConfigParams.TD_JDBC_URL, "jdbc:TAOS-WS://localhost:6041/power_sink?user=root&password=taosdata");
        sinkProps.setProperty(TDengineConfigParams.TD_BATCH_SIZE, "2000");

        // Arrays.asList The list of target table field names needs to be consistent with the data order
        TDengineSink<RowData> sink = new TDengineSink<>(sinkProps, 
            Arrays.asList("ts", "current", "voltage", "phase", "groupid", "location", "tbname"));
        
        input.sinkTo(sink);
        env.execute("flink tdengine source");
    }
    //ANCHOR_END: RowDataToSink

    //ANCHOR: CdcRowDataToSink
    static void testCdcToSink() throws Exception {
        System.out.println("testTDengineCdcToTdSink start！");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.enableCheckpointing(500, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(5000);
        Properties config = new Properties();
        config.setProperty(TDengineCdcParams.CONNECT_TYPE, "ws");
        config.setProperty(TDengineCdcParams.BOOTSTRAP_SERVERS, "localhost:6041");
        config.setProperty(TDengineCdcParams.AUTO_OFFSET_RESET, "earliest");
        config.setProperty(TDengineCdcParams.GROUP_ID, "group_1");
        config.setProperty(TDengineCdcParams.CONNECT_USER, "root");
        config.setProperty(TDengineCdcParams.CONNECT_PASS, "taosdata");
        config.setProperty(TDengineCdcParams.VALUE_DESERIALIZER, "RowData");
        config.setProperty(TDengineCdcParams.VALUE_DESERIALIZER_ENCODING, "UTF-8");

        TDengineCdcSource<RowData> tdengineSource = new TDengineCdcSource<>("topic_meters", config, RowData.class);
        DataStreamSource<RowData> input = env.fromSource(tdengineSource, WatermarkStrategy.noWatermarks(), "tdengine-source");

        Properties sinkProps = new Properties();
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        sinkProps.setProperty(TDengineConfigParams.VALUE_DESERIALIZER, "RowData");
        sinkProps.setProperty(TDengineConfigParams.TD_DATABASE_NAME, "power_sink");
        sinkProps.setProperty(TDengineConfigParams.TD_SUPERTABLE_NAME, "sink_meters");
        sinkProps.setProperty(TDengineConfigParams.TD_JDBC_URL, "jdbc:TAOS-WS://localhost:6041/power?user=root&password=taosdata");
        sinkProps.setProperty(TDengineConfigParams.TD_BATCH_SIZE, "2000");

        TDengineSink<RowData> sink = new TDengineSink<>(sinkProps, Arrays.asList("ts", "current", "voltage", "phase", "location", "groupid", "tbname"));
        input.sinkTo(sink);
        JobClient jobClient = env.executeAsync("Flink test cdc Example");
        Thread.sleep(6000L);
        jobClient.cancel().get();
        System.out.println("testTDengineCdcToTdSink finish！");
    }
    //ANCHOR_END: CdcRowDataToSink

    //ANCHOR: source_table
    static void testTableToSink() throws Exception {
        System.out.println("testTableToSink start！");
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.enableCheckpointing(1000, CheckpointingMode.AT_LEAST_ONCE);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, fsSettings);
        String tdengineSourceTableDDL = "CREATE TABLE `meters` (" +
                " ts TIMESTAMP," +
                " `current` FLOAT," +
                " voltage INT," +
                " phase FLOAT," +
                " location VARCHAR(255)," +
                " groupid INT," +
                " tbname VARCHAR(255)" +
                ") WITH (" +
                "  'connector' = 'tdengine-connector'," +
                "  'td.jdbc.url' = 'jdbc:TAOS-WS://localhost:6041/power?user=root&password=taosdata'," +
                "  'td.jdbc.mode' = 'source'," +
                "  'table-name' = 'meters'," +
                "  'scan.query' = 'SELECT ts, `current`, voltage, phase, location, groupid, tbname FROM `meters`'" +
                ")";


        String tdengineSinkTableDDL = "CREATE TABLE `sink_meters` (" +
                " ts TIMESTAMP," +
                " `current` FLOAT," +
                " voltage INT," +
                " phase FLOAT," +
                " location VARCHAR(255)," +
                " groupid INT," +
                " tbname VARCHAR(255)" +
                ") WITH (" +
                "  'connector' = 'tdengine-connector'," +
                "  'td.jdbc.mode' = 'sink'," +
                "  'td.jdbc.url' = 'jdbc:TAOS-WS://localhost:6041/power_sink?user=root&password=taosdata'," +
                "  'sink.db.name' = 'power_sink'," +
                "  'sink.supertable.name' = 'sink_meters'" +
                ")";

        tableEnv.executeSql(tdengineSourceTableDDL);
        tableEnv.executeSql(tdengineSinkTableDDL);
        tableEnv.executeSql("INSERT INTO sink_meters SELECT ts, `current`, voltage, phase, location, groupid, tbname FROM `meters`");
    }
    //ANCHOR_END: source_table
    
    //ANCHOR: cdc_table
    static void testCdcTableToSink() throws Exception {
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(5);
        env.enableCheckpointing(1000, CheckpointingMode.AT_LEAST_ONCE);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, fsSettings);
        String tdengineSourceTableDDL = "CREATE TABLE `meters` (" +
                " ts TIMESTAMP," +
                " `current` FLOAT," +
                " voltage INT," +
                " phase FLOAT," +
                " location VARCHAR(255)," +
                " groupid INT," +
                " tbname VARCHAR(255)" +
                ") WITH (" +
                "  'connector' = 'tdengine-connector'," +
                "  'bootstrap.servers' = 'localhost:6041'," +
                "  'td.jdbc.mode' = 'cdc'," +
                "  'group.id' = 'group_22'," +
                "  'auto.offset.reset' = 'earliest'," +
                "  'enable.auto.commit' = 'false'," +
                "  'topic' = 'topic_meters'" +
                ")";


        String tdengineSinkTableDDL = "CREATE TABLE `sink_meters` (" +
                " ts TIMESTAMP," +
                " `current` FLOAT," +
                " voltage INT," +
                " phase FLOAT," +
                " location VARCHAR(255)," +
                " groupid INT," +
                " tbname VARCHAR(255)" +
                ") WITH (" +
                "  'connector' = 'tdengine-connector'," +
                "  'td.jdbc.mode' = 'sink'," +
                "  'td.jdbc.url' = 'jdbc:TAOS-WS://localhost:6041/power_sink?user=root&password=taosdata'," +
                "  'sink.db.name' = 'power_sink'," +
                "  'sink.supertable.name' = 'sink_meters'" +
                ")";

        tableEnv.executeSql(tdengineSourceTableDDL);
        tableEnv.executeSql(tdengineSinkTableDDL);

        TableResult tableResult = tableEnv.executeSql("INSERT INTO sink_meters SELECT ts, `current`, voltage, phase, location, groupid, tbname FROM `meters`");

        Thread.sleep(5000L);
        tableResult.getJobClient().get().cancel().get();
    }
    //ANCHOR_END: cdc_table


}
