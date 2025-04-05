package com.taosdata.flink.example;

import com.example.ResultBean;
import com.taosdata.flink.common.TDengineConfigParams;
import com.taosdata.flink.sink.TDengineSink;
import com.taosdata.jdbc.TSDBDriver;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.Row;

import java.sql.*;
import java.util.*;

public class Main {
    static String jdbcUrl = "jdbc:TAOS-WS://localhost:6041?user=root&password=taosdata";
    static void prepare() throws ClassNotFoundException, SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        Class.forName("com.taosdata.jdbc.ws.WebSocketDriver");
        try (Connection connection = DriverManager.getConnection(jdbcUrl, properties);
             Statement stmt = connection.createStatement()) {

            stmt.executeUpdate("DROP database IF EXISTS power_sink");
            // create database
            stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS power_sink vgroups 5");

            stmt.executeUpdate("use power_sink");

            // create table
            stmt.executeUpdate("CREATE STABLE IF NOT EXISTS sink_meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);");
            // you can check rowsAffected here

            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS sink_normal (ts timestamp, current float, voltage int, phase float);");
            // you can check rowsAffected here


        } catch (Exception ex) {
            // please refer to the JDBC specifications for detailed exceptions info
            System.out.printf("Failed to create database power_sink or stable meters, %sErrMessage: %s%n",
                    ex instanceof SQLException ? "ErrCode: " + ((SQLException) ex).getErrorCode() + ", " : "",
                    ex.getMessage());
            // Print stack trace for context in examples. Use logging in production.
            throw ex;
        }
    }

    public static void main(String[] args) throws Exception {
        prepare();
        if (args != null && args.length > 0 && args[0].equals("sink")) {
            testRowDataToSuperTable();
            testRowDataToNormalTable();
            testCustomTypeToSink();

        } else if (args != null && args.length > 0 && args[0].equals("table")) {
            testTableSqlToSink();
            testTableRowToSink();
        }
    }

    //ANCHOR: RowDataToSuperTable
    static void testRowDataToSuperTable() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        RowData[] rowDatas = new GenericRowData[10];
        Random random = new Random(System.currentTimeMillis());
        for (int i = 0; i < 10; i++) {
            GenericRowData row = new GenericRowData(7);
            long current = System.currentTimeMillis() + i * 1000;
            row.setField(0, TimestampData.fromEpochMillis(current)); // ts
            row.setField(1, random.nextFloat() * 30); // current
            row.setField(2, 300 + (i + 1)); // voltage
            row.setField(3, random.nextFloat()); // phase
            row.setField(4, StringData.fromString("location_" + i)); // location
            row.setField(5, i); // groupid
            row.setField(6, StringData.fromString("d0" + i)); // tbname
            rowDatas[i] = row;
        }
        DataStream<RowData> dataStream = env.fromElements(RowData.class, rowDatas);

        Properties sinkProps = new Properties();
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        sinkProps.setProperty(TDengineConfigParams.VALUE_DESERIALIZER, "RowData");
        sinkProps.setProperty(TDengineConfigParams.PROPERTY_KEY_DBNAME, "power_sink");
        sinkProps.setProperty(TDengineConfigParams.TD_SUPERTABLE_NAME, "sink_meters");
        sinkProps.setProperty(TDengineConfigParams.TD_JDBC_URL, "jdbc:TAOS-WS://localhost:6041/power_sink?user=root&password=taosdata");
        sinkProps.setProperty(TDengineConfigParams.TD_BATCH_SIZE, "2000");

        TDengineSink<RowData> sink = new TDengineSink<>(sinkProps, Arrays.asList("ts", "current", "voltage", "phase", "location", "groupid", "tbname"));
        dataStream.sinkTo(sink);
        env.execute("flink tdengine sink");
    }
    //ANCHOR_END: RowDataToSuperTable

    //ANCHOR: RowDataToNormalTable
    static void testRowDataToNormalTable() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        RowData[] rowDatas = new GenericRowData[10];
        Random random = new Random(System.currentTimeMillis());
        for (int i = 0; i < 10; i++) {
            GenericRowData row = new GenericRowData(4);
            long current = System.currentTimeMillis() + i * 1000;
            row.setField(0, TimestampData.fromEpochMillis(current)); // ts
            row.setField(1, random.nextFloat() * 30); // current
            row.setField(2, 300 + (i + 1)); // voltage
            row.setField(3, random.nextFloat()); // phase
            rowDatas[i] = row;
        }
        DataStream<RowData> dataStream = env.fromElements(RowData.class, rowDatas);

        Properties sinkProps = new Properties();
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        sinkProps.setProperty(TDengineConfigParams.VALUE_DESERIALIZER, "RowData");
        sinkProps.setProperty(TDengineConfigParams.PROPERTY_KEY_DBNAME, "power_sink");
        sinkProps.setProperty(TDengineConfigParams.TD_TABLE_NAME, "sink_normal");
        sinkProps.setProperty(TDengineConfigParams.TD_JDBC_URL, "jdbc:TAOS-WS://localhost:6041/power_sink?user=root&password=taosdata");
        sinkProps.setProperty(TDengineConfigParams.TD_BATCH_SIZE, "2000");

        TDengineSink<RowData> sink = new TDengineSink<>(sinkProps, Arrays.asList("ts", "current", "voltage", "phase"));
        dataStream.sinkTo(sink);
        env.execute("flink tdengine sink");
    }
    //ANCHOR_END: RowDataToNormalTable

    //ANCHOR: CustomTypeToNormalTable
    static void testCustomTypeToSink() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ResultBean[] rowDatas = new ResultBean[10];
        Random random = new Random(System.currentTimeMillis());
        for (int i = 0; i < 10; i++) {
            ResultBean rowData = new ResultBean();
            long current = System.currentTimeMillis() + i * 1000;
            rowData.setTs(new Timestamp(current));
            rowData.setCurrent(random.nextFloat() * 30);
            rowData.setVoltage(300 + (i + 1));
            rowData.setPhase(random.nextFloat());
            rowData.setLocation("location_" + i);
            rowData.setGroupid(i);
            rowData.setTbname("d0" + i);
            rowDatas[i] = rowData;
        }

        DataStream<ResultBean> dataStream = env.fromElements(ResultBean.class, rowDatas);

        Properties sinkProps = new Properties();
        sinkProps.setProperty(TDengineConfigParams.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        sinkProps.setProperty(TDengineConfigParams.PROPERTY_KEY_CHARSET, "UTF-8");
        sinkProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        sinkProps.setProperty(TDengineConfigParams.VALUE_DESERIALIZER, "com.taosdata.flink.entity.ResultBeanSinkSerializer");
        sinkProps.setProperty(TDengineConfigParams.PROPERTY_KEY_DBNAME, "power_sink");
        sinkProps.setProperty(TDengineConfigParams.TD_SUPERTABLE_NAME, "sink_meters");
        sinkProps.setProperty(TDengineConfigParams.TD_JDBC_URL, "jdbc:TAOS-WS://localhost:6041/power_sink?user=root&password=taosdata");
        sinkProps.setProperty(TDengineConfigParams.TD_BATCH_SIZE, "2000");

        TDengineSink<ResultBean> sink = new TDengineSink<>(sinkProps, Arrays.asList("ts", "current", "voltage", "phase", "location", "groupid", "tbname"));
        dataStream.sinkTo(sink);
        env.execute("flink tdengine sink");
    }
    //ANCHOR_END: CustomTypeToNormalTable

    //ANCHOR: TableSqlToSink
    static void testTableSqlToSink() throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);
    
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
        tEnv.executeSql(tdengineSinkTableDDL);

        String insertQuery = "INSERT INTO sink_meters " +
                "VALUES " +
                "(CAST('2024-12-19 19:12:45' AS TIMESTAMP(6)), 50.30000, 201, 3.31003, 'California.SanFrancisco', 1, 'd1001')," +
                "(CAST('2024-12-19 19:12:46' AS TIMESTAMP(6)), 82.60000, 202, 0.33000, 'California.SanFrancisco', 1, 'd1001')," +
                "(CAST('2024-12-19 19:12:47' AS TIMESTAMP(6)), 92.30000, 203, 0.31000, 'California.SanFrancisco', 1, 'd1001')," +
                "(CAST('2024-12-19 19:12:45' AS TIMESTAMP(6)), 50.30000, 204, 3.25003, 'Alabama.Montgomery', 2, 'd1002')," +
                "(CAST('2024-12-19 19:12:46' AS TIMESTAMP(6)), 62.60000, 205, 0.33000, 'Alabama.Montgomery', 2, 'd1002')," +
                "(CAST('2024-12-19 19:12:47' AS TIMESTAMP(6)), 72.30000, 206, 0.31000, 'Alabama.Montgomery', 2, 'd1002');";

        TableResult tableResult = tEnv.executeSql(insertQuery);
        tableResult.await();
    }
    //ANCHOR_END: TableSqlToSink

   //ANCHOR: NormalTableSqlToSink
    static void testNormalTableSqlToSink() throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);
    
        String tdengineSinkTableDDL = "CREATE TABLE `sink_normal` (" +
                " ts TIMESTAMP," +
                " `current` FLOAT," +
                " voltage INT," +
                " phase FLOAT"
                ") WITH (" +
                "  'connector' = 'tdengine-connector'," +
                "  'td.jdbc.mode' = 'sink'," +
                "  'td.jdbc.url' = 'jdbc:TAOS-WS://localhost:6041/power_sink?user=root&password=taosdata'," +
                "  'sink.db.name' = 'power_sink'," +
                "  'sink.table.name' = 'sink_normal'" +
                ")";
        tEnv.executeSql(tdengineSinkTableDDL);

        String insertQuery = "INSERT INTO sink_normal " +
                "VALUES " +
                "(CAST('2024-12-19 19:12:45' AS TIMESTAMP(6)), 50.30000, 201, 3.31003)," +
                "(CAST('2024-12-19 19:12:46' AS TIMESTAMP(6)), 82.60000, 202, 0.33000)," +
                "(CAST('2024-12-19 19:12:47' AS TIMESTAMP(6)), 92.30000, 203, 0.31000)," +
                "(CAST('2024-12-19 19:12:45' AS TIMESTAMP(6)), 50.30000, 204, 3.25003)," +
                "(CAST('2024-12-19 19:12:46' AS TIMESTAMP(6)), 62.60000, 205, 0.33000)," +
                "(CAST('2024-12-19 19:12:47' AS TIMESTAMP(6)), 72.30000, 206, 0.31000);";

        TableResult tableResult = tEnv.executeSql(insertQuery);
        tableResult.await();
    }
    //ANCHOR_END: NormalTableSqlToSink

    //ANCHOR: TableRowToSink
    static void testTableRowToSink() throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

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
        tEnv.executeSql(tdengineSinkTableDDL);

        int sum = 0;
        String tbname = "d001";
        int groupId = 1;
        String location = "California.SanFrancisco";
        List<Row> rowDatas = new ArrayList<>();
        Random random = new Random(System.currentTimeMillis());
        for (int i = 0; i < 50; i++) {
            sum += 300 + (i + 1);
            long timestampInMillis = System.currentTimeMillis() + i * 1000;
            Row row = Row.of(
                    new Timestamp(timestampInMillis), // ts
                    random.nextFloat() * 30, // current
                    300 + (i + 1), // voltage
                    random.nextFloat(), // phase
                    location,
                    groupId,
                    tbname

            );
            rowDatas.add(row);
        }
        
        Table inputTable = tEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("ts", DataTypes.TIMESTAMP(6)),
                        DataTypes.FIELD("current", DataTypes.FLOAT()),
                        DataTypes.FIELD("voltage", DataTypes.INT()),
                        DataTypes.FIELD("phase", DataTypes.FLOAT()),
                        DataTypes.FIELD("location", DataTypes.STRING()),
                        DataTypes.FIELD("groupid", DataTypes.INT()),
                        DataTypes.FIELD("tbname", DataTypes.STRING())
                ),
                rowDatas
        );

        TableResult result = inputTable.executeInsert("sink_meters");
        result.await(); // waiting for task completion
    }
    //ANCHOR_END: TableRowToSink


}
