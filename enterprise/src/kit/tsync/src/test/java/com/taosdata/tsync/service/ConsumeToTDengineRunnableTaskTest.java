package com.taosdata.tsync.service;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Range;
import com.taosdata.tsync.tqueue.TQueueConsumer;
import com.taosdata.tsync.tqueue.TQueueProducer;
import com.taosdata.tsync.entity.config.DatabaseConfiguration;
import com.taosdata.tsync.entity.config.SchemaConfiguration;
import com.taosdata.tsync.entity.config.StableConfiguration;
import com.taosdata.tsync.entity.ConsumerConfig;
import com.taosdata.tsync.entity.ProducerConfig;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.enums.SchemaMissingStrategy;
import com.taosdata.tsync.factory.ConfigurationFactory;
import com.taosdata.tsync.factory.ConsumeToTDengineRunnableTaskFactory;
import com.taosdata.tsync.factory.ProduceToTQueueCallableTaskFactory;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.*;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ConsumeToTDengineRunnableTaskTest {

    private String host_tq = "192.168.17.156";
    private String host_td = "192.168.17.82";
    private String topic = "tq_test";
    private Connection taosdConnection;
    private SchemaMissingStrategy schemaMissingStrategy = SchemaMissingStrategy.CREATE;
    private TQueueConsumer consumer;
    private SchemaConfiguration schemaConfiguration;
    private String dbname;
    private String stableName;

    @Test
    public void test() {
        // given
        final List<Integer> partitionsToWrite = IntStream.of(1, 2, 3).boxed().collect(Collectors.toList());
        final int pollingInterval = 1000;
        final long recordSize = 1000L;

        ConsumeToTDengineRunnableTask runnable = new ConsumeToTDengineRunnableTaskFactory()
                .setConsumer(consumer)
                .setTopic(topic)
                .setPartitionsToWrite(partitionsToWrite)
                .setTaosdConnection(taosdConnection)
                .setPollingInterval(pollingInterval)
                .build();

        // when
        Thread thread = new Thread(runnable, "consume-thread");
        thread.start();
        long affectedRows = 0;
        try {
            // wait few seconds
            TimeUnit.SECONDS.sleep(5);
            // produce few data to tq
            affectedRows = produceFewData(recordSize);
            // wait consume job
            thread.join(1000 * 10);
            // interrupt thread
            thread.interrupt();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        // then
        long rowInDatabase = queryCountFromDatabase();
        Assert.assertEquals(recordSize, affectedRows);
        Assert.assertEquals(recordSize, rowInDatabase);
    }

    private long queryCountFromDatabase() {
        long rowsInDb = 0;
        try (Statement stmt = taosdConnection.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + dbname + "." + stableName);
            while (rs.next()) {
                rowsInDb = rs.getLong("count(*)");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return rowsInDb;
    }

    private long produceFewData(long recordSize) throws InterruptedException, ExecutionException {
        List<Integer> partitionsToWrite = IntStream.of(1).boxed().collect(Collectors.toList());
        Range<Long> tablesToWrite = Range.openClosed(1L, 101L);
        long records = recordSize;
        long batchTables = 10L;
        long batchValues = 10L;
        Properties props = new Properties();
        props.setProperty(ProducerConfig.HOST_CONFIG, host_tq);
        TQueueProducer producer = new TQueueProducer<>(props);

        ProduceToTQueueCallableTask callable = new ProduceToTQueueCallableTaskFactory()
                .setProducer(producer)
                .setTopic(topic)
                .setPartitionsToWrite(partitionsToWrite)
                .setTablesToWrite(tablesToWrite)
                .setRecordsToWrite(records)
                .setBatchTables(batchTables)
                .setBatchValues(batchValues)
                .setSchemaConfiguration(schemaConfiguration)
                .build();
        FutureTask<Long> task = new FutureTask<>(callable);
        Thread thread = new Thread(task);
        thread.start();
        thread.join();
        return task.get();
    }

    @Before
    public void before() {
        try {
            String producerConfigStr = IOUtils.toString(Objects.requireNonNull(getClass().getClassLoader().getResourceAsStream("schema.json")));
            JSONObject schemaJSON = JSONObject.parseObject(producerConfigStr);
            schemaConfiguration = (SchemaConfiguration) ConfigurationFactory.build(ConfigurationType.SCHEMA, schemaJSON);
            assert schemaConfiguration != null;
            DatabaseConfiguration dbConfiguration = (DatabaseConfiguration) schemaConfiguration.findFirst(ConfigurationType.DATABASE);
            dbname = dbConfiguration.getName();
            StableConfiguration stableConfiguration = (StableConfiguration) schemaConfiguration.findFirst(ConfigurationType.STABLE);
            stableName = stableConfiguration.getName();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            taosdConnection = DriverManager.getConnection("jdbc:TAOS-RS://" + host_td + ":6041/?user=root&password=taosdata");
            Statement stmt1 = taosdConnection.createStatement();
            stmt1.execute("drop database if exists " + dbname);
            stmt1.close();

            Connection tqueueConnection = DriverManager.getConnection("jdbc:TAOS-RS://" + host_tq + ":6041/?user=root&password=tqueue");
            Statement stmt2 = tqueueConnection.createStatement();
            stmt2.execute("drop topic if exists " + topic);
            stmt2.execute("create topic if not exists " + topic + " partitions 10");
            stmt2.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.HOST_CONFIG, host_tq);
        consumer = new TQueueConsumer(props);
    }

}