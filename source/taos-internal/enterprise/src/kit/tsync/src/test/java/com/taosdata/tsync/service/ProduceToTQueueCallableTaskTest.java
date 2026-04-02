package com.taosdata.tsync.service;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Range;
import com.taosdata.tsync.entity.ProducerConfig;
import com.taosdata.tsync.entity.config.SchemaConfiguration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.factory.ConfigurationFactory;
import com.taosdata.tsync.factory.ProduceToTQueueCallableTaskFactory;
import com.taosdata.tsync.tqueue.TQueueProducer;
import com.taosdata.tsync.utils.SqlUtil;
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ProduceToTQueueCallableTaskTest {

    private static final String host_tq = "192.168.17.156";
    private String topic = "tq_test";
    private TQueueProducer producer;
    private JSONObject schemaJSON;

    @Test
    public void buildSql() {


    }

    @Test
    public void run() throws ExecutionException, InterruptedException {
        // given
        int[] partitionsToWrite = new int[]{1};
        Range<Long> tablesToWrite = Range.openClosed(1L, 101L);
        long records = 1000L;
        long batchTables = 10L;
        long batchValues = 10L;
        final SchemaConfiguration schemaConfiguration = (SchemaConfiguration) ConfigurationFactory.build(ConfigurationType.SCHEMA, schemaJSON);

        // when
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
        long affectRows = task.get();

        // then
        long countFromTQueue = queryCountFromTQueue();
        Assert.assertEquals(1000L, affectRows);
        Assert.assertEquals(10L, countFromTQueue);
    }

    private long queryCountFromTQueue() {
        int count = 0;
        try {
            ResultSet rs = SqlUtil.executeQuery(host_tq, "", "root", "tqueue", "select count( *) from " + topic + ".ps ");
            rs.next();
            count = rs.getInt("count(*)");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return count;
    }

    @Before
    public void before() {
        try {
            Connection connection = DriverManager.getConnection("jdbc:TAOS-RS://" + host_tq + ":6041/?user=root&password=tqueue");
            Statement stmt = connection.createStatement();
            stmt.execute("drop database if exists " + topic);
            stmt.execute("create topic if not exists " + topic + " partitions 10");
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        Properties props = new Properties();
        props.setProperty(ProducerConfig.HOST_CONFIG, host_tq);
        producer = new TQueueProducer<>(props);
        try {
            String producerConfigStr = IOUtils.toString(Objects.requireNonNull(getClass().getClassLoader().getResourceAsStream("schema.json")));
            schemaJSON = JSONObject.parseObject(producerConfigStr);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}