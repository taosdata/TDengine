package com.taosdata.tsync.entity;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.*;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.enums.JobStatus;
import com.taosdata.tsync.factory.ConfigurationFactory;
import com.taosdata.tsync.factory.ConsumeJobFactory;
import com.taosdata.tsync.factory.ProduceJobFactory;
import com.taosdata.tsync.repository.ConfigurationRepository;
import com.taosdata.tsync.service.*;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

public class JobTest {

    private JSONObject producerTaskConfigJSON;
    private JSONObject consumerTaskConfigJSON;

    @Test
    public void runProduceJob() {
        // given
        ConfigurationRepository configurationRepository = ConfigurationRepository.getInstance();
        ResultProcessService resultProcessService = new AffectRowsProcessService();
        JobService jobService = new ProduceJobServiceImpl(resultProcessService);

        // when
        Job job = ProduceJobFactory.build(producerTaskConfigJSON, configurationRepository);
        // then
        Assert.assertEquals(JobStatus.INIT, job.getStatus());

        // when
        Configuration configuration = job.getConfiguration(jobService);
        // then
        Assert.assertEquals(ConfigurationType.PRODUCE_JOB, configuration.getConfigurationType());

        // when
        job.prepare(jobService);
        // then
        Assert.assertEquals(JobStatus.PREPARED, job.getStatus());

        // when
        job.execute(jobService);
        // then
        Assert.assertEquals(JobStatus.COMPLETED, job.getStatus());
    }

    @Test
    public void runConsumeJob() {
        cleanTaosdAndTqueue();

        // given
        ConfigurationRepository configurationRepository = ConfigurationRepository.getInstance();
        JobService jobService = new ConsumeJobServiceImpl();

        // when
        Job job = ConsumeJobFactory.build(consumerTaskConfigJSON, configurationRepository);
        // then
        Assert.assertEquals(JobStatus.INIT, job.getStatus());

        // when
        Configuration configuration = job.getConfiguration(jobService);
        // then
        Assert.assertEquals(ConfigurationType.CONSUME_JOB, configuration.getConfigurationType());

        // when
        job.prepare(jobService);
        // then
        Assert.assertEquals(JobStatus.PREPARED, job.getStatus());

        // when
        job.execute(jobService);
        // then
        Assert.assertEquals(JobStatus.COMPLETED, job.getStatus());
    }

    @Before
    public void before() throws IOException {
        // read producer-job.json
        String producerConfigStr = IOUtils.toString(Objects.requireNonNull(getClass().getClassLoader().getResourceAsStream("producer-job.json")));
        producerTaskConfigJSON = JSONObject.parseObject(producerConfigStr);

        // read consumer-job.json
        String consumerConfigStr = IOUtils.toString(Objects.requireNonNull(getClass().getClassLoader().getResourceAsStream("consumer-job.json")));
        consumerTaskConfigJSON = JSONObject.parseObject(consumerConfigStr);
    }

    private void cleanTaosdAndTqueue() {
        ConsumeJobConfiguration consumeJobConfiguration = (ConsumeJobConfiguration) ConfigurationFactory.build(ConfigurationType.CONSUME_JOB, consumerTaskConfigJSON);
        ConsumerConfiguration consumerConfiguration = (ConsumerConfiguration) consumeJobConfiguration.findFirst(ConfigurationType.CONSUMER);
        String host_tq = consumerConfiguration.getHost();
        TaskConfiguration taskConfiguration = (TaskConfiguration) consumeJobConfiguration.findFirst(ConfigurationType.TASK);
        String topic = taskConfiguration.getTopic();
        TaosdConfiguration taosdConfiguration = (TaosdConfiguration) consumeJobConfiguration.findFirst(ConfigurationType.TAOSD);
        String host_td = taosdConfiguration.getHost();
        DatabaseConfiguration databaseConfiguration = (DatabaseConfiguration) consumeJobConfiguration.findFirst(ConfigurationType.DATABASE);
        String dbname = databaseConfiguration.getName();

        try {
            Connection taosdConnection = DriverManager.getConnection("jdbc:TAOS-RS://" + host_td + ":6041/?user=root&password=taosdata");
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
    }

}