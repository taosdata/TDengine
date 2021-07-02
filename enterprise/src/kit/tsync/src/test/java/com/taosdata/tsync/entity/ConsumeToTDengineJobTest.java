package com.taosdata.tsync.entity;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.*;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.enums.JobStatus;
import com.taosdata.tsync.factory.ConfigurationFactory;
import com.taosdata.tsync.factory.JobFactory;
import com.taosdata.tsync.repository.ConfigurationRepository;
import com.taosdata.tsync.service.ConsumeToTDengineJobServiceImpl;
import com.taosdata.tsync.service.JobService;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

public class ConsumeToTDengineJobTest {

    private JSONObject configJson;

    @Test
    public void runConsumeJob() {
        // given
        ConfigurationRepository configurationRepository = ConfigurationRepository.getInstance();
        JobService jobService = new ConsumeToTDengineJobServiceImpl();

        // when
        Job job = JobFactory.build(ConfigurationType.CONSUME_TO_TDENGINE, configJson, configurationRepository);
        // then
        Assert.assertEquals(JobStatus.INIT, job.getStatus());

        // when
        Configuration configuration = job.getConfiguration(jobService);
        // then
        Assert.assertEquals(ConfigurationType.CONSUME_TO_TDENGINE, configuration.getConfigurationType());

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
        String consumerConfigStr = IOUtils.toString(Objects.requireNonNull(getClass().getClassLoader().getResourceAsStream("consume-to-tdengine.json")));
        configJson = JSONObject.parseObject(consumerConfigStr);
        cleanTaosdAndTqueue();
    }

    private void cleanTaosdAndTqueue() {
        ConsumeToTDengineConfiguration consumeToTDengineConfiguration = (ConsumeToTDengineConfiguration) ConfigurationFactory.build(ConfigurationType.CONSUME_TO_TDENGINE, configJson);
        ConsumerConfiguration consumerConfiguration = (ConsumerConfiguration) consumeToTDengineConfiguration.findFirst(ConfigurationType.CONSUMER);
        String host_tq = consumerConfiguration.getHost();
        TaskConfiguration taskConfiguration = (TaskConfiguration) consumeToTDengineConfiguration.findFirst(ConfigurationType.TASK);
        String topic = taskConfiguration.getTopic();
        TaosdConfiguration taosdConfiguration = (TaosdConfiguration) consumeToTDengineConfiguration.findFirst(ConfigurationType.TAOSD);
        String host_td = taosdConfiguration.getHost();
        DatabaseConfiguration databaseConfiguration = (DatabaseConfiguration) consumeToTDengineConfiguration.findFirst(ConfigurationType.DATABASE);
        String dbname = databaseConfiguration.getName();

        try {
            Connection taosdConnection = DriverManager.getConnection("jdbc:TAOS-RS://" + host_td + ":6041/?user=root&password=taosdata");
            Statement stmt1 = taosdConnection.createStatement();
            stmt1.execute("drop database if exists " + dbname);
            stmt1.close();

            Connection tqueueConnection = DriverManager.getConnection("jdbc:TAOS-RS://" + host_tq + ":6041/?user=root&password=tqueue");
            Statement stmt2 = tqueueConnection.createStatement();
            stmt2.execute("drop database if exists topic_info");
            stmt2.execute("drop topic if exists " + topic);
            stmt2.execute("create topic if not exists " + topic + " partitions 10");
            stmt2.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
