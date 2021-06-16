package com.taosdata.tsync.entity;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.enums.JobStatus;
import com.taosdata.tsync.factory.ConsumeJobFactory;
import com.taosdata.tsync.factory.ProduceJobFactory;
import com.taosdata.tsync.repository.ConfigurationRepository;
import com.taosdata.tsync.service.*;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

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
        String producerConfigStr = IOUtils.toString(getClass().getClassLoader().getResourceAsStream("producer-job.json"));
        producerTaskConfigJSON = JSONObject.parseObject(producerConfigStr);

        // read consumer-job.json
        String consumerConfigStr = IOUtils.toString(getClass().getClassLoader().getResourceAsStream("consumer-job.json"));
        consumerTaskConfigJSON = JSONObject.parseObject(consumerConfigStr);
    }

}