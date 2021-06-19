package com.taosdata.tsync.entity;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.enums.JobStatus;
import com.taosdata.tsync.factory.JobFactory;
import com.taosdata.tsync.repository.ConfigurationRepository;
import com.taosdata.tsync.service.AffectRowsProcessService;
import com.taosdata.tsync.service.JobService;
import com.taosdata.tsync.service.ProduceToTQueueJobServiceImpl;
import com.taosdata.tsync.service.ResultProcessService;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Objects;

public class FileToTQueueTest {

    private JSONObject producerTaskConfigJSON;

    @Test
    public void runProduceJob() {
        // given
        ConfigurationRepository configurationRepository = ConfigurationRepository.getInstance();
        ResultProcessService resultProcessService = new AffectRowsProcessService();
        JobService jobService = new ProduceToTQueueJobServiceImpl(resultProcessService);

        // when
        Job job = JobFactory.build(ConfigurationType.PRODUCE_TO_TQUEUE, producerTaskConfigJSON, configurationRepository);
        // then
        Assert.assertEquals(JobStatus.INIT, job.getStatus());

        // when
        Configuration configuration = job.getConfiguration(jobService);
        // then
        Assert.assertEquals(ConfigurationType.PRODUCE_TO_TQUEUE, configuration.getConfigurationType());

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
        // read produce-to-tqueue.json
        String producerConfigStr = IOUtils.toString(Objects.requireNonNull(getClass().getClassLoader().getResourceAsStream("file-to-tqueue.json")));
        producerTaskConfigJSON = JSONObject.parseObject(producerConfigStr);
    }
}
