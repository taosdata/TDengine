package com.taosdata.tsync.entity;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.enums.JobStatus;
import com.taosdata.tsync.factory.ProduceJobFactory;
import com.taosdata.tsync.repository.CallableTaskRepository;
import com.taosdata.tsync.repository.ConfigurationRepository;
import com.taosdata.tsync.service.AffectRowsProcessService;
import com.taosdata.tsync.service.JobService;
import com.taosdata.tsync.service.ProduceJobServiceImpl;
import com.taosdata.tsync.service.ResultProcessService;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

public class JobTest {

    private JSONObject configJSON;

    @Test
    public void runProduceJob() {
        // given
        ConfigurationRepository configurationRepository = ConfigurationRepository.getInstance();
        CallableTaskRepository callableTaskRepository = CallableTaskRepository.getInstance();
        ResultProcessService resultProcessService = new AffectRowsProcessService();
        JobService jobService = new ProduceJobServiceImpl(configurationRepository, callableTaskRepository, resultProcessService);

        // when
        Job job = ProduceJobFactory.build(configJSON, configurationRepository);
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

        // when

        // then
    }

    @Before
    public void before() throws IOException {
        // read config file
        InputStream is = getClass().getClassLoader().getResourceAsStream("producer-task.json");
        String configStr = IOUtils.toString(is);
        configJSON = JSONObject.parseObject(configStr);
    }
}