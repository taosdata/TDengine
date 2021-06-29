package com.taosdata.tsync.entity;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.enums.JobStatus;
import com.taosdata.tsync.factory.JobFactory;
import com.taosdata.tsync.repository.ConfigurationRepository;
import com.taosdata.tsync.service.*;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Objects;

public class NetToTQueueJobTest {

    private JSONObject netToTQueueConfigJSON;

    @Test
    public void runProduceJob() {
        // given
        JobService jobService = new NetToTQueueJobServiceImpl();

        // when
        ConfigurationRepository configurationRepository = ConfigurationRepository.getInstance();
        Job job = JobFactory.build(ConfigurationType.NET_TO_TQUEUE, netToTQueueConfigJSON, configurationRepository);
        // then
        Assert.assertEquals(JobStatus.INIT, job.getStatus());

        // when
        Configuration configuration = job.getConfiguration(jobService);
        // then
        Assert.assertEquals(ConfigurationType.NET_TO_TQUEUE, configuration.getConfigurationType());

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
        String producerConfigStr = IOUtils.toString(Objects.requireNonNull(getClass().getClassLoader().getResourceAsStream("net-to-tqueue.json")));
        netToTQueueConfigJSON = JSONObject.parseObject(producerConfigStr);
    }
}