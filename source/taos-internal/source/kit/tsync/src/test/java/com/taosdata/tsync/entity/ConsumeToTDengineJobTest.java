package com.taosdata.tsync.entity;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.enums.JobStatus;
import com.taosdata.tsync.factory.JobFactory;
import com.taosdata.tsync.repository.ConfigurationRepository;
import com.taosdata.tsync.service.ConsumeToTDengineJobServiceImpl;
import com.taosdata.tsync.service.JobService;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
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
    }

}
