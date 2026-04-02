package com.taosdata.tsync.entity;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.enums.JobStatus;
import com.taosdata.tsync.factory.JobFactory;
import com.taosdata.tsync.repository.ConfigurationRepository;
import com.taosdata.tsync.service.ConsumeToNetJobServiceImpl;
import com.taosdata.tsync.service.JobService;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Objects;

public class ConsumeToNetJobTest {

    private JSONObject consumeToNetConfigurationJSON;

    @Test
    public void testConsumeToFile() {
        // given
        JobService jobService = new ConsumeToNetJobServiceImpl();

        // when build
        ConfigurationRepository configurationRepository = ConfigurationRepository.getInstance();
        Job job = JobFactory.build(ConfigurationType.CONSUME_TO_NET, consumeToNetConfigurationJSON, configurationRepository);
        // then
        Assert.assertEquals(JobStatus.INIT, job.getStatus());

        // when
        Configuration configuration = job.getConfiguration(jobService);
        // then
        Assert.assertEquals(ConfigurationType.CONSUME_TO_NET, configuration.getConfigurationType());

        // when prepare
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
        // read consume-to-net.json
        String consumeConfigStr = IOUtils.toString(Objects.requireNonNull(getClass().getClassLoader().getResourceAsStream("consume-to-net.json")));
        consumeToNetConfigurationJSON = JSONObject.parseObject(consumeConfigStr);
    }
}
