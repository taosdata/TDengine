package com.taosdata.tsync.entity;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.enums.JobStatus;
import com.taosdata.tsync.factory.JobFactory;
import com.taosdata.tsync.repository.ConfigurationRepository;
import com.taosdata.tsync.service.ConsumeToFileJobServiceImpl;
import com.taosdata.tsync.service.JobService;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Objects;

public class ConsumeToFileJobTest {

    private JSONObject consumeToFileConfigJSON;

    @Test
    public void testConsumeToFile() {
        // given
        ConfigurationRepository configurationRepository = ConfigurationRepository.getInstance();
        JobService jobService = new ConsumeToFileJobServiceImpl();

        // when build
        Job job = JobFactory.build(ConfigurationType.CONSUME_TO_FILE, consumeToFileConfigJSON, configurationRepository);
        // then
        Assert.assertEquals(JobStatus.INIT, job.getStatus());

        // when
        Configuration configuration = job.getConfiguration(jobService);
        // then
        Assert.assertEquals(ConfigurationType.CONSUME_TO_FILE, configuration.getConfigurationType());

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
        // read consume-to-file.json
        String consumeConfigStr = IOUtils.toString(Objects.requireNonNull(getClass().getClassLoader().getResourceAsStream("consume-to-file.json")));
        consumeToFileConfigJSON = JSONObject.parseObject(consumeConfigStr);
    }
}
