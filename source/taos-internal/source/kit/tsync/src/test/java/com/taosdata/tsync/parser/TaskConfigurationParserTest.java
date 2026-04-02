package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.TaskConfiguration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.factory.ConfigurationFactory;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Objects;


public class TaskConfigurationParserTest {

    private String json;

    @Test
    public void parseTask() {
        // given
        JSONObject taskJSON = JSONObject.parseObject(json);
        // when
        TaskConfiguration configuration = (TaskConfiguration) ConfigurationFactory.build(ConfigurationType.TASK, taskJSON);
        // then
        assert configuration != null;
        Assert.assertEquals("tq_test", configuration.getTopic());

        int[] partitions = configuration.getPartitions();
        Assert.assertEquals(1000, partitions.length);

        Assert.assertEquals(1, partitions[0]);
        Assert.assertEquals(1000, partitions[999]);

        Assert.assertEquals(10, configuration.getThreads());

        Assert.assertEquals(1628747171000000L, configuration.getStartOffset());
    }

    @Before
    public void before() throws IOException {
        json = IOUtils.toString(Objects.requireNonNull(getClass().getClassLoader().getResourceAsStream("task.json")));
    }

}