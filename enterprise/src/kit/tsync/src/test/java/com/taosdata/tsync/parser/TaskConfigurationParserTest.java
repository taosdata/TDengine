package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSON;
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
        Assert.assertEquals("tq_test", configuration.getTopic());

        int[] partitions = configuration.getPartitions();
        Assert.assertEquals(10, partitions.length);
        Assert.assertEquals(1, partitions[0]);
        Assert.assertEquals(2, partitions[1]);
        Assert.assertEquals(3, partitions[2]);
        Assert.assertEquals(4, partitions[3]);
        Assert.assertEquals(5, partitions[4]);
        Assert.assertEquals(6, partitions[5]);
        Assert.assertEquals(7, partitions[6]);
        Assert.assertEquals(8, partitions[7]);
        Assert.assertEquals(9, partitions[8]);
        Assert.assertEquals(10, partitions[9]);

        Assert.assertEquals(10, configuration.getThreads());
    }

    @Before
    public void before() throws IOException {
        json = IOUtils.toString(Objects.requireNonNull(getClass().getClassLoader().getResourceAsStream("task.json")));
    }

}