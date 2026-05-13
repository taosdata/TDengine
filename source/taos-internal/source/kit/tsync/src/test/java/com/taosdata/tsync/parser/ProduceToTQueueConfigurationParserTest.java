package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.entity.config.ProduceToTQueueConfiguration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.factory.ConfigurationFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class ProduceToTQueueConfigurationParserTest {
    private String jsonStr = "{\"producer\":{\"host\":\"192.168.17.156\",\"port\":6041,\"user\":\"root\",\"password\":\"taosdata\",\"charset\":\"UTF-8\",\"locale\":\"en_US.UTF-8\",\"timezone\":\"UTC-8\",\"serializer\":\"STRING\"},\"task\":{\"threads\":10,\"topic\":\"tq_test\",\"partitions\":[1,2,3]},\"message\":{\"total\":100,\"batchTables\":10,\"batchValues\":10,\"schema\":{\"database\":{\"name\":\"test\",\"precision\":\"ms\"},\"stable\":{\"name\":\"weather\",\"tables\":10,\"columns\":[{\"name\":\"ts\",\"type\":\"timestamp\"},{\"name\":\"temperature\",\"type\":\"float\"},{\"name\":\"humidity\",\"type\":\"int\"}],\"tags\":[{\"name\":\"loc\",\"type\":\"binary\",\"length\":64},{\"name\":\"groupId\",\"type\":\"int\"}]}}}}";
    private JSONObject configJSON;

    @Before
    public void before() {
        configJSON = JSONObject.parseObject(jsonStr);
    }

    @Test
    public void parseProduceJobConfiguration() {
        // when
        ProduceToTQueueConfiguration configuration = (ProduceToTQueueConfiguration) ConfigurationFactory.build(ConfigurationType.PRODUCE_TO_TQUEUE, configJSON);
        // then
        List<Configuration> producers = configuration.find(ConfigurationType.PRODUCER);
        Assert.assertEquals(1, producers.size());
        List<Configuration> tasks = configuration.find(ConfigurationType.TASK);
        Assert.assertEquals(1, tasks.size());
        List<Configuration> messages = configuration.find(ConfigurationType.MESSAGE);
        Assert.assertEquals(1, messages.size());
        List<Configuration> schemas = configuration.find(ConfigurationType.SCHEMA);
        Assert.assertEquals(1, schemas.size());
        List<Configuration> databases = configuration.find(ConfigurationType.DATABASE);
        Assert.assertEquals(1, databases.size());
        List<Configuration> stables = configuration.find(ConfigurationType.STABLE);
        Assert.assertEquals(1, stables.size());
        List<Configuration> columns = configuration.find(ConfigurationType.COLUMN);
        Assert.assertEquals(3, columns.size());
        List<Configuration> tags = configuration.find(ConfigurationType.TAG);
        Assert.assertEquals(2, tags.size());
    }

}