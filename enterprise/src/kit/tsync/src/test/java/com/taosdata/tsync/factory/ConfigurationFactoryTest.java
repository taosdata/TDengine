package com.taosdata.tsync.factory;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.Configuration;
import com.taosdata.tsync.entity.ConfigurationType;
import com.taosdata.tsync.entity.produceJob.*;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class ConfigurationFactoryTest {
    private JSONObject configJSON;

    @Before
    public void before() throws IOException {
        InputStream is = ConfigurationFactoryTest.class.getClassLoader().getResourceAsStream("producer-task.json");
        String configStr = IOUtils.toString(is);
        configJSON = JSONObject.parseObject(configStr);
    }

    @Test
    public void parseProducerConfiguration() {
        // given
        JSONObject producerJSON = configJSON.getJSONObject("producer");
        // when
        ProducerConfiguration configuration = (ProducerConfiguration) ConfigurationFactory.build(ConfigurationType.PRODUCER, producerJSON);
        // then
        Assert.assertEquals("192.168.17.156", configuration.getHost());
        Assert.assertEquals(new Integer(6041), configuration.getPort());
        Assert.assertEquals("root", configuration.getUser());
        Assert.assertEquals("taosdata", configuration.getPassword());
        Assert.assertEquals("UTF-8", configuration.getCharset());
        Assert.assertEquals("en_US.UTF-8", configuration.getLocale());
        Assert.assertEquals("UTC-8", configuration.getTimezone());
        Assert.assertEquals("STRING", configuration.getSerializer());
    }

    @Test
    public void parseTaskConfiguration() {
        // given
        JSONObject taskJSON = configJSON.getJSONObject("task");
        // when
        TaskConfiguration configuration = (TaskConfiguration) ConfigurationFactory.build(ConfigurationType.TASK, taskJSON);
        // then
        Assert.assertEquals(10, configuration.getThreads());
        Assert.assertEquals("tq_test", configuration.getTopic());
        int[] partitions = configuration.getPartitions();
        Assert.assertEquals(3, partitions.length);
        Assert.assertEquals(1, partitions[0]);
        Assert.assertEquals(2, partitions[1]);
        Assert.assertEquals(3, partitions[2]);
    }

    @Test
    public void parseColumnConfiguration() {
        // given
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("name", "ts");
        jsonObject.put("type", "timestamp");
        jsonObject.put("length", 8);
        // when
        ColumnConfiguration columnConfiguration = (ColumnConfiguration) ConfigurationFactory.build(ConfigurationType.COLUMN, jsonObject);
        // then
        Assert.assertEquals("ts", columnConfiguration.getName());
        Assert.assertEquals("timestamp", columnConfiguration.getType());
        Assert.assertEquals(new Integer(8), columnConfiguration.getLength());
    }

    @Test
    public void parseTagConfiguration() {
        // given
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("name", "loc");
        jsonObject.put("type", "binary");
        jsonObject.put("length", 64);
        // when
        TagConfiguration tagConfiguration = (TagConfiguration) ConfigurationFactory.build(ConfigurationType.TAG, jsonObject);
        // then
        Assert.assertEquals("loc", tagConfiguration.getName());
        Assert.assertEquals("binary", tagConfiguration.getType());
        Assert.assertEquals(new Integer(64), tagConfiguration.getLength());
    }

    @Test
    public void parseStableConfiguration() {
        // given
        JSONObject stableJSON = configJSON.getJSONObject("message").getJSONObject("schema").getJSONObject("stable");
        // when
        StableConfiguration configuration = (StableConfiguration) ConfigurationFactory.build(ConfigurationType.STABLE, stableJSON);
        // then
        Assert.assertEquals("weather", configuration.getName());
        Assert.assertEquals(new Integer(10), configuration.getTables());
        List<Configuration> columns = configuration.find(ConfigurationType.COLUMN);
        Assert.assertEquals(3, columns.size());
        ColumnConfiguration column = (ColumnConfiguration) columns.get(2);
        Assert.assertEquals("humidity", column.getName());
        Assert.assertEquals("int", column.getType());
    }

    @Test
    public void parseDatabaseConfiguration() {
        // given
        JSONObject databaseJSON = configJSON.getJSONObject("message").getJSONObject("schema").getJSONObject("database");
        // when
        DatabaseConfiguration configuration = (DatabaseConfiguration) ConfigurationFactory.build(ConfigurationType.DATABASE, databaseJSON);
        // then
        Assert.assertEquals("test", configuration.getName());
        Assert.assertEquals("ms", configuration.getPrecision());
    }

    @Test
    public void parseSchemaConfiguration() {
        // given
        JSONObject schemaJSON = configJSON.getJSONObject("message").getJSONObject("schema");
        // when
        SchemaConfiguration configuration = (SchemaConfiguration) ConfigurationFactory.build(ConfigurationType.SCHEMA, schemaJSON);
        // then
        // assert database
        List<Configuration> databases = configuration.find(ConfigurationType.DATABASE);
        Assert.assertEquals(1, databases.size());
        DatabaseConfiguration database = (DatabaseConfiguration) databases.get(0);
        Assert.assertEquals("test", database.getName());
        Assert.assertEquals("ms", database.getPrecision());
        // assert stable
        List<Configuration> stables = configuration.find(ConfigurationType.STABLE);
        Assert.assertEquals(1, stables.size());
        StableConfiguration stable = (StableConfiguration) stables.get(0);
        Assert.assertEquals("weather", stable.getName());
        Assert.assertEquals(new Integer(10), stable.getTables());
        // assert tags
        List<Configuration> tags = stable.find(ConfigurationType.TAG);
        Assert.assertEquals(2, tags.size());
        TagConfiguration tag = (TagConfiguration) tags.get(1);
        Assert.assertEquals("groupId", tag.getName());
        Assert.assertEquals("int", tag.getType());
    }

    @Test
    public void parseMessageConfiguration() {
        // given
        JSONObject messageJSON = configJSON.getJSONObject("message");
        // when
        MessageConfiguration configuration = (MessageConfiguration) ConfigurationFactory.build(ConfigurationType.MESSAGE, messageJSON);
        // then
        Assert.assertEquals(new Long(100), configuration.getTotal());
        Assert.assertEquals(new Long(10), configuration.getBatchSize());
        List<Configuration> schemas = configuration.find(ConfigurationType.SCHEMA);
        Assert.assertEquals(1, schemas.size());
    }

    @Test
    public void parseProduceJobConfiguration() {
        // when
        ProduceJobConfiguration configuration = (ProduceJobConfiguration) ConfigurationFactory.build(ConfigurationType.PRODUCE_JOB, configJSON);
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