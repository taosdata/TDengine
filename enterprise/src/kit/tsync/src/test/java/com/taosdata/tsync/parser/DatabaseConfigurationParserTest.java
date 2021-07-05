package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.DatabaseConfiguration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.enums.DatabasePrecision;
import com.taosdata.tsync.factory.ConfigurationFactory;
import org.junit.Assert;
import org.junit.Test;

public class DatabaseConfigurationParserTest {


    @Test
    public void milliSec() {
        // given
        JSONObject databaseJSON = JSONObject.parseObject("{\"name\": \"test\",\"precision\": \"ms\"}");
        // when
        DatabaseConfiguration configuration = (DatabaseConfiguration) ConfigurationFactory.build(ConfigurationType.DATABASE, databaseJSON);
        // then
        Assert.assertEquals("test", configuration.getName());
        Assert.assertEquals(DatabasePrecision.MS, configuration.getPrecision());
    }

    @Test
    public void microSec() {
        // given
        JSONObject databaseJSON = JSONObject.parseObject("{\"name\": \"test\",\"precision\": \"us\"}");
        // when
        DatabaseConfiguration configuration = (DatabaseConfiguration) ConfigurationFactory.build(ConfigurationType.DATABASE, databaseJSON);
        // then
        Assert.assertEquals("test", configuration.getName());
        Assert.assertEquals(DatabasePrecision.US, configuration.getPrecision());
    }

    @Test
    public void nanSec() {
        // given
        JSONObject databaseJSON = JSONObject.parseObject("{\"name\": \"test\",\"precision\": \"ns\"}");
        // when
        DatabaseConfiguration configuration = (DatabaseConfiguration) ConfigurationFactory.build(ConfigurationType.DATABASE, databaseJSON);
        // then
        Assert.assertEquals("test", configuration.getName());
        Assert.assertEquals(DatabasePrecision.NS, configuration.getPrecision());
    }

}