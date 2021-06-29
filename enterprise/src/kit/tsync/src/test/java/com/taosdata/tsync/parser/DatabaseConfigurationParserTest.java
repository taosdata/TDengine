package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.DatabaseConfiguration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.factory.ConfigurationFactory;
import org.junit.Assert;
import org.junit.Test;

public class DatabaseConfigurationParserTest {

    String json = "{\"name\": \"test\",\"precision\": \"ms\"}";

    @Test
    public void parseDatabase() {
        // given
        JSONObject databaseJSON = JSONObject.parseObject(json);
        // when
        DatabaseConfiguration configuration = (DatabaseConfiguration) ConfigurationFactory.build(ConfigurationType.DATABASE, databaseJSON);
        // then
        Assert.assertEquals("test", configuration.getName());
        Assert.assertEquals("ms", configuration.getPrecision());
    }

}