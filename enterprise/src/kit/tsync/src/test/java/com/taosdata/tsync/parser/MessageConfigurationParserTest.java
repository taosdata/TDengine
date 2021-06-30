package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.entity.config.MessageConfiguration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.factory.ConfigurationFactory;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class MessageConfigurationParserTest {

    private String json;

    @Test
    public void parseMessage() {
        // given
        JSONObject messageJSON = JSONObject.parseObject(json);
        // when
        MessageConfiguration configuration = (MessageConfiguration) ConfigurationFactory.build(ConfigurationType.MESSAGE, messageJSON);
        // then
        Assert.assertEquals(new Long(100), configuration.getTotal());
        Assert.assertEquals(new Long(10), configuration.getBatchTables());
        Assert.assertEquals(new Long(10), configuration.getBatchValues());
        List<Configuration> schemas = configuration.find(ConfigurationType.SCHEMA);
        Assert.assertEquals(1, schemas.size());
    }

    @Before
    public void before() throws IOException {
        json = IOUtils.toString(Objects.requireNonNull(getClass().getClassLoader().getResourceAsStream("message.json")));
    }

}