package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.ColumnConfiguration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.factory.ConfigurationFactory;
import org.junit.Assert;
import org.junit.Test;

public class ColumnConfigurationParserTest {

    @Test
    public void parseTimestampColumn() {
        // given
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("name", "ts");
        jsonObject.put("type", "timestamp");
        // when
        ColumnConfiguration columnConfiguration = (ColumnConfiguration) ConfigurationFactory.build(ConfigurationType.COLUMN, jsonObject);
        // then
        Assert.assertEquals("ts", columnConfiguration.getName());
        Assert.assertEquals("timestamp", columnConfiguration.getType());
        Assert.assertEquals(null, columnConfiguration.getLength());
    }

    @Test
    public void parseNcharColumn() {
        // given
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("name", "ts");
        jsonObject.put("type", "Nchar");
        jsonObject.put("length", "64");
        // when
        ColumnConfiguration columnConfiguration = (ColumnConfiguration) ConfigurationFactory.build(ConfigurationType.COLUMN, jsonObject);
        // then
        Assert.assertEquals("ts", columnConfiguration.getName());
        Assert.assertEquals("Nchar", columnConfiguration.getType());
        Assert.assertEquals(new Integer(64), columnConfiguration.getLength());
    }

}