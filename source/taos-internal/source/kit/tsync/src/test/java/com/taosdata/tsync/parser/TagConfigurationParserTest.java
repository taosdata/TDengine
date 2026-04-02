package com.taosdata.tsync.parser;


import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.TagConfiguration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.factory.ConfigurationFactory;
import org.junit.Assert;
import org.junit.Test;

public class TagConfigurationParserTest {

    @Test
    public void parseTag() {
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

}