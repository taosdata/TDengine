package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.ConsumerConfiguration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.factory.ConfigurationFactory;
import org.junit.Assert;
import org.junit.Test;

public class ConsumerConfigurationParserTest {
    private String json = "{" +
            "    \"host\": \"192.168.17.156\"," +
            "    \"port\": 6041," +
            "    \"user\": \"root\"," +
            "    \"password\": \"tqueue\"," +
            "    \"charset\": \"UTF-8\"," +
            "    \"locale\": \"en_US.UTF-8\"," +
            "    \"timezone\": \"UTC-8\"," +
            "    \"serializer\": \"STRING\"" +
            "  }";

    @Test
    public void parseConsumer() {
        // given
        JSONObject jsonObject = JSONObject.parseObject(json);
        // when
        ConsumerConfiguration configuration = (ConsumerConfiguration) ConfigurationFactory.build(ConfigurationType.CONSUMER, jsonObject);
        // then
        Assert.assertEquals("192.168.17.156", configuration.getHost());
        Assert.assertEquals(new Integer(6041), configuration.getPort());
        Assert.assertEquals("root", configuration.getUser());
        Assert.assertEquals("tqueue", configuration.getPassword());
        Assert.assertEquals("UTF-8", configuration.getCharset());
        Assert.assertEquals("en_US.UTF-8", configuration.getLocale());
        Assert.assertEquals("UTC-8", configuration.getTimezone());
        Assert.assertEquals("STRING", configuration.getSerializer());
    }

}