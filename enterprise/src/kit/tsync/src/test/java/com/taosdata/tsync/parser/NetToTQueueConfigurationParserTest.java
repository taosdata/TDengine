package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.*;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.factory.ConfigurationFactory;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Objects;

public class NetToTQueueConfigurationParserTest {

    private JSONObject netToTQueueConfigurationJSON;

    @Test
    public void parseConsumeToNetConfiguration() {
        // when
        Configuration configuration = ConfigurationFactory.build(ConfigurationType.NET_TO_TQUEUE, netToTQueueConfigurationJSON);
        // then
        Assert.assertEquals(ConfigurationType.NET_TO_TQUEUE, configuration.getConfigurationType());

        // when
        NetToTQueueConfiguration netToTQueueConfiguration = (NetToTQueueConfiguration) configuration;
        ProducerConfiguration producer = (ProducerConfiguration) netToTQueueConfiguration.findFirst(ConfigurationType.PRODUCER);

        // then
        Assert.assertEquals("192.168.17.82", producer.getHost());
        Assert.assertEquals(new Integer(6041), producer.getPort());
        Assert.assertEquals("root", producer.getUser());
        Assert.assertEquals("tqueue", producer.getPassword());
        Assert.assertEquals("UTF-8", producer.getCharset());
        Assert.assertEquals("en_US.UTF-8", producer.getLocale());
        Assert.assertEquals("UTC-8", producer.getTimezone());
        Assert.assertEquals("STRING", producer.getSerializer());

        // when
        NetConfiguration net = (NetConfiguration) netToTQueueConfiguration.findFirst(ConfigurationType.NET);
        // then
        Assert.assertEquals(8899, net.getPort());
    }

    @Before
    public void before() throws IOException {
        // read consume-to-net.json
        String configStr = IOUtils.toString(Objects.requireNonNull(getClass().getClassLoader().getResourceAsStream("net-to-tqueue.json")));
        netToTQueueConfigurationJSON = JSONObject.parseObject(configStr);
    }

}