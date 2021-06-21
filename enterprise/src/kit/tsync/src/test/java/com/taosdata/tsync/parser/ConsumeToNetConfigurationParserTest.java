package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.*;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.exceptions.TsyncException;
import com.taosdata.tsync.factory.ConfigurationFactory;
import org.apache.commons.io.IOUtils;
import org.checkerframework.checker.units.qual.A;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Objects;

public class ConsumeToNetConfigurationParserTest {

    private JSONObject consumeToNetConfigruationJSON;

    @Test
    public void parseConsumeToNetConfiguration() throws TsyncException {
        // when
        Configuration configuration = ConfigurationFactory.build(ConfigurationType.CONSUME_TO_NET, consumeToNetConfigruationJSON);
        // then
        Assert.assertEquals(ConfigurationType.CONSUME_TO_NET, configuration.getConfigurationType());

        // when
        ConsumeToNetConfiguration consumeToNetConfiguration = (ConsumeToNetConfiguration) configuration;
        ConsumerConfiguration consumer = (ConsumerConfiguration) consumeToNetConfiguration.findFirst(ConfigurationType.CONSUMER);

        // then
        Assert.assertEquals("192.168.17.156", consumer.getHost());
        Assert.assertEquals(new Integer(6041), consumer.getPort());
        Assert.assertEquals("root", consumer.getUser());
        Assert.assertEquals("tqueue", consumer.getPassword());
        Assert.assertEquals("UTF-8", consumer.getCharset());
        Assert.assertEquals("en_US.UTF-8", consumer.getLocale());
        Assert.assertEquals("UTC-8", consumer.getTimezone());
        Assert.assertEquals("STRING", consumer.getSerializer());

        // when
        TaskConfiguration task = (TaskConfiguration) consumeToNetConfiguration.findFirst(ConfigurationType.TASK);
        // then
        Assert.assertEquals("tq_test", task.getTopic());
        int[] partitions = task.getPartitions();
        Assert.assertEquals(8, partitions.length);
        Assert.assertEquals(1, partitions[0]);
        Assert.assertEquals(2, partitions[1]);
        Assert.assertEquals(3, partitions[2]);
        Assert.assertEquals(4, partitions[3]);
        Assert.assertEquals(5, partitions[4]);
        Assert.assertEquals(6, partitions[5]);
        Assert.assertEquals(7, partitions[6]);
        Assert.assertEquals(8, partitions[7]);

        // when
        NetConfiguration net = (NetConfiguration) consumeToNetConfiguration.findFirst(ConfigurationType.NET);
        // then
        Assert.assertEquals("192.168.17.82", net.getHost());
        Assert.assertEquals(8899, net.getPort());

        // when
        StrategyConfiguration strategy = (StrategyConfiguration) consumeToNetConfiguration.findFirst(ConfigurationType.STRATEGY);
        // then
        Assert.assertEquals(1000, strategy.getPollingInterval());
    }

    @Before
    public void before() throws IOException {
        // read consume-to-net.json
        String consumeConfigStr = IOUtils.toString(Objects.requireNonNull(getClass().getClassLoader().getResourceAsStream("consume-to-net.json")));
        consumeToNetConfigruationJSON = JSONObject.parseObject(consumeConfigStr);
    }

}