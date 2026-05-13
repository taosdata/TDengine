package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.entity.config.NetToTQueueConfiguration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.exceptions.TsyncException;

public class NetToTQueueConfigurationParser extends AbstractConfigurationParser {
    private ConfigurationType type = ConfigurationType.NET_TO_TQUEUE;

    private final ProducerConfigurationParser producerParser = new ProducerConfigurationParser();
    private final SourceConfigurationParser sourceParser = new SourceConfigurationParser();

    @Override
    public boolean canParse(ConfigurationType type, JSONObject configJSON) {
        return this.type == type;
    }

    @Override
    public Configuration parse(ConfigurationType type, JSONObject configJSON) throws TsyncException {
        NetToTQueueConfiguration configuration = new NetToTQueueConfiguration();

        Configuration producer = parseConfiguration(configJSON, "producer", ConfigurationType.PRODUCER, producerParser);
        if (producer != null)
            configuration.add(producer);

        Configuration source = parseConfiguration(configJSON, "source", ConfigurationType.SOURCE, sourceParser);
        if (source != null)
            configuration.add(source);

        return configuration;
    }
}
