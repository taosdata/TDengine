package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.entity.config.ConsumeToTDengineConfiguration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.exceptions.TsyncException;

public class ConsumeToTDengineConfigurationParser extends AbstractConfigurationParser {

    private final ConfigurationType type = ConfigurationType.CONSUME_TO_TDENGINE;

    private final ConsumerConfigurationParser consumerParser = new ConsumerConfigurationParser();
    private final TaskConfigurationParser taskParser = new TaskConfigurationParser();
    private final DestinationConfigurationParser destinationParser = new DestinationConfigurationParser();

    @Override
    public boolean canParse(ConfigurationType type, JSONObject configJSON) {
        return this.type == type;
    }

    @Override
    public Configuration parse(ConfigurationType type, JSONObject configJSON) throws TsyncException {
        ConsumeToTDengineConfiguration config = new ConsumeToTDengineConfiguration();

        Configuration consumer = parseConfiguration(configJSON, "consumer", ConfigurationType.CONSUMER, consumerParser);
        if (consumer != null)
            config.add(consumer);

        Configuration task = parseConfiguration(configJSON, "task", ConfigurationType.TASK, taskParser);
        if (task != null)
            config.add(task);

        Configuration destination = parseConfiguration(configJSON, "destination", ConfigurationType.DESTINATION, destinationParser);
        if (destination != null)
            config.add(destination);

        return config;
    }

}
