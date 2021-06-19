package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.entity.config.ProduceToTQueueConfiguration;
import com.taosdata.tsync.enums.ConfigurationType;

public class ProduceJobConfigurationParser extends AbstractConfigurationParser {
    private final ConfigurationType type = ConfigurationType.PRODUCE_TO_TQUEUE;
    private final ProducerConfigurationParser producerParser = new ProducerConfigurationParser();
    private final TaskConfigurationParser taskParser = new TaskConfigurationParser();
    private final MessageConfigurationParser messageParser = new MessageConfigurationParser();

    @Override
    public boolean canParse(ConfigurationType type, JSONObject configJSON) {
        return this.type == type;
    }

    @Override
    public Configuration parse(ConfigurationType type, JSONObject configJSON) {
        ProduceToTQueueConfiguration config = new ProduceToTQueueConfiguration();

        Configuration producer = parseConfiguration(configJSON, "producer", ConfigurationType.PRODUCER, producerParser);
        if (producer != null)
            config.add(producer);

        Configuration task = parseConfiguration(configJSON, "task", ConfigurationType.TASK, taskParser);
        if (task != null)
            config.add(task);

        Configuration message = parseConfiguration(configJSON, "message", ConfigurationType.MESSAGE, messageParser);
        if (message != null)
            config.add(message);

        return config;
    }
}
