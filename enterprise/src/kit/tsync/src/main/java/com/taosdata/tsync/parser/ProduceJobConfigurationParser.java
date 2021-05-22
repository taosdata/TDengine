package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.Configuration;
import com.taosdata.tsync.entity.ConfigurationType;
import com.taosdata.tsync.entity.produceJob.ProduceJobConfiguration;

public class ProduceJobConfigurationParser implements ConfigurationParser {
    private final ConfigurationType type = ConfigurationType.PRODUCE_JOB;
    private final ProducerConfigurationParser producerParser = new ProducerConfigurationParser();
    private final TaskConfigurationParser taskParser = new TaskConfigurationParser();
    private final MessageConfigurationParser messageParser = new MessageConfigurationParser();

    @Override
    public boolean canParse(ConfigurationType type, JSONObject configJSON) {
        return this.type == type;
    }

    @Override
    public Configuration parse(ConfigurationType type, JSONObject configJSON) {
        ProduceJobConfiguration config = new ProduceJobConfiguration();

        if (configJSON.containsKey("producer")) {
            JSONObject producerJSON = configJSON.getJSONObject("producer");
            if (producerParser.canParse(ConfigurationType.PRODUCER, producerJSON)) {
                Configuration producer = producerParser.parse(ConfigurationType.PRODUCER, producerJSON);
                config.add(producer);
            }
        }

        if (configJSON.containsKey("task")) {
            JSONObject taskJSON = configJSON.getJSONObject("task");
            if (taskParser.canParse(ConfigurationType.TASK, taskJSON)) {
                Configuration task = taskParser.parse(ConfigurationType.TASK, taskJSON);
                config.add(task);
            }
        }

        if (configJSON.containsKey("message")) {
            JSONObject messageJSON = configJSON.getJSONObject("message");
            if (messageParser.canParse(ConfigurationType.MESSAGE, messageJSON)) {
                Configuration message = messageParser.parse(ConfigurationType.MESSAGE, messageJSON);
                config.add(message);
            }
        }
        return config;
    }
}
