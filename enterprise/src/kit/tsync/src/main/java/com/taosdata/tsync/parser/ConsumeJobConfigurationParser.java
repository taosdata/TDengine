package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.entity.config.ConsumeJobConfiguration;
import com.taosdata.tsync.enums.ConfigurationType;

public class ConsumeJobConfigurationParser implements ConfigurationParser {

    private final ConfigurationType type = ConfigurationType.CONSUME_JOB;
    private final ConsumerConfigurationParser consumerParser = new ConsumerConfigurationParser();
    private final TaskConfigurationParser taskParser = new TaskConfigurationParser();
    private final DestinationConfigurationParser destinationParser = new DestinationConfigurationParser();

    @Override
    public boolean canParse(ConfigurationType type, JSONObject configJSON) {
        return this.type == type;
    }

    @Override
    public Configuration parse(ConfigurationType type, JSONObject configJSON) {
        ConsumeJobConfiguration config = new ConsumeJobConfiguration();

        if (configJSON.containsKey("consumer")) {
            JSONObject consumerJSON = configJSON.getJSONObject("consumer");
            if (consumerParser.canParse(ConfigurationType.CONSUMER, consumerJSON)) {
                Configuration producer = consumerParser.parse(ConfigurationType.CONSUMER, consumerJSON);
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

        if (configJSON.containsKey("destination")) {
            JSONObject destinationJSON = configJSON.getJSONObject("destination");
            if (destinationParser.canParse(ConfigurationType.DESTINATION, destinationJSON)) {
                Configuration destination = destinationParser.parse(ConfigurationType.DESTINATION, destinationJSON);
                config.add(destination);
            }
        }

        return config;
    }

}
