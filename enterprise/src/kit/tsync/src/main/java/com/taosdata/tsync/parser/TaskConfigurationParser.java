package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.entity.config.ConfigurationType;
import com.taosdata.tsync.entity.config.TaskConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskConfigurationParser implements ConfigurationParser {
    private static final int DEFAULT_THREADS = 1;
    private static final Logger logger = LoggerFactory.getLogger(TaskConfigurationParser.class);

    private final ConfigurationType type = ConfigurationType.TASK;

    @Override
    public boolean canParse(ConfigurationType type, JSONObject configJSON) {
        return this.type == type;
    }

    @Override
    public Configuration parse(ConfigurationType type, JSONObject configJSON) {
        TaskConfiguration config = new TaskConfiguration();
        // topic
        if (configJSON.containsKey("topic")) {
            String topic = configJSON.getString("topic");
            config.setTopic(topic);
        } else {
            logger.error("configuration item[topic] missing.");
        }

        // partitions
        if (configJSON.containsKey("partitions")) {
            JSONArray arr = configJSON.getJSONArray("partitions");
            int[] partitions = new int[arr.size()];
            for (int i = 0; i < arr.size(); i++) {
                partitions[i] = arr.getInteger(i);
            }
            config.setPartitions(partitions);
        } else {
            //TODO: get partitions from tqueue
        }

        // threads
        if (configJSON.containsKey("threads")) {
            Integer threads = configJSON.getInteger("threads");
            config.setThreads(threads);
        } else {
            config.setThreads(DEFAULT_THREADS);
        }
        return config;
    }
}
