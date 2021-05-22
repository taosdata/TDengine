package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.Configuration;
import com.taosdata.tsync.entity.ConfigurationType;
import com.taosdata.tsync.entity.produceJob.DatabaseConfiguration;

public class DatabaseConfigurationParser implements ConfigurationParser {
    private final ConfigurationType type = ConfigurationType.DATABASE;

    @Override
    public boolean canParse(ConfigurationType type, JSONObject configJSON) {
        return type == this.type;
    }

    @Override
    public Configuration parse(ConfigurationType type, JSONObject configJSON) {
        DatabaseConfiguration configuration = new DatabaseConfiguration();
        // name
        if (configJSON.containsKey("name")) {
            configuration.setName(configJSON.getString("name"));
        }
        // precision
        if (configJSON.containsKey("precision")) {
            configuration.setPrecision(configJSON.getString("precision"));
        }
        // TODO: other options
        return configuration;
    }
}