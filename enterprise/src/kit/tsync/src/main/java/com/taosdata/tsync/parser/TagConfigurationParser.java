package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.Configuration;
import com.taosdata.tsync.entity.ConfigurationType;
import com.taosdata.tsync.entity.produceJob.TagConfiguration;

public class TagConfigurationParser implements ConfigurationParser {
    private final ConfigurationType type = ConfigurationType.TAG;

    @Override
    public boolean canParse(ConfigurationType type, JSONObject configJSON) {
        //TODO: configJSON.containsKey("name") == false, then throw Exception and log error
        return this.type == type;
    }

    @Override
    public Configuration parse(ConfigurationType type, JSONObject configJSON) {
        TagConfiguration configuration = new TagConfiguration();
        if (configJSON.containsKey("name")) {
            configuration.setName(configJSON.getString("name"));
        }
        if (configJSON.containsKey("type")) {
            configuration.setType(configJSON.getString("type"));
        }
        if (configJSON.containsKey("length")) {
            configuration.setLength(configJSON.getInteger("length"));
        }
        return configuration;
    }
}
