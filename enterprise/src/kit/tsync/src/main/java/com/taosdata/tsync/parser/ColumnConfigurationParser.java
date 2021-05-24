package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.entity.config.ConfigurationType;
import com.taosdata.tsync.entity.config.ColumnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ColumnConfigurationParser implements ConfigurationParser {
    private final ConfigurationType type = ConfigurationType.COLUMN;
    private static final Logger logger = LoggerFactory.getLogger(ColumnConfigurationParser.class);

    @Override
    public boolean canParse(ConfigurationType type, JSONObject configJSON) {
        //TODO: configJSON.containsKey("name") == false, then throw Exception and log error
        return type == this.type;
    }

    @Override
    public Configuration parse(ConfigurationType type, JSONObject configJSON) {
        ColumnConfiguration configuration = new ColumnConfiguration();
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
