package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.entity.config.ColumnConfiguration;
import com.taosdata.tsync.exceptions.TsyncException;
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
    public Configuration parse(ConfigurationType type, JSONObject configJSON) throws TsyncException {
        ColumnConfiguration configuration = new ColumnConfiguration();
        if (configJSON.containsKey("name")) {
            configuration.setName(configJSON.getString("name"));
        } else {
            throw new TsyncException("name is necessary in Column Configuration");
        }

        if (configJSON.containsKey("type")) {
            configuration.setType(configJSON.getString("type"));
        } else {
            throw new TsyncException("type is necessary in Column Configuration");
        }

        //TODO: tinyint, smallint, int, bigint, float, double, boolean do not need length configuration
        if (configJSON.containsKey("length")) {
            configuration.setLength(configJSON.getInteger("length"));
        }

        return configuration;
    }
}
