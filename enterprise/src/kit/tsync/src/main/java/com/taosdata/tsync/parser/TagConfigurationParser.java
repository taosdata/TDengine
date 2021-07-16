package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.entity.config.TagConfiguration;
import com.taosdata.tsync.exceptions.TsyncException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TagConfigurationParser implements ConfigurationParser {
    private static final Logger logger = LoggerFactory.getLogger(TagConfigurationParser.class);
    private final ConfigurationType type = ConfigurationType.TAG;

    @Override
    public boolean canParse(ConfigurationType type, JSONObject configJSON) {
        //TODO: configJSON.containsKey("name") == false, then throw Exception and log error
        return this.type == type;
    }

    @Override
    public Configuration parse(ConfigurationType type, JSONObject configJSON) throws TsyncException {
        TagConfiguration configuration = new TagConfiguration();
        if (configJSON.containsKey("name")) {
            configuration.setName(configJSON.getString("name"));
        } else {
            String errorMsg = "name is necessary in Tag Configuration";
            logger.error(errorMsg);
            throw new TsyncException(errorMsg);
        }

        if (configJSON.containsKey("type")) {
            configuration.setType(configJSON.getString("type"));
        } else {
            String errorMsg = "type is necessary in Tag Configuration";
            logger.error(errorMsg);
            throw new TsyncException(errorMsg);
        }

        //TODO: tinyint, smallint, int, bigint, float, double, boolean do not need length configuration
        if (configJSON.containsKey("length")) {
            configuration.setLength(configJSON.getInteger("length"));
        }

        return configuration;
    }
}
