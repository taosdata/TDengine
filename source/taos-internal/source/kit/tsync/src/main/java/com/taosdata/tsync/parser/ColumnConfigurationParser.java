package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.entity.config.ColumnConfiguration;
import com.taosdata.tsync.exceptions.TsyncException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ColumnConfigurationParser implements ConfigurationParser {
    private static final Logger logger = LoggerFactory.getLogger(ColumnConfigurationParser.class);

    private final ConfigurationType type = ConfigurationType.COLUMN;

    @Override
    public boolean canParse(ConfigurationType type, JSONObject configJSON) {
        return type == this.type;
    }

    @Override
    public Configuration parse(ConfigurationType type, JSONObject configJSON) throws TsyncException {
        ColumnConfiguration configuration = new ColumnConfiguration();
        if (configJSON.containsKey("name")) {
            configuration.setName(configJSON.getString("name"));
        } else {
            String errorMsg = "name is necessary in Column Configuration";
            logger.error(errorMsg);
            throw new TsyncException(errorMsg);
        }

        if (configJSON.containsKey("type")) {
            configuration.setType(configJSON.getString("type"));
        } else {
            String errorMsg = "type is necessary in Column Configuration";
            logger.error(errorMsg);
            throw new TsyncException(errorMsg);
        }

        if (needParseLength(configuration.getType())) {
            if (configJSON.containsKey("length")) {
                configuration.setLength(configJSON.getInteger("length"));
            } else {
                String errorMsg = "length is necessary for type: " + configuration.getType() + " in column configuration";
                logger.error(errorMsg);
                throw new TsyncException(errorMsg);
            }
        }

        return configuration;
    }

    private boolean needParseLength(String type) {
        return "binary".equals(type.toLowerCase()) || "nchar".equals(type.toLowerCase());
    }
}
