package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.entity.config.MessageConfiguration;
import com.taosdata.tsync.exceptions.TsyncException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageConfigurationParser implements ConfigurationParser {
    private static final Logger logger = LoggerFactory.getLogger(MessageConfigurationParser.class);

    private final ConfigurationType type = ConfigurationType.MESSAGE;
    private final SchemaConfigurationParser schemaParser = new SchemaConfigurationParser();

    @Override
    public boolean canParse(ConfigurationType type, JSONObject configJSON) {
        return this.type == type;
    }

    @Override
    public Configuration parse(ConfigurationType type, JSONObject configJSON) throws TsyncException {
        MessageConfiguration configuration = new MessageConfiguration();
        // total
        if (configJSON.containsKey("total")) {
            configuration.setTotal(configJSON.getLong("total"));
        } else {
            String errorMsg = "total is necessary in Message Configuration";
            logger.error(errorMsg);
            throw new TsyncException(errorMsg);
        }

        // batchTables
        if (configJSON.containsKey("batchTables")) {
            configuration.setBatchTables(configJSON.getLong("batchTables"));
        } else {
            logger.warn("use default batchTables: " + MessageConfiguration.DEFAULT_BATCH_TABLES);
            configuration.setBatchTables(MessageConfiguration.DEFAULT_BATCH_TABLES);
        }

        // batchValues
        if (configJSON.containsKey("batchValues")) {
            configuration.setBatchValues(configJSON.getLong("batchValues"));
        } else {
            logger.warn("use default batchValues: " + MessageConfiguration.DEFAULT_BATCH_VALUES);
            configuration.setBatchValues(MessageConfiguration.DEFAULT_BATCH_VALUES);
        }

        // schema
        if (configJSON.containsKey("schema")) {
            JSONObject schemaJSON = configJSON.getJSONObject("schema");
            if (schemaParser.canParse(ConfigurationType.SCHEMA, schemaJSON)) {
                Configuration schema = schemaParser.parse(ConfigurationType.SCHEMA, schemaJSON);
                configuration.add(schema);
            }
        } else {
            String errorMsg = "schema is necessary in Message Configuration";
            logger.error(errorMsg);
            throw new TsyncException(errorMsg);
        }

        return configuration;
    }
}
