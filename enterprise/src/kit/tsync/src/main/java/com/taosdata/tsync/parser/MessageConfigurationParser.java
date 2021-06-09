package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.entity.config.MessageConfiguration;

public class MessageConfigurationParser implements ConfigurationParser {
    private final ConfigurationType type = ConfigurationType.MESSAGE;
    private final SchemaConfigurationParser schemaParser = new SchemaConfigurationParser();

    @Override
    public boolean canParse(ConfigurationType type, JSONObject configJSON) {
        return this.type == type;
    }

    @Override
    public Configuration parse(ConfigurationType type, JSONObject configJSON) {
        MessageConfiguration configuration = new MessageConfiguration();
        // total
        if (configJSON.containsKey("total")) {
            configuration.setTotal(configJSON.getLong("total"));
        }
        // batchTables
        if (configJSON.containsKey("batchTables")) {
            configuration.setBatchTables(configJSON.getLong("batchTables"));
        }
        // batchValues
        if (configJSON.containsKey("batchValues")) {
            configuration.setBatchValues(configJSON.getLong("batchValues"));
        }
        // schema
        if (configJSON.containsKey("schema")) {
            JSONObject schemaJSON = configJSON.getJSONObject("schema");
            if (schemaParser.canParse(ConfigurationType.SCHEMA, schemaJSON)) {
                Configuration schema = schemaParser.parse(ConfigurationType.SCHEMA, schemaJSON);
                configuration.add(schema);
            }
        }
        return configuration;
    }
}
