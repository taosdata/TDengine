package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.entity.config.SchemaConfiguration;
import com.taosdata.tsync.enums.ConfigurationType;

public class SchemaConfigurationParser extends AbstractConfigurationParser {
    private final ConfigurationType type = ConfigurationType.SCHEMA;
    private final DatabaseConfigurationParser databaseParser = new DatabaseConfigurationParser();
    private final StableConfigurationParser stableParser = new StableConfigurationParser();

    @Override
    public boolean canParse(ConfigurationType type, JSONObject configJSON) {
        return type == this.type;
    }

    @Override
    public Configuration parse(ConfigurationType type, JSONObject configJSON) {
        SchemaConfiguration configuration = new SchemaConfiguration();

        Configuration database = parseConfiguration(configJSON, "database", ConfigurationType.DATABASE, databaseParser);
        if (database != null)
            configuration.add(database);

        Configuration stable = parseConfiguration(configJSON, "stable", ConfigurationType.STABLE, stableParser);
        if (stable != null)
            configuration.add(stable);

        return configuration;
    }
}
