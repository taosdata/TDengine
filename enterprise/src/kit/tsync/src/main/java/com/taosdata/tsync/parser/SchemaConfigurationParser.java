package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.Configuration;
import com.taosdata.tsync.entity.ConfigurationType;
import com.taosdata.tsync.entity.produceJob.DatabaseConfiguration;
import com.taosdata.tsync.entity.produceJob.SchemaConfiguration;
import com.taosdata.tsync.entity.produceJob.StableConfiguration;

public class SchemaConfigurationParser implements ConfigurationParser {
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
        if (configJSON.containsKey("database")) {
            JSONObject databaseJSON = configJSON.getJSONObject("database");
            if (databaseParser.canParse(ConfigurationType.DATABASE, databaseJSON)) {
                DatabaseConfiguration database = (DatabaseConfiguration) databaseParser.parse(ConfigurationType.DATABASE, databaseJSON);
                configuration.add(database);
            }
        }

        if (configJSON.containsKey("stable")) {
            JSONObject stableJSON = configJSON.getJSONObject("stable");
            if (stableParser.canParse(ConfigurationType.STABLE, stableJSON)) {
                StableConfiguration stable = (StableConfiguration) stableParser.parse(ConfigurationType.STABLE, stableJSON);
                configuration.add(stable);
            }
        }

        return configuration;
    }
}
