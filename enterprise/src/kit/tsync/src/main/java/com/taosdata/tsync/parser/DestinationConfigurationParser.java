package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.entity.config.DestinationConfiguration;
import com.taosdata.tsync.enums.ConfigurationType;

public class DestinationConfigurationParser extends AbstractConfigurationParser {
    private final ConfigurationType type = ConfigurationType.DESTINATION;

    private final TaosdConfigurationParser taosdParser = new TaosdConfigurationParser();
    private final StrategyConfigurationParser strategyParser = new StrategyConfigurationParser();
    private final SchemaConfigurationParser schemaParser = new SchemaConfigurationParser();
    private final FileConfigurationParser fileParser = new FileConfigurationParser();

    @Override
    public boolean canParse(ConfigurationType type, JSONObject configJSON) {
        return this.type == type;
    }

    @Override
    public Configuration parse(ConfigurationType type, JSONObject configJSON) {
        DestinationConfiguration configuration = new DestinationConfiguration();

        Configuration taosd = parseConfiguration(configJSON, "taosd", ConfigurationType.TAOSD, taosdParser);
        if (taosd != null)
            configuration.add(taosd);

        Configuration file = parseConfiguration(configJSON, "file", ConfigurationType.FILE, fileParser);
        if (file != null)
            configuration.add(file);

        Configuration strategy = parseConfiguration(configJSON, "strategy", ConfigurationType.STRATEGY, strategyParser);
        if (strategy != null)
            configuration.add(strategy);

        Configuration schema = parseConfiguration(configJSON, "schema", ConfigurationType.SCHEMA, schemaParser);
        if (schema != null)
            configuration.add(schema);

        return configuration;
    }
}