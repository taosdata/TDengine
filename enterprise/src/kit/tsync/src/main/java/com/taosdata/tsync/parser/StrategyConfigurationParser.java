package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.entity.config.StrategyConfiguration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.enums.SchemaMissingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StrategyConfigurationParser implements ConfigurationParser {
    private static final Logger logger = LoggerFactory.getLogger(StrategyConfigurationParser.class);
    private final ConfigurationType type = ConfigurationType.STRATEGY;

    @Override
    public boolean canParse(ConfigurationType type, JSONObject configJSON) {
        return this.type == type;
    }

    @Override
    public Configuration parse(ConfigurationType type, JSONObject configJSON) {
        StrategyConfiguration configuration = new StrategyConfiguration();

        if (configJSON.containsKey("pollingInterval")) {
            configuration.setPollingInterval(configJSON.getInteger("pollingInterval"));
        } else {
            logger.warn("use default polling interval: " + StrategyConfiguration.DEFAULT_POLLING_INTERVAL);
            configuration.setPollingInterval(StrategyConfiguration.DEFAULT_POLLING_INTERVAL);
        }

        if (configJSON.containsKey("schemaMissing")) {
            String schemaMissing = configJSON.getString("schemaMissing");
            if (schemaMissing.equalsIgnoreCase("CREATE")) {
                configuration.setSchemaMissing(SchemaMissingStrategy.CREATE);
            } else {
                configuration.setSchemaMissing(SchemaMissingStrategy.ABORT);
            }
        }

        return configuration;
    }
}
