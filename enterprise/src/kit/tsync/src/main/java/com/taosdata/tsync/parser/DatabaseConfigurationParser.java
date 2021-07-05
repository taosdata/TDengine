package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.entity.config.DatabaseConfiguration;
import com.taosdata.tsync.enums.DatabasePrecision;
import com.taosdata.tsync.exceptions.TsyncException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseConfigurationParser implements ConfigurationParser {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseConfigurationParser.class);
    private final ConfigurationType type = ConfigurationType.DATABASE;

    @Override
    public boolean canParse(ConfigurationType type, JSONObject configJSON) {
        return type == this.type;
    }

    @Override
    public Configuration parse(ConfigurationType type, JSONObject configJSON) throws TsyncException {
        DatabaseConfiguration configuration = new DatabaseConfiguration();
        // name
        if (configJSON.containsKey("name")) {
            configuration.setName(configJSON.getString("name"));
        } else {
            String errorMsg = "name is necessary in Database Configuration";
            logger.error(errorMsg);
            throw new TsyncException(errorMsg);
        }
        // precision
        if (configJSON.containsKey("precision")) {
            String precision = configJSON.getString("precision");
            configuration.setPrecision(parsePrecision(precision));
        } else {
            logger.warn("use default precision: " + DatabaseConfiguration.DEFAULT_PRECISION);
            configuration.setPrecision(DatabaseConfiguration.DEFAULT_PRECISION);
        }

        return configuration;
    }

    private DatabasePrecision parsePrecision(String precision) {
        switch (precision.toLowerCase()) {
            case "ns":
                return DatabasePrecision.NS;
            case "us":
                return DatabasePrecision.US;
            case "ms":
            default:
                return DatabasePrecision.MS;
        }
    }
}