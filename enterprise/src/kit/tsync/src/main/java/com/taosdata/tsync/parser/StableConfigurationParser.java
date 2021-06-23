package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.entity.config.ColumnConfiguration;
import com.taosdata.tsync.entity.config.StableConfiguration;
import com.taosdata.tsync.entity.config.TagConfiguration;
import com.taosdata.tsync.exceptions.TsyncException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class StableConfigurationParser implements ConfigurationParser {
    private static final Logger logger = LoggerFactory.getLogger(StableConfigurationParser.class);

    private final ConfigurationType type = ConfigurationType.STABLE;
    private final ColumnConfigurationParser columnConfigurationParser = new ColumnConfigurationParser();
    private final TagConfigurationParser tagConfigurationParser = new TagConfigurationParser();

    @Override
    public boolean canParse(ConfigurationType type, JSONObject configJSON) {
        return this.type == type;
    }

    @Override
    public Configuration parse(ConfigurationType type, JSONObject configJSON) throws TsyncException {
        StableConfiguration configuration = new StableConfiguration();

        if (configJSON.containsKey("name")) {
            configuration.setName(configJSON.getString("name"));
        } else {
            String errorMsg = "name is necessary in stable Configuration";
            logger.error(errorMsg);
            throw new TsyncException(errorMsg);
        }

        if (configJSON.containsKey("tables")) {
            configuration.setTables(configJSON.getLong("tables"));
        } else {
            String errorMsg = "tables is necessary in stable Configuration";
            logger.error(errorMsg);
            throw new TsyncException(errorMsg);
        }

        if (configJSON.containsKey("columns")) {
            JSONArray columnArr = configJSON.getJSONArray("columns");
            for (int i = 0; i < columnArr.size(); i++) {
                ColumnConfiguration column = (ColumnConfiguration) columnConfigurationParser.parse(ConfigurationType.COLUMN, columnArr.getJSONObject(i));
                configuration.add(column);
            }
        } else {
            String errorMsg = "columns is necessary in stable Configuration";
            logger.error(errorMsg);
            throw new TsyncException(errorMsg);
        }

        if (configJSON.containsKey("tags")) {
            JSONArray tagArr = configJSON.getJSONArray("tags");
            for (int i = 0; i < tagArr.size(); i++) {
                TagConfiguration tag = (TagConfiguration) tagConfigurationParser.parse(ConfigurationType.TAG, tagArr.getJSONObject(i));
                configuration.add(tag);
            }
        } else {
            String errorMsg = "tags is necessary in stable Configuration";
            logger.error(errorMsg);
            throw new TsyncException(errorMsg);
        }

        return configuration;
    }
}
