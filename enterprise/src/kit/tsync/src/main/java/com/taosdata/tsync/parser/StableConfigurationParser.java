package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.Configuration;
import com.taosdata.tsync.entity.ConfigurationType;
import com.taosdata.tsync.entity.produceJob.ColumnConfiguration;
import com.taosdata.tsync.entity.produceJob.StableConfiguration;
import com.taosdata.tsync.entity.produceJob.TagConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

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
    public Configuration parse(ConfigurationType type, JSONObject configJSON) {
        StableConfiguration configuration = new StableConfiguration();
        if (configJSON.containsKey("name")) {
            configuration.setName(configJSON.getString("name"));
        }
        if (configJSON.containsKey("tables")) {
            configuration.setTables(configJSON.getInteger("tables"));
        }
        if (configJSON.containsKey("columns")) {
            JSONArray columnArr = configJSON.getJSONArray("columns");
            for (int i = 0; i < columnArr.size(); i++) {
                ColumnConfiguration column = (ColumnConfiguration) columnConfigurationParser.parse(ConfigurationType.COLUMN, columnArr.getJSONObject(i));
                configuration.add(column);
            }
        }
        if (configJSON.containsKey("tags")) {
            JSONArray tagArr = configJSON.getJSONArray("tags");
            for (int i = 0; i < tagArr.size(); i++) {
                TagConfiguration tag = (TagConfiguration) tagConfigurationParser.parse(ConfigurationType.TAG, tagArr.getJSONObject(i));
                configuration.add(tag);
            }
        }
        return configuration;
    }
}
