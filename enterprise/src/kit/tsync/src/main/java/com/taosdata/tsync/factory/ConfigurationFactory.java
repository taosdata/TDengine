package com.taosdata.tsync.factory;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.Configuration;
import com.taosdata.tsync.entity.ConfigurationType;
import com.taosdata.tsync.parser.*;

import java.util.ArrayList;
import java.util.List;

public class ConfigurationFactory {
    private static List<ConfigurationParser> parserList = new ArrayList<>();

    static {
        parserList.add(new ProducerConfigurationParser());
        parserList.add(new TaskConfigurationParser());
        parserList.add(new ColumnConfigurationParser());
        parserList.add(new TagConfigurationParser());
        parserList.add(new StableConfigurationParser());
        parserList.add(new DatabaseConfigurationParser());
        parserList.add(new SchemaConfigurationParser());
        parserList.add(new MessageConfigurationParser());
        parserList.add(new ProduceJobConfigurationParser());
    }

    public static Configuration build(ConfigurationType type, JSONObject configJSON) {
        for (ConfigurationParser parser : parserList) {
            if (!parser.canParse(type, configJSON))
                continue;
            return parser.parse(type, configJSON);
        }
        return null;
    }
}