package com.taosdata.tsync.factory;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.exceptions.TsyncException;
import com.taosdata.tsync.parser.*;

import java.util.ArrayList;
import java.util.List;

public class ConfigurationFactory {
    private static final List<ConfigurationParser> parserList = new ArrayList<>();

    static {
        // produce-to-tqueue
        parserList.add(new ProducerConfigurationParser());
        parserList.add(new TaskConfigurationParser());
        parserList.add(new ColumnConfigurationParser());
        parserList.add(new TagConfigurationParser());
        parserList.add(new StableConfigurationParser());
        parserList.add(new DatabaseConfigurationParser());
        parserList.add(new SchemaConfigurationParser());
        parserList.add(new MessageConfigurationParser());
        parserList.add(new ProduceJobConfigurationParser());
        // consume-to-tdengine
        parserList.add(new ConsumeToTDengineConfigurationParser());
        parserList.add(new DestinationConfigurationParser());
        parserList.add(new TaosdConfigurationParser());
        // consume-to-file
        parserList.add(new ConsumeToFileConfigurationParser());
        parserList.add(new FileConfigurationParser());
        // consume-to-net
        parserList.add(new ConsumeToNetConfigurationParser());
        parserList.add(new NetConfigurationParser());
        // net-to-tqueue
        parserList.add(new NetToTQueueConfigurationParser());
        parserList.add(new SourceConfigurationParser());
    }

    public static Configuration build(ConfigurationType type, JSONObject configJSON) {
        try {
            for (ConfigurationParser parser : parserList) {
                if (!parser.canParse(type, configJSON))
                    continue;
                return parser.parse(type, configJSON);
            }
        } catch (TsyncException e) {
            e.printStackTrace();
        }
        return null;
    }
}