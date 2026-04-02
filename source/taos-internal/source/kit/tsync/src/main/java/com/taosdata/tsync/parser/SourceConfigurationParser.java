package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.entity.config.SourceConfiguration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.exceptions.TsyncException;

public class SourceConfigurationParser extends AbstractConfigurationParser {
    private final ConfigurationType type = ConfigurationType.SOURCE;

    private final NetConfigurationParser netParser = new NetConfigurationParser();

    @Override
    public boolean canParse(ConfigurationType type, JSONObject configJSON) {
        return this.type == type;
    }

    @Override
    public Configuration parse(ConfigurationType type, JSONObject configJSON) throws TsyncException {
        SourceConfiguration configuration = new SourceConfiguration();

        Configuration net = parseConfiguration(configJSON, "net", ConfigurationType.NET, netParser);
        if (net != null)
            configuration.add(net);

        return configuration;
    }
}
