package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.entity.config.DestinationConfiguration;
import com.taosdata.tsync.enums.ConfigurationType;

public class DestinationConfigurationParser implements ConfigurationParser {
    private final ConfigurationType type = ConfigurationType.DESTINATION;

    private TaosdConfigurationParser taosdParser = new TaosdConfigurationParser();

    @Override
    public boolean canParse(ConfigurationType type, JSONObject configJSON) {
        return this.type == type;
    }

    @Override
    public Configuration parse(ConfigurationType type, JSONObject configJSON) {
        DestinationConfiguration configuration = new DestinationConfiguration();

        if (configJSON.containsKey("taosd")) {
            JSONObject taosdJSON = configJSON.getJSONObject("taosd");
            if (taosdParser.canParse(ConfigurationType.TAOSD, taosdJSON)) {
                Configuration taosd = taosdParser.parse(ConfigurationType.TAOSD, taosdJSON);
                configuration.add(taosd);
            }
        }

        return configuration;
    }
}