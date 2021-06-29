package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.entity.config.NetConfiguration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.exceptions.TsyncException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetConfigurationParser implements ConfigurationParser {
    private static final Logger logger = LoggerFactory.getLogger(NetConfigurationParser.class);
    private final ConfigurationType type = ConfigurationType.NET;

    @Override
    public boolean canParse(ConfigurationType type, JSONObject configJSON) {
        return this.type == type;
    }

    @Override
    public Configuration parse(ConfigurationType type, JSONObject configJSON) throws TsyncException {
        NetConfiguration netConfiguration = new NetConfiguration();

        if (configJSON.containsKey("host")) {
            netConfiguration.setHost(configJSON.getString("host"));
        } else {
            logger.warn("use default host: " + NetConfiguration.DEFAULT_HOST);
            netConfiguration.setHost(NetConfiguration.DEFAULT_HOST);
        }
        if (configJSON.containsKey("port")) {
            netConfiguration.setPort(configJSON.getInteger("port"));
        } else {
            logger.warn("use default port: " + NetConfiguration.DEFAULT_PORT);
            netConfiguration.setPort(NetConfiguration.DEFAULT_PORT);
        }

        return netConfiguration;
    }
}
