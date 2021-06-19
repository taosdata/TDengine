package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.entity.config.FileConfiguration;
import com.taosdata.tsync.enums.ConfigurationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileConfigurationParser implements ConfigurationParser {
    private static final Logger logger = LoggerFactory.getLogger(FileConfigurationParser.class);
    private final ConfigurationType type = ConfigurationType.FILE;


    @Override
    public boolean canParse(ConfigurationType type, JSONObject configJSON) {
        return this.type == type;
    }

    @Override
    public Configuration parse(ConfigurationType type, JSONObject configJSON) {
        FileConfiguration fileConfiguration = new FileConfiguration();

        if (configJSON.containsKey("directory")) {
            fileConfiguration.setDirectory(configJSON.getString("directory"));
        } else {
            String errorMsg = "directory is missing";
            logger.error(errorMsg);
            throw new RuntimeException(errorMsg);
        }

        if (configJSON.containsKey("prefix")) {
            fileConfiguration.setPrefix(configJSON.getString("prefix"));
        } else {
            logger.warn("use default prefix: '" + FileConfiguration.DEFAULT_PREFIX + "'");
            fileConfiguration.setPrefix(FileConfiguration.DEFAULT_PREFIX);
        }

        return fileConfiguration;
    }
}
