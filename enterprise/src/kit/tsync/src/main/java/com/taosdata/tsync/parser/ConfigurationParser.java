package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.entity.config.ConfigurationType;

public interface ConfigurationParser {

    boolean canParse(ConfigurationType type, JSONObject configJSON);

    Configuration parse(ConfigurationType type, JSONObject configJSON);
}
