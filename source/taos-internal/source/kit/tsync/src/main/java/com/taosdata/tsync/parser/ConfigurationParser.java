package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.exceptions.TsyncException;

public interface ConfigurationParser {

    boolean canParse(ConfigurationType type, JSONObject configJSON);

    Configuration parse(ConfigurationType type, JSONObject configJSON) throws TsyncException;
}
