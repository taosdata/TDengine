package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.enums.ConfigurationType;

public abstract class AbstractConfigurationParser implements ConfigurationParser {

    protected Configuration parseConfiguration(JSONObject configJSON, String key, ConfigurationType type, ConfigurationParser parser) {
        if (configJSON.containsKey(key)) {
            JSONObject json = configJSON.getJSONObject(key);
            if (parser.canParse(type, json)) {
                return parser.parse(type, json);
            }
        }
        return null;
    }

}
