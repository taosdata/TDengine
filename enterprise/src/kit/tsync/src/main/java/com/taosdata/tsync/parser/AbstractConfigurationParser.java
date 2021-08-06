package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.exceptions.TsyncException;

public abstract class AbstractConfigurationParser implements ConfigurationParser {

    @Override
    public abstract boolean canParse(ConfigurationType type, JSONObject configJSON);

    @Override
    public abstract Configuration parse(ConfigurationType type, JSONObject configJSON) throws TsyncException;

    protected Configuration parseConfiguration(JSONObject configJSON, String key, ConfigurationType type, ConfigurationParser parser) throws TsyncException {
        if (configJSON.containsKey(key)) {
            JSONObject json = configJSON.getJSONObject(key);
            if (parser.canParse(type, json)) {
                return parser.parse(type, json);
            }
        }
        return null;
    }

}
