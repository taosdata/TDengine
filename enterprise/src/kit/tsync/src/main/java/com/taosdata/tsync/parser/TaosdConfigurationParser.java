package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.entity.config.TaosdConfiguration;
import com.taosdata.tsync.enums.ConfigurationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaosdConfigurationParser implements ConfigurationParser {

    private static final Logger logger = LoggerFactory.getLogger(TaosdConfigurationParser.class);
    private static final Integer PORT_DEFAULT = 6041;
    private static final String USER_DEFAULT = "root";
    private static final String PASSWORD_DEFAULT = "taosdata";
    private static final String CHARSET_DEFAULT = "UTF-8";
    private static final String LOCALE_DEFAULT = "en_US.UTF-8";
    private static final String TIMEZONE_DEFAULT = "UTC-8";

    private final ConfigurationType type = ConfigurationType.TAOSD;

    @Override
    public boolean canParse(ConfigurationType type, JSONObject configJSON) {
        return this.type == type;
    }

    @Override
    public Configuration parse(ConfigurationType type, JSONObject configJSON) {
        TaosdConfiguration config = new TaosdConfiguration();
        // host
        if (configJSON.containsKey("host")) {
            config.setHost(configJSON.getString("host"));
        } else {
            String exceptionMsg = "configuration item[host] missing";
            logger.error(exceptionMsg);
            throw new RuntimeException(exceptionMsg);
        }
        // port
        if (configJSON.containsKey("port")) {
            config.setPort(configJSON.getInteger("port"));
        } else {
            config.setPort(PORT_DEFAULT);
        }
        // user
        if (configJSON.containsKey("user")) {
            config.setUser(configJSON.getString("user"));
        } else {
            config.setUser(USER_DEFAULT);
        }
        // password
        if (configJSON.containsKey("password")) {
            config.setPassword(configJSON.getString("password"));
        } else {
            config.setPassword(PASSWORD_DEFAULT);
        }
        // charset
        if (configJSON.containsKey("charset")) {
            config.setCharset(configJSON.getString("charset"));
        } else {
            config.setCharset(CHARSET_DEFAULT);
        }
        // locale
        if (configJSON.containsKey("locale")) {
            config.setLocale(configJSON.getString("locale"));
        } else {
            config.setLocale(LOCALE_DEFAULT);
        }
        // timezone
        if (configJSON.containsKey("timezone")) {
            config.setTimezone(configJSON.getString("timezone"));
        } else {
            config.setTimezone(TIMEZONE_DEFAULT);
        }

        return config;
    }
}
