package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.entity.config.TaosdConfiguration;
import com.taosdata.tsync.enums.ConfigurationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaosdConfigurationParser implements ConfigurationParser {

    private static final Logger logger = LoggerFactory.getLogger(TaosdConfigurationParser.class);

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
            logger.warn("use default port: " + TaosdConfiguration.PORT_DEFAULT);
            config.setPort(TaosdConfiguration.PORT_DEFAULT);
        }
        // user
        if (configJSON.containsKey("user")) {
            config.setUser(configJSON.getString("user"));
        } else {
            logger.warn("use default user: " + TaosdConfiguration.USER_DEFAULT);
            config.setUser(TaosdConfiguration.USER_DEFAULT);
        }
        // password
        if (configJSON.containsKey("password")) {
            config.setPassword(configJSON.getString("password"));
        } else {
            logger.warn("use default password: ******");
            config.setPassword(TaosdConfiguration.PASSWORD_DEFAULT);
        }
        // charset
        if (configJSON.containsKey("charset")) {
            config.setCharset(configJSON.getString("charset"));
        } else {
            logger.warn("use default charset: " + TaosdConfiguration.CHARSET_DEFAULT);
            config.setCharset(TaosdConfiguration.CHARSET_DEFAULT);
        }
        // locale
        if (configJSON.containsKey("locale")) {
            config.setLocale(configJSON.getString("locale"));
        } else {
            logger.warn("use default locale: " + TaosdConfiguration.LOCALE_DEFAULT);
            config.setLocale(TaosdConfiguration.LOCALE_DEFAULT);
        }
        // timezone
        if (configJSON.containsKey("timezone")) {
            config.setTimezone(configJSON.getString("timezone"));
        } else {
            logger.warn("use default timezone: " + TaosdConfiguration.TIMEZONE_DEFAULT);
            config.setTimezone(TaosdConfiguration.TIMEZONE_DEFAULT);
        }

        return config;
    }
}
