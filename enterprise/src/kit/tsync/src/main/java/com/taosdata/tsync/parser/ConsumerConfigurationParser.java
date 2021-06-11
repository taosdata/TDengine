package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.entity.config.ConsumerConfiguration;
import com.taosdata.tsync.enums.ConfigurationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerConfigurationParser implements ConfigurationParser {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerConfigurationParser.class);
    private static final Integer PORT_DEFAULT = 6041;
    private static final String USER_DEFAULT = "root";
    private static final String PASSWORD_DEFAULT = "tqueue";
    private static final String CHARSET_DEFAULT = "UTF-8";
    private static final String LOCALE_DEFAULT = "en_US.UTF-8";
    private static final String TIMEZONE_DEFAULT = "UTC-8";
    private static final String SERIALIZER_DEFAULT = "STRING";

    private final ConfigurationType type = ConfigurationType.CONSUMER;

    @Override
    public boolean canParse(ConfigurationType type, JSONObject configJSON) {
        return this.type == type;
    }

    @Override
    public Configuration parse(ConfigurationType type, JSONObject jsonObject) {
        ConsumerConfiguration config = new ConsumerConfiguration();
        // host
        if (jsonObject.containsKey("host")) {
            config.setHost(jsonObject.getString("host"));
        } else {
            String exceptionMsg = "configuration item[host] missing";
            logger.error(exceptionMsg);
            throw new RuntimeException(exceptionMsg);
        }
        // port
        if (jsonObject.containsKey("port")) {
            config.setPort(jsonObject.getInteger("port"));
        } else {
            config.setPort(PORT_DEFAULT);
        }
        // user
        if (jsonObject.containsKey("user")) {
            config.setUser(jsonObject.getString("user"));
        } else {
            config.setUser(USER_DEFAULT);
        }
        // password
        if (jsonObject.containsKey("password")) {
            config.setPassword(jsonObject.getString("password"));
        } else {
            config.setPassword(PASSWORD_DEFAULT);
        }
        // charset
        if (jsonObject.containsKey("charset")) {
            config.setCharset(jsonObject.getString("charset"));
        } else {
            config.setCharset(CHARSET_DEFAULT);
        }
        // locale
        if (jsonObject.containsKey("locale")) {
            config.setLocale(jsonObject.getString("locale"));
        } else {
            config.setLocale(LOCALE_DEFAULT);
        }
        // timezone
        if (jsonObject.containsKey("timezone")) {
            config.setTimezone(jsonObject.getString("timezone"));
        } else {
            config.setTimezone(TIMEZONE_DEFAULT);
        }
        // serializer
        if (jsonObject.containsKey("serializer")) {
            config.setSerializer(jsonObject.getString("serializer"));
        } else {
            config.setSerializer(SERIALIZER_DEFAULT);
        }
        return config;
    }
}
