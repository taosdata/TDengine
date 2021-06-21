package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.entity.config.ConsumerConfiguration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.exceptions.TsyncException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerConfigurationParser implements ConfigurationParser {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerConfigurationParser.class);

    private final ConfigurationType type = ConfigurationType.CONSUMER;

    @Override
    public boolean canParse(ConfigurationType type, JSONObject configJSON) {
        return this.type == type;
    }

    @Override
    public Configuration parse(ConfigurationType type, JSONObject jsonObject) throws TsyncException {
        ConsumerConfiguration config = new ConsumerConfiguration();
        // host
        if (jsonObject.containsKey("host")) {
            config.setHost(jsonObject.getString("host"));
        } else {
            String exceptionMsg = "host is necessary in Consumer Configuration";
            logger.error(exceptionMsg);
            throw new TsyncException(exceptionMsg);
        }
        // port
        if (jsonObject.containsKey("port")) {
            config.setPort(jsonObject.getInteger("port"));
        } else {
            logger.warn("use default port: " + ConsumerConfiguration.PORT_DEFAULT);
            config.setPort(ConsumerConfiguration.PORT_DEFAULT);
        }
        // user
        if (jsonObject.containsKey("user")) {
            config.setUser(jsonObject.getString("user"));
        } else {
            logger.warn("use default user: " + ConsumerConfiguration.USER_DEFAULT);
            config.setUser(ConsumerConfiguration.USER_DEFAULT);
        }
        // password
        if (jsonObject.containsKey("password")) {
            config.setPassword(jsonObject.getString("password"));
        } else {
            logger.warn("use default password: " + ConsumerConfiguration.PASSWORD_DEFAULT);
            config.setPassword(ConsumerConfiguration.PASSWORD_DEFAULT);
        }
        // charset
        if (jsonObject.containsKey("charset")) {
            config.setCharset(jsonObject.getString("charset"));
        } else {
            logger.warn("use default charset: " + ConsumerConfiguration.CHARSET_DEFAULT);
            config.setCharset(ConsumerConfiguration.CHARSET_DEFAULT);
        }
        // locale
        if (jsonObject.containsKey("locale")) {
            config.setLocale(jsonObject.getString("locale"));
        } else {
            logger.warn("use default locale: " + ConsumerConfiguration.LOCALE_DEFAULT);
            config.setLocale(ConsumerConfiguration.LOCALE_DEFAULT);
        }
        // timezone
        if (jsonObject.containsKey("timezone")) {
            config.setTimezone(jsonObject.getString("timezone"));
        } else {
            logger.warn("use default timezone: " + ConsumerConfiguration.TIMEZONE_DEFAULT);
            config.setTimezone(ConsumerConfiguration.TIMEZONE_DEFAULT);
        }
        // serializer
        if (jsonObject.containsKey("serializer")) {
            config.setSerializer(jsonObject.getString("serializer"));
        } else {
            logger.warn("use default serializer: " + ConsumerConfiguration.SERIALIZER_DEFAULT);
            config.setSerializer(ConsumerConfiguration.SERIALIZER_DEFAULT);
        }
        return config;
    }
}
