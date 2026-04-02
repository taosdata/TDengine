package com.taosdata.tsync.parser;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.entity.config.ProducerConfiguration;
import com.taosdata.tsync.exceptions.TsyncException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerConfigurationParser implements ConfigurationParser {
    private static final Logger logger = LoggerFactory.getLogger(ProducerConfigurationParser.class);

    private final ConfigurationType type = ConfigurationType.PRODUCER;

    @Override
    public boolean canParse(ConfigurationType type, JSONObject configJSON) {
        return this.type == type;
    }

    @Override
    public Configuration parse(ConfigurationType type, JSONObject jsonObject) throws TsyncException {
        ProducerConfiguration config = new ProducerConfiguration();
        // host
        if (jsonObject.containsKey("host")) {
            config.setHost(jsonObject.getString("host"));
        } else {
            String errorMsg = "host is necessary in Producer Configuration";
            logger.error(errorMsg);
            throw new TsyncException(errorMsg);
        }
        // port
        if (jsonObject.containsKey("port")) {
            config.setPort(jsonObject.getInteger("port"));
        } else {
            logger.warn("use default port: " + ProducerConfiguration.PORT_DEFAULT);
            config.setPort(ProducerConfiguration.PORT_DEFAULT);
        }
        // user
        if (jsonObject.containsKey("user")) {
            config.setUser(jsonObject.getString("user"));
        } else {
            logger.warn("use default user: " + ProducerConfiguration.USER_DEFAULT);
            config.setUser(ProducerConfiguration.USER_DEFAULT);
        }
        // password
        if (jsonObject.containsKey("password")) {
            config.setPassword(jsonObject.getString("password"));
        } else {
            logger.warn("use default password: " + ProducerConfiguration.PASSWORD_DEFAULT);
            config.setPassword(ProducerConfiguration.PASSWORD_DEFAULT);
        }
        // charset
        if (jsonObject.containsKey("charset")) {
            config.setCharset(jsonObject.getString("charset"));
        } else {
            logger.warn("use default charset: " + ProducerConfiguration.CHARSET_DEFAULT);
            config.setCharset(ProducerConfiguration.CHARSET_DEFAULT);
        }
        // locale
        if (jsonObject.containsKey("locale")) {
            config.setLocale(jsonObject.getString("locale"));
        } else {
            logger.warn("use default locale: " + ProducerConfiguration.LOCALE_DEFAULT);
            config.setLocale(ProducerConfiguration.LOCALE_DEFAULT);
        }
        // timezone
        if (jsonObject.containsKey("timezone")) {
            config.setTimezone(jsonObject.getString("timezone"));
        } else {
            logger.warn("use default timezone: " + ProducerConfiguration.TIMEZONE_DEFAULT);
            config.setTimezone(ProducerConfiguration.TIMEZONE_DEFAULT);
        }
        // serializer
        if (jsonObject.containsKey("serializer")) {
            config.setSerializer(jsonObject.getString("serializer"));
        } else {
            logger.warn("use default serializer: " + ProducerConfiguration.SERIALIZER_DEFAULT);
            config.setSerializer(ProducerConfiguration.SERIALIZER_DEFAULT);
        }
        return config;
    }
}
