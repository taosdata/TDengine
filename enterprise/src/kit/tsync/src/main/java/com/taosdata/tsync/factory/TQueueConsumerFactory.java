package com.taosdata.tsync.factory;

import com.taosdata.tsync.TQueueConsumer;
import com.taosdata.tsync.entity.config.ConsumerConfiguration;
import com.taosdata.tsync.entity.producer.ProducerConfig;

import java.util.Properties;

public class TQueueConsumerFactory {

    public static TQueueConsumer build(ConsumerConfiguration consumerConfiguration) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.HOST_CONFIG, consumerConfiguration.getHost());
        props.setProperty(ProducerConfig.PORT_CONFIG, consumerConfiguration.getPort().toString());
        props.setProperty(ProducerConfig.USER_CONFIG, consumerConfiguration.getUser());
        props.setProperty(ProducerConfig.PASSWORD_CONFIG, consumerConfiguration.getPassword());
        props.setProperty(ProducerConfig.CHARSET_CONFIG, consumerConfiguration.getCharset());
        props.setProperty(ProducerConfig.LOCALE_CONFIG, consumerConfiguration.getLocale());
        props.setProperty(ProducerConfig.TIMEZONE_CONFIG, consumerConfiguration.getTimezone());
        props.setProperty(ProducerConfig.SERIALIZER, consumerConfiguration.getSerializer());
        return new TQueueConsumer(props);
    }
}
