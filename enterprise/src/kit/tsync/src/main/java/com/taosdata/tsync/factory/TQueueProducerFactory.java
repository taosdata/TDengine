package com.taosdata.tsync.factory;

import com.taosdata.tsync.TQueueProducer;
import com.taosdata.tsync.entity.config.ProducerConfiguration;
import com.taosdata.tsync.entity.producer.ProducerConfig;

import java.util.Properties;

public class TQueueProducerFactory {

    public static TQueueProducer build(ProducerConfiguration producerConfiguration) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.HOST_CONFIG, producerConfiguration.getHost());
        props.setProperty(ProducerConfig.PORT_CONFIG, producerConfiguration.getPort().toString());
        props.setProperty(ProducerConfig.USER_CONFIG, producerConfiguration.getUser());
        props.setProperty(ProducerConfig.PASSWORD_CONFIG, producerConfiguration.getPassword());
        props.setProperty(ProducerConfig.CHARSET_CONFIG, producerConfiguration.getCharset());
        props.setProperty(ProducerConfig.LOCALE_CONFIG, producerConfiguration.getLocale());
        props.setProperty(ProducerConfig.TIMEZONE_CONFIG, producerConfiguration.getTimezone());
        props.setProperty(ProducerConfig.SERIALIZER, producerConfiguration.getSerializer());
        return new TQueueProducer(props);
    }
}
