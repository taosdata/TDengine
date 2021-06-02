package com.taosdata.tsync.entity.producer;

import com.taosdata.tsync.entity.AbstractBaseConfig;

public class ProducerConfig extends AbstractBaseConfig {

    public static final String STRING_SERIALIZER = com.taosdata.tsync.serializer.TQueueStringSerializer.class.getName();
    public static final String AVRO_SERIALIZER = com.taosdata.tsync.serializer.TQueueAvroSerializer.class.getName();

    public static final String SERIALIZER = "serializer";

    private ProducerConfig() {

    }
}
