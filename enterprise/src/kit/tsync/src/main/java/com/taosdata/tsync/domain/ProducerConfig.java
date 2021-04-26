package com.taosdata.tsync.domain;

public class ProducerConfig extends AbstractBaseConfig {

    public static final String STRING_SERIALIZER = com.taosdata.tsync.serializer.TQueueStringSerializer.class.getName();
    public static final String AVRO_SERIALIZER = com.taosdata.tsync.serializer.TQueueAvroSerializer.class.getName();

    public static final String SERIALIZER = "serializer";

    private ProducerConfig() {
    }
}
