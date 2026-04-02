package com.taosdata.tsync.enums;

public enum ConfigurationType {
    PRODUCE_TO_TQUEUE,
    PRODUCER,
    TASK,
    MESSAGE,
    SCHEMA,
    DATABASE,
    STABLE,
    COLUMN,
    TAG,
    /******/
    CONSUME_TO_TDENGINE,
    CONSUMER,
    DESTINATION,
    TAOSD,
    STRATEGY,
    /******/
    CONSUME_TO_FILE,
    FILE,
    /******/
    CONSUME_TO_NET,
    NET,
    /******/
    NET_TO_TQUEUE,
    SOURCE,
    /******/
    FILE_TO_TQUEUE
}