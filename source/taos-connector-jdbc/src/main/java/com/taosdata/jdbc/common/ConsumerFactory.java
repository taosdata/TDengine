package com.taosdata.jdbc.common;


public interface ConsumerFactory {

    boolean acceptsType(String type);

    Consumer<?> getConsumer();
}
