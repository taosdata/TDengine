package com.taosdata.tsync.serializer;

public interface Serializer<T> {

    byte[] serialize(T message) throws Exception;
}