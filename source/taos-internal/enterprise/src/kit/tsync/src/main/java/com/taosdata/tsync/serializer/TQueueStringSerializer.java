package com.taosdata.tsync.serializer;

import java.io.UnsupportedEncodingException;

public class TQueueStringSerializer<T> implements Serializer<T> {
    private String encoding = "UTF8";

    @Override
    public byte[] serialize(T message) throws Exception {
        try {
            if (message == null)
                return null;
            return message.toString().getBytes(encoding);
        } catch (UnsupportedEncodingException e) {
            throw new Exception("Error when serializing string to byte[] due to unsupported encoding " + encoding);
        }
    }

}
