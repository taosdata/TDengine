package com.taosdata.jdbc.utils;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.math.BigInteger;

public class UInt64Deserializer extends JsonDeserializer<Long> {
    @Override
    public Long deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        String text = p.getText();
        try {
            return new BigInteger(text).longValue();
        } catch (NumberFormatException e) {
            throw new IOException("Unable to deserialize UInt64 value: " + text, e);
        }
    }
}