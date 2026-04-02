package com.taosdata.jdbc.ws.tmq.meta;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;

public class MetaTypeDeserializer extends JsonDeserializer<MetaType> {
    @Override
    public MetaType deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        String value = p.getText();
        return MetaType.valueOf(value.toUpperCase());
    }
}