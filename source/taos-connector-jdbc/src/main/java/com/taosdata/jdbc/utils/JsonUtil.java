package com.taosdata.jdbc.utils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.introspect.VisibilityChecker;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.taosdata.jdbc.ws.tmq.meta.*;

import java.text.SimpleDateFormat;

public class JsonUtil {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        configureObjectMapper(objectMapper);
    }

    private JsonUtil() {
        // private constructor to prevent instantiation
    }

    public static ObjectMapper getObjectMapper() {
        return objectMapper;
    }
    public static ObjectReader getObjectReader(Class<?> clazz) {
        return objectMapper.readerFor(clazz);
    }

    public static ObjectReader getObjectReader() {
        return objectMapper.reader();
    }
    public static ObjectWriter getObjectWriter(Class<?> clazz) {
        return objectMapper.writerFor(clazz);
    }

    public static ObjectWriter getObjectWriter() {
        return objectMapper.writer();
    }

    private static void configureObjectMapper(ObjectMapper objectMapper) {
        // ignore unknown properties
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        // ignore null values
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        // timestamp format
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        objectMapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));

        // set visibility for fields, getters, setters, and constructors
        objectMapper.setVisibility(VisibilityChecker.Std.defaultInstance()
                .withFieldVisibility(JsonAutoDetect.Visibility.NONE)  // disable field detection
                .withGetterVisibility(JsonAutoDetect.Visibility.PUBLIC_ONLY)  // only detect public getters
                .withSetterVisibility(JsonAutoDetect.Visibility.PUBLIC_ONLY)  // only detect public setters
                .withCreatorVisibility(JsonAutoDetect.Visibility.NONE));  // disable constructor detection

        // register JavaTimeModule
        objectMapper.registerModule(new JavaTimeModule());

        // register meta deserializers
        SimpleModule module = new SimpleModule();

        module.addDeserializer(AlterType.class, new AlterTypeDeserializer());
        module.addDeserializer(TableType.class, new TableTypeDeserializer());
        module.addDeserializer(MetaType.class, new MetaTypeDeserializer());
        module.addDeserializer(Meta.class, new MetaDeserializer());

        objectMapper.registerModule(module);
    }
}
