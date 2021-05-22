package com.taosdata.tsync.utils;

import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Utils {
    private static final DateTimeFormatter formatter = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .toFormatter();

    private Utils() {
    }

    public static Map<String, Object> propsToMap(Properties properties) {
        Map<String, Object> map = new HashMap<>(properties.size());
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            if (entry.getKey() instanceof String) {
                String k = (String) entry.getKey();
                map.put(k, properties.get(k));
            } else {
                throw new RuntimeException(entry.getKey().toString() + "must be string.");
            }
        }
        return map;
    }

    public static long toMicroSecond(Timestamp timestamp) {
        long high13digits = timestamp.getTime();
        long low3digits = timestamp.getNanos() % 1000_000l / 1000;
        return high13digits * 1000 + low3digits;
    }
}
