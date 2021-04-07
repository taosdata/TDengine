package com.taosdata.tsync.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Utils {
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
}
