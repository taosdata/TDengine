package com.taosdata.jdbc.ws.entity;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * generate id for request
 */
public class IdUtil {
    private static final Map<String, AtomicLong> ids = new HashMap<>();

    public static long getId(String action) {
        return ids.get(action).incrementAndGet();
    }

    public static void init(String action) {
        ids.put(action, new AtomicLong(0));
    }
}
