package com.taosdata.jdbc.utils;

import com.taosdata.jdbc.ws.FetchBlockData;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FetchDataUtil {
    private static final Map<Long, FetchBlockData> INSTANCE = new ConcurrentHashMap<>();
    private FetchDataUtil() {}

    public static Map<Long, FetchBlockData> getFetchMap() {
        return INSTANCE;
    }
}
