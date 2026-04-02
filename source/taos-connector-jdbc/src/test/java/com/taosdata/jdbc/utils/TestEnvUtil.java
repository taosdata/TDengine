package com.taosdata.jdbc.utils;

public class TestEnvUtil {
    private static final String HOST = System.getenv().getOrDefault("TDENGINE_HOST", "localhost");
    private static final int JNI_PORT = Integer.parseInt(System.getenv().getOrDefault("TDENGINE_JNI_PORT", "6030"));
    private static final int RS_PORT = Integer.parseInt(System.getenv().getOrDefault("TDENGINE_RS_PORT", "6041"));
    private static final int WS_PORT = Integer.parseInt(System.getenv().getOrDefault("TDENGINE_WS_PORT", "6041"));
    private static final String USER = System.getenv().getOrDefault("TDENGINE_USER", "root");
    private static final String PASSWORD = System.getenv().getOrDefault("TDENGINE_PASSWORD", "taosdata");

    private TestEnvUtil() {
    }

    public static String getHost() {
        return HOST;
    }
    public static int getJniPort() {
        return JNI_PORT;
    }
    public static int getRsPort() {
        return RS_PORT;
    }
    public static int getWsPort() {
        return WS_PORT;
    }

    public static String getUser() {
        return USER;
    }
    public static String getPassword() {
        return PASSWORD;
    }
}
