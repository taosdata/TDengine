package com.taosdata.jdbc;

public class TaosGlobalConfig {
    static String charset = "";

    public static String getCharset() {
        if (charset == null || charset.isEmpty()) {
            charset = System.getProperty("file.encoding");
        }
        return charset;
    }

    public static void setCharset(String tsCharset) {
        TaosGlobalConfig.charset = tsCharset;
    }
}
