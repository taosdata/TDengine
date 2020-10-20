package com.hivemq.extensions.taosdata.configuration;

import java.nio.charset.Charset;

public final class Constants {
    public static final Charset UTF8 = Charset.forName("UTF-8");
    public static final String WORK_DIR = System.getProperty("user.dir");
    public static final String DEFAULT_CFG_PATH = WORK_DIR + "/extensions/taosdata-extension/";

    public static final String TAOS_CFG_FILE = "taos.properties";
    public static final String DEFAULT_TAOS_CFG_PATH = DEFAULT_CFG_PATH + TAOS_CFG_FILE;

    public static final String TOPICS_CFG_FILE = "taos.yml";
    public static final String DEFAULT_TOPICS_CFG_PATH = DEFAULT_CFG_PATH + TOPICS_CFG_FILE;
}
