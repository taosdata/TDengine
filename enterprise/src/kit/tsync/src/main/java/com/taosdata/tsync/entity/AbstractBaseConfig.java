package com.taosdata.tsync.entity;

import com.taosdata.jdbc.TSDBDriver;

public abstract class AbstractBaseConfig {
    public static final String HOST_CONFIG = TSDBDriver.PROPERTY_KEY_HOST;
    public static final String PORT_CONFIG = TSDBDriver.PROPERTY_KEY_PORT;
    public static final String USER_CONFIG = TSDBDriver.PROPERTY_KEY_USER;
    public static final String PASSWORD_CONFIG = TSDBDriver.PROPERTY_KEY_PASSWORD;
    public static final String CHARSET_CONFIG = TSDBDriver.PROPERTY_KEY_CHARSET;
    public static final String LOCALE_CONFIG = TSDBDriver.PROPERTY_KEY_LOCALE;
    public static final String TIMEZONE_CONFIG = TSDBDriver.PROPERTY_KEY_TIME_ZONE;
    public static final String TIMESTAMP_FORMAT = TSDBDriver.PROPERTY_KEY_TIMESTAMP_FORMAT;
}
