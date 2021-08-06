package com.taosdata.tsync.entity.config;

import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.enums.TQueueConstants;

public class ProducerConfiguration extends Configuration {

    public static final Integer PORT_DEFAULT = 6041;
    public static final String USER_DEFAULT = "root";
    public static final String PASSWORD_DEFAULT = TQueueConstants.DEFAULT_PASSWORD;
    public static final String CHARSET_DEFAULT = "UTF-8";
    public static final String LOCALE_DEFAULT = "en_US.UTF-8";
    public static final String TIMEZONE_DEFAULT = "UTC-8";
    public static final String SERIALIZER_DEFAULT = "STRING";

    private String host;
    private Integer port;
    private String user;
    private String password;
    private String charset;
    private String locale;
    private String timezone;
    private String serializer;

    public ProducerConfiguration() {
        super(ConfigurationType.PRODUCER);
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getCharset() {
        return charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

    public String getLocale() {
        return locale;
    }

    public void setLocale(String locale) {
        this.locale = locale;
    }

    public String getTimezone() {
        return timezone;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }

    public String getSerializer() {
        return serializer;
    }

    public void setSerializer(String serializer) {
        this.serializer = serializer;
    }
}
