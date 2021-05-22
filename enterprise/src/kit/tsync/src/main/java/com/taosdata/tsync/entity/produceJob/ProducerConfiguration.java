package com.taosdata.tsync.entity.produceJob;

import com.taosdata.tsync.entity.Configuration;
import com.taosdata.tsync.entity.ConfigurationType;

public class ProducerConfiguration extends Configuration {
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
