package com.taosdata.iot.clients;

/**
 * @author Jiangyi Hou
 * @since 19-1-30
 */
public class SqlSubscription {

    private String host;
    private String database;
    private String topic;
    private String user;
    private String password;
    private long timestamp;
    private int period;

    private long subscribePointer = 0l;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
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

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public int getPeriod() {
        return period;
    }

    public void setPeriod(int period) {
        this.period = period;
    }

    public long getSubscribePointer() {
        return subscribePointer;
    }

    public void setSubscribePointer(long subscribePointer) {
        this.subscribePointer = subscribePointer;
    }

    public boolean isValid() {
        boolean isValid = true;
        return isValid;
    }
}
