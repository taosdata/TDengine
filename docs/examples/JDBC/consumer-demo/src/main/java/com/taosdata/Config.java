package com.taosdata;

public class Config {
    public static final int TAOS_JDBC_CONSUMER_NUM = 1;
    public static final int TAOS_JDBC_PROCESSOR_NUM = 2;
    public static final int TAOS_JDBC_RATE_PER_PROCESSOR = 1000;
    public static final int TAOS_JDBC_POLL_SLEEP = 100;

    private final int consumerNum;
    private final int processCapacity;
    private final int rate;
    private final int pollSleep;
    private final String type;
    private final String host;
    private final String port;

    public Config(String type, String host, String port, int consumerNum, int processCapacity, int rate, int pollSleep) {
        this.type = type;
        this.consumerNum = consumerNum;
        this.processCapacity = processCapacity;
        this.rate = rate;
        this.pollSleep = pollSleep;
        this.host = host;
        this.port = port;
    }

    public int getConsumerNum() {
        return consumerNum;
    }

    public int getProcessCapacity() {
        return processCapacity;
    }

    public int getRate() {
        return rate;
    }

    public int getPollSleep() {
        return pollSleep;
    }

    public String getHost() {
        return host;
    }

    public String getPort() {
        return port;
    }

    public String getType() {
        return type;
    }

}
