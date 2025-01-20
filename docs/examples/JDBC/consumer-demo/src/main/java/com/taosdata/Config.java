package com.taosdata;

public class Config {
    public static final String TOPIC = "test_consumer";
    public static final String TAOS_HOST = "127.0.0.1";
    public static final String TAOS_PORT = "6041";
    public static final String TAOS_TYPE = "ws";
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

    public static Config getFromENV() {
        String host = System.getenv("TAOS_HOST") != null ? System.getenv("TAOS_HOST") : TAOS_HOST;
        String port = System.getenv("TAOS_PORT") != null ? System.getenv("TAOS_PORT") : TAOS_PORT;
        String type = System.getenv("TAOS_TYPE") != null ? System.getenv("TAOS_TYPE") : TAOS_TYPE;

        String c = System.getenv("TAOS_JDBC_CONSUMER_NUM");
        int num = c != null ? Integer.parseInt(c) : TAOS_JDBC_CONSUMER_NUM;

        String p = System.getenv("TAOS_JDBC_PROCESSOR_NUM");
        int capacity = p != null ? Integer.parseInt(p) : TAOS_JDBC_PROCESSOR_NUM;

        String r = System.getenv("TAOS_JDBC_RATE_PER_PROCESSOR");
        int rate = r != null ? Integer.parseInt(r) : TAOS_JDBC_RATE_PER_PROCESSOR;

        String s = System.getenv("TAOS_JDBC_POLL_SLEEP");
        int sleep = s != null ? Integer.parseInt(s) : TAOS_JDBC_POLL_SLEEP;

        return new Config(type, host, port, num, capacity, rate, sleep);
    }
}
