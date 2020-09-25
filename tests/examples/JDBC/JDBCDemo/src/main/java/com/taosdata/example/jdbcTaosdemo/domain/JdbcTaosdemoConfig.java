package com.taosdata.example.jdbcTaosdemo.domain;

public class JdbcTaosdemoConfig {

    //The host to connect to TDengine. Must insert one
    private String host;
    //The TCP/IP port number to use for the connection. Default is 6030.
    private int port = 6030;
    //The TDengine user name to use when connecting to the server. Default is 'root'
    private String user = "root";
    //The password to use when connecting to the server. Default is 'taosdata'
    private String password = "taosdata";
    //Destination database. Default is 'test'
    private String dbName = "test";
    //keep
    private int keep = 365 * 20;
    //
    private int days = 30;
    //Super table Name. Default is 'meters'
    private String stbName = "meters";
    //Table name prefix. Default is 'd'
    private String tbPrefix = "d";
    //The number of threads. Default is 10.
    private int numberOfThreads = 10;
    //The number of tables. Default is 10000.
    private int numberOfTable = 10000;
    //The number of records per table. Default is 100000
    private int numberOfRecordsPerTable = 100000;
    //Delete data. Default is false
    private boolean deleteTable = true;

    public static void printHelp() {
        System.out.println("Usage: java -jar JDBCConnectorChecker.jar -h host [OPTION...]");
        System.out.println("-p    port                       The TCP/IP port number to use for the connection. Default is 6030");
        System.out.println("-u    user                       The TDengine user name to use when connecting to the server. Default is 'root'");
        System.out.println("-P    password                   The password to use when connecting to the server.Default is 'taosdata'");
        System.out.println("-d    database                   Destination database. Default is 'test'");
        System.out.println("-m    tablePrefix                Table prefix name. Default is 'd'");
        System.out.println("-T    num_of_threads             The number of threads. Default is 10");
        System.out.println("-t    num_of_tables              The number of tables. Default is 10000");
        System.out.println("-n    num_of_records_per_table   The number of records per table. Default is 100000");
        System.out.println("-D    delete table               Delete data methods. Default is false");
        System.out.println("--help                           Give this help list");
    }

    /**
     * parse args from command line
     *
     * @param args command line args
     * @return JdbcTaosdemoConfig
     */
    public static JdbcTaosdemoConfig build(String[] args) {

        JdbcTaosdemoConfig config = new JdbcTaosdemoConfig();
        for (int i = 0; i < args.length; i++) {
            if ("-h".equals(args[i]) && i < args.length - 1) {
                config.setHost(args[++i]);
            }
            if ("-p".equals(args[i]) && i < args.length - 1) {
                config.setPort(Integer.parseInt(args[++i]));
            }
            if ("-u".equals(args[i]) && i < args.length - 1) {
                config.setUser(args[++i]);
            }
            if ("-P".equals(args[i]) && i < args.length - 1) {
                config.setPassword(args[++i]);
            }
            if ("-d".equals(args[i]) && i < args.length - 1) {
                config.setDbName(args[++i]);
            }
            if ("-m".equals(args[i]) && i < args.length - 1) {
                config.setTbPrefix(args[++i]);
            }
            if ("-T".equals(args[i]) && i < args.length - 1) {
                config.setNumberOfThreads(Integer.parseInt(args[++i]));
            }
            if ("-t".equals(args[i]) && i < args.length - 1) {
                config.setNumberOfTable(Integer.parseInt(args[++i]));
            }
            if ("-n".equals(args[i]) && i < args.length - 1) {
                config.setNumberOfRecordsPerTable(Integer.parseInt(args[++i]));
            }
            if ("-D".equals(args[i]) && i < args.length - 1) {
                config.setDeleteTable(Boolean.parseBoolean(args[++i]));
            }
        }

        return config;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getHost() {
        return host;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
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

    public String getStbName() {
        return stbName;
    }

    public void setStbName(String stbName) {
        this.stbName = stbName;
    }

    public String getTbPrefix() {
        return tbPrefix;
    }

    public void setTbPrefix(String tbPrefix) {
        this.tbPrefix = tbPrefix;
    }

    public int getNumberOfThreads() {
        return numberOfThreads;
    }

    public void setNumberOfThreads(int numberOfThreads) {
        this.numberOfThreads = numberOfThreads;
    }

    public int getNumberOfTable() {
        return numberOfTable;
    }

    public void setNumberOfTable(int numberOfTable) {
        this.numberOfTable = numberOfTable;
    }

    public int getNumberOfRecordsPerTable() {
        return numberOfRecordsPerTable;
    }

    public void setNumberOfRecordsPerTable(int numberOfRecordsPerTable) {
        this.numberOfRecordsPerTable = numberOfRecordsPerTable;
    }

    public boolean isDeleteTable() {
        return deleteTable;
    }

    public void setDeleteTable(boolean deleteTable) {
        this.deleteTable = deleteTable;
    }

    public int getKeep() {
        return keep;
    }

    public void setKeep(int keep) {
        this.keep = keep;
    }

    public int getDays() {
        return days;
    }

    public void setDays(int days) {
        this.days = days;
    }
}
