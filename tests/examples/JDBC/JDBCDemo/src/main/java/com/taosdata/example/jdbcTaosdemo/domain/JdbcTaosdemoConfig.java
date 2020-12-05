package com.taosdata.example.jdbcTaosdemo.domain;

public final class JdbcTaosdemoConfig {

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
    private int keep = 36500;
    //days
    private int days = 120;

    //Super table Name. Default is 'meters'
    private String stbName = "meters";
    //Table name prefix. Default is 'd'
    private String tbPrefix = "d";
    //The number of tables. Default is 10.
    private int numberOfTable = 10;
    //The number of records per table. Default is 2
    private int numberOfRecordsPerTable = 2;
    //The number of records per request. Default is 100
    private int numberOfRecordsPerRequest = 100;

    //The number of threads. Default is 1.
    private int numberOfThreads = 1;
    //Delete data. Default is false
    private boolean deleteTable = false;

    public static void printHelp() {
        System.out.println("Usage: java -jar JdbcTaosDemo.jar [OPTION...]");
        System.out.println("-h    host                       The host to connect to TDengine. you must input one");
        System.out.println("-p    port                       The TCP/IP port number to use for the connection. Default is 6030");
        System.out.println("-u    user                       The TDengine user name to use when connecting to the server. Default is 'root'");
        System.out.println("-P    password                   The password to use when connecting to the server.Default is 'taosdata'");
        System.out.println("-d    database                   Destination database. Default is 'test'");
        System.out.println("-m    tablePrefix                Table prefix name. Default is 'd'");
        System.out.println("-t    num_of_tables              The number of tables. Default is 10");
        System.out.println("-n    num_of_records_per_table   The number of records per table. Default is 2");
        System.out.println("-r    num_of_records_per_req     The number of records per request. Default is 100");
        System.out.println("-T    num_of_threads             The number of threads. Default is 1");
        System.out.println("-D    delete table               Delete data methods. Default is false");
        System.out.println("--help                           Give this help list");
//        System.out.println("--infinite                       infinite insert mode");
    }

    /**
     * parse args from command line
     *
     * @param args command line args
     * @return JdbcTaosdemoConfig
     */
    public JdbcTaosdemoConfig(String[] args) {
        for (int i = 0; i < args.length; i++) {
            if ("-h".equals(args[i]) && i < args.length - 1) {
                host = args[++i];
            }
            if ("-p".equals(args[i]) && i < args.length - 1) {
                port = Integer.parseInt(args[++i]);
            }
            if ("-u".equals(args[i]) && i < args.length - 1) {
                user = args[++i];
            }
            if ("-P".equals(args[i]) && i < args.length - 1) {
                password = args[++i];
            }
            if ("-d".equals(args[i]) && i < args.length - 1) {
                dbName = args[++i];
            }
            if ("-m".equals(args[i]) && i < args.length - 1) {
                tbPrefix = args[++i];
            }
            if ("-t".equals(args[i]) && i < args.length - 1) {
                numberOfTable = Integer.parseInt(args[++i]);
            }
            if ("-n".equals(args[i]) && i < args.length - 1) {
                numberOfRecordsPerTable = Integer.parseInt(args[++i]);
            }
            if ("-r".equals(args[i]) && i < args.length - 1) {
                numberOfRecordsPerRequest = Integer.parseInt(args[++i]);
            }
            if ("-T".equals(args[i]) && i < args.length - 1) {
                numberOfThreads = Integer.parseInt(args[++i]);
            }
            if ("-D".equals(args[i]) && i < args.length - 1) {
                deleteTable = Boolean.parseBoolean(args[++i]);
            }
        }
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getDbName() {
        return dbName;
    }

    public int getKeep() {
        return keep;
    }

    public int getDays() {
        return days;
    }

    public String getStbName() {
        return stbName;
    }

    public String getTbPrefix() {
        return tbPrefix;
    }

    public int getNumberOfTable() {
        return numberOfTable;
    }

    public int getNumberOfRecordsPerTable() {
        return numberOfRecordsPerTable;
    }

    public int getNumberOfThreads() {
        return numberOfThreads;
    }

    public boolean isDeleteTable() {
        return deleteTable;
    }

    public int getNumberOfRecordsPerRequest() {
        return numberOfRecordsPerRequest;
    }
}
