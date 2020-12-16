package com.taosdata.taosdemo.utils;

public final class JdbcTaosdemoConfig {
    // instance
    public String host;                     //host
    public int port = 6030;                 //port
    public String user = "root";            //user
    public String password = "taosdata";    //password
    // database
    public String database = "test";        //database
    public int keep = 3650;                 //keep
    public int days = 30;                   //days
    public int replica = 1;                 //replica
    //super table
    public boolean doCreateTable = true;
    public String superTable = "weather";   //super table name
    public String prefixOfFields = "col";
    public int numOfFields;
    public String prefixOfTags = "tag";
    public int numOfTags;
    public String superTableSQL;
    //sub table
    public String prefixOfTable = "t";
    // insert task
    public boolean autoCreateTable = true;
    public long numOfTables = 100;
    public long numOfRowsPerTable = 100;
    public int numOfTablesPerSQL = 10;
    public int numOfValuesPerSQL = 10;
    public int numOfThreadsForCreate = 1;
    public int numOfThreadsForInsert = 1;
    public long startTime;
    public long timeGap = 1;
    public int frequency;
    public int order;
    public int rate = 10;
    public long range = 1000l;
    // select task

    // drop task
    public boolean dropTable = false;

    public static void printHelp() {
        System.out.println("Usage: java -jar jdbc-taosdemo-2.0.jar [OPTION...]");
        // instance
        System.out.println("-host                       The host to connect to TDengine which you must specify");
        System.out.println("-port                       The TCP/IP port number to use for the connection. Default is 6030");
        System.out.println("-user                       The TDengine user name to use when connecting to the server. Default is 'root'");
        System.out.println("-password                   The password to use when connecting to the server.Default is 'taosdata'");
        // database
        System.out.println("-database                   Destination database. Default is 'test'");
        System.out.println("-keep                       database keep parameter. Default is 3650");
        System.out.println("-days                       database days parameter. Default is 30");
        System.out.println("-replica                    database replica parameter. Default 1, min: 1, max: 3");
        // super table
        System.out.println("-doCreateTable              do create super table and sub table, true or false, Default true");
        System.out.println("-superTable                 super table name. Default 'weather'");
        System.out.println("-prefixOfFields             The prefix of field in super table. Default is 'col'");
        System.out.println("-numOfFields                The number of field in super table. Default is (ts timestamp, temperature float, humidity int).");
        System.out.println("-prefixOfTags               The prefix of tag in super table. Default is 'tag'");
        System.out.println("-numOfTags                  The number of tag in super table. Default is (location nchar(64), groupId int).");
        System.out.println("-superTableSQL              specify a sql statement for the super table.\n" +
                "                            Default is 'create table weather(ts timestamp, temperature float, humidity int) tags(location nchar(64), groupId int). \n" +
                "                            if you use this parameter, the numOfFields and numOfTags will be invalid'");
        // sub table
        System.out.println("-prefixOfTable              The prefix of sub tables. Default is 't'");
        System.out.println("-numOfTables                The number of tables. Default is 1");
        System.out.println("-numOfThreadsForCreate      The number of thread during create sub table. Default is 1");
        // insert task
        System.out.println("-autoCreateTable            Use auto Create sub tables SQL. Default is false");
        System.out.println("-numOfRowsPerTable          The number of records per table. Default is 1");
        System.out.println("-numOfThreadsForInsert      The number of threads during insert row. Default is 1");
        System.out.println("-numOfTablesPerSQL          The number of table per SQL. Default is 1");
        System.out.println("-numOfValuesPerSQL          The number of value per SQL. Default is 1");
        System.out.println("-startTime                  start time for insert task, The format is \"yyyy-MM-dd HH:mm:ss.SSS\".");
        System.out.println("-timeGap                    the number of time gap. Default is 1000 ms");
        System.out.println("-frequency                  the number of records per second inserted into one table. default is 0, do not control frequency");
        System.out.println("-order                      Insert mode--0: In order, 1: Out of order. Default is in order");
        System.out.println("-rate                       The proportion of data out of order. effective only if order is 1. min 0, max 100, default is 10");
        System.out.println("-range                      The range of data out of order. effective only if order is 1. default is 1000 ms");
        // query task
//        System.out.println("-sqlFile                   The select sql file");
        // drop task
        System.out.println("-dropTable                  Drop data before quit. Default is false");
        System.out.println("--help                      Give this help list");
    }

    /**
     * parse args from command line
     *
     * @param args command line args
     * @return JdbcTaosdemoConfig
     */
    public JdbcTaosdemoConfig(String[] args) {
        for (int i = 0; i < args.length; i++) {
            // instance
            if ("-host".equals(args[i]) && i < args.length - 1) {
                host = args[++i];
            }
            if ("-port".equals(args[i]) && i < args.length - 1) {
                port = Integer.parseInt(args[++i]);
            }
            if ("-user".equals(args[i]) && i < args.length - 1) {
                user = args[++i];
            }
            if ("-password".equals(args[i]) && i < args.length - 1) {
                password = args[++i];
            }
            // database
            if ("-database".equals(args[i]) && i < args.length - 1) {
                database = args[++i];
            }
            if ("-keep".equals(args[i]) && i < args.length - 1) {
                keep = Integer.parseInt(args[++i]);
            }
            if ("-days".equals(args[i]) && i < args.length - 1) {
                days = Integer.parseInt(args[++i]);
            }
            if ("-replica".equals(args[i]) && i < args.length - 1) {
                replica = Integer.parseInt(args[++i]);
            }
            // super table
            if ("-doCreateTable".equals(args[i]) && i < args.length - 1) {
                doCreateTable = Boolean.parseBoolean(args[++i]);
            }
            if ("-superTable".equals(args[i]) && i < args.length - 1) {
                superTable = args[++i];
            }
            if ("-prefixOfFields".equals(args[i]) && i < args.length - 1) {
                prefixOfFields = args[++i];
            }
            if ("-numOfFields".equals(args[i]) && i < args.length - 1) {
                numOfFields = Integer.parseInt(args[++i]);
            }
            if ("-prefixOfTags".equals(args[i]) && i < args.length - 1) {
                prefixOfTags = args[++i];
            }
            if ("-numOfTags".equals(args[i]) && i < args.length - 1) {
                numOfTags = Integer.parseInt(args[++i]);
            }
            if ("-superTableSQL".equals(args[i]) && i < args.length - 1) {
                superTableSQL = args[++i];
            }
            // sub table
            if ("-prefixOfTable".equals(args[i]) && i < args.length - 1) {
                prefixOfTable = args[++i];
            }
            if ("-numOfTables".equals(args[i]) && i < args.length - 1) {
                numOfTables = Long.parseLong(args[++i]);
            }
            if ("-autoCreateTable".equals(args[i]) && i < args.length - 1) {
                autoCreateTable = Boolean.parseBoolean(args[++i]);
            }
            if ("-numOfThreadsForCreate".equals(args[i]) && i < args.length - 1) {
                numOfThreadsForCreate = Integer.parseInt(args[++i]);
            }
            // insert task
            if ("-numOfRowsPerTable".equals(args[i]) && i < args.length - 1) {
                numOfRowsPerTable = Long.parseLong(args[++i]);
            }
            if ("-numOfThreadsForInsert".equals(args[i]) && i < args.length - 1) {
                numOfThreadsForInsert = Integer.parseInt(args[++i]);
            }
            if ("-numOfTablesPerSQL".equals(args[i]) && i < args.length - 1) {
                numOfTablesPerSQL = Integer.parseInt(args[++i]);
            }
            if ("-numOfValuesPerSQL".equals(args[i]) && i < args.length - 1) {
                numOfValuesPerSQL = Integer.parseInt(args[++i]);
            }
            if ("-startTime".equals(args[i]) && i < args.length - 1) {
                startTime = TimeStampUtil.datetimeToLong(args[++i]);
            }
            if ("-timeGap".equals(args[i]) && i < args.length - 1) {
                timeGap = Long.parseLong(args[++i]);
            }
            if ("-frequency".equals(args[i]) && i < args.length - 1) {
                frequency = Integer.parseInt(args[++i]);
            }
            if ("-order".equals(args[i]) && i < args.length - 1) {
                order = Integer.parseInt(args[++i]);
            }
            if ("-rate".equals(args[i]) && i < args.length - 1) {
                rate = Integer.parseInt(args[++i]);
                if (rate < 0 || rate > 100)
                    throw new IllegalArgumentException("rate must between 0 and 100");
            }
            if ("-range".equals(args[i]) && i < args.length - 1) {
                range = Integer.parseInt(args[++i]);
            }
            // select task

            // drop task
            if ("-dropTable".equals(args[i]) && i < args.length - 1) {
                dropTable = Boolean.parseBoolean(args[++i]);
            }
        }
    }

    public static void main(String[] args) {
        JdbcTaosdemoConfig config = new JdbcTaosdemoConfig(args);
    }

}
