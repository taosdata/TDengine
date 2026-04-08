package com.zddt.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.lang.reflect.Array;
import java.util.ArrayList;

public class TDConfig {
    public static String configFileName;

    // program section
    public static int batchSize;  //no configuration required
    public static long fileBeginTimestamp; //no configuration required ( for autoTimestamp generation)

    // jdbc section   
    public static String jdbcUrl;
    public static int jdbcCacheRows;
    public static int jdbcThreadNum;
    public static int jdbcSubThreadNum;
    public static ArrayList<String> jdbcSqls = new ArrayList<String>();

    // timestamp section
    public static boolean autoTimestamp;

    // log section
    public static String logDir;
    public static int debugFlag;
    public static long numOfLogLines;

    // tdengine section
    public static String user;
    public static String password;
    public static String host;

    // logdb section
    public static boolean logdbRecord;
    public static String logdbName;
    public static int logdbCache;
    public static float logdbAblocks;
    public static int logdbTblocks;
    public static int logdbTables;
    public static int logdbRows;
    public static int logdbKeep;
    public static int logdbDays;
    public static int logdbReplica;
    public static String logdbTablePrefix;

    // datadb section
    public static String datadbName;
    public static int datadbCache;
    public static float datadbAblocks;
    public static int datadbTblocks;
    public static int datadbTables;
    public static int datadbRows;
    public static int datadbKeep;
    public static int datadbDays;
    public static int datadbReplica;
    public static boolean datadbMicroSecond;

    // stable section
    public static String stableName;

    // table section
    public static String tablePrefix;
    public static int tableNameIgnoreFrontChars;
    public static int tableNameIgnoreBackChars;

    public static TDField[] fields;
    public static TDField[] tags;

    public static void init() {
        // program section
        batchSize = 3000;
        fileBeginTimestamp = TDUtil.getTimeStampUs();

        // jdbc section
        jdbcCacheRows = 100000;
        jdbcThreadNum = 1;
        jdbcSubThreadNum = 5;

        // timestamp section
        autoTimestamp = false;

        // log section
        logDir = ".";
        debugFlag = 199;
        numOfLogLines = 1000000;

        // tdengine section
        user = "root";
        password = "taosdata";
        host = "127.0.0.1";

        // logdb section
        logdbRecord = true;
        logdbName = "logdb";
        logdbCache = 4096;
        logdbAblocks = 1.0f;
        logdbTblocks = 10;
        logdbTables = 100;
        logdbRows = 4096;
        logdbKeep = 3650;
        logdbDays = 10;
        logdbReplica = 1;
        logdbTablePrefix = "jdbc";

        // datadb section
        datadbName = "db";
        datadbCache = 16384;
        datadbAblocks = 2.0f;
        datadbTblocks = 200;
        datadbTables = 5000;
        datadbRows = 4096;
        datadbKeep = 3650;
        datadbDays = 5;
        datadbReplica = 1;
        datadbMicroSecond = true;

        // stable section
        stableName = "st";

        // table section
        tablePrefix = "t";
        tableNameIgnoreFrontChars = 0;
        tableNameIgnoreBackChars = 0;
    }

    public static synchronized void setSchema(ArrayList<TDField> fieldsArray, ArrayList<TDField> tagsArray) {
        if (fields == null) {
            fields = new TDField[fieldsArray.size()];
            for (int i = 0; i < fieldsArray.size(); ++i) {
                fields[i] = fieldsArray.get(i);
            }
            tags = new TDField[tagsArray.size()];
            for (int i = 0; i < tagsArray.size(); ++i) {
                tags[i] = tagsArray.get(i);
            }
        }
    }

    public static boolean read(String _configFileName) {
        configFileName = _configFileName;
        File infile = new File(configFileName);
        StringBuilder result = new StringBuilder();
        try {
            BufferedReader br = new BufferedReader(new FileReader(infile));
            String s = null;
            while ((s = br.readLine()) != null) {
                String[] words = s.split("\\s+");
                if (words.length < 2) {
                    continue;
                }

                String option = words[0];
                String value = words[1];

                if (option.length() <= 0) {
                    continue;
                }

                if (option.charAt(0) == '#' || option.charAt(0) == ' ') {
                    continue;
                }

                // program section
                if (option.equalsIgnoreCase("jdbcUrl")) {
                    jdbcUrl = value;
                } else if (option.equalsIgnoreCase("jdbcCacheRows")) {
                    jdbcCacheRows = Integer.valueOf(value);
                } else if (option.equalsIgnoreCase("jdbcThreadNum")) {
                    jdbcThreadNum = Integer.valueOf(value);
                } else if (option.equalsIgnoreCase("jdbcSubThreadNum")) {
                    jdbcSubThreadNum = Integer.valueOf(value);
                }

                // timestamp section
                else if (option.equalsIgnoreCase("autoTimestamp")) {
                    autoTimestamp = value.equalsIgnoreCase("true");
                }

                // log section
                else if (option.equalsIgnoreCase("logDir")) {
                    logDir = value;
                } else if (option.equalsIgnoreCase("numOfLogLines")) {
                    numOfLogLines = Long.valueOf(value);
                } else if (option.equalsIgnoreCase("debugFlag")) {
                    debugFlag = Integer.valueOf(value);
                }

                // TDengine Connection Configuration
                else if (option.equalsIgnoreCase("user")) {
                    user = value;
                } else if (option.equalsIgnoreCase("password")) {
                    password = value;
                } else if (option.equalsIgnoreCase("host")) {
                    host = value;
                }

                // Log Configuration Items
                else if (option.equalsIgnoreCase("logdbRecord")) {
                    logdbRecord = value.equalsIgnoreCase("true");
                } else if (option.equalsIgnoreCase("logdbName")) {
                    logdbName = value;
                } else if (option.equalsIgnoreCase("logdbCache")) {
                    logdbCache = Integer.valueOf(value);
                } else if (option.equalsIgnoreCase("logdbAblocks")) {
                    logdbAblocks = Float.valueOf(value);
                } else if (option.equalsIgnoreCase("logdbTblocks")) {
                    logdbTblocks = Integer.valueOf(value);
                } else if (option.equalsIgnoreCase("logdbTables")) {
                    logdbTables = Integer.valueOf(value);
                } else if (option.equalsIgnoreCase("logdbRows")) {
                    logdbRows = Integer.valueOf(value);
                } else if (option.equalsIgnoreCase("logdbKeep")) {
                    logdbKeep = Integer.valueOf(value);
                } else if (option.equalsIgnoreCase("logdbDays")) {
                    logdbDays = Integer.valueOf(value);
                } else if (option.equalsIgnoreCase("logdbReplica")) {
                    logdbReplica = Integer.valueOf(value);
                } else if (option.equalsIgnoreCase("logdbTablePrefix")) {
                    logdbTablePrefix = value;
                }

                // Target database configuration items
                else if (option.equalsIgnoreCase("datadbName")) {
                    datadbName = value;
                } else if (option.equalsIgnoreCase("datadbCache")) {
                    datadbCache = Integer.valueOf(value);
                } else if (option.equalsIgnoreCase("datadbAblocks")) {
                    datadbAblocks = Float.valueOf(value);
                } else if (option.equalsIgnoreCase("datadbTblocks")) {
                    datadbTblocks = Integer.valueOf(value);
                } else if (option.equalsIgnoreCase("datadbTables")) {
                    datadbTables = Integer.valueOf(value);
                } else if (option.equalsIgnoreCase("datadbRows")) {
                    datadbRows = Integer.valueOf(value);
                } else if (option.equalsIgnoreCase("datadbKeep")) {
                    datadbKeep = Integer.valueOf(value);
                } else if (option.equalsIgnoreCase("datadbDays")) {
                    datadbDays = Integer.valueOf(value);
                } else if (option.equalsIgnoreCase("datadbReplica")) {
                    datadbReplica = Integer.valueOf(value);
                } else if (option.equalsIgnoreCase("datadbMicroSecond")) {
                    datadbMicroSecond = value.equalsIgnoreCase("true");
                }

                // stable configuration items
                else if (option.equalsIgnoreCase("stableName")) {
                    stableName = value;
                }

                // table configuration items
                else if (option.equalsIgnoreCase("tablePrefix")) {
                    tablePrefix = value;
                } else if (option.equalsIgnoreCase("tableNameIgnoreFrontChars")) {
                    tableNameIgnoreFrontChars = Integer.valueOf(value);
                } else if (option.equalsIgnoreCase("tableNameIgnoreBackChars")) {
                    tableNameIgnoreBackChars = Integer.valueOf(value);
                }

                // fields array
                else if (option.equalsIgnoreCase("jdbcSql")) {
                    String jdbcSql = s.substring(8);
                    jdbcSqls.add(jdbcSql);
                } else {
                }

            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
            TDLog.error(String.format("failed to read config file:%s, error:%s", _configFileName, e.getMessage()));
            return false;
        }

        TDLog.print(String.format("read config file:%s successful", _configFileName));
        return parse();
    }


    private static boolean parse() {
        boolean parsedOk = true;

        if (jdbcThreadNum < 1 || jdbcThreadNum > 30) {
            TDLog.error(String.format("jdbcThreadNum range [1, 30], input:%d, use default 1", jdbcThreadNum));
            jdbcThreadNum = 1;
        }

        if (jdbcSubThreadNum < 1 || jdbcSubThreadNum > 30) {
            TDLog.error(String.format("jdbcSubThreadNum range [1, 30], input:%d, use default 1", jdbcSubThreadNum));
            jdbcSubThreadNum = 1;
        }


        if (jdbcCacheRows < jdbcSubThreadNum || jdbcCacheRows > 5000000) {
            TDLog.error(String.format("jdbcCacheRows range [%d, 5000000], input:%d, use default 100", jdbcSubThreadNum, jdbcCacheRows));
            jdbcCacheRows = 100;
        }

        // log section
        if (logDir.length() == 0) {
            TDLog.error("logDir is empty");
            parsedOk = false;
        }
        if (numOfLogLines < 1000 || numOfLogLines > 100000000) {
            TDLog.error(String.format("numOfLogLines range [1000, 100000000], input:%d", numOfLogLines));
            parsedOk = false;
        }
        TDLog.init(TDConfig.logDir, TDConfig.debugFlag, TDConfig.numOfLogLines);

        // logdb section
        if (logdbRecord) {
            if (logdbName.length() < 1 || logdbName.length() > 32) {
                TDLog.error(String.format("logdbName size range [1, 32], input:%d", logdbName.length()));
                parsedOk = false;
            }
            if (logdbCache < 200 || logdbCache > 204800000) {
                TDLog.error(String.format("logdbCache range [200, 204800000], input:%d", logdbCache));
                parsedOk = false;
            }
            if (logdbAblocks < 0.01 || logdbAblocks > 1000) {
                TDLog.error(String.format("logdbAblocks range [0.01, 100], input:%f", logdbAblocks));
                parsedOk = false;
            }
            if (logdbTblocks < 1 || logdbTblocks > 100000) {
                TDLog.error(String.format("logdbTblocks range [1, 100000], input:%d", logdbTblocks));
                parsedOk = false;
            }
            if (logdbTables < 1 || logdbTables > 1000000) {
                TDLog.error(String.format("logdbTables range [1, 1000000], input:%d", logdbTables));
                parsedOk = false;
            }
            if (logdbRows < 1 || logdbRows > 100000) {
                TDLog.error(String.format("logdbRows range [1, 100000], input:%d", logdbRows));
                parsedOk = false;
            }
            if (logdbKeep < 1 || logdbKeep > 100000) {
                TDLog.error(String.format("logdbKeep range [1, 100000], input:%d", logdbKeep));
                parsedOk = false;
            }
            if (logdbDays < 1 || logdbDays > 100) {
                TDLog.error(String.format("logdbDays range [1, 100], input:%d", logdbDays));
                parsedOk = false;
            }
            if (logdbReplica < 1 || logdbReplica > 3) {
                TDLog.error(String.format("logdbReplica range [1, 3], input:%d", logdbReplica));
                parsedOk = false;
            }
            if (logdbTablePrefix.length() < 1 || logdbTablePrefix.length() > 10) {
                TDLog.error(String.format("logdbTablePrefix size range [1, 10], input:%d", logdbTablePrefix.length()));
                parsedOk = false;
            }
        }

        // datadb section
        if (datadbName.length() < 1 || datadbName.length() > 32) {
            TDLog.error(String.format("datadbName size range [1, 32], input:%d", datadbName.length()));
            parsedOk = false;
        }
        if (datadbCache < 200 || datadbCache > 204800000) {
            TDLog.error(String.format("datadbCache range [200, 204800000], input:%d", datadbCache));
            parsedOk = false;
        }
        if (datadbAblocks < 0.01 || datadbAblocks > 1000) {
            TDLog.error(String.format("datadbAblocks range [0.01, 100], input:%f", datadbAblocks));
            parsedOk = false;
        }
        if (datadbTblocks < 1 || datadbTblocks > 100000) {
            TDLog.error(String.format("datadbTblocks range [1, 100000], input:%d", datadbTblocks));
            parsedOk = false;
        }
        if (datadbTables < 1 || datadbTables > 1000000) {
            TDLog.error(String.format("datadbTables range [1, 1000000], input:%d", datadbTables));
            parsedOk = false;
        }
        if (datadbRows < 1 || datadbRows > 100000) {
            TDLog.error(String.format("datadbRows range [1, 100000], input:%d", datadbRows));
            parsedOk = false;
        }
        if (datadbKeep < 1 || datadbKeep > 100000) {
            TDLog.error(String.format("datadbKeep range [1, 100000], input:%d", datadbKeep));
            parsedOk = false;
        }
        if (datadbDays < 1 || datadbDays > 100) {
            TDLog.error(String.format("datadbDays range [1, 100], input:%d", datadbDays));
            parsedOk = false;
        }
        if (datadbReplica < 1 || datadbReplica > 3) {
            TDLog.error(String.format("datadbReplica range [1, 3], input:%d", datadbReplica));
            parsedOk = false;
        }

        // stable section
        if (stableName.length() < 1 || stableName.length() > 32) {
            TDLog.error(String.format("stableName size range [1, 32], input:%d", stableName.length()));
            parsedOk = false;
        }

        // table section
        if (tablePrefix.length() < 0 || tablePrefix.length() > 10) {
            TDLog.error(String.format("tablePrefix size range [0, 10], input:%d", tablePrefix.length()));
            parsedOk = false;
        }
        if (tableNameIgnoreFrontChars < 0 || tableNameIgnoreFrontChars > 32) {
            TDLog.error(String.format("tableNameIgnoreFrontChars range [0, 32], input:%d", tableNameIgnoreFrontChars));
            parsedOk = false;
        }
        if (tableNameIgnoreBackChars < 0 || tableNameIgnoreBackChars > 32) {
            TDLog.error(String.format("tableNameIgnoreFrontChars range [0, 32], input:%d", tableNameIgnoreBackChars));
            parsedOk = false;
        }

        if (jdbcSqls.size() < 0) {
            TDLog.error("no jdbcSql input");
            parsedOk = false;
        }

        if (datadbMicroSecond) {
            fileBeginTimestamp = TDUtil.getTimeStampUs();
        } else {
            fileBeginTimestamp = TDUtil.getTimeStampMs();
        }

        if (!parsedOk) {
            TDLog.error(String.format("failed to parse config file:%s", configFileName));
        } else {
            print();
        }

        return parsedOk;
    }

    public static void print() {

        // program section
        TDLog.print(String.format("batchSize:                %d", batchSize));
        TDLog.print(String.format("fileBeginTimestamp:       %d", fileBeginTimestamp));

        TDLog.print(String.format("jdbcUrl:                  %s", jdbcUrl));
        TDLog.print(String.format("jdbcCacheRows:            %d", jdbcCacheRows));
        TDLog.print(String.format("jdbcThreadNum:            %d", jdbcThreadNum));
        TDLog.print(String.format("jdbcSubThreadNum:         %d", jdbcSubThreadNum));

        // timestamp section
        TDLog.print(String.format("autoTimestamp:            %s", autoTimestamp ? "true" : "false"));

        // logfile section
        TDLog.print(String.format("logDir:                   %s", logDir));
        TDLog.print(String.format("debugFlag:                %d", debugFlag));
        TDLog.print(String.format("numOfLogLines:            %d", numOfLogLines));

        // tdengine section
        TDLog.print(String.format("user:                     %s", user));
        TDLog.print(String.format("password:                 %s", password));
        TDLog.print(String.format("host:                     %s", host));

        // logdb section
        TDLog.print(String.format("logdbRecord:              %s", logdbRecord ? "true" : "false"));
        TDLog.print(String.format("logdbName:                %s", logdbName));
        TDLog.print(String.format("logdbCache:               %d", logdbCache));
        TDLog.print(String.format("logdbAblocks:             %f", logdbAblocks));
        TDLog.print(String.format("logdbTblocks:             %d", logdbTblocks));
        TDLog.print(String.format("logdbTables:              %d", logdbTables));
        TDLog.print(String.format("logdbRows:                %d", logdbRows));
        TDLog.print(String.format("logdbKeep:                %d", logdbKeep));
        TDLog.print(String.format("logdbDays:                %d", logdbDays));
        TDLog.print(String.format("logdbReplica:             %d", logdbReplica));
        TDLog.print(String.format("logdbTablePrefix:         %s", logdbTablePrefix));

        // datadb section
        TDLog.print(String.format("datadbName:               %s", datadbName));
        TDLog.print(String.format("datadbCache:              %d", datadbCache));
        TDLog.print(String.format("datadbAblocks:            %f", datadbAblocks));
        TDLog.print(String.format("datadbTblocks:            %d", datadbTblocks));
        TDLog.print(String.format("datadbTables:             %d", datadbTables));
        TDLog.print(String.format("datadbRows:               %d", datadbRows));
        TDLog.print(String.format("datadbKeep:               %d", datadbKeep));
        TDLog.print(String.format("datadbDays:               %d", datadbDays));
        TDLog.print(String.format("datadbReplica:            %d", datadbReplica));
        TDLog.print(String.format("datadbMicroSecond:        %s", datadbMicroSecond ? "true" : "false"));

        // stable section
        TDLog.print(String.format("stableName:               %s", stableName));

        // table section
        TDLog.print(String.format("tablePrefix:              %s", tablePrefix));
        TDLog.print(String.format("tableNameIgnoreFrontChars:%d", tableNameIgnoreFrontChars));
        TDLog.print(String.format("tableNameIgnoreBackChars: %d", tableNameIgnoreBackChars));

        for (int i = 0; i < jdbcSqls.size(); ++i) {
            TDLog.print(String.format("jdbcSql%d:                 %s", i, jdbcSqls.get(i)));
        }

        TDLog.print(String.format("version:                  %s", "1.0.0"));
        TDLog.print(String.format("=================================="));
    }
}