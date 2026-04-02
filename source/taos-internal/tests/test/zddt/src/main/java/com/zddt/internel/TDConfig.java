package com.zddt.internel;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;

public class TDConfig {
    public static String configFileName;

    // program section
    public static int batchSize;  //no configuration required
    public static int cacheRows;
    public static int threadNum;

    // log section
    public static String logDir;
    public static int debugFlag;
    public static long numOfLogLines;

    // jdbc section
    public static String jdbcUrl;
    public static int jdbcThreadNum;
    public static ArrayList<String> jdbcSqls = new ArrayList<String>();

    // task section
    public static String localFile;
    public static String localDir;
    public static char split;
    public static int colSize;
    public static int maxColSize;

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

    // timestamp section
    public static String timestampPattern;
    public static long autoTimestampBegin;
    public static int autoTimestampInterval;

    // stable section
    public static String stableName;

    // table section
    public static String tableNameColumn;
    private static ArrayList<Integer> tableNameColumnsRead = new ArrayList<Integer>();
    public static int tableNameColumns[];
    public static String tablePrefix;
    public static int tableFieldSize;
    public static int tableTagSize;
    public static int tableNameIgnoreFrontChars;
    public static int tableNameIgnoreBackChars;

    // fields section
    private static ArrayList<TDField> fieldsRead = new ArrayList<TDField>();
    public static TDField fields[];

    // tags section
    private static ArrayList<TDField> tagsRead = new ArrayList<TDField>();
    public static TDField tags[];

    public static void init() {
        // program section
        batchSize = 3000;
        cacheRows = 100000;
        threadNum = 5;

        logDir = ".";
        debugFlag = 195;
        numOfLogLines = 1000000;

        // jdbc section
        jdbcUrl = "";
        jdbcThreadNum = 1;

        // task section
        localFile = "";
        localDir = "";
        split = ',';
        colSize = 0;

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

        // timestamp section
        timestampPattern = "yyyyMMdd/HHmmss";
        autoTimestampBegin = TDUtil.getTimeStampUs();
        autoTimestampInterval = 10;

        // stable section
        stableName = "st";

        // table section
        tablePrefix = "t";
        tableFieldSize = 0;
        tableTagSize = 0;
        tableNameIgnoreFrontChars = 0;
        tableNameIgnoreBackChars = 0;
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
                if (option.equalsIgnoreCase("cacheRows")) {
                    cacheRows = Integer.valueOf(value);
                } else if (option.equalsIgnoreCase("threadNum")) {
                    threadNum = Integer.valueOf(value);
                }

                // log section
                else if (option.equalsIgnoreCase("logDir")) {
                    logDir = value;
                } else if (option.equalsIgnoreCase("numOfLogLines")) {
                    numOfLogLines = Long.valueOf(value);
                } else if (option.equalsIgnoreCase("debugFlag")) {
                    debugFlag = Integer.valueOf(value);
                }

                // task section
                else if (option.equalsIgnoreCase("localFile")) {
                    localFile = value;
                } else if (option.equalsIgnoreCase("localDir")) {
                    localDir = value;
                } else if (option.equalsIgnoreCase("split")) {
                    split = value.charAt(0);
                } else if (option.equalsIgnoreCase("colSize")) {
                    colSize = Integer.valueOf(value);
                }

                // jdbc section
                else if (option.equalsIgnoreCase("jdbcUrl")) {
                    jdbcUrl = value;
                } else if (option.equalsIgnoreCase("jdbcThreadNum")) {
                    jdbcThreadNum = Integer.valueOf(value);
                }// fields array
                else if (option.equalsIgnoreCase("jdbcSql")) {
                    String jdbcSql = s.substring(8);
                    jdbcSqls.add(jdbcSql.trim());
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

                // timestamp section
                else if (option.equalsIgnoreCase("timestampPattern")) {
                    timestampPattern = value;
                    if (words.length > 2) {
                        timestampPattern = timestampPattern + " " + words[2];
                    }
                } else if (option.equalsIgnoreCase("autoTimestampBegin")) {
                    autoTimestampBegin = TDUtil.getTimeMsFromYYYYMMDD(value);
                }else if (option.equalsIgnoreCase("autoTimestampInterval")) {
                    autoTimestampInterval = Integer.valueOf(value);;
                }

                // stable configuration items
                else if (option.equalsIgnoreCase("stableName")) {
                    stableName = value;
                }

                // table configuration items
                else if (option.equalsIgnoreCase("tableNameColumn")) {
                    tableNameColumn = value;
                } else if (option.equalsIgnoreCase("tablePrefix")) {
                    tablePrefix = value;
                } else if (option.equalsIgnoreCase("tableNameIgnoreFrontChars")) {
                    tableNameIgnoreFrontChars = Integer.valueOf(value);
                } else if (option.equalsIgnoreCase("tableNameIgnoreBackChars")) {
                    tableNameIgnoreBackChars = Integer.valueOf(value);
                } else if (option.equalsIgnoreCase("tableFieldSize")) {
                    tableFieldSize = Integer.valueOf(value);
                } else if (option.equalsIgnoreCase("tableTagSize")) {
                    tableTagSize = Integer.valueOf(value);
                }

                // fields array
                else if (option.equalsIgnoreCase("fieldColumn")) {
                    if (words.length < 4)
                        continue;
                    fieldsRead.add(new TDField(words[1], words[2], words[3]));
                }
                // tags array
                else if (option.equalsIgnoreCase("tagColumn")) {
                    if (words.length < 4)
                        continue;
                    tagsRead.add(new TDField(words[1], words[2], words[3]));
                } else {
                }
            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
            TDLog.error(String.format("failed to read config file:%s, error:%s", _configFileName, e.getMessage()));
            return false;
        }

        TDLog.print(String.format("read config file:%s, successful", _configFileName));
        return parse();
    }


    private static boolean parse() {
        boolean parsedOk = true;

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

        if (cacheRows < 10000 || cacheRows > 5000000) {
            TDLog.error(String.format("cacheRows range [10000, 5000000], input:%d, use default 10000", cacheRows));
            cacheRows = 10000;
        }

        if (threadNum < 1 || threadNum > 30) {
            TDLog.error(String.format("threadNum range [1, 30], input:%d, use default 1", threadNum));
            threadNum = 1;
        }

        if (jdbcThreadNum < 1 || jdbcThreadNum > 30) {
            TDLog.error(String.format("jdbcThreadNum range [1, 30], input:%d, use default 1", jdbcThreadNum));
            jdbcThreadNum = 1;
        }

        if (colSize < 2 || colSize > 10000) {
            TDLog.error(String.format("colSize range [2, 10000], input:%d", colSize));
            parsedOk = false;
        }
        maxColSize = colSize + 5;


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
        if (tableNameColumn.length() < 1) {
            TDLog.error(String.format("tableNameColumn is empty"));
            parsedOk = false;
        } else {
            String[] words = tableNameColumn.split("\\$");
            for (int i = 1; i < (int) words.length; ++i) {
                int pos = Integer.valueOf(words[i]);
                tableNameColumnsRead.add(pos);
                if (pos < 0 || pos >= colSize) {
                    TDLog.error(String.format("tableNameColumn pos:%d not in range [0, %d]", i, colSize));
                    parsedOk = false;
                }
            }
            if (tableNameColumnsRead.size() < 1 || tableNameColumnsRead.size() > 100) {
                TDLog.error(String.format("tableNameColumn size range [1, 100], input:%d", tableNameColumnsRead.size()));
                parsedOk = false;
            }
        }
        if (tablePrefix.length() < 0 || tablePrefix.length() > 10) {
            TDLog.error(String.format("tablePrefix size range [0, 10], input:%d", tablePrefix.length()));
            parsedOk = false;
        }
        if (tableFieldSize < 2 || tableFieldSize > 250) {
            TDLog.error(String.format("tableFieldSize range [2, 250], input:%d", tableFieldSize));
            parsedOk = false;
        }
        if (tableTagSize < 1 || tableTagSize > 6) {
            TDLog.error(String.format("tableTagSize range [1, 6], input:%d", tableTagSize));
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

        if ((int) fieldsRead.size() != tableFieldSize) {
            TDLog.error(String.format("the size:%d of fields not equal with tableFieldSize:%d, quit", fieldsRead.size(), tableFieldSize));
            parsedOk = false;
        }
        for (int f = 0; f < (int) fieldsRead.size(); ++f) {
            TDField field = fieldsRead.get(f);
            if (field.isUseTableName)
                continue;
            if (field.isUseFileName)
                continue;

            String[] words = field.column.split("\\$+");
            for (int i = 1; i < (int) words.length; ++i) {
                int pos = Integer.valueOf(words[i]);
                field.columnsRead.add(pos);
                if (pos < 0 || pos >= colSize) {
                    TDLog.error(String.format("fields:%d pos:%d not in range [0, %d]", f, i, colSize));
                    parsedOk = false;
                }
            }
            if (field.columnsRead.size() < 1 || field.columnsRead.size() > 100) {
                TDLog.error(String.format("field:%d size range [1, 100], input:%d", f, field.columnsRead.size()));
                parsedOk = false;
            }
        }
        for (int i = 0; i < (int) fieldsRead.size(); ++i) {
            for (int j = i + 1; j < (int) fieldsRead.size(); ++j) {
                if (fieldsRead.get(i).name.equalsIgnoreCase(fieldsRead.get(j).name)) {
                    TDLog.error(String.format("field:%d name:%s equal with field:%d", i, fieldsRead.get(i).name, j));
                    parsedOk = false;
                }
            }
        }
        if (fieldsRead.size() > 0) {
            if (!fieldsRead.get(0).type.equalsIgnoreCase("timestamp")) {
                TDLog.error(String.format("field:0 type must be timestamp"));
                parsedOk = false;
            }
        }

        // tags section
        if ((int) tagsRead.size() != tableTagSize) {
            TDLog.error(String.format("the size:%d of tags not equal with tableTagSize:%d, quit", tagsRead.size(), tableTagSize));
            parsedOk = false;
        }
        for (int f = 0; f < (int) tagsRead.size(); ++f) {
            TDField tag = tagsRead.get(f);
            if (tag.isUseTableName)
                continue;
            if (tag.isUseFileName)
                continue;

            String[] words = tag.column.split("\\$");
            for (int i = 1; i < (int) words.length; ++i) {
                int pos = Integer.valueOf(words[i]);
                tag.columnsRead.add(pos);
                if (pos < 0 || pos >= colSize) {
                    TDLog.error(String.format("tag:%d pos:%d not in range [0, %d]", f, i, colSize));
                    parsedOk = false;
                }
            }
            if (tag.columnsRead.size() < 1 || tag.columnsRead.size() > 100) {
                TDLog.error(String.format("tag:%d size range [1, 100], input:%d", f, tag.columnsRead.size()));
                parsedOk = false;
            }
        }
        for (int i = 0; i < (int) tagsRead.size(); ++i) {
            for (int j = i + 1; j < (int) tagsRead.size(); ++j) {
                if (tagsRead.get(i).name.equalsIgnoreCase(tagsRead.get(j).name)) {
                    TDLog.error(String.format("tag:%d name:%s equal with tag:%d", i, tagsRead.get(i).name, j));
                    parsedOk = false;
                }
            }
        }
        for (int i = 0; i < (int) tagsRead.size(); ++i) {
            for (int j = 0; j < (int) fieldsRead.size(); ++j) {
                if (tagsRead.get(i).name.equalsIgnoreCase(fieldsRead.get(j).name)) {
                    TDLog.error(String.format("tag:%d name:%s equal with field:%d", i, tagsRead.get(i).name, j));
                    parsedOk = false;
                }
            }
        }

        if (datadbMicroSecond) {
            autoTimestampBegin = TDUtil.getTimeStampUs();
        } else {
            autoTimestampBegin = TDUtil.getTimeStampMs();
        }

        fields = new TDField[fieldsRead.size()];
        for (int i = 0; i < (int) fieldsRead.size(); ++i) {
            fields[i] = fieldsRead.get(i);
            fields[i].normalize();
        }

        tags = new TDField[tagsRead.size()];
        for (int i = 0; i < (int) tagsRead.size(); ++i) {
            tags[i] = tagsRead.get(i);
            tags[i].normalize();
        }

        tableNameColumns = new int[tableNameColumnsRead.size()];
        for (int i = 0; i < (int) tableNameColumnsRead.size(); ++i) {
            tableNameColumns[i] = tableNameColumnsRead.get(i);
        }

        if (jdbcSqls.size() == 0) {
            jdbcThreadNum = 1;
        }

        if (!parsedOk) {
            TDLog.error(String.format("failed to parse config file:%s", configFileName));
        } else {
            print();
        }

        return parsedOk;
    }

    public static void print() {
        TDLog.print(String.format("=================================="));

        // program section
        TDLog.print(String.format("cacheRows:                %d", cacheRows));
        TDLog.print(String.format("threadNum:                %d", threadNum));

        // logfile section
        TDLog.print(String.format("logDir:                   %s", logDir));
        TDLog.print(String.format("debugFlag:                %d", debugFlag));
        TDLog.print(String.format("numOfLogLines:            %d", numOfLogLines));

        TDLog.print(String.format("jdbcUrl:                  %s", jdbcUrl));
        TDLog.print(String.format("jdbcThreadNum:            %d", jdbcThreadNum));
        for (int i = 0; i < jdbcSqls.size(); ++i) {
            TDLog.print(String.format("jdbcSql%d:                 %s", i, jdbcSqls.get(i)));
        }

        TDLog.print(String.format("localFile:                %s", localFile));
        TDLog.print(String.format("localDir:                 %s", localDir));
        TDLog.print(String.format("split:                    %c", split));
        TDLog.print(String.format("colSize:                  %d", colSize));

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

        // timestamp section
        TDLog.print(String.format("timestampPattern:         %s", timestampPattern));
        TDLog.print(String.format("autoTimestampBegin:       %d", autoTimestampBegin));
        TDLog.print(String.format("autoTimestampInterval:    %d", autoTimestampInterval));

        // stable section
        TDLog.print(String.format("stableName:               %s", stableName));

        // table section
        TDLog.print(String.format("tableNameColumn:          %s", tableNameColumn));
        TDLog.print(String.format("tablePrefix:              %s", tablePrefix));
        TDLog.print(String.format("tableFieldSize:           %d", tableFieldSize));
        TDLog.print(String.format("tableTagSize:             %d", tableTagSize));
        TDLog.print(String.format("tableNameIgnoreFrontChars:%d", tableNameIgnoreFrontChars));
        TDLog.print(String.format("tableNameIgnoreBackChars: %d", tableNameIgnoreBackChars));

        for (int i = 0; i < (int) fieldsRead.size(); ++i) {
            TDLog.print(String.format("fieldColumn:              %s %s %s", fieldsRead.get(i).column, fieldsRead.get(i).name, fieldsRead.get(i).type));
        }

        for (int i = 0; i < (int) tagsRead.size(); ++i) {
            TDLog.print(String.format("tagColumn:                %s %s %s", tagsRead.get(i).column, tagsRead.get(i).name, tagsRead.get(i).type));
        }

        TDLog.print(String.format("version:                  %s", "1.0.0"));
        TDLog.print(String.format("=================================="));
    }

    public static synchronized long getAutoTimestamp() {
        TDConfig.autoTimestampBegin += TDConfig.autoTimestampInterval;
        return TDConfig.autoTimestampBegin;
    }
}