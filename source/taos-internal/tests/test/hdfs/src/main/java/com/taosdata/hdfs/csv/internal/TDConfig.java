package com.taosdata.hdfs.csv.internal;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;

public class TDConfig {
    public static String configFileName;

    // program section
    public static int batchSize;  //no configuration required
    public static int maxColSize; //no configuration required
    public static long fileBeginTimestampUs = TDUtil.getTimeStampUs(); //no configuration required ( for autoTimestamp generation)
    public static int fileCacheRows;
    public static int csvThreadNum;

    // local file section
    public static String localFile;
    public static String localDir;
    public static String jdbcUrl;

    // hdfs section
    public static String hdfsUrl;
    public static int hdfsIoStreamBuffSize;
    public static String hdfsFileEncoding;
    public static boolean hdfsDecompressData;
    public static String hdfsCompressedDataFormat;
    public static String hdfsFile;
    public static String hdfsDir;

    // timestamp section
    public static String timestampPattern;
    public static TDTimePrecision timestampPrecision;
    public static boolean isTimestampPatternBigInt; //not config
    public static long timestampMinValue;
    public static long timestampMaxValue;
    public static boolean retainDuplicate;
    public static boolean sortBeforeInsert;
    public static boolean autoTimestamp;
    public static boolean retryOnError;
    public static boolean discardOldData;
    public static String insertStr = "import";

    // log section
    public static String logDir;
    public static int debugFlag;
    public static long numOfLogLines;

    // csv section
    public static char split;
    public static int colSize;
    public static boolean ignoreFirstLine;
    public static boolean binaryContainQuotation;
    public static boolean splitContainQuotation;
    public static boolean splitContainBlank;
    public static boolean splitContainColon;
    public static boolean splitContainRightBracket;
    public static TDFileType fileType;

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
    public static int logdbRows;//
    public static int logdbKeep;
    public static int logdbDays;
    public static int logdbReplica;//
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
    public static String tableNameColumn;
    public static boolean tableNameUseFileName;
    private static ArrayList<Integer> tableNameColumnsRead = new ArrayList<Integer>();
    public static int tableNameColumns[];
    public static int tableFieldSize;
    public static int tableTagSize;
    public static String tablePrefix;
    public static int tableRadix;
    public static int tableTolerance;
    public static boolean tableNameMd5;
    public static int tableIgnoreFrontNum;

    // fields section
    private static ArrayList<TDField> fieldsRead = new ArrayList<TDField>();
    public static TDField fields[];

    // tags section
    private static ArrayList<TDField> tagsRead = new ArrayList<TDField>();
    public static TDField tags[];

    public static void init() {
        // program section
        batchSize = 3000;
        csvThreadNum = 5;
        fileCacheRows = 100000;

        // input section
        localFile = "";
        localDir = "";
        jdbcUrl = "";
        hdfsUrl = "";
        hdfsIoStreamBuffSize = 4096;
        hdfsFileEncoding = "UTF-8";
        hdfsDecompressData = true;
        hdfsCompressedDataFormat = "lzo";
        hdfsFile = "";
        hdfsDir = "";

        // timestamp section
        timestampPattern = "bigint";
        timestampPrecision = TDTimePrecision.TD_TIME_PRECISION_MILLI_SECOND;
        isTimestampPatternBigInt = true;
        sortBeforeInsert = true;
        retainDuplicate = false;
        timestampMinValue = 1262275200; //2010/01/01 0:0:0
        timestampMaxValue = 1735660800; //2025/01/01 0:0:0
        autoTimestamp = false;
        retryOnError = true;
        discardOldData = false;

        // log section
        logDir = ".";
        debugFlag = 199;
        numOfLogLines = 1000000;

        // file section
        split = ',';
        colSize = 0;
        ignoreFirstLine = false;
        binaryContainQuotation = false;
        splitContainQuotation = false;
        splitContainBlank = false;
        splitContainColon = false;
        splitContainRightBracket = false;
        fileType = TDFileType.TD_FILE_TYPE_CSV;

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
        logdbTablePrefix = "csv";

        // datadb section
        datadbName = "cdb";
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
        tableNameColumn = "";
        tableNameUseFileName = false;
        tableFieldSize = 0;
        tableTagSize = 0;
        tablePrefix = "t";
        tableRadix = 0;
        tableTolerance = 0;
        tableNameMd5 = false;
        tableIgnoreFrontNum = 0;
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
                if (option.equalsIgnoreCase("csvThreadNum")) {
                    csvThreadNum = Integer.valueOf(value);
                } else if (option.equalsIgnoreCase("fileCacheRows")) {
                    fileCacheRows = Integer.valueOf(value);
                }

                // input section
                else if (option.equalsIgnoreCase("localFile")) {
                    localFile = value;
                } else if (option.equalsIgnoreCase("localDir")) {
                    localDir = value;
                } else if (option.equalsIgnoreCase("jdbcUrl")) {
                    jdbcUrl = value;
                } else if (option.equalsIgnoreCase("hdfsUrl")) {
                    hdfsUrl = value;
                } else if (option.equalsIgnoreCase("hdfsIoStreamBuffSize")) {
                    hdfsIoStreamBuffSize = Integer.valueOf(value);
                } else if (option.equalsIgnoreCase("hdfsFileEncoding")) {
                    hdfsFileEncoding = value;
                } else if (option.equalsIgnoreCase("hdfsDecompressData")) {
                    hdfsDecompressData = value.equalsIgnoreCase("true");
                } else if (option.equalsIgnoreCase("hdfsCompressedDataFormat")) {
                    hdfsCompressedDataFormat = value;
                } else if (option.equalsIgnoreCase("hdfsFile")) {
                    hdfsFile = value;
                } else if (option.equalsIgnoreCase("hdfsDir")) {
                    hdfsDir = value;
                }

                // timestamp section
                else if (option.equalsIgnoreCase("timestampPattern")) {
                    timestampPattern = value;
                    if (words.length > 2) {
                        timestampPattern = timestampPattern + " " + words[2];
                    }
                    isTimestampPatternBigInt = value.equalsIgnoreCase("bigint");
                } else if (option.equalsIgnoreCase("timestampPrecision")) {
                    if (value.equalsIgnoreCase("second")) {
                        timestampPrecision = TDTimePrecision.TD_TIME_PRECISION_SECOND;
                    } else if (value.equalsIgnoreCase("millisecond")) {
                        timestampPrecision = TDTimePrecision.TD_TIME_PRECISION_MILLI_SECOND;
                    } else if (value.equalsIgnoreCase("microsecond")) {
                        timestampPrecision = TDTimePrecision.TD_TIME_PRECISION_MICRO_SECOND;
                    } else {
                        timestampPrecision = TDTimePrecision.TD_TIME_PRECISION_MILLI_SECOND;
                    }
                } else if (option.equalsIgnoreCase("sortBeforeInsert")) {
                    sortBeforeInsert = value.equalsIgnoreCase("true");
                } else if (option.equalsIgnoreCase("retainDuplicate")) {
                    retainDuplicate = value.equalsIgnoreCase("true");
                } else if (option.equalsIgnoreCase("timestampMinValue")) {
                    timestampMinValue = TDUtil.getTimeMsFromYYYYMMDD(value);
                } else if (option.equalsIgnoreCase("timestampMaxValue")) {
                    timestampMaxValue = TDUtil.getTimeMsFromYYYYMMDD(value);
                } else if (option.equalsIgnoreCase("autoTimestamp")) {
                    autoTimestamp = value.equalsIgnoreCase("true");
                } else if (option.equalsIgnoreCase("retryOnError")) {
                    retryOnError = value.equalsIgnoreCase("true");
                }else if (option.equalsIgnoreCase("discardOldData")) {
                    discardOldData = value.equalsIgnoreCase("true");
                }

                // log section
                else if (option.equalsIgnoreCase("logDir")) {
                    logDir = value;
                } else if (option.equalsIgnoreCase("numOfLogLines")) {
                    numOfLogLines = Long.valueOf(value);
                } else if (option.equalsIgnoreCase("debugFlag")) {
                    debugFlag = Integer.valueOf(value);
                }

                // csv section
                else if (option.equalsIgnoreCase("split")) {
                    split = value.charAt(0);
                } else if (option.equalsIgnoreCase("colSize")) {
                    colSize = Integer.valueOf(value);
                } else if (option.equalsIgnoreCase("ignoreFirstLine")) {
                    ignoreFirstLine = value.equalsIgnoreCase("true");
                } else if (option.equalsIgnoreCase("binaryContainQuotation")) {
                    binaryContainQuotation = value.equalsIgnoreCase("true");
                } else if (option.equalsIgnoreCase("splitContainQuotation")) {
                    splitContainQuotation = value.equalsIgnoreCase("true");
                } else if (option.equalsIgnoreCase("splitContainBlank")) {
                    splitContainBlank = value.equalsIgnoreCase("true");
                } else if (option.equalsIgnoreCase("splitContainColon")) {
                    splitContainColon = value.equalsIgnoreCase("true");
                } else if (option.equalsIgnoreCase("splitContainRightBracket")) {
                    splitContainRightBracket = (value.equalsIgnoreCase("true"));
                } else if (option.equalsIgnoreCase("fileType")) {
                    if (value.equalsIgnoreCase("zjxl")) {
                        fileType = TDFileType.TD_FILE_TYPE_ZJXL;
                    }
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
                else if (option.equalsIgnoreCase("tableNameColumn")) {
                    tableNameColumn = value;
                } else if (option.equalsIgnoreCase("tableFieldSize")) {
                    tableFieldSize = Integer.valueOf(value);
                } else if (option.equalsIgnoreCase("tableTagSize")) {
                    tableTagSize = Integer.valueOf(value);
                } else if (option.equalsIgnoreCase("tablePrefix")) {
                    tablePrefix = value;
                } else if (option.equalsIgnoreCase("tableRadix")) {
                    tableRadix = Integer.valueOf(value);
                } else if (option.equalsIgnoreCase("tableNameMd5")) {
                    tableNameMd5 = value.equalsIgnoreCase("true");
                } else if (option.equalsIgnoreCase("tableIgnoreFrontNum")) {
                    tableIgnoreFrontNum = Integer.valueOf(value);
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

        if (csvThreadNum < 1 || csvThreadNum > 30) {
            TDLog.error(String.format("csvThreadNum range [1, 30], input:%d, use default 5", csvThreadNum));
            csvThreadNum = 5;
        }

        if (fileCacheRows < 5 || fileCacheRows > 5000000) {
            TDLog.error(String.format("fileCacheRows range [10000, 5000000], input:%d, use default 5", fileCacheRows));
            fileCacheRows = 5;
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

        // file section
        if (colSize < 2 || colSize > 10000) {
            TDLog.error(String.format("colSize range [2, 10000], input:%d", colSize));
            parsedOk = false;
        }

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
        } else if (tableNameColumn.equalsIgnoreCase("fileName")) {
            tableNameUseFileName = true;
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

        if (tableFieldSize < 2 || tableFieldSize > 250) {
            TDLog.error(String.format("tableFieldSize range [2, 250], input:%d", tableFieldSize));
            parsedOk = false;
        }
        if (tableTagSize < 1 || tableTagSize > 6) {
            TDLog.error(String.format("tableTagSize range [1, 6], input:%d", tableTagSize));
            parsedOk = false;
        }
        if (tablePrefix.length() < 0 || tablePrefix.length() > 4) {
            TDLog.error(String.format("tablePrefix size range [0, 4], input:%d", tablePrefix.length()));
            parsedOk = false;
        }
        if (tableRadix < 0 || tableRadix > 6) {
            TDLog.error(String.format("tableRadix range [0, 6], input:%d", tableRadix));
            parsedOk = false;
        }
        if (tableIgnoreFrontNum < 0 || tableIgnoreFrontNum > 16) {
            TDLog.error(String.format("tableIgnoreFrontNum range [0, 16], input:%d", tableIgnoreFrontNum));
            parsedOk = false;
        }

        if (!isTimestampPatternBigInt) {
            timestampPrecision = TDTimePrecision.TD_TIME_PRECISION_MILLI_SECOND;
        }

        int tolerance = 0;
        if (datadbMicroSecond)
            tolerance = 6;
        else
            tolerance = 3;

        if (tableRadix > tolerance) {
            TDLog.error(String.format("tableRadix:%d too long, no enough timestamp for tolerance:%d, please check the option of timestampPrecision and datadbMicroSecond", tableRadix, tolerance));
            parsedOk = false;
        }

        if (retainDuplicate && tolerance - tableRadix <= 0) {
            TDLog.error(String.format("retainDuplicate cannot enabled, no enough timestamp tolerance:%d, please check the option of timestampPrecision and datadbMicroSecond", tolerance));
            parsedOk = false;
        }

        if (!sortBeforeInsert && retainDuplicate) {
            TDLog.error(String.format("retainDuplicate cannot enabled, for sortBeforeInsert is not enabled"));
            //parsedOk = false;
        }

        tableTolerance = (int) Math.pow(10, tolerance - tableRadix);

        // fields sectiorn
        if ((int) fieldsRead.size() != tableFieldSize) {
            TDLog.error(String.format("the size:%d of fields not equal with tableFieldSize:%d, quit", fieldsRead.size(), tableFieldSize));
            parsedOk = false;
        }
        for (int f = 0; f < (int) fieldsRead.size(); ++f) {
            if (fieldsRead.get(f).isUseTableName)
                continue;
            if (fieldsRead.get(f).isUseFileName)
                continue;

            String[] words = fieldsRead.get(f).column.split("\\$+");
            for (int i = 1; i < (int) words.length; ++i) {
                int pos = Integer.valueOf(words[i]);
                fieldsRead.get(f).columnsRead.add(pos);
                if (pos < 0 || pos >= colSize) {
                    TDLog.error(String.format("fields:%d pos:%d not in range [0, %d]", f, i, colSize));
                    parsedOk = false;
                }
            }
            if (fieldsRead.get(f).columnsRead.size() < 1 || fieldsRead.get(f).columnsRead.size() > 100) {
                TDLog.error(String.format("field:%d size range [1, 100], input:%d", fieldsRead.get(f).columnsRead.size()));
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
            if (tagsRead.get(f).isUseTableName)
                continue;
            if (tagsRead.get(f).isUseFileName)
                continue;

            String[] words = tagsRead.get(f).column.split("\\$");
            for (int i = 1; i < (int) words.length; ++i) {
                int pos = Integer.valueOf(words[i]);
                tagsRead.get(f).columnsRead.add(pos);
                if (pos < 0 || pos >= colSize) {
                    TDLog.error(String.format("tag:%d pos:%d not in range [0, %d]", f, i, colSize));
                    parsedOk = false;
                }
            }
            if (tagsRead.get(f).columnsRead.size() < 1 || tagsRead.get(f).columnsRead.size() > 100) {
                TDLog.error(String.format("tag:%d size range [1, 100], input:%d", tagsRead.get(f).columnsRead.size()));
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
            timestampMaxValue *= 1000;
            timestampMinValue *= 1000;
        }

        if (timestampMaxValue < 0 || timestampMinValue < 0) {
            TDLog.error(String.format("invalid format of timestampMaxValue or timestampMinValue"));
            parsedOk = false;
        }

        if (autoTimestamp) {
            if (!datadbMicroSecond) {
                TDLog.error(String.format("datadb must support us, while autoTimestamp is enabled"));
                parsedOk = false;
            }
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

        maxColSize = colSize + 5;

        if (discardOldData) {
            insertStr = "insert";
        }

        if (!parsedOk) {
            TDLog.error(String.format("failed to parse config file:%s", configFileName));
        } else {
            print();
        }

        return parsedOk;
    }

    public static void print() {
        String timestampPrecisionString;
        if (timestampPrecision == TDTimePrecision.TD_TIME_PRECISION_SECOND) {
            timestampPrecisionString = "second";
        } else if (timestampPrecision == TDTimePrecision.TD_TIME_PRECISION_MILLI_SECOND) {
            timestampPrecisionString = "millisecond";
        } else {
            timestampPrecisionString = "microsecond";
        }

        // program section
        TDLog.print(String.format("=================================="));
        TDLog.print(String.format("batchSize:               %d", batchSize));
        TDLog.print(String.format("csvThreadNum:            %d", csvThreadNum));
        TDLog.print(String.format("fileCacheRows:           %d", fileCacheRows));

        // input section
        TDLog.print(String.format("localFile:               %s", localFile));
        TDLog.print(String.format("localDir:                %s", localDir));
        TDLog.print(String.format("jdbcUrl:                 %s", jdbcUrl));
        TDLog.print(String.format("hdfsUrl:                 %s", hdfsUrl));
        TDLog.print(String.format("hdfsIoStreamBuffSize:    %d", hdfsIoStreamBuffSize));
        TDLog.print(String.format("hdfsFileEncoding:        %s", hdfsFileEncoding));
        TDLog.print(String.format("hdfsDecompressData:      %s", hdfsDecompressData ? "true" : "false"));
        TDLog.print(String.format("hdfsCompressedDataFormat:%s", hdfsCompressedDataFormat));
        TDLog.print(String.format("hdfsDir:                 %s", hdfsDir));
        TDLog.print(String.format("hdfsFile:                %s", hdfsFile));

        // timestamp section
        TDLog.print(String.format("retainDuplicate:         %s", retainDuplicate ? "true" : "false"));
        TDLog.print(String.format("timestampPattern:        %s", timestampPattern));
        TDLog.print(String.format("timestampPrecision:      %s", timestampPrecisionString));
        TDLog.print(String.format("timestampMinValue:       %d", timestampMinValue));
        TDLog.print(String.format("timestampMaxValue:       %d", timestampMaxValue));
        TDLog.print(String.format("sortBeforeInsert:        %s", sortBeforeInsert ? "true" : "false"));
        TDLog.print(String.format("autoTimestamp:           %s", autoTimestamp ? "true" : "false"));
        TDLog.print(String.format("retryOnError:            %s", retryOnError ? "true" : "false"));
        TDLog.print(String.format("discardOldData:          %s", discardOldData ? "true" : "false"));

        // logfile section
        TDLog.print(String.format("logDir:                  %s", logDir));
        TDLog.print(String.format("debugFlag:               %d", debugFlag));
        TDLog.print(String.format("numOfLogLines:           %d", numOfLogLines));

        // file section
        TDLog.print(String.format("split:                   %c", split));
        TDLog.print(String.format("colSize:                 %d", colSize));
        TDLog.print(String.format("ignoreFirstLine:         %s", ignoreFirstLine ? "true" : "false"));
        TDLog.print(String.format("binaryContainQuotation:  %s", binaryContainQuotation ? "true" : "false"));
        TDLog.print(String.format("splitContainQuotation:   %s", splitContainQuotation ? "true" : "false"));
        TDLog.print(String.format("splitContainBlank:       %s", splitContainBlank ? "true" : "false"));
        TDLog.print(String.format("splitContainColon:       %s", splitContainColon ? "true" : "false"));
        TDLog.print(String.format("splitContainRightBracket:%s", splitContainRightBracket ? "true" : "false"));
        if (fileType == TDFileType.TD_FILE_TYPE_ZJXL) {
            TDLog.print(String.format("fileType:                zjxl"));
        } else {
            TDLog.print(String.format("fileType:                csv"));
        }

        // tdengine section
        TDLog.print(String.format("user:                    %s", user));
        TDLog.print(String.format("password:                %s", password));
        TDLog.print(String.format("host:                    %s", host));

        // logdb section
        TDLog.print(String.format("logdbRecord:             %s", logdbRecord ? "true" : "false"));
        TDLog.print(String.format("logdbName:               %s", logdbName));
        TDLog.print(String.format("logdbCache:              %d", logdbCache));
        TDLog.print(String.format("logdbAblocks:            %f", logdbAblocks));
        TDLog.print(String.format("logdbTblocks:            %d", logdbTblocks));
        TDLog.print(String.format("logdbTables:             %d", logdbTables));
        TDLog.print(String.format("logdbRows:               %d", logdbRows));
        TDLog.print(String.format("logdbKeep:               %d", logdbKeep));
        TDLog.print(String.format("logdbDays:               %d", logdbDays));
        TDLog.print(String.format("logdbReplica:            %d", logdbReplica));
        TDLog.print(String.format("logdbTablePrefix:        %s", logdbTablePrefix));

        // datadb section
        TDLog.print(String.format("datadbName:              %s", datadbName));
        TDLog.print(String.format("datadbCache:             %d", datadbCache));
        TDLog.print(String.format("datadbAblocks:           %f", datadbAblocks));
        TDLog.print(String.format("datadbTblocks:           %d", datadbTblocks));
        TDLog.print(String.format("datadbTables:            %d", datadbTables));
        TDLog.print(String.format("datadbRows:              %d", datadbRows));
        TDLog.print(String.format("datadbKeep:              %d", datadbKeep));
        TDLog.print(String.format("datadbDays:              %d", datadbDays));
        TDLog.print(String.format("datadbReplica:           %d", datadbReplica));
        TDLog.print(String.format("datadbMicroSecond:       %s", datadbMicroSecond ? "true" : "false"));

        // stable section
        TDLog.print(String.format("stableName:              %s", stableName));

        // table section
        TDLog.print(String.format("tableNameColumn:         %s", tableNameColumn));
        TDLog.print(String.format("tableFieldSize:          %d", tableFieldSize));
        TDLog.print(String.format("tableTagSize:            %d", tableTagSize));
        TDLog.print(String.format("tablePrefix:             %s", tablePrefix));
        TDLog.print(String.format("tableRadix:              %d", tableRadix));
        TDLog.print(String.format("tableNameMd5:            %s", tableNameMd5 ? "true" : "false"));
        TDLog.print(String.format("tableIgnoreFrontNum:     %d", tableIgnoreFrontNum));

        for (int i = 0; i < (int) fieldsRead.size(); ++i) {
            TDLog.print(String.format("fieldColumn:             %s %s %s", fieldsRead.get(i).column, fieldsRead.get(i).name, fieldsRead.get(i).type));
        }

        for (int i = 0; i < (int) tagsRead.size(); ++i) {
            TDLog.print(String.format("tagColumn:               %s %s %s", tagsRead.get(i).column, tagsRead.get(i).name, tagsRead.get(i).type));
        }

        TDLog.print(String.format("version:                 %s", "1.0.0"));
        TDLog.print(String.format("=================================="));
    }
}