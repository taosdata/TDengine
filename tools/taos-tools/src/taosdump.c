/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the MIT license as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 */

#define _GNU_SOURCE

#include "pub.h"
#include "cus_name.h"  // include/util/
#include "dump.h"
#include "dumpUtil.h"

static char    **g_tsDumpInDebugFiles     = NULL;
static char      g_dumpInCharset[64] = {0};
static char      g_dumpInServerVer[64] = {0};
static int       g_dumpInDataMajorVer;
static int       g_dumpInDataMinorVer;
static char      g_dumpInEscapeChar[64] = {0};
static char      g_dumpInLooseMode[64] = {0};
static bool      g_dumpInLooseModeFlag = false;


#ifdef WINDOWS
static char      g_configDir[MAX_PATH_LEN] = "C:\\TDengine\\cfg";
#else
static char      g_configDir[MAX_PATH_LEN] = "/etc/"CUS_PROMPT;
#endif

static char    **g_tsDumpInAvroTagsTbs = NULL;
static char    **g_tsDumpInAvroNtbs = NULL;
static char    **g_tsDumpInAvroFiles = NULL;

char      g_escapeChar[2] = "`";
static char      g_client_info[MIDDLE_BUFF_LEN] = {0};
int       g_majorVersionOfClient = 0;

static int      g_maxFilesPerDir = 100000;
volatile int64_t g_countOfDataFile = 0;

// progress
int64_t   g_tableCount = 0;
int64_t   g_tableDone  = 0;
char      g_dbName[TSDB_DB_NAME_LEN]= "";
char      g_stbName[TSDB_TABLE_NAME_LEN] = "";


static int  convertStringToReadable(char *str, int size,
        char *buf, int bufsize);
static int  convertNCharToReadable(char *str, int size,
        char *buf, int bufsize);
static int dumpExtraInfo(void **taos_v, FILE *fp);

char *g_avro_codec[] = {
    "null",
    "deflate",
    "snappy",
    "lzma",
    "unknown",
    "invalid"
};


volatile int64_t g_uniqueID = 0;
int64_t g_totalDumpOutRows = 0;
static int64_t g_totalDumpInRecSuccess = 0;
static int64_t g_totalDumpInRecFailed = 0;
static int64_t g_totalDumpInStbSuccess = 0;
static int64_t g_totalDumpInStbFailed = 0;
static int64_t g_totalDumpInNtbSuccess = 0;
static int64_t g_totalDumpInNtbFailed = 0;

SDbInfo **g_dbInfos = NULL;
TableInfo *g_tablesList = NULL;

/* Program documentation. */
static char doc[] = "";
/* "Argp example #4 -- a program with somewhat more complicated\ */
/*         options\ */
/*         \vThis part of the documentation comes *after* the options;\ */
/*         note that the text is automatically filled, but it's possible\ */
/*         to force a line-break, e.g.\n<-- here."; */

/* A description of the arguments we accept. */
static char args_doc[] = "dbname [tbname ...]\n--databases db1,db2,... \n"
    "--all-databases\n-i inpath\n-o outpath";

/* Keys for options without short-options. */
#define OPT_ABORT 1 /* –abort */

const char *              argp_program_bug_address = CUS_EMAIL;

/* The options we understand. */
static struct argp_option options[] = {
    // connection option
    {"host", 'h', "HOST",    0,
        "Server host from which to dump data. Default is localhost.", 0},
    {"user", 'u', "USER",    0,
        "User name used to connect to server. Default is root.", 0},
    {"password", 'p', 0, 0,
        "User password to connect to server. Default is taosdata.", 0},
    {"port", 'P', "PORT",        0,  "Port to connect.", 0},
    // input/output file
    {"outpath", 'o', "OUTPATH",     0,  "Output file path.", 1},
    {"inpath", 'i', "INPATH",      0,  "Input file path.", 1},
    {"resultFile", 'r', "RESULTFILE",  0,
        "DumpOut/In Result file path and name.", 1},
    {"config-dir", 'c', "CONFIG_DIR",  0,
        "Configure directory. Default is /etc/"CUS_PROMPT".", 1},
    // dump unit options
    {"all-databases", 'A', 0, 0,  "Dump all databases.", 2},
    {"databases", 'D', "DATABASES", 0,
        "Dump listed databases. Use comma to separate databases names.", 2},
    {"escape-character",   'e', 0, 0,  "Use escaped character for database name.", 2},
    {"allow-sys",   'a', 0, 0,  "Allow to dump system database (2.0 only).", 2},
    // dump format options
    {"schemaonly", 's', 0, 0,  "Only dump table schemas.", 2},
    {"without-property", 'N', 0, 0,
        "Dump database without its properties.", 2},
    {"avro-codec", 'd', "snappy", 0,
        "Choose an avro codec among null, deflate, snappy, and lzma(Windows is not currently supported).", 4},
    {"start-time",    'S', "START_TIME",  0,
        "Start time to dump. Either epoch or ISO8601/RFC3339 format is "
            "acceptable. ISO8601 format example: 2017-10-01T00:00:00.000+0800 "
            "or 2017-10-0100:00:00:000+0800 or '2017-10-01 00:00:00.000+0800'.",
        8},
    {"end-time",      'E', "END_TIME",    0,
        "End time to dump. Either epoch or ISO8601/RFC3339 format is "
            "acceptable. ISO8601 format example: 2017-10-01T00:00:00.000+0800 "
            "or 2017-10-0100:00:00.000+0800 or '2017-10-01 00:00:00.000+0800'.",
        9},
    {"data-batch",  'B', "DATA_BATCH",  0,  "Number of data per query/insert "
        "statement when backup/restore. Default value is 16384. If you see "
            "'error actual dump .. batch ..' when backup or if you see "
            "'WAL size exceeds limit' error when restore, "
                 "please adjust the value to a "
            "smaller one and try. The workable value is related to the length "
            "of the row and type of table schema.", 10},
    {"thread-num",  'T', "THREAD_NUM",  0,
// DEFAULT_THREAD_NUM
        "Number of threads for dump in/out data. Default is 8.", 10},
    {"loose-mode",  'L', 0,  0,
        "Use loose mode if the table name and column name use letter and "
            "number only. Default is NOT.", 10},
    {"inspect",  'I', 0,  0,
        "inspect avro file content and print on screen.", 10},
    {"no-escape",  'n', 0,  0,  "No escape char '`'. Default is using it.", 10},
    {"restful",  'R', 0,  0,  "Use RESTful interface to connect server.", 11},
    {"cloud",  'C', "CLOUD_DSN",  0, OLD_DSN_DESC, 11},
    {"timeout", 't', "SECONDS", 0, "The timeout seconds for "
                 "websocket to interact."},
    {"debug",   'g', 0, 0,  "Print debug info.", 15},
    {"dot-replace", 'Q', 0, 0,  "Replace dot character with underline character in the table name.", 10},
    {"rename", 'W', "RENAME-LIST", 0, "Rename database name with new name during importing data. \
        RENAME-LIST: \"db1=newDB1|db2=newDB2\" means rename db1 to newDB1 and rename db2 to newDB2.", 10},
    {"retry-count", 'k', "VALUE", 0, "Set the number of retry attempts for connection or query failures.", 11},
    {"retry-sleep-ms", 'z', "VALUE", 0, "Sleep interval between retries, in milliseconds.", 11},
    {"dsn",  'X', "DSN",  0, DSN_DESC, 11},
    {DRIVER_OPT, 'Z', "DRIVER", 0, DRIVER_DESC},
    {"version", 'V', 0, 0, "Print program version."},
    {0}
};


static resultStatistics g_resultStatistics = {0};
FILE *g_fpOfResult = NULL;
static int g_numOfCores = 1;

#define DEFAULT_START_TIME    (-INT64_MAX + 1)  // start_time
#define DEFAULT_END_TIME    (INT64_MAX)         // end_time

#define DEFAULT_THREAD_NUM  8

struct arguments g_args = {
    // connection option
    NULL,
    "root",
    "taosdata",
    0,          // port
    // outpath and inpath
    "",
    "",
    "./dump_result.txt",  // resultFile
    // dump unit option
    false,      // all_databases
    false,      // databases
    NULL,       // databasesSeq
    // dump format option
    false,      // schemaonly
    true,       // with_property
    true,       // avro
    AVRO_CODEC_SNAPPY,    // avro_codec
    DEFAULT_START_TIME,   // start_time
    {0},        // humanStartTime
    DEFAULT_END_TIME,   // end_time
    {0},        // humanEndTime
    "ms",       // precision
    MAX_RECORDS_PER_REQ / 2,    // data_batch
    false,      // data_batch_input
    TSDB_DEFAULT_PKT_SIZE,   // max_sql_len
    false,      // allow_sys
    true,       // escape_char
    false,      // db_escape_char
    false,      // loose_mode
    false,      // inspect
    // other options
    DEFAULT_THREAD_NUM,   // thread_num
    0,          // abort
    NULL,       // arg_list
    0,          // arg_list_len
    false,      // isDumpIn
    false,      // debug_print
    false,      // verbose_print
    false,      // performance_print
    false,      // dotRepalce
        0,      // dumpDbCount

    CONN_MODE_INVALID, // connMode
    NULL,       // dsn
    false,      // port_inputted

    NULL,       // renameBuf
    NULL,       // renameHead
    3,          // retryCount
    1000        // retrySleepMs
};

static uint64_t getUniqueIDFromEpoch() {
    struct timeval tv;

    toolsGetTimeOfDay(&tv);

    uint64_t id =
        (uint64_t)(tv.tv_sec) * 1000 +
        (uint64_t)(tv.tv_usec) / 1000;

    atomic_add_fetch_64(&g_uniqueID, 1);
    id += g_uniqueID;

    debugPrint("%s() LN%d unique ID: %"PRIu64"\n",
            __func__, __LINE__, id);

    return id;
}

// --version -V
static void printVersion(FILE *file) {
    if (file == NULL) {
        printf("fail, printVersion file is null.\n");
        return ;
    }

    // version, macro define in src/CMakeLists.txt
    fprintf(file, "%s\n%sdump version: %s\n", TD_PRODUCT_NAME, CUS_PROMPT, TD_VER_NUMBER);
    fprintf(file, "git: %s\n", TAOSDUMP_COMMIT_ID);
    fprintf(file, "build: %s\n", BUILD_INFO);
}

static char *typeToStr(int type) {
    switch (type) {
        case TSDB_DATA_TYPE_BOOL:
            return "bool";
        case TSDB_DATA_TYPE_TINYINT:
            return "tinyint";
        case TSDB_DATA_TYPE_SMALLINT:
            return "smallint";
        case TSDB_DATA_TYPE_INT:
            return "int";
        case TSDB_DATA_TYPE_BIGINT:
            return "bigint";
        case TSDB_DATA_TYPE_FLOAT:
            return "float";
        case TSDB_DATA_TYPE_DOUBLE:
            return "double";
        case TSDB_DATA_TYPE_BINARY:
            return "binary";
        case TSDB_DATA_TYPE_TIMESTAMP:
            return "timestamp";
        case TSDB_DATA_TYPE_NCHAR:
            return "nchar";
        case TSDB_DATA_TYPE_UTINYINT:
            return "tinyint unsigned";
        case TSDB_DATA_TYPE_USMALLINT:
            return "smallint unsigned";
        case TSDB_DATA_TYPE_UINT:
            return "int unsigned";
        case TSDB_DATA_TYPE_UBIGINT:
            return "bigint unsigned";
        case TSDB_DATA_TYPE_JSON:
            return "JSON";
        case TSDB_DATA_TYPE_VARBINARY:
            return "varbinary";
        case TSDB_DATA_TYPE_GEOMETRY:
            return "geometry";
        default:
            break;
    }

    return "unknown";
}

int typeStrToType(const char *type_str) {
    if ((0 == strcasecmp(type_str, "bool"))
            || (0 == strcasecmp(type_str, "boolean"))) {
        return TSDB_DATA_TYPE_BOOL;
    } else if (0 == strcasecmp(type_str, "tinyint")) {
        return TSDB_DATA_TYPE_TINYINT;
    } else if (0 == strcasecmp(type_str, "smallint")) {
        return TSDB_DATA_TYPE_SMALLINT;
    } else if (0 == strcasecmp(type_str, "int")) {
        return TSDB_DATA_TYPE_INT;
    } else if ((0 == strcasecmp(type_str, "bigint"))
            || (0 == strcasecmp(type_str, "long"))) {
        return TSDB_DATA_TYPE_BIGINT;
    } else if (0 == strcasecmp(type_str, "float")) {
        return TSDB_DATA_TYPE_FLOAT;
    } else if (0 == strcasecmp(type_str, "double")) {
        return TSDB_DATA_TYPE_DOUBLE;
    } else if ((0 == strcasecmp(type_str, "binary"))
            || (0 == strcasecmp(type_str, "varchar"))
            || (0 == strcasecmp(type_str, "string"))) {
        return TSDB_DATA_TYPE_BINARY;
    } else if (0 == strcasecmp(type_str, "timestamp")) {
        return TSDB_DATA_TYPE_TIMESTAMP;
    } else if ((0 == strcasecmp(type_str, "nchar"))
            || (0 == strcasecmp(type_str, "bytes"))) {
        return TSDB_DATA_TYPE_NCHAR;
    } else if (0 == strcasecmp(type_str, "tinyint unsigned")) {
        return TSDB_DATA_TYPE_UTINYINT;
    } else if (0 == strcasecmp(type_str, "smallint unsigned")) {
        return TSDB_DATA_TYPE_USMALLINT;
    } else if (0 == strcasecmp(type_str, "int unsigned")) {
        return TSDB_DATA_TYPE_UINT;
    } else if (0 == strcasecmp(type_str, "bigint unsigned")) {
        return TSDB_DATA_TYPE_UBIGINT;
    } else if (0 == strcasecmp(type_str, "JSON")) {
        return TSDB_DATA_TYPE_JSON;
    } else if (0 == strcasecmp(type_str, "varbinary")) {
        return TSDB_DATA_TYPE_VARBINARY;
    } else if (0 == strcasecmp(type_str, "geometry")) {
        return TSDB_DATA_TYPE_GEOMETRY;
    } else {
        errorPrint("%s() LN%d Unknown type: %s\n",
                __func__, __LINE__, type_str);
    }

    return TSDB_DATA_TYPE_NULL;
}

int64_t getStartTime(int precision) {
    int64_t start_time;

    if (strlen(g_args.humanStartTime)) {
        if (TSDB_CODE_SUCCESS != toolsParseTime(
                    g_args.humanStartTime, &start_time,
                    strlen(g_args.humanStartTime),
                    precision, 0)) {
            errorPrint("Input %s, time format error!\n",
                    g_args.humanStartTime);
            return -1;
        }
    } else {
        start_time = g_args.start_time;
    }

    return start_time;
}

int64_t getEndTime(int precision) {
    int64_t end_time;

    if (strlen(g_args.humanEndTime)) {
        if (TSDB_CODE_SUCCESS != toolsParseTime(
                g_args.humanEndTime, &end_time, strlen(g_args.humanEndTime),
                precision, 0)) {
            errorPrint("Input %s, time format error!\n", g_args.humanEndTime);
            return -1;
        }
    } else {
        end_time = g_args.end_time;
    }

    return end_time;
}

SRenameDB* newNode(char* first, SRenameDB* prev) {
    SRenameDB* node = (SRenameDB*) malloc(sizeof(SRenameDB));
    memset(node, 0, sizeof(SRenameDB));
    node->old = first;
    // link to list
    if(prev) {
        prev->next = node;
    }

    return node;
}

void setRenameDbs(char* arg) {
    if (arg == NULL) return ;
    // malloc new
    int len = strlen(arg);
    if(len <= 2) {
        return ;
    }
    len += 1; // include \0

    // malloc
    char* p = malloc(len);
    int j = 0; // j is p pos
    for (int i = 0; i < len; i++) {
        if (arg[i] == ' ') {
            // do nothing
        } else if (arg[i] == '=' || arg[i] == '|') {
            // set zero
            p[j++] = 0;
        } else {
            // copy
            p[j++] = arg[i];
        }
    }

    // splite
    SRenameDB* node = newNode(p, NULL);
    g_args.renameHead = node;
    for (int k = 0; k < j; k++) {
        if(p[k] == 0 && k + 1 != j && k > 0) {
            // string end and not last end
            char* name = &p[k] + 1;
            if (node->new == NULL) {
                node->new = name;
            } else {
                node = newNode(name, node);
            }
        }
    }

    // end
    g_args.renameBuf = p;
}

// find newName
char* findNewName(char* oldName) {
    SRenameDB* node = g_args.renameHead;
    while(node) {
        if (strcmp(node->old, oldName) == 0) {
            return node->new;
        }
        node = (SRenameDB* )node->next;
    }
    return NULL;
}

bool replaceCopy(char *des, char *src) {
    size_t len = strlen(src);
    bool replace = false;
    for (size_t i = 0; i <= len; i++) {
        if (src[i] == '.') {
            des[i] = '_';
            replace = true;
        } else {
            des[i] = src[i];
        }
    }

    return replace;
}

// repalce old name with new
char * replaceNewName(char* cmd, int len) {
    // database name left char and right char
    int nLeftSql = len;
    char left = cmd[len];
    char right = '.';
    if(left == '`') {
        right = left;
        nLeftSql += 1;
    }

    // get old database name
    char oldName[TSDB_DB_NAME_LEN];
    char* s = &cmd[nLeftSql];
    char* e = strchr(s, right);
    char* e1 = strchr(s, ' ');
    if(e == NULL && e1 == NULL) {
        return NULL;
    } else if(e == NULL && e1) {
        e = e1;
    } else if(e && e1 ) {
        if (e > e1) {
            e = e1;
        }
    }

    int oldLen = e - s;
    if(oldLen + 1 > TSDB_DB_NAME_LEN) {
        return NULL;
    }
    memcpy(oldName, s, oldLen);
    oldName[oldLen] = 0;

    // macth new database
    char* newName = findNewName(oldName);
    if(newName == NULL){
        return NULL;
    }

    // malloc new buff put new sql with new name
    int newLen = strlen(cmd) + (strlen(newName) - oldLen) + 1;
    char* newCmd = (char *)malloc(newLen);
    memset(newCmd, 0, newLen);

    // copy left + newName + right from cmd
    memcpy(newCmd, cmd, nLeftSql); // left sql
    strcat(newCmd, newName); // newName
    strcat(newCmd, e); // right sql

    return newCmd;
}

// if have database name rename, return new sql with new database name
// retrn value need call free() to free memory
char * afterRenameSql(char *cmd) {
    // match pattern
    const char* CREATE_DB  = "CREATE DATABASE IF NOT EXISTS ";
    const char* CREATE_STB = "CREATE STABLE IF NOT EXISTS ";
    const char* CREATE_TB  = "CREATE TABLE IF NOT EXISTS ";

    const char* pres[] = {CREATE_DB, CREATE_STB, CREATE_TB};
    for (int i = 0; i < sizeof(pres)/sizeof(char*); i++ ) {
        int len = strlen(pres[i]);
        if (strncmp(cmd, pres[i], len) == 0) {
            // found
            return replaceNewName(cmd, len);
        }
    }
    return NULL;
}

/* Parse a single option. */
static error_t parse_opt(int key, char *arg, struct argp_state *state) {
    /* Get the input argument from argp_parse, which we
       know is a pointer to our arguments structure. */
    wordexp_t full_path;
    int avroCodec = AVRO_CODEC_START;

    switch (key) {
        // connection option
        case 'a':
            g_args.allow_sys = true;
            break;

        case 'h':
            g_args.host = arg;
            break;

        case 'u':
            g_args.user = arg;
            break;

        case 'p':
            break;

        case 'P':
            if (!toolsIsStringNumber(arg)) {
                errorPrintReqArg2(CUS_PROMPT"dump", "P");
                exit(EXIT_FAILURE);
            }

            uint64_t port = atoi((const char *)arg);
            if (port > 65535) {
                errorWrongValue(CUS_PROMPT"dump", "-P or --port", arg);
                exit(EXIT_FAILURE);
            }
            g_args.port = (uint16_t)port;
            g_args.port_inputted = true;
            break;

        case 'o':
            if (wordexp(arg, &full_path, 0) != 0) {
                errorPrint("Invalid path %s\n", arg);
                return -1;
            }

            if (full_path.we_wordv[0]) {
                snprintf(g_args.outpath, DUMP_DIR_LEN, "%s/",
                        full_path.we_wordv[0]);
                wordfree(&full_path);
            } else {
                errorPrintReqArg3(CUS_PROMPT"dump", "-o or --outpath");
                exit(EXIT_FAILURE);
            }
            break;

        case 'g':
            g_args.debug_print = true;
            break;

        case 'i':
            g_args.isDumpIn = true;
            if (wordexp(arg, &full_path, 0) != 0) {
                errorPrint("Invalid path %s\n", arg);
                return -1;
            }

            if (full_path.we_wordv[0]) {
                TOOLS_STRNCPY(g_args.inpath, full_path.we_wordv[0], DUMP_DIR_LEN);
                wordfree(&full_path);
            } else {
                errorPrintReqArg3(CUS_PROMPT"dump", "-i or --inpath");
                exit(EXIT_FAILURE);
            }
            break;

        case 'd':
            for (; avroCodec < AVRO_CODEC_INVALID; avroCodec++) {
                if (0 == strcmp(arg, g_avro_codec[avroCodec])) {
                    g_args.avro_codec = avroCodec;
                    break;
                }
            }

            if (AVRO_CODEC_UNKNOWN == avroCodec) {
                if (g_args.debug_print || g_args.verbose_print) {
                    g_args.avro = false;
                }
            } else if (AVRO_CODEC_INVALID == avroCodec) {
                errorPrint("%s",
                        "Invalid AVRO codec inputted. Exit program!\n");
                exit(1);
            }
            break;

        case 'r':
            g_args.resultFile = arg;
            break;

        case 'c':
            if (0 == strlen(arg)) {
                errorPrintReqArg3(CUS_PROMPT"dump", "-c or --config-dir");
                exit(EXIT_FAILURE);
            }
            if (wordexp(arg, &full_path, 0) != 0) {
                errorPrint("Invalid path %s\n", arg);
                exit(EXIT_FAILURE);
            }
            TOOLS_STRNCPY(g_configDir, full_path.we_wordv[0], MAX_PATH_LEN);
            wordfree(&full_path);
            break;

        case 'A':
            break;

        case 'D':
            g_args.databases = true;
            break;

        case 'N':
            g_args.with_property = false;
            break;

        case 'S':
            // parse time here.
            break;

        case 'e':
            g_args.db_escape_char = true;
            break;

        case 'E':
        case 's':
        case 'L':
        case 'I':
        case 'n':
            break;

        case 'B':
            g_args.data_batch_input = true;
            g_args.data_batch = atoi((const char *)arg);
            if (g_args.data_batch > MAX_RECORDS_PER_REQ/2) {
                g_args.data_batch = MAX_RECORDS_PER_REQ/2;
            }
            break;

        case 'T':
            if (!toolsIsStringNumber(arg)) {
                errorPrint("%s", "\n\t-T need a number following!\n");
                exit(EXIT_FAILURE);
            }
            g_args.thread_num = atoi((const char *)arg);
            break;
        case 'R':
            warnPrint("%s\n", "'-R' is not supported, ignore this options.");
            break;
        case 'C':
        case 'X':
            if (arg) {
                if (arg[0]!= 0) {
                    g_args.dsn = arg;
                }

            } else {
                errorPrint("%s", "\n\t-C need a valid cloud DSN following!\n");
                exit(EXIT_FAILURE);
            }
            break;

        case OPT_ABORT:
            g_args.abort = 1;
            break;

        case ARGP_KEY_ARG:
            if (strlen(state->argv[state->next - 1])) {
                g_args.arg_list     = &state->argv[state->next - 1];
                g_args.arg_list_len = state->argc - state->next + 1;
            }
            state->next             = state->argc;
            break;
        case 'W':
            setRenameDbs(arg);
            break;
        case 'k':
            g_args.retryCount = atoi((const char *)arg);
            printf(" set argument retry count=%d\n", g_args.retryCount);
            break;
        case 'z':
            g_args.retrySleepMs = atoi((const char *)arg);
            printf(" set argument retry interval sleep = %d ms\n", g_args.retrySleepMs);
            break;
        case 'Z':
            g_args.connMode = getConnMode(arg);
            break;

        default:
            return ARGP_ERR_UNKNOWN;
    }
    return 0;
}

/* Our argp parser. */
static error_t parse_opt(int key, char *arg, struct argp_state *state);
static struct argp argp = {options, parse_opt, args_doc, doc};


static void parse_args(
        int argc, char *argv[], SArguments *arguments) {
    for (int i = 1; i < argc; i++) {
        if ((strncmp(argv[i], "-p", 2) == 0)
              || (strncmp(argv[i], "--password", 10) == 0)) {
            if ((strlen(argv[i]) == 2)
                  || (strncmp(argv[i], "--password", 10) == 0)) {
                printf("Enter password: ");
                setConsoleEcho(false);
                if (scanf("%255s", arguments->password) > 1) {
                    errorPrint("%s() LN%d, password read error!\n",
                            __func__, __LINE__);
                }
                setConsoleEcho(true);
            } else {
                strcpy(arguments->password, (char *)(argv[i] + 2));
                strcpy(argv[i], "-p");
            }
        } else if (strcmp(argv[i], "-n") == 0) {
            g_args.escape_char = false;
            strcpy(argv[i], "");
        } else if ((strcmp(argv[i], "-s") == 0)
                || (0 == strcmp(argv[i], "--schemaonly"))) {
            g_args.schemaonly = true;
            strcpy(argv[i], "");
        } else if ((strcmp(argv[i], "-I") == 0)
                || (0 == strcmp(argv[i], "--inspect"))) {
            g_args.inspect = true;
            strcpy(argv[i], "");
        } else if ((strcmp(argv[i], "-L") == 0)
                || (0 == strcmp(argv[i], "--lose-mode"))) {
            g_args.loose_mode = true;
            strcpy(argv[i], "");
        // dot replace
        } else if ((strcmp(argv[i], "-Q") == 0)
                || (0 == strcmp(argv[i], "--dot-replace"))) {
            g_args.dotReplace = true;
            strcpy(argv[i], "");
        } else if (strcmp(argv[i], "-gg") == 0) {
            arguments->verbose_print = true;
            strcpy(argv[i], "");
        } else if (strcmp(argv[i], "-PP") == 0) {
            arguments->performance_print = true;
            strcpy(argv[i], "");
        } else if ((strcmp(argv[i], "-A") == 0)
                || (0 == strncmp(
                            argv[i], "--all-database",
                            strlen("--all-database")))) {
            g_args.all_databases = true;
        } else if ((strncmp(argv[i], "-D", strlen("-D")) == 0)
                || (0 == strncmp(
                        argv[i], "--databases",
                        strlen("--databases")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg(argv[0], "D");
                    exit(EXIT_FAILURE);
                }
                arguments->databasesSeq = argv[++i];
            } else if (0 == strncmp(argv[i], "--databases=",
                        strlen("--databases="))) {
                arguments->databasesSeq = (char *)(argv[i]
                        + strlen("--databases="));
            } else if (0 == strncmp(argv[i], "-D", strlen("-D"))) {
                arguments->databasesSeq = (char *)(argv[i] + strlen("-D"));
            } else if (strlen("--databases") == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg3(argv[0], "--databases");
                    exit(EXIT_FAILURE);
                }
                arguments->databasesSeq = argv[++i];
            }
            g_args.databases = true;
        } else if (0 == strncmp(argv[i], "--version", strlen("--version")) ||
            0 == strncmp(argv[i], "-V", strlen("-V"))) {
                printVersion(stdout);
                exit(EXIT_SUCCESS);
        } else {
            continue;
        }
    }
}

static void copyHumanTimeToArg(char *timeStr, bool isStartTime) {
    if (0 == strncmp(timeStr, "--start-time=", strlen("--start-time="))) {
        timeStr += strlen("--start-time=");
    } else if (0 == strncmp(timeStr, "--end-time=", strlen("--end-time="))) {
        timeStr += strlen("--end-time=");
    }
    if (isStartTime) {
        TOOLS_STRNCPY(g_args.humanStartTime, timeStr, HUMAN_TIME_LEN);
    } else {
        TOOLS_STRNCPY(g_args.humanEndTime, timeStr, HUMAN_TIME_LEN);
    }
}

static void copyTimestampToArg(char *timeStr, bool isStartTime) {
    if (isStartTime) {
        g_args.start_time = strtoll((const char *)timeStr, NULL, 10);
    } else {
        g_args.end_time = strtoll((const char *)timeStr, NULL, 10);
    }
}

static void parseTimestampConvert(char *input, bool isStartTime) {
    if (NULL == input) {
        errorPrint("%s", "input timestamp need a valid value!\n");
        exit(-1);
    }

    char *tmp = strdup(input);
    if (strchr(tmp, ':') && strchr(tmp, '-')) {
        copyHumanTimeToArg(tmp, isStartTime);
    } else {
        copyTimestampToArg(tmp, isStartTime);
    }
    free(tmp);
}

static void parse_timestamp(
        int argc, char *argv[], SArguments *arguments) {
    for (int i = 1; i < argc; i++) {
        char *tmp;

        if ((0 == strcmp(argv[i], "-S")
                 || (0 == strncmp(
                         argv[i], "--start-time=",
                         strlen("--start-time="))))) {
            if (0 == strcmp(argv[i], "-S")) {
                tmp = argv[i+1];
            } else {
                tmp = argv[i];
            }
            parseTimestampConvert(tmp, true);
        } else if ((0 == strcmp(argv[i], "-E")
                     || (0 == strncmp(
                             argv[i],
                             "--end-time=",
                             strlen("--end-time="))))) {
            if (0 == strcmp(argv[i], "-E")) {
                tmp = argv[i+1];
            } else {
                tmp = argv[i];
            }
            parseTimestampConvert(tmp, false);
        }
    }
}

static int getPrecisionByString(char *precision) {
    if (0 == strncasecmp(precision,
                "ms", 2)) {
        return TSDB_TIME_PRECISION_MILLI;
    } else if (0 == strncasecmp(precision,
                "us", 2)) {
        return TSDB_TIME_PRECISION_MICRO;
    } else if (0 == strncasecmp(precision,
                "ns", 2)) {
        return TSDB_TIME_PRECISION_NANO;
    } else {
        errorPrint("Invalid time precision: %s.\n",
                precision);
    }

    return -1;
}

static void freeDbInfos() {
    if (g_dbInfos == NULL) return;
    for (int i = 0; i < g_args.dumpDbCount; i++)
        tfree(g_dbInfos[i]);
    tfree(g_dbInfos);
}


static int cleanIfQueryFailed(const char *funcname, int lineno,
                              char *command, TAOS_RES *res) {
    errorPrint("%s() LN%d, failed to run command <%s>. "
               "code: 0x%08x, reason: %s\n",
        funcname, lineno, command, taos_errno(res), taos_errstr(res));
    taos_free_result(res);
    free(command);
    return -1;
}

// check table is normal table or super table
static int getTableRecordInfoImplNative(
        char *dbName,
        char *table, TableRecordInfo *pTableRecordInfo,
        bool tryStable) {
    TAOS *taos;
    if (NULL == (taos = taosConnect(dbName))) {
        return -1;
    }

    TAOS_ROW row = NULL;
    bool isSet = false;
    TAOS_RES *res     = NULL;

    memset(pTableRecordInfo, 0, sizeof(TableRecordInfo));

    char *command = calloc(1, TSDB_MAX_ALLOWED_SQL_LEN);
    if (NULL == command) {
        errorPrint("%s() LN%d, memory allocation failed\n", __func__, __LINE__);
        taos_close(taos);
        return -1;
    }

    snprintf(command, TSDB_MAX_ALLOWED_SQL_LEN,
            g_args.db_escape_char
            ? "USE `%s`"
            : "USE %s",
            dbName);
    int32_t code = -1;
    res = taosQuery(taos, command, &code);
    if (code != 0) {
        errorPrint("Invalid database %s, reason: %s\n",
                dbName, taos_errstr(res));
        taos_free_result(res);
        free(command);
        taos_close(taos);
        return 0;
    }
    taos_free_result(res);

    // check table is stb or child table
    if (tryStable) {
        snprintf(command, TSDB_MAX_ALLOWED_SQL_LEN,
                "SELECT STABLE_NAME FROM information_schema.ins_stables "
                "WHERE db_name='%s' AND stable_name='%s'",
                dbName, table);
    } else {
        snprintf(command, TSDB_MAX_ALLOWED_SQL_LEN,
                "SELECT TABLE_NAME,STABLE_NAME FROM "
                "information_schema.ins_tables "
                "WHERE db_name='%s' AND table_name='%s'",
                dbName, table);
    }


    res = taosQuery(taos, command, &code);
    if (code != 0) {
        cleanIfQueryFailed(__func__, __LINE__, command, res);
        taos_close(taos);
        return -1;
    }

    while ((row = taos_fetch_row(res)) != NULL) {
        int32_t* lengths = taos_fetch_lengths(res);

        if (tryStable) {
            pTableRecordInfo->isStb = true;
            TOOLS_STRNCPY(pTableRecordInfo->tableRecord.stable, table,
                    TSDB_TABLE_NAME_LEN);
            isSet = true;
        } else {
            pTableRecordInfo->isStb = false;
            TOOLS_STRNCPY(pTableRecordInfo->tableRecord.name,
                    (char *)row[TSDB_SHOW_TABLES_NAME_INDEX],
                    min(TSDB_TABLE_NAME_LEN,
                        lengths[TSDB_SHOW_TABLES_NAME_INDEX] + 1));

            // check child table or normal table
            if (row[1]) {
                if (strlen((char *)row[1]) > 0) {
                    // child table
                    pTableRecordInfo->belongStb = true;
                    strncpy(pTableRecordInfo->tableRecord.stable,
                            (char *)row[1],
                            min(TSDB_TABLE_NAME_LEN-1,
                                lengths[1]));
                } else {
                    pTableRecordInfo->belongStb = false;
                }
            } else {
                pTableRecordInfo->belongStb = false;
            }
            isSet = true;
        }

        if (isSet) {
            break;
        }
    }

    taos_free_result(res);
    taos_close(taos);
    free(command);

    if (isSet) {
        return 0;
    }

    return -1;
}

static int getTableRecordInfo(
        char *dbName,
        char *table, TableRecordInfo *pTableRecordInfo) {
    if (0 == getTableRecordInfoImplNative(
                dbName, table, pTableRecordInfo, false)) {
        return 0;
    } else if (0 == getTableRecordInfoImplNative(
                dbName, table, pTableRecordInfo, true)) {
        return 0;
    }

    errorPrint("Invalid table/stable %s\n", table);
    return -1;
}

bool isSystemDatabase(char *dbName) {
    if (g_majorVersionOfClient == 3) {
        if ((strcmp(dbName, "information_schema") == 0)
                || (strcmp(dbName, "performance_schema") == 0)) {
            return true;
        }
    } else if (g_majorVersionOfClient == 2) {
        if (strcmp(dbName, "log") == 0) {
            return true;
        }
    } else {
        return false;
    }

    return false;
}

int inDatabasesSeq(const char *dbName) {
    if (NULL == g_args.databasesSeq) {
        return -1;
    }

    if (strstr(g_args.databasesSeq, ",") == NULL) {
        if (0 == strcmp(g_args.databasesSeq, dbName)) {
            return 0;
        }
    } else {
        char *dupSeq = strdup(g_args.databasesSeq);
        char *running = dupSeq;
        char *name = strsep(&running, ",");
        while (name) {
            if (0 == strcmp(name, dbName)) {
                tfree(dupSeq);
                return 0;
            }

            name = strsep(&running, ",");
        }

        free(dupSeq);
    }

    return -1;
}

static int getDbCountNative(TAOS_RES *res) {
    int count = 0;
    TAOS_ROW row;

    while ((row = taos_fetch_row(res)) != NULL) {
        int32_t* lengths = taos_fetch_lengths(res);
        if (lengths[TSDB_SHOW_DB_NAME_INDEX] < 0) {
            errorPrint("%s() LN%d, fetch_row() get %d length!\n",
                    __func__, __LINE__, lengths[TSDB_SHOW_DB_NAME_INDEX]);
            continue;
        }

        char dbName[TSDB_DB_NAME_LEN] = {0};
        strncpy(dbName, (char*)row[TSDB_SHOW_DB_NAME_INDEX],
                lengths[TSDB_SHOW_DB_NAME_INDEX]);
        if (isSystemDatabase(dbName)) {
            if (!g_args.allow_sys) {
                continue;
            }
        } else if (g_args.databases) {  // input multi dbs
            if (inDatabasesSeq(dbName) != 0) {
                continue;
            }
        } else if (!g_args.all_databases) {  // only input one db
            if (strcmp(g_args.arg_list[0], dbName)) {
                continue;
            }
        }

        count++;
    }

    return count;
}

static int getDumpDbCount() {
    int count = 0;

    TAOS     *taos = NULL;
    TAOS_RES *res     = NULL;

    char *command = calloc(1, TSDB_MAX_ALLOWED_SQL_LEN);
    if (NULL == command) {
        errorPrint("%s() LN%d, memory allocation failed", __func__, __LINE__);
        return -1;
    }

    snprintf(command, TSDB_MAX_ALLOWED_SQL_LEN,
                 "SELECT name FROM information_schema.ins_databases");

    int32_t code = -1;

    if (NULL == (taos = taosConnect(NULL))) {
        free(command);
        return 0;
    }
    res = taosQuery(taos, command, &code);
    if (0 != code) {
        cleanIfQueryFailed(__func__, __LINE__, command, res);
        taos_close(taos);
        return 0;
    }

    count = getDbCountNative(res);
    taos_free_result(res);
    taos_close(taos);

    free(command);
    return count;
}

static int dumpCreateMTableClause(
        const char* dbName,
        const char *stable,
        TableDes *tableDes,
        int numColsAndTags,
        FILE *fp
        ) {
    int counter = 0;
    int count_temp = 0;

    char* tmpBuf = (char *)malloc(TSDB_DEFAULT_PKT_SIZE);
    if (tmpBuf == NULL) {
        errorPrint("%s() LN%d, failed to allocate %d memory\n",
               __func__, __LINE__, TSDB_DEFAULT_PKT_SIZE);
        return -1;
    }

    char *pstr = NULL;
    pstr = tmpBuf;

    // outName is output to file table name
    char * outName = tableDes->name;
    char tableName[TSDB_TABLE_NAME_LEN+1];
    if(g_args.dotReplace && replaceCopy(tableName, tableDes->name)) {
        outName = tableName;
    }

    pstr += snprintf(tmpBuf, TSDB_DEFAULT_PKT_SIZE,
            g_args.db_escape_char
            ? "CREATE TABLE IF NOT EXISTS `%s`.%s%s%s USING `%s`.%s%s%s TAGS("
            : "CREATE TABLE IF NOT EXISTS %s.%s%s%s USING %s.%s%s%s TAGS(",
            dbName, g_escapeChar, outName, g_escapeChar,
            dbName, g_escapeChar, stable, g_escapeChar);

    for (; counter < numColsAndTags; counter++) {
        if (tableDes->cols[counter].note[0] != '\0') break;
    }

    TOOLS_ASSERT(counter < numColsAndTags);
    count_temp = counter;

    for (; counter < numColsAndTags; counter++) {
        if (counter != count_temp) {
            if ((TSDB_DATA_TYPE_BINARY    == tableDes->cols[counter].type) ||
                (TSDB_DATA_TYPE_VARBINARY == tableDes->cols[counter].type) ||
                (TSDB_DATA_TYPE_GEOMETRY  == tableDes->cols[counter].type) ||
                (TSDB_DATA_TYPE_NCHAR     == tableDes->cols[counter].type)) {
                if (tableDes->cols[counter].var_value) {
                    pstr += sprintf(pstr, ",\'%s\'",
                            tableDes->cols[counter].var_value);
                } else {
                    pstr += sprintf(pstr, ",\'%s\'",
                                    tableDes->cols[counter].value);
                }
            } else {
                pstr += sprintf(pstr, ",%s", tableDes->cols[counter].value);
            }
        } else {
            if ((TSDB_DATA_TYPE_BINARY    == tableDes->cols[counter].type) ||
                (TSDB_DATA_TYPE_VARBINARY == tableDes->cols[counter].type) ||
                (TSDB_DATA_TYPE_GEOMETRY  == tableDes->cols[counter].type) ||
                (TSDB_DATA_TYPE_NCHAR     == tableDes->cols[counter].type)) {
                if (tableDes->cols[counter].var_value) {
                    pstr += sprintf(pstr, "\'%s\'",
                                    tableDes->cols[counter].var_value);
                } else {
                    pstr += sprintf(pstr, "\'%s\'",
                                    tableDes->cols[counter].value);
                }
            } else {
                pstr += sprintf(pstr, "%s", tableDes->cols[counter].value);
            }
            /* pstr += sprintf(pstr, "%s", tableDes->cols[counter].note); */
        }

        /* if (strcasecmp(tableDes->cols[counter].type, "binary") == 0 || strcasecmp(tableDes->cols[counter].type, "nchar")
         * == 0) { */
        /*     pstr += sprintf(pstr, "(%d)", tableDes->cols[counter].length); */
        /* } */
    }

    pstr += sprintf(pstr, ");");

    int ret = fprintf(fp, "%s\n", tmpBuf);
    free(tmpBuf);

    return ret;
}


static int64_t getTbCountOfStbNative(const char *dbName, const char *stbName) {
    TAOS *taos;
    if (NULL == (taos = taosConnect(dbName))) {
        return -1;
    }

    char *command = calloc(1, TSDB_MAX_ALLOWED_SQL_LEN);
    if (NULL == command) {
        errorPrint("%s() LN%d, memory allocation failed\n", __func__, __LINE__);
        return -1;
    }

    snprintf(command, TSDB_MAX_ALLOWED_SQL_LEN,
            g_args.db_escape_char
            ? "SELECT COUNT(*) FROM (SELECT DISTINCT(TBNAME) "
                "FROM `%s`.%s%s%s)"
            : "SELECT COUNT(*) FROM (SELECT DISTINCT(TBNAME) "
                "FROM %s.%s%s%s)",
            dbName, g_escapeChar, stbName, g_escapeChar);
    debugPrint("get stable child count %s", command);

    int64_t count = 0;
    int32_t code = -1;
    TAOS_RES *res = taosQuery(taos, command, &code);
    if (code != 0) {
        cleanIfQueryFailed(__func__, __LINE__, command, res);
        taos_close(taos);
        return -1;
    }

    TAOS_ROW row = NULL;

    if ((row = taos_fetch_row(res)) != NULL) {
        count = *(int64_t*)row[TSDB_SHOW_TABLES_NAME_INDEX];
    }

    infoPrint("Get super table (%s) child tables (%"PRId64") ok\n", stbName, count);

    taos_free_result(res);
    taos_close(taos);
    free(command);
    return count;
}

int processFieldsValueV3(
        int index,
        TableDes *tableDes,
        const void *value,
        int32_t len) {
    switch (tableDes->cols[index].type) {
        case TSDB_DATA_TYPE_BOOL:
            if (0 == strncmp(value, "true", len)) {
                strcpy(tableDes->cols[index].value, "1");
            } else {
                strcpy(tableDes->cols[index].value, "0");
            }
            break;

        case TSDB_DATA_TYPE_TINYINT:
        case TSDB_DATA_TYPE_SMALLINT:
        case TSDB_DATA_TYPE_INT:
        case TSDB_DATA_TYPE_BIGINT:
        case TSDB_DATA_TYPE_UTINYINT:
        case TSDB_DATA_TYPE_USMALLINT:
        case TSDB_DATA_TYPE_UINT:
        case TSDB_DATA_TYPE_UBIGINT:
        case TSDB_DATA_TYPE_TIMESTAMP:
            strncpy(tableDes->cols[index].value, (char*)value, len);
            break;

        case TSDB_DATA_TYPE_FLOAT:
        case TSDB_DATA_TYPE_DOUBLE:
            memset(tableDes->cols[index].value, 0,
                    sizeof(tableDes->cols[index].value));

            if (len < (COL_VALUEBUF_LEN -1)) {
                strncpy(tableDes->cols[index].value, (char*)value, len);
            } else {
                if (tableDes->cols[index].var_value) {
                    free(tableDes->cols[index].var_value);
                    tableDes->cols[index].var_value = NULL;
                }
                tableDes->cols[index].var_value =
                    calloc(1, len + 1);

                if (NULL == tableDes->cols[index].var_value) {
                    errorPrint("%s() LN%d, memory allocation failed!\n",
                            __func__, __LINE__);
                    return -1;
                }
                strncpy(tableDes->cols[index].var_value, (char*)value, len);
            }
            break;

        case TSDB_DATA_TYPE_BINARY:
            memset(tableDes->cols[index].value, 0,
                    sizeof(tableDes->cols[index].value));

            if (g_args.avro) {
                if (len < (COL_VALUEBUF_LEN - 1)) {
                    strncpy(tableDes->cols[index].value, (char *)value, len);
                } else {
                    if (tableDes->cols[index].var_value) {
                        free(tableDes->cols[index].var_value);
                        tableDes->cols[index].var_value = NULL;
                    }
                    tableDes->cols[index].var_value = calloc(1,
                            1 + len);

                    if (NULL == tableDes->cols[index].var_value) {
                        errorPrint("%s() LN%d, memory allocation failed!\n",
                                __func__, __LINE__);
                        return -1;
                    }
                    strncpy(tableDes->cols[index].var_value,
                            (char *)value, len);
                }
            } else {
                if (len < (COL_VALUEBUF_LEN - 2)) {
                    convertStringToReadable(
                            (char *)value,
                            len,
                            tableDes->cols[index].value,
                            len);
                } else {
                    if (tableDes->cols[index].var_value) {
                        free(tableDes->cols[index].var_value);
                        tableDes->cols[index].var_value = NULL;
                    }
                    tableDes->cols[index].var_value = calloc(1,
                            len * 2);

                    if (NULL == tableDes->cols[index].var_value) {
                        errorPrint("%s() LN%d, memory allocation failed!\n",
                                __func__, __LINE__);
                        return -1;
                    }
                    convertStringToReadable((char *)value,
                            len,
                            (char *)(tableDes->cols[index].var_value),
                            len);
                }
            }
            break;

        case TSDB_DATA_TYPE_NCHAR:
        case TSDB_DATA_TYPE_JSON:
        case TSDB_DATA_TYPE_VARBINARY:
        case TSDB_DATA_TYPE_GEOMETRY:
            {
                if (g_args.avro) {
                    if (len < (COL_VALUEBUF_LEN - 1)) {
                        strncpy(tableDes->cols[index].value,
                                (char *)value,
                                len);
                    } else {
                        tableDes->cols[index].var_value = calloc(1, len + 1);

                        if (NULL == tableDes->cols[index].var_value) {
                            errorPrint("%s() LN%d, memory allocation failed!\n",
                                    __func__, __LINE__);
                            return -1;
                        }
                        strncpy(tableDes->cols[index].var_value,
                                (char *)value, len);
                    }
                } else {
                    if (len < (COL_VALUEBUF_LEN-2)) {
                        // need reserve 2 bytes for ' '
                        char tbuf[COL_VALUEBUF_LEN-2];
                        convertNCharToReadable(
                                (char *)value,
                                len, tbuf, COL_VALUEBUF_LEN-2);
                        snprintf(tableDes->cols[index].value,
                                 COL_VALUEBUF_LEN, "%s", tbuf);
                    } else {
                        if (tableDes->cols[index].var_value) {
                            free(tableDes->cols[index].var_value);
                            tableDes->cols[index].var_value = NULL;
                        }
                        tableDes->cols[index].var_value = calloc(1, len * 5);
                        TOOLS_ASSERT(tableDes->cols[index].var_value);

                        if (NULL == tableDes->cols[index].var_value) {
                            errorPrint("%s() LN%d, memory allocation failed!\n",
                                    __func__, __LINE__);
                            return -1;
                        }
                        convertStringToReadable(
                                (char *)value,
                                len,
                                (char *)(tableDes->cols[index].var_value), len);
                    }
                }
            }
            break;

        default:
            errorPrint("%s() LN%d, unknown type: %d\n",
                    __func__, __LINE__, tableDes->cols[index].type);
            break;
    }

    return 0;
}

int processFieldsValueV2(
        int index,
        TableDes *tableDes,
        const void *value,
        int32_t len) {
    switch (tableDes->cols[index].type) {
        case TSDB_DATA_TYPE_BOOL:
            snprintf(tableDes->cols[index].value, COL_VALUEBUF_LEN,
                     "%d", ((((int32_t)(*((char *)value))) == 1)?1:0));
            break;
        case TSDB_DATA_TYPE_TINYINT:
            snprintf(tableDes->cols[index].value, COL_VALUEBUF_LEN,
                     "%d", *((int8_t *)value));
            break;
        case TSDB_DATA_TYPE_INT:
            snprintf(tableDes->cols[index].value, COL_VALUEBUF_LEN,
                     "%d", *((int32_t *)value));
            break;
        case TSDB_DATA_TYPE_BIGINT:
            snprintf(tableDes->cols[index].value, COL_VALUEBUF_LEN,
                     "%" PRId64, *((int64_t *)value));
            break;
        case TSDB_DATA_TYPE_UTINYINT:
            snprintf(tableDes->cols[index].value, COL_VALUEBUF_LEN,
                     "%u", *((uint8_t *)value));
            break;
        case TSDB_DATA_TYPE_SMALLINT:
            snprintf(tableDes->cols[index].value, COL_VALUEBUF_LEN,
                     "%d", *((int16_t *)value));
            break;
        case TSDB_DATA_TYPE_USMALLINT:
            snprintf(tableDes->cols[index].value, COL_VALUEBUF_LEN,
                     "%u", *((uint16_t *)value));
            break;
        case TSDB_DATA_TYPE_UINT:
            snprintf(tableDes->cols[index].value, COL_VALUEBUF_LEN,
                     "%u", *((uint32_t *)value));
            break;
        case TSDB_DATA_TYPE_UBIGINT:
            snprintf(tableDes->cols[index].value, COL_VALUEBUF_LEN,
                     "%" PRIu64, *((uint64_t *)value));
            break;
        case TSDB_DATA_TYPE_FLOAT:
            {
                char tmpFloat[LARGE_BUFF_LEN] = {0};
                snprintf(tmpFloat, LARGE_BUFF_LEN,
                         "%f", GET_FLOAT_VAL(value));
                verbosePrint("%s() LN%d, float value: %s\n",
                        __func__, __LINE__, tmpFloat);
                int bufLenOfFloat = strlen(tmpFloat);

                if (bufLenOfFloat < (COL_VALUEBUF_LEN -1)) {
                    snprintf(tableDes->cols[index].value, COL_VALUEBUF_LEN,
                             "%f", GET_FLOAT_VAL(value));
                } else {
                    if (tableDes->cols[index].var_value) {
                        free(tableDes->cols[index].var_value);
                        tableDes->cols[index].var_value = NULL;
                    }
                    tableDes->cols[index].var_value =
                        calloc(1, bufLenOfFloat + 1);

                    if (NULL == tableDes->cols[index].var_value) {
                        errorPrint("%s() LN%d, memory allocation failed!\n",
                                __func__, __LINE__);
                        return -1;
                    }
                    snprintf(tableDes->cols[index].var_value, bufLenOfFloat + 1,
                             "%f", GET_FLOAT_VAL(value));
                }
            }
            break;
        case TSDB_DATA_TYPE_DOUBLE:
            {
                char tmpDouble[LARGE_BUFF_LEN] = {0};
                snprintf(tmpDouble, LARGE_BUFF_LEN,
                         "%f", GET_DOUBLE_VAL(value));
                verbosePrint("%s() LN%d, double value: %s\n",
                        __func__, __LINE__, tmpDouble);
                int bufLenOfDouble = strlen(tmpDouble);

                if (bufLenOfDouble < (COL_VALUEBUF_LEN -1)) {
                    snprintf(tableDes->cols[index].value, COL_VALUEBUF_LEN,
                             "%f", GET_DOUBLE_VAL(value));
                } else {
                    if (tableDes->cols[index].var_value) {
                        free(tableDes->cols[index].var_value);
                        tableDes->cols[index].var_value = NULL;
                    }
                    tableDes->cols[index].var_value =
                        calloc(1, bufLenOfDouble + 1);

                    if (NULL == tableDes->cols[index].var_value) {
                        errorPrint("%s() LN%d, memory allocation failed!\n",
                                __func__, __LINE__);
                        return -1;
                    }
                    snprintf(tableDes->cols[index].var_value,
                             bufLenOfDouble + 1,
                             "%f", GET_DOUBLE_VAL(value));
                }
            }
            break;
        case TSDB_DATA_TYPE_BINARY:
            memset(tableDes->cols[index].value, 0,
                    sizeof(tableDes->cols[index].value));
            if (g_args.avro) {
                if (len < (COL_VALUEBUF_LEN - 1)) {
                    strncpy(tableDes->cols[index].value, (char *)value, len);
                } else {
                    if (tableDes->cols[index].var_value) {
                        free(tableDes->cols[index].var_value);
                        tableDes->cols[index].var_value = NULL;
                    }
                    tableDes->cols[index].var_value = calloc(1,
                            1 + len);

                    if (NULL == tableDes->cols[index].var_value) {
                        errorPrint("%s() LN%d, memory allocation failed!\n",
                                __func__, __LINE__);
                        return -1;
                    }
                    strncpy(tableDes->cols[index].var_value,
                            (char *)value, len);
                }
            } else {
                if (len < (COL_VALUEBUF_LEN - 2)) {
                    convertStringToReadable(
                            (char *)value,
                            len,
                            tableDes->cols[index].value,
                            len);
                } else {
                    if (tableDes->cols[index].var_value) {
                        free(tableDes->cols[index].var_value);
                        tableDes->cols[index].var_value = NULL;
                    }
                    tableDes->cols[index].var_value = calloc(1,
                            len * 2);

                    if (NULL == tableDes->cols[index].var_value) {
                        errorPrint("%s() LN%d, memory allocation failed!\n",
                                __func__, __LINE__);
                        return -1;
                    }
                    convertStringToReadable((char *)value,
                            len,
                            (char *)(tableDes->cols[index].var_value),
                            len);
                }
            }
            break;

        case TSDB_DATA_TYPE_NCHAR:
        case TSDB_DATA_TYPE_JSON:
        case TSDB_DATA_TYPE_VARBINARY:
        case TSDB_DATA_TYPE_GEOMETRY:
            {
                if (g_args.avro) {
                    if (len < (COL_VALUEBUF_LEN - 1)) {
                        strncpy(tableDes->cols[index].value,
                                (char *)value,
                                len);
                    } else {
                        tableDes->cols[index].var_value = calloc(1, len + 1);

                        if (NULL == tableDes->cols[index].var_value) {
                            errorPrint("%s() LN%d, memory allocation failed!\n",
                                    __func__, __LINE__);
                            return -1;
                        }
                        strncpy(
                                (char *)(tableDes->cols[index].var_value),
                                (char *)value, len);
                    }
                } else {
                    if (len < (COL_VALUEBUF_LEN-2)) {
                        // need reserve 2 bytes for ' '
                        char tbuf[COL_VALUEBUF_LEN-2];
                        convertNCharToReadable(
                                (char *)value,
                                len, tbuf, COL_VALUEBUF_LEN-2);
                        snprintf(tableDes->cols[index].value,
                                 COL_VALUEBUF_LEN, "%s", tbuf);
                    } else {
                        if (tableDes->cols[index].var_value) {
                            free(tableDes->cols[index].var_value);
                            tableDes->cols[index].var_value = NULL;
                        }
                        tableDes->cols[index].var_value = calloc(1, len * 5);

                        if (NULL == tableDes->cols[index].var_value) {
                            errorPrint("%s() LN%d, memory allocation failed!\n",
                                    __func__, __LINE__);
                            return -1;
                        }
                        convertStringToReadable(
                                (char *)value,
                                len,
                                (char *)(tableDes->cols[index].var_value), len);
                    }
                }
            }
            break;
        case TSDB_DATA_TYPE_TIMESTAMP:
            snprintf(tableDes->cols[index].value, COL_VALUEBUF_LEN,
                     "%" PRId64, *(int64_t *)value);
            break;
        default:
            errorPrint("%s() LN%d, unknown type: %d\n",
                    __func__, __LINE__, tableDes->cols[index].type);
            break;
    }
    return 0;
}

void constructTableDesFromStb(const TableDes *stbTableDes,
        const char *table,
        TableDes **ppTableDes) {
    TableDes *tableDes = *ppTableDes;

    TOOLS_STRNCPY(tableDes->name, table, TSDB_TABLE_NAME_LEN);
    tableDes->columns = stbTableDes->columns;
    tableDes->tags = stbTableDes->tags;
    memcpy(tableDes->cols, stbTableDes->cols,
            (stbTableDes->columns + stbTableDes->tags) * sizeof(ColDes));
    for (int col = 0; col < (tableDes->columns + tableDes->tags); col++) {
        tableDes->cols[col].var_value = NULL;
    }
}


static int getTableTagValue(
        TAOS *taos,
        const char *dbName,
        const char *table,
        TableDes **ppTableDes) {
    TableDes *tableDes = *ppTableDes;

    char *command = calloc(1, TSDB_MAX_ALLOWED_SQL_LEN);
    if (NULL == command) {
        errorPrint("%s() LN%d, memory allocation failed\n", __func__, __LINE__);
        return -1;
    }

    snprintf(command, TSDB_MAX_ALLOWED_SQL_LEN,
            "SELECT tag_name,tag_value FROM information_schema.ins_tags "
            "WHERE db_name = '%s' AND table_name = '%s'",
            dbName, table);

    int32_t code = -1;
    TAOS_RES *res = taosQuery(taos, command, &code);
    if (code) {
        return cleanIfQueryFailed(__func__, __LINE__, command, res);
    }

    TAOS_ROW row;
    int index = tableDes->columns;
    while ((row = taos_fetch_row(res))) {
        int32_t* length = taos_fetch_lengths(res);

        if (NULL == row[1]) {
            strcpy(tableDes->cols[index].value, "NULL");
            strcpy(tableDes->cols[index].note , "NUL");
        } else if (0 != processFieldsValueV3(
                    index, tableDes, row[1], length[1])) {
            errorPrint("%s() LN%d, processFieldsValueV3 tag_value: %p\n",
                    __func__, __LINE__, row[1]);
            taos_free_result(res);
            free(command);
            return -1;
        }

        index++;
    }

    taos_free_result(res);
    free(command);

    return (tableDes->columns + tableDes->tags);
}

static inline int getTableDesFromStbNative(
        TAOS *taos,
        const char* dbName,
        const TableDes *stbTableDes,
        const char *table,
        TableDes **pptableDes) {
    constructTableDesFromStb(stbTableDes, table, pptableDes);
    return getTableTagValue(taos, dbName, table, pptableDes);
}

int getTableDes(TAOS *taos,
        const char* dbName,
        const char *table,
        TableDes *tableDes,
        const bool colOnly) {
    TAOS_ROW row = NULL;
    TAOS_RES* res = NULL;
    int colCount = 0;

    //
    // fill tags and columns
    //
    char *command = calloc(1, TSDB_MAX_ALLOWED_SQL_LEN);
    if (NULL == command) {
        errorPrint("%s() LN%d, memory allocation failed\n", __func__, __LINE__);
        return -1;
    }

    snprintf(command, TSDB_MAX_ALLOWED_SQL_LEN,
            g_args.db_escape_char
            ? "DESCRIBE `%s`.%s%s%s"
            : "DESCRIBE %s.%s%s%s",
            dbName, g_escapeChar, table, g_escapeChar);

    int32_t code = -1;
    res = taosQuery(taos, command, &code);
    if (code != 0) {
        return cleanIfQueryFailed(__func__, __LINE__, command, res);
    } else {
        debugPrint("%s() LN%d, run command <%s> success, taos: %p\n",
                __func__, __LINE__, command, taos);
    }

    TOOLS_STRNCPY(tableDes->name, table, TSDB_TABLE_NAME_LEN);
    uint32_t columns = 0, tags = 0;
    while ((row = taos_fetch_row(res)) != NULL) {
        int32_t* lengths = taos_fetch_lengths(res);
        char type[20] = {0};
        TOOLS_STRNCPY(tableDes->cols[colCount].field,
                (char *)row[TSDB_DESCRIBE_METRIC_FIELD_INDEX],
                lengths[TSDB_DESCRIBE_METRIC_FIELD_INDEX]+1);
        TOOLS_STRNCPY(type, (char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                lengths[TSDB_DESCRIBE_METRIC_TYPE_INDEX]+1);
        tableDes->cols[colCount].type = typeStrToType(type);
        tableDes->cols[colCount].length =
            *((int *)row[TSDB_DESCRIBE_METRIC_LENGTH_INDEX]);

        if (lengths[TSDB_DESCRIBE_METRIC_NOTE_INDEX] > 0) {
            char note[COL_NOTE_LEN] = {0};
            strncpy(note, (char *)row[TSDB_DESCRIBE_METRIC_NOTE_INDEX],
                    min(
                        lengths[TSDB_DESCRIBE_METRIC_NOTE_INDEX],
                        COL_NOTE_LEN-1));
            TOOLS_STRNCPY(tableDes->cols[colCount].note,
                    note, lengths[TSDB_DESCRIBE_METRIC_NOTE_INDEX]+1);
        }

        if (strcmp(tableDes->cols[colCount].note, "TAG") != 0) {
            columns++;
        } else {
            tags++;
        }
        colCount++;
    }

    tableDes->columns = columns;
    tableDes->tags = tags;

    taos_free_result(res);
    free(command);

    if (colOnly) {
        return colCount;
    }

    //
    // fill tag values
    //
    return getTableTagValue(taos, dbName, table, &tableDes);
}

// query from server
char *queryCreateTableSql(void** taos_v, const char *dbName, char *tbName) {
    // combine sql
    char sql[TSDB_DB_NAME_LEN + TSDB_TABLE_NAME_LEN + 128] = "";
    snprintf(sql, sizeof(sql), "show create table `%s`.`%s`", dbName, tbName);
    debugPrint("show sql:%s\n", sql);

    // query
    void* res = openQuery(taos_v, sql);
    if (res == NULL) {
        return NULL;
    }

    // read
    uint32_t len = 0;
    char* data = 0;

    int32_t ret = readRow(res, 0, 1, &len, &data);
    if (ret != 0) {
        closeQuery(res);
        return NULL;
    }

    // prefix check
    const char* pre = "CREATE STABLE ";
    const char* pre1 = "CREATE TABLE ";
    int32_t npre = strlen(pre);
    if (strncasecmp(data, pre, npre) != 0) {
        if (strncasecmp(data, pre1, strlen(pre1)) != 0) {
            char buf[64];
            memset(buf, 0, sizeof(buf));
            memcpy(buf, data, len > 63 ? 63 : len);
            errorPrint("Query create table sql prefix unexpect. pre=%s sql=%s\n", pre, buf);
            closeQuery(res);
            return NULL;
        } else {
            // foud create table
            npre = strlen(pre1);
        }
    }
    // table name check
    if (strncasecmp(data + npre + 1, tbName, strlen(tbName)) != 0) {
        char buf[64];
        memset(buf, 0, sizeof(buf));
        memcpy(buf, data, len > 63 ? 63 : len);
        errorPrint("Query create table sql tbName unexpect. tbName=%s sql=%s\n", tbName, buf);
        closeQuery(res);
        return NULL;
    }

    // tb is output to file table name
    char *tb = tbName;
    char tableName[TSDB_TABLE_NAME_LEN+1];
    if(g_args.dotReplace && replaceCopy(tableName, tbName)) {
        tb = tableName;
    }

    // create sql -> csql
    int32_t clen = len + TSDB_DB_NAME_LEN + 128;
    char *csql = (char *)calloc(1, clen);
    int32_t nskip = npre + 1 + strlen(tbName) + 1;
    int32_t pos = snprintf(csql, clen, "CREATE TABLE IF NOT EXISTS `%s`.`%s`", dbName, tb);
    memcpy(csql + pos, data + nskip, len - nskip);
    debugPrint("export create table sql:%s\n", csql);

    // close
    closeQuery(res);

    // return
    return csql;
}


void freeRecordSchema(RecordSchema *recordSchema) {
    if (recordSchema) {
        if (recordSchema->fields) {
            free(recordSchema->fields);
        }

        if (recordSchema->tableDes) {
            freeTbDes(recordSchema->tableDes, true);
        }

        free(recordSchema);
    }
}

//
//   -------------  read schema json -------------
//

// read fields
static int32_t readJsonFields( json_t *value, RecordSchema *recordSchema) {
    if (JSON_ARRAY == json_typeof(value)) {
        size_t i;
        size_t size = json_array_size(value);

        verbosePrint("%s() LN%d, JSON Array of %zu element: %s\n",
                __func__, __LINE__,
                size, json_plural(size));

        recordSchema->num_fields = size;
        recordSchema->fields = calloc(1, sizeof(FieldStruct) * size);
        ASSERT(recordSchema->fields);

        for (i = 0; i < size; i++) {
            FieldStruct *field = (FieldStruct *)
                (recordSchema->fields + sizeof(FieldStruct) * i);
            json_t *arr_element = json_array_get(value, i);
            const char *ele_key;
            json_t *ele_value;

            json_object_foreach(arr_element, ele_key, ele_value) {
                if (0 == strcmp(ele_key, "name")) {
                    tstrncpy(field->name,
                            json_string_value(ele_value),
                            TSDB_COL_NAME_LEN-1);
                } else if (0 == strcmp(ele_key, "type")) {
                    int ele_type = json_typeof(ele_value);

                    if (JSON_STRING == ele_type) {
                        field->type =
                            typeStrToType(json_string_value(ele_value));
                    } else if (JSON_ARRAY == ele_type) {
                        size_t ele_size = json_array_size(ele_value);

                        for (size_t ele_i = 0; ele_i < ele_size;
                                ele_i++) {
                            json_t *arr_type_ele =
                                json_array_get(ele_value, ele_i);

                            if (JSON_STRING == json_typeof(arr_type_ele)) {
                                const char *arr_type_ele_str =
                                    json_string_value(arr_type_ele);

                                if (0 == strcmp(arr_type_ele_str,
                                            "null")) {
                                    field->nullable = true;
                                } else {
                                    field->type = typeStrToType(arr_type_ele_str);
                                }
                            } else if (JSON_OBJECT ==
                                    json_typeof(arr_type_ele)) {
                                const char *arr_type_ele_key;
                                json_t *arr_type_ele_value;

                                json_object_foreach(arr_type_ele,
                                        arr_type_ele_key,
                                        arr_type_ele_value) {
                                    if (JSON_STRING ==
                                            json_typeof(arr_type_ele_value)) {
                                        const char *arr_type_ele_value_str =
                                            json_string_value(arr_type_ele_value);
                                        if (0 == strcmp(arr_type_ele_value_str,
                                                    "null")) {
                                            field->nullable = true;
                                        } else {
                                            if (0 == strcmp(arr_type_ele_value_str,
                                                        "array")) {
                                                field->is_array = true;
                                            } else {
                                                field->type =
                                                    typeStrToType(arr_type_ele_value_str);
                                            }
                                        }
                                    } else if (JSON_OBJECT == json_typeof(arr_type_ele_value)) {
                                        const char *arr_type_ele_value_key;
                                        json_t *arr_type_ele_value_value;

                                        json_object_foreach(arr_type_ele_value,
                                                arr_type_ele_value_key,
                                                arr_type_ele_value_value) {
                                            if (JSON_STRING == json_typeof(arr_type_ele_value_value)) {
                                                const char *arr_type_ele_value_value_str =
                                                    json_string_value(arr_type_ele_value_value);
                                                field->array_type = typeStrToType(
                                                        arr_type_ele_value_value_str);
                                            }
                                        }
                                    }
                                }
                            } else {
                                errorPrint("%s", "Error: not supported!\n");
                            }
                        }
                    } else if (JSON_OBJECT == ele_type) {
                        const char *obj_key;
                        json_t *obj_value;

                        json_object_foreach(ele_value, obj_key, obj_value) {
                            if (0 == strcmp(obj_key, "type")) {
                                int obj_value_type = json_typeof(obj_value);
                                if (JSON_STRING == obj_value_type) {
                                    const char *obj_value_str = json_string_value(obj_value);
                                    if (0 == strcmp(obj_value_str, "array")) {
                                        field->type = TSDB_DATA_TYPE_NULL;
                                        field->is_array = true;
                                    } else {
                                        field->type =
                                            typeStrToType(obj_value_str);
                                    }
                                } else if (JSON_OBJECT == obj_value_type) {
                                    const char *field_key;
                                    json_t *field_value;

                                    json_object_foreach(obj_value, field_key, field_value) {
                                        if (JSON_STRING == json_typeof(field_value)) {
                                            const char *field_value_str =
                                                json_string_value(field_value);
                                            field->type =
                                                typeStrToType(field_value_str);
                                        } else {
                                            field->nullable = true;
                                        }
                                    }
                                }
                            } else if (0 == strcmp(obj_key, "items")) {
                                int obj_value_items = json_typeof(obj_value);
                                if (JSON_STRING == obj_value_items) {
                                    field->is_array = true;
                                    const char *obj_value_str =
                                        json_string_value(obj_value);
                                    field->array_type = typeStrToType(obj_value_str);
                                } else if (JSON_OBJECT == obj_value_items) {
                                    const char *item_key;
                                    json_t *item_value;

                                    json_object_foreach(obj_value, item_key, item_value) {
                                        if (JSON_STRING == json_typeof(item_value)) {
                                            const char *item_value_str =
                                                json_string_value(item_value);
                                            field->array_type =
                                                typeStrToType(item_value_str);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    } else {
        errorPrint("%s() LN%d, fields have no array\n",
                __func__, __LINE__);
        return -1;
    }

    return 0;
}

//
// read avro json
//
static RecordSchema *parse_json_to_recordschema(json_t *element) {
    RecordSchema *recordSchema = calloc(1, sizeof(RecordSchema));
    if (NULL == recordSchema) {
        errorPrint("%s() LN%d, memory allocation failed!\n",
                __func__, __LINE__);
        return NULL;
    }

    if (JSON_OBJECT != json_typeof(element)) {
        errorPrint("%s() LN%d, json passed is not an object\n",
                __func__, __LINE__);
        free(recordSchema);
        return NULL;
    }

    const char *key;
    json_t *value;

    json_object_foreach(element, key, value) {
        // name
        if (0 == strcmp(key, NAME_KEY)) {
            tstrncpy(recordSchema->name, json_string_value(value), RECORD_NAME_LEN - 1);
        // fields
        } else if (0 == strcmp(key, FIELDS_KEY)) {
            if (readJsonFields(value, recordSchema)) {
                free(recordSchema);
                return NULL;
            }
        }
    }

    return recordSchema;
}

avro_value_iface_t* prepareAvroWface(
        const char *avroFilename,
        char *jsonSchema,
        avro_schema_t *schema,
        RecordSchema **recordSchema,
        avro_file_writer_t *writer) {
    TOOLS_ASSERT(avroFilename);
    if (avro_schema_from_json_length(jsonSchema, strlen(jsonSchema), schema)) {
        errorPrint("%s() LN%d, Unable to parse:\n%s \nto schema\n"
                "error message: %s\n",
                __func__, __LINE__, jsonSchema, avro_strerror());
        exit(EXIT_FAILURE);
    }

    json_t *json_root = load_json(jsonSchema);
    verbosePrint("\n%s() LN%d\n === Schema parsed:\n", __func__, __LINE__);

    if (json_root) {
        if (g_args.verbose_print) {
            print_json(json_root);
        }

        *recordSchema = parse_json_to_recordschema(json_root);
        if (NULL == *recordSchema) {
            errorPrint("%s", "Failed to parse json to recordschema\n");
            exit(EXIT_FAILURE);
        }

        json_decref(json_root);
    } else {
        errorPrint("json:\n%s\n can't be parsed by jansson\n", jsonSchema);
        exit(EXIT_FAILURE);
    }

    int rval = avro_file_writer_create_with_codec
        (avroFilename, *schema, writer, g_avro_codec[g_args.avro_codec], 70*1024);
    if (rval) {
        errorPrint("There was an error creating %s. reason: %s\n",
                avroFilename, avro_strerror());
        exit(EXIT_FAILURE);
    }

    avro_value_iface_t* wface =
        avro_generic_class_from_schema(*schema);

    return wface;
}

static int dumpCreateTableClauseAvro(
        void **taos_v,
        const char *dumpFilename,
        TableDes *tableDes,
        int numOfCols,
        const char* dbName) {
    TOOLS_ASSERT(dumpFilename);
    // {
    // "type": "record",
    // "name": "_ntb",
    // "namespace": "dbname",
    // "fields": [
    //      {
    //      "name": "tbname",
    //      "type": ["null","string"]
    //      },
    //      {
    //      "name": "sql",
    //      "type": ["null","string"]
    //      }]
    //  }
    int jsonSchemaLen =
            17 + TSDB_DB_NAME_LEN               /* dbname section */
            + 17                                /* type: record */
            + 11 + TSDB_TABLE_NAME_LEN          /* stbname section */
            + 120;                              /* fields section */
    char *jsonSchema = (char *)calloc(1, jsonSchemaLen);
    if (NULL == jsonSchema) {
        errorPrint("%s() LN%d, memory allocation failed!\n",
                __func__, __LINE__);
        return -1;
    }

    snprintf(jsonSchema, jsonSchemaLen,
            "{\"type\":\"record\",\"name\":\"%s.%s\","
            "\"fields\":[{\"name\":\"tbname\","
            "\"type\":[\"null\",\"string\"]},"
            "{\"name\":\"sql\",\"type\":[\"null\",\"string\"]}]}",
            dbName, "_ntb");

    // get create sql
    char *sql = queryCreateTableSql(taos_v, dbName, tableDes->name);
    if(sql == NULL) {
        free(jsonSchema);
        return -1;
    }

    debugPrint("%s() LN%d, write string: %s\n", __func__, __LINE__, sql);

    avro_schema_t schema;
    RecordSchema *recordSchema;
    avro_file_writer_t db;

    avro_value_iface_t *wface = prepareAvroWface(
            dumpFilename,
            jsonSchema, &schema, &recordSchema, &db);

    avro_value_t record;
    avro_generic_value_new(wface, &record);

    avro_value_t value, branch;
    if (0 != avro_value_get_by_name(
                &record, "tbname", &value, NULL)) {
        errorPrint("%s() LN%d, avro_value_get_by_name(..%s..) failed",
                __func__, __LINE__, "sql");
    }

    avro_value_set_branch(&value, 1, &branch);
    if(g_args.dotReplace) {
        char tableName[TSDB_TABLE_NAME_LEN+1] = "";
        if(replaceCopy(tableName, tableDes->name)) {
            avro_value_set_string(&branch, tableName);
        } else {
            avro_value_set_string(&branch, tableDes->name);
        }
    } else {
        avro_value_set_string(&branch, tableDes->name);
    }

    if (0 != avro_value_get_by_name(
                &record, "sql", &value, NULL)) {
        errorPrint("%s() LN%d, avro_value_get_by_name(..%s..) failed",
                __func__, __LINE__, "sql");
    }

    avro_value_set_branch(&value, 1, &branch);
    avro_value_set_string(&branch, sql);

    if (0 != avro_file_writer_append_value(db, &record)) {
        errorPrint("%s() LN%d, Unable to write record to file. Message: %s\n",
                __func__, __LINE__,
                avro_strerror());
    }

    avro_value_decref(&record);
    avro_value_iface_decref(wface);
    freeRecordSchema(recordSchema);
    avro_file_writer_close(db);
    avro_schema_decref(schema);

    free(jsonSchema);
    free(sql);

    return 0;
}

static int dumpCreateTableClause(
        void ** taos_v,
        TableDes *tableDes,
        int numOfCols,
        FILE *fp,
        const char* dbName) {
    // get create sql
    char *sql = queryCreateTableSql(taos_v, dbName, tableDes->name);
    if(sql == NULL) {
        return -1;
    }

    // write to file
    debugPrint("%s() LN%d, export create table sql: %s\n", __func__, __LINE__, sql);
    int len = fprintf(fp, "%s\n", sql);
    if (len != strlen(sql) + 1) {
        errorPrint(" write create sql failed. expect=%ld real=%d\n sql=%s\n", strlen(sql), len, sql);
        free(sql);
        return -1;
    }
    free(sql);
    return 0;
}

static int dumpStableClasuse(
        void **taos_v,
        SDbInfo *dbInfo,
        const char *stbName,
        TableDes **pStbTableDes,
        FILE *fp) {

    TableDes *tableDes = *pStbTableDes;

    int32_t colCount = getTableDes(*taos_v, dbInfo->name,
                stbName, tableDes, true);

    if (colCount < 0) {
        errorPrint("%s() LN%d, failed to get stable[%s] schema\n",
               __func__, __LINE__, stbName);
        exit(-1);
    }

    return dumpCreateTableClause(taos_v, tableDes, colCount, fp, dbInfo->name);
}

static void dumpCreateDbClause(
        void **taos_v,
        SDbInfo *dbInfo, bool isDumpProperty, FILE *fp) {
    char sqlstr[TSDB_DEFAULT_PKT_SIZE] = {0};

    dumpExtraInfo(taos_v, fp);

    char *pstr = sqlstr;
    pstr += sprintf(pstr,
            g_args.db_escape_char
            ? "CREATE DATABASE IF NOT EXISTS `%s` "
            : "CREATE DATABASE IF NOT EXISTS %s ",
            dbInfo->name);
    if (isDumpProperty) {
        char strict[STRICT_LEN] = "";
        if (0 == strcmp(dbInfo->strict, "strict")) {
            sprintf(strict, "STRICT %d", 1);
        } else if (0 == strcmp(dbInfo->strict, "no_strict")) {
            sprintf(strict, "STRICT %d", 0);
        }

        char quorum[32] = "";
        if (0 != dbInfo->quorum) {
            sprintf(quorum, "QUORUM %d", dbInfo->quorum);
        }

        char days[32] = "";
        if (0 != dbInfo->days) {
            sprintf(days, "DAYS %d", dbInfo->days);
        }

        char duration[DURATION_LEN + 10] = "";
        if (strlen(dbInfo->duration)) {
            sprintf(duration, "DURATION %s", dbInfo->duration);
        }

        char cache[32] = "";
        if (0 != dbInfo->cache) {
            sprintf(cache, "CACHE %d", dbInfo->cache);
        }

        char blocks[32] = "";
        if (0 != dbInfo->blocks) {
            sprintf(blocks, "BLOCKS %d", dbInfo->blocks);
        }

        char fsync[32] = {0};
        if (0 != dbInfo->fsync) {
            sprintf(fsync, "FSYNC %d", dbInfo->fsync);
        }

        char cachelast[32] = {0};
        if (0 != dbInfo->cachelast) {
            sprintf(cachelast, "CACHELAST %d", dbInfo->cachelast);
        }

        char update[32] = "";
        if (0 != dbInfo->update) {
            sprintf(update, "UPDATE %d", dbInfo->update);
        }

        char single_stable_model[32] = {0};
        sprintf(cache, "SINGLE_STABLE_MODEL %d",
                dbInfo->single_stable_model?1:0);


        if (dbInfo->replica) {
            pstr += sprintf(pstr, "REPLICA %d ", dbInfo->replica);
        }

        char keep[64] = "";
        if (strlen(dbInfo->keeplist)) {
            sprintf(keep, "KEEP %s", dbInfo->keeplist);
        }

        pstr += sprintf(pstr,
                "%s %s %s %s %s %s "
                "%s %s PRECISION '%s' %s %s ",
                (g_majorVersionOfClient < 3)?"":strict,
                (g_majorVersionOfClient < 3)?quorum:"",
                (g_majorVersionOfClient < 3)?days:duration,
                keep,
                (g_majorVersionOfClient < 3)?cache:"",
                (g_majorVersionOfClient < 3)?blocks:"",
                (g_majorVersionOfClient < 3)?fsync:"",
                (g_majorVersionOfClient < 3)?cachelast:"",
                dbInfo->precision,
                single_stable_model,
                (g_majorVersionOfClient < 3)?update:"");
        if (dbInfo->minrows) {
            pstr += sprintf(pstr, "MINROWS %d ", dbInfo->minrows);
        }

        if (dbInfo->maxrows) {
            pstr += sprintf(pstr, "MAXROWS %d ", dbInfo->maxrows);
        }

        if (dbInfo->comp) {
            pstr += sprintf(pstr, "COMP %d ", dbInfo->comp);
        }
    }

    pstr += sprintf(pstr, ";");
    debugPrint("%s() LN%d db clause: %s\n", __func__, __LINE__, sqlstr);
    fprintf(fp, "%s\n\n", sqlstr);
}

static FILE* openDumpInFile(char *fptr) {
    wordexp_t full_path;

    if (wordexp(fptr, &full_path, 0) != 0) {
        errorPrint("illegal file name: %s\n", fptr);
        return NULL;
    }

    char *fname = full_path.we_wordv[0];

    FILE *f = NULL;
    if ((fname) && (strlen(fname) > 0)) {
        f = fopen(fname, "r");
        if (f == NULL) {
            errorPrint("Failed to open file %s. Errno is %d. Reason is %s.\n",
                    fname, errno, strerror(errno));
        }
    }

    wordfree(&full_path);
    return f;
}

static uint64_t getFilesNum(const char *dbPath, const char *ext) {
    uint64_t count = 0;

    int namelen, extlen;
    TdDirEntryPtr pDirent;
    TdDirPtr pDir;

    extlen = strlen(ext);

    bool isSql = (0 == strcmp(ext, "sql"));

    pDir = toolsOpenDir(dbPath);
    if (pDir != NULL) {
        while ((pDirent = toolsReadDir(pDir)) != NULL) {
            char *entryName = toolsGetDirEntryName(pDirent);
            namelen = strlen(entryName);

            if (namelen > extlen) {
                if (strcmp(ext, &(entryName[namelen - extlen])) == 0) {
                    if (isSql) {
                        if (0 == strcmp(entryName, "dbs.sql")) {
                            continue;
                        }
                    }
                    verbosePrint("%s found\n", entryName);
                    count++;
                }
            }
        }
        toolsCloseDir(&pDir);
    }

    debugPrint("%"PRId64" .%s files found!\n", count, ext);
    return count;
}

static void freeFileList(enAVROTYPE avroType, int64_t count) {
    char **fileList = NULL;

    switch (avroType) {
        case enAVRO_DATA:
            fileList = g_tsDumpInAvroFiles;
            break;

        case enAVRO_TBTAGS:
            fileList = g_tsDumpInAvroTagsTbs;
            break;

        case enAVRO_NTB:
            fileList = g_tsDumpInAvroNtbs;
            break;

        case enAVRO_UNKNOWN:
            fileList = g_tsDumpInDebugFiles;
            break;

        default:
            errorPrint("%s() LN%d input mistake list: %d\n",
                    __func__, __LINE__, avroType);
            return;
    }

    for (int64_t i = 0; i < count; i++) {
        tfree(fileList[i]);
    }
    tfree(fileList);
}

static enAVROTYPE createDumpinList(const char *dbPath,
        const char *ext, int64_t count) {
    enAVROTYPE avroType = enAVRO_INVALID;
    if (0 == strcmp(ext, "sql")) {
        avroType = enAVRO_UNKNOWN;
    } else if (0 == strncmp(ext, "avro-ntb",
                strlen("avro-ntb"))) {
        avroType = enAVRO_NTB;
    } else if (0 == strncmp(ext, "avro-tbtags",
                strlen("avro-tbtags"))) {
        avroType = enAVRO_TBTAGS;
    } else if (0 == strncmp(ext, "avro", strlen("avro"))) {
        avroType = enAVRO_DATA;
    }

    switch (avroType) {
        case enAVRO_UNKNOWN:
            g_tsDumpInDebugFiles = (char **)calloc(count, sizeof(char *));
            TOOLS_ASSERT(g_tsDumpInDebugFiles);

            for (int64_t i = 0; i < count; i++) {
                g_tsDumpInDebugFiles[i] = calloc(1, MAX_FILE_NAME_LEN);
                TOOLS_ASSERT(g_tsDumpInDebugFiles[i]);
            }
            break;

        case enAVRO_NTB:
            g_tsDumpInAvroNtbs = (char **)calloc(count, sizeof(char *));
            TOOLS_ASSERT(g_tsDumpInAvroNtbs);

            for (int64_t i = 0; i < count; i++) {
                g_tsDumpInAvroNtbs[i] = calloc(1, MAX_FILE_NAME_LEN);
                TOOLS_ASSERT(g_tsDumpInAvroNtbs[i]);
            }
            break;

        case enAVRO_TBTAGS:
            g_tsDumpInAvroTagsTbs = (char **)calloc(count, sizeof(char *));
            TOOLS_ASSERT(g_tsDumpInAvroTagsTbs);

            for (int64_t i = 0; i < count; i++) {
                g_tsDumpInAvroTagsTbs[i] = calloc(1, MAX_FILE_NAME_LEN);
                TOOLS_ASSERT(g_tsDumpInAvroTagsTbs[i]);
            }
            break;

        case enAVRO_DATA:
            g_tsDumpInAvroFiles = (char **)calloc(count, sizeof(char *));
            TOOLS_ASSERT(g_tsDumpInAvroFiles);

            for (int64_t i = 0; i < count; i++) {
                g_tsDumpInAvroFiles[i] = calloc(1, MAX_FILE_NAME_LEN);
                TOOLS_ASSERT(g_tsDumpInAvroFiles[i]);
            }
            break;

        default:
            errorPrint("%s() LN%d input mistake list: %d\n",
                    __func__, __LINE__, avroType);
            return avroType;
    }

    int namelen, extlen;
    TdDirEntryPtr pDirent;
    TdDirPtr pDir;

    extlen = strlen(ext);

    int64_t nCount = 0;
    pDir = toolsOpenDir(dbPath);
    if (pDir != NULL) {
        while ((pDirent = toolsReadDir(pDir)) != NULL) {
            char *entryName = toolsGetDirEntryName(pDirent);
            namelen = strlen(entryName);

            if (namelen > extlen) {
                if (strcmp(ext, &(entryName[namelen - extlen])) == 0) {
                    verbosePrint("%s found\n", entryName);
                    switch (avroType) {
                        case enAVRO_UNKNOWN:
                            if (0 == strcmp(entryName, "dbs.sql")) {
                                continue;
                            }
                            TOOLS_STRNCPY(g_tsDumpInDebugFiles[nCount],
                                    entryName,
                                    min(namelen+1, MAX_FILE_NAME_LEN));
                            break;

                        case enAVRO_NTB:
                            TOOLS_STRNCPY(g_tsDumpInAvroNtbs[nCount],
                                    entryName,
                                    min(namelen+1, MAX_FILE_NAME_LEN));
                            break;

                        case enAVRO_TBTAGS:
                            TOOLS_STRNCPY(g_tsDumpInAvroTagsTbs[nCount],
                                    entryName,
                                    min(namelen+1, MAX_FILE_NAME_LEN));
                            break;

                        case enAVRO_DATA:
                            TOOLS_STRNCPY(g_tsDumpInAvroFiles[nCount],
                                    entryName,
                                    min(namelen+1, MAX_FILE_NAME_LEN));
                            break;

                        default:
                            errorPrint("%s() LN%d input mistake list: %d\n",
                                    __func__, __LINE__, avroType);
                            break;
                    }
                    nCount++;
                }
            }
        }
        toolsCloseDir(&pDir);
    }

    debugPrint("%"PRId64" .%s files filled to list!\n", nCount, ext);
    return avroType;
}

static int convertTbDesToJsonImplMore(
        TableDes *tableDes, int pos, char **ppstr, char *colOrTag, int i) {
    int ret = 0;
    char *pstr = *ppstr;
    int adjust = 2;

    if (g_args.loose_mode) {
        adjust = 1;
    }

    switch (tableDes->cols[pos].type) {
        case TSDB_DATA_TYPE_BINARY:
            ret = sprintf(pstr,
                    "{\"name\":\"%s%d\",\"type\":[\"null\",\"%s\"]",
                    colOrTag, i-adjust, "string");
            break;

        case TSDB_DATA_TYPE_NCHAR:
        case TSDB_DATA_TYPE_JSON:
        case TSDB_DATA_TYPE_VARBINARY:
        case TSDB_DATA_TYPE_GEOMETRY:
            ret = sprintf(pstr,
                    "{\"name\":\"%s%d\",\"type\":[\"null\",\"%s\"]",
                    colOrTag, i-adjust, "bytes");
            break;

        case TSDB_DATA_TYPE_BOOL:
            ret = sprintf(pstr,
                    "{\"name\":\"%s%d\",\"type\":[\"null\",\"%s\"]",
                    colOrTag, i-adjust, "boolean");
            break;

        case TSDB_DATA_TYPE_TINYINT:
            ret = sprintf(pstr,
                    "{\"name\":\"%s%d\",\"type\":[\"null\",\"%s\"]",
                    colOrTag, i-adjust, "int");
            break;

        case TSDB_DATA_TYPE_SMALLINT:
            ret = sprintf(pstr,
                    "{\"name\":\"%s%d\",\"type\":[\"null\",\"%s\"]",
                    colOrTag, i-adjust, "int");
            break;

        case TSDB_DATA_TYPE_INT:
            ret = sprintf(pstr,
                    "{\"name\":\"%s%d\",\"type\":[\"null\",\"%s\"]",
                    colOrTag, i-adjust, "int");
            break;

        case TSDB_DATA_TYPE_BIGINT:
            ret = sprintf(pstr,
                    "{\"name\":\"%s%d\",\"type\":[\"null\",\"%s\"]",
                    colOrTag, i-adjust, "long");
            break;

        case TSDB_DATA_TYPE_FLOAT:
            ret = sprintf(pstr,
                    "{\"name\":\"%s%d\",\"type\":[\"null\",\"%s\"]",
                    colOrTag, i-adjust, "float");
            break;

        case TSDB_DATA_TYPE_DOUBLE:
            ret = sprintf(pstr,
                    "{\"name\":\"%s%d\",\"type\":[\"null\",\"%s\"]",
                    colOrTag, i-adjust, "double");
            break;

        case TSDB_DATA_TYPE_TIMESTAMP:
            ret = sprintf(pstr,
                    "{\"name\":\"%s%d\",\"type\":[\"null\",\"%s\"]",
                    colOrTag, i-adjust, "long");
            break;

        case TSDB_DATA_TYPE_UTINYINT:
            ret = sprintf(pstr,
                    "{\"name\":\"%s%d\",\"type\":[{\"type\":\"null\"},"
                          "{\"type\":\"array\",\"items\":\"%s\"}]",
                    colOrTag, i-adjust, "int");
            break;

        case TSDB_DATA_TYPE_USMALLINT:
            ret = sprintf(pstr,
                    "{\"name\":\"%s%d\",\"type\":[{\"type\":\"null\"},"
                    "{\"type\":\"array\",\"items\":\"%s\"}]",
                    colOrTag, i-adjust, "int");
            break;

        case TSDB_DATA_TYPE_UINT:
            ret = sprintf(pstr,
                    "{\"name\":\"%s%d\",\"type\":[{\"type\":\"null\"},"
                    "{\"type\":\"array\",\"items\":\"%s\"}]",
                    colOrTag, i-adjust, "int");
            break;

        case TSDB_DATA_TYPE_UBIGINT:
            ret = sprintf(pstr,
                    "{\"name\":\"%s%d\",\"type\":[{\"type\":\"null\"},"
                    "{\"type\":\"array\",\"items\":\"%s\"}]",
                    colOrTag, i-adjust, "long");
            break;

        default:
            errorPrint("%s() LN%d, wrong type: %d\n",
                    __func__, __LINE__, tableDes->cols[pos].type);
            break;
    }

    return ret;
}


static int convertTbDesToJsonImpl(
        const char *namespace,
        const char *stable,
        const char *tbName,
        TableDes *tableDes,
        char **jsonSchema, bool onlyColumn) {

    char* outName = (char*)tbName;
    char tableName[TSDB_TABLE_NAME_LEN + 1];
    if(g_args.dotReplace && replaceCopy(tableName, (char*)tbName)) {
        outName = tableName;
    }

    char *pstr = *jsonSchema;
    pstr += sprintf(pstr,
            "{\"type\":\"record\",\"name\":\"%s.%s\",\"fields\":[",
            namespace,
            (onlyColumn)?(g_args.loose_mode?outName:"_record")
            :(g_args.loose_mode?outName:"_stb"));

    int iterate = 0;
    if (g_args.loose_mode) {
        // isCol: first column is ts
        // isTag: first column is tbname
        iterate = (onlyColumn)?(tableDes->columns):(tableDes->tags+1);
    } else {
        // isCol: add one iterates for tbname
        // isTag: add two iterates for stbname and tbname
        iterate = (onlyColumn)?(tableDes->columns+1):(tableDes->tags+2);
    }

    char *colOrTag = (onlyColumn)?"col":"tag";

    for (int i = 0; i < iterate; i++) {
        if ((0 == i) && (!g_args.loose_mode)) {
            pstr += sprintf(pstr,
                    "{\"name\":\"%s\",\"type\":%s",
                    onlyColumn?"tbname":"stbname",
                    "[\"null\",\"string\"]");
        } else if (((1 == i) && (!g_args.loose_mode))
                || ((0 == i) && (g_args.loose_mode))) {
            pstr += sprintf(pstr,
                    "{\"name\":\"%s\",\"type\":%s",
                    onlyColumn?"ts":"tbname",
                    onlyColumn?"[\"null\",\"long\"]":"[\"null\",\"string\"]");
        } else {
            int pos = i;
            if (g_args.loose_mode) {
                // isTag: pos is i-1 for tbname
                if (!onlyColumn)
                    pos = i + tableDes->columns-1;
            } else {
                // isTag: pos is i-2 for stbname and tbname
                pos = i +
                    ((onlyColumn)?(-1):
                     (tableDes->columns-2));
            }

            pstr += convertTbDesToJsonImplMore(
                    tableDes, pos, &pstr, colOrTag, i);
        }

        if (i != (iterate-1)) {
            pstr += sprintf(pstr, "},");
        } else {
            pstr += sprintf(pstr, "}");
            break;
        }
    }

    // fields end
    pstr += sprintf(pstr, "]}");

    debugPrint("%s() LN%d, jsonSchema:\n %s\n",
            __func__, __LINE__, *jsonSchema);

    return 0;
}


static int convertTbTagsDesToJsonLoose(
        const char *dbName, const char *stable, TableDes *tableDes,
        char **jsonSchema) {
    bool onlyColumn = false;
    *jsonSchema = (char *)calloc(1, getTbDesJsonSize(tableDes, onlyColumn));

    if (NULL == *jsonSchema) {
        errorPrint("%s() LN%d, memory allocation failed!\n",
                __func__, __LINE__);
        return -1;
    }

    return convertTbDesToJsonImpl(dbName, stable, tableDes->name, tableDes, jsonSchema, onlyColumn);
}

static int convertTbTagsDesToJson(
        const char *dbName, const char *stable, TableDes *tableDes,
        char **jsonSchema) {

    bool onlyColumn = false;
    *jsonSchema = (char *)calloc(1, getTbDesJsonSize(tableDes, onlyColumn));
    if (NULL == *jsonSchema) {
        errorPrint("%s() LN%d, memory allocation failed!\n", __func__, __LINE__);
        return -1;
    }

    return convertTbDesToJsonImpl(dbName, stable, tableDes->name, tableDes, jsonSchema, onlyColumn);
}

static int convertTbDesToJsonLoose(
        const char *dbName,
        const char *stable,
        const char *tbName,
        TableDes *tableDes, int colCount,
        char **jsonSchema) {

    bool onlyColumn = true;
    *jsonSchema = (char *)calloc(1, getTbDesJsonSize(tableDes, onlyColumn));

    if (NULL == *jsonSchema) {
        errorPrint("%s() LN%d, memory allocation failed!\n",
                __func__, __LINE__);
        return -1;
    }

    return convertTbDesToJsonImpl(dbName, stable, tbName, tableDes, jsonSchema, onlyColumn);
}

static int convertTbDesToJson(
        const char *dbName,
        const char *stable,
        const char *tbName, TableDes *tableDes, int colCount,
        char **jsonSchema) {

    bool onlyColumn = true;
    *jsonSchema = (char *)calloc(1, getTbDesJsonSize(tableDes, onlyColumn));

    if (NULL == *jsonSchema) {
        errorPrint("%s() LN%d, memory allocation failed!\n",
                __func__, __LINE__);
        return -1;
    }

    return convertTbDesToJsonImpl(dbName, stable, tbName, tableDes, jsonSchema, onlyColumn);
}


/* not used so far
static void print_json_integer(json_t *element, int indent) {
    print_json_indent(indent);
    printf("JSON Integer: \"%" JSON_INTEGER_FORMAT "\"\n", json_integer_value(element));
}

static void print_json_real(json_t *element, int indent) {
    print_json_indent(indent);
    printf("JSON Real: %f\n", json_real_value(element));
}

static void print_json_true(json_t *element, int indent) {
    (void)element;
    print_json_indent(indent);
    printf("JSON True\n");
}

static void print_json_false(json_t *element, int indent) {
    (void)element;
    print_json_indent(indent);
    printf("JSON False\n");
}

static void print_json_null(json_t *element, int indent) {
    (void)element;
    print_json_indent(indent);
    printf("JSON Null\n");
}
*/

void printDotOrX(int64_t count, bool *printDot) {
    if (0 == (count % g_args.data_batch)) {
        if (*printDot) {
            putchar('.');
            *printDot = false;
        } else {
            putchar('x');
            *printDot = true;
        }
    }

    fflush(stdout);
}

int64_t queryDbForDumpOutCountNative(
        char *command, TAOS *taos, const char *dbName, const char *tbName,
        const int precision) {
    int64_t count = -1;

    int32_t code = -1;
    TAOS_RES* res = taosQuery(taos, command, &code);
    if (code != 0) {
        return cleanIfQueryFailed(__func__, __LINE__, command, res);
    }

    TAOS_ROW row = taos_fetch_row(res);
    if (NULL == row) {
        if (0 == taos_errno(res)) {
            count = -1;
            debugPrint("%s fetch row null, count: %" PRId64 "\n",
                    command, count);
        } else {
            count = -1;
            errorPrint("%s() LN%d, failed run %s to fetch row, taos: %p, "
                    "code: 0x%08x, reason: %s\n",
                    __func__, __LINE__,
                    command, taos, code, taos_errstr(res));
        }
    } else {
        count = *(int64_t*)row[TSDB_SHOW_TABLES_NAME_INDEX];
        debugPrint("%s fetch row successfully, count: %" PRId64 "\n",
                command, count);
    }

    taos_free_result(res);
    free(command);
    return count;
}

int64_t queryDbForDumpOutCount(
        void  **taos_v,
        const char *dbName,
        const char *tbName,
        const int precision) {
    int64_t count = -1;

    for (int32_t i = 0; i <= g_args.retryCount; i++) {
        char *command = calloc(1, TSDB_MAX_ALLOWED_SQL_LEN);
        if (NULL == command) {
            errorPrint("%s() LN%d, memory allocation failed\n", __func__, __LINE__);
            return -1;
        }

        int64_t startTime = getStartTime(precision);
        int64_t endTime = getEndTime(precision);

        snprintf(command, TSDB_MAX_ALLOWED_SQL_LEN,
                g_args.db_escape_char
                ? "SELECT COUNT(*) FROM `%s`.%s%s%s WHERE _c0 >= %" PRId64 " "
                "AND _c0 <= %" PRId64 ""
                : "SELECT COUNT(*) FROM %s.%s%s%s WHERE _c0 >= %" PRId64 " "
                "AND _c0 <= %" PRId64,
                dbName, g_escapeChar, tbName, g_escapeChar,
                startTime, endTime);

        count = queryDbForDumpOutCountNative(
                    command, *taos_v, dbName, tbName, precision);

        if (count >= 0) {
            if (i > 0) {
                debugPrint("Retry %d to execute queryDbForDumpOutCount successfully, dbName: %s, tbName: %s\n", i, dbName, tbName);
            }
            return count;
        }

        // fail
        errorPrint("Failed to execute queryDbForDumpOutCount, dbName: %s, tbName: %s\n", dbName, tbName);

        // retry again
        debugPrint("Retry to execute queryDbForDumpOutCount for %d time(s) after sleep %dms, dbName: %s, tbName: %s\n", i, g_args.retrySleepMs, dbName, tbName);
        toolsMsleep(g_args.retrySleepMs);
    }

    errorPrint("Retry count exceeds %d, give up retry.\n", g_args.retryCount);
    return count;
}


static TAOS_RES *queryDbForDumpOutOffsetNative(TAOS *taos, char *command) {
    int32_t code = -1;
    TAOS_RES* res = taosQuery(taos, command, &code);
    if (code != 0) {
        cleanIfQueryFailed(__func__, __LINE__, command, res);
        return NULL;
    }

    free(command);
    return res;
}

void *queryDbForDumpOutOffset(
        void **taos_v,
        const char *dbName,
        const char *tbName,
        const int precision,
        const int64_t start_time,
        const int64_t end_time,
        const int64_t limit,
        const int64_t offset) {
    char *command = calloc(1, TSDB_MAX_ALLOWED_SQL_LEN);
    if (NULL == command) {
        errorPrint("%s() LN%d, memory allocation failed\n", __func__, __LINE__);
        return NULL;
    }

    if (-1 == limit) {
        snprintf(command, TSDB_MAX_ALLOWED_SQL_LEN,
                g_args.db_escape_char
                ? "SELECT * FROM `%s`.%s%s%s WHERE _c0 >= %" PRId64 " "
                "AND _c0 <= %" PRId64 " ORDER BY _c0 ASC ;"
                : "SELECT * FROM %s.%s%s%s WHERE _c0 >= %" PRId64 " "
                "AND _c0 <= %" PRId64 " ORDER BY _c0 ASC ;",
                dbName, g_escapeChar, tbName, g_escapeChar,
                start_time, end_time);
    } else {
        snprintf(command, TSDB_MAX_ALLOWED_SQL_LEN,
                g_args.db_escape_char
                ? "SELECT * FROM `%s`.%s%s%s WHERE _c0 >= %" PRId64 " "
                "AND _c0 <= %" PRId64 " ORDER BY _c0 ASC LIMIT %" PRId64 " "
                "OFFSET %" PRId64 ";"
                : "SELECT * FROM %s.%s%s%s WHERE _c0 >= %" PRId64 " "
                "AND _c0 <= %" PRId64 " ORDER BY _c0 ASC LIMIT %" PRId64 " "
                "OFFSET %" PRId64 ";",
                dbName, g_escapeChar, tbName, g_escapeChar,
                start_time, end_time, limit, offset);
    }

    void *res = queryDbForDumpOutOffsetNative(*taos_v, command);
    return res;
}

int processValueToAvro(
        const int32_t col,
        avro_value_t record,
        avro_value_t avro_value,
        avro_value_t branch,
        const char *name,
        const uint8_t type,
        const int32_t bytes,
        const void *value,
        const int32_t len
        ) {
    char tmpBuf[TSDB_COL_NAME_LEN] = {0};

    if (0 == col) {
        snprintf(tmpBuf, TSDB_COL_NAME_LEN, "ts");
    } else {
        snprintf(tmpBuf, TSDB_COL_NAME_LEN, "col%d", col-1);
    }

    if (0 != avro_value_get_by_name(
                &record,
                tmpBuf,
                &avro_value, NULL)) {
        errorPrint("%s() LN%d, avro_value_get_by_name(%s) failed\n",
                __func__, __LINE__, name);
        return -1;
    }

    avro_value_t firsthalf, secondhalf;
    uint8_t u8Temp = 0;
    uint16_t u16Temp = 0;
    uint32_t u32Temp = 0;
    uint64_t u64Temp = 0;
    memset(&firsthalf, 0, sizeof(avro_value_t));
    memset(&secondhalf, 0, sizeof(avro_value_t));

    switch (type) {
        case TSDB_DATA_TYPE_BOOL:
            if (NULL == value) {
                avro_value_set_branch(&avro_value, 0, &branch);
                verbosePrint("%s() LN%d, before set_bool() null\n",
                        __func__, __LINE__);
                avro_value_set_null(&branch);
            } else {
                avro_value_set_branch(&avro_value, 1, &branch);
                char tmp = *(char*)value;
                verbosePrint("%s() LN%d, before set_bool() tmp=%d\n",
                        __func__, __LINE__, (int)tmp);
                avro_value_set_boolean(&branch, (tmp)?1:0);
            }
            break;

        case TSDB_DATA_TYPE_TINYINT:
            if (NULL == value) {
                avro_value_set_branch(&avro_value, 0, &branch);
                avro_value_set_null(&branch);
            } else {
                avro_value_set_branch(&avro_value, 1, &branch);
                avro_value_set_int(&branch, *((int8_t *)value));
            }
            break;

        case TSDB_DATA_TYPE_SMALLINT:
            if (NULL == value) {
                avro_value_set_branch(&avro_value, 0, &branch);
                avro_value_set_null(&branch);
            } else {
                avro_value_set_branch(&avro_value, 1, &branch);
                avro_value_set_int(&branch, *((int16_t *)value));
            }
            break;

        case TSDB_DATA_TYPE_INT:
            if (NULL == value) {
                avro_value_set_branch(&avro_value, 0, &branch);
                avro_value_set_null(&branch);
            } else {
                avro_value_set_branch(&avro_value, 1, &branch);
                avro_value_set_int(&branch, *((int32_t *)value));
            }
            break;

        case TSDB_DATA_TYPE_UTINYINT:
            if (NULL == value) {
                avro_value_set_branch(&avro_value, 0, &branch);
                avro_value_set_null(&branch);
            } else {
                avro_value_set_branch(&avro_value, 1, &branch);
                u8Temp = *((uint8_t *)value);

                int8_t n8tmp = (int8_t)(u8Temp - SCHAR_MAX);
                avro_value_append(&branch, &firsthalf, NULL);
                avro_value_set_int(&firsthalf, n8tmp);
                debugPrint("%s() LN%d, first half is: %d, ",
                        __func__, __LINE__, (int32_t)n8tmp);
                avro_value_append(&branch, &secondhalf, NULL);
                avro_value_set_int(&secondhalf, (int32_t)SCHAR_MAX);
                debugPrint("second half is: %d\n", (int32_t)SCHAR_MAX);
            }

            break;

        case TSDB_DATA_TYPE_USMALLINT:
            if (NULL == value) {
                avro_value_set_branch(&avro_value, 0, &branch);
                avro_value_set_null(&branch);
            } else {
                avro_value_set_branch(&avro_value, 1, &branch);
                u16Temp = *((uint16_t *)value);

                int16_t n16tmp = (int16_t)(u16Temp - SHRT_MAX);
                avro_value_append(&branch, &firsthalf, NULL);
                avro_value_set_int(&firsthalf, n16tmp);
                debugPrint("%s() LN%d, first half is: %d, ",
                        __func__, __LINE__, (int32_t)n16tmp);
                avro_value_append(&branch, &secondhalf, NULL);
                avro_value_set_int(&secondhalf, (int32_t)SHRT_MAX);
                debugPrint("second half is: %d\n", (int32_t)SHRT_MAX);
            }

            break;

        case TSDB_DATA_TYPE_UINT:
            if (NULL == value) {
                avro_value_set_branch(&avro_value, 0, &branch);
                avro_value_set_null(&branch);
            } else {
                avro_value_set_branch(&avro_value, 1, &branch);
                u32Temp = *((uint32_t *)value);

                int32_t n32tmp = (int32_t)(u32Temp - INT_MAX);
                avro_value_append(&branch, &firsthalf, NULL);
                avro_value_set_int(&firsthalf, n32tmp);
                debugPrint("%s() LN%d, first half is: %d, ",
                        __func__, __LINE__, n32tmp);
                avro_value_append(&branch, &secondhalf, NULL);
                avro_value_set_int(&secondhalf, (int32_t)INT_MAX);
                debugPrint("second half is: %d\n", INT_MAX);
            }

            break;

        case TSDB_DATA_TYPE_UBIGINT:
            if (NULL == value) {
                avro_value_set_branch(&avro_value, 0, &branch);
                avro_value_set_null(&branch);
            } else {
                avro_value_set_branch(&avro_value, 1, &branch);
                u64Temp = *((uint64_t *)value);

                int64_t n64tmp = (int64_t)(u64Temp - LONG_MAX);
                avro_value_append(&branch, &firsthalf, NULL);
                avro_value_set_long(&firsthalf, n64tmp);
                debugPrint("%s() LN%d, first half is: %"PRId64", ",
                        __func__, __LINE__, n64tmp);
                avro_value_append(&branch, &secondhalf, NULL);
                avro_value_set_long(&secondhalf, LONG_MAX);
                debugPrint("second half is: %"PRId64"\n", (int64_t) LONG_MAX);
            }

            break;

        case TSDB_DATA_TYPE_BIGINT:
            if (NULL == value) {
                avro_value_set_branch(&avro_value, 0, &branch);
                avro_value_set_null(&branch);
            } else {
                avro_value_set_branch(&avro_value, 1, &branch);
                avro_value_set_long(&branch, *((int64_t *)value));
            }
            break;

        case TSDB_DATA_TYPE_FLOAT:
            if (NULL == value) {
                avro_value_set_branch(&avro_value, 0, &branch);
                avro_value_set_float(&branch, TSDB_DATA_FLOAT_NULL);
            } else {
                avro_value_set_branch(&avro_value, 1, &branch);
                avro_value_set_float(&branch, GET_FLOAT_VAL(value));
            }
            break;

        case TSDB_DATA_TYPE_DOUBLE:
            if (NULL == value) {
                avro_value_set_branch(&avro_value, 0, &branch);
                avro_value_set_double(&branch, TSDB_DATA_DOUBLE_NULL);
            } else {
                avro_value_set_branch(&avro_value, 1, &branch);
                avro_value_set_double(&branch, GET_DOUBLE_VAL(value));
            }
            break;
        case TSDB_DATA_TYPE_BINARY:
            if (NULL == value) {
                avro_value_set_branch(&avro_value, 0, &branch);
                avro_value_set_null(&branch);
            } else {
                avro_value_set_branch(&avro_value, 1, &branch);
                char *binTemp = calloc(1, 1+bytes);
                TOOLS_ASSERT(binTemp);
                strncpy(binTemp, (char*)value, len);
                avro_value_set_string(&branch, binTemp);
                free(binTemp);
            }
            break;

        case TSDB_DATA_TYPE_NCHAR:
        case TSDB_DATA_TYPE_JSON:
        case TSDB_DATA_TYPE_VARBINARY:
        case TSDB_DATA_TYPE_GEOMETRY:
            if (NULL == value) {
                avro_value_set_branch(&avro_value, 0, &branch);
                avro_value_set_null(&branch);
            } else {
                avro_value_set_branch(&avro_value, 1, &branch);
                avro_value_set_bytes(&branch, (void*)(value),
                        len);
            }
            break;

        case TSDB_DATA_TYPE_TIMESTAMP:
            if (NULL == value) {
                avro_value_set_branch(&avro_value, 0, &branch);
                avro_value_set_null(&branch);
            } else {
                avro_value_set_branch(&avro_value, 1, &branch);
                avro_value_set_long(&branch, *((int64_t *)value));
            }
            break;

        default:
            break;
    }

    return 0;
}

static int64_t writeResultToAvro(
        const char *avroFilename,
        const char *dbName,
        const char *tbName,
        char *jsonSchema,
        void **taos_v,
        int precision,
        int64_t start_time,
        int64_t end_time) {
    int64_t queryCount = queryDbForDumpOutCount(
            taos_v, dbName, tbName, precision);
    if (queryCount <=0) {
        return 0;
    }

    char* outName = (char*)tbName;
    char tableName[TSDB_TABLE_NAME_LEN + 1];
    if(g_args.dotReplace && replaceCopy(tableName, (char*)tbName)) {
        outName = tableName;
    }

    avro_schema_t schema;
    RecordSchema *recordSchema;
    avro_file_writer_t db;

    avro_value_iface_t *wface = prepareAvroWface(
            avroFilename,
            jsonSchema, &schema, &recordSchema, &db);

    int64_t success = 0;
    int64_t failed = 0;

    bool printDot = true;

    int numFields = -1;
    TAOS_FIELD *fields = NULL;

    int currentPercent = 0;
    int percentComplete = 0;

    int64_t limit = g_args.data_batch;
    int64_t offset = 0;

    do {
        if (queryCount > limit) {
            if (limit > (queryCount - offset )) {
                limit = queryCount - offset;
            }
        } else {
            limit = queryCount;
        }

        void *res = queryDbForDumpOutOffset(
                taos_v, dbName, tbName, precision,
                start_time, end_time, limit, offset);
        if (NULL == res) {
            break;
        }

        numFields = taos_field_count(res);

        fields = taos_fetch_fields(res);
        TOOLS_ASSERT(fields);

        int32_t countInBatch = 0;
        TAOS_ROW row;

        // loop row
        while (NULL != (row = taos_fetch_row(res))) {
            int32_t *lengths = taos_fetch_lengths(res);

            avro_value_t record;
            avro_generic_value_new(wface, &record);

            // avro_value is key , branch is value
            avro_value_t avro_value, branch;

            // set tbname none loose mode
            if (!g_args.loose_mode) {
                if (0 != avro_value_get_by_name(
                            &record, "tbname", &avro_value, NULL)) {
                    errorPrint("%s() LN%d, avro_value_get_by_name(tbname) "
                            "failed dbName=%s tbName=%s\n",
                            __func__, __LINE__, dbName, tbName);
                    break;
                }
                avro_value_set_branch(&avro_value, 1, &branch);
                avro_value_set_string(&branch, outName);
            }

            // loop col for row
            for (int32_t col = 0; col < numFields; col++) {
                processValueToAvro(col,
                        record,
                        avro_value, branch,
                        fields[col].name,
                        fields[col].type,
                        fields[col].bytes,
                        row[col],
                        lengths[col]);
            }

            if (0 != avro_file_writer_append_value(db, &record)) {
                errorPrint("%s() LN%d, "
                        "Unable to write record to file. Message: %s dbName=%s tbName=%s\n",
                        __func__, __LINE__,
                        avro_strerror(), dbName, tbName);
                failed--;
            } else {
                success++;
            }

            countInBatch++;
            avro_value_decref(&record);
        }

        if (countInBatch != limit) {
            errorPrint("%s() LN%d, actual dump out rows not equal to batch size. actual dump out: %d, batch %" PRId64 " dbName=%s tbName=%s\n",
                    __func__, __LINE__,
                    countInBatch, limit, dbName, tbName);
        }

        taos_free_result(res);
        printDotOrX(offset, &printDot);
        offset += countInBatch;

        currentPercent = ((offset) * 100 / queryCount);
        if (currentPercent > percentComplete) {
            infoPrint("%s.%s [%" PRId64 "/%" PRId64 "] write avro %d%% of %s\n", g_dbName, g_stbName ,g_tableDone + 1, g_tableCount, currentPercent, tbName);
            percentComplete = currentPercent;
        }
    } while (offset < queryCount);

    if (percentComplete < 100) {
        errorPrint("%d%% of %s\n", percentComplete, tbName);
    }

    avro_value_iface_decref(wface);
    freeRecordSchema(recordSchema);
    avro_file_writer_close(db);
    avro_schema_decref(schema);

    return success;
}

static void freeBindArray(char *bindArray, int elements) {
    TAOS_MULTI_BIND *bind;

    for (int j = 0; j < elements; j++) {
        bind = (TAOS_MULTI_BIND *)((char *)bindArray
                + (sizeof(TAOS_MULTI_BIND) * j));
        if ((TSDB_DATA_TYPE_BINARY != bind->buffer_type)
                && (TSDB_DATA_TYPE_VARBINARY != bind->buffer_type)
                && (TSDB_DATA_TYPE_GEOMETRY  != bind->buffer_type)
                && (TSDB_DATA_TYPE_NCHAR     != bind->buffer_type)
                && (TSDB_DATA_TYPE_JSON      != bind->buffer_type)) {
            tfree(bind->buffer);
        }
    }
}

static int32_t dumpInAvroTagTinyInt(FieldStruct *field, avro_value_t *value,
                              char *sqlstr, int32_t curr_sqlstr_len) {
    if (field->nullable) {
        avro_value_t tinyint_branch;
        avro_value_get_current_branch(
                         value, &tinyint_branch);

        if (0 == avro_value_get_null(&tinyint_branch)) {
            debugPrint2("%s | ", "null");
            curr_sqlstr_len += sprintf(
                             sqlstr+curr_sqlstr_len, "NULL,");
        } else {
            int32_t n8 = 0;
            avro_value_get_int(&tinyint_branch, &n8);
            debugPrint2("%d | ", n8);
            curr_sqlstr_len += sprintf(
                             sqlstr+curr_sqlstr_len, "%d,", n8);
        }
    } else {
        int32_t n8 = 0;
        avro_value_get_int(value, &n8);

        verbosePrint("%s() LN%d: *n8=%d null=%d\n",
                     __func__, __LINE__, (int8_t)n8,
                     (int8_t)TSDB_DATA_TINYINT_NULL);

        if ((int8_t)TSDB_DATA_TINYINT_NULL == (int8_t)n8) {
            debugPrint2("%s | ", "null");
            curr_sqlstr_len += sprintf(
                             sqlstr+curr_sqlstr_len, "NULL,");
        } else {
            debugPrint2("%d | ", n8);
            curr_sqlstr_len += sprintf(
                             sqlstr+curr_sqlstr_len, "%d,", n8);
        }
    }
    return curr_sqlstr_len;
}

static int32_t dumpInAvroTagSmallInt(FieldStruct *field, avro_value_t *value,
                              char *sqlstr, int32_t curr_sqlstr_len) {
    if (field->nullable) {
        avro_value_t smallint_branch;
        avro_value_get_current_branch(value, &smallint_branch);

        if (0 == avro_value_get_null(&smallint_branch)) {
            debugPrint2("%s | ", "null");
            curr_sqlstr_len += sprintf(
                             sqlstr+curr_sqlstr_len, "NULL,");
        } else {
            int32_t n16 = 0;
            avro_value_get_int(&smallint_branch, &n16);
            debugPrint2("%d | ", n16);
            curr_sqlstr_len += sprintf(
                             sqlstr+curr_sqlstr_len, "%d,", n16);
        }
    } else {
        int32_t n16 = 0;
        avro_value_get_int(value, &n16);
        verbosePrint("%s() LN%d: *n16=%d null=%d\n",
                              __func__, __LINE__, (int16_t)n16,
                              (int16_t)TSDB_DATA_SMALLINT_NULL);
        if ((int16_t)TSDB_DATA_SMALLINT_NULL == (int16_t)n16) {
            debugPrint2("%s | ", "null");
            curr_sqlstr_len += sprintf(
                             sqlstr+curr_sqlstr_len, "NULL,");
        } else {
            debugPrint2("%d | ", n16);
            curr_sqlstr_len += sprintf(
                             sqlstr+curr_sqlstr_len, "%d,", n16);
        }
    }
    return curr_sqlstr_len;
}

static int32_t dumpInAvroTagInt(FieldStruct *field, avro_value_t *value,
                              char *sqlstr, int32_t curr_sqlstr_len) {
    if (field->nullable) {
        avro_value_t int_branch;
        avro_value_get_current_branch(
            value, &int_branch);

        if (0 == avro_value_get_null(&int_branch)) {
            debugPrint2("%s | ", "null");
            curr_sqlstr_len += sprintf(
                             sqlstr+curr_sqlstr_len, "NULL,");
        } else {
            int32_t n32 = 0;
            avro_value_get_int(&int_branch, &n32);
            debugPrint2("%d | ", n32);
            curr_sqlstr_len += sprintf(
                             sqlstr+curr_sqlstr_len,
                             "%d,", n32);
        }
    } else {
        int32_t n32 = 0;
        avro_value_get_int(value, &n32);
        verbosePrint("%s() LN%d: *n32=%d null=%d\n",
                              __func__, __LINE__, n32,
                              (int32_t)TSDB_DATA_INT_NULL);
        if ((int32_t)TSDB_DATA_INT_NULL == n32) {
            debugPrint2("%s | ", "null");
            curr_sqlstr_len += sprintf(
                             sqlstr+curr_sqlstr_len, "NULL,");
        } else {
            debugPrint2("%d | ", n32);
            curr_sqlstr_len += sprintf(
                             sqlstr+curr_sqlstr_len,
                             "%d,", n32);
        }
    }
    return curr_sqlstr_len;
}

static int32_t dumpInAvroTagBigInt(FieldStruct *field, avro_value_t *value,
                              char *sqlstr, int32_t curr_sqlstr_len) {
    if (field->nullable) {
        avro_value_t bigint_branch;
        avro_value_get_current_branch(
            value, &bigint_branch);
        if (0 == avro_value_get_null(&bigint_branch)) {
            debugPrint2("%s | ", "null");
            curr_sqlstr_len += sprintf(
                    sqlstr+curr_sqlstr_len, "NULL,");
        } else {
            int64_t n64 = 0;
            avro_value_get_long(&bigint_branch, &n64);
            debugPrint2("%"PRId64" | ", n64);
            curr_sqlstr_len += sprintf(
                    sqlstr+curr_sqlstr_len,
                    "%"PRId64",", n64);
        }
    } else {
        int64_t n64 = 0;
        avro_value_get_long(value, &n64);
        verbosePrint("%s() LN%d: *n64=%"PRId64" null=%"PRId64"\n",
            __func__, __LINE__, n64,
            (int64_t)TSDB_DATA_BIGINT_NULL);
        if ((int64_t)TSDB_DATA_BIGINT_NULL == n64) {
            debugPrint2("%s | ", "null");
            curr_sqlstr_len += sprintf(
                sqlstr+curr_sqlstr_len, "NULL,");
        } else {
            debugPrint2("%"PRId64" | ", n64);
            curr_sqlstr_len += sprintf(sqlstr+curr_sqlstr_len,
                                       "%"PRId64",", n64);
        }
    }
    return curr_sqlstr_len;
}

static int32_t dumpInAvroTagFloat(FieldStruct *field, avro_value_t *value,
                              char *sqlstr, int32_t curr_sqlstr_len) {
    if (field->nullable) {
        avro_value_t float_branch;
        avro_value_get_current_branch(value, &float_branch);
        if (0 == avro_value_get_null(&float_branch)) {
            debugPrint2("%s | ", "NULL");
            curr_sqlstr_len += sprintf(sqlstr+curr_sqlstr_len, "NULL,");
        } else {
            float f = 0.0;
            avro_value_get_float(&float_branch, &f);
            debugPrint2("%f | ", f);
            curr_sqlstr_len += sprintf(
                sqlstr+curr_sqlstr_len, "%f,", f);
        }
    } else {
        float f = 0.0;
        avro_value_get_float(value, &f);
        if (TSDB_DATA_FLOAT_NULL == f) {
            debugPrint2("%s | ", "NULL");
            curr_sqlstr_len += sprintf(
                sqlstr+curr_sqlstr_len, "NULL,");
        } else {
            debugPrint2("%f | ", f);
            curr_sqlstr_len += sprintf(sqlstr+curr_sqlstr_len, "%f,", f);
        }
    }
    return curr_sqlstr_len;
}

static int32_t dumpInAvroTagDouble(FieldStruct *field, avro_value_t *value,
                              char *sqlstr, int32_t curr_sqlstr_len) {
    if (field->nullable) {
        avro_value_t dbl_branch;
        avro_value_get_current_branch(value,
            &dbl_branch);
        if (0 == avro_value_get_null(&dbl_branch)) {
            debugPrint2("%s | ", "NULL");
            curr_sqlstr_len += sprintf(sqlstr+curr_sqlstr_len, "NULL,");
        } else {
            double dbl = 0.0;
            avro_value_get_double(&dbl_branch, &dbl);
            debugPrint2("%f | ", dbl);
            curr_sqlstr_len += sprintf(sqlstr+curr_sqlstr_len, "%f,", dbl);
        }
    } else {
        double dbl = 0.0;
        avro_value_get_double(value, &dbl);
        if (TSDB_DATA_DOUBLE_NULL == dbl) {
            debugPrint2("%s | ", "NULL");
            curr_sqlstr_len += sprintf(sqlstr+curr_sqlstr_len, "NULL,");
        } else {
            debugPrint2("%f | ", dbl);
            curr_sqlstr_len += sprintf(sqlstr+curr_sqlstr_len, "%f,", dbl);
        }
    }
    return curr_sqlstr_len;
}

static int32_t appendValues(char *buf, char* val) {
    int32_t val_len = (int32_t)strlen(val);
    int32_t n1      = 0; // '
    int32_t n2      = 0; // ""
    int32_t i;

    // check
    for (i = 0; i < val_len; i++) {
        if(val[i] == '\'') {
            n1 ++;
        } else if(val[i] == '\"') {
            n2 ++;
        }
    }

    if (n1 == 0) {
        // no single quotation mark
        return sprintf(buf, "\'%s\',", val);
    } else if (n2 == 0) {
        // no double quotation mark
        return sprintf(buf, "\"%s\",", val);
    }

    // n1 > 0 && n2 > 0
    int pos = 0;
    buf[0] = '\"';
    pos ++;
    for (i = 0; i < val_len; i++) {
        buf[pos] = val[i];
        if(val[i] == '\'') {
            buf[++pos] = '\'';
        } else if(buf[i] == '\"') {
            buf[++pos] = '\"';
        }
    }
    buf[pos] = '\"';
    pos ++;

    return pos;
}

static int32_t dumpInAvroTagBinary(FieldStruct *field, avro_value_t *value,
                              char *sqlstr, int32_t curr_sqlstr_len) {
    avro_value_t branch;
    avro_value_get_current_branch(
        value, &branch);

    char *buf = NULL;
    size_t bin_size;

    avro_value_get_string(&branch,
        (const char **)&buf, &bin_size);

    if (NULL == buf) {
        debugPrint2("%s | ", "NULL");
        curr_sqlstr_len += sprintf(
            sqlstr+curr_sqlstr_len, "NULL,");
    } else {
        debugPrint2("%s | ", (char *)buf);
        curr_sqlstr_len += appendValues(sqlstr + curr_sqlstr_len, buf);
    }
    return curr_sqlstr_len;
}

static int32_t dumpInAvroTagNChar(FieldStruct *field, avro_value_t *value,
                              char *sqlstr, int32_t curr_sqlstr_len) {
    size_t bytessize;
    void *bytesbuf = NULL;

    avro_value_t nchar_branch;
    avro_value_get_current_branch(value, &nchar_branch);

    avro_value_get_bytes(&nchar_branch,
        (const void **)&bytesbuf, &bytessize);

    if (NULL == bytesbuf) {
        debugPrint2("%s | ", "NULL");
        curr_sqlstr_len += sprintf(sqlstr+curr_sqlstr_len, "NULL,");
    } else {
        debugPrint2("%s | ", (char *)bytesbuf);
        curr_sqlstr_len += appendValues(sqlstr + curr_sqlstr_len, (char *)bytesbuf);
    }
    return curr_sqlstr_len;
}

static int32_t dumpInAvroTagTimeStamp(FieldStruct *field, avro_value_t *value,
                              char *sqlstr, int32_t curr_sqlstr_len) {
    if (field->nullable) {
        avro_value_t ts_branch;
        avro_value_get_current_branch(value, &ts_branch);
        if (0 == avro_value_get_null(&ts_branch)) {
            debugPrint2("%s | ", "null");
            curr_sqlstr_len += sprintf(
                sqlstr+curr_sqlstr_len, "NULL,");
        } else {
            int64_t n64 = 0;
            avro_value_get_long(&ts_branch, &n64);
            debugPrint2("%"PRId64" | ", n64);
            curr_sqlstr_len += sprintf(
                sqlstr+curr_sqlstr_len,
                "%"PRId64",", n64);
        }
    } else {
        int64_t n64 = 0;
        avro_value_get_long(value, &n64);
        verbosePrint("%s() LN%d: *n64=%"PRId64" null=%"PRId64"\n",
            __func__, __LINE__, n64,
            (int64_t)TSDB_DATA_BIGINT_NULL);
        if ((int64_t)TSDB_DATA_BIGINT_NULL == n64) {
            debugPrint2("%s | ", "null");
            curr_sqlstr_len += sprintf(
                sqlstr+curr_sqlstr_len, "NULL,");
        } else {
            debugPrint2("%"PRId64" | ", n64);
            curr_sqlstr_len += sprintf(
                sqlstr+curr_sqlstr_len,
                "%"PRId64",", n64);
        }
    }
    return curr_sqlstr_len;
}

static int32_t dumpInAvroTagUnsignedTinyInt(FieldStruct *field,
                                         avro_value_t *value,
                              char *sqlstr, int32_t curr_sqlstr_len) {
    if (field->nullable) {
        avro_value_t utinyint_branch;
        avro_value_get_current_branch(value,
            &utinyint_branch);

        if (0 == avro_value_get_null(&utinyint_branch)) {
            debugPrint2("%s | ", "null");
            curr_sqlstr_len += sprintf(
                                 sqlstr+curr_sqlstr_len, "NULL,");
        } else {
            if (TSDB_DATA_TYPE_INT == field->array_type) {
                uint8_t array_u8 = 0;
                size_t array_size = 0;
                avro_value_get_size(&utinyint_branch, &array_size);

                debugPrint("%s() LN%d, array_size: %zu\n",
                                        __func__, __LINE__, array_size);
                for (size_t item = 0; item < array_size; item++) {
                    int32_t n32tmp = 0;
                    avro_value_t item_value;
                    avro_value_get_by_index(&utinyint_branch, item,
                        &item_value, NULL);
                    avro_value_get_int(&item_value, &n32tmp);
                    array_u8 += (int8_t)n32tmp;
                }

                debugPrint2("%u | ", (uint32_t)array_u8);
                curr_sqlstr_len += sprintf(
                                     sqlstr+curr_sqlstr_len, "%u,",
                                     (uint32_t)array_u8);
            } else {
                errorPrint("%s() LN%d mix type %s(%d) with int array.\n",
                    __func__, __LINE__,
                    typeToStr(field->array_type), field->array_type);
            }
        }
    } else {
        if (TSDB_DATA_TYPE_INT == field->array_type) {
            uint8_t array_u8 = 0;
            size_t array_size = 0;
            avro_value_get_size(value, &array_size);

            debugPrint("%s() LN%d, array_size: %zu\n",
                                __func__, __LINE__, array_size);
            for (size_t item = 0; item < array_size; item++) {
                int32_t n32tmp = 0;
                avro_value_t item_value;
                avro_value_get_by_index(value, item,
                                                 &item_value, NULL);
                avro_value_get_int(&item_value, &n32tmp);
                array_u8 += (int8_t)n32tmp;
            }

            if (TSDB_DATA_UTINYINT_NULL == array_u8) {
                debugPrint2("%s |", "null");
                curr_sqlstr_len += sprintf(
                                 sqlstr+curr_sqlstr_len, "NULL,");
            } else {
                debugPrint2("%u | ", (uint32_t)array_u8);
                curr_sqlstr_len += sprintf(
                                 sqlstr+curr_sqlstr_len, "%u,",
                                 (uint32_t)array_u8);
            }
        } else {
            errorPrint("%s() LN%d mix type %s(%d) with int array\n",
                __func__, __LINE__,
                typeToStr(field->array_type), field->array_type);
        }
    }
    return curr_sqlstr_len;
}

static int32_t dumpInAvroTagUnsignedSmallInt(FieldStruct *field,
                                         avro_value_t *value,
                              char *sqlstr, int32_t curr_sqlstr_len) {
    if (field->nullable) {
        avro_value_t usmint_branch;
        avro_value_get_current_branch(value, &usmint_branch);

        if (0 == avro_value_get_null(&usmint_branch)) {
            debugPrint2("%s | ", "null");
            curr_sqlstr_len += sprintf(
                                 sqlstr+curr_sqlstr_len, "NULL,");
        } else {
            if (TSDB_DATA_TYPE_INT == field->array_type) {
                uint16_t array_u16 = 0;
                size_t array_size = 0;
                avro_value_get_size(&usmint_branch, &array_size);
                debugPrint("%s() LN%d, array_size: %zu\n",
                                        __func__, __LINE__, array_size);
                for (size_t item = 0; item < array_size; item++) {
                    int32_t n32tmp = 0;
                    avro_value_t item_value;
                    avro_value_get_by_index(&usmint_branch, item,
                                                         &item_value, NULL);
                    avro_value_get_int(&item_value, &n32tmp);
                    array_u16 += (int16_t)n32tmp;
                }
                debugPrint2("%u | ", (uint32_t)array_u16);
                curr_sqlstr_len += sprintf(
                                     sqlstr+curr_sqlstr_len, "%u,",
                                     (uint32_t)array_u16);
            } else {
                errorPrint("%s() LN%d mix type %s with int array",
                                        __func__, __LINE__,
                typeToStr(field->array_type));
            }
        }
    } else {
        if (TSDB_DATA_TYPE_INT == field->array_type) {
            uint16_t array_u16 = 0;
            size_t array_size = 0;
            avro_value_get_size(value, &array_size);
            debugPrint("%s() LN%d, array_size: %zu\n",
                                __func__, __LINE__, array_size);
            for (size_t item = 0; item < array_size; item++) {
                int32_t n32tmp = 0;
                avro_value_t item_value;
                avro_value_get_by_index(value, item,
                                                 &item_value, NULL);
                avro_value_get_int(&item_value, &n32tmp);
                array_u16 += (int16_t)n32tmp;
            }

            if (TSDB_DATA_USMALLINT_NULL == array_u16) {
                debugPrint2("%s |", "null");
                curr_sqlstr_len += sprintf(
                                 sqlstr+curr_sqlstr_len, "NULL,");
            } else {
                debugPrint2("%u | ", (uint32_t)array_u16);
                curr_sqlstr_len += sprintf(
                                 sqlstr+curr_sqlstr_len, "%u,",
                                 (uint32_t)array_u16);
            }
        } else {
            errorPrint("%s() LN%d mix type %s with int array",
                            __func__, __LINE__,
            typeToStr(field->array_type));
        }
    }
    return curr_sqlstr_len;
}

static int32_t dumpInAvroTagUnsignedInt(FieldStruct *field,
                                     avro_value_t *value,
                              char *sqlstr, int32_t curr_sqlstr_len) {
    if (field->nullable) {
        avro_value_t uint_branch;
        avro_value_get_current_branch(value, &uint_branch);

        if (0 == avro_value_get_null(&uint_branch)) {
            debugPrint2("%s | ", "null");
            curr_sqlstr_len += sprintf(
                    sqlstr+curr_sqlstr_len, "NULL,");
        } else {
            if (TSDB_DATA_TYPE_INT == field->array_type) {
                uint32_t array_u32 = 0;
                size_t array_size = 0;
                int32_t n32tmp;
                avro_value_get_size(&uint_branch, &array_size);

                debugPrint("%s() LN%d, array_size: %zu\n",
                    __func__, __LINE__, array_size);
                for (size_t item = 0; item < array_size; item++) {
                    avro_value_t item_value;
                    avro_value_get_by_index(&uint_branch, item,
                        &item_value, NULL);
                    avro_value_get_int(&item_value, &n32tmp);
                    array_u32 += n32tmp;
                    debugPrint("%s() LN%d, array index: %d, n32tmp: %d, "
                               "array_u32: %u\n",
                               __func__, __LINE__,
                               (int)item, n32tmp,
                               (uint32_t)array_u32);
                }
                debugPrint2("%u | ", (uint32_t)array_u32);
                curr_sqlstr_len += sprintf(
                    sqlstr+curr_sqlstr_len, "%u,",
                    (uint32_t)array_u32);
            } else {
                errorPrint("%s() LN%d mix type %s with int array",
                    __func__, __LINE__,
                typeToStr(field->array_type));
            }
        }
    } else {
        if (TSDB_DATA_TYPE_INT == field->array_type) {
            uint32_t array_u32 = 0;
            size_t array_size = 0;
            int32_t n32tmp;
            avro_value_get_size(value, &array_size);
            debugPrint("%s() LN%d, array_size: %zu\n",
                __func__, __LINE__, array_size);
            for (size_t item = 0; item < array_size; item++) {
                avro_value_t item_value;
                avro_value_get_by_index(value, item,
                    &item_value, NULL);
                avro_value_get_int(&item_value, &n32tmp);
                array_u32 += n32tmp;
                debugPrint("%s() LN%d, array index: %d, n32tmp: %d, "
                           "array_u32: %u\n",
                           __func__, __LINE__,
                           (int)item, n32tmp,
                           (uint32_t)array_u32);
            }

            if (TSDB_DATA_UINT_NULL == array_u32) {
                debugPrint2("%s |", "null");
                curr_sqlstr_len += sprintf(
                    sqlstr+curr_sqlstr_len, "NULL,");
            } else {
                debugPrint2("%u | ", (uint32_t)array_u32);
                curr_sqlstr_len += sprintf(
                    sqlstr+curr_sqlstr_len, "%u,",
                    (uint32_t)array_u32);
            }
        } else {
            errorPrint("%s() LN%d mix type %s with int array",
                __func__, __LINE__,
                typeToStr(field->array_type));
        }
    }
    return curr_sqlstr_len;
}

static int32_t dumpInAvroTagUnsignedBigInt(FieldStruct *field,
                                     avro_value_t *value,
                              char *sqlstr, int32_t curr_sqlstr_len) {
    if (field->nullable) {
        avro_value_t ubigint_branch;
        avro_value_get_current_branch(value, &ubigint_branch);

        if (0 == avro_value_get_null(&ubigint_branch)) {
            debugPrint2("%s | ", "null");
            curr_sqlstr_len += sprintf(
                                 sqlstr+curr_sqlstr_len, "NULL,");
        } else {
            if (TSDB_DATA_TYPE_BIGINT == field->array_type) {
                uint64_t array_u64 = 0;
                size_t array_size = 0;
                int64_t n64tmp;
                avro_value_get_size(&ubigint_branch, &array_size);
                debugPrint("%s() LN%d, array_size: %zu\n",
                                        __func__, __LINE__, array_size);
                for (size_t item = 0; item < array_size; item++) {
                    avro_value_t item_value;
                    avro_value_get_by_index(&ubigint_branch, item,
                                            &item_value, NULL);
                    avro_value_get_long(&item_value, &n64tmp);
                    array_u64 += n64tmp;
                    debugPrint("%s() LN%d, array "
                               "index: %d, n64tmp: %"PRId64","
                               " array_u64: %"PRIu64"\n",
                               __func__, __LINE__,
                               (int)item, n64tmp,
                               (uint64_t)array_u64);
                }
                debugPrint2("%"PRIu64" | ", (uint64_t)array_u64);
                curr_sqlstr_len += sprintf(
                                     sqlstr+curr_sqlstr_len, "%"PRIu64",",
                                     (uint64_t)array_u64);
            } else {
                errorPrint("%s() LN%d mix type %s with int array",
                    __func__, __LINE__, typeToStr(field->array_type));
            }
        }
    } else {
        if (TSDB_DATA_TYPE_BIGINT == field->array_type) {
            uint64_t array_u64 = 0;
            size_t array_size = 0;
            int64_t n64tmp;
            avro_value_get_size(value, &array_size);
            debugPrint("%s() LN%d, array_size: %zu\n",
                    __func__, __LINE__, array_size);
            for (size_t item = 0; item < array_size; item++) {
                avro_value_t item_value;
                avro_value_get_by_index(value, item,
                    &item_value, NULL);
                avro_value_get_long(&item_value, &n64tmp);
                array_u64 += n64tmp;
                debugPrint("%s() LN%d, array "
                           "index: %d, n64tmp: %"PRId64","
                           " array_u64: %"PRIu64"\n",
                           __func__, __LINE__, (int)item, n64tmp,
                           (uint64_t)array_u64);
            }
            if (TSDB_DATA_UBIGINT_NULL == array_u64) {
                debugPrint2("%s |", "null");
                curr_sqlstr_len += sprintf(
                        sqlstr+curr_sqlstr_len, "NULL,");
            } else {
                debugPrint2("%"PRIu64" | ", (uint64_t)array_u64);
                curr_sqlstr_len += sprintf(
                                 sqlstr+curr_sqlstr_len, "%"PRIu64",",
                                 (uint64_t)array_u64);
            }
        } else {
            errorPrint("%s() LN%d mix type %s with int array",
                            __func__, __LINE__,
                            typeToStr(field->array_type));
        }
    }
    return curr_sqlstr_len;
}

static int32_t dumpInAvroBool(avro_value_t *value,
                              char *sqlstr, int32_t curr_sqlstr_len) {
    avro_value_t bool_branch;
    avro_value_get_current_branch(value, &bool_branch);

    if (0 == avro_value_get_null(&bool_branch)) {
        debugPrint2("%s | ", "null");
        curr_sqlstr_len += sprintf(
                         sqlstr+curr_sqlstr_len, "NULL,");
    } else {
        int32_t bl = 0;
        avro_value_get_boolean(&bool_branch, &bl);
        verbosePrint("%s() LN%d, bl=%d\n",
                              __func__, __LINE__, bl);
        debugPrint2("%s | ", (bl)?"true":"false");
        curr_sqlstr_len += sprintf(
                         sqlstr+curr_sqlstr_len, "%d,",
                         (bl)?1:0);
    }
    return curr_sqlstr_len;
}

static int64_t dumpInAvroTbTagsImpl(
        void **taos_v,
        const char *namespace,
        avro_schema_t schema,
        avro_file_reader_t reader,
        char *fileName,
        DBChange *pDbChange,
        RecordSchema *recordSchema) {
    int64_t success = 0;
    int64_t failed = 0;

    // table des
    TableDes  *tableDes   = NULL;
    TableDes  *mallocDes  = NULL;

    //
    // add stb schema changed info to dbChanged
    //
    StbChange * stbChange = NULL;
    // if recordSchema->version == 0, stbChange return NULL
    int32_t code = AddStbChanged(pDbChange, namespace, *taos_v, recordSchema, &stbChange);
    if (code) {
        return code;
    }
    // part tags
    char *partTags = "";
    if (stbChange && stbChange->strTags) {
        partTags = stbChange->strTags;
    }

    char *sqlstr = calloc(1, TSDB_MAX_ALLOWED_SQL_LEN);
    if (NULL == sqlstr) {
        errorPrint("%s() LN%d, memory allocation failed\n", __func__, __LINE__);
        return -1;
    }


    if(stbChange) {
        // use super table des
        tableDes = stbChange->tableDes;
    }

    // malloc TableDes
    if(tableDes == NULL) {
        // old data format no super table des
        mallocDes = (TableDes *)calloc(1, sizeof(TableDes) + sizeof(ColDes) * TSDB_MAX_COLUMNS);
        if (NULL == mallocDes) {
            errorPrint("%s() LN%d, mallocDes memory allocation failed!\n", __func__, __LINE__);
            free(sqlstr);
            return -1;
        }
        // set
        tableDes = mallocDes;
    }

    avro_value_iface_t *value_class = avro_generic_class_from_schema(schema);
    avro_value_t value;
    avro_generic_value_new(value_class, &value);

    int tagAdjExt = g_dumpInLooseModeFlag ? 0 : 1;

    // loop read each child table
    while (!avro_file_reader_read_value(reader, &value)) {

        //
        // combine create child table sql with stbName and tags values
        //

        char *stbName = NULL;
        char *tbName = NULL;

        int32_t  n = 0; // compatible old data
        int32_t  curr_sqlstr_len = 0;
        for (int i = 0; i < recordSchema->num_fields - tagAdjExt; i++) {
            avro_value_t field_value, field_branch;
            size_t size;

            if (0 == i) {
                // first do once
                if (!g_dumpInLooseModeFlag) {
                    // obtain stbName from avro "stbname" item
                    avro_value_get_by_name(
                            &value, "stbname",
                            &field_value, NULL);

                    avro_value_get_current_branch(
                            &field_value, &field_branch);
                    avro_value_get_string(&field_branch,
                            (const char **)&stbName, &size);
                } else {
                    // obtain stbName from filename
                    stbName = calloc(1, TSDB_TABLE_NAME_LEN);
                    TOOLS_ASSERT(stbName);

                    char *dupSeq = strdup(fileName);
                    char *running = dupSeq;
                    strsep(&running, ".");
                    char *stb = strsep(&running, ".");
                    debugPrint("%s() LN%d stable : %s parsed from file:%s\n",
                            __func__, __LINE__, stb, fileName);

                    TOOLS_STRNCPY(stbName, stb, TSDB_TABLE_NAME_LEN);
                    free(dupSeq);
                }

                if ((0 == strlen(tableDes->name))
                        || (0 != strcmp(tableDes->name, stbName))) {
                    // from server get tableDes
                    if (mallocDes) {
                        // only old data format can get des from server
                        if(getTableDes(*taos_v, namespace, stbName, mallocDes, false) < 0) {
                            if (mallocDes) {
                                freeTbDes(mallocDes, true);
                            }
                            free(sqlstr);
                            return -1;
                        }
                    }
                }

                // get tbName from avro with "tbname"
                avro_value_get_by_name(&value, "tbname", &field_value, NULL);
                avro_value_get_current_branch(
                                        &field_value, &field_branch);
                avro_value_get_string(&field_branch,
                        (const char **)&tbName, &size);

                // combine sql with stbName and tbName
                curr_sqlstr_len = snprintf(sqlstr, TSDB_MAX_ALLOWED_SQL_LEN,
                        g_args.db_escape_char
                        ? "CREATE TABLE IF NOT EXISTS `%s`.%s%s%s  USING `%s`.%s%s%s%s TAGS("
                        : "CREATE TABLE IF NOT EXISTS %s.%s%s%s    USING  %s.%s%s%s%s  TAGS(",
                        namespace, g_escapeChar, tbName, g_escapeChar,
                        namespace, g_escapeChar, stbName, g_escapeChar, partTags);

                debugPrint("%s() LN%d, pre sql: %s\n",
                        __func__, __LINE__, sqlstr);
            } else {
                // not first
                FieldStruct *field = (FieldStruct *)
                    (recordSchema->fields + sizeof(FieldStruct) * (i + tagAdjExt));

                // check filter
                int16_t idx = i - 1;
                if (stbChange && stbChange->schemaChanged) {
                    if (!idxInBindTags(idx, tableDes)) {
                        debugPrint("tag idx:%d field:%s not in server, ignore.\n", idx, field->name);
                        continue;
                    }
                }

                // get tag value
                if (0 == avro_value_get_by_name(
                            &value, field->name, &field_value, NULL)) {

                    // read value with type
                    switch (tableDes->cols[tableDes->columns + n].type) {
                        case TSDB_DATA_TYPE_BOOL:
                            curr_sqlstr_len = dumpInAvroBool(
                                         &field_value, sqlstr,
                                           curr_sqlstr_len);
                            break;
                        case TSDB_DATA_TYPE_TINYINT:
                            curr_sqlstr_len = dumpInAvroTagTinyInt(
                                         field, &field_value, sqlstr,
                                           curr_sqlstr_len);
                            break;
                        case TSDB_DATA_TYPE_SMALLINT:
                            curr_sqlstr_len = dumpInAvroTagSmallInt(
                                         field, &field_value, sqlstr,
                                           curr_sqlstr_len);
                            break;
                        case TSDB_DATA_TYPE_INT:
                            curr_sqlstr_len = dumpInAvroTagInt(
                                         field, &field_value, sqlstr,
                                           curr_sqlstr_len);
                            break;
                        case TSDB_DATA_TYPE_BIGINT:
                            curr_sqlstr_len = dumpInAvroTagBigInt(
                                         field, &field_value, sqlstr,
                                           curr_sqlstr_len);
                            break;
                        case TSDB_DATA_TYPE_FLOAT:
                            curr_sqlstr_len = dumpInAvroTagFloat(
                                         field, &field_value, sqlstr,
                                           curr_sqlstr_len);
                            break;
                        case TSDB_DATA_TYPE_DOUBLE:
                            curr_sqlstr_len = dumpInAvroTagDouble(
                                         field, &field_value, sqlstr,
                                           curr_sqlstr_len);
                            break;
                        case TSDB_DATA_TYPE_BINARY:
                            curr_sqlstr_len = dumpInAvroTagBinary(
                                         field, &field_value, sqlstr,
                                           curr_sqlstr_len);
                            break;
                        case TSDB_DATA_TYPE_NCHAR:
                        case TSDB_DATA_TYPE_JSON:
                        case TSDB_DATA_TYPE_VARBINARY:
                        case TSDB_DATA_TYPE_GEOMETRY:
                            curr_sqlstr_len = dumpInAvroTagNChar(
                                         field, &field_value, sqlstr,
                                           curr_sqlstr_len);
                            break;
                        case TSDB_DATA_TYPE_TIMESTAMP:
                            curr_sqlstr_len = dumpInAvroTagTimeStamp(
                                         field, &field_value, sqlstr,
                                           curr_sqlstr_len);
                            break;
                        case TSDB_DATA_TYPE_UTINYINT:
                            curr_sqlstr_len = dumpInAvroTagUnsignedTinyInt(
                                         field, &field_value, sqlstr,
                                           curr_sqlstr_len);
                            break;
                        case TSDB_DATA_TYPE_USMALLINT:
                            curr_sqlstr_len = dumpInAvroTagUnsignedSmallInt(
                                         field, &field_value, sqlstr,
                                           curr_sqlstr_len);
                            break;
                        case TSDB_DATA_TYPE_UINT:
                            curr_sqlstr_len = dumpInAvroTagUnsignedInt(
                                         field, &field_value, sqlstr,
                                           curr_sqlstr_len);
                            break;
                        case TSDB_DATA_TYPE_UBIGINT:
                            curr_sqlstr_len = dumpInAvroTagUnsignedBigInt(
                                         field, &field_value, sqlstr,
                                           curr_sqlstr_len);
                            break;
                        default:
                            errorPrint("%s() LN%d Unknown type: %d\n",
                                    __func__, __LINE__, field->type);
                            break;
                    }
                } else {
                    errorPrint("Failed to get value by name: %s\n",
                            field->name);
                }

                // bind tags move next
                n++;
            }

            // check curr_sqlstr_len invalid size
            if(curr_sqlstr_len > TSDB_MAX_ALLOWED_SQL_LEN - 128) {
                errorPrint("%s() LN%d, create child table combine tags sql length (%d) over %d (TSDB_MAX_ALLOWED_SQL_LEN - 128)!\n",
                   __func__, __LINE__, curr_sqlstr_len, TSDB_MAX_ALLOWED_SQL_LEN - 128);

                avro_value_decref(&value);
                avro_value_iface_decref(value_class);
                if (mallocDes) {
                    freeTbDes(mallocDes, true);
                }
                free(sqlstr);
                return -1;
            }
        }
        debugPrint2("%s", "\n");
        curr_sqlstr_len += sprintf(sqlstr + curr_sqlstr_len-1, ")");
        debugPrint("%s() LN%d, sqlstr=\n%s\n", __func__, __LINE__, sqlstr);
        freeTbNameIfLooseMode(stbName);


        //
        // exec sqlstr
        //
        TAOS_RES *res = taosQuery(*taos_v, sqlstr, &code);
        if (code != 0) {
            warnPrint("%s() LN%d taosQuery() failed! sqlstr: %s, reason: %s\n",
                    __func__, __LINE__, sqlstr, taos_errstr(res));
            failed++;
        } else {
            success++;
        }
        taos_free_result(res);
    }

    avro_value_decref(&value);
    avro_value_iface_decref(value_class);

    if (mallocDes) {
        freeTbDes(mallocDes, true);
    }
    free(sqlstr);

    if (failed)
        return -1;
    return success;
}

static int64_t dumpInAvroNtbImpl(
        void **taos_v,
        const char *namespace,
        avro_schema_t schema,
        avro_file_reader_t reader,
        DBChange* pDbChange,
        RecordSchema *recordSchema) {
    int64_t success = 0;
    int64_t failed = 0;

    //
    // Add normal table schema to hashmap
    //
    int32_t code = AddStbChanged(pDbChange, namespace, *taos_v, recordSchema, NULL);
    if (code) {
        return code;
    }

    //
    // create normal table sql
    //
    avro_value_iface_t *value_class = avro_generic_class_from_schema(schema);
    avro_value_t value;
    avro_generic_value_new(value_class, &value);

    while (!avro_file_reader_read_value(reader, &value)) {
        for (int i = 0; i < recordSchema->num_fields
                -(g_dumpInLooseModeFlag?0:1); i++) {
            avro_value_t field_value, field_branch;
            avro_value_get_by_name(&value, "sql", &field_value, NULL);

            size_t size;
            char *buf = NULL;

            avro_value_get_current_branch(
                    &field_value, &field_branch);
            avro_value_get_string(&field_branch, (const char **)&buf, &size);

            if (NULL == buf) {
                errorPrint("%s() LN%d, buf is NULL is impossible\n",
                        __func__, __LINE__);
                continue;
            }

            char* newBuf = afterRenameSql(buf);
            if(newBuf) {
                infoPrint(" rename database name for create normal table sql: \n  old=%s\n new=%s\n", buf, newBuf);
                buf = newBuf;
            }

            TAOS_RES *res = taosQuery(*taos_v, buf, &code);
            if (0 != code) {
                errorPrint("%s() LN%d,"
                        " Failed to execute taosQuery(%s)."
                        " taos: %p, code: 0x%08x, reason: %s\n",
                        __func__, __LINE__, buf,
                        *taos_v, code, taos_errstr(res));
                failed++;
            } else {
                success++;
            }
            taos_free_result(res);

            // free
            if (newBuf) {
                free(newBuf);
                newBuf = NULL;
            }
        }

    }

    avro_value_decref(&value);
    avro_value_iface_decref(value_class);

    if (failed)
        return failed;
    return success;
}

static void dumpInAvroDataUnsignedBigInt(FieldStruct *field,
                              avro_value_t *value,
                              TAOS_MULTI_BIND *bind,
                              char *is_null) {
    if (field->nullable) {
        avro_value_t ubigint_branch;
        avro_value_get_current_branch(value, &ubigint_branch);

        if (0 == avro_value_get_null(&ubigint_branch)) {
            bind->is_null = is_null;
            debugPrint2("%s | ", "null");
        } else {
            if (TSDB_DATA_TYPE_BIGINT == field->array_type) {
                uint64_t *array_u64 = malloc(sizeof(uint64_t));
                TOOLS_ASSERT(array_u64);
                *array_u64 = 0;

                size_t array_size = 0;
                int64_t n64tmp;
                avro_value_get_size(&ubigint_branch, &array_size);

                debugPrint("%s() LN%d, array_size: %zu\n",
                    __func__, __LINE__, array_size);
                for (size_t item = 0; item < array_size; item++) {
                    avro_value_t item_value;
                    avro_value_get_by_index(&ubigint_branch, item,
                        &item_value, NULL);
                    avro_value_get_long(&item_value, &n64tmp);
                    *array_u64 += n64tmp;
                }
                debugPrint2("%"PRIu64" | ", *array_u64);
                bind->buffer_length = sizeof(uint64_t);
                bind->buffer = array_u64;
            } else {
                errorPrint("%s() LN%d mix type %s with int array",
                    __func__, __LINE__, typeToStr(field->array_type));
            }
        }
    } else {
        if (TSDB_DATA_TYPE_BIGINT == field->array_type) {
            uint64_t *array_u64 = malloc(sizeof(uint64_t));
            TOOLS_ASSERT(array_u64);
            *array_u64 = 0;

            size_t array_size = 0;
            int64_t n64tmp;
            avro_value_get_size(value, &array_size);

            debugPrint("%s() LN%d, array_size: %zu\n",
                            __func__, __LINE__, array_size);
            for (size_t item = 0; item < array_size; item++) {
                avro_value_t item_value;
                avro_value_get_by_index(value, item,
                        &item_value, NULL);
                avro_value_get_long(&item_value, &n64tmp);
                *array_u64 += n64tmp;
            }
            debugPrint2("%"PRIu64" | ", *array_u64);
            bind->buffer_length = sizeof(uint64_t);
            bind->buffer = array_u64;
        } else {
            errorPrint("%s() LN%d mix type %s with int array",
                    __func__, __LINE__, typeToStr(field->array_type));
        }
    }
}

static void dumpInAvroDataUnsignedSmallInt(FieldStruct *field,
                              avro_value_t *value,
                              TAOS_MULTI_BIND *bind,
                              char *is_null) {
    if (field->nullable) {
        avro_value_t usmint_branch;
        avro_value_get_current_branch(value, &usmint_branch);

        if (0 == avro_value_get_null(&usmint_branch)) {
            bind->is_null = is_null;
            debugPrint2("%s | ", "null");
        } else {
            if (TSDB_DATA_TYPE_INT == field->array_type) {
                uint16_t *array_u16 = malloc(sizeof(uint16_t));
                TOOLS_ASSERT(array_u16);
                *array_u16 = 0;

                size_t array_size = 0;
                avro_value_get_size(&usmint_branch, &array_size);

                debugPrint("%s() LN%d, array_size: %zu\n",
                        __func__, __LINE__, array_size);
                for (size_t item = 0; item < array_size; item++) {
                    int32_t n32tmp = 0;
                    avro_value_t item_value;
                    avro_value_get_by_index(&usmint_branch, item,
                            &item_value, NULL);
                    avro_value_get_int(&item_value, &n32tmp);
                    *array_u16 += (int16_t)n32tmp;
                }
                debugPrint2("%u | ", (uint32_t)*array_u16);
                bind->buffer_length = sizeof(uint16_t);
                bind->buffer = array_u16;
            } else {
                errorPrint("%s() LN%d mix type %s with int array",
                        __func__, __LINE__, typeToStr(field->array_type));
            }
        }
    } else {
        if (TSDB_DATA_TYPE_INT == field->array_type) {
            uint16_t *array_u16 = malloc(sizeof(uint16_t));
            TOOLS_ASSERT(array_u16);
            *array_u16 = 0;

            size_t array_size = 0;
            avro_value_get_size(value, &array_size);

            debugPrint("%s() LN%d, array_size: %zu\n",
                    __func__, __LINE__, array_size);
            for (size_t item = 0; item < array_size; item++) {
                int32_t n32tmp = 0;
                avro_value_t item_value;
                avro_value_get_by_index(value, item, &item_value, NULL);
                avro_value_get_int(&item_value, &n32tmp);
                *array_u16 += (int16_t)n32tmp;
            }
            debugPrint2("%u | ", (uint32_t)*array_u16);
            bind->buffer_length = sizeof(uint16_t);
            bind->buffer = array_u16;
        } else {
            errorPrint("%s() LN%d mix type %s with int array",
                    __func__, __LINE__,
            typeToStr(field->array_type));
        }
    }
}

static void dumpInAvroDataUnsignedTinyInt(FieldStruct *field,
                              avro_value_t *value,
                              TAOS_MULTI_BIND *bind,
                              char *is_null) {
    if (field->nullable) {
        avro_value_t utinyint_branch;
        avro_value_get_current_branch(value, &utinyint_branch);

        if (0 == avro_value_get_null(&utinyint_branch)) {
            bind->is_null = is_null;
            debugPrint2("%s | ", "null");
        } else {
            if (TSDB_DATA_TYPE_INT == field->array_type) {
                uint8_t *array_u8 = malloc(sizeof(uint8_t));
                TOOLS_ASSERT(array_u8);
                *array_u8 = 0;

                size_t array_size = 0;
                avro_value_get_size(&utinyint_branch, &array_size);

                debugPrint("%s() LN%d, array_size: %zu\n",
                                            __func__, __LINE__, array_size);
                for (size_t item = 0; item < array_size; item++) {
                    int32_t n32tmp = 0;
                    avro_value_t item_value;
                    avro_value_get_by_index(&utinyint_branch, item,
                            &item_value, NULL);
                    avro_value_get_int(&item_value, &n32tmp);
                    *array_u8 += (int8_t)n32tmp;
                }
                debugPrint2("%u | ", (uint32_t)*array_u8);
                bind->buffer_length = sizeof(uint8_t);
                bind->buffer = array_u8;
            } else {
                errorPrint("%s() LN%d mix type %s with int array",
                        __func__, __LINE__,
                        typeToStr(field->array_type));
            }
        }
    } else {
        if (TSDB_DATA_TYPE_INT == field->array_type) {
            uint8_t *array_u8 = malloc(sizeof(uint8_t));
            TOOLS_ASSERT(array_u8);
            *array_u8 = 0;

            size_t array_size = 0;
            avro_value_get_size(value, &array_size);

            debugPrint("%s() LN%d, array_size: %zu\n",
                    __func__, __LINE__, array_size);
            for (size_t item = 0; item < array_size; item++) {
                int32_t n32tmp = 0;
                avro_value_t item_value;
                avro_value_get_by_index(value, item,
                        &item_value, NULL);
                avro_value_get_int(&item_value, &n32tmp);
                *array_u8 += (int8_t)n32tmp;
            }
            debugPrint2("%u | ", (uint32_t)*array_u8);
            bind->buffer_length = sizeof(uint8_t);
            bind->buffer = array_u8;
        } else {
            errorPrint("%s() LN%d mix type %s with int array",
                    __func__, __LINE__, typeToStr(field->array_type));
        }
    }
}

static void dumpInAvroDataUnsignedInt(FieldStruct *field,
                              avro_value_t *value,
                              TAOS_MULTI_BIND *bind,
                              char *is_null) {
    if (field->nullable) {
        avro_value_t uint_branch;
        avro_value_get_current_branch(value, &uint_branch);

        if (0 == avro_value_get_null(&uint_branch)) {
            bind->is_null = is_null;
            debugPrint2("%s | ", "null");
        } else {
            if (TSDB_DATA_TYPE_INT == field->array_type) {
                uint32_t *array_u32 = malloc(sizeof(uint32_t));
                TOOLS_ASSERT(array_u32);
                *array_u32 = 0;

                size_t array_size = 0;
                avro_value_get_size(&uint_branch, &array_size);

                debugPrint("%s() LN%d, array_size: %zu\n",
                    __func__, __LINE__, array_size);
                for (size_t item = 0; item < array_size; item++) {
                    int32_t n32tmp = 0;
                    avro_value_t item_value;
                    avro_value_get_by_index(&uint_branch, item,
                        &item_value, NULL);
                    avro_value_get_int(&item_value, &n32tmp);
                    *array_u32 += n32tmp;
                    debugPrint("%s() LN%d, array index: %d, n32tmp: %d, "
                               "array_u32: %u\n",
                               __func__, __LINE__, (int)item, n32tmp,
                               (uint32_t)*array_u32);
                }
                debugPrint2("%u | ", (uint32_t)*array_u32);
                bind->buffer_length = sizeof(uint32_t);
                bind->buffer = array_u32;
            } else {
                errorPrint("%s() LN%d mix type %s with int array",
                        __func__, __LINE__,
                        typeToStr(field->array_type));
            }
        }
    } else {
        if (TSDB_DATA_TYPE_INT == field->array_type) {
            uint32_t *array_u32 = malloc(sizeof(uint32_t));
            TOOLS_ASSERT(array_u32);
            *array_u32 = 0;

            size_t array_size = 0;
            avro_value_get_size(value, &array_size);

            debugPrint("%s() LN%d, array_size: %zu\n",
                    __func__, __LINE__, array_size);
            for (size_t item = 0; item < array_size; item++) {
                int32_t n32tmp = 0;
                avro_value_t item_value;
                avro_value_get_by_index(value, item,
                        &item_value, NULL);
                avro_value_get_int(&item_value, &n32tmp);
                *array_u32 += n32tmp;
                debugPrint("%s() LN%d, array index: %d, n32tmp: %d, array_u32: %u\n",
                    __func__, __LINE__, (int)item, n32tmp, (uint32_t)*array_u32);
            }
            debugPrint2("%u | ", (uint32_t)*array_u32);
            bind->buffer_length = sizeof(uint32_t);
            bind->buffer = array_u32;
        } else {
            errorPrint("%s() LN%d mix type %s with int array",
                __func__, __LINE__, typeToStr(field->array_type));
        }
    }
}

static void dumpInAvroDataBool(FieldStruct *field,
                              avro_value_t *value,
                              TAOS_MULTI_BIND *bind,
                              char *is_null) {
    avro_value_t bool_branch;
    avro_value_get_current_branch(value, &bool_branch);
    if (0 == avro_value_get_null(&bool_branch)) {
        bind->is_null = is_null;
        debugPrint2("%s | ", "null");
    } else {
        int32_t *bl = malloc(sizeof(int32_t));
        TOOLS_ASSERT(bl);
        avro_value_get_boolean(&bool_branch, bl);
        verbosePrint("%s() LN%d, *bl=%d\n",
            __func__, __LINE__, *bl);
        debugPrint2("%s | ", (*bl)?"true":"false");
            bind->buffer_length = sizeof(int8_t);
            bind->buffer = (int8_t*)bl;
    }
}

static void dumpInAvroDataNChar(FieldStruct *field,
                              avro_value_t *value,
                              TAOS_MULTI_BIND *bind,
                              char *is_null) {
    size_t bytessize = 0;
    void *bytesbuf = NULL;

    avro_value_t nchar_branch;
    avro_value_get_current_branch(value, &nchar_branch);

    avro_value_get_bytes(&nchar_branch,
        (const void **)&bytesbuf, &bytessize);
    if (NULL == bytesbuf) {
        debugPrint2("%s | ", "NULL");
        bind->is_null = is_null;
    } else {
        debugPrint2("%s | ", (char*)bytesbuf);
        bind->buffer_length = bytessize;
    }
    bind->buffer = bytesbuf;
}

static void dumpInAvroDataBytes(FieldStruct *field,
                              avro_value_t *value,
                              TAOS_MULTI_BIND *bind,
                              char *is_null) {
    size_t bytessize = 0;
    void *bytesbuf = NULL;

    avro_value_t branch;
    avro_value_get_current_branch(value, &branch);

    avro_value_get_bytes(&branch, (const void **)&bytesbuf, &bytessize);
    if (NULL == bytesbuf) {
        debugPrint2("%s | ", "NULL");
        bind->is_null = is_null;
    } else {
        debugPrint2("bytes len =%ld | ", bytessize);
        bind->buffer_length = bytessize;
    }
    bind->buffer = bytesbuf;
}

static void dumpInAvroDataBinary(FieldStruct *field,
                              avro_value_t *value,
                              TAOS_MULTI_BIND *bind,
                              char *is_null) {
    avro_value_t branch;
    avro_value_get_current_branch(value, &branch);

    char *buf = NULL;
    size_t size;
    avro_value_get_string(&branch, (const char **)&buf, &size);

    if (NULL == buf || size == 0) {
        debugPrint2("%s | ", "NULL");
        bind->is_null = is_null;
    } else {
        debugPrint2("%s | ", (char *)buf);
        bind->buffer_length = strlen(buf);
    }
    bind->buffer = buf;
}

static void dumpInAvroDataDouble(FieldStruct *field,
                              avro_value_t *value,
                              TAOS_MULTI_BIND *bind,
                              char *is_null) {
    if (field->nullable) {
        avro_value_t dbl_branch;
        avro_value_get_current_branch(value, &dbl_branch);
        if (0 == avro_value_get_null(&dbl_branch)) {
            debugPrint2("%s | ", "NULL");
            bind->is_null = is_null;
        } else {
            double *dbl = malloc(sizeof(double));
            TOOLS_ASSERT(dbl);
            avro_value_get_double(&dbl_branch, dbl);
            debugPrint2("%f | ", *dbl);
            bind->buffer = dbl;
            bind->buffer_length = sizeof(double);
        }
    } else {
        double *dbl = malloc(sizeof(double));
        TOOLS_ASSERT(dbl);
        avro_value_get_double(value, dbl);
        if (TSDB_DATA_DOUBLE_NULL == *dbl) {
            debugPrint2("%s | ", "NULL");
            bind->is_null = is_null;
        } else {
            debugPrint2("%f | ", *dbl);
        }
        bind->buffer = dbl;
        bind->buffer_length = sizeof(double);
    }
}

static void dumpInAvroDataFloat(FieldStruct *field,
                              avro_value_t *value,
                              TAOS_MULTI_BIND *bind,
                              char *is_null) {
    if (field->nullable) {
        avro_value_t float_branch;
        avro_value_get_current_branch(value, &float_branch);
        if (0 == avro_value_get_null(&float_branch)) {
            debugPrint2("%s | ", "NULL");
            bind->is_null = is_null;
        } else {
            float *f = malloc(sizeof(float));
            TOOLS_ASSERT(f);
            avro_value_get_float(&float_branch, f);
            debugPrint2("%f | ", *f);
            bind->buffer = f;
            bind->buffer_length = sizeof(float);
        }
    } else {
        float *f = malloc(sizeof(float));
        TOOLS_ASSERT(f);
        avro_value_get_float(value, f);
        if (TSDB_DATA_FLOAT_NULL == *f) {
            debugPrint2("%s | ", "NULL");
            bind->is_null = is_null;
        } else {
            debugPrint2("%f | ", *f);
        }
        bind->buffer = f;
        bind->buffer_length = sizeof(float);
    }
}

static void dumpInAvroDataTimeStamp(FieldStruct *field,
                              avro_value_t *value,
                              TAOS_MULTI_BIND *bind,
                              char *is_null) {
    if (field->nullable) {
        avro_value_t ts_branch;
        avro_value_get_current_branch(value, &ts_branch);
        if (0 == avro_value_get_null(&ts_branch)) {
            bind->is_null = is_null;
            debugPrint2("%s | ", "null");
        } else {
            int64_t *n64 = malloc(sizeof(int64_t));
            TOOLS_ASSERT(n64);
            avro_value_get_long(&ts_branch, n64);
            debugPrint2("%"PRId64" | ", *n64);
            bind->buffer_length = sizeof(int64_t);
            bind->buffer = n64;
        }
    } else {
        int64_t *n64 = malloc(sizeof(int64_t));
        TOOLS_ASSERT(n64);
        avro_value_get_long(value, n64);
        debugPrint2("%"PRId64" | ", *n64);
        bind->buffer_length = sizeof(int64_t);
        bind->buffer = n64;
    }
}

static void dumpInAvroDataBigInt(FieldStruct *field,
                              avro_value_t *value,
                              TAOS_MULTI_BIND *bind,
                              char *is_null) {
    if (field->nullable) {
        avro_value_t bigint_branch;
        avro_value_get_current_branch(value, &bigint_branch);
        if (0 == avro_value_get_null(&bigint_branch)) {
            bind->is_null = is_null;
            debugPrint2("%s | ", "null");
        } else {
            int64_t *n64 = malloc(sizeof(int64_t));
            TOOLS_ASSERT(n64);
            avro_value_get_long(&bigint_branch, n64);
            verbosePrint("%s() LN%d: *n64=%"PRId64" null=%"PRId64"\n",
                                  __func__, __LINE__, *n64,
                                  (int64_t)TSDB_DATA_BIGINT_NULL);
            debugPrint2("%"PRId64" | ", *n64);
            bind->buffer_length = sizeof(int64_t);
            bind->buffer = n64;
        }
    } else {
        int64_t *n64 = malloc(sizeof(int64_t));
        TOOLS_ASSERT(n64);
        avro_value_get_long(value, n64);
        verbosePrint("%s() LN%d: *n64=%"PRId64" null=%"PRId64"\n",
                              __func__, __LINE__, *n64,
                              (int64_t)TSDB_DATA_BIGINT_NULL);
        if ((int64_t)TSDB_DATA_BIGINT_NULL == *n64) {
            debugPrint2("%s | ", "null");
            bind->is_null = is_null;
            free(n64);
        } else {
            debugPrint2("%"PRId64" | ", *n64);
            bind->buffer_length = sizeof(int64_t);
            bind->buffer = n64;
        }
    }
}

static void dumpInAvroDataSmallInt(FieldStruct *field,
                              avro_value_t *value,
                              TAOS_MULTI_BIND *bind,
                              char *is_null) {
    if (field->nullable) {
        avro_value_t smallint_branch;
        avro_value_get_current_branch(value, &smallint_branch);
        if (0 == avro_value_get_null(&smallint_branch)) {
            bind->is_null = is_null;
            debugPrint2("%s | ", "null");
        } else {
            int32_t *n16 = malloc(sizeof(int32_t));
            TOOLS_ASSERT(n16);

            avro_value_get_int(&smallint_branch, n16);

            debugPrint2("%d | ", *n16);
            bind->buffer_length = sizeof(int16_t);
            bind->buffer = n16;
        }
    } else {
        int32_t *n16 = malloc(sizeof(int32_t));
        TOOLS_ASSERT(n16);

        avro_value_get_int(value, n16);
        verbosePrint("%s() LN%d: *n16=%d null=%d\n",
                              __func__, __LINE__, *n16,
                              (int16_t)TSDB_DATA_SMALLINT_NULL);

        if ((int16_t)TSDB_DATA_SMALLINT_NULL == *n16) {
            debugPrint2("%s | ", "null");
            bind->is_null = is_null;
            free(n16);
        } else {
            debugPrint2("%d | ", *n16);
            bind->buffer_length = sizeof(int16_t);
            bind->buffer = (int32_t*)n16;
        }
    }
}

static void dumpInAvroDataTinyInt(FieldStruct *field,
                              avro_value_t *value,
                              TAOS_MULTI_BIND *bind,
                              char *is_null) {
    if (field->nullable) {
        avro_value_t tinyint_branch;
        avro_value_get_current_branch(value, &tinyint_branch);
        if (0 == avro_value_get_null(&tinyint_branch)) {
            bind->is_null = is_null;
            debugPrint2("%s | ", "null");
        } else {
            int32_t *n8 = malloc(sizeof(int32_t));
            TOOLS_ASSERT(n8);

            avro_value_get_int(&tinyint_branch, n8);

            debugPrint2("%d | ", *n8);
            bind->buffer_length = sizeof(int8_t);
            bind->buffer = n8;
        }
    } else {
        int32_t *n8 = malloc(sizeof(int32_t));
        TOOLS_ASSERT(n8);

        avro_value_get_int(value, n8);

        verbosePrint("%s() LN%d: *n8=%d null=%d\n",
                              __func__, __LINE__, *n8,
                              (int8_t)TSDB_DATA_TINYINT_NULL);

        if ((int8_t)TSDB_DATA_TINYINT_NULL == *n8) {
            debugPrint2("%s | ", "null");
            bind->is_null = is_null;
            free(n8);
        } else {
            debugPrint2("%d | ", *n8);
            bind->buffer_length = sizeof(int8_t);
            bind->buffer = (int8_t *)n8;
        }
    }
}

static void dumpInAvroDataInt(FieldStruct *field,
                              avro_value_t *value,
                              TAOS_MULTI_BIND *bind,
                              char *is_null) {
    if (field->nullable) {
        avro_value_t int_branch;
        avro_value_get_current_branch(value, &int_branch);
        if (0 == avro_value_get_null(&int_branch)) {
            bind->is_null = is_null;
            debugPrint2("%s | ", "null");
        } else {
            int32_t *n32 = malloc(sizeof(int32_t));
            TOOLS_ASSERT(n32);

            avro_value_get_int(&int_branch, n32);

            if ((int32_t)TSDB_DATA_INT_NULL == *n32) {
                debugPrint2("%s | ", "null");
                bind->is_null = is_null;
                free(n32);
            } else {
                debugPrint2("%d | ", *n32);
                bind->buffer_length = sizeof(int32_t);
                bind->buffer = n32;
            }
        }
    } else {
        int32_t *n32 = malloc(sizeof(int32_t));
        TOOLS_ASSERT(n32);

        avro_value_get_int(value, n32);
        if ((int32_t)TSDB_DATA_INT_NULL == *n32) {
            debugPrint2("%s | ", "null");
            bind->is_null = is_null;
            free(n32);
        } else {
            debugPrint2("%d | ", *n32);
            bind->buffer_length = sizeof(int32_t);
            bind->buffer = n32;
        }
    }
}

static void countFailureAndFree(char *bindArray,
        int32_t onlyCol, int64_t *failed, char *tbName) {
    freeBindArray(bindArray, onlyCol);
    (*failed)++;
    freeTbNameIfLooseMode(tbName);
}

// stmt prepare
static int32_t stmtPrepare(TAOS_STMT *stmt, char *tbName, StbChange *stbChange, RecordSchema *recordSchema, int32_t nBindCols) {
    // part cols
    char *partCols = "";
    if (stbChange && stbChange->strCols) {
        partCols = stbChange->strCols;
    }

    char *stmtBuffer = calloc(1, TSDB_MAX_ALLOWED_SQL_LEN);
    if (NULL == stmtBuffer) {
        errorPrint("%s() LN%d, memory allocation failed!\n", __func__, __LINE__);
        return -1;
    }

    char *pstr = stmtBuffer;
    pstr += snprintf(pstr, TSDB_MAX_ALLOWED_SQL_LEN, "INSERT INTO %s %s VALUES(?", tbName, partCols);

    for (int i = 1; i < nBindCols; i++) {
        pstr += sprintf(pstr, ",?");
    }
    pstr += sprintf(pstr, ")");
    debugPrint("%s() LN%d, stmt buffer: %s\n",  __func__, __LINE__, stmtBuffer);

    // prepare
    int ret = taos_stmt_prepare(stmt, stmtBuffer, 0);
    if (ret) {
        errorPrint("Failed to execute taos_stmt_prepare(). reason: %s\n", taos_stmt_errstr(stmt));
    }

    free(stmtBuffer);
    return ret;
}

#define FREE_DATAIMPL()                       \
    {                                         \
        tfree(bindArray);                     \
        tfree(tbName);                        \
        if (mallocDes) {                      \
            freeTbDes(mallocDes, true);       \
        }                                     \
        taos_stmt_close(stmt);                \
        avro_value_decref(&value);            \
        avro_value_iface_decref(value_class); \
    }

// dump child table data
static int64_t dumpInAvroDataImpl(
        void **taos_v,
        char *namespace,
        avro_schema_t schema,
        avro_file_reader_t reader,
        RecordSchema *recordSchema,
        DBChange     *pDbChange,
        StbChange    *stbChange,
        const char   *dbPath,
        char *fileName) {

    // init stmt
    int32_t    code = 0;
    TAOS_STMT *stmt = NULL;
    stmt = taos_stmt_init(*taos_v);
    if (NULL == stmt) {
        errorPrint("%s() LN%d, stmt init failed! taos: %p, code: 0x%08x, "
                "reason: %s\n",
                __func__, __LINE__, *taos_v,
                taos_errno(NULL), taos_errstr(NULL));

        return -1;
    }

    // table des
    TableDes  *tableDes   = NULL;
    TableDes  *mallocDes  = NULL;

    if(stbChange) {
        // use super table des
        tableDes = stbChange->tableDes;
    }

    // calc bind cols count
    int32_t colAdj    = g_dumpInLooseModeFlag ? 0 : 1;
    int32_t nBindCols = recordSchema->num_fields - colAdj;
    if (stbChange && stbChange->schemaChanged) {
        // need part columns bind
        infoPrint("Full fields:%d bind part fields:%d stb:%s\n", nBindCols, stbChange->tableDes->columns, stbChange->tableDes->name);
        nBindCols = stbChange->tableDes->columns;
    }

    char *bindArray = calloc(1, sizeof(TAOS_MULTI_BIND) * nBindCols);
    if (NULL == bindArray) {
        errorPrint("%s() LN%d, memory allocation failed!\n", __func__, __LINE__);
        taos_stmt_close(stmt);
        return -1;
    }

    avro_value_iface_t *value_class = avro_generic_class_from_schema(schema);
    avro_value_t value;
    avro_generic_value_new(value_class, &value);

    int64_t success = 0;
    int64_t failed = 0;
    int64_t count = 0;
    int64_t countTSOutOfRange = 0;
    char    *tbName = NULL;


    bool printDot = true;
    while (!avro_file_reader_read_value(reader, &value)) {
        //
        // get tbName from avro
        //
        if(tbName == NULL) {
            // get tb name
            avro_value_t tbname_value, tbname_branch;
            if (!g_dumpInLooseModeFlag) {
                avro_value_get_by_name(&value, "tbname", &tbname_value, NULL);
                avro_value_get_current_branch(&tbname_value, &tbname_branch);

                size_t tbname_size;
                char * avroName = NULL;
                avro_value_get_string(&tbname_branch,
                        (const char **)&avroName, &tbname_size);
                tbName = strdup(avroName);
            } else {
                tbName = malloc(TSDB_TABLE_NAME_LEN+1);
                TOOLS_ASSERT(tbName);

                char *dupSeq = strdup(fileName);
                char *running = dupSeq;
                strsep(&running, ".");
                char *tb = strsep(&running, ".");

                strcpy(tbName, tb);
                free(dupSeq);
            }
            debugPrint("%s() LN%d table: %s parsed from file:%s\n",
                    __func__, __LINE__, tbName, fileName);


            // read normal table stbChange
            if (stbChange == NULL) {
                if (normalTableFolder(dbPath)) {
                    stbChange = findStbChange(pDbChange, tbName);
                    if (stbChange) {
                        // bind
                        nBindCols = stbChange->tableDes->columns;
                        // tableDes
                        tableDes = stbChange->tableDes;
                    }
                }
            }

            // malloc table des
            if(tableDes == NULL) {
                // old data format no super table des
                mallocDes = (TableDes *)calloc(1, sizeof(TableDes) + sizeof(ColDes) * TSDB_MAX_COLUMNS);
                if (NULL == mallocDes) {
                    errorPrint("%s() LN%d, mallocDes memory allocation failed!\n", __func__, __LINE__);
                    FREE_DATAIMPL();
                    return -1;
                }
                // set
                tableDes = mallocDes;
            }


            // escapedTbName
            const int escapedTbNameLen = TSDB_DB_NAME_LEN + TSDB_TABLE_NAME_LEN + 10;
            char *escapedTbName = calloc(1, escapedTbNameLen);
            if (NULL == escapedTbName) {
                errorPrint("%s() LN%d, memory allocation failed!\n", __func__, __LINE__);
                FREE_DATAIMPL();
                return -1;
            }
            snprintf(escapedTbName, escapedTbNameLen, "%s%s%s.%s%s%s",
                g_escapeChar, namespace, g_escapeChar,
                g_escapeChar, tbName,    g_escapeChar);

            debugPrint("%s() LN%d escaped table: %s\n",
                    __func__, __LINE__, escapedTbName);

            // prepare
            if (stmtPrepare(stmt, escapedTbName, stbChange, recordSchema, nBindCols)) {
                // failed
                tfree(escapedTbName);
                FREE_DATAIMPL();
                return -1;
            }
            free(escapedTbName);
            escapedTbName = NULL;

            // get table des
            if ((0 == strlen(tableDes->name))
                    || (0 != strcmp(tableDes->name, tbName))) {

                if (mallocDes) {
                    // only old data format can get des from server
                    if (getTableDes(*taos_v, namespace, tbName, mallocDes, true) < 0) {
                        FREE_DATAIMPL();
                        return -1;
                    }
                }
            }
        } // tbName

        debugPrint("%s() LN%d, count: %"PRId64"\n",
                    __func__, __LINE__, count);
        printDotOrX(count, &printDot);
        count++;

        TAOS_MULTI_BIND *bind;

        char is_null = 1;
        int64_t ts_debug = -1;
        int32_t n = 0; // tableDes index

        // cols loop
        for (int i = 0; i < recordSchema->num_fields - colAdj; i++) {

            bind = (TAOS_MULTI_BIND *)((char *)bindArray + (sizeof(TAOS_MULTI_BIND) * n));
            avro_value_t field_value;

            FieldStruct *field = (FieldStruct *)(recordSchema->fields + sizeof(FieldStruct)*(i + colAdj));

            //
            //  check  avro fields need filter
            //
            if (stbChange && stbChange->schemaChanged) {
                if(!idxInBindCols(i, stbChange->tableDes)) {
                    // remove col not exist on server
                    debugPrint("col idx:%d field:%s not in server, ignore.\n", i, field->name);
                    continue;
                } else {
                    debugPrint("col idx:%d field:%s in server.\n", i, field->name);
                }
            }

            bind->is_null = NULL;
            bind->num = 1;
            if (0 == i) {
                avro_value_get_by_name(&value,
                        field->name, &field_value, NULL);
                if (field->nullable) {
                    avro_value_t ts_branch;
                    avro_value_get_current_branch(&field_value, &ts_branch);
                    if (0 == avro_value_get_null(&ts_branch)) {
                        errorPrint("%s() LN%d, first column timestamp "
                                "should never be a NULL!\n",
                                __func__, __LINE__);
                    } else {
                        int64_t *ts = malloc(sizeof(int64_t));
                        TOOLS_ASSERT(ts);

                        avro_value_get_long(&ts_branch, ts);

                        ts_debug = *ts;
                        debugPrint2("%"PRId64" | ", *ts);
                        bind->buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
                        bind->buffer_length = sizeof(int64_t);
                        bind->buffer = ts;
                        bind->length = (int32_t*)&bind->buffer_length;
                    }
                } else {
                    int64_t *ts = malloc(sizeof(int64_t));
                    TOOLS_ASSERT(ts);

                    avro_value_get_long(&field_value, ts);

                    ts_debug = *ts;
                    debugPrint2("%"PRId64" | ", *ts);
                    bind->buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
                    bind->buffer_length = sizeof(int64_t);
                    bind->buffer = ts;
                }
            } else if (0 == avro_value_get_by_name(
                        &value, field->name, &field_value, NULL)) {

                // switch type read col value
                switch (tableDes->cols[n].type) {
                    case TSDB_DATA_TYPE_INT:
                        if (field->type != TSDB_DATA_TYPE_INT) {
                            warnPrint("field[%d] type is not int!\n", i);
                            bind->is_null = &is_null;
                        } else {
                            dumpInAvroDataInt(field, &field_value,
                                    bind, &is_null);
                        }
                        break;
                    case TSDB_DATA_TYPE_TINYINT:
                        if (field->type != TSDB_DATA_TYPE_INT) {
                            warnPrint("field[%d] type is not tinyint!\n", i);
                            bind->is_null = &is_null;
                        } else {
                            dumpInAvroDataTinyInt(field, &field_value,
                                    bind, &is_null);
                        }
                        break;
                    case TSDB_DATA_TYPE_SMALLINT:
                        if (field->type != TSDB_DATA_TYPE_INT) {
                            warnPrint("field[%d] type is not smallint!\n", i);
                            bind->is_null = &is_null;
                        } else {
                            dumpInAvroDataSmallInt(field, &field_value,
                                    bind, &is_null);
                        }
                        break;
                    case TSDB_DATA_TYPE_BIGINT:
                        if (field->type != TSDB_DATA_TYPE_BIGINT) {
                            warnPrint("field[%d] type is not bigint!\n", i);
                            bind->is_null = &is_null;
                        } else {
                            dumpInAvroDataBigInt(field, &field_value,
                                    bind, &is_null);
                        }
                        break;
                    case TSDB_DATA_TYPE_TIMESTAMP:
                        if (field->type != TSDB_DATA_TYPE_BIGINT) {
                            warnPrint("field[%d] type is not timestamp!\n", i);
                            bind->is_null = &is_null;
                        } else {
                            dumpInAvroDataTimeStamp(field, &field_value,
                                    bind, &is_null);
                        }
                        break;
                    case TSDB_DATA_TYPE_FLOAT:
                        if (field->type != TSDB_DATA_TYPE_FLOAT) {
                            warnPrint("field[%d] type is not float!\n", i);
                            bind->is_null = &is_null;
                        } else {
                            dumpInAvroDataFloat(field, &field_value,
                                    bind, &is_null);
                        }
                        break;
                    case TSDB_DATA_TYPE_DOUBLE:
                        if (field->type != TSDB_DATA_TYPE_DOUBLE) {
                            warnPrint("field[%d] type is not double!\n", i);
                            bind->is_null = &is_null;
                        } else {
                            dumpInAvroDataDouble(field, &field_value,
                                    bind, &is_null);
                        }
                        break;
                    case TSDB_DATA_TYPE_BINARY:
                        if (field->type != TSDB_DATA_TYPE_BINARY) {
                            warnPrint("field[%d] type is not binary!\n", i);
                            bind->is_null = &is_null;
                        } else {
                            dumpInAvroDataBinary(field, &field_value,
                                    bind, &is_null);
                        }
                        break;
                    case TSDB_DATA_TYPE_JSON:
                    case TSDB_DATA_TYPE_NCHAR:
                        // RecordSchema bytes only covert to nchar type
                        if (field->type == TSDB_DATA_TYPE_NCHAR) {
                            dumpInAvroDataNChar(field, &field_value, bind, &is_null);
                        } else {
                            warnPrint("field[%d] type is not nchar/json! field->type=%d\n", i, field->type);
                            bind->is_null = &is_null;
                        }
                        break;
                    case TSDB_DATA_TYPE_VARBINARY:
                    case TSDB_DATA_TYPE_GEOMETRY:
                        // RecordSchema bytes only covert to nchar type
                        if (field->type == TSDB_DATA_TYPE_NCHAR) {
                            dumpInAvroDataBytes(field, &field_value, bind, &is_null);
                        } else {
                            warnPrint("field[%d] type is not varbinary/geometry! field->type=%d\n", i, field->type);
                            bind->is_null = &is_null;
                        }
                        break;
                    case TSDB_DATA_TYPE_BOOL:
                        if (field->type != TSDB_DATA_TYPE_BOOL) {
                            warnPrint("field[%d] type is not bool!\n", i);
                            bind->is_null = &is_null;
                        } else {
                            dumpInAvroDataBool(field, &field_value,
                                    bind, &is_null);
                        }
                        break;
                    case TSDB_DATA_TYPE_UINT:
                        if (!field->is_array
                                || field->array_type != TSDB_DATA_TYPE_INT) {
                            warnPrint("field[%d] type is not uint!\n", i);
                            bind->is_null = &is_null;
                        } else {
                            dumpInAvroDataUnsignedInt(field, &field_value,
                                    bind, &is_null);
                        }
                        break;
                    case TSDB_DATA_TYPE_UTINYINT:
                        if (!field->is_array
                                || field->array_type != TSDB_DATA_TYPE_INT) {
                            warnPrint("field[%d] type is not utinyint!\n", i);
                            bind->is_null = &is_null;
                        } else {
                            dumpInAvroDataUnsignedTinyInt(field, &field_value,
                                    bind, &is_null);
                        }
                        break;
                    case TSDB_DATA_TYPE_USMALLINT:
                        if (!field->is_array ||
                                field->array_type != TSDB_DATA_TYPE_INT) {
                            warnPrint("field[%d] type is not usmallint!\n", i);
                            bind->is_null = &is_null;
                        } else {
                            dumpInAvroDataUnsignedSmallInt(field, &field_value,
                                    bind, &is_null);
                        }
                        break;
                    case TSDB_DATA_TYPE_UBIGINT:
                        if (!field->is_array
                                || field->array_type != TSDB_DATA_TYPE_BIGINT) {
                            warnPrint("field[%d] type is not ubigint!\n", i);
                            bind->is_null = &is_null;
                        } else {
                            dumpInAvroDataUnsignedBigInt(field, &field_value,
                                    bind, &is_null);
                        }
                        break;
                    default:
                        errorPrint("%s() LN%d, %s's %s is not supported!\n",
                                __func__, __LINE__,
                                tbName,
                                typeToStr(field->type));
                        break;
                }
                bind->buffer_type = tableDes->cols[n].type;
                bind->length = (int32_t *)&bind->buffer_length;
            }
            bind->num = 1;

            // tableDes index
            n ++;
        } // cols loop end
        debugPrint2("%s", "\n");
        assert (n == nBindCols);

        // bind batch
        if (0 != (code = taos_stmt_bind_param_batch(stmt,
                (TAOS_MULTI_BIND *)bindArray))) {
            errorPrint("%s() LN%d stmt_bind_param_batch() failed! "
                        "reason: %s %s\n",
                        __func__, __LINE__, taos_stmt_errstr(stmt), tbName);
            countFailureAndFree(bindArray, nBindCols, &failed, tbName);
            continue;
        }

        // add batch
        if (0 != (code = taos_stmt_add_batch(stmt))) {
            errorPrint("%s() LN%d stmt_bind_param() failed! reason: %s\n",
                    __func__, __LINE__, taos_stmt_errstr(stmt));
            countFailureAndFree(bindArray, nBindCols, &failed, tbName);
            continue;
        }

        // batch execute
        if ( 0 == (count % g_args.data_batch) ) {
            if( 0 != (code = taos_stmt_execute(stmt)) ){
                if (code == TSDB_CODE_TDB_TIMESTAMP_OUT_OF_RANGE) {
                    countTSOutOfRange++;
                } else {
                    errorPrint("%s() LN%d taos_stmt_execute() failed! "
                        "code: 0x%08x, reason: %s, timestamp: %"PRId64"\n",
                        __func__, __LINE__,
                        code, taos_stmt_errstr(stmt), ts_debug);
                }
                countFailureAndFree(bindArray, nBindCols, &failed, tbName);
                continue;
            } else {
                success += g_args.data_batch;
                debugPrint("ok call taos_stmt_execute count=%"PRId64" success=%"PRId64" failed=%"PRId64"\n",
                            count, success, failed);
            }
        }
        freeBindArray(bindArray, nBindCols);
    }

    // last batch execute
    if (0 != (count % g_args.data_batch)) {
        if (0 != (code = taos_stmt_execute(stmt))) {
            errorPrint("error last execute taos_stmt_execute. errstr=%s tbName=%s\n", taos_stmt_errstr(stmt), tbName);
            failed++;
        } else {
            success += count % g_args.data_batch;
            debugPrint("ok call last ws_stmt_execute count=%"PRId64" success=%"PRId64" failed=%"PRId64"\n",
                        count, success, failed);
        }
    }

    FREE_DATAIMPL();
    if (failed) {
        if (countTSOutOfRange) {
            errorPrint("Total %"PRId64" record(s) ts out of range!\n",
                    countTSOutOfRange);
        }
        return (-failed);
    }
    return success;
}

static RecordSchema *getSchemaAndReaderFromFile(
        enAVROTYPE avroType, char *avroFile,
        avro_schema_t *schema,
        avro_file_reader_t *reader) {
    if (avro_file_reader(avroFile, reader)) {
        errorPrint("%s() LN%d, Unable to open avro file %s: %s\n",
                __func__, __LINE__,
                avroFile, avro_strerror());
        return NULL;
    }

    // base
    int buf_len =
        ITEM_SPACE +                    // type: record
        ITEM_SPACE + TSDB_DB_NAME_LEN + // namespace: dbname
        ITEM_SPACE;                     // field : {}

    switch (avroType) {
        case enAVRO_TBTAGS:
            buf_len += (TSDB_MAX_COLUMNS + 2) * (TSDB_COL_NAME_LEN + TSDB_DB_NAME_LEN);
            break;

        case enAVRO_DATA:
            // add stbname with ITEM_SPACE
            buf_len = (TSDB_MAX_COLUMNS + 2) * (TSDB_COL_NAME_LEN + ITEM_SPACE) + 2 * ITEM_SPACE;
            break;

        case enAVRO_NTB:
            // no stbname
            buf_len = (TSDB_MAX_COLUMNS + 2) * (TSDB_COL_NAME_LEN + 1 * ITEM_SPACE);
            break;

        default:
            errorPrint("%s() LN%d input mistake list: %d\n",
                    __func__, __LINE__, avroType);
            return NULL;
    }

    char *jsonbuf = calloc(1, buf_len);
    if (NULL == jsonbuf) {
        errorPrint("%s() LN%d, memory allocation failed!\n", __func__, __LINE__);
        return NULL;
    }

    avro_writer_t jsonwriter = avro_writer_memory(jsonbuf, buf_len);

    *schema = avro_file_reader_get_writer_schema(*reader);
    avro_schema_to_json(*schema, jsonwriter);

    if (0 == strlen(jsonbuf)) {
        errorPrint("Failed to parse avro file: %s schema. reason: %s\n",
                avroFile, avro_strerror());
        avro_writer_free(jsonwriter);
        return NULL;
    }
    verbosePrint("Schema:\n  %s\n", jsonbuf);

    json_t *json_root = load_json(jsonbuf);
    verbosePrint("\n%s() LN%d\n === Schema parsed:\n", __func__, __LINE__);
    if (g_args.verbose_print) {
        print_json(json_root);
    }

    avro_writer_free(jsonwriter);
    tfree(jsonbuf);

    if (NULL == json_root) {
        errorPrint("%s() LN%d, cannot read valid schema from %s\n",
                __func__, __LINE__, avroFile);
        return NULL;
    }

    RecordSchema *recordSchema = parse_json_to_recordschema(json_root);
    if (NULL == recordSchema) {
        errorPrint("Failed to parse json to recordschema. reason: %s\n",
                avro_strerror());
        return NULL;
    }
    json_decref(json_root);

    //
    // read stb schema from avroFile + .m file
    //
    if (avroType == enAVRO_TBTAGS || avroType == enAVRO_NTB) {
        mFileToRecordSchema(avroFile, recordSchema);
    }

    return recordSchema;
}

static void closeTaosConnWrapper(void *taos) {
    taos_close(taos);
}

static int64_t dumpInOneAvroFile(
        const char *dbPath,
        const enAVROTYPE avroType,
        char *fcharset,
        char *fileName,
        DBChange *pDbChange,
        StbChange* stbChange) {
    char avroFile[MAX_PATH_LEN];
    snprintf(avroFile, MAX_PATH_LEN, "%s/%s", dbPath, fileName);

    debugPrint("avroFile: %s\n", avroFile);

    avro_file_reader_t reader;
    avro_schema_t schema;

    // read recordSchema from file
    RecordSchema *recordSchema = getSchemaAndReaderFromFile(
            avroType, avroFile, &schema, &reader);
    if (NULL == recordSchema) {
        errorPrint("%s() LN%d, failed to get schema\n", __func__, __LINE__);
        return -1;
    }

    // get db name
    const char *namespace = avro_schema_namespace((const avro_schema_t)schema);
    if(g_args.renameHead) {
        char* newDbName = findNewName((char *)namespace);
        if(newDbName) {
            infoPrint(" ------- rename DB Name %s to %s ------\n", namespace, newDbName);
            namespace = newDbName;
        }
    }
    debugPrint("%s() LN%d, Namespace: %s\n",
            __func__, __LINE__, namespace);

    TAOS *taos = NULL;
    void **taos_v = NULL;
    if (NULL == (taos = taosConnect(namespace))) {
        return -1;
    }
    taos_v = &taos;

    int64_t retExec = 0;
    switch (avroType) {
        case enAVRO_DATA:
            debugPrint("%s() LN%d will dump %s's data\n",
                    __func__, __LINE__, namespace);
            retExec = dumpInAvroDataImpl(taos_v,
                    (char *)namespace,
                    schema, reader, recordSchema,
                    pDbChange,
                    stbChange,
                    dbPath,
                    fileName);
            break;

        case enAVRO_TBTAGS:
            debugPrint("%s() LN%d will dump %s's normal table with tags\n",
                    __func__, __LINE__, namespace);
            retExec = dumpInAvroTbTagsImpl(
                    taos_v,
                    (char *)namespace,
                    schema, reader,
                    fileName,
                    pDbChange,
                    recordSchema);
            break;

        case enAVRO_NTB:
            debugPrint("%s() LN%d will dump %s's normal tables\n",
                    __func__, __LINE__, namespace);
            retExec = dumpInAvroNtbImpl(taos_v,
                    (char *)namespace,
                    schema, reader, pDbChange, recordSchema);
            break;

        default:
            errorPrint("%s() LN%d input mistake list: %d\n",
                    __func__, __LINE__, avroType);
            retExec = -1;
    }

    closeTaosConnWrapper(*taos_v);

    freeRecordSchema(recordSchema);
    avro_schema_decref(schema);
    avro_file_reader_close(reader);

    return retExec;
}

static void* dumpInAvroWorkThreadFp(void *arg) {
    threadInfo *pThreadInfo = (threadInfo*)arg;
    SET_THREAD_NAME("dumpInAvroWorkThrd");
    verbosePrint("[%d] process %"PRId64" files from %"PRId64"\n",
                    pThreadInfo->threadIndex, pThreadInfo->count,
                    pThreadInfo->from);


    //
    //  stb name
    //
    StbChange *stbChange        = NULL;

    char **fileList = NULL;
    switch (pThreadInfo->avroType) {
        case enAVRO_DATA:
            fileList = g_tsDumpInAvroFiles;
            stbChange = readFolderStbName(pThreadInfo->dbPath, pThreadInfo->pDbChange);
            break;

        case enAVRO_TBTAGS:
            fileList = g_tsDumpInAvroTagsTbs;
            break;

        case enAVRO_NTB:
            fileList = g_tsDumpInAvroNtbs;
            break;

        default:
            errorPrint("%s() LN%d input mistake list: %d\n",
                    __func__, __LINE__, pThreadInfo->avroType);
            return NULL;
    }

    int currentPercent = 0;
    int percentComplete = 0;

    for (int64_t i = 0; i < pThreadInfo->count; i++) {
        if (0 == currentPercent) {
            infoPrint("[%d]: Restoring from %s ...\n",
                    pThreadInfo->threadIndex,
                    fileList[pThreadInfo->from + i]);
        }

        char *avroFile = fileList[pThreadInfo->from + i];
        int64_t rows = dumpInOneAvroFile(
                pThreadInfo->dbPath,
                pThreadInfo->avroType,
                g_dumpInCharset,
                avroFile,
                pThreadInfo->pDbChange,
                stbChange);
        if (rows < 0) {
            errorPrint("%s() LN%d, failed to dump file: %s\n", __func__, __LINE__, avroFile);
            switch (pThreadInfo->avroType) {
                case enAVRO_DATA:
                    atomic_add_fetch_64(&g_totalDumpInRecFailed, rows);
                    warnPrint("[%d] %"PRId64" row(s) of file(%s) failed to dumped in!\n",
                                        pThreadInfo->threadIndex, rows,
                                        avroFile);
                    break;

                case enAVRO_TBTAGS:
                    atomic_add_fetch_64(&g_totalDumpInStbFailed, rows);
                    errorPrint("[%d] %"PRId64""
                                        " table(s) belong stb from the file(%s) failed to dumped in!\n",
                                        pThreadInfo->threadIndex, rows,
                                        avroFile);
                    break;

                case enAVRO_NTB:
                    atomic_add_fetch_64(&g_totalDumpInNtbFailed, rows);
                    errorPrint("[%d] %"PRId64" "
                                        " normal tables from (%s) failed to dumped in!\n",
                                        pThreadInfo->threadIndex, rows,
                                        avroFile);
                    break;

                default:
                    errorPrint("%s() LN%d input mistake list: %d\n",
                                        __func__, __LINE__, pThreadInfo->avroType);
                    return NULL;
            }
        } else {
            switch (pThreadInfo->avroType) {
                case enAVRO_DATA:
                    atomic_add_fetch_64(&g_totalDumpInRecSuccess, rows);
                    okPrint("[%d] %"PRId64" row(s) of file(%s) be successfully dumped in!\n",
                                         pThreadInfo->threadIndex, rows,
                                         avroFile);
                    break;

                case enAVRO_TBTAGS:
                    atomic_add_fetch_64(&g_totalDumpInStbSuccess, rows);
                    okPrint("[%d] %"PRId64""
                                         "table(s) belong stb from the file(%s) be successfully dumped in!\n",
                                         pThreadInfo->threadIndex, rows,
                                         avroFile);
                    break;

                case enAVRO_NTB:
                    atomic_add_fetch_64(&g_totalDumpInNtbSuccess, rows);
                    okPrint("[%d] %"PRId64" "
                                         "normal table(s) from (%s) be successfully dumped in!\n",
                                         pThreadInfo->threadIndex, rows,
                                         avroFile);
                    break;

                default:
                    errorPrint("%s() LN%d input mistake list: %d\n",
                                        __func__, __LINE__, pThreadInfo->avroType);
                    return NULL;
            }
        }

        currentPercent = ((i+1) * 100 / pThreadInfo->count);
        if (currentPercent > percentComplete) {
            infoPrint("[%d]:%d%%\n",
                    pThreadInfo->threadIndex, currentPercent);
            percentComplete = currentPercent;
        }
    }

    if (percentComplete < 100) {
        infoPrint("[%d]:%d%%\n", pThreadInfo->threadIndex, 100);
    }

    return NULL;
}

static int dumpInAvroWorkThreads(const char* dbPath, const char *typeExt, DBChange *pDbChange) {
    infoPrint("%s() dump in %s files ...\n", __func__, typeExt);
    int64_t fileCount = getFilesNum(dbPath, typeExt);

    if (0 == fileCount) {
        debugPrint("No .%s file not found in %s\n", typeExt, dbPath);
        return 0;
    }

    int32_t threads = g_args.thread_num;

    int64_t a = fileCount / threads;
    if (a < 1) {
        threads = fileCount;
        a = 1;
    }

    int64_t b = 0;
    if (threads != 0) {
        b = fileCount % threads;
    }

    enAVROTYPE avroType = createDumpinList(dbPath, typeExt, fileCount);

    threadInfo *pThreadInfo;

    pthread_t *pids = calloc(1, threads * sizeof(pthread_t));
    threadInfo *infos = (threadInfo *)calloc(
            threads, sizeof(threadInfo));
    TOOLS_ASSERT(pids);
    TOOLS_ASSERT(infos);

    int64_t from = 0;

    for (int32_t t = 0; t < threads; ++t) {
        pThreadInfo = infos + t;
        pThreadInfo->threadIndex = t;
        pThreadInfo->avroType = avroType;
        pThreadInfo->pDbChange = pDbChange;

        pThreadInfo->from = from;
        pThreadInfo->count = (t < b)?a+1:a;
        from += pThreadInfo->count;
        verbosePrint(
                "Thread[%d] takes care avro files total %"PRId64" files "
                "from %"PRId64"\n",
                t, pThreadInfo->count, pThreadInfo->from);
        TOOLS_STRNCPY(pThreadInfo->dbPath, dbPath, MAX_DIR_LEN);

        if (pthread_create(pids + t, NULL,
                    dumpInAvroWorkThreadFp, (void*)pThreadInfo) != 0) {
            errorPrint("%s() LN%d, thread[%d] failed to start. "
                    "The errno is %d. Reason: %s\n",
                    __func__, __LINE__,
                    pThreadInfo->threadIndex, errno, strerror(errno));
            exit(EXIT_FAILURE);
        }
    }

    for (int32_t i = 0; i < threads; i++) {
        if (pthread_join(pids[i], NULL) !=0) {
            errorPrint("%s() LN%d, thread[%d] failed to join. "
                    "The errno is %d. Reason: %s\n",
                    __func__, __LINE__,
                    i, errno, strerror(errno));
            exit(EXIT_FAILURE);
        }
    }

    free(infos);
    free(pids);

    freeFileList(avroType, fileCount);

    return 0;
}

static int dumpInAvroWorkThreadsSub(const char *dbPath, const char *typeExt, DBChange* pDbChange) {
    int ret = 0;

#ifdef WINDOWS
    TdDirEntryPtr pDirent;
    TdDirPtr pDir;

    pDir = toolsOpenDir(dbPath);

    if (pDir != NULL) {
        while ((pDirent = toolsReadDir(pDir)) != NULL) {
            char *entryName = toolsGetDirEntryName(pDirent);
            if (strncmp ("data", entryName, strlen("data"))
                    == 0) {
                char dataPath[MAX_PATH_LEN] = {0};
                snprintf(dataPath, MAX_PATH_LEN, "%s/%s", dbPath, entryName);
                debugPrint("%s() LN%d, will dump from %s\n",
                        __func__, __LINE__, dataPath);
                ret = dumpInAvroWorkThreads(dataPath, typeExt, pDbChange);
            }
        }
        toolsCloseDir(&pDir);
#else
    struct dirent *pDirent;
    DIR *pDir;

    pDir = opendir(dbPath);

    if (pDir != NULL) {
        while ((pDirent = readdir(pDir)) != NULL) {
            if (strncmp ("data", pDirent->d_name, strlen("data"))
                    == 0) {
                char dataPath[MAX_PATH_LEN] = {0};
                snprintf(dataPath, MAX_PATH_LEN, "%s/%s",
                         dbPath, pDirent->d_name);
                debugPrint("%s() LN%d, will dump from %s\n",
                        __func__, __LINE__, dataPath);
                ret = dumpInAvroWorkThreads(dataPath, typeExt, pDbChange);
            }
        }
        closedir(pDir);
#endif
    } else {
        errorPrint("opendir(%s)\n", g_args.inpath);
        ret = -1;
    }

    return ret;
}

int processResultValue(
        char *pstr,
        const int curr_sqlstr_len,
        const uint8_t type,
        const void *value,
        uint32_t len) {
    if (NULL == value) {
        return sprintf(pstr + curr_sqlstr_len, "NULL");
    }

    switch (type) {
        case TSDB_DATA_TYPE_BOOL:
            return sprintf(pstr + curr_sqlstr_len, "%d",
                    ((((int32_t)(*((char *)value))) == 1)?1:0));

        case TSDB_DATA_TYPE_TINYINT:
            return sprintf(pstr + curr_sqlstr_len, "%d",
                    *((int8_t *)value));

        case TSDB_DATA_TYPE_SMALLINT:
            return sprintf(pstr + curr_sqlstr_len, "%d",
                    *((int16_t *)value));

        case TSDB_DATA_TYPE_INT:
            return sprintf(pstr + curr_sqlstr_len, "%d",
                    *((int32_t *)value));

        case TSDB_DATA_TYPE_BIGINT:
            return sprintf(pstr + curr_sqlstr_len,
                    "%" PRId64,
                    *((int64_t *)value));

        case TSDB_DATA_TYPE_UTINYINT:
            return sprintf(pstr + curr_sqlstr_len, "%d",
                    *((uint8_t *)value));

        case TSDB_DATA_TYPE_USMALLINT:
            return sprintf(pstr + curr_sqlstr_len, "%d",
                    *((uint16_t *)value));

        case TSDB_DATA_TYPE_UINT:
            return sprintf(pstr + curr_sqlstr_len, "%d",
                    *((uint32_t *)value));

        case TSDB_DATA_TYPE_UBIGINT:
            return sprintf(pstr + curr_sqlstr_len,
                    "%" PRIu64,
                    *((uint64_t *)value));

        case TSDB_DATA_TYPE_FLOAT:
            return sprintf(pstr + curr_sqlstr_len, "%f",
                    GET_FLOAT_VAL(value));

        case TSDB_DATA_TYPE_DOUBLE:
            return sprintf(pstr + curr_sqlstr_len, "%f",
                    GET_DOUBLE_VAL(value));

        case TSDB_DATA_TYPE_BINARY:
        case TSDB_DATA_TYPE_VARBINARY:
        case TSDB_DATA_TYPE_GEOMETRY:
            {
                char *bbuf = calloc(1, TSDB_MAX_ALLOWED_SQL_LEN);
                if (NULL == bbuf) {
                    errorPrint("%s() LN%d, memory allocation failed\n",
                               __func__, __LINE__);
                    return -1;
                }
                convertStringToReadable((char *)value, len,
                    bbuf, TSDB_MAX_ALLOWED_SQL_LEN);
                int ret = sprintf(pstr + curr_sqlstr_len,
                    "\'%s\'", bbuf);
                free(bbuf);
                return ret;
            }
        case TSDB_DATA_TYPE_NCHAR:
            {
                char *nbuf = calloc(1, TSDB_MAX_ALLOWED_SQL_LEN);
                if (NULL == nbuf) {
                    errorPrint("%s() LN%d, memory allocation failed\n",
                               __func__, __LINE__);
                    return -1;
                }
                convertNCharToReadable((char *)value, len,
                    nbuf, TSDB_MAX_ALLOWED_SQL_LEN);
                int ret = sprintf(pstr + curr_sqlstr_len,
                    "\'%s\'", nbuf);
                free(nbuf);
                return ret;
            }
        case TSDB_DATA_TYPE_TIMESTAMP:
            return sprintf(pstr + curr_sqlstr_len,
                    "%" PRId64, *(int64_t *)value);
            break;
        default:
            break;
    }
    return 0;
}

static int64_t writeResultDebugNative(
        TAOS_RES *res,
        FILE *fp,
        const char *dbName,
        const char *tbName) {
    int64_t    totalRows     = 0;

    int32_t  sql_buf_len = g_args.max_sql_len;
    char* tmpBuffer = (char *)calloc(1, sql_buf_len + 128);
    if (NULL == tmpBuffer) {
        errorPrint("%s() LN%d, memory allocation failed!\n", __func__, __LINE__);
        return 0;
    }

    char *pstr = tmpBuffer;

    TAOS_ROW row = NULL;
    int64_t lastRowsPrint = 5000000;
    int count = 0;

    int numFields = taos_field_count(res);
    TOOLS_ASSERT(numFields > 0);
    TAOS_FIELD *fields = taos_fetch_fields(res);

    int32_t  total_sqlstr_len = 0;

    while ((row = taos_fetch_row(res)) != NULL) {
        int32_t* lengths = taos_fetch_lengths(res);   // act len

        int32_t curr_sqlstr_len = 0;

        if (count == 0) {
            total_sqlstr_len = 0;
            curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len,
                    "INSERT INTO %s.%s VALUES (", dbName, tbName);
        } else {
            curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, "(");
        }

        for (int col = 0; col < numFields; col++) {
            if (col != 0) {
                curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, ", ");
            }

            curr_sqlstr_len += processResultValue(
                    pstr,
                    curr_sqlstr_len,
                    fields[col].type,
                    row[col],
                    lengths[col]);
        }

        curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, ")");

        totalRows++;
        count++;
        if(fp) fprintf(fp, "%s", tmpBuffer);

        if (totalRows >= lastRowsPrint) {
            infoPrint(" %"PRId64 " rows already be dump-out from %s.%s\n",
                    totalRows, dbName, tbName);
            lastRowsPrint += 5000000;
        }

        total_sqlstr_len += curr_sqlstr_len;

        if ((count >= g_args.data_batch)
                || (sql_buf_len - total_sqlstr_len < TSDB_MAX_BYTES_PER_ROW)) {
            if (fp) fprintf(fp, ";\n");
            count = 0;
        }
    }

    debugPrint("total_sqlstr_len: %d\n", total_sqlstr_len);

    if(fp) fprintf(fp, "\n");
    free(tmpBuffer);

    return totalRows;
}


TAOS_RES *queryDbForDumpOutNative(TAOS *taos,
        const char *dbName,
        const char *tbName,
        const int precision,
        const int64_t start_time,
        const int64_t end_time) {
    char *command = calloc(1, TSDB_MAX_ALLOWED_SQL_LEN);
    if (NULL == command) {
        errorPrint("%s() LN%d, memory allocation failed\n", __func__, __LINE__);
        return NULL;
    }

    snprintf(command, TSDB_MAX_ALLOWED_SQL_LEN,
            g_args.db_escape_char
            ? "SELECT * FROM `%s`.%s%s%s WHERE _c0 >= %" PRId64 " "
            "AND _c0 <= %" PRId64 " ORDER BY _c0 ASC;"
            : "SELECT * FROM %s.%s%s%s WHERE _c0 >= %" PRId64 " "
            "AND _c0 <= %" PRId64 " ORDER BY _c0 ASC;",
            dbName, g_escapeChar, tbName, g_escapeChar,
            start_time, end_time);

    int32_t code = -1;
    TAOS_RES* res = taosQuery(taos, command, &code);
    if (code != 0) {
        cleanIfQueryFailed(__func__, __LINE__, command, res);
        return NULL;
    }
    free(command);
    return res;
}


static int64_t dumpTableDataAvroTsRange(
        char *dataFilename,
        int64_t index,
        const char *stable,
        const char *tbName,
        const bool belongStb,
        const char* dbName,
        const int precision,
        int colCount,
        TableDes *tableDes,
        int64_t start_time,
        int64_t end_time
        ) {
    TAOS *taos;
    if (NULL == (taos = taosConnect(dbName))) {
        return -1;
    }

    char *jsonSchema = NULL;
    int  ret = 0;
    if (g_args.loose_mode) {
        ret = convertTbDesToJsonLoose(dbName, stable, tbName, tableDes, colCount, &jsonSchema);
    } else {
        ret = convertTbDesToJson(     dbName, stable, tbName, tableDes, colCount, &jsonSchema);
    }
    if (ret) {
        errorPrint("%s() LN%d, covert tableDes cols to json failed\n",
                __func__,
                __LINE__);
        taos_close(taos);
        return -1;
    }

    int64_t totalRows = writeResultToAvro(
            dataFilename, dbName, tbName, jsonSchema, &taos, precision,
            start_time, end_time);

    taos_close(taos);
    tfree(jsonSchema);

    return totalRows;
}

static int generateSubDirName(
        const SDbInfo *dbInfo, char *subDirName, const char *stable) {

    uint32_t uidStb = bkdrHash(stable);

    // gen sub dir name
    snprintf(subDirName, MAX_FILE_NAME_LEN, "data%"PRIu64"-%X",
            (g_countOfDataFile / g_maxFilesPerDir), uidStb);
    atomic_add_fetch_64(&g_countOfDataFile, 1);

    // create sub dir
    char dirToCreate[MAX_PATH_LEN] = {0};
    if (g_args.loose_mode) {
        snprintf(dirToCreate, MAX_PATH_LEN, "%s"CUS_PROMPT"dump.%s/%s",
                g_args.outpath, dbInfo->name, subDirName);
    } else {
        snprintf(dirToCreate, MAX_PATH_LEN, "%s"CUS_PROMPT"dump.%"PRIu64"/%s",
                g_args.outpath, dbInfo->uniqueID, subDirName);
    }

    int ret = 0;

    TdDirPtr dir = toolsOpenDir(dirToCreate);
    if (dir) {
        /* Directory exists. */
        toolsCloseDir(&dir);
    } else {
        /* Directory does not exist. */
        ret = mkdir(dirToCreate, 0755);
        if (ret) {
            if (EEXIST == errno) {
                warnPrint("%s() LN%d, %s exists.\n",
                        __func__, __LINE__, dirToCreate);
                ret = 0;
            } else {
                /* mkdir() failed for some other reason. */
                errorPrint("%s() LN%d, mkdir(%s) failed. Errno: %d\n",
                        __func__, __LINE__, dirToCreate, errno);
                ret = errno;
            }
        }
    }

    //
    // create stbname file
    //
    if (stable) {
        strcat(dirToCreate, STBNAME_FILE);
        if (writeFile(dirToCreate, (char *)stable)) {
            warnPrint("create file stbname failed. stb:%s file:%s\n", stable, dirToCreate);
        }
    }

    return ret;
}

static int generateFilename(enAVROTYPE avroType, char *fileName,
        const SDbInfo *dbInfo, const char * stable, const char *tbName, const int64_t index) {
    int ret = 0;
    if (g_args.loose_mode) {
        switch (avroType) {
            case enAVRO_TBTAGS:
                snprintf(fileName, MAX_PATH_LEN,
                         "%s"CUS_PROMPT"dump.%s/%s.%s.%"PRId64".avro-tbtags",
                        g_args.outpath, dbInfo->name, dbInfo->name,
                        tbName, index);
                break;

            case enAVRO_NTB:
                snprintf(fileName, MAX_PATH_LEN,
                         "%s"CUS_PROMPT"dump.%s/%s.%s.avro-ntb",
                        g_args.outpath, dbInfo->name, dbInfo->name, tbName);
                break;

            case enAVRO_DATA:
                {
                    // to avoid buffer overflow
                    char subDirName[MAX_FILE_NAME_LEN - 39] = {0};
                    if (0 != generateSubDirName(dbInfo, subDirName, stable)) {
                        return -1;
                    }

                    snprintf(fileName, MAX_PATH_LEN,
                             "%s"CUS_PROMPT"dump.%s/%s/%s.%s.%"PRId64".avro",
                            g_args.outpath, dbInfo->name,
                            subDirName,
                            dbInfo->name,
                            tbName,
                            index);
                }
                break;

            case enAVRO_UNKNOWN:
                snprintf(fileName, MAX_PATH_LEN,
                         "%s%s.%s.%"PRId64".sql",
                        g_args.outpath,
                        dbInfo->name, tbName, index);
                break;

            default:
                break;
        }
    } else {
        switch (avroType) {
            case enAVRO_TBTAGS: {
                uint32_t uidStb = bkdrHash(stable);
                snprintf(fileName, MAX_PATH_LEN,
                         "%s"CUS_PROMPT"dump.%"PRIu64"/%s.%X.avro-tbtags",
                        g_args.outpath, dbInfo->uniqueID, dbInfo->name, uidStb);
                }
                break;

            case enAVRO_NTB:
                snprintf(fileName, MAX_PATH_LEN,
                         "%s"CUS_PROMPT"dump.%"PRIu64"/%s.%"PRIu64".avro-ntb",
                        g_args.outpath, dbInfo->uniqueID, dbInfo->name,
                        getUniqueIDFromEpoch());
                break;

            case enAVRO_DATA:
                {
                    char subDirName[MAX_FILE_NAME_LEN] = {0};
                    if (0 != generateSubDirName(dbInfo, subDirName, stable)) {
                        return -1;
                    }

                    snprintf(fileName, MAX_PATH_LEN,
                            "%s"CUS_PROMPT"dump.%"PRIu64"/%s/%s.%"PRIu64".%"PRId64".avro",
                            g_args.outpath, dbInfo->uniqueID,
                            subDirName,
                            dbInfo->name,
                            getUniqueIDFromEpoch(),
                            index);
                }
                break;

            case enAVRO_UNKNOWN:
                    snprintf(fileName, MAX_PATH_LEN,
                            "%s%s.%s.%"PRId64".sql",
                            g_args.outpath,
                            dbInfo->name,
                            tbName, index);
                break;

            default:
                break;
        }
    }

    return ret;
}

//
// dump table data with tableDes
//
static int64_t dumpTableDataAvro(
        const int64_t index,
        const char *stable,
        const bool belongStb,
        const SDbInfo *dbInfo,
        const int precision,
        const int colCount,
        TableDes *tableDes
        ) {
    char *tbName = tableDes->name;
    char dataFilename[MAX_PATH_LEN] = {0};
    if (0 != generateFilename(enAVRO_DATA, dataFilename,
                dbInfo, stable, tbName, index)) {
        return -1;
    }

    int64_t start_time = getStartTime(precision);
    int64_t end_time = getEndTime(precision);
    if ((-1 == start_time) || (-1 == end_time)) {
        return -1;
    }

    int64_t rows = dumpTableDataAvroTsRange(dataFilename, index, stable, tbName,
                belongStb, dbInfo->name, precision, colCount, tableDes,
                start_time, end_time);
    return rows;
}

static int64_t dumpTableDataDebugTsRange(
        const int64_t index,
        FILE *fp,
        const char *tbName,
        const char* dbName,
        const int precision,
        TableDes *tableDes,
        const int64_t start_time,
        const int64_t end_time
        ) {
    TAOS *taos;
    if (NULL == (taos = taosConnect(dbName))) {
        return -1;
    }

    TAOS_RES *res = queryDbForDumpOutNative(
            taos, dbName, tbName, precision, start_time, end_time);

    int64_t totalRows = writeResultDebugNative(res, fp, dbName, tbName);

    taos_free_result(res);
    taos_close(taos);

    return totalRows;
}

static int64_t dumpTableDataDebug(
        const int64_t index,
        FILE *fp,
        const char *tbName,
        const SDbInfo* dbInfo,
        const int precision,
        TableDes *tableDes
        ) {
    int64_t start_time = getStartTime(precision);
    int64_t end_time = getEndTime(precision);

    if ((-1 == start_time) || (-1 == end_time)) {
        return -1;
    }


    int64_t rows = dumpTableDataDebugTsRange(index, fp, tbName, dbInfo->name,
                precision, tableDes, start_time, end_time);
    return rows;
}

// dump table (stable/child table/normal table) meta and data
int64_t dumpTable(
        const int64_t index,
        void  **taos_v,
        const SDbInfo *dbInfo,
        const bool belongStb,
        const char *stable,
        const TableDes *stbDes,
        const char *tbName,
        const int precision,
        char *dumpFilename,
        FILE *fp
        ) {
    if (0 == strlen(tbName)) {
        errorPrint("%s() LN%d, pass wrong tbname\n", __func__, __LINE__);
        return -1;
    }
    int numColsAndTags = 0;
    TableDes *tableDes = NULL;

    //
    // dump table meta (stable or child table or normal table)
    //

    // dump table schema which is created by using super table
    if (stable != NULL && stable[0] != '\0') {
        // super table
        if (!g_args.avro) {
            tableDes = (TableDes *)calloc(1, sizeof(TableDes)
                    + sizeof(ColDes) * TSDB_MAX_COLUMNS);
            if (NULL == tableDes) {
                errorPrint("%s() LN%d, memory allocation failed!\n",
                        __func__, __LINE__);
                return -1;
            }

            numColsAndTags = getTableDes(*taos_v,
                        dbInfo->name, tbName, tableDes, !belongStb);

            if (numColsAndTags < 0) {
                errorPrint("%s() LN%d, failed to get table[%s] schema\n",
                        __func__,
                        __LINE__,
                        tbName);
                free(tableDes);
                return -1;
            }

            dumpCreateMTableClause(dbInfo->name,
                    stable, tableDes, numColsAndTags, fp);
        }
    } else {
        // child table or normal table
        tableDes = (TableDes *)calloc(1, sizeof(TableDes)
                + sizeof(ColDes) * TSDB_MAX_COLUMNS);
        if (NULL == tableDes) {
            errorPrint("%s() LN%d, memory allocation failed!\n", __func__, __LINE__);
            return -1;
        }

        numColsAndTags = getTableDes(*taos_v,
                    dbInfo->name, tbName, tableDes, !belongStb);

        if (numColsAndTags < 0) {
            errorPrint("%s() LN%d, failed to get table[%s] schema\n",
                    __func__,
                    __LINE__,
                    tbName);
            free(tableDes);
            return -1;
        }

        if (g_args.avro) {
            // avro
            if (belongStb) {
                // child table
                if (0 != generateFilename(enAVRO_TBTAGS,
                        dumpFilename,
                        dbInfo, stable, tbName, 0)) {
                    return -1;
                }
                debugPrint("%s() LN%d dumpFilename: %s\n",
                        __func__, __LINE__, dumpFilename);
            } else {
                // normal-table
                if (0 != generateFilename(enAVRO_NTB,
                        dumpFilename, dbInfo, stable, tbName, 0)) {
                    return -1;
                }

                // create mfile for normal table
                if( 0 != createNTableMFile(dumpFilename, tableDes)) {
                    return -1;
                }
            }
            int32_t ret = dumpCreateTableClauseAvro(taos_v,
                    dumpFilename, tableDes, numColsAndTags, dbInfo->name);
            if (ret != 0) {
                if (tableDes) {
                    freeTbDes(tableDes, true);
                }
                errorPrint("%s() LN%d, call dumpCreateTableClauseAvro failed!\n",  __func__, __LINE__);
                return ret;
            }
        } else {
            int32_t ret = dumpCreateTableClause(taos_v, tableDes, numColsAndTags, fp, dbInfo->name);
            if (ret != 0) {
                if (tableDes) {
                    freeTbDes(tableDes, true);
                }
                errorPrint("%s() LN%d, call dumpCreateTableClause failed!\n",  __func__, __LINE__);
                return ret;
            }
        }
    }

    //
    // dump out data
    //
    int64_t totalRows = 0;
    if (!g_args.schemaonly) {
        if (g_args.avro) {
            if (NULL == tableDes) {
                tableDes = (TableDes *)calloc(1, sizeof(TableDes)
                        + sizeof(ColDes) * TSDB_MAX_COLUMNS);
                if (NULL == tableDes) {
                    errorPrint("%s() LN%d, memory allocation failed!\n",
                            __func__, __LINE__);
                    return -1;
                }
                numColsAndTags = getTableDesFromStbNative(
                            *taos_v, dbInfo->name,
                            stbDes, tbName, &tableDes);
                if (numColsAndTags < 0) {
                    errorPrint("%s() LN%d columns/tags count is %d\n",
                            __func__, __LINE__, numColsAndTags);
                    if (tableDes) {
                        freeTbDes(tableDes, true);
                        return -1;
                    }
                }
            }

            totalRows = dumpTableDataAvro(
                    index,
                    stable,
                    belongStb,
                    dbInfo, precision,
                    numColsAndTags, tableDes);
        } else {
            totalRows = dumpTableDataDebug(
                    index,
                    fp, tbName,
                    dbInfo, precision,
                    tableDes);
        }
    }

    if (tableDes) {
        freeTbDes(tableDes, true);
    }
    return totalRows;
}

int64_t dumpTableNotBelong(
        int64_t index,
        void **taos_v, SDbInfo *dbInfo, char *ntbName) {
    int64_t count = 0;

    char dumpFilename[MAX_PATH_LEN] = {0};
    FILE *fp = NULL;

    if (g_args.avro) {
        if (0 == strlen(ntbName)) {
            errorPrint("%s() LN%d, pass wrong tbname, length:0\n",
                    __func__, __LINE__);
            return -1;
        }
        // gen meta file name
        if (0 != generateFilename(enAVRO_NTB,
                dumpFilename, dbInfo, NULL, ntbName, index)) {
            return -1;
        }
        count = dumpTable(
                index,
                taos_v,
                dbInfo,
                false,
                NULL,
                NULL,
                ntbName,
                getPrecisionByString(dbInfo->precision),
                dumpFilename,
                NULL);
    } else {
        // AVRO_UNKNOWN is save with sql clause
        if (0 != generateFilename(enAVRO_UNKNOWN,
                dumpFilename, dbInfo, NULL, ntbName, 0)) {
            return -1;
        }

        fp = fopen(dumpFilename, "w");
        if (fp == NULL) {
            errorPrint("%s() LN%d, failed to open file %s. "
                    "Errno is %d. Reason is %s.\n",
                    __func__, __LINE__, dumpFilename, errno, strerror(errno));
            return -1;
        }

        if (0 == strlen(ntbName)) {
            errorPrint("%s() LN%d, pass wrong tbname\n", __func__, __LINE__);
            fclose(fp);
            return -1;
        }
        count = dumpTable(
                index,
                taos_v,
                dbInfo,
                false,
                NULL,
                NULL,
                ntbName,
                getPrecisionByString(dbInfo->precision),
                NULL,
                fp);
        fclose(fp);
    }
    if (count > 0) {
        atomic_add_fetch_64(&g_totalDumpOutRows, count);
        return 0;
    }

    return count;
}

static int createMTableAvroHeadImp(
        void  **taos_v,
        const char *dbName,
        const char *stable,
        const TableDes *stbTableDes,
        const char *tbName,
        avro_file_writer_t writer,
        avro_value_iface_t *wface) {
    if (0 == strlen(tbName)) {
        errorPrint("%s() LN%d, pass wrong tbname\n", __func__, __LINE__);
        return -1;
    }

    avro_value_t record;
    avro_generic_value_new(wface, &record);

    avro_value_t value, branch;

    if (!g_args.loose_mode) {
        if (0 != avro_value_get_by_name(
                    &record, "stbname", &value, NULL)) {
            errorPrint("%s() LN%d, avro_value_get_by_name(..%s..) failed",
                    __func__, __LINE__, "stbname");
            return -1;
        }

        avro_value_set_branch(&value, 1, &branch);
        char* outSName = (char*)stable;
        char stableName[TSDB_TABLE_NAME_LEN + 1];
        if(g_args.dotReplace && replaceCopy(stableName, (char*)stable)) {
            outSName = stableName;
        }
        avro_value_set_string(&branch, outSName);
    }

    if (0 != avro_value_get_by_name(
                &record, "tbname", &value, NULL)) {
        errorPrint("%s() LN%d, avro_value_get_by_name(..%s..) failed",
                __func__, __LINE__, "tbname");
        return -1;
    }

    avro_value_set_branch(&value, 1, &branch);

    char* outName = (char*)tbName;
    char tableName[TSDB_TABLE_NAME_LEN + 1];
    if(g_args.dotReplace && replaceCopy(tableName, (char*)tbName)) {
        outName = tableName;
    }
    avro_value_set_string(&branch, outName);

    TableDes *subTableDes = (TableDes *) calloc(1, sizeof(TableDes)
            + sizeof(ColDes) * (stbTableDes->columns + stbTableDes->tags));
    if (NULL == subTableDes) {
        errorPrint("%s() LN%d, memory allocation failed!\n", __func__, __LINE__);
        return -1;
    }

    getTableDesFromStbNative(*taos_v, dbName,
                stbTableDes,
                tbName,
                &subTableDes);

    for (int tag = 0; tag < subTableDes->tags; tag++) {
        debugPrint("%s() LN%d, sub table %s no. %d tags is %s, "
                   "type is %d, value is %s\n",
                __func__, __LINE__, tbName, tag,
                subTableDes->cols[subTableDes->columns + tag].field,
                subTableDes->cols[subTableDes->columns + tag].type,
                subTableDes->cols[subTableDes->columns + tag].value);

        char tmpBuf[MIDDLE_BUFF_LEN] = {0};
        snprintf(tmpBuf, MIDDLE_BUFF_LEN, "tag%d", tag);

        if (0 != avro_value_get_by_name(
                    &record, tmpBuf, &value, NULL)) {
            errorPrint("%s() LN%d, avro_value_get_by_name(..%s..) failed\n",
                    __func__, __LINE__,
                    subTableDes->cols[subTableDes->columns + tag].field);
        }

        avro_value_t firsthalf, secondhalf;
        uint8_t u8Temp = 0;
        uint16_t u16Temp = 0;
        uint32_t u32Temp = 0;
        uint64_t u64Temp = 0;
        memset(&firsthalf, 0, sizeof(avro_value_t));
        memset(&secondhalf, 0, sizeof(avro_value_t));


        int type = subTableDes->cols[subTableDes->columns + tag].type;
        switch (type) {
            case TSDB_DATA_TYPE_BOOL:
                if (0 == strncmp(
                            subTableDes->cols[subTableDes->columns+tag].note,
                            "NUL", 3)) {
                    avro_value_set_branch(&value, 0, &branch);
                    avro_value_set_null(&branch);
                } else {
                    avro_value_set_branch(&value, 1, &branch);
                    int tmp = atoi((const char *)
                                subTableDes->cols[subTableDes->columns+tag].value);
                    verbosePrint("%s() LN%d, before set_bool() tmp=%d\n",
                            __func__, __LINE__, (int)tmp);
                    avro_value_set_boolean(&branch, (tmp)?1:0);
                }
                break;

            case TSDB_DATA_TYPE_TINYINT:
                if (0 == strncmp(
                            subTableDes->cols[subTableDes->columns+tag].note,
                            "NUL", 3)) {
                    avro_value_set_branch(&value, 0, &branch);
                    avro_value_set_null(&branch);
                } else {
                    avro_value_set_branch(&value, 1, &branch);
                    avro_value_set_int(&branch,
                            (int8_t)atoi((const char *)
                                subTableDes->cols[subTableDes->columns
                                + tag].value));
                }
                break;

            case TSDB_DATA_TYPE_SMALLINT:
                if (0 == strncmp(
                            subTableDes->cols[subTableDes->columns+tag].note,
                            "NUL", 3)) {
                    avro_value_set_branch(&value, 0, &branch);
                    avro_value_set_null(&branch);
                } else {
                    avro_value_set_branch(&value, 1, &branch);
                    avro_value_set_int(&branch,
                            (int16_t)atoi((const char *)
                                subTableDes->cols[subTableDes->columns
                                + tag].value));
                }
                break;

            case TSDB_DATA_TYPE_INT:
                if (0 == strncmp(
                            subTableDes->cols[subTableDes->columns+tag].note,
                            "NUL", 3)) {
                    avro_value_set_branch(&value, 0, &branch);
                    avro_value_set_null(&branch);
                } else {
                    avro_value_set_branch(&value, 1, &branch);
                    avro_value_set_int(&branch,
                            (int32_t)atoi((const char *)
                                subTableDes->cols[subTableDes->columns
                                + tag].value));
                }
                break;

            case TSDB_DATA_TYPE_BIGINT:
                if (0 == strncmp(
                            subTableDes->cols[subTableDes->columns+tag].note,
                            "NUL", 3)) {
                    avro_value_set_branch(&value, 0, &branch);
                    avro_value_set_null(&branch);
                } else {
                    avro_value_set_branch(&value, 1, &branch);
                    avro_value_set_long(&branch,
                            (int64_t)atoll((const char *)
                                subTableDes->cols[subTableDes->columns + tag].value));
                }
                break;

            case TSDB_DATA_TYPE_FLOAT:
                if (0 == strncmp(
                            subTableDes->cols[subTableDes->columns+tag].note,
                            "NUL", 3)) {
                    avro_value_set_branch(&value, 0, &branch);
                    avro_value_set_null(&branch);
                } else {
                    avro_value_set_branch(&value, 1, &branch);
                    if (subTableDes->cols[subTableDes->columns + tag].var_value) {
                        avro_value_set_float(&branch,
                                atof(subTableDes->cols[subTableDes->columns
                                    + tag].var_value));
                    } else {
                        avro_value_set_float(&branch,
                                atof(subTableDes->cols[subTableDes->columns
                                    + tag].value));
                    }
                }
                break;

            case TSDB_DATA_TYPE_DOUBLE:
                if (0 == strncmp(
                            subTableDes->cols[subTableDes->columns+tag].note,
                            "NUL", 3)) {
                    avro_value_set_branch(&value, 0, &branch);
                    avro_value_set_null(&branch);
                } else {
                    avro_value_set_branch(&value, 1, &branch);
                    if (subTableDes->cols[subTableDes->columns + tag].var_value) {
                        avro_value_set_double(&branch,
                                atof(subTableDes->cols[subTableDes->columns
                                    + tag].var_value));
                    } else {
                        avro_value_set_double(&branch,
                                atof(subTableDes->cols[subTableDes->columns
                                    + tag].value));
                    }
                }
                break;

            case TSDB_DATA_TYPE_BINARY:
                if (0 == strncmp(
                            subTableDes->cols[subTableDes->columns+tag].note,
                            "NUL", 3)) {
                    avro_value_set_branch(&value, 0, &branch);
                    avro_value_set_null(&branch);
                } else {
                    avro_value_set_branch(&value, 1, &branch);
                    if (subTableDes->cols[subTableDes->columns + tag].var_value) {
                        avro_value_set_string(&branch,
                                subTableDes->cols[subTableDes->columns
                                + tag].var_value);
                    } else {
                        avro_value_set_string(&branch,
                                subTableDes->cols[subTableDes->columns
                                + tag].value);
                    }
                }
                break;

            case TSDB_DATA_TYPE_NCHAR:
            case TSDB_DATA_TYPE_JSON:
            case TSDB_DATA_TYPE_VARBINARY:
            case TSDB_DATA_TYPE_GEOMETRY:
                if (0 == strncmp(
                            subTableDes->cols[subTableDes->columns+tag].note,
                            "NUL", 3)) {
                    avro_value_set_branch(&value, 0, &branch);
                    avro_value_set_null(&branch);
                } else {
                    avro_value_set_branch(&value, 1, &branch);
                    if (subTableDes->cols[subTableDes->columns + tag].var_value) {
                        size_t nlen = strlen(
                                subTableDes->cols[subTableDes->columns
                                + tag].var_value);
                        char *bytes = malloc(nlen+1);
                        TOOLS_ASSERT(bytes);

                        memcpy(bytes,
                                subTableDes->cols[subTableDes->columns
                                + tag].var_value,
                                nlen);
                        bytes[nlen] = 0;
                        avro_value_set_bytes(&branch, bytes, nlen);
                        free(bytes);
                    } else {
                        avro_value_set_bytes(&branch,
                                subTableDes->cols[subTableDes->columns + tag].value,
                                strlen(subTableDes->cols[subTableDes->columns
                                    + tag].value));
                    }
                }
                break;

            case TSDB_DATA_TYPE_TIMESTAMP:
                if (0 == strncmp(
                            subTableDes->cols[subTableDes->columns+tag].note,
                            "NUL", 3)) {
                    avro_value_set_branch(&value, 0, &branch);
                    avro_value_set_null(&branch);
                } else {
                    avro_value_set_branch(&value, 1, &branch);
                    avro_value_set_long(&branch,
                            (int64_t)atoll((const char *)
                                subTableDes->cols[subTableDes->columns + tag].value));
                }
                break;

            case TSDB_DATA_TYPE_UTINYINT:
                if (0 == strncmp(
                            subTableDes->cols[subTableDes->columns+tag].note,
                            "NUL", 3)) {
                    avro_value_set_branch(&value, 0, &branch);
                    avro_value_set_null(&branch);
                } else {
                    avro_value_set_branch(&value, 1, &branch);
                    u8Temp = (int8_t)atoi((const char *)
                            subTableDes->cols[subTableDes->columns + tag].value);

                    int8_t n8tmp = (int8_t)(u8Temp - SCHAR_MAX);
                    avro_value_append(&branch, &firsthalf, NULL);
                    avro_value_set_int(&firsthalf, n8tmp);
                    debugPrint("%s() LN%d, first half is: %d, ",
                            __func__, __LINE__, n8tmp);
                    avro_value_append(&branch, &secondhalf, NULL);
                    avro_value_set_int(&secondhalf, (int32_t)SCHAR_MAX);
                    debugPrint("second half is: %d\n", SCHAR_MAX);
                }

                break;

            case TSDB_DATA_TYPE_USMALLINT:
                if (0 == strncmp(
                            subTableDes->cols[subTableDes->columns+tag].note,
                            "NUL", 3)) {
                    avro_value_set_branch(&value, 0, &branch);
                    avro_value_set_null(&branch);
                } else {
                    avro_value_set_branch(&value, 1, &branch);
                    u16Temp = (int16_t)atoi((const char *)
                            subTableDes->cols[subTableDes->columns + tag].value);

                    int16_t n16tmp = (int16_t)(u16Temp - SHRT_MAX);
                    avro_value_append(&branch, &firsthalf, NULL);
                    avro_value_set_int(&firsthalf, n16tmp);
                    debugPrint("%s() LN%d, first half is: %d, ",
                            __func__, __LINE__, n16tmp);
                    avro_value_append(&branch, &secondhalf, NULL);
                    avro_value_set_int(&secondhalf, (int32_t)SHRT_MAX);
                    debugPrint("second half is: %d\n", SHRT_MAX);
                }

                break;

            case TSDB_DATA_TYPE_UINT:
                if (0 == strncmp(
                            subTableDes->cols[subTableDes->columns+tag].note,
                            "NUL", 3)) {
                    avro_value_set_branch(&value, 0, &branch);
                    avro_value_set_null(&branch);
                } else {
                    avro_value_set_branch(&value, 1, &branch);
                    u32Temp = (uint32_t)strtoul((const char *)
                            subTableDes->cols[subTableDes->columns + tag].value, NULL, 10);

                    int32_t n32tmp = (int32_t)(u32Temp - INT_MAX);
                    avro_value_append(&branch, &firsthalf, NULL);
                    avro_value_set_int(&firsthalf, n32tmp);
                    debugPrint("%s() LN%d, first half is: %d, ",
                            __func__, __LINE__, n32tmp);
                    avro_value_append(&branch, &secondhalf, NULL);
                    avro_value_set_int(&secondhalf, (int32_t)INT_MAX);
                    debugPrint("second half is: %d\n", INT_MAX);
                }

                break;

            case TSDB_DATA_TYPE_UBIGINT:
                if (0 == strncmp(
                            subTableDes->cols[subTableDes->columns+tag].note,
                            "NUL", 3)) {
                    avro_value_set_branch(&value, 0, &branch);
                    avro_value_set_null(&branch);
                } else {
                    avro_value_set_branch(&value, 1, &branch);
                    char *eptr;
                    u64Temp = strtoull((const char *)
                            subTableDes->cols[subTableDes->columns + tag].value,
                            &eptr, 10);

                    int64_t n64tmp = (int64_t)(u64Temp - LONG_MAX);
                    avro_value_append(&branch, &firsthalf, NULL);
                    avro_value_set_long(&firsthalf, n64tmp);
                    debugPrint("%s() LN%d, first half is: %"PRId64", ",
                            __func__, __LINE__, n64tmp);
                    avro_value_append(&branch, &secondhalf, NULL);
                    avro_value_set_long(&secondhalf, (int64_t)LONG_MAX);
                    debugPrint("second half is: %"PRId64"\n", (int64_t)LONG_MAX);
                }

                break;

            default:
                errorPrint("Unknown type: %d\n", type);
                break;
        }
    }

    if (0 != avro_file_writer_append_value(writer, &record)) {
        errorPrint("%s() LN%d, Unable to write record to file. Message: %s\n",
                __func__, __LINE__,
                avro_strerror());
    }
    avro_value_decref(&record);
    freeTbDes(subTableDes, true);

    return 0;
}

static int createMTableAvroHeadSpecified(
        void **taos_v,
        char *dumpFilename,
        const char *dbName,
        const char *stable,
        const char *specifiedTb) {
    if (0 == strlen(stable)) {
        errorPrint("%s() LN%d, pass wrong tbname\n", __func__, __LINE__);
        return -1;
    }
    TableDes *stbTableDes = (TableDes *)calloc(1, sizeof(TableDes)
            + sizeof(ColDes) * TSDB_MAX_COLUMNS);
    if (NULL == stbTableDes) {
        errorPrint("%s() LN%d, memory allocation failed!\n", __func__, __LINE__);
        return -1;
    }


    getTableDes(*taos_v, dbName, stable, stbTableDes, false);

    char *jsonTagsSchema = NULL;
    int32_t ret = 0;
    if(g_args.loose_mode) {
        ret = convertTbTagsDesToJsonLoose(dbName, stable, stbTableDes, &jsonTagsSchema);
    } else {
        ret = convertTbTagsDesToJson(     dbName, stable, stbTableDes, &jsonTagsSchema);
    }
    if (ret) {
        errorPrint("%s() LN%d, convertTbTagsDesToJsonWrap failed\n",
                __func__,
                __LINE__);
        tfree(jsonTagsSchema);
        freeTbDes(stbTableDes, true);
        return -1;
    }

    debugPrint("tagsJson:\n%s\n", jsonTagsSchema);

    avro_schema_t schema;
    RecordSchema *recordSchema;
    avro_file_writer_t db;

    avro_value_iface_t *wface = prepareAvroWface(
            dumpFilename,
            jsonTagsSchema, &schema, &recordSchema, &db);

    if (specifiedTb) {
        createMTableAvroHeadImp(
                taos_v, dbName, stable,
                stbTableDes,
                specifiedTb, db, wface);
    }


    avro_value_iface_decref(wface);
    freeRecordSchema(recordSchema);
    avro_file_writer_close(db);
    avro_schema_decref(schema);

    tfree(jsonTagsSchema);

    ret = -1;
    char *stbJson = tableDesToJson(stbTableDes);
    if (stbJson) {
        strcat(dumpFilename, MFILE_EXT);
        ret = writeFile(dumpFilename, stbJson);
        free(stbJson);
        stbJson = NULL;
    }

    freeTbDes(stbTableDes, true);

    return ret;
}

static int64_t fillTbNameArrNative(
        TAOS *taos,
        char *command,
        char **tbNameArr,
        const char *stable,
        const int64_t preCount) {
    int32_t code = -1;
    TAOS_RES *res = taosQuery(taos, command, &code);
    if (code) {
        errorPrint("%s() LN%d, fillTbNameArrNative failed to run command <%s>. "
                "code: 0x%08x, reason: %s\n",
            __func__, __LINE__, command, taos_errno(res), taos_errstr(res));
        taos_free_result(res);
        return -1;
    }

    TAOS_ROW row = NULL;
    int64_t  n = 0;

    int currentPercent = 0;
    int percentComplete = 0;

    while ((row = taos_fetch_row(res)) != NULL) {
        int32_t *lengths = taos_fetch_lengths(res);
        // calc name len
        int32_t len = lengths[TSDB_SHOW_TABLES_NAME_INDEX];
        if (len <= 0) {
            errorPrint("%s() LN%d, fetch_row() get %d length!\n",
                    __func__, __LINE__, len);
            continue;
        }
        // malloc and copy
        tbNameArr[n] = calloc(len + 1, 1); // add string end
        strncpy(tbNameArr[n], (char *)row[TSDB_SHOW_TABLES_NAME_INDEX], len);

        debugPrint("child table name: %s. %"PRId64" of stable: %s\n",
                tbNameArr[n], n, stable);
        // tb count add and check
        if(++n == preCount) {
            break;
        }

        currentPercent = (n * 100 / preCount);

        if (currentPercent > percentComplete) {
            infoPrint("connection %p fetched %d%% of %s' tbname\n",
                    taos, currentPercent, stable);
            percentComplete = currentPercent;
        }
    }

    if (preCount == n) {
        okPrint("total %"PRId64" sub-table's name of stable: %s fetched\n", n, stable);
    } else {
        errorPrint("%d%% - total %"PRId64" sub-table's names of stable: %s fetched\n",
            percentComplete, n, stable);
    }

    taos_free_result(res);
    return n;
}

static int64_t fillTbNameArr(
        void **taos_v, char **tbNameArr,
        const SDbInfo *dbInfo,
        const char *stable,
        int64_t preCount) {
    //
    char *command2 = calloc(1, TSDB_MAX_ALLOWED_SQL_LEN);
    if (NULL == command2) {
        errorPrint("%s() LN%d, memory allocation failed\n", __func__, __LINE__);
        return -1;
    }


    snprintf(command2, TSDB_MAX_ALLOWED_SQL_LEN,
            g_args.db_escape_char
            ? "SELECT DISTINCT(TBNAME) FROM `%s`.%s%s%s "
            : "SELECT DISTINCT(TBNAME) FROM %s.%s%s%s ",
            dbInfo->name, g_escapeChar, stable, g_escapeChar);

    debugPrint("%s() LN%d, run command <%s>.\n",
                __func__, __LINE__, command2);

     int64_t ntbCount = fillTbNameArrNative(
                *taos_v, command2, tbNameArr, stable, preCount);
    infoPrint("The number of tables of %s be filled is %"PRId64"!\n",
            stable, ntbCount);

    free(command2);
    return ntbCount;
}


// old createMTableAvroHeadImp
static int writeTagsToAvro(
            const char *dbName,
            const TableDes *stbDes,
            const TableDes *tbDes,
            avro_file_writer_t writer,
            avro_value_iface_t *wface) {
    // avro
    avro_value_t record;
    avro_generic_value_new(wface, &record);
    avro_value_t value, branch;

    if (!g_args.loose_mode) {
        if (0 != avro_value_get_by_name(
                    &record, "stbname", &value, NULL)) {
            errorPrint("%s() LN%d, avro_value_get_by_name(..%s..) failed",
                    __func__, __LINE__, "stbname");
            return -1;
        }

        avro_value_set_branch(&value, 1, &branch);
        char* outSName = (char*)stbDes->name;
        char stableName[TSDB_TABLE_NAME_LEN + 1];
        if(g_args.dotReplace && replaceCopy(stableName, (char*)stbDes->name)) {
            outSName = stableName;
        }
        avro_value_set_string(&branch, outSName);
    }

    if (0 != avro_value_get_by_name(
                &record, "tbname", &value, NULL)) {
        errorPrint("%s() LN%d, avro_value_get_by_name(..%s..) failed",
                __func__, __LINE__, "tbname");
        return -1;
    }

    avro_value_set_branch(&value, 1, &branch);

    char* outName = (char*)tbDes->name;
    char tableName[TSDB_TABLE_NAME_LEN + 1];
    if(g_args.dotReplace && replaceCopy(tableName, (char*)tbDes->name)) {
        outName = tableName;
    }
    avro_value_set_string(&branch, outName);

    for (int tag = 0; tag < tbDes->tags; tag++) {
        debugPrint("%s() LN%d, sub table %s no. %d tags is %s, "
                   "type is %d, value is %s\n",
                __func__, __LINE__, tbDes->name, tag,
                tbDes->cols[tbDes->columns + tag].field,
                tbDes->cols[tbDes->columns + tag].type,
                tbDes->cols[tbDes->columns + tag].value);

        char tmpBuf[MIDDLE_BUFF_LEN] = {0};
        snprintf(tmpBuf, MIDDLE_BUFF_LEN, "tag%d", tag);

        if (0 != avro_value_get_by_name(
                    &record, tmpBuf, &value, NULL)) {
            errorPrint("%s() LN%d, avro_value_get_by_name(..%s..) failed\n",
                    __func__, __LINE__,
                    tbDes->cols[tbDes->columns + tag].field);
        }

        avro_value_t firsthalf, secondhalf;
        uint8_t u8Temp = 0;
        uint16_t u16Temp = 0;
        uint32_t u32Temp = 0;
        uint64_t u64Temp = 0;
        memset(&firsthalf, 0, sizeof(avro_value_t));
        memset(&secondhalf, 0, sizeof(avro_value_t));

        int type = tbDes->cols[tbDes->columns + tag].type;
        switch (type) {
            case TSDB_DATA_TYPE_BOOL:
                if (0 == strncmp(
                            tbDes->cols[tbDes->columns+tag].note,
                            "NUL", 3)) {
                    avro_value_set_branch(&value, 0, &branch);
                    avro_value_set_null(&branch);
                } else {
                    avro_value_set_branch(&value, 1, &branch);
                    int tmp = atoi((const char *)
                                tbDes->cols[tbDes->columns+tag].value);
                    verbosePrint("%s() LN%d, before set_bool() tmp=%d\n",
                            __func__, __LINE__, (int)tmp);
                    avro_value_set_boolean(&branch, (tmp)?1:0);
                }
                break;

            case TSDB_DATA_TYPE_TINYINT:
                if (0 == strncmp(
                            tbDes->cols[tbDes->columns+tag].note,
                            "NUL", 3)) {
                    avro_value_set_branch(&value, 0, &branch);
                    avro_value_set_null(&branch);
                } else {
                    avro_value_set_branch(&value, 1, &branch);
                    avro_value_set_int(&branch,
                            (int8_t)atoi((const char *)
                                tbDes->cols[tbDes->columns
                                + tag].value));
                }
                break;

            case TSDB_DATA_TYPE_SMALLINT:
                if (0 == strncmp(
                            tbDes->cols[tbDes->columns+tag].note,
                            "NUL", 3)) {
                    avro_value_set_branch(&value, 0, &branch);
                    avro_value_set_null(&branch);
                } else {
                    avro_value_set_branch(&value, 1, &branch);
                    avro_value_set_int(&branch,
                            (int16_t)atoi((const char *)
                                tbDes->cols[tbDes->columns
                                + tag].value));
                }
                break;

            case TSDB_DATA_TYPE_INT:
                if (0 == strncmp(
                            tbDes->cols[tbDes->columns+tag].note,
                            "NUL", 3)) {
                    avro_value_set_branch(&value, 0, &branch);
                    avro_value_set_null(&branch);
                } else {
                    avro_value_set_branch(&value, 1, &branch);
                    avro_value_set_int(&branch,
                            (int32_t)atoi((const char *)
                                tbDes->cols[tbDes->columns
                                + tag].value));
                }
                break;

            case TSDB_DATA_TYPE_BIGINT:
                if (0 == strncmp(
                            tbDes->cols[tbDes->columns+tag].note,
                            "NUL", 3)) {
                    avro_value_set_branch(&value, 0, &branch);
                    avro_value_set_null(&branch);
                } else {
                    avro_value_set_branch(&value, 1, &branch);
                    avro_value_set_long(&branch,
                            (int64_t)atoll((const char *)
                                tbDes->cols[tbDes->columns + tag].value));
                }
                break;

            case TSDB_DATA_TYPE_FLOAT:
                if (0 == strncmp(
                            tbDes->cols[tbDes->columns+tag].note,
                            "NUL", 3)) {
                    avro_value_set_branch(&value, 0, &branch);
                    avro_value_set_null(&branch);
                } else {
                    avro_value_set_branch(&value, 1, &branch);
                    if (tbDes->cols[tbDes->columns + tag].var_value) {
                        avro_value_set_float(&branch,
                                atof(tbDes->cols[tbDes->columns
                                    + tag].var_value));
                    } else {
                        avro_value_set_float(&branch,
                                atof(tbDes->cols[tbDes->columns
                                    + tag].value));
                    }
                }
                break;

            case TSDB_DATA_TYPE_DOUBLE:
                if (0 == strncmp(
                            tbDes->cols[tbDes->columns+tag].note,
                            "NUL", 3)) {
                    avro_value_set_branch(&value, 0, &branch);
                    avro_value_set_null(&branch);
                } else {
                    avro_value_set_branch(&value, 1, &branch);
                    if (tbDes->cols[tbDes->columns + tag].var_value) {
                        avro_value_set_double(&branch,
                                atof(tbDes->cols[tbDes->columns
                                    + tag].var_value));
                    } else {
                        avro_value_set_double(&branch,
                                atof(tbDes->cols[tbDes->columns
                                    + tag].value));
                    }
                }
                break;

            case TSDB_DATA_TYPE_BINARY:
                if (0 == strncmp(
                            tbDes->cols[tbDes->columns+tag].note,
                            "NUL", 3)) {
                    avro_value_set_branch(&value, 0, &branch);
                    avro_value_set_null(&branch);
                } else {
                    avro_value_set_branch(&value, 1, &branch);
                    if (tbDes->cols[tbDes->columns + tag].var_value) {
                        avro_value_set_string(&branch,
                                tbDes->cols[tbDes->columns
                                + tag].var_value);
                    } else {
                        avro_value_set_string(&branch,
                                tbDes->cols[tbDes->columns
                                + tag].value);
                    }
                }
                break;

            case TSDB_DATA_TYPE_NCHAR:
            case TSDB_DATA_TYPE_JSON:
            case TSDB_DATA_TYPE_VARBINARY:
            case TSDB_DATA_TYPE_GEOMETRY:
                if (0 == strncmp(
                            tbDes->cols[tbDes->columns+tag].note,
                            "NUL", 3)) {
                    avro_value_set_branch(&value, 0, &branch);
                    avro_value_set_null(&branch);
                } else {
                    avro_value_set_branch(&value, 1, &branch);
                    if (tbDes->cols[tbDes->columns + tag].var_value) {
                        size_t nlen = strlen(
                                tbDes->cols[tbDes->columns
                                + tag].var_value);
                        char *bytes = malloc(nlen+1);
                        TOOLS_ASSERT(bytes);

                        memcpy(bytes,
                                tbDes->cols[tbDes->columns
                                + tag].var_value,
                                nlen);
                        bytes[nlen] = 0;
                        avro_value_set_bytes(&branch, bytes, nlen);
                        free(bytes);
                    } else {
                        avro_value_set_bytes(&branch,
                                (void *)tbDes->cols[tbDes->columns + tag].value,
                                strlen(tbDes->cols[tbDes->columns + tag].value));
                    }
                }
                break;

            case TSDB_DATA_TYPE_TIMESTAMP:
                if (0 == strncmp(
                            tbDes->cols[tbDes->columns+tag].note,
                            "NUL", 3)) {
                    avro_value_set_branch(&value, 0, &branch);
                    avro_value_set_null(&branch);
                } else {
                    avro_value_set_branch(&value, 1, &branch);
                    avro_value_set_long(&branch,
                            (int64_t)atoll((const char *)
                                tbDes->cols[tbDes->columns + tag].value));
                }
                break;

            case TSDB_DATA_TYPE_UTINYINT:
                if (0 == strncmp(
                            tbDes->cols[tbDes->columns+tag].note,
                            "NUL", 3)) {
                    avro_value_set_branch(&value, 0, &branch);
                    avro_value_set_null(&branch);
                } else {
                    avro_value_set_branch(&value, 1, &branch);
                    u8Temp = (int8_t)atoi((const char *)
                            tbDes->cols[tbDes->columns + tag].value);

                    int8_t n8tmp = (int8_t)(u8Temp - SCHAR_MAX);
                    avro_value_append(&branch, &firsthalf, NULL);
                    avro_value_set_int(&firsthalf, n8tmp);
                    debugPrint("%s() LN%d, first half is: %d, ",
                            __func__, __LINE__, n8tmp);
                    avro_value_append(&branch, &secondhalf, NULL);
                    avro_value_set_int(&secondhalf, (int32_t)SCHAR_MAX);
                    debugPrint("second half is: %d\n", SCHAR_MAX);
                }

                break;

            case TSDB_DATA_TYPE_USMALLINT:
                if (0 == strncmp(
                            tbDes->cols[tbDes->columns+tag].note,
                            "NUL", 3)) {
                    avro_value_set_branch(&value, 0, &branch);
                    avro_value_set_null(&branch);
                } else {
                    avro_value_set_branch(&value, 1, &branch);
                    u16Temp = (int16_t)atoi((const char *)
                            tbDes->cols[tbDes->columns + tag].value);

                    int16_t n16tmp = (int16_t)(u16Temp - SHRT_MAX);
                    avro_value_append(&branch, &firsthalf, NULL);
                    avro_value_set_int(&firsthalf, n16tmp);
                    debugPrint("%s() LN%d, first half is: %d, ",
                            __func__, __LINE__, n16tmp);
                    avro_value_append(&branch, &secondhalf, NULL);
                    avro_value_set_int(&secondhalf, (int32_t)SHRT_MAX);
                    debugPrint("second half is: %d\n", SHRT_MAX);
                }

                break;

            case TSDB_DATA_TYPE_UINT:
                if (0 == strncmp(
                            tbDes->cols[tbDes->columns+tag].note,
                            "NUL", 3)) {
                    avro_value_set_branch(&value, 0, &branch);
                    avro_value_set_null(&branch);
                } else {
                    avro_value_set_branch(&value, 1, &branch);
                    u32Temp = (uint32_t)strtoul((const char *)
                            tbDes->cols[tbDes->columns + tag].value, NULL, 10);

                    int32_t n32tmp = (int32_t)(u32Temp - INT_MAX);
                    avro_value_append(&branch, &firsthalf, NULL);
                    avro_value_set_int(&firsthalf, n32tmp);
                    debugPrint("%s() LN%d, first half is: %d, ",
                            __func__, __LINE__, n32tmp);
                    avro_value_append(&branch, &secondhalf, NULL);
                    avro_value_set_int(&secondhalf, (int32_t)INT_MAX);
                    debugPrint("second half is: %d\n", INT_MAX);
                }

                break;

            case TSDB_DATA_TYPE_UBIGINT:
                if (0 == strncmp(
                            tbDes->cols[tbDes->columns+tag].note,
                            "NUL", 3)) {
                    avro_value_set_branch(&value, 0, &branch);
                    avro_value_set_null(&branch);
                } else {
                    avro_value_set_branch(&value, 1, &branch);
                    char *eptr;
                    u64Temp = strtoull((const char *)
                            tbDes->cols[tbDes->columns + tag].value,
                            &eptr, 10);

                    int64_t n64tmp = (int64_t)(u64Temp - LONG_MAX);
                    avro_value_append(&branch, &firsthalf, NULL);
                    avro_value_set_long(&firsthalf, n64tmp);
                    debugPrint("%s() LN%d, first half is: %"PRId64", ",
                            __func__, __LINE__, n64tmp);
                    avro_value_append(&branch, &secondhalf, NULL);
                    avro_value_set_long(&secondhalf, (int64_t)LONG_MAX);
                    debugPrint("second half is: %"PRId64"\n", (int64_t)LONG_MAX);
                }

                break;

            default:
                errorPrint("Unknown type: %d\n", type);
                break;
        }
    }

    if (0 != avro_file_writer_append_value(writer, &record)) {
        errorPrint("%s() LN%d, Unable to write record to file. Message: %s\n",
                __func__, __LINE__,
                avro_strerror());
    }
    avro_value_decref(&record);

    return 0;
}

// open query with native or websocket
void* openQuery(void** taos_v, const char * sql) {
    int32_t code = -1;
    TAOS_RES* res = taosQuery(*taos_v, sql, &code);
    if (code != 0) {
        taos_free_result(res);
        errorPrint("open query: %s execute failed. errcode=%d\n", sql, code);
        return NULL;
    }
    return res;
}

// close query and free result
void closeQuery(void* res) {
    if(res) {
        taos_free_result(res);
    }
}

// read next table tags to tbDes
int readNextTableDesNative(void* res, TableDes* tbDes) {
    // tbname, tagName , tagValue
    TAOS_ROW row = NULL;
    int index = 0;
    while( index < tbDes->tags && NULL != (row = taos_fetch_row(res))) {
        // tbname changed check
        int* lengths = taos_fetch_lengths(res);
        if(tbDes->name[0] == 0) {
            // first set tbName
            strncpy(tbDes->name, row[0], lengths[0]);
        } else {
            // compare tbname change
            if(!(strncmp(tbDes->name, row[0], lengths[0]) == 0
               && tbDes->name[lengths[0]] == 0)){
                // tbname cnanged, break
                break;
            }
        }

        // copy tagname
        if (NULL == row[2]) {
            strcpy(tbDes->cols[index].value, "NULL");
            strcpy(tbDes->cols[index].note , "NUL");
        } else if (0 != processFieldsValueV3(index, tbDes, row[2], lengths[2])) {
            errorPrint("%s() LN%d, call processFieldsValueV3 tag_value: %p\n",
                    __func__, __LINE__, row[1]);
            return -1;
        }
        index++;
    }

    // check tags count corrent
    if(row && index != tbDes->tags) {
        errorPrint("child table %s read tags(%d) not equal stable tags (%d).",
                    tbDes->name, index, tbDes->tags);
        return -1;
    }

    return index;
}

int32_t readRow(void *res, int32_t idx, int32_t col, uint32_t *len, char **data) {
  TAOS_ROW row = NULL;
  int32_t  i = 0;
  while (NULL != (row = taos_fetch_row(res))) {
    // check
    if (i < idx) {
      continue;
    }

    // set
    int32_t *lens = taos_fetch_lengths(res);
    *data = row[col];
    *len = lens[col];
    break;

    // move next
    i++;
  }

  return 0;
}


#define SQL_LEN 512
static int dumpStableMeta(
        void **taos_v,
        const SDbInfo *dbInfo,
        TableDes *stbDes,
        char **tbNameArr,
        int64_t tbCount) {
    // valid
    char * stable = stbDes->name;
    if (0 == stable[0]) {
        errorPrint("%s() LN%d, pass wrong tbname\n", __func__, __LINE__);
        return -1;
    }

    // dump file name .avro-tbtags
    char dumpFilename[MAX_PATH_LEN] = {0};
    if (0 != generateFilename(enAVRO_TBTAGS, dumpFilename,
            dbInfo, stable, stable, 0)) {
        return -1;
    }
    debugPrint("%s() LN%d dumpFilename: %s\n",
            __func__, __LINE__, dumpFilename);

    // gen tags json
    char *jsonTagsSchema = NULL;
    int32_t ret = 0;
    if(g_args.loose_mode) {
        ret = convertTbTagsDesToJsonLoose(dbInfo->name, stable, stbDes, &jsonTagsSchema);
    } else {
        ret = convertTbTagsDesToJson(dbInfo->name, stable, stbDes, &jsonTagsSchema);
    }
    if (ret) {
        errorPrint("%s() LN%d, convert tableDes to json failed.\n", __func__, __LINE__);
        tfree(jsonTagsSchema);
        return -1;
    }

    // gen cols json

    // avro
    debugPrint("tagsJson:\n%s\n", jsonTagsSchema);
    avro_schema_t schema;
    RecordSchema *recordSchema;
    avro_file_writer_t avroWriter;
    avro_value_iface_t *wface = prepareAvroWface(
            dumpFilename,
            jsonTagsSchema, &schema, &recordSchema, &avroWriter);
    infoPrint("connection: %p is dumping out schema of sub-table(s) of %s \n",
            *taos_v, stable);
    // free json
    tfree(jsonTagsSchema);


    // query tag and values
    char sql[SQL_LEN] = {0};
    snprintf(sql, SQL_LEN,
             "select table_name,tag_name,tag_value from information_schema.ins_tags "
             "where db_name='%s' and stable_name='%s';",
             dbInfo->name, stable);

    void* tagsRes = openQuery(taos_v, sql);
    if (tagsRes == NULL ) {
        return -1;
    }

    // loop read tables des
    int size = sizeof(TableDes) + sizeof(ColDes) * stbDes->tags;
    TableDes *tbDes = calloc(1, size);
    int64_t tb = 0;
    while (tb <= tbCount) {
        // read tags
        freeTbDes(tbDes, false); // free cols values
        memset(tbDes->name, 0, sizeof(tbDes->name)); // reset zero
        tbDes->tags = stbDes->tags; // stable tags same with child table
        memcpy(tbDes->cols, &stbDes->cols[stbDes->columns], sizeof(ColDes)* stbDes->tags); // copy tag info
        ret = readNextTableDesNative(tagsRes, tbDes);
        if(ret < 0){
            // read error
            freeTbDes(tbDes, true);
            return ret;
        } else if (ret == 0) {
            // read end , break
            break;
        }

        // dump tbname to array
        tbNameArr[tb] = strdup(tbDes->name);

        // write tags to avro
        ret = writeTagsToAvro(
                dbInfo->name,
                stbDes, tbDes,
                avroWriter, wface);
        if(ret < 0) {
            // write error
            freeTbDes(tbDes, true);
            return ret;
        }

        // sucess print
        tb++;
        infoPrint("connection %p is dumping out schema: %"PRId64" from %s.%s\n", *taos_v, tb, stable, tbDes->name);
    }

    //
    // output stable meta file
    //
    ret = -1;
    char *stbJson = tableDesToJson(stbDes);
    if (stbJson) {
        strcat(dumpFilename, MFILE_EXT);
        ret = writeFile(dumpFilename, stbJson);
        free(stbJson);
        stbJson = NULL;
    }

    if (ret == 0) {
        okPrint("total %"PRId64" table(s) of stable: %s schema dumped.\n", tb, stable);
    }

    // free
    closeQuery(tagsRes);
    avro_value_iface_decref(wface);
    freeRecordSchema(recordSchema);
    avro_file_writer_close(avroWriter);
    avro_schema_decref(schema);
    freeTbDes(tbDes, true);

    return ret;
}

static int64_t dumpTableBelongStb(
        int64_t index,
        TAOS **taos_v,
        SDbInfo *dbInfo, char *stbName,
        const TableDes *stbTableDes,
        char *childName) {
    int64_t count = 0;

    char dumpFilename[MAX_PATH_LEN] = {0};
    FILE *fp = NULL;

    if (g_args.avro) {
        if (0 != generateFilename(enAVRO_TBTAGS,
                dumpFilename, dbInfo, stbName, stbName, 0)) {
            return -1;
        }
        debugPrint("%s() LN%d dumpFilename: %s\n",
                __func__, __LINE__, dumpFilename);

        int ret = createMTableAvroHeadSpecified(
                taos_v,
                dumpFilename,
                dbInfo->name,
                stbName,
                childName);
        if (-1 == ret) {
            errorPrint("%s() LN%d, failed to open file %s\n",
                    __func__, __LINE__, dumpFilename);
            return -1;
        }
    } else {
        if (0 != generateFilename(enAVRO_UNKNOWN,
                    dumpFilename, dbInfo, NULL, childName, 0)) {
            return -1;
        }
        fp = fopen(dumpFilename, "w");

        if (fp == NULL) {
            errorPrint("%s() LN%d, failed to open file %s. "
                    "Errno is %d. Reason is %s.\n",
                    __func__, __LINE__, dumpFilename, errno, strerror(errno));
            return -1;
        }
    }

    if (0 == strlen(childName)) {
        errorPrint("%s() LN%d, pass wrong tbname\n", __func__, __LINE__);
        if (NULL != fp) {
            fclose(fp);
        }
        return -1;
    }
    count = dumpTable(
            index,
            taos_v,
            dbInfo,
            true,
            stbName,
            stbTableDes,
            childName,
            getPrecisionByString(dbInfo->precision),
            dumpFilename,
            fp);

    if (!g_args.avro) {
        fclose(fp);
    }

    if (count > 0) {
        atomic_add_fetch_64(&g_totalDumpOutRows, count);
        return 0;
    }

    return count;
}

static void printArgs(FILE *file) {
    fprintf(file, "========== arguments config =========\n");

    printVersion(file);

    fprintf(file, "host: %s\n", g_args.host);
    fprintf(file, "user: %s\n", g_args.user);
    fprintf(file, "port: %u\n", g_args.port);
    fprintf(file, "outpath: %s\n", g_args.outpath);
    fprintf(file, "inpath: %s\n", g_args.inpath);
    fprintf(file, "resultFile: %s\n", g_args.resultFile);
    fprintf(file, "all_databases: %s\n", g_args.all_databases?"true":"false");
    fprintf(file, "databases: %s\n", g_args.databases?"true":"false");
    fprintf(file, "databasesSeq: %s\n", g_args.databasesSeq);
    fprintf(file, "schemaonly: %s\n", g_args.schemaonly?"true":"false");
    fprintf(file, "with_property: %s\n", g_args.with_property?"true":"false");
    fprintf(file, "avro codec: %s\n", g_avro_codec[g_args.avro_codec]);

    if (strlen(g_args.humanStartTime)) {
        fprintf(file, "human readable start time: %s \n", g_args.humanStartTime);
    } else if (DEFAULT_START_TIME != g_args.start_time) {
        fprintf(file, "start_time: %" PRId64 "\n", g_args.start_time);
    }
    if (strlen(g_args.humanEndTime)) {
        fprintf(file, "human readable end time: %s \n", g_args.humanEndTime);
    } else if (DEFAULT_END_TIME != g_args.end_time) {
        fprintf(file, "end_time: %" PRId64 "\n", g_args.end_time);
    }
    fprintf(file, "data_batch: %d\n", g_args.data_batch);
    fprintf(file, "thread_num: %d\n", g_args.thread_num);
    fprintf(file, "allow_sys: %s\n", g_args.allow_sys?"true":"false");
    fprintf(file, "escape_char: %s\n", g_args.escape_char?"true":"false");
    fprintf(file, "loose_mode: %s\n", g_args.loose_mode?"true":"false");
    fprintf(file, "isDumpIn: %s\n", g_args.isDumpIn?"true":"false");
    fprintf(file, "arg_list_len: %d\n", g_args.arg_list_len);

/* TODO
#ifdef WEBSOCKET
    if (g_args.cloud) {
        fprintf(file, "cloud: %s\n", g_args.cloud?"true":"false");
        fprintf(file, "cloudHost: %s\n", g_args.cloudHost);
        fprintf(file, "cloud port: %d\n", g_args.cloudPort);
        char first4OfToken[5] = {0};
        char last4OfToken[5] = {0};

        if (g_args.cloudToken && strlen(g_args.cloudToken) > 12) {
            strncpy(first4OfToken, g_args.cloudToken, 4);
            strncpy(last4OfToken, g_args.cloudToken + strlen(g_args.cloudToken) - 4, 4);
            fprintf(file, "first 4 letter of cloud token: %s\n", first4OfToken);
            fprintf(file, "last 4 letter of cloud token: %s\n", last4OfToken);
        }
    }
#endif  // WEBSOCKET
*/

    fflush(file);
}

static int checkParam() {
    if (AVRO_CODEC_UNKNOWN == g_args.avro_codec) {
        if (g_args.debug_print || g_args.verbose_print) {
            g_args.avro = false;
        } else {
            errorPrint("%s", "Unknown AVRO codec inputted. Exit program!\n");
            exit(1);
        }
    }

    if (!g_args.escape_char || g_args.loose_mode) {
        strcpy(g_escapeChar, "");
    }

    printf("==============================\n");
    printArgs(stdout);

    for (int32_t i = 0; i < g_args.arg_list_len; i++) {
        if (g_args.databases || g_args.all_databases) {
            errorPrint("%s is an invalid input if database(s) "
                    "be already specified.\n",
                    g_args.arg_list[i]);
            exit(EXIT_FAILURE);
        } else {
            printf("arg_list[%d]: %s\n", i, g_args.arg_list[i]);
        }
    }

    if (g_args.all_databases && g_args.databases) {
        errorPrint("%s", "conflict option --all-databases and --databases\n");
        return -1;
    }

    if (g_args.start_time > g_args.end_time) {
        errorPrint("%s", "start time is larger than end time\n");
        return -1;
    }

    if (g_args.arg_list_len == 0) {
        if ((!g_args.all_databases)
                && (!g_args.databases)
                && (!g_args.isDumpIn)) {
            errorPrint("%s", CUS_PROMPT"dump requires parameters\n");
            return -1;
        }
    }

    if ((!g_args.isDumpIn)
            && (!g_args.databases)
            && (!g_args.all_databases)
            && (0 == g_args.arg_list_len)) {
        errorPrint("%s", "Invalid option in dump out\n");
        return -1;
    }

    g_fpOfResult = fopen(g_args.resultFile, "a");
    if (NULL == g_fpOfResult) {
        errorPrint("Failed to open %s for save res. "
                "Errno is %d. Reason is %s.\n",
                g_args.resultFile,
                errno, strerror(errno));
        exit(EXIT_FAILURE);
    }

    return 0;
}

char *ascii_literal_list[] = {
    "\\x00", "\\x01", "\\x02", "\\x03", "\\x04", "\\x05", "\\x06", "\\x07", "\\x08", "\\t",   "\\n",   "\\x0b", "\\x0c",
    "\\r",   "\\x0e", "\\x0f", "\\x10", "\\x11", "\\x12", "\\x13", "\\x14", "\\x15", "\\x16", "\\x17", "\\x18", "\\x19",
    "\\x1a", "\\x1b", "\\x1c", "\\x1d", "\\x1e", "\\x1f", " ",     "!",     "\\\"",  "#",     "$",     "%",     "&",
    "\\'",   "(",     ")",     "*",     "+",     ",",     "-",     ".",     "/",     "0",     "1",     "2",     "3",
    "4",     "5",     "6",     "7",     "8",     "9",     ":",     ";",     "<",     "=",     ">",     "?",     "@",
    "A",     "B",     "C",     "D",     "E",     "F",     "G",     "H",     "I",     "J",     "K",     "L",     "M",
    "N",     "O",     "P",     "Q",     "R",     "S",     "T",     "U",     "V",     "W",     "X",     "Y",     "Z",
    "[",     "\\\\",  "]",     "^",     "_",     "`",     "a",     "b",     "c",     "d",     "e",     "f",     "g",
    "h",     "i",     "j",     "k",     "l",     "m",     "n",     "o",     "p",     "q",     "r",     "s",     "t",
    "u",     "v",     "w",     "x",     "y",     "z",     "{",     "|",     "}",     "~",     "\\x7f", "\\x80", "\\x81",
    "\\x82", "\\x83", "\\x84", "\\x85", "\\x86", "\\x87", "\\x88", "\\x89", "\\x8a", "\\x8b", "\\x8c", "\\x8d", "\\x8e",
    "\\x8f", "\\x90", "\\x91", "\\x92", "\\x93", "\\x94", "\\x95", "\\x96", "\\x97", "\\x98", "\\x99", "\\x9a", "\\x9b",
    "\\x9c", "\\x9d", "\\x9e", "\\x9f", "\\xa0", "\\xa1", "\\xa2", "\\xa3", "\\xa4", "\\xa5", "\\xa6", "\\xa7", "\\xa8",
    "\\xa9", "\\xaa", "\\xab", "\\xac", "\\xad", "\\xae", "\\xaf", "\\xb0", "\\xb1", "\\xb2", "\\xb3", "\\xb4", "\\xb5",
    "\\xb6", "\\xb7", "\\xb8", "\\xb9", "\\xba", "\\xbb", "\\xbc", "\\xbd", "\\xbe", "\\xbf", "\\xc0", "\\xc1", "\\xc2",
    "\\xc3", "\\xc4", "\\xc5", "\\xc6", "\\xc7", "\\xc8", "\\xc9", "\\xca", "\\xcb", "\\xcc", "\\xcd", "\\xce", "\\xcf",
    "\\xd0", "\\xd1", "\\xd2", "\\xd3", "\\xd4", "\\xd5", "\\xd6", "\\xd7", "\\xd8", "\\xd9", "\\xda", "\\xdb", "\\xdc",
    "\\xdd", "\\xde", "\\xdf", "\\xe0", "\\xe1", "\\xe2", "\\xe3", "\\xe4", "\\xe5", "\\xe6", "\\xe7", "\\xe8", "\\xe9",
    "\\xea", "\\xeb", "\\xec", "\\xed", "\\xee", "\\xef", "\\xf0", "\\xf1", "\\xf2", "\\xf3", "\\xf4", "\\xf5", "\\xf6",
    "\\xf7", "\\xf8", "\\xf9", "\\xfa", "\\xfb", "\\xfc", "\\xfd", "\\xfe", "\\xff"};

static int convertStringToReadable(char *str, int size, char *buf, int bufsize) {
    char *pstr = str;
    char *pbuf = buf;
    while (size > 0) {
        if (*pstr == '\0') break;
        pbuf = stpcpy(pbuf, ascii_literal_list[((uint8_t)(*pstr))]);
        pstr++;
        size--;
    }
    *pbuf = '\0';
    return 0;
}

static int convertNCharToReadable(char *str, int size, char *buf, int bufsize) {
    char *pstr = str;
    char *pbuf = buf;
    wchar_t wc;
    while (size > 0) {
        if (*pstr == '\0') break;
        int byte_width = mbtowc(&wc, pstr, MB_CUR_MAX);
        if (byte_width < 0) {
            errorPrint("%s() LN%d, mbtowc() return fail.\n", __func__, __LINE__);
            exit(-1);
        }

        if ((int)wc < 256) {
            pbuf = stpcpy(pbuf, ascii_literal_list[(int)wc]);
        } else {
            memcpy(pbuf, pstr, byte_width);
            pbuf += byte_width;
        }
        pstr += byte_width;
    }

    *pbuf = '\0';

    return 0;
}

static void dumpExtraInfoVar(void *taos, FILE *fp) {
    char buffer[BUFFER_LEN];
    char *command = calloc(1, TSDB_MAX_ALLOWED_SQL_LEN);
    if (NULL == command) {
        errorPrint("%s() LN%d, memory allocation failed\n", __func__, __LINE__);
        return;
    }
    strcpy(command, "SHOW VARIABLES");

    int32_t code = -1;
    TAOS_RES* res = taosQuery(taos, command, &code);

    code = taos_errno(res);
    if (code != 0) {
        warnPrint("failed to run command %s, "
                "code: 0x%08x, reason: %s. Will use default settings\n",
                command, code, taos_errstr(res));
        fprintf(g_fpOfResult, "# SHOW VARIABLES failed, "
                "code: 0x%08x, reason:%s\n",
                taos_errno(res), taos_errstr(res));
        fprintf(g_fpOfResult, "# charset: %s\n", "UTF-8 (default)");
        snprintf(buffer, BUFFER_LEN, "#!charset: %s\n", "UTF-8");
        size_t len = fwrite(buffer, 1, strlen(buffer), fp);
        if (len != strlen(buffer)) {
            errorPrint("%s() LN%d, write to file. "
                    "try to write %zu, actual len %zu, "
                    "Errno is %d. Reason is %s.\n",
                    __func__, __LINE__, strlen(buffer), len,
                    errno, strerror(errno));
        }

        taos_free_result(res);
        free(command);
        return;
    }

    TAOS_ROW row;
    while ((row = taos_fetch_row(res)) != NULL) {
        int32_t *lengths = taos_fetch_lengths(res);
        char tempRow0[BUFFER_LEN - 13] = {0};
        char tempRow1[BUFFER_LEN - 13] = {0};
        strncpy(tempRow0, row[0], min(lengths[0], BUFFER_LEN-13));
        strncpy(tempRow1, row[1], min(lengths[1], BUFFER_LEN-13));
        debugPrint("row[0]=%s, row[1]=%s\n",
                tempRow0, tempRow1);
        if (0 == strcmp(tempRow0, "charset")) {
            snprintf(buffer, BUFFER_LEN, "#!charset: %s\n", tempRow1);
            size_t len = fwrite(buffer, 1, strlen(buffer), fp);
            if (len != strlen(buffer)) {
                errorPrint("%s() LN%d, write to file. "
                        "try to write %zu, actual len %zu, "
                        "Errno is %d. Reason is %s.\n",
                        __func__, __LINE__, strlen(buffer), len,
                        errno, strerror(errno));
            }
        }
    }
    taos_free_result(res);
    free(command);
}

static int dumpExtraInfoHead(void *taos, FILE *fp) {
    char buffer[BUFFER_LEN];

    if (fseek(fp, 0, SEEK_SET) != 0) {
        return -1;
    }

    snprintf(buffer, BUFFER_LEN, "#!server_ver: %s\n",
                taos_get_server_info(taos));

    char *firstline = strchr(buffer, '\n');

    if (NULL == firstline) {
        return -1;
    }

    int firstreturn = (int)(firstline - buffer);
    size_t len;
    len = fwrite(buffer, 1, firstreturn+1, fp);
    if (len != firstreturn+1) {
        errorPrint("%s() LN%d, write to file. "
                "try to write %d, actual len %zu, "
                "Errno is %d. Reason is %s.\n",
                __func__, __LINE__, firstreturn +1, len,
                errno, strerror(errno));
    }

    char taostools_ver[]   = TD_VER_NUMBER;
    char taosdump_commit[] = TAOSDUMP_COMMIT_ID;

    snprintf(buffer, BUFFER_LEN, "#!"CUS_PROMPT"dump_ver: %s_%s\n",
                taostools_ver, taosdump_commit);
    len = fwrite(buffer, 1, strlen(buffer), fp);
    if (len != strlen(buffer)) {
        errorPrint("%s() LN%d, write to file. "
                "try to write %zd, actual len %zd, "
                "Errno is %d. Reason is %s.\n",
                __func__, __LINE__, strlen(buffer), len,
                    errno, strerror(errno));
    }
#ifdef WINDOWS
    snprintf(buffer, BUFFER_LEN, "#!os_id: Windows\n");
#elif defined(LINUX)
    snprintf(buffer, BUFFER_LEN, "#!os_id: LINUX\n");
#elif defined(DARWIN)
    snprintf(buffer, BUFFER_LEN, "#!os_id: macOS\n");
#else
    snprintf(buffer, BUFFER_LEN, "#!os_id: unknown\n");
#endif
    len = fwrite(buffer, 1, strlen(buffer), fp);
    if (len != strlen(buffer)) {
        errorPrint("%s() LN%d, write to file. "
                "try to write %zd, actual len %zd, "
                "Errno is %d. Reason is %s.\n",
                __func__, __LINE__, strlen(buffer), len,
                    errno, strerror(errno));
    }

    snprintf(buffer, BUFFER_LEN, "#!escape_char: %s\n",
                g_args.escape_char?"true":"false");
    len = fwrite(buffer, 1, strlen(buffer), fp);
    if (len != strlen(buffer)) {
        errorPrint("%s() LN%d, write to file. "
                "try to write %zd, actual len %zd, "
                "Errno is %d. Reason is %s.\n",
                __func__, __LINE__, strlen(buffer), len,
                errno, strerror(errno));
    }

    snprintf(buffer, BUFFER_LEN, "#!loose_mode: %s\n",
                g_args.loose_mode?"true":"false");
    len = fwrite(buffer, 1, strlen(buffer), fp);
    if (len != strlen(buffer)) {
        errorPrint("%s() LN%d, write to file. "
                "try to write %zd, actual len %zd, "
                "Errno is %d. Reason is %s.\n",
                __func__, __LINE__, strlen(buffer), len,
                errno, strerror(errno));
    }

    return 0;
}

static int dumpExtraInfo(void **taos_v, FILE *fp) {
    int ret = 0;

    ret = dumpExtraInfoHead(*taos_v, fp);

    if (ret != 0) {
        return ret;
    }

    dumpExtraInfoVar(*taos_v, fp);

    ret = ferror(fp);

    return ret;
}

static void loadFileMark(FILE *fp, char *mark, char *fcharset) {
    char *line = NULL;
    ssize_t size;
    int markLen = strlen(mark);

    (void)fseek(fp, 0, SEEK_SET);

    do {
#ifdef WINDOWS
        line = calloc(1, MAX_LINE_LENGTH);
        TOOLS_ASSERT(line);
        if (NULL == fgets(line, MAX_LINE_LENGTH, fp)) {
            goto _exit_no_charset;
        }
        size = strlen(line?line:"");
#else
        size_t line_size;
        size = getline(&line, &line_size, fp);
#endif
        if (size <= 2) {
            goto _exit_no_charset;
        }

        if (strncmp(line, mark, markLen) == 0) {
            strncpy(fcharset, line + markLen,
                    (strlen(line) - (markLen+1)));  // remove '\n'
            break;
        }

        tfree(line);
    } while (1);

    tfree(line);
    return;

_exit_no_charset:
    (void)fseek(fp, 0, SEEK_SET);
    *fcharset = '\0';
    tfree(line);
    return;
}

bool convertDbClauseForV3(char **cmd) {
    if (NULL == *cmd) {
        errorPrint("%s() LN%d, **cmd is NULL\n", __func__, __LINE__);
        return false;
    }

    int lenOfCmd = strlen(*cmd);
    if (0 == lenOfCmd) {
        errorPrint("%s() LN%d, length of cmd is 0\n", __func__, __LINE__);
        return false;
    }

    char *dup_str = strdup(*cmd);

    char *running = dup_str;
    char *sub_str = strsep(&running, " ");

    int pos = 0;
    while (sub_str) {
        if ((0 == strncmp(sub_str, "UPDATE1", strlen("UPDATE1")))
                || (0 == strncmp(sub_str, "UPDATE2", strlen("UPDATE2")))) {
            sub_str += strlen("UPDATE1");
        }

        if (0 == strcmp(sub_str, "QUORUM")) {
            sub_str = strsep(&running, " ");
        } else if (0 == strcmp(sub_str, "DAYS")) {
            sub_str = strsep(&running, " ");
            pos += sprintf(*cmd + pos, "DURATION %dm ", atoi(sub_str)*24*60);
        } else if (0 == strcmp(sub_str, "CACHE")) {
            sub_str = strsep(&running, " ");
        } else if (0 == strcmp(sub_str, "UPDATE")) {
            sub_str = strsep(&running, " ");
        } else if (0 == strcmp(sub_str, "BLOCKS")) {
            sub_str = strsep(&running, " ");
        } else if (0 == strcmp(sub_str, "FSYNC")) {
            sub_str = strsep(&running, " ");
        } else {
            pos += sprintf(*cmd + pos, "%s ", sub_str);
        }

        sub_str = strsep(&running, " ");
    }

    free(dup_str);
    return true;
}

static int queryDbImplNative(TAOS *taos, char *command) {
    int ret = 0;
    TAOS_RES *res = NULL;
    int32_t   code = -1;

    res = taosQuery(taos, command, &code);

    if (code != 0) {
        errorPrint("Failed to run <%s>, reason: %s\n",
                command, taos_errstr(res));
        ret = -1;
    }

    taos_free_result(res);
    return ret;
}

// dumpIn support multi threads functions
static int64_t dumpInOneDebugFile(
        void** taos_v, FILE* fp,
        char* fcharset,
        char* fileName) {
    int       read_len = 0;
    char *    cmd      = NULL;
    size_t    cmd_len  = 0;
    char *    line     = NULL;

    cmd  = (char *)malloc(TSDB_MAX_ALLOWED_SQL_LEN);
    if (cmd == NULL) {
        errorPrint("%s() LN%d, failed to allocate memory\n",
                __func__, __LINE__);
        return -1;
    }

    int lastRowsPrint = 5000000;
    int64_t lineNo = 0;
    int64_t success = 0;
    int64_t failed = 0;
#ifdef WINDOWS
    line = calloc(1, TSDB_MAX_ALLOWED_SQL_LEN);
    TOOLS_ASSERT(line);
    while (fgets(line, TSDB_MAX_ALLOWED_SQL_LEN, fp) != NULL) {
        read_len = strlen(line?line:"");
#else
    size_t line_len;
    while ((read_len = getline(&line, &line_len, fp)) != -1) {
#endif  // WINDOWS
        ++lineNo;
        // if (read_len == 0 || isCommentLine(line)) {  // line starts with #
        if (read_len == 0) {
            continue;
        }
        if (line[0] == '#') {
            continue;
        }
        for (ssize_t n = 0; n < read_len; n++) {
             if (('\r' == line[n]) || ('\n' == line[n])) {
                 line[n] = '\0';
             }
        }
        read_len = strlen(line);
        if (0 == read_len) {
            continue;
        }
        if (read_len >= TSDB_MAX_ALLOWED_SQL_LEN) {
            errorPrint("the No.%"PRId64" line is exceed "
                    "max allowed SQL length!\n", lineNo);
            debugPrint("%s() LN%d, line: %s", __func__, __LINE__, line);
            continue;
        }
        if (line[read_len] == '\\') {
            line[read_len] = ' ';
            memcpy(cmd + cmd_len, line, read_len);
            cmd_len += read_len;
            continue;
        }
        memcpy(cmd + cmd_len, line, read_len);
        cmd[read_len + cmd_len]= '\0';
        bool isInsert = (0 == strncmp(cmd, "INSERT ", strlen("INSERT ")));
        bool isCreateDb = (0 == strncmp(cmd,
                    "CREATE DATABASE ", strlen("CREATE DATABASE ")));
        if (isCreateDb && (0 == strncmp(g_dumpInServerVer, "2.", 2))) {
            if (3 == g_majorVersionOfClient) {
                convertDbClauseForV3(&cmd);
            }
        }

        int ret;
        char *newSql = NULL;

        if(g_args.renameHead) {
            // have rename database options
            newSql = afterRenameSql(cmd);
        }

        debugPrint("%s() LN%d, cmd: %s\n", __func__, __LINE__, cmd);
        ret = queryDbImplNative(*taos_v, newSql?newSql:cmd);
        // free
        if (newSql) {
            free(newSql);
            newSql = NULL;
        }

        if (ret) {
            errorPrint("%s() LN%d, SQL: lineno:%"PRId64", file:%s\n",
                    __func__, __LINE__, lineNo, fileName);
            fprintf(g_fpOfResult, "SQL: lineno:%"PRId64", file:%s\n",
                    lineNo, fileName);
            if (isInsert)
                failed++;
        } else {
            if (isInsert)
                success++;
        }

        memset(cmd, 0, TSDB_MAX_ALLOWED_SQL_LEN);
        cmd_len = 0;

        if (lineNo >= lastRowsPrint) {
            infoPrint(" %"PRId64" lines already be executed from file %s\n",
                    lineNo, fileName);
            lastRowsPrint += 5000000;
        }
    }

    tfree(cmd);
    tfree(line);

    if (success > 0)
        return success;
    return failed;
}

static void* dumpInDebugWorkThreadFp(void *arg) {
    threadInfo *pThreadInfo = (threadInfo*)arg;
    SET_THREAD_NAME("dumpInDebugWorkThrd");
    debugPrint2("[%d] Start to process %"PRId64" files from %"PRId64"\n",
                    pThreadInfo->threadIndex,
                    pThreadInfo->count,
                    pThreadInfo->from);

    for (int64_t i = 0; i < pThreadInfo->count; i++) {
        char sqlFile[MAX_PATH_LEN];
        snprintf(sqlFile, MAX_PATH_LEN, "%s/%s", pThreadInfo->dbPath,
                g_tsDumpInDebugFiles[pThreadInfo->from + i]);

        FILE* fp = openDumpInFile(sqlFile);
        if (NULL == fp) {
            errorPrint("[%d] Failed to open input file: %s\n",
                    pThreadInfo->threadIndex, sqlFile);
            continue;
        }

        int64_t rows = dumpInOneDebugFile(
                &pThreadInfo->taos, fp, g_dumpInCharset,
                sqlFile);

        if (rows > 0) {
            atomic_add_fetch_64(&pThreadInfo->recSuccess, rows);
            okPrint("[%d] Total %"PRId64" line(s) "
                    "command be successfully dumped in file: %s\n",
                    pThreadInfo->threadIndex, rows, sqlFile);
        } else if (rows < 0) {
            atomic_add_fetch_64(&pThreadInfo->recFailed, rows);
            errorPrint("[%d] Total %"PRId64" line(s) "
                    "command failed to dump in file: %s\n",
                    pThreadInfo->threadIndex, rows, sqlFile);
        }
        fclose(fp);
    }

    return NULL;
}

static int dumpInDebugWorkThreads(const char *dbPath) {
    int ret = 0;
    int32_t threads = g_args.thread_num;

    uint64_t sqlFileCount = getFilesNum(dbPath, "sql");
    if (0 == sqlFileCount) {
        debugPrint("No .sql file found in %s\n", dbPath);
        return 0;
    }

    enAVROTYPE avroType = createDumpinList(dbPath, "sql", sqlFileCount);

    threadInfo *pThreadInfo;

    pthread_t *pids = calloc(1, threads * sizeof(pthread_t));
    threadInfo *infos = (threadInfo *)calloc(
            threads, sizeof(threadInfo));
    TOOLS_ASSERT(pids);
    TOOLS_ASSERT(infos);

    int64_t a = sqlFileCount / threads;
    if (a < 1) {
        threads = sqlFileCount;
        a = 1;
    }

    int64_t b = 0;
    if (threads != 0) {
        b = sqlFileCount % threads;
    }

    int64_t from = 0;

    for (int32_t t = 0; t < threads; ++t) {
        pThreadInfo = infos + t;
        pThreadInfo->threadIndex = t;
        pThreadInfo->avroType = avroType;

        pThreadInfo->stbSuccess = 0;
        pThreadInfo->stbFailed = 0;
        pThreadInfo->ntbSuccess = 0;
        pThreadInfo->ntbFailed = 0;
        pThreadInfo->recSuccess = 0;
        pThreadInfo->recFailed = 0;

        strncpy(pThreadInfo->dbPath, dbPath, MAX_DIR_LEN-1);
        pThreadInfo->from = from;
        pThreadInfo->count = (t < b)?a+1:a;
        from += pThreadInfo->count;
        verbosePrint(
                "Thread[%d] takes care sql files total %"PRId64" files"
                " from %"PRId64"\n",
                t, pThreadInfo->count, pThreadInfo->from);

        if (NULL == (pThreadInfo->taos = taosConnect(NULL))) {
            free(infos);
            free(pids);
            return -1;
        }

        if (pthread_create(pids + t, NULL,
                    dumpInDebugWorkThreadFp, (void*)pThreadInfo) != 0) {
            errorPrint("%s() LN%d, thread[%d] failed to start. "
                    "The errno is %d. Reason: %s\n",
                    __func__, __LINE__,
                    pThreadInfo->threadIndex, errno, strerror(errno));
            exit(EXIT_FAILURE);
        }
    }

    for (int32_t i = 0; i < threads; i++) {
        if (pthread_join(pids[i], NULL) != 0) {
            errorPrint("%s() LN%d, thread[%d] failed to join. "
                    "The errno is %d. Reason: %s\n",
                    __func__, __LINE__,
                    i, errno, strerror(errno));
            exit(EXIT_FAILURE);
        }
    }

    for (int32_t t = 0; t < threads; ++t) {
        taos_close(infos[t].taos);
    }

    for (int32_t t = 0; t < threads; ++t) {
        atomic_add_fetch_64(&g_totalDumpInRecSuccess, infos[t].recSuccess);
        atomic_add_fetch_64(&g_totalDumpInRecFailed, infos[t].recFailed);
    }

    free(infos);
    free(pids);

    freeFileList(avroType, sqlFileCount);

    return ret;
}

// compare local and server, if stable or normal table schemal have changed, return DBChange* else return NULL
DBChange *getDBChange(FILE *fp, TAOS* taos) {
    // TODO

    return NULL;
}

static int dumpInDbs(const char *dbPath) {
    void **taos_v = NULL;
    TAOS *taos = NULL;
    if (NULL == (taos = taosConnect(NULL))) {
        return -1;
    }
    taos_v = &taos;
    char dbsSql[MAX_PATH_LEN];
    snprintf(dbsSql, MAX_PATH_LEN, "%s/%s", dbPath, "dbs.sql");

    FILE *fp = openDumpInFile(dbsSql);
    if (NULL == fp) {
        debugPrint("%s() LN%d, failed to open input file %s\n",
                __func__, __LINE__, dbsSql);
        return -1;
    }
    debugPrint("Success Open input file: %s\n", dbsSql);

    char *mark = "#!server_ver: ";
    loadFileMark(fp, mark, g_dumpInServerVer);

    mark = "#!"CUS_PROMPT"dump_ver: ";
    char dumpInTaosdumpVer[64] = {0};
    loadFileMark(fp, mark, dumpInTaosdumpVer);

    g_dumpInDataMajorVer = atoi(dumpInTaosdumpVer);
    g_dumpInDataMinorVer = atoi(dumpInTaosdumpVer+2);
    debugPrint("%s() LN%d, dump in data minor version is: %d\n",
               __func__, __LINE__, g_dumpInDataMinorVer);
#ifdef WINDOWS
    if ((g_dumpInDataMajorVer == 2) && (g_dumpInDataMinorVer < 4)) {
        errorPrint("%s", "The data file dumped by "CUS_PROMPT"dump < 2.4 on Windows "
                   "might be corrupted. "
                   "Please use version 2.4 or up to dump again\n");
        closeTaosConnWrapper(*taos_v);
        return -1;
    }
#endif

    int taosToolsMajorVer = atoi(TD_VER_NUMBER);
    if ((g_dumpInDataMajorVer > 1) && (1 == taosToolsMajorVer)) {
        errorPrint("\tThe data file was generated by version %d\n"
                   "\tCannot be restored by current version: %d\n\n"
                   "\tPlease use a correct version "CUS_PROMPT"dump "
                   "to restore them.\n\n",
                g_dumpInDataMajorVer, taosToolsMajorVer);
        closeTaosConnWrapper(*taos_v);
        fclose(fp);
        return -1;
    }

    mark = "#!escape_char: ";
    loadFileMark(fp, mark, g_dumpInEscapeChar);

    mark = "#!loose_mode: ";
    loadFileMark(fp, mark, g_dumpInLooseMode);
    if ((g_args.loose_mode)
            || (0 == strcasecmp(g_dumpInLooseMode, "true"))) {
        g_dumpInLooseModeFlag = true;
        strcpy(g_escapeChar, "");
    }

    mark = "#!charset: ";
    loadFileMark(fp, mark, g_dumpInCharset);

    int64_t rows = dumpInOneDebugFile(
            taos_v, fp, g_dumpInCharset, dbsSql);

    if (rows > 0) {
        okPrint("Total %"PRId64" line(s) SQL be successfully "
                "dumped in file: %s!\n",
                rows, dbsSql);
    } else if (rows < 0) {
        errorPrint("Total %"PRId64" line(s) SQL failed to dump "
                "in file: %s!\n",
                rows, dbsSql);
        closeTaosConnWrapper(*taos_v);
        fclose(fp);
        return -1;
    }

    // close
    fclose(fp);
    closeTaosConnWrapper(*taos_v);

    return 0;
}


static int dumpInWithDbPath(const char *dbPath) {
    int ret = 0;


    infoPrint("%s(), dump in from %s ...\n", __func__, dbPath);

    if (dumpInDbs(dbPath)) {
        errorPrint("Failed to dump database path: %s in!\n", dbPath);
        return -1;
    }

    // create
    DBChange *pDbChange = createDbChange(dbPath);

    if (g_args.avro) {
        // super table meta
        ret = dumpInAvroWorkThreads(dbPath, "avro-tbtags", pDbChange);
        if (0 == ret) {
            // normal table meta
            ret = dumpInAvroWorkThreads(dbPath, "avro-ntb", pDbChange);

            if (0 == ret) {
                // main folder for super and normal table data
                ret = dumpInAvroWorkThreads(dbPath, "avro", pDbChange);
                // sub folder
                ret = dumpInAvroWorkThreadsSub(dbPath, "avro", pDbChange);
            }
        }
    } else {
        ret = dumpInDebugWorkThreads(dbPath);
    }

    // free
    if (pDbChange) {
        freeDBChange(pDbChange);
    }

    return ret;
}

static int dumpIn() {
    TOOLS_ASSERT(g_args.isDumpIn);

    int ret = 0;
    ret = dumpInWithDbPath(g_args.inpath);
    if(ret) {
        return ret;
    }

#ifdef WINDOWS
    TdDirEntryPtr pDirent;
    TdDirPtr pDir;

    pDir = toolsOpenDir(g_args.inpath);

    if (pDir != NULL) {
        while ((pDirent = toolsReadDir(pDir)) != NULL) {
            char *entryName = toolsGetDirEntryName(pDirent);
            if (strncmp (CUS_PROMPT"dump.", entryName, strlen(CUS_PROMPT"dump."))
                    == 0) {
                char dbPath[MAX_PATH_LEN] = {0};
                snprintf(dbPath, MAX_PATH_LEN, "%s/%s",
                         g_args.inpath, entryName);
#else
    struct dirent *pDirent;
    DIR *pDir;

    pDir = opendir(g_args.inpath);

    if (pDir != NULL) {
        while ((pDirent = readdir(pDir)) != NULL) {
            if (strncmp (CUS_PROMPT"dump.", pDirent->d_name, strlen(CUS_PROMPT"dump."))
                    == 0) {
                char dbPath[MAX_PATH_LEN] = {0};
                snprintf(dbPath, MAX_PATH_LEN, "%s/%s",
                         g_args.inpath, pDirent->d_name);
#endif
                debugPrint("%s() LN%d, will dump from %s\n",
                        __func__, __LINE__, dbPath);
                ret = dumpInWithDbPath(dbPath);
            }
        }
#ifdef WINDOWS
        toolsCloseDir(&pDir);
#else
        closedir(pDir);
#endif
    } else {
        errorPrint("opendir(%s)\n", g_args.inpath);
    }

    return ret;
}

static void dumpTablesOfStbNative(
        threadInfo *pThreadInfo,
        FILE *fp,
        char *dumpFilename) {
    for (int64_t i = pThreadInfo->from;
            i < pThreadInfo->from + pThreadInfo->count; i++) {
        char* tbName = pThreadInfo->tbNameArr[i];
        debugPrint("%s() LN%d, [%d] sub table %"PRId64": name: %s\n",
                __func__, __LINE__,
                pThreadInfo->threadIndex, i, tbName);

        int64_t count;
        if (g_args.avro) {
            count = dumpTable(
                    i,
                    &pThreadInfo->taos,
                    pThreadInfo->dbInfo,
                    true,
                    pThreadInfo->stbName,
                    pThreadInfo->stbDes,
                    tbName,
                    pThreadInfo->precision,
                    dumpFilename,
                    NULL);
        } else {
            count = dumpTable(
                    i,
                    &pThreadInfo->taos,
                    pThreadInfo->dbInfo,
                    true,
                    pThreadInfo->stbName,
                    pThreadInfo->stbDes,
                    tbName,
                    pThreadInfo->precision,
                    NULL,
                    fp);
        }

        // update progress
        atomic_add_fetch_64(&g_tableDone, 1);

        if (count < 0) {
            break;
        } else {
            atomic_add_fetch_64(&g_totalDumpOutRows, count);
        }
    }

    return;
}

static void *dumpTablesOfStbThread(void *arg) {
    threadInfo *pThreadInfo = (threadInfo *)arg;

    debugPrint("dump table from = \t%"PRId64"\n", pThreadInfo->from);
    debugPrint("dump table count = \t%"PRId64"\n", pThreadInfo->count);

    FILE *fp = NULL;
    char dumpFilename[MAX_PATH_LEN] = {0};

    if (g_args.avro) {
        if (0 != generateFilename(enAVRO_TBTAGS, dumpFilename,
                pThreadInfo->dbInfo, pThreadInfo->stbName, pThreadInfo->stbName,
                pThreadInfo->threadIndex)) {
            return NULL;
        }
        debugPrint("%s() LN%d dumpFilename: %s\n",
                __func__, __LINE__, dumpFilename);
    } else {
        if (0 != generateFilename(enAVRO_UNKNOWN, dumpFilename,
                pThreadInfo->dbInfo, pThreadInfo->stbName, pThreadInfo->stbName,
                pThreadInfo->threadIndex)) {
            return NULL;
        }
        fp = fopen(dumpFilename, "w");

        if (fp == NULL) {
            errorPrint("%s() LN%d, failed to open file %s. "
                    "Errno is %d. Reason is %s.\n",
                    __func__, __LINE__, dumpFilename, errno, strerror(errno));
            return NULL;
        }
    }

    dumpTablesOfStbNative(pThreadInfo, fp, dumpFilename);
    if (fp) {
        fclose(fp);
        fp = NULL;
    }

    return NULL;
}

int dumpSTableData(SDbInfo* dbInfo, TableDes* stbDes, char** tbNameArr, int64_t tbCount) {
    int threads = g_args.thread_num;
    int64_t batch = tbCount / threads;
    if (batch < 1) {
        threads = tbCount;
        batch = 1;
    }

    TOOLS_ASSERT(threads);
    int64_t mod = tbCount % threads;

    pthread_t *pids = calloc(1, threads * sizeof(pthread_t));
    threadInfo *infos = calloc(1, threads * sizeof(threadInfo));
    TOOLS_ASSERT(pids);
    TOOLS_ASSERT(infos);

    infoPrint("create %d thread(s) to export data ...\n", threads);
    threadInfo *pThreadInfo;
    for (int32_t i = 0; i < threads; i++) {
        pThreadInfo = infos + i;
        if (NULL == (pThreadInfo->taos = taosConnect(dbInfo->name))) {
            free(pids);
            free(infos);
            return -1;
        }

        pThreadInfo->threadIndex = i;
        pThreadInfo->count = (i < mod) ? batch+1 : batch;
        pThreadInfo->from = (i == 0)?0:
            ((threadInfo *)(infos + i - 1))->from +
            ((threadInfo *)(infos + i - 1))->count;
        pThreadInfo->dbInfo = dbInfo;
        pThreadInfo->precision = getPrecisionByString(dbInfo->precision);
        if (-1 == pThreadInfo->precision) {
            errorPrint("%s() LN%d, get precision error\n", __func__, __LINE__);
            exit(EXIT_FAILURE);
        }

        strcpy(pThreadInfo->stbName, stbDes->name);
        pThreadInfo->stbDes = stbDes;
        pThreadInfo->tbNameArr   = tbNameArr;
        if (pthread_create(pids + i, NULL,
                    dumpTablesOfStbThread, pThreadInfo) != 0) {
            errorPrint("%s() LN%d, thread[%d] failed to start. "
                    "The errno is %d. Reason: %s\n",
                    __func__, __LINE__,
                    pThreadInfo->threadIndex, errno, strerror(errno));
            exit(EXIT_FAILURE);
        }
    }

    for (int32_t i = 0; i < threads; i++) {
        if (pthread_join(pids[i], NULL) != 0) {
            errorPrint("%s() LN%d, thread[%d] failed to join. "
                    "The errno is %d. Reason: %s\n",
                    __func__, __LINE__,
                    i, errno, strerror(errno));
            exit(EXIT_FAILURE);
        }
    }

    infoPrint("super table (%s) dump %"PRId64" child data ok. close taos connections...\n",
            stbDes->name, tbCount);
    for (int32_t i = 0; i < threads; i++) {
        pThreadInfo = infos + i;
        taos_close(pThreadInfo->taos);
    }

    free(pids);
    free(infos);
    return 0;
}

// free names
void freeTbNameArr(char ** tbNameArr, int64_t tbCount) {
    for (int64_t i = 0; i < tbCount; i++) {
        if (tbNameArr[i]) {
            free(tbNameArr[i]);
        }
    }
    free(tbNameArr);
}

// dump stable meta and data by threads
static int64_t dumpStable(
        void **taos_v,
        SDbInfo *dbInfo,
        const char *stbName) {
    // show progress
    int ret = -1;
    infoPrint("start dump out super table data (%s) ...\n", stbName);

    //
    // get super table meta
    //

    // malloc stable des
    TableDes *stbDes = (TableDes *)calloc(1, sizeof(TableDes)
            + sizeof(ColDes) * TSDB_MAX_COLUMNS);
    if (NULL == stbDes) {
        errorPrint("%s() LN%d, memory allocation failed!\n",
                __func__, __LINE__);
        return -1;
    }

    // obtain stable des data

    int32_t colCount = getTableDes(*taos_v, dbInfo->name,
            stbName, stbDes, true);
    if (colCount < 0) {
        errorPrint("%s() LN%d, failed to get stable[%s] schema\n",
               __func__, __LINE__, stbName);
        freeTbDes(stbDes, true);
        exit(-1);
    }
    // show progress
    infoPrint("start dump super table meta (%s) col:%d tags:%d ...\n",
                stbName, stbDes->columns, stbDes->tags);

    // get stable child count
    int64_t tbCount = getTbCountOfStbNative(dbInfo->name, stbName);
    if(tbCount < 0 ) {
        errorPrint("get stable %s failed.", stbName);
        freeTbDes(stbDes, true);
        exit(-1);
    }
    // show progress
    infoPrint("The number of tables of %s is %"PRId64"!\n", stbName, tbCount);
    // set progress to global
    g_tableCount = tbCount;
    g_tableDone  = 0;
    strcpy(g_dbName,  dbInfo->name);
    strcpy(g_stbName, stbName);

    //
    //  dump meta
    //
    char** tbNameArr = (char**)calloc(tbCount, sizeof(char*));
    if (g_args.avro) {
        ret = dumpStableMeta(
                taos_v,
                dbInfo,
                stbDes,
                tbNameArr,
                tbCount);
        if (-1 == ret) {
            errorPrint("%s() LN%d, failed to dump table\n",
                    __func__, __LINE__);
            freeTbNameArr(tbNameArr, tbCount);
            freeTbDes(stbDes, true);
            return -1;
        }
    } else {
        fillTbNameArr(taos_v, tbNameArr, dbInfo, stbName, tbCount);
    }

    if(tbCount <= 0) {
        freeTbNameArr(tbNameArr, tbCount);
        freeTbDes(stbDes, true);

        if (tbCount == 0) {
            infoPrint("super table (%s) no child table, skip dump out.\n", stbName);
            return 0;
        } else {
            infoPrint("super table (%s) get child count failed.\n", stbName);
            return -1;
        }
    }

    //
    //  dump data
    //
    ret = dumpSTableData(dbInfo, stbDes, tbNameArr, tbCount);
    freeTbNameArr(tbNameArr, tbCount);
    freeTbDes(stbDes, true);
    return ret;
}

int64_t dumpStbAndChildTb(
        void **taos_v, SDbInfo *dbInfo, const char *stable, FILE *fpDbs) {
    int64_t ret = 0;

    uint64_t sizeOfTableDes =
        (uint64_t)(sizeof(TableDes) + sizeof(ColDes) * TSDB_MAX_COLUMNS);

    TableDes *stbTableDes = (TableDes *)calloc(1, sizeOfTableDes);

    if (NULL == stbTableDes) {
        errorPrint("%s() LN%d, failed to allocate %"PRIu64" memory\n",
                __func__, __LINE__, sizeOfTableDes);
        exit(-1);
    }

    // dump stable create sql to db.sql
    ret = dumpStableClasuse(
            taos_v,
            dbInfo,
            stable,
            &stbTableDes,
            fpDbs);

    // dump stable data
    if (ret >= 0) {
        ret = dumpStable(
                taos_v,
                dbInfo,
                stable);
    } else {
        errorPrint("%s() LN%d, dumpNtbOfStb(%s) ByThread failed\n",
                __func__, __LINE__,
                stable);
    }

    freeTbDes(stbTableDes, true);

    return ret;
}

static int64_t dumpNTablesOfDb(TAOS **taos_v, SDbInfo *dbInfo) {
    int64_t ret = 0;

    if (0 == dbInfo->ntables) {
        warnPrint("%s() LN%d, database: %s has 0 tables\n",
                __func__, __LINE__, dbInfo->name);
        return 0;
    }

    char *command = calloc(1, TSDB_MAX_ALLOWED_SQL_LEN);
    if (NULL == command) {
        errorPrint("%s() LN%d, memory allocation failed\n", __func__, __LINE__);
        return -1;
    }

    TAOS_RES *res;
    int32_t code = -1;

    snprintf(command, TSDB_MAX_ALLOWED_SQL_LEN,
            "SELECT TABLE_NAME,STABLE_NAME "
            " FROM information_schema.ins_tables WHERE db_name='%s'",
            dbInfo->name);

    res = taosQuery(*taos_v, command, &code);
    if (code != 0) {
        errorPrint("Failed to show %s\'s tables, reason: %s\n",
                dbInfo->name, taos_errstr(res));
        taos_free_result(res);
        free(command);
        return 0;
    }

    TAOS_ROW row;
    int64_t count = 0;
    while ((row = taos_fetch_row(res))) {
        int32_t *lengths = taos_fetch_lengths(res);
        if (lengths[TSDB_SHOW_TABLES_NAME_INDEX] <= 0) {
            errorPrint("%s() LN%d, fetch_row() get %d length!\n",
                    __func__, __LINE__, lengths[TSDB_SHOW_TABLES_NAME_INDEX]);
            continue;
        }

        if (0 != lengths[1]) {
            continue;
        }

        char ntable[TSDB_TABLE_NAME_LEN] = {0};
        strncpy(ntable,
                (char *)row[0],
                lengths[0]);
        ret = dumpTableNotBelong(
                count,
                taos_v, dbInfo, ntable);
        if (0 == ret) {
            okPrint("%s() LN%d, dump normal table: %s\n",
                    __func__, __LINE__, ntable);
        } else {
            errorPrint("%s() LN%d, dump normal table: %s\n",
                    __func__, __LINE__, ntable);
        }

        count++;
    }

    taos_free_result(res);
    free(command);

    return ret;
}

static int64_t dumpStbAndChildTbOfDb(
        TAOS **taos_v, SDbInfo *dbInfo, FILE *fpDbs) {
    int64_t ret = 0;

    char *command = calloc(1, TSDB_MAX_ALLOWED_SQL_LEN);
    if (NULL == command) {
        errorPrint("%s() LN%d, memory allocation failed\n", __func__, __LINE__);
        return -1;
    }

    snprintf(command, TSDB_MAX_ALLOWED_SQL_LEN,
            g_args.db_escape_char
            ? "USE `%s`"
            : "USE %s",
            dbInfo->name);
    TAOS_RES *res;
    int32_t code = -1;

    res = taosQuery(*taos_v, command, &code);
    if (code != 0) {
        errorPrint("Invalid database %s, reason: %s\n",
                dbInfo->name, taos_errstr(res));
        taos_free_result(res);
        free(command);
        return -1;
    }

    taos_free_result(res);

    // get all stable name
    snprintf(command, TSDB_MAX_ALLOWED_SQL_LEN,
            "SELECT STABLE_NAME FROM information_schema.ins_stables "
            "WHERE db_name='%s'",
            dbInfo->name);

    res = taosQuery(*taos_v, command, &code);

    if (code != 0) {
        return cleanIfQueryFailed(__func__, __LINE__, command, res);
    }

    TAOS_ROW row = NULL;

    while ((row = taos_fetch_row(res))) {
        int *lengths = taos_fetch_lengths(res);
        char stable[TSDB_TABLE_NAME_LEN] = {0};
        strncpy(stable, row[0], lengths[0]);
        debugPrint("%s() LN%d, dump stable name: %s\n",
                __func__, __LINE__, stable);

        // dump each stable data
        ret = dumpStbAndChildTb(
                taos_v,
                dbInfo,
                stable,
                fpDbs);
        if (ret < 0) {
            errorPrint("%s() LN%d, stable: %s dump out failed\n",
                    __func__, __LINE__, stable);
            break;
        }
    }

    taos_free_result(res);
    free(command);

    return ret;
}

static int createDirForDbDump(SDbInfo *dbInfo) {
    if (g_args.loose_mode) {
        snprintf(dbInfo->dirForDbDump, MAX_DIR_LEN, "%s"CUS_PROMPT"dump.%s",
                g_args.outpath, dbInfo->name);
    } else {
        dbInfo->uniqueID = getUniqueIDFromEpoch();
        snprintf(dbInfo->dirForDbDump, MAX_DIR_LEN, "%s"CUS_PROMPT"dump.%"PRId64"",
                g_args.outpath, dbInfo->uniqueID);
    }

    int ret = mkdir(dbInfo->dirForDbDump, 0775);
    if (ret) {
        if (EEXIST == errno) {
            ret = 0;
        } else {
            errorPrint("%s() LN%d, mkdir(%s) failed. Errno: %d\n",
                    __func__, __LINE__, dbInfo->dirForDbDump, errno);
        }
    }

    return ret;
}

static FILE *createDbsSqlPerDb(SDbInfo *dbInfo) {
    FILE *fpDbs = NULL;
    char dumpDbsSql[MAX_PATH_LEN] = {0};
    if (AVRO_CODEC_UNKNOWN != g_args.avro_codec) {
        if (0 != createDirForDbDump(dbInfo)) {
            return NULL;
        }
        snprintf(dumpDbsSql, MAX_PATH_LEN, "%s/dbs.sql", dbInfo->dirForDbDump);

        fpDbs = fopen(dumpDbsSql, "w");

        if (NULL == fpDbs) {
            errorPrint("%s() LN%d, failed to open file %s. "
                    "Errno is %d. Reason is %s.\n",
                    __func__, __LINE__, dumpDbsSql, errno, strerror(errno));
        }
    } else {
        snprintf(dumpDbsSql, MAX_PATH_LEN, "%sdbs.sql", g_args.outpath);
    }
    return fpDbs;
}

static int64_t dumpWholeDatabase(void **taos_v, SDbInfo *dbInfo, FILE *fp) {
    int64_t ret;
    infoPrint("Start to dump out database: %s\n", dbInfo->name);

    fprintf(fp, "#!dumpdb: %s: %s\n\n", dbInfo->name, dbInfo->dirForDbDump);

    // fpDbs is dbs.sql for second level for db
    FILE *fpDbs;
    if (AVRO_CODEC_UNKNOWN == g_args.avro_codec) {
        fpDbs = fp;
    } else {
        fpDbs = createDbsSqlPerDb(dbInfo);
        if (NULL == fpDbs) {
            return -1;
        }
    }

    // write db to dbs.sql
    dumpCreateDbClause(taos_v, dbInfo, g_args.with_property, fpDbs);

    fprintf(g_fpOfResult, "\n#### database:                       %s\n",
            dbInfo->name);
    atomic_add_fetch_64(
            &g_resultStatistics.totalDatabasesOfDumpOut, 1);


    // write stb and child data
    ret = dumpStbAndChildTbOfDb(taos_v, dbInfo, fpDbs);
    if (ret >= 0) {
        // wirte normal table data
        ret = dumpNTablesOfDb(taos_v, dbInfo);
    }
    if (AVRO_CODEC_UNKNOWN != g_args.avro_codec) {
        fclose(fpDbs);
    }

    return ret;
}

static bool checkFileExists(char *path, char *filename) {
    char filePath[MAX_PATH_LEN] = {0};
    if (strlen(path)) {
        snprintf(filePath, MAX_PATH_LEN, "%s/%s", path, filename);
    } else {
        snprintf(filePath, MAX_PATH_LEN, "%s/%s", ".", filename);
    }

    if (access(filePath, F_OK) == 0) {
        return true;
    }

    return false;
}

static bool checkFileExistsDir(char *path, char *dirname) {
    bool bRet = false;

    int namelen, dirlen;
#ifdef WINDOWS
    TdDirEntryPtr pDirent;
    TdDirPtr pDir;

    dirlen = strlen(dirname);
    pDir = toolsOpenDir(path);

    if (pDir != NULL) {
        while ((pDirent = toolsReadDir(pDir)) != NULL) {
            char *entryName = toolsGetDirEntryName(pDirent);
            namelen = strlen(entryName);
            if (namelen > dirlen) {
                if (strncmp(dirname, entryName, dirlen) == 0) {
                    bRet = true;
                    break;
                }
            }
        }
        toolsCloseDir(&pDir);
    }
#else

    struct dirent *pDirent;
    DIR *pDir;

    dirlen = strlen(dirname);
    pDir = opendir(path);

    if (pDir != NULL) {
        while ((pDirent = readdir(pDir)) != NULL) {
            namelen = strlen(pDirent->d_name);
            if (namelen > dirlen) {
                if (strncmp(dirname, pDirent->d_name, dirlen) == 0) {
                    bRet = true;
                    break;
                }
            }
        }
        closedir(pDir);
    }
#endif

    return bRet;
}

static bool checkFileExistsExt(char *path, char *ext) {
    bool bRet = false;

    int namelen, extlen;
    extlen = strlen(ext);

#ifdef WINDOWS
    TdDirEntryPtr pDirent;
    TdDirPtr pDir;

    pDir = toolsOpenDir(path);

    if (pDir != NULL) {
        while ((pDirent = toolsReadDir(pDir)) != NULL) {
            char *entryName = toolsGetDirEntryName(pDirent);
            namelen = strlen(entryName);
            if (namelen > extlen) {
                if (strcmp(ext, &(entryName[namelen - extlen])) == 0) {
                    bRet = true;
                    break;
                }
            }
        }
        toolsCloseDir(&pDir);
    }
#else
    struct dirent *pDirent;
    DIR *pDir;

    pDir = opendir(path);

    if (pDir != NULL) {
        while ((pDirent = readdir(pDir)) != NULL) {
            namelen = strlen(pDirent->d_name);
            if (namelen > extlen) {
                if (strcmp(ext, &(pDirent->d_name[namelen - extlen])) == 0) {
                    bRet = true;
                    break;
                }
            }
        }
        closedir(pDir);
    }
#endif

    return bRet;
}

static bool checkOutDir(char *outpath) {
    bool ret = true;
    TdDirPtr pDir = NULL;

    if (strlen(outpath)) {
        if (NULL == (pDir= toolsOpenDir(outpath))) {
            errorPrint("%s is not exist!\n", outpath);
            return false;
        }
    } else {
        outpath = ".";
    }

    if ((checkFileExists(outpath, "dbs.sql"))
            || (checkFileExistsDir(outpath, CUS_PROMPT"dump."))
            || (checkFileExistsExt(outpath, "avro-tbstb"))
            || (checkFileExistsExt(outpath, "avro-ntb"))
            || (checkFileExistsExt(outpath, "avro"))) {
        if (strlen(outpath)) {
            errorPrint("Found data file(s) exists in %s!"
                    " Please use other place to dump out!\n",
                    outpath);
        } else {
            errorPrint("Found data file(s) exists in %s!"
                    " Please use other place to dump out!\n",
                    "current path");
        }
        ret = false;
    }

    if (pDir) {
        toolsCloseDir(&pDir);
    }

    return ret;
}

static bool fillDBInfoWithFieldsNative(const int index,
        const TAOS_FIELD *fields, const TAOS_ROW row,
        const int *lengths, int fieldCount) {
    for (int f = 0; f < fieldCount; f++) {
        if (0 == strcmp(fields[f].name, "name")) {
            TOOLS_STRNCPY(g_dbInfos[index]->name, (char*)(row[f]),
                    min(TSDB_DB_NAME_LEN, lengths[f]+1));
            debugPrint("%s() LN%d, db name: %s, len: %d\n",
                    __func__, __LINE__,
                    g_dbInfos[index]->name, lengths[f]);
        } else if (0 == strcmp(fields[f].name, "vgroups")) {
            if (TSDB_DATA_TYPE_INT == fields[f].type) {
                g_dbInfos[index]->vgroups = *((int32_t *)row[f]);
            } else if (TSDB_DATA_TYPE_SMALLINT == fields[f].type) {
                g_dbInfos[index]->vgroups = *((int16_t *)row[f]);
            } else {
                errorPrint("%s() LN%d, unexpected type: %d\n",
                        __func__, __LINE__, fields[f].type);
                return false;
            }
        } else if (0 == strcmp(fields[f].name, "ntables")) {
            if (TSDB_DATA_TYPE_INT == fields[f].type) {
                g_dbInfos[index]->ntables = *((int32_t *)row[f]);
            } else if (TSDB_DATA_TYPE_BIGINT == fields[f].type) {
                g_dbInfos[index]->ntables = *((int64_t *)row[f]);
            } else {
                errorPrint("%s() LN%d, unexpected type: %d\n",
                        __func__, __LINE__, fields[f].type);
                return false;
            }
        } else if (0 == strcmp(fields[f].name, "replica")) {
            if (TSDB_DATA_TYPE_TINYINT == fields[f].type) {
                g_dbInfos[index]->replica = *((int8_t *)row[f]);
            } else if (TSDB_DATA_TYPE_SMALLINT == fields[f].type) {
                g_dbInfos[index]->replica = *((int16_t *)row[f]);
            } else {
                errorPrint("%s() LN%d, unexpected type: %d\n",
                        __func__, __LINE__, fields[f].type);
                return false;
            }
        } else if (0 == strcmp(fields[f].name, "strict")) {
            TOOLS_STRNCPY(g_dbInfos[index]->strict,
                    (char*)row[f], min(STRICT_LEN, lengths[f]+1));
            debugPrint("%s() LN%d: field: %d, keep: %s, length:%d\n",
                    __func__, __LINE__, f,
                    g_dbInfos[index]->strict,
                    lengths[f]);
        } else if (0 == strcmp(fields[f].name, "quorum")) {
            g_dbInfos[index]->quorum =
                *((int16_t *)row[f]);
        } else if (0 == strcmp(fields[f].name, "days")) {
            g_dbInfos[index]->days = *((int16_t *)row[f]);
        } else if ((0 == strcmp(fields[f].name, "keep"))
                || (0 == strcmp(fields[f].name, "keep0,keep1,keep2"))) {
            TOOLS_STRNCPY(g_dbInfos[index]->keeplist, (char*)row[f],
                    min(KEEPLIST_LEN, lengths[f]+1));
            debugPrint("%s() LN%d: field: %d, keep: %s, length:%d\n",
                    __func__, __LINE__, f,
                    g_dbInfos[index]->keeplist,
                    lengths[f]);
        } else if (0 == strcmp(fields[f].name, "duration")) {
            TOOLS_STRNCPY(g_dbInfos[index]->duration, (char*) row[f],
                    min(DURATION_LEN, lengths[f]+1));
            debugPrint("%s() LN%d: field: %d, duration: %s, length:%d\n",
                    __func__, __LINE__, f,
                    g_dbInfos[index]->duration,
                    lengths[f]);
        } else if ((0 == strcmp(fields[f].name, "cache"))
                || (0 == strcmp(fields[f].name, "cache(MB)"))) {
            g_dbInfos[index]->cache = *((int32_t *)row[f]);
        } else if (0 == strcmp(fields[f].name, "blocks")) {
            g_dbInfos[index]->blocks = *((int32_t *)row[f]);
        } else if (0 == strcmp(fields[f].name, "minrows")) {
            if (TSDB_DATA_TYPE_INT == fields[f].type) {
                g_dbInfos[index]->minrows = *((int32_t *)row[f]);
            } else {
                errorPrint("%s() LN%d, unexpected type: %d\n",
                        __func__, __LINE__, fields[f].type);
                return false;
            }
        } else if (0 == strcmp(fields[f].name, "maxrows")) {
            if (TSDB_DATA_TYPE_INT == fields[f].type) {
                g_dbInfos[index]->maxrows = *((int32_t *)row[f]);
            } else {
                errorPrint("%s() LN%d, unexpected type: %d\n",
                        __func__, __LINE__, fields[f].type);
                return false;
            }
        } else if (0 == strcmp(fields[f].name, "wallevel")) {
            g_dbInfos[index]->wallevel = *((int8_t *)row[f]);
        } else if (0 == strcmp(fields[f].name, "wal")) {
            if (TSDB_DATA_TYPE_TINYINT == fields[f].type) {
                g_dbInfos[index]->wal = *((int8_t *)row[f]);
            } else {
                errorPrint("%s() LN%d, unexpected type: %d\n",
                        __func__, __LINE__, fields[f].type);
                return false;
            }
        } else if (0 == strcmp(fields[f].name, "fsync")) {
            if (TSDB_DATA_TYPE_INT == fields[f].type) {
                g_dbInfos[index]->fsync = *((int32_t *)row[f]);
            } else {
                errorPrint("%s() LN%d, unexpected type: %d\n",
                        __func__, __LINE__, fields[f].type);
                return false;
            }
        } else if (0 == strcmp(fields[f].name, "comp")) {
            if (TSDB_DATA_TYPE_TINYINT == fields[f].type) {
                g_dbInfos[index]->comp = (int8_t)(*((int8_t *)row[f]));
            } else {
                errorPrint("%s() LN%d, unexpected type: %d\n",
                        __func__, __LINE__, fields[f].type);
                return false;
            }
        } else if (0 == strcmp(fields[f].name, "cachelast")) {
            if (TSDB_DATA_TYPE_TINYINT == fields[f].type) {
                g_dbInfos[index]->cachelast = (int8_t)(*((int8_t *)row[f]));
            } else {
                errorPrint("%s() LN%d, unexpected type: %d\n",
                        __func__, __LINE__, fields[f].type);
                return false;
            }
        } else if (0 == strcmp(fields[f].name, "cache_model")) {
            if (TSDB_DATA_TYPE_TINYINT == fields[f].type) {
                g_dbInfos[index]->cache_model = (int8_t)(*((int8_t*)row[f]));
            } else {
                errorPrint("%s() LN%d, unexpected type: %d\n",
                        __func__, __LINE__, fields[f].type);
                return false;
            }
        } else if (0 == strcmp(fields[f].name, "single_stable_model")) {
            if (TSDB_DATA_TYPE_BOOL == fields[f].type) {
                g_dbInfos[index]->single_stable_model
                             = (bool)(*((bool*)row[f]));
            } else {
                errorPrint("%s() LN%d, unexpected type: %d\n",
                        __func__, __LINE__, fields[f].type);
                return false;
            }
        } else if (0 == strcmp(fields[f].name, "precision")) {
            TOOLS_STRNCPY(g_dbInfos[index]->precision, (char*)row[f],
                    min(DB_PRECISION_LEN, lengths[f]+1));
            debugPrint("%s() LN%d, db precision: %s, len: %d\n",
                    __func__, __LINE__,
                    g_dbInfos[index]->precision, lengths[f]);
        } else if (0 == strcmp(fields[f].name, "update")) {
            g_dbInfos[index]->update = *((int8_t *)row[f]);
        }
    }

    return true;
}

static int fillDbExtraInfoV3Native(
        void *taos,
        const char *dbName,
        const int dbIndex) {
    int ret = 0;
    char *command = calloc(1, TSDB_MAX_ALLOWED_SQL_LEN);
    if (NULL == command) {
        errorPrint("%s() LN%d, memory allocation failed\n", __func__, __LINE__);
        return -1;
    }
    snprintf(command, TSDB_MAX_ALLOWED_SQL_LEN,
            "SELECT COUNT(table_name) FROM "
            "information_schema.ins_tables WHERE db_name='%s'",
            dbName);

    infoPrint("Getting table(s) count of db (%s) ...\n", dbName);

    int32_t code = -1;
    TAOS_RES *res = taosQuery(taos, command, &code);
    if (code != 0) {
        return cleanIfQueryFailed(__func__, __LINE__, command, res);
    } else {
        TAOS_ROW row;
        TAOS_FIELD *fields = taos_fetch_fields(res);

        while ((row = taos_fetch_row(res)) != NULL) {
            if (TSDB_DATA_TYPE_BIGINT == fields[0].type) {
                g_dbInfos[dbIndex]->ntables = *(int64_t *)row[0];
            } else {
                errorPrint("%s() LN%d, type: %d, not converted\n",
                        __func__, __LINE__, fields[0].type);
            }
        }
    }

    taos_free_result(res);
    free(command);
    return ret;
}

static int fillDbInfoNative(void *taos) {
    int ret = 0;
    int dbIndex = 0;

    char *command = calloc(1, TSDB_MAX_ALLOWED_SQL_LEN);
    if (NULL == command) {
        errorPrint("%s() LN%d, memory allocation failed\n", __func__, __LINE__);
        return -1;
    }

    snprintf(command, TSDB_MAX_ALLOWED_SQL_LEN,
                "SELECT * FROM information_schema.ins_databases");

    int32_t code = -1;
    TAOS_RES *res = taosQuery(taos, command, &code);
    if (code != 0) {
        return cleanIfQueryFailed(__func__, __LINE__, command, res);
    } else {
        TAOS_ROW row;
        TAOS_FIELD *fields = taos_fetch_fields(res);
        int fieldCount = taos_field_count(res);

        while ((row = taos_fetch_row(res)) != NULL) {
            int32_t* lengths = taos_fetch_lengths(res);
            if (lengths[TSDB_SHOW_DB_NAME_INDEX] <= 0) {
                errorPrint("%s() LN%d, fetch_row() get %d length!\n",
                        __func__, __LINE__, lengths[TSDB_SHOW_DB_NAME_INDEX]);
                continue;
            }

            char dbName[TSDB_DB_NAME_LEN] = {0};
            strncpy(dbName, row[TSDB_SHOW_DB_NAME_INDEX],
                    lengths[TSDB_SHOW_DB_NAME_INDEX]);
            if (isSystemDatabase(dbName)) {
                if (!g_args.allow_sys) {
                    continue;
                }
            } else if (g_args.databases) {
                if (inDatabasesSeq(dbName) != 0) {
                    continue;
                }
            } else if (!g_args.all_databases) {
                if (strcmp(g_args.arg_list[0], dbName)) {
                    continue;
                }
            }

            g_dbInfos[dbIndex] = (SDbInfo *)calloc(1, sizeof(SDbInfo));
            if (NULL == g_dbInfos[dbIndex]) {
                errorPrint("%s() LN%d, failed to allocate %"PRIu64" memory\n",
                        __func__, __LINE__, (uint64_t)sizeof(SDbInfo));
                ret = -1;
                break;
            }

            okPrint("Database: %s exists\n", dbName);

            if (false == fillDBInfoWithFieldsNative(dbIndex,
                        fields, row, lengths, fieldCount)) {
                ret = -1;
                break;
            }

            fillDbExtraInfoV3Native(taos, dbName, dbIndex);

            dbIndex++;

            if (g_args.databases) {
                if (dbIndex > g_args.dumpDbCount)
                    break;
            } else if (!g_args.all_databases) {
                if (dbIndex >= 1)
                    break;
            }
        }
    }

    taos_free_result(res);
    free(command);

    if (0 != ret) {
        return ret;
    }

    return dbIndex;
}

static int dumpOut() {
    int ret = 0;
    TAOS     *taos       = NULL;

    FILE *fp = NULL;
    FILE *fpDbs = NULL;
    int32_t dbCount = 0;

    if (false == checkOutDir(g_args.outpath)) {
        return -1;
    }

    char dumpFilename[MAX_PATH_LEN] = {0};
    snprintf(dumpFilename, MAX_PATH_LEN, "%sdbs.sql", g_args.outpath);

    // fp is dbs.sql on top level
    fp = fopen(dumpFilename, "w");
    if (fp == NULL) {
        errorPrint("%s() LN%d, failed to open file %s. "
                "Errno is %d. Reason is %s.\n",
                __func__, __LINE__, dumpFilename, errno, strerror(errno));
        return -1;
    }

    g_args.dumpDbCount = getDumpDbCount();
    debugPrint("%s() LN%d, dump db count: %d\n",
            __func__, __LINE__, g_args.dumpDbCount);

    if (g_args.dumpDbCount <= 0) {
        errorPrint("%d database(s) valid to dump\n", g_args.dumpDbCount);
        fclose(fp);
        return -1;
    }

    g_dbInfos = (SDbInfo **)calloc(g_args.dumpDbCount, sizeof(SDbInfo *));
    if (NULL == g_dbInfos) {
        errorPrint("%s() LN%d, failed to allocate memory\n",
                __func__, __LINE__);
        ret = -1;
        goto _exit_failure_2;
    }

    /* Connect to server and dump extra info*/
    void **taos_v = NULL;
    if (NULL == (taos = taosConnect(NULL))) {
        ret = -1;
        goto _exit_failure;
    }

    taos_v = &taos;
    ret = dumpExtraInfo(taos_v, fp);

    if (ret < 0) {
        goto _exit_failure;
    }

    dbCount = fillDbInfoNative(taos);

    if (dbCount <= 0) {
        errorPrint("%d database(s) valid to dump\n", dbCount);
        ret = -1;
        goto _exit_failure;
    }

    // case: taosdump --databases dbx,dby ...   OR  taosdump --all-databases
    if (g_args.databases || g_args.all_databases) {
        // -A option for all database
        for (int i = 0; i < dbCount; i++) {
            int64_t records = 0;
            records = dumpWholeDatabase(taos_v, g_dbInfos[i], fp);
            if (records >= 0) {
                okPrint("Database %s dumped\n", g_dbInfos[i]->name);
                g_totalDumpOutRows += records;
            }
        }
    } else {
        if (1 == g_args.arg_list_len) {
            // -D option specify database
            int64_t records = dumpWholeDatabase(taos_v, g_dbInfos[0], fp);
            if (records >= 0) {
                okPrint("Database %s dumped\n", g_dbInfos[0]->name);
                g_totalDumpOutRows += records;
            }
        } else {
            if (AVRO_CODEC_UNKNOWN == g_args.avro_codec) {
                dumpCreateDbClause(taos_v, g_dbInfos[0],
                        g_args.with_property, fp);
            } else {
                fpDbs = createDbsSqlPerDb(g_dbInfos[0]);
                if (fpDbs) {
                    dumpCreateDbClause(taos_v, g_dbInfos[0],
                            g_args.with_property, fpDbs);
                } else {
                    fclose(fp);
                    return -1;
                }
            }
        }

        int superTblCnt = 0;
        // specify table
        for (int64_t i = 1; g_args.arg_list[i]; i++) {
            if (0 == strlen(g_args.arg_list[i])) {
                continue;
            }
            TableRecordInfo tableRecordInfo;

            if (getTableRecordInfo(g_dbInfos[0]->name,
                        g_args.arg_list[i],
                        &tableRecordInfo) < 0) {
                errorPrint("input the invalid table %s\n",
                        g_args.arg_list[i]);
                continue;
            }

            if (tableRecordInfo.isStb) {  // dump all table of this stable
                // super table
                ret = dumpStbAndChildTb(taos_v, g_dbInfos[0],
                        tableRecordInfo.tableRecord.stable,
                        (g_args.avro_codec == AVRO_CODEC_UNKNOWN)?fp:fpDbs);
                if (ret < 0) {
                    errorPrint("%s() LN%d, dump %s and its child table\n",
                            __func__, __LINE__, g_args.arg_list[i]);
                }
            } else if (tableRecordInfo.belongStb) {
                // child table
                uint64_t sizeOfTableDes =
                    (uint64_t)(sizeof(TableDes)
                            + sizeof(ColDes) * TSDB_MAX_COLUMNS);

                TableDes *stbTableDes = (TableDes *)calloc(1, sizeOfTableDes);
                if (NULL == stbTableDes) {
                    errorPrint("%s() LN%d, failed to allocate "
                            "%"PRIu64" memory\n",
                            __func__, __LINE__, sizeOfTableDes);
                    exit(-1);
                }

                // child table's super table meta
                ret = dumpStableClasuse(
                        taos_v,
                        g_dbInfos[0],
                        tableRecordInfo.tableRecord.stable,
                        &stbTableDes,
                        (g_args.avro_codec == AVRO_CODEC_UNKNOWN)?fp:fpDbs);
                if (ret >= 0) {
                    superTblCnt++;
                } else {
                    errorPrint("%s() LN%d, dumpStableClasuse(%s) failed\n",
                            __func__, __LINE__,
                            tableRecordInfo.tableRecord.stable);
                }
                // child table data
                ret = dumpTableBelongStb(
                        i,
                        taos_v,
                        g_dbInfos[0],
                        tableRecordInfo.tableRecord.stable,
                        stbTableDes,
                        g_args.arg_list[i]);
                if (ret >= 0) {
                    okPrint("%s() LN%d, "
                            "dumpTableBelongStb(%s) success\n",
                            __func__, __LINE__,
                            tableRecordInfo.tableRecord.stable);
                } else {
                    errorPrint("%s() LN%d, "
                               "dumpTableBelongStb(%s) failed\n",
                            __func__, __LINE__,
                            tableRecordInfo.tableRecord.stable);
                }
                freeTbDes(stbTableDes, true);
            } else {
                // normal table
                ret = dumpTableNotBelong(
                        i,
                        taos_v, g_dbInfos[0], g_args.arg_list[i]);
            }

            if (ret >= 0) {
                okPrint("table: %s dumped\n", g_args.arg_list[i]);
            }
        }
    }

    /* Close the handle and return */
    ret = 0;

_exit_failure:
    if(taos_v && *taos_v) {
        closeTaosConnWrapper(*taos_v);
    }

_exit_failure_2:
    freeDbInfos();
    if (fpDbs) {
        fclose(fpDbs);
    }
    fclose(fp);
    if (0 == ret) {
        okPrint("%" PRId64 " row(s) dumped out!\n",
                g_totalDumpOutRows);
        atomic_add_fetch_64(
                &g_resultStatistics.totalRowsOfDumpOut,
                g_totalDumpOutRows);
    } else {
        errorPrint("%" PRId64 " row(s) dumped out!\n",
                g_totalDumpOutRows);
    }

    return ret;
}

static int dumpEntry() {
    int ret = 0;

    if (checkParam() < 0) {
        exit(EXIT_FAILURE);
    }

    printArgs(g_fpOfResult);

    for (int32_t i = 0; i < g_args.arg_list_len; i++) {
        fprintf(g_fpOfResult, "arg_list[%d]: %s\n", i, g_args.arg_list[i]);
    }
    fprintf(g_fpOfResult, "debug_print: %d\n", g_args.debug_print);

    g_numOfCores = toolsGetNumberOfCores();

    time_t tTime = time(NULL);
    struct tm tm = *localtime(&tTime);
    printf("start time: %d-%02d-%02d %02d:%02d:%02d\n",
            tm.tm_year + 1900, tm.tm_mon + 1,
            tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);

    taos_options(TSDB_OPTION_CONFIGDIR, g_configDir);

    char *unit;
    if (AVRO_CODEC_UNKNOWN == g_args.avro_codec) {
        unit = "line(s)";
    } else {
        unit = "row(s)";
    }

    if (g_args.isDumpIn) {
        fprintf(g_fpOfResult, "========== DUMP IN ========== \n");
        fprintf(g_fpOfResult, "# DumpIn start time: "
                "%d-%02d-%02d %02d:%02d:%02d\n",
                tm.tm_year + 1900, tm.tm_mon + 1,
                tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);
        int dumpInRet = dumpIn();
        if (dumpInRet) {
            errorPrint("%s\n", "dumpIn() failed!");
            okPrint("%"PRId64" %s dumped in!\n",
                    g_totalDumpInRecSuccess, unit);
            errorPrint("%"PRId64" failures occurred to dump in!\n",
                    g_totalDumpInRecFailed * -1);
            ret = -1;
        } else {
            if (g_totalDumpInRecFailed < 0) {
                if (g_totalDumpInRecSuccess > 0) {
                    okPrint("%"PRId64" %s dumped in!\n",
                            g_totalDumpInRecSuccess, unit);
                }
                errorPrint("%"PRId64" failures occurred to dump in!\n",
                        g_totalDumpInRecFailed * -1);
            } else {
                okPrint("%"PRId64" %s dumped in!\n",
                        g_totalDumpInRecSuccess, unit);
            }
            ret = 0;
        }
    } else {
        fprintf(g_fpOfResult, "========== DUMP OUT ========== \n");
        fprintf(g_fpOfResult, "# DumpOut start time: "
                "%d-%02d-%02d %02d:%02d:%02d\n",
                tm.tm_year + 1900, tm.tm_mon + 1,
                tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);

        if (dumpOut() < 0) {
            ret = -1;
        }

        fprintf(g_fpOfResult, "\n============================== "
                "TOTAL STATISTICS ============================== \n");
        fprintf(g_fpOfResult, "# total database count:     %"PRId64"\n",
                g_resultStatistics.totalDatabasesOfDumpOut);
        fprintf(g_fpOfResult, "# total super table count:  %"PRId64"\n",
                g_resultStatistics.totalSuperTblsOfDumpOut);
        fprintf(g_fpOfResult, "# total child table count:  %"PRId64"\n",
                g_resultStatistics.totalChildTblsOfDumpOut);
        fprintf(g_fpOfResult, "# total row count:          %"PRId64"\n",
                g_resultStatistics.totalRowsOfDumpOut);
    }

    // end time
    tTime = time(NULL);
    tm = *localtime(&tTime);
    printf("end time: %d-%02d-%02d %02d:%02d:%02d\n",
            tm.tm_year + 1900, tm.tm_mon + 1,
            tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);

    fprintf(g_fpOfResult, "end time: %d-%02d-%02d %02d:%02d:%02d\n",
            tm.tm_year + 1900, tm.tm_mon + 1,
            tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);

    fprintf(g_fpOfResult, "\n");
    fclose(g_fpOfResult);

    if (g_tablesList) {
        free(g_tablesList);
    }

    return ret;
}

static RecordSchema *parse_json_for_inspect(json_t *element) {
    RecordSchema *recordSchema = calloc(1, sizeof(RecordSchema));
    if (NULL == recordSchema) {
        errorPrint("%s() LN%d, memory allocation failed!\n",
                   __func__, __LINE__);
        return NULL;
    }

    if (JSON_OBJECT != json_typeof(element)) {
        errorPrint("%s() LN%d, json passed is not an object\n",
                __func__, __LINE__);
        free(recordSchema);
        return NULL;
    }

    const char *key;
    json_t *value;

    json_object_foreach(element, key, value) {
        if (0 == strcmp(key, "name")) {
            TOOLS_STRNCPY(recordSchema->name, json_string_value(value),
                    RECORD_NAME_LEN);
        } else if (0 == strcmp(key, "fields")) {
            if (JSON_ARRAY == json_typeof(value)) {
                size_t i;
                size_t size = json_array_size(value);

                debugPrint("%s() LN%d, JSON Array of %zu element%s:\n",
                        __func__, __LINE__,
                        size, json_plural(size));

                recordSchema->num_fields = size;
                recordSchema->fields = calloc(1, sizeof(InspectStruct) * size);
                if (NULL== recordSchema->fields) {
                    errorPrint("%s() LN%d, memory allocation failed!\n",
                            __func__, __LINE__);
                    free(recordSchema);
                    return NULL;
                }

                for (i = 0; i < size; i++) {
                    InspectStruct *field = (InspectStruct *)
                        (recordSchema->fields + sizeof(InspectStruct) * i);
                    json_t *arr_element = json_array_get(value, i);
                    const char *ele_key;
                    json_t *ele_value;

                    json_object_foreach(arr_element, ele_key, ele_value) {
                        if (0 == strcmp(ele_key, "name")) {
                            TOOLS_STRNCPY(field->name,
                                    json_string_value(ele_value),
                                    TSDB_COL_NAME_LEN-1);
                        } else if (0 == strcmp(ele_key, "type")) {
                            int ele_type = json_typeof(ele_value);

                            if (JSON_STRING == ele_type) {
                                TOOLS_STRNCPY(field->type,
                                        json_string_value(ele_value),
                                        TYPE_NAME_LEN-1);
                            } else if (JSON_ARRAY == ele_type) {
                                size_t ele_size = json_array_size(ele_value);

                                for (size_t ele_i = 0; ele_i < ele_size;
                                        ele_i++) {
                                    json_t *arr_type_ele =
                                        json_array_get(ele_value, ele_i);

                                    if (JSON_STRING == json_typeof(arr_type_ele)) {
                                        const char *arr_type_ele_str =
                                            json_string_value(arr_type_ele);

                                        if (0 == strcmp(arr_type_ele_str,
                                                            "null")) {
                                            field->nullable = true;
                                        } else {
                                            TOOLS_STRNCPY(field->type,
                                                    arr_type_ele_str,
                                                    TYPE_NAME_LEN-1);
                                        }
                                    } else if (JSON_OBJECT ==
                                            json_typeof(arr_type_ele)) {
                                        const char *arr_type_ele_key;
                                        json_t *arr_type_ele_value;

                                        json_object_foreach(arr_type_ele,
                                                arr_type_ele_key,
                                                arr_type_ele_value) {
                                            if (JSON_STRING ==
                                                    json_typeof(arr_type_ele_value)) {
                                                const char *arr_type_ele_value_str =
                                                    json_string_value(arr_type_ele_value);
                                                if (0 == strcmp(arr_type_ele_value_str,
                                                            "null")) {
                                                    field->nullable = true;
                                                } else if (0 == strcmp(arr_type_ele_value_str,
                                                            "array")) {
                                                    field->is_array = true;
                                                    TOOLS_STRNCPY(field->type,
                                                            arr_type_ele_value_str,
                                                            TYPE_NAME_LEN-1);
                                                } else {
                                                    TOOLS_STRNCPY(field->type,
                                                            arr_type_ele_value_str,
                                                            TYPE_NAME_LEN-1);
                                                }
                                            } else if (JSON_OBJECT ==
                                                         json_typeof(arr_type_ele_value)) {
                                                const char *arr_type_ele_value_key;
                                                json_t *arr_type_ele_value_value;

                                                json_object_foreach(arr_type_ele_value,
                                                        arr_type_ele_value_key,
                                                        arr_type_ele_value_value) {
                                                    if (JSON_STRING ==
                                                             json_typeof(
                                                                 arr_type_ele_value_value)) {
                                                        const char *arr_type_ele_value_value_str =
                                                            json_string_value(
                                                                     arr_type_ele_value_value);
                                                        TOOLS_STRNCPY(field->array_type_str,
                                                                arr_type_ele_value_value_str,
                                                                TYPE_NAME_LEN-1);
                                                    }
                                                }
                                            }
                                        }
                                    } else {
                                        errorPrint("%s", "Error: not supported!\n");
                                    }
                                }
                            } else if (JSON_OBJECT == ele_type) {
                                const char *obj_key;
                                json_t *obj_value;

                                json_object_foreach(ele_value, obj_key, obj_value) {
                                    if (0 == strcmp(obj_key, "type")) {
                                        int obj_value_type = json_typeof(obj_value);
                                        if (JSON_STRING == obj_value_type) {
                                            TOOLS_STRNCPY(field->type,
                                                    json_string_value(obj_value), TYPE_NAME_LEN-1);
                                            if (0 == strcmp(field->type, "array")) {
                                                field->is_array = true;
                                            }
                                        } else if (JSON_OBJECT == obj_value_type) {
                                            const char *field_key;
                                            json_t *field_value;

                                            json_object_foreach(obj_value, field_key, field_value) {
                                                if (JSON_STRING == json_typeof(field_value)) {
                                                    TOOLS_STRNCPY(field->type,
                                                            json_string_value(field_value),
                                                            TYPE_NAME_LEN-1);
                                                } else {
                                                    field->nullable = true;
                                                }
                                            }
                                        }
                                    } else if (0 == strcmp(obj_key, "items")) {
                                        int obj_value_items = json_typeof(obj_value);
                                        if (JSON_STRING == obj_value_items) {
                                            field->is_array = true;
                                            TOOLS_STRNCPY(field->array_type_str,
                                                    json_string_value(obj_value), TYPE_NAME_LEN-1);
                                        } else if (JSON_OBJECT == obj_value_items) {
                                            const char *item_key;
                                            json_t *item_value;

                                            json_object_foreach(obj_value, item_key, item_value) {
                                                if (JSON_STRING == json_typeof(item_value)) {
                                                    TOOLS_STRNCPY(field->array_type_str,
                                                            json_string_value(item_value),
                                                            TYPE_NAME_LEN-1);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                errorPrint("%s() LN%d, fields have no array\n",
                        __func__, __LINE__);
                free(recordSchema);
                return NULL;
            }

            break;
        }
    }

    return recordSchema;
}


int inspectAvroFile(char *filename) {
    int ret = 0;

    if (filename) {
        size_t len = strlen(filename);
        if (len > strlen(MFILE_EXT)) {
            char * ext = filename + len - strlen(MFILE_EXT);
            if(strcasecmp(ext, MFILE_EXT) == 0) {
                debugPrint("mfile ignore inspect. %s\n", filename);
                return 0;
            }
        }
    }

    avro_file_reader_t reader;

    avro_schema_t schema;

    if (avro_file_reader(filename, &reader)) {
        errorPrint("Unable to open avro file %s: %s\n",
                filename, avro_strerror());
        return -1;
    }

    int buf_len = 256*1024;
    char *jsonbuf = calloc(1, buf_len);
    if (NULL == jsonbuf) {
        errorPrint("%s() LN%d, memory allocation failed!\n",
                   __func__, __LINE__);
        return -1;
    }

    avro_writer_t jsonwriter = avro_writer_memory(jsonbuf, buf_len);

    schema = avro_file_reader_get_writer_schema(reader);
    avro_schema_to_json(schema, jsonwriter);

    if (0 == strlen(jsonbuf)) {
        errorPrint("Failed to parse avro file: %s schema. reason: %s\n",
                filename, avro_strerror());
        avro_writer_free(jsonwriter);
        return -1;
    }
    printf("Schema:\n  %s\n", jsonbuf);

    json_t *json_root = load_json(jsonbuf);
    verbosePrint("\n%s() LN%d\n === Schema parsed:\n", __func__, __LINE__);
    if (g_args.verbose_print) {
        print_json(json_root);
    }

    if (NULL == json_root) {
        errorPrint("%s() LN%d, cannot read valid schema from %s\n",
                __func__, __LINE__, filename);
        avro_writer_free(jsonwriter);
        tfree(jsonbuf);

        return -1;
    }

    RecordSchema *recordSchema = parse_json_for_inspect(json_root);
    if (NULL == recordSchema) {
        errorPrint("Failed to parse json to recordschema. reason: %s\n",
                avro_strerror());
        avro_writer_free(jsonwriter);
        tfree(jsonbuf);
        return -1;
    }

    uint64_t count = 0;

    if (false == g_args.schemaonly) {
        fprintf(stdout, "\n=== Records:\n");
        avro_value_iface_t *value_class =
            avro_generic_class_from_schema(schema);
        avro_value_t value;
        avro_generic_value_new(value_class, &value);

        while (!avro_file_reader_read_value(reader, &value)) {
            for (int i = 0; i < recordSchema->num_fields; i++) {
                InspectStruct *field = (InspectStruct *)(recordSchema->fields
                        + sizeof(InspectStruct) * i);
                avro_value_t field_value;
                float f = 0.0;
                double dbl = 0.0;
                int b = 0;
                const char *buf = NULL;
                size_t size;
                const void *bytesbuf = NULL;
                size_t bytessize;
                if (0 == avro_value_get_by_name(&value, field->name,
                            &field_value, NULL)) {
                    if (0 == strcmp(field->type, "int")) {
                        int32_t n32 = 0;
                        if (field->nullable) {
                            avro_value_t branch;
                            avro_value_get_current_branch(&field_value,
                                    &branch);
                            if (0 == avro_value_get_null(&branch)) {
                                fprintf(stdout, "%s |\t", "null");
                            } else {
                                avro_value_get_int(&branch, &n32);
                                fprintf(stdout, "%d |\t", n32);
                            }
                        } else {
                            avro_value_get_int(&field_value, &n32);
                            if (((int32_t)TSDB_DATA_INT_NULL == n32)
                                    || (TSDB_DATA_SMALLINT_NULL == n32)
                                    || (TSDB_DATA_TINYINT_NULL == n32)) {
                                fprintf(stdout, "%s |\t", "null?");
                            } else {
                                fprintf(stdout, "%d |\t", n32);
                            }
                        }
                    } else if (0 == strcmp(field->type, "float")) {
                        if (field->nullable) {
                            avro_value_t branch;
                            avro_value_get_current_branch(&field_value,
                                    &branch);
                            if (0 == avro_value_get_null(&branch)) {
                                fprintf(stdout, "%s |\t", "null");
                            } else {
                                avro_value_get_float(&branch, &f);
                                fprintf(stdout, "%f |\t", f);
                            }
                        } else {
                            avro_value_get_float(&field_value, &f);
                            if (TSDB_DATA_FLOAT_NULL == f) {
                                fprintf(stdout, "%s |\t", "null");
                            } else {
                                fprintf(stdout, "%f |\t", f);
                            }
                        }
                    } else if (0 == strcmp(field->type, "double")) {
                        if (field->nullable) {
                            avro_value_t branch;
                            avro_value_get_current_branch(&field_value,
                                    &branch);
                            if (0 == avro_value_get_null(&branch)) {
                                fprintf(stdout, "%s |\t", "null");
                            } else {
                                avro_value_get_double(&branch, &dbl);
                                fprintf(stdout, "%f |\t", dbl);
                            }
                        } else {
                            avro_value_get_double(&field_value, &dbl);
                            if (TSDB_DATA_DOUBLE_NULL == dbl) {
                                fprintf(stdout, "%s |\t", "null");
                            } else {
                                fprintf(stdout, "%f |\t", dbl);
                            }
                        }
                    } else if (0 == strcmp(field->type, "long")) {
                        int64_t n64 = 0;
                        if (field->nullable) {
                            avro_value_t branch;
                            avro_value_get_current_branch(&field_value,
                                    &branch);
                            if (0 == avro_value_get_null(&branch)) {
                                fprintf(stdout, "%s |\t", "null");
                            } else {
                                avro_value_get_long(&branch, &n64);
                                fprintf(stdout, "%"PRId64" |\t", n64);
                            }
                        } else {
                            avro_value_get_long(&field_value, &n64);
                            if ((int64_t)TSDB_DATA_BIGINT_NULL == n64) {
                                fprintf(stdout, "%s |\t", "null");
                            } else {
                                fprintf(stdout, "%"PRId64" |\t", n64);
                            }
                        }
                    } else if (0 == strcmp(field->type, "string")) {
                        if (field->nullable) {
                            avro_value_t branch;
                            avro_value_get_current_branch(&field_value,
                                    &branch);
                            avro_value_get_string(&branch, &buf, &size);
                        } else {
                            avro_value_get_string(&field_value, &buf, &size);
                        }
                        fprintf(stdout, "%s |\t", buf);
                    } else if (0 == strcmp(field->type, "bytes")) {
                        if (field->nullable) {
                            avro_value_t branch;
                            avro_value_get_current_branch(&field_value,
                                    &branch);
                            avro_value_get_bytes(&branch, &bytesbuf,
                                    &bytessize);
                        } else {
                            avro_value_get_bytes(
                                &field_value,
                                &bytesbuf,
                                &bytessize);
                        }
                        fprintf(stdout, "%s |\t", (char*)bytesbuf);
                    } else if (0 == strcmp(field->type, "boolean")) {
                        if (field->nullable) {
                            avro_value_t bool_branch;
                            avro_value_get_current_branch(&field_value,
                                    &bool_branch);
                            if (0 == avro_value_get_null(&bool_branch)) {
                                fprintf(stdout, "%s |\t", "null");
                            } else {
                                avro_value_get_boolean(&bool_branch, &b);
                                fprintf(stdout, "%s |\t", b?"true":"false");
                            }
                        } else {
                            avro_value_get_boolean(&field_value, &b);
                            fprintf(stdout, "%s |\t", b?"true":"false");
                        }
                    } else if (0 == strcmp(field->type, "array")) {
                        if (0 == strcmp(field->array_type_str, "int")) {
                            int32_t n32 = 0;
                            if (field->nullable) {
                                avro_value_t branch;
                                avro_value_get_current_branch(&field_value,
                                        &branch);
                                if (0 == avro_value_get_null(&branch)) {
                                    fprintf(stdout, "%s |\t", "null");
                                } else {
                                    size_t array_size;
                                    avro_value_get_size(&branch, &array_size);

                                    debugPrint("array_size is %zu\n",
                                               array_size);

                                    uint32_t array_u32 = 0;
                                    for (size_t item = 0; item < array_size;
                                            item++) {
                                        avro_value_t item_value;
                                        avro_value_get_by_index(&branch, item,
                                                &item_value, NULL);
                                        avro_value_get_int(&item_value, &n32);
                                        array_u32 += n32;
                                    }
                                    fprintf(stdout, "%u |\t", array_u32);
                                }
                            } else {
                                size_t array_size;
                                avro_value_get_size(&field_value, &array_size);

                                debugPrint("array_size is %zu\n", array_size);
                                uint32_t array_u32 = 0;
                                for (size_t item = 0; item < array_size;
                                        item++) {
                                    avro_value_t item_value;
                                    avro_value_get_by_index(&field_value, item,
                                            &item_value, NULL);
                                    avro_value_get_int(&item_value, &n32);
                                    array_u32 += n32;
                                }
                                if ((TSDB_DATA_UINT_NULL == array_u32)
                                        || (TSDB_DATA_USMALLINT_NULL
                                            == array_u32)
                                        || (TSDB_DATA_UTINYINT_NULL
                                            == array_u32)) {
                                    fprintf(stdout, "%s |\t", "null?");
                                } else {
                                    fprintf(stdout, "%u |\t", array_u32);
                                }
                            }
                        } else if (0 == strcmp(field->array_type_str, "long")) {
                            int64_t n64 = 0;
                            if (field->nullable) {
                                avro_value_t branch;
                                avro_value_get_current_branch(&field_value,
                                        &branch);
                                if (0 == avro_value_get_null(&branch)) {
                                    fprintf(stdout, "%s |\t", "null");
                                } else {
                                    size_t array_size;
                                    avro_value_get_size(&branch, &array_size);

                                    debugPrint("array_size is %zu\n",
                                               array_size);
                                    uint64_t array_u64 = 0;
                                    for (size_t item = 0; item < array_size;
                                            item++) {
                                        avro_value_t item_value;
                                        avro_value_get_by_index(&branch, item,
                                                &item_value, NULL);
                                        avro_value_get_long(&item_value, &n64);
                                        array_u64 += n64;
                                    }
                                    fprintf(stdout, "%"PRIu64" |\t", array_u64);
                                }
                            } else {
                                size_t array_size;
                                avro_value_get_size(&field_value, &array_size);

                                debugPrint("array_size is %zu\n", array_size);
                                uint64_t array_u64 = 0;
                                for (size_t item = 0; item < array_size;
                                        item++) {
                                    avro_value_t item_value;
                                    avro_value_get_by_index(&field_value, item,
                                            &item_value, NULL);
                                    avro_value_get_long(&item_value, &n64);
                                    array_u64 += n64;
                                }
                                if (TSDB_DATA_UBIGINT_NULL == array_u64) {
                                    fprintf(stdout, "%s |\t", "null?");
                                } else {
                                    fprintf(stdout, "%"PRIu64" |\t", array_u64);
                                }
                            }
                        } else {
                            errorPrint("%s is not supported!\n",
                                    field->array_type_str);
                        }
                    }
                }
            }
            fprintf(stdout, "\n");

            count++;
        }

        avro_value_decref(&value);
        avro_value_iface_decref(value_class);
    }

    freeRecordSchema(recordSchema);
    avro_schema_decref(schema);
    avro_file_reader_close(reader);

    json_decref(json_root);
    avro_writer_free(jsonwriter);
    tfree(jsonbuf);

    fprintf(stdout, "\n");

    return ret;
}

static int inspectAvroFiles(int argc, char *argv[]) {
    int ret = 0;

    for (int i = 1; i < argc; i++) {
        if (strlen(argv[i])) {
            fprintf(stdout, "\n### Inspecting %s...\n", argv[i]);
            ret = inspectAvroFile(argv[i]);
            fprintf(stdout, "\n### avro file %s inspected\n", argv[i]);
        }
    }
    return ret;
}


int main(int argc, char *argv[]) {
    g_uniqueID = getUniqueIDFromEpoch();

    int ret = 0;
    /* Parse our arguments; every option seen by parse_opt will be
       reflected in arguments. */
    if (argc > 1) {
        parse_timestamp(argc, argv, &g_args);
        parse_args(argc, argv, &g_args);
    }

    // command line
    argp_parse(&argp, argc, argv, 0, 0, &g_args);
    if (g_args.abort) {
        abort();
    }

    // client info
    snprintf(g_client_info, MIDDLE_BUFF_LEN, "%s", taos_get_client_info());
    g_majorVersionOfClient = atoi(g_client_info);
    debugPrint("Client info: %s, major version: %d\n",
            g_client_info,
            g_majorVersionOfClient);

    if (g_majorVersionOfClient > 2) {
        if (g_args.allow_sys) {
            warnPrint("The system database should not be backuped "
                    "with server version: %d\n",
                    g_majorVersionOfClient);
            g_args.allow_sys = false;
        }
    }

    // env dsn
    if ( NULL == g_args.dsn) {
        char *dsn = getenv("TDENGINE_CLOUD_DSN");
        if(dsn && dsn[0] != 0) {
            if (g_args.connMode != CONN_MODE_NATIVE) {
                g_args.dsn = dsn;
                infoPrint("read dsn from evn dsn=%s\n", dsn);
            } else {
                warnPrint("command line pass native mode , ignore evn dsn:%s\n", dsn);
            }
        }
    } else {
        // check conflict
        if (g_args.connMode == CONN_MODE_NATIVE) {
            errorPrint("%s", DSN_NATIVE_CONFLICT);
            return -1;
        }
    }

    // conn mode
    if (setConnMode(g_args.connMode, g_args.dsn, true) != 0) {
        return -1;
    }

    // running
    if (g_args.inspect) {
        ret = inspectAvroFiles(argc, argv);
    } else {
        ret = dumpEntry();
    }

    // free buf
    if (g_args.renameBuf) {
        free(g_args.renameBuf);
        g_args.renameBuf = NULL;
    }

    // free node
    SRenameDB* node = g_args.renameHead;
    g_args.renameHead = NULL;
    while(node) {
        SRenameDB* next = (SRenameDB*)node->next;
        free(node);
        node = next;
    }

    return ret;
}
