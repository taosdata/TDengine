/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include <iconv.h>
#include <sys/stat.h>
#include <sys/syscall.h>

#include "os.h"
#include "taos.h"
#include "taosdef.h"
#include "taosmsg.h"
#include "tglobal.h"
#include "tsclient.h"
#include "tsdb.h"
#include "tutil.h"
#include <taos.h>

#define TSDB_SUPPORT_NANOSECOND 1

#define MAX_FILE_NAME_LEN       256             // max file name length on linux is 255
#define COMMAND_SIZE            65536
#define MAX_RECORDS_PER_REQ     32766
//#define DEFAULT_DUMP_FILE "taosdump.sql"

// for strncpy buffer overflow
#define min(a, b) (((a) < (b)) ? (a) : (b))

static int  converStringToReadable(char *str, int size, char *buf, int bufsize);
static int  convertNCharToReadable(char *str, int size, char *buf, int bufsize);
static void taosDumpCharset(FILE *fp);
static void taosLoadFileCharset(FILE *fp, char *fcharset);

typedef struct {
  short bytes;
  int8_t type;
} SOColInfo;

#define debugPrint(fmt, ...) \
    do { if (g_args.debug_print || g_args.verbose_print) \
      fprintf(stderr, "DEBG: "fmt, __VA_ARGS__); } while(0)

#define verbosePrint(fmt, ...) \
    do { if (g_args.verbose_print) \
        fprintf(stderr, "VERB: "fmt, __VA_ARGS__); } while(0)

#define performancePrint(fmt, ...) \
    do { if (g_args.performance_print) \
        fprintf(stderr, "VERB: "fmt, __VA_ARGS__); } while(0)

#define errorPrint(fmt, ...) \
    do { fprintf(stderr, "\033[31m"); fprintf(stderr, "ERROR: "fmt, __VA_ARGS__); fprintf(stderr, "\033[0m"); } while(0)


// -------------------------- SHOW DATABASE INTERFACE-----------------------
enum _show_db_index {
    TSDB_SHOW_DB_NAME_INDEX,
    TSDB_SHOW_DB_CREATED_TIME_INDEX,
    TSDB_SHOW_DB_NTABLES_INDEX,
    TSDB_SHOW_DB_VGROUPS_INDEX,
    TSDB_SHOW_DB_REPLICA_INDEX,
    TSDB_SHOW_DB_QUORUM_INDEX,
    TSDB_SHOW_DB_DAYS_INDEX,
    TSDB_SHOW_DB_KEEP_INDEX,
    TSDB_SHOW_DB_CACHE_INDEX,
    TSDB_SHOW_DB_BLOCKS_INDEX,
    TSDB_SHOW_DB_MINROWS_INDEX,
    TSDB_SHOW_DB_MAXROWS_INDEX,
    TSDB_SHOW_DB_WALLEVEL_INDEX,
    TSDB_SHOW_DB_FSYNC_INDEX,
    TSDB_SHOW_DB_COMP_INDEX,
    TSDB_SHOW_DB_CACHELAST_INDEX,
    TSDB_SHOW_DB_PRECISION_INDEX,
    TSDB_SHOW_DB_UPDATE_INDEX,
    TSDB_SHOW_DB_STATUS_INDEX,
    TSDB_MAX_SHOW_DB
};

// -----------------------------------------SHOW TABLES CONFIGURE -------------------------------------
enum _show_tables_index {
    TSDB_SHOW_TABLES_NAME_INDEX,
    TSDB_SHOW_TABLES_CREATED_TIME_INDEX,
    TSDB_SHOW_TABLES_COLUMNS_INDEX,
    TSDB_SHOW_TABLES_METRIC_INDEX,
    TSDB_SHOW_TABLES_UID_INDEX,
    TSDB_SHOW_TABLES_TID_INDEX,
    TSDB_SHOW_TABLES_VGID_INDEX,
    TSDB_MAX_SHOW_TABLES
};

// ---------------------------------- DESCRIBE METRIC CONFIGURE ------------------------------
enum _describe_table_index {
    TSDB_DESCRIBE_METRIC_FIELD_INDEX,
    TSDB_DESCRIBE_METRIC_TYPE_INDEX,
    TSDB_DESCRIBE_METRIC_LENGTH_INDEX,
    TSDB_DESCRIBE_METRIC_NOTE_INDEX,
    TSDB_MAX_DESCRIBE_METRIC
};

#define COL_NOTE_LEN    128

typedef struct {
    char field[TSDB_COL_NAME_LEN + 1];
    char type[16];
    int length;
    char note[COL_NOTE_LEN];
} SColDes;

typedef struct {
    char name[TSDB_TABLE_NAME_LEN];
    SColDes cols[];
} STableDef;

extern char version[];

#define DB_PRECISION_LEN   8
#define DB_STATUS_LEN      16

typedef struct {
    char     name[TSDB_DB_NAME_LEN];
    char     create_time[32];
    int32_t  ntables;
    int32_t  vgroups;
    int16_t  replica;
    int16_t  quorum;
    int16_t  days;
    char     keeplist[32];
    //int16_t  daysToKeep;
    //int16_t  daysToKeep1;
    //int16_t  daysToKeep2;
    int32_t  cache; //MB
    int32_t  blocks;
    int32_t  minrows;
    int32_t  maxrows;
    int8_t   wallevel;
    int32_t  fsync;
    int8_t   comp;
    int8_t   cachelast;
    char     precision[DB_PRECISION_LEN];   // time resolution
    int8_t   update;
    char     status[DB_STATUS_LEN];
} SDbInfo;

typedef struct {
    char name[TSDB_TABLE_NAME_LEN];
    char metric[TSDB_TABLE_NAME_LEN];
} STableRecord;

typedef struct {
    bool isMetric;
    STableRecord tableRecord;
} STableRecordInfo;

typedef struct {
    pthread_t threadID;
    int32_t   threadIndex;
    int32_t   totalThreads;
    char      dbName[TSDB_DB_NAME_LEN];
    void     *taosCon;
    int64_t   rowsOfDumpOut;
    int64_t   tablesOfDumpOut;
} SThreadParaObj;

typedef struct {
    int64_t   totalRowsOfDumpOut;
    int64_t   totalChildTblsOfDumpOut;
    int32_t   totalSuperTblsOfDumpOut;
    int32_t   totalDatabasesOfDumpOut;
} resultStatistics;

static int64_t g_totalDumpOutRows = 0;

SDbInfo **g_dbInfos = NULL;

const char *argp_program_version = version;
const char *argp_program_bug_address = "<support@taosdata.com>";

/* Program documentation. */
static char doc[] = "";
/* "Argp example #4 -- a program with somewhat more complicated\ */
/*         options\ */
/*         \vThis part of the documentation comes *after* the options;\ */
/*         note that the text is automatically filled, but it's possible\ */
/*         to force a line-break, e.g.\n<-- here."; */

/* A description of the arguments we accept. */
static char args_doc[] = "dbname [tbname ...]\n--databases dbname ...\n--all-databases\n-i inpath\n-o outpath";

/* Keys for options without short-options. */
#define OPT_ABORT 1 /* –abort */

/* The options we understand. */
static struct argp_option options[] = {
    // connection option
    {"host", 'h', "HOST",    0,  "Server host dumping data from. Default is localhost.", 0},
    {"user", 'u', "USER",    0,  "User name used to connect to server. Default is root.", 0},
#ifdef _TD_POWER_
    {"password", 'p', "PASSWORD",    0,  "User password to connect to server. Default is powerdb.", 0},
#else
    {"password", 'p', "PASSWORD",    0,  "User password to connect to server. Default is taosdata.", 0},
#endif
    {"port", 'P', "PORT",        0,  "Port to connect", 0},
    {"cversion",      'v', "CVERION",     0,  "client version", 0},
    {"mysqlFlag",     'q', "MYSQLFLAG",   0,  "mysqlFlag, Default is 0", 0},
    // input/output file
    {"outpath", 'o', "OUTPATH",     0,  "Output file path.", 1},
    {"inpath", 'i', "INPATH",      0,  "Input file path.", 1},
    {"resultFile", 'r', "RESULTFILE",  0,  "DumpOut/In Result file path and name.", 1},
#ifdef _TD_POWER_
    {"config", 'c', "CONFIG_DIR",  0,  "Configure directory. Default is /etc/power/taos.cfg.", 1},
#else
    {"config", 'c', "CONFIG_DIR",  0,  "Configure directory. Default is /etc/taos/taos.cfg.", 1},
#endif
    {"encode", 'e', "ENCODE", 0,  "Input file encoding.", 1},
    // dump unit options
    {"all-databases", 'A', 0, 0,  "Dump all databases.", 2},
    {"databases", 'D', 0, 0,  "Dump assigned databases", 2},
    {"allow-sys",   'a', 0, 0,  "Allow to dump sys database", 2},
    // dump format options
    {"schemaonly", 's', 0, 0,  "Only dump schema.", 2},
    {"without-property", 'N', 0, 0,  "Dump schema without properties.", 2},
    {"avro", 'V', 0, 0,  "Dump apache avro format data file. By default, dump sql command sequence.", 2},
    {"start-time",    'S', "START_TIME",  0,  "Start time to dump. Either epoch or ISO8601/RFC3339 format is acceptable. ISO8601 format example: 2017-10-01T00:00:00.000+0800 or 2017-10-0100:00:00:000+0800 or '2017-10-01 00:00:00.000+0800'",  4},
    {"end-time",      'E', "END_TIME",    0,  "End time to dump. Either epoch or ISO8601/RFC3339 format is acceptable. ISO8601 format example: 2017-10-01T00:00:00.000+0800 or 2017-10-0100:00:00.000+0800 or '2017-10-01 00:00:00.000+0800'",  5},
#if TSDB_SUPPORT_NANOSECOND == 1
    {"precision",  'C', "PRECISION",  0,  "Specify precision for converting human-readable time to epoch. Valid value is one of ms, us, and ns. Default is ms.", 6},
#else
    {"precision",  'C', "PRECISION",  0,  "Use specified precision to convert human-readable time. Valid value is one of ms and us. Default is ms.", 6},
#endif
    {"data-batch",  'B', "DATA_BATCH",  0,  "Number of data point per insert statement. Max value is 32766. Default is 1.", 3},
    {"max-sql-len", 'L', "SQL_LEN",     0,  "Max length of one sql. Default is 65480.",   3},
    {"table-batch", 't', "TABLE_BATCH", 0,  "Number of table dumpout into one output file. Default is 1.",  3},
    {"thread_num",  'T', "THREAD_NUM",  0,  "Number of thread for dump in file. Default is 5.", 3},
    {"debug",   'g', 0, 0,  "Print debug info.",    8},
    {"verbose", 'b', 0, 0,  "Print verbose debug info.", 9},
    {"performanceprint", 'm', 0, 0,  "Print performance debug info.", 10},
    {0}
};

/* Used by main to communicate with parse_opt. */
typedef struct arguments {
    // connection option
    char    *host;
    char    *user;
    char    *password;
    uint16_t port;
    char     cversion[12];
    uint16_t mysqlFlag;
    // output file
    char     outpath[MAX_FILE_NAME_LEN];
    char     inpath[MAX_FILE_NAME_LEN];
    // result file
    char    *resultFile;
    char    *encode;
    // dump unit option
    bool     all_databases;
    bool     databases;
    // dump format option
    bool     schemaonly;
    bool     with_property;
    bool     avro;
    int64_t  start_time;
    int64_t  end_time;
    char     precision[8];
    int32_t  data_batch;
    int32_t  max_sql_len;
    int32_t  table_batch; // num of table which will be dump into one output file.
    bool     allow_sys;
    // other options
    int32_t  thread_num;
    int      abort;
    char   **arg_list;
    int       arg_list_len;
    bool      isDumpIn;
    bool      debug_print;
    bool      verbose_print;
    bool      performance_print;
} SArguments;

/* Our argp parser. */
static error_t parse_opt(int key, char *arg, struct argp_state *state);

static struct argp argp = {options, parse_opt, args_doc, doc};
static resultStatistics g_resultStatistics = {0};
static FILE *g_fpOfResult = NULL;
static int g_numOfCores = 1;

static int taosDumpOut();
static int taosDumpIn();
static void taosDumpCreateDbClause(SDbInfo *dbInfo, bool isDumpProperty,
        FILE *fp);
static int taosDumpDb(SDbInfo *dbInfo, FILE *fp, TAOS *taosCon);
static int32_t taosDumpStable(char *table, FILE *fp, TAOS* taosCon,
        char* dbName);
static void taosDumpCreateTableClause(STableDef *tableDes, int numOfCols,
        FILE *fp, char* dbName);
static void taosDumpCreateMTableClause(STableDef *tableDes, char *metric,
        int numOfCols, FILE *fp, char* dbName);
static int32_t taosDumpTable(char *tbName, char *metric,
        FILE *fp, TAOS* taosCon, char* dbName);
static int taosDumpTableData(FILE *fp, char *tbName,
        TAOS* taosCon, char* dbName,
        char *jsonAvroSchema);
static int taosCheckParam(struct arguments *arguments);
static void taosFreeDbInfos();
static void taosStartDumpOutWorkThreads(int32_t numOfThread, char *dbName);

struct arguments g_args = {
    // connection option
    NULL,
    "root",
#ifdef _TD_POWER_
    "powerdb",
#else
    "taosdata",
#endif
    0,
    "",
    0,
    // outpath and inpath
    "",
    "",
    "./dump_result.txt",
    NULL,
    // dump unit option
    false,
    false,
    // dump format option
    false,      // schemeonly
    true,       // with_property
    false,      // avro format
    -INT64_MAX, // start_time
    INT64_MAX,  // end_time
    "ms",       // precision
    1,          // data_batch
    TSDB_MAX_SQL_LEN,   // max_sql_len
    1,          // table_batch
    false,      // allow_sys
    // other options
    5,          // thread_num
    0,          // abort
    NULL,       // arg_list
    0,          // arg_list_len
    false,      // isDumpIn
    false,      // debug_print
    false,      // verbose_print
    false       // performance_print
};

/* Parse a single option. */
static error_t parse_opt(int key, char *arg, struct argp_state *state) {
    /* Get the input argument from argp_parse, which we
       know is a pointer to our arguments structure. */
    wordexp_t full_path;

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
            g_args.password = arg;
            break;
        case 'P':
            g_args.port = atoi(arg);
            break;
        case 'q':
            g_args.mysqlFlag = atoi(arg);
            break;
        case 'v':
            if (wordexp(arg, &full_path, 0) != 0) {
                errorPrint("Invalid client vesion %s\n", arg);
                return -1;
            }
            tstrncpy(g_args.cversion, full_path.we_wordv[0], 11);
            wordfree(&full_path);
            break;
            // output file path
        case 'o':
            if (wordexp(arg, &full_path, 0) != 0) {
                errorPrint("Invalid path %s\n", arg);
                return -1;
            }
            tstrncpy(g_args.outpath, full_path.we_wordv[0],
                    MAX_FILE_NAME_LEN);
            wordfree(&full_path);
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
            tstrncpy(g_args.inpath, full_path.we_wordv[0],
                    MAX_FILE_NAME_LEN);
            wordfree(&full_path);
            break;
        case 'r':
            g_args.resultFile = arg;
            break;
        case 'c':
            if (wordexp(arg, &full_path, 0) != 0) {
                errorPrint("Invalid path %s\n", arg);
                return -1;
            }
            tstrncpy(configDir, full_path.we_wordv[0], MAX_FILE_NAME_LEN);
            wordfree(&full_path);
            break;
        case 'e':
            g_args.encode = arg;
            break;
            // dump unit option
        case 'A':
            g_args.all_databases = true;
            break;
        case 'D':
            g_args.databases = true;
            break;
            // dump format option
        case 's':
            g_args.schemaonly = true;
            break;
        case 'N':
            g_args.with_property = false;
            break;
        case 'V':
            g_args.avro = true;
            break;
        case 'S':
            // parse time here.
            g_args.start_time = atol(arg);
            break;
        case 'E':
            g_args.end_time = atol(arg);
            break;
        case 'C':
            break;
        case 'B':
            g_args.data_batch = atoi(arg);
            if (g_args.data_batch > MAX_RECORDS_PER_REQ) {
                g_args.data_batch = MAX_RECORDS_PER_REQ;
            }
            break;
        case 'L':
            {
                int32_t len = atoi(arg);
                if (len > TSDB_MAX_ALLOWED_SQL_LEN) {
                    len = TSDB_MAX_ALLOWED_SQL_LEN;
                } else if (len < TSDB_MAX_SQL_LEN) {
                    len = TSDB_MAX_SQL_LEN;
                }
                g_args.max_sql_len = len;
                break;
            }
        case 't':
            g_args.table_batch = atoi(arg);
            break;
        case 'T':
            g_args.thread_num = atoi(arg);
            break;
        case OPT_ABORT:
            g_args.abort = 1;
            break;
        case ARGP_KEY_ARG:
            g_args.arg_list     = &state->argv[state->next - 1];
            g_args.arg_list_len = state->argc - state->next + 1;
            state->next             = state->argc;
            break;

        default:
            return ARGP_ERR_UNKNOWN;
    }
    return 0;
}

static int queryDbImpl(TAOS *taos, char *command) {
    int i;
    TAOS_RES *res = NULL;
    int32_t   code = -1;

    for (i = 0; i < 5; i++) {
        if (NULL != res) {
            taos_free_result(res);
            res = NULL;
        }

        res = taos_query(taos, command);
        code = taos_errno(res);
        if (0 == code) {
            break;
        }
    }

    if (code != 0) {
        errorPrint("Failed to run <%s>, reason: %s\n", command, taos_errstr(res));
        taos_free_result(res);
        //taos_close(taos);
        return -1;
    }

    taos_free_result(res);
    return 0;
}

static void parse_precision_first(
        int argc, char *argv[], SArguments *arguments) {
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-C") == 0) {
            if (NULL == argv[i+1]) {
                errorPrint("%s need a valid value following!\n", argv[i]);
                exit(-1);
            }
            char *tmp = strdup(argv[i+1]);
            if (tmp == NULL) {
                errorPrint("%s() LN%d, strdup() cannot allocate memory\n",
                        __func__, __LINE__);
                exit(-1);
            }
            if ((0 != strncasecmp(tmp, "ms", strlen("ms")))
                    && (0 != strncasecmp(tmp, "us", strlen("us")))
#if TSDB_SUPPORT_NANOSECOND == 1
                    && (0 != strncasecmp(tmp, "ns", strlen("ns")))
#endif
                    ) {
                //
                errorPrint("input precision: %s is invalid value\n", tmp);
                free(tmp);
                exit(-1);
            }
            tstrncpy(g_args.precision, tmp,
                min(DB_PRECISION_LEN, strlen(tmp) + 1));
            free(tmp);
        }
    }
}

static void parse_timestamp(
        int argc, char *argv[], SArguments *arguments) {
    for (int i = 1; i < argc; i++) {
        if ((strcmp(argv[i], "-S") == 0)
                || (strcmp(argv[i], "-E") == 0)) {
            if (NULL == argv[i+1]) {
                errorPrint("%s need a valid value following!\n", argv[i]);
                exit(-1);
            }
            char *tmp = strdup(argv[i+1]);
            if (NULL == tmp) {
                errorPrint("%s() LN%d, strdup() cannot allocate memory\n",
                        __func__, __LINE__);
                exit(-1);
            }

            int64_t tmpEpoch;
            if (strchr(tmp, ':') && strchr(tmp, '-')) {
                int32_t timePrec;
                if (0 == strncasecmp(arguments->precision,
                            "ms", strlen("ms"))) {
                    timePrec = TSDB_TIME_PRECISION_MILLI;
                } else if (0 == strncasecmp(arguments->precision,
                            "us", strlen("us"))) {
                    timePrec = TSDB_TIME_PRECISION_MICRO;
#if TSDB_SUPPORT_NANOSECOND == 1
                } else if (0 == strncasecmp(arguments->precision,
                            "ns", strlen("ns"))) {
                    timePrec = TSDB_TIME_PRECISION_NANO;
#endif
                } else {
                    errorPrint("Invalid time precision: %s",
                            arguments->precision);
                    free(tmp);
                    return;
                }

                if (TSDB_CODE_SUCCESS != taosParseTime(
                            tmp, &tmpEpoch, strlen(tmp),
                            timePrec, 0)) {
                    errorPrint("Input %s, end time error!\n", tmp);
                    free(tmp);
                    return;
                }
            } else {
                tstrncpy(arguments->precision, "n/a", strlen("n/a") + 1);
                tmpEpoch = atoll(tmp);
            }

            sprintf(argv[i+1], "%"PRId64"", tmpEpoch);
            debugPrint("%s() LN%d, tmp is: %s, argv[%d]: %s\n",
                    __func__, __LINE__, tmp, i, argv[i]);
            free(tmp);
        }
    }
}

int main(int argc, char *argv[]) {

    int ret = 0;
    /* Parse our arguments; every option seen by parse_opt will be
       reflected in arguments. */
    if (argc > 2) {
        parse_precision_first(argc, argv, &g_args);
        parse_timestamp(argc, argv, &g_args);
    }

    argp_parse(&argp, argc, argv, 0, 0, &g_args);

    if (g_args.abort) {
#ifndef _ALPINE
        error(10, 0, "ABORTED");
#else
        abort();
#endif
    }

    printf("====== arguments config ======\n");
    {
        printf("host: %s\n", g_args.host);
        printf("user: %s\n", g_args.user);
        printf("password: %s\n", g_args.password);
        printf("port: %u\n", g_args.port);
        printf("cversion: %s\n", g_args.cversion);
        printf("mysqlFlag: %d\n", g_args.mysqlFlag);
        printf("outpath: %s\n", g_args.outpath);
        printf("inpath: %s\n", g_args.inpath);
        printf("resultFile: %s\n", g_args.resultFile);
        printf("encode: %s\n", g_args.encode);
        printf("all_databases: %s\n", g_args.all_databases?"true":"false");
        printf("databases: %d\n", g_args.databases);
        printf("schemaonly: %s\n", g_args.schemaonly?"true":"false");
        printf("with_property: %s\n", g_args.with_property?"true":"false");
        printf("avro format: %s\n", g_args.avro?"true":"false");
        printf("start_time: %" PRId64 "\n", g_args.start_time);
        printf("end_time: %" PRId64 "\n", g_args.end_time);
        printf("precision: %s\n", g_args.precision);
        printf("data_batch: %d\n", g_args.data_batch);
        printf("max_sql_len: %d\n", g_args.max_sql_len);
        printf("table_batch: %d\n", g_args.table_batch);
        printf("thread_num: %d\n", g_args.thread_num);
        printf("allow_sys: %d\n", g_args.allow_sys);
        printf("abort: %d\n", g_args.abort);
        printf("isDumpIn: %d\n", g_args.isDumpIn);
        printf("arg_list_len: %d\n", g_args.arg_list_len);
        printf("debug_print: %d\n", g_args.debug_print);

        for (int32_t i = 0; i < g_args.arg_list_len; i++) {
            printf("arg_list[%d]: %s\n", i, g_args.arg_list[i]);
        }
    }
    printf("==============================\n");

    if (g_args.cversion[0] != 0){
        tstrncpy(version, g_args.cversion, 11);
    }

    if (taosCheckParam(&g_args) < 0) {
        exit(EXIT_FAILURE);
    }

    g_fpOfResult = fopen(g_args.resultFile, "a");
    if (NULL == g_fpOfResult) {
        errorPrint("Failed to open %s for save result\n", g_args.resultFile);
        exit(-1);
    };

    fprintf(g_fpOfResult, "#############################################################################\n");
    fprintf(g_fpOfResult, "============================== arguments config =============================\n");
    {
        fprintf(g_fpOfResult, "host: %s\n", g_args.host);
        fprintf(g_fpOfResult, "user: %s\n", g_args.user);
        fprintf(g_fpOfResult, "password: %s\n", g_args.password);
        fprintf(g_fpOfResult, "port: %u\n", g_args.port);
        fprintf(g_fpOfResult, "cversion: %s\n", g_args.cversion);
        fprintf(g_fpOfResult, "mysqlFlag: %d\n", g_args.mysqlFlag);
        fprintf(g_fpOfResult, "outpath: %s\n", g_args.outpath);
        fprintf(g_fpOfResult, "inpath: %s\n", g_args.inpath);
        fprintf(g_fpOfResult, "resultFile: %s\n", g_args.resultFile);
        fprintf(g_fpOfResult, "encode: %s\n", g_args.encode);
        fprintf(g_fpOfResult, "all_databases: %s\n", g_args.all_databases?"true":"false");
        fprintf(g_fpOfResult, "databases: %d\n", g_args.databases);
        fprintf(g_fpOfResult, "schemaonly: %s\n", g_args.schemaonly?"true":"false");
        fprintf(g_fpOfResult, "with_property: %s\n", g_args.with_property?"true":"false");
        fprintf(g_fpOfResult, "avro format: %s\n", g_args.avro?"true":"false");
        fprintf(g_fpOfResult, "start_time: %" PRId64 "\n", g_args.start_time);
        fprintf(g_fpOfResult, "end_time: %" PRId64 "\n", g_args.end_time);
        fprintf(g_fpOfResult, "precision: %s\n", g_args.precision);
        fprintf(g_fpOfResult, "data_batch: %d\n", g_args.data_batch);
        fprintf(g_fpOfResult, "max_sql_len: %d\n", g_args.max_sql_len);
        fprintf(g_fpOfResult, "table_batch: %d\n", g_args.table_batch);
        fprintf(g_fpOfResult, "thread_num: %d\n", g_args.thread_num);
        fprintf(g_fpOfResult, "allow_sys: %d\n", g_args.allow_sys);
        fprintf(g_fpOfResult, "abort: %d\n", g_args.abort);
        fprintf(g_fpOfResult, "isDumpIn: %d\n", g_args.isDumpIn);
        fprintf(g_fpOfResult, "arg_list_len: %d\n", g_args.arg_list_len);

        for (int32_t i = 0; i < g_args.arg_list_len; i++) {
            fprintf(g_fpOfResult, "arg_list[%d]: %s\n", i, g_args.arg_list[i]);
        }
    }

    g_numOfCores = (int32_t)sysconf(_SC_NPROCESSORS_ONLN);

    time_t tTime = time(NULL);
    struct tm tm = *localtime(&tTime);

    if (g_args.isDumpIn) {
        fprintf(g_fpOfResult, "============================== DUMP IN ============================== \n");
        fprintf(g_fpOfResult, "# DumpIn start time:                   %d-%02d-%02d %02d:%02d:%02d\n",
                tm.tm_year + 1900, tm.tm_mon + 1,
                tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);
        if (taosDumpIn() < 0) {
            ret = -1;
        }
    } else {
        fprintf(g_fpOfResult, "============================== DUMP OUT ============================== \n");
        fprintf(g_fpOfResult, "# DumpOut start time:                   %d-%02d-%02d %02d:%02d:%02d\n",
                tm.tm_year + 1900, tm.tm_mon + 1,
                tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);
        if (taosDumpOut() < 0) {
            ret = -1;
        } else {
            fprintf(g_fpOfResult, "\n============================== TOTAL STATISTICS ============================== \n");
            fprintf(g_fpOfResult, "# total database count:     %d\n",
                    g_resultStatistics.totalDatabasesOfDumpOut);
            fprintf(g_fpOfResult, "# total super table count:  %d\n",
                    g_resultStatistics.totalSuperTblsOfDumpOut);
            fprintf(g_fpOfResult, "# total child table count:  %"PRId64"\n",
                    g_resultStatistics.totalChildTblsOfDumpOut);
            fprintf(g_fpOfResult, "# total row count:          %"PRId64"\n",
                    g_resultStatistics.totalRowsOfDumpOut);
        }
    }

    fprintf(g_fpOfResult, "\n");
    fclose(g_fpOfResult);

    return ret;
}

static void taosFreeDbInfos() {
    if (g_dbInfos == NULL) return;
    for (int i = 0; i < 128; i++) tfree(g_dbInfos[i]);
    tfree(g_dbInfos);
}

// check table is normal table or super table
static int taosGetTableRecordInfo(
        char *table, STableRecordInfo *pTableRecordInfo, TAOS *taosCon) {
    TAOS_ROW row = NULL;
    bool isSet = false;
    TAOS_RES *result     = NULL;

    memset(pTableRecordInfo, 0, sizeof(STableRecordInfo));

    char* tempCommand = (char *)malloc(COMMAND_SIZE);
    if (tempCommand == NULL) {
        errorPrint("%s() LN%d, failed to allocate memory\n",
                __func__, __LINE__);
        return -1;
    }

    sprintf(tempCommand, "show tables like %s", table);

    result = taos_query(taosCon, tempCommand);
    int32_t code = taos_errno(result);

    if (code != 0) {
        errorPrint("%s() LN%d, failed to run command %s\n",
                __func__, __LINE__, tempCommand);
        free(tempCommand);
        taos_free_result(result);
        return -1;
    }

    TAOS_FIELD *fields = taos_fetch_fields(result);

    while ((row = taos_fetch_row(result)) != NULL) {
        isSet = true;
        pTableRecordInfo->isMetric = false;
        tstrncpy(pTableRecordInfo->tableRecord.name,
                (char *)row[TSDB_SHOW_TABLES_NAME_INDEX],
                min(TSDB_TABLE_NAME_LEN,
                    fields[TSDB_SHOW_TABLES_NAME_INDEX].bytes + 1));
        tstrncpy(pTableRecordInfo->tableRecord.metric,
                (char *)row[TSDB_SHOW_TABLES_METRIC_INDEX],
                min(TSDB_TABLE_NAME_LEN,
                    fields[TSDB_SHOW_TABLES_METRIC_INDEX].bytes + 1));
        break;
    }

    taos_free_result(result);
    result = NULL;

    if (isSet) {
        free(tempCommand);
        return 0;
    }

    sprintf(tempCommand, "show stables like %s", table);

    result = taos_query(taosCon, tempCommand);
    code = taos_errno(result);

    if (code != 0) {
        errorPrint("%s() LN%d, failed to run command %s\n",
                __func__, __LINE__, tempCommand);
        free(tempCommand);
        taos_free_result(result);
        return -1;
    }

    while ((row = taos_fetch_row(result)) != NULL) {
        isSet = true;
        pTableRecordInfo->isMetric = true;
        tstrncpy(pTableRecordInfo->tableRecord.metric, table,
                TSDB_TABLE_NAME_LEN);
        break;
    }

    taos_free_result(result);
    result = NULL;

    if (isSet) {
        free(tempCommand);
        return 0;
    }
    errorPrint("%s() LN%d, invalid table/metric %s\n",
            __func__, __LINE__, table);
    free(tempCommand);
    return -1;
}


static int32_t taosSaveAllNormalTableToTempFile(TAOS *taosCon, char*meter,
        char* metric, int* fd) {
    STableRecord tableRecord;

    if (-1 == *fd) {
        *fd = open(".tables.tmp.0",
                O_RDWR | O_CREAT, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH);
        if (*fd == -1) {
            errorPrint("%s() LN%d, failed to open temp file: .tables.tmp.0\n",
                    __func__, __LINE__);
            return -1;
        }
    }

    memset(&tableRecord, 0, sizeof(STableRecord));
    tstrncpy(tableRecord.name, meter, TSDB_TABLE_NAME_LEN);
    tstrncpy(tableRecord.metric, metric, TSDB_TABLE_NAME_LEN);

    taosWrite(*fd, &tableRecord, sizeof(STableRecord));
    return 0;
}

static int32_t taosSaveTableOfMetricToTempFile(
        TAOS *taosCon, char* metric,
        int32_t*  totalNumOfThread) {
    TAOS_ROW row;
    int fd = -1;
    STableRecord tableRecord;

    char* tmpCommand = (char *)malloc(COMMAND_SIZE);
    if (tmpCommand == NULL) {
        errorPrint("%s() LN%d, failed to allocate memory\n", __func__, __LINE__);
        return -1;
    }

    sprintf(tmpCommand, "select tbname from %s", metric);

    TAOS_RES *res = taos_query(taosCon, tmpCommand);
    int32_t code = taos_errno(res);
    if (code != 0) {
        errorPrint("%s() LN%d, failed to run command %s\n",
                __func__, __LINE__, tmpCommand);
        free(tmpCommand);
        taos_free_result(res);
        return -1;
    }
    free(tmpCommand);

    char     tmpBuf[MAX_FILE_NAME_LEN];
    memset(tmpBuf, 0, MAX_FILE_NAME_LEN);
    sprintf(tmpBuf, ".select-tbname.tmp");
    fd = open(tmpBuf, O_RDWR | O_CREAT | O_TRUNC, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH);
    if (fd == -1) {
        errorPrint("%s() LN%d, failed to open temp file: %s\n",
                __func__, __LINE__, tmpBuf);
        taos_free_result(res);
        return -1;
    }

    TAOS_FIELD *fields = taos_fetch_fields(res);

    int32_t  numOfTable  = 0;
    while ((row = taos_fetch_row(res)) != NULL) {

        memset(&tableRecord, 0, sizeof(STableRecord));
        tstrncpy(tableRecord.name, (char *)row[0], fields[0].bytes);
        tstrncpy(tableRecord.metric, metric, TSDB_TABLE_NAME_LEN);

        taosWrite(fd, &tableRecord, sizeof(STableRecord));
        numOfTable++;
    }
    taos_free_result(res);
    lseek(fd, 0, SEEK_SET);

    int maxThreads = g_args.thread_num;
    int tableOfPerFile ;
    if (numOfTable <= g_args.thread_num) {
        tableOfPerFile = 1;
        maxThreads = numOfTable;
    } else {
        tableOfPerFile = numOfTable / g_args.thread_num;
        if (0 != numOfTable % g_args.thread_num) {
            tableOfPerFile += 1;
        }
    }

    char* tblBuf = (char*)calloc(1, tableOfPerFile * sizeof(STableRecord));
    if (NULL == tblBuf){
        errorPrint("%s() LN%d, failed to calloc %" PRIzu "\n",
                __func__, __LINE__, tableOfPerFile * sizeof(STableRecord));
        close(fd);
        return -1;
    }

    int32_t  numOfThread = *totalNumOfThread;
    int      subFd = -1;
    for (; numOfThread <= maxThreads; numOfThread++) {
        memset(tmpBuf, 0, MAX_FILE_NAME_LEN);
        sprintf(tmpBuf, ".tables.tmp.%d", numOfThread);
        subFd = open(tmpBuf, O_RDWR | O_CREAT | O_TRUNC, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH);
        if (subFd == -1) {
            errorPrint("%s() LN%d, failed to open temp file: %s\n",
                    __func__, __LINE__, tmpBuf);
            for (int32_t loopCnt = 0; loopCnt < numOfThread; loopCnt++) {
                sprintf(tmpBuf, ".tables.tmp.%d", loopCnt);
                (void)remove(tmpBuf);
            }
            sprintf(tmpBuf, ".select-tbname.tmp");
            (void)remove(tmpBuf);
            free(tblBuf);
            close(fd);
            return -1;
        }

        // read tableOfPerFile for fd, write to subFd
        ssize_t readLen = read(fd, tblBuf, tableOfPerFile * sizeof(STableRecord));
        if (readLen <= 0) {
            close(subFd);
            break;
        }
        taosWrite(subFd, tblBuf, readLen);
        close(subFd);
    }

    sprintf(tmpBuf, ".select-tbname.tmp");
    (void)remove(tmpBuf);

    if (fd >= 0) {
        close(fd);
        fd = -1;
    }

    *totalNumOfThread = numOfThread;

    free(tblBuf);
    return 0;
}

static int taosDumpOut() {
    TAOS     *taos       = NULL;
    TAOS_RES *result     = NULL;
    char     *command    = NULL;

    TAOS_ROW row;
    FILE *fp = NULL;
    int32_t count = 0;
    STableRecordInfo tableRecordInfo;

    char tmpBuf[4096] = {0};
    if (g_args.outpath[0] != 0) {
        sprintf(tmpBuf, "%s/dbs.sql", g_args.outpath);
    } else {
        sprintf(tmpBuf, "dbs.sql");
    }

    fp = fopen(tmpBuf, "w");
    if (fp == NULL) {
        errorPrint("%s() LN%d, failed to open file %s\n",
                __func__, __LINE__, tmpBuf);
        return -1;
    }

    g_dbInfos = (SDbInfo **)calloc(128, sizeof(SDbInfo *));
    if (g_dbInfos == NULL) {
        errorPrint("%s() LN%d, failed to allocate memory\n",
                __func__, __LINE__);
        goto _exit_failure;
    }

    command = (char *)malloc(COMMAND_SIZE);
    if (command == NULL) {
        errorPrint("%s() LN%d, failed to allocate memory\n", __func__, __LINE__);
        goto _exit_failure;
    }

    /* Connect to server */
    taos = taos_connect(g_args.host, g_args.user, g_args.password,
            NULL, g_args.port);
    if (taos == NULL) {
        errorPrint("Failed to connect to TDengine server %s\n", g_args.host);
        goto _exit_failure;
    }

    /* --------------------------------- Main Code -------------------------------- */
    /* if (g_args.databases || g_args.all_databases) { // dump part of databases or all databases */
    /*  */
    taosDumpCharset(fp);

    sprintf(command, "show databases");
    result = taos_query(taos, command);
    int32_t code = taos_errno(result);

    if (code != 0) {
        errorPrint("%s() LN%d, failed to run command: %s, reason: %s\n",
                __func__, __LINE__, command, taos_errstr(result));
        goto _exit_failure;
    }

    TAOS_FIELD *fields = taos_fetch_fields(result);

    while ((row = taos_fetch_row(result)) != NULL) {
        // sys database name : 'log', but subsequent version changed to 'log'
        if ((strncasecmp(row[TSDB_SHOW_DB_NAME_INDEX], "log",
                        fields[TSDB_SHOW_DB_NAME_INDEX].bytes) == 0)
                && (!g_args.allow_sys)) {
            continue;
        }

        if (g_args.databases) {  // input multi dbs
            for (int i = 0; g_args.arg_list[i]; i++) {
                if (strncasecmp(g_args.arg_list[i],
                            (char *)row[TSDB_SHOW_DB_NAME_INDEX],
                            fields[TSDB_SHOW_DB_NAME_INDEX].bytes) == 0)
                    goto _dump_db_point;
            }
            continue;
        } else if (!g_args.all_databases) {  // only input one db
            if (strncasecmp(g_args.arg_list[0],
                        (char *)row[TSDB_SHOW_DB_NAME_INDEX],
                        fields[TSDB_SHOW_DB_NAME_INDEX].bytes) == 0)
                goto _dump_db_point;
            else
                continue;
        }

_dump_db_point:

        g_dbInfos[count] = (SDbInfo *)calloc(1, sizeof(SDbInfo));
        if (g_dbInfos[count] == NULL) {
            errorPrint("%s() LN%d, failed to allocate %"PRIu64" memory\n",
                    __func__, __LINE__, (uint64_t)sizeof(SDbInfo));
            goto _exit_failure;
        }

        tstrncpy(g_dbInfos[count]->name, (char *)row[TSDB_SHOW_DB_NAME_INDEX],
                min(TSDB_DB_NAME_LEN, fields[TSDB_SHOW_DB_NAME_INDEX].bytes + 1));
        if (g_args.with_property) {
            g_dbInfos[count]->ntables = *((int32_t *)row[TSDB_SHOW_DB_NTABLES_INDEX]);
            g_dbInfos[count]->vgroups = *((int32_t *)row[TSDB_SHOW_DB_VGROUPS_INDEX]);
            g_dbInfos[count]->replica = *((int16_t *)row[TSDB_SHOW_DB_REPLICA_INDEX]);
            g_dbInfos[count]->quorum = *((int16_t *)row[TSDB_SHOW_DB_QUORUM_INDEX]);
            g_dbInfos[count]->days = *((int16_t *)row[TSDB_SHOW_DB_DAYS_INDEX]);

            tstrncpy(g_dbInfos[count]->keeplist, (char *)row[TSDB_SHOW_DB_KEEP_INDEX],
                    min(32, fields[TSDB_SHOW_DB_KEEP_INDEX].bytes + 1));
            //g_dbInfos[count]->daysToKeep = *((int16_t *)row[TSDB_SHOW_DB_KEEP_INDEX]);
            //g_dbInfos[count]->daysToKeep1;
            //g_dbInfos[count]->daysToKeep2;
            g_dbInfos[count]->cache = *((int32_t *)row[TSDB_SHOW_DB_CACHE_INDEX]);
            g_dbInfos[count]->blocks = *((int32_t *)row[TSDB_SHOW_DB_BLOCKS_INDEX]);
            g_dbInfos[count]->minrows = *((int32_t *)row[TSDB_SHOW_DB_MINROWS_INDEX]);
            g_dbInfos[count]->maxrows = *((int32_t *)row[TSDB_SHOW_DB_MAXROWS_INDEX]);
            g_dbInfos[count]->wallevel = *((int8_t *)row[TSDB_SHOW_DB_WALLEVEL_INDEX]);
            g_dbInfos[count]->fsync = *((int32_t *)row[TSDB_SHOW_DB_FSYNC_INDEX]);
            g_dbInfos[count]->comp = (int8_t)(*((int8_t *)row[TSDB_SHOW_DB_COMP_INDEX]));
            g_dbInfos[count]->cachelast = (int8_t)(*((int8_t *)row[TSDB_SHOW_DB_CACHELAST_INDEX]));

            tstrncpy(g_dbInfos[count]->precision, (char *)row[TSDB_SHOW_DB_PRECISION_INDEX],
                    min(8, fields[TSDB_SHOW_DB_PRECISION_INDEX].bytes + 1));
            //g_dbInfos[count]->precision = *((int8_t *)row[TSDB_SHOW_DB_PRECISION_INDEX]);
            g_dbInfos[count]->update = *((int8_t *)row[TSDB_SHOW_DB_UPDATE_INDEX]);
        }
        count++;

        if (g_args.databases) {
            if (count > g_args.arg_list_len) break;

        } else if (!g_args.all_databases) {
            if (count >= 1) break;
        }
    }

    if (count == 0) {
        errorPrint("%d databases valid to dump\n", count);
        goto _exit_failure;
    }

    if (g_args.databases || g_args.all_databases) { // case: taosdump --databases dbx dby ...   OR  taosdump --all-databases
        for (int i = 0; i < count; i++) {
            taosDumpDb(g_dbInfos[i], fp, taos);
        }
    } else {
        if (g_args.arg_list_len == 1) {             // case: taosdump <db>
            taosDumpDb(g_dbInfos[0], fp, taos);
        } else {                                        // case: taosdump <db> tablex tabley ...
            taosDumpCreateDbClause(g_dbInfos[0], g_args.with_property, fp);
            fprintf(g_fpOfResult, "\n#### database:                       %s\n",
                    g_dbInfos[0]->name);
            g_resultStatistics.totalDatabasesOfDumpOut++;

            sprintf(command, "use %s", g_dbInfos[0]->name);

            result = taos_query(taos, command);
            code = taos_errno(result);
            if (code != 0) {
                errorPrint("invalid database %s\n", g_dbInfos[0]->name);
                goto _exit_failure;
            }

            fprintf(fp, "USE %s;\n\n", g_dbInfos[0]->name);

            int32_t totalNumOfThread = 1;  // 0: all normal talbe into .tables.tmp.0
            int  normalTblFd = -1;
            int32_t retCode;
            int superTblCnt = 0 ;
            for (int i = 1; g_args.arg_list[i]; i++) {
                if (taosGetTableRecordInfo(g_args.arg_list[i],
                            &tableRecordInfo, taos) < 0) {
                    errorPrint("input the invalide table %s\n",
                            g_args.arg_list[i]);
                    continue;
                }

                if (tableRecordInfo.isMetric) {  // dump all table of this metric
                    int ret = taosDumpStable(
                            tableRecordInfo.tableRecord.metric,
                            fp, taos, g_dbInfos[0]->name);
                    if (0 == ret) {
                        superTblCnt++;
                    }
                    retCode = taosSaveTableOfMetricToTempFile(
                            taos, tableRecordInfo.tableRecord.metric,
                            &totalNumOfThread);
                } else {
                    if (tableRecordInfo.tableRecord.metric[0] != '\0') {  // dump this sub table and it's metric
                        int ret = taosDumpStable(
                                tableRecordInfo.tableRecord.metric,
                                fp, taos, g_dbInfos[0]->name);
                        if (0 == ret) {
                            superTblCnt++;
                        }
                    }
                    retCode = taosSaveAllNormalTableToTempFile(
                            taos, tableRecordInfo.tableRecord.name,
                            tableRecordInfo.tableRecord.metric, &normalTblFd);
                }

                if (retCode < 0) {
                    if (-1 != normalTblFd){
                        taosClose(normalTblFd);
                    }
                    goto _clean_tmp_file;
                }
            }

            // TODO: save dump super table <superTblCnt> into result_output.txt
            fprintf(g_fpOfResult, "# super table counter:               %d\n",
                    superTblCnt);
            g_resultStatistics.totalSuperTblsOfDumpOut += superTblCnt;

            if (-1 != normalTblFd){
                taosClose(normalTblFd);
            }

            // start multi threads to dumpout
            taosStartDumpOutWorkThreads(totalNumOfThread,
                    g_dbInfos[0]->name);

            char tmpFileName[MAX_FILE_NAME_LEN];
_clean_tmp_file:
            for (int loopCnt = 0; loopCnt < totalNumOfThread; loopCnt++) {
                sprintf(tmpFileName, ".tables.tmp.%d", loopCnt);
                remove(tmpFileName);
            }
        }
    }

    /* Close the handle and return */
    fclose(fp);
    taos_close(taos);
    taos_free_result(result);
    tfree(command);
    taosFreeDbInfos();
    fprintf(stderr, "dump out rows: %" PRId64 "\n", g_totalDumpOutRows);
    return 0;

_exit_failure:
    fclose(fp);
    taos_close(taos);
    taos_free_result(result);
    tfree(command);
    taosFreeDbInfos();
    errorPrint("dump out rows: %" PRId64 "\n", g_totalDumpOutRows);
    return -1;
}

static int taosGetTableDes(
        char* dbName, char *table,
        STableDef *stableDes, TAOS* taosCon, bool isSuperTable) {
    TAOS_ROW row = NULL;
    TAOS_RES* res = NULL;
    int count = 0;

    char sqlstr[COMMAND_SIZE];
    sprintf(sqlstr, "describe %s.%s;", dbName, table);

    res = taos_query(taosCon, sqlstr);
    int32_t code = taos_errno(res);
    if (code != 0) {
        errorPrint("%s() LN%d, failed to run command <%s>, reason:%s\n",
                __func__, __LINE__, sqlstr, taos_errstr(res));
        taos_free_result(res);
        return -1;
    }

    TAOS_FIELD *fields = taos_fetch_fields(res);

    tstrncpy(stableDes->name, table, TSDB_TABLE_NAME_LEN);
    while ((row = taos_fetch_row(res)) != NULL) {
        tstrncpy(stableDes->cols[count].field,
                (char *)row[TSDB_DESCRIBE_METRIC_FIELD_INDEX],
                min(TSDB_COL_NAME_LEN + 1,
                    fields[TSDB_DESCRIBE_METRIC_FIELD_INDEX].bytes + 1));
        tstrncpy(stableDes->cols[count].type,
                (char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                min(16, fields[TSDB_DESCRIBE_METRIC_TYPE_INDEX].bytes + 1));
        stableDes->cols[count].length =
            *((int *)row[TSDB_DESCRIBE_METRIC_LENGTH_INDEX]);
        tstrncpy(stableDes->cols[count].note,
                (char *)row[TSDB_DESCRIBE_METRIC_NOTE_INDEX],
                min(COL_NOTE_LEN,
                    fields[TSDB_DESCRIBE_METRIC_NOTE_INDEX].bytes + 1));

        count++;
    }

    taos_free_result(res);
    res = NULL;

    if (isSuperTable) {
        return count;
    }

    // if chidl-table have tag, using  select tagName from table to get tagValue
    for (int i = 0 ; i < count; i++) {
        if (strcmp(stableDes->cols[i].note, "TAG") != 0) continue;


        sprintf(sqlstr, "select %s from %s.%s",
                stableDes->cols[i].field, dbName, table);

        res = taos_query(taosCon, sqlstr);
        code = taos_errno(res);
        if (code != 0) {
            errorPrint("%s() LN%d, failed to run command <%s>, reason:%s\n",
                    __func__, __LINE__, sqlstr, taos_errstr(res));
            taos_free_result(res);
            return -1;
        }

        fields = taos_fetch_fields(res);

        row = taos_fetch_row(res);
        if (NULL == row) {
            errorPrint("%s() LN%d, fetch failed to run command <%s>, reason:%s\n",
                    __func__, __LINE__, sqlstr, taos_errstr(res));
            taos_free_result(res);
            return -1;
        }

        if (row[0] == NULL) {
            sprintf(stableDes->cols[i].note, "%s", "NULL");
            taos_free_result(res);
            res = NULL;
            continue;
        }

        int32_t* length = taos_fetch_lengths(res);

        //int32_t* length = taos_fetch_lengths(tmpResult);
        switch (fields[0].type) {
            case TSDB_DATA_TYPE_BOOL:
                sprintf(stableDes->cols[i].note, "%d",
                        ((((int32_t)(*((char *)row[0]))) == 1) ? 1 : 0));
                break;
            case TSDB_DATA_TYPE_TINYINT:
                sprintf(stableDes->cols[i].note, "%d", *((int8_t *)row[0]));
                break;
            case TSDB_DATA_TYPE_SMALLINT:
                sprintf(stableDes->cols[i].note, "%d", *((int16_t *)row[0]));
                break;
            case TSDB_DATA_TYPE_INT:
                sprintf(stableDes->cols[i].note, "%d", *((int32_t *)row[0]));
                break;
            case TSDB_DATA_TYPE_BIGINT:
                sprintf(stableDes->cols[i].note, "%" PRId64 "", *((int64_t *)row[0]));
                break;
            case TSDB_DATA_TYPE_FLOAT:
                sprintf(stableDes->cols[i].note, "%f", GET_FLOAT_VAL(row[0]));
                break;
            case TSDB_DATA_TYPE_DOUBLE:
                sprintf(stableDes->cols[i].note, "%f", GET_DOUBLE_VAL(row[0]));
                break;
            case TSDB_DATA_TYPE_BINARY:
                {
                    memset(stableDes->cols[i].note, 0, sizeof(stableDes->cols[i].note));
                    stableDes->cols[i].note[0] = '\'';
                    char tbuf[COL_NOTE_LEN];
                    converStringToReadable((char *)row[0], length[0], tbuf, COL_NOTE_LEN);
                    char* pstr = stpcpy(&(stableDes->cols[i].note[1]), tbuf);
                    *(pstr++) = '\'';
                    break;
                }
            case TSDB_DATA_TYPE_NCHAR:
                {
                    memset(stableDes->cols[i].note, 0, sizeof(stableDes->cols[i].note));
                    char tbuf[COL_NOTE_LEN-2];    // need reserve 2 bytes for ' '
                    convertNCharToReadable((char *)row[0], length[0], tbuf, COL_NOTE_LEN);
                    sprintf(stableDes->cols[i].note, "\'%s\'", tbuf);
                    break;
                }
            case TSDB_DATA_TYPE_TIMESTAMP:
                sprintf(stableDes->cols[i].note, "%" PRId64 "", *(int64_t *)row[0]);
#if 0
                if (!g_args.mysqlFlag) {
                    sprintf(tableDes->cols[i].note, "%" PRId64 "", *(int64_t *)row[0]);
                } else {
                    char buf[64] = "\0";
                    int64_t ts = *((int64_t *)row[0]);
                    time_t tt = (time_t)(ts / 1000);
                    struct tm *ptm = localtime(&tt);
                    strftime(buf, 64, "%y-%m-%d %H:%M:%S", ptm);
                    sprintf(tableDes->cols[i].note, "\'%s.%03d\'", buf, (int)(ts % 1000));
                }
#endif
                break;
            default:
                break;
        }

        taos_free_result(res);
        res = NULL;
    }

    return count;
}

static int convertSchemaToAvroSchema(STableDef *stableDes, char **avroSchema)
{
    errorPrint("%s() LN%d TODO: covert table schema to avro schema\n",
            __func__, __LINE__);
    return 0;
}

static int32_t taosDumpTable(
        char *tbName, char *metric,
        FILE *fp, TAOS* taosCon, char* dbName) {
    int count = 0;

    STableDef *tableDes = (STableDef *)calloc(1, sizeof(STableDef)
            + sizeof(SColDes) * TSDB_MAX_COLUMNS);

    if (metric != NULL && metric[0] != '\0') {  // dump table schema which is created by using super table
        /*
           count = taosGetTableDes(metric, tableDes, taosCon);

           if (count < 0) {
           free(tableDes);
           return -1;
           }

           taosDumpCreateTableClause(tableDes, count, fp);

           memset(tableDes, 0, sizeof(STableDef) + sizeof(SColDes) * TSDB_MAX_COLUMNS);
           */

        count = taosGetTableDes(dbName, tbName, tableDes, taosCon, false);

        if (count < 0) {
            free(tableDes);
            return -1;
        }

        // create child-table using super-table
        taosDumpCreateMTableClause(tableDes, metric, count, fp, dbName);

    } else {  // dump table definition
        count = taosGetTableDes(dbName, tbName, tableDes, taosCon, false);

        if (count < 0) {
            free(tableDes);
            return -1;
        }

        // create normal-table or super-table
        taosDumpCreateTableClause(tableDes, count, fp, dbName);
    }

    char *jsonAvroSchema = NULL;
    if (g_args.avro) {
        convertSchemaToAvroSchema(tableDes, &jsonAvroSchema);
    }

    free(tableDes);

    int32_t ret = 0;
    if (!g_args.schemaonly) {
        ret = taosDumpTableData(fp, tbName, taosCon, dbName,
            jsonAvroSchema);
    }

    return ret;
}

static void taosDumpCreateDbClause(
        SDbInfo *dbInfo, bool isDumpProperty, FILE *fp) {
    char sqlstr[TSDB_MAX_SQL_LEN] = {0};

    char *pstr = sqlstr;
    pstr += sprintf(pstr, "CREATE DATABASE IF NOT EXISTS %s ", dbInfo->name);
    if (isDumpProperty) {
        pstr += sprintf(pstr,
                "REPLICA %d QUORUM %d DAYS %d KEEP %s CACHE %d BLOCKS %d MINROWS %d MAXROWS %d FSYNC %d CACHELAST %d COMP %d PRECISION '%s' UPDATE %d",
                dbInfo->replica, dbInfo->quorum, dbInfo->days,
                dbInfo->keeplist,
                dbInfo->cache,
                dbInfo->blocks, dbInfo->minrows, dbInfo->maxrows,
                dbInfo->fsync,
                dbInfo->cachelast,
                dbInfo->comp, dbInfo->precision, dbInfo->update);
    }

    pstr += sprintf(pstr, ";");
    fprintf(fp, "%s\n\n", sqlstr);
}

static void* taosDumpOutWorkThreadFp(void *arg)
{
    SThreadParaObj *pThread = (SThreadParaObj*)arg;
    STableRecord    tableRecord;
    int fd;

    setThreadName("dumpOutWorkThrd");

    char tmpBuf[4096] = {0};
    sprintf(tmpBuf, ".tables.tmp.%d", pThread->threadIndex);
    fd = open(tmpBuf, O_RDWR | O_CREAT, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH);
    if (fd == -1) {
        errorPrint("%s() LN%d, failed to open temp file: %s\n",
                __func__, __LINE__, tmpBuf);
        return NULL;
    }

    FILE *fp = NULL;
    memset(tmpBuf, 0, 4096);

    if (g_args.outpath[0] != 0) {
        sprintf(tmpBuf, "%s/%s.tables.%d.sql",
                g_args.outpath, pThread->dbName, pThread->threadIndex);
    } else {
        sprintf(tmpBuf, "%s.tables.%d.sql",
                pThread->dbName, pThread->threadIndex);
    }

    fp = fopen(tmpBuf, "w");
    if (fp == NULL) {
        errorPrint("%s() LN%d, failed to open file %s\n",
                __func__, __LINE__, tmpBuf);
        close(fd);
        return NULL;
    }

    memset(tmpBuf, 0, 4096);
    sprintf(tmpBuf, "use %s", pThread->dbName);

    TAOS_RES* tmpResult = taos_query(pThread->taosCon, tmpBuf);
    int32_t code = taos_errno(tmpResult);
    if (code != 0) {
        errorPrint("%s() LN%d, invalid database %s. reason: %s\n",
                __func__, __LINE__, pThread->dbName, taos_errstr(tmpResult));
        taos_free_result(tmpResult);
        fclose(fp);
        close(fd);
        return NULL;
    }

#if 0
    int     fileNameIndex = 1;
    int     tablesInOneFile = 0;
#endif
    int64_t lastRowsPrint = 5000000;
    fprintf(fp, "USE %s;\n\n", pThread->dbName);
    while (1) {
        ssize_t readLen = read(fd, &tableRecord, sizeof(STableRecord));
        if (readLen <= 0) break;

        int ret = taosDumpTable(
                tableRecord.name, tableRecord.metric,
                fp, pThread->taosCon, pThread->dbName);
        if (ret >= 0) {
            // TODO: sum table count and table rows by self
            pThread->tablesOfDumpOut++;
            pThread->rowsOfDumpOut += ret;

            if (pThread->rowsOfDumpOut >= lastRowsPrint) {
                printf(" %"PRId64 " rows already be dumpout from database %s\n",
                        pThread->rowsOfDumpOut, pThread->dbName);
                lastRowsPrint += 5000000;
            }

#if 0
            tablesInOneFile++;
            if (tablesInOneFile >= g_args.table_batch) {
                fclose(fp);
                tablesInOneFile = 0;

                memset(tmpBuf, 0, 4096);
                if (g_args.outpath[0] != 0) {
                    sprintf(tmpBuf, "%s/%s.tables.%d-%d.sql",
                            g_args.outpath, pThread->dbName,
                            pThread->threadIndex, fileNameIndex);
                } else {
                    sprintf(tmpBuf, "%s.tables.%d-%d.sql",
                            pThread->dbName, pThread->threadIndex, fileNameIndex);
                }
                fileNameIndex++;

                fp = fopen(tmpBuf, "w");
                if (fp == NULL) {
                    errorPrint("%s() LN%d, failed to open file %s\n",
                            __func__, __LINE__, tmpBuf);
                    close(fd);
                    taos_free_result(tmpResult);
                    return NULL;
                }
            }
#endif
        }
    }

    taos_free_result(tmpResult);
    close(fd);
    fclose(fp);

    return NULL;
}

static void taosStartDumpOutWorkThreads(int32_t  numOfThread, char *dbName)
{
    pthread_attr_t thattr;
    SThreadParaObj *threadObj =
        (SThreadParaObj *)calloc(numOfThread, sizeof(SThreadParaObj));

    if (threadObj == NULL) {
        errorPrint("%s() LN%d, memory allocation failed!\n",
                __func__, __LINE__);
        return;
    }

    for (int t = 0; t < numOfThread; ++t) {
        SThreadParaObj *pThread = threadObj + t;
        pThread->rowsOfDumpOut = 0;
        pThread->tablesOfDumpOut = 0;
        pThread->threadIndex = t;
        pThread->totalThreads = numOfThread;
        tstrncpy(pThread->dbName, dbName, TSDB_DB_NAME_LEN);
        pThread->taosCon = taos_connect(g_args.host, g_args.user, g_args.password,
            NULL, g_args.port);
        if (pThread->taosCon == NULL) {
            errorPrint("Failed to connect to TDengine server %s\n", g_args.host);
            free(threadObj);
            return;
        }
        pthread_attr_init(&thattr);
        pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);

        if (pthread_create(&(pThread->threadID), &thattr,
                    taosDumpOutWorkThreadFp,
                    (void*)pThread) != 0) {
            errorPrint("%s() LN%d, thread:%d failed to start\n",
                   __func__, __LINE__, pThread->threadIndex);
            exit(-1);
        }
    }

    for (int32_t t = 0; t < numOfThread; ++t) {
        pthread_join(threadObj[t].threadID, NULL);
    }

    // TODO: sum all thread dump table count and rows of per table, then save into result_output.txt
    int64_t   totalRowsOfDumpOut = 0;
    int64_t   totalChildTblsOfDumpOut = 0;
    for (int32_t t = 0; t < numOfThread; ++t) {
        totalChildTblsOfDumpOut += threadObj[t].tablesOfDumpOut;
        totalRowsOfDumpOut      += threadObj[t].rowsOfDumpOut;
    }

    fprintf(g_fpOfResult, "# child table counter:               %"PRId64"\n",
            totalChildTblsOfDumpOut);
    fprintf(g_fpOfResult, "# row counter:                       %"PRId64"\n",
            totalRowsOfDumpOut);
    g_resultStatistics.totalChildTblsOfDumpOut += totalChildTblsOfDumpOut;
    g_resultStatistics.totalRowsOfDumpOut      += totalRowsOfDumpOut;
    free(threadObj);
}

static int32_t taosDumpStable(char *table, FILE *fp,
        TAOS* taosCon, char* dbName) {

    uint64_t sizeOfTableDes =
        (uint64_t)(sizeof(STableDef) + sizeof(SColDes) * TSDB_MAX_COLUMNS);
    STableDef *stableDes = (STableDef *)calloc(1, sizeOfTableDes);
    if (NULL == stableDes) {
        errorPrint("%s() LN%d, failed to allocate %"PRIu64" memory\n",
                __func__, __LINE__, sizeOfTableDes);
        exit(-1);
    }

    int count = taosGetTableDes(dbName, table, stableDes, taosCon, true);

    if (count < 0) {
        free(stableDes);
        errorPrint("%s() LN%d, failed to get stable[%s] schema\n",
               __func__, __LINE__, table);
        exit(-1);
    }

    taosDumpCreateTableClause(stableDes, count, fp, dbName);

    free(stableDes);
    return 0;
}

static int32_t taosDumpCreateSuperTableClause(TAOS* taosCon, char* dbName, FILE *fp)
{
    TAOS_ROW row;
    int fd = -1;
    STableRecord tableRecord;
    char sqlstr[TSDB_MAX_SQL_LEN] = {0};

    sprintf(sqlstr, "show %s.stables", dbName);

    TAOS_RES* res = taos_query(taosCon, sqlstr);
    int32_t  code = taos_errno(res);
    if (code != 0) {
        errorPrint("%s() LN%d, failed to run command <%s>, reason: %s\n",
                __func__, __LINE__, sqlstr, taos_errstr(res));
        taos_free_result(res);
        exit(-1);
    }

    TAOS_FIELD *fields = taos_fetch_fields(res);

    char     tmpFileName[MAX_FILE_NAME_LEN];
    memset(tmpFileName, 0, MAX_FILE_NAME_LEN);
    sprintf(tmpFileName, ".stables.tmp");
    fd = open(tmpFileName, O_RDWR | O_CREAT, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH);
    if (fd == -1) {
        errorPrint("%s() LN%d, failed to open temp file: %s\n",
                __func__, __LINE__, tmpFileName);
        taos_free_result(res);
        (void)remove(".stables.tmp");
        exit(-1);
    }

    while ((row = taos_fetch_row(res)) != NULL) {
        memset(&tableRecord, 0, sizeof(STableRecord));
        tstrncpy(tableRecord.name, (char *)row[TSDB_SHOW_TABLES_NAME_INDEX],
                min(TSDB_TABLE_NAME_LEN,
                    fields[TSDB_SHOW_TABLES_NAME_INDEX].bytes + 1));
        taosWrite(fd, &tableRecord, sizeof(STableRecord));
    }

    taos_free_result(res);
    (void)lseek(fd, 0, SEEK_SET);

    int superTblCnt = 0;
    while (1) {
        ssize_t readLen = read(fd, &tableRecord, sizeof(STableRecord));
        if (readLen <= 0) break;

        int ret = taosDumpStable(tableRecord.name, fp, taosCon, dbName);
        if (0 == ret) {
            superTblCnt++;
        }
    }

    // TODO: save dump super table <superTblCnt> into result_output.txt
    fprintf(g_fpOfResult, "# super table counter:               %d\n", superTblCnt);
    g_resultStatistics.totalSuperTblsOfDumpOut += superTblCnt;

    close(fd);
    (void)remove(".stables.tmp");

    return 0;
}


static int taosDumpDb(SDbInfo *dbInfo, FILE *fp, TAOS *taosCon) {
    TAOS_ROW row;
    int fd = -1;
    STableRecord tableRecord;

    taosDumpCreateDbClause(dbInfo, g_args.with_property, fp);

    fprintf(g_fpOfResult, "\n#### database:                       %s\n",
            dbInfo->name);
    g_resultStatistics.totalDatabasesOfDumpOut++;

    char sqlstr[TSDB_MAX_SQL_LEN] = {0};

    fprintf(fp, "USE %s;\n\n", dbInfo->name);

    (void)taosDumpCreateSuperTableClause(taosCon, dbInfo->name, fp);

    sprintf(sqlstr, "show %s.tables", dbInfo->name);

    TAOS_RES* res = taos_query(taosCon, sqlstr);
    int code = taos_errno(res);
    if (code != 0) {
        errorPrint("%s() LN%d, failed to run command <%s>, reason:%s\n",
                __func__, __LINE__, sqlstr, taos_errstr(res));
        taos_free_result(res);
        return -1;
    }

    char tmpBuf[MAX_FILE_NAME_LEN];
    memset(tmpBuf, 0, MAX_FILE_NAME_LEN);
    sprintf(tmpBuf, ".show-tables.tmp");
    fd = open(tmpBuf, O_RDWR | O_CREAT | O_TRUNC, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH);
    if (fd == -1) {
        errorPrint("%s() LN%d, failed to open temp file: %s\n",
                __func__, __LINE__, tmpBuf);
        taos_free_result(res);
        return -1;
    }

    TAOS_FIELD *fields = taos_fetch_fields(res);

    int32_t  numOfTable  = 0;
    while ((row = taos_fetch_row(res)) != NULL) {
        memset(&tableRecord, 0, sizeof(STableRecord));
        tstrncpy(tableRecord.name, (char *)row[TSDB_SHOW_TABLES_NAME_INDEX],
                min(TSDB_TABLE_NAME_LEN,
                    fields[TSDB_SHOW_TABLES_NAME_INDEX].bytes + 1));
        tstrncpy(tableRecord.metric, (char *)row[TSDB_SHOW_TABLES_METRIC_INDEX],
                min(TSDB_TABLE_NAME_LEN,
                    fields[TSDB_SHOW_TABLES_METRIC_INDEX].bytes + 1));

        taosWrite(fd, &tableRecord, sizeof(STableRecord));

        numOfTable++;
    }
    taos_free_result(res);
    lseek(fd, 0, SEEK_SET);

    int maxThreads = g_args.thread_num;
    int tableOfPerFile ;
    if (numOfTable <= g_args.thread_num) {
        tableOfPerFile = 1;
        maxThreads = numOfTable;
    } else {
        tableOfPerFile = numOfTable / g_args.thread_num;
        if (0 != numOfTable % g_args.thread_num) {
            tableOfPerFile += 1;
        }
    }

    char* tblBuf = (char*)calloc(1, tableOfPerFile * sizeof(STableRecord));
    if (NULL == tblBuf){
        errorPrint("failed to calloc %" PRIzu "\n",
                tableOfPerFile * sizeof(STableRecord));
        close(fd);
        return -1;
    }

    int32_t  numOfThread = 0;
    int      subFd = -1;
    for (numOfThread = 0; numOfThread < maxThreads; numOfThread++) {
        memset(tmpBuf, 0, MAX_FILE_NAME_LEN);
        sprintf(tmpBuf, ".tables.tmp.%d", numOfThread);
        subFd = open(tmpBuf, O_RDWR | O_CREAT | O_TRUNC, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH);
        if (subFd == -1) {
            errorPrint("%s() LN%d, failed to open temp file: %s\n",
                    __func__, __LINE__, tmpBuf);
            for (int32_t loopCnt = 0; loopCnt < numOfThread; loopCnt++) {
                sprintf(tmpBuf, ".tables.tmp.%d", loopCnt);
                (void)remove(tmpBuf);
            }
            sprintf(tmpBuf, ".show-tables.tmp");
            (void)remove(tmpBuf);
            free(tblBuf);
            close(fd);
            return -1;
        }

        // read tableOfPerFile for fd, write to subFd
        ssize_t readLen = read(fd, tblBuf, tableOfPerFile * sizeof(STableRecord));
        if (readLen <= 0) {
            close(subFd);
            break;
        }
        taosWrite(subFd, tblBuf, readLen);
        close(subFd);
    }

    sprintf(tmpBuf, ".show-tables.tmp");
    (void)remove(tmpBuf);

    if (fd >= 0) {
        close(fd);
        fd = -1;
    }

    // start multi threads to dumpout
    taosStartDumpOutWorkThreads(numOfThread, dbInfo->name);
    for (int loopCnt = 0; loopCnt < numOfThread; loopCnt++) {
        sprintf(tmpBuf, ".tables.tmp.%d", loopCnt);
        (void)remove(tmpBuf);
    }

    free(tblBuf);
    return 0;
}

static void taosDumpCreateTableClause(STableDef *tableDes, int numOfCols,
        FILE *fp, char* dbName) {
    int counter = 0;
    int count_temp = 0;
    char sqlstr[COMMAND_SIZE];

    char* pstr = sqlstr;

    pstr += sprintf(sqlstr, "CREATE TABLE IF NOT EXISTS %s.%s",
            dbName, tableDes->name);

    for (; counter < numOfCols; counter++) {
        if (tableDes->cols[counter].note[0] != '\0') break;

        if (counter == 0) {
            pstr += sprintf(pstr, " (%s %s",
                    tableDes->cols[counter].field, tableDes->cols[counter].type);
        } else {
            pstr += sprintf(pstr, ", %s %s",
                    tableDes->cols[counter].field, tableDes->cols[counter].type);
        }

        if (strcasecmp(tableDes->cols[counter].type, "binary") == 0 ||
                strcasecmp(tableDes->cols[counter].type, "nchar") == 0) {
            pstr += sprintf(pstr, "(%d)", tableDes->cols[counter].length);
        }
    }

    count_temp = counter;

    for (; counter < numOfCols; counter++) {
        if (counter == count_temp) {
            pstr += sprintf(pstr, ") TAGS (%s %s",
                    tableDes->cols[counter].field, tableDes->cols[counter].type);
        } else {
            pstr += sprintf(pstr, ", %s %s",
                    tableDes->cols[counter].field, tableDes->cols[counter].type);
        }

        if (strcasecmp(tableDes->cols[counter].type, "binary") == 0 ||
                strcasecmp(tableDes->cols[counter].type, "nchar") == 0) {
            pstr += sprintf(pstr, "(%d)", tableDes->cols[counter].length);
        }
    }

    pstr += sprintf(pstr, ");");

    fprintf(fp, "%s\n\n", sqlstr);
}

static void taosDumpCreateMTableClause(STableDef *tableDes, char *metric,
        int numOfCols, FILE *fp, char* dbName) {
    int counter = 0;
    int count_temp = 0;

    char* tmpBuf = (char *)malloc(COMMAND_SIZE);
    if (tmpBuf == NULL) {
        errorPrint("%s() LN%d, failed to allocate %d memory\n",
               __func__, __LINE__, COMMAND_SIZE);
        return;
    }

    char *pstr = NULL;
    pstr = tmpBuf;

    pstr += sprintf(tmpBuf,
            "CREATE TABLE IF NOT EXISTS %s.%s USING %s.%s TAGS (",
            dbName, tableDes->name, dbName, metric);

    for (; counter < numOfCols; counter++) {
        if (tableDes->cols[counter].note[0] != '\0') break;
    }

    assert(counter < numOfCols);
    count_temp = counter;

    for (; counter < numOfCols; counter++) {
        if (counter != count_temp) {
            if (strcasecmp(tableDes->cols[counter].type, "binary") == 0 ||
                    strcasecmp(tableDes->cols[counter].type, "nchar") == 0) {
                //pstr += sprintf(pstr, ", \'%s\'", tableDes->cols[counter].note);
                pstr += sprintf(pstr, ", %s", tableDes->cols[counter].note);
            } else {
                pstr += sprintf(pstr, ", %s", tableDes->cols[counter].note);
            }
        } else {
            if (strcasecmp(tableDes->cols[counter].type, "binary") == 0 ||
                    strcasecmp(tableDes->cols[counter].type, "nchar") == 0) {
                //pstr += sprintf(pstr, "\'%s\'", tableDes->cols[counter].note);
                pstr += sprintf(pstr, "%s", tableDes->cols[counter].note);
            } else {
                pstr += sprintf(pstr, "%s", tableDes->cols[counter].note);
            }
            /* pstr += sprintf(pstr, "%s", tableDes->cols[counter].note); */
        }

        /* if (strcasecmp(tableDes->cols[counter].type, "binary") == 0 || strcasecmp(tableDes->cols[counter].type, "nchar")
         * == 0) { */
        /*     pstr += sprintf(pstr, "(%d)", tableDes->cols[counter].length); */
        /* } */
    }

    pstr += sprintf(pstr, ");");

    fprintf(fp, "%s\n", tmpBuf);
    free(tmpBuf);
}

static int writeSchemaToAvro(char *jsonAvroSchema)
{
    errorPrint("%s() LN%d, TODO: implement write schema to avro",
            __func__, __LINE__);
    return 0;
}

static int64_t writeResultToAvro(TAOS_RES *res)
{
    errorPrint("%s() LN%d, TODO: implementation need\n", __func__, __LINE__);
    return 0;
}

static int64_t writeResultToSql(TAOS_RES *res, FILE *fp, char *dbName, char *tbName)
{
    int64_t    totalRows     = 0;

    int32_t  sql_buf_len = g_args.max_sql_len;
    char* tmpBuffer = (char *)calloc(1, sql_buf_len + 128);
    if (tmpBuffer == NULL) {
        errorPrint("failed to allocate %d memory\n", sql_buf_len + 128);
        return -1;
    }

    char *pstr = tmpBuffer;

    TAOS_ROW row = NULL;
    int numFields = 0;
    int rowFlag = 0;
    int64_t    lastRowsPrint = 5000000;
    int count = 0;

    numFields = taos_field_count(res);
    assert(numFields > 0);
    TAOS_FIELD *fields = taos_fetch_fields(res);

    int32_t  curr_sqlstr_len = 0;
    int32_t  total_sqlstr_len = 0;

    while ((row = taos_fetch_row(res)) != NULL) {
        curr_sqlstr_len = 0;

        int32_t* length = taos_fetch_lengths(res);   // act len

        if (count == 0) {
            total_sqlstr_len = 0;
            curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len,
                    "INSERT INTO %s.%s VALUES (", dbName, tbName);
        } else {
            if (g_args.mysqlFlag) {
                if (0 == rowFlag) {
                    curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, "(");
                    rowFlag++;
                } else {
                    curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, ", (");
                }
            } else {
                curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, "(");
            }
        }

        for (int col = 0; col < numFields; col++) {
            if (col != 0) curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, ", ");

            if (row[col] == NULL) {
                curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, "NULL");
                continue;
            }

            switch (fields[col].type) {
                case TSDB_DATA_TYPE_BOOL:
                    curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, "%d",
                            ((((int32_t)(*((char *)row[col]))) == 1) ? 1 : 0));
                    break;
                case TSDB_DATA_TYPE_TINYINT:
                    curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, "%d", *((int8_t *)row[col]));
                    break;
                case TSDB_DATA_TYPE_SMALLINT:
                    curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, "%d", *((int16_t *)row[col]));
                    break;
                case TSDB_DATA_TYPE_INT:
                    curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, "%d", *((int32_t *)row[col]));
                    break;
                case TSDB_DATA_TYPE_BIGINT:
                    curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, "%" PRId64 "",
                            *((int64_t *)row[col]));
                    break;
                case TSDB_DATA_TYPE_FLOAT:
                    curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, "%f", GET_FLOAT_VAL(row[col]));
                    break;
                case TSDB_DATA_TYPE_DOUBLE:
                    curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, "%f", GET_DOUBLE_VAL(row[col]));
                    break;
                case TSDB_DATA_TYPE_BINARY:
                    {
                        char tbuf[COMMAND_SIZE] = {0};
                        //*(pstr++) = '\'';
                        converStringToReadable((char *)row[col], length[col], tbuf, COMMAND_SIZE);
                        //pstr = stpcpy(pstr, tbuf);
                        //*(pstr++) = '\'';
                        curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, "\'%s\'", tbuf);
                        break;
                    }
                case TSDB_DATA_TYPE_NCHAR:
                    {
                        char tbuf[COMMAND_SIZE] = {0};
                        convertNCharToReadable((char *)row[col], length[col], tbuf, COMMAND_SIZE);
                        curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, "\'%s\'", tbuf);
                        break;
                    }
                case TSDB_DATA_TYPE_TIMESTAMP:
                    if (!g_args.mysqlFlag) {
                        curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, "%" PRId64 "",
                                *(int64_t *)row[col]);
                    } else {
                        char buf[64] = "\0";
                        int64_t ts = *((int64_t *)row[col]);
                        time_t tt = (time_t)(ts / 1000);
                        struct tm *ptm = localtime(&tt);
                        strftime(buf, 64, "%y-%m-%d %H:%M:%S", ptm);
                        curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, "\'%s.%03d\'",
                                buf, (int)(ts % 1000));
                    }
                    break;
                default:
                    break;
            }
        }

        curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, ")");

        totalRows++;
        count++;
        fprintf(fp, "%s", tmpBuffer);

        if (totalRows >= lastRowsPrint) {
            printf(" %"PRId64 " rows already be dumpout from %s.%s\n",
                    totalRows, dbName, tbName);
            lastRowsPrint += 5000000;
        }

        total_sqlstr_len += curr_sqlstr_len;

        if ((count >= g_args.data_batch)
                || (sql_buf_len - total_sqlstr_len < TSDB_MAX_BYTES_PER_ROW)) {
            fprintf(fp, ";\n");
            count = 0;
        }
    }

    debugPrint("total_sqlstr_len: %d\n", total_sqlstr_len);

    fprintf(fp, "\n");
    atomic_add_fetch_64(&g_totalDumpOutRows, totalRows);
    free(tmpBuffer);

    return 0;
}

static int taosDumpTableData(FILE *fp, char *tbName,
        TAOS* taosCon, char* dbName,
        char *jsonAvroSchema) {
    int64_t    totalRows     = 0;

    char sqlstr[1024] = {0};
    sprintf(sqlstr,
            "select * from %s.%s where _c0 >= %" PRId64 " and _c0 <= %" PRId64 " order by _c0 asc;",
            dbName, tbName, g_args.start_time, g_args.end_time);

    TAOS_RES* res = taos_query(taosCon, sqlstr);
    int32_t code = taos_errno(res);
    if (code != 0) {
        errorPrint("failed to run command %s, reason: %s\n",
                sqlstr, taos_errstr(res));
        taos_free_result(res);
        return -1;
    }

    if (g_args.avro) {
        writeSchemaToAvro(jsonAvroSchema);
        totalRows = writeResultToAvro(res);
    } else {
        totalRows = writeResultToSql(res, fp, dbName, tbName);
    }

    taos_free_result(res);
    return totalRows;
}

static int taosCheckParam(struct arguments *arguments) {
    if (g_args.all_databases && g_args.databases) {
        fprintf(stderr, "conflict option --all-databases and --databases\n");
        return -1;
    }

    if (g_args.start_time > g_args.end_time) {
        fprintf(stderr, "start time is larger than end time\n");
        return -1;
    }

    if (g_args.arg_list_len == 0) {
        if ((!g_args.all_databases) && (!g_args.isDumpIn)) {
            errorPrint("%s", "taosdump requires parameters for database and operation\n");
            return -1;
        }
    }
    /*
       if (g_args.isDumpIn && (strcmp(g_args.outpath, DEFAULT_DUMP_FILE) != 0)) {
       fprintf(stderr, "duplicate parameter input and output file path\n");
       return -1;
       }
       */
    if (!g_args.isDumpIn && g_args.encode != NULL) {
        fprintf(stderr, "invalid option in dump out\n");
        return -1;
    }

    if (g_args.table_batch <= 0) {
        fprintf(stderr, "invalid option in dump out\n");
        return -1;
    }

    return 0;
}

/*
static bool isEmptyCommand(char *cmd) {
  char *pchar = cmd;

  while (*pchar != '\0') {
    if (*pchar != ' ') return false;
    pchar++;
  }

  return true;
}

static void taosReplaceCtrlChar(char *str) {
  bool ctrlOn = false;
  char *pstr = NULL;

  for (pstr = str; *str != '\0'; ++str) {
    if (ctrlOn) {
      switch (*str) {
        case 'n':
          *pstr = '\n';
          pstr++;
          break;
        case 'r':
          *pstr = '\r';
          pstr++;
          break;
        case 't':
          *pstr = '\t';
          pstr++;
          break;
        case '\\':
          *pstr = '\\';
          pstr++;
          break;
        case '\'':
          *pstr = '\'';
          pstr++;
          break;
        default:
          break;
      }
      ctrlOn = false;
    } else {
      if (*str == '\\') {
        ctrlOn = true;
      } else {
        *pstr = *str;
        pstr++;
      }
    }
  }

  *pstr = '\0';
}
*/

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

static int converStringToReadable(char *str, int size, char *buf, int bufsize) {
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
    // TODO
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

static void taosDumpCharset(FILE *fp) {
    char charsetline[256];

    (void)fseek(fp, 0, SEEK_SET);
    sprintf(charsetline, "#!%s\n", tsCharset);
    (void)fwrite(charsetline, strlen(charsetline), 1, fp);
}

static void taosLoadFileCharset(FILE *fp, char *fcharset) {
    char * line = NULL;
    size_t line_size = 0;

    (void)fseek(fp, 0, SEEK_SET);
    ssize_t size = getline(&line, &line_size, fp);
    if (size <= 2) {
        goto _exit_no_charset;
    }

    if (strncmp(line, "#!", 2) != 0) {
        goto _exit_no_charset;
    }
    if (line[size - 1] == '\n') {
        line[size - 1] = '\0';
        size--;
    }
    strcpy(fcharset, line + 2);

    tfree(line);
    return;

_exit_no_charset:
    (void)fseek(fp, 0, SEEK_SET);
    *fcharset = '\0';
    tfree(line);
    return;
}

// ========  dumpIn support multi threads functions ================================//

static char    **g_tsDumpInSqlFiles   = NULL;
static int32_t   g_tsSqlFileNum = 0;
static char      g_tsDbSqlFile[MAX_FILE_NAME_LEN] = {0};
static char      g_tsCharset[64] = {0};

static int taosGetFilesNum(const char *directoryName,
        const char *prefix, const char *prefix2)
{
    char cmd[1024] = { 0 };

    if (prefix2)
        sprintf(cmd, "ls %s/*.%s %s/*.%s | wc -l ",
                directoryName, prefix, directoryName, prefix2);
    else
        sprintf(cmd, "ls %s/*.%s | wc -l ", directoryName, prefix);

    FILE *fp = popen(cmd, "r");
    if (fp == NULL) {
        errorPrint("failed to execute:%s, error:%s\n", cmd, strerror(errno));
        exit(-1);
    }

    int fileNum = 0;
    if (fscanf(fp, "%d", &fileNum) != 1) {
        errorPrint("failed to execute:%s, parse result error\n", cmd);
        exit(-1);
    }

    if (fileNum <= 0) {
        errorPrint("directory:%s is empry\n", directoryName);
        exit(-1);
    }

    pclose(fp);
    return fileNum;
}

static void taosParseDirectory(const char *directoryName,
        const char *prefix, const char *prefix2,
        char **fileArray, int totalFiles)
{
    char cmd[1024] = { 0 };

    if (prefix2) {
        sprintf(cmd, "ls %s/*.%s %s/*.%s | sort",
                directoryName, prefix, directoryName, prefix2);
    } else {
        sprintf(cmd, "ls %s/*.%s | sort", directoryName, prefix);
    }

    FILE *fp = popen(cmd, "r");
    if (fp == NULL) {
        errorPrint("failed to execute:%s, error:%s\n", cmd, strerror(errno));
        exit(-1);
    }

    int fileNum = 0;
    while (fscanf(fp, "%128s", fileArray[fileNum++])) {
        if (strcmp(fileArray[fileNum-1], g_tsDbSqlFile) == 0) {
            fileNum--;
        }
        if (fileNum >= totalFiles) {
            break;
        }
    }

    if (fileNum != totalFiles) {
        errorPrint("directory:%s changed while read\n", directoryName);
        pclose(fp);
        exit(-1);
    }

    pclose(fp);
}

static void taosCheckDatabasesSQLFile(const char *directoryName)
{
    char cmd[1024] = { 0 };
    sprintf(cmd, "ls %s/dbs.sql", directoryName);

    FILE *fp = popen(cmd, "r");
    if (fp == NULL) {
        errorPrint("failed to execute:%s, error:%s\n", cmd, strerror(errno));
        exit(-1);
    }

    while (fscanf(fp, "%128s", g_tsDbSqlFile)) {
        break;
    }

    pclose(fp);
}

static void taosMallocDumpFiles()
{
    g_tsDumpInSqlFiles = (char**)calloc(g_tsSqlFileNum, sizeof(char*));
    for (int i = 0; i < g_tsSqlFileNum; i++) {
        g_tsDumpInSqlFiles[i] = calloc(1, MAX_FILE_NAME_LEN);
    }
}

static void taosFreeDumpFiles()
{
    for (int i = 0; i < g_tsSqlFileNum; i++) {
        tfree(g_tsDumpInSqlFiles[i]);
    }
    tfree(g_tsDumpInSqlFiles);
}

static void taosGetDirectoryFileList(char *inputDir)
{
    struct stat fileStat;
    if (stat(inputDir, &fileStat) < 0) {
        errorPrint("%s not exist\n", inputDir);
        exit(-1);
    }

    if (fileStat.st_mode & S_IFDIR) {
        taosCheckDatabasesSQLFile(inputDir);
        if (g_args.avro)
            g_tsSqlFileNum = taosGetFilesNum(inputDir, "sql", "avro");
        else
            g_tsSqlFileNum += taosGetFilesNum(inputDir, "sql", NULL);

        int tsSqlFileNumOfTbls = g_tsSqlFileNum;
        if (g_tsDbSqlFile[0] != 0) {
            tsSqlFileNumOfTbls--;
        }
        taosMallocDumpFiles();
        if (0 != tsSqlFileNumOfTbls) {
            if (g_args.avro) {
                taosParseDirectory(inputDir, "sql", "avro",
                        g_tsDumpInSqlFiles, tsSqlFileNumOfTbls);
            } else {
                taosParseDirectory(inputDir, "sql", NULL,
                        g_tsDumpInSqlFiles, tsSqlFileNumOfTbls);
            }
        }
        fprintf(stdout, "\nstart to dispose %d files in %s\n",
                g_tsSqlFileNum, inputDir);
    } else {
        errorPrint("%s is not a directory\n", inputDir);
        exit(-1);
    }
}

static FILE* taosOpenDumpInFile(char *fptr) {
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
            errorPrint("%s() LN%d, failed to open file %s\n",
                    __func__, __LINE__, fname);
        }
    }

    wordfree(&full_path);
    return f;
}

static int taosDumpInOneFile(TAOS* taos, FILE* fp, char* fcharset,
        char* encode, char* fileName) {
    int       read_len = 0;
    char *    cmd      = NULL;
    size_t    cmd_len  = 0;
    char *    line     = NULL;
    size_t    line_len = 0;

    cmd  = (char *)malloc(TSDB_MAX_ALLOWED_SQL_LEN);
    if (cmd == NULL) {
        errorPrint("%s() LN%d, failed to allocate memory\n",
                __func__, __LINE__);
        return -1;
    }

    int lastRowsPrint = 5000000;
    int lineNo = 0;
    while ((read_len = getline(&line, &line_len, fp)) != -1) {
        ++lineNo;
        if (read_len >= TSDB_MAX_ALLOWED_SQL_LEN) continue;
        line[--read_len] = '\0';

        //if (read_len == 0 || isCommentLine(line)) {  // line starts with #
        if (read_len == 0 ) {
            continue;
        }

        if (line[read_len - 1] == '\\') {
            line[read_len - 1] = ' ';
            memcpy(cmd + cmd_len, line, read_len);
            cmd_len += read_len;
            continue;
        }

        memcpy(cmd + cmd_len, line, read_len);
        cmd[read_len + cmd_len]= '\0';
        if (queryDbImpl(taos, cmd)) {
            errorPrint("%s() LN%d, error sql: linenu:%d, file:%s\n",
                    __func__, __LINE__, lineNo, fileName);
            fprintf(g_fpOfResult, "error sql: linenu:%d, file:%s\n", lineNo, fileName);
        }

        memset(cmd, 0, TSDB_MAX_ALLOWED_SQL_LEN);
        cmd_len = 0;

        if (lineNo >= lastRowsPrint) {
            printf(" %d lines already be executed from file %s\n", lineNo, fileName);
            lastRowsPrint += 5000000;
        }
    }

    tfree(cmd);
    tfree(line);
    fclose(fp);
    return 0;
}

static void* taosDumpInWorkThreadFp(void *arg)
{
    SThreadParaObj *pThread = (SThreadParaObj*)arg;
    setThreadName("dumpInWorkThrd");

    for (int32_t f = 0; f < g_tsSqlFileNum; ++f) {
        if (f % pThread->totalThreads == pThread->threadIndex) {
            char *SQLFileName = g_tsDumpInSqlFiles[f];
            FILE* fp = taosOpenDumpInFile(SQLFileName);
            if (NULL == fp) {
                continue;
            }
            fprintf(stderr, ", Success Open input file: %s\n",
                    SQLFileName);
            taosDumpInOneFile(pThread->taosCon, fp, g_tsCharset, g_args.encode, SQLFileName);
        }
    }

    return NULL;
}

static void taosStartDumpInWorkThreads()
{
    pthread_attr_t  thattr;
    SThreadParaObj *pThread;
    int32_t         totalThreads = g_args.thread_num;

    if (totalThreads > g_tsSqlFileNum) {
        totalThreads = g_tsSqlFileNum;
    }

    SThreadParaObj *threadObj = (SThreadParaObj *)calloc(
            totalThreads, sizeof(SThreadParaObj));

    if (NULL == threadObj) {
        errorPrint("%s() LN%d, memory allocation failed\n", __func__, __LINE__);
    }

    for (int32_t t = 0; t < totalThreads; ++t) {
        pThread = threadObj + t;
        pThread->threadIndex = t;
        pThread->totalThreads = totalThreads;
        pThread->taosCon = taos_connect(g_args.host, g_args.user, g_args.password,
            NULL, g_args.port);
        if (pThread->taosCon == NULL) {
            errorPrint("Failed to connect to TDengine server %s\n", g_args.host);
            free(threadObj);
            return;
        }
        pthread_attr_init(&thattr);
        pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);

        if (pthread_create(&(pThread->threadID), &thattr,
                    taosDumpInWorkThreadFp, (void*)pThread) != 0) {
            errorPrint("%s() LN%d, thread:%d failed to start\n",
                    __func__, __LINE__, pThread->threadIndex);
            exit(0);
        }
    }

    for (int t = 0; t < totalThreads; ++t) {
        pthread_join(threadObj[t].threadID, NULL);
    }

    for (int t = 0; t < totalThreads; ++t) {
        taos_close(threadObj[t].taosCon);
    }
    free(threadObj);
}

static int taosDumpIn() {
    assert(g_args.isDumpIn);

    TAOS     *taos    = NULL;
    FILE     *fp      = NULL;

    taos = taos_connect(
            g_args.host, g_args.user, g_args.password,
            NULL, g_args.port);
    if (taos == NULL) {
        errorPrint("%s() LN%d, failed to connect to TDengine server\n",
                __func__, __LINE__);
        return -1;
    }

    taosGetDirectoryFileList(g_args.inpath);

    int32_t  tsSqlFileNumOfTbls = g_tsSqlFileNum;
    if (g_tsDbSqlFile[0] != 0) {
        tsSqlFileNumOfTbls--;

        fp = taosOpenDumpInFile(g_tsDbSqlFile);
        if (NULL == fp) {
            errorPrint("%s() LN%d, failed to open input file %s\n",
                    __func__, __LINE__, g_tsDbSqlFile);
            return -1;
        }
        fprintf(stderr, "Success Open input file: %s\n", g_tsDbSqlFile);

        taosLoadFileCharset(fp, g_tsCharset);

        taosDumpInOneFile(taos, fp, g_tsCharset, g_args.encode,
                g_tsDbSqlFile);
    }

    taos_close(taos);

    if (0 != tsSqlFileNumOfTbls) {
        taosStartDumpInWorkThreads();
    }

    taosFreeDumpFiles();
    return 0;
}

