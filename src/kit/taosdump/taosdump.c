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

#include <stdio.h>
#include <pthread.h>
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


static char    **g_tsDumpInSqlFiles   = NULL;
static char      g_tsCharset[63] = {0};

#ifdef AVRO_SUPPORT
#include <avro.h>
#include <jansson.h>

static char    **g_tsDumpInAvroFiles   = NULL;

static void print_json_aux(json_t *element, int indent);

#endif /* AVRO_SUPPORT */

#define TSDB_SUPPORT_NANOSECOND 1

#define MAX_FILE_NAME_LEN       256             // max file name length on linux is 255
#define MAX_PATH_LEN            4096            // max path length on linux is 4095
#define COMMAND_SIZE            65536
#define MAX_RECORDS_PER_REQ     32766
//#define DEFAULT_DUMP_FILE "taosdump.sql"

// for strncpy buffer overflow
#define min(a, b) (((a) < (b)) ? (a) : (b))

static int  converStringToReadable(char *str, int size, char *buf, int bufsize);
static int  convertNCharToReadable(char *str, int size, char *buf, int bufsize);

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
        fprintf(stderr, "PERF: "fmt, __VA_ARGS__); } while(0)

#define warnPrint(fmt, ...) \
    do { fprintf(stderr, "\033[33m"); \
        fprintf(stderr, "WARN: "fmt, __VA_ARGS__); \
        fprintf(stderr, "\033[0m"); } while(0)

#define errorPrint(fmt, ...) \
    do { fprintf(stderr, "\033[31m"); \
        fprintf(stderr, "ERROR: "fmt, __VA_ARGS__); \
        fprintf(stderr, "\033[0m"); } while(0)

#define okPrint(fmt, ...) \
    do { fprintf(stderr, "\033[32m"); \
        fprintf(stderr, "OK: "fmt, __VA_ARGS__); \
        fprintf(stderr, "\033[0m"); } while(0)

static bool isStringNumber(char *input)
{
    int len = strlen(input);
    if (0 == len) {
        return false;
    }

    for (int i = 0; i < len; i++) {
        if (!isdigit(input[i]))
            return false;
    }

    return true;
}

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

// ---------------------------------- DESCRIBE STABLE CONFIGURE ------------------------------
enum _describe_table_index {
    TSDB_DESCRIBE_METRIC_FIELD_INDEX,
    TSDB_DESCRIBE_METRIC_TYPE_INDEX,
    TSDB_DESCRIBE_METRIC_LENGTH_INDEX,
    TSDB_DESCRIBE_METRIC_NOTE_INDEX,
    TSDB_MAX_DESCRIBE_METRIC
};

#define COL_NOTE_LEN        4
#define COL_TYPEBUF_LEN     16
#define COL_VALUEBUF_LEN    32

typedef struct {
    char field[TSDB_COL_NAME_LEN];
    char type[COL_TYPEBUF_LEN];
    int length;
    char note[COL_NOTE_LEN];
    char value[COL_VALUEBUF_LEN];
    char *var_value;
} ColDes;

typedef struct {
    char name[TSDB_TABLE_NAME_LEN];
    ColDes cols[];
} TableDef;

extern char version[];

#define DB_PRECISION_LEN   8
#define DB_STATUS_LEN      16

typedef struct {
    char name[TSDB_TABLE_NAME_LEN];
    bool belongStb;
    char stable[TSDB_TABLE_NAME_LEN];
} TableInfo;

typedef struct {
    char name[TSDB_TABLE_NAME_LEN];
    char stable[TSDB_TABLE_NAME_LEN];
} TableRecord;

typedef struct {
    bool isStb;
    bool belongStb;
    int64_t dumpNtbCount;
    TableRecord **dumpNtbInfos;
    TableRecord tableRecord;
} TableRecordInfo;

typedef struct {
    char     name[TSDB_DB_NAME_LEN];
    char     create_time[32];
    int64_t  ntables;
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
    int64_t  dumpTbCount;
    TableRecordInfo **dumpTbInfos;
} SDbInfo;

typedef struct {
    pthread_t threadID;
    int32_t   threadIndex;
    char      dbName[TSDB_DB_NAME_LEN];
    char      stbName[TSDB_TABLE_NAME_LEN];
    int       precision;
    TAOS      *taos;
    int64_t   rowsOfDumpOut;
    int64_t   count;
    int64_t   from;
} threadInfo;

typedef struct {
    int64_t   totalRowsOfDumpOut;
    int64_t   totalChildTblsOfDumpOut;
    int32_t   totalSuperTblsOfDumpOut;
    int32_t   totalDatabasesOfDumpOut;
} resultStatistics;

#ifdef AVRO_SUPPORT

enum enAvro_Codec {
    AVRO_CODEC_START = 0,
    AVRO_CODEC_NULL = AVRO_CODEC_START,
    AVRO_CODEC_DEFLATE,
    AVRO_CODEC_SNAPPY,
    AVRO_CODEC_LZMA,
    AVRO_CODEC_UNKNOWN = 255
};

char *g_avro_codec[] = {
    "null",
    "deflate",
    "snappy",
    "lzma",
    "unknown"
};

/* avro sectin begin */
#define RECORD_NAME_LEN     64
#define FIELD_NAME_LEN      64
#define TYPE_NAME_LEN       16

typedef struct FieldStruct_S {
    char name[FIELD_NAME_LEN];
    char type[TYPE_NAME_LEN];
} FieldStruct;

typedef struct RecordSchema_S {
    char name[RECORD_NAME_LEN];
    char *fields;
    int  num_fields;
} RecordSchema;

/* avro section end */
#endif

static int64_t g_totalDumpOutRows = 0;

SDbInfo **g_dbInfos = NULL;
TableInfo *g_tablesList = NULL;

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
static char args_doc[] = "dbname [tbname ...]\n--databases db1,db2,... \n--all-databases\n-i inpath\n-o outpath";

/* Keys for options without short-options. */
#define OPT_ABORT 1 /* –abort */

/* The options we understand. */
static struct argp_option options[] = {
    // connection option
    {"host", 'h', "HOST",    0,  "Server host dumping data from. Default is localhost.", 0},
    {"user", 'u', "USER",    0,  "User name used to connect to server. Default is root.", 0},
#ifdef _TD_POWER_
    {"password", 'p', 0,    0,  "User password to connect to server. Default is powerdb.", 0},
#else
    {"password", 'p', 0,    0,  "User password to connect to server. Default is taosdata.", 0},
#endif
    {"port", 'P', "PORT",        0,  "Port to connect", 0},
    {"mysqlFlag",     'q', "MYSQLFLAG",   0,  "mysqlFlag, Default is 0", 0},
    // input/output file
    {"outpath", 'o', "OUTPATH",     0,  "Output file path.", 1},
    {"inpath", 'i', "INPATH",      0,  "Input file path.", 1},
    {"resultFile", 'r', "RESULTFILE",  0,  "DumpOut/In Result file path and name.", 1},
#ifdef _TD_POWER_
    {"config-dir", 'c', "CONFIG_DIR",  0,  "Configure directory. Default is /etc/power/taos.cfg.", 1},
#else
    {"config-dir", 'c', "CONFIG_DIR",  0,  "Configure directory. Default is /etc/taos/taos.cfg.", 1},
#endif
    {"encode", 'e', "ENCODE", 0,  "Input file encoding.", 1},
    // dump unit options
    {"all-databases", 'A', 0, 0,  "Dump all databases.", 2},
    {"databases", 'D', "DATABASES", 0,  "Dump inputed databases. Use comma to seprate databases\' name.", 2},
    {"allow-sys",   'a', 0, 0,  "Allow to dump sys database", 2},
    // dump format options
    {"schemaonly", 's', 0, 0,  "Only dump schema.", 2},
    {"without-property", 'N', 0, 0,  "Dump schema without properties.", 2},
#ifdef AVRO_SUPPORT
    {"avro", 'v', 0, 0,  "Dump apache avro format data file. By default, dump sql command sequence.", 3},
    {"avro-codec", 'd', "snappy", 0,  "Choose an avro codec among null, deflate, snappy, and lzma.", 4},
#endif
    {"start-time",    'S', "START_TIME",  0,  "Start time to dump. Either epoch or ISO8601/RFC3339 format is acceptable. ISO8601 format example: 2017-10-01T00:00:00.000+0800 or 2017-10-0100:00:00:000+0800 or '2017-10-01 00:00:00.000+0800'",  8},
    {"end-time",      'E', "END_TIME",    0,  "End time to dump. Either epoch or ISO8601/RFC3339 format is acceptable. ISO8601 format example: 2017-10-01T00:00:00.000+0800 or 2017-10-0100:00:00.000+0800 or '2017-10-01 00:00:00.000+0800'",  9},
    {"data-batch",  'B', "DATA_BATCH",  0,  "Number of data point per insert statement. Max value is 32766. Default is 1.", 10},
    {"max-sql-len", 'L', "SQL_LEN",     0,  "Max length of one sql. Default is 65480.", 10},
    {"table-batch", 't', "TABLE_BATCH", 0,  "Number of table dumpout into one output file. Default is 1.", 10},
    {"thread_num",  'T', "THREAD_NUM",  0,  "Number of thread for dump in file. Default is 5.", 10},
    {"debug",   'g', 0, 0,  "Print debug info.", 15},
    {0}
};

#define HUMAN_TIME_LEN      28

/* Used by main to communicate with parse_opt. */
typedef struct arguments {
    // connection option
    char    *host;
    char    *user;
    char    password[SHELL_MAX_PASSWORD_LEN];
    uint16_t port;
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
    char    *databasesSeq;
    // dump format option
    bool     schemaonly;
    bool     with_property;
#ifdef AVRO_SUPPORT
    bool     avro;
    int      avro_codec;
#endif
    int64_t  start_time;
    char     humanStartTime[HUMAN_TIME_LEN];
    int64_t  end_time;
    char     humanEndTime[HUMAN_TIME_LEN];
    char     precision[8];

    int32_t  data_batch;
    int32_t  max_sql_len;
    int32_t  table_batch; // num of table which will be dump into one output file.
    bool     allow_sys;
    // other options
    int32_t  thread_num;
    int      abort;
    char   **arg_list;
    int      arg_list_len;
    bool     isDumpIn;
    bool     debug_print;
    bool     verbose_print;
    bool     performance_print;

    int      dumpDbCount;
} SArguments;

/* Our argp parser. */
static error_t parse_opt(int key, char *arg, struct argp_state *state);

static struct argp argp = {options, parse_opt, args_doc, doc};
static resultStatistics g_resultStatistics = {0};
static FILE *g_fpOfResult = NULL;
static int g_numOfCores = 1;

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
    0,
    // outpath and inpath
    "",
    "",
    "./dump_result.txt",
    NULL,
    // dump unit option
    false,      // all_databases
    false,      // databases
    NULL,       // databasesSeq
    // dump format option
    false,      // schemaonly
    true,       // with_property
#ifdef AVRO_SUPPORT
    false,      // avro
    AVRO_CODEC_SNAPPY,  // avro_codec
#endif
    -INT64_MAX + 1, // start_time
    {0},        // humanStartTime
    INT64_MAX,  // end_time
    {0},        // humanEndTime
    "ms",       // precision
    1,          // data_batch
    TSDB_MAX_SQL_LEN,   // max_sql_len
    1,          // table_batch
    false,      // allow_sys
    // other options
    8,          // thread_num
    0,          // abort
    NULL,       // arg_list
    0,          // arg_list_len
    false,      // isDumpIn
    false,      // debug_print
    false,      // verbose_print
    false,      // performance_print
        0,      // dumpDbCount
};

// get taosdump commit number version
#ifndef TAOSDUMP_COMMIT_SHA1
#define TAOSDUMP_COMMIT_SHA1 "unknown"
#endif

#ifndef TD_VERNUMBER
#define TD_VERNUMBER "unknown"
#endif

#ifndef TAOSDUMP_STATUS
#define TAOSDUMP_STATUS "unknown"
#endif

static void printVersion() {
    char tdengine_ver[] = TD_VERNUMBER;
    char taosdump_ver[] = TAOSDUMP_COMMIT_SHA1;
    char taosdump_status[] = TAOSDUMP_STATUS;

    if (strlen(taosdump_status) == 0) {
        printf("taosdump version %s-%s\n",
                tdengine_ver, taosdump_ver);
    } else {
        printf("taosdump version %s-%s, status:%s\n",
                tdengine_ver, taosdump_ver, taosdump_status);
    }
}

void errorWrongValue(char *program, char *wrong_arg, char *wrong_value)
{
    fprintf(stderr, "%s %s: %s is an invalid value\n", program, wrong_arg, wrong_value);
    fprintf(stderr, "Try `taosdump --help' or `taosdump --usage' for more information.\n");
}

static void errorUnrecognized(char *program, char *wrong_arg)
{
    fprintf(stderr, "%s: unrecognized options '%s'\n", program, wrong_arg);
    fprintf(stderr, "Try `taosdump --help' or `taosdump --usage' for more information.\n");
}

static void errorPrintReqArg(char *program, char *wrong_arg)
{
    fprintf(stderr,
            "%s: option requires an argument -- '%s'\n",
            program, wrong_arg);
    fprintf(stderr,
            "Try `taosdump --help' or `taosdump --usage' for more information.\n");
}

static void errorPrintReqArg2(char *program, char *wrong_arg)
{
    fprintf(stderr,
            "%s: option requires a number argument '-%s'\n",
            program, wrong_arg);
    fprintf(stderr,
            "Try `taosdump --help' or `taosdump --usage' for more information.\n");
}

static void errorPrintReqArg3(char *program, char *wrong_arg)
{
    fprintf(stderr,
            "%s: option '%s' requires an argument\n",
            program, wrong_arg);
    fprintf(stderr,
            "Try `taosdump --help' or `taosdump --usage' for more information.\n");
}

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
            break;
        case 'P':
            if (!isStringNumber(arg)) {
                errorPrintReqArg2("taosdump", "P");
                exit(EXIT_FAILURE);
            }

            uint64_t port = atoi(arg);
            if (port > 65535) {
                errorWrongValue("taosdump", "-P or --port", arg);
                exit(EXIT_FAILURE);
            }
            g_args.port = (uint16_t)port;

            break;
        case 'q':
            g_args.mysqlFlag = atoi(arg);
            break;
        case 'o':
            if (wordexp(arg, &full_path, 0) != 0) {
                errorPrint("Invalid path %s\n", arg);
                return -1;
            }

            if (full_path.we_wordv[0]) {
                tstrncpy(g_args.outpath, full_path.we_wordv[0],
                        MAX_FILE_NAME_LEN);
                wordfree(&full_path);
            } else {
                errorPrintReqArg3("taosdump", "-o or --outpath");
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
                tstrncpy(g_args.inpath, full_path.we_wordv[0],
                        MAX_FILE_NAME_LEN);
                wordfree(&full_path);
            } else {
                errorPrintReqArg3("taosdump", "-i or --inpath");
                exit(EXIT_FAILURE);
            }
            break;

#ifdef AVRO_SUPPORT
        case 'v':
            g_args.avro = true;
            break;

        case 'd':
            for (int i = AVRO_CODEC_START; i < AVRO_CODEC_UNKNOWN; i ++) {
                if (0 == strcmp(arg, g_avro_codec[i])) {
                    g_args.avro_codec = i;
                    break;
                }
            }
            break;
#endif

        case 'r':
            g_args.resultFile = arg;
            break;
        case 'c':
            if (0 == strlen(arg)) {
                errorPrintReqArg3("taosdump", "-c or --config-dir");
                exit(EXIT_FAILURE);
            }
            if (wordexp(arg, &full_path, 0) != 0) {
                errorPrint("Invalid path %s\n", arg);
                exit(EXIT_FAILURE);
            }
            tstrncpy(configDir, full_path.we_wordv[0], MAX_FILE_NAME_LEN);
            wordfree(&full_path);
            break;
        case 'e':
            g_args.encode = arg;
            break;
            // dump unit option
        case 'A':
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
        case 'S':
            // parse time here.
            break;
        case 'E':
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
            if (!isStringNumber(arg)) {
                errorPrint("%s", "\n\t-T need a number following!\n");
                exit(EXIT_FAILURE);
            }
            g_args.thread_num = atoi(arg);
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

        default:
            return ARGP_ERR_UNKNOWN;
    }
    return 0;
}

static void freeTbDes(TableDef *tableDes)
{
    for (int i = 0; i < TSDB_MAX_COLUMNS; i ++) {
        if (tableDes->cols[i].var_value) {
            free(tableDes->cols[i].var_value);
        }
    }

    free(tableDes);
}

static int queryDbImpl(TAOS *taos, char *command) {
    TAOS_RES *res = NULL;
    int32_t   code = -1;

    res = taos_query(taos, command);
    code = taos_errno(res);

    if (code != 0) {
        errorPrint("Failed to run <%s>, reason: %s\n",
                command, taos_errstr(res));
        taos_free_result(res);
        //taos_close(taos);
        return code;
    }

    taos_free_result(res);
    return 0;
}

static void parse_args(
        int argc, char *argv[], SArguments *arguments) {

    for (int i = 1; i < argc; i++) {
        if ((strncmp(argv[i], "-p", 2) == 0)
              || (strncmp(argv[i], "--password", 10) == 0)) {
            if ((strlen(argv[i]) == 2)
                  || (strncmp(argv[i], "--password", 10) == 0)) {
                printf("Enter password: ");
                taosSetConsoleEcho(false);
                if(scanf("%20s", arguments->password) > 1) {
                    errorPrint("%s() LN%d, password read error!\n", __func__, __LINE__);
                }
                taosSetConsoleEcho(true);
            } else {
                tstrncpy(arguments->password, (char *)(argv[i] + 2),
                        SHELL_MAX_PASSWORD_LEN);
                strcpy(argv[i], "-p");
            }
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
                        argv[i], "--database",
                        strlen("--database")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg(argv[0], "D");
                    exit(EXIT_FAILURE);
                }
                arguments->databasesSeq = argv[++i];
            } else if (0 == strncmp(argv[i], "--databases=", strlen("--databases="))) {
                arguments->databasesSeq = (char *)(argv[i] + strlen("--databases="));
            } else if (0 == strncmp(argv[i], "-D", strlen("-D"))) {
                arguments->databasesSeq = (char *)(argv[i] + strlen("-D"));
            } else if (strlen("--databases") == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg3(argv[0], "--databases");
                    exit(EXIT_FAILURE);
                }
                arguments->databasesSeq = argv[++i];
            } else {
                errorUnrecognized(argv[0], argv[i]);
                exit(EXIT_FAILURE);
            }
            g_args.databases = true;
        } else if (0 == strncmp(argv[i], "--version", strlen("--version")) ||
            0 == strncmp(argv[i], "-V", strlen("-V"))) {
                printVersion();
                exit(EXIT_SUCCESS);
        } else {
            continue;
        }

    }
}

static void copyHumanTimeToArg(char *timeStr, bool isStartTime)
{
    if (isStartTime)
        tstrncpy(g_args.humanStartTime, timeStr, HUMAN_TIME_LEN);
    else
        tstrncpy(g_args.humanEndTime, timeStr, HUMAN_TIME_LEN);
}

static void copyTimestampToArg(char *timeStr, bool isStartTime)
{
    if (isStartTime)
        g_args.start_time = atol(timeStr);
    else
        g_args.end_time = atol(timeStr);
}

static void parse_timestamp(
        int argc, char *argv[], SArguments *arguments) {
    for (int i = 1; i < argc; i++) {
        char *tmp;
        bool isStartTime = false;
        bool isEndTime = false;

        if (strcmp(argv[i], "-S") == 0) {
            isStartTime = true;
        } else if (strcmp(argv[i], "-E") == 0) {
            isEndTime = true;
        }

        if (isStartTime || isEndTime) {
            if (NULL == argv[i+1]) {
                errorPrint("%s need a valid value following!\n", argv[i]);
                exit(-1);
            }
            tmp = strdup(argv[i+1]);

            if (strchr(tmp, ':') && strchr(tmp, '-')) {
                copyHumanTimeToArg(tmp, isStartTime);
            } else {
                copyTimestampToArg(tmp, isStartTime);
            }

            free(tmp);
        }
    }
}

static int getPrecisionByString(char *precision)
{
    if (0 == strncasecmp(precision,
                "ms", 2)) {
        return TSDB_TIME_PRECISION_MILLI;
    } else if (0 == strncasecmp(precision,
                "us", 2)) {
        return TSDB_TIME_PRECISION_MICRO;
#if TSDB_SUPPORT_NANOSECOND == 1
    } else if (0 == strncasecmp(precision,
                "ns", 2)) {
        return TSDB_TIME_PRECISION_NANO;
#endif
    } else {
        errorPrint("Invalid time precision: %s",
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

// check table is normal table or super table
static int getTableRecordInfo(
        char *dbName,
        char *table, TableRecordInfo *pTableRecordInfo) {
    TAOS *taos = taos_connect(g_args.host, g_args.user, g_args.password,
            dbName, g_args.port);
    if (taos == NULL) {
        errorPrint("Failed to connect to TDengine server %s\n", g_args.host);
        return -1;
    }

    TAOS_ROW row = NULL;
    bool isSet = false;
    TAOS_RES *result     = NULL;

    memset(pTableRecordInfo, 0, sizeof(TableRecordInfo));

    char command[COMMAND_SIZE];

    sprintf(command, "USE %s", dbName);
    result = taos_query(taos, command);
    int32_t code = taos_errno(result);
    if (code != 0) {
        errorPrint("invalid database %s, reason: %s\n",
                dbName, taos_errstr(result));
        return 0;
    }

    sprintf(command, "SHOW TABLES LIKE \'%s\'", table);

    result = taos_query(taos, command);
    code = taos_errno(result);

    if (code != 0) {
        errorPrint("%s() LN%d, failed to run command <%s>. reason: %s\n",
                __func__, __LINE__, command, taos_errstr(result));
        taos_free_result(result);
        return -1;
    }

    TAOS_FIELD *fields = taos_fetch_fields(result);

    while ((row = taos_fetch_row(result)) != NULL) {
        isSet = true;
        pTableRecordInfo->isStb = false;
        tstrncpy(pTableRecordInfo->tableRecord.name,
                (char *)row[TSDB_SHOW_TABLES_NAME_INDEX],
                min(TSDB_TABLE_NAME_LEN,
                    fields[TSDB_SHOW_TABLES_NAME_INDEX].bytes + 1));
        if (strlen((char *)row[TSDB_SHOW_TABLES_METRIC_INDEX]) > 0) {
            pTableRecordInfo->belongStb = true;
            tstrncpy(pTableRecordInfo->tableRecord.stable,
                    (char *)row[TSDB_SHOW_TABLES_METRIC_INDEX],
                    min(TSDB_TABLE_NAME_LEN,
                        fields[TSDB_SHOW_TABLES_METRIC_INDEX].bytes + 1));
        } else {
            pTableRecordInfo->belongStb = false;
        }
        break;
    }

    taos_free_result(result);
    result = NULL;

    if (isSet) {
        return 0;
    }

    sprintf(command, "SHOW STABLES LIKE \'%s\'", table);

    result = taos_query(taos, command);
    code = taos_errno(result);

    if (code != 0) {
        errorPrint("%s() LN%d, failed to run command <%s>. reason: %s\n",
                __func__, __LINE__, command, taos_errstr(result));
        taos_free_result(result);
        return -1;
    }

    while ((row = taos_fetch_row(result)) != NULL) {
        isSet = true;
        pTableRecordInfo->isStb = true;
        tstrncpy(pTableRecordInfo->tableRecord.stable, table,
                TSDB_TABLE_NAME_LEN);
        break;
    }

    taos_free_result(result);
    result = NULL;

    if (isSet) {
        return 0;
    }
    errorPrint("%s() LN%d, invalid table/stable %s\n",
            __func__, __LINE__, table);
    return -1;
}

static int inDatabasesSeq(
        char *name,
        int len)
{
    if (strstr(g_args.databasesSeq, ",") == NULL) {
        if (0 == strncmp(g_args.databasesSeq, name, len)) {
            return 0;
        }
    } else {
        char *dupSeq = strdup(g_args.databasesSeq);
        char *running = dupSeq;
        char *dbname = strsep(&running, ",");
        while (dbname) {
            if (0 == strncmp(dbname, name, len)) {
                tfree(dupSeq);
                return 0;
            }

            dbname = strsep(&running, ",");
        }
    }

    return -1;
}

static int getDumpDbCount()
{
    int count = 0;

    TAOS     *taos = NULL;
    TAOS_RES *result     = NULL;
    char     *command    = "show databases";
    TAOS_ROW row;

    /* Connect to server */
    taos = taos_connect(g_args.host, g_args.user, g_args.password,
            NULL, g_args.port);
    if (NULL == taos) {
        errorPrint("Failed to connect to TDengine server %s\n", g_args.host);
        return 0;
    }

    result = taos_query(taos, command);
    int32_t code = taos_errno(result);

    if (0 != code) {
        errorPrint("%s() LN%d, failed to run command <%s>, reason: %s\n",
                __func__, __LINE__, command, taos_errstr(result));
        taos_close(taos);
        return 0;
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
            if (inDatabasesSeq(
                        (char *)row[TSDB_SHOW_DB_NAME_INDEX],
                        fields[TSDB_SHOW_DB_NAME_INDEX].bytes) != 0)
                continue;
        } else if (!g_args.all_databases) {  // only input one db
            if (strncasecmp(g_args.arg_list[0],
                        (char *)row[TSDB_SHOW_DB_NAME_INDEX],
                        fields[TSDB_SHOW_DB_NAME_INDEX].bytes) != 0)
                continue;
        }

        count++;
    }

    if (count == 0) {
        errorPrint("%d databases valid to dump\n", count);
    }

    taos_close(taos);
    return count;
}

static void dumpCreateMTableClause(
        char* dbName,
        char *stable,
        TableDef *tableDes,
        int numOfCols,
        FILE *fp
        ) {
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
            dbName, tableDes->name, dbName, stable);

    for (; counter < numOfCols; counter++) {
        if (tableDes->cols[counter].note[0] != '\0') break;
    }

    assert(counter < numOfCols);
    count_temp = counter;

    for (; counter < numOfCols; counter++) {
        if (counter != count_temp) {
            if (0 == strcasecmp(tableDes->cols[counter].type, "binary")
                    || 0 == strcasecmp(tableDes->cols[counter].type, "nchar")) {
                //pstr += sprintf(pstr, ", \'%s\'", tableDes->cols[counter].note);
                if (tableDes->cols[counter].var_value) {
                    pstr += sprintf(pstr, ", \'%s\'",
                            tableDes->cols[counter].var_value);
                } else {
                    pstr += sprintf(pstr, ", \'%s\'", tableDes->cols[counter].value);
                }
            } else {
                pstr += sprintf(pstr, ", \'%s\'", tableDes->cols[counter].value);
            }
        } else {
            if (0 == strcasecmp(tableDes->cols[counter].type, "binary")
                    || 0 == strcasecmp(tableDes->cols[counter].type, "nchar")) {
                //pstr += sprintf(pstr, "\'%s\'", tableDes->cols[counter].note);
                if (tableDes->cols[counter].var_value) {
                    pstr += sprintf(pstr, "\'%s\'", tableDes->cols[counter].var_value);
                } else {
                    pstr += sprintf(pstr, "\'%s\'", tableDes->cols[counter].value);
                }
            } else {
                pstr += sprintf(pstr, "\'%s\'", tableDes->cols[counter].value);
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

static int64_t getNtbCountOfStb(char *dbName, char *stbName)
{
    TAOS *taos = taos_connect(g_args.host, g_args.user, g_args.password,
            dbName, g_args.port);
    if (taos == NULL) {
        errorPrint("Failed to connect to TDengine server %s\n", g_args.host);
        return -1;
    }

    int64_t count = 0;

    char command[COMMAND_SIZE];

    sprintf(command, "SELECT COUNT(TBNAME) FROM %s.%s", dbName, stbName);

    TAOS_RES *res = taos_query(taos, command);
    int32_t code = taos_errno(res);
    if (code != 0) {
        errorPrint("%s() LN%d, failed to run command <%s>. reason: %s\n",
                __func__, __LINE__, command, taos_errstr(res));
        taos_free_result(res);
        taos_close(taos);
        return -1;
    }

    TAOS_ROW row = NULL;

    if ((row = taos_fetch_row(res)) != NULL) {
        count = *(int64_t*)row[TSDB_SHOW_TABLES_NAME_INDEX];
    }

    taos_close(taos);
    return count;
}

static int getTableDes(
        TAOS *taos,
        char* dbName, char *table,
        TableDef *tableDes, bool isSuperTable) {
    TAOS_ROW row = NULL;
    TAOS_RES* res = NULL;
    int colCount = 0;

    char sqlstr[COMMAND_SIZE];
    sprintf(sqlstr, "describe %s.%s;", dbName, table);

    res = taos_query(taos, sqlstr);
    int32_t code = taos_errno(res);
    if (code != 0) {
        errorPrint("%s() LN%d, failed to run command <%s>, reason: %s\n",
                __func__, __LINE__, sqlstr, taos_errstr(res));
        taos_free_result(res);
        return -1;
    }

    TAOS_FIELD *fields = taos_fetch_fields(res);

    tstrncpy(tableDes->name, table, TSDB_TABLE_NAME_LEN);
    while ((row = taos_fetch_row(res)) != NULL) {
        tstrncpy(tableDes->cols[colCount].field,
                (char *)row[TSDB_DESCRIBE_METRIC_FIELD_INDEX],
                min(TSDB_COL_NAME_LEN,
                    fields[TSDB_DESCRIBE_METRIC_FIELD_INDEX].bytes + 1));
        tstrncpy(tableDes->cols[colCount].type,
                (char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                min(16, fields[TSDB_DESCRIBE_METRIC_TYPE_INDEX].bytes + 1));
        tableDes->cols[colCount].length =
            *((int *)row[TSDB_DESCRIBE_METRIC_LENGTH_INDEX]);
        tstrncpy(tableDes->cols[colCount].note,
                (char *)row[TSDB_DESCRIBE_METRIC_NOTE_INDEX],
                min(COL_NOTE_LEN,
                    fields[TSDB_DESCRIBE_METRIC_NOTE_INDEX].bytes + 1));
        colCount++;
    }

    taos_free_result(res);
    res = NULL;

    if (isSuperTable) {
        return colCount;
    }

    // if child-table have tag, using  select tagName from table to get tagValue
    for (int i = 0 ; i < colCount; i++) {
        if (strcmp(tableDes->cols[i].note, "TAG") != 0) continue;

        sprintf(sqlstr, "select %s from %s.%s",
                tableDes->cols[i].field, dbName, table);

        res = taos_query(taos, sqlstr);
        code = taos_errno(res);
        if (code != 0) {
            errorPrint("%s() LN%d, failed to run command <%s>, reason: %s\n",
                    __func__, __LINE__, sqlstr, taos_errstr(res));
            taos_free_result(res);
            taos_close(taos);
            return -1;
        }

        fields = taos_fetch_fields(res);

        row = taos_fetch_row(res);
        if (NULL == row) {
            errorPrint("%s() LN%d, fetch failed to run command <%s>, reason:%s\n",
                    __func__, __LINE__, sqlstr, taos_errstr(res));
            taos_free_result(res);
            taos_close(taos);
            return -1;
        }

        if (row[TSDB_SHOW_TABLES_NAME_INDEX] == NULL) {
            sprintf(tableDes->cols[i].note, "%s", "NUL");
            sprintf(tableDes->cols[i].value, "%s", "NULL");
            taos_free_result(res);
            res = NULL;
            continue;
        }

        int32_t* length = taos_fetch_lengths(res);

        switch (fields[0].type) {
            case TSDB_DATA_TYPE_BOOL:
                sprintf(tableDes->cols[i].value, "%d",
                        ((((int32_t)(*((char *)
                                       row[TSDB_SHOW_TABLES_NAME_INDEX])))==1)
                         ?1:0));
                break;
            case TSDB_DATA_TYPE_TINYINT:
                sprintf(tableDes->cols[i].value, "%d",
                        *((int8_t *)row[TSDB_SHOW_TABLES_NAME_INDEX]));
                break;
            case TSDB_DATA_TYPE_SMALLINT:
                sprintf(tableDes->cols[i].value, "%d",
                        *((int16_t *)row[TSDB_SHOW_TABLES_NAME_INDEX]));
                break;
            case TSDB_DATA_TYPE_INT:
                sprintf(tableDes->cols[i].value, "%d",
                        *((int32_t *)row[TSDB_SHOW_TABLES_NAME_INDEX]));
                break;
            case TSDB_DATA_TYPE_BIGINT:
                sprintf(tableDes->cols[i].value, "%" PRId64 "",
                        *((int64_t *)row[TSDB_SHOW_TABLES_NAME_INDEX]));
                break;
            case TSDB_DATA_TYPE_FLOAT:
                sprintf(tableDes->cols[i].value, "%f",
                        GET_FLOAT_VAL(row[TSDB_SHOW_TABLES_NAME_INDEX]));
                break;
            case TSDB_DATA_TYPE_DOUBLE:
                sprintf(tableDes->cols[i].value, "%f",
                        GET_DOUBLE_VAL(row[TSDB_SHOW_TABLES_NAME_INDEX]));
                break;
            case TSDB_DATA_TYPE_BINARY:
                memset(tableDes->cols[i].value, 0,
                        sizeof(tableDes->cols[i].value));
                int len = strlen((char *)row[TSDB_SHOW_TABLES_NAME_INDEX]);
                // FIXME for long value
                if (len < (COL_VALUEBUF_LEN - 2)) {
                    converStringToReadable(
                            (char *)row[TSDB_SHOW_TABLES_NAME_INDEX],
                            length[0],
                            tableDes->cols[i].value,
                            len);
                } else {
                    tableDes->cols[i].var_value = calloc(1, len * 2);
                    if (tableDes->cols[i].var_value == NULL) {
                        errorPrint("%s() LN%d, memory alalocation failed!\n",
                                __func__, __LINE__);
                        taos_free_result(res);
                        return -1;
                    }
                    converStringToReadable((char *)row[0],
                            length[0],
                            (char *)(tableDes->cols[i].var_value), len);
                }
                break;

            case TSDB_DATA_TYPE_NCHAR:
                memset(tableDes->cols[i].value, 0,
                        sizeof(tableDes->cols[i].note));
                int nlen = strlen((char *)row[TSDB_SHOW_TABLES_NAME_INDEX]);
                if (nlen < (COL_VALUEBUF_LEN-2)) {
                    char tbuf[COL_VALUEBUF_LEN-2];    // need reserve 2 bytes for ' '
                    convertNCharToReadable(
                            (char *)row[TSDB_SHOW_TABLES_NAME_INDEX],
                            length[0], tbuf, COL_VALUEBUF_LEN-2);
                    sprintf(tableDes->cols[i].value, "%s", tbuf);
                } else {
                    tableDes->cols[i].var_value = calloc(1, len * 4);
                    if (tableDes->cols[i].var_value == NULL) {
                        errorPrint("%s() LN%d, memory alalocation failed!\n",
                                __func__, __LINE__);
                        taos_free_result(res);
                        return -1;
                    }
                    converStringToReadable(
                            (char *)row[TSDB_SHOW_TABLES_NAME_INDEX],
                            length[0],
                            (char *)(tableDes->cols[i].var_value), len);
                }
                break;
            case TSDB_DATA_TYPE_TIMESTAMP:
                sprintf(tableDes->cols[i].value, "%" PRId64 "",
                        *(int64_t *)row[TSDB_SHOW_TABLES_NAME_INDEX]);
#if 0
                if (!g_args.mysqlFlag) {
                    sprintf(tableDes->cols[i].value, "%" PRId64 "",
                            *(int64_t *)row[TSDB_SHOW_TABLES_NAME_INDEX]);
                } else {
                    char buf[64] = "\0";
                    int64_t ts = *((int64_t *)row[TSDB_SHOW_TABLES_NAME_INDEX]);
                    time_t tt = (time_t)(ts / 1000);
                    struct tm *ptm = localtime(&tt);
                    strftime(buf, 64, "%y-%m-%d %H:%M:%S", ptm);
                    sprintf(tableDes->cols[i].value, "\'%s.%03d\'", buf,
                            (int)(ts % 1000));
                }
#endif
                break;
            default:
                break;
        }

        taos_free_result(res);
    }

    return colCount;
}

static int dumpCreateTableClause(TableDef *tableDes, int numOfCols,
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

        if (0 == strcasecmp(tableDes->cols[counter].type, "binary")
                || 0 == strcasecmp(tableDes->cols[counter].type, "nchar")) {
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

        if (0 == strcasecmp(tableDes->cols[counter].type, "binary")
                || 0 == strcasecmp(tableDes->cols[counter].type, "nchar")) {
            pstr += sprintf(pstr, "(%d)", tableDes->cols[counter].length);
        }
    }

    pstr += sprintf(pstr, ");");

    debugPrint("%s() LN%d, write string: %s\n", __func__, __LINE__, sqlstr);
    return fprintf(fp, "%s\n\n", sqlstr);
}

static int dumpStableClasuse(TAOS *taos, SDbInfo *dbInfo, char *stbName, FILE *fp)
{
    uint64_t sizeOfTableDes =
        (uint64_t)(sizeof(TableDef) + sizeof(ColDes) * TSDB_MAX_COLUMNS);

    TableDef *tableDes = (TableDef *)calloc(1, sizeOfTableDes);
    if (NULL == tableDes) {
        errorPrint("%s() LN%d, failed to allocate %"PRIu64" memory\n",
                __func__, __LINE__, sizeOfTableDes);
        exit(-1);
    }

    int colCount = getTableDes(taos, dbInfo->name,
            stbName, tableDes, true);

    if (colCount < 0) {
        free(tableDes);
        errorPrint("%s() LN%d, failed to get stable[%s] schema\n",
               __func__, __LINE__, stbName);
        exit(-1);
    }

    dumpCreateTableClause(tableDes, colCount, fp, dbInfo->name);
    free(tableDes);

    return 0;
}

static int64_t dumpCreateSTableClauseOfDb(
        SDbInfo *dbInfo, FILE *fp)
{
    TAOS *taos = taos_connect(g_args.host,
            g_args.user, g_args.password, dbInfo->name, g_args.port);
    if (NULL == taos) {
        errorPrint(
                "Failed to connect to TDengine server %s by specified database %s\n",
                g_args.host, dbInfo->name);
        return 0;
    }

    TAOS_ROW row;
    char command[COMMAND_SIZE] = {0};

    sprintf(command, "SHOW %s.STABLES", dbInfo->name);

    TAOS_RES* res = taos_query(taos, command);
    int32_t  code = taos_errno(res);
    if (code != 0) {
        errorPrint("%s() LN%d, failed to run command <%s>, reason: %s\n",
                __func__, __LINE__, command, taos_errstr(res));
        taos_free_result(res);
        taos_close(taos);
        exit(-1);
    }

    int64_t superTblCnt = 0;
    while ((row = taos_fetch_row(res)) != NULL) {
        if (0 == dumpStableClasuse(taos, dbInfo,
                    row[TSDB_SHOW_TABLES_NAME_INDEX], fp)) {
            superTblCnt ++;
        }
    }

    taos_free_result(res);

    fprintf(g_fpOfResult,
            "# super table counter:               %"PRId64"\n",
            superTblCnt);
    g_resultStatistics.totalSuperTblsOfDumpOut += superTblCnt;

    taos_close(taos);

    return superTblCnt;
}

static void dumpCreateDbClause(
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
            errorPrint("%s() LN%d, failed to open file %s\n",
                    __func__, __LINE__, fname);
        }
    }

    wordfree(&full_path);
    return f;
}

static uint64_t getFilesNum(char *ext)
{
    uint64_t count = 0;

    int namelen, extlen;
    struct dirent *pDirent;
    DIR *pDir;

    extlen = strlen(ext);

    bool isSql = (0 == strcmp(ext, "sql"));

    pDir = opendir(g_args.inpath);
    if (pDir != NULL) {
        while ((pDirent = readdir(pDir)) != NULL) {
            namelen = strlen (pDirent->d_name);

            if (namelen > extlen) {
                if (strcmp (ext, &(pDirent->d_name[namelen - extlen])) == 0) {
                    if (isSql) {
                        if (0 == strcmp(pDirent->d_name, "dbs.sql")) {
                            continue;
                        }
                    }
                    verbosePrint("%s found\n", pDirent->d_name);
                    count ++;
                }
            }
        }
        closedir (pDir);
    }

    debugPrint("%"PRId64" .%s files found!\n", count, ext);
    return count;
}

static void freeFileList(char **fileList, int64_t count)
{
    for (int64_t i = 0; i < count; i++) {
        tfree(fileList[i]);
    }
    tfree(fileList);
}

static void createDumpinList(char *ext, int64_t count)
{
    bool isSql = (0 == strcmp(ext, "sql"));

    if (isSql) {
        g_tsDumpInSqlFiles = (char **)calloc(count, sizeof(char *));
        assert(g_tsDumpInSqlFiles);

        for (int64_t i = 0; i < count; i++) {
            g_tsDumpInSqlFiles[i] = calloc(1, MAX_FILE_NAME_LEN);
            assert(g_tsDumpInSqlFiles[i]);
        }
    }
#ifdef AVRO_SUPPORT
    else {
        g_tsDumpInAvroFiles = (char **)calloc(count, sizeof(char *));
        assert(g_tsDumpInAvroFiles);

        for (int64_t i = 0; i < count; i++) {
            g_tsDumpInAvroFiles[i] = calloc(1, MAX_FILE_NAME_LEN);
            assert(g_tsDumpInAvroFiles[i]);
        }

    }
#endif

    int namelen, extlen;
    struct dirent *pDirent;
    DIR *pDir;

    extlen = strlen(ext);

    count = 0;
    pDir = opendir(g_args.inpath);
    if (pDir != NULL) {
        while ((pDirent = readdir(pDir)) != NULL) {
            namelen = strlen (pDirent->d_name);

            if (namelen > extlen) {
                if (strcmp (ext, &(pDirent->d_name[namelen - extlen])) == 0) {
                    verbosePrint("%s found\n", pDirent->d_name);
                    if (isSql) {
                        if (0 == strcmp(pDirent->d_name, "dbs.sql")) {
                            continue;
                        }
                        strncpy(g_tsDumpInSqlFiles[count++], pDirent->d_name, MAX_FILE_NAME_LEN);
                    }
#ifdef AVRO_SUPPORT
                    else {
                        strncpy(g_tsDumpInAvroFiles[count++], pDirent->d_name, MAX_FILE_NAME_LEN);
                    }
#endif
                }
            }
        }
        closedir (pDir);
    }

    debugPrint("%"PRId64" .%s files filled to list!\n", count, ext);
}

#ifdef AVRO_SUPPORT

static int convertTbDesToJson(
        char *dbName, char *tbName, TableDef *tableDes, int colCount,
        char **jsonSchema)
{
    // {
    // "type": "record",
    // "name": "dbname.tbname",
    // "fields": [
    //      {
    //      "name": "col0 name",
    //      "type": "long"
    //      },
    //      {
    //      "name": "col1 name",
    //      "type": "int"
    //      },
    //      {
    //      "name": "col2 name",
    //      "type": "float"
    //      },
    //      {
    //      "name": "col3 name",
    //      "type": "boolean"
    //      },
    //      ...
    //      {
    //      "name": "coln name",
    //      "type": "string"
    //      }
    // ]
    // }
    *jsonSchema = (char *)calloc(1,
            17 + TSDB_DB_NAME_LEN               /* dbname section */
            + 17                                /* type: record */
            + 11 + TSDB_TABLE_NAME_LEN          /* tbname section */
            + 10                                /* fields section */
            + (TSDB_COL_NAME_LEN + 11 + 16) * colCount + 4);    /* fields section */
    if (*jsonSchema == NULL) {
        errorPrint("%s() LN%d, memory allocation failed!\n", __func__, __LINE__);
        return -1;
    }

    char *pstr = *jsonSchema;
    pstr += sprintf(pstr,
            "{\"type\": \"record\", \"name\": \"%s.%s\", \"fields\": [",
            dbName, tbName);
    for (int i = 0; i < colCount; i ++) {
        if (0 == i) {
            pstr += sprintf(pstr,
                    "{\"name\": \"%s\", \"type\": \"%s\"",
                    tableDes->cols[i].field, "long");
        } else {
            if (strcasecmp(tableDes->cols[i].type, "binary") == 0) {
                pstr += sprintf(pstr,
                    "{\"name\": \"%s\", \"type\": \"%s\"",
                    tableDes->cols[i].field, "string");
            } else if (strcasecmp(tableDes->cols[i].type, "nchar") == 0) {
                pstr += sprintf(pstr,
                    "{\"name\": \"%s\", \"type\": \"%s\"",
                    tableDes->cols[i].field, "bytes");
            } else if (strcasecmp(tableDes->cols[i].type, "bool") == 0) {
                pstr += sprintf(pstr,
                    "{\"name\": \"%s\", \"type\": \"%s\"",
                    tableDes->cols[i].field, "boolean");
            } else if (strcasecmp(tableDes->cols[i].type, "tinyint") == 0) {
                pstr += sprintf(pstr,
                    "{\"name\": \"%s\", \"type\": \"%s\"",
                    tableDes->cols[i].field, "int");
            } else if (strcasecmp(tableDes->cols[i].type, "smallint") == 0) {
                pstr += sprintf(pstr,
                    "{\"name\": \"%s\", \"type\": \"%s\"",
                    tableDes->cols[i].field, "int");
            } else if (strcasecmp(tableDes->cols[i].type, "bigint") == 0) {
                pstr += sprintf(pstr,
                    "{\"name\": \"%s\", \"type\": \"%s\"",
                    tableDes->cols[i].field, "long");
            } else if (strcasecmp(tableDes->cols[i].type, "timestamp") == 0) {
                pstr += sprintf(pstr,
                    "{\"name\": \"%s\", \"type\": \"%s\"",
                    tableDes->cols[i].field, "long");
            } else {
                pstr += sprintf(pstr,
                    "{\"name\": \"%s\", \"type\": \"%s\"",
                    tableDes->cols[i].field,
                    strtolower(tableDes->cols[i].type, tableDes->cols[i].type));
            }
        }
        if ((i != (colCount -1))
                && (strcmp(tableDes->cols[i + 1].note, "TAG") != 0)) {
            pstr += sprintf(pstr, "},");
        } else {
            pstr += sprintf(pstr, "}");
            break;
        }
    }

    pstr += sprintf(pstr, "]}");

    debugPrint("%s() LN%d, jsonSchema:\n %s\n", __func__, __LINE__, *jsonSchema);

    return 0;
}

static void print_json_indent(int indent) {
    int i;
    for (i = 0; i < indent; i++) {
        putchar(' ');
    }
}

const char *json_plural(size_t count) { return count == 1 ? "" : "s"; }

static void print_json_object(json_t *element, int indent) {
    size_t size;
    const char *key;
    json_t *value;

    print_json_indent(indent);
    size = json_object_size(element);

    printf("JSON Object of %lld pair%s:\n", (long long)size, json_plural(size));
    json_object_foreach(element, key, value) {
        print_json_indent(indent + 2);
        printf("JSON Key: \"%s\"\n", key);
        print_json_aux(value, indent + 2);
    }
}

static void print_json_array(json_t *element, int indent) {
    size_t i;
    size_t size = json_array_size(element);
    print_json_indent(indent);

    printf("JSON Array of %lld element%s:\n", (long long)size, json_plural(size));
    for (i = 0; i < size; i++) {
        print_json_aux(json_array_get(element, i), indent + 2);
    }
}

static void print_json_string(json_t *element, int indent) {
    print_json_indent(indent);
    printf("JSON String: \"%s\"\n", json_string_value(element));
}

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

static void print_json_aux(json_t *element, int indent)
{
    switch(json_typeof(element)) {
        case JSON_OBJECT:
            print_json_object(element, indent);
            break;

        case JSON_ARRAY:
            print_json_array(element, indent);
            break;

        case JSON_STRING:
            print_json_string(element, indent);
            break;

        case JSON_INTEGER:
            print_json_integer(element, indent);
            break;

        case JSON_REAL:
            print_json_real(element, indent);
            break;

        case JSON_TRUE:
            print_json_true(element, indent);
            break;

        case JSON_FALSE:
            print_json_false(element, indent);
            break;

        case JSON_NULL:
            print_json_null(element, indent);
            break;

        default:
            fprintf(stderr, "unrecongnized JSON type %d\n", json_typeof(element));
    }
}

static void print_json(json_t *root) { print_json_aux(root, 0); }

static json_t *load_json(char *jsonbuf)
{
    json_t *root;
    json_error_t error;

    root = json_loads(jsonbuf, 0, &error);

    if (root) {
        return root;
    } else {
        fprintf(stderr, "json error on line %d: %s\n", error.line, error.text);
        return NULL;
    }
}

static RecordSchema *parse_json_to_recordschema(json_t *element)
{
    RecordSchema *recordSchema = malloc(sizeof(RecordSchema));
    assert(recordSchema);

    if (JSON_OBJECT != json_typeof(element)) {
        fprintf(stderr, "%s() LN%d, json passed is not an object\n",
                __func__, __LINE__);
        return NULL;
    }

    const char *key;
    json_t *value;

    json_object_foreach(element, key, value) {
        if (0 == strcmp(key, "name")) {
            tstrncpy(recordSchema->name, json_string_value(value), RECORD_NAME_LEN-1);
        } else if (0 == strcmp(key, "fields")) {
            if (JSON_ARRAY == json_typeof(value)) {

                size_t i;
                size_t size = json_array_size(value);

                verbosePrint("%s() LN%d, JSON Array of %lld element%s:\n",
                        __func__, __LINE__,
                        (long long)size, json_plural(size));

                recordSchema->num_fields = size;
                recordSchema->fields = malloc(sizeof(FieldStruct) * size);
                assert(recordSchema->fields);

                for (i = 0; i < size; i++) {
                    FieldStruct *field = (FieldStruct *)(recordSchema->fields + sizeof(FieldStruct) * i);
                    json_t *arr_element = json_array_get(value, i);
                    const char *ele_key;
                    json_t *ele_value;

                    json_object_foreach(arr_element, ele_key, ele_value) {
                        if (0 == strcmp(ele_key, "name")) {
                            tstrncpy(field->name, json_string_value(ele_value), FIELD_NAME_LEN-1);
                        } else if (0 == strcmp(ele_key, "type")) {
                            if (JSON_STRING == json_typeof(ele_value)) {
                                tstrncpy(field->type, json_string_value(ele_value), TYPE_NAME_LEN-1);
                            } else if (JSON_OBJECT == json_typeof(ele_value)) {
                                const char *obj_key;
                                json_t *obj_value;

                                json_object_foreach(ele_value, obj_key, obj_value) {
                                    if (0 == strcmp(obj_key, "type")) {
                                        if (JSON_STRING == json_typeof(obj_value)) {
                                            tstrncpy(field->type,
                                                    json_string_value(obj_value), TYPE_NAME_LEN-1);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                fprintf(stderr, "%s() LN%d, fields have no array\n",
                        __func__, __LINE__);
                return NULL;
            }

            break;
        }
    }

    return recordSchema;
}

static void freeRecordSchema(RecordSchema *recordSchema)
{
    if (recordSchema) {
        if (recordSchema->fields) {
            free(recordSchema->fields);
        }
        free(recordSchema);
    }
}

static int64_t writeResultToAvro(
        char *avroFilename,
        char *jsonSchema,
        TAOS_RES *res)
{
    avro_schema_t schema;
    if (avro_schema_from_json_length(jsonSchema, strlen(jsonSchema), &schema)) {
        errorPrint("%s() LN%d, Unable to parse:\n%s \nto schema\nerror message: %s\n",
                __func__, __LINE__, jsonSchema, avro_strerror());
        exit(EXIT_FAILURE);
    }

    json_t *json_root = load_json(jsonSchema);
    debugPrint("\n%s() LN%d\n *** Schema parsed:\n", __func__, __LINE__);

    RecordSchema *recordSchema;
    if (json_root) {
        if (g_args.debug_print || g_args.verbose_print) {
            print_json(json_root);
        }

        recordSchema = parse_json_to_recordschema(json_root);
        if (NULL == recordSchema) {
            fprintf(stderr, "Failed to parse json to recordschema\n");
            exit(EXIT_FAILURE);
        }

        json_decref(json_root);
    } else {
        errorPrint("json:\n%s\n can't be parsed by jansson\n", jsonSchema);
        exit(EXIT_FAILURE);
    }

    avro_file_writer_t db;

    int rval = avro_file_writer_create_with_codec
        (avroFilename, schema, &db, g_avro_codec[g_args.avro_codec], 0);
    if (rval) {
        errorPrint("There was an error creating %s. reason: %s\n",
                avroFilename, avro_strerror());
        exit(EXIT_FAILURE);
    }

    TAOS_ROW row = NULL;

    int numFields = taos_field_count(res);
    assert(numFields > 0);
    TAOS_FIELD *fields = taos_fetch_fields(res);

    avro_value_iface_t  *wface =
        avro_generic_class_from_schema(schema);

    avro_value_t record;
    avro_generic_value_new(wface, &record);

    int64_t count = 0;
    while ((row = taos_fetch_row(res)) != NULL) {
        avro_value_t value;

        for (int col = 0; col < numFields; col++) {
            if (0 != avro_value_get_by_name(
                        &record, fields[col].name, &value, NULL)) {
                errorPrint("%s() LN%d, avro_value_get_by_name(..%s..) failed",
                        __func__, __LINE__, fields[col].name);
                continue;
            }

            int len;
            switch (fields[col].type) {
                case TSDB_DATA_TYPE_BOOL:
                    if (NULL == row[col]) {
                        avro_value_set_int(&value, TSDB_DATA_BOOL_NULL);
                    } else {
                        avro_value_set_boolean(&value,
                                ((((int32_t)(*((char *)row[col])))==1)?1:0));
                    }
                    break;

                case TSDB_DATA_TYPE_TINYINT:
                    if (NULL == row[col]) {
                        avro_value_set_int(&value, TSDB_DATA_TINYINT_NULL);
                    } else {
                        avro_value_set_int(&value, *((int8_t *)row[col]));
                    }
                    break;

                case TSDB_DATA_TYPE_SMALLINT:
                    if (NULL == row[col]) {
                        avro_value_set_int(&value, TSDB_DATA_SMALLINT_NULL);
                    } else {
                        avro_value_set_int(&value, *((int16_t *)row[col]));
                    }
                    break;

                case TSDB_DATA_TYPE_INT:
                    if (NULL == row[col]) {
                        avro_value_set_int(&value, TSDB_DATA_INT_NULL);
                    } else {
                        avro_value_set_int(&value, *((int32_t *)row[col]));
                    }
                    break;

                case TSDB_DATA_TYPE_BIGINT:
                    if (NULL == row[col]) {
                        avro_value_set_long(&value, TSDB_DATA_BIGINT_NULL);
                    } else {
                        avro_value_set_long(&value, *((int64_t *)row[col]));
                    }
                    break;

                case TSDB_DATA_TYPE_FLOAT:
                    if (NULL == row[col]) {
                        avro_value_set_float(&value, TSDB_DATA_FLOAT_NULL);
                    } else {
                        avro_value_set_float(&value, GET_FLOAT_VAL(row[col]));
                    }
                    break;

                case TSDB_DATA_TYPE_DOUBLE:
                    if (NULL == row[col]) {
                        avro_value_set_double(&value, TSDB_DATA_DOUBLE_NULL);
                    } else {
                        avro_value_set_double(&value, GET_DOUBLE_VAL(row[col]));
                    }
                    break;

                case TSDB_DATA_TYPE_BINARY:
                    if (NULL == row[col]) {
                        avro_value_set_string(&value,
                                (char *)NULL);
                    } else {
                        avro_value_set_string(&value, (char *)row[col]);
                    }
                    break;

                case TSDB_DATA_TYPE_NCHAR:
                    if (NULL == row[col]) {
                        avro_value_set_bytes(&value,
                                (void*)NULL,0);
                    } else {
                        len = strlen((char*)row[col]);
                        avro_value_set_bytes(&value, (void*)(row[col]),len);
                    }
                    break;

                case TSDB_DATA_TYPE_TIMESTAMP:
                    if (NULL == row[col]) {
                        avro_value_set_long(&value, TSDB_DATA_BIGINT_NULL);
                    } else {
                        avro_value_set_long(&value, *((int64_t *)row[col]));
                    }
                    break;

                default:
                    break;
            }
        }

        if (0 != avro_file_writer_append_value(db, &record)) {
            errorPrint("%s() LN%d, Unable to write record to file. Message: %s\n",
                    __func__, __LINE__,
                    avro_strerror());
        } else {
            count ++;
        }
    }

    avro_value_decref(&record);
    avro_value_iface_decref(wface);
    freeRecordSchema(recordSchema);
    avro_file_writer_close(db);
    avro_schema_decref(schema);

    return count;
}

void freeBindArray(char *bindArray, int onlyCol)
{
    TAOS_BIND *bind;

    for (int j = 0; j < onlyCol; j++) {
        bind = (TAOS_BIND *)((char *)bindArray + (sizeof(TAOS_BIND) * j));
        if ((TSDB_DATA_TYPE_BINARY != bind->buffer_type)
                && (TSDB_DATA_TYPE_NCHAR != bind->buffer_type)) {
            tfree(bind->buffer);
        }
    }
}

static int dumpInOneAvroFile(char* fcharset,
        char* encode, char *avroFilepath)
{
    debugPrint("avroFilepath: %s\n", avroFilepath);

    avro_file_reader_t reader;

    if(avro_file_reader(avroFilepath, &reader)) {
        fprintf(stderr, "Unable to open avro file %s: %s\n",
                avroFilepath, avro_strerror());
        return -1;
    }

    int buf_len = TSDB_MAX_COLUMNS * (TSDB_COL_NAME_LEN + 11 + 16) + 4;
    char *jsonbuf = calloc(1, buf_len);
    assert(jsonbuf);

    avro_writer_t jsonwriter = avro_writer_memory(jsonbuf, buf_len);;

    avro_schema_t schema;
    schema = avro_file_reader_get_writer_schema(reader);
    avro_schema_to_json(schema, jsonwriter);

    if (0 == strlen(jsonbuf)) {
        errorPrint("Failed to parse avro file: %s schema. reason: %s\n",
                avroFilepath, avro_strerror());
        avro_schema_decref(schema);
        avro_file_reader_close(reader);
        avro_writer_free(jsonwriter);
        return -1;
    }
    debugPrint("Schema:\n  %s\n", jsonbuf);

    json_t *json_root = load_json(jsonbuf);
    debugPrint("\n%s() LN%d\n *** Schema parsed:\n", __func__, __LINE__);
    if (g_args.debug_print) {
        print_json(json_root);
    }

    const char *namespace = avro_schema_namespace((const avro_schema_t)schema);
    debugPrint("Namespace: %s\n", namespace);

    TAOS *taos = taos_connect(g_args.host, g_args.user, g_args.password,
            namespace, g_args.port);
    if (taos == NULL) {
        errorPrint("Failed to connect to TDengine server %s\n", g_args.host);
        return -1;
    }

    TAOS_STMT *stmt = taos_stmt_init(taos);
    if (NULL == stmt) {
        taos_close(taos);
        errorPrint("%s() LN%d, stmt init failed! reason: %s\n",
                __func__, __LINE__, taos_errstr(NULL));
        return -1;
    }

    RecordSchema *recordSchema = parse_json_to_recordschema(json_root);
    if (NULL == recordSchema) {
        errorPrint("Failed to parse json to recordschema. reason: %s\n",
                avro_strerror());
        avro_schema_decref(schema);
        avro_file_reader_close(reader);
        avro_writer_free(jsonwriter);
        return -1;
    }
    json_decref(json_root);

    TableDef *tableDes = (TableDef *)calloc(1, sizeof(TableDef)
            + sizeof(ColDes) * TSDB_MAX_COLUMNS);

    int allColCount = getTableDes(taos, (char *)namespace, recordSchema->name, tableDes, false);

    if (allColCount < 0) {
        errorPrint("%s() LN%d, failed to get table[%s] schema\n",
                __func__,
                __LINE__,
                recordSchema->name);
        free(tableDes);
        freeRecordSchema(recordSchema);
        avro_schema_decref(schema);
        avro_file_reader_close(reader);
        avro_writer_free(jsonwriter);
        return -1;
    }

    char *stmtBuffer = calloc(1, TSDB_MAX_ALLOWED_SQL_LEN);
    assert(stmtBuffer);
    char *pstr = stmtBuffer;
    pstr += sprintf(pstr, "INSERT INTO ? VALUES(?");

    int onlyCol = 1; // at least timestamp
    for (int col = 1; col < allColCount; col++) {
        if (strcmp(tableDes->cols[col].note, "TAG") == 0) continue;
        pstr += sprintf(pstr, ",?");
        onlyCol ++;
    }
    pstr += sprintf(pstr, ")");

    if (0 != taos_stmt_prepare(stmt, stmtBuffer, 0)) {
        errorPrint("Failed to execute taos_stmt_prepare(). reason: %s\n",
                taos_stmt_errstr(stmt));

        free(stmtBuffer);
        free(tableDes);
        freeRecordSchema(recordSchema);
        avro_schema_decref(schema);
        avro_file_reader_close(reader);
        avro_writer_free(jsonwriter);
        return -1;
    }

    if (0 != taos_stmt_set_tbname(stmt, recordSchema->name)) {
        errorPrint("Failed to execute taos_stmt_set_tbname(%s). reason: %s\n",
                recordSchema->name, taos_stmt_errstr(stmt));

        free(stmtBuffer);
        free(tableDes);
        avro_schema_decref(schema);
        avro_file_reader_close(reader);
        avro_writer_free(jsonwriter);
        return -1;
    }

    avro_value_iface_t *value_class = avro_generic_class_from_schema(schema);
    avro_value_t value;
    avro_generic_value_new(value_class, &value);

    char *bindArray =
            malloc(sizeof(TAOS_BIND) * onlyCol);
    assert(bindArray);

    int success = 0;
    int failed = 0;
    while(!avro_file_reader_read_value(reader, &value)) {
        memset(bindArray, 0, sizeof(TAOS_BIND) * onlyCol);
        TAOS_BIND *bind;

        for (int i = 0; i < recordSchema->num_fields; i++) {
            bind = (TAOS_BIND *)((char *)bindArray + (sizeof(TAOS_BIND) * i));

            avro_value_t field_value;

            FieldStruct *field = (FieldStruct *)(recordSchema->fields + sizeof(FieldStruct) * i);

            bind->is_null = NULL;
            int is_null = 1;
            if (0 == i) {
                int64_t *ts = malloc(sizeof(int64_t));
                assert(ts);

                avro_value_get_by_name(&value, field->name, &field_value, NULL);
                avro_value_get_long(&field_value, ts);

                bind->buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
                bind->buffer_length = sizeof(int64_t);
                bind->buffer = ts;
                bind->length = &bind->buffer_length;
            } else if (0 == avro_value_get_by_name(
                        &value, field->name, &field_value, NULL)) {

                if (0 == strcasecmp(tableDes->cols[i].type, "int")) {
                    int32_t *n32 = malloc(sizeof(int32_t));
                    assert(n32);

                    avro_value_get_int(&field_value, n32);
                    debugPrint("%d | ", *n32);
                    bind->buffer_type = TSDB_DATA_TYPE_INT;
                    bind->buffer_length = sizeof(int32_t);
                    bind->buffer = n32;
                } else if (0 == strcasecmp(tableDes->cols[i].type, "tinyint")) {
                    int32_t *n8 = malloc(sizeof(int32_t));
                    assert(n8);

                    avro_value_get_int(&field_value, n8);
                    debugPrint("%d | ", *n8);
                    bind->buffer_type = TSDB_DATA_TYPE_TINYINT;
                    bind->buffer_length = sizeof(int8_t);
                    bind->buffer = (int8_t *)n8;
                } else if (0 == strcasecmp(tableDes->cols[i].type, "smallint")) {
                    int32_t *n16 = malloc(sizeof(int32_t));
                    assert(n16);

                    avro_value_get_int(&field_value, n16);
                    debugPrint("%d | ", *n16);
                    bind->buffer_type = TSDB_DATA_TYPE_SMALLINT;
                    bind->buffer_length = sizeof(int16_t);
                    bind->buffer = (int32_t*)n16;
                } else if (0 == strcasecmp(tableDes->cols[i].type, "bigint")) {
                    int64_t *n64 = malloc(sizeof(int64_t));
                    assert(n64);

                    avro_value_get_long(&field_value, n64);
                    debugPrint("%"PRId64" | ", *n64);
                    bind->buffer_type = TSDB_DATA_TYPE_BIGINT;
                    bind->buffer_length = sizeof(int64_t);
                    bind->buffer = n64;
                } else if (0 == strcasecmp(tableDes->cols[i].type, "timestamp")) {
                    int64_t *n64 = malloc(sizeof(int64_t));
                    assert(n64);

                    avro_value_get_long(&field_value, n64);
                    debugPrint("%"PRId64" | ", *n64);
                    bind->buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
                    bind->buffer_length = sizeof(int64_t);
                    bind->buffer = n64;
                } else if (0 == strcasecmp(tableDes->cols[i].type, "float")) {
                    float *f = malloc(sizeof(float));
                    assert(f);

                    avro_value_get_float(&field_value, f);
                    if (TSDB_DATA_FLOAT_NULL == *f) {
                        debugPrint("%s | ", "NULL");
                        bind->is_null = &is_null;
                    } else {
                        debugPrint("%f | ", *f);
                        bind->buffer = f;
                    }
                    bind->buffer_type = TSDB_DATA_TYPE_FLOAT;
                    bind->buffer_length = sizeof(float);
                } else if (0 == strcasecmp(tableDes->cols[i].type, "double")) {
                    double *dbl = malloc(sizeof(double));
                    assert(dbl);

                    avro_value_get_double(&field_value, dbl);
                    if (TSDB_DATA_DOUBLE_NULL == *dbl) {
                        debugPrint("%s | ", "NULL");
                        bind->is_null = &is_null;
                    } else {
                        debugPrint("%f | ", *dbl);
                        bind->buffer = dbl;
                    }
                    bind->buffer = dbl;
                    bind->buffer_type = TSDB_DATA_TYPE_DOUBLE;
                    bind->buffer_length = sizeof(double);
                } else if (0 == strcasecmp(tableDes->cols[i].type, "binary")) {
                    size_t size;

                    char *buf = NULL;
                    avro_value_get_string(&field_value, (const char **)&buf, &size);
                    debugPrint("%s | ", (char *)buf);
                    bind->buffer_type = TSDB_DATA_TYPE_BINARY;
                    bind->buffer_length = tableDes->cols[i].length;
                    bind->buffer = buf;
                } else if (0 == strcasecmp(tableDes->cols[i].type, "nchar")) {
                    size_t bytessize;
                    void *bytesbuf = NULL;

                    avro_value_get_bytes(&field_value, (const void **)&bytesbuf, &bytessize);
                    debugPrint("%s | ", (char*)bytesbuf);
                    bind->buffer_type = TSDB_DATA_TYPE_NCHAR;
                    bind->buffer_length = tableDes->cols[i].length;
                    bind->buffer = bytesbuf;
                } else if (0 == strcasecmp(tableDes->cols[i].type, "bool")) {
                    int32_t *bl = malloc(sizeof(int32_t));
                    assert(bl);

                    avro_value_get_boolean(&field_value, bl);
                    debugPrint("%s | ", (*bl)?"true":"false");
                    bind->buffer_type = TSDB_DATA_TYPE_BOOL;
                    bind->buffer_length = sizeof(int8_t);
                    bind->buffer = (int8_t*)bl;
                }

                bind->length = &bind->buffer_length;
            }

        }
        debugPrint("%s", "\n");

        if (0 != taos_stmt_bind_param(stmt, (TAOS_BIND *)bindArray)) {
            errorPrint("%s() LN%d stmt_bind_param() failed! reason: %s\n",
                    __func__, __LINE__, taos_stmt_errstr(stmt));
            freeBindArray(bindArray, onlyCol);
            failed --;
            continue;
        }
        if (0 != taos_stmt_add_batch(stmt)) {
            errorPrint("%s() LN%d stmt_bind_param() failed! reason: %s\n",
                    __func__, __LINE__, taos_stmt_errstr(stmt));
            freeBindArray(bindArray, onlyCol);
            failed --;
            continue;
        }

        freeBindArray(bindArray, onlyCol);

        success ++;
        continue;
    }

    if (0 != taos_stmt_execute(stmt)) {
        errorPrint("%s() LN%d stmt_bind_param() failed! reason: %s\n",
                __func__, __LINE__, taos_stmt_errstr(stmt));
        failed = success;
    }

    avro_value_decref(&value);
    avro_value_iface_decref(value_class);

    tfree(bindArray);

    tfree(stmtBuffer);
    tfree(tableDes);

    freeRecordSchema(recordSchema);
    avro_schema_decref(schema);
    avro_file_reader_close(reader);
    avro_writer_free(jsonwriter);

    tfree(jsonbuf);

    taos_stmt_close(stmt);
    taos_close(taos);

    if (failed < 0)
        return failed;
    return success;
}

static void* dumpInAvroWorkThreadFp(void *arg)
{
    threadInfo *pThread = (threadInfo*)arg;
    setThreadName("dumpInAvroWorkThrd");
    verbosePrint("[%d] process %"PRId64" files from %"PRId64"\n",
                    pThread->threadIndex, pThread->count, pThread->from);

    for (int64_t i = 0; i < pThread->count; i++) {
        char avroFile[MAX_PATH_LEN];
        sprintf(avroFile, "%s/%s", g_args.inpath,
                g_tsDumpInAvroFiles[pThread->from + i]);

        if (0 == dumpInOneAvroFile(g_tsCharset,
                    g_args.encode,
                    avroFile)) {
            okPrint("[%d] Success dump in file: %s\n",
                    pThread->threadIndex, avroFile);
        }
    }

    return NULL;
}

static int64_t dumpInAvroWorkThreads()
{
    int64_t ret = 0;

    int32_t threads = g_args.thread_num;

    uint64_t avroFileCount = getFilesNum("avro");
    if (0 == avroFileCount) {
        debugPrint("No .avro file found in %s\n", g_args.inpath);
        return 0;
    }

    createDumpinList("avro", avroFileCount);

    threadInfo *pThread;

    pthread_t *pids = calloc(1, threads * sizeof(pthread_t));
    threadInfo *infos = (threadInfo *)calloc(
            threads, sizeof(threadInfo));
    assert(pids);
    assert(infos);

    int64_t a = avroFileCount / threads;
    if (a < 1) {
        threads = avroFileCount;
        a = 1;
    }

    int64_t b = 0;
    if (threads != 0) {
        b = avroFileCount % threads;
    }

    int64_t from = 0;

    for (int32_t t = 0; t < threads; ++t) {
        pThread = infos + t;
        pThread->threadIndex = t;

        pThread->from = from;
        pThread->count = t<b?a+1:a;
        from += pThread->count;
        verbosePrint(
                "Thread[%d] takes care avro files total %"PRId64" files from %"PRId64"\n",
                t, pThread->count, pThread->from);

        if (pthread_create(pids + t, NULL,
                    dumpInAvroWorkThreadFp, (void*)pThread) != 0) {
            errorPrint("%s() LN%d, thread[%d] failed to start\n",
                    __func__, __LINE__, pThread->threadIndex);
            exit(EXIT_FAILURE);
        }
    }

    for (int t = 0; t < threads; ++t) {
        pthread_join(pids[t], NULL);
    }

    free(infos);
    free(pids);

    freeFileList(g_tsDumpInAvroFiles, avroFileCount);

    return ret;
}

#endif /* AVRO_SUPPORT */

static int64_t writeResultToSql(TAOS_RES *res, FILE *fp, char *dbName, char *tbName)
{
    int64_t    totalRows     = 0;

    int32_t  sql_buf_len = g_args.max_sql_len;
    char* tmpBuffer = (char *)calloc(1, sql_buf_len + 128);
    assert(tmpBuffer);

    char *pstr = tmpBuffer;

    TAOS_ROW row = NULL;
    int rowFlag = 0;
    int64_t    lastRowsPrint = 5000000;
    int count = 0;

    int numFields = taos_field_count(res);
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
                            ((((int32_t)(*((char *)row[col])))==1)?1:0));
                    break;

                case TSDB_DATA_TYPE_TINYINT:
                    curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, "%d",
                            *((int8_t *)row[col]));
                    break;

                case TSDB_DATA_TYPE_SMALLINT:
                    curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, "%d",
                            *((int16_t *)row[col]));
                    break;

                case TSDB_DATA_TYPE_INT:
                    curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, "%d",
                            *((int32_t *)row[col]));
                    break;

                case TSDB_DATA_TYPE_BIGINT:
                    curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len,
                            "%" PRId64 "",
                            *((int64_t *)row[col]));
                    break;

                case TSDB_DATA_TYPE_FLOAT:
                    curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, "%f",
                            GET_FLOAT_VAL(row[col]));
                    break;

                case TSDB_DATA_TYPE_DOUBLE:
                    curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, "%f",
                            GET_DOUBLE_VAL(row[col]));
                    break;

                case TSDB_DATA_TYPE_BINARY:
                    {
                        char tbuf[COMMAND_SIZE] = {0};
                        converStringToReadable((char *)row[col], length[col],
                                tbuf, COMMAND_SIZE);
                        curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len,
                                "\'%s\'", tbuf);
                        break;
                    }
                case TSDB_DATA_TYPE_NCHAR:
                    {
                        char tbuf[COMMAND_SIZE] = {0};
                        convertNCharToReadable((char *)row[col], length[col],
                                tbuf, COMMAND_SIZE);
                        curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len,
                                "\'%s\'", tbuf);
                        break;
                    }
                case TSDB_DATA_TYPE_TIMESTAMP:
                    if (!g_args.mysqlFlag) {
                        curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len,
                                "%" PRId64 "",
                                *(int64_t *)row[col]);
                    } else {
                        char buf[64] = "\0";
                        int64_t ts = *((int64_t *)row[col]);
                        time_t tt = (time_t)(ts / 1000);
                        struct tm *ptm = localtime(&tt);
                        strftime(buf, 64, "%y-%m-%d %H:%M:%S", ptm);
                        curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len,
                                "\'%s.%03d\'",
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
    free(tmpBuffer);

    return totalRows;
}

static int64_t dumpTableData(FILE *fp, char *tbName,
        char* dbName, int precision,
        char *jsonSchema) {
    int64_t    totalRows     = 0;

    char sqlstr[1024] = {0};

    int64_t start_time, end_time;
    if (strlen(g_args.humanStartTime)) {
        if (TSDB_CODE_SUCCESS != taosParseTime(
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

    if (strlen(g_args.humanEndTime)) {
        if (TSDB_CODE_SUCCESS != taosParseTime(
                g_args.humanEndTime, &end_time, strlen(g_args.humanEndTime),
                precision, 0)) {
            errorPrint("Input %s, time format error!\n", g_args.humanEndTime);
            return -1;
        }
    } else {
        end_time = g_args.end_time;
    }

    sprintf(sqlstr,
            "select * from %s.%s where _c0 >= %" PRId64 " and _c0 <= %" PRId64 " order by _c0 asc;",
            dbName, tbName, start_time, end_time);

    TAOS *taos = taos_connect(g_args.host,
            g_args.user, g_args.password, dbName, g_args.port);
    if (NULL == taos) {
        errorPrint(
                "Failed to connect to TDengine server %s by specified database %s\n",
                g_args.host, dbName);
        return -1;
    }

    TAOS_RES* res = taos_query(taos, sqlstr);
    int32_t code = taos_errno(res);
    if (code != 0) {
        errorPrint("failed to run command %s, reason: %s\n",
                sqlstr, taos_errstr(res));
        taos_free_result(res);
        taos_close(taos);
        return -1;
    }

#ifdef AVRO_SUPPORT
    if (g_args.avro) {
        char avroFilename[MAX_PATH_LEN] = {0};

        if (g_args.outpath[0] != 0) {
            sprintf(avroFilename, "%s/%s.%s.avro",
                    g_args.outpath, dbName, tbName);
        } else {
            sprintf(avroFilename, "%s.%s.avro",
                    dbName, tbName);
        }

        totalRows = writeResultToAvro(avroFilename, jsonSchema, res);
    } else
#endif
        totalRows = writeResultToSql(res, fp, dbName, tbName);

    taos_free_result(res);
    taos_close(taos);
    return totalRows;
}

static int64_t dumpNormalTable(
        TAOS *taos,
        char *dbName,
        char *stable,
        char *tbName,
        int precision,
        FILE *fp
        ) {
    int colCount = 0;

    TableDef *tableDes = (TableDef *)calloc(1, sizeof(TableDef)
            + sizeof(ColDes) * TSDB_MAX_COLUMNS);

    if (stable != NULL && stable[0] != '\0') {  // dump table schema which is created by using super table
        colCount = getTableDes(taos, dbName, tbName, tableDes, false);

        if (colCount < 0) {
            errorPrint("%s() LN%d, failed to get table[%s] schema\n",
                    __func__,
                    __LINE__,
                    tbName);
            free(tableDes);
            return -1;
        }

        // create child-table using super-table
        dumpCreateMTableClause(dbName, stable, tableDes, colCount, fp);
    } else {  // dump table definition
        colCount = getTableDes(taos, dbName, tbName, tableDes, false);

        if (colCount < 0) {
            errorPrint("%s() LN%d, failed to get table[%s] schema\n",
                    __func__,
                    __LINE__,
                    tbName);
            free(tableDes);
            return -1;
        }

        // create normal-table or super-table
        dumpCreateTableClause(tableDes, colCount, fp, dbName);
    }

    char *jsonSchema = NULL;
#ifdef AVRO_SUPPORT
    if (g_args.avro) {
        if (0 != convertTbDesToJson(
                    dbName, tbName, tableDes, colCount, &jsonSchema)) {
            errorPrint("%s() LN%d, convertTbDesToJson failed\n",
                    __func__,
                    __LINE__);
            freeTbDes(tableDes);
            return -1;
        }
    }
#endif

    int64_t totalRows = 0;
    if (!g_args.schemaonly) {
        totalRows = dumpTableData(fp, tbName, dbName, precision,
            jsonSchema);
    }

    tfree(jsonSchema);
    freeTbDes(tableDes);
    return totalRows;
}

static int64_t dumpNormalTableWithoutStb(TAOS *taos, SDbInfo *dbInfo, char *ntbName)
{
    int64_t count = 0;

    char tmpBuf[MAX_PATH_LEN] = {0};
    FILE *fp = NULL;

    if (g_args.outpath[0] != 0) {
        sprintf(tmpBuf, "%s/%s.%s.sql",
                g_args.outpath, dbInfo->name, ntbName);
    } else {
        sprintf(tmpBuf, "%s.%s.sql",
                dbInfo->name, ntbName);
    }

    fp = fopen(tmpBuf, "w");
    if (fp == NULL) {
        errorPrint("%s() LN%d, failed to open file %s\n",
                __func__, __LINE__, tmpBuf);
        return -1;
    }

    count = dumpNormalTable(
            taos,
            dbInfo->name,
            NULL,
            ntbName,
            getPrecisionByString(dbInfo->precision),
            fp);
    if (count > 0) {
        atomic_add_fetch_64(&g_totalDumpOutRows, count);
    }
    fclose(fp);
    return count;
}

static int64_t dumpNormalTableBelongStb(
        TAOS *taos,
        SDbInfo *dbInfo, char *stbName, char *ntbName)
{
    int64_t count = 0;

    char tmpBuf[MAX_PATH_LEN] = {0};
    FILE *fp = NULL;

    if (g_args.outpath[0] != 0) {
        sprintf(tmpBuf, "%s/%s.%s.sql",
                g_args.outpath, dbInfo->name, ntbName);
    } else {
        sprintf(tmpBuf, "%s.%s.sql",
                dbInfo->name, ntbName);
    }

    fp = fopen(tmpBuf, "w");
    if (fp == NULL) {
        errorPrint("%s() LN%d, failed to open file %s\n",
                __func__, __LINE__, tmpBuf);
        return -1;
    }

    count = dumpNormalTable(
            taos,
            dbInfo->name,
            stbName,
            ntbName,
            getPrecisionByString(dbInfo->precision),
            fp);
    if (count > 0) {
        atomic_add_fetch_64(&g_totalDumpOutRows, count);
    }

    fclose(fp);
    return count;
}

static void *dumpNtbOfDb(void *arg) {
    threadInfo *pThreadInfo = (threadInfo *)arg;

    debugPrint("dump table from = \t%"PRId64"\n", pThreadInfo->from);
    debugPrint("dump table count = \t%"PRId64"\n",
            pThreadInfo->count);

    FILE *fp = NULL;
    char tmpBuf[MAX_PATH_LEN] = {0};

    if (g_args.outpath[0] != 0) {
        sprintf(tmpBuf, "%s/%s.%d.sql",
                g_args.outpath, pThreadInfo->dbName, pThreadInfo->threadIndex);
    } else {
        sprintf(tmpBuf, "%s.%d.sql",
                pThreadInfo->dbName, pThreadInfo->threadIndex);
    }

    fp = fopen(tmpBuf, "w");

    if (fp == NULL) {
        errorPrint("%s() LN%d, failed to open file %s\n",
                __func__, __LINE__, tmpBuf);
        return NULL;
    }

    int64_t count;
    for (int64_t i = 0; i < pThreadInfo->count; i++) {
        debugPrint("[%d] No.\t%"PRId64" table name: %s\n",
                pThreadInfo->threadIndex, i,
                ((TableInfo *)(g_tablesList + pThreadInfo->from+i))->name);
        count = dumpNormalTable(
                pThreadInfo->taos,
                pThreadInfo->dbName,
                ((TableInfo *)(g_tablesList + pThreadInfo->from+i))->stable,
                ((TableInfo *)(g_tablesList + pThreadInfo->from+i))->name,
                pThreadInfo->precision,
                fp);
        if (count < 0) {
            break;
        } else {
            atomic_add_fetch_64(&g_totalDumpOutRows, count);
        }
    }

    fclose(fp);
    return NULL;
}

static int checkParam() {
    if (g_args.all_databases && g_args.databases) {
        errorPrint("%s", "conflict option --all-databases and --databases\n");
        return -1;
    }

    if (g_args.start_time > g_args.end_time) {
        errorPrint("%s", "start time is larger than end time\n");
        return -1;
    }

    if (g_args.arg_list_len == 0) {
        if ((!g_args.all_databases) && (!g_args.databases) && (!g_args.isDumpIn)) {
            errorPrint("%s", "taosdump requires parameters\n");
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

static void dumpCharset(FILE *fp) {
    char charsetline[256];

    (void)fseek(fp, 0, SEEK_SET);
    sprintf(charsetline, "#!%s\n", tsCharset);
    (void)fwrite(charsetline, strlen(charsetline), 1, fp);
}

static void loadFileCharset(FILE *fp, char *fcharset) {
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

static int dumpInOneSqlFile(TAOS* taos, FILE* fp, char* fcharset,
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
            errorPrint("%s() LN%d, error sql: lineno:%d, file:%s\n",
                    __func__, __LINE__, lineNo, fileName);
            fprintf(g_fpOfResult, "error sql: lineno:%d, file:%s\n", lineNo, fileName);
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
    return 0;
}

static void* dumpInSqlWorkThreadFp(void *arg)
{
    threadInfo *pThread = (threadInfo*)arg;
    setThreadName("dumpInSqlWorkThrd");
    fprintf(stderr, "[%d] Start to process %"PRId64" files from %"PRId64"\n",
                    pThread->threadIndex, pThread->count, pThread->from);

    for (int64_t i = 0; i < pThread->count; i++) {
        char sqlFile[MAX_PATH_LEN];
        sprintf(sqlFile, "%s/%s", g_args.inpath, g_tsDumpInSqlFiles[pThread->from + i]);

        FILE* fp = openDumpInFile(sqlFile);
        if (NULL == fp) {
            errorPrint("[%d] Failed to open input file: %s\n",
                    pThread->threadIndex, sqlFile);
            continue;
        }

        if (0 == dumpInOneSqlFile(pThread->taos, fp, g_tsCharset, g_args.encode,
                    sqlFile)) {
            okPrint("[%d] Success dump in file: %s\n",
                    pThread->threadIndex, sqlFile);
        }
        fclose(fp);
    }

    return NULL;
}

static int dumpInSqlWorkThreads()
{
    int32_t threads = g_args.thread_num;

    uint64_t sqlFileCount = getFilesNum("sql");
    if (0 == sqlFileCount) {
        debugPrint("No .sql file found in %s\n", g_args.inpath);
        return 0;
    }

    createDumpinList("sql", sqlFileCount);

    threadInfo *pThread;

    pthread_t *pids = calloc(1, threads * sizeof(pthread_t));
    threadInfo *infos = (threadInfo *)calloc(
            threads, sizeof(threadInfo));
    assert(pids);
    assert(infos);

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
        pThread = infos + t;
        pThread->threadIndex = t;

        pThread->from = from;
        pThread->count = t<b?a+1:a;
        from += pThread->count;
        verbosePrint(
                "Thread[%d] takes care sql files total %"PRId64" files from %"PRId64"\n",
                t, pThread->count, pThread->from);

        pThread->taos = taos_connect(g_args.host, g_args.user, g_args.password,
            NULL, g_args.port);
        if (pThread->taos == NULL) {
            errorPrint("Failed to connect to TDengine server %s\n", g_args.host);
            free(infos);
            free(pids);
            return -1;
        }

        if (pthread_create(pids + t, NULL,
                    dumpInSqlWorkThreadFp, (void*)pThread) != 0) {
            errorPrint("%s() LN%d, thread[%d] failed to start\n",
                    __func__, __LINE__, pThread->threadIndex);
            exit(EXIT_FAILURE);
        }
    }

    for (int t = 0; t < threads; ++t) {
        pthread_join(pids[t], NULL);
    }

    for (int t = 0; t < threads; ++t) {
        taos_close(infos[t].taos);
    }
    free(infos);
    free(pids);

    freeFileList(g_tsDumpInSqlFiles, sqlFileCount);

    return 0;
}

static int dumpInDbs()
{
    TAOS *taos = taos_connect(
            g_args.host, g_args.user, g_args.password,
            NULL, g_args.port);

    if (taos == NULL) {
        errorPrint("%s() LN%d, failed to connect to TDengine server\n",
                __func__, __LINE__);
        return -1;
    }

    char dbsSql[MAX_PATH_LEN];
    sprintf(dbsSql, "%s/%s", g_args.inpath, "dbs.sql");

    FILE *fp = openDumpInFile(dbsSql);
    if (NULL == fp) {
        errorPrint("%s() LN%d, failed to open input file %s\n",
                __func__, __LINE__, dbsSql);
        return -1;
    }
    debugPrint("Success Open input file: %s\n", dbsSql);
    loadFileCharset(fp, g_tsCharset);

    if(0 == dumpInOneSqlFile(taos, fp, g_tsCharset, g_args.encode, dbsSql)) {
        okPrint("Success dump in file: %s !\n", dbsSql);
    }

    fclose(fp);
    taos_close(taos);

    return 0;
}

static int64_t dumpIn() {
    assert(g_args.isDumpIn);

    int64_t ret = 0;
    if (dumpInDbs()) {
        errorPrint("%s", "Failed to dump dbs in!\n");
        exit(EXIT_FAILURE);
    }

    ret = dumpInSqlWorkThreads();

#ifdef AVRO_SUPPORT
    if (0 == ret) {
        ret = dumpInAvroWorkThreads();
    }
#endif

    return ret;
}

static void *dumpNormalTablesOfStb(void *arg) {
    threadInfo *pThreadInfo = (threadInfo *)arg;

    debugPrint("dump table from = \t%"PRId64"\n", pThreadInfo->from);
    debugPrint("dump table count = \t%"PRId64"\n", pThreadInfo->count);

    char command[COMMAND_SIZE];

    sprintf(command, "SELECT TBNAME FROM %s.%s LIMIT %"PRId64" OFFSET %"PRId64"",
            pThreadInfo->dbName, pThreadInfo->stbName,
            pThreadInfo->count, pThreadInfo->from);

    TAOS_RES *res = taos_query(pThreadInfo->taos, command);
    int32_t code = taos_errno(res);
    if (code) {
        errorPrint("%s() LN%d, failed to run command <%s>. reason: %s\n",
                __func__, __LINE__, command, taos_errstr(res));
        taos_free_result(res);
        return NULL;
    }

    FILE *fp = NULL;
    char tmpBuf[MAX_PATH_LEN] = {0};

    if (g_args.outpath[0] != 0) {
        sprintf(tmpBuf, "%s/%s.%s.%d.sql",
                g_args.outpath,
                pThreadInfo->dbName,
                pThreadInfo->stbName,
                pThreadInfo->threadIndex);
    } else {
        sprintf(tmpBuf, "%s.%s.%d.sql",
                pThreadInfo->dbName,
                pThreadInfo->stbName,
                pThreadInfo->threadIndex);
    }

    fp = fopen(tmpBuf, "w");

    if (fp == NULL) {
        errorPrint("%s() LN%d, failed to open file %s\n",
                __func__, __LINE__, tmpBuf);
        return NULL;
    }

    TAOS_ROW row = NULL;
    int64_t i = 0;
    int64_t count;
    while((row = taos_fetch_row(res)) != NULL) {
        debugPrint("[%d] sub table %"PRId64": name: %s\n",
                pThreadInfo->threadIndex, i++, (char *)row[TSDB_SHOW_TABLES_NAME_INDEX]);

        count = dumpNormalTable(
                pThreadInfo->taos,
                pThreadInfo->dbName,
                pThreadInfo->stbName,
                (char *)row[TSDB_SHOW_TABLES_NAME_INDEX],
                pThreadInfo->precision,
                fp);
        if (count < 0) {
            break;
        } else {
            atomic_add_fetch_64(&g_totalDumpOutRows, count);
        }
    }

    fclose(fp);
    return NULL;
}

static int64_t dumpNtbOfDbByThreads(
        SDbInfo *dbInfo,
        int64_t ntbCount)
{
    if (ntbCount <= 0) {
        return 0;
    }

    int threads = g_args.thread_num;

    int64_t a = ntbCount / threads;
    if (a < 1) {
        threads = ntbCount;
        a = 1;
    }

    assert(threads);
    int64_t b = ntbCount % threads;

    threadInfo *infos = calloc(1, threads * sizeof(threadInfo));
    pthread_t *pids = calloc(1, threads * sizeof(pthread_t));
    assert(pids);
    assert(infos);

    for (int64_t i = 0; i < threads; i++) {
        threadInfo *pThreadInfo = infos + i;
        pThreadInfo->taos = taos_connect(
                g_args.host,
                g_args.user,
                g_args.password,
                dbInfo->name,
                g_args.port
                );
        if (NULL == pThreadInfo->taos) {
            errorPrint("%s() LN%d, Failed to connect to TDengine, reason: %s\n",
                    __func__,
                    __LINE__,
                    taos_errstr(NULL));
            free(pids);
            free(infos);

            return -1;
        }

        pThreadInfo->threadIndex = i;
        pThreadInfo->count = (i<b)?a+1:a;
        pThreadInfo->from = (i==0)?0:
            ((threadInfo *)(infos + i - 1))->from +
            ((threadInfo *)(infos + i - 1))->count;
        strcpy(pThreadInfo->dbName, dbInfo->name);
        pThreadInfo->precision = getPrecisionByString(dbInfo->precision);

        pthread_create(pids + i, NULL, dumpNtbOfDb, pThreadInfo);
    }

    for (int64_t i = 0; i < threads; i++) {
        pthread_join(pids[i], NULL);
    }

    for (int64_t i = 0; i < threads; i++) {
        threadInfo *pThreadInfo = infos + i;
        taos_close(pThreadInfo->taos);
    }

    free(pids);
    free(infos);

    return 0;
}

static int64_t dumpNTablesOfDb(SDbInfo *dbInfo)
{
    TAOS *taos = taos_connect(g_args.host,
            g_args.user, g_args.password, dbInfo->name, g_args.port);
    if (NULL == taos) {
        errorPrint(
                "Failed to connect to TDengine server %s by specified database %s\n",
                g_args.host, dbInfo->name);
        return 0;
    }

    char command[COMMAND_SIZE];
    TAOS_RES *result;
    int32_t code;

    sprintf(command, "USE %s", dbInfo->name);
    result = taos_query(taos, command);
    code = taos_errno(result);
    if (code != 0) {
        errorPrint("invalid database %s, reason: %s\n",
                dbInfo->name, taos_errstr(result));
        taos_close(taos);
        return 0;
    }

    sprintf(command, "SHOW TABLES");
    result = taos_query(taos, command);
    code = taos_errno(result);
    if (code != 0) {
        errorPrint("Failed to show %s\'s tables, reason: %s\n",
                dbInfo->name, taos_errstr(result));
        taos_close(taos);
        return 0;
    }

    g_tablesList = calloc(1, dbInfo->ntables * sizeof(TableInfo));
    assert(g_tablesList);

    TAOS_ROW row;
    int64_t count = 0;
    while(NULL != (row = taos_fetch_row(result))) {
        debugPrint("%s() LN%d, No.\t%"PRId64" table name: %s\n",
                __func__, __LINE__,
                count, (char *)row[TSDB_SHOW_TABLES_NAME_INDEX]);
        tstrncpy(((TableInfo *)(g_tablesList + count))->name,
                (char *)row[TSDB_SHOW_TABLES_NAME_INDEX], TSDB_TABLE_NAME_LEN);
        char *stbName = (char *) row[TSDB_SHOW_TABLES_METRIC_INDEX];
        if (stbName) {
            tstrncpy(((TableInfo *)(g_tablesList + count))->stable,
                (char *)row[TSDB_SHOW_TABLES_METRIC_INDEX], TSDB_TABLE_NAME_LEN);
            ((TableInfo *)(g_tablesList + count))->belongStb = true;
        }
        count ++;
    }
    taos_close(taos);

    int64_t records = dumpNtbOfDbByThreads(dbInfo, count);

    free(g_tablesList);
    g_tablesList = NULL;

    return records;
}

static int64_t dumpNtbOfStbByThreads(
        SDbInfo *dbInfo, char *stbName)
{
    int64_t ntbCount = getNtbCountOfStb(dbInfo->name, stbName);

    if (ntbCount <= 0) {
        return 0;
    }

    int threads = g_args.thread_num;

    int64_t a = ntbCount / threads;
    if (a < 1) {
        threads = ntbCount;
        a = 1;
    }

    assert(threads);
    int64_t b = ntbCount % threads;

    pthread_t *pids = calloc(1, threads * sizeof(pthread_t));
    threadInfo *infos = calloc(1, threads * sizeof(threadInfo));
    assert(pids);
    assert(infos);

    for (int64_t i = 0; i < threads; i++) {
        threadInfo *pThreadInfo = infos + i;
        pThreadInfo->taos = taos_connect(
                g_args.host,
                g_args.user,
                g_args.password,
                dbInfo->name,
                g_args.port
                );
        if (NULL == pThreadInfo->taos) {
            errorPrint("%s() LN%d, Failed to connect to TDengine, reason: %s\n",
                    __func__,
                    __LINE__,
                    taos_errstr(NULL));
            free(pids);
            free(infos);

            return -1;
        }

        pThreadInfo->threadIndex = i;
        pThreadInfo->count = (i<b)?a+1:a;
        pThreadInfo->from = (i==0)?0:
            ((threadInfo *)(infos + i - 1))->from +
            ((threadInfo *)(infos + i - 1))->count;
        strcpy(pThreadInfo->dbName, dbInfo->name);
        pThreadInfo->precision = getPrecisionByString(dbInfo->precision);

        strcpy(pThreadInfo->stbName, stbName);
        pthread_create(pids + i, NULL, dumpNormalTablesOfStb, pThreadInfo);
    }

    for (int64_t i = 0; i < threads; i++) {
        pthread_join(pids[i], NULL);
    }

    int64_t records = 0;
    for (int64_t i = 0; i < threads; i++) {
        threadInfo *pThreadInfo = infos + i;
        records += pThreadInfo->rowsOfDumpOut;
        taos_close(pThreadInfo->taos);
    }

    free(pids);
    free(infos);

    return records;
}

static int64_t dumpWholeDatabase(SDbInfo *dbInfo, FILE *fp)
{
    dumpCreateDbClause(dbInfo, g_args.with_property, fp);

    fprintf(g_fpOfResult, "\n#### database:                       %s\n",
            dbInfo->name);
    g_resultStatistics.totalDatabasesOfDumpOut++;

    dumpCreateSTableClauseOfDb(dbInfo, fp);

    return dumpNTablesOfDb(dbInfo);
}

static int dumpOut() {
    TAOS     *taos       = NULL;
    TAOS_RES *result     = NULL;

    TAOS_ROW row;
    FILE *fp = NULL;
    int32_t count = 0;

    char tmpBuf[MAX_PATH_LEN] = {0};
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

    g_args.dumpDbCount = getDumpDbCount();
    debugPrint("%s() LN%d, dump db count: %d\n",
            __func__, __LINE__, g_args.dumpDbCount);

    if (0 == g_args.dumpDbCount) {
        errorPrint("%d databases valid to dump\n", g_args.dumpDbCount);
        fclose(fp);
        return -1;
    }

    g_dbInfos = (SDbInfo **)calloc(g_args.dumpDbCount, sizeof(SDbInfo *));
    if (g_dbInfos == NULL) {
        errorPrint("%s() LN%d, failed to allocate memory\n",
                __func__, __LINE__);
        goto _exit_failure;
    }

    char command[COMMAND_SIZE];

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
    dumpCharset(fp);

    sprintf(command, "show databases");
    result = taos_query(taos, command);
    int32_t code = taos_errno(result);

    if (code != 0) {
        errorPrint("%s() LN%d, failed to run command <%s>, reason: %s\n",
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
            if (inDatabasesSeq(
                        (char *)row[TSDB_SHOW_DB_NAME_INDEX],
                        fields[TSDB_SHOW_DB_NAME_INDEX].bytes) != 0) {
                continue;
            }
        } else if (!g_args.all_databases) {  // only input one db
            if (strncasecmp(g_args.arg_list[0],
                        (char *)row[TSDB_SHOW_DB_NAME_INDEX],
                        fields[TSDB_SHOW_DB_NAME_INDEX].bytes) != 0)
                continue;
        }

        g_dbInfos[count] = (SDbInfo *)calloc(1, sizeof(SDbInfo));
        if (g_dbInfos[count] == NULL) {
            errorPrint("%s() LN%d, failed to allocate %"PRIu64" memory\n",
                    __func__, __LINE__, (uint64_t)sizeof(SDbInfo));
            goto _exit_failure;
        }

        okPrint("%s exists\n", (char *)row[TSDB_SHOW_DB_NAME_INDEX]);
        tstrncpy(g_dbInfos[count]->name, (char *)row[TSDB_SHOW_DB_NAME_INDEX],
                min(TSDB_DB_NAME_LEN,
                    fields[TSDB_SHOW_DB_NAME_INDEX].bytes + 1));
        if (g_args.with_property) {
            g_dbInfos[count]->ntables =
                *((int32_t *)row[TSDB_SHOW_DB_NTABLES_INDEX]);
            g_dbInfos[count]->vgroups =
                *((int32_t *)row[TSDB_SHOW_DB_VGROUPS_INDEX]);
            g_dbInfos[count]->replica =
                *((int16_t *)row[TSDB_SHOW_DB_REPLICA_INDEX]);
            g_dbInfos[count]->quorum =
                *((int16_t *)row[TSDB_SHOW_DB_QUORUM_INDEX]);
            g_dbInfos[count]->days =
                *((int16_t *)row[TSDB_SHOW_DB_DAYS_INDEX]);

            tstrncpy(g_dbInfos[count]->keeplist,
                    (char *)row[TSDB_SHOW_DB_KEEP_INDEX],
                    min(32, fields[TSDB_SHOW_DB_KEEP_INDEX].bytes + 1));
            //g_dbInfos[count]->daysToKeep = *((int16_t *)row[TSDB_SHOW_DB_KEEP_INDEX]);
            //g_dbInfos[count]->daysToKeep1;
            //g_dbInfos[count]->daysToKeep2;
            g_dbInfos[count]->cache =
                *((int32_t *)row[TSDB_SHOW_DB_CACHE_INDEX]);
            g_dbInfos[count]->blocks =
                *((int32_t *)row[TSDB_SHOW_DB_BLOCKS_INDEX]);
            g_dbInfos[count]->minrows =
                *((int32_t *)row[TSDB_SHOW_DB_MINROWS_INDEX]);
            g_dbInfos[count]->maxrows =
                *((int32_t *)row[TSDB_SHOW_DB_MAXROWS_INDEX]);
            g_dbInfos[count]->wallevel =
                *((int8_t *)row[TSDB_SHOW_DB_WALLEVEL_INDEX]);
            g_dbInfos[count]->fsync =
                *((int32_t *)row[TSDB_SHOW_DB_FSYNC_INDEX]);
            g_dbInfos[count]->comp =
                (int8_t)(*((int8_t *)row[TSDB_SHOW_DB_COMP_INDEX]));
            g_dbInfos[count]->cachelast =
                (int8_t)(*((int8_t *)row[TSDB_SHOW_DB_CACHELAST_INDEX]));

            tstrncpy(g_dbInfos[count]->precision,
                    (char *)row[TSDB_SHOW_DB_PRECISION_INDEX],
                    DB_PRECISION_LEN);
            g_dbInfos[count]->update =
                *((int8_t *)row[TSDB_SHOW_DB_UPDATE_INDEX]);
        }
        count++;

        if (g_args.databases) {
            if (count > g_args.dumpDbCount)
                break;
        } else if (!g_args.all_databases) {
            if (count >= 1)
                break;
        }
    }

    if (count == 0) {
        errorPrint("%d databases valid to dump\n", count);
        goto _exit_failure;
    }

    if (g_args.databases || g_args.all_databases) { // case: taosdump --databases dbx,dby ...   OR  taosdump --all-databases
        for (int i = 0; i < count; i++) {
            int64_t records = 0;
            records = dumpWholeDatabase(g_dbInfos[i], fp);
            if (records >= 0) {
                okPrint("Database %s dumped\n", g_dbInfos[i]->name);
                g_totalDumpOutRows += records;
            }
        }
    } else {
        if (1 == g_args.arg_list_len) {
            int64_t records = dumpWholeDatabase(g_dbInfos[0], fp);
            if (records >= 0) {
                okPrint("Database %s dumped\n", g_dbInfos[0]->name);
                g_totalDumpOutRows += records;
            }
        } else {
            dumpCreateDbClause(g_dbInfos[0], g_args.with_property, fp);
        }

        int superTblCnt = 0 ;
        for (int i = 1; g_args.arg_list[i]; i++) {
            TableRecordInfo tableRecordInfo;

            if (getTableRecordInfo(g_dbInfos[0]->name,
                        g_args.arg_list[i],
                        &tableRecordInfo) < 0) {
                errorPrint("input the invalid table %s\n",
                        g_args.arg_list[i]);
                continue;
            }

            int64_t records = 0;
            if (tableRecordInfo.isStb) {  // dump all table of this stable
                int ret = dumpStableClasuse(
                        taos,
                        g_dbInfos[0],
                        tableRecordInfo.tableRecord.stable,
                        fp);
                if (ret >= 0) {
                    superTblCnt++;
                    records = dumpNtbOfStbByThreads(g_dbInfos[0], g_args.arg_list[i]);
                }
            } else if (tableRecordInfo.belongStb){
                dumpStableClasuse(
                        taos,
                        g_dbInfos[0],
                        tableRecordInfo.tableRecord.stable,
                        fp);
                records = dumpNormalTableBelongStb(
                        taos,
                        g_dbInfos[0],
                        tableRecordInfo.tableRecord.stable,
                        g_args.arg_list[i]);
            } else {
                records = dumpNormalTableWithoutStb(taos, g_dbInfos[0], g_args.arg_list[i]);
            }

            if (records >= 0) {
                okPrint("table: %s dumped\n", g_args.arg_list[i]);
                g_totalDumpOutRows += records;
            }
        }
    }

    taos_close(taos);

    /* Close the handle and return */
    fclose(fp);
    taos_free_result(result);
    freeDbInfos();
    fprintf(stderr, "dump out rows: %" PRId64 "\n", g_totalDumpOutRows);
    return 0;

_exit_failure:
    fclose(fp);
    taos_close(taos);
    taos_free_result(result);
    freeDbInfos();
    errorPrint("dump out rows: %" PRId64 "\n", g_totalDumpOutRows);
    return -1;
}

int main(int argc, char *argv[]) {
    static char verType[32] = {0};
    sprintf(verType, "version: %s\n", version);
    argp_program_version = verType;

    int ret = 0;
    /* Parse our arguments; every option seen by parse_opt will be
       reflected in arguments. */
    if (argc > 1) {
//        parse_precision_first(argc, argv, &g_args);
        parse_timestamp(argc, argv, &g_args);
        parse_args(argc, argv, &g_args);
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

    printf("host: %s\n", g_args.host);
    printf("user: %s\n", g_args.user);
    printf("password: %s\n", g_args.password);
    printf("port: %u\n", g_args.port);
    printf("mysqlFlag: %d\n", g_args.mysqlFlag);
    printf("outpath: %s\n", g_args.outpath);
    printf("inpath: %s\n", g_args.inpath);
    printf("resultFile: %s\n", g_args.resultFile);
    printf("encode: %s\n", g_args.encode);
    printf("all_databases: %s\n", g_args.all_databases?"true":"false");
    printf("databases: %d\n", g_args.databases);
    printf("databasesSeq: %s\n", g_args.databasesSeq);
    printf("schemaonly: %s\n", g_args.schemaonly?"true":"false");
    printf("with_property: %s\n", g_args.with_property?"true":"false");
#ifdef AVRO_SUPPORT
    printf("avro format: %s\n", g_args.avro?"true":"false");
    printf("avro codec: %s\n", g_avro_codec[g_args.avro_codec]);
#endif
    printf("start_time: %" PRId64 "\n", g_args.start_time);
    printf("human readable start time: %s \n", g_args.humanStartTime);
    printf("end_time: %" PRId64 "\n", g_args.end_time);
    printf("human readable end time: %s \n", g_args.humanEndTime);
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
        if (g_args.databases || g_args.all_databases) {
            errorPrint("%s is an invalid input if database(s) be already specified.\n",
                    g_args.arg_list[i]);
            exit(EXIT_FAILURE);
        } else {
            printf("arg_list[%d]: %s\n", i, g_args.arg_list[i]);
        }
    }

    printf("==============================\n");
    if (checkParam(&g_args) < 0) {
        exit(EXIT_FAILURE);
    }

    g_fpOfResult = fopen(g_args.resultFile, "a");
    if (NULL == g_fpOfResult) {
        errorPrint("Failed to open %s for save result\n", g_args.resultFile);
        exit(-1);
    };

    fprintf(g_fpOfResult, "#############################################################################\n");
    fprintf(g_fpOfResult, "============================== arguments config =============================\n");

    fprintf(g_fpOfResult, "host: %s\n", g_args.host);
    fprintf(g_fpOfResult, "user: %s\n", g_args.user);
    fprintf(g_fpOfResult, "password: %s\n", g_args.password);
    fprintf(g_fpOfResult, "port: %u\n", g_args.port);
    fprintf(g_fpOfResult, "mysqlFlag: %d\n", g_args.mysqlFlag);
    fprintf(g_fpOfResult, "outpath: %s\n", g_args.outpath);
    fprintf(g_fpOfResult, "inpath: %s\n", g_args.inpath);
    fprintf(g_fpOfResult, "resultFile: %s\n", g_args.resultFile);
    fprintf(g_fpOfResult, "encode: %s\n", g_args.encode);
    fprintf(g_fpOfResult, "all_databases: %s\n", g_args.all_databases?"true":"false");
    fprintf(g_fpOfResult, "databases: %d\n", g_args.databases);
    fprintf(g_fpOfResult, "databasesSeq: %s\n", g_args.databasesSeq);
    fprintf(g_fpOfResult, "schemaonly: %s\n", g_args.schemaonly?"true":"false");
    fprintf(g_fpOfResult, "with_property: %s\n", g_args.with_property?"true":"false");
#ifdef AVRO_SUPPORT
    fprintf(g_fpOfResult, "avro format: %s\n", g_args.avro?"true":"false");
    fprintf(g_fpOfResult, "avro codec: %s\n", g_avro_codec[g_args.avro_codec]);
#endif
    fprintf(g_fpOfResult, "start_time: %" PRId64 "\n", g_args.start_time);
    fprintf(g_fpOfResult, "human readable start time: %s \n", g_args.humanStartTime);
    fprintf(g_fpOfResult, "end_time: %" PRId64 "\n", g_args.end_time);
    fprintf(g_fpOfResult, "human readable end time: %s \n", g_args.humanEndTime);
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

    g_numOfCores = (int32_t)sysconf(_SC_NPROCESSORS_ONLN);

    time_t tTime = time(NULL);
    struct tm tm = *localtime(&tTime);

    if (g_args.isDumpIn) {
        fprintf(g_fpOfResult, "============================== DUMP IN ============================== \n");
        fprintf(g_fpOfResult, "# DumpIn start time:                   %d-%02d-%02d %02d:%02d:%02d\n",
                tm.tm_year + 1900, tm.tm_mon + 1,
                tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);
        if (dumpIn() < 0) {
            errorPrint("%s\n", "dumpIn() failed!");
            ret = -1;
        }
    } else {
        fprintf(g_fpOfResult, "============================== DUMP OUT ============================== \n");
        fprintf(g_fpOfResult, "# DumpOut start time:                   %d-%02d-%02d %02d:%02d:%02d\n",
                tm.tm_year + 1900, tm.tm_mon + 1,
                tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);
        if (dumpOut() < 0) {
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

    if (g_tablesList) {
        free(g_tablesList);
    }

    return ret;
}
