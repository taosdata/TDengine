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


/*
   when in some thread query return error, thread don't exit, but return, otherwise coredump in other thread.
   */

#include <stdint.h>
#include <taos.h>
#include <taoserror.h>
#define _GNU_SOURCE
#define CURL_STATICLIB

#ifdef LINUX
#include <argp.h>
#include <inttypes.h>
#ifndef _ALPINE
#include <error.h>
#endif
#include <pthread.h>
#include <semaphore.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <wordexp.h>
#include <regex.h>
#else
#include <regex.h>
#include <stdio.h>
#endif

#include <assert.h>
#include <stdlib.h>
#include "cJSON.h"

#include "os.h"
#include "taos.h"
#include "taoserror.h"
#include "tutil.h"

#define REQ_EXTRA_BUF_LEN   1024
#define RESP_BUF_LEN        4096

extern char configDir[];

#define STR_INSERT_INTO     "INSERT INTO "

#define MAX_RECORDS_PER_REQ     32766

#define HEAD_BUFF_LEN       TSDB_MAX_COLUMNS*24  // 16*MAX_COLUMNS + (192+32)*2 + insert into ..

#define BUFFER_SIZE         TSDB_MAX_ALLOWED_SQL_LEN
#define COND_BUF_LEN        (BUFFER_SIZE - 30)
#define COL_BUFFER_LEN      ((TSDB_COL_NAME_LEN + 15) * TSDB_MAX_COLUMNS)

#define MAX_USERNAME_SIZE  64
#define MAX_HOSTNAME_SIZE  253      // https://man7.org/linux/man-pages/man7/hostname.7.html
#define MAX_TB_NAME_SIZE   64
#define MAX_DATA_SIZE      (16*TSDB_MAX_COLUMNS)+20     // max record len: 16*MAX_COLUMNS, timestamp string and ,('') need extra space
#define OPT_ABORT          1 /* –abort */
#define MAX_FILE_NAME_LEN  256              // max file name length on linux is 255.
#define MAX_PATH_LEN       4096

#define DEFAULT_START_TIME 1500000000000

#define MAX_PREPARED_RAND  1000000
#define INT_BUFF_LEN            12
#define BIGINT_BUFF_LEN         21
#define SMALLINT_BUFF_LEN       7
#define TINYINT_BUFF_LEN        5
#define BOOL_BUFF_LEN           6
#define FLOAT_BUFF_LEN          22
#define DOUBLE_BUFF_LEN         42
#define TIMESTAMP_BUFF_LEN      21

#define MAX_SAMPLES             10000
#define MAX_NUM_COLUMNS        (TSDB_MAX_COLUMNS - 1)      // exclude first column timestamp

#define MAX_DB_COUNT            8
#define MAX_SUPER_TABLE_COUNT   200

#define MAX_QUERY_SQL_COUNT     100

#define MAX_DATABASE_COUNT      256
#define INPUT_BUF_LEN           256

#define TBNAME_PREFIX_LEN       (TSDB_TABLE_NAME_LEN - 20) // 20 characters reserved for seq
#define SMALL_BUFF_LEN          8
#define DATATYPE_BUFF_LEN       (SMALL_BUFF_LEN*3)
#define NOTE_BUFF_LEN           (SMALL_BUFF_LEN*16)

#define DEFAULT_NTHREADS        8
#define DEFAULT_TIMESTAMP_STEP  1
#define DEFAULT_INTERLACE_ROWS  0
#define DEFAULT_DATATYPE_NUM    1
#define DEFAULT_CHILDTABLES     10000

#define STMT_BIND_PARAM_BATCH   1

char* g_sampleDataBuf = NULL;
#if STMT_BIND_PARAM_BATCH == 1
    // bind param batch
char* g_sampleBindBatchArray = NULL;
#endif

enum TEST_MODE {
    INSERT_TEST,            // 0
    QUERY_TEST,             // 1
    SUBSCRIBE_TEST,         // 2
    INVAID_TEST
};

typedef enum CREATE_SUB_TABLE_MOD_EN {
    PRE_CREATE_SUBTBL,
    AUTO_CREATE_SUBTBL,
    NO_CREATE_SUBTBL
} CREATE_SUB_TABLE_MOD_EN;

typedef enum TABLE_EXISTS_EN {
    TBL_NO_EXISTS,
    TBL_ALREADY_EXISTS,
    TBL_EXISTS_BUTT
} TABLE_EXISTS_EN;

enum enumSYNC_MODE {
    SYNC_MODE,
    ASYNC_MODE,
    MODE_BUT
};

enum enum_TAOS_INTERFACE {
    TAOSC_IFACE,
    REST_IFACE,
    STMT_IFACE,
    INTERFACE_BUT
};

typedef enum enumQUERY_CLASS {
    SPECIFIED_CLASS,
    STABLE_CLASS,
    CLASS_BUT
} QUERY_CLASS;

typedef enum enum_PROGRESSIVE_OR_INTERLACE {
    PROGRESSIVE_INSERT_MODE,
    INTERLACE_INSERT_MODE,
    INVALID_INSERT_MODE
} PROG_OR_INTERLACE_MODE;

typedef enum enumQUERY_TYPE {
    NO_INSERT_TYPE,
    INSERT_TYPE,
    QUERY_TYPE_BUT
} QUERY_TYPE;

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
enum _show_stables_index {
    TSDB_SHOW_STABLES_NAME_INDEX,
    TSDB_SHOW_STABLES_CREATED_TIME_INDEX,
    TSDB_SHOW_STABLES_COLUMNS_INDEX,
    TSDB_SHOW_STABLES_METRIC_INDEX,
    TSDB_SHOW_STABLES_UID_INDEX,
    TSDB_SHOW_STABLES_TID_INDEX,
    TSDB_SHOW_STABLES_VGID_INDEX,
    TSDB_MAX_SHOW_STABLES
};

enum _describe_table_index {
    TSDB_DESCRIBE_METRIC_FIELD_INDEX,
    TSDB_DESCRIBE_METRIC_TYPE_INDEX,
    TSDB_DESCRIBE_METRIC_LENGTH_INDEX,
    TSDB_DESCRIBE_METRIC_NOTE_INDEX,
    TSDB_MAX_DESCRIBE_METRIC
};

/* Used by main to communicate with parse_opt. */
static char *g_dupstr = NULL;

typedef struct SArguments_S {
    char     *metaFile;
    uint32_t test_mode;
    char     *host;
    uint16_t port;
    uint16_t iface;
    char *   user;
    char     password[SHELL_MAX_PASSWORD_LEN];
    char *   database;
    int      replica;
    char *   tb_prefix;
    char *   sqlFile;
    bool     use_metric;
    bool     drop_database;
    bool     aggr_func;
    bool     answer_yes;
    bool     debug_print;
    bool     verbose_print;
    bool     performance_print;
    char *   output_file;
    bool     async_mode;
    char     data_type[MAX_NUM_COLUMNS+1];
    char     *dataType[MAX_NUM_COLUMNS+1];
    uint32_t binwidth;
    uint32_t columnCount;
    uint64_t lenOfOneRow;
    uint32_t nthreads;
    uint64_t insert_interval;
    uint64_t timestamp_step;
    int64_t  query_times;
    int64_t  prepared_rand;
    uint32_t interlaceRows;
    uint32_t reqPerReq;                  // num_of_records_per_req
    uint64_t max_sql_len;
    int64_t  ntables;
    int64_t  insertRows;
    int      abort;
    uint32_t disorderRatio;               // 0: no disorder, >0: x%
    int      disorderRange;               // ms, us or ns. according to database precision
    uint32_t method_of_delete;
    uint64_t totalInsertRows;
    uint64_t totalAffectedRows;
    bool     demo_mode;                  // use default column name and semi-random data
} SArguments;

typedef struct SColumn_S {
    char        field[TSDB_COL_NAME_LEN];
    char        data_type;
    char        dataType[DATATYPE_BUFF_LEN];
    uint32_t    dataLen;
    char        note[NOTE_BUFF_LEN];
} StrColumn;

typedef struct SSuperTable_S {
    char         stbName[TSDB_TABLE_NAME_LEN];
    char         dataSource[SMALL_BUFF_LEN];  // rand_gen or sample
    char         childTblPrefix[TBNAME_PREFIX_LEN];
    uint16_t     childTblExists;
    int64_t      childTblCount;
    uint64_t     batchCreateTableNum;     // 0: no batch,  > 0: batch table number in one sql
    uint8_t      autoCreateTable;         // 0: create sub table, 1: auto create sub table
    uint16_t     iface;                   // 0: taosc, 1: rest, 2: stmt
    int64_t      childTblLimit;
    uint64_t     childTblOffset;

    //  int          multiThreadWriteOneTbl;  // 0: no, 1: yes
    uint32_t     interlaceRows;           //
    int          disorderRatio;           // 0: no disorder, >0: x%
    int          disorderRange;           // ms, us or ns. according to database precision
    uint64_t     maxSqlLen;               //

    uint64_t     insertInterval;          // insert interval, will override global insert interval
    int64_t      insertRows;
    int64_t      timeStampStep;
    char         startTimestamp[MAX_TB_NAME_SIZE];
    char         sampleFormat[SMALL_BUFF_LEN];  // csv, json
    char         sampleFile[MAX_FILE_NAME_LEN];
    char         tagsFile[MAX_FILE_NAME_LEN];

    uint32_t     columnCount;
    StrColumn    columns[TSDB_MAX_COLUMNS];
    uint32_t     tagCount;
    StrColumn    tags[TSDB_MAX_TAGS];

    char*        childTblName;
    char*        colsOfCreateChildTable;
    uint64_t     lenOfOneRow;
    uint64_t     lenOfTagOfOneRow;

    char*        sampleDataBuf;
    bool         useSampleTs;

    uint32_t     tagSource;    // 0: rand, 1: tag sample
    char*        tagDataBuf;
    uint32_t     tagSampleCount;
    uint32_t     tagUsePos;

#if STMT_BIND_PARAM_BATCH == 1
    // bind param batch
    char        *sampleBindBatchArray;
#endif
    // statistics
    uint64_t     totalInsertRows;
    uint64_t     totalAffectedRows;
} SSuperTable;

typedef struct {
    char     name[TSDB_DB_NAME_LEN];
    char     create_time[32];
    int64_t  ntables;
    int32_t  vgroups;
    int16_t  replica;
    int16_t  quorum;
    int16_t  days;
    char     keeplist[64];
    int32_t  cache; //MB
    int32_t  blocks;
    int32_t  minrows;
    int32_t  maxrows;
    int8_t   wallevel;
    int32_t  fsync;
    int8_t   comp;
    int8_t   cachelast;
    char     precision[SMALL_BUFF_LEN];   // time resolution
    int8_t   update;
    char     status[16];
} SDbInfo;

typedef struct SDbCfg_S {
    //  int       maxtablesPerVnode;
    uint32_t  minRows;        // 0 means default
    uint32_t  maxRows;        // 0 means default
    int       comp;
    int       walLevel;
    int       cacheLast;
    int       fsync;
    int       replica;
    int       update;
    int       keep;
    int       days;
    int       cache;
    int       blocks;
    int       quorum;
    char      precision[SMALL_BUFF_LEN];
} SDbCfg;

typedef struct SDataBase_S {
    char         dbName[TSDB_DB_NAME_LEN];
    bool         drop;  // 0: use exists, 1: if exists, drop then new create
    SDbCfg       dbCfg;
    uint64_t     superTblCount;
    SSuperTable*  superTbls;
} SDataBase;

typedef struct SDbs_S {
    char        cfgDir[MAX_FILE_NAME_LEN];
    char        host[MAX_HOSTNAME_SIZE];
    struct      sockaddr_in serv_addr;

    uint16_t    port;
    char        user[MAX_USERNAME_SIZE];
    char        password[SHELL_MAX_PASSWORD_LEN];
    char        resultFile[MAX_FILE_NAME_LEN];
    bool        use_metric;
    bool        aggr_func;
    bool        asyncMode;

    uint32_t    threadCount;
    uint32_t    threadCountForCreateTbl;
    uint32_t    dbCount;
    // statistics
    uint64_t    totalInsertRows;
    uint64_t    totalAffectedRows;

    SDataBase*  db;
} SDbs;

typedef struct SpecifiedQueryInfo_S {
    uint64_t     queryInterval;  // 0: unlimited  > 0   loop/s
    uint32_t     concurrent;
    int          sqlCount;
    uint32_t     asyncMode; // 0: sync, 1: async
    uint64_t     subscribeInterval; // ms
    uint64_t     queryTimes;
    bool         subscribeRestart;
    int          subscribeKeepProgress;
    char         sql[MAX_QUERY_SQL_COUNT][BUFFER_SIZE+1];
    char         result[MAX_QUERY_SQL_COUNT][MAX_FILE_NAME_LEN];
    int          resubAfterConsume[MAX_QUERY_SQL_COUNT];
    int          endAfterConsume[MAX_QUERY_SQL_COUNT];
    TAOS_SUB*    tsub[MAX_QUERY_SQL_COUNT];
    char         topic[MAX_QUERY_SQL_COUNT][32];
    int          consumed[MAX_QUERY_SQL_COUNT];
    TAOS_RES*    res[MAX_QUERY_SQL_COUNT];
    uint64_t     totalQueried;
} SpecifiedQueryInfo;

typedef struct SuperQueryInfo_S {
    char         stbName[TSDB_TABLE_NAME_LEN];
    uint64_t     queryInterval;  // 0: unlimited  > 0   loop/s
    uint32_t     threadCnt;
    uint32_t     asyncMode; // 0: sync, 1: async
    uint64_t     subscribeInterval; // ms
    bool         subscribeRestart;
    int          subscribeKeepProgress;
    uint64_t     queryTimes;
    int64_t      childTblCount;
    char         childTblPrefix[TBNAME_PREFIX_LEN];    // 20 characters reserved for seq
    int          sqlCount;
    char         sql[MAX_QUERY_SQL_COUNT][BUFFER_SIZE+1];
    char         result[MAX_QUERY_SQL_COUNT][MAX_FILE_NAME_LEN];
    int          resubAfterConsume;
    int          endAfterConsume;
    TAOS_SUB*    tsub[MAX_QUERY_SQL_COUNT];

    char*        childTblName;
    uint64_t     totalQueried;
} SuperQueryInfo;

typedef struct SQueryMetaInfo_S {
    char         cfgDir[MAX_FILE_NAME_LEN];
    char         host[MAX_HOSTNAME_SIZE];
    uint16_t     port;
    struct       sockaddr_in serv_addr;
    char         user[MAX_USERNAME_SIZE];
    char         password[SHELL_MAX_PASSWORD_LEN];
    char         dbName[TSDB_DB_NAME_LEN];
    char         queryMode[SMALL_BUFF_LEN];  // taosc, rest

    SpecifiedQueryInfo  specifiedQueryInfo;
    SuperQueryInfo      superQueryInfo;
    uint64_t     totalQueried;
} SQueryMetaInfo;

typedef struct SThreadInfo_S {
    TAOS *    taos;
    TAOS_STMT *stmt;
    int64_t     *bind_ts;

#if STMT_BIND_PARAM_BATCH == 1
    int64_t     *bind_ts_array;
    char        *bindParams;
    char        *is_null;
#else
    char*       sampleBindArray;
#endif

    int       threadID;
    char      db_name[TSDB_DB_NAME_LEN];
    uint32_t  time_precision;
    char      filePath[4096];
    FILE      *fp;
    char      tb_prefix[TSDB_TABLE_NAME_LEN];
    uint64_t  start_table_from;
    uint64_t  end_table_to;
    int64_t   ntables;
    int64_t   tables_created;
    uint64_t  data_of_rate;
    int64_t   start_time;
    char*     cols;
    bool      use_metric;
    SSuperTable* stbInfo;
    char      *buffer;    // sql cmd buffer

    // for async insert
    tsem_t    lock_sem;
    int64_t   counter;
    uint64_t  st;
    uint64_t  et;
    uint64_t  lastTs;

    // sample data
    int64_t   samplePos;
    // statistics
    uint64_t  totalInsertRows;
    uint64_t  totalAffectedRows;

    // insert delay statistics
    uint64_t  cntDelay;
    uint64_t  totalDelay;
    uint64_t  avgDelay;
    uint64_t  maxDelay;
    uint64_t  minDelay;

    // seq of query or subscribe
    uint64_t  querySeq;   // sequence number of sql command
    TAOS_SUB*  tsub;

    int       sockfd;
} threadInfo;

#ifdef WINDOWS
#define _CRT_RAND_S

#include <windows.h>
#include <winsock2.h>

typedef unsigned __int32 uint32_t;

#pragma comment ( lib, "ws2_32.lib" )
// Some old MinGW/CYGWIN distributions don't define this:
#ifndef ENABLE_VIRTUAL_TERMINAL_PROCESSING
#define ENABLE_VIRTUAL_TERMINAL_PROCESSING  0x0004
#endif // ENABLE_VIRTUAL_TERMINAL_PROCESSING

static HANDLE g_stdoutHandle;
static DWORD g_consoleMode;

static void setupForAnsiEscape(void) {
    DWORD mode = 0;
    g_stdoutHandle = GetStdHandle(STD_OUTPUT_HANDLE);

    if(g_stdoutHandle == INVALID_HANDLE_VALUE) {
        exit(GetLastError());
    }

    if(!GetConsoleMode(g_stdoutHandle, &mode)) {
        exit(GetLastError());
    }

    g_consoleMode = mode;

    // Enable ANSI escape codes
    mode |= ENABLE_VIRTUAL_TERMINAL_PROCESSING;

    if(!SetConsoleMode(g_stdoutHandle, mode)) {
        exit(GetLastError());
    }
}

static void resetAfterAnsiEscape(void) {
    // Reset colors
    printf("\x1b[0m");

    // Reset console mode
    if(!SetConsoleMode(g_stdoutHandle, g_consoleMode)) {
        exit(GetLastError());
    }
}

static int taosRandom()
{
    int number;
    rand_s(&number);

    return number;
}
#else   // Not windows
static void setupForAnsiEscape(void) {}

static void resetAfterAnsiEscape(void) {
    // Reset colors
    printf("\x1b[0m");
}

#include <time.h>

static int taosRandom()
{
    return rand();
}

#endif // ifdef Windows

static void prompt();
static int createDatabasesAndStables();
static void createChildTables();
static int queryDbExec(TAOS *taos, char *command, QUERY_TYPE type, bool quiet);
static int postProceSql(char *host, uint16_t port, char* sqlstr, threadInfo *pThreadInfo);
static int64_t getTSRandTail(int64_t timeStampStep, int32_t seq,
        int disorderRatio, int disorderRange);
static bool getInfoFromJsonFile(char* file);
static void init_rand_data();
static int regexMatch(const char *s, const char *reg, int cflags);

/* ************ Global variables ************  */

int32_t*  g_randint;
uint32_t*  g_randuint;
int64_t*  g_randbigint;
uint64_t*  g_randubigint;
float*    g_randfloat;
double*   g_randdouble;

char    *g_randbool_buff = NULL;
char    *g_randint_buff = NULL;
char    *g_randuint_buff = NULL;
char    *g_rand_voltage_buff = NULL;
char    *g_randbigint_buff = NULL;
char    *g_randubigint_buff = NULL;
char    *g_randsmallint_buff = NULL;
char    *g_randusmallint_buff = NULL;
char    *g_randtinyint_buff = NULL;
char    *g_randutinyint_buff = NULL;
char    *g_randfloat_buff = NULL;
char    *g_rand_current_buff = NULL;
char    *g_rand_phase_buff = NULL;
char    *g_randdouble_buff = NULL;

char    *g_aggreFuncDemo[] = {"*", "count(*)", "avg(current)", "sum(current)",
    "max(current)", "min(current)", "first(current)", "last(current)"};

char    *g_aggreFunc[] = {"*", "count(*)", "avg(C0)", "sum(C0)",
    "max(C0)", "min(C0)", "first(C0)", "last(C0)"};

SArguments g_args = {
    NULL,           // metaFile
    0,              // test_mode
    "localhost",    // host
    6030,           // port
    INTERFACE_BUT,  // iface
    "root",         // user
    "taosdata",     // password
    "test",         // database
    1,              // replica
    "d",             // tb_prefix
    NULL,            // sqlFile
    true,            // use_metric
    true,            // drop_database
    false,           // aggr_func
    false,           // debug_print
    false,           // verbose_print
    false,           // performance statistic print
    false,           // answer_yes;
    "./output.txt",  // output_file
    0,               // mode : sync or async
    {TSDB_DATA_TYPE_FLOAT,
    TSDB_DATA_TYPE_INT,
    TSDB_DATA_TYPE_FLOAT},
    {
        "FLOAT",         // dataType
        "INT",           // dataType
        "FLOAT",         // dataType. demo mode has 3 columns
    },
    64,              // binwidth
    4,               // columnCount, timestamp + float + int + float
    20 + FLOAT_BUFF_LEN + INT_BUFF_LEN + FLOAT_BUFF_LEN, // lenOfOneRow
    DEFAULT_NTHREADS,// nthreads
    0,               // insert_interval
    DEFAULT_TIMESTAMP_STEP, // timestamp_step
    1,               // query_times
    10000,           // prepared_rand
    DEFAULT_INTERLACE_ROWS, // interlaceRows;
    30000,           // reqPerReq
    (1024*1024),     // max_sql_len
    DEFAULT_CHILDTABLES,    // ntables
    10000,           // insertRows
    0,               // abort
    0,               // disorderRatio
    1000,            // disorderRange
    1,               // method_of_delete
    0,               // totalInsertRows;
    0,               // totalAffectedRows;
    true,            // demo_mode;
};

static SDbs            g_Dbs;
static int64_t         g_totalChildTables = DEFAULT_CHILDTABLES;
static int64_t         g_actualChildTables = 0;
static SQueryMetaInfo  g_queryInfo;
static FILE *          g_fpOfInsertResult = NULL;

#if _MSC_VER <= 1900
#define __func__ __FUNCTION__
#endif

#define debugPrint(fmt, ...) \
    do { if (g_args.debug_print || g_args.verbose_print) \
        fprintf(stderr, "DEBG: "fmt, __VA_ARGS__); } while(0)

#define verbosePrint(fmt, ...) \
    do { if (g_args.verbose_print) \
        fprintf(stderr, "VERB: "fmt, __VA_ARGS__); } while(0)

#define performancePrint(fmt, ...) \
    do { if (g_args.performance_print) \
        fprintf(stderr, "PERF: "fmt, __VA_ARGS__); } while(0)

#define errorPrint(fmt, ...) \
    do {\
        fprintf(stderr, " \033[31m");\
        fprintf(stderr, "ERROR: "fmt, __VA_ARGS__);\
        fprintf(stderr, " \033[0m");\
    } while(0)

#define errorPrint2(fmt, ...) \
    do {\
        struct tm      Tm, *ptm;\
        struct timeval timeSecs; \
        time_t         curTime;\
        gettimeofday(&timeSecs, NULL); \
        curTime = timeSecs.tv_sec;\
        ptm = localtime_r(&curTime, &Tm);\
        fprintf(stderr, " \033[31m");\
        fprintf(stderr, "%02d/%02d %02d:%02d:%02d.%06d %08" PRId64 " ",\
                ptm->tm_mon + 1, ptm->tm_mday, ptm->tm_hour,\
                ptm->tm_min, ptm->tm_sec, (int32_t)timeSecs.tv_usec,\
                taosGetSelfPthreadId());\
        fprintf(stderr, " \033[0m");\
        errorPrint(fmt, __VA_ARGS__);\
    } while(0)

// for strncpy buffer overflow
#define min(a, b) (((a) < (b)) ? (a) : (b))


///////////////////////////////////////////////////

static void ERROR_EXIT(const char *msg) { errorPrint("%s", msg); exit(-1); }

#ifndef TAOSDEMO_COMMIT_SHA1
#define TAOSDEMO_COMMIT_SHA1 "unknown"
#endif

#ifndef TD_VERNUMBER
#define TD_VERNUMBER    "unknown"
#endif

#ifndef TAOSDEMO_STATUS
#define TAOSDEMO_STATUS "unknown"
#endif

static void printVersion() {
    char tdengine_ver[] = TD_VERNUMBER;
    char taosdemo_ver[] = TAOSDEMO_COMMIT_SHA1;
    char taosdemo_status[] = TAOSDEMO_STATUS;

    if (strlen(taosdemo_status) == 0) {
        printf("taosdemo version %s-%s\n",
                tdengine_ver, taosdemo_ver);
    } else {
        printf("taosdemo version %s-%s, status:%s\n",
                tdengine_ver, taosdemo_ver, taosdemo_status);
    }
}

static void printHelp() {
    char indent[10] = "  ";
    printf("%s\n\n", "Usage: taosdemo [OPTION...]");
    printf("%s%s%s%s\n", indent, "-f, --file=FILE", "\t\t",
            "The meta file to the execution procedure.");
    printf("%s%s%s%s\n", indent, "-u, --user=USER", "\t\t",
            "The user name to use when connecting to the server.");
    printf("%s%s%s%s\n", indent, "-p, --password", "\t\t",
            "The password to use when connecting to the server.");
    printf("%s%s%s%s\n", indent, "-c, --config-dir=CONFIG_DIR", "\t",
            "Configuration directory.");
    printf("%s%s%s%s\n", indent, "-h, --host=HOST", "\t\t",
            "Server FQDN to connect. The default host is localhost.");
    printf("%s%s%s%s\n", indent, "-P, --port=PORT", "\t\t",
            "The TCP/IP port number to use for the connection.");
    printf("%s%s%s%s\n", indent, "-I, --interface=INTERFACE", "\t",
            "The interface (taosc, rest, and stmt) taosdemo uses. By default use 'taosc'.");
    printf("%s%s%s%s\n", indent, "-d, --database=DATABASE", "\t",
            "Destination database. By default is 'test'.");
    printf("%s%s%s%s\n", indent, "-a, --replica=REPLICA", "\t\t",
            "Set the replica parameters of the database, By default use 1, min: 1, max: 3.");
    printf("%s%s%s%s\n", indent, "-m, --table-prefix=TABLEPREFIX", "\t",
            "Table prefix name. By default use 'd'.");
    printf("%s%s%s%s\n", indent, "-E, --escape-character", "\t",
            "Use escape character for Both Stable and normmal table name");
    printf("%s%s%s%s\n", indent, "-s, --sql-file=FILE", "\t\t",
            "The select sql file.");
    printf("%s%s%s%s\n", indent, "-N, --normal-table", "\t\t", "Use normal table flag.");
    printf("%s%s%s%s\n", indent, "-o, --output=FILE", "\t\t",
            "Direct output to the named file. By default use './output.txt'.");
    printf("%s%s%s%s\n", indent, "-q, --query-mode=MODE", "\t\t",
            "Query mode -- 0: SYNC, 1: ASYNC. By default use SYNC.");
    printf("%s%s%s%s\n", indent, "-b, --data-type=DATATYPE", "\t",
            "The data_type of columns, By default use: FLOAT,INT,FLOAT. NCHAR and BINARY can also use custom length. Eg: NCHAR(16),BINARY(8)");
    printf("%s%s%s%s%d\n", indent, "-w, --binwidth=WIDTH", "\t\t",
            "The width of data_type 'BINARY' or 'NCHAR'. By default use ",
            g_args.binwidth);
    printf("%s%s%s%s%d%s%d\n", indent, "-l, --columns=COLUMNS", "\t\t",
            "The number of columns per record. Demo mode by default is ",
            DEFAULT_DATATYPE_NUM,
            " (float, int, float). Max values is ",
            MAX_NUM_COLUMNS);
    printf("%s%s%s%s\n", indent, indent, indent,
            "\t\t\t\tAll of the new column(s) type is INT. If use -b to specify column type, -l will be ignored.");
    printf("%s%s%s%s%d.\n", indent, "-T, --threads=NUMBER", "\t\t",
            "The number of threads. By default use ", DEFAULT_NTHREADS);
    printf("%s%s%s%s\n", indent, "-i, --insert-interval=NUMBER", "\t",
            "The sleep time (ms) between insertion. By default is 0.");
    printf("%s%s%s%s%d.\n", indent, "-S, --time-step=TIME_STEP", "\t",
            "The timestamp step between insertion. By default is ",
            DEFAULT_TIMESTAMP_STEP);
    printf("%s%s%s%s%d.\n", indent, "-B, --interlace-rows=NUMBER", "\t",
            "The interlace rows of insertion. By default is ",
            DEFAULT_INTERLACE_ROWS);
    printf("%s%s%s%s\n", indent, "-r, --rec-per-req=NUMBER", "\t",
            "The number of records per request. By default is 30000.");
    printf("%s%s%s%s\n", indent, "-t, --tables=NUMBER", "\t\t",
            "The number of tables. By default is 10000.");
    printf("%s%s%s%s\n", indent, "-n, --records=NUMBER", "\t\t",
            "The number of records per table. By default is 10000.");
    printf("%s%s%s%s\n", indent, "-M, --random", "\t\t\t",
            "The value of records generated are totally random.");
    printf("%s\n", "\t\t\t\tBy default to simulate power equipment scenario.");
    printf("%s%s%s%s\n", indent, "-x, --aggr-func", "\t\t",
            "Test aggregation functions after insertion.");
    printf("%s%s%s%s\n", indent, "-y, --answer-yes", "\t\t", "Input yes for prompt.");
    printf("%s%s%s%s\n", indent, "-O, --disorder=NUMBER", "\t\t",
            "Insert order mode--0: In order, 1 ~ 50: disorder ratio. By default is in order.");
    printf("%s%s%s%s\n", indent, "-R, --disorder-range=NUMBER", "\t",
            "Out of order data's range. Unit is ms. By default is 1000.");
    printf("%s%s%s%s\n", indent, "-g, --debug", "\t\t\t",
            "Print debug info.");
    printf("%s%s%s%s\n", indent, "-?, --help\t", "\t\t",
            "Give this help list");
    printf("%s%s%s%s\n", indent, "    --usage\t", "\t\t",
            "Give a short usage message");
    printf("%s%s\n", indent, "-V, --version\t\t\tPrint program version.");
    /*    printf("%s%s%s%s\n", indent, "-D", indent,
          "Delete database if exists. 0: no, 1: yes, default is 1");
          */
    printf("\nMandatory or optional arguments to long options are also mandatory or optional\n\
for any corresponding short options.\n\
\n\
Report bugs to <support@taosdata.com>.\n");
}

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

static void errorWrongValue(char *program, char *wrong_arg, char *wrong_value)
{
    fprintf(stderr, "%s %s: %s is an invalid value\n", program, wrong_arg, wrong_value);
    fprintf(stderr, "Try `taosdemo --help' or `taosdemo --usage' for more information.\n");
}

static void errorUnrecognized(char *program, char *wrong_arg)
{
    fprintf(stderr, "%s: unrecognized options '%s'\n", program, wrong_arg);
    fprintf(stderr, "Try `taosdemo --help' or `taosdemo --usage' for more information.\n");
}

static void errorPrintReqArg(char *program, char *wrong_arg)
{
    fprintf(stderr,
            "%s: option requires an argument -- '%s'\n",
            program, wrong_arg);
    fprintf(stderr,
            "Try `taosdemo --help' or `taosdemo --usage' for more information.\n");
}

static void errorPrintReqArg2(char *program, char *wrong_arg)
{
    fprintf(stderr,
            "%s: option requires a number argument '-%s'\n",
            program, wrong_arg);
    fprintf(stderr,
            "Try `taosdemo --help' or `taosdemo --usage' for more information.\n");
}

static void errorPrintReqArg3(char *program, char *wrong_arg)
{
    fprintf(stderr,
            "%s: option '%s' requires an argument\n",
            program, wrong_arg);
    fprintf(stderr,
            "Try `taosdemo --help' or `taosdemo --usage' for more information.\n");
}

static void parse_args(int argc, char *argv[], SArguments *arguments) {

    for (int i = 1; i < argc; i++) {
        if ((0 == strncmp(argv[i], "-f", strlen("-f")))
                || (0 == strncmp(argv[i], "--file", strlen("--file")))) {
            arguments->demo_mode = false;

            if (2 == strlen(argv[i])) {
                if (i+1 == argc) {
                    errorPrintReqArg(argv[0], "f");
                    exit(EXIT_FAILURE);
                }
                arguments->metaFile = argv[++i];
            } else if (0 == strncmp(argv[i], "-f", strlen("-f"))) {
                arguments->metaFile = (char *)(argv[i] + strlen("-f"));
            } else if (strlen("--file") == strlen(argv[i])) {
                if (i+1 == argc) {
                    errorPrintReqArg3(argv[0], "--file");
                    exit(EXIT_FAILURE);
                }
                arguments->metaFile = argv[++i];
            } else if (0 == strncmp(argv[i], "--file=", strlen("--file="))) {
                arguments->metaFile = (char *)(argv[i] + strlen("--file="));
            } else {
                errorUnrecognized(argv[0], argv[i]);
                exit(EXIT_FAILURE);
            }
        } else if ((0 == strncmp(argv[i], "-c", strlen("-c")))
                || (0 == strncmp(argv[i], "--config-dir", strlen("--config-dir")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg(argv[0], "c");
                    exit(EXIT_FAILURE);
                }
                tstrncpy(configDir, argv[++i], TSDB_FILENAME_LEN);
            } else if (0 == strncmp(argv[i], "-c", strlen("-c"))) {
                tstrncpy(configDir, (char *)(argv[i] + strlen("-c")), TSDB_FILENAME_LEN);
            } else if (strlen("--config-dir") == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg3(argv[0], "--config-dir");
                    exit(EXIT_FAILURE);
                }
                tstrncpy(configDir, argv[++i], TSDB_FILENAME_LEN);
            } else if (0 == strncmp(argv[i], "--config-dir=", strlen("--config-dir="))) {
                tstrncpy(configDir, (char *)(argv[i] + strlen("--config-dir=")), TSDB_FILENAME_LEN);
            } else {
                errorUnrecognized(argv[0], argv[i]);
                exit(EXIT_FAILURE);
            }
        } else if ((0 == strncmp(argv[i], "-h", strlen("-h")))
                || (0 == strncmp(argv[i], "--host", strlen("--host")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg(argv[0], "h");
                    exit(EXIT_FAILURE);
                }
                arguments->host = argv[++i];
            } else if (0 == strncmp(argv[i], "-h", strlen("-h"))) {
                arguments->host = (char *)(argv[i] + strlen("-h"));
            } else if (strlen("--host") == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg3(argv[0], "--host");
                    exit(EXIT_FAILURE);
                }
                arguments->host = argv[++i];
            } else if (0 == strncmp(argv[i], "--host=", strlen("--host="))) {
                arguments->host = (char *)(argv[i] + strlen("--host="));
            } else {
                errorUnrecognized(argv[0], argv[i]);
                exit(EXIT_FAILURE);
            }
        } else if (strcmp(argv[i], "-PP") == 0) {
            arguments->performance_print = true;
        } else if ((0 == strncmp(argv[i], "-P", strlen("-P")))
                || (0 == strncmp(argv[i], "--port", strlen("--port")))) {
            uint64_t port;
            char strPort[BIGINT_BUFF_LEN];

            if (2 == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg(argv[0], "P");
                    exit(EXIT_FAILURE);
                } else if (isStringNumber(argv[i+1])) {
                    tstrncpy(strPort, argv[++i], BIGINT_BUFF_LEN);
                } else {
                    errorPrintReqArg2(argv[0], "P");
                    exit(EXIT_FAILURE);
                }
            } else if (0 == strncmp(argv[i], "--port=", strlen("--port="))) {
                if (isStringNumber((char *)(argv[i] + strlen("--port=")))) {
                    tstrncpy(strPort, (char *)(argv[i]+strlen("--port=")), BIGINT_BUFF_LEN);
                } else {
                    errorPrintReqArg2(argv[0], "--port");
                    exit(EXIT_FAILURE);
                }
            } else if (0 == strncmp(argv[i], "-P", strlen("-P"))) {
                if (isStringNumber((char *)(argv[i] + strlen("-P")))) {
                    tstrncpy(strPort, (char *)(argv[i]+strlen("-P")), BIGINT_BUFF_LEN);
                } else {
                    errorPrintReqArg2(argv[0], "--port");
                    exit(EXIT_FAILURE);
                }
            } else if (strlen("--port") == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg3(argv[0], "--port");
                    exit(EXIT_FAILURE);
                } else if (isStringNumber(argv[i+1])) {
                    tstrncpy(strPort, argv[++i], BIGINT_BUFF_LEN);
                } else {
                    errorPrintReqArg2(argv[0], "--port");
                    exit(EXIT_FAILURE);
                }
            } else {
                errorUnrecognized(argv[0], argv[i]);
                exit(EXIT_FAILURE);
            }

            port = atoi(strPort);
            if (port > 65535) {
                errorWrongValue("taosdump", "-P or --port", strPort);
                exit(EXIT_FAILURE);
            }
            arguments->port = (uint16_t)port;

        } else if ((0 == strncmp(argv[i], "-I", strlen("-I")))
                || (0 == strncmp(argv[i], "--interface", strlen("--interface")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg(argv[0], "I");
                    exit(EXIT_FAILURE);
                }
                if (0 == strcasecmp(argv[i+1], "taosc")) {
                    arguments->iface = TAOSC_IFACE;
                } else if (0 == strcasecmp(argv[i+1], "rest")) {
                    arguments->iface = REST_IFACE;
                } else if (0 == strcasecmp(argv[i+1], "stmt")) {
                    arguments->iface = STMT_IFACE;
                } else {
                    errorWrongValue(argv[0], "-I", argv[i+1]);
                    exit(EXIT_FAILURE);
                }
                i++;
            } else if (0 == strncmp(argv[i], "--interface=", strlen("--interface="))) {
                if (0 == strcasecmp((char *)(argv[i] + strlen("--interface=")), "taosc")) {
                    arguments->iface = TAOSC_IFACE;
                } else if (0 == strcasecmp((char *)(argv[i] + strlen("--interface=")), "rest")) {
                    arguments->iface = REST_IFACE;
                } else if (0 == strcasecmp((char *)(argv[i] + strlen("--interface=")), "stmt")) {
                    arguments->iface = STMT_IFACE;
                } else {
                    errorPrintReqArg3(argv[0], "--interface");
                    exit(EXIT_FAILURE);
                }
            } else if (0 == strncmp(argv[i], "-I", strlen("-I"))) {
                if (0 == strcasecmp((char *)(argv[i] + strlen("-I")), "taosc")) {
                    arguments->iface = TAOSC_IFACE;
                } else if (0 == strcasecmp((char *)(argv[i] + strlen("-I")), "rest")) {
                    arguments->iface = REST_IFACE;
                } else if (0 == strcasecmp((char *)(argv[i] + strlen("-I")), "stmt")) {
                    arguments->iface = STMT_IFACE;
                } else {
                    errorWrongValue(argv[0], "-I",
                            (char *)(argv[i] + strlen("-I")));
                    exit(EXIT_FAILURE);
                }
            } else if (strlen("--interface") == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg3(argv[0], "--interface");
                    exit(EXIT_FAILURE);
                }
                if (0 == strcasecmp(argv[i+1], "taosc")) {
                    arguments->iface = TAOSC_IFACE;
                } else if (0 == strcasecmp(argv[i+1], "rest")) {
                    arguments->iface = REST_IFACE;
                } else if (0 == strcasecmp(argv[i+1], "stmt")) {
                    arguments->iface = STMT_IFACE;
                } else {
                    errorWrongValue(argv[0], "--interface", argv[i+1]);
                    exit(EXIT_FAILURE);
                }
                i++;
            } else {
                errorUnrecognized(argv[0], argv[i]);
                exit(EXIT_FAILURE);
            }
        } else if ((0 == strncmp(argv[i], "-u", strlen("-u")))
                || (0 == strncmp(argv[i], "--user", strlen("--user")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg(argv[0], "u");
                    exit(EXIT_FAILURE);
                }
                arguments->user = argv[++i];
            } else if (0 == strncmp(argv[i], "-u", strlen("-u"))) {
                arguments->user = (char *)(argv[i++] + strlen("-u"));
            } else if (0 == strncmp(argv[i], "--user=", strlen("--user="))) {
                arguments->user = (char *)(argv[i++] + strlen("--user="));
            } else if (strlen("--user") == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg3(argv[0], "--user");
                    exit(EXIT_FAILURE);
                }
                arguments->user = argv[++i];
            } else {
                errorUnrecognized(argv[0], argv[i]);
                exit(EXIT_FAILURE);
            }
        } else if ((0 == strncmp(argv[i], "-p", strlen("-p")))
                || (0 == strcmp(argv[i], "--password"))) {
            if ((strlen(argv[i]) == 2) || (0 == strcmp(argv[i], "--password"))) {
                printf("Enter password: ");
                taosSetConsoleEcho(false);
                if (scanf("%s", arguments->password) > 1) {
                    fprintf(stderr, "password read error!\n");
                }
                taosSetConsoleEcho(true);
            } else {
                tstrncpy(arguments->password, (char *)(argv[i] + 2), SHELL_MAX_PASSWORD_LEN);
            }
        } else if ((0 == strncmp(argv[i], "-o", strlen("-o")))
                || (0 == strncmp(argv[i], "--output", strlen("--output")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg3(argv[0], "--output");
                    exit(EXIT_FAILURE);
                }
                arguments->output_file = argv[++i];
            } else if (0 == strncmp(argv[i], "--output=", strlen("--output="))) {
                arguments->output_file = (char *)(argv[i++] + strlen("--output="));
            } else if (0 == strncmp(argv[i], "-o", strlen("-o"))) {
                arguments->output_file = (char *)(argv[i++] + strlen("-o"));
            } else if (strlen("--output") == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg3(argv[0], "--output");
                    exit(EXIT_FAILURE);
                }
                arguments->output_file = argv[++i];
            } else {
                errorUnrecognized(argv[0], argv[i]);
                exit(EXIT_FAILURE);
            }
        } else if ((0 == strncmp(argv[i], "-s", strlen("-s")))
                || (0 == strncmp(argv[i], "--sql-file", strlen("--sql-file")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg(argv[0], "s");
                    exit(EXIT_FAILURE);
                }
                arguments->sqlFile = argv[++i];
            } else if (0 == strncmp(argv[i], "--sql-file=", strlen("--sql-file="))) {
                arguments->sqlFile = (char *)(argv[i++] + strlen("--sql-file="));
            } else if (0 == strncmp(argv[i], "-s", strlen("-s"))) {
                arguments->sqlFile = (char *)(argv[i++] + strlen("-s"));
            } else if (strlen("--sql-file") == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg3(argv[0], "--sql-file");
                    exit(EXIT_FAILURE);
                }
                arguments->sqlFile = argv[++i];
            } else {
                errorUnrecognized(argv[0], argv[i]);
                exit(EXIT_FAILURE);
            }
        } else if ((0 == strncmp(argv[i], "-q", strlen("-q")))
                || (0 == strncmp(argv[i], "--query-mode", strlen("--query-mode")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg(argv[0], "q");
                    exit(EXIT_FAILURE);
                } else if (!isStringNumber(argv[i+1])) {
                    errorPrintReqArg2(argv[0], "q");
                    exit(EXIT_FAILURE);
                }
                arguments->async_mode = atoi(argv[++i]);
            } else if (0 == strncmp(argv[i], "--query-mode=", strlen("--query-mode="))) {
                if (isStringNumber((char *)(argv[i] + strlen("--query-mode=")))) {
                    arguments->async_mode = atoi((char *)(argv[i]+strlen("--query-mode=")));
                } else {
                    errorPrintReqArg2(argv[0], "--query-mode");
                    exit(EXIT_FAILURE);
                }
            } else if (0 == strncmp(argv[i], "-q", strlen("-q"))) {
                if (isStringNumber((char *)(argv[i] + strlen("-q")))) {
                    arguments->async_mode = atoi((char *)(argv[i]+strlen("-q")));
                } else {
                    errorPrintReqArg2(argv[0], "-q");
                    exit(EXIT_FAILURE);
                }
            } else if (strlen("--query-mode") == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg3(argv[0], "--query-mode");
                    exit(EXIT_FAILURE);
                } else if (!isStringNumber(argv[i+1])) {
                    errorPrintReqArg2(argv[0], "--query-mode");
                    exit(EXIT_FAILURE);
                }
                arguments->async_mode = atoi(argv[++i]);
            } else {
                errorUnrecognized(argv[0], argv[i]);
                exit(EXIT_FAILURE);
            }
        } else if ((0 == strncmp(argv[i], "-T", strlen("-T")))
                || (0 == strncmp(argv[i], "--threads", strlen("--threads")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg(argv[0], "T");
                    exit(EXIT_FAILURE);
                } else if (!isStringNumber(argv[i+1])) {
                    errorPrintReqArg2(argv[0], "T");
                    exit(EXIT_FAILURE);
                }
                arguments->nthreads = atoi(argv[++i]);
            } else if (0 == strncmp(argv[i], "--threads=", strlen("--threads="))) {
                if (isStringNumber((char *)(argv[i] + strlen("--threads=")))) {
                    arguments->nthreads = atoi((char *)(argv[i]+strlen("--threads=")));
                } else {
                    errorPrintReqArg2(argv[0], "--threads");
                    exit(EXIT_FAILURE);
                }
            } else if (0 == strncmp(argv[i], "-T", strlen("-T"))) {
                if (isStringNumber((char *)(argv[i] + strlen("-T")))) {
                    arguments->nthreads = atoi((char *)(argv[i]+strlen("-T")));
                } else {
                    errorPrintReqArg2(argv[0], "-T");
                    exit(EXIT_FAILURE);
                }
            } else if (strlen("--threads") == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg3(argv[0], "--threads");
                    exit(EXIT_FAILURE);
                } else if (!isStringNumber(argv[i+1])) {
                    errorPrintReqArg2(argv[0], "--threads");
                    exit(EXIT_FAILURE);
                }
                arguments->nthreads = atoi(argv[++i]);
            } else {
                errorUnrecognized(argv[0], argv[i]);
                exit(EXIT_FAILURE);
            }
        } else if ((0 == strncmp(argv[i], "-i", strlen("-i")))
                || (0 == strncmp(argv[i], "--insert-interval", strlen("--insert-interval")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg(argv[0], "i");
                    exit(EXIT_FAILURE);
                } else if (!isStringNumber(argv[i+1])) {
                    errorPrintReqArg2(argv[0], "i");
                    exit(EXIT_FAILURE);
                }
                arguments->insert_interval = atoi(argv[++i]);
            } else if (0 == strncmp(argv[i], "--insert-interval=", strlen("--insert-interval="))) {
                if (isStringNumber((char *)(argv[i] + strlen("--insert-interval=")))) {
                    arguments->insert_interval = atoi((char *)(argv[i]+strlen("--insert-interval=")));
                } else {
                    errorPrintReqArg3(argv[0], "--insert-innterval");
                    exit(EXIT_FAILURE);
                }
            } else if (0 == strncmp(argv[i], "-i", strlen("-i"))) {
                if (isStringNumber((char *)(argv[i] + strlen("-i")))) {
                    arguments->insert_interval = atoi((char *)(argv[i]+strlen("-i")));
                } else {
                    errorPrintReqArg3(argv[0], "-i");
                    exit(EXIT_FAILURE);
                }
            } else if (strlen("--insert-interval")== strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg3(argv[0], "--insert-interval");
                    exit(EXIT_FAILURE);
                } else if (!isStringNumber(argv[i+1])) {
                    errorPrintReqArg2(argv[0], "--insert-interval");
                    exit(EXIT_FAILURE);
                }
                arguments->insert_interval = atoi(argv[++i]);
            } else {
                errorUnrecognized(argv[0], argv[i]);
                exit(EXIT_FAILURE);
            }
        } else if ((0 == strncmp(argv[i], "-S", strlen("-S")))
                || (0 == strncmp(argv[i], "--time-step", strlen("--time-step")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg(argv[0], "S");
                    exit(EXIT_FAILURE);
                } else if (!isStringNumber(argv[i+1])) {
                    errorPrintReqArg2(argv[0], "S");
                    exit(EXIT_FAILURE);
                }
                arguments->timestamp_step = atoi(argv[++i]);
            } else if (0 == strncmp(argv[i], "--time-step=", strlen("--time-step="))) {
                if (isStringNumber((char *)(argv[i] + strlen("--time-step=")))) {
                    arguments->async_mode = atoi((char *)(argv[i]+strlen("--time-step=")));
                } else {
                    errorPrintReqArg2(argv[0], "--time-step");
                    exit(EXIT_FAILURE);
                }
            } else if (0 == strncmp(argv[i], "-S", strlen("-S"))) {
                if (isStringNumber((char *)(argv[i] + strlen("-S")))) {
                    arguments->timestamp_step = atoi((char *)(argv[i]+strlen("-S")));
                } else {
                    errorPrintReqArg2(argv[0], "-S");
                    exit(EXIT_FAILURE);
                }
            } else if (strlen("--time-step") == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg3(argv[0], "--time-step");
                    exit(EXIT_FAILURE);
                } else if (!isStringNumber(argv[i+1])) {
                    errorPrintReqArg2(argv[0], "--time-step");
                    exit(EXIT_FAILURE);
                }
                arguments->timestamp_step = atoi(argv[++i]);
            } else {
                errorUnrecognized(argv[0], argv[i]);
                exit(EXIT_FAILURE);
            }
        } else if (strcmp(argv[i], "-qt") == 0) {
            if ((argc == i+1)
                    || (!isStringNumber(argv[i+1]))) {
                printHelp();
                errorPrint("%s", "\n\t-qt need a number following!\n");
                exit(EXIT_FAILURE);
            }
            arguments->query_times = atoi(argv[++i]);
        } else if ((0 == strncmp(argv[i], "-B", strlen("-B")))
                || (0 == strncmp(argv[i], "--interlace-rows", strlen("--interlace-rows")))) {
            if (strlen("-B") == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg(argv[0], "B");
                    exit(EXIT_FAILURE);
                } else if (!isStringNumber(argv[i+1])) {
                    errorPrintReqArg2(argv[0], "B");
                    exit(EXIT_FAILURE);
                }
                arguments->interlaceRows = atoi(argv[++i]);
            } else if (0 == strncmp(argv[i], "--interlace-rows=", strlen("--interlace-rows="))) {
                if (isStringNumber((char *)(argv[i] + strlen("--interlace-rows=")))) {
                    arguments->interlaceRows = atoi((char *)(argv[i]+strlen("--interlace-rows=")));
                } else {
                    errorPrintReqArg2(argv[0], "--interlace-rows");
                    exit(EXIT_FAILURE);
                }
            } else if (0 == strncmp(argv[i], "-B", strlen("-B"))) {
                if (isStringNumber((char *)(argv[i] + strlen("-B")))) {
                    arguments->interlaceRows = atoi((char *)(argv[i]+strlen("-B")));
                } else {
                    errorPrintReqArg2(argv[0], "-B");
                    exit(EXIT_FAILURE);
                }
            } else if (strlen("--interlace-rows")== strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg3(argv[0], "--interlace-rows");
                    exit(EXIT_FAILURE);
                } else if (!isStringNumber(argv[i+1])) {
                    errorPrintReqArg2(argv[0], "--interlace-rows");
                    exit(EXIT_FAILURE);
                }
                arguments->interlaceRows = atoi(argv[++i]);
            } else {
                errorUnrecognized(argv[0], argv[i]);
                exit(EXIT_FAILURE);
            }
        } else if ((0 == strncmp(argv[i], "-r", strlen("-r")))
                || (0 == strncmp(argv[i], "--rec-per-req", 13))) {
            if (strlen("-r") == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg(argv[0], "r");
                    exit(EXIT_FAILURE);
                } else if (!isStringNumber(argv[i+1])) {
                    errorPrintReqArg2(argv[0], "r");
                    exit(EXIT_FAILURE);
                }
                arguments->reqPerReq = atoi(argv[++i]);
            } else if (0 == strncmp(argv[i], "--rec-per-req=", strlen("--rec-per-req="))) {
                if (isStringNumber((char *)(argv[i] + strlen("--rec-per-req=")))) {
                    arguments->reqPerReq = atoi((char *)(argv[i]+strlen("--rec-per-req=")));
                } else {
                    errorPrintReqArg2(argv[0], "--rec-per-req");
                    exit(EXIT_FAILURE);
                }
            } else if (0 == strncmp(argv[i], "-r", strlen("-r"))) {
                if (isStringNumber((char *)(argv[i] + strlen("-r")))) {
                    arguments->reqPerReq = atoi((char *)(argv[i]+strlen("-r")));
                } else {
                    errorPrintReqArg2(argv[0], "-r");
                    exit(EXIT_FAILURE);
                }
            } else if (strlen("--rec-per-req")== strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg3(argv[0], "--rec-per-req");
                    exit(EXIT_FAILURE);
                } else if (!isStringNumber(argv[i+1])) {
                    errorPrintReqArg2(argv[0], "--rec-per-req");
                    exit(EXIT_FAILURE);
                }
                arguments->reqPerReq = atoi(argv[++i]);
            } else {
                errorUnrecognized(argv[0], argv[i]);
                exit(EXIT_FAILURE);
            }
        } else if ((0 == strncmp(argv[i], "-t", strlen("-t")))
                || (0 == strncmp(argv[i], "--tables", strlen("--tables")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg(argv[0], "t");
                    exit(EXIT_FAILURE);
                } else if (!isStringNumber(argv[i+1])) {
                    errorPrintReqArg2(argv[0], "t");
                    exit(EXIT_FAILURE);
                }
                arguments->ntables = atoi(argv[++i]);
            } else if (0 == strncmp(argv[i], "--tables=", strlen("--tables="))) {
                if (isStringNumber((char *)(argv[i] + strlen("--tables=")))) {
                    arguments->ntables = atoi((char *)(argv[i]+strlen("--tables=")));
                } else {
                    errorPrintReqArg2(argv[0], "--tables");
                    exit(EXIT_FAILURE);
                }
            } else if (0 == strncmp(argv[i], "-t", strlen("-t"))) {
                if (isStringNumber((char *)(argv[i] + strlen("-t")))) {
                    arguments->ntables = atoi((char *)(argv[i]+strlen("-t")));
                } else {
                    errorPrintReqArg2(argv[0], "-t");
                    exit(EXIT_FAILURE);
                }
            } else if (strlen("--tables") == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg3(argv[0], "--tables");
                    exit(EXIT_FAILURE);
                } else if (!isStringNumber(argv[i+1])) {
                    errorPrintReqArg2(argv[0], "--tables");
                    exit(EXIT_FAILURE);
                }
                arguments->ntables = atoi(argv[++i]);
            } else {
                errorUnrecognized(argv[0], argv[i]);
                exit(EXIT_FAILURE);
            }

            g_totalChildTables = arguments->ntables;
        } else if ((0 == strncmp(argv[i], "-n", strlen("-n")))
                || (0 == strncmp(argv[i], "--records", strlen("--records")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg(argv[0], "n");
                    exit(EXIT_FAILURE);
                } else if (!isStringNumber(argv[i+1])) {
                    errorPrintReqArg2(argv[0], "n");
                    exit(EXIT_FAILURE);
                }
                arguments->insertRows = atoi(argv[++i]);
            } else if (0 == strncmp(argv[i], "--records=", strlen("--records="))) {
                if (isStringNumber((char *)(argv[i] + strlen("--records=")))) {
                    arguments->insertRows = atoi((char *)(argv[i]+strlen("--records=")));
                } else {
                    errorPrintReqArg2(argv[0], "--records");
                    exit(EXIT_FAILURE);
                }
            } else if (0 == strncmp(argv[i], "-n", strlen("-n"))) {
                if (isStringNumber((char *)(argv[i] + strlen("-n")))) {
                    arguments->insertRows = atoi((char *)(argv[i]+strlen("-n")));
                } else {
                    errorPrintReqArg2(argv[0], "-n");
                    exit(EXIT_FAILURE);
                }
            } else if (strlen("--records") == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg3(argv[0], "--records");
                    exit(EXIT_FAILURE);
                } else if (!isStringNumber(argv[i+1])) {
                    errorPrintReqArg2(argv[0], "--records");
                    exit(EXIT_FAILURE);
                }
                arguments->insertRows = atoi(argv[++i]);
            } else {
                errorUnrecognized(argv[0], argv[i]);
                exit(EXIT_FAILURE);
            }
        } else if ((0 == strncmp(argv[i], "-d", strlen("-d")))
                || (0 == strncmp(argv[i], "--database", strlen("--database")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg(argv[0], "d");
                    exit(EXIT_FAILURE);
                }
                arguments->database = argv[++i];
            } else if (0 == strncmp(argv[i], "--database=", strlen("--database="))) {
                arguments->output_file = (char *)(argv[i] + strlen("--database="));
            } else if (0 == strncmp(argv[i], "-d", strlen("-d"))) {
                arguments->output_file = (char *)(argv[i] + strlen("-d"));
            } else if (strlen("--database") == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg3(argv[0], "--database");
                    exit(EXIT_FAILURE);
                }
                arguments->database = argv[++i];
            } else {
                errorUnrecognized(argv[0], argv[i]);
                exit(EXIT_FAILURE);
            }
        } else if ((0 == strncmp(argv[i], "-l", strlen("-l")))
                || (0 == strncmp(argv[i], "--columns", strlen("--columns")))) {
            arguments->demo_mode = false;
            if (2 == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg(argv[0], "l");
                    exit(EXIT_FAILURE);
                } else if (!isStringNumber(argv[i+1])) {
                    errorPrintReqArg2(argv[0], "l");
                    exit(EXIT_FAILURE);
                }
                arguments->columnCount = atoi(argv[++i]);
            } else if (0 == strncmp(argv[i], "--columns=", strlen("--columns="))) {
                if (isStringNumber((char *)(argv[i] + strlen("--columns=")))) {
                    arguments->columnCount = atoi((char *)(argv[i]+strlen("--columns=")));
                } else {
                    errorPrintReqArg2(argv[0], "--columns");
                    exit(EXIT_FAILURE);
                }
            } else if (0 == strncmp(argv[i], "-l", strlen("-l"))) {
                if (isStringNumber((char *)(argv[i] + strlen("-l")))) {
                    arguments->columnCount = atoi((char *)(argv[i]+strlen("-l")));
                } else {
                    errorPrintReqArg2(argv[0], "-l");
                    exit(EXIT_FAILURE);
                }
            } else if (strlen("--columns")== strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg3(argv[0], "--columns");
                    exit(EXIT_FAILURE);
                } else if (!isStringNumber(argv[i+1])) {
                    errorPrintReqArg2(argv[0], "--columns");
                    exit(EXIT_FAILURE);
                }
                arguments->columnCount = atoi(argv[++i]);
            } else {
                errorUnrecognized(argv[0], argv[i]);
                exit(EXIT_FAILURE);
            }

            if (arguments->columnCount > MAX_NUM_COLUMNS) {
                printf("WARNING: max acceptable columns count is %d\n", MAX_NUM_COLUMNS);
                prompt();
                arguments->columnCount = MAX_NUM_COLUMNS;
            }

            for (int col = DEFAULT_DATATYPE_NUM; col < arguments->columnCount; col ++) {
                arguments->dataType[col] = "INT";
                arguments->data_type[col] = TSDB_DATA_TYPE_INT;
            }
            for (int col = arguments->columnCount; col < MAX_NUM_COLUMNS; col++) {
                arguments->dataType[col] = NULL;
                arguments->data_type[col] = TSDB_DATA_TYPE_NULL;
            }
        } else if ((0 == strncmp(argv[i], "-b", strlen("-b")))
                || (0 == strncmp(argv[i], "--data-type", strlen("--data-type")))) {
            arguments->demo_mode = false;

            char *dataType;
            if (2 == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg(argv[0], "b");
                    exit(EXIT_FAILURE);
                }
                dataType = argv[++i];
            } else if (0 == strncmp(argv[i], "--data-type=", strlen("--data-type="))) {
                dataType = (char *)(argv[i] + strlen("--data-type="));
            } else if (0 == strncmp(argv[i], "-b", strlen("-b"))) {
                dataType = (char *)(argv[i] + strlen("-b"));
            } else if (strlen("--data-type") == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg3(argv[0], "--data-type");
                    exit(EXIT_FAILURE);
                }
                dataType = argv[++i];
            } else {
                errorUnrecognized(argv[0], argv[i]);
                exit(EXIT_FAILURE);
            }

            if (strstr(dataType, ",") == NULL) {
                // only one col
                if (strcasecmp(dataType, "INT")
                        && strcasecmp(dataType, "FLOAT")
                        && strcasecmp(dataType, "TINYINT")
                        && strcasecmp(dataType, "BOOL")
                        && strcasecmp(dataType, "SMALLINT")
                        && strcasecmp(dataType, "BIGINT")
                        && strcasecmp(dataType, "DOUBLE")
                        && strcasecmp(dataType, "TIMESTAMP")
                        && !regexMatch(dataType,
                            "^(NCHAR|BINARY)(\\([1-9][0-9]*\\))?$",
                            REG_ICASE | REG_EXTENDED)
                        && strcasecmp(dataType, "UTINYINT")
                        && strcasecmp(dataType, "USMALLINT")
                        && strcasecmp(dataType, "UINT")
                        && strcasecmp(dataType, "UBIGINT")) {
                    printHelp();
                    errorPrint("%s", "-b: Invalid data_type!\n");
                    exit(EXIT_FAILURE);
                }
                arguments->dataType[0] = dataType;
                if (0 == strcasecmp(dataType, "INT")) {
                    arguments->data_type[0] = TSDB_DATA_TYPE_INT;
                } else if (0 == strcasecmp(dataType, "TINYINT")) {
                    arguments->data_type[0] = TSDB_DATA_TYPE_TINYINT;
                } else if (0 == strcasecmp(dataType, "SMALLINT")) {
                    arguments->data_type[0] = TSDB_DATA_TYPE_SMALLINT;
                } else if (0 == strcasecmp(dataType, "BIGINT")) {
                    arguments->data_type[0] = TSDB_DATA_TYPE_BIGINT;
                } else if (0 == strcasecmp(dataType, "FLOAT")) {
                    arguments->data_type[0] = TSDB_DATA_TYPE_FLOAT;
                } else if (0 == strcasecmp(dataType, "DOUBLE")) {
                    arguments->data_type[0] = TSDB_DATA_TYPE_DOUBLE;
                } else if (1 == regexMatch(dataType,
                            "^BINARY(\\([1-9][0-9]*\\))?$",
                            REG_ICASE | REG_EXTENDED)) {
                    arguments->data_type[0] = TSDB_DATA_TYPE_BINARY;
                } else if (1 == regexMatch(dataType,
                            "^NCHAR(\\([1-9][0-9]*\\))?$",
                            REG_ICASE | REG_EXTENDED)) {
                    arguments->data_type[0] = TSDB_DATA_TYPE_NCHAR;
                } else if (0 == strcasecmp(dataType, "BOOL")) {
                    arguments->data_type[0] = TSDB_DATA_TYPE_BOOL;
                } else if (0 == strcasecmp(dataType, "TIMESTAMP")) {
                    arguments->data_type[0] = TSDB_DATA_TYPE_TIMESTAMP;
                } else if (0 == strcasecmp(dataType, "UTINYINT")) {
                    arguments->data_type[0] = TSDB_DATA_TYPE_UTINYINT;
                } else if (0 == strcasecmp(dataType, "USMALLINT")) {
                    arguments->data_type[0] = TSDB_DATA_TYPE_USMALLINT;
                } else if (0 == strcasecmp(dataType, "UINT")) {
                    arguments->data_type[0] = TSDB_DATA_TYPE_UINT;
                } else if (0 == strcasecmp(dataType, "UBIGINT")) {
                    arguments->data_type[0] = TSDB_DATA_TYPE_UBIGINT;
                } else {
                    arguments->data_type[0] = TSDB_DATA_TYPE_NULL;
                }
                arguments->dataType[1] = NULL;
                arguments->data_type[1] = TSDB_DATA_TYPE_NULL;
            } else {
                // more than one col
                int tdm_index = 0;
                g_dupstr = strdup(dataType);
                char *running = g_dupstr;
                char *token = strsep(&running, ",");
                while(token != NULL) {
                    if (strcasecmp(token, "INT")
                            && strcasecmp(token, "FLOAT")
                            && strcasecmp(token, "TINYINT")
                            && strcasecmp(token, "BOOL")
                            && strcasecmp(token, "SMALLINT")
                            && strcasecmp(token, "BIGINT")
                            && strcasecmp(token, "DOUBLE")
                            && strcasecmp(token, "TIMESTAMP")
                            && !regexMatch(token, "^(NCHAR|BINARY)(\\([1-9][0-9]*\\))?$", REG_ICASE | REG_EXTENDED)
                            && strcasecmp(token, "UTINYINT")
                            && strcasecmp(token, "USMALLINT")
                            && strcasecmp(token, "UINT")
                            && strcasecmp(token, "UBIGINT")) {
                        printHelp();
                        free(g_dupstr);
                        errorPrint("%s", "-b: Invalid data_type!\n");
                        exit(EXIT_FAILURE);
                    }

                    if (0 == strcasecmp(token, "INT")) {
                        arguments->data_type[tdm_index] = TSDB_DATA_TYPE_INT;
                    } else if (0 == strcasecmp(token, "FLOAT")) {
                        arguments->data_type[tdm_index] = TSDB_DATA_TYPE_FLOAT;
                    } else if (0 == strcasecmp(token, "SMALLINT")) {
                        arguments->data_type[tdm_index] = TSDB_DATA_TYPE_SMALLINT;
                    } else if (0 == strcasecmp(token, "BIGINT")) {
                        arguments->data_type[tdm_index] = TSDB_DATA_TYPE_BIGINT;
                    } else if (0 == strcasecmp(token, "DOUBLE")) {
                        arguments->data_type[tdm_index] = TSDB_DATA_TYPE_DOUBLE;
                    } else if (0 == strcasecmp(token, "TINYINT")) {
                        arguments->data_type[tdm_index] = TSDB_DATA_TYPE_TINYINT;
                    } else if (1 == regexMatch(token, "^BINARY(\\([1-9][0-9]*\\))?$", REG_ICASE |
                    REG_EXTENDED)) {
                        arguments->data_type[tdm_index] = TSDB_DATA_TYPE_BINARY;
                    } else if (1 == regexMatch(token, "^NCHAR(\\([1-9][0-9]*\\))?$", REG_ICASE |
                    REG_EXTENDED)) {
                        arguments->data_type[tdm_index] = TSDB_DATA_TYPE_NCHAR;
                    } else if (0 == strcasecmp(token, "BOOL")) {
                        arguments->data_type[tdm_index] = TSDB_DATA_TYPE_BOOL;
                    } else if (0 == strcasecmp(token, "TIMESTAMP")) {
                        arguments->data_type[tdm_index] = TSDB_DATA_TYPE_TIMESTAMP;
                    } else if (0 == strcasecmp(token, "UTINYINT")) {
                        arguments->data_type[tdm_index] = TSDB_DATA_TYPE_UTINYINT;
                    } else if (0 == strcasecmp(token, "USMALLINT")) {
                        arguments->data_type[tdm_index] = TSDB_DATA_TYPE_USMALLINT;
                    } else if (0 == strcasecmp(token, "UINT")) {
                        arguments->data_type[tdm_index] = TSDB_DATA_TYPE_UINT;
                    } else if (0 == strcasecmp(token, "UBIGINT")) {
                        arguments->data_type[tdm_index] = TSDB_DATA_TYPE_UBIGINT;
                    } else {
                        arguments->data_type[tdm_index] = TSDB_DATA_TYPE_NULL;
                    }
                    arguments->dataType[tdm_index] = token;
                    tdm_index ++;
                    token = strsep(&running, ",");
                    if (tdm_index >= MAX_NUM_COLUMNS) break;
                }
                arguments->dataType[tdm_index] = NULL;
                arguments->data_type[tdm_index] = TSDB_DATA_TYPE_NULL;
            }
        } else if ((0 == strncmp(argv[i], "-w", strlen("-w")))
                || (0 == strncmp(argv[i], "--binwidth", strlen("--binwidth")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg(argv[0], "w");
                    exit(EXIT_FAILURE);
                } else if (!isStringNumber(argv[i+1])) {
                    errorPrintReqArg2(argv[0], "w");
                    exit(EXIT_FAILURE);
                }
                arguments->binwidth = atoi(argv[++i]);
            } else if (0 == strncmp(argv[i], "--binwidth=", strlen("--binwidth="))) {
                if (isStringNumber((char *)(argv[i] + strlen("--binwidth=")))) {
                    arguments->binwidth = atoi((char *)(argv[i]+strlen("--binwidth=")));
                } else {
                    errorPrintReqArg2(argv[0], "--binwidth");
                    exit(EXIT_FAILURE);
                }
            } else if (0 == strncmp(argv[i], "-w", strlen("-w"))) {
                if (isStringNumber((char *)(argv[i] + strlen("-w")))) {
                    arguments->binwidth = atoi((char *)(argv[i]+strlen("-w")));
                } else {
                    errorPrintReqArg2(argv[0], "-w");
                    exit(EXIT_FAILURE);
                }
            } else if (strlen("--binwidth") == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg3(argv[0], "--binwidth");
                    exit(EXIT_FAILURE);
                } else if (!isStringNumber(argv[i+1])) {
                    errorPrintReqArg2(argv[0], "--binwidth");
                    exit(EXIT_FAILURE);
                }
                arguments->binwidth = atoi(argv[++i]);
            } else {
                errorUnrecognized(argv[0], argv[i]);
                exit(EXIT_FAILURE);
            }
        } else if ((0 == strncmp(argv[i], "-m", strlen("-m")))
                || (0 == strncmp(argv[i], "--table-prefix", strlen("--table-prefix")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg(argv[0], "m");
                    exit(EXIT_FAILURE);
                }
                arguments->tb_prefix = argv[++i];
            } else if (0 == strncmp(argv[i], "--table-prefix=", strlen("--table-prefix="))) {
                arguments->tb_prefix = (char *)(argv[i] + strlen("--table-prefix="));
            } else if (0 == strncmp(argv[i], "-m", strlen("-m"))) {
                arguments->tb_prefix = (char *)(argv[i] + strlen("-m"));
            } else if (strlen("--table-prefix") == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg3(argv[0], "--table-prefix");
                    exit(EXIT_FAILURE);
                }
                arguments->tb_prefix = argv[++i];
            } else {
                errorUnrecognized(argv[0], argv[i]);
                exit(EXIT_FAILURE);
            }
        } else if ((strcmp(argv[i], "-N") == 0)
                || (0 == strcmp(argv[i], "--normal-table"))) {
            arguments->demo_mode = false;
            arguments->use_metric = false;
        } else if ((strcmp(argv[i], "-M") == 0)
                || (0 == strcmp(argv[i], "--random"))) {
            arguments->demo_mode = false;
        } else if ((strcmp(argv[i], "-x") == 0)
                || (0 == strcmp(argv[i], "--aggr-func"))) {
            arguments->aggr_func = true;
        } else if ((strcmp(argv[i], "-y") == 0)
                || (0 == strcmp(argv[i], "--answer-yes"))) {
            arguments->answer_yes = true;
        } else if ((strcmp(argv[i], "-g") == 0)
                || (0 == strcmp(argv[i], "--debug"))) {
            arguments->debug_print = true;
        } else if (strcmp(argv[i], "-gg") == 0) {
            arguments->verbose_print = true;
        } else if ((0 == strncmp(argv[i], "-R", strlen("-R")))
                || (0 == strncmp(argv[i], "--disorder-range",
                        strlen("--disorder-range")))) {
            if (strlen("-R") == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg(argv[0], "R");
                    exit(EXIT_FAILURE);
                } else if (!isStringNumber(argv[i+1])) {
                    errorPrintReqArg2(argv[0], "R");
                    exit(EXIT_FAILURE);
                }
                arguments->disorderRange = atoi(argv[++i]);
            } else if (0 == strncmp(argv[i], "--disorder-range=",
                        strlen("--disorder-range="))) {
                if (isStringNumber((char *)(argv[i] + strlen("--disorder-range=")))) {
                    arguments->disorderRange =
                        atoi((char *)(argv[i]+strlen("--disorder-range=")));
                } else {
                    errorPrintReqArg2(argv[0], "--disorder-range");
                    exit(EXIT_FAILURE);
                }
            } else if (0 == strncmp(argv[i], "-R", strlen("-R"))) {
                if (isStringNumber((char *)(argv[i] + strlen("-R")))) {
                    arguments->disorderRange =
                        atoi((char *)(argv[i]+strlen("-R")));
                } else {
                    errorPrintReqArg2(argv[0], "-R");
                    exit(EXIT_FAILURE);
                }

                if (arguments->disorderRange < 0) {
                    errorPrint("Invalid disorder range %d, will be set to %d\n",
                            arguments->disorderRange, 1000);
                    arguments->disorderRange = 1000;
                }
            } else if (strlen("--disorder-range") == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg3(argv[0], "--disorder-range");
                    exit(EXIT_FAILURE);
                } else if (!isStringNumber(argv[i+1])) {
                    errorPrintReqArg2(argv[0], "--disorder-range");
                    exit(EXIT_FAILURE);
                }
                arguments->disorderRange = atoi(argv[++i]);
            } else {
                errorUnrecognized(argv[0], argv[i]);
                exit(EXIT_FAILURE);
            }
        } else if ((0 == strncmp(argv[i], "-O", strlen("-O")))
                || (0 == strncmp(argv[i], "--disorder", strlen("--disorder")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg(argv[0], "O");
                    exit(EXIT_FAILURE);
                } else if (!isStringNumber(argv[i+1])) {
                    errorPrintReqArg2(argv[0], "O");
                    exit(EXIT_FAILURE);
                }
                arguments->disorderRatio = atoi(argv[++i]);
            } else if (0 == strncmp(argv[i], "--disorder=", strlen("--disorder="))) {
                if (isStringNumber((char *)(argv[i] + strlen("--disorder=")))) {
                    arguments->disorderRatio = atoi((char *)(argv[i]+strlen("--disorder=")));
                } else {
                    errorPrintReqArg2(argv[0], "--disorder");
                    exit(EXIT_FAILURE);
                }
            } else if (0 == strncmp(argv[i], "-O", strlen("-O"))) {
                if (isStringNumber((char *)(argv[i] + strlen("-O")))) {
                    arguments->disorderRatio = atoi((char *)(argv[i]+strlen("-O")));
                } else {
                    errorPrintReqArg2(argv[0], "-O");
                    exit(EXIT_FAILURE);
                }
            } else if (strlen("--disorder") == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg3(argv[0], "--disorder");
                    exit(EXIT_FAILURE);
                } else if (!isStringNumber(argv[i+1])) {
                    errorPrintReqArg2(argv[0], "--disorder");
                    exit(EXIT_FAILURE);
                }
                arguments->disorderRatio = atoi(argv[++i]);
            } else {
                errorUnrecognized(argv[0], argv[i]);
                exit(EXIT_FAILURE);
            }

            if (arguments->disorderRatio > 50) {
                errorPrint("Invalid disorder ratio %d, will be set to %d\n",
                        arguments->disorderRatio, 50);
                arguments->disorderRatio = 50;
            }
        } else if ((0 == strncmp(argv[i], "-a", strlen("-a")))
                || (0 == strncmp(argv[i], "--replica",
                        strlen("--replica")))) {
            if (2 == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg(argv[0], "a");
                    exit(EXIT_FAILURE);
                } else if (!isStringNumber(argv[i+1])) {
                    errorPrintReqArg2(argv[0], "a");
                    exit(EXIT_FAILURE);
                }
                arguments->replica = atoi(argv[++i]);
            } else if (0 == strncmp(argv[i], "--replica=",
                        strlen("--replica="))) {
                if (isStringNumber((char *)(argv[i] + strlen("--replica=")))) {
                    arguments->replica =
                        atoi((char *)(argv[i]+strlen("--replica=")));
                } else {
                    errorPrintReqArg2(argv[0], "--replica");
                    exit(EXIT_FAILURE);
                }
            } else if (0 == strncmp(argv[i], "-a", strlen("-a"))) {
                if (isStringNumber((char *)(argv[i] + strlen("-a")))) {
                    arguments->replica =
                        atoi((char *)(argv[i]+strlen("-a")));
                } else {
                    errorPrintReqArg2(argv[0], "-a");
                    exit(EXIT_FAILURE);
                }
            } else if (strlen("--replica") == strlen(argv[i])) {
                if (argc == i+1) {
                    errorPrintReqArg3(argv[0], "--replica");
                    exit(EXIT_FAILURE);
                } else if (!isStringNumber(argv[i+1])) {
                    errorPrintReqArg2(argv[0], "--replica");
                    exit(EXIT_FAILURE);
                }
                arguments->replica = atoi(argv[++i]);
            } else {
                errorUnrecognized(argv[0], argv[i]);
                exit(EXIT_FAILURE);
            }

            if (arguments->replica > 3 || arguments->replica < 1) {
                errorPrint("Invalid replica value %d, will be set to %d\n",
                        arguments->replica, 1);
                arguments->replica = 1;
            }
        } else if (strcmp(argv[i], "-D") == 0) {
            arguments->method_of_delete = atoi(argv[++i]);
            if (arguments->method_of_delete > 3) {
                errorPrint("%s", "\n\t-D need a value (0~3) number following!\n");
                exit(EXIT_FAILURE);
            }
        } else if ((strcmp(argv[i], "--version") == 0)
                || (strcmp(argv[i], "-V") == 0)) {
            printVersion();
            exit(0);
        } else if ((strcmp(argv[i], "--help") == 0)
                || (strcmp(argv[i], "-?") == 0)) {
            printHelp();
            exit(0);
        } else if (strcmp(argv[i], "--usage") == 0) {
            printf("    Usage: taosdemo [-f JSONFILE] [-u USER] [-p PASSWORD] [-c CONFIG_DIR]\n\
                    [-h HOST] [-P PORT] [-I INTERFACE] [-d DATABASE] [-a REPLICA]\n\
                    [-m TABLEPREFIX] [-s SQLFILE] [-N] [-o OUTPUTFILE] [-q QUERYMODE]\n\
                    [-b DATATYPES] [-w WIDTH_OF_BINARY] [-l COLUMNS] [-T THREADNUMBER]\n\
                    [-i SLEEPTIME] [-S TIME_STEP] [-B INTERLACE_ROWS] [-t TABLES]\n\
                    [-n RECORDS] [-M] [-x] [-y] [-O ORDERMODE] [-R RANGE] [-a REPLIcA][-g]\n\
                    [--help] [--usage] [--version]\n");
            exit(0);
        } else {
            // to simulate argp_option output
            if (strlen(argv[i]) > 2) {
                if (0 == strncmp(argv[i], "--", 2)) {
                    fprintf(stderr, "%s: unrecognized options '%s'\n", argv[0], argv[i]);
                } else if (0 == strncmp(argv[i], "-", 1)) {
                    char tmp[2] = {0};
                    tstrncpy(tmp, argv[i]+1, 2);
                    fprintf(stderr, "%s: invalid options -- '%s'\n", argv[0], tmp);
                } else {
                    fprintf(stderr, "%s: Too many arguments\n", argv[0]);
                }
            } else {
                fprintf(stderr, "%s invalid options -- '%s'\n", argv[0],
                        (char *)((char *)argv[i])+1);
            }
            fprintf(stderr, "Try `taosdemo --help' or `taosdemo --usage' for more information.\n");
            exit(EXIT_FAILURE);
        }
    }

    int columnCount;
    for (columnCount = 0; columnCount < MAX_NUM_COLUMNS; columnCount ++) {
        if (g_args.dataType[columnCount] == NULL) {
            break;
        }
    }

    if (0 == columnCount) {
        ERROR_EXIT("data type error!");
    }
    g_args.columnCount = columnCount;

    g_args.lenOfOneRow = 20; // timestamp
    for (int c = 0; c < g_args.columnCount; c++) {
        switch(g_args.data_type[c]) {
            case TSDB_DATA_TYPE_BINARY:
                g_args.lenOfOneRow += g_args.binwidth + 3;
                break;

            case TSDB_DATA_TYPE_NCHAR:
                g_args.lenOfOneRow += g_args.binwidth + 3;
                break;

            case TSDB_DATA_TYPE_INT:
            case TSDB_DATA_TYPE_UINT:
                g_args.lenOfOneRow += INT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_BIGINT:
            case TSDB_DATA_TYPE_UBIGINT:
                g_args.lenOfOneRow += BIGINT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_SMALLINT:
            case TSDB_DATA_TYPE_USMALLINT:
                g_args.lenOfOneRow += SMALLINT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_TINYINT:
            case TSDB_DATA_TYPE_UTINYINT:
                g_args.lenOfOneRow += TINYINT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_BOOL:
                g_args.lenOfOneRow += BOOL_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_FLOAT:
                g_args.lenOfOneRow += FLOAT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_DOUBLE:
                g_args.lenOfOneRow += DOUBLE_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_TIMESTAMP:
                g_args.lenOfOneRow += TIMESTAMP_BUFF_LEN;
                break;

            default:
                errorPrint2("get error data type : %s\n", g_args.dataType[c]);
                exit(EXIT_FAILURE);
        }
    }

    if (((arguments->debug_print) && (NULL != arguments->metaFile))
            || arguments->verbose_print) {
        printf("###################################################################\n");
        printf("# meta file:                         %s\n", arguments->metaFile);
        printf("# Server IP:                         %s:%hu\n",
                arguments->host == NULL ? "localhost" : arguments->host,
                arguments->port );
        printf("# User:                              %s\n", arguments->user);
        printf("# Password:                          %s\n", arguments->password);
        printf("# Use metric:                        %s\n",
                arguments->use_metric ? "true" : "false");
        if (*(arguments->dataType)) {
            printf("# Specified data type:               ");
            for (int c = 0; c < MAX_NUM_COLUMNS; c++)
                if (arguments->dataType[c])
                    printf("%s,", arguments->dataType[c]);
                else
                    break;
            printf("\n");
        }
        printf("# Insertion interval:                %"PRIu64"\n",
                arguments->insert_interval);
        printf("# Number of records per req:         %u\n",
                arguments->reqPerReq);
        printf("# Max SQL length:                    %"PRIu64"\n",
                arguments->max_sql_len);
        printf("# Length of Binary:                  %d\n", arguments->binwidth);
        printf("# Number of Threads:                 %d\n", arguments->nthreads);
        printf("# Number of Tables:                  %"PRId64"\n",
                arguments->ntables);
        printf("# Number of Data per Table:          %"PRId64"\n",
                arguments->insertRows);
        printf("# Database name:                     %s\n", arguments->database);
        printf("# Table prefix:                      %s\n", arguments->tb_prefix);
        if (arguments->disorderRatio) {
            printf("# Data order:                        %d\n", arguments->disorderRatio);
            printf("# Data out of order rate:            %d\n", arguments->disorderRange);
        }
        printf("# Delete method:                     %d\n", arguments->method_of_delete);
        printf("# Answer yes when prompt:            %d\n", arguments->answer_yes);
        printf("# Print debug info:                  %d\n", arguments->debug_print);
        printf("# Print verbose info:                %d\n", arguments->verbose_print);
        printf("###################################################################\n");

        prompt();
    }
}

static void tmfclose(FILE *fp) {
    if (NULL != fp) {
        fclose(fp);
    }
}

static void tmfree(void *buf) {
    if (NULL != buf) {
        free(buf);
        buf = NULL;
    }
}

static int queryDbExec(TAOS *taos, char *command, QUERY_TYPE type, bool quiet) {

    verbosePrint("%s() LN%d - command: %s\n", __func__, __LINE__, command);

    TAOS_RES *res = taos_query(taos, command);
    int32_t code = taos_errno(res);

    if (code != 0) {
        if (!quiet) {
            errorPrint2("Failed to execute <%s>, reason: %s\n",
                    command, taos_errstr(res));
        }
        taos_free_result(res);
        //taos_close(taos);
        return -1;
    }

    if (INSERT_TYPE == type) {
        int affectedRows = taos_affected_rows(res);
        taos_free_result(res);
        return affectedRows;
    }

    taos_free_result(res);
    return 0;
}

static void appendResultBufToFile(char *resultBuf, threadInfo *pThreadInfo)
{
    pThreadInfo->fp = fopen(pThreadInfo->filePath, "at");
    if (pThreadInfo->fp == NULL) {
        errorPrint2(
                "%s() LN%d, failed to open result file: %s, result will not save to file\n",
                __func__, __LINE__, pThreadInfo->filePath);
        return;
    }

    fprintf(pThreadInfo->fp, "%s", resultBuf);
    tmfclose(pThreadInfo->fp);
    pThreadInfo->fp = NULL;
}

static void fetchResult(TAOS_RES *res, threadInfo* pThreadInfo) {
    TAOS_ROW    row = NULL;
    int         num_rows = 0;
    int         num_fields = taos_field_count(res);
    TAOS_FIELD *fields     = taos_fetch_fields(res);

    char* databuf = (char*) calloc(1, 100*1024*1024);
    if (databuf == NULL) {
        errorPrint2("%s() LN%d, failed to malloc, warning: save result to file slowly!\n",
                __func__, __LINE__);
        return ;
    }

    int64_t   totalLen = 0;

    // fetch the records row by row
    while((row = taos_fetch_row(res))) {
        if (totalLen >= (100*1024*1024 - HEAD_BUFF_LEN*2)) {
            if (strlen(pThreadInfo->filePath) > 0)
                appendResultBufToFile(databuf, pThreadInfo);
            totalLen = 0;
            memset(databuf, 0, 100*1024*1024);
        }
        num_rows++;
        char  temp[HEAD_BUFF_LEN] = {0};
        int len = taos_print_row(temp, row, fields, num_fields);
        len += sprintf(temp + len, "\n");
        //printf("query result:%s\n", temp);
        memcpy(databuf + totalLen, temp, len);
        totalLen += len;
        verbosePrint("%s() LN%d, totalLen: %"PRId64"\n",
                __func__, __LINE__, totalLen);
    }

    verbosePrint("%s() LN%d, databuf=%s resultFile=%s\n",
            __func__, __LINE__, databuf, pThreadInfo->filePath);
    if (strlen(pThreadInfo->filePath) > 0) {
        appendResultBufToFile(databuf, pThreadInfo);
    }
    free(databuf);
}

static void selectAndGetResult(
        threadInfo *pThreadInfo, char *command)
{
    if (0 == strncasecmp(g_queryInfo.queryMode, "taosc", strlen("taosc"))) {
        TAOS_RES *res = taos_query(pThreadInfo->taos, command);
        if (res == NULL || taos_errno(res) != 0) {
            errorPrint2("%s() LN%d, failed to execute sql:%s, reason:%s\n",
                    __func__, __LINE__, command, taos_errstr(res));
            taos_free_result(res);
            return;
        }

        fetchResult(res, pThreadInfo);
        taos_free_result(res);

    } else if (0 == strncasecmp(g_queryInfo.queryMode, "rest", strlen("rest"))) {
        int retCode = postProceSql(
                g_queryInfo.host, g_queryInfo.port,
                command,
                pThreadInfo);
        if (0 != retCode) {
            printf("====restful return fail, threadID[%d]\n", pThreadInfo->threadID);
        }

    } else {
        errorPrint2("%s() LN%d, unknown query mode: %s\n",
                __func__, __LINE__, g_queryInfo.queryMode);
    }
}

static char *rand_bool_str() {
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_randbool_buff + ((cursor % g_args.prepared_rand) * BOOL_BUFF_LEN);
}

static int32_t rand_bool() {
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_randint[cursor % g_args.prepared_rand] % 2;
}

static char *rand_tinyint_str()
{
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_randtinyint_buff +
        ((cursor % g_args.prepared_rand) * TINYINT_BUFF_LEN);
}

static int32_t rand_tinyint()
{
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_randint[cursor % g_args.prepared_rand] % 128;
}

static char *rand_utinyint_str()
{
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_randutinyint_buff +
        ((cursor % g_args.prepared_rand) * TINYINT_BUFF_LEN);
}

static int32_t rand_utinyint()
{
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_randuint[cursor % g_args.prepared_rand] % 255;
}

static char *rand_smallint_str()
{
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_randsmallint_buff +
        ((cursor % g_args.prepared_rand) * SMALLINT_BUFF_LEN);
}

static int32_t rand_smallint()
{
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_randint[cursor % g_args.prepared_rand] % 32768;
}

static char *rand_usmallint_str()
{
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_randusmallint_buff +
        ((cursor % g_args.prepared_rand) * SMALLINT_BUFF_LEN);
}

static int32_t rand_usmallint()
{
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_randuint[cursor % g_args.prepared_rand] % 65535;
}

static char *rand_int_str()
{
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_randint_buff + ((cursor % g_args.prepared_rand) * INT_BUFF_LEN);
}

static int32_t rand_int()
{
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_randint[cursor % g_args.prepared_rand];
}

static char *rand_uint_str()
{
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_randuint_buff + ((cursor % g_args.prepared_rand) * INT_BUFF_LEN);
}

static int32_t rand_uint()
{
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_randuint[cursor % g_args.prepared_rand];
}

static char *rand_bigint_str()
{
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_randbigint_buff +
        ((cursor % g_args.prepared_rand) * BIGINT_BUFF_LEN);
}

static int64_t rand_bigint()
{
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_randbigint[cursor % g_args.prepared_rand];
}

static char *rand_ubigint_str()
{
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_randubigint_buff +
        ((cursor % g_args.prepared_rand) * BIGINT_BUFF_LEN);
}

static int64_t rand_ubigint()
{
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_randubigint[cursor % g_args.prepared_rand];
}

static char *rand_float_str()
{
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_randfloat_buff + ((cursor % g_args.prepared_rand) * FLOAT_BUFF_LEN);
}


static float rand_float()
{
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_randfloat[cursor % g_args.prepared_rand];
}

static char *demo_current_float_str()
{
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_rand_current_buff +
        ((cursor % g_args.prepared_rand) * FLOAT_BUFF_LEN);
}

static float UNUSED_FUNC demo_current_float()
{
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return (float)(9.8 + 0.04 * (g_randint[cursor % g_args.prepared_rand] % 10)
            + g_randfloat[cursor % g_args.prepared_rand]/1000000000);
}

static char *demo_voltage_int_str()
{
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_rand_voltage_buff +
        ((cursor % g_args.prepared_rand) * INT_BUFF_LEN);
}

static int32_t UNUSED_FUNC demo_voltage_int()
{
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return 215 + g_randint[cursor % g_args.prepared_rand] % 10;
}

static char *demo_phase_float_str() {
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_rand_phase_buff + ((cursor % g_args.prepared_rand) * FLOAT_BUFF_LEN);
}

static float UNUSED_FUNC demo_phase_float() {
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return (float)((115 + g_randint[cursor % g_args.prepared_rand] % 10
                + g_randfloat[cursor % g_args.prepared_rand]/1000000000)/360);
}

#if 0
static const char charNum[] = "0123456789";

static void nonrand_string(char *, int) __attribute__ ((unused));   // reserve for debugging purpose
static void nonrand_string(char *str, int size)
{
    str[0] = 0;
    if (size > 0) {
        int n;
        for (n = 0; n < size; n++) {
            str[n] = charNum[n % 10];
        }
        str[n] = 0;
    }
}
#endif

static const char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";

static void rand_string(char *str, int size) {
    str[0] = 0;
    if (size > 0) {
        //--size;
        int n;
        for (n = 0; n < size; n++) {
            int key = abs(rand_tinyint()) % (int)(sizeof(charset) - 1);
            str[n] = charset[key];
        }
        str[n] = 0;
    }
}

static char *rand_double_str()
{
    static int cursor;
    cursor++;
    if (cursor > (g_args.prepared_rand - 1)) cursor = 0;
    return g_randdouble_buff + (cursor * DOUBLE_BUFF_LEN);
}

static double rand_double()
{
    static int cursor;
    cursor++;
    cursor = cursor % g_args.prepared_rand;
    return g_randdouble[cursor];
}

static void init_rand_data() {

    g_randint_buff = calloc(1, INT_BUFF_LEN * g_args.prepared_rand);
    assert(g_randint_buff);
    g_rand_voltage_buff = calloc(1, INT_BUFF_LEN * g_args.prepared_rand);
    assert(g_rand_voltage_buff);
    g_randbigint_buff = calloc(1, BIGINT_BUFF_LEN * g_args.prepared_rand);
    assert(g_randbigint_buff);
    g_randsmallint_buff = calloc(1, SMALLINT_BUFF_LEN * g_args.prepared_rand);
    assert(g_randsmallint_buff);
    g_randtinyint_buff = calloc(1, TINYINT_BUFF_LEN * g_args.prepared_rand);
    assert(g_randtinyint_buff);
    g_randbool_buff = calloc(1, BOOL_BUFF_LEN * g_args.prepared_rand);
    assert(g_randbool_buff);
    g_randfloat_buff = calloc(1, FLOAT_BUFF_LEN * g_args.prepared_rand);
    assert(g_randfloat_buff);
    g_rand_current_buff = calloc(1, FLOAT_BUFF_LEN * g_args.prepared_rand);
    assert(g_rand_current_buff);
    g_rand_phase_buff = calloc(1, FLOAT_BUFF_LEN * g_args.prepared_rand);
    assert(g_rand_phase_buff);
    g_randdouble_buff = calloc(1, DOUBLE_BUFF_LEN * g_args.prepared_rand);
    assert(g_randdouble_buff);
    g_randuint_buff = calloc(1, INT_BUFF_LEN * g_args.prepared_rand);
    assert(g_randuint_buff);
    g_randutinyint_buff = calloc(1, TINYINT_BUFF_LEN * g_args.prepared_rand);
    assert(g_randutinyint_buff);
    g_randusmallint_buff = calloc(1, SMALLINT_BUFF_LEN * g_args.prepared_rand);
    assert(g_randusmallint_buff);
    g_randubigint_buff = calloc(1, BIGINT_BUFF_LEN * g_args.prepared_rand);
    assert(g_randubigint_buff);
    g_randint = calloc(1, sizeof(int32_t) * g_args.prepared_rand);
    assert(g_randint);
    g_randuint = calloc(1, sizeof(uint32_t) * g_args.prepared_rand);
    assert(g_randuint);
    g_randbigint = calloc(1, sizeof(int64_t) * g_args.prepared_rand);
    assert(g_randbigint);
    g_randubigint = calloc(1, sizeof(uint64_t) * g_args.prepared_rand);
    assert(g_randubigint);
    g_randfloat = calloc(1, sizeof(float) * g_args.prepared_rand);
    assert(g_randfloat);
    g_randdouble = calloc(1, sizeof(double) * g_args.prepared_rand);
    assert(g_randdouble);

    for (int i = 0; i < g_args.prepared_rand; i++) {
        g_randint[i] = (int)(taosRandom() % RAND_MAX - (RAND_MAX >> 1));
        g_randuint[i] = (int)(taosRandom());
        sprintf(g_randint_buff + i * INT_BUFF_LEN, "%d",
                g_randint[i]);
        sprintf(g_rand_voltage_buff + i * INT_BUFF_LEN, "%d",
                215 + g_randint[i] % 10);

        sprintf(g_randbool_buff + i * BOOL_BUFF_LEN, "%s",
                ((g_randint[i] % 2) & 1)?"true":"false");
        sprintf(g_randsmallint_buff + i * SMALLINT_BUFF_LEN, "%d",
                g_randint[i] % 32768);
        sprintf(g_randtinyint_buff + i * TINYINT_BUFF_LEN, "%d",
                g_randint[i] % 128);
        sprintf(g_randuint_buff + i * INT_BUFF_LEN, "%d",
                g_randuint[i]);
        sprintf(g_randusmallint_buff + i * SMALLINT_BUFF_LEN, "%d",
                g_randuint[i] % 65535);
        sprintf(g_randutinyint_buff + i * TINYINT_BUFF_LEN, "%d",
                g_randuint[i] % 255);

        g_randbigint[i] = (int64_t)(taosRandom() % RAND_MAX - (RAND_MAX >> 1));
        g_randubigint[i] = (uint64_t)(taosRandom());
        sprintf(g_randbigint_buff + i * BIGINT_BUFF_LEN, "%"PRId64"",
                g_randbigint[i]);
        sprintf(g_randubigint_buff + i * BIGINT_BUFF_LEN, "%"PRId64"",
                g_randubigint[i]);

        g_randfloat[i] = (float)(taosRandom() / 1000.0) * (taosRandom() % 2 > 0.5 ? 1 : -1);
        sprintf(g_randfloat_buff + i * FLOAT_BUFF_LEN, "%f",
                g_randfloat[i]);
        sprintf(g_rand_current_buff + i * FLOAT_BUFF_LEN, "%f",
                (float)(9.8 + 0.04 * (g_randint[i] % 10)
                    + g_randfloat[i]/1000000000));
        sprintf(g_rand_phase_buff + i * FLOAT_BUFF_LEN, "%f",
                (float)((115 + g_randint[i] % 10
                        + g_randfloat[i]/1000000000)/360));

        g_randdouble[i] = (double)(taosRandom() / 1000000.0) * (taosRandom() % 2 > 0.5 ? 1 : -1);
        sprintf(g_randdouble_buff + i * DOUBLE_BUFF_LEN, "%f",
                g_randdouble[i]);
    }
}

#define SHOW_PARSE_RESULT_START()   \
    do { if (g_args.metaFile)  \
        printf("\033[1m\033[40;32m================ %s parse result START ================\033[0m\n", \
                g_args.metaFile); } while(0)

#define SHOW_PARSE_RESULT_END() \
    do { if (g_args.metaFile)   \
        printf("\033[1m\033[40;32m================ %s parse result END================\033[0m\n", \
                g_args.metaFile); } while(0)

#define SHOW_PARSE_RESULT_START_TO_FILE(fp)   \
    do { if (g_args.metaFile)  \
        fprintf(fp, "\033[1m\033[40;32m================ %s parse result START ================\033[0m\n", \
                g_args.metaFile); } while(0)

#define SHOW_PARSE_RESULT_END_TO_FILE(fp) \
    do { if (g_args.metaFile)   \
        fprintf(fp, "\033[1m\033[40;32m================ %s parse result END================\033[0m\n", \
                g_args.metaFile); } while(0)

static int printfInsertMeta() {
    SHOW_PARSE_RESULT_START();

    if (g_args.demo_mode) {
        printf("\ntaosdemo is simulating data generated by power equipment monitoring...\n\n");
    } else {
        printf("\ntaosdemo is simulating random data as you request..\n\n");
    }

    if (g_args.iface != INTERFACE_BUT) {
        // first time if no iface specified
        printf("interface:                  \033[33m%s\033[0m\n",
                (g_args.iface==TAOSC_IFACE)?"taosc":
                (g_args.iface==REST_IFACE)?"rest":"stmt");
    }

    printf("host:                       \033[33m%s:%u\033[0m\n",
            g_Dbs.host, g_Dbs.port);
    printf("user:                       \033[33m%s\033[0m\n", g_Dbs.user);
    printf("password:                   \033[33m%s\033[0m\n", g_Dbs.password);
    printf("configDir:                  \033[33m%s\033[0m\n", configDir);
    printf("resultFile:                 \033[33m%s\033[0m\n", g_Dbs.resultFile);
    printf("thread num of insert data:  \033[33m%d\033[0m\n", g_Dbs.threadCount);
    printf("thread num of create table: \033[33m%d\033[0m\n",
            g_Dbs.threadCountForCreateTbl);
    printf("top insert interval:        \033[33m%"PRIu64"\033[0m\n",
            g_args.insert_interval);
    printf("number of records per req:  \033[33m%u\033[0m\n",
            g_args.reqPerReq);
    printf("max sql length:             \033[33m%"PRIu64"\033[0m\n",
            g_args.max_sql_len);

    printf("database count:             \033[33m%d\033[0m\n", g_Dbs.dbCount);

    for (int i = 0; i < g_Dbs.dbCount; i++) {
        printf("database[\033[33m%d\033[0m]:\n", i);
        printf("  database[%d] name:      \033[33m%s\033[0m\n",
                i, g_Dbs.db[i].dbName);
        if (0 == g_Dbs.db[i].drop) {
            printf("  drop:                  \033[33m no\033[0m\n");
        } else {
            printf("  drop:                  \033[33m yes\033[0m\n");
        }

        if (g_Dbs.db[i].dbCfg.blocks > 0) {
            printf("  blocks:                \033[33m%d\033[0m\n",
                    g_Dbs.db[i].dbCfg.blocks);
        }
        if (g_Dbs.db[i].dbCfg.cache > 0) {
            printf("  cache:                 \033[33m%d\033[0m\n",
                    g_Dbs.db[i].dbCfg.cache);
        }
        if (g_Dbs.db[i].dbCfg.days > 0) {
            printf("  days:                  \033[33m%d\033[0m\n",
                    g_Dbs.db[i].dbCfg.days);
        }
        if (g_Dbs.db[i].dbCfg.keep > 0) {
            printf("  keep:                  \033[33m%d\033[0m\n",
                    g_Dbs.db[i].dbCfg.keep);
        }
        if (g_Dbs.db[i].dbCfg.replica > 0) {
            printf("  replica:               \033[33m%d\033[0m\n",
                    g_Dbs.db[i].dbCfg.replica);
        }
        if (g_Dbs.db[i].dbCfg.update > 0) {
            printf("  update:                \033[33m%d\033[0m\n",
                    g_Dbs.db[i].dbCfg.update);
        }
        if (g_Dbs.db[i].dbCfg.minRows > 0) {
            printf("  minRows:               \033[33m%d\033[0m\n",
                    g_Dbs.db[i].dbCfg.minRows);
        }
        if (g_Dbs.db[i].dbCfg.maxRows > 0) {
            printf("  maxRows:               \033[33m%d\033[0m\n",
                    g_Dbs.db[i].dbCfg.maxRows);
        }
        if (g_Dbs.db[i].dbCfg.comp > 0) {
            printf("  comp:                  \033[33m%d\033[0m\n", g_Dbs.db[i].dbCfg.comp);
        }
        if (g_Dbs.db[i].dbCfg.walLevel > 0) {
            printf("  walLevel:              \033[33m%d\033[0m\n",
                    g_Dbs.db[i].dbCfg.walLevel);
        }
        if (g_Dbs.db[i].dbCfg.fsync > 0) {
            printf("  fsync:                 \033[33m%d\033[0m\n",
                    g_Dbs.db[i].dbCfg.fsync);
        }
        if (g_Dbs.db[i].dbCfg.quorum > 0) {
            printf("  quorum:                \033[33m%d\033[0m\n",
                    g_Dbs.db[i].dbCfg.quorum);
        }
        if (g_Dbs.db[i].dbCfg.precision[0] != 0) {
            if ((0 == strncasecmp(g_Dbs.db[i].dbCfg.precision, "ms", 2))
                    || (0 == strncasecmp(g_Dbs.db[i].dbCfg.precision, "us", 2))
                    || (0 == strncasecmp(g_Dbs.db[i].dbCfg.precision, "ns", 2))) {
                printf("  precision:             \033[33m%s\033[0m\n",
                        g_Dbs.db[i].dbCfg.precision);
            } else {
                printf("\033[1m\033[40;31m  precision error:       %s\033[0m\n",
                        g_Dbs.db[i].dbCfg.precision);
                return -1;
            }
        }


        if (g_args.use_metric) {
            printf("  super table count:     \033[33m%"PRIu64"\033[0m\n",
                g_Dbs.db[i].superTblCount);
            for (uint64_t j = 0; j < g_Dbs.db[i].superTblCount; j++) {
                printf("  super table[\033[33m%"PRIu64"\033[0m]:\n", j);

                printf("      stbName:           \033[33m%s\033[0m\n",
                        g_Dbs.db[i].superTbls[j].stbName);

                if (PRE_CREATE_SUBTBL == g_Dbs.db[i].superTbls[j].autoCreateTable) {
                    printf("      autoCreateTable:   \033[33m%s\033[0m\n",  "no");
                } else if (AUTO_CREATE_SUBTBL ==
                        g_Dbs.db[i].superTbls[j].autoCreateTable) {
                    printf("      autoCreateTable:   \033[33m%s\033[0m\n",  "yes");
                } else {
                    printf("      autoCreateTable:   \033[33m%s\033[0m\n",  "error");
                }

                if (TBL_NO_EXISTS == g_Dbs.db[i].superTbls[j].childTblExists) {
                    printf("      childTblExists:    \033[33m%s\033[0m\n",  "no");
                } else if (TBL_ALREADY_EXISTS == g_Dbs.db[i].superTbls[j].childTblExists) {
                    printf("      childTblExists:    \033[33m%s\033[0m\n",  "yes");
                } else {
                    printf("      childTblExists:    \033[33m%s\033[0m\n",  "error");
                }

                printf("      childTblCount:     \033[33m%"PRId64"\033[0m\n",
                        g_Dbs.db[i].superTbls[j].childTblCount);
                printf("      childTblPrefix:    \033[33m%s\033[0m\n",
                        g_Dbs.db[i].superTbls[j].childTblPrefix);
                printf("      dataSource:        \033[33m%s\033[0m\n",
                        g_Dbs.db[i].superTbls[j].dataSource);
                printf("      iface:             \033[33m%s\033[0m\n",
                        (g_Dbs.db[i].superTbls[j].iface==TAOSC_IFACE)?"taosc":
                        (g_Dbs.db[i].superTbls[j].iface==REST_IFACE)?"rest":"stmt");
                if (g_Dbs.db[i].superTbls[j].childTblLimit > 0) {
                    printf("      childTblLimit:     \033[33m%"PRId64"\033[0m\n",
                            g_Dbs.db[i].superTbls[j].childTblLimit);
                }
                if (g_Dbs.db[i].superTbls[j].childTblOffset > 0) {
                    printf("      childTblOffset:    \033[33m%"PRIu64"\033[0m\n",
                            g_Dbs.db[i].superTbls[j].childTblOffset);
                }
                printf("      insertRows:        \033[33m%"PRId64"\033[0m\n",
                        g_Dbs.db[i].superTbls[j].insertRows);
                /*
                if (0 == g_Dbs.db[i].superTbls[j].multiThreadWriteOneTbl) {
                printf("      multiThreadWriteOneTbl:  \033[33m no\033[0m\n");
                }else {
                printf("      multiThreadWriteOneTbl:  \033[33m yes\033[0m\n");
                }
                */
                printf("      interlaceRows:     \033[33m%u\033[0m\n",
                        g_Dbs.db[i].superTbls[j].interlaceRows);

                if (g_Dbs.db[i].superTbls[j].interlaceRows > 0) {
                    printf("      stable insert interval:   \033[33m%"PRIu64"\033[0m\n",
                            g_Dbs.db[i].superTbls[j].insertInterval);
                }

                printf("      disorderRange:     \033[33m%d\033[0m\n",
                        g_Dbs.db[i].superTbls[j].disorderRange);
                printf("      disorderRatio:     \033[33m%d\033[0m\n",
                        g_Dbs.db[i].superTbls[j].disorderRatio);
                printf("      maxSqlLen:         \033[33m%"PRIu64"\033[0m\n",
                        g_Dbs.db[i].superTbls[j].maxSqlLen);
                printf("      timeStampStep:     \033[33m%"PRId64"\033[0m\n",
                        g_Dbs.db[i].superTbls[j].timeStampStep);
                printf("      startTimestamp:    \033[33m%s\033[0m\n",
                        g_Dbs.db[i].superTbls[j].startTimestamp);
                printf("      sampleFormat:      \033[33m%s\033[0m\n",
                        g_Dbs.db[i].superTbls[j].sampleFormat);
                printf("      sampleFile:        \033[33m%s\033[0m\n",
                        g_Dbs.db[i].superTbls[j].sampleFile);
                printf("      useSampleTs:       \033[33m%s\033[0m\n",
                        g_Dbs.db[i].superTbls[j].useSampleTs ? "yes (warning: disorderRange/disorderRatio is disabled)" : "no");
                printf("      tagsFile:          \033[33m%s\033[0m\n",
                        g_Dbs.db[i].superTbls[j].tagsFile);
                printf("      columnCount:       \033[33m%d\033[0m\n        ",
                        g_Dbs.db[i].superTbls[j].columnCount);
                for (int k = 0; k < g_Dbs.db[i].superTbls[j].columnCount; k++) {
                    //printf("dataType:%s, dataLen:%d\t", g_Dbs.db[i].superTbls[j].columns[k].dataType, g_Dbs.db[i].superTbls[j].columns[k].dataLen);
                    if ((0 == strncasecmp(g_Dbs.db[i].superTbls[j].columns[k].dataType,
                                    "binary", 6))
                            || (0 == strncasecmp(g_Dbs.db[i].superTbls[j].columns[k].dataType,
                                    "nchar", 5))) {
                        printf("column[%d]:\033[33m%s(%d)\033[0m ", k,
                                g_Dbs.db[i].superTbls[j].columns[k].dataType,
                                g_Dbs.db[i].superTbls[j].columns[k].dataLen);
                    } else {
                        printf("column[%d]:\033[33m%s\033[0m ", k,
                                g_Dbs.db[i].superTbls[j].columns[k].dataType);
                    }
                }
                printf("\n");

                printf("      tagCount:            \033[33m%d\033[0m\n        ",
                        g_Dbs.db[i].superTbls[j].tagCount);
                for (int k = 0; k < g_Dbs.db[i].superTbls[j].tagCount; k++) {
                    //printf("dataType:%s, dataLen:%d\t", g_Dbs.db[i].superTbls[j].tags[k].dataType, g_Dbs.db[i].superTbls[j].tags[k].dataLen);
                    if ((0 == strncasecmp(g_Dbs.db[i].superTbls[j].tags[k].dataType,
                                    "binary", strlen("binary")))
                            || (0 == strncasecmp(g_Dbs.db[i].superTbls[j].tags[k].dataType,
                                    "nchar", strlen("nchar")))) {
                        printf("tag[%d]:\033[33m%s(%d)\033[0m ", k,
                                g_Dbs.db[i].superTbls[j].tags[k].dataType,
                                g_Dbs.db[i].superTbls[j].tags[k].dataLen);
                    } else {
                        printf("tag[%d]:\033[33m%s\033[0m ", k,
                                g_Dbs.db[i].superTbls[j].tags[k].dataType);
                    }
                }
                printf("\n");
            }
        } else {
            printf("  childTblCount:     \033[33m%"PRId64"\033[0m\n",
                        g_args.ntables);
            printf("  insertRows:        \033[33m%"PRId64"\033[0m\n",
                        g_args.insertRows);
        }
        printf("\n");
    }

    SHOW_PARSE_RESULT_END();

    return 0;
}

static void printfInsertMetaToFile(FILE* fp) {

    SHOW_PARSE_RESULT_START_TO_FILE(fp);

    fprintf(fp, "host:                       %s:%u\n", g_Dbs.host, g_Dbs.port);
    fprintf(fp, "user:                       %s\n", g_Dbs.user);
    fprintf(fp, "configDir:                  %s\n", configDir);
    fprintf(fp, "resultFile:                 %s\n", g_Dbs.resultFile);
    fprintf(fp, "thread num of insert data:  %d\n", g_Dbs.threadCount);
    fprintf(fp, "thread num of create table: %d\n", g_Dbs.threadCountForCreateTbl);
    fprintf(fp, "number of records per req:  %u\n", g_args.reqPerReq);
    fprintf(fp, "max sql length:             %"PRIu64"\n", g_args.max_sql_len);
    fprintf(fp, "database count:          %d\n", g_Dbs.dbCount);

    for (int i = 0; i < g_Dbs.dbCount; i++) {
        fprintf(fp, "database[%d]:\n", i);
        fprintf(fp, "  database[%d] name:       %s\n", i, g_Dbs.db[i].dbName);
        if (0 == g_Dbs.db[i].drop) {
            fprintf(fp, "  drop:                  no\n");
        }else {
            fprintf(fp, "  drop:                  yes\n");
        }

        if (g_Dbs.db[i].dbCfg.blocks > 0) {
            fprintf(fp, "  blocks:                %d\n", g_Dbs.db[i].dbCfg.blocks);
        }
        if (g_Dbs.db[i].dbCfg.cache > 0) {
            fprintf(fp, "  cache:                 %d\n", g_Dbs.db[i].dbCfg.cache);
        }
        if (g_Dbs.db[i].dbCfg.days > 0) {
            fprintf(fp, "  days:                  %d\n", g_Dbs.db[i].dbCfg.days);
        }
        if (g_Dbs.db[i].dbCfg.keep > 0) {
            fprintf(fp, "  keep:                  %d\n", g_Dbs.db[i].dbCfg.keep);
        }
        if (g_Dbs.db[i].dbCfg.replica > 0) {
            fprintf(fp, "  replica:               %d\n", g_Dbs.db[i].dbCfg.replica);
        }
        if (g_Dbs.db[i].dbCfg.update > 0) {
            fprintf(fp, "  update:                %d\n", g_Dbs.db[i].dbCfg.update);
        }
        if (g_Dbs.db[i].dbCfg.minRows > 0) {
            fprintf(fp, "  minRows:               %d\n", g_Dbs.db[i].dbCfg.minRows);
        }
        if (g_Dbs.db[i].dbCfg.maxRows > 0) {
            fprintf(fp, "  maxRows:               %d\n", g_Dbs.db[i].dbCfg.maxRows);
        }
        if (g_Dbs.db[i].dbCfg.comp > 0) {
            fprintf(fp, "  comp:                  %d\n", g_Dbs.db[i].dbCfg.comp);
        }
        if (g_Dbs.db[i].dbCfg.walLevel > 0) {
            fprintf(fp, "  walLevel:              %d\n", g_Dbs.db[i].dbCfg.walLevel);
        }
        if (g_Dbs.db[i].dbCfg.fsync > 0) {
            fprintf(fp, "  fsync:                 %d\n", g_Dbs.db[i].dbCfg.fsync);
        }
        if (g_Dbs.db[i].dbCfg.quorum > 0) {
            fprintf(fp, "  quorum:                %d\n", g_Dbs.db[i].dbCfg.quorum);
        }
        if (g_Dbs.db[i].dbCfg.precision[0] != 0) {
            if ((0 == strncasecmp(g_Dbs.db[i].dbCfg.precision, "ms", 2))
                    || (0 == strncasecmp(g_Dbs.db[i].dbCfg.precision, "ns", 2))
                    || (0 == strncasecmp(g_Dbs.db[i].dbCfg.precision, "us", 2))) {
                fprintf(fp, "  precision:             %s\n",
                        g_Dbs.db[i].dbCfg.precision);
            } else {
                fprintf(fp, "  precision error:       %s\n",
                        g_Dbs.db[i].dbCfg.precision);
            }
        }

        fprintf(fp, "  super table count:     %"PRIu64"\n",
                g_Dbs.db[i].superTblCount);
        for (int j = 0; j < g_Dbs.db[i].superTblCount; j++) {
            fprintf(fp, "  super table[%d]:\n", j);

            fprintf(fp, "      stbName:           %s\n",
                    g_Dbs.db[i].superTbls[j].stbName);

            if (PRE_CREATE_SUBTBL == g_Dbs.db[i].superTbls[j].autoCreateTable) {
                fprintf(fp, "      autoCreateTable:   %s\n",  "no");
            } else if (AUTO_CREATE_SUBTBL
                    == g_Dbs.db[i].superTbls[j].autoCreateTable) {
                fprintf(fp, "      autoCreateTable:   %s\n",  "yes");
            } else {
                fprintf(fp, "      autoCreateTable:   %s\n",  "error");
            }

            if (TBL_NO_EXISTS == g_Dbs.db[i].superTbls[j].childTblExists) {
                fprintf(fp, "      childTblExists:    %s\n",  "no");
            } else if (TBL_ALREADY_EXISTS
                    == g_Dbs.db[i].superTbls[j].childTblExists) {
                fprintf(fp, "      childTblExists:    %s\n",  "yes");
            } else {
                fprintf(fp, "      childTblExists:    %s\n",  "error");
            }

            fprintf(fp, "      childTblCount:     %"PRId64"\n",
                    g_Dbs.db[i].superTbls[j].childTblCount);
            fprintf(fp, "      childTblPrefix:    %s\n",
                    g_Dbs.db[i].superTbls[j].childTblPrefix);
            fprintf(fp, "      dataSource:        %s\n",
                    g_Dbs.db[i].superTbls[j].dataSource);
            fprintf(fp, "      iface:             %s\n",
                    (g_Dbs.db[i].superTbls[j].iface==TAOSC_IFACE)?"taosc":
                    (g_Dbs.db[i].superTbls[j].iface==REST_IFACE)?"rest":"stmt");
            fprintf(fp, "      insertRows:        %"PRId64"\n",
                    g_Dbs.db[i].superTbls[j].insertRows);
            fprintf(fp, "      interlace rows:    %u\n",
                    g_Dbs.db[i].superTbls[j].interlaceRows);
            if (g_Dbs.db[i].superTbls[j].interlaceRows > 0) {
                fprintf(fp, "      stable insert interval:   %"PRIu64"\n",
                        g_Dbs.db[i].superTbls[j].insertInterval);
            }
            /*
               if (0 == g_Dbs.db[i].superTbls[j].multiThreadWriteOneTbl) {
               fprintf(fp, "      multiThreadWriteOneTbl:  no\n");
               }else {
               fprintf(fp, "      multiThreadWriteOneTbl:  yes\n");
               }
               */
            fprintf(fp, "      interlaceRows:     %u\n",
                    g_Dbs.db[i].superTbls[j].interlaceRows);
            fprintf(fp, "      disorderRange:     %d\n",
                    g_Dbs.db[i].superTbls[j].disorderRange);
            fprintf(fp, "      disorderRatio:     %d\n",
                    g_Dbs.db[i].superTbls[j].disorderRatio);
            fprintf(fp, "      maxSqlLen:         %"PRIu64"\n",
                    g_Dbs.db[i].superTbls[j].maxSqlLen);

            fprintf(fp, "      timeStampStep:     %"PRId64"\n",
                    g_Dbs.db[i].superTbls[j].timeStampStep);
            fprintf(fp, "      startTimestamp:    %s\n",
                    g_Dbs.db[i].superTbls[j].startTimestamp);
            fprintf(fp, "      sampleFormat:      %s\n",
                    g_Dbs.db[i].superTbls[j].sampleFormat);
            fprintf(fp, "      sampleFile:        %s\n",
                    g_Dbs.db[i].superTbls[j].sampleFile);
            fprintf(fp, "      tagsFile:          %s\n",
                    g_Dbs.db[i].superTbls[j].tagsFile);

            fprintf(fp, "      columnCount:       %d\n        ",
                    g_Dbs.db[i].superTbls[j].columnCount);
            for (int k = 0; k < g_Dbs.db[i].superTbls[j].columnCount; k++) {
                //printf("dataType:%s, dataLen:%d\t", g_Dbs.db[i].superTbls[j].columns[k].dataType, g_Dbs.db[i].superTbls[j].columns[k].dataLen);
                if ((0 == strncasecmp(
                                g_Dbs.db[i].superTbls[j].columns[k].dataType,
                                "binary", strlen("binary")))
                        || (0 == strncasecmp(
                                g_Dbs.db[i].superTbls[j].columns[k].dataType,
                                "nchar", strlen("nchar")))) {
                    fprintf(fp, "column[%d]:%s(%d) ", k,
                            g_Dbs.db[i].superTbls[j].columns[k].dataType,
                            g_Dbs.db[i].superTbls[j].columns[k].dataLen);
                } else {
                    fprintf(fp, "column[%d]:%s ",
                            k, g_Dbs.db[i].superTbls[j].columns[k].dataType);
                }
            }
            fprintf(fp, "\n");

            fprintf(fp, "      tagCount:            %d\n        ",
                    g_Dbs.db[i].superTbls[j].tagCount);
            for (int k = 0; k < g_Dbs.db[i].superTbls[j].tagCount; k++) {
                //printf("dataType:%s, dataLen:%d\t", g_Dbs.db[i].superTbls[j].tags[k].dataType, g_Dbs.db[i].superTbls[j].tags[k].dataLen);
                if ((0 == strncasecmp(g_Dbs.db[i].superTbls[j].tags[k].dataType,
                                "binary", strlen("binary")))
                        || (0 == strncasecmp(g_Dbs.db[i].superTbls[j].tags[k].dataType,
                                "nchar", strlen("nchar")))) {
                    fprintf(fp, "tag[%d]:%s(%d) ",
                            k, g_Dbs.db[i].superTbls[j].tags[k].dataType,
                            g_Dbs.db[i].superTbls[j].tags[k].dataLen);
                } else {
                    fprintf(fp, "tag[%d]:%s ", k, g_Dbs.db[i].superTbls[j].tags[k].dataType);
                }
            }
            fprintf(fp, "\n");
        }
        fprintf(fp, "\n");
    }

    SHOW_PARSE_RESULT_END_TO_FILE(fp);
}

static void printfQueryMeta() {

    SHOW_PARSE_RESULT_START();

    printf("host:                    \033[33m%s:%u\033[0m\n",
            g_queryInfo.host, g_queryInfo.port);
    printf("user:                    \033[33m%s\033[0m\n", g_queryInfo.user);
    printf("database name:           \033[33m%s\033[0m\n", g_queryInfo.dbName);

    printf("\n");

    if ((SUBSCRIBE_TEST == g_args.test_mode) || (QUERY_TEST == g_args.test_mode)) {
        printf("specified table query info:                   \n");
        printf("sqlCount:       \033[33m%d\033[0m\n",
                g_queryInfo.specifiedQueryInfo.sqlCount);
        if (g_queryInfo.specifiedQueryInfo.sqlCount > 0) {
            printf("specified tbl query times:\n");
            printf("                \033[33m%"PRIu64"\033[0m\n",
                    g_queryInfo.specifiedQueryInfo.queryTimes);
            printf("query interval: \033[33m%"PRIu64" ms\033[0m\n",
                    g_queryInfo.specifiedQueryInfo.queryInterval);
            printf("top query times:\033[33m%"PRIu64"\033[0m\n", g_args.query_times);
            printf("concurrent:     \033[33m%d\033[0m\n",
                    g_queryInfo.specifiedQueryInfo.concurrent);
            printf("mod:            \033[33m%s\033[0m\n",
                    (g_queryInfo.specifiedQueryInfo.asyncMode)?"async":"sync");
            printf("interval:       \033[33m%"PRIu64"\033[0m\n",
                    g_queryInfo.specifiedQueryInfo.subscribeInterval);
            printf("restart:        \033[33m%d\033[0m\n",
                    g_queryInfo.specifiedQueryInfo.subscribeRestart);
            printf("keepProgress:   \033[33m%d\033[0m\n",
                    g_queryInfo.specifiedQueryInfo.subscribeKeepProgress);

            for (int i = 0; i < g_queryInfo.specifiedQueryInfo.sqlCount; i++) {
                printf("  sql[%d]: \033[33m%s\033[0m\n",
                        i, g_queryInfo.specifiedQueryInfo.sql[i]);
            }
            printf("\n");
        }

        printf("super table query info:\n");
        printf("sqlCount:       \033[33m%d\033[0m\n",
                g_queryInfo.superQueryInfo.sqlCount);

        if (g_queryInfo.superQueryInfo.sqlCount > 0) {
            printf("query interval: \033[33m%"PRIu64"\033[0m\n",
                    g_queryInfo.superQueryInfo.queryInterval);
            printf("threadCnt:      \033[33m%d\033[0m\n",
                    g_queryInfo.superQueryInfo.threadCnt);
            printf("childTblCount:  \033[33m%"PRId64"\033[0m\n",
                    g_queryInfo.superQueryInfo.childTblCount);
            printf("stable name:    \033[33m%s\033[0m\n",
                    g_queryInfo.superQueryInfo.stbName);
            printf("stb query times:\033[33m%"PRIu64"\033[0m\n",
                    g_queryInfo.superQueryInfo.queryTimes);

            printf("mod:            \033[33m%s\033[0m\n",
                    (g_queryInfo.superQueryInfo.asyncMode)?"async":"sync");
            printf("interval:       \033[33m%"PRIu64"\033[0m\n",
                    g_queryInfo.superQueryInfo.subscribeInterval);
            printf("restart:        \033[33m%d\033[0m\n",
                    g_queryInfo.superQueryInfo.subscribeRestart);
            printf("keepProgress:   \033[33m%d\033[0m\n",
                    g_queryInfo.superQueryInfo.subscribeKeepProgress);

            for (int i = 0; i < g_queryInfo.superQueryInfo.sqlCount; i++) {
                printf("  sql[%d]: \033[33m%s\033[0m\n",
                        i, g_queryInfo.superQueryInfo.sql[i]);
            }
            printf("\n");
        }
    }

    SHOW_PARSE_RESULT_END();
}

static char* formatTimestamp(char* buf, int64_t val, int precision) {
    time_t tt;
    if (precision == TSDB_TIME_PRECISION_MICRO) {
        tt = (time_t)(val / 1000000);
    } if (precision == TSDB_TIME_PRECISION_NANO) {
        tt = (time_t)(val / 1000000000);
    } else {
        tt = (time_t)(val / 1000);
    }

    /* comment out as it make testcases like select_with_tags.sim fail.
       but in windows, this may cause the call to localtime crash if tt < 0,
       need to find a better solution.
       if (tt < 0) {
       tt = 0;
       }
       */

#ifdef WINDOWS
    if (tt < 0) tt = 0;
#endif

    struct tm* ptm = localtime(&tt);
    size_t pos = strftime(buf, 32, "%Y-%m-%d %H:%M:%S", ptm);

    if (precision == TSDB_TIME_PRECISION_MICRO) {
        sprintf(buf + pos, ".%06d", (int)(val % 1000000));
    } else if (precision == TSDB_TIME_PRECISION_NANO) {
        sprintf(buf + pos, ".%09d", (int)(val % 1000000000));
    } else {
        sprintf(buf + pos, ".%03d", (int)(val % 1000));
    }

    return buf;
}

static void xDumpFieldToFile(FILE* fp, const char* val,
        TAOS_FIELD* field, int32_t length, int precision) {

    if (val == NULL) {
        fprintf(fp, "%s", TSDB_DATA_NULL_STR);
        return;
    }

    char buf[TSDB_MAX_BYTES_PER_ROW];
    switch (field->type) {
        case TSDB_DATA_TYPE_BOOL:
            fprintf(fp, "%d", ((((int32_t)(*((int8_t*)val))) == 1) ? 1 : 0));
            break;

        case TSDB_DATA_TYPE_TINYINT:
            fprintf(fp, "%d", *((int8_t *)val));
            break;

        case TSDB_DATA_TYPE_UTINYINT:
            fprintf(fp, "%d", *((uint8_t *)val));
            break;

        case TSDB_DATA_TYPE_SMALLINT:
            fprintf(fp, "%d", *((int16_t *)val));
            break;

        case TSDB_DATA_TYPE_USMALLINT:
            fprintf(fp, "%d", *((uint16_t *)val));
            break;

        case TSDB_DATA_TYPE_INT:
            fprintf(fp, "%d", *((int32_t *)val));
            break;

        case TSDB_DATA_TYPE_UINT:
            fprintf(fp, "%d", *((uint32_t *)val));
            break;

        case TSDB_DATA_TYPE_BIGINT:
            fprintf(fp, "%"PRId64"", *((int64_t *)val));
            break;

        case TSDB_DATA_TYPE_UBIGINT:
            fprintf(fp, "%"PRId64"", *((uint64_t *)val));
            break;

        case TSDB_DATA_TYPE_FLOAT:
            fprintf(fp, "%.5f", GET_FLOAT_VAL(val));
            break;

        case TSDB_DATA_TYPE_DOUBLE:
            fprintf(fp, "%.9f", GET_DOUBLE_VAL(val));
            break;

        case TSDB_DATA_TYPE_BINARY:
        case TSDB_DATA_TYPE_NCHAR:
            memcpy(buf, val, length);
            buf[length] = 0;
            fprintf(fp, "\'%s\'", buf);
            break;

        case TSDB_DATA_TYPE_TIMESTAMP:
            formatTimestamp(buf, *(int64_t*)val, precision);
            fprintf(fp, "'%s'", buf);
            break;

        default:
            break;
    }
}

static int xDumpResultToFile(const char* fname, TAOS_RES* tres) {
    TAOS_ROW row = taos_fetch_row(tres);
    if (row == NULL) {
        return 0;
    }

    FILE* fp = fopen(fname, "at");
    if (fp == NULL) {
        errorPrint2("%s() LN%d, failed to open file: %s\n",
                __func__, __LINE__, fname);
        return -1;
    }

    int num_fields = taos_num_fields(tres);
    TAOS_FIELD *fields = taos_fetch_fields(tres);
    int precision = taos_result_precision(tres);

    for (int col = 0; col < num_fields; col++) {
        if (col > 0) {
            fprintf(fp, ",");
        }
        fprintf(fp, "%s", fields[col].name);
    }
    fputc('\n', fp);

    int numOfRows = 0;
    do {
        int32_t* length = taos_fetch_lengths(tres);
        for (int i = 0; i < num_fields; i++) {
            if (i > 0) {
                fputc(',', fp);
            }
            xDumpFieldToFile(fp,
                    (const char*)row[i], fields +i, length[i], precision);
        }
        fputc('\n', fp);

        numOfRows++;
        row = taos_fetch_row(tres);
    } while( row != NULL);

    fclose(fp);

    return numOfRows;
}

static int getDbFromServer(TAOS * taos, SDbInfo** dbInfos) {
    TAOS_RES * res;
    TAOS_ROW row = NULL;
    int count = 0;

    res = taos_query(taos, "show databases;");
    int32_t code = taos_errno(res);

    if (code != 0) {
        errorPrint2("failed to run <show databases>, reason: %s\n",
                taos_errstr(res));
        return -1;
    }

    TAOS_FIELD *fields = taos_fetch_fields(res);

    while((row = taos_fetch_row(res)) != NULL) {
        // sys database name : 'log'
        if (strncasecmp(row[TSDB_SHOW_DB_NAME_INDEX], "log",
                    fields[TSDB_SHOW_DB_NAME_INDEX].bytes) == 0) {
            continue;
        }

        dbInfos[count] = (SDbInfo *)calloc(1, sizeof(SDbInfo));
        if (dbInfos[count] == NULL) {
            errorPrint2("failed to allocate memory for some dbInfo[%d]\n", count);
            return -1;
        }

        tstrncpy(dbInfos[count]->name, (char *)row[TSDB_SHOW_DB_NAME_INDEX],
                fields[TSDB_SHOW_DB_NAME_INDEX].bytes);
        formatTimestamp(dbInfos[count]->create_time,
                *(int64_t*)row[TSDB_SHOW_DB_CREATED_TIME_INDEX],
                TSDB_TIME_PRECISION_MILLI);
        dbInfos[count]->ntables = *((int64_t *)row[TSDB_SHOW_DB_NTABLES_INDEX]);
        dbInfos[count]->vgroups = *((int32_t *)row[TSDB_SHOW_DB_VGROUPS_INDEX]);
        dbInfos[count]->replica = *((int16_t *)row[TSDB_SHOW_DB_REPLICA_INDEX]);
        dbInfos[count]->quorum = *((int16_t *)row[TSDB_SHOW_DB_QUORUM_INDEX]);
        dbInfos[count]->days = *((int16_t *)row[TSDB_SHOW_DB_DAYS_INDEX]);

        tstrncpy(dbInfos[count]->keeplist, (char *)row[TSDB_SHOW_DB_KEEP_INDEX],
                fields[TSDB_SHOW_DB_KEEP_INDEX].bytes);
        dbInfos[count]->cache = *((int32_t *)row[TSDB_SHOW_DB_CACHE_INDEX]);
        dbInfos[count]->blocks = *((int32_t *)row[TSDB_SHOW_DB_BLOCKS_INDEX]);
        dbInfos[count]->minrows = *((int32_t *)row[TSDB_SHOW_DB_MINROWS_INDEX]);
        dbInfos[count]->maxrows = *((int32_t *)row[TSDB_SHOW_DB_MAXROWS_INDEX]);
        dbInfos[count]->wallevel = *((int8_t *)row[TSDB_SHOW_DB_WALLEVEL_INDEX]);
        dbInfos[count]->fsync = *((int32_t *)row[TSDB_SHOW_DB_FSYNC_INDEX]);
        dbInfos[count]->comp = (int8_t)(*((int8_t *)row[TSDB_SHOW_DB_COMP_INDEX]));
        dbInfos[count]->cachelast =
            (int8_t)(*((int8_t *)row[TSDB_SHOW_DB_CACHELAST_INDEX]));

        tstrncpy(dbInfos[count]->precision,
                (char *)row[TSDB_SHOW_DB_PRECISION_INDEX],
                fields[TSDB_SHOW_DB_PRECISION_INDEX].bytes);
        dbInfos[count]->update = *((int8_t *)row[TSDB_SHOW_DB_UPDATE_INDEX]);
        tstrncpy(dbInfos[count]->status, (char *)row[TSDB_SHOW_DB_STATUS_INDEX],
                fields[TSDB_SHOW_DB_STATUS_INDEX].bytes);

        count++;
        if (count > MAX_DATABASE_COUNT) {
            errorPrint("%s() LN%d, The database count overflow than %d\n",
                    __func__, __LINE__, MAX_DATABASE_COUNT);
            break;
        }
    }

    return count;
}

static void printfDbInfoForQueryToFile(
        char* filename, SDbInfo* dbInfos, int tdm_index) {

    if (filename[0] == 0)
        return;

    FILE *fp = fopen(filename, "at");
    if (fp == NULL) {
        errorPrint( "failed to open file: %s\n", filename);
        return;
    }

    fprintf(fp, "================ database[%d] ================\n", tdm_index);
    fprintf(fp, "name: %s\n", dbInfos->name);
    fprintf(fp, "created_time: %s\n", dbInfos->create_time);
    fprintf(fp, "ntables: %"PRId64"\n", dbInfos->ntables);
    fprintf(fp, "vgroups: %d\n", dbInfos->vgroups);
    fprintf(fp, "replica: %d\n", dbInfos->replica);
    fprintf(fp, "quorum: %d\n", dbInfos->quorum);
    fprintf(fp, "days: %d\n", dbInfos->days);
    fprintf(fp, "keep0,keep1,keep(D): %s\n", dbInfos->keeplist);
    fprintf(fp, "cache(MB): %d\n", dbInfos->cache);
    fprintf(fp, "blocks: %d\n", dbInfos->blocks);
    fprintf(fp, "minrows: %d\n", dbInfos->minrows);
    fprintf(fp, "maxrows: %d\n", dbInfos->maxrows);
    fprintf(fp, "wallevel: %d\n", dbInfos->wallevel);
    fprintf(fp, "fsync: %d\n", dbInfos->fsync);
    fprintf(fp, "comp: %d\n", dbInfos->comp);
    fprintf(fp, "cachelast: %d\n", dbInfos->cachelast);
    fprintf(fp, "precision: %s\n", dbInfos->precision);
    fprintf(fp, "update: %d\n", dbInfos->update);
    fprintf(fp, "status: %s\n", dbInfos->status);
    fprintf(fp, "\n");

    fclose(fp);
}

static void printfQuerySystemInfo(TAOS * taos) {
    char filename[MAX_FILE_NAME_LEN] = {0};
    char buffer[1024] = {0};
    TAOS_RES* res;

    time_t t;
    struct tm* lt;
    time(&t);
    lt = localtime(&t);
    snprintf(filename, MAX_FILE_NAME_LEN, "querySystemInfo-%d-%d-%d %d:%d:%d",
            lt->tm_year+1900, lt->tm_mon, lt->tm_mday, lt->tm_hour, lt->tm_min,
            lt->tm_sec);

    // show variables
    res = taos_query(taos, "show variables;");
    //fetchResult(res, filename);
    xDumpResultToFile(filename, res);

    // show dnodes
    res = taos_query(taos, "show dnodes;");
    xDumpResultToFile(filename, res);
    //fetchResult(res, filename);

    // show databases
    res = taos_query(taos, "show databases;");
    SDbInfo** dbInfos = (SDbInfo **)calloc(MAX_DATABASE_COUNT, sizeof(SDbInfo *));
    if (dbInfos == NULL) {
        errorPrint("%s() LN%d, failed to allocate memory\n", __func__, __LINE__);
        return;
    }
    int dbCount = getDbFromServer(taos, dbInfos);
    if (dbCount <= 0) {
        free(dbInfos);
        return;
    }

    for (int i = 0; i < dbCount; i++) {
        // printf database info
        printfDbInfoForQueryToFile(filename, dbInfos[i], i);

        // show db.vgroups
        snprintf(buffer, 1024, "show %s.vgroups;", dbInfos[i]->name);
        res = taos_query(taos, buffer);
        xDumpResultToFile(filename, res);

        // show db.stables
        snprintf(buffer, 1024, "show %s.stables;", dbInfos[i]->name);
        res = taos_query(taos, buffer);
        xDumpResultToFile(filename, res);
        free(dbInfos[i]);
    }

    free(dbInfos);
}

static int postProceSql(char *host, uint16_t port,
        char* sqlstr, threadInfo *pThreadInfo)
{
    char *req_fmt = "POST %s HTTP/1.1\r\nHost: %s:%d\r\nAccept: */*\r\nAuthorization: Basic %s\r\nContent-Length: %d\r\nContent-Type: application/x-www-form-urlencoded\r\n\r\n%s";

    char *url = "/rest/sql";

    int bytes, sent, received, req_str_len, resp_len;
    char *request_buf;
    char response_buf[RESP_BUF_LEN];
    uint16_t rest_port = port + TSDB_PORT_HTTP;

    int req_buf_len = strlen(sqlstr) + REQ_EXTRA_BUF_LEN;

    request_buf = malloc(req_buf_len);
    if (NULL == request_buf) {
        errorPrint("%s", "cannot allocate memory.\n");
        exit(EXIT_FAILURE);
    }

    char userpass_buf[INPUT_BUF_LEN];
    int mod_table[] = {0, 2, 1};

    static char base64[] = {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
        'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
        'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X',
        'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f',
        'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n',
        'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
        'w', 'x', 'y', 'z', '0', '1', '2', '3',
        '4', '5', '6', '7', '8', '9', '+', '/'};

    if (g_args.test_mode == INSERT_TEST) {
        snprintf(userpass_buf, INPUT_BUF_LEN, "%s:%s",
            g_Dbs.user, g_Dbs.password);
    } else {
        snprintf(userpass_buf, INPUT_BUF_LEN, "%s:%s",
            g_queryInfo.user, g_queryInfo.password);
    }
    
    size_t userpass_buf_len = strlen(userpass_buf);
    size_t encoded_len = 4 * ((userpass_buf_len +2) / 3);

    char base64_buf[INPUT_BUF_LEN];

    memset(base64_buf, 0, INPUT_BUF_LEN);

    for (int n = 0, m = 0; n < userpass_buf_len;) {
        uint32_t oct_a = n < userpass_buf_len ?
            (unsigned char) userpass_buf[n++]:0;
        uint32_t oct_b = n < userpass_buf_len ?
            (unsigned char) userpass_buf[n++]:0;
        uint32_t oct_c = n < userpass_buf_len ?
            (unsigned char) userpass_buf[n++]:0;
        uint32_t triple = (oct_a << 0x10) + (oct_b << 0x08) + oct_c;

        base64_buf[m++] = base64[(triple >> 3* 6) & 0x3f];
        base64_buf[m++] = base64[(triple >> 2* 6) & 0x3f];
        base64_buf[m++] = base64[(triple >> 1* 6) & 0x3f];
        base64_buf[m++] = base64[(triple >> 0* 6) & 0x3f];
    }

    for (int l = 0; l < mod_table[userpass_buf_len % 3]; l++)
        base64_buf[encoded_len - 1 - l] = '=';

    debugPrint("%s() LN%d: auth string base64 encoded: %s\n",
            __func__, __LINE__, base64_buf);
    char *auth = base64_buf;

    int r = snprintf(request_buf,
            req_buf_len,
            req_fmt, url, host, rest_port,
            auth, strlen(sqlstr), sqlstr);
    if (r >= req_buf_len) {
        free(request_buf);
        ERROR_EXIT("too long request");
    }
    verbosePrint("%s() LN%d: Request:\n%s\n", __func__, __LINE__, request_buf);

    req_str_len = strlen(request_buf);
    sent = 0;
    do {
#ifdef WINDOWS
        bytes = send(pThreadInfo->sockfd, request_buf + sent, req_str_len - sent, 0);
#else
        bytes = write(pThreadInfo->sockfd, request_buf + sent, req_str_len - sent);
#endif
        if (bytes < 0)
            ERROR_EXIT("writing message to socket");
        if (bytes == 0)
            break;
        sent+=bytes;
    } while(sent < req_str_len);

    memset(response_buf, 0, RESP_BUF_LEN);
    resp_len = sizeof(response_buf) - 1;
    received = 0;

    char resEncodingChunk[] = "Encoding: chunked";
    char resHttp[] = "HTTP/1.1 ";
    char resHttpOk[] = "HTTP/1.1 200 OK";

    do {
#ifdef WINDOWS
        bytes = recv(pThreadInfo->sockfd, response_buf + received, resp_len - received, 0);
#else
        bytes = read(pThreadInfo->sockfd, response_buf + received, resp_len - received);
#endif
        verbosePrint("%s() LN%d: bytes:%d\n", __func__, __LINE__, bytes);
        if (bytes < 0) {
            free(request_buf);
            ERROR_EXIT("reading response from socket");
        }
        if (bytes == 0)
            break;
        received += bytes;

        verbosePrint("%s() LN%d: received:%d resp_len:%d, response_buf:\n%s\n",
                __func__, __LINE__, received, resp_len, response_buf);

        response_buf[RESP_BUF_LEN - 1] = '\0';
        if (strlen(response_buf)) {
            if (((NULL == strstr(response_buf, resEncodingChunk))
                        && (NULL != strstr(response_buf, resHttp)))
                    || ((NULL != strstr(response_buf, resHttpOk))
                        && (NULL != strstr(response_buf, "\"status\":")))) {
                debugPrint(
                        "%s() LN%d: received:%d resp_len:%d, response_buf:\n%s\n",
                        __func__, __LINE__, received, resp_len, response_buf);
                break;
            }
        }
    } while(received < resp_len);

    if (received == resp_len) {
        free(request_buf);
        ERROR_EXIT("storing complete response from socket");
    }

    if (strlen(pThreadInfo->filePath) > 0) {
        appendResultBufToFile(response_buf, pThreadInfo);
    }

    free(request_buf);

    response_buf[RESP_BUF_LEN - 1] = '\0';
    if (NULL == strstr(response_buf, resHttpOk)) {
        errorPrint("%s() LN%d, Response:\n%s\n",
                __func__, __LINE__, response_buf);
        return -1;
    }
    return 0;
}

static char* getTagValueFromTagSample(SSuperTable* stbInfo, int tagUsePos) {
    char*  dataBuf = (char*)calloc(TSDB_MAX_SQL_LEN+1, 1);
    if (NULL == dataBuf) {
        errorPrint2("%s() LN%d, calloc failed! size:%d\n",
                __func__, __LINE__, TSDB_MAX_SQL_LEN+1);
        return NULL;
    }

    int    dataLen = 0;
    dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
            "(%s)", stbInfo->tagDataBuf + stbInfo->lenOfTagOfOneRow * tagUsePos);

    return dataBuf;
}

static char *generateBinaryNCharTagValues(int64_t tableSeq, uint32_t len)
{
    char* buf = (char*)calloc(len, 1);
    if (NULL == buf) {
        printf("calloc failed! size:%d\n", len);
        return NULL;
    }

    if (tableSeq % 2) {
        tstrncpy(buf, "beijing", len);
    } else {
        tstrncpy(buf, "shanghai", len);
    }
    //rand_string(buf, stbInfo->tags[i].dataLen);

    return buf;
}

static char* generateTagValuesForStb(SSuperTable* stbInfo, int64_t tableSeq) {
    char*  dataBuf = (char*)calloc(TSDB_MAX_SQL_LEN+1, 1);
    if (NULL == dataBuf) {
        printf("calloc failed! size:%d\n", TSDB_MAX_SQL_LEN+1);
        return NULL;
    }

    int    dataLen = 0;
    dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen, "(");
    for (int i = 0; i < stbInfo->tagCount; i++) {
        if ((0 == strncasecmp(stbInfo->tags[i].dataType,
                        "binary", strlen("binary")))
                || (0 == strncasecmp(stbInfo->tags[i].dataType,
                        "nchar", strlen("nchar")))) {
            if (stbInfo->tags[i].dataLen > TSDB_MAX_BINARY_LEN) {
                printf("binary or nchar length overflow, max size:%u\n",
                        (uint32_t)TSDB_MAX_BINARY_LEN);
                tmfree(dataBuf);
                return NULL;
            }

            int32_t tagBufLen = stbInfo->tags[i].dataLen + 1;
            char *buf = generateBinaryNCharTagValues(tableSeq, tagBufLen);
            if (NULL == buf) {
                tmfree(dataBuf);
                return NULL;
            }
            dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                    "\'%s\',", buf);
            tmfree(buf);
        } else if (0 == strncasecmp(stbInfo->tags[i].dataType,
                    "int", strlen("int"))) {
            if ((g_args.demo_mode) && (i == 0)) {
                dataLen += snprintf(dataBuf + dataLen,
                        TSDB_MAX_SQL_LEN - dataLen,
                        "%"PRId64",", (tableSeq % 10) + 1);
            } else {
                dataLen += snprintf(dataBuf + dataLen,
                        TSDB_MAX_SQL_LEN - dataLen,
                        "%"PRId64",", tableSeq);
            }
        } else if (0 == strncasecmp(stbInfo->tags[i].dataType,
                    "bigint", strlen("bigint"))) {
            dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                    "%"PRId64",", rand_bigint());
        }  else if (0 == strncasecmp(stbInfo->tags[i].dataType,
                    "float", strlen("float"))) {
            dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                    "%f,", rand_float());
        }  else if (0 == strncasecmp(stbInfo->tags[i].dataType,
                    "double", strlen("double"))) {
            dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                    "%f,", rand_double());
        }  else if (0 == strncasecmp(stbInfo->tags[i].dataType,
                    "smallint", strlen("smallint"))) {
            dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                    "%d,", rand_smallint());
        }  else if (0 == strncasecmp(stbInfo->tags[i].dataType,
                    "tinyint", strlen("tinyint"))) {
            dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                    "%d,", rand_tinyint());
        }  else if (0 == strncasecmp(stbInfo->tags[i].dataType,
                    "bool", strlen("bool"))) {
            dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                    "%d,", rand_bool());
        }  else if (0 == strncasecmp(stbInfo->tags[i].dataType,
                    "timestamp", strlen("timestamp"))) {
            dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                    "%"PRId64",", rand_ubigint());
        }  else if (0 == strncasecmp(stbInfo->tags[i].dataType,
                    "utinyint", strlen("utinyint"))) {
            dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                    "%d,", rand_utinyint());
        }  else if (0 == strncasecmp(stbInfo->tags[i].dataType,
                    "usmallint", strlen("usmallint"))) {
            dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                    "%d,", rand_usmallint());
        }  else if (0 == strncasecmp(stbInfo->tags[i].dataType,
                    "uint", strlen("uint"))) {
            dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                    "%d,", rand_uint());
        }  else if (0 == strncasecmp(stbInfo->tags[i].dataType,
                    "ubigint", strlen("ubigint"))) {
            dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                    "%"PRId64",", rand_ubigint());
        }  else {
            errorPrint2("No support data type: %s\n", stbInfo->tags[i].dataType);
            tmfree(dataBuf);
            return NULL;
        }
    }

    dataLen -= 1;
    dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen, ")");
    return dataBuf;
}

static int calcRowLen(SSuperTable*  superTbls) {
    int colIndex;
    int  lenOfOneRow = 0;

    for (colIndex = 0; colIndex < superTbls->columnCount; colIndex++) {
        char* dataType = superTbls->columns[colIndex].dataType;

        switch(superTbls->columns[colIndex].data_type) {
            case TSDB_DATA_TYPE_BINARY:
                lenOfOneRow += superTbls->columns[colIndex].dataLen + 3;
                break;

            case TSDB_DATA_TYPE_NCHAR:
                lenOfOneRow += superTbls->columns[colIndex].dataLen + 3;
                break;

            case TSDB_DATA_TYPE_INT:
            case TSDB_DATA_TYPE_UINT:
                lenOfOneRow += INT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_BIGINT:
            case TSDB_DATA_TYPE_UBIGINT:
                lenOfOneRow += BIGINT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_SMALLINT:
            case TSDB_DATA_TYPE_USMALLINT:
                lenOfOneRow += SMALLINT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_TINYINT:
            case TSDB_DATA_TYPE_UTINYINT:
                lenOfOneRow += TINYINT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_BOOL:
                lenOfOneRow += BOOL_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_FLOAT:
                lenOfOneRow += FLOAT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_DOUBLE:
                lenOfOneRow += DOUBLE_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_TIMESTAMP:
                lenOfOneRow += TIMESTAMP_BUFF_LEN;
                break;

            default:
                errorPrint2("get error data type : %s\n", dataType);
                exit(EXIT_FAILURE);
        }
    }

    superTbls->lenOfOneRow = lenOfOneRow + 20; // timestamp

    int tagIndex;
    int lenOfTagOfOneRow = 0;
    for (tagIndex = 0; tagIndex < superTbls->tagCount; tagIndex++) {
        char * dataType = superTbls->tags[tagIndex].dataType;
        switch (superTbls->tags[tagIndex].data_type)
        {
        case TSDB_DATA_TYPE_BINARY:
            lenOfTagOfOneRow += superTbls->tags[tagIndex].dataLen + 3;
            break;
        case TSDB_DATA_TYPE_NCHAR:
            lenOfTagOfOneRow += superTbls->tags[tagIndex].dataLen + 3;
            break;
        case TSDB_DATA_TYPE_INT:
        case TSDB_DATA_TYPE_UINT:
            lenOfTagOfOneRow += superTbls->tags[tagIndex].dataLen + INT_BUFF_LEN;
            break;
        case TSDB_DATA_TYPE_BIGINT:
        case TSDB_DATA_TYPE_UBIGINT:
            lenOfTagOfOneRow += superTbls->tags[tagIndex].dataLen + BIGINT_BUFF_LEN;
            break;
        case TSDB_DATA_TYPE_SMALLINT:
        case TSDB_DATA_TYPE_USMALLINT:
            lenOfTagOfOneRow += superTbls->tags[tagIndex].dataLen + SMALLINT_BUFF_LEN;
            break;
        case TSDB_DATA_TYPE_TINYINT:
        case TSDB_DATA_TYPE_UTINYINT:
            lenOfTagOfOneRow += superTbls->tags[tagIndex].dataLen + TINYINT_BUFF_LEN;
            break;
        case TSDB_DATA_TYPE_BOOL:
            lenOfTagOfOneRow += superTbls->tags[tagIndex].dataLen + BOOL_BUFF_LEN;
            break;
        case TSDB_DATA_TYPE_FLOAT:
            lenOfTagOfOneRow += superTbls->tags[tagIndex].dataLen + FLOAT_BUFF_LEN;
            break;
        case TSDB_DATA_TYPE_DOUBLE:
            lenOfTagOfOneRow += superTbls->tags[tagIndex].dataLen + DOUBLE_BUFF_LEN;
            break;
        default:
            errorPrint2("get error tag type : %s\n", dataType);
            exit(EXIT_FAILURE);
        }
    }

    superTbls->lenOfTagOfOneRow = lenOfTagOfOneRow;

    return 0;
}

static int getChildNameOfSuperTableWithLimitAndOffset(TAOS * taos,
        char* dbName, char* stbName, char** childTblNameOfSuperTbl,
        int64_t* childTblCountOfSuperTbl, int64_t limit, uint64_t offset) {

    char command[1024] = "\0";
    char limitBuf[100] = "\0";

    TAOS_RES * res;
    TAOS_ROW row = NULL;

    char* childTblName = *childTblNameOfSuperTbl;

    snprintf(limitBuf, 100, " limit %"PRId64" offset %"PRIu64"",
        limit, offset);

    //get all child table name use cmd: select tbname from superTblName;
    snprintf(command, 1024, "select tbname from %s.%s %s", dbName, stbName, limitBuf);

    res = taos_query(taos, command);
    int32_t code = taos_errno(res);
    if (code != 0) {
        taos_free_result(res);
        taos_close(taos);
        errorPrint2("%s() LN%d, failed to run command %s\n",
                __func__, __LINE__, command);
        exit(EXIT_FAILURE);
    }

    int64_t childTblCount = (limit < 0)?10000:limit;
    int64_t count = 0;
    if (childTblName == NULL) {
        childTblName = (char*)calloc(1, childTblCount * TSDB_TABLE_NAME_LEN);
        if (NULL ==  childTblName) {
            taos_free_result(res);
            taos_close(taos);
            errorPrint2("%s() LN%d, failed to allocate memory!\n", __func__, __LINE__);
            exit(EXIT_FAILURE);
        }
    }

    char* pTblName = childTblName;
    while((row = taos_fetch_row(res)) != NULL) {
        int32_t* len = taos_fetch_lengths(res);

        if (0 == strlen((char *)row[0])) {
            errorPrint2("%s() LN%d, No.%"PRId64" table return empty name\n",
                    __func__, __LINE__, count);
            exit(EXIT_FAILURE);
        }

        tstrncpy(pTblName, (char *)row[0], len[0]+1);
        //printf("==== sub table name: %s\n", pTblName);
        count++;
        if (count >= childTblCount - 1) {
            char *tmp = realloc(childTblName,
                    (size_t)childTblCount*1.5*TSDB_TABLE_NAME_LEN+1);
            if (tmp != NULL) {
                childTblName = tmp;
                childTblCount = (int)(childTblCount*1.5);
                memset(childTblName + count*TSDB_TABLE_NAME_LEN, 0,
                        (size_t)((childTblCount-count)*TSDB_TABLE_NAME_LEN));
            } else {
                // exit, if allocate more memory failed
                tmfree(childTblName);
                taos_free_result(res);
                taos_close(taos);
                errorPrint2("%s() LN%d, realloc fail for save child table name of %s.%s\n",
                        __func__, __LINE__, dbName, stbName);
                exit(EXIT_FAILURE);
            }
        }
        pTblName = childTblName + count * TSDB_TABLE_NAME_LEN;
    }

    *childTblCountOfSuperTbl = count;
    *childTblNameOfSuperTbl  = childTblName;

    taos_free_result(res);
    return 0;
}

static int getAllChildNameOfSuperTable(TAOS * taos, char* dbName,
        char* stbName, char** childTblNameOfSuperTbl,
        int64_t* childTblCountOfSuperTbl) {

    return getChildNameOfSuperTableWithLimitAndOffset(taos, dbName, stbName,
            childTblNameOfSuperTbl, childTblCountOfSuperTbl,
            -1, 0);
}

static int getSuperTableFromServer(TAOS * taos, char* dbName,
        SSuperTable*  superTbls) {

    char command[1024] = "\0";
    TAOS_RES * res;
    TAOS_ROW row = NULL;
    int count = 0;

    //get schema use cmd: describe superTblName;
    snprintf(command, 1024, "describe %s.%s", dbName, superTbls->stbName);
    res = taos_query(taos, command);
    int32_t code = taos_errno(res);
    if (code != 0) {
        printf("failed to run command %s\n", command);
        taos_free_result(res);
        return -1;
    }

    int tagIndex = 0;
    int columnIndex = 0;
    TAOS_FIELD *fields = taos_fetch_fields(res);
    while((row = taos_fetch_row(res)) != NULL) {
        if (0 == count) {
            count++;
            continue;
        }

        if (strcmp((char *)row[TSDB_DESCRIBE_METRIC_NOTE_INDEX], "TAG") == 0) {
            tstrncpy(superTbls->tags[tagIndex].field,
                    (char *)row[TSDB_DESCRIBE_METRIC_FIELD_INDEX],
                    fields[TSDB_DESCRIBE_METRIC_FIELD_INDEX].bytes);
            if (0 == strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                        "INT", strlen("INT"))) {
                superTbls->tags[tagIndex].data_type = TSDB_DATA_TYPE_INT;
            } else if (0 == strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                        "TINYINT", strlen("TINYINT"))) {
                superTbls->tags[tagIndex].data_type = TSDB_DATA_TYPE_TINYINT;
            } else if (0 == strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                        "SMALLINT", strlen("SMALLINT"))) {
                superTbls->tags[tagIndex].data_type = TSDB_DATA_TYPE_SMALLINT;
            } else if (0 == strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                        "BIGINT", strlen("BIGINT"))) {
                superTbls->tags[tagIndex].data_type = TSDB_DATA_TYPE_BIGINT;
            } else if (0 == strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                        "FLOAT", strlen("FLOAT"))) {
                superTbls->tags[tagIndex].data_type = TSDB_DATA_TYPE_FLOAT;
            } else if (0 == strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                        "DOUBLE", strlen("DOUBLE"))) {
                superTbls->tags[tagIndex].data_type = TSDB_DATA_TYPE_DOUBLE;
            } else if (0 == strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                        "BINARY", strlen("BINARY"))) {
                superTbls->tags[tagIndex].data_type = TSDB_DATA_TYPE_BINARY;
            } else if (0 == strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                        "NCHAR", strlen("NCHAR"))) {
                superTbls->tags[tagIndex].data_type = TSDB_DATA_TYPE_NCHAR;
            } else if (0 == strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                        "BOOL", strlen("BOOL"))) {
                superTbls->tags[tagIndex].data_type = TSDB_DATA_TYPE_BOOL;
            } else if (0 == strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                        "TIMESTAMP", strlen("TIMESTAMP"))) {
                superTbls->tags[tagIndex].data_type = TSDB_DATA_TYPE_TIMESTAMP;
            } else if (0 == strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                        "TINYINT UNSIGNED", strlen("TINYINT UNSIGNED"))) {
                superTbls->tags[tagIndex].data_type = TSDB_DATA_TYPE_UTINYINT;
                tstrncpy(superTbls->tags[tagIndex].dataType,"UTINYINT",
                    min(DATATYPE_BUFF_LEN,
                        fields[TSDB_DESCRIBE_METRIC_TYPE_INDEX].bytes) + 1);
            } else if (0 == strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                        "SMALLINT UNSIGNED", strlen("SMALLINT UNSIGNED"))) {
                superTbls->tags[tagIndex].data_type = TSDB_DATA_TYPE_USMALLINT;
                tstrncpy(superTbls->tags[tagIndex].dataType,"USMALLINT",
                    min(DATATYPE_BUFF_LEN,
                        fields[TSDB_DESCRIBE_METRIC_TYPE_INDEX].bytes) + 1);
            } else if (0 == strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                        "INT UNSIGNED", strlen("INT UNSIGNED"))) {
                superTbls->tags[tagIndex].data_type = TSDB_DATA_TYPE_UINT;
                tstrncpy(superTbls->tags[tagIndex].dataType,"UINT",
                    min(DATATYPE_BUFF_LEN,
                        fields[TSDB_DESCRIBE_METRIC_TYPE_INDEX].bytes) + 1);
            }else if (0 == strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                        "BIGINT UNSIGNED", strlen("BIGINT UNSIGNED"))) {
                superTbls->tags[tagIndex].data_type = TSDB_DATA_TYPE_UBIGINT;
                tstrncpy(superTbls->tags[tagIndex].dataType,"UBIGINT",
                    min(DATATYPE_BUFF_LEN,
                        fields[TSDB_DESCRIBE_METRIC_TYPE_INDEX].bytes) + 1);
            } else {
                superTbls->tags[tagIndex].data_type = TSDB_DATA_TYPE_NULL;
            }
            superTbls->tags[tagIndex].dataLen =
                *((int *)row[TSDB_DESCRIBE_METRIC_LENGTH_INDEX]);
            tstrncpy(superTbls->tags[tagIndex].note,
                    (char *)row[TSDB_DESCRIBE_METRIC_NOTE_INDEX],
                    min(NOTE_BUFF_LEN,
                        fields[TSDB_DESCRIBE_METRIC_NOTE_INDEX].bytes) + 1);
            if (strstr((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX], "UNSIGNED") == NULL)
            {
                tstrncpy(superTbls->tags[tagIndex].dataType,
                    (char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                    min(DATATYPE_BUFF_LEN,
                        fields[TSDB_DESCRIBE_METRIC_TYPE_INDEX].bytes) + 1);
            }
            tagIndex++;
        } else {
            tstrncpy(superTbls->columns[columnIndex].field,
                    (char *)row[TSDB_DESCRIBE_METRIC_FIELD_INDEX],
                    fields[TSDB_DESCRIBE_METRIC_FIELD_INDEX].bytes);


            if (0 == strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                        "INT", strlen("INT")) &&
                        strstr((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX], "UNSIGNED") == NULL) {
                superTbls->columns[columnIndex].data_type = TSDB_DATA_TYPE_INT;
            } else if (0 == strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                        "TINYINT", strlen("TINYINT")) &&
                        strstr((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX], "UNSIGNED") == NULL) {
                superTbls->columns[columnIndex].data_type = TSDB_DATA_TYPE_TINYINT;
            } else if (0 == strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                        "SMALLINT", strlen("SMALLINT")) &&
                        strstr((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX], "UNSIGNED") == NULL) {
                superTbls->columns[columnIndex].data_type = TSDB_DATA_TYPE_SMALLINT;
            } else if (0 == strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                        "BIGINT", strlen("BIGINT")) &&
                        strstr((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX], "UNSIGNED") == NULL) {
                superTbls->columns[columnIndex].data_type = TSDB_DATA_TYPE_BIGINT;
            } else if (0 == strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                        "FLOAT", strlen("FLOAT"))) {
                superTbls->columns[columnIndex].data_type = TSDB_DATA_TYPE_FLOAT;
            } else if (0 == strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                        "DOUBLE", strlen("DOUBLE"))) {
                superTbls->columns[columnIndex].data_type = TSDB_DATA_TYPE_DOUBLE;
            } else if (0 == strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                        "BINARY", strlen("BINARY"))) {
                superTbls->columns[columnIndex].data_type = TSDB_DATA_TYPE_BINARY;
            } else if (0 == strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                        "NCHAR", strlen("NCHAR"))) {
                superTbls->columns[columnIndex].data_type = TSDB_DATA_TYPE_NCHAR;
            } else if (0 == strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                        "BOOL", strlen("BOOL"))) {
                superTbls->columns[columnIndex].data_type = TSDB_DATA_TYPE_BOOL;
            } else if (0 == strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                        "TIMESTAMP", strlen("TIMESTAMP"))) {
                superTbls->columns[columnIndex].data_type = TSDB_DATA_TYPE_TIMESTAMP;
            } else if (0 == strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                        "TINYINT UNSIGNED", strlen("TINYINT UNSIGNED"))) {
                superTbls->columns[columnIndex].data_type = TSDB_DATA_TYPE_UTINYINT;
                tstrncpy(superTbls->columns[columnIndex].dataType,"UTINYINT",
                    min(DATATYPE_BUFF_LEN,
                        fields[TSDB_DESCRIBE_METRIC_TYPE_INDEX].bytes) + 1);
            } else if (0 == strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                        "SMALLINT UNSIGNED", strlen("SMALLINT UNSIGNED"))) {
                superTbls->columns[columnIndex].data_type = TSDB_DATA_TYPE_USMALLINT;
                tstrncpy(superTbls->columns[columnIndex].dataType,"USMALLINT",
                    min(DATATYPE_BUFF_LEN,
                        fields[TSDB_DESCRIBE_METRIC_TYPE_INDEX].bytes) + 1);
            } else if (0 == strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                        "INT UNSIGNED", strlen("INT UNSIGNED"))) {
                superTbls->columns[columnIndex].data_type = TSDB_DATA_TYPE_UINT;
                tstrncpy(superTbls->columns[columnIndex].dataType,"UINT",
                    min(DATATYPE_BUFF_LEN,
                        fields[TSDB_DESCRIBE_METRIC_TYPE_INDEX].bytes) + 1);
            } else if (0 == strncasecmp((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                        "BIGINT UNSIGNED", strlen("BIGINT UNSIGNED"))) {
                superTbls->columns[columnIndex].data_type = TSDB_DATA_TYPE_UBIGINT;
                tstrncpy(superTbls->columns[columnIndex].dataType,"UBIGINT",
                    min(DATATYPE_BUFF_LEN,
                        fields[TSDB_DESCRIBE_METRIC_TYPE_INDEX].bytes) + 1);
            } else {
                superTbls->columns[columnIndex].data_type = TSDB_DATA_TYPE_NULL;
            }
            superTbls->columns[columnIndex].dataLen =
                *((int *)row[TSDB_DESCRIBE_METRIC_LENGTH_INDEX]);
            tstrncpy(superTbls->columns[columnIndex].note,
                    (char *)row[TSDB_DESCRIBE_METRIC_NOTE_INDEX],
                    min(NOTE_BUFF_LEN,
                        fields[TSDB_DESCRIBE_METRIC_NOTE_INDEX].bytes) + 1);

            if (strstr((char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX], "UNSIGNED") == NULL) {
                tstrncpy(superTbls->columns[columnIndex].dataType,
                    (char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
                    min(DATATYPE_BUFF_LEN,
                        fields[TSDB_DESCRIBE_METRIC_TYPE_INDEX].bytes) + 1);
            }

            columnIndex++;
        }
        count++;
    }

    superTbls->columnCount = columnIndex;
    superTbls->tagCount    = tagIndex;
    taos_free_result(res);

    calcRowLen(superTbls);

    /*
       if (TBL_ALREADY_EXISTS == superTbls->childTblExists) {
    //get all child table name use cmd: select tbname from superTblName;
    int childTblCount = 10000;
    superTbls->childTblName = (char*)calloc(1, childTblCount * TSDB_TABLE_NAME_LEN);
    if (superTbls->childTblName == NULL) {
    errorPrint2("%s() LN%d, alloc memory failed!\n", __func__, __LINE__);
    return -1;
    }
    getAllChildNameOfSuperTable(taos, dbName,
    superTbls->stbName,
    &superTbls->childTblName,
    &superTbls->childTblCount);
    }
    */
    return 0;
}

static int createSuperTable(
        TAOS * taos, char* dbName,
        SSuperTable*  superTbl) {

    char *command = calloc(1, BUFFER_SIZE);
    assert(command);

    char cols[COL_BUFFER_LEN] = "\0";
    int len = 0;

    int  lenOfOneRow = 0;

    if (superTbl->columnCount == 0) {
        errorPrint2("%s() LN%d, super table column count is %d\n",
                __func__, __LINE__, superTbl->columnCount);
        free(command);
        return -1;
    }

    for (int colIndex = 0; colIndex < superTbl->columnCount; colIndex++) {

        switch(superTbl->columns[colIndex].data_type) {
            case TSDB_DATA_TYPE_BINARY:
                len += snprintf(cols + len, COL_BUFFER_LEN - len,
                        ",C%d %s(%d)", colIndex, "BINARY",
                        superTbl->columns[colIndex].dataLen);
                lenOfOneRow += superTbl->columns[colIndex].dataLen + 3;
                break;

            case TSDB_DATA_TYPE_NCHAR:
                len += snprintf(cols + len, COL_BUFFER_LEN - len,
                        ",C%d %s(%d)", colIndex, "NCHAR",
                        superTbl->columns[colIndex].dataLen);
                lenOfOneRow += superTbl->columns[colIndex].dataLen + 3;
                break;

            case TSDB_DATA_TYPE_INT:
                if ((g_args.demo_mode) && (colIndex == 1)) {
                    len += snprintf(cols + len, COL_BUFFER_LEN - len,
                            ", VOLTAGE INT");
                } else {
                    len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s", colIndex, "INT");
                }
                lenOfOneRow += INT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_BIGINT:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s",
                        colIndex, "BIGINT");
                lenOfOneRow += BIGINT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_SMALLINT:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s",
                        colIndex, "SMALLINT");
                lenOfOneRow += SMALLINT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_TINYINT:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s", colIndex, "TINYINT");
                lenOfOneRow += TINYINT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_BOOL:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s", colIndex, "BOOL");
                lenOfOneRow += BOOL_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_FLOAT:
                if (g_args.demo_mode) {
                    if (colIndex == 0) {
                        len += snprintf(cols + len, COL_BUFFER_LEN - len, ", CURRENT FLOAT");
                    } else if (colIndex == 2) {
                        len += snprintf(cols + len, COL_BUFFER_LEN - len, ", PHASE FLOAT");
                    }
                } else {
                    len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s", colIndex, "FLOAT");
                }

                lenOfOneRow += FLOAT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_DOUBLE:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s",
                        colIndex, "DOUBLE");
                lenOfOneRow += DOUBLE_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_TIMESTAMP:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s",
                        colIndex, "TIMESTAMP");
                lenOfOneRow += TIMESTAMP_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_UTINYINT:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s",
                        colIndex, "TINYINT UNSIGNED");
                lenOfOneRow += TINYINT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_USMALLINT:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s",
                        colIndex, "SMALLINT UNSIGNED");
                lenOfOneRow += SMALLINT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_UINT:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s",
                        colIndex, "INT UNSIGNED");
                lenOfOneRow += INT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_UBIGINT:
                len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s",
                        colIndex, "BIGINT UNSIGNED");
                lenOfOneRow += BIGINT_BUFF_LEN;
                break;

            default:
                taos_close(taos);
                free(command);
                errorPrint2("%s() LN%d, config error data type : %s\n",
                        __func__, __LINE__, superTbl->columns[colIndex].dataType);
                exit(EXIT_FAILURE);
        }
    }

    superTbl->lenOfOneRow = lenOfOneRow + 20; // timestamp

    // save for creating child table
    superTbl->colsOfCreateChildTable = (char*)calloc(len+20, 1);
    if (NULL == superTbl->colsOfCreateChildTable) {
        taos_close(taos);
        free(command);
        errorPrint2("%s() LN%d, Failed when calloc, size:%d",
                __func__, __LINE__, len+1);
        exit(EXIT_FAILURE);
    }

    snprintf(superTbl->colsOfCreateChildTable, len+20, "(ts timestamp%s)", cols);
    verbosePrint("%s() LN%d: %s\n",
            __func__, __LINE__, superTbl->colsOfCreateChildTable);

    if (superTbl->tagCount == 0) {
        errorPrint2("%s() LN%d, super table tag count is %d\n",
                __func__, __LINE__, superTbl->tagCount);
        free(command);
        return -1;
    }

    char tags[TSDB_MAX_TAGS_LEN] = "\0";
    int tagIndex;
    len = 0;

    int lenOfTagOfOneRow = 0;
    len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "(");
    for (tagIndex = 0; tagIndex < superTbl->tagCount; tagIndex++) {
        char* dataType = superTbl->tags[tagIndex].dataType;

        if (strcasecmp(dataType, "BINARY") == 0) {
            if ((g_args.demo_mode) && (tagIndex == 1)) {
                len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len,
                        "location BINARY(%d),",
                        superTbl->tags[tagIndex].dataLen);
            } else {
                len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len,
                        "T%d %s(%d),", tagIndex, "BINARY",
                        superTbl->tags[tagIndex].dataLen);
            }
            lenOfTagOfOneRow += superTbl->tags[tagIndex].dataLen + 3;
        } else if (strcasecmp(dataType, "NCHAR") == 0) {
            len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len,
                    "T%d %s(%d),", tagIndex,
                    "NCHAR", superTbl->tags[tagIndex].dataLen);
            lenOfTagOfOneRow += superTbl->tags[tagIndex].dataLen + 3;
        } else if (strcasecmp(dataType, "INT") == 0)  {
            if ((g_args.demo_mode) && (tagIndex == 0)) {
                len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len,
                        "groupId INT, ");
            } else {
                len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len,
                        "T%d %s,", tagIndex, "INT");
            }
            lenOfTagOfOneRow += superTbl->tags[tagIndex].dataLen + INT_BUFF_LEN;
        } else if (strcasecmp(dataType, "BIGINT") == 0)  {
            len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len,
                    "T%d %s,", tagIndex, "BIGINT");
            lenOfTagOfOneRow += superTbl->tags[tagIndex].dataLen + BIGINT_BUFF_LEN;
        } else if (strcasecmp(dataType, "SMALLINT") == 0)  {
            len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len,
                    "T%d %s,", tagIndex, "SMALLINT");
            lenOfTagOfOneRow += superTbl->tags[tagIndex].dataLen + SMALLINT_BUFF_LEN;
        } else if (strcasecmp(dataType, "TINYINT") == 0)  {
            len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len,
                    "T%d %s,", tagIndex, "TINYINT");
            lenOfTagOfOneRow += superTbl->tags[tagIndex].dataLen + TINYINT_BUFF_LEN;
        } else if (strcasecmp(dataType, "BOOL") == 0)  {
            len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len,
                    "T%d %s,", tagIndex, "BOOL");
            lenOfTagOfOneRow += superTbl->tags[tagIndex].dataLen + BOOL_BUFF_LEN;
        } else if (strcasecmp(dataType, "FLOAT") == 0) {
            len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len,
                    "T%d %s,", tagIndex, "FLOAT");
            lenOfTagOfOneRow += superTbl->tags[tagIndex].dataLen + FLOAT_BUFF_LEN;
        } else if (strcasecmp(dataType, "DOUBLE") == 0) {
            len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len,
                    "T%d %s,", tagIndex, "DOUBLE");
            lenOfTagOfOneRow += superTbl->tags[tagIndex].dataLen + DOUBLE_BUFF_LEN;
        } else if (strcasecmp(dataType, "UTINYINT") == 0) {
            len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len,
                    "T%d %s,", tagIndex, "TINYINT UNSIGNED");
            lenOfTagOfOneRow += superTbl->tags[tagIndex].dataLen + TINYINT_BUFF_LEN;
        } else if (strcasecmp(dataType, "USMALLINT") == 0) {
            len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len,
                    "T%d %s,", tagIndex, "SMALLINT UNSIGNED");
            lenOfTagOfOneRow += superTbl->tags[tagIndex].dataLen + SMALLINT_BUFF_LEN;
        } else if (strcasecmp(dataType, "UINT") == 0) {
            len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len,
                    "T%d %s,", tagIndex, "INT UNSIGNED");
            lenOfTagOfOneRow += superTbl->tags[tagIndex].dataLen + INT_BUFF_LEN;
        } else if (strcasecmp(dataType, "UBIGINT") == 0) {
            len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len,
                    "T%d %s,", tagIndex, "BIGINT UNSIGNED");
            lenOfTagOfOneRow += superTbl->tags[tagIndex].dataLen + BIGINT_BUFF_LEN;
        } else if (strcasecmp(dataType, "TIMESTAMP") == 0) {
            len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len,
                    "T%d %s,", tagIndex, "TIMESTAMP");
            lenOfTagOfOneRow += superTbl->tags[tagIndex].dataLen + TIMESTAMP_BUFF_LEN;
        } else {
            taos_close(taos);
            free(command);
            errorPrint2("%s() LN%d, config error tag type : %s\n",
                    __func__, __LINE__, dataType);
            exit(EXIT_FAILURE);
        }
    }

    len -= 1;
    len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, ")");

    superTbl->lenOfTagOfOneRow = lenOfTagOfOneRow;

    
    snprintf(command, BUFFER_SIZE,
        "CREATE TABLE IF NOT EXISTS %s.%s (ts TIMESTAMP%s) TAGS %s",
        dbName, superTbl->stbName, cols, tags);
    if (0 != queryDbExec(taos, command, NO_INSERT_TYPE, false)) {
        errorPrint2("create supertable %s failed!\n\n",
                superTbl->stbName);
        free(command);
        return -1;
    }

    debugPrint("create supertable %s success!\n\n", superTbl->stbName);
    free(command);
    return 0;
}

int createDatabasesAndStables(char *command) {
    TAOS * taos = NULL;
    int    ret = 0;
    taos = taos_connect(g_Dbs.host, g_Dbs.user, g_Dbs.password, NULL, g_Dbs.port);
    if (taos == NULL) {
        errorPrint2("Failed to connect to TDengine, reason:%s\n", taos_errstr(NULL));
        return -1;
    }

    for (int i = 0; i < g_Dbs.dbCount; i++) {
        if (g_Dbs.db[i].drop) {
            sprintf(command, "drop database if exists %s;", g_Dbs.db[i].dbName);
            if (0 != queryDbExec(taos, command, NO_INSERT_TYPE, false)) {
                taos_close(taos);
                return -1;
            }

            int dataLen = 0;
            dataLen += snprintf(command + dataLen,
                    BUFFER_SIZE - dataLen, "CREATE DATABASE IF NOT EXISTS %s",
                    g_Dbs.db[i].dbName);

            if (g_Dbs.db[i].dbCfg.blocks > 0) {
                dataLen += snprintf(command + dataLen,
                        BUFFER_SIZE - dataLen, " BLOCKS %d",
                        g_Dbs.db[i].dbCfg.blocks);
            }
            if (g_Dbs.db[i].dbCfg.cache > 0) {
                dataLen += snprintf(command + dataLen,
                        BUFFER_SIZE - dataLen, " CACHE %d",
                        g_Dbs.db[i].dbCfg.cache);
            }
            if (g_Dbs.db[i].dbCfg.days > 0) {
                dataLen += snprintf(command + dataLen,
                        BUFFER_SIZE - dataLen, " DAYS %d",
                        g_Dbs.db[i].dbCfg.days);
            }
            if (g_Dbs.db[i].dbCfg.keep > 0) {
                dataLen += snprintf(command + dataLen,
                        BUFFER_SIZE - dataLen, " KEEP %d",
                        g_Dbs.db[i].dbCfg.keep);
            }
            if (g_Dbs.db[i].dbCfg.quorum > 1) {
                dataLen += snprintf(command + dataLen,
                        BUFFER_SIZE - dataLen, " QUORUM %d",
                        g_Dbs.db[i].dbCfg.quorum);
            }
            if (g_Dbs.db[i].dbCfg.replica > 0) {
                dataLen += snprintf(command + dataLen,
                        BUFFER_SIZE - dataLen, " REPLICA %d",
                        g_Dbs.db[i].dbCfg.replica);
            }
            if (g_Dbs.db[i].dbCfg.update > 0) {
                dataLen += snprintf(command + dataLen,
                        BUFFER_SIZE - dataLen, " UPDATE %d",
                        g_Dbs.db[i].dbCfg.update);
            }
            //if (g_Dbs.db[i].dbCfg.maxtablesPerVnode > 0) {
            //  dataLen += snprintf(command + dataLen,
            //  BUFFER_SIZE - dataLen, "tables %d ", g_Dbs.db[i].dbCfg.maxtablesPerVnode);
            //}
            if (g_Dbs.db[i].dbCfg.minRows > 0) {
                dataLen += snprintf(command + dataLen,
                        BUFFER_SIZE - dataLen, " MINROWS %d",
                        g_Dbs.db[i].dbCfg.minRows);
            }
            if (g_Dbs.db[i].dbCfg.maxRows > 0) {
                dataLen += snprintf(command + dataLen,
                        BUFFER_SIZE - dataLen, " MAXROWS %d",
                        g_Dbs.db[i].dbCfg.maxRows);
            }
            if (g_Dbs.db[i].dbCfg.comp > 0) {
                dataLen += snprintf(command + dataLen,
                        BUFFER_SIZE - dataLen, " COMP %d",
                        g_Dbs.db[i].dbCfg.comp);
            }
            if (g_Dbs.db[i].dbCfg.walLevel > 0) {
                dataLen += snprintf(command + dataLen,
                        BUFFER_SIZE - dataLen, " wal %d",
                        g_Dbs.db[i].dbCfg.walLevel);
            }
            if (g_Dbs.db[i].dbCfg.cacheLast > 0) {
                dataLen += snprintf(command + dataLen,
                        BUFFER_SIZE - dataLen, " CACHELAST %d",
                        g_Dbs.db[i].dbCfg.cacheLast);
            }
            if (g_Dbs.db[i].dbCfg.fsync > 0) {
                dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                        " FSYNC %d", g_Dbs.db[i].dbCfg.fsync);
            }
            if ((0 == strncasecmp(g_Dbs.db[i].dbCfg.precision, "ms", 2))
                    || (0 == strncasecmp(g_Dbs.db[i].dbCfg.precision,
                            "ns", 2))
                    || (0 == strncasecmp(g_Dbs.db[i].dbCfg.precision,
                            "us", 2))) {
                dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                        " precision \'%s\';", g_Dbs.db[i].dbCfg.precision);
            }

            if (0 != queryDbExec(taos, command, NO_INSERT_TYPE, false)) {
                taos_close(taos);
                errorPrint("\ncreate database %s failed!\n\n",
                        g_Dbs.db[i].dbName);
                return -1;
            }
            printf("\ncreate database %s success!\n\n", g_Dbs.db[i].dbName);
        }

        debugPrint("%s() LN%d supertbl count:%"PRIu64"\n",
                __func__, __LINE__, g_Dbs.db[i].superTblCount);

        int validStbCount = 0;

        for (uint64_t j = 0; j < g_Dbs.db[i].superTblCount; j++) {
            sprintf(command, "describe %s.%s;", g_Dbs.db[i].dbName,
                    g_Dbs.db[i].superTbls[j].stbName);
            ret = queryDbExec(taos, command, NO_INSERT_TYPE, true);

            if ((ret != 0) || (g_Dbs.db[i].drop)) {
                ret = createSuperTable(taos, g_Dbs.db[i].dbName,
                        &g_Dbs.db[i].superTbls[j]);

                if (0 != ret) {
                    errorPrint("create super table %"PRIu64" failed!\n\n", j);
                    continue;
                }
            } else {
                ret = getSuperTableFromServer(taos, g_Dbs.db[i].dbName,
                    &g_Dbs.db[i].superTbls[j]);
                if (0 != ret) {
                    errorPrint2("\nget super table %s.%s info failed!\n\n",
                            g_Dbs.db[i].dbName, g_Dbs.db[i].superTbls[j].stbName);
                    continue;
                }
            }
            validStbCount ++;
        }
        g_Dbs.db[i].superTblCount = validStbCount;
    }

    taos_close(taos);
    return 0;
}

static void* createTable(void *sarg)
{
    threadInfo *pThreadInfo = (threadInfo *)sarg;
    SSuperTable* stbInfo = pThreadInfo->stbInfo;

    setThreadName("createTable");

    uint64_t  lastPrintTime = taosGetTimestampMs();

    int buff_len = BUFFER_SIZE;

    pThreadInfo->buffer = calloc(buff_len, 1);
    if (pThreadInfo->buffer == NULL) {
        errorPrint2("%s() LN%d, Memory allocated failed!\n", __func__, __LINE__);
        exit(EXIT_FAILURE);
    }

    int len = 0;
    int batchNum = 0;

    verbosePrint("%s() LN%d: Creating table from %"PRIu64" to %"PRIu64"\n",
            __func__, __LINE__,
            pThreadInfo->start_table_from, pThreadInfo->end_table_to);

    for (uint64_t i = pThreadInfo->start_table_from;
            i <= pThreadInfo->end_table_to; i++) {
        if (0 == g_Dbs.use_metric) {
            snprintf(pThreadInfo->buffer, buff_len,
                    "CREATE TABLE IF NOT EXISTS %s.%s%"PRIu64" %s;",
                    pThreadInfo->db_name,
                    g_args.tb_prefix, i,
                    pThreadInfo->cols);
            batchNum ++;
        } else {
            if (stbInfo == NULL) {
                free(pThreadInfo->buffer);
                errorPrint2("%s() LN%d, use metric, but super table info is NULL\n",
                        __func__, __LINE__);
                exit(EXIT_FAILURE);
            } else {
                if (0 == len) {
                    batchNum = 0;
                    memset(pThreadInfo->buffer, 0, buff_len);
                    len += snprintf(pThreadInfo->buffer + len,
                            buff_len - len, "CREATE TABLE ");
                }

                char* tagsValBuf = NULL;
                if (0 == stbInfo->tagSource) {
                    tagsValBuf = generateTagValuesForStb(stbInfo, i);
                } else {
                    if (0 == stbInfo->tagSampleCount) {
                        free(pThreadInfo->buffer);
                        ERROR_EXIT("use sample file for tag, but has no content!\n");
                    }
                    tagsValBuf = getTagValueFromTagSample(
                            stbInfo,
                            i % stbInfo->tagSampleCount);
                }

                if (NULL == tagsValBuf) {
                    free(pThreadInfo->buffer);
                    ERROR_EXIT("use metric, but tag buffer is NULL\n");
                }
                len += snprintf(pThreadInfo->buffer + len,
                        buff_len - len,
                        "if not exists %s.%s%"PRIu64" using %s.%s tags %s ",
                        pThreadInfo->db_name, stbInfo->childTblPrefix,
                        i, pThreadInfo->db_name,
                        stbInfo->stbName, tagsValBuf);
                free(tagsValBuf);
                batchNum++;
                if ((batchNum < stbInfo->batchCreateTableNum)
                        && ((buff_len - len)
                            >= (stbInfo->lenOfTagOfOneRow + 256))) {
                    continue;
                }
            }
        }

        len = 0;

        if (0 != queryDbExec(pThreadInfo->taos, pThreadInfo->buffer,
                    NO_INSERT_TYPE, false)) {
            errorPrint2("queryDbExec() failed. buffer:\n%s\n", pThreadInfo->buffer);
            free(pThreadInfo->buffer);
            return NULL;
        }
        pThreadInfo->tables_created += batchNum;
        uint64_t currentPrintTime = taosGetTimestampMs();
        if (currentPrintTime - lastPrintTime > 30*1000) {
            printf("thread[%d] already create %"PRIu64" - %"PRIu64" tables\n",
                    pThreadInfo->threadID, pThreadInfo->start_table_from, i);
            lastPrintTime = currentPrintTime;
        }
    }

    if (0 != len) {
        if (0 != queryDbExec(pThreadInfo->taos, pThreadInfo->buffer,
                    NO_INSERT_TYPE, false)) {
            errorPrint2("queryDbExec() failed. buffer:\n%s\n", pThreadInfo->buffer);
        }
        pThreadInfo->tables_created += batchNum;
    }
    free(pThreadInfo->buffer);
    return NULL;
}

static int startMultiThreadCreateChildTable(
        char* cols, int threads, uint64_t tableFrom, int64_t ntables,
        char* db_name, SSuperTable* stbInfo) {

    pthread_t *pids = calloc(1, threads * sizeof(pthread_t));
    threadInfo *infos = calloc(1, threads * sizeof(threadInfo));

    if ((NULL == pids) || (NULL == infos)) {
        ERROR_EXIT("createChildTable malloc failed\n");
    }

    if (threads < 1) {
        threads = 1;
    }

    int64_t a = ntables / threads;
    if (a < 1) {
        threads = ntables;
        a = 1;
    }

    int64_t b = 0;
    b = ntables % threads;

    for (int64_t i = 0; i < threads; i++) {
        threadInfo *pThreadInfo = infos + i;
        pThreadInfo->threadID = i;
        tstrncpy(pThreadInfo->db_name, db_name, TSDB_DB_NAME_LEN);
        pThreadInfo->stbInfo = stbInfo;
        verbosePrint("%s() %d db_name: %s\n", __func__, __LINE__, db_name);
        pThreadInfo->taos = taos_connect(
                g_Dbs.host,
                g_Dbs.user,
                g_Dbs.password,
                db_name,
                g_Dbs.port);
        if (pThreadInfo->taos == NULL) {
            errorPrint2("%s() LN%d, Failed to connect to TDengine, reason:%s\n",
                    __func__, __LINE__, taos_errstr(NULL));
            free(pids);
            free(infos);
            return -1;
        }

        pThreadInfo->start_table_from = tableFrom;
        pThreadInfo->ntables = i<b?a+1:a;
        pThreadInfo->end_table_to = i < b ? tableFrom + a : tableFrom + a - 1;
        tableFrom = pThreadInfo->end_table_to + 1;
        pThreadInfo->use_metric = true;
        pThreadInfo->cols = cols;
        pThreadInfo->minDelay = UINT64_MAX;
        pThreadInfo->tables_created = 0;
        pthread_create(pids + i, NULL, createTable, pThreadInfo);
    }

    for (int i = 0; i < threads; i++) {
        pthread_join(pids[i], NULL);
    }

    for (int i = 0; i < threads; i++) {
        threadInfo *pThreadInfo = infos + i;
        taos_close(pThreadInfo->taos);

        g_actualChildTables += pThreadInfo->tables_created;
    }

    free(pids);
    free(infos);

    return 0;
}

static void createChildTables() {
    char tblColsBuf[TSDB_MAX_BYTES_PER_ROW];
    int len;

    for (int i = 0; i < g_Dbs.dbCount; i++) {
        if (g_Dbs.use_metric) {
            if (g_Dbs.db[i].superTblCount > 0) {
                // with super table
                for (int j = 0; j < g_Dbs.db[i].superTblCount; j++) {
                    if ((AUTO_CREATE_SUBTBL
                                == g_Dbs.db[i].superTbls[j].autoCreateTable)
                            || (TBL_ALREADY_EXISTS
                                == g_Dbs.db[i].superTbls[j].childTblExists)) {
                        continue;
                    }
                    verbosePrint("%s() LN%d: %s\n", __func__, __LINE__,
                            g_Dbs.db[i].superTbls[j].colsOfCreateChildTable);
                    uint64_t startFrom = 0;

                    verbosePrint("%s() LN%d: create %"PRId64" child tables from %"PRIu64"\n",
                            __func__, __LINE__, g_totalChildTables, startFrom);

                    startMultiThreadCreateChildTable(
                            g_Dbs.db[i].superTbls[j].colsOfCreateChildTable,
                            g_Dbs.threadCountForCreateTbl,
                            startFrom,
                            g_Dbs.db[i].superTbls[j].childTblCount,
                            g_Dbs.db[i].dbName, &(g_Dbs.db[i].superTbls[j]));
                }
            }
        } else {
            // normal table
            len = snprintf(tblColsBuf, TSDB_MAX_BYTES_PER_ROW, "(TS TIMESTAMP");
            for (int j = 0; j < g_args.columnCount; j++) {
                if ((strncasecmp(g_args.dataType[j], "BINARY", strlen("BINARY")) == 0)
                        || (strncasecmp(g_args.dataType[j],
                                "NCHAR", strlen("NCHAR")) == 0)) {
                    snprintf(tblColsBuf + len, TSDB_MAX_BYTES_PER_ROW - len,
                            ",C%d %s(%d)", j, g_args.dataType[j], g_args.binwidth);
                } else {
                    snprintf(tblColsBuf + len, TSDB_MAX_BYTES_PER_ROW - len,
                            ",C%d %s", j, g_args.dataType[j]);
                }
                len = strlen(tblColsBuf);
            }

            snprintf(tblColsBuf + len, TSDB_MAX_BYTES_PER_ROW - len, ")");

            verbosePrint("%s() LN%d: dbName: %s num of tb: %"PRId64" schema: %s\n",
                    __func__, __LINE__,
                    g_Dbs.db[i].dbName, g_args.ntables, tblColsBuf);
            startMultiThreadCreateChildTable(
                    tblColsBuf,
                    g_Dbs.threadCountForCreateTbl,
                    0,
                    g_args.ntables,
                    g_Dbs.db[i].dbName,
                    NULL);
        }
    }
}

/*
   Read 10000 lines at most. If more than 10000 lines, continue to read after using
   */
static int readTagFromCsvFileToMem(SSuperTable  * stbInfo) {
    size_t  n = 0;
    ssize_t readLen = 0;
    char *  line = NULL;

    FILE *fp = fopen(stbInfo->tagsFile, "r");
    if (fp == NULL) {
        printf("Failed to open tags file: %s, reason:%s\n",
                stbInfo->tagsFile, strerror(errno));
        return -1;
    }

    if (stbInfo->tagDataBuf) {
        free(stbInfo->tagDataBuf);
        stbInfo->tagDataBuf = NULL;
    }

    int tagCount = 10000;
    int count = 0;
    char* tagDataBuf = calloc(1, stbInfo->lenOfTagOfOneRow * tagCount);
    if (tagDataBuf == NULL) {
        printf("Failed to calloc, reason:%s\n", strerror(errno));
        fclose(fp);
        return -1;
    }

    while((readLen = tgetline(&line, &n, fp)) != -1) {
        if (('\r' == line[readLen - 1]) || ('\n' == line[readLen - 1])) {
            line[--readLen] = 0;
        }

        if (readLen == 0) {
            continue;
        }

        memcpy(tagDataBuf + count * stbInfo->lenOfTagOfOneRow, line, readLen);
        count++;

        if (count >= tagCount - 1) {
            char *tmp = realloc(tagDataBuf,
                    (size_t)tagCount*1.5*stbInfo->lenOfTagOfOneRow);
            if (tmp != NULL) {
                tagDataBuf = tmp;
                tagCount = (int)(tagCount*1.5);
                memset(tagDataBuf + count*stbInfo->lenOfTagOfOneRow,
                        0, (size_t)((tagCount-count)*stbInfo->lenOfTagOfOneRow));
            } else {
                // exit, if allocate more memory failed
                printf("realloc fail for save tag val from %s\n", stbInfo->tagsFile);
                tmfree(tagDataBuf);
                free(line);
                fclose(fp);
                return -1;
            }
        }
    }

    stbInfo->tagDataBuf = tagDataBuf;
    stbInfo->tagSampleCount = count;

    free(line);
    fclose(fp);
    return 0;
}

static void getAndSetRowsFromCsvFile(SSuperTable *stbInfo) {
    FILE *fp = fopen(stbInfo->sampleFile, "r");
    int line_count = 0;
    if (fp == NULL) {
        errorPrint("Failed to open sample file: %s, reason:%s\n",
                stbInfo->sampleFile, strerror(errno));
        exit(EXIT_FAILURE);
    }
    char *buf = calloc(1, stbInfo->maxSqlLen);
    while (fgets(buf, stbInfo->maxSqlLen, fp)) {
        line_count++;
    }
    fclose(fp);
    tmfree(buf);
    stbInfo->insertRows = line_count;
}

/*
   Read 10000 lines at most. If more than 10000 lines, continue to read after using
   */
static int generateSampleFromCsvForStb(
        SSuperTable* stbInfo) {
    size_t  n = 0;
    ssize_t readLen = 0;
    char *  line = NULL;
    int getRows = 0;

    FILE*  fp = fopen(stbInfo->sampleFile, "r");
    if (fp == NULL) {
        errorPrint("Failed to open sample file: %s, reason:%s\n",
                stbInfo->sampleFile, strerror(errno));
        return -1;
    }

    assert(stbInfo->sampleDataBuf);
    memset(stbInfo->sampleDataBuf, 0,
            MAX_SAMPLES * stbInfo->lenOfOneRow);
    while(1) {
        readLen = tgetline(&line, &n, fp);
        if (-1 == readLen) {
            if(0 != fseek(fp, 0, SEEK_SET)) {
                errorPrint("Failed to fseek file: %s, reason:%s\n",
                        stbInfo->sampleFile, strerror(errno));
                fclose(fp);
                return -1;
            }
            continue;
        }

        if (('\r' == line[readLen - 1]) || ('\n' == line[readLen - 1])) {
            line[--readLen] = 0;
        }

        if (readLen == 0) {
            continue;
        }

        if (readLen > stbInfo->lenOfOneRow) {
            printf("sample row len[%d] overflow define schema len[%"PRIu64"], so discard this row\n",
                    (int32_t)readLen, stbInfo->lenOfOneRow);
            continue;
        }

        memcpy(stbInfo->sampleDataBuf + getRows * stbInfo->lenOfOneRow,
                line, readLen);
        getRows++;

        if (getRows == MAX_SAMPLES) {
            break;
        }
    }

    fclose(fp);
    tmfree(line);
    return 0;
}

static bool getColumnAndTagTypeFromInsertJsonFile(
        cJSON* stbInfo, SSuperTable* superTbls) {
    bool  ret = false;

    // columns
    cJSON *columns = cJSON_GetObjectItem(stbInfo, "columns");
    if (columns && columns->type != cJSON_Array) {
        errorPrint("%s", "failed to read json, columns not found\n");
        goto PARSE_OVER;
    } else if (NULL == columns) {
        superTbls->columnCount = 0;
        superTbls->tagCount    = 0;
        return true;
    }

    int columnSize = cJSON_GetArraySize(columns);
    if ((columnSize + 1/* ts */) > TSDB_MAX_COLUMNS) {
        errorPrint("failed to read json, column size overflow, max column size is %d\n",
                TSDB_MAX_COLUMNS);
        goto PARSE_OVER;
    }

    int count = 1;
    int tdm_index = 0;
    StrColumn    columnCase;

    //superTbls->columnCount = columnSize;
    for (int k = 0; k < columnSize; ++k) {
        cJSON* column = cJSON_GetArrayItem(columns, k);
        if (column == NULL) continue;

        count = 1;
        cJSON* countObj = cJSON_GetObjectItem(column, "count");
        if (countObj && countObj->type == cJSON_Number) {
            count = countObj->valueint;
        } else if (countObj && countObj->type != cJSON_Number) {
            errorPrint("%s", "failed to read json, column count not found\n");
            goto PARSE_OVER;
        } else {
            count = 1;
        }

        // column info
        memset(&columnCase, 0, sizeof(StrColumn));
        cJSON *dataType = cJSON_GetObjectItem(column, "type");
        if (!dataType || dataType->type != cJSON_String
                || dataType->valuestring == NULL) {
            errorPrint("%s", "failed to read json, column type not found\n");
            goto PARSE_OVER;
        }
        //tstrncpy(superTbls->columns[k].dataType, dataType->valuestring, DATATYPE_BUFF_LEN);
        tstrncpy(columnCase.dataType, dataType->valuestring,
                min(DATATYPE_BUFF_LEN, strlen(dataType->valuestring) + 1));

        cJSON* dataLen = cJSON_GetObjectItem(column, "len");
        if (dataLen && dataLen->type == cJSON_Number) {
            columnCase.dataLen = dataLen->valueint;
        } else if (dataLen && dataLen->type != cJSON_Number) {
            debugPrint("%s() LN%d: failed to read json, column len not found\n",
                    __func__, __LINE__);
            goto PARSE_OVER;
        } else {
            columnCase.dataLen = SMALL_BUFF_LEN;
        }

        for (int n = 0; n < count; ++n) {
            tstrncpy(superTbls->columns[tdm_index].dataType,
                    columnCase.dataType,
                    min(DATATYPE_BUFF_LEN, strlen(columnCase.dataType) + 1));

            superTbls->columns[tdm_index].dataLen = columnCase.dataLen;
            tdm_index++;
        }
    }

    if ((tdm_index + 1 /* ts */) > MAX_NUM_COLUMNS) {
        errorPrint("failed to read json, column size overflow, allowed max column size is %d\n",
                MAX_NUM_COLUMNS);
        goto PARSE_OVER;
    }

    superTbls->columnCount = tdm_index;

    for (int c = 0; c < superTbls->columnCount; c++) {
        if (0 == strncasecmp(superTbls->columns[c].dataType,
                    "INT", strlen("INT"))) {
            superTbls->columns[c].data_type = TSDB_DATA_TYPE_INT;
	    superTbls->columns[c].dataLen = sizeof(int);
        } else if (0 == strncasecmp(superTbls->columns[c].dataType,
                    "TINYINT", strlen("TINYINT"))) {
            superTbls->columns[c].data_type = TSDB_DATA_TYPE_TINYINT;
	    superTbls->columns[c].dataLen = sizeof(char);
        } else if (0 == strncasecmp(superTbls->columns[c].dataType,
                    "SMALLINT", strlen("SMALLINT"))) {
            superTbls->columns[c].data_type = TSDB_DATA_TYPE_SMALLINT;
	    superTbls->columns[c].dataLen = sizeof(int16_t);
        } else if (0 == strncasecmp(superTbls->columns[c].dataType,
                    "BIGINT", strlen("BIGINT"))) {
            superTbls->columns[c].data_type = TSDB_DATA_TYPE_BIGINT;
	    superTbls->columns[c].dataLen = sizeof(int64_t);
        } else if (0 == strncasecmp(superTbls->columns[c].dataType,
                    "FLOAT", strlen("FLOAT"))) {
            superTbls->columns[c].data_type = TSDB_DATA_TYPE_FLOAT;
	    superTbls->columns[c].dataLen = sizeof(float);
        } else if (0 == strncasecmp(superTbls->columns[c].dataType,
                    "DOUBLE", strlen("DOUBLE"))) {
            superTbls->columns[c].data_type = TSDB_DATA_TYPE_DOUBLE;
	    superTbls->columns[c].dataLen = sizeof(double);
        } else if (0 == strncasecmp(superTbls->columns[c].dataType,
                    "BINARY", strlen("BINARY"))) {
            superTbls->columns[c].data_type = TSDB_DATA_TYPE_BINARY;
        } else if (0 == strncasecmp(superTbls->columns[c].dataType,
                    "NCHAR", strlen("NCHAR"))) {
            superTbls->columns[c].data_type = TSDB_DATA_TYPE_NCHAR;
        } else if (0 == strncasecmp(superTbls->columns[c].dataType,
                    "BOOL", strlen("BOOL"))) {
            superTbls->columns[c].data_type = TSDB_DATA_TYPE_BOOL;
	    superTbls->columns[c].dataLen = sizeof(char);
        } else if (0 == strncasecmp(superTbls->columns[c].dataType,
                    "TIMESTAMP", strlen("TIMESTAMP"))) {
            superTbls->columns[c].data_type = TSDB_DATA_TYPE_TIMESTAMP;
	    superTbls->columns[c].dataLen = sizeof(int64_t);
        } else if (0 == strncasecmp(superTbls->columns[c].dataType,
                    "UTINYINT", strlen("UTINYINT"))) {
            superTbls->columns[c].data_type = TSDB_DATA_TYPE_UTINYINT;
	    superTbls->columns[c].dataLen = sizeof(char);
        } else if (0 == strncasecmp(superTbls->columns[c].dataType,
                    "USMALLINT", strlen("USMALLINT"))) {
            superTbls->columns[c].data_type = TSDB_DATA_TYPE_USMALLINT;
	    superTbls->columns[c].dataLen = sizeof(uint16_t);
        } else if (0 == strncasecmp(superTbls->columns[c].dataType,
                    "UINT", strlen("UINT"))) {
            superTbls->columns[c].data_type = TSDB_DATA_TYPE_UINT;
	    superTbls->columns[c].dataLen = sizeof(uint32_t);
        } else if (0 == strncasecmp(superTbls->columns[c].dataType,
                    "UBIGINT", strlen("UBIGINT"))) {
            superTbls->columns[c].data_type = TSDB_DATA_TYPE_UBIGINT;
	    superTbls->columns[c].dataLen = sizeof(uint64_t);
        } else {
            superTbls->columns[c].data_type = TSDB_DATA_TYPE_NULL;
        }
    }

    count = 1;
    tdm_index = 0;
    // tags
    cJSON *tags = cJSON_GetObjectItem(stbInfo, "tags");
    if (!tags || tags->type != cJSON_Array) {
        errorPrint("%s", "failed to read json, tags not found\n");
        goto PARSE_OVER;
    }

    int tagSize = cJSON_GetArraySize(tags);
    if (tagSize > TSDB_MAX_TAGS) {
        errorPrint("failed to read json, tags size overflow, max tag size is %d\n",
                TSDB_MAX_TAGS);
        goto PARSE_OVER;
    }

    //superTbls->tagCount = tagSize;
    for (int k = 0; k < tagSize; ++k) {
        cJSON* tag = cJSON_GetArrayItem(tags, k);
        if (tag == NULL) continue;

        count = 1;
        cJSON* countObj = cJSON_GetObjectItem(tag, "count");
        if (countObj && countObj->type == cJSON_Number) {
            count = countObj->valueint;
        } else if (countObj && countObj->type != cJSON_Number) {
            errorPrint("%s", "failed to read json, column count not found\n");
            goto PARSE_OVER;
        } else {
            count = 1;
        }

        // column info
        memset(&columnCase, 0, sizeof(StrColumn));
        cJSON *dataType = cJSON_GetObjectItem(tag, "type");
        if (!dataType || dataType->type != cJSON_String
                || dataType->valuestring == NULL) {
            errorPrint("%s", "failed to read json, tag type not found\n");
            goto PARSE_OVER;
        }
        tstrncpy(columnCase.dataType, dataType->valuestring,
                min(DATATYPE_BUFF_LEN, strlen(dataType->valuestring) + 1));

        cJSON* dataLen = cJSON_GetObjectItem(tag, "len");
        if (dataLen && dataLen->type == cJSON_Number) {
            columnCase.dataLen = dataLen->valueint;
        } else if (dataLen && dataLen->type != cJSON_Number) {
            errorPrint("%s", "failed to read json, column len not found\n");
            goto PARSE_OVER;
        } else {
            columnCase.dataLen = 0;
        }

        for (int n = 0; n < count; ++n) {
            tstrncpy(superTbls->tags[tdm_index].dataType, columnCase.dataType,
                    min(DATATYPE_BUFF_LEN, strlen(columnCase.dataType) + 1));
            superTbls->tags[tdm_index].dataLen = columnCase.dataLen;
            tdm_index++;
        }
    }

    if (tdm_index > TSDB_MAX_TAGS) {
        errorPrint("failed to read json, tags size overflow, allowed max tag count is %d\n",
                TSDB_MAX_TAGS);
        goto PARSE_OVER;
    }

    superTbls->tagCount = tdm_index;

    for (int t = 0; t < superTbls->tagCount; t++) {
        if (0 == strncasecmp(superTbls->tags[t].dataType,
                    "INT", strlen("INT"))) {
            superTbls->tags[t].data_type = TSDB_DATA_TYPE_INT;
        } else if (0 == strncasecmp(superTbls->tags[t].dataType,
                    "TINYINT", strlen("TINYINT"))) {
            superTbls->tags[t].data_type = TSDB_DATA_TYPE_TINYINT;
        } else if (0 == strncasecmp(superTbls->tags[t].dataType,
                    "SMALLINT", strlen("SMALLINT"))) {
            superTbls->tags[t].data_type = TSDB_DATA_TYPE_SMALLINT;
        } else if (0 == strncasecmp(superTbls->tags[t].dataType,
                    "BIGINT", strlen("BIGINT"))) {
            superTbls->tags[t].data_type = TSDB_DATA_TYPE_BIGINT;
        } else if (0 == strncasecmp(superTbls->tags[t].dataType,
                    "FLOAT", strlen("FLOAT"))) {
            superTbls->tags[t].data_type = TSDB_DATA_TYPE_FLOAT;
        } else if (0 == strncasecmp(superTbls->tags[t].dataType,
                    "DOUBLE", strlen("DOUBLE"))) {
            superTbls->tags[t].data_type = TSDB_DATA_TYPE_DOUBLE;
        } else if (0 == strncasecmp(superTbls->tags[t].dataType,
                    "BINARY", strlen("BINARY"))) {
            superTbls->tags[t].data_type = TSDB_DATA_TYPE_BINARY;
        } else if (0 == strncasecmp(superTbls->tags[t].dataType,
                    "NCHAR", strlen("NCHAR"))) {
            superTbls->tags[t].data_type = TSDB_DATA_TYPE_NCHAR;
        } else if (0 == strncasecmp(superTbls->tags[t].dataType,
                    "BOOL", strlen("BOOL"))) {
            superTbls->tags[t].data_type = TSDB_DATA_TYPE_BOOL;
        } else if (0 == strncasecmp(superTbls->tags[t].dataType,
                    "TIMESTAMP", strlen("TIMESTAMP"))) {
            superTbls->tags[t].data_type = TSDB_DATA_TYPE_TIMESTAMP;
        } else if (0 == strncasecmp(superTbls->tags[t].dataType,
                    "UTINYINT", strlen("UTINYINT"))) {
            superTbls->tags[t].data_type = TSDB_DATA_TYPE_UTINYINT;
        } else if (0 == strncasecmp(superTbls->tags[t].dataType,
                    "USMALLINT", strlen("USMALLINT"))) {
            superTbls->tags[t].data_type = TSDB_DATA_TYPE_USMALLINT;
        } else if (0 == strncasecmp(superTbls->tags[t].dataType,
                    "UINT", strlen("UINT"))) {
            superTbls->tags[t].data_type = TSDB_DATA_TYPE_UINT;
        } else if (0 == strncasecmp(superTbls->tags[t].dataType,
                    "UBIGINT", strlen("UBIGINT"))) {
            superTbls->tags[t].data_type = TSDB_DATA_TYPE_UBIGINT;
        } else {
            superTbls->tags[t].data_type = TSDB_DATA_TYPE_NULL;
        }
    }

    if ((superTbls->columnCount + superTbls->tagCount + 1 /* ts */) > TSDB_MAX_COLUMNS) {
        errorPrint("columns + tags is more than allowed max columns count: %d\n",
                TSDB_MAX_COLUMNS);
        goto PARSE_OVER;
    }
    ret = true;

PARSE_OVER:
    return ret;
}

static bool getMetaFromInsertJsonFile(cJSON* root) {
    bool  ret = false;

    cJSON* cfgdir = cJSON_GetObjectItem(root, "cfgdir");
    if (cfgdir && cfgdir->type == cJSON_String && cfgdir->valuestring != NULL) {
        tstrncpy(g_Dbs.cfgDir, cfgdir->valuestring, MAX_FILE_NAME_LEN);
    }

    cJSON* host = cJSON_GetObjectItem(root, "host");
    if (host && host->type == cJSON_String && host->valuestring != NULL) {
        tstrncpy(g_Dbs.host, host->valuestring, MAX_HOSTNAME_SIZE);
    } else if (!host) {
        tstrncpy(g_Dbs.host, "127.0.0.1", MAX_HOSTNAME_SIZE);
    } else {
        errorPrint("%s", "failed to read json, host not found\n");
        goto PARSE_OVER;
    }

    cJSON* port = cJSON_GetObjectItem(root, "port");
    if (port && port->type == cJSON_Number) {
        g_Dbs.port = port->valueint;
    } else if (!port) {
        g_Dbs.port = 6030;
    }

    cJSON* user = cJSON_GetObjectItem(root, "user");
    if (user && user->type == cJSON_String && user->valuestring != NULL) {
        tstrncpy(g_Dbs.user, user->valuestring, MAX_USERNAME_SIZE);
    } else if (!user) {
        tstrncpy(g_Dbs.user, "root", MAX_USERNAME_SIZE);
    }

    cJSON* password = cJSON_GetObjectItem(root, "password");
    if (password && password->type == cJSON_String && password->valuestring != NULL) {
        tstrncpy(g_Dbs.password, password->valuestring, SHELL_MAX_PASSWORD_LEN);
    } else if (!password) {
        tstrncpy(g_Dbs.password, "taosdata", SHELL_MAX_PASSWORD_LEN);
    }

    cJSON* resultfile = cJSON_GetObjectItem(root, "result_file");
    if (resultfile && resultfile->type == cJSON_String && resultfile->valuestring != NULL) {
        tstrncpy(g_Dbs.resultFile, resultfile->valuestring, MAX_FILE_NAME_LEN);
    } else if (!resultfile) {
        tstrncpy(g_Dbs.resultFile, "./insert_res.txt", MAX_FILE_NAME_LEN);
    }

    cJSON* threads = cJSON_GetObjectItem(root, "thread_count");
    if (threads && threads->type == cJSON_Number) {
        g_Dbs.threadCount = threads->valueint;
    } else if (!threads) {
        g_Dbs.threadCount = 1;
    } else {
        errorPrint("%s", "failed to read json, threads not found\n");
        goto PARSE_OVER;
    }

    cJSON* threads2 = cJSON_GetObjectItem(root, "thread_count_create_tbl");
    if (threads2 && threads2->type == cJSON_Number) {
        g_Dbs.threadCountForCreateTbl = threads2->valueint;
    } else if (!threads2) {
        g_Dbs.threadCountForCreateTbl = 1;
    } else {
        errorPrint("%s", "failed to read json, threads2 not found\n");
        goto PARSE_OVER;
    }

    cJSON* gInsertInterval = cJSON_GetObjectItem(root, "insert_interval");
    if (gInsertInterval && gInsertInterval->type == cJSON_Number) {
        if (gInsertInterval->valueint <0) {
            errorPrint("%s", "failed to read json, insert interval input mistake\n");
            goto PARSE_OVER;
        }
        g_args.insert_interval = gInsertInterval->valueint;
    } else if (!gInsertInterval) {
        g_args.insert_interval = 0;
    } else {
        errorPrint("%s", "failed to read json, insert_interval input mistake\n");
        goto PARSE_OVER;
    }

    cJSON* interlaceRows = cJSON_GetObjectItem(root, "interlace_rows");
    if (interlaceRows && interlaceRows->type == cJSON_Number) {
        if (interlaceRows->valueint < 0) {
            errorPrint("%s", "failed to read json, interlaceRows input mistake\n");
            goto PARSE_OVER;

        }
        g_args.interlaceRows = interlaceRows->valueint;
    } else if (!interlaceRows) {
        g_args.interlaceRows = 0; // 0 means progressive mode, > 0 mean interlace mode. max value is less or equ num_of_records_per_req
    } else {
        errorPrint("%s", "failed to read json, interlaceRows input mistake\n");
        goto PARSE_OVER;
    }

    cJSON* maxSqlLen = cJSON_GetObjectItem(root, "max_sql_len");
    if (maxSqlLen && maxSqlLen->type == cJSON_Number) {
        if (maxSqlLen->valueint < 0) {
            errorPrint("%s() LN%d, failed to read json, max_sql_len input mistake\n",
                    __func__, __LINE__);
            goto PARSE_OVER;
        }
        g_args.max_sql_len = maxSqlLen->valueint;
    } else if (!maxSqlLen) {
        g_args.max_sql_len = (1024*1024);
    } else {
        errorPrint("%s() LN%d, failed to read json, max_sql_len input mistake\n",
                __func__, __LINE__);
        goto PARSE_OVER;
    }

    cJSON* numRecPerReq = cJSON_GetObjectItem(root, "num_of_records_per_req");
    if (numRecPerReq && numRecPerReq->type == cJSON_Number) {
        if (numRecPerReq->valueint <= 0) {
            errorPrint("%s() LN%d, failed to read json, num_of_records_per_req input mistake\n",
                    __func__, __LINE__);
            goto PARSE_OVER;
        } else if (numRecPerReq->valueint > MAX_RECORDS_PER_REQ) {
            printf("NOTICE: number of records per request value %"PRIu64" > %d\n\n",
                    numRecPerReq->valueint, MAX_RECORDS_PER_REQ);
            printf("        number of records per request value will be set to %d\n\n",
                    MAX_RECORDS_PER_REQ);
            prompt();
            numRecPerReq->valueint = MAX_RECORDS_PER_REQ;
        }
        g_args.reqPerReq = numRecPerReq->valueint;
    } else if (!numRecPerReq) {
        g_args.reqPerReq = MAX_RECORDS_PER_REQ;
    } else {
        errorPrint("%s() LN%d, failed to read json, num_of_records_per_req not found\n",
                __func__, __LINE__);
        goto PARSE_OVER;
    }

    cJSON* prepareRand = cJSON_GetObjectItem(root, "prepared_rand");
    if (prepareRand && prepareRand->type == cJSON_Number) {
        if (prepareRand->valueint <= 0) {
            errorPrint("%s() LN%d, failed to read json, prepared_rand input mistake\n",
                    __func__, __LINE__);
            goto PARSE_OVER;
        }
        g_args.prepared_rand = prepareRand->valueint;
    } else if (!prepareRand) {
        g_args.prepared_rand = 10000;
    } else {
        errorPrint("%s() LN%d, failed to read json, prepared_rand not found\n",
                __func__, __LINE__);
        goto PARSE_OVER;
    }

    cJSON *answerPrompt = cJSON_GetObjectItem(root, "confirm_parameter_prompt"); // yes, no,
    if (answerPrompt
            && answerPrompt->type == cJSON_String
            && answerPrompt->valuestring != NULL) {
        if (0 == strncasecmp(answerPrompt->valuestring, "yes", 3)) {
            g_args.answer_yes = false;
        } else if (0 == strncasecmp(answerPrompt->valuestring, "no", 2)) {
            g_args.answer_yes = true;
        } else {
            g_args.answer_yes = false;
        }
    } else if (!answerPrompt) {
        g_args.answer_yes = true;   // default is no, mean answer_yes.
    } else {
        errorPrint("%s", "failed to read json, confirm_parameter_prompt input mistake\n");
        goto PARSE_OVER;
    }

    // rows per table need be less than insert batch
    if (g_args.interlaceRows > g_args.reqPerReq) {
        printf("NOTICE: interlace rows value %u > num_of_records_per_req %u\n\n",
                g_args.interlaceRows, g_args.reqPerReq);
        printf("        interlace rows value will be set to num_of_records_per_req %u\n\n",
                g_args.reqPerReq);
        prompt();
        g_args.interlaceRows = g_args.reqPerReq;
    }

    cJSON* dbs = cJSON_GetObjectItem(root, "databases");
    if (!dbs || dbs->type != cJSON_Array) {
        errorPrint("%s", "failed to read json, databases not found\n");
        goto PARSE_OVER;
    }

    int dbSize = cJSON_GetArraySize(dbs);
    if (dbSize > MAX_DB_COUNT) {
        errorPrint(
                "failed to read json, databases size overflow, max database is %d\n",
                MAX_DB_COUNT);
        goto PARSE_OVER;
    }
    g_Dbs.db = calloc(1, sizeof(SDataBase)*dbSize);
    assert(g_Dbs.db);
    g_Dbs.dbCount = dbSize;
    for (int i = 0; i < dbSize; ++i) {
        cJSON* dbinfos = cJSON_GetArrayItem(dbs, i);
        if (dbinfos == NULL) continue;

        // dbinfo
        cJSON *dbinfo = cJSON_GetObjectItem(dbinfos, "dbinfo");
        if (!dbinfo || dbinfo->type != cJSON_Object) {
            errorPrint("%s", "failed to read json, dbinfo not found\n");
            goto PARSE_OVER;
        }

        cJSON *dbName = cJSON_GetObjectItem(dbinfo, "name");
        if (!dbName || dbName->type != cJSON_String || dbName->valuestring == NULL) {
            errorPrint("%s", "failed to read json, db name not found\n");
            goto PARSE_OVER;
        }
        tstrncpy(g_Dbs.db[i].dbName, dbName->valuestring, TSDB_DB_NAME_LEN);

        cJSON *drop = cJSON_GetObjectItem(dbinfo, "drop");
        if (drop && drop->type == cJSON_String && drop->valuestring != NULL) {
            if (0 == strncasecmp(drop->valuestring, "yes", strlen("yes"))) {
                g_Dbs.db[i].drop = true;
            } else {
                g_Dbs.db[i].drop = false;
            }
        } else if (!drop) {
            g_Dbs.db[i].drop = g_args.drop_database;
        } else {
            errorPrint("%s", "failed to read json, drop input mistake\n");
            goto PARSE_OVER;
        }

        cJSON *precision = cJSON_GetObjectItem(dbinfo, "precision");
        if (precision && precision->type == cJSON_String
                && precision->valuestring != NULL) {
            tstrncpy(g_Dbs.db[i].dbCfg.precision, precision->valuestring,
                    SMALL_BUFF_LEN);
        } else if (!precision) {
            memset(g_Dbs.db[i].dbCfg.precision, 0, SMALL_BUFF_LEN);
        } else {
            errorPrint("%s", "failed to read json, precision not found\n");
            goto PARSE_OVER;
        }

        cJSON* update = cJSON_GetObjectItem(dbinfo, "update");
        if (update && update->type == cJSON_Number) {
            g_Dbs.db[i].dbCfg.update = update->valueint;
        } else if (!update) {
            g_Dbs.db[i].dbCfg.update = -1;
        } else {
            errorPrint("%s", "failed to read json, update not found\n");
            goto PARSE_OVER;
        }

        cJSON* replica = cJSON_GetObjectItem(dbinfo, "replica");
        if (replica && replica->type == cJSON_Number) {
            g_Dbs.db[i].dbCfg.replica = replica->valueint;
        } else if (!replica) {
            g_Dbs.db[i].dbCfg.replica = -1;
        } else {
            errorPrint("%s", "failed to read json, replica not found\n");
            goto PARSE_OVER;
        }

        cJSON* keep = cJSON_GetObjectItem(dbinfo, "keep");
        if (keep && keep->type == cJSON_Number) {
            g_Dbs.db[i].dbCfg.keep = keep->valueint;
        } else if (!keep) {
            g_Dbs.db[i].dbCfg.keep = -1;
        } else {
            errorPrint("%s", "failed to read json, keep not found\n");
            goto PARSE_OVER;
        }

        cJSON* days = cJSON_GetObjectItem(dbinfo, "days");
        if (days && days->type == cJSON_Number) {
            g_Dbs.db[i].dbCfg.days = days->valueint;
        } else if (!days) {
            g_Dbs.db[i].dbCfg.days = -1;
        } else {
            errorPrint("%s", "failed to read json, days not found\n");
            goto PARSE_OVER;
        }

        cJSON* cache = cJSON_GetObjectItem(dbinfo, "cache");
        if (cache && cache->type == cJSON_Number) {
            g_Dbs.db[i].dbCfg.cache = cache->valueint;
        } else if (!cache) {
            g_Dbs.db[i].dbCfg.cache = -1;
        } else {
            errorPrint("%s", "failed to read json, cache not found\n");
            goto PARSE_OVER;
        }

        cJSON* blocks= cJSON_GetObjectItem(dbinfo, "blocks");
        if (blocks && blocks->type == cJSON_Number) {
            g_Dbs.db[i].dbCfg.blocks = blocks->valueint;
        } else if (!blocks) {
            g_Dbs.db[i].dbCfg.blocks = -1;
        } else {
            errorPrint("%s", "failed to read json, block not found\n");
            goto PARSE_OVER;
        }

        //cJSON* maxtablesPerVnode= cJSON_GetObjectItem(dbinfo, "maxtablesPerVnode");
        //if (maxtablesPerVnode && maxtablesPerVnode->type == cJSON_Number) {
        //  g_Dbs.db[i].dbCfg.maxtablesPerVnode = maxtablesPerVnode->valueint;
        //} else if (!maxtablesPerVnode) {
        //  g_Dbs.db[i].dbCfg.maxtablesPerVnode = TSDB_DEFAULT_TABLES;
        //} else {
        // printf("failed to read json, maxtablesPerVnode not found");
        // goto PARSE_OVER;
        //}

        cJSON* minRows= cJSON_GetObjectItem(dbinfo, "minRows");
        if (minRows && minRows->type == cJSON_Number) {
            g_Dbs.db[i].dbCfg.minRows = minRows->valueint;
        } else if (!minRows) {
            g_Dbs.db[i].dbCfg.minRows = 0;    // 0 means default
        } else {
            errorPrint("%s", "failed to read json, minRows not found\n");
            goto PARSE_OVER;
        }

        cJSON* maxRows= cJSON_GetObjectItem(dbinfo, "maxRows");
        if (maxRows && maxRows->type == cJSON_Number) {
            g_Dbs.db[i].dbCfg.maxRows = maxRows->valueint;
        } else if (!maxRows) {
            g_Dbs.db[i].dbCfg.maxRows = 0;    // 0 means default
        } else {
            errorPrint("%s", "failed to read json, maxRows not found\n");
            goto PARSE_OVER;
        }

        cJSON* comp= cJSON_GetObjectItem(dbinfo, "comp");
        if (comp && comp->type == cJSON_Number) {
            g_Dbs.db[i].dbCfg.comp = comp->valueint;
        } else if (!comp) {
            g_Dbs.db[i].dbCfg.comp = -1;
        } else {
            errorPrint("%s", "failed to read json, comp not found\n");
            goto PARSE_OVER;
        }

        cJSON* walLevel= cJSON_GetObjectItem(dbinfo, "walLevel");
        if (walLevel && walLevel->type == cJSON_Number) {
            g_Dbs.db[i].dbCfg.walLevel = walLevel->valueint;
        } else if (!walLevel) {
            g_Dbs.db[i].dbCfg.walLevel = -1;
        } else {
            errorPrint("%s", "failed to read json, walLevel not found\n");
            goto PARSE_OVER;
        }

        cJSON* cacheLast= cJSON_GetObjectItem(dbinfo, "cachelast");
        if (cacheLast && cacheLast->type == cJSON_Number) {
            g_Dbs.db[i].dbCfg.cacheLast = cacheLast->valueint;
        } else if (!cacheLast) {
            g_Dbs.db[i].dbCfg.cacheLast = -1;
        } else {
            errorPrint("%s", "failed to read json, cacheLast not found\n");
            goto PARSE_OVER;
        }

        cJSON* quorum= cJSON_GetObjectItem(dbinfo, "quorum");
        if (quorum && quorum->type == cJSON_Number) {
            g_Dbs.db[i].dbCfg.quorum = quorum->valueint;
        } else if (!quorum) {
            g_Dbs.db[i].dbCfg.quorum = 1;
        } else {
            printf("failed to read json, quorum input mistake");
            goto PARSE_OVER;
        }

        cJSON* kit_fsync= cJSON_GetObjectItem(dbinfo, "fsync");
        if (kit_fsync && kit_fsync->type == cJSON_Number) {
            g_Dbs.db[i].dbCfg.fsync = kit_fsync->valueint;
        } else if (!kit_fsync) {
            g_Dbs.db[i].dbCfg.fsync = -1;
        } else {
            errorPrint("%s", "failed to read json, fsync input mistake\n");
            goto PARSE_OVER;
        }

        // super_tables
        cJSON *stables = cJSON_GetObjectItem(dbinfos, "super_tables");
        if (!stables || stables->type != cJSON_Array) {
            errorPrint("%s", "failed to read json, super_tables not found\n");
            goto PARSE_OVER;
        }

        int stbSize = cJSON_GetArraySize(stables);
        if (stbSize > MAX_SUPER_TABLE_COUNT) {
            errorPrint(
                    "failed to read json, supertable size overflow, max supertable is %d\n",
                    MAX_SUPER_TABLE_COUNT);
            goto PARSE_OVER;
        }
        g_Dbs.db[i].superTbls = calloc(1, stbSize * sizeof(SSuperTable));
        assert(g_Dbs.db[i].superTbls);
        g_Dbs.db[i].superTblCount = stbSize;
        for (int j = 0; j < stbSize; ++j) {
            cJSON* stbInfo = cJSON_GetArrayItem(stables, j);
            if (stbInfo == NULL) continue;

            // dbinfo
            cJSON *stbName = cJSON_GetObjectItem(stbInfo, "name");
            if (!stbName || stbName->type != cJSON_String
                    || stbName->valuestring == NULL) {
                errorPrint("%s", "failed to read json, stb name not found\n");
                goto PARSE_OVER;
            }
            tstrncpy(g_Dbs.db[i].superTbls[j].stbName, stbName->valuestring,
                    TSDB_TABLE_NAME_LEN);

            cJSON *prefix = cJSON_GetObjectItem(stbInfo, "childtable_prefix");
            if (!prefix || prefix->type != cJSON_String || prefix->valuestring == NULL) {
                errorPrint("%s", "failed to read json, childtable_prefix not found\n");
                goto PARSE_OVER;
            }
            tstrncpy(g_Dbs.db[i].superTbls[j].childTblPrefix, prefix->valuestring,
                    TBNAME_PREFIX_LEN);

            cJSON *autoCreateTbl = cJSON_GetObjectItem(stbInfo, "auto_create_table");
            if (autoCreateTbl
                    && autoCreateTbl->type == cJSON_String
                    && autoCreateTbl->valuestring != NULL) {
                if ((0 == strncasecmp(autoCreateTbl->valuestring, "yes", 3))
                        && (TBL_ALREADY_EXISTS != g_Dbs.db[i].superTbls[j].childTblExists)) {
                    g_Dbs.db[i].superTbls[j].autoCreateTable = AUTO_CREATE_SUBTBL;
                } else if (0 == strncasecmp(autoCreateTbl->valuestring, "no", 2)) {
                    g_Dbs.db[i].superTbls[j].autoCreateTable = PRE_CREATE_SUBTBL;
                } else {
                    g_Dbs.db[i].superTbls[j].autoCreateTable = PRE_CREATE_SUBTBL;
                }
            } else if (!autoCreateTbl) {
                g_Dbs.db[i].superTbls[j].autoCreateTable = PRE_CREATE_SUBTBL;
            } else {
                errorPrint("%s", "failed to read json, auto_create_table not found\n");
                goto PARSE_OVER;
            }

            cJSON* batchCreateTbl = cJSON_GetObjectItem(stbInfo, "batch_create_tbl_num");
            if (batchCreateTbl && batchCreateTbl->type == cJSON_Number) {
                g_Dbs.db[i].superTbls[j].batchCreateTableNum = batchCreateTbl->valueint;
            } else if (!batchCreateTbl) {
                g_Dbs.db[i].superTbls[j].batchCreateTableNum = 10;
            } else {
                errorPrint("%s", "failed to read json, batch_create_tbl_num not found\n");
                goto PARSE_OVER;
            }

            cJSON *childTblExists = cJSON_GetObjectItem(stbInfo, "child_table_exists"); // yes, no
            if (childTblExists
                    && childTblExists->type == cJSON_String
                    && childTblExists->valuestring != NULL) {
                if ((0 == strncasecmp(childTblExists->valuestring, "yes", 3))
                        && (g_Dbs.db[i].drop == false)) {
                    g_Dbs.db[i].superTbls[j].childTblExists = TBL_ALREADY_EXISTS;
                } else if ((0 == strncasecmp(childTblExists->valuestring, "no", 2)
                            || (g_Dbs.db[i].drop == true))) {
                    g_Dbs.db[i].superTbls[j].childTblExists = TBL_NO_EXISTS;
                } else {
                    g_Dbs.db[i].superTbls[j].childTblExists = TBL_NO_EXISTS;
                }
            } else if (!childTblExists) {
                g_Dbs.db[i].superTbls[j].childTblExists = TBL_NO_EXISTS;
            } else {
                errorPrint("%s",
                        "failed to read json, child_table_exists not found\n");
                goto PARSE_OVER;
            }

            if (TBL_ALREADY_EXISTS == g_Dbs.db[i].superTbls[j].childTblExists) {
                g_Dbs.db[i].superTbls[j].autoCreateTable = PRE_CREATE_SUBTBL;
            }

            cJSON* count = cJSON_GetObjectItem(stbInfo, "childtable_count");
            if (!count || count->type != cJSON_Number || 0 >= count->valueint) {
                errorPrint("%s",
                        "failed to read json, childtable_count input mistake\n");
                goto PARSE_OVER;
            }
            g_Dbs.db[i].superTbls[j].childTblCount = count->valueint;
            g_totalChildTables += g_Dbs.db[i].superTbls[j].childTblCount;

            cJSON *dataSource = cJSON_GetObjectItem(stbInfo, "data_source");
            if (dataSource && dataSource->type == cJSON_String
                    && dataSource->valuestring != NULL) {
                tstrncpy(g_Dbs.db[i].superTbls[j].dataSource,
                        dataSource->valuestring,
                        min(SMALL_BUFF_LEN, strlen(dataSource->valuestring) + 1));
            } else if (!dataSource) {
                tstrncpy(g_Dbs.db[i].superTbls[j].dataSource, "rand",
                        min(SMALL_BUFF_LEN, strlen("rand") + 1));
            } else {
                errorPrint("%s", "failed to read json, data_source not found\n");
                goto PARSE_OVER;
            }

            cJSON *stbIface = cJSON_GetObjectItem(stbInfo, "insert_mode"); // taosc , rest, stmt
            if (stbIface && stbIface->type == cJSON_String
                    && stbIface->valuestring != NULL) {
                if (0 == strcasecmp(stbIface->valuestring, "taosc")) {
                    g_Dbs.db[i].superTbls[j].iface= TAOSC_IFACE;
                } else if (0 == strcasecmp(stbIface->valuestring, "rest")) {
                    g_Dbs.db[i].superTbls[j].iface= REST_IFACE;
                } else if (0 == strcasecmp(stbIface->valuestring, "stmt")) {
                    g_Dbs.db[i].superTbls[j].iface= STMT_IFACE;
                } else {
                    errorPrint("failed to read json, insert_mode %s not recognized\n",
                            stbIface->valuestring);
                    goto PARSE_OVER;
                }
            } else if (!stbIface) {
                g_Dbs.db[i].superTbls[j].iface = TAOSC_IFACE;
            } else {
                errorPrint("%s", "failed to read json, insert_mode not found\n");
                goto PARSE_OVER;
            }

            cJSON* childTbl_limit = cJSON_GetObjectItem(stbInfo, "childtable_limit");
            if ((childTbl_limit) && (g_Dbs.db[i].drop != true)
                    && (g_Dbs.db[i].superTbls[j].childTblExists == TBL_ALREADY_EXISTS)) {
                if (childTbl_limit->type != cJSON_Number) {
                    errorPrint("%s", "failed to read json, childtable_limit\n");
                    goto PARSE_OVER;
                }
                g_Dbs.db[i].superTbls[j].childTblLimit = childTbl_limit->valueint;
            } else {
                g_Dbs.db[i].superTbls[j].childTblLimit = -1;    // select ... limit -1 means all query result, drop = yes mean all table need recreate, limit value is invalid.
            }

            cJSON* childTbl_offset = cJSON_GetObjectItem(stbInfo, "childtable_offset");
            if ((childTbl_offset) && (g_Dbs.db[i].drop != true)
                    && (g_Dbs.db[i].superTbls[j].childTblExists == TBL_ALREADY_EXISTS)) {
                if ((childTbl_offset->type != cJSON_Number)
                        || (0 > childTbl_offset->valueint)) {
                    errorPrint("%s", "failed to read json, childtable_offset\n");
                    goto PARSE_OVER;
                }
                g_Dbs.db[i].superTbls[j].childTblOffset = childTbl_offset->valueint;
            } else {
                g_Dbs.db[i].superTbls[j].childTblOffset = 0;
            }

            cJSON *ts = cJSON_GetObjectItem(stbInfo, "start_timestamp");
            if (ts && ts->type == cJSON_String && ts->valuestring != NULL) {
                tstrncpy(g_Dbs.db[i].superTbls[j].startTimestamp,
                        ts->valuestring, TSDB_DB_NAME_LEN);
            } else if (!ts) {
                tstrncpy(g_Dbs.db[i].superTbls[j].startTimestamp,
                        "now", TSDB_DB_NAME_LEN);
            } else {
                errorPrint("%s", "failed to read json, start_timestamp not found\n");
                goto PARSE_OVER;
            }

            cJSON* timestampStep = cJSON_GetObjectItem(stbInfo, "timestamp_step");
            if (timestampStep && timestampStep->type == cJSON_Number) {
                g_Dbs.db[i].superTbls[j].timeStampStep = timestampStep->valueint;
            } else if (!timestampStep) {
                g_Dbs.db[i].superTbls[j].timeStampStep = g_args.timestamp_step;
            } else {
                errorPrint("%s", "failed to read json, timestamp_step not found\n");
                goto PARSE_OVER;
            }

            cJSON *sampleFormat = cJSON_GetObjectItem(stbInfo, "sample_format");
            if (sampleFormat && sampleFormat->type
                    == cJSON_String && sampleFormat->valuestring != NULL) {
                tstrncpy(g_Dbs.db[i].superTbls[j].sampleFormat,
                        sampleFormat->valuestring,
                        min(SMALL_BUFF_LEN,
                            strlen(sampleFormat->valuestring) + 1));
            } else if (!sampleFormat) {
                tstrncpy(g_Dbs.db[i].superTbls[j].sampleFormat, "csv",
                        SMALL_BUFF_LEN);
            } else {
                errorPrint("%s", "failed to read json, sample_format not found\n");
                goto PARSE_OVER;
            }

            cJSON *sampleFile = cJSON_GetObjectItem(stbInfo, "sample_file");
            if (sampleFile && sampleFile->type == cJSON_String
                    && sampleFile->valuestring != NULL) {
                tstrncpy(g_Dbs.db[i].superTbls[j].sampleFile,
                        sampleFile->valuestring,
                        min(MAX_FILE_NAME_LEN,
                            strlen(sampleFile->valuestring) + 1));
            } else if (!sampleFile) {
                memset(g_Dbs.db[i].superTbls[j].sampleFile, 0,
                        MAX_FILE_NAME_LEN);
            } else {
                errorPrint("%s", "failed to read json, sample_file not found\n");
                goto PARSE_OVER;
            }

            cJSON *useSampleTs = cJSON_GetObjectItem(stbInfo, "use_sample_ts");
            if (useSampleTs && useSampleTs->type == cJSON_String
                    && useSampleTs->valuestring != NULL) {
                if (0 == strncasecmp(useSampleTs->valuestring, "yes", 3)) {
                    g_Dbs.db[i].superTbls[j].useSampleTs = true;
                } else if (0 == strncasecmp(useSampleTs->valuestring, "no", 2)){
                    g_Dbs.db[i].superTbls[j].useSampleTs = false;
                } else {
                    g_Dbs.db[i].superTbls[j].useSampleTs = false;
                }
            } else if (!useSampleTs) {
                g_Dbs.db[i].superTbls[j].useSampleTs = false;
            } else {
                errorPrint("%s", "failed to read json, use_sample_ts not found\n");
                goto PARSE_OVER;
            }

            cJSON *tagsFile = cJSON_GetObjectItem(stbInfo, "tags_file");
            if ((tagsFile && tagsFile->type == cJSON_String)
                    && (tagsFile->valuestring != NULL)) {
                tstrncpy(g_Dbs.db[i].superTbls[j].tagsFile,
                        tagsFile->valuestring, MAX_FILE_NAME_LEN);
                if (0 == g_Dbs.db[i].superTbls[j].tagsFile[0]) {
                    g_Dbs.db[i].superTbls[j].tagSource = 0;
                } else {
                    g_Dbs.db[i].superTbls[j].tagSource = 1;
                }
            } else if (!tagsFile) {
                memset(g_Dbs.db[i].superTbls[j].tagsFile, 0, MAX_FILE_NAME_LEN);
                g_Dbs.db[i].superTbls[j].tagSource = 0;
            } else {
                errorPrint("%s", "failed to read json, tags_file not found\n");
                goto PARSE_OVER;
            }

            cJSON* stbMaxSqlLen = cJSON_GetObjectItem(stbInfo, "max_sql_len");
            if (stbMaxSqlLen && stbMaxSqlLen->type == cJSON_Number) {
                int32_t len = stbMaxSqlLen->valueint;
                if (len > TSDB_MAX_ALLOWED_SQL_LEN) {
                    len = TSDB_MAX_ALLOWED_SQL_LEN;
                } else if (len < 5) {
                    len = 5;
                }
                g_Dbs.db[i].superTbls[j].maxSqlLen = len;
            } else if (!maxSqlLen) {
                g_Dbs.db[i].superTbls[j].maxSqlLen = g_args.max_sql_len;
            } else {
                errorPrint("%s", "failed to read json, stbMaxSqlLen input mistake\n");
                goto PARSE_OVER;
            }
            /*
               cJSON *multiThreadWriteOneTbl =
               cJSON_GetObjectItem(stbInfo, "multi_thread_write_one_tbl"); // no , yes
               if (multiThreadWriteOneTbl
               && multiThreadWriteOneTbl->type == cJSON_String
               && multiThreadWriteOneTbl->valuestring != NULL) {
               if (0 == strncasecmp(multiThreadWriteOneTbl->valuestring, "yes", 3)) {
               g_Dbs.db[i].superTbls[j].multiThreadWriteOneTbl = 1;
               } else {
               g_Dbs.db[i].superTbls[j].multiThreadWriteOneTbl = 0;
               }
               } else if (!multiThreadWriteOneTbl) {
               g_Dbs.db[i].superTbls[j].multiThreadWriteOneTbl = 0;
               } else {
               errorPrint("%s", "failed to read json, multiThreadWriteOneTbl not found\n");
               goto PARSE_OVER;
               }
               */
            cJSON* insertRows = cJSON_GetObjectItem(stbInfo, "insert_rows");
            if (insertRows && insertRows->type == cJSON_Number) {
                if (insertRows->valueint < 0) {
                    errorPrint("%s", "failed to read json, insert_rows input mistake\n");
                    goto PARSE_OVER;
                }
                g_Dbs.db[i].superTbls[j].insertRows = insertRows->valueint;
            } else if (!insertRows) {
                g_Dbs.db[i].superTbls[j].insertRows = 0x7FFFFFFFFFFFFFFF;
            } else {
                errorPrint("%s", "failed to read json, insert_rows input mistake\n");
                goto PARSE_OVER;
            }

            cJSON* stbInterlaceRows = cJSON_GetObjectItem(stbInfo, "interlace_rows");
            if (stbInterlaceRows && stbInterlaceRows->type == cJSON_Number) {
                if (stbInterlaceRows->valueint < 0) {
                    errorPrint("%s", "failed to read json, interlace rows input mistake\n");
                    goto PARSE_OVER;
                }
                g_Dbs.db[i].superTbls[j].interlaceRows = stbInterlaceRows->valueint;

                if (g_Dbs.db[i].superTbls[j].interlaceRows > g_Dbs.db[i].superTbls[j].insertRows) {
                    printf("NOTICE: db[%d].superTbl[%d]'s interlace rows value %u > insert_rows %"PRId64"\n\n",
                            i, j, g_Dbs.db[i].superTbls[j].interlaceRows,
                            g_Dbs.db[i].superTbls[j].insertRows);
                    printf("        interlace rows value will be set to insert_rows %"PRId64"\n\n",
                            g_Dbs.db[i].superTbls[j].insertRows);
                    prompt();
                    g_Dbs.db[i].superTbls[j].interlaceRows = g_Dbs.db[i].superTbls[j].insertRows;
                }
            } else if (!stbInterlaceRows) {
                g_Dbs.db[i].superTbls[j].interlaceRows = g_args.interlaceRows; // 0 means progressive mode, > 0 mean interlace mode. max value is less or equ num_of_records_per_req
            } else {
                errorPrint(
                        "%s", "failed to read json, interlace rows input mistake\n");
                goto PARSE_OVER;
            }

            cJSON* disorderRatio = cJSON_GetObjectItem(stbInfo, "disorder_ratio");
            if (disorderRatio && disorderRatio->type == cJSON_Number) {
                if (disorderRatio->valueint > 50)
                    disorderRatio->valueint = 50;

                if (disorderRatio->valueint < 0)
                    disorderRatio->valueint = 0;

                g_Dbs.db[i].superTbls[j].disorderRatio = disorderRatio->valueint;
            } else if (!disorderRatio) {
                g_Dbs.db[i].superTbls[j].disorderRatio = 0;
            } else {
                errorPrint("%s", "failed to read json, disorderRatio not found\n");
                goto PARSE_OVER;
            }

            cJSON* disorderRange = cJSON_GetObjectItem(stbInfo, "disorder_range");
            if (disorderRange && disorderRange->type == cJSON_Number) {
                g_Dbs.db[i].superTbls[j].disorderRange = disorderRange->valueint;
            } else if (!disorderRange) {
                g_Dbs.db[i].superTbls[j].disorderRange = 1000;
            } else {
                errorPrint("%s", "failed to read json, disorderRange not found\n");
                goto PARSE_OVER;
            }

            cJSON* insertInterval = cJSON_GetObjectItem(stbInfo, "insert_interval");
            if (insertInterval && insertInterval->type == cJSON_Number) {
                g_Dbs.db[i].superTbls[j].insertInterval = insertInterval->valueint;
                if (insertInterval->valueint < 0) {
                    errorPrint("%s", "failed to read json, insert_interval input mistake\n");
                    goto PARSE_OVER;
                }
            } else if (!insertInterval) {
                verbosePrint("%s() LN%d: stable insert interval be overrode by global %"PRIu64".\n",
                        __func__, __LINE__, g_args.insert_interval);
                g_Dbs.db[i].superTbls[j].insertInterval = g_args.insert_interval;
            } else {
                errorPrint("%s", "failed to read json, insert_interval input mistake\n");
                goto PARSE_OVER;
            }

            int retVal = getColumnAndTagTypeFromInsertJsonFile(
                    stbInfo, &g_Dbs.db[i].superTbls[j]);
            if (false == retVal) {
                goto PARSE_OVER;
            }
        }
    }

    ret = true;

PARSE_OVER:
    return ret;
}

static bool getMetaFromQueryJsonFile(cJSON* root) {
    bool  ret = false;

    cJSON* cfgdir = cJSON_GetObjectItem(root, "cfgdir");
    if (cfgdir && cfgdir->type == cJSON_String && cfgdir->valuestring != NULL) {
        tstrncpy(g_queryInfo.cfgDir, cfgdir->valuestring, MAX_FILE_NAME_LEN);
    }

    cJSON* host = cJSON_GetObjectItem(root, "host");
    if (host && host->type == cJSON_String && host->valuestring != NULL) {
        tstrncpy(g_queryInfo.host, host->valuestring, MAX_HOSTNAME_SIZE);
    } else if (!host) {
        tstrncpy(g_queryInfo.host, "127.0.0.1", MAX_HOSTNAME_SIZE);
    } else {
        errorPrint("%s", "failed to read json, host not found\n");
        goto PARSE_OVER;
    }

    cJSON* port = cJSON_GetObjectItem(root, "port");
    if (port && port->type == cJSON_Number) {
        g_queryInfo.port = port->valueint;
    } else if (!port) {
        g_queryInfo.port = 6030;
    }

    cJSON* user = cJSON_GetObjectItem(root, "user");
    if (user && user->type == cJSON_String && user->valuestring != NULL) {
        tstrncpy(g_queryInfo.user, user->valuestring, MAX_USERNAME_SIZE);
    } else if (!user) {
        tstrncpy(g_queryInfo.user, "root", MAX_USERNAME_SIZE); ;
    }

    cJSON* password = cJSON_GetObjectItem(root, "password");
    if (password && password->type == cJSON_String && password->valuestring != NULL) {
        tstrncpy(g_queryInfo.password, password->valuestring, SHELL_MAX_PASSWORD_LEN);
    } else if (!password) {
        tstrncpy(g_queryInfo.password, "taosdata", SHELL_MAX_PASSWORD_LEN);;
    }

    cJSON *answerPrompt = cJSON_GetObjectItem(root, "confirm_parameter_prompt"); // yes, no,
    if (answerPrompt && answerPrompt->type == cJSON_String
            && answerPrompt->valuestring != NULL) {
        if (0 == strncasecmp(answerPrompt->valuestring, "yes", 3)) {
            g_args.answer_yes = false;
        } else if (0 == strncasecmp(answerPrompt->valuestring, "no", 2)) {
            g_args.answer_yes = true;
        } else {
            g_args.answer_yes = false;
        }
    } else if (!answerPrompt) {
        g_args.answer_yes = false;
    } else {
        errorPrint("%s", "failed to read json, confirm_parameter_prompt not found\n");
        goto PARSE_OVER;
    }

    cJSON* gQueryTimes = cJSON_GetObjectItem(root, "query_times");
    if (gQueryTimes && gQueryTimes->type == cJSON_Number) {
        if (gQueryTimes->valueint <= 0) {
            errorPrint("%s()", "failed to read json, query_times input mistake\n");
            goto PARSE_OVER;
        }
        g_args.query_times = gQueryTimes->valueint;
    } else if (!gQueryTimes) {
        g_args.query_times = 1;
    } else {
        errorPrint("%s", "failed to read json, query_times input mistake\n");
        goto PARSE_OVER;
    }

    cJSON* dbs = cJSON_GetObjectItem(root, "databases");
    if (dbs && dbs->type == cJSON_String && dbs->valuestring != NULL) {
        tstrncpy(g_queryInfo.dbName, dbs->valuestring, TSDB_DB_NAME_LEN);
    } else if (!dbs) {
        errorPrint("%s", "failed to read json, databases not found\n");
        goto PARSE_OVER;
    }

    cJSON* queryMode = cJSON_GetObjectItem(root, "query_mode");
    if (queryMode
            && queryMode->type == cJSON_String
            && queryMode->valuestring != NULL) {
        tstrncpy(g_queryInfo.queryMode, queryMode->valuestring,
                min(SMALL_BUFF_LEN, strlen(queryMode->valuestring) + 1));
    } else if (!queryMode) {
        tstrncpy(g_queryInfo.queryMode, "taosc",
                min(SMALL_BUFF_LEN, strlen("taosc") + 1));
    } else {
        errorPrint("%s", "failed to read json, query_mode not found\n");
        goto PARSE_OVER;
    }

    // specified_table_query
    cJSON *specifiedQuery = cJSON_GetObjectItem(root, "specified_table_query");
    if (!specifiedQuery) {
        g_queryInfo.specifiedQueryInfo.concurrent = 1;
        g_queryInfo.specifiedQueryInfo.sqlCount = 0;
    } else if (specifiedQuery->type != cJSON_Object) {
        errorPrint("%s", "failed to read json, super_table_query not found\n");
        goto PARSE_OVER;
    } else {
        cJSON* queryInterval = cJSON_GetObjectItem(specifiedQuery, "query_interval");
        if (queryInterval && queryInterval->type == cJSON_Number) {
            g_queryInfo.specifiedQueryInfo.queryInterval = queryInterval->valueint;
        } else if (!queryInterval) {
            g_queryInfo.specifiedQueryInfo.queryInterval = 0;
        }

        cJSON* specifiedQueryTimes = cJSON_GetObjectItem(specifiedQuery,
                "query_times");
        if (specifiedQueryTimes && specifiedQueryTimes->type == cJSON_Number) {
            if (specifiedQueryTimes->valueint <= 0) {
                errorPrint(
                        "failed to read json, query_times: %"PRId64", need be a valid (>0) number\n",
                        specifiedQueryTimes->valueint);
                goto PARSE_OVER;

            }
            g_queryInfo.specifiedQueryInfo.queryTimes = specifiedQueryTimes->valueint;
        } else if (!specifiedQueryTimes) {
            g_queryInfo.specifiedQueryInfo.queryTimes = g_args.query_times;
        } else {
            errorPrint("%s() LN%d, failed to read json, query_times input mistake\n",
                    __func__, __LINE__);
            goto PARSE_OVER;
        }

        cJSON* concurrent = cJSON_GetObjectItem(specifiedQuery, "concurrent");
        if (concurrent && concurrent->type == cJSON_Number) {
            if (concurrent->valueint <= 0) {
                errorPrint(
                        "query sqlCount %d or concurrent %d is not correct.\n",
                        g_queryInfo.specifiedQueryInfo.sqlCount,
                        g_queryInfo.specifiedQueryInfo.concurrent);
                goto PARSE_OVER;
            }
            g_queryInfo.specifiedQueryInfo.concurrent = concurrent->valueint;
        } else if (!concurrent) {
            g_queryInfo.specifiedQueryInfo.concurrent = 1;
        }

        cJSON* specifiedAsyncMode = cJSON_GetObjectItem(specifiedQuery, "mode");
        if (specifiedAsyncMode && specifiedAsyncMode->type == cJSON_String
                && specifiedAsyncMode->valuestring != NULL) {
            if (0 == strcmp("sync", specifiedAsyncMode->valuestring)) {
                g_queryInfo.specifiedQueryInfo.asyncMode = SYNC_MODE;
            } else if (0 == strcmp("async", specifiedAsyncMode->valuestring)) {
                g_queryInfo.specifiedQueryInfo.asyncMode = ASYNC_MODE;
            } else {
                errorPrint("%s", "failed to read json, async mode input error\n");
                goto PARSE_OVER;
            }
        } else {
            g_queryInfo.specifiedQueryInfo.asyncMode = SYNC_MODE;
        }

        cJSON* interval = cJSON_GetObjectItem(specifiedQuery, "interval");
        if (interval && interval->type == cJSON_Number) {
            g_queryInfo.specifiedQueryInfo.subscribeInterval = interval->valueint;
        } else if (!interval) {
            //printf("failed to read json, subscribe interval no found\n");
            //goto PARSE_OVER;
            g_queryInfo.specifiedQueryInfo.subscribeInterval = 10000;
        }

        cJSON* restart = cJSON_GetObjectItem(specifiedQuery, "restart");
        if (restart && restart->type == cJSON_String && restart->valuestring != NULL) {
            if (0 == strcmp("yes", restart->valuestring)) {
                g_queryInfo.specifiedQueryInfo.subscribeRestart = true;
            } else if (0 == strcmp("no", restart->valuestring)) {
                g_queryInfo.specifiedQueryInfo.subscribeRestart = false;
            } else {
                errorPrint("%s", "failed to read json, subscribe restart error\n");
                goto PARSE_OVER;
            }
        } else {
            g_queryInfo.specifiedQueryInfo.subscribeRestart = true;
        }

        cJSON* keepProgress = cJSON_GetObjectItem(specifiedQuery, "keepProgress");
        if (keepProgress
                && keepProgress->type == cJSON_String
                && keepProgress->valuestring != NULL) {
            if (0 == strcmp("yes", keepProgress->valuestring)) {
                g_queryInfo.specifiedQueryInfo.subscribeKeepProgress = 1;
            } else if (0 == strcmp("no", keepProgress->valuestring)) {
                g_queryInfo.specifiedQueryInfo.subscribeKeepProgress = 0;
            } else {
                errorPrint("%s", "failed to read json, subscribe keepProgress error\n");
                goto PARSE_OVER;
            }
        } else {
            g_queryInfo.specifiedQueryInfo.subscribeKeepProgress = 0;
        }

        // sqls
        cJSON* specifiedSqls = cJSON_GetObjectItem(specifiedQuery, "sqls");
        if (!specifiedSqls) {
            g_queryInfo.specifiedQueryInfo.sqlCount = 0;
        } else if (specifiedSqls->type != cJSON_Array) {
            errorPrint("%s", "failed to read json, super sqls not found\n");
            goto PARSE_OVER;
        } else {
            int superSqlSize = cJSON_GetArraySize(specifiedSqls);
            if (superSqlSize * g_queryInfo.specifiedQueryInfo.concurrent
                    > MAX_QUERY_SQL_COUNT) {
                errorPrint("failed to read json, query sql(%d) * concurrent(%d) overflow, max is %d\n",
                        superSqlSize,
                        g_queryInfo.specifiedQueryInfo.concurrent,
                        MAX_QUERY_SQL_COUNT);
                goto PARSE_OVER;
            }

            g_queryInfo.specifiedQueryInfo.sqlCount = superSqlSize;
            for (int j = 0; j < superSqlSize; ++j) {
                cJSON* sql = cJSON_GetArrayItem(specifiedSqls, j);
                if (sql == NULL) continue;

                cJSON *sqlStr = cJSON_GetObjectItem(sql, "sql");
                if (!sqlStr || sqlStr->type != cJSON_String || sqlStr->valuestring == NULL) {
                    errorPrint("%s", "failed to read json, sql not found\n");
                    goto PARSE_OVER;
                }
                tstrncpy(g_queryInfo.specifiedQueryInfo.sql[j],
                        sqlStr->valuestring, BUFFER_SIZE);

                // default value is -1, which mean infinite loop
                g_queryInfo.specifiedQueryInfo.endAfterConsume[j] = -1;
                cJSON* endAfterConsume =
                    cJSON_GetObjectItem(specifiedQuery, "endAfterConsume");
                if (endAfterConsume
                        && endAfterConsume->type == cJSON_Number) {
                    g_queryInfo.specifiedQueryInfo.endAfterConsume[j]
                        = endAfterConsume->valueint;
                }
                if (g_queryInfo.specifiedQueryInfo.endAfterConsume[j] < -1)
                    g_queryInfo.specifiedQueryInfo.endAfterConsume[j] = -1;

                g_queryInfo.specifiedQueryInfo.resubAfterConsume[j] = -1;
                cJSON* resubAfterConsume =
                    cJSON_GetObjectItem(specifiedQuery, "resubAfterConsume");
                if ((resubAfterConsume)
                        && (resubAfterConsume->type == cJSON_Number)
                        && (resubAfterConsume->valueint >= 0)) {
                    g_queryInfo.specifiedQueryInfo.resubAfterConsume[j]
                        = resubAfterConsume->valueint;
                }

                if (g_queryInfo.specifiedQueryInfo.resubAfterConsume[j] < -1)
                    g_queryInfo.specifiedQueryInfo.resubAfterConsume[j] = -1;

                cJSON *result = cJSON_GetObjectItem(sql, "result");
                if ((NULL != result) && (result->type == cJSON_String)
                        && (result->valuestring != NULL)) {
                    tstrncpy(g_queryInfo.specifiedQueryInfo.result[j],
                            result->valuestring, MAX_FILE_NAME_LEN);
                } else if (NULL == result) {
                    memset(g_queryInfo.specifiedQueryInfo.result[j],
                            0, MAX_FILE_NAME_LEN);
                } else {
                    errorPrint("%s",
                            "failed to read json, super query result file not found\n");
                    goto PARSE_OVER;
                }
            }
        }
    }

    // super_table_query
    cJSON *superQuery = cJSON_GetObjectItem(root, "super_table_query");
    if (!superQuery) {
        g_queryInfo.superQueryInfo.threadCnt = 1;
        g_queryInfo.superQueryInfo.sqlCount = 0;
    } else if (superQuery->type != cJSON_Object) {
        errorPrint("%s", "failed to read json, sub_table_query not found\n");
        ret = true;
        goto PARSE_OVER;
    } else {
        cJSON* subrate = cJSON_GetObjectItem(superQuery, "query_interval");
        if (subrate && subrate->type == cJSON_Number) {
            g_queryInfo.superQueryInfo.queryInterval = subrate->valueint;
        } else if (!subrate) {
            g_queryInfo.superQueryInfo.queryInterval = 0;
        }

        cJSON* superQueryTimes = cJSON_GetObjectItem(superQuery, "query_times");
        if (superQueryTimes && superQueryTimes->type == cJSON_Number) {
            if (superQueryTimes->valueint <= 0) {
                errorPrint("failed to read json, query_times: %"PRId64", need be a valid (>0) number\n",
                        superQueryTimes->valueint);
                goto PARSE_OVER;
            }
            g_queryInfo.superQueryInfo.queryTimes = superQueryTimes->valueint;
        } else if (!superQueryTimes) {
            g_queryInfo.superQueryInfo.queryTimes = g_args.query_times;
        } else {
            errorPrint("%s", "failed to read json, query_times input mistake\n");
            goto PARSE_OVER;
        }

        cJSON* threads = cJSON_GetObjectItem(superQuery, "threads");
        if (threads && threads->type == cJSON_Number) {
            if (threads->valueint <= 0) {
                errorPrint("%s", "failed to read json, threads input mistake\n");
                goto PARSE_OVER;

            }
            g_queryInfo.superQueryInfo.threadCnt = threads->valueint;
        } else if (!threads) {
            g_queryInfo.superQueryInfo.threadCnt = 1;
        }

        //cJSON* subTblCnt = cJSON_GetObjectItem(superQuery, "childtable_count");
        //if (subTblCnt && subTblCnt->type == cJSON_Number) {
        //  g_queryInfo.superQueryInfo.childTblCount = subTblCnt->valueint;
        //} else if (!subTblCnt) {
        //  g_queryInfo.superQueryInfo.childTblCount = 0;
        //}

        cJSON* stblname = cJSON_GetObjectItem(superQuery, "stblname");
        if (stblname && stblname->type == cJSON_String
                && stblname->valuestring != NULL) {
            tstrncpy(g_queryInfo.superQueryInfo.stbName, stblname->valuestring,
                    TSDB_TABLE_NAME_LEN);
        } else {
            errorPrint("%s", "failed to read json, super table name input error\n");
            goto PARSE_OVER;
        }

        cJSON* superAsyncMode = cJSON_GetObjectItem(superQuery, "mode");
        if (superAsyncMode && superAsyncMode->type == cJSON_String
                && superAsyncMode->valuestring != NULL) {
            if (0 == strcmp("sync", superAsyncMode->valuestring)) {
                g_queryInfo.superQueryInfo.asyncMode = SYNC_MODE;
            } else if (0 == strcmp("async", superAsyncMode->valuestring)) {
                g_queryInfo.superQueryInfo.asyncMode = ASYNC_MODE;
            } else {
                errorPrint("%s", "failed to read json, async mode input error\n");
                goto PARSE_OVER;
            }
        } else {
            g_queryInfo.superQueryInfo.asyncMode = SYNC_MODE;
        }

        cJSON* superInterval = cJSON_GetObjectItem(superQuery, "interval");
        if (superInterval && superInterval->type == cJSON_Number) {
            if (superInterval->valueint < 0) {
                errorPrint("%s", "failed to read json, interval input mistake\n");
                goto PARSE_OVER;
            }
            g_queryInfo.superQueryInfo.subscribeInterval = superInterval->valueint;
        } else if (!superInterval) {
            //printf("failed to read json, subscribe interval no found\n");
            //goto PARSE_OVER;
            g_queryInfo.superQueryInfo.subscribeInterval = 10000;
        }

        cJSON* subrestart = cJSON_GetObjectItem(superQuery, "restart");
        if (subrestart && subrestart->type == cJSON_String
                && subrestart->valuestring != NULL) {
            if (0 == strcmp("yes", subrestart->valuestring)) {
                g_queryInfo.superQueryInfo.subscribeRestart = true;
            } else if (0 == strcmp("no", subrestart->valuestring)) {
                g_queryInfo.superQueryInfo.subscribeRestart = false;
            } else {
                errorPrint("%s", "failed to read json, subscribe restart error\n");
                goto PARSE_OVER;
            }
        } else {
            g_queryInfo.superQueryInfo.subscribeRestart = true;
        }

        cJSON* superkeepProgress = cJSON_GetObjectItem(superQuery, "keepProgress");
        if (superkeepProgress &&
                superkeepProgress->type == cJSON_String
                && superkeepProgress->valuestring != NULL) {
            if (0 == strcmp("yes", superkeepProgress->valuestring)) {
                g_queryInfo.superQueryInfo.subscribeKeepProgress = 1;
            } else if (0 == strcmp("no", superkeepProgress->valuestring)) {
                g_queryInfo.superQueryInfo.subscribeKeepProgress = 0;
            } else {
                errorPrint("%s",
                        "failed to read json, subscribe super table keepProgress error\n");
                goto PARSE_OVER;
            }
        } else {
            g_queryInfo.superQueryInfo.subscribeKeepProgress = 0;
        }

        // default value is -1, which mean do not resub
        g_queryInfo.superQueryInfo.endAfterConsume = -1;
        cJSON* superEndAfterConsume =
            cJSON_GetObjectItem(superQuery, "endAfterConsume");
        if (superEndAfterConsume
                && superEndAfterConsume->type == cJSON_Number) {
            g_queryInfo.superQueryInfo.endAfterConsume =
                superEndAfterConsume->valueint;
        }
        if (g_queryInfo.superQueryInfo.endAfterConsume < -1)
            g_queryInfo.superQueryInfo.endAfterConsume = -1;

        // default value is -1, which mean do not resub
        g_queryInfo.superQueryInfo.resubAfterConsume = -1;
        cJSON* superResubAfterConsume =
            cJSON_GetObjectItem(superQuery, "resubAfterConsume");
        if ((superResubAfterConsume)
                && (superResubAfterConsume->type == cJSON_Number)
                && (superResubAfterConsume->valueint >= 0)) {
            g_queryInfo.superQueryInfo.resubAfterConsume =
                superResubAfterConsume->valueint;
        }
        if (g_queryInfo.superQueryInfo.resubAfterConsume < -1)
            g_queryInfo.superQueryInfo.resubAfterConsume = -1;

        // supert table sqls
        cJSON* superSqls = cJSON_GetObjectItem(superQuery, "sqls");
        if (!superSqls) {
            g_queryInfo.superQueryInfo.sqlCount = 0;
        } else if (superSqls->type != cJSON_Array) {
            errorPrint("%s", "failed to read json, super sqls not found\n");
            goto PARSE_OVER;
        } else {
            int superSqlSize = cJSON_GetArraySize(superSqls);
            if (superSqlSize > MAX_QUERY_SQL_COUNT) {
                errorPrint("failed to read json, query sql size overflow, max is %d\n",
                        MAX_QUERY_SQL_COUNT);
                goto PARSE_OVER;
            }

            g_queryInfo.superQueryInfo.sqlCount = superSqlSize;
            for (int j = 0; j < superSqlSize; ++j) {
                cJSON* sql = cJSON_GetArrayItem(superSqls, j);
                if (sql == NULL) continue;

                cJSON *sqlStr = cJSON_GetObjectItem(sql, "sql");
                if (!sqlStr || sqlStr->type != cJSON_String
                        || sqlStr->valuestring == NULL) {
                    errorPrint("%s", "failed to read json, sql not found\n");
                    goto PARSE_OVER;
                }
                tstrncpy(g_queryInfo.superQueryInfo.sql[j], sqlStr->valuestring,
                        BUFFER_SIZE);

                cJSON *result = cJSON_GetObjectItem(sql, "result");
                if (result != NULL && result->type == cJSON_String
                        && result->valuestring != NULL) {
                    tstrncpy(g_queryInfo.superQueryInfo.result[j],
                            result->valuestring, MAX_FILE_NAME_LEN);
                } else if (NULL == result) {
                    memset(g_queryInfo.superQueryInfo.result[j], 0, MAX_FILE_NAME_LEN);
                }  else {
                    errorPrint("%s", "failed to read json, sub query result file not found\n");
                    goto PARSE_OVER;
                }
            }
        }
    }

    ret = true;

PARSE_OVER:
    return ret;
}

static bool getInfoFromJsonFile(char* file) {
    debugPrint("%s %d %s\n", __func__, __LINE__, file);

    FILE *fp = fopen(file, "r");
    if (!fp) {
        errorPrint("failed to read %s, reason:%s\n", file, strerror(errno));
        return false;
    }

    bool  ret = false;
    int   maxLen = 6400000;
    char *content = calloc(1, maxLen + 1);
    int   len = fread(content, 1, maxLen, fp);
    if (len <= 0) {
        free(content);
        fclose(fp);
        errorPrint("failed to read %s, content is null", file);
        return false;
    }

    content[len] = 0;
    cJSON* root = cJSON_Parse(content);
    if (root == NULL) {
        errorPrint("failed to cjson parse %s, invalid json format\n", file);
        goto PARSE_OVER;
    }

    cJSON* filetype = cJSON_GetObjectItem(root, "filetype");
    if (filetype && filetype->type == cJSON_String && filetype->valuestring != NULL) {
        if (0 == strcasecmp("insert", filetype->valuestring)) {
            g_args.test_mode = INSERT_TEST;
        } else if (0 == strcasecmp("query", filetype->valuestring)) {
            g_args.test_mode = QUERY_TEST;
        } else if (0 == strcasecmp("subscribe", filetype->valuestring)) {
            g_args.test_mode = SUBSCRIBE_TEST;
        } else {
            errorPrint("%s", "failed to read json, filetype not support\n");
            goto PARSE_OVER;
        }
    } else if (!filetype) {
        g_args.test_mode = INSERT_TEST;
    } else {
        errorPrint("%s", "failed to read json, filetype not found\n");
        goto PARSE_OVER;
    }

    if (INSERT_TEST == g_args.test_mode) {
        memset(&g_Dbs, 0, sizeof(SDbs));
        g_Dbs.use_metric = g_args.use_metric;
        ret = getMetaFromInsertJsonFile(root);
    } else if ((QUERY_TEST == g_args.test_mode)
            || (SUBSCRIBE_TEST == g_args.test_mode)) {
        memset(&g_queryInfo, 0, sizeof(SQueryMetaInfo));
        ret = getMetaFromQueryJsonFile(root);
    } else {
        errorPrint("%s",
                "input json file type error! please input correct file type: insert or query or subscribe\n");
        goto PARSE_OVER;
    }

PARSE_OVER:
    free(content);
    cJSON_Delete(root);
    fclose(fp);
    return ret;
}

static int prepareSampleData() {
    for (int i = 0; i < g_Dbs.dbCount; i++) {
        for (int j = 0; j < g_Dbs.db[i].superTblCount; j++) {
            if (g_Dbs.db[i].superTbls[j].tagsFile[0] != 0) {
                if (readTagFromCsvFileToMem(&g_Dbs.db[i].superTbls[j]) != 0) {
                    return -1;
                }
            }
        }
    }

    return 0;
}

static void postFreeResource() {
    tmfclose(g_fpOfInsertResult);

    for (int i = 0; i < g_Dbs.dbCount; i++) {
        for (uint64_t j = 0; j < g_Dbs.db[i].superTblCount; j++) {
            if (0 != g_Dbs.db[i].superTbls[j].colsOfCreateChildTable) {
                tmfree(g_Dbs.db[i].superTbls[j].colsOfCreateChildTable);
                g_Dbs.db[i].superTbls[j].colsOfCreateChildTable = NULL;
            }
            if (0 != g_Dbs.db[i].superTbls[j].sampleDataBuf) {
                tmfree(g_Dbs.db[i].superTbls[j].sampleDataBuf);
                g_Dbs.db[i].superTbls[j].sampleDataBuf = NULL;
            }

#if STMT_BIND_PARAM_BATCH == 1
            for (int c = 0;
                    c < g_Dbs.db[i].superTbls[j].columnCount; c ++) {

                if (g_Dbs.db[i].superTbls[j].sampleBindBatchArray) {

                    tmfree((char *)((uintptr_t)*(uintptr_t*)(
                                    g_Dbs.db[i].superTbls[j].sampleBindBatchArray
                                    + sizeof(char*) * c)));
                }
            }
            tmfree(g_Dbs.db[i].superTbls[j].sampleBindBatchArray);
#endif
            if (0 != g_Dbs.db[i].superTbls[j].tagDataBuf) {
                tmfree(g_Dbs.db[i].superTbls[j].tagDataBuf);
                g_Dbs.db[i].superTbls[j].tagDataBuf = NULL;
            }
            if (0 != g_Dbs.db[i].superTbls[j].childTblName) {
                tmfree(g_Dbs.db[i].superTbls[j].childTblName);
                g_Dbs.db[i].superTbls[j].childTblName = NULL;
            }
        }
        tmfree(g_Dbs.db[i].superTbls);
    }
    tmfree(g_Dbs.db);
    tmfree(g_randbool_buff);
    tmfree(g_randint_buff);
    tmfree(g_rand_voltage_buff);
    tmfree(g_randbigint_buff);
    tmfree(g_randsmallint_buff);
    tmfree(g_randtinyint_buff);
    tmfree(g_randfloat_buff);
    tmfree(g_rand_current_buff);
    tmfree(g_rand_phase_buff);
    tmfree(g_randdouble_buff);
    tmfree(g_randuint_buff);
    tmfree(g_randutinyint_buff);
    tmfree(g_randusmallint_buff);
    tmfree(g_randubigint_buff);
    tmfree(g_randint);
    tmfree(g_randuint);
    tmfree(g_randbigint);
    tmfree(g_randubigint);
    tmfree(g_randfloat);
    tmfree(g_randdouble);

    tmfree(g_sampleDataBuf);

#if STMT_BIND_PARAM_BATCH == 1
    for (int l = 0;
             l < g_args.columnCount; l ++) {
        if (g_sampleBindBatchArray) {
            tmfree((char *)((uintptr_t)*(uintptr_t*)(
                            g_sampleBindBatchArray
                            + sizeof(char*) * l)));
        }
    }
    tmfree(g_sampleBindBatchArray);

#endif
}

static int getRowDataFromSample(
        char* dataBuf, int64_t maxLen, int64_t timestamp,
        SSuperTable* stbInfo, int64_t* sampleUsePos)
{
    if ((*sampleUsePos) == MAX_SAMPLES) {
        *sampleUsePos = 0;
    }

    int    dataLen = 0;
    if(stbInfo->useSampleTs) {
        dataLen += snprintf(dataBuf + dataLen, maxLen - dataLen,
            "(%s",
            stbInfo->sampleDataBuf
            + stbInfo->lenOfOneRow * (*sampleUsePos));
    } else {
        dataLen += snprintf(dataBuf + dataLen, maxLen - dataLen,
            "(%" PRId64 ", ", timestamp);
        dataLen += snprintf(dataBuf + dataLen, maxLen - dataLen,
            "%s",
            stbInfo->sampleDataBuf
            + stbInfo->lenOfOneRow * (*sampleUsePos));
    }
    
    dataLen += snprintf(dataBuf + dataLen, maxLen - dataLen, ")");

    (*sampleUsePos)++;

    return dataLen;
}

static int64_t generateStbRowData(
        SSuperTable* stbInfo,
        char* recBuf,
        int64_t remainderBufLen,
        int64_t timestamp)
{
    int64_t   dataLen = 0;
    char  *pstr = recBuf;
    int64_t maxLen = MAX_DATA_SIZE;
    int tmpLen;

    dataLen += snprintf(pstr + dataLen, maxLen - dataLen,
            "(%" PRId64 "", timestamp);

    for (int i = 0; i < stbInfo->columnCount; i++) {
        tstrncpy(pstr + dataLen, ",", 2);
        dataLen += 1;

        if ((stbInfo->columns[i].data_type == TSDB_DATA_TYPE_BINARY)
                || (stbInfo->columns[i].data_type == TSDB_DATA_TYPE_NCHAR)) {
            if (stbInfo->columns[i].dataLen > TSDB_MAX_BINARY_LEN) {
                errorPrint2("binary or nchar length overflow, max size:%u\n",
                        (uint32_t)TSDB_MAX_BINARY_LEN);
                return -1;
            }

            if ((stbInfo->columns[i].dataLen + 1) >
                    /* need count 3 extra chars \', \', and , */
                    (remainderBufLen - dataLen - 3)) {
                return 0;
            }
            char* buf = (char*)calloc(stbInfo->columns[i].dataLen+1, 1);
            if (NULL == buf) {
                errorPrint2("calloc failed! size:%d\n", stbInfo->columns[i].dataLen);
                return -1;
            }
            rand_string(buf, stbInfo->columns[i].dataLen);
            dataLen += snprintf(pstr + dataLen, maxLen - dataLen, "\'%s\'", buf);
            tmfree(buf);

        } else {
            char *tmp = NULL;
            switch(stbInfo->columns[i].data_type) {
                case TSDB_DATA_TYPE_INT:
                    if ((g_args.demo_mode) && (i == 1)) {
                        tmp = demo_voltage_int_str();
                    } else {
                        tmp = rand_int_str();
                    }
                    tmpLen = strlen(tmp);
                    tstrncpy(pstr + dataLen, tmp, min(tmpLen + 1, INT_BUFF_LEN));
                    break;

                case TSDB_DATA_TYPE_UINT:
                    tmp = rand_uint_str();
                    tmpLen = strlen(tmp);
                    tstrncpy(pstr + dataLen, tmp, min(tmpLen + 1, INT_BUFF_LEN));
                    break;

                case TSDB_DATA_TYPE_BIGINT:
                    tmp = rand_bigint_str();
                    tmpLen = strlen(tmp);
                    tstrncpy(pstr + dataLen, tmp, min(tmpLen + 1, BIGINT_BUFF_LEN));
                    break;

                case TSDB_DATA_TYPE_UBIGINT:
                    tmp = rand_ubigint_str();
                    tmpLen = strlen(tmp);
                    tstrncpy(pstr + dataLen, tmp, min(tmpLen + 1, BIGINT_BUFF_LEN));
                    break;

                case TSDB_DATA_TYPE_FLOAT:
                    if (g_args.demo_mode) {
                        if (i == 0) {
                            tmp = demo_current_float_str();
                        } else {
                            tmp = demo_phase_float_str();
                        }
                    } else {
                        tmp = rand_float_str();
                    }
                    tmpLen = strlen(tmp);
                    tstrncpy(pstr + dataLen, tmp, min(tmpLen + 1, FLOAT_BUFF_LEN));
                    break;

                case TSDB_DATA_TYPE_DOUBLE:
                    tmp = rand_double_str();
                    tmpLen = strlen(tmp);
                    tstrncpy(pstr + dataLen, tmp, min(tmpLen + 1, DOUBLE_BUFF_LEN));
                    break;

                case TSDB_DATA_TYPE_SMALLINT:
                    tmp = rand_smallint_str();
                    tmpLen = strlen(tmp);
                    tstrncpy(pstr + dataLen, tmp, min(tmpLen + 1, SMALLINT_BUFF_LEN));
                    break;

                case TSDB_DATA_TYPE_USMALLINT:
                    tmp = rand_usmallint_str();
                    tmpLen = strlen(tmp);
                    tstrncpy(pstr + dataLen, tmp, min(tmpLen + 1, SMALLINT_BUFF_LEN));
                    break;

                case TSDB_DATA_TYPE_TINYINT:
                    tmp = rand_tinyint_str();
                    tmpLen = strlen(tmp);
                    tstrncpy(pstr + dataLen, tmp, min(tmpLen + 1, TINYINT_BUFF_LEN));
                    break;

                case TSDB_DATA_TYPE_UTINYINT:
                    tmp = rand_utinyint_str();
                    tmpLen = strlen(tmp);
                    tstrncpy(pstr + dataLen, tmp, min(tmpLen + 1, TINYINT_BUFF_LEN));
                    break;

                case TSDB_DATA_TYPE_BOOL:
                    tmp = rand_bool_str();
                    tmpLen = strlen(tmp);
                    tstrncpy(pstr + dataLen, tmp, min(tmpLen + 1, BOOL_BUFF_LEN));
                    break;

                case TSDB_DATA_TYPE_TIMESTAMP:
                    tmp = rand_bigint_str();
                    tmpLen = strlen(tmp);
                    tstrncpy(pstr + dataLen, tmp, min(tmpLen + 1, BIGINT_BUFF_LEN));
                    break;

                case TSDB_DATA_TYPE_NULL:
                    break;

                default:
                    errorPrint2("Not support data type: %s\n",
                            stbInfo->columns[i].dataType);
                    exit(EXIT_FAILURE);
            }
            if (tmp) {
                dataLen += tmpLen;
            }
        }

        if (dataLen > (remainderBufLen - (128)))
            return 0;
    }

    dataLen += snprintf(pstr + dataLen, 2, ")");

    verbosePrint("%s() LN%d, dataLen:%"PRId64"\n", __func__, __LINE__, dataLen);
    verbosePrint("%s() LN%d, recBuf:\n\t%s\n", __func__, __LINE__, recBuf);

    return strlen(recBuf);
}

static int64_t generateData(char *recBuf, char *data_type,
        int64_t timestamp, int lenOfBinary) {
    memset(recBuf, 0, MAX_DATA_SIZE);
    char *pstr = recBuf;
    pstr += sprintf(pstr, "(%"PRId64"", timestamp);

    int columnCount = g_args.columnCount;

    bool b;
    char *s;
    for (int i = 0; i < columnCount; i++) {
        switch (data_type[i]) {
            case TSDB_DATA_TYPE_TINYINT:
                pstr += sprintf(pstr, ",%d", rand_tinyint() );
                break;

            case TSDB_DATA_TYPE_SMALLINT:
                pstr += sprintf(pstr, ",%d", rand_smallint());
                break;

            case TSDB_DATA_TYPE_INT:
                pstr += sprintf(pstr, ",%d", rand_int());
                break;

            case TSDB_DATA_TYPE_BIGINT:
                pstr += sprintf(pstr, ",%"PRId64"", rand_bigint());
                break;

            case TSDB_DATA_TYPE_TIMESTAMP:
                pstr += sprintf(pstr, ",%"PRId64"", rand_bigint());
                break;

            case TSDB_DATA_TYPE_FLOAT:
                pstr += sprintf(pstr, ",%10.4f", rand_float());
                break;

            case TSDB_DATA_TYPE_DOUBLE:
                pstr += sprintf(pstr, ",%20.8f", rand_double());
                break;

            case TSDB_DATA_TYPE_BOOL:
                b = rand_bool() & 1;
                pstr += sprintf(pstr, ",%s", b ? "true" : "false");
                break;

            case TSDB_DATA_TYPE_BINARY:
                s = malloc(lenOfBinary + 1);
                if (s == NULL) {
                    errorPrint2("%s() LN%d, memory allocation %d bytes failed\n",
                            __func__, __LINE__, lenOfBinary + 1);
                    exit(EXIT_FAILURE);
                }
                rand_string(s, lenOfBinary);
                pstr += sprintf(pstr, ",\"%s\"", s);
                free(s);
                break;

            case TSDB_DATA_TYPE_NCHAR:
                s = malloc(lenOfBinary + 1);
                if (s == NULL) {
                    errorPrint2("%s() LN%d, memory allocation %d bytes failed\n",
                            __func__, __LINE__, lenOfBinary + 1);
                    exit(EXIT_FAILURE);
                }
                rand_string(s, lenOfBinary);
                pstr += sprintf(pstr, ",\"%s\"", s);
                free(s);
                break;

            case TSDB_DATA_TYPE_UTINYINT:
                pstr += sprintf(pstr, ",%d", rand_utinyint() );
                break;

            case TSDB_DATA_TYPE_USMALLINT:
                pstr += sprintf(pstr, ",%d", rand_usmallint());
                break;

            case TSDB_DATA_TYPE_UINT:
                pstr += sprintf(pstr, ",%d", rand_uint());
                break;

            case TSDB_DATA_TYPE_UBIGINT:
                pstr += sprintf(pstr, ",%"PRId64"", rand_ubigint());
                break;

            case TSDB_DATA_TYPE_NULL:
                break;

            default:
                errorPrint2("%s() LN%d, Unknown data type %d\n",
                        __func__, __LINE__,
                        data_type[i]);
                exit(EXIT_FAILURE);
        }

        if (strlen(recBuf) > MAX_DATA_SIZE) {
            ERROR_EXIT("column length too long, abort");
        }
    }

    pstr += sprintf(pstr, ")");

    verbosePrint("%s() LN%d, recBuf:\n\t%s\n", __func__, __LINE__, recBuf);

    return (int32_t)strlen(recBuf);
}

static int generateSampleFromRand(
        char *sampleDataBuf,
        uint64_t lenOfOneRow,
        int columnCount,
        StrColumn *columns
        )
{
    char data[MAX_DATA_SIZE];
    memset(data, 0, MAX_DATA_SIZE);

    char *buff = malloc(lenOfOneRow);
    if (NULL == buff) {
        errorPrint2("%s() LN%d, memory allocation %"PRIu64" bytes failed\n",
                __func__, __LINE__, lenOfOneRow);
        exit(EXIT_FAILURE);
    }

    for (int i=0; i < MAX_SAMPLES; i++) {
        uint64_t pos = 0;
        memset(buff, 0, lenOfOneRow);

        for (int c = 0; c < columnCount; c++) {
            char *tmp = NULL;

            uint32_t dataLen;
            char data_type = (columns)?(columns[c].data_type):g_args.data_type[c];

            switch(data_type) {
                case TSDB_DATA_TYPE_BINARY:
                    dataLen = (columns)?columns[c].dataLen:g_args.binwidth;
                    rand_string(data, dataLen);
                    pos += sprintf(buff + pos, "%s,", data);
                    break;

                case TSDB_DATA_TYPE_NCHAR:
                    dataLen = (columns)?columns[c].dataLen:g_args.binwidth;
                    rand_string(data, dataLen - 1);
                    pos += sprintf(buff + pos, "%s,", data);
                    break;

                case TSDB_DATA_TYPE_INT:
                    if ((g_args.demo_mode) && (c == 1)) {
                        tmp = demo_voltage_int_str();
                    } else {
                        tmp = rand_int_str();
                    }
                    pos += sprintf(buff + pos, "%s,", tmp);
                    break;

                case TSDB_DATA_TYPE_UINT:
                    pos += sprintf(buff + pos, "%s,", rand_uint_str());
                    break;

                case TSDB_DATA_TYPE_BIGINT:
                    pos += sprintf(buff + pos, "%s,", rand_bigint_str());
                    break;

                case TSDB_DATA_TYPE_UBIGINT:
                    pos += sprintf(buff + pos, "%s,", rand_ubigint_str());
                    break;

                case TSDB_DATA_TYPE_FLOAT:
                    if (g_args.demo_mode) {
                        if (c == 0) {
                            tmp = demo_current_float_str();
                        } else {
                            tmp = demo_phase_float_str();
                        }
                    } else {
                        tmp = rand_float_str();
                    }
                    pos += sprintf(buff + pos, "%s,", tmp);
                    break;

                case TSDB_DATA_TYPE_DOUBLE:
                    pos += sprintf(buff + pos, "%s,", rand_double_str());
                    break;

                case TSDB_DATA_TYPE_SMALLINT:
                    pos += sprintf(buff + pos, "%s,", rand_smallint_str());
                    break;

                case TSDB_DATA_TYPE_USMALLINT:
                    pos += sprintf(buff + pos, "%s,", rand_usmallint_str());
                    break;

                case TSDB_DATA_TYPE_TINYINT:
                    pos += sprintf(buff + pos, "%s,", rand_tinyint_str());
                    break;

                case TSDB_DATA_TYPE_UTINYINT:
                    pos += sprintf(buff + pos, "%s,", rand_utinyint_str());
                    break;

                case TSDB_DATA_TYPE_BOOL:
                    pos += sprintf(buff + pos, "%s,", rand_bool_str());
                    break;

                case TSDB_DATA_TYPE_TIMESTAMP:
                    pos += sprintf(buff + pos, "%s,", rand_bigint_str());
                    break;

                case TSDB_DATA_TYPE_NULL:
                    break;

                default:
                    errorPrint2("%s() LN%d, Unknown data type %s\n",
                            __func__, __LINE__,
                            (columns)?(columns[c].dataType):g_args.dataType[c]);
                    exit(EXIT_FAILURE);
            }
        }

        *(buff + pos - 1) = 0;
        memcpy(sampleDataBuf + i * lenOfOneRow, buff, pos);
    }

    free(buff);
    return 0;
}

static int generateSampleFromRandForNtb()
{
    return generateSampleFromRand(
            g_sampleDataBuf,
            g_args.lenOfOneRow,
            g_args.columnCount,
            NULL);
}

static int generateSampleFromRandForStb(SSuperTable *stbInfo)
{
    return generateSampleFromRand(
            stbInfo->sampleDataBuf,
            stbInfo->lenOfOneRow,
            stbInfo->columnCount,
            stbInfo->columns);
}

static int prepareSampleForNtb() {
    g_sampleDataBuf = calloc(g_args.lenOfOneRow * MAX_SAMPLES, 1);
    if (NULL == g_sampleDataBuf) {
        errorPrint2("%s() LN%d, Failed to calloc %"PRIu64" Bytes, reason:%s\n",
                __func__, __LINE__,
                g_args.lenOfOneRow * MAX_SAMPLES,
                strerror(errno));
        return -1;
    }

    return generateSampleFromRandForNtb();
}

static int prepareSampleForStb(SSuperTable *stbInfo) {

    stbInfo->sampleDataBuf = calloc(
            stbInfo->lenOfOneRow * MAX_SAMPLES, 1);
    if (NULL == stbInfo->sampleDataBuf) {
        errorPrint2("%s() LN%d, Failed to calloc %"PRIu64" Bytes, reason:%s\n",
                __func__, __LINE__,
                stbInfo->lenOfOneRow * MAX_SAMPLES,
                strerror(errno));
        return -1;
    }

    int ret;
    if (0 == strncasecmp(stbInfo->dataSource, "sample", strlen("sample"))) {
        if(stbInfo->useSampleTs) {
            getAndSetRowsFromCsvFile(stbInfo);
        }
        ret = generateSampleFromCsvForStb(stbInfo);
    } else {
        ret = generateSampleFromRandForStb(stbInfo);
    }

    if (0 != ret) {
        errorPrint2("%s() LN%d, read sample from csv file failed.\n",
                __func__, __LINE__);
        tmfree(stbInfo->sampleDataBuf);
        stbInfo->sampleDataBuf = NULL;
        return -1;
    }

    return 0;
}

static int32_t execInsert(threadInfo *pThreadInfo, uint32_t k)
{
    int32_t affectedRows;
    SSuperTable* stbInfo = pThreadInfo->stbInfo;

    uint16_t iface;
    if (stbInfo)
        iface = stbInfo->iface;
    else {
        if (g_args.iface == INTERFACE_BUT)
            iface = TAOSC_IFACE;
        else
            iface = g_args.iface;
    }

    debugPrint("[%d] %s() LN%d %s\n", pThreadInfo->threadID,
            __func__, __LINE__,
            (iface==TAOSC_IFACE)?
            "taosc":(iface==REST_IFACE)?"rest":"stmt");

    switch(iface) {
        case TAOSC_IFACE:
            verbosePrint("[%d] %s() LN%d %s\n", pThreadInfo->threadID,
                    __func__, __LINE__, pThreadInfo->buffer);

            affectedRows = queryDbExec(
                    pThreadInfo->taos,
                    pThreadInfo->buffer, INSERT_TYPE, false);
            break;

        case REST_IFACE:
            verbosePrint("[%d] %s() LN%d %s\n", pThreadInfo->threadID,
                    __func__, __LINE__, pThreadInfo->buffer);

            if (0 != postProceSql(g_Dbs.host, g_Dbs.port,
                        pThreadInfo->buffer, pThreadInfo)) {
                affectedRows = -1;
                printf("========restful return fail, threadID[%d]\n",
                        pThreadInfo->threadID);
            } else {
                affectedRows = k;
            }
            break;

        case STMT_IFACE:
            debugPrint("%s() LN%d, stmt=%p",
                    __func__, __LINE__, pThreadInfo->stmt);
            if (0 != taos_stmt_execute(pThreadInfo->stmt)) {
                errorPrint2("%s() LN%d, failied to execute insert statement. reason: %s\n",
                        __func__, __LINE__, taos_stmt_errstr(pThreadInfo->stmt));

                fprintf(stderr, "\n\033[31m === Please reduce batch number if WAL size exceeds limit. ===\033[0m\n\n");
                exit(EXIT_FAILURE);
            }
            affectedRows = k;
            break;

        default:
            errorPrint2("%s() LN%d: unknown insert mode: %d\n",
                    __func__, __LINE__, stbInfo->iface);
            affectedRows = 0;
    }

    return affectedRows;
}

static void getTableName(char *pTblName,
        threadInfo* pThreadInfo, uint64_t tableSeq)
{
    SSuperTable* stbInfo = pThreadInfo->stbInfo;
    if (stbInfo) {
        if (AUTO_CREATE_SUBTBL != stbInfo->autoCreateTable) {
            if (stbInfo->childTblLimit > 0) {
                snprintf(pTblName, TSDB_TABLE_NAME_LEN, "%s",
                        stbInfo->childTblName +
                        (tableSeq - stbInfo->childTblOffset) * TSDB_TABLE_NAME_LEN);
            } else {
                verbosePrint("[%d] %s() LN%d: from=%"PRIu64" count=%"PRId64" seq=%"PRIu64"\n",
                        pThreadInfo->threadID, __func__, __LINE__,
                        pThreadInfo->start_table_from,
                        pThreadInfo->ntables, tableSeq);
                snprintf(pTblName, TSDB_TABLE_NAME_LEN, "%s",
                        stbInfo->childTblName + tableSeq * TSDB_TABLE_NAME_LEN);
            }
        } else {
            snprintf(pTblName, TSDB_TABLE_NAME_LEN, 
            "%s%"PRIu64"", stbInfo->childTblPrefix, tableSeq);
        }
    } else {
        snprintf(pTblName, TSDB_TABLE_NAME_LEN, "%s%"PRIu64"", g_args.tb_prefix, tableSeq);
    }
}

static int32_t generateDataTailWithoutStb(
        uint32_t batch, char* buffer,
        int64_t remainderBufLen, int64_t insertRows,
        uint64_t recordFrom, int64_t startTime,
        /* int64_t *pSamplePos, */int64_t *dataLen) {

    uint64_t len = 0;
    char *pstr = buffer;

    verbosePrint("%s() LN%d batch=%d\n", __func__, __LINE__, batch);

    int32_t k = 0;
    for (k = 0; k < batch;) {
        char *data = pstr;
        memset(data, 0, MAX_DATA_SIZE);

        int64_t retLen = 0;

        char *data_type = g_args.data_type;
        int lenOfBinary = g_args.binwidth;

        if (g_args.disorderRatio) {
            retLen = generateData(data, data_type,
                    startTime + getTSRandTail(
                        g_args.timestamp_step, k,
                        g_args.disorderRatio,
                        g_args.disorderRange),
                    lenOfBinary);
        } else {
            retLen = generateData(data, data_type,
                    startTime + g_args.timestamp_step * k,
                    lenOfBinary);
        }

        if (len > remainderBufLen)
            break;

        pstr += retLen;
        k++;
        len += retLen;
        remainderBufLen -= retLen;

        verbosePrint("%s() LN%d len=%"PRIu64" k=%d \nbuffer=%s\n",
                __func__, __LINE__, len, k, buffer);

        recordFrom ++;

        if (recordFrom >= insertRows) {
            break;
        }
    }

    *dataLen = len;
    return k;
}

static int64_t getTSRandTail(int64_t timeStampStep, int32_t seq,
        int disorderRatio, int disorderRange)
{
    int64_t randTail = timeStampStep * seq;
    if (disorderRatio > 0) {
        int rand_num = taosRandom() % 100;
        if(rand_num < disorderRatio) {
            randTail = (randTail +
                    (taosRandom() % disorderRange + 1)) * (-1);
            debugPrint("rand data generated, back %"PRId64"\n", randTail);
        }
    }

    return randTail;
}

static int32_t generateStbDataTail(
        SSuperTable* stbInfo,
        uint32_t batch, char* buffer,
        int64_t remainderBufLen, int64_t insertRows,
        uint64_t recordFrom, int64_t startTime,
        int64_t *pSamplePos, int64_t *dataLen) {
    uint64_t len = 0;

    char *pstr = buffer;

    bool tsRand;
    if (0 == strncasecmp(stbInfo->dataSource, "rand", strlen("rand"))) {
        tsRand = true;
    } else {
        tsRand = false;
    }
    verbosePrint("%s() LN%d batch=%u buflen=%"PRId64"\n",
            __func__, __LINE__, batch, remainderBufLen);

    int32_t k;
    for (k = 0; k < batch;) {
        char *data = pstr;

        int64_t lenOfRow = 0;

        if (tsRand) {
            if (stbInfo->disorderRatio > 0) {
                lenOfRow = generateStbRowData(stbInfo, data,
                        remainderBufLen,
                        startTime + getTSRandTail(
                            stbInfo->timeStampStep, k,
                            stbInfo->disorderRatio,
                            stbInfo->disorderRange)
                        );
            } else {
                lenOfRow = generateStbRowData(stbInfo, data,
                        remainderBufLen,
                        startTime + stbInfo->timeStampStep * k
                        );
            }
        } else {
            lenOfRow = getRowDataFromSample(
                    data,
                    (remainderBufLen < MAX_DATA_SIZE)?remainderBufLen:MAX_DATA_SIZE,
                    startTime + stbInfo->timeStampStep * k,
                    stbInfo,
                    pSamplePos);
        }

        if (lenOfRow == 0) {
            data[0] = '\0';
            break;
        }
        if ((lenOfRow + 1) > remainderBufLen) {
            break;
        }

        pstr += lenOfRow;
        k++;
        len += lenOfRow;
        remainderBufLen -= lenOfRow;

        verbosePrint("%s() LN%d len=%"PRIu64" k=%u \nbuffer=%s\n",
                __func__, __LINE__, len, k, buffer);

        recordFrom ++;

        if (recordFrom >= insertRows) {
            break;
        }
    }

    *dataLen = len;
    return k;
}


static int generateSQLHeadWithoutStb(char *tableName,
        char *dbName,
        char *buffer, int remainderBufLen)
{
    int len;

    char headBuf[HEAD_BUFF_LEN];

    len = snprintf(
            headBuf,
            HEAD_BUFF_LEN,
            "%s.%s values",
            dbName,
            tableName);

    if (len > remainderBufLen)
        return -1;

    tstrncpy(buffer, headBuf, len + 1);

    return len;
}

static int generateStbSQLHead(
        SSuperTable* stbInfo,
        char *tableName, int64_t tableSeq,
        char *dbName,
        char *buffer, int remainderBufLen)
{
    int len;

    char headBuf[HEAD_BUFF_LEN];

    if (AUTO_CREATE_SUBTBL == stbInfo->autoCreateTable) {
        char* tagsValBuf = NULL;
        if (0 == stbInfo->tagSource) {
            tagsValBuf = generateTagValuesForStb(stbInfo, tableSeq);
        } else {
            tagsValBuf = getTagValueFromTagSample(
                    stbInfo,
                    tableSeq % stbInfo->tagSampleCount);
        }
        if (NULL == tagsValBuf) {
            errorPrint2("%s() LN%d, tag buf failed to allocate  memory\n",
                    __func__, __LINE__);
            return -1;
        }

        len = snprintf(
                headBuf,
                HEAD_BUFF_LEN,
                "%s.%s using %s.%s TAGS%s values",
                dbName,
                tableName,
                dbName,
                stbInfo->stbName,
                tagsValBuf);
        tmfree(tagsValBuf);
    } else if (TBL_ALREADY_EXISTS == stbInfo->childTblExists) {
        len = snprintf(
                headBuf,
                HEAD_BUFF_LEN,
                "%s.%s values",
                dbName,
                tableName);
    } else {
        len = snprintf(
                headBuf,
                HEAD_BUFF_LEN,
                "%s.%s values",
                dbName,
                tableName);
    }

    if (len > remainderBufLen)
        return -1;

    tstrncpy(buffer, headBuf, len + 1);

    return len;
}

static int32_t generateStbInterlaceData(
        threadInfo *pThreadInfo,
        char *tableName, uint32_t batchPerTbl,
        uint64_t i,
        uint32_t batchPerTblTimes,
        uint64_t tableSeq,
        char *buffer,
        int64_t insertRows,
        int64_t startTime,
        uint64_t *pRemainderBufLen)
{
    assert(buffer);
    char *pstr = buffer;

    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    int headLen = generateStbSQLHead(
            stbInfo,
            tableName, tableSeq, pThreadInfo->db_name,
            pstr, *pRemainderBufLen);

    if (headLen <= 0) {
        return 0;
    }
    // generate data buffer
    verbosePrint("[%d] %s() LN%d i=%"PRIu64" buffer:\n%s\n",
            pThreadInfo->threadID, __func__, __LINE__, i, buffer);

    pstr += headLen;
    *pRemainderBufLen -= headLen;

    int64_t dataLen = 0;

    verbosePrint("[%d] %s() LN%d i=%"PRIu64" batchPerTblTimes=%u batchPerTbl = %u\n",
            pThreadInfo->threadID, __func__, __LINE__,
            i, batchPerTblTimes, batchPerTbl);

    if (0 == strncasecmp(stbInfo->startTimestamp, "now", 3)) {
        startTime = taosGetTimestamp(pThreadInfo->time_precision);
    }

    int32_t k = generateStbDataTail(
            stbInfo,
            batchPerTbl, pstr, *pRemainderBufLen, insertRows, 0,
            startTime,
            &(pThreadInfo->samplePos), &dataLen);

    if (k == batchPerTbl) {
        pstr += dataLen;
        *pRemainderBufLen -= dataLen;
    } else {
        debugPrint("%s() LN%d, generated data tail: %u, not equal batch per table: %u\n",
                __func__, __LINE__, k, batchPerTbl);
        pstr -= headLen;
        pstr[0] = '\0';
        k = 0;
    }

    return k;
}

static int64_t generateInterlaceDataWithoutStb(
        char *tableName, uint32_t batch,
        uint64_t tableSeq,
        char *dbName, char *buffer,
        int64_t insertRows,
        int64_t startTime,
        uint64_t *pRemainderBufLen)
{
    assert(buffer);
    char *pstr = buffer;

    int headLen = generateSQLHeadWithoutStb(
            tableName, dbName,
            pstr, *pRemainderBufLen);

    if (headLen <= 0) {
        return 0;
    }

    pstr += headLen;
    *pRemainderBufLen -= headLen;

    int64_t dataLen = 0;

    int32_t k = generateDataTailWithoutStb(
            batch, pstr, *pRemainderBufLen, insertRows, 0,
            startTime,
            &dataLen);

    if (k == batch) {
        pstr += dataLen;
        *pRemainderBufLen -= dataLen;
    } else {
        debugPrint("%s() LN%d, generated data tail: %d, not equal batch per table: %u\n",
                __func__, __LINE__, k, batch);
        pstr -= headLen;
        pstr[0] = '\0';
        k = 0;
    }

    return k;
}

static int32_t prepareStmtBindArrayByType(
        TAOS_BIND *kit_bind,
        char data_type, int32_t dataLen,
        int32_t timePrec,
        char *value)
{
    int32_t *bind_int;
    uint32_t *bind_uint;
    int64_t *bind_bigint;
    uint64_t *bind_ubigint;
    float   *bind_float;
    double  *bind_double;
    int8_t  *bind_bool;
    int64_t *bind_ts2;
    int16_t *bind_smallint;
    uint16_t *bind_usmallint;
    int8_t  *bind_tinyint;
    uint8_t  *bind_utinyint;

    switch(data_type) {
        case TSDB_DATA_TYPE_BINARY:
            if (dataLen > TSDB_MAX_BINARY_LEN) {
                errorPrint2("binary length overflow, max size:%u\n",
                        (uint32_t)TSDB_MAX_BINARY_LEN);
                return -1;
            }
            char *bind_binary;

            kit_bind->buffer_type = TSDB_DATA_TYPE_BINARY;
            if (value) {
                bind_binary = calloc(1, strlen(value) + 1);
                strncpy(bind_binary, value, strlen(value));
                kit_bind->buffer_length = strlen(bind_binary);
            } else {
                bind_binary = calloc(1, dataLen + 1);
                rand_string(bind_binary, dataLen);
                kit_bind->buffer_length = dataLen;
            }

            kit_bind->length = &kit_bind->buffer_length;
            kit_bind->buffer = bind_binary;
            kit_bind->is_null = NULL;
            break;

        case TSDB_DATA_TYPE_NCHAR:
            if (dataLen > TSDB_MAX_BINARY_LEN) {
                errorPrint2("nchar length overflow, max size:%u\n",
                        (uint32_t)TSDB_MAX_BINARY_LEN);
                return -1;
            }
            char *bind_nchar;

            kit_bind->buffer_type = TSDB_DATA_TYPE_NCHAR;
            if (value) {
                bind_nchar = calloc(1, strlen(value) + 1);
                strncpy(bind_nchar, value, strlen(value));
            } else {
                bind_nchar = calloc(1, dataLen + 1);
                rand_string(bind_nchar, dataLen);
            }

            kit_bind->buffer_length = strlen(bind_nchar);
            kit_bind->buffer = bind_nchar;
            kit_bind->length = &kit_bind->buffer_length;
            kit_bind->is_null = NULL;
            break;

        case TSDB_DATA_TYPE_INT:
            bind_int = malloc(sizeof(int32_t));
            assert(bind_int);

            if (value) {
                *bind_int = atoi(value);
            } else {
                *bind_int = rand_int();
            }
            kit_bind->buffer_type = TSDB_DATA_TYPE_INT;
            kit_bind->buffer_length = sizeof(int32_t);
            kit_bind->buffer = bind_int;
            kit_bind->length = &kit_bind->buffer_length;
            kit_bind->is_null = NULL;
            break;

        case TSDB_DATA_TYPE_UINT:
            bind_uint = malloc(sizeof(uint32_t));
            assert(bind_uint);

            if (value) {
                *bind_uint = atoi(value);
            } else {
                *bind_uint = rand_int();
            }
            kit_bind->buffer_type = TSDB_DATA_TYPE_UINT;
            kit_bind->buffer_length = sizeof(uint32_t);
            kit_bind->buffer = bind_uint;
            kit_bind->length = &kit_bind->buffer_length;
            kit_bind->is_null = NULL;
            break;

        case TSDB_DATA_TYPE_BIGINT:
            bind_bigint = malloc(sizeof(int64_t));
            assert(bind_bigint);

            if (value) {
                *bind_bigint = atoll(value);
            } else {
                *bind_bigint = rand_bigint();
            }
            kit_bind->buffer_type = TSDB_DATA_TYPE_BIGINT;
            kit_bind->buffer_length = sizeof(int64_t);
            kit_bind->buffer = bind_bigint;
            kit_bind->length = &kit_bind->buffer_length;
            kit_bind->is_null = NULL;
            break;

        case TSDB_DATA_TYPE_UBIGINT:
            bind_ubigint = malloc(sizeof(uint64_t));
            assert(bind_ubigint);

            if (value) {
                *bind_ubigint = atoll(value);
            } else {
                *bind_ubigint = rand_bigint();
            }
            kit_bind->buffer_type = TSDB_DATA_TYPE_UBIGINT;
            kit_bind->buffer_length = sizeof(uint64_t);
            kit_bind->buffer = bind_ubigint;
            kit_bind->length = &kit_bind->buffer_length;
            kit_bind->is_null = NULL;
            break;

        case TSDB_DATA_TYPE_FLOAT:
            bind_float = malloc(sizeof(float));
            assert(bind_float);

            if (value) {
                *bind_float = (float)atof(value);
            } else {
                *bind_float = rand_float();
            }
            kit_bind->buffer_type = TSDB_DATA_TYPE_FLOAT;
            kit_bind->buffer_length = sizeof(float);
            kit_bind->buffer = bind_float;
            kit_bind->length = &kit_bind->buffer_length;
            kit_bind->is_null = NULL;
            break;

        case TSDB_DATA_TYPE_DOUBLE:
            bind_double = malloc(sizeof(double));
            assert(bind_double);

            if (value) {
                *bind_double = atof(value);
            } else {
                *bind_double = rand_double();
            }
            kit_bind->buffer_type = TSDB_DATA_TYPE_DOUBLE;
            kit_bind->buffer_length = sizeof(double);
            kit_bind->buffer = bind_double;
            kit_bind->length = &kit_bind->buffer_length;
            kit_bind->is_null = NULL;
            break;

        case TSDB_DATA_TYPE_SMALLINT:
            bind_smallint = malloc(sizeof(int16_t));
            assert(bind_smallint);

            if (value) {
                *bind_smallint = (int16_t)atoi(value);
            } else {
                *bind_smallint = rand_smallint();
            }
            kit_bind->buffer_type = TSDB_DATA_TYPE_SMALLINT;
            kit_bind->buffer_length = sizeof(int16_t);
            kit_bind->buffer = bind_smallint;
            kit_bind->length = &kit_bind->buffer_length;
            kit_bind->is_null = NULL;
            break;

        case TSDB_DATA_TYPE_USMALLINT:
            bind_usmallint = malloc(sizeof(uint16_t));
            assert(bind_usmallint);

            if (value) {
                *bind_usmallint = (uint16_t)atoi(value);
            } else {
                *bind_usmallint = rand_smallint();
            }
            kit_bind->buffer_type = TSDB_DATA_TYPE_SMALLINT;
            kit_bind->buffer_length = sizeof(uint16_t);
            kit_bind->buffer = bind_usmallint;
            kit_bind->length = &kit_bind->buffer_length;
            kit_bind->is_null = NULL;
            break;

        case TSDB_DATA_TYPE_TINYINT:
            bind_tinyint = malloc(sizeof(int8_t));
            assert(bind_tinyint);

            if (value) {
                *bind_tinyint = (int8_t)atoi(value);
            } else {
                *bind_tinyint = rand_tinyint();
            }
            kit_bind->buffer_type = TSDB_DATA_TYPE_TINYINT;
            kit_bind->buffer_length = sizeof(int8_t);
            kit_bind->buffer = bind_tinyint;
            kit_bind->length = &kit_bind->buffer_length;
            kit_bind->is_null = NULL;
            break;

        case TSDB_DATA_TYPE_UTINYINT:
            bind_utinyint = malloc(sizeof(uint8_t));
            assert(bind_utinyint);

            if (value) {
                *bind_utinyint = (int8_t)atoi(value);
            } else {
                *bind_utinyint = rand_tinyint();
            }
            kit_bind->buffer_type = TSDB_DATA_TYPE_UTINYINT;
            kit_bind->buffer_length = sizeof(uint8_t);
            kit_bind->buffer = bind_utinyint;
            kit_bind->length = &kit_bind->buffer_length;
            kit_bind->is_null = NULL;
            break;

        case TSDB_DATA_TYPE_BOOL:
            bind_bool = malloc(sizeof(int8_t));
            assert(bind_bool);

            if (value) {
                if (strncasecmp(value, "true", 4)) {
                    *bind_bool = true;
                } else {
                    *bind_bool = false;
                }
            } else {
                *bind_bool = rand_bool();
            }
            kit_bind->buffer_type = TSDB_DATA_TYPE_BOOL;
            kit_bind->buffer_length = sizeof(int8_t);
            kit_bind->buffer = bind_bool;
            kit_bind->length = &kit_bind->buffer_length;
            kit_bind->is_null = NULL;
            break;

        case TSDB_DATA_TYPE_TIMESTAMP:
            bind_ts2 = malloc(sizeof(int64_t));
            assert(bind_ts2);

            if (value) {
                if (strchr(value, ':') && strchr(value, '-')) {
                    int i = 0;
                    while(value[i] != '\0') {
                        if (value[i] == '\"' || value[i] == '\'') {
                            value[i] = ' ';
                        }
                        i++;
                    }
                    int64_t tmpEpoch;
                    if (TSDB_CODE_SUCCESS != taosParseTime(
                                value, &tmpEpoch, strlen(value),
                                timePrec, 0)) {
                        free(bind_ts2);
                        errorPrint2("Input %s, time format error!\n", value);
                        return -1;
                    }
                    *bind_ts2 = tmpEpoch;
                } else {
                    *bind_ts2 = atoll(value);
                }
            } else {
                *bind_ts2 = rand_bigint();
            }
            kit_bind->buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
            kit_bind->buffer_length = sizeof(int64_t);
            kit_bind->buffer = bind_ts2;
            kit_bind->length = &kit_bind->buffer_length;
            kit_bind->is_null = NULL;
            break;

        case TSDB_DATA_TYPE_NULL:
            break;

        default:
            errorPrint2("Not support data type: %d\n", data_type);
            exit(EXIT_FAILURE);
    }

    return 0;
}

static int32_t prepareStmtBindArrayByTypeForRand(
        TAOS_BIND *kit_bind,
        char data_type, int32_t dataLen,
        int32_t timePrec,
        char **ptr,
        char *value)
{
    int32_t *bind_int;
    uint32_t *bind_uint;
    int64_t *bind_bigint;
    uint64_t *bind_ubigint;
    float   *bind_float;
    double  *bind_double;
    int16_t *bind_smallint;
    uint16_t *bind_usmallint;
    int8_t  *bind_tinyint;
    uint8_t  *bind_utinyint;
    int8_t  *bind_bool;
    int64_t *bind_ts2;

    switch(data_type) {
        case TSDB_DATA_TYPE_BINARY:

            if (dataLen > TSDB_MAX_BINARY_LEN) {
                errorPrint2("binary length overflow, max size:%u\n",
                        (uint32_t)TSDB_MAX_BINARY_LEN);
                return -1;
            }
            char *bind_binary = (char *)*ptr;

            kit_bind->buffer_type = TSDB_DATA_TYPE_BINARY;
            if (value) {
                strncpy(bind_binary, value, strlen(value));
                kit_bind->buffer_length = strlen(bind_binary);
            } else {
                rand_string(bind_binary, dataLen);
                kit_bind->buffer_length = dataLen;
            }

            kit_bind->length = &kit_bind->buffer_length;
            kit_bind->buffer = bind_binary;
            kit_bind->is_null = NULL;

            *ptr += kit_bind->buffer_length;
            break;

        case TSDB_DATA_TYPE_NCHAR:
            if (dataLen > TSDB_MAX_BINARY_LEN) {
                errorPrint2("nchar length overflow, max size: %u\n",
                        (uint32_t)TSDB_MAX_BINARY_LEN);
                return -1;
            }
            char *bind_nchar = (char *)*ptr;

            kit_bind->buffer_type = TSDB_DATA_TYPE_NCHAR;
            if (value) {
                strncpy(bind_nchar, value, strlen(value));
            } else {
                rand_string(bind_nchar, dataLen);
            }

            kit_bind->buffer_length = strlen(bind_nchar);
            kit_bind->buffer = bind_nchar;
            kit_bind->length = &kit_bind->buffer_length;
            kit_bind->is_null = NULL;

            *ptr += kit_bind->buffer_length;
            break;

        case TSDB_DATA_TYPE_INT:
            bind_int = (int32_t *)*ptr;

            if (value) {
                *bind_int = atoi(value);
            } else {
                *bind_int = rand_int();
            }
            kit_bind->buffer_type = TSDB_DATA_TYPE_INT;
            kit_bind->buffer_length = sizeof(int32_t);
            kit_bind->buffer = bind_int;
            kit_bind->length = &kit_bind->buffer_length;
            kit_bind->is_null = NULL;

            *ptr += kit_bind->buffer_length;
            break;

        case TSDB_DATA_TYPE_UINT:
            bind_uint = (uint32_t *)*ptr;

            if (value) {
                *bind_uint = atoi(value);
            } else {
                *bind_uint = rand_int();
            }
            kit_bind->buffer_type = TSDB_DATA_TYPE_UINT;
            kit_bind->buffer_length = sizeof(uint32_t);
            kit_bind->buffer = bind_uint;
            kit_bind->length = &kit_bind->buffer_length;
            kit_bind->is_null = NULL;

            *ptr += kit_bind->buffer_length;
            break;

        case TSDB_DATA_TYPE_BIGINT:
            bind_bigint = (int64_t *)*ptr;

            if (value) {
                *bind_bigint = atoll(value);
            } else {
                *bind_bigint = rand_bigint();
            }
            kit_bind->buffer_type = TSDB_DATA_TYPE_BIGINT;
            kit_bind->buffer_length = sizeof(int64_t);
            kit_bind->buffer = bind_bigint;
            kit_bind->length = &kit_bind->buffer_length;
            kit_bind->is_null = NULL;

            *ptr += kit_bind->buffer_length;
            break;

        case TSDB_DATA_TYPE_UBIGINT:
            bind_ubigint = (uint64_t *)*ptr;

            if (value) {
                *bind_ubigint = atoll(value);
            } else {
                *bind_ubigint = rand_bigint();
            }
            kit_bind->buffer_type = TSDB_DATA_TYPE_UBIGINT;
            kit_bind->buffer_length = sizeof(uint64_t);
            kit_bind->buffer = bind_ubigint;
            kit_bind->length = &kit_bind->buffer_length;
            kit_bind->is_null = NULL;

            *ptr += kit_bind->buffer_length;
            break;

        case TSDB_DATA_TYPE_FLOAT:
            bind_float = (float *)*ptr;

            if (value) {
                *bind_float = (float)atof(value);
            } else {
                *bind_float = rand_float();
            }
            kit_bind->buffer_type = TSDB_DATA_TYPE_FLOAT;
            kit_bind->buffer_length = sizeof(float);
            kit_bind->buffer = bind_float;
            kit_bind->length = &kit_bind->buffer_length;
            kit_bind->is_null = NULL;

            *ptr += kit_bind->buffer_length;
            break;

        case TSDB_DATA_TYPE_DOUBLE:
            bind_double = (double *)*ptr;

            if (value) {
                *bind_double = atof(value);
            } else {
                *bind_double = rand_double();
            }
            kit_bind->buffer_type = TSDB_DATA_TYPE_DOUBLE;
            kit_bind->buffer_length = sizeof(double);
            kit_bind->buffer = bind_double;
            kit_bind->length = &kit_bind->buffer_length;
            kit_bind->is_null = NULL;

            *ptr += kit_bind->buffer_length;
            break;

        case TSDB_DATA_TYPE_SMALLINT:
            bind_smallint = (int16_t *)*ptr;

            if (value) {
                *bind_smallint = (int16_t)atoi(value);
            } else {
                *bind_smallint = rand_smallint();
            }
            kit_bind->buffer_type = TSDB_DATA_TYPE_SMALLINT;
            kit_bind->buffer_length = sizeof(int16_t);
            kit_bind->buffer = bind_smallint;
            kit_bind->length = &kit_bind->buffer_length;
            kit_bind->is_null = NULL;

            *ptr += kit_bind->buffer_length;
            break;

        case TSDB_DATA_TYPE_USMALLINT:
            bind_usmallint = (uint16_t *)*ptr;

            if (value) {
                *bind_usmallint = (uint16_t)atoi(value);
            } else {
                *bind_usmallint = rand_smallint();
            }
            kit_bind->buffer_type = TSDB_DATA_TYPE_USMALLINT;
            kit_bind->buffer_length = sizeof(uint16_t);
            kit_bind->buffer = bind_usmallint;
            kit_bind->length = &kit_bind->buffer_length;
            kit_bind->is_null = NULL;

            *ptr += kit_bind->buffer_length;
            break;

        case TSDB_DATA_TYPE_TINYINT:
            bind_tinyint = (int8_t *)*ptr;

            if (value) {
                *bind_tinyint = (int8_t)atoi(value);
            } else {
                *bind_tinyint = rand_tinyint();
            }
            kit_bind->buffer_type = TSDB_DATA_TYPE_TINYINT;
            kit_bind->buffer_length = sizeof(int8_t);
            kit_bind->buffer = bind_tinyint;
            kit_bind->length = &kit_bind->buffer_length;
            kit_bind->is_null = NULL;

            *ptr += kit_bind->buffer_length;
            break;

        case TSDB_DATA_TYPE_UTINYINT:
            bind_utinyint = (uint8_t *)*ptr;

            if (value) {
                *bind_utinyint = (uint8_t)atoi(value);
            } else {
                *bind_utinyint = rand_tinyint();
            }
            kit_bind->buffer_type = TSDB_DATA_TYPE_UTINYINT;
            kit_bind->buffer_length = sizeof(uint8_t);
            kit_bind->buffer = bind_utinyint;
            kit_bind->length = &kit_bind->buffer_length;
            kit_bind->is_null = NULL;

            *ptr += kit_bind->buffer_length;
            break;

        case TSDB_DATA_TYPE_BOOL:
            bind_bool = (int8_t *)*ptr;

            if (value) {
                if (strncasecmp(value, "true", 4)) {
                    *bind_bool = true;
                } else {
                    *bind_bool = false;
                }
            } else {
                *bind_bool = rand_bool();
            }
            kit_bind->buffer_type = TSDB_DATA_TYPE_BOOL;
            kit_bind->buffer_length = sizeof(int8_t);
            kit_bind->buffer = bind_bool;
            kit_bind->length = &kit_bind->buffer_length;
            kit_bind->is_null = NULL;

            *ptr += kit_bind->buffer_length;
            break;

        case TSDB_DATA_TYPE_TIMESTAMP:
            bind_ts2 = (int64_t *)*ptr;

            if (value) {
                if (strchr(value, ':') && strchr(value, '-')) {
                    int i = 0;
                    while(value[i] != '\0') {
                        if (value[i] == '\"' || value[i] == '\'') {
                            value[i] = ' ';
                        }
                        i++;
                    }
                    int64_t tmpEpoch;
                    if (TSDB_CODE_SUCCESS != taosParseTime(
                                value, &tmpEpoch, strlen(value),
                                timePrec, 0)) {
                        errorPrint2("Input %s, time format error!\n", value);
                        return -1;
                    }
                    *bind_ts2 = tmpEpoch;
                } else {
                    *bind_ts2 = atoll(value);
                }
            } else {
                *bind_ts2 = rand_bigint();
            }
            kit_bind->buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
            kit_bind->buffer_length = sizeof(int64_t);
            kit_bind->buffer = bind_ts2;
            kit_bind->length = &kit_bind->buffer_length;
            kit_bind->is_null = NULL;

            *ptr += kit_bind->buffer_length;
            break;

        default:
            errorPrint2("No support data type: %d\n", data_type);
            return -1;
    }

    return 0;
}

static int32_t prepareStmtWithoutStb(
        threadInfo *pThreadInfo,
        char *tableName,
        uint32_t batch,
        int64_t insertRows,
        int64_t recordFrom,
        int64_t startTime)
{
    TAOS_STMT *stmt = pThreadInfo->stmt;
    int ret = taos_stmt_set_tbname(stmt, tableName);
    if (ret != 0) {
        errorPrint2("failed to execute taos_stmt_set_tbname(%s). return 0x%x. reason: %s\n",
                tableName, ret, taos_stmt_errstr(stmt));
        return ret;
    }

    char *data_type = g_args.data_type;

    char *bindArray = malloc(sizeof(TAOS_BIND) * (g_args.columnCount + 1));
    if (bindArray == NULL) {
        errorPrint2("Failed to allocate %d bind params\n",
                (g_args.columnCount + 1));
        return -1;
    }

    int32_t k = 0;
    for (k = 0; k < batch;) {
        /* columnCount + 1 (ts) */

        TAOS_BIND *kit_bind = (TAOS_BIND *)(bindArray + 0);

        int64_t *bind_ts = pThreadInfo->bind_ts;

        kit_bind->buffer_type = TSDB_DATA_TYPE_TIMESTAMP;

        if (g_args.disorderRatio) {
            *bind_ts = startTime + getTSRandTail(
                    g_args.timestamp_step, k,
                    g_args.disorderRatio,
                    g_args.disorderRange);
        } else {
            *bind_ts = startTime + g_args.timestamp_step * k;
        }
        kit_bind->buffer_length = sizeof(int64_t);
        kit_bind->buffer = bind_ts;
        kit_bind->length = &kit_bind->buffer_length;
        kit_bind->is_null = NULL;

        for (int i = 0; i < g_args.columnCount; i ++) {
            kit_bind = (TAOS_BIND *)((char *)bindArray
                    + (sizeof(TAOS_BIND) * (i + 1)));
            if ( -1 == prepareStmtBindArrayByType(
                        kit_bind,
                        data_type[i],
                        g_args.binwidth,
                        pThreadInfo->time_precision,
                        NULL)) {
                free(bindArray);
                return -1;
            }
        }
        if (0 != taos_stmt_bind_param(stmt, (TAOS_BIND *)bindArray)) {
            errorPrint2("%s() LN%d, stmt_bind_param() failed! reason: %s\n",
                    __func__, __LINE__, taos_stmt_errstr(stmt));
            break;
        }
        // if msg > 3MB, break
        if (0 != taos_stmt_add_batch(stmt)) {
            errorPrint2("%s() LN%d, stmt_add_batch() failed! reason: %s\n",
                    __func__, __LINE__, taos_stmt_errstr(stmt));
            break;
        }

        k++;
        recordFrom ++;
        if (recordFrom >= insertRows) {
            break;
        }
    }

    free(bindArray);
    return k;
}

static int32_t prepareStbStmtBindTag(
        char *bindArray, SSuperTable *stbInfo,
        char *tagsVal,
        int32_t timePrec)
{
    TAOS_BIND *tag;

    for (int t = 0; t < stbInfo->tagCount; t ++) {
        tag = (TAOS_BIND *)((char *)bindArray + (sizeof(TAOS_BIND) * t));
        if ( -1 == prepareStmtBindArrayByType(
                    tag,
                    stbInfo->tags[t].data_type,
                    stbInfo->tags[t].dataLen,
                    timePrec,
                    NULL)) {
            return -1;
        }
    }

    return 0;
}

static int32_t prepareStbStmtBindRand(
        int64_t *ts,
        char *bindArray, SSuperTable *stbInfo,
        int64_t startTime, int32_t recSeq,
        int32_t timePrec)
{
    char data[MAX_DATA_SIZE];
    memset(data, 0, MAX_DATA_SIZE);
    char *ptr = data;

    TAOS_BIND *kit_bind;

    for (int i = 0; i < stbInfo->columnCount + 1; i ++) {
        kit_bind = (TAOS_BIND *)((char *)bindArray + (sizeof(TAOS_BIND) * i));

        if (i == 0) {
            int64_t *bind_ts = ts;

            kit_bind->buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
            if (stbInfo->disorderRatio) {
                *bind_ts = startTime + getTSRandTail(
                        stbInfo->timeStampStep, recSeq,
                        stbInfo->disorderRatio,
                        stbInfo->disorderRange);
            } else {
                *bind_ts = startTime + stbInfo->timeStampStep * recSeq;
            }
            kit_bind->buffer_length = sizeof(int64_t);
            kit_bind->buffer = bind_ts;
            kit_bind->length = &kit_bind->buffer_length;
            kit_bind->is_null = NULL;

            ptr += kit_bind->buffer_length;
        } else if ( -1 == prepareStmtBindArrayByTypeForRand(
                    kit_bind,
                    stbInfo->columns[i-1].data_type,
                    stbInfo->columns[i-1].dataLen,
                    timePrec,
                    &ptr,
                    NULL)) {
            return -1;
        }
    }

    return 0;
}

UNUSED_FUNC static int32_t prepareStbStmtRand(
        threadInfo *pThreadInfo,
        char *tableName,
        int64_t tableSeq,
        uint32_t batch,
        uint64_t insertRows,
        uint64_t recordFrom,
        int64_t startTime)
{
    int ret;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    TAOS_STMT *stmt = pThreadInfo->stmt;

    if (AUTO_CREATE_SUBTBL == stbInfo->autoCreateTable) {
        char* tagsValBuf = NULL;

        if (0 == stbInfo->tagSource) {
            tagsValBuf = generateTagValuesForStb(stbInfo, tableSeq);
        } else {
            tagsValBuf = getTagValueFromTagSample(
                    stbInfo,
                    tableSeq % stbInfo->tagSampleCount);
        }

        if (NULL == tagsValBuf) {
            errorPrint2("%s() LN%d, tag buf failed to allocate  memory\n",
                    __func__, __LINE__);
            return -1;
        }

        char *tagsArray = calloc(1, sizeof(TAOS_BIND) * stbInfo->tagCount);
        if (NULL == tagsArray) {
            tmfree(tagsValBuf);
            errorPrint2("%s() LN%d, tag buf failed to allocate  memory\n",
                    __func__, __LINE__);
            return -1;
        }

        if (-1 == prepareStbStmtBindTag(
                    tagsArray, stbInfo, tagsValBuf, pThreadInfo->time_precision
                    /* is tag */)) {
            tmfree(tagsValBuf);
            tmfree(tagsArray);
            return -1;
        }

        ret = taos_stmt_set_tbname_tags(stmt, tableName, (TAOS_BIND *)tagsArray);

        tmfree(tagsValBuf);
        tmfree(tagsArray);

        if (0 != ret) {
            errorPrint2("%s() LN%d, stmt_set_tbname_tags() failed! reason: %s\n",
                    __func__, __LINE__, taos_stmt_errstr(stmt));
            return -1;
        }
    } else {
        ret = taos_stmt_set_tbname(stmt, tableName);
        if (0 != ret) {
            errorPrint2("%s() LN%d, stmt_set_tbname() failed! reason: %s\n",
                    __func__, __LINE__, taos_stmt_errstr(stmt));
            return -1;
        }
    }

    char *bindArray = calloc(1, sizeof(TAOS_BIND) * (stbInfo->columnCount + 1));
    if (bindArray == NULL) {
        errorPrint2("%s() LN%d, Failed to allocate %d bind params\n",
                __func__, __LINE__, (stbInfo->columnCount + 1));
        return -1;
    }

    uint32_t k;
    for (k = 0; k < batch;) {
        /* columnCount + 1 (ts) */
        if (-1 == prepareStbStmtBindRand(
                    pThreadInfo->bind_ts,
                    bindArray, stbInfo,
                    startTime, k,
                    pThreadInfo->time_precision
                    /* is column */)) {
            free(bindArray);
            return -1;
        }
        ret = taos_stmt_bind_param(stmt, (TAOS_BIND *)bindArray);
        if (0 != ret) {
            errorPrint2("%s() LN%d, stmt_bind_param() failed! reason: %s\n",
                    __func__, __LINE__, taos_stmt_errstr(stmt));
            free(bindArray);
            return -1;
        }
        // if msg > 3MB, break
        ret = taos_stmt_add_batch(stmt);
        if (0 != ret) {
            errorPrint2("%s() LN%d, stmt_add_batch() failed! reason: %s\n",
                    __func__, __LINE__, taos_stmt_errstr(stmt));
            free(bindArray);
            return -1;
        }

        k++;
        recordFrom ++;

        if (recordFrom >= insertRows) {
            break;
        }
    }

    free(bindArray);
    return k;
}

#if STMT_BIND_PARAM_BATCH == 1
static int execStbBindParamBatch(
        threadInfo *pThreadInfo,
        char *tableName,
        int64_t tableSeq,
        uint32_t batch,
        uint64_t insertRows,
        uint64_t recordFrom,
        int64_t startTime,
        int64_t *pSamplePos)
{
    int ret;
    TAOS_STMT *stmt = pThreadInfo->stmt;

    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    assert(stbInfo);

    uint32_t columnCount = pThreadInfo->stbInfo->columnCount;

    uint32_t thisBatch = MAX_SAMPLES - (*pSamplePos);

    if (thisBatch > batch) {
        thisBatch = batch;
    }
    verbosePrint("%s() LN%d, batch=%d pos=%"PRId64" thisBatch=%d\n",
            __func__, __LINE__, batch, *pSamplePos, thisBatch);

    memset(pThreadInfo->bindParams, 0,
            (sizeof(TAOS_MULTI_BIND) * (columnCount + 1)));
    memset(pThreadInfo->is_null, 0, thisBatch);

    for (int c = 0; c < columnCount + 1; c ++) {
        TAOS_MULTI_BIND *param = (TAOS_MULTI_BIND *)(pThreadInfo->bindParams + sizeof(TAOS_MULTI_BIND) * c);

        char data_type;

        if (c == 0) {
            data_type = TSDB_DATA_TYPE_TIMESTAMP;
            param->buffer_length = sizeof(int64_t);
            param->buffer = pThreadInfo->bind_ts_array;

        } else {
            data_type = stbInfo->columns[c-1].data_type;

            char *tmpP;

            switch(data_type) {
                case TSDB_DATA_TYPE_BINARY:
                    param->buffer_length =
                        stbInfo->columns[c-1].dataLen;

                    tmpP =
                        (char *)((uintptr_t)*(uintptr_t*)(stbInfo->sampleBindBatchArray
                                    +sizeof(char*)*(c-1)));

                    verbosePrint("%s() LN%d, tmpP=%p pos=%"PRId64" width=%"PRIxPTR" position=%"PRId64"\n",
                            __func__, __LINE__, tmpP, *pSamplePos, param->buffer_length,
                            (*pSamplePos) * param->buffer_length);

                    param->buffer = (void *)(tmpP + *pSamplePos * param->buffer_length);
                    break;

                case TSDB_DATA_TYPE_NCHAR:
                    param->buffer_length =
                        stbInfo->columns[c-1].dataLen;

                    tmpP =
                        (char *)((uintptr_t)*(uintptr_t*)(stbInfo->sampleBindBatchArray
                                    +sizeof(char*)*(c-1)));

                    verbosePrint("%s() LN%d, tmpP=%p pos=%"PRId64" width=%"PRIxPTR" position=%"PRId64"\n",
                            __func__, __LINE__, tmpP, *pSamplePos, param->buffer_length,
                            (*pSamplePos) * param->buffer_length);

                    param->buffer = (void *)(tmpP + *pSamplePos * param->buffer_length);
                    break;

                case TSDB_DATA_TYPE_INT:
                case TSDB_DATA_TYPE_UINT:
                    param->buffer_length = sizeof(int32_t);
                    param->buffer =
                        (void *)((uintptr_t)*(uintptr_t*)(stbInfo->sampleBindBatchArray+sizeof(char*)*(c-1))
                                        + stbInfo->columns[c-1].dataLen * (*pSamplePos));
                    break;

                case TSDB_DATA_TYPE_TINYINT:
                case TSDB_DATA_TYPE_UTINYINT:
                    param->buffer_length = sizeof(int8_t);
                    param->buffer =
                        (void *)((uintptr_t)*(uintptr_t*)(
                                    stbInfo->sampleBindBatchArray
                                    +sizeof(char*)*(c-1))
                                + stbInfo->columns[c-1].dataLen*(*pSamplePos));
                    break;

                case TSDB_DATA_TYPE_SMALLINT:
                case TSDB_DATA_TYPE_USMALLINT:
                    param->buffer_length = sizeof(int16_t);
                    param->buffer =
                        (void *)((uintptr_t)*(uintptr_t*)(stbInfo->sampleBindBatchArray+sizeof(char*)*(c-1))
                                        + stbInfo->columns[c-1].dataLen * (*pSamplePos));
                    break;

                case TSDB_DATA_TYPE_BIGINT:
                case TSDB_DATA_TYPE_UBIGINT:
                    param->buffer_length = sizeof(int64_t);
                    param->buffer =
                        (void *)((uintptr_t)*(uintptr_t*)(stbInfo->sampleBindBatchArray+sizeof(char*)*(c-1))
                                        + stbInfo->columns[c-1].dataLen * (*pSamplePos));
                    break;

                case TSDB_DATA_TYPE_BOOL:
                    param->buffer_length = sizeof(int8_t);
                    param->buffer =
                        (void *)((uintptr_t)*(uintptr_t*)(stbInfo->sampleBindBatchArray+sizeof(char*)*(c-1))
                                        + stbInfo->columns[c-1].dataLen * (*pSamplePos));
                    break;

                case TSDB_DATA_TYPE_FLOAT:
                    param->buffer_length = sizeof(float);
                    param->buffer =
                        (void *)((uintptr_t)*(uintptr_t*)(stbInfo->sampleBindBatchArray+sizeof(char*)*(c-1))
                                        + stbInfo->columns[c-1].dataLen * (*pSamplePos));
                    break;

                case TSDB_DATA_TYPE_DOUBLE:
                    param->buffer_length = sizeof(double);
                    param->buffer =
                        (void *)((uintptr_t)*(uintptr_t*)(stbInfo->sampleBindBatchArray+sizeof(char*)*(c-1))
                                        + stbInfo->columns[c-1].dataLen * (*pSamplePos));
                    break;

                case TSDB_DATA_TYPE_TIMESTAMP:
                    param->buffer_length = sizeof(int64_t);
                    param->buffer =
                        (void *)((uintptr_t)*(uintptr_t*)(stbInfo->sampleBindBatchArray+sizeof(char*)*(c-1))
                                        + stbInfo->columns[c-1].dataLen * (*pSamplePos));
                    break;

                default:
                    errorPrint("%s() LN%d, wrong data type: %d\n",
                            __func__,
                            __LINE__,
                            data_type);
                    exit(EXIT_FAILURE);

            }
        }

        param->buffer_type = data_type;
        param->length = malloc(sizeof(int32_t) * thisBatch);
        assert(param->length);

        for (int b = 0; b < thisBatch; b++) {
            if (param->buffer_type == TSDB_DATA_TYPE_NCHAR) {
                param->length[b] = strlen(
                     (char *)param->buffer + b *
                         stbInfo->columns[c].dataLen
                        );
            } else {
                param->length[b] = param->buffer_length;
            }
        }
        param->is_null = pThreadInfo->is_null;
        param->num = thisBatch;
    }

    uint32_t k;
    for (k = 0; k < thisBatch;) {
        /* columnCount + 1 (ts) */
        if (stbInfo->disorderRatio) {
            *(pThreadInfo->bind_ts_array + k) = startTime + getTSRandTail(
                    stbInfo->timeStampStep, k,
                    stbInfo->disorderRatio,
                    stbInfo->disorderRange);
        } else {
            *(pThreadInfo->bind_ts_array + k) = startTime + stbInfo->timeStampStep * k;
        }

        debugPrint("%s() LN%d, k=%d ts=%"PRId64"\n",
                __func__, __LINE__,
                k, *(pThreadInfo->bind_ts_array +k));
        k++;
        recordFrom ++;

        (*pSamplePos) ++;
        if ((*pSamplePos) == MAX_SAMPLES) {
            *pSamplePos = 0;
        }

        if (recordFrom >= insertRows) {
            break;
        }
    }

    ret = taos_stmt_bind_param_batch(stmt, (TAOS_MULTI_BIND *)pThreadInfo->bindParams);
    if (0 != ret) {
        errorPrint2("%s() LN%d, stmt_bind_param() failed! reason: %s\n",
                __func__, __LINE__, taos_stmt_errstr(stmt));
        return -1;
    }

    for (int c = 0; c < stbInfo->columnCount + 1; c ++) {
        TAOS_MULTI_BIND *param = (TAOS_MULTI_BIND *)(pThreadInfo->bindParams + sizeof(TAOS_MULTI_BIND) * c);
        free(param->length);
    }

    // if msg > 3MB, break
    ret = taos_stmt_add_batch(stmt);
    if (0 != ret) {
        errorPrint2("%s() LN%d, stmt_add_batch() failed! reason: %s\n",
                __func__, __LINE__, taos_stmt_errstr(stmt));
        return -1;
    }
    return k;
}

static int parseSamplefileToStmtBatch(
        SSuperTable* stbInfo)
{
    // char *sampleDataBuf = (stbInfo)?
    //    stbInfo->sampleDataBuf:g_sampleDataBuf;
    int32_t columnCount = (stbInfo)?stbInfo->columnCount:g_args.columnCount;
    char *sampleBindBatchArray = NULL;

    if (stbInfo) {
        stbInfo->sampleBindBatchArray = calloc(1, sizeof(uintptr_t *) * columnCount);
        sampleBindBatchArray = stbInfo->sampleBindBatchArray;
    } else {
        g_sampleBindBatchArray = calloc(1, sizeof(uintptr_t *) * columnCount);
        sampleBindBatchArray = g_sampleBindBatchArray;
    }
    assert(sampleBindBatchArray);

    for (int c = 0; c < columnCount; c++) {
        char data_type = (stbInfo)?stbInfo->columns[c].data_type:g_args.data_type[c];

        char *tmpP = NULL;

        switch(data_type) {
            case TSDB_DATA_TYPE_INT:
            case TSDB_DATA_TYPE_UINT:
                tmpP = calloc(1, sizeof(int) * MAX_SAMPLES);
                assert(tmpP);
                *(uintptr_t*)(sampleBindBatchArray+ sizeof(uintptr_t*)*c) = (uintptr_t)tmpP;
                break;

            case TSDB_DATA_TYPE_TINYINT:
            case TSDB_DATA_TYPE_UTINYINT:
                tmpP = calloc(1, sizeof(int8_t) * MAX_SAMPLES);
                assert(tmpP);
                *(uintptr_t*)(sampleBindBatchArray+ sizeof(uintptr_t*)*c) = (uintptr_t)tmpP;
                break;

            case TSDB_DATA_TYPE_SMALLINT:
            case TSDB_DATA_TYPE_USMALLINT:
                tmpP = calloc(1, sizeof(int16_t) * MAX_SAMPLES);
                assert(tmpP);
                *(uintptr_t*)(sampleBindBatchArray+ sizeof(uintptr_t*)*c) = (uintptr_t)tmpP;
                break;

            case TSDB_DATA_TYPE_BIGINT:
            case TSDB_DATA_TYPE_UBIGINT:
                tmpP = calloc(1, sizeof(int64_t) * MAX_SAMPLES);
                assert(tmpP);
                *(uintptr_t*)(sampleBindBatchArray+ sizeof(uintptr_t*)*c) = (uintptr_t)tmpP;
                break;

            case TSDB_DATA_TYPE_BOOL:
                tmpP = calloc(1, sizeof(int8_t) * MAX_SAMPLES);
                assert(tmpP);
                *(uintptr_t*)(sampleBindBatchArray+ sizeof(uintptr_t*)*c) = (uintptr_t)tmpP;
                break;

            case TSDB_DATA_TYPE_FLOAT:
                tmpP = calloc(1, sizeof(float) * MAX_SAMPLES);
                assert(tmpP);
                *(uintptr_t*)(sampleBindBatchArray+ sizeof(uintptr_t*)*c) = (uintptr_t)tmpP;
                break;

            case TSDB_DATA_TYPE_DOUBLE:
                tmpP = calloc(1, sizeof(double) * MAX_SAMPLES);
                assert(tmpP);
                *(uintptr_t*)(sampleBindBatchArray+ sizeof(uintptr_t*)*c) = (uintptr_t)tmpP;
                break;

            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_NCHAR:
                tmpP = calloc(1, MAX_SAMPLES *
                        (((stbInfo)?stbInfo->columns[c].dataLen:g_args.binwidth) + 1));
                assert(tmpP);
                *(uintptr_t*)(sampleBindBatchArray+ sizeof(uintptr_t*)*c) = (uintptr_t)tmpP;
                break;

            case TSDB_DATA_TYPE_TIMESTAMP:
                tmpP = calloc(1, sizeof(int64_t) * MAX_SAMPLES);
                assert(tmpP);
                *(uintptr_t*)(sampleBindBatchArray+ sizeof(uintptr_t*)*c) = (uintptr_t)tmpP;
                break;

            default:
                errorPrint("Unknown data type: %s\n",
                        (stbInfo)?stbInfo->columns[c].dataType:g_args.dataType[c]);
                exit(EXIT_FAILURE);
        }
    }

    char *sampleDataBuf = (stbInfo)?stbInfo->sampleDataBuf:g_sampleDataBuf;
    int64_t lenOfOneRow = (stbInfo)?stbInfo->lenOfOneRow:g_args.lenOfOneRow;

    for (int i=0; i < MAX_SAMPLES; i++) {
        int cursor = 0;

        for (int c = 0; c < columnCount; c++) {
            char data_type = (stbInfo)?
                stbInfo->columns[c].data_type:
                g_args.data_type[c];
            char *restStr = sampleDataBuf
                + lenOfOneRow * i + cursor;
            int lengthOfRest = strlen(restStr);

            int tdm_index = 0;
            for (tdm_index = 0; tdm_index < lengthOfRest; tdm_index ++) {
                if (restStr[tdm_index] == ',') {
                    break;
                }
            }

            char *tmpStr = calloc(1, tdm_index + 1);
            if (NULL == tmpStr) {
                errorPrint2("%s() LN%d, Failed to allocate %d bind buffer\n",
                        __func__, __LINE__, tdm_index + 1);
                return -1;
            }

            strncpy(tmpStr, restStr, tdm_index);
            cursor += tdm_index + 1; // skip ',' too
            char *tmpP;

            switch(data_type) {
                case TSDB_DATA_TYPE_INT:
                case TSDB_DATA_TYPE_UINT:
                    *((int32_t*)((uintptr_t)*(uintptr_t*)(sampleBindBatchArray
                                    +sizeof(char*)*c)+sizeof(int32_t)*i)) =
                        atoi(tmpStr);
                    break;

                case TSDB_DATA_TYPE_FLOAT:
                    *(float*)(((uintptr_t)*(uintptr_t*)(sampleBindBatchArray
                                    +sizeof(char*)*c)+sizeof(float)*i)) =
                        (float)atof(tmpStr);
                    break;

                case TSDB_DATA_TYPE_DOUBLE:
                    *(double*)(((uintptr_t)*(uintptr_t*)(sampleBindBatchArray
                                    +sizeof(char*)*c)+sizeof(double)*i)) =
                        atof(tmpStr);
                    break;

                case TSDB_DATA_TYPE_TINYINT:
                case TSDB_DATA_TYPE_UTINYINT:
                    *((int8_t*)((uintptr_t)*(uintptr_t*)(sampleBindBatchArray
                                    +sizeof(char*)*c)+sizeof(int8_t)*i)) =
                        (int8_t)atoi(tmpStr);
                    break;

                case TSDB_DATA_TYPE_SMALLINT:
                case TSDB_DATA_TYPE_USMALLINT:
                    *((int16_t*)((uintptr_t)*(uintptr_t*)(sampleBindBatchArray
                                    +sizeof(char*)*c)+sizeof(int16_t)*i)) =
                        (int16_t)atoi(tmpStr);
                    break;

                case TSDB_DATA_TYPE_BIGINT:
                case TSDB_DATA_TYPE_UBIGINT:
                    *((int64_t*)((uintptr_t)*(uintptr_t*)(sampleBindBatchArray
                                    +sizeof(char*)*c)+sizeof(int64_t)*i)) =
                        (int64_t)atol(tmpStr);
                    break;

                case TSDB_DATA_TYPE_BOOL:
                    *((int8_t*)((uintptr_t)*(uintptr_t*)(sampleBindBatchArray
                                    +sizeof(char*)*c)+sizeof(int8_t)*i)) =
                        (int8_t)atoi(tmpStr);
                    break;

                case TSDB_DATA_TYPE_TIMESTAMP:
                    *((int64_t*)((uintptr_t)*(uintptr_t*)(sampleBindBatchArray
                                    +sizeof(char*)*c)+sizeof(int64_t)*i)) =
                        (int64_t)atol(tmpStr);
                    break;

                case TSDB_DATA_TYPE_BINARY:
                case TSDB_DATA_TYPE_NCHAR:
                    tmpP = (char *)(*(uintptr_t*)(sampleBindBatchArray
                            +sizeof(char*)*c));
                    strcpy(tmpP + i*
                        (((stbInfo)?stbInfo->columns[c].dataLen:g_args.binwidth))
                    , tmpStr);
                    break;

                default:
                    break;
            }

            free(tmpStr);
        }
    }

    return 0;
}

static int parseSampleToStmtBatchForThread(
        threadInfo *pThreadInfo, SSuperTable *stbInfo,
        uint32_t timePrec,
        uint32_t batch)
{
    uint32_t columnCount = (stbInfo)?stbInfo->columnCount:g_args.columnCount;

    pThreadInfo->bind_ts_array = malloc(sizeof(int64_t) * batch);
    assert(pThreadInfo->bind_ts_array);

    pThreadInfo->bindParams = malloc(sizeof(TAOS_MULTI_BIND) * (columnCount + 1));
    assert(pThreadInfo->bindParams);

    pThreadInfo->is_null = malloc(batch);
    assert(pThreadInfo->is_null);

    return 0;
}

static int parseStbSampleToStmtBatchForThread(
        threadInfo *pThreadInfo,
        SSuperTable *stbInfo,
        uint32_t timePrec,
        uint32_t batch)
{
    return parseSampleToStmtBatchForThread(
        pThreadInfo, stbInfo, timePrec, batch);
}

static int parseNtbSampleToStmtBatchForThread(
        threadInfo *pThreadInfo, uint32_t timePrec, uint32_t batch)
{
    return parseSampleToStmtBatchForThread(
        pThreadInfo, NULL, timePrec, batch);
}

#else
static int parseSampleToStmt(
        threadInfo *pThreadInfo,
        SSuperTable *stbInfo, uint32_t timePrec)
{
    pThreadInfo->sampleBindArray =
        (char *)calloc(1, sizeof(char *) * MAX_SAMPLES);
    if (pThreadInfo->sampleBindArray == NULL) {
        errorPrint2("%s() LN%d, Failed to allocate %"PRIu64" bind array buffer\n",
                __func__, __LINE__,
                (uint64_t)sizeof(char *) * MAX_SAMPLES);
        return -1;
    }

    int32_t columnCount = (stbInfo)?stbInfo->columnCount:g_args.columnCount;
    char *sampleDataBuf = (stbInfo)?stbInfo->sampleDataBuf:g_sampleDataBuf;
    int64_t lenOfOneRow = (stbInfo)?stbInfo->lenOfOneRow:g_args.lenOfOneRow;

    for (int i=0; i < MAX_SAMPLES; i++) {
        char *bindArray =
            calloc(1, sizeof(TAOS_BIND) * (columnCount + 1));
        if (bindArray == NULL) {
            errorPrint2("%s() LN%d, Failed to allocate %d bind params\n",
                    __func__, __LINE__, (columnCount + 1));
            return -1;
        }

        TAOS_BIND *kit_bind;
        int cursor = 0;

        for (int c = 0; c < columnCount + 1; c++) {
            kit_bind = (TAOS_BIND *)((char *)bindArray + (sizeof(TAOS_BIND) * c));

            if (c == 0) {
                kit_bind->buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
                kit_bind->buffer_length = sizeof(int64_t);
                kit_bind->buffer = NULL; //bind_ts;
                kit_bind->length = &kit_bind->buffer_length;
                kit_bind->is_null = NULL;
            } else {
                char data_type = (stbInfo)?
                    stbInfo->columns[c-1].data_type:
                    g_args.data_type[c-1];
                int32_t dataLen = (stbInfo)?
                    stbInfo->columns[c-1].dataLen:
                    g_args.binwidth;
                char *restStr = sampleDataBuf
                    + lenOfOneRow * i + cursor;
                int lengthOfRest = strlen(restStr);

                int tdm_index = 0;
                for (tdm_index = 0; tdm_index < lengthOfRest; tdm_index ++) {
                    if (restStr[tdm_index] == ',') {
                        break;
                    }
                }

                char *bindBuffer = calloc(1, tdm_index + 1);
                if (bindBuffer == NULL) {
                    errorPrint2("%s() LN%d, Failed to allocate %d bind buffer\n",
                            __func__, __LINE__, tdm_index + 1);
                    return -1;
                }

                strncpy(bindBuffer, restStr, tdm_index);
                cursor += tdm_index + 1; // skip ',' too

                if (-1 == prepareStmtBindArrayByType(
                            kit_bind,
                            data_type,
                            dataLen,
                            timePrec,
                            bindBuffer)) {
                    free(bindBuffer);
                    free(bindArray);
                    return -1;
                }
                free(bindBuffer);
            }
        }
        *((uintptr_t *)(pThreadInfo->sampleBindArray + (sizeof(char *)) * i)) =
            (uintptr_t)bindArray;
    }

    return 0;
}

static int parseStbSampleToStmt(
        threadInfo *pThreadInfo,
        SSuperTable *stbInfo, uint32_t timePrec)
{
    return parseSampleToStmt(
        pThreadInfo,
        stbInfo, timePrec);
}

static int parseNtbSampleToStmt(
        threadInfo *pThreadInfo,
        uint32_t timePrec)
{
    return parseSampleToStmt(
        pThreadInfo,
        NULL,
        timePrec);
}

static int32_t prepareStbStmtBindStartTime(
        char *tableName,
        int64_t *ts,
        char *bindArray, SSuperTable *stbInfo,
        int64_t startTime, int32_t recSeq)
{
    TAOS_BIND *kit_bind;

    kit_bind = (TAOS_BIND *)bindArray;

    int64_t *bind_ts = ts;

    kit_bind->buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    if (stbInfo->disorderRatio) {
        *bind_ts = startTime + getTSRandTail(
                stbInfo->timeStampStep, recSeq,
                stbInfo->disorderRatio,
                stbInfo->disorderRange);
    } else {
        *bind_ts = startTime + stbInfo->timeStampStep * recSeq;
    }

    verbosePrint("%s() LN%d, tableName: %s, bind_ts=%"PRId64"\n",
            __func__, __LINE__, tableName, *bind_ts);

    kit_bind->buffer_length = sizeof(int64_t);
    kit_bind->buffer = bind_ts;
    kit_bind->length = &kit_bind->buffer_length;
    kit_bind->is_null = NULL;

    return 0;
}

static uint32_t execBindParam(
        threadInfo *pThreadInfo,
        char *tableName,
        int64_t tableSeq,
        uint32_t batch,
        uint64_t insertRows,
        uint64_t recordFrom,
        int64_t startTime,
        int64_t *pSamplePos)
{
    int ret;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    TAOS_STMT *stmt = pThreadInfo->stmt;

    uint32_t k;
    for (k = 0; k < batch;) {
        char *bindArray = (char *)(*((uintptr_t *)
                    (pThreadInfo->sampleBindArray + (sizeof(char *)) * (*pSamplePos))));
        /* columnCount + 1 (ts) */
        if (-1 == prepareStbStmtBindStartTime(
                    tableName,
                    pThreadInfo->bind_ts,
                    bindArray, stbInfo,
                    startTime, k
                    /* is column */)) {
            return -1;
        }
        ret = taos_stmt_bind_param(stmt, (TAOS_BIND *)bindArray);
        if (0 != ret) {
            errorPrint2("%s() LN%d, stmt_bind_param() failed! reason: %s\n",
                    __func__, __LINE__, taos_stmt_errstr(stmt));
            return -1;
        }
        // if msg > 3MB, break
        ret = taos_stmt_add_batch(stmt);
        if (0 != ret) {
            errorPrint2("%s() LN%d, stmt_add_batch() failed! reason: %s\n",
                    __func__, __LINE__, taos_stmt_errstr(stmt));
            return -1;
        }

        k++;
        recordFrom ++;

        (*pSamplePos) ++;
        if ((*pSamplePos) == MAX_SAMPLES) {
            *pSamplePos = 0;
        }

        if (recordFrom >= insertRows) {
            break;
        }
    }

    return k;
}
#endif

static int32_t prepareStbStmt(
        threadInfo *pThreadInfo,
        char *tableName,
        int64_t tableSeq,
        uint32_t batch,
        uint64_t insertRows,
        uint64_t recordFrom,
        int64_t startTime,
        int64_t *pSamplePos)
{
    int ret;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    TAOS_STMT *stmt = pThreadInfo->stmt;

    if (AUTO_CREATE_SUBTBL == stbInfo->autoCreateTable) {
        char* tagsValBuf = NULL;

        if (0 == stbInfo->tagSource) {
            tagsValBuf = generateTagValuesForStb(stbInfo, tableSeq);
        } else {
            tagsValBuf = getTagValueFromTagSample(
                    stbInfo,
                    tableSeq % stbInfo->tagSampleCount);
        }

        if (NULL == tagsValBuf) {
            errorPrint2("%s() LN%d, tag buf failed to allocate  memory\n",
                    __func__, __LINE__);
            return -1;
        }

        char *tagsArray = calloc(1, sizeof(TAOS_BIND) * stbInfo->tagCount);
        if (NULL == tagsArray) {
            tmfree(tagsValBuf);
            errorPrint2("%s() LN%d, tag buf failed to allocate  memory\n",
                    __func__, __LINE__);
            return -1;
        }

        if (-1 == prepareStbStmtBindTag(
                    tagsArray, stbInfo, tagsValBuf, pThreadInfo->time_precision
                    /* is tag */)) {
            tmfree(tagsValBuf);
            tmfree(tagsArray);
            return -1;
        }

        ret = taos_stmt_set_tbname_tags(stmt, tableName, (TAOS_BIND *)tagsArray);

        tmfree(tagsValBuf);
        tmfree(tagsArray);

        if (0 != ret) {
            errorPrint2("%s() LN%d, stmt_set_tbname_tags() failed! reason: %s\n",
                    __func__, __LINE__, taos_stmt_errstr(stmt));
            return -1;
        }
    } else {
        ret = taos_stmt_set_tbname(stmt, tableName);
        if (0 != ret) {
            errorPrint2("%s() LN%d, stmt_set_tbname() failed! reason: %s\n",
                    __func__, __LINE__, taos_stmt_errstr(stmt));
            return -1;
        }
    }

#if STMT_BIND_PARAM_BATCH == 1
    return execStbBindParamBatch(
        pThreadInfo,
        tableName,
        tableSeq,
        batch,
        insertRows,
        recordFrom,
        startTime,
        pSamplePos);
#else
    return execBindParam(
        pThreadInfo,
        tableName,
        tableSeq,
        batch,
        insertRows,
        recordFrom,
        startTime,
        pSamplePos);
#endif
}

static int32_t generateStbProgressiveData(
        SSuperTable *stbInfo,
        char *tableName,
        int64_t tableSeq,
        char *dbName, char *buffer,
        int64_t insertRows,
        uint64_t recordFrom, int64_t startTime, int64_t *pSamplePos,
        int64_t *pRemainderBufLen)
{
    assert(buffer != NULL);
    char *pstr = buffer;

    memset(pstr, 0, *pRemainderBufLen);

    int64_t headLen = generateStbSQLHead(
            stbInfo,
            tableName, tableSeq, dbName,
            buffer, *pRemainderBufLen);

    if (headLen <= 0) {
        return 0;
    }
    pstr += headLen;
    *pRemainderBufLen -= headLen;

    int64_t dataLen;

    return generateStbDataTail(stbInfo,
            g_args.reqPerReq, pstr, *pRemainderBufLen,
            insertRows, recordFrom,
            startTime,
            pSamplePos, &dataLen);
}

static int32_t generateProgressiveDataWithoutStb(
        char *tableName,
        /* int64_t tableSeq, */
        threadInfo *pThreadInfo, char *buffer,
        int64_t insertRows,
        uint64_t recordFrom, int64_t startTime, /*int64_t *pSamplePos, */
        int64_t *pRemainderBufLen)
{
    assert(buffer != NULL);
    char *pstr = buffer;

    memset(buffer, 0, *pRemainderBufLen);

    int64_t headLen = generateSQLHeadWithoutStb(
            tableName, pThreadInfo->db_name,
            buffer, *pRemainderBufLen);

    if (headLen <= 0) {
        return 0;
    }
    pstr += headLen;
    *pRemainderBufLen -= headLen;

    int64_t dataLen;

    return generateDataTailWithoutStb(
            g_args.reqPerReq, pstr, *pRemainderBufLen, insertRows, recordFrom,
            startTime,
            /*pSamplePos, */&dataLen);
}

static void printStatPerThread(threadInfo *pThreadInfo)
{
    if (0 == pThreadInfo->totalDelay)
        pThreadInfo->totalDelay = 1;

    fprintf(stderr, "====thread[%d] completed total inserted rows: %"PRIu64 ", total affected rows: %"PRIu64". %.2f records/second====\n",
            pThreadInfo->threadID,
            pThreadInfo->totalInsertRows,
            pThreadInfo->totalAffectedRows,
            (double)(pThreadInfo->totalAffectedRows/((double)pThreadInfo->totalDelay/1000000.0))
            );
}

#if STMT_BIND_PARAM_BATCH == 1
// stmt sync write interlace data
static void* syncWriteInterlaceStmtBatch(threadInfo *pThreadInfo, uint32_t interlaceRows) {
    debugPrint("[%d] %s() LN%d: ### stmt interlace write\n",
            pThreadInfo->threadID, __func__, __LINE__);

    int64_t insertRows;
    int64_t timeStampStep;
    uint64_t insert_interval;

    SSuperTable* stbInfo = pThreadInfo->stbInfo;

    if (stbInfo) {
        insertRows = stbInfo->insertRows;
        timeStampStep = stbInfo->timeStampStep;
        insert_interval = stbInfo->insertInterval;
    } else {
        insertRows = g_args.insertRows;
        timeStampStep = g_args.timestamp_step;
        insert_interval = g_args.insert_interval;
    }

    debugPrint("[%d] %s() LN%d: start_table_from=%"PRIu64" ntables=%"PRId64" insertRows=%"PRIu64"\n",
            pThreadInfo->threadID, __func__, __LINE__,
            pThreadInfo->start_table_from,
            pThreadInfo->ntables, insertRows);

    uint64_t timesInterlace = (insertRows / interlaceRows) + 1;
    uint32_t precalcBatch = interlaceRows;

    if (precalcBatch > g_args.reqPerReq)
        precalcBatch = g_args.reqPerReq;

    if (precalcBatch > MAX_SAMPLES)
        precalcBatch = MAX_SAMPLES;

    pThreadInfo->totalInsertRows = 0;
    pThreadInfo->totalAffectedRows = 0;

    uint64_t st = 0;
    uint64_t et = UINT64_MAX;

    uint64_t lastPrintTime = taosGetTimestampMs();
    uint64_t startTs = taosGetTimestampMs();
    uint64_t endTs;

    uint64_t tableSeq = pThreadInfo->start_table_from;
    int64_t startTime;

    bool flagSleep = true;
    uint64_t sleepTimeTotal = 0;

    int percentComplete = 0;
    int64_t totalRows = insertRows * pThreadInfo->ntables;
    pThreadInfo->samplePos = 0;

    for (int64_t interlace = 0;
            interlace < timesInterlace; interlace ++) {
        if ((flagSleep) && (insert_interval)) {
            st = taosGetTimestampMs();
            flagSleep = false;
        }

        int64_t generated = 0;
        int64_t samplePos;

        for (; tableSeq < pThreadInfo->start_table_from + pThreadInfo->ntables; tableSeq ++) {
            char tableName[TSDB_TABLE_NAME_LEN];
            getTableName(tableName, pThreadInfo, tableSeq);
            if (0 == strlen(tableName)) {
                errorPrint2("[%d] %s() LN%d, getTableName return null\n",
                        pThreadInfo->threadID, __func__, __LINE__);
                return NULL;
            }

            samplePos = pThreadInfo->samplePos;
            startTime = pThreadInfo->start_time
                + interlace * interlaceRows * timeStampStep;
            uint64_t remainRecPerTbl =
                    insertRows - interlaceRows * interlace;
            uint64_t recPerTbl = 0;

            uint64_t remainPerInterlace;
            if (remainRecPerTbl > interlaceRows) {
                remainPerInterlace = interlaceRows;
            } else {
                remainPerInterlace = remainRecPerTbl;
            }

            while(remainPerInterlace > 0) {

                uint32_t batch;
                if (remainPerInterlace > precalcBatch) {
                    batch = precalcBatch;
                } else {
                    batch = remainPerInterlace;
                }
                debugPrint("[%d] %s() LN%d, tableName:%s, batch:%d startTime:%"PRId64"\n",
                        pThreadInfo->threadID,
                        __func__, __LINE__,
                        tableName, batch, startTime);

                if (stbInfo) {
                    generated = prepareStbStmt(
                            pThreadInfo,
                            tableName,
                            tableSeq,
                            batch,
                            insertRows, 0,
                            startTime,
                            &samplePos);
                } else {
                    generated = prepareStmtWithoutStb(
                            pThreadInfo,
                            tableName,
                            batch,
                            insertRows,
                            interlaceRows * interlace + recPerTbl,
                            startTime);
                }

                debugPrint("[%d] %s() LN%d, generated records is %"PRId64"\n",
                        pThreadInfo->threadID, __func__, __LINE__, generated);
                if (generated < 0) {
                    errorPrint2("[%d] %s() LN%d, generated records is %"PRId64"\n",
                            pThreadInfo->threadID, __func__, __LINE__, generated);
                    goto free_of_interlace_stmt;
                } else if (generated == 0) {
                    break;
                }

                recPerTbl += generated;
                remainPerInterlace -= generated;
                pThreadInfo->totalInsertRows += generated;

                verbosePrint("[%d] %s() LN%d totalInsertRows=%"PRIu64"\n",
                        pThreadInfo->threadID, __func__, __LINE__,
                        pThreadInfo->totalInsertRows);

                startTs = taosGetTimestampUs();

                int64_t affectedRows = execInsert(pThreadInfo, generated);

                endTs = taosGetTimestampUs();
                uint64_t delay = endTs - startTs;
                performancePrint("%s() LN%d, insert execution time is %10.2f ms\n",
                        __func__, __LINE__, delay / 1000.0);
                verbosePrint("[%d] %s() LN%d affectedRows=%"PRId64"\n",
                        pThreadInfo->threadID,
                        __func__, __LINE__, affectedRows);

                if (delay > pThreadInfo->maxDelay) pThreadInfo->maxDelay = delay;
                if (delay < pThreadInfo->minDelay) pThreadInfo->minDelay = delay;
                pThreadInfo->cntDelay++;
                pThreadInfo->totalDelay += delay;

                if (generated != affectedRows) {
                    errorPrint2("[%d] %s() LN%d execInsert() insert %"PRId64", affected rows: %"PRId64"\n\n",
                            pThreadInfo->threadID, __func__, __LINE__,
                            generated, affectedRows);
                    goto free_of_interlace_stmt;
                }

                pThreadInfo->totalAffectedRows += affectedRows;

                int currentPercent = pThreadInfo->totalAffectedRows * 100 / totalRows;
                if (currentPercent > percentComplete ) {
                    printf("[%d]:%d%%\n", pThreadInfo->threadID, currentPercent);
                    percentComplete = currentPercent;
                }
                int64_t  currentPrintTime = taosGetTimestampMs();
                if (currentPrintTime - lastPrintTime > 30*1000) {
                    printf("thread[%d] has currently inserted rows: %"PRIu64 ", affected rows: %"PRIu64 "\n",
                            pThreadInfo->threadID,
                            pThreadInfo->totalInsertRows,
                            pThreadInfo->totalAffectedRows);
                    lastPrintTime = currentPrintTime;
                }

                startTime += (generated * timeStampStep);
            }
        }
        pThreadInfo->samplePos = samplePos;

        if (tableSeq == pThreadInfo->start_table_from
                + pThreadInfo->ntables) {
            // turn to first table
            tableSeq = pThreadInfo->start_table_from;

            flagSleep = true;
        }

        if ((insert_interval) && flagSleep) {
            et = taosGetTimestampMs();

            if (insert_interval > (et - st) ) {
                uint64_t sleepTime = insert_interval - (et -st);
                performancePrint("%s() LN%d sleep: %"PRId64" ms for insert interval\n",
                        __func__, __LINE__, sleepTime);
                taosMsleep(sleepTime); // ms
                sleepTimeTotal += insert_interval;
            }
        }
    }
    if (percentComplete < 100)
        printf("[%d]:%d%%\n", pThreadInfo->threadID, percentComplete);

free_of_interlace_stmt:
    printStatPerThread(pThreadInfo);
    return NULL;
}
#else
// stmt sync write interlace data
static void* syncWriteInterlaceStmt(threadInfo *pThreadInfo, uint32_t interlaceRows) {
    debugPrint("[%d] %s() LN%d: ### stmt interlace write\n",
            pThreadInfo->threadID, __func__, __LINE__);

    int64_t insertRows;
    uint64_t maxSqlLen;
    int64_t timeStampStep;
    uint64_t insert_interval;

    SSuperTable* stbInfo = pThreadInfo->stbInfo;

    if (stbInfo) {
        insertRows = stbInfo->insertRows;
        maxSqlLen = stbInfo->maxSqlLen;
        timeStampStep = stbInfo->timeStampStep;
        insert_interval = stbInfo->insertInterval;
    } else {
        insertRows = g_args.insertRows;
        maxSqlLen = g_args.max_sql_len;
        timeStampStep = g_args.timestamp_step;
        insert_interval = g_args.insert_interval;
    }

    debugPrint("[%d] %s() LN%d: start_table_from=%"PRIu64" ntables=%"PRId64" insertRows=%"PRIu64"\n",
            pThreadInfo->threadID, __func__, __LINE__,
            pThreadInfo->start_table_from,
            pThreadInfo->ntables, insertRows);

    uint32_t batchPerTbl = interlaceRows;
    uint32_t batchPerTblTimes;

    if (interlaceRows > g_args.reqPerReq)
        interlaceRows = g_args.reqPerReq;

    if ((interlaceRows > 0) && (pThreadInfo->ntables > 1)) {
        batchPerTblTimes =
            g_args.reqPerReq / interlaceRows;
    } else {
        batchPerTblTimes = 1;
    }

    pThreadInfo->totalInsertRows = 0;
    pThreadInfo->totalAffectedRows = 0;

    uint64_t st = 0;
    uint64_t et = UINT64_MAX;

    uint64_t lastPrintTime = taosGetTimestampMs();
    uint64_t startTs = taosGetTimestampMs();
    uint64_t endTs;

    uint64_t tableSeq = pThreadInfo->start_table_from;
    int64_t startTime = pThreadInfo->start_time;

    uint64_t generatedRecPerTbl = 0;
    bool flagSleep = true;
    uint64_t sleepTimeTotal = 0;

    int percentComplete = 0;
    int64_t totalRows = insertRows * pThreadInfo->ntables;

    while(pThreadInfo->totalInsertRows < pThreadInfo->ntables * insertRows) {
        if ((flagSleep) && (insert_interval)) {
            st = taosGetTimestampMs();
            flagSleep = false;
        }

        uint32_t recOfBatch = 0;

        int32_t generated;
        for (uint64_t i = 0; i < batchPerTblTimes; i ++) {
            char tableName[TSDB_TABLE_NAME_LEN];

            getTableName(tableName, pThreadInfo, tableSeq);
            if (0 == strlen(tableName)) {
                errorPrint2("[%d] %s() LN%d, getTableName return null\n",
                        pThreadInfo->threadID, __func__, __LINE__);
                return NULL;
            }

            debugPrint("[%d] %s() LN%d, tableName:%s, batch:%d startTime:%"PRId64"\n",
                    pThreadInfo->threadID,
                    __func__, __LINE__,
                    tableName, batchPerTbl, startTime);
            if (stbInfo) {
                generated = prepareStbStmt(
                        pThreadInfo,
                        tableName,
                        tableSeq,
                        batchPerTbl,
                        insertRows, 0,
                        startTime,
                        &(pThreadInfo->samplePos));
            } else {
                generated = prepareStmtWithoutStb(
                        pThreadInfo,
                        tableName,
                        batchPerTbl,
                        insertRows, i,
                        startTime);
            }

            debugPrint("[%d] %s() LN%d, generated records is %d\n",
                    pThreadInfo->threadID, __func__, __LINE__, generated);
            if (generated < 0) {
                errorPrint2("[%d] %s() LN%d, generated records is %d\n",
                        pThreadInfo->threadID, __func__, __LINE__, generated);
                goto free_of_interlace_stmt;
            } else if (generated == 0) {
                break;
            }

            tableSeq ++;
            recOfBatch += batchPerTbl;

            pThreadInfo->totalInsertRows += batchPerTbl;

            verbosePrint("[%d] %s() LN%d batchPerTbl=%d recOfBatch=%d\n",
                    pThreadInfo->threadID, __func__, __LINE__,
                    batchPerTbl, recOfBatch);

            if (tableSeq == pThreadInfo->start_table_from + pThreadInfo->ntables) {
                // turn to first table
                tableSeq = pThreadInfo->start_table_from;
                generatedRecPerTbl += batchPerTbl;

                startTime = pThreadInfo->start_time
                    + generatedRecPerTbl * timeStampStep;

                flagSleep = true;
                if (generatedRecPerTbl >= insertRows)
                    break;

                int64_t remainRows = insertRows - generatedRecPerTbl;
                if ((remainRows > 0) && (batchPerTbl > remainRows))
                    batchPerTbl = remainRows;

                if (pThreadInfo->ntables * batchPerTbl < g_args.reqPerReq)
                    break;
            }

            verbosePrint("[%d] %s() LN%d generatedRecPerTbl=%"PRId64" insertRows=%"PRId64"\n",
                    pThreadInfo->threadID, __func__, __LINE__,
                    generatedRecPerTbl, insertRows);

            if ((g_args.reqPerReq - recOfBatch) < batchPerTbl)
                break;
        }

        verbosePrint("[%d] %s() LN%d recOfBatch=%d totalInsertRows=%"PRIu64"\n",
                pThreadInfo->threadID, __func__, __LINE__, recOfBatch,
                pThreadInfo->totalInsertRows);

        startTs = taosGetTimestampUs();

        if (recOfBatch == 0) {
            errorPrint2("[%d] %s() LN%d Failed to insert records of batch %d\n",
                    pThreadInfo->threadID, __func__, __LINE__,
                    batchPerTbl);
            if (batchPerTbl > 0) {
                errorPrint("\tIf the batch is %d, the length of the SQL to insert a row must be less then %"PRId64"\n",
                        batchPerTbl, maxSqlLen / batchPerTbl);
            }
            goto free_of_interlace_stmt;
        }
        int64_t affectedRows = execInsert(pThreadInfo, recOfBatch);

        endTs = taosGetTimestampUs();
        uint64_t delay = endTs - startTs;
        performancePrint("%s() LN%d, insert execution time is %10.2f ms\n",
                __func__, __LINE__, delay / 1000.0);
        verbosePrint("[%d] %s() LN%d affectedRows=%"PRId64"\n",
                pThreadInfo->threadID,
                __func__, __LINE__, affectedRows);

        if (delay > pThreadInfo->maxDelay) pThreadInfo->maxDelay = delay;
        if (delay < pThreadInfo->minDelay) pThreadInfo->minDelay = delay;
        pThreadInfo->cntDelay++;
        pThreadInfo->totalDelay += delay;

        if (recOfBatch != affectedRows) {
            errorPrint2("[%d] %s() LN%d execInsert insert %d, affected rows: %"PRId64"\n\n",
                    pThreadInfo->threadID, __func__, __LINE__,
                    recOfBatch, affectedRows);
            goto free_of_interlace_stmt;
        }

        pThreadInfo->totalAffectedRows += affectedRows;

        int currentPercent = pThreadInfo->totalAffectedRows * 100 / totalRows;
        if (currentPercent > percentComplete ) {
            printf("[%d]:%d%%\n", pThreadInfo->threadID, currentPercent);
            percentComplete = currentPercent;
        }
        int64_t  currentPrintTime = taosGetTimestampMs();
        if (currentPrintTime - lastPrintTime > 30*1000) {
            printf("thread[%d] has currently inserted rows: %"PRIu64 ", affected rows: %"PRIu64 "\n",
                    pThreadInfo->threadID,
                    pThreadInfo->totalInsertRows,
                    pThreadInfo->totalAffectedRows);
            lastPrintTime = currentPrintTime;
        }

        if ((insert_interval) && flagSleep) {
            et = taosGetTimestampMs();

            if (insert_interval > (et - st) ) {
                uint64_t sleepTime = insert_interval - (et -st);
                performancePrint("%s() LN%d sleep: %"PRId64" ms for insert interval\n",
                        __func__, __LINE__, sleepTime);
                taosMsleep(sleepTime); // ms
                sleepTimeTotal += insert_interval;
            }
        }
    }
    if (percentComplete < 100)
        printf("[%d]:%d%%\n", pThreadInfo->threadID, percentComplete);

free_of_interlace_stmt:
    printStatPerThread(pThreadInfo);
    return NULL;
}

#endif

// sync write interlace data
static void* syncWriteInterlace(threadInfo *pThreadInfo, uint32_t interlaceRows) {
    debugPrint("[%d] %s() LN%d: ### interlace write\n",
            pThreadInfo->threadID, __func__, __LINE__);

    int64_t insertRows;
    uint64_t maxSqlLen;
    int64_t timeStampStep;
    uint64_t insert_interval;

    SSuperTable* stbInfo = pThreadInfo->stbInfo;

    if (stbInfo) {
        insertRows = stbInfo->insertRows;
        maxSqlLen = stbInfo->maxSqlLen;
        timeStampStep = stbInfo->timeStampStep;
        insert_interval = stbInfo->insertInterval;
    } else {
        insertRows = g_args.insertRows;
        maxSqlLen = g_args.max_sql_len;
        timeStampStep = g_args.timestamp_step;
        insert_interval = g_args.insert_interval;
    }

    debugPrint("[%d] %s() LN%d: start_table_from=%"PRIu64" ntables=%"PRId64" insertRows=%"PRIu64"\n",
            pThreadInfo->threadID, __func__, __LINE__,
            pThreadInfo->start_table_from,
            pThreadInfo->ntables, insertRows);
#if 1
    if (interlaceRows > g_args.reqPerReq)
        interlaceRows = g_args.reqPerReq;

    uint32_t batchPerTbl = interlaceRows;
    uint32_t batchPerTblTimes;

    if ((interlaceRows > 0) && (pThreadInfo->ntables > 1)) {
        batchPerTblTimes =
            g_args.reqPerReq / interlaceRows;
    } else {
        batchPerTblTimes = 1;
    }
#else
    uint32_t batchPerTbl;
    if (interlaceRows > g_args.reqPerReq)
        batchPerTbl = g_args.reqPerReq;
    else
        batchPerTbl = interlaceRows;

    uint32_t batchPerTblTimes;

    if ((interlaceRows > 0) && (pThreadInfo->ntables > 1)) {
        batchPerTblTimes =
            interlaceRows / batchPerTbl;
    } else {
        batchPerTblTimes = 1;
    }
#endif
    pThreadInfo->buffer = calloc(maxSqlLen, 1);
    if (NULL == pThreadInfo->buffer) {
        errorPrint2( "%s() LN%d, Failed to alloc %"PRIu64" Bytes, reason:%s\n",
                __func__, __LINE__, maxSqlLen, strerror(errno));
        return NULL;
    }

    pThreadInfo->totalInsertRows = 0;
    pThreadInfo->totalAffectedRows = 0;

    uint64_t st = 0;
    uint64_t et = UINT64_MAX;

    uint64_t lastPrintTime = taosGetTimestampMs();
    uint64_t startTs = taosGetTimestampMs();
    uint64_t endTs;

    uint64_t tableSeq = pThreadInfo->start_table_from;
    int64_t startTime = pThreadInfo->start_time;

    uint64_t generatedRecPerTbl = 0;
    bool flagSleep = true;
    uint64_t sleepTimeTotal = 0;

    int percentComplete = 0;
    int64_t totalRows = insertRows * pThreadInfo->ntables;

    while(pThreadInfo->totalInsertRows < pThreadInfo->ntables * insertRows) {
        if ((flagSleep) && (insert_interval)) {
            st = taosGetTimestampMs();
            flagSleep = false;
        }

        // generate data
        memset(pThreadInfo->buffer, 0, maxSqlLen);
        uint64_t remainderBufLen = maxSqlLen;

        char *pstr = pThreadInfo->buffer;

        int len = snprintf(pstr,
                strlen(STR_INSERT_INTO) + 1, "%s", STR_INSERT_INTO);
        pstr += len;
        remainderBufLen -= len;

        uint32_t recOfBatch = 0;

        int32_t generated;
        for (uint64_t i = 0; i < batchPerTblTimes; i ++) {
            char tableName[TSDB_TABLE_NAME_LEN];

            getTableName(tableName, pThreadInfo, tableSeq);
            if (0 == strlen(tableName)) {
                errorPrint2("[%d] %s() LN%d, getTableName return null\n",
                        pThreadInfo->threadID, __func__, __LINE__);
                free(pThreadInfo->buffer);
                return NULL;
            }

            uint64_t oldRemainderLen = remainderBufLen;

            if (stbInfo) {
                generated = generateStbInterlaceData(
                        pThreadInfo,
                        tableName, batchPerTbl, i,
                        batchPerTblTimes,
                        tableSeq,
                        pstr,
                        insertRows,
                        startTime,
                        &remainderBufLen);
            } else {
                generated = generateInterlaceDataWithoutStb(
                        tableName, batchPerTbl,
                        tableSeq,
                        pThreadInfo->db_name, pstr,
                        insertRows,
                        startTime,
                        &remainderBufLen);
            }

            debugPrint("[%d] %s() LN%d, generated records is %d\n",
                    pThreadInfo->threadID, __func__, __LINE__, generated);
            if (generated < 0) {
                errorPrint2("[%d] %s() LN%d, generated records is %d\n",
                        pThreadInfo->threadID, __func__, __LINE__, generated);
                goto free_of_interlace;
            } else if (generated == 0) {
                break;
            }

            tableSeq ++;
            recOfBatch += batchPerTbl;

            pstr += (oldRemainderLen - remainderBufLen);
            pThreadInfo->totalInsertRows += batchPerTbl;

            verbosePrint("[%d] %s() LN%d batchPerTbl=%d recOfBatch=%d\n",
                    pThreadInfo->threadID, __func__, __LINE__,
                    batchPerTbl, recOfBatch);

            if (tableSeq == pThreadInfo->start_table_from + pThreadInfo->ntables) {
                // turn to first table
                tableSeq = pThreadInfo->start_table_from;
                generatedRecPerTbl += batchPerTbl;

                startTime = pThreadInfo->start_time
                    + generatedRecPerTbl * timeStampStep;

                flagSleep = true;
                if (generatedRecPerTbl >= insertRows)
                    break;

                int64_t remainRows = insertRows - generatedRecPerTbl;
                if ((remainRows > 0) && (batchPerTbl > remainRows))
                    batchPerTbl = remainRows;

                if (pThreadInfo->ntables * batchPerTbl < g_args.reqPerReq)
                    break;
            }

            verbosePrint("[%d] %s() LN%d generatedRecPerTbl=%"PRId64" insertRows=%"PRId64"\n",
                    pThreadInfo->threadID, __func__, __LINE__,
                    generatedRecPerTbl, insertRows);

            if ((g_args.reqPerReq - recOfBatch) < batchPerTbl)
                break;
        }

        verbosePrint("[%d] %s() LN%d recOfBatch=%d totalInsertRows=%"PRIu64"\n",
                pThreadInfo->threadID, __func__, __LINE__, recOfBatch,
                pThreadInfo->totalInsertRows);
        verbosePrint("[%d] %s() LN%d, buffer=%s\n",
                pThreadInfo->threadID, __func__, __LINE__, pThreadInfo->buffer);

        startTs = taosGetTimestampUs();

        if (recOfBatch == 0) {
            errorPrint2("[%d] %s() LN%d Failed to insert records of batch %d\n",
                    pThreadInfo->threadID, __func__, __LINE__,
                    batchPerTbl);
            if (batchPerTbl > 0) {
                errorPrint("\tIf the batch is %d, the length of the SQL to insert a row must be less then %"PRId64"\n",
                        batchPerTbl, maxSqlLen / batchPerTbl);
            }
            errorPrint("\tPlease check if the buffer length(%"PRId64") or batch(%d) is set with proper value!\n",
                    maxSqlLen, batchPerTbl);
            goto free_of_interlace;
        }
        int64_t affectedRows = execInsert(pThreadInfo, recOfBatch);

        endTs = taosGetTimestampUs();
        uint64_t delay = endTs - startTs;
        performancePrint("%s() LN%d, insert execution time is %10.2f ms\n",
                __func__, __LINE__, delay / 1000.0);
        verbosePrint("[%d] %s() LN%d affectedRows=%"PRId64"\n",
                pThreadInfo->threadID,
                __func__, __LINE__, affectedRows);

        if (delay > pThreadInfo->maxDelay) pThreadInfo->maxDelay = delay;
        if (delay < pThreadInfo->minDelay) pThreadInfo->minDelay = delay;
        pThreadInfo->cntDelay++;
        pThreadInfo->totalDelay += delay;

        if (recOfBatch != affectedRows) {
            errorPrint2("[%d] %s() LN%d execInsert insert %d, affected rows: %"PRId64"\n%s\n",
                    pThreadInfo->threadID, __func__, __LINE__,
                    recOfBatch, affectedRows, pThreadInfo->buffer);
            goto free_of_interlace;
        }

        pThreadInfo->totalAffectedRows += affectedRows;

        int currentPercent = pThreadInfo->totalAffectedRows * 100 / totalRows;
        if (currentPercent > percentComplete ) {
            printf("[%d]:%d%%\n", pThreadInfo->threadID, currentPercent);
            percentComplete = currentPercent;
        }
        int64_t  currentPrintTime = taosGetTimestampMs();
        if (currentPrintTime - lastPrintTime > 30*1000) {
            printf("thread[%d] has currently inserted rows: %"PRIu64 ", affected rows: %"PRIu64 "\n",
                    pThreadInfo->threadID,
                    pThreadInfo->totalInsertRows,
                    pThreadInfo->totalAffectedRows);
            lastPrintTime = currentPrintTime;
        }

        if ((insert_interval) && flagSleep) {
            et = taosGetTimestampMs();

            if (insert_interval > (et - st) ) {
                uint64_t sleepTime = insert_interval - (et -st);
                performancePrint("%s() LN%d sleep: %"PRId64" ms for insert interval\n",
                        __func__, __LINE__, sleepTime);
                taosMsleep(sleepTime); // ms
                sleepTimeTotal += insert_interval;
            }
        }
    }
    if (percentComplete < 100)
        printf("[%d]:%d%%\n", pThreadInfo->threadID, percentComplete);

free_of_interlace:
    tmfree(pThreadInfo->buffer);
    printStatPerThread(pThreadInfo);
    return NULL;
}

static void* syncWriteProgressiveStmt(threadInfo *pThreadInfo) {
    debugPrint("%s() LN%d: ### stmt progressive write\n", __func__, __LINE__);

    SSuperTable* stbInfo = pThreadInfo->stbInfo;
    int64_t timeStampStep =
        stbInfo?stbInfo->timeStampStep:g_args.timestamp_step;
    int64_t insertRows =
        (stbInfo)?stbInfo->insertRows:g_args.insertRows;
    verbosePrint("%s() LN%d insertRows=%"PRId64"\n",
            __func__, __LINE__, insertRows);

    uint64_t lastPrintTime = taosGetTimestampMs();
    uint64_t startTs = taosGetTimestampMs();
    uint64_t endTs;

    pThreadInfo->totalInsertRows = 0;
    pThreadInfo->totalAffectedRows = 0;

    pThreadInfo->samplePos = 0;

    int percentComplete = 0;
    int64_t totalRows = insertRows * pThreadInfo->ntables;

    for (uint64_t tableSeq = pThreadInfo->start_table_from;
            tableSeq <= pThreadInfo->end_table_to;
            tableSeq ++) {
        int64_t start_time = pThreadInfo->start_time;

        for (uint64_t i = 0; i < insertRows;) {
            char tableName[TSDB_TABLE_NAME_LEN];
            getTableName(tableName, pThreadInfo, tableSeq);
            verbosePrint("%s() LN%d: tid=%d seq=%"PRId64" tableName=%s\n",
                    __func__, __LINE__,
                    pThreadInfo->threadID, tableSeq, tableName);
            if (0 == strlen(tableName)) {
                errorPrint2("[%d] %s() LN%d, getTableName return null\n",
                        pThreadInfo->threadID, __func__, __LINE__);
                return NULL;
            }

            // measure prepare + insert
            startTs = taosGetTimestampUs();

            int32_t generated;
            if (stbInfo) {
                generated = prepareStbStmt(
                        pThreadInfo,
                        tableName,
                        tableSeq,
                        (g_args.reqPerReq>stbInfo->insertRows)?
                        stbInfo->insertRows:
                        g_args.reqPerReq,
                        insertRows, i, start_time,
                        &(pThreadInfo->samplePos));
            } else {
                generated = prepareStmtWithoutStb(
                        pThreadInfo,
                        tableName,
                        g_args.reqPerReq,
                        insertRows, i,
                        start_time);
            }

            verbosePrint("[%d] %s() LN%d generated=%d\n",
                    pThreadInfo->threadID,
                    __func__, __LINE__, generated);

            if (generated > 0)
                i += generated;
            else
                goto free_of_stmt_progressive;

            start_time +=  generated * timeStampStep;
            pThreadInfo->totalInsertRows += generated;

            // only measure insert
            // startTs = taosGetTimestampUs();

            int32_t affectedRows = execInsert(pThreadInfo, generated);

            endTs = taosGetTimestampUs();
            uint64_t delay = endTs - startTs;
            performancePrint("%s() LN%d, insert execution time is %10.f ms\n",
                    __func__, __LINE__, delay/1000.0);
            verbosePrint("[%d] %s() LN%d affectedRows=%d\n",
                    pThreadInfo->threadID,
                    __func__, __LINE__, affectedRows);

            if (delay > pThreadInfo->maxDelay) pThreadInfo->maxDelay = delay;
            if (delay < pThreadInfo->minDelay) pThreadInfo->minDelay = delay;
            pThreadInfo->cntDelay++;
            pThreadInfo->totalDelay += delay;

            if (affectedRows < 0) {
                errorPrint2("%s() LN%d, affected rows: %d\n",
                        __func__, __LINE__, affectedRows);
                goto free_of_stmt_progressive;
            }

            pThreadInfo->totalAffectedRows += affectedRows;

            int currentPercent = pThreadInfo->totalAffectedRows * 100 / totalRows;
            if (currentPercent > percentComplete ) {
                printf("[%d]:%d%%\n", pThreadInfo->threadID, currentPercent);
                percentComplete = currentPercent;
            }
            int64_t  currentPrintTime = taosGetTimestampMs();
            if (currentPrintTime - lastPrintTime > 30*1000) {
                printf("thread[%d] has currently inserted rows: %"PRId64 ", affected rows: %"PRId64 "\n",
                        pThreadInfo->threadID,
                        pThreadInfo->totalInsertRows,
                        pThreadInfo->totalAffectedRows);
                lastPrintTime = currentPrintTime;
            }

            if (i >= insertRows)
                break;
        }   // insertRows

        if ((g_args.verbose_print) &&
                (tableSeq == pThreadInfo->ntables - 1) && (stbInfo)
                && (0 == strncasecmp(
                        stbInfo->dataSource,
                        "sample", strlen("sample")))) {
            verbosePrint("%s() LN%d samplePos=%"PRId64"\n",
                    __func__, __LINE__, pThreadInfo->samplePos);
        }
    } // tableSeq

    if (percentComplete < 100) {
        printf("[%d]:%d%%\n", pThreadInfo->threadID, percentComplete);
    }

free_of_stmt_progressive:
    tmfree(pThreadInfo->buffer);
    printStatPerThread(pThreadInfo);
    return NULL;
}
// sync insertion progressive data
static void* syncWriteProgressive(threadInfo *pThreadInfo) {
    debugPrint("%s() LN%d: ### progressive write\n", __func__, __LINE__);

    SSuperTable* stbInfo = pThreadInfo->stbInfo;
    uint64_t maxSqlLen = stbInfo?stbInfo->maxSqlLen:g_args.max_sql_len;
    int64_t timeStampStep =
        stbInfo?stbInfo->timeStampStep:g_args.timestamp_step;
    int64_t insertRows =
        (stbInfo)?stbInfo->insertRows:g_args.insertRows;
    verbosePrint("%s() LN%d insertRows=%"PRId64"\n",
            __func__, __LINE__, insertRows);

    pThreadInfo->buffer = calloc(maxSqlLen, 1);
    if (NULL == pThreadInfo->buffer) {
        errorPrint2("Failed to alloc %"PRIu64" bytes, reason:%s\n",
                maxSqlLen,
                strerror(errno));
        return NULL;
    }

    uint64_t lastPrintTime = taosGetTimestampMs();
    uint64_t startTs = taosGetTimestampMs();
    uint64_t endTs;

    pThreadInfo->totalInsertRows = 0;
    pThreadInfo->totalAffectedRows = 0;

    pThreadInfo->samplePos = 0;

    int percentComplete = 0;
    int64_t totalRows = insertRows * pThreadInfo->ntables;

    for (uint64_t tableSeq = pThreadInfo->start_table_from;
            tableSeq <= pThreadInfo->end_table_to;
            tableSeq ++) {
        int64_t start_time = pThreadInfo->start_time;

        for (uint64_t i = 0; i < insertRows;) {
            char tableName[TSDB_TABLE_NAME_LEN];
            getTableName(tableName, pThreadInfo, tableSeq);
            verbosePrint("%s() LN%d: tid=%d seq=%"PRId64" tableName=%s\n",
                    __func__, __LINE__,
                    pThreadInfo->threadID, tableSeq, tableName);
            if (0 == strlen(tableName)) {
                errorPrint2("[%d] %s() LN%d, getTableName return null\n",
                        pThreadInfo->threadID, __func__, __LINE__);
                free(pThreadInfo->buffer);
                return NULL;
            }

            int64_t remainderBufLen = maxSqlLen - 2000;
            char *pstr = pThreadInfo->buffer;

            int len = snprintf(pstr,
                    strlen(STR_INSERT_INTO) + 1, "%s", STR_INSERT_INTO);

            pstr += len;
            remainderBufLen -= len;

            // measure prepare + insert
            startTs = taosGetTimestampUs();

            int32_t generated;
            if (stbInfo) {
                if (stbInfo->iface == STMT_IFACE) {
                    generated = prepareStbStmt(
                            pThreadInfo,
                            tableName,
                            tableSeq,
                            (g_args.reqPerReq>stbInfo->insertRows)?
                                stbInfo->insertRows:
                                g_args.reqPerReq,
                            insertRows, i, start_time,
                            &(pThreadInfo->samplePos));
                } else {
                    generated = generateStbProgressiveData(
                            stbInfo,
                            tableName, tableSeq,
                            pThreadInfo->db_name, pstr,
                            insertRows, i, start_time,
                            &(pThreadInfo->samplePos),
                            &remainderBufLen);
                }
            } else {
                if (g_args.iface == STMT_IFACE) {
                    generated = prepareStmtWithoutStb(
                            pThreadInfo,
                            tableName,
                            g_args.reqPerReq,
                            insertRows, i,
                            start_time);
                } else {
                    generated = generateProgressiveDataWithoutStb(
                            tableName,
                            /*  tableSeq, */
                            pThreadInfo, pstr, insertRows,
                            i, start_time,
                            /* &(pThreadInfo->samplePos), */
                            &remainderBufLen);
                }
            }

            verbosePrint("[%d] %s() LN%d generated=%d\n",
                    pThreadInfo->threadID,
                    __func__, __LINE__, generated);

            if (generated > 0)
                i += generated;
            else
                goto free_of_progressive;

            start_time +=  generated * timeStampStep;
            pThreadInfo->totalInsertRows += generated;

            // only measure insert
            // startTs = taosGetTimestampUs();

            int32_t affectedRows = execInsert(pThreadInfo, generated);

            endTs = taosGetTimestampUs();
            uint64_t delay = endTs - startTs;
            performancePrint("%s() LN%d, insert execution time is %10.f ms\n",
                    __func__, __LINE__, delay/1000.0);
            verbosePrint("[%d] %s() LN%d affectedRows=%d\n",
                    pThreadInfo->threadID,
                    __func__, __LINE__, affectedRows);

            if (delay > pThreadInfo->maxDelay) pThreadInfo->maxDelay = delay;
            if (delay < pThreadInfo->minDelay) pThreadInfo->minDelay = delay;
            pThreadInfo->cntDelay++;
            pThreadInfo->totalDelay += delay;

            if (affectedRows < 0) {
                errorPrint2("%s() LN%d, affected rows: %d\n",
                        __func__, __LINE__, affectedRows);
                goto free_of_progressive;
            }

            pThreadInfo->totalAffectedRows += affectedRows;

            int currentPercent = pThreadInfo->totalAffectedRows * 100 / totalRows;
            if (currentPercent > percentComplete ) {
                printf("[%d]:%d%%\n", pThreadInfo->threadID, currentPercent);
                percentComplete = currentPercent;
            }
            int64_t  currentPrintTime = taosGetTimestampMs();
            if (currentPrintTime - lastPrintTime > 30*1000) {
                printf("thread[%d] has currently inserted rows: %"PRId64 ", affected rows: %"PRId64 "\n",
                        pThreadInfo->threadID,
                        pThreadInfo->totalInsertRows,
                        pThreadInfo->totalAffectedRows);
                lastPrintTime = currentPrintTime;
            }

            if (i >= insertRows)
                break;
        }   // insertRows

        if ((g_args.verbose_print) &&
                (tableSeq == pThreadInfo->ntables - 1) && (stbInfo)
                && (0 == strncasecmp(
                        stbInfo->dataSource,
                        "sample", strlen("sample")))) {
            verbosePrint("%s() LN%d samplePos=%"PRId64"\n",
                    __func__, __LINE__, pThreadInfo->samplePos);
        }
    } // tableSeq

    if (percentComplete < 100) {
        printf("[%d]:%d%%\n", pThreadInfo->threadID, percentComplete);
    }

free_of_progressive:
    tmfree(pThreadInfo->buffer);
    printStatPerThread(pThreadInfo);
    return NULL;
}

static void* syncWrite(void *sarg) {

    threadInfo *pThreadInfo = (threadInfo *)sarg;
    SSuperTable* stbInfo = pThreadInfo->stbInfo;

    setThreadName("syncWrite");

    uint32_t interlaceRows = 0;

    if (stbInfo) {
        if (stbInfo->interlaceRows < stbInfo->insertRows)
            interlaceRows = stbInfo->interlaceRows;
    } else {
        if (g_args.interlaceRows < g_args.insertRows)
            interlaceRows = g_args.interlaceRows;
    }

    if (interlaceRows > 0) {
        // interlace mode
        if (stbInfo) {
            if (STMT_IFACE == stbInfo->iface) {
#if STMT_BIND_PARAM_BATCH == 1
                return syncWriteInterlaceStmtBatch(pThreadInfo, interlaceRows);
#else
                return syncWriteInterlaceStmt(pThreadInfo, interlaceRows);
#endif
            } else {
                return syncWriteInterlace(pThreadInfo, interlaceRows);
            }
        }
    } else {
      // progressive mode
      if (((stbInfo) && (STMT_IFACE == stbInfo->iface))
              || (STMT_IFACE == g_args.iface)) {
          return syncWriteProgressiveStmt(pThreadInfo);
      } else {
          return syncWriteProgressive(pThreadInfo);
      }
    }

    return NULL;
}

static void callBack(void *param, TAOS_RES *res, int code) {
    threadInfo* pThreadInfo = (threadInfo*)param;
    SSuperTable* stbInfo = pThreadInfo->stbInfo;

    int insert_interval =
        stbInfo?stbInfo->insertInterval:g_args.insert_interval;
    if (insert_interval) {
        pThreadInfo->et = taosGetTimestampMs();
        if ((pThreadInfo->et - pThreadInfo->st) < insert_interval) {
            taosMsleep(insert_interval - (pThreadInfo->et - pThreadInfo->st)); // ms
        }
    }

    char *buffer = calloc(1, pThreadInfo->stbInfo->maxSqlLen);
    char data[MAX_DATA_SIZE];
    char *pstr = buffer;
    pstr += sprintf(pstr, "INSERT INTO %s.%s%"PRId64" VALUES",
            pThreadInfo->db_name, pThreadInfo->tb_prefix,
            pThreadInfo->start_table_from);
    //  if (pThreadInfo->counter >= pThreadInfo->stbInfo->insertRows) {
    if (pThreadInfo->counter >= g_args.reqPerReq) {
        pThreadInfo->start_table_from++;
        pThreadInfo->counter = 0;
    }
    if (pThreadInfo->start_table_from > pThreadInfo->end_table_to) {
        tsem_post(&pThreadInfo->lock_sem);
        free(buffer);
        taos_free_result(res);
        return;
    }

    for (int i = 0; i < g_args.reqPerReq; i++) {
        int rand_num = taosRandom() % 100;
        if (0 != pThreadInfo->stbInfo->disorderRatio
                && rand_num < pThreadInfo->stbInfo->disorderRatio) {
            int64_t d = pThreadInfo->lastTs
                - (taosRandom() % pThreadInfo->stbInfo->disorderRange + 1);
            generateStbRowData(pThreadInfo->stbInfo, data,
                    MAX_DATA_SIZE,
                    d);
        } else {
            generateStbRowData(pThreadInfo->stbInfo,
                    data,
                    MAX_DATA_SIZE,
                    pThreadInfo->lastTs += 1000);
        }
        pstr += sprintf(pstr, "%s", data);
        pThreadInfo->counter++;

        if (pThreadInfo->counter >= pThreadInfo->stbInfo->insertRows) {
            break;
        }
    }

    if (insert_interval) {
        pThreadInfo->st = taosGetTimestampMs();
    }
    taos_query_a(pThreadInfo->taos, buffer, callBack, pThreadInfo);
    free(buffer);

    taos_free_result(res);
}

static void *asyncWrite(void *sarg) {
    threadInfo *pThreadInfo = (threadInfo *)sarg;
    SSuperTable* stbInfo = pThreadInfo->stbInfo;

    setThreadName("asyncWrite");

    pThreadInfo->st = 0;
    pThreadInfo->et = 0;
    pThreadInfo->lastTs = pThreadInfo->start_time;

    int insert_interval =
        stbInfo?stbInfo->insertInterval:g_args.insert_interval;
    if (insert_interval) {
        pThreadInfo->st = taosGetTimestampMs();
    }
    taos_query_a(pThreadInfo->taos, "show databases", callBack, pThreadInfo);

    tsem_wait(&(pThreadInfo->lock_sem));

    return NULL;
}

static int convertHostToServAddr(char *host, uint16_t port, struct sockaddr_in *serv_addr)
{
    uint16_t rest_port = port + TSDB_PORT_HTTP;
    struct hostent *server = gethostbyname(host);
    if ((server == NULL) || (server->h_addr == NULL)) {
        errorPrint2("%s", "no such host");
        return -1;
    }

    debugPrint("h_name: %s\nh_addr=%p\nh_addretype: %s\nh_length: %d\n",
            server->h_name,
            server->h_addr,
            (server->h_addrtype == AF_INET)?"ipv4":"ipv6",
            server->h_length);

    memset(serv_addr, 0, sizeof(struct sockaddr_in));
    serv_addr->sin_family = AF_INET;
    serv_addr->sin_port = htons(rest_port);
#ifdef WINDOWS
    serv_addr->sin_addr.s_addr = inet_addr(host);
#else
    memcpy(&(serv_addr->sin_addr.s_addr), server->h_addr, server->h_length);
#endif
    return 0;
}

static void startMultiThreadInsertData(int threads, char* db_name,
        char* precision, SSuperTable* stbInfo) {

    int32_t timePrec = TSDB_TIME_PRECISION_MILLI;
    if (0 != precision[0]) {
        if (0 == strncasecmp(precision, "ms", 2)) {
            timePrec = TSDB_TIME_PRECISION_MILLI;
        } else if (0 == strncasecmp(precision, "us", 2)) {
            timePrec = TSDB_TIME_PRECISION_MICRO;
        } else if (0 == strncasecmp(precision, "ns", 2)) {
            timePrec = TSDB_TIME_PRECISION_NANO;
        } else {
            errorPrint2("Not support precision: %s\n", precision);
            exit(EXIT_FAILURE);
        }
    }

    int64_t startTime;
    if (stbInfo) {
        if (0 == strncasecmp(stbInfo->startTimestamp, "now", 3)) {
            startTime = taosGetTimestamp(timePrec);
        } else {
            if (TSDB_CODE_SUCCESS != taosParseTime(
                        stbInfo->startTimestamp,
                        &startTime,
                        strlen(stbInfo->startTimestamp),
                        timePrec, 0)) {
                ERROR_EXIT("failed to parse time!\n");
            }
        }
    } else {
        startTime = DEFAULT_START_TIME;
    }
    debugPrint("%s() LN%d, startTime= %"PRId64"\n",
            __func__, __LINE__, startTime);

    // read sample data from file first
    int ret;
    if (stbInfo) {
        ret = prepareSampleForStb(stbInfo);
    } else {
        ret = prepareSampleForNtb();
    }

    if (0 != ret) {
        errorPrint2("%s() LN%d, prepare sample data for stable failed!\n",
                __func__, __LINE__);
        exit(EXIT_FAILURE);
    }

    TAOS* taos0 = taos_connect(
            g_Dbs.host, g_Dbs.user,
            g_Dbs.password, db_name, g_Dbs.port);
    if (NULL == taos0) {
        errorPrint2("%s() LN%d, connect to server fail , reason: %s\n",
                __func__, __LINE__, taos_errstr(NULL));
        exit(EXIT_FAILURE);
    }

    int64_t ntables = 0;
    uint64_t tableFrom = 0;

    if (stbInfo) {
        int64_t limit;
        uint64_t offset;

        if ((NULL != g_args.sqlFile)
                && (stbInfo->childTblExists == TBL_NO_EXISTS)
                && ((stbInfo->childTblOffset != 0)
                    || (stbInfo->childTblLimit >= 0))) {
            printf("WARNING: offset and limit will not be used since the child tables not exists!\n");
        }

        if (stbInfo->childTblExists == TBL_ALREADY_EXISTS) {
            if ((stbInfo->childTblLimit < 0)
                    || ((stbInfo->childTblOffset
                            + stbInfo->childTblLimit)
                        > (stbInfo->childTblCount))) {

                if (stbInfo->childTblCount < stbInfo->childTblOffset) {
                    printf("WARNING: offset will not be used since the child tables count is less then offset!\n");

                    stbInfo->childTblOffset = 0;
                }
                stbInfo->childTblLimit =
                    stbInfo->childTblCount - stbInfo->childTblOffset;
            }

            offset = stbInfo->childTblOffset;
            limit = stbInfo->childTblLimit;
        } else {
            limit = stbInfo->childTblCount;
            offset = 0;
        }

        ntables = limit;
        tableFrom = offset;

        if ((stbInfo->childTblExists != TBL_NO_EXISTS)
                && ((stbInfo->childTblOffset + stbInfo->childTblLimit)
                    > stbInfo->childTblCount)) {
            printf("WARNING: specified offset + limit > child table count!\n");
            prompt();
        }

        if ((stbInfo->childTblExists != TBL_NO_EXISTS)
                && (0 == stbInfo->childTblLimit)) {
            printf("WARNING: specified limit = 0, which cannot find table name to insert or query! \n");
            prompt();
        }

        stbInfo->childTblName = (char*)calloc(1,
                limit * TSDB_TABLE_NAME_LEN);
        if (stbInfo->childTblName == NULL) {
            taos_close(taos0);
            errorPrint2("%s() LN%d, alloc memory failed!\n", __func__, __LINE__);
            exit(EXIT_FAILURE);
        }

        int64_t childTblCount;
        getChildNameOfSuperTableWithLimitAndOffset(
                taos0,
                db_name, stbInfo->stbName,
                &stbInfo->childTblName, &childTblCount,
                limit,
                offset);
        ntables = childTblCount;
    } else {
        ntables = g_args.ntables;
        tableFrom = 0;
    }

    taos_close(taos0);

    int64_t a = ntables / threads;
    if (a < 1) {
        threads = ntables;
        a = 1;
    }

    int64_t b = 0;
    if (threads != 0) {
        b = ntables % threads;
    }

    if (g_args.iface == REST_IFACE || ((stbInfo) && (stbInfo->iface == REST_IFACE))) {
        if (convertHostToServAddr(
                    g_Dbs.host, g_Dbs.port, &(g_Dbs.serv_addr)) != 0) {
            ERROR_EXIT("convert host to server address");
        }
    }

    pthread_t *pids = calloc(1, threads * sizeof(pthread_t));
    threadInfo *infos = calloc(1, threads * sizeof(threadInfo));
    assert(pids != NULL);
    assert(infos != NULL);

    char *stmtBuffer = calloc(1, BUFFER_SIZE);
    assert(stmtBuffer);

#if STMT_BIND_PARAM_BATCH == 1
    uint32_t interlaceRows = 0;
    uint32_t batch;

    if (stbInfo) {
        if (stbInfo->interlaceRows < stbInfo->insertRows)
            interlaceRows = stbInfo->interlaceRows;
    } else {
        if (g_args.interlaceRows < g_args.insertRows)
            interlaceRows = g_args.interlaceRows;
    }

    if (interlaceRows > 0) {
        batch = interlaceRows;
    } else {
        batch = (g_args.reqPerReq>g_args.insertRows)?
            g_args.insertRows:g_args.reqPerReq;
    }

#endif

    if ((g_args.iface == STMT_IFACE)
            || ((stbInfo)
                && (stbInfo->iface == STMT_IFACE))) {
        char *pstr = stmtBuffer;

        if ((stbInfo)
                && (AUTO_CREATE_SUBTBL
                    == stbInfo->autoCreateTable)) {
            pstr += sprintf(pstr, "INSERT INTO ? USING %s TAGS(?",
                    stbInfo->stbName);
            for (int tag = 0; tag < (stbInfo->tagCount - 1);
                    tag ++ ) {
                pstr += sprintf(pstr, ",?");
            }
            pstr += sprintf(pstr, ") VALUES(?");
        } else {
            pstr += sprintf(pstr, "INSERT INTO ? VALUES(?");
        }

        int columnCount = (stbInfo)?
            stbInfo->columnCount:
            g_args.columnCount;

        for (int col = 0; col < columnCount; col ++) {
            pstr += sprintf(pstr, ",?");
        }
        pstr += sprintf(pstr, ")");

        debugPrint("%s() LN%d, stmtBuffer: %s", __func__, __LINE__, stmtBuffer);
#if STMT_BIND_PARAM_BATCH == 1
        parseSamplefileToStmtBatch(stbInfo);
#endif
    }

    for (int i = 0; i < threads; i++) {
        threadInfo *pThreadInfo = infos + i;
        pThreadInfo->threadID = i;

        tstrncpy(pThreadInfo->db_name, db_name, TSDB_DB_NAME_LEN);
        pThreadInfo->time_precision = timePrec;
        pThreadInfo->stbInfo = stbInfo;

        pThreadInfo->start_time = startTime;
        pThreadInfo->minDelay = UINT64_MAX;

        if ((NULL == stbInfo) ||
                (stbInfo->iface != REST_IFACE)) {
            //t_info->taos = taos;
            pThreadInfo->taos = taos_connect(
                    g_Dbs.host, g_Dbs.user,
                    g_Dbs.password, db_name, g_Dbs.port);
            if (NULL == pThreadInfo->taos) {
                free(infos);
                errorPrint2(
                        "%s() LN%d, connect to server fail from insert sub thread, reason: %s\n",
                        __func__, __LINE__,
                        taos_errstr(NULL));
                exit(EXIT_FAILURE);
            }

            if ((g_args.iface == STMT_IFACE)
                    || ((stbInfo)
                        && (stbInfo->iface == STMT_IFACE))) {

                pThreadInfo->stmt = taos_stmt_init(pThreadInfo->taos);
                if (NULL == pThreadInfo->stmt) {
                    free(pids);
                    free(infos);
                    errorPrint2(
                            "%s() LN%d, failed init stmt, reason: %s\n",
                            __func__, __LINE__,
                            taos_errstr(NULL));
                    exit(EXIT_FAILURE);
                }

                if (0 != taos_stmt_prepare(pThreadInfo->stmt, stmtBuffer, 0)) {
                    free(pids);
                    free(infos);
                    free(stmtBuffer);
                    errorPrint2("failed to execute taos_stmt_prepare. return 0x%x. reason: %s\n",
                            ret, taos_stmt_errstr(pThreadInfo->stmt));
                    exit(EXIT_FAILURE);
                }
                pThreadInfo->bind_ts = malloc(sizeof(int64_t));

                if (stbInfo) {
#if STMT_BIND_PARAM_BATCH == 1
                    parseStbSampleToStmtBatchForThread(
                            pThreadInfo, stbInfo, timePrec, batch);
#else
                    parseStbSampleToStmt(pThreadInfo, stbInfo, timePrec);
#endif
                } else {
#if STMT_BIND_PARAM_BATCH == 1
                    parseNtbSampleToStmtBatchForThread(
                            pThreadInfo, timePrec, batch);
#else
                    parseNtbSampleToStmt(pThreadInfo, timePrec);
#endif
                }
            }
        } else {
            pThreadInfo->taos = NULL;
        }

        /*    if ((NULL == stbInfo)
              || (0 == stbInfo->multiThreadWriteOneTbl)) {
              */
        pThreadInfo->start_table_from = tableFrom;
        pThreadInfo->ntables = i<b?a+1:a;
        pThreadInfo->end_table_to = i < b ? tableFrom + a : tableFrom + a - 1;
        tableFrom = pThreadInfo->end_table_to + 1;
        /*    } else {
              pThreadInfo->start_table_from = 0;
              pThreadInfo->ntables = stbInfo->childTblCount;
              pThreadInfo->start_time = pThreadInfo->start_time + rand_int() % 10000 - rand_tinyint();
              }
              */
        
        if (g_args.iface == REST_IFACE || ((stbInfo) && (stbInfo->iface == REST_IFACE))) {
#ifdef WINDOWS
            WSADATA wsaData;
            WSAStartup(MAKEWORD(2, 2), &wsaData);
            SOCKET sockfd;
#else
            int sockfd;
#endif
            sockfd = socket(AF_INET, SOCK_STREAM, 0);
            if (sockfd < 0) {
#ifdef WINDOWS
                errorPrint( "Could not create socket : %d" , WSAGetLastError());
#endif
                debugPrint("%s() LN%d, sockfd=%d\n", __func__, __LINE__, sockfd);
                ERROR_EXIT("opening socket");
            }

            int retConn = connect(sockfd, (struct sockaddr *)&(g_Dbs.serv_addr), sizeof(struct sockaddr));
            debugPrint("%s() LN%d connect() return %d\n", __func__, __LINE__, retConn);
            if (retConn < 0) {
                ERROR_EXIT("connecting");
            }
            pThreadInfo->sockfd = sockfd;
        }
        

        tsem_init(&(pThreadInfo->lock_sem), 0, 0);
        if (ASYNC_MODE == g_Dbs.asyncMode) {
            pthread_create(pids + i, NULL, asyncWrite, pThreadInfo);
        } else {
            pthread_create(pids + i, NULL, syncWrite, pThreadInfo);
        }
    }

    free(stmtBuffer);

    int64_t start = taosGetTimestampUs();

    for (int i = 0; i < threads; i++) {
        pthread_join(pids[i], NULL);
    }

    uint64_t totalDelay = 0;
    uint64_t maxDelay = 0;
    uint64_t minDelay = UINT64_MAX;
    uint64_t cntDelay = 1;
    double  avgDelay = 0;

    for (int i = 0; i < threads; i++) {
        threadInfo *pThreadInfo = infos + i;

        tsem_destroy(&(pThreadInfo->lock_sem));
        taos_close(pThreadInfo->taos);

        if (pThreadInfo->stmt) {
            taos_stmt_close(pThreadInfo->stmt);
        }

        tmfree((char *)pThreadInfo->bind_ts);
#if STMT_BIND_PARAM_BATCH == 1
        tmfree((char *)pThreadInfo->bind_ts_array);
        tmfree(pThreadInfo->bindParams);
        tmfree(pThreadInfo->is_null);
        if (g_args.iface == REST_IFACE || ((stbInfo) && (stbInfo->iface == REST_IFACE))) {
#ifdef WINDOWS
            closesocket(pThreadInfo->sockfd);
            WSACleanup();
#else
            close(pThreadInfo->sockfd);
#endif
        }
#else
        if (pThreadInfo->sampleBindArray) {
            for (int k = 0; k < MAX_SAMPLES; k++) {
                uintptr_t *tmp = (uintptr_t *)(*(uintptr_t *)(
                            pThreadInfo->sampleBindArray
                            + sizeof(uintptr_t *) * k));
                int columnCount = (pThreadInfo->stbInfo)?
                    pThreadInfo->stbInfo->columnCount:
                    g_args.columnCount;
                for (int c = 1; c < columnCount + 1; c++) {
                    TAOS_BIND *kit_bind = (TAOS_BIND *)((char *)tmp + (sizeof(TAOS_BIND) * c));
                    if (kit_bind)
                        tmfree(kit_bind->buffer);
                }
                tmfree((char *)tmp);
            }
            tmfree(pThreadInfo->sampleBindArray);
        }
#endif

        debugPrint("%s() LN%d, [%d] totalInsert=%"PRIu64" totalAffected=%"PRIu64"\n",
                __func__, __LINE__,
                pThreadInfo->threadID, pThreadInfo->totalInsertRows,
                pThreadInfo->totalAffectedRows);
        if (stbInfo) {
            stbInfo->totalAffectedRows += pThreadInfo->totalAffectedRows;
            stbInfo->totalInsertRows += pThreadInfo->totalInsertRows;
        } else {
            g_args.totalAffectedRows += pThreadInfo->totalAffectedRows;
            g_args.totalInsertRows += pThreadInfo->totalInsertRows;
        }

        totalDelay  += pThreadInfo->totalDelay;
        cntDelay   += pThreadInfo->cntDelay;
        if (pThreadInfo->maxDelay > maxDelay) maxDelay = pThreadInfo->maxDelay;
        if (pThreadInfo->minDelay < minDelay) minDelay = pThreadInfo->minDelay;
    }

    if (cntDelay == 0)    cntDelay = 1;
    avgDelay = (double)totalDelay / cntDelay;

    int64_t end = taosGetTimestampUs();
    int64_t t = end - start;
    if (0 == t) t = 1;

    double tInMs = (double) t / 1000000.0;

    if (stbInfo) {
        fprintf(stderr, "Spent %.4f seconds to insert rows: %"PRIu64", affected rows: %"PRIu64" with %d thread(s) into %s.%s. %.2f records/second\n\n",
                tInMs, stbInfo->totalInsertRows,
                stbInfo->totalAffectedRows,
                threads, db_name, stbInfo->stbName,
                (double)(stbInfo->totalInsertRows/tInMs));

        if (g_fpOfInsertResult) {
            fprintf(g_fpOfInsertResult,
                    "Spent %.4f seconds to insert rows: %"PRIu64", affected rows: %"PRIu64" with %d thread(s) into %s.%s. %.2f records/second\n\n",
                    tInMs, stbInfo->totalInsertRows,
                    stbInfo->totalAffectedRows,
                    threads, db_name, stbInfo->stbName,
                    (double)(stbInfo->totalInsertRows/tInMs));
        }
    } else {
        fprintf(stderr, "Spent %.4f seconds to insert rows: %"PRIu64", affected rows: %"PRIu64" with %d thread(s) into %s %.2f records/second\n\n",
                tInMs, g_args.totalInsertRows,
                g_args.totalAffectedRows,
                threads, db_name,
                (double)(g_args.totalInsertRows/tInMs));
        if (g_fpOfInsertResult) {
            fprintf(g_fpOfInsertResult,
                    "Spent %.4f seconds to insert rows: %"PRIu64", affected rows: %"PRIu64" with %d thread(s) into %s %.2f records/second\n\n",
                    tInMs, g_args.totalInsertRows,
                    g_args.totalAffectedRows,
                    threads, db_name,
                    (double)(g_args.totalInsertRows/tInMs));
        }
    }

    if (minDelay != UINT64_MAX) {
        fprintf(stderr, "insert delay, avg: %10.2fms, max: %10.2fms, min: %10.2fms\n\n",
                (double)avgDelay/1000.0,
                (double)maxDelay/1000.0,
                (double)minDelay/1000.0);

        if (g_fpOfInsertResult) {
            fprintf(g_fpOfInsertResult, "insert delay, avg:%10.2fms, max: %10.2fms, min: %10.2fms\n\n",
                    (double)avgDelay/1000.0,
                    (double)maxDelay/1000.0,
                    (double)minDelay/1000.0);
        }
    }

    //taos_close(taos);

    free(pids);
    free(infos);
}

static void *queryNtableAggrFunc(void *sarg) {
    threadInfo *pThreadInfo = (threadInfo *)sarg;
    TAOS *taos = pThreadInfo->taos;
    setThreadName("queryNtableAggrFunc");
    char *command = calloc(1, BUFFER_SIZE);
    assert(command);

    uint64_t startTime = pThreadInfo->start_time;
    char *tb_prefix = pThreadInfo->tb_prefix;
    FILE *fp = fopen(pThreadInfo->filePath, "a");
    if (NULL == fp) {
        errorPrint2("fopen %s fail, reason:%s.\n", pThreadInfo->filePath, strerror(errno));
        free(command);
        return NULL;
    }

    int64_t insertRows;
    /*  if (pThreadInfo->stbInfo) {
        insertRows = pThreadInfo->stbInfo->insertRows; //  nrecords_per_table;
        } else {
        */
    insertRows = g_args.insertRows;
    //  }

    int64_t ntables = pThreadInfo->ntables; // pThreadInfo->end_table_to - pThreadInfo->start_table_from + 1;
    int64_t totalData = insertRows * ntables;
    bool aggr_func = g_Dbs.aggr_func;

    char **aggreFunc;
    int n;

    if (g_args.demo_mode) {
        aggreFunc = g_aggreFuncDemo;
        n = aggr_func?(sizeof(g_aggreFuncDemo) / sizeof(g_aggreFuncDemo[0])) : 2;
    } else {
        aggreFunc = g_aggreFunc;
        n = aggr_func?(sizeof(g_aggreFunc) / sizeof(g_aggreFunc[0])) : 2;
    }

    if (!aggr_func) {
        printf("\nThe first field is either Binary or Bool. Aggregation functions are not supported.\n");
    }
    printf("%"PRId64" records:\n", totalData);
    fprintf(fp, "| QFunctions |    QRecords    |   QSpeed(R/s)   |  QLatency(ms) |\n");

    for (int j = 0; j < n; j++) {
        double totalT = 0;
        uint64_t count = 0;
        for (int64_t i = 0; i < ntables; i++) {
            sprintf(command, "SELECT %s FROM %s%"PRId64" WHERE ts>= %" PRIu64,
                    aggreFunc[j], tb_prefix, i, startTime);

            double t = taosGetTimestampUs();
            debugPrint("%s() LN%d, sql command: %s\n",
                    __func__, __LINE__, command);
            TAOS_RES *pSql = taos_query(taos, command);
            int32_t code = taos_errno(pSql);

            if (code != 0) {
                errorPrint2("Failed to query:%s\n", taos_errstr(pSql));
                taos_free_result(pSql);
                taos_close(taos);
                fclose(fp);
                free(command);
                return NULL;
            }

            while(taos_fetch_row(pSql) != NULL) {
                count++;
            }

            t = taosGetTimestampUs() - t;
            totalT += t;

            taos_free_result(pSql);
        }

        fprintf(fp, "|%10s  |   %"PRId64"   |  %12.2f   |   %10.2f  |\n",
                aggreFunc[j][0] == '*' ? "   *   " : aggreFunc[j], totalData,
                (double)(ntables * insertRows) / totalT, totalT / 1000000);
        printf("select %10s took %.6f second(s)\n", aggreFunc[j], totalT / 1000000);
    }
    fprintf(fp, "\n");
    fclose(fp);
    free(command);
    return NULL;
}

static void *queryStableAggrFunc(void *sarg) {
    threadInfo *pThreadInfo = (threadInfo *)sarg;
    TAOS *taos = pThreadInfo->taos;
    setThreadName("queryStableAggrFunc");
    char *command = calloc(1, BUFFER_SIZE);
    assert(command);

    FILE *fp = fopen(pThreadInfo->filePath, "a");
    if (NULL == fp) {
        printf("fopen %s fail, reason:%s.\n", pThreadInfo->filePath, strerror(errno));
        free(command);
        return NULL;
    }

    int64_t insertRows = pThreadInfo->stbInfo->insertRows;
    int64_t ntables = pThreadInfo->ntables; // pThreadInfo->end_table_to - pThreadInfo->start_table_from + 1;
    int64_t totalData = insertRows * ntables;
    bool aggr_func = g_Dbs.aggr_func;

    char **aggreFunc;
    int n;

    if (g_args.demo_mode) {
        aggreFunc = g_aggreFuncDemo;
        n = aggr_func?(sizeof(g_aggreFuncDemo) / sizeof(g_aggreFuncDemo[0])) : 2;
    } else {
        aggreFunc = g_aggreFunc;
        n = aggr_func?(sizeof(g_aggreFunc) / sizeof(g_aggreFunc[0])) : 2;
    }

    if (!aggr_func) {
        printf("\nThe first field is either Binary or Bool. Aggregation functions are not supported.\n");
    }

    printf("%"PRId64" records:\n", totalData);
    fprintf(fp, "Querying On %"PRId64" records:\n", totalData);

    for (int j = 0; j < n; j++) {
        char condition[COND_BUF_LEN] = "\0";
        char tempS[64] = "\0";

        int64_t m = 10 < ntables ? 10 : ntables;

        for (int64_t i = 1; i <= m; i++) {
            if (i == 1) {
                if (g_args.demo_mode) {
                    sprintf(tempS, "groupid = %"PRId64"", i);
                } else {
                    sprintf(tempS, "t0 = %"PRId64"", i);
                }
            } else {
                if (g_args.demo_mode) {
                    sprintf(tempS, " or groupid = %"PRId64" ", i);
                } else {
                    sprintf(tempS, " or t0 = %"PRId64" ", i);
                }
            }
            strncat(condition, tempS, COND_BUF_LEN - 1);

            sprintf(command, "SELECT %s FROM meters WHERE %s", aggreFunc[j], condition);

            printf("Where condition: %s\n", condition);

            debugPrint("%s() LN%d, sql command: %s\n",
                    __func__, __LINE__, command);
            fprintf(fp, "%s\n", command);

            double t = taosGetTimestampUs();

            TAOS_RES *pSql = taos_query(taos, command);
            int32_t code = taos_errno(pSql);

            if (code != 0) {
                errorPrint2("Failed to query:%s\n", taos_errstr(pSql));
                taos_free_result(pSql);
                taos_close(taos);
                fclose(fp);
                free(command);
                return NULL;
            }
            int count = 0;
            while(taos_fetch_row(pSql) != NULL) {
                count++;
            }
            t = taosGetTimestampUs() - t;

            fprintf(fp, "| Speed: %12.2f(per s) | Latency: %.4f(ms) |\n",
                    ntables * insertRows / (t / 1000), t);
            printf("select %10s took %.6f second(s)\n\n", aggreFunc[j], t / 1000000);

            taos_free_result(pSql);
        }
        fprintf(fp, "\n");
    }
    fclose(fp);
    free(command);

    return NULL;
}

static void prompt()
{
    if (!g_args.answer_yes) {
        printf("         Press enter key to continue or Ctrl-C to stop\n\n");
        (void)getchar();
    }
}

static int insertTestProcess() {

    setupForAnsiEscape();
    int ret = printfInsertMeta();
    resetAfterAnsiEscape();

    if (ret == -1)
        exit(EXIT_FAILURE);

    debugPrint("%d result file: %s\n", __LINE__, g_Dbs.resultFile);
    g_fpOfInsertResult = fopen(g_Dbs.resultFile, "a");
    if (NULL == g_fpOfInsertResult) {
        errorPrint("Failed to open %s for save result\n", g_Dbs.resultFile);
        return -1;
    }

    if (g_fpOfInsertResult)
        printfInsertMetaToFile(g_fpOfInsertResult);

    prompt();

    init_rand_data();

    // create database and super tables
    char *cmdBuffer = calloc(1, BUFFER_SIZE);
    assert(cmdBuffer);

    if(createDatabasesAndStables(cmdBuffer) != 0) {
        free(cmdBuffer);
        return -1;
    }
    free(cmdBuffer);

    // pretreatment
    if (prepareSampleData() != 0) {
        if (g_fpOfInsertResult)
            fclose(g_fpOfInsertResult);
        return -1;
    }

    double start;
    double end;

    if (g_totalChildTables > 0) {
        fprintf(stderr,
                "creating %"PRId64" table(s) with %d thread(s)\n\n",
                g_totalChildTables, g_Dbs.threadCountForCreateTbl);
        if (g_fpOfInsertResult) {
            fprintf(g_fpOfInsertResult,
                "creating %"PRId64" table(s) with %d thread(s)\n\n",
                g_totalChildTables, g_Dbs.threadCountForCreateTbl);
        }

        // create child tables
        start = taosGetTimestampMs();
        createChildTables();
        end = taosGetTimestampMs();

        fprintf(stderr,
                "\nSpent %.4f seconds to create %"PRId64" table(s) with %d thread(s), actual %"PRId64" table(s) created\n\n",
                (end - start)/1000.0, g_totalChildTables,
                g_Dbs.threadCountForCreateTbl, g_actualChildTables);
        if (g_fpOfInsertResult) {
            fprintf(g_fpOfInsertResult,
                "\nSpent %.4f seconds to create %"PRId64" table(s) with %d thread(s), actual %"PRId64" table(s) created\n\n",
                (end - start)/1000.0, g_totalChildTables,
                g_Dbs.threadCountForCreateTbl, g_actualChildTables);
        }
    }

    // create sub threads for inserting data
    //start = taosGetTimestampMs();
    for (int i = 0; i < g_Dbs.dbCount; i++) {
        if (g_Dbs.use_metric) {
            if (g_Dbs.db[i].superTblCount > 0) {
                for (uint64_t j = 0; j < g_Dbs.db[i].superTblCount; j++) {

                    SSuperTable* stbInfo = &g_Dbs.db[i].superTbls[j];

                    if (stbInfo && (stbInfo->insertRows > 0)) {
                        startMultiThreadInsertData(
                                g_Dbs.threadCount,
                                g_Dbs.db[i].dbName,
                                g_Dbs.db[i].dbCfg.precision,
                                stbInfo);
                    }
                }
            }
        } else {
            startMultiThreadInsertData(
                    g_Dbs.threadCount,
                    g_Dbs.db[i].dbName,
                    g_Dbs.db[i].dbCfg.precision,
                    NULL);
        }
    }
    //end = taosGetTimestampMs();

    //int64_t    totalInsertRows = 0;
    //int64_t    totalAffectedRows = 0;
    //for (int i = 0; i < g_Dbs.dbCount; i++) {
    //  for (int j = 0; j < g_Dbs.db[i].superTblCount; j++) {
    //  totalInsertRows+= g_Dbs.db[i].superTbls[j].totalInsertRows;
    //  totalAffectedRows += g_Dbs.db[i].superTbls[j].totalAffectedRows;
    //}
    //printf("Spent %.4f seconds to insert rows: %"PRId64", affected rows: %"PRId64" with %d thread(s)\n\n", end - start, totalInsertRows, totalAffectedRows, g_Dbs.threadCount);

    return 0;
}

static void *specifiedTableQuery(void *sarg) {
    threadInfo *pThreadInfo = (threadInfo *)sarg;

    setThreadName("specTableQuery");

    if (pThreadInfo->taos == NULL) {
        TAOS * taos = NULL;
        taos = taos_connect(g_queryInfo.host,
                g_queryInfo.user,
                g_queryInfo.password,
                NULL,
                g_queryInfo.port);
        if (taos == NULL) {
            errorPrint2("[%d] Failed to connect to TDengine, reason:%s\n",
                    pThreadInfo->threadID, taos_errstr(NULL));
            return NULL;
        } else {
            pThreadInfo->taos = taos;
        }
    }

    char sqlStr[TSDB_DB_NAME_LEN + 5];
    sprintf(sqlStr, "use %s", g_queryInfo.dbName);
    if (0 != queryDbExec(pThreadInfo->taos, sqlStr, NO_INSERT_TYPE, false)) {
        taos_close(pThreadInfo->taos);
        errorPrint("use database %s failed!\n\n",
                g_queryInfo.dbName);
        return NULL;
    }

    uint64_t st = 0;
    uint64_t et = 0;

    uint64_t queryTimes = g_queryInfo.specifiedQueryInfo.queryTimes;

    uint64_t totalQueried = 0;
    uint64_t lastPrintTime = taosGetTimestampMs();
    uint64_t startTs = taosGetTimestampMs();

    if (g_queryInfo.specifiedQueryInfo.result[pThreadInfo->querySeq][0] != '\0') {
        sprintf(pThreadInfo->filePath, "%s-%d",
                g_queryInfo.specifiedQueryInfo.result[pThreadInfo->querySeq],
                pThreadInfo->threadID);
    }

    while(queryTimes --) {
        if (g_queryInfo.specifiedQueryInfo.queryInterval && (et - st) <
                (int64_t)g_queryInfo.specifiedQueryInfo.queryInterval) {
            taosMsleep(g_queryInfo.specifiedQueryInfo.queryInterval - (et - st)); // ms
        }

        st = taosGetTimestampMs();

        selectAndGetResult(pThreadInfo,
                g_queryInfo.specifiedQueryInfo.sql[pThreadInfo->querySeq]);

        et = taosGetTimestampMs();
        printf("=thread[%"PRId64"] use %s complete one sql, Spent %10.3f s\n",
                taosGetSelfPthreadId(), g_queryInfo.queryMode, (et - st)/1000.0);

        totalQueried ++;
        g_queryInfo.specifiedQueryInfo.totalQueried ++;

        uint64_t  currentPrintTime = taosGetTimestampMs();
        uint64_t  endTs = taosGetTimestampMs();
        if (currentPrintTime - lastPrintTime > 30*1000) {
            debugPrint("%s() LN%d, endTs=%"PRIu64" ms, startTs=%"PRIu64" ms\n",
                    __func__, __LINE__, endTs, startTs);
            printf("thread[%d] has currently completed queries: %"PRIu64", QPS: %10.6f\n",
                    pThreadInfo->threadID,
                    totalQueried,
                    (double)(totalQueried/((endTs-startTs)/1000.0)));
            lastPrintTime = currentPrintTime;
        }
    }
    return NULL;
}

static void replaceChildTblName(char* inSql, char* outSql, int tblIndex) {
    char sourceString[32] = "xxxx";
    char subTblName[TSDB_TABLE_NAME_LEN];
    sprintf(subTblName, "%s.%s",
            g_queryInfo.dbName,
            g_queryInfo.superQueryInfo.childTblName + tblIndex*TSDB_TABLE_NAME_LEN);

    //printf("inSql: %s\n", inSql);

    char* pos = strstr(inSql, sourceString);
    if (0 == pos) {
        return;
    }

    tstrncpy(outSql, inSql, pos - inSql + 1);
    //printf("1: %s\n", outSql);
    strncat(outSql, subTblName, BUFFER_SIZE - 1);
    //printf("2: %s\n", outSql);
    strncat(outSql, pos+strlen(sourceString), BUFFER_SIZE - 1);
    //printf("3: %s\n", outSql);
}

static void *superTableQuery(void *sarg) {
    char *sqlstr = calloc(1, BUFFER_SIZE);
    assert(sqlstr);

    threadInfo *pThreadInfo = (threadInfo *)sarg;

    setThreadName("superTableQuery");

    if (pThreadInfo->taos == NULL) {
        TAOS * taos = NULL;
        taos = taos_connect(g_queryInfo.host,
                g_queryInfo.user,
                g_queryInfo.password,
                NULL,
                g_queryInfo.port);
        if (taos == NULL) {
            errorPrint("[%d] Failed to connect to TDengine, reason:%s\n",
                    pThreadInfo->threadID, taos_errstr(NULL));
            free(sqlstr);
            return NULL;
        } else {
            pThreadInfo->taos = taos;
        }
    }

    uint64_t st = 0;
    uint64_t et = (int64_t)g_queryInfo.superQueryInfo.queryInterval;

    uint64_t queryTimes = g_queryInfo.superQueryInfo.queryTimes;
    uint64_t totalQueried = 0;
    uint64_t  startTs = taosGetTimestampMs();

    uint64_t  lastPrintTime = taosGetTimestampMs();
    while(queryTimes --) {
        if (g_queryInfo.superQueryInfo.queryInterval
                && (et - st) < (int64_t)g_queryInfo.superQueryInfo.queryInterval) {
            taosMsleep(g_queryInfo.superQueryInfo.queryInterval - (et - st)); // ms
            //printf("========sleep duration:%"PRId64 "========inserted rows:%d, table range:%d - %d\n", (1000 - (et - st)), i, pThreadInfo->start_table_from, pThreadInfo->end_table_to);
        }

        st = taosGetTimestampMs();
        for (int i = pThreadInfo->start_table_from; i <= pThreadInfo->end_table_to; i++) {
            for (int j = 0; j < g_queryInfo.superQueryInfo.sqlCount; j++) {
                memset(sqlstr, 0, BUFFER_SIZE);
                replaceChildTblName(g_queryInfo.superQueryInfo.sql[j], sqlstr, i);
                if (g_queryInfo.superQueryInfo.result[j][0] != '\0') {
                    sprintf(pThreadInfo->filePath, "%s-%d",
                            g_queryInfo.superQueryInfo.result[j],
                            pThreadInfo->threadID);
                }
                selectAndGetResult(pThreadInfo, sqlstr);

                totalQueried++;
                g_queryInfo.superQueryInfo.totalQueried ++;

                int64_t  currentPrintTime = taosGetTimestampMs();
                int64_t  endTs = taosGetTimestampMs();
                if (currentPrintTime - lastPrintTime > 30*1000) {
                    printf("thread[%d] has currently completed queries: %"PRIu64", QPS: %10.3f\n",
                            pThreadInfo->threadID,
                            totalQueried,
                            (double)(totalQueried/((endTs-startTs)/1000.0)));
                    lastPrintTime = currentPrintTime;
                }
            }
        }
        et = taosGetTimestampMs();
        printf("####thread[%"PRId64"] complete all sqls to allocate all sub-tables[%"PRIu64" - %"PRIu64"] once queries duration:%.4fs\n\n",
                taosGetSelfPthreadId(),
                pThreadInfo->start_table_from,
                pThreadInfo->end_table_to,
                (double)(et - st)/1000.0);
    }

    free(sqlstr);
    return NULL;
}

static int queryTestProcess() {

    setupForAnsiEscape();
    printfQueryMeta();
    resetAfterAnsiEscape();

    TAOS * taos = NULL;
    taos = taos_connect(g_queryInfo.host,
            g_queryInfo.user,
            g_queryInfo.password,
            NULL,
            g_queryInfo.port);
    if (taos == NULL) {
        errorPrint("Failed to connect to TDengine, reason:%s\n",
                taos_errstr(NULL));
        exit(EXIT_FAILURE);
    }

    if (0 != g_queryInfo.superQueryInfo.sqlCount) {
        getAllChildNameOfSuperTable(taos,
                g_queryInfo.dbName,
                g_queryInfo.superQueryInfo.stbName,
                &g_queryInfo.superQueryInfo.childTblName,
                &g_queryInfo.superQueryInfo.childTblCount);
    }

    prompt();

    if (g_args.debug_print || g_args.verbose_print) {
        printfQuerySystemInfo(taos);
    }

    if (0 == strncasecmp(g_queryInfo.queryMode, "rest", strlen("rest"))) {
        if (convertHostToServAddr(
                    g_queryInfo.host, g_queryInfo.port, &g_queryInfo.serv_addr) != 0)
            ERROR_EXIT("convert host to server address");
    }

    pthread_t  *pids  = NULL;
    threadInfo *infos = NULL;
    //==== create sub threads for query from specify table
    int nConcurrent = g_queryInfo.specifiedQueryInfo.concurrent;
    uint64_t nSqlCount = g_queryInfo.specifiedQueryInfo.sqlCount;

    uint64_t startTs = taosGetTimestampMs();

    if ((nSqlCount > 0) && (nConcurrent > 0)) {

        pids  = calloc(1, nConcurrent * nSqlCount * sizeof(pthread_t));
        infos = calloc(1, nConcurrent * nSqlCount * sizeof(threadInfo));

        if ((NULL == pids) || (NULL == infos)) {
            taos_close(taos);
            ERROR_EXIT("memory allocation failed for create threads\n");
        }

        for (uint64_t i = 0; i < nSqlCount; i++) {
            for (int j = 0; j < nConcurrent; j++) {
                uint64_t seq = i * nConcurrent + j;
                threadInfo *pThreadInfo = infos + seq;
                pThreadInfo->threadID = seq;
                pThreadInfo->querySeq = i;

                if (0 == strncasecmp(g_queryInfo.queryMode, "taosc", 5)) {

                    char sqlStr[TSDB_DB_NAME_LEN + 5];
                    sprintf(sqlStr, "USE %s", g_queryInfo.dbName);
                    if (0 != queryDbExec(taos, sqlStr, NO_INSERT_TYPE, false)) {
                        taos_close(taos);
                        free(infos);
                        free(pids);
                        errorPrint2("use database %s failed!\n\n",
                                g_queryInfo.dbName);
                        return -1;
                    }
                }

                if (0 == strncasecmp(g_queryInfo.queryMode, "rest", 4)) {
#ifdef WINDOWS
                    WSADATA wsaData;
                    WSAStartup(MAKEWORD(2, 2), &wsaData);
                    SOCKET sockfd;
#else
                    int sockfd;
#endif
                    sockfd = socket(AF_INET, SOCK_STREAM, 0);
                    if (sockfd < 0) {
#ifdef WINDOWS
                        errorPrint( "Could not create socket : %d" , WSAGetLastError());
#endif
                        debugPrint("%s() LN%d, sockfd=%d\n", __func__, __LINE__, sockfd);
                        ERROR_EXIT("opening socket");
                    }

                    int retConn = connect(sockfd, (struct sockaddr *)&(g_queryInfo.serv_addr),
                         sizeof(struct sockaddr));
                    debugPrint("%s() LN%d connect() return %d\n", __func__, __LINE__, retConn);
                    if (retConn < 0) {
                        ERROR_EXIT("connecting");
                    }
                    pThreadInfo->sockfd = sockfd;
                }
                pThreadInfo->taos = NULL;// workaround to use separate taos connection;

                pthread_create(pids + seq, NULL, specifiedTableQuery,
                        pThreadInfo);
            }
        }
    } else {
        g_queryInfo.specifiedQueryInfo.concurrent = 0;
    }

    taos_close(taos);

    pthread_t  *pidsOfSub  = NULL;
    threadInfo *infosOfSub = NULL;
    //==== create sub threads for query from all sub table of the super table
    if ((g_queryInfo.superQueryInfo.sqlCount > 0)
            && (g_queryInfo.superQueryInfo.threadCnt > 0)) {
        pidsOfSub  = calloc(1, g_queryInfo.superQueryInfo.threadCnt * sizeof(pthread_t));
        infosOfSub = calloc(1, g_queryInfo.superQueryInfo.threadCnt * sizeof(threadInfo));

        if ((NULL == pidsOfSub) || (NULL == infosOfSub)) {
            free(infos);
            free(pids);

            ERROR_EXIT("memory allocation failed for create threads\n");
        }

        int64_t ntables = g_queryInfo.superQueryInfo.childTblCount;
        int threads = g_queryInfo.superQueryInfo.threadCnt;

        int64_t a = ntables / threads;
        if (a < 1) {
            threads = ntables;
            a = 1;
        }

        int64_t b = 0;
        if (threads != 0) {
            b = ntables % threads;
        }

        uint64_t tableFrom = 0;
        for (int i = 0; i < threads; i++) {
            threadInfo *pThreadInfo = infosOfSub + i;
            pThreadInfo->threadID = i;

            pThreadInfo->start_table_from = tableFrom;
            pThreadInfo->ntables = i<b?a+1:a;
            pThreadInfo->end_table_to = i < b ? tableFrom + a : tableFrom + a - 1;
            tableFrom = pThreadInfo->end_table_to + 1;
            pThreadInfo->taos = NULL; // workaround to use separate taos connection;
            if (0 == strncasecmp(g_queryInfo.queryMode, "rest", 4)) {
#ifdef WINDOWS
                WSADATA wsaData;
                WSAStartup(MAKEWORD(2, 2), &wsaData);
                SOCKET sockfd;
#else
                int sockfd;
#endif
                sockfd = socket(AF_INET, SOCK_STREAM, 0);
                if (sockfd < 0) {
#ifdef WINDOWS
                    errorPrint( "Could not create socket : %d" , WSAGetLastError());
#endif
                    debugPrint("%s() LN%d, sockfd=%d\n", __func__, __LINE__, sockfd);
                    ERROR_EXIT("opening socket");
                }

                int retConn = connect(sockfd, (struct sockaddr *)&(g_queryInfo.serv_addr),
                        sizeof(struct sockaddr));
                debugPrint("%s() LN%d connect() return %d\n", __func__, __LINE__, retConn);
                if (retConn < 0) {
                    ERROR_EXIT("connecting");
                }
                pThreadInfo->sockfd = sockfd;
            }
            pthread_create(pidsOfSub + i, NULL, superTableQuery, pThreadInfo);
        }

        g_queryInfo.superQueryInfo.threadCnt = threads;
    } else {
        g_queryInfo.superQueryInfo.threadCnt = 0;
    }

    if ((nSqlCount > 0) && (nConcurrent > 0)) {
        for (int i = 0; i < nConcurrent; i++) {
            for (int j = 0; j < nSqlCount; j++) {
                pthread_join(pids[i * nSqlCount + j], NULL);
                if (0 == strncasecmp(g_queryInfo.queryMode, "rest", 4)) {
                    threadInfo *pThreadInfo = infos + i * nSqlCount + j;
#ifdef WINDOWS
                    closesocket(pThreadInfo->sockfd);
                    WSACleanup();
#else
                    close(pThreadInfo->sockfd);
#endif
                }
            }
        }
    }

    tmfree((char*)pids);
    tmfree((char*)infos);

    for (int i = 0; i < g_queryInfo.superQueryInfo.threadCnt; i++) {
        pthread_join(pidsOfSub[i], NULL);
        if (0 == strncasecmp(g_queryInfo.queryMode, "rest", 4)) {
            threadInfo *pThreadInfo = infosOfSub + i;
#ifdef WINDOWS
            closesocket(pThreadInfo->sockfd);
            WSACleanup();
#else
            close(pThreadInfo->sockfd);
#endif
        }
    }

    tmfree((char*)pidsOfSub);
    tmfree((char*)infosOfSub);

    //  taos_close(taos);// workaround to use separate taos connection;
    uint64_t endTs = taosGetTimestampMs();

    uint64_t totalQueried = g_queryInfo.specifiedQueryInfo.totalQueried +
        g_queryInfo.superQueryInfo.totalQueried;

    fprintf(stderr, "==== completed total queries: %"PRIu64", the QPS of all threads: %10.3f====\n",
            totalQueried,
            (double)(totalQueried/((endTs-startTs)/1000.0)));
    return 0;
}

static void stable_sub_callback(
        TAOS_SUB* tsub, TAOS_RES *res, void* param, int code) {
    if (res == NULL || taos_errno(res) != 0) {
        errorPrint2("%s() LN%d, failed to subscribe result, code:%d, reason:%s\n",
                __func__, __LINE__, code, taos_errstr(res));
        return;
    }

    if (param)
        fetchResult(res, (threadInfo *)param);
    // tao_unsubscribe() will free result.
}

static void specified_sub_callback(
        TAOS_SUB* tsub, TAOS_RES *res, void* param, int code) {
    if (res == NULL || taos_errno(res) != 0) {
        errorPrint2("%s() LN%d, failed to subscribe result, code:%d, reason:%s\n",
                __func__, __LINE__, code, taos_errstr(res));
        return;
    }

    if (param)
        fetchResult(res, (threadInfo *)param);
    // tao_unsubscribe() will free result.
}

static TAOS_SUB* subscribeImpl(
        QUERY_CLASS class,
        threadInfo *pThreadInfo,
        char *sql, char* topic, bool restart, uint64_t interval)
{
    TAOS_SUB* tsub = NULL;

    if ((SPECIFIED_CLASS == class)
            && (ASYNC_MODE == g_queryInfo.specifiedQueryInfo.asyncMode)) {
        tsub = taos_subscribe(
                pThreadInfo->taos,
                restart,
                topic, sql, specified_sub_callback, (void*)pThreadInfo,
                g_queryInfo.specifiedQueryInfo.subscribeInterval);
    } else if ((STABLE_CLASS == class)
            && (ASYNC_MODE == g_queryInfo.superQueryInfo.asyncMode)) {
        tsub = taos_subscribe(
                pThreadInfo->taos,
                restart,
                topic, sql, stable_sub_callback, (void*)pThreadInfo,
                g_queryInfo.superQueryInfo.subscribeInterval);
    } else {
        tsub = taos_subscribe(
                pThreadInfo->taos,
                restart,
                topic, sql, NULL, NULL, interval);
    }

    if (tsub == NULL) {
        errorPrint2("failed to create subscription. topic:%s, sql:%s\n", topic, sql);
        return NULL;
    }

    return tsub;
}

static void *superSubscribe(void *sarg) {
    threadInfo *pThreadInfo = (threadInfo *)sarg;
    char *subSqlStr = calloc(1, BUFFER_SIZE);
    assert(subSqlStr);

    TAOS_SUB*    tsub[MAX_QUERY_SQL_COUNT] = {0};
    uint64_t tsubSeq;

    setThreadName("superSub");

    if (pThreadInfo->ntables > MAX_QUERY_SQL_COUNT) {
        free(subSqlStr);
        errorPrint("The table number(%"PRId64") of the thread is more than max query sql count: %d\n",
                pThreadInfo->ntables, MAX_QUERY_SQL_COUNT);
        exit(EXIT_FAILURE);
    }

    if (pThreadInfo->taos == NULL) {
        pThreadInfo->taos = taos_connect(g_queryInfo.host,
                g_queryInfo.user,
                g_queryInfo.password,
                g_queryInfo.dbName,
                g_queryInfo.port);
        if (pThreadInfo->taos == NULL) {
            errorPrint2("[%d] Failed to connect to TDengine, reason:%s\n",
                    pThreadInfo->threadID, taos_errstr(NULL));
            free(subSqlStr);
            return NULL;
        }
    }

    char sqlStr[TSDB_DB_NAME_LEN + 5];
    sprintf(sqlStr, "USE %s", g_queryInfo.dbName);
    if (0 != queryDbExec(pThreadInfo->taos, sqlStr, NO_INSERT_TYPE, false)) {
        taos_close(pThreadInfo->taos);
        errorPrint2("use database %s failed!\n\n",
                g_queryInfo.dbName);
        free(subSqlStr);
        return NULL;
    }

    char topic[32] = {0};
    for (uint64_t i = pThreadInfo->start_table_from;
            i <= pThreadInfo->end_table_to; i++) {
        tsubSeq = i - pThreadInfo->start_table_from;
        verbosePrint("%s() LN%d, [%d], start=%"PRId64" end=%"PRId64" i=%"PRIu64"\n",
                __func__, __LINE__,
                pThreadInfo->threadID,
                pThreadInfo->start_table_from,
                pThreadInfo->end_table_to, i);
        sprintf(topic, "taosdemo-subscribe-%"PRIu64"-%"PRIu64"",
                i, pThreadInfo->querySeq);
        memset(subSqlStr, 0, BUFFER_SIZE);
        replaceChildTblName(
                g_queryInfo.superQueryInfo.sql[pThreadInfo->querySeq],
                subSqlStr, i);
        if (g_queryInfo.superQueryInfo.result[pThreadInfo->querySeq][0] != 0) {
            sprintf(pThreadInfo->filePath, "%s-%d",
                    g_queryInfo.superQueryInfo.result[pThreadInfo->querySeq],
                    pThreadInfo->threadID);
        }

        verbosePrint("%s() LN%d, [%d] subSqlStr: %s\n",
                __func__, __LINE__, pThreadInfo->threadID, subSqlStr);
        tsub[tsubSeq] = subscribeImpl(
                STABLE_CLASS,
                pThreadInfo, subSqlStr, topic,
                g_queryInfo.superQueryInfo.subscribeRestart,
                g_queryInfo.superQueryInfo.subscribeInterval);
        if (NULL == tsub[tsubSeq]) {
            taos_close(pThreadInfo->taos);
            free(subSqlStr);
            return NULL;
        }
    }

    // start loop to consume result
    int consumed[MAX_QUERY_SQL_COUNT];
    for (int i = 0; i < MAX_QUERY_SQL_COUNT; i++) {
        consumed[i] = 0;
    }
    TAOS_RES* res = NULL;

    uint64_t st = 0, et = 0;

    while ((g_queryInfo.superQueryInfo.endAfterConsume == -1)
            || (g_queryInfo.superQueryInfo.endAfterConsume >
                consumed[pThreadInfo->end_table_to
                - pThreadInfo->start_table_from])) {

        verbosePrint("super endAfterConsume: %d, consumed: %d\n",
                g_queryInfo.superQueryInfo.endAfterConsume,
                consumed[pThreadInfo->end_table_to
                - pThreadInfo->start_table_from]);
        for (uint64_t i = pThreadInfo->start_table_from;
                i <= pThreadInfo->end_table_to; i++) {
            tsubSeq = i - pThreadInfo->start_table_from;
            if (ASYNC_MODE == g_queryInfo.superQueryInfo.asyncMode) {
                continue;
            }

            st = taosGetTimestampMs();
            performancePrint("st: %"PRIu64" et: %"PRIu64" st-et: %"PRIu64"\n", st, et, (st - et));
            res = taos_consume(tsub[tsubSeq]);
            et = taosGetTimestampMs();
            performancePrint("st: %"PRIu64" et: %"PRIu64" delta: %"PRIu64"\n", st, et, (et - st));

            if (res) {
                if (g_queryInfo.superQueryInfo.result[pThreadInfo->querySeq][0] != 0) {
                    sprintf(pThreadInfo->filePath, "%s-%d",
                            g_queryInfo.superQueryInfo.result[pThreadInfo->querySeq],
                            pThreadInfo->threadID);
                    fetchResult(res, pThreadInfo);
                }
                consumed[tsubSeq] ++;

                if ((g_queryInfo.superQueryInfo.resubAfterConsume != -1)
                        && (consumed[tsubSeq] >=
                            g_queryInfo.superQueryInfo.resubAfterConsume)) {
                    verbosePrint("%s() LN%d, keepProgress:%d, resub super table query: %"PRIu64"\n",
                            __func__, __LINE__,
                            g_queryInfo.superQueryInfo.subscribeKeepProgress,
                            pThreadInfo->querySeq);
                    taos_unsubscribe(tsub[tsubSeq],
                            g_queryInfo.superQueryInfo.subscribeKeepProgress);
                    consumed[tsubSeq]= 0;
                    tsub[tsubSeq] = subscribeImpl(
                            STABLE_CLASS,
                            pThreadInfo, subSqlStr, topic,
                            g_queryInfo.superQueryInfo.subscribeRestart,
                            g_queryInfo.superQueryInfo.subscribeInterval
                            );
                    if (NULL == tsub[tsubSeq]) {
                        taos_close(pThreadInfo->taos);
                        free(subSqlStr);
                        return NULL;
                    }
                }
            }
        }
    }
    verbosePrint("%s() LN%d, super endAfterConsume: %d, consumed: %d\n",
            __func__, __LINE__,
            g_queryInfo.superQueryInfo.endAfterConsume,
            consumed[pThreadInfo->end_table_to - pThreadInfo->start_table_from]);
    taos_free_result(res);

    for (uint64_t i = pThreadInfo->start_table_from;
            i <= pThreadInfo->end_table_to; i++) {
        tsubSeq = i - pThreadInfo->start_table_from;
        taos_unsubscribe(tsub[tsubSeq], 0);
    }

    taos_close(pThreadInfo->taos);
    free(subSqlStr);
    return NULL;
}

static void *specifiedSubscribe(void *sarg) {
    threadInfo *pThreadInfo = (threadInfo *)sarg;
    //  TAOS_SUB*  tsub = NULL;

    setThreadName("specSub");

    if (pThreadInfo->taos == NULL) {
        pThreadInfo->taos = taos_connect(g_queryInfo.host,
                g_queryInfo.user,
                g_queryInfo.password,
                g_queryInfo.dbName,
                g_queryInfo.port);
        if (pThreadInfo->taos == NULL) {
            errorPrint2("[%d] Failed to connect to TDengine, reason:%s\n",
                    pThreadInfo->threadID, taos_errstr(NULL));
            return NULL;
        }
    }

    char sqlStr[TSDB_DB_NAME_LEN + 5];
    sprintf(sqlStr, "USE %s", g_queryInfo.dbName);
    if (0 != queryDbExec(pThreadInfo->taos, sqlStr, NO_INSERT_TYPE, false)) {
        taos_close(pThreadInfo->taos);
        return NULL;
    }

    sprintf(g_queryInfo.specifiedQueryInfo.topic[pThreadInfo->threadID],
            "taosdemo-subscribe-%"PRIu64"-%d",
            pThreadInfo->querySeq,
            pThreadInfo->threadID);
    if (g_queryInfo.specifiedQueryInfo.result[pThreadInfo->querySeq][0] != '\0') {
        sprintf(pThreadInfo->filePath, "%s-%d",
                g_queryInfo.specifiedQueryInfo.result[pThreadInfo->querySeq],
                pThreadInfo->threadID);
    }
    g_queryInfo.specifiedQueryInfo.tsub[pThreadInfo->threadID] = subscribeImpl(
            SPECIFIED_CLASS, pThreadInfo,
            g_queryInfo.specifiedQueryInfo.sql[pThreadInfo->querySeq],
            g_queryInfo.specifiedQueryInfo.topic[pThreadInfo->threadID],
            g_queryInfo.specifiedQueryInfo.subscribeRestart,
            g_queryInfo.specifiedQueryInfo.subscribeInterval);
    if (NULL == g_queryInfo.specifiedQueryInfo.tsub[pThreadInfo->threadID]) {
        taos_close(pThreadInfo->taos);
        return NULL;
    }

    // start loop to consume result

    g_queryInfo.specifiedQueryInfo.consumed[pThreadInfo->threadID] = 0;
    while((g_queryInfo.specifiedQueryInfo.endAfterConsume[pThreadInfo->querySeq] == -1)
            || (g_queryInfo.specifiedQueryInfo.consumed[pThreadInfo->threadID] <
                g_queryInfo.specifiedQueryInfo.endAfterConsume[pThreadInfo->querySeq])) {

        printf("consumed[%d]: %d, endAfterConsum[%"PRId64"]: %d\n",
                pThreadInfo->threadID,
                g_queryInfo.specifiedQueryInfo.consumed[pThreadInfo->threadID],
                pThreadInfo->querySeq,
                g_queryInfo.specifiedQueryInfo.endAfterConsume[pThreadInfo->querySeq]);
        if (ASYNC_MODE == g_queryInfo.specifiedQueryInfo.asyncMode) {
            continue;
        }

        g_queryInfo.specifiedQueryInfo.res[pThreadInfo->threadID] = taos_consume(
                g_queryInfo.specifiedQueryInfo.tsub[pThreadInfo->threadID]);
        if (g_queryInfo.specifiedQueryInfo.res[pThreadInfo->threadID]) {
            if (g_queryInfo.specifiedQueryInfo.result[pThreadInfo->querySeq][0]
                    != 0) {
                sprintf(pThreadInfo->filePath, "%s-%d",
                        g_queryInfo.specifiedQueryInfo.result[pThreadInfo->querySeq],
                        pThreadInfo->threadID);
            }
            fetchResult(
                    g_queryInfo.specifiedQueryInfo.res[pThreadInfo->threadID],
                    pThreadInfo);

            g_queryInfo.specifiedQueryInfo.consumed[pThreadInfo->threadID] ++;
            if ((g_queryInfo.specifiedQueryInfo.resubAfterConsume[pThreadInfo->querySeq] != -1)
                    && (g_queryInfo.specifiedQueryInfo.consumed[pThreadInfo->threadID] >=
                        g_queryInfo.specifiedQueryInfo.resubAfterConsume[pThreadInfo->querySeq])) {
                printf("keepProgress:%d, resub specified query: %"PRIu64"\n",
                        g_queryInfo.specifiedQueryInfo.subscribeKeepProgress,
                        pThreadInfo->querySeq);
                g_queryInfo.specifiedQueryInfo.consumed[pThreadInfo->threadID] = 0;
                taos_unsubscribe(g_queryInfo.specifiedQueryInfo.tsub[pThreadInfo->threadID],
                        g_queryInfo.specifiedQueryInfo.subscribeKeepProgress);
                g_queryInfo.specifiedQueryInfo.tsub[pThreadInfo->threadID] =
                    subscribeImpl(
                            SPECIFIED_CLASS,
                            pThreadInfo,
                            g_queryInfo.specifiedQueryInfo.sql[pThreadInfo->querySeq],
                            g_queryInfo.specifiedQueryInfo.topic[pThreadInfo->threadID],
                            g_queryInfo.specifiedQueryInfo.subscribeRestart,
                            g_queryInfo.specifiedQueryInfo.subscribeInterval);
                if (NULL == g_queryInfo.specifiedQueryInfo.tsub[pThreadInfo->threadID]) {
                    taos_close(pThreadInfo->taos);
                    return NULL;
                }
            }
        }
    }
    taos_free_result(g_queryInfo.specifiedQueryInfo.res[pThreadInfo->threadID]);
    taos_close(pThreadInfo->taos);

    return NULL;
}

static int subscribeTestProcess() {
    setupForAnsiEscape();
    printfQueryMeta();
    resetAfterAnsiEscape();

    prompt();

    TAOS * taos = NULL;
    taos = taos_connect(g_queryInfo.host,
            g_queryInfo.user,
            g_queryInfo.password,
            g_queryInfo.dbName,
            g_queryInfo.port);
    if (taos == NULL) {
        errorPrint2("Failed to connect to TDengine, reason:%s\n",
                taos_errstr(NULL));
        exit(EXIT_FAILURE);
    }

    if (0 != g_queryInfo.superQueryInfo.sqlCount) {
        getAllChildNameOfSuperTable(taos,
                g_queryInfo.dbName,
                g_queryInfo.superQueryInfo.stbName,
                &g_queryInfo.superQueryInfo.childTblName,
                &g_queryInfo.superQueryInfo.childTblCount);
    }

    taos_close(taos); // workaround to use separate taos connection;

    pthread_t  *pids = NULL;
    threadInfo *infos = NULL;

    pthread_t  *pidsOfStable  = NULL;
    threadInfo *infosOfStable = NULL;

    //==== create threads for query for specified table
    if (g_queryInfo.specifiedQueryInfo.sqlCount <= 0) {
        debugPrint("%s() LN%d, specified query sqlCount %d.\n",
                __func__, __LINE__,
                g_queryInfo.specifiedQueryInfo.sqlCount);
    } else {
        if (g_queryInfo.specifiedQueryInfo.concurrent <= 0) {
            errorPrint2("%s() LN%d, specified query sqlCount %d.\n",
                    __func__, __LINE__,
                    g_queryInfo.specifiedQueryInfo.sqlCount);
            exit(EXIT_FAILURE);
        }

        pids  = calloc(
                1,
                g_queryInfo.specifiedQueryInfo.sqlCount *
                g_queryInfo.specifiedQueryInfo.concurrent *
                sizeof(pthread_t));
        infos = calloc(
                1,
                g_queryInfo.specifiedQueryInfo.sqlCount *
                g_queryInfo.specifiedQueryInfo.concurrent *
                sizeof(threadInfo));
        if ((NULL == pids) || (NULL == infos)) {
            errorPrint2("%s() LN%d, malloc failed for create threads\n", __func__, __LINE__);
            exit(EXIT_FAILURE);
        }

        for (int i = 0; i < g_queryInfo.specifiedQueryInfo.sqlCount; i++) {
            for (int j = 0; j < g_queryInfo.specifiedQueryInfo.concurrent; j++) {
                uint64_t seq = i * g_queryInfo.specifiedQueryInfo.concurrent + j;
                threadInfo *pThreadInfo = infos + seq;
                pThreadInfo->threadID = seq;
                pThreadInfo->querySeq = i;
                pThreadInfo->taos = NULL;  // workaround to use separate taos connection;
                pthread_create(pids + seq, NULL, specifiedSubscribe, pThreadInfo);
            }
        }
    }

    //==== create threads for super table query
    if (g_queryInfo.superQueryInfo.sqlCount <= 0) {
        debugPrint("%s() LN%d, super table query sqlCount %d.\n",
                __func__, __LINE__,
                g_queryInfo.superQueryInfo.sqlCount);
    } else {
        if ((g_queryInfo.superQueryInfo.sqlCount > 0)
                && (g_queryInfo.superQueryInfo.threadCnt > 0)) {
            pidsOfStable  = calloc(
                    1,
                    g_queryInfo.superQueryInfo.sqlCount *
                    g_queryInfo.superQueryInfo.threadCnt *
                    sizeof(pthread_t));
            infosOfStable = calloc(
                    1,
                    g_queryInfo.superQueryInfo.sqlCount *
                    g_queryInfo.superQueryInfo.threadCnt *
                    sizeof(threadInfo));
            if ((NULL == pidsOfStable) || (NULL == infosOfStable)) {
                errorPrint2("%s() LN%d, malloc failed for create threads\n",
                        __func__, __LINE__);
                // taos_close(taos);
                exit(EXIT_FAILURE);
            }

            int64_t ntables = g_queryInfo.superQueryInfo.childTblCount;
            int threads = g_queryInfo.superQueryInfo.threadCnt;

            int64_t a = ntables / threads;
            if (a < 1) {
                threads = ntables;
                a = 1;
            }

            int64_t b = 0;
            if (threads != 0) {
                b = ntables % threads;
            }

            for (uint64_t i = 0; i < g_queryInfo.superQueryInfo.sqlCount; i++) {
                uint64_t tableFrom = 0;
                for (int j = 0; j < threads; j++) {
                    uint64_t seq = i * threads + j;
                    threadInfo *pThreadInfo = infosOfStable + seq;
                    pThreadInfo->threadID = seq;
                    pThreadInfo->querySeq = i;

                    pThreadInfo->start_table_from = tableFrom;
                    pThreadInfo->ntables = j<b?a+1:a;
                    pThreadInfo->end_table_to = j<b?tableFrom+a:tableFrom+a-1;
                    tableFrom = pThreadInfo->end_table_to + 1;
                    pThreadInfo->taos = NULL; // workaround to use separate taos connection;
                    pthread_create(pidsOfStable + seq,
                            NULL, superSubscribe, pThreadInfo);
                }
            }

            g_queryInfo.superQueryInfo.threadCnt = threads;

            for (int i = 0; i < g_queryInfo.superQueryInfo.sqlCount; i++) {
                for (int j = 0; j < threads; j++) {
                    uint64_t seq = i * threads + j;
                    pthread_join(pidsOfStable[seq], NULL);
                }
            }
        }
    }

    for (int i = 0; i < g_queryInfo.specifiedQueryInfo.sqlCount; i++) {
        for (int j = 0; j < g_queryInfo.specifiedQueryInfo.concurrent; j++) {
            uint64_t seq = i * g_queryInfo.specifiedQueryInfo.concurrent + j;
            pthread_join(pids[seq], NULL);
        }
    }

    tmfree((char*)pids);
    tmfree((char*)infos);

    tmfree((char*)pidsOfStable);
    tmfree((char*)infosOfStable);
    //   taos_close(taos);
    return 0;
}

static void setParaFromArg() {
    char type[20];
    char length[20];
    if (g_args.host) {
        tstrncpy(g_Dbs.host, g_args.host, MAX_HOSTNAME_SIZE);
    } else {
        tstrncpy(g_Dbs.host, "127.0.0.1", MAX_HOSTNAME_SIZE);
    }

    if (g_args.user) {
        tstrncpy(g_Dbs.user, g_args.user, MAX_USERNAME_SIZE);
    }

    tstrncpy(g_Dbs.password, g_args.password, SHELL_MAX_PASSWORD_LEN);

    if (g_args.port) {
        g_Dbs.port = g_args.port;
    }

    g_Dbs.threadCount = g_args.nthreads;
    g_Dbs.threadCountForCreateTbl = g_args.nthreads;

    g_Dbs.dbCount = 1;
    g_Dbs.db[0].drop = true;

    tstrncpy(g_Dbs.db[0].dbName, g_args.database, TSDB_DB_NAME_LEN);
    g_Dbs.db[0].dbCfg.replica = g_args.replica;
    tstrncpy(g_Dbs.db[0].dbCfg.precision, "ms", SMALL_BUFF_LEN);

    tstrncpy(g_Dbs.resultFile, g_args.output_file, MAX_FILE_NAME_LEN);

    g_Dbs.use_metric = g_args.use_metric;
    g_args.prepared_rand = min(g_args.insertRows, MAX_PREPARED_RAND);
    g_Dbs.aggr_func = g_args.aggr_func;

    char dataString[TSDB_MAX_BYTES_PER_ROW];
    char *data_type = g_args.data_type;
    char **dataType = g_args.dataType;

    memset(dataString, 0, TSDB_MAX_BYTES_PER_ROW);

    if ((data_type[0] == TSDB_DATA_TYPE_BINARY)
            || (data_type[0] == TSDB_DATA_TYPE_BOOL)
            || (data_type[0] == TSDB_DATA_TYPE_NCHAR)) {
        g_Dbs.aggr_func = false;
    }

    if (g_args.use_metric) {
        g_Dbs.db[0].superTblCount = 1;
        tstrncpy(g_Dbs.db[0].superTbls[0].stbName, "meters", TSDB_TABLE_NAME_LEN);
        g_Dbs.db[0].superTbls[0].childTblCount = g_args.ntables;
        g_Dbs.threadCount = g_args.nthreads;
        g_Dbs.threadCountForCreateTbl = g_args.nthreads;
        g_Dbs.asyncMode = g_args.async_mode;

        g_Dbs.db[0].superTbls[0].autoCreateTable = PRE_CREATE_SUBTBL;
        g_Dbs.db[0].superTbls[0].childTblExists = TBL_NO_EXISTS;
        g_Dbs.db[0].superTbls[0].disorderRange = g_args.disorderRange;
        g_Dbs.db[0].superTbls[0].disorderRatio = g_args.disorderRatio;
        tstrncpy(g_Dbs.db[0].superTbls[0].childTblPrefix,
                g_args.tb_prefix, TBNAME_PREFIX_LEN);
        tstrncpy(g_Dbs.db[0].superTbls[0].dataSource, "rand", SMALL_BUFF_LEN);

        if (g_args.iface == INTERFACE_BUT) {
            g_Dbs.db[0].superTbls[0].iface = TAOSC_IFACE;
        } else {
            g_Dbs.db[0].superTbls[0].iface = g_args.iface;
        }
        tstrncpy(g_Dbs.db[0].superTbls[0].startTimestamp,
                "2017-07-14 10:40:00.000", MAX_TB_NAME_SIZE);
        g_Dbs.db[0].superTbls[0].timeStampStep = g_args.timestamp_step;

        g_Dbs.db[0].superTbls[0].insertRows = g_args.insertRows;
        g_Dbs.db[0].superTbls[0].maxSqlLen = g_args.max_sql_len;

        g_Dbs.db[0].superTbls[0].columnCount = 0;
        for (int i = 0; i < MAX_NUM_COLUMNS; i++) {
            if (data_type[i] == TSDB_DATA_TYPE_NULL) {
                break;
            }

            g_Dbs.db[0].superTbls[0].columns[i].data_type = data_type[i];
            tstrncpy(g_Dbs.db[0].superTbls[0].columns[i].dataType,
                    dataType[i], min(DATATYPE_BUFF_LEN, strlen(dataType[i]) + 1));
            if (1 == regexMatch(dataType[i], "^(NCHAR|BINARY)(\\([1-9][0-9]*\\))$", REG_ICASE |
                    REG_EXTENDED)) {
                sscanf(dataType[i], "%[^(](%[^)]", type, length);
                g_Dbs.db[0].superTbls[0].columns[i].dataLen = atoi(length);
                tstrncpy(g_Dbs.db[0].superTbls[0].columns[i].dataType,
                    type, min(DATATYPE_BUFF_LEN, strlen(type) + 1));
            } else {
		    switch (g_Dbs.db[0].superTbls[0].columns[i].data_type){
		    	case TSDB_DATA_TYPE_BOOL:
			case TSDB_DATA_TYPE_UTINYINT:
			case TSDB_DATA_TYPE_TINYINT:
				g_Dbs.db[0].superTbls[0].columns[i].dataLen = sizeof(char);
				break;
			case TSDB_DATA_TYPE_SMALLINT:
			case TSDB_DATA_TYPE_USMALLINT:
				g_Dbs.db[0].superTbls[0].columns[i].dataLen = sizeof(int16_t);
				break;
			case TSDB_DATA_TYPE_INT:
			case TSDB_DATA_TYPE_UINT:
				g_Dbs.db[0].superTbls[0].columns[i].dataLen = sizeof(int32_t);
				break;
			case TSDB_DATA_TYPE_TIMESTAMP:
			case TSDB_DATA_TYPE_BIGINT:
			case TSDB_DATA_TYPE_UBIGINT:
				g_Dbs.db[0].superTbls[0].columns[i].dataLen = sizeof(int64_t);
				break;
			case TSDB_DATA_TYPE_FLOAT:
				g_Dbs.db[0].superTbls[0].columns[i].dataLen = sizeof(float);
				break;
			case TSDB_DATA_TYPE_DOUBLE:
				g_Dbs.db[0].superTbls[0].columns[i].dataLen = sizeof(double);
				break;
			default:
				g_Dbs.db[0].superTbls[0].columns[i].dataLen = g_args.binwidth;
				break;
		    }
                tstrncpy(g_Dbs.db[0].superTbls[0].columns[i].dataType,
                    dataType[i], min(DATATYPE_BUFF_LEN, strlen(dataType[i]) + 1));
            }
            g_Dbs.db[0].superTbls[0].columnCount++;
        }

        if (g_Dbs.db[0].superTbls[0].columnCount > g_args.columnCount) {
            g_Dbs.db[0].superTbls[0].columnCount = g_args.columnCount;
        } else {
            for (int i = g_Dbs.db[0].superTbls[0].columnCount;
                    i < g_args.columnCount; i++) {
                g_Dbs.db[0].superTbls[0].columns[i].data_type = TSDB_DATA_TYPE_INT;
                tstrncpy(g_Dbs.db[0].superTbls[0].columns[i].dataType,
                        "INT", min(DATATYPE_BUFF_LEN, strlen("INT") + 1));
                g_Dbs.db[0].superTbls[0].columns[i].dataLen = 0;
                g_Dbs.db[0].superTbls[0].columnCount++;
            }
        }

        tstrncpy(g_Dbs.db[0].superTbls[0].tags[0].dataType,
                "INT", min(DATATYPE_BUFF_LEN, strlen("INT") + 1));
        g_Dbs.db[0].superTbls[0].tags[0].dataLen = 0;

        tstrncpy(g_Dbs.db[0].superTbls[0].tags[1].dataType,
                "BINARY", min(DATATYPE_BUFF_LEN, strlen("BINARY") + 1));
        g_Dbs.db[0].superTbls[0].tags[1].dataLen = g_args.binwidth;
        g_Dbs.db[0].superTbls[0].tagCount = 2;
    } else {
        g_Dbs.threadCountForCreateTbl = g_args.nthreads;
        g_Dbs.db[0].superTbls[0].tagCount = 0;
    }
}

/* Function to do regular expression check */
static int regexMatch(const char *s, const char *reg, int cflags) {
    regex_t regex;
    char    msgbuf[100] = {0};

    /* Compile regular expression */
    if (regcomp(&regex, reg, cflags) != 0) {
        ERROR_EXIT("Fail to compile regex\n");
    }

    /* Execute regular expression */
    int reti = regexec(&regex, s, 0, NULL, 0);
    if (!reti) {
        regfree(&regex);
        return 1;
    } else if (reti == REG_NOMATCH) {
        regfree(&regex);
        return 0;
    } else {
        regerror(reti, &regex, msgbuf, sizeof(msgbuf));
        regfree(&regex);
        printf("Regex match failed: %s\n", msgbuf);
        exit(EXIT_FAILURE);
    }
    return 0;
}

static int isCommentLine(char *line) {
    if (line == NULL) return 1;

    return regexMatch(line, "^\\s*#.*", REG_EXTENDED);
}

static void querySqlFile(TAOS* taos, char* sqlFile)
{
    FILE *fp = fopen(sqlFile, "r");
    if (fp == NULL) {
        printf("failed to open file %s, reason:%s\n", sqlFile, strerror(errno));
        return;
    }

    int       read_len = 0;
    char *    cmd = calloc(1, TSDB_MAX_BYTES_PER_ROW);
    size_t    cmd_len = 0;
    char *    line = NULL;
    size_t    line_len = 0;

    double t = taosGetTimestampMs();

    while((read_len = tgetline(&line, &line_len, fp)) != -1) {
        if (read_len >= TSDB_MAX_BYTES_PER_ROW) continue;
        line[--read_len] = '\0';

        if (read_len == 0 || isCommentLine(line)) {  // line starts with #
            continue;
        }

        if (line[read_len - 1] == '\\') {
            line[read_len - 1] = ' ';
            memcpy(cmd + cmd_len, line, read_len);
            cmd_len += read_len;
            continue;
        }

        memcpy(cmd + cmd_len, line, read_len);
        if (0 != queryDbExec(taos, cmd, NO_INSERT_TYPE, false)) {
            errorPrint2("%s() LN%d, queryDbExec %s failed!\n",
                    __func__, __LINE__, cmd);
            tmfree(cmd);
            tmfree(line);
            tmfclose(fp);
            return;
        }
        memset(cmd, 0, TSDB_MAX_BYTES_PER_ROW);
        cmd_len = 0;
    }

    t = taosGetTimestampMs() - t;
    printf("run %s took %.6f second(s)\n\n", sqlFile, t);

    tmfree(cmd);
    tmfree(line);
    tmfclose(fp);
    return;
}

static void testMetaFile() {
    if (INSERT_TEST == g_args.test_mode) {
        if (g_Dbs.cfgDir[0])
            taos_options(TSDB_OPTION_CONFIGDIR, g_Dbs.cfgDir);

        insertTestProcess();

    } else if (QUERY_TEST == g_args.test_mode) {
        if (g_queryInfo.cfgDir[0])
            taos_options(TSDB_OPTION_CONFIGDIR, g_queryInfo.cfgDir);

        queryTestProcess();

    } else if (SUBSCRIBE_TEST == g_args.test_mode) {
        if (g_queryInfo.cfgDir[0])
            taos_options(TSDB_OPTION_CONFIGDIR, g_queryInfo.cfgDir);

        subscribeTestProcess();

    }  else {
        ;
    }
}

static void queryAggrFunc() {
    // query data

    pthread_t read_id;
    threadInfo *pThreadInfo = calloc(1, sizeof(threadInfo));
    assert(pThreadInfo);
    pThreadInfo->start_time = DEFAULT_START_TIME;  // 2017-07-14 10:40:00.000
    pThreadInfo->start_table_from = 0;

    if (g_args.use_metric) {
        pThreadInfo->ntables = g_Dbs.db[0].superTbls[0].childTblCount;
        pThreadInfo->end_table_to = g_Dbs.db[0].superTbls[0].childTblCount - 1;
        pThreadInfo->stbInfo = &g_Dbs.db[0].superTbls[0];
        tstrncpy(pThreadInfo->tb_prefix,
                g_Dbs.db[0].superTbls[0].childTblPrefix, TBNAME_PREFIX_LEN);
    } else {
        pThreadInfo->ntables = g_args.ntables;
        pThreadInfo->end_table_to = g_args.ntables -1;
        tstrncpy(pThreadInfo->tb_prefix, g_args.tb_prefix, TSDB_TABLE_NAME_LEN);
    }

    pThreadInfo->taos = taos_connect(
            g_Dbs.host,
            g_Dbs.user,
            g_Dbs.password,
            g_Dbs.db[0].dbName,
            g_Dbs.port);
    if (pThreadInfo->taos == NULL) {
        free(pThreadInfo);
        errorPrint2("Failed to connect to TDengine, reason:%s\n",
                taos_errstr(NULL));
        exit(EXIT_FAILURE);
    }

    tstrncpy(pThreadInfo->filePath, g_Dbs.resultFile, MAX_FILE_NAME_LEN);

    if (!g_Dbs.use_metric) {
        pthread_create(&read_id, NULL, queryNtableAggrFunc, pThreadInfo);
    } else {
        pthread_create(&read_id, NULL, queryStableAggrFunc, pThreadInfo);
    }
    pthread_join(read_id, NULL);
    taos_close(pThreadInfo->taos);
    free(pThreadInfo);
}

static void testCmdLine() {

    if (strlen(configDir)) {
        wordexp_t full_path;
        if (wordexp(configDir, &full_path, 0) != 0) {
            errorPrint("Invalid path %s\n", configDir);
            return;
        }
        taos_options(TSDB_OPTION_CONFIGDIR, full_path.we_wordv[0]);
        wordfree(&full_path);
    }

    g_args.test_mode = INSERT_TEST;
    insertTestProcess();

    if (g_Dbs.aggr_func) {
        queryAggrFunc();
    }
}

int main(int argc, char *argv[]) {
    parse_args(argc, argv, &g_args);

    debugPrint("meta file: %s\n", g_args.metaFile);

    if (g_args.metaFile) {
        g_totalChildTables = 0;

        if (false == getInfoFromJsonFile(g_args.metaFile)) {
            printf("Failed to read %s\n", g_args.metaFile);
            return 1;
        }

        testMetaFile();
    } else {
        memset(&g_Dbs, 0, sizeof(SDbs));
        g_Dbs.db = calloc(1, sizeof(SDataBase));
        assert(g_Dbs.db);
        g_Dbs.db[0].superTbls = calloc(1, sizeof(SSuperTable));
        assert(g_Dbs.db[0].superTbls);
        setParaFromArg();

        if (NULL != g_args.sqlFile) {
            TAOS* qtaos = taos_connect(
                    g_Dbs.host,
                    g_Dbs.user,
                    g_Dbs.password,
                    g_Dbs.db[0].dbName,
                    g_Dbs.port);
            querySqlFile(qtaos, g_args.sqlFile);
            taos_close(qtaos);

        } else {
            testCmdLine();
        }

        if (g_dupstr)
            free(g_dupstr);
    }
    postFreeResource();

    return 0;
}
