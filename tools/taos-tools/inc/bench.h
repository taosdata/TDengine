/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the MIT license as published by the Free Software
 * Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef INC_BENCH_H_
#define INC_BENCH_H_

#define _GNU_SOURCE
#define CURL_STATICLIB
#define ALLOW_FORBID_FUNC

#ifdef LINUX

#ifndef _ALPINE
#include <error.h>
#endif

#include <semaphore.h>
#include <stdbool.h>
#include <time.h>
#include <unistd.h>
#include <wordexp.h>
#include <netdb.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <syscall.h>
#include <sys/ioctl.h>
#include <signal.h>

#elif DARWIN
#include <argp.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <netdb.h>
#else
#include <winsock2.h>
#endif

#include <limits.h>
#include <regex.h>
#include <stdio.h>
#include <assert.h>
#include <toolscJson.h>
#include <ctype.h>
#include <inttypes.h>
#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdarg.h>

#include <taos.h>
#include <toolsdef.h>
#include <taoserror.h>

#ifdef WEBSOCKET
#include <taosws.h>
#endif

#ifdef WINDOWS
#define _CRT_RAND_S
#include <windows.h>
#include <winsock2.h>
#define SHUT_WR   SD_SEND

typedef unsigned __int32 uint32_t;

#pragma comment(lib, "ws2_32.lib")
// Some old MinGW/CYGWIN distributions don't define this:
#ifndef ENABLE_VIRTUAL_TERMINAL_PROCESSING
#define ENABLE_VIRTUAL_TERMINAL_PROCESSING 0x0004
#endif  // ENABLE_VIRTUAL_TERMINAL_PROCESSING
#else
#define SOCKET_ERROR      -1
#endif


#ifndef TSDB_DATA_TYPE_VARCHAR
#define TSDB_DATA_TYPE_VARCHAR 8
#endif

#ifndef TSDB_DATA_TYPE_VARBINARY
#define TSDB_DATA_TYPE_VARBINARY 16
#endif

#ifndef TSDB_DATA_TYPE_DECIMAL
#define TSDB_DATA_TYPE_DECIMAL 17
#endif

#ifndef TSDB_DATA_TYPE_MEDIUMBLOB
#define TSDB_DATA_TYPE_MEDIUMBLOB 19
#endif

#ifndef TSDB_DATA_TYPE_MAX
#define TSDB_DATA_TYPE_MAX        20
#endif

#define REQ_EXTRA_BUF_LEN         1024
#define RESP_BUF_LEN              4096
#define SHORT_1K_SQL_BUFF_LEN     1024
#define URL_BUFF_LEN              1024

#define STR_INSERT_INTO           "INSERT INTO "

// 16*MAX_COLUMNS + (192+32)*2 + insert into
#define HEAD_BUFF_LEN         (TSDB_MAX_COLUMNS * 24)

#define FETCH_BUFFER_SIZE     (100 * TSDB_MAX_ALLOWED_SQL_LEN)
#define COND_BUF_LEN          (TSDB_MAX_ALLOWED_SQL_LEN - 30)

#define OPT_ABORT                 1    /* –abort */
#define MAX_RECORDS_PER_REQ       65536
#define DEFAULT_START_TIME        1500000000000
#define TELNET_TCP_PORT           6046
#define INT_BUFF_LEN              12
#define BIGINT_BUFF_LEN     21
#define SMALLINT_BUFF_LEN   8
#define TINYINT_BUFF_LEN    6
#define BOOL_BUFF_LEN       6
#define FLOAT_BUFF_LEN      22
#define DOUBLE_BUFF_LEN     42
#define JSON_BUFF_LEN       20
#define TIMESTAMP_BUFF_LEN  21
#define PRINT_STAT_INTERVAL 30 * 1000

#define MAX_QUERY_SQL_COUNT 100

#define MAX_JSON_BUFF 6400000

#define INPUT_BUF_LEN         256
#define EXTRA_SQL_LEN         256
#define DATATYPE_BUFF_LEN     (TINY_BUFF_LEN * 3)
#define SML_MAX_BATCH          65536 * 32
#define DEFAULT_NTHREADS       8

#define DEFAULT_CHILDTABLES    10000
#define DEFAULT_PORT           6030
#define DEFAULT_REST_PORT      6041
#define DEFAULT_DATABASE       "test"
#define DEFAULT_TB_PREFIX      "d"
#define DEFAULT_OUTPUT         "./output.txt"
#define DEFAULT_BINWIDTH       64
#define DEFAULT_REPLICA        1
#define DEFAULT_CFGNAME_LEN    10
#define DEFAULT_PREPARED_RAND  10000
#define DEFAULT_REQ_PER_REQ    30000
#define DEFAULT_INSERT_ROWS    10000
#define DEFAULT_DISORDER_RANGE 1000
#define DEFAULT_CREATE_BATCH   10
#define DEFAULT_SUB_INTERVAL   10000
#define DEFAULT_QUERY_INTERVAL 10000
#define BARRAY_MIN_SIZE             8
#define SML_LINE_SQL_SYNTAX_OFFSET  7

// tdengine define macro
#define TSDB_DEFAULT_DURATION_PER_FILE  (10 * 1440)

#define TS_COL_NAME "ts"
#define  RD(max)      ((max ==0)? 1 : (taosRandom() % (max)))
#define SML_JSON_TAOS_FORMAT    255



#define BENCH_FILE              \
    "(**IMPORTANT**) Set JSON configuration file "  \
    "(all options are going to read from this JSON file), " \
    "which is mutually exclusive with other commandline options. "  \
    "You can find examples from official repository. "
#define BENCH_CFG_DIR "Configuration directory."
#define BENCH_HOST                \
    "Specify FQDN to connect server, default is localhost."
#define BENCH_PORT                \
    "The TCP/IP port number to use for the connection, default is 6030."
#define BENCH_MODE                \
    "insert mode, default is taosc, options: taosc|rest|stmt|sml"
#define BENCH_USER                \
    "The user name to use when connecting to the server, default is root."
#define BENCH_PASS                \
    "The password to use when connecting to the server, default is taosdata."
#define BENCH_OUTPUT  "The path of result output file, default is ./output.txt."
#define BENCH_THREAD  "The number of thread when insert data, default is 8."
#define BENCH_INTERVAL            \
    "Insert interval for interlace mode in milliseconds, default is 0."
#define BENCH_STEP  "Timestamp step in milliseconds, default is 1."
#define ANGLE_STEP  "Angle step in milliseconds, default is 1."
#define BENCH_SUPPLEMENT          \
    "Supplementally insert data without create "  \
    "database and table, optional, default is off."
#define BENCH_START_TIMESTAMP     \
    "Specify timestamp to insert data. Optional, "  \
    "default is 1500000000000 (2017-07-14 10:40:00.000)."
#define BENCH_INTERLACE           \
    "The number of interlace rows insert into tables, default is 0."
#define BENCH_BATCH               \
    "Number of records in each insert request, default is 30000."
#define BENCH_TABLE "Number of child tables, default is 10000."
#define BENCH_ROWS  "Number of records for each table, default is 10000."
#define BENCH_DATABASE  "Name of database, default is test."
#define BENCH_COLS_NUM            \
    "Number of INT data type columns in table, default is 0."
#define BENCH_PARTIAL_COL_NUM     \
    "Specify first numbers of columns has data. " \
    "Rest of columns' data are NULL. Default is all columns have data"
#define BENCH_TAGS  "Data type of tables' tags, default is INT,BINARY(16)."
#define BENCH_COLS  "Data type of tables' cols, default is FLOAT,INT,FLOAT."
#define BENCH_WIDTH     \
    "The default length of nchar and binary if not specified, default is 64."
#define BENCH_PREFIX  "Prefix of child table name, default is d."
#define BENCH_ESCAPE    \
    "Use escape character in stable and child table name, optional."
#define BENCH_CHINESE   \
    "Nchar and binary are basic unicode chinese characters, optional."
#define BENCH_NORMAL  "Only create normal table without super table, optional."
#define BENCH_RANDOM  "Each child table generates different random data, this option need much memory. ( all memory = childs count * prepared_rand)"
#define BENCH_AGGR  "Query aggregation function after insertion, optional."
#define BENCH_YES "Pass confirmation prompt to continue, optional."
#define BENCH_RANGE "Range of disordered timestamp, default is 1000."
#define BENCH_DISORDER    \
    "Ratio of inserting data with disorder timestamp, default is 0."
#define BENCH_REPLICA     \
    "The number of replica when create database, default is 1."
#define BENCH_DEBUG "Debug mode, optional."
#define BENCH_PERFORMANCE "Performance mode, optional."
#define BENCH_PREPARE "Random data source size, default is 10000."
#define BENCH_VGROUPS "Specify Vgroups number for creating database, "    \
                        "only valid with daemon version 3.0+"
#define BENCH_VERSION "Print program version."
#define BENCH_KEEPTRYING "Keep trying if failed to insert, default is no."
#define BENCH_TRYING_INTERVAL     \
    "Specify interval between keep trying insert. " \
    "Valid value is a positive number. Only valid " \
    "when keep trying be enabled."
#define BENCH_NODROP "Do not drop database."

#ifdef WEBSOCKET
#define BENCH_DSN "The dsn to connect the cloud service."
#define BENCH_TIMEOUT     \
    "The timeout wait on websocket query in seconds, default is 10."
#endif


#define debugPrint(fmt, ...)                                                \
    do {                                                                    \
        if (g_arguments->debug_print) {                                     \
            struct tm      Tm, *ptm;                                        \
            struct timeval timeSecs;                                        \
            time_t         curTime;                                         \
            toolsGetTimeOfDay(&timeSecs);                                   \
            curTime = timeSecs.tv_sec;                                      \
            ptm = toolsLocalTime(&curTime, &Tm);                            \
            fprintf(stdout, "[%02d/%02d %02d:%02d:%02d.%06d] ",             \
                    ptm->tm_mon + 1,                                        \
                    ptm->tm_mday, ptm->tm_hour, ptm->tm_min, ptm->tm_sec,   \
                    (int32_t)timeSecs.tv_usec);                             \
            fprintf(stdout, "DEBG: ");                                      \
            fprintf(stdout, "%s(%d) ", __FILE__, __LINE__);                 \
            fprintf(stdout, "" fmt, __VA_ARGS__);                           \
        }                                                                   \
    } while (0)

#define debugPrintWithLen(fmt, len, ...)                                    \
    do {                                                                    \
        if (g_arguments->debug_print) {                                     \
            struct tm      Tm, *ptm;                                        \
            struct timeval timeSecs;                                        \
            time_t         curTime;                                         \
            toolsGetTimeOfDay(&timeSecs);                                   \
            curTime = timeSecs.tv_sec;                                      \
            ptm = toolsLocalTime(&curTime, &Tm);                            \
            fnprintf(stdout, len, "[%02d/%02d %02d:%02d:%02d.%06d] ",       \
                    ptm->tm_mon + 1,                                        \
                    ptm->tm_mday, ptm->tm_hour, ptm->tm_min, ptm->tm_sec,   \
                    (int32_t)timeSecs.tv_usec);                             \
            fprintf(stdout, "DEBG: ");                                      \
            fprintf(stdout, "%s(%d) ", __FILE__, __LINE__);                 \
            fprintf(stdout, "" fmt, __VA_ARGS__);                           \
        }                                                                   \
    } while (0)

#define debugPrintJsonNoTime(json)                                          \
    do {                                                                    \
        if (g_arguments->debug_print) {                                     \
            char *out = tools_cJSON_PrintUnformatted(json);                 \
            fprintf(stdout, "JSON: %s\n", out);                             \
            free(out);                                                      \
        }                                                                   \
    } while (0)

#define debugPrintNoTimestamp(fmt, ...)                                     \
    do {                                                                    \
        if (g_arguments->debug_print) {                                     \
            fprintf(stdout, "" fmt, __VA_ARGS__);                           \
        }                                                                   \
    } while (0)

#define infoPrintNoTimestamp(fmt, ...)                                      \
    do {                                                                    \
        fprintf(stdout, "" fmt, __VA_ARGS__);                               \
    } while (0)

#define infoPrintNoTimestampToFile(fp, fmt, ...)                            \
    do {                                                                    \
        fprintf(fp, "" fmt, __VA_ARGS__);                                   \
    } while (0)

#define infoPrint(fmt, ...)                                                 \
    do {                                                                    \
        struct tm      Tm, *ptm;                                            \
        struct timeval timeSecs;                                            \
        time_t         curTime;                                             \
        toolsGetTimeOfDay(&timeSecs);                                       \
        curTime = timeSecs.tv_sec;                                          \
        ptm = toolsLocalTime(&curTime, &Tm);                                \
        fprintf(stdout, "[%02d/%02d %02d:%02d:%02d.%06d] ",                 \
                ptm->tm_mon + 1,                                            \
                ptm->tm_mday, ptm->tm_hour, ptm->tm_min, ptm->tm_sec,       \
                (int32_t)timeSecs.tv_usec);                                 \
        fprintf(stdout, "INFO: " fmt, __VA_ARGS__);                         \
    } while (0)

#define infoPrintToFile(fp, fmt, ...)                                    \
    do {                                                                 \
        struct tm      Tm, *ptm;                                         \
        struct timeval timeSecs;                                         \
        time_t         curTime;                                          \
        toolsGetTimeOfDay(&timeSecs);                                    \
        curTime = timeSecs.tv_sec;                                       \
        ptm = toolsLocalTime(&curTime, &Tm);                             \
        fprintf(fp, "[%02d/%02d %02d:%02d:%02d.%06d] ", ptm->tm_mon + 1, \
                ptm->tm_mday, ptm->tm_hour, ptm->tm_min, ptm->tm_sec,    \
                (int32_t)timeSecs.tv_usec);                              \
        fprintf(fp, "INFO: " fmt, __VA_ARGS__);                          \
    } while (0)

#define perfPrint(fmt, ...)                                                 \
    do {                                                                    \
        if (g_arguments->performance_print) {                               \
            struct tm      Tm, *ptm;                                        \
            struct timeval timeSecs;                                        \
            time_t         curTime;                                         \
            toolsGetTimeOfDay(&timeSecs);                                   \
            curTime = timeSecs.tv_sec;                                      \
            ptm = toolsLocalTime(&curTime, &Tm);                            \
            fprintf(stderr, "[%02d/%02d %02d:%02d:%02d.%06d] ",             \
                    ptm->tm_mon + 1,                                        \
                    ptm->tm_mday, ptm->tm_hour, ptm->tm_min, ptm->tm_sec,   \
                    (int32_t)timeSecs.tv_usec);                             \
            fprintf(stderr, "PERF: " fmt, __VA_ARGS__);                     \
            if (g_arguments->fpOfInsertResult && !g_arguments->terminate) { \
                fprintf(g_arguments->fpOfInsertResult,                      \
                        "[%02d/%02d %02d:%02d:%02d.%06d] ",                 \
                        ptm->tm_mon + 1,                                    \
                        ptm->tm_mday, ptm->tm_hour, ptm->tm_min,            \
                        ptm->tm_sec,                                        \
                        (int32_t)timeSecs.tv_usec);                         \
                fprintf(g_arguments->fpOfInsertResult, "PERF: ");           \
                fprintf(g_arguments->fpOfInsertResult,                      \
                        "" fmt, __VA_ARGS__);                               \
            }                                                               \
        }                                                                   \
    } while (0)

#define errorPrint(fmt, ...)                                                \
    do {                                                                    \
        struct tm      Tm, *ptm;                                            \
        struct timeval timeSecs;                                            \
        time_t         curTime;                                             \
        toolsGetTimeOfDay(&timeSecs);                                       \
        curTime = timeSecs.tv_sec;                                          \
        ptm = toolsLocalTime(&curTime, &Tm);                                \
        fprintf(stderr, "[%02d/%02d %02d:%02d:%02d.%06d] ",                 \
                ptm->tm_mon + 1,                                            \
                ptm->tm_mday, ptm->tm_hour, ptm->tm_min, ptm->tm_sec,       \
                (int32_t)timeSecs.tv_usec);                                 \
        fprintf(stderr, "\033[31m");                                        \
        fprintf(stderr, "ERROR: ");                                         \
        if (g_arguments->debug_print) {                                     \
            fprintf(stderr, "%s(%d) ", __FILE__, __LINE__);                 \
        }                                                                   \
        fprintf(stderr, "" fmt, __VA_ARGS__);                               \
        fprintf(stderr, "\033[0m");                                         \
        if (g_arguments->fpOfInsertResult && !g_arguments->terminate) {     \
            fprintf(g_arguments->fpOfInsertResult,                          \
                    "[%02d/%02d %02d:%02d:%02d.%06d] ", ptm->tm_mon + 1,    \
                ptm->tm_mday, ptm->tm_hour, ptm->tm_min, ptm->tm_sec,       \
                (int32_t)timeSecs.tv_usec);                                 \
            fprintf(g_arguments->fpOfInsertResult, "ERROR: ");              \
            fprintf(g_arguments->fpOfInsertResult, "" fmt, __VA_ARGS__);    \
        }                                                                   \
    } while (0)

#define warnPrint(fmt, ...)                                                 \
    do {                                                                    \
        struct tm      Tm, *ptm;                                            \
        struct timeval timeSecs;                                            \
        time_t         curTime;                                             \
        toolsGetTimeOfDay(&timeSecs);                                       \
        curTime = timeSecs.tv_sec;                                          \
        ptm = toolsLocalTime(&curTime, &Tm);                                \
        fprintf(stderr, "[%02d/%02d %02d:%02d:%02d.%06d] ",                 \
                ptm->tm_mon + 1,                                            \
                ptm->tm_mday, ptm->tm_hour, ptm->tm_min, ptm->tm_sec,       \
                (int32_t)timeSecs.tv_usec);                                 \
        fprintf(stderr, "\033[33m");                                        \
        fprintf(stderr, "WARN: ");                                          \
        if (g_arguments->debug_print) {                                     \
            fprintf(stderr, "%s(%d) ", __FILE__, __LINE__);                 \
        }                                                                   \
        fprintf(stderr, "" fmt, __VA_ARGS__);                               \
        fprintf(stderr, "\033[0m");                                         \
        if (g_arguments->fpOfInsertResult && !g_arguments->terminate) {     \
            fprintf(g_arguments->fpOfInsertResult,                          \
                    "[%02d/%02d %02d:%02d:%02d.%06d] ", ptm->tm_mon + 1,    \
                ptm->tm_mday, ptm->tm_hour, ptm->tm_min, ptm->tm_sec,       \
                (int32_t)timeSecs.tv_usec);                                 \
            fprintf(g_arguments->fpOfInsertResult, "WARN: ");               \
            fprintf(g_arguments->fpOfInsertResult, "" fmt, __VA_ARGS__);    \
        }                                                                   \
    } while (0)

#define succPrint(fmt, ...)                                                 \
    do {                                                                    \
        struct tm      Tm, *ptm;                                            \
        struct timeval timeSecs;                                            \
        time_t         curTime;                                             \
        toolsGetTimeOfDay(&timeSecs);                                       \
        curTime = timeSecs.tv_sec;                                          \
        ptm = toolsLocalTime(&curTime, &Tm);                                \
        fprintf(stderr, "[%02d/%02d %02d:%02d:%02d.%06d] ",                 \
                ptm->tm_mon + 1,                                            \
                ptm->tm_mday, ptm->tm_hour, ptm->tm_min, ptm->tm_sec,       \
                (int32_t)timeSecs.tv_usec);                                 \
        fprintf(stderr, "\033[32m");                                        \
        fprintf(stderr, "SUCC: ");                                          \
        if (g_arguments->debug_print) {                                     \
            fprintf(stderr, "%s(%d) ", __FILE__, __LINE__);                 \
        }                                                                   \
        fprintf(stderr, "" fmt, __VA_ARGS__);                               \
        fprintf(stderr, "\033[0m");                                         \
        if (g_arguments->fpOfInsertResult && !g_arguments->terminate) {     \
            fprintf(g_arguments->fpOfInsertResult,                          \
                    "[%02d/%02d %02d:%02d:%02d.%06d] ", ptm->tm_mon + 1,    \
                ptm->tm_mday, ptm->tm_hour, ptm->tm_min, ptm->tm_sec,       \
                (int32_t)timeSecs.tv_usec);                                 \
            fprintf(g_arguments->fpOfInsertResult, "SUCC: ");               \
            fprintf(g_arguments->fpOfInsertResult, "" fmt, __VA_ARGS__);    \
        }                                                                   \
    } while (0)

enum TEST_MODE {
    INSERT_TEST,     // 0
    QUERY_TEST,      // 1
    SUBSCRIBE_TEST,  // 2
};

enum enumSYNC_MODE { SYNC_MODE, ASYNC_MODE, MODE_BUT };

enum enum_TAOS_INTERFACE {
    TAOSC_IFACE,
    REST_IFACE,
    STMT_IFACE,
    SML_IFACE,
    SML_REST_IFACE,
    INTERFACE_BUT
};

typedef enum enumQUERY_CLASS {
    SPECIFIED_CLASS,
    STABLE_CLASS,
    CLASS_BUT
} QUERY_CLASS;

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

// -----------------------------------------SHOW TABLES CONFIGURE
// -------------------------------------

enum _describe_table_index {
    TSDB_DESCRIBE_METRIC_FIELD_INDEX,
    TSDB_DESCRIBE_METRIC_TYPE_INDEX,
    TSDB_DESCRIBE_METRIC_LENGTH_INDEX,
    TSDB_DESCRIBE_METRIC_NOTE_INDEX,
    TSDB_MAX_DESCRIBE_METRIC
};

typedef struct BArray {
    size_t   size;
    uint64_t capacity;
    uint64_t elemSize;
    void*    pData;
} BArray;

typedef struct {
    uint64_t magic;
    uint64_t custom;
    uint64_t len;
    uint64_t cap;
    char data[];
} dstr;

static const int DS_HEADER_SIZE = sizeof(uint64_t) * 4;
static const uint64_t MAGIC_NUMBER = 0xDCDC52545344DADA;

static const int OFF_MAGIC     = -4;
static const int OFF_CUSTOM     = -3;
static const int OFF_LEN     = -2;
static const int OFF_CAP     = -1;

typedef struct SStmtData {
    void    *data;
    char    *is_null;
} StmtData;

typedef struct SChildField {
    StmtData stmtData;
} ChildField;

#define PI  3.141592654
#define ATOR(x)  (x*3.141592654/180)

#define FUNTYPE_NONE  0
#define FUNTYPE_SIN   1
#define FUNTYPE_COS   2
#define FUNTYPE_COUNT 3
#define FUNTYPE_SAW   4
#define FUNTYPE_SQUARE 5
#define FUNTYPE_TRI    6

#define FUNTYPE_CNT   7

#define TAG_BATCH_COUNT 100

#define GEN_RANDOM  0
#define GEN_ORDER   1

#define COL_GEN (field->gen == GEN_ORDER ? k : taosRandom())

#define tmpInt8(field)    tmpInt8Impl(field, 0)
#define tmpUint8(field)   tmpUint8Impl(field, 0)
#define tmpInt16(field)   tmpInt16Impl(field, 0)
#define tmpUint16(field)  tmpUint16Impl(field, 0)

#define tmpInt32(field)   tmpInt32Impl (field,0,0,0)
#define tmpUint32(field)  tmpUint32Impl(field,0,0,0)
#define tmpInt64(field)   tmpInt64Impl (field,0,0)
#define tmpUint64(field)  tmpUint64Impl(field,0,0)
#define tmpFloat(field)   tmpFloatImpl (field,0,0,0)
#define tmpDouble(field)  tmpDoubleImpl(field,0,0)

#define COMP_NAME_LEN 32

typedef struct SField {
    uint8_t  type;
    char     name[TSDB_COL_NAME_LEN + 1];
    uint32_t length;
    bool     none;
    bool     null;
    StmtData stmtData;
    int64_t  max;
    int64_t  min;
    tools_cJSON *  values;

    // fun
    uint8_t  funType;
    float    multiple;
    float    addend;
    float    base;
    int32_t  random;

    int32_t    period;
    int32_t    offset;
    int32_t    step;

    bool     sma;
    bool     fillNull;
    uint8_t   gen; // see GEN_ define

    // compress
    char     encode[COMP_NAME_LEN];
    char     compress[COMP_NAME_LEN];
    char     level[COMP_NAME_LEN];

} Field;

typedef struct STSMA {
    char* name;
    char* func;
    char* interval;
    char* sliding;
    int   start_when_inserted;
    char* custom;
    bool  done;
} TSMA;

// generate row data rule
#define RULE_OLD           0  // old generator method
#define RULE_MIX_RANDOM    1  // old data mix update delete ratio
#define RULE_MIX_ALL       2  // mix with all var data
#define RULE_MIX_TS_CALC   3  // ts calc other column
#define RULE_MIX_FIX_VALUE 4  // fixed value with give

// define suit
#define SUIT_DATAPOS_MEM       1
#define SUIT_DATAPOS_STT       2
#define SUIT_DATAPOS_FILE      3
#define SUIT_DATAPOS_MUL_FILE  4
#define SUIT_DATAPOS_MIX       5

enum CONTINUE_IF_FAIL_MODE {
    NO_IF_FAILED,     // 0
    YES_IF_FAILED,    // 1
    SMART_IF_FAILED,  // 2
};

typedef struct SChildTable_S {
    char*     name;
    bool      useOwnSample;
    char      *sampleDataBuf;
    uint64_t  insertRows;
    BArray    *childCols;
    int64_t   ts;  // record child table ts
    int32_t   pkCur;
    int32_t   pkCnt;
} SChildTable;

#define PRIMARY_KEY "PRIMARY KEY"
typedef struct SSuperTable_S {
    char      *stbName;
    bool      random_data_source;  // rand_gen or sample
    bool      use_metric;
    char      *childTblPrefix;
    char      *childTblSample;
    bool      childTblExists;
    uint64_t  childTblCount;
    uint64_t  batchTblCreatingNum;  // 0: no batch,  > 0: batch table number in
    char     *batchTblCreatingNumbers;  // NULL: no numbers
    BArray   *batchTblCreatingNumbersArray;
    char     *batchTblCreatingIntervals;  // NULL: no interval
    BArray   *batchTblCreatingIntervalsArray;
                                   // one sql
    bool      autoTblCreating;
    uint16_t  iface;  // 0: taosc, 1: rest, 2: stmt
    uint16_t  lineProtocol;
    int64_t   childTblLimit;
    int64_t   childTblOffset;
    int64_t   childTblFrom;
    int64_t   childTblTo;
    enum CONTINUE_IF_FAIL_MODE continueIfFail;

    //  int          multiThreadWriteOneTbl;  // 0: no, 1: yes
    uint32_t  interlaceRows;  //
    int       disorderRatio;  // 0: no disorder, >0: x%
    int       disorderRange;  // ms, us or ns. according to database precision

    // ratio
    uint8_t   disRatio;   // disorder ratio 0 ~ 100 %
    uint8_t   updRatio;   // update ratio   0 ~ 100 %
    uint8_t   delRatio;   // delete ratio   0 ~ 100 %

    // range
    uint64_t  disRange;  // disorder range
    uint64_t  updRange;  // update range
    uint64_t  delRange;  // delete range

    // generate row value rule see pre RULE_ define
    uint8_t   genRowRule;

    // data position
    uint8_t   dataPos;  //  see define DATAPOS_

    uint32_t  fillIntervalUpd;  // fill Upd interval rows cnt
    uint32_t  fillIntervalDis;  // fill Dis interval rows cnt

    // binary prefix
    char      *binaryPrefex;
    // nchar prefix
    char      *ncharPrefex;

    // random write future time
    bool      useNow;
    bool      writeFuture;
    int32_t   durMinute;  // passed database->durMinute
    int32_t   checkInterval;  // check correct interval

    int64_t   max_sql_len;
    uint64_t  insert_interval;
    uint64_t  insertRows;
    uint64_t  timestamp_step;
    uint64_t  angle_step;
    int64_t   startTimestamp;
    int64_t   startFillbackTime;
    int64_t   specifiedColumns;
    char      sampleFile[MAX_FILE_NAME_LEN];
    char      tagsFile[MAX_FILE_NAME_LEN];
    uint32_t  partialColNum;
    uint32_t  partialColFrom;
    char      *partialColNameBuf;
    BArray    *cols;
    BArray    *tags;
    BArray    *tsmas;
    SChildTable   **childTblArray;
    char      *colsOfCreateChildTable;
    uint32_t  lenOfTags;
    uint32_t  lenOfCols;

    char      *sampleDataBuf;
    bool      useSampleTs;
    bool      tcpTransfer;
    bool      non_stop;
    bool      autoFillback; // "start_fillback_time" item set "auto"
    char      *calcNow;      // need calculate now timestamp expression
    char      *comment;
    int       delay;
    int       file_factor;
    char      *rollup;
    char      *max_delay;
    char      *watermark;
    int       ttl;
    int32_t   keep_trying;
    uint32_t  trying_interval;
    // primary key
    bool primary_key;
    int  repeat_ts_min;
    int  repeat_ts_max;

    // execute sqls after create super table
    char **sqls;
} SSuperTable;

typedef struct SDbCfg_S {
    char*   name;
    char*   valuestring;
    int     valueint;
} SDbCfg;

typedef struct SSTREAM_S {
    char stream_name[TSDB_TABLE_NAME_LEN];
    char stream_stb[TSDB_TABLE_NAME_LEN];
    char stream_stb_field[TSDB_DEFAULT_PKT_SIZE];
    char stream_tag_field[TSDB_DEFAULT_PKT_SIZE];
    char subtable[TSDB_DEFAULT_PKT_SIZE];
    char trigger_mode[BIGINT_BUFF_LEN];
    char watermark[BIGINT_BUFF_LEN];
    char ignore_expired[BIGINT_BUFF_LEN];
    char ignore_update[BIGINT_BUFF_LEN];
    char fill_history[BIGINT_BUFF_LEN];
    char source_sql[TSDB_DEFAULT_PKT_SIZE];
    bool drop;
} SSTREAM;

#ifdef TD_VER_COMPATIBLE_3_0_0_0
typedef struct SVGroup_S {
    int32_t       vgId;
    uint64_t      tbCountPerVgId;
    SChildTable   **childTblArray;
    uint64_t      tbOffset;  // internal use
} SVGroup;
#endif  // TD_VER_COMPATIBLE_3_0_0_0
        //
typedef struct SDataBase_S {
    char *      dbName;
    bool        drop;  // 0: use exists, 1: if exists, drop then new create
    int         precision;
    int         sml_precision;
    int         durMinute;  // duration minutes
    BArray     *cfgs;
    BArray     *superTbls;
#ifdef TD_VER_COMPATIBLE_3_0_0_0
    int32_t     vgroups;
    BArray      *vgArray;
#endif  // TD_VER_COMPATIBLE_3_0_0_0
    bool        flush;
} SDataBase;

typedef struct SSQL_S {
    char *command;
    char result[MAX_FILE_NAME_LEN];
    int64_t* delay_list;
} SSQL;

typedef struct SpecifiedQueryInfo_S {
    uint64_t  queryInterval;  // 0: unlimited  > 0   loop/s
    uint64_t  queryTimes;
    uint32_t  concurrent;
    uint32_t  asyncMode;          // 0: sync, 1: async
    uint64_t  subscribeInterval;  // ms
    uint64_t  subscribeTimes;  // ms
    bool      subscribeRestart;
    int       subscribeKeepProgress;
    BArray*   sqls;
    int       resubAfterConsume[MAX_QUERY_SQL_COUNT];
    int       endAfterConsume[MAX_QUERY_SQL_COUNT];
    TAOS_SUB *tsub[MAX_QUERY_SQL_COUNT];
    char      topic[MAX_QUERY_SQL_COUNT][32];
    int       consumed[MAX_QUERY_SQL_COUNT];
    TAOS_RES *res[MAX_QUERY_SQL_COUNT];
    uint64_t  totalQueried;
    bool      mixed_query;
} SpecifiedQueryInfo;

typedef struct SuperQueryInfo_S {
    char      stbName[TSDB_TABLE_NAME_LEN];
    uint64_t  queryInterval;  // 0: unlimited  > 0   loop/s
    uint64_t  queryTimes;
    uint32_t  threadCnt;
    uint32_t  asyncMode;          // 0: sync, 1: async
    uint64_t  subscribeInterval;  // ms
    uint64_t  subscribeTimes;  // ms
    bool      subscribeRestart;
    int       subscribeKeepProgress;
    int64_t   childTblCount;
    int       sqlCount;
    char      sql[MAX_QUERY_SQL_COUNT][TSDB_MAX_ALLOWED_SQL_LEN + 1];
    char      result[MAX_QUERY_SQL_COUNT][MAX_FILE_NAME_LEN];
    int       resubAfterConsume;
    int       endAfterConsume;
    TAOS_SUB *tsub[MAX_QUERY_SQL_COUNT];
    char **   childTblName;
    uint64_t  totalQueried;
} SuperQueryInfo;

typedef struct SQueryMetaInfo_S {
    SpecifiedQueryInfo  specifiedQueryInfo;
    SuperQueryInfo      superQueryInfo;
    uint64_t            totalQueried;
    uint64_t            query_times;
    uint64_t            killQueryThreshold;
    int32_t             killQueryInterval;
    uint64_t            response_buffer;
    bool                reset_query_cache;
    uint16_t            iface;
    char*               dbName;
} SQueryMetaInfo;


typedef struct SConsumerInfo_S {
    uint32_t    concurrent;
    uint32_t    pollDelay;  // ms
    char*       groupId;
    char*       clientId;
    char*       autoOffsetReset;

	char*       createMode;
	char*       groupMode;

    char*       enableManualCommit;
    char*       enableAutoCommit;
    uint32_t    autoCommitIntervalMs;  // ms
    char*       enableHeartbeatBackground;
    char*       snapshotEnable;
    char*       msgWithTableName;
    char*       rowsFile;
    int32_t     expectRows;

    char        topicName[MAX_QUERY_SQL_COUNT][256];
    char        topicSql[MAX_QUERY_SQL_COUNT][256];
    int         topicCount;
} SConsumerInfo;

typedef struct STmqMetaInfo_S {
    SConsumerInfo      consumerInfo;
    uint16_t           iface;
} STmqMetaInfo;

typedef struct SArguments_S {
    uint8_t             taosc_version;
    char *              metaFile;
    int32_t             test_mode;
    char *              host;
    uint16_t            port;
    uint16_t            telnet_tcp_port;
    bool                host_auto;
    bool                port_auto;
    bool                port_inputted;
    bool                cfg_inputted;
    char *              user;
    char *              password;
    bool                answer_yes;
    bool                debug_print;
    bool                performance_print;
    bool                chinese;
    char *              output_file;
    uint32_t            binwidth;
    uint32_t            intColumnCount;
    uint32_t            nthreads;
    bool                nthreads_auto;
    uint32_t            table_threads;
    uint64_t            prepared_rand;
    uint32_t            reqPerReq;
    uint64_t            insert_interval;
    bool                demo_mode;
    bool                aggr_func;
    struct sockaddr_in  serv_addr;
    uint64_t            totalChildTables;
    uint64_t            actualChildTables;
    uint64_t            autoCreatedChildTables;
    uint64_t            existedChildTables;
    FILE *              fpOfInsertResult;
    BArray *            databases;
    BArray*             streams;
    char                base64_buf[INPUT_BUF_LEN];
#ifdef LINUX
    sem_t               cancelSem;
#endif
    bool                terminate;
    bool                in_prompt;
#ifdef WEBSOCKET
    int32_t             timeout;
    char*               dsn;
    bool                websocket;
#endif
    bool                supplementInsert;
    int64_t             startTimestamp;
    int32_t             partialColNum;
    int32_t             keep_trying;
    uint32_t            trying_interval;
    int                 iface;
    int                 rest_server_ver_major;
    bool                check_sql;
    int                 suit;  // see define SUIT_
#ifdef TD_VER_COMPATIBLE_3_0_0_0
    int16_t             inputted_vgroups;
#endif
    enum CONTINUE_IF_FAIL_MODE continueIfFail;
    bool                mistMode;
    bool                escape_character;
    bool                pre_load_tb_meta;
} SArguments;

typedef struct SBenchConn {
    TAOS* taos;
    TAOS* ctaos;  // check taos
    TAOS_STMT* stmt;
#ifdef WEBSOCKET
    WS_TAOS* taos_ws;
    WS_STMT* stmt_ws;
#endif
} SBenchConn;

#define MAX_BATCOLS 256
typedef struct SThreadInfo_S {
    SBenchConn  *conn;
    uint64_t    *bind_ts;
    uint64_t    *bind_ts_array;
    char        *bindParams;
    char        *is_null;
    uint32_t    threadID;
    uint64_t    start_table_from;
    uint64_t    end_table_to;
    uint64_t    ntables;
    uint64_t    tables_created;
    char *      buffer;
    uint64_t    counter;
    uint64_t    st;
    uint64_t    et;
    uint64_t    samplePos;
    uint64_t    totalInsertRows;
    uint64_t    totalQueried;
    int64_t     totalDelay;
    int64_t     totalDelay1;
    int64_t     totalDelay2;
    int64_t     totalDelay3;
    uint64_t    querySeq;
    TAOS_SUB    *tsub;
    char **     lines;
    uint32_t    line_buf_len;
    int32_t     sockfd;
    SDataBase   *dbInfo;
    SSuperTable *stbInfo;
    char        **sml_tags;
    tools_cJSON *json_array;
    tools_cJSON *sml_json_tags;
    char        **sml_tags_json_array;
    char        **sml_json_value_array;
    uint64_t    start_time;
    uint64_t    pos; // point for sampleDataBuff
    uint64_t    max_sql_len;
    FILE        *fp;
    char        filePath[MAX_PATH_LEN];
    BArray*     delayList;
    uint64_t    *query_delay_list;
    double      avg_delay;
#ifdef TD_VER_COMPATIBLE_3_0_0_0
    SVGroup     *vg;
#endif

    int         posOfTblCreatingBatch;
    int         posOfTblCreatingInterval;
    // new
    uint16_t    batCols[MAX_BATCOLS];
    uint16_t    nBatCols;  // valid count for array batCols

    // check sql result
    char        *csql;
    int32_t     clen;  // csql current write position
    bool        stmtBind;
} threadInfo;

typedef struct SQueryThreadInfo_S {
    int start_sql;
    int end_sql;
    int threadId;
    BArray*  query_delay_list;
    int   sockfd;
    SBenchConn* conn;
    int64_t total_delay;
} queryThreadInfo;

typedef struct STSmaThreadInfo_S {
    char* dbName;
    char* stbName;
    BArray* tsmas;
} tsmaThreadInfo;

typedef void (*ToolsSignalHandler)(int signum, void *sigInfo, void *context);

/* ************ Global variables ************  */
extern char *         g_aggreFuncDemo[];
extern char *         g_aggreFunc[];
extern SArguments *   g_arguments;
extern SQueryMetaInfo g_queryInfo;
extern STmqMetaInfo   g_tmqInfo;
extern bool           g_fail;
extern char           configDir[];
extern tools_cJSON *  root;
extern uint64_t       g_memoryUsage;

#define min(a, b) (((a) < (b)) ? (a) : (b))
#define BARRAY_GET_ELEM(array, index) \
    ((void*)((char*)((array)->pData) + (index) * (array)->elemSize))
/* ************ Function declares ************  */
/* benchCommandOpt.c */
int32_t benchParseArgs(int32_t argc, char* argv[]);
void modifyArgument();
void initArgument();
void queryAggrFunc();
void parseFieldDatatype(char *dataType, BArray *fields, bool isTag);
/* demoJsonOpt.c */
int getInfoFromJsonFile();
/* demoUtil.c */
int     compare(const void *a, const void *b);
void    encodeAuthBase64();
void    replaceChildTblName(char *inSql, char *outSql, int tblIndex);
void    setupForAnsiEscape(void);
void    resetAfterAnsiEscape(void);
char *  convertDatatypeToString(int type);
int     convertStringToDatatype(char *type, int length);
unsigned int     taosRandom();
void    tmfree(void *buf);
void    tmfclose(FILE *fp);
void    fetchResult(TAOS_RES *res, threadInfo *pThreadInfo);
void    prompt(bool NonStopMode);
void    ERROR_EXIT(const char *msg);
int     getServerVersionRest(int16_t rest_port);
int     postProceSql(char *sqlstr, char* dbName, int precision, int iface,
                    int protocol, uint16_t rest_port, bool tcp,
                    int sockfd, char* filePath);
int     queryDbExecCall(SBenchConn *conn, char *command);
int     queryDbExecRest(char *command, char* dbName, int precision,
                    int iface, int protocol, bool tcp, int sockfd);
SBenchConn* initBenchConn();
void    closeBenchConn(SBenchConn* conn);
int     regexMatch(const char *s, const char *reg, int cflags);
int     convertHostToServAddr(char *host, uint16_t port,
                              struct sockaddr_in *serv_addr);
int     getAllChildNameOfSuperTable(TAOS *taos, char *dbName, char *stbName,
                                    char ** childTblNameOfSuperTbl,
                                    int64_t childTblCountOfSuperTbl);
void*   benchCalloc(size_t nmemb, size_t size, bool record);
BArray* benchArrayInit(size_t size, size_t elemSize);
void* benchArrayPush(BArray* pArray, void* pData);
void* benchArrayDestroy(BArray* pArray);
void benchArrayClear(BArray* pArray);
void* benchArrayGet(const BArray* pArray, size_t index);
void* benchArrayAddBatch(BArray* pArray, void* pData, int32_t elems);

#ifdef LINUX
int32_t bsem_wait(sem_t* sem);
void benchSetSignal(int32_t signum, ToolsSignalHandler sigfp);
#endif

int convertTypeToLength(uint8_t type);
int64_t convertDatatypeToDefaultMax(uint8_t type);
int64_t convertDatatypeToDefaultMin(uint8_t type);

// dynamic string
char* new_ds(size_t size);
void free_ds(char** ps);
int is_ds(const char* s);
uint64_t ds_custom(const char* s);
void ds_set_custom(char* s, uint64_t custom);
uint64_t ds_len(const char* s);
uint64_t ds_cap(const char* s);
int ds_last(char* s);
char* ds_end(char* s);
char* ds_grow(char**ps, size_t needsize);
char* ds_resize(char** ps, size_t cap);
char * ds_pack(char **ps);
char * ds_add_char(char **ps, char c);
char * ds_add_str(char **ps, const char* sub);
char * ds_add_strs(char **ps, int count, ...);
char * ds_ins_str(char **ps, size_t pos, const char *sub, size_t len);

int  insertTestProcess();
void postFreeResource();
int queryTestProcess();
int subscribeTestProcess();
int convertServAddr(int iface, bool tcp, int protocol);
int createSockFd();
void destroySockFd(int sockfd);

void printVersion();
int32_t benchParseSingleOpt(int32_t key, char* arg);

void printErrCmdCodeStr(char *cmd, int32_t code, TAOS_RES *res);

int32_t benchGetTotalMemory(int64_t *totalKB);

#ifndef LINUX
int32_t benchParseArgsNoArgp(int argc, char* argv[]);
#endif

int32_t execInsert(threadInfo *pThreadInfo, uint32_t k);
// if return true, timestmap must add timestap_step, else timestamp no need changed
bool needChangeTs(SSuperTable * stbInfo, int32_t *pkCur, int32_t *pkCnt);

// tmp function
bool tmpBool(Field *field);
int8_t tmpInt8Impl(Field *field, int64_t k);
uint8_t tmpUint8Impl(Field *field, int64_t k);
int16_t tmpInt16Impl(Field *field, int64_t k);
uint16_t tmpUint16Impl(Field *field, int64_t k);
int tmpInt32Impl(Field *field, int i, int angle, int32_t k);
uint32_t tmpUint32Impl(Field *field, int i, int angle, int64_t k);
int64_t tmpInt64Impl(Field *field, int32_t angle, int32_t k);
uint64_t tmpUint64Impl(Field *field, int32_t angle, int64_t k);
float tmpFloatImpl(Field *field, int i, int32_t angle, int32_t k);
double tmpDoubleImpl(Field *field, int32_t angle, int32_t k);
int tmpStr(char *tmp, int iface, Field *field, int64_t k);

#endif   // INC_BENCH_H_
