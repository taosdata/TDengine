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

#define STMT_IFACE_ENABLED  1

#define REQ_EXTRA_BUF_LEN   1024
#define RESP_BUF_LEN        4096

extern char configDir[];

#define INSERT_JSON_NAME      "insert.json"
#define QUERY_JSON_NAME       "query.json"
#define SUBSCRIBE_JSON_NAME   "subscribe.json"

#define STR_INSERT_INTO     "INSERT INTO "

enum TEST_MODE {
    INSERT_TEST,            // 0
    QUERY_TEST,             // 1
    SUBSCRIBE_TEST,         // 2
    INVAID_TEST
};

#define MAX_RECORDS_PER_REQ     32766

#define HEAD_BUFF_LEN       TSDB_MAX_COLUMNS*24  // 16*MAX_COLUMNS + (192+32)*2 + insert into ..

#define MAX_SQL_SIZE       65536
#define BUFFER_SIZE        (65536*2)
#define COND_BUF_LEN       (BUFFER_SIZE - 30)
#define MAX_USERNAME_SIZE  64
#define MAX_PASSWORD_SIZE  64
#define MAX_HOSTNAME_SIZE  64
#define MAX_TB_NAME_SIZE   64
#define MAX_DATA_SIZE      (16*TSDB_MAX_COLUMNS)+20     // max record len: 16*MAX_COLUMNS, timestamp string and ,('') need extra space
#define OPT_ABORT          1 /* –abort */
#define STRING_LEN         60000
#define MAX_PREPARED_RAND  1000000
#define MAX_FILE_NAME_LEN  256              // max file name length on linux is 255.

#define MAX_SAMPLES_ONCE_FROM_FILE   10000
#define MAX_NUM_COLUMNS        (TSDB_MAX_COLUMNS - 1)      // exclude first column timestamp

#define MAX_DB_COUNT            8
#define MAX_SUPER_TABLE_COUNT   200

#define MAX_QUERY_SQL_COUNT     100
#define MAX_QUERY_SQL_LENGTH    1024

#define MAX_DATABASE_COUNT      256
#define INPUT_BUF_LEN           256

#define DEFAULT_TIMESTAMP_STEP  1


typedef enum CREATE_SUB_TALBE_MOD_EN {
    PRE_CREATE_SUBTBL,
    AUTO_CREATE_SUBTBL,
    NO_CREATE_SUBTBL
} CREATE_SUB_TALBE_MOD_EN;

typedef enum TALBE_EXISTS_EN {
  TBL_NO_EXISTS,
  TBL_ALREADY_EXISTS,
  TBL_EXISTS_BUTT
} TALBE_EXISTS_EN;

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
    char *   metaFile;
    uint32_t test_mode;
    char *   host;
    uint16_t port;
    uint16_t iface;
    char *   user;
    char *   password;
    char *   database;
    int      replica;
    char *   tb_prefix;
    char *   sqlFile;
    bool     use_metric;
    bool     drop_database;
    bool     insert_only;
    bool     answer_yes;
    bool     debug_print;
    bool     verbose_print;
    bool     performance_print;
    char *   output_file;
    bool     async_mode;
    char *   datatype[MAX_NUM_COLUMNS + 1];
    uint32_t len_of_binary;
    uint32_t num_of_CPR;
    uint32_t num_of_threads;
    uint64_t insert_interval;
    int64_t  query_times;
    uint32_t interlace_rows;
    uint32_t num_of_RPR;                  // num_of_records_per_req
    uint64_t max_sql_len;
    int64_t  num_of_tables;
    int64_t  num_of_DPT;
    int      abort;
    uint32_t disorderRatio;               // 0: no disorder, >0: x%
    int      disorderRange;               // ms or us by database precision
    uint32_t method_of_delete;
    char **  arg_list;
    uint64_t totalInsertRows;
    uint64_t totalAffectedRows;
    bool     demo_mode;                  // use default column name and semi-random data
} SArguments;

typedef struct SColumn_S {
  char      field[TSDB_COL_NAME_LEN];
  char      dataType[16];
  uint32_t  dataLen;
  char      note[128];
} StrColumn;

typedef struct SSuperTable_S {
  char         sTblName[TSDB_TABLE_NAME_LEN];
  char         dataSource[MAX_TB_NAME_SIZE];  // rand_gen or sample
  char         childTblPrefix[TSDB_TABLE_NAME_LEN - 20]; // 20 characters reserved for seq
  char         insertMode[MAX_TB_NAME_SIZE];    // taosc, rest
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
  int          disorderRange;           // ms or us by database precision
  uint64_t     maxSqlLen;               //

  uint64_t     insertInterval;          // insert interval, will override global insert interval
  int64_t      insertRows;
  int64_t      timeStampStep;
  char         startTimestamp[MAX_TB_NAME_SIZE];
  char         sampleFormat[MAX_TB_NAME_SIZE];  // csv, json
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
  //int          sampleRowCount;
  //int          sampleUsePos;

  uint32_t     tagSource;    // 0: rand, 1: tag sample
  char*        tagDataBuf;
  uint32_t     tagSampleCount;
  uint32_t     tagUsePos;

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
  char     keeplist[32];
  int32_t  cache; //MB
  int32_t  blocks;
  int32_t  minrows;
  int32_t  maxrows;
  int8_t   wallevel;
  int32_t  fsync;
  int8_t   comp;
  int8_t   cachelast;
  char     precision[8];   // time resolution
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
  char      precision[8];
} SDbCfg;

typedef struct SDataBase_S {
  char         dbName[TSDB_DB_NAME_LEN];
  bool         drop;  // 0: use exists, 1: if exists, drop then new create
  SDbCfg       dbCfg;
  uint64_t     superTblCount;
  SSuperTable  superTbls[MAX_SUPER_TABLE_COUNT];
} SDataBase;

typedef struct SDbs_S {
  char         cfgDir[MAX_FILE_NAME_LEN];
  char         host[MAX_HOSTNAME_SIZE];
  struct sockaddr_in serv_addr;

  uint16_t     port;
  char         user[MAX_USERNAME_SIZE];
  char         password[MAX_PASSWORD_SIZE];
  char         resultFile[MAX_FILE_NAME_LEN];
  bool         use_metric;
  bool         insert_only;
  bool         do_aggreFunc;
  bool         asyncMode;

  uint32_t     threadCount;
  uint32_t     threadCountByCreateTbl;
  uint32_t     dbCount;
  SDataBase    db[MAX_DB_COUNT];

  // statistics
  uint64_t     totalInsertRows;
  uint64_t     totalAffectedRows;

} SDbs;

typedef struct SpecifiedQueryInfo_S {
  uint64_t     queryInterval;  // 0: unlimit  > 0   loop/s
  uint32_t     concurrent;
  int          sqlCount;
  uint32_t     asyncMode; // 0: sync, 1: async
  uint64_t     subscribeInterval; // ms
  uint64_t     queryTimes;
  bool         subscribeRestart;
  int          subscribeKeepProgress;
  char         sql[MAX_QUERY_SQL_COUNT][MAX_QUERY_SQL_LENGTH+1];
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
  char         sTblName[TSDB_TABLE_NAME_LEN];
  uint64_t     queryInterval;  // 0: unlimit  > 0   loop/s
  uint32_t     threadCnt;
  uint32_t     asyncMode; // 0: sync, 1: async
  uint64_t     subscribeInterval; // ms
  bool         subscribeRestart;
  int          subscribeKeepProgress;
  uint64_t     queryTimes;
  int64_t      childTblCount;
  char         childTblPrefix[TSDB_TABLE_NAME_LEN - 20];    // 20 characters reserved for seq
  int          sqlCount;
  char         sql[MAX_QUERY_SQL_COUNT][MAX_QUERY_SQL_LENGTH+1];
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
  char         password[MAX_PASSWORD_SIZE];
  char         dbName[TSDB_DB_NAME_LEN];
  char         queryMode[MAX_TB_NAME_SIZE];  // taosc, rest

  SpecifiedQueryInfo  specifiedQueryInfo;
  SuperQueryInfo      superQueryInfo;
  uint64_t     totalQueried;
} SQueryMetaInfo;

typedef struct SThreadInfo_S {
    TAOS *    taos;
    TAOS_STMT *stmt;
    int       threadID;
    char      db_name[TSDB_DB_NAME_LEN];
    uint32_t  time_precision;
    char      filePath[4096];
    FILE      *fp;
    char      tb_prefix[TSDB_TABLE_NAME_LEN];
    uint64_t  start_table_from;
    uint64_t  end_table_to;
    int64_t   ntables;
    uint64_t  data_of_rate;
    int64_t   start_time;
    char*     cols;
    bool      use_metric;
    SSuperTable* superTblInfo;
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
static int postProceSql(char *host, struct sockaddr_in *pServAddr,
        uint16_t port, char* sqlstr, threadInfo *pThreadInfo);
static int64_t getTSRandTail(int64_t timeStampStep, int32_t seq,
        int disorderRatio, int disorderRange);
static bool getInfoFromJsonFile(char* file);
static void init_rand_data();

/* ************ Global variables ************  */

int32_t  randint[MAX_PREPARED_RAND];
int64_t  randbigint[MAX_PREPARED_RAND];
float    randfloat[MAX_PREPARED_RAND];
double   randdouble[MAX_PREPARED_RAND];
char *aggreFunc[] = {"*", "count(*)", "avg(col0)", "sum(col0)",
    "max(col0)", "min(col0)", "first(col0)", "last(col0)"};

#define DEFAULT_DATATYPE_NUM    3

SArguments g_args = {
    NULL,            // metaFile
    0,               // test_mode
    "127.0.0.1",     // host
    6030,            // port
    INTERFACE_BUT,   // iface
    "root",          // user
#ifdef _TD_POWER_
    "powerdb",      // password
#elif (_TD_TQ_ == true)
    "tqueue",      // password
#else
    "taosdata",      // password
#endif
    "test",          // database
    1,               // replica
    "d",             // tb_prefix
    NULL,            // sqlFile
    true,            // use_metric
    true,            // drop_database
    true,            // insert_only
    false,           // debug_print
    false,           // verbose_print
    false,           // performance statistic print
    false,           // answer_yes;
    "./output.txt",  // output_file
    0,               // mode : sync or async
    {
        "FLOAT",         // datatype
        "INT",           // datatype
        "FLOAT",         // datatype. DEFAULT_DATATYPE_NUM is 3
    },
    16,              // len_of_binary
    4,               // num_of_CPR
    10,              // num_of_connections/thread
    0,               // insert_interval
    1,               // query_times
    0,               // interlace_rows;
    30000,           // num_of_RPR
    (1024*1024),     // max_sql_len
    10000,           // num_of_tables
    10000,           // num_of_DPT
    0,               // abort
    0,               // disorderRatio
    1000,            // disorderRange
    1,               // method_of_delete
    NULL,            // arg_list
    0,               // totalInsertRows;
    0,               // totalAffectedRows;
    true,            // demo_mode;
};



static SDbs            g_Dbs;
static int64_t         g_totalChildTables = 0;
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
    do { fprintf(stderr, "ERROR: "fmt, __VA_ARGS__); } while(0)

// for strncpy buffer overflow
#define min(a, b) (((a) < (b)) ? (a) : (b))


///////////////////////////////////////////////////

static void ERROR_EXIT(const char *msg) { perror(msg); exit(-1); }

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
        printf("taosdemo verison %s-%s\n",
                tdengine_ver, taosdemo_ver);
    } else {
        printf("taosdemo verison %s-%s, status:%s\n",
                tdengine_ver, taosdemo_ver, taosdemo_status);
    }
}

static void printHelp() {
    char indent[10] = "        ";
    printf("%s%s%s%s\n", indent, "-f", indent,
            "The meta file to the execution procedure. Default is './meta.json'.");
    printf("%s%s%s%s\n", indent, "-u", indent,
            "The TDengine user name to use when connecting to the server. Default is 'root'.");
#ifdef _TD_POWER_
    printf("%s%s%s%s\n", indent, "-P", indent,
            "The password to use when connecting to the server. Default is 'powerdb'.");
    printf("%s%s%s%s\n", indent, "-c", indent,
            "Configuration directory. Default is '/etc/power/'.");
#elif (_TD_TQ_ == true)
    printf("%s%s%s%s\n", indent, "-P", indent,
            "The password to use when connecting to the server. Default is 'tqueue'.");
    printf("%s%s%s%s\n", indent, "-c", indent,
            "Configuration directory. Default is '/etc/tq/'.");
#else
    printf("%s%s%s%s\n", indent, "-P", indent,
            "The password to use when connecting to the server. Default is 'taosdata'.");
    printf("%s%s%s%s\n", indent, "-c", indent,
            "Configuration directory. Default is '/etc/taos/'.");
#endif
    printf("%s%s%s%s\n", indent, "-h", indent,
            "The host to connect to TDengine. Default is localhost.");
    printf("%s%s%s%s\n", indent, "-p", indent,
            "The TCP/IP port number to use for the connection. Default is 0.");
    printf("%s%s%s%s\n", indent, "-I", indent,
#if STMT_IFACE_ENABLED == 1
            "The interface (taosc, rest, and stmt) taosdemo uses. Default is 'taosc'.");
#else
    "The interface (taosc, rest) taosdemo uses. Default is 'taosc'.");
#endif
    printf("%s%s%s%s\n", indent, "-d", indent,
            "Destination database. Default is 'test'.");
    printf("%s%s%s%s\n", indent, "-a", indent,
            "Set the replica parameters of the database, Default 1, min: 1, max: 3.");
    printf("%s%s%s%s\n", indent, "-m", indent,
            "Table prefix name. Default is 'd'.");
    printf("%s%s%s%s\n", indent, "-s", indent, "The select sql file.");
    printf("%s%s%s%s\n", indent, "-N", indent, "Use normal table flag.");
    printf("%s%s%s%s\n", indent, "-o", indent,
            "Direct output to the named file. Default is './output.txt'.");
    printf("%s%s%s%s\n", indent, "-q", indent,
            "Query mode -- 0: SYNC, 1: ASYNC. Default is SYNC.");
    printf("%s%s%s%s\n", indent, "-b", indent,
            "The data_type of columns, default: FLOAT, INT, FLOAT.");
    printf("%s%s%s%s\n", indent, "-w", indent,
            "The length of data_type 'BINARY' or 'NCHAR'. Default is 16");
    printf("%s%s%s%s%d%s%d\n", indent, "-l", indent,
            "The number of columns per record. Default is ",
            DEFAULT_DATATYPE_NUM,
            ". Max values is ",
            MAX_NUM_COLUMNS);
    printf("%s%s%s%s\n", indent, indent, indent,
            "All of the new column(s) type is INT. If use -b to specify column type, -l will be ignored.");
    printf("%s%s%s%s\n", indent, "-T", indent,
            "The number of threads. Default is 10.");
    printf("%s%s%s%s\n", indent, "-i", indent,
            "The sleep time (ms) between insertion. Default is 0.");
    printf("%s%s%s%s\n", indent, "-r", indent,
            "The number of records per request. Default is 30000.");
    printf("%s%s%s%s\n", indent, "-t", indent,
            "The number of tables. Default is 10000.");
    printf("%s%s%s%s\n", indent, "-n", indent,
            "The number of records per table. Default is 10000.");
    printf("%s%s%s%s\n", indent, "-M", indent,
            "The value of records generated are totally random.");
    printf("%s%s%s%s\n", indent, indent, indent,
            " The default is to simulate power equipment senario.");
    printf("%s%s%s%s\n", indent, "-x", indent, "Not insert only flag.");
    printf("%s%s%s%s\n", indent, "-y", indent, "Default input yes for prompt.");
    printf("%s%s%s%s\n", indent, "-O", indent,
            "Insert mode--0: In order, 1 ~ 50: disorder ratio. Default is in order.");
    printf("%s%s%s%s\n", indent, "-R", indent,
            "Out of order data's range, ms, default is 1000.");
    printf("%s%s%s%s\n", indent, "-g", indent,
            "Print debug info.");
    printf("%s%s%s\n", indent, "-V, --version\t",
            "Print version info.");
    printf("%s%s%s%s\n", indent, "--help\t", indent,
            "Print command line arguments list info.");
    /*    printf("%s%s%s%s\n", indent, "-D", indent,
          "Delete database if exists. 0: no, 1: yes, default is 1");
          */
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

static void parse_args(int argc, char *argv[], SArguments *arguments) {

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-f") == 0) {
            arguments->demo_mode = false;
            arguments->metaFile = argv[++i];
        } else if (strcmp(argv[i], "-c") == 0) {
            if (argc == i+1) {
                printHelp();
                errorPrint("%s", "\n\t-c need a valid path following!\n");
                exit(EXIT_FAILURE);
            }
            tstrncpy(configDir, argv[++i], TSDB_FILENAME_LEN);
        } else if (strcmp(argv[i], "-h") == 0) {
            if (argc == i+1) {
                printHelp();
                errorPrint("%s", "\n\t-h need a valid string following!\n");
                exit(EXIT_FAILURE);
            }
            arguments->host = argv[++i];
        } else if (strcmp(argv[i], "-p") == 0) {
            if ((argc == i+1) ||
                    (!isStringNumber(argv[i+1]))) {
                printHelp();
                errorPrint("%s", "\n\t-p need a number following!\n");
                exit(EXIT_FAILURE);
            }
            arguments->port = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-I") == 0) {
            if (argc == i+1) {
                printHelp();
                errorPrint("%s", "\n\t-I need a valid string following!\n");
                exit(EXIT_FAILURE);
            }
            ++i;
            if (0 == strcasecmp(argv[i], "taosc")) {
                arguments->iface = TAOSC_IFACE;
            } else if (0 == strcasecmp(argv[i], "rest")) {
                arguments->iface = REST_IFACE;
#if STMT_IFACE_ENABLED == 1
            } else if (0 == strcasecmp(argv[i], "stmt")) {
                arguments->iface = STMT_IFACE;
#endif
            } else {
                errorPrint("%s", "\n\t-I need a valid string following!\n");
                exit(EXIT_FAILURE);
            }
        } else if (strcmp(argv[i], "-u") == 0) {
            if (argc == i+1) {
                printHelp();
                errorPrint("%s", "\n\t-u need a valid string following!\n");
                exit(EXIT_FAILURE);
            }
            arguments->user = argv[++i];
        } else if (strcmp(argv[i], "-P") == 0) {
            if (argc == i+1) {
                printHelp();
                errorPrint("%s", "\n\t-P need a valid string following!\n");
                exit(EXIT_FAILURE);
            }
            arguments->password = argv[++i];
        } else if (strcmp(argv[i], "-o") == 0) {
            if (argc == i+1) {
                printHelp();
                errorPrint("%s", "\n\t-o need a valid string following!\n");
                exit(EXIT_FAILURE);
            }
            arguments->output_file = argv[++i];
        } else if (strcmp(argv[i], "-s") == 0) {
            if (argc == i+1) {
                printHelp();
                errorPrint("%s", "\n\t-s need a valid string following!\n");
                exit(EXIT_FAILURE);
            }
            arguments->sqlFile = argv[++i];
        } else if (strcmp(argv[i], "-q") == 0) {
            if ((argc == i+1)
                    || (!isStringNumber(argv[i+1]))) {
                printHelp();
                errorPrint("%s", "\n\t-q need a number following!\nQuery mode -- 0: SYNC, not-0: ASYNC. Default is SYNC.\n");
                exit(EXIT_FAILURE);
            }
            arguments->async_mode = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-T") == 0) {
            if ((argc == i+1)
                    || (!isStringNumber(argv[i+1]))) {
                printHelp();
                errorPrint("%s", "\n\t-T need a number following!\n");
                exit(EXIT_FAILURE);
            }
            arguments->num_of_threads = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-i") == 0) {
            if ((argc == i+1) ||
                    (!isStringNumber(argv[i+1]))) {
                printHelp();
                errorPrint("%s", "\n\t-i need a number following!\n");
                exit(EXIT_FAILURE);
            }
            arguments->insert_interval = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-qt") == 0) {
            if ((argc == i+1)
                    || (!isStringNumber(argv[i+1]))) {
                printHelp();
                errorPrint("%s", "\n\t-qt need a number following!\n");
                exit(EXIT_FAILURE);
            }
            arguments->query_times = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-B") == 0) {
            if ((argc == i+1)
                    || (!isStringNumber(argv[i+1]))) {
                printHelp();
                errorPrint("%s", "\n\t-B need a number following!\n");
                exit(EXIT_FAILURE);
            }
            arguments->interlace_rows = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-r") == 0) {
            if ((argc == i+1)
                    || (!isStringNumber(argv[i+1]))) {
                printHelp();
                errorPrint("%s", "\n\t-r need a number following!\n");
                exit(EXIT_FAILURE);
            }
            arguments->num_of_RPR = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-t") == 0) {
            if ((argc == i+1) ||
                    (!isStringNumber(argv[i+1]))) {
                printHelp();
                errorPrint("%s", "\n\t-t need a number following!\n");
                exit(EXIT_FAILURE);
            }
            arguments->num_of_tables = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-n") == 0) {
            if ((argc == i+1) ||
                    (!isStringNumber(argv[i+1]))) {
                printHelp();
                errorPrint("%s", "\n\t-n need a number following!\n");
                exit(EXIT_FAILURE);
            }
            arguments->num_of_DPT = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-d") == 0) {
            if (argc == i+1) {
                printHelp();
                errorPrint("%s", "\n\t-d need a valid string following!\n");
                exit(EXIT_FAILURE);
            }
            arguments->database = argv[++i];
        } else if (strcmp(argv[i], "-l") == 0) {
            arguments->demo_mode = false;
            if (argc == i+1) {
                if (!isStringNumber(argv[i+1])) {
                    printHelp();
                    errorPrint("%s", "\n\t-l need a number following!\n");
                    exit(EXIT_FAILURE);
                }
            }
            arguments->num_of_CPR = atoi(argv[++i]);

            if (arguments->num_of_CPR > MAX_NUM_COLUMNS) {
                printf("WARNING: max acceptible columns count is %d\n", MAX_NUM_COLUMNS);
                prompt();
                arguments->num_of_CPR = MAX_NUM_COLUMNS;
            }

            for (int col = DEFAULT_DATATYPE_NUM; col < arguments->num_of_CPR; col ++) {
                arguments->datatype[col] = "INT";
            }
            for (int col = arguments->num_of_CPR; col < MAX_NUM_COLUMNS; col++) {
                arguments->datatype[col] = NULL;
            }
        } else if (strcmp(argv[i], "-b") == 0) {
            arguments->demo_mode = false;
            if (argc == i+1) {
                printHelp();
                errorPrint("%s", "\n\t-b need valid string following!\n");
                exit(EXIT_FAILURE);
            }
            ++i;
            if (strstr(argv[i], ",") == NULL) {
                // only one col
                if (strcasecmp(argv[i], "INT")
                        && strcasecmp(argv[i], "FLOAT")
                        && strcasecmp(argv[i], "TINYINT")
                        && strcasecmp(argv[i], "BOOL")
                        && strcasecmp(argv[i], "SMALLINT")
                        && strcasecmp(argv[i], "BIGINT")
                        && strcasecmp(argv[i], "DOUBLE")
                        && strcasecmp(argv[i], "BINARY")
                        && strcasecmp(argv[i], "TIMESTAMP")
                        && strcasecmp(argv[i], "NCHAR")) {
                    printHelp();
                    errorPrint("%s", "-b: Invalid data_type!\n");
                    exit(EXIT_FAILURE);
                }
                arguments->datatype[0] = argv[i];
            } else {
                // more than one col
                int index = 0;
                g_dupstr = strdup(argv[i]);
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
                            && strcasecmp(token, "BINARY")
                            && strcasecmp(token, "TIMESTAMP")
                            && strcasecmp(token, "NCHAR")) {
                        printHelp();
                        free(g_dupstr);
                        errorPrint("%s", "-b: Invalid data_type!\n");
                        exit(EXIT_FAILURE);
                    }
                    arguments->datatype[index++] = token;
                    token = strsep(&running, ",");
                    if (index >= MAX_NUM_COLUMNS) break;
                }
                arguments->datatype[index] = NULL;
            }
        } else if (strcmp(argv[i], "-w") == 0) {
            if ((argc == i+1) ||
                    (!isStringNumber(argv[i+1]))) {
                printHelp();
                errorPrint("%s", "\n\t-w need a number following!\n");
                exit(EXIT_FAILURE);
            }
            arguments->len_of_binary = atoi(argv[++i]);
        } else if (strcmp(argv[i], "-m") == 0) {
            if ((argc == i+1) ||
                    (isStringNumber(argv[i+1]))) {
                printHelp();
                errorPrint("%s", "\n\t-m need a letter-initial string following!\n");
                exit(EXIT_FAILURE);
            }
            arguments->tb_prefix = argv[++i];
        } else if (strcmp(argv[i], "-N") == 0) {
            arguments->use_metric = false;
        } else if (strcmp(argv[i], "-M") == 0) {
            arguments->demo_mode = false;
        } else if (strcmp(argv[i], "-x") == 0) {
            arguments->insert_only = false;
        } else if (strcmp(argv[i], "-y") == 0) {
            arguments->answer_yes = true;
        } else if (strcmp(argv[i], "-g") == 0) {
            arguments->debug_print = true;
        } else if (strcmp(argv[i], "-gg") == 0) {
            arguments->verbose_print = true;
        } else if (strcmp(argv[i], "-pp") == 0) {
            arguments->performance_print = true;
        } else if (strcmp(argv[i], "-O") == 0) {
            if ((argc == i+1) ||
                    (!isStringNumber(argv[i+1]))) {
                printHelp();
                errorPrint("%s", "\n\t-O need a number following!\n");
                exit(EXIT_FAILURE);
            }

            arguments->disorderRatio = atoi(argv[++i]);

            if (arguments->disorderRatio > 50) {
                arguments->disorderRatio = 50;
            }

            if (arguments->disorderRatio < 0) {
                arguments->disorderRatio = 0;
            }

        } else if (strcmp(argv[i], "-R") == 0) {
            if ((argc == i+1) ||
                    (!isStringNumber(argv[i+1]))) {
                printHelp();
                errorPrint("%s", "\n\t-R need a number following!\n");
                exit(EXIT_FAILURE);
            }

            arguments->disorderRange = atoi(argv[++i]);
            if (arguments->disorderRange < 0)
                arguments->disorderRange = 1000;

        } else if (strcmp(argv[i], "-a") == 0) {
            if ((argc == i+1) ||
                    (!isStringNumber(argv[i+1]))) {
                printHelp();
                errorPrint("%s", "\n\t-a need a number following!\n");
                exit(EXIT_FAILURE);
            }
            arguments->replica = atoi(argv[++i]);
            if (arguments->replica > 3 || arguments->replica < 1) {
                arguments->replica = 1;
            }
        } else if (strcmp(argv[i], "-D") == 0) {
            arguments->method_of_delete = atoi(argv[++i]);
            if (arguments->method_of_delete > 3) {
                errorPrint("%s", "\n\t-D need a valud (0~3) number following!\n");
                exit(EXIT_FAILURE);
            }
        } else if ((strcmp(argv[i], "--version") == 0) ||
                (strcmp(argv[i], "-V") == 0)){
            printVersion();
            exit(0);
        } else if (strcmp(argv[i], "--help") == 0) {
            printHelp();
            exit(0);
        } else {
            printHelp();
            errorPrint("%s", "ERROR: wrong options\n");
            exit(EXIT_FAILURE);
        }
    }

    int columnCount;
    for (columnCount = 0; columnCount < MAX_NUM_COLUMNS; columnCount ++) {
        if (g_args.datatype[columnCount] == NULL) {
            break;
        }
    }

    if (0 == columnCount) {
        perror("data type error!");
        exit(-1);
    }
    g_args.num_of_CPR = columnCount;

    if (((arguments->debug_print) && (arguments->metaFile == NULL))
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
        if (*(arguments->datatype)) {
            printf("# Specified data type:               ");
            for (int i = 0; i < MAX_NUM_COLUMNS; i++)
                if (arguments->datatype[i])
                    printf("%s,", arguments->datatype[i]);
                else
                    break;
            printf("\n");
        }
        printf("# Insertion interval:                %"PRIu64"\n",
                arguments->insert_interval);
        printf("# Number of records per req:         %u\n",
                arguments->num_of_RPR);
        printf("# Max SQL length:                    %"PRIu64"\n",
                arguments->max_sql_len);
        printf("# Length of Binary:                  %d\n", arguments->len_of_binary);
        printf("# Number of Threads:                 %d\n", arguments->num_of_threads);
        printf("# Number of Tables:                  %"PRId64"\n",
                arguments->num_of_tables);
        printf("# Number of Data per Table:          %"PRId64"\n",
                arguments->num_of_DPT);
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

static void tmfree(char *buf) {
    if (NULL != buf) {
        free(buf);
    }
}

static int queryDbExec(TAOS *taos, char *command, QUERY_TYPE type, bool quiet) {
    int i;
    TAOS_RES *res = NULL;
    int32_t   code = -1;

    for (i = 0; i < 5 /* retry */; i++) {
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

    verbosePrint("%s() LN%d - command: %s\n", __func__, __LINE__, command);
    if (code != 0) {
        if (!quiet) {
            errorPrint("Failed to execute %s, reason: %s\n",
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
        errorPrint(
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
        errorPrint("%s() LN%d, failed to malloc, warning: save result to file slowly!\n",
                __func__, __LINE__);
        return ;
    }

    int64_t   totalLen = 0;

    // fetch the records row by row
    while((row = taos_fetch_row(res))) {
        if (totalLen >= 100*1024*1024 - 32000) {
            if (strlen(pThreadInfo->filePath) > 0)
                appendResultBufToFile(databuf, pThreadInfo);
            totalLen = 0;
            memset(databuf, 0, 100*1024*1024);
        }
        num_rows++;
        char  temp[16000] = {0};
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
            errorPrint("%s() LN%d, failed to execute sql:%s, reason:%s\n",
                    __func__, __LINE__, command, taos_errstr(res));
            taos_free_result(res);
            return;
        }

        fetchResult(res, pThreadInfo);
        taos_free_result(res);

    } else if (0 == strncasecmp(g_queryInfo.queryMode, "rest", strlen("rest"))) {
        int retCode = postProceSql(
                g_queryInfo.host, &(g_queryInfo.serv_addr), g_queryInfo.port,
                command,
                pThreadInfo);
        if (0 != retCode) {
            printf("====restful return fail, threadID[%d]\n", pThreadInfo->threadID);
        }

    } else {
        errorPrint("%s() LN%d, unknown query mode: %s\n",
                __func__, __LINE__, g_queryInfo.queryMode);
    }
}

static int32_t rand_bool(){
    static int cursor;
    cursor++;
    cursor = cursor % MAX_PREPARED_RAND;
    return randint[cursor] % 2;
}

static int32_t rand_tinyint(){
    static int cursor;
    cursor++;
    cursor = cursor % MAX_PREPARED_RAND;
    return randint[cursor] % 128;
}

static int32_t rand_smallint(){
    static int cursor;
    cursor++;
    cursor = cursor % MAX_PREPARED_RAND;
    return randint[cursor] % 32767;
}

static int32_t rand_int(){
    static int cursor;
    cursor++;
    cursor = cursor % MAX_PREPARED_RAND;
    return randint[cursor];
}

static int64_t rand_bigint(){
    static int cursor;
    cursor++;
    cursor = cursor % MAX_PREPARED_RAND;
    return randbigint[cursor];
}

static float rand_float(){
    static int cursor;
    cursor++;
    cursor = cursor % MAX_PREPARED_RAND;
    return randfloat[cursor];
}

static float demo_current_float(){
    static int cursor;
    cursor++;
    cursor = cursor % MAX_PREPARED_RAND;
    return (float)(9.8 + 0.04 * (randint[cursor] % 10) + randfloat[cursor]/1000000000);
}

static int32_t demo_voltage_int(){
    static int cursor;
    cursor++;
    cursor = cursor % MAX_PREPARED_RAND;
    return 215 + randint[cursor] % 10;
}

static float demo_phase_float(){
    static int cursor;
    cursor++;
    cursor = cursor % MAX_PREPARED_RAND;
    return (float)((115 + randint[cursor] % 10 + randfloat[cursor]/1000000000)/360);
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

static double rand_double() {
    static int cursor;
    cursor++;
    cursor = cursor % MAX_PREPARED_RAND;
    return randdouble[cursor];
}

static void init_rand_data() {
    for (int i = 0; i < MAX_PREPARED_RAND; i++){
        randint[i] = (int)(taosRandom() % 65535);
        randbigint[i] = (int64_t)(taosRandom() % 2147483648);
        randfloat[i] = (float)(taosRandom() / 1000.0);
        randdouble[i] = (double)(taosRandom() / 1000000.0);
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

    if (g_args.demo_mode)
        printf("\ntaosdemo is simulating data generated by power equipments monitoring...\n\n");
    else
        printf("\ntaosdemo is simulating random data as you request..\n\n");

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
            g_Dbs.threadCountByCreateTbl);
    printf("top insert interval:        \033[33m%"PRIu64"\033[0m\n",
            g_args.insert_interval);
    printf("number of records per req:  \033[33m%u\033[0m\n",
            g_args.num_of_RPR);
    printf("max sql length:             \033[33m%"PRIu64"\033[0m\n",
            g_args.max_sql_len);

    printf("database count:             \033[33m%d\033[0m\n", g_Dbs.dbCount);

    for (int i = 0; i < g_Dbs.dbCount; i++) {
        printf("database[\033[33m%d\033[0m]:\n", i);
        printf("  database[%d] name:      \033[33m%s\033[0m\n",
                i, g_Dbs.db[i].dbName);
        if (0 == g_Dbs.db[i].drop) {
            printf("  drop:                  \033[33mno\033[0m\n");
        } else {
            printf("  drop:                  \033[33myes\033[0m\n");
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
                    || (0 == strncasecmp(g_Dbs.db[i].dbCfg.precision, "us", 2))) {
                printf("  precision:             \033[33m%s\033[0m\n",
                        g_Dbs.db[i].dbCfg.precision);
            } else {
                printf("\033[1m\033[40;31m  precision error:       %s\033[0m\n",
                        g_Dbs.db[i].dbCfg.precision);
                return -1;
            }
        }

        printf("  super table count:     \033[33m%"PRIu64"\033[0m\n",
                g_Dbs.db[i].superTblCount);
        for (uint64_t j = 0; j < g_Dbs.db[i].superTblCount; j++) {
            printf("  super table[\033[33m%"PRIu64"\033[0m]:\n", j);

            printf("      stbName:           \033[33m%s\033[0m\n",
                    g_Dbs.db[i].superTbls[j].sTblName);

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
               printf("      multiThreadWriteOneTbl:  \033[33mno\033[0m\n");
               }else {
               printf("      multiThreadWriteOneTbl:  \033[33myes\033[0m\n");
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
            printf("      tagsFile:          \033[33m%s\033[0m\n",
                    g_Dbs.db[i].superTbls[j].tagsFile);
            printf("      columnCount:       \033[33m%d\033[0m\n",
                    g_Dbs.db[i].superTbls[j].columnCount);
            for (int k = 0; k < g_Dbs.db[i].superTbls[j].columnCount; k++) {
                //printf("dataType:%s, dataLen:%d\t", g_Dbs.db[i].superTbls[j].columns[k].dataType, g_Dbs.db[i].superTbls[j].columns[k].dataLen);
                if ((0 == strncasecmp(g_Dbs.db[i].superTbls[j].columns[k].dataType,
                                "binary", 6))
                        || (0 == strncasecmp(g_Dbs.db[i].superTbls[j].columns[k].dataType,
                                "nchar", 5))) {
                    printf("column[\033[33m%d\033[0m]:\033[33m%s(%d)\033[0m ", k,
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
    fprintf(fp, "thread num of create table: %d\n", g_Dbs.threadCountByCreateTbl);
    fprintf(fp, "number of records per req:  %u\n", g_args.num_of_RPR);
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
                    g_Dbs.db[i].superTbls[j].sTblName);

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
                    g_queryInfo.superQueryInfo.sTblName);
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
    if (precision == TSDB_TIME_PRECISION_NANO) {
        tt = (time_t)(val / 1000000000);
    } else if (precision == TSDB_TIME_PRECISION_MICRO) {
        tt = (time_t)(val / 1000000);
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

    if (precision == TSDB_TIME_PRECISION_NANO) {
        sprintf(buf + pos, ".%09d", (int)(val % 1000000000));
    } else if (precision == TSDB_TIME_PRECISION_MICRO) {
        sprintf(buf + pos, ".%06d", (int)(val % 1000000));
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
            fprintf(fp, "%d", ((((int32_t)(*((char *)val))) == 1) ? 1 : 0));
            break;
        case TSDB_DATA_TYPE_TINYINT:
            fprintf(fp, "%d", *((int8_t *)val));
            break;
        case TSDB_DATA_TYPE_SMALLINT:
            fprintf(fp, "%d", *((int16_t *)val));
            break;
        case TSDB_DATA_TYPE_INT:
            fprintf(fp, "%d", *((int32_t *)val));
            break;
        case TSDB_DATA_TYPE_BIGINT:
            fprintf(fp, "%" PRId64, *((int64_t *)val));
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
        errorPrint("%s() LN%d, failed to open file: %s\n",
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
        errorPrint( "failed to run <show databases>, reason: %s\n",
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
            errorPrint( "failed to allocate memory for some dbInfo[%d]\n", count);
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
        char* filename, SDbInfo* dbInfos, int index) {

    if (filename[0] == 0)
        return;

    FILE *fp = fopen(filename, "at");
    if (fp == NULL) {
        errorPrint( "failed to open file: %s\n", filename);
        return;
    }

    fprintf(fp, "================ database[%d] ================\n", index);
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
    char filename[MAX_QUERY_SQL_LENGTH+1] = {0};
    char buffer[MAX_QUERY_SQL_LENGTH+1] = {0};
    TAOS_RES* res;

    time_t t;
    struct tm* lt;
    time(&t);
    lt = localtime(&t);
    snprintf(filename, MAX_QUERY_SQL_LENGTH, "querySystemInfo-%d-%d-%d %d:%d:%d",
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
        snprintf(buffer, MAX_QUERY_SQL_LENGTH, "show %s.vgroups;", dbInfos[i]->name);
        res = taos_query(taos, buffer);
        xDumpResultToFile(filename, res);

        // show db.stables
        snprintf(buffer, MAX_QUERY_SQL_LENGTH, "show %s.stables;", dbInfos[i]->name);
        res = taos_query(taos, buffer);
        xDumpResultToFile(filename, res);

        free(dbInfos[i]);
    }

    free(dbInfos);
}

static int postProceSql(char *host, struct sockaddr_in *pServAddr, uint16_t port,
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
      errorPrint("%s", "ERROR, cannot allocate memory.\n");
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

    snprintf(userpass_buf, INPUT_BUF_LEN, "%s:%s",
        g_Dbs.user, g_Dbs.password);
    size_t userpass_buf_len = strlen(userpass_buf);
    size_t encoded_len = 4 * ((userpass_buf_len +2) / 3);

    char base64_buf[INPUT_BUF_LEN];
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
        free(request_buf);
        ERROR_EXIT("ERROR opening socket");
    }

    int retConn = connect(sockfd, (struct sockaddr *)pServAddr, sizeof(struct sockaddr));
    debugPrint("%s() LN%d connect() return %d\n", __func__, __LINE__, retConn);
    if (retConn < 0) {
        free(request_buf);
        ERROR_EXIT("ERROR connecting");
    }

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
        ERROR_EXIT("ERROR too long request");
    }
    verbosePrint("%s() LN%d: Request:\n%s\n", __func__, __LINE__, request_buf);

    req_str_len = strlen(request_buf);
    sent = 0;
    do {
#ifdef WINDOWS
        bytes = send(sockfd, request_buf + sent, req_str_len - sent, 0);
#else
        bytes = write(sockfd, request_buf + sent, req_str_len - sent);
#endif
        if (bytes < 0)
            ERROR_EXIT("ERROR writing message to socket");
        if (bytes == 0)
            break;
        sent+=bytes;
    } while(sent < req_str_len);

    memset(response_buf, 0, RESP_BUF_LEN);
    resp_len = sizeof(response_buf) - 1;
    received = 0;
    do {
#ifdef WINDOWS
        bytes = recv(sockfd, response_buf + received, resp_len - received, 0);
#else
        bytes = read(sockfd, response_buf + received, resp_len - received);
#endif
        if (bytes < 0) {
            free(request_buf);
            ERROR_EXIT("ERROR reading response from socket");
        }
        if (bytes == 0)
            break;
        received += bytes;
    } while(received < resp_len);

    if (received == resp_len) {
        free(request_buf);
        ERROR_EXIT("ERROR storing complete response from socket");
    }

    response_buf[RESP_BUF_LEN - 1] = '\0';
    printf("Response:\n%s\n", response_buf);

    if (strlen(pThreadInfo->filePath) > 0) {
       appendResultBufToFile(response_buf, pThreadInfo);
    }

    free(request_buf);
#ifdef WINDOWS
    closesocket(sockfd);
    WSACleanup();
#else
    close(sockfd);
#endif

    return 0;
}

static char* getTagValueFromTagSample(SSuperTable* stbInfo, int tagUsePos) {
    char*  dataBuf = (char*)calloc(TSDB_MAX_SQL_LEN+1, 1);
    if (NULL == dataBuf) {
        errorPrint("%s() LN%d, calloc failed! size:%d\n",
                __func__, __LINE__, TSDB_MAX_SQL_LEN+1);
        return NULL;
    }

    int    dataLen = 0;
    dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
            "(%s)", stbInfo->tagDataBuf + stbInfo->lenOfTagOfOneRow * tagUsePos);

    return dataBuf;
}

static char* generateTagVaulesForStb(SSuperTable* stbInfo, int32_t tableSeq) {
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

            int tagBufLen = stbInfo->tags[i].dataLen + 1;
            char* buf = (char*)calloc(tagBufLen, 1);
            if (NULL == buf) {
                printf("calloc failed! size:%d\n", stbInfo->tags[i].dataLen);
                tmfree(dataBuf);
                return NULL;
            }

            if (tableSeq % 2) {
                tstrncpy(buf, "beijing", tagBufLen);
            } else {
                tstrncpy(buf, "shanghai", tagBufLen);
            }
            //rand_string(buf, stbInfo->tags[i].dataLen);
            dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                    "\'%s\', ", buf);
            tmfree(buf);
        } else if (0 == strncasecmp(stbInfo->tags[i].dataType,
                    "int", strlen("int"))) {
            if ((g_args.demo_mode) && (i == 0)) {
                dataLen += snprintf(dataBuf + dataLen,
                        TSDB_MAX_SQL_LEN - dataLen,
                    "%d, ", tableSeq % 10);
            } else {
                dataLen += snprintf(dataBuf + dataLen,
                        TSDB_MAX_SQL_LEN - dataLen,
                    "%d, ", tableSeq);
            }
        } else if (0 == strncasecmp(stbInfo->tags[i].dataType,
                    "bigint", strlen("bigint"))) {
            dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                    "%"PRId64", ", rand_bigint());
        }  else if (0 == strncasecmp(stbInfo->tags[i].dataType,
                    "float", strlen("float"))) {
            dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                    "%f, ", rand_float());
        }  else if (0 == strncasecmp(stbInfo->tags[i].dataType,
                    "double", strlen("double"))) {
            dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                    "%f, ", rand_double());
        }  else if (0 == strncasecmp(stbInfo->tags[i].dataType,
                    "smallint", strlen("smallint"))) {
            dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                    "%d, ", rand_smallint());
        }  else if (0 == strncasecmp(stbInfo->tags[i].dataType,
                    "tinyint", strlen("tinyint"))) {
            dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                    "%d, ", rand_tinyint());
        }  else if (0 == strncasecmp(stbInfo->tags[i].dataType,
                    "bool", strlen("bool"))) {
            dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                    "%d, ", rand_bool());
        }  else if (0 == strncasecmp(stbInfo->tags[i].dataType,
                    "timestamp", strlen("timestamp"))) {
            dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen,
                    "%"PRId64", ", rand_bigint());
        }  else {
            printf("No support data type: %s\n", stbInfo->tags[i].dataType);
            tmfree(dataBuf);
            return NULL;
        }
    }

    dataLen -= 2;
    dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen, ")");
    return dataBuf;
}

static int calcRowLen(SSuperTable*  superTbls) {
  int colIndex;
  int  lenOfOneRow = 0;

  for (colIndex = 0; colIndex < superTbls->columnCount; colIndex++) {
    char* dataType = superTbls->columns[colIndex].dataType;

    if (strcasecmp(dataType, "BINARY") == 0) {
      lenOfOneRow += superTbls->columns[colIndex].dataLen + 3;
    } else if (strcasecmp(dataType, "NCHAR") == 0) {
      lenOfOneRow += superTbls->columns[colIndex].dataLen + 3;
    } else if (strcasecmp(dataType, "INT") == 0)  {
      lenOfOneRow += 11;
    } else if (strcasecmp(dataType, "BIGINT") == 0)  {
      lenOfOneRow += 21;
    } else if (strcasecmp(dataType, "SMALLINT") == 0)  {
      lenOfOneRow += 6;
    } else if (strcasecmp(dataType, "TINYINT") == 0)  {
      lenOfOneRow += 4;
    } else if (strcasecmp(dataType, "BOOL") == 0)  {
      lenOfOneRow += 6;
    } else if (strcasecmp(dataType, "FLOAT") == 0) {
      lenOfOneRow += 22;
    } else if (strcasecmp(dataType, "DOUBLE") == 0) {
      lenOfOneRow += 42;
    }  else if (strcasecmp(dataType, "TIMESTAMP") == 0) {
      lenOfOneRow += 21;
    } else {
      printf("get error data type : %s\n", dataType);
      exit(-1);
    }
  }

  superTbls->lenOfOneRow = lenOfOneRow + 20; // timestamp

  int tagIndex;
  int lenOfTagOfOneRow = 0;
  for (tagIndex = 0; tagIndex < superTbls->tagCount; tagIndex++) {
    char* dataType = superTbls->tags[tagIndex].dataType;

    if (strcasecmp(dataType, "BINARY") == 0) {
      lenOfTagOfOneRow += superTbls->tags[tagIndex].dataLen + 3;
    } else if (strcasecmp(dataType, "NCHAR") == 0) {
      lenOfTagOfOneRow += superTbls->tags[tagIndex].dataLen + 3;
    } else if (strcasecmp(dataType, "INT") == 0)  {
      lenOfTagOfOneRow += superTbls->tags[tagIndex].dataLen + 11;
    } else if (strcasecmp(dataType, "BIGINT") == 0)  {
      lenOfTagOfOneRow += superTbls->tags[tagIndex].dataLen + 21;
    } else if (strcasecmp(dataType, "SMALLINT") == 0)  {
      lenOfTagOfOneRow += superTbls->tags[tagIndex].dataLen + 6;
    } else if (strcasecmp(dataType, "TINYINT") == 0)  {
      lenOfTagOfOneRow += superTbls->tags[tagIndex].dataLen + 4;
    } else if (strcasecmp(dataType, "BOOL") == 0)  {
      lenOfTagOfOneRow += superTbls->tags[tagIndex].dataLen + 6;
    } else if (strcasecmp(dataType, "FLOAT") == 0) {
      lenOfTagOfOneRow += superTbls->tags[tagIndex].dataLen + 22;
    } else if (strcasecmp(dataType, "DOUBLE") == 0) {
      lenOfTagOfOneRow += superTbls->tags[tagIndex].dataLen + 42;
    } else {
      printf("get error tag type : %s\n", dataType);
      exit(-1);
    }
  }

  superTbls->lenOfTagOfOneRow = lenOfTagOfOneRow;

  return 0;
}


static int getChildNameOfSuperTableWithLimitAndOffset(TAOS * taos,
        char* dbName, char* sTblName, char** childTblNameOfSuperTbl,
        int64_t* childTblCountOfSuperTbl, int64_t limit, uint64_t offset) {

  char command[BUFFER_SIZE] = "\0";
  char limitBuf[100] = "\0";

  TAOS_RES * res;
  TAOS_ROW row = NULL;

  char* childTblName = *childTblNameOfSuperTbl;

  if (offset >= 0) {
    snprintf(limitBuf, 100, " limit %"PRId64" offset %"PRIu64"",
            limit, offset);
  }

  //get all child table name use cmd: select tbname from superTblName;
  snprintf(command, BUFFER_SIZE, "select tbname from %s.%s %s",
          dbName, sTblName, limitBuf);

  res = taos_query(taos, command);
  int32_t code = taos_errno(res);
  if (code != 0) {
    taos_free_result(res);
    taos_close(taos);
    errorPrint("%s() LN%d, failed to run command %s\n",
           __func__, __LINE__, command);
    exit(-1);
  }

  int64_t childTblCount = (limit < 0)?10000:limit;
  int64_t count = 0;
  if (childTblName == NULL) {
    childTblName = (char*)calloc(1, childTblCount * TSDB_TABLE_NAME_LEN);
    if (NULL ==  childTblName) {
    taos_free_result(res);
        taos_close(taos);
        errorPrint("%s() LN%d, failed to allocate memory!\n", __func__, __LINE__);
        exit(-1);
    }
  }

  char* pTblName = childTblName;
  while((row = taos_fetch_row(res)) != NULL) {
    int32_t* len = taos_fetch_lengths(res);

    if (0 == strlen((char *)row[0])) {
        errorPrint("%s() LN%d, No.%"PRId64" table return empty name\n",
            __func__, __LINE__, count);
        exit(-1);
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
        errorPrint("%s() LN%d, realloc fail for save child table name of %s.%s\n",
               __func__, __LINE__, dbName, sTblName);
        tmfree(childTblName);
        taos_free_result(res);
        taos_close(taos);
        exit(-1);
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
        char* sTblName, char** childTblNameOfSuperTbl,
        int64_t* childTblCountOfSuperTbl) {

    return getChildNameOfSuperTableWithLimitAndOffset(taos, dbName, sTblName,
            childTblNameOfSuperTbl, childTblCountOfSuperTbl,
            -1, 0);
}

static int getSuperTableFromServer(TAOS * taos, char* dbName,
        SSuperTable*  superTbls) {

  char command[BUFFER_SIZE] = "\0";
  TAOS_RES * res;
  TAOS_ROW row = NULL;
  int count = 0;

  //get schema use cmd: describe superTblName;
  snprintf(command, BUFFER_SIZE, "describe %s.%s", dbName, superTbls->sTblName);
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
      tstrncpy(superTbls->tags[tagIndex].dataType,
              (char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
              min(15, fields[TSDB_DESCRIBE_METRIC_TYPE_INDEX].bytes));
      superTbls->tags[tagIndex].dataLen =
          *((int *)row[TSDB_DESCRIBE_METRIC_LENGTH_INDEX]);
      tstrncpy(superTbls->tags[tagIndex].note,
              (char *)row[TSDB_DESCRIBE_METRIC_NOTE_INDEX],
              fields[TSDB_DESCRIBE_METRIC_NOTE_INDEX].bytes);
      tagIndex++;
    } else {
      tstrncpy(superTbls->columns[columnIndex].field,
              (char *)row[TSDB_DESCRIBE_METRIC_FIELD_INDEX],
              fields[TSDB_DESCRIBE_METRIC_FIELD_INDEX].bytes);
      tstrncpy(superTbls->columns[columnIndex].dataType,
              (char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
              min(15, fields[TSDB_DESCRIBE_METRIC_TYPE_INDEX].bytes));
      superTbls->columns[columnIndex].dataLen =
          *((int *)row[TSDB_DESCRIBE_METRIC_LENGTH_INDEX]);
      tstrncpy(superTbls->columns[columnIndex].note,
              (char *)row[TSDB_DESCRIBE_METRIC_NOTE_INDEX],
              fields[TSDB_DESCRIBE_METRIC_NOTE_INDEX].bytes);
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
      errorPrint("%s() LN%d, alloc memory failed!\n", __func__, __LINE__);
      return -1;
    }
    getAllChildNameOfSuperTable(taos, dbName,
            superTbls->sTblName,
            &superTbls->childTblName,
            &superTbls->childTblCount);
  }
  */
  return 0;
}

static int createSuperTable(
        TAOS * taos, char* dbName,
        SSuperTable*  superTbl) {

    char command[BUFFER_SIZE] = "\0";

    char cols[STRING_LEN] = "\0";
    int colIndex;
    int len = 0;

    int  lenOfOneRow = 0;

    if (superTbl->columnCount == 0) {
        errorPrint("%s() LN%d, super table column count is %d\n",
                __func__, __LINE__, superTbl->columnCount);
        return -1;
    }

    for (colIndex = 0; colIndex < superTbl->columnCount; colIndex++) {
        char* dataType = superTbl->columns[colIndex].dataType;

        if (strcasecmp(dataType, "BINARY") == 0) {
            len += snprintf(cols + len, STRING_LEN - len,
                    ", col%d %s(%d)", colIndex, "BINARY",
                    superTbl->columns[colIndex].dataLen);
            lenOfOneRow += superTbl->columns[colIndex].dataLen + 3;
        } else if (strcasecmp(dataType, "NCHAR") == 0) {
            len += snprintf(cols + len, STRING_LEN - len,
                    ", col%d %s(%d)", colIndex, "NCHAR",
                    superTbl->columns[colIndex].dataLen);
            lenOfOneRow += superTbl->columns[colIndex].dataLen + 3;
        } else if (strcasecmp(dataType, "INT") == 0)  {
            if ((g_args.demo_mode) && (colIndex == 1)) {
                    len += snprintf(cols + len, STRING_LEN - len,
                            ", VOLTAGE INT");
            } else {
                len += snprintf(cols + len, STRING_LEN - len, ", col%d %s", colIndex, "INT");
            }
            lenOfOneRow += 11;
        } else if (strcasecmp(dataType, "BIGINT") == 0)  {
            len += snprintf(cols + len, STRING_LEN - len, ", col%d %s",
                    colIndex, "BIGINT");
            lenOfOneRow += 21;
        } else if (strcasecmp(dataType, "SMALLINT") == 0)  {
            len += snprintf(cols + len, STRING_LEN - len, ", col%d %s",
                    colIndex, "SMALLINT");
            lenOfOneRow += 6;
        } else if (strcasecmp(dataType, "TINYINT") == 0)  {
            len += snprintf(cols + len, STRING_LEN - len, ", col%d %s", colIndex, "TINYINT");
            lenOfOneRow += 4;
        } else if (strcasecmp(dataType, "BOOL") == 0)  {
            len += snprintf(cols + len, STRING_LEN - len, ", col%d %s", colIndex, "BOOL");
            lenOfOneRow += 6;
        } else if (strcasecmp(dataType, "FLOAT") == 0) {
            if (g_args.demo_mode) {
                if (colIndex == 0) {
                    len += snprintf(cols + len, STRING_LEN - len, ", CURRENT FLOAT");
                } else if (colIndex == 2) {
                    len += snprintf(cols + len, STRING_LEN - len, ", PHASE FLOAT");
                }
            } else {
                len += snprintf(cols + len, STRING_LEN - len, ", col%d %s", colIndex, "FLOAT");
            }

            lenOfOneRow += 22;
        } else if (strcasecmp(dataType, "DOUBLE") == 0) {
            len += snprintf(cols + len, STRING_LEN - len, ", col%d %s",
                    colIndex, "DOUBLE");
            lenOfOneRow += 42;
        }  else if (strcasecmp(dataType, "TIMESTAMP") == 0) {
            len += snprintf(cols + len, STRING_LEN - len, ", col%d %s",
                    colIndex, "TIMESTAMP");
            lenOfOneRow += 21;
        } else {
            taos_close(taos);
            errorPrint("%s() LN%d, config error data type : %s\n",
                    __func__, __LINE__, dataType);
            exit(-1);
        }
    }

    superTbl->lenOfOneRow = lenOfOneRow + 20; // timestamp

    // save for creating child table
    superTbl->colsOfCreateChildTable = (char*)calloc(len+20, 1);
    if (NULL == superTbl->colsOfCreateChildTable) {
        errorPrint("%s() LN%d, Failed when calloc, size:%d",
                __func__, __LINE__, len+1);
        taos_close(taos);
        exit(-1);
    }

    snprintf(superTbl->colsOfCreateChildTable, len+20, "(ts timestamp%s)", cols);
    verbosePrint("%s() LN%d: %s\n",
            __func__, __LINE__, superTbl->colsOfCreateChildTable);

    if (superTbl->tagCount == 0) {
        errorPrint("%s() LN%d, super table tag count is %d\n",
                __func__, __LINE__, superTbl->tagCount);
        return -1;
    }

    char tags[STRING_LEN] = "\0";
    int tagIndex;
    len = 0;

    int lenOfTagOfOneRow = 0;
    len += snprintf(tags + len, STRING_LEN - len, "(");
    for (tagIndex = 0; tagIndex < superTbl->tagCount; tagIndex++) {
        char* dataType = superTbl->tags[tagIndex].dataType;

        if (strcasecmp(dataType, "BINARY") == 0) {
            if ((g_args.demo_mode) && (tagIndex == 1)) {
                len += snprintf(tags + len, STRING_LEN - len,
                        "loction BINARY(%d), ",
                        superTbl->tags[tagIndex].dataLen);
            } else {
                len += snprintf(tags + len, STRING_LEN - len, "t%d %s(%d), ",
                        tagIndex, "BINARY", superTbl->tags[tagIndex].dataLen);
            }
            lenOfTagOfOneRow += superTbl->tags[tagIndex].dataLen + 3;
        } else if (strcasecmp(dataType, "NCHAR") == 0) {
            len += snprintf(tags + len, STRING_LEN - len, "t%d %s(%d), ", tagIndex,
                    "NCHAR", superTbl->tags[tagIndex].dataLen);
            lenOfTagOfOneRow += superTbl->tags[tagIndex].dataLen + 3;
        } else if (strcasecmp(dataType, "INT") == 0)  {
            if ((g_args.demo_mode) && (tagIndex == 0)) {
                len += snprintf(tags + len, STRING_LEN - len, "groupId INT, ");
            } else {
                len += snprintf(tags + len, STRING_LEN - len, "t%d %s, ", tagIndex,
                    "INT");
            }
            lenOfTagOfOneRow += superTbl->tags[tagIndex].dataLen + 11;
        } else if (strcasecmp(dataType, "BIGINT") == 0)  {
            len += snprintf(tags + len, STRING_LEN - len, "t%d %s, ", tagIndex,
                    "BIGINT");
            lenOfTagOfOneRow += superTbl->tags[tagIndex].dataLen + 21;
        } else if (strcasecmp(dataType, "SMALLINT") == 0)  {
            len += snprintf(tags + len, STRING_LEN - len, "t%d %s, ", tagIndex,
                    "SMALLINT");
            lenOfTagOfOneRow += superTbl->tags[tagIndex].dataLen + 6;
        } else if (strcasecmp(dataType, "TINYINT") == 0)  {
            len += snprintf(tags + len, STRING_LEN - len, "t%d %s, ", tagIndex,
                    "TINYINT");
            lenOfTagOfOneRow += superTbl->tags[tagIndex].dataLen + 4;
        } else if (strcasecmp(dataType, "BOOL") == 0)  {
            len += snprintf(tags + len, STRING_LEN - len, "t%d %s, ", tagIndex,
                    "BOOL");
            lenOfTagOfOneRow += superTbl->tags[tagIndex].dataLen + 6;
        } else if (strcasecmp(dataType, "FLOAT") == 0) {
            len += snprintf(tags + len, STRING_LEN - len, "t%d %s, ", tagIndex,
                    "FLOAT");
            lenOfTagOfOneRow += superTbl->tags[tagIndex].dataLen + 22;
        } else if (strcasecmp(dataType, "DOUBLE") == 0) {
            len += snprintf(tags + len, STRING_LEN - len, "t%d %s, ", tagIndex,
                    "DOUBLE");
            lenOfTagOfOneRow += superTbl->tags[tagIndex].dataLen + 42;
        } else {
            taos_close(taos);
            errorPrint("%s() LN%d, config error tag type : %s\n",
                    __func__, __LINE__, dataType);
            exit(-1);
        }
    }

    len -= 2;
    len += snprintf(tags + len, STRING_LEN - len, ")");

    superTbl->lenOfTagOfOneRow = lenOfTagOfOneRow;

    snprintf(command, BUFFER_SIZE,
            "create table if not exists %s.%s (ts timestamp%s) tags %s",
            dbName, superTbl->sTblName, cols, tags);
    if (0 != queryDbExec(taos, command, NO_INSERT_TYPE, false)) {
        errorPrint( "create supertable %s failed!\n\n",
                superTbl->sTblName);
        return -1;
    }
    debugPrint("create supertable %s success!\n\n", superTbl->sTblName);
    return 0;
}

static int createDatabasesAndStables() {
  TAOS * taos = NULL;
  int    ret = 0;
  taos = taos_connect(g_Dbs.host, g_Dbs.user, g_Dbs.password, NULL, g_Dbs.port);
  if (taos == NULL) {
    errorPrint( "Failed to connect to TDengine, reason:%s\n", taos_errstr(NULL));
    return -1;
  }
  char command[BUFFER_SIZE] = "\0";

  for (int i = 0; i < g_Dbs.dbCount; i++) {
    if (g_Dbs.db[i].drop) {
      sprintf(command, "drop database if exists %s;", g_Dbs.db[i].dbName);
      if (0 != queryDbExec(taos, command, NO_INSERT_TYPE, false)) {
        taos_close(taos);
        return -1;
      }

      int dataLen = 0;
      dataLen += snprintf(command + dataLen,
          BUFFER_SIZE - dataLen, "create database if not exists %s", g_Dbs.db[i].dbName);

      if (g_Dbs.db[i].dbCfg.blocks > 0) {
        dataLen += snprintf(command + dataLen,
            BUFFER_SIZE - dataLen, " blocks %d", g_Dbs.db[i].dbCfg.blocks);
      }
      if (g_Dbs.db[i].dbCfg.cache > 0) {
        dataLen += snprintf(command + dataLen,
            BUFFER_SIZE - dataLen, " cache %d", g_Dbs.db[i].dbCfg.cache);
      }
      if (g_Dbs.db[i].dbCfg.days > 0) {
        dataLen += snprintf(command + dataLen,
            BUFFER_SIZE - dataLen, " days %d", g_Dbs.db[i].dbCfg.days);
      }
      if (g_Dbs.db[i].dbCfg.keep > 0) {
        dataLen += snprintf(command + dataLen,
            BUFFER_SIZE - dataLen, " keep %d", g_Dbs.db[i].dbCfg.keep);
      }
      if (g_Dbs.db[i].dbCfg.quorum > 1) {
        dataLen += snprintf(command + dataLen,
            BUFFER_SIZE - dataLen, " quorum %d", g_Dbs.db[i].dbCfg.quorum);
      }
      if (g_Dbs.db[i].dbCfg.replica > 0) {
        dataLen += snprintf(command + dataLen,
            BUFFER_SIZE - dataLen, " replica %d", g_Dbs.db[i].dbCfg.replica);
      }
      if (g_Dbs.db[i].dbCfg.update > 0) {
        dataLen += snprintf(command + dataLen,
            BUFFER_SIZE - dataLen, " update %d", g_Dbs.db[i].dbCfg.update);
      }
      //if (g_Dbs.db[i].dbCfg.maxtablesPerVnode > 0) {
      //  dataLen += snprintf(command + dataLen,
      //  BUFFER_SIZE - dataLen, "tables %d ", g_Dbs.db[i].dbCfg.maxtablesPerVnode);
      //}
      if (g_Dbs.db[i].dbCfg.minRows > 0) {
        dataLen += snprintf(command + dataLen,
            BUFFER_SIZE - dataLen, " minrows %d", g_Dbs.db[i].dbCfg.minRows);
      }
      if (g_Dbs.db[i].dbCfg.maxRows > 0) {
        dataLen += snprintf(command + dataLen,
            BUFFER_SIZE - dataLen, " maxrows %d", g_Dbs.db[i].dbCfg.maxRows);
      }
      if (g_Dbs.db[i].dbCfg.comp > 0) {
        dataLen += snprintf(command + dataLen,
            BUFFER_SIZE - dataLen, " comp %d", g_Dbs.db[i].dbCfg.comp);
      }
      if (g_Dbs.db[i].dbCfg.walLevel > 0) {
        dataLen += snprintf(command + dataLen,
            BUFFER_SIZE - dataLen, " wal %d", g_Dbs.db[i].dbCfg.walLevel);
      }
      if (g_Dbs.db[i].dbCfg.cacheLast > 0) {
        dataLen += snprintf(command + dataLen,
            BUFFER_SIZE - dataLen, " cachelast %d", g_Dbs.db[i].dbCfg.cacheLast);
      }
      if (g_Dbs.db[i].dbCfg.fsync > 0) {
        dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                " fsync %d", g_Dbs.db[i].dbCfg.fsync);
      }
      if ((0 == strncasecmp(g_Dbs.db[i].dbCfg.precision, "ms", strlen("ms")))
              || (0 == strncasecmp(g_Dbs.db[i].dbCfg.precision,
                      "us", strlen("us")))) {
        dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen,
                " precision \'%s\';", g_Dbs.db[i].dbCfg.precision);
      }

      if (0 != queryDbExec(taos, command, NO_INSERT_TYPE, false)) {
        taos_close(taos);
        errorPrint( "\ncreate database %s failed!\n\n", g_Dbs.db[i].dbName);
        return -1;
      }
      printf("\ncreate database %s success!\n\n", g_Dbs.db[i].dbName);
    }

    debugPrint("%s() LN%d supertbl count:%"PRIu64"\n",
            __func__, __LINE__, g_Dbs.db[i].superTblCount);

    int validStbCount = 0;

    for (uint64_t j = 0; j < g_Dbs.db[i].superTblCount; j++) {
      sprintf(command, "describe %s.%s;", g_Dbs.db[i].dbName,
              g_Dbs.db[i].superTbls[j].sTblName);
      ret = queryDbExec(taos, command, NO_INSERT_TYPE, true);

      if ((ret != 0) || (g_Dbs.db[i].drop)) {
        ret = createSuperTable(taos, g_Dbs.db[i].dbName,
                &g_Dbs.db[i].superTbls[j]);

        if (0 != ret) {
          errorPrint("create super table %"PRIu64" failed!\n\n", j);
          continue;
        }
      }

      ret = getSuperTableFromServer(taos, g_Dbs.db[i].dbName,
                &g_Dbs.db[i].superTbls[j]);
      if (0 != ret) {
        errorPrint("\nget super table %s.%s info failed!\n\n",
                g_Dbs.db[i].dbName, g_Dbs.db[i].superTbls[j].sTblName);
        continue;
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
  SSuperTable* superTblInfo = pThreadInfo->superTblInfo;

  uint64_t  lastPrintTime = taosGetTimestampMs();

  int buff_len;
  buff_len = BUFFER_SIZE / 8;

  pThreadInfo->buffer = calloc(buff_len, 1);
  if (pThreadInfo->buffer == NULL) {
    errorPrint("%s() LN%d, Memory allocated failed!\n", __func__, __LINE__);
    exit(-1);
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
              "create table if not exists %s.%s%"PRIu64" %s;",
              pThreadInfo->db_name,
              g_args.tb_prefix, i,
              pThreadInfo->cols);
    } else {
      if (superTblInfo == NULL) {
        errorPrint("%s() LN%d, use metric, but super table info is NULL\n",
                  __func__, __LINE__);
        free(pThreadInfo->buffer);
        exit(-1);
      } else {
        if (0 == len) {
          batchNum = 0;
          memset(pThreadInfo->buffer, 0, buff_len);
          len += snprintf(pThreadInfo->buffer + len,
                  buff_len - len, "create table ");
        }
        char* tagsValBuf = NULL;
        if (0 == superTblInfo->tagSource) {
          tagsValBuf = generateTagVaulesForStb(superTblInfo, i);
        } else {
          tagsValBuf = getTagValueFromTagSample(
                  superTblInfo,
                  i % superTblInfo->tagSampleCount);
        }
        if (NULL == tagsValBuf) {
          free(pThreadInfo->buffer);
          return NULL;
        }
        len += snprintf(pThreadInfo->buffer + len,
                buff_len - len,
                "if not exists %s.%s%"PRIu64" using %s.%s tags %s ",
                pThreadInfo->db_name, superTblInfo->childTblPrefix,
                i, pThreadInfo->db_name,
                superTblInfo->sTblName, tagsValBuf);
        free(tagsValBuf);
        batchNum++;
        if ((batchNum < superTblInfo->batchCreateTableNum)
                && ((buff_len - len)
                    >= (superTblInfo->lenOfTagOfOneRow + 256))) {
          continue;
        }
      }
    }

    len = 0;
    if (0 != queryDbExec(pThreadInfo->taos, pThreadInfo->buffer,
                NO_INSERT_TYPE, false)){
      errorPrint( "queryDbExec() failed. buffer:\n%s\n", pThreadInfo->buffer);
      free(pThreadInfo->buffer);
      return NULL;
    }

    uint64_t  currentPrintTime = taosGetTimestampMs();
    if (currentPrintTime - lastPrintTime > 30*1000) {
      printf("thread[%d] already create %"PRIu64" - %"PRIu64" tables\n",
              pThreadInfo->threadID, pThreadInfo->start_table_from, i);
      lastPrintTime = currentPrintTime;
    }
  }

  if (0 != len) {
    if (0 != queryDbExec(pThreadInfo->taos, pThreadInfo->buffer,
                NO_INSERT_TYPE, false)) {
      errorPrint( "queryDbExec() failed. buffer:\n%s\n", pThreadInfo->buffer);
    }
  }

  free(pThreadInfo->buffer);
  return NULL;
}

static int startMultiThreadCreateChildTable(
        char* cols, int threads, uint64_t tableFrom, int64_t ntables,
        char* db_name, SSuperTable* superTblInfo) {

  pthread_t *pids = calloc(1, threads * sizeof(pthread_t));
  threadInfo *infos = calloc(1, threads * sizeof(threadInfo));

  if ((NULL == pids) || (NULL == infos)) {
    printf("malloc failed\n");
    exit(-1);
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
    pThreadInfo->superTblInfo = superTblInfo;
    verbosePrint("%s() %d db_name: %s\n", __func__, __LINE__, db_name);
    pThreadInfo->taos = taos_connect(
            g_Dbs.host,
            g_Dbs.user,
            g_Dbs.password,
            db_name,
            g_Dbs.port);
    if (pThreadInfo->taos == NULL) {
      errorPrint( "%s() LN%d, Failed to connect to TDengine, reason:%s\n",
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
    pthread_create(pids + i, NULL, createTable, pThreadInfo);
  }

  for (int i = 0; i < threads; i++) {
    pthread_join(pids[i], NULL);
  }

  for (int i = 0; i < threads; i++) {
    threadInfo *pThreadInfo = infos + i;
    taos_close(pThreadInfo->taos);
  }

  free(pids);
  free(infos);

  return 0;
}

static void createChildTables() {
    char tblColsBuf[MAX_SQL_SIZE];
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
                    g_totalChildTables += g_Dbs.db[i].superTbls[j].childTblCount;

                    verbosePrint("%s() LN%d: create %"PRId64" child tables from %"PRIu64"\n",
                            __func__, __LINE__, g_totalChildTables, startFrom);

                    startMultiThreadCreateChildTable(
                            g_Dbs.db[i].superTbls[j].colsOfCreateChildTable,
                            g_Dbs.threadCountByCreateTbl,
                            startFrom,
                            g_Dbs.db[i].superTbls[j].childTblCount,
                            g_Dbs.db[i].dbName, &(g_Dbs.db[i].superTbls[j]));
                }
            }
        } else {
            // normal table
            len = snprintf(tblColsBuf, MAX_SQL_SIZE, "(TS TIMESTAMP");
            for (int j = 0; j < g_args.num_of_CPR; j++) {
                if ((strncasecmp(g_args.datatype[j], "BINARY", strlen("BINARY")) == 0)
                        || (strncasecmp(g_args.datatype[j],
                                "NCHAR", strlen("NCHAR")) == 0)) {
                    snprintf(tblColsBuf + len, MAX_SQL_SIZE - len,
                            ", COL%d %s(%d)", j, g_args.datatype[j], g_args.len_of_binary);
                } else {
                    snprintf(tblColsBuf + len, MAX_SQL_SIZE - len,
                            ", COL%d %s", j, g_args.datatype[j]);
                }
                len = strlen(tblColsBuf);
            }

            snprintf(tblColsBuf + len, MAX_SQL_SIZE - len, ")");

            verbosePrint("%s() LN%d: dbName: %s num of tb: %"PRId64" schema: %s\n",
                    __func__, __LINE__,
                    g_Dbs.db[i].dbName, g_args.num_of_tables, tblColsBuf);
            startMultiThreadCreateChildTable(
                    tblColsBuf,
                    g_Dbs.threadCountByCreateTbl,
                    0,
                    g_args.num_of_tables,
                    g_Dbs.db[i].dbName,
                    NULL);
        }
    }
}

/*
  Read 10000 lines at most. If more than 10000 lines, continue to read after using
*/
static int readTagFromCsvFileToMem(SSuperTable  * superTblInfo) {
  size_t  n = 0;
  ssize_t readLen = 0;
  char *  line = NULL;

  FILE *fp = fopen(superTblInfo->tagsFile, "r");
  if (fp == NULL) {
    printf("Failed to open tags file: %s, reason:%s\n",
            superTblInfo->tagsFile, strerror(errno));
    return -1;
  }

  if (superTblInfo->tagDataBuf) {
    free(superTblInfo->tagDataBuf);
    superTblInfo->tagDataBuf = NULL;
  }

  int tagCount = 10000;
  int count = 0;
  char* tagDataBuf = calloc(1, superTblInfo->lenOfTagOfOneRow * tagCount);
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

    memcpy(tagDataBuf + count * superTblInfo->lenOfTagOfOneRow, line, readLen);
    count++;

    if (count >= tagCount - 1) {
      char *tmp = realloc(tagDataBuf,
              (size_t)tagCount*1.5*superTblInfo->lenOfTagOfOneRow);
      if (tmp != NULL) {
        tagDataBuf = tmp;
        tagCount = (int)(tagCount*1.5);
        memset(tagDataBuf + count*superTblInfo->lenOfTagOfOneRow,
                0, (size_t)((tagCount-count)*superTblInfo->lenOfTagOfOneRow));
      } else {
        // exit, if allocate more memory failed
        printf("realloc fail for save tag val from %s\n", superTblInfo->tagsFile);
        tmfree(tagDataBuf);
        free(line);
        fclose(fp);
        return -1;
      }
    }
  }

  superTblInfo->tagDataBuf = tagDataBuf;
  superTblInfo->tagSampleCount = count;

  free(line);
  fclose(fp);
  return 0;
}

/*
  Read 10000 lines at most. If more than 10000 lines, continue to read after using
*/
static int readSampleFromCsvFileToMem(
        SSuperTable* superTblInfo) {
  size_t  n = 0;
  ssize_t readLen = 0;
  char *  line = NULL;
  int getRows = 0;

  FILE*  fp = fopen(superTblInfo->sampleFile, "r");
  if (fp == NULL) {
      errorPrint( "Failed to open sample file: %s, reason:%s\n",
              superTblInfo->sampleFile, strerror(errno));
      return -1;
  }

  assert(superTblInfo->sampleDataBuf);
  memset(superTblInfo->sampleDataBuf, 0,
          MAX_SAMPLES_ONCE_FROM_FILE * superTblInfo->lenOfOneRow);
  while(1) {
    readLen = tgetline(&line, &n, fp);
    if (-1 == readLen) {
      if(0 != fseek(fp, 0, SEEK_SET)) {
        errorPrint( "Failed to fseek file: %s, reason:%s\n",
                superTblInfo->sampleFile, strerror(errno));
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

    if (readLen > superTblInfo->lenOfOneRow) {
      printf("sample row len[%d] overflow define schema len[%"PRIu64"], so discard this row\n",
              (int32_t)readLen, superTblInfo->lenOfOneRow);
      continue;
    }

    memcpy(superTblInfo->sampleDataBuf + getRows * superTblInfo->lenOfOneRow,
          line, readLen);
    getRows++;

    if (getRows == MAX_SAMPLES_ONCE_FROM_FILE) {
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
    printf("ERROR: failed to read json, columns not found\n");
    goto PARSE_OVER;
  } else if (NULL == columns) {
    superTbls->columnCount = 0;
    superTbls->tagCount    = 0;
    return true;
  }

  int columnSize = cJSON_GetArraySize(columns);
  if ((columnSize + 1/* ts */) > TSDB_MAX_COLUMNS) {
    errorPrint("%s() LN%d, failed to read json, column size overflow, max column size is %d\n",
            __func__, __LINE__, TSDB_MAX_COLUMNS);
    goto PARSE_OVER;
  }

  int count = 1;
  int index = 0;
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
      errorPrint("%s() LN%d, failed to read json, column count not found\n",
          __func__, __LINE__);
      goto PARSE_OVER;
    } else {
      count = 1;
    }

    // column info
    memset(&columnCase, 0, sizeof(StrColumn));
    cJSON *dataType = cJSON_GetObjectItem(column, "type");
    if (!dataType || dataType->type != cJSON_String
        || dataType->valuestring == NULL) {
      errorPrint("%s() LN%d: failed to read json, column type not found\n",
          __func__, __LINE__);
      goto PARSE_OVER;
    }
    //tstrncpy(superTbls->columns[k].dataType, dataType->valuestring, MAX_TB_NAME_SIZE);
    tstrncpy(columnCase.dataType, dataType->valuestring, strlen(dataType->valuestring) + 1);

    cJSON* dataLen = cJSON_GetObjectItem(column, "len");
    if (dataLen && dataLen->type == cJSON_Number) {
      columnCase.dataLen = dataLen->valueint;
    } else if (dataLen && dataLen->type != cJSON_Number) {
      debugPrint("%s() LN%d: failed to read json, column len not found\n",
          __func__, __LINE__);
      goto PARSE_OVER;
    } else {
      columnCase.dataLen = 8;
    }

    for (int n = 0; n < count; ++n) {
      tstrncpy(superTbls->columns[index].dataType,
              columnCase.dataType, strlen(columnCase.dataType) + 1);
      superTbls->columns[index].dataLen = columnCase.dataLen;
      index++;
    }
  }

  if ((index + 1 /* ts */) > MAX_NUM_COLUMNS) {
    errorPrint("%s() LN%d, failed to read json, column size overflow, allowed max column size is %d\n",
            __func__, __LINE__, MAX_NUM_COLUMNS);
    goto PARSE_OVER;
  }

  superTbls->columnCount = index;

  count = 1;
  index = 0;
  // tags
  cJSON *tags = cJSON_GetObjectItem(stbInfo, "tags");
  if (!tags || tags->type != cJSON_Array) {
    errorPrint("%s() LN%d, failed to read json, tags not found\n",
        __func__, __LINE__);
    goto PARSE_OVER;
  }

  int tagSize = cJSON_GetArraySize(tags);
  if (tagSize > TSDB_MAX_TAGS) {
    errorPrint("%s() LN%d, failed to read json, tags size overflow, max tag size is %d\n",
        __func__, __LINE__, TSDB_MAX_TAGS);
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
      printf("ERROR: failed to read json, column count not found\n");
      goto PARSE_OVER;
    } else {
      count = 1;
    }

    // column info
    memset(&columnCase, 0, sizeof(StrColumn));
    cJSON *dataType = cJSON_GetObjectItem(tag, "type");
    if (!dataType || dataType->type != cJSON_String
        || dataType->valuestring == NULL) {
      errorPrint("%s() LN%d, failed to read json, tag type not found\n",
          __func__, __LINE__);
      goto PARSE_OVER;
    }
    tstrncpy(columnCase.dataType, dataType->valuestring, strlen(dataType->valuestring) + 1);

    cJSON* dataLen = cJSON_GetObjectItem(tag, "len");
    if (dataLen && dataLen->type == cJSON_Number) {
      columnCase.dataLen = dataLen->valueint;
    } else if (dataLen && dataLen->type != cJSON_Number) {
      errorPrint("%s() LN%d, failed to read json, column len not found\n",
          __func__, __LINE__);
      goto PARSE_OVER;
    } else {
      columnCase.dataLen = 0;
    }

    for (int n = 0; n < count; ++n) {
      tstrncpy(superTbls->tags[index].dataType, columnCase.dataType,
          strlen(columnCase.dataType) + 1);
      superTbls->tags[index].dataLen = columnCase.dataLen;
      index++;
    }
  }

  if (index > TSDB_MAX_TAGS) {
    errorPrint("%s() LN%d, failed to read json, tags size overflow, allowed max tag count is %d\n",
        __func__, __LINE__, TSDB_MAX_TAGS);
    goto PARSE_OVER;
  }

  superTbls->tagCount = index;

  if ((superTbls->columnCount + superTbls->tagCount + 1 /* ts */) > TSDB_MAX_COLUMNS) {
    errorPrint("%s() LN%d, columns + tags is more than allowed max columns count: %d\n",
        __func__, __LINE__, TSDB_MAX_COLUMNS);
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
    printf("ERROR: failed to read json, host not found\n");
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
    tstrncpy(g_Dbs.password, password->valuestring, MAX_PASSWORD_SIZE);
  } else if (!password) {
    tstrncpy(g_Dbs.password, "taosdata", MAX_PASSWORD_SIZE);
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
    printf("ERROR: failed to read json, threads not found\n");
    goto PARSE_OVER;
  }

  cJSON* threads2 = cJSON_GetObjectItem(root, "thread_count_create_tbl");
  if (threads2 && threads2->type == cJSON_Number) {
    g_Dbs.threadCountByCreateTbl = threads2->valueint;
  } else if (!threads2) {
    g_Dbs.threadCountByCreateTbl = 1;
  } else {
    errorPrint("%s() LN%d, failed to read json, threads2 not found\n",
            __func__, __LINE__);
    goto PARSE_OVER;
  }

  cJSON* gInsertInterval = cJSON_GetObjectItem(root, "insert_interval");
  if (gInsertInterval && gInsertInterval->type == cJSON_Number) {
    if (gInsertInterval->valueint <0) {
      errorPrint("%s() LN%d, failed to read json, insert interval input mistake\n",
            __func__, __LINE__);
      goto PARSE_OVER;
    }
    g_args.insert_interval = gInsertInterval->valueint;
  } else if (!gInsertInterval) {
    g_args.insert_interval = 0;
  } else {
    errorPrint("%s() LN%d, failed to read json, insert_interval input mistake\n",
        __func__, __LINE__);
    goto PARSE_OVER;
  }

  cJSON* interlaceRows = cJSON_GetObjectItem(root, "interlace_rows");
  if (interlaceRows && interlaceRows->type == cJSON_Number) {
    if (interlaceRows->valueint < 0) {
      errorPrint("%s() LN%d, failed to read json, interlace_rows input mistake\n",
        __func__, __LINE__);
      goto PARSE_OVER;

    }
    g_args.interlace_rows = interlaceRows->valueint;
  } else if (!interlaceRows) {
    g_args.interlace_rows = 0; // 0 means progressive mode, > 0 mean interlace mode. max value is less or equ num_of_records_per_req
  } else {
    errorPrint("%s() LN%d, failed to read json, interlace_rows input mistake\n",
        __func__, __LINE__);
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
    g_args.num_of_RPR = numRecPerReq->valueint;
  } else if (!numRecPerReq) {
    g_args.num_of_RPR = MAX_RECORDS_PER_REQ;
  } else {
    errorPrint("%s() LN%d, failed to read json, num_of_records_per_req not found\n",
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
  if (g_args.interlace_rows > g_args.num_of_RPR) {
      printf("NOTICE: interlace rows value %u > num_of_records_per_req %u\n\n",
              g_args.interlace_rows, g_args.num_of_RPR);
      printf("        interlace rows value will be set to num_of_records_per_req %u\n\n",
              g_args.num_of_RPR);
      prompt();
      g_args.interlace_rows = g_args.num_of_RPR;
  }

  cJSON* dbs = cJSON_GetObjectItem(root, "databases");
  if (!dbs || dbs->type != cJSON_Array) {
    printf("ERROR: failed to read json, databases not found\n");
    goto PARSE_OVER;
  }

  int dbSize = cJSON_GetArraySize(dbs);
  if (dbSize > MAX_DB_COUNT) {
    errorPrint(
            "ERROR: failed to read json, databases size overflow, max database is %d\n",
            MAX_DB_COUNT);
    goto PARSE_OVER;
  }

  g_Dbs.dbCount = dbSize;
  for (int i = 0; i < dbSize; ++i) {
    cJSON* dbinfos = cJSON_GetArrayItem(dbs, i);
    if (dbinfos == NULL) continue;

    // dbinfo
    cJSON *dbinfo = cJSON_GetObjectItem(dbinfos, "dbinfo");
    if (!dbinfo || dbinfo->type != cJSON_Object) {
      printf("ERROR: failed to read json, dbinfo not found\n");
      goto PARSE_OVER;
    }

    cJSON *dbName = cJSON_GetObjectItem(dbinfo, "name");
    if (!dbName || dbName->type != cJSON_String || dbName->valuestring == NULL) {
      printf("ERROR: failed to read json, db name not found\n");
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
      errorPrint("%s() LN%d, failed to read json, drop input mistake\n",
              __func__, __LINE__);
      goto PARSE_OVER;
    }

    cJSON *precision = cJSON_GetObjectItem(dbinfo, "precision");
    if (precision && precision->type == cJSON_String
            && precision->valuestring != NULL) {
      tstrncpy(g_Dbs.db[i].dbCfg.precision, precision->valuestring,
              8);
    } else if (!precision) {
      memset(g_Dbs.db[i].dbCfg.precision, 0, 8);
    } else {
      printf("ERROR: failed to read json, precision not found\n");
      goto PARSE_OVER;
    }

    cJSON* update = cJSON_GetObjectItem(dbinfo, "update");
    if (update && update->type == cJSON_Number) {
      g_Dbs.db[i].dbCfg.update = update->valueint;
    } else if (!update) {
      g_Dbs.db[i].dbCfg.update = -1;
    } else {
      printf("ERROR: failed to read json, update not found\n");
      goto PARSE_OVER;
    }

    cJSON* replica = cJSON_GetObjectItem(dbinfo, "replica");
    if (replica && replica->type == cJSON_Number) {
      g_Dbs.db[i].dbCfg.replica = replica->valueint;
    } else if (!replica) {
      g_Dbs.db[i].dbCfg.replica = -1;
    } else {
      printf("ERROR: failed to read json, replica not found\n");
      goto PARSE_OVER;
    }

    cJSON* keep = cJSON_GetObjectItem(dbinfo, "keep");
    if (keep && keep->type == cJSON_Number) {
      g_Dbs.db[i].dbCfg.keep = keep->valueint;
    } else if (!keep) {
      g_Dbs.db[i].dbCfg.keep = -1;
    } else {
     printf("ERROR: failed to read json, keep not found\n");
     goto PARSE_OVER;
    }

    cJSON* days = cJSON_GetObjectItem(dbinfo, "days");
    if (days && days->type == cJSON_Number) {
      g_Dbs.db[i].dbCfg.days = days->valueint;
    } else if (!days) {
      g_Dbs.db[i].dbCfg.days = -1;
    } else {
     printf("ERROR: failed to read json, days not found\n");
     goto PARSE_OVER;
    }

    cJSON* cache = cJSON_GetObjectItem(dbinfo, "cache");
    if (cache && cache->type == cJSON_Number) {
      g_Dbs.db[i].dbCfg.cache = cache->valueint;
    } else if (!cache) {
      g_Dbs.db[i].dbCfg.cache = -1;
    } else {
     printf("ERROR: failed to read json, cache not found\n");
     goto PARSE_OVER;
    }

    cJSON* blocks= cJSON_GetObjectItem(dbinfo, "blocks");
    if (blocks && blocks->type == cJSON_Number) {
      g_Dbs.db[i].dbCfg.blocks = blocks->valueint;
    } else if (!blocks) {
      g_Dbs.db[i].dbCfg.blocks = -1;
    } else {
     printf("ERROR: failed to read json, block not found\n");
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
     printf("ERROR: failed to read json, minRows not found\n");
     goto PARSE_OVER;
    }

    cJSON* maxRows= cJSON_GetObjectItem(dbinfo, "maxRows");
    if (maxRows && maxRows->type == cJSON_Number) {
      g_Dbs.db[i].dbCfg.maxRows = maxRows->valueint;
    } else if (!maxRows) {
      g_Dbs.db[i].dbCfg.maxRows = 0;    // 0 means default
    } else {
     printf("ERROR: failed to read json, maxRows not found\n");
     goto PARSE_OVER;
    }

    cJSON* comp= cJSON_GetObjectItem(dbinfo, "comp");
    if (comp && comp->type == cJSON_Number) {
      g_Dbs.db[i].dbCfg.comp = comp->valueint;
    } else if (!comp) {
      g_Dbs.db[i].dbCfg.comp = -1;
    } else {
     printf("ERROR: failed to read json, comp not found\n");
     goto PARSE_OVER;
    }

    cJSON* walLevel= cJSON_GetObjectItem(dbinfo, "walLevel");
    if (walLevel && walLevel->type == cJSON_Number) {
      g_Dbs.db[i].dbCfg.walLevel = walLevel->valueint;
    } else if (!walLevel) {
      g_Dbs.db[i].dbCfg.walLevel = -1;
    } else {
     printf("ERROR: failed to read json, walLevel not found\n");
     goto PARSE_OVER;
    }

    cJSON* cacheLast= cJSON_GetObjectItem(dbinfo, "cachelast");
    if (cacheLast && cacheLast->type == cJSON_Number) {
      g_Dbs.db[i].dbCfg.cacheLast = cacheLast->valueint;
    } else if (!cacheLast) {
      g_Dbs.db[i].dbCfg.cacheLast = -1;
    } else {
     printf("ERROR: failed to read json, cacheLast not found\n");
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

    cJSON* fsync= cJSON_GetObjectItem(dbinfo, "fsync");
    if (fsync && fsync->type == cJSON_Number) {
      g_Dbs.db[i].dbCfg.fsync = fsync->valueint;
    } else if (!fsync) {
      g_Dbs.db[i].dbCfg.fsync = -1;
    } else {
      errorPrint("%s() LN%d, failed to read json, fsync input mistake\n",
              __func__, __LINE__);
      goto PARSE_OVER;
    }

    // super_talbes
    cJSON *stables = cJSON_GetObjectItem(dbinfos, "super_tables");
    if (!stables || stables->type != cJSON_Array) {
      errorPrint("%s() LN%d, failed to read json, super_tables not found\n",
              __func__, __LINE__);
      goto PARSE_OVER;
    }

    int stbSize = cJSON_GetArraySize(stables);
    if (stbSize > MAX_SUPER_TABLE_COUNT) {
      errorPrint(
              "%s() LN%d, failed to read json, supertable size overflow, max supertable is %d\n",
              __func__, __LINE__, MAX_SUPER_TABLE_COUNT);
      goto PARSE_OVER;
    }

    g_Dbs.db[i].superTblCount = stbSize;
    for (int j = 0; j < stbSize; ++j) {
      cJSON* stbInfo = cJSON_GetArrayItem(stables, j);
      if (stbInfo == NULL) continue;

      // dbinfo
      cJSON *stbName = cJSON_GetObjectItem(stbInfo, "name");
      if (!stbName || stbName->type != cJSON_String
              || stbName->valuestring == NULL) {
        errorPrint("%s() LN%d, failed to read json, stb name not found\n",
                __func__, __LINE__);
        goto PARSE_OVER;
      }
      tstrncpy(g_Dbs.db[i].superTbls[j].sTblName, stbName->valuestring,
              TSDB_TABLE_NAME_LEN);

      cJSON *prefix = cJSON_GetObjectItem(stbInfo, "childtable_prefix");
      if (!prefix || prefix->type != cJSON_String || prefix->valuestring == NULL) {
        printf("ERROR: failed to read json, childtable_prefix not found\n");
        goto PARSE_OVER;
      }
      tstrncpy(g_Dbs.db[i].superTbls[j].childTblPrefix, prefix->valuestring,
              TSDB_TABLE_NAME_LEN - 20);

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
          printf("ERROR: failed to read json, auto_create_table not found\n");
          goto PARSE_OVER;
      }

      cJSON* batchCreateTbl = cJSON_GetObjectItem(stbInfo, "batch_create_tbl_num");
      if (batchCreateTbl && batchCreateTbl->type == cJSON_Number) {
        g_Dbs.db[i].superTbls[j].batchCreateTableNum = batchCreateTbl->valueint;
      } else if (!batchCreateTbl) {
        g_Dbs.db[i].superTbls[j].batchCreateTableNum = 1000;
      } else {
        printf("ERROR: failed to read json, batch_create_tbl_num not found\n");
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
        errorPrint("%s() LN%d, failed to read json, child_table_exists not found\n",
                __func__, __LINE__);
        goto PARSE_OVER;
      }

      if (TBL_ALREADY_EXISTS == g_Dbs.db[i].superTbls[j].childTblExists) {
          g_Dbs.db[i].superTbls[j].autoCreateTable = PRE_CREATE_SUBTBL;
      }

      cJSON* count = cJSON_GetObjectItem(stbInfo, "childtable_count");
      if (!count || count->type != cJSON_Number || 0 >= count->valueint) {
        errorPrint("%s() LN%d, failed to read json, childtable_count input mistake\n",
                __func__, __LINE__);
        goto PARSE_OVER;
      }
      g_Dbs.db[i].superTbls[j].childTblCount = count->valueint;

      cJSON *dataSource = cJSON_GetObjectItem(stbInfo, "data_source");
      if (dataSource && dataSource->type == cJSON_String
              && dataSource->valuestring != NULL) {
        tstrncpy(g_Dbs.db[i].superTbls[j].dataSource,
                dataSource->valuestring, TSDB_DB_NAME_LEN);
      } else if (!dataSource) {
        tstrncpy(g_Dbs.db[i].superTbls[j].dataSource, "rand", TSDB_DB_NAME_LEN);
      } else {
        errorPrint("%s() LN%d, failed to read json, data_source not found\n",
            __func__, __LINE__);
        goto PARSE_OVER;
      }

      cJSON *stbIface = cJSON_GetObjectItem(stbInfo, "insert_mode"); // taosc , rest, stmt
      if (stbIface && stbIface->type == cJSON_String
              && stbIface->valuestring != NULL) {
        if (0 == strcasecmp(stbIface->valuestring, "taosc")) {
            g_Dbs.db[i].superTbls[j].iface= TAOSC_IFACE;
        } else if (0 == strcasecmp(stbIface->valuestring, "rest")) {
            g_Dbs.db[i].superTbls[j].iface= REST_IFACE;
#if STMT_IFACE_ENABLED == 1
        } else if (0 == strcasecmp(stbIface->valuestring, "stmt")) {
            g_Dbs.db[i].superTbls[j].iface= STMT_IFACE;
#endif
        } else {
            errorPrint("%s() LN%d, failed to read json, insert_mode %s not recognized\n",
                    __func__, __LINE__, stbIface->valuestring);
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
            printf("ERROR: failed to read json, childtable_limit\n");
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
            printf("ERROR: failed to read json, childtable_offset\n");
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
        printf("ERROR: failed to read json, start_timestamp not found\n");
        goto PARSE_OVER;
      }

      cJSON* timestampStep = cJSON_GetObjectItem(stbInfo, "timestamp_step");
      if (timestampStep && timestampStep->type == cJSON_Number) {
        g_Dbs.db[i].superTbls[j].timeStampStep = timestampStep->valueint;
      } else if (!timestampStep) {
        g_Dbs.db[i].superTbls[j].timeStampStep = DEFAULT_TIMESTAMP_STEP;
      } else {
        printf("ERROR: failed to read json, timestamp_step not found\n");
        goto PARSE_OVER;
      }

      cJSON *sampleFormat = cJSON_GetObjectItem(stbInfo, "sample_format");
      if (sampleFormat && sampleFormat->type
              == cJSON_String && sampleFormat->valuestring != NULL) {
        tstrncpy(g_Dbs.db[i].superTbls[j].sampleFormat,
                sampleFormat->valuestring, TSDB_DB_NAME_LEN);
      } else if (!sampleFormat) {
        tstrncpy(g_Dbs.db[i].superTbls[j].sampleFormat, "csv", TSDB_DB_NAME_LEN);
      } else {
        printf("ERROR: failed to read json, sample_format not found\n");
        goto PARSE_OVER;
      }

      cJSON *sampleFile = cJSON_GetObjectItem(stbInfo, "sample_file");
      if (sampleFile && sampleFile->type == cJSON_String
          && sampleFile->valuestring != NULL) {
        tstrncpy(g_Dbs.db[i].superTbls[j].sampleFile,
                sampleFile->valuestring, MAX_FILE_NAME_LEN);
      } else if (!sampleFile) {
        memset(g_Dbs.db[i].superTbls[j].sampleFile, 0, MAX_FILE_NAME_LEN);
      } else {
        printf("ERROR: failed to read json, sample_file not found\n");
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
        printf("ERROR: failed to read json, tags_file not found\n");
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
        errorPrint("%s() LN%d, failed to read json, stbMaxSqlLen input mistake\n",
            __func__, __LINE__);
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
        printf("ERROR: failed to read json, multiThreadWriteOneTbl not found\n");
        goto PARSE_OVER;
      }
*/
      cJSON* insertRows = cJSON_GetObjectItem(stbInfo, "insert_rows");
      if (insertRows && insertRows->type == cJSON_Number) {
        if (insertRows->valueint < 0) {
          errorPrint("%s() LN%d, failed to read json, insert_rows input mistake\n",
                __func__, __LINE__);
          goto PARSE_OVER;
        }
        g_Dbs.db[i].superTbls[j].insertRows = insertRows->valueint;
      } else if (!insertRows) {
        g_Dbs.db[i].superTbls[j].insertRows = 0x7FFFFFFFFFFFFFFF;
      } else {
        errorPrint("%s() LN%d, failed to read json, insert_rows input mistake\n",
                __func__, __LINE__);
        goto PARSE_OVER;
      }

      cJSON* stbInterlaceRows = cJSON_GetObjectItem(stbInfo, "interlace_rows");
      if (stbInterlaceRows && stbInterlaceRows->type == cJSON_Number) {
        if (stbInterlaceRows->valueint < 0) {
          errorPrint("%s() LN%d, failed to read json, interlace rows input mistake\n",
            __func__, __LINE__);
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
        g_Dbs.db[i].superTbls[j].interlaceRows = 0; // 0 means progressive mode, > 0 mean interlace mode. max value is less or equ num_of_records_per_req
      } else {
        errorPrint(
                "%s() LN%d, failed to read json, interlace rows input mistake\n",
                __func__, __LINE__);
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
        printf("ERROR: failed to read json, disorderRatio not found\n");
        goto PARSE_OVER;
      }

      cJSON* disorderRange = cJSON_GetObjectItem(stbInfo, "disorder_range");
      if (disorderRange && disorderRange->type == cJSON_Number) {
        g_Dbs.db[i].superTbls[j].disorderRange = disorderRange->valueint;
      } else if (!disorderRange) {
        g_Dbs.db[i].superTbls[j].disorderRange = 1000;
      } else {
        printf("ERROR: failed to read json, disorderRange not found\n");
        goto PARSE_OVER;
      }

      cJSON* insertInterval = cJSON_GetObjectItem(stbInfo, "insert_interval");
      if (insertInterval && insertInterval->type == cJSON_Number) {
        g_Dbs.db[i].superTbls[j].insertInterval = insertInterval->valueint;
        if (insertInterval->valueint < 0) {
          errorPrint("%s() LN%d, failed to read json, insert_interval input mistake\n",
                __func__, __LINE__);
          goto PARSE_OVER;
        }
      } else if (!insertInterval) {
        verbosePrint("%s() LN%d: stable insert interval be overrided by global %"PRIu64".\n",
                __func__, __LINE__, g_args.insert_interval);
        g_Dbs.db[i].superTbls[j].insertInterval = g_args.insert_interval;
      } else {
        errorPrint("%s() LN%d, failed to read json, insert_interval input mistake\n",
                __func__, __LINE__);
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
    printf("ERROR: failed to read json, host not found\n");
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
    tstrncpy(g_queryInfo.password, password->valuestring, MAX_PASSWORD_SIZE);
  } else if (!password) {
    tstrncpy(g_queryInfo.password, "taosdata", MAX_PASSWORD_SIZE);;
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
    printf("ERROR: failed to read json, confirm_parameter_prompt not found\n");
    goto PARSE_OVER;
  }

  cJSON* gQueryTimes = cJSON_GetObjectItem(root, "query_times");
  if (gQueryTimes && gQueryTimes->type == cJSON_Number) {
    if (gQueryTimes->valueint <= 0) {
      errorPrint("%s() LN%d, failed to read json, query_times input mistake\n",
        __func__, __LINE__);
      goto PARSE_OVER;
    }
    g_args.query_times = gQueryTimes->valueint;
  } else if (!gQueryTimes) {
    g_args.query_times = 1;
  } else {
    errorPrint("%s() LN%d, failed to read json, query_times input mistake\n",
        __func__, __LINE__);
    goto PARSE_OVER;
  }

  cJSON* dbs = cJSON_GetObjectItem(root, "databases");
  if (dbs && dbs->type == cJSON_String && dbs->valuestring != NULL) {
    tstrncpy(g_queryInfo.dbName, dbs->valuestring, TSDB_DB_NAME_LEN);
  } else if (!dbs) {
    printf("ERROR: failed to read json, databases not found\n");
    goto PARSE_OVER;
  }

  cJSON* queryMode = cJSON_GetObjectItem(root, "query_mode");
  if (queryMode && queryMode->type == cJSON_String && queryMode->valuestring != NULL) {
    tstrncpy(g_queryInfo.queryMode, queryMode->valuestring, MAX_TB_NAME_SIZE);
  } else if (!queryMode) {
    tstrncpy(g_queryInfo.queryMode, "taosc", MAX_TB_NAME_SIZE);
  } else {
    printf("ERROR: failed to read json, query_mode not found\n");
    goto PARSE_OVER;
  }

  // specified_table_query
  cJSON *specifiedQuery = cJSON_GetObjectItem(root, "specified_table_query");
  if (!specifiedQuery) {
    g_queryInfo.specifiedQueryInfo.concurrent = 1;
    g_queryInfo.specifiedQueryInfo.sqlCount = 0;
  } else if (specifiedQuery->type != cJSON_Object) {
    printf("ERROR: failed to read json, super_table_query not found\n");
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
                "%s() LN%d, failed to read json, query_times: %"PRId64", need be a valid (>0) number\n",
          __func__, __LINE__, specifiedQueryTimes->valueint);
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
                "%s() LN%d, query sqlCount %d or concurrent %d is not correct.\n",
              __func__, __LINE__,
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
        errorPrint("%s() LN%d, failed to read json, async mode input error\n",
            __func__, __LINE__);
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
        printf("ERROR: failed to read json, subscribe restart error\n");
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
        printf("ERROR: failed to read json, subscribe keepProgress error\n");
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
      errorPrint("%s() LN%d, failed to read json, super sqls not found\n",
          __func__, __LINE__);
      goto PARSE_OVER;
    } else {
      int superSqlSize = cJSON_GetArraySize(specifiedSqls);
      if (superSqlSize * g_queryInfo.specifiedQueryInfo.concurrent
              > MAX_QUERY_SQL_COUNT) {
        errorPrint("%s() LN%d, failed to read json, query sql(%d) * concurrent(%d) overflow, max is %d\n",
           __func__, __LINE__,
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
          printf("ERROR: failed to read json, sql not found\n");
          goto PARSE_OVER;
        }
        tstrncpy(g_queryInfo.specifiedQueryInfo.sql[j],
                sqlStr->valuestring, MAX_QUERY_SQL_LENGTH);

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
          printf("ERROR: failed to read json, super query result file not found\n");
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
    printf("ERROR: failed to read json, sub_table_query not found\n");
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
        errorPrint("%s() LN%d, failed to read json, query_times: %"PRId64", need be a valid (>0) number\n",
          __func__, __LINE__, superQueryTimes->valueint);
        goto PARSE_OVER;
      }
      g_queryInfo.superQueryInfo.queryTimes = superQueryTimes->valueint;
    } else if (!superQueryTimes) {
      g_queryInfo.superQueryInfo.queryTimes = g_args.query_times;
    } else {
      errorPrint("%s() LN%d, failed to read json, query_times input mistake\n",
          __func__, __LINE__);
      goto PARSE_OVER;
    }

    cJSON* threads = cJSON_GetObjectItem(superQuery, "threads");
    if (threads && threads->type == cJSON_Number) {
      if (threads->valueint <= 0) {
        errorPrint("%s() LN%d, failed to read json, threads input mistake\n",
          __func__, __LINE__);
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
      tstrncpy(g_queryInfo.superQueryInfo.sTblName, stblname->valuestring,
          TSDB_TABLE_NAME_LEN);
    } else {
      errorPrint("%s() LN%d, failed to read json, super table name input error\n",
          __func__, __LINE__);
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
        errorPrint("%s() LN%d, failed to read json, async mode input error\n",
            __func__, __LINE__);
        goto PARSE_OVER;
      }
    } else {
      g_queryInfo.superQueryInfo.asyncMode = SYNC_MODE;
    }

    cJSON* superInterval = cJSON_GetObjectItem(superQuery, "interval");
    if (superInterval && superInterval->type == cJSON_Number) {
      if (superInterval->valueint < 0) {
        errorPrint("%s() LN%d, failed to read json, interval input mistake\n",
            __func__, __LINE__);
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
        printf("ERROR: failed to read json, subscribe restart error\n");
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
        printf("ERROR: failed to read json, subscribe super table keepProgress error\n");
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
      errorPrint("%s() LN%d: failed to read json, super sqls not found\n",
          __func__, __LINE__);
      goto PARSE_OVER;
    } else {
      int superSqlSize = cJSON_GetArraySize(superSqls);
      if (superSqlSize > MAX_QUERY_SQL_COUNT) {
        errorPrint("%s() LN%d, failed to read json, query sql size overflow, max is %d\n",
           __func__, __LINE__, MAX_QUERY_SQL_COUNT);
        goto PARSE_OVER;
      }

      g_queryInfo.superQueryInfo.sqlCount = superSqlSize;
      for (int j = 0; j < superSqlSize; ++j) {
        cJSON* sql = cJSON_GetArrayItem(superSqls, j);
        if (sql == NULL) continue;

        cJSON *sqlStr = cJSON_GetObjectItem(sql, "sql");
        if (!sqlStr || sqlStr->type != cJSON_String
            || sqlStr->valuestring == NULL) {
          errorPrint("%s() LN%d, failed to read json, sql not found\n",
              __func__, __LINE__);
          goto PARSE_OVER;
        }
        tstrncpy(g_queryInfo.superQueryInfo.sql[j], sqlStr->valuestring,
            MAX_QUERY_SQL_LENGTH);

        cJSON *result = cJSON_GetObjectItem(sql, "result");
        if (result != NULL && result->type == cJSON_String
            && result->valuestring != NULL){
          tstrncpy(g_queryInfo.superQueryInfo.result[j],
              result->valuestring, MAX_FILE_NAME_LEN);
        } else if (NULL == result) {
          memset(g_queryInfo.superQueryInfo.result[j], 0, MAX_FILE_NAME_LEN);
        }  else {
          errorPrint("%s() LN%d, failed to read json, sub query result file not found\n",
              __func__, __LINE__);
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
    printf("failed to read %s, reason:%s\n", file, strerror(errno));
    return false;
  }

  bool  ret = false;
  int   maxLen = 6400000;
  char *content = calloc(1, maxLen + 1);
  int   len = fread(content, 1, maxLen, fp);
  if (len <= 0) {
    free(content);
    fclose(fp);
    printf("failed to read %s, content is null", file);
    return false;
  }

  content[len] = 0;
  cJSON* root = cJSON_Parse(content);
  if (root == NULL) {
    printf("ERROR: failed to cjson parse %s, invalid json format\n", file);
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
      printf("ERROR: failed to read json, filetype not support\n");
      goto PARSE_OVER;
    }
  } else if (!filetype) {
    g_args.test_mode = INSERT_TEST;
  } else {
    printf("ERROR: failed to read json, filetype not found\n");
    goto PARSE_OVER;
  }

  if (INSERT_TEST == g_args.test_mode) {
    ret = getMetaFromInsertJsonFile(root);
  } else if ((QUERY_TEST == g_args.test_mode)
          || (SUBSCRIBE_TEST == g_args.test_mode)) {
    ret = getMetaFromQueryJsonFile(root);
  } else {
    errorPrint("%s() LN%d, input json file type error! please input correct file type: insert or query or subscribe\n",
            __func__, __LINE__);
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
        free(g_Dbs.db[i].superTbls[j].colsOfCreateChildTable);
        g_Dbs.db[i].superTbls[j].colsOfCreateChildTable = NULL;
      }
      if (0 != g_Dbs.db[i].superTbls[j].sampleDataBuf) {
        free(g_Dbs.db[i].superTbls[j].sampleDataBuf);
        g_Dbs.db[i].superTbls[j].sampleDataBuf = NULL;
      }
      if (0 != g_Dbs.db[i].superTbls[j].tagDataBuf) {
        free(g_Dbs.db[i].superTbls[j].tagDataBuf);
        g_Dbs.db[i].superTbls[j].tagDataBuf = NULL;
      }
      if (0 != g_Dbs.db[i].superTbls[j].childTblName) {
        free(g_Dbs.db[i].superTbls[j].childTblName);
        g_Dbs.db[i].superTbls[j].childTblName = NULL;
      }
    }
  }
}

static int getRowDataFromSample(
        char* dataBuf, int64_t maxLen, int64_t timestamp,
      SSuperTable* superTblInfo, int64_t* sampleUsePos)
{
    if ((*sampleUsePos) == MAX_SAMPLES_ONCE_FROM_FILE) {
        /*    int ret = readSampleFromCsvFileToMem(superTblInfo);
              if (0 != ret) {
              tmfree(superTblInfo->sampleDataBuf);
              superTblInfo->sampleDataBuf = NULL;
              return -1;
              }
              */
        *sampleUsePos = 0;
    }

    int    dataLen = 0;

    dataLen += snprintf(dataBuf + dataLen, maxLen - dataLen,
            "(%" PRId64 ", ", timestamp);
    dataLen += snprintf(dataBuf + dataLen, maxLen - dataLen,
            "%s",
            superTblInfo->sampleDataBuf
            + superTblInfo->lenOfOneRow * (*sampleUsePos));
    dataLen += snprintf(dataBuf + dataLen, maxLen - dataLen, ")");

    (*sampleUsePos)++;

    return dataLen;
}

static int64_t generateStbRowData(
        SSuperTable* stbInfo,
        char* recBuf, int64_t timestamp)
{
    int64_t   dataLen = 0;
    char  *pstr = recBuf;
    int64_t maxLen = MAX_DATA_SIZE;

    dataLen += snprintf(pstr + dataLen, maxLen - dataLen,
            "(%" PRId64 ",", timestamp);

    for (int i = 0; i < stbInfo->columnCount; i++) {
        if ((0 == strncasecmp(stbInfo->columns[i].dataType,
                        "BINARY", strlen("BINARY")))
                || (0 == strncasecmp(stbInfo->columns[i].dataType,
                        "NCHAR", strlen("NCHAR")))) {
            if (stbInfo->columns[i].dataLen > TSDB_MAX_BINARY_LEN) {
                errorPrint( "binary or nchar length overflow, max size:%u\n",
                        (uint32_t)TSDB_MAX_BINARY_LEN);
                return -1;
            }

            char* buf = (char*)calloc(stbInfo->columns[i].dataLen+1, 1);
            if (NULL == buf) {
                errorPrint( "calloc failed! size:%d\n", stbInfo->columns[i].dataLen);
                return -1;
            }
            rand_string(buf, stbInfo->columns[i].dataLen);
            dataLen += snprintf(pstr + dataLen, maxLen - dataLen, "\'%s\',", buf);
            tmfree(buf);
        } else if (0 == strncasecmp(stbInfo->columns[i].dataType,
                    "INT", strlen("INT"))) {
            if ((g_args.demo_mode) && (i == 1)) {
                dataLen += snprintf(pstr + dataLen, maxLen - dataLen,
                        "%d,", demo_voltage_int());
            } else {
                dataLen += snprintf(pstr + dataLen, maxLen - dataLen,
                        "%d,", rand_int());
            }
        } else if (0 == strncasecmp(stbInfo->columns[i].dataType,
                    "BIGINT", strlen("BIGINT"))) {
            dataLen += snprintf(pstr + dataLen, maxLen - dataLen,
                    "%"PRId64",", rand_bigint());
        }  else if (0 == strncasecmp(stbInfo->columns[i].dataType,
                    "FLOAT", strlen("FLOAT"))) {
            if (g_args.demo_mode) {
                if (i == 0) {
                    dataLen += snprintf(pstr + dataLen, maxLen - dataLen,
                            "%f,", demo_current_float());
                } else {
                    dataLen += snprintf(pstr + dataLen, maxLen - dataLen,
                            "%f,", demo_phase_float());
                }
            } else {
                dataLen += snprintf(pstr + dataLen, maxLen - dataLen,
                        "%f,", rand_float());
            }
        }  else if (0 == strncasecmp(stbInfo->columns[i].dataType,
                    "DOUBLE", strlen("DOUBLE"))) {
            dataLen += snprintf(pstr + dataLen, maxLen - dataLen,
                    "%f,", rand_double());
        }  else if (0 == strncasecmp(stbInfo->columns[i].dataType,
                    "SMALLINT", strlen("SMALLINT"))) {
            dataLen += snprintf(pstr + dataLen, maxLen - dataLen,
                    "%d,", rand_smallint());
        }  else if (0 == strncasecmp(stbInfo->columns[i].dataType,
                    "TINYINT", strlen("TINYINT"))) {
            dataLen += snprintf(pstr + dataLen, maxLen - dataLen,
                    "%d,", rand_tinyint());
        }  else if (0 == strncasecmp(stbInfo->columns[i].dataType,
                    "BOOL", strlen("BOOL"))) {
            dataLen += snprintf(pstr + dataLen, maxLen - dataLen,
                    "%d,", rand_bool());
        }  else if (0 == strncasecmp(stbInfo->columns[i].dataType,
                    "TIMESTAMP", strlen("TIMESTAMP"))) {
            dataLen += snprintf(pstr + dataLen, maxLen - dataLen,
                    "%"PRId64",", rand_bigint());
        }  else {
            errorPrint( "Not support data type: %s\n", stbInfo->columns[i].dataType);
            return -1;
        }
    }

    dataLen -= 1;
    dataLen += snprintf(pstr + dataLen, maxLen - dataLen, ")");

    verbosePrint("%s() LN%d, dataLen:%"PRId64"\n", __func__, __LINE__, dataLen);
    verbosePrint("%s() LN%d, recBuf:\n\t%s\n", __func__, __LINE__, recBuf);

    return strlen(recBuf);
}

static int64_t generateData(char *recBuf, char **data_type,
        int64_t timestamp, int lenOfBinary) {
  memset(recBuf, 0, MAX_DATA_SIZE);
  char *pstr = recBuf;
  pstr += sprintf(pstr, "(%" PRId64, timestamp);

  int columnCount = g_args.num_of_CPR;

  for (int i = 0; i < columnCount; i++) {
    if (strcasecmp(data_type[i % columnCount], "TINYINT") == 0) {
      pstr += sprintf(pstr, ",%d", rand_tinyint() );
    } else if (strcasecmp(data_type[i % columnCount], "SMALLINT") == 0) {
      pstr += sprintf(pstr, ",%d", rand_smallint());
    } else if (strcasecmp(data_type[i % columnCount], "INT") == 0) {
      pstr += sprintf(pstr, ",%d", rand_int());
    } else if (strcasecmp(data_type[i % columnCount], "BIGINT") == 0) {
      pstr += sprintf(pstr, ",%" PRId64, rand_bigint());
    } else if (strcasecmp(data_type[i % columnCount], "TIMESTAMP") == 0) {
      pstr += sprintf(pstr, ",%" PRId64, rand_bigint());
    } else if (strcasecmp(data_type[i % columnCount], "FLOAT") == 0) {
      pstr += sprintf(pstr, ",%10.4f", rand_float());
    } else if (strcasecmp(data_type[i % columnCount], "DOUBLE") == 0) {
      double t = rand_double();
      pstr += sprintf(pstr, ",%20.8f", t);
    } else if (strcasecmp(data_type[i % columnCount], "BOOL") == 0) {
      bool b = rand_bool() & 1;
      pstr += sprintf(pstr, ",%s", b ? "true" : "false");
    } else if (strcasecmp(data_type[i % columnCount], "BINARY") == 0) {
      char *s = malloc(lenOfBinary + 1);
      if (s == NULL) {
          errorPrint("%s() LN%d, memory allocation %d bytes failed\n",
                  __func__, __LINE__, lenOfBinary + 1);
      }
      rand_string(s, lenOfBinary);
      pstr += sprintf(pstr, ",\"%s\"", s);
      free(s);
    } else if (strcasecmp(data_type[i % columnCount], "NCHAR") == 0) {
      char *s = malloc(lenOfBinary + 1);
      if (s == NULL) {
          errorPrint("%s() LN%d, memory allocation %d bytes failed\n",
                  __func__, __LINE__, lenOfBinary + 1);
      }
      rand_string(s, lenOfBinary);
      pstr += sprintf(pstr, ",\"%s\"", s);
      free(s);
    }

    if (strlen(recBuf) > MAX_DATA_SIZE) {
      perror("column length too long, abort");
      exit(-1);
    }
  }

  pstr += sprintf(pstr, ")");

  verbosePrint("%s() LN%d, recBuf:\n\t%s\n", __func__, __LINE__, recBuf);

  return (int32_t)strlen(recBuf);
}

static int prepareSampleDataForSTable(SSuperTable *superTblInfo) {
  char* sampleDataBuf = NULL;

  sampleDataBuf = calloc(
            superTblInfo->lenOfOneRow * MAX_SAMPLES_ONCE_FROM_FILE, 1);
  if (sampleDataBuf == NULL) {
      errorPrint("%s() LN%d, Failed to calloc %"PRIu64" Bytes, reason:%s\n",
              __func__, __LINE__,
              superTblInfo->lenOfOneRow * MAX_SAMPLES_ONCE_FROM_FILE,
              strerror(errno));
      return -1;
  }

  superTblInfo->sampleDataBuf = sampleDataBuf;
  int ret = readSampleFromCsvFileToMem(superTblInfo);

  if (0 != ret) {
      errorPrint("%s() LN%d, read sample from csv file failed.\n",
          __func__, __LINE__);
      tmfree(sampleDataBuf);
      superTblInfo->sampleDataBuf = NULL;
      return -1;
  }

  return 0;
}

static int32_t execInsert(threadInfo *pThreadInfo, uint32_t k)
{
    int32_t affectedRows;
    SSuperTable* superTblInfo = pThreadInfo->superTblInfo;

    verbosePrint("[%d] %s() LN%d %s\n", pThreadInfo->threadID,
            __func__, __LINE__, pThreadInfo->buffer);

    uint16_t iface;
    if (superTblInfo)
        iface = superTblInfo->iface;
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
            affectedRows = queryDbExec(
                    pThreadInfo->taos,
                    pThreadInfo->buffer, INSERT_TYPE, false);
            break;

        case REST_IFACE:
            if (0 != postProceSql(g_Dbs.host, &g_Dbs.serv_addr, g_Dbs.port,
                        pThreadInfo->buffer, pThreadInfo)) {
                affectedRows = -1;
                printf("========restful return fail, threadID[%d]\n",
                        pThreadInfo->threadID);
            } else {
                affectedRows = k;
            }
            break;

#if STMT_IFACE_ENABLED == 1
        case STMT_IFACE:
            debugPrint("%s() LN%d, stmt=%p", __func__, __LINE__, pThreadInfo->stmt);
            if (0 != taos_stmt_execute(pThreadInfo->stmt)) {
                errorPrint("%s() LN%d, failied to execute insert statement\n",
                        __func__, __LINE__);
                exit(-1);
            }
            affectedRows = k;
            break;
#endif

        default:
            errorPrint("%s() LN%d: unknown insert mode: %d\n",
                    __func__, __LINE__, superTblInfo->iface);
            affectedRows = 0;
    }

    return affectedRows;
}

static void getTableName(char *pTblName,
        threadInfo* pThreadInfo, uint64_t tableSeq)
{
    SSuperTable* superTblInfo = pThreadInfo->superTblInfo;
    if (superTblInfo) {
        if (AUTO_CREATE_SUBTBL != superTblInfo->autoCreateTable) {
            if (superTblInfo->childTblLimit > 0) {
                snprintf(pTblName, TSDB_TABLE_NAME_LEN, "%s",
                        superTblInfo->childTblName +
                        (tableSeq - superTblInfo->childTblOffset) * TSDB_TABLE_NAME_LEN);
            } else {
                verbosePrint("[%d] %s() LN%d: from=%"PRIu64" count=%"PRId64" seq=%"PRIu64"\n",
                        pThreadInfo->threadID, __func__, __LINE__,
                        pThreadInfo->start_table_from,
                        pThreadInfo->ntables, tableSeq);
                snprintf(pTblName, TSDB_TABLE_NAME_LEN, "%s",
                        superTblInfo->childTblName + tableSeq * TSDB_TABLE_NAME_LEN);
            }
        } else {
            snprintf(pTblName, TSDB_TABLE_NAME_LEN, "%s%"PRIu64"",
                    superTblInfo->childTblPrefix, tableSeq);
        }
    } else {
        snprintf(pTblName, TSDB_TABLE_NAME_LEN, "%s%"PRIu64"",
                g_args.tb_prefix, tableSeq);
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
    char data[MAX_DATA_SIZE];
    memset(data, 0, MAX_DATA_SIZE);

    int64_t retLen = 0;

    char **data_type = g_args.datatype;
    int lenOfBinary = g_args.len_of_binary;

    if (g_args.disorderRatio) {
        retLen = generateData(data, data_type,
                startTime + getTSRandTail(
                    (int64_t) DEFAULT_TIMESTAMP_STEP, k,
                    g_args.disorderRatio,
                    g_args.disorderRange),
                lenOfBinary);
    } else {
        retLen = generateData(data, data_type,
                startTime + (int64_t) (DEFAULT_TIMESTAMP_STEP* k),
                lenOfBinary);
    }

    if (len > remainderBufLen)
        break;

    pstr += sprintf(pstr, "%s", data);
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
        SSuperTable* superTblInfo,
        uint32_t batch, char* buffer,
        int64_t remainderBufLen, int64_t insertRows,
        uint64_t recordFrom, int64_t startTime,
        int64_t *pSamplePos, int64_t *dataLen) {
  uint64_t len = 0;

  char *pstr = buffer;

  bool tsRand;
  if (0 == strncasecmp(superTblInfo->dataSource, "rand", strlen("rand"))) {
     tsRand = true;
  } else {
     tsRand = false;
  }
  verbosePrint("%s() LN%d batch=%u buflen=%"PRId64"\n",
          __func__, __LINE__, batch, remainderBufLen);

  int32_t k;
  for (k = 0; k < batch;) {
    char data[MAX_DATA_SIZE];
    memset(data, 0, MAX_DATA_SIZE);

    int64_t lenOfRow = 0;

    if (tsRand) {
        lenOfRow = generateStbRowData(superTblInfo, data,
                startTime + getTSRandTail(
                    superTblInfo->timeStampStep, k,
                    superTblInfo->disorderRatio,
                    superTblInfo->disorderRange)
                );
    } else {
        lenOfRow = getRowDataFromSample(
                  data,
                  (remainderBufLen < MAX_DATA_SIZE)?remainderBufLen:MAX_DATA_SIZE,
                  startTime + superTblInfo->timeStampStep * k,
                  superTblInfo,
                  pSamplePos);
    }

    if ((lenOfRow + 1) > remainderBufLen) {
        break;
    }

    pstr += snprintf(pstr , lenOfRow + 1, "%s", data);
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
        SSuperTable* superTblInfo,
        char *tableName, int32_t tableSeq,
        char *dbName,
        char *buffer, int remainderBufLen)
{
  int len;

  char headBuf[HEAD_BUFF_LEN];

  if (AUTO_CREATE_SUBTBL == superTblInfo->autoCreateTable) {
      char* tagsValBuf = NULL;
      if (0 == superTblInfo->tagSource) {
            tagsValBuf = generateTagVaulesForStb(superTblInfo, tableSeq);
      } else {
            tagsValBuf = getTagValueFromTagSample(
                    superTblInfo,
                    tableSeq % superTblInfo->tagSampleCount);
      }
      if (NULL == tagsValBuf) {
        errorPrint("%s() LN%d, tag buf failed to allocate  memory\n",
            __func__, __LINE__);
        return -1;
      }

      len = snprintf(
          headBuf,
                  HEAD_BUFF_LEN,
                  "%s.%s using %s.%s tags %s values",
                  dbName,
                  tableName,
                  dbName,
                  superTblInfo->sTblName,
                  tagsValBuf);
      tmfree(tagsValBuf);
    } else if (TBL_ALREADY_EXISTS == superTblInfo->childTblExists) {
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
        SSuperTable *superTblInfo,
        char *tableName, uint32_t batchPerTbl,
        uint64_t i,
        uint32_t batchPerTblTimes,
        uint64_t tableSeq,
        threadInfo *pThreadInfo, char *buffer,
        int64_t insertRows,
        int64_t startTime,
        uint64_t *pRemainderBufLen)
{
  assert(buffer);
  char *pstr = buffer;

  int headLen = generateStbSQLHead(
          superTblInfo,
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

  if (0 == strncasecmp(superTblInfo->startTimestamp, "now", 3)) {
      startTime = taosGetTimestamp(pThreadInfo->time_precision);
  }

  int32_t k = generateStbDataTail(
            superTblInfo,
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

#if STMT_IFACE_ENABLED == 1
static int32_t prepareStmtBindArrayByType(TAOS_BIND *bind,
        char *dataType, int32_t dataLen, char **ptr, char *value)
{
    if (0 == strncasecmp(dataType,
                "BINARY", strlen("BINARY"))) {
        if (dataLen > TSDB_MAX_BINARY_LEN) {
            errorPrint( "binary length overflow, max size:%u\n",
                    (uint32_t)TSDB_MAX_BINARY_LEN);
            return -1;
        }
        char *bind_binary = (char *)*ptr;

        bind->buffer_type = TSDB_DATA_TYPE_BINARY;
        if (value) {
            strncpy(bind_binary, value, strlen(value));
            bind->buffer_length = strlen(bind_binary);
        } else {
            rand_string(bind_binary, dataLen);
            bind->buffer_length = dataLen;
        }

        bind->length = &bind->buffer_length;
        bind->buffer = bind_binary;
        bind->is_null = NULL;

        *ptr += bind->buffer_length;
    } else if (0 == strncasecmp(dataType,
                "NCHAR", strlen("NCHAR"))) {
        if (dataLen > TSDB_MAX_BINARY_LEN) {
            errorPrint( "nchar length overflow, max size:%u\n",
                    (uint32_t)TSDB_MAX_BINARY_LEN);
            return -1;
        }
        char *bind_nchar = (char *)*ptr;

        bind->buffer_type = TSDB_DATA_TYPE_NCHAR;
        if (value) {
            strncpy(bind_nchar, value, strlen(value));
        } else {
            rand_string(bind_nchar, dataLen);
        }

        bind->buffer_length = strlen(bind_nchar);
        bind->buffer = bind_nchar;
        bind->length = &bind->buffer_length;
        bind->is_null = NULL;

        *ptr += bind->buffer_length;
    } else if (0 == strncasecmp(dataType,
                "INT", strlen("INT"))) {
        int32_t *bind_int = (int32_t *)*ptr;

        if (value) {
            *bind_int = atoi(value);
        } else {
            *bind_int = rand_int();
        }
        bind->buffer_type = TSDB_DATA_TYPE_INT;
        bind->buffer_length = sizeof(int32_t);
        bind->buffer = bind_int;
        bind->length = &bind->buffer_length;
        bind->is_null = NULL;

        *ptr += bind->buffer_length;
    } else if (0 == strncasecmp(dataType,
                "BIGINT", strlen("BIGINT"))) {
        int64_t *bind_bigint = (int64_t *)*ptr;

        if (value) {
            *bind_bigint = atoll(value);
        } else {
            *bind_bigint = rand_bigint();
        }
        bind->buffer_type = TSDB_DATA_TYPE_BIGINT;
        bind->buffer_length = sizeof(int64_t);
        bind->buffer = bind_bigint;
        bind->length = &bind->buffer_length;
        bind->is_null = NULL;

        *ptr += bind->buffer_length;
    }  else if (0 == strncasecmp(dataType,
                "FLOAT", strlen("FLOAT"))) {
        float   *bind_float = (float *) *ptr;

        if (value) {
            *bind_float = (float)atof(value);
        } else {
            *bind_float = rand_float();
        }
        bind->buffer_type = TSDB_DATA_TYPE_FLOAT;
        bind->buffer_length = sizeof(float);
        bind->buffer = bind_float;
        bind->length = &bind->buffer_length;
        bind->is_null = NULL;

        *ptr += bind->buffer_length;
    }  else if (0 == strncasecmp(dataType,
                "DOUBLE", strlen("DOUBLE"))) {
        double  *bind_double = (double *)*ptr;

        if (value) {
            *bind_double = atof(value);
        } else {
            *bind_double = rand_double();
        }
        bind->buffer_type = TSDB_DATA_TYPE_DOUBLE;
        bind->buffer_length = sizeof(double);
        bind->buffer = bind_double;
        bind->length = &bind->buffer_length;
        bind->is_null = NULL;

        *ptr += bind->buffer_length;
    }  else if (0 == strncasecmp(dataType,
                "SMALLINT", strlen("SMALLINT"))) {
        int16_t *bind_smallint = (int16_t *)*ptr;

        if (value) {
            *bind_smallint = (int16_t)atoi(value);
        } else {
            *bind_smallint = rand_smallint();
        }
        bind->buffer_type = TSDB_DATA_TYPE_SMALLINT;
        bind->buffer_length = sizeof(int16_t);
        bind->buffer = bind_smallint;
        bind->length = &bind->buffer_length;
        bind->is_null = NULL;

        *ptr += bind->buffer_length;
    }  else if (0 == strncasecmp(dataType,
                "TINYINT", strlen("TINYINT"))) {
        int8_t  *bind_tinyint = (int8_t *)*ptr;

        if (value) {
            *bind_tinyint = (int8_t)atoi(value);
        } else {
            *bind_tinyint = rand_tinyint();
        }
        bind->buffer_type = TSDB_DATA_TYPE_TINYINT;
        bind->buffer_length = sizeof(int8_t);
        bind->buffer = bind_tinyint;
        bind->length = &bind->buffer_length;
        bind->is_null = NULL;
        *ptr += bind->buffer_length;
    }  else if (0 == strncasecmp(dataType,
                "BOOL", strlen("BOOL"))) {
        int8_t  *bind_bool = (int8_t *)*ptr;

        *bind_bool = rand_bool();
        bind->buffer_type = TSDB_DATA_TYPE_BOOL;
        bind->buffer_length = sizeof(int8_t);
        bind->buffer = bind_bool;
        bind->length = &bind->buffer_length;
        bind->is_null = NULL;

        *ptr += bind->buffer_length;
    }  else if (0 == strncasecmp(dataType,
                "TIMESTAMP", strlen("TIMESTAMP"))) {
        int64_t *bind_ts2 = (int64_t *) *ptr;

        if (value) {
            *bind_ts2 = atoll(value);
        } else {
            *bind_ts2 = rand_bigint();
        }
        bind->buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
        bind->buffer_length = sizeof(int64_t);
        bind->buffer = bind_ts2;
        bind->length = &bind->buffer_length;
        bind->is_null = NULL;

        *ptr += bind->buffer_length;
    }  else {
        errorPrint( "No support data type: %s\n",
                dataType);
        return -1;
    }

    return 0;
}

static int32_t prepareStmtWithoutStb(
        TAOS_STMT *stmt,
        char *tableName,
        uint32_t batch,
        int64_t insertRows,
        int64_t recordFrom,
        int64_t startTime)
{
    int ret = taos_stmt_set_tbname(stmt, tableName);
    if (ret != 0) {
        errorPrint("failed to execute taos_stmt_set_tbname(%s). return 0x%x. reason: %s\n",
                tableName, ret, taos_errstr(NULL));
        return ret;
    }

    char **data_type = g_args.datatype;

    char *bindArray = malloc(sizeof(TAOS_BIND) * (g_args.num_of_CPR + 1));
    if (bindArray == NULL) {
        errorPrint("Failed to allocate %d bind params\n",
                (g_args.num_of_CPR + 1));
        return -1;
    }

    int32_t k = 0;
    for (k = 0; k < batch;) {
        /* columnCount + 1 (ts) */
        char data[MAX_DATA_SIZE];
        memset(data, 0, MAX_DATA_SIZE);

        char *ptr = data;
        TAOS_BIND *bind = (TAOS_BIND *)(bindArray + 0);

        int64_t *bind_ts;

        bind_ts = (int64_t *)ptr;
        bind->buffer_type = TSDB_DATA_TYPE_TIMESTAMP;

        if (g_args.disorderRatio) {
            *bind_ts = startTime + getTSRandTail(
                    (int64_t)DEFAULT_TIMESTAMP_STEP, k,
                    g_args.disorderRatio,
                    g_args.disorderRange);
        } else {
            *bind_ts = startTime + (int64_t)(DEFAULT_TIMESTAMP_STEP * k);
        }
        bind->buffer_length = sizeof(int64_t);
        bind->buffer = bind_ts;
        bind->length = &bind->buffer_length;
        bind->is_null = NULL;

        ptr += bind->buffer_length;

        for (int i = 0; i < g_args.num_of_CPR; i ++) {
            bind = (TAOS_BIND *)((char *)bindArray
                    + (sizeof(TAOS_BIND) * (i + 1)));
            if ( -1 == prepareStmtBindArrayByType(
                        bind,
                        data_type[i],
                        g_args.len_of_binary,
                        &ptr, NULL)) {
                return -1;
            }
        }
        taos_stmt_bind_param(stmt, (TAOS_BIND *)bindArray);
        // if msg > 3MB, break
        taos_stmt_add_batch(stmt);

        k++;
        recordFrom ++;
        if (recordFrom >= insertRows) {
            break;
        }
    }

    free(bindArray);
    return k;
}

static int32_t prepareStbStmt(
        SSuperTable *stbInfo,
        TAOS_STMT *stmt,
        char *tableName, uint32_t batch,
        uint64_t insertRows,
        uint64_t recordFrom,
        int64_t startTime,
        int64_t *pSamplePos)
{
    int ret = taos_stmt_set_tbname(stmt, tableName);
    if (ret != 0) {
        errorPrint("failed to execute taos_stmt_set_tbname(%s). return 0x%x. reason: %s\n",
                tableName, ret, taos_errstr(NULL));
        return ret;
    }

    char *bindArray = malloc(sizeof(TAOS_BIND) * (stbInfo->columnCount + 1));
    if (bindArray == NULL) {
        errorPrint("%s() LN%d, Failed to allocate %d bind params\n",
                __func__, __LINE__, (stbInfo->columnCount + 1));
        return -1;
    }

    bool sourceRand;
    if (0 == strncasecmp(stbInfo->dataSource, "rand", strlen("rand"))) {
        sourceRand = true;
    } else {
        sourceRand = false;     // from sample data file
    }

    char *bindBuffer = malloc(g_args.len_of_binary);
    if (bindBuffer == NULL) {
        errorPrint("%s() LN%d, Failed to allocate %d bind buffer\n",
                __func__, __LINE__, g_args.len_of_binary);
        free(bindArray);
        return -1;
    }

    uint32_t k;
    for (k = 0; k < batch;) {
        /* columnCount + 1 (ts) */
        char data[MAX_DATA_SIZE];
        memset(data, 0, MAX_DATA_SIZE);

        char *ptr = data;
        TAOS_BIND *bind = (TAOS_BIND *)(bindArray + 0);

        int64_t *bind_ts;

        bind_ts = (int64_t *)ptr;
        bind->buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
        if (stbInfo->disorderRatio) {
            *bind_ts = startTime + getTSRandTail(
                    stbInfo->timeStampStep, k,
                    stbInfo->disorderRatio,
                    stbInfo->disorderRange);
        } else {
            *bind_ts = startTime + stbInfo->timeStampStep * k;
        }
        bind->buffer_length = sizeof(int64_t);
        bind->buffer = bind_ts;
        bind->length = &bind->buffer_length;
        bind->is_null = NULL;

        ptr += bind->buffer_length;

        int cursor = 0;
        for (int i = 0; i < stbInfo->columnCount; i ++) {
            bind = (TAOS_BIND *)((char *)bindArray + (sizeof(TAOS_BIND) * (i + 1)));

            if (sourceRand) {
                if ( -1 == prepareStmtBindArrayByType(
                            bind,
                            stbInfo->columns[i].dataType,
                            stbInfo->columns[i].dataLen,
                            &ptr,
                            NULL)) {
                    free(bindArray);
                    free(bindBuffer);
                    return -1;
                }
            } else {
                char *restStr = stbInfo->sampleDataBuf + cursor;
                int lengthOfRest = strlen(restStr);

                int index = 0;
                for (index = 0; index < lengthOfRest; index ++) {
                    if (restStr[index] == ',') {
                        break;
                    }
                }

                memset(bindBuffer, 0, g_args.len_of_binary);
                strncpy(bindBuffer, restStr, index);
                cursor += index + 1; // skip ',' too

                if ( -1 == prepareStmtBindArrayByType(
                            bind,
                            stbInfo->columns[i].dataType,
                            stbInfo->columns[i].dataLen,
                            &ptr,
                            bindBuffer)) {
                    free(bindArray);
                    free(bindBuffer);
                    return -1;
                }
            }
        }
        taos_stmt_bind_param(stmt, (TAOS_BIND *)bindArray);
        // if msg > 3MB, break
        taos_stmt_add_batch(stmt);

        k++;
        recordFrom ++;

        if (!sourceRand) {
            (*pSamplePos) ++;
        }

        if (recordFrom >= insertRows) {
            break;
        }
    }

    free(bindBuffer);
    free(bindArray);
    return k;
}

static int32_t prepareStbStmtInterlace(
        SSuperTable *stbInfo,
        TAOS_STMT *stmt,
        char *tableName, uint32_t batch,
        uint64_t insertRows,
        uint64_t recordFrom,
        int64_t startTime,
        int64_t *pSamplePos)
{
    return prepareStbStmt(
            stbInfo,
            stmt,
            tableName,
            g_args.num_of_RPR,
            insertRows, 0, startTime,
            pSamplePos);
}

static int32_t prepareStbStmtProgressive(
        SSuperTable *stbInfo,
        TAOS_STMT *stmt,
        char *tableName, uint32_t batch,
        uint64_t insertRows,
        uint64_t recordFrom,
        int64_t startTime,
        int64_t *pSamplePos)
{
    return prepareStbStmt(
            stbInfo,
            stmt,
            tableName,
            g_args.num_of_RPR,
            insertRows, recordFrom, startTime,
            pSamplePos);
}

#endif

static int32_t generateStbProgressiveData(
        SSuperTable *superTblInfo,
        char *tableName,
        int64_t tableSeq,
        char *dbName, char *buffer,
        int64_t insertRows,
        uint64_t recordFrom, int64_t startTime, int64_t *pSamplePos,
        int64_t *pRemainderBufLen)
{
  assert(buffer != NULL);
  char *pstr = buffer;

  memset(buffer, 0, *pRemainderBufLen);

  int64_t headLen = generateStbSQLHead(
              superTblInfo,
          tableName, tableSeq, dbName,
          buffer, *pRemainderBufLen);

  if (headLen <= 0) {
    return 0;
  }
  pstr += headLen;
  *pRemainderBufLen -= headLen;

  int64_t dataLen;

  return generateStbDataTail(superTblInfo,
          g_args.num_of_RPR, pstr, *pRemainderBufLen,
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
          g_args.num_of_RPR, pstr, *pRemainderBufLen, insertRows, recordFrom,
          startTime,
          /*pSamplePos, */&dataLen);
}

static void printStatPerThread(threadInfo *pThreadInfo)
{
    fprintf(stderr, "====thread[%d] completed total inserted rows: %"PRIu64 ", total affected rows: %"PRIu64". %.2f records/second====\n",
          pThreadInfo->threadID,
          pThreadInfo->totalInsertRows,
          pThreadInfo->totalAffectedRows,
          (pThreadInfo->totalDelay)?(double)((pThreadInfo->totalAffectedRows / (pThreadInfo->totalDelay)/1000.0)): FLT_MAX);
}

// sync write interlace data
static void* syncWriteInterlace(threadInfo *pThreadInfo) {
  debugPrint("[%d] %s() LN%d: ### interlace write\n",
         pThreadInfo->threadID, __func__, __LINE__);

  int64_t insertRows;
  uint32_t interlaceRows;
  uint64_t maxSqlLen;
  int64_t nTimeStampStep;
  uint64_t insert_interval;

  SSuperTable* superTblInfo = pThreadInfo->superTblInfo;

  if (superTblInfo) {
    insertRows = superTblInfo->insertRows;

    if ((superTblInfo->interlaceRows == 0)
        && (g_args.interlace_rows > 0)) {
      interlaceRows = g_args.interlace_rows;
    } else {
      interlaceRows = superTblInfo->interlaceRows;
    }
    maxSqlLen = superTblInfo->maxSqlLen;
    nTimeStampStep = superTblInfo->timeStampStep;
    insert_interval = superTblInfo->insertInterval;
  } else {
    insertRows = g_args.num_of_DPT;
    interlaceRows = g_args.interlace_rows;
    maxSqlLen = g_args.max_sql_len;
    nTimeStampStep = DEFAULT_TIMESTAMP_STEP;
    insert_interval = g_args.insert_interval;
  }

  debugPrint("[%d] %s() LN%d: start_table_from=%"PRIu64" ntables=%"PRId64" insertRows=%"PRIu64"\n",
          pThreadInfo->threadID, __func__, __LINE__,
          pThreadInfo->start_table_from,
          pThreadInfo->ntables, insertRows);

  if (interlaceRows > insertRows)
    interlaceRows = insertRows;

  if (interlaceRows > g_args.num_of_RPR)
    interlaceRows = g_args.num_of_RPR;

  uint32_t batchPerTbl = interlaceRows;
  uint32_t batchPerTblTimes;

  if ((interlaceRows > 0) && (pThreadInfo->ntables > 1)) {
    batchPerTblTimes =
        g_args.num_of_RPR / interlaceRows;
  } else {
    batchPerTblTimes = 1;
  }

  pThreadInfo->buffer = calloc(maxSqlLen, 1);
  if (NULL == pThreadInfo->buffer) {
    errorPrint( "%s() LN%d, Failed to alloc %"PRIu64" Bytes, reason:%s\n",
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

    for (uint64_t i = 0; i < batchPerTblTimes; i ++) {
        char tableName[TSDB_TABLE_NAME_LEN];

      getTableName(tableName, pThreadInfo, tableSeq);
      if (0 == strlen(tableName)) {
        errorPrint("[%d] %s() LN%d, getTableName return null\n",
            pThreadInfo->threadID, __func__, __LINE__);
        free(pThreadInfo->buffer);
        return NULL;
      }

      uint64_t oldRemainderLen = remainderBufLen;

      int32_t generated;
      if (superTblInfo) {
          if (superTblInfo->iface == STMT_IFACE) {
#if STMT_IFACE_ENABLED == 1
              generated = prepareStbStmtInterlace(
                      superTblInfo,
                      pThreadInfo->stmt,
                      tableName,
                      batchPerTbl,
                      insertRows, i,
                      startTime,
                      &(pThreadInfo->samplePos));
#else
              generated = -1;
#endif
          } else {
              generated = generateStbInterlaceData(
                      superTblInfo,
                      tableName, batchPerTbl, i,
                      batchPerTblTimes,
                      tableSeq,
                      pThreadInfo, pstr,
                      insertRows,
                      startTime,
                      &remainderBufLen);
          }
      } else {
          if (g_args.iface == STMT_IFACE) {
              debugPrint("[%d] %s() LN%d, tableName:%s, batch:%d startTime:%"PRId64"\n",
                      pThreadInfo->threadID,
                      __func__, __LINE__,
                      tableName, batchPerTbl, startTime);
#if STMT_IFACE_ENABLED == 1
              generated = prepareStmtWithoutStb(
                      pThreadInfo->stmt, tableName,
                      batchPerTbl,
                      insertRows, i,
                      startTime);
#else
              generated = -1;
#endif
          } else {
              generated = generateInterlaceDataWithoutStb(
                      tableName, batchPerTbl,
                      tableSeq,
                      pThreadInfo->db_name, pstr,
                      insertRows,
                      startTime,
                      &remainderBufLen);
          }
      }

      debugPrint("[%d] %s() LN%d, generated records is %d\n",
                  pThreadInfo->threadID, __func__, __LINE__, generated);
      if (generated < 0) {
        errorPrint("[%d] %s() LN%d, generated records is %d\n",
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
              + generatedRecPerTbl * nTimeStampStep;

            flagSleep = true;
            if (generatedRecPerTbl >= insertRows)
              break;

            int64_t remainRows = insertRows - generatedRecPerTbl;
            if ((remainRows > 0) && (batchPerTbl > remainRows))
              batchPerTbl = remainRows;

            if (pThreadInfo->ntables * batchPerTbl < g_args.num_of_RPR)
                break;
      }

      verbosePrint("[%d] %s() LN%d generatedRecPerTbl=%"PRId64" insertRows=%"PRId64"\n",
                pThreadInfo->threadID, __func__, __LINE__,
                generatedRecPerTbl, insertRows);

      if ((g_args.num_of_RPR - recOfBatch) < batchPerTbl)
        break;
    }

    verbosePrint("[%d] %s() LN%d recOfBatch=%d totalInsertRows=%"PRIu64"\n",
              pThreadInfo->threadID, __func__, __LINE__, recOfBatch,
              pThreadInfo->totalInsertRows);
    verbosePrint("[%d] %s() LN%d, buffer=%s\n",
           pThreadInfo->threadID, __func__, __LINE__, pThreadInfo->buffer);

    startTs = taosGetTimestampMs();

    if (recOfBatch == 0) {
        errorPrint("[%d] %s() LN%d Failed to insert records of batch %d\n",
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

    endTs = taosGetTimestampMs();
    uint64_t delay = endTs - startTs;
    performancePrint("%s() LN%d, insert execution time is %"PRIu64"ms\n",
            __func__, __LINE__, delay);
    verbosePrint("[%d] %s() LN%d affectedRows=%"PRId64"\n",
            pThreadInfo->threadID,
            __func__, __LINE__, affectedRows);

    if (delay > pThreadInfo->maxDelay) pThreadInfo->maxDelay = delay;
    if (delay < pThreadInfo->minDelay) pThreadInfo->minDelay = delay;
    pThreadInfo->cntDelay++;
    pThreadInfo->totalDelay += delay;

    if (recOfBatch != affectedRows) {
        errorPrint("[%d] %s() LN%d execInsert insert %d, affected rows: %"PRId64"\n%s\n",
                pThreadInfo->threadID, __func__, __LINE__,
                recOfBatch, affectedRows, pThreadInfo->buffer);
        goto free_of_interlace;
    }

    pThreadInfo->totalAffectedRows += affectedRows;

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

free_of_interlace:
  tmfree(pThreadInfo->buffer);
  printStatPerThread(pThreadInfo);
  return NULL;
}

// sync insertion progressive data
static void* syncWriteProgressive(threadInfo *pThreadInfo) {
  debugPrint("%s() LN%d: ### progressive write\n", __func__, __LINE__);

  SSuperTable* superTblInfo = pThreadInfo->superTblInfo;
  uint64_t maxSqlLen = superTblInfo?superTblInfo->maxSqlLen:g_args.max_sql_len;
  int64_t timeStampStep =
      superTblInfo?superTblInfo->timeStampStep:DEFAULT_TIMESTAMP_STEP;
  int64_t insertRows =
        (superTblInfo)?superTblInfo->insertRows:g_args.num_of_DPT;
  verbosePrint("%s() LN%d insertRows=%"PRId64"\n",
            __func__, __LINE__, insertRows);

  pThreadInfo->buffer = calloc(maxSqlLen, 1);
  if (NULL == pThreadInfo->buffer) {
    errorPrint( "Failed to alloc %"PRIu64" Bytes, reason:%s\n",
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
        errorPrint("[%d] %s() LN%d, getTableName return null\n",
            pThreadInfo->threadID, __func__, __LINE__);
        free(pThreadInfo->buffer);
        return NULL;
      }

      int64_t remainderBufLen = maxSqlLen;
      char *pstr = pThreadInfo->buffer;

      int len = snprintf(pstr,
              strlen(STR_INSERT_INTO) + 1, "%s", STR_INSERT_INTO);

      pstr += len;
      remainderBufLen -= len;

      int32_t generated;
      if (superTblInfo) {
          if (superTblInfo->iface == STMT_IFACE) {
#if STMT_IFACE_ENABLED == 1
              generated = prepareStbStmtProgressive(
                      superTblInfo,
                      pThreadInfo->stmt,
                      tableName,
                      g_args.num_of_RPR,
                      insertRows, i, start_time,
                      &(pThreadInfo->samplePos));
#else
              generated = -1;
#endif
          } else {
              generated = generateStbProgressiveData(
                      superTblInfo,
                      tableName, tableSeq, pThreadInfo->db_name, pstr,
                      insertRows, i, start_time,
                      &(pThreadInfo->samplePos),
                      &remainderBufLen);
          }
      } else {
          if (g_args.iface == STMT_IFACE) {
#if STMT_IFACE_ENABLED == 1
              generated = prepareStmtWithoutStb(
                      pThreadInfo->stmt,
                      tableName,
                      g_args.num_of_RPR,
                      insertRows, i,
                      start_time);
#else
              generated = -1;
#endif
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
      if (generated > 0)
        i += generated;
      else
        goto free_of_progressive;

      start_time +=  generated * timeStampStep;
      pThreadInfo->totalInsertRows += generated;

      startTs = taosGetTimestampMs();

      int32_t affectedRows = execInsert(pThreadInfo, generated);

      endTs = taosGetTimestampMs();
      uint64_t delay = endTs - startTs;
      performancePrint("%s() LN%d, insert execution time is %"PRId64"ms\n",
              __func__, __LINE__, delay);
      verbosePrint("[%d] %s() LN%d affectedRows=%d\n",
            pThreadInfo->threadID,
            __func__, __LINE__, affectedRows);

      if (delay > pThreadInfo->maxDelay) pThreadInfo->maxDelay = delay;
      if (delay < pThreadInfo->minDelay) pThreadInfo->minDelay = delay;
      pThreadInfo->cntDelay++;
      pThreadInfo->totalDelay += delay;

      if (affectedRows < 0) {
        errorPrint("%s() LN%d, affected rows: %d\n",
                __func__, __LINE__, affectedRows);
        goto free_of_progressive;
      }

      pThreadInfo->totalAffectedRows += affectedRows;

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
    }   // num_of_DPT

    if ((g_args.verbose_print) &&
      (tableSeq == pThreadInfo->ntables - 1) && (superTblInfo) &&
        (0 == strncasecmp(
                    superTblInfo->dataSource, "sample", strlen("sample")))) {
          verbosePrint("%s() LN%d samplePos=%"PRId64"\n",
                  __func__, __LINE__, pThreadInfo->samplePos);
    }
  } // tableSeq

free_of_progressive:
  tmfree(pThreadInfo->buffer);
  printStatPerThread(pThreadInfo);
  return NULL;
}

static void* syncWrite(void *sarg) {

  threadInfo *pThreadInfo = (threadInfo *)sarg;
  SSuperTable* superTblInfo = pThreadInfo->superTblInfo;

  uint32_t interlaceRows;

  if (superTblInfo) {
    if ((superTblInfo->interlaceRows == 0)
        && (g_args.interlace_rows > 0)) {
      interlaceRows = g_args.interlace_rows;
    } else {
      interlaceRows = superTblInfo->interlaceRows;
    }
  } else {
    interlaceRows = g_args.interlace_rows;
  }

  if (interlaceRows > 0) {
    // interlace mode
    return syncWriteInterlace(pThreadInfo);
  } else {
    // progressive mode
    return syncWriteProgressive(pThreadInfo);
  }
}

static void callBack(void *param, TAOS_RES *res, int code) {
  threadInfo* pThreadInfo = (threadInfo*)param;
  SSuperTable* superTblInfo = pThreadInfo->superTblInfo;

  int insert_interval =
      superTblInfo?superTblInfo->insertInterval:g_args.insert_interval;
  if (insert_interval) {
    pThreadInfo->et = taosGetTimestampMs();
    if ((pThreadInfo->et - pThreadInfo->st) < insert_interval) {
      taosMsleep(insert_interval - (pThreadInfo->et - pThreadInfo->st)); // ms
    }
  }

  char *buffer = calloc(1, pThreadInfo->superTblInfo->maxSqlLen);
  char data[MAX_DATA_SIZE];
  char *pstr = buffer;
  pstr += sprintf(pstr, "insert into %s.%s%"PRId64" values",
          pThreadInfo->db_name, pThreadInfo->tb_prefix,
          pThreadInfo->start_table_from);
//  if (pThreadInfo->counter >= pThreadInfo->superTblInfo->insertRows) {
  if (pThreadInfo->counter >= g_args.num_of_RPR) {
    pThreadInfo->start_table_from++;
    pThreadInfo->counter = 0;
  }
  if (pThreadInfo->start_table_from > pThreadInfo->end_table_to) {
    tsem_post(&pThreadInfo->lock_sem);
    free(buffer);
    taos_free_result(res);
    return;
  }

  for (int i = 0; i < g_args.num_of_RPR; i++) {
    int rand_num = taosRandom() % 100;
    if (0 != pThreadInfo->superTblInfo->disorderRatio
            && rand_num < pThreadInfo->superTblInfo->disorderRatio) {
      int64_t d = pThreadInfo->lastTs
          - (taosRandom() % pThreadInfo->superTblInfo->disorderRange + 1);
      generateStbRowData(pThreadInfo->superTblInfo, data, d);
    } else {
      generateStbRowData(pThreadInfo->superTblInfo,
              data, pThreadInfo->lastTs += 1000);
    }
    pstr += sprintf(pstr, "%s", data);
    pThreadInfo->counter++;

    if (pThreadInfo->counter >= pThreadInfo->superTblInfo->insertRows) {
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
  SSuperTable* superTblInfo = pThreadInfo->superTblInfo;

  pThreadInfo->st = 0;
  pThreadInfo->et = 0;
  pThreadInfo->lastTs = pThreadInfo->start_time;

  int insert_interval =
      superTblInfo?superTblInfo->insertInterval:g_args.insert_interval;
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
    errorPrint("%s", "ERROR, no such host");
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
        char* precision, SSuperTable* superTblInfo) {

  int32_t timePrec = TSDB_TIME_PRECISION_MILLI;
  if (0 != precision[0]) {
    if (0 == strncasecmp(precision, "ms", 2)) {
      timePrec = TSDB_TIME_PRECISION_MILLI;
    } else if (0 == strncasecmp(precision, "us", 2)) {
      timePrec = TSDB_TIME_PRECISION_MICRO;
    } else if (0 == strncasecmp(precision, "ns", 2)) {
      timePrec = TSDB_TIME_PRECISION_NANO;
    } else {
      errorPrint("Not support precision: %s\n", precision);
      exit(-1);
    }
  }

  int64_t start_time;
  if (superTblInfo) {
    if (0 == strncasecmp(superTblInfo->startTimestamp, "now", 3)) {
        start_time = taosGetTimestamp(timePrec);
    } else {
      if (TSDB_CODE_SUCCESS != taosParseTime(
        superTblInfo->startTimestamp,
        &start_time,
        strlen(superTblInfo->startTimestamp),
        timePrec, 0)) {
          ERROR_EXIT("failed to parse time!\n");
      }
    }
  } else {
     start_time = 1500000000000;
  }

  int64_t start = taosGetTimestampMs();

  // read sample data from file first
  if ((superTblInfo) && (0 == strncasecmp(superTblInfo->dataSource,
              "sample", strlen("sample")))) {
    if (0 != prepareSampleDataForSTable(superTblInfo)) {
      errorPrint("%s() LN%d, prepare sample data for stable failed!\n",
              __func__, __LINE__);
      exit(-1);
    }
  }

  TAOS* taos0 = taos_connect(
              g_Dbs.host, g_Dbs.user,
              g_Dbs.password, db_name, g_Dbs.port);
  if (NULL == taos0) {
    errorPrint("%s() LN%d, connect to server fail , reason: %s\n",
                __func__, __LINE__, taos_errstr(NULL));
    exit(-1);
  }

  int64_t ntables = 0;
  uint64_t tableFrom;

  if (superTblInfo) {
    int64_t limit;
    uint64_t offset;

    if ((NULL != g_args.sqlFile) && (superTblInfo->childTblExists == TBL_NO_EXISTS) &&
            ((superTblInfo->childTblOffset != 0) || (superTblInfo->childTblLimit >= 0))) {
      printf("WARNING: offset and limit will not be used since the child tables not exists!\n");
    }

    if (superTblInfo->childTblExists == TBL_ALREADY_EXISTS) {
      if ((superTblInfo->childTblLimit < 0)
          || ((superTblInfo->childTblOffset + superTblInfo->childTblLimit)
            > (superTblInfo->childTblCount))) {
        superTblInfo->childTblLimit =
            superTblInfo->childTblCount - superTblInfo->childTblOffset;
      }

      offset = superTblInfo->childTblOffset;
      limit = superTblInfo->childTblLimit;
    } else {
      limit = superTblInfo->childTblCount;
      offset = 0;
    }

    ntables = limit;
    tableFrom = offset;

    if ((superTblInfo->childTblExists != TBL_NO_EXISTS)
        && ((superTblInfo->childTblOffset + superTblInfo->childTblLimit )
            > superTblInfo->childTblCount)) {
      printf("WARNING: specified offset + limit > child table count!\n");
      prompt();
    }

    if ((superTblInfo->childTblExists != TBL_NO_EXISTS)
            && (0 == superTblInfo->childTblLimit)) {
      printf("WARNING: specified limit = 0, which cannot find table name to insert or query! \n");
      prompt();
    }

    superTblInfo->childTblName = (char*)calloc(1,
        limit * TSDB_TABLE_NAME_LEN);
    if (superTblInfo->childTblName == NULL) {
      errorPrint("%s() LN%d, alloc memory failed!\n", __func__, __LINE__);
      taos_close(taos0);
      exit(-1);
    }

    int64_t childTblCount;
    getChildNameOfSuperTableWithLimitAndOffset(
        taos0,
        db_name, superTblInfo->sTblName,
        &superTblInfo->childTblName, &childTblCount,
        limit,
        offset);
  } else {
    ntables = g_args.num_of_tables;
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

  if ((superTblInfo)
      && (superTblInfo->iface == REST_IFACE)) {
      if (convertHostToServAddr(
                  g_Dbs.host, g_Dbs.port, &(g_Dbs.serv_addr)) != 0) {
        exit(-1);
      }
  }

  pthread_t *pids = calloc(1, threads * sizeof(pthread_t));
  assert(pids != NULL);

  threadInfo *infos = calloc(1, threads * sizeof(threadInfo));
  assert(infos != NULL);

  memset(pids, 0, threads * sizeof(pthread_t));
  memset(infos, 0, threads * sizeof(threadInfo));

  for (int i = 0; i < threads; i++) {
    threadInfo *pThreadInfo = infos + i;
    pThreadInfo->threadID = i;
    tstrncpy(pThreadInfo->db_name, db_name, TSDB_DB_NAME_LEN);
    pThreadInfo->time_precision = timePrec;
    pThreadInfo->superTblInfo = superTblInfo;

    pThreadInfo->start_time = start_time;
    pThreadInfo->minDelay = UINT64_MAX;

    if ((NULL == superTblInfo) ||
            (superTblInfo->iface != REST_IFACE)) {
      //t_info->taos = taos;
      pThreadInfo->taos = taos_connect(
              g_Dbs.host, g_Dbs.user,
              g_Dbs.password, db_name, g_Dbs.port);
      if (NULL == pThreadInfo->taos) {
        errorPrint(
                "%s() LN%d, connect to server fail from insert sub thread, reason: %s\n",
                __func__, __LINE__,
                taos_errstr(NULL));
        free(infos);
        exit(-1);
      }

#if STMT_IFACE_ENABLED == 1
      if ((g_args.iface == STMT_IFACE)
          || ((superTblInfo) && (superTblInfo->iface == STMT_IFACE))) {

          int columnCount;
          if (superTblInfo) {
              columnCount = superTblInfo->columnCount;
          } else {
              columnCount = g_args.num_of_CPR;
          }

        pThreadInfo->stmt = taos_stmt_init(pThreadInfo->taos);
        if (NULL == pThreadInfo->stmt) {
            errorPrint(
                "%s() LN%d, failed init stmt, reason: %s\n",
                __func__, __LINE__,
                taos_errstr(NULL));
            free(pids);
            free(infos);
            exit(-1);
        }

        char buffer[3000];
        char *pstr = buffer;

        if (AUTO_CREATE_SUBTBL == superTblInfo->autoCreateTable) {
            pstr += sprintf(pstr, "INSERT INTO ? USING %s TAGS(?",
                    superTblInfo->sTblName);
            for (int tag = 0; tag < (superTblInfo->tagCount - 1); tag ++ ) {
                pstr += sprintf(pstr, ",?");
            }
            pstr += sprintf(pstr, ") VALUES(?");
        } else {
            pstr += sprintf(pstr, "INSERT INTO ? VALUES(?");
        }

        for (int col = 0; col < columnCount; col ++) {
            pstr += sprintf(pstr, ",?");
        }
        pstr += sprintf(pstr, ")");

        debugPrint("%s() LN%d, buffer: %s", __func__, __LINE__, buffer);
        int ret = taos_stmt_prepare(pThreadInfo->stmt, buffer, 0);
        if (ret != 0){
            errorPrint("failed to execute taos_stmt_prepare. return 0x%x. reason: %s\n",
                    ret, taos_errstr(NULL));
            free(pids);
            free(infos);
            exit(-1);
        }
      }
#endif
    } else {
      pThreadInfo->taos = NULL;
    }

/*    if ((NULL == superTblInfo)
            || (0 == superTblInfo->multiThreadWriteOneTbl)) {
            */
      pThreadInfo->start_table_from = tableFrom;
      pThreadInfo->ntables = i<b?a+1:a;
      pThreadInfo->end_table_to = i < b ? tableFrom + a : tableFrom + a - 1;
      tableFrom = pThreadInfo->end_table_to + 1;
/*    } else {
      pThreadInfo->start_table_from = 0;
      pThreadInfo->ntables = superTblInfo->childTblCount;
      pThreadInfo->start_time = pThreadInfo->start_time + rand_int() % 10000 - rand_tinyint();
    }
*/
    tsem_init(&(pThreadInfo->lock_sem), 0, 0);
    if (ASYNC_MODE == g_Dbs.asyncMode) {
      pthread_create(pids + i, NULL, asyncWrite, pThreadInfo);
    } else {
      pthread_create(pids + i, NULL, syncWrite, pThreadInfo);
    }
  }

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

#if STMT_IFACE_ENABLED == 1
    if (pThreadInfo->stmt) {
      taos_stmt_close(pThreadInfo->stmt);
    }
#endif
    tsem_destroy(&(pThreadInfo->lock_sem));
    taos_close(pThreadInfo->taos);

    debugPrint("%s() LN%d, [%d] totalInsert=%"PRIu64" totalAffected=%"PRIu64"\n",
            __func__, __LINE__,
            pThreadInfo->threadID, pThreadInfo->totalInsertRows,
            pThreadInfo->totalAffectedRows);
    if (superTblInfo) {
        superTblInfo->totalAffectedRows += pThreadInfo->totalAffectedRows;
        superTblInfo->totalInsertRows += pThreadInfo->totalInsertRows;
    } else {
        g_args.totalAffectedRows += pThreadInfo->totalAffectedRows;
        g_args.totalInsertRows += pThreadInfo->totalInsertRows;
    }

    totalDelay  += pThreadInfo->totalDelay;
    cntDelay   += pThreadInfo->cntDelay;
    if (pThreadInfo->maxDelay > maxDelay) maxDelay = pThreadInfo->maxDelay;
    if (pThreadInfo->minDelay < minDelay) minDelay = pThreadInfo->minDelay;
  }
  cntDelay -= 1;

  if (cntDelay == 0)    cntDelay = 1;
  avgDelay = (double)totalDelay / cntDelay;

  int64_t end = taosGetTimestampMs();
  int64_t t = end - start;

  if (superTblInfo) {
    fprintf(stderr, "Spent %.2f seconds to insert rows: %"PRIu64", affected rows: %"PRIu64" with %d thread(s) into %s.%s. %.2f records/second\n\n",
          t / 1000.0, superTblInfo->totalInsertRows,
          superTblInfo->totalAffectedRows,
          threads, db_name, superTblInfo->sTblName,
          (double)superTblInfo->totalInsertRows / (t / 1000.0));

    if (g_fpOfInsertResult) {
      fprintf(g_fpOfInsertResult,
          "Spent %.2f seconds to insert rows: %"PRIu64", affected rows: %"PRIu64" with %d thread(s) into %s.%s. %.2f records/second\n\n",
          t / 1000.0, superTblInfo->totalInsertRows,
          superTblInfo->totalAffectedRows,
          threads, db_name, superTblInfo->sTblName,
          (double)superTblInfo->totalInsertRows / (t / 1000.0));
    }
  } else {
    fprintf(stderr, "Spent %.2f seconds to insert rows: %"PRIu64", affected rows: %"PRIu64" with %d thread(s) into %s %.2f records/second\n\n",
          t / 1000.0, g_args.totalInsertRows,
          g_args.totalAffectedRows,
          threads, db_name,
          (double)g_args.totalInsertRows / (t / 1000.0));
    if (g_fpOfInsertResult) {
      fprintf(g_fpOfInsertResult,
          "Spent %.2f seconds to insert rows: %"PRIu64", affected rows: %"PRIu64" with %d thread(s) into %s %.2f records/second\n\n",
          t * 1000.0, g_args.totalInsertRows,
          g_args.totalAffectedRows,
          threads, db_name,
          (double)g_args.totalInsertRows / (t / 1000.0));
    }
  }

  fprintf(stderr, "insert delay, avg: %10.2fms, max: %"PRIu64"ms, min: %"PRIu64"ms\n\n",
          avgDelay, maxDelay, minDelay);
  if (g_fpOfInsertResult) {
    fprintf(g_fpOfInsertResult, "insert delay, avg:%10.2fms, max: %"PRIu64"ms, min: %"PRIu64"ms\n\n",
          avgDelay, maxDelay, minDelay);
  }

  //taos_close(taos);

  free(pids);
  free(infos);
}

static void *readTable(void *sarg) {
#if 1
  threadInfo *pThreadInfo = (threadInfo *)sarg;
  TAOS *taos = pThreadInfo->taos;
  char command[BUFFER_SIZE] = "\0";
  uint64_t sTime = pThreadInfo->start_time;
  char *tb_prefix = pThreadInfo->tb_prefix;
  FILE *fp = fopen(pThreadInfo->filePath, "a");
  if (NULL == fp) {
    errorPrint( "fopen %s fail, reason:%s.\n", pThreadInfo->filePath, strerror(errno));
    return NULL;
  }

  int64_t num_of_DPT;
/*  if (pThreadInfo->superTblInfo) {
    num_of_DPT = pThreadInfo->superTblInfo->insertRows; //  nrecords_per_table;
  } else {
  */
      num_of_DPT = g_args.num_of_DPT;
//  }

  int64_t num_of_tables = pThreadInfo->ntables; // rinfo->end_table_to - rinfo->start_table_from + 1;
  int64_t totalData = num_of_DPT * num_of_tables;
  bool do_aggreFunc = g_Dbs.do_aggreFunc;

  int n = do_aggreFunc ? (sizeof(aggreFunc) / sizeof(aggreFunc[0])) : 2;
  if (!do_aggreFunc) {
    printf("\nThe first field is either Binary or Bool. Aggregation functions are not supported.\n");
  }
  printf("%"PRId64" records:\n", totalData);
  fprintf(fp, "| QFunctions |    QRecords    |   QSpeed(R/s)   |  QLatency(ms) |\n");

  for (int j = 0; j < n; j++) {
    double totalT = 0;
    uint64_t count = 0;
    for (int64_t i = 0; i < num_of_tables; i++) {
      sprintf(command, "select %s from %s%"PRId64" where ts>= %" PRIu64,
              aggreFunc[j], tb_prefix, i, sTime);

      double t = taosGetTimestampMs();
      TAOS_RES *pSql = taos_query(taos, command);
      int32_t code = taos_errno(pSql);

      if (code != 0) {
        errorPrint( "Failed to query:%s\n", taos_errstr(pSql));
        taos_free_result(pSql);
        taos_close(taos);
        fclose(fp);
        return NULL;
      }

      while(taos_fetch_row(pSql) != NULL) {
        count++;
      }

      t = taosGetTimestampMs() - t;
      totalT += t;

      taos_free_result(pSql);
    }

    fprintf(fp, "|%10s  |   %"PRId64"   |  %12.2f   |   %10.2f  |\n",
            aggreFunc[j][0] == '*' ? "   *   " : aggreFunc[j], totalData,
            (double)(num_of_tables * num_of_DPT) / totalT, totalT * 1000);
    printf("select %10s took %.6f second(s)\n", aggreFunc[j], totalT * 1000);
  }
  fprintf(fp, "\n");
  fclose(fp);
#endif
  return NULL;
}

static void *readMetric(void *sarg) {
#if 1
  threadInfo *pThreadInfo = (threadInfo *)sarg;
  TAOS *taos = pThreadInfo->taos;
  char command[BUFFER_SIZE] = "\0";
  FILE *fp = fopen(pThreadInfo->filePath, "a");
  if (NULL == fp) {
    printf("fopen %s fail, reason:%s.\n", pThreadInfo->filePath, strerror(errno));
    return NULL;
  }

  int64_t num_of_DPT = pThreadInfo->superTblInfo->insertRows;
  int64_t num_of_tables = pThreadInfo->ntables; // rinfo->end_table_to - rinfo->start_table_from + 1;
  int64_t totalData = num_of_DPT * num_of_tables;
  bool do_aggreFunc = g_Dbs.do_aggreFunc;

  int n = do_aggreFunc ? (sizeof(aggreFunc) / sizeof(aggreFunc[0])) : 2;
  if (!do_aggreFunc) {
    printf("\nThe first field is either Binary or Bool. Aggregation functions are not supported.\n");
  }
  printf("%"PRId64" records:\n", totalData);
  fprintf(fp, "Querying On %"PRId64" records:\n", totalData);

  for (int j = 0; j < n; j++) {
    char condition[COND_BUF_LEN] = "\0";
    char tempS[64] = "\0";

    int64_t m = 10 < num_of_tables ? 10 : num_of_tables;

    for (int64_t i = 1; i <= m; i++) {
      if (i == 1) {
        sprintf(tempS, "t1 = %"PRId64"", i);
      } else {
        sprintf(tempS, " or t1 = %"PRId64" ", i);
      }
      strncat(condition, tempS, COND_BUF_LEN - 1);

      sprintf(command, "select %s from meters where %s", aggreFunc[j], condition);

      printf("Where condition: %s\n", condition);
      fprintf(fp, "%s\n", command);

      double t = taosGetTimestampMs();

      TAOS_RES *pSql = taos_query(taos, command);
      int32_t code = taos_errno(pSql);

      if (code != 0) {
        errorPrint( "Failed to query:%s\n", taos_errstr(pSql));
        taos_free_result(pSql);
        taos_close(taos);
        fclose(fp);
        return NULL;
      }
      int count = 0;
      while(taos_fetch_row(pSql) != NULL) {
        count++;
      }
      t = taosGetTimestampMs() - t;

      fprintf(fp, "| Speed: %12.2f(per s) | Latency: %.4f(ms) |\n",
              num_of_tables * num_of_DPT / (t * 1000.0), t);
      printf("select %10s took %.6f second(s)\n\n", aggreFunc[j], t * 1000.0);

      taos_free_result(pSql);
    }
    fprintf(fp, "\n");
  }
  fclose(fp);
#endif
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
    errorPrint( "Failed to open %s for save result\n", g_Dbs.resultFile);
    return -1;
  }

  if (g_fpOfInsertResult)
    printfInsertMetaToFile(g_fpOfInsertResult);

  prompt();

  init_rand_data();

  // create database and super tables
  if(createDatabasesAndStables() != 0) {
    if (g_fpOfInsertResult)
      fclose(g_fpOfInsertResult);
    return -1;
  }

  // pretreatement
  if (prepareSampleData() != 0) {
    if (g_fpOfInsertResult)
      fclose(g_fpOfInsertResult);
    return -1;
  }

  double start;
  double end;

  // create child tables
  start = taosGetTimestampMs();
  createChildTables();
  end = taosGetTimestampMs();

  if (g_totalChildTables > 0) {
    fprintf(stderr, "Spent %.4f seconds to create %"PRId64" tables with %d thread(s)\n\n",
            (end - start)/1000.0, g_totalChildTables, g_Dbs.threadCountByCreateTbl);
    if (g_fpOfInsertResult) {
      fprintf(g_fpOfInsertResult,
            "Spent %.4f seconds to create %"PRId64" tables with %d thread(s)\n\n",
            (end - start)/1000.0, g_totalChildTables, g_Dbs.threadCountByCreateTbl);
    }
  }

  // create sub threads for inserting data
  //start = taosGetTimestampMs();
  for (int i = 0; i < g_Dbs.dbCount; i++) {
    if (g_Dbs.use_metric) {
      if (g_Dbs.db[i].superTblCount > 0) {
        for (uint64_t j = 0; j < g_Dbs.db[i].superTblCount; j++) {

          SSuperTable* superTblInfo = &g_Dbs.db[i].superTbls[j];

          if (superTblInfo && (superTblInfo->insertRows > 0)) {
            startMultiThreadInsertData(
              g_Dbs.threadCount,
              g_Dbs.db[i].dbName,
              g_Dbs.db[i].dbCfg.precision,
              superTblInfo);
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
  postFreeResource();

  return 0;
}

static void *specifiedTableQuery(void *sarg) {
  threadInfo *pThreadInfo = (threadInfo *)sarg;

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
      return NULL;
    } else {
      pThreadInfo->taos = taos;
    }
  }

  char sqlStr[TSDB_DB_NAME_LEN + 5];
  sprintf(sqlStr, "use %s", g_queryInfo.dbName);
  if (0 != queryDbExec(pThreadInfo->taos, sqlStr, NO_INSERT_TYPE, false)) {
    taos_close(pThreadInfo->taos);
    errorPrint( "use database %s failed!\n\n",
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
      debugPrint("%s() LN%d, endTs=%"PRIu64"ms, startTs=%"PRIu64"ms\n",
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
  char subTblName[MAX_TB_NAME_SIZE*3];
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
  strncat(outSql, subTblName, MAX_QUERY_SQL_LENGTH - 1);
  //printf("2: %s\n", outSql);
  strncat(outSql, pos+strlen(sourceString), MAX_QUERY_SQL_LENGTH - 1);
  //printf("3: %s\n", outSql);
}

static void *superTableQuery(void *sarg) {
  char sqlstr[MAX_QUERY_SQL_LENGTH];
  threadInfo *pThreadInfo = (threadInfo *)sarg;

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
        memset(sqlstr,0,sizeof(sqlstr));
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
    errorPrint( "Failed to connect to TDengine, reason:%s\n",
            taos_errstr(NULL));
    exit(-1);
  }

  if (0 != g_queryInfo.superQueryInfo.sqlCount) {
    getAllChildNameOfSuperTable(taos,
            g_queryInfo.dbName,
            g_queryInfo.superQueryInfo.sTblName,
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
      exit(-1);
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

                char sqlStr[MAX_TB_NAME_SIZE*2];
                sprintf(sqlStr, "use %s", g_queryInfo.dbName);
                if (0 != queryDbExec(taos, sqlStr, NO_INSERT_TYPE, false)) {
                    taos_close(taos);
                    free(infos);
                    free(pids);
                    errorPrint( "use database %s failed!\n\n",
                        g_queryInfo.dbName);
                    return -1;
                }
            }

            pThreadInfo->taos = NULL;// TODO: workaround to use separate taos connection;

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
      pThreadInfo->taos = NULL; // TODO: workaround to use separate taos connection;
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
      }
    }
  }

  tmfree((char*)pids);
  tmfree((char*)infos);

  for (int i = 0; i < g_queryInfo.superQueryInfo.threadCnt; i++) {
    pthread_join(pidsOfSub[i], NULL);
  }

  tmfree((char*)pidsOfSub);
  tmfree((char*)infosOfSub);

//  taos_close(taos);// TODO: workaround to use separate taos connection;
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
    errorPrint("%s() LN%d, failed to subscribe result, code:%d, reason:%s\n",
           __func__, __LINE__, code, taos_errstr(res));
    return;
  }

  if (param)
    fetchResult(res, (threadInfo *)param);
  // tao_unscribe() will free result.
}

static void specified_sub_callback(
        TAOS_SUB* tsub, TAOS_RES *res, void* param, int code) {
  if (res == NULL || taos_errno(res) != 0) {
    errorPrint("%s() LN%d, failed to subscribe result, code:%d, reason:%s\n",
           __func__, __LINE__, code, taos_errstr(res));
    return;
  }

  if (param)
    fetchResult(res, (threadInfo *)param);
  // tao_unscribe() will free result.
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
      errorPrint("failed to create subscription. topic:%s, sql:%s\n", topic, sql);
      return NULL;
  }

  return tsub;
}

static void *superSubscribe(void *sarg) {
    threadInfo *pThreadInfo = (threadInfo *)sarg;
    char subSqlstr[MAX_QUERY_SQL_LENGTH];
    TAOS_SUB*    tsub[MAX_QUERY_SQL_COUNT] = {0};
    uint64_t tsubSeq;

    if (pThreadInfo->ntables > MAX_QUERY_SQL_COUNT) {
        errorPrint("The table number(%"PRId64") of the thread is more than max query sql count: %d\n",
                pThreadInfo->ntables, MAX_QUERY_SQL_COUNT);
        exit(-1);
    }

    if (pThreadInfo->taos == NULL) {
        pThreadInfo->taos = taos_connect(g_queryInfo.host,
                g_queryInfo.user,
                g_queryInfo.password,
                g_queryInfo.dbName,
                g_queryInfo.port);
        if (pThreadInfo->taos == NULL) {
            errorPrint("[%d] Failed to connect to TDengine, reason:%s\n",
                    pThreadInfo->threadID, taos_errstr(NULL));
            return NULL;
        }
    }

    char sqlStr[MAX_TB_NAME_SIZE*2];
    sprintf(sqlStr, "use %s", g_queryInfo.dbName);
    if (0 != queryDbExec(pThreadInfo->taos, sqlStr, NO_INSERT_TYPE, false)) {
        taos_close(pThreadInfo->taos);
        errorPrint( "use database %s failed!\n\n",
                g_queryInfo.dbName);
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
        memset(subSqlstr, 0, sizeof(subSqlstr));
        replaceChildTblName(
                g_queryInfo.superQueryInfo.sql[pThreadInfo->querySeq],
                subSqlstr, i);
        if (g_queryInfo.superQueryInfo.result[pThreadInfo->querySeq][0] != 0) {
            sprintf(pThreadInfo->filePath, "%s-%d",
                    g_queryInfo.superQueryInfo.result[pThreadInfo->querySeq],
                    pThreadInfo->threadID);
        }

        verbosePrint("%s() LN%d, [%d] subSqlstr: %s\n",
                __func__, __LINE__, pThreadInfo->threadID, subSqlstr);
        tsub[tsubSeq] = subscribeImpl(
                STABLE_CLASS,
                pThreadInfo, subSqlstr, topic,
                g_queryInfo.superQueryInfo.subscribeRestart,
                g_queryInfo.superQueryInfo.subscribeInterval);
        if (NULL == tsub[tsubSeq]) {
            taos_close(pThreadInfo->taos);
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
                            pThreadInfo, subSqlstr, topic,
                            g_queryInfo.superQueryInfo.subscribeRestart,
                            g_queryInfo.superQueryInfo.subscribeInterval
                            );
                    if (NULL == tsub[tsubSeq]) {
                        taos_close(pThreadInfo->taos);
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
    return NULL;
}

static void *specifiedSubscribe(void *sarg) {
  threadInfo *pThreadInfo = (threadInfo *)sarg;
//  TAOS_SUB*  tsub = NULL;

  if (pThreadInfo->taos == NULL) {
    pThreadInfo->taos = taos_connect(g_queryInfo.host,
          g_queryInfo.user,
          g_queryInfo.password,
          g_queryInfo.dbName,
          g_queryInfo.port);
    if (pThreadInfo->taos == NULL) {
      errorPrint("[%d] Failed to connect to TDengine, reason:%s\n",
            pThreadInfo->threadID, taos_errstr(NULL));
      return NULL;
    }
  }

  char sqlStr[MAX_TB_NAME_SIZE*2];
  sprintf(sqlStr, "use %s", g_queryInfo.dbName);
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
    errorPrint( "Failed to connect to TDengine, reason:%s\n",
            taos_errstr(NULL));
    exit(-1);
  }

  if (0 != g_queryInfo.superQueryInfo.sqlCount) {
    getAllChildNameOfSuperTable(taos,
            g_queryInfo.dbName,
            g_queryInfo.superQueryInfo.sTblName,
            &g_queryInfo.superQueryInfo.childTblName,
            &g_queryInfo.superQueryInfo.childTblCount);
  }

  taos_close(taos); // TODO: workaround to use separate taos connection;

  pthread_t  *pids = NULL;
  threadInfo *infos = NULL;

  pthread_t  *pidsOfStable  = NULL;
  threadInfo *infosOfStable = NULL;

  //==== create threads for query for specified table
  if (g_queryInfo.specifiedQueryInfo.sqlCount <= 0) {
    debugPrint("%s() LN%d, sepcified query sqlCount %d.\n",
              __func__, __LINE__,
              g_queryInfo.specifiedQueryInfo.sqlCount);
  } else {
    if (g_queryInfo.specifiedQueryInfo.concurrent <= 0) {
        errorPrint("%s() LN%d, sepcified query sqlCount %d.\n",
              __func__, __LINE__,
              g_queryInfo.specifiedQueryInfo.sqlCount);
        exit(-1);
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
        errorPrint("%s() LN%d, malloc failed for create threads\n", __func__, __LINE__);
        exit(-1);
    }

    for (int i = 0; i < g_queryInfo.specifiedQueryInfo.sqlCount; i++) {
        for (int j = 0; j < g_queryInfo.specifiedQueryInfo.concurrent; j++) {
            uint64_t seq = i * g_queryInfo.specifiedQueryInfo.concurrent + j;
            threadInfo *pThreadInfo = infos + seq;
            pThreadInfo->threadID = seq;
            pThreadInfo->querySeq = i;
            pThreadInfo->taos = NULL;  // TODO: workaround to use separate taos connection;
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
            errorPrint("%s() LN%d, malloc failed for create threads\n",
              __func__, __LINE__);
            // taos_close(taos);
            exit(-1);
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
                pThreadInfo->taos = NULL; // TODO: workaround to use separate taos connection;
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

static void initOfInsertMeta() {
  memset(&g_Dbs, 0, sizeof(SDbs));

  // set default values
  tstrncpy(g_Dbs.host, "127.0.0.1", MAX_HOSTNAME_SIZE);
  g_Dbs.port = 6030;
  tstrncpy(g_Dbs.user, TSDB_DEFAULT_USER, MAX_USERNAME_SIZE);
  tstrncpy(g_Dbs.password, TSDB_DEFAULT_PASS, MAX_PASSWORD_SIZE);
  g_Dbs.threadCount = 2;

  g_Dbs.use_metric = g_args.use_metric;
}

static void initOfQueryMeta() {
  memset(&g_queryInfo, 0, sizeof(SQueryMetaInfo));

  // set default values
  tstrncpy(g_queryInfo.host, "127.0.0.1", MAX_HOSTNAME_SIZE);
  g_queryInfo.port = 6030;
  tstrncpy(g_queryInfo.user, TSDB_DEFAULT_USER, MAX_USERNAME_SIZE);
  tstrncpy(g_queryInfo.password, TSDB_DEFAULT_PASS, MAX_PASSWORD_SIZE);
}

static void setParaFromArg(){
  if (g_args.host) {
    tstrncpy(g_Dbs.host, g_args.host, MAX_HOSTNAME_SIZE);
  } else {
    tstrncpy(g_Dbs.host, "127.0.0.1", MAX_HOSTNAME_SIZE);
  }

  if (g_args.user) {
    tstrncpy(g_Dbs.user, g_args.user, MAX_USERNAME_SIZE);
  }

  if (g_args.password) {
    tstrncpy(g_Dbs.password, g_args.password, MAX_PASSWORD_SIZE);
  }

  if (g_args.port) {
    g_Dbs.port = g_args.port;
  }

  g_Dbs.threadCount = g_args.num_of_threads;
  g_Dbs.threadCountByCreateTbl = g_args.num_of_threads;

  g_Dbs.dbCount = 1;
  g_Dbs.db[0].drop = true;

  tstrncpy(g_Dbs.db[0].dbName, g_args.database, TSDB_DB_NAME_LEN);
  g_Dbs.db[0].dbCfg.replica = g_args.replica;
  tstrncpy(g_Dbs.db[0].dbCfg.precision, "ms", 8);

  tstrncpy(g_Dbs.resultFile, g_args.output_file, MAX_FILE_NAME_LEN);

  g_Dbs.use_metric = g_args.use_metric;
  g_Dbs.insert_only = g_args.insert_only;

  g_Dbs.do_aggreFunc = true;

  char dataString[STRING_LEN];
  char **data_type = g_args.datatype;

  memset(dataString, 0, STRING_LEN);

  if (strcasecmp(data_type[0], "BINARY") == 0
          || strcasecmp(data_type[0], "BOOL") == 0
          || strcasecmp(data_type[0], "NCHAR") == 0 ) {
    g_Dbs.do_aggreFunc = false;
  }

  if (g_args.use_metric) {
    g_Dbs.db[0].superTblCount = 1;
    tstrncpy(g_Dbs.db[0].superTbls[0].sTblName, "meters", TSDB_TABLE_NAME_LEN);
    g_Dbs.db[0].superTbls[0].childTblCount = g_args.num_of_tables;
    g_Dbs.threadCount = g_args.num_of_threads;
    g_Dbs.threadCountByCreateTbl = g_args.num_of_threads;
    g_Dbs.asyncMode = g_args.async_mode;

    g_Dbs.db[0].superTbls[0].autoCreateTable = PRE_CREATE_SUBTBL;
    g_Dbs.db[0].superTbls[0].childTblExists = TBL_NO_EXISTS;
    g_Dbs.db[0].superTbls[0].disorderRange = g_args.disorderRange;
    g_Dbs.db[0].superTbls[0].disorderRatio = g_args.disorderRatio;
    tstrncpy(g_Dbs.db[0].superTbls[0].childTblPrefix,
            g_args.tb_prefix, TSDB_TABLE_NAME_LEN - 20);
    tstrncpy(g_Dbs.db[0].superTbls[0].dataSource, "rand", MAX_TB_NAME_SIZE);

    if (g_args.iface == INTERFACE_BUT) {
        g_Dbs.db[0].superTbls[0].iface = TAOSC_IFACE;
    } else {
        g_Dbs.db[0].superTbls[0].iface = g_args.iface;
    }
    tstrncpy(g_Dbs.db[0].superTbls[0].startTimestamp,
            "2017-07-14 10:40:00.000", MAX_TB_NAME_SIZE);
    g_Dbs.db[0].superTbls[0].timeStampStep = DEFAULT_TIMESTAMP_STEP;

    g_Dbs.db[0].superTbls[0].insertRows = g_args.num_of_DPT;
    g_Dbs.db[0].superTbls[0].maxSqlLen = g_args.max_sql_len;

    g_Dbs.db[0].superTbls[0].columnCount = 0;
    for (int i = 0; i < MAX_NUM_COLUMNS; i++) {
      if (data_type[i] == NULL) {
        break;
      }

      tstrncpy(g_Dbs.db[0].superTbls[0].columns[i].dataType,
              data_type[i], strlen(data_type[i]) + 1);
      g_Dbs.db[0].superTbls[0].columns[i].dataLen = g_args.len_of_binary;
      g_Dbs.db[0].superTbls[0].columnCount++;
    }

    if (g_Dbs.db[0].superTbls[0].columnCount > g_args.num_of_CPR) {
      g_Dbs.db[0].superTbls[0].columnCount = g_args.num_of_CPR;
    } else {
      for (int i = g_Dbs.db[0].superTbls[0].columnCount;
              i < g_args.num_of_CPR; i++) {
        tstrncpy(g_Dbs.db[0].superTbls[0].columns[i].dataType,
                "INT", strlen("INT") + 1);
        g_Dbs.db[0].superTbls[0].columns[i].dataLen = 0;
        g_Dbs.db[0].superTbls[0].columnCount++;
      }
    }

    tstrncpy(g_Dbs.db[0].superTbls[0].tags[0].dataType,
            "INT", strlen("INT") + 1);
    g_Dbs.db[0].superTbls[0].tags[0].dataLen = 0;

    tstrncpy(g_Dbs.db[0].superTbls[0].tags[1].dataType,
            "BINARY", strlen("BINARY") + 1);
    g_Dbs.db[0].superTbls[0].tags[1].dataLen = g_args.len_of_binary;
    g_Dbs.db[0].superTbls[0].tagCount = 2;
  } else {
    g_Dbs.threadCountByCreateTbl = g_args.num_of_threads;
    g_Dbs.db[0].superTbls[0].tagCount = 0;
  }
}

/* Function to do regular expression check */
static int regexMatch(const char *s, const char *reg, int cflags) {
  regex_t regex;
  char    msgbuf[100] = {0};

  /* Compile regular expression */
  if (regcomp(&regex, reg, cflags) != 0) {
    printf("Fail to compile regex\n");
    exit(-1);
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
    printf("Regex match failed: %s\n", msgbuf);
    regfree(&regex);
    exit(-1);
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
  char *    cmd = calloc(1, MAX_SQL_SIZE);
  size_t    cmd_len = 0;
  char *    line = NULL;
  size_t    line_len = 0;

  double t = taosGetTimestampMs();

  while((read_len = tgetline(&line, &line_len, fp)) != -1) {
    if (read_len >= MAX_SQL_SIZE) continue;
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
        errorPrint("%s() LN%d, queryDbExec %s failed!\n",
               __func__, __LINE__, cmd);
        tmfree(cmd);
        tmfree(line);
        tmfclose(fp);
        return;
    }
    memset(cmd, 0, MAX_SQL_SIZE);
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

static void queryResult() {
  // query data

  pthread_t read_id;
  threadInfo *pThreadInfo = calloc(1, sizeof(threadInfo));
  assert(pThreadInfo);
  pThreadInfo->start_time = 1500000000000;  // 2017-07-14 10:40:00.000
  pThreadInfo->start_table_from = 0;

  //pThreadInfo->do_aggreFunc = g_Dbs.do_aggreFunc;
  if (g_args.use_metric) {
    pThreadInfo->ntables = g_Dbs.db[0].superTbls[0].childTblCount;
    pThreadInfo->end_table_to = g_Dbs.db[0].superTbls[0].childTblCount - 1;
    pThreadInfo->superTblInfo = &g_Dbs.db[0].superTbls[0];
    tstrncpy(pThreadInfo->tb_prefix,
          g_Dbs.db[0].superTbls[0].childTblPrefix, TSDB_TABLE_NAME_LEN - 20);
  } else {
    pThreadInfo->ntables = g_args.num_of_tables;
    pThreadInfo->end_table_to = g_args.num_of_tables -1;
    tstrncpy(pThreadInfo->tb_prefix, g_args.tb_prefix, TSDB_TABLE_NAME_LEN);
  }

  pThreadInfo->taos = taos_connect(
          g_Dbs.host,
          g_Dbs.user,
          g_Dbs.password,
          g_Dbs.db[0].dbName,
          g_Dbs.port);
  if (pThreadInfo->taos == NULL) {
    errorPrint( "Failed to connect to TDengine, reason:%s\n",
            taos_errstr(NULL));
    free(pThreadInfo);
    exit(-1);
  }

  tstrncpy(pThreadInfo->filePath, g_Dbs.resultFile, MAX_FILE_NAME_LEN);

  if (!g_Dbs.use_metric) {
    pthread_create(&read_id, NULL, readTable, pThreadInfo);
  } else {
    pthread_create(&read_id, NULL, readMetric, pThreadInfo);
  }
  pthread_join(read_id, NULL);
  taos_close(pThreadInfo->taos);
  free(pThreadInfo);
}

static void testCmdLine() {

  if (strlen(configDir)) {
    wordexp_t full_path;
    if (wordexp(configDir, &full_path, 0) != 0) {
      errorPrint( "Invalid path %s\n", configDir);
      return;
    }
    taos_options(TSDB_OPTION_CONFIGDIR, full_path.we_wordv[0]);
    wordfree(&full_path);
  }

  g_args.test_mode = INSERT_TEST;
  insertTestProcess();

  if (false == g_Dbs.insert_only)
    queryResult();
}

int main(int argc, char *argv[]) {
  parse_args(argc, argv, &g_args);

  debugPrint("meta file: %s\n", g_args.metaFile);

  if (g_args.metaFile) {
    initOfInsertMeta();
    initOfQueryMeta();

    if (false == getInfoFromJsonFile(g_args.metaFile)) {
      printf("Failed to read %s\n", g_args.metaFile);
      return 1;
    }

    testMetaFile();
  } else {
    memset(&g_Dbs, 0, sizeof(SDbs));
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

  return 0;
}

