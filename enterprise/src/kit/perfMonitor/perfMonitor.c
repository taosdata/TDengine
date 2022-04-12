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
   This program is used for tdengine performance test, including insertion and query functions. 
   Only run in linux.
*/

/*
   when in some thread query return error, thread don't exit, but return, otherwise coredump in other thread.
   */

#include <stdint.h>
#include <taos.h>
#define _GNU_SOURCE
#define CURL_STATICLIB

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

#define MAX_RECORDS_PER_REQ     32766

#define HEAD_BUFF_LEN       TSDB_MAX_COLUMNS*24  // 16*MAX_COLUMNS + (192+32)*2 + insert into ..

#define BUFFER_SIZE         TSDB_MAX_ALLOWED_SQL_LEN
#define COND_BUF_LEN        (BUFFER_SIZE - 30)
#define COL_BUFFER_LEN      ((TSDB_COL_NAME_LEN + 15) * TSDB_MAX_COLUMNS)

#define MAX_USERNAME_SIZE  64
#define MAX_PASSWORD_SIZE  20
#define MAX_HOSTNAME_SIZE  253      // https://man7.org/linux/man-pages/man7/hostname.7.html
#define MAX_TB_NAME_SIZE   64
#define MAX_DATA_SIZE      (16*TSDB_MAX_COLUMNS)+20     // max record len: 16*MAX_COLUMNS, timestamp string and ,('') need extra space
#define OPT_ABORT          1 /* –abort */
#define MAX_FILE_NAME_LEN  256              // max file name length on linux is 255.

#define MAX_PREPARED_RAND  1000000
#define INT_BUFF_LEN            11
#define BIGINT_BUFF_LEN         21
#define SMALLINT_BUFF_LEN       6
#define TINYINT_BUFF_LEN        4
#define BOOL_BUFF_LEN           6
#define FLOAT_BUFF_LEN          22
#define DOUBLE_BUFF_LEN         42
#define TIMESTAMP_BUFF_LEN      21

#define MAX_NUM_COLUMNS        (TSDB_MAX_COLUMNS - 1)      // exclude first column timestamp

#define MAX_DB_COUNT            2
#define MAX_SUPER_TABLE_COUNT   8

#define MAX_QUERY_SQL_COUNT     100

#define MAX_DATABASE_COUNT      256
#define INPUT_BUF_LEN           256

#define TBNAME_PREFIX_LEN       (TSDB_TABLE_NAME_LEN - 20) // 20 characters reserved for seq
#define SMALL_BUFF_LEN          8
#define DATATYPE_BUFF_LEN       (SMALL_BUFF_LEN*3)
#define NOTE_BUFF_LEN           (SMALL_BUFF_LEN*16)

#define DEFAULT_TIMESTAMP_STEP  1


enum TEST_MODE {
    INSERT_TEST,            // 0
    QUERY_TEST,             // 1
    SUBSCRIBE_TEST,         // 2
    INVAID_TEST
};

// 0: write file, 1: only format but not write file, 2: only pull but not format
enum RESULT_MODE {
    RESULT_WRITE_FILE,        // 0
    RESULT_ONLY_FORMAT,       // 1
    RESULT_ONLY_PULL,         // 2
    RESULT_BUTT
};

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

enum enum_TAOS_INSERT_MODE {
    TAOSC_RAND,
    TAOSC_CSV,
    TAOSC_SCHEMALESS,
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
typedef struct InArguments_S {
    char *   metaFile;
    uint32_t test_mode;
    char *   host;
    uint16_t port;
    char *   user;
    char     password[MAX_PASSWORD_SIZE];
    bool     answer_yes;
    bool     debug_print;
    bool     verbose_print;
    bool     performance_print;
    //int      abort;
    //uint64_t totalInsertRows;
    //uint64_t totalAffectedRows;
} InArguments;

typedef struct SColumn_S {
    char      field[TSDB_COL_NAME_LEN];
    char      dataType[DATATYPE_BUFF_LEN];
    uint32_t  dataLen;
    char      note[NOTE_BUFF_LEN];
} StrColumn;

typedef struct SSuperTable_S {
    char         sTblName[TSDB_TABLE_NAME_LEN];
    char         dataSource[SMALL_BUFF_LEN];  // rand_gen or sample
    char         childTblPrefix[TBNAME_PREFIX_LEN];
    uint16_t     childTblExists;
    int64_t      childTblCount;
    uint64_t     batchCreateTableNum;     // 0: no batch,  > 0: batch table number in one sql

    uint64_t     maxSqlLen;               //
    int64_t      insertMode;          // rand generate, csv file
    int64_t      insertRows;
    int64_t      timeStampStep;
    char         startTimestamp[MAX_TB_NAME_SIZE];
    char         sampleFormat[SMALL_BUFF_LEN];  // csv, json
    char         csvFile[MAX_FILE_NAME_LEN];
    char         tagsFile[MAX_FILE_NAME_LEN];

    uint32_t     columnCount;
    StrColumn    columns[TSDB_MAX_COLUMNS];
    uint32_t     tagCount;
    StrColumn    tags[TSDB_MAX_TAGS];

    char*        colsOfCreateChildTable;
    //uint64_t     lenOfOneRow;
    uint64_t     lenOfTagOfOneRow;

    char*        schemalessLineTemplate;

    char*        randDataBuf;
    int32_t      randDataLen;
    int32_t      batchRows;
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
    SSuperTable  superTbls[MAX_SUPER_TABLE_COUNT];
} SDataBase;

typedef struct SDbs_S {
    char        cfgDir[MAX_FILE_NAME_LEN];
    char        host[MAX_HOSTNAME_SIZE];
    //struct      sockaddr_in serv_addr;

    uint16_t    port;
    char        user[MAX_USERNAME_SIZE];
    char        password[MAX_PASSWORD_SIZE];
    char        resultFile[MAX_FILE_NAME_LEN];
    //bool        use_metric;
    //bool        insert_only;
    //bool        do_aggreFunc;
    //bool        asyncMode;

    uint32_t    threadCount;
    uint32_t    threadCountByCreateTbl;
    uint32_t    dbCount;
    SDataBase   db[MAX_DB_COUNT];

    // statistics
    uint64_t    totalInsertRows;
    uint64_t    totalAffectedRows;

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
    char         sql[MAX_QUERY_SQL_COUNT][BUFFER_SIZE+1];
    char         result[MAX_QUERY_SQL_COUNT][MAX_FILE_NAME_LEN];
    int          resultMode[MAX_QUERY_SQL_COUNT]; // 0: write file, 1: only format but not write file, 2: only pull but not format
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
    char         password[MAX_PASSWORD_SIZE];
    char         dbName[TSDB_DB_NAME_LEN];
    char         queryMode[SMALL_BUFF_LEN];  // taosc, rest

    SpecifiedQueryInfo  specifiedQueryInfo;
    SuperQueryInfo      superQueryInfo;
    uint64_t     totalQueried;
} SQueryMetaInfo;

typedef struct SThreadInfo_S {
    TAOS *    taos;
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
    SSuperTable* stbInfo;
    char      *buffer;    // sql cmd buffer

    // for async insert
    //tsem_t    lock_sem;
    int64_t   counter;
    uint64_t  st;
    uint64_t  et;
    uint64_t  lastTs;

    // sample data
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

static void resetAfterAnsiEscape(void) {
    // Reset colors
    printf("\x1b[0m");
}

#include <time.h>

static int taosRandom()
{
    return rand();
}


static int createDatabasesAndStables();
static void createChildTables();
static int queryDbExec(TAOS *taos, char *command, QUERY_TYPE type, bool quiet);
static bool getInfoFromJsonFile(char* file);
static void init_rand_data();

/* ************ Global variables ************  */

int32_t  g_randint[MAX_PREPARED_RAND];
int64_t  g_randbigint[MAX_PREPARED_RAND];
float    g_randfloat[MAX_PREPARED_RAND];
double   g_randdouble[MAX_PREPARED_RAND];

char    *g_randbool_buff = NULL;
char    *g_randint_buff = NULL;
char    *g_rand_voltage_buff = NULL;
char    *g_randbigint_buff = NULL;
char    *g_randsmallint_buff = NULL;
char    *g_randtinyint_buff = NULL;
char    *g_randfloat_buff = NULL;
char    *g_rand_current_buff = NULL;
char    *g_rand_phase_buff = NULL;
char    *g_randdouble_buff = NULL;

static InArguments g_args = {
    NULL,            // metaFile
    0,               // test_mode
    "127.0.0.1",     // host
    6030,            // port
    "root",          // user
#ifdef _TD_POWER_
    "powerdb",      // password
#elif (_TD_TQ_ == true)
    "tqueue",      // password
#else
    "taosdata",      // password
#endif
    false,           // answer yes
    false,           // debug_print
    false,           // verbose_print
    false,           // performance statistic print
//    0,               // abort
};



static SDbs*           g_pDbs;
static int64_t         g_totalChildTables = 0;
static SQueryMetaInfo  g_queryInfo;
static FILE *          g_fpOfInsertResult = NULL;

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
    do { fprintf(stderr, " \033[31m"); fprintf(stderr, "ERROR: "fmt, __VA_ARGS__); fprintf(stderr, " \033[0m"); } while(0)

// for strncpy buffer overflow
#define min(a, b) (((a) < (b)) ? (a) : (b))


///////////////////////////////////////////////////

static void ERROR_EXIT(const char *msg) { errorPrint("%s", msg); exit(-1); }

#ifndef TD_VERNUMBER
#define TD_VERNUMBER    "unknown"
#endif

#ifndef PERF_MONITOR_VERSION
#define PERF_MONITOR_VERSION "1.0.0.0"
#endif

static void printVersion() {
    char tdengine_ver[] = TD_VERNUMBER;
    char perfmonitor_ver[] =  PERF_MONITOR_VERSION;

    printf("TDengine verison %s, perf version:%s\n", tdengine_ver, perfmonitor_ver);

}

static void printHelp() {
    char indent[10] = "        ";
    printf("%s%s%s%s\n", indent, "-f", indent, "The json file to the execution procedure.\n");
}

static void parse_args(int argc, char *argv[], InArguments *arguments) {

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-f") == 0) {
            arguments->metaFile = argv[++i];
        } else if ((strcmp(argv[i], "--version") == 0) || (strcmp(argv[i], "-V") == 0)){
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
}

static void tmfclose(FILE *fp) {
    if (NULL != fp) {
        fclose(fp);
    }
}

static void tmfree(char *buf) {
    if (NULL != buf) {
        taosMemoryFree(buf);
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

    char* databuf = (char*) taosMemoryCalloc(1, 100*1024*1024);
    if (databuf == NULL) {
        errorPrint("%s() LN%d, failed to malloc, warning: save result to file slowly!\n",
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
    taosMemoryFree(databuf);
}

static void selectAndGetResult(     threadInfo *pThreadInfo, char *command, int resultMode)
{
  TAOS_RES *res = taos_query(pThreadInfo->taos, command);
  if (res == NULL || taos_errno(res) != 0) {
      errorPrint("%s() LN%d, failed to execute sql:%s, reason:%s\n",
              __func__, __LINE__, command, taos_errstr(res));
      taos_free_result(res);
      return;
  }

  TAOS_ROW    row = NULL;
  int         num_rows = 0;
  int         num_fields = taos_field_count(res);
  TAOS_FIELD *fields     = taos_fetch_fields(res);

  // 0: write file, 1: only format but not write file, 2: only pull but not format
  char* databuf = NULL;
  if (RESULT_WRITE_FILE == resultMode) {
    databuf = (char*) taosMemoryCalloc(1, 100*1024*1024);
    if (databuf == NULL) {
        errorPrint("%s() LN%d, failed to malloc, warning: save result to file slowly!\n",
                __func__, __LINE__);
        return ;
    }
  }
  
  int64_t   totalLen = 0;

  // fetch the records row by row
  while((row = taos_fetch_row(res))) {
      num_rows++;
      
      if (RESULT_ONLY_PULL == resultMode) continue;

      char  temp[HEAD_BUFF_LEN] = {0};
      int len = taos_print_row(temp, row, fields, num_fields);
      len += sprintf(temp + len, "\n");

      if (RESULT_ONLY_FORMAT == resultMode) continue;      
      
      if (totalLen >= (100*1024*1024 - HEAD_BUFF_LEN*2)) {
          appendResultBufToFile(databuf, pThreadInfo);
          totalLen = 0;
          memset(databuf, 0, 100*1024*1024);
      }

      memcpy(databuf + totalLen, temp, len);
      totalLen += len;
  }

  if ((RESULT_WRITE_FILE == resultMode) && (totalLen > 0)) {
      appendResultBufToFile(databuf, pThreadInfo);
  }

  taosMemoryFree(databuf);  
  taos_free_result(res);
}

static char *rand_bool_str(){
    static int cursor;
    cursor++;
    if (cursor > (MAX_PREPARED_RAND - 1)) cursor = 0;
    return g_randbool_buff + (cursor * BOOL_BUFF_LEN);
}

static int32_t rand_bool(){
    static int cursor;
    cursor++;
    if (cursor > (MAX_PREPARED_RAND - 1)) cursor = 0;
    return g_randint[cursor % MAX_PREPARED_RAND] % 2;
}

static char *rand_tinyint_str()
{
    static int cursor;
    cursor++;
    if (cursor > (MAX_PREPARED_RAND - 1)) cursor = 0;
    return g_randtinyint_buff + (cursor * TINYINT_BUFF_LEN);
}

static int32_t rand_tinyint()
{
    static int cursor;
    cursor++;
    if (cursor > (MAX_PREPARED_RAND - 1)) cursor = 0;
    return g_randint[cursor % MAX_PREPARED_RAND] % 128;
}

static char *rand_smallint_str()
{
    static int cursor;
    cursor++;
    if (cursor > (MAX_PREPARED_RAND - 1)) cursor = 0;
    return g_randsmallint_buff + (cursor * SMALLINT_BUFF_LEN);
}

static int32_t rand_smallint()
{
    static int cursor;
    cursor++;
    if (cursor > (MAX_PREPARED_RAND - 1)) cursor = 0;
    return g_randint[cursor % MAX_PREPARED_RAND] % 32767;
}

static char *rand_int_str()
{
    static int cursor;
    cursor++;
    if (cursor > (MAX_PREPARED_RAND - 1)) cursor = 0;
    return g_randint_buff + (cursor * INT_BUFF_LEN);
}

static char *rand_bigint_str()
{
    static int cursor;
    cursor++;
    if (cursor > (MAX_PREPARED_RAND - 1)) cursor = 0;
    return g_randbigint_buff + (cursor * BIGINT_BUFF_LEN);
}

static int64_t rand_bigint()
{
    static int cursor;
    cursor++;
    if (cursor > (MAX_PREPARED_RAND - 1)) cursor = 0;
    return g_randbigint[cursor % MAX_PREPARED_RAND];
}

static char *rand_float_str()
{
    static int cursor;
    cursor++;
    if (cursor > (MAX_PREPARED_RAND - 1)) cursor = 0;
    return g_randfloat_buff + (cursor * FLOAT_BUFF_LEN);
}


static float rand_float()
{
    static int cursor;
    cursor++;
    if (cursor > (MAX_PREPARED_RAND - 1)) cursor = 0;
    return g_randfloat[cursor % MAX_PREPARED_RAND];
}

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
    if (cursor > (MAX_PREPARED_RAND - 1)) cursor = 0;
    return g_randdouble_buff + (cursor * DOUBLE_BUFF_LEN);
}

static double rand_double()
{
    static int cursor;
    cursor++;
    cursor = cursor % MAX_PREPARED_RAND;
    return g_randdouble[cursor];
}

static void init_rand_data() {

    g_randint_buff = taosMemoryCalloc(1, INT_BUFF_LEN * MAX_PREPARED_RAND);
    assert(g_randint_buff);
    g_rand_voltage_buff = taosMemoryCalloc(1, INT_BUFF_LEN * MAX_PREPARED_RAND);
    assert(g_rand_voltage_buff);
    g_randbigint_buff = taosMemoryCalloc(1, BIGINT_BUFF_LEN * MAX_PREPARED_RAND);
    assert(g_randbigint_buff);
    g_randsmallint_buff = taosMemoryCalloc(1, SMALLINT_BUFF_LEN * MAX_PREPARED_RAND);
    assert(g_randsmallint_buff);
    g_randtinyint_buff = taosMemoryCalloc(1, TINYINT_BUFF_LEN * MAX_PREPARED_RAND);
    assert(g_randtinyint_buff);
    g_randbool_buff = taosMemoryCalloc(1, BOOL_BUFF_LEN * MAX_PREPARED_RAND);
    assert(g_randbool_buff);
    g_randfloat_buff = taosMemoryCalloc(1, FLOAT_BUFF_LEN * MAX_PREPARED_RAND);
    assert(g_randfloat_buff);
    g_rand_current_buff = taosMemoryCalloc(1, FLOAT_BUFF_LEN * MAX_PREPARED_RAND);
    assert(g_rand_current_buff);
    g_rand_phase_buff = taosMemoryCalloc(1, FLOAT_BUFF_LEN * MAX_PREPARED_RAND);
    assert(g_rand_phase_buff);
    g_randdouble_buff = taosMemoryCalloc(1, DOUBLE_BUFF_LEN * MAX_PREPARED_RAND);
    assert(g_randdouble_buff);

    for (int i = 0; i < MAX_PREPARED_RAND; i++){
        g_randint[i] = (int)(taosRandom() % 65535);
        sprintf(g_randint_buff + i * INT_BUFF_LEN, "%d",
                g_randint[i]);
        sprintf(g_rand_voltage_buff + i * INT_BUFF_LEN, "%d",
                215 + g_randint[i] % 10);

        sprintf(g_randbool_buff + i * BOOL_BUFF_LEN, "%s",
                ((g_randint[i] % 2) & 1)?"true":"false");
        sprintf(g_randsmallint_buff + i * SMALLINT_BUFF_LEN, "%d",
                g_randint[i] % 32767);
        sprintf(g_randtinyint_buff + i * TINYINT_BUFF_LEN, "%d",
                g_randint[i] % 128);

        g_randbigint[i] = (int64_t)(taosRandom() % 2147483648);
        sprintf(g_randbigint_buff + i * BIGINT_BUFF_LEN, "%"PRId64"",
                g_randbigint[i]);

        g_randfloat[i] = (float)(taosRandom() / 1000.0);
        sprintf(g_randfloat_buff + i * FLOAT_BUFF_LEN, "%f",
                g_randfloat[i]);
        sprintf(g_rand_current_buff + i * FLOAT_BUFF_LEN, "%f",
                (float)(9.8 + 0.04 * (g_randint[i] % 10)
                    + g_randfloat[i]/1000000000));
        sprintf(g_rand_phase_buff + i * FLOAT_BUFF_LEN, "%f",
                (float)((115 + g_randint[i] % 10
                        + g_randfloat[i]/1000000000)/360));

        g_randdouble[i] = (double)(taosRandom() / 1000000.0);
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

    printf("host:                       \033[33m%s:%u\033[0m\n",
            g_pDbs->host, g_pDbs->port);
    printf("user:                       \033[33m%s\033[0m\n", g_pDbs->user);
    printf("password:                   \033[33m%s\033[0m\n", g_pDbs->password);
    printf("configDir:                  \033[33m%s\033[0m\n", configDir);
    printf("resultFile:                 \033[33m%s\033[0m\n", g_pDbs->resultFile);
    printf("thread num of insert data:  \033[33m%d\033[0m\n", g_pDbs->threadCount);
    printf("thread num of create table: \033[33m%d\033[0m\n", g_pDbs->threadCountByCreateTbl);
    printf("database count:             \033[33m%d\033[0m\n", g_pDbs->dbCount);

    for (int i = 0; i < g_pDbs->dbCount; i++) {
        printf("database[\033[33m%d\033[0m]:\n", i);
        printf("  database[%d] name:      \033[33m%s\033[0m\n", i, g_pDbs->db[i].dbName);
        if (0 == g_pDbs->db[i].drop) {
            printf("  drop:                  \033[33mno\033[0m\n");
        } else {
            printf("  drop:                  \033[33myes\033[0m\n");
        }

        if (g_pDbs->db[i].dbCfg.blocks > 0) {
            printf("  blocks:                \033[33m%d\033[0m\n",
                    g_pDbs->db[i].dbCfg.blocks);
        }
        if (g_pDbs->db[i].dbCfg.cache > 0) {
            printf("  cache:                 \033[33m%d\033[0m\n",
                    g_pDbs->db[i].dbCfg.cache);
        }
        if (g_pDbs->db[i].dbCfg.days > 0) {
            printf("  days:                  \033[33m%d\033[0m\n",
                    g_pDbs->db[i].dbCfg.days);
        }
        if (g_pDbs->db[i].dbCfg.keep > 0) {
            printf("  keep:                  \033[33m%d\033[0m\n",
                    g_pDbs->db[i].dbCfg.keep);
        }
        if (g_pDbs->db[i].dbCfg.replica > 0) {
            printf("  replica:               \033[33m%d\033[0m\n",
                    g_pDbs->db[i].dbCfg.replica);
        }
        if (g_pDbs->db[i].dbCfg.update > 0) {
            printf("  update:                \033[33m%d\033[0m\n",
                    g_pDbs->db[i].dbCfg.update);
        }
        if (g_pDbs->db[i].dbCfg.minRows > 0) {
            printf("  minRows:               \033[33m%d\033[0m\n",
                    g_pDbs->db[i].dbCfg.minRows);
        }
        if (g_pDbs->db[i].dbCfg.maxRows > 0) {
            printf("  maxRows:               \033[33m%d\033[0m\n",
                    g_pDbs->db[i].dbCfg.maxRows);
        }
        if (g_pDbs->db[i].dbCfg.comp > 0) {
            printf("  comp:                  \033[33m%d\033[0m\n", g_pDbs->db[i].dbCfg.comp);
        }
        if (g_pDbs->db[i].dbCfg.walLevel > 0) {
            printf("  walLevel:              \033[33m%d\033[0m\n",
                    g_pDbs->db[i].dbCfg.walLevel);
        }
        if (g_pDbs->db[i].dbCfg.fsync > 0) {
            printf("  fsync:                 \033[33m%d\033[0m\n",
                    g_pDbs->db[i].dbCfg.fsync);
        }
        if (g_pDbs->db[i].dbCfg.quorum > 0) {
            printf("  quorum:                \033[33m%d\033[0m\n",
                    g_pDbs->db[i].dbCfg.quorum);
        }
        if (g_pDbs->db[i].dbCfg.precision[0] != 0) {
            if ((0 == strncasecmp(g_pDbs->db[i].dbCfg.precision, "ms", 2))
                    || (0 == strncasecmp(g_pDbs->db[i].dbCfg.precision, "us", 2))
                    || (0 == strncasecmp(g_pDbs->db[i].dbCfg.precision, "ns", 2))) {
                printf("  precision:             \033[33m%s\033[0m\n",
                        g_pDbs->db[i].dbCfg.precision);
            } else {
                printf("\033[1m\033[40;31m  precision error:       %s\033[0m\n",
                        g_pDbs->db[i].dbCfg.precision);
                return -1;
            }
        }

        printf("  super table count:     \033[33m%"PRIu64"\033[0m\n",
                g_pDbs->db[i].superTblCount);
        for (uint64_t j = 0; j < g_pDbs->db[i].superTblCount; j++) {
            printf("  super table[\033[33m%"PRIu64"\033[0m]:\n", j);

            printf("      stbName:           \033[33m%s\033[0m\n",
                    g_pDbs->db[i].superTbls[j].sTblName);

            printf("      childTblCount:     \033[33m%"PRId64"\033[0m\n",
                    g_pDbs->db[i].superTbls[j].childTblCount);
            printf("      childTblPrefix:    \033[33m%s\033[0m\n",
                    g_pDbs->db[i].superTbls[j].childTblPrefix);
            printf("      insertMode:        \033[33m%s\033[0m\n",
                    (g_pDbs->db[i].superTbls[j].insertMode==TAOSC_RAND)?"rand":"csv");

            printf("      maxSqlLen:         \033[33m%"PRIu64"\033[0m\n",
                    g_pDbs->db[i].superTbls[j].maxSqlLen);
            printf("      timeStampStep:     \033[33m%"PRId64"\033[0m\n",
                    g_pDbs->db[i].superTbls[j].timeStampStep);
            printf("      startTimestamp:    \033[33m%s\033[0m\n",
                    g_pDbs->db[i].superTbls[j].startTimestamp);
            printf("      csvFile:           \033[33m%s\033[0m\n",
                    g_pDbs->db[i].superTbls[j].csvFile);
            printf("      tagsFile:          \033[33m%s\033[0m\n",
                    g_pDbs->db[i].superTbls[j].tagsFile);
            printf("      columnCount:       \033[33m%d\033[0m\n",
                    g_pDbs->db[i].superTbls[j].columnCount);
            for (int k = 0; k < g_pDbs->db[i].superTbls[j].columnCount; k++) {
                if ((0 == strncasecmp(g_pDbs->db[i].superTbls[j].columns[k].dataType,"binary", 6))
                 || (0 == strncasecmp(g_pDbs->db[i].superTbls[j].columns[k].dataType, "nchar", 5))) {
                    printf("column[\033[33m%d\033[0m]:\033[33m%s(%d)\033[0m ", k,
                            g_pDbs->db[i].superTbls[j].columns[k].dataType,
                            g_pDbs->db[i].superTbls[j].columns[k].dataLen);
                } else {
                    printf("column[%d]:\033[33m%s\033[0m ", k,
                            g_pDbs->db[i].superTbls[j].columns[k].dataType);
                }
            }
            printf("\n");

            printf("      tagCount:            \033[33m%d\033[0m\n        ",
                    g_pDbs->db[i].superTbls[j].tagCount);
            for (int k = 0; k < g_pDbs->db[i].superTbls[j].tagCount; k++) {
                if ((0 == strncasecmp(g_pDbs->db[i].superTbls[j].tags[k].dataType, "binary", strlen("binary")))
                 || (0 == strncasecmp(g_pDbs->db[i].superTbls[j].tags[k].dataType, "nchar", strlen("nchar")))) {
                    printf("tag[%d]:\033[33m%s(%d)\033[0m ", k,
                            g_pDbs->db[i].superTbls[j].tags[k].dataType,
                            g_pDbs->db[i].superTbls[j].tags[k].dataLen);
                } else {
                    printf("tag[%d]:\033[33m%s\033[0m ", k,
                            g_pDbs->db[i].superTbls[j].tags[k].dataType);
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

    fprintf(fp, "host:                       %s:%u\n", g_pDbs->host, g_pDbs->port);
    fprintf(fp, "user:                       %s\n", g_pDbs->user);
    fprintf(fp, "configDir:                  %s\n", configDir);
    fprintf(fp, "resultFile:                 %s\n", g_pDbs->resultFile);
    fprintf(fp, "thread num of insert data:  %d\n", g_pDbs->threadCount);
    fprintf(fp, "thread num of create table: %d\n", g_pDbs->threadCountByCreateTbl);
    fprintf(fp, "database count:          %d\n", g_pDbs->dbCount);

    for (int i = 0; i < g_pDbs->dbCount; i++) {
        fprintf(fp, "database[%d]:\n", i);
        fprintf(fp, "  database[%d] name:       %s\n", i, g_pDbs->db[i].dbName);
        if (0 == g_pDbs->db[i].drop) {
            fprintf(fp, "  drop:                  no\n");
        }else {
            fprintf(fp, "  drop:                  yes\n");
        }

        if (g_pDbs->db[i].dbCfg.blocks > 0) {
            fprintf(fp, "  blocks:                %d\n", g_pDbs->db[i].dbCfg.blocks);
        }
        if (g_pDbs->db[i].dbCfg.cache > 0) {
            fprintf(fp, "  cache:                 %d\n", g_pDbs->db[i].dbCfg.cache);
        }
        if (g_pDbs->db[i].dbCfg.days > 0) {
            fprintf(fp, "  days:                  %d\n", g_pDbs->db[i].dbCfg.days);
        }
        if (g_pDbs->db[i].dbCfg.keep > 0) {
            fprintf(fp, "  keep:                  %d\n", g_pDbs->db[i].dbCfg.keep);
        }
        if (g_pDbs->db[i].dbCfg.replica > 0) {
            fprintf(fp, "  replica:               %d\n", g_pDbs->db[i].dbCfg.replica);
        }
        if (g_pDbs->db[i].dbCfg.update > 0) {
            fprintf(fp, "  update:                %d\n", g_pDbs->db[i].dbCfg.update);
        }
        if (g_pDbs->db[i].dbCfg.minRows > 0) {
            fprintf(fp, "  minRows:               %d\n", g_pDbs->db[i].dbCfg.minRows);
        }
        if (g_pDbs->db[i].dbCfg.maxRows > 0) {
            fprintf(fp, "  maxRows:               %d\n", g_pDbs->db[i].dbCfg.maxRows);
        }
        if (g_pDbs->db[i].dbCfg.comp > 0) {
            fprintf(fp, "  comp:                  %d\n", g_pDbs->db[i].dbCfg.comp);
        }
        if (g_pDbs->db[i].dbCfg.walLevel > 0) {
            fprintf(fp, "  walLevel:              %d\n", g_pDbs->db[i].dbCfg.walLevel);
        }
        if (g_pDbs->db[i].dbCfg.fsync > 0) {
            fprintf(fp, "  fsync:                 %d\n", g_pDbs->db[i].dbCfg.fsync);
        }
        if (g_pDbs->db[i].dbCfg.quorum > 0) {
            fprintf(fp, "  quorum:                %d\n", g_pDbs->db[i].dbCfg.quorum);
        }
        if (g_pDbs->db[i].dbCfg.precision[0] != 0) {
            if ((0 == strncasecmp(g_pDbs->db[i].dbCfg.precision, "ms", 2))
             || (0 == strncasecmp(g_pDbs->db[i].dbCfg.precision, "ns", 2))
             || (0 == strncasecmp(g_pDbs->db[i].dbCfg.precision, "us", 2))) {
                fprintf(fp, "  precision:             %s\n", g_pDbs->db[i].dbCfg.precision);
            } else {
                fprintf(fp, "  precision error:       %s\n", g_pDbs->db[i].dbCfg.precision);
            }
        }

        fprintf(fp, "  super table count:     %"PRIu64"\n",
                g_pDbs->db[i].superTblCount);
        for (int j = 0; j < g_pDbs->db[i].superTblCount; j++) {
            fprintf(fp, "  super table[%d]:\n", j);

            fprintf(fp, "      stbName:           %s\n",
                    g_pDbs->db[i].superTbls[j].sTblName);


            fprintf(fp, "      childTblCount:     %"PRId64"\n",
                    g_pDbs->db[i].superTbls[j].childTblCount);
            fprintf(fp, "      childTblPrefix:    %s\n",
                    g_pDbs->db[i].superTbls[j].childTblPrefix);
            fprintf(fp, "      insertMode:        %s\n",
                    (g_pDbs->db[i].superTbls[j].insertMode==TAOSC_RAND)?"rand":"csv");
            fprintf(fp, "      maxSqlLen:         %"PRIu64"\n",
                    g_pDbs->db[i].superTbls[j].maxSqlLen);

            fprintf(fp, "      timeStampStep:     %"PRId64"\n",
                    g_pDbs->db[i].superTbls[j].timeStampStep);
            fprintf(fp, "      startTimestamp:    %s\n",
                    g_pDbs->db[i].superTbls[j].startTimestamp);
            fprintf(fp, "      csvFile:           %s\n",
                    g_pDbs->db[i].superTbls[j].csvFile);
            fprintf(fp, "      tagsFile:          %s\n",
                    g_pDbs->db[i].superTbls[j].tagsFile);

            fprintf(fp, "      columnCount:       %d\n        ",
                    g_pDbs->db[i].superTbls[j].columnCount);
            for (int k = 0; k < g_pDbs->db[i].superTbls[j].columnCount; k++) {
                if ((0 == strncasecmp(g_pDbs->db[i].superTbls[j].columns[k].dataType, "binary", strlen("binary")))
                 || (0 == strncasecmp(g_pDbs->db[i].superTbls[j].columns[k].dataType, "nchar", strlen("nchar")))) {
                    fprintf(fp, "column[%d]:%s(%d) ", k,
                            g_pDbs->db[i].superTbls[j].columns[k].dataType,
                            g_pDbs->db[i].superTbls[j].columns[k].dataLen);
                } else {
                    fprintf(fp, "column[%d]:%s ",
                            k, g_pDbs->db[i].superTbls[j].columns[k].dataType);
                }
            }
            fprintf(fp, "\n");

            fprintf(fp, "      tagCount:            %d\n        ",
                    g_pDbs->db[i].superTbls[j].tagCount);
            for (int k = 0; k < g_pDbs->db[i].superTbls[j].tagCount; k++) {
                if ((0 == strncasecmp(g_pDbs->db[i].superTbls[j].tags[k].dataType, "binary", strlen("binary")))
                 || (0 == strncasecmp(g_pDbs->db[i].superTbls[j].tags[k].dataType, "nchar", strlen("nchar")))) {
                    fprintf(fp, "tag[%d]:%s(%d) ",
                            k, g_pDbs->db[i].superTbls[j].tags[k].dataType,
                            g_pDbs->db[i].superTbls[j].tags[k].dataLen);
                } else {
                    fprintf(fp, "tag[%d]:%s ", k, g_pDbs->db[i].superTbls[j].tags[k].dataType);
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

        dbInfos[count] = (SDbInfo *)taosMemoryCalloc(1, sizeof(SDbInfo));
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
    SDbInfo** dbInfos = (SDbInfo **)taosMemoryCalloc(MAX_DATABASE_COUNT, sizeof(SDbInfo *));
    if (dbInfos == NULL) {
        errorPrint("%s() LN%d, failed to allocate memory\n", __func__, __LINE__);
        return;
    }
    int dbCount = getDbFromServer(taos, dbInfos);
    if (dbCount <= 0) {
        taosMemoryFree(dbInfos);
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
        taosMemoryFree(dbInfos[i]);
    }

    taosMemoryFree(dbInfos);
}

static char* getTagValueFromTagSample(SSuperTable* stbInfo, int tagUsePos) {
    char*  dataBuf = (char*)taosMemoryCalloc(TSDB_MAX_SQL_LEN+1, 1);
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

static char *generateBinaryNCharTagValues(int64_t tableSeq, uint32_t len)
{
    char* buf = (char*)taosMemoryCalloc(len, 1);
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
    char*  dataBuf = (char*)taosMemoryCalloc(TSDB_MAX_SQL_LEN+1, 1);
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
            dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen, "\'%s\',", buf);
            tmfree(buf);
        } else if (0 == strncasecmp(stbInfo->tags[i].dataType, "int", strlen("int"))) {
            dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen, "%"PRId64",", tableSeq);
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
                    "%"PRId64",", rand_bigint());
        }  else {
            errorPrint("No support data type: %s\n", stbInfo->tags[i].dataType);
            tmfree(dataBuf);
            return NULL;
        }
    }

    dataLen -= 1;
    dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen, ")");
    return dataBuf;
}

static int64_t generateNoTimestampRowData(       SSuperTable* stbInfo,  char* recBuf, int64_t maxSqlLen)
{
    int64_t   dataLen = 0;
    char     *pstr = recBuf;
    int64_t   maxLen = MAX_DATA_SIZE;
    int       tmpLen;

    for (int i = 0; i < stbInfo->columnCount; i++) {
        if ((0 == strncasecmp(stbInfo->columns[i].dataType, "BINARY", 6))
         || (0 == strncasecmp(stbInfo->columns[i].dataType, "NCHAR", 5))) {
            if (stbInfo->columns[i].dataLen > TSDB_MAX_BINARY_LEN) {
                errorPrint( "binary or nchar length overflow, max size:%u\n", (uint32_t)TSDB_MAX_BINARY_LEN);
                return -1;
            }

            /* need count 3 extra chars \', \', and , */
            if ((stbInfo->columns[i].dataLen + 1) >  (maxSqlLen - dataLen - 3)) {
                return 0;
            }
            char* buf = (char*)taosMemoryCalloc(stbInfo->columns[i].dataLen+1, 1);
            if (NULL == buf) {
                errorPrint( "calloc failed! size:%d\n", stbInfo->columns[i].dataLen);
                return -1;
            }
            rand_string(buf, stbInfo->columns[i].dataLen);
            dataLen += snprintf(pstr + dataLen, maxLen - dataLen, "\'%s\',", buf);
            tmfree(buf);
        } else {
            char *tmp;
            if (0 == strncasecmp(stbInfo->columns[i].dataType, "INT", 3)) {
                tmp = rand_int_str();
                tmpLen = strlen(tmp);
                tstrncpy(pstr + dataLen, tmp, min(tmpLen + 1, INT_BUFF_LEN));
            } else if (0 == strncasecmp(stbInfo->columns[i].dataType, "BIGINT", 6)) {
                tmp = rand_bigint_str();
                tstrncpy(pstr + dataLen, tmp, BIGINT_BUFF_LEN);
            }  else if (0 == strncasecmp(stbInfo->columns[i].dataType, "FLOAT", 5)) {
                tmp = rand_float_str();
                tmpLen = strlen(tmp);
                tstrncpy(pstr + dataLen, tmp, min(tmpLen +1, FLOAT_BUFF_LEN));
            }  else if (0 == strncasecmp(stbInfo->columns[i].dataType, "DOUBLE", 6)) {
                tmp = rand_double_str();
                tmpLen = strlen(tmp);
                tstrncpy(pstr + dataLen, tmp, min(tmpLen +1, DOUBLE_BUFF_LEN));
            }  else if (0 == strncasecmp(stbInfo->columns[i].dataType, "SMALLINT", 8)) {
                tmp = rand_smallint_str();
                tmpLen = strlen(tmp);
                tstrncpy(pstr + dataLen, tmp, min(tmpLen + 1, SMALLINT_BUFF_LEN));
            }  else if (0 == strncasecmp(stbInfo->columns[i].dataType, "TINYINT", 7)) {
                tmp = rand_tinyint_str();
                tmpLen = strlen(tmp);
                tstrncpy(pstr + dataLen, tmp, min(tmpLen +1, TINYINT_BUFF_LEN));
            }  else if (0 == strncasecmp(stbInfo->columns[i].dataType, "BOOL", 4)) {
                tmp = rand_bool_str();
                tmpLen = strlen(tmp);
                tstrncpy(pstr + dataLen, tmp, min(tmpLen +1, BOOL_BUFF_LEN));
            }  else if (0 == strncasecmp(stbInfo->columns[i].dataType, "TIMESTAMP", 9)) {
                tmp = rand_int_str();
                tmpLen = strlen(tmp);
                tstrncpy(pstr + dataLen, tmp, min(tmpLen +1, INT_BUFF_LEN));
            }  else {
                errorPrint( "Not support data type: %s\n", stbInfo->columns[i].dataType);
                return -1;
            }

            dataLen += strlen(tmp);
            tstrncpy(pstr + dataLen, ",", 2);
            dataLen += 1;
        }

        if (dataLen > (maxSqlLen - (16000))) {
          printf( "data len of row over than max sql len\n");
          return -1;
        } 
    }

    dataLen -= 1;

    return strlen(recBuf);
}


int32_t setSchemalessLineTemplate(char* dbName, SSuperTable*           superTbl) {
  int   templateLen  = 65535;
  char* lineTemplate = taosMemoryCalloc(templateLen+1, sizeof(char));
  if (NULL == lineTemplate) {
    printf("calloc fail for lineTemplate\n");
    exit(-1);
  }

  superTbl->schemalessLineTemplate = lineTemplate;

  int len = 0;
  int colIndex;
  int tagIndex;
  
  len += snprintf(lineTemplate + len, templateLen - len, "%s,id=\"%s%s\",", superTbl->sTblName, superTbl->childTblPrefix, "%d");

  for (tagIndex = 0; tagIndex < superTbl->tagCount; tagIndex++) {
      char* dataType = superTbl->tags[tagIndex].dataType;
  
      if (strcasecmp(dataType, "BINARY") == 0) {
          len += snprintf(lineTemplate + len, TSDB_MAX_TAGS_LEN - len, "t%d=\"%.*s\",", tagIndex, superTbl->tags[tagIndex].dataLen, "BINARY-tagvalue");
      } else if (strcasecmp(dataType, "NCHAR") == 0) {
          len += snprintf(lineTemplate + len, TSDB_MAX_TAGS_LEN - len, "t%d=\"%.*s\",", tagIndex, superTbl->tags[tagIndex].dataLen, "NCHAR-tagvalue");
      } else if (strcasecmp(dataType, "INT") == 0)  {
          len += snprintf(lineTemplate + len, TSDB_MAX_TAGS_LEN - len, "t%d=32i32,", tagIndex);
      } else if (strcasecmp(dataType, "BIGINT") == 0)  {
          len += snprintf(lineTemplate + len, TSDB_MAX_TAGS_LEN - len, "t%d=64i32,", tagIndex);
      } else if (strcasecmp(dataType, "SMALLINT") == 0)  {
          len += snprintf(lineTemplate + len, TSDB_MAX_TAGS_LEN - len, "t%d=16i32,", tagIndex);
      } else if (strcasecmp(dataType, "TINYINT") == 0)  {
          len += snprintf(lineTemplate + len, TSDB_MAX_TAGS_LEN - len, "t%d=8i32,", tagIndex);
      } else if (strcasecmp(dataType, "BOOL") == 0)  {
          len += snprintf(lineTemplate + len, TSDB_MAX_TAGS_LEN - len, "t%d=1b,", tagIndex);
      } else if (strcasecmp(dataType, "FLOAT") == 0) {
          len += snprintf(lineTemplate + len, TSDB_MAX_TAGS_LEN - len, "t%d=32.32f32,", tagIndex);
      } else if (strcasecmp(dataType, "DOUBLE") == 0) {
          len += snprintf(lineTemplate + len, TSDB_MAX_TAGS_LEN - len, "t%d=64.64f64,", tagIndex);
      } else {
          errorPrint("%s() LN%d, config error tag type : %s\n", __func__, __LINE__, dataType);
          exit(EXIT_FAILURE);
      }
  }

  len = len - 1 ;
  len += snprintf(lineTemplate + len, TSDB_MAX_TAGS_LEN - len, " ");

  
  for (colIndex = 0; colIndex < superTbl->columnCount; colIndex++) {
      char* dataType = superTbl->columns[colIndex].dataType;
  
      if (strcasecmp(dataType, "BINARY") == 0) {
          len += snprintf(lineTemplate + len, TSDB_MAX_TAGS_LEN - len, "c%d=\"%.*s\",", colIndex, superTbl->columns[colIndex].dataLen, "BINARY-tagvalue");
      } else if (strcasecmp(dataType, "NCHAR") == 0) {
          len += snprintf(lineTemplate + len, TSDB_MAX_TAGS_LEN - len, "c%d=\"%.*s\",", colIndex, superTbl->columns[colIndex].dataLen, "NCHAR-tagvalue");
      } else if (strcasecmp(dataType, "INT") == 0)  {
          len += snprintf(lineTemplate + len, TSDB_MAX_TAGS_LEN - len, "c%d=32i32,", colIndex);
      } else if (strcasecmp(dataType, "BIGINT") == 0)  {
          len += snprintf(lineTemplate + len, TSDB_MAX_TAGS_LEN - len, "c%d=64i32,", colIndex);
      } else if (strcasecmp(dataType, "SMALLINT") == 0)  {
          len += snprintf(lineTemplate + len, TSDB_MAX_TAGS_LEN - len, "c%d=16i32,", colIndex);
      } else if (strcasecmp(dataType, "TINYINT") == 0)  {
          len += snprintf(lineTemplate + len, TSDB_MAX_TAGS_LEN - len, "c%d=8i32,", colIndex);
      } else if (strcasecmp(dataType, "BOOL") == 0)  {
          len += snprintf(lineTemplate + len, TSDB_MAX_TAGS_LEN - len, "c%d=1b,", colIndex);
      } else if (strcasecmp(dataType, "FLOAT") == 0) {
          len += snprintf(lineTemplate + len, TSDB_MAX_TAGS_LEN - len, "c%d=32.32f32,", colIndex);
      } else if (strcasecmp(dataType, "DOUBLE") == 0) {
          len += snprintf(lineTemplate + len, TSDB_MAX_TAGS_LEN - len, "c%d=64.64f64,", colIndex);
      } else {
          errorPrint("%s() LN%d, config error column type : %s\n", __func__, __LINE__, dataType);
          exit(EXIT_FAILURE);
      }
  }

  len = len - 1 ;
  len += snprintf(lineTemplate + len, TSDB_MAX_TAGS_LEN - len, " ");

  len += snprintf(lineTemplate + len, TSDB_MAX_TAGS_LEN - len, "%s", "%lldms");

  return 0;
}

static int createSuperTable(      TAOS * taos, char* dbName, SSuperTable*  superTbl) {

    char *command = taosMemoryCalloc(1, BUFFER_SIZE);
    assert(command);

    char cols[COL_BUFFER_LEN] = "\0";
    int colIndex;
    int len = 0;

    //int  lenOfOneRow = 0;

    if (superTbl->columnCount == 0) {
        errorPrint("%s() LN%d, super table column count is %d\n",
                __func__, __LINE__, superTbl->columnCount);
        taosMemoryFree(command);
        return -1;
    }

    uint64_t maxSqlLen = superTbl->maxSqlLen;
    superTbl->randDataBuf = taosMemoryCalloc(maxSqlLen, 1);
    if (NULL == superTbl->randDataBuf) {
        errorPrint( "Failed to alloc %"PRIu64" Bytes, reason:%s\n", maxSqlLen, strerror(errno));
        return -1;
    }

    superTbl->randDataLen = generateNoTimestampRowData(superTbl, superTbl->randDataBuf, maxSqlLen);

    for (colIndex = 0; colIndex < superTbl->columnCount; colIndex++) {
        char* dataType = superTbl->columns[colIndex].dataType;

        if (strcasecmp(dataType, "BINARY") == 0) {
            len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s(%d)", colIndex, "BINARY", superTbl->columns[colIndex].dataLen);
            //lenOfOneRow += superTbl->columns[colIndex].dataLen + 3;
        } else if (strcasecmp(dataType, "NCHAR") == 0) {
            len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s(%d)", colIndex, "NCHAR", superTbl->columns[colIndex].dataLen);
            //lenOfOneRow += superTbl->columns[colIndex].dataLen + 3;
        } else if (strcasecmp(dataType, "INT") == 0)  {
            len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s", colIndex, "INT");
            //lenOfOneRow += INT_BUFF_LEN;
        } else if (strcasecmp(dataType, "BIGINT") == 0)  {
            len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s",
                    colIndex, "BIGINT");
            //lenOfOneRow += BIGINT_BUFF_LEN;
        } else if (strcasecmp(dataType, "SMALLINT") == 0)  {
            len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s",
                    colIndex, "SMALLINT");
            //lenOfOneRow += SMALLINT_BUFF_LEN;
        } else if (strcasecmp(dataType, "TINYINT") == 0)  {
            len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s", colIndex, "TINYINT");
            //lenOfOneRow += TINYINT_BUFF_LEN;
        } else if (strcasecmp(dataType, "BOOL") == 0)  {
            len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s", colIndex, "BOOL");
            //lenOfOneRow += BOOL_BUFF_LEN;
        } else if (strcasecmp(dataType, "FLOAT") == 0) {
            len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s", colIndex, "FLOAT");
            //lenOfOneRow += FLOAT_BUFF_LEN;
        } else if (strcasecmp(dataType, "DOUBLE") == 0) {
            len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s",
                    colIndex, "DOUBLE");
            //lenOfOneRow += DOUBLE_BUFF_LEN;
        }  else if (strcasecmp(dataType, "TIMESTAMP") == 0) {
            len += snprintf(cols + len, COL_BUFFER_LEN - len, ",C%d %s",
                    colIndex, "TIMESTAMP");
            //lenOfOneRow += TIMESTAMP_BUFF_LEN;
        } else {
            taos_close(taos);
            taosMemoryFree(command);
            errorPrint("%s() LN%d, config error data type : %s\n",
                    __func__, __LINE__, dataType);
            exit(EXIT_FAILURE);
        }
    }

    //superTbl->lenOfOneRow = lenOfOneRow + 20; // timestamp

    // save for creating child table
    superTbl->colsOfCreateChildTable = (char*)taosMemoryCalloc(len+20, 1);
    if (NULL == superTbl->colsOfCreateChildTable) {
        taos_close(taos);
        taosMemoryFree(command);
        errorPrint("%s() LN%d, Failed when calloc, size:%d", __func__, __LINE__, len+1);
        exit(EXIT_FAILURE);
    }

    snprintf(superTbl->colsOfCreateChildTable, len+20, "(ts timestamp%s)", cols);
    verbosePrint("%s() LN%d: %s\n", __func__, __LINE__, superTbl->colsOfCreateChildTable);

    if (superTbl->tagCount == 0) {
        errorPrint("%s() LN%d, super table tag count is %d\n", __func__, __LINE__, superTbl->tagCount);
        taosMemoryFree(command);
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
            len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "T%d %s(%d),", tagIndex, "BINARY", superTbl->tags[tagIndex].dataLen);
            lenOfTagOfOneRow += superTbl->tags[tagIndex].dataLen + 3;
        } else if (strcasecmp(dataType, "NCHAR") == 0) {
            len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "T%d %s(%d),", tagIndex, "NCHAR", superTbl->tags[tagIndex].dataLen);
            lenOfTagOfOneRow += superTbl->tags[tagIndex].dataLen + 3;
        } else if (strcasecmp(dataType, "INT") == 0)  {
            len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, "T%d %s,", tagIndex, "INT");
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
        } else {
            taos_close(taos);
            taosMemoryFree(command);
            errorPrint("%s() LN%d, config error tag type : %s\n",
                    __func__, __LINE__, dataType);
            exit(EXIT_FAILURE);
        }
    }

    len -= 1;
    len += snprintf(tags + len, TSDB_MAX_TAGS_LEN - len, ")");

    superTbl->lenOfTagOfOneRow = lenOfTagOfOneRow;

    snprintf(command, BUFFER_SIZE,
            "create table if not exists %s.%s (ts timestamp%s) tags %s",
            dbName, superTbl->sTblName, cols, tags);
    if (0 != queryDbExec(taos, command, NO_INSERT_TYPE, false)) {
        errorPrint( "create supertable %s failed!\n\n",
                superTbl->sTblName);
        taosMemoryFree(command);
        return -1;
    }

    debugPrint("create supertable %s success!\n\n", superTbl->sTblName);
    taosMemoryFree(command);
    return 0;
}

int createDatabasesAndStables(char *command) {
    TAOS * taos = NULL;
    int    ret = 0;
    taos = taos_connect(g_pDbs->host, g_pDbs->user, g_pDbs->password, NULL, g_pDbs->port);
    if (taos == NULL) {
        errorPrint( "Failed to connect to TDengine, reason:%s\n", taos_errstr(NULL));
        return -1;
    }

    for (int i = 0; i < g_pDbs->dbCount; i++) {
        if (g_pDbs->db[i].drop) {
            sprintf(command, "drop database if exists %s;", g_pDbs->db[i].dbName);
            if (0 != queryDbExec(taos, command, NO_INSERT_TYPE, false)) {
                taos_close(taos);
                return -1;
            }

            int dataLen = 0;
            dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen, "create database if not exists %s", g_pDbs->db[i].dbName);

            if (g_pDbs->db[i].dbCfg.blocks > 0) {
                dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen, " blocks %d", g_pDbs->db[i].dbCfg.blocks);
            }
            if (g_pDbs->db[i].dbCfg.cache > 0) {
                dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen, " cache %d", g_pDbs->db[i].dbCfg.cache);
            }
            if (g_pDbs->db[i].dbCfg.days > 0) {
                dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen, " days %d", g_pDbs->db[i].dbCfg.days);
            }
            if (g_pDbs->db[i].dbCfg.keep > 0) {
                dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen, " keep %d", g_pDbs->db[i].dbCfg.keep);
            }
            if (g_pDbs->db[i].dbCfg.quorum > 1) {
                dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen, " quorum %d", g_pDbs->db[i].dbCfg.quorum);
            }
            if (g_pDbs->db[i].dbCfg.replica > 0) {
                dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen, " replica %d", g_pDbs->db[i].dbCfg.replica);
            }
            if (g_pDbs->db[i].dbCfg.update > 0) {
                dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen, " update %d",g_pDbs->db[i].dbCfg.update);
            }
            //if (g_pDbs->db[i].dbCfg.maxtablesPerVnode > 0) {
            //  dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen, "tables %d ", g_pDbs->db[i].dbCfg.maxtablesPerVnode);
            //}
            if (g_pDbs->db[i].dbCfg.minRows > 0) {
                dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen, " minrows %d", g_pDbs->db[i].dbCfg.minRows);
            }
            if (g_pDbs->db[i].dbCfg.maxRows > 0) {
                dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen, " maxrows %d", g_pDbs->db[i].dbCfg.maxRows);
            }
            if (g_pDbs->db[i].dbCfg.comp > 0) {
                dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen, " comp %d", g_pDbs->db[i].dbCfg.comp);
            }
            if (g_pDbs->db[i].dbCfg.walLevel > 0) {
                dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen, " wal %d", g_pDbs->db[i].dbCfg.walLevel);
            }
            if (g_pDbs->db[i].dbCfg.cacheLast > 0) {
                dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen, " cachelast %d", g_pDbs->db[i].dbCfg.cacheLast);
            }
            if (g_pDbs->db[i].dbCfg.fsync > 0) {
                dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen, " fsync %d", g_pDbs->db[i].dbCfg.fsync);
            }
            if ((0 == strncasecmp(g_pDbs->db[i].dbCfg.precision, "ms", 2))
             || (0 == strncasecmp(g_pDbs->db[i].dbCfg.precision, "ns", 2))
             || (0 == strncasecmp(g_pDbs->db[i].dbCfg.precision, "us", 2))) {
                dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen, " precision \'%s\';", g_pDbs->db[i].dbCfg.precision);
            }

            if (0 != queryDbExec(taos, command, NO_INSERT_TYPE, false)) {
                taos_close(taos);
                errorPrint( "\ncreate database %s failed!\n\n", g_pDbs->db[i].dbName);
                return -1;
            }
            printf("\ncreate database %s success!\n\n", g_pDbs->db[i].dbName);
        }

        debugPrint("%s() LN%d supertbl count:%"PRIu64"\n", __func__, __LINE__, g_pDbs->db[i].superTblCount);

        for (uint64_t j = 0; j < g_pDbs->db[i].superTblCount; j++) {
            ret = createSuperTable(taos, g_pDbs->db[i].dbName, &g_pDbs->db[i].superTbls[j]);
            if (0 != ret) {
                errorPrint("create super table %"PRIu64" failed!\n\n", j);
                continue;
            }

            if (TAOSC_SCHEMALESS == g_pDbs->db[i].superTbls[j].insertMode) {
              setSchemalessLineTemplate(g_pDbs->db[i].dbName, &g_pDbs->db[i].superTbls[j]);
            }

            //ret = getSuperTableFromServer(taos, g_pDbs->db[i].dbName, &g_pDbs->db[i].superTbls[j]);
            //if (0 != ret) {
            //    errorPrint("\nget super table %s.%s info failed!\n\n",
            //            g_pDbs->db[i].dbName, g_pDbs->db[i].superTbls[j].sTblName);
            //    continue;
            //}

            //validStbCount ++;
        }

        //g_pDbs->db[i].superTblCount = validStbCount;
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

    pThreadInfo->buffer = taosMemoryCalloc(buff_len, 1);
    if (pThreadInfo->buffer == NULL) {
        errorPrint("%s() LN%d, Memory allocated failed!\n", __func__, __LINE__);
        return NULL;
    }

    int len = 0;
    int batchNum = 0;

    verbosePrint("%s() LN%d: Creating table from %"PRIu64" to %"PRIu64"\n",
            __func__, __LINE__,
            pThreadInfo->start_table_from, pThreadInfo->end_table_to);

    for (uint64_t i = pThreadInfo->start_table_from; i <= pThreadInfo->end_table_to; i++) {
      if (stbInfo == NULL) {
          taosMemoryFree(pThreadInfo->buffer);
          errorPrint("%s() LN%d, use metric, but super table info is NULL\n", __func__, __LINE__);
          return NULL;
      }
      
      if (0 == len) {
          batchNum = 0;
          memset(pThreadInfo->buffer, 0, buff_len);
          len += snprintf(pThreadInfo->buffer + len, buff_len - len, "create table ");
      }
  
      char* tagsValBuf = NULL;
      if (0 == stbInfo->tagSource) {
          tagsValBuf = generateTagValuesForStb(stbInfo, i);
      } else {
          if (0 == stbInfo->tagSampleCount) {
              taosMemoryFree(pThreadInfo->buffer);
              ERROR_EXIT("use sample file for tag, but has no content!\n");
          }
          tagsValBuf = getTagValueFromTagSample(
                  stbInfo,
                  i % stbInfo->tagSampleCount);
      }
  
      if (NULL == tagsValBuf) {
          taosMemoryFree(pThreadInfo->buffer);
          ERROR_EXIT("use metric, but tag buffer is NULL\n");
      }
      len += snprintf(pThreadInfo->buffer + len,
              buff_len - len,
              "if not exists %s.%s%"PRIu64" using %s.%s tags %s ",
              pThreadInfo->db_name, stbInfo->childTblPrefix,
              i, pThreadInfo->db_name,
              stbInfo->sTblName, tagsValBuf);
      taosMemoryFree(tagsValBuf);
      batchNum++;
      if ((batchNum < stbInfo->batchCreateTableNum)
       && ((buff_len - len) >= (stbInfo->lenOfTagOfOneRow + 256))) {
          continue;
      }

      len = 0;
      if (0 != queryDbExec(pThreadInfo->taos, pThreadInfo->buffer,
                  NO_INSERT_TYPE, false)){
          errorPrint( "queryDbExec() failed. buffer:\n%s\n", pThreadInfo->buffer);
          taosMemoryFree(pThreadInfo->buffer);
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

    taosMemoryFree(pThreadInfo->buffer);
    return NULL;
}

static int startMultiThreadCreateChildTable(
        char* cols, int threads, uint64_t tableFrom, int64_t ntables,
        char* db_name, SSuperTable* stbInfo) {

    TdThread  *pids = taosMemoryCalloc(1, threads * sizeof(TdThread));
    threadInfo *infos = taosMemoryCalloc(1, threads * sizeof(threadInfo));

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
                g_pDbs->host,
                g_pDbs->user,
                g_pDbs->password,
                db_name,
                g_pDbs->port);
        if (pThreadInfo->taos == NULL) {
            errorPrint( "%s() LN%d, Failed to connect to TDengine, reason:%s\n",
                    __func__, __LINE__, taos_errstr(NULL));
            taosMemoryFree(pids);
            taosMemoryFree(infos);
            return -1;
        }

        pThreadInfo->start_table_from = tableFrom;
        pThreadInfo->ntables = i<b?a+1:a;
        pThreadInfo->end_table_to = i < b ? tableFrom + a : tableFrom + a - 1;
        tableFrom = pThreadInfo->end_table_to + 1;
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

    taosMemoryFree(pids);
    taosMemoryFree(infos);

    return 0;
}

static void createChildTables() {
  for (int i = 0; i < g_pDbs->dbCount; i++) {
    if (g_pDbs->db[i].superTblCount > 0) {
      for (int j = 0; j < g_pDbs->db[i].superTblCount; j++) {

        if (TAOSC_SCHEMALESS == g_pDbs->db[i].superTbls[j].insertMode) {      
          continue;
        }
        
        uint64_t startFrom = 0;
        g_totalChildTables += g_pDbs->db[i].superTbls[j].childTblCount;
      
        startMultiThreadCreateChildTable(
                g_pDbs->db[i].superTbls[j].colsOfCreateChildTable,
                g_pDbs->threadCountByCreateTbl,
                startFrom,
                g_pDbs->db[i].superTbls[j].childTblCount,
                g_pDbs->db[i].dbName, &(g_pDbs->db[i].superTbls[j]));
      }
    }
  }
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
            tstrncpy(superTbls->columns[index].dataType,
                    columnCase.dataType,
                    min(DATATYPE_BUFF_LEN, strlen(columnCase.dataType) + 1));
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
        tstrncpy(columnCase.dataType, dataType->valuestring,
                min(DATATYPE_BUFF_LEN, strlen(dataType->valuestring) + 1));

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
                    min(DATATYPE_BUFF_LEN, strlen(columnCase.dataType) + 1));
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
        tstrncpy(g_pDbs->cfgDir, cfgdir->valuestring, MAX_FILE_NAME_LEN);
    }

    cJSON* host = cJSON_GetObjectItem(root, "host");
    if (host && host->type == cJSON_String && host->valuestring != NULL) {
        tstrncpy(g_pDbs->host, host->valuestring, MAX_HOSTNAME_SIZE);
    } else if (!host) {
        tstrncpy(g_pDbs->host, "127.0.0.1", MAX_HOSTNAME_SIZE);
    } else {
        printf("ERROR: failed to read json, host not found\n");
        goto PARSE_OVER;
    }

    cJSON* port = cJSON_GetObjectItem(root, "port");
    if (port && port->type == cJSON_Number) {
        g_pDbs->port = port->valueint;
    } else if (!port) {
        g_pDbs->port = 6030;
    }

    cJSON* user = cJSON_GetObjectItem(root, "user");
    if (user && user->type == cJSON_String && user->valuestring != NULL) {
        tstrncpy(g_pDbs->user, user->valuestring, MAX_USERNAME_SIZE);
    } else if (!user) {
        tstrncpy(g_pDbs->user, "root", MAX_USERNAME_SIZE);
    }

    cJSON* password = cJSON_GetObjectItem(root, "password");
    if (password && password->type == cJSON_String && password->valuestring != NULL) {
        tstrncpy(g_pDbs->password, password->valuestring, MAX_PASSWORD_SIZE);
    } else if (!password) {
        tstrncpy(g_pDbs->password, "taosdata", MAX_PASSWORD_SIZE);
    }

    cJSON* resultfile = cJSON_GetObjectItem(root, "result_file");
    if (resultfile && resultfile->type == cJSON_String && resultfile->valuestring != NULL) {
        tstrncpy(g_pDbs->resultFile, resultfile->valuestring, MAX_FILE_NAME_LEN);
    } else if (!resultfile) {
        tstrncpy(g_pDbs->resultFile, "./insert_res.txt", MAX_FILE_NAME_LEN);
    }

    cJSON* threads = cJSON_GetObjectItem(root, "thread_count");
    if (threads && threads->type == cJSON_Number) {
        g_pDbs->threadCount = threads->valueint;
    } else if (!threads) {
        g_pDbs->threadCount = 1;
    } else {
        printf("ERROR: failed to read json, threads not found\n");
        goto PARSE_OVER;
    }

    cJSON* threads2 = cJSON_GetObjectItem(root, "thread_count_create_tbl");
    if (threads2 && threads2->type == cJSON_Number) {
        g_pDbs->threadCountByCreateTbl = threads2->valueint;
    } else if (!threads2) {
        g_pDbs->threadCountByCreateTbl = 1;
    } else {
        errorPrint("%s() LN%d, failed to read json, threads2 not found\n",
                __func__, __LINE__);
        goto PARSE_OVER;
    }
/*
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
*/
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

    g_pDbs->dbCount = dbSize;
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
        tstrncpy(g_pDbs->db[i].dbName, dbName->valuestring, TSDB_DB_NAME_LEN);

        cJSON *drop = cJSON_GetObjectItem(dbinfo, "drop");
        if (drop && drop->type == cJSON_String && drop->valuestring != NULL) {
            if (0 == strncasecmp(drop->valuestring, "yes", strlen("yes"))) {
                g_pDbs->db[i].drop = true;
            } else {
                g_pDbs->db[i].drop = false;
            }
        } else if (!drop) {
            g_pDbs->db[i].drop = 1;
        } else {
            errorPrint("%s() LN%d, failed to read json, drop input mistake\n",
                    __func__, __LINE__);
            goto PARSE_OVER;
        }

        cJSON *precision = cJSON_GetObjectItem(dbinfo, "precision");
        if (precision && precision->type == cJSON_String
                && precision->valuestring != NULL) {
            tstrncpy(g_pDbs->db[i].dbCfg.precision, precision->valuestring,
                    SMALL_BUFF_LEN);
        } else if (!precision) {
            memset(g_pDbs->db[i].dbCfg.precision, 0, SMALL_BUFF_LEN);
        } else {
            printf("ERROR: failed to read json, precision not found\n");
            goto PARSE_OVER;
        }

        cJSON* update = cJSON_GetObjectItem(dbinfo, "update");
        if (update && update->type == cJSON_Number) {
            g_pDbs->db[i].dbCfg.update = update->valueint;
        } else if (!update) {
            g_pDbs->db[i].dbCfg.update = -1;
        } else {
            printf("ERROR: failed to read json, update not found\n");
            goto PARSE_OVER;
        }

        cJSON* replica = cJSON_GetObjectItem(dbinfo, "replica");
        if (replica && replica->type == cJSON_Number) {
            g_pDbs->db[i].dbCfg.replica = replica->valueint;
        } else if (!replica) {
            g_pDbs->db[i].dbCfg.replica = -1;
        } else {
            printf("ERROR: failed to read json, replica not found\n");
            goto PARSE_OVER;
        }

        cJSON* keep = cJSON_GetObjectItem(dbinfo, "keep");
        if (keep && keep->type == cJSON_Number) {
            g_pDbs->db[i].dbCfg.keep = keep->valueint;
        } else if (!keep) {
            g_pDbs->db[i].dbCfg.keep = -1;
        } else {
            printf("ERROR: failed to read json, keep not found\n");
            goto PARSE_OVER;
        }

        cJSON* days = cJSON_GetObjectItem(dbinfo, "days");
        if (days && days->type == cJSON_Number) {
            g_pDbs->db[i].dbCfg.days = days->valueint;
        } else if (!days) {
            g_pDbs->db[i].dbCfg.days = -1;
        } else {
            printf("ERROR: failed to read json, days not found\n");
            goto PARSE_OVER;
        }

        cJSON* cache = cJSON_GetObjectItem(dbinfo, "cache");
        if (cache && cache->type == cJSON_Number) {
            g_pDbs->db[i].dbCfg.cache = cache->valueint;
        } else if (!cache) {
            g_pDbs->db[i].dbCfg.cache = -1;
        } else {
            printf("ERROR: failed to read json, cache not found\n");
            goto PARSE_OVER;
        }

        cJSON* blocks= cJSON_GetObjectItem(dbinfo, "blocks");
        if (blocks && blocks->type == cJSON_Number) {
            g_pDbs->db[i].dbCfg.blocks = blocks->valueint;
        } else if (!blocks) {
            g_pDbs->db[i].dbCfg.blocks = -1;
        } else {
            printf("ERROR: failed to read json, block not found\n");
            goto PARSE_OVER;
        }

        cJSON* minRows= cJSON_GetObjectItem(dbinfo, "minRows");
        if (minRows && minRows->type == cJSON_Number) {
            g_pDbs->db[i].dbCfg.minRows = minRows->valueint;
        } else if (!minRows) {
            g_pDbs->db[i].dbCfg.minRows = 0;    // 0 means default
        } else {
            printf("ERROR: failed to read json, minRows not found\n");
            goto PARSE_OVER;
        }

        cJSON* maxRows= cJSON_GetObjectItem(dbinfo, "maxRows");
        if (maxRows && maxRows->type == cJSON_Number) {
            g_pDbs->db[i].dbCfg.maxRows = maxRows->valueint;
        } else if (!maxRows) {
            g_pDbs->db[i].dbCfg.maxRows = 0;    // 0 means default
        } else {
            printf("ERROR: failed to read json, maxRows not found\n");
            goto PARSE_OVER;
        }

        cJSON* comp= cJSON_GetObjectItem(dbinfo, "comp");
        if (comp && comp->type == cJSON_Number) {
            g_pDbs->db[i].dbCfg.comp = comp->valueint;
        } else if (!comp) {
            g_pDbs->db[i].dbCfg.comp = -1;
        } else {
            printf("ERROR: failed to read json, comp not found\n");
            goto PARSE_OVER;
        }

        cJSON* walLevel= cJSON_GetObjectItem(dbinfo, "walLevel");
        if (walLevel && walLevel->type == cJSON_Number) {
            g_pDbs->db[i].dbCfg.walLevel = walLevel->valueint;
        } else if (!walLevel) {
            g_pDbs->db[i].dbCfg.walLevel = -1;
        } else {
            printf("ERROR: failed to read json, walLevel not found\n");
            goto PARSE_OVER;
        }

        cJSON* cacheLast= cJSON_GetObjectItem(dbinfo, "cachelast");
        if (cacheLast && cacheLast->type == cJSON_Number) {
            g_pDbs->db[i].dbCfg.cacheLast = cacheLast->valueint;
        } else if (!cacheLast) {
            g_pDbs->db[i].dbCfg.cacheLast = -1;
        } else {
            printf("ERROR: failed to read json, cacheLast not found\n");
            goto PARSE_OVER;
        }

        cJSON* quorum= cJSON_GetObjectItem(dbinfo, "quorum");
        if (quorum && quorum->type == cJSON_Number) {
            g_pDbs->db[i].dbCfg.quorum = quorum->valueint;
        } else if (!quorum) {
            g_pDbs->db[i].dbCfg.quorum = 1;
        } else {
            printf("failed to read json, quorum input mistake");
            goto PARSE_OVER;
        }

        cJSON* fsync= cJSON_GetObjectItem(dbinfo, "fsync");
        if (fsync && fsync->type == cJSON_Number) {
            g_pDbs->db[i].dbCfg.fsync = fsync->valueint;
        } else if (!fsync) {
            g_pDbs->db[i].dbCfg.fsync = -1;
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

        g_pDbs->db[i].superTblCount = stbSize;
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
            tstrncpy(g_pDbs->db[i].superTbls[j].sTblName, stbName->valuestring,
                    TSDB_TABLE_NAME_LEN);

            cJSON *prefix = cJSON_GetObjectItem(stbInfo, "childtable_prefix");
            if (!prefix || prefix->type != cJSON_String || prefix->valuestring == NULL) {
                printf("ERROR: failed to read json, childtable_prefix not found\n");
                goto PARSE_OVER;
            }
            tstrncpy(g_pDbs->db[i].superTbls[j].childTblPrefix, prefix->valuestring,
                    TBNAME_PREFIX_LEN);

            cJSON* batchCreateTbl = cJSON_GetObjectItem(stbInfo, "batch_create_tbl_num");
            if (batchCreateTbl && batchCreateTbl->type == cJSON_Number) {
                g_pDbs->db[i].superTbls[j].batchCreateTableNum = batchCreateTbl->valueint;
            } else if (!batchCreateTbl) {
                g_pDbs->db[i].superTbls[j].batchCreateTableNum = 1000;
            } else {
                printf("ERROR: failed to read json, batch_create_tbl_num not found\n");
                goto PARSE_OVER;
            }
            
            cJSON* batchRow = cJSON_GetObjectItem(stbInfo, "batch_rows");
            if (batchRow && batchRow->type == cJSON_Number) {
                g_pDbs->db[i].superTbls[j].batchRows = batchRow->valueint;
            } else if (!batchRow) {
                g_pDbs->db[i].superTbls[j].batchRows = 32766;
            } else {
                printf("ERROR: failed to read json, batch_rows not found\n");
                goto PARSE_OVER;
            }
            
            cJSON* insertRows = cJSON_GetObjectItem(stbInfo, "insert_rows");
            if (insertRows && insertRows->type == cJSON_Number) {
                g_pDbs->db[i].superTbls[j].insertRows = insertRows->valueint;
            } else if (!insertRows) {
                g_pDbs->db[i].superTbls[j].insertRows = 0;
            } else {
                printf("ERROR: failed to read json, insert_rows not found\n");
                goto PARSE_OVER;
            }

            cJSON* count = cJSON_GetObjectItem(stbInfo, "childtable_count");
            if (!count || count->type != cJSON_Number || 0 >= count->valueint) {
                errorPrint("%s() LN%d, failed to read json, childtable_count input mistake\n",
                        __func__, __LINE__);
                goto PARSE_OVER;
            }
            g_pDbs->db[i].superTbls[j].childTblCount = count->valueint;

            cJSON *insertMode = cJSON_GetObjectItem(stbInfo, "insert_mode"); // rand(rand generate), csv(csv file), schemaless(rand once)
            if (insertMode && insertMode->type == cJSON_String && insertMode->valuestring != NULL) {
                if (0 == strcasecmp(insertMode->valuestring, "rand")) {
                    g_pDbs->db[i].superTbls[j].insertMode= TAOSC_RAND;
                } else if (0 == strcasecmp(insertMode->valuestring, "csv")) {
                    g_pDbs->db[i].superTbls[j].insertMode= TAOSC_CSV;
                }  else if (0 == strcasecmp(insertMode->valuestring, "schemaless")) {
                    g_pDbs->db[i].superTbls[j].insertMode= TAOSC_SCHEMALESS;
                } else {
                    errorPrint("%s() LN%d, failed to read json, insert_mode %s not recognized\n",
                            __func__, __LINE__, insertMode->valuestring);
                    goto PARSE_OVER;
                }
            } else if (!insertMode) {
                g_pDbs->db[i].superTbls[j].insertMode = TAOSC_RAND;
            } else {
                errorPrint("%s", "failed to read json, insert_mode not found\n");
                goto PARSE_OVER;
            }

            cJSON *ts = cJSON_GetObjectItem(stbInfo, "start_timestamp");
            if (ts && ts->type == cJSON_String && ts->valuestring != NULL) {
                tstrncpy(g_pDbs->db[i].superTbls[j].startTimestamp,
                        ts->valuestring, TSDB_DB_NAME_LEN);
            } else if (!ts) {
                tstrncpy(g_pDbs->db[i].superTbls[j].startTimestamp,
                        "now", TSDB_DB_NAME_LEN);
            } else {
                printf("ERROR: failed to read json, start_timestamp not found\n");
                goto PARSE_OVER;
            }

            cJSON* timestampStep = cJSON_GetObjectItem(stbInfo, "timestamp_step");
            if (timestampStep && timestampStep->type == cJSON_Number) {
                g_pDbs->db[i].superTbls[j].timeStampStep = timestampStep->valueint;
            } else if (!timestampStep) {
                g_pDbs->db[i].superTbls[j].timeStampStep = 1;
            } else {
                printf("ERROR: failed to read json, timestamp_step not found\n");
                goto PARSE_OVER;
            }

            cJSON *csvFile = cJSON_GetObjectItem(stbInfo, "csv_file");
            if (csvFile && csvFile->type == cJSON_String
                    && csvFile->valuestring != NULL) {
                tstrncpy(g_pDbs->db[i].superTbls[j].csvFile,
                        csvFile->valuestring,
                        min(MAX_FILE_NAME_LEN,
                            strlen(csvFile->valuestring) + 1));
            } else if (!csvFile) {
                memset(g_pDbs->db[i].superTbls[j].csvFile, 0, MAX_FILE_NAME_LEN);
            } else {
                printf("ERROR: failed to read json, csv_file not found\n");
                goto PARSE_OVER;
            }

            cJSON *tagsFile = cJSON_GetObjectItem(stbInfo, "tags_file");
            if ((tagsFile && tagsFile->type == cJSON_String)
                    && (tagsFile->valuestring != NULL)) {
                tstrncpy(g_pDbs->db[i].superTbls[j].tagsFile,
                        tagsFile->valuestring, MAX_FILE_NAME_LEN);
                if (0 == g_pDbs->db[i].superTbls[j].tagsFile[0]) {
                    g_pDbs->db[i].superTbls[j].tagSource = 0;
                } else {
                    g_pDbs->db[i].superTbls[j].tagSource = 1;
                }
            } else if (!tagsFile) {
                memset(g_pDbs->db[i].superTbls[j].tagsFile, 0, MAX_FILE_NAME_LEN);
                g_pDbs->db[i].superTbls[j].tagSource = 0;
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
                g_pDbs->db[i].superTbls[j].maxSqlLen = len;
            } else if (!stbMaxSqlLen) {
                g_pDbs->db[i].superTbls[j].maxSqlLen = 64000;
            } else {
                errorPrint("%s() LN%d, failed to read json, stbMaxSqlLen input mistake\n",
                        __func__, __LINE__);
                goto PARSE_OVER;
            }

            int retVal = getColumnAndTagTypeFromInsertJsonFile(
                    stbInfo, &g_pDbs->db[i].superTbls[j]);
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
/*
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
*/    
/*
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
*/
    cJSON* dbs = cJSON_GetObjectItem(root, "databases");
    if (dbs && dbs->type == cJSON_String && dbs->valuestring != NULL) {
        tstrncpy(g_queryInfo.dbName, dbs->valuestring, TSDB_DB_NAME_LEN);
    } else if (!dbs) {
        printf("ERROR: failed to read json, databases not found\n");
        goto PARSE_OVER;
    }
/*
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
        printf("ERROR: failed to read json, query_mode not found\n");
        goto PARSE_OVER;
    }
*/
    // specified_table_query
    cJSON *specifiedQuery = cJSON_GetObjectItem(root, "specified_table_query");
    if (!specifiedQuery) {
        g_queryInfo.specifiedQueryInfo.concurrent = 1;
        g_queryInfo.specifiedQueryInfo.sqlCount = 0;
    } else if (specifiedQuery->type != cJSON_Object) {
        printf("ERROR: failed to read json, super_table_query not found\n");
        goto PARSE_OVER;
    } else {
        cJSON* specifiedQueryTimes = cJSON_GetObjectItem(specifiedQuery, "query_times");
        if (specifiedQueryTimes && specifiedQueryTimes->type == cJSON_Number) {
            if (specifiedQueryTimes->valueint <= 0) {
                errorPrint("%s() LN%d, failed to read json, query_times: %"PRId64", need be a valid (>0) number\n",
                           __func__, __LINE__, specifiedQueryTimes->valueint);
                goto PARSE_OVER;
            }
            g_queryInfo.specifiedQueryInfo.queryTimes = specifiedQueryTimes->valueint;
        } else if (!specifiedQueryTimes) {
            g_queryInfo.specifiedQueryInfo.queryTimes = 1;
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
/*
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
*/

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
            if (superSqlSize * g_queryInfo.specifiedQueryInfo.concurrent > MAX_QUERY_SQL_COUNT) {
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
                tstrncpy(g_queryInfo.specifiedQueryInfo.sql[j], sqlStr->valuestring, BUFFER_SIZE);

                // default value is -1, which mean infinite loop
                //g_queryInfo.specifiedQueryInfo.endAfterConsume[j] = -1;
                //cJSON* endAfterConsume =
                //    cJSON_GetObjectItem(specifiedQuery, "endAfterConsume");
                //if (endAfterConsume
                //        && endAfterConsume->type == cJSON_Number) {
                //    g_queryInfo.specifiedQueryInfo.endAfterConsume[j] = endAfterConsume->valueint;
                //}
                //if (g_queryInfo.specifiedQueryInfo.endAfterConsume[j] < -1)
                //    g_queryInfo.specifiedQueryInfo.endAfterConsume[j] = -1;

                //g_queryInfo.specifiedQueryInfo.resubAfterConsume[j] = -1;
                //cJSON* resubAfterConsume = cJSON_GetObjectItem(specifiedQuery, "resubAfterConsume");
                //if ((resubAfterConsume) && (resubAfterConsume->type == cJSON_Number) && (resubAfterConsume->valueint >= 0)) {
                //    g_queryInfo.specifiedQueryInfo.resubAfterConsume[j] = resubAfterConsume->valueint;
                //}

                //if (g_queryInfo.specifiedQueryInfo.resubAfterConsume[j] < -1)
                //    g_queryInfo.specifiedQueryInfo.resubAfterConsume[j] = -1;

                cJSON *resultMode = cJSON_GetObjectItem(sql, "result_mode");
                if ((NULL != resultMode) && (resultMode->type == cJSON_String) && (resultMode->valuestring != NULL)) {
                    if (0 == strcasecmp(resultMode->valuestring, "writefile")) {
                        g_queryInfo.specifiedQueryInfo.resultMode[j]= RESULT_WRITE_FILE;
                    } else if (0 == strcasecmp(resultMode->valuestring, "onlyformat")) {
                        g_queryInfo.specifiedQueryInfo.resultMode[j]= RESULT_ONLY_FORMAT;
                    } else if (0 == strcasecmp(resultMode->valuestring, "onlypull")) {
                        g_queryInfo.specifiedQueryInfo.resultMode[j]= RESULT_ONLY_PULL;
                    } else {
                        errorPrint("%s() LN%d, failed to read json, result_mode %s not recognized\n",
                                __func__, __LINE__, resultMode->valuestring);
                        goto PARSE_OVER;
                    }
                } else if (NULL == resultMode) {
                  g_queryInfo.specifiedQueryInfo.resultMode[j]= RESULT_ONLY_PULL;
                } else {
                    printf("ERROR: failed to read json, result mode error\n");
                    goto PARSE_OVER;
                }

                cJSON *result = cJSON_GetObjectItem(sql, "result_file");
                if ((NULL != result) && (result->type == cJSON_String) && (result->valuestring != NULL)) {
                    tstrncpy(g_queryInfo.specifiedQueryInfo.result[j], result->valuestring, MAX_FILE_NAME_LEN);
                } else if (NULL == result) {
                    memset(g_queryInfo.specifiedQueryInfo.result[j], 0, MAX_FILE_NAME_LEN);
                    if (RESULT_WRITE_FILE == g_queryInfo.specifiedQueryInfo.resultMode[j]) {
                      printf("ERROR: result mode is write file, please config one reusult file\n");
                      goto PARSE_OVER;
                    }
                } else {
                    printf("ERROR: failed to read json, super query result file not found\n");
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
    char *content = taosMemoryCalloc(1, maxLen + 1);
    int   len = fread(content, 1, maxLen, fp);
    if (len <= 0) {
        taosMemoryFree(content);
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
    taosMemoryFree(content);
    cJSON_Delete(root);
    fclose(fp);
    return ret;
}

static void postFreeResource() {
    tmfclose(g_fpOfInsertResult);
    for (int i = 0; i < g_pDbs->dbCount; i++) {
        for (uint64_t j = 0; j < g_pDbs->db[i].superTblCount; j++) {
            if (0 != g_pDbs->db[i].superTbls[j].colsOfCreateChildTable) {
                taosMemoryFree(g_pDbs->db[i].superTbls[j].colsOfCreateChildTable);
                g_pDbs->db[i].superTbls[j].colsOfCreateChildTable = NULL;
            }
            if (0 != g_pDbs->db[i].superTbls[j].tagDataBuf) {
                taosMemoryFree(g_pDbs->db[i].superTbls[j].tagDataBuf);
                g_pDbs->db[i].superTbls[j].tagDataBuf = NULL;
            }
            if (0 != g_pDbs->db[i].superTbls[j].randDataBuf) {
                taosMemoryFree(g_pDbs->db[i].superTbls[j].randDataBuf);
                g_pDbs->db[i].superTbls[j].randDataBuf = NULL;
            }
            if (0 != g_pDbs->db[i].superTbls[j].schemalessLineTemplate) {
                taosMemoryFree(g_pDbs->db[i].superTbls[j].schemalessLineTemplate);
                g_pDbs->db[i].superTbls[j].schemalessLineTemplate = NULL;
            }
        }
    }

    tmfree(g_randbool_buff);
    tmfree(g_randint_buff);
    tmfree(g_rand_voltage_buff);
    tmfree(g_randbigint_buff);
    tmfree(g_randsmallint_buff);
    tmfree(g_randtinyint_buff);
    tmfree(g_randfloat_buff);
    tmfree(g_rand_current_buff);
    tmfree(g_rand_phase_buff);

}

static void printStatPerThread(threadInfo *pThreadInfo)
{
    fprintf(stderr, "====thread[%d] completed total inserted rows: %"PRIu64 ", total affected rows: %"PRIu64". %.2f records/second====\n",
            pThreadInfo->threadID,
            pThreadInfo->totalInsertRows,
            pThreadInfo->totalAffectedRows,
            (pThreadInfo->totalDelay)?
            (double)(pThreadInfo->totalAffectedRows/((double)pThreadInfo->totalDelay/1000000.0)):
            FLT_MAX);
}

static void* syncWriteForSchemaless(threadInfo *pThreadInfo, SSuperTable* stbInfo) {
    int64_t startTimestamp = pThreadInfo->start_time;
    int64_t timestampStep  = stbInfo->timeStampStep;

    int  lenOfOneRows;
    lenOfOneRows = sizeof(stbInfo->schemalessLineTemplate)+128;  // 128 for timestamp    
    char* lineBuf = (char*)taosMemoryCalloc(stbInfo->batchRows * lenOfOneRows, sizeof(char));
    if (NULL == lineBuf) {
      return NULL;
    }

    char** lineArry = (char**)taosMemoryCalloc(stbInfo->batchRows, sizeof(char*));
    if (NULL == lineArry) {
      return NULL;
    }

    for (int x = 0; x < stbInfo->batchRows; x++) {
      lineArry[x] = lineBuf + x * lenOfOneRows;
    }

    for (uint64_t tableSeq = pThreadInfo->start_table_from; tableSeq <= pThreadInfo->end_table_to; tableSeq ++) {

        for (int i = 0; i < stbInfo->insertRows;) {
            int32_t k = 0;
            for (k = 0; k < stbInfo->batchRows;) {
                snprintf(lineArry[k], lenOfOneRows, stbInfo->schemalessLineTemplate, tableSeq, startTimestamp + i * timestampStep);
                k++;
                i++;
                if (i >= stbInfo->insertRows) {
                    break;
                }
            }

            pThreadInfo->totalInsertRows += k;

            uint64_t startTs = taosGetTimestampUs();

            int32_t code = taos_insert_lines(pThreadInfo->taos, lineArry, k);
            if (code != TSDB_CODE_SUCCESS) {
              printf("taos_insert_lines() faile, code: %d, %s\n", code, tstrerror(code));
            }

            uint64_t endTs = taosGetTimestampUs();
            uint64_t delay = endTs - startTs;

            if (delay > pThreadInfo->maxDelay) pThreadInfo->maxDelay = delay;
            if (delay < pThreadInfo->minDelay) pThreadInfo->minDelay = delay;
            pThreadInfo->cntDelay++;
            pThreadInfo->totalDelay += delay;

            if (i >= stbInfo->insertRows) break;
        }   
    }

    pThreadInfo->totalAffectedRows = pThreadInfo->totalInsertRows;
    return NULL;
}

static void* syncWrite(void *sarg) {
    threadInfo *pThreadInfo = (threadInfo *)sarg;
    SSuperTable* stbInfo = pThreadInfo->stbInfo;

    setThreadName("syncWrite");

    if (TAOSC_SCHEMALESS == stbInfo->insertMode) {
      syncWriteForSchemaless(pThreadInfo, stbInfo);
    }
    
    int64_t maxSqlLen = stbInfo->maxSqlLen;
    pThreadInfo->buffer = taosMemoryCalloc(stbInfo->maxSqlLen+1, 1);
    if (NULL == pThreadInfo->buffer) {
        errorPrint( "Failed to alloc %"PRIu64" Bytes, reason:%s\n", stbInfo->maxSqlLen, strerror(errno));
        return NULL;
    }

    //uint64_t lastPrintTime = taosGetTimestampMs();
    uint64_t startTs = taosGetTimestampMs();
    uint64_t endTs;

    pThreadInfo->totalInsertRows = 0;
    pThreadInfo->totalAffectedRows = 0;

    //int percentComplete = 0;
    //int64_t totalRows = stbInfo->insertRows * pThreadInfo->ntables;

    for (uint64_t tableSeq = pThreadInfo->start_table_from; tableSeq <= pThreadInfo->end_table_to; tableSeq ++) {
        int64_t start_time = pThreadInfo->start_time;

        for (uint64_t i = 0; i < stbInfo->insertRows;) {
            uint64_t len = 0;
            
            char *pstr = pThreadInfo->buffer;
            
            len += snprintf(pstr + len, maxSqlLen - len,  "insert into %s.%s%" PRId64 " values ", pThreadInfo->db_name, stbInfo->childTblPrefix, tableSeq);
            
            int32_t k = 0;
            for (k = 0; k < stbInfo->batchRows;) {
                len += snprintf(pstr + len, maxSqlLen - len, "(%" PRId64 ",", start_time + stbInfo->timeStampStep * i);
                len += snprintf(pstr + len, maxSqlLen - len, "%s)", stbInfo->randDataBuf);
            
                k++;
                i++;
                if (i >= stbInfo->insertRows) {
                    break;
                }
            
                if (maxSqlLen - len < stbInfo->randDataLen + 32) {
                    break;
                }
            }

            pThreadInfo->totalInsertRows += k;

            startTs = taosGetTimestampUs();

            int32_t affectedRows = queryDbExec(pThreadInfo->taos, pThreadInfo->buffer, INSERT_TYPE, false);            

            endTs = taosGetTimestampUs();
            uint64_t delay = endTs - startTs;
            //performancePrint("%s() LN%d, insert execution time is %10.f ms\n", __func__, __LINE__, delay/1000.0);
            //verbosePrint("[%d] %s() LN%d affectedRows=%d\n", pThreadInfo->threadID, __func__, __LINE__, affectedRows);

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

            /*
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
            */

            if (i >= stbInfo->insertRows) break;
        }   // num_of_DPT
    } // tableSeq

    //if (percentComplete < 100) printf("[%d]:%d%%\n", pThreadInfo->threadID, percentComplete);

free_of_progressive:
    tmfree(pThreadInfo->buffer);
    printStatPerThread(pThreadInfo);
    return NULL;
}

static void startMultiThreadInsertData(int threads, char* db_name, char* precision, SSuperTable* stbInfo) {

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
          exit(EXIT_FAILURE);
      }
  }

  int64_t start_time;
  if (stbInfo) {
      if (0 == strncasecmp(stbInfo->startTimestamp, "now", 3)) {
          start_time = taosGetTimestamp(timePrec);
      } else {
          if (TSDB_CODE_SUCCESS != taosParseTime(stbInfo->startTimestamp, &start_time, strlen(stbInfo->startTimestamp), timePrec, 0)) {
              ERROR_EXIT("failed to parse time!\n");
          }
      }
  } else {
      start_time = 1577808000000; // 2020-01-01 00:00:00
  }

  if (TAOSC_SCHEMALESS == stbInfo->insertMode) {
    int64_t start = taosGetTimestampMs();
    int64_t timestampStep = stbInfo->timeStampStep;
    printf("setup child tables...\n");
    {
      TAOS * taos = NULL;
      taos = taos_connect(g_pDbs->host, g_pDbs->user, g_pDbs->password, db_name, g_pDbs->port);
      if (taos == NULL) {
          errorPrint( "Failed to connect to TDengine, reason:%s\n", taos_errstr(NULL));
          exit(EXIT_FAILURE);
      }
  
      char** linesStb = taosMemoryCalloc(stbInfo->childTblCount, sizeof(char*));
      for (int i = 0; i < stbInfo->childTblCount; i++) {
        char* lineStb = taosMemoryCalloc(strlen(stbInfo->schemalessLineTemplate)+128, 1);
        snprintf(lineStb, strlen(stbInfo->schemalessLineTemplate)+128, stbInfo->schemalessLineTemplate, i, start_time - timestampStep);
        linesStb[i] = lineStb;
      }
  
      int32_t code = taos_insert_lines(taos, linesStb, stbInfo->childTblCount);
     
      for (int i = 0; i < stbInfo->childTblCount; ++i) {
        taosMemoryFree(linesStb[i]);
      }
      taosMemoryFree(linesStb);
      taos_close(taos);
  
      if (code != TSDB_CODE_SUCCESS) {
        printf("taos_insert_lines() faile, code: %d, %s.\n", code, tstrerror(code));
        exit(EXIT_FAILURE);
      }
    }    
    int64_t end = taosGetTimestampMs();
    int64_t t   = end - start;
    double tInMs = t/1000.0;
  
    fprintf(stderr, "Spent %.2f seconds to pre-create child tables: %"PRId64", with one thread(s) into %s.%s. %.2f tables/second\n\n",
            tInMs, stbInfo->childTblCount, db_name, stbInfo->sTblName, (tInMs)? (double)(stbInfo->childTblCount/tInMs):FLT_MAX);    
  }
  
  int64_t start = taosGetTimestampMs();
  int64_t ntables = stbInfo->childTblCount;
  uint64_t tableFrom = 0;

  int64_t a = ntables / threads;
  if (a < 1) {
      threads = ntables;
      a = 1;
  }

  int64_t b = 0;
  if (threads != 0) {
      b = ntables % threads;
  }

  TdThread  *pids = taosMemoryCalloc(1, threads * sizeof(TdThread));
  assert(pids != NULL);

  threadInfo *infos = taosMemoryCalloc(1, threads * sizeof(threadInfo));
  assert(infos != NULL);

  memset(pids, 0, threads * sizeof(TdThread));
  memset(infos, 0, threads * sizeof(threadInfo));

  for (int i = 0; i < threads; i++) {
    threadInfo *pThreadInfo = infos + i;
    pThreadInfo->threadID = i;

    tstrncpy(pThreadInfo->db_name, db_name, TSDB_DB_NAME_LEN);
    pThreadInfo->time_precision = timePrec;
    pThreadInfo->stbInfo = stbInfo;

    pThreadInfo->start_time = start_time;
    pThreadInfo->minDelay = UINT64_MAX;

    pThreadInfo->taos = taos_connect(g_pDbs->host, g_pDbs->user, g_pDbs->password, db_name, g_pDbs->port);
    if (NULL == pThreadInfo->taos) {
      taosMemoryFree(infos);
      errorPrint("%s() LN%d, connect to server fail from insert sub thread, reason: %s\n", __func__, __LINE__, taos_errstr(NULL));
      exit(EXIT_FAILURE);
    }

    pThreadInfo->start_table_from = tableFrom;
    pThreadInfo->ntables = i<b?a+1:a;
    pThreadInfo->end_table_to = i < b ? tableFrom + a : tableFrom + a - 1;
    tableFrom = pThreadInfo->end_table_to + 1;

    pthread_create(pids + i, NULL, syncWrite, pThreadInfo);
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

      //tsem_destroy(&(pThreadInfo->lock_sem));
      taos_close(pThreadInfo->taos);

      debugPrint("%s() LN%d, [%d] totalInsert=%"PRIu64" totalAffected=%"PRIu64"\n",
              __func__, __LINE__,
              pThreadInfo->threadID, 
              pThreadInfo->totalInsertRows,
              pThreadInfo->totalAffectedRows);

      stbInfo->totalAffectedRows += pThreadInfo->totalAffectedRows;
      stbInfo->totalInsertRows   += pThreadInfo->totalInsertRows;

      totalDelay  += pThreadInfo->totalDelay;
      cntDelay    += pThreadInfo->cntDelay;
      if (pThreadInfo->maxDelay > maxDelay) maxDelay = pThreadInfo->maxDelay;
      if (pThreadInfo->minDelay < minDelay) minDelay = pThreadInfo->minDelay;
  }
  cntDelay -= 1;

  if (cntDelay == 0)    cntDelay = 1;
  avgDelay = (double)totalDelay / cntDelay;

  int64_t end = taosGetTimestampMs();
  int64_t t = end - start;

  double tInMs = t/1000.0;

  fprintf(stderr, "Spent %.2f seconds to insert rows: %"PRIu64", affected rows: %"PRIu64" with %d thread(s) into %s.%s. %.2f records/second\n\n",
          tInMs, stbInfo->totalInsertRows,
          stbInfo->totalAffectedRows,
          threads, db_name, stbInfo->sTblName,
          (tInMs)? (double)(stbInfo->totalInsertRows/tInMs):FLT_MAX);

  if (g_fpOfInsertResult) {
      fprintf(g_fpOfInsertResult,
          "Spent %.2f seconds to insert rows: %"PRIu64", affected rows: %"PRIu64" with %d thread(s) into %s.%s. %.2f records/second\n\n",
          tInMs, stbInfo->totalInsertRows,
          stbInfo->totalAffectedRows,
          threads, db_name, stbInfo->sTblName,
          (tInMs)?
          (double)(stbInfo->totalInsertRows/tInMs):FLT_MAX);
  }

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

  taosMemoryFree(pids);
  taosMemoryFree(infos);
}

static void startInsertCsvFile(char* db_name, SSuperTable* stbInfo) {
  TAOS* taos = taos_connect(g_pDbs->host, g_pDbs->user, g_pDbs->password, db_name, g_pDbs->port);
  if (NULL == taos) {
      errorPrint("%s() LN%d, connect to server fail from insert csv file, reason: %s\n",
              __func__, __LINE__, taos_errstr(NULL));
      exit(EXIT_FAILURE);
  }

  uint64_t st = 0;
  uint64_t et = 0;
  uint64_t maxDelay = 0;
  uint64_t minDelay = UINT64_MAX;
  uint64_t sumDelay = 0;
  char cmdBuf[TBNAME_PREFIX_LEN+TSDB_DB_NAME_LEN+MAX_FILE_NAME_LEN+32];
  //int32_t affectedRows;
  for (int j = 0; j < stbInfo->childTblCount; j++) {
    memset(cmdBuf, 0, sizeof(cmdBuf));
    (void)snprintf(cmdBuf, TBNAME_PREFIX_LEN+TSDB_DB_NAME_LEN+MAX_FILE_NAME_LEN+32, "insert into %s.%s%d file \"%s\";", db_name, stbInfo->childTblPrefix, j, stbInfo->csvFile);
    st = taosGetTimestampMs();
    (void)queryDbExec(taos, cmdBuf, INSERT_TYPE, false);
    et = taosGetTimestampMs();

    uint64_t curDelay = et - st;
    sumDelay  += curDelay;
    if (curDelay > maxDelay) maxDelay = curDelay;
    if (curDelay < minDelay) minDelay = curDelay;
  }
    
  double avgDelay = (double)sumDelay / stbInfo->childTblCount;
  
  printf("Complete insert into stb_xx file %s, Spent time:\n", stbInfo->csvFile);
  printf("    avgDelay: %10.3f s, maxDelay: %10.3f s, mixDelay: %10.3f s\n", avgDelay/1000.0, (double)maxDelay/1000.0, (double)minDelay/1000.0);
}

static int insertTestProcess() {
  int ret = printfInsertMeta();
  
  if (ret == -1) exit(EXIT_FAILURE);
  
  debugPrint("%d result file: %s\n", __LINE__, g_pDbs->resultFile);
  g_fpOfInsertResult = fopen(g_pDbs->resultFile, "a");
  if (NULL == g_fpOfInsertResult) {
      errorPrint( "Failed to open %s for save result\n", g_pDbs->resultFile);
      return -1;
  }
  
  if (g_fpOfInsertResult) {
      printfInsertMetaToFile(g_fpOfInsertResult);
  }
 
  init_rand_data();
  
  // create database and super tables
  char *cmdBuffer = taosMemoryCalloc(1, BUFFER_SIZE);
  assert(cmdBuffer);
  
  if(createDatabasesAndStables(cmdBuffer) != 0) {
    if (g_fpOfInsertResult) {
      fclose(g_fpOfInsertResult);
    }
    taosMemoryFree(cmdBuffer);
    return -1;
  }
  taosMemoryFree(cmdBuffer);
  
  double start;
  double end;
  
  // create child tables
  start = taosGetTimestampMs();
  createChildTables();
  end = taosGetTimestampMs();
  
  if (g_totalChildTables > 0) {
      fprintf(stderr, "Spent %.4f seconds to create %"PRId64" tables with %d thread(s)\n\n",
              (end - start)/1000.0, g_totalChildTables, g_pDbs->threadCountByCreateTbl);
      if (g_fpOfInsertResult) {
          fprintf(g_fpOfInsertResult,
                  "Spent %.4f seconds to create %"PRId64" tables with %d thread(s)\n\n",
                  (end - start)/1000.0, g_totalChildTables, g_pDbs->threadCountByCreateTbl);
      }
  }
  
  // create sub threads for inserting data
  for (int i = 0; i < g_pDbs->dbCount; i++) {
    if (g_pDbs->db[i].superTblCount > 0) {
        for (uint64_t j = 0; j < g_pDbs->db[i].superTblCount; j++) {
            SSuperTable* stbInfo = &g_pDbs->db[i].superTbls[j];

            if (stbInfo && (stbInfo->insertMode == TAOSC_CSV)) {
              startInsertCsvFile(g_pDbs->db[i].dbName, stbInfo);
              continue;
            }
            
            if (stbInfo && (stbInfo->insertRows > 0)) {
                startMultiThreadInsertData(
                        g_pDbs->threadCount,
                        g_pDbs->db[i].dbName,
                        g_pDbs->db[i].dbCfg.precision,
                        stbInfo);
            }
        }
    }
  }
  postFreeResource();
  
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
            errorPrint("[%d] Failed to connect to TDengine, reason:%s\n",
                    pThreadInfo->threadID, taos_errstr(NULL));
            return NULL;
        }

        pThreadInfo->taos = taos;
    }

    char sqlStr[TSDB_DB_NAME_LEN + 5];
    sprintf(sqlStr, "use %s", g_queryInfo.dbName);
    if (0 != queryDbExec(pThreadInfo->taos, sqlStr, NO_INSERT_TYPE, false)) {
        taos_close(pThreadInfo->taos);
        errorPrint( "use database %s failed!\n\n", g_queryInfo.dbName);
        return NULL;
    }

    uint64_t queryTimes = g_queryInfo.specifiedQueryInfo.queryTimes;
    
    for (int i = 0; i < g_queryInfo.specifiedQueryInfo.sqlCount; i++) {
      if (g_queryInfo.specifiedQueryInfo.result[i][0] != '\0') {
          sprintf(pThreadInfo->filePath, "%s-%d", g_queryInfo.specifiedQueryInfo.result[i], pThreadInfo->threadID);
      }

      uint64_t st = 0;
      uint64_t et = 0;
      uint64_t maxDelay = 0;
      uint64_t minDelay = UINT64_MAX;
      uint64_t sumDelay = 0;
      for (int j = 0; j < queryTimes; j++) {

        st = taosGetTimestampMs();        
        selectAndGetResult(pThreadInfo, g_queryInfo.specifiedQueryInfo.sql[i], g_queryInfo.specifiedQueryInfo.resultMode[i]);        
        et = taosGetTimestampMs();

        uint64_t curDelay = et - st;
        sumDelay  += curDelay;
        if (curDelay > maxDelay) maxDelay = curDelay;
        if (curDelay < minDelay) minDelay = curDelay;
      }
      
      double avgDelay = (double)sumDelay / queryTimes;
      
      printf("Thread[%d] complete sql[%.*s], Spent time:\n", pThreadInfo->threadID, 128, g_queryInfo.specifiedQueryInfo.sql[i]);
      printf("    avgDelay: %10.3f s, maxDelay: %10.3f s, mixDelay: %10.3f s\n", avgDelay/1000.0, (double)maxDelay/1000.0, (double)minDelay/1000.0);
    }
    
    return NULL;
}

static int queryTestProcess() {

    printfQueryMeta();

    TAOS * taos = NULL;
    taos = taos_connect(g_queryInfo.host, g_queryInfo.user, g_queryInfo.password, NULL, g_queryInfo.port);
    if (taos == NULL) {
        errorPrint( "Failed to connect to TDengine, reason:%s\n", taos_errstr(NULL));
        exit(EXIT_FAILURE);
    }

    if (g_args.debug_print || g_args.verbose_print) {
        printfQuerySystemInfo(taos);
    }
    taos_close(taos);

    TdThread   *pids  = NULL;
    threadInfo *infos = NULL;
    //==== create sub threads for query from specify table
    int nConcurrent    = g_queryInfo.specifiedQueryInfo.concurrent;
    uint64_t nSqlCount = g_queryInfo.specifiedQueryInfo.sqlCount;

    //uint64_t startTs = taosGetTimestampMs();

    if ((nSqlCount > 0) && (nConcurrent > 0)) {
        pids  = taosMemoryCalloc(1, nConcurrent * sizeof(TdThread));
        infos = taosMemoryCalloc(1, nConcurrent * sizeof(threadInfo));

        if ((NULL == pids) || (NULL == infos)) {
            ERROR_EXIT("memory allocation failed for create threads\n");
        }

        for (int j = 0; j < nConcurrent; j++) {
            threadInfo *pThreadInfo = infos + j;
            pThreadInfo->threadID = j;
            pThreadInfo->taos = NULL;// TODO: workaround to use separate taos connection;        
            pthread_create(pids + j, NULL, specifiedTableQuery, pThreadInfo);
        }


/*
        for (uint64_t i = 0; i < nSqlCount; i++) {
            for (int j = 0; j < nConcurrent; j++) {
                uint64_t seq = i * nConcurrent + j;
                threadInfo *pThreadInfo = infos + seq;
                pThreadInfo->threadID = seq;
                pThreadInfo->querySeq = i;
                pThreadInfo->taos = NULL;// TODO: workaround to use separate taos connection;

                pthread_create(pids + seq, NULL, specifiedTableQuery, pThreadInfo);
            }
        }
*/        
    }

    if ((nSqlCount > 0) && (nConcurrent > 0)) {
        for (int i = 0; i < nConcurrent; i++) {
          pthread_join(pids[i], NULL);
        }
    }

    tmfree((char*)pids);
    tmfree((char*)infos);

    //  taos_close(taos);// TODO: workaround to use separate taos connection;
    //uint64_t endTs = taosGetTimestampMs();

    //uint64_t totalQueried = g_queryInfo.specifiedQueryInfo.totalQueried;

    //fprintf(stderr, "==== completed total queries: %"PRIu64", the QPS of all threads: %10.3f====\n",
    //        totalQueried, (double)(totalQueried/((endTs-startTs)/1000.0)));
    return 0;
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
    } else if ((STABLE_CLASS == class) && (ASYNC_MODE == g_queryInfo.superQueryInfo.asyncMode)) {
       ;
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
            errorPrint("[%d] Failed to connect to TDengine, reason:%s\n",
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
    printfQueryMeta();
    resetAfterAnsiEscape();

    TAOS * taos = NULL;
    taos = taos_connect(g_queryInfo.host,
            g_queryInfo.user,
            g_queryInfo.password,
            g_queryInfo.dbName,
            g_queryInfo.port);
    if (taos == NULL) {
        errorPrint( "Failed to connect to TDengine, reason:%s\n",
                taos_errstr(NULL));
        exit(EXIT_FAILURE);
    }

    taos_close(taos); // TODO: workaround to use separate taos connection;

    TdThread   *pids = NULL;
    threadInfo *infos = NULL;

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
            exit(EXIT_FAILURE);
        }

        pids  = taosMemoryCalloc(
                1,
                g_queryInfo.specifiedQueryInfo.sqlCount *
                g_queryInfo.specifiedQueryInfo.concurrent *
                sizeof(TdThread));
        infos = taosMemoryCalloc(
                1,
                g_queryInfo.specifiedQueryInfo.sqlCount *
                g_queryInfo.specifiedQueryInfo.concurrent *
                sizeof(threadInfo));
        if ((NULL == pids) || (NULL == infos)) {
            errorPrint("%s() LN%d, malloc failed for create threads\n", __func__, __LINE__);
            exit(EXIT_FAILURE);
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


    for (int i = 0; i < g_queryInfo.specifiedQueryInfo.sqlCount; i++) {
        for (int j = 0; j < g_queryInfo.specifiedQueryInfo.concurrent; j++) {
            uint64_t seq = i * g_queryInfo.specifiedQueryInfo.concurrent + j;
            pthread_join(pids[seq], NULL);
        }
    }

    tmfree((char*)pids);
    tmfree((char*)infos);

    return 0;
}

static void initOfInsertMeta() {
    g_pDbs = taosMemoryMalloc(sizeof(SDbs)+1);
    if (NULL == g_pDbs) {
      printf("malloc fail for sdb\n");
      exit(-1);
    }
    memset(g_pDbs, 0, sizeof(SDbs));

    // set default values
    tstrncpy(g_pDbs->host, "127.0.0.1", MAX_HOSTNAME_SIZE);
    g_pDbs->port = 6030;
    tstrncpy(g_pDbs->user, TSDB_DEFAULT_USER, MAX_USERNAME_SIZE);
    tstrncpy(g_pDbs->password, TSDB_DEFAULT_PASS, MAX_PASSWORD_SIZE);
    g_pDbs->threadCount = 2;
    g_pDbs->threadCountByCreateTbl = 1;

}

static void initOfQueryMeta() {
    memset(&g_queryInfo, 0, sizeof(SQueryMetaInfo));

    // set default values
    tstrncpy(g_queryInfo.host, "127.0.0.1", MAX_HOSTNAME_SIZE);
    g_queryInfo.port = 6030;
    tstrncpy(g_queryInfo.user, TSDB_DEFAULT_USER, MAX_USERNAME_SIZE);
    tstrncpy(g_queryInfo.password, TSDB_DEFAULT_PASS, MAX_PASSWORD_SIZE);
}

static void testMetaFile() {
    if (INSERT_TEST == g_args.test_mode) {
        if (g_pDbs->cfgDir[0]) taos_options(TSDB_OPTION_CONFIGDIR, g_pDbs->cfgDir);

        insertTestProcess();
    } else if (QUERY_TEST == g_args.test_mode) {
        if (g_queryInfo.cfgDir[0]) taos_options(TSDB_OPTION_CONFIGDIR, g_queryInfo.cfgDir);

        queryTestProcess();
    } else if (SUBSCRIBE_TEST == g_args.test_mode) {
        if (g_queryInfo.cfgDir[0])  taos_options(TSDB_OPTION_CONFIGDIR, g_queryInfo.cfgDir);

        subscribeTestProcess();
    }  else {
        ;
    }
}

int main(int argc, char *argv[]) {
    parse_args(argc, argv, &g_args);

    debugPrint("meta file: %s\n", g_args.metaFile);
    if (NULL == g_args.metaFile) {
      printf("Please specify a json!\n");
      return 1;
    }

    initOfInsertMeta();
    initOfQueryMeta();

    if (false == getInfoFromJsonFile(g_args.metaFile)) {
        printf("Failed to read %s\n", g_args.metaFile);
        return 1;
    }

    testMetaFile();

    return 0;
}


