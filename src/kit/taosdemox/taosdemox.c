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

#define _GNU_SOURCE
#define CURL_STATICLIB

#ifdef TD_LOWA_CURL
#include "curl/curl.h"
#endif

#ifdef LINUX
  #include "os.h"
  #include "cJSON.h"
  #include <argp.h>
  #include <assert.h>
  #include <inttypes.h>
  #ifndef _ALPINE
    #include <error.h>
  #endif
  #include <pthread.h>
  #include <semaphore.h>
  #include <stdbool.h>
  #include <stdio.h>
  #include <stdlib.h>
  #include <string.h>
  #include <sys/time.h>
  #include <time.h>
  #include <unistd.h>
  #include <wordexp.h>
  #include <regex.h>
#else  
  #include <assert.h>
  #include <regex.h>
  #include <stdio.h>
  #include "os.h"
  
  #pragma comment ( lib, "libcurl.lib" )
  #pragma comment ( lib, "ws2_32.lib" )
  #pragma comment ( lib, "winmm.lib" )
  #pragma comment ( lib, "wldap32.lib" )  
#endif  

#include "taos.h"
#include "tutil.h"

extern char configDir[];

#define INSERT_JSON_NAME      "insert.json"
#define QUERY_JSON_NAME       "query.json"
#define SUBSCRIBE_JSON_NAME   "subscribe.json"

#define INSERT_MODE        0
#define QUERY_MODE         1
#define SUBSCRIBE_MODE     2

#define MAX_SQL_SIZE       65536
#define BUFFER_SIZE        (65536*2)
#define MAX_DB_NAME_SIZE   64
#define MAX_TB_NAME_SIZE   64
#define MAX_DATA_SIZE      16000
#define MAX_NUM_DATATYPE   10
#define OPT_ABORT          1 /* –abort */
#define STRING_LEN         60000
#define MAX_PREPARED_RAND  1000000
//#define MAX_SQL_SIZE       65536
#define MAX_FILE_NAME_LEN  256

#define   MAX_SAMPLES_ONCE_FROM_FILE   10000
#define   MAX_NUM_DATATYPE 10

#define   MAX_DB_COUNT           8
#define   MAX_SUPER_TABLE_COUNT  8
#define   MAX_COLUMN_COUNT       1024
#define   MAX_TAG_COUNT          128

#define   MAX_QUERY_SQL_COUNT    10
#define   MAX_QUERY_SQL_LENGTH   256

typedef enum CREATE_SUB_TALBE_MOD_EN {
  PRE_CREATE_SUBTBL,
  AUTO_CREATE_SUBTBL,
  NO_CREATE_SUBTBL
} CREATE_SUB_TALBE_MOD_EN;
  
typedef enum TALBE_EXISTS_EN {
  TBL_ALREADY_EXISTS,
  TBL_NO_EXISTS,
  TBL_EXISTS_BUTT
} TALBE_EXISTS_EN;

enum MODE {
  SYNC, 
  ASYNC,
  MODE_BUT
};
  
enum QUERY_TYPE {
  NO_INSERT_TYPE,
  INSERT_TYPE, 
  QUERY_TYPE_BUT
} ;  
  
enum _describe_table_index {
  TSDB_DESCRIBE_METRIC_FIELD_INDEX,
  TSDB_DESCRIBE_METRIC_TYPE_INDEX,
  TSDB_DESCRIBE_METRIC_LENGTH_INDEX,
  TSDB_DESCRIBE_METRIC_NOTE_INDEX,
  TSDB_MAX_DESCRIBE_METRIC
};

typedef struct {
  char field[TSDB_COL_NAME_LEN + 1];
  char type[16];
  int length;
  char note[128];
} SColDes;

/* Used by main to communicate with parse_opt. */
typedef struct SArguments_S {
  char *   metaFile;
  char *   host;
  uint16_t port;
  char *   user;
  char *   password;
  char *   database;
  int      replica;
  char *   tb_prefix;
  char *   sqlFile;
  bool     use_metric;
  bool     insert_only;
  char *   output_file;
  int      mode;
  char *   datatype[MAX_NUM_DATATYPE + 1];
  int      len_of_binary;
  int      num_of_CPR;
  int      num_of_threads;
  int      num_of_RPR;
  int      num_of_tables;
  int      num_of_DPT;
  int      abort;
  int      disorderRatio;
  int      disorderRange;
  int      method_of_delete;
  char **  arg_list;
} SArguments;

typedef struct SColumn_S {
  char  field[TSDB_COL_NAME_LEN + 1];
  char  dataType[MAX_TB_NAME_SIZE];
  int   dataLen;
  char  note[128];  
} StrColumn;

typedef struct SSuperTable_S {
  char         sTblName[MAX_TB_NAME_SIZE];
  int          childTblCount;
  bool         superTblExists;    // 0: no, 1: yes
  bool         childTblExists;    // 0: no, 1: yes  
  int          batchCreateTableNum;  // 0: no batch,  > 0: batch table number in one sql
  int8_t       autoCreateTable;                  // 0: create sub table, 1: auto create sub table
  char         childTblPrefix[MAX_TB_NAME_SIZE];
  char         dataSource[MAX_TB_NAME_SIZE];  // rand_gen or sample
  char         insertMode[MAX_TB_NAME_SIZE];  // taosc, restful
  int          insertRate;  // 0: unlimit  > 0   rows/s

  int          multiThreadWriteOneTbl;   // 0: no, 1: yes
  int          numberOfTblInOneSql;      // 0/1: one table, > 1: number of tbl
  int          rowsPerTbl;               // 
  int          disorderRatio;            // 0: no disorder, >0: x%
  int          disorderRange;            // ms or us by database precision
  int          maxSqlLen;                // 
  
  int64_t      insertRows;               // 0: no limit
  int          timeStampStep;
  char         startTimestamp[MAX_TB_NAME_SIZE];  // 
  char         sampleFormat[MAX_TB_NAME_SIZE];  // csv, json
  char         sampleFile[MAX_FILE_NAME_LEN];
  char         tagsFile[MAX_FILE_NAME_LEN];

  int          columnCount;
  StrColumn    columns[MAX_COLUMN_COUNT];
  int          tagCount;
  StrColumn    tags[MAX_TAG_COUNT];

  char*        childTblName;
  char*        colsOfCreatChildTable;
  int          lenOfOneRow;
  int          lenOfTagOfOneRow;

  char*        sampleDataBuf;
  int          sampleDataBufSize;
  //int          sampleRowCount;
  //int          sampleUsePos;

  int          tagSource;    // 0: rand, 1: tag sample
  char*        tagDataBuf;
  int          tagSampleCount;
  int          tagUsePos;

  // statistics  
  int64_t    totalRowsInserted;
  int64_t    totalAffectedRows;
} SSuperTable;

typedef struct SDbCfg_S { 
//  int       maxtablesPerVnode;
  int       minRows; 
  int       maxRows;
  int       comp;
  int       walLevel;
  int       fsync;  
  int       replica;
  int       update;
  int       keep;
  int       days;
  int       cache;
  int       blocks;
  int       quorum;
  char      precision[MAX_TB_NAME_SIZE];  
} SDbCfg;

typedef struct SDataBase_S {
  char         dbName[MAX_DB_NAME_SIZE];
  int          drop;  // 0: use exists, 1: if exists, drop then new create
  SDbCfg       dbCfg;
  int          superTblCount;
  SSuperTable  superTbls[MAX_SUPER_TABLE_COUNT];
} SDataBase;

typedef struct SDbs_S {
  char         cfgDir[MAX_FILE_NAME_LEN];
  char         host[MAX_DB_NAME_SIZE];
  uint16_t     port;
  char         user[MAX_DB_NAME_SIZE];
  char         password[MAX_DB_NAME_SIZE];
  char         resultFile[MAX_FILE_NAME_LEN];
  bool         use_metric;
  bool         insert_only;
  bool         do_aggreFunc;
  bool         queryMode;
  
  int          threadCount;
  int          threadCountByCreateTbl;
  int          dbCount;
  SDataBase    db[MAX_DB_COUNT];

  // statistics
  int64_t    totalRowsInserted;
  int64_t    totalAffectedRows;
} SDbs;

typedef struct SuperQueryInfo_S {
  int          rate;  // 0: unlimit  > 0   loop/s
  int          concurrent;
  int          sqlCount;
  int          subscribeMode; // 0: sync, 1: async
  int          subscribeInterval; // ms
  int          subscribeRestart;
  int          subscribeKeepProgress;
  char         sql[MAX_QUERY_SQL_COUNT][MAX_QUERY_SQL_LENGTH];  
  char         result[MAX_QUERY_SQL_COUNT][MAX_FILE_NAME_LEN];
  TAOS_SUB*    tsub[MAX_QUERY_SQL_COUNT];
} SuperQueryInfo;

typedef struct SubQueryInfo_S {
  char         sTblName[MAX_TB_NAME_SIZE];
  int          rate;  // 0: unlimit  > 0   loop/s
  int          threadCnt;  
  int          subscribeMode; // 0: sync, 1: async
  int          subscribeInterval; // ms
  int          subscribeRestart;
  int          subscribeKeepProgress;
  int          childTblCount;
  char         childTblPrefix[MAX_TB_NAME_SIZE];
  int          sqlCount;
  char         sql[MAX_QUERY_SQL_COUNT][MAX_QUERY_SQL_LENGTH];  
  char         result[MAX_QUERY_SQL_COUNT][MAX_FILE_NAME_LEN];
  TAOS_SUB*    tsub[MAX_QUERY_SQL_COUNT];
  
  char*        childTblName;
} SubQueryInfo;

typedef struct SQueryMetaInfo_S {
  char         cfgDir[MAX_FILE_NAME_LEN];
  char         host[MAX_DB_NAME_SIZE];
  uint16_t     port;
  char         user[MAX_DB_NAME_SIZE];
  char         password[MAX_DB_NAME_SIZE];
  char         dbName[MAX_DB_NAME_SIZE];
  char         queryMode[MAX_TB_NAME_SIZE];  // taosc, restful

  SuperQueryInfo  superQueryInfo;
  SubQueryInfo    subQueryInfo;  
} SQueryMetaInfo;

typedef struct SThreadInfo_S {
  TAOS *taos;
  #ifdef TD_LOWA_CURL
  CURL *curl_handle;
  #endif
  int threadID;
  char db_name[MAX_DB_NAME_SIZE];
  char fp[4096];
  char tb_prefix[MAX_TB_NAME_SIZE];
  int start_table_id;
  int end_table_id;
  int data_of_rate;
  int64_t start_time;  
  char* cols;  
  bool  use_metric;  
  SSuperTable* superTblInfo;

  // for async insert
  tsem_t lock_sem;
  int64_t  counter;  
  int64_t  st;
  int64_t  et;
  int64_t  lastTs;
  int nrecords_per_request;

  // statistics
  int64_t totalRowsInserted;
  int64_t totalAffectedRows;
} threadInfo;

typedef  struct curlMemInfo_S {
    char *buf;
    size_t sizeleft;
  } curlMemInfo;
  
  

#ifdef LINUX
  /* The options we understand. */
  static struct argp_option options[] = {
    {0, 'f', "meta file",                0, "The meta data to the execution procedure, if use -f, all others options invalid. Default is NULL.",     0},
    #ifdef _TD_POWER_
    {0, 'c', "config_directory",         0, "Configuration directory. Default is '/etc/power/'.",                                                               1},
    {0, 'P', "password",                 0, "The password to use when connecting to the server. Default is 'powerdb'.",                                         2},
    #else
    {0, 'c', "config_directory",         0, "Configuration directory. Default is '/etc/taos/'.",                                                                1},
    {0, 'P', "password",                 0, "The password to use when connecting to the server. Default is 'taosdata'.",                                        2},
    #endif  
    {0, 'h', "host",                     0, "The host to connect to TDengine. Default is localhost.",                                                           2},
    {0, 'p', "port",                     0, "The TCP/IP port number to use for the connection. Default is 0.",                                                  2},
    {0, 'u', "user",                     0, "The TDengine user name to use when connecting to the server. Default is 'root'.",                                  2},
    {0, 'd', "database",                 0, "Destination database. Default is 'test'.",                                                                         3},
    {0, 'a', "replica",                  0, "Set the replica parameters of the database, Default 1, min: 1, max: 3.",                                           4},
    {0, 'm', "table_prefix",             0, "Table prefix name. Default is 't'.",                                                                               4},
    {0, 's', "sql file",                 0, "The select sql file.",                                                                                             6},
    {0, 'M', 0,                          0, "Use metric flag.",                                                                                                 4},
    {0, 'o', "outputfile",               0, "Direct output to the named file. Default is './output.txt'.",                                                      6},
    {0, 'q', "query_mode",               0, "Query mode--0: SYNC, 1: ASYNC. Default is SYNC.",                                                                  4},
    {0, 'b', "type_of_cols",             0, "The data_type of columns, default: TINYINT,SMALLINT,INT,BIGINT,FLOAT,DOUBLE,BINARY,NCHAR,BOOL,TIMESTAMP.",         4},
    {0, 'w', "length_of_chartype",       0, "The length of data_type 'BINARY' or 'NCHAR'. Default is 16",                                                       4},
    {0, 'l', "num_of_cols_per_record",   0, "The number of columns per record. Default is 10.",                                                                 4},
    {0, 'T', "num_of_threads",           0, "The number of threads. Default is 10.",                                                                            4},
    // {0, 'r', "num_of_records_per_req",   0, "The number of records per request. Default is 100.",                                                               4},
    {0, 't', "num_of_tables",            0, "The number of tables. Default is 10000.",                                                                          4},
    {0, 'n', "num_of_records_per_table", 0, "The number of records per table. Default is 10000.",                                                               4},
    {0, 'x', 0,                          0, "Not insert only flag.",                                                                                                4},
    {0, 'O', "disorderRatio",            0, "Insert mode--0: In order, > 0: disorder ratio. Default is in order.",                                              4},
    {0, 'R', "disorderRang",             0, "Out of order data's range, ms, default is 1000.",                                                                  4},
    //{0, 'D', "delete database",          0, "if elete database if exists. 0: no, 1: yes, default is 1",                                                         5},
    {0}};

/* Parse a single option. */
static error_t parse_opt(int key, char *arg, struct argp_state *state) {
  // Get the input argument from argp_parse, which we know is a pointer to our arguments structure. 
  SArguments *arguments = state->input;
  wordexp_t full_path;
  char **sptr;
  switch (key) {
    case 'f':
      arguments->metaFile = arg;
      break;
    case 'h':
      arguments->host = arg;
      break;
    case 'p':
      arguments->port = atoi(arg);
      break;
    case 'u':
      arguments->user = arg;
      break;
    case 'P':
      arguments->password = arg;
      break;
    case 'o':
      arguments->output_file = arg;
      break;
    case 's':
      arguments->sqlFile = arg;
      break;
    case 'q':
      arguments->mode = atoi(arg);
      break;
    case 'T':
      arguments->num_of_threads = atoi(arg);
      break;
    //case 'r':
    //  arguments->num_of_RPR = atoi(arg);
    //  break;
    case 't':
      arguments->num_of_tables = atoi(arg);
      break;
    case 'n':
      arguments->num_of_DPT = atoi(arg);
      break;
    case 'd':
      arguments->database = arg;
      break;
    case 'l':
      arguments->num_of_CPR = atoi(arg);
      break;
    case 'b':
      sptr = arguments->datatype;
      if (strstr(arg, ",") == NULL) {
        if (strcasecmp(arg, "INT")      != 0 && strcasecmp(arg, "FLOAT")     != 0 &&
            strcasecmp(arg, "TINYINT")  != 0 && strcasecmp(arg, "BOOL")      != 0 &&
            strcasecmp(arg, "SMALLINT") != 0 && strcasecmp(arg, "TIMESTAMP") != 0 &&
            strcasecmp(arg, "BIGINT")   != 0 && strcasecmp(arg, "DOUBLE")    != 0 &&
            strcasecmp(arg, "BINARY")   != 0 && strcasecmp(arg, "NCHAR")     != 0) {
          argp_error(state, "Invalid data_type!");
        }
        sptr[0] = arg;
      } else {
        int index = 0;
        char *dupstr = strdup(arg);
        char *running = dupstr;
        char *token = strsep(&running, ",");
        while (token != NULL) {
        if (strcasecmp(token, "INT")      != 0 && strcasecmp(token, "FLOAT")     != 0 &&
            strcasecmp(token, "TINYINT")  != 0 && strcasecmp(token, "BOOL")      != 0 &&
            strcasecmp(token, "SMALLINT") != 0 && strcasecmp(token, "TIMESTAMP") != 0 &&
            strcasecmp(token, "BIGINT")   != 0 && strcasecmp(token, "DOUBLE")    != 0 &&
            strcasecmp(token, "BINARY")   != 0 && strcasecmp(token, "NCHAR")     != 0) {
            argp_error(state, "Invalid data_type!");
          }
          sptr[index++] = token;
          token = strsep(&running, ",");
          if (index >= MAX_NUM_DATATYPE) break;
        }
      }
      break;
    case 'w':
      arguments->len_of_binary = atoi(arg);
      break;
    case 'm':
      arguments->tb_prefix = arg;
      break;
    case 'M':
      arguments->use_metric = true;
      break;
    case 'x':
      arguments->insert_only = false;
      break;
    case 'c':
      if (wordexp(arg, &full_path, 0) != 0) {
        fprintf(stderr, "Invalid path %s\n", arg);
        return -1;
      }
      taos_options(TSDB_OPTION_CONFIGDIR, full_path.we_wordv[0]);
      wordfree(&full_path);
      break;  
    case 'O':
      arguments->disorderRatio = atoi(arg);
      if (arguments->disorderRatio < 0 || arguments->disorderRatio > 100)
      {
        argp_error(state, "Invalid disorder ratio, should 1 ~ 100!");
      }
      break;
    case 'R':
      arguments->disorderRange = atoi(arg);
      break;
    case 'a':
      arguments->replica = atoi(arg);
      if (arguments->replica > 3 || arguments->replica < 1)
      {
        arguments->replica = 1;
      }
      break;
    //case 'D':
    //  arguments->method_of_delete = atoi(arg);
    //  break;
    case OPT_ABORT:
      arguments->abort = 1;
      break;
    case ARGP_KEY_ARG:
      /*arguments->arg_list = &state->argv[state->next-1];
      state->next = state->argc;*/
      argp_usage(state);
      break;

    default:
      return ARGP_ERR_UNKNOWN;
  }
  return 0;
}

static struct argp argp = {options, parse_opt, 0, 0};

void parse_args(int argc, char *argv[], SArguments *arguments) {
  argp_parse(&argp, argc, argv, 0, 0, arguments);
  if (arguments->abort) {
    #ifndef _ALPINE
      error(10, 0, "ABORTED");
    #else
      abort();
    #endif
  }
}

#else
  void printHelp() {
    char indent[10] = "        ";
    printf("%s%s\n", indent, "-f");
    printf("%s%s%s\n", indent, indent, "The meta file to the execution procedure. Default is './meta.json'.");
    printf("%s%s\n", indent, "-c");
    printf("%s%s%s\n", indent, indent, "config_directory, Configuration directory. Default is '/etc/taos/'.");
  }

  void parse_args(int argc, char *argv[], SArguments *arguments) {
    for (int i = 1; i < argc; i++) {
      if (strcmp(argv[i], "-f") == 0) {
        arguments->metaFile = argv[++i];
      } else if (strcmp(argv[i], "-c") == 0) {
        strcpy(configDir, argv[++i]);
      } else if (strcmp(argv[i], "--help") == 0) {
        printHelp();
        exit(EXIT_FAILURE);
      } else {
        fprintf(stderr, "wrong options\n");
        printHelp();
        exit(EXIT_FAILURE);
      }
    }
  }
#endif

static bool getInfoFromJsonFile(char* file);
//static int generateOneRowDataForStb(SSuperTable* stbInfo);
//static int getDataIntoMemForStb(SSuperTable* stbInfo);
static void init_rand_data();
static int createDatabases();
static void createChildTables();
static int queryDbExec(TAOS *taos, char *command, int type);

/* ************ Global variables ************  */

int32_t  randint[MAX_PREPARED_RAND];
int64_t  randbigint[MAX_PREPARED_RAND];
float    randfloat[MAX_PREPARED_RAND];
double   randdouble[MAX_PREPARED_RAND];
char *aggreFunc[] = {"*", "count(*)", "avg(col0)", "sum(col0)", "max(col0)", "min(col0)", "first(col0)", "last(col0)"};

SArguments g_args = {NULL,
                     "127.0.0.1",     // host
                     6030,            // port
                     "root",          // user
                     #ifdef _TD_POWER_ 
                     "powerdb",      // password
                     #else
                     "taosdata",      // password
                     #endif
                     "test",          // database
                     1,               // replica
                     "t",             // tb_prefix
                     NULL,            // sqlFile
                     false,           // use_metric
                     true,            // insert_only
                     "./output.txt",  // output_file
                     0,               // mode : sync or async
                     {
                     "TINYINT",           // datatype
                     "SMALLINT",
                     "INT",
                     "BIGINT",
                     "FLOAT",
                     "DOUBLE",
                     "BINARY",
                     "NCHAR",
                     "BOOL",
                     "TIMESTAMP"
                     },
                     16,              // len_of_binary
                     10,              // num_of_CPR
                     10,              // num_of_connections/thread
                     100,             // num_of_RPR
                     10000,           // num_of_tables
                     10000,           // num_of_DPT
                     0,               // abort
                     0,               // disorderRatio
                     1000,            // disorderRange
                     1,               // method_of_delete
                     NULL             // arg_list
};


static int             g_jsonType = 0;
static SDbs            g_Dbs;
static int             g_totalChildTables = 0;
static SQueryMetaInfo  g_queryInfo;
static FILE *          g_fpOfInsertResult = NULL;


void tmfclose(FILE *fp) {
  if (NULL != fp) {
    fclose(fp);
  }
}

void tmfree(char *buf) {
  if (NULL != buf) {
    free(buf);
  }
}

static int queryDbExec(TAOS *taos, char *command, int type) {
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
    fprintf(stderr, "Failed to run %s, reason: %s\n", command, taos_errstr(res));
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

static void getResult(TAOS_RES *res, char* resultFileName) {  
  TAOS_ROW    row = NULL;
  int         num_rows = 0;
  int         num_fields = taos_field_count(res);
  TAOS_FIELD *fields     = taos_fetch_fields(res);

  FILE *fp = NULL;
  if (resultFileName[0] != 0) {
    fp = fopen(resultFileName, "at");
    if (fp == NULL) {
      fprintf(stderr, "failed to open result file: %s, result will not save to file\n", resultFileName);
    }
  }
  
  char* databuf = (char*) calloc(1, 100*1024*1024);
  if (databuf == NULL) {
    fprintf(stderr, "failed to malloc, warning: save result to file slowly!\n");
    return ;
  }

  int   totalLen = 0;
  char  temp[16000];

  // fetch the records row by row
  while ((row = taos_fetch_row(res))) {
    if (totalLen >= 100*1024*1024 - 32000) {
      if (fp) fprintf(fp, "%s", databuf);
      totalLen = 0;
      memset(databuf, 0, 100*1024*1024);
    }
    num_rows++;
    int len = taos_print_row(temp, row, fields, num_fields);
    len += sprintf(temp + len, "\n");
    //printf("query result:%s\n", temp);
    memcpy(databuf + totalLen, temp, len);
    totalLen += len;
  }

  if (fp) fprintf(fp, "%s", databuf);
  tmfclose(fp);
  free(databuf);
}

static void selectAndGetResult(TAOS *taos, char *command, char* resultFileName) {
  TAOS_RES *res = taos_query(taos, command);
  if (res == NULL || taos_errno(res) != 0) {
    printf("failed to sql:%s, reason:%s\n", command, taos_errstr(res));
    taos_free_result(res);
    return;
  }
  
  getResult(res, resultFileName);
  taos_free_result(res);
}

double getCurrentTime() {
  struct timeval tv;
  if (gettimeofday(&tv, NULL) != 0) {
    perror("Failed to get current time in ms");
    return 0.0;
  }

  return tv.tv_sec + tv.tv_usec / 1E6;
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

static const char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
void rand_string(char *str, int size) {
  str[0] = 0;
  if (size > 0) {
    //--size;
    int n;
    for (n = 0; n < size; n++) {
      int key = rand_tinyint() % (int)(sizeof(charset) - 1);
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
    randint[i] = (int)(rand() % 65535);
    randbigint[i] = (int64_t)(rand() % 2147483648);
    randfloat[i] = (float)(rand() / 1000.0);
    randdouble[i] = (double)(rand() / 1000000.0);
  }
}

static void printfInsertMeta() {
  printf("\033[1m\033[40;32m================ insert.json parse result START ================\033[0m\n");
  printf("host:                       \033[33m%s:%u\033[0m\n", g_Dbs.host, g_Dbs.port);
  printf("user:                       \033[33m%s\033[0m\n", g_Dbs.user);
  printf("password:                   \033[33m%s\033[0m\n", g_Dbs.password);
  printf("resultFile:                 \033[33m%s\033[0m\n", g_Dbs.resultFile);
  printf("thread num of insert data:  \033[33m%d\033[0m\n", g_Dbs.threadCount);
  printf("thread num of create table: \033[33m%d\033[0m\n", g_Dbs.threadCountByCreateTbl);

  printf("database count:             \033[33m%d\033[0m\n", g_Dbs.dbCount);
  for (int i = 0; i < g_Dbs.dbCount; i++) {
    printf("database[\033[33m%d\033[0m]:\n", i);
    printf("  database name:         \033[33m%s\033[0m\n", g_Dbs.db[i].dbName);
    if (0 == g_Dbs.db[i].drop) {
      printf("  drop:                  \033[33mno\033[0m\n");     
    }else {
      printf("  drop:                  \033[33myes\033[0m\n"); 
    }

    if (g_Dbs.db[i].dbCfg.blocks > 0) {
      printf("  blocks:                \033[33m%d\033[0m\n", g_Dbs.db[i].dbCfg.blocks);
    }
    if (g_Dbs.db[i].dbCfg.cache > 0) {
      printf("  cache:                 \033[33m%d\033[0m\n", g_Dbs.db[i].dbCfg.cache);
    }
    if (g_Dbs.db[i].dbCfg.days > 0) {
      printf("  days:                  \033[33m%d\033[0m\n", g_Dbs.db[i].dbCfg.days);
    }
    if (g_Dbs.db[i].dbCfg.keep > 0) {
      printf("  keep:                  \033[33m%d\033[0m\n", g_Dbs.db[i].dbCfg.keep);
    }
    if (g_Dbs.db[i].dbCfg.replica > 0) {
      printf("  replica:               \033[33m%d\033[0m\n", g_Dbs.db[i].dbCfg.replica);
    }
    if (g_Dbs.db[i].dbCfg.update > 0) {
      printf("  update:                \033[33m%d\033[0m\n", g_Dbs.db[i].dbCfg.update);
    }
    if (g_Dbs.db[i].dbCfg.minRows > 0) {
      printf("  minRows:               \033[33m%d\033[0m\n", g_Dbs.db[i].dbCfg.minRows);
    }
    if (g_Dbs.db[i].dbCfg.maxRows > 0) {
      printf("  maxRows:               \033[33m%d\033[0m\n", g_Dbs.db[i].dbCfg.maxRows);
    }
    if (g_Dbs.db[i].dbCfg.comp > 0) {
      printf("  comp:                  \033[33m%d\033[0m\n", g_Dbs.db[i].dbCfg.comp);
    }
    if (g_Dbs.db[i].dbCfg.walLevel > 0) {
      printf("  walLevel:              \033[33m%d\033[0m\n", g_Dbs.db[i].dbCfg.walLevel);
    }
    if (g_Dbs.db[i].dbCfg.fsync > 0) {
      printf("  fsync:                 \033[33m%d\033[0m\n", g_Dbs.db[i].dbCfg.fsync);
    }
    if (g_Dbs.db[i].dbCfg.quorum > 0) {
      printf("  quorum:                \033[33m%d\033[0m\n", g_Dbs.db[i].dbCfg.quorum);
    }
    if (g_Dbs.db[i].dbCfg.precision[0] != 0) {
      if ((0 == strncasecmp(g_Dbs.db[i].dbCfg.precision, "ms", 2)) || (0 == strncasecmp(g_Dbs.db[i].dbCfg.precision, "us", 2))) {
        printf("  precision:             \033[33m%s\033[0m\n", g_Dbs.db[i].dbCfg.precision);
      } else {
        printf("  precision error:       \033[33m%s\033[0m\n", g_Dbs.db[i].dbCfg.precision);
        exit(EXIT_FAILURE);
      }
    }

    printf("  super table count:     \033[33m%d\033[0m\n", g_Dbs.db[i].superTblCount);
    for (int j = 0; j < g_Dbs.db[i].superTblCount; j++) {
      printf("  super table[\033[33m%d\033[0m]:\n", j);
    
      printf("      stbName:           \033[33m%s\033[0m\n",  g_Dbs.db[i].superTbls[j].sTblName);   

      if (PRE_CREATE_SUBTBL == g_Dbs.db[i].superTbls[j].autoCreateTable) {
        printf("      autoCreateTable:   \033[33m%s\033[0m\n",  "no");
      } else if (AUTO_CREATE_SUBTBL == g_Dbs.db[i].superTbls[j].autoCreateTable) {
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
      
      printf("      childTblCount:     \033[33m%d\033[0m\n",  g_Dbs.db[i].superTbls[j].childTblCount);      
      printf("      childTblPrefix:    \033[33m%s\033[0m\n",  g_Dbs.db[i].superTbls[j].childTblPrefix);      
      printf("      dataSource:        \033[33m%s\033[0m\n",  g_Dbs.db[i].superTbls[j].dataSource);      
      printf("      insertMode:        \033[33m%s\033[0m\n",  g_Dbs.db[i].superTbls[j].insertMode);      
      printf("      insertRate:        \033[33m%d\033[0m\n",  g_Dbs.db[i].superTbls[j].insertRate);     
      printf("      insertRows:        \033[33m%"PRId64"\033[0m\n", g_Dbs.db[i].superTbls[j].insertRows); 

      if (0 == g_Dbs.db[i].superTbls[j].multiThreadWriteOneTbl) {
        printf("      multiThreadWriteOneTbl:  \033[33mno\033[0m\n");     
      }else {
        printf("      multiThreadWriteOneTbl:  \033[33myes\033[0m\n"); 
      }
      printf("      numberOfTblInOneSql:     \033[33m%d\033[0m\n",  g_Dbs.db[i].superTbls[j].numberOfTblInOneSql);     
      printf("      rowsPerTbl:        \033[33m%d\033[0m\n",  g_Dbs.db[i].superTbls[j].rowsPerTbl);     
      printf("      disorderRange:     \033[33m%d\033[0m\n",  g_Dbs.db[i].superTbls[j].disorderRange);     
      printf("      disorderRatio:     \033[33m%d\033[0m\n",  g_Dbs.db[i].superTbls[j].disorderRatio);
      printf("      maxSqlLen:         \033[33m%d\033[0m\n",  g_Dbs.db[i].superTbls[j].maxSqlLen);     
      
      printf("      timeStampStep:     \033[33m%d\033[0m\n",  g_Dbs.db[i].superTbls[j].timeStampStep);      
      printf("      startTimestamp:    \033[33m%s\033[0m\n",  g_Dbs.db[i].superTbls[j].startTimestamp);             
      printf("      sampleFormat:      \033[33m%s\033[0m\n",  g_Dbs.db[i].superTbls[j].sampleFormat);
      printf("      sampleFile:        \033[33m%s\033[0m\n",  g_Dbs.db[i].superTbls[j].sampleFile); 
      printf("      tagsFile:          \033[33m%s\033[0m\n",  g_Dbs.db[i].superTbls[j].tagsFile);   
    
      printf("      columnCount:       \033[33m%d\033[0m\n        ",  g_Dbs.db[i].superTbls[j].columnCount);
      for (int k = 0; k < g_Dbs.db[i].superTbls[j].columnCount; k++) {
        //printf("dataType:%s, dataLen:%d\t", g_Dbs.db[i].superTbls[j].columns[k].dataType, g_Dbs.db[i].superTbls[j].columns[k].dataLen);
        if ((0 == strncasecmp(g_Dbs.db[i].superTbls[j].columns[k].dataType, "binary", 6)) || (0 == strncasecmp(g_Dbs.db[i].superTbls[j].columns[k].dataType, "nchar", 5))) {
          printf("column[\033[33m%d\033[0m]:\033[33m%s(%d)\033[0m ", k, g_Dbs.db[i].superTbls[j].columns[k].dataType, g_Dbs.db[i].superTbls[j].columns[k].dataLen);
        } else {
          printf("column[%d]:\033[33m%s\033[0m ", k, g_Dbs.db[i].superTbls[j].columns[k].dataType);
        }
      }
      printf("\n");
      
      printf("      tagCount:            \033[33m%d\033[0m\n        ",  g_Dbs.db[i].superTbls[j].tagCount);
      for (int k = 0; k < g_Dbs.db[i].superTbls[j].tagCount; k++) {
        //printf("dataType:%s, dataLen:%d\t", g_Dbs.db[i].superTbls[j].tags[k].dataType, g_Dbs.db[i].superTbls[j].tags[k].dataLen);
        if ((0 == strncasecmp(g_Dbs.db[i].superTbls[j].tags[k].dataType, "binary", 6)) || (0 == strncasecmp(g_Dbs.db[i].superTbls[j].tags[k].dataType, "nchar", 5))) {
          printf("tag[%d]:\033[33m%s(%d)\033[0m ", k, g_Dbs.db[i].superTbls[j].tags[k].dataType, g_Dbs.db[i].superTbls[j].tags[k].dataLen);
        } else {
          printf("tag[%d]:\033[33m%s\033[0m ", k, g_Dbs.db[i].superTbls[j].tags[k].dataType);
        }     
      }
      printf("\n");
    }
    printf("\n");
  }
  printf("\033[1m\033[40;32m================ insert.json parse result END================\033[0m\n");
}

static void printfInsertMetaToFile(FILE* fp) {
  fprintf(fp, "================ insert.json parse result START================\n");
  fprintf(fp, "host:                       %s:%u\n", g_Dbs.host, g_Dbs.port);
  fprintf(fp, "user:                       %s\n", g_Dbs.user);
  fprintf(fp, "password:                   %s\n", g_Dbs.password);
  fprintf(fp, "resultFile:                 %s\n", g_Dbs.resultFile);
  fprintf(fp, "thread num of insert data:  %d\n", g_Dbs.threadCount);
  fprintf(fp, "thread num of create table: %d\n", g_Dbs.threadCountByCreateTbl)

  fprintf(fp, "database count:          %d\n", g_Dbs.dbCount);
  for (int i = 0; i < g_Dbs.dbCount; i++) {
    fprintf(fp, "database[%d]:\n", i);
    fprintf(fp, "  database name:         %s\n", g_Dbs.db[i].dbName);
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
      if ((0 == strncasecmp(g_Dbs.db[i].dbCfg.precision, "ms", 2)) || (0 == strncasecmp(g_Dbs.db[i].dbCfg.precision, "us", 2))) {
        fprintf(fp, "  precision:             %s\n", g_Dbs.db[i].dbCfg.precision);
      } else {
        fprintf(fp, "  precision error:       %s\n", g_Dbs.db[i].dbCfg.precision);
      }
    }

    fprintf(fp, "  super table count:     %d\n", g_Dbs.db[i].superTblCount);
    for (int j = 0; j < g_Dbs.db[i].superTblCount; j++) {
      fprintf(fp, "  super table[%d]:\n", j);
    
      fprintf(fp, "      stbName:           %s\n",  g_Dbs.db[i].superTbls[j].sTblName);   

      if (PRE_CREATE_SUBTBL == g_Dbs.db[i].superTbls[j].autoCreateTable) {
        fprintf(fp, "      autoCreateTable:   %s\n",  "no");
      } else if (AUTO_CREATE_SUBTBL == g_Dbs.db[i].superTbls[j].autoCreateTable) {
        fprintf(fp, "      autoCreateTable:   %s\n",  "yes");
      } else {
        fprintf(fp, "      autoCreateTable:   %s\n",  "error");
      }
      
      if (TBL_NO_EXISTS == g_Dbs.db[i].superTbls[j].childTblExists) {
        fprintf(fp, "      childTblExists:    %s\n",  "no");
      } else if (TBL_ALREADY_EXISTS == g_Dbs.db[i].superTbls[j].childTblExists) {
        fprintf(fp, "      childTblExists:    %s\n",  "yes");
      } else {
        fprintf(fp, "      childTblExists:    %s\n",  "error");
      }
      
      fprintf(fp, "      childTblCount:     %d\n",  g_Dbs.db[i].superTbls[j].childTblCount);      
      fprintf(fp, "      childTblPrefix:    %s\n",  g_Dbs.db[i].superTbls[j].childTblPrefix);      
      fprintf(fp, "      dataSource:        %s\n",  g_Dbs.db[i].superTbls[j].dataSource);      
      fprintf(fp, "      insertMode:        %s\n",  g_Dbs.db[i].superTbls[j].insertMode);      
      fprintf(fp, "      insertRate:        %d\n",  g_Dbs.db[i].superTbls[j].insertRate);     
      fprintf(fp, "      insertRows:        %"PRId64"\n", g_Dbs.db[i].superTbls[j].insertRows); 

      if (0 == g_Dbs.db[i].superTbls[j].multiThreadWriteOneTbl) {
        fprintf(fp, "      multiThreadWriteOneTbl:  no\n");     
      }else {
        fprintf(fp, "      multiThreadWriteOneTbl:  yes\n"); 
      }
      fprintf(fp, "      numberOfTblInOneSql:     %d\n",  g_Dbs.db[i].superTbls[j].numberOfTblInOneSql);     
      fprintf(fp, "      rowsPerTbl:        %d\n",  g_Dbs.db[i].superTbls[j].rowsPerTbl);     
      fprintf(fp, "      disorderRange:     %d\n",  g_Dbs.db[i].superTbls[j].disorderRange);     
      fprintf(fp, "      disorderRatio:     %d\n",  g_Dbs.db[i].superTbls[j].disorderRatio);
      fprintf(fp, "      maxSqlLen:         %d\n",  g_Dbs.db[i].superTbls[j].maxSqlLen);     
      
      fprintf(fp, "      timeStampStep:     %d\n",  g_Dbs.db[i].superTbls[j].timeStampStep);      
      fprintf(fp, "      startTimestamp:    %s\n",  g_Dbs.db[i].superTbls[j].startTimestamp);             
      fprintf(fp, "      sampleFormat:      %s\n",  g_Dbs.db[i].superTbls[j].sampleFormat);
      fprintf(fp, "      sampleFile:        %s\n",  g_Dbs.db[i].superTbls[j].sampleFile); 
      fprintf(fp, "      tagsFile:          %s\n",  g_Dbs.db[i].superTbls[j].tagsFile);   
    
      fprintf(fp, "      columnCount:       %d\n        ",  g_Dbs.db[i].superTbls[j].columnCount);
      for (int k = 0; k < g_Dbs.db[i].superTbls[j].columnCount; k++) {
        //printf("dataType:%s, dataLen:%d\t", g_Dbs.db[i].superTbls[j].columns[k].dataType, g_Dbs.db[i].superTbls[j].columns[k].dataLen);
        if ((0 == strncasecmp(g_Dbs.db[i].superTbls[j].columns[k].dataType, "binary", 6)) || (0 == strncasecmp(g_Dbs.db[i].superTbls[j].columns[k].dataType, "nchar", 5))) {
          fprintf(fp, "column[%d]:%s(%d) ", k, g_Dbs.db[i].superTbls[j].columns[k].dataType, g_Dbs.db[i].superTbls[j].columns[k].dataLen);
        } else {
          fprintf(fp, "column[%d]:%s ", k, g_Dbs.db[i].superTbls[j].columns[k].dataType);
        }
      }
      fprintf(fp, "\n");
      
      fprintf(fp, "      tagCount:            %d\n        ",  g_Dbs.db[i].superTbls[j].tagCount);
      for (int k = 0; k < g_Dbs.db[i].superTbls[j].tagCount; k++) {
        //printf("dataType:%s, dataLen:%d\t", g_Dbs.db[i].superTbls[j].tags[k].dataType, g_Dbs.db[i].superTbls[j].tags[k].dataLen);
        if ((0 == strncasecmp(g_Dbs.db[i].superTbls[j].tags[k].dataType, "binary", 6)) || (0 == strncasecmp(g_Dbs.db[i].superTbls[j].tags[k].dataType, "nchar", 5))) {
          fprintf(fp, "tag[%d]:%s(%d) ", k, g_Dbs.db[i].superTbls[j].tags[k].dataType, g_Dbs.db[i].superTbls[j].tags[k].dataLen);
        } else {
          fprintf(fp, "tag[%d]:%s ", k, g_Dbs.db[i].superTbls[j].tags[k].dataType);
        }     
      }
      fprintf(fp, "\n");
    }
    fprintf(fp, "\n");
  }
  fprintf(fp, "================ insert.json parse result END ================\n\n");
}

static void printfQueryMeta() {
  printf("\033[1m\033[40;32m================ query.json parse result ================\033[0m\n");
  printf("host:                    \033[33m%s:%u\033[0m\n", g_queryInfo.host, g_queryInfo.port);
  printf("user:                    \033[33m%s\033[0m\n", g_queryInfo.user);
  printf("password:                \033[33m%s\033[0m\n", g_queryInfo.password);
  printf("database name:           \033[33m%s\033[0m\n", g_queryInfo.dbName);

  printf("\n");
  printf("specified table query info:                   \n");  
  printf("query interval: \033[33m%d\033[0m\n", g_queryInfo.superQueryInfo.rate);
  printf("concurrent:     \033[33m%d\033[0m\n", g_queryInfo.superQueryInfo.concurrent);
  printf("sqlCount:       \033[33m%d\033[0m\n", g_queryInfo.superQueryInfo.sqlCount); 

  if (SUBSCRIBE_MODE == g_jsonType) {
    printf("mod:            \033[33m%d\033[0m\n", g_queryInfo.superQueryInfo.subscribeMode);
    printf("interval:       \033[33m%d\033[0m\n", g_queryInfo.superQueryInfo.subscribeInterval);
    printf("restart:        \033[33m%d\033[0m\n", g_queryInfo.superQueryInfo.subscribeRestart);
    printf("keepProgress:   \033[33m%d\033[0m\n", g_queryInfo.superQueryInfo.subscribeKeepProgress);
  }

  
  for (int i = 0; i < g_queryInfo.superQueryInfo.sqlCount; i++) {
    printf("  sql[%d]: \033[33m%s\033[0m\n", i, g_queryInfo.superQueryInfo.sql[i]);
  }
  printf("\n");
  printf("super table query info:                   \n");  
  printf("query interval: \033[33m%d\033[0m\n", g_queryInfo.subQueryInfo.rate);
  printf("threadCnt:      \033[33m%d\033[0m\n", g_queryInfo.subQueryInfo.threadCnt);
  printf("childTblCount:  \033[33m%d\033[0m\n", g_queryInfo.subQueryInfo.childTblCount);
  printf("stable name:    \033[33m%s\033[0m\n", g_queryInfo.subQueryInfo.sTblName);

  if (SUBSCRIBE_MODE == g_jsonType) {
    printf("mod:            \033[33m%d\033[0m\n", g_queryInfo.subQueryInfo.subscribeMode);
    printf("interval:       \033[33m%d\033[0m\n", g_queryInfo.subQueryInfo.subscribeInterval);
    printf("restart:        \033[33m%d\033[0m\n", g_queryInfo.subQueryInfo.subscribeRestart);
    printf("keepProgress:   \033[33m%d\033[0m\n", g_queryInfo.subQueryInfo.subscribeKeepProgress);
  }
  
  printf("sqlCount:       \033[33m%d\033[0m\n", g_queryInfo.subQueryInfo.sqlCount);  
  for (int i = 0; i < g_queryInfo.subQueryInfo.sqlCount; i++) {
    printf("  sql[%d]: \033[33m%s\033[0m\n", i, g_queryInfo.subQueryInfo.sql[i]);
  }  
  printf("\n");
  printf("\033[1m\033[40;32m================ query.json parse result ================\033[0m\n");
}

#ifdef TD_LOWA_CURL
static size_t responseCallback(void *contents, size_t size, size_t nmemb, void *userp)
{
  size_t realsize = size * nmemb;
  curlMemInfo* mem = (curlMemInfo*)userp;
 
  char *ptr = realloc(mem->buf, mem->sizeleft + realsize + 1);
  if(ptr == NULL) {
    /* out of memory! */ 
    printf("not enough memory (realloc returned NULL)\n");
    return 0;
  }
 
  mem->buf = ptr;
  memcpy(&(mem->buf[mem->sizeleft]), contents, realsize);
  mem->sizeleft += realsize;
  mem->buf[mem->sizeleft] = 0;

  //printf("result:%s\n\n", mem->buf);
 
  return realsize;
}

void curlProceLogin(void)
{
  CURL *curl_handle;
  CURLcode res;
 
  curlMemInfo chunk;
 
  chunk.buf = malloc(1);  /* will be grown as needed by the realloc above */ 
  chunk.sizeleft = 0;    /* no data at this point */ 
 
  //curl_global_init(CURL_GLOBAL_ALL);
 
  /* init the curl session */ 
  curl_handle = curl_easy_init();

  curl_easy_setopt(curl_handle,CURLOPT_POSTFIELDS,"");
  curl_easy_setopt(curl_handle, CURLOPT_POST, 1);

  char dstUrl[128] = {0};
  snprintf(dstUrl, 128, "http://%s:6041/rest/login/root/taosdata", g_Dbs.host);
        
  /* specify URL to get */ 
  curl_easy_setopt(curl_handle, CURLOPT_URL, dstUrl);
 
  /* send all data to this function  */ 
  curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, responseCallback);
 
  /* we pass our 'chunk' struct to the callback function */ 
  curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, (void *)&chunk);
 
  /* do it! */ 
  res = curl_easy_perform(curl_handle);
 
  /* check for errors */ 
  if(res != CURLE_OK) {
    fprintf(stderr, "curl_easy_perform() failed: %s\n", curl_easy_strerror(res));
  }
  else {
    //printf("response len:%lu, content: %s \n", (unsigned long)chunk.sizeleft, chunk.buf);
    ;
  }
 
  /* cleanup curl stuff */ 
  curl_easy_cleanup(curl_handle);
 
  free(chunk.buf);
 
  /* we're done with libcurl, so clean it up */ 
  //curl_global_cleanup();
 
  return;
}

int curlProceSql(char* host, uint16_t port, char* sqlstr, CURL *curl_handle)
{
  //curlProceLogin();

  //CURL *curl_handle;
  CURLcode res;
 
  curlMemInfo chunk;
 
  chunk.buf = malloc(1);  /* will be grown as needed by the realloc above */ 
  chunk.sizeleft = 0;    /* no data at this point */ 

  
  char dstUrl[128] = {0};
  snprintf(dstUrl, 128, "http://%s:%u/rest/sql", host, port+TSDB_PORT_HTTP);
        
  //curl_global_init(CURL_GLOBAL_ALL);
 
  /* init the curl session */ 
  //curl_handle = curl_easy_init();
 
  //curl_easy_setopt(curl_handle,CURLOPT_POSTFIELDS,"");
  curl_easy_setopt(curl_handle, CURLOPT_POST, 1L);
  
  /* specify URL to get */ 
  curl_easy_setopt(curl_handle, CURLOPT_URL, dstUrl);

  /* enable TCP keep-alive for this transfer */
  curl_easy_setopt(curl_handle, CURLOPT_TCP_KEEPALIVE, 1L);
  /* keep-alive idle time to 120 seconds */
  curl_easy_setopt(curl_handle, CURLOPT_TCP_KEEPIDLE, 120L);
  /* interval time between keep-alive probes: 60 seconds */
  curl_easy_setopt(curl_handle, CURLOPT_TCP_KEEPINTVL, 60L);
  
  /* send all data to this function  */ 
  curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, responseCallback);
 
  /* we pass our 'chunk' struct to the callback function */ 
  curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, (void *)&chunk);

  struct curl_slist *list = NULL;
  list = curl_slist_append(list, "Authorization: Basic cm9vdDp0YW9zZGF0YQ==");
  curl_easy_setopt(curl_handle, CURLOPT_HTTPHEADER, list);
  curl_easy_setopt(curl_handle, CURLOPT_HTTPHEADER, list);

  /* Set the expected upload size. */ 
  curl_easy_setopt(curl_handle, CURLOPT_POSTFIELDSIZE_LARGE, (curl_off_t)strlen(sqlstr));
  curl_easy_setopt(curl_handle, CURLOPT_POSTFIELDS, sqlstr);

  /* get it! */ 
  res = curl_easy_perform(curl_handle);
 
  /* check for errors */ 
  if(res != CURLE_OK) {
    fprintf(stderr, "curl_easy_perform() failed: %s\n", curl_easy_strerror(res));
    return -1;
  }
  else {
    /* curl_easy_perform() block end and return result */  
    //printf("[%32.32s] sql response len:%lu, content: %s \n\n", sqlstr, (unsigned long)chunk.sizeleft, chunk.buf);
    ;
  }

  curl_slist_free_all(list); /* free the list again */
  
  /* cleanup curl stuff */ 
  //curl_easy_cleanup(curl_handle);
 
  free(chunk.buf);
 
  /* we're done with libcurl, so clean it up */ 
  //curl_global_cleanup();
 
  return 0;
}
#endif

char* getTagValueFromTagSample(        SSuperTable* stbInfo, int tagUsePos) {
  char*  dataBuf = (char*)calloc(TSDB_MAX_SQL_LEN+1, 1);
  if (NULL == dataBuf) {
    printf("calloc failed! size:%d\n", TSDB_MAX_SQL_LEN+1);
    return NULL;
  }
  
  int    dataLen = 0;
  dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen, "(%s)", stbInfo->tagDataBuf + stbInfo->lenOfTagOfOneRow * tagUsePos);
  
  return dataBuf;
}

char* generateTagVaulesForStb(SSuperTable* stbInfo) {
  char*  dataBuf = (char*)calloc(TSDB_MAX_SQL_LEN+1, 1);
  if (NULL == dataBuf) {
    printf("calloc failed! size:%d\n", TSDB_MAX_SQL_LEN+1);
    return NULL;
  }

  int    dataLen = 0;
  dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen, "(");
  for (int i = 0; i < stbInfo->tagCount; i++) {
    if ((0 == strncasecmp(stbInfo->tags[i].dataType, "binary", 6)) || (0 == strncasecmp(stbInfo->tags[i].dataType, "nchar", 5))) {
      if (stbInfo->tags[i].dataLen > TSDB_MAX_BINARY_LEN) {
        printf("binary or nchar length overflow, max size:%u\n", (uint32_t)TSDB_MAX_BINARY_LEN);
        tmfree(dataBuf);
        return NULL;
      }
    
      char* buf = (char*)calloc(stbInfo->tags[i].dataLen+1, 1);
      if (NULL == buf) {
        printf("calloc failed! size:%d\n", stbInfo->tags[i].dataLen);
        tmfree(dataBuf);
        return NULL;
      }
      rand_string(buf, stbInfo->tags[i].dataLen);
      dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen, "\'%s\', ", buf);
      tmfree(buf);
    } else if (0 == strncasecmp(stbInfo->tags[i].dataType, "int", 3)) {
      dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen, "%d, ", rand_int());
    } else if (0 == strncasecmp(stbInfo->tags[i].dataType, "bigint", 6)) {
      dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen, "%"PRId64", ", rand_bigint());
    }  else if (0 == strncasecmp(stbInfo->tags[i].dataType, "float", 5)) {
      dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen, "%f, ", rand_float());
    }  else if (0 == strncasecmp(stbInfo->tags[i].dataType, "double", 6)) {
      dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen, "%f, ", rand_double());
    }  else if (0 == strncasecmp(stbInfo->tags[i].dataType, "smallint", 8)) {
      dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen, "%d, ", rand_smallint());
    }  else if (0 == strncasecmp(stbInfo->tags[i].dataType, "tinyint", 7)) {
      dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen, "%d, ", rand_tinyint());
    }  else if (0 == strncasecmp(stbInfo->tags[i].dataType, "bool", 4)) {
      dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen, "%d, ", rand_bool());
    }  else if (0 == strncasecmp(stbInfo->tags[i].dataType, "timestamp", 4)) {
      dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen, "%"PRId64", ", rand_bigint());
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


static int getAllChildNameOfSuperTable(TAOS * taos, char* dbName, char* sTblName, char** childTblNameOfSuperTbl, int* childTblCountOfSuperTbl) {  
  char command[BUFFER_SIZE] = "\0";
  TAOS_RES * res;  
  TAOS_ROW row = NULL;

  char* childTblName = *childTblNameOfSuperTbl;
  
  //get all child table name use cmd: select tbname from superTblName;  
  snprintf(command, BUFFER_SIZE, "select tbname from %s.%s", dbName, sTblName);
  res = taos_query(taos, command);  
  int32_t code = taos_errno(res);
  if (code != 0) {
    printf("failed to run command %s\n", command);
    taos_free_result(res);
    taos_close(taos);
    exit(-1);
  }

  int childTblCount = 10000;
  int count = 0;
  childTblName = (char*)calloc(1, childTblCount * TSDB_TABLE_NAME_LEN);
  char* pTblName = childTblName;
  while ((row = taos_fetch_row(res)) != NULL) {
    int32_t* len = taos_fetch_lengths(res);
    strncpy(pTblName, (char *)row[0], len[0]);
    //printf("==== sub table name: %s\n", pTblName);
    count++;
    if (count >= childTblCount - 1) {
      char *tmp = realloc(childTblName, (size_t)childTblCount*1.5*TSDB_TABLE_NAME_LEN+1);
      if (tmp != NULL) {
        childTblName = tmp;
        childTblCount = (int)(childTblCount*1.5);
        memset(childTblName + count*TSDB_TABLE_NAME_LEN, 0, (size_t)((childTblCount-count)*TSDB_TABLE_NAME_LEN));
      } else {
        // exit, if allocate more memory failed
        printf("realloc fail for save child table name of %s.%s\n", dbName, sTblName);
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

static int getSuperTableFromServer(TAOS * taos, char* dbName, SSuperTable*  superTbls) {  
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
  while ((row = taos_fetch_row(res)) != NULL) {
    if (0 == count) {
      count++;
      continue;
    }    

    if (strcmp((char *)row[TSDB_DESCRIBE_METRIC_NOTE_INDEX], "TAG") == 0) {
      strncpy(superTbls->tags[tagIndex].field, (char *)row[TSDB_DESCRIBE_METRIC_FIELD_INDEX], fields[TSDB_DESCRIBE_METRIC_FIELD_INDEX].bytes);
      strncpy(superTbls->tags[tagIndex].dataType, (char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX], fields[TSDB_DESCRIBE_METRIC_TYPE_INDEX].bytes);
      superTbls->tags[tagIndex].dataLen = *((int *)row[TSDB_DESCRIBE_METRIC_LENGTH_INDEX]);
      strncpy(superTbls->tags[tagIndex].note, (char *)row[TSDB_DESCRIBE_METRIC_NOTE_INDEX], fields[TSDB_DESCRIBE_METRIC_NOTE_INDEX].bytes);
      tagIndex++;
    } else {    
      strncpy(superTbls->columns[columnIndex].field, (char *)row[TSDB_DESCRIBE_METRIC_FIELD_INDEX], fields[TSDB_DESCRIBE_METRIC_FIELD_INDEX].bytes);
      strncpy(superTbls->columns[columnIndex].dataType, (char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX], fields[TSDB_DESCRIBE_METRIC_TYPE_INDEX].bytes);
      superTbls->columns[columnIndex].dataLen = *((int *)row[TSDB_DESCRIBE_METRIC_LENGTH_INDEX]);
      strncpy(superTbls->columns[columnIndex].note, (char *)row[TSDB_DESCRIBE_METRIC_NOTE_INDEX], fields[TSDB_DESCRIBE_METRIC_NOTE_INDEX].bytes);
      columnIndex++;
    }
    count++;
  }

  superTbls->columnCount = columnIndex;
  superTbls->tagCount    = tagIndex;
  taos_free_result(res);

  calcRowLen(superTbls);

  if (TBL_ALREADY_EXISTS == superTbls->childTblExists) {
    //get all child table name use cmd: select tbname from superTblName;  
    getAllChildNameOfSuperTable(taos, dbName, superTbls->sTblName, &superTbls->childTblName, &superTbls->childTblCount);  
  }
  return 0;
}

static int createSuperTable(TAOS * taos, char* dbName, SSuperTable*  superTbls, bool use_metric) {  
  char command[BUFFER_SIZE] = "\0";
    
  char cols[STRING_LEN] = "\0";
  int colIndex;
  int len = 0;

  int  lenOfOneRow = 0;
  for (colIndex = 0; colIndex < superTbls->columnCount; colIndex++) {
    char* dataType = superTbls->columns[colIndex].dataType;
    
    if (strcasecmp(dataType, "BINARY") == 0) {
      len += snprintf(cols + len, STRING_LEN - len, ", col%d %s(%d)", colIndex, "BINARY", superTbls->columns[colIndex].dataLen);
      lenOfOneRow += superTbls->columns[colIndex].dataLen + 3;
    } else if (strcasecmp(dataType, "NCHAR") == 0) {
      len += snprintf(cols + len, STRING_LEN - len, ", col%d %s(%d)", colIndex, "NCHAR", superTbls->columns[colIndex].dataLen);
      lenOfOneRow += superTbls->columns[colIndex].dataLen + 3;
    } else if (strcasecmp(dataType, "INT") == 0)  {
      len += snprintf(cols + len, STRING_LEN - len, ", col%d %s", colIndex, "INT");
      lenOfOneRow += 11;
    } else if (strcasecmp(dataType, "BIGINT") == 0)  {
      len += snprintf(cols + len, STRING_LEN - len, ", col%d %s", colIndex, "BIGINT");
      lenOfOneRow += 21;
    } else if (strcasecmp(dataType, "SMALLINT") == 0)  {
      len += snprintf(cols + len, STRING_LEN - len, ", col%d %s", colIndex, "SMALLINT");
      lenOfOneRow += 6;
    } else if (strcasecmp(dataType, "TINYINT") == 0)  {
      len += snprintf(cols + len, STRING_LEN - len, ", col%d %s", colIndex, "TINYINT");
      lenOfOneRow += 4;
    } else if (strcasecmp(dataType, "BOOL") == 0)  {
      len += snprintf(cols + len, STRING_LEN - len, ", col%d %s", colIndex, "BOOL");
      lenOfOneRow += 6;
    } else if (strcasecmp(dataType, "FLOAT") == 0) {
      len += snprintf(cols + len, STRING_LEN - len, ", col%d %s", colIndex, "FLOAT");
      lenOfOneRow += 22;
    } else if (strcasecmp(dataType, "DOUBLE") == 0) { 
      len += snprintf(cols + len, STRING_LEN - len, ", col%d %s", colIndex, "DOUBLE");
      lenOfOneRow += 42;
    }  else if (strcasecmp(dataType, "TIMESTAMP") == 0) { 
      len += snprintf(cols + len, STRING_LEN - len, ", col%d %s", colIndex, "TIMESTAMP");
      lenOfOneRow += 21;
    } else {
      taos_close(taos);
      printf("config error data type : %s\n", dataType);
      exit(-1);
    }
  }

  superTbls->lenOfOneRow = lenOfOneRow + 20; // timestamp
  //printf("%s.%s column count:%d, column length:%d\n\n", g_Dbs.db[i].dbName, g_Dbs.db[i].superTbls[j].sTblName, g_Dbs.db[i].superTbls[j].columnCount, lenOfOneRow);

  // save for creating child table
  superTbls->colsOfCreatChildTable = (char*)calloc(len+20, 1);
  if (NULL == superTbls->colsOfCreatChildTable) {
    printf("Failed when calloc, size:%d", len+1);
    taos_close(taos);
    exit(-1);
  }
  snprintf(superTbls->colsOfCreatChildTable, len+20, "(ts timestamp%s)", cols);

  if (use_metric) {
    char tags[STRING_LEN] = "\0";
    int tagIndex;
    len = 0;

    int lenOfTagOfOneRow = 0;
    len += snprintf(tags + len, STRING_LEN - len, "(");
    for (tagIndex = 0; tagIndex < superTbls->tagCount; tagIndex++) {
      char* dataType = superTbls->tags[tagIndex].dataType;
      
      if (strcasecmp(dataType, "BINARY") == 0) {
        len += snprintf(tags + len, STRING_LEN - len, "t%d %s(%d), ", tagIndex, "BINARY", superTbls->tags[tagIndex].dataLen);
        lenOfTagOfOneRow += superTbls->tags[tagIndex].dataLen + 3;
      } else if (strcasecmp(dataType, "NCHAR") == 0) {
        len += snprintf(tags + len, STRING_LEN - len, "t%d %s(%d), ", tagIndex, "NCHAR", superTbls->tags[tagIndex].dataLen);
        lenOfTagOfOneRow += superTbls->tags[tagIndex].dataLen + 3;
      } else if (strcasecmp(dataType, "INT") == 0)  {
        len += snprintf(tags + len, STRING_LEN - len, "t%d %s, ", tagIndex, "INT");
        lenOfTagOfOneRow += superTbls->tags[tagIndex].dataLen + 11;
      } else if (strcasecmp(dataType, "BIGINT") == 0)  {
        len += snprintf(tags + len, STRING_LEN - len, "t%d %s, ", tagIndex, "BIGINT");
        lenOfTagOfOneRow += superTbls->tags[tagIndex].dataLen + 21;
      } else if (strcasecmp(dataType, "SMALLINT") == 0)  {
        len += snprintf(tags + len, STRING_LEN - len, "t%d %s, ", tagIndex, "SMALLINT");
        lenOfTagOfOneRow += superTbls->tags[tagIndex].dataLen + 6;
      } else if (strcasecmp(dataType, "TINYINT") == 0)  {
        len += snprintf(tags + len, STRING_LEN - len, "t%d %s, ", tagIndex, "TINYINT");
        lenOfTagOfOneRow += superTbls->tags[tagIndex].dataLen + 4;
      } else if (strcasecmp(dataType, "BOOL") == 0)  {
        len += snprintf(tags + len, STRING_LEN - len, "t%d %s, ", tagIndex, "BOOL");
        lenOfTagOfOneRow += superTbls->tags[tagIndex].dataLen + 6;
      } else if (strcasecmp(dataType, "FLOAT") == 0) {
        len += snprintf(tags + len, STRING_LEN - len, "t%d %s, ", tagIndex, "FLOAT");
        lenOfTagOfOneRow += superTbls->tags[tagIndex].dataLen + 22;
      } else if (strcasecmp(dataType, "DOUBLE") == 0) { 
        len += snprintf(tags + len, STRING_LEN - len, "t%d %s, ", tagIndex, "DOUBLE");
        lenOfTagOfOneRow += superTbls->tags[tagIndex].dataLen + 42;
      } else {
        taos_close(taos);
        printf("config error tag type : %s\n", dataType);
        exit(-1);
      }
    }
    len -= 2;
    len += snprintf(tags + len, STRING_LEN - len, ")");

    superTbls->lenOfTagOfOneRow = lenOfTagOfOneRow;
    
    snprintf(command, BUFFER_SIZE, "create table if not exists %s.%s (ts timestamp%s) tags %s", dbName, superTbls->sTblName, cols, tags);
    if (0 != queryDbExec(taos, command, NO_INSERT_TYPE)) {
      return -1;
    }
    printf("\ncreate supertable %s success!\n\n", superTbls->sTblName);
  }
  return 0;
}


static int createDatabases() {
  TAOS * taos = NULL;
  int    ret = 0;
  taos_init();
  taos = taos_connect(g_Dbs.host, g_Dbs.user, g_Dbs.password, NULL, g_Dbs.port);
  if (taos == NULL) {
    fprintf(stderr, "Failed to connect to TDengine, reason:%s\n", taos_errstr(NULL));
    exit(-1);
  }
  char command[BUFFER_SIZE] = "\0";


  for (int i = 0; i < g_Dbs.dbCount; i++) {   
    if (g_Dbs.db[i].drop) {
      sprintf(command, "drop database if exists %s;", g_Dbs.db[i].dbName);
      if (0 != queryDbExec(taos, command, NO_INSERT_TYPE)) {
        taos_close(taos);
        return -1;
      }
    }
    
    int dataLen = 0;
    dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen, "create database if not exists %s ", g_Dbs.db[i].dbName);

    if (g_Dbs.db[i].dbCfg.blocks > 0) {
      dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen, "blocks %d ", g_Dbs.db[i].dbCfg.blocks);
    }
    if (g_Dbs.db[i].dbCfg.cache > 0) {
      dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen, "cache %d ", g_Dbs.db[i].dbCfg.cache);
    }
    if (g_Dbs.db[i].dbCfg.days > 0) {
      dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen, "days %d ", g_Dbs.db[i].dbCfg.days);
    }
    if (g_Dbs.db[i].dbCfg.keep > 0) {
      dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen, "keep %d ", g_Dbs.db[i].dbCfg.keep);
    }
    if (g_Dbs.db[i].dbCfg.replica > 0) {
      dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen, "replica %d ", g_Dbs.db[i].dbCfg.replica);
    }
    if (g_Dbs.db[i].dbCfg.update > 0) {
      dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen, "update %d ", g_Dbs.db[i].dbCfg.update);
    }
    //if (g_Dbs.db[i].dbCfg.maxtablesPerVnode > 0) {
    //  dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen, "tables %d ", g_Dbs.db[i].dbCfg.maxtablesPerVnode);
    //}
    if (g_Dbs.db[i].dbCfg.minRows > 0) {
      dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen, "minrows %d ", g_Dbs.db[i].dbCfg.minRows);
    }
    if (g_Dbs.db[i].dbCfg.maxRows > 0) {
      dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen, "maxrows %d ", g_Dbs.db[i].dbCfg.maxRows);
    }
    if (g_Dbs.db[i].dbCfg.comp > 0) {
      dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen, "comp %d ", g_Dbs.db[i].dbCfg.comp);
    }
    if (g_Dbs.db[i].dbCfg.walLevel > 0) {
      dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen, "wal %d ", g_Dbs.db[i].dbCfg.walLevel);
    }
    if (g_Dbs.db[i].dbCfg.fsync > 0) {
      dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen, "fsync %d ", g_Dbs.db[i].dbCfg.fsync);
    }
    if ((0 == strncasecmp(g_Dbs.db[i].dbCfg.precision, "ms", 2)) || (0 == strncasecmp(g_Dbs.db[i].dbCfg.precision, "us", 2))) {
      dataLen += snprintf(command + dataLen, BUFFER_SIZE - dataLen, "precision \'%s\';", g_Dbs.db[i].dbCfg.precision);
    }
    
    if (0 != queryDbExec(taos, command, NO_INSERT_TYPE)) {
      taos_close(taos);
      return -1;
    }
    printf("\ncreate database %s success!\n\n", g_Dbs.db[i].dbName);

    for (int j = 0; j < g_Dbs.db[i].superTblCount; j++) {
      // describe super table, if exists
      sprintf(command, "describe %s.%s;", g_Dbs.db[i].dbName, g_Dbs.db[i].superTbls[j].sTblName);
      if (0 != queryDbExec(taos, command, NO_INSERT_TYPE)) {
        g_Dbs.db[i].superTbls[j].superTblExists = TBL_NO_EXISTS;
        ret = createSuperTable(taos, g_Dbs.db[i].dbName, &g_Dbs.db[i].superTbls[j], g_Dbs.use_metric);
      } else {      
        g_Dbs.db[i].superTbls[j].superTblExists = TBL_ALREADY_EXISTS;
        ret = getSuperTableFromServer(taos, g_Dbs.db[i].dbName, &g_Dbs.db[i].superTbls[j]);
      }

      if (0 != ret) {
        taos_close(taos);
        return -1;
      }
    }    
  }

  taos_close(taos);
  return 0;
}


void * createTable(void *sarg) 
{  
  threadInfo *winfo = (threadInfo *)sarg; 
  SSuperTable* superTblInfo = winfo->superTblInfo;

  int64_t  lastPrintTime = taosGetTimestampMs();

  char* buffer = calloc(superTblInfo->maxSqlLen, 1);

  int len = 0;
  int batchNum = 0;
  //printf("Creating table from %d to %d\n", winfo->start_table_id, winfo->end_table_id);
  for (int i = winfo->start_table_id; i <= winfo->end_table_id; i++) {
    if (0 == g_Dbs.use_metric) {
      snprintf(buffer, BUFFER_SIZE, "create table if not exists %s.%s%d %s;", winfo->db_name, superTblInfo->childTblPrefix, i, superTblInfo->colsOfCreatChildTable);
    } else {
      if (0 == len) {  
        batchNum = 0;
        memset(buffer, 0, superTblInfo->maxSqlLen);
        len += snprintf(buffer + len, superTblInfo->maxSqlLen - len, "create table ");
      }
      
      char* tagsValBuf = NULL;
      if (0 == superTblInfo->tagSource) {
        tagsValBuf = generateTagVaulesForStb(superTblInfo);
      } else {
        tagsValBuf = getTagValueFromTagSample(superTblInfo, i % superTblInfo->tagSampleCount);
      }
      if (NULL == tagsValBuf) {
        free(buffer);
        return NULL;
      }
      
      len += snprintf(buffer + len, superTblInfo->maxSqlLen - len, "if not exists %s.%s%d using %s.%s tags %s ", winfo->db_name, superTblInfo->childTblPrefix, i, winfo->db_name, superTblInfo->sTblName, tagsValBuf);
      free(tagsValBuf);
      batchNum++;

      if ((batchNum < superTblInfo->batchCreateTableNum) && ((superTblInfo->maxSqlLen - len) >= (superTblInfo->lenOfTagOfOneRow + 256))) {
        continue;
      }
    }

    len = 0;
    if (0 != queryDbExec(winfo->taos, buffer, NO_INSERT_TYPE)){
      free(buffer);
      return NULL;
    }

    int64_t  currentPrintTime = taosGetTimestampMs();
    if (currentPrintTime - lastPrintTime > 30*1000) {
      printf("thread[%d] already create %d - %d tables\n", winfo->threadID, winfo->start_table_id, i);
      lastPrintTime = currentPrintTime;
    }
  }
  
  if (0 != len) {
    (void)queryDbExec(winfo->taos, buffer, NO_INSERT_TYPE);
  }
  
  free(buffer);
  return NULL;
}

void startMultiThreadCreateChildTable(char* cols, int threads, int ntables, char* db_name, SSuperTable* superTblInfo) {
  pthread_t *pids = malloc(threads * sizeof(pthread_t));
  threadInfo *infos = malloc(threads * sizeof(threadInfo));

  if ((NULL == pids) || (NULL == infos)) {
    printf("malloc failed\n");
    exit(-1);
  }

  if (threads < 1) {
    threads = 1;
  }

  int a = ntables / threads;
  if (a < 1) {
    threads = ntables;
    a = 1;
  }

  int b = 0;
  b = ntables % threads;
  
  int last = 0;
  for (int i = 0; i < threads; i++) {
    threadInfo *t_info = infos + i;
    t_info->threadID = i;
    tstrncpy(t_info->db_name, db_name, MAX_DB_NAME_SIZE);
    t_info->superTblInfo = superTblInfo;
    t_info->taos = taos_connect(g_Dbs.host, g_Dbs.user, g_Dbs.password, db_name, g_Dbs.port);
    t_info->start_table_id = last;
    t_info->end_table_id = i < b ? last + a : last + a - 1;
    last = t_info->end_table_id + 1;
    t_info->use_metric = 1;
    t_info->cols = cols;
    pthread_create(pids + i, NULL, createTable, t_info);
  }
  
  for (int i = 0; i < threads; i++) {
    pthread_join(pids[i], NULL);
  }

  for (int i = 0; i < threads; i++) {
    threadInfo *t_info = infos + i;
    taos_close(t_info->taos);
  }

  free(pids);
  free(infos);  
}


static void createChildTables() {
  for (int i = 0; i < g_Dbs.dbCount; i++) {    
    for (int j = 0; j < g_Dbs.db[i].superTblCount; j++) {
      if ((AUTO_CREATE_SUBTBL == g_Dbs.db[i].superTbls[j].autoCreateTable) || (TBL_ALREADY_EXISTS == g_Dbs.db[i].superTbls[j].childTblExists)) {
        continue;
      }
      startMultiThreadCreateChildTable(g_Dbs.db[i].superTbls[j].colsOfCreatChildTable, g_Dbs.threadCountByCreateTbl, g_Dbs.db[i].superTbls[j].childTblCount, g_Dbs.db[i].dbName, &(g_Dbs.db[i].superTbls[j]));
      g_totalChildTables += g_Dbs.db[i].superTbls[j].childTblCount;
    }    
  }
}

/*
static int taosGetLineNum(const char *fileName)
{
  int lineNum = 0;
  char cmd[1024] = { 0 };
  char buf[1024] = { 0 };
  sprintf(cmd, "wc -l %s", fileName);

  FILE *fp = popen(cmd, "r");
  if (fp == NULL) {
    fprintf(stderr, "ERROR: failed to execute:%s, error:%s\n", cmd, strerror(errno));
    return lineNum;
  }

  if (fgets(buf, sizeof(buf), fp)) {
    int index = strchr((const char*)buf, ' ') - buf;
    buf[index] = '\0';
    lineNum = atoi(buf);
  }
  pclose(fp);
  return lineNum;
}
*/

/*
  Read 10000 lines at most. If more than 10000 lines, continue to read after using
*/
int readTagFromCsvFileToMem(SSuperTable  * superTblInfo) {
  size_t  n = 0;
  ssize_t readLen = 0;
  char *  line = NULL;
  
  FILE *fp = fopen(superTblInfo->tagsFile, "r");
  if (fp == NULL) {
    printf("Failed to open tags file: %s, reason:%s\n", superTblInfo->tagsFile, strerror(errno));
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

  while ((readLen = getline(&line, &n, fp)) != -1) {
    if (('\r' == line[readLen - 1]) || ('\n' == line[readLen - 1])) {
      line[--readLen] = 0;
    }

    if (readLen == 0) {
      continue;
    }

    memcpy(tagDataBuf + count * superTblInfo->lenOfTagOfOneRow, line, readLen);
    count++;

    if (count >= tagCount - 1) {
      char *tmp = realloc(tagDataBuf, (size_t)tagCount*1.5*superTblInfo->lenOfTagOfOneRow);
      if (tmp != NULL) {
        tagDataBuf = tmp;
        tagCount = (int)(tagCount*1.5);
        memset(tagDataBuf + count*superTblInfo->lenOfTagOfOneRow, 0, (size_t)((tagCount-count)*superTblInfo->lenOfTagOfOneRow));
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

int readSampleFromJsonFileToMem(SSuperTable  * superTblInfo) {
  // TODO
  return 0;
}


/*
  Read 10000 lines at most. If more than 10000 lines, continue to read after using
*/
int readSampleFromCsvFileToMem(FILE *fp, SSuperTable* superTblInfo, char* sampleBuf) {
  size_t  n = 0;
  ssize_t readLen = 0;
  char *  line = NULL;
  int getRows = 0;

  memset(sampleBuf, 0, MAX_SAMPLES_ONCE_FROM_FILE* superTblInfo->lenOfOneRow);
  while (1) {
    readLen = getline(&line, &n, fp);
    if (-1 == readLen) {
      if(0 != fseek(fp, 0, SEEK_SET)) {
        printf("Failed to fseek file: %s, reason:%s\n", superTblInfo->sampleFile, strerror(errno));
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
      printf("sample row len[%d] overflow define schema len[%d], so discard this row\n", (int32_t)readLen, superTblInfo->lenOfOneRow);
      continue;
    }

    memcpy(sampleBuf + getRows * superTblInfo->lenOfOneRow, line, readLen);
    getRows++;

    if (getRows == MAX_SAMPLES_ONCE_FROM_FILE) {
      break;
    }
  }

  tmfree(line);
  return 0;
}

/*
void readSampleFromFileToMem(SSuperTable  * supterTblInfo) {
  int ret;
  if (0 == strncasecmp(supterTblInfo->sampleFormat, "csv", 3)) {
    ret = readSampleFromCsvFileToMem(supterTblInfo);
  } else if (0 == strncasecmp(supterTblInfo->sampleFormat, "json", 4)) {
    ret = readSampleFromJsonFileToMem(supterTblInfo);
  }

  if (0 != ret) {
    exit(-1);
  }
}
*/
static bool getColumnAndTagTypeFromInsertJsonFile(cJSON* stbInfo, SSuperTable* superTbls) {  
  bool  ret = false;
  
  // columns 
  cJSON *columns = cJSON_GetObjectItem(stbInfo, "columns");
  if (columns && columns->type != cJSON_Array) {
    printf("failed to read json, columns not found\n");
    goto PARSE_OVER;
  } else if (NULL == columns) {
    superTbls->columnCount = 0;
    superTbls->tagCount    = 0;
    return true;
  }
  
  int columnSize = cJSON_GetArraySize(columns);
  if (columnSize > MAX_COLUMN_COUNT) {
    printf("failed to read json, column size overflow, max column size is %d\n", MAX_COLUMN_COUNT);
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
      printf("failed to read json, column count not found");
      goto PARSE_OVER;
    } else {
      count = 1;
    }

    // column info 
    memset(&columnCase, 0, sizeof(StrColumn));
    cJSON *dataType = cJSON_GetObjectItem(column, "type");
    if (!dataType || dataType->type != cJSON_String || dataType->valuestring == NULL) {
      printf("failed to read json, column type not found");
      goto PARSE_OVER;
    }
    //strncpy(superTbls->columns[k].dataType, dataType->valuestring, MAX_TB_NAME_SIZE);
    strncpy(columnCase.dataType, dataType->valuestring, MAX_TB_NAME_SIZE);
            
    cJSON* dataLen = cJSON_GetObjectItem(column, "len");
    if (dataLen && dataLen->type == cJSON_Number) {
      columnCase.dataLen = dataLen->valueint;    
    } else if (dataLen && dataLen->type != cJSON_Number) {
      printf("failed to read json, column len not found");
      goto PARSE_OVER;
    } else {
      columnCase.dataLen = 8;
    }
    
    for (int n = 0; n < count; ++n) {
      strncpy(superTbls->columns[index].dataType, columnCase.dataType, MAX_TB_NAME_SIZE);
      superTbls->columns[index].dataLen = columnCase.dataLen; 
      index++;
    }
  }  
  superTbls->columnCount = index;  
  
  count = 1;
  index = 0;
  // tags 
  cJSON *tags = cJSON_GetObjectItem(stbInfo, "tags");
  if (!tags || tags->type != cJSON_Array) {
    printf("failed to read json, tags not found");
    goto PARSE_OVER;
  }
  
  int tagSize = cJSON_GetArraySize(tags);
  if (tagSize > MAX_TAG_COUNT) {
    printf("failed to read json, tags size overflow, max tag size is %d\n", MAX_TAG_COUNT);
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
      printf("failed to read json, column count not found");
      goto PARSE_OVER;
    } else {
      count = 1;
    }

    // column info 
    memset(&columnCase, 0, sizeof(StrColumn));
    cJSON *dataType = cJSON_GetObjectItem(tag, "type");
    if (!dataType || dataType->type != cJSON_String || dataType->valuestring == NULL) {
      printf("failed to read json, tag type not found");
      goto PARSE_OVER;
    }
    strncpy(columnCase.dataType, dataType->valuestring, MAX_TB_NAME_SIZE);
            
    cJSON* dataLen = cJSON_GetObjectItem(tag, "len");
    if (dataLen && dataLen->type == cJSON_Number) {
      columnCase.dataLen = dataLen->valueint;    
    } else if (dataLen && dataLen->type != cJSON_Number) {
      printf("failed to read json, column len not found");
      goto PARSE_OVER;
    } else {
      columnCase.dataLen = 0;
    }  
    
    for (int n = 0; n < count; ++n) {
      strncpy(superTbls->tags[index].dataType, columnCase.dataType, MAX_TB_NAME_SIZE);
      superTbls->tags[index].dataLen = columnCase.dataLen; 
      index++;
    }
  }      
  superTbls->tagCount = index;

  ret = true;

PARSE_OVER:
  //free(content);
  //cJSON_Delete(root);
  //fclose(fp);
  return ret;
}

static bool getMetaFromInsertJsonFile(cJSON* root) {
  bool  ret = false;

  cJSON* cfgdir = cJSON_GetObjectItem(root, "cfgdir");
  if (cfgdir && cfgdir->type == cJSON_String && cfgdir->valuestring != NULL) {
    strncpy(g_Dbs.cfgDir, cfgdir->valuestring, MAX_FILE_NAME_LEN);
  }

  cJSON* host = cJSON_GetObjectItem(root, "host");
  if (host && host->type == cJSON_String && host->valuestring != NULL) {
    strncpy(g_Dbs.host, host->valuestring, MAX_DB_NAME_SIZE);
  } else if (!host) {
    strncpy(g_Dbs.host, "127.0.0.1", MAX_DB_NAME_SIZE);
  } else {
    printf("failed to read json, host not found\n");
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
    strncpy(g_Dbs.user, user->valuestring, MAX_DB_NAME_SIZE);   
  } else if (!user) {
    strncpy(g_Dbs.user, "root", MAX_DB_NAME_SIZE);
  }

  cJSON* password = cJSON_GetObjectItem(root, "password");
  if (password && password->type == cJSON_String && password->valuestring != NULL) {
    strncpy(g_Dbs.password, password->valuestring, MAX_DB_NAME_SIZE);
  } else if (!password) {
    strncpy(g_Dbs.password, "taosdata", MAX_DB_NAME_SIZE);
  }

  cJSON* resultfile = cJSON_GetObjectItem(root, "result_file");
  if (resultfile && resultfile->type == cJSON_String && resultfile->valuestring != NULL) {
    strncpy(g_Dbs.resultFile, resultfile->valuestring, MAX_FILE_NAME_LEN);
  } else if (!resultfile) {
    strncpy(g_Dbs.resultFile, "./insert_res.txt", MAX_FILE_NAME_LEN);
  }

  cJSON* threads = cJSON_GetObjectItem(root, "thread_count");
  if (threads && threads->type == cJSON_Number) {
    g_Dbs.threadCount = threads->valueint;
  } else if (!threads) {
    g_Dbs.threadCount = 1;
  } else {
    printf("failed to read json, threads not found");
    goto PARSE_OVER;
  }  
  
  cJSON* threads2 = cJSON_GetObjectItem(root, "thread_count_create_tbl");
  if (threads2 && threads2->type == cJSON_Number) {
    g_Dbs.threadCountByCreateTbl = threads2->valueint;
  } else if (!threads2) {
    g_Dbs.threadCountByCreateTbl = 1;
  } else {
    printf("failed to read json, threads2 not found");
    goto PARSE_OVER;
  }  

  cJSON* dbs = cJSON_GetObjectItem(root, "databases");
  if (!dbs || dbs->type != cJSON_Array) {
    printf("failed to read json, databases not found\n");
    goto PARSE_OVER;
  }

  int dbSize = cJSON_GetArraySize(dbs);
  if (dbSize > MAX_DB_COUNT) {
    printf("failed to read json, databases size overflow, max database is %d\n", MAX_DB_COUNT);
    goto PARSE_OVER;
  }

  g_Dbs.dbCount = dbSize;
  for (int i = 0; i < dbSize; ++i) {
    cJSON* dbinfos = cJSON_GetArrayItem(dbs, i);
    if (dbinfos == NULL) continue;

    // dbinfo 
    cJSON *dbinfo = cJSON_GetObjectItem(dbinfos, "dbinfo");
    if (!dbinfo || dbinfo->type != cJSON_Object) {
      printf("failed to read json, dbinfo not found");
      goto PARSE_OVER;
    }
    
    cJSON *dbName = cJSON_GetObjectItem(dbinfo, "name");
    if (!dbName || dbName->type != cJSON_String || dbName->valuestring == NULL) {
      printf("failed to read json, db name not found");
      goto PARSE_OVER;
    }
    strncpy(g_Dbs.db[i].dbName, dbName->valuestring, MAX_DB_NAME_SIZE);

    cJSON *drop = cJSON_GetObjectItem(dbinfo, "drop");
    if (drop && drop->type == cJSON_String && drop->valuestring != NULL) {
      if (0 == strncasecmp(drop->valuestring, "yes", 3)) {
        g_Dbs.db[i].drop = 1;
      } else {
        g_Dbs.db[i].drop = 0;
      }        
    } else if (!drop) {
      g_Dbs.db[i].drop = 0;
    } else {
      printf("failed to read json, drop not found");
      goto PARSE_OVER;
    }

    cJSON *precision = cJSON_GetObjectItem(dbinfo, "precision");
    if (precision && precision->type == cJSON_String && precision->valuestring != NULL) {
      strncpy(g_Dbs.db[i].dbCfg.precision, precision->valuestring, MAX_DB_NAME_SIZE);
    } else if (!precision) {
      //strncpy(g_Dbs.db[i].dbCfg.precision, "ms", MAX_DB_NAME_SIZE);
      memset(g_Dbs.db[i].dbCfg.precision, 0, MAX_DB_NAME_SIZE);
    } else {
      printf("failed to read json, precision not found");
      goto PARSE_OVER;
    }

    cJSON* update = cJSON_GetObjectItem(dbinfo, "update");
    if (update && update->type == cJSON_Number) {
      g_Dbs.db[i].dbCfg.update = update->valueint;
    } else if (!update) {
      g_Dbs.db[i].dbCfg.update = -1;
    } else {
      printf("failed to read json, update not found");
      goto PARSE_OVER;
    }

    cJSON* replica = cJSON_GetObjectItem(dbinfo, "replica");
    if (replica && replica->type == cJSON_Number) {
      g_Dbs.db[i].dbCfg.replica = replica->valueint;
    } else if (!replica) {
      g_Dbs.db[i].dbCfg.replica = -1;
    } else {
      printf("failed to read json, replica not found");
      goto PARSE_OVER;
    }

    cJSON* keep = cJSON_GetObjectItem(dbinfo, "keep");
    if (keep && keep->type == cJSON_Number) {
      g_Dbs.db[i].dbCfg.keep = keep->valueint;
    } else if (!keep) {
      g_Dbs.db[i].dbCfg.keep = -1;
    } else {
     printf("failed to read json, keep not found");
     goto PARSE_OVER;
    }
    
    cJSON* days = cJSON_GetObjectItem(dbinfo, "days");
    if (days && days->type == cJSON_Number) {
      g_Dbs.db[i].dbCfg.days = days->valueint;
    } else if (!days) {
      g_Dbs.db[i].dbCfg.days = -1;
    } else {
     printf("failed to read json, days not found");
     goto PARSE_OVER;
    }
    
    cJSON* cache = cJSON_GetObjectItem(dbinfo, "cache");
    if (cache && cache->type == cJSON_Number) {
      g_Dbs.db[i].dbCfg.cache = cache->valueint;
    } else if (!cache) {
      g_Dbs.db[i].dbCfg.cache = -1;
    } else {
     printf("failed to read json, cache not found");
     goto PARSE_OVER;
    }
        
    cJSON* blocks= cJSON_GetObjectItem(dbinfo, "blocks");
    if (blocks && blocks->type == cJSON_Number) {
      g_Dbs.db[i].dbCfg.blocks = blocks->valueint;
    } else if (!blocks) {
      g_Dbs.db[i].dbCfg.blocks = -1;
    } else {
     printf("failed to read json, block not found");
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
      g_Dbs.db[i].dbCfg.minRows = -1;
    } else {
     printf("failed to read json, minRows not found");
     goto PARSE_OVER;
    }

    cJSON* maxRows= cJSON_GetObjectItem(dbinfo, "maxRows");
    if (maxRows && maxRows->type == cJSON_Number) {
      g_Dbs.db[i].dbCfg.maxRows = maxRows->valueint;
    } else if (!maxRows) {
      g_Dbs.db[i].dbCfg.maxRows = -1;
    } else {
     printf("failed to read json, maxRows not found");
     goto PARSE_OVER;
    }

    cJSON* comp= cJSON_GetObjectItem(dbinfo, "comp");
    if (comp && comp->type == cJSON_Number) {
      g_Dbs.db[i].dbCfg.comp = comp->valueint;
    } else if (!comp) {
      g_Dbs.db[i].dbCfg.comp = -1;
    } else {
     printf("failed to read json, comp not found");
     goto PARSE_OVER;
    }

    cJSON* walLevel= cJSON_GetObjectItem(dbinfo, "walLevel");
    if (walLevel && walLevel->type == cJSON_Number) {
      g_Dbs.db[i].dbCfg.walLevel = walLevel->valueint;
    } else if (!walLevel) {
      g_Dbs.db[i].dbCfg.walLevel = -1;
    } else {
     printf("failed to read json, walLevel not found");
     goto PARSE_OVER;
    }

    cJSON* quorum= cJSON_GetObjectItem(dbinfo, "quorum");
    if (quorum && quorum->type == cJSON_Number) {
      g_Dbs.db[i].dbCfg.quorum = quorum->valueint;
    } else if (!quorum) {
      g_Dbs.db[i].dbCfg.quorum = -1;
    } else {
     printf("failed to read json, walLevel not found");
     goto PARSE_OVER;
    }

    cJSON* fsync= cJSON_GetObjectItem(dbinfo, "fsync");
    if (fsync && fsync->type == cJSON_Number) {
      g_Dbs.db[i].dbCfg.fsync = fsync->valueint;
    } else if (!fsync) {
      g_Dbs.db[i].dbCfg.fsync = -1;
    } else {
     printf("failed to read json, fsync not found");
     goto PARSE_OVER;
    }    

    // super_talbes 
    cJSON *stables = cJSON_GetObjectItem(dbinfos, "super_tables");
    if (!stables || stables->type != cJSON_Array) {
      printf("failed to read json, super_tables not found");
      goto PARSE_OVER;
    }    
    
    int stbSize = cJSON_GetArraySize(stables);
    if (stbSize > MAX_SUPER_TABLE_COUNT) {
      printf("failed to read json, databases size overflow, max database is %d\n", MAX_SUPER_TABLE_COUNT);
      goto PARSE_OVER;
    }

    g_Dbs.db[i].superTblCount = stbSize;
    for (int j = 0; j < stbSize; ++j) {
      cJSON* stbInfo = cJSON_GetArrayItem(stables, j);
      if (stbInfo == NULL) continue;
    
      // dbinfo 
      cJSON *stbName = cJSON_GetObjectItem(stbInfo, "name");
      if (!stbName || stbName->type != cJSON_String || stbName->valuestring == NULL) {
        printf("failed to read json, stb name not found");
        goto PARSE_OVER;
      }
      strncpy(g_Dbs.db[i].superTbls[j].sTblName, stbName->valuestring, MAX_TB_NAME_SIZE);
    
      cJSON *prefix = cJSON_GetObjectItem(stbInfo, "childtable_prefix");
      if (!prefix || prefix->type != cJSON_String || prefix->valuestring == NULL) {
        printf("failed to read json, childtable_prefix not found");
        goto PARSE_OVER;
      }
      strncpy(g_Dbs.db[i].superTbls[j].childTblPrefix, prefix->valuestring, MAX_DB_NAME_SIZE);

      cJSON *autoCreateTbl = cJSON_GetObjectItem(stbInfo, "auto_create_table"); // yes, no, null
      if (autoCreateTbl && autoCreateTbl->type == cJSON_String && autoCreateTbl->valuestring != NULL) {
        if (0 == strncasecmp(autoCreateTbl->valuestring, "yes", 3)) {
          g_Dbs.db[i].superTbls[j].autoCreateTable = AUTO_CREATE_SUBTBL;
        } else if (0 == strncasecmp(autoCreateTbl->valuestring, "no", 2)) {
          g_Dbs.db[i].superTbls[j].autoCreateTable = PRE_CREATE_SUBTBL;
        } else {
          g_Dbs.db[i].superTbls[j].autoCreateTable = PRE_CREATE_SUBTBL;
        }
      } else if (!autoCreateTbl) {
        g_Dbs.db[i].superTbls[j].autoCreateTable = PRE_CREATE_SUBTBL;
      } else {
        printf("failed to read json, auto_create_table not found");
        goto PARSE_OVER;
      }
      
      cJSON* batchCreateTbl = cJSON_GetObjectItem(stbInfo, "batch_create_tbl_num");
      if (batchCreateTbl && batchCreateTbl->type == cJSON_Number) {
        g_Dbs.db[i].superTbls[j].batchCreateTableNum = batchCreateTbl->valueint;
      } else if (!batchCreateTbl) {
        g_Dbs.db[i].superTbls[j].batchCreateTableNum = 2000;
      } else {
        printf("failed to read json, batch_create_tbl_num not found");
        goto PARSE_OVER;
      }      

      cJSON *childTblExists = cJSON_GetObjectItem(stbInfo, "child_table_exists"); // yes, no
      if (childTblExists && childTblExists->type == cJSON_String && childTblExists->valuestring != NULL) {
        if (0 == strncasecmp(childTblExists->valuestring, "yes", 3)) {
          g_Dbs.db[i].superTbls[j].childTblExists = TBL_ALREADY_EXISTS;
        } else if (0 == strncasecmp(childTblExists->valuestring, "no", 2)) {
          g_Dbs.db[i].superTbls[j].childTblExists = TBL_NO_EXISTS;
        } else {
          g_Dbs.db[i].superTbls[j].childTblExists = TBL_NO_EXISTS;
        }
      } else if (!childTblExists) {
        g_Dbs.db[i].superTbls[j].childTblExists = TBL_NO_EXISTS;
      } else {
        printf("failed to read json, child_table_exists not found");
        goto PARSE_OVER;
      }
      
      cJSON* count = cJSON_GetObjectItem(stbInfo, "childtable_count");
      if (!count || count->type != cJSON_Number || 0 >= count->valueint) {
        printf("failed to read json, childtable_count not found");
        goto PARSE_OVER;
      }
      g_Dbs.db[i].superTbls[j].childTblCount = count->valueint;

      cJSON *dataSource = cJSON_GetObjectItem(stbInfo, "data_source");
      if (dataSource && dataSource->type == cJSON_String && dataSource->valuestring != NULL) {
        strncpy(g_Dbs.db[i].superTbls[j].dataSource, dataSource->valuestring, MAX_DB_NAME_SIZE);
      } else if (!dataSource) {
        strncpy(g_Dbs.db[i].superTbls[j].dataSource, "rand", MAX_DB_NAME_SIZE);
      } else {
        printf("failed to read json, data_source not found");
        goto PARSE_OVER;
      }

      cJSON *insertMode = cJSON_GetObjectItem(stbInfo, "insert_mode"); // taosc , restful
      if (insertMode && insertMode->type == cJSON_String && insertMode->valuestring != NULL) {
        strncpy(g_Dbs.db[i].superTbls[j].insertMode, insertMode->valuestring, MAX_DB_NAME_SIZE);
        #ifndef TD_LOWA_CURL
        if (0 == strncasecmp(g_Dbs.db[i].superTbls[j].insertMode, "restful", 7)) {          
          printf("There no libcurl, so no support resetful test! please use taosc mode.\n");
          goto PARSE_OVER;
        } 
        #endif
      } else if (!insertMode) {
        strncpy(g_Dbs.db[i].superTbls[j].insertMode, "taosc", MAX_DB_NAME_SIZE);
      } else {
        printf("failed to read json, insert_mode not found");
        goto PARSE_OVER;
      }

      cJSON *ts = cJSON_GetObjectItem(stbInfo, "start_timestamp");
      if (ts && ts->type == cJSON_String && ts->valuestring != NULL) {
        strncpy(g_Dbs.db[i].superTbls[j].startTimestamp, ts->valuestring, MAX_DB_NAME_SIZE);
      } else if (!ts) {
        strncpy(g_Dbs.db[i].superTbls[j].startTimestamp, "now", MAX_DB_NAME_SIZE);
      } else {
        printf("failed to read json, start_timestamp not found");
        goto PARSE_OVER;
      }
    
      cJSON* timestampStep = cJSON_GetObjectItem(stbInfo, "timestamp_step");
      if (timestampStep && timestampStep->type == cJSON_Number) {
        g_Dbs.db[i].superTbls[j].timeStampStep = timestampStep->valueint;
      } else if (!timestampStep) {
        g_Dbs.db[i].superTbls[j].timeStampStep = 1000;
      } else {
        printf("failed to read json, timestamp_step not found");
        goto PARSE_OVER;
      }

      cJSON* sampleDataBufSize = cJSON_GetObjectItem(stbInfo, "sample_buf_size");
      if (sampleDataBufSize && sampleDataBufSize->type == cJSON_Number) {
        g_Dbs.db[i].superTbls[j].sampleDataBufSize = sampleDataBufSize->valueint;
        if (g_Dbs.db[i].superTbls[j].sampleDataBufSize < 1024*1024) {
          g_Dbs.db[i].superTbls[j].sampleDataBufSize = 1024*1024 + 1024;
        }
      } else if (!sampleDataBufSize) {
        g_Dbs.db[i].superTbls[j].sampleDataBufSize = 1024*1024 + 1024;
      } else {
        printf("failed to read json, sample_buf_size not found");
        goto PARSE_OVER;
      }      
       
      cJSON *sampleFormat = cJSON_GetObjectItem(stbInfo, "sample_format");
      if (sampleFormat && sampleFormat->type == cJSON_String && sampleFormat->valuestring != NULL) {
        strncpy(g_Dbs.db[i].superTbls[j].sampleFormat, sampleFormat->valuestring, MAX_DB_NAME_SIZE);
      } else if (!sampleFormat) {
        strncpy(g_Dbs.db[i].superTbls[j].sampleFormat, "csv", MAX_DB_NAME_SIZE);
      } else {
        printf("failed to read json, sample_format not found");
        goto PARSE_OVER;
      }      
      
      cJSON *sampleFile = cJSON_GetObjectItem(stbInfo, "sample_file");
      if (sampleFile && sampleFile->type == cJSON_String && sampleFile->valuestring != NULL) {
        strncpy(g_Dbs.db[i].superTbls[j].sampleFile, sampleFile->valuestring, MAX_FILE_NAME_LEN);
      } else if (!sampleFile) {
        memset(g_Dbs.db[i].superTbls[j].sampleFile, 0, MAX_FILE_NAME_LEN);
      } else {
        printf("failed to read json, sample_file not found");
        goto PARSE_OVER;
      }          
      
      cJSON *tagsFile = cJSON_GetObjectItem(stbInfo, "tags_file");
      if (tagsFile && tagsFile->type == cJSON_String && tagsFile->valuestring != NULL) {
        strncpy(g_Dbs.db[i].superTbls[j].tagsFile, tagsFile->valuestring, MAX_FILE_NAME_LEN);
        if (0 == g_Dbs.db[i].superTbls[j].tagsFile[0]) {
          g_Dbs.db[i].superTbls[j].tagSource = 0;
        } else {
          g_Dbs.db[i].superTbls[j].tagSource = 1;
        }
      } else if (!tagsFile) {
        memset(g_Dbs.db[i].superTbls[j].tagsFile, 0, MAX_FILE_NAME_LEN);
        g_Dbs.db[i].superTbls[j].tagSource = 0;
      } else {
        printf("failed to read json, tags_file not found");
        goto PARSE_OVER;
      }
    
      cJSON* maxSqlLen = cJSON_GetObjectItem(stbInfo, "max_sql_len");
      if (maxSqlLen && maxSqlLen->type == cJSON_Number) {
        int32_t len = maxSqlLen->valueint;
        if (len > TSDB_MAX_ALLOWED_SQL_LEN) {
          len = TSDB_MAX_ALLOWED_SQL_LEN;
        } else if (len < TSDB_MAX_SQL_LEN) {
          len = TSDB_MAX_SQL_LEN;
        }       
        g_Dbs.db[i].superTbls[j].maxSqlLen = len;
      } else if (!maxSqlLen) {
        g_Dbs.db[i].superTbls[j].maxSqlLen = TSDB_MAX_SQL_LEN;
      } else {
        printf("failed to read json, maxSqlLen not found");
        goto PARSE_OVER;
      }      

      cJSON *multiThreadWriteOneTbl = cJSON_GetObjectItem(stbInfo, "multi_thread_write_one_tbl"); // no , yes
      if (multiThreadWriteOneTbl && multiThreadWriteOneTbl->type == cJSON_String && multiThreadWriteOneTbl->valuestring != NULL) {
        if (0 == strncasecmp(multiThreadWriteOneTbl->valuestring, "yes", 3)) {
          g_Dbs.db[i].superTbls[j].multiThreadWriteOneTbl = 1;
        } else {
          g_Dbs.db[i].superTbls[j].multiThreadWriteOneTbl = 0;
        }        
      } else if (!multiThreadWriteOneTbl) {
        g_Dbs.db[i].superTbls[j].multiThreadWriteOneTbl = 0;
      } else {
        printf("failed to read json, multiThreadWriteOneTbl not found");
        goto PARSE_OVER;
      }

      cJSON* numberOfTblInOneSql = cJSON_GetObjectItem(stbInfo, "number_of_tbl_in_one_sql");
      if (numberOfTblInOneSql && numberOfTblInOneSql->type == cJSON_Number) {
        g_Dbs.db[i].superTbls[j].numberOfTblInOneSql = numberOfTblInOneSql->valueint;
      } else if (!numberOfTblInOneSql) {
        g_Dbs.db[i].superTbls[j].numberOfTblInOneSql = 0;
      } else {
        printf("failed to read json, numberOfTblInOneSql not found");
        goto PARSE_OVER;
      }      

      cJSON* rowsPerTbl = cJSON_GetObjectItem(stbInfo, "rows_per_tbl");
      if (rowsPerTbl && rowsPerTbl->type == cJSON_Number) {
        g_Dbs.db[i].superTbls[j].rowsPerTbl = rowsPerTbl->valueint;
      } else if (!rowsPerTbl) {
        g_Dbs.db[i].superTbls[j].rowsPerTbl = 1;
      } else {
        printf("failed to read json, rowsPerTbl not found");
        goto PARSE_OVER;
      }      

      cJSON* disorderRatio = cJSON_GetObjectItem(stbInfo, "disorder_ratio");
      if (disorderRatio && disorderRatio->type == cJSON_Number) {
        g_Dbs.db[i].superTbls[j].disorderRatio = disorderRatio->valueint;
      } else if (!disorderRatio) {
        g_Dbs.db[i].superTbls[j].disorderRatio = 0;
      } else {
        printf("failed to read json, disorderRatio not found");
        goto PARSE_OVER;
      }      

      cJSON* disorderRange = cJSON_GetObjectItem(stbInfo, "disorder_range");
      if (disorderRange && disorderRange->type == cJSON_Number) {
        g_Dbs.db[i].superTbls[j].disorderRange = disorderRange->valueint;
      } else if (!disorderRange) {
        g_Dbs.db[i].superTbls[j].disorderRange = 1000;
      } else {
        printf("failed to read json, disorderRange not found");
        goto PARSE_OVER;
      }
      
      cJSON* insertRate = cJSON_GetObjectItem(stbInfo, "insert_rate");
      if (insertRate && insertRate->type == cJSON_Number) {
        g_Dbs.db[i].superTbls[j].insertRate = insertRate->valueint;
      } else if (!insertRate) {
        g_Dbs.db[i].superTbls[j].insertRate = 0;
      } else {
        printf("failed to read json, insert_rate not found");
        goto PARSE_OVER;
      }
 
      cJSON* insertRows = cJSON_GetObjectItem(stbInfo, "insert_rows");
      if (insertRows && insertRows->type == cJSON_Number) {
        g_Dbs.db[i].superTbls[j].insertRows = insertRows->valueint;
        if (0 == g_Dbs.db[i].superTbls[j].insertRows) {
          g_Dbs.db[i].superTbls[j].insertRows = 0x7FFFFFFFFFFFFFFF;
        }
      } else if (!insertRows) {
        g_Dbs.db[i].superTbls[j].insertRows = 0x7FFFFFFFFFFFFFFF;
      } else {
        printf("failed to read json, insert_rows not found");
        goto PARSE_OVER;
      }

      if (NO_CREATE_SUBTBL == g_Dbs.db[i].superTbls[j].autoCreateTable || (TBL_ALREADY_EXISTS == g_Dbs.db[i].superTbls[j].childTblExists)) {
        continue;
      }

      int retVal = getColumnAndTagTypeFromInsertJsonFile(stbInfo, &g_Dbs.db[i].superTbls[j]);
      if (false == retVal) {
        goto PARSE_OVER;
      }      
    }    
  }

  ret = true;

PARSE_OVER:
  //free(content);
  //cJSON_Delete(root);
  //fclose(fp);
  return ret;
}

static bool getMetaFromQueryJsonFile(cJSON* root) {
  bool  ret = false;

  cJSON* cfgdir = cJSON_GetObjectItem(root, "cfgdir");
  if (cfgdir && cfgdir->type == cJSON_String && cfgdir->valuestring != NULL) {
    strncpy(g_queryInfo.cfgDir, cfgdir->valuestring, MAX_FILE_NAME_LEN);
  }

  cJSON* host = cJSON_GetObjectItem(root, "host");
  if (host && host->type == cJSON_String && host->valuestring != NULL) {
    strncpy(g_queryInfo.host, host->valuestring, MAX_DB_NAME_SIZE);
  } else if (!host) {
    strncpy(g_queryInfo.host, "127.0.0.1", MAX_DB_NAME_SIZE);
  } else {
    printf("failed to read json, host not found\n");
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
    strncpy(g_queryInfo.user, user->valuestring, MAX_DB_NAME_SIZE);   
  } else if (!user) {
    strncpy(g_queryInfo.user, "root", MAX_DB_NAME_SIZE); ;
  }

  cJSON* password = cJSON_GetObjectItem(root, "password");
  if (password && password->type == cJSON_String && password->valuestring != NULL) {
    strncpy(g_queryInfo.password, password->valuestring, MAX_DB_NAME_SIZE);
  } else if (!password) {
    strncpy(g_queryInfo.password, "taosdata", MAX_DB_NAME_SIZE);;
  }

  cJSON* dbs = cJSON_GetObjectItem(root, "databases");
  if (dbs && dbs->type == cJSON_String && dbs->valuestring != NULL) {
    strncpy(g_queryInfo.dbName, dbs->valuestring, MAX_DB_NAME_SIZE);
  } else if (!dbs) {
    printf("failed to read json, databases not found\n");
    goto PARSE_OVER;
  }

  cJSON* queryMode = cJSON_GetObjectItem(root, "query_mode");
  if (queryMode && queryMode->type == cJSON_String && queryMode->valuestring != NULL) {
    strncpy(g_queryInfo.queryMode, queryMode->valuestring, MAX_TB_NAME_SIZE);
  } else if (!queryMode) {
    strncpy(g_queryInfo.queryMode, "taosc", MAX_TB_NAME_SIZE);
  } else {
    printf("failed to read json, query_mode not found\n");
    goto PARSE_OVER;
  }
  
  // super_table_query 
  cJSON *superQuery = cJSON_GetObjectItem(root, "specified_table_query");
  if (!superQuery) {
    g_queryInfo.superQueryInfo.concurrent = 0;
    g_queryInfo.superQueryInfo.sqlCount = 0;
  } else if (superQuery->type != cJSON_Object) {
    printf("failed to read json, super_table_query not found");
    goto PARSE_OVER;
  } else {  
    cJSON* rate = cJSON_GetObjectItem(superQuery, "query_interval");
    if (rate && rate->type == cJSON_Number) {
      g_queryInfo.superQueryInfo.rate = rate->valueint;
    } else if (!rate) {
      g_queryInfo.superQueryInfo.rate = 0;
    }
  
    cJSON* concurrent = cJSON_GetObjectItem(superQuery, "concurrent");
    if (concurrent && concurrent->type == cJSON_Number) {
      g_queryInfo.superQueryInfo.concurrent = concurrent->valueint;
    } else if (!concurrent) {
      g_queryInfo.superQueryInfo.concurrent = 1;
    }
  
    cJSON* mode = cJSON_GetObjectItem(superQuery, "mode");
    if (mode && mode->type == cJSON_String && mode->valuestring != NULL) {
      if (0 == strcmp("sync", mode->valuestring)) {      
        g_queryInfo.superQueryInfo.subscribeMode = 0;
      } else if (0 == strcmp("async", mode->valuestring)) {      
        g_queryInfo.superQueryInfo.subscribeMode = 1;
      } else {
        printf("failed to read json, subscribe mod error\n");
        goto PARSE_OVER;
      }
    } else {
      g_queryInfo.superQueryInfo.subscribeMode = 0;
    }
    
    cJSON* interval = cJSON_GetObjectItem(superQuery, "interval");
    if (interval && interval->type == cJSON_Number) {
      g_queryInfo.superQueryInfo.subscribeInterval = interval->valueint;
    } else if (!interval) {    
      //printf("failed to read json, subscribe interval no found\n");
      //goto PARSE_OVER;
      g_queryInfo.superQueryInfo.subscribeInterval = 10000;
    }
  
    cJSON* restart = cJSON_GetObjectItem(superQuery, "restart");
    if (restart && restart->type == cJSON_String && restart->valuestring != NULL) {
      if (0 == strcmp("yes", restart->valuestring)) {      
        g_queryInfo.superQueryInfo.subscribeRestart = 1;
      } else if (0 == strcmp("no", restart->valuestring)) {      
        g_queryInfo.superQueryInfo.subscribeRestart = 0;
      } else {
        printf("failed to read json, subscribe restart error\n");
        goto PARSE_OVER;
      }
    } else {
      g_queryInfo.superQueryInfo.subscribeRestart = 1;
    }
  
    cJSON* keepProgress = cJSON_GetObjectItem(superQuery, "keepProgress");
    if (keepProgress && keepProgress->type == cJSON_String && keepProgress->valuestring != NULL) {
      if (0 == strcmp("yes", keepProgress->valuestring)) {      
        g_queryInfo.superQueryInfo.subscribeKeepProgress = 1;
      } else if (0 == strcmp("no", keepProgress->valuestring)) {      
        g_queryInfo.superQueryInfo.subscribeKeepProgress = 0;
      } else {
        printf("failed to read json, subscribe keepProgress error\n");
        goto PARSE_OVER;
      }
    } else {
      g_queryInfo.superQueryInfo.subscribeKeepProgress = 0;
    }

    // sqls   
    cJSON* superSqls = cJSON_GetObjectItem(superQuery, "sqls");
    if (!superSqls) {
      g_queryInfo.superQueryInfo.sqlCount = 0;
    } else if (superSqls->type != cJSON_Array) {
      printf("failed to read json, super sqls not found\n");
      goto PARSE_OVER;
    } else {  
      int superSqlSize = cJSON_GetArraySize(superSqls);
      if (superSqlSize > MAX_QUERY_SQL_COUNT) {
        printf("failed to read json, query sql size overflow, max is %d\n", MAX_QUERY_SQL_COUNT);
        goto PARSE_OVER;
      }
      
      g_queryInfo.superQueryInfo.sqlCount = superSqlSize;
      for (int j = 0; j < superSqlSize; ++j) {
        cJSON* sql = cJSON_GetArrayItem(superSqls, j);
        if (sql == NULL) continue;
      
        cJSON *sqlStr = cJSON_GetObjectItem(sql, "sql");
        if (!sqlStr || sqlStr->type != cJSON_String || sqlStr->valuestring == NULL) {
          printf("failed to read json, sql not found\n");
          goto PARSE_OVER;
        }
        strncpy(g_queryInfo.superQueryInfo.sql[j], sqlStr->valuestring, MAX_QUERY_SQL_LENGTH);

        cJSON *result = cJSON_GetObjectItem(sql, "result");
        if (NULL != result && result->type == cJSON_String && result->valuestring != NULL) {
          strncpy(g_queryInfo.superQueryInfo.result[j], result->valuestring, MAX_FILE_NAME_LEN);
        } else if (NULL == result) {
          memset(g_queryInfo.superQueryInfo.result[j], 0, MAX_FILE_NAME_LEN);
        } else {
          printf("failed to read json, super query result file not found\n");
          goto PARSE_OVER;
        } 
      }
    }
  }
  
  // sub_table_query 
  cJSON *subQuery = cJSON_GetObjectItem(root, "super_table_query");
  if (!subQuery) {
    g_queryInfo.subQueryInfo.threadCnt = 0;
    g_queryInfo.subQueryInfo.sqlCount = 0;
  } else if (subQuery->type != cJSON_Object) {
    printf("failed to read json, sub_table_query not found");
    ret = true;
    goto PARSE_OVER;
  } else {
    cJSON* subrate = cJSON_GetObjectItem(subQuery, "query_interval");
    if (subrate && subrate->type == cJSON_Number) {
      g_queryInfo.subQueryInfo.rate = subrate->valueint;
    } else if (!subrate) {
      g_queryInfo.subQueryInfo.rate = 0;
    }
  
    cJSON* threads = cJSON_GetObjectItem(subQuery, "threads");
    if (threads && threads->type == cJSON_Number) {
      g_queryInfo.subQueryInfo.threadCnt = threads->valueint;
    } else if (!threads) {
      g_queryInfo.subQueryInfo.threadCnt = 1;
    }
  
    //cJSON* subTblCnt = cJSON_GetObjectItem(subQuery, "childtable_count");
    //if (subTblCnt && subTblCnt->type == cJSON_Number) {
    //  g_queryInfo.subQueryInfo.childTblCount = subTblCnt->valueint;
    //} else if (!subTblCnt) {
    //  g_queryInfo.subQueryInfo.childTblCount = 0;
    //}
  
    cJSON* stblname = cJSON_GetObjectItem(subQuery, "stblname");
    if (stblname && stblname->type == cJSON_String && stblname->valuestring != NULL) {
      strncpy(g_queryInfo.subQueryInfo.sTblName, stblname->valuestring, MAX_TB_NAME_SIZE);
    } else {
      printf("failed to read json, super table name not found\n");
      goto PARSE_OVER;
    }
  
    cJSON* submode = cJSON_GetObjectItem(subQuery, "mode");
    if (submode && submode->type == cJSON_String && submode->valuestring != NULL) {
      if (0 == strcmp("sync", submode->valuestring)) {      
        g_queryInfo.subQueryInfo.subscribeMode = 0;
      } else if (0 == strcmp("async", submode->valuestring)) {      
        g_queryInfo.subQueryInfo.subscribeMode = 1;
      } else {
        printf("failed to read json, subscribe mod error\n");
        goto PARSE_OVER;
      }
    } else {
      g_queryInfo.subQueryInfo.subscribeMode = 0;
    }
    
    cJSON* subinterval = cJSON_GetObjectItem(subQuery, "interval");
    if (subinterval && subinterval->type == cJSON_Number) {
      g_queryInfo.subQueryInfo.subscribeInterval = subinterval->valueint;
    } else if (!subinterval) {    
      //printf("failed to read json, subscribe interval no found\n");
      //goto PARSE_OVER;
      g_queryInfo.subQueryInfo.subscribeInterval = 10000;
    }
  
    cJSON* subrestart = cJSON_GetObjectItem(subQuery, "restart");
    if (subrestart && subrestart->type == cJSON_String && subrestart->valuestring != NULL) {
      if (0 == strcmp("yes", subrestart->valuestring)) {      
        g_queryInfo.subQueryInfo.subscribeRestart = 1;
      } else if (0 == strcmp("no", subrestart->valuestring)) {      
        g_queryInfo.subQueryInfo.subscribeRestart = 0;
      } else {
        printf("failed to read json, subscribe restart error\n");
        goto PARSE_OVER;
      }
    } else {
      g_queryInfo.subQueryInfo.subscribeRestart = 1;
    }
  
    cJSON* subkeepProgress = cJSON_GetObjectItem(subQuery, "keepProgress");
    if (subkeepProgress && subkeepProgress->type == cJSON_String && subkeepProgress->valuestring != NULL) {
      if (0 == strcmp("yes", subkeepProgress->valuestring)) {      
        g_queryInfo.subQueryInfo.subscribeKeepProgress = 1;
      } else if (0 == strcmp("no", subkeepProgress->valuestring)) {      
        g_queryInfo.subQueryInfo.subscribeKeepProgress = 0;
      } else {
        printf("failed to read json, subscribe keepProgress error\n");
        goto PARSE_OVER;
      }
    } else {
      g_queryInfo.subQueryInfo.subscribeKeepProgress = 0;
    }  

    // sqls     
    cJSON* subsqls = cJSON_GetObjectItem(subQuery, "sqls");
    if (!subsqls) {
      g_queryInfo.subQueryInfo.sqlCount = 0;
    } else if (subsqls->type != cJSON_Array) {
      printf("failed to read json, super sqls not found\n");
      goto PARSE_OVER;
    } else {  
      int superSqlSize = cJSON_GetArraySize(subsqls);
      if (superSqlSize > MAX_QUERY_SQL_COUNT) {
        printf("failed to read json, query sql size overflow, max is %d\n", MAX_QUERY_SQL_COUNT);
        goto PARSE_OVER;
      }
    
      g_queryInfo.subQueryInfo.sqlCount = superSqlSize;
      for (int j = 0; j < superSqlSize; ++j) {      
        cJSON* sql = cJSON_GetArrayItem(subsqls, j);
        if (sql == NULL) continue;
        
        cJSON *sqlStr = cJSON_GetObjectItem(sql, "sql");
        if (!sqlStr || sqlStr->type != cJSON_String || sqlStr->valuestring == NULL) {
          printf("failed to read json, sql not found\n");
          goto PARSE_OVER;
        }
        strncpy(g_queryInfo.subQueryInfo.sql[j], sqlStr->valuestring, MAX_QUERY_SQL_LENGTH);

        cJSON *result = cJSON_GetObjectItem(sql, "result");
        if (result != NULL && result->type == cJSON_String && result->valuestring != NULL){
          strncpy(g_queryInfo.subQueryInfo.result[j], result->valuestring, MAX_FILE_NAME_LEN);
        } else if (NULL == result) {
          memset(g_queryInfo.subQueryInfo.result[j], 0, MAX_FILE_NAME_LEN);
        }  else {
          printf("failed to read json, sub query result file not found\n");
          goto PARSE_OVER;
        } 
      }
    }
  }

  ret = true;

PARSE_OVER:
  //free(content);
  //cJSON_Delete(root);
  //fclose(fp);
  return ret;
}

static bool getInfoFromJsonFile(char* file) {
  FILE *fp = fopen(file, "r");
  if (!fp) {
    printf("failed to read %s, reason:%s\n", file, strerror(errno));
    return false;
  }

  bool  ret = false;
  int   maxLen = 64000;
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
    printf("failed to cjson parse %s, invalid json format", file);
    goto PARSE_OVER;
  }

  cJSON* filetype = cJSON_GetObjectItem(root, "filetype");
  if (filetype && filetype->type == cJSON_String && filetype->valuestring != NULL) {
    if (0 == strcasecmp("insert", filetype->valuestring)) {
      g_jsonType = INSERT_MODE;
    } else if (0 == strcasecmp("query", filetype->valuestring)) {
      g_jsonType = QUERY_MODE;
    } else if (0 == strcasecmp("subscribe", filetype->valuestring)) {
      g_jsonType = SUBSCRIBE_MODE;
    } else {
      printf("failed to read json, filetype not support\n");
      goto PARSE_OVER;
    }
  } else if (!filetype) {
    g_jsonType = INSERT_MODE;
  } else {
    printf("failed to read json, filetype not found\n");
    goto PARSE_OVER;
  }

  if (INSERT_MODE == g_jsonType) {
    ret = getMetaFromInsertJsonFile(root);
  } else if (QUERY_MODE == g_jsonType) {
    ret = getMetaFromQueryJsonFile(root);
  } else if (SUBSCRIBE_MODE == g_jsonType) {
    ret = getMetaFromQueryJsonFile(root);
  } else {
    printf("input json file type error! please input correct file type: insert or query or subscribe\n");
    goto PARSE_OVER;
  }  

PARSE_OVER:
  free(content);
  cJSON_Delete(root);
  fclose(fp);
  return ret;
}


void prePareSampleData() {
  for (int i = 0; i < g_Dbs.dbCount; i++) {    
    for (int j = 0; j < g_Dbs.db[i].superTblCount; j++) {
      //if (0 == strncasecmp(g_Dbs.db[i].superTbls[j].dataSource, "sample", 6)) {
      //  readSampleFromFileToMem(&g_Dbs.db[i].superTbls[j]);
      //}
      
      if (g_Dbs.db[i].superTbls[j].tagsFile[0] != 0) {
        (void)readTagFromCsvFileToMem(&g_Dbs.db[i].superTbls[j]);
      }

      #ifdef TD_LOWA_CURL
      if (0 == strncasecmp(g_Dbs.db[i].superTbls[j].insertMode, "restful", 8)) {
        curl_global_init(CURL_GLOBAL_ALL);
      }
      #endif
    }
  }
}

void postFreeResource() {
  tmfclose(g_fpOfInsertResult);
  for (int i = 0; i < g_Dbs.dbCount; i++) {    
    for (int j = 0; j < g_Dbs.db[i].superTblCount; j++) {
      if (0 != g_Dbs.db[i].superTbls[j].colsOfCreatChildTable) {
        free(g_Dbs.db[i].superTbls[j].colsOfCreatChildTable);
        g_Dbs.db[i].superTbls[j].colsOfCreatChildTable = NULL;
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

      #ifdef TD_LOWA_CURL
      if (0 == strncasecmp(g_Dbs.db[i].superTbls[j].insertMode, "restful", 8)) {
        curl_global_cleanup();
      }
      #endif
    }
  }
}

int getRowDataFromSample(char*  dataBuf, int maxLen, int64_t timestamp, SSuperTable* superTblInfo, int* sampleUsePos, FILE *fp, char* sampleBuf) {
  if ((*sampleUsePos) == MAX_SAMPLES_ONCE_FROM_FILE) {
    int ret = readSampleFromCsvFileToMem(fp, superTblInfo, sampleBuf);
    if (0 != ret) {
      return -1;
    }
    *sampleUsePos = 0;
  }

  int    dataLen = 0;
  dataLen += snprintf(dataBuf + dataLen, maxLen - dataLen, "(%" PRId64 ", ", timestamp);
  dataLen += snprintf(dataBuf + dataLen, maxLen - dataLen, "%s", sampleBuf + superTblInfo->lenOfOneRow * (*sampleUsePos));
  dataLen += snprintf(dataBuf + dataLen, maxLen - dataLen, ")");

  (*sampleUsePos)++;
  
  return dataLen;
}

int generateRowData(char*  dataBuf, int maxLen, int64_t timestamp, SSuperTable* stbInfo) {
  int    dataLen = 0;
  dataLen += snprintf(dataBuf + dataLen, maxLen - dataLen, "(%" PRId64 ", ", timestamp);
  for (int i = 0; i < stbInfo->columnCount; i++) {    
    if ((0 == strncasecmp(stbInfo->columns[i].dataType, "binary", 6)) || (0 == strncasecmp(stbInfo->columns[i].dataType, "nchar", 5))) {
      if (stbInfo->columns[i].dataLen > TSDB_MAX_BINARY_LEN) {
        printf("binary or nchar length overflow, max size:%u\n", (uint32_t)TSDB_MAX_BINARY_LEN);
        return (-1);
      }
    
      char* buf = (char*)calloc(stbInfo->columns[i].dataLen+1, 1);
      if (NULL == buf) {
        printf("calloc failed! size:%d\n", stbInfo->columns[i].dataLen);
        return (-1);
      }
      rand_string(buf, stbInfo->columns[i].dataLen);
      dataLen += snprintf(dataBuf + dataLen, maxLen - dataLen, "\'%s\', ", buf);
      tmfree(buf);
    } else if (0 == strncasecmp(stbInfo->columns[i].dataType, "int", 3)) {
      dataLen += snprintf(dataBuf + dataLen, maxLen - dataLen, "%d, ", rand_int());
    } else if (0 == strncasecmp(stbInfo->columns[i].dataType, "bigint", 6)) {
      dataLen += snprintf(dataBuf + dataLen, maxLen - dataLen, "%"PRId64", ", rand_bigint());
    }  else if (0 == strncasecmp(stbInfo->columns[i].dataType, "float", 5)) {
      dataLen += snprintf(dataBuf + dataLen, maxLen - dataLen, "%f, ", rand_float());
    }  else if (0 == strncasecmp(stbInfo->columns[i].dataType, "double", 6)) {
      dataLen += snprintf(dataBuf + dataLen, maxLen - dataLen, "%f, ", rand_double());
    }  else if (0 == strncasecmp(stbInfo->columns[i].dataType, "smallint", 8)) {
      dataLen += snprintf(dataBuf + dataLen, maxLen - dataLen, "%d, ", rand_smallint());
    }  else if (0 == strncasecmp(stbInfo->columns[i].dataType, "tinyint", 7)) {
      dataLen += snprintf(dataBuf + dataLen, maxLen - dataLen, "%d, ", rand_tinyint());
    }  else if (0 == strncasecmp(stbInfo->columns[i].dataType, "bool", 4)) {
      dataLen += snprintf(dataBuf + dataLen, maxLen - dataLen, "%d, ", rand_bool());
    }  else if (0 == strncasecmp(stbInfo->columns[i].dataType, "timestamp", 9)) {
      dataLen += snprintf(dataBuf + dataLen, maxLen - dataLen, "%"PRId64", ", rand_bigint());
    }  else {
      printf("No support data type: %s\n", stbInfo->columns[i].dataType);
      return (-1);
    }
  }
  dataLen -= 2;
  dataLen += snprintf(dataBuf + dataLen, maxLen - dataLen, ")");
  
  return dataLen;
}

void syncWriteForNumberOfTblInOneSql(threadInfo *winfo, FILE *fp, char* sampleDataBuf) {
  SSuperTable* superTblInfo = winfo->superTblInfo;

  int   samplePos = 0;

  //printf("========threadID[%d], table rang: %d - %d \n", winfo->threadID, winfo->start_table_id, winfo->end_table_id);
  int64_t    totalRowsInserted = 0;
  int64_t    totalAffectedRows = 0;
  int64_t    lastPrintTime = taosGetTimestampMs();

  char* buffer = calloc(superTblInfo->maxSqlLen+1, 1);
  if (NULL == buffer) {
    printf("========calloc size[ %d ] fail!\n", superTblInfo->maxSqlLen);
    return;
  }

  int32_t  numberOfTblInOneSql = superTblInfo->numberOfTblInOneSql;
  int32_t  tbls = winfo->end_table_id - winfo->start_table_id + 1;
  if (numberOfTblInOneSql > tbls) {
    numberOfTblInOneSql = tbls;
  }

  int64_t time_counter = winfo->start_time;
  int64_t tmp_time;
  int sampleUsePos;

  int64_t st = 0;
  int64_t et = 0;
  for (int i = 0; i < superTblInfo->insertRows;) {
    if (superTblInfo->insertRate && (et - st) < 1000) {
      taosMsleep(1000 - (et - st)); // ms
      //printf("========sleep duration:%"PRId64 "========inserted rows:%d, table range:%d - %d\n", (1000 - (et - st)), i, winfo->start_table_id, winfo->end_table_id);
    }

    if (superTblInfo->insertRate) {
      st = taosGetTimestampMs();
    }

    int32_t  tbl_id = 0;
    for (int tID = winfo->start_table_id; tID <= winfo->end_table_id; ) {
      int inserted = i;

      int k = 0;
      int batchRowsSql = 0;
      while (1)
      {        
        int len = 0;
        memset(buffer, 0, superTblInfo->maxSqlLen);
        char *pstr = buffer;

        int32_t  end_tbl_id = tID + numberOfTblInOneSql;
        if (end_tbl_id > winfo->end_table_id) {
          end_tbl_id = winfo->end_table_id+1;
        }
        for (tbl_id = tID; tbl_id < end_tbl_id; tbl_id++) {  
          sampleUsePos = samplePos;
          if (AUTO_CREATE_SUBTBL == superTblInfo->autoCreateTable) {
            char* tagsValBuf = NULL;
            if (0 == superTblInfo->tagSource) {
              tagsValBuf = generateTagVaulesForStb(superTblInfo);
            } else {
              tagsValBuf = getTagValueFromTagSample(superTblInfo, tbl_id % superTblInfo->tagSampleCount);
            }
            if (NULL == tagsValBuf) {
              goto free_and_statistics;
            }

            if (0 == len) {
              len += snprintf(pstr + len, superTblInfo->maxSqlLen - len, "insert into %s.%s%d using %s.%s tags %s values ", winfo->db_name, superTblInfo->childTblPrefix, tbl_id, winfo->db_name, superTblInfo->sTblName, tagsValBuf);
            } else {
              len += snprintf(pstr + len, superTblInfo->maxSqlLen - len, " %s.%s%d using %s.%s tags %s values ", winfo->db_name, superTblInfo->childTblPrefix, tbl_id, winfo->db_name, superTblInfo->sTblName, tagsValBuf);
            }
            tmfree(tagsValBuf);
          } else if (TBL_ALREADY_EXISTS == superTblInfo->childTblExists) {
            if (0 == len) {
              len += snprintf(pstr + len, superTblInfo->maxSqlLen - len, "insert into %s.%s values ", winfo->db_name, superTblInfo->childTblName + tbl_id * TSDB_TABLE_NAME_LEN);
            } else {
              len += snprintf(pstr + len, superTblInfo->maxSqlLen - len, " %s.%s values ", winfo->db_name, superTblInfo->childTblName + tbl_id * TSDB_TABLE_NAME_LEN);
            }
          } else {  // pre-create child table
            if (0 == len) {
              len += snprintf(pstr + len, superTblInfo->maxSqlLen - len, "insert into %s.%s%d values ", winfo->db_name, superTblInfo->childTblPrefix, tbl_id);
            } else {
              len += snprintf(pstr + len, superTblInfo->maxSqlLen - len, " %s.%s%d values ", winfo->db_name, superTblInfo->childTblPrefix, tbl_id);
            }            
          }
          
          tmp_time = time_counter;
          for (k = 0; k < superTblInfo->rowsPerTbl;) {
            int retLen = 0;
            if (0 == strncasecmp(superTblInfo->dataSource, "sample", 6)) {
              retLen = getRowDataFromSample(pstr + len, superTblInfo->maxSqlLen - len, tmp_time += superTblInfo->timeStampStep, superTblInfo, &sampleUsePos, fp, sampleDataBuf);
              if (retLen < 0) {
                goto free_and_statistics;
              }
            } else if (0 == strncasecmp(superTblInfo->dataSource, "rand", 8)) {        
              int rand_num = rand_tinyint() % 100;            
              if (0 != superTblInfo->disorderRatio && rand_num < superTblInfo->disorderRatio) {
                int64_t d = tmp_time - rand() % superTblInfo->disorderRange;
                retLen = generateRowData(pstr + len, superTblInfo->maxSqlLen - len, d, superTblInfo);
              } else {
                retLen = generateRowData(pstr + len, superTblInfo->maxSqlLen - len, tmp_time += superTblInfo->timeStampStep, superTblInfo);
              }
              if (retLen < 0) {
                goto free_and_statistics;
              }
            }
            len += retLen;
            //inserted++;
            k++;
            totalRowsInserted++;
            batchRowsSql++;
    
            if (inserted >= superTblInfo->insertRows || (superTblInfo->maxSqlLen - len) < (superTblInfo->lenOfOneRow + 128) || batchRowsSql >= INT16_MAX - 1) {
              tID = tbl_id + 1;
              printf("config rowsPerTbl and numberOfTblInOneSql not match with max_sql_lenth, please reconfig![lenOfOneRow:%d]\n", superTblInfo->lenOfOneRow);
              goto send_to_server;
            }
          }
          
        }

        tID = tbl_id;
        inserted += superTblInfo->rowsPerTbl;

        send_to_server:
        batchRowsSql = 0;
        if (0 == strncasecmp(superTblInfo->insertMode, "taosc", 5)) {    
          //printf("multi table===== sql: %s \n\n", buffer);
          //int64_t t1 = taosGetTimestampMs();
          int affectedRows = queryDbExec(winfo->taos, buffer, INSERT_TYPE);
          if (0 > affectedRows) {
            goto free_and_statistics;
          }          
          totalAffectedRows += affectedRows;

          int64_t  currentPrintTime = taosGetTimestampMs();
          if (currentPrintTime - lastPrintTime > 30*1000) {
            printf("thread[%d] has currently inserted rows: %"PRId64 ", affected rows: %"PRId64 "\n", winfo->threadID, totalRowsInserted, totalAffectedRows);
            lastPrintTime = currentPrintTime;
          }
          //int64_t t2 = taosGetTimestampMs();          
          //printf("taosc insert sql return, Spent %.4f seconds \n", (double)(t2 - t1)/1000.0);          
        } else {
          #ifdef TD_LOWA_CURL
          //int64_t t1 = taosGetTimestampMs();
          int retCode = curlProceSql(g_Dbs.host, g_Dbs.port, buffer, winfo->curl_handle);
          //int64_t t2 = taosGetTimestampMs();          
          //printf("http insert sql return, Spent %ld ms \n", t2 - t1);
          
          if (0 != retCode) {
            printf("========curl return fail, threadID[%d]\n", winfo->threadID);
            goto free_and_statistics;
          }
          #else
          printf("========no use http mode for no curl lib!\n");
          goto free_and_statistics;
          #endif
        }
        
        //printf("========tID:%d, k:%d, loop_cnt:%d\n", tID, k, loop_cnt);
        break;
      }

      if (tID > winfo->end_table_id) {
        if (0 == strncasecmp(superTblInfo->dataSource, "sample", 6)) {
          samplePos = sampleUsePos;
        }
        i = inserted;
        time_counter = tmp_time;
      }
    }   
    
    if (superTblInfo->insertRate) {
      et = taosGetTimestampMs();
    }
    //printf("========loop %d childTables duration:%"PRId64 "========inserted rows:%d\n", winfo->end_table_id - winfo->start_table_id, et - st, i);
  }

  free_and_statistics:
  tmfree(buffer);    
  winfo->totalRowsInserted = totalRowsInserted;
  winfo->totalAffectedRows = totalAffectedRows;
  printf("====thread[%d] completed total inserted rows: %"PRId64 ", affected rows: %"PRId64 "====\n", winfo->threadID, totalRowsInserted, totalAffectedRows);
  return;
}

// sync insertion
/*
   1 thread: 100 tables * 2000  rows/s
   1 thread: 10  tables * 20000 rows/s
   6 thread: 300 tables * 2000  rows/s

   2 taosinsertdata , 1 thread:  10  tables * 20000 rows/s
*/
void *syncWrite(void *sarg) {
  int64_t    totalRowsInserted = 0;
  int64_t    totalAffectedRows = 0;
  int64_t    lastPrintTime = taosGetTimestampMs();
  
  threadInfo *winfo = (threadInfo *)sarg; 
  SSuperTable* superTblInfo = winfo->superTblInfo;

  FILE *fp = NULL;
  char* sampleDataBuf = NULL;
  int   samplePos     = 0;

  // each thread read sample data from csv file 
  if (0 == strncasecmp(superTblInfo->dataSource, "sample", 6)) {
    sampleDataBuf = calloc(superTblInfo->lenOfOneRow * MAX_SAMPLES_ONCE_FROM_FILE, 1);
    if (sampleDataBuf == NULL) {
      printf("Failed to calloc %d Bytes, reason:%s\n", superTblInfo->lenOfOneRow * MAX_SAMPLES_ONCE_FROM_FILE, strerror(errno));
      return NULL;
    }
    
    fp = fopen(superTblInfo->sampleFile, "r");
    if (fp == NULL) {
      printf("Failed to open sample file: %s, reason:%s\n", superTblInfo->sampleFile, strerror(errno));
      tmfree(sampleDataBuf);
      return NULL;
    }
    int ret = readSampleFromCsvFileToMem(fp, superTblInfo, sampleDataBuf);
    if (0 != ret) {
      tmfree(sampleDataBuf);
      tmfclose(fp);
      return NULL;
    }
  }

  if (superTblInfo->numberOfTblInOneSql > 0) {
    syncWriteForNumberOfTblInOneSql(winfo, fp, sampleDataBuf);
    tmfree(sampleDataBuf);
    tmfclose(fp);
    return NULL;
  }

  //printf("========threadID[%d], table rang: %d - %d \n", winfo->threadID, winfo->start_table_id, winfo->end_table_id);

  char* buffer = calloc(superTblInfo->maxSqlLen, 1);

  int nrecords_per_request = 0;
  if (AUTO_CREATE_SUBTBL == superTblInfo->autoCreateTable) {
    nrecords_per_request = (superTblInfo->maxSqlLen - 1280 - superTblInfo->lenOfTagOfOneRow) / superTblInfo->lenOfOneRow;
  } else {
    nrecords_per_request = (superTblInfo->maxSqlLen - 1280) / superTblInfo->lenOfOneRow;
  }  

  int nrecords_no_last_req = nrecords_per_request;
  int nrecords_last_req = 0;
  int loop_cnt = 0;
  if (0 != superTblInfo->insertRate) { 
    if (nrecords_no_last_req >= superTblInfo->insertRate) {
      nrecords_no_last_req = superTblInfo->insertRate;
    } else {  
      nrecords_last_req = superTblInfo->insertRate % nrecords_per_request;
      loop_cnt = (superTblInfo->insertRate / nrecords_per_request) + (superTblInfo->insertRate % nrecords_per_request ? 1 : 0) ;
    }
  }
  
  if (nrecords_no_last_req <= 0) {
    nrecords_no_last_req = 1;
  }

  if (nrecords_no_last_req >= INT16_MAX) {
    nrecords_no_last_req = INT16_MAX - 1;
  }

  if (nrecords_last_req >= INT16_MAX) {
    nrecords_last_req = INT16_MAX - 1;
  }

  int nrecords_cur_req = nrecords_no_last_req;
  int loop_cnt_orig = loop_cnt;

  //printf("========nrecords_per_request:%d, nrecords_no_last_req:%d, nrecords_last_req:%d, loop_cnt:%d\n", nrecords_per_request, nrecords_no_last_req, nrecords_last_req, loop_cnt);

  int64_t time_counter = winfo->start_time;

  int64_t st = 0;
  int64_t et = 0;
  for (int i = 0; i < superTblInfo->insertRows;) {
    if (superTblInfo->insertRate && (et - st) < 1000) {
      taosMsleep(1000 - (et - st)); // ms
      //printf("========sleep duration:%"PRId64 "========inserted rows:%d, table range:%d - %d\n", (1000 - (et - st)), i, winfo->start_table_id, winfo->end_table_id);
    }    

    if (superTblInfo->insertRate) {
      st = taosGetTimestampMs();
    }
    
    for (int tID = winfo->start_table_id; tID <= winfo->end_table_id; tID++) {
      int inserted = i;
      int64_t tmp_time = time_counter;

      int sampleUsePos = samplePos;
      int k = 0;
      while (1)
      {        
        int len = 0;
        memset(buffer, 0, superTblInfo->maxSqlLen);
        char *pstr = buffer;

        if (AUTO_CREATE_SUBTBL == superTblInfo->autoCreateTable) {
          char* tagsValBuf = NULL;
          if (0 == superTblInfo->tagSource) {
            tagsValBuf = generateTagVaulesForStb(superTblInfo);
          } else {
            tagsValBuf = getTagValueFromTagSample(superTblInfo, tID % superTblInfo->tagSampleCount);
          }
          if (NULL == tagsValBuf) {
            goto free_and_statistics_2;
          }
        
          len += snprintf(pstr + len, superTblInfo->maxSqlLen - len, "insert into %s.%s%d using %s.%s tags %s values", winfo->db_name, superTblInfo->childTblPrefix, tID, winfo->db_name, superTblInfo->sTblName, tagsValBuf);
          tmfree(tagsValBuf);
        } else if (TBL_ALREADY_EXISTS == superTblInfo->childTblExists) {
          len += snprintf(pstr + len, superTblInfo->maxSqlLen - len, "insert into %s.%s values", winfo->db_name, superTblInfo->childTblName + tID * TSDB_TABLE_NAME_LEN);
        } else {
          len += snprintf(pstr + len, superTblInfo->maxSqlLen - len, "insert into %s.%s%d values", winfo->db_name, superTblInfo->childTblPrefix, tID);
        }
        
        for (k = 0; k < nrecords_cur_req;) {
          int retLen = 0;
          if (0 == strncasecmp(superTblInfo->dataSource, "sample", 6)) {
            retLen = getRowDataFromSample(pstr + len, superTblInfo->maxSqlLen - len, tmp_time += superTblInfo->timeStampStep, superTblInfo, &sampleUsePos, fp, sampleDataBuf);
            if (retLen < 0) {
              goto free_and_statistics_2;
            }
          } else if (0 == strncasecmp(superTblInfo->dataSource, "rand", 8)) {        
            int rand_num = rand_tinyint() % 100;
            if (0 != superTblInfo->disorderRatio && rand_num < superTblInfo->disorderRatio) {
              int64_t d = tmp_time - rand() % superTblInfo->disorderRange;
              retLen = generateRowData(pstr + len, superTblInfo->maxSqlLen - len, d, superTblInfo);
              //printf("disorder rows, rand_num:%d, last ts:%"PRId64" current ts:%"PRId64"\n", rand_num, tmp_time, d);
            } else {
              retLen = generateRowData(pstr + len, superTblInfo->maxSqlLen - len, tmp_time += superTblInfo->timeStampStep, superTblInfo);
            }
            if (retLen < 0) {
              goto free_and_statistics_2;
            }
          }
          len += retLen;
          inserted++;
          k++;
          totalRowsInserted++;
  
          if (inserted >= superTblInfo->insertRows || (superTblInfo->maxSqlLen - len) < (superTblInfo->lenOfOneRow + 128)) break;
        }
  
        if (0 == strncasecmp(superTblInfo->insertMode, "taosc", 5)) {     
          //printf("===== sql: %s \n\n", buffer);
          //int64_t t1 = taosGetTimestampMs();
          int affectedRows = queryDbExec(winfo->taos, buffer, INSERT_TYPE);
          if (0 > affectedRows){
            goto free_and_statistics_2;
          }
          totalAffectedRows += affectedRows;

          int64_t  currentPrintTime = taosGetTimestampMs();
          if (currentPrintTime - lastPrintTime > 30*1000) {
            printf("thread[%d] has currently inserted rows: %"PRId64 ", affected rows: %"PRId64 "\n", winfo->threadID, totalRowsInserted, totalAffectedRows);
            lastPrintTime = currentPrintTime;
          }
          //int64_t t2 = taosGetTimestampMs();          
          //printf("taosc insert sql return, Spent %.4f seconds \n", (double)(t2 - t1)/1000.0);  
        } else {
          #ifdef TD_LOWA_CURL
          //int64_t t1 = taosGetTimestampMs();
          int retCode = curlProceSql(g_Dbs.host, g_Dbs.port, buffer, winfo->curl_handle);
          //int64_t t2 = taosGetTimestampMs();          
          //printf("http insert sql return, Spent %ld ms \n", t2 - t1);
          
          if (0 != retCode) {
            printf("========curl return fail, threadID[%d]\n", winfo->threadID);
            goto free_and_statistics_2;
          }
          #else
          printf("========no use http mode for no curl lib!\n");
          goto free_and_statistics_2;
          #endif
        }
        
        //printf("========tID:%d, k:%d, loop_cnt:%d\n", tID, k, loop_cnt);
        
        if (loop_cnt) {
          loop_cnt--;
          if ((1 == loop_cnt) && (0 != nrecords_last_req)) {
            nrecords_cur_req = nrecords_last_req;
          } else if (0 == loop_cnt){
            nrecords_cur_req = nrecords_no_last_req;
            loop_cnt = loop_cnt_orig;
            break;
          }  
        } else {
          break;
        }    
      }

      if (tID == winfo->end_table_id) {
        if (0 == strncasecmp(superTblInfo->dataSource, "sample", 6)) {
          samplePos = sampleUsePos;
        } 
        i = inserted;
        time_counter = tmp_time;
      }
    }   
    
    if (superTblInfo->insertRate) {
      et = taosGetTimestampMs();
    }
    //printf("========loop %d childTables duration:%"PRId64 "========inserted rows:%d\n", winfo->end_table_id - winfo->start_table_id, et - st, i);
  }

  free_and_statistics_2:
  tmfree(buffer);
  tmfree(sampleDataBuf);
  tmfclose(fp);

  winfo->totalRowsInserted = totalRowsInserted;
  winfo->totalAffectedRows = totalAffectedRows;
  
  printf("====thread[%d] completed total inserted rows: %"PRId64 ", total affected rows: %"PRId64 "====\n", winfo->threadID, totalRowsInserted, totalAffectedRows);
  return NULL;
}

void callBack(void *param, TAOS_RES *res, int code) {
  threadInfo* winfo = (threadInfo*)param; 

  if (winfo->superTblInfo->insertRate) {
    winfo->et = taosGetTimestampMs();
    if (winfo->et - winfo->st < 1000) {
      taosMsleep(1000 - (winfo->et - winfo->st)); // ms
    }
  }
  
  char *buffer = calloc(1, winfo->superTblInfo->maxSqlLen);
  char *data   = calloc(1, MAX_DATA_SIZE);
  char *pstr = buffer;
  pstr += sprintf(pstr, "insert into %s.%s%d values", winfo->db_name, winfo->tb_prefix, winfo->start_table_id);
  if (winfo->counter >= winfo->superTblInfo->insertRows) {
    winfo->start_table_id++;
    winfo->counter = 0;
  }
  if (winfo->start_table_id > winfo->end_table_id) {
    tsem_post(&winfo->lock_sem);
    free(buffer);
    free(data);
    taos_free_result(res);
    return;
  }
  
  for (int i = 0; i < winfo->nrecords_per_request; i++) {
    int rand_num = rand() % 100;
    if (0 != winfo->superTblInfo->disorderRatio && rand_num < winfo->superTblInfo->disorderRatio)
    {
      int64_t d = winfo->lastTs - rand() % 1000000 + rand_num;
      //generateData(data, datatype, ncols_per_record, d, len_of_binary);
      (void)generateRowData(data, MAX_DATA_SIZE, d, winfo->superTblInfo);
    } else {
      //generateData(data, datatype, ncols_per_record, tmp_time += 1000, len_of_binary);
      (void)generateRowData(data, MAX_DATA_SIZE, winfo->lastTs += 1000, winfo->superTblInfo);
    }
    pstr += sprintf(pstr, "%s", data);
    winfo->counter++;

    if (winfo->counter >= winfo->superTblInfo->insertRows) {
      break;
    }
  }
  
  if (winfo->superTblInfo->insertRate) {
    winfo->st = taosGetTimestampMs();
  }
  taos_query_a(winfo->taos, buffer, callBack, winfo);
  free(buffer);
  free(data);

  taos_free_result(res);
}

void *asyncWrite(void *sarg) {
  threadInfo *winfo = (threadInfo *)sarg;

  winfo->nrecords_per_request = 0;
  //if (AUTO_CREATE_SUBTBL == winfo->superTblInfo->autoCreateTable) {
    winfo->nrecords_per_request = (winfo->superTblInfo->maxSqlLen - 1280 - winfo->superTblInfo->lenOfTagOfOneRow) / winfo->superTblInfo->lenOfOneRow;
  //} else {
  //  winfo->nrecords_per_request = (winfo->superTblInfo->maxSqlLen - 1280) / winfo->superTblInfo->lenOfOneRow;
  //}  

  if (0 != winfo->superTblInfo->insertRate) { 
    if (winfo->nrecords_per_request >= winfo->superTblInfo->insertRate) {
      winfo->nrecords_per_request = winfo->superTblInfo->insertRate;
    }
  }
  
  if (winfo->nrecords_per_request <= 0) {
    winfo->nrecords_per_request = 1;
  }

  if (winfo->nrecords_per_request >= INT16_MAX) {
    winfo->nrecords_per_request = INT16_MAX - 1;
  }

  if (winfo->nrecords_per_request >= INT16_MAX) {
    winfo->nrecords_per_request = INT16_MAX - 1;
  }

  winfo->st = 0;
  winfo->et = 0;
  winfo->lastTs = winfo->start_time;
  
  if (winfo->superTblInfo->insertRate) {
    winfo->st = taosGetTimestampMs();
  }
  taos_query_a(winfo->taos, "show databases", callBack, winfo);

  tsem_wait(&(winfo->lock_sem));

  return NULL;
}

void startMultiThreadInsertData(int threads, char* db_name, char* precision, SSuperTable* superTblInfo) {
  pthread_t *pids = malloc(threads * sizeof(pthread_t));
  threadInfo *infos = malloc(threads * sizeof(threadInfo));
  memset(pids, 0, threads * sizeof(pthread_t));
  memset(infos, 0, threads * sizeof(threadInfo));
  int ntables = superTblInfo->childTblCount;

  int a = ntables / threads;
  if (a < 1) {
    threads = ntables;
    a = 1;
  }

  int b = 0;
  if (threads != 0) {
    b = ntables % threads;
  }

  //TAOS* taos;
  //if (0 == strncasecmp(superTblInfo->insertMode, "taosc", 5)) {
  //  taos = taos_connect(g_Dbs.host, g_Dbs.user, g_Dbs.password, db_name, g_Dbs.port);
  //  if (NULL == taos) {
  //    printf("connect to server fail, reason: %s\n", taos_errstr(NULL));
  //    exit(-1);
  //  }
  //}

  int32_t timePrec = TSDB_TIME_PRECISION_MILLI;
  if (0 != precision[0]) {
    if (0 == strncasecmp(precision, "ms", 2)) {
      timePrec = TSDB_TIME_PRECISION_MILLI;
    }  else if (0 == strncasecmp(precision, "us", 2)) {
      timePrec = TSDB_TIME_PRECISION_MICRO;
    }  else {
      printf("No support precision: %s\n", precision);
      exit(-1);
    }
  }

  int64_t start_time;  
  if (0 == strncasecmp(superTblInfo->startTimestamp, "now", 3)) {
    start_time = taosGetTimestamp(timePrec);
  } else {    
    (void)taosParseTime(superTblInfo->startTimestamp, &start_time, strlen(superTblInfo->startTimestamp), timePrec, 0);
  }

  double start = getCurrentTime();
  
  int last = 0;
  for (int i = 0; i < threads; i++) {
    threadInfo *t_info = infos + i;
    t_info->threadID = i;
    tstrncpy(t_info->db_name, db_name, MAX_DB_NAME_SIZE);
    t_info->superTblInfo = superTblInfo;

    t_info->start_time = start_time;

    if (0 == strncasecmp(superTblInfo->insertMode, "taosc", 5)) {
      //t_info->taos = taos;
      t_info->taos = taos_connect(g_Dbs.host, g_Dbs.user, g_Dbs.password, db_name, g_Dbs.port);
      if (NULL == t_info->taos) {
        printf("connect to server fail from insert sub thread, reason: %s\n", taos_errstr(NULL));
        exit(-1);
      }
    } else {
      t_info->taos = NULL;
      #ifdef TD_LOWA_CURL
      t_info->curl_handle = curl_easy_init();     
      #endif
    }

    if (0 == superTblInfo->multiThreadWriteOneTbl) {
      t_info->start_table_id = last;
      t_info->end_table_id = i < b ? last + a : last + a - 1;
      last = t_info->end_table_id + 1;
    } else {
      t_info->start_table_id = 0;
      t_info->end_table_id = superTblInfo->childTblCount - 1;
      t_info->start_time = t_info->start_time + rand_int() % 10000 - rand_tinyint();
    }

    tsem_init(&(t_info->lock_sem), 0, 0);
    
    if (SYNC == g_Dbs.queryMode) {
      pthread_create(pids + i, NULL, syncWrite, t_info);
    } else {      
      pthread_create(pids + i, NULL, asyncWrite, t_info);
    }
  }
  
  for (int i = 0; i < threads; i++) {
    pthread_join(pids[i], NULL);
  }

  for (int i = 0; i < threads; i++) {
    threadInfo *t_info = infos + i;

    tsem_destroy(&(t_info->lock_sem));
    taos_close(t_info->taos);

    superTblInfo->totalAffectedRows += t_info->totalAffectedRows;
    superTblInfo->totalRowsInserted += t_info->totalRowsInserted;
    #ifdef TD_LOWA_CURL
    if (t_info->curl_handle) {
      curl_easy_cleanup(t_info->curl_handle);
    }
    #endif
  }

  double end = getCurrentTime();
  
  //taos_close(taos);

  free(pids);
  free(infos);  

  printf("Spent %.4f seconds to insert rows: %"PRId64", affected rows: %"PRId64" with %d thread(s) into %s.%s\n\n", 
          end - start, superTblInfo->totalRowsInserted, superTblInfo->totalAffectedRows, threads, db_name, superTblInfo->sTblName);
  fprintf(g_fpOfInsertResult, "Spent %.4f seconds to insert rows: %"PRId64", affected rows: %"PRId64" with %d thread(s) into %s.%s\n\n", 
          end - start, superTblInfo->totalRowsInserted, superTblInfo->totalAffectedRows, threads, db_name, superTblInfo->sTblName);
}


void *readTable(void *sarg) {
#if 1  
  threadInfo *rinfo = (threadInfo *)sarg;
  TAOS *taos = rinfo->taos;
  char command[BUFFER_SIZE] = "\0";
  int64_t sTime = rinfo->start_time;
  char *tb_prefix = rinfo->tb_prefix;
  FILE *fp = fopen(rinfo->fp, "a");
  if (NULL == fp) {
    printf("fopen %s fail, reason:%s.\n", rinfo->fp, strerror(errno));
    return NULL;
  }

  int num_of_DPT = rinfo->superTblInfo->insertRows; //  nrecords_per_table;
  int num_of_tables = rinfo->end_table_id - rinfo->start_table_id + 1;
  int totalData = num_of_DPT * num_of_tables;
  bool do_aggreFunc = g_Dbs.do_aggreFunc;

  int n = do_aggreFunc ? (sizeof(aggreFunc) / sizeof(aggreFunc[0])) : 2;
  if (!do_aggreFunc) {
    printf("\nThe first field is either Binary or Bool. Aggregation functions are not supported.\n");
  }
  printf("%d records:\n", totalData);
  fprintf(fp, "| QFunctions |    QRecords    |   QSpeed(R/s)   |  QLatency(ms) |\n");

  for (int j = 0; j < n; j++) {
    double totalT = 0;
    int count = 0;
    for (int i = 0; i < num_of_tables; i++) {
      sprintf(command, "select %s from %s%d where ts>= %" PRId64, aggreFunc[j], tb_prefix, i, sTime);

      double t = getCurrentTime();
      TAOS_RES *pSql = taos_query(taos, command);
      int32_t code = taos_errno(pSql);

      if (code != 0) {
        fprintf(stderr, "Failed to query:%s\n", taos_errstr(pSql));
        taos_free_result(pSql);
        taos_close(taos);
        return NULL;
      }

      while (taos_fetch_row(pSql) != NULL) {
        count++;
      }

      t = getCurrentTime() - t;
      totalT += t;

      taos_free_result(pSql);
    }

    fprintf(fp, "|%10s  |   %10d   |  %12.2f   |   %10.2f  |\n",
            aggreFunc[j][0] == '*' ? "   *   " : aggreFunc[j], totalData,
            (double)(num_of_tables * num_of_DPT) / totalT, totalT * 1000);
    printf("select %10s took %.6f second(s)\n", aggreFunc[j], totalT);
  }
  fprintf(fp, "\n");
  fclose(fp);
#endif
  return NULL;
}

void *readMetric(void *sarg) {
#if 1  
  threadInfo *rinfo = (threadInfo *)sarg;
  TAOS *taos = rinfo->taos;
  char command[BUFFER_SIZE] = "\0";
  FILE *fp = fopen(rinfo->fp, "a");
  if (NULL == fp) {
    printf("fopen %s fail, reason:%s.\n", rinfo->fp, strerror(errno));
    return NULL;
  }

  int num_of_DPT = rinfo->superTblInfo->insertRows;
  int num_of_tables = rinfo->end_table_id - rinfo->start_table_id + 1;
  int totalData = num_of_DPT * num_of_tables;
  bool do_aggreFunc = g_Dbs.do_aggreFunc;

  int n = do_aggreFunc ? (sizeof(aggreFunc) / sizeof(aggreFunc[0])) : 2;
  if (!do_aggreFunc) {
    printf("\nThe first field is either Binary or Bool. Aggregation functions are not supported.\n");
  }
  printf("%d records:\n", totalData);
  fprintf(fp, "Querying On %d records:\n", totalData);

  for (int j = 0; j < n; j++) {
    char condition[BUFFER_SIZE - 30] = "\0";
    char tempS[64] = "\0";

    int m = 10 < num_of_tables ? 10 : num_of_tables;

    for (int i = 1; i <= m; i++) {
      if (i == 1) {
        sprintf(tempS, "t1 = %d", i);
      } else {
        sprintf(tempS, " or t1 = %d ", i);
      }
      strcat(condition, tempS);

      sprintf(command, "select %s from meters where %s", aggreFunc[j], condition);

      printf("Where condition: %s\n", condition);
      fprintf(fp, "%s\n", command);

      double t = getCurrentTime();

      TAOS_RES *pSql = taos_query(taos, command);
      int32_t code = taos_errno(pSql);

      if (code != 0) {
        fprintf(stderr, "Failed to query:%s\n", taos_errstr(pSql));
        taos_free_result(pSql);
        taos_close(taos);
        return NULL;
      }
      int count = 0;
      while (taos_fetch_row(pSql) != NULL) {
        count++;
      }
      t = getCurrentTime() - t;

      fprintf(fp, "| Speed: %12.2f(per s) | Latency: %.4f(ms) |\n", num_of_tables * num_of_DPT / t, t * 1000);
      printf("select %10s took %.6f second(s)\n\n", aggreFunc[j], t);

      taos_free_result(pSql);
    }
    fprintf(fp, "\n");
  }
  fclose(fp);
#endif
  return NULL;
}


int insertTestProcess() {

  g_fpOfInsertResult = fopen(g_Dbs.resultFile, "a");
  if (NULL == g_fpOfInsertResult) {
    fprintf(stderr, "Failed to open %s for save result\n", g_Dbs.resultFile);
    return 1;
  };

  printfInsertMeta();
  printfInsertMetaToFile(g_fpOfInsertResult);

  printf("Press enter key to continue\n\n");
  (void)getchar();
 
  init_rand_data();

  // create database and super tables
  (void)createDatabases();

  // pretreatement
  prePareSampleData();
  
  double start;
  double end;

  // create child tables
  start = getCurrentTime();
  createChildTables();
  end = getCurrentTime();
  if (g_totalChildTables > 0) {
    printf("Spent %.4f seconds to create %d tables with %d thread(s)\n\n", end - start, g_totalChildTables, g_Dbs.threadCount);
    fprintf(g_fpOfInsertResult, "Spent %.4f seconds to create %d tables with %d thread(s)\n\n", end - start, g_totalChildTables, g_Dbs.threadCount);
  }
  
  usleep(1000*1000);

  // create sub threads for inserting data
  //start = getCurrentTime();
  for (int i = 0; i < g_Dbs.dbCount; i++) {    
    for (int j = 0; j < g_Dbs.db[i].superTblCount; j++) {
      SSuperTable* superTblInfo = &g_Dbs.db[i].superTbls[j];
      startMultiThreadInsertData(g_Dbs.threadCount, g_Dbs.db[i].dbName, g_Dbs.db[i].dbCfg.precision, superTblInfo);
    }
  }  
  //end = getCurrentTime();
  
  //int64_t    totalRowsInserted = 0;
  //int64_t    totalAffectedRows = 0;
  //for (int i = 0; i < g_Dbs.dbCount; i++) {    
  //  for (int j = 0; j < g_Dbs.db[i].superTblCount; j++) {
  //  totalRowsInserted += g_Dbs.db[i].superTbls[j].totalRowsInserted;
  //  totalAffectedRows += g_Dbs.db[i].superTbls[j].totalAffectedRows;
  //}
  //printf("Spent %.4f seconds to insert rows: %"PRId64", affected rows: %"PRId64" with %d thread(s)\n\n", end - start, totalRowsInserted, totalAffectedRows, g_Dbs.threadCount);
  if (NULL == g_args.metaFile && false == g_Dbs.insert_only) {
    // query data
    pthread_t read_id;
    threadInfo *rInfo = malloc(sizeof(threadInfo));
    rInfo->start_time = 1500000000000;  // 2017-07-14 10:40:00.000
    rInfo->start_table_id = 0;
    rInfo->end_table_id = g_Dbs.db[0].superTbls[0].childTblCount - 1;
    //rInfo->do_aggreFunc = g_Dbs.do_aggreFunc;
    //rInfo->nrecords_per_table = g_Dbs.db[0].superTbls[0].insertRows;
    rInfo->superTblInfo = &g_Dbs.db[0].superTbls[0];
    rInfo->taos = taos_connect(g_Dbs.host, g_Dbs.user, g_Dbs.password, g_Dbs.db[0].dbName, g_Dbs.port);
    strcpy(rInfo->tb_prefix, g_Dbs.db[0].superTbls[0].childTblPrefix);
    strcpy(rInfo->fp, g_Dbs.resultFile);

    if (!g_Dbs.use_metric) {
      pthread_create(&read_id, NULL, readTable, rInfo);
    } else {
      pthread_create(&read_id, NULL, readMetric, rInfo);
    }
    pthread_join(read_id, NULL);
    taos_close(rInfo->taos);
  }

  postFreeResource();
  
  return 0;
}

void *superQueryProcess(void *sarg) {
  threadInfo *winfo = (threadInfo *)sarg; 

  //char sqlStr[MAX_TB_NAME_SIZE*2];
  //sprintf(sqlStr, "use %s", g_queryInfo.dbName);
  //queryDB(winfo->taos, sqlStr);
  
  int64_t st = 0;
  int64_t et = 0;
  while (1) {
    if (g_queryInfo.superQueryInfo.rate && (et - st) < g_queryInfo.superQueryInfo.rate*1000) {
      taosMsleep(g_queryInfo.superQueryInfo.rate*1000 - (et - st)); // ms
      //printf("========sleep duration:%"PRId64 "========inserted rows:%d, table range:%d - %d\n", (1000 - (et - st)), i, winfo->start_table_id, winfo->end_table_id);
    }

    st = taosGetTimestampMs();
    for (int i = 0; i < g_queryInfo.superQueryInfo.sqlCount; i++) {
      if (0 == strncasecmp(g_queryInfo.queryMode, "taosc", 5)) {          
        int64_t t1 = taosGetTimestampUs();
        char tmpFile[MAX_FILE_NAME_LEN*2] = {0};
        if (g_queryInfo.superQueryInfo.result[i][0] != 0) {
          sprintf(tmpFile, "%s-%d", g_queryInfo.superQueryInfo.result[i], winfo->threadID);
        }
        selectAndGetResult(winfo->taos, g_queryInfo.superQueryInfo.sql[i], tmpFile); 
        int64_t t2 = taosGetTimestampUs();          
        printf("=[taosc] thread[%"PRIu64"] complete one sql, Spent %f s\n", (uint64_t)pthread_self(), (t2 - t1)/1000000.0);          
      } else {
        #ifdef TD_LOWA_CURL
        int64_t t1 = taosGetTimestampUs();
        int retCode = curlProceSql(g_queryInfo.host, g_queryInfo.port, g_queryInfo.superQueryInfo.sql[i], winfo->curl_handle);
        int64_t t2 = taosGetTimestampUs();          
        printf("=[restful] thread[%"PRIu64"] complete one sql, Spent %f s\n", (uint64_t)pthread_self(), (t2 - t1)/1000000.0);
        
        if (0 != retCode) {
          printf("====curl return fail, threadID[%d]\n", winfo->threadID);
          return NULL;
        }
        #endif
      }   
    }
    et = taosGetTimestampMs();
    printf("==thread[%"PRIu64"] complete all sqls to specify tables once queries duration:%.6fs\n\n", (uint64_t)pthread_self(), (double)(et - st)/1000.0);
  }
  return NULL;
}

void replaceSubTblName(char* inSql, char* outSql, int tblIndex) {
  char sourceString[32] = "xxxx";
  char subTblName[MAX_TB_NAME_SIZE*3];
  sprintf(subTblName, "%s.%s", g_queryInfo.dbName, g_queryInfo.subQueryInfo.childTblName + tblIndex*TSDB_TABLE_NAME_LEN);

  //printf("inSql: %s\n", inSql);
  
  char* pos = strstr(inSql, sourceString);
  if (0 == pos) {
    return; 
  }
  
  strncpy(outSql, inSql, pos - inSql);
  //printf("1: %s\n", outSql);
  strcat(outSql, subTblName);  
  //printf("2: %s\n", outSql);  
  strcat(outSql, pos+strlen(sourceString));  
  //printf("3: %s\n", outSql); 
}

void *subQueryProcess(void *sarg) {
  char sqlstr[1024];
  threadInfo *winfo = (threadInfo *)sarg; 
  int64_t st = 0;
  int64_t et = g_queryInfo.subQueryInfo.rate*1000;
  while (1) {
    if (g_queryInfo.subQueryInfo.rate && (et - st) < g_queryInfo.subQueryInfo.rate*1000) {
      taosMsleep(g_queryInfo.subQueryInfo.rate*1000 - (et - st)); // ms
      //printf("========sleep duration:%"PRId64 "========inserted rows:%d, table range:%d - %d\n", (1000 - (et - st)), i, winfo->start_table_id, winfo->end_table_id);
    }

    st = taosGetTimestampMs();
    for (int i = winfo->start_table_id; i <= winfo->end_table_id; i++) {
      for (int i = 0; i < g_queryInfo.subQueryInfo.sqlCount; i++) {
        memset(sqlstr,0,sizeof(sqlstr));
        replaceSubTblName(g_queryInfo.subQueryInfo.sql[i], sqlstr, i);
        char tmpFile[MAX_FILE_NAME_LEN*2] = {0};
        if (g_queryInfo.subQueryInfo.result[i][0] != 0) {
          sprintf(tmpFile, "%s-%d", g_queryInfo.subQueryInfo.result[i], winfo->threadID);
        }
        selectAndGetResult(winfo->taos, sqlstr, tmpFile); 
      }
    }
    et = taosGetTimestampMs();
    printf("####thread[%"PRIu64"] complete all sqls to allocate all sub-tables[%d - %d] once queries duration:%.4fs\n\n", (uint64_t)pthread_self(), winfo->start_table_id, winfo->end_table_id, (double)(et - st)/1000.0);
  }
  return NULL;
}

int queryTestProcess() {
  TAOS * taos = NULL;  
  taos_init();
  taos = taos_connect(g_queryInfo.host, g_queryInfo.user, g_queryInfo.password, g_queryInfo.dbName, g_queryInfo.port);
  if (taos == NULL) {
    fprintf(stderr, "Failed to connect to TDengine, reason:%s\n", taos_errstr(NULL));
    exit(-1);
  }

  if (0 != g_queryInfo.subQueryInfo.sqlCount) {
    (void)getAllChildNameOfSuperTable(taos, g_queryInfo.dbName, g_queryInfo.subQueryInfo.sTblName, &g_queryInfo.subQueryInfo.childTblName, &g_queryInfo.subQueryInfo.childTblCount);
  }  
  
  printfQueryMeta();
  printf("Press enter key to continue\n\n");
  (void)getchar();
  
  pthread_t  *pids  = NULL;
  threadInfo *infos = NULL;
  //==== create sub threads for query from specify table
  if (g_queryInfo.superQueryInfo.sqlCount > 0 && g_queryInfo.superQueryInfo.concurrent > 0) {
    
    pids  = malloc(g_queryInfo.superQueryInfo.concurrent * sizeof(pthread_t));
    infos = malloc(g_queryInfo.superQueryInfo.concurrent * sizeof(threadInfo));
    if ((NULL == pids) || (NULL == infos)) {
      printf("malloc failed for create threads\n");
      taos_close(taos);
      exit(-1);
    }
    
    for (int i = 0; i < g_queryInfo.superQueryInfo.concurrent; i++) {  
      threadInfo *t_info = infos + i;
      t_info->threadID = i;    
  
      if (0 == strncasecmp(g_queryInfo.queryMode, "taosc", 5)) {
        t_info->taos = taos;
        
        char sqlStr[MAX_TB_NAME_SIZE*2];
        sprintf(sqlStr, "use %s", g_queryInfo.dbName);
        (void)queryDbExec(t_info->taos, sqlStr, NO_INSERT_TYPE);
      } else {
        t_info->taos = NULL;
        #ifdef TD_LOWA_CURL
        t_info->curl_handle = curl_easy_init();      
        #endif
      }
  
      pthread_create(pids + i, NULL, superQueryProcess, t_info);    
    }  
  }else {
    g_queryInfo.superQueryInfo.concurrent = 0;
  }
  
  pthread_t  *pidsOfSub  = NULL;
  threadInfo *infosOfSub = NULL;
  //==== create sub threads for query from all sub table of the super table
  if ((g_queryInfo.subQueryInfo.sqlCount > 0) && (g_queryInfo.subQueryInfo.threadCnt > 0)) {
    pidsOfSub  = malloc(g_queryInfo.subQueryInfo.threadCnt * sizeof(pthread_t));
    infosOfSub = malloc(g_queryInfo.subQueryInfo.threadCnt * sizeof(threadInfo));
    if ((NULL == pidsOfSub) || (NULL == infosOfSub)) {
      printf("malloc failed for create threads\n");
      taos_close(taos);
      exit(-1);
    }
    
    int ntables = g_queryInfo.subQueryInfo.childTblCount;
    int threads = g_queryInfo.subQueryInfo.threadCnt;
  
    int a = ntables / threads;
    if (a < 1) {
      threads = ntables;
      a = 1;
    }
  
    int b = 0;
    if (threads != 0) {
      b = ntables % threads;
    }
    
    int last = 0;
    for (int i = 0; i < threads; i++) {  
      threadInfo *t_info = infosOfSub + i;
      t_info->threadID = i;
      
      t_info->start_table_id = last;
      t_info->end_table_id = i < b ? last + a : last + a - 1;
      last = t_info->end_table_id + 1;
      t_info->taos = taos;
      pthread_create(pidsOfSub + i, NULL, subQueryProcess, t_info);
    }

    g_queryInfo.subQueryInfo.threadCnt = threads;
  }else {
    g_queryInfo.subQueryInfo.threadCnt = 0;
  }  
  
  for (int i = 0; i < g_queryInfo.superQueryInfo.concurrent; i++) {
    pthread_join(pids[i], NULL);
  }

  tmfree((char*)pids);
  tmfree((char*)infos);  
  
  for (int i = 0; i < g_queryInfo.subQueryInfo.threadCnt; i++) {
    pthread_join(pidsOfSub[i], NULL);
  }

  tmfree((char*)pidsOfSub);
  tmfree((char*)infosOfSub);  
  
  taos_close(taos);
  return 0;
}

static void subscribe_callback(TAOS_SUB* tsub, TAOS_RES *res, void* param, int code) {  
  if (res == NULL || taos_errno(res) != 0) {
    printf("failed to subscribe result, code:%d, reason:%s\n", code, taos_errstr(res));
    return;
  }
  
  getResult(res, (char*)param);
  taos_free_result(res);
}

static TAOS_SUB* subscribeImpl(TAOS *taos, char *sql, char* topic, char* resultFileName) {
  TAOS_SUB* tsub = NULL;  

  if (g_queryInfo.superQueryInfo.subscribeMode) {
    tsub = taos_subscribe(taos, g_queryInfo.superQueryInfo.subscribeRestart, topic, sql, subscribe_callback, (void*)resultFileName, g_queryInfo.superQueryInfo.subscribeInterval);
  } else {
    tsub = taos_subscribe(taos, g_queryInfo.superQueryInfo.subscribeRestart, topic, sql, NULL, NULL, 0);
  }

  if (tsub == NULL) {
    printf("failed to create subscription. topic:%s, sql:%s\n", topic, sql);
    return NULL;
  } 

  return tsub;
}

void *subSubscribeProcess(void *sarg) {
  threadInfo *winfo = (threadInfo *)sarg; 
  char subSqlstr[1024];

  char sqlStr[MAX_TB_NAME_SIZE*2];
  sprintf(sqlStr, "use %s", g_queryInfo.dbName);
  if (0 != queryDbExec(winfo->taos, sqlStr, NO_INSERT_TYPE)){
    return NULL;
  }
  
  //int64_t st = 0;
  //int64_t et = 0;
  do {
    //if (g_queryInfo.superQueryInfo.rate && (et - st) < g_queryInfo.superQueryInfo.rate*1000) {
    //  taosMsleep(g_queryInfo.superQueryInfo.rate*1000 - (et - st)); // ms
    //  //printf("========sleep duration:%"PRId64 "========inserted rows:%d, table range:%d - %d\n", (1000 - (et - st)), i, winfo->start_table_id, winfo->end_table_id);
    //}

    //st = taosGetTimestampMs();
    char topic[32] = {0};
    for (int i = 0; i < g_queryInfo.subQueryInfo.sqlCount; i++) {
      sprintf(topic, "taosdemo-subscribe-%d", i);
      memset(subSqlstr,0,sizeof(subSqlstr));
      replaceSubTblName(g_queryInfo.subQueryInfo.sql[i], subSqlstr, i);
      char tmpFile[MAX_FILE_NAME_LEN*2] = {0};
      if (g_queryInfo.subQueryInfo.result[i][0] != 0) {
        sprintf(tmpFile, "%s-%d", g_queryInfo.subQueryInfo.result[i], winfo->threadID);
      }
      g_queryInfo.subQueryInfo.tsub[i] = subscribeImpl(winfo->taos, subSqlstr, topic, tmpFile); 
      if (NULL == g_queryInfo.subQueryInfo.tsub[i]) {
        return NULL;
      }
    }
    //et = taosGetTimestampMs();
    //printf("========thread[%"PRId64"] complete all sqls to super table once queries duration:%.4fs\n", pthread_self(), (double)(et - st)/1000.0);
  } while (0);

  // start loop to consume result
  TAOS_RES* res = NULL;
  while (1) {
    for (int i = 0; i < g_queryInfo.subQueryInfo.sqlCount; i++) {
      if (1 == g_queryInfo.subQueryInfo.subscribeMode) {
        continue;
      }
      
      res = taos_consume(g_queryInfo.subQueryInfo.tsub[i]);
      if (res) {
        char tmpFile[MAX_FILE_NAME_LEN*2] = {0};
        if (g_queryInfo.subQueryInfo.result[i][0] != 0) {
          sprintf(tmpFile, "%s-%d", g_queryInfo.subQueryInfo.result[i], winfo->threadID);
        }
        getResult(res, tmpFile);
      }
    }
  }
  taos_free_result(res);
  
  for (int i = 0; i < g_queryInfo.subQueryInfo.sqlCount; i++) {
    taos_unsubscribe(g_queryInfo.subQueryInfo.tsub[i], g_queryInfo.subQueryInfo.subscribeKeepProgress);
  }
  return NULL;
}

void *superSubscribeProcess(void *sarg) {
  threadInfo *winfo = (threadInfo *)sarg; 

  char sqlStr[MAX_TB_NAME_SIZE*2];
  sprintf(sqlStr, "use %s", g_queryInfo.dbName);
  if (0 != queryDbExec(winfo->taos, sqlStr, NO_INSERT_TYPE)) {
    return NULL;
  }
  
  //int64_t st = 0;
  //int64_t et = 0;
  do {
    //if (g_queryInfo.superQueryInfo.rate && (et - st) < g_queryInfo.superQueryInfo.rate*1000) {
    //  taosMsleep(g_queryInfo.superQueryInfo.rate*1000 - (et - st)); // ms
    //  //printf("========sleep duration:%"PRId64 "========inserted rows:%d, table range:%d - %d\n", (1000 - (et - st)), i, winfo->start_table_id, winfo->end_table_id);
    //}

    //st = taosGetTimestampMs();
    char topic[32] = {0};
    for (int i = 0; i < g_queryInfo.superQueryInfo.sqlCount; i++) {
      sprintf(topic, "taosdemo-subscribe-%d", i);
      char tmpFile[MAX_FILE_NAME_LEN*2] = {0};
      if (g_queryInfo.subQueryInfo.result[i][0] != 0) {
        sprintf(tmpFile, "%s-%d", g_queryInfo.superQueryInfo.result[i], winfo->threadID);
      }
      g_queryInfo.superQueryInfo.tsub[i] = subscribeImpl(winfo->taos, g_queryInfo.superQueryInfo.sql[i], topic, tmpFile); 
      if (NULL == g_queryInfo.superQueryInfo.tsub[i]) {
        return NULL;
      }
    }
    //et = taosGetTimestampMs();
    //printf("========thread[%"PRId64"] complete all sqls to super table once queries duration:%.4fs\n", pthread_self(), (double)(et - st)/1000.0);
  } while (0);

  // start loop to consume result
  TAOS_RES* res = NULL;
  while (1) {
    for (int i = 0; i < g_queryInfo.superQueryInfo.sqlCount; i++) {
      if (1 == g_queryInfo.superQueryInfo.subscribeMode) {
        continue;
      }
      
      res = taos_consume(g_queryInfo.superQueryInfo.tsub[i]);
      if (res) {
        char tmpFile[MAX_FILE_NAME_LEN*2] = {0};
        if (g_queryInfo.superQueryInfo.result[i][0] != 0) {
          sprintf(tmpFile, "%s-%d", g_queryInfo.superQueryInfo.result[i], winfo->threadID);
        }
        getResult(res, tmpFile);
      }
    }
  }
  taos_free_result(res);
  
  for (int i = 0; i < g_queryInfo.superQueryInfo.sqlCount; i++) {
    taos_unsubscribe(g_queryInfo.superQueryInfo.tsub[i], g_queryInfo.superQueryInfo.subscribeKeepProgress);
  }
  return NULL;
}

int subscribeTestProcess() {
  printfQueryMeta();

  printf("Press enter key to continue\n\n");
  (void)getchar();

  TAOS * taos = NULL;  
  taos_init();
  taos = taos_connect(g_queryInfo.host, g_queryInfo.user, g_queryInfo.password, g_queryInfo.dbName, g_queryInfo.port);
  if (taos == NULL) {
    fprintf(stderr, "Failed to connect to TDengine, reason:%s\n", taos_errstr(NULL));
    exit(-1);
  }

  if (0 != g_queryInfo.subQueryInfo.sqlCount) {
    (void)getAllChildNameOfSuperTable(taos, g_queryInfo.dbName, g_queryInfo.subQueryInfo.sTblName, &g_queryInfo.subQueryInfo.childTblName, &g_queryInfo.subQueryInfo.childTblCount);
  }


  pthread_t  *pids = NULL;
  threadInfo *infos = NULL;
  //==== create sub threads for query from super table
  if (g_queryInfo.superQueryInfo.sqlCount > 0 && g_queryInfo.superQueryInfo.concurrent > 0) {
    pids  = malloc(g_queryInfo.superQueryInfo.concurrent * sizeof(pthread_t));
    infos = malloc(g_queryInfo.superQueryInfo.concurrent * sizeof(threadInfo));
    if ((NULL == pids) || (NULL == infos)) {
      printf("malloc failed for create threads\n");      
      taos_close(taos);
      exit(-1);
    }
    
    for (int i = 0; i < g_queryInfo.superQueryInfo.concurrent; i++) {  
      threadInfo *t_info = infos + i;
      t_info->threadID = i;
      t_info->taos = taos;
      pthread_create(pids + i, NULL, superSubscribeProcess, t_info);
    }
  }
  
  //==== create sub threads for query from sub table  
  pthread_t  *pidsOfSub  = NULL;
  threadInfo *infosOfSub = NULL;
  if ((g_queryInfo.subQueryInfo.sqlCount > 0) && (g_queryInfo.subQueryInfo.threadCnt > 0)) {
    pidsOfSub  = malloc(g_queryInfo.subQueryInfo.threadCnt * sizeof(pthread_t));
    infosOfSub = malloc(g_queryInfo.subQueryInfo.threadCnt * sizeof(threadInfo));
    if ((NULL == pidsOfSub) || (NULL == infosOfSub)) {
      printf("malloc failed for create threads\n");      
      taos_close(taos);
      exit(-1);
    }
    
    int ntables = g_queryInfo.subQueryInfo.childTblCount;
    int threads = g_queryInfo.subQueryInfo.threadCnt;
  
    int a = ntables / threads;
    if (a < 1) {
      threads = ntables;
      a = 1;
    }
  
    int b = 0;
    if (threads != 0) {
      b = ntables % threads;
    }
    
    int last = 0;
    for (int i = 0; i < threads; i++) {  
      threadInfo *t_info = infosOfSub + i;
      t_info->threadID = i;
      
      t_info->start_table_id = last;
      t_info->end_table_id = i < b ? last + a : last + a - 1;
      t_info->taos = taos;
      pthread_create(pidsOfSub + i, NULL, subSubscribeProcess, t_info);
    }
    g_queryInfo.subQueryInfo.threadCnt = threads;
  }
  
  for (int i = 0; i < g_queryInfo.superQueryInfo.concurrent; i++) {
    pthread_join(pids[i], NULL);
  }  

  tmfree((char*)pids);
  tmfree((char*)infos);  

  for (int i = 0; i < g_queryInfo.subQueryInfo.threadCnt; i++) {
    pthread_join(pidsOfSub[i], NULL);
  }

  tmfree((char*)pidsOfSub);
  tmfree((char*)infosOfSub);    
  taos_close(taos);
  return 0;
}

void initOfInsertMeta() {
  memset(&g_Dbs, 0, sizeof(SDbs));
   
   // set default values
   strncpy(g_Dbs.host, "127.0.0.1", MAX_DB_NAME_SIZE);
   g_Dbs.port = 6030;
   strncpy(g_Dbs.user, TSDB_DEFAULT_USER, MAX_DB_NAME_SIZE);
   strncpy(g_Dbs.password, TSDB_DEFAULT_PASS, MAX_DB_NAME_SIZE);
   g_Dbs.threadCount = 2;
   g_Dbs.use_metric = true;
}

void initOfQueryMeta() {
  memset(&g_queryInfo, 0, sizeof(SQueryMetaInfo));
   
   // set default values
   strncpy(g_queryInfo.host, "127.0.0.1", MAX_DB_NAME_SIZE);
   g_queryInfo.port = 6030;
   strncpy(g_queryInfo.user, TSDB_DEFAULT_USER, MAX_DB_NAME_SIZE);
   strncpy(g_queryInfo.password, TSDB_DEFAULT_PASS, MAX_DB_NAME_SIZE);
}

void setParaFromArg(){
  if (g_args.host) {
    strcpy(g_Dbs.host, g_args.host);
  } else {
    strncpy(g_Dbs.host, "127.0.0.1", MAX_DB_NAME_SIZE);
  }

  if (g_args.user) {
    strcpy(g_Dbs.user, g_args.user);
  } 

  if (g_args.password) {
    strcpy(g_Dbs.password, g_args.password);
  } 
  
  if (g_args.port) {
    g_Dbs.port = g_args.port;
  } 

  g_Dbs.dbCount = 1;
  g_Dbs.db[0].drop = 1;
  
  strncpy(g_Dbs.db[0].dbName, g_args.database, MAX_DB_NAME_SIZE);
  g_Dbs.db[0].dbCfg.replica = g_args.replica;
  strncpy(g_Dbs.db[0].dbCfg.precision, "ms", MAX_DB_NAME_SIZE);

  
  strncpy(g_Dbs.resultFile, g_args.output_file, MAX_FILE_NAME_LEN);

  g_Dbs.use_metric = g_args.use_metric;
  g_Dbs.insert_only = g_args.insert_only;

  g_Dbs.db[0].superTblCount = 1;
  strncpy(g_Dbs.db[0].superTbls[0].sTblName, "meters", MAX_TB_NAME_SIZE);
  g_Dbs.db[0].superTbls[0].childTblCount = g_args.num_of_tables;
  g_Dbs.threadCount = g_args.num_of_threads;
  g_Dbs.threadCountByCreateTbl = 1;
  g_Dbs.queryMode = g_args.mode;

  g_Dbs.db[0].superTbls[0].autoCreateTable = PRE_CREATE_SUBTBL;
  g_Dbs.db[0].superTbls[0].superTblExists = TBL_NO_EXISTS;
  g_Dbs.db[0].superTbls[0].childTblExists = TBL_NO_EXISTS;
  g_Dbs.db[0].superTbls[0].insertRate    = 0;
  g_Dbs.db[0].superTbls[0].disorderRange = g_args.disorderRange;
  g_Dbs.db[0].superTbls[0].disorderRatio = g_args.disorderRatio;
  strncpy(g_Dbs.db[0].superTbls[0].childTblPrefix, g_args.tb_prefix, MAX_TB_NAME_SIZE);
  strncpy(g_Dbs.db[0].superTbls[0].dataSource, "rand", MAX_TB_NAME_SIZE);
  strncpy(g_Dbs.db[0].superTbls[0].insertMode, "taosc", MAX_TB_NAME_SIZE);
  strncpy(g_Dbs.db[0].superTbls[0].startTimestamp, "2017-07-14 10:40:00.000", MAX_TB_NAME_SIZE);
  g_Dbs.db[0].superTbls[0].timeStampStep = 10;

  // g_args.num_of_RPR;
  g_Dbs.db[0].superTbls[0].insertRows = g_args.num_of_DPT;
  g_Dbs.db[0].superTbls[0].maxSqlLen = TSDB_PAYLOAD_SIZE;

  g_Dbs.do_aggreFunc = true;

  char dataString[STRING_LEN];
  char **data_type = g_args.datatype;
  
  memset(dataString, 0, STRING_LEN);

  if (strcasecmp(data_type[0], "BINARY") == 0 || strcasecmp(data_type[0], "BOOL") == 0 || strcasecmp(data_type[0], "NCHAR") == 0 ) {
    g_Dbs.do_aggreFunc = false;
  }

  g_Dbs.db[0].superTbls[0].columnCount = 0;
  for (int i = 0; i < MAX_NUM_DATATYPE; i++) {
    if (data_type[i] == NULL) {
      break;
    }

    strncpy(g_Dbs.db[0].superTbls[0].columns[i].dataType, data_type[i], MAX_TB_NAME_SIZE);
    g_Dbs.db[0].superTbls[0].columns[i].dataLen = g_args.len_of_binary;    
    g_Dbs.db[0].superTbls[0].columnCount++;
  }

  if (g_Dbs.db[0].superTbls[0].columnCount > g_args.num_of_CPR) {
    g_Dbs.db[0].superTbls[0].columnCount = g_args.num_of_CPR;
  } else {
    for (int i = g_Dbs.db[0].superTbls[0].columnCount; i < g_args.num_of_CPR; i++) {
      strncpy(g_Dbs.db[0].superTbls[0].columns[i].dataType, "INT", MAX_TB_NAME_SIZE);
      g_Dbs.db[0].superTbls[0].columns[i].dataLen = 0;    
      g_Dbs.db[0].superTbls[0].columnCount++;
    }
  }

  if (g_Dbs.use_metric) {
    strncpy(g_Dbs.db[0].superTbls[0].tags[0].dataType, "INT", MAX_TB_NAME_SIZE);
    g_Dbs.db[0].superTbls[0].tags[0].dataLen = 0;    
  
    strncpy(g_Dbs.db[0].superTbls[0].tags[1].dataType, "BINARY", MAX_TB_NAME_SIZE);
    g_Dbs.db[0].superTbls[0].tags[1].dataLen = g_args.len_of_binary;    
    g_Dbs.db[0].superTbls[0].tagCount = 2;  
  } else {
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

void querySqlFile(TAOS* taos, char* sqlFile)
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

  double t = getCurrentTime();
  
  while ((read_len = tgetline(&line, &line_len, fp)) != -1) {
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
    queryDbExec(taos, cmd, NO_INSERT_TYPE);
    memset(cmd, 0, MAX_SQL_SIZE);
    cmd_len = 0;
  }

  t = getCurrentTime() - t;
  printf("run %s took %.6f second(s)\n\n", sqlFile, t);

  tmfree(cmd);
  tmfree(line);
  tmfclose(fp);
  return;
}

int main(int argc, char *argv[]) {
  parse_args(argc, argv, &g_args);

  if (g_args.metaFile) {
    initOfInsertMeta();
    initOfQueryMeta();    
    if (false == getInfoFromJsonFile(g_args.metaFile)) {
      printf("Failed to read %s\n", g_args.metaFile);
      return 1;
    }
  } else {  
    
    memset(&g_Dbs, 0, sizeof(SDbs));
    g_jsonType = INSERT_MODE;
    setParaFromArg();

    if (NULL != g_args.sqlFile) {
      TAOS* qtaos = taos_connect(g_Dbs.host, g_Dbs.user, g_Dbs.password, g_Dbs.db[0].dbName, g_Dbs.port);
      querySqlFile(qtaos, g_args.sqlFile);  
      taos_close(qtaos);
      return 0;
    }
    
    (void)insertTestProcess();
    if (g_Dbs.insert_only) return 0;

    // select
    
    //printf("At present, there is no integration of taosdemo, please wait patiently!\n");
    return 0;
  }
 
  if (INSERT_MODE == g_jsonType) {
    if (g_Dbs.cfgDir[0]) taos_options(TSDB_OPTION_CONFIGDIR, g_Dbs.cfgDir);
    (void)insertTestProcess();
  } else if (QUERY_MODE == g_jsonType) {
    if (g_queryInfo.cfgDir[0])  taos_options(TSDB_OPTION_CONFIGDIR, g_queryInfo.cfgDir);
    (void)queryTestProcess();
  } else if (SUBSCRIBE_MODE == g_jsonType) {
    if (g_queryInfo.cfgDir[0])  taos_options(TSDB_OPTION_CONFIGDIR, g_queryInfo.cfgDir);
    (void)subscribeTestProcess();
  }  else {
    ;
  }

  taos_cleanup();
  return 0;
}

