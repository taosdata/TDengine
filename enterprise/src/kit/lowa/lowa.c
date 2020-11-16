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

#define _GNU_SOURCE
#define CURL_STATICLIB
#include "curl/curl.h"

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


#define   MAX_DB_COUNT           8
#define   MAX_SUPER_TABLE_COUNT  8
#define   MAX_COLUMN_COUNT       1024
#define   MAX_TAG_COUNT          128

#define   MAX_QUERY_SQL_COUNT    10
#define   MAX_QUERY_SQL_LENGTH   256


#define   MAX_LINE_COUNT_IN_MEM  10000

typedef enum CREATE_SUB_TALBE_MOD_EN {
  PRE_CREATE_SUBTBL,
  AUTO_CREATE_SUBTBL,
  NO_CREATE_SUBTBL
} CREATE_SUB_TALBE_MOD_EN;


/* Used by main to communicate with parse_opt. */
typedef struct SArguments_S {
  char *   metaFile;
  int      abort;
} SArguments;

typedef struct SColumn_S {
  char  dataType[MAX_TB_NAME_SIZE];
  int   dataLen;
} SColumn;

typedef struct SSuperTable_S {
  char         sTblName[MAX_TB_NAME_SIZE];
  int          childTblCount;
  int8_t       autoCreateTable;                  // 0: create sub table, 1: auto create sub table, 2: not create sub table, already exists
  char         childTblPrefix[MAX_TB_NAME_SIZE];
  char         dataSource[MAX_TB_NAME_SIZE];  // rand_gen or sample
  char         insertMode[MAX_TB_NAME_SIZE];  // taosc, restful
  int          insertRate;  // 0: unlimit  > 0   rows/s
  int64_t      insertRows;
  int          timeStampStep;
  char         startTimestamp[MAX_TB_NAME_SIZE];  // 
  char         sampleFormat[MAX_TB_NAME_SIZE];  // csv, json
  char         sampleFile[MAX_FILE_NAME_LEN];
  char         tagsFile[MAX_FILE_NAME_LEN];

  int          columnCount;
  SColumn      columns[MAX_COLUMN_COUNT];
  int          tagCount;
  SColumn      tags[MAX_TAG_COUNT];

  char*        colsOfCreatChildTable;
  int          lenOfOneRow;
  int          lenOfTagOfOneRow;

  char*        sampleDataBuf;
  int          sampleRowCount;
  int          sampleUsePos;

  int          tagSource;    // 0: rand, 1: tag sample
  char*        tagDataBuf;
  int          tagSampleCount;
  int          tagUsePos;
} SSuperTable;

typedef struct SDbCfg_S { 
  int       maxtablesPerVnode;
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
  char      precision[MAX_TB_NAME_SIZE];  
} SDbCfg;

typedef struct SDataBase_S {
  char         dbName[MAX_DB_NAME_SIZE];
  SDbCfg       dbCfg;
  int          superTblCount;
  SSuperTable  supterTbls[MAX_SUPER_TABLE_COUNT];
} SDataBase;

typedef struct SDbs_S {
  char         cfgDir[MAX_FILE_NAME_LEN];
  char         host[MAX_DB_NAME_SIZE];
  uint16_t     port;
  char         user[MAX_DB_NAME_SIZE];
  char         password[MAX_DB_NAME_SIZE];
  int          threadCount;
  int          dbCount;
  SDataBase    db[MAX_DB_COUNT];
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
  TAOS_SUB*    tsub[MAX_QUERY_SQL_COUNT];
} SuperQueryInfo;

typedef struct SubQueryInfo_S {
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
  TAOS_SUB*    tsub[MAX_QUERY_SQL_COUNT];
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
  CURL *curl_handle;
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
} threadInfo;

typedef  struct curlMemInfo_S {
    char *buf;
    size_t sizeleft;
  } curlMemInfo;


#ifdef LINUX
  /* The options we understand. */
  static struct argp_option options[] = {
    {0, 'f', "meta file",            0, "The meta data to the execution procedure. Default is './insert.json'.",    0},
    {0, 'c', "config_directory",     0, "Configuration directory. Default is '/etc/taos/'.",                        1},
    {0}};

  /* Parse a single option. */
  static error_t parse_opt(int key, char *arg, struct argp_state *state) {
    /* Get the input argument from argp_parse, which we
      know is a pointer to our arguments structure. */
    SArguments *arguments = state->input;
    wordexp_t full_path;
    switch (key) {
      case 'f':
        arguments->metaFile = arg;
        break;
      case 'c':
        if (wordexp(arg, &full_path, 0) != 0) {
          fprintf(stderr, "Invalid path %s\n", arg);
          return -1;
        }
        taos_options(TSDB_OPTION_CONFIGDIR, full_path.we_wordv[0]);
        wordfree(&full_path);
        break;
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
static void queryDB(TAOS *taos, char *command);


/* ************ Global variables ************  */

int32_t  randint[MAX_PREPARED_RAND];
int64_t  randbigint[MAX_PREPARED_RAND];
float    randfloat[MAX_PREPARED_RAND];
double   randdouble[MAX_PREPARED_RAND];

SArguments g_args = {"./meta.json", 0};

#define    INSERT_MODE        0
#define    QUERY_MODE         1
#define    SUBSCRIBE_MODE     2

int        g_jsonType = 0;
SDbs       g_Dbs;
int        g_totalChildTables = 0;
int64_t    g_totalRecords = 0;

SQueryMetaInfo g_queryInfo;


static void queryDB(TAOS *taos, char *command) {
  int i;
  TAOS_RES *pSql = NULL;
  int32_t   code = -1;

  for (i = 0; i < 5; i++) {
    if (NULL != pSql) {
      taos_free_result(pSql);
      pSql = NULL;
    }
    
    pSql = taos_query(taos, command);
    code = taos_errno(pSql);
    if (0 == code) {
      break;
    }    
  }

  if (code != 0) {
    fprintf(stderr, "Failed to run %s, reason: %s\n", command, taos_errstr(pSql));
    taos_free_result(pSql);
    taos_close(taos);
    exit(EXIT_FAILURE);
  }

  taos_free_result(pSql);
}

static void selectAndGetResult(TAOS *taos, char *command) {
  TAOS_RES *res;

  res = taos_query(taos, command);
  if (res == NULL || taos_errno(res) != 0) {
    printf("failed to sql:%s, reason:%s\n", command, taos_errstr(res));
    taos_free_result(res);
    exit(1);
  }
  
  TAOS_ROW    row;
  int         num_rows = 0;
  int         num_fields = taos_field_count(res);
  TAOS_FIELD *fields     = taos_fetch_fields(res);
  char        temp[4096];

  // fetch the records row by row
  while ((row = taos_fetch_row(res))) {
    num_rows++;
    taos_print_row(temp, row, fields, num_fields);
    printf("query result:%s\n", temp);
  }

  taos_free_result(res);
}

double getCurrentTime() {
  struct timeval tv;
  if (gettimeofday(&tv, NULL) != 0) {
    perror("Failed to get current time in ms");
    exit(EXIT_FAILURE);
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
    randint[i] = (int)(rand() % 10);
    randbigint[i] = (int64_t)(rand() % 2147483648);
    randfloat[i] = (float)(rand() / 1000.0);
    randdouble[i] = (double)(rand() / 1000000.0);
  }
}

static void printfInsertMeta() {
  printf("\033[1m\033[40;32m================ insert.json parse result ================\033[0m\n");
  printf("host:                    \033[33m%s:%u\033[0m\n", g_Dbs.host, g_Dbs.port);
  //printf("port:                    \033[33m%u\033[0m\n", g_Dbs.port);
  printf("user:                    \033[33m%s\033[0m\n", g_Dbs.user);
  printf("password:                \033[33m%s\033[0m\n", g_Dbs.password);
  printf("thread count:            \033[33m%d\033[0m\n", g_Dbs.threadCount);

  printf("database count:          \033[33m%d\033[0m\n", g_Dbs.dbCount);
  for (int i = 0; i < g_Dbs.dbCount; i++) {
    printf("database[\033[33m%d\033[0m]:\n", i);
    printf("  database name:         \033[33m%s\033[0m\n", g_Dbs.db[i].dbName);
  
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
    if (g_Dbs.db[i].dbCfg.maxtablesPerVnode > 0) {
      printf("  maxtablesPerVnode:     \033[33m%d\033[0m\n", g_Dbs.db[i].dbCfg.maxtablesPerVnode);
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
    if ((0 == strncasecmp(g_Dbs.db[i].dbCfg.precision, "ms", 2)) || (0 == strncasecmp(g_Dbs.db[i].dbCfg.precision, "us", 2))) {
      printf("  precision:             \033[33m%s\033[0m\n", g_Dbs.db[i].dbCfg.precision);
    }

    printf("  super table count:     \033[33m%d\033[0m\n", g_Dbs.db[i].superTblCount);
    for (int j = 0; j < g_Dbs.db[i].superTblCount; j++) {
      printf("  super table[\033[33m%d\033[0m]:\n", j);
    
      printf("      stbName:           \033[33m%s\033[0m\n",  g_Dbs.db[i].supterTbls[j].sTblName);   

      if (PRE_CREATE_SUBTBL == g_Dbs.db[i].supterTbls[j].autoCreateTable) {
        printf("      autoCreateTable:   \033[33m%s\033[0m\n",  "no");
      } else if (AUTO_CREATE_SUBTBL == g_Dbs.db[i].supterTbls[j].autoCreateTable) {
        printf("      autoCreateTable:   \033[33m%s\033[0m\n",  "yes");
      } else if (NO_CREATE_SUBTBL == g_Dbs.db[i].supterTbls[j].autoCreateTable) {
        printf("      autoCreateTable:   \033[33m%s\033[0m\n",  "null");
      }
      
      printf("      childTblCount:     \033[33m%d\033[0m\n",  g_Dbs.db[i].supterTbls[j].childTblCount);      
      printf("      childTblPrefix:    \033[33m%s\033[0m\n",  g_Dbs.db[i].supterTbls[j].childTblPrefix);      
      printf("      dataSource:        \033[33m%s\033[0m\n",  g_Dbs.db[i].supterTbls[j].dataSource);      
      printf("      insertMode:        \033[33m%s\033[0m\n",  g_Dbs.db[i].supterTbls[j].insertMode);      
      printf("      insertRate:        \033[33m%d\033[0m\n",  g_Dbs.db[i].supterTbls[j].insertRate);     
      printf("      insertRows:        \033[33m%ld\033[0m\n", g_Dbs.db[i].supterTbls[j].insertRows);     
      printf("      timeStampStep:     \033[33m%d\033[0m\n",  g_Dbs.db[i].supterTbls[j].timeStampStep);      
      printf("      startTimestamp:    \033[33m%s\033[0m\n",  g_Dbs.db[i].supterTbls[j].startTimestamp);             
      printf("      sampleFormat:      \033[33m%s\033[0m\n",  g_Dbs.db[i].supterTbls[j].sampleFormat);
      printf("      sampleFile:        \033[33m%s\033[0m\n",  g_Dbs.db[i].supterTbls[j].sampleFile); 
      printf("      tagsFile:          \033[33m%s\033[0m\n",  g_Dbs.db[i].supterTbls[j].tagsFile);   
    
      printf("      columnCount:       \033[33m%d\033[0m\n        ",  g_Dbs.db[i].supterTbls[j].columnCount);
      for (int k = 0; k < g_Dbs.db[i].supterTbls[j].columnCount; k++) {
        //printf("dataType:%s, dataLen:%d\t", g_Dbs.db[i].supterTbls[j].columns[k].dataType, g_Dbs.db[i].supterTbls[j].columns[k].dataLen);
        if ((0 == strncasecmp(g_Dbs.db[i].supterTbls[j].columns[k].dataType, "binary", 6)) || (0 == strncasecmp(g_Dbs.db[i].supterTbls[j].columns[k].dataType, "nchar", 5))) {
          printf("column[\033[33m%d\033[0m]:\033[33m%s(%d)\033[0m ", k, g_Dbs.db[i].supterTbls[j].columns[k].dataType, g_Dbs.db[i].supterTbls[j].columns[k].dataLen);
        } else {
          printf("column[%d]:\033[33m%s\033[0m ", k, g_Dbs.db[i].supterTbls[j].columns[k].dataType);
        }
      }
      printf("\n");
      
      printf("      tagCount:            \033[33m%d\033[0m\n        ",  g_Dbs.db[i].supterTbls[j].tagCount);
      for (int k = 0; k < g_Dbs.db[i].supterTbls[j].tagCount; k++) {
        //printf("dataType:%s, dataLen:%d\t", g_Dbs.db[i].supterTbls[j].tags[k].dataType, g_Dbs.db[i].supterTbls[j].tags[k].dataLen);
        if ((0 == strncasecmp(g_Dbs.db[i].supterTbls[j].tags[k].dataType, "binary", 6)) || (0 == strncasecmp(g_Dbs.db[i].supterTbls[j].tags[k].dataType, "nchar", 5))) {
          printf("tag[%d]:\033[33m%s(%d)\033[0m ", k, g_Dbs.db[i].supterTbls[j].tags[k].dataType, g_Dbs.db[i].supterTbls[j].tags[k].dataLen);
        } else {
          printf("tag[%d]:\033[33m%s\033[0m ", k, g_Dbs.db[i].supterTbls[j].tags[k].dataType);
        }     
      }
      printf("\n");
    }
    printf("\n");
  }
  printf("\033[1m\033[40;32m================ insert.json parse result ================\033[0m\n");
}

static void printfQueryMeta() {
  printf("\033[1m\033[40;32m================ query.json parse result ================\033[0m\n");
  printf("host:                    \033[33m%s:%u\033[0m\n", g_queryInfo.host, g_queryInfo.port);
  printf("user:                    \033[33m%s\033[0m\n", g_queryInfo.user);
  printf("password:                \033[33m%s\033[0m\n", g_queryInfo.password);
  printf("database name:           \033[33m%s\033[0m\n", g_queryInfo.dbName);

  printf("\n");
  printf("super table query info:                   \n");  
  printf("rate:           \033[33m%d\033[0m\n", g_queryInfo.superQueryInfo.rate);
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
  printf("sub table query info:                   \n");  
  printf("rate:           \033[33m%d\033[0m\n", g_queryInfo.subQueryInfo.rate);
  printf("threadCnt:      \033[33m%d\033[0m\n", g_queryInfo.subQueryInfo.threadCnt);
  printf("childTblCount:  \033[33m%d\033[0m\n", g_queryInfo.subQueryInfo.childTblCount);
  printf("childTblPrefix: \033[33m%s\033[0m\n", g_queryInfo.subQueryInfo.childTblPrefix);

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

char* getTagValueFromTagSample(        SSuperTable* stbInfo, int tagUsePos) {
  char*  dataBuf = (char*)calloc(TSDB_MAX_SQL_LEN+1, 1);
  if (NULL == dataBuf) {
    printf("calloc failed! size:%d\n", TSDB_MAX_SQL_LEN+1);
    exit(-1);
  }
  
  int    dataLen = 0;
  dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen, "(%s)", stbInfo->tagDataBuf + stbInfo->lenOfTagOfOneRow * tagUsePos);
  
  return dataBuf;
}

char* generateTagVaulesForStb(SSuperTable* stbInfo) {
  char*  dataBuf = (char*)calloc(TSDB_MAX_SQL_LEN+1, 1);
  if (NULL == dataBuf) {
    printf("calloc failed! size:%d\n", TSDB_MAX_SQL_LEN+1);
    exit(-1);
  }

  int    dataLen = 0;
  dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen, "(");
  for (int i = 0; i < stbInfo->tagCount; i++) {
    if ((0 == strncasecmp(stbInfo->tags[i].dataType, "binary", 6)) || (0 == strncasecmp(stbInfo->tags[i].dataType, "nchar", 5))) {
      if (stbInfo->tags[i].dataLen > TSDB_MAX_BINARY_LEN) {
        printf("binary or nchar length overflow, max size:%"PRId64 "\n", TSDB_MAX_BINARY_LEN);
        exit(-1);
      }
    
      char* buf = (char*)calloc(stbInfo->tags[i].dataLen+1, 1);
      if (NULL == buf) {
        printf("calloc failed! size:%d\n", stbInfo->tags[i].dataLen);
        exit(-1);
      }
      rand_string(buf, stbInfo->tags[i].dataLen);
      dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen, "\'%s\', ", buf);
      free(buf);
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
      exit(-1);
    }
  }
  dataLen -= 2;
  dataLen += snprintf(dataBuf + dataLen, TSDB_MAX_SQL_LEN - dataLen, ")");  
  return dataBuf;
}

static int createDatabases() {
  TAOS * taos = NULL;
  
  taos_init();
  taos = taos_connect(g_Dbs.host, g_Dbs.user, g_Dbs.password, NULL, g_Dbs.port);
  if (taos == NULL) {
    fprintf(stderr, "Failed to connect to TDengine, reason:%s\n", taos_errstr(NULL));
    exit(-1);
  }
  char command[BUFFER_SIZE] = "\0";

  for (int i = 0; i < g_Dbs.dbCount; i++) {   
    //sprintf(command, "drop database if exists %s;", g_Dbs.db[i].dbName);
    //(void)queryDB(taos, command);
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
    
    (void)queryDB(taos, command);
    
    for (int j = 0; j < g_Dbs.db[i].superTblCount; j++) {
      char cols[STRING_LEN] = "\0";
      int colIndex;
      int len = 0;

      int  lenOfOneRow = 0;
      for (colIndex = 0; colIndex < g_Dbs.db[i].supterTbls[j].columnCount; colIndex++) {
        char* dataType = g_Dbs.db[i].supterTbls[j].columns[colIndex].dataType;
        
        if (strcasecmp(dataType, "BINARY") == 0) {
          len += snprintf(cols + len, STRING_LEN - len, ", c%d %s(%d)", colIndex, "BINARY", g_Dbs.db[i].supterTbls[j].columns[colIndex].dataLen);
          lenOfOneRow += g_Dbs.db[i].supterTbls[j].columns[colIndex].dataLen + 3;
        } else if (strcasecmp(dataType, "NCHAR") == 0) {
          len += snprintf(cols + len, STRING_LEN - len, ", c%d %s(%d)", colIndex, "NCHAR", g_Dbs.db[i].supterTbls[j].columns[colIndex].dataLen);
          lenOfOneRow += g_Dbs.db[i].supterTbls[j].columns[colIndex].dataLen + 3;
        } else if (strcasecmp(dataType, "INT") == 0)  {
          len += snprintf(cols + len, STRING_LEN - len, ", c%d %s", colIndex, "INT");
          lenOfOneRow += 11;
        } else if (strcasecmp(dataType, "BIGINT") == 0)  {
          len += snprintf(cols + len, STRING_LEN - len, ", c%d %s", colIndex, "BIGINT");
          lenOfOneRow += 21;
        } else if (strcasecmp(dataType, "SMALLINT") == 0)  {
          len += snprintf(cols + len, STRING_LEN - len, ", c%d %s", colIndex, "SMALLINT");
          lenOfOneRow += 6;
        } else if (strcasecmp(dataType, "TINYINT") == 0)  {
          len += snprintf(cols + len, STRING_LEN - len, ", c%d %s", colIndex, "TINYINT");
          lenOfOneRow += 4;
        } else if (strcasecmp(dataType, "BOOL") == 0)  {
          len += snprintf(cols + len, STRING_LEN - len, ", c%d %s", colIndex, "BOOL");
          lenOfOneRow += 6;
        } else if (strcasecmp(dataType, "FLOAT") == 0) {
          len += snprintf(cols + len, STRING_LEN - len, ", c%d %s", colIndex, "FLOAT");
          lenOfOneRow += 22;
        } else if (strcasecmp(dataType, "DOUBLE") == 0) { 
          len += snprintf(cols + len, STRING_LEN - len, ", c%d %s", colIndex, "DOUBLE");
          lenOfOneRow += 42;
        }  else if (strcasecmp(dataType, "TIMESTAMP") == 0) { 
          len += snprintf(cols + len, STRING_LEN - len, ", c%d %s", colIndex, "TIMESTAMP");
          lenOfOneRow += 21;
        } else {
          taos_close(taos);
          exit(-1);
        }
      }

      g_Dbs.db[i].supterTbls[j].lenOfOneRow = lenOfOneRow + 20; // timestamp
      //printf("%s.%s column count:%d, column length:%d\n\n", g_Dbs.db[i].dbName, g_Dbs.db[i].supterTbls[j].sTblName, g_Dbs.db[i].supterTbls[j].columnCount, lenOfOneRow);

      // save for creating child table
      g_Dbs.db[i].supterTbls[j].colsOfCreatChildTable = (char*)calloc(len+1, 1);
      if (NULL == g_Dbs.db[i].supterTbls[j].colsOfCreatChildTable) {
        printf("Failed when calloc, size:%d", len+1);
        taos_close(taos);
        exit(-1);
      }
      snprintf(g_Dbs.db[i].supterTbls[j].colsOfCreatChildTable, len, "(ts timestamp%s)", cols);

      char tags[STRING_LEN] = "\0";
      int tagIndex;
      len = 0;

      int lenOfTagOfOneRow = 0;
      len += snprintf(tags + len, STRING_LEN - len, "(");
      for (tagIndex = 0; tagIndex < g_Dbs.db[i].supterTbls[j].tagCount; tagIndex++) {
        char* dataType = g_Dbs.db[i].supterTbls[j].tags[tagIndex].dataType;
        
        if (strcasecmp(dataType, "BINARY") == 0) {
          len += snprintf(tags + len, STRING_LEN - len, "t%d %s(%d), ", tagIndex, "BINARY", g_Dbs.db[i].supterTbls[j].tags[tagIndex].dataLen);
          lenOfTagOfOneRow += g_Dbs.db[i].supterTbls[j].tags[tagIndex].dataLen + 3;
        } else if (strcasecmp(dataType, "NCHAR") == 0) {
          len += snprintf(tags + len, STRING_LEN - len, "t%d %s(%d), ", tagIndex, "NCHAR", g_Dbs.db[i].supterTbls[j].tags[tagIndex].dataLen);
          lenOfTagOfOneRow += g_Dbs.db[i].supterTbls[j].tags[tagIndex].dataLen + 3;
        } else if (strcasecmp(dataType, "INT") == 0)  {
          len += snprintf(tags + len, STRING_LEN - len, "t%d %s, ", tagIndex, "INT");
          lenOfTagOfOneRow += g_Dbs.db[i].supterTbls[j].tags[tagIndex].dataLen + 11;
        } else if (strcasecmp(dataType, "BIGINT") == 0)  {
          len += snprintf(tags + len, STRING_LEN - len, "t%d %s, ", tagIndex, "BIGINT");
          lenOfTagOfOneRow += g_Dbs.db[i].supterTbls[j].tags[tagIndex].dataLen + 21;
        } else if (strcasecmp(dataType, "SMALLINT") == 0)  {
          len += snprintf(tags + len, STRING_LEN - len, "t%d %s, ", tagIndex, "SMALLINT");
          lenOfTagOfOneRow += g_Dbs.db[i].supterTbls[j].tags[tagIndex].dataLen + 6;
        } else if (strcasecmp(dataType, "TINYINT") == 0)  {
          len += snprintf(tags + len, STRING_LEN - len, "t%d %s, ", tagIndex, "TINYINT");
          lenOfTagOfOneRow += g_Dbs.db[i].supterTbls[j].tags[tagIndex].dataLen + 4;
        } else if (strcasecmp(dataType, "BOOL") == 0)  {
          len += snprintf(tags + len, STRING_LEN - len, "t%d %s, ", tagIndex, "BOOL");
          lenOfTagOfOneRow += g_Dbs.db[i].supterTbls[j].tags[tagIndex].dataLen + 6;
        } else if (strcasecmp(dataType, "FLOAT") == 0) {
          len += snprintf(tags + len, STRING_LEN - len, "t%d %s, ", tagIndex, "FLOAT");
          lenOfTagOfOneRow += g_Dbs.db[i].supterTbls[j].tags[tagIndex].dataLen + 22;
        } else if (strcasecmp(dataType, "DOUBLE") == 0) { 
          len += snprintf(tags + len, STRING_LEN - len, "t%d %s, ", tagIndex, "DOUBLE");
          lenOfTagOfOneRow += g_Dbs.db[i].supterTbls[j].tags[tagIndex].dataLen + 42;
        } else {
          taos_close(taos);
          exit(-1);
        }
      }
      len -= 2;
      len += snprintf(tags + len, STRING_LEN - len, ")");

      g_Dbs.db[i].supterTbls[j].lenOfTagOfOneRow = lenOfTagOfOneRow;
      
      snprintf(command, BUFFER_SIZE, "create table if not exists %s.%s (ts timestamp%s) tags %s", g_Dbs.db[i].dbName, g_Dbs.db[i].supterTbls[j].sTblName, cols, tags);
      (void)queryDB(taos, command);
    }    
  }

  taos_close(taos);
  return 0;
}


void * createTable(void *sarg) 
{
  char command[BUFFER_SIZE] = "\0";
  
  threadInfo *winfo = (threadInfo *)sarg; 
  SSuperTable* superTblInfo = winfo->superTblInfo;

  //printf("Creating table from %d to %d\n", winfo->start_table_id, winfo->end_table_id);
  for (int i = winfo->start_table_id; i <= winfo->end_table_id; i++) {
    char* tagsValBuf = NULL;
    if (0 == superTblInfo->tagSource) {
      tagsValBuf = generateTagVaulesForStb(superTblInfo);
    } else {
      tagsValBuf = getTagValueFromTagSample(superTblInfo, i % superTblInfo->tagSampleCount);
    }
    snprintf(command, BUFFER_SIZE, "create table if not exists %s.%s%d using %s.%s tags %s;", winfo->db_name, superTblInfo->childTblPrefix, i, winfo->db_name, superTblInfo->sTblName, tagsValBuf);
    free(tagsValBuf);
    queryDB(winfo->taos, command);
  }

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
      if ((AUTO_CREATE_SUBTBL == g_Dbs.db[i].supterTbls[j].autoCreateTable) || (NO_CREATE_SUBTBL == g_Dbs.db[i].supterTbls[j].autoCreateTable)) {
        continue;
      }
      startMultiThreadCreateChildTable(g_Dbs.db[i].supterTbls[j].colsOfCreatChildTable, g_Dbs.threadCount, g_Dbs.db[i].supterTbls[j].childTblCount, g_Dbs.db[i].dbName, &(g_Dbs.db[i].supterTbls[j]));
      g_totalChildTables += g_Dbs.db[i].supterTbls[j].childTblCount;
    }    
  }
}

/*
  Read 10000 lines at most. If more than 10000 lines, continue to read after using
*/
int readTagFromCsvFileToMem(SSuperTable  * supterTblInfo) {
  size_t  n = 0;
  ssize_t readLen = 0;
  char *  line = NULL;
  
  FILE *fp = fopen(supterTblInfo->tagsFile, "r");
  if (fp == NULL) {
    printf("Failed to open tags file: %s, reason:%s\n", supterTblInfo->tagsFile, strerror(errno));
    return -1;
  }

  if (supterTblInfo->tagDataBuf) {
    free(supterTblInfo->tagDataBuf);
    supterTblInfo->tagDataBuf = NULL;
  }

  supterTblInfo->tagDataBuf = calloc(supterTblInfo->lenOfTagOfOneRow * MAX_LINE_COUNT_IN_MEM, 1);
  if (supterTblInfo->tagDataBuf == NULL) {
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

    memcpy(supterTblInfo->tagDataBuf + supterTblInfo->tagSampleCount * supterTblInfo->lenOfTagOfOneRow, line, readLen);
    supterTblInfo->tagSampleCount++;

    if (supterTblInfo->tagSampleCount >= MAX_LINE_COUNT_IN_MEM) {
      break;
    }
  }

  free(line);
  fclose(fp);
  return 0;
}

int readSampleFromJsonFileToMem(SSuperTable  * supterTblInfo) {
  // TODO
  return 0;
}

/*
  Read 10000 lines at most. If more than 10000 lines, continue to read after using
*/
int readSampleFromCsvFileToMem(SSuperTable  * supterTblInfo) {
  size_t  n = 0;
  ssize_t readLen = 0;
  char *  line = NULL;
  
  FILE *fp = fopen(supterTblInfo->sampleFile, "r");
  if (fp == NULL) {
    printf("Failed to open sample file: %s, reason:%s\n", supterTblInfo->sampleFile, strerror(errno));
    return -1;
  }

  if (supterTblInfo->sampleDataBuf) {
    free(supterTblInfo->sampleDataBuf);
    supterTblInfo->sampleDataBuf = NULL;
  }

  supterTblInfo->sampleDataBuf = calloc(supterTblInfo->lenOfOneRow * MAX_LINE_COUNT_IN_MEM, 1);
  if (supterTblInfo->sampleDataBuf == NULL) {
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

    memcpy(supterTblInfo->sampleDataBuf + supterTblInfo->sampleRowCount * supterTblInfo->lenOfOneRow, line, readLen);
    supterTblInfo->sampleRowCount++;

    if (supterTblInfo->sampleRowCount >= MAX_LINE_COUNT_IN_MEM) {
      break;
    }
  }

  free(line);
  fclose(fp);
  return 0;
}

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

static bool getMetaFromInsertJsonFile(cJSON* root) {
/*
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
  */
  
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
    printf("failed to read json, port not found");
    goto PARSE_OVER;
  }

  cJSON* user = cJSON_GetObjectItem(root, "user");
  if (user && user->type == cJSON_String && user->valuestring != NULL) {
    strncpy(g_Dbs.user, user->valuestring, MAX_DB_NAME_SIZE);   
  } else if (!user) {
    printf("failed to read json, user not found\n");
    goto PARSE_OVER;
  }

  cJSON* password = cJSON_GetObjectItem(root, "password");
  if (password && password->type == cJSON_String && password->valuestring != NULL) {
    strncpy(g_Dbs.password, password->valuestring, MAX_DB_NAME_SIZE);
  } else if (!password) {
    printf("failed to read json, password not found\n");
    goto PARSE_OVER;
  }

  cJSON* threads = cJSON_GetObjectItem(root, "thread_count");
  if (!threads || threads->type != cJSON_Number) {
    printf("failed to read json, threads not found");
    goto PARSE_OVER;
  }
  g_Dbs.threadCount = threads->valueint;

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

    cJSON *precision = cJSON_GetObjectItem(dbinfo, "precision");
    if (precision && precision->type == cJSON_String && precision->valuestring != NULL) {
      strncpy(g_Dbs.db[i].dbCfg.precision, precision->valuestring, MAX_DB_NAME_SIZE);
    } else if (!precision) {
      strncpy(g_Dbs.db[i].dbCfg.precision, "ms", MAX_DB_NAME_SIZE);
    } else {
      printf("failed to read json, precision not found");
      goto PARSE_OVER;
    }

    cJSON* update = cJSON_GetObjectItem(dbinfo, "update");
    if (update && update->type == cJSON_Number) {
      g_Dbs.db[i].dbCfg.update = update->valueint;
    } else if (!update) {
      g_Dbs.db[i].dbCfg.update = 0;
    } else {
      printf("failed to read json, update not found");
      goto PARSE_OVER;
    }

    cJSON* replica = cJSON_GetObjectItem(dbinfo, "replica");
    if (replica && replica->type == cJSON_Number) {
      g_Dbs.db[i].dbCfg.replica = replica->valueint;
    } else if (!replica) {
      g_Dbs.db[i].dbCfg.replica = 1;
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

    cJSON* maxtablesPerVnode= cJSON_GetObjectItem(dbinfo, "maxtablesPerVnode");
    if (maxtablesPerVnode && maxtablesPerVnode->type == cJSON_Number) {
      g_Dbs.db[i].dbCfg.maxtablesPerVnode = maxtablesPerVnode->valueint;
    } else if (!maxtablesPerVnode) {
      g_Dbs.db[i].dbCfg.maxtablesPerVnode = -1;
    } else {
     printf("failed to read json, maxtablesPerVnode not found");
     goto PARSE_OVER;
    }

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
      strncpy(g_Dbs.db[i].supterTbls[j].sTblName, stbName->valuestring, MAX_TB_NAME_SIZE);
    
      cJSON *prefix = cJSON_GetObjectItem(stbInfo, "childtable_prefix");
      if (!prefix || prefix->type != cJSON_String || prefix->valuestring == NULL) {
        printf("failed to read json, childtable_prefix not found");
        goto PARSE_OVER;
      }
      strncpy(g_Dbs.db[i].supterTbls[j].childTblPrefix, prefix->valuestring, MAX_DB_NAME_SIZE);

      cJSON *autoCreateTbl = cJSON_GetObjectItem(stbInfo, "auto_create_table");
      if (autoCreateTbl && autoCreateTbl->type == cJSON_String && autoCreateTbl->valuestring != NULL) {
        if (0 == strncasecmp(autoCreateTbl->valuestring, "yes", 3)) {
          g_Dbs.db[i].supterTbls[j].autoCreateTable = AUTO_CREATE_SUBTBL;
        } else if (0 == strncasecmp(autoCreateTbl->valuestring, "null", 4)) {
          g_Dbs.db[i].supterTbls[j].autoCreateTable = NO_CREATE_SUBTBL;
        }else {
          g_Dbs.db[i].supterTbls[j].autoCreateTable = PRE_CREATE_SUBTBL;
        }
      } else if (!autoCreateTbl) {
        g_Dbs.db[i].supterTbls[j].autoCreateTable = PRE_CREATE_SUBTBL;
      } else {
        printf("failed to read json, auto_create_table not found");
        goto PARSE_OVER;
      }
      
      cJSON* count = cJSON_GetObjectItem(stbInfo, "childtable_count");
      if (!count || count->type != cJSON_Number) {
        printf("failed to read json, childtable_count not found");
        goto PARSE_OVER;
      }
      g_Dbs.db[i].supterTbls[j].childTblCount = count->valueint;

      cJSON *dataSource = cJSON_GetObjectItem(stbInfo, "data_source");
      if (dataSource && dataSource->type == cJSON_String && dataSource->valuestring != NULL) {
        strncpy(g_Dbs.db[i].supterTbls[j].dataSource, dataSource->valuestring, MAX_DB_NAME_SIZE);
      } else if (!dataSource) {
        strncpy(g_Dbs.db[i].supterTbls[j].dataSource, "rand", MAX_DB_NAME_SIZE);
      } else {
        printf("failed to read json, data_source not found");
        goto PARSE_OVER;
      }

      cJSON *insertMode = cJSON_GetObjectItem(stbInfo, "insert_mode"); // taosc , restful
      if (insertMode && insertMode->type == cJSON_String && insertMode->valuestring != NULL) {
        strncpy(g_Dbs.db[i].supterTbls[j].insertMode, insertMode->valuestring, MAX_DB_NAME_SIZE);
      } else if (!insertMode) {
        strncpy(g_Dbs.db[i].supterTbls[j].insertMode, "taosc", MAX_DB_NAME_SIZE);
      } else {
        printf("failed to read json, insert_mode not found");
        goto PARSE_OVER;
      }

      cJSON *ts = cJSON_GetObjectItem(stbInfo, "start_timestamp");
      if (ts && ts->type == cJSON_String && ts->valuestring != NULL) {
        strncpy(g_Dbs.db[i].supterTbls[j].startTimestamp, ts->valuestring, MAX_DB_NAME_SIZE);
      } else if (!ts) {
        strncpy(g_Dbs.db[i].supterTbls[j].startTimestamp, "2020-09-01 00:00:00.000", MAX_DB_NAME_SIZE);
      } else {
        printf("failed to read json, start_timestamp not found");
        goto PARSE_OVER;
      }
    
      cJSON* timestampStep = cJSON_GetObjectItem(stbInfo, "timestamp_step");
      if (timestampStep && timestampStep->type == cJSON_Number) {
        g_Dbs.db[i].supterTbls[j].timeStampStep = timestampStep->valueint;
      } else if (!timestampStep) {
        g_Dbs.db[i].supterTbls[j].timeStampStep = 1000;
      } else {
        printf("failed to read json, timestamp_step not found");
        goto PARSE_OVER;
      }
       
      cJSON *sampleFormat = cJSON_GetObjectItem(stbInfo, "sample_format");
      if (sampleFormat && sampleFormat->type == cJSON_String && sampleFormat->valuestring != NULL) {
        strncpy(g_Dbs.db[i].supterTbls[j].sampleFormat, sampleFormat->valuestring, MAX_DB_NAME_SIZE);
      } else if (!sampleFormat) {
        strncpy(g_Dbs.db[i].supterTbls[j].sampleFormat, "csv", MAX_DB_NAME_SIZE);
      } else {
        printf("failed to read json, sample_format not found");
        goto PARSE_OVER;
      }      
      
      cJSON *sampleFile = cJSON_GetObjectItem(stbInfo, "sample_file");
      if (sampleFile && sampleFile->type == cJSON_String && sampleFile->valuestring != NULL) {
        strncpy(g_Dbs.db[i].supterTbls[j].sampleFile, sampleFile->valuestring, MAX_FILE_NAME_LEN);
      } else if (!sampleFile) {
        memset(g_Dbs.db[i].supterTbls[j].sampleFile, 0, MAX_FILE_NAME_LEN);
      } else {
        printf("failed to read json, sample_file not found");
        goto PARSE_OVER;
      }          
      
      cJSON *tagsFile = cJSON_GetObjectItem(stbInfo, "tags_file");
      if (tagsFile && tagsFile->type == cJSON_String && tagsFile->valuestring != NULL) {
        strncpy(g_Dbs.db[i].supterTbls[j].tagsFile, tagsFile->valuestring, MAX_FILE_NAME_LEN);
        if (0 == g_Dbs.db[i].supterTbls[j].tagsFile[0]) {
          g_Dbs.db[i].supterTbls[j].tagSource = 0;
        } else {
          g_Dbs.db[i].supterTbls[j].tagSource = 1;
        }
      } else if (!tagsFile) {
        memset(g_Dbs.db[i].supterTbls[j].tagsFile, 0, MAX_FILE_NAME_LEN);
        g_Dbs.db[i].supterTbls[j].tagSource = 0;
      } else {
        printf("failed to read json, tags_file not found");
        goto PARSE_OVER;
      }    
    
      cJSON* insertRate = cJSON_GetObjectItem(stbInfo, "insert_rate");
      if (insertRate && insertRate->type == cJSON_Number) {
        g_Dbs.db[i].supterTbls[j].insertRate = insertRate->valueint;
      } else if (!insertRate) {
        g_Dbs.db[i].supterTbls[j].insertRate = 0;
      } else {
        printf("failed to read json, insert_rate not found");
        goto PARSE_OVER;
      }
 
      cJSON* insertRows = cJSON_GetObjectItem(stbInfo, "insert_rows");
      if (insertRows && insertRows->type == cJSON_Number) {
        g_Dbs.db[i].supterTbls[j].insertRows = insertRows->valueint;
        if (0 == g_Dbs.db[i].supterTbls[j].insertRows) {
        g_Dbs.db[i].supterTbls[j].insertRows = 0x7FFFFFFFFFFFFFFF;
      }
      } else if (!insertRows) {
        g_Dbs.db[i].supterTbls[j].insertRows = 0x7FFFFFFFFFFFFFFF;
      } else {
        printf("failed to read json, insert_rows not found");
        goto PARSE_OVER;
      }
  
      // columns 
      cJSON *columns = cJSON_GetObjectItem(stbInfo, "columns");
      if (!columns || columns->type != cJSON_Array) {
        printf("failed to read json, columns not found");
        goto PARSE_OVER;
      }
      
      int columnSize = cJSON_GetArraySize(columns);
      if (columnSize > MAX_COLUMN_COUNT) {
        printf("failed to read json, column size overflow, max column size is %d\n", MAX_COLUMN_COUNT);
        goto PARSE_OVER;
      }

      g_Dbs.db[i].supterTbls[j].columnCount = columnSize;  
      for (int k = 0; k < columnSize; ++k) {
        cJSON* column = cJSON_GetArrayItem(columns, k);
        if (column == NULL) continue;
      
        // column info 
        cJSON *dataType = cJSON_GetObjectItem(column, "type");
        if (!dataType || dataType->type != cJSON_String || dataType->valuestring == NULL) {
          printf("failed to read json, column type not found");
          goto PARSE_OVER;
        }
        strncpy(g_Dbs.db[i].supterTbls[j].columns[k].dataType, dataType->valuestring, MAX_TB_NAME_SIZE);
                
        cJSON* dataLen = cJSON_GetObjectItem(column, "len");
        if (dataLen && dataLen->type == cJSON_Number) {
          g_Dbs.db[i].supterTbls[j].columns[k].dataLen = dataLen->valueint;    
        } else if (dataLen && dataLen->type != cJSON_Number) {
          printf("failed to read json, column len not found");
          goto PARSE_OVER;
        } else {
          g_Dbs.db[i].supterTbls[j].columns[k].dataLen = 0;
        }            
      }
  
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
      
      g_Dbs.db[i].supterTbls[j].tagCount = tagSize;  
      for (int k = 0; k < tagSize; ++k) {
        cJSON* tag = cJSON_GetArrayItem(tags, k);
        if (tag == NULL) continue;
      
        // column info 
        cJSON *dataType = cJSON_GetObjectItem(tag, "type");
        if (!dataType || dataType->type != cJSON_String || dataType->valuestring == NULL) {
          printf("failed to read json, tag type not found");
          goto PARSE_OVER;
        }
        strncpy(g_Dbs.db[i].supterTbls[j].tags[k].dataType, dataType->valuestring, MAX_TB_NAME_SIZE);
                
        cJSON* dataLen = cJSON_GetObjectItem(tag, "len");
        if (dataLen && dataLen->type == cJSON_Number) {
          g_Dbs.db[i].supterTbls[j].tags[k].dataLen = dataLen->valueint;    
        } else if (dataLen && dataLen->type != cJSON_Number) {
          printf("failed to read json, column len not found");
          goto PARSE_OVER;
        } else {
          g_Dbs.db[i].supterTbls[j].tags[k].dataLen = 0;
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

static bool getMetaFromQueryJsonFile(cJSON* root) {
/*
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
*/

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
    printf("failed to read json, port not found");
    goto PARSE_OVER;
  }

  cJSON* user = cJSON_GetObjectItem(root, "user");
  if (user && user->type == cJSON_String && user->valuestring != NULL) {
    strncpy(g_queryInfo.user, user->valuestring, MAX_DB_NAME_SIZE);   
  } else if (!user) {
    printf("failed to read json, user not found\n");
    goto PARSE_OVER;
  }

  cJSON* password = cJSON_GetObjectItem(root, "password");
  if (password && password->type == cJSON_String && password->valuestring != NULL) {
    strncpy(g_queryInfo.password, password->valuestring, MAX_DB_NAME_SIZE);
  } else if (!password) {
    printf("failed to read json, password not found\n");
    goto PARSE_OVER;
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
  cJSON *superQuery = cJSON_GetObjectItem(root, "super_table_query");
  if (!superQuery) {
    g_queryInfo.superQueryInfo.concurrent = 0;
    g_queryInfo.superQueryInfo.sqlCount = 0;
  } else if (superQuery->type != cJSON_Object) {
    printf("failed to read json, super_table_query not found");
    goto PARSE_OVER;
  } else {  
    cJSON* rate = cJSON_GetObjectItem(superQuery, "rate");
    if (rate && rate->type == cJSON_Number) {
      g_queryInfo.superQueryInfo.rate = rate->valueint;
    } else if (!rate) {
      g_queryInfo.superQueryInfo.rate = 0;
    }
  
    cJSON* concurrent = cJSON_GetObjectItem(superQuery, "concurrent");
    if (concurrent && concurrent->type == cJSON_Number) {
      g_queryInfo.superQueryInfo.concurrent = concurrent->valueint;
    } else if (!concurrent) {
      g_queryInfo.superQueryInfo.concurrent = 0;
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
  
    cJSON* superSql = cJSON_GetObjectItem(superQuery, "sql");
    if (!superSql) {
      g_queryInfo.superQueryInfo.sqlCount = 0;
    } else if (superSql->type != cJSON_Array) {
      printf("failed to read json, super sql not found\n");
      goto PARSE_OVER;
    } else {  
      int superSqlSize = cJSON_GetArraySize(superSql);
      if (superSqlSize > MAX_QUERY_SQL_COUNT) {
        printf("failed to read json, query sql size overflow, max is %d\n", MAX_QUERY_SQL_COUNT);
        goto PARSE_OVER;
      }
    
      g_queryInfo.superQueryInfo.sqlCount = superSqlSize;
      for (int j = 0; j < superSqlSize; ++j) {
        cJSON* sqlStr = cJSON_GetArrayItem(superSql, j);
        if (sqlStr == NULL) continue;
      
        if (sqlStr->type != cJSON_String || sqlStr->valuestring == NULL) {
          printf("failed to read json, super sql string not found");
          goto PARSE_OVER;
        }
        strncpy(g_queryInfo.superQueryInfo.sql[j], sqlStr->valuestring, MAX_QUERY_SQL_LENGTH);
      }    
    }
  }

  // sub_table_query 
  cJSON *subQuery = cJSON_GetObjectItem(root, "sub_table_query");
  if (!subQuery) {
    g_queryInfo.subQueryInfo.threadCnt = 0;
    g_queryInfo.subQueryInfo.sqlCount = 0;
  } else if (subQuery->type != cJSON_Object) {
    printf("failed to read json, sub_table_query not found");
    ret = true;
    goto PARSE_OVER;
  } else {
    cJSON* subrate = cJSON_GetObjectItem(subQuery, "rate");
    if (subrate && subrate->type == cJSON_Number) {
      g_queryInfo.subQueryInfo.rate = subrate->valueint;
    } else if (!subrate) {
      g_queryInfo.subQueryInfo.rate = 0;
    }
  
    cJSON* threads = cJSON_GetObjectItem(subQuery, "threads");
    if (threads && threads->type == cJSON_Number) {
      g_queryInfo.subQueryInfo.threadCnt = threads->valueint;
    } else if (!threads) {
      g_queryInfo.subQueryInfo.threadCnt = 0;
    }
  
    cJSON* subTblCnt = cJSON_GetObjectItem(subQuery, "childtable_count");
    if (subTblCnt && subTblCnt->type == cJSON_Number) {
      g_queryInfo.subQueryInfo.childTblCount = subTblCnt->valueint;
    } else if (!subTblCnt) {
      g_queryInfo.subQueryInfo.childTblCount = 0;
    }
  
    cJSON* subTblPrefix = cJSON_GetObjectItem(subQuery, "childtable_prefix");
    if (subTblPrefix && subTblPrefix->type == cJSON_String && subTblPrefix->valuestring != NULL) {
      strncpy(g_queryInfo.subQueryInfo.childTblPrefix, subTblPrefix->valuestring, MAX_DB_NAME_SIZE);
    } else {
      printf("failed to read json, childtable_prefix not found\n");
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
  
    cJSON* subSql = cJSON_GetObjectItem(subQuery, "sql");
    if (!subSql || subSql->type != cJSON_Array) {
      printf("failed to read json, sub sql not found\n");
      goto PARSE_OVER;
    }
  
    int subSqlSize = cJSON_GetArraySize(subSql);
    if (subSqlSize > MAX_QUERY_SQL_COUNT) {
      printf("failed to read json, query sql size overflow, max is %d\n", MAX_QUERY_SQL_COUNT);
      goto PARSE_OVER;
    }
  
    g_queryInfo.subQueryInfo.sqlCount = subSqlSize;
    for (int j = 0; j < subSqlSize; ++j) {
      cJSON* sqlStr = cJSON_GetArrayItem(subSql, j);
      if (sqlStr == NULL) continue;
    
      if (sqlStr->type != cJSON_String || sqlStr->valuestring == NULL) {
        printf("failed to read json, sub sql string not found");
        goto PARSE_OVER;
      }
      strncpy(g_queryInfo.subQueryInfo.sql[j], sqlStr->valuestring, MAX_QUERY_SQL_LENGTH);
    }
  }

  ret = true;

PARSE_OVER:
  //free(content);
  //cJSON_Delete(root);
  //fclose(fp);
  return ret;
}

#if 0
static bool getMetaFromJsonFile(char* file) {
  char *name = strrchr(file, '/');
  if (NULL == name) {
    name = file;
  } else {
    name += 1;
  }
  
  if (0 == strcmp(INSERT_JSON_NAME, name)) {
    g_jsonType = INSERT_MODE;
    return getMetaFromInsertJsonFile(file);
  } else if (0 == strcmp(QUERY_JSON_NAME, name)) {
    g_jsonType = QUERY_MODE;
    return getMetaFromQueryJsonFile(file);
  } else if (0 == strcmp(SUBSCRIBE_JSON_NAME, name)) {
    g_jsonType = SUBSCRIBE_MODE;
    return getMetaFromQueryJsonFile(file);
  } else {
    printf("input json file name error! please input correct json file: insert.json or query.json or subscribe.json\n");
    return false;
  }  
}
#endif

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
      if (0 == strncasecmp(g_Dbs.db[i].supterTbls[j].dataSource, "sample", 6)) {
        readSampleFromFileToMem(&g_Dbs.db[i].supterTbls[j]);
      }
      
      if (g_Dbs.db[i].supterTbls[j].tagsFile[0] != 0) {
        (void)readTagFromCsvFileToMem(&g_Dbs.db[i].supterTbls[j]);
      }

      if (0 == strncasecmp(g_Dbs.db[i].supterTbls[j].insertMode, "restful", 8)) {
        curl_global_init(CURL_GLOBAL_ALL);
      }
    }
  }
}

void postFreeResource() {
  for (int i = 0; i < g_Dbs.dbCount; i++) {    
    for (int j = 0; j < g_Dbs.db[i].superTblCount; j++) {
      if (0 == g_Dbs.db[i].supterTbls[j].colsOfCreatChildTable) {
        free(g_Dbs.db[i].supterTbls[j].colsOfCreatChildTable);
        g_Dbs.db[i].supterTbls[j].colsOfCreatChildTable = NULL;
      }
      if (0 == g_Dbs.db[i].supterTbls[j].sampleDataBuf) {
        free(g_Dbs.db[i].supterTbls[j].sampleDataBuf);
        g_Dbs.db[i].supterTbls[j].sampleDataBuf = NULL;
      }

      if (0 == strncasecmp(g_Dbs.db[i].supterTbls[j].insertMode, "restful", 8)) {
        curl_global_cleanup();
      }
    }
  }
}

int getRowDataFromSample(char*  dataBuf, int maxLen, int64_t timestamp, SSuperTable* stbInfo, int sampleUsePos) {
  int    dataLen = 0;
  dataLen += snprintf(dataBuf + dataLen, maxLen - dataLen, "(%" PRId64 ", ", timestamp);
  dataLen += snprintf(dataBuf + dataLen, maxLen - dataLen, "%s", stbInfo->sampleDataBuf + stbInfo->lenOfOneRow * sampleUsePos);
  dataLen += snprintf(dataBuf + dataLen, maxLen - dataLen, ")");
  
  return dataLen;
}

int generateRowData(char*  dataBuf, int maxLen, int64_t timestamp, SSuperTable* stbInfo) {
  int    dataLen = 0;
  dataLen += snprintf(dataBuf + dataLen, maxLen - dataLen, "(%" PRId64 ", ", timestamp);
  for (int i = 0; i < stbInfo->columnCount; i++) {    
    if ((0 == strncasecmp(stbInfo->columns[i].dataType, "binary", 6)) || (0 == strncasecmp(stbInfo->columns[i].dataType, "nchar", 5))) {
      if (stbInfo->columns[i].dataLen > TSDB_MAX_BINARY_LEN) {
        printf("binary or nchar length overflow, max size:%"PRId64"\n", TSDB_MAX_BINARY_LEN);
        exit(-1);
      }
    
      char* buf = (char*)calloc(stbInfo->columns[i].dataLen+1, 1);
      if (NULL == buf) {
        printf("calloc failed! size:%d\n", stbInfo->columns[i].dataLen);
        exit(-1);
      }
      rand_string(buf, stbInfo->columns[i].dataLen);
      dataLen += snprintf(dataBuf + dataLen, maxLen - dataLen, "\'%s\', ", buf);
      free(buf);
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
      exit(-1);
    }
  }
  dataLen -= 2;
  dataLen += snprintf(dataBuf + dataLen, maxLen - dataLen, ")");
  
  return dataLen;
}

// sync insertion
/*
   1 thread: 100 tables * 2000  rows/s
   1 thread: 10  tables * 20000 rows/s
   6 thread: 300 tables * 2000  rows/s

   2 taosinsertdata , 1 thread:  10  tables * 20000 rows/s
*/
void *syncWrite(void *sarg) {

  threadInfo *winfo = (threadInfo *)sarg; 
  SSuperTable* superTblInfo = winfo->superTblInfo;

  //printf("========threadID[%d], table rang: %d - %d \n", winfo->threadID, winfo->start_table_id, winfo->end_table_id);

  char* buffer = calloc(TSDB_MAX_SQL_LEN, 1);

  int nrecords_per_request = 0;
  if (AUTO_CREATE_SUBTBL == superTblInfo->autoCreateTable) {
    nrecords_per_request = (TSDB_MAX_SQL_LEN - 1280 - superTblInfo->lenOfTagOfOneRow) / superTblInfo->lenOfOneRow;
  } else {
    nrecords_per_request = (TSDB_MAX_SQL_LEN - 1280) / superTblInfo->lenOfOneRow;
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

      int sampleUsePos = superTblInfo->sampleUsePos;
      int k = 0;
      while (1)
      {        
        int len = 0;
        memset(buffer, 0, TSDB_MAX_SQL_LEN);
        char *pstr = buffer;

        if (AUTO_CREATE_SUBTBL == superTblInfo->autoCreateTable) {
          char* tagsValBuf = NULL;
          if (0 == superTblInfo->tagSource) {
            tagsValBuf = generateTagVaulesForStb(superTblInfo);
          } else {
            tagsValBuf = getTagValueFromTagSample(superTblInfo, tID % superTblInfo->tagSampleCount);
          }
        
          len += snprintf(pstr + len, TSDB_MAX_SQL_LEN - len, "insert into %s.%s%d using %s.%s tags %s values", winfo->db_name, superTblInfo->childTblPrefix, tID, winfo->db_name, superTblInfo->sTblName, tagsValBuf);
          free(tagsValBuf);
        } else {
          len += snprintf(pstr + len, TSDB_MAX_SQL_LEN - len, "insert into %s.%s%d values", winfo->db_name, superTblInfo->childTblPrefix, tID);
        }
        
        for (k = 0; k < nrecords_cur_req;) {
          int retLen = 0;
          if (0 == strncasecmp(superTblInfo->dataSource, "sample", 6)) {
            retLen = getRowDataFromSample(pstr + len, TSDB_MAX_SQL_LEN - len, tmp_time += superTblInfo->timeStampStep, superTblInfo, sampleUsePos);
            sampleUsePos++;
            sampleUsePos %= superTblInfo->sampleRowCount;
          } else if (0 == strncasecmp(superTblInfo->dataSource, "rand", 8)) {        
            retLen = generateRowData(pstr + len, TSDB_MAX_SQL_LEN - len, tmp_time += superTblInfo->timeStampStep, superTblInfo);
          }
          len += retLen;
          inserted++;
          k++;
  
          if (inserted >= superTblInfo->insertRows || (TSDB_MAX_SQL_LEN - len) < (superTblInfo->lenOfOneRow + 128)) break;
        }
  
        if (0 == strncasecmp(superTblInfo->insertMode, "taosc", 5)) {          
          //int64_t t1 = taosGetTimestampMs();
          queryDB(winfo->taos, buffer);
          //int64_t t2 = taosGetTimestampMs();          
          //printf("taosc insert sql return, Spent %.4f seconds \n", (double)(t2 - t1)/1000.0);          
        } else {
          //int64_t t1 = taosGetTimestampMs();
          int retCode = curlProceSql(g_Dbs.host, g_Dbs.port, buffer, winfo->curl_handle);
          //int64_t t2 = taosGetTimestampMs();          
          //printf("http insert sql return, Spent %ld ms \n", t2 - t1);
          
          if (0 != retCode) {
            printf("========curl return fail, threadID[%d]\n", winfo->threadID);
            free(buffer);
            return NULL;
          }
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
          superTblInfo->sampleUsePos = sampleUsePos;
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
  free(buffer);
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
  
  int last = 0;
  for (int i = 0; i < threads; i++) {
    threadInfo *t_info = infos + i;
    t_info->threadID = i;
    tstrncpy(t_info->db_name, db_name, MAX_DB_NAME_SIZE);
    t_info->superTblInfo = superTblInfo;
    if (0 == strncasecmp(precision, "ms", 2)) {
      (void)taosParseTime(superTblInfo->startTimestamp, &t_info->start_time, strlen(superTblInfo->startTimestamp), TSDB_TIME_PRECISION_MILLI, 0);
    }  else if (0 == strncasecmp(precision, "us", 2)) {
      (void)taosParseTime(superTblInfo->startTimestamp, &t_info->start_time, strlen(superTblInfo->startTimestamp), TSDB_TIME_PRECISION_MICRO, 0);
    }  else {
      printf("No support precision: %s\n", precision);
      exit(-1);
    }

    if (0 == strncasecmp(superTblInfo->insertMode, "taosc", 5)) {
      t_info->taos = taos_connect(g_Dbs.host, g_Dbs.user, g_Dbs.password, db_name, g_Dbs.port);
    } else {
      t_info->taos = NULL;
      t_info->curl_handle = curl_easy_init();      
    }
    
    t_info->start_table_id = last;
    t_info->end_table_id = i < b ? last + a : last + a - 1;
    last = t_info->end_table_id + 1;

    pthread_create(pids + i, NULL, syncWrite, t_info);
  }
  
  for (int i = 0; i < threads; i++) {
    pthread_join(pids[i], NULL);
  }

  for (int i = 0; i < threads; i++) {
    threadInfo *t_info = infos + i;
    taos_close(t_info->taos);
    if (t_info->curl_handle) {
      curl_easy_cleanup(t_info->curl_handle);
    }
  }

  free(pids);
  free(infos);
}


int insertTestProcess() {
  printfInsertMeta();

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
  printf("Spent %.4f seconds to create %d tables with %d thread(s)\n\n", end - start, g_totalChildTables, g_Dbs.threadCount);

  usleep(1000*1000);

  // create sub threads for inserting data
  start = getCurrentTime();
  for (int i = 0; i < g_Dbs.dbCount; i++) {    
    for (int j = 0; j < g_Dbs.db[i].superTblCount; j++) {
      SSuperTable* superTblInfo = &g_Dbs.db[i].supterTbls[j];
      startMultiThreadInsertData(g_Dbs.threadCount, g_Dbs.db[i].dbName, g_Dbs.db[i].dbCfg.precision, superTblInfo);
      g_totalRecords += superTblInfo->insertRows * superTblInfo->childTblCount;
    }    
  }  
  end = getCurrentTime();
  printf("Spent %.4f seconds to insert %"PRId64" records with %d thread(s)\n\n", end - start, g_totalRecords, g_Dbs.threadCount);

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
        selectAndGetResult(winfo->taos, g_queryInfo.superQueryInfo.sql[i]); 
        int64_t t2 = taosGetTimestampUs();          
        printf("taosc select sql return, Spent %ld us \n", t2 - t1);          
      } else {
        int64_t t1 = taosGetTimestampUs();
        int retCode = curlProceSql(g_queryInfo.host, g_queryInfo.port, g_queryInfo.superQueryInfo.sql[i], winfo->curl_handle);
        int64_t t2 = taosGetTimestampUs();          
        printf("http select sql return, Spent %ld us \n", t2 - t1);
        
        if (0 != retCode) {
          printf("========curl return fail, threadID[%d]\n", winfo->threadID);
          return NULL;
        }
      }    
    }
    et = taosGetTimestampMs();
    printf("========thread[%"PRId64"] complete all sqls to super table once queries duration:%.6fs\n\n", pthread_self(), (double)(et - st)/1000.0);
  }
  return NULL;
}

void replaceSubTblName(char* inSql, char* outSql, int tblIndex) {
  char sourceString[32] = "xxxx";
  char subTblName[MAX_TB_NAME_SIZE*3];
  sprintf(subTblName, "%s.%s%d", g_queryInfo.dbName, g_queryInfo.subQueryInfo.childTblPrefix, tblIndex);

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
  int64_t et = 0;
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
        selectAndGetResult(winfo->taos, sqlstr); 
      }
    }
    et = taosGetTimestampMs();
    printf("========thread[%"PRId64"] complete all sqls to allocate all sub-tables once queries duration:%.4fs\n\n", pthread_self(), (double)(et - st)/1000.0);
  }
  return NULL;
}

int queryTestProcess() {
  printfQueryMeta();

  printf("Press enter key to continue\n\n");
  (void)getchar();

  
  pthread_t  *pids  = NULL;
  threadInfo *infos = NULL;
  //==== create sub threads for query from super table
  if (g_queryInfo.superQueryInfo.sqlCount > 0) {
    if (0 == g_queryInfo.superQueryInfo.concurrent)  g_queryInfo.superQueryInfo.concurrent = 1;
    
    pids  = malloc(g_queryInfo.superQueryInfo.concurrent * sizeof(pthread_t));
    infos = malloc(g_queryInfo.superQueryInfo.concurrent * sizeof(threadInfo));
    if ((NULL == pids) || (NULL == infos)) {
      printf("malloc failed for create threads\n");
      exit(-1);
    }
    
    for (int i = 0; i < g_queryInfo.superQueryInfo.concurrent; i++) {  
      threadInfo *t_info = infos + i;
      t_info->threadID = i;    
  
      if (0 == strncasecmp(g_queryInfo.queryMode, "taosc", 5)) {
        t_info->taos = taos_connect(g_queryInfo.host, g_queryInfo.user, g_queryInfo.password, g_queryInfo.dbName, g_queryInfo.port);
        
        char sqlStr[MAX_TB_NAME_SIZE*2];
        sprintf(sqlStr, "use %s", g_queryInfo.dbName);
        queryDB(t_info->taos, sqlStr);
      } else {
        t_info->taos = NULL;
        t_info->curl_handle = curl_easy_init();      
      }
  
      pthread_create(pids + i, NULL, superQueryProcess, t_info);    
    }  
  }else {
    g_queryInfo.superQueryInfo.concurrent = 0;
  }
  

  pthread_t  *pidsOfSub  = NULL;
  threadInfo *infosOfSub = NULL;
  //==== create sub threads for query from sub table
  if (g_queryInfo.subQueryInfo.sqlCount > 0) {
    if (0 == g_queryInfo.subQueryInfo.threadCnt)  g_queryInfo.subQueryInfo.threadCnt = 1;
    
    pidsOfSub  = malloc(g_queryInfo.subQueryInfo.threadCnt * sizeof(pthread_t));
    infosOfSub = malloc(g_queryInfo.subQueryInfo.threadCnt * sizeof(threadInfo));
    if ((NULL == pidsOfSub) || (NULL == infosOfSub)) {
      printf("malloc failed for create threads\n");
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
    for (int i = 0; i < g_queryInfo.subQueryInfo.threadCnt; i++) {  
      threadInfo *t_info = infosOfSub + i;
      t_info->threadID = i;
      
      t_info->start_table_id = last;
      t_info->end_table_id = i < b ? last + a : last + a - 1;
      t_info->taos = taos_connect(g_queryInfo.host, g_queryInfo.user, g_queryInfo.password, g_queryInfo.dbName, g_queryInfo.port);
      pthread_create(pidsOfSub + i, NULL, subQueryProcess, t_info);
    }
  }else {
    g_queryInfo.subQueryInfo.threadCnt = 0;
  }  
  
  for (int i = 0; i < g_queryInfo.superQueryInfo.concurrent; i++) {
    pthread_join(pids[i], NULL);
  }

  for (int i = 0; i < g_queryInfo.superQueryInfo.concurrent; i++) {
    threadInfo *t_info = infos + i;
    taos_close(t_info->taos);
  }

  if (pids) free(pids);
  if (infos) free(infos);  
  
  for (int i = 0; i < g_queryInfo.subQueryInfo.threadCnt; i++) {
    pthread_join(pidsOfSub[i], NULL);
  }

  for (int i = 0; i < g_queryInfo.subQueryInfo.threadCnt; i++) {
    threadInfo *t_info = infosOfSub + i;
    taos_close(t_info->taos);
  }

  if (pidsOfSub) free(pidsOfSub);
  if (infosOfSub) free(infosOfSub);  

  return 0;
}

static void getResult(TAOS_RES *res) {  
  TAOS_ROW    row = NULL;
  int         num_rows = 0;
  int         num_fields = taos_field_count(res);
  TAOS_FIELD *fields     = taos_fetch_fields(res);
  char        temp[4096];

  // fetch the records row by row
  while ((row = taos_fetch_row(res))) {
    num_rows++;
    taos_print_row(temp, row, fields, num_fields);
    printf("query result:%s\n", temp);
  }

  taos_free_result(res);
}

static void subscribe_callback(TAOS_SUB* tsub, TAOS_RES *res, void* param, int code) {  
  if (res == NULL || taos_errno(res) != 0) {
    printf("failed to subscribe result, code:%d, reason:%s\n", code, taos_errstr(res));
    exit(1);
  }
  
  getResult(res);
}

static TAOS_SUB* subscribeImpl(TAOS *taos, char *sql, char* topic) {
  TAOS_SUB* tsub = NULL;  
  int blockFetch = 0;

  if (g_queryInfo.superQueryInfo.subscribeMode) {
    tsub = taos_subscribe(taos, g_queryInfo.superQueryInfo.subscribeRestart, topic, sql, subscribe_callback, &blockFetch, g_queryInfo.superQueryInfo.subscribeInterval);
  } else {
    tsub = taos_subscribe(taos, g_queryInfo.superQueryInfo.subscribeRestart, topic, sql, NULL, NULL, 0);
  }

  if (tsub == NULL) {
    printf("failed to create subscription. topic:%s, sql:%s\n", topic, sql);
    exit(0);
  } 

  return tsub;
}

void *subSubscribeProcess(void *sarg) {
  threadInfo *winfo = (threadInfo *)sarg; 
  char subSqlstr[1024];

  char sqlStr[MAX_TB_NAME_SIZE*2];
  sprintf(sqlStr, "use %s", g_queryInfo.dbName);
  queryDB(winfo->taos, sqlStr);
  
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
      sprintf(topic, "lowa-subscribe-%d", i);
        memset(subSqlstr,0,sizeof(subSqlstr));
        replaceSubTblName(g_queryInfo.subQueryInfo.sql[i], subSqlstr, i);
      g_queryInfo.subQueryInfo.tsub[i] = subscribeImpl(winfo->taos, subSqlstr, topic); 
    }
    //et = taosGetTimestampMs();
    //printf("========thread[%"PRId64"] complete all sqls to super table once queries duration:%.4fs\n", pthread_self(), (double)(et - st)/1000.0);
  } while (0);

  // start loop to consume result
  while (1) {
    for (int i = 0; i < g_queryInfo.subQueryInfo.sqlCount; i++) {
      if (1 == g_queryInfo.subQueryInfo.subscribeMode) {
        continue;
      }
      
      TAOS_RES* res = taos_consume(g_queryInfo.subQueryInfo.tsub[i]);
      if (res) {
        getResult(res);
      }
    }
  }
  
  for (int i = 0; i < g_queryInfo.subQueryInfo.sqlCount; i++) {
    taos_unsubscribe(g_queryInfo.subQueryInfo.tsub[i], g_queryInfo.subQueryInfo.subscribeKeepProgress);
    taos_close(winfo->taos);  
  }
  return NULL;
}

void *superSubscribeProcess(void *sarg) {
  threadInfo *winfo = (threadInfo *)sarg; 

  char sqlStr[MAX_TB_NAME_SIZE*2];
  sprintf(sqlStr, "use %s", g_queryInfo.dbName);
  queryDB(winfo->taos, sqlStr);
  
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
      sprintf(topic, "lowa-subscribe-%d", i);
      g_queryInfo.superQueryInfo.tsub[i] = subscribeImpl(winfo->taos, g_queryInfo.superQueryInfo.sql[i], topic); 
    }
    //et = taosGetTimestampMs();
    //printf("========thread[%"PRId64"] complete all sqls to super table once queries duration:%.4fs\n", pthread_self(), (double)(et - st)/1000.0);
  } while (0);

  // start loop to consume result
  while (1) {
    for (int i = 0; i < g_queryInfo.superQueryInfo.sqlCount; i++) {
      if (1 == g_queryInfo.superQueryInfo.subscribeMode) {
        continue;
      }
      
      TAOS_RES* res = taos_consume(g_queryInfo.superQueryInfo.tsub[i]);
      if (res) {
        getResult(res);
      }
    }
  }
  
  for (int i = 0; i < g_queryInfo.superQueryInfo.sqlCount; i++) {
    taos_unsubscribe(g_queryInfo.superQueryInfo.tsub[i], g_queryInfo.superQueryInfo.subscribeKeepProgress);
    taos_close(winfo->taos);  
  }
  return NULL;
}

int subscribeTestProcess() {
  printfQueryMeta();

  printf("Press enter key to continue\n\n");
  (void)getchar();
  
  //==== create sub threads for query from super table
  if (0 == g_queryInfo.superQueryInfo.concurrent)  g_queryInfo.superQueryInfo.concurrent = 1;
  
  pthread_t  *pids  = malloc(g_queryInfo.superQueryInfo.concurrent * sizeof(pthread_t));
  threadInfo *infos = malloc(g_queryInfo.superQueryInfo.concurrent * sizeof(threadInfo));
  if ((NULL == pids) || (NULL == infos)) {
    printf("malloc failed for create threads\n");
    exit(-1);
  }
  
  for (int i = 0; i < g_queryInfo.superQueryInfo.concurrent; i++) {  
    threadInfo *t_info = infos + i;
    t_info->threadID = i;
    t_info->taos = taos_connect(g_queryInfo.host, g_queryInfo.user, g_queryInfo.password, g_queryInfo.dbName, g_queryInfo.port);
    pthread_create(pids + i, NULL, superSubscribeProcess, t_info);
  }  

  int subscribeFlag = 1;
  if (0 == g_queryInfo.subQueryInfo.sqlCount) {
    subscribeFlag = 0;
    goto no_subscribe;
  }
  
  //==== create sub threads for query from sub table
  if (0 == g_queryInfo.subQueryInfo.threadCnt)  g_queryInfo.subQueryInfo.threadCnt = 1;
  
  pthread_t  *pidsOfSub  = malloc(g_queryInfo.subQueryInfo.threadCnt * sizeof(pthread_t));
  threadInfo *infosOfSub = malloc(g_queryInfo.subQueryInfo.threadCnt * sizeof(threadInfo));
  if ((NULL == pidsOfSub) || (NULL == infosOfSub)) {
    printf("malloc failed for create threads\n");
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
  for (int i = 0; i < g_queryInfo.subQueryInfo.threadCnt; i++) {  
    threadInfo *t_info = infosOfSub + i;
    t_info->threadID = i;
    
    t_info->start_table_id = last;
    t_info->end_table_id = i < b ? last + a : last + a - 1;
    t_info->taos = taos_connect(g_queryInfo.host, g_queryInfo.user, g_queryInfo.password, g_queryInfo.dbName, g_queryInfo.port);
    pthread_create(pidsOfSub + i, NULL, subSubscribeProcess, t_info);
  }

  no_subscribe:
  
  for (int i = 0; i < g_queryInfo.superQueryInfo.concurrent; i++) {
    pthread_join(pids[i], NULL);
  }  

  for (int i = 0; i < g_queryInfo.superQueryInfo.concurrent; i++) {
    threadInfo *t_info = infos + i;
    taos_close(t_info->taos);
  }

  free(pids);
  free(infos);  

  if (subscribeFlag) {    
    for (int i = 0; i < g_queryInfo.subQueryInfo.threadCnt; i++) {
      pthread_join(pidsOfSub[i], NULL);
    }

    for (int i = 0; i < g_queryInfo.subQueryInfo.threadCnt; i++) {
      threadInfo *t_info = infosOfSub + i;
      taos_close(t_info->taos);
    }
  
    free(pidsOfSub);
    free(infosOfSub);  
  }
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
}

void initOfQueryMeta() {
  memset(&g_queryInfo, 0, sizeof(SQueryMetaInfo));
   
   // set default values
   strncpy(g_queryInfo.host, "127.0.0.1", MAX_DB_NAME_SIZE);
   g_queryInfo.port = 6030;
   strncpy(g_queryInfo.user, TSDB_DEFAULT_USER, MAX_DB_NAME_SIZE);
   strncpy(g_queryInfo.password, TSDB_DEFAULT_PASS, MAX_DB_NAME_SIZE);
}

int main(int argc, char *argv[]) {
  parse_args(argc, argv, &g_args);

  initOfInsertMeta();
  initOfQueryMeta();
  
  if (false == getInfoFromJsonFile(g_args.metaFile)) {
    printf("Failed to read %s\n", g_args.metaFile);
    return 1;
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

