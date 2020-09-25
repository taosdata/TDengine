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

#define   MAX_LINE_COUNT_IN_MEM  10000

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
  bool         autoCreateTable;
  char         childTblPrefix[MAX_TB_NAME_SIZE];
  char         dataSource[MAX_TB_NAME_SIZE];  // rand_gen or sample
  char         insertMode[MAX_TB_NAME_SIZE];  // taosc, resetful
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
  char         host[MAX_DB_NAME_SIZE];
  uint16_t     port;
  char         user[MAX_DB_NAME_SIZE];
  char         password[MAX_DB_NAME_SIZE];
  int          threadCount;
  int          dbCount;
  SDataBase    db[MAX_DB_COUNT];
} SDbs;

typedef struct SThreadInfo_S {
  TAOS *taos;
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
    {0, 'f', "meta file",            0, "The meta data to the execution procedure. Default is './meta.json'.",      0},
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

static bool getMetaFromJsonFile(char* fileName);
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

SDbs       g_Dbs;
int        g_totalChildTables = 0;
int64_t    g_totalRecords = 0;


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
    --size;
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

static void printfMeta() {
  printf("\033[1m\033[40;32m================ json parse result ================\033[0m\n");
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
  printf("\033[1m\033[40;32m================ json parse result ================\033[0m\n");
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

void curlProceSql(char* host, char* sqlstr)
{
  curlProceLogin();

  CURL *curl_handle;
  CURLcode res;
 
  curlMemInfo chunk;
 
  chunk.buf = malloc(1);  /* will be grown as needed by the realloc above */ 
  chunk.sizeleft = 0;    /* no data at this point */ 

  
  char dstUrl[128] = {0};
  snprintf(dstUrl, 128, "http://%s:6041/rest/sql", host);
        
  //curl_global_init(CURL_GLOBAL_ALL);
 
  /* init the curl session */ 
  curl_handle = curl_easy_init();
 
  //curl_easy_setopt(curl_handle,CURLOPT_POSTFIELDS,"");
  curl_easy_setopt(curl_handle, CURLOPT_POST, 1L);
  
  /* specify URL to get */ 
  curl_easy_setopt(curl_handle, CURLOPT_URL, dstUrl);
 
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
  }
  else {
    /* curl_easy_perform() block end and return result */  
    //printf("[%32.32s] sql response len:%lu, content: %s \n\n", sqlstr, (unsigned long)chunk.sizeleft, chunk.buf);
    ;
  }

  curl_slist_free_all(list); /* free the list again */
  
  /* cleanup curl stuff */ 
  curl_easy_cleanup(curl_handle);
 
  free(chunk.buf);
 
  /* we're done with libcurl, so clean it up */ 
  //curl_global_cleanup();
 
  return;
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
        } else {
          exit(-1);
        }
      }

      g_Dbs.db[i].supterTbls[j].lenOfOneRow = lenOfOneRow;
      //printf("%s.%s column count:%d, column length:%d\n\n", g_Dbs.db[i].dbName, g_Dbs.db[i].supterTbls[j].sTblName, g_Dbs.db[i].supterTbls[j].columnCount, lenOfOneRow);

      // save for creating child table
      g_Dbs.db[i].supterTbls[j].colsOfCreatChildTable = (char*)calloc(len+1, 1);
      if (NULL == g_Dbs.db[i].supterTbls[j].colsOfCreatChildTable) {
        printf("Failed when calloc, size:%d", len+1);
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

  free(pids);
  free(infos);  
}


static void createChildTables() {
  for (int i = 0; i < g_Dbs.dbCount; i++) {    
    for (int j = 0; j < g_Dbs.db[i].superTblCount; j++) {
      if (g_Dbs.db[i].supterTbls[j].autoCreateTable) {
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
  
  while ((readLen = taosGetline(&line, &n, fp)) != -1) {
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
  
  while ((readLen = taosGetline(&line, &n, fp)) != -1) {
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

static bool getMetaFromJsonFile(char* fileName) {
  FILE *fp = fopen(fileName, "r");
  if (!fp) {
    printf("failed to read %s, reason:%s\n", fileName, strerror(errno));
    return false;
  }

  bool  ret = false;
  int   maxLen = 64000;
  char *content = calloc(1, maxLen + 1);
  int   len = fread(content, 1, maxLen, fp);
  if (len <= 0) {
    free(content);
    fclose(fp);
    printf("failed to read %s, content is null", fileName);
    return false;
  }

  content[len] = 0;
  cJSON* root = cJSON_Parse(content);
  if (root == NULL) {
    printf("failed to cjson parse %s, invalid json format", fileName);
    goto PARSE_OVER;
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
      memset(g_Dbs.db[i].dbCfg.precision, 0, MAX_DB_NAME_SIZE);
    } else {
      printf("failed to read json, name not found");
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
          g_Dbs.db[i].supterTbls[j].autoCreateTable = 1;
        } else {
          g_Dbs.db[i].supterTbls[j].autoCreateTable = 0;
        }
      } else if (!autoCreateTbl) {
        g_Dbs.db[i].supterTbls[j].autoCreateTable = 0;
      } else {
        printf("failed to read json, childtable_prefix not found");
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

      cJSON *insertMode = cJSON_GetObjectItem(stbInfo, "insert_mode"); // taosc , resetful
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
        g_Dbs.db[i].supterTbls[j].tagSource = 1;
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
        readTagFromCsvFileToMem(&g_Dbs.db[i].supterTbls[j]);
      }

      if (0 == strncasecmp(g_Dbs.db[i].supterTbls[j].insertMode, "resetful", 8)) {
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

      if (0 == strncasecmp(g_Dbs.db[i].supterTbls[j].insertMode, "resetful", 8)) {
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

  char* buffer = calloc(TSDB_MAX_SQL_LEN, 1);

  int nrecords_per_request = 0;
  if (superTblInfo->autoCreateTable) {
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

    st = taosGetTimestampMs();
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

        if (superTblInfo->autoCreateTable) {
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
  
          if (inserted >= superTblInfo->insertRows) break;
        }
  
        if (0 == strncasecmp(superTblInfo->insertMode, "taosc", 5)) {
          queryDB(winfo->taos, buffer);
        } else {
          curlProceSql(g_Dbs.host, buffer);
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
    et = taosGetTimestampMs();
    //printf("========loop %d childTables duration:%"PRId64 "========inserted rows:%d\n", winfo->end_table_id - winfo->start_table_id, et - st, i);
  }
  return NULL;
}

void startMultiThreadInsertData(int threads, char* db_name, char* precision, SSuperTable* superTblInfo) {
  pthread_t *pids = malloc(threads * sizeof(pthread_t));
  threadInfo *infos = malloc(threads * sizeof(threadInfo));
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
  }

  free(pids);
  free(infos);
}

int main(int argc, char *argv[]) {
  parse_args(argc, argv, &g_args);

  memset(&g_Dbs, 0, sizeof(SDbs));
  
  // set default values
  strncpy(g_Dbs.host, "127.0.0.1", MAX_DB_NAME_SIZE);
  g_Dbs.port = 6030;
  strncpy(g_Dbs.user, TSDB_DEFAULT_USER, MAX_DB_NAME_SIZE);
  strncpy(g_Dbs.password, TSDB_DEFAULT_PASS, MAX_DB_NAME_SIZE);
  g_Dbs.threadCount = 2;
  
  if (false == getMetaFromJsonFile(g_args.metaFile)) {
    printf("Failed to read %s\n", g_args.metaFile);
    return 1;
  }

  printfMeta();

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

