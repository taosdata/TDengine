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
#include "os.h"
#include "taos.h"
#include "taosdef.h"
#include "taosmsg.h"
#include "tglobal.h"
#include "tsclient.h"
#include "tsdb.h"
#include "tutil.h"

#define COMMAND_SIZE 65536
//#define DEFAULT_DUMP_FILE "taosdump.sql"

int  converStringToReadable(char *str, int size, char *buf, int bufsize);
int  convertNCharToReadable(char *str, int size, char *buf, int bufsize);
void taosDumpCharset(FILE *fp);
void taosLoadFileCharset(FILE *fp, char *fcharset);

typedef struct {
  short bytes;
  int8_t type;
} SOColInfo;

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

typedef struct {
  char field[TSDB_COL_NAME_LEN + 1];
  char type[16];
  int length;
  char note[128];
} SColDes;

typedef struct {
  char name[TSDB_COL_NAME_LEN + 1];
  SColDes cols[];
} STableDef;

extern char version[];

typedef struct {
  char name[TSDB_DB_NAME_LEN + 1];
  int32_t  tables;
  int32_t  vgroups;  
  int16_t  replications;
  int16_t  quorum;
  int16_t  daysPerFile;  
  int16_t  daysToKeep;
  int16_t  daysToKeep1;
  int16_t  daysToKeep2;
  int32_t  cacheBlockSize; //MB
  int32_t  totalBlocks;
  int32_t  minRowsPerFileBlock;
  int32_t  maxRowsPerFileBlock;
  int8_t   walLevel;
  int32_t  fsyncPeriod;
  int8_t   compression;
  int8_t   precision;   // time resolution
  int8_t   update;
} SDbInfo;

typedef struct {
  char name[TSDB_TABLE_NAME_LEN + 1];
  char metric[TSDB_TABLE_NAME_LEN + 1];
} STableRecord;

typedef struct {
  bool isMetric;
  STableRecord tableRecord;
} STableRecordInfo;

typedef struct {
  pthread_t threadID;
  int32_t   threadIndex;
  int32_t   totalThreads;
  char      dbName[TSDB_TABLE_NAME_LEN + 1];
  void     *taosCon;
} SThreadParaObj;

static int64_t totalDumpOutRows = 0;

SDbInfo **dbInfos = NULL;

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
  {"host",          'h', "HOST",        0,  "Server host dumping data from. Default is localhost.",        0},
  {"user",          'u', "USER",        0,  "User name used to connect to server. Default is root.",       0},
  #ifdef _TD_POWER_
  {"password",      'p', "PASSWORD",    0,  "User password to connect to server. Default is powerdb.",     0},
  #else
  {"password",      'p', "PASSWORD",    0,  "User password to connect to server. Default is taosdata.",    0},
  #endif
  {"port",          'P', "PORT",        0,  "Port to connect",                                             0},
  {"cversion",      'v', "CVERION",     0,  "client version",                                              0},
  {"mysqlFlag",     'q', "MYSQLFLAG",   0,  "mysqlFlag, Default is 0",                                     0},
  // input/output file
  {"outpath",       'o', "OUTPATH",     0,  "Output file path.",                                          1},
  {"inpath",        'i', "INPATH",      0,  "Input file path.",                                           1},
  #ifdef _TD_POWER_
  {"config",        'c', "CONFIG_DIR",  0,  "Configure directory. Default is /etc/power/taos.cfg.",       1},
  #else
  {"config",        'c', "CONFIG_DIR",  0,  "Configure directory. Default is /etc/taos/taos.cfg.",        1},
  #endif
  {"encode",        'e', "ENCODE",      0,  "Input file encoding.",                                       1},
  // dump unit options
  {"all-databases", 'A', 0,             0,  "Dump all databases.",                                         2},
  {"databases",     'B', 0,             0,  "Dump assigned databases",                                     2},
  // dump format options
  {"schemaonly",    's', 0,             0,  "Only dump schema.",                                           3},
  {"with-property", 'M', 0,             0,  "Dump schema with properties.",                                3},
  {"start-time",    'S', "START_TIME",  0,  "Start time to dump.",                                         3},
  {"end-time",      'E', "END_TIME",    0,  "End time to dump.",                                           3},
  {"data-batch",    'N', "DATA_BATCH",  0,  "Number of data point per insert statement. Default is 1.",    3},
  {"max-sql-len",   'L', "SQL_LEN",     0,  "Max length of one sql. Default is 65480.",                    3},
  {"table-batch",   't', "TABLE_BATCH", 0,  "Number of table dumpout into one output file. Default is 1.", 3},
  {"thread_num",    'T', "THREAD_NUM",  0,  "Number of thread for dump in file. Default is 5.",            3},  
  {"allow-sys",     'a', 0,             0,  "Allow to dump sys database",                                  3},
  {0}};

/* Used by main to communicate with parse_opt. */
struct arguments {
  // connection option
  char    *host;
  char    *user;
  char    *password;
  uint16_t port;  
  char     cversion[12];
  uint16_t mysqlFlag;
  // output file
  char     outpath[TSDB_FILENAME_LEN+1];
  char     inpath[TSDB_FILENAME_LEN+1];
  char    *encode;
  // dump unit option
  bool     all_databases;
  bool     databases;
  // dump format option
  bool     schemaonly;
  bool     with_property;
  int64_t  start_time;
  int64_t  end_time;
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
};

/* Parse a single option. */
static error_t parse_opt(int key, char *arg, struct argp_state *state) {
  /* Get the input argument from argp_parse, which we
     know is a pointer to our arguments structure. */
  struct arguments *arguments = state->input;
  wordexp_t full_path;

  switch (key) {
    // connection option
    case 'a':
      arguments->allow_sys = true;
      break;
    case 'h':
      arguments->host = arg;
      break;
    case 'u':
      arguments->user = arg;
      break;
    case 'p':
      arguments->password = arg;
      break;
    case 'P':
      arguments->port = atoi(arg);
      break;
    case 'q':
      arguments->mysqlFlag = atoi(arg);
      break;
    case 'v':
      if (wordexp(arg, &full_path, 0) != 0) {
        fprintf(stderr, "Invalid client vesion %s\n", arg);
        return -1;
      }
      tstrncpy(arguments->cversion, full_path.we_wordv[0], 11);
      wordfree(&full_path);
      break;
    // output file path
    case 'o':
      if (wordexp(arg, &full_path, 0) != 0) {
        fprintf(stderr, "Invalid path %s\n", arg);
        return -1;
      }
      tstrncpy(arguments->outpath, full_path.we_wordv[0], TSDB_FILENAME_LEN);
      wordfree(&full_path);
      break;
    case 'i':
      arguments->isDumpIn = true;
      if (wordexp(arg, &full_path, 0) != 0) {
        fprintf(stderr, "Invalid path %s\n", arg);
        return -1;
      }
      tstrncpy(arguments->inpath, full_path.we_wordv[0], TSDB_FILENAME_LEN);
      wordfree(&full_path);
      break;
    case 'c':
      if (wordexp(arg, &full_path, 0) != 0) {
        fprintf(stderr, "Invalid path %s\n", arg);
        return -1;
      }
      tstrncpy(configDir, full_path.we_wordv[0], TSDB_FILENAME_LEN);
      wordfree(&full_path);
      break;
    case 'e':
      arguments->encode = arg;
      break;
    // dump unit option
    case 'A':
      arguments->all_databases = true;
      break;
    case 'B':
      arguments->databases = true;
      break;
    // dump format option
    case 's':
      arguments->schemaonly = true;
      break;
    case 'M':
      arguments->with_property = true;
      break;
    case 'S':
      // parse time here.
      arguments->start_time = atol(arg);
      break;
    case 'E':
      arguments->end_time = atol(arg);
      break;
    case 'N':
      arguments->data_batch = atoi(arg);
      break;
    case 'L': 
    {
      int32_t len = atoi(arg);
      if (len > TSDB_MAX_ALLOWED_SQL_LEN) {
        len = TSDB_MAX_ALLOWED_SQL_LEN;
      } else if (len < TSDB_MAX_SQL_LEN) {
        len = TSDB_MAX_SQL_LEN;
      } 
      arguments->max_sql_len = len;
      break;
    }  
    case 't':
      arguments->table_batch = atoi(arg);
      break;
    case 'T':
      arguments->thread_num = atoi(arg);
      break;
    case OPT_ABORT:
      arguments->abort = 1;
      break;
    case ARGP_KEY_ARG:
      arguments->arg_list     = &state->argv[state->next - 1];
      arguments->arg_list_len = state->argc - state->next + 1;
      state->next             = state->argc;
      break;

    default:
      return ARGP_ERR_UNKNOWN;
  }
  return 0;
}

/* Our argp parser. */
static struct argp argp = {options, parse_opt, args_doc, doc};

int taosDumpOut(struct arguments *arguments);
int taosDumpIn(struct arguments *arguments);
void taosDumpCreateDbClause(SDbInfo *dbInfo, bool isDumpProperty, FILE *fp);
int taosDumpDb(SDbInfo *dbInfo, struct arguments *arguments, FILE *fp, TAOS *taosCon);
int32_t taosDumpStable(char *table, FILE *fp, TAOS* taosCon);
void taosDumpCreateTableClause(STableDef *tableDes, int numOfCols, FILE *fp);
void taosDumpCreateMTableClause(STableDef *tableDes, char *metric, int numOfCols, FILE *fp);
int32_t taosDumpTable(char *table, char *metric, struct arguments *arguments, FILE *fp, TAOS* taosCon);
int taosDumpTableData(FILE *fp, char *tbname, struct arguments *arguments, TAOS* taosCon);
int taosCheckParam(struct arguments *arguments);
void taosFreeDbInfos();
static void taosStartDumpOutWorkThreads(struct arguments* args, int32_t  numOfThread, char *dbName);

struct arguments tsArguments = {
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
  NULL,
  // dump unit option
  false, 
  false,
  // dump format option
  false, 
  false, 
  0, 
  INT64_MAX, 
  1,
  TSDB_MAX_SQL_LEN,
  1,
  false,
  // other options
  5,
  0,
  NULL, 
  0, 
  false
};

int queryDB(TAOS *taos, char *command) {
  TAOS_RES *pSql = NULL;
  int32_t   code = -1;
    
  pSql = taos_query(taos, command);
  code = taos_errno(pSql);
  if (code) {
    fprintf(stderr, "sql error: %s, reason:%s\n", command, taos_errstr(pSql));
  } 
  taos_free_result(pSql);
  return code;
}

int main(int argc, char *argv[]) {

  /* Parse our arguments; every option seen by parse_opt will be
     reflected in arguments. */
  argp_parse(&argp, argc, argv, 0, 0, &tsArguments);

  if (tsArguments.abort) {
    #ifndef _ALPINE
      error(10, 0, "ABORTED");
    #else
      abort();
    #endif
  }

  printf("====== arguments config ======\n");
  {
    printf("host: %s\n", tsArguments.host);
    printf("user: %s\n", tsArguments.user);
    printf("password: %s\n", tsArguments.password);
    printf("port: %u\n", tsArguments.port);
    printf("cversion: %s\n", tsArguments.cversion);    
    printf("mysqlFlag: %d\n", tsArguments.mysqlFlag);    
    printf("outpath: %s\n", tsArguments.outpath);
    printf("inpath: %s\n", tsArguments.inpath);
    printf("encode: %s\n", tsArguments.encode);
    printf("all_databases: %d\n", tsArguments.all_databases);
    printf("databases: %d\n", tsArguments.databases);
    printf("schemaonly: %d\n", tsArguments.schemaonly);
    printf("with_property: %d\n", tsArguments.with_property);
    printf("start_time: %" PRId64 "\n", tsArguments.start_time);
    printf("end_time: %" PRId64 "\n", tsArguments.end_time);
    printf("data_batch: %d\n", tsArguments.data_batch);
    printf("max_sql_len: %d\n", tsArguments.max_sql_len);
    printf("table_batch: %d\n", tsArguments.table_batch);
    printf("thread_num: %d\n", tsArguments.thread_num);    
    printf("allow_sys: %d\n", tsArguments.allow_sys);
    printf("abort: %d\n", tsArguments.abort);
    printf("isDumpIn: %d\n", tsArguments.isDumpIn);
    printf("arg_list_len: %d\n", tsArguments.arg_list_len);

    for (int32_t i = 0; i < tsArguments.arg_list_len; i++) {
      printf("arg_list[%d]: %s\n", i, tsArguments.arg_list[i]);
    }
  }  
  printf("==============================\n");

  if (tsArguments.cversion[0] != 0){
    tstrncpy(version, tsArguments.cversion, 11);
  }

  if (taosCheckParam(&tsArguments) < 0) {
    exit(EXIT_FAILURE);
  }

  if (tsArguments.isDumpIn) {
    if (taosDumpIn(&tsArguments) < 0) return -1;
  } else {
    if (taosDumpOut(&tsArguments) < 0) return -1;
  }

  return 0;
}

void taosFreeDbInfos() {
  if (dbInfos == NULL) return;
  for (int i = 0; i < 128; i++) tfree(dbInfos[i]);
  tfree(dbInfos);
}

// check table is normal table or super table
int taosGetTableRecordInfo(char *table, STableRecordInfo *pTableRecordInfo, TAOS *taosCon) {
  TAOS_ROW row = NULL;
  bool isSet = false;
  TAOS_RES *result     = NULL;

  memset(pTableRecordInfo, 0, sizeof(STableRecordInfo));

  char* tempCommand = (char *)malloc(COMMAND_SIZE);
  if (tempCommand == NULL) {
    fprintf(stderr, "failed to allocate memory\n");
    return -1;
  }

  sprintf(tempCommand, "show tables like %s", table);
  
  result = taos_query(taosCon, tempCommand);  
  int32_t code = taos_errno(result);
  
  if (code != 0) {
    fprintf(stderr, "failed to run command %s\n", tempCommand);
    free(tempCommand);
    taos_free_result(result);
    return -1;
  }

  TAOS_FIELD *fields = taos_fetch_fields(result);

  while ((row = taos_fetch_row(result)) != NULL) {
    isSet = true;
    pTableRecordInfo->isMetric = false;
    strncpy(pTableRecordInfo->tableRecord.name, (char *)row[TSDB_SHOW_TABLES_NAME_INDEX],
            fields[TSDB_SHOW_TABLES_NAME_INDEX].bytes);
    strncpy(pTableRecordInfo->tableRecord.metric, (char *)row[TSDB_SHOW_TABLES_METRIC_INDEX],
            fields[TSDB_SHOW_TABLES_METRIC_INDEX].bytes);
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
    fprintf(stderr, "failed to run command %s\n", tempCommand);
    free(tempCommand);
    taos_free_result(result);
    return -1;
  }

  while ((row = taos_fetch_row(result)) != NULL) {
    isSet = true;
    pTableRecordInfo->isMetric = true;
    tstrncpy(pTableRecordInfo->tableRecord.metric, table, TSDB_TABLE_NAME_LEN);
    break;
  }

  taos_free_result(result);
  result = NULL;

  if (isSet) {
    free(tempCommand);
    return 0;
  }
  fprintf(stderr, "invalid table/metric %s\n", table);
  free(tempCommand);
  return -1;
}


int32_t taosSaveAllNormalTableToTempFile(TAOS *taosCon, char*meter, char* metric, int* fd) {
  STableRecord tableRecord;

  if (-1 == *fd) {
    *fd = open(".tables.tmp.0", O_RDWR | O_CREAT, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH);
    if (*fd == -1) {
      fprintf(stderr, "failed to open temp file: .tables.tmp.0\n");
      return -1;
    }
  }
  
  memset(&tableRecord, 0, sizeof(STableRecord));
  tstrncpy(tableRecord.name, meter, TSDB_TABLE_NAME_LEN);
  tstrncpy(tableRecord.metric, metric, TSDB_TABLE_NAME_LEN);

  taosWrite(*fd, &tableRecord, sizeof(STableRecord));
  return 0;
}


int32_t taosSaveTableOfMetricToTempFile(TAOS *taosCon, char* metric, struct arguments *arguments, int32_t*  totalNumOfThread) {
  TAOS_ROW row;
  int fd = -1;
  STableRecord tableRecord;

  char* tmpCommand = (char *)malloc(COMMAND_SIZE);
  if (tmpCommand == NULL) {
    fprintf(stderr, "failed to allocate memory\n");
    return -1;
  }

  sprintf(tmpCommand, "select tbname from %s", metric);
  
  TAOS_RES *result = taos_query(taosCon, tmpCommand);  
  int32_t code = taos_errno(result);
  if (code != 0) {
    fprintf(stderr, "failed to run command %s\n", tmpCommand);
    free(tmpCommand);
    taos_free_result(result);
    return -1;
  }

  TAOS_FIELD *fields = taos_fetch_fields(result);

  int32_t  numOfTable  = 0;
  int32_t  numOfThread = *totalNumOfThread;
  char     tmpFileName[TSDB_FILENAME_LEN + 1];
  while ((row = taos_fetch_row(result)) != NULL) {
    if (0 == numOfTable) {
      memset(tmpFileName, 0, TSDB_FILENAME_LEN);
      sprintf(tmpFileName, ".tables.tmp.%d", numOfThread);
      fd = open(tmpFileName, O_RDWR | O_CREAT, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH);
      if (fd == -1) {
        fprintf(stderr, "failed to open temp file: %s\n", tmpFileName);
        taos_free_result(result);
        for (int32_t loopCnt = 0; loopCnt < numOfThread; loopCnt++) {
          sprintf(tmpFileName, ".tables.tmp.%d", loopCnt);
          (void)remove(tmpFileName);
        }
        free(tmpCommand);
        return -1;
      }
      
      numOfThread++;
    }
  
    memset(&tableRecord, 0, sizeof(STableRecord));
    tstrncpy(tableRecord.name, (char *)row[0], fields[0].bytes);
    tstrncpy(tableRecord.metric, metric, TSDB_TABLE_NAME_LEN);

    taosWrite(fd, &tableRecord, sizeof(STableRecord));

    numOfTable++;

    if (numOfTable >= arguments->table_batch) {
      numOfTable = 0;
      close(fd);
      fd = -1;
    }
  }
  
  if (fd >= 0) {
    close(fd);
    fd = -1;
  }
  
  taos_free_result(result);

  *totalNumOfThread = numOfThread;
 
  free(tmpCommand);

  return 0;
}

int taosDumpOut(struct arguments *arguments) {
  TAOS     *taos       = NULL;
  TAOS_RES *result     = NULL;
  char     *command    = NULL;

  TAOS_ROW row;
  FILE *fp = NULL;
  int32_t count = 0;
  STableRecordInfo tableRecordInfo;

  char tmpBuf[TSDB_FILENAME_LEN+9] = {0};
  if (arguments->outpath[0] != 0) {
      sprintf(tmpBuf, "%s/dbs.sql", arguments->outpath);
  } else {
    sprintf(tmpBuf, "dbs.sql");
  }
  
  fp = fopen(tmpBuf, "w");
  if (fp == NULL) {
    fprintf(stderr, "failed to open file %s\n", tmpBuf);
    return -1;
  }

  dbInfos = (SDbInfo **)calloc(128, sizeof(SDbInfo *));
  if (dbInfos == NULL) {
    fprintf(stderr, "failed to allocate memory\n");
    goto _exit_failure;
  }

  command = (char *)malloc(COMMAND_SIZE);
  if (command == NULL) {
    fprintf(stderr, "failed to allocate memory\n");
    goto _exit_failure;
  }

  /* Connect to server */
  taos = taos_connect(arguments->host, arguments->user, arguments->password, NULL, arguments->port);
  if (taos == NULL) {
    fprintf(stderr, "failed to connect to TDengine server\n");
    goto _exit_failure;
  }

  /* --------------------------------- Main Code -------------------------------- */
  /* if (arguments->databases || arguments->all_databases) { // dump part of databases or all databases */
  /*  */
  taosDumpCharset(fp);

  sprintf(command, "show databases");
  result = taos_query(taos, command);  
  int32_t code = taos_errno(result);
  
  if (code != 0) {
    fprintf(stderr, "failed to run command: %s, reason: %s\n", command, taos_errstr(taos));
    goto _exit_failure;
  }

  TAOS_FIELD *fields = taos_fetch_fields(result);

  while ((row = taos_fetch_row(result)) != NULL) {
    // sys database name : 'log', but subsequent version changed to 'log'
    if (strncasecmp(row[TSDB_SHOW_DB_NAME_INDEX], "log", fields[TSDB_SHOW_DB_NAME_INDEX].bytes) == 0 &&
        (!arguments->allow_sys))
      continue;

    if (arguments->databases) {  // input multi dbs
      for (int i = 0; arguments->arg_list[i]; i++) {
        if (strncasecmp(arguments->arg_list[i], (char *)row[TSDB_SHOW_DB_NAME_INDEX],
                        fields[TSDB_SHOW_DB_NAME_INDEX].bytes) == 0)
          goto _dump_db_point;
      }
      continue;
    } else if (!arguments->all_databases) {  // only input one db
      if (strncasecmp(arguments->arg_list[0], (char *)row[TSDB_SHOW_DB_NAME_INDEX],
                      fields[TSDB_SHOW_DB_NAME_INDEX].bytes) == 0)
        goto _dump_db_point;
      else
        continue;
    }

  _dump_db_point:

    dbInfos[count] = (SDbInfo *)calloc(1, sizeof(SDbInfo));
    if (dbInfos[count] == NULL) {
      fprintf(stderr, "failed to allocate memory\n");
      goto _exit_failure;
    }

    strncpy(dbInfos[count]->name, (char *)row[TSDB_SHOW_DB_NAME_INDEX], fields[TSDB_SHOW_DB_NAME_INDEX].bytes);
#if 0
    if (arguments->with_property) {
      dbInfos[count]->tables = *((int32_t *)row[TSDB_SHOW_DB_NTABLES_INDEX]);
      dbInfos[count]->vgroups = *((int32_t *)row[TSDB_SHOW_DB_VGROUPS_INDEX]);  
      dbInfos[count]->replications = *((int16_t *)row[TSDB_SHOW_DB_REPLICA_INDEX]);
      dbInfos[count]->quorum = *((int16_t *)row[TSDB_SHOW_DB_QUORUM_INDEX]);
      dbInfos[count]->daysPerFile = *((int16_t *)row[TSDB_SHOW_DB_DAYS_INDEX]);  
      dbInfos[count]->daysToKeep = *((int16_t *)row[TSDB_SHOW_DB_KEEP_INDEX]);
      dbInfos[count]->daysToKeep1;
      dbInfos[count]->daysToKeep2;
      dbInfos[count]->cacheBlockSize = *((int32_t *)row[TSDB_SHOW_DB_CACHE_INDEX]);
      dbInfos[count]->totalBlocks = *((int32_t *)row[TSDB_SHOW_DB_BLOCKS_INDEX]);
      dbInfos[count]->minRowsPerFileBlock = *((int32_t *)row[TSDB_SHOW_DB_MINROWS_INDEX]);
      dbInfos[count]->maxRowsPerFileBlock = *((int32_t *)row[TSDB_SHOW_DB_MAXROWS_INDEX]);
      dbInfos[count]->walLevel = *((int8_t *)row[TSDB_SHOW_DB_WALLEVEL_INDEX]);
      dbInfos[count]->fsyncPeriod = *((int32_t *)row[TSDB_SHOW_DB_FSYNC_INDEX]);
      dbInfos[count]->compression = (int8_t)(*((int8_t *)row[TSDB_SHOW_DB_COMP_INDEX]));
      dbInfos[count]->precision = *((int8_t *)row[TSDB_SHOW_DB_PRECISION_INDEX]);
      dbInfos[count]->update = *((int8_t *)row[TSDB_SHOW_DB_UPDATE_INDEX]);
    }
#endif
    count++;

    if (arguments->databases) {
      if (count > arguments->arg_list_len) break;

    } else if (!arguments->all_databases) {
      if (count >= 1) break;
    }
  }

  if (count == 0) {
    fprintf(stderr, "No databases valid to dump\n");
    goto _exit_failure;
  }

  if (arguments->databases || arguments->all_databases) { // case: taosdump --databases dbx dby ...   OR  taosdump --all-databases
    for (int i = 0; i < count; i++) {
      taosDumpDb(dbInfos[i], arguments, fp, taos);
    }
  } else {
    if (arguments->arg_list_len == 1) {             // case: taosdump <db>
      taosDumpDb(dbInfos[0], arguments, fp, taos);
    } else {                                        // case: taosdump <db> tablex tabley ...
      taosDumpCreateDbClause(dbInfos[0], arguments->with_property, fp);

      sprintf(command, "use %s", dbInfos[0]->name);
      
      result = taos_query(taos, command);  
      int32_t code = taos_errno(result);
      if (code != 0) {
        fprintf(stderr, "invalid database %s\n", dbInfos[0]->name);
        goto _exit_failure;
      }

      fprintf(fp, "USE %s;\n\n", dbInfos[0]->name);

      int32_t totalNumOfThread = 1;  // 0: all normal talbe into .tables.tmp.0
      int  normalTblFd = -1;
      int32_t retCode;
      for (int i = 1; arguments->arg_list[i]; i++) {
        if (taosGetTableRecordInfo(arguments->arg_list[i], &tableRecordInfo, taos) < 0) {
          fprintf(stderr, "input the invalide table %s\n", arguments->arg_list[i]);
          continue;
        }

        if (tableRecordInfo.isMetric) {  // dump all table of this metric
          (void)taosDumpStable(tableRecordInfo.tableRecord.metric, fp, taos);
          retCode = taosSaveTableOfMetricToTempFile(taos, tableRecordInfo.tableRecord.metric, arguments, &totalNumOfThread);
        } else {
          if (tableRecordInfo.tableRecord.metric[0] != '\0') {  // dump this sub table and it's metric
            (void)taosDumpStable(tableRecordInfo.tableRecord.metric, fp, taos);          
          }
          retCode = taosSaveAllNormalTableToTempFile(taos, tableRecordInfo.tableRecord.name, tableRecordInfo.tableRecord.metric, &normalTblFd);
        }

        if (retCode < 0) {
          if (-1 != normalTblFd){
            taosClose(normalTblFd);
          }
          goto _clean_tmp_file;
        }
      }

      if (-1 != normalTblFd){
        taosClose(normalTblFd);
      }

      // start multi threads to dumpout
      taosStartDumpOutWorkThreads(arguments, totalNumOfThread, dbInfos[0]->name);

      char tmpFileName[TSDB_FILENAME_LEN + 1];
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
  fprintf(stderr, "dump out rows: %" PRId64 "\n", totalDumpOutRows);
  return 0;

_exit_failure:
  fclose(fp);
  taos_close(taos);
  taos_free_result(result);
  tfree(command);
  taosFreeDbInfos();
  fprintf(stderr, "dump out rows: %" PRId64 "\n", totalDumpOutRows);
  return -1;
}

int taosGetTableDes(char *table, STableDef *tableDes, TAOS* taosCon, bool isSuperTable) {
  TAOS_ROW row = NULL;
  TAOS_RES *tmpResult = NULL;
  int count = 0;

  char* tempCommand = (char *)malloc(COMMAND_SIZE);
  if (tempCommand == NULL) {
    fprintf(stderr, "failed to allocate memory\n");
    return -1;
  }

  char* tbuf = (char *)malloc(COMMAND_SIZE);
  if (tbuf == NULL) {
    fprintf(stderr, "failed to allocate memory\n");
    free(tempCommand);
    return -1;
  }

  sprintf(tempCommand, "describe %s", table);
  
  tmpResult = taos_query(taosCon, tempCommand);  
  int32_t code = taos_errno(tmpResult);
  if (code != 0) {
    fprintf(stderr, "failed to run command %s\n", tempCommand);
    free(tempCommand);
    free(tbuf);
    taos_free_result(tmpResult);
    return -1;
  }

  TAOS_FIELD *fields = taos_fetch_fields(tmpResult);

  tstrncpy(tableDes->name, table, TSDB_COL_NAME_LEN);

  while ((row = taos_fetch_row(tmpResult)) != NULL) {
    strncpy(tableDes->cols[count].field, (char *)row[TSDB_DESCRIBE_METRIC_FIELD_INDEX],
            fields[TSDB_DESCRIBE_METRIC_FIELD_INDEX].bytes);
    strncpy(tableDes->cols[count].type, (char *)row[TSDB_DESCRIBE_METRIC_TYPE_INDEX],
            fields[TSDB_DESCRIBE_METRIC_TYPE_INDEX].bytes);
    tableDes->cols[count].length = *((int *)row[TSDB_DESCRIBE_METRIC_LENGTH_INDEX]);
    strncpy(tableDes->cols[count].note, (char *)row[TSDB_DESCRIBE_METRIC_NOTE_INDEX],
            fields[TSDB_DESCRIBE_METRIC_NOTE_INDEX].bytes);

    count++;
  }

  taos_free_result(tmpResult);
  tmpResult = NULL;

  if (isSuperTable) {
    free(tempCommand);
    free(tbuf);
    return count;
  }
  
  // if chidl-table have tag, using  select tagName from table to get tagValue
  for (int i = 0 ; i < count; i++) {
    if (strcmp(tableDes->cols[i].note, "TAG") != 0) continue;


    sprintf(tempCommand, "select %s from %s", tableDes->cols[i].field, table);
    
    tmpResult = taos_query(taosCon, tempCommand);  
    code = taos_errno(tmpResult);
    if (code != 0) {
      fprintf(stderr, "failed to run command %s\n", tempCommand);
      free(tempCommand);
      free(tbuf);
      taos_free_result(tmpResult);
      return -1;
    }
    
    fields = taos_fetch_fields(tmpResult); 

    row = taos_fetch_row(tmpResult);
    if (NULL == row) {
      fprintf(stderr, " fetch failed to run command %s\n", tempCommand);
      free(tempCommand);
      free(tbuf);
      taos_free_result(tmpResult);
      return -1;
    }

    if (row[0] == NULL) {
      sprintf(tableDes->cols[i].note, "%s", "NULL");
      taos_free_result(tmpResult);
      tmpResult = NULL;
      continue;
    }
    
    int32_t* length = taos_fetch_lengths(tmpResult);

    //int32_t* length = taos_fetch_lengths(tmpResult);
    switch (fields[0].type) {
      case TSDB_DATA_TYPE_BOOL:
        sprintf(tableDes->cols[i].note, "%d", ((((int32_t)(*((char *)row[0]))) == 1) ? 1 : 0));
        break;
      case TSDB_DATA_TYPE_TINYINT:
        sprintf(tableDes->cols[i].note, "%d", *((int8_t *)row[0]));
        break;
      case TSDB_DATA_TYPE_SMALLINT:
        sprintf(tableDes->cols[i].note, "%d", *((int16_t *)row[0]));
        break;
      case TSDB_DATA_TYPE_INT:
        sprintf(tableDes->cols[i].note, "%d", *((int32_t *)row[0]));
        break;
      case TSDB_DATA_TYPE_BIGINT:
        sprintf(tableDes->cols[i].note, "%" PRId64 "", *((int64_t *)row[0]));
        break;
      case TSDB_DATA_TYPE_FLOAT:
        sprintf(tableDes->cols[i].note, "%f", GET_FLOAT_VAL(row[0]));
        break;
      case TSDB_DATA_TYPE_DOUBLE:
        sprintf(tableDes->cols[i].note, "%f", GET_DOUBLE_VAL(row[0]));
        break;
      case TSDB_DATA_TYPE_BINARY:
        memset(tableDes->cols[i].note, 0, sizeof(tableDes->cols[i].note));
        tableDes->cols[i].note[0] = '\'';
        converStringToReadable((char *)row[0], length[0], tbuf, COMMAND_SIZE);
        char* pstr = stpcpy(&(tableDes->cols[i].note[1]), tbuf);
        *(pstr++) = '\'';
        break;
      case TSDB_DATA_TYPE_NCHAR:
        memset(tableDes->cols[i].note, 0, sizeof(tableDes->cols[i].note));
        convertNCharToReadable((char *)row[0], length[0], tbuf, COMMAND_SIZE);
        sprintf(tableDes->cols[i].note, "\'%s\'", tbuf);
        break;
      case TSDB_DATA_TYPE_TIMESTAMP:
        sprintf(tableDes->cols[i].note, "%" PRId64 "", *(int64_t *)row[0]);
        #if 0
        if (!arguments->mysqlFlag) {
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
  
    taos_free_result(tmpResult);
    tmpResult = NULL;    
  }

  free(tempCommand);
  free(tbuf);

  return count;
}

int32_t taosDumpTable(char *table, char *metric, struct arguments *arguments, FILE *fp, TAOS* taosCon) {
  int count = 0;

  STableDef *tableDes = (STableDef *)calloc(1, sizeof(STableDef) + sizeof(SColDes) * TSDB_MAX_COLUMNS);

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

    count = taosGetTableDes(table, tableDes, taosCon, false);

    if (count < 0) {
      free(tableDes);
      return -1;
    }

    // create child-table using super-table
    taosDumpCreateMTableClause(tableDes, metric, count, fp);

  } else {  // dump table definition
    count = taosGetTableDes(table, tableDes, taosCon, false);

    if (count < 0) {
      free(tableDes);
      return -1;
    }

    // create normal-table or super-table
    taosDumpCreateTableClause(tableDes, count, fp);
  }

  free(tableDes);

  return taosDumpTableData(fp, table, arguments, taosCon);
}

void taosDumpCreateDbClause(SDbInfo *dbInfo, bool isDumpProperty, FILE *fp) {

  char* tmpCommand = (char *)malloc(COMMAND_SIZE);
  if (tmpCommand == NULL) {
    fprintf(stderr, "failed to allocate memory\n");
    return;
  }

  char *pstr = tmpCommand;

  pstr += sprintf(pstr, "CREATE DATABASE IF NOT EXISTS %s", dbInfo->name);
  if (isDumpProperty) {
    #if 0
    pstr += sprintf(pstr,
        "TABLES %d vgroups %d REPLICA %d quorum %d DAYS %d KEEP %d CACHE %d BLOCKS %d MINROWS %d MAXROWS %d WALLEVEL %d FYNC %d COMP %d PRECISION %s UPDATE %d",
        dbInfo->tables, dbInfo->vgroups, dbInfo->replications, dbInfo->quorum, dbInfo->daysPerFile, dbInfo->daysToKeep, dbInfo->cacheBlockSize,
        dbInfo->totalBlocks, dbInfo->minRowsPerFileBlock, dbInfo->maxRowsPerFileBlock,  dbInfo->walLevel, dbInfo->fsyncPeriod, dbInfo->compression, 
        dbInfo->precision, dbInfo->update);
    #endif
  }

  pstr += sprintf(pstr, ";");

  fprintf(fp, "%s\n\n", tmpCommand);
  free(tmpCommand);
}

void* taosDumpOutWorkThreadFp(void *arg)
{
  SThreadParaObj *pThread = (SThreadParaObj*)arg;
  STableRecord    tableRecord;
  int fd;  
  
  char tmpFileName[TSDB_FILENAME_LEN*4] = {0};
  sprintf(tmpFileName, ".tables.tmp.%d", pThread->threadIndex);
  fd = open(tmpFileName, O_RDWR | O_CREAT, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH);
  if (fd == -1) {
    fprintf(stderr, "taosDumpTableFp() failed to open temp file: %s\n", tmpFileName);
    return NULL;
  }

  FILE *fp = NULL;
  memset(tmpFileName, 0, TSDB_FILENAME_LEN + 128);
  
  if (tsArguments.outpath[0] != 0) {
    sprintf(tmpFileName, "%s/%s.tables.%d.sql", tsArguments.outpath, pThread->dbName, pThread->threadIndex);
  } else {
    sprintf(tmpFileName, "%s.tables.%d.sql", pThread->dbName, pThread->threadIndex);
  }
  
  fp = fopen(tmpFileName, "w");
  if (fp == NULL) {
    fprintf(stderr, "failed to open file %s\n", tmpFileName);
    close(fd);
    return NULL;
  }

  memset(tmpFileName, 0, TSDB_FILENAME_LEN);
  sprintf(tmpFileName, "use %s", pThread->dbName);
  
  TAOS_RES* tmpResult = taos_query(pThread->taosCon, tmpFileName);  
  int32_t code = taos_errno(tmpResult);
  if (code != 0) {
    fprintf(stderr, "invalid database %s\n", pThread->dbName);
    taos_free_result(tmpResult);
    fclose(fp);  
    close(fd);
    return NULL;
  }

  fprintf(fp, "USE %s;\n\n", pThread->dbName);
  while (1) {
    ssize_t readLen = read(fd, &tableRecord, sizeof(STableRecord));
    if (readLen <= 0) break;
    taosDumpTable(tableRecord.name, tableRecord.metric, &tsArguments, fp, pThread->taosCon);
  }

  taos_free_result(tmpResult);
  close(fd);
  fclose(fp);  

  return NULL;
}

static void taosStartDumpOutWorkThreads(struct arguments* args, int32_t  numOfThread, char *dbName)
{
  pthread_attr_t thattr;
  SThreadParaObj *threadObj = (SThreadParaObj *)calloc(numOfThread, sizeof(SThreadParaObj));
  for (int t = 0; t < numOfThread; ++t) {
    SThreadParaObj *pThread = threadObj + t;
    pThread->threadIndex = t;
    pThread->totalThreads = numOfThread;
    tstrncpy(pThread->dbName, dbName, TSDB_TABLE_NAME_LEN);
    pThread->taosCon = taos_connect(args->host, args->user, args->password, NULL, args->port);
                                
    if (pThread->taosCon == NULL) {
      fprintf(stderr, "ERROR: thread:%d failed connect to TDengine, error:%s\n", pThread->threadIndex, taos_errstr(pThread->taosCon));
      exit(0);
    }

    pthread_attr_init(&thattr);
    pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);

    if (pthread_create(&(pThread->threadID), &thattr, taosDumpOutWorkThreadFp, (void*)pThread) != 0) {
      fprintf(stderr, "ERROR: thread:%d failed to start\n", pThread->threadIndex);
      exit(0);
    }
  }

  for (int32_t t = 0; t < numOfThread; ++t) {
    pthread_join(threadObj[t].threadID, NULL);
  }

  for (int32_t t = 0; t < numOfThread; ++t) {
    taos_close(threadObj[t].taosCon);
  }
  free(threadObj);
}



int32_t taosDumpStable(char *table, FILE *fp, TAOS* taosCon) {
  int count = 0;

  STableDef *tableDes = (STableDef *)calloc(1, sizeof(STableDef) + sizeof(SColDes) * TSDB_MAX_COLUMNS);
  if (NULL == tableDes) {
    fprintf(stderr, "failed to allocate memory\n");
    exit(-1);
  }

  count = taosGetTableDes(table, tableDes, taosCon, true);

  if (count < 0) {
    free(tableDes);
    fprintf(stderr, "failed to get stable schema\n");
    exit(-1);
  }

  taosDumpCreateTableClause(tableDes, count, fp);

  free(tableDes);
  return 0;
}


int32_t taosDumpCreateSuperTableClause(TAOS* taosCon, char* dbName, FILE *fp) 
{
  TAOS_ROW row;
  int fd = -1;
  STableRecord tableRecord;

  char* tmpCommand = (char *)malloc(COMMAND_SIZE);
  if (tmpCommand == NULL) {
    fprintf(stderr, "failed to allocate memory\n");
    exit(-1);
  }

  sprintf(tmpCommand, "use %s", dbName);
  
  TAOS_RES* tmpResult = taos_query(taosCon, tmpCommand);  
  int32_t code = taos_errno(tmpResult);
  if (code != 0) {
    fprintf(stderr, "invalid database %s, error: %s\n", dbName, taos_errstr(taosCon));
    free(tmpCommand);
    taos_free_result(tmpResult);
    exit(-1);
  }
  
  taos_free_result(tmpResult);

  sprintf(tmpCommand, "show stables");
  
  tmpResult = taos_query(taosCon, tmpCommand);  
  code = taos_errno(tmpResult);
  if (code != 0) {
    fprintf(stderr, "failed to run command %s, error: %s\n", tmpCommand, taos_errstr(taosCon));
    free(tmpCommand);
    taos_free_result(tmpResult);
    exit(-1);
  }

  TAOS_FIELD *fields = taos_fetch_fields(tmpResult);

  char     tmpFileName[TSDB_FILENAME_LEN + 1];
  memset(tmpFileName, 0, TSDB_FILENAME_LEN);
  sprintf(tmpFileName, ".stables.tmp");
  fd = open(tmpFileName, O_RDWR | O_CREAT, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH);
  if (fd == -1) {
    fprintf(stderr, "failed to open temp file: %s\n", tmpFileName);
    taos_free_result(tmpResult);
    free(tmpCommand);
    (void)remove(".stables.tmp");
    exit(-1);
  }
  
  while ((row = taos_fetch_row(tmpResult)) != NULL) {  
    memset(&tableRecord, 0, sizeof(STableRecord));
    strncpy(tableRecord.name, (char *)row[TSDB_SHOW_TABLES_NAME_INDEX], fields[TSDB_SHOW_TABLES_NAME_INDEX].bytes);
    taosWrite(fd, &tableRecord, sizeof(STableRecord));
  }  
  
  taos_free_result(tmpResult);
  (void)lseek(fd, 0, SEEK_SET);

  while (1) {
    ssize_t readLen = read(fd, &tableRecord, sizeof(STableRecord));
    if (readLen <= 0) break;
    
    (void)taosDumpStable(tableRecord.name, fp, taosCon);
  }

  close(fd);
  (void)remove(".stables.tmp");
  
  free(tmpCommand);
  return 0;  
}


int taosDumpDb(SDbInfo *dbInfo, struct arguments *arguments, FILE *fp, TAOS *taosCon) {
  TAOS_ROW row;
  int fd = -1;
  STableRecord tableRecord;

  taosDumpCreateDbClause(dbInfo, arguments->with_property, fp);

  char* tmpCommand = (char *)malloc(COMMAND_SIZE);
  if (tmpCommand == NULL) {
    fprintf(stderr, "failed to allocate memory\n");
    return -1;
  }

  sprintf(tmpCommand, "use %s", dbInfo->name);
  
  TAOS_RES* tmpResult = taos_query(taosCon, tmpCommand);  
  int32_t code = taos_errno(tmpResult);
  if (code != 0) {
    fprintf(stderr, "invalid database %s\n", dbInfo->name);
    free(tmpCommand);
    taos_free_result(tmpResult);
    return -1;
  }
  taos_free_result(tmpResult);

  fprintf(fp, "USE %s;\n\n", dbInfo->name);
  
  (void)taosDumpCreateSuperTableClause(taosCon, dbInfo->name, fp);

  sprintf(tmpCommand, "show tables");
  
  tmpResult = taos_query(taosCon, tmpCommand);  
  code = taos_errno(tmpResult);
  if (code != 0) {
    fprintf(stderr, "failed to run command %s\n", tmpCommand);
    free(tmpCommand);
    taos_free_result(tmpResult);
    return -1;
  }

  TAOS_FIELD *fields = taos_fetch_fields(tmpResult);

  int32_t  numOfTable  = 0;
  int32_t  numOfThread = 0;
  char     tmpFileName[TSDB_FILENAME_LEN + 1];
  while ((row = taos_fetch_row(tmpResult)) != NULL) {
    if (0 == numOfTable) {
      memset(tmpFileName, 0, TSDB_FILENAME_LEN);
      sprintf(tmpFileName, ".tables.tmp.%d", numOfThread);
      fd = open(tmpFileName, O_RDWR | O_CREAT, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH);
      if (fd == -1) {
        fprintf(stderr, "failed to open temp file: %s\n", tmpFileName);
        taos_free_result(tmpResult);
        for (int32_t loopCnt = 0; loopCnt < numOfThread; loopCnt++) {
          sprintf(tmpFileName, ".tables.tmp.%d", loopCnt);
          (void)remove(tmpFileName);
        }
        free(tmpCommand);
        return -1;
      }

      numOfThread++;
    }
  
    memset(&tableRecord, 0, sizeof(STableRecord));
    tstrncpy(tableRecord.name, (char *)row[TSDB_SHOW_TABLES_NAME_INDEX], fields[TSDB_SHOW_TABLES_NAME_INDEX].bytes);
    tstrncpy(tableRecord.metric, (char *)row[TSDB_SHOW_TABLES_METRIC_INDEX], fields[TSDB_SHOW_TABLES_METRIC_INDEX].bytes);

    taosWrite(fd, &tableRecord, sizeof(STableRecord));

    numOfTable++;

    if (numOfTable >= arguments->table_batch) {
      numOfTable = 0;
      close(fd);
      fd = -1;
    }
  }

  if (fd >= 0) {
    close(fd);
    fd = -1;
  }
  
  taos_free_result(tmpResult);

  // start multi threads to dumpout
  taosStartDumpOutWorkThreads(arguments, numOfThread, dbInfo->name);
  for (int loopCnt = 0; loopCnt < numOfThread; loopCnt++) {
    sprintf(tmpFileName, ".tables.tmp.%d", loopCnt);
    (void)remove(tmpFileName);
  }
  
  free(tmpCommand);

  return 0;
}

void taosDumpCreateTableClause(STableDef *tableDes, int numOfCols, FILE *fp) {
  int counter = 0;
  int count_temp = 0;

  char* tmpBuf = (char *)malloc(COMMAND_SIZE);
  if (tmpBuf == NULL) {
    fprintf(stderr, "failed to allocate memory\n");
    return;
  }

  char* pstr = tmpBuf;

  pstr += sprintf(tmpBuf, "CREATE TABLE IF NOT EXISTS %s", tableDes->name);

  for (; counter < numOfCols; counter++) {
    if (tableDes->cols[counter].note[0] != '\0') break;

    if (counter == 0) {
      pstr += sprintf(pstr, " (%s %s", tableDes->cols[counter].field, tableDes->cols[counter].type);
    } else {
      pstr += sprintf(pstr, ", %s %s", tableDes->cols[counter].field, tableDes->cols[counter].type);
    }

    if (strcasecmp(tableDes->cols[counter].type, "binary") == 0 ||
        strcasecmp(tableDes->cols[counter].type, "nchar") == 0) {
      pstr += sprintf(pstr, "(%d)", tableDes->cols[counter].length);
    }
  }

  count_temp = counter;

  for (; counter < numOfCols; counter++) {
    if (counter == count_temp) {
      pstr += sprintf(pstr, ") TAGS (%s %s", tableDes->cols[counter].field, tableDes->cols[counter].type);
    } else {
      pstr += sprintf(pstr, ", %s %s", tableDes->cols[counter].field, tableDes->cols[counter].type);
    }

    if (strcasecmp(tableDes->cols[counter].type, "binary") == 0 ||
        strcasecmp(tableDes->cols[counter].type, "nchar") == 0) {
      pstr += sprintf(pstr, "(%d)", tableDes->cols[counter].length);
    }
  }

  pstr += sprintf(pstr, ");");

  fprintf(fp, "%s\n", tmpBuf);

  free(tmpBuf);
}

void taosDumpCreateMTableClause(STableDef *tableDes, char *metric, int numOfCols, FILE *fp) {
  int counter = 0;
  int count_temp = 0;

  char* tmpBuf = (char *)malloc(COMMAND_SIZE);
  if (tmpBuf == NULL) {
    fprintf(stderr, "failed to allocate memory\n");
    return;
  }

  char *pstr = NULL;
  pstr = tmpBuf;

  pstr += sprintf(tmpBuf, "CREATE TABLE IF NOT EXISTS %s USING %s TAGS (", tableDes->name, metric);

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

int taosDumpTableData(FILE *fp, char *tbname, struct arguments *arguments, TAOS* taosCon) {
  /* char       temp[MAX_COMMAND_SIZE] = "\0"; */
  int64_t totalRows = 0;
  int count = 0;
  char *pstr = NULL;
  TAOS_ROW row = NULL;
  int numFields = 0;
  char *tbuf = NULL;

  char* tmpCommand = (char *)calloc(1, COMMAND_SIZE);
  if (tmpCommand == NULL) {
    fprintf(stderr, "failed to allocate memory\n");
    return -1;
  }

  int32_t  sql_buf_len = arguments->max_sql_len;
  char* tmpBuffer = (char *)calloc(1, sql_buf_len + 128);
  if (tmpBuffer == NULL) {
    fprintf(stderr, "failed to allocate memory\n");
    free(tmpCommand);
    return -1;
  }

  pstr = tmpBuffer;

  if (arguments->schemaonly) {
    free(tmpCommand);
    free(tmpBuffer);
    return 0;
  }
  
  sprintf(tmpCommand, 
          "select * from %s where _c0 >= %" PRId64 " and _c0 <= %" PRId64 " order by _c0 asc", 
          tbname, 
          arguments->start_time,
          arguments->end_time);
  
  TAOS_RES* tmpResult = taos_query(taosCon, tmpCommand);  
  int32_t code = taos_errno(tmpResult);
  if (code != 0) {
    fprintf(stderr, "failed to run command %s, reason: %s\n", tmpCommand, taos_errstr(taosCon));
    free(tmpCommand);
    free(tmpBuffer);
    taos_free_result(tmpResult);
    return -1;
  }

  numFields = taos_field_count(tmpResult);
  assert(numFields > 0);
  TAOS_FIELD *fields = taos_fetch_fields(tmpResult);
  tbuf = (char *)malloc(COMMAND_SIZE);
  if (tbuf == NULL) {
    fprintf(stderr, "No enough memory\n");
    free(tmpCommand);
    free(tmpBuffer);
    taos_free_result(tmpResult);
    return -1;
  }

  int rowFlag = 0;
  int32_t  curr_sqlstr_len = 0;
  int32_t  total_sqlstr_len = 0;
  count = 0;
  while ((row = taos_fetch_row(tmpResult)) != NULL) {
    pstr = tmpBuffer;
    curr_sqlstr_len = 0;
    
    int32_t* length = taos_fetch_lengths(tmpResult);   // act len

    if (count == 0) {
      total_sqlstr_len = 0;
      curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, "INSERT INTO %s VALUES (", tbname);
    } else {
      if (arguments->mysqlFlag) {
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
          curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, "%d", ((((int32_t)(*((char *)row[col]))) == 1) ? 1 : 0));
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
          curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, "%" PRId64 "", *((int64_t *)row[col]));
          break;
        case TSDB_DATA_TYPE_FLOAT:
          curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, "%f", GET_FLOAT_VAL(row[col]));
          break;
        case TSDB_DATA_TYPE_DOUBLE:
          curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, "%f", GET_DOUBLE_VAL(row[col]));
          break;
        case TSDB_DATA_TYPE_BINARY:
          //*(pstr++) = '\'';
          converStringToReadable((char *)row[col], length[col], tbuf, COMMAND_SIZE);
          //pstr = stpcpy(pstr, tbuf);
          //*(pstr++) = '\'';
          pstr += sprintf(pstr + curr_sqlstr_len, "\'%s\'", tbuf);          
          break;
        case TSDB_DATA_TYPE_NCHAR:
          convertNCharToReadable((char *)row[col], length[col], tbuf, COMMAND_SIZE);
          pstr += sprintf(pstr + curr_sqlstr_len, "\'%s\'", tbuf);
          break;
        case TSDB_DATA_TYPE_TIMESTAMP:
          if (!arguments->mysqlFlag) {
            curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, "%" PRId64 "", *(int64_t *)row[col]);
          } else {
            char buf[64] = "\0";
            int64_t ts = *((int64_t *)row[col]);
            time_t tt = (time_t)(ts / 1000);
            struct tm *ptm = localtime(&tt);
            strftime(buf, 64, "%y-%m-%d %H:%M:%S", ptm);
            curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, "\'%s.%03d\'", buf, (int)(ts % 1000));
          }
          break;
        default:
          break;
      }
    }

    curr_sqlstr_len += sprintf(pstr + curr_sqlstr_len, ") ");

    totalRows++;
    count++;
    fprintf(fp, "%s", tmpBuffer);

    total_sqlstr_len += curr_sqlstr_len;

    if ((count >= arguments->data_batch) || (sql_buf_len - total_sqlstr_len < TSDB_MAX_BYTES_PER_ROW)) {
      fprintf(fp, ";\n");
      count = 0;
    } //else {
      //fprintf(fp, "\\\n");
    //}
  }

  atomic_add_fetch_64(&totalDumpOutRows, totalRows);

  fprintf(fp, "\n");

  if (tbuf) {
    free(tbuf);
  }
  
  taos_free_result(tmpResult);
  tmpResult = NULL;
  free(tmpCommand);
  free(tmpBuffer);
  return 0;
}

int taosCheckParam(struct arguments *arguments) {
  if (arguments->all_databases && arguments->databases) {
    fprintf(stderr, "conflict option --all-databases and --databases\n");
    return -1;
  }

  if (arguments->start_time > arguments->end_time) {
    fprintf(stderr, "start time is larger than end time\n");
    return -1;
  }
  
  if (arguments->arg_list_len == 0) {
    if ((!arguments->all_databases) && (!arguments->isDumpIn)) {
      fprintf(stderr, "taosdump requires parameters\n");
      return -1;
    }
  }
/*
  if (arguments->isDumpIn && (strcmp(arguments->outpath, DEFAULT_DUMP_FILE) != 0)) {
    fprintf(stderr, "duplicate parameter input and output file path\n");
    return -1;
  }
*/
  if (!arguments->isDumpIn && arguments->encode != NULL) {
    fprintf(stderr, "invalid option in dump out\n");
    return -1;
  }

  if (arguments->table_batch <= 0) {
    fprintf(stderr, "invalid option in dump out\n");
    return -1;
  }

  return 0;
}

bool isEmptyCommand(char *cmd) {
  char *pchar = cmd;

  while (*pchar != '\0') {
    if (*pchar != ' ') return false;
    pchar++;
  }

  return true;
}

void taosReplaceCtrlChar(char *str) {
  _Bool ctrlOn = false;
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

int converStringToReadable(char *str, int size, char *buf, int bufsize) {
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

int convertNCharToReadable(char *str, int size, char *buf, int bufsize) {
  char *pstr = str;
  char *pbuf = buf;
  // TODO
  wchar_t wc;
  while (size > 0) {
    if (*pstr == '\0') break;
    int byte_width = mbtowc(&wc, pstr, MB_CUR_MAX);
    if (byte_width < 0) {
      fprintf(stderr, "mbtowc() return fail.\n");
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

void taosDumpCharset(FILE *fp) {
  char charsetline[256];

  (void)fseek(fp, 0, SEEK_SET);
  sprintf(charsetline, "#!%s\n", tsCharset);
  (void)fwrite(charsetline, strlen(charsetline), 1, fp);
}

void taosLoadFileCharset(FILE *fp, char *fcharset) {
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

static char    **tsDumpInSqlFiles   = NULL;
static int32_t   tsSqlFileNum = 0;
static char      tsDbSqlFile[TSDB_FILENAME_LEN] = {0};
static char      tsfCharset[64] = {0};
static int taosGetFilesNum(const char *directoryName, const char *prefix)
{
  char cmd[1024] = { 0 };
  sprintf(cmd, "ls %s/*.%s | wc -l ", directoryName, prefix);

  FILE *fp = popen(cmd, "r");
  if (fp == NULL) {
    fprintf(stderr, "ERROR: failed to execute:%s, error:%s\n", cmd, strerror(errno));
    exit(0);
  }

  int fileNum = 0;
  if (fscanf(fp, "%d", &fileNum) != 1) {
    fprintf(stderr, "ERROR: failed to execute:%s, parse result error\n", cmd);
    exit(0);
  }

  if (fileNum <= 0) {
    fprintf(stderr, "ERROR: directory:%s is empry\n", directoryName);
    exit(0);
  }

  pclose(fp);
  return fileNum;
}

static void taosParseDirectory(const char *directoryName, const char *prefix, char **fileArray, int totalFiles)
{
  char cmd[1024] = { 0 };
  sprintf(cmd, "ls %s/*.%s | sort", directoryName, prefix);

  FILE *fp = popen(cmd, "r");
  if (fp == NULL) {
    fprintf(stderr, "ERROR: failed to execute:%s, error:%s\n", cmd, strerror(errno));
    exit(0);
  }

  int fileNum = 0;
  while (fscanf(fp, "%128s", fileArray[fileNum++])) {
    if (strcmp(fileArray[fileNum-1], tsDbSqlFile) == 0) {
      fileNum--;
    }
    if (fileNum >= totalFiles) {
      break;
    }
  }

  if (fileNum != totalFiles) {
    fprintf(stderr, "ERROR: directory:%s changed while read\n", directoryName);
    pclose(fp);
    exit(0);
  }

  pclose(fp);
}

static void taosCheckTablesSQLFile(const char *directoryName)
{
  char cmd[1024] = { 0 };
  sprintf(cmd, "ls %s/dbs.sql", directoryName);

  FILE *fp = popen(cmd, "r");
  if (fp == NULL) {
    fprintf(stderr, "ERROR: failed to execute:%s, error:%s\n", cmd, strerror(errno));
    exit(0);
  }

  while (fscanf(fp, "%128s", tsDbSqlFile)) {
    break;
  }

  pclose(fp);
}

static void taosMallocSQLFiles()
{
  tsDumpInSqlFiles = (char**)calloc(tsSqlFileNum, sizeof(char*));
  for (int i = 0; i < tsSqlFileNum; i++) {
    tsDumpInSqlFiles[i] = calloc(1, TSDB_FILENAME_LEN);
  }
}

static void taosFreeSQLFiles()
{
  for (int i = 0; i < tsSqlFileNum; i++) {
    tfree(tsDumpInSqlFiles[i]);
  }
  tfree(tsDumpInSqlFiles);
}

static void taosGetDirectoryFileList(char *inputDir)
{
  struct stat fileStat;
  if (stat(inputDir, &fileStat) < 0) {
    fprintf(stderr, "ERROR: %s not exist\n", inputDir);
    exit(0);
  }

  if (fileStat.st_mode & S_IFDIR) {
    taosCheckTablesSQLFile(inputDir);
    tsSqlFileNum = taosGetFilesNum(inputDir, "sql");
    int totalSQLFileNum = tsSqlFileNum;
    if (tsDbSqlFile[0] != 0) {
      tsSqlFileNum--;
    }
    taosMallocSQLFiles();
    taosParseDirectory(inputDir, "sql", tsDumpInSqlFiles, tsSqlFileNum);
    fprintf(stdout, "\nstart to dispose %d files in %s\n", totalSQLFileNum, inputDir);
  }
  else {
    fprintf(stderr, "ERROR: %s is not a directory\n", inputDir);
    exit(0);
  }
}

static FILE*  taosOpenDumpInFile(char *fptr) {
  wordexp_t full_path;

  if (wordexp(fptr, &full_path, 0) != 0) {
    fprintf(stderr, "ERROR: illegal file name: %s\n", fptr);
    return NULL;
  }

  char *fname = full_path.we_wordv[0];
  
  FILE *f = fopen(fname, "r");
  if (f == NULL) {
    fprintf(stderr, "ERROR: failed to open file %s\n", fname);
    wordfree(&full_path);
    return NULL;
  }

  wordfree(&full_path);

  return f;
}

int taosDumpInOneFile_old(TAOS     * taos, FILE* fp, char* fcharset, char* encode) {
  char     *command       = NULL;
  char     *lcommand      = NULL;
  int       tsize         = 0;
  char     *line          = NULL;
  _Bool     isRun         = true;
  size_t    line_size     = 0;
  char     *pstr          = NULL;
  char     *lstr          = NULL;
  size_t    inbytesleft   = 0;
  size_t    outbytesleft  = COMMAND_SIZE;
  char     *tcommand      = NULL;
  char     *charsetOfFile = NULL;
  iconv_t   cd            = (iconv_t)(-1);

  command  = (char *)malloc(COMMAND_SIZE);
  lcommand = (char *)malloc(COMMAND_SIZE);
  if (command == NULL || lcommand == NULL) {
    fprintf(stderr, "failed to connect to allocate memory\n");
    goto _dumpin_exit_failure;
  }

  // Resolve locale
  if (*fcharset != '\0') {
    charsetOfFile = fcharset;
  } else {
    charsetOfFile = encode;
  }

  if (charsetOfFile != NULL && strcasecmp(tsCharset, charsetOfFile) != 0) {
    cd = iconv_open(tsCharset, charsetOfFile);
    if (cd == ((iconv_t)(-1))) {
      fprintf(stderr, "Failed to open iconv handle\n");
      goto _dumpin_exit_failure;
    }
  }

  pstr = command;
  int64_t linenu = 0;
  while (1) {
    ssize_t size = getline(&line, &line_size, fp);
    linenu++;
    if (size <= 0) break;
    if (size == 1) {
      if (pstr != command) {
        inbytesleft = pstr - command;
        memset(lcommand, 0, COMMAND_SIZE);
        pstr = command;
        lstr = lcommand;
        outbytesleft = COMMAND_SIZE;
        if (cd != ((iconv_t)(-1))) {
          iconv(cd, &pstr, &inbytesleft, &lstr, &outbytesleft);
          tcommand = lcommand;
        } else {
          tcommand = command;
        }
        
        taosReplaceCtrlChar(tcommand);
        
        if (queryDB(taos, tcommand) != 0) {
          fprintf(stderr, "error sql: linenu: %" PRId64 " failed\n", linenu);
          exit(0);
        }
        
        pstr = command;
        pstr[0] = '\0';
        tsize = 0;
        isRun = true;
      }

      continue;
    }

    /* if (line[0] == '-' && line[1] == '-') continue; */

    line[size - 1] = 0;

    if (tsize + size - 1 > COMMAND_SIZE) {
      fprintf(stderr, "command is too long\n");
      goto _dumpin_exit_failure;
    }

    if (line[size - 2] == '\\') {
      line[size - 2] = ' ';
      isRun = false;
    } else {
      isRun = true;
    }

    memcpy(pstr, line, size - 1);
    pstr += (size - 1);
    *pstr = '\0';

    if (!isRun) continue;

    if (command != pstr && !isEmptyCommand(command)) {
      inbytesleft = pstr - command;
      memset(lcommand, 0, COMMAND_SIZE);
      pstr = command;
      lstr = lcommand;
      outbytesleft = COMMAND_SIZE;
      if (cd != ((iconv_t)(-1))) {
        iconv(cd, &pstr, &inbytesleft, &lstr, &outbytesleft);
        tcommand = lcommand;
      } else {
        tcommand = command;
      }
      taosReplaceCtrlChar(tcommand);
      if (queryDB(taos, tcommand) != 0) {
        fprintf(stderr, "error sql: linenu:%" PRId64 " failed\n", linenu);
        exit(0); 
      }
    }

    pstr = command;
    *pstr = '\0';
    tsize = 0;
  }

  if (pstr != command) {
    inbytesleft = pstr - command;
    memset(lcommand, 0, COMMAND_SIZE);
    pstr = command;
    lstr = lcommand;
    outbytesleft = COMMAND_SIZE;
    if (cd != ((iconv_t)(-1))) {
      iconv(cd, &pstr, &inbytesleft, &lstr, &outbytesleft);
      tcommand = lcommand;
    } else {
      tcommand = command;
    }
    taosReplaceCtrlChar(lcommand);
    if (queryDB(taos, tcommand) != 0)
      fprintf(stderr, "error sql: linenu:%" PRId64 " failed \n", linenu);
  }

  if (cd != ((iconv_t)(-1))) iconv_close(cd);
  tfree(line);
  tfree(command);
  tfree(lcommand);
  taos_close(taos);
  fclose(fp);
  return 0;

_dumpin_exit_failure:
  if (cd != ((iconv_t)(-1))) iconv_close(cd);
  tfree(command);
  tfree(lcommand);
  taos_close(taos);
  fclose(fp);
  return -1;
}

int taosDumpInOneFile(TAOS     * taos, FILE* fp, char* fcharset, char* encode, char* fileName) {
  int       read_len = 0;
  char *    cmd      = NULL;
  size_t    cmd_len  = 0;
  char *    line     = NULL;
  size_t    line_len = 0;

  cmd  = (char *)malloc(COMMAND_SIZE);
  if (cmd == NULL) {
    fprintf(stderr, "failed to allocate memory\n");
    return -1;
  }

  int lineNo = 0;
  while ((read_len = getline(&line, &line_len, fp)) != -1) {
    ++lineNo;
    if (read_len >= COMMAND_SIZE) continue;
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
    if (queryDB(taos, cmd)) {
      fprintf(stderr, "error sql: linenu:%d, file:%s\n", lineNo, fileName);
    }

    memset(cmd, 0, COMMAND_SIZE);
    cmd_len = 0;
  }

  tfree(cmd);
  tfree(line);
  fclose(fp);
  return 0;
}

void* taosDumpInWorkThreadFp(void *arg)
{
  SThreadParaObj *pThread = (SThreadParaObj*)arg;
  for (int32_t f = 0; f < tsSqlFileNum; ++f) {
    if (f % pThread->totalThreads == pThread->threadIndex) {
      char *SQLFileName = tsDumpInSqlFiles[f];
      FILE* fp = taosOpenDumpInFile(SQLFileName);
      if (NULL == fp) {
        continue;
      }
      fprintf(stderr, "Success Open input file: %s\n", SQLFileName);
      taosDumpInOneFile(pThread->taosCon, fp, tsfCharset, tsArguments.encode, SQLFileName);
    }
  }

  return NULL;
}

static void taosStartDumpInWorkThreads(struct arguments *args)
{
  pthread_attr_t  thattr;
  SThreadParaObj *pThread;
  int32_t         totalThreads = args->thread_num;

  if (totalThreads > tsSqlFileNum) {
    totalThreads = tsSqlFileNum;
  }
  
  SThreadParaObj *threadObj = (SThreadParaObj *)calloc(totalThreads, sizeof(SThreadParaObj));
  for (int32_t t = 0; t < totalThreads; ++t) {
    pThread = threadObj + t;
    pThread->threadIndex = t;
    pThread->totalThreads = totalThreads;
    pThread->taosCon = taos_connect(args->host, args->user, args->password, NULL, args->port);
    if (pThread->taosCon == NULL) {
      fprintf(stderr, "ERROR: thread:%d failed connect to TDengine, error:%s\n", pThread->threadIndex, taos_errstr(pThread->taosCon));
      exit(0);
    }

    pthread_attr_init(&thattr);
    pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);

    if (pthread_create(&(pThread->threadID), &thattr, taosDumpInWorkThreadFp, (void*)pThread) != 0) {
      fprintf(stderr, "ERROR: thread:%d failed to start\n", pThread->threadIndex);
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


int taosDumpIn(struct arguments *arguments) {
  assert(arguments->isDumpIn);
  
  TAOS     *taos    = NULL;
  FILE     *fp      = NULL;

  taos = taos_connect(arguments->host, arguments->user, arguments->password, NULL, arguments->port);
  if (taos == NULL) {
    fprintf(stderr, "failed to connect to TDengine server\n");
    return -1;
  }

  taosGetDirectoryFileList(arguments->inpath);

  if (tsDbSqlFile[0] != 0) {
    fp = taosOpenDumpInFile(tsDbSqlFile);
    if (NULL == fp) {
      fprintf(stderr, "failed to open input file %s\n", tsDbSqlFile);
      return -1;
    }
    fprintf(stderr, "Success Open input file: %s\n", tsDbSqlFile);
    
    taosLoadFileCharset(fp, tsfCharset);
    
    taosDumpInOneFile(taos, fp, tsfCharset, arguments->encode, tsDbSqlFile);
  }

  taosStartDumpInWorkThreads(arguments);

  taos_close(taos);
  taosFreeSQLFiles();
  return 0;
}


