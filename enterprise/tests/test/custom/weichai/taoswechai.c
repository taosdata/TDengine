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

#define _XOPEN_SOURCE
#define _DEFAULT_SOURCE

#include "os.h"
#include "tlog.h"
#include "ihash.h"
#include "shash.h"
#include "taosdef.h"
#include "taosmsg.h"
#include "tutil.h"
#include "taos.h"
#include <error.h>

#define TIME_UNIT 1000  //ms
#define COMMAND_SIZE (50*1024)
#define MAX_TABLES 3000
#define MAX_DAYS 365*50
#define BEGIN_DAYS (946656000L * TIME_UNIT) //2001-01-01 00:00:00
#define INTERVAL_DAYS (86400 * TIME_UNIT)  //ms

// pre define
bool taosContainSchema = true;

int64_t tdGetMsFromYYYYMMDD(const char *timeStr) {
  if (timeStr == 0) {
    fprintf(stderr, "time string is null\n");
    return 0;
  }

  struct tm tm;
  memset(&tm, 0, sizeof(struct tm));
  char *str = strptime(timeStr, "%Y-%m-%d", &tm);
  if (str == NULL) {
    fprintf(stderr, "time format shall be: YYYY-MM-DD\n");
    return 0;
  }

  int64_t second = mktime(&tm);
  if (second <= 0) {
    fprintf(stderr, "time should larger than 0\n");
    return 0;
  }

  return second * TIME_UNIT;
}

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
static char args_doc[] = "The purpose of this program is to import json file to tdengine.";

/* Keys for options without short-options. */
#define OPT_ABORT 1 /* –abort */

/* The options we understand. */
static struct argp_option options[] = {
    // input option
    {"inputDir",  'i', "./input", 0, "Raw data file path",                                                                       0},

    // database option
    {"database",  'd', "db",      0, "The name of the database to be created",                                                   1},
    {"ablocks",   'a', "4",       0, "Ablocks option for the database to be created",                                            1},
    {"cache",     'c', "16384",   0, "Cache options for the database to be created",                                             1},
    {"stable",    's', "s_",      0, "The name of the super table to be created",                                                1},
    {"prefix",    'p', "u_",      0, "Prefix of table name to be created",                                                       1},
    {"mode ",     'm', "0",       0, "-1 write with auto ts, 0 - write, 1 - only create table, 2 - only parse",                  1},
    {"batch ",    'B', "1000",    0, "Number of batches of SQL statements, When it is 0, it means that 50K of SQL is specified", 1},

    {"debugflag", 'D', "199",     0, "Debug of the program, 131- output warning and error, 199 - both screen and file",          2},

    {0}
};

/* Used by main to communicate with parse_opt. */
struct arguments {
  char inputDir[TSDB_FILENAME_LEN];
  char database[TSDB_DB_NAME_LEN];
  int ablocks;
  int cache;
  char stable[TSDB_TABLE_NAME_LEN];
  char prefix[TSDB_TABLE_NAME_LEN];
  int batch;
  int writeMode;
  int debugFlag;
  int abort;
  char **arg_list;
  int arg_list_len;
};

/* Parse a single option. */
static error_t parse_opt(int key, char *arg, struct argp_state *state) {
  /* Get the input argument from argp_parse, which we
     know is a pointer to our arguments structure. */
  struct arguments *arguments = state->input;
  wordexp_t full_path;

  switch (key) {
    // connection option
    case 'i':
      if (wordexp(arg, &full_path, 0) != 0) {
        fprintf(stderr, "Invalid path %s\n", arg);
        return -1;
      }
      strcpy(arguments->inputDir, full_path.we_wordv[0]);
      wordfree(&full_path);
      break;
    case 'd':
      strcpy(arguments->database, arg);
      break;
    case 'a':
      arguments->ablocks = atoi(arg);
      break;
    case 'c':
      arguments->cache = atoi(arg);
      break;
    case 's':
      strcpy(arguments->stable, arg);
      break;
    case 'p':
      strcpy(arguments->prefix, arg);
      break;
    case 'm':
      arguments->writeMode = atoi(arg);
      break;
    case 'B':
      arguments->batch = atoi(arg);
      break;
    case 'D':
      arguments->debugFlag = atoi(arg);
      break;
    case OPT_ABORT:
      arguments->abort = 1;
      break;
    case ARGP_KEY_ARG:
      arguments->arg_list = &state->argv[state->next - 1];
      arguments->arg_list_len = state->argc - state->next + 1;
      state->next = state->argc;
      break;

    default:
      return ARGP_ERR_UNKNOWN;
  }
  return 0;
}

/* Our argp parser. */
static struct argp argp = {options, parse_opt, args_doc, doc};
static struct arguments tdArgs = {
    .inputDir = "./input",
    .database = "db",
    .ablocks = 4,
    .cache = 16384,
    .stable = "st",
    .prefix = "u_",
    .batch = 500,
    .writeMode = 0,
    .debugFlag = 199};

// log function
#define tdError(...)                          \
  if (tdArgs.debugFlag & DEBUG_ERROR) {            \
    taosPrintLog("ERROR TD  ", 255, __VA_ARGS__); \
  }
#define tdWarn(...)                                  \
  if (tdArgs.debugFlag & DEBUG_WARN) {                    \
    taosPrintLog("WARN  TD  ", tdArgs.debugFlag, __VA_ARGS__); \
  }
#define tdTrace(...)                           \
  if (tdArgs.debugFlag & DEBUG_TRACE) {             \
    taosPrintLog("TD  ", tdArgs.debugFlag, __VA_ARGS__); \
  }
#define tdPrint(...) \
  { taosPrintLog("TD  ", 255, __VA_ARGS__); }

int tdCheckParam(struct arguments *arguments) {
  tdPrint("program parameters");
  tdPrint("inputDir: %s", arguments->inputDir);
  tdPrint("database: %s", arguments->database);
  tdPrint("stable: %s", arguments->stable);
  tdPrint("prefix: %s", arguments->prefix);
  tdPrint("batch: %d", arguments->batch);
  tdPrint("writeMode: %d", arguments->writeMode);
  tdPrint("ablocks: %d", arguments->ablocks);
  tdPrint("cache: %d", arguments->cache);
  tdPrint("debugFlag: %d", arguments->debugFlag);

  return 0;
}

typedef struct {
  void *taos;
  void *tableHash;
  int threadIndex;
  int64_t inserted;
} DataFp;

// output
char **tdCsvFiles = NULL;
int32_t tdCsvFileNum = 0;
int64_t tdTotalRows = 0;
int64_t tdTotalLines = 0;
DataFp *tdDataFps = NULL;
int tdDataFpNum = 0;

typedef struct {
  char sql[65000];
  int sqlPos;
  int batch;
  DataFp *dataFp;
  int64_t timestamp;
} TdTable;

typedef struct {
  char *RECORD_TIME;
  char *DX;
  char *DY;
  char *SPEED;
  char *DIRECTION;
  char *HIGH_LEVEL;
  char *ALARM_TAG;
  char *STATUS;
  char *ORG_ID;
  char *VEHICLE_ID;
  int64_t RECORD_TIME_TS;
  bool parsedOk;
} TdRow;

int tdGetDirectoryFileNum(const char *directoryName, const char *prefix) {
  char cmd[1024] = {0};
  sprintf(cmd, "ls %s/*.%s | wc -l ", directoryName, prefix);

  FILE *fp = popen(cmd, "r");
  if (fp == NULL) {
    tdError("failed to execute:%s, error:%s", cmd, strerror(errno));
    exit(0);
  }

  int fileNum = 0;
  if (fscanf(fp, "%d", &fileNum) != 1) {
    tdError("failed to execute:%s, parse result error", cmd);
    exit(0);
  }

  if (fileNum <= 0) {
    tdError("directory:%s is empry", directoryName);
    exit(0);
  }

  pclose(fp);
  return fileNum;
}

void tdParseDirectory(const char *directoryName, const char *prefix, char **fileArray, int totalFiles) {
  char cmd[1024] = {0};
  sprintf(cmd, "ls %s/*.%s | sort", directoryName, prefix);

  FILE *fp = popen(cmd, "r");
  if (fp == NULL) {
    tdError("failed to execute:%s, error:%s", cmd, strerror(errno));
    exit(0);
  }

  int fileNum = 0;
  while (fscanf(fp, "%s", fileArray[fileNum++])) {
    if (fileNum >= totalFiles) {
      break;
    }
  }

  if (fileNum != totalFiles) {
    tdError("directory:%s changed while read", directoryName);
    exit(EXIT_FAILURE);
  }

  pclose(fp);
}

const char *tdGenerateTime(int64_t timeMs) {
  time_t tt = timeMs / 1000;
  static char buf[25] = {0};
  struct tm *ptm;
  ptm = localtime(&tt);
  strftime(buf, 64, "%Y-%m-%d %H:%M:%S", ptm);
  return buf;
}

bool tdReformatTime(const char *timeString, int64_t *timeVal) {
  struct tm tm = {0};
  //char* str = strptime(timeString, "%Y-%m-%d %H:%M:%S", &tm);

  //01-MAY-19 09.00.08.000000 AM
  char *str = strptime(timeString, "%d-%b-%y %l.%M.%S.000000 %p", &tm);
  if (str == NULL) return false;


  int64_t seconds = mktime(&tm);
  int64_t fraction = 0;

  char *sep = strstr(timeString, ".");
  if (sep != NULL && *sep == '.') {
    fraction = strnatoi(sep + 1, 3);
  }

  *timeVal = 1000 * seconds + fraction;
  if (TIME_UNIT == 1000000) {
    *timeVal *= 1000;
  }

  return true;
}

void tdSortCsvFiles() {
  int64_t *fileDate = calloc(tdCsvFileNum, sizeof(int64_t));

  for (int i = 0; i < tdCsvFileNum; ++i) {
    char *fileName = tdCsvFiles[i];
    int len = (int) strlen(fileName);
    if (len < 15) {
      tdError("invalid file name:%s length should large than 15", fileName);
      exit(EXIT_FAILURE);
    }
    fileDate[i] = strnatoi(fileName + (len - 12), 8);
  }

  for (int i = 0; i < tdCsvFileNum; ++i) {
    for (int j = i + 1; j < tdCsvFileNum; ++j) {
      if (fileDate[i] > fileDate[j]) {
        int64_t tmp = fileDate[i];
        fileDate[i] = fileDate[j];
        fileDate[j] = tmp;

        char *tmpFile = tdCsvFiles[i];
        tdCsvFiles[i] = tdCsvFiles[j];
        tdCsvFiles[j] = tmpFile;
      }
    }
  }

  free(fileDate);
}

void tdMallocCsvFiles() {
  tdCsvFiles = (char **) calloc(tdCsvFileNum, sizeof(char *));
  for (int i = 0; i < tdCsvFileNum; i++) {
    tdCsvFiles[i] = calloc(1, TSDB_FILENAME_LEN);
  }
}

void tdGetDirectoryFileList() {
  wordexp_t full_path;

  if (wordexp(tdArgs.inputDir, &full_path, 0) != 0) {
    tdError("illegal file name:%s", tdArgs.inputDir);
    return;
  }

  char *fname = full_path.we_wordv[0];
  struct stat fileStat;
  if (stat(fname, &fileStat) < 0) {
    tdError("%s not exist", tdArgs.inputDir);
    exit(0);
  }

  if (fileStat.st_mode & S_IFDIR) {
    tdCsvFileNum = tdGetDirectoryFileNum(fname, "txt");
    tdMallocCsvFiles();
    tdParseDirectory(fname, "txt", tdCsvFiles, tdCsvFileNum);
    //tdSortCsvFiles();
    tdPrint("start to dispose %d files in %s", tdCsvFileNum, tdArgs.inputDir);
  } else {
    tdCsvFileNum = 1;
    tdCsvFiles = (char **) calloc(tdCsvFileNum, sizeof(char *));
    tdCsvFiles[0] = fname;
    tdPrint("start to dispose %s", tdArgs.inputDir);
  }
}

//import function

void tdParseResourceLine(int threadindex, char *line) {
  char *columns[500];
  char *fields[500];
  int fieldPos = -1;
  int columnPos = 0;
  int len = 0;
  DataFp *dataFp = &tdDataFps[threadindex];

  while (line[len] != 0) {
    if (line[len] == '\"') {
      if (fieldPos == -1) {
        fieldPos = len + 1;
      } else {
        line[len] = 0;
        char *field = line + fieldPos;
        fieldPos = -1;
        columns[columnPos++] = field;
      }
    }

    if (columnPos >= 500) {
      return;
    }

    len++;
  }

  columnPos = columnPos / 2 * 2;
  char *terminal_number = NULL; //binary(50)
  char *config_id = NULL;  //binary(50)
  char *vehicle_id = NULL; //binary(40)
  char *cyclekey = NULL;   //bigint
  char *terminal_id = NULL; //int
  char *EVENTNUM = NULL;
  char *timestamp1 = NULL;

  int fieldNum = 0;
  for (int i = 0; i < columnPos; i += 2) {
    if (terminal_number == NULL && strcmp(columns[i], "terminal_number") == 0) {
      terminal_number = columns[i + 1];
      continue;
    }
    if (config_id == NULL && strcmp(columns[i], "CONFIG_ID") == 0) {
      config_id = columns[i + 1];
      continue;
    }
    if (vehicle_id == NULL && strcmp(columns[i], "VEHICLE_ID") == 0) {
      vehicle_id = columns[i + 1];
      continue;
    }
    if (cyclekey == NULL && strcmp(columns[i], "CYCLEKEY") == 0) {
      cyclekey = columns[i + 1];
      continue;
    }
    if (terminal_id == NULL && strcmp(columns[i], "TERMINAL_ID") == 0) {
      terminal_id = columns[i + 1];
      continue;
    }
    if (EVENTNUM == NULL && strcmp(columns[i], "EVENTNUM") == 0) {
      EVENTNUM = columns[i + 1];
      continue;
    }
    if (timestamp1 == NULL && strcmp(columns[i], "timestamp1") == 0) {
      timestamp1 = columns[i + 1];
      continue;
    }
    fields[fieldNum] = columns[i];
    fields[fieldNum + 1] = columns[i + 1];
    fieldNum += 2;
  }

  if (terminal_number == NULL || timestamp1 == NULL || EVENTNUM == NULL) {
    return;
  }

  if (strcmp(EVENTNUM, "0") == 0 || strcmp(EVENTNUM, "3") == 0) {
    return;
  }

  for (int f = 0; f < fieldNum; f += 2) {
    char tableName[64];
    sprintf(tableName, "%s_%s", terminal_number, fields[f]);
    for (int i = 0; i < 64; ++i) {
      if (tableName[i] == '[' || tableName[i] == ']' || tableName[i] == '.') {
        tableName[i] = '_';
      }
    }

    TdTable *table = taosGetStrHashData(dataFp->tableHash, tableName);
    if (table == NULL) {
      TdTable newTable = {.batch = 0, .sqlPos = 0, .dataFp = dataFp};
      table = taosAddStrHash(dataFp->tableHash, tableName, (char *) (&newTable));
      table->sqlPos = sprintf(table->sql, "insert into %s values ", tableName);
      table->timestamp = 1545550734000L;

      char sql[2048];
      sprintf(sql, "create table if not exists %s using %s tags('%s', '%s', '%s', %s, %s, '%s')",
              tableName, tdArgs.stable, config_id, terminal_number, vehicle_id, terminal_id, cyclekey, fields[f]);
      if (tdArgs.writeMode <= 1) {
        if (taos_query(dataFp->taos, sql)) {
          tdError("thread:%d failed to execute sql:%s, error:%s\n", dataFp->threadIndex, sql,
                  taos_errstr(dataFp->taos));
        }
      }
    }
    if (table == NULL) {
      tdError("thread:%s table:%s add failed", threadindex, tableName);
      exit(0);
    }

    if (table->batch == tdArgs.batch) {
      if (tdArgs.writeMode <= 0) {
        if (taos_query(dataFp->taos, table->sql)) {
          tdError("thread:%d failed to execute sql:%s, error:%s\n", dataFp->threadIndex, table->sql,
                  taos_errstr(dataFp->taos));
        }
      }
      table->sqlPos = sprintf(table->sql, "insert into %s values ", tableName);
      dataFp->inserted += table->batch;
      table->batch = 0;
    } else {
      if (tdArgs.writeMode == -1) {
        table->sqlPos += sprintf(table->sql + table->sqlPos, "(%ld,%s)", table->timestamp, fields[f + 1]);
        table->timestamp += 1000;
      } else if (tdArgs.writeMode == -2) {
        table->sqlPos += sprintf(table->sql + table->sqlPos, "(%s,%s)", timestamp1, fields[f + 1]);
      } else {
        table->sqlPos += sprintf(table->sql + table->sqlPos, "('%s',%s)", timestamp1, fields[f + 1]);
      }
      table->batch++;
    }
  }
}

void tdParseCsvFile(char *csvfile, int threadIndex) {
  FILE *fp = fopen(csvfile, "r");
  if (fp == NULL) {
    tdError("failed to open file:%s, error:%s", csvfile, strerror(errno));
    exit(0);
  }

  char *line = NULL;
  size_t len = 0;
  int lineNum = 0;

  do {
    tfree(line);
    int ret = getline(&line, &len, fp);
    if (line == NULL || ret == -1 || len == 0) {
      tdPrint("file:%s read finished, totalRows:%d", csvfile, lineNum);
      break;
    }

    tdParseResourceLine(threadIndex, line);
    lineNum++;
  } while (true);

  tdTotalRows += lineNum;
  fclose(fp);
}

//import function
void tdInitResources(int resourceNum) {
  void *taos = taos_connect("127.0.0.1", "root", "taosdata", NULL, 0);
  if (taos == NULL) {
    tdError("failed connect to TDengine, error:%s\n", taos_errstr(taos));
    exit(0);
  }

  char sql[1024];
  sprintf(sql, "create database if not exists %s ablocks %d cache %d", tdArgs.database, tdArgs.ablocks, tdArgs.cache);
  if (taos_query(taos, sql)) {
    tdError("failed to execute sql:%s, error:%s", sql, taos_errstr(taos));
    exit(0);
  }

  sprintf(sql, "create table if not exists %s.%s (ts timestamp, val double) "
               "tags(configId binary(50),  terminal_number binary(30), vehicle_id binary(40),  terminal_id int,  cyclekey bigint, param binary(40))",
          tdArgs.database, tdArgs.stable);
  if (taos_query(taos, sql)) {
    tdError("failed to execute sql:%s, error:%s", sql, taos_errstr(taos));
    exit(0);
  }

  sprintf(sql, "use %s", tdArgs.database);

  tdDataFps = (DataFp *) calloc(resourceNum, sizeof(DataFp));

  for (int i = 0; i < resourceNum; ++i) {
    DataFp *dataFp = &tdDataFps[i];
    dataFp->taos = taos_connect("127.0.0.1", "root", "taosdata", NULL, 0);
    dataFp->tableHash = taosInitStrHash(MAX_TABLES, sizeof(TdTable), taosHashStringStep1);
    dataFp->threadIndex = i;
    dataFp->inserted = 0;

    if (dataFp->taos == NULL) {
      tdError("thread:%d failed connect to TDengine, error:%s\n", dataFp->threadIndex, taos_errstr(dataFp->taos));
      exit(0);
    }

    if (taos_query(dataFp->taos, sql)) {
      tdError("thread:%d, failed to execute sql:%s, error:%s", dataFp->threadIndex, sql, taos_errstr(taos));
      exit(0);
    }
  }
}

void tdCloseResouceImp(void *rawTable) {
  TdTable *table = (TdTable*)rawTable;
  if (table->batch != 0) {
    DataFp *dataFp = table->dataFp;
    if (tdArgs.writeMode <= 0) {
      if (taos_query(dataFp->taos, table->sql)) {
        tdError("thread:%d, failed to execute sql:%s, error:%s", dataFp->threadIndex, table->sql,
                taos_errstr(dataFp->taos));
      }
    }
    dataFp->inserted += table->batch;
  }
}

//import function
void tdCloseResource(int threadIndex) {
  DataFp *dataFp = &tdDataFps[threadIndex];
  taosCleanUpStrHashWithFp(dataFp->tableHash, tdCloseResouceImp);
}

//import funciton
int tdGetResourceInserted(int threadIndex) {
  DataFp *dataFp = &tdDataFps[threadIndex];
  return dataFp->inserted;
}

typedef struct {
  pthread_t threadID;
  int threadIndex;
  int totalThreads;
  void *taos;
} ShellThreadObj;

void *shellImportThreadFp(void *arg) {
  ShellThreadObj *pThread = (ShellThreadObj *) arg;

  char *csvfile = tdCsvFiles[pThread->threadIndex];
  tdPrint("parse file:%s, index:%d", csvfile, pThread->threadIndex);

  tdParseCsvFile(csvfile, pThread->threadIndex);

  tdCloseResource(pThread->threadIndex);

  return NULL;
}

void tdParseData() {
  tdGetDirectoryFileList();

  tdInitResources(tdCsvFileNum);

  pthread_attr_t thattr;
  ShellThreadObj *threadObj = (ShellThreadObj *) calloc(tdCsvFileNum, sizeof(ShellThreadObj));
  for (int t = 0; t < tdCsvFileNum; ++t) {
    ShellThreadObj *pThread = threadObj + t;
    pThread->threadIndex = t;
    pThread->totalThreads = tdCsvFileNum;

    pthread_attr_init(&thattr);
    pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);

    if (pthread_create(&(pThread->threadID), &thattr, shellImportThreadFp, (void *) pThread) != 0) {
      tdError("ERROR: thread:%d failed to start", pThread->threadIndex);
      exit(0);
    }
  }

  for (int t = 0; t < tdCsvFileNum; ++t) {
    pthread_join(threadObj[t].threadID, NULL);
  }

  for (int t = 0; t < tdCsvFileNum; ++t) {
    taos_close(tdDataFps[t].taos);
    tdTotalLines += tdDataFps[t].inserted;
  }

  free(threadObj);
}

int main(int argc, char *argv[]) {
  /* Parse our arguments; every option seen by parse_opt will be
     reflected in arguments. */
  argp_parse(&argp, argc, argv, 0, 0, &tdArgs);

  if (tdArgs.abort) error(10, 0, "ABORTED");

  if (tdCheckParam(&tdArgs) < 0) {
    exit(EXIT_FAILURE);
  }

  int64_t start = taosGetTimestampMs();

  tdParseData();

  int64_t end = taosGetTimestampMs();

  float seconds = (end - start) / 1000.0;
  tdPrint("parse %d files in %s, total %ld json %ld lines, time spent: %.2f seconds, speed: %d row/s",
          tdCsvFileNum, tdArgs.inputDir, tdTotalRows, tdTotalLines, seconds,  (int)(tdTotalLines / seconds));

  return 0;
}
