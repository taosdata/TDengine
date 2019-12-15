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

#include <argp.h>
#include <assert.h>
#include <error.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <unistd.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>
#include <sys/stat.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>

#include "taos.h"
#include "tglobalcfg.h"
#include "tlog.h"
#include "ihash.h"
#include "shash.h"
#include "tsdb.h"
#include "taosmsg.h"
#include "tutil.h"

int64_t taosGetMsFromYYYYMMDD(const char* timeStr) {
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

  int64_t expire = mktime(&tm);
  if (expire <= 0) {
    fprintf(stderr, "time should larger than 0\n");
    return 0;
  }

  return expire;
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
static char args_doc[] = "dbname [tbname ...]\n--databases dbname ...\n--all-databases\n-i input_file";

/* Keys for options without short-options. */
#define OPT_ABORT 1 /* –abort */

/* The options we understand. */
static struct argp_option options[] = {
    // input option
    {"inputDir",     'i', "./input",    0, "Raw data file path",                                                                       0},
    {"beginTime",    'b', "YYYY-MM-DD", 0, "Raw data start time, need to be specified when repeat is greater than 1",                  0},
    {"endTime",      'e', "YYYY-MM-DD", 0, "Raw data end time, need to be specified when repeat is greater than 1",                    0},

    // output option
    {"outputDir",    'o', "./output",   0, "Output data file path",                                                                    1},
    {"numOfFiles",   'n', "100",        0, "Number of generated SQL files",                                                            1},
    {"repeat",       'r', "0",          0, "Repeat number of splits, if 0, only statistics",                                           1},
    {"generateTime", 'g', "YYYY-MM-DD", 0, "The time when the data was generated, need to be specified when repeat is greater than 1", 1},

    // database option
    {"database",     'd', "100",        0, "The name of the database to be created",                                                   2},
    {"ablocks",      'a', "4",          0, "Ablocks option for the database to be created",                                            2},
    {"cache",        'c', "16384",      0, "Cache options for the database to be created",                                             2},
    {"stable",       's', "st",         0, "The name of the super table to be created",                                                2},
    {"prefix",       'p', "t_",         0, "Prefix of table name to be created",                                                       2},
    {"batch ",       'p', "0",          0, "Number of batches of SQL statements, When it is 0, it means that 50K of SQL is specified", 2},

    {"debugflag",    'D', "135",        0, "Debug of the program, 131- output warning and error, 199 - both screen and file",          3},

    {0}
};

/* Used by main to communicate with parse_opt. */
struct arguments {
  char inputDir[TSDB_FILENAME_LEN + 1];
  char outputDir[TSDB_FILENAME_LEN + 1];
  int64_t beginTime;
  int64_t endTime;
  int numOfFiles;
  int repeat;
  int64_t generateTime;
  char database[TSDB_DB_NAME_LEN + 1];
  int ablocks;
  int cache;
  char stable[TSDB_METER_NAME_LEN + 1];
  char prefix[TSDB_METER_NAME_LEN + 1];
  int batch;
  int debugFlag;
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
    case 'o':
      if (wordexp(arg, &full_path, 0) != 0) {
        fprintf(stderr, "Invalid path %s\n", arg);
        return -1;
      }
      strcpy(arguments->outputDir, full_path.we_wordv[0]);
      wordfree(&full_path);
      break;
    case 'b':
      arguments->beginTime = taosGetMsFromYYYYMMDD(arg);
      break;
    case 'e':
      arguments->endTime = taosGetMsFromYYYYMMDD(arg);;
      break;
    case 'n':
      arguments->numOfFiles = atoi(arg);
      break;
    case 'r':
      arguments->repeat = atoi(arg);
      break;
    case 'g':
      arguments->generateTime = taosGetMsFromYYYYMMDD(arg);
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
    case 'b':
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

int taosCheckParam(struct arguments *arguments) {
  tdPrint("program parameters:");
  tdPrint("inputDir: %s", arguments.inputDir);
  tdPrint("beginTime: %ld", arguments.beginTime);
  tdPrint("endTime:%ld", arguments.endTime);
  tdPrint("outputDir:%s", arguments.outputDir);
  tdPrint("numOfFiles:%d", arguments.numOfFiles);
  tdPrint("repeat:%d", arguments.repeat);
  tdPrint("generateTime:%ld", arguments.generateTime);
  tdPrint("database: %s", arguments.database);
  tdPrint("stable: %s", arguments.stable);
  tdPrint("prefix: %s", arguments.prefix);
  tdPrint("batch:%d", arguments.batch);
  tdPrint("ablocks:%d", arguments.ablocks);
  tdPrint("cache:%d", arguments.cache);
  tdPrint("debugFlag:%d", arguments.debugFlag);

  if (arguments->repeat >= 2) {
    if (arguments->beginTime == 0) {
      fprintf(stderr, "conflict option --repeat >= 2 but beginTime not specified\n");
      return -1;
    }
    if (arguments->endTime == 0) {
      fprintf(stderr, "conflict option --repeat >= 2 but endTime not specified\n");
      return -1;
    }
    if (arguments->generateTime == 0) {
      fprintf(stderr, "conflict option --repeat >=2 but generateTime not specified\n");
      return -1;
    }
  }

  return 0;
}

/* Our argp parser. */
static struct argp argp = {options, parse_opt, args_doc, doc};
static struct arguments arguments = {"./input", "./output", 0, 0, 100, 0, 0, db, 4, 16384, "st", "t_", 0, 199};

// mnode log function
#define tdError(...)                          \
  if (mdebugFlag & DEBUG_ERROR) {            \
    tprintf("ERROR TD  ", 255, __VA_ARGS__); \
  }
#define tdWarn(...)                                  \
  if (mdebugFlag & DEBUG_WARN) {                    \
    tprintf("WARN  TD  ", mdebugFlag, __VA_ARGS__); \
  }
#define tdTrace(...)                           \
  if (mdebugFlag & DEBUG_TRACE) {             \
    tprintf("TD  ", mdebugFlag, __VA_ARGS__); \
  }
#define tdPrint(...) \
  { tprintf("TD  ", 255, __VA_ARGS__); }


#define COMMAND_SIZE (50*1024)
#define MAX_TABLES 100000
char *insertString = "\ninsert into ";
int insertStringLen = strlen(insertString);

// pre define
int32_t taosColumnSize = 10;
bool taosContainSchema = false;

// output
void *taosTableHash = NULL;
char **taosCsvFiles = NULL;
int32_t taosCsvFileNum = 0;
int64_t taosTotalRows = 0;
int32_t taosTotalTables = 0;
int32_t taosBatchSize = 0;
FILE  **taosDataFps = NULL;
FILE    taosTableFp = NULL;
int32_t taosPrintSize = 0;

typedef struct {
  int tableId;
  int parseRows;
} TaosTable;

typedef struct {
  char *RECORD_TIME
  char *DX;
  char *DY;
  char *SPEED;
  char *DIRECTION;
  char *HIGH_LEVEL
  char *ALARM_TAG;
  char *STATUS;
  char *ORG_ID;
  char *VEHICLE_ID;
  int64_t RECORD_TIME_TS;
  bool parsedOk;
} TaosLine;

int taosGetDirectoryFileNum(const char *directoryName, const char *prefix)
{
  char cmd[1024] = { 0 };
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

void taosParseDirectory(const char *directoryName, const char *prefix, char **fileArray, int totalFiles)
{
  char cmd[1024] = { 0 };
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

const char* taosGenerateTime(int64_t timeMs)
{
  time_t tt = timeMs / 1000;
  static char buf[25] = { 0 };
  struct tm  *ptm;
  ptm = localtime(&tt);
  strftime(buf, 64, "%Y-%m-%d %H:%M:%S", ptm);
  return buf;
}

bool taosParseTime(const char *timeString, int64_t *timeVal)
{
  struct tm tm = { 0 };
  char* str = strptime(timeString, "%Y-%m-%d %H:%M:%S", &tm);
  if (str == NULL) return false;

  int64_t seconds = mktime(&tm);
  int64_t fraction = 0;

  char* sep = strstr(timeString, ".");
  if (sep != NULL && *sep == '.') {
    fraction = strnatoi(sep + 1, 3);
  }

  *timeVal = 1000 * seconds + fraction;
  return true;
}

void taosSortCsvFiles()
{
  int64_t *fileDate = calloc(taosCsvFileNum, sizeof(int64_t));

  for (int i = 0; i < taosCsvFileNum; ++i) {
    char *fileName = taosCsvFiles[i];
    int len = (int)strlen(fileName);
    if (len < 15) {
      tdError("invalid file name:%s length should large than 15", fileName);
      exit(EXIT_FAILURE);
    }
    fileDate[i] = strnatoi(fileName + (len - 12), 8);
  }

  for (int i = 0; i < taosCsvFileNum; ++i) {
    for (int j = i + 1; j < taosCsvFileNum; ++j) {
      if (fileDate[i] > fileDate[j]) {
        int64_t tmp = fileDate[i];
        fileDate[i] = fileDate[j];
        fileDate[j] = tmp;

        char *tmpFile = taosCsvFiles[i];
        taosCsvFiles[i] = taosCsvFiles[j];
        taosCsvFiles[j] = tmpFile;
      }
    }
  }

  free(fileDate);
}

void taosMallocCsvFiles()
{
  taosCsvFiles = (char**)calloc(taosCsvFileNum, sizeof(char*));
  for (int i = 0; i < taosCsvFileNum; i++) {
    taosCsvFiles[i] = calloc(1, TSDB_FILENAME_LEN);
  }
}

void taosGetDirectoryFileList()
{
  struct stat fileStat;
  if (stat(arguments.inputDir, &fileStat) < 0) {
    tdError("%s not exist", gsCsvFileName);
    exit(0);
  }

  if (fileStat.st_mode & S_IFDIR) {
    taosCsvFileNum = taosGetDirectoryFileNum(arguments.inputDir, "csv");
    taosMallocCsvFiles();
    taosParseDirectory(arguments.inputDir, "csv", taosCsvFiles, taosCsvFileNum);
    taosSortCsvFiles();
    tdPrint("start to dispose %d files in %s", taosCsvFileNum, taosCsvFileName);
  }
  else {
    taosCsvFileNum = 1;
    taosCsvFiles = (char**)calloc(taosCsvFileNum, sizeof(char*));
    taosCsvFiles[0] = gsCsvFileName;
    taosPrint("start to dispose %s", taosCsvFileName);
  }
}

int taosSplitLine(char *line, char**columns)
{
  int i = 0;
  int column_index = 0;
  columns[column_index++] = line;

  while (line[i] != 0) {
    if (line[i] == '\n' || line[i] == '\r') {
      line[i] = 0;
      break;
    }

    if (line[i] == '.' || line[i] == '-') {
      line[i] = '_';
    }
    else if (line[i] == ',') {
      line[i] = 0;
      columns[column_index++] = line + i + 1;
    }
    else {}

    i++;
  }

  return column_index;
}

void taosParseDataLine(char *line, int lineNum, char *csvfile)
{
  int fieldPos = -1;
  int columnPos = -1;
  TaosLine line;
  line.parsedOk = false;

  int len = 0;
  while (line[len] != 0) {
    if (line[len] == '\"') {
      if (fieldPos == -1) {
        fieldPos = len;
      } else {
        line[len] = 0;
        char *field = line + fieldPos;
        fieldPos = -1;
        columnPos++;
        switch (columnPos) {
          case 1:
            line.VEHICLE_ID = field;
            break;
          case 3:
            line.DX = field;
            break;
          case 4:
            line.DY = field;
            break;
          case 5:
            line.SPEED = field;
            break;
          case 6:
            line.RECORD_TIME = field;
            break;
          case 7:
            line.DIRECTION = field;
            break;
          case 8:
            line.HIGH_LEVEL = field;
            break;
          case 9:
            line.ALARM_TAG = field;
            break;
          case 10:
            line.STATUS = field;
            break;
          case 11:
            line.ORG_ID = field;
            break;
          case 12:
            line.parsedOk = true;
            break;
          default:
            break;
        }
      }
    }
  }

  if (!line.parsedOk) {
    return;
  }

  TaosTable *table = taosGetStrHashData(taosTableHash, line.VEHICLE_ID);
  if (table == NULL) {
    TaosTable newTable = {0, 0}
    table = taosAddStrHash(taosTableHash, field, &newTable);
    taosTotalTables++;
  }
  if (table == NULL) {
    tdError("file:%s table:%s add failed", csvfile, line.VEHICLE_ID);
    exit(0);
  }

  if ((arguments.batch == 0 && taosPrintSize > COMMAND_SIZE) || (arguments.batch == 0 && taosBatchSize > arguments.batch)) {
    taosBatchSize = 0;
    taosPrintSize = fprintf(taosDataFps[table->tableId % arguments.numOfFiles], "import into");
  }

  taosPrintSize += fprintf(taosDataFps[table->tableId % arguments.numOfFiles], " %s values(%ld,%s,%s,%s,%s,%s,%s,%s)",
        line.RECORD_TIME_TS, line.DX, line.DY, line.SPEED, line.DIRECTION, line.HIGH_LEVEL, line.ALARM_TAG, line.STATUS);
  taosBatchSize++;
}

void taosParseCsvFile(char *csvfile) {
  FILE *fp = fopen(csvfile, "r");
  if (fp == NULL) {
    tdError("failed to open file:%s, error:%s", csvfile, strerror(errno));
    exit(0);
  }

  char *line = NULL;
  int len = 0;
  int lineNum = 0;

  if (taosContainSchema) {
    tfree(line);
    getline(&line, &len, fp);
    if (line == NULL) {
      tdPrint("file:%s is empty", csvfile);
      return;
    }
  }

  do {
    tfree(line);
    getline(&line, &len, fp);
    if (line == NULL) {
      tdPrint("file:%s read finished, totallines:%d", csvfile, lineNum);
      break;
    }

    taosParseDataLine(line, lineNum, csvfile);
    lineNum++;
  } while (true);

  taosTotalRows += lineNum;
  fclose(fp);
}

void taosParseData() {
  for (int i = 0; i < taosCsvFileNum; ++i) {
    char *csvfile = taosCsvFiles[i];
    tdPrint("parse file:%s, index:%d", csvfile, i + 1);
    taosParseCsvFile(csvfile);
  }
}

void taosInitResources() {
  taosTableHash = taosInitStrHash(MAX_TABLES, sizeof(TaosTable), taosHashStringStep1);
  taosDataFps = (FILE**)calloc(arguments.numOfFiles, sizeof(FILE*));
  for (int f = 0; f < arguments.numOfFiles; ++f) {
    char fileName[TSDB_FILENAME_LEN] = {0};
    sprintf(fileName, "%s/d%d.sql", arguments.outputDir, f);
    taosDataFps[f] = fopen(fileName, "w");
    if (taosDataFps[f] == NULL) {
      tdError("failed to open file:%s, error:%s", fileName, strerror(errno));
      exit(0);
    }
  }

  char fileName[TSDB_FILENAME_LEN] = {0};
  sprintf(fileName, "%s/tables.sql", arguments.outputDir);
  taosTableFp = fopen(fileName, "w");
  if (taosTableFp == NULL) {
    tdError("failed to open file:%s, error:%s", fileName, strerror(errno));
    exit(0);
  }
}

void taosCloseResources() {
  for (int f = 0; f < arguments.numOfFiles; ++f) {
    fclose(taosDataFps[f]);
  }
  fclose(taosTableFp);
}

int main(int argc, char *argv[]) {
  /* Parse our arguments; every option seen by parse_opt will be
     reflected in arguments. */
  argp_parse(&argp, argc, argv, 0, 0, &arguments);

  if (arguments.abort) error(10, 0, "ABORTED");

  if (taosCheckParam(&arguments) < 0) {
    exit(EXIT_FAILURE);
  }

  int64_t start = taosGetTimestampMs();

  taosInitResources();

  taosParseData();

  int64_t end = taosGetTimestampMs();

  tdPrint("parse %d files in %s, find %d tables, total %ld rows", taosCsvFileNum, arguments.inputDir, taosTotalTables, taosTotalRows);
 
  tdPrint("generate %d files in %s, time spent: %d seconds", arguments.numOfFiles, arguments.outputDir, (end - start) / 1000);

  return 0;
}
