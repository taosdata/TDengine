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
#include <error.h>

#define TIME_UNIT 1000  //ms
#define COMMAND_SIZE (50*1024)
#define MAX_TABLES 100000
#define MAX_DAYS 365*50
#define BEGIN_DAYS (946656000L * TIME_UNIT) //2001-01-01 00:00:00
#define INTERVAL_DAYS (86400 * TIME_UNIT)  //ms

// pre define
bool taosContainSchema = true;

int64_t tdGetMsFromYYYYMMDD(const char* timeStr) {
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
static char args_doc[] = "The purpose of this program is to split the CSV file into multiple SQL files.";

/* Keys for options without short-options. */
#define OPT_ABORT 1 /* –abort */

/* The options we understand. */
static struct argp_option options[] = {
    // input option
    {"inputDir",   'i', "./input",        0, "Raw data file path",                                                                       0},
    {"beginTime",  'b', "YYYY-MM-DD",     0, "Raw data start time, need to be specified when repeat is greater than 1",                  0},
    {"endTime",    'e', "YYYY-MM-DD",     0, "Raw data end time, need to be specified when repeat is greater than 1",                    0},

    // output option
    {"outputDir",  'o', "./output",       0, "Output data file path",                                                                    1},
    {"numOfFiles", 'n', "10",             0, "Number of generated SQL files",                                                            1},
    {"repeat",     'r', "1",              0, "Repeat number of splits, if 0, only statistics",                                           1},
    {"genTime",    'g', "YYYY-MM-DD",     0, "The time when the data was generated, need to be specified when repeat is greater than 1", 1},

    // database option
    {"database",   'd', "db",             0, "The name of the database to be created",                                                   2},
    {"ablocks",    'a', "4",              0, "Ablocks option for the database to be created",                                            2},
    {"cache",      'c', "16384",          0, "Cache options for the database to be created",                                             2},
    {"stable",     's', "UVMP",           0, "The name of the super table to be created",                                                2},
    {"prefix",     'p', "u_",             0, "Prefix of table name to be created",                                                       2},
    {"batch ",     'B', "100",              0, "Number of batches of SQL statements, When it is 0, it means that 50K of SQL is specified", 2},

    {"debugflag",  'D', "199",            0, "Debug of the program, 131- output warning and error, 199 - both screen and file",          3},

    {0}
};

/* Used by main to communicate with parse_opt. */
struct arguments {
  char inputDir[TSDB_FILENAME_LEN];
  char outputDir[TSDB_FILENAME_LEN];
  int64_t beginTime;
  int64_t endTime;
  int numOfFiles;
  int repeat;
  int64_t generateTime;
  char database[TSDB_DB_NAME_LEN];
  int ablocks;
  int cache;
  char stable[TSDB_TABLE_NAME_LEN];
  char prefix[TSDB_TABLE_NAME_LEN];
  int batch;
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
    case 'o':
      if (wordexp(arg, &full_path, 0) != 0) {
        fprintf(stderr, "Invalid path %s\n", arg);
        return -1;
      }
      strcpy(arguments->outputDir, full_path.we_wordv[0]);
      wordfree(&full_path);
      break;
    case 'b':
      arguments->beginTime = tdGetMsFromYYYYMMDD(arg);
      break;
    case 'e':
      arguments->endTime = tdGetMsFromYYYYMMDD(arg);;
      break;
    case 'n':
      arguments->numOfFiles = atoi(arg);
      break;
    case 'r':
      arguments->repeat = atoi(arg);
      break;
    case 'g':
      arguments->generateTime = tdGetMsFromYYYYMMDD(arg);
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
    .outputDir = "./output",
    .beginTime = 0,
    .endTime = 0,
    .numOfFiles = 10,
    .repeat = 1,
    .generateTime = 0,
    .database = "db",
    .ablocks = 4,
    .cache = 16384,
    .stable = "UVMP",
    .prefix = "u_",
    .batch = 100,
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
  tdPrint("beginTime: %ld", arguments->beginTime);
  tdPrint("endTime: %ld", arguments->endTime);
  tdPrint("outputDir: %s", arguments->outputDir);
  tdPrint("numOfFiles: %d", arguments->numOfFiles);
  tdPrint("repeat: %d", arguments->repeat);
  tdPrint("generateTime: %ld", arguments->generateTime);
  tdPrint("database: %s", arguments->database);
  tdPrint("stable: %s", arguments->stable);
  tdPrint("prefix: %s", arguments->prefix);
  tdPrint("batch: %d", arguments->batch);
  tdPrint("ablocks: %d", arguments->ablocks);
  tdPrint("cache: %d", arguments->cache);
  tdPrint("debugFlag: %d", arguments->debugFlag);


  if (arguments->repeat >= 2) {
    if (arguments->beginTime == 0) {
      tdError("conflict option --repeat >= 2 but beginTime not specified\n");
      return -1;
    }
    if (arguments->endTime == 0) {
      tdError("conflict option --repeat >= 2 but endTime not specified\n");
      return -1;
    }
    if (arguments->generateTime == 0) {
      tdError("conflict option --repeat >=2 but generateTime not specified\n");
      return -1;
    }
  }

  if (arguments->repeat != 0) {
    if (arguments->generateTime != 0) {
      if (arguments->beginTime == 0) {
        tdError("conflict option --generateTime != 0 but beginTime not specified\n");
        return -1;
      }
      if (arguments->endTime == 0) {
        tdError("conflict option --generateTime != 0 but endTime not specified\n");
        return -1;
      }
    }
  }

  return 0;
}

typedef struct {
  FILE* fp;
  bool used;
  int batch;
  int printSize;
} DataFp;

// output
void *tdTableHash = NULL;
char **tdCsvFiles = NULL;
int32_t tdCsvFileNum = 0;
int64_t tdTotalRows = 0;
int32_t tdTotalTables = 0;
DataFp  *tdDataFps = NULL;
FILE   *tdTableFp = NULL;
int32_t tdDays[MAX_DAYS] = {0};

typedef struct {
  int tableId;
  int parseRows;
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

int tdGetDirectoryFileNum(const char *directoryName, const char *prefix)
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

void tdParseDirectory(const char *directoryName, const char *prefix, char **fileArray, int totalFiles)
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

const char* tdGenerateTime(int64_t timeMs)
{
  time_t tt = timeMs / 1000;
  static char buf[25] = { 0 };
  struct tm  *ptm;
  ptm = localtime(&tt);
  strftime(buf, 64, "%Y-%m-%d %H:%M:%S", ptm);
  return buf;
}

bool tdReformatTime(const char *timeString, int64_t *timeVal)
{
  struct tm tm = { 0 };
  //char* str = strptime(timeString, "%Y-%m-%d %H:%M:%S", &tm);

  //01-MAY-19 09.00.08.000000 AM
  char* str = strptime(timeString, "%d-%b-%y %l.%M.%S.000000 %p", &tm);
  if (str == NULL) return false;


  int64_t seconds = mktime(&tm);
  int64_t fraction = 0;

  char* sep = strstr(timeString, ".");
  if (sep != NULL && *sep == '.') {
    fraction = strnatoi(sep + 1, 3);
  }

  *timeVal = 1000 * seconds + fraction;
  if (TIME_UNIT == 1000000) {
    *timeVal *= 1000;
  }

  return true;
}

void tdSortCsvFiles()
{
  int64_t *fileDate = calloc(tdCsvFileNum, sizeof(int64_t));

  for (int i = 0; i < tdCsvFileNum; ++i) {
    char *fileName = tdCsvFiles[i];
    int len = (int)strlen(fileName);
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

void tdMallocCsvFiles()
{
  tdCsvFiles = (char**)calloc(tdCsvFileNum, sizeof(char*));
  for (int i = 0; i < tdCsvFileNum; i++) {
    tdCsvFiles[i] = calloc(1, TSDB_FILENAME_LEN);
  }
}

void tdGetDirectoryFileList()
{
  struct stat fileStat;
  if (stat(tdArgs.inputDir, &fileStat) < 0) {
    tdError("%s not exist", tdArgs.inputDir);
    exit(0);
  }

  if (fileStat.st_mode & S_IFDIR) {
    tdCsvFileNum = tdGetDirectoryFileNum(tdArgs.inputDir, "csv");
    tdMallocCsvFiles();
    tdParseDirectory(tdArgs.inputDir, "csv", tdCsvFiles, tdCsvFileNum);
    //tdSortCsvFiles();
    tdPrint("start to dispose %d files in %s", tdCsvFileNum, tdArgs.inputDir);
  }
  else {
    tdCsvFileNum = 1;
    tdCsvFiles = (char**)calloc(tdCsvFileNum, sizeof(char*));
    tdCsvFiles[0] = tdArgs.inputDir;
    tdPrint("start to dispose %s", tdArgs.inputDir);
  }
}

void tdParseDataLine(char *line, int lineNum, char *csvfile)
{
  int fieldPos = -1;
  int columnPos = -1;
  TdRow row;
  row.parsedOk = false;

  int len = 0;
  while (line[len] != 0) {
    if (line[len] == '\"') {
      if (fieldPos == -1) {
        fieldPos = len + 1;
      } else {
        line[len] = 0;
        char *field = line + fieldPos;
        fieldPos = -1;
        columnPos++;
        switch (columnPos) {
          case 1:
            row.VEHICLE_ID = field;
            break;
          case 3:
            row.DX = field;
            break;
          case 4:
            row.DY = field;
            break;
          case 5:
            row.SPEED = field;
            break;
          case 6:
            row.RECORD_TIME = field;
            break;
          case 7:
            row.DIRECTION = field;
            break;
          case 8:
            row.HIGH_LEVEL = field;
            break;
          case 9:
            row.ALARM_TAG = field;
            break;
          case 10:
            row.STATUS = field;
            break;
          case 11:
            row.ORG_ID = field;
            break;
          case 12:
            row.parsedOk = true;
            break;
          default:
            break;
        }
      }
    }
    len ++;
  }

  if (!row.parsedOk) {
    return;
  }

  if (row.RECORD_TIME[0] == 0) {
    return;
  }
  if (row.VEHICLE_ID[0] == 0) {
    return;
  }

  if (row.DX[0] == 0) { row.DX = "NULL"; }
  if (row.DY[0] == 0) { row.DY = "NULL"; }
  if (row.SPEED[0] == 0) { row.SPEED = "NULL"; }
  if (row.DIRECTION[0] == 0) { row.DIRECTION = "NULL"; }
  if (row.HIGH_LEVEL[0] == 0) { row.HIGH_LEVEL = "NULL"; }
  if (row.ALARM_TAG[0] == 0) { row.ALARM_TAG = "NULL"; }
  if (row.STATUS[0] == 0) { row.STATUS = "NULL"; }
  if (row.ORG_ID[0] == 0) { row.ORG_ID = "NULL"; }

  bool parseTime = tdReformatTime(row.RECORD_TIME, &row.RECORD_TIME_TS);
  if (!parseTime) {
    tdError("file:%s table:%s timestamp:%s parse failed", csvfile, row.VEHICLE_ID, row.RECORD_TIME);
    return;
  }

  if (tdArgs.beginTime != 0 && row.RECORD_TIME_TS < tdArgs.beginTime) {
    return;
  }

  if (tdArgs.endTime != 0 && row.RECORD_TIME_TS > tdArgs.endTime) {
    return;
  }

  if (tdArgs.generateTime != 0) {
    row.RECORD_TIME_TS = row.RECORD_TIME_TS - tdArgs.beginTime + tdArgs.generateTime;
  }

  if (tdArgs.repeat == 0) {
    int index = (row.RECORD_TIME_TS - BEGIN_DAYS) / INTERVAL_DAYS;
    if (index < 0 || index > MAX_DAYS) {
      return ;
    }
    tdDays[index] ++;
    return;
  }

  int tbnameLen = strlen(row.VEHICLE_ID);
  for (int i = 0; i < tbnameLen; ++i) {
    row.VEHICLE_ID[i] = tolower(row.VEHICLE_ID[i]);
  }

  TdTable *table = taosGetStrHashData(tdTableHash, row.VEHICLE_ID);
  if (table == NULL) {
    TdTable newTable = {tdTotalTables++, 0};
    table = taosAddStrHash(tdTableHash, row.VEHICLE_ID, (char*)(&newTable));
    fprintf(tdTableFp, "create table %s%s using %s tags('%s');\n", tdArgs.prefix, row.VEHICLE_ID, tdArgs.stable, row.ORG_ID);
  }
  if (table == NULL) {
    tdError("file:%s table:%s add failed", csvfile, row.VEHICLE_ID);
    exit(0);
  }

  table->parseRows++;
  int fileIndex = table->tableId % tdArgs.numOfFiles;

  if (!tdDataFps[fileIndex].used) {
    tdDataFps[fileIndex].used = true;
    fprintf(tdDataFps[fileIndex].fp, "use %s", tdArgs.database);
  }

  if (tdDataFps[fileIndex].printSize == 0) {
    tdDataFps[fileIndex].printSize = fprintf(tdDataFps[fileIndex].fp, ";\nimport into");
  }

  tdDataFps[fileIndex].batch++;
  tdDataFps[fileIndex].printSize += fprintf(tdDataFps[fileIndex].fp, " %s%s values(%ld,%s,%s,%s,%s,%s,%s,%s)",
                           tdArgs.prefix, row.VEHICLE_ID,
                           row.RECORD_TIME_TS,
                           row.DX, row.DY, row.SPEED, row.DIRECTION, row.HIGH_LEVEL, row.ALARM_TAG, row.STATUS);

  if (tdArgs.batch == 0 && tdDataFps[fileIndex].printSize > COMMAND_SIZE) {
    tdDataFps[fileIndex].printSize = 0;
    tdDataFps[fileIndex].batch = 0;
  }

  if (tdArgs.batch != 0 && tdDataFps[fileIndex].batch >= tdArgs.batch) {
    tdDataFps[fileIndex].printSize = 0;
    tdDataFps[fileIndex].batch = 0;
  }
}

void tdParseCsvFile(char *csvfile) {
  FILE *fp = fopen(csvfile, "r");
  if (fp == NULL) {
    tdError("failed to open file:%s, error:%s", csvfile, strerror(errno));
    exit(0);
  }

  char *line = NULL;
  size_t len = 0;
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
    int ret = getline(&line, &len, fp);
    if (line == NULL || ret == -1 || len == 0) {
      tdPrint("file:%s read finished, totallines:%d", csvfile, lineNum);
      break;
    }

    tdParseDataLine(line, lineNum, csvfile);
    lineNum++;
  } while (true);

  tdTotalRows += lineNum;
  fclose(fp);
}

void tdParseData() {
  tdGetDirectoryFileList();
  for (int i = 0; i < tdCsvFileNum; ++i) {
    char *csvfile = tdCsvFiles[i];
    tdPrint("parse file:%s, index:%d", csvfile, i + 1);
    tdParseCsvFile(csvfile);
  }
}

void tdInitResources() {
  tdTableHash = taosInitStrHash(MAX_TABLES, sizeof(TdTable), taosHashStringStep1);

  if (tdArgs.repeat != 0) {
    tdDataFps = (DataFp*)calloc(tdArgs.numOfFiles, sizeof(DataFp));
    for (int f = 0; f < tdArgs.numOfFiles; ++f) {
      char fileName[TSDB_FILENAME_LEN] = {0};
      sprintf(fileName, "%s/d%d.sql", tdArgs.outputDir, f);
      tdDataFps[f].fp = fopen(fileName, "w");
      if (tdDataFps[f].fp == NULL) {
        tdError("failed to open file:%s, error:%s", fileName, strerror(errno));
        exit(0);
      }
    }

    char fileName[TSDB_FILENAME_LEN] = {0};
    sprintf(fileName, "%s/tables.sql", tdArgs.outputDir);
    tdTableFp = fopen(fileName, "w");
    if (tdTableFp == NULL) {
      tdError("failed to open file:%s, error:%s", fileName, strerror(errno));
      exit(0);
    }

    if (TIME_UNIT == 1000000) {
      fprintf(tdTableFp, "create database if not exists %s ablocks %d cache %d precision 'us';\n", tdArgs.database, tdArgs.ablocks, tdArgs.cache);
    } else {
      fprintf(tdTableFp, "create database if not exists %s ablocks %d cache %d;\n", tdArgs.database, tdArgs.ablocks, tdArgs.cache);
    };
    fprintf(tdTableFp, "use %s;\n", tdArgs.database);
    fprintf(tdTableFp, "create table if not exists %s (RECORD_TIME TIMESTAMP, DX DOUBLE, DY DOUBLE, SPEED FLOAT, "
                       "DIRECTION SMALLINT, HIGH_LEVEL FLOAT, ALARM_TAG BOOL, STATUS BIGINT) tags(ORG_ID BINARY(32));\n",
                       tdArgs.stable);
  }
}

void tdCloseResources() {
  if (tdArgs.repeat != 0) {
    for (int f = 0; f < tdArgs.numOfFiles; ++f) {
      if (tdDataFps[f].used) {
        fprintf(tdDataFps[f].fp, ";\n");
      }
      fclose(tdDataFps[f].fp);
    }
    fclose(tdTableFp);
  }
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

  tdInitResources();

  int loopTimes = tdArgs.repeat;
  loopTimes = MAX(loopTimes, 1);
  for (int i = 0; i < loopTimes; ++i) {
    tdParseData();
    tdPrint("repeat:%d finished", i);
    tdArgs.generateTime += (tdArgs.endTime - tdArgs.beginTime);
  }

  tdCloseResources();

  int64_t end = taosGetTimestampMs();

  tdPrint("parse %d files in %s, find %d tables, total %ld rows, repeat:%d, time spent: %.2f seconds",
      tdCsvFileNum, tdArgs.inputDir, tdTotalTables, tdTotalRows, tdArgs.repeat, (end - start) / 1000.0);

  if (tdArgs.repeat != 0) {
    tdPrint("generate %d files in %s", tdArgs.numOfFiles, tdArgs.outputDir);
  } else {
    for (int64_t i = 0; i < MAX_DAYS; ++i) {
      if (tdDays[i] != 0) {
        int64_t ts = i * INTERVAL_DAYS + BEGIN_DAYS;
        const char *days = tdGenerateTime(ts);
        tdPrint("%s rows:%d", days, tdDays[i]);
      }
    }
  }

  return 0;
}
