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
#include "tglobal.h"
#include "tlog.h"
#include "ihash.h"
#include "shash.h"
#include "taosdef.h"
#include "tmsg.h"
#include "tutil.h"

//
// log functions
// 

#define gsError(...) taosPrintLog("ERROR ", DEBUG_ERROR, 199, __VA_ARGS__); 
#define gsWarn(...)  taosPrintLog("WARN  ", DEBUG_WARN, 199, __VA_ARGS__); 
#define gsPrint(...) taosPrintLog("INFO  ", DEBUG_INFO, 199, __VA_ARGS__); 
#define gsDump(...)  taosPrintLongString("ERROR ", DEBUG_ERROR, 199, __VA_ARGS__); 

#define GS_ARG_MAX_LEN 100


//
// util functions
// 

void gsExit(int code)
{
  exit(code);
}

#if !defined(_WIN32) && !defined(_WIN64) 

int gsGetDirectoryFileNum(const char *directoryName, const char *prefix)
{
  char cmd[1024] = { 0 };
  sprintf(cmd, "ls %s/*.%s | wc -l ", directoryName, prefix);

  FILE *fp = popen(cmd, "r");
  if (fp == NULL) {
    gsError("failed to execute:%s, error:%s", cmd, strerror(errno));
    gsExit(EXIT_FAILURE);
  }

  int fileNum = 0;
  if (fscanf(fp, "%d", &fileNum) != 1) {
    gsError("failed to execute:%s, parse result error", cmd);
    gsExit(EXIT_FAILURE);
  }

  if (fileNum <= 0) {
    gsError("directory:%s is empry", directoryName);
    gsExit(EXIT_FAILURE);
  }

  pclose(fp);
  return fileNum;
}

void gsParseDirectory(const char *directoryName, const char *prefix, char **fileArray, int totalFiles)
{
  char cmd[1024] = { 0 };
  sprintf(cmd, "ls %s/*.%s | sort", directoryName, prefix);

  FILE *fp = popen(cmd, "r");
  if (fp == NULL) {
    gsError("failed to execute:%s, error:%s", cmd, strerror(errno));
    gsExit(EXIT_FAILURE);
  }

  int fileNum = 0;
  while (fscanf(fp, "%s", fileArray[fileNum++])) {
    if (fileNum >= totalFiles) {
      break;
    }
  }

  if (fileNum != totalFiles) {
    gsError("directory:%s changed while read", directoryName);
    gsExit(EXIT_FAILURE);
  }

  pclose(fp);
}

#else

int gsGetDirectoryFileNum(const char *directoryName, const char *prefix)
{
  return 10;
}

void gsParseDirectory(const char *directoryName, const char *prefix, char **fileArray, int totalFiles)
{
  fileArray[0] = "GW20000120170809.csv";
  fileArray[1] = "GW20000120170810.csv";
  fileArray[2] = "GW20000120170811.csv";
  fileArray[3] = "GW20000120170812.csv";
  fileArray[4] = "GW20000120170813.csv";
  fileArray[5] = "GW20000220170806.csv";
  fileArray[6] = "GW20000220170807.csv";
  fileArray[7] = "GW20000220170808.csv";
  fileArray[8] = "GW20000220170809.csv";
  fileArray[9] = "GW20000220170810.csv";
}

#endif

enum {
  GS_ARG_TYPE_BOOL,
  GS_ARG_TYPE_INT,
  GS_ARG_TYPE_FLOAT,
  GS_ARG_TYPE_STRING
};

typedef struct {
  const char *name;
  const char *note;
  void       *ptr;
  int         ptrType;
  bool        isFlag;
  int         minVal;
  int         maxVal;
  int         strLen;
} GsArg;

int gsArgLen = 0;
GsArg gsArgs[GS_ARG_MAX_LEN];
char *gsPrograms = "";

void gsAddArgs(const char *name, void *ptr, int ptrType, bool isFlag, int minVal, int maxVal, int strLen, const char *note)
{
  if (gsArgLen >= GS_ARG_MAX_LEN) {
    gsError("too many arguments:%d max:%d", gsArgLen, GS_ARG_MAX_LEN);
    gsExit(EXIT_FAILURE);
  }

  GsArg *arg = &gsArgs[gsArgLen++];
  arg->name = name;
  arg->note = note;
  arg->ptr = ptr;
  arg->ptrType = ptrType;
  arg->isFlag = isFlag;
  arg->minVal = minVal;
  arg->maxVal = maxVal;
  arg->strLen = strLen;
}

void gsPrintArgs()
{
  char indent[] = "        ";
  printf("%s\n", gsPrograms);

  for (int i = 0; i < gsArgLen; ++i) {
    GsArg *arg = &gsArgs[i];
    printf("%s%s\n", indent, arg->name);
    printf("%s%s%s\n", indent, indent, arg->note);
  }

  gsExit(EXIT_SUCCESS);
}

void gsParseArgs(int argc, char *argv[])
{
  for (int i = 0; i < argc; ++i) {
    for (int j = 0; j < gsArgLen; ++j) {
      GsArg *arg = &gsArgs[j];
      if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
        gsPrintArgs();
      }

      if (strcmp(argv[i], arg->name) != 0) {
        continue;
      }

      if (arg->isFlag) {
        *((bool*)arg->ptr) = true;
        break;
      }

      if (i >= argc - 1) {
        fprintf(stderr, "option %s requires an argument\n", arg->name);
        gsExit(EXIT_FAILURE);
      }

      char *tmp = argv[++i];
      if (tmp[0] == '-') {
        fprintf(stderr, "option %s requires an argument\n", arg->name);
        gsExit(EXIT_FAILURE);
      }

      switch (arg->ptrType) {
      case GS_ARG_TYPE_BOOL:
        *((bool*)arg->ptr) = atoi(tmp) != 0;
        break;
      case GS_ARG_TYPE_INT:
        *((int*)arg->ptr) = atoi(tmp);
        break;
      case GS_ARG_TYPE_FLOAT:
        *((float*)arg->ptr) = (float)atof(tmp);
        break;
      case GS_ARG_TYPE_STRING:
        strncpy((char*)arg->ptr, tmp, arg->strLen);
        break;
      default:
        fprintf(stderr, "option %s is a invalid type:%d\n", arg->name, arg->ptrType);
        gsExit(EXIT_FAILURE);
      }

      break;
    }
  }
}

const char* gsGenerateTime(int64_t timeMs)
{
  time_t tt = timeMs / 1000;
  static char buf[25] = { 0 };
  struct tm  *ptm;
  ptm = localtime(&tt);
  strftime(buf, 64, "%Y-%m-%d %H:%M:%S", ptm);
  return buf;
}

bool gsParseTime(const char *timeString, int64_t *timeVal)
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

//
// logic structs
//

#define GS_CSV_FILE_LEN      256
#define GS_TMP_FILE_LEN      256
#define GS_CACHE_SIZE        256
#define GS_CACHE_BLOCK_SIZE  65536
#define GS_MAX_TABLES        5000    
#define GS_SLEEP_INTERVAL    10

typedef struct {
  pthread_t threadID;
  int       threadIndex;
  void     *taos;
} GsThread;

typedef struct {
  char    **cache;
  int      *cacheInserts;
  char    **cacheFileName;
  int       cacheRows;  //rows in a single cache
  int       cachePos;   //write position in a single cache
  int       writePos;
  int       readPos;
  int       tableId;
  GsThread *pThread;
  int       parseRows;
  int       insertRows;
  int       failedRows;
  int       errorRows;
  int64_t   lastTimestamp;
  char     *lastFile;
  int       lastLine;
} GsTable;

//argument
char gsCsvFileName[256] = "./";
char gsDatabaseName[32] = "db";
char gsMetricsName[32] = "mt";
bool gsCreateTable = true;  //false
bool gsInsertData = true;
bool gsOldData = false;
bool gsContainSchema = false;
int  gsReplica = 1;
int  gsThreadNum = 5;
int  gsInsertBatchNum = 70;
char gsUserName[32] = { 0 };
char gsUserPass[32] = { 0 };
char gsHostIp[20] = { 0 };
char gsLogSuccessFileName[256] = "success.log";
char gsLogFailedFileName[256] = "failed.log";
char gsDataFailedFileName[256] = "failed.data";
char gsTablePrefix[16] = "t";

int  gsAblocks = 40;
int  gsTblocks = 500;
int  gsCache = 1024 * 400; //512K
int  gsRows = 20000;      //2^24 / 900
int  gsMaxTables = 50;
bool gsSort = true;

//connection string
char *gsConnectHost = NULL;
char *gsConnectUser = NULL;
char *gsConnectPwd = NULL;

//parse from -f
char **gsCsvFiles = 0;
int    gsCsvFileNum = 0;

//global variable
void    *gsTaos = NULL;
int      gsColumnSize = 0;
GsThread*gsThreads = NULL;
GsTable *gsTables[GS_MAX_TABLES] = { 0 };
void    *gsTableHash = NULL;
void    *gsFailedFileHash = NULL;
int      gsTableNum = 0;
bool     gsParseFinished = false;
FILE    *gsLogSuccessFileFp = NULL;
FILE    *gsLogFailedFileFp = NULL;
FILE    *gsDataFailedFileFp = NULL;
pthread_mutex_t gsRecordMutex;

//
// logic functions
//

void gsWait(GsTable *table)
{
  while (true) {
    int interval = table->writePos - table->readPos;
    if (interval < GS_CACHE_SIZE) {
      break;
    }
    else {
      taosMsleep(GS_SLEEP_INTERVAL);
    }
  }
}

void gsInit(int argc, char **argv)
{
  gsPrograms = "taosGoldWind import data from CSV files to TDengine, version 1.4.4.";

  gsAddArgs("-ablocks",     &gsAblocks,           GS_ARG_TYPE_INT,    false, 1,   100000,    0,   "Max cache blocks per vnode, used by create database statement, default is 40");
  gsAddArgs("-batch",       &gsInsertBatchNum,    GS_ARG_TYPE_INT,    false, 1,   3000,      0,   "How many rows per insert batch, default is 700");
  gsAddArgs("-cache",       &gsCache,             GS_ARG_TYPE_INT,    false, 100, 104857600, 0,   "Cache block size of vnode, should large than rowsize*2048, default is 4M");
  gsAddArgs("-configDir",   configDir,            GS_ARG_TYPE_STRING, false, 0,   0,         128, "Configuration directory");
  gsAddArgs("-d",           gsDatabaseName,       GS_ARG_TYPE_STRING, false, 0,   0,         32,  "Database used to create table or import data, default is db");
  gsAddArgs("-f",           gsCsvFileName,        GS_ARG_TYPE_STRING, false, 0,   0,         256, "CSV file name or directory, default is ./");
  gsAddArgs("-host",        gsHostIp,             GS_ARG_TYPE_STRING, false, 0,   0,         20,  "TDEngine server IP address to connect, default is localhost");
  gsAddArgs("-import",      &gsOldData,           GS_ARG_TYPE_BOOL,   true,  0,   0,         0,   "The provided file is history data");
  gsAddArgs("-ls",          gsLogSuccessFileName, GS_ARG_TYPE_STRING, false, 0,   0,         255, "Log file name of successed files, default is success.log");
  gsAddArgs("-lf",          gsLogFailedFileName,  GS_ARG_TYPE_STRING, false, 0,   0,         255, "Log file name of failed files, default is failed.log");
  gsAddArgs("-ld",          gsDataFailedFileName, GS_ARG_TYPE_STRING, false, 0,   0,         255, "Log file name of failed data, default is failed.data");
  gsAddArgs("-m",           gsMetricsName,        GS_ARG_TYPE_STRING, false, 0,   0,         32,  "Metrics used to create table, default is mt");
  gsAddArgs("-pass",        gsUserPass,           GS_ARG_TYPE_STRING, false, 0,   0,         32,  "The password to use when connecting to the server, default is taosdata");
  gsAddArgs("-r",           &gsReplica,           GS_ARG_TYPE_INT,    false, 1,   3,         0,   "Replica of Database, default is 3");
  gsAddArgs("-rows",        &gsRows,              GS_ARG_TYPE_INT,    false, 200, 500000,     0,   "Rows of blocks in file per database, used by create database statement, default is 10000");
  gsAddArgs("-s",           &gsContainSchema,     GS_ARG_TYPE_BOOL,   true,  0,   0,         0,   "The provided file contain schema definition at 2rd column");
  gsAddArgs("-sort",        &gsSort,              GS_ARG_TYPE_BOOL,   true,  0,   0,         0,   "Whether to sort file before insert");
  gsAddArgs("-tables",      &gsMaxTables,         GS_ARG_TYPE_INT,    false, 5,   220000,    0,   "Max tables per database, used by create database statement, default is 100");
  gsAddArgs("-tblocks",     &gsTblocks,           GS_ARG_TYPE_INT,    false, 20,  100000,    0,   "Max cache blocks can be used by a table, used by create database statement, default is 500");
  gsAddArgs("-tablePrefix", gsTablePrefix,        GS_ARG_TYPE_STRING, false, 0,   0,         16,  "Table prefixs, default is t");
  gsAddArgs("-threadNum",   &gsThreadNum,         GS_ARG_TYPE_INT,    false, 1,   30,        0,   "How many threads used to insert data, default is 5");
  gsAddArgs("-user",        gsUserName,           GS_ARG_TYPE_STRING, false, 0,   0,         32,  "The TDEngine user name to use when connecting to the server, default is root");
 
  gsParseArgs(argc, argv);

  gsLogSuccessFileFp = fopen(gsLogSuccessFileName, "w");
  if (gsLogSuccessFileFp == NULL) {
    fprintf(stderr, "Create success log file:%s failed", gsLogSuccessFileName);
    gsExit(EXIT_FAILURE);
  }

  gsLogFailedFileFp = fopen(gsLogFailedFileName, "w");
  if (gsLogFailedFileFp == NULL) {
    fprintf(stderr, "Create failed log file:%s failed", gsLogFailedFileName);
    gsExit(EXIT_FAILURE);
  }

  gsDataFailedFileFp = fopen(gsDataFailedFileName, "w");
  if (gsDataFailedFileFp == NULL) {
    fprintf(stderr, "Create failed data file:%s failed", gsDataFailedFileName);
    gsExit(EXIT_FAILURE);
  }
}

void gsConnectTDengine()
{
  taos_init();
  
  gsConnectHost = NULL;
  if (strlen(gsHostIp) != 0) {
    gsConnectHost = gsHostIp;
  }

  gsConnectUser = tsDefaultUser;
  if (strlen(gsUserName) != 0) {
    gsConnectUser = gsUserName;
  }

  gsConnectPwd = tsDefaultPass;
  if (strlen(gsUserPass) != 0) {
    gsConnectPwd = gsUserPass;
  }
  
  gsTaos = taos_connect(gsConnectHost, gsConnectUser, gsConnectPwd, NULL, 0);
  if (gsTaos == NULL) {
    gsError("failed connect to TDengine, error:%s", taos_errstr(gsTaos));
    gsExit(EXIT_FAILURE);
  }

  gsPrint("connect to TDengine success");
}

void gsRecordFaileSql(char *sql, int len)
{
  pthread_mutex_lock(&gsRecordMutex);

  char *import = sql;
  import[0] = 'i';
  import[1] = 'm';
  import[2] = 'p';
  import[3] = 'o';
  import[4] = 'r';
  import[5] = 't';

  char *tmp = strstr(sql, "(");
  if (tmp == NULL) {
    pthread_mutex_unlock(&gsRecordMutex);
    return;
  }
  *tmp = 0;
  tmp++;

  for (int i = 0; i < len; ++i) {
    char *str = strstr(tmp, ")");
    if (str != NULL) {
      str++;
      *str = 0;
      fprintf(gsDataFailedFileFp, "%s(%s ;\n", import, tmp);
      tmp = str + 1;
    }
    else {
      break;
    }
  }

  pthread_mutex_unlock(&gsRecordMutex);
}

void gsSortCsvFiles()
{
  int64_t *fileDate = calloc(gsCsvFileNum, sizeof(int64_t));

  for (int i = 0; i < gsCsvFileNum; ++i) {
    char *fileName = gsCsvFiles[i];
    int len = (int)strlen(fileName);
    if (len < 15) {
      gsError("invalid file name:%s length should large than 15", fileName);
      gsExit(EXIT_FAILURE);
    }
    fileDate[i] = strnatoi(fileName + (len - 12), 8);
    //gsPrint("%s, %ld", fileName, fileDate[i]);
  }

  for (int i = 0; i < gsCsvFileNum; ++i) {
    for (int j = i + 1; j < gsCsvFileNum; ++j) {
      if (fileDate[i] > fileDate[j]) {
        int64_t tmp = fileDate[i];
        fileDate[i] = fileDate[j];
        fileDate[j] = tmp;

        char *tmpFile = gsCsvFiles[i];
        gsCsvFiles[i] = gsCsvFiles[j];
        gsCsvFiles[j] = tmpFile;
      }
    }
  }

  taosMemoryFree(fileDate);
}

void gsMallocCsvFiles()
{
  gsCsvFiles = (char**)calloc(gsCsvFileNum, sizeof(char*));
  for (int i = 0; i < gsCsvFileNum; i++) {
    gsCsvFiles[i] = calloc(1, GS_CSV_FILE_LEN);
  }
}

void gsParseFile()
{
  struct stat fileStat;
  if (stat(gsCsvFileName, &fileStat) < 0) {
    gsError("%s not exist", gsCsvFileName);
#if !defined(_WIN32) && !defined(_WIN64) 
    gsExit(EXIT_FAILURE);
#endif
  }

  if (fileStat.st_mode & S_IFDIR) {
    gsCsvFileNum = gsGetDirectoryFileNum(gsCsvFileName, "csv");
    gsMallocCsvFiles();
    gsParseDirectory(gsCsvFileName, "csv", gsCsvFiles, gsCsvFileNum);
    if (gsSort)
      gsSortCsvFiles();
    gsPrint("start to dispose %d files in %s", gsCsvFileNum, gsCsvFileName);
  }
  else {
    gsCsvFileNum = 1;
    gsCsvFiles = (char**)calloc(gsCsvFileNum, sizeof(char*));
    gsCsvFiles[0] = gsCsvFileName;
    gsPrint("start to dispose %s", gsCsvFileName);
  }
}

int gsSplitLine(char *line, char**columns)
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

void gsParseSchemaLine(char *line, char *schemaLine)
{
  char *column_names[270] = { 0 };
  int column_size = gsSplitLine(line, column_names);
  if (gsColumnSize != 0) {
    if (column_size != gsColumnSize) {
      gsError("column size:%d not matched with previous:%d, line:%s", column_size, gsColumnSize, line);
      gsExit(EXIT_FAILURE);
    }
    return;
  }

  gsColumnSize = column_size;
  gsPrint("column size is %d, tag size is 2", gsColumnSize);

  char *column_types[270] = { 0 };
  if (schemaLine != NULL) {
    int tmp = gsSplitLine(schemaLine, column_types);
    if (tmp != gsColumnSize) {
      gsError("wrong file format, schema columns:%d not equal with name columns:%d", tmp, gsColumnSize);
      gsExit(EXIT_FAILURE);
    }
  }
  else {
    column_types[0] = "timestamp";
    for (int i = 1; i < column_size; ++i) {
      column_types[i] = "float";
    }
  }

  char sql[20480];
  int len = sprintf(sql, "create table %s (ts timestamp", gsMetricsName);
  for (int i = 3; i < column_size; ++i) {
    len += sprintf(sql + len, ",%s %s", column_names[i], column_types[i]);
  }  
  sprintf(sql + len, "%s", ") tags(wfid int, wtid int)");
  
  if (gsCreateTable) {
    char qstr[128] = { 0 };
    sprintf(qstr, "create database %s replica %d rows %d cache %d ablocks %d tblocks %d tables %d", gsDatabaseName, gsReplica, gsRows, gsCache, gsAblocks, gsTblocks, gsMaxTables);
    int code = taos_query(gsTaos, qstr);
    if (code == TSDB_CODE_DB_ALREADY_EXIST) {
      gsPrint("database:%s already exist", gsDatabaseName);
    }
    else if (code != 0) {
      gsError("failed to create database:%s, code:%d, error:%s, sql:%s", gsDatabaseName, code, taos_errstr(gsTaos), qstr);
      gsExit(EXIT_FAILURE);
    }
    else {
      gsPrint("create database:%s finished", gsDatabaseName);
    }

    sprintf(qstr, "use %s", gsDatabaseName);
    code = taos_query(gsTaos, qstr);
    if (code != 0) {
      gsError("failed to use database:%s, code:%d, error:%s, sql:%s", gsDatabaseName, code, taos_errstr(gsTaos), qstr);
      gsExit(EXIT_FAILURE);
    }

    code = taos_query(gsTaos, sql);
    if (code == TSDB_CODE_TABLE_ALREADY_EXIST) {
      gsPrint("metrics:%s.%s already exist", gsDatabaseName, gsMetricsName);
      sprintf(sql, "describe %s", gsMetricsName);
      taos_query(gsTaos, sql);
      void *result = taos_use_result(gsTaos);
      if (result == NULL) {
        gsError("get metrics fields failure, code:%d, error:%s, sql:%s", code, taos_errstr(gsTaos), sql);
        gsExit(EXIT_FAILURE);
      }

      TAOS_ROW row;
      int numOfRows = 0;
      while ((row = taos_fetch_row(result)))
      {
        numOfRows++;
      }
      taos_free_result(result);

      if (numOfRows != gsColumnSize) {
        gsError("exist metrics:%s fields:%d not equal with fields:%d from file", gsMetricsName, numOfRows, gsColumnSize);
        gsExit(EXIT_FAILURE);
      }
    }
    else if (code != 0) {
      gsError("failed to create metrics:%s.%s, code:%d, error:%s, sql:%s", gsDatabaseName, gsMetricsName, code, taos_errstr(gsTaos), sql);
      gsExit(EXIT_FAILURE);
    }
    else {
      gsPrint("create metrics:%s.%s finished", gsDatabaseName, gsMetricsName);
    }
  }
}

GsTable *gsCreateNewTable(int tableId, int wfid)
{  
  GsTable tmp;
  GsTable *table = (GsTable *)taosAddIntHash(gsTableHash, tableId, (char*)(&tmp));  
  table->cache = (char**)calloc(GS_CACHE_SIZE, sizeof(char*));
  table->cacheInserts = (int*)calloc(GS_CACHE_SIZE, sizeof(int));
  table->cacheFileName = (char**)calloc(GS_CACHE_SIZE, sizeof(char*));
  for (int i = 0; i < GS_CACHE_SIZE; ++i) {
    table->cache[i] = (char*)calloc(1, GS_CACHE_BLOCK_SIZE);
    table->cacheInserts[i] = 0;
    table->cacheFileName[i] = 0;
  }

  table->cachePos = 0;
  table->cacheRows = 0;
  table->parseRows = 0;
  table->insertRows = 0;
  table->failedRows = 0;
  table->errorRows = 0;
  table->pThread = &gsThreads[gsTableNum % gsThreadNum];
  table->readPos = 0;
  table->writePos = 0;
  table->tableId = tableId;
  table->lastTimestamp = 0;
  table->lastFile = NULL;
  table->lastLine = 0;
  
  if (gsCreateTable) {
    char sql[1024] = { 0 };
    sprintf(sql, "create table %s%d using %s tags(%d, %d)", gsTablePrefix, tableId, gsMetricsName, wfid, tableId);
    int code = taos_query(gsTaos, sql);
    if (code == TSDB_CODE_TABLE_ALREADY_EXIST) {
      gsPrint("table:%s.%s%d already exist", gsDatabaseName, gsTablePrefix, table->tableId);
      sprintf(sql, "describe %s%d", gsTablePrefix, table->tableId);
      taos_query(gsTaos, sql);
      void *result = taos_use_result(gsTaos);
      if (result == NULL) {
        gsError("get table fields failure, code:%d, error:%s, sql:%s", code, taos_errstr(gsTaos), sql);
        gsExit(EXIT_FAILURE);
      }

      TAOS_ROW row;
      int numOfRows = 0;
      while ((row = taos_fetch_row(result)))
      {
        numOfRows++;
      }
      taos_free_result(result);

      if (numOfRows != gsColumnSize) {
        gsError("exist table:%s%d fields:%d not equal with fields:%d from file", gsTablePrefix, table->tableId, numOfRows, gsColumnSize);
        gsExit(EXIT_FAILURE);
      }
    }
    else if (code != 0) {
      gsError("failed to create table:%s.%s%d, code:%d, error:%s, sql:%s", gsDatabaseName, gsTablePrefix, tableId, code, taos_errstr(gsTaos), sql);
      gsExit(EXIT_FAILURE);
    }
    else {
      gsPrint("create table:%s.%s%d successed", gsDatabaseName, gsTablePrefix, table->tableId);
    }
  }

  //gsPrint("start insert data to table:%s.%s%d", gsDatabaseName, gsTablePrefix, table->tableId);
  gsTables[gsTableNum] = table;
  gsTableNum++;

  return table;
}

void gsParseDataLine(char *line, int lineNum, char *csvfile)
{
  int comma1 = 0;
  int comma2 = 0;
  int comma3 = 0;
  int commaNum = 0;

  int len = 0;
  while (line[len] != 0) {
    if (line[len] == ',') {
      commaNum++;
      switch (commaNum) {
      case 1:
        comma1 = len;
        break;
      case 2:
        comma2 = len;
        break;
      case 3:
        comma3 = len;
        break;
      default:
        break;
      }
    }
    //else if (line[len] == '\n' || line[len] == '\r') {
    //  line[len] = 0;
    //  break;
    //}
    len++;
  }

  if (commaNum <= 1) return;
  if (commaNum != gsColumnSize - 1) {
    gsDump("wrong line format, comma num:%d not equal with previous:%d, file:%s, line:%d, content:%s", commaNum, gsColumnSize - 1, csvfile, lineNum, line);
    return;
  }

  int tableId = atoi(&line[comma2 + 1]);
  
  GsTable *table = (GsTable*)taosGetIntHashData(gsTableHash, tableId);
  if (table == NULL) {
    int wtid = atoi(&line[comma1 + 1]);
    table = gsCreateNewTable(tableId, wtid);
  }
  
  line[comma1] = 0;
  char *ts = line;
  int64_t timeMs;
  bool validTs = gsParseTime(line, &timeMs);
  if (!validTs) {
    gsDump("invalid time format in file:%s, line:%d content:%s", csvfile, lineNum, line);
    return;
  }

  gsWait(table);

  table->parseRows++;
  int writepos = table->writePos % GS_CACHE_SIZE;
  char *data = line + comma3 + 1;
  char *cache = table->cache[writepos];
  table->cacheFileName[writepos] = csvfile;

  if (table->lastTimestamp > timeMs) {
    table->errorRows++;
    taosAddStrHash(gsFailedFileHash, csvfile, csvfile);
    gsPrint("table:%s%d file:%s line:%d %s smaller than file:%s line:%d %s, try import"
      , gsTablePrefix, tableId, csvfile, lineNum, line, table->lastFile, table->lastLine, gsGenerateTime(table->lastTimestamp));
    
    //whether import
    if (false) {
      if (table->cachePos != 0 && table->cacheRows != 0) {
        table->cachePos = 0;
        table->cacheRows = 0;
        table->writePos++;
      }

      gsWait(table);

      writepos = table->writePos % GS_CACHE_SIZE;
      char *cache = table->cache[writepos];
      table->cacheFileName[writepos] = csvfile;
      table->cachePos += sprintf(cache, "import into %s.%s%d values('%s',%s)", gsDatabaseName, gsTablePrefix, tableId, ts, data);
      table->cacheRows++;
      table->cacheInserts[writepos] ++;

      table->cachePos = 0;
      table->cacheRows = 0;
      table->writePos++;
    }
  }
  else {
    table->lastTimestamp = timeMs;
    table->lastFile = csvfile;
    table->lastLine = lineNum;
    table->cacheInserts[writepos] ++;

    if (table->cacheRows == 0) {
      table->cachePos += sprintf(cache, "%s into %s.%s%d values('%s',%s)", gsOldData ? "import" : "insert", gsDatabaseName, gsTablePrefix, tableId, ts, data);
      table->cacheRows++;
      if (gsInsertBatchNum == 1) {
        table->cachePos = 0;
        table->cacheRows = 0;
        table->writePos++;
      }
    }
    else {
      table->cachePos += sprintf(cache + table->cachePos, "('%s',%s)", ts, data);
      table->cacheRows++;
      if (table->cacheRows >= gsInsertBatchNum || table->cachePos > (GS_CACHE_BLOCK_SIZE - 5000)) {
        table->cachePos = 0;
        table->cacheRows = 0;
        table->writePos++;
      }
    }
  }
}

void gsParseCsvFile(char *csvfile)
{
  FILE *fp = fopen(csvfile, "r");
  if (fp == NULL) {
    gsError("failed to open file:%s, error:%s", csvfile, strerror(errno));
    gsExit(EXIT_FAILURE);
  }

  char line[10240] = { 0 };
  int num = fscanf(fp, "%s", line);
  if (num != 1) {
    gsError("file:%s is empty", csvfile);
    gsExit(EXIT_FAILURE);
  }

  int lineNum = 1;

  if (!gsContainSchema) {
    gsParseSchemaLine(line, NULL);
  }
  else {
    char schemaLine[10240] = { 0 };
    num = fscanf(fp, "%s", schemaLine);
    if (num != 1) {
      gsError("file:%s line small than 2", csvfile);
      gsExit(EXIT_FAILURE);
    }
    gsParseSchemaLine(line, schemaLine);
    lineNum++;
  }
 
  do {
    num = fscanf(fp, "%s%s", line, line + 11);
    if (num != 2) break;
    if (line[10] != 0) {
      gsError("file:%s invalid format, line num:%d", csvfile, lineNum);
      gsExit(EXIT_FAILURE);
      break;
    }
    line[10] = ' ';
    //gsPrint("%s", line);
    lineNum++;
    gsParseDataLine(line, lineNum, csvfile);
    
  } while (true);
  
  fclose(fp);
}

void gsParseData()
{
  for (int i = 0; i < gsCsvFileNum; ++i) {
    char *csvfile = gsCsvFiles[i];
    gsPrint("parse file:%s, index:%d", csvfile, i + 1);
    gsParseCsvFile(csvfile);

    for (int i = 0; i < gsTableNum; ++i) {
      GsTable *table = gsTables[i];
      gsWait(table);
      if (table->cachePos != 0 && table->cacheRows != 0) {
        table->cachePos = 0;
        table->cacheRows = 0;
        table->writePos++;
      }
    }
  }

  //dipose the end of file
  for (int i = 0; i < gsTableNum; ++i) {
    GsTable *table = gsTables[i];
    gsWait(table);
    if (table->cachePos != 0 && table->cacheRows != 0) {
      table->cachePos = 0;
      table->cacheRows = 0;
      table->writePos++;
      //gsPrint("table:t%d readPos:%d writePos:%d", table->tableId, table->readPos, table->writePos);
    }
  }
  gsParseFinished = true;
}

void gsThreadInsert(GsTable *table)
{
  for (; table->readPos < table->writePos; ++table->readPos) {
    int readPos = table->readPos % GS_CACHE_SIZE;
    char *sql = table->cache[readPos];
    //gsPrint("thread:%d tableId:%d readPos:%d writePos:%d cacheRows:%d, cachePos:%d, sql:%s", table->pThread->threadIndex, table->tableId, table->readPos, table->writePos, table->cacheRows, table->cachePos, sql);
    
    int code = 0;
    int affectRows = 0;
    for (int tryTimes = 0; tryTimes < 10; ++tryTimes) {
      code = taos_query(table->pThread->taos, sql);
      
      if (code != 0) {
        taosMsleep(400);
        //gsPrint("code:%d, affect_rows:%d, already insert this batch:%d", code, taos_affected_rows(table->pThread->taos), affectRows);
        affectRows += taos_affected_rows(table->pThread->taos);
        continue;
      }
	      
      affectRows += taos_affected_rows(table->pThread->taos);
      if (affectRows != table->cacheInserts[readPos]) {
        //gsPrint("code:%d, affect_rows:%d, already insert this batch:%d", code, taos_affected_rows(table->pThread->taos), affectRows);

        taosMsleep(400);
        continue;
      }

      break;
    }
    
    if (code != 0) {
      gsDump("thread:%d table:%s%d file:%s rows:%d, code:%d, error:%s, sql:%s", table->pThread->threadIndex, gsTablePrefix, table->tableId, table->cacheFileName[readPos], table->cacheInserts[readPos], code, taos_errstr(table->pThread->taos), sql);
      taosAddStrHash(gsFailedFileHash, table->cacheFileName[readPos], table->cacheFileName[readPos]);
      table->failedRows += table->cacheInserts[readPos];
    }
    else {
      //table->insertRows += affectRows;
      table->insertRows += table->cacheInserts[readPos]; //know there still a bug

      //if (affectRows != table->cacheInserts[readPos]) {
        //gsDump("thread:%d table:%s%d file:%s try insert:%d, success:%d, error:%s, sql:%s", table->pThread->threadIndex, gsTablePrefix, table->tableId, table->cacheFileName[readPos], table->cacheInserts[readPos], affectRows, taos_errstr(table->pThread->taos), sql);
        //taosAddStrHash(gsFailedFileHash, table->cacheFileName[readPos], table->cacheFileName[readPos]);
        //table->failedRows += (table->cacheInserts[readPos] - affectRows);

        //gsRecordFaileSql(sql, table->cacheInserts[readPos]);
      //}
    }
    
    table->cacheInserts[readPos] = 0;
  }
}

void *gsThreadFp(void *arg)
{
  GsThread *pThread = (GsThread*)arg;  
  while (1) {  
    for (int i = 0; i < gsTableNum; ++i) {
      GsTable *table = gsTables[i];
      if (table->pThread != pThread) continue;
      gsThreadInsert(table);
    }

    taosMsleep(GS_SLEEP_INTERVAL);

    if (gsParseFinished) {
      for (int i = 0; i < gsTableNum; ++i) {
        GsTable *table = gsTables[i];
        if (table->pThread != pThread) continue;
        gsThreadInsert(table);
      }
      break;
    }
  }

  return NULL;
}

void gsInitThread()
{
  gsThreads = (GsThread*)calloc(gsThreadNum, sizeof(GsThread));

  pthread_attr_t thattr;
  for (int i = 0; i < gsThreadNum; ++i) {
    GsThread *pThread = gsThreads + i;
    pThread->threadIndex = i;

    pThread->taos = taos_connect(gsConnectHost, gsConnectUser, gsConnectPwd, NULL, 0);
    if (pThread->taos == NULL) {
      gsError("thread:%d failed connect to TDengine, error:%s", pThread->threadIndex, taos_errstr(pThread->taos));
      gsExit(EXIT_FAILURE);
    }
    
    pthread_attr_init(&thattr);
    pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);

    if (pthread_create(&(pThread->threadID), &thattr, gsThreadFp, (void*)pThread) != 0) {
      gsError("thread:%d failed to start", pThread->threadIndex);
      gsExit(EXIT_FAILURE);
    }
  }

  gsPrint("working thread init success");
}

void gsInitHash()
{
  gsTableHash = taosInitIntHash(GS_MAX_TABLES, sizeof(GsTable), taosHashInt);
  gsFailedFileHash = taosInitStrHash(GS_MAX_TABLES, sizeof(char*), taosHashStringStep1);
}

int main(int argc, char *argv[])
{
  pthread_mutex_init(&gsRecordMutex, NULL);

  gsInit(argc, argv);

  gsConnectTDengine();  
  
  gsInitHash();

  gsInitThread();
  
  gsParseFile();

  int64_t start = taosGetTimestampMs();
  
  gsParseData();

  for (int i = 0; i < gsThreadNum; i++) {
    pthread_join(gsThreads[i].threadID, NULL);
  }

  int64_t end = taosGetTimestampMs();

  int parsedRows = 0;
  int insertRows = 0;
  int errorRows = 0;
  int failedRows = 0;
  gsPrint("parse %d files, find %d tables, database:%s, metrics:%s", gsCsvFileNum, gsTableNum, gsDatabaseName, gsMetricsName);
  for (int i = 0; i < gsTableNum; ++i) {
    GsTable *table = gsTables[i];
    parsedRows += table->parseRows;
    insertRows += table->insertRows;
    errorRows += table->errorRows;
    failedRows += table->failedRows;
    gsPrint("index:%02d, table:%s%d, parsed:%d insert:%d, failed:%d, error:%d", i, gsTablePrefix, table->tableId, table->parseRows, table->insertRows, table->failedRows, table->errorRows);
  }
  gsPrint("total %d parsed, %d inserted, %d failed, %d error, time spent: %d seconds", parsedRows, insertRows, failedRows, errorRows, (end - start) / 1000);

  for (int i = 0; i < gsThreadNum; i++) {
    taos_close(gsThreads[i].taos);
  }

  for (int i = 0; i < gsCsvFileNum; ++i) {
    char *csvfile = gsCsvFiles[i];
    if (taosGetStrHashData(gsFailedFileHash, csvfile) != NULL) {
      fprintf(gsLogFailedFileFp, "%s\n", csvfile);
    }
    else {
      fprintf(gsLogSuccessFileFp, "%s\n", csvfile);
    }
  }

  fclose(gsLogSuccessFileFp);
  fclose(gsLogFailedFileFp);
  fclose(gsDataFailedFileFp);
  pthread_mutex_destroy(&gsRecordMutex);

  gsExit(EXIT_SUCCESS);
}
