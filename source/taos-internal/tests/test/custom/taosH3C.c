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
#include "tmd5.h"

//
// log functions
// 

#define gsError(...) taosPrintLog("ERROR ", DEBUG_ERROR, 199, __VA_ARGS__); 
#define gsWarn(...)  taosPrintLog("WARN  ", DEBUG_WARN, 199, __VA_ARGS__); 
#define gsPrint(...) taosPrintLog("INFO  ", DEBUG_INFO, 199, __VA_ARGS__); 
#define gsDump(...)  taosPrintLongString("ERROR ", DEBUG_ERROR, 199, __VA_ARGS__); 

#define GS_ARG_MAX_LEN 100

//
// common
//

#define GS_CSV_FILE_LEN      256
#define GS_CACHE_SIZE        64
#define GS_CACHE_BLOCK_SIZE  65536
#define GS_MAX_TABLES        200000    
#define GS_SLEEP_INTERVAL    10

typedef struct {
  pthread_t threadID;
  int       threadIndex;
  void     *taos;
  char    **cache;
  int      *cacheInserts;
  int       cacheRows;  //rows in a single cache
  int       cachePos;   //write position in a single cache
  int       writePos;
  int       readPos;
  int       parseRows;
  int       insertRows;
  int       failedRows;
} GsThread;

typedef struct {
  char      filename[GS_CSV_FILE_LEN];
  int64_t   timestamp;
} GsFile;


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

void gsParseDirectory(const char *directoryName, const char *prefix, GsFile *fileArray, int totalFiles)
{
  char cmd[1024] = { 0 };
  sprintf(cmd, "ls %s/*.%s | sort", directoryName, prefix);

  FILE *fp = popen(cmd, "r");
  if (fp == NULL) {
    gsError("failed to execute:%s, error:%s", cmd, strerror(errno));
    gsExit(EXIT_FAILURE);
  }

  int fileNum = 0;
  while (fscanf(fp, "%s", fileArray[fileNum++].filename)) {
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
  return 4;
}

void gsParseDirectory(const char *directoryName, const char *prefix, GsFile *fileArray, int totalFiles)
{
  strcpy(fileArray[0].filename, "data/1535731200000.csv");
  strcpy(fileArray[1].filename, "data/1535731800000.csv");
  strcpy(fileArray[2].filename, "data/1535731500000.csv");
  strcpy(fileArray[3].filename, "data/1535732400000.csv");
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
  time_t tt = timeMs / 10;
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

typedef struct {
  char tag1[33];
  char tag2[33];
  char tag3[33];
  char tag4[33];
  char tag5[33];
  char tableId[33];
  int64_t timestamp;
  int value;
} GsLine;

//argument
char gsCsvFileName[256] = "./";
char gsDatabaseName[32] = "db";
char gsStablesName[32] = "st";
int  gsReplica = 1;
int  gsThreadNum = 1;
int  gsInsertBatchNum = 1000;
char gsUserName[32] = { 0 };
char gsUserPass[32] = { 0 };
char gsHostIp[20] = { 0 };
char gsTablePrefix[16] = "t";

int  gsAblocks = 4;
int  gsTblocks = 2000;
int  gsCache = 16384;   //512K
int  gsRows = 8000;     //2^24 / 900
int  gsMaxTables = 2000;

//connection string
char *gsConnectHost = NULL;
char *gsConnectUser = NULL;
char *gsConnectPwd = NULL;

//parse from -f
GsFile*gsCsvFiles = 0;
int    gsCsvFileNum = 0;

//global variable
void    *gsTaos = NULL;
GsThread*gsThreads = NULL;

void    *gsTableHash = NULL;
int      gsTableNum = 0;

bool     gsParseFinished = false;

void gsShrinkTableName(char *name, int len, char *target)
{
  for (int i = 0; i < len; i++) {
    if (name[i] == ' ' || name[i] == ':' || name[i] == '.' || name[i] == '-' || name[i] == '/' || name[i] == '\'' || name[i] == '\"' || name[i] == ',')
      name[i] = '_';
  }

  if (len < TSDB_TABLE_NAME_LEN - 1) {
    strncpy(target + 1, name, len);
    target[0] = gsTablePrefix[0];
    return;
  }

  MD5_CTX context;
  MD5Init(&context);
  MD5Update(&context, (uint8_t*)name, (uint32_t)len);
  MD5Final(&context);

  sprintf(target, "%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x"
    , context.digest[0]
    , context.digest[1]
    , context.digest[2]
    , context.digest[3]
    , context.digest[4]
    , context.digest[5]
    , context.digest[6]
    , context.digest[7]
    , context.digest[8]
    , context.digest[9]
    , context.digest[10]
    , context.digest[11]
    , context.digest[12]
    , context.digest[13]
    , context.digest[14]
    , context.digest[15]);

  target[0] = gsTablePrefix[0];
}

//
// logic functions
//

void gsWait(GsThread *thread)
{
  while (true) {
    int interval = thread->writePos - thread->readPos;
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
  
  gsAddArgs("-host",        gsHostIp,             GS_ARG_TYPE_STRING, false, 0,   0,         20,  "TDEngine server IP address to connect, default is localhost");
  gsAddArgs("-user",        gsUserName,           GS_ARG_TYPE_STRING, false, 0,   0,         32,  "The TDEngine user name to use when connecting to the server, default is root");
  gsAddArgs("-pass",        gsUserPass,           GS_ARG_TYPE_STRING, false, 0,   0,         32,  "The password to use when connecting to the server, default is taosdata");
  gsAddArgs("-configDir",   configDir,            GS_ARG_TYPE_STRING, false, 0,   0,         128, "Configuration directory");
  gsAddArgs("-threadNum",   &gsThreadNum,         GS_ARG_TYPE_INT,    false, 1,   30,        0,   "How many threads used to insert data, default is 1");
  
  gsAddArgs("-db",          gsDatabaseName,       GS_ARG_TYPE_STRING, false, 0,   0,         32,  "Database used to create table or import data, default is db");
  gsAddArgs("-cache",       &gsCache,             GS_ARG_TYPE_INT,    false, 1,   10000,     0,   "Cache block size of vnode, should large than rowsize*2048, default is 16384");
  gsAddArgs("-ablocks",     &gsAblocks,           GS_ARG_TYPE_INT,    false, 1,   100,       0,   "Average cache blocks per vnode, used by create database statement, default is 4");
  gsAddArgs("-tblocks",     &gsTblocks,           GS_ARG_TYPE_INT,    false, 20,  100000,    0,   "Max cache blocks can be used by a table, used by create database statement, default is 4000");
  gsAddArgs("-rows",        &gsRows,              GS_ARG_TYPE_INT,    false, 200, 500000,    0,   "Rows of blocks in file per database, used by create database statement, default is 10000");
  gsAddArgs("-tables",      &gsMaxTables,         GS_ARG_TYPE_INT,    false, 5,   220000,    0,   "Max tables per database, used by create database statement, default is 1000");
  gsAddArgs("-replica",     &gsReplica,           GS_ARG_TYPE_INT,    false, 1,   3,         0,   "Replica of Database, default is 1");
  
  gsAddArgs("-f",           gsCsvFileName,        GS_ARG_TYPE_STRING, false, 0,   0,         256, "Sql file name or directory, default is ./");
  gsAddArgs("-s",           gsStablesName,        GS_ARG_TYPE_STRING, false, 0,   0,         32,  "Stables used to create table, default is st");
  gsAddArgs("-t",           gsTablePrefix,        GS_ARG_TYPE_STRING, false, 0,   0,         16,  "Table prefixs, default is t");
  gsAddArgs("-b",           &gsInsertBatchNum,    GS_ARG_TYPE_INT,    false, 1,   3000,      0,   "How many rows per insert batch, default is 1000");
  
  gsParseArgs(argc, argv);
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

void gsSortCsvFiles()
{
  for (int i = 0; i < gsCsvFileNum; ++i) {
    char *fileName = gsCsvFiles[i].filename;
    int len = (int)strlen(fileName);
    if (len < 17) {
      gsError("invalid file name:%s length should large than 17", fileName);
      gsExit(EXIT_FAILURE);
    }
    gsCsvFiles[i].timestamp = strnatoi(fileName + (len - 17), 13);
    //gsPrint("%s, %ld", fileName, fileDate[i]);
  }

  for (int i = 0; i < gsCsvFileNum; ++i) {
    for (int j = i + 1; j < gsCsvFileNum; ++j) {
      if (gsCsvFiles[i].timestamp > gsCsvFiles[j].timestamp) {
        GsFile tmpFile = gsCsvFiles[i];
        gsCsvFiles[i] = gsCsvFiles[j];
        gsCsvFiles[j] = tmpFile;
      }
    }
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
    gsCsvFiles = (GsFile*)calloc(gsCsvFileNum, sizeof(GsFile));
    gsParseDirectory(gsCsvFileName, "csv", gsCsvFiles, gsCsvFileNum);
    gsSortCsvFiles();
    gsPrint("start to dispose %d files in %s", gsCsvFileNum, gsCsvFileName);
  }
  else {
    gsCsvFileNum = 1;
    gsCsvFiles = (GsFile*)calloc(gsCsvFileNum, sizeof(GsFile));
    strcpy(gsCsvFiles[0].filename, gsCsvFileName);
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

    if (line[i] == '-') {
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

void gsCreateDbAndSt()
{
  char qstr[128] = { 0 };
  sprintf(qstr, "create database if not exists %s replica %d rows %d cache %d ablocks %d tblocks %d tables %d", gsDatabaseName, gsReplica, gsRows, gsCache, gsAblocks, gsTblocks, gsMaxTables);
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

  char sql[2048];
  int len = sprintf(sql, "create table if not exists %s (ts timestamp, value int) tags(tag1 binary(10), tag2 binary(10), tag3 binary(10), tag4 binary(10), tag5 binary(10))", gsStablesName);
  code = taos_query(gsTaos, sql);
  if (code == TSDB_CODE_TABLE_ALREADY_EXIST) {
    gsPrint("stable:%s.%s already exist", gsDatabaseName, gsStablesName);
  }
  else if (code != 0) {
    gsError("failed to create stable:%s.%s, code:%d, error:%s, sql:%s", gsDatabaseName, gsStablesName, code, taos_errstr(gsTaos), sql);
    gsExit(EXIT_FAILURE);
  }
  else {
    gsPrint("create stables:%s.%s finished", gsDatabaseName, gsStablesName);
  }
}

GsThread *gsCreateNewTable(GsLine *data)
{  
  int *threadIndex = (int *)taosGetStrHashData(gsTableHash, data->tableId);
  if (threadIndex != NULL) {
    return &gsThreads[*threadIndex];
  }

  char sql[1024] = { 0 };
  sprintf(sql, "create table %s using %s tags('%s', '%s', '%s', '%s', '%s')", data->tableId, gsStablesName, data->tag1, data->tag2, data->tag3, data->tag4, data->tag5);
  int code = taos_query(gsTaos, sql);
  if (code == TSDB_CODE_TABLE_ALREADY_EXIST) {
    //gsPrint("index:%d table:%s.%s already exist", gsTableNum + 1, gsDatabaseName, data->tableId);
  }
  else if (code != 0) {
    gsError("index:%d failed to create table:%s.%s, code:%d, error:%s, sql:%s", gsTableNum + 1, gsDatabaseName, data->tableId, taos_errstr(gsTaos), sql);
    gsExit(EXIT_FAILURE);
  }
  else {
    //gsPrint("index:%d create table:%s.%s successed", gsTableNum + 1, gsDatabaseName, data->tableId);
  }

  int tmp;
  threadIndex = taosAddStrHash(gsTableHash, data->tableId, (char*)(&tmp));
  
  gsTableNum++;
  *threadIndex = gsTableNum % gsThreadNum;
 
  return &gsThreads[*threadIndex];
}

int gsParseDataSingleLine(char *line, GsLine *data)
{
  int dotPostion[6] = {0};
  int len = 0;
  int pos = 0;

  while (line[len] != 0) {
    if (pos >= 6) break;
    if (line[len] == ',') {
      dotPostion[pos++] = len;
    }
    else if (line[len] == '\n' || line[len] == '\r') {
      line[len] = 0;
      break;
    }
    len++;
  }

  if (pos != 5) {
    gsPrint("wrong line format, line:%s", line);
    return -1;
  }

  strncpy(data->tag1, line, dotPostion[0]);
  strncpy(data->tag2, line + dotPostion[0] + 1, dotPostion[1] - dotPostion[0] - 1);
  strncpy(data->tag3, line + dotPostion[1] + 1, dotPostion[2] - dotPostion[1] - 1);
  strncpy(data->tag4, line + dotPostion[2] + 1, dotPostion[3] - dotPostion[2] - 1);
  strncpy(data->tag5, line + dotPostion[3] + 1, dotPostion[4] - dotPostion[3] - 1);
  data->value = atoi(line + dotPostion[4] + 1);
  gsShrinkTableName(line, dotPostion[4], data->tableId);

  return 0;
}

void gsParseDataLine(GsLine *data, int lineNum)
{
  GsThread *thread = gsCreateNewTable(data);
  gsWait(thread);

  thread->parseRows++;
  int writepos = thread->writePos % GS_CACHE_SIZE;
  char *cache = thread->cache[writepos];
  thread->cacheInserts[writepos] ++;

  if (thread->cacheRows == 0) {
    thread->cachePos += sprintf(cache, "insert into %s.%s values(%lld,%d)", gsDatabaseName,
      data->tableId, data->timestamp, data->value);
    thread->cacheRows++;
    if (gsInsertBatchNum == 1) {
      thread->cachePos = 0;
      thread->cacheRows = 0;
      thread->writePos++;
    }
  }
  else {
    thread->cachePos += sprintf(cache + thread->cachePos, " %s.%s values(%lld,%d)", gsDatabaseName,
      data->tableId, data->timestamp, data->value);
    thread->cacheRows++;
    if (thread->cacheRows >= gsInsertBatchNum || thread->cachePos > (GS_CACHE_BLOCK_SIZE - 5000)) {
      thread->cachePos = 0;
      thread->cacheRows = 0;
      thread->writePos++;
    }
  }
}

void gsParseCsvFile(GsFile *file)
{
  FILE *fp = fopen(file->filename, "r");
  if (fp == NULL) {
    gsError("failed to open file:%s, error:%s", file->filename, strerror(errno));
    gsExit(EXIT_FAILURE);
  }

  char line[10240] = { 0 };
  int num = 0;
  int lineNum = 0;

  GsLine data;
  memset(&data, 0, sizeof(GsLine));
  do {
    num = fscanf(fp, "%s", line);
    if (num != 1) break;
    lineNum++;

    data.timestamp = file->timestamp;
    int finished = gsParseDataSingleLine(line, &data);
    if (finished == -1) {
      memset(&data, 0, sizeof(GsLine));
      break;
    }
    else {
      gsParseDataLine(&data, lineNum);
      memset(&data, 0, sizeof(GsLine));
    }
  } while (true);
  
  fclose(fp);
}

void gsParseData()
{
  for (int i = 0; i < gsCsvFileNum; ++i) {
    GsFile *csvfile = &gsCsvFiles[i];
    gsPrint("parse file:%s, index:%d", csvfile, i + 1);
    gsParseCsvFile(csvfile);

    for (int i = 0; i < gsThreadNum; ++i) {
      GsThread *thread = &gsThreads[i];
      gsWait(thread);
      if (thread->cachePos != 0 && thread->cacheRows != 0) {
        thread->cachePos = 0;
        thread->cacheRows = 0;
        thread->writePos++;
      }
    }
  }

  //dipose the end of file
  for (int i = 0; i < gsThreadNum; ++i) {
    GsThread *thread = &gsThreads[i];
    gsWait(thread);
    if (thread->cachePos != 0 && thread->cacheRows != 0) {
      thread->cachePos = 0;
      thread->cacheRows = 0;
      thread->writePos++;
    }
  }
  gsParseFinished = true;
}

void gsThreadInsert(GsThread *thread)
{
  for (; thread->readPos < thread->writePos; ++thread->readPos) {
    int readPos = thread->readPos % GS_CACHE_SIZE;
    char *sql = thread->cache[readPos];
    //gsPrint("thread:%d readPos:%d writePos:%d cacheRows:%d, cachePos:%d, sql:%s", thread->threadIndex, thread->readPos, thread->writePos, thread->cacheRows, thread->cachePos, sql);
    
    int code = 0;
    int affectRows = 0;
    for (int tryTimes = 0; tryTimes < 10; ++tryTimes) {
      code = taos_query(thread->taos, sql);
      
      if (code != 0) {
        taosMsleep(200);
        gsPrint("code:%d, affect_rows:%d, already insert this batch:%d", code, taos_affected_rows(thread->taos), affectRows);
        affectRows += taos_affected_rows(thread->taos);
        continue;
      }
	      
      affectRows += taos_affected_rows(thread->taos);
      if (affectRows != thread->cacheInserts[readPos]) {
        //gsPrint("code:%d, affect_rows:%d, already insert this batch:%d", code, taos_affected_rows(thread->taos), affectRows);
        //taosMsleep(400);
        //continue;
      }

      break;
    }
    
    thread->failedRows += (thread->cacheInserts[readPos] - affectRows);
    thread->insertRows += affectRows;
    
    thread->cacheInserts[readPos] = 0;
  }
}

void *gsThreadFp(void *arg)
{
  GsThread *thread = (GsThread*)arg;  
  while (1) {  
    gsThreadInsert(thread);
    taosMsleep(GS_SLEEP_INTERVAL);

    if (gsParseFinished) {
      gsThreadInsert(thread);
      break;
    }
  }

  return NULL;
}

void gsInitThread()
{
  gsThreads = (GsThread*)calloc(gsThreadNum, sizeof(GsThread));
  gsTableHash = taosInitStrHash(GS_MAX_TABLES, sizeof(GsThread), taosHashStringStep1);

  pthread_attr_t thattr;
  for (int i = 0; i < gsThreadNum; ++i) {
    GsThread *thread = gsThreads + i;
    thread->threadIndex = i;
    thread->cache = (char**)calloc(GS_CACHE_SIZE, sizeof(char*));
    thread->cacheInserts = (int*)calloc(GS_CACHE_SIZE, sizeof(int));
    for (int i = 0; i < GS_CACHE_SIZE; ++i) {
      thread->cache[i] = (char*)calloc(1, GS_CACHE_BLOCK_SIZE);
      thread->cacheInserts[i] = 0;
    }

    thread->cachePos = 0;
    thread->cacheRows = 0;
    thread->readPos = 0;
    thread->writePos = 0;
    thread->parseRows = 0;
    thread->insertRows = 0;
    thread->failedRows = 0;
   
    thread->taos = taos_connect(gsConnectHost, gsConnectUser, gsConnectPwd, NULL, 0);
    if (thread->taos == NULL) {
      gsError("thread:%d failed connect to TDengine, error:%s", thread->threadIndex, taos_errstr(thread->taos));
      gsExit(EXIT_FAILURE);
    }
    
    pthread_attr_init(&thattr);
    pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);

    if (pthread_create(&(thread->threadID), &thattr, gsThreadFp, (void*)thread) != 0) {
      gsError("thread:%d failed to start", thread->threadIndex);
      gsExit(EXIT_FAILURE);
    }
  }

  gsPrint("working thread init success");
}

int main(int argc, char *argv[])
{
  gsInit(argc, argv);

  gsConnectTDengine();

  gsCreateDbAndSt();

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
  int failedRows = 0;
  gsPrint("parse %d files, find %d tables, database:%s, stable:%s", gsCsvFileNum, gsTableNum, gsDatabaseName, gsStablesName);
  for (int i = 0; i < gsThreadNum; ++i) {
    GsThread *thread = &gsThreads[i];
    parsedRows += thread->parseRows;
    insertRows += thread->insertRows;
    failedRows += thread->failedRows;
    gsPrint("thread:%01d, parsed:%d insert:%d, failed:%d", i, thread->parseRows, thread->insertRows, thread->failedRows);
  }
  gsPrint("total %d parsed, %d inserted, %d failed time spent: %d seconds", parsedRows, insertRows, failedRows, (end - start) / 1000);

  for (int i = 0; i < gsThreadNum; i++) {
    taos_close(gsThreads[i].taos);
  }

  gsExit(EXIT_SUCCESS);
}
