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
#include <string.h>

#include "taos.h"
#include "tglobal.h"
#include "tlog.h"
#include "ihash.h"
#include "taosdef.h"
#include "tmsg.h"
#include "tutil.h"

#define GW_FILE_LEN          256
#define GW_METRIC_COL_LEN    38
#define GW_TABLE_COL_LEN     35
#define GW_INSERT_BATCH_NUM  700
#define GW_CACHE_BLOCK_SIZE  65536

#define gwError(...) taosPrintLog("ERROR ", DEBUG_ERROR, 199, __VA_ARGS__); 
#define gwWarn(...)  taosPrintLog("WARN  ", DEBUG_WARN, 199, __VA_ARGS__); 
#define gwPrint(...) taosPrintLog("INFO  ", DEBUG_INFO, 199, __VA_ARGS__); 

typedef struct {
  pthread_t threadID;
  int       threadIndex;
  void     *taos;
  char      sql[GW_CACHE_BLOCK_SIZE];
} GwThread;

typedef struct {
  char      tableId[32];
  char      tableDirectory[GW_FILE_LEN];
  int       parseFiles;
  int       insertRows;
  int       parseRows;
  int       threadIndex;
  char    **files;
  int       fileNum;
} GwTable;

//argument
char gwFileDirectory[256] = "./";
char gwDatabaseName[32] = "db";
char gwMetricsName[32] = "mt";
bool gwOldData = false;
int  gwReplica = 1;
int  gwThreadNum = 5;
char gwUserName[32] = { 0 };
char gwUserPass[32] = { 0 };
char gwHostIp[20] = { 0 };

//connection string
char *gwConnectHost = NULL;
char *gwConnectUser = NULL;
char *gwConnectPwd = NULL;

//parse from -f
char **gwDirectctories = 0;
char **gwShortDirectctories = 0;
int    gwDirectoryNum = 0;

//global variable
void          *gwTaos = NULL;
GwThread      *gwThreads = NULL;
GwTable       *gwTables = NULL;
int            gwTableNum = 0;

void gwExit(int code)
{
  exit(code);
}

void gwPrintHelp()
{
  char indent[] = "        ";
  printf("taosMVB import data from Shanghai Metro Company to TDengine\n");

  printf("%s%s\n", indent, "-f");
  printf("%s%s%s\n\n", indent, indent, "log file directory, default is ./");
  printf("%s%s\n", indent, "-d");
  printf("%s%s%s\n\n", indent, indent, "Database used to create table or import data, default is db");
  printf("%s%s\n", indent, "-m");
  printf("%s%s%s\n\n", indent, indent, "Metrics used to create table, default is mt");
  printf("%s%s\n", indent, "-o");
  printf("%s%s%s\n\n", indent, indent, "The provided file is history data");
  printf("%s%s\n", indent, "-r");
  printf("%s%s%s\n\n", indent, indent, "Replica of Database, default is 1");
  printf("%s%s\n", indent, "-t");
  printf("%s%s%s\n\n", indent, indent, "Process Thread Num, default is 5");
  printf("%s%s\n", indent, "-h");
  printf("%s%s%s\n\n", indent, indent, "TDEngine server IP address to connect, default is localhost");
  printf("%s%s\n", indent, "-u");
  printf("%s%s%s\n\n", indent, indent, "The TDEngine user name to use when connecting to the server, default is root");
  printf("%s%s\n", indent, "-p");
  printf("%s%s%s\n\n", indent, indent, "The password to use when connecting to the server, default is taosdata");
  printf("%s%s\n", indent, "-c");
  printf("%s%s%s\n\n", indent, indent, "Configuration directory");

  gwExit(EXIT_SUCCESS);
}

void gwInit(int argc, char **argv)
{
  for (int i = 0; i < argc; ++i) {
    if (strcmp(argv[i], "-f") == 0) {
      if (i < argc - 1) {
        char *tmp = argv[++i];
        if (tmp[0] == '-') {
          fprintf(stderr, "option -f requires an argument\n");
          gwExit(EXIT_FAILURE);
        }

        int len = strlen(tmp);
        if (len >= 256) {
          fprintf(stderr, "option -f max length is 256\n");
          gwExit(EXIT_FAILURE);
        }
        strcpy(gwFileDirectory, tmp);
        if (gwFileDirectory[len - 1] == '/') {
          gwFileDirectory[len - 1] = 0;
        }
      }
      else {
        fprintf(stderr, "option -f requires an argument\n");
        gwExit(EXIT_FAILURE);
      }
    }

    else if (strcmp(argv[i], "-d") == 0) {
      if (i < argc - 1) {
        char *tmp = argv[++i];
        if (tmp[0] == '-') {
          fprintf(stderr, "option -d requires an argument\n");
          gwExit(EXIT_FAILURE);
        }

        if (strlen(tmp) >= 32) {
          fprintf(stderr, "option -d max length is 32\n");
          gwExit(EXIT_FAILURE);
        }
        strcpy(gwDatabaseName, tmp);
      }
      else {
        fprintf(stderr, "option -d requires an argument\n");
        gwExit(EXIT_FAILURE);
      }
    }

    else if (strcmp(argv[i], "-m") == 0) {
      if (i < argc - 1) {
        char *tmp = argv[++i];
        if (tmp[0] == '-') {
          fprintf(stderr, "option -m requires an argument\n");
          gwExit(EXIT_FAILURE);
        }

        if (strlen(tmp) >= 32) {
          fprintf(stderr, "option -m max length is 32\n");
          gwExit(EXIT_FAILURE);
        }
        strcpy(gwMetricsName, tmp);
      }
      else {
        fprintf(stderr, "option -m requires an argument\n");
        gwExit(EXIT_FAILURE);
      }
    }

    else if (strcmp(argv[i], "-r") == 0) {
      if (i < argc - 1) {
        char *tmp = argv[++i];
        if (tmp[0] == '-') {
          fprintf(stderr, "option -r requires an argument\n");
          gwExit(EXIT_FAILURE);
        }
        gwReplica = atoi(tmp);
      }
      else {
        fprintf(stderr, "option -r requires an argument\n");
        gwExit(EXIT_FAILURE);
      }
      if (gwReplica < 1 || gwReplica > 3) {
        fprintf(stderr, "replica should in range [1, 3]\n");
        gwExit(EXIT_FAILURE);
      }
    }

    else if (strcmp(argv[i], "-t") == 0) {
      if (i < argc - 1) {
        char *tmp = argv[++i];
        if (tmp[0] == '-') {
          fprintf(stderr, "option -t requires an argument\n");
          gwExit(EXIT_FAILURE);
        }
        gwThreadNum = atoi(tmp);
      }
      else {
        fprintf(stderr, "option -t requires an argument\n");
        gwExit(EXIT_FAILURE);
      }
      if (gwThreadNum < 1 || gwThreadNum > 30) {
        fprintf(stderr, "replica should in range [1, 30]\n");
        gwExit(EXIT_FAILURE);
      }
    }

    else if (strcmp(argv[i], "-o") == 0) {
      gwOldData = true;
    }

    else if (strcmp(argv[i], "-h") == 0) {
      if (i < argc - 1) {
        char *tmp = argv[++i];
        if (tmp[0] == '-') {
          fprintf(stderr, "option -h requires an argument\n");
          gwExit(EXIT_FAILURE);
        }
        if (strlen(tmp) >= 20) {
          fprintf(stderr, "option -h max length is 20\n");
          gwExit(EXIT_FAILURE);
        }
        strcpy(gwHostIp, tmp);
      }
      else {
        fprintf(stderr, "option -h requires an argument\n");
        gwExit(EXIT_FAILURE);
      }
    }

    else if (strcmp(argv[i], "-u") == 0) {
      if (i < argc - 1) {
        char *tmp = argv[++i];
        if (tmp[0] == '-') {
          fprintf(stderr, "option -u requires an argument\n");
          gwExit(EXIT_FAILURE);
        }
        if (strlen(tmp) >= 32) {
          fprintf(stderr, "option -u max length is 32\n");
          gwExit(EXIT_FAILURE);
        }
        strcpy(gwUserName, tmp);
      }
      else {
        fprintf(stderr, "option -u requires an argument\n");
        gwExit(EXIT_FAILURE);
      }
    }

    else if (strcmp(argv[i], "-p") == 0) {
      if (i < argc - 1) {
        char *tmp = argv[++i];
        if (tmp[0] == '-') {
          fprintf(stderr, "option -p requires an argument\n");
          gwExit(EXIT_FAILURE);
        }
        if (strlen(tmp) >= 32) {
          fprintf(stderr, "option -p max length is 32\n");
          gwExit(EXIT_FAILURE);
        }
        strcpy(gwUserPass, tmp);
      }
      else {
        fprintf(stderr, "option -p requires an argument\n");
        gwExit(EXIT_FAILURE);
      }
    }

    if (strcmp(argv[i], "-c") == 0) {
      if (i < argc - 1) {
        char *tmp = argv[++i];
        if (tmp[0] == '-') {
          fprintf(stderr, "option -c requires an argument\n");
          gwExit(EXIT_FAILURE);
        }
        if (strlen(tmp) >= 128) {
          fprintf(stderr, "option -c max length is 128\n");
          gwExit(EXIT_FAILURE);
        }
        strcpy(configDir, tmp);
      }
      else {
        fprintf(stderr, "Option -c requires an argument\n");
        gwExit(EXIT_FAILURE);
      }
    }

    else if (strcmp(argv[i], "--help") == 0) {
      gwPrintHelp();
      gwExit(EXIT_SUCCESS);
    }
  }
}

void gwConnectTDengine()
{
  taos_init();

  gwConnectHost = NULL;
  if (strlen(gwHostIp) != 0) {
    gwConnectHost = gwHostIp;
  }

  gwConnectUser = tsDefaultUser;
  if (strlen(gwUserName) != 0) {
    gwConnectUser = gwUserName;
  }

  gwConnectPwd = tsDefaultPass;
  if (strlen(gwUserPass) != 0) {
    gwConnectPwd = gwUserPass;
  }

  gwTaos = taos_connect(gwConnectHost, gwConnectUser, gwConnectPwd, NULL, 0);
  if (gwTaos == NULL) {
    gwError("failed connect to TDengine, error:%s", taos_errstr(gwTaos));
    gwExit(EXIT_FAILURE);
  }

  gwPrint("connect to TDengine success");
}

int gwGetDirectoryNum(char *directoryName)
{
  char cmd[1024] = { 0 };
  sprintf(cmd, "ls %s | wc -l ", directoryName);

  FILE *fp = popen(cmd, "r");
  if (fp == NULL) {
    gwError("failed to execute:%s, error:%s", cmd, strerror(errno));
    gwExit(EXIT_FAILURE);
  }

  int dirNum = 0;
  if (fscanf(fp, "%d", &dirNum) != 1) {
    gwError("failed to execute:%s, parse result error", cmd);
    gwExit(EXIT_FAILURE);
  }

  if (dirNum <= 0) {
    gwError("directory:%s is empry", directoryName);
    gwExit(EXIT_FAILURE);
  }

  pclose(fp);
  return dirNum;
}

void gwGetDirectoryName(char *directoryName)
{
  char cmd[1024] = { 0 };
  sprintf(cmd, "ls %s | sort", directoryName);

  FILE *fp = popen(cmd, "r");
  if (fp == NULL) {
    gwError("failed to execute:%s, error:%s", cmd, strerror(errno));
    gwExit(EXIT_FAILURE);
  }

  int dirNum = 0;
  while (fscanf(fp, "%s", gwShortDirectctories[dirNum])) {
    sprintf(gwDirectctories[dirNum], "%s/%s", directoryName, gwShortDirectctories[dirNum]);
    dirNum++;
    if (dirNum >= gwDirectoryNum) {
      break;
    }
  }

  if (dirNum != gwDirectoryNum) {
    gwError("directory:%s changed while read", directoryName);
    gwExit(EXIT_FAILURE);
  }

  pclose(fp);
}

void gwMallocDirectories()
{
  gwDirectctories = (char**)calloc(gwDirectoryNum, sizeof(char*));
  gwShortDirectctories = (char**)calloc(gwDirectoryNum, sizeof(char*));
  for (int i = 0; i < gwDirectoryNum; i++) {
    gwDirectctories[i] = calloc(1, GW_FILE_LEN);
    gwShortDirectctories[i] = calloc(1, GW_FILE_LEN);
  }
}

void gwParseDirectory()
{
  struct stat fileStat;
  if (stat(gwFileDirectory, &fileStat) < 0) {
    gwError("%s not exist", gwFileDirectory);
    gwExit(EXIT_FAILURE);
  }

  if (!(fileStat.st_mode & S_IFDIR)) {
    gwError("%s not a directory", gwFileDirectory);
    gwExit(EXIT_FAILURE);
  }

  gwDirectoryNum = gwGetDirectoryNum(gwFileDirectory);
  gwMallocDirectories();
  gwGetDirectoryName(gwFileDirectory);
  gwPrint("start to dispose %d directories in %s", gwDirectoryNum, gwFileDirectory);
}

bool gwCheckLogDirecoryName(char *dir, char *shortDir, int *len)
{
  struct stat fileStat;
  if (stat(dir, &fileStat) < 0) {
    gwError("%s not exist", dir);
    return false;
  }

  if (!(fileStat.st_mode & S_IFDIR)) {
    gwError("%s not a directory", dir);
    return false;
  }

  *len = strlen(shortDir);
  if (*len < 5) {
    gwPrint("%s name too short", shortDir);
    return false;
  }
  if (*len > 20) {
    gwPrint("%s name too long", shortDir);
    return false;
  }

  int dotNum = 0;
  for (int i = 0; i < *len; i++) {
    if (shortDir[i] == '_') {
      dotNum++;
    }
  }

  if (dotNum != 1) {
    gwPrint("%s name must have a '_' ", shortDir);
    return false;
  }

  return true;
}

void gwGetTableName(char *directoryName, char **files, int fileNum)
{
  char cmd[1024] = { 0 };
  sprintf(cmd, "ls %s/*.log | sort", directoryName);

  FILE *fp = popen(cmd, "r");
  if (fp == NULL) {
    gwError("failed to execute:%s, error:%s", cmd, strerror(errno));
    gwExit(EXIT_FAILURE);
  }

  int num = 0;
  while (fscanf(fp, "%s", files[num++])) {
    if (num >= fileNum) {
      break;
    }
  }

  if (num != fileNum) {
    gwError("directory:%s changed while read", directoryName);
    gwExit(EXIT_FAILURE);
  }

  pclose(fp);
}

void gwCreateTable()
{
  gwTables = (GwTable*)calloc(gwDirectoryNum, sizeof(GwTable));
  int tableIndex = 0;

  //create database
  char sql[1024] = { 0 };
  sprintf(sql, "create database %s replica %d", gwDatabaseName, gwReplica);
  int code = taos_query(gwTaos, sql);
  if (code == TSDB_CODE_DB_ALREADY_EXIST) {
    gwPrint("database:%s already exist", gwDatabaseName);
  }
  else if (code != 0) {
    gwError("failed to create database:%s, error:%s, sql:%s", gwDatabaseName, taos_errstr(gwTaos), sql);
    gwExit(EXIT_FAILURE);
  }
  else {
    gwPrint("create database:%s finished", gwDatabaseName);
  }

  //use database
  sprintf(sql, "use %s", gwDatabaseName);
  code = taos_query(gwTaos, sql);
  if (code != 0) {
    gwError("failed to use database:%s, error:%s, sql:%s", gwDatabaseName, taos_errstr(gwTaos), sql);
    gwExit(EXIT_FAILURE);
  }

  //create metrics
  int len = sprintf(sql, "create table %s (ts timestamp", gwMetricsName);
  for (int i = 0; i < GW_TABLE_COL_LEN - 1; ++i) {
    len += sprintf(sql + len, ",f%d smallint", i + 1);
  }
  sprintf(sql + len, ") tags(t1 binary(10), t2 smallint, t3 smallint)");
  code = taos_query(gwTaos, sql);
  if (code == TSDB_CODE_TABLE_ALREADY_EXIST) {
    gwPrint("metrics:%s.%s already exist", gwDatabaseName, gwMetricsName);
    sprintf(sql, "describe %s", gwMetricsName);
    taos_query(gwTaos, sql);
    void *result = taos_use_result(gwTaos);
    if (result == NULL) {
      gwError("get metrics fields failure, error:%s, sql:%s", taos_errstr(gwTaos), sql);
      gwExit(EXIT_FAILURE);
    }

    TAOS_ROW row;
    int numOfRows = 0;
    while ((row = taos_fetch_row(result)))
    {
      numOfRows++;
    }
    taos_free_result(result);

    if (numOfRows != GW_METRIC_COL_LEN) {
      gwError("exist metrics:%s fields:%d not equal with file:%d", gwMetricsName, numOfRows, GW_METRIC_COL_LEN);
      gwExit(EXIT_FAILURE);
    }
  }
  else if (code != 0) {
    gwError("failed to create metrics:%s.%s, error:%s, sql:%s", gwDatabaseName, gwMetricsName, taos_errstr(gwTaos), sql);
    gwExit(EXIT_FAILURE);
  }
  else {
    gwPrint("create metrics:%s.%s finished", gwDatabaseName, gwMetricsName);
  }

  for (int i = 0; i < gwDirectoryNum; ++i) {
    char *dir = gwDirectctories[i];
    char *shortDir = gwShortDirectctories[i];
    int len = 0;
    if (!gwCheckLogDirecoryName(dir, shortDir, &len)) {
      continue;
    }

    char tableId[32] = { 0 };
    int tag1 = 0, tag2 = 0;
    int dotnum = 0;
    for (int j = 0, k = 0; j < len && j < 31; ++j) {
      if (shortDir[j] == '_') {
        dotnum++;
        char str1[3] = { 0 };
        char str2[3] = { 0 };
        strncpy(str1, shortDir + j + 1, 2);
        strncpy(str2, shortDir + j + 3, 2);
        sscanf(str1, "%x", &tag1);
        sscanf(str2, "%x", &tag2);
        break;
      }
      else {
        tableId[k++] = shortDir[j];
      }
    }

    sprintf(sql, "create table %s using %s tags('%s', %d, %d)", tableId, gwMetricsName, tableId, tag1, tag2);

    int code = taos_query(gwTaos, sql);
    if (code == TSDB_CODE_TABLE_ALREADY_EXIST) {
      gwPrint("table:%s.%s already exist", gwDatabaseName, tableId);
      sprintf(sql, "describe %s", tableId);
      taos_query(gwTaos, sql);
      void *result = taos_use_result(gwTaos);
      if (result == NULL) {
        gwError("get table fields failure, error:%s, sql:%s", taos_errstr(gwTaos), sql);
        gwExit(EXIT_FAILURE);
      }

      TAOS_ROW row;
      int numOfRows = 0;
      while ((row = taos_fetch_row(result)))
      {
        numOfRows++;
      }
      taos_free_result(result);

      if (numOfRows != GW_METRIC_COL_LEN) {
        gwError("exist table:%s fields:%d not equal with file:%d", tableId, numOfRows, GW_METRIC_COL_LEN);
        gwExit(EXIT_FAILURE);
      }
    }
    else if (code != 0) {
      gwError("failed to create table:%s.%s, error:%s, sql:%s", gwDatabaseName, tableId, taos_errstr(gwTaos), sql);
      gwExit(EXIT_FAILURE);
    }
    else {
      gwPrint("create table:%s.%s successed", gwDatabaseName, tableId);
    }

    GwTable *table = &gwTables[gwTableNum++];
    strcpy(table->tableId, tableId);
    strcpy(table->tableDirectory, gwDirectctories[i]);
    table->threadIndex = (gwTableNum - 1) % gwThreadNum;

    table->fileNum = gwGetDirectoryNum(table->tableDirectory);
    table->files = calloc(table->fileNum, sizeof(char*));
    for (int i = 0; i < table->fileNum; ++i) {
      table->files[i] = (char*)calloc(1, GW_FILE_LEN);
    }
    gwGetTableName(table->tableDirectory, table->files, table->fileNum);
    gwPrint("table:%s.%s, files:%d, in directory:%s", gwDatabaseName, table->tableId, table->fileNum, table->tableDirectory);
  }
}

void gwThreadProcessFile(GwThread *thread, GwTable *table, char *logfile)
{
  gwPrint("dispose file:%s", logfile);

  FILE *fp = fopen(logfile, "r");
  if (fp == NULL) {
    gwError("failed to open file:%s, error:%s", logfile, strerror(errno));
    gwExit(EXIT_FAILURE);
  }

  uint32_t data[36] = { 0 };
  char date1[20] = { 0 };
  char date2[20] = { 0 };
  char *sql = thread->sql;
  int batchNum = 0;
  int len = 0;
  len = sprintf(sql, "%s into %s.%s values", gwOldData ? "import" : "insert", gwDatabaseName, table->tableId);
  sql += len;
  len = 0;

  char oldDate2[20] = { 0 };
  int oldIndex = 1;
  do {
    //column num is 38, data column is 36
    //[2018-08-22 10:04:56] 31 01 5B 7D 35 48 DF 1A D1 2A 00 00 30 30 30 01 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 08 
    //int num = fscanf(fp, "%s%s%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x"
    //  , date1, date2
    //  , &data[0], &data[1], &data[2], &data[3], &data[4]
    //  , &data[5], &data[6], &data[7], &data[8], &data[9]
    //  , &data[10], &data[11], &data[12], &data[13], &data[14]
    //  , &data[15], &data[16], &data[17], &data[18], &data[19]
    //  , &data[20], &data[21], &data[22], &data[23], &data[24]
    //  , &data[25], &data[26], &data[27], &data[28], &data[29]
    //  , &data[30], &data[31], &data[32], &data[33], &data[34]
    //  , &data[35]);

    char *line = NULL;
    size_t lineLen = 0;
    getline(&line, &lineLen, fp);
    if (line == NULL) {
      break;
    }
    int num = sscanf(line, "%s%s%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x%x"
      , date1, date2
      , &data[0], &data[1], &data[2], &data[3], &data[4]
      , &data[5], &data[6], &data[7], &data[8], &data[9]
      , &data[10], &data[11], &data[12], &data[13], &data[14]
      , &data[15], &data[16], &data[17], &data[18], &data[19]
      , &data[20], &data[21], &data[22], &data[23], &data[24]
      , &data[25], &data[26], &data[27], &data[28], &data[29]
      , &data[30], &data[31], &data[32], &data[33], &data[34]
      , &data[35]);
    taosMemoryFree(line);

    if (num != GW_METRIC_COL_LEN) {
      //gwError("column length invalid");
      break;
    }
    if (date1[0] != '[' || date2[8] != ']') {
      gwError("date format invalid");
      break;
    }
    if (strlen(date1) != 11 || strlen(date2) != 9){
      gwError("date length invalid");
      break;
    }
    char *d1 = date1 + 1;
    if (strcmp(d1, "1970-01-01") == 0) {
      //gwError("invalid date:%s", d1);
      continue;
    }

    date2[8] = 0;
    if (strcmp(oldDate2, date2) == 0) {
      oldIndex++;
      if (oldIndex > 999) {
        gwPrint("too many same timestamps");
        continue;
      }
    }
    else {
      oldIndex = 1;
    }
    
    strcpy(oldDate2, date2);
    
    len += sprintf(sql + len, "('%s %s.%03d',%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d)"
      , d1, date2, oldIndex
      , data[2], data[3], data[4]
      , data[5], data[6], data[7], data[8], data[9]
      , data[10], data[11], data[12], data[13], data[14]
      , data[15], data[16], data[17], data[18], data[19]
      , data[20], data[21], data[22], data[23], data[24]
      , data[25], data[26], data[27], data[28], data[29]
      , data[30], data[31], data[32], data[33], data[34]
      , data[35]
      );

    batchNum++;
    table->parseRows++;

    if (batchNum >= GW_INSERT_BATCH_NUM || len > (GW_CACHE_BLOCK_SIZE - 1500)) {
      int code = taos_query(thread->taos, thread->sql);
      if (code != 0) {
        gwError("table:%s insert failed, rows:%d, error:%s, sql:%s", table->parseRows, taos_errstr(thread->taos), sql);
      }
      table->insertRows += taos_affected_rows(thread->taos);
      len = 0;
      batchNum = 0;
    }
    
  } while (true);

  if (batchNum != 0) {
    int code = taos_query(thread->taos, thread->sql);
    if (code != 0) {
      gwError("table:%s insert failed, rows:%d, error:%s, sql:%s", table->parseRows, taos_errstr(thread->taos), sql);
    }
    table->insertRows += taos_affected_rows(thread->taos);
    len = 0;
    batchNum = 0;
  }

  //gwPrint("dispose table:%s.%s in file:%s, insert:%d, failed:%d"
    //, gwDatabaseName, table->tableId, logfile, table->insertRows, table->parseRows - table->insertRows);

  fclose(fp);
}

void* gwThreadFp(void *arg)
{
  GwThread *pThread = (GwThread*)arg;
  for (int i = 0; i < gwTableNum; ++i) {
    GwTable *table = &gwTables[i];
    if (table->threadIndex != pThread->threadIndex) continue;
    for (int j = 0; j < table->fileNum; ++j)
      gwThreadProcessFile(pThread, table, table->files[j]);
  }

  return NULL;
}

void gwInitThread()
{
  pthread_attr_t thattr;
  gwThreads = (GwThread *)calloc(gwThreadNum, sizeof(GwThread));
  for (int i = 0; i < gwThreadNum; ++i) {
    GwThread *pThread = gwThreads + i;
    pThread->threadIndex = i;

    pThread->taos = taos_connect(gwConnectHost, gwConnectUser, gwConnectPwd, NULL, 0);
    if (pThread->taos == NULL) {
      gwError("thread:%d failed connect to TDengine, error:%s", pThread->threadIndex, taos_errstr(pThread->taos));
      gwExit(EXIT_FAILURE);
    }
    
    pthread_attr_init(&thattr);
    pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);

    if (pthread_create(&(pThread->threadID), &thattr, gwThreadFp, (void*)pThread) != 0) {
      gwError("thread:%d failed to start", pThread->threadIndex);
      gwExit(EXIT_FAILURE);
    }
  }

  gwPrint("working thread init success");
}

int main(int argc, char *argv[])
{
  gwInit(argc, argv);

  gwConnectTDengine();

  gwParseDirectory();

  gwCreateTable();

  int64_t start = taosGetTimestampMs();
  
  gwInitThread();

  for (int i = 0; i < gwThreadNum; i++) {
    pthread_join(gwThreads[i].threadID, NULL);
  }

  int64_t end = taosGetTimestampMs();

  int parsedRows = 0;
  int insertRows = 0;
  gwPrint("parse %d tables, database:%s, metrics:%s", gwTableNum, gwDatabaseName, gwMetricsName);
  
  for (int i = 0; i < gwTableNum; ++i) {
    GwTable *table = &gwTables[i];
    parsedRows += table->parseRows;
    insertRows += table->insertRows;
    gwPrint("index:%03d, table:%s, parsed:%d, inserted:%d, failed:%d, files:%d"
      , i + 1, table->tableId, table->parseRows, table->insertRows, table->parseRows - table->insertRows, table->parseFiles);
  }
  
  gwPrint("total %d rows parsed, %d rows inserted, %d rows failed, time spent: %d seconds"
    , parsedRows, insertRows, parsedRows - insertRows, (end - start) / 1000);

  for (int i = 0; i < gwThreadNum; i++) {
    taos_close(gwThreads[i].taos);
  }

  gwExit(EXIT_SUCCESS);
}
