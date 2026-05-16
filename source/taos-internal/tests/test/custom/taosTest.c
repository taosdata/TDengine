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
#include "taosdef.h"
#include "tmsg.h"
#include "tutil.h"
  
#define gsError(...) taosPrintLog("ERROR ", DEBUG_ERROR, 199, __VA_ARGS__); 
#define gsWarn(...)  taosPrintLog("WARN  ", DEBUG_WARN, 199, __VA_ARGS__); 
#define gsPrint(...) taosPrintLog("INFO  ", DEBUG_INFO, 199, __VA_ARGS__); 

//global variable
char     gsHostIp[24] = { 0 };
void    *gsTaos = NULL;
int      gsTableIndex = -1;
int      gsInsertNum = -1;
char     gsDatabaseName[32] = "db";
char     gsMetricsName[32] = "mt";
bool     gsQueryFlag = false;
int      gsInsertBatchNum = 3000;

void gsExit(int code)
{
  exit(code);
}

void gsPrintHelp()
{
  char indent[] = "        ";
  printf("taosTest is used to test effiency of TDengine\n");

  printf("%s%s\n", indent, "-t");
  printf("%s%s%s\n\n", indent, indent, "Table index");
  printf("%s%s\n", indent, "-n");
  printf("%s%s%s\n\n", indent, indent, "Num of insert rows");
  printf("%s%s\n", indent, "-q");
  printf("%s%s%s\n\n", indent, indent, "Query");
  printf("%s%s\n", indent, "-b");
  printf("%s%s%s\n\n", indent, indent, "Batch num per insert");
  printf("%s%s\n", indent, "-h");
  printf("%s%s%s\n\n", indent, indent, "TDEngine server IP address to connect, default is localhost");
  printf("%s%s\n", indent, "-c");
  printf("%s%s%s\n\n", indent, indent, "Configuration directory");

  gsExit(EXIT_SUCCESS);
}

void gsInit(int argc, char **argv)
{
  for (int i = 0; i < argc; ++i) {
    if (strcmp(argv[i], "-t") == 0) {
      if (i < argc - 1) {
        gsTableIndex = atoi(argv[++i]);
      }
      else {
        fprintf(stderr, "option -t requires an argument\n");
        gsExit(EXIT_FAILURE);
      }
    }

    if (strcmp(argv[i], "-n") == 0) {
      if (i < argc - 1) {
        gsInsertNum = atoi(argv[++i]);
      }
      else {
        fprintf(stderr, "option -n requires an argument\n");
        gsExit(EXIT_FAILURE);
      }
    }

    if (strcmp(argv[i], "-b") == 0) {
      if (i < argc - 1) {
        gsInsertBatchNum = atoi(argv[++i]);
        if (gsInsertBatchNum < 1 || gsInsertBatchNum > 3200) {
          fprintf(stderr, "batch num should in range [1-3200]\n");
          gsExit(EXIT_FAILURE);
        }
      }
      else {
        fprintf(stderr, "option -b requires an argument\n");
        gsExit(EXIT_FAILURE);
      }
    }

    if (strcmp(argv[i], "-q") == 0) {
      gsQueryFlag = true;
    }

    else if (strcmp(argv[i], "-h") == 0) {
      if (i < argc - 1) {
        char *tmp = argv[++i];
        if (strlen(tmp) >= 20) {
          fprintf(stderr, "option -h max length is 20\n");
          gsExit(EXIT_FAILURE);
        }
        strcpy(gsHostIp, tmp);
      }
      else {
        fprintf(stderr, "option -h requires an argument\n");
        gsExit(EXIT_FAILURE);
      }
    }

    if (strcmp(argv[i], "-c") == 0) {
      if (i < argc - 1) {
        char *tmp = argv[++i];
        if (strlen(tmp) >= 128) {
          fprintf(stderr, "option -c max length is 128\n");
          gsExit(EXIT_FAILURE);
        }
        strcpy(configDir, tmp);
      }
      else {
        fprintf(stderr, "Option -c requires an argument\n");
        gsExit(EXIT_FAILURE);
      }
    }

    else if (strcmp(argv[i], "--help") == 0) {
      gsPrintHelp();
      gsExit(EXIT_SUCCESS);
    }
  }

  if (gsTableIndex == -1) {
    fprintf(stderr, "Table index should input by -t option\n");
    gsExit(EXIT_FAILURE);
  }

  if (!gsQueryFlag) {
    if (gsInsertNum == -1) {
      fprintf(stderr, "Num of insert row should input by -n option\n");
      gsExit(EXIT_FAILURE);
    }
  }
}

void gsConnect()
{
  taos_init();

  char *host = NULL;
  if (strlen(gsHostIp) != 0) {
    host = gsHostIp;
  }

  gsTaos = taos_connect(host, "root", "taosdata", NULL, 0);
  if (gsTaos == NULL) {
    gsError("failed connect to TDengine, error:%s", taos_errstr(gsTaos));
    gsExit(EXIT_FAILURE);
  }

  gsPrint("connect to TDengine success");
}

void gsCreateTable(char *prefix)
{  
  char sql[1024] = { 0 };
  sprintf(sql, "create database %s replica 1", gsDatabaseName);
  int code = taos_query(gsTaos, sql);
  if (code == TSDB_CODE_DB_ALREADY_EXIST) {
    gsPrint("database:%s already exist", gsDatabaseName);
  }
  else if (code != 0) {
    gsError("failed to create database:%s, error:%s, sql:%s", gsDatabaseName, taos_errstr(gsTaos), sql);
    gsExit(EXIT_FAILURE);
  }
  else {
    gsPrint("create database:%s finished", gsDatabaseName);
  }

  sprintf(sql, "use %s", gsDatabaseName);
  code = taos_query(gsTaos, sql);
  if (code != 0) {
    gsError("failed to use database:%s, error:%s, sql:%s", gsDatabaseName, taos_errstr(gsTaos), sql);
    gsExit(EXIT_FAILURE);
  }

  sprintf(sql, "create table %s (ts timestamp, id int, val int) tags(type int)", gsMetricsName);
  code = taos_query(gsTaos, sql);
  if (code == TSDB_CODE_TABLE_ALREADY_EXIST) {
    gsPrint("metrics:%s.%s already exist", gsDatabaseName, gsMetricsName);
  }
  else if (code != 0) {
    gsError("failed to create metrics:%s.%s, error:%s, sql:%s", gsDatabaseName, gsMetricsName, taos_errstr(gsTaos), sql);
    gsExit(EXIT_FAILURE);
  }
  else {
    gsPrint("create metrics:%s.%s finished", gsDatabaseName, gsMetricsName);
  }

  sprintf(sql, "create table %s%d using %s tags(%d)", prefix, gsTableIndex, gsMetricsName, gsTableIndex);
  code = taos_query(gsTaos, sql);
  if (code == TSDB_CODE_TABLE_ALREADY_EXIST) {
    gsPrint("table:%s.%s%d already exist", gsDatabaseName, prefix, gsTableIndex);
    gsExit(EXIT_FAILURE);
  }
  else if (code != 0) {
    gsError("failed to create table:%s.%s%d, error:%s, sql:%s", gsDatabaseName, prefix, gsTableIndex, taos_errstr(gsTaos), sql);
    gsExit(EXIT_FAILURE);
  }
  else {
    gsPrint("create table:%s.%s%d finished", gsDatabaseName, prefix, gsTableIndex);
  }
}

void gsInsertData(char *prefix)
{
  char sqlBuffer[64*1024] = { 0 };
  int startlen = sprintf(sqlBuffer, "insert into %s.%s%d values", gsDatabaseName, prefix, gsTableIndex);
  char *sql = sqlBuffer + startlen;

  int64_t start = taosGetTimestampMs();

  int len = 0;
  int64_t startTime = 1535178760000;
  int row;
  for (row = 1; row < gsInsertNum + 100; /*++row*/) {
    len += sprintf(sql + len, "(%ld,%d,%d)", startTime++, row, row);
    if (row >= gsInsertNum) {
      int code = taos_query(gsTaos, sqlBuffer);
      if (code != 0) {
        gsError("insert data failed, rows:%d, error:%s, sql:%s", row, taos_errstr(gsTaos), sql);
      }
      break;
    }
    else if (row % gsInsertBatchNum == 0 || len > 62 * 1024) {
      int code = taos_query(gsTaos, sqlBuffer);
      if (code != 0) {
        gsError("insert data failed, rows:%d, error:%s, sql:%s", row, taos_errstr(gsTaos), sql);
      }
      len = 0;
    }
    row++;
  }

  int64_t end = taosGetTimestampMs();

  gsPrint("insert data to table:%s.%s%d finished, rows:%d, timespent:%ldms", gsDatabaseName, prefix, gsTableIndex, row, end - start);
}

void gsQueryThenInsert(char *prefix, char *targetPrefix)
{
  char sql[1024] = { 0 };
  sprintf(sql, "select * from %s.%s%d", gsDatabaseName, prefix, gsTableIndex);

  int64_t start = taosGetTimestampMs();

  int code = taos_query(gsTaos, sql);
  if (code != 0) {
    gsPrint("query data failure, error:%s, sql:%s", taos_errstr(gsTaos), sql);
    gsExit(EXIT_FAILURE);
  }

  void *result = taos_use_result(gsTaos);

  if (result == NULL) {
    gsPrint("query data failure, error:%s, sql:%s", taos_errstr(gsTaos), sql);
    gsExit(EXIT_FAILURE);
  }

  TAOS_ROW row;
  int numOfRows = 0;
  while ((row = taos_fetch_row(result)))
  {
    numOfRows++;
  }
  taos_free_result(result);

  int64_t end = taosGetTimestampMs();

  gsPrint("query data from table:%s.%s%d finished, rows:%d, timespent:%ldms", gsDatabaseName, prefix, gsTableIndex, numOfRows, end - start);
}

int main(int argc, char *argv[])
{
  gsInit(argc, argv);
  gsConnect();

  if (gsQueryFlag) {
    //gsCreateTable("q");
    gsQueryThenInsert("t", "q");
  }
  else {
    gsCreateTable("t");
    gsInsertData("t");
  }
  gsExit(EXIT_SUCCESS);
}
