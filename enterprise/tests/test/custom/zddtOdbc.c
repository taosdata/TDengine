/*******************************************************************
 *           Copyright (c) 2017 by TAOS Technologies, Inc.
 *                     All rights reserved.
 *
 *  This file is proprietary and confidential to TAOS Technologies.
 *  No part of this file may be reproduced, stored, transmitted,
 *  disclosed or used in any form or by any means other than as
 *  expressly provided by the written permission from Jianhui Tao
 *
 * ****************************************************************/

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
#include <sql.h>
#include <sqltypes.h>
#include <sqlext.h>
#include <stdint.h>
#include <stdbool.h>
#include <wordexp.h>

#include "taos.h"
#include "tglobal.h"
#include "tlog.h"
#include "ihash.h"
#include "shash.h"
#include "taosdef.h"
#include "taosmsg.h"
#include "tutil.h"
#include "tmd5.h"

//
// log functions
// 

#define gsError(...) taosPrintLog("ERROR ", 199, __VA_ARGS__); 
#define gsWarn(...)  taosPrintLog("WARN  ", 199, __VA_ARGS__); 
#define gsPrint(...) taosPrintLog("INFO  ", 199, __VA_ARGS__); 
#define gsDump(...)  taosPrintLongString("ERROR ", 199, __VA_ARGS__); 

#define GS_RESULT_FAILURE  "failure"
#define GS_RESULT_SUCCESS  "success"
#define GS_RESULT_SCHEDULE "schedule"
#define GS_RESULT_FAILURE_INT  2
#define GS_RESULT_SUCCESS_INT  1
#define GS_RESULT_SCHEDULE_INT 0

#define GS_USER_LEN   32
#define GS_PASS_LEN   32
#define GS_SOURCE_LEN 32
#define GS_IP_LEN     32
#define GS_DIR_LEN    256
#define GS_FILE_LEN   256
#define GS_SLEEP_INTERVAL 10000
#define GS_OUTPUT_TYPE_CSV 0 
#define GS_OUTPUT_TYPE_TARGZ 1 

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

//void gsParseDirectory(const char *directoryName, const char *prefix, GsFile *fileArray, int totalFiles)
//{
//  strcpy(fileArray[0].filename, "data/1535731200000.csv");
//  strcpy(fileArray[1].filename, "data/1535731800000.csv");
//  strcpy(fileArray[2].filename, "data/1535731500000.csv");
//  strcpy(fileArray[3].filename, "data/1535732400000.csv");
//}

#endif

typedef struct {
  char *name;
  char *status;
  char *sql;
} GsTask;

typedef struct {
  pthread_t threadID;
  int       threadIndex;
  SQLHDBC   hdbc;
  SQLHSTMT  hstmt;
  GsTask   *taskList;
  int       taskPos;
  int       taskLen;

  //
  int64_t totalTasks;
  int64_t successTasks;
  int64_t failedTasks;
  int64_t norunTasks;
  int64_t totalRows;
} GsThread;

typedef struct {
  char odbcUser[GS_USER_LEN];
  char odbcPassword[GS_PASS_LEN];
  char odbcSourceName[GS_SOURCE_LEN];
  char odbcIp[GS_IP_LEN];
  char tmpDir[GS_DIR_LEN];
  char outputDir[GS_DIR_LEN];
  bool outputType;
  int  threadNum;
  int  maxExistFiles;

  //global variables
  GsTask *tasks;
  int     taskLen;
  GsThread *threads;
} GsGlobal;
GsGlobal *gsGlobal;

typedef struct {
  SQLHENV henv;
  bool loopRun;
} GsArgument;
GsArgument gsArg;

void gsPrintArgs()
{
  gsPrint("odbcUser: %s", gsGlobal->odbcUser);
  gsPrint("odbcPassword: %s", gsGlobal->odbcPassword);
  gsPrint("odbcSourceName: %s", gsGlobal->odbcSourceName);
  gsPrint("odbcIp: %s", gsGlobal->odbcIp);
  gsPrint("tmpDir: %s", gsGlobal->odbcIp);
  gsPrint("logDir: %s", gsGlobal->odbcIp);
  gsPrint("outputDir: %s", gsGlobal->odbcIp);
  gsPrint("outputType: %d", gsGlobal->odbcIp);
  gsPrint("threadNum: %d", gsGlobal->odbcIp);
  gsPrint("maxExistFilesPerThread: %d", gsGlobal->odbcIp);
  gsPrint("totalTaskNum: %d", gsGlobal->taskLen);
  for (int i = 0; i < gsGlobal->taskLen; ++i) {
    GsTask *task = &gsGlobal->tasks[i];
    gsPrint("%s %s %s", task->name, task->status, task->sql);
  }
}

void gsFreeGlobal()
{
  for (int i = 0; i < gsGlobal->taskLen; ++i) {
    GsTask *task = &gsGlobal->tasks[i];
    free(task->name);
    free(task->sql);
  }
  free(gsGlobal->tasks);

  for (int i = 0; i < gsGlobal->threadNum; ++i) {
    GsThread *thread = &gsGlobal->threads[i];
    if (thread->hdbc != NULL) {
      SQLFreeHandle(SQL_HANDLE_DBC, thread->hdbc);
    }
    if (thread->hstmt != NULL) {
      SQLFreeHandle(SQL_HANDLE_DBC, thread->hstmt);
    }

    free(thread);
  }
  free(gsGlobal->threads);

  free(gsGlobal);
  gsGlobal = 0;
}

void gsMallocGlobal()
{
  gsGlobal = (GsGlobal*)malloc(sizeof(gsGlobal));
  memset(&gsGlobal, 0, sizeof(gsGlobal));
  strcpy(gsGlobal->odbcUser, "root");
  strcpy(gsGlobal->odbcPassword, "123456");
  strcpy(gsGlobal->odbcSourceName, "test1");
  strcpy(gsGlobal->odbcIp, "127.0.0.1");
  strcpy(gsGlobal->tmpDir, "/var/log/zddt");
  strcpy(gsGlobal->outputDir, "/var/log/zddt");
  gsGlobal->outputType = GS_OUTPUT_TYPE_CSV;
  gsGlobal->threadNum = 1;
  gsGlobal->maxExistFiles = 10;
}

GsTask * gsNewTask()
{
  return NULL;
}

void gsReadConfigFile()
{
  FILE *fp;
  char *line, *option, *value, *value1, *value2;
  size_t  len;
  int   olen, vlen, vlen1, vlen2;
  char fileName[128];

  wordexp_t full_path;
  wordexp(configDir, &full_path, 0);
  if (full_path.we_wordv != NULL && full_path.we_wordv[0] != NULL) {
    strcpy(configDir, full_path.we_wordv[0]);
  }
  else {
    strcpy(configDir, "/etc/zddt");
    gsPrint("configDir:%s not there, use default value: /etc/zddt", configDir);
  }
  wordfree(&full_path);

  sprintf(fileName, "%s/odbc.cfg", configDir);
  fp = fopen(fileName, "r");
  if (fp == NULL) {
    gsPrint("option file:%s not found, all options are set to system default\n", fileName);
    return;
  }

  line = NULL;
  while (!feof(fp)) {
    tfree(line);
    line = option = value = NULL;
    len = olen = vlen = 0;

    getline(&line, &len, fp);
    if (line == NULL) break;

    paGetToken(line, &option, &olen);
    if (olen == 0) continue;
    option[olen] = 0;

    paGetToken(option + olen + 1, &value, &vlen);
    if (vlen == 0) continue;
    value[vlen] = 0;

    if (strcasecmp(option, "odbcUser") == 0) {
      strncpy(gsGlobal->odbcUser, GS_USER_LEN, value);
    }
    else if (strcasecmp(option, "odbcPassword") == 0) {
      strncpy(gsGlobal->odbcPassword, GS_PASS_LEN, value);
    }
    else if (strcasecmp(option, "odbcSourceName") == 0) {
      strncpy(gsGlobal->odbcSourceName, GS_SOURCE_LEN, value);
    }
    else if (strcasecmp(option, "odbcIp") == 0) {
      strncpy(gsGlobal->odbcIp, GS_IP_LEN, value);
    }
    else if (strcasecmp(option, "tmpDir") == 0) {
      strncpy(gsGlobal->odbcIp, GS_DIR_LEN, value);
    }
    else if (strcasecmp(option, "outputDir") == 0) {
      strncpy(gsGlobal->outputDir, GS_DIR_LEN, value);
    }
    else if (strcasecmp(option, "outputType") == 0) {
      if (strcasecmp(value, "csv") == 0) {
        gsGlobal->outputType = GS_OUTPUT_TYPE_CSV;
      }
      else if (strcasecmp(value, "targz") == 0 || strcasecmp(value, "tar.gz") == 0) {
        gsGlobal->outputType = GS_OUTPUT_TYPE_TARGZ;
      }
    }
    else if (strcasecmp(option, "threadNum") == 0) {
      gsGlobal->threadNum = atoi(value);
    }
    else if (strcasecmp(option, "maxExistFiles") == 0) {
      gsGlobal->maxExistFiles = atoi(value);
    }
    else {
      int taskStatus = GS_RESULT_SCHEDULE_INT;
      if (strcasecmp(value, GS_RESULT_FAILURE) == 0) {
        taskStatus = GS_RESULT_FAILURE_INT;
      }
      else if (strcasecmp(value, GS_RESULT_SUCCESS) == 0) {
        taskStatus = GS_RESULT_SUCCESS_INT;
      }
      else {}

      paGetToken(line, &value1, &vlen1);
      if (vlen1 == 0) continue;
      if (strncasecmp(value1, "select", 6) != 0)
        continue;
      
      gsGlobal->taskLen++;
      GsTask *task = gsNewTask();
      task->name = calloc(1, olen);
      strcpy(task->name, olen);
      
      task->status = taskStatus;
      
      int sqllen = strlen(value1);
      task->sql = calloc(1, sqllen);
      strcpy(task->sql, sqllen);
    }
  }

  tfree(line);
  fclose(fp);

  gsPrintArgs();
}

void gsReadBasicConfig()
{
  FILE *fp;
  char *line, *option, *value;
  size_t  len;
  int   olen, vlen;
  char fileName[128];

  wordexp_t full_path;
  wordexp(configDir, &full_path, 0);
  if (full_path.we_wordv != NULL && full_path.we_wordv[0] != NULL) {
    strcpy(configDir, full_path.we_wordv[0]);
  }
  else {
    strcpy(configDir, "/etc/zddt");
    printf("configDir:%s not there, use default value: /etc/zddt", configDir);
  }
  wordfree(&full_path);

  sprintf(fileName, "%s/task.cfg", configDir);
  fp = fopen(fileName, "r");
  if (fp == NULL) {
    printf("option file:%s not found, all options are set to system default\n", fileName);
    return;
  }

  line = NULL;
  while (!feof(fp)) {
    tfree(line);
    line = option = value = NULL;
    len = olen = vlen = 0;

    getline(&line, &len, fp);
    if (line == NULL) break;

    paGetToken(line, &option, &olen);
    if (olen == 0) continue;
    option[olen] = 0;

    paGetToken(option + olen + 1, &value, &vlen);
    if (vlen == 0) continue;
    value[vlen] = 0;

    if (strcasecmp(option, "logDir") == 0) {
      strcpy(logDir, value);
    }
    else if (strcasecmp(option, "loopRun") == 0) {
      gsArg.loopRun = atoi(value) != 0;
    }
    else {}
  }

  tfree(line);
  fclose(fp);

  struct stat  dirstat;
  if (stat(logDir, &dirstat) < 0)
    mkdir(logDir, 0755);

  char temp[128] = { 0 };
  sprintf(temp, "%s/tasklog", logDir);
  if (taosOpenLogFileWithMaxLines(temp, tsNumOfLogLines, 1) < 0)
    printf("failed to init log file\n");

  gsPrint("   task config info ");
  gsPrint("==================================");
  gsPrint(" configDir: %s", configDir);
  gsPrint(" logDir: %s", logDir);
  gsPrint(" loopRun: %d", gsArg.loopRun);
}

void gsPrintHelp()
{
  fprintf(stdout, "this program read data from odbc datasources to CSV or TAR.GZ files.\n");
  fprintf(stdout, "-c config file's directory, default is /var/log/zddt \n");
  gsExit(EXIT_SUCCESS);
}

void gsParseArgs(int argc, char *argv[])
{
  for (int i = 1; i < argc; ++i) {
    if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
      gsPrintHelp();
    }
    if (strcmp(argv[i], "-c") == 0){
      if (i < argc - 1) {
        strcpy(configDir, argv[++i]);
      }
      else {
        printf("'-c' requires a parameter, default:%s\n", configDir);
        exit(EXIT_FAILURE);
      }
    }
  }

  gsArg.henv = 0;
  gsArg.loopRun = false;
}

void gsUpdateConfig()
{

}

void gsConnectOdbc()
{
  taos_init();
  gsPrint("connect to Odbc success");
}

bool gsThreadCheckAllFinished()
{
  return true;
}

int gsThreadCheckExistFiles()
{
  return 0;
}

void gsThreadRun(GsThread *thread)
{

}

void *gsThreadFp(void *arg)
{
  GsThread *thread = (GsThread*)arg;  
  while (1) {  
    gsThreadRun(thread);
    gsUpdateConfig();

    bool allFinished = gsThreadCheckAllFinished();
    if (allFinished) {
      gsPrint("thread:%d all tasks are finished, quit this thread", thread->threadIndex);
      break;
    }

    while (true) {
      int existFiles = gsThreadCheckExistFiles(thread);
      if (existFiles > gsGlobal->maxExistFiles) {
        gsPrint("thread:%d existFiles:%d larger then pre-defined:%d, wait %d seconds", thread->threadIndex, existFiles, gsGlobal->maxExistFiles, GS_SLEEP_INTERVAL / 1000);
        taosMsleep(GS_SLEEP_INTERVAL);
      }
      else {
        gsPrint("thread:%d process next task:%d-%d", thread->threadIndex, thread->taskPos, thread->taskLen);
        break;
      }
    }
  }

  return NULL;
}

void gsInitParameters(int argc, char *argv[])
{
  gsPrint("parse parameters");
}

void gsInitThread()
{
  gsPrint("parse parameters");

  //RETCODE retcode;
  //SQLCHAR err[1024] = { 0 };
  //SQLCHAR sql[1024] = { 0 };
  //SQLCHAR sqlState[20] = { 0 };
  //SQLINTEGER nativeErr = 0;
  //SQLSMALLINT errlen;
  //SQLLEN lenp;



  gsGlobal->threads = (GsThread*)calloc(gsGlobal->threadNum, sizeof(GsThread));
  
  pthread_attr_t thattr;
  for (int i = 0; i < gsGlobal->threadNum; ++i) {
    GsThread *thread = gsGlobal->threads + i;
    thread->threadIndex = i;
    //if (SQLAllocHandle(SQL_HANDLE_DBC, gsGlobal->henv, &thread->hdbc) == SQL_ERROR) {
    //  gsError("thread:%d failed to alloc odbc dbc", thread->threadIndex);
    //  gsExit(EXIT_FAILURE);
    //}

    //retcode = SQLConnect(thread->hdbc, gsGlobal->odbcSourceName, SQL_NTS, gsGlobal->odbcUser, SQL_NTS, gsGlobal->odbcPassword, SQL_NTS);
    //if (retcode == SQL_ERROR) {
    //  SQLGetDiagRec(SQL_HANDLE_DBC, thread->hdbc, 1, sqlState, &nativeErr, err, sizeof(err), &errlen);
    //  gsError("thread:%d failed to create odbc connection, code:%d, state:%s, error:%s", thread->threadIndex, nativeErr, sqlState, err);
    //  gsExit(EXIT_FAILURE);
    //}

    //if (SQLAllocHandle(SQL_HANDLE_STMT, thread->hdbc, &thread->hstmt) == SQL_ERROR) {
    //  SQLGetDiagRec(SQL_HANDLE_STMT, thread->hstmt, 1, sqlState, &nativeErr, err, sizeof(err), &errlen);
    //  gsError("thread:%d failed to create odbc statment, code:%d, state:%s, error:%s", thread->threadIndex, nativeErr, sqlState, err);
    //  gsExit(EXIT_FAILURE);
    //}

    thread->taskPos = thread->taskLen = 0;
    for (int j = 0; j < gsGlobal->taskLen; ++j) {
      if (j % gsGlobal->threadNum == thread->threadIndex) {
        thread->taskLen++;
      }
    }
    thread->taskList = (GsTask*)calloc(thread->taskLen, sizeof(GsTask));
    thread->taskLen = 0;
    for (int j = 0; j < gsGlobal->taskLen; ++j) {
      if (j % gsGlobal->threadNum == thread->threadIndex) {
        thread->taskList[thread->taskLen++] = gsGlobal->tasks[j];
      }
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

void gsCleanThread()
{
  gsPrint("reset threads");
}

void gsCleanParameters()
{
  gsPrint("reset parameters");
}

void gsRun(int argc, char *argv[])
{
  static int runTimes = 0;
  gsPrint("======= loop:%d start ========", ++runTimes);

  gsReadConfigFile();
  gsCleanThread();
  gsCleanParameters();

  gsInitParameters(argc, argv);
  gsInitThread();

  int64_t start = taosGetTimestampMs();
  for (int i = 0; i < gsGlobal->threadNum; i++) {
    pthread_join(gsGlobal->threads[i].threadID, NULL);
  }
  int64_t end = taosGetTimestampMs();
  gsPrint("working thread run finished");
  
  int64_t totalTasks = 0;
  int64_t successTasks = 0;
  int64_t failedTasks = 0;
  int64_t norunTasks = 0;
  int64_t totalRows = 0;  
  for (int i = 0; i < gsGlobal->threadNum; ++i) {
    GsThread *thread = &gsGlobal->threads[i];
    totalTasks += thread->totalTasks;
    successTasks += thread->successTasks;
    failedTasks += thread->failedTasks;
    norunTasks += thread->norunTasks;
    totalRows += thread->totalRows;
    gsPrint("thread:%01d, totalTasks:%lld successTasks:%lld, failedTasks:%lld, norunTasks:%lld totalRows:%lld"
      , thread->threadIndex, thread->totalTasks, thread->successTasks, thread->failedTasks, thread->norunTasks, thread->totalRows);
  }
  gsPrint("total timespent:%fsec, totalTasks:%lld successTasks:%lld, failedTasks:%lld, norunTasks:%lld totalRows:%lld"
    , (float)end/(float)start, totalTasks, successTasks, failedTasks, norunTasks, totalRows);

  gsPrint("======= loop:%d finished ========", runTimes);
}

bool gsConfigFileModified()
{
  static int lastTime = 0;
  int curTime = 1;
  if (curTime != lastTime) {
    if (lastTime != 0)
      gsPrint("config file:%s time changed from:%d to %d", logDir, lastTime, curTime);
    lastTime = curTime;
    return true;
  }

  return false;
}

void gsInitOdbcEnv()
{
  if (SQLAllocHandle(SQL_HANDLE_ENV, NULL, &gsArg.henv) == SQL_ERROR) {
    gsError("failed to alloc odbc env");
    gsExit(EXIT_FAILURE);
  }
  if (SQLSetEnvAttr(gsArg.henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, SQL_IS_INTEGER) == SQL_ERROR) {
    gsError("failed to set odbc env variable of odbc3");
    gsExit(EXIT_FAILURE);
  }
}

int main(int argc, char *argv[])
{
  gsParseArgs(argc, argv);
  gsReadBasicConfig();

  gsInitOdbcEnv();
  gsConfigFileModified();

  while (true) {
    gsRun(argc, argv);

    if (!gsArg.loopRun) {
      gsExit(EXIT_SUCCESS);
    }

    if (!gsConfigFileModified()) {
      taosMsleep(GS_SLEEP_INTERVAL);
    }
  }
}

//
//
//SQLHENV henv;
//SQLHDBC hdbc;
//SQLHSTMT hstmt;
//SQLCHAR pszSourceName[1024] = "test1";   //IP=172.16.170.137,USER=root,PASS=taosdata,db=odbc1,port=6101,log_level=FULL,LOG_DIR=C:\Users\slguan\Desktop\dll
//SQLCHAR pszUserId[20] = "root";
//SQLCHAR pszPassword[20] = "123456";
//SQLCHAR defaultDb[20] = "";
//SQLCHAR defaultTable[20] = "";
//RETCODE retcode;
//SQLCHAR err[1024] = { 0 };
//SQLCHAR sql[1024] = { 0 };
//SQLCHAR sqlState[20] = { 0 };
//SQLINTEGER nativeErr = 0;
//SQLSMALLINT errlen;
//SQLLEN lenp;
//
//int main(int argc, char *argv[])
//{
//  printf("odbc test begin \n");
//  
//  printf("======>>  Test_SQLExecute\n");
//
//  if (SQLAllocHandle(SQL_HANDLE_ENV, NULL, &henv) == SQL_ERROR) {
//    return -1;
//  }
//  if (SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, SQL_IS_INTEGER) == SQL_ERROR) {
//    return -1;
//  }
//  if (SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc) == SQL_ERROR) {
//    return -1;
//  }
//
//  retcode = SQLConnect(hdbc, pszSourceName, SQL_NTS, pszUserId, SQL_NTS, pszPassword, SQL_NTS);
//  if (retcode == SQL_ERROR) {
//    SQLGetDiagRec(SQL_HANDLE_STMT, hstmt, 1, sqlState, &nativeErr, err, sizeof(err), &errlen);
//    printf("code:%d, state:%s, error:%s.\n", nativeErr, sqlState, err);
//    return -1;
//  }
//
//  if (SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt) == SQL_ERROR) {
//    SQLGetDiagRec(SQL_HANDLE_STMT, hstmt, 1, sqlState, &nativeErr, err, sizeof(err), &errlen);
//    printf("code:%d, state:%s, error:%s.\n", nativeErr, sqlState, err);
//    return -1;
//  }
//
//  //char *sql = "show databases";
//  //retcode = SQLExecDirect(hstmt, sql, SQL_NTS);
//  //if (retcode == SQL_ERROR) {
//  //  SQLGetDiagRec(SQL_HANDLE_STMT, hstmt, 1, sqlState, &nativeErr, err, sizeof(err), &errlen);
//  //  printf("code:%d, state:%s, error:%s.\n", nativeErr, sqlState, err);
//  //  return -1;
//  //}
//  //else {
//  //  printf("success execute sql:%s .\n", sql);
//  //}
//
//  SQLSMALLINT ncols;
//
//  sprintf(sql, "select v, c, b from zddt.bills");
//  retcode = SQLExecDirect(hstmt, sql, SQL_NTS);
//
//  printf("01->\n");
//  if (retcode == SQL_ERROR) {
//    SQLGetDiagRec(SQL_HANDLE_STMT, hstmt, 1, sqlState, &nativeErr, err, sizeof(err), &errlen);
//    printf("code:%d, state:%s, error:%s.\n", nativeErr, sqlState, err);
//    SQLError(henv, hdbc, hstmt, sqlState, &nativeErr, err, sizeof(err), &errlen);
//    printf("code:%d, state:%s, error:%s.\n", nativeErr, sqlState, err);
//    return -1;
//  }
//
//  printf("11->\n");
//
//  retcode = SQLNumResultCols(hstmt, &ncols);
//  if (retcode == SQL_ERROR) {
//    SQLGetDiagRec(SQL_HANDLE_STMT, hstmt, 1, sqlState, &nativeErr, err, sizeof(err), &errlen);
//    printf("code:%d, state:%s, error:%s.\n", nativeErr, sqlState, err);
//    SQLError(henv, hdbc, hstmt, sqlState, &nativeErr, err, sizeof(err), &errlen);
//    printf("code:%d, state:%s, error:%s.\n", nativeErr, sqlState, err);
//    return -1;
//  }
//
//  if (ncols != 3) {
//    printf("ncols:%d", ncols);
//    return -1;
//  }
//
//  printf("12->\n");
//
//  printf("ncols:%d\n", ncols);
//
//  printf("13->\n");
//
//  for (int col = 0; col < ncols; col++) {
//    SQLCHAR name[24] = { 0 };
//    SQLSMALLINT len;
//    SQLSMALLINT type;
//    SQLULEN size;
//    SQLSMALLINT digits;
//    SQLSMALLINT nullable;
//    SQLDescribeCol(hstmt, col + 1, name, sizeof(name), &len, &type, &size, &digits, &nullable);
//    printf("col:%d, name:%s, len:%d, type:%d, size:%d, digits:%d, null:%d\n", col + 1, name, len, type, size, digits, nullable);
//
//    //if (col == 0 && strcmp(name, "ts") != 0) {
//    //  printf("col1 != ts");
//    //  return -1;
//    //}
//  }
//
//  printf("14->\n");
//
//  char v[20];
//  char c[20];
//  int64_t b;
//  int index = 0;
//  while (SQLFetch(hstmt) == SQL_SUCCESS) {
//    SQLGetData(hstmt, 1, SQL_C_CHAR, &v, sizeof(v), &lenp);
//    SQLGetData(hstmt, 2, SQL_C_CHAR, &c, sizeof(c), &lenp);
//    SQLGetData(hstmt, 3, SQL_C_SBIGINT, &b, sizeof(b), &lenp);
//    printf("index:%d, col1:%s, col2:%s, col3:%lld \n", ++index, v, c, b);
//  }
//
//  if (index != 5) {
//    printf("index != 5.\n");
//    return -1;
//  }
//
//
//
//  return 0;
//
//
//  printf("odbc test passed, press any key to quit \n");
//  return getchar();
//}

