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
#define _XOPEN_SOURCE
#define _DEFAULT_SOURCE

#include "os.h"
#include "shell.h"
#include "shellCommand.h"
#include "tglobal.h"
#include "tutil.h"

static char **shellSQLFiles = NULL;
static int32_t shellSQLFileNum = 0;
static char shellTablesSQLFile[TSDB_FILENAME_LEN] = {0};

typedef struct {
  TdThread threadID;
  int       threadIndex;
  int       totalThreads;
  void     *taos;
} ShellThreadObj;

static int shellGetFilesNum(const char *directoryName, const char *prefix)
{
  char cmd[1024] = { 0 };
  sprintf(cmd, "ls %s/*.%s | wc -l ", directoryName, prefix);

  char buf[1024] = { 0 };
  if (taosSystem(cmd, buf, sizeof(buf)) < 0) {
    fprintf(stderr, "ERROR: failed to execute:%s, error:%s\n", cmd, strerror(errno));
    exit(0);
  }

  int fileNum = 0;
  if (sscanf(buf, "%d", &fileNum) != 1) {
    fprintf(stderr, "ERROR: failed to execute:%s, parse result error\n", cmd);
    exit(0);
  }

  if (fileNum <= 0) {
    fprintf(stderr, "ERROR: directory:%s is empry\n", directoryName);
    exit(0);
  }

  return fileNum;
}

static void shellParseDirectory(const char *directoryName, const char *prefix, char **fileArray, int totalFiles)
{
  char cmd[1024] = { 0 };
  sprintf(cmd, "ls %s/*.%s | sort", directoryName, prefix);

  char buf[1024] = { 0 };
  if (taosSystem(cmd, buf, sizeof(buf)) < 0) {
    fprintf(stderr, "ERROR: failed to execute:%s, error:%s\n", cmd, strerror(errno));
    exit(0);
  }

  int fileNum = 0;
  while (sscanf(buf, "%128s", fileArray[fileNum++])) {
    if (strcmp(fileArray[fileNum-1], shellTablesSQLFile) == 0) {
      fileNum--;
    }
    if (fileNum >= totalFiles) {
      break;
    }
  }

  if (fileNum != totalFiles) {
    fprintf(stderr, "ERROR: directory:%s changed while read\n", directoryName);
    exit(0);
  }
}

static void shellCheckTablesSQLFile(const char *directoryName)
{
  sprintf(shellTablesSQLFile, "%s/tables.sql", directoryName);

  if (taosFStatFile(shellTablesSQLFile, NULL, NULL) < 0) {
    shellTablesSQLFile[0] = 0;
  }
}

static void shellMallocSQLFiles()
{
  shellSQLFiles = (char**)taosMemoryCalloc(shellSQLFileNum, sizeof(char*));
  for (int i = 0; i < shellSQLFileNum; i++) {
    shellSQLFiles[i] = taosMemoryCalloc(1, TSDB_FILENAME_LEN);
  }
}

static void shellGetDirectoryFileList(char *inputDir)
{
  if (!taosDirExist(inputDir)) {
    fprintf(stderr, "ERROR: %s not exist\n", inputDir);
    exit(0);
  }

  if (taosIsDir(inputDir)) {
    shellCheckTablesSQLFile(inputDir);
    shellSQLFileNum = shellGetFilesNum(inputDir, "sql");
    int totalSQLFileNum = shellSQLFileNum;
    if (shellTablesSQLFile[0] != 0) {
      shellSQLFileNum--;
    }
    shellMallocSQLFiles();
    shellParseDirectory(inputDir, "sql", shellSQLFiles, shellSQLFileNum);
    fprintf(stdout, "\nstart to dispose %d files in %s\n", totalSQLFileNum, inputDir);
  }
  else {
    fprintf(stderr, "ERROR: %s is not a directory\n", inputDir);
    exit(0);
  }
}

static void shellSourceFile(TAOS *con, char *fptr) {
  wordexp_t full_path;
  int       read_len = 0;
  char *    cmd = taosMemoryMalloc(tsMaxSQLStringLen);
  size_t    cmd_len = 0;
  char *    line = NULL;

  if (wordexp(fptr, &full_path, 0) != 0) {
    fprintf(stderr, "ERROR: illegal file name\n");
    taosMemoryFree(cmd);
    return;
  }

  char *fname = full_path.we_wordv[0];
  if (fname == NULL) {
    fprintf(stderr, "ERROR: invalid filename\n");
    taosMemoryFree(cmd);
    return;
  }

  /*
  if (access(fname, F_OK) != 0) {
    fprintf(stderr, "ERROR: file %s is not exist\n", fptr);
    
    wordfree(&full_path);
    taosMemoryFree(cmd);
    return;
  }
  
  if (access(fname, R_OK) != 0) {
    fprintf(stderr, "ERROR: file %s is not readable\n", fptr);
    
    wordfree(&full_path);
    taosMemoryFree(cmd);
    return;
  }
  */

  // FILE *f = fopen(fname, "r");
  TdFilePtr pFile = taosOpenFile(fname, TD_FILE_READ | TD_FILE_STREAM);
  if (pFile == NULL) {
    fprintf(stderr, "ERROR: failed to open file %s\n", fname);
    wordfree(&full_path);
    taosMemoryFree(cmd);
    return;
  }

  fprintf(stdout, "begin import file:%s\n", fname);

  int lineNo = 0;
  while ((read_len = taosGetLineFile(pFile, &line)) != -1) {
    ++lineNo;
    if (read_len >= tsMaxSQLStringLen) continue;
    line[--read_len] = '\0';

    if (read_len == 0 || isCommentLine(line)) {  // line starts with #
      continue;
    }

    if (line[read_len - 1] == '\\') {
      line[read_len - 1] = ' ';
      memcpy(cmd + cmd_len, line, read_len);
      cmd_len += read_len;
      continue;
    }

    memcpy(cmd + cmd_len, line, read_len);
    
    TAOS_RES* pSql = taos_query(con, cmd);
    int32_t code = taos_errno(pSql);
    
    if (code != 0) {
      fprintf(stderr, "DB error: %s: %s (%d)\n", taos_errstr(pSql), fname, lineNo);
    }
    
    /* free local resouce: allocated memory/metric-meta refcnt */
    taos_free_result(pSql);

    memset(cmd, 0, MAX_COMMAND_SIZE);
    cmd_len = 0;
  }

  taosMemoryFree(cmd);
  if(line != NULL) taosMemoryFree(line);
  wordfree(&full_path);
  taosCloseFile(&pFile);
}

void* shellImportThreadFp(void *arg)
{
  ShellThreadObj *pThread = (ShellThreadObj*)arg;
  setThreadName("shellImportThrd");

  for (int f = 0; f < shellSQLFileNum; ++f) {
    if (f % pThread->totalThreads == pThread->threadIndex) {
      char *SQLFileName = shellSQLFiles[f];
      shellSourceFile(pThread->taos, SQLFileName);
    }
  }

  return NULL;
}

static void shellRunImportThreads(SShellArguments* _args)
{
  TdThreadAttr thattr;
  ShellThreadObj *threadObj = (ShellThreadObj *)taosMemoryCalloc(_args->threadNum, sizeof(ShellThreadObj));
  for (int t = 0; t < _args->threadNum; ++t) {
    ShellThreadObj *pThread = threadObj + t;
    pThread->threadIndex = t;
    pThread->totalThreads = _args->threadNum;
    pThread->taos = taos_connect(_args->host, _args->user, _args->password, _args->database, tsDnodeShellPort);
    if (pThread->taos == NULL) {
      fprintf(stderr, "ERROR: thread:%d failed connect to TDengine, error:%s\n", pThread->threadIndex, "null taos"/*taos_errstr(pThread->taos)*/);
      exit(0);
    }

    taosThreadAttrInit(&thattr);
    taosThreadAttrSetDetachState(&thattr, PTHREAD_CREATE_JOINABLE);

    if (taosThreadCreate(&(pThread->threadID), &thattr, shellImportThreadFp, (void*)pThread) != 0) {
      fprintf(stderr, "ERROR: thread:%d failed to start\n", pThread->threadIndex);
      exit(0);
    }
  }

  for (int t = 0; t < _args->threadNum; ++t) {
    taosThreadJoin(threadObj[t].threadID, NULL);
  }

  for (int t = 0; t < _args->threadNum; ++t) {
    taos_close(threadObj[t].taos);
  }
  taosMemoryFree(threadObj);
}

void source_dir(TAOS* con, SShellArguments* _args) {
  shellGetDirectoryFileList(_args->dir);
  int64_t start = taosGetTimestampMs();

  if (shellTablesSQLFile[0] != 0) {
    shellSourceFile(con, shellTablesSQLFile);
    int64_t end = taosGetTimestampMs();
    fprintf(stdout, "import %s finished, time spent %.2f seconds\n", shellTablesSQLFile, (end - start) / 1000.0);
  }

  shellRunImportThreads(_args);
  int64_t end = taosGetTimestampMs();
  fprintf(stdout, "import %s finished, time spent %.2f seconds\n", _args->dir, (end - start) / 1000.0);
}
