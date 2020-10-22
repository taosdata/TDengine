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
  pthread_t threadID;
  int       threadIndex;
  int       totalThreads;
  void     *taos;
} ShellThreadObj;

static int shellGetFilesNum(const char *directoryName, const char *prefix)
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

static void shellParseDirectory(const char *directoryName, const char *prefix, char **fileArray, int totalFiles)
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

  pclose(fp);
}

static void shellCheckTablesSQLFile(const char *directoryName)
{
  sprintf(shellTablesSQLFile, "%s/tables.sql", directoryName);

  struct stat fstat;
  if (stat(shellTablesSQLFile, &fstat) < 0) {
    shellTablesSQLFile[0] = 0;
  }
}

static void shellMallocSQLFiles()
{
  shellSQLFiles = (char**)calloc(shellSQLFileNum, sizeof(char*));
  for (int i = 0; i < shellSQLFileNum; i++) {
    shellSQLFiles[i] = calloc(1, TSDB_FILENAME_LEN);
  }
}

static void shellGetDirectoryFileList(char *inputDir)
{
  struct stat fileStat;
  if (stat(inputDir, &fileStat) < 0) {
    fprintf(stderr, "ERROR: %s not exist\n", inputDir);
    exit(0);
  }

  if (fileStat.st_mode & S_IFDIR) {
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
  char *    cmd = malloc(tsMaxSQLStringLen);
  size_t    cmd_len = 0;
  char *    line = NULL;
  size_t    line_len = 0;

  if (wordexp(fptr, &full_path, 0) != 0) {
    fprintf(stderr, "ERROR: illegal file name\n");
    free(cmd);
    return;
  }

  char *fname = full_path.we_wordv[0];
  if (fname == NULL) {
    fprintf(stderr, "ERROR: invalid filename\n");
    free(cmd);
    return;
  }

  /*
  if (access(fname, F_OK) != 0) {
    fprintf(stderr, "ERROR: file %s is not exist\n", fptr);
    
    wordfree(&full_path);
    free(cmd);
    return;
  }
  
  if (access(fname, R_OK) != 0) {
    fprintf(stderr, "ERROR: file %s is not readable\n", fptr);
    
    wordfree(&full_path);
    free(cmd);
    return;
  }
  */

  FILE *f = fopen(fname, "r");
  if (f == NULL) {
    fprintf(stderr, "ERROR: failed to open file %s\n", fname);
    wordfree(&full_path);
    free(cmd);
    return;
  }

  fprintf(stdout, "begin import file:%s\n", fname);

  int lineNo = 0;
  while ((read_len = getline(&line, &line_len, f)) != -1) {
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

  free(cmd);
  if (line) free(line);
  wordfree(&full_path);
  fclose(f);
}

void* shellImportThreadFp(void *arg)
{
  ShellThreadObj *pThread = (ShellThreadObj*)arg;
  for (int f = 0; f < shellSQLFileNum; ++f) {
    if (f % pThread->totalThreads == pThread->threadIndex) {
      char *SQLFileName = shellSQLFiles[f];
      shellSourceFile(pThread->taos, SQLFileName);
    }
  }

  return NULL;
}

static void shellRunImportThreads(SShellArguments* args)
{
  pthread_attr_t thattr;
  ShellThreadObj *threadObj = (ShellThreadObj *)calloc(args->threadNum, sizeof(ShellThreadObj));
  for (int t = 0; t < args->threadNum; ++t) {
    ShellThreadObj *pThread = threadObj + t;
    pThread->threadIndex = t;
    pThread->totalThreads = args->threadNum;
    pThread->taos = taos_connect(args->host, args->user, args->password, args->database, tsDnodeShellPort);
    if (pThread->taos == NULL) {
      fprintf(stderr, "ERROR: thread:%d failed connect to TDengine, error:%s\n", pThread->threadIndex, "null taos"/*taos_errstr(pThread->taos)*/);
      exit(0);
    }

    pthread_attr_init(&thattr);
    pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);

    if (pthread_create(&(pThread->threadID), &thattr, shellImportThreadFp, (void*)pThread) != 0) {
      fprintf(stderr, "ERROR: thread:%d failed to start\n", pThread->threadIndex);
      exit(0);
    }
  }

  for (int t = 0; t < args->threadNum; ++t) {
    pthread_join(threadObj[t].threadID, NULL);
  }

  for (int t = 0; t < args->threadNum; ++t) {
    taos_close(threadObj[t].taos);
  }
  free(threadObj);
}

void source_dir(TAOS* con, SShellArguments* args) {
  shellGetDirectoryFileList(args->dir);
  int64_t start = taosGetTimestampMs();

  if (shellTablesSQLFile[0] != 0) {
    shellSourceFile(con, shellTablesSQLFile);
    int64_t end = taosGetTimestampMs();
    fprintf(stdout, "import %s finished, time spent %.2f seconds\n", shellTablesSQLFile, (end - start) / 1000.0);
  }

  shellRunImportThreads(args);
  int64_t end = taosGetTimestampMs();
  fprintf(stdout, "import %s finished, time spent %.2f seconds\n", args->dir, (end - start) / 1000.0);
}
