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

#define _DEFAULT_SOURCE
#include "tlog.h"
#include "os.h"
#include "tconfig.h"
#include "tglobal.h"
#include "tjson.h"
#include "ttime.h"
#include "tutil.h"
#include "tcommon.h"

#define LOG_MAX_LINE_SIZE              (10024)
#define LOG_MAX_LINE_BUFFER_SIZE       (LOG_MAX_LINE_SIZE + 3)
#define LOG_MAX_STACK_LINE_SIZE        (512)
#define LOG_MAX_STACK_LINE_BUFFER_SIZE (LOG_MAX_STACK_LINE_SIZE + 3)
#define LOG_MAX_LINE_DUMP_SIZE         (1024 * 1024)
#define LOG_MAX_LINE_DUMP_BUFFER_SIZE  (LOG_MAX_LINE_DUMP_SIZE + 128)

#define LOG_FILE_DAY_LEN 64

#define LOG_DEFAULT_BUF_SIZE (20 * 1024 * 1024)  // 20MB
#define LOG_SLOW_BUF_SIZE    (10 * 1024 * 1024)  // 10MB

#define LOG_DEFAULT_INTERVAL 25
#define LOG_INTERVAL_STEP    5
#define LOG_MIN_INTERVAL     5
#define LOG_MAX_INTERVAL     25
#define LOG_MAX_WAIT_MSEC    1000

#define LOG_BUF_BUFFER(x) ((x)->buffer)
#define LOG_BUF_START(x)  ((x)->buffStart)
#define LOG_BUF_END(x)    ((x)->buffEnd)
#define LOG_BUF_SIZE(x)   ((x)->buffSize)
#define LOG_BUF_MUTEX(x)  ((x)->buffMutex)

#ifdef TD_ENTERPRISE
#define LOG_EDITION_FLG ("E")
#else
#define LOG_EDITION_FLG ("C")
#endif

typedef enum {
  LOG_OUTPUT_FILE = 0,    // default
  LOG_OUTPUT_STDOUT = 1,  // stdout set by -o option on the command line
  LOG_OUTPUT_STDERR = 2,  // stderr set by -o option on the command line
  LOG_OUTPUT_NULL = 4,    // /dev/null set by -o option on the command line
} ELogOutputType;

typedef struct {
  char         *buffer;
  int32_t       buffStart;
  int32_t       buffEnd;
  int32_t       buffSize;
  int32_t       minBuffSize;
  TdFilePtr     pFile;
  int32_t       stop;
  TdThread      asyncThread;
  TdThreadMutex buffMutex;
  int32_t       writeInterval;
  int32_t       lastDuration;
  int32_t       lock;
} SLogBuff;

typedef struct {
  int32_t       fileNum;
  int32_t       lines;
  int32_t       flag;
  int32_t       openInProgress;
  int64_t       lastKeepFileSec;
  int64_t       timestampToday;
  int8_t        outputType;  // ELogOutputType
  pid_t         pid;
  char          logName[PATH_MAX];
  char          slowLogName[PATH_MAX];
  SLogBuff     *logHandle;
  SLogBuff     *slowHandle;
  TdThreadMutex logMutex;
} SLogObj;

extern SConfig *tsCfg;
static int8_t   tsLogInited = 0;
static SLogObj  tsLogObj = {.fileNum = 1, .slowHandle = NULL};
static int64_t  tsAsyncLogLostLines = 0;
static int32_t  tsDaylightActive; /* Currently in daylight saving time. */
static SRWLatch tsLogRotateLatch = 0;

bool tsLogEmbedded = 0;
bool tsAsyncLog = true;
#ifdef ASSERT_NOT_CORE
bool tsAssert = false;
#else
bool tsAssert = true;
#endif
int32_t tsNumOfLogLines = 10000000;
int32_t tsLogKeepDays = 0;
char   *tsLogOutput = NULL;
LogFp   tsLogFp = NULL;
int64_t tsNumOfErrorLogs = 0;
int64_t tsNumOfInfoLogs = 0;
int64_t tsNumOfDebugLogs = 0;
int64_t tsNumOfTraceLogs = 0;
int64_t tsNumOfSlowLogs = 0;

// log
int32_t dDebugFlag = 131;
int32_t vDebugFlag = 131;
int32_t mDebugFlag = 131;
int32_t cDebugFlag = 131;
int32_t jniDebugFlag = 131;
int32_t tmrDebugFlag = 131;
int32_t uDebugFlag = 131;
int32_t rpcDebugFlag = 131;
int32_t qDebugFlag = 131;
int32_t stDebugFlag = 131;
int32_t wDebugFlag = 131;
int32_t azDebugFlag = 131;
int32_t tssDebugFlag = 131;
int32_t sDebugFlag = 131;
int32_t tsdbDebugFlag = 131;
int32_t tdbDebugFlag = 131;
int32_t tqDebugFlag = 131;
int32_t fsDebugFlag = 131;
int32_t metaDebugFlag = 131;
int32_t udfDebugFlag = 131;
int32_t smaDebugFlag = 131;
int32_t idxDebugFlag = 131;
int32_t sndDebugFlag = 131;
int32_t bndDebugFlag = 131;
int32_t xndDebugFlag = 131;
int32_t simDebugFlag = 131;
int32_t bseDebugFlag = 131;

int32_t tqClientDebugFlag = 131;

int64_t dbgEmptyW = 0;
int64_t dbgWN = 0;
int64_t dbgSmallWN = 0;
int64_t dbgBigWN = 0;
int64_t dbgWSize = 0;

static void     *taosAsyncOutputLog(void *param);
static int32_t   taosPushLogBuffer(SLogBuff *pLogBuf, const char *msg, int32_t msgLen);
static SLogBuff *taosLogBuffNew(int32_t bufSize);
static void      taosCloseLogByFd(TdFilePtr pFile);
static int32_t   taosInitNormalLog(const char *fn, int32_t maxFileNum);
static void      taosWriteLog(SLogBuff *pLogBuf);
static void      taosWriteSlowLog(SLogBuff *pLogBuf);

static int32_t taosStartLog() {
  TdThreadAttr threadAttr;
  (void)taosThreadAttrInit(&threadAttr);
#ifdef TD_COMPACT_OS
  (void)taosThreadAttrSetStackSize(&threadAttr, STACK_SIZE_SMALL);
#endif
  if (taosThreadCreate(&(tsLogObj.logHandle->asyncThread), &threadAttr, taosAsyncOutputLog, tsLogObj.logHandle) != 0) {
    return terrno;
  }
  (void)taosThreadAttrDestroy(&threadAttr);
  return 0;
}

static int32_t getDay(char *buf, int32_t bufSize) {
  time_t  t;
  int32_t code = taosTime(&t);
  if (code != 0) {
    return code;
  }
  struct tm tmInfo;
  if (taosLocalTime(&t, &tmInfo, buf, bufSize, NULL) != NULL) {
    TAOS_UNUSED(taosStrfTime(buf, bufSize, "%Y-%m-%d", &tmInfo));
  }
  return 0;
}

static void getFullPathName(char *fullName, const char *logName) {
  if (strlen(tsLogDir) != 0) {
    char lastC = tsLogDir[strlen(tsLogDir) - 1];
    if (lastC == '\\' || lastC == '/') {
      snprintf(fullName, PATH_MAX,
               "%s"
               "%s",
               tsLogDir, logName);
    } else {
      snprintf(fullName, PATH_MAX, "%s" TD_DIRSEP "%s", tsLogDir, logName);
    }
  } else {
    snprintf(fullName, PATH_MAX, "%s", logName);
  }
}

int32_t taosInitSlowLog() {
  char logFileName[64] = {0};
#ifdef CUS_PROMPT
  (void)snprintf(logFileName, 64, "%sSlowLog", CUS_PROMPT);
#else
  (void)snprintf(logFileName, 64, "taosSlowLog");
#endif

  getFullPathName(tsLogObj.slowLogName, logFileName);

  char    name[PATH_MAX + TD_TIME_STR_LEN] = {0};
  char    day[TD_TIME_STR_LEN] = {0};
  int32_t code = getDay(day, sizeof(day));
  if (code != 0) {
    (void)printf("failed to get day, reason:%s\n", tstrerror(code));
    return code;
  }
  (void)snprintf(name, PATH_MAX + TD_TIME_STR_LEN, "%s.%s", tsLogObj.slowLogName, day);

  tsLogObj.timestampToday = taosGetTimestampToday(TSDB_TIME_PRECISION_SECONDS, NULL);
  tsLogObj.slowHandle = taosLogBuffNew(LOG_SLOW_BUF_SIZE);
  if (tsLogObj.slowHandle == NULL) return terrno;

  TAOS_UNUSED(taosUmaskFile(0));
  tsLogObj.slowHandle->pFile = taosOpenFile(name, TD_FILE_CREATE | TD_FILE_READ | TD_FILE_WRITE | TD_FILE_APPEND);
  if (tsLogObj.slowHandle->pFile == NULL) {
    (void)printf("\nfailed to open slow log file:%s, reason:%s\n", name, strerror(ERRNO));
    return terrno;
  }

  return 0;
}

int32_t taosInitLogOutput(const char **ppLogName) {
  const char *pLog = tsLogOutput;
  const char *pLogName = NULL;
  if (pLog) {
    if (!tIsValidFilePath(pLog, NULL)) {
      fprintf(stderr, "invalid log output destination:%s, contains illegal char\n", pLog);
      return TSDB_CODE_INVALID_CFG;
    }
    if (0 == strcasecmp(pLog, "stdout")) {
      tsLogObj.outputType = LOG_OUTPUT_STDOUT;
      if (ppLogName) *ppLogName = pLog;
      return 0;
    }
    if (0 == strcasecmp(pLog, "stderr")) {
      tsLogObj.outputType = LOG_OUTPUT_STDERR;
      if (ppLogName) *ppLogName = pLog;
      return 0;
    }
    if (0 == strcasecmp(pLog, "/dev/null")) {
      tsLogObj.outputType = LOG_OUTPUT_NULL;
      if (ppLogName) *ppLogName = pLog;
      return 0;
    }
    int32_t len = strlen(pLog);
    if (len < 1) {
      fprintf(stderr, "invalid log output destination:%s, should not be empty\n", pLog);
      return TSDB_CODE_INVALID_CFG;
    }
    const char *p = pLog + (len - 1);
    if (*p == '/' || *p == '\\') {
      return 0;
    }

    if ((p = strrchr(pLog, '/')) || (p = strrchr(pLog, '\\'))) {
      pLogName = p + 1;
    } else {
      pLogName = pLog;
    }
    if (strcmp(pLogName, ".") == 0 || strcmp(pLogName, "..") == 0) {
      fprintf(stderr, "invalid log output destination:%s\n", pLog);
      return TSDB_CODE_INVALID_CFG;
    }

    if (!tIsValidFileName(pLogName, NULL)) {
      fprintf(stderr, "invalid log output destination:%s, contains illegal char\n", pLog);
      return TSDB_CODE_INVALID_CFG;
    }
    if (ppLogName) *ppLogName = pLogName;
  }
  return 0;
}

int32_t taosInitLog(const char *logName, int32_t maxFiles, bool tsc) {
  if (atomic_val_compare_exchange_8(&tsLogInited, 0, 1) != 0) return 0;
  int32_t code = osUpdate();
  if (code != 0) {
    uError("failed to update os info, reason:%s", tstrerror(code));
  }

  if (tsLogObj.outputType == LOG_OUTPUT_STDOUT || tsLogObj.outputType == LOG_OUTPUT_STDERR ||
      tsLogObj.outputType == LOG_OUTPUT_NULL) {
    return 0;
  }

  TAOS_CHECK_RETURN(taosInitNormalLog(logName, maxFiles));
  if (tsc) {
    TAOS_CHECK_RETURN(taosInitSlowLog());
  }
  TAOS_CHECK_RETURN(taosStartLog());
  return 0;
}

void taosSetNoNewFile() { tsLogObj.openInProgress = 1; }

static void taosStopLog() {
  if (tsLogObj.logHandle) {
    tsLogObj.logHandle->stop = 1;
  }
  if (tsLogObj.slowHandle) {
    tsLogObj.slowHandle->stop = 1;
  }
}

void taosCloseLog() {
  taosStopLog();

  if (tsLogObj.logHandle != NULL && taosCheckPthreadValid(tsLogObj.logHandle->asyncThread)) {
    (void)taosThreadJoin(tsLogObj.logHandle->asyncThread, NULL);
    taosThreadClear(&tsLogObj.logHandle->asyncThread);
  }

  if (tsLogObj.slowHandle != NULL) {
    (void)taosThreadMutexDestroy(&tsLogObj.slowHandle->buffMutex);
    (void)taosCloseFile(&tsLogObj.slowHandle->pFile);
    taosMemoryFreeClear(tsLogObj.slowHandle->buffer);
    taosMemoryFreeClear(tsLogObj.slowHandle);
  }

  if (tsLogObj.logHandle != NULL) {
    tsLogInited = 0;

    (void)taosThreadMutexDestroy(&tsLogObj.logHandle->buffMutex);
    (void)taosCloseFile(&tsLogObj.logHandle->pFile);
    taosMemoryFreeClear(tsLogObj.logHandle->buffer);
    (void)taosThreadMutexDestroy(&tsLogObj.logMutex);
    taosMemoryFreeClear(tsLogObj.logHandle);
    tsLogObj.logHandle = NULL;
  }
  taosMemoryFreeClear(tsLogOutput);
}

static bool taosLockLogFile(TdFilePtr pFile) {
  if (pFile == NULL) return false;

  if (tsLogObj.fileNum > 1) {
    int32_t ret = taosLockFile(pFile);
    if (ret == 0) {
      return true;
    }
  }

  return false;
}

static void taosUnLockLogFile(TdFilePtr pFile) {
  if (pFile == NULL) return;

  if (tsLogObj.fileNum > 1) {
    int32_t code = taosUnLockFile(pFile);
    if (code != 0) {
      TAOS_UNUSED(printf("failed to unlock log file:%p, reason:%s\n", pFile, tstrerror(code)));
    }
  }
}

static void taosReserveOldLog(char *oldName, char *keepName) {
  if (tsLogKeepDays == 0) {
    keepName[0] = 0;
    return;
  }

  int32_t code = 0;
  int64_t fileSec = taosGetTimestampSec();
  if (tsLogObj.lastKeepFileSec < fileSec) {
    tsLogObj.lastKeepFileSec = fileSec;
  } else {
    fileSec = ++tsLogObj.lastKeepFileSec;
  }
  snprintf(keepName, PATH_MAX + 20, "%s.%" PRId64, tsLogObj.logName, fileSec);
  if ((code = taosRenameFile(oldName, keepName))) {
    keepName[0] = 0;
    uError("failed to rename file:%s to %s since %s", oldName, keepName, tstrerror(code));
  }
}

static void taosKeepOldLog(char *oldName) {
  if (oldName[0] != 0) {
    int32_t   code = 0, lino = 0;
    TdFilePtr oldFile = NULL;
    if ((oldFile = taosOpenFile(oldName, TD_FILE_READ))) {
      TAOS_CHECK_GOTO(taosLockFile(oldFile), &lino, _exit2);
      char compressFileName[PATH_MAX + 20];
      snprintf(compressFileName, PATH_MAX + 20, "%s.gz", oldName);
      TAOS_CHECK_GOTO(taosCompressFile(oldName, compressFileName), &lino, _exit1);
      TAOS_CHECK_GOTO(taosRemoveFile(oldName), &lino, _exit1);
    _exit1:
      TAOS_UNUSED(taosUnLockFile(oldFile));
    _exit2:
      TAOS_UNUSED(taosCloseFile(&oldFile));
    } else {
      code = terrno;
    }
    if (code != 0 && tsLogEmbedded == 1) {  // print error messages only in embedded log mode
      // avoid using uWarn or uError, as they may open a new log file and potentially cause a deadlock.
      fprintf(stderr, "WARN: failed at line %d to keep old log file:%s, reason:%s\n", lino, oldName, tstrerror(code));
    }
  }
}
typedef struct {
  TdFilePtr pOldFile;
  char      keepName[PATH_MAX + 20];
} OldFileKeeper;
static OldFileKeeper *taosOpenNewFile() {
  char keepName[PATH_MAX + 20];
  TAOS_UNUSED(snprintf(keepName, sizeof(keepName), "%s.%d", tsLogObj.logName, tsLogObj.flag));

  tsLogObj.flag ^= 1;
  tsLogObj.lines = 0;
  char name[PATH_MAX + 20];
  TAOS_UNUSED(snprintf(name, sizeof(name), "%s.%d", tsLogObj.logName, tsLogObj.flag));

  TAOS_UNUSED(taosUmaskFile(0));

  TdFilePtr pFile = taosOpenFile(name, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pFile == NULL) {
    tsLogObj.flag ^= 1;
    tsLogObj.lines = tsNumOfLogLines - 1000;
    uError("open new log file %s fail! reason:%s, reuse lastlog", name, tstrerror(terrno));
    return NULL;
  }

  TAOS_UNUSED(taosLockLogFile(pFile));
  if (taosLSeekFile(pFile, 0, SEEK_SET) < 0) {
    uWarn("failed to seek file:%s, reason:%s", name, tstrerror(terrno));
  }

  TdFilePtr pOldFile = tsLogObj.logHandle->pFile;
  tsLogObj.logHandle->pFile = pFile;
  tsLogObj.lines = 0;
  OldFileKeeper *oldFileKeeper = taosMemoryMalloc(sizeof(OldFileKeeper));
  if (oldFileKeeper == NULL) {
    uError("create old log keep info faild! mem is not enough.");
    return NULL;
  }
  oldFileKeeper->pOldFile = pOldFile;
  taosReserveOldLog(keepName, oldFileKeeper->keepName);

  uInfo("   new log file:%d is opened", tsLogObj.flag);
  uInfo("==================================");
  return oldFileKeeper;
}

static void *taosThreadToCloseOldFile(void *param) {
  if (!param) return NULL;
  OldFileKeeper *oldFileKeeper = (OldFileKeeper *)param;
  taosSsleep(20);
  taosWLockLatch(&tsLogRotateLatch);
  taosCloseLogByFd(oldFileKeeper->pOldFile);
  taosKeepOldLog(oldFileKeeper->keepName);
  taosMemoryFree(oldFileKeeper);
  if (tsLogKeepDays != 0) {
    taosRemoveOldFiles(tsLogDir, abs(tsLogKeepDays));
  }
  taosWUnLockLatch(&tsLogRotateLatch);
  return NULL;
}

static int32_t taosOpenNewLogFile() {
  (void)taosThreadMutexLock(&tsLogObj.logMutex);

  if (tsLogObj.lines > tsNumOfLogLines && tsLogObj.openInProgress == 0) {
    tsLogObj.openInProgress = 1;

    uInfo("open new log file ......");
    TdThread     thread;
    TdThreadAttr attr;
    (void)taosThreadAttrInit(&attr);
    (void)taosThreadAttrSetDetachState(&attr, PTHREAD_CREATE_DETACHED);
#ifdef TD_COMPACT_OS
    (void)taosThreadAttrSetStackSize(&attr, STACK_SIZE_SMALL);
#endif
    OldFileKeeper *oldFileKeeper = taosOpenNewFile();
    if (!oldFileKeeper) {
      tsLogObj.openInProgress = 0;
      TAOS_UNUSED(taosThreadMutexUnlock(&tsLogObj.logMutex));
      (void)taosThreadAttrDestroy(&attr);
      return terrno;
    }
    if (taosThreadCreate(&thread, &attr, taosThreadToCloseOldFile, oldFileKeeper) != 0) {
      uError("failed to create thread to close old log file");
      taosMemoryFreeClear(oldFileKeeper);
    }
    (void)taosThreadAttrDestroy(&attr);
    tsLogObj.openInProgress = 0;
  }

  (void)taosThreadMutexUnlock(&tsLogObj.logMutex);

  return 0;
}

void taosOpenNewSlowLogFile() {
  (void)taosThreadMutexLock(&tsLogObj.logMutex);
  int64_t delta = taosGetTimestampSec() - tsLogObj.timestampToday;
  if (delta >= 0 && delta < 86400) {
    uInfo("timestampToday is already equal to today, no need to open new slow log file");
    (void)taosThreadMutexUnlock(&tsLogObj.logMutex);
    return;
  }

  for (int32_t i = 1; atomic_val_compare_exchange_32(&tsLogObj.slowHandle->lock, 0, 1) == 1; ++i) {
    if (i % 1000 == 0) {
      TAOS_UNUSED(sched_yield());
    }
  }
  tsLogObj.slowHandle->lastDuration = LOG_MAX_WAIT_MSEC;  // force write
  taosWriteLog(tsLogObj.slowHandle);
  atomic_store_32(&tsLogObj.slowHandle->lock, 0);

  char    day[TD_TIME_STR_LEN] = {0};
  int32_t code = getDay(day, sizeof(day));
  if (code != 0) {
    uError("failed to get day, reason:%s", tstrerror(code));
    (void)taosThreadMutexUnlock(&tsLogObj.logMutex);
    return;
  }
  TdFilePtr pFile = NULL;
  char      name[PATH_MAX + TD_TIME_STR_LEN] = {0};
  (void)snprintf(name, PATH_MAX + TD_TIME_STR_LEN, "%s.%s", tsLogObj.slowLogName, day);
  pFile = taosOpenFile(name, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_APPEND);
  if (pFile == NULL) {
    uError("open new log file fail! reason:%s, reuse lastlog", strerror(ERRNO));
    (void)taosThreadMutexUnlock(&tsLogObj.logMutex);
    return;
  }

  TdFilePtr pOldFile = tsLogObj.slowHandle->pFile;
  tsLogObj.slowHandle->pFile = pFile;
  (void)taosCloseFile(&pOldFile);
  tsLogObj.timestampToday = taosGetTimestampToday(TSDB_TIME_PRECISION_SECONDS, NULL);
  (void)taosThreadMutexUnlock(&tsLogObj.logMutex);
}

void taosResetLog() {
  // force create a new log file
  tsLogObj.lines = tsNumOfLogLines + 10;

  if (tsLogObj.logHandle) {
    int32_t code = taosOpenNewLogFile();
    if (code != 0) {
      uError("failed to open new log file, reason:%s", tstrerror(code));
    }
    uInfo("==================================");
    uInfo("   reset log file ");
  }
}

void taosLogObjSetToday(int64_t ts) { tsLogObj.timestampToday = ts; }

static bool taosCheckFileIsOpen(char *logFileName) {
  TdFilePtr pFile = taosOpenFile(logFileName, TD_FILE_WRITE);
  if (pFile == NULL) {
    if (lastErrorIsFileNotExist()) {
      return false;
    } else {
      printf("\n%s:%d failed to open log file:%s, reason:%s\n", __func__, __LINE__, logFileName, strerror(ERRNO));
      return true;
    }
  }

  if (taosLockLogFile(pFile)) {
    taosUnLockLogFile(pFile);
    (void)taosCloseFile(&pFile);
    return false;
  } else {
    (void)taosCloseFile(&pFile);
    return true;
  }
}

static void decideLogFileName(const char *fn, int32_t maxFileNum) {
  tsLogObj.fileNum = maxFileNum;
  if (tsLogObj.fileNum > 1) {
    for (int32_t i = 0; i < tsLogObj.fileNum; i++) {
      char fileName[PATH_MAX + 10];

      (void)snprintf(fileName, PATH_MAX + 10, "%s%d.0", fn, i);
      bool file1open = taosCheckFileIsOpen(fileName);

      (void)snprintf(fileName, PATH_MAX + 10, "%s%d.1", fn, i);
      bool file2open = taosCheckFileIsOpen(fileName);

      if (!file1open && !file2open) {
        (void)snprintf(tsLogObj.logName, PATH_MAX, "%s%d", fn, i);
        return;
      }
    }
  }

  if (strlen(fn) < PATH_MAX) {
    tstrncpy(tsLogObj.logName, fn, PATH_MAX);
  }
}

static void decideLogFileNameFlag() {
  char    name[PATH_MAX] = "\0";
  int64_t logstat0_mtime = 0;
  int64_t logstat1_mtime = 0;
  bool    log0Exist = false;
  bool    log1Exist = false;

  int32_t logNameLen = strlen(tsLogObj.logName) + 2;
  if (logNameLen < PATH_MAX) {
    TAOS_UNUSED(snprintf(name, PATH_MAX, "%s%s", tsLogObj.logName, ".0"));
    log0Exist = taosStatFile(name, NULL, &logstat0_mtime, NULL) == 0;
    name[logNameLen - 1] = '1';
    log1Exist = taosStatFile(name, NULL, &logstat1_mtime, NULL) == 0;
  }

  // if none of the log files exist, open 0, if both exists, open the old one
  if (!log0Exist && !log1Exist) {
    tsLogObj.flag = 0;
  } else if (!log1Exist) {
    tsLogObj.flag = 0;
  } else if (!log0Exist) {
    tsLogObj.flag = 1;
  } else {
    tsLogObj.flag = (logstat0_mtime > logstat1_mtime) ? 0 : 1;
  }
}

static void processLogFileName(const char *logName, int32_t maxFileNum) {
  char fullName[PATH_MAX] = {0};
  getFullPathName(fullName, logName);
  decideLogFileName(fullName, maxFileNum);
  decideLogFileNameFlag();
}

static int32_t taosInitNormalLog(const char *logName, int32_t maxFileNum) {
  int32_t code = 0, lino = 0;
#ifdef WINDOWS_STASH
  /*
   * always set maxFileNum to 1
   * means client log filename is unique in windows
   */
  maxFileNum = 1;
#endif

  processLogFileName(logName, maxFileNum);

  int32_t logNameLen = strlen(tsLogObj.logName) + 2;  // logName + ".0" or ".1"

  if (logNameLen < 0 || logNameLen >= PATH_MAX) {
    uError("log name:%s is invalid since length:%d is out of range", logName, logNameLen);
    return TSDB_CODE_INVALID_CFG;
  }

  char name[PATH_MAX] = "\0";
  (void)snprintf(name, sizeof(name), "%s.%d", tsLogObj.logName, tsLogObj.flag);
  (void)taosThreadMutexInit(&tsLogObj.logMutex, NULL);

  TAOS_UNUSED(taosUmaskFile(0));
  tsLogObj.logHandle = taosLogBuffNew(LOG_DEFAULT_BUF_SIZE);
  if (tsLogObj.logHandle == NULL) return terrno;

  tsLogObj.logHandle->pFile = taosOpenFile(name, TD_FILE_CREATE | TD_FILE_READ | TD_FILE_WRITE);
  if (tsLogObj.logHandle->pFile == NULL) {
    (void)printf("\n%s:%d failed to open log file:%s, reason:%s\n", __func__, __LINE__, name, strerror(ERRNO));
    return terrno;
  }
  TAOS_UNUSED(taosLockLogFile(tsLogObj.logHandle->pFile));

  // only an estimate for number of lines
  int64_t filesize = 0;
  TAOS_CHECK_EXIT(taosFStatFile(tsLogObj.logHandle->pFile, &filesize, NULL));

  tsLogObj.lines = (int32_t)(filesize / 60);

  if (taosLSeekFile(tsLogObj.logHandle->pFile, 0, SEEK_END) < 0) {
    TAOS_CHECK_EXIT(terrno);
  }

  (void)snprintf(name, sizeof(name),
                 "==================================================\n"
                 "                new log file\n"
                 "==================================================\n");
  if (taosWriteFile(tsLogObj.logHandle->pFile, name, (uint32_t)strlen(name)) <= 0) {
    TAOS_CHECK_EXIT(terrno);
  }

_exit:
  if (code != 0) {
    taosUnLockLogFile(tsLogObj.logHandle->pFile);
    TAOS_UNUSED(printf("failed to init normal log file:%s at line %d, reason:%s\n", name, lino, tstrerror(code)));
  }
  return code;
}

static void taosUpdateLogNums(ELogLevel level) {
  switch (level) {
    case DEBUG_ERROR:
      TAOS_UNUSED(atomic_add_fetch_64(&tsNumOfErrorLogs, 1));
      break;
    case DEBUG_INFO:
      TAOS_UNUSED(atomic_add_fetch_64(&tsNumOfInfoLogs, 1));
      break;
    case DEBUG_DEBUG:
      TAOS_UNUSED(atomic_add_fetch_64(&tsNumOfDebugLogs, 1));
      break;
    case DEBUG_DUMP:
    case DEBUG_TRACE:
      TAOS_UNUSED(atomic_add_fetch_64(&tsNumOfTraceLogs, 1));
      break;
    default:
      break;
  }
}

static inline int32_t taosBuildLogHead(char *buffer, const char *flags) {
  struct tm      Tm, *ptm;
  struct timeval timeSecs;

  TAOS_UNUSED(taosGetTimeOfDay(&timeSecs));
  time_t curTime = timeSecs.tv_sec;
  ptm = taosLocalTime(&curTime, &Tm, NULL, 0, NULL);
  if (ptm == NULL) {
    uError("%s failed to get local time, code:%d", __FUNCTION__, ERRNO);
    return 0;
  }
  return snprintf(buffer, LOG_MAX_STACK_LINE_BUFFER_SIZE, "%02d/%02d %02d:%02d:%02d.%06d %08" PRId64 " %s %s",
                  ptm->tm_mon + 1, ptm->tm_mday, ptm->tm_hour, ptm->tm_min, ptm->tm_sec, (int32_t)timeSecs.tv_usec,
                  taosGetSelfPthreadId(), LOG_EDITION_FLG, flags);
}

static inline void taosPrintLogImp(ELogLevel level, int32_t dflag, const char *buffer, int32_t len) {
  if ((dflag & DEBUG_FILE) && tsLogObj.logHandle && tsLogObj.logHandle->pFile != NULL && osLogSpaceSufficient()) {
    taosUpdateLogNums(level);
    if (tsAsyncLog) {
      TAOS_UNUSED(taosPushLogBuffer(tsLogObj.logHandle, buffer, len));
    } else {
      TAOS_UNUSED(taosWriteFile(tsLogObj.logHandle->pFile, buffer, len));
    }

    if (tsNumOfLogLines > 0) {
      TAOS_UNUSED(atomic_add_fetch_32(&tsLogObj.lines, 1));
      if ((tsLogObj.lines > tsNumOfLogLines) && (tsLogObj.openInProgress == 0)) {
        TAOS_UNUSED(taosOpenNewLogFile());
      }
    }
  }

  int fd = 0;
  if (tsLogObj.outputType == LOG_OUTPUT_FILE) {
#ifndef TAOSD_INTEGRATED    
    if (dflag & DEBUG_SCREEN) fd = 1;
#else
    if ((dflag & DEBUG_SCREEN) && tsLogEmbedded) fd = 1;
#endif
  } else if (tsLogObj.outputType == LOG_OUTPUT_STDOUT) {
    fd = 1;
  } else if (tsLogObj.outputType == LOG_OUTPUT_STDERR) {
    fd = 2;
  }

  if (fd) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-result"
#ifndef TD_ASTRA
    if (write(fd, buffer, (uint32_t)len) < 0) {
      TAOS_UNUSED(printf("failed to write log to screen, reason:%s\n", strerror(ERRNO)));
    }
#else
    TAOS_UNUSED(fprintf(fd == 1 ? stdout : stderr, "%s", buffer));
#endif
#pragma GCC diagnostic pop
  }
}

/*
  use taosPrintLogImpl_useStackBuffer to avoid stack overflow

*/
static int8_t taosPrintLogImplUseStackBuffer(const char *flags, int32_t level, int32_t dflag, const char *format,
                                             va_list args) {
  char    buffer[LOG_MAX_STACK_LINE_BUFFER_SIZE];
  int32_t len = taosBuildLogHead(buffer, flags);

  int32_t writeLen = len + vsnprintf(buffer + len, LOG_MAX_STACK_LINE_BUFFER_SIZE - len - 1, format, args);
  if (writeLen > LOG_MAX_STACK_LINE_SIZE) {
    return 1;
  }

  buffer[writeLen++] = '\n';
  buffer[writeLen] = 0;

  taosPrintLogImp(level, dflag, buffer, writeLen);

  if (tsLogFp && level <= DEBUG_INFO) {
    buffer[writeLen - 1] = 0;
    (*tsLogFp)(taosGetTimestampMs(), level, buffer + len);
  }
  return 0;
}
static int8_t taosPrintLogImplUseHeapBuffer(const char *flags, int32_t level, int32_t dflag, const char *format,
                                            va_list args) {
  char *buffer = taosMemoryCalloc(1, LOG_MAX_LINE_BUFFER_SIZE + 1);
  if (buffer == NULL) {
    return 1;
  }
  int32_t len = taosBuildLogHead(buffer, flags);

  int32_t writeLen = len + vsnprintf(buffer + len, LOG_MAX_LINE_BUFFER_SIZE - len - 1, format, args);

  if (writeLen > LOG_MAX_LINE_SIZE) writeLen = LOG_MAX_LINE_SIZE;
  buffer[writeLen++] = '\n';
  buffer[writeLen] = 0;

  taosPrintLogImp(level, dflag, buffer, writeLen);

  if (tsLogFp && level <= DEBUG_INFO) {
    buffer[writeLen - 1] = 0;
    (*tsLogFp)(taosGetTimestampMs(), level, buffer + len);
  }
  taosMemoryFree(buffer);
  return 0;
}
void taosPrintLog(const char *flags, int32_t level, int32_t dflag, const char *format, ...) {
  if (!(dflag & DEBUG_FILE) && !(dflag & DEBUG_SCREEN)) return;

  va_list argpointer, argpointer_copy;
  va_start(argpointer, format);
  va_copy(argpointer_copy, argpointer);

  if (taosPrintLogImplUseStackBuffer(flags, level, dflag, format, argpointer) == 0) {
  } else {
    TAOS_UNUSED(taosPrintLogImplUseHeapBuffer(flags, level, dflag, format, argpointer_copy));
  }
  va_end(argpointer_copy);
  va_end(argpointer);
}

void taosPrintLongString(const char *flags, int32_t level, int32_t dflag, const char *format, ...) {
  if (!osLogSpaceSufficient()) return;
  if (!(dflag & DEBUG_FILE) && !(dflag & DEBUG_SCREEN)) return;

  char *buffer = taosMemoryMalloc(LOG_MAX_LINE_DUMP_BUFFER_SIZE);
  if (!buffer) return;
  int32_t len = taosBuildLogHead(buffer, flags);

  va_list argpointer;
  va_start(argpointer, format);
  len += vsnprintf(buffer + len, LOG_MAX_LINE_DUMP_BUFFER_SIZE - 2 - len, format, argpointer);
  va_end(argpointer);

  len = len > LOG_MAX_LINE_DUMP_BUFFER_SIZE - 2 ? LOG_MAX_LINE_DUMP_BUFFER_SIZE - 2 : len;
  buffer[len++] = '\n';
  buffer[len] = 0;

  taosPrintLogImp(level, dflag, buffer, len);
  taosMemoryFree(buffer);
}

void taosPrintSlowLog(const char *format, ...) {
  if (!osLogSpaceSufficient()) return;

  int64_t delta = taosGetTimestampSec() - tsLogObj.timestampToday;
  if (delta >= 86400 || delta < 0) {
    taosOpenNewSlowLogFile();
  }

  char   *buffer = taosMemoryMalloc(LOG_MAX_LINE_DUMP_BUFFER_SIZE);
  int32_t len = taosBuildLogHead(buffer, "");

  va_list argpointer;
  va_start(argpointer, format);
  len += vsnprintf(buffer + len, LOG_MAX_LINE_DUMP_BUFFER_SIZE - 2 - len, format, argpointer);
  va_end(argpointer);

  if (len < 0 || len > LOG_MAX_LINE_DUMP_BUFFER_SIZE - 2) {
    len = LOG_MAX_LINE_DUMP_BUFFER_SIZE - 2;
  }
  buffer[len++] = '\n';
  buffer[len] = 0;

  TAOS_UNUSED(atomic_add_fetch_64(&tsNumOfSlowLogs, 1));

  if (tsAsyncLog) {
    TAOS_UNUSED(taosPushLogBuffer(tsLogObj.slowHandle, buffer, len));
  } else {
    TAOS_UNUSED(taosWriteFile(tsLogObj.slowHandle->pFile, buffer, len));
  }

  taosMemoryFree(buffer);
}

static void taosCloseLogByFd(TdFilePtr pFile) {
  if (pFile != NULL) {
    taosUnLockLogFile(pFile);
    (void)taosCloseFile(&pFile);
  }
}

static SLogBuff *taosLogBuffNew(int32_t bufSize) {
  SLogBuff *pLogBuf = NULL;

  pLogBuf = taosMemoryCalloc(1, sizeof(SLogBuff));
  if (pLogBuf == NULL) return NULL;

  LOG_BUF_BUFFER(pLogBuf) = taosMemoryMalloc(bufSize);
  if (LOG_BUF_BUFFER(pLogBuf) == NULL) goto _err;

  LOG_BUF_START(pLogBuf) = LOG_BUF_END(pLogBuf) = 0;
  LOG_BUF_SIZE(pLogBuf) = bufSize;
  pLogBuf->minBuffSize = bufSize / 10;
  pLogBuf->stop = 0;
  pLogBuf->writeInterval = LOG_DEFAULT_INTERVAL;
  pLogBuf->lock = 0;
  if (taosThreadMutexInit(&LOG_BUF_MUTEX(pLogBuf), NULL) < 0) goto _err;
  // tsem_init(&(pLogBuf->buffNotEmpty), 0, 0);

  return pLogBuf;

_err:
  taosMemoryFreeClear(LOG_BUF_BUFFER(pLogBuf));
  taosMemoryFreeClear(pLogBuf);
  return NULL;
}

static void taosCopyLogBuffer(SLogBuff *pLogBuf, int32_t start, int32_t end, const char *msg, int32_t msgLen) {
  if (start > end) {
    memcpy(LOG_BUF_BUFFER(pLogBuf) + end, msg, msgLen);
  } else {
    if (LOG_BUF_SIZE(pLogBuf) - end < msgLen) {
      memcpy(LOG_BUF_BUFFER(pLogBuf) + end, msg, LOG_BUF_SIZE(pLogBuf) - end);
      memcpy(LOG_BUF_BUFFER(pLogBuf), msg + LOG_BUF_SIZE(pLogBuf) - end, msgLen - LOG_BUF_SIZE(pLogBuf) + end);
    } else {
      memcpy(LOG_BUF_BUFFER(pLogBuf) + end, msg, msgLen);
    }
  }
  LOG_BUF_END(pLogBuf) = (LOG_BUF_END(pLogBuf) + msgLen) % LOG_BUF_SIZE(pLogBuf);
}

static int32_t taosPushLogBuffer(SLogBuff *pLogBuf, const char *msg, int32_t msgLen) {
  int32_t        start = 0;
  int32_t        end = 0;
  int32_t        remainSize = 0;
  static int64_t lostLine = 0;
  char           tmpBuf[128];
  int32_t        tmpBufLen = 0;

  if (pLogBuf == NULL || pLogBuf->stop) return -1;

  (void)taosThreadMutexLock(&LOG_BUF_MUTEX(pLogBuf));
  start = LOG_BUF_START(pLogBuf);
  end = LOG_BUF_END(pLogBuf);

  remainSize = (start > end) ? (start - end - 1) : (start + LOG_BUF_SIZE(pLogBuf) - end - 1);

  if (lostLine > 0) {
    tmpBufLen = snprintf(tmpBuf, tListLen(tmpBuf), "...Lost %" PRId64 " lines here...\n", lostLine);
  }

  if (remainSize <= msgLen || ((lostLine > 0) && (remainSize <= (msgLen + tmpBufLen)))) {
    lostLine++;
    tsAsyncLogLostLines++;
    (void)taosThreadMutexUnlock(&LOG_BUF_MUTEX(pLogBuf));
    return -1;
  }

  if (lostLine > 0) {
    taosCopyLogBuffer(pLogBuf, start, end, tmpBuf, tmpBufLen);
    lostLine = 0;
  }

  taosCopyLogBuffer(pLogBuf, LOG_BUF_START(pLogBuf), LOG_BUF_END(pLogBuf), msg, msgLen);

  // int32_t w = atomic_sub_fetch_32(&waitLock, 1);
  /*
  if (w <= 0 || ((remainSize - msgLen - tmpBufLen) < (LOG_BUF_SIZE(pLogBuf) * 4 /5))) {
    tsem_post(&(pLogBuf->buffNotEmpty));
    dbgPostN++;
  } else {
    dbgNoPostN++;
  }
  */

  (void)taosThreadMutexUnlock(&LOG_BUF_MUTEX(pLogBuf));

  return 0;
}

static int32_t taosGetLogRemainSize(SLogBuff *pLogBuf, int32_t start, int32_t end) {
  int32_t rSize = end - start;

  return rSize >= 0 ? rSize : LOG_BUF_SIZE(pLogBuf) + rSize;
}

static void taosWriteSlowLog(SLogBuff *pLogBuf) {
  int32_t lock = atomic_val_compare_exchange_32(&pLogBuf->lock, 0, 1);
  if (lock == 1) return;
  taosWriteLog(pLogBuf);
  atomic_store_32(&pLogBuf->lock, 0);
}
static void taosWriteLog(SLogBuff *pLogBuf) {
  (void)taosThreadMutexLock(&LOG_BUF_MUTEX(pLogBuf));
  int32_t start = LOG_BUF_START(pLogBuf);
  int32_t end = LOG_BUF_END(pLogBuf);
  (void)taosThreadMutexUnlock(&LOG_BUF_MUTEX(pLogBuf));

  if (start == end) {
    dbgEmptyW++;
    pLogBuf->writeInterval = LOG_MAX_INTERVAL;
    return;
  }

  int32_t pollSize = taosGetLogRemainSize(pLogBuf, start, end);
  if (pollSize < pLogBuf->minBuffSize) {
    pLogBuf->lastDuration += pLogBuf->writeInterval;
    if (pLogBuf->lastDuration < LOG_MAX_WAIT_MSEC) {
      return;
    }
  }

  pLogBuf->lastDuration = 0;

  if (start < end) {
    TAOS_UNUSED(taosWriteFile(pLogBuf->pFile, LOG_BUF_BUFFER(pLogBuf) + start, pollSize));
  } else {
    int32_t tsize = LOG_BUF_SIZE(pLogBuf) - start;
    TAOS_UNUSED(taosWriteFile(pLogBuf->pFile, LOG_BUF_BUFFER(pLogBuf) + start, tsize));

    TAOS_UNUSED(taosWriteFile(pLogBuf->pFile, LOG_BUF_BUFFER(pLogBuf), end));
  }

  dbgWN++;
  dbgWSize += pollSize;

  if (pollSize < pLogBuf->minBuffSize) {
    dbgSmallWN++;
    if (pLogBuf->writeInterval < LOG_MAX_INTERVAL) {
      pLogBuf->writeInterval += LOG_INTERVAL_STEP;
    }
  } else if (pollSize > LOG_BUF_SIZE(pLogBuf) / 3) {
    dbgBigWN++;
    pLogBuf->writeInterval = LOG_MIN_INTERVAL;
  } else if (pollSize > LOG_BUF_SIZE(pLogBuf) / 4) {
    if (pLogBuf->writeInterval > LOG_MIN_INTERVAL) {
      pLogBuf->writeInterval -= LOG_INTERVAL_STEP;
    }
  }

  (void)taosThreadMutexLock(&LOG_BUF_MUTEX(pLogBuf));
  LOG_BUF_START(pLogBuf) = (LOG_BUF_START(pLogBuf) + pollSize) % LOG_BUF_SIZE(pLogBuf);

  start = LOG_BUF_START(pLogBuf);
  end = LOG_BUF_END(pLogBuf);
  (void)taosThreadMutexUnlock(&LOG_BUF_MUTEX(pLogBuf));

  pollSize = taosGetLogRemainSize(pLogBuf, start, end);
  if (pollSize < pLogBuf->minBuffSize) {
    return;
  }

  pLogBuf->writeInterval = 0;
}

#define LOG_ROTATE_INTERVAL 3600
#if !defined(TD_ENTERPRISE) || defined(ASSERT_NOT_CORE) || defined(GRANTS_CFG)
#define LOG_INACTIVE_TIME 7200
#define LOG_ROTATE_BOOT   900
#else
#define LOG_INACTIVE_TIME 5
#define LOG_ROTATE_BOOT   (LOG_INACTIVE_TIME + 1)
#endif
static int8_t tsLogRotateRunning = 0;
static void  *taosLogRotateFunc(void *param) {
  setThreadName("logRotate");
  int32_t code = 0;
  if (0 != atomic_val_compare_exchange_8(&tsLogRotateRunning, 0, 1)) {
    uInfo("log rotation is already in progress");
    return NULL;
  }
  // get prefix of logfile name
  char *filePrefix = NULL;
  char *filePos = strrchr(tsLogObj.logName, TD_DIRSEP_CHAR);
  if (!filePos || !(++filePos)) {
    atomic_store_8(&tsLogRotateRunning, 0);
    return NULL;
  }
  int32_t filePrefixLen = strlen(filePos);
  if (!(filePrefix = taosMemoryMalloc(filePrefixLen + 1))) {
    atomic_store_8(&tsLogRotateRunning, 0);
    return NULL;
  }
  tstrncpy(filePrefix, filePos, filePrefixLen + 1);
  int32_t i = filePrefixLen - 1;
  while (i > 0 && isdigit(filePrefix[i])) filePrefix[i--] = '\0';

  taosWLockLatch(&tsLogRotateLatch);
  // compress or remove the old log files
  TdDirPtr pDir = taosOpenDir(tsLogDir);
  if (!pDir) goto _exit;
  TdDirEntryPtr de = NULL;
  while ((de = taosReadDir(pDir))) {
    if (taosDirEntryIsDir(de)) {
      continue;
    }
    char *fname = taosGetDirEntryName(de);
    if (!fname) {
      continue;
    }

    if (!strstr(fname, filePrefix)) continue;

    char *pSec = strrchr(fname, '.');
    if (!pSec) {
      continue;
    }
    char *pIter = pSec;
    bool  isSec = true;
    while (*(++pIter)) {
      if (!isdigit(*pIter)) {
        isSec = false;
        break;
      }
    }
    if (!isSec) {
      continue;
    }

    int64_t fileSec = 0;
    if ((code = taosStr2int64(pSec + 1, &fileSec)) != 0) {
      uWarn("%s:%d failed to convert %s to int64 since %s", __func__, __LINE__, pSec + 1, tstrerror(code));
      continue;
    }
    if (fileSec <= 100) {
      continue;
    }

    char fullName[PATH_MAX] = {0};
    snprintf(fullName, sizeof(fullName), "%s%s%s", tsLogDir, TD_DIRSEP, fname);

    int64_t mtime = 0;
    if ((code = taosStatFile(fullName, NULL, &mtime, NULL)) != 0) {
      uWarn("%s:%d failed to stat file %s since %s", __func__, __LINE__, fullName, tstrerror(code));
      continue;
    }

    int64_t inactiveSec = taosGetTimestampMs() / 1000 - mtime;

    if (inactiveSec < LOG_INACTIVE_TIME) {
      continue;
    }

    int32_t days = inactiveSec / 86400 + 1;
    if (tsLogKeepDays != 0 && days > abs(tsLogKeepDays)) {
      TAOS_UNUSED(taosRemoveFile(fullName));
      uInfo("file:%s is removed, days:%d, keepDays:%d, sed:%" PRId64, fullName, days, tsLogKeepDays, fileSec);
    } else {
      taosKeepOldLog(fullName);  // compress
    }
  }
  if ((code = taosCloseDir(&pDir)) != 0) {
    uWarn("%s:%d failed to close dir %s since %s\n", __func__, __LINE__, tsLogDir, tstrerror(code));
  }

  if (tsLogKeepDays != 0) {
    taosRemoveOldFiles(tsLogDir, abs(tsLogKeepDays));
  }
_exit:
  taosWUnLockLatch(&tsLogRotateLatch);
  atomic_store_8(&tsLogRotateRunning, 0);
  taosMemFreeClear(filePrefix);
  return NULL;
}

static void *taosAsyncOutputLog(void *param) {
  SLogBuff *pLogBuf = (SLogBuff *)tsLogObj.logHandle;
  SLogBuff *pSlowBuf = (SLogBuff *)tsLogObj.slowHandle;

  setThreadName("log");
  int32_t count = 0;
  int32_t updateCron = 0;
  int32_t writeInterval = 0;
  int64_t lastCheckSec = taosGetTimestampMs() / 1000 - (LOG_ROTATE_INTERVAL - LOG_ROTATE_BOOT);

  while (1) {
    if (pSlowBuf) {
      writeInterval = TMIN(pLogBuf->writeInterval, pSlowBuf->writeInterval);
    } else {
      writeInterval = pLogBuf->writeInterval;
    }
    count += writeInterval;
    updateCron++;
    taosMsleep(writeInterval);
    if (count > 1000) {
      TAOS_UNUSED(osUpdate());
      count = 0;
    }

    // Polling the buffer
    taosWriteLog(pLogBuf);
    if (pSlowBuf) taosWriteSlowLog(pSlowBuf);

    if (pLogBuf->stop || (pSlowBuf && pSlowBuf->stop)) {
      pLogBuf->lastDuration = LOG_MAX_WAIT_MSEC;
      taosWriteLog(pLogBuf);
      if (pSlowBuf) taosWriteSlowLog(pSlowBuf);
      break;
    }

    // process the log rotation every LOG_ROTATE_INTERVAL
    int64_t curSec = taosGetTimestampMs() / 1000;
    if (curSec >= lastCheckSec) {
      if ((curSec - lastCheckSec) >= (LOG_ROTATE_INTERVAL + (taosRand() % LOG_ROTATE_BOOT))) {
        TdThread     thread;
        TdThreadAttr attr;
        (void)taosThreadAttrInit(&attr);
        (void)taosThreadAttrSetDetachState(&attr, PTHREAD_CREATE_DETACHED);
#ifdef TD_COMPACT_OS
        (void)taosThreadAttrSetStackSize(&attr, STACK_SIZE_SMALL);
#endif
        if (taosThreadCreate(&thread, &attr, taosLogRotateFunc, tsLogObj.logHandle) == 0) {
          uInfo("process log rotation");
          lastCheckSec = curSec;
        } else {
          uWarn("failed to create thread to process log rotation");
        }
        (void)taosThreadAttrDestroy(&attr);
      }
    } else if (curSec < lastCheckSec) {
      lastCheckSec = curSec;
    }
  }

  return NULL;
}

bool taosAssertDebug(bool condition, const char *file, int32_t line, bool core, const char *format, ...) {
  if (condition) return false;

  const char *flags = "UTL FATAL ";
  ELogLevel   level = DEBUG_FATAL;
  int32_t     dflag = 255;  // tsLogEmbedded ? 255 : uDebugFlag
  char        buffer[LOG_MAX_LINE_BUFFER_SIZE];
  int32_t     len = taosBuildLogHead(buffer, flags);

  va_list argpointer;
  va_start(argpointer, format);
  len = len + vsnprintf(buffer + len, LOG_MAX_LINE_BUFFER_SIZE - len, format, argpointer);
  va_end(argpointer);
  buffer[len++] = '\n';
  buffer[len] = 0;
  taosPrintLogImp(1, 255, buffer, len);

  taosPrintLog(flags, level, dflag, "tAssert at file %s:%d exit:%d", file, line, tsAssert);
#ifndef TD_ASTRA
  taosPrintTrace(flags, level, dflag, -1);
#endif
  if (tsAssert || core) {
    taosCloseLog();
    taosMsleep(300);

#ifdef NDEBUG
    abort();
#else
    assert(0);
#endif
  }

  return true;
}
#ifdef USE_REPORT
void taosLogCrashInfo(char *nodeType, char *pMsg, int64_t msgLen, int signum, void *sigInfo) {
  const char *flags = "UTL FATAL ";
  ELogLevel   level = DEBUG_FATAL;
  int32_t     dflag = 255;
  char        filepath[PATH_MAX] = {0};
  TdFilePtr   pFile = NULL;

  if (pMsg && msgLen > 0) {
    snprintf(filepath, sizeof(filepath), "%s%s.%sCrashLog", tsLogDir, TD_DIRSEP, nodeType);

    pFile = taosOpenFile(filepath, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_APPEND);
    if (pFile == NULL) {
      taosPrintLog(flags, level, dflag, "failed to open file:%s since %s", filepath, terrstr());
      goto _return;
    }

    if (taosLockFile(pFile) < 0) {
      taosPrintLog(flags, level, dflag, "failed to lock file:%s since %s", filepath, terrstr());
      goto _return;
    }

    int64_t writeSize = taosWriteFile(pFile, &msgLen, sizeof(msgLen));
    if (sizeof(msgLen) != writeSize) {
      TAOS_UNUSED(taosUnLockFile(pFile));
      taosPrintLog(flags, level, dflag, "failed to write len to file:%s,%p wlen:%" PRId64 " tlen:%lu since %s",
                   filepath, pFile, writeSize, sizeof(msgLen), terrstr());
      goto _return;
    }

    writeSize = taosWriteFile(pFile, pMsg, msgLen);
    if (msgLen != writeSize) {
      TAOS_UNUSED(taosUnLockFile(pFile));
      taosPrintLog(flags, level, dflag, "failed to write file:%s,%p wlen:%" PRId64 " tlen:%" PRId64 " since %s",
                   filepath, pFile, writeSize, msgLen, terrstr());
      goto _return;
    }

    TAOS_UNUSED(taosUnLockFile(pFile));
  }

_return:

  if (pFile) (void)taosCloseFile(&pFile);

  taosPrintLog(flags, level, dflag, "crash signal is %d", signum);

// print the stack trace
#if 0
#ifdef _TD_DARWIN_64
  taosPrintTrace(flags, level, dflag, 4);
#elif !defined(WINDOWS)
  taosPrintLog(flags, level, dflag, "sender PID:%d cmdline:%s", ((siginfo_t *)sigInfo)->si_pid,
               taosGetCmdlineByPID(((siginfo_t *)sigInfo)->si_pid));
  taosPrintTrace(flags, level, dflag, 3);
#else
  taosPrintTrace(flags, level, dflag, 8);
#endif
#endif
  taosMemoryFree(pMsg);
}

typedef enum {
  CRASH_LOG_WRITER_UNKNOWN = 0,
  CRASH_LOG_WRITER_INIT = 1,
  CRASH_LOG_WRITER_WAIT,
  CRASH_LOG_WRITER_RUNNING,
  CRASH_LOG_WRITER_QUIT
} CrashStatus;
typedef struct crashBasicInfo {
  int8_t  status;
  int64_t clusterId;
  int64_t startTime;
  char   *nodeType;
  int     signum;
  void   *sigInfo;
  tsem_t  sem;
  int64_t reportThread;
} crashBasicInfo;

crashBasicInfo gCrashBasicInfo = {0};

void setCrashWriterStatus(int8_t status) { atomic_store_8(&gCrashBasicInfo.status, status); }
bool reportThreadSetQuit() {
  CrashStatus status =
      atomic_val_compare_exchange_8(&gCrashBasicInfo.status, CRASH_LOG_WRITER_INIT, CRASH_LOG_WRITER_QUIT);
  if (status == CRASH_LOG_WRITER_INIT) {
    return true;
  } else {
    return false;
  }
}

bool setReportThreadWait() {
  CrashStatus status =
      atomic_val_compare_exchange_8(&gCrashBasicInfo.status, CRASH_LOG_WRITER_INIT, CRASH_LOG_WRITER_WAIT);
  if (status == CRASH_LOG_WRITER_INIT) {
    return true;
  } else {
    return false;
  }
}
bool setReportThreadRunning() {
  CrashStatus status =
      atomic_val_compare_exchange_8(&gCrashBasicInfo.status, CRASH_LOG_WRITER_WAIT, CRASH_LOG_WRITER_RUNNING);
  if (status == CRASH_LOG_WRITER_WAIT) {
    return true;
  } else {
    return false;
  }
}
static void checkWriteCrashLogToFileInNewThead() {
  if (setReportThreadRunning()) {
    char       *pMsg = NULL;
    const char *flags = "UTL FATAL ";
    ELogLevel   level = DEBUG_FATAL;
    int32_t     dflag = 255;
    int64_t     msgLen = -1;

    if (tsEnableCrashReport) {
      if (taosGenCrashJsonMsg(gCrashBasicInfo.signum, &pMsg, gCrashBasicInfo.clusterId, gCrashBasicInfo.startTime)) {
        taosPrintLog(flags, level, dflag, "failed to generate crash json msg");
      } else {
        msgLen = strlen(pMsg);
      }
    }
    taosLogCrashInfo(gCrashBasicInfo.nodeType, pMsg, msgLen, gCrashBasicInfo.signum, gCrashBasicInfo.sigInfo);
    setCrashWriterStatus(CRASH_LOG_WRITER_INIT);
    int32_t code = tsem_post(&gCrashBasicInfo.sem);
    if (code != 0 ) {
      uError("failed to post sem for crashBasicInfo, code:%d", code);
    }
    TAOS_UNUSED(tsem_post(&gCrashBasicInfo.sem));
  }
}

void checkAndPrepareCrashInfo() {
  return checkWriteCrashLogToFileInNewThead();
}

int32_t initCrashLogWriter() {
  int32_t code = tsem_init(&gCrashBasicInfo.sem, 0, 0);
  if (code != 0) {
    uError("failed to init sem for crashLogWriter, code:%d", code);
    return code;
  }
  gCrashBasicInfo.reportThread = taosGetSelfPthreadId();
  setCrashWriterStatus(CRASH_LOG_WRITER_INIT);
  return code;
}

void writeCrashLogToFile(int signum, void *sigInfo, char *nodeType, int64_t clusterId, int64_t startTime) {
  if (gCrashBasicInfo.reportThread == taosGetSelfPthreadId()) {
    return;
  }
  if (setReportThreadWait()) {
    gCrashBasicInfo.clusterId = clusterId;
    gCrashBasicInfo.startTime = startTime;
    gCrashBasicInfo.nodeType = nodeType;
    gCrashBasicInfo.signum = signum;
    gCrashBasicInfo.sigInfo = sigInfo;
    TAOS_UNUSED(tsem_wait(&gCrashBasicInfo.sem));
  }
}

void taosReadCrashInfo(char *filepath, char **pMsg, int64_t *pMsgLen, TdFilePtr *pFd) {
  const char *flags = "UTL FATAL ";
  ELogLevel   level = DEBUG_FATAL;
  int32_t     dflag = 255;
  TdFilePtr   pFile = NULL;
  bool        truncateFile = false;
  char       *buf = NULL;

  if (NULL == *pFd) {
    int64_t filesize = 0;
    if (taosStatFile(filepath, &filesize, NULL, NULL) < 0) {
      if (TAOS_SYSTEM_ERROR(ENOENT) == terrno) {
        return;
      }

      taosPrintLog(flags, level, dflag, "failed to stat file:%s since %s", filepath, terrstr());
      return;
    }

    if (filesize <= 0) {
      return;
    }

    pFile = taosOpenFile(filepath, TD_FILE_READ | TD_FILE_WRITE);
    if (pFile == NULL) {
      if (ENOENT == ERRNO) {
        return;
      }

      taosPrintLog(flags, level, dflag, "failed to open file:%s since %s", filepath, terrstr());
      return;
    }

    TAOS_UNUSED(taosLockFile(pFile));
  } else {
    pFile = *pFd;
  }

  int64_t msgLen = 0;
  int64_t readSize = taosReadFile(pFile, &msgLen, sizeof(msgLen));
  if (sizeof(msgLen) != readSize) {
    truncateFile = true;
    if (readSize < 0) {
      taosPrintLog(flags, level, dflag, "failed to read len from file:%s,%p wlen:%" PRId64 " tlen:%lu since %s",
                   filepath, pFile, readSize, sizeof(msgLen), terrstr());
    }
    goto _return;
  }

  buf = taosMemoryMalloc(msgLen);
  if (NULL == buf) {
    taosPrintLog(flags, level, dflag, "failed to malloc buf, size:%" PRId64, msgLen);
    goto _return;
  }

  readSize = taosReadFile(pFile, buf, msgLen);
  if (msgLen != readSize) {
    truncateFile = true;
    taosPrintLog(flags, level, dflag, "failed to read file:%s,%p wlen:%" PRId64 " tlen:%" PRId64 " since %s", filepath,
                 pFile, readSize, msgLen, terrstr());
    goto _return;
  }

  *pMsg = buf;
  *pMsgLen = msgLen;
  *pFd = pFile;

  return;

_return:

  if (truncateFile) {
    TAOS_UNUSED(taosFtruncateFile(pFile, 0));
  }
  TAOS_UNUSED(taosUnLockFile(pFile));
  TAOS_UNUSED(taosCloseFile(&pFile));
  taosMemoryFree(buf);

  *pMsg = NULL;
  *pMsgLen = 0;
  *pFd = NULL;
}

void taosReleaseCrashLogFile(TdFilePtr pFile, bool truncateFile) {
  if (truncateFile) {
    TAOS_UNUSED(taosFtruncateFile(pFile, 0));
  }

  TAOS_UNUSED(taosUnLockFile(pFile));
  TAOS_UNUSED(taosCloseFile(&pFile));
}
#endif // USE_REPORT

#ifdef NDEBUG
bool taosAssertRelease(bool condition) {
  if (condition) return false;

  const char *flags = "UTL FATAL ";
  ELogLevel   level = DEBUG_FATAL;
  int32_t     dflag = 255;  // tsLogEmbedded ? 255 : uDebugFlag

  taosPrintLog(flags, level, dflag, "tAssert called in release mode, exit:%d", tsAssert);
  taosPrintTrace(flags, level, dflag, 0);

  if (tsAssert) {
    taosMsleep(300);
    abort();
  }

  return true;
}
#endif

#define NUM_BASE 100
#define DIGIT_LENGTH 2
#define MAX_DIGITS 24

char* u64toaFastLut(uint64_t val, char* buf) {
  // Look-up table for 2-digit numbers
  static const char* lut =
      "0001020304050607080910111213141516171819202122232425262728293031323334353637383940414243444546474849505152535455"
      "5657585960616263646566676869707172737475767778798081828384858687888990919293949596979899";

  char  temp[MAX_DIGITS];
  char* p = temp + tListLen(temp);

  // Process the digits greater than or equal to 100
  while (val >= NUM_BASE) {
    // Get the last 2 digits from the look-up table and add to the buffer
    p -= DIGIT_LENGTH;
    strncpy(p, lut + (val % NUM_BASE) * DIGIT_LENGTH, DIGIT_LENGTH);
    val /= NUM_BASE;
  }

  // Process the remaining 1 or 2 digits
  if (val >= 10) {
    // If the number is 10 or more, get the 2 digits from the look-up table
    p -= DIGIT_LENGTH;
    strncpy(p, lut + val * DIGIT_LENGTH, DIGIT_LENGTH);
  } else if (val > 0 || p == temp) {
    // If the number is less than 10, add the single digit to the buffer
    p -= 1;
    *p = val + '0';
  }

  int64_t len = temp + tListLen(temp) - p;
  if (len > 0) {
    memcpy(buf, p, len);
  } else {
    buf[0] = '0';
    len = 1;
  }
  buf[len] = '\0';

  return buf + len;
}
