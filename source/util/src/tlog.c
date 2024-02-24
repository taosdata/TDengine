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

#define LOG_MAX_LINE_SIZE             (10024)
#define LOG_MAX_LINE_BUFFER_SIZE      (LOG_MAX_LINE_SIZE + 3)
#define LOG_MAX_LINE_DUMP_SIZE        (1024 * 1024)
#define LOG_MAX_LINE_DUMP_BUFFER_SIZE (LOG_MAX_LINE_DUMP_SIZE + 128)

#define LOG_FILE_NAME_LEN    300
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
} SLogBuff;

typedef struct {
  int32_t       fileNum;
  int32_t       lines;
  int32_t       flag;
  int32_t       openInProgress;
  pid_t         pid;
  char          logName[LOG_FILE_NAME_LEN];
  SLogBuff     *logHandle;
  SLogBuff     *slowHandle;
  TdThreadMutex logMutex;
} SLogObj;

extern SConfig *tsCfg;
static int8_t   tsLogInited = 0;
static SLogObj  tsLogObj = {.fileNum = 1};
static int64_t  tsAsyncLogLostLines = 0;
static int32_t  tsDaylightActive; /* Currently in daylight saving time. */

bool tsLogEmbedded = 0;
bool tsAsyncLog = true;
#ifdef ASSERT_NOT_CORE
bool tsAssert = false;
#else
bool tsAssert = true;
#endif
int32_t tsNumOfLogLines = 10000000;
int32_t tsLogKeepDays = 0;
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
int32_t simDebugFlag = 131;

int64_t dbgEmptyW = 0;
int64_t dbgWN = 0;
int64_t dbgSmallWN = 0;
int64_t dbgBigWN = 0;
int64_t dbgWSize = 0;

static void     *taosAsyncOutputLog(void *param);
static int32_t   taosPushLogBuffer(SLogBuff *pLogBuf, const char *msg, int32_t msgLen);
static SLogBuff *taosLogBuffNew(int32_t bufSize);
static void      taosCloseLogByFd(TdFilePtr pFile);
static int32_t   taosOpenLogFile(char *fn, int32_t maxFileNum);

static FORCE_INLINE void taosUpdateDaylight() {
  struct tm      Tm, *ptm;
  struct timeval timeSecs;
  taosGetTimeOfDay(&timeSecs);
  time_t curTime = timeSecs.tv_sec;
  ptm = taosLocalTime(&curTime, &Tm, NULL);
  tsDaylightActive = ptm->tm_isdst;
}
static FORCE_INLINE int32_t taosGetDaylight() { return tsDaylightActive; }

static int32_t taosStartLog() {
  TdThreadAttr threadAttr;
  taosThreadAttrInit(&threadAttr);
  if (taosThreadCreate(&(tsLogObj.logHandle->asyncThread), &threadAttr, taosAsyncOutputLog, tsLogObj.logHandle) != 0) {
    return -1;
  }
  taosThreadAttrDestroy(&threadAttr);
  return 0;
}

int32_t taosInitSlowLog() {
  char fullName[PATH_MAX] = {0};
  char logFileName[64] = {0};
#ifdef CUS_PROMPT
  snprintf(logFileName, 64, "%sSlowLog", CUS_PROMPT);
#else
  snprintf(logFileName, 64, "taosSlowLog");
#endif

  if (strlen(tsLogDir) != 0) {
    char lastC = tsLogDir[strlen(tsLogDir) - 1];
    if (lastC == '\\' || lastC == '/') {
      snprintf(fullName, PATH_MAX, "%s" "%s", tsLogDir, logFileName);
    } else {
      snprintf(fullName, PATH_MAX, "%s" TD_DIRSEP "%s", tsLogDir, logFileName);
    }
  } else {
    snprintf(fullName, PATH_MAX, "%s", logFileName);
  }

  tsLogObj.slowHandle = taosLogBuffNew(LOG_SLOW_BUF_SIZE);
  if (tsLogObj.slowHandle == NULL) return -1;

  taosUmaskFile(0);
  tsLogObj.slowHandle->pFile = taosOpenFile(fullName, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_APPEND);
  if (tsLogObj.slowHandle->pFile == NULL) {
    printf("\nfailed to open slow log file:%s, reason:%s\n", fullName, strerror(errno));
    return -1;
  }

  return 0;
}

int32_t taosInitLog(const char *logName, int32_t maxFiles) {
  if (atomic_val_compare_exchange_8(&tsLogInited, 0, 1) != 0) return 0;
  osUpdate();

  char fullName[PATH_MAX] = {0};
  if (strlen(tsLogDir) != 0) {
    char lastC = tsLogDir[strlen(tsLogDir) - 1];
    if (lastC == '\\' || lastC == '/') {
      snprintf(fullName, PATH_MAX, "%s" "%s", tsLogDir, logName);
    } else {
      snprintf(fullName, PATH_MAX, "%s" TD_DIRSEP "%s", tsLogDir, logName);
    }
  } else {
    snprintf(fullName, PATH_MAX, "%s", logName);
  }
  taosUpdateDaylight();

  tsLogObj.logHandle = taosLogBuffNew(LOG_DEFAULT_BUF_SIZE);
  if (tsLogObj.logHandle == NULL) return -1;
  if (taosOpenLogFile(fullName, maxFiles) < 0) return -1;

  if (taosInitSlowLog() < 0) return -1;
  if (taosStartLog() < 0) return -1;
  return 0;
}

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
    taosThreadJoin(tsLogObj.logHandle->asyncThread, NULL);
    taosThreadClear(&tsLogObj.logHandle->asyncThread);
  }

  if (tsLogObj.slowHandle != NULL) {
    taosThreadMutexDestroy(&tsLogObj.slowHandle->buffMutex);
    taosCloseFile(&tsLogObj.slowHandle->pFile);
    taosMemoryFreeClear(tsLogObj.slowHandle->buffer);
    taosMemoryFreeClear(tsLogObj.slowHandle);
  }

  if (tsLogObj.logHandle != NULL) {
    tsLogInited = 0;

    taosThreadMutexDestroy(&tsLogObj.logHandle->buffMutex);
    taosCloseFile(&tsLogObj.logHandle->pFile);
    taosMemoryFreeClear(tsLogObj.logHandle->buffer);
    taosThreadMutexDestroy(&tsLogObj.logMutex);
    taosMemoryFreeClear(tsLogObj.logHandle);
  }
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
    taosUnLockFile(pFile);
  }
}

static void taosKeepOldLog(char *oldName) {
  if (tsLogKeepDays == 0) return;

  int64_t fileSec = taosGetTimestampSec();
  char    fileName[LOG_FILE_NAME_LEN + 20];
  snprintf(fileName, LOG_FILE_NAME_LEN + 20, "%s.%" PRId64, tsLogObj.logName, fileSec);

  (void)taosRenameFile(oldName, fileName);

  char compressFileName[LOG_FILE_NAME_LEN + 20];
  snprintf(compressFileName, LOG_FILE_NAME_LEN + 20, "%s.%" PRId64 ".gz", tsLogObj.logName, fileSec);
  if (taosCompressFile(fileName, compressFileName) == 0) {
    (void)taosRemoveFile(fileName);
  }

  if (tsLogKeepDays > 0) {
    taosRemoveOldFiles(tsLogDir, tsLogKeepDays);
  }
}

static void *taosThreadToOpenNewFile(void *param) {
  char keepName[LOG_FILE_NAME_LEN + 20];
  sprintf(keepName, "%s.%d", tsLogObj.logName, tsLogObj.flag);

  tsLogObj.flag ^= 1;
  tsLogObj.lines = 0;
  char name[LOG_FILE_NAME_LEN + 20];
  sprintf(name, "%s.%d", tsLogObj.logName, tsLogObj.flag);

  taosUmaskFile(0);

  TdFilePtr pFile = taosOpenFile(name, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pFile == NULL) {
    tsLogObj.openInProgress = 0;
    tsLogObj.lines = tsNumOfLogLines - 1000;
    uError("open new log file fail! reason:%s, reuse lastlog", strerror(errno));
    return NULL;
  }

  taosLockLogFile(pFile);
  (void)taosLSeekFile(pFile, 0, SEEK_SET);

  TdFilePtr pOldFile = tsLogObj.logHandle->pFile;
  tsLogObj.logHandle->pFile = pFile;
  tsLogObj.lines = 0;
  tsLogObj.openInProgress = 0;
  taosSsleep(20);
  taosCloseLogByFd(pOldFile);

  uInfo("   new log file:%d is opened", tsLogObj.flag);
  uInfo("==================================");
  taosKeepOldLog(keepName);

  return NULL;
}

static int32_t taosOpenNewLogFile() {
  taosThreadMutexLock(&tsLogObj.logMutex);

  if (tsLogObj.lines > tsNumOfLogLines && tsLogObj.openInProgress == 0) {
    tsLogObj.openInProgress = 1;

    uInfo("open new log file ......");
    TdThread     thread;
    TdThreadAttr attr;
    taosThreadAttrInit(&attr);
    taosThreadAttrSetDetachState(&attr, PTHREAD_CREATE_DETACHED);

    taosThreadCreate(&thread, &attr, taosThreadToOpenNewFile, NULL);
    taosThreadAttrDestroy(&attr);
  }

  taosThreadMutexUnlock(&tsLogObj.logMutex);

  return 0;
}

void taosResetLog() {
  // force create a new log file
  tsLogObj.lines = tsNumOfLogLines + 10;

  taosOpenNewLogFile();

  uInfo("==================================");
  uInfo("   reset log file ");
}

static bool taosCheckFileIsOpen(char *logFileName) {
  TdFilePtr pFile = taosOpenFile(logFileName, TD_FILE_WRITE);
  if (pFile == NULL) {
    if (lastErrorIsFileNotExist()) {
      return false;
    } else {
      printf("\nfailed to open log file:%s, reason:%s\n", logFileName, strerror(errno));
      return true;
    }
  }

  if (taosLockLogFile(pFile)) {
    taosUnLockLogFile(pFile);
    taosCloseFile(&pFile);
    return false;
  } else {
    taosCloseFile(&pFile);
    return true;
  }
}

static void taosGetLogFileName(char *fn) {
  if (tsLogObj.fileNum > 1) {
    for (int32_t i = 0; i < tsLogObj.fileNum; i++) {
      char fileName[LOG_FILE_NAME_LEN];

      snprintf(fileName, LOG_FILE_NAME_LEN, "%s%d.0", fn, i);
      bool file1open = taosCheckFileIsOpen(fileName);

      snprintf(fileName, LOG_FILE_NAME_LEN, "%s%d.1", fn, i);
      bool file2open = taosCheckFileIsOpen(fileName);

      if (!file1open && !file2open) {
        snprintf(tsLogObj.logName, LOG_FILE_NAME_LEN, "%s%d", fn, i);
        return;
      }
    }
  }

  if (strlen(fn) < LOG_FILE_NAME_LEN) {
    strcpy(tsLogObj.logName, fn);
  }
}

static int32_t taosOpenLogFile(char *fn, int32_t maxFileNum) {
#ifdef WINDOWS_STASH
  /*
   * always set maxFileNum to 1
   * means client log filename is unique in windows
   */
  maxFileNum = 1;
#endif

  char    name[LOG_FILE_NAME_LEN + 50] = "\0";
  int32_t logstat0_mtime, logstat1_mtime;

  tsLogObj.fileNum = maxFileNum;
  taosGetLogFileName(fn);

  if (strlen(fn) < LOG_FILE_NAME_LEN + 50 - 2) {
    strcpy(name, fn);
    strcat(name, ".0");
  }
  bool log0Exist = taosStatFile(name, NULL, &logstat0_mtime, NULL) >= 0;

  if (strlen(fn) < LOG_FILE_NAME_LEN + 50 - 2) {
    strcpy(name, fn);
    strcat(name, ".1");
  }
  bool log1Exist = taosStatFile(name, NULL, &logstat1_mtime, NULL) >= 0;

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

  char fileName[LOG_FILE_NAME_LEN + 50] = "\0";
  sprintf(fileName, "%s.%d", tsLogObj.logName, tsLogObj.flag);
  taosThreadMutexInit(&tsLogObj.logMutex, NULL);

  taosUmaskFile(0);
  tsLogObj.logHandle->pFile = taosOpenFile(fileName, TD_FILE_CREATE | TD_FILE_WRITE);

  if (tsLogObj.logHandle->pFile == NULL) {
    printf("\nfailed to open log file:%s, reason:%s\n", fileName, strerror(errno));
    return -1;
  }
  taosLockLogFile(tsLogObj.logHandle->pFile);

  // only an estimate for number of lines
  int64_t filesize = 0;
  if (taosFStatFile(tsLogObj.logHandle->pFile, &filesize, NULL) < 0) {
    printf("\nfailed to fstat log file:%s, reason:%s\n", fileName, strerror(errno));
    return -1;
  }
  tsLogObj.lines = (int32_t)(filesize / 60);

  taosLSeekFile(tsLogObj.logHandle->pFile, 0, SEEK_END);

  sprintf(name, "==================================================\n");
  taosWriteFile(tsLogObj.logHandle->pFile, name, (uint32_t)strlen(name));
  sprintf(name, "                new log file                      \n");
  taosWriteFile(tsLogObj.logHandle->pFile, name, (uint32_t)strlen(name));
  sprintf(name, "==================================================\n");
  taosWriteFile(tsLogObj.logHandle->pFile, name, (uint32_t)strlen(name));

  return 0;
}

static void taosUpdateLogNums(ELogLevel level) {
  switch (level) {
    case DEBUG_ERROR:
      atomic_add_fetch_64(&tsNumOfErrorLogs, 1);
      break;
    case DEBUG_INFO:
      atomic_add_fetch_64(&tsNumOfInfoLogs, 1);
      break;
    case DEBUG_DEBUG:
      atomic_add_fetch_64(&tsNumOfDebugLogs, 1);
      break;
    case DEBUG_DUMP:
    case DEBUG_TRACE:
      atomic_add_fetch_64(&tsNumOfTraceLogs, 1);
      break;
    default:
      break;
  }
}

static inline int32_t taosBuildLogHead(char *buffer, const char *flags) {
  struct tm      Tm, *ptm;
  struct timeval timeSecs;

  taosGetTimeOfDay(&timeSecs);
  time_t curTime = timeSecs.tv_sec;
  ptm = taosLocalTime(&curTime, &Tm, NULL);

  return sprintf(buffer, "%02d/%02d %02d:%02d:%02d.%06d %08" PRId64 " %s", ptm->tm_mon + 1, ptm->tm_mday, ptm->tm_hour,
                 ptm->tm_min, ptm->tm_sec, (int32_t)timeSecs.tv_usec, taosGetSelfPthreadId(), flags);
}

static inline void taosPrintLogImp(ELogLevel level, int32_t dflag, const char *buffer, int32_t len) {
  if ((dflag & DEBUG_FILE) && tsLogObj.logHandle && tsLogObj.logHandle->pFile != NULL && osLogSpaceAvailable()) {
    taosUpdateLogNums(level);
    if (tsAsyncLog) {
      taosPushLogBuffer(tsLogObj.logHandle, buffer, len);
    } else {
      taosWriteFile(tsLogObj.logHandle->pFile, buffer, len);
    }

    if (tsNumOfLogLines > 0) {
      atomic_add_fetch_32(&tsLogObj.lines, 1);
      if ((tsLogObj.lines > tsNumOfLogLines) && (tsLogObj.openInProgress == 0)) {
        taosOpenNewLogFile();
      }
    }
  }

  if (dflag & DEBUG_SCREEN) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-result"
    write(1, buffer, (uint32_t)len);
#pragma GCC diagnostic pop
  }
}

void taosPrintLog(const char *flags, ELogLevel level, int32_t dflag, const char *format, ...) {
  if (!(dflag & DEBUG_FILE) && !(dflag & DEBUG_SCREEN)) return;

  char    buffer[LOG_MAX_LINE_BUFFER_SIZE];
  int32_t len = taosBuildLogHead(buffer, flags);

  va_list argpointer;
  va_start(argpointer, format);
  int32_t writeLen = len + vsnprintf(buffer + len, LOG_MAX_LINE_BUFFER_SIZE - len, format, argpointer);
  va_end(argpointer);

  if (writeLen > LOG_MAX_LINE_SIZE) writeLen = LOG_MAX_LINE_SIZE;
  buffer[writeLen++] = '\n';
  buffer[writeLen] = 0;

  taosPrintLogImp(level, dflag, buffer, writeLen);

  if (tsLogFp && level <= DEBUG_INFO) {
    buffer[writeLen - 1] = 0;
    (*tsLogFp)(taosGetTimestampMs(), level, buffer + len);
  }
}

void taosPrintLongString(const char *flags, ELogLevel level, int32_t dflag, const char *format, ...) {
  if (!osLogSpaceAvailable()) return;
  if (!(dflag & DEBUG_FILE) && !(dflag & DEBUG_SCREEN)) return;

  char   *buffer = taosMemoryMalloc(LOG_MAX_LINE_DUMP_BUFFER_SIZE);
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
  if (!osLogSpaceAvailable()) return;

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

  atomic_add_fetch_64(&tsNumOfSlowLogs, 1);

  if (tsAsyncLog) {
    taosPushLogBuffer(tsLogObj.slowHandle, buffer, len);
  } else {
    taosWriteFile(tsLogObj.slowHandle->pFile, buffer, len);
  }

  taosMemoryFree(buffer);
}

void taosDumpData(unsigned char *msg, int32_t len) {
  if (!osLogSpaceAvailable()) return;
  taosUpdateLogNums(DEBUG_DUMP);

  char    temp[256] = {0};
  int32_t i, pos = 0, c = 0;

  for (i = 0; i < len; ++i) {
    sprintf(temp + pos, "%02x ", msg[i]);
    c++;
    pos += 3;
    if (c >= 16) {
      temp[pos++] = '\n';
      taosWriteFile(tsLogObj.logHandle->pFile, temp, (uint32_t)pos);
      c = 0;
      pos = 0;
    }
  }

  temp[pos++] = '\n';

  taosWriteFile(tsLogObj.logHandle->pFile, temp, (uint32_t)pos);
}

static void taosCloseLogByFd(TdFilePtr pFile) {
  if (pFile != NULL) {
    taosUnLockLogFile(pFile);
    taosCloseFile(&pFile);
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

  taosThreadMutexLock(&LOG_BUF_MUTEX(pLogBuf));
  start = LOG_BUF_START(pLogBuf);
  end = LOG_BUF_END(pLogBuf);

  remainSize = (start > end) ? (start - end - 1) : (start + LOG_BUF_SIZE(pLogBuf) - end - 1);

  if (lostLine > 0) {
    snprintf(tmpBuf, tListLen(tmpBuf), "...Lost %" PRId64 " lines here...\n", lostLine);
    tmpBufLen = (int32_t)strlen(tmpBuf);
  }

  if (remainSize <= msgLen || ((lostLine > 0) && (remainSize <= (msgLen + tmpBufLen)))) {
    lostLine++;
    tsAsyncLogLostLines++;
    taosThreadMutexUnlock(&LOG_BUF_MUTEX(pLogBuf));
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

  taosThreadMutexUnlock(&LOG_BUF_MUTEX(pLogBuf));

  return 0;
}

static int32_t taosGetLogRemainSize(SLogBuff *pLogBuf, int32_t start, int32_t end) {
  int32_t rSize = end - start;

  return rSize >= 0 ? rSize : LOG_BUF_SIZE(pLogBuf) + rSize;
}

static void taosWriteLog(SLogBuff *pLogBuf) {
  int32_t start = LOG_BUF_START(pLogBuf);
  int32_t end = LOG_BUF_END(pLogBuf);

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
    taosWriteFile(pLogBuf->pFile, LOG_BUF_BUFFER(pLogBuf) + start, pollSize);
  } else {
    int32_t tsize = LOG_BUF_SIZE(pLogBuf) - start;
    taosWriteFile(pLogBuf->pFile, LOG_BUF_BUFFER(pLogBuf) + start, tsize);

    taosWriteFile(pLogBuf->pFile, LOG_BUF_BUFFER(pLogBuf), end);
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

  LOG_BUF_START(pLogBuf) = (LOG_BUF_START(pLogBuf) + pollSize) % LOG_BUF_SIZE(pLogBuf);

  start = LOG_BUF_START(pLogBuf);
  end = LOG_BUF_END(pLogBuf);

  pollSize = taosGetLogRemainSize(pLogBuf, start, end);
  if (pollSize < pLogBuf->minBuffSize) {
    return;
  }

  pLogBuf->writeInterval = 0;
}

static void *taosAsyncOutputLog(void *param) {
  SLogBuff *pLogBuf = (SLogBuff *)tsLogObj.logHandle;
  SLogBuff *pSlowBuf = (SLogBuff *)tsLogObj.slowHandle;

  setThreadName("log");
  int32_t count = 0;
  int32_t updateCron = 0;
  int32_t writeInterval = 0;

  while (1) {
    writeInterval = TMIN(pLogBuf->writeInterval, pSlowBuf->writeInterval);
    count += writeInterval;
    updateCron++;
    taosMsleep(writeInterval);
    if (count > 1000) {
      osUpdate();
      count = 0;
    }

    // Polling the buffer
    taosWriteLog(pLogBuf);
    taosWriteLog(pSlowBuf);

    if (updateCron >= 3600 * 24 * 40 / 2) {
      taosUpdateDaylight();
      updateCron = 0;
    }

    if (pLogBuf->stop || pSlowBuf->stop) {
      pLogBuf->lastDuration = LOG_MAX_WAIT_MSEC;
      taosWriteLog(pLogBuf);
      taosWriteLog(pSlowBuf);
      break;
    }
  }

  return NULL;
}

bool taosAssertDebug(bool condition, const char *file, int32_t line, const char *format, ...) {
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
  taosPrintTrace(flags, level, dflag, -1);

  if (tsAssert) {
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
      terrno = TAOS_SYSTEM_ERROR(errno);
      taosPrintLog(flags, level, dflag, "failed to open file:%s since %s", filepath, terrstr());
      goto _return;
    }

    taosLockFile(pFile);

    int64_t writeSize = taosWriteFile(pFile, &msgLen, sizeof(msgLen));
    if (sizeof(msgLen) != writeSize) {
      taosUnLockFile(pFile);
      taosPrintLog(flags, level, dflag, "failed to write len to file:%s,%p wlen:%" PRId64 " tlen:%lu since %s",
                   filepath, pFile, writeSize, sizeof(msgLen), terrstr());
      goto _return;
    }

    writeSize = taosWriteFile(pFile, pMsg, msgLen);
    if (msgLen != writeSize) {
      taosUnLockFile(pFile);
      taosPrintLog(flags, level, dflag, "failed to write file:%s,%p wlen:%" PRId64 " tlen:%" PRId64 " since %s",
                   filepath, pFile, writeSize, msgLen, terrstr());
      goto _return;
    }

    taosUnLockFile(pFile);
  }

_return:

  if (pFile) taosCloseFile(&pFile);

  terrno = TAOS_SYSTEM_ERROR(errno);
  taosPrintLog(flags, level, dflag, "crash signal is %d", signum);

#ifdef _TD_DARWIN_64
  taosPrintTrace(flags, level, dflag, 4);
#elif !defined(WINDOWS)
  taosPrintLog(flags, level, dflag, "sender PID:%d cmdline:%s", ((siginfo_t *)sigInfo)->si_pid,
               taosGetCmdlineByPID(((siginfo_t *)sigInfo)->si_pid));
  taosPrintTrace(flags, level, dflag, 3);
#else
  taosPrintTrace(flags, level, dflag, 8);
#endif

  taosMemoryFree(pMsg);
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
      if (ENOENT == errno) {
        return;
      }

      terrno = TAOS_SYSTEM_ERROR(errno);
      taosPrintLog(flags, level, dflag, "failed to stat file:%s since %s", filepath, terrstr());
      return;
    }

    if (filesize <= 0) {
      return;
    }

    pFile = taosOpenFile(filepath, TD_FILE_READ | TD_FILE_WRITE);
    if (pFile == NULL) {
      if (ENOENT == errno) {
        return;
      }

      terrno = TAOS_SYSTEM_ERROR(errno);
      taosPrintLog(flags, level, dflag, "failed to open file:%s since %s", filepath, terrstr());
      return;
    }

    taosLockFile(pFile);
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
    taosFtruncateFile(pFile, 0);
  }
  taosUnLockFile(pFile);
  taosCloseFile(&pFile);
  taosMemoryFree(buf);

  *pMsg = NULL;
  *pMsgLen = 0;
  *pFd = NULL;
}

void taosReleaseCrashLogFile(TdFilePtr pFile, bool truncateFile) {
  if (truncateFile) {
    taosFtruncateFile(pFile, 0);
  }

  taosUnLockFile(pFile);
  taosCloseFile(&pFile);
}

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
