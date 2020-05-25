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
#include "os.h"
#include "tulog.h"
#include "tlog.h"
#include "tutil.h"

#define MAX_LOGLINE_SIZE              (1000)
#define MAX_LOGLINE_BUFFER_SIZE       (MAX_LOGLINE_SIZE + 10)
#define MAX_LOGLINE_CONTENT_SIZE      (MAX_LOGLINE_SIZE - 100)
#define MAX_LOGLINE_DUMP_SIZE         (65 * 1024)
#define MAX_LOGLINE_DUMP_BUFFER_SIZE  (MAX_LOGLINE_DUMP_SIZE + 10)
#define MAX_LOGLINE_DUMP_CONTENT_SIZE (MAX_LOGLINE_DUMP_SIZE - 100)

#define LOG_FILE_NAME_LEN          300
#define TSDB_DEFAULT_LOG_BUF_SIZE (512 * 1024)  // 512K
#define TSDB_MIN_LOG_BUF_SIZE      1024         // 1K
#define TSDB_MAX_LOG_BUF_SIZE     (1024 * 1024) // 1M
#define TSDB_DEFAULT_LOG_BUF_UNIT  1024         // 1K

#define LOG_BUF_BUFFER(x) ((x)->buffer)
#define LOG_BUF_START(x)  ((x)->buffStart)
#define LOG_BUF_END(x)    ((x)->buffEnd)
#define LOG_BUF_SIZE(x)   ((x)->buffSize)
#define LOG_BUF_MUTEX(x)  ((x)->buffMutex)

typedef struct {
  char *          buffer;
  int32_t         buffStart;
  int32_t         buffEnd;
  int32_t         buffSize;
  int32_t         fd;
  int32_t         stop;
  pthread_t       asyncThread;
  pthread_mutex_t buffMutex;
  tsem_t          buffNotEmpty;
} SLogBuff;

typedef struct {
  int32_t fileNum;
  int32_t maxLines;
  int32_t lines;
  int32_t flag;
  int32_t openInProgress;
  pid_t   pid;
  char    logName[LOG_FILE_NAME_LEN];
  SLogBuff *      logHandle;
  pthread_mutex_t logMutex;
} SLogObj;

int32_t tsAsyncLog = 1;
float   tsTotalLogDirGB = 0;
float   tsAvailLogDirGB = 0;
float   tsMinimalLogDirGB = 0.1;
char    tsLogDir[TSDB_FILENAME_LEN] = "/var/log/taos";

static SLogObj   tsLogObj = { .fileNum = 1 };
static void *    taosAsyncOutputLog(void *param);
static int32_t   taosPushLogBuffer(SLogBuff *tLogBuff, char *msg, int32_t msgLen);
static SLogBuff *taosLogBuffNew(int32_t bufSize);
static void      taosCloseLogByFd(int32_t oldFd);
static int32_t   taosOpenLogFile(char *fn, int32_t maxLines, int32_t maxFileNum);

static int32_t taosStartLog() {
  pthread_attr_t threadAttr;
  pthread_attr_init(&threadAttr);
  if (pthread_create(&(tsLogObj.logHandle->asyncThread), &threadAttr, taosAsyncOutputLog, tsLogObj.logHandle) != 0) {
    return -1;
  }
  pthread_attr_destroy(&threadAttr);
  return 0;
}

int32_t taosInitLog(char *logName, int numOfLogLines, int maxFiles) {
  tsLogObj.logHandle = taosLogBuffNew(TSDB_DEFAULT_LOG_BUF_SIZE);
  if (tsLogObj.logHandle == NULL) return -1;
  if (taosOpenLogFile(logName, numOfLogLines, maxFiles) < 0) return -1;
  if (taosStartLog() < 0) return -1;
  return 0;
}

static void taosStopLog() {
  if (tsLogObj.logHandle) {
    tsLogObj.logHandle->stop = 1;
  }
}

void taosCloseLog() {
  taosStopLog();
  tsem_post(&(tsLogObj.logHandle->buffNotEmpty));
  if (taosCheckPthreadValid(tsLogObj.logHandle->asyncThread)) {
    pthread_join(tsLogObj.logHandle->asyncThread, NULL);
  }
  // In case that other threads still use log resources causing invalid write in valgrind
  // we comment two lines below.
  // taosLogBuffDestroy(tsLogObj.logHandle);
  // taosCloseLog();
}

static bool taosLockFile(int32_t fd) {
  if (fd < 0) return false;

  if (tsLogObj.fileNum > 1) {
    int32_t ret = flock(fd, LOCK_EX | LOCK_NB);
    if (ret == 0) {
      return true;
    }
  }

  return false;
}

static void taosUnLockFile(int32_t fd) {
  if (fd < 0) return;

  if (tsLogObj.fileNum > 1) {
    flock(fd, LOCK_UN | LOCK_NB);
  }
}

static void *taosThreadToOpenNewFile(void *param) {
  char name[LOG_FILE_NAME_LEN + 20];

  tsLogObj.flag ^= 1;
  tsLogObj.lines = 0;
  sprintf(name, "%s.%d", tsLogObj.logName, tsLogObj.flag);

  umask(0);

  int32_t fd = open(name, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IRWXG | S_IRWXO);
  taosLockFile(fd);
  lseek(fd, 0, SEEK_SET);

  int32_t oldFd = tsLogObj.logHandle->fd;
  tsLogObj.logHandle->fd = fd;
  tsLogObj.lines = 0;
  tsLogObj.openInProgress = 0;
  uPrint("new log file is opened!!!");

  taosCloseLogByFd(oldFd);
  return NULL;
}

static int32_t taosOpenNewLogFile() {
  pthread_mutex_lock(&tsLogObj.logMutex);

  if (tsLogObj.lines > tsLogObj.maxLines && tsLogObj.openInProgress == 0) {
    tsLogObj.openInProgress = 1;

    uPrint("open new log file ......");
    pthread_t      thread;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

    pthread_create(&thread, &attr, taosThreadToOpenNewFile, NULL);
    pthread_attr_destroy(&attr);
  }

  pthread_mutex_unlock(&tsLogObj.logMutex);

  return 0;
}

void taosResetLog() {
  char lastName[LOG_FILE_NAME_LEN + 20];
  sprintf(lastName, "%s.%d", tsLogObj.logName, tsLogObj.flag);

  // force create a new log file
  tsLogObj.lines = tsLogObj.maxLines + 10;

  taosOpenNewLogFile();
  remove(lastName);

  uPrint("==================================");
  uPrint("   reset log file ");
}

static bool taosCheckFileIsOpen(char *logFileName) {
  int32_t exist = access(logFileName, F_OK);
  if (exist != 0) {
    return false;
  }

  int32_t fd = open(logFileName, O_WRONLY | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
  if (fd < 0) {
    printf("\nfailed to open log file:%s, reason:%s\n", logFileName, strerror(errno));
    return true;
  }

  if (taosLockFile(fd)) {
    taosUnLockFile(fd);
    tclose(fd);
    return false;
  } else {
    tclose(fd);
    return true;
  }
}

static void taosGetLogFileName(char *fn) {
  if (tsLogObj.fileNum > 1) {
    for (int32_t i = 0; i < tsLogObj.fileNum; i++) {
      char fileName[LOG_FILE_NAME_LEN];

      sprintf(fileName, "%s%d.0", fn, i);
      bool file1open = taosCheckFileIsOpen(fileName);

      sprintf(fileName, "%s%d.1", fn, i);
      bool file2open = taosCheckFileIsOpen(fileName);

      if (!file1open && !file2open) {
        sprintf(tsLogObj.logName, "%s%d", fn, i);
        return;
      }
    }
  }

  strcpy(tsLogObj.logName, fn);
}

static int32_t taosOpenLogFile(char *fn, int32_t maxLines, int32_t maxFileNum) {
#ifdef WINDOWS
  /*
  * always set maxFileNum to 1
  * means client log filename is unique in windows
  */
  maxFileNum = 1;
#endif

  char        name[LOG_FILE_NAME_LEN + 50] = "\0";
  struct stat logstat0, logstat1;
  int32_t     size;

  tsLogObj.maxLines = maxLines;
  tsLogObj.fileNum = maxFileNum;
  taosGetLogFileName(fn);

  strcpy(name, fn);
  strcat(name, ".0");

  // if none of the log files exist, open 0, if both exists, open the old one
  if (stat(name, &logstat0) < 0) {
    tsLogObj.flag = 0;
  } else {
    strcpy(name, fn);
    strcat(name, ".1");
    if (stat(name, &logstat1) < 0) {
      tsLogObj.flag = 1;
    } else {
      tsLogObj.flag = (logstat0.st_mtime > logstat1.st_mtime) ? 0 : 1;
    }
  }

  sprintf(name, "%s.%d", tsLogObj.logName, tsLogObj.flag);
  pthread_mutex_init(&tsLogObj.logMutex, NULL);

  umask(0);
  tsLogObj.logHandle->fd = open(name, O_WRONLY | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);

  if (tsLogObj.logHandle->fd < 0) {
    printf("\nfailed to open log file:%s, reason:%s\n", name, strerror(errno));
    return -1;
  }
  taosLockFile(tsLogObj.logHandle->fd);

  // only an estimate for number of lines
  struct stat filestat;
  fstat(tsLogObj.logHandle->fd, &filestat);
  size = (int32_t)filestat.st_size;
  tsLogObj.lines = size / 60;

  lseek(tsLogObj.logHandle->fd, 0, SEEK_END);

  sprintf(name, "==================================================\n");
  twrite(tsLogObj.logHandle->fd, name, (uint32_t)strlen(name));
  sprintf(name, "                new log file                      \n");
  twrite(tsLogObj.logHandle->fd, name, (uint32_t)strlen(name));
  sprintf(name, "==================================================\n");
  twrite(tsLogObj.logHandle->fd, name, (uint32_t)strlen(name));

  return 0;
}

void taosPrintLog(const char *const flags, int32_t dflag, const char *const format, ...) {
  if (tsTotalLogDirGB != 0 && tsAvailLogDirGB < tsMinimalLogDirGB) {
    printf("server disk:%s space remain %.3f GB, total %.1f GB, stop print log.\n", tsLogDir, tsAvailLogDirGB, tsTotalLogDirGB);
    fflush(stdout);
    return;
  }

  va_list        argpointer;
  char           buffer[MAX_LOGLINE_BUFFER_SIZE] = { 0 };
  int32_t        len;
  struct tm      Tm, *ptm;
  struct timeval timeSecs;
  time_t         curTime;

  gettimeofday(&timeSecs, NULL);
  curTime = timeSecs.tv_sec;
  ptm = localtime_r(&curTime, &Tm);

  len = sprintf(buffer, "%02d/%02d %02d:%02d:%02d.%06d 0x%" PRId64 " ", ptm->tm_mon + 1, ptm->tm_mday, ptm->tm_hour,
                ptm->tm_min, ptm->tm_sec, (int32_t)timeSecs.tv_usec, taosGetPthreadId());
  len += sprintf(buffer + len, "%s", flags);

  va_start(argpointer, format);
  int32_t writeLen = vsnprintf(buffer + len, MAX_LOGLINE_CONTENT_SIZE, format, argpointer);
  if (writeLen <= 0) {
    char tmp[MAX_LOGLINE_DUMP_BUFFER_SIZE] = {0};
    writeLen = vsnprintf(tmp, MAX_LOGLINE_DUMP_CONTENT_SIZE, format, argpointer);
    strncpy(buffer + len, tmp, MAX_LOGLINE_CONTENT_SIZE);
    len += MAX_LOGLINE_CONTENT_SIZE;
  } else if (writeLen >= MAX_LOGLINE_CONTENT_SIZE) {
    len += MAX_LOGLINE_CONTENT_SIZE;
  } else {
    len += writeLen;
  }
  va_end(argpointer);

  if (len > MAX_LOGLINE_SIZE) len = MAX_LOGLINE_SIZE;

  buffer[len++] = '\n';
  buffer[len] = 0;

  if ((dflag & DEBUG_FILE) && tsLogObj.logHandle && tsLogObj.logHandle->fd >= 0) {
    if (tsAsyncLog) {
      taosPushLogBuffer(tsLogObj.logHandle, buffer, len);
    } else {
      twrite(tsLogObj.logHandle->fd, buffer, len);
    }

    if (tsLogObj.maxLines > 0) {
      atomic_add_fetch_32(&tsLogObj.lines, 1);

      if ((tsLogObj.lines > tsLogObj.maxLines) && (tsLogObj.openInProgress == 0)) taosOpenNewLogFile();
    }
  }

  if (dflag & DEBUG_SCREEN) twrite(1, buffer, (uint32_t)len);
}

void taosDumpData(unsigned char *msg, int32_t len) {
  if (tsTotalLogDirGB != 0 && tsAvailLogDirGB < tsMinimalLogDirGB) {
    printf("server disk:%s space remain %.3f GB, total %.1f GB, stop dump log.\n", tsLogDir, tsAvailLogDirGB, tsTotalLogDirGB);
    fflush(stdout);
    return;
  }

  char temp[256];
  int32_t  i, pos = 0, c = 0;

  for (i = 0; i < len; ++i) {
    sprintf(temp + pos, "%02x ", msg[i]);
    c++;
    pos += 3;
    if (c >= 16) {
      temp[pos++] = '\n';
      twrite(tsLogObj.logHandle->fd, temp, (uint32_t)pos);
      c = 0;
      pos = 0;
    }
  }

  temp[pos++] = '\n';

  twrite(tsLogObj.logHandle->fd, temp, (uint32_t)pos);

  return;
}

void taosPrintLongString(const char *const flags, int32_t dflag, const char *const format, ...) {
  if (tsTotalLogDirGB != 0 && tsAvailLogDirGB < tsMinimalLogDirGB) {
    printf("server disk:%s space remain %.3f GB, total %.1f GB, stop write log.\n", tsLogDir, tsAvailLogDirGB, tsTotalLogDirGB);
    fflush(stdout);
    return;
  }

  va_list        argpointer;
  char           buffer[MAX_LOGLINE_DUMP_BUFFER_SIZE];
  int32_t            len;
  struct tm      Tm, *ptm;
  struct timeval timeSecs;
  time_t         curTime;

  gettimeofday(&timeSecs, NULL);
  curTime = timeSecs.tv_sec;
  ptm = localtime_r(&curTime, &Tm);

  len = sprintf(buffer, "%02d/%02d %02d:%02d:%02d.%06d 0x%" PRId64 " ", ptm->tm_mon + 1, ptm->tm_mday, ptm->tm_hour,
                ptm->tm_min, ptm->tm_sec, (int32_t)timeSecs.tv_usec, taosGetPthreadId());
  len += sprintf(buffer + len, "%s", flags);

  va_start(argpointer, format);
  len += vsnprintf(buffer + len, MAX_LOGLINE_DUMP_CONTENT_SIZE, format, argpointer);
  va_end(argpointer);

  if (len > MAX_LOGLINE_DUMP_SIZE) len = MAX_LOGLINE_DUMP_SIZE;

  buffer[len++] = '\n';
  buffer[len] = 0;

  if ((dflag & DEBUG_FILE) && tsLogObj.logHandle && tsLogObj.logHandle->fd >= 0) {
    taosPushLogBuffer(tsLogObj.logHandle, buffer, len);

    if (tsLogObj.maxLines > 0) {
      atomic_add_fetch_32(&tsLogObj.lines, 1);

      if ((tsLogObj.lines > tsLogObj.maxLines) && (tsLogObj.openInProgress == 0)) taosOpenNewLogFile();
    }
  }

  if (dflag & DEBUG_SCREEN) twrite(1, buffer, (uint32_t)len);
}

#if 0
void taosCloseLog() { 
  taosCloseLogByFd(tsLogObj.logHandle->fd); 
}
#endif

static void taosCloseLogByFd(int32_t fd) {
  if (fd >= 0) {
    taosUnLockFile(fd);
    tclose(fd);
  }
}

static SLogBuff *taosLogBuffNew(int32_t bufSize) {
  SLogBuff *tLogBuff = NULL;

  if (bufSize < TSDB_MIN_LOG_BUF_SIZE || bufSize > TSDB_MAX_LOG_BUF_SIZE) return NULL;

  tLogBuff = calloc(1, sizeof(SLogBuff));
  if (tLogBuff == NULL) return NULL;

  LOG_BUF_BUFFER(tLogBuff) = malloc(bufSize);
  if (LOG_BUF_BUFFER(tLogBuff) == NULL) goto _err;

  LOG_BUF_START(tLogBuff) = LOG_BUF_END(tLogBuff) = 0;
  LOG_BUF_SIZE(tLogBuff) = bufSize;
  tLogBuff->stop = 0;

  if (pthread_mutex_init(&LOG_BUF_MUTEX(tLogBuff), NULL) < 0) goto _err;
  tsem_init(&(tLogBuff->buffNotEmpty), 0, 0);

  return tLogBuff;

_err:
  tfree(LOG_BUF_BUFFER(tLogBuff));
  tfree(tLogBuff);
  return NULL;
}

#if 0
static void taosLogBuffDestroy(SLogBuff *tLogBuff) {
  tsem_destroy(&(tLogBuff->buffNotEmpty));
  pthread_mutex_destroy(&(tLogBuff->buffMutex));
  free(tLogBuff->buffer);
  tfree(tLogBuff);
}
#endif

static int32_t taosPushLogBuffer(SLogBuff *tLogBuff, char *msg, int32_t msgLen) {
  int32_t start = 0;
  int32_t end = 0;
  int32_t remainSize = 0;

  if (tLogBuff == NULL || tLogBuff->stop) return -1;

  pthread_mutex_lock(&LOG_BUF_MUTEX(tLogBuff));
  start = LOG_BUF_START(tLogBuff);
  end = LOG_BUF_END(tLogBuff);

  remainSize = (start > end) ? (end - start - 1) : (start + LOG_BUF_SIZE(tLogBuff) - end - 1);

  if (remainSize <= msgLen) {
    pthread_mutex_unlock(&LOG_BUF_MUTEX(tLogBuff));
    return -1;
  }

  if (start > end) {
    memcpy(LOG_BUF_BUFFER(tLogBuff) + end, msg, msgLen);
  } else {
    if (LOG_BUF_SIZE(tLogBuff) - end < msgLen) {
      memcpy(LOG_BUF_BUFFER(tLogBuff) + end, msg, LOG_BUF_SIZE(tLogBuff) - end);
      memcpy(LOG_BUF_BUFFER(tLogBuff), msg + LOG_BUF_SIZE(tLogBuff) - end, msgLen - LOG_BUF_SIZE(tLogBuff) + end);
    } else {
      memcpy(LOG_BUF_BUFFER(tLogBuff) + end, msg, msgLen);
    }
  }
  LOG_BUF_END(tLogBuff) = (LOG_BUF_END(tLogBuff) + msgLen) % LOG_BUF_SIZE(tLogBuff);

  // TODO : put string in the buffer

  tsem_post(&(tLogBuff->buffNotEmpty));

  pthread_mutex_unlock(&LOG_BUF_MUTEX(tLogBuff));

  return 0;
}

static int32_t taosPollLogBuffer(SLogBuff *tLogBuff, char *buf, int32_t bufSize) {
  int32_t start = LOG_BUF_START(tLogBuff);
  int32_t end = LOG_BUF_END(tLogBuff);
  int32_t pollSize = 0;

  if (start == end) {
    return 0;
  } else if (start < end) {
    pollSize = MIN(end - start, bufSize);

    memcpy(buf, LOG_BUF_BUFFER(tLogBuff) + start, pollSize);
    return pollSize;
  } else {
    pollSize = MIN(end + LOG_BUF_SIZE(tLogBuff) - start, bufSize);
    if (pollSize > LOG_BUF_SIZE(tLogBuff) - start) {
      int32_t tsize = LOG_BUF_SIZE(tLogBuff) - start;
      memcpy(buf, LOG_BUF_BUFFER(tLogBuff) + start, tsize);
      memcpy(buf + tsize, LOG_BUF_BUFFER(tLogBuff), pollSize - tsize);

    } else {
      memcpy(buf, LOG_BUF_BUFFER(tLogBuff) + start, pollSize);
    }
    return pollSize;
  }
}

static void *taosAsyncOutputLog(void *param) {
  SLogBuff *tLogBuff = (SLogBuff *)param;
  int32_t   log_size = 0;

  char tempBuffer[TSDB_DEFAULT_LOG_BUF_UNIT];

  while (1) {
    tsem_wait(&(tLogBuff->buffNotEmpty));

    // Polling the buffer
    while (1) {
      log_size = taosPollLogBuffer(tLogBuff, tempBuffer, TSDB_DEFAULT_LOG_BUF_UNIT);
      if (log_size) {
        twrite(tLogBuff->fd, tempBuffer, log_size);
        LOG_BUF_START(tLogBuff) = (LOG_BUF_START(tLogBuff) + log_size) % LOG_BUF_SIZE(tLogBuff);
      } else {
        break;
      }
    }

    if (tLogBuff->stop) break;
  }

  return NULL;
}
