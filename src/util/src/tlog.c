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

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "tlog.h"
#include "tutil.h"

#define MAX_LOGLINE_SIZE           1000
#define LOG_FILE_NAME_LEN          300
#define TSDB_DEFAULT_LOG_BUF_SIZE (64 * 1024)   // 10K
#define TSDB_MIN_LOG_BUF_SIZE      1024         // 1K
#define TSDB_MAX_LOG_BUF_SIZE     (1024 * 1024) // 1M
#define TSDB_DEFAULT_LOG_BUF_UNIT  1024         // 1K

typedef struct {
  char *          buffer;
  int             buffStart;
  int             buffEnd;
  int             buffSize;
  int             fd;
  int             stop;
  pthread_t       asyncThread;
  pthread_mutex_t buffMutex;
  sem_t           buffNotEmpty;
} SLogBuff;

int uDebugFlag = 131;  // all the messages
int tsAsyncLog = 1;

static SLogBuff *logHandle;
static int       taosLogFileNum = 1;
static int       taosLogMaxLines = 0;
static int       taosLogLines = 0;
static char      taosLogName[LOG_FILE_NAME_LEN];
static int       taosLogFlag = 0;
// static int  logFd = -1;
static int             openInProgress = 0;
static pthread_mutex_t logMutex;
void (*taosLogFp)(int level, const char *const format, ...) = NULL;
void (*taosLogSqlFp)(char *sql) = NULL;
void (*taosLogAcctFp)(char *acctId, int64_t currentPointsPerSecond, int64_t maxPointsPerSecond, int64_t totalTimeSeries,
                      int64_t maxTimeSeries, int64_t totalStorage, int64_t maxStorage, int64_t totalQueryTime,
                      int64_t maxQueryTime, int64_t totalInbound, int64_t maxInbound, int64_t totalOutbound,
                      int64_t maxOutbound, int64_t totalDbs, int64_t maxDbs, int64_t totalUsers, int64_t maxUsers,
                      int64_t totalStreams, int64_t maxStreams, int64_t totalConns, int64_t maxConns,
                      int8_t accessState) = NULL;
void *taosAsyncOutputLog(void *param);
int taosPushLogBuffer(SLogBuff *tLogBuff, char *msg, int msgLen);
SLogBuff *taosLogBuffNew(int bufSize);
void taosLogBuffDestroy(SLogBuff *tLogBuff);

int taosStartLog() {
  pthread_attr_t threadAttr;

  pthread_attr_init(&threadAttr);

  if (pthread_create(&(logHandle->asyncThread), &threadAttr, taosAsyncOutputLog, logHandle) != 0) {
    return -1;
  }

  pthread_attr_destroy(&threadAttr);

  return 0;
}

int taosInitLog(char *logName, int numOfLogLines, int maxFiles) {
  logHandle = taosLogBuffNew(TSDB_DEFAULT_LOG_BUF_SIZE);
  if (logHandle == NULL) return -1;

  if (taosOpenLogFileWithMaxLines(logName, numOfLogLines, maxFiles) < 0) return -1;

  if (taosStartLog() < 0) return -1;
  return 0;
}

void taosStopLog() {
  if (logHandle) logHandle->stop = 1;
}

void taosCloseLogger() {
  taosStopLog();
  sem_post(&(logHandle->buffNotEmpty));
  if (logHandle->asyncThread) pthread_join(logHandle->asyncThread, NULL);
  // In case that other threads still use log resources causing invalid write in
  // valgrind, we comment two lines below.
  // taosLogBuffDestroy(logHandle);
  // taosCloseLog();
}

void taosCloseLogByFd(int oldFd);
bool taosLockFile(int fd) {
  if (fd < 0) return false;

  if (taosLogFileNum > 1) {
    int ret = flock(fd, LOCK_EX | LOCK_NB);
    if (ret == 0) {
      return true;
    }
  }

  return false;
}

void taosUnLockFile(int fd) {
  if (fd < 0) return;

  if (taosLogFileNum > 1) {
    flock(fd, LOCK_UN | LOCK_NB);
  }
}

void *taosThreadToOpenNewFile(void *param) {
  char name[LOG_FILE_NAME_LEN];

  taosLogFlag ^= 1;
  taosLogLines = 0;
  sprintf(name, "%s.%d", taosLogName, taosLogFlag);

  umask(0);

  int fd = open(name, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IRWXG | S_IRWXO);
  taosLockFile(fd);
  lseek(fd, 0, SEEK_SET);

  int oldFd = logHandle->fd;
  logHandle->fd = fd;
  taosLogLines = 0;
  openInProgress = 0;
  pPrint("new log file is opened!!!");

  taosCloseLogByFd(oldFd);
  return NULL;
}

int taosOpenNewLogFile() {
  pthread_mutex_lock(&logMutex);

  if (taosLogLines > taosLogMaxLines && openInProgress == 0) {
    openInProgress = 1;

    pPrint("open new log file ......");
    pthread_t      thread;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

    pthread_create(&thread, &attr, taosThreadToOpenNewFile, NULL);
    pthread_attr_destroy(&attr);
  }

  pthread_mutex_unlock(&logMutex);

  return 0;
}

void taosResetLogFile() {
  char lastName[LOG_FILE_NAME_LEN];
  sprintf(lastName, "%s.%d", taosLogName, taosLogFlag);

  // force create a new log file
  taosLogLines = taosLogMaxLines + 10;

  taosOpenNewLogFile();
  remove(lastName);

  pPrint("==================================");
  pPrint("   reset log file ");
}

bool taosCheckFileIsOpen(char *logFileName) {
  int exist = access(logFileName, F_OK);
  if (exist != 0) {
    return false;
  }

  int fd = open(logFileName, O_WRONLY | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
  if (fd < 0) {
    printf("failed to open log file:%s, reason:%s\n", logFileName, strerror(errno));
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

void taosGetLogFileName(char *fn) {
  if (taosLogFileNum > 1) {
    for (int i = 0; i < taosLogFileNum; i++) {
      char fileName[LOG_FILE_NAME_LEN];

      sprintf(fileName, "%s%d.0", fn, i);
      bool file1open = taosCheckFileIsOpen(fileName);

      sprintf(fileName, "%s%d.1", fn, i);
      bool file2open = taosCheckFileIsOpen(fileName);

      if (!file1open && !file2open) {
        sprintf(taosLogName, "%s%d", fn, i);
        return;
      }
    }
  }

  strcpy(taosLogName, fn);
}

int taosOpenLogFileWithMaxLines(char *fn, int maxLines, int maxFileNum) {
  char        name[LOG_FILE_NAME_LEN] = "\0";
  struct stat logstat0, logstat1;
  int         size;

  taosLogMaxLines = maxLines;
  taosLogFileNum = maxFileNum;
  taosGetLogFileName(fn);

  strcpy(name, fn);
  strcat(name, ".0");

  // if none of the log files exist, open 0, if both exists, open the old one
  if (stat(name, &logstat0) < 0) {
    taosLogFlag = 0;
  } else {
    strcpy(name, fn);
    strcat(name, ".1");
    if (stat(name, &logstat1) < 0) {
      taosLogFlag = 1;
    } else {
      taosLogFlag = (logstat0.st_mtime > logstat1.st_mtime) ? 0 : 1;
    }
  }

  sprintf(name, "%s.%d", taosLogName, taosLogFlag);
  pthread_mutex_init(&logMutex, NULL);

  umask(0);
  logHandle->fd = open(name, O_WRONLY | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);

  if (logHandle->fd < 0) {
    printf("failed to open log file:%s, reason:%s\n", name, strerror(errno));
    return -1;
  }
  taosLockFile(logHandle->fd);

  // only an estimate for number of lines
  struct stat filestat;
  fstat(logHandle->fd, &filestat);
  size = (int)filestat.st_size;
  taosLogLines = size / 60;

  lseek(logHandle->fd, 0, SEEK_END);

  sprintf(name, "==================================================\n");
  write(logHandle->fd, name, (uint32_t)strlen(name));
  sprintf(name, "                new log file                      \n");
  write(logHandle->fd, name, (uint32_t)strlen(name));
  sprintf(name, "==================================================\n");
  write(logHandle->fd, name, (uint32_t)strlen(name));

  return 0;
}

char *tprefix(char *prefix) {
  struct tm      Tm, *ptm;
  struct timeval timeSecs;
  time_t         curTime;

  gettimeofday(&timeSecs, NULL);
  curTime = timeSecs.tv_sec;
  ptm = localtime_r(&curTime, &Tm);

  sprintf(prefix, "%02d/%02d %02d:%02d:%02d.%06d 0x%lx ", ptm->tm_mon + 1, ptm->tm_mday, ptm->tm_hour, ptm->tm_min,
          ptm->tm_sec, (int)timeSecs.tv_usec, pthread_self());
  return prefix;
}

void tprintf(const char *const flags, int dflag, const char *const format, ...) {
  va_list        argpointer;
  char           buffer[MAX_LOGLINE_SIZE + 10] = {0};
  int            len;
  struct tm      Tm, *ptm;
  struct timeval timeSecs;
  time_t         curTime;

  gettimeofday(&timeSecs, NULL);
  curTime = timeSecs.tv_sec;
  ptm = localtime_r(&curTime, &Tm);
  len = sprintf(buffer, "%02d/%02d %02d:%02d:%02d.%06d %lx ", ptm->tm_mon + 1, ptm->tm_mday, ptm->tm_hour, ptm->tm_min,
                ptm->tm_sec, (int)timeSecs.tv_usec, pthread_self());
  len += sprintf(buffer + len, "%s", flags);

  va_start(argpointer, format);
  len += vsnprintf(buffer + len, 900, format, argpointer);
  va_end(argpointer);

  if (len > MAX_LOGLINE_SIZE) len = MAX_LOGLINE_SIZE;

  buffer[len++] = '\n';
  buffer[len] = 0;

  if ((dflag & DEBUG_FILE) && logHandle && logHandle->fd >= 0) {
    if (tsAsyncLog) {
      taosPushLogBuffer(logHandle, buffer, len);
    } else {
      write(logHandle->fd, buffer, len);
    }

    if (taosLogMaxLines > 0) {
      __sync_fetch_and_add(&taosLogLines, 1);

      if ((taosLogLines > taosLogMaxLines) && (openInProgress == 0)) taosOpenNewLogFile();
    }
  }

  if (dflag & DEBUG_SCREEN) write(1, buffer, (unsigned int)len);
}

void taosDumpData(unsigned char *msg, int len) {
  char temp[256];
  int  i, pos = 0, c = 0;

  for (i = 0; i < len; ++i) {
    sprintf(temp + pos, "%02x ", msg[i]);
    c++;
    pos += 3;
    if (c >= 16) {
      temp[pos++] = '\n';
      write(logHandle->fd, temp, (unsigned int)pos);
      c = 0;
      pos = 0;
    }
  }

  temp[pos++] = '\n';

  write(logHandle->fd, temp, (unsigned int)pos);

  return;
}

void taosPrintLongString(const char *const flags, int dflag, const char *const format, ...) {
  va_list        argpointer;
  char           buffer[65 * 1024 + 10];
  int            len;
  struct tm      Tm, *ptm;
  struct timeval timeSecs;
  time_t         curTime;

  gettimeofday(&timeSecs, NULL);
  curTime = timeSecs.tv_sec;
  ptm = localtime_r(&curTime, &Tm);
  len = sprintf(buffer, "%02d/%02d %02d:%02d:%02d.%06d %lx ", ptm->tm_mon + 1, ptm->tm_mday, ptm->tm_hour, ptm->tm_min,
                ptm->tm_sec, (int)timeSecs.tv_usec, pthread_self());
  len += sprintf(buffer + len, "%s", flags);

  va_start(argpointer, format);
  len += vsnprintf(buffer + len, 64 * 1024, format, argpointer);
  va_end(argpointer);

  if (len > 64 * 1024) len = 64 * 1024;

  buffer[len++] = '\n';
  buffer[len] = 0;

  if ((dflag & DEBUG_FILE) && logHandle && logHandle->fd >= 0) {
    taosPushLogBuffer(logHandle, buffer, len);

    if (taosLogMaxLines > 0) {
      __sync_fetch_and_add(&taosLogLines, 1);

      if ((taosLogLines > taosLogMaxLines) && (openInProgress == 0)) taosOpenNewLogFile();
    }
  }

  if (dflag & DEBUG_SCREEN) write(1, buffer, (unsigned int)len);
}

void taosCloseLog() { taosCloseLogByFd(logHandle->fd); }

void taosCloseLogByFd(int fd) {
  if (fd >= 0) {
    taosUnLockFile(fd);
    tclose(fd);
  }
}

#define LOG_BUF_BUFFER(x) ((x)->buffer)
#define LOG_BUF_START(x) ((x)->buffStart)
#define LOG_BUF_END(x) ((x)->buffEnd)
#define LOG_BUF_SIZE(x) ((x)->buffSize)
#define LOG_BUF_MUTEX(x) ((x)->buffMutex)

SLogBuff *taosLogBuffNew(int bufSize) {
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
  sem_init(&(tLogBuff->buffNotEmpty), 0, 0);

  return tLogBuff;

_err:
  tfree(LOG_BUF_BUFFER(tLogBuff));
  tfree(tLogBuff);
  return NULL;
}

void taosLogBuffDestroy(SLogBuff *tLogBuff) {
  sem_destroy(&(tLogBuff->buffNotEmpty));
  pthread_mutex_destroy(&(tLogBuff->buffMutex));
  free(tLogBuff->buffer);
  tfree(tLogBuff);
}

int taosPushLogBuffer(SLogBuff *tLogBuff, char *msg, int msgLen) {
  int start = 0;
  int end = 0;
  int remainSize = 0;

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

  sem_post(&(tLogBuff->buffNotEmpty));

  pthread_mutex_unlock(&LOG_BUF_MUTEX(tLogBuff));

  return 0;
}

int taosPollLogBuffer(SLogBuff *tLogBuff, char *buf, int bufSize) {
  int start = LOG_BUF_START(tLogBuff);
  int end = LOG_BUF_END(tLogBuff);
  int pollSize = 0;

  if (start == end) {
    return 0;
  } else if (start < end) {
    pollSize = MIN(end - start, bufSize);

    memcpy(buf, LOG_BUF_BUFFER(tLogBuff) + start, pollSize);
    return pollSize;
  } else {
    pollSize = MIN(end + LOG_BUF_SIZE(tLogBuff) - start, bufSize);
    if (pollSize > LOG_BUF_SIZE(tLogBuff) - start) {
      int tsize = LOG_BUF_SIZE(tLogBuff) - start;
      memcpy(buf, LOG_BUF_BUFFER(tLogBuff) + start, tsize);
      memcpy(buf + tsize, LOG_BUF_BUFFER(tLogBuff), pollSize - tsize);

    } else {
      memcpy(buf, LOG_BUF_BUFFER(tLogBuff) + start, pollSize);
    }
    return pollSize;
  }
}

void *taosAsyncOutputLog(void *param) {
  SLogBuff *tLogBuff = (SLogBuff *)param;
  int       log_size = 0;

  char tempBuffer[TSDB_DEFAULT_LOG_BUF_UNIT];

  while (1) {
    sem_wait(&(tLogBuff->buffNotEmpty));

    // Polling the buffer
    while (1) {
      log_size = taosPollLogBuffer(tLogBuff, tempBuffer, TSDB_DEFAULT_LOG_BUF_UNIT);
      if (log_size) {
        write(tLogBuff->fd, tempBuffer, log_size);
        LOG_BUF_START(tLogBuff) = (LOG_BUF_START(tLogBuff) + log_size) % LOG_BUF_SIZE(tLogBuff);
      } else {
        break;
      }
    }

    if (tLogBuff->stop) break;
  }

  return NULL;
}
