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

#ifndef TDENGINE_TLOG_H
#define TDENGINE_TLOG_H

#ifdef __cplusplus
extern "C" {
#endif

#define DEBUG_ERROR 1U
#define DEBUG_WARN  2U
#define DEBUG_TRACE 4U
#define DEBUG_DUMP  8U

#define DEBUG_FILE   0x80
#define DEBUG_SCREEN 0x40

int32_t taosInitLog(char *logName, int32_t numOfLogLines, int32_t maxFiles);
void    taosCloseLog();
void    taosResetLog();

void    taosPrintLog(const char *const flags, int32_t dflag, const char *const format, ...);
void    taosPrintLongString(const char *const flags, int32_t dflag, const char *const format, ...);
void    taosDumpData(unsigned char *msg, int32_t len);

void    taosPrintStrLog(int32_t dflag, int longorshort, const char *str);

#define TLOG_MAX_LOGLINE_SIZE              (1000)
#define TLOG_MAX_LOGLINE_BUFFER_SIZE       (TLOG_MAX_LOGLINE_SIZE + 10)

#define TLOG(flags, dflag, fmt, ...) do {                                                   \
  char           buffer[TLOG_MAX_LOGLINE_BUFFER_SIZE] = { 0 };                              \
  int            bytes = sizeof(buffer);                                                    \
  char          *p     = buffer;                                                            \
  int            n;                                                                         \
  struct tm      Tm, *ptm;                                                                  \
  struct timeval timeSecs;                                                                  \
  time_t         curTime;                                                                   \
                                                                                            \
  gettimeofday(&timeSecs, NULL);                                                            \
  curTime = timeSecs.tv_sec;                                                                \
  ptm = localtime_r(&curTime, &Tm);                                                         \
                                                                                            \
  n = snprintf(p, bytes,                                                                    \
               "%02d/%02d %02d:%02d:%02d.%06d 0x%" PRId64 " ",                              \
               ptm->tm_mon + 1, ptm->tm_mday, ptm->tm_hour,                                 \
               ptm->tm_min, ptm->tm_sec, (int32_t)timeSecs.tv_usec, taosGetPthreadId());    \
  p += n; bytes -= n;                                                                       \
                                                                                            \
  n = snprintf(p, bytes, "%s", flags);                                                      \
  p += n; bytes -= n;                                                                       \
                                                                                            \
  n = snprintf(p, bytes, fmt, ##__VA_ARGS__);                                               \
  p += n; bytes -= n;                                                                       \
                                                                                            \
  taosPrintStrLog(dflag, 0, buffer);                                                        \
} while (0)

#define TLOG_MAX_LOGLINE_DUMP_SIZE         (65 * 1024)
#define TLOG_MAX_LOGLINE_DUMP_BUFFER_SIZE  (TLOG_MAX_LOGLINE_DUMP_SIZE + 10)
#define TLOGLONG(flags, dflag, fmt, ...) do {                                               \
  char           buffer[TLOG_MAX_LOGLINE_DUMP_BUFFER_SIZE];                                 \
  int            bytes = sizeof(buffer);                                                    \
  char          *p = buffer;                                                                \
  int            n;                                                                         \
  struct tm      Tm, *ptm;                                                                  \
  struct timeval timeSecs;                                                                  \
  time_t         curTime;                                                                   \
                                                                                            \
  gettimeofday(&timeSecs, NULL);                                                            \
  curTime = timeSecs.tv_sec;                                                                \
  ptm = localtime_r(&curTime, &Tm);                                                         \
                                                                                            \
  n = snprintf(p, bytes,                                                                    \
               "%02d/%02d %02d:%02d:%02d.%06d 0x%" PRId64 " ",                              \
               ptm->tm_mon + 1, ptm->tm_mday, ptm->tm_hour,                                 \
               ptm->tm_min, ptm->tm_sec, (int32_t)timeSecs.tv_usec, taosGetPthreadId());    \
  p += n; bytes -= n;                                                                       \
                                                                                            \
  n = snprintf(p, bytes, "%s", flags);                                                      \
  p += n; bytes -= n;                                                                       \
                                                                                            \
  n = snprintf(p, bytes, fmt, ##__VA_ARGS__);                                             \
  p += n; bytes -= n;                                                                       \
                                                                                            \
  taosPrintStrLog(dflag, 1, buffer);                                                        \
} while (0)



#ifdef __cplusplus
}
#endif

#endif
