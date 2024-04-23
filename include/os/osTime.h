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

#ifndef _TD_OS_TIME_H_
#define _TD_OS_TIME_H_

#ifdef __cplusplus
extern "C" {
#endif

// If the error is in a third-party library, place this header file under the third-party library header file.
// When you want to use this feature, you should find or add the same function in the following section.
#ifndef ALLOW_FORBID_FUNC
#define strptime     STRPTIME_FUNC_TAOS_FORBID
#define gettimeofday GETTIMEOFDAY_FUNC_TAOS_FORBID
#define localtime    LOCALTIME_FUNC_TAOS_FORBID
#define localtime_s  LOCALTIMES_FUNC_TAOS_FORBID
#define localtime_r  LOCALTIMER_FUNC_TAOS_FORBID
#define time         TIME_FUNC_TAOS_FORBID
#define mktime       MKTIME_FUNC_TAOS_FORBID
#endif

#ifdef WINDOWS

#define CLOCK_REALTIME 0
#define CLOCK_MONOTONIC 0

#define MILLISECOND_PER_SECOND (1000i64)
#else
#define MILLISECOND_PER_SECOND ((int64_t)1000LL)
#endif

#define MILLISECOND_PER_MINUTE (MILLISECOND_PER_SECOND * 60)
#define MILLISECOND_PER_HOUR   (MILLISECOND_PER_MINUTE * 60)
#define MILLISECOND_PER_DAY    (MILLISECOND_PER_HOUR * 24)
#define MILLISECOND_PER_WEEK   (MILLISECOND_PER_DAY * 7)

#define NANOSECOND_PER_USEC   (1000LL)
#define NANOSECOND_PER_MSEC   (1000000LL)
#define NANOSECOND_PER_SEC    (1000000000LL)
#define NANOSECOND_PER_MINUTE (NANOSECOND_PER_SEC * 60)
#define NANOSECOND_PER_HOUR   (NANOSECOND_PER_MINUTE * 60)
#define NANOSECOND_PER_DAY    (NANOSECOND_PER_HOUR * 24)
#define NANOSECOND_PER_WEEK   (NANOSECOND_PER_DAY * 7)

int32_t taosGetTimeOfDay(struct timeval *tv);

int32_t taosClockGetTime(int clock_id, struct timespec *pTS);

//@return timestamp in second
int32_t taosGetTimestampSec();

//@return timestamp in millisecond
static FORCE_INLINE int64_t taosGetTimestampMs() {
  struct timeval systemTime;
  taosGetTimeOfDay(&systemTime);
  return (int64_t)systemTime.tv_sec * 1000LL + (int64_t)systemTime.tv_usec / 1000;
}

//@return timestamp in microsecond
static FORCE_INLINE int64_t taosGetTimestampUs() {
  struct timeval systemTime;
  taosGetTimeOfDay(&systemTime);
  return (int64_t)systemTime.tv_sec * 1000000LL + (int64_t)systemTime.tv_usec;
}

//@return timestamp in nanosecond
static FORCE_INLINE int64_t taosGetTimestampNs() {
  struct timespec systemTime = {0};
  taosClockGetTime(CLOCK_REALTIME, &systemTime);
  return (int64_t)systemTime.tv_sec * 1000000000LL + (int64_t)systemTime.tv_nsec;
}

//@return timestamp of monotonic clock in millisecond
static FORCE_INLINE int64_t taosGetMonoTimestampMs() {
  struct timespec systemTime = {0};
  taosClockGetTime(CLOCK_MONOTONIC, &systemTime);
  return (int64_t)systemTime.tv_sec * 1000LL + (int64_t)systemTime.tv_nsec / 1000000;
}

char      *taosStrpTime(const char *buf, const char *fmt, struct tm *tm);
struct tm *taosLocalTime(const time_t *timep, struct tm *result, char *buf);
struct tm *taosLocalTimeNolock(struct tm *result, const time_t *timep, int dst);
time_t     taosTime(time_t *t);
time_t     taosMktime(struct tm *timep);
int64_t    user_mktime64(const uint32_t year, const uint32_t mon, const uint32_t day, const uint32_t hour,
                         const uint32_t min, const uint32_t sec, int64_t time_zone);

#ifdef __cplusplus
}
#endif

#endif /*_TD_OS_TIME_H_*/
