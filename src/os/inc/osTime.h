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

#ifndef TDENGINE_OS_TIME_H
#define TDENGINE_OS_TIME_H

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"
#include "taosdef.h"

#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  #ifdef _TD_GO_DLL_
    #define MILLISECOND_PER_SECOND (1000LL)
  #else
    #define MILLISECOND_PER_SECOND (1000i64)
  #endif
#else
  #define MILLISECOND_PER_SECOND ((int64_t)1000L)
#endif

#define MILLISECOND_PER_MINUTE (MILLISECOND_PER_SECOND * 60)
#define MILLISECOND_PER_HOUR   (MILLISECOND_PER_MINUTE * 60)
#define MILLISECOND_PER_DAY    (MILLISECOND_PER_HOUR * 24)
#define MILLISECOND_PER_WEEK   (MILLISECOND_PER_DAY * 7)

//@return timestamp in second
int32_t taosGetTimestampSec();

//@return timestamp in millisecond
static FORCE_INLINE int64_t taosGetTimestampMs() {
  struct timeval systemTime;
  gettimeofday(&systemTime, NULL);
  return (int64_t)systemTime.tv_sec * 1000L + (int64_t)systemTime.tv_usec / 1000;
}

//@return timestamp in microsecond
static FORCE_INLINE int64_t taosGetTimestampUs() {
  struct timeval systemTime;
  gettimeofday(&systemTime, NULL);
  return (int64_t)systemTime.tv_sec * 1000000L + (int64_t)systemTime.tv_usec;
}

//@return timestamp in nanosecond
static FORCE_INLINE int64_t taosGetTimestampNs() {
  struct timespec systemTime = {0};
  clock_gettime(CLOCK_REALTIME, &systemTime);
  return (int64_t)systemTime.tv_sec * 1000000000L + (int64_t)systemTime.tv_nsec;
}

/*
 * @return timestamp decided by global conf variable, tsTimePrecision
 * if precision == TSDB_TIME_PRECISION_MICRO, it returns timestamp in microsecond.
 *    precision == TSDB_TIME_PRECISION_MILLI, it returns timestamp in millisecond.
 */
static FORCE_INLINE int64_t taosGetTimestamp(int32_t precision) {
  if (precision == TSDB_TIME_PRECISION_MICRO) {
    return taosGetTimestampUs();
  } else if (precision == TSDB_TIME_PRECISION_NANO) {
    return taosGetTimestampNs();
  }else {
    return taosGetTimestampMs();
  }
}


typedef struct SInterval {
  int32_t tz;            // query client timezone
  char    intervalUnit;
  char    slidingUnit;
  char    offsetUnit;
  int64_t interval;
  int64_t sliding;
  int64_t offset;
} SInterval;

typedef struct SSessionWindow {
  int64_t gap;             // gap between two session window(in microseconds)
  int32_t primaryColId;    // primary timestamp column
} SSessionWindow;

int64_t taosTimeAdd(int64_t t, int64_t duration, char unit, int32_t precision);
int64_t taosTimeTruncate(int64_t t, const SInterval* pInterval, int32_t precision);
int32_t taosTimeCountInterval(int64_t skey, int64_t ekey, int64_t interval, char unit, int32_t precision);

int32_t parseAbsoluteDuration(char* token, int32_t tokenlen, int64_t* ts, char* unit, int32_t timePrecision);
int32_t parseNatualDuration(const char* token, int32_t tokenLen, int64_t* duration, char* unit, int32_t timePrecision);

int32_t taosParseTime(char* timestr, int64_t* time, int32_t len, int32_t timePrec, int8_t dayligth);
void    deltaToUtcInitOnce();

int64_t convertTimePrecision(int64_t time, int32_t fromPrecision, int32_t toPrecision);
#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TTIME_H
