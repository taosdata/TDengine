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

int32_t taosGetTimeOfDay(struct timeval *tv);

//@return timestamp in second
int32_t taosGetTimestampSec();

//@return timestamp in millisecond
static FORCE_INLINE int64_t taosGetTimestampMs() {
  struct timeval systemTime;
  taosGetTimeOfDay(&systemTime);
  return (int64_t)systemTime.tv_sec * 1000L + (int64_t)systemTime.tv_usec / 1000;
}

//@return timestamp in microsecond
static FORCE_INLINE int64_t taosGetTimestampUs() {
  struct timeval systemTime;
  taosGetTimeOfDay(&systemTime);
  return (int64_t)systemTime.tv_sec * 1000000L + (int64_t)systemTime.tv_usec;
}

//@return timestamp in nanosecond
static FORCE_INLINE int64_t taosGetTimestampNs() {
  struct timespec systemTime = {0};
  clock_gettime(CLOCK_REALTIME, &systemTime);
  return (int64_t)systemTime.tv_sec * 1000000000L + (int64_t)systemTime.tv_nsec;
}

#ifdef __cplusplus
}
#endif

#endif  /*_TD_OS_TIME_H_*/
