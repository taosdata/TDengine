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

int32_t taosParseTime(char* timestr, int64_t* time, int32_t len, int32_t timePrec, int8_t dayligth);
void    deltaToUtcInitOnce();

int64_t convertTimePrecision(int64_t time, int32_t fromPrecision, int32_t toPrecision);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TTIME_H
