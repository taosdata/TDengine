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

#ifndef _TD_OS_INT_H_
#define _TD_OS_INT_H_

#ifdef __cplusplus
extern "C" {
#endif

#ifdef WINDOWS
typedef struct WindowsTimezoneObj {
  // UTC offset in seconds, using POSIX `timezone` convention: east-negative, west-positive.
  // Examples: UTC+8 (East 8) = -28800, UTC-8 (West 8) = +28800.
  // This matches what user_mktime64() and taosLocalTime() expect internally.
  // External callers that need east-positive (tm_gmtoff) should use taosGetTZOffsetSeconds().
  int64_t offset_seconds;
  char    name[TD_TIMEZONE_LEN];
  int32_t refCount;            // Reference count
  TdThreadMutex mutex;         // Protect concurrent access
} WindowsTimezoneObj;
#endif

#ifdef __cplusplus
}
#endif

#endif /*_TD_OS_INT_H_*/
