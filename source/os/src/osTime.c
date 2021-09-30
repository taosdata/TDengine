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

#include "os.h"

#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
#include <winsock2.h>
#else
#endif

FORCE_INLINE int32_t taosGetTimeOfDay(struct timeval *tv) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
    time_t t;
  t = time(NULL);
  SYSTEMTIME st;
  GetLocalTime(&st);

  tv->tv_sec = (long)t;
  tv->tv_usec = st.wMilliseconds * 1000;

  return 0;
#else
  return gettimeofday(tv, NULL);
#endif
}