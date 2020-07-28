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

#include <time.h>  
#include <winsock2.h>

int gettimeofday(struct timeval *tv, struct timezone *tz) {
  time_t t;
  t = time(NULL);
  SYSTEMTIME st;
  GetLocalTime(&st);

  tv->tv_sec = (long)t;
  tv->tv_usec = st.wMilliseconds * 1000;

  return 0;
}

struct tm *localtime_r(const time_t *timep, struct tm *result) {
  localtime_s(result, timep);
  return result;
}