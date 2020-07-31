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

int taosInitTimer(void (*callback)(int), int ms) {
  signal(SIGALRM, callback);

  struct itimerval tv;
  tv.it_interval.tv_sec = 0;  /* my timer resolution */
  tv.it_interval.tv_usec = 1000 * ms;  // resolution is in msecond
  tv.it_value = tv.it_interval;

  setitimer(ITIMER_REAL, &tv, NULL);

  return 0;
}

void taosUninitTimer() {
  struct itimerval tv = { 0 };
  setitimer(ITIMER_REAL, &tv, NULL);
}

