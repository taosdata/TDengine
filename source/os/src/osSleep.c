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

#define ALLOW_FORBID_FUNC
#define _DEFAULT_SOURCE
#include "os.h"

#if !(defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32))
#include <unistd.h>
#endif

void taosSsleep(int32_t s) {
  if (s < 0) return;
#ifdef WINDOWS
  Sleep(1000 * s);
#else
  sleep(s);
#endif
}

void taosMsleep(int32_t ms) {
  if (ms < 0) return;
#ifdef WINDOWS
  Sleep(ms);
#else
  usleep(ms * 1000);
#endif
}

void taosUsleep(int32_t us) {
  if (us < 0) return;
#ifdef WINDOWS
  HANDLE        timer;
  LARGE_INTEGER interval;
  interval.QuadPart = (10 * us);

  timer = CreateWaitableTimer(NULL, TRUE, NULL);
  SetWaitableTimer(timer, &interval, 0, NULL, NULL, 0);
  WaitForSingleObject(timer, INFINITE);
  CloseHandle(timer);
#else
  usleep(us);
#endif
}
