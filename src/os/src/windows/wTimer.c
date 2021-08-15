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

#include <Windows.h>
#include <Mmsystem.h>
#include <stdio.h>
#include <stdint.h>

#pragma warning( disable : 4244 )

typedef void (*win_timer_f)(int signo);

void WINAPI taosWinOnTimer(UINT wTimerID, UINT msg, DWORD_PTR dwUser, DWORD_PTR dwl, DWORD_PTR dw2) {
  win_timer_f callback = *((win_timer_f *)&dwUser);
  if (callback != NULL) {
    callback(0);
  }
}

static MMRESULT timerId;
int taosInitTimer(win_timer_f callback, int ms) {
  DWORD_PTR param = *((int64_t *) & callback);

  timerId = timeSetEvent(ms, 1, (LPTIMECALLBACK)taosWinOnTimer, param, TIME_PERIODIC);
  if (timerId == 0) {
    return -1;
  }
  return 0;
}

void taosUninitTimer() {
  timeKillEvent(timerId);
}
