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
#include <signal.h>
#include <windows.h>

typedef void (*FWinSignalHandler)(int32_t signum);

void taosSetSignal(int32_t signum, FSignalHandler sigfp) {
  if (signum == SIGUSR1) return;

  // SIGHUP doesn't exist in windows, we handle it in the way of ctrlhandler
  if (signum == SIGHUP) {
    SetConsoleCtrlHandler((PHANDLER_ROUTINE)sigfp, TRUE);
  } else {
    signal(signum, (FWinSignalHandler)sigfp);	
  }
}

void taosIgnSignal(int32_t signum) {
  if (signum == SIGUSR1 || signum == SIGHUP) return;
  signal(signum, SIG_IGN);
}

void taosDflSignal(int32_t signum) {
  if (signum == SIGUSR1 || signum == SIGHUP) return;
  signal(signum, SIG_DFL);
}
