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

#ifndef TDENGINE_OS_SIGNAL_H
#define TDENGINE_OS_SIGNAL_H

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"
#include "taosdef.h"
#include <signal.h>

#ifndef SIGALRM
  #define SIGALRM 1234
#endif

#ifndef SIGHUP
  #define SIGHUP 1230
#endif

#ifndef SIGCHLD
  #define SIGCHLD 1234
#endif

#ifndef SIGUSR1
  #define SIGUSR1 1234
#endif

#ifndef SIGUSR2
  #define SIGUSR2 1234
#endif

#ifndef SIGBREAK
  #define SIGBREAK 1234
#endif

typedef void (*FSignalHandler)(int32_t signum);
void taosSetSignal(int32_t signum, FSignalHandler sigfp);
void taosIgnSignal(int32_t signum);
void taosDflSignal(int32_t signum);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TTIME_H
