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
#include "tconfig.h"
#include "tglobal.h"
#include "tulog.h"

#ifndef TAOS_OS_FUNC_SIGNAL
typedef void (*FLinuxSignalHandler)(int32_t signum, siginfo_t *sigInfo, void *context);

void taosSetSignal(int32_t signum, FSignalHandler sigfp) {
  struct sigaction act = {{0}};	
#if 1
  act.sa_flags = SA_SIGINFO;
  act.sa_sigaction = (FLinuxSignalHandler)sigfp;
#else  
  act.sa_handler = sigfp;	
#endif  
  sigaction(signum, &act, NULL);		
}

void taosIgnSignal(int32_t signum) {
  signal(signum, SIG_IGN);
}

void taosDflSignal(int32_t signum) {
  signal(signum, SIG_DFL);
}

#endif
