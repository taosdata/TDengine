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

#ifndef _TD_OS_SIGNAL_H_
#define _TD_OS_SIGNAL_H_

#ifdef __cplusplus
extern "C" {
#endif

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

#ifdef WINDOWS
typedef BOOL (*FSignalHandler)(DWORD fdwCtrlType);
#else
typedef void (*FSignalHandler)(int32_t signum, void *sigInfo, void *context);
#endif
void taosSetSignal(int32_t signum, FSignalHandler sigfp);
void taosIgnSignal(int32_t signum);
void taosDflSignal(int32_t signum);

void taosKillChildOnParentStopped();

#ifdef __cplusplus
}
#endif

#endif /*_TD_OS_SIGNAL_H_*/
