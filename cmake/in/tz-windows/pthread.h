// Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This program is free software: you can use, redistribute, and/or modify
// it under the terms of the GNU Affero General Public License, version 3
// or later ("AGPL"), as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
// FITNESS FOR A PARTICULAR PURPOSE.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

#ifndef TD_TZ_WIN_LOCK_H
#define TD_TZ_WIN_LOCK_H

#include <windows.h>
typedef INIT_ONCE pthread_once_t;
#define PTHREAD_ONCE_INIT INIT_ONCE_STATIC_INIT

static BOOL CALLBACK taosTzInitializeOnce(PINIT_ONCE once, PVOID parameter,
                                          PVOID *context) {
  (void)once;
  (void)context;
  void (**initialize)(void) = parameter;
  (*initialize)();
  return TRUE;
}

static int pthread_once(pthread_once_t *once, void (*initialize)(void)) {
  // InitOnceExecuteOnce consumes this callback pointer synchronously.
  return InitOnceExecuteOnce(once, taosTzInitializeOnce, &initialize, NULL) ?
         0 : (int)GetLastError();
}

typedef SRWLOCK pthread_mutex_t;
#define PTHREAD_MUTEX_INITIALIZER SRWLOCK_INIT
static int pthread_mutex_lock(pthread_mutex_t *lock) {
  AcquireSRWLockExclusive(lock);
  return 0;
}
static int pthread_mutex_unlock(pthread_mutex_t *lock) {
  ReleaseSRWLockExclusive(lock);
  return 0;
}

#endif
