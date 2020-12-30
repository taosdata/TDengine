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
#include "taosdef.h"
#include "tglobal.h"
#include "ttimer.h"
#include "tulog.h"
#include "tutil.h"

bool taosCheckPthreadValid(pthread_t thread) { return thread.p != NULL; }

void taosResetPthread(pthread_t *thread) { thread->p = 0; }

int64_t taosGetPthreadId(pthread_t thread) {
#ifdef PTW32_VERSION
  return pthread_getw32threadid_np(thread);
#else
  return (int64_t)thread;
#endif
}

int64_t taosGetSelfPthreadId() { return taosGetPthreadId(pthread_self()); }

bool taosComparePthread(pthread_t first, pthread_t second) {
  return first.p == second.p;
}

int32_t taosGetPId() {
  return GetCurrentProcessId();
}

int32_t taosGetCurrentAPPName(char *name, int32_t* len) {
  char filepath[1024] = {0};

  GetModuleFileName(NULL, filepath, MAX_PATH);
  *strrchr(filepath,'.') = '\0';
  strcpy(name, filepath);

  if (len != NULL) {
    *len = (int32_t) strlen(filepath);
  }

  return 0;
}