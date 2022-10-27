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
#include "tthread.h"

TdThread* taosCreateThread(void* (*__start_routine)(void*), void* param) {
  TdThread*    pthread = (TdThread*)taosMemoryMalloc(sizeof(TdThread));
  TdThreadAttr thattr;
  taosThreadAttrInit(&thattr);
  taosThreadAttrSetDetachState(&thattr, PTHREAD_CREATE_JOINABLE);
  int32_t ret = taosThreadCreate(pthread, &thattr, __start_routine, param);
  taosThreadAttrDestroy(&thattr);

  if (ret != 0) {
    taosMemoryFree(pthread);
    return NULL;
  }
  return pthread;
}

bool taosDestroyThread(TdThread* pthread) {
  if (pthread == NULL) return false;
  if (taosThreadRunning(pthread)) {
    taosThreadCancel(*pthread);
    taosThreadJoin(*pthread, NULL);
  }

  taosMemoryFree(pthread);
  return true;
}

bool taosThreadRunning(TdThread* pthread) {
  if (pthread == NULL) return false;
  int32_t ret = taosThreadKill(*pthread, 0);
  if (ret == ESRCH) return false;
  if (ret == EINVAL) return false;
  // alive
  return true;
}
