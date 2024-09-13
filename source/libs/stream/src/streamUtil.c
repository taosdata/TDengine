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

#include "streamInt.h"

void streamMutexLock(TdThreadMutex *pMutex) {
  int32_t code = taosThreadMutexLock(pMutex);
  if (code) {
    stError("%p mutex lock failed, code:%s", pMutex, tstrerror(code));
  }
}

void streamMutexUnlock(TdThreadMutex *pMutex) {
  int32_t code = taosThreadMutexUnlock(pMutex);
  if (code) {
    stError("%p mutex unlock failed, code:%s", pMutex, tstrerror(code));
  }
}

void streamMutexDestroy(TdThreadMutex *pMutex) {
  int32_t code = taosThreadMutexDestroy(pMutex);
  if (code) {
    stError("%p mutex destroy, code:%s", pMutex, tstrerror(code));
  }
}
