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

int32_t taosCreateShm(SShm* pShm, int32_t shmsize) {
  pShm->id = -1;

  int32_t shmid = shmget(0X95279527, shmsize, IPC_CREAT | 0600);
  if (shmid < 0) {
    return -1;
  }

  void* shmptr = shmat(shmid, NULL, 0);
  if (shmptr == NULL) {
    return -1;
  }

  pShm->id = shmid;
  pShm->size = shmsize;
  pShm->ptr = shmptr;
  return 0;
}

void taosDropShm(SShm* pShm) {
  if (pShm->id >= 0) {
    if (pShm->ptr != NULL) {
      shmdt(pShm->ptr);
    }
    shmctl(pShm->id, IPC_RMID, NULL);
  }
  pShm->id = -1;
  pShm->size = 0;
  pShm->ptr = NULL;
}

int32_t taosAttachShm(SShm* pShm) {
  if (pShm->id >= 0) {
    pShm->ptr = shmat(pShm->id, NULL, 0);
    if (pShm->ptr != NULL) {
      return 0;
    }
  }

  return -1;
}
