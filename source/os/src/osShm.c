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

#define MAX_SHMIDS 6

static int32_t shmids[MAX_SHMIDS] = {0};

static void taosDeleteCreatedShms() {
#if defined(WINDOWS)
#else
  for (int32_t i = 0; i < MAX_SHMIDS; ++i) {
    int32_t shmid = shmids[i] - 1;
    if (shmid >= 0) {
      shmctl(shmid, IPC_RMID, NULL);
    }
  }
#endif
}

int32_t taosCreateShm(SShm* pShm, int32_t key, int32_t shmsize) {
#if defined(WINDOWS)
#else
  pShm->id = -1;

#if 1
  key_t   __shkey = IPC_PRIVATE;
  int32_t __shmflag = IPC_CREAT | IPC_EXCL | 0600;
#else
  key_t   __shkey = 0X95270000 + key;
  int32_t __shmflag = IPC_CREAT | 0600;
#endif

  int32_t shmid = shmget(__shkey, shmsize, __shmflag);
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

#if 0
  if (key >= 0 && key < MAX_SHMIDS) {
    shmids[key] = pShm->id + 1;
  }
  atexit(taosDeleteCreatedShms);
#else
  shmctl(pShm->id, IPC_RMID, NULL);
#endif

#endif
  return 0;
}

void taosDropShm(SShm* pShm) {
#if defined(WINDOWS)
#else
  if (pShm->id >= 0) {
    if (pShm->ptr != NULL) {
      shmdt(pShm->ptr);
    }
    shmctl(pShm->id, IPC_RMID, NULL);
  }
  pShm->id = -1;
  pShm->size = 0;
  pShm->ptr = NULL;
#endif
}

int32_t taosAttachShm(SShm* pShm) {
#if defined(WINDOWS)
#else
  errno = 0;

  void* ptr = shmat(pShm->id, NULL, 0);
  if (errno == 0) {
    pShm->ptr = ptr;
  }
#endif
  return errno;
}
