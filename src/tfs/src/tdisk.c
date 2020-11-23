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
#include "os.h"

#include "taoserror.h"
#include "tfsint.h"

typedef struct {
  uint64_t size;
  uint64_t free;
  uint64_t nfiles;
} SDiskMeta;

struct SDisk {
  int       level;
  int       id;
  char      dir[TSDB_FILENAME_LEN];
  SDiskMeta dmeta;
};

// PROTECTED ====================================
SDisk *tfsNewDisk(int level, int id, char *dir) {
  SDisk *pDisk = (SDisk *)calloc(1, sizeof(*pDisk));
  if (pDisk == NULL) {
    terrno = TSDB_CODE_FS_OUT_OF_MEMORY;
    return NULL;
  }

  pDisk->level = level;
  pDisk->id = id;
  strncpy(pDisk->dir, dir, TSDB_FILENAME_LEN);

  return pDisk;
}

void tfsFreeDisk(SDisk *pDisk) {
  if (pDisk) {
    free(pDisk)
  }
}

int tfsUpdateDiskInfo(SDisk *pDisk) {
  SysDiskSize dstat;
  if (taosGetDiskSize(pDisk->dir, &dstat) < 0) {
    fError("failed to get dir %s information since %s", pDisk->dir, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  pDisk->dmeta.size = dstat.tsize;
  pDisk->dmeta.free = dstat.avail;

  return 0;
}