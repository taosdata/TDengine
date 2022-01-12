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
#include "tfsint.h"

SDisk *tfsNewDisk(int32_t level, int32_t id, const char *dir) {
  SDisk *pDisk = calloc(1, sizeof(SDisk));
  if (pDisk == NULL) {
    terrno = TSDB_CODE_FS_OUT_OF_MEMORY;
    return NULL;
  }

  pDisk->level = level;
  pDisk->id = id;
  tstrncpy(pDisk->dir, dir, TSDB_FILENAME_LEN);

  return pDisk;
}

SDisk *tfsFreeDisk(SDisk *pDisk) {
  if (pDisk != NULL) {
    free(pDisk);
  }
  return NULL;
}

int32_t tfsUpdateDiskInfo(SDisk *pDisk) {
  if (pDisk == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return -1;
  }

  SysDiskSize diskSize = {0};

  int32_t code = taosGetDiskSize(pDisk->dir, &diskSize);
  if (code != 0) {
    fError("failed to update disk information at level %d id %d dir %s since %s", pDisk->level, pDisk->id, pDisk->dir,
           strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
  }

  pDisk->dmeta.size = diskSize.tsize;
  pDisk->dmeta.used = diskSize.used;
  pDisk->dmeta.free = diskSize.avail;

  return code;
}
