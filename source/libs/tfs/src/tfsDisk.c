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
#include "tfsInt.h"

SDisk *tfsNewDisk(int32_t level, int32_t id, const char *path) {
  SDisk *pDisk = calloc(1, sizeof(SDisk));
  if (pDisk == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pDisk->path = strdup(path);
  if (pDisk->path == NULL) {
    free(pDisk);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pDisk->level = level;
  pDisk->id = id;
  taosGetDiskSize(pDisk->path, &pDisk->size);
  return pDisk;
}

SDisk *tfsFreeDisk(SDisk *pDisk) {
  if (pDisk != NULL) {
    free(pDisk->path);
    free(pDisk);
  }

  return NULL;
}

int32_t tfsUpdateDiskSize(SDisk *pDisk) {
  if (taosGetDiskSize(pDisk->path, &pDisk->size) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    fError("failed to get disk:%s size, level:%d id:%d since %s", pDisk->path, pDisk->level, pDisk->id, terrstr());
    return -1;
  }

  return 0;
}
