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

int32_t tfsNewDisk(int32_t level, int32_t id, int8_t disable, const char *path, STfsDisk **ppDisk) {
  int32_t   code = 0;
  int32_t   lino = 0;
  STfsDisk *pDisk = NULL;

  if ((pDisk = taosMemoryCalloc(1, sizeof(STfsDisk))) == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _exit);
  }

  if ((pDisk->path = taosStrdup(path)) == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, &lino, _exit);
  }

  pDisk->level = level;
  pDisk->id = id;
  pDisk->disable = disable;
  if (taosGetDiskSize(pDisk->path, &pDisk->size) < 0) {
    code = terrno;
    TAOS_CHECK_GOTO(code, &lino, _exit);
  }
_exit:
  if (code != 0) {
    pDisk = tfsFreeDisk(pDisk);
    fError("%s failed at line %d since %s, disk:%s level:%d id:%d ", __func__, lino, tstrerror(code), path, level, id);
  }
  *ppDisk = pDisk;

  TAOS_RETURN(code);
}

STfsDisk *tfsFreeDisk(STfsDisk *pDisk) {
  if (pDisk != NULL) {
    taosMemoryFree(pDisk->path);
    taosMemoryFree(pDisk);
  }

  return NULL;
}

int32_t tfsUpdateDiskSize(STfsDisk *pDisk) {
  if (taosGetDiskSize(pDisk->path, &pDisk->size) < 0) {
    int32_t code = terrno;
    fError("failed to get disk:%s size, level:%d id:%d since %s", pDisk->path, pDisk->level, pDisk->id,
           tstrerror(code));
    TAOS_RETURN(code);
  }

  return 0;
}
