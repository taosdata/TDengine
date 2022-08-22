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
#include "tlog.h"
#include "tglobal.h"

void taosAddDataDir(int32_t index, char *v1, int32_t level, int32_t primary) {
  tstrncpy(tsDiskCfg[index].dir, v1, TSDB_FILENAME_LEN);
  tsDiskCfg[index].level = level;
  tsDiskCfg[index].primary = primary;
  uInfo("dataDir:%s, level:%d primary:%d is configured", v1, level, primary);
}

int32_t taosSetTfsCfg(SConfig *pCfg) {
  SConfigItem *pItem = cfgGetItem(pCfg, "dataDir");
  memset(tsDataDir, 0, PATH_MAX);

  int32_t size = taosArrayGetSize(pItem->array);
  if (size <= 0) {
    tsDiskCfgNum = 1;
    taosAddDataDir(0, pItem->str, 0, 1);
    tstrncpy(tsDataDir, pItem->str, PATH_MAX);
    if (taosMulMkDir(tsDataDir) != 0) {
      uError("failed to create dataDir:%s", tsDataDir);
      return -1;
    }
  } else {
    tsDiskCfgNum = size < TFS_MAX_DISKS ? size : TFS_MAX_DISKS;
    for (int32_t index = 0; index < tsDiskCfgNum; ++index) {
      SDiskCfg *pCfg = taosArrayGet(pItem->array, index);
      memcpy(&tsDiskCfg[index], pCfg, sizeof(SDiskCfg));
      uInfo("dataDir:%s, level:%d primary:%d is configured", pCfg->dir, pCfg->level, pCfg->primary);
      if (pCfg->level == 0 && pCfg->primary == 1) {
        tstrncpy(tsDataDir, pCfg->dir, PATH_MAX);
      }
      if (taosMulMkDir(pCfg->dir) != 0) {
        uError("failed to create tfsDir:%s", tsDataDir);
        return -1;
      }
    }
  }

  if (tsDataDir[0] == 0) {
    if (pItem->str != NULL) {
      taosAddDataDir(tsDiskCfgNum, pItem->str, 0, 1);
      tstrncpy(tsDataDir, pItem->str, PATH_MAX);
      if (taosMulMkDir(tsDataDir) != 0) {
        uError("failed to create tfsDir:%s", tsDataDir);
        return -1;
      }
      tsDiskCfgNum++;
    } else {
      uError("datadir not set");
      return -1;
    }
  }

  return 0;
}