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
#include "tfs.h"
#include "tglobal.h"
#include "tlog.h"
#include "tutil.h"

static void taosAddDataDir(int32_t index, char *v1, int32_t level, int32_t primary, int8_t disable) {
  tstrncpy(tsDiskCfg[index].dir, v1, TSDB_FILENAME_LEN);
  tsDiskCfg[index].level = level;
  tsDiskCfg[index].primary = primary;
  tsDiskCfg[index].disable = disable;
  uInfo("dataDir:%s, level:%d primary:%d disable:%" PRIi8 " is configured", v1, level, primary, disable);
}

int32_t taosSetTfsCfg(SConfig *pCfg) {
  int32_t      code = 0;
  SConfigItem *pItem = cfgGetItem(pCfg, "dataDir");
  if (pItem == NULL) {
    TAOS_RETURN(terrno);  // TODO: remove this terrno if possible
  }
  (void)memset(tsDataDir, 0, PATH_MAX);

  int32_t size = taosArrayGetSize(pItem->array);
  if (size <= 0) {
    tsDiskCfgNum = 1;
    taosAddDataDir(0, pItem->str, 0, 1, 0);
    tstrncpy(tsDataDir, pItem->str, PATH_MAX);
    if ((code = taosMulMkDir(tsDataDir)) != 0) {
      uError("failed to create dataDir:%s", tsDataDir);
      TAOS_RETURN(code);
    }
  } else {
    tsDiskCfgNum = size < TFS_MAX_DISKS ? size : TFS_MAX_DISKS;
    for (int32_t index = 0; index < tsDiskCfgNum; ++index) {
      SDiskCfg *pCfg = TARRAY_GET_ELEM(pItem->array, index);
      memcpy(&tsDiskCfg[index], pCfg, sizeof(SDiskCfg));
      uInfo("dataDir:%s, level:%d primary:%d disable:%" PRIi8 " is configured", pCfg->dir, pCfg->level, pCfg->primary,
            pCfg->disable);
      if (pCfg->level == 0 && pCfg->primary == 1) {
        tstrncpy(tsDataDir, pCfg->dir, PATH_MAX);
      }
      if ((code = taosMulMkDir(pCfg->dir)) != 0) {
        uError("failed to create tfsDir:%s", pCfg->dir);
        TAOS_RETURN(code);
      }
    }
  }

  TAOS_RETURN(code);
}

int32_t cfgUpdateTfsItemDisable(SConfig *pCfg, const char *value, void *pTfs) {
  int32_t code = 0, lino = 0;
  int8_t  disable = 0;
  char   *dataDirStr = NULL;
  char    disableStr[2] = {0};
  cfgLock(pCfg);

  dataDirStr = taosMemoryMalloc(PATH_MAX);
  if (dataDirStr == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TAOS_CHECK_EXIT(code);
  }
  const char *p = value;
  while (*p) {
    if (*p == ' ') {
      break;
    }
    p++;
  }

  size_t optLen = p - value;
  tstrncpy(dataDirStr, value, PATH_MAX);
  dataDirStr[optLen] = 0;

  if (' ' == value[optLen] && strlen(value) > optLen + 1) {
    disableStr[0] = value[optLen + 1];
    disableStr[1] = 0;
    if ((taosStr2int8(disableStr, &disable)) < 0) {
      code = TSDB_CODE_INVALID_CFG_VALUE;
      TAOS_CHECK_EXIT(code);
    }
    if (disable < 0 || disable > 1) {
      code = TSDB_CODE_INVALID_CFG_VALUE;
      TAOS_CHECK_EXIT(code);
    }
  } else {
    code = TSDB_CODE_INVALID_CFG_VALUE;
    TAOS_CHECK_EXIT(code);
  }

  SConfigItem *pItem = cfgGetItem(pCfg, "dataDir");
  if (pItem == NULL) {
    code = TSDB_CODE_CFG_NOT_FOUND;
    TAOS_CHECK_EXIT(code);
  }

  int32_t sz = taosArrayGetSize(pItem->array);
  bool    dirFound = false;
  for (int32_t i = 0; i < sz; ++i) {
    SDiskCfg *cfg = taosArrayGet(pItem->array, i);
    if (strcmp(cfg->dir, dataDirStr) == 0) {
      uInfo("update tfs item:%s disable:%d", cfg->dir, cfg->disable);
      dirFound = true;
      cfg->disable = disable;
      break;
    }
  }

  if (!dirFound) {
    code = TSDB_CODE_INVALID_CFG_VALUE;
    TAOS_CHECK_EXIT(code);
  }

  bool update = false;
  for (int32_t i = 0; i < TFS_MAX_DISKS; i++) {
    if (strcmp(tsDiskCfg[i].dir, dataDirStr) == 0) {
      tsDiskCfg[i].disable = disable;
      update = true;
      break;
    }
  }

  if (!update) {
    code = TSDB_CODE_INVALID_CFG_VALUE;
    TAOS_CHECK_EXIT(code);
  }

  TAOS_CHECK_GOTO(tfsUpdateDiskDisable(pTfs, dataDirStr, disable), &lino, _exit);

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    uError("failed to update tfs item:%s disable:%d, reason:%s, at line:%d", dataDirStr, disable, tstrerror(code),
           lino);
  }
  cfgUnLock(pCfg);
  taosMemoryFree(dataDirStr);
  TAOS_RETURN(code);
}