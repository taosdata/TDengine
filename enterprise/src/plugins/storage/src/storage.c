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
#include "tulog.h"
#include "tglobal.h"

void taosReadDataDirCfg(char *v1, char *v2, char *v3) {
  int level = 0;
  if (v2 != NULL) {
    int length = strlen(v2);
    if (length > 0) level = atoi(v2);
    if (level < 0 || level >= TSDB_MAX_TIERS) {
      uError("config option:dataDir, input level:%s, not in range [0, %d), set default 0", v2, TSDB_MAX_TIERS);
      level = 0;
    }
  }

  int primary = 1;
  if (v3 != NULL) {
    int length = strlen(v3);
    if (length > 0) primary = atoi(v3);
    if (primary < 0 || primary > 1) {
      uError("config option:dataDir, input primary:%s, not in range [0, 1], set default 1", v3);
      primary = 1;
    }
  }

  if (tsDiskCfgNum >= TSDB_MAX_DISKS) return;
  taosAddDataDir(tsDiskCfgNum, v1, level, primary);
  tsDiskCfgNum++;
}

void taosPrintDataDirCfg() {
  for (int i = 0; i < tsDiskCfgNum; ++i) {
    SDiskCfg *cfg = &tsDiskCfg[i];
    uInfo(" dataDir: %s level:%d primary:%d", cfg->dir, cfg->level, cfg->primary);
  }
}
