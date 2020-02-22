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

#include <locale.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "os.h"
#include "tglobalcfg.h"
#include "tkey.h"
#include "tlog.h"
#include "taosdef.h"
#include "tsocket.h"
#include "tsystem.h"
#include "ttier.h"
#include "tutil.h"

/*
 * Cluster or Enterprise version need to read multi-level storage configuration information
 *
 */
bool tsReadGlobalConfigSpec() {
#ifdef LINUX
  char   path[TSDB_FILENAME_LEN] = "\0";
  TIERID tid;

  if (taosInitTier() < 0) {
    return false;
  }

  FILE * fp;
  char * line, *option, *value, *value1;
  size_t len;
  int    olen, vlen, vlen1;
  char   fileName[128];

  sprintf(fileName, "%s/taos.cfg", configDir);
  fp = fopen(fileName, "r");
  if (fp == NULL) {
  } else {
    line = NULL;
    while (!feof(fp)) {
      tfree(line);
      line = option = value = NULL;
      len = olen = vlen = 0;

      getline(&line, &len, fp);
      if (line == NULL) break;

      paGetToken(line, &option, &olen);
      if (olen == 0) continue;
      option[olen] = 0;

      paGetToken(option + olen + 1, &value, &vlen);
      if (vlen == 0) continue;
      value[vlen] = 0;

      // For dataDir, the format is:
      // dataDir    /mnt/disk1    0
      paGetToken(value + vlen + 1, &value1, &vlen1);
      if (strncasecmp(option, "dataDir", 7) == 0) {
        if (!tscEmbedded) continue;
        if (vlen1 == 0)
          tid = -1;
        else
          tid = (TIERID)atoi(value1);

        memset(path, 0, TSDB_FILENAME_LEN);
        memcpy(path, value, vlen);
        path[vlen] = '\0';

        tsExpandFilePath("dataDir", path);
        taosAddMountPoint(path, tid);
        strcpy(dataDir, path);
      }
    }

    tfree(line);
    fclose(fp);
  }

  if (tscEmbedded)
    if (!taosValidTierInfo()) return false;

  return true;
#else
  return true;
#endif
}

/*
 * Cluster or Enterprise version need to print multi-level storage configuration information
 */
void tsPrintGlobalConfigSpec() {
#ifdef LINUX
  if (tscEmbedded) {
    taosPrintTierInfo();
  }
#endif
}