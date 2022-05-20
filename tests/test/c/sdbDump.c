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
#include "dmMgmt.h"
#include "tconfig.h"

int32_t main(int32_t argc, char *argv[]) {
  char datafile[PATH_MAX] = {0};

  for (int32_t i = 1; i < argc; ++i) {
    if (strcmp(argv[i], "-c") == 0) {
      if (i < argc - 1) {
        if (strlen(argv[++i]) >= PATH_MAX) {
          printf("config file path overflow");
          return -1;
        }
        tstrncpy(configDir, argv[i], PATH_MAX);
      } else {
        printf("'-c' requires a parameter, default is %s\n", configDir);
        return -1;
      }
    } else if (strcmp(argv[i], "-f") == 0) {
      if (i < argc - 1) {
        if (strlen(argv[++i]) >= PATH_MAX) {
          printf("file path overflow");
          return -1;
        }
        tstrncpy(datafile, argv[i], PATH_MAX);
      } else {
        printf("'-f' requires a parameter, default is %s\n", configDir);
        return -1;
      }
    }
  }

  if (taosCreateLog("dumplog", 1, configDir, NULL, NULL, NULL, NULL, 1) != 0) {
    printf("failed to dump since init log error\n");
    return -1;
  }

  if (taosInitCfg(configDir, NULL, NULL, NULL, NULL, 0) != 0) {
    uError("failed to dump since read config error");
    taosCloseLog();
    return -1;
  }

  if (datafile[0] == 0) {
    snprintf(datafile, sizeof(datafile), "%s%sdata%smnode%sdata%ssdb.data", tsDataDir, TD_DIRSEP, TD_DIRSEP, TD_DIRSEP,
             TD_DIRSEP);
  }

  dInfo("dump %s to sdb.json", datafile);

  taosCleanupCfg();
  taosCloseLog();
  return 0;
}
