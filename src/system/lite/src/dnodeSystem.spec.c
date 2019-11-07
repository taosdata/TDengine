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
#include "mgmt.h"
#include "dnodeSystem.h"

extern SModule tsModule[TSDB_MOD_MAX];

int taosCreateTierDirectory() {
  struct stat dirstat;
  strcpy(tsDirectory, dataDir);
  if (stat(dataDir, &dirstat) < 0) {
    mkdir(dataDir, 0755);
  }

  char fileName[128];

  sprintf(fileName, "%s/tsdb", tsDirectory);
  mkdir(fileName, 0755);

  sprintf(fileName, "%s/data", tsDirectory);
  mkdir(fileName, 0755);

  sprintf(mgmtDirectory, "%s/mgmt", tsDirectory);
  sprintf(tsDirectory, "%s/tsdb", dataDir);
  dnodeCheckDbRunning(dataDir);

  return 0;
}

int dnodeInitSystemSpec() { return 0; }

void dnodeStartModuleSpec() {
  for (int mod = 1; mod < TSDB_MOD_MAX; ++mod) {
    if (tsModule[mod].num != 0 && tsModule[mod].startFp) {
      if ((*tsModule[mod].startFp)() != 0) {
        dError("failed to start module:%d", mod);
      }
    }
  }
}

void dnodeParseParameterK() {}