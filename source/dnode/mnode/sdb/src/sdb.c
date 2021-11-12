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
#include "sdbInt.h"
#include "tglobal.h"

SSdbMgr tsSdb = {0};

int32_t sdbInit() {
  char path[PATH_MAX + 100];

  snprintf(path, PATH_MAX + 100, "%s%scur%s", tsMnodeDir, TD_DIRSEP, TD_DIRSEP);
  tsSdb.currDir = strdup(path);

  snprintf(path, PATH_MAX + 100, "%s%ssync%s", tsMnodeDir, TD_DIRSEP, TD_DIRSEP);
  tsSdb.syncDir = strdup(path);

  snprintf(path, PATH_MAX + 100, "%s%stmp%s", tsMnodeDir, TD_DIRSEP, TD_DIRSEP);
  tsSdb.tmpDir = strdup(path);

  if (tsSdb.currDir == NULL || tsSdb.currDir == NULL || tsSdb.currDir == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  for (int32_t i = 0; i < SDB_MAX; ++i) {
    int32_t type;
    if (tsSdb.keyTypes[i] == SDB_KEY_INT32) {
      type = TSDB_DATA_TYPE_INT;
    } else if (tsSdb.keyTypes[i] == SDB_KEY_INT64) {
      type = TSDB_DATA_TYPE_BIGINT;
    } else {
      type = TSDB_DATA_TYPE_BINARY;
    }

    SHashObj *hash = taosHashInit(64, taosGetDefaultHashFunction(type), true, HASH_NO_LOCK);
    if (hash == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    tsSdb.hashObjs[i] = hash;
    taosInitRWLatch(&tsSdb.locks[i]);
  }

  return 0;
}

void sdbCleanup() {
  if (tsSdb.curVer != tsSdb.lastCommitVer) {
    sdbCommit();
  }

  if (tsSdb.currDir != NULL) {
    tfree(tsSdb.currDir);
  }

  if (tsSdb.syncDir != NULL) {
    tfree(tsSdb.syncDir);
  }

  if (tsSdb.tmpDir != NULL) {
    tfree(tsSdb.tmpDir);
  }

  for (int32_t i = 0; i < SDB_MAX; ++i) {
    SHashObj *hash = tsSdb.hashObjs[i];
    if (hash != NULL) {
      taosHashCleanup(hash);
    }
    tsSdb.hashObjs[i] = NULL;
  }
}

void sdbSetTable(SSdbTable table) {
  ESdbType sdb = table.sdbType;
  tsSdb.keyTypes[sdb] = table.keyType;
  tsSdb.insertFps[sdb] = table.insertFp;
  tsSdb.updateFps[sdb] = table.updateFp;
  tsSdb.deleteFps[sdb] = table.deleteFp;
  tsSdb.deployFps[sdb] = table.deployFp;
  tsSdb.encodeFps[sdb] = table.encodeFp;
  tsSdb.decodeFps[sdb] = table.decodeFp;
}