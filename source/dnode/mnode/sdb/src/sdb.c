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

SSdb *sdbOpen(SSdbOpt *pOption) {
  mDebug("start to open sdb in %s", pOption->path);

  SSdb *pSdb = calloc(1, sizeof(SSdb));
  if (pSdb == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("failed to open sdb since %s", terrstr());
    return NULL;
  }

  char path[PATH_MAX + 100];
  snprintf(path, PATH_MAX + 100, "%s%scur", pOption->path, TD_DIRSEP);
  pSdb->currDir = strdup(path);
  snprintf(path, PATH_MAX + 100, "%s%ssync", pOption->path, TD_DIRSEP);
  pSdb->syncDir = strdup(path);
  snprintf(path, PATH_MAX + 100, "%s%stmp", pOption->path, TD_DIRSEP);
  pSdb->tmpDir = strdup(path);
  if (pSdb->currDir == NULL || pSdb->currDir == NULL || pSdb->currDir == NULL) {
    sdbClose(pSdb);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("failed to open sdb since %s", terrstr());
    return NULL;
  }

  for (int32_t i = 0; i < SDB_MAX; ++i) {
    int32_t type;
    if (pSdb->keyTypes[i] == SDB_KEY_INT32) {
      type = TSDB_DATA_TYPE_INT;
    } else if (pSdb->keyTypes[i] == SDB_KEY_INT64) {
      type = TSDB_DATA_TYPE_BIGINT;
    } else {
      type = TSDB_DATA_TYPE_BINARY;
    }

    SHashObj *hash = taosHashInit(64, taosGetDefaultHashFunction(type), true, HASH_NO_LOCK);
    if (hash == NULL) {
      sdbClose(pSdb);
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      mError("failed to open sdb since %s", terrstr());
      return NULL;
    }

    pSdb->hashObjs[i] = hash;
    taosInitRWLatch(&pSdb->locks[i]);
  }

  int32_t code = sdbReadFile(pSdb);
  if (code != 0) {
    sdbClose(pSdb);
    terrno = code;
    mError("failed to open sdb since %s", terrstr());
    return NULL;
  }

  mDebug("sdb open successfully");
  return pSdb;
}

void sdbClose(SSdb *pSdb) {
  mDebug("start to close sdb");

  if (pSdb->curVer != pSdb->lastCommitVer) {
    mDebug("start to write sdb file since curVer:% " PRId64 " and lastCommitVer:%" PRId64 " inequal", pSdb->curVer,
           pSdb->lastCommitVer);
    sdbWriteFile(pSdb);
  }

  if (pSdb->currDir != NULL) {
    tfree(pSdb->currDir);
  }

  if (pSdb->syncDir != NULL) {
    tfree(pSdb->syncDir);
  }

  if (pSdb->tmpDir != NULL) {
    tfree(pSdb->tmpDir);
  }

  for (int32_t i = 0; i < SDB_MAX; ++i) {
    SHashObj *hash = pSdb->hashObjs[i];
    if (hash != NULL) {
      taosHashClear(hash);
      taosHashCleanup(hash);
    }
    pSdb->hashObjs[i] = NULL;
  }

  mDebug("sdb is closed");
}

void sdbSetTable(SSdb *pSdb, SSdbTable table) {
  ESdbType sdb = table.sdbType;
  pSdb->keyTypes[sdb] = table.keyType;
  pSdb->insertFps[sdb] = table.insertFp;
  pSdb->updateFps[sdb] = table.updateFp;
  pSdb->deleteFps[sdb] = table.deleteFp;
  pSdb->deployFps[sdb] = table.deployFp;
  pSdb->encodeFps[sdb] = table.encodeFp;
  pSdb->decodeFps[sdb] = table.decodeFp;

  mDebug("set sdb handle of table %d", pSdb, table);
}