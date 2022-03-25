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

static int32_t sdbCreateDir(SSdb *pSdb);

SSdb *sdbInit(SSdbOpt *pOption) {
  mDebug("start to init sdb in %s", pOption->path);

  SSdb *pSdb = taosMemoryCalloc(1, sizeof(SSdb));
  if (pSdb == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("failed to init sdb since %s", terrstr());
    return NULL;
  }

  char path[PATH_MAX + 100];
  snprintf(path, PATH_MAX + 100, "%s%sdata", pOption->path, TD_DIRSEP);
  pSdb->currDir = strdup(path);
  snprintf(path, PATH_MAX + 100, "%s%ssync", pOption->path, TD_DIRSEP);
  pSdb->syncDir = strdup(path);
  snprintf(path, PATH_MAX + 100, "%s%stmp", pOption->path, TD_DIRSEP);
  pSdb->tmpDir = strdup(path);
  if (pSdb->currDir == NULL || pSdb->currDir == NULL || pSdb->currDir == NULL) {
    sdbCleanup(pSdb);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("failed to init sdb since %s", terrstr());
    return NULL;
  }

  if (sdbCreateDir(pSdb) != 0) {
    sdbCleanup(pSdb);
    return NULL;
  }

  for (ESdbType i = 0; i < SDB_MAX; ++i) {
    taosInitRWLatch(&pSdb->locks[i]);
    pSdb->maxId[i] = 0;
    pSdb->tableVer[i] = -1;
    pSdb->keyTypes[i] = SDB_KEY_INT32;
  }

  pSdb->curVer = -1;
  pSdb->lastCommitVer = -1;
  pSdb->pMnode = pOption->pMnode;
  mDebug("sdb init successfully");
  return pSdb;
}

void sdbCleanup(SSdb *pSdb) {
  mDebug("start to cleanup sdb");

  sdbWriteFile(pSdb);

  if (pSdb->currDir != NULL) {
    taosMemoryFreeClear(pSdb->currDir);
  }

  if (pSdb->syncDir != NULL) {
    taosMemoryFreeClear(pSdb->syncDir);
  }

  if (pSdb->tmpDir != NULL) {
    taosMemoryFreeClear(pSdb->tmpDir);
  }

  for (ESdbType i = 0; i < SDB_MAX; ++i) {
    SHashObj *hash = pSdb->hashObjs[i];
    if (hash == NULL) continue;

    SSdbRow **ppRow = taosHashIterate(hash, NULL);
    while (ppRow != NULL) {
      SSdbRow *pRow = *ppRow;
      if (pRow == NULL) continue;

      sdbFreeRow(pSdb, pRow);
      ppRow = taosHashIterate(hash, ppRow);
    }
  }

  for (ESdbType i = 0; i < SDB_MAX; ++i) {
    SHashObj *hash = pSdb->hashObjs[i];
    if (hash == NULL) continue;

    taosHashClear(hash);
    taosHashCleanup(hash);
    pSdb->hashObjs[i] = NULL;
    mDebug("sdb table:%d is cleaned up", i);
  }

  taosMemoryFree(pSdb);
  mDebug("sdb is cleaned up");
}

int32_t sdbSetTable(SSdb *pSdb, SSdbTable table) {
  ESdbType sdbType = table.sdbType;
  EKeyType keyType = table.keyType;
  pSdb->keyTypes[sdbType] = table.keyType;
  pSdb->insertFps[sdbType] = table.insertFp;
  pSdb->updateFps[sdbType] = table.updateFp;
  pSdb->deleteFps[sdbType] = table.deleteFp;
  pSdb->deployFps[sdbType] = table.deployFp;
  pSdb->encodeFps[sdbType] = table.encodeFp;
  pSdb->decodeFps[sdbType] = table.decodeFp;

  int32_t hashType = 0;
  if (keyType == SDB_KEY_INT32) {
    hashType = TSDB_DATA_TYPE_INT;
  } else if (keyType == SDB_KEY_INT64) {
    hashType = TSDB_DATA_TYPE_BIGINT;
  } else {
    hashType = TSDB_DATA_TYPE_BINARY;
  }

  SHashObj *hash = taosHashInit(64, taosGetDefaultHashFunction(hashType), true, HASH_NO_LOCK);
  if (hash == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pSdb->maxId[sdbType] = 0;
  pSdb->hashObjs[sdbType] = hash;
  taosInitRWLatch(&pSdb->locks[sdbType]);
  mDebug("sdb table:%s is initialized", sdbTableName(sdbType));

  return 0;
}

static int32_t sdbCreateDir(SSdb *pSdb) {
  if (taosMkDir(pSdb->currDir) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    mError("failed to create dir:%s since %s", pSdb->currDir, terrstr());
    return -1;
  }

  if (taosMkDir(pSdb->syncDir) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    mError("failed to create dir:%s since %s", pSdb->syncDir, terrstr());
    return -1;
  }

  if (taosMkDir(pSdb->tmpDir) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    mError("failed to create dir:%s since %s", pSdb->tmpDir, terrstr());
    return -1;
  }

  return 0;
}

int64_t sdbUpdateVer(SSdb *pSdb, int32_t val) {
  pSdb->curVer += val;
  return pSdb->curVer;
}