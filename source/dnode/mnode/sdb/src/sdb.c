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

static SSdbObj tsSdb = {0};

static int32_t sdbCreateDir() {
  if (!taosMkDir(tsSdb.currDir)) {
    mError("failed to create dir:%s", tsSdb.currDir);
    return TAOS_SYSTEM_ERROR(errno);
  }

  if (!taosMkDir(tsSdb.syncDir)) {
    mError("failed to create dir:%s", tsSdb.syncDir);
    return -1;
  }

  if (!taosMkDir(tsSdb.tmpDir)) {
    mError("failed to create dir:%s", tsSdb.tmpDir);
    return -1;
  }

  return 0;
}

static int32_t sdbRunDeployFp() {
  for (int32_t i = SDB_START; i < SDB_MAX; ++i) {
    SdbDeployFp fp = tsSdb.deployFps[i];
    if (fp) {
      (*fp)();
    }
  }

  return 0;
}

static SHashObj *sdbGetHash(int32_t sdb) {
  if (sdb >= SDB_MAX || sdb <= SDB_START) {
    return NULL;
  }

  SHashObj *hash = tsSdb.hashObjs[sdb];
  if (hash == NULL) {
    return NULL;
  }

  return hash;
}

int32_t sdbWrite(SSdbRaw *pRaw) {
  SHashObj *hash = sdbGetHash(pRaw->type);
  switch (pRaw->action) {
    case SDB_ACTION_INSERT:
      break;
    case SDB_ACTION_UPDATE:
      break;
    case SDB_ACTION_DELETE:
      break;

    default:
      break;
  }

  return 0;
}

static int32_t sdbWriteVersion(FileFd fd) { return 0; }

static int32_t sdbReadVersion(FileFd fd) { return 0; }

static int32_t sdbReadDataFile() {
  int32_t code = 0;

  SSdbRaw *pRaw = malloc(SDB_MAX_SIZE);
  if (pRaw == NULL) {
    return TSDB_CODE_MND_OUT_OF_MEMORY;
  }

  char file[PATH_MAX] = {0};
  snprintf(file, sizeof(file), "%ssdb.data", tsSdb.currDir);
  FileFd fd = taosOpenFileCreateWrite(file);
  if (fd <= 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    mError("failed to open file:%s for read since %s", file, tstrerror(code));
    return code;
  }

  int64_t offset = 0;
  while (1) {
    int32_t ret = (int32_t)taosReadFile(fd, pRaw, sizeof(SSdbRaw));
    if (ret == 0) break;

    if (ret < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      mError("failed to read file:%s since %s", file, tstrerror(code));
      break;
    }

    if (ret < sizeof(SSdbRaw)) {
      code = TSDB_CODE_SDB_INTERNAL_ERROR;
      mError("failed to read file:%s since %s", file, tstrerror(code));
      break;
    }

    code = sdbWrite(pRaw);
    if (code != 0) {
      mError("failed to read file:%s since %s", file, tstrerror(code));
      goto PARSE_SDB_DATA_ERROR;
    }
  }

  code = 0;

PARSE_SDB_DATA_ERROR:
  taosCloseFile(fd);
  return code;
}

static int32_t sdbWriteDataFile() {
  int32_t code = 0;

  char tmpfile[PATH_MAX] = {0};
  snprintf(tmpfile, sizeof(tmpfile), "%ssdb.data", tsSdb.tmpDir);

  FileFd fd = taosOpenFileCreateWrite(tmpfile);
  if (fd <= 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    mError("failed to open file:%s for write since %s", tmpfile, tstrerror(code));
    return code;
  }

  for (int32_t i = SDB_MAX - 1; i > SDB_START; --i) {
    SHashObj *hash = tsSdb.hashObjs[i];
    if (!hash) continue;

    SdbEncodeFp encodeFp = tsSdb.encodeFps[i];
    if (!encodeFp) continue;

    SSdbRow *pRow = taosHashIterate(hash, NULL);
    while (pRow != NULL) {
      if (pRow->status == SDB_STATUS_READY) continue;
      SSdbRaw *pRaw = (*encodeFp)(pRow->data);
      if (pRaw != NULL) {
        taosWriteFile(fd, pRaw, sizeof(SSdbRaw) + pRaw->dataLen);
      } else {
        taosHashCancelIterate(hash, pRow);
        code = TSDB_CODE_SDB_INTERNAL_ERROR;
        break;
      }

      pRow = taosHashIterate(hash, pRow);
    }
  }

  if (code == 0) {
    code = sdbWriteVersion(fd);
  }

  taosCloseFile(fd);

  if (code == 0) {
    code = taosFsyncFile(fd);
  }

  if (code != 0) {
    char curfile[PATH_MAX] = {0};
    snprintf(curfile, sizeof(curfile), "%ssdb.data", tsSdb.currDir);
    code = taosRenameFile(tmpfile, curfile);
  }

  if (code != 0) {
    mError("failed to write sdb file since %s", tstrerror(code));
  } else {
    mInfo("write sdb file successfully");
  }

  return code;
}

int32_t sdbRead() {
  int32_t code = sdbReadDataFile();
  if (code != 0) {
    return code;
  }

  mInfo("read sdb file successfully");
  return -1;
}

int32_t sdbCommit() {
  int32_t code = sdbWriteDataFile();
  if (code != 0) {
    return code;
  }

  return 0;
}

int32_t sdbDeploy() {
  if (sdbCreateDir() != 0) {
    return -1;
  }

  if (sdbRunDeployFp() != 0) {
    return -1;
  }

  if (sdbCommit() != 0) {
    return -1;
  }

  return 0;
}

void sdbUnDeploy() {}

int32_t sdbInit() {
  char path[PATH_MAX + 100];

  snprintf(path, PATH_MAX + 100, "%s%scurrent%s", tsMnodeDir, TD_DIRSEP, TD_DIRSEP);
  tsSdb.currDir = strdup(path);

  snprintf(path, PATH_MAX + 100, "%s%ssync%s", tsMnodeDir, TD_DIRSEP, TD_DIRSEP);
  tsSdb.syncDir = strdup(path);

  snprintf(path, PATH_MAX + 100, "%s%stmp%s", tsMnodeDir, TD_DIRSEP, TD_DIRSEP);
  tsSdb.tmpDir = strdup(path);

  if (tsSdb.currDir == NULL || tsSdb.currDir == NULL || tsSdb.currDir == NULL) {
    return TSDB_CODE_MND_OUT_OF_MEMORY;
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

    SHashObj *hash = taosHashInit(128, taosGetDefaultHashFunction(type), true, HASH_NO_LOCK);
    if (hash == NULL) {
      return TSDB_CODE_MND_OUT_OF_MEMORY;
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

void sdbSetHandler(SSdbDesc desc) {
  ESdbType sdb = desc.sdbType;
  tsSdb.keyTypes[sdb] = desc.keyType;
  tsSdb.insertFps[sdb] = desc.insertFp;
  tsSdb.updateFps[sdb] = desc.updateFp;
  tsSdb.deleteFps[sdb] = desc.deleteFp;
  tsSdb.deployFps[sdb] = desc.deployFp;
  tsSdb.encodeFps[sdb] = desc.encodeFp;
  tsSdb.decodeFps[sdb] = desc.decodeFp;
}

#if 0
void *sdbInsertRow(ESdbType sdb, void *p) {
  SdbHead *pHead = p;
  pHead->type = sdb;
  pHead->status = SDB_AVAIL;

  char    *pKey = (char *)pHead + sizeof(pHead);
  int32_t  keySize;
  EKeyType keyType = tsSdb.keyTypes[pHead->type];
  int32_t  dataSize = tsSdb.dataSize[pHead->type];

  SHashObj *hash = sdbGetHash(pHead->type);
  if (hash == NULL) {
    return NULL;
  }

  if (keyType == SDBINT32) {
    keySize = sizeof(int32_t);
  } else if (keyType == SDB_KEY_BINARY) {
    keySize = strlen(pKey) + 1;
  } else {
    keySize = sizeof(int64_t);
  }

  taosHashPut(hash, pKey, keySize, pHead, dataSize);
  return taosHashGet(hash, pKey, keySize);
}

void sdbDeleteRow(ESdbType sdb, void *p) {
  SdbHead *pHead = p;
  pHead->status = SDB_STATUS_DROPPED;
}

void *sdbUpdateRow(ESdbType sdb, void *pHead) { return sdbInsertRow(sdb, pHead); }

#endif

void *sdbAcquire(ESdbType sdb, void *pKey) {
  terrno = 0;

  SHashObj *hash = sdbGetHash(sdb);
  if (hash == NULL) {
    return NULL;
  }

  int32_t  keySize;
  EKeyType keyType = tsSdb.keyTypes[sdb];

  switch (keyType) {
    case SDB_KEY_INT32:
      keySize = sizeof(int32_t);
      break;
    case SDB_KEY_INT64:
      keySize = sizeof(int64_t);
      break;
    case SDB_KEY_BINARY:
      keySize = strlen(pKey) + 1;
      break;
    default:
      keySize = sizeof(int32_t);
  }

  SSdbRow *pRow = taosHashGet(hash, pKey, keySize);
  if (pRow == NULL) return NULL;

  if (pRow->status == SDB_STATUS_READY) {
    atomic_add_fetch_32(&pRow->refCount, 1);
    return pRow->data;
  } else {
    terrno = -1;  // todo
    return NULL;
  }
}

void sdbRelease(void *pObj) {
  SSdbRow *pRow = (SSdbRow *)((char *)pObj - sizeof(SSdbRow));
  atomic_sub_fetch_32(&pRow->refCount, 1);
}

void *sdbFetchRow(ESdbType sdb, void *pIter) {
  SHashObj *hash = sdbGetHash(sdb);
  if (hash == NULL) {
    return NULL;
  }

  return taosHashIterate(hash, pIter);
}

void sdbCancelFetch(ESdbType sdb, void *pIter) {
  SHashObj *hash = sdbGetHash(sdb);
  if (hash == NULL) {
    return;
  }
  taosHashCancelIterate(hash, pIter);
}

int32_t sdbGetSize(ESdbType sdb) {
  SHashObj *hash = sdbGetHash(sdb);
  if (hash == NULL) {
    return 0;
  }
  return taosHashGetSize(hash);
}