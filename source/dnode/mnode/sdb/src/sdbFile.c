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
#include "sdb.h"
#include "sync.h"
#include "tchecksum.h"
#include "wal.h"

#define SDB_TABLE_SIZE   24
#define SDB_RESERVE_SIZE 512
#define SDB_FILE_VER     1

static int32_t sdbDeployData(SSdb *pSdb) {
  mInfo("start to deploy sdb");

  for (int32_t i = SDB_MAX - 1; i >= 0; --i) {
    SdbDeployFp fp = pSdb->deployFps[i];
    if (fp == NULL) continue;

    mInfo("start to deploy sdb:%s", sdbTableName(i));
    if ((*fp)(pSdb->pMnode) != 0) {
      mError("failed to deploy sdb:%s since %s", sdbTableName(i), terrstr());
      return -1;
    }
  }

  mInfo("sdb deploy success");
  return 0;
}

static void sdbResetData(SSdb *pSdb) {
  mInfo("start to reset sdb");

  for (ESdbType i = 0; i < SDB_MAX; ++i) {
    SHashObj *hash = pSdb->hashObjs[i];
    if (hash == NULL) continue;

    SSdbRow **ppRow = taosHashIterate(hash, NULL);
    while (ppRow != NULL) {
      SSdbRow *pRow = *ppRow;
      if (pRow == NULL) continue;

      sdbFreeRow(pSdb, pRow, true);
      ppRow = taosHashIterate(hash, ppRow);
    }
  }

  for (ESdbType i = 0; i < SDB_MAX; ++i) {
    SHashObj *hash = pSdb->hashObjs[i];
    if (hash == NULL) continue;

    taosHashClear(pSdb->hashObjs[i]);
    pSdb->tableVer[i] = 0;
    pSdb->maxId[i] = 0;
    mInfo("sdb:%s is reset", sdbTableName(i));
  }

  pSdb->applyIndex = -1;
  pSdb->applyTerm = -1;
  pSdb->applyConfig = -1;
  pSdb->commitIndex = -1;
  pSdb->commitTerm = -1;
  pSdb->commitConfig = -1;
  mInfo("sdb reset success");
}

static int32_t sdbReadFileHead(SSdb *pSdb, TdFilePtr pFile) {
  int64_t sver = 0;
  int32_t ret = taosReadFile(pFile, &sver, sizeof(int64_t));
  if (ret < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }
  if (ret != sizeof(int64_t)) {
    terrno = TSDB_CODE_FILE_CORRUPTED;
    return -1;
  }
  if (sver != SDB_FILE_VER) {
    terrno = TSDB_CODE_FILE_CORRUPTED;
    return -1;
  }

  ret = taosReadFile(pFile, &pSdb->applyIndex, sizeof(int64_t));
  if (ret < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }
  if (ret != sizeof(int64_t)) {
    terrno = TSDB_CODE_FILE_CORRUPTED;
    return -1;
  }

  ret = taosReadFile(pFile, &pSdb->applyTerm, sizeof(int64_t));
  if (ret < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }
  if (ret != sizeof(int64_t)) {
    terrno = TSDB_CODE_FILE_CORRUPTED;
    return -1;
  }

  ret = taosReadFile(pFile, &pSdb->applyConfig, sizeof(int64_t));
  if (ret < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }
  if (ret != sizeof(int64_t)) {
    terrno = TSDB_CODE_FILE_CORRUPTED;
    return -1;
  }

  for (int32_t i = 0; i < SDB_TABLE_SIZE; ++i) {
    int64_t maxId = 0;
    ret = taosReadFile(pFile, &maxId, sizeof(int64_t));
    if (ret < 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      return -1;
    }
    if (ret != sizeof(int64_t)) {
      terrno = TSDB_CODE_FILE_CORRUPTED;
      return -1;
    }
    if (i < SDB_MAX) {
      pSdb->maxId[i] = maxId;
    }
  }

  for (int32_t i = 0; i < SDB_TABLE_SIZE; ++i) {
    int64_t ver = 0;
    ret = taosReadFile(pFile, &ver, sizeof(int64_t));
    if (ret < 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      return -1;
    }
    if (ret != sizeof(int64_t)) {
      terrno = TSDB_CODE_FILE_CORRUPTED;
      return -1;
    }
    if (i < SDB_MAX) {
      pSdb->tableVer[i] = ver;
    }
  }

  char reserve[SDB_RESERVE_SIZE] = {0};
  ret = taosReadFile(pFile, reserve, sizeof(reserve));
  if (ret < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }
  if (ret != sizeof(reserve)) {
    terrno = TSDB_CODE_FILE_CORRUPTED;
    return -1;
  }

  return 0;
}

static int32_t sdbWriteFileHead(SSdb *pSdb, TdFilePtr pFile) {
  int64_t sver = SDB_FILE_VER;
  if (taosWriteFile(pFile, &sver, sizeof(int64_t)) != sizeof(int64_t)) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (taosWriteFile(pFile, &pSdb->applyIndex, sizeof(int64_t)) != sizeof(int64_t)) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (taosWriteFile(pFile, &pSdb->applyTerm, sizeof(int64_t)) != sizeof(int64_t)) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (taosWriteFile(pFile, &pSdb->applyConfig, sizeof(int64_t)) != sizeof(int64_t)) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  for (int32_t i = 0; i < SDB_TABLE_SIZE; ++i) {
    int64_t maxId = 0;
    if (i < SDB_MAX) {
      maxId = pSdb->maxId[i];
    }
    if (taosWriteFile(pFile, &maxId, sizeof(int64_t)) != sizeof(int64_t)) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      return -1;
    }
  }

  for (int32_t i = 0; i < SDB_TABLE_SIZE; ++i) {
    int64_t ver = 0;
    if (i < SDB_MAX) {
      ver = pSdb->tableVer[i];
    }
    if (taosWriteFile(pFile, &ver, sizeof(int64_t)) != sizeof(int64_t)) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      return -1;
    }
  }

  char reserve[SDB_RESERVE_SIZE] = {0};
  if (taosWriteFile(pFile, reserve, sizeof(reserve)) != sizeof(reserve)) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return 0;
}

static int32_t sdbReadFileImp(SSdb *pSdb) {
  int64_t offset = 0;
  int32_t code = 0;
  int32_t readLen = 0;
  int64_t ret = 0;
  char    file[PATH_MAX] = {0};
  int32_t bufLen = TSDB_MAX_MSG_SIZE;

  snprintf(file, sizeof(file), "%s%ssdb.data", pSdb->currDir, TD_DIRSEP);
  mInfo("start to read sdb file:%s", file);

  SSdbRaw *pRaw = taosMemoryMalloc(bufLen + 100);
  if (pRaw == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("failed read sdb file since %s", terrstr());
    return -1;
  }

  TdFilePtr pFile = taosOpenFile(file, TD_FILE_READ);
  if (pFile == NULL) {
    taosMemoryFree(pRaw);
    terrno = TAOS_SYSTEM_ERROR(errno);
    mInfo("read sdb file:%s finished since %s", file, terrstr());
    return 0;
  }

  if (sdbReadFileHead(pSdb, pFile) != 0) {
    mError("failed to read sdb file:%s head since %s", file, terrstr());
    taosMemoryFree(pRaw);
    taosCloseFile(&pFile);
    return -1;
  }

  int64_t tableVer[SDB_MAX] = {0};
  memcpy(tableVer, pSdb->tableVer, sizeof(tableVer));

  while (1) {
    readLen = sizeof(SSdbRaw);
    ret = taosReadFile(pFile, pRaw, readLen);
    if (ret == 0) break;

    if (ret < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      mError("failed to read sdb file:%s since %s", file, tstrerror(code));
      goto _OVER;
    }

    if (ret != readLen) {
      code = TSDB_CODE_FILE_CORRUPTED;
      mError("failed to read sdb file:%s since %s, ret:%" PRId64 " != readLen:%d", file, tstrerror(code), ret, readLen);
      goto _OVER;
    }

    readLen = pRaw->dataLen + sizeof(int32_t);
    if (readLen >= bufLen) {
      bufLen = pRaw->dataLen * 2;
      SSdbRaw *pNewRaw = taosMemoryMalloc(bufLen + 100);
      if (pNewRaw == NULL) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        mError("failed read sdb file since malloc new sdbRaw size:%d failed", bufLen);
        goto _OVER;
      }
      mInfo("malloc new sdb raw size:%d, type:%d", bufLen, pRaw->type);
      memcpy(pNewRaw, pRaw, sizeof(SSdbRaw));
      sdbFreeRaw(pRaw);
      pRaw = pNewRaw;
    }

    ret = taosReadFile(pFile, pRaw->pData, readLen);
    if (ret < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      mError("failed to read sdb file:%s since %s, ret:%" PRId64 " readLen:%d", file, tstrerror(code), ret, readLen);
      goto _OVER;
    }

    if (ret != readLen) {
      code = TSDB_CODE_FILE_CORRUPTED;
      mError("failed to read sdb file:%s since %s, ret:%" PRId64 " != readLen:%d", file, tstrerror(code), ret, readLen);
      goto _OVER;
    }

    int32_t totalLen = sizeof(SSdbRaw) + pRaw->dataLen + sizeof(int32_t);
    if ((!taosCheckChecksumWhole((const uint8_t *)pRaw, totalLen)) != 0) {
      code = TSDB_CODE_CHECKSUM_ERROR;
      mError("failed to read sdb file:%s since %s, readLen:%d", file, tstrerror(code), readLen);
      goto _OVER;
    }

    code = sdbWriteWithoutFree(pSdb, pRaw);
    if (code != 0) {
      mError("failed to read sdb file:%s since %s", file, terrstr());
      goto _OVER;
    }
  }

  code = 0;
  pSdb->commitIndex = pSdb->applyIndex;
  pSdb->commitTerm = pSdb->applyTerm;
  pSdb->commitConfig = pSdb->applyConfig;
  memcpy(pSdb->tableVer, tableVer, sizeof(tableVer));
  mInfo("read sdb file:%s success, commit index:%" PRId64 " term:%" PRId64 " config:%" PRId64, file, pSdb->commitIndex,
        pSdb->commitTerm, pSdb->commitConfig);

_OVER:
  taosCloseFile(&pFile);
  sdbFreeRaw(pRaw);

  terrno = code;
  return code;
}

int32_t sdbReadFile(SSdb *pSdb) {
  taosThreadMutexLock(&pSdb->filelock);

  sdbResetData(pSdb);
  int32_t code = sdbReadFileImp(pSdb);
  if (code != 0) {
    mError("failed to read sdb file since %s", terrstr());
    sdbResetData(pSdb);
  }

  taosThreadMutexUnlock(&pSdb->filelock);
  return code;
}

static int32_t sdbWriteFileImp(SSdb *pSdb) {
  int32_t code = 0;

  char tmpfile[PATH_MAX] = {0};
  snprintf(tmpfile, sizeof(tmpfile), "%s%ssdb.data", pSdb->tmpDir, TD_DIRSEP);
  char curfile[PATH_MAX] = {0};
  snprintf(curfile, sizeof(curfile), "%s%ssdb.data", pSdb->currDir, TD_DIRSEP);

  mInfo("start to write sdb file, apply index:%" PRId64 " term:%" PRId64 " config:%" PRId64 ", commit index:%" PRId64
        " term:%" PRId64 " config:%" PRId64 ", file:%s",
        pSdb->applyIndex, pSdb->applyTerm, pSdb->applyConfig, pSdb->commitIndex, pSdb->commitTerm, pSdb->commitConfig,
        curfile);

  TdFilePtr pFile = taosOpenFile(tmpfile, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    mError("failed to open sdb file:%s for write since %s", tmpfile, terrstr());
    return -1;
  }

  if (sdbWriteFileHead(pSdb, pFile) != 0) {
    mError("failed to write sdb file:%s head since %s", tmpfile, terrstr());
    taosCloseFile(&pFile);
    return -1;
  }

  for (int32_t i = SDB_MAX - 1; i >= 0; --i) {
    SdbEncodeFp encodeFp = pSdb->encodeFps[i];
    if (encodeFp == NULL) continue;

    mInfo("write %s to sdb file, total %d rows", sdbTableName(i), sdbGetSize(pSdb, i));

    SHashObj *hash = pSdb->hashObjs[i];
    sdbWriteLock(pSdb, i);

    SSdbRow **ppRow = taosHashIterate(hash, NULL);
    while (ppRow != NULL) {
      SSdbRow *pRow = *ppRow;
      if (pRow == NULL) {
        ppRow = taosHashIterate(hash, ppRow);
        continue;
      }

      if (pRow->status != SDB_STATUS_READY && pRow->status != SDB_STATUS_DROPPING) {
        sdbPrintOper(pSdb, pRow, "not-write");
        ppRow = taosHashIterate(hash, ppRow);
        continue;
      }

      sdbPrintOper(pSdb, pRow, "write");

      SSdbRaw *pRaw = (*encodeFp)(pRow->pObj);
      if (pRaw != NULL) {
        pRaw->status = pRow->status;
        int32_t writeLen = sizeof(SSdbRaw) + pRaw->dataLen;
        if (taosWriteFile(pFile, pRaw, writeLen) != writeLen) {
          code = TAOS_SYSTEM_ERROR(errno);
          taosHashCancelIterate(hash, ppRow);
          sdbFreeRaw(pRaw);
          break;
        }

        int32_t cksum = taosCalcChecksum(0, (const uint8_t *)pRaw, sizeof(SSdbRaw) + pRaw->dataLen);
        if (taosWriteFile(pFile, &cksum, sizeof(int32_t)) != sizeof(int32_t)) {
          code = TAOS_SYSTEM_ERROR(errno);
          taosHashCancelIterate(hash, ppRow);
          sdbFreeRaw(pRaw);
          break;
        }
      } else {
        code = TSDB_CODE_APP_ERROR;
        taosHashCancelIterate(hash, ppRow);
        break;
      }

      sdbFreeRaw(pRaw);
      ppRow = taosHashIterate(hash, ppRow);
    }
    sdbUnLock(pSdb, i);
  }

  if (code == 0) {
    code = taosFsyncFile(pFile);
    if (code != 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      mError("failed to sync sdb file:%s since %s", tmpfile, tstrerror(code));
    }
  }

  taosCloseFile(&pFile);

  if (code == 0) {
    code = taosRenameFile(tmpfile, curfile);
    if (code != 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      mError("failed to write sdb file:%s since %s", curfile, tstrerror(code));
    }
  }

  if (code != 0) {
    mError("failed to write sdb file:%s since %s", curfile, tstrerror(code));
  } else {
    pSdb->commitIndex = pSdb->applyIndex;
    pSdb->commitTerm = pSdb->applyTerm;
    pSdb->commitConfig = pSdb->applyConfig;
    mInfo("write sdb file success, commit index:%" PRId64 " term:%" PRId64 " config:%" PRId64 " file:%s",
          pSdb->commitIndex, pSdb->commitTerm, pSdb->commitConfig, curfile);
  }

  terrno = code;
  return code;
}

int32_t sdbWriteFile(SSdb *pSdb, int32_t delta) {
  int32_t code = 0;
  if (pSdb->applyIndex == pSdb->commitIndex) {
    return 0;
  }

  if (pSdb->applyIndex - pSdb->commitIndex < delta) {
    return 0;
  }

  taosThreadMutexLock(&pSdb->filelock);
  if (pSdb->pWal != NULL) {
    if (pSdb->sync > 0) {
      code = syncBeginSnapshot(pSdb->sync, pSdb->applyIndex);
    }
  }
  if (code == 0) {
    code = sdbWriteFileImp(pSdb);
  }
  if (code == 0) {
    if (pSdb->pWal != NULL) {
      if (pSdb->sync > 0) {
        code = syncEndSnapshot(pSdb->sync);
      }
    }
  }
  if (code != 0) {
    mError("failed to write sdb file since %s", terrstr());
  }
  taosThreadMutexUnlock(&pSdb->filelock);
  return code;
}

int32_t sdbDeploy(SSdb *pSdb) {
  if (sdbDeployData(pSdb) != 0) {
    return -1;
  }

  if (sdbWriteFile(pSdb, 0) != 0) {
    return -1;
  }

  return 0;
}

static SSdbIter *sdbCreateIter(SSdb *pSdb) {
  SSdbIter *pIter = taosMemoryCalloc(1, sizeof(SSdbIter));
  if (pIter == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  char name[PATH_MAX + 100] = {0};
  snprintf(name, sizeof(name), "%s%ssdb.data.%" PRIu64, pSdb->tmpDir, TD_DIRSEP, (uint64_t)pIter);
  pIter->name = taosStrdup(name);
  if (pIter->name == NULL) {
    taosMemoryFree(pIter);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  return pIter;
}

static void sdbCloseIter(SSdbIter *pIter) {
  if (pIter == NULL) return;

  if (pIter->file != NULL) {
    taosCloseFile(&pIter->file);
    pIter->file = NULL;
  }

  if (pIter->name != NULL) {
    (void)taosRemoveFile(pIter->name);
    taosMemoryFree(pIter->name);
    pIter->name = NULL;
  }

  mInfo("sdbiter:%p, is closed, total:%" PRId64, pIter, pIter->total);
  taosMemoryFree(pIter);
}

int32_t sdbStartRead(SSdb *pSdb, SSdbIter **ppIter, int64_t *index, int64_t *term, int64_t *config) {
  SSdbIter *pIter = sdbCreateIter(pSdb);
  if (pIter == NULL) return -1;

  char datafile[PATH_MAX] = {0};
  snprintf(datafile, sizeof(datafile), "%s%ssdb.data", pSdb->currDir, TD_DIRSEP);

  taosThreadMutexLock(&pSdb->filelock);
  int64_t commitIndex = pSdb->commitIndex;
  int64_t commitTerm = pSdb->commitTerm;
  int64_t commitConfig = pSdb->commitConfig;
  if (taosCopyFile(datafile, pIter->name) < 0) {
    taosThreadMutexUnlock(&pSdb->filelock);
    terrno = TAOS_SYSTEM_ERROR(errno);
    mError("failed to copy sdb file %s to %s since %s", datafile, pIter->name, terrstr());
    sdbCloseIter(pIter);
    return -1;
  }
  taosThreadMutexUnlock(&pSdb->filelock);

  pIter->file = taosOpenFile(pIter->name, TD_FILE_READ);
  if (pIter->file == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    mError("failed to open sdb file:%s since %s", pIter->name, terrstr());
    sdbCloseIter(pIter);
    return -1;
  }

  *ppIter = pIter;
  if (index != NULL) *index = commitIndex;
  if (term != NULL) *term = commitTerm;
  if (config != NULL) *config = commitConfig;

  mInfo("sdbiter:%p, is created to read snapshot, commit index:%" PRId64 " term:%" PRId64 " config:%" PRId64 " file:%s",
        pIter, commitIndex, commitTerm, commitConfig, pIter->name);
  return 0;
}

void sdbStopRead(SSdb *pSdb, SSdbIter *pIter) { sdbCloseIter(pIter); }

int32_t sdbDoRead(SSdb *pSdb, SSdbIter *pIter, void **ppBuf, int32_t *len) {
  int32_t maxlen = 4096;
  void   *pBuf = taosMemoryCalloc(1, maxlen);
  if (pBuf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  int32_t readlen = taosReadFile(pIter->file, pBuf, maxlen);
  if (readlen < 0 || readlen > maxlen) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    mError("sdbiter:%p, failed to read snapshot since %s, total:%" PRId64, pIter, terrstr(), pIter->total);
    *ppBuf = NULL;
    *len = 0;
    taosMemoryFree(pBuf);
    return -1;
  } else if (readlen == 0) {
    mInfo("sdbiter:%p, read snapshot to the end, total:%" PRId64, pIter, pIter->total);
    *ppBuf = NULL;
    *len = 0;
    taosMemoryFree(pBuf);
    return 0;
  } else {  // (readlen <= maxlen)
    pIter->total += readlen;
    mInfo("sdbiter:%p, read:%d bytes from snapshot, total:%" PRId64, pIter, readlen, pIter->total);
    *ppBuf = pBuf;
    *len = readlen;
    return 0;
  }
}

int32_t sdbStartWrite(SSdb *pSdb, SSdbIter **ppIter) {
  SSdbIter *pIter = sdbCreateIter(pSdb);
  if (pIter == NULL) return -1;

  pIter->file = taosOpenFile(pIter->name, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pIter->file == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    mError("failed to open %s since %s", pIter->name, terrstr());
    sdbCloseIter(pIter);
    return -1;
  }

  *ppIter = pIter;
  mInfo("sdbiter:%p, is created to write snapshot, file:%s", pIter, pIter->name);
  return 0;
}

int32_t sdbStopWrite(SSdb *pSdb, SSdbIter *pIter, bool isApply, int64_t index, int64_t term, int64_t config) {
  int32_t code = -1;

  if (!isApply) {
    mInfo("sdbiter:%p, not apply to sdb", pIter);
    code = 0;
    goto _OVER;
  }

  if (taosFsyncFile(pIter->file) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    mError("sdbiter:%p, failed to fasync file %s since %s", pIter, pIter->name, terrstr());
    goto _OVER;
  }

  taosCloseFile(&pIter->file);
  pIter->file = NULL;

  char datafile[PATH_MAX] = {0};
  snprintf(datafile, sizeof(datafile), "%s%ssdb.data", pSdb->currDir, TD_DIRSEP);
  if (taosRenameFile(pIter->name, datafile) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    mError("sdbiter:%p, failed to rename file %s to %s since %s", pIter, pIter->name, datafile, terrstr());
    goto _OVER;
  }

  if (sdbReadFile(pSdb) != 0) {
    mError("sdbiter:%p, failed to read from %s since %s", pIter, datafile, terrstr());
    goto _OVER;
  }

  if (config > 0) {
    pSdb->commitConfig = config;
  }
  if (term > 0) {
    pSdb->commitTerm = term;
  }
  if (index > 0) {
    pSdb->commitIndex = index;
  }

  mInfo("sdbiter:%p, success applyed to sdb", pIter);
  code = 0;

_OVER:
  sdbCloseIter(pIter);
  return code;
}

int32_t sdbDoWrite(SSdb *pSdb, SSdbIter *pIter, void *pBuf, int32_t len) {
  int32_t writelen = taosWriteFile(pIter->file, pBuf, len);
  if (writelen != len) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    mError("failed to write len:%d since %s, total:%" PRId64, len, terrstr(), pIter->total);
    return -1;
  }

  pIter->total += writelen;
  mInfo("sdbiter:%p, write:%d bytes to snapshot, total:%" PRId64, pIter, writelen, pIter->total);
  return 0;
}
