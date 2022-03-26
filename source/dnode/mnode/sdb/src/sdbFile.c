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
#include "tchecksum.h"

#define SDB_TABLE_SIZE 24
#define SDB_RESERVE_SIZE 512

static int32_t sdbRunDeployFp(SSdb *pSdb) {
  mDebug("start to deploy sdb");

  for (int32_t i = SDB_MAX - 1; i >= 0; --i) {
    SdbDeployFp fp = pSdb->deployFps[i];
    if (fp == NULL) continue;

    if ((*fp)(pSdb->pMnode) != 0) {
      mError("failed to deploy sdb:%d since %s", i, terrstr());
      return -1;
    }
  }

  mDebug("sdb deploy successfully");
  return 0;
}

static int32_t sdbReadFileHead(SSdb *pSdb, TdFilePtr pFile) {
  int32_t ret = taosReadFile(pFile, &pSdb->curVer, sizeof(int64_t));
  if (ret < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }
  if (ret != sizeof(int64_t)) {
    terrno = TSDB_CODE_FILE_CORRUPTED;
    return -1;
  }

  for (int32_t i = 0; i < SDB_TABLE_SIZE; ++i) {
    int64_t maxId = -1;
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
    int64_t ver = -1;
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
  if (taosWriteFile(pFile, &pSdb->curVer, sizeof(int64_t)) != sizeof(int64_t)) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  for (int32_t i = 0; i < SDB_TABLE_SIZE; ++i) {
    int64_t maxId = -1;
    if (i < SDB_MAX) {
      maxId = pSdb->maxId[i];
    }
    if (taosWriteFile(pFile, &maxId, sizeof(int64_t)) != sizeof(int64_t)) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      return -1;
    }
  }

  for (int32_t i = 0; i < SDB_TABLE_SIZE; ++i) {
    int64_t ver = -1;
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

int32_t sdbReadFile(SSdb *pSdb) {
  int64_t offset = 0;
  int32_t code = 0;
  int32_t readLen = 0;
  int64_t ret = 0;

  SSdbRaw *pRaw = taosMemoryMalloc(SDB_MAX_SIZE);
  if (pRaw == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("failed read file since %s", terrstr());
    return -1;
  }

  char file[PATH_MAX] = {0};
  snprintf(file, sizeof(file), "%s%ssdb.data", pSdb->currDir, TD_DIRSEP);
  mDebug("start to read file:%s", file);

  TdFilePtr pFile = taosOpenFile(file, TD_FILE_READ);
  if (pFile == NULL) {
    taosMemoryFree(pRaw);
    terrno = TAOS_SYSTEM_ERROR(errno);
    mError("failed to read file:%s since %s", file, terrstr());
    return 0;
  }

  if (sdbReadFileHead(pSdb, pFile) != 0) {
    mError("failed to read file:%s head since %s", file, terrstr());
    pSdb->curVer = -1;
    taosMemoryFree(pRaw);
    taosCloseFile(&pFile);
    return -1;
  }

  while (1) {
    readLen = sizeof(SSdbRaw);
    ret = taosReadFile(pFile, pRaw, readLen);
    if (ret == 0) break;

    if (ret < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      mError("failed to read file:%s since %s", file, tstrerror(code));
      break;
    }

    if (ret != readLen) {
      code = TSDB_CODE_FILE_CORRUPTED;
      mError("failed to read file:%s since %s", file, tstrerror(code));
      break;
    }

    readLen = pRaw->dataLen + sizeof(int32_t);
    ret = taosReadFile(pFile, pRaw->pData, readLen);
    if (ret < 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      mError("failed to read file:%s since %s", file, tstrerror(code));
      break;
    }

    if (ret != readLen) {
      code = TSDB_CODE_FILE_CORRUPTED;
      mError("failed to read file:%s since %s", file, tstrerror(code));
      break;
    }

    int32_t totalLen = sizeof(SSdbRaw) + pRaw->dataLen + sizeof(int32_t);
    if ((!taosCheckChecksumWhole((const uint8_t *)pRaw, totalLen)) != 0) {
      code = TSDB_CODE_CHECKSUM_ERROR;
      mError("failed to read file:%s since %s", file, tstrerror(code));
      break;
    }

    code = sdbWriteNotFree(pSdb, pRaw);
    if (code != 0) {
      mError("failed to read file:%s since %s", file, terrstr());
      goto PARSE_SDB_DATA_ERROR;
    }
  }

  code = 0;
  pSdb->lastCommitVer = pSdb->curVer;
  mDebug("read file:%s successfully, ver:%" PRId64, file, pSdb->lastCommitVer);

PARSE_SDB_DATA_ERROR:
  taosCloseFile(&pFile);
  sdbFreeRaw(pRaw);

  terrno = code;
  return code;
}

static int32_t sdbWriteFileImp(SSdb *pSdb) {
  int32_t code = 0;

  char tmpfile[PATH_MAX] = {0};
  snprintf(tmpfile, sizeof(tmpfile), "%s%ssdb.data", pSdb->tmpDir, TD_DIRSEP);
  char curfile[PATH_MAX] = {0};
  snprintf(curfile, sizeof(curfile), "%s%ssdb.data", pSdb->currDir, TD_DIRSEP);

  mDebug("start to write file:%s, current ver:%" PRId64 ", commit ver:%" PRId64, curfile, pSdb->curVer,
         pSdb->lastCommitVer);

  TdFilePtr pFile = taosOpenFile(tmpfile, TD_FILE_CTEATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    mError("failed to open file:%s for write since %s", tmpfile, terrstr());
    return -1;
  }

  if (sdbWriteFileHead(pSdb, pFile) != 0) {
    mError("failed to write file:%s head since %s", tmpfile, terrstr());
    taosCloseFile(&pFile);
    return -1;
  }

  for (int32_t i = SDB_MAX - 1; i >= 0; --i) {
    SdbEncodeFp encodeFp = pSdb->encodeFps[i];
    if (encodeFp == NULL) continue;

    mTrace("write %s to file, total %d rows", sdbTableName(i), sdbGetSize(pSdb, i));

    SHashObj *hash = pSdb->hashObjs[i];
    SRWLatch *pLock = &pSdb->locks[i];
    taosWLockLatch(pLock);

    SSdbRow **ppRow = taosHashIterate(hash, NULL);
    while (ppRow != NULL) {
      SSdbRow *pRow = *ppRow;
      if (pRow == NULL || pRow->status != SDB_STATUS_READY) {
        ppRow = taosHashIterate(hash, ppRow);
        continue;
      }

      sdbPrintOper(pSdb, pRow, "writeFile");

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
        code = TSDB_CODE_SDB_APP_ERROR;
        taosHashCancelIterate(hash, ppRow);
        break;
      }

      sdbFreeRaw(pRaw);
      ppRow = taosHashIterate(hash, ppRow);
    }
    taosWUnLockLatch(pLock);
  }

  if (code == 0) {
    code = taosFsyncFile(pFile);
    if (code != 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      mError("failed to write file:%s since %s", tmpfile, tstrerror(code));
    }
  }

  taosCloseFile(&pFile);

  if (code == 0) {
    code = taosRenameFile(tmpfile, curfile);
    if (code != 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      mError("failed to write file:%s since %s", curfile, tstrerror(code));
    }
  }

  if (code != 0) {
    mError("failed to write file:%s since %s", curfile, tstrerror(code));
  } else {
    pSdb->lastCommitVer = pSdb->curVer;
    mDebug("write file:%s successfully, ver:%" PRId64, curfile, pSdb->lastCommitVer);
  }

  terrno = code;
  return code;
}

int32_t sdbWriteFile(SSdb *pSdb) {
  if (pSdb->curVer == pSdb->lastCommitVer) {
    return 0;
  }

  return sdbWriteFileImp(pSdb);
}

int32_t sdbDeploy(SSdb *pSdb) {
  if (sdbRunDeployFp(pSdb) != 0) {
    return -1;
  }

  if (sdbWriteFileImp(pSdb) != 0) {
    return -1;
  }

  return 0;
}
