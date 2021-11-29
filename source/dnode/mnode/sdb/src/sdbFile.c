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

static int32_t sdbCreateDir(SSdb *pSdb) {
  int32_t code = taosMkDir(pSdb->currDir);
  if (code != 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    mError("failed to create dir:%s since %s", pSdb->currDir, tstrerror(code));
    return code;
  }

  code = taosMkDir(pSdb->syncDir);
  if (code != 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    mError("failed to create dir:%s since %s", pSdb->syncDir, tstrerror(code));
    return code;
  }

  code = taosMkDir(pSdb->tmpDir);
  if (code != 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    mError("failed to create dir:%s since %s", pSdb->tmpDir, tstrerror(code));
    return code;
  }

  return 0;
}

static int32_t sdbRunDeployFp(SSdb *pSdb) {
  mDebug("start to run sdb deploy functions");

  for (int32_t i = SDB_MAX - 1; i > SDB_START; --i) {
    SdbDeployFp fp = pSdb->deployFps[i];
    if (fp == NULL) continue;

    int32_t code = (*fp)(pSdb);
    if (code != 0) {
      mError("failed to deploy sdb:%d since %s", i, tstrerror(code));
      return code;
    }
  }

  mDebug("sdb deploy functions run finished");
  return 0;
}

int32_t sdbReadFile(SSdb *pSdb) {
  int64_t offset = 0;
  int32_t code = 0;
  int32_t readLen = 0;
  int64_t ret = 0;

  SSdbRaw *pRaw = malloc(SDB_MAX_SIZE);
  if (pRaw == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    mError("failed read file since %s", tstrerror(code));
    return code;
  }

  char file[PATH_MAX] = {0};
  snprintf(file, sizeof(file), "%s%ssdb.data", pSdb->currDir, TD_DIRSEP);

  FileFd fd = taosOpenFileRead(file);
  if (fd <= 0) {
    free(pRaw);
    code = TAOS_SYSTEM_ERROR(errno);
    mError("failed to read file:%s since %s", file, tstrerror(code));
    return code;
  }

  while (1) {
    readLen = sizeof(SSdbRaw);
    ret = taosReadFile(fd, pRaw, readLen);
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
    ret = taosReadFile(fd, pRaw->pData, readLen);
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
    if (!taosCheckChecksumWhole((const uint8_t *)pRaw, totalLen) != 0) {
      code = TSDB_CODE_CHECKSUM_ERROR;
      mError("failed to read file:%s since %s", file, tstrerror(code));
      break;
    }

    code = sdbWriteRaw(pSdb, pRaw);
    if (code != 0) {
      mError("failed to read file:%s since %s", file, terrstr());
      goto PARSE_SDB_DATA_ERROR;
    }
  }

  code = 0;

PARSE_SDB_DATA_ERROR:
  taosCloseFile(fd);
  sdbFreeRaw(pRaw);

  return code;
}

int32_t sdbWriteFile(SSdb *pSdb) {
  int32_t code = 0;

  char tmpfile[PATH_MAX] = {0};
  snprintf(tmpfile, sizeof(tmpfile), "%ssdb.data", pSdb->tmpDir);
  char curfile[PATH_MAX] = {0};
  snprintf(curfile, sizeof(curfile), "%ssdb.data", pSdb->currDir);

  FileFd fd = taosOpenFileCreateWrite(tmpfile);
  if (fd <= 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    mError("failed to open file:%s for write since %s", tmpfile, tstrerror(code));
    return code;
  }

  for (int32_t i = SDB_MAX - 1; i > SDB_START; --i) {
    SdbEncodeFp encodeFp = pSdb->encodeFps[i];
    if (encodeFp == NULL) continue;

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

      SSdbRaw *pRaw = (*encodeFp)(pRow->pObj);
      if (pRaw != NULL) {
        pRaw->status = pRow->status;
        int32_t writeLen = sizeof(SSdbRaw) + pRaw->dataLen;
        if (taosWriteFile(fd, pRaw, writeLen) != writeLen) {
          code = TAOS_SYSTEM_ERROR(terrno);
          taosHashCancelIterate(hash, ppRow);
          break;
        }

        int32_t cksum = taosCalcChecksum(0, (const uint8_t *)pRaw, sizeof(SSdbRaw) + pRaw->dataLen);
        if (taosWriteFile(fd, &cksum, sizeof(int32_t)) != sizeof(int32_t)) {
          code = TAOS_SYSTEM_ERROR(terrno);
          taosHashCancelIterate(hash, ppRow);
          break;
        }
      } else {
        code = TSDB_CODE_SDB_APP_ERROR;
        taosHashCancelIterate(hash, ppRow);
        break;
      }

      ppRow = taosHashIterate(hash, ppRow);
    }
    taosWUnLockLatch(pLock);
  }

  if (code == 0) {
    code = taosFsyncFile(fd);
    if (code != 0) {
      code = TAOS_SYSTEM_ERROR(errno);
      mError("failed to write file:%s since %s", tmpfile, tstrerror(code));
    }
  }

  taosCloseFile(fd);

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
    mDebug("write file:%s successfully", curfile);
  }

  return code;
}

int32_t sdbDeploy(SSdb *pSdb) {
  int32_t code = sdbCreateDir(pSdb);
  if (code != 0) {
    return code;
  }

  code = sdbRunDeployFp(pSdb);
  if (code != 0) {
    return code;
  }

  code = sdbWriteFile(pSdb);
  if (code != 0) {
    return code;
  }

  return 0;
}
