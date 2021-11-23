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
#include "tchecksum.h"

static int32_t sdbCreateDir() {
  mDebug("start to create mnode at %s", tsMnodeDir);
  
  if (taosMkDir(tsSdb.currDir) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    mError("failed to create dir:%s since %s", tsSdb.currDir, terrstr());
    return -1;
  }

  if (taosMkDir(tsSdb.syncDir) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    mError("failed to create dir:%s since %s", tsSdb.syncDir, terrstr());
    return -1;
  }

  if (taosMkDir(tsSdb.tmpDir) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    mError("failed to create dir:%s since %s", tsSdb.tmpDir, terrstr());
    return -1;
  }

  return 0;
}

static int32_t sdbRunDeployFp() {
  mDebug("start to run deploy functions");

  for (int32_t i = SDB_MAX - 1; i > SDB_START; --i) {
    SdbDeployFp fp = tsSdb.deployFps[i];
    if (fp == NULL) continue;
    if ((*fp)() != 0) {
      mError("failed to deploy sdb:%d since %s", i, terrstr());
      return -1;
    }
  }

  mDebug("end of run deploy functions");
  return 0;
}

static int32_t sdbReadDataFile() {
  SSdbRaw *pRaw = malloc(SDB_MAX_SIZE);
  if (pRaw == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  char file[PATH_MAX] = {0};
  snprintf(file, sizeof(file), "%ssdb.data", tsSdb.currDir);
  FileFd fd = taosOpenFileRead(file);
  if (fd <= 0) {
    free(pRaw);
    terrno = TAOS_SYSTEM_ERROR(errno);
    mError("failed to open file:%s for read since %s", file, terrstr());
    return -1;
  }

  int64_t offset = 0;
  int32_t code = 0;
  int32_t readLen = 0;
  int64_t ret = 0;

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

    code = sdbWriteImp(pRaw);
    if (code != 0) {
      mError("failed to read file:%s since %s", file, terrstr());
      goto PARSE_SDB_DATA_ERROR;
    }
  }

  code = 0;

PARSE_SDB_DATA_ERROR:
  taosCloseFile(fd);
  sdbFreeRaw(pRaw);
  terrno = code;
  return code;
}

static int32_t sdbWriteDataFile() {
  char tmpfile[PATH_MAX] = {0};
  snprintf(tmpfile, sizeof(tmpfile), "%ssdb.data", tsSdb.tmpDir);

  FileFd fd = taosOpenFileCreateWrite(tmpfile);
  if (fd <= 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    mError("failed to open file:%s for write since %s", tmpfile, terrstr());
    return -1;
  }

  int32_t code = 0;

  for (int32_t i = SDB_MAX - 1; i > SDB_START; --i) {
    SdbEncodeFp encodeFp = tsSdb.encodeFps[i];
    if (encodeFp == NULL) continue;

    SHashObj *hash = tsSdb.hashObjs[i];
    SRWLatch *pLock = &tsSdb.locks[i];
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
  }

  taosCloseFile(fd);

  if (code == 0) {
    char curfile[PATH_MAX] = {0};
    snprintf(curfile, sizeof(curfile), "%ssdb.data", tsSdb.currDir);
    code = taosRenameFile(tmpfile, curfile);
  }

  if (code != 0) {
    terrno = code;
    mError("failed to write sdb file since %s", terrstr());
  } else {
    mDebug("write sdb file successfully");
  }

  return code;
}

int32_t sdbOpen() {
  mDebug("start to read mnode file");

  if (sdbReadDataFile() != 0) {
    return -1;
  }

  return 0;
}

void sdbClose() {
  if (tsSdb.curVer != tsSdb.lastCommitVer) {
    mDebug("start to write mnode file");
    sdbWriteDataFile();
  }

  for (int32_t i = 0; i < SDB_MAX; ++i) {
    SHashObj *hash = tsSdb.hashObjs[i];
    if (hash != NULL) {
      taosHashClear(hash);
    }
  }
}

int32_t sdbDeploy() {
  if (sdbCreateDir() != 0) {
    return -1;
  }

  if (sdbRunDeployFp() != 0) {
    return -1;
  }

  if (sdbWriteDataFile() != 0) {
    return -1;
  }

  sdbClose();
  return 0;
}

void sdbUnDeploy() {
  mDebug("start to undeploy mnode");
  taosRemoveDir(tsMnodeDir);
}
