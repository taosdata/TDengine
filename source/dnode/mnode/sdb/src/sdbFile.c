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


static int32_t sdbCreateDir() {
  if (!taosMkDir(tsSdb.currDir)) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    mError("failed to create dir:%s since %s", tsSdb.currDir, terrstr());
    return -1;
  }

  if (!taosMkDir(tsSdb.syncDir)) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    mError("failed to create dir:%s since %s", tsSdb.syncDir, terrstr());
    return -1;
  }

  if (!taosMkDir(tsSdb.tmpDir)) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    mError("failed to create dir:%s since %s", tsSdb.tmpDir, terrstr());
    return -1;
  }

  return 0;
}

static int32_t sdbRunDeployFp() {
  for (int32_t i = SDB_START; i < SDB_MAX; ++i) {
    SdbDeployFp fp = tsSdb.deployFps[i];
    if (fp == NULL) continue;
    if ((*fp)() != 0) {
      mError("failed to deploy sdb:%d since %s", i, terrstr());
      return -1;
    }
  }

  return 0;
}

static int32_t sdbWriteVersion(FileFd fd) { return 0; }

static int32_t sdbReadVersion(FileFd fd) { return 0; }

static int32_t sdbReadDataFile() {
  int32_t code = 0;

  SSdbRaw *pRaw = malloc(SDB_MAX_SIZE);
  if (pRaw == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
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
      code = TSDB_CODE_SDB_APP_ERROR;
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
      SSdbRaw *pRaw = (*encodeFp)(pRow->pObj);
      if (pRaw != NULL) {
        taosWriteFile(fd, pRaw, sizeof(SSdbRaw) + pRaw->dataLen);
      } else {
        taosHashCancelIterate(hash, pRow);
        code = TSDB_CODE_SDB_APP_ERROR;
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
