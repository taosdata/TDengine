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
#include "os.h"
#include "thash.h"
#include "tglobal.h"
#include "cJSON.h"
#include "mnodeSdb.h"

static struct {
  char        currDir[PATH_MAX];
  char        backDir[PATH_MAX];
  char        tmpDir[PATH_MAX];
  int64_t     version;
  EMnKey      hashKey[MN_SDB_MAX];
  int32_t     dataSize[MN_SDB_MAX];
  SHashObj   *hashObj[MN_SDB_MAX];
  SdbDeployFp deployFp[MN_SDB_MAX];
  SdbEncodeFp encodeFp[MN_SDB_MAX];
  SdbDecodeFp decodeFp[MN_SDB_MAX];
} tsSdb = {0};

static int32_t sdbCreateDir() {
  if (!taosMkDir(tsSdb.currDir)) {
    mError("failed to create dir:%s", tsSdb.currDir);
    return TAOS_SYSTEM_ERROR(errno);
  }

  if (!taosMkDir(tsSdb.backDir)) {
    mError("failed to create dir:%s", tsSdb.backDir);
    return -1;
  }

  if (!taosMkDir(tsSdb.tmpDir)) {
    mError("failed to create dir:%s", tsSdb.tmpDir);
    return -1;
  }

  return 0;
}

static int32_t sdbRunDeployFp() {
  for (int32_t i = MN_SDB_START; i < MN_SDB_MAX; ++i) {
    SdbDeployFp fp = tsSdb.deployFp[i];
    if (fp) {
      (*fp)();
    }
  }

  return 0;
}

static int32_t sdbReadVersion(cJSON *root) {
  cJSON *ver = cJSON_GetObjectItem(root, "version");
  if (!ver || ver->type != cJSON_String) {
    mError("failed to parse version since version not found");
    return -1;
  }

  tsSdb.version = (int64_t)atoll(ver->valuestring);
  mTrace("parse version success, version:%" PRIu64, tsSdb.version);

  return 0;
}

static void sdbWriteVersion(FileFd fd) {
  char    content[128];
  int32_t len =
      snprintf(content, sizeof(content), "{\"type\":0, \"version\":\"%" PRIu64 "\", \"updateTime\":\"%" PRIu64 "\"}\n",
               tsSdb.version, taosGetTimestampMs());
  taosWriteFile(fd, content, len);
}

static int32_t sdbReadDataFile() {
  ssize_t _bytes = 0;
  size_t  len = 4096;
  char   *line = calloc(1, len);
  int32_t code = -1;
  FILE   *fp = NULL;
  cJSON  *root = NULL;

  char file[PATH_MAX + 20];
  snprintf(file, sizeof(file), "%ssdb.data", tsSdb.currDir);
  fp = fopen(file, "r");
  if (!fp) {
    mError("failed to open file:%s for read since %s", file, strerror(errno));
    goto PARSE_SDB_DATA_ERROR;
  }

  while (!feof(fp)) {
    memset(line, 0, len);
    _bytes = tgetline(&line, &len, fp);
    if (_bytes < 0) {
      break;
    }

    line[len - 1] = 0;
    if (len <= 10) continue;

    root = cJSON_Parse(line);
    if (root == NULL) {
      mError("failed to parse since invalid json format, %s", line);
      goto PARSE_SDB_DATA_ERROR;
    }

    cJSON *type = cJSON_GetObjectItem(root, "type");
    if (!type || type->type != cJSON_Number) {
      mError("failed to parse since invalid type not found, %s", line);
      goto PARSE_SDB_DATA_ERROR;
    }

    if (type->valueint >= MN_SDB_MAX || type->valueint < MN_SDB_START) {
      mError("failed to parse since invalid type, %s", line);
      goto PARSE_SDB_DATA_ERROR;
    }

    if (type->valueint == MN_SDB_START) {
      if (sdbReadVersion(root) != 0) {
        mError("failed to parse version, %s", line);
        goto PARSE_SDB_DATA_ERROR;
      }
      cJSON_Delete(root);
      root = NULL;
      continue;
    }

    SdbDecodeFp func = tsSdb.decodeFp[type->valueint];
    SdbHead    *pHead = (*func)(root);
    if (pHead == NULL) {
      mError("failed to parse since decode error, %s", line);
      goto PARSE_SDB_DATA_ERROR;
    }

    sdbInsertRow(pHead->type, pHead);
    cJSON_Delete(root);
    root = NULL;
  }

  code = 0;

PARSE_SDB_DATA_ERROR:
  tfree(line);
  fclose(fp);
  cJSON_Delete(root);

  return code;
}

static int32_t sdbWriteDataFile() {
  char file[PATH_MAX + 20] = {0};
  snprintf(file, sizeof(file), "%ssdb.data", tsSdb.currDir);
  FileFd fd = taosOpenFileCreateWrite(file);
  if (fd <= 0) {
    mError("failed to open file:%s for write since %s", file, strerror(errno));
    return -1;
  }

  int32_t len;
  int32_t maxLen = 10240;
  char   *buf = malloc(maxLen);

  for (int32_t i = MN_SDB_START; i < MN_SDB_MAX; ++i) {
    SHashObj *hash = tsSdb.hashObj[i];
    if (!hash) continue;

    SdbEncodeFp fp = tsSdb.encodeFp[i];
    if (!fp) continue;

    SdbHead *pHead = taosHashIterate(hash, NULL);
    while (pHead != NULL) {
      len = (*fp)(pHead, buf, maxLen);
      if (len >= 0) {
        taosWriteFile(fd, buf, len);
      }

      pHead = taosHashIterate(hash, pHead);
    }
  }

  sdbWriteVersion(fd);
  taosFsyncFile(fd);
  taosCloseFile(fd);

  mInfo("write file:%s successfully", file);
  return 0;
}

int32_t sdbCommit() {
  int32_t code = sdbWriteDataFile();
  if (code != 0) {
    return code;
  }

  return 0;
}

int32_t sdbRead() {
  int32_t code = sdbReadDataFile();
  if (code != 0) {
    return code;
  }

  mInfo("read sdb file successfully");
  return -1;
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

  // if (!taosMkDir())
  // if (pMinfos == NULL) {  // first deploy
  //   tsMint.dnodeId = 1;
  //   bool getuid = taosGetSystemUid(tsMint.clusterId);
  //   if (!getuid) {
  //     strcpy(tsMint.clusterId, "tdengine3.0");
  //     mError("deploy new mnode but failed to get uid, set to default val %s", tsMint.clusterId);
  //   } else {
  //     mDebug("deploy new mnode and uid is %s", tsMint.clusterId);
  //   }
  // } else {  // todo
  // }

  // if (mkdir(tsMnodeDir, 0755) != 0 && errno != EEXIST) {
  //   mError("failed to init mnode dir:%s, reason:%s", tsMnodeDir, strerror(errno));
  //   return -1;
  // }
  return 0;
}

void sdbUnDeploy() {}

int32_t sdbInit() {
  snprintf(tsSdb.currDir, PATH_MAX, "%s%scurrent%s", tsMnodeDir, TD_DIRSEP, TD_DIRSEP);
  snprintf(tsSdb.backDir, PATH_MAX, "%s%sbackup%s", tsMnodeDir, TD_DIRSEP, TD_DIRSEP);
  snprintf(tsSdb.tmpDir, PATH_MAX, "%s%stmp%s", tsMnodeDir, TD_DIRSEP, TD_DIRSEP);

  for (int32_t i = 0; i < MN_SDB_MAX; ++i) {
    int32_t type;
    if (tsSdb.hashKey[i] == MN_KEY_INT32) {
      type = TSDB_DATA_TYPE_INT;
    } else if (tsSdb.hashKey[i] == MN_KEY_INT64) {
      type = TSDB_DATA_TYPE_BIGINT;
    } else {
      type = TSDB_DATA_TYPE_BINARY;
    }

    SHashObj *hash = taosHashInit(128, taosGetDefaultHashFunction(type), true, HASH_NO_LOCK);
    if (hash == NULL) {
      return -1;
    }

    tsSdb.hashObj[i] = hash;
  }

  return 0;
}

void sdbCleanup() {
  for (int32_t i = 0; i < MN_SDB_MAX; ++i) {
    SHashObj *hash = tsSdb.hashObj[i];
    if (hash != NULL) {
      taosHashCleanup(hash);
    }
    tsSdb.hashObj[i] = NULL;
  }
}

void sdbSetFp(EMnSdb sdb, EMnKey keyType, SdbDeployFp deployFp, SdbEncodeFp encodeFp, SdbDecodeFp decodeFp,
              int32_t dataSize) {
  tsSdb.deployFp[sdb] = deployFp;
  tsSdb.encodeFp[sdb] = encodeFp;
  tsSdb.decodeFp[sdb] = decodeFp;
  tsSdb.dataSize[sdb] = dataSize;
  tsSdb.hashKey[sdb] = keyType;
}

static SHashObj *sdbGetHash(int32_t sdb) {
  if (sdb >= MN_SDB_MAX || sdb <= MN_SDB_START) {
    return NULL;
  }

  SHashObj *hash = tsSdb.hashObj[sdb];
  if (hash == NULL) {
    return NULL;
  }

  return hash;
}

void *sdbInsertRow(EMnSdb sdb, void *p) {
  SdbHead *pHead = p;
  pHead->type = sdb;
  pHead->status = MN_SDB_STAT_AVAIL;

  char   *pKey = (char *)pHead + sizeof(pHead);
  int32_t keySize;
  EMnKey  keyType = tsSdb.hashKey[pHead->type];
  int32_t dataSize = tsSdb.dataSize[pHead->type];

  SHashObj *hash = sdbGetHash(pHead->type);
  if (hash == NULL) {
    return NULL;
  }

  if (keyType == MN_KEY_INT32) {
    keySize = sizeof(int32_t);
  } else if (keyType == MN_KEY_BINARY) {
    keySize = strlen(pKey) + 1;
  } else {
    keySize = sizeof(int64_t);
  }

  taosHashPut(hash, pKey, keySize, pHead, dataSize);
  return taosHashGet(hash, pKey, keySize);
}

void sdbDeleteRow(EMnSdb sdb, void *p) {
  SdbHead *pHead = p;
  pHead->status = MN_SDB_STAT_DROPPED;
}

void *sdbUpdateRow(EMnSdb sdb, void *pHead) { return sdbInsertRow(sdb, pHead); }

void *sdbGetRow(EMnSdb sdb, void *pKey) {
  SHashObj *hash = sdbGetHash(sdb);
  if (hash == NULL) {
    return NULL;
  }

  int32_t keySize;
  EMnKey  keyType = tsSdb.hashKey[sdb];

  if (keyType == MN_KEY_INT32) {
    keySize = sizeof(int32_t);
  } else if (keyType == MN_KEY_BINARY) {
    keySize = strlen(pKey) + 1;
  } else {
    keySize = sizeof(int64_t);
  }

  return taosHashGet(hash, pKey, keySize);
}

void *sdbFetchRow(EMnSdb sdb, void *pIter) {
  SHashObj *hash = sdbGetHash(sdb);
  if (hash == NULL) {
    return NULL;
  }

  return taosHashIterate(hash, pIter);
}

void sdbCancelFetch(EMnSdb sdb, void *pIter) {
  SHashObj *hash = sdbGetHash(sdb);
  if (hash == NULL) {
    return;
  }

  taosHashCancelIterate(hash, pIter);
}

int32_t sdbGetCount(EMnSdb sdb) {
  SHashObj *hash = sdbGetHash(sdb);
  if (hash == NULL) {
    return 0;
  }
  return taosHashGetSize(hash);
}