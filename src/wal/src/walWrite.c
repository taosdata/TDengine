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
#include "talloc.h"
#include "taoserror.h"
#include "tchecksum.h"
#include "tutil.h"
#include "tqueue.h"
#include "twal.h"
#include "walInt.h"

static int32_t walRestoreWalFile(SWal *pWal, void *pVnode, FWalWrite writeFp, char *name);
static int32_t walGetNextFile(SWal *pWal, int64_t lastFileId, int64_t *nexFileId, char *nextFileName);

int32_t walRenew(void *handle) {
  if (handle == NULL) return 0;

  SWal *  pWal = handle;
  int32_t code = 0;

  pthread_mutex_lock(&pWal->mutex);

  if (pWal->fd >= 0) {
    close(pWal->fd);
    wDebug("vgId:%d, file:%s, it is closed", pWal->vgId, pWal->name);
  }

  int64_t lastId = pWal->fileId;
  if (pWal->keep) {
    pWal->fileId = 0;
  } else {
    pWal->fileId = taosGetTimestampUs();
  }

  snprintf(pWal->name, sizeof(pWal->name), "%s/%s%" PRId64, pWal->path, WAL_PREFIX, pWal->fileId);
  pWal->fd = open(pWal->name, O_WRONLY | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);

  if (pWal->fd < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    wError("vgId:%d, file:%s, failed to open since %s", pWal->vgId, pWal->name, strerror(errno));
  } else {
    wDebug("vgId:%d, file:%s, it is created", pWal->vgId, pWal->name);
  }

  if (!pWal->keep && lastId != -1) {
    // remove last wal file
    char name[TSDB_FILENAME_LEN + 20];
    snprintf(name, sizeof(name), "%s/%s%" PRId64, pWal->path, WAL_PREFIX, lastId);
    if (remove(name) < 0) {
      wError("vgId:%d, file:%s, failed to remove since %s", pWal->vgId, name, strerror(errno));
    } else {
      wDebug("vgId:%d, file:%s, it is removed", pWal->vgId, name);
    }
  }

  pthread_mutex_unlock(&pWal->mutex);

  return code;
}

int32_t walWrite(void *handle, SWalHead *pHead) {
  if (handle == NULL) return -1;

  SWal *  pWal = handle;
  int32_t code = 0;

  // no wal
  if (pWal->level == TAOS_WAL_NOLOG) return 0;
  if (pHead->version <= pWal->version) return 0;

  pHead->signature = WAL_SIGNATURE;
  taosCalcChecksumAppend(0, (uint8_t *)pHead, sizeof(SWalHead));
  int32_t contLen = pHead->len + sizeof(SWalHead);

  if (taosTWrite(pWal->fd, pHead, contLen) != contLen) {
    code = TAOS_SYSTEM_ERROR(errno);
    wError("vgId:%d, file:%s, failed to write since %s", pWal->vgId, pWal->name, strerror(errno));
  } else {
    pWal->version = pHead->version;
    wTrace("vgId:%d, write version:%" PRId64 ", fileId:%" PRId64, pWal->vgId, pWal->version, pWal->fileId);
  }

  ASSERT(contLen == pHead->len + sizeof(SWalHead));

  return code;
}

void walFsync(void *handle) {
  SWal *pWal = handle;
  if (pWal == NULL || pWal->level != TAOS_WAL_FSYNC || pWal->fd < 0) return;

  if (pWal->fsyncPeriod == 0) {
    if (fsync(pWal->fd) < 0) {
      wError("vgId:%d, file:%s, fsync failed since %s", pWal->vgId, pWal->name, strerror(errno));
    }
  }
}

int32_t walRestore(void *handle, void *pVnode, int32_t (*writeFp)(void *, void *, int32_t)) {
  if (handle == NULL) return -1;

  SWal *  pWal = handle;
  int32_t count = 0;

  DIR *dir = opendir(pWal->path);
  if (dir == NULL && errno == ENOENT) return 0;
  if (dir == NULL) return TAOS_SYSTEM_ERROR(errno);

  struct dirent *ent;
  while ((ent = readdir(dir)) != NULL) {
    char *fileName = ent->d_name;

    if (strncmp(fileName, WAL_PREFIX, WAL_PREFIX_LEN) == 0) {
      int64_t fileId = atoll(fileName + WAL_PREFIX_LEN);
      if (fileId == pWal->fileId) continue;

      char walName[WAL_FILE_LEN];
      snprintf(walName, sizeof(pWal->name), "%s/%s%" PRId64, pWal->path, WAL_PREFIX, fileId);

      wDebug("vgId:%d, file:%s, will be restored", pWal->vgId, walName);
      int32_t code = walRestoreWalFile(pWal, pVnode, writeFp, walName);
      if (code != TSDB_CODE_SUCCESS) continue;

      
      if (!pWal->keep) {
        wDebug("vgId:%d, file:%s, restore success, remove this file", pWal->vgId, walName);
        remove(walName);
      } else {
        wDebug("vgId:%d, file:%s, restore success and keep it", pWal->vgId, walName);
      }

      count++;
    }
  }
  closedir(dir);

  if (pWal->keep) {
    if (count == 0) {
      wDebug("vgId:%d, file:%s not exist, renew it", pWal->vgId, pWal->name);
      return walRenew(pWal);
    } else {
      // open the existing WAL file in append mode
      pWal->fileId = 0;
      snprintf(pWal->name, sizeof(pWal->name), "%s/%s%" PRId64, pWal->path, WAL_PREFIX, pWal->fileId);
      pWal->fd = open(pWal->name, O_WRONLY | O_CREAT | O_APPEND, S_IRWXU | S_IRWXG | S_IRWXO);
      if (pWal->fd < 0) {
        wError("vgId:%d, file:%s, failed to open since %s", pWal->vgId, pWal->name, strerror(errno));
        return TAOS_SYSTEM_ERROR(errno);
      }
      wDebug("vgId:%d, file:%s open success", pWal->vgId, pWal->name);
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t walGetWalFile(void *handle, char *fileName, int64_t *fileId) {
  if (handle == NULL) return -1;
  SWal *pWal = handle;

  pthread_mutex_lock(&(pWal->mutex));
  int32_t code = walGetNextFile(pWal, *fileId, fileId, fileName);
  if (code == 0) {
    code = (*fileId == pWal->fileId) ? 0 : 1;
  }
  pthread_mutex_unlock(&(pWal->mutex));

  return code;
}

static int32_t walRestoreWalFile(SWal *pWal, void *pVnode, FWalWrite writeFp, char *name) {
  int32_t size = WAL_MAX_SIZE;
  void *  buffer = tmalloc(size);
  if (buffer == NULL) {
    wError("vgId:%d, file:%s, failed to open for restore since %s", pWal->vgId, name, strerror(errno));
    return TAOS_SYSTEM_ERROR(errno);
  }

  int32_t fd = open(name, O_RDWR);
  if (fd < 0) {
    wError("vgId:%d, file:%s, failed to open for restore since %s", pWal->vgId, name, strerror(errno));
    tfree(buffer);
    return TAOS_SYSTEM_ERROR(errno);
  }

  wDebug("vgId:%d, file:%s, start to restore", pWal->vgId, name);

  int32_t   code = TSDB_CODE_SUCCESS;
  size_t    offset = 0;
  SWalHead *pHead = buffer;

  while (1) {
    int32_t ret = taosTRead(fd, pHead, sizeof(SWalHead));
    if (ret == 0) break;

    if (ret < 0) {
      wError("vgId:%d, file:%s, failed to read wal head part since %s", pWal->vgId, name, strerror(errno));
      code = TAOS_SYSTEM_ERROR(errno);
      break;
    }

    if (ret < sizeof(SWalHead)) {
      wError("vgId:%d, file:%s, failed to read wal head since %s, read size:%d, skip the rest of file", pWal->vgId,
             name, strerror(errno), ret);
      taosFtruncate(fd, offset);
      fsync(fd);
      break;
    }

    if (!taosCheckChecksumWhole((uint8_t *)pHead, sizeof(SWalHead))) {
      wError("vgId:%d, file:%s, wal head cksum is messed up, skip the rest of file", pWal->vgId, name);
      code = TSDB_CODE_WAL_FILE_CORRUPTED;
      ASSERT(false);
      break;
    }

    if (pHead->len > size - sizeof(SWalHead)) {
      size = sizeof(SWalHead) + pHead->len;
      buffer = realloc(buffer, size);
      if (buffer == NULL) {
        wError("vgId:%d, file:%s, failed to open for restore since %s", pWal->vgId, name, strerror(errno));
        code = TAOS_SYSTEM_ERROR(errno);
        break;
      }

      pHead = buffer;
    }

    ret = taosTRead(fd, pHead->cont, pHead->len);
    if (ret < 0) {
      wError("vgId:%d, file:%s failed to read wal body part since %s", pWal->vgId, name, strerror(errno));
      code = TAOS_SYSTEM_ERROR(errno);
      break;
    }

    if (ret < pHead->len) {
      wError("vgId:%d, file:%s, failed to read body since %s, read size:%d len:%d , skip the rest of file", pWal->vgId,
             name, strerror(errno), ret, pHead->len);
      taosFtruncate(fd, offset);
      fsync(fd);
      break;
    }

    offset = offset + sizeof(SWalHead) + pHead->len;

    if (pWal->keep) pWal->version = pHead->version;

    wTrace("vgId:%d, restore version:%" PRIu64 ", fileId:%" PRId64, pWal->vgId, pWal->version, pWal->fileId);

    (*writeFp)(pVnode, pHead, TAOS_QTYPE_WAL);
  }

  close(fd);
  tfree(buffer);

  return code;
}

int64_t walGetVersion(twalh param) {
  SWal *pWal = param;
  if (pWal == 0) return 0;

  return pWal->version;
}

static int32_t walGetNextFile(SWal *pWal, int64_t lastFileId, int64_t *nexFileId, char *nextFileName) {
  int64_t nearFileId = INT64_MAX;
  char    nearFileName[WAL_FILE_LEN] = {0};

  DIR *dir = opendir(pWal->path);
  if (dir == NULL) {
    wError("vgId:%d, path:%s, failed to open since %s", pWal->vgId, pWal->path, strerror(errno));
    return -1;
  }

  struct dirent *ent;
  while ((ent = readdir(dir)) != NULL) {
    char *fileName = ent->d_name;

    if (strncmp(fileName, WAL_PREFIX, WAL_PREFIX_LEN) == 0) {
      int64_t fileId = atoll(fileName + WAL_PREFIX_LEN);
      if (fileId <= lastFileId) continue;

      if (fileId < nearFileId) {
        nearFileId = fileId;
        tstrncpy(nearFileName, fileName, WAL_FILE_LEN);
      }
    }
  }
  closedir(dir);

  if (nearFileId == INT64_MAX) return -1;

  *nexFileId = nearFileId;
  tstrncpy(nextFileName, nearFileName, WAL_FILE_LEN);
  wTrace("vgId:%d, path:%s, lastfile %" PRId64 ", nextfile is %s", pWal->vgId, pWal->path, lastFileId, nextFileName);

  return 0;
}
