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
#define TAOS_RANDOM_FILE_FAIL_TEST
#include "os.h"
#include "taoserror.h"
#include "tchecksum.h"
#include "tfile.h"
#include "twal.h"
#include "walInt.h"

static int32_t walRestoreWalFile(SWal *pWal, void *pVnode, FWalWrite writeFp, char *name, int64_t fileId);

int32_t walRenew(void *handle) {
  if (handle == NULL) return 0;

  SWal *  pWal = handle;
  int32_t code = 0;

  if (pWal->stop) {
    wDebug("vgId:%d, do not create a new wal file", pWal->vgId);
    return 0;
  }

  pthread_mutex_lock(&pWal->mutex);

  if (tfValid(pWal->tfd)) {
    tfClose(pWal->tfd);
    wDebug("vgId:%d, file:%s, it is closed", pWal->vgId, pWal->name);
  }

  if (pWal->keep == TAOS_WAL_KEEP) {
    pWal->fileId = 0;
  } else {
    if (walGetNewFile(pWal, &pWal->fileId) != 0) pWal->fileId = 0;
    pWal->fileId++;
  }

  snprintf(pWal->name, sizeof(pWal->name), "%s/%s%" PRId64, pWal->path, WAL_PREFIX, pWal->fileId);
  pWal->tfd = tfOpenM(pWal->name, O_WRONLY | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);

  if (!tfValid(pWal->tfd)) {
    code = TAOS_SYSTEM_ERROR(errno);
    wError("vgId:%d, file:%s, failed to open since %s", pWal->vgId, pWal->name, strerror(errno));
  } else {
    wDebug("vgId:%d, file:%s, it is created", pWal->vgId, pWal->name);
  }

  pthread_mutex_unlock(&pWal->mutex);

  return code;
}

void walRemoveOneOldFile(void *handle) {
  SWal *pWal = handle;
  if (pWal == NULL) return;
  if (pWal->keep == TAOS_WAL_KEEP) return;
  if (!tfValid(pWal->tfd)) return;

  pthread_mutex_lock(&pWal->mutex);

  // remove the oldest wal file
  int64_t oldFileId = -1;
  if (walGetOldFile(pWal, pWal->fileId, WAL_FILE_NUM, &oldFileId) == 0) {
    char walName[WAL_FILE_LEN] = {0};
    snprintf(walName, sizeof(walName), "%s/%s%" PRId64, pWal->path, WAL_PREFIX, oldFileId);

    if (remove(walName) < 0) {
      wError("vgId:%d, file:%s, failed to remove since %s", pWal->vgId, walName, strerror(errno));
    } else {
      wInfo("vgId:%d, file:%s, it is removed", pWal->vgId, walName);
    }
  }

  pthread_mutex_unlock(&pWal->mutex);
}

void walRemoveAllOldFiles(void *handle) {
  if (handle == NULL) return;

  SWal *  pWal = handle;
  int64_t fileId = -1;

  pthread_mutex_lock(&pWal->mutex);
  while (walGetNextFile(pWal, &fileId) >= 0) {
    snprintf(pWal->name, sizeof(pWal->name), "%s/%s%" PRId64, pWal->path, WAL_PREFIX, fileId);

    if (remove(pWal->name) < 0) {
      wError("vgId:%d, wal:%p file:%s, failed to remove", pWal->vgId, pWal, pWal->name);
    } else {
      wInfo("vgId:%d, wal:%p file:%s, it is removed", pWal->vgId, pWal, pWal->name);
    }
  }
  pthread_mutex_unlock(&pWal->mutex);
}

int32_t walWrite(void *handle, SWalHead *pHead) {
  if (handle == NULL) return -1;

  SWal *  pWal = handle;
  int32_t code = 0;

  // no wal
  if (!tfValid(pWal->tfd)) return 0;
  if (pWal->level == TAOS_WAL_NOLOG) return 0;
  if (pHead->version <= pWal->version) return 0;

  pHead->signature = WAL_SIGNATURE;
  taosCalcChecksumAppend(0, (uint8_t *)pHead, sizeof(SWalHead));
  int32_t contLen = pHead->len + sizeof(SWalHead);

  pthread_mutex_lock(&pWal->mutex);

  if (tfWrite(pWal->tfd, pHead, contLen) != contLen) {
    code = TAOS_SYSTEM_ERROR(errno);
    wError("vgId:%d, file:%s, failed to write since %s", pWal->vgId, pWal->name, strerror(errno));
  } else {
    wTrace("vgId:%d, write wal, fileId:%" PRId64 " tfd:%" PRId64 " hver:%" PRId64 " wver:%" PRIu64 " len:%d", pWal->vgId,
           pWal->fileId, pWal->tfd, pHead->version, pWal->version, pHead->len);
    pWal->version = pHead->version;
  }

  pthread_mutex_unlock(&pWal->mutex);

  ASSERT(contLen == pHead->len + sizeof(SWalHead));

  return code;
}

void walFsync(void *handle, bool forceFsync) {
  SWal *pWal = handle;
  if (pWal == NULL || !tfValid(pWal->tfd)) return;

  if (forceFsync || (pWal->level == TAOS_WAL_FSYNC && pWal->fsyncPeriod == 0)) {
    wTrace("vgId:%d, fileId:%" PRId64 ", do fsync", pWal->vgId, pWal->fileId);
    if (tfFsync(pWal->tfd) < 0) {
      wError("vgId:%d, fileId:%" PRId64 ", fsync failed since %s", pWal->vgId, pWal->fileId, strerror(errno));
    }
  }
}

int32_t walRestore(void *handle, void *pVnode, FWalWrite writeFp) {
  if (handle == NULL) return -1;

  SWal *  pWal = handle;
  int32_t count = 0;
  int32_t code = 0;
  int64_t fileId = -1;

  while ((code = walGetNextFile(pWal, &fileId)) >= 0) {
    if (fileId == pWal->fileId) continue;

    char walName[WAL_FILE_LEN];
    snprintf(walName, sizeof(pWal->name), "%s/%s%" PRId64, pWal->path, WAL_PREFIX, fileId);

    wInfo("vgId:%d, file:%s, will be restored", pWal->vgId, walName);
    int32_t code = walRestoreWalFile(pWal, pVnode, writeFp, walName, fileId);
    if (code != TSDB_CODE_SUCCESS) {
      wError("vgId:%d, file:%s, failed to restore since %s", pWal->vgId, walName, tstrerror(code));
      continue;
    }

    wInfo("vgId:%d, file:%s, restore success", pWal->vgId, walName);

    count++;
  }

  if (pWal->keep != TAOS_WAL_KEEP) return TSDB_CODE_SUCCESS;

  if (count == 0) {
    wDebug("vgId:%d, wal file not exist, renew it", pWal->vgId);
    return walRenew(pWal);
  } else {
    // open the existing WAL file in append mode
    pWal->fileId = 0;
    snprintf(pWal->name, sizeof(pWal->name), "%s/%s%" PRId64, pWal->path, WAL_PREFIX, pWal->fileId);
    pWal->tfd = tfOpenM(pWal->name, O_WRONLY | O_CREAT | O_APPEND, S_IRWXU | S_IRWXG | S_IRWXO);
    if (!tfValid(pWal->tfd)) {
      wError("vgId:%d, file:%s, failed to open since %s", pWal->vgId, pWal->name, strerror(errno));
      return TAOS_SYSTEM_ERROR(errno);
    }
    wDebug("vgId:%d, file:%s open success", pWal->vgId, pWal->name);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t walGetWalFile(void *handle, char *fileName, int64_t *fileId) {
  if (handle == NULL) return -1;
  SWal *pWal = handle;

  if (*fileId == 0) *fileId = -1;

  pthread_mutex_lock(&(pWal->mutex));

  int32_t code = walGetNextFile(pWal, fileId);
  if (code >= 0) {
    sprintf(fileName, "wal/%s%" PRId64, WAL_PREFIX, *fileId);
    code = (*fileId == pWal->fileId) ? 0 : 1;
  }

  wDebug("vgId:%d, get wal file, code:%d curId:%" PRId64 " outId:%" PRId64, pWal->vgId, code, pWal->fileId, *fileId);
  pthread_mutex_unlock(&(pWal->mutex));

  return code;
}

static void walFtruncate(SWal *pWal, int64_t tfd, int64_t offset) {
  tfFtruncate(tfd, offset);
  tfFsync(tfd);
}

static int32_t walSkipCorruptedRecord(SWal *pWal, SWalHead *pHead, int64_t tfd, int64_t *offset) {
  int64_t pos = *offset;
  while (1) {
    pos++;

    if (tfLseek(tfd, pos, SEEK_SET) < 0) {
      wError("vgId:%d, failed to seek from corrupted wal file since %s", pWal->vgId, strerror(errno));
      return TSDB_CODE_WAL_FILE_CORRUPTED;
    }

    if (tfRead(tfd, pHead, sizeof(SWalHead)) <= 0) {
      wError("vgId:%d, read to end of corrupted wal file, offset:%" PRId64, pWal->vgId, pos);
      return TSDB_CODE_WAL_FILE_CORRUPTED;
    }

    if (pHead->signature != WAL_SIGNATURE) {
      continue;
    }

    if (taosCheckChecksumWhole((uint8_t *)pHead, sizeof(SWalHead))) {
      wInfo("vgId:%d, wal head cksum check passed, offset:%" PRId64, pWal->vgId, pos);
      *offset = pos;
      return TSDB_CODE_SUCCESS;
    }
  }

  return TSDB_CODE_WAL_FILE_CORRUPTED;
}

static int32_t walRestoreWalFile(SWal *pWal, void *pVnode, FWalWrite writeFp, char *name, int64_t fileId) {
  int32_t size = WAL_MAX_SIZE;
  void *  buffer = tmalloc(size);
  if (buffer == NULL) {
    wError("vgId:%d, file:%s, failed to open for restore since %s", pWal->vgId, name, strerror(errno));
    return TAOS_SYSTEM_ERROR(errno);
  }

  int64_t tfd = tfOpen(name, O_RDWR);
  if (!tfValid(tfd)) {
    wError("vgId:%d, file:%s, failed to open for restore since %s", pWal->vgId, name, strerror(errno));
    tfree(buffer);
    return TAOS_SYSTEM_ERROR(errno);
  }

  wDebug("vgId:%d, file:%s, start to restore", pWal->vgId, name);

  int32_t   code = TSDB_CODE_SUCCESS;
  int64_t   offset = 0;
  SWalHead *pHead = buffer;

  while (1) {
    int32_t ret = tfRead(tfd, pHead, sizeof(SWalHead));
    if (ret == 0) break;

    if (ret < 0) {
      wError("vgId:%d, file:%s, failed to read wal head since %s", pWal->vgId, name, strerror(errno));
      code = TAOS_SYSTEM_ERROR(errno);
      break;
    }

    if (ret < sizeof(SWalHead)) {
      wError("vgId:%d, file:%s, failed to read wal head, ret is %d", pWal->vgId, name, ret);
      walFtruncate(pWal, tfd, offset);
      break;
    }

    if (!taosCheckChecksumWhole((uint8_t *)pHead, sizeof(SWalHead))) {
      wError("vgId:%d, file:%s, wal head cksum is messed up, hver:%" PRIu64 " len:%d offset:%" PRId64, pWal->vgId, name,
             pHead->version, pHead->len, offset);
      code = walSkipCorruptedRecord(pWal, pHead, tfd, &offset);
      if (code != TSDB_CODE_SUCCESS) {
        walFtruncate(pWal, tfd, offset);
        break;
      }
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

    ret = tfRead(tfd, pHead->cont, pHead->len);
    if (ret < 0) {
      wError("vgId:%d, file:%s, failed to read wal body since %s", pWal->vgId, name, strerror(errno));
      code = TAOS_SYSTEM_ERROR(errno);
      break;
    }

    if (ret < pHead->len) {
      wError("vgId:%d, file:%s, failed to read wal body, ret:%d len:%d", pWal->vgId, name, ret, pHead->len);
      offset += sizeof(SWalHead);
      continue;
    }

    offset = offset + sizeof(SWalHead) + pHead->len;

    wTrace("vgId:%d, restore wal, fileId:%" PRId64 " hver:%" PRIu64 " wver:%" PRIu64 " len:%d", pWal->vgId,
           fileId, pHead->version, pWal->version, pHead->len);

    pWal->version = pHead->version;
    (*writeFp)(pVnode, pHead, TAOS_QTYPE_WAL, NULL);
  }

  tfClose(tfd);
  tfree(buffer);

  return code;
}

uint64_t walGetVersion(twalh param) {
  SWal *pWal = param;
  if (pWal == 0) return 0;

  return pWal->version;
}
