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

static int32_t walRestoreWalFile(SWal *pWal, void *pVnode, FWalWrite writeFp);
static int32_t walRemoveWalFiles(const char *path);

int32_t walRenew(void *handle) {
  if (handle == NULL) return 0;
  SWal *pWal = handle;

  terrno = 0;

  pthread_mutex_lock(&pWal->mutex);

  if (pWal->fd >= 0) {
    close(pWal->fd);
    pWal->id++;
    wDebug("vgId:%d, wal:%s, it is closed", pWal->vgId, pWal->name);
  }

  pWal->num++;

  snprintf(pWal->name, sizeof(pWal->name), "%s/%s%d", pWal->path, walPrefix, pWal->id);
  pWal->fd = open(pWal->name, O_WRONLY | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);

  if (pWal->fd < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    wError("vgId:%d, wal:%s, failed to open, reason:%s", pWal->vgId, pWal->name, strerror(errno));
  } else {
    wDebug("vgId:%d, wal:%s, it is created", pWal->vgId, pWal->name);

    if (pWal->num > pWal->max) {
      // remove the oldest wal file
      char name[TSDB_FILENAME_LEN * 3];
      snprintf(name, sizeof(name), "%s/%s%d", pWal->path, walPrefix, pWal->id - pWal->max);
      if (remove(name) < 0) {
        wError("vgId:%d, wal:%s, failed to remove(%s)", pWal->vgId, name, strerror(errno));
      } else {
        wDebug("vgId:%d, wal:%s, it is removed", pWal->vgId, name);
      }

      pWal->num--;
    }
  }

  pthread_mutex_unlock(&pWal->mutex);

  return terrno;
}

int32_t walWrite(void *handle, SWalHead *pHead) {
  SWal *pWal = handle;
  if (pWal == NULL) return -1;

  terrno = 0;

  // no wal
  if (pWal->level == TAOS_WAL_NOLOG) return 0;
  if (pHead->version <= pWal->version) return 0;

  pHead->signature = walSignature;
  taosCalcChecksumAppend(0, (uint8_t *)pHead, sizeof(SWalHead));
  int32_t contLen = pHead->len + sizeof(SWalHead);

  if (taosTWrite(pWal->fd, pHead, contLen) != contLen) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    wError("vgId:%d, wal:%s, failed to write(%s)", pWal->vgId, pWal->name, strerror(errno));
    return terrno;
  } else {
    pWal->version = pHead->version;
  }
  ASSERT(contLen == pHead->len + sizeof(SWalHead));

  return 0;
}

void walFsync(void *handle) {
  SWal *pWal = handle;
  if (pWal == NULL || pWal->level != TAOS_WAL_FSYNC || pWal->fd < 0) return;

  if (pWal->fsyncPeriod == 0) {
    if (fsync(pWal->fd) < 0) {
      wError("vgId:%d, wal:%s, fsync failed(%s)", pWal->vgId, pWal->name, strerror(errno));
    }
  }
}

int32_t walRestore(void *handle, void *pVnode, int32_t (*writeFp)(void *, void *, int32_t)) {
  SWal *   pWal  = handle;
  int32_t  count = 0;
  uint32_t maxId = 0;
  uint32_t minId = -1;
  uint32_t index = 0;
  int32_t  code  = 0;
  struct dirent *ent;

  terrno = 0;
  int32_t plen = strlen(walPrefix);
  char opath[TSDB_FILENAME_LEN + 5];
  snprintf(opath, sizeof(opath), "%s", pWal->path);

  DIR *dir = opendir(opath);
  if (dir == NULL && errno == ENOENT) return 0;
  if (dir == NULL) {
    code = TAOS_SYSTEM_ERROR(errno);
    return code;
  }

  while ((ent = readdir(dir)) != NULL) {
    if (strncmp(ent->d_name, walPrefix, plen) == 0) {
      index = atol(ent->d_name + plen);
      if (index > maxId) maxId = index;
      if (index < minId) minId = index;
      count++;
    }
  }

  closedir(dir);
  pWal->fileIndex = maxId;

  if (count == 0) {
    if (pWal->keep) terrno = walRenew(pWal);
    return terrno;
  }

  if (count != (maxId - minId + 1)) {
    wError("vgId:%d, wal:%s, messed up, count:%d max:%d min:%d", pWal->vgId, opath, count, maxId, minId);
    terrno = TSDB_CODE_WAL_APP_ERROR;
  } else {
    wDebug("vgId:%d, wal:%s, %d files will be restored", pWal->vgId, opath, count);

    for (index = minId; index <= maxId; ++index) {
      snprintf(pWal->name, sizeof(pWal->name), "%s/%s%d", opath, walPrefix, index);
      terrno = walRestoreWalFile(pWal, pVnode, writeFp);
      if (terrno < 0) continue;
    }
  }

  if (terrno == 0) {
    if (pWal->keep == 0) {
      terrno = walRemoveWalFiles(opath);
      if (terrno == 0) {
        if (remove(opath) < 0) {
          wError("vgId:%d, wal:%s, failed to remove directory, reason:%s", pWal->vgId, opath, strerror(errno));
          terrno = TAOS_SYSTEM_ERROR(errno);
        }
      }
    } else {
      // open the existing WAL file in append mode
      pWal->num = count;
      pWal->id = maxId;
      snprintf(pWal->name, sizeof(pWal->name), "%s/%s%d", opath, walPrefix, maxId);
      pWal->fd = open(pWal->name, O_WRONLY | O_CREAT | O_APPEND, S_IRWXU | S_IRWXG | S_IRWXO);
      if (pWal->fd < 0) {
        wError("vgId:%d, wal:%s, failed to open file, reason:%s", pWal->vgId, pWal->name, strerror(errno));
        terrno = TAOS_SYSTEM_ERROR(errno);
      }
    }
  }

  return terrno;
}

int32_t walGetWalFile(void *handle, char *name, uint32_t *index) {
  SWal *  pWal = handle;
  int32_t     code = 1;
  int32_t first = 0;

  name[0] = 0;
  if (pWal == NULL || pWal->num == 0) return 0;

  pthread_mutex_lock(&(pWal->mutex));

  first = pWal->id + 1 - pWal->num;
  if (*index == 0) *index = first;  // set to first one

  if (*index < first && *index > pWal->id) {
    code = -1;  // index out of range
  } else {
    sprintf(name, "wal/%s%d", walPrefix, *index);
    code = (*index == pWal->id) ? 0 : 1;
  }

  pthread_mutex_unlock(&(pWal->mutex));

  return code;
}

static int32_t walRestoreWalFile(SWal *pWal, void *pVnode, FWalWrite writeFp) {
  char *  name = pWal->name;
  int32_t size = 1024 * 1024;  // default 1M buffer size

  terrno = 0;
  char *buffer = malloc(size);
  if (buffer == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return terrno;
  }

  SWalHead *pHead = (SWalHead *)buffer;

  int32_t fd = open(name, O_RDWR);
  if (fd < 0) {
    wError("vgId:%d, wal:%s, failed to open for restore(%s)", pWal->vgId, name, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    free(buffer);
    return terrno;
  }

  wDebug("vgId:%d, wal:%s, start to restore", pWal->vgId, name);

  size_t offset = 0;
  while (1) {
    int32_t ret = taosTRead(fd, pHead, sizeof(SWalHead));
    if (ret == 0) break;

    if (ret < 0) {
      wError("vgId:%d, wal:%s, failed to read wal head part since %s", pWal->vgId, name, strerror(errno));
      terrno = TAOS_SYSTEM_ERROR(errno);
      break;
    }

    if (ret < sizeof(SWalHead)) {
      wError("vgId:%d, wal:%s, failed to read head, ret:%d, skip the rest of file", pWal->vgId, name, ret);
      taosFtruncate(fd, offset);
      fsync(fd);
      break;
    }

    if (!taosCheckChecksumWhole((uint8_t *)pHead, sizeof(SWalHead))) {
      wWarn("vgId:%d, wal:%s, cksum is messed up, skip the rest of file", pWal->vgId, name);
      terrno = TSDB_CODE_WAL_FILE_CORRUPTED;
      ASSERT(false);
      break;
    }

    if (pHead->len > size - sizeof(SWalHead)) {
      size = sizeof(SWalHead) + pHead->len;
      buffer = realloc(buffer, size);
      if (buffer == NULL) {
        terrno = TAOS_SYSTEM_ERROR(errno);
        break;
      }

      pHead = (SWalHead *)buffer;
    }

    ret = taosTRead(fd, pHead->cont, pHead->len);
    if (ret < 0) {
      wError("vgId:%d, wal:%s failed to read wal body part since %s", pWal->vgId, name, strerror(errno));
      terrno = TAOS_SYSTEM_ERROR(errno);
      break;
    }

    if (ret < pHead->len) {
      wError("vgId:%d, wal:%s, failed to read body, len:%d ret:%d, skip the rest of file", pWal->vgId, name, pHead->len,
             ret);
      taosFtruncate(fd, offset);
      fsync(fd);
      break;
    }

    offset = offset + sizeof(SWalHead) + pHead->len;

    if (pWal->keep) pWal->version = pHead->version;
    (*writeFp)(pVnode, pHead, TAOS_QTYPE_WAL);
  }

  close(fd);
  free(buffer);

  return terrno;
}

static int32_t walRemoveWalFiles(const char *path) {
  int32_t plen = strlen(walPrefix);
  char    name[TSDB_FILENAME_LEN * 3];

  terrno = 0;

  struct dirent *ent;
  DIR *dir = opendir(path);
  if (dir == NULL && errno == ENOENT) return 0;
  if (dir == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return terrno;
  }

  while ((ent = readdir(dir)) != NULL) {
    if (strncmp(ent->d_name, walPrefix, plen) == 0) {
      snprintf(name, sizeof(name), "%s/%s", path, ent->d_name);
      if (remove(name) < 0) {
        wError("wal:%s, failed to remove(%s)", name, strerror(errno));
        terrno = TAOS_SYSTEM_ERROR(errno);
      }
    }
  }

  closedir(dir);

  return terrno;
}

int64_t walGetVersion(twalh param) {
  SWal *pWal = param;
  if (pWal == 0) return 0;

  return pWal->version;
}
