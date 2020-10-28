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
#include "tlog.h"
#include "tchecksum.h"
#include "tutil.h"
#include "ttimer.h"
#include "taoserror.h"
#include "twal.h"
#include "tqueue.h"

#define walPrefix "wal"

#define wFatal(...) { if (wDebugFlag & DEBUG_FATAL) { taosPrintLog("WAL FATAL ", 255, __VA_ARGS__); }}
#define wError(...) { if (wDebugFlag & DEBUG_ERROR) { taosPrintLog("WAL ERROR ", 255, __VA_ARGS__); }}
#define wWarn(...)  { if (wDebugFlag & DEBUG_WARN)  { taosPrintLog("WAL WARN ", 255, __VA_ARGS__); }}
#define wInfo(...)  { if (wDebugFlag & DEBUG_INFO)  { taosPrintLog("WAL ", 255, __VA_ARGS__); }}
#define wDebug(...) { if (wDebugFlag & DEBUG_DEBUG) { taosPrintLog("WAL ", wDebugFlag, __VA_ARGS__); }}
#define wTrace(...) { if (wDebugFlag & DEBUG_TRACE) { taosPrintLog("WAL ", wDebugFlag, __VA_ARGS__); }}

typedef struct {
  uint64_t version;
  int      fd;
  int      keep;
  int      level;
  int32_t  fsyncPeriod;
  void    *timer;
  void    *signature;
  int      max;  // maximum number of wal files
  uint32_t id;   // increase continuously
  int      num;  // number of wal files
  char     path[TSDB_FILENAME_LEN];
  char     name[TSDB_FILENAME_LEN+16];
  pthread_mutex_t mutex;
} SWal;

static void    *walTmrCtrl = NULL;
static int     tsWalNum = 0;
static pthread_once_t walModuleInit = PTHREAD_ONCE_INIT;
static uint32_t walSignature = 0xFAFBFDFE;
static int  walHandleExistingFiles(const char *path);
static int  walRestoreWalFile(SWal *pWal, void *pVnode, FWalWrite writeFp);
static int  walRemoveWalFiles(const char *path);
static void walProcessFsyncTimer(void *param, void *tmrId);
static void walRelease(SWal *pWal);
static int walGetMaxOldFileId(char *odir);

static void walModuleInitFunc() {
  walTmrCtrl = taosTmrInit(1000, 100, 300000, "WAL");
  if (walTmrCtrl == NULL)
    walModuleInit = PTHREAD_ONCE_INIT;
  else
    wDebug("WAL module is initialized");
}

static inline bool walNeedFsyncTimer(SWal *pWal) {
  if (pWal->fsyncPeriod > 0 && pWal->level == TAOS_WAL_FSYNC) {
    return true;
  }
  return false;
}

void *walOpen(const char *path, const SWalCfg *pCfg) {
  SWal *pWal = calloc(sizeof(SWal), 1);
  if (pWal == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return NULL;
  }

  pthread_once(&walModuleInit, walModuleInitFunc);
  if (walTmrCtrl == NULL) {
    free(pWal);
    terrno = TAOS_SYSTEM_ERROR(errno);
    return NULL;
  }

  atomic_add_fetch_32(&tsWalNum, 1);
  pWal->fd = -1;
  pWal->max = pCfg->wals;
  pWal->id = 0;
  pWal->num = 0;
  pWal->level = pCfg->walLevel;
  pWal->keep = pCfg->keep;
  pWal->fsyncPeriod = pCfg->fsyncPeriod;
  pWal->signature = pWal;
  tstrncpy(pWal->path, path, sizeof(pWal->path));
  pthread_mutex_init(&pWal->mutex, NULL);

  if (walNeedFsyncTimer(pWal)) {
    pWal->timer = taosTmrStart(walProcessFsyncTimer, pWal->fsyncPeriod, pWal, walTmrCtrl);
    if (pWal->timer == NULL) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      walRelease(pWal);
      return NULL;
    }
  }

  if (taosMkDir(path, 0755) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    wError("wal:%s, failed to create directory(%s)", path, strerror(errno));
    walRelease(pWal);
    pWal = NULL;
  }

  if (pCfg->keep == 1) return pWal;

  if (walHandleExistingFiles(path) == 0) walRenew(pWal);

  if (pWal && pWal->fd < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    wError("wal:%s, failed to open(%s)", path, strerror(errno));
    walRelease(pWal);
    pWal = NULL;
  }

  if (pWal) wDebug("wal:%s, it is open, level:%d fsyncPeriod:%d", path, pWal->level, pWal->fsyncPeriod);
  return pWal;
}

int walAlter(twalh wal, const SWalCfg *pCfg) {
  SWal *pWal = wal;
  if (pWal == NULL) {
    return TSDB_CODE_WAL_APP_ERROR;
  }

  if (pWal->level == pCfg->walLevel && pWal->fsyncPeriod == pCfg->fsyncPeriod) {
    wDebug("wal:%s, old walLevel:%d fsync:%d, new walLevel:%d fsync:%d not change", pWal->name, pWal->level,
           pWal->fsyncPeriod, pCfg->walLevel, pCfg->fsyncPeriod);
    return TSDB_CODE_SUCCESS;
  }

  wInfo("wal:%s, change old walLevel:%d fsync:%d, new walLevel:%d fsync:%d", pWal->name, pWal->level, pWal->fsyncPeriod,
        pCfg->walLevel, pCfg->fsyncPeriod);

  pthread_mutex_lock(&pWal->mutex);
  pWal->level = pCfg->walLevel;
  pWal->fsyncPeriod = pCfg->fsyncPeriod;
  if (walNeedFsyncTimer(pWal)) {
    wInfo("wal:%s, reset fsync timer, walLevel:%d fsyncPeriod:%d", pWal->name, pWal->level, pWal->fsyncPeriod);
    taosTmrReset(walProcessFsyncTimer, pWal->fsyncPeriod, pWal, &pWal->timer, walTmrCtrl);
  } else {
    wInfo("wal:%s, stop fsync timer, walLevel:%d fsyncPeriod:%d", pWal->name, pWal->level, pWal->fsyncPeriod);
    taosTmrStop(pWal->timer);
    pWal->timer = NULL;
  }
  pthread_mutex_unlock(&pWal->mutex);

  return TSDB_CODE_SUCCESS;
}

void walClose(void *handle) {
  if (handle == NULL) return;

  SWal *pWal = handle;
  taosClose(pWal->fd);
  if (pWal->timer) taosTmrStopA(&pWal->timer);

  if (pWal->keep == 0) {
    // remove all files in the directory
    for (int i = 0; i < pWal->num; ++i) {
      snprintf(pWal->name, sizeof(pWal->name), "%s/%s%d", pWal->path, walPrefix, pWal->id - i);
      if (remove(pWal->name) < 0) {
        wError("wal:%s, failed to remove", pWal->name);
      } else {
        wDebug("wal:%s, it is removed", pWal->name);
      }
    }
  } else {
    wDebug("wal:%s, it is closed and kept", pWal->name);
  }

  walRelease(pWal);
}

int walRenew(void *handle) {
  if (handle == NULL) return 0;
  SWal *pWal = handle;

  terrno = 0;

  pthread_mutex_lock(&pWal->mutex);

  if (pWal->fd >= 0) {
    close(pWal->fd);
    pWal->id++;
    wDebug("wal:%s, it is closed", pWal->name);
  }

  pWal->num++;

  snprintf(pWal->name, sizeof(pWal->name), "%s/%s%d", pWal->path, walPrefix, pWal->id);
  pWal->fd = open(pWal->name, O_WRONLY | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);

  if (pWal->fd < 0) {
    wError("wal:%s, failed to open(%s)", pWal->name, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
  } else {
    wDebug("wal:%s, it is created", pWal->name);

    if (pWal->num > pWal->max) {
      // remove the oldest wal file
      char name[TSDB_FILENAME_LEN * 3];
      snprintf(name, sizeof(name), "%s/%s%d", pWal->path, walPrefix, pWal->id - pWal->max);
      if (remove(name) < 0) {
        wError("wal:%s, failed to remove(%s)", name, strerror(errno));
      } else {
        wDebug("wal:%s, it is removed", name);
      }

      pWal->num--;
    }
  }

  pthread_mutex_unlock(&pWal->mutex);

  return terrno;
}

int walWrite(void *handle, SWalHead *pHead) {
  SWal *pWal = handle;
  if (pWal == NULL) return -1;

  terrno = 0;

  // no wal
  if (pWal->level == TAOS_WAL_NOLOG) return 0;
  if (pHead->version <= pWal->version) return 0;

  pHead->signature = walSignature;
  taosCalcChecksumAppend(0, (uint8_t *)pHead, sizeof(SWalHead));
  int contLen = pHead->len + sizeof(SWalHead);

  if (taosTWrite(pWal->fd, pHead, contLen) != contLen) {
    wError("wal:%s, failed to write(%s)", pWal->name, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
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
      wError("wal:%s, fsync failed(%s)", pWal->name, strerror(errno));
    }
  }
}

int walRestore(void *handle, void *pVnode, int (*writeFp)(void *, void *, int)) {
  SWal    *pWal = handle;
  struct   dirent *ent;
  int      count = 0;
  uint32_t maxId = 0, minId = -1, index =0;

  terrno = 0;
  int  plen = strlen(walPrefix);
  char opath[TSDB_FILENAME_LEN + 5];

  int slen = snprintf(opath, sizeof(opath), "%s", pWal->path);
  if (pWal->keep == 0) strcpy(opath + slen, "/old");

  DIR *dir = opendir(opath);
  if (dir == NULL && errno == ENOENT) return 0;
  if (dir == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return terrno;
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

  if (count == 0) {
    if (pWal->keep) terrno = walRenew(pWal);
    return terrno;
  }

  if (count != (maxId - minId + 1)) {
    wError("wal:%s, messed up, count:%d max:%d min:%d", opath, count, maxId, minId);
    terrno = TSDB_CODE_WAL_APP_ERROR;
  } else {
    wDebug("wal:%s, %d files will be restored", opath, count);

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
          wError("wal:%s, failed to remove directory(%s)", opath, strerror(errno));
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
        wError("wal:%s, failed to open file(%s)", pWal->name, strerror(errno));
        terrno = TAOS_SYSTEM_ERROR(errno);
      }
    }
  }

  return terrno;
}

int walGetWalFile(void *handle, char *name, uint32_t *index) {
  SWal *  pWal = handle;
  int     code = 1;
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

static void walRelease(SWal *pWal) {
  pthread_mutex_destroy(&pWal->mutex);
  pWal->signature = NULL;
  free(pWal);

  if (atomic_sub_fetch_32(&tsWalNum, 1) == 0) {
    if (walTmrCtrl) taosTmrCleanUp(walTmrCtrl);
    walTmrCtrl = NULL;
    walModuleInit = PTHREAD_ONCE_INIT;
    wDebug("WAL module is cleaned up");
  }
}

static int walRestoreWalFile(SWal *pWal, void *pVnode, FWalWrite writeFp) {
  char *name = pWal->name;
  int   size = 1024 * 1024;  // default 1M buffer size

  terrno = 0;
  char *buffer = malloc(size);
  if (buffer == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return terrno;
  }

  SWalHead *pHead = (SWalHead *)buffer;

  int fd = open(name, O_RDWR);
  if (fd < 0) {
    wError("wal:%s, failed to open for restore(%s)", name, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    free(buffer);
    return terrno;
  }

  wDebug("wal:%s, start to restore", name);

  size_t offset = 0;
  while (1) {
    int ret = taosTRead(fd, pHead, sizeof(SWalHead));
    if (ret == 0) break;

    if (ret < 0) {
      wError("wal:%s, failed to read wal head part since %s", name, strerror(errno));
      terrno = TAOS_SYSTEM_ERROR(errno);
      break;
    }

    if (ret < sizeof(SWalHead)) {
      wError("wal:%s, failed to read head, ret:%d, skip the rest of file", name, ret);
      taosFtruncate(fd, offset);
      fsync(fd);
      break;
    }

    if (!taosCheckChecksumWhole((uint8_t *)pHead, sizeof(SWalHead))) {
      wWarn("wal:%s, cksum is messed up, skip the rest of file", name);
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
      wError("wal:%s failed to read wal body part since %s", name, strerror(errno));
      terrno = TAOS_SYSTEM_ERROR(errno);
      break;
    }

    if (ret < pHead->len) {
      wError("wal:%s, failed to read body, len:%d ret:%d, skip the rest of file", name, pHead->len, ret);
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

int walHandleExistingFiles(const char *path) {
  char   oname[TSDB_FILENAME_LEN * 3];
  char   nname[TSDB_FILENAME_LEN * 3];
  char   opath[TSDB_FILENAME_LEN];

  snprintf(opath, sizeof(opath), "%s/old", path);

  struct dirent *ent;
  DIR   *dir = opendir(path);
  int    plen = strlen(walPrefix);
  terrno = 0;

  int midx = walGetMaxOldFileId(opath);
  int count = 0;
  while ((ent = readdir(dir)) != NULL) {
    if (strncmp(ent->d_name, walPrefix, plen) == 0) {
      midx++;
      snprintf(oname, sizeof(oname), "%s/%s", path, ent->d_name);
      snprintf(nname, sizeof(nname), "%s/old/wal%d", path, midx);
      if (taosMkDir(opath, 0755) != 0) {
        wError("wal:%s, failed to create directory:%s(%s)", oname, opath, strerror(errno));
        terrno = TAOS_SYSTEM_ERROR(errno);
        break;
      }

      if (rename(oname, nname) < 0) {
        wError("wal:%s, failed to move to new:%s", oname, nname);
        terrno = TAOS_SYSTEM_ERROR(errno);
        break;
      }

      count++;
    }

    wDebug("wal:%s, %d files are moved for restoration", path, count);
  }

  closedir(dir);
  return terrno;
}

static int walRemoveWalFiles(const char *path) {
  int  plen = strlen(walPrefix);
  char name[TSDB_FILENAME_LEN * 3];

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

static void walProcessFsyncTimer(void *param, void *tmrId) {
  SWal *pWal = param;

  if (pWal->signature != pWal) return;
  if (pWal->fd < 0) return;

  if (fsync(pWal->fd) < 0) {
    wError("wal:%s, fsync failed(%s)", pWal->name, strerror(errno));
  }

  if (walNeedFsyncTimer(pWal)) {
    pWal->timer = taosTmrStart(walProcessFsyncTimer, pWal->fsyncPeriod, pWal, walTmrCtrl);
  } else {
    wInfo("wal:%s, stop fsync timer for walLevel:%d fsyncPeriod:%d", pWal->name, pWal->level, pWal->fsyncPeriod);
    taosTmrStop(pWal->timer);
    pWal->timer = NULL;
  }
}

int64_t walGetVersion(twalh param) {
  SWal *pWal = param;
  if (pWal == 0) return 0;

  return pWal->version;
}

static int walGetMaxOldFileId(char *odir) {
  int            midx = 0;
  DIR *          dir = NULL;
  struct dirent *dp = NULL;
  int            plen = strlen(walPrefix);

  if (access(odir, F_OK) != 0) return midx;

  dir = opendir(odir);
  if (dir == NULL) {
    wError("failed to open directory %s since %s", odir, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  while ((dp = readdir(dir)) != NULL) {
    if (strncmp(dp->d_name, walPrefix, plen) == 0) {
      int idx = atol(dp->d_name + plen);
      if (midx < idx) midx = idx;
    }
  }

  closedir(dir);
  return midx;
}