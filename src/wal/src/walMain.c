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

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <dirent.h>
#include <unistd.h>
#include <fcntl.h> 

#include "os.h"
#include "tlog.h"
#include "tchecksum.h"
#include "tutil.h"
#include "taoserror.h"
#include "twal.h"
#include "tqueue.h"

#define walPrefix "wal"
#define wError(...) if (wDebugFlag & DEBUG_ERROR) {taosPrintLog("ERROR WAL ", wDebugFlag, __VA_ARGS__);}
#define wWarn(...) if (wDebugFlag & DEBUG_WARN) {taosPrintLog("WARN WAL ", wDebugFlag, __VA_ARGS__);}
#define wTrace(...) if (wDebugFlag & DEBUG_TRACE) {taosPrintLog("WAL ", wDebugFlag, __VA_ARGS__);}
#define wPrint(...) {taosPrintLog("WAL ", 255, __VA_ARGS__);}

typedef struct {
  uint64_t version;
  int      fd;
  int      keep;
  int      level;
  int      max;  // maximum number of wal files
  uint32_t id;   // increase continuously
  int      num;  // number of wal files
  char     path[TSDB_FILENAME_LEN];
  char     name[TSDB_FILENAME_LEN];
  pthread_mutex_t mutex;
} SWal;

int wDebugFlag = 135;

static uint32_t walSignature = 0xFAFBFDFE;
static int walHandleExistingFiles(const char *path);
static int walRestoreWalFile(SWal *pWal, void *pVnode, FWalWrite writeFp);
static int walRemoveWalFiles(const char *path);

void *walOpen(const char *path, const SWalCfg *pCfg) {
  SWal *pWal = calloc(sizeof(SWal), 1);
  if (pWal == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return NULL;
  }

  pWal->fd = -1;
  pWal->max = pCfg->wals;
  pWal->id = 0;
  pWal->num = 0;
  pWal->level = pCfg->walLevel;
  pWal->keep = pCfg->keep;
  strcpy(pWal->path, path);
  pthread_mutex_init(&pWal->mutex, NULL);

  if (access(path, F_OK) != 0) mkdir(path, 0755);
  
  if (pCfg->keep == 1) return pWal;

  if (walHandleExistingFiles(path) == 0) 
    walRenew(pWal);

  if (pWal->fd <0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    wError("wal:%s, failed to open", path);
    pthread_mutex_destroy(&pWal->mutex);
    free(pWal);
    pWal = NULL;
  } else {
    wTrace("wal:%s, it is open, level:%d", path, pWal->level);
  }

  return pWal;
}

void walClose(void *handle) {
  if (handle == NULL) return;
  
  SWal *pWal = handle;  
  close(pWal->fd);

  if (pWal->keep == 0) {
    // remove all files in the directory
    for (int i=0; i<pWal->num; ++i) {
      sprintf(pWal->name, "%s/%s%d", pWal->path, walPrefix, pWal->id-i);
      if (remove(pWal->name) <0) {
        wError("wal:%s, failed to remove", pWal->name);
      } else {
        wTrace("wal:%s, it is removed", pWal->name);
      }
    }
  } else {
    wTrace("wal:%s, it is closed and kept", pWal->name);
  }

  pthread_mutex_destroy(&pWal->mutex);

  free(pWal);
}

int walRenew(void *handle) {
  if (handle == NULL) return 0;
  SWal *pWal = handle;
  int   code = 0;

  pthread_mutex_lock(&pWal->mutex);

  if (pWal->fd >=0) {
    close(pWal->fd);
    pWal->id++;
    wTrace("wal:%s, it is closed", pWal->name);
  }

  pWal->num++;

  sprintf(pWal->name, "%s/%s%d", pWal->path, walPrefix, pWal->id);
  pWal->fd = open(pWal->name, O_WRONLY | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);

  if (pWal->fd < 0) {
    wError("wal:%d, failed to open(%s)", pWal->name, strerror(errno));
    code = -1;
  } else {
    wTrace("wal:%s, it is created", pWal->name);

    if (pWal->num > pWal->max) {
      // remove the oldest wal file
      char name[TSDB_FILENAME_LEN * 3];
      sprintf(name, "%s/%s%d", pWal->path, walPrefix, pWal->id - pWal->max);
      if (remove(name) <0) {
        wError("wal:%s, failed to remove(%s)", name, strerror(errno));
      } else {
        wTrace("wal:%s, it is removed", name);
      }

      pWal->num--;
    }
  }  
  
  pthread_mutex_unlock(&pWal->mutex);

  return code;
}

int walWrite(void *handle, SWalHead *pHead) {
  SWal *pWal = handle;
  int   code = 0;
  if (pWal == NULL) return -1;

  // no wal  
  if (pWal->level == TAOS_WAL_NOLOG) return 0;
  if (pHead->version <= pWal->version) return 0;

  pHead->signature = walSignature;
  taosCalcChecksumAppend(0, (uint8_t *)pHead, sizeof(SWalHead));
  int contLen = pHead->len + sizeof(SWalHead);

  if(write(pWal->fd, pHead, contLen) != contLen) {
    wError("wal:%s, failed to write(%s)", pWal->name, strerror(errno));
    code = -1;
  } else {
    pWal->version = pHead->version;
  }

  return code;
}

void walFsync(void *handle) {

  SWal *pWal = handle;
  if (pWal == NULL) return;

  if (pWal->level == TAOS_WAL_FSYNC && pWal->fd >=0) {
    if (fsync(pWal->fd) < 0) {
      wError("wal:%s, fsync failed(%s)", pWal->name, strerror(errno));
    }
  }
}

int walRestore(void *handle, void *pVnode, int (*writeFp)(void *, void *, int)) {
  SWal    *pWal = handle;
  int      code = 0;
  struct   dirent *ent;
  int      count = 0;
  uint32_t maxId = 0, minId = -1, index =0;

  int   plen = strlen(walPrefix);
  char  opath[TSDB_FILENAME_LEN+5];
   
  int slen = sprintf(opath, "%s", pWal->path);
  if ( pWal->keep == 0) 
    strcpy(opath+slen, "/old");

  // is there old directory?
  if (access(opath, F_OK)) return 0; 

  DIR *dir = opendir(opath);
  while ((ent = readdir(dir))!= NULL) {
    if ( strncmp(ent->d_name, walPrefix, plen) == 0) {
      index = atol(ent->d_name + plen);
      if (index > maxId) maxId = index;
      if (index < minId) minId = index;
      count++;
    }
  }

  closedir(dir);

  if (count == 0) {
    if (pWal->keep) code = walRenew(pWal);
    return code;
  }

  if ( count != (maxId-minId+1) ) {
    wError("wal:%s, messed up, count:%d max:%d min:%d", opath, count, maxId, minId);
    code = -1;
  } else {
    wTrace("wal:%s, %d files will be restored", opath, count);

    for (index = minId; index<=maxId; ++index) {
      sprintf(pWal->name, "%s/%s%d", opath, walPrefix, index);
      code = walRestoreWalFile(pWal, pVnode, writeFp);
      if (code < 0) break;
    }
  }

  if (code == 0) {
    if (pWal->keep == 0) {
      code = walRemoveWalFiles(opath);
      if (code == 0) {
        if (remove(opath) < 0) {
          wError("wal:%s, failed to remove directory(%s)", opath, strerror(errno));
          code = -1;
        }
      }
    } else { 
      // open the existing WAL file in append mode
      pWal->num = count;
      pWal->id = maxId;
      sprintf(pWal->name, "%s/%s%d", opath, walPrefix, maxId);
      pWal->fd = open(pWal->name, O_WRONLY | O_CREAT | O_APPEND, S_IRWXU | S_IRWXG | S_IRWXO);
      if (pWal->fd < 0) {
        wError("wal:%s, failed to open file(%s)", pWal->name, strerror(errno));
        code = -1;
      }
    }
  }

  return code;
}

int walGetWalFile(void *handle, char *name, uint32_t *index) {
  SWal   *pWal = handle;
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
    code = (*index == pWal->id) ? 0:1;
  }

  pthread_mutex_unlock(&(pWal->mutex));

  return code;
}  

static int walRestoreWalFile(SWal *pWal, void *pVnode, FWalWrite writeFp) {
  int   code = 0;
  char *name = pWal->name;

  char *buffer = malloc(1024000);  // size for one record
  if (buffer == NULL) return -1;

  SWalHead *pHead = (SWalHead *)buffer;

  int fd = open(name, O_RDONLY);
  if (fd < 0) {
    wError("wal:%s, failed to open for restore(%s)", name, strerror(errno));
    free(buffer);
    return -1;
  }

  wTrace("wal:%s, start to restore", name);

  while (1) {
    int ret = read(fd, pHead, sizeof(SWalHead));
    if ( ret == 0) { code = 0; break;}  

    if (ret != sizeof(SWalHead)) {
      wWarn("wal:%s, failed to read head, skip, ret:%d(%s)", name, ret, strerror(errno));
      break;
    }

    if (!taosCheckChecksumWhole((uint8_t *)pHead, sizeof(SWalHead))) {
      wWarn("wal:%s, cksum is messed up, skip the rest of file", name);
      break;
    } 

    ret = read(fd, pHead->cont, pHead->len);
    if ( ret != pHead->len) {
      wWarn("wal:%s, failed to read body, skip, len:%d ret:%d", name, pHead->len, ret);
      break;
    }

    if (pWal->keep) pWal->version = pHead->version;
    (*writeFp)(pVnode, pHead, TAOS_QTYPE_WAL);
  }

  close(fd);
  free(buffer);

  return code;
}

int walHandleExistingFiles(const char *path) {
  int    code = 0;
  char   oname[TSDB_FILENAME_LEN * 3];
  char   nname[TSDB_FILENAME_LEN * 3];
  char   opath[TSDB_FILENAME_LEN];

  sprintf(opath, "%s/old", path);

  struct dirent *ent;
  DIR   *dir = opendir(path);
  int    plen = strlen(walPrefix);

  if (access(opath, F_OK) == 0) {
    // old directory is there, it means restore process is not finished
    walRemoveWalFiles(path);

  } else {
    // move all files to old directory
    int count = 0;
    while ((ent = readdir(dir))!= NULL) {  
      if ( strncmp(ent->d_name, walPrefix, plen) == 0) {
        if (access(opath, F_OK) != 0) mkdir(opath, 0755);

        sprintf(oname, "%s/%s", path, ent->d_name);
        sprintf(nname, "%s/old/%s", path, ent->d_name);
        if (rename(oname, nname) < 0) {
          wError("wal:%s, failed to move to new:%s", oname, nname);
          code = -1;
          break;
        } 

        count++;
      }
    }

    wTrace("wal:%s, %d files are moved for restoration", path, count);
  }
  
  closedir(dir);
  return code;
}

static int walRemoveWalFiles(const char *path) {
  int    plen = strlen(walPrefix);
  char   name[TSDB_FILENAME_LEN * 3];
  int    code = 0;

  if (access(path, F_OK) != 0) return 0;

  struct dirent *ent;
  DIR   *dir = opendir(path);

  while ((ent = readdir(dir))!= NULL) {
    if ( strncmp(ent->d_name, walPrefix, plen) == 0) {
      sprintf(name, "%s/%s", path, ent->d_name);
      if (remove(name) <0) {
        wError("wal:%s, failed to remove(%s)", name, strerror(errno));
        code = -1; break;
      }
    }
  } 

  closedir(dir);

  return code;
}

