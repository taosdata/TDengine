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
#include <errno.h>
#include <fcntl.h>
#include <libgen.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "hash.h"
#include "os.h"
#include "taoserror.h"
#include "tchecksum.h"
#include "tcoding.h"
#include "tkvstore.h"
#include "tulog.h"

#define TD_KVSTORE_HEADER_SIZE 512
#define TD_KVSTORE_MAJOR_VERSION 1
#define TD_KVSTORE_MAINOR_VERSION 0
#define TD_KVSTORE_SNAP_SUFFIX ".snap"
#define TD_KVSTORE_NEW_SUFFIX ".new"

static int       tdInitKVStoreHeader(int fd, char *fname);
static void *    tdEncodeStoreInfo(void *buf, SStoreInfo *pInfo);
// static void *    tdDecodeStoreInfo(void *buf, SStoreInfo *pInfo);
static SKVStore *tdNewKVStore(char *fname, iterFunc iFunc, afterFunc aFunc, void *appH);
static char *    tdGetKVStoreSnapshotFname(char *fdata);
static char *    tdGetKVStoreNewFname(char *fdata);
static void      tdFreeKVStore(SKVStore *pStore);
static int       tdUpdateKVStoreHeader(int fd, char *fname, SStoreInfo *pInfo);

int tdCreateKVStore(char *fname) {
  char *tname = strdup(fname);
  if (tname == NULL) return TSDB_CODE_COM_OUT_OF_MEMORY;

  int fd = open(fname, O_RDWR | O_CREAT, 0755);
  if (fd < 0) {
    uError("failed to open file %s since %s", fname, strerror(errno));
    return TAOS_SYSTEM_ERROR(errno);
  }

  int code = tdInitKVStoreHeader(fd, fname);
  if (code != TSDB_CODE_SUCCESS) return code;

  if (fsync(fd) < 0) {
    uError("failed to fsync file %s since %s", fname, strerror(errno));
    return TAOS_SYSTEM_ERROR(errno);
  }

  if (close(fd) < 0) {
    uError("failed to close file %s since %s", fname, strerror(errno));
    return TAOS_SYSTEM_ERROR(errno);
  }

  return TSDB_CODE_SUCCESS;
}

SKVStore *tdOpenKVStore(char *fname, iterFunc iFunc, afterFunc aFunc, void *appH) {
  SKVStore *pStore = tdNewKVStore(fname, iFunc, aFunc, appH);
  if (pStore == NULL) return NULL;

  pStore->fd = open(pStore->fname, O_RDWR);
  if (pStore->fd < 0) {
    uError("failed to open file %s since %s", pStore->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  if (access(pStore->fsnap, F_OK) == 0) {
    uTrace("file %s exists, try to recover the KV store", pStore->fsnap);
    pStore->sfd = open(pStore->fsnap, O_RDONLY);
    if (pStore->sfd < 0) {
      uError("failed to open file %s since %s", pStore->fsnap, strerror(errno));
      terrno = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }

    // TODO: rewind the file

    close(pStore->sfd);
    pStore->sfd = -1;
    remove(pStore->fsnap);
  }

  // TODO: Recover from the file

  return pStore;

_err:
  if (pStore->fd > 0) {
    close(pStore->fd);
    pStore->fd = -1;
  }
  if (pStore->sfd > 0) {
    close(pStore->sfd);
    pStore->sfd = -1;
  }
  tdFreeKVStore(pStore);
  return NULL;
}

int tdKVStoreStartCommit(SKVStore *pStore) {
  pStore->fd = open(pStore->fname, O_RDWR);
  if (pStore->fd < 0) {
    uError("failed to open file %s since %s", pStore->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  pStore->sfd = open(pStore->fsnap, O_WRONLY | O_CREAT, 0755);
  if (pStore->sfd < 0) {
    uError("failed to open file %s since %s", pStore->fsnap, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  if (tsendfile(pStore->sfd, pStore->fd, NULL, TD_KVSTORE_HEADER_SIZE) < TD_KVSTORE_HEADER_SIZE) {
    uError("failed to send file %d bytes since %s", TD_KVSTORE_HEADER_SIZE, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  if (fsync(pStore->sfd) < 0) {
    uError("failed to fsync file %s since %s", pStore->fsnap, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  if (close(pStore->sfd) < 0) {
    uError("failed to close file %s since %s", pStore->fsnap, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }
  pStore->sfd = -1;

  return 0;

_err:
  if (pStore->sfd > 0) {
    close(pStore->sfd);
    pStore->sfd = -1;
    remove(pStore->fsnap);
  }
  if (pStore->fd > 0) {
    close(pStore->fd);
    pStore->fd = -1;
  }
  return -1;
}

int tdKVStoreEndCommit(SKVStore *pStore) {
  ASSERT(pStore->fd > 0);

  terrno = tdUpdateKVStoreHeader(pStore->fd, pStore->fname, &(pStore->info));
  if (terrno != TSDB_CODE_SUCCESS) return -1;

  if (fsync(pStore->fd) < 0) {
    uError("failed to fsync file %s since %s", pStore->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (close(pStore->fd) < 0) {
    uError("failed to close file %s since %s", pStore->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  remove(pStore->fsnap);
  return 0;
}

static int tdUpdateKVStoreHeader(int fd, char *fname, SStoreInfo *pInfo) {
  char buf[TD_KVSTORE_HEADER_SIZE] = "\0";

  if (lseek(fd, 0, SEEK_SET) < 0) {
    uError("failed to lseek file %s since %s", fname, strerror(errno));
    return TAOS_SYSTEM_ERROR(errno);
  }

  tdEncodeStoreInfo(buf, pInfo);
  taosCalcChecksumAppend(0, (uint8_t *)buf, TD_KVSTORE_HEADER_SIZE);
  if (twrite(fd, buf, TD_KVSTORE_HEADER_SIZE) < TD_KVSTORE_HEADER_SIZE) {
    uError("failed to write file %s %d bytes since %s", fname, TD_KVSTORE_HEADER_SIZE, strerror(errno));
    return TAOS_SYSTEM_ERROR(errno);
  }

  return TSDB_CODE_SUCCESS;
}

static int tdInitKVStoreHeader(int fd, char *fname) {
  SStoreInfo info = {TD_KVSTORE_HEADER_SIZE, 0, 0, 0};

  return tdUpdateKVStoreHeader(fd, fname, &info);
}

static void *tdEncodeStoreInfo(void *buf, SStoreInfo *pInfo) {
  buf = taosEncodeVariantI64(buf, pInfo->size);
  buf = taosEncodeVariantI64(buf, pInfo->tombSize);
  buf = taosEncodeVariantI64(buf, pInfo->nRecords);
  buf = taosEncodeVariantI64(buf, pInfo->nDels);

  return buf;
}

// static void *tdDecodeStoreInfo(void *buf, SStoreInfo *pInfo) {
//   buf = taosDecodeVariantI64(buf, &(pInfo->size));
//   buf = taosDecodeVariantI64(buf, &(pInfo->tombSize));
//   buf = taosDecodeVariantI64(buf, &(pInfo->nRecords));
//   buf = taosDecodeVariantI64(buf, &(pInfo->nDels));

//   return buf;
// }

static SKVStore *tdNewKVStore(char *fname, iterFunc iFunc, afterFunc aFunc, void *appH) {
  SKVStore *pStore = (SKVStore *)malloc(sizeof(SKVStore));
  if (pStore == NULL) goto _err;

  pStore->fname = strdup(fname);
  if (pStore->map == NULL) goto _err;

  pStore->fsnap = tdGetKVStoreSnapshotFname(fname);
  if (pStore->fsnap == NULL) goto _err;

  pStore->fnew = tdGetKVStoreNewFname(fname);
  if (pStore->fnew == NULL) goto _err;

  pStore->fd = -1;
  pStore->sfd = -1;
  pStore->nfd = -1;
  pStore->iFunc = iFunc;
  pStore->aFunc = aFunc;
  pStore->appH = appH;
  pStore->map = taosHashInit(4096, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false);
  if (pStore->map == NULL) {
    terrno = TSDB_CODE_COM_OUT_OF_MEMORY;
    goto _err;
  }

  return pStore;

_err:
  terrno = TSDB_CODE_COM_OUT_OF_MEMORY;
  tdFreeKVStore(pStore);
  return NULL;
}

static void tdFreeKVStore(SKVStore *pStore) {
  if (pStore) {
    tfree(pStore->fname);
    tfree(pStore->fsnap);
    tfree(pStore->fnew);
    taosHashCleanup(pStore->map);
    free(pStore);
  }
}

static char *tdGetKVStoreSnapshotFname(char *fdata) {
  size_t size = strlen(fdata) + strlen(TD_KVSTORE_SNAP_SUFFIX) + 1;
  char * fname = malloc(size);
  if (fname == NULL) {
    terrno = TSDB_CODE_COM_OUT_OF_MEMORY;
    return NULL;
  }
  sprintf(fname, "%s%s", fdata, TD_KVSTORE_SNAP_SUFFIX);
  return fname;
}

static char *tdGetKVStoreNewFname(char *fdata) {
  size_t size = strlen(fdata) + strlen(TD_KVSTORE_NEW_SUFFIX) + 1;
  char * fname = malloc(size);
  if (fname == NULL) {
    terrno = TSDB_CODE_COM_OUT_OF_MEMORY;
    return NULL;
  }
  sprintf(fname, "%s%s", fdata, TD_KVSTORE_NEW_SUFFIX);
  return fname;
}