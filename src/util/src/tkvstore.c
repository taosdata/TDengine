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
#include "hash.h"
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
#define TD_KVSTORE_INIT_MAGIC 0xFFFFFFFF

typedef struct {
  uint64_t uid;
  int64_t  offset;
  int64_t  size;
} SKVRecord;

static int       tdInitKVStoreHeader(int fd, char *fname);
static int       tdEncodeStoreInfo(void **buf, SStoreInfo *pInfo);
static void *    tdDecodeStoreInfo(void *buf, SStoreInfo *pInfo);
static SKVStore *tdNewKVStore(char *fname, iterFunc iFunc, afterFunc aFunc, void *appH);
static char *    tdGetKVStoreSnapshotFname(char *fdata);
static char *    tdGetKVStoreNewFname(char *fdata);
static void      tdFreeKVStore(SKVStore *pStore);
static int       tdUpdateKVStoreHeader(int fd, char *fname, SStoreInfo *pInfo);
static int       tdLoadKVStoreHeader(int fd, char *fname, SStoreInfo *pInfo, uint32_t *version);
static int       tdEncodeKVRecord(void **buf, SKVRecord *pRecord);
static void *    tdDecodeKVRecord(void *buf, SKVRecord *pRecord);
static int       tdRestoreKVStore(SKVStore *pStore);

int tdCreateKVStore(char *fname) {
  int fd = open(fname, O_RDWR | O_CREAT, 0755);
  if (fd < 0) {
    uError("failed to open file %s since %s", fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (tdInitKVStoreHeader(fd, fname) < 0) goto _err;

  if (fsync(fd) < 0) {
    uError("failed to fsync file %s since %s", fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  if (close(fd) < 0) {
    uError("failed to close file %s since %s", fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  return 0;

_err:
  if (fd >= 0) close(fd);
  (void)remove(fname);
  return -1;
}

int tdDestroyKVStore(char *fname) {
  if (remove(fname) < 0) {
    uError("failed to remove file %s since %s", fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return 0;
}

SKVStore *tdOpenKVStore(char *fname, iterFunc iFunc, afterFunc aFunc, void *appH) {
  SStoreInfo info = {0};
  uint32_t   version = 0;

  SKVStore *pStore = tdNewKVStore(fname, iFunc, aFunc, appH);
  if (pStore == NULL) return NULL;

  pStore->fd = open(pStore->fname, O_RDWR);
  if (pStore->fd < 0) {
    uError("failed to open file %s since %s", pStore->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  pStore->sfd = open(pStore->fsnap, O_RDONLY);
  if (pStore->sfd < 0) {
    if (errno != ENOENT) {
      uError("failed to open file %s since %s", pStore->fsnap, strerror(errno));
      terrno = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }
  } else {
    uDebug("file %s exists, try to recover the KV store", pStore->fsnap);
    if (tdLoadKVStoreHeader(pStore->sfd, pStore->fsnap, &info, &version) < 0) {
      if (terrno != TSDB_CODE_COM_FILE_CORRUPTED) goto _err;
    } else {
      if (version != KVSTORE_FILE_VERSION) {
        uError("file %s version %u is not the same as program version %u, this may cause problem", pStore->fsnap,
               version, KVSTORE_FILE_VERSION);
      }

      if (taosFtruncate(pStore->fd, info.size) < 0) {
        uError("failed to truncate %s to %" PRId64 " size since %s", pStore->fname, info.size, strerror(errno));
        terrno = TAOS_SYSTEM_ERROR(errno);
        goto _err;
      }

      if (tdUpdateKVStoreHeader(pStore->fd, pStore->fname, &info) < 0) goto _err;
      if (fsync(pStore->fd) < 0) {
        uError("failed to fsync file %s since %s", pStore->fname, strerror(errno));
        goto _err;
      }
    }

    close(pStore->sfd);
    pStore->sfd = -1;
    (void)remove(pStore->fsnap);
  }

  if (tdLoadKVStoreHeader(pStore->fd, pStore->fname, &info, &version) < 0) goto _err;
  if (version != KVSTORE_FILE_VERSION) {
    uError("file %s version %u is not the same as program version %u, this may cause problem", pStore->fname, version,
           KVSTORE_FILE_VERSION);
  }

  pStore->info.size = TD_KVSTORE_HEADER_SIZE;
  pStore->info.magic = info.magic;

  if (tdRestoreKVStore(pStore) < 0) goto _err;

  close(pStore->fd);
  pStore->fd = -1;

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

void tdCloseKVStore(SKVStore *pStore) { tdFreeKVStore(pStore); }

int tdKVStoreStartCommit(SKVStore *pStore) {
  ASSERT(pStore->fd < 0);

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

  if (taosSendFile(pStore->sfd, pStore->fd, NULL, TD_KVSTORE_HEADER_SIZE) < TD_KVSTORE_HEADER_SIZE) {
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

  if (lseek(pStore->fd, 0, SEEK_END) < 0) {
    uError("failed to lseek file %s since %s", pStore->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  ASSERT(pStore->info.size == lseek(pStore->fd, 0, SEEK_CUR));

  return 0;

_err:
  if (pStore->sfd > 0) {
    close(pStore->sfd);
    pStore->sfd = -1;
    (void)remove(pStore->fsnap);
  }
  if (pStore->fd > 0) {
    close(pStore->fd);
    pStore->fd = -1;
  }
  return -1;
}

int tdUpdateKVStoreRecord(SKVStore *pStore, uint64_t uid, void *cont, int contLen) {
  SKVRecord rInfo = {0};
  char      buf[64] = "\0";
  char *    pBuf = buf;

  rInfo.offset = lseek(pStore->fd, 0, SEEK_CUR);
  if (rInfo.offset < 0) {
    uError("failed to lseek file %s since %s", pStore->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  rInfo.uid = uid;
  rInfo.size = contLen;

  int tlen = tdEncodeKVRecord((void *)(&pBuf), &rInfo);
  ASSERT(tlen == POINTER_DISTANCE(pBuf, buf));
  ASSERT(tlen == sizeof(SKVRecord));

  if (taosWrite(pStore->fd, buf, tlen) < tlen) {
    uError("failed to write %d bytes to file %s since %s", tlen, pStore->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (taosWrite(pStore->fd, cont, contLen) < contLen) {
    uError("failed to write %d bytes to file %s since %s", contLen, pStore->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  pStore->info.magic =
      taosCalcChecksum(pStore->info.magic, (uint8_t *)POINTER_SHIFT(cont, contLen - sizeof(TSCKSUM)), sizeof(TSCKSUM));
  pStore->info.size += (sizeof(SKVRecord) + contLen);
  SKVRecord *pRecord = taosHashGet(pStore->map, (void *)&uid, sizeof(uid));
  if (pRecord != NULL) {  // just to insert
    pStore->info.tombSize += pRecord->size;
  } else {
    pStore->info.nRecords++;
  }

  taosHashPut(pStore->map, (void *)(&uid), sizeof(uid), (void *)(&rInfo), sizeof(rInfo));
  uTrace("put uid %" PRIu64 " into kvStore %s", uid, pStore->fname);

  return 0;
}

int tdDropKVStoreRecord(SKVStore *pStore, uint64_t uid) {
  SKVRecord rInfo = {0};
  char      buf[128] = "\0";

  SKVRecord *pRecord = taosHashGet(pStore->map, (void *)(&uid), sizeof(uid));
  if (pRecord == NULL) {
    uError("failed to drop KV store record with key %" PRIu64 " since not find", uid);
    return -1;
  }

  rInfo.offset = -pRecord->offset;
  rInfo.uid = pRecord->uid;
  rInfo.size = pRecord->size;

  void *pBuf = buf;
  tdEncodeKVRecord(&pBuf, &rInfo);

  if (taosWrite(pStore->fd, buf, POINTER_DISTANCE(pBuf, buf)) < POINTER_DISTANCE(pBuf, buf)) {
    uError("failed to write %" PRId64 " bytes to file %s since %s", (int64_t)(POINTER_DISTANCE(pBuf, buf)), pStore->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  pStore->info.magic = taosCalcChecksum(pStore->info.magic, (uint8_t *)buf, (uint32_t)POINTER_DISTANCE(pBuf, buf));
  pStore->info.size += POINTER_DISTANCE(pBuf, buf);
  pStore->info.nDels++;
  pStore->info.nRecords--;
  pStore->info.tombSize += (rInfo.size + sizeof(SKVRecord) * 2);

  taosHashRemove(pStore->map, (void *)(&uid), sizeof(uid));
  uDebug("drop uid %" PRIu64 " from KV store %s", uid, pStore->fname);

  return 0;
}

int tdKVStoreEndCommit(SKVStore *pStore) {
  ASSERT(pStore->fd > 0);

  if (tdUpdateKVStoreHeader(pStore->fd, pStore->fname, &(pStore->info)) < 0) return -1;

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
  pStore->fd = -1;

  (void)remove(pStore->fsnap);
  return 0;
}

void tsdbGetStoreInfo(char *fname, uint32_t *magic, int64_t *size) {
  char       buf[TD_KVSTORE_HEADER_SIZE] = "\0";
  SStoreInfo info = {0};

  int fd = open(fname, O_RDONLY);
  if (fd < 0) goto _err;

  if (taosRead(fd, buf, TD_KVSTORE_HEADER_SIZE) < TD_KVSTORE_HEADER_SIZE) goto _err;
  if (!taosCheckChecksumWhole((uint8_t *)buf, TD_KVSTORE_HEADER_SIZE)) goto _err;

  void *pBuf = (void *)buf;
  pBuf = tdDecodeStoreInfo(pBuf, &info);
  off_t offset = lseek(fd, 0, SEEK_END);
  if (offset < 0) goto _err;
  close(fd);

  *magic = info.magic;
  *size = offset;

  return;

_err:
  if (fd >= 0) close(fd);
  *magic = TD_KVSTORE_INIT_MAGIC;
  *size = 0;
}

static int tdLoadKVStoreHeader(int fd, char *fname, SStoreInfo *pInfo, uint32_t *version) {
  char buf[TD_KVSTORE_HEADER_SIZE] = "\0";

  if (lseek(fd, 0, SEEK_SET) < 0) {
    uError("failed to lseek file %s since %s", fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (taosRead(fd, buf, TD_KVSTORE_HEADER_SIZE) < TD_KVSTORE_HEADER_SIZE) {
    uError("failed to read %d bytes from file %s since %s", TD_KVSTORE_HEADER_SIZE, fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (!taosCheckChecksumWhole((uint8_t *)buf, TD_KVSTORE_HEADER_SIZE)) {
    uError("file %s is broken", fname);
    terrno = TSDB_CODE_COM_FILE_CORRUPTED;
    return -1;
  }

  void *pBuf = (void *)buf;
  pBuf = tdDecodeStoreInfo(pBuf, pInfo);
  pBuf = taosDecodeFixedU32(pBuf, version);

  return 0;
}

static int tdUpdateKVStoreHeader(int fd, char *fname, SStoreInfo *pInfo) {
  char buf[TD_KVSTORE_HEADER_SIZE] = "\0";

  if (lseek(fd, 0, SEEK_SET) < 0) {
    uError("failed to lseek file %s since %s", fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  void *pBuf = buf;
  tdEncodeStoreInfo(&pBuf, pInfo);
  taosEncodeFixedU32(&pBuf, KVSTORE_FILE_VERSION);
  ASSERT(POINTER_DISTANCE(pBuf, buf) + sizeof(TSCKSUM) <= TD_KVSTORE_HEADER_SIZE);

  taosCalcChecksumAppend(0, (uint8_t *)buf, TD_KVSTORE_HEADER_SIZE);
  if (taosWrite(fd, buf, TD_KVSTORE_HEADER_SIZE) < TD_KVSTORE_HEADER_SIZE) {
    uError("failed to write %d bytes to file %s since %s", TD_KVSTORE_HEADER_SIZE, fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return 0;
}

static int tdInitKVStoreHeader(int fd, char *fname) {
  SStoreInfo info = {TD_KVSTORE_HEADER_SIZE, 0, 0, 0, TD_KVSTORE_INIT_MAGIC};

  return tdUpdateKVStoreHeader(fd, fname, &info);
}

static int tdEncodeStoreInfo(void **buf, SStoreInfo *pInfo) {
  int tlen = 0;
  tlen += taosEncodeVariantI64(buf, pInfo->size);
  tlen += taosEncodeVariantI64(buf, pInfo->tombSize);
  tlen += taosEncodeVariantI64(buf, pInfo->nRecords);
  tlen += taosEncodeVariantI64(buf, pInfo->nDels);
  tlen += taosEncodeFixedU32(buf, pInfo->magic);

  return tlen;
}

static void *tdDecodeStoreInfo(void *buf, SStoreInfo *pInfo) {
  buf = taosDecodeVariantI64(buf, &(pInfo->size));
  buf = taosDecodeVariantI64(buf, &(pInfo->tombSize));
  buf = taosDecodeVariantI64(buf, &(pInfo->nRecords));
  buf = taosDecodeVariantI64(buf, &(pInfo->nDels));
  buf = taosDecodeFixedU32(buf, &(pInfo->magic));

  return buf;
}

static SKVStore *tdNewKVStore(char *fname, iterFunc iFunc, afterFunc aFunc, void *appH) {
  SKVStore *pStore = (SKVStore *)calloc(1, sizeof(SKVStore));
  if (pStore == NULL) goto _err;

  pStore->fname = strdup(fname);
  if (pStore->fname == NULL) {
    terrno = TSDB_CODE_COM_OUT_OF_MEMORY;
    goto _err;
  }

  pStore->fsnap = tdGetKVStoreSnapshotFname(fname);
  if (pStore->fsnap == NULL) {
    goto _err;
  }

  pStore->fnew = tdGetKVStoreNewFname(fname);
  if (pStore->fnew == NULL) goto _err;

  pStore->fd = -1;
  pStore->sfd = -1;
  pStore->nfd = -1;
  pStore->iFunc = iFunc;
  pStore->aFunc = aFunc;
  pStore->appH = appH;
  pStore->map = taosHashInit(4096, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, false);
  if (pStore->map == NULL) {
    terrno = TSDB_CODE_COM_OUT_OF_MEMORY;
    goto _err;
  }

  return pStore;

_err:
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

static int tdEncodeKVRecord(void **buf, SKVRecord *pRecord) {
  int tlen = 0;
  tlen += taosEncodeFixedU64(buf, pRecord->uid);
  tlen += taosEncodeFixedI64(buf, pRecord->offset);
  tlen += taosEncodeFixedI64(buf, pRecord->size);

  return tlen;
}

static void *tdDecodeKVRecord(void *buf, SKVRecord *pRecord) {
  buf = taosDecodeFixedU64(buf, &(pRecord->uid));
  buf = taosDecodeFixedI64(buf, &(pRecord->offset));
  buf = taosDecodeFixedI64(buf, &(pRecord->size));

  return buf;
}

static int tdRestoreKVStore(SKVStore *pStore) {
  char                  tbuf[128] = "\0";
  void *                buf = NULL;
  int64_t               maxBufSize = 0;
  SKVRecord             rInfo = {0};
  SHashMutableIterator *pIter = NULL;

  ASSERT(TD_KVSTORE_HEADER_SIZE == lseek(pStore->fd, 0, SEEK_CUR));
  ASSERT(pStore->info.size == TD_KVSTORE_HEADER_SIZE);

  while (true) {
    int64_t tsize = taosRead(pStore->fd, tbuf, sizeof(SKVRecord));
    if (tsize == 0) break;
    if (tsize < sizeof(SKVRecord)) {
      uError("failed to read %" PRIzu " bytes from file %s at offset %" PRId64 "since %s", sizeof(SKVRecord), pStore->fname,
             pStore->info.size, strerror(errno));
      terrno = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }

    char *pBuf = tdDecodeKVRecord(tbuf, &rInfo);
    ASSERT(POINTER_DISTANCE(pBuf, tbuf) == sizeof(SKVRecord));
    ASSERT((rInfo.offset > 0) ? (pStore->info.size == rInfo.offset) : true);

    if (rInfo.offset < 0) {
      taosHashRemove(pStore->map, (void *)(&rInfo.uid), sizeof(rInfo.uid));
      pStore->info.size += sizeof(SKVRecord);
      pStore->info.nRecords--;
      pStore->info.nDels++;
      pStore->info.tombSize += (rInfo.size + sizeof(SKVRecord) * 2);
    } else {
      ASSERT(rInfo.offset > 0 && rInfo.size > 0);
      if (taosHashPut(pStore->map, (void *)(&rInfo.uid), sizeof(rInfo.uid), &rInfo, sizeof(rInfo)) < 0) {
        uError("failed to put record in KV store %s", pStore->fname);
        terrno = TSDB_CODE_COM_OUT_OF_MEMORY;
        goto _err;
      }

      maxBufSize = MAX(maxBufSize, rInfo.size);

      if (lseek(pStore->fd, (off_t)rInfo.size, SEEK_CUR) < 0) {
        uError("failed to lseek file %s since %s", pStore->fname, strerror(errno));
        terrno = TAOS_SYSTEM_ERROR(errno);
        goto _err;
      }

      pStore->info.size += (sizeof(SKVRecord) + rInfo.size);
      pStore->info.nRecords++;
    }
  }

  buf = malloc((size_t)maxBufSize);
  if (buf == NULL) {
    uError("failed to allocate %" PRId64 " bytes in KV store %s", maxBufSize, pStore->fname);
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  pIter = taosHashCreateIter(pStore->map);
  if (pIter == NULL) {
    uError("failed to create hash iter while opening KV store %s", pStore->fname);
    terrno = TSDB_CODE_COM_OUT_OF_MEMORY;
    goto _err;
  }

  while (taosHashIterNext(pIter)) {
    SKVRecord *pRecord = taosHashIterGet(pIter);

    if (lseek(pStore->fd, (off_t)(pRecord->offset + sizeof(SKVRecord)), SEEK_SET) < 0) {
      uError("failed to lseek file %s since %s, offset %" PRId64, pStore->fname, strerror(errno), pRecord->offset);
      terrno = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }

    if (taosRead(pStore->fd, buf, (size_t)pRecord->size) < pRecord->size) {
      uError("failed to read %" PRId64 " bytes from file %s since %s, offset %" PRId64, pRecord->size, pStore->fname,
             strerror(errno), pRecord->offset);
      terrno = TAOS_SYSTEM_ERROR(errno);
      goto _err;
    }

    if (pStore->iFunc) {
      if ((*pStore->iFunc)(pStore->appH, buf, (int)pRecord->size) < 0) {
        uError("failed to restore record uid %" PRIu64 " in kv store %s at offset %" PRId64 " size %" PRId64
               " since %s",
               pRecord->uid, pStore->fname, pRecord->offset, pRecord->size, tstrerror(terrno));
        goto _err;
      }
    }
  }

  if (pStore->aFunc) (*pStore->aFunc)(pStore->appH);

  taosHashDestroyIter(pIter);
  tfree(buf);
  return 0;

_err:
  taosHashDestroyIter(pIter);
  tfree(buf);
  return -1;
}
