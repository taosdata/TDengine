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

#ifndef _TD_TSDB_FS_H_
#define _TD_TSDB_FS_H_

#ifdef __cplusplus
extern "C" {
#endif

#define TSDB_FS_VERSION 0

// ================== CURRENT file header info
typedef struct {
  uint32_t version;  // Current file system version (relating to code)
  uint32_t len;      // Encode content length (including checksum)
} SFSHeader;

// ================== TSDB File System Meta
typedef struct {
  uint32_t version;       // Commit version from 0 to increase
  int64_t  totalPoints;   // total points
  int64_t  totalStorage;  // Uncompressed total storage
} STsdbFSMeta;

// ==================
typedef struct {
  STsdbFSMeta meta;  // FS meta
  SMFile*     pmf;   // meta file pointer
  SMFile      mf;    // meta file
  SArray*     df;    // data file array
} SFSStatus;

typedef struct {
  pthread_rwlock_t lock;

  SFSStatus* cstatus;    // current status
  SHashObj*  metaCache;  // meta cache
  bool       intxn;
  SFSStatus* nstatus;  // new status
} STsdbFS;

#define FS_CURRENT_STATUS(pfs) ((pfs)->cstatus)
#define FS_NEW_STATUS(pfs) ((pfs)->nstatus)
#define FS_IN_TXN(pfs) (pfs)->intxn
#define FS_TXN_VERSION(pfs) ((pfs)->nstatus->meta.version)

typedef struct {
  int        direction;
  uint64_t   version;  // current FS version
  STsdbFS*   pfs;
  int        index;  // used to position next fset when version the same
  int        fid;    // used to seek when version is changed
  SDFileSet* pSet;
} SFSIter;

#define TSDB_FS_ITER_FORWARD TSDB_ORDER_ASC
#define TSDB_FS_ITER_BACKWARD TSDB_ORDER_DESC

STsdbFS *tsdbNewFS(STsdbCfg *pCfg);
void *   tsdbFreeFS(STsdbFS *pfs);
int      tsdbOpenFS(STsdbRepo *pRepo);
void     tsdbCloseFS(STsdbRepo *pRepo);
void     tsdbStartFSTxn(STsdbRepo *pRepo, int64_t pointsAdd, int64_t storageAdd);
int      tsdbEndFSTxn(STsdbRepo *pRepo);
int      tsdbEndFSTxnWithError(STsdbFS *pfs);
void     tsdbUpdateFSTxnMeta(STsdbFS *pfs, STsdbFSMeta *pMeta);
void     tsdbUpdateMFile(STsdbFS *pfs, const SMFile *pMFile);
int      tsdbUpdateDFileSet(STsdbFS *pfs, const SDFileSet *pSet);

void       tsdbFSIterInit(SFSIter *pIter, STsdbFS *pfs, int direction);
void       tsdbFSIterSeek(SFSIter *pIter, int fid);
SDFileSet *tsdbFSIterNext(SFSIter *pIter);

static FORCE_INLINE int tsdbRLockFS(STsdbFS* pFs) {
  int code = pthread_rwlock_rdlock(&(pFs->lock));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  return 0;
}

static FORCE_INLINE int tsdbWLockFS(STsdbFS* pFs) {
  int code = pthread_rwlock_wrlock(&(pFs->lock));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  return 0;
}

static FORCE_INLINE int tsdbUnLockFS(STsdbFS* pFs) {
  int code = pthread_rwlock_unlock(&(pFs->lock));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  return 0;
}

#ifdef __cplusplus
}
#endif

#endif /* _TD_TSDB_FS_H_ */