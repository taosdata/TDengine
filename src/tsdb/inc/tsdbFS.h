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
  uint32_t version;  // Current file version
  uint32_t len;
} SFSHeader;

// ================== TSDB File System Meta
typedef struct {
  uint64_t version;       // Commit version from 0 to increase
  int64_t  totalPoints;   // total points
  int64_t  totalStorage;  // Uncompressed total storage
} STsdbFSMeta;

// ==================
typedef struct {
  STsdbFSMeta meta;  // FS meta
  SMFile      mf;    // meta file
  SArray*     df;    // data file array
} SFSStatus;

typedef struct {
  pthread_rwlock_t lock;

  SFSStatus* cstatus;    // current stage
  SHashObj*  metaCache;  // meta

  bool       intxn;
  SFSStatus* nstatus;
  SList*     metaDelta;
} STsdbFS;

#define FS_CURRENT_STATUS(pfs) ((pfs)->cstatus)
#define FS_NEW_STATUS(pfs) ((pfs)->nstatus)
#define FS_IN_TXN(pfs) (pfs)->intxn

typedef struct {
  uint64_t   version;  // current FS version
  int        index;
  int        fid;
  SDFileSet* pSet;
} SFSIter;

#if 0
int        tsdbOpenFS(STsdbRepo* pRepo);
void       tsdbCloseFS(STsdbRepo* pRepo);
int        tsdbFSNewTxn(STsdbRepo* pRepo);
int        tsdbFSEndTxn(STsdbRepo* pRepo, bool hasError);
int        tsdbUpdateMFile(STsdbRepo* pRepo, SMFile* pMFile);
int        tsdbUpdateDFileSet(STsdbRepo* pRepo, SDFileSet* pSet);
int        tsdbInitFSIter(STsdbRepo* pRepo, SFSIter* pIter);
SDFileSet* tsdbFSIterNext(SFSIter* pIter);
#endif

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