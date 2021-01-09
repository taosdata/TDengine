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

typedef struct {
  int64_t fsversion; // file system version, related to program
  int64_t version;
  int64_t totalPoints;
  int64_t totalStorage;
} STsdbFSMeta;

typedef struct {
  int64_t     version;
  STsdbFSMeta meta;
  SMFile      mf;  // meta file
  SArray*     df;  // data file array
} SFSVer;

typedef struct {
  pthread_rwlock_t lock;

  SFSVer fsv;
} STsdbFS;

typedef struct {
  int        version;  // current FS version
  int        index;
  int        fid;
  SDFileSet* pSet;
} SFSIter;

int        tsdbOpenFS(STsdbRepo* pRepo);
void       tsdbCloseFS(STsdbRepo* pRepo);
int        tsdbFSNewTxn(STsdbRepo* pRepo);
int        tsdbFSEndTxn(STsdbRepo* pRepo, bool hasError);
int        tsdbUpdateMFile(STsdbRepo* pRepo, SMFile* pMFile);
int        tsdbUpdateDFileSet(STsdbRepo* pRepo, SDFileSet* pSet);
void       tsdbRemoveExpiredDFileSet(STsdbRepo* pRepo, int mfid);
int        tsdbRemoveDFileSet(SDFileSet* pSet);
int        tsdbEncodeMFInfo(void** buf, SMFInfo* pInfo);
void*      tsdbDecodeMFInfo(void* buf, SMFInfo* pInfo);
SDFileSet  tsdbMoveDFileSet(SDFileSet* pOldSet, int to);
int        tsdbInitFSIter(STsdbRepo* pRepo, SFSIter* pIter);
SDFileSet* tsdbFSIterNext(SFSIter* pIter);
int        tsdbCreateDFileSet(int fid, int level, SDFileSet* pSet);

static FORCE_INLINE int tsdbRLockFS(STsdbFS *pFs) {
  int code = pthread_rwlock_rdlock(&(pFs->lock));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  return 0;
}

static FORCE_INLINE int tsdbWLockFS(STsdbFS *pFs) {
  int code = pthread_rwlock_wrlock(&(pFs->lock));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    return -1;
  }
  return 0;
}

static FORCE_INLINE int tsdbUnLockFS(STsdbFS *pFs) {
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