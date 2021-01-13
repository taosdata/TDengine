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

#include "tsdbint.h"

#define TSDB_MAX_FSETS(keep, days) ((keep) / (days) + 3)

// ================== CURRENT file header info
static int tsdbEncodeFSHeader(void **buf, SFSHeader *pHeader) {
  int tlen = 0;

  tlen += taosEncodeFixedU32(buf, pHeader->version);
  tlen += taosEncodeFixedU32(buf, pHeader->len);

  return tlen;
}

static void *tsdbEncodeFSHeader(void *buf, SFSHeader *pHeader) {
  buf = taosEncodeFixedU32(buf, &(pHeader->version));
  buf = taosEncodeFixedU32(buf, &(pHeader->len));

  return buf;
}

// ================== STsdbFSMeta
static int tsdbEncodeFSMeta(void **buf, STsdbFSMeta *pMeta) {
  int tlen = 0;

  tlen += taosEncodeFixedU64(buf, pMeta->version);
  tlen += taosEncodeFixedI64(buf, pMeta->totalPoints);
  tlen += taosEncodeFixedI64(buf, pMeta->totalStorage);

  return tlen;
}

static void *tsdbDecodeFSMeta(void *buf, STsdbFSMeta *pMeta) {
  buf = taosDecodeFixedU64(buf, &(pMeta->version));
  buf = taosDecodeFixedI64(buf, &(pMeta->totalPoints));
  buf = taosDecodeFixedI64(buf, &(pMeta->totalStorage));

  return buf;
}

// ================== SFSStatus
static int tsdbEncodeDFileSetArray(void **buf, SArray *pArray) {
  int      tlen = 0;
  uint64_t nset = taosArrayGetSize(pArray);

  tlen += taosEncodeFixedU64(buf, nset);
  for (size_t i = 0; i < nset; i++) {
    SDFileSet *pSet = taosArrayGet(pArray, i);

    tlen += tsdbEncodeDFileSet(buf, pSet);
  }

  return tlen;
}

static int tsdbDecodeDFileSetArray(void *buf, SArray *pArray) {
  uint64_t  nset;
  SDFileSet dset;

  taosArrayClear(pArray);

  buf = taosDecodeFixedU64(buf, &nset);
  for (size_t i = 0; i < nset; i++) {
    SDFileSet *pSet = taosArrayGet(pArray, i);

    buf = tsdbDecodeDFileSet(buf, &dset);
    taosArrayPush(pArray, (void *)(&dset));
  }
  return buf;
}

static int tsdbEncodeFSStatus(void **buf, SFSStatus *pStatus) {
  int tlen = 0;

  tlen += tsdbEncodeFSMeta(buf, &(pStatus->meta));
  tlen += tsdbEncodeSMFile(buf, &(pStatus->mf));
  tlen += tsdbEncodeDFileSetArray(buf, pStatus->df);

  return tlen;
}

static void *tsdbDecodeFSStatus(void *buf, SFSStatus *pStatus) {
  buf = taosDecodeFixedU32(buf, pStatus->fsVer);
  buf = tsdbDecodeFSMeta(buf, &(pStatus->meta));
  buf = tsdbDecodeSMFile(buf, &(pStatus->mf));
  buf = tsdbDecodeDFileSetArray(buf, pStatus->df);

  return buf;
}

static SFSStatus *tsdbNewFSStatus(int maxFSet) {
  SFSStatus *pStatus = (SFSStatus *)calloc(1, sizeof(*pStatus));
  if (pStatus == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return NULL;
  }

  pStatus->df = taosArrayInit(maxFSet, sizeof(SDFileSet));
  if (pStatus->df) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    free(pStatus);
    return NULL;
  }

  return pStatus;
}

static SFSStatus *tsdbFreeFSStatus(SFSStatus *pStatus) {
  if (pStatus) {
    pStatus->df = taosArrayDestroy(pStatus->df);
    free(pStatus);
  }

  return NULL;
}

static void tsdbResetFSStatus(SFSStatus *pStatus) {
  if (pStatus == NULL) {
    return;
  }

  taosArrayClear(pStatus->df);
}

// ================== STsdbFS
STsdbFS *tsdbNewFS(int maxFSet) {
  STsdbFS *pFs = (STsdbFS *)calloc(1, sizeof(*pFs));
  if (pFs == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return NULL;
  }

  int code = pthread_rwlock_init(&(pFs->lock), NULL);
  if (code) {
    terrno = TAOS_SYSTEM_ERROR(code);
    free(pFs);
    return NULL;
  }

  pFs->cstatus = tsdbNewFSStatus(maxFSet);
  if (pFs->cstatus == NULL) {
    tsdbFreeFS(pFs);
    return NULL;
  }

  pFs->metaCache = taosHashInit(4096, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, false);
  if (pFs->metaCache == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbFreeFS(pFs);
    return NULL;
  }

  pFs->nstatus = tsdbNewFSStatus(maxFSet);
  if (pFs->nstatus == NULL) {
    tsdbFreeFS(pFs);
    return NULL;
  }

  pFs->metaDelta = tdListNew(sizeof(SKVRecord));
  if (pFs->metaDelta == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbFreeFS(pFs);
    return NULL;
  }

  return NULL;
}

void *tsdbFreeFS(STsdbFS *pFs) {
  if (pFs) {
    pFs->metaDelta = tdListFree(pFs->metaDelta);
    pFs->nstatus = tsdbFreeFSStatus(pFs->nstatus);
    taosHashCleanup(pFs->metaCache);
    pFs->metaCache = NULL;
    pFs->cstatus = tsdbFreeFSStatus(pFs->cstatus);
    pthread_rwlock_destroy(&(pFs->lock));
  }
  return NULL;
}

int tsdbOpenFS(STsdbFS *pFs, int keep, int days) {
  // TODO

  return 0;
}

void tsdbCloseFS(STsdbFS *pFs) {
  // TODO
}

int tsdbStartTxn(STsdbFS *pFs) {
  tsdbResetFSStatus(pFs->nstatus);
  tdListEmpty(pFs->metaDelta);
  return 0;
}

int tsdbEndTxn(STsdbFS *pFs, bool hasError) {
  SFSStatus *pTStatus;

  if (hasError) {
    // TODO
  } else {
    // TODO 1. Create and open a new file current.t

    // TODO 2. write new status to new file and fysnc and close

    // TODO 3. rename current.t to current

    // TODO 4. apply change to file
    tsdbWLockFS(pFs);
    pTStatus = pFs->cstatus;
    pFs->cstatus = pFs->nstatus;
    pFs->nstatus = pTStatus;
    tsdbUnLockFS(pFs);

    // TODO 5: apply meta change to cache
  }

  return 0;
}

// ================== SFSIter
void tsdbFSIterInit(STsdbFS *pFs, SFSIter *pIter) {
  // TODO
}

SDFileSet *tsdbFSIterNext(STsdbFS *pFs) {
  // TODO
  return NULL;
}

#if 0
int tsdbOpenFS(STsdbRepo *pRepo) {
  ASSERT(REPO_FS == NULL);

  STsdbCfg *pCfg = TSDB_CFG(pRepo);

  // Create fs object
  REPO_FS(pRepo) = tsdbNewFS(pCfg->keep, pCfg->daysPerFile);
  if (REPO_FS(pRepo) == NULL) {
    tsdbError("vgId:%d failed to open TSDB FS since %s", REPO_ID(pRepo), tstrerror(terrno));
    return -1;
  }

  // Load TSDB file system from disk
  if (tsdbOpenFSImpl(pRepo) < 0) {
    tsdbError("vgId:%d failed to open TSDB FS since %s", REPO_ID(pRepo), tstrerror(terrno));
    tsdbCloseFS(pRepo);
    return -1;
  }

  return 0;
}

void tsdbCloseFS(STsdbRepo *pRepo) {
  REPO_FS(pRepo) = tsdbFreeFS(REPO_FS(pRepo));
  return 0;
}

// Start a new FS transaction
int tsdbFSNewTxn(STsdbRepo *pRepo) {
  STsdbFS *pFs = REPO_FS(pRepo);

  if (tsdbCopySnapshot(pFs->curr, pFs->new) < 0) {
    return -1;
  }

  pFs->new->version++;

  return 0;
}

// End an existing FS transaction
int tsdbFSEndTxn(STsdbRepo *pRepo, bool hasError) {
  STsdbFS *pFs = REPO_FS(pRepo);

  if (hasError) { // roll back files

  } else { // apply file change
    if (tsdbSaveFSSnapshot(-1, pFs->new) < 0) {
      // TODO
    }

    // rename();

    // apply all file changes

  }

  return 0;
}

int tsdbUpdateMFile(STsdbRepo *pRepo, SMFile *pMFile) {
  STsdbFS *pFs = REPO_FS(pRepo);
  pFs->new->mf = *pMFile;
  return 0;
}

int tsdbUpdateDFileSet(STsdbRepo *pRepo, SDFileSet *pSet) {
  SFSStatus *pSnapshot = REPO_FS(pRepo)->new;
  SDFileSet *  pOldSet;

  pOldSet = tsdbSearchDFileSet(pSnapshot, pSet->id, TD_GE);
  if (pOldSet == NULL) {
    if (taosArrayPush(pSnapshot->df, pSet) == NULL) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      return -1;
    }
  } else {
    int index = TARRAY_ELEM_IDX(pSnapshot->df, pOldSet);

    if (pOldSet->id == pSet->id) {
      taosArraySet(pSnapshot->df, index, pSet);
    } else if (pOldSet->id > pSet->id) {
      if (taosArrayInsert(pSnapshot->df, index, pSet) == NULL) {
        terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
        return -1;
      }
    } else {
      ASSERT(0);
    }
  }

  return 0;
}

void tsdbRemoveExpiredDFileSet(STsdbRepo *pRepo, int mfid) {
  SFSStatus *pSnapshot = REPO_FS(pRepo)->new;
  while (taosArrayGetSize(pSnapshot->df) > 0) {
    SDFileSet *pSet = (SDFileSet *)taosArrayGet(pSnapshot->df, 0);
    if (pSet->id < mfid) {
      taosArrayRemove(pSnapshot->df, 0);
    }
  }
}


SDFileSet tsdbMoveDFileSet(SDFileSet *pOldSet, int to) {
  // TODO
}

int tsdbInitFSIter(STsdbRepo *pRepo, SFSIter *pIter) {
  // TODO
  return 0;
}

SDFileSet *tsdbFSIterNext(SFSIter *pIter) {
  // TODO
  return NULL;
}

static int tsdbSaveFSSnapshot(int fd, SFSStatus *pSnapshot) {
  // TODO
  return 0;
}

static int tsdbLoadFSSnapshot(SFSStatus *pSnapshot) {
  // TODO
  return 0;
}

static int tsdbOpenFSImpl(STsdbRepo *pRepo) {
  char manifest[TSDB_FILENAME_LEN] = "\0";

  // TODO: use API here
  sprintf(manifest, "%s/manifest", pRepo->rootDir);

  if (access(manifest, F_OK) == 0) {
    // manifest file exist, just load
    // TODO
  } else {
    // manifest file not exists, scan all the files and construct 
    // TODO
  }

  return 0;
}


static int tsdbEncodeFSMeta(void **buf, STsdbFSMeta *pMeta) {
  int tlen = 0;

  tlen += taosEncodeVariantI64(buf, pMeta->fsversion);
  tlen += taosEncodeVariantI64(buf, pMeta->version);
  tlen += taosEncodeVariantI64(buf, pMeta->totalPoints);
  tlen += taosEncodeVariantI64(buf, pMeta->totalStorage);

  return tlen;
}

static void *tsdbDecodeFSMeta(void *buf, STsdbFSMeta *pMeta) {
  buf = taosDecodeVariantI64(buf, &(pMeta->fsversion));
  buf = taosDecodeVariantI64(buf, &(pMeta->version));
  buf = taosDecodeVariantI64(buf, &(pMeta->totalPoints));
  buf = taosDecodeVariantI64(buf, &(pMeta->totalStorage));

  return buf;
}

static int tsdbEncodeFSSnapshot(void **buf, SFSStatus *pSnapshot) {
  int     tlen = 0;
  int64_t size = 0;

  // Encode meta file
  tlen += tsdbEncodeMFile(buf, &(pSnapshot->mf));

  // Encode data files
  size = taosArrayGetSize(pSnapshot->df);
  tlen += taosEncodeVariantI64(buf, size);
  for (size_t index = 0; index < size; index++) {
    SDFile *pFile = taosArrayGet(pSnapshot->df, index);
    
    tlen += tsdbEncodeDFInfo(buf, &pFile);
  }
  

  return tlen;
}

static void *tsdbDecodeFSSnapshot(void *buf, SFSStatus *pSnapshot) {
  int64_t size = 0;
  SDFile  df;

  // Decode meta file
  buf = tsdbDecodeMFile(buf, &(pSnapshot->mf));

  // Decode data files
  buf = taosDecodeVariantI64(buf, &size);
  for (size_t index = 0; index < size; index++) {
    buf = tsdbDecodeDFInfo(buf, &df);
    taosArrayPush(pSnapshot->df, (void *)(&df));
  }

  return buf;
}

static SFSStatus *tsdbNewSnapshot(int32_t nfiles) {
  SFSStatus *pSnapshot;

  pSnapshot = (SFSStatus *)calloc(1, sizeof(pSnapshot));
  if (pSnapshot == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return NULL;
  }

  pSnapshot->df = taosArrayInit(nfiles, sizeof(SDFileSet));
  if (pSnapshot->df == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    free(pSnapshot);
    return NULL;
  }

  return pSnapshot;
}

static SFSStatus *tsdbFreeSnapshot(SFSStatus *pSnapshot) {
  if (pSnapshot) {
    taosArrayDestroy(pSnapshot->df);
    free(pSnapshot);
  }

  return NULL;
}

static STsdbFS *tsdbNewFS(int32_t keep, int32_t days) {
  STsdbFS *pFs;
  int      code;
  int32_t  nfiles;

  pFs = (STsdbFS *)calloc(1, sizeof(*pFs));
  if (pFs == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return NULL;
  }

  code = pthread_rwlock_init(&(pFs->lock));
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    free(pFs);
    return NULL;
  }

  nfiles = TSDB_MAX_DFILES(keep, days);
  if (((pFs->curr = tsdbNewSnapshot(nfiles)) == NULL) || ((pFs->new = tsdbNewSnapshot(nfiles)) == NULL)) {
    tsdbFreeFS(pFs);
    return NULL;
  }

  return pFs;
}

static STsdbFS *tsdbFreeFS(STsdbFS *pFs) {
  if (pFs) {
    pFs->new = tsdbFreeSnapshot(pFs->new);
    pFs->curr = tsdbFreeSnapshot(pFs->curr);
    pthread_rwlock_destroy(&(pFs->lock));
    free(pFs);
  }

  return NULL;
}

static int tsdbCopySnapshot(SFSStatus *src, SFSStatus *dst) {
  dst->meta = src->meta;
  dst->mf = src->meta;
  taosArrayCopy(dst->df, src->df);
  return 0;
}

static int tsdbCompFSetId(const void *key1, const void *key2) {
  int        id = *(int *)key1;
  SDFileSet *pSet = (SDFileSet *)key2;

  if (id < pSet->id) {
    return -1;
  } else if (id == pSet->id) {
    return 0;
  } else {
    return 1;
  }
}

static SDFileSet *tsdbSearchDFileSet(SFSStatus *pSnapshot, int fid, int flags) {
  void *ptr = taosArraySearch(pSnapshot->df, (void *)(&fid), tsdbCompFSetId, flags);
  return (ptr == NULL) ? NULL : ((SDFileSet *)ptr);
}

static int tsdbMakeFSChange(STsdbRepo *pRepo) {
  tsdbMakeFSMFileChange(pRepo);
  tsdbMakeFSDFileChange(pRepo);
  return 0;
}

static int tsdbMakeFSMFileChange(STsdbRepo *pRepo) {
  STsdbFS *pFs = REPO_FS(pRepo);
  SMFile * pDstMFile = &(pFs->curr->mf);
  SMFile * pSrcMFile = &(pFs->new->mf);

  if (tfsIsSameFile(&(pDstMFile->f), &(pSrcMFile->f))) { // the same file
    if (pDstMFile->info != pSrcMFile->info) {
      if (pDstMFile->info.size > pDstMFile->info.size) {
        // Commit succeed, do nothing
      } else if (pDstMFile->info.size < pDstMFile->info.size) {
        // Commit failed, back
        // TODO
      } else {
        ASSERT(0);
      }
    }
  } else {
    tfsremove(&(pSrcMFile->f));
  }

  return 0;
}

static int tsdbMakeFSDFileChange(STsdbRepo *pRepo) {
  STsdbFS *  pFs = REPO_FS(pRepo);
  int        cidx = 0;
  int        nidx = 0;
  SDFileSet *pCSet = NULL;
  SDFileSet *pNSet = NULL;

  if (cidx < taosArrayGetSize(pFs->curr->df)) {
    pCSet = taosArrayGet(pFs->curr->df, cidx);
  } else {
    pCSet = NULL;
  }

  if (nidx < taosArrayGetSize(pFs->new->df)) {
    pNSet = taosArrayGet(pFs->new->df, nidx);
  } else {
    pNSet = NULL;
  }

  while (true) {
    if (pCSet == NULL && pNSet == NULL) break;

    if (pCSet == NULL || (pNSet != NULL && pCSet->id > pNSet->id)) {
      tsdbRemoveDFileSet(pNSet);

      nidx++;
      if (nidx < taosArrayGetSize(pFs->new->df)) {
        pNSet = taosArrayGet(pFs->new->df, nidx);
      } else {
        pNSet = NULL;
      }
    } else if (pNSet == NULL || (pCSet != NULL && pCSet->id < pNSet->id)) {
      cidx++;
      if (cidx < taosArrayGetSize(pFs->curr->df)) {
        pCSet = taosArrayGet(pFs->curr->df, cidx);
      } else {
        pCSet = NULL;
      }
    } else {
      // TODO: apply dfileset change
      nidx++;
      if (nidx < taosArrayGetSize(pFs->new->df)) {
        pNSet = taosArrayGet(pFs->new->df, nidx);
      } else {
        pNSet = NULL;
      }

      cidx++;
      if (cidx < taosArrayGetSize(pFs->curr->df)) {
        pCSet = taosArrayGet(pFs->curr->df, cidx);
      } else {
        pCSet = NULL;
      }
    }
  }

  return 0;
}
#endif