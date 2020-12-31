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

#include <stdio.h>
#include <unistd.h>

#include "tsdbMain.h"

#define REPO_FS(r) ((r)->fs)
#define TSDB_MAX_DFILES(keep, days) ((keep) / (days) + 3)

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
  SFSSnapshot *pSnapshot = REPO_FS(pRepo)->new;
  SDFileSet *  pOldSet;

  pOldSet = tsdbSearchDFileSet(pSnapshot, pSet->id, TD_GE);
  if (pOldSet == NULL) {
    if (taosArrayPush(pSnapshot->df, pSet) == NULL) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      return -1;
    }
  } else {
    int index = TARRAY_ELEM_IDX(dfArray, ptr);

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
  SFSSnapshot *pSnapshot = REPO_FS(pRepo)->new;
  while (taosArrayGetSize(pSnapshot->df) > 0) {
    SDFileSet *pSet = (SDFileSet *)taosArrayGet(pSnapshot->df, 0);
    if (pSet->id < mfid) {
      taosArrayRemove(pSnapshot->df, 0);
    }
  }
}

static int tsdbSaveFSSnapshot(int fd, SFSSnapshot *pSnapshot) {
  // TODO
  return 0;
}

static int tsdbLoadFSSnapshot(SFSSnapshot *pSnapshot) {
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

static int tsdbEncodeMFInfo(void **buf, SMFInfo *pInfo) {
  int tlen = 0;

  tlen += taosEncodeVariantI64(buf, pInfo->size);
  tlen += taosEncodeVariantI64(buf, pInfo->tombSize);
  tlen += taosEncodeVariantI64(buf, pInfo->nRecords);
  tlen += taosEncodeVariantI64(buf, pInfo->nDels);
  tlen += taosEncodeFixedU32(buf, pInfo->magic);

  return tlen;
}

static void *tsdbDecodeMFInfo(void *buf, SMFInfo *pInfo) {
  buf = taosDecodeVariantI64(buf, &(pInfo->size));
  buf = taosDecodeVariantI64(buf, &(pInfo->tombSize));
  buf = taosDecodeVariantI64(buf, &(pInfo->nRecords));
  buf = taosDecodeVariantI64(buf, &(pInfo->nDels));
  buf = taosDecodeFixedU32(buf, &(pInfo->magic));

  return buf;
}

static int tsdbEncodeMFile(void **buf, SMFile *pMFile) {
  int tlen = 0;

  tlen += tsdbEncodeMFInfo(buf, &(pMFile->info));
  tlen += tfsEncodeFile(buf, &(pMFile->f));

  return tlen;
}

static void *tsdbDecodeMFile(void *buf, SMFile *pMFile) {
  buf = tsdbDecodeMFInfo(buf, &(pMFile->info));
  buf = tfsDecodeFile(buf, &(pMFile->f));

  return buf;
}

static int tsdbEncodeDFInfo(void **buf, SDFInfo *pInfo) {
  int tlen = 0;

  tlen += taosEncodeFixedU32(buf, pInfo->magic);
  tlen += taosEncodeFixedU32(buf, pInfo->len);
  tlen += taosEncodeFixedU32(buf, pInfo->totalBlocks);
  tlen += taosEncodeFixedU32(buf, pInfo->totalSubBlocks);
  tlen += taosEncodeFixedU32(buf, pInfo->offset);
  tlen += taosEncodeFixedU64(buf, pInfo->size);
  tlen += taosEncodeFixedU64(buf, pInfo->tombSize);

  return tlen;
}

static void *tsdbDecodeDFInfo(void *buf, SDFInfo *pInfo) {
  buf = taosDecodeFixedU32(buf, &(pInfo->magic));
  buf = taosDecodeFixedU32(buf, &(pInfo->len));
  buf = taosDecodeFixedU32(buf, &(pInfo->totalBlocks));
  buf = taosDecodeFixedU32(buf, &(pInfo->totalSubBlocks));
  buf = taosDecodeFixedU32(buf, &(pInfo->offset));
  buf = taosDecodeFixedU64(buf, &(pInfo->size));
  buf = taosDecodeFixedU64(buf, &(pInfo->tombSize));

  return buf;
}

static int tsdbEncodeDFile(void **buf, SDFile *pDFile) {
  int tlen = 0;

  tlen += tsdbEncodeDFInfo(buf, &(pDFile->info));
  tlen += tfsEncodeFile(buf, &(pDFile->f));

  return tlen;
}

static void *tsdbDecodeDFile(void *buf, SDFile *pDFile) {
  buf = tsdbDecodeDFInfo(buf, &(pDFile->info));
  buf = tfsDecodeFile(buf, &(pDFile->f));

  return buf;
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

static int tsdbEncodeFSSnapshot(void **buf, SFSSnapshot *pSnapshot) {
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

static void *tsdbDecodeFSSnapshot(void *buf, SFSSnapshot *pSnapshot) {
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

static SFSSnapshot *tsdbNewSnapshot(int32_t nfiles) {
  SFSSnapshot *pSnapshot;

  pSnapshot = (SFSSnapshot *)calloc(1, sizeof(pSnapshot));
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

static SFSSnapshot *tsdbFreeSnapshot(SFSSnapshot *pSnapshot) {
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

static int tsdbCopySnapshot(SFSSnapshot *src, SFSSnapshot *dst) {
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

static SDFileSet *tsdbSearchDFileSet(SFSSnapshot *pSnapshot, int fid, int flags) {
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