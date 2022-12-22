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

#include "tsdb.h"

typedef struct {
} SMemDIter;

typedef struct {
  SDataFReader *pReader;
  SArray       *aBlockIdx;  // SArray<SBlockIdx>
  SMapData      mDataBlk;   // SMapData<SDataBlk>
  SBlockData    bData;
  int32_t       iBlockIdx;
  int32_t       iDataBlk;
  int32_t       iRow;
} SDataDIter;

typedef struct {
  SDataFReader *pReader;
  int32_t       iStt;
  SArray       *aSttBlk;  // SArray<SSttBlk>
  SBlockData    bData;
  int32_t       iSttBlk;
  int32_t       iRow;
} SSttDIter;

typedef struct {
  int32_t     flag;
  SRowInfo    rowInfo;
  SRBTreeNode n;
  char        handle[];
} STsdbDataIter;

typedef struct {
  STsdb        *pTsdb;
  STsdbFS       fs;
  int64_t       cid;
  int32_t       fid;
  SDataFReader *pReader;
  SDFileSet    *pDFileSet;
  SRBTree       rtree;
} STsdbCompactor;

#define TSDB_FLG_DEEP_COMPACT 0x1

// ITER =========================
static int32_t tsdbDataIterCmprFn(const SRBTreeNode *n1, const SRBTreeNode *n2) {
  const STsdbDataIter *pIter1 = (STsdbDataIter *)((char *)n1 - offsetof(STsdbDataIter, n));
  const STsdbDataIter *pIter2 = (STsdbDataIter *)((char *)n2 - offsetof(STsdbDataIter, n));

  return tRowInfoCmprFn(&pIter1->rowInfo, &pIter2->rowInfo);
}

static int32_t tsdbMemDIterOpen(STsdbDataIter **ppIter) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdbDataIter *pIter = (STsdbDataIter *)taosMemoryCalloc(1, sizeof(*pIter) + sizeof(SMemDIter));
  if (pIter == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  // TODO

_exit:
  if (code) {
    *ppIter = NULL;
  } else {
    *ppIter = pIter;
  }
  return code;
}

static int32_t tsdbDataDIterOpen(SDataFReader *pReader, STsdbDataIter **ppIter) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdbDataIter *pIter = (STsdbDataIter *)taosMemoryCalloc(1, sizeof(*pIter) + sizeof(SDataDIter));
  if (NULL == pIter) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  SDataDIter *pDataDIter = (SDataDIter *)pIter->handle;
  pDataDIter->pReader = pReader;
  pDataDIter->aBlockIdx = taosArrayInit(0, sizeof(SBlockIdx));
  if (pDataDIter->aBlockIdx == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  code = tsdbReadBlockIdx(pReader, pDataDIter->aBlockIdx);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (taosArrayGetSize(pDataDIter->aBlockIdx) == 0) goto _clear_exit;

  code = tBlockDataCreate(&pDataDIter->bData);
  TSDB_CHECK_CODE(code, lino, _exit);

  // read first data block
  pDataDIter->iBlockIdx = 0;
  code = tsdbReadDataBlk(pReader, taosArrayGet(pDataDIter->aBlockIdx, pDataDIter->iBlockIdx), &pDataDIter->mDataBlk);
  TSDB_CHECK_CODE(code, lino, _exit);

  pDataDIter->iDataBlk = 0;
  // code = tsdbReadDataBlock(pReader, tMapDat);
  // TSDB_CHECK_CODE(code, lino, _exit);

  pDataDIter->iRow = 0;

  // TODO

_exit:
  if (code) {
  _clear_exit:
    *ppIter = NULL;
    if (pIter) {
      tBlockDataDestroy(&pDataDIter->bData, 1);
      tMapDataClear(&pDataDIter->mDataBlk);
      taosArrayDestroy(pDataDIter->aBlockIdx);
      taosMemoryFree(pIter);
    }
  } else {
    *ppIter = pIter;
  }
  return code;
}

static int32_t tsdbSttDIterOpen(SDataFReader *pReader, int32_t iStt, STsdbDataIter **ppIter) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdbDataIter *pIter = (STsdbDataIter *)taosMemoryCalloc(1, sizeof(*pIter) + sizeof(SSttDIter));
  if (pIter == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  SSttDIter *pSttDIter = (SSttDIter *)pIter->handle;
  pSttDIter->pReader = pReader;
  pSttDIter->iStt = iStt;
  pSttDIter->aSttBlk = taosArrayInit(0, sizeof(SSttBlk));
  if (pSttDIter->aSttBlk == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  code = tsdbReadSttBlk(pReader, pSttDIter->iStt, pSttDIter->aSttBlk);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (taosArrayGetSize(pSttDIter->aSttBlk) == 0) goto _clear_exit;

  code = tBlockDataCreate(&pSttDIter->bData);
  TSDB_CHECK_CODE(code, lino, _exit);

  pSttDIter->iSttBlk = 0;
  // code = tsdbReadSttBlock(pReader, taosArrayGet(pSttDIter->aSttBlk, pSttDIter->iSttBlk), &pSttDIter->bData);
  // TSDB_CHECK_CODE(code, lino, _exit);

  pSttDIter->iRow = 0;

  // TODO

_exit:
  if (code) {
  _clear_exit:
    *ppIter = NULL;
    if (pIter) {
      tBlockDataDestroy(&pSttDIter->bData, 1);
      taosArrayDestroy(pSttDIter->aSttBlk);
      taosMemoryFree(pIter);
    }
  } else {
    *ppIter = pIter;
  }
  return code;
}

static void tsdbDataIterClose(STsdbDataIter *pIter) {
  // TODO
}

static int32_t tsdbDataIterNext(STsdbDataIter *pIter) {
  int32_t code = 0;
  int32_t lino = 0;
  // TODO
_exit:
  return code;
}

// COMPACT =========================
static int32_t tsdbBeginCompact(STsdb *pTsdb, STsdbCompactor *pCompactor) {
  int32_t code = 0;
  int32_t lino = 0;

  pCompactor->pTsdb = pTsdb;

  code = tsdbFSCopy(pTsdb, &pCompactor->fs);
  TSDB_CHECK_CODE(code, lino, _exit);

  pCompactor->fid = INT32_MIN;

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbCommitCompact(STsdbCompactor *pCompactor) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdb *pTsdb = pCompactor->pTsdb;

  // TODO

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbAbortCompact(STsdbCompactor *pCompactor) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdb *pTsdb = pCompactor->pTsdb;

  // TODO

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbDeepCompact(STsdbCompactor *pCompactor) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdb *pTsdb = pCompactor->pTsdb;

  code = tsdbDataFReaderOpen(&pCompactor->pReader, pTsdb, pCompactor->pDFileSet);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbShallowCompact(STsdbCompactor *pCompactor) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdb *pTsdb = pCompactor->pTsdb;

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t tsdbCompact(STsdb *pTsdb, int32_t flag) {
  int32_t code = 0;
  int32_t lino = 0;

  // Check if can do compact (TODO)

  // Do compact
  STsdbCompactor compactor = {0};

  code = tsdbBeginCompact(pTsdb, &compactor);
  TSDB_CHECK_CODE(code, lino, _exit);

  while (true) {
    compactor.pDFileSet = (SDFileSet *)taosArraySearch(compactor.fs.aDFileSet, &compactor.fid, tDFileSetCmprFn, TD_GT);
    if (compactor.pDFileSet == NULL) break;

    compactor.fid = compactor.pDFileSet->fid;

    code = tsdbDataFReaderOpen(&compactor.pReader, pTsdb, compactor.pDFileSet);
    TSDB_CHECK_CODE(code, lino, _exit);

    // open those iterators
    tRBTreeCreate(&compactor.rtree, tsdbDataIterCmprFn);

    STsdbDataIter *pIter;

    code = tsdbDataDIterOpen(compactor.pReader, &pIter);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (pIter) tRBTreePut(&compactor.rtree, &pIter->n);

    for (int32_t iStt = 0; iStt < compactor.pReader->pSet->nSttF; iStt++) {
      code = tsdbSttDIterOpen(compactor.pReader, iStt, &pIter);
      TSDB_CHECK_CODE(code, lino, _exit);

      if (pIter) tRBTreePut(&compactor.rtree, &pIter->n);
    }

#if 0
    if (flag & TSDB_FLG_DEEP_COMPACT) {
      code = tsdbDeepCompact(&compactor);
      TSDB_CHECK_CODE(code, lino, _exit);
    } else {
      code = tsdbShallowCompact(&compactor);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
#endif
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
    tsdbAbortCompact(&compactor);
  } else {
    tsdbCommitCompact(&compactor);
  }
  return code;
}
