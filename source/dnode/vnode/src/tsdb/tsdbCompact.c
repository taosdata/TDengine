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

#define TSDB_ITER_TYPE_MEM 0x0
#define TSDB_ITER_TYPE_DAT 0x1
#define TSDB_ITER_TYPE_STT 0x2

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

typedef struct STsdbDataIter {
  struct STsdbDataIter *next;

  int32_t     flag;
  SRowInfo    rowInfo;
  SRBTreeNode n;
  char        handle[];
} STsdbDataIter;

#define TSDB_DATA_ITER_FROM_RBTN(N) ((STsdbDataIter *)((char *)N - offsetof(STsdbDataIter, n)))

typedef struct {
  STsdb         *pTsdb;
  STsdbFS        fs;
  int64_t        cid;
  int32_t        fid;
  SDFileSet     *pDFileSet;
  SDataFReader  *pReader;
  STsdbDataIter *iterList;  // list of iterators
  SRBTree        rtree;
  STsdbDataIter *pIter;
  SBlockData     bData;
} STsdbCompactor;

#define TSDB_FLG_DEEP_COMPACT 0x1

// ITER =========================
static int32_t tsdbDataIterNext(STsdbDataIter *pIter);

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
  pIter->flag = TSDB_ITER_TYPE_DAT;

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

  // TODO
  code = tBlockDataCreate(&pDataDIter->bData);
  TSDB_CHECK_CODE(code, lino, _exit);

  pDataDIter->iBlockIdx = -1;
  pDataDIter->iDataBlk = 0;
  pDataDIter->iRow = 0;

  code = tsdbDataIterNext(pIter);
  TSDB_CHECK_CODE(code, lino, _exit);

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
  pIter->flag = TSDB_ITER_TYPE_STT;

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

  pSttDIter->iSttBlk = -1;
  pSttDIter->iRow = -1;

  code = tsdbDataIterNext(pIter);
  TSDB_CHECK_CODE(code, lino, _exit);

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
  ASSERT(0);
}

static int32_t tsdbDataIterNext(STsdbDataIter *pIter) {
  int32_t code = 0;
  int32_t lino = 0;

  if (pIter->flag & TSDB_ITER_TYPE_MEM) {
    // TODO
    ASSERT(0);
  } else if (pIter->flag & TSDB_ITER_TYPE_DAT) {
    // TODO
    ASSERT(0);
  } else if (pIter->flag & TSDB_ITER_TYPE_STT) {
    SSttDIter *pSttDIter = (SSttDIter *)pIter->handle;

    pSttDIter->iRow++;
    if (pSttDIter->iRow < pSttDIter->bData.nRow) {
      pIter->rowInfo.uid = pSttDIter->bData.uid ? pSttDIter->bData.uid : pSttDIter->bData.aUid[pSttDIter->iRow];
      pIter->rowInfo.row = tsdbRowFromBlockData(&pSttDIter->bData, pSttDIter->iRow);
    } else {
      pSttDIter->iSttBlk++;
      if (pSttDIter->iSttBlk < taosArrayGetSize(pSttDIter->aSttBlk)) {
        code = tsdbReadSttBlockEx(pSttDIter->pReader, pSttDIter->iStt,
                                  taosArrayGet(pSttDIter->aSttBlk, pSttDIter->iSttBlk), &pSttDIter->bData);
        TSDB_CHECK_CODE(code, lino, _exit);

        pSttDIter->iRow = 0;
        pIter->rowInfo.suid = pSttDIter->bData.suid;
        pIter->rowInfo.uid = pSttDIter->bData.uid ? pSttDIter->bData.uid : pSttDIter->bData.aUid[pSttDIter->iRow];
        pIter->rowInfo.row = tsdbRowFromBlockData(&pSttDIter->bData, pSttDIter->iRow);
      } else {
        pIter->rowInfo.suid = 0;
        pIter->rowInfo.uid = 0;
      }
    }
  } else {
    ASSERT(0);
  }

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

static int32_t tsdbCompactNextRow(STsdbCompactor *pCompactor) {
  int32_t code = 0;
  int32_t lino = 0;

  if (pCompactor->pIter) {
    code = tsdbDataIterNext(pCompactor->pIter);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (pCompactor->pIter->rowInfo.suid == 0 && pCompactor->pIter->rowInfo.uid == 0) {
      pCompactor->pIter = NULL;
    } else {
      SRBTreeNode *pNode = tRBTreeMin(&pCompactor->rtree);
      if (pNode) {
        STsdbDataIter *pIter = TSDB_DATA_ITER_FROM_RBTN(pNode);

        int32_t c = tRowInfoCmprFn(&pCompactor->pIter->rowInfo, &pIter->rowInfo);
        ASSERT(c);

        if (c > 0) {
          tRBTreePut(&pCompactor->rtree, &pCompactor->pIter->n);
          pCompactor->pIter = NULL;
        }
      }
    }
  }

  if (pCompactor->pIter == NULL) {
    SRBTreeNode *pNode = tRBTreeMin(&pCompactor->rtree);
    if (pNode) {
      pCompactor->pIter = TSDB_DATA_ITER_FROM_RBTN(pNode);
      tRBTreeDrop(&pCompactor->rtree, pNode);
    }
  }

_exit:
  return code;
}

static int32_t tsdbCompactGetRow(STsdbCompactor *pCompactor, TSDBROW **ppRow) {
  int32_t code = 0;
  int32_t lino = 0;

  if (pCompactor->pIter == NULL) {
    code = tsdbCompactNextRow(pCompactor);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (pCompactor->pIter) {
    *ppRow = &pCompactor->pIter->rowInfo.row;
  } else {
    *ppRow = NULL;
  }

_exit:
  return code;
}

static int32_t tsdbOpenCompactor(STsdbCompactor *pCompactor) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdb *pTsdb = pCompactor->pTsdb;

  // next compact file
  pCompactor->pDFileSet = (SDFileSet *)taosArraySearch(pCompactor->fs.aDFileSet, &(SDFileSet){.fid = pCompactor->fid},
                                                       tDFileSetCmprFn, TD_GT);
  if (pCompactor->pDFileSet == NULL) goto _exit;

  pCompactor->fid = pCompactor->pDFileSet->fid;

  code = tsdbDataFReaderOpen(&pCompactor->pReader, pTsdb, pCompactor->pDFileSet);
  TSDB_CHECK_CODE(code, lino, _exit);

  // open iters
  STsdbDataIter *pIter;

  pCompactor->iterList = NULL;
  tRBTreeCreate(&pCompactor->rtree, tsdbDataIterCmprFn);

  code = tsdbDataDIterOpen(pCompactor->pReader, &pIter);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (pIter) {
    pIter->next = pCompactor->iterList;
    pCompactor->iterList = pIter;
    tRBTreePut(&pCompactor->rtree, &pIter->n);
  }

  for (int32_t iStt = 0; iStt < pCompactor->pReader->pSet->nSttF; iStt++) {
    code = tsdbSttDIterOpen(pCompactor->pReader, iStt, &pIter);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (pIter) {
      pIter->next = pCompactor->iterList;
      pCompactor->iterList = pIter;
      tRBTreePut(&pCompactor->rtree, &pIter->n);
    }
  }
  pCompactor->pIter = NULL;

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s done", TD_VID(pTsdb->pVnode), __func__);
  }
  return code;
}

static void tsdbCloseCompactor(STsdbCompactor *pCompactor) {
  STsdb *pTsdb = pCompactor->pTsdb;

  for (STsdbDataIter *pIter = pCompactor->iterList; pIter;) {
    STsdbDataIter *pIterNext = pIter->next;
    tsdbDataIterClose(pIter);
    pIter = pIterNext;
  }

  // TODO
  ASSERT(0);

_exit:
  tsdbDebug("vgId:%d %s done", TD_VID(pTsdb->pVnode), __func__);
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
    code = tsdbOpenCompactor(&compactor);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (compactor.pDFileSet == NULL) break;

    // loop to merge row by row
    TSDBROW *pRow = NULL;
    int64_t  nRow = 0;
    for (;;) {
      code = tsdbCompactGetRow(&compactor, &pRow);
      TSDB_CHECK_CODE(code, lino, _exit);

      if (pRow == NULL) break;

      nRow++;

      code = tsdbCompactNextRow(&compactor);
      TSDB_CHECK_CODE(code, lino, _exit);

      // code = tBlockDataAppendRow(&compactor.bData, pRow, pRow, NULL, 0);
      // TSDB_CHECK_CODE(code, lino, _exit);

      // if (compactor.bData.nRows >= TSDB_MAX_ROWS_PER_BLOCK) {
      //   code = tsdbFlushBlock(&compactor);
      //   TSDB_CHECK_CODE(code, lino, _exit);
      // }
    }

    tsdbCloseCompactor(&compactor);
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
