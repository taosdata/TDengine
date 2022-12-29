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
  STsdb     *pTsdb;
  int64_t    cid;
  int8_t     cmprAlg;
  int32_t    maxRows;
  int32_t    minRows;
  STsdbFS    fs;
  int32_t    fid;
  SDFileSet *pDFileSet;

  // Tombstone
  SDelFReader *pDelFReader;
  SArray      *aDelIdx;   // SArray<SDelIdx>
  SArray      *aDelData;  // SArray<SDelData>
  SArray      *aSkyLine;  // SArray<TSDBKEY>
  TSDBKEY     *aTSDBKEY;

  // Reader
  SDataFReader  *pReader;
  STsdbDataIter *iterList;  // list of iterators
  SRBTree        rtree;
  STsdbDataIter *pIter;
  SBlockData     bData;
  SSkmInfo       tbSkm;

  // Writer
  SDataFWriter *pWriter;
  SArray       *aBlockIdx;  // SArray<SBlockIdx>
  TABLEID       tableId;
  SMapData      mDataBlk;  // SMapData<SDataBlk>
  SArray       *aSttBlk;   // SArray<SSttBlk>
} STsdbCompactor;

#define TSDB_FLG_DEEP_COMPACT 0x1

// ITER =========================
static int32_t tsdbDataIterNext(STsdbDataIter *pIter, TABLEID *pExcludeTableId);

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

  code = tsdbDataIterNext(pIter, NULL);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
  _clear_exit:
    *ppIter = NULL;
    if (pIter) {
      tBlockDataDestroy(&pDataDIter->bData);
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

  code = tsdbDataIterNext(pIter, NULL);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
  _clear_exit:
    *ppIter = NULL;
    if (pIter) {
      tBlockDataDestroy(&pSttDIter->bData);
      taosArrayDestroy(pSttDIter->aSttBlk);
      taosMemoryFree(pIter);
    }
  } else {
    *ppIter = pIter;
  }
  return code;
}

static void tsdbDataIterClose(STsdbDataIter *pIter) {
  if (pIter == NULL) return;

  if (pIter->flag & TSDB_ITER_TYPE_MEM) {
    ASSERT(0);
  } else if (pIter->flag & TSDB_ITER_TYPE_DAT) {
    ASSERT(0);
  } else if (pIter->flag & TSDB_ITER_TYPE_STT) {
    SSttDIter *pSttDIter = (SSttDIter *)pIter->handle;

    tBlockDataDestroy(&pSttDIter->bData);
    taosArrayDestroy(pSttDIter->aSttBlk);
  } else {
    ASSERT(0);
  }

  taosMemoryFree(pIter);
}

static int32_t tsdbDataIterNext(STsdbDataIter *pIter, TABLEID *pExcludeTableId) {
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

    for (;;) {
      if (++pSttDIter->iRow >= pSttDIter->bData.nRow) {
        for (;;) {
          if (++pSttDIter->iSttBlk < taosArrayGetSize(pSttDIter->aSttBlk)) {
            SSttBlk *pSttBlk = (SSttBlk *)taosArrayGet(pSttDIter->aSttBlk, pSttDIter->iSttBlk);

            // check exclusion
            if (pExcludeTableId) {
              if (pExcludeTableId->uid) {  // exclude (suid, uid)
                if (pSttBlk->minUid == pExcludeTableId->uid && pSttBlk->maxUid == pExcludeTableId->uid) continue;
              } else {  // exclude (suid, *)
                if (pSttBlk->suid == pExcludeTableId->suid) continue;
              }
            }

            code = tsdbReadSttBlockEx(pSttDIter->pReader, pSttDIter->iStt, pSttBlk, &pSttDIter->bData);
            TSDB_CHECK_CODE(code, lino, _exit);

            pIter->rowInfo.suid = pSttBlk->suid;
            pSttDIter->iRow = 0;
            break;
          } else {
            // iter end, all set 0 and exit
            pIter->rowInfo.suid = 0;
            pIter->rowInfo.uid = 0;
            goto _exit;
          }
        }
      }

      pIter->rowInfo.uid = pSttDIter->bData.uid ? pSttDIter->bData.uid : pSttDIter->bData.aUid[pSttDIter->iRow];
      pIter->rowInfo.row = tsdbRowFromBlockData(&pSttDIter->bData, pSttDIter->iRow);

      // check exclusion
      if (pExcludeTableId) {
        if (pExcludeTableId->uid) {  // exclude (suid, uid)
          if (pIter->rowInfo.uid == pExcludeTableId->uid) continue;
        } else {  // exclude (suid, *)
          if (pIter->rowInfo.suid == pExcludeTableId->suid) continue;
        }
      }

      break;
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
  pCompactor->cid = 0;  // TODO
  pCompactor->cmprAlg = pTsdb->pVnode->config.tsdbCfg.compression;
  pCompactor->maxRows = pTsdb->pVnode->config.tsdbCfg.maxRows;
  pCompactor->minRows = pTsdb->pVnode->config.tsdbCfg.minRows;

  code = tsdbFSCopy(pTsdb, &pCompactor->fs);
  TSDB_CHECK_CODE(code, lino, _exit);

  pCompactor->fid = INT32_MIN;

  code = tBlockDataCreate(&pCompactor->bData);
  TSDB_CHECK_CODE(code, lino, _exit);

  // tombstone
  if (pCompactor->fs.pDelFile) {
    code = tsdbDelFReaderOpen(&pCompactor->pDelFReader, pCompactor->fs.pDelFile, pTsdb);
    TSDB_CHECK_CODE(code, lino, _exit);

    pCompactor->aDelIdx = taosArrayInit(0, sizeof(SDelIdx));
    if (pCompactor->aDelIdx == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    pCompactor->aDelData = taosArrayInit(0, sizeof(SDelData));
    if (pCompactor->aDelData == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    pCompactor->aSkyLine = taosArrayInit(0, sizeof(TSDBKEY));
    if (pCompactor->aSkyLine == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tsdbReadDelIdx(pCompactor->pDelFReader, pCompactor->aDelIdx);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static void tsdbEndCompact(STsdbCompactor *pCompactor) {
  taosArrayDestroy(pCompactor->aSkyLine);
  taosArrayDestroy(pCompactor->aDelData);
  taosArrayDestroy(pCompactor->aDelIdx);
  tsdbDelFReaderClose(&pCompactor->pDelFReader);
  tsdbFSDestroy(&pCompactor->fs);
  tBlockDataDestroy(&pCompactor->bData);
  tDestroyTSchema(pCompactor->tbSkm.pTSchema);
  pCompactor->tbSkm.pTSchema = NULL;
  taosArrayDestroy(pCompactor->aBlockIdx);
  tMapDataClear(&pCompactor->mDataBlk);
  taosArrayDestroy(pCompactor->aSttBlk);
}

static int32_t tsdbCommitCompact(STsdbCompactor *pCompactor) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdb *pTsdb = pCompactor->pTsdb;

  code = tsdbFSPrepareCommit(pTsdb, &pCompactor->fs);
  TSDB_CHECK_CODE(code, lino, _exit);

  taosThreadRwlockWrlock(&pTsdb->rwLock);

  code = tsdbFSCommit(pTsdb);
  if (code) {
    taosThreadRwlockUnlock(&pTsdb->rwLock);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  taosThreadRwlockUnlock(&pTsdb->rwLock);

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

static int32_t tsdbCompactNextRowImpl(STsdbCompactor *pCompactor, TABLEID *pExcludeTableId) {
  int32_t code = 0;
  int32_t lino = 0;

  for (;;) {
    if (pCompactor->pIter) {
      code = tsdbDataIterNext(pCompactor->pIter, pExcludeTableId);
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

        if (pExcludeTableId) {
          if (pExcludeTableId->uid) {
            if (pCompactor->pIter->rowInfo.uid == pExcludeTableId->uid) continue;
          } else {
            if (pCompactor->pIter->rowInfo.suid == pExcludeTableId->suid) continue;
          }
        }
      }
    }

    break;
  }

_exit:
  return code;
}

static int32_t tDelIdxCmprFn(const SDelIdx *pDelIdx1, const SDelIdx *pDelIdx2) {
  if (pDelIdx1->suid < pDelIdx2->suid) {
    return -1;
  } else if (pDelIdx1->suid > pDelIdx2->suid) {
    return 1;
  }

  if (pDelIdx1->uid < pDelIdx2->uid) {
    return -1;
  } else if (pDelIdx1->uid > pDelIdx2->uid) {
    return 1;
  }

  return 0;
}
static int32_t tsdbCompactNextRow(STsdbCompactor *pCompactor) {
  int32_t code = 0;
  int32_t lino = 0;

  TABLEID  excludeTableId;
  TABLEID *pExcludeTableId = NULL;

  for (;;) {
    code = tsdbCompactNextRowImpl(pCompactor, pExcludeTableId);
    TSDB_CHECK_CODE(code, lino, _exit);

    // check if the table of the row exists
    if (pCompactor->pIter) {
      SRowInfo *pRowInfo = &pCompactor->pIter->rowInfo;

      // Table exists
      if (pRowInfo->uid == pCompactor->tbSkm.uid) {
        if (pCompactor->aTSDBKEY) {
          // TODO: check if the row is deleted. if deleted, continue, else break
          ASSERT(0);
        } else {
          break;
        }
      }

      SMetaInfo info;
      if (pRowInfo->suid) {  // child table

        // check if super table exists
        if (pRowInfo->suid != pCompactor->tbSkm.suid) {
          if (metaGetInfo(pCompactor->pTsdb->pVnode->pMeta, pRowInfo->uid, &info, NULL) != TSDB_CODE_SUCCESS) {
            excludeTableId.suid = pRowInfo->suid;
            excludeTableId.uid = 0;
            pExcludeTableId = &excludeTableId;
            continue;
          }

          // super table exists
          pCompactor->tbSkm.suid = pRowInfo->suid;
          pCompactor->tbSkm.uid = 0;
          tDestroyTSchema(pCompactor->tbSkm.pTSchema);
          pCompactor->tbSkm.pTSchema = metaGetTbTSchema(pCompactor->pTsdb->pVnode->pMeta, pRowInfo->suid, -1, 1);
          if (pCompactor->tbSkm.pTSchema == NULL) {
            code = TSDB_CODE_OUT_OF_MEMORY;
            TSDB_CHECK_CODE(code, lino, _exit);
          }
        }

        // check if table exists
        if (metaGetInfo(pCompactor->pTsdb->pVnode->pMeta, pRowInfo->uid, &info, NULL) != TSDB_CODE_SUCCESS) {
          excludeTableId.suid = pRowInfo->suid;
          excludeTableId.uid = pRowInfo->uid;
          pExcludeTableId = &excludeTableId;
          continue;
        }

        // table exists
        pCompactor->tbSkm.uid = pRowInfo->uid;
      } else {  // normal table
        // check if table exists
        if (metaGetInfo(pCompactor->pTsdb->pVnode->pMeta, pRowInfo->uid, &info, NULL) != TSDB_CODE_SUCCESS) {
          excludeTableId.suid = pRowInfo->suid;
          excludeTableId.uid = pRowInfo->uid;
          pExcludeTableId = &excludeTableId;
          continue;
        }

        // table exists
        pCompactor->tbSkm.suid = pRowInfo->suid;
        pCompactor->tbSkm.uid = pRowInfo->uid;
        tDestroyTSchema(pCompactor->tbSkm.pTSchema);

        pCompactor->tbSkm.pTSchema = metaGetTbTSchema(pCompactor->pTsdb->pVnode->pMeta, pRowInfo->suid, -1, 1);
        if (pCompactor->tbSkm.pTSchema == NULL) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          TSDB_CHECK_CODE(code, lino, _exit);
        }
      }

      // load delData and build the skyline
      if (pCompactor->pDelFReader) {
        SDelIdx *pDelIdx =
            taosArraySearch(pCompactor->aDelIdx, &(SDelIdx){.suid = pRowInfo->suid, .uid = pRowInfo->uid},
                            (__compar_fn_t)tDelIdxCmprFn, TD_EQ);
        if (pDelIdx) {
          code = tsdbReadDelData(pCompactor->pDelFReader, pDelIdx, pCompactor->aDelData);
          TSDB_CHECK_CODE(code, lino, _exit);

          code = tsdbBuildDeleteSkyline(pCompactor->aDelData, 0, taosArrayGetSize(pCompactor->aDelData) - 1,
                                        pCompactor->aSkyLine);
          TSDB_CHECK_CODE(code, lino, _exit);

          pCompactor->aTSDBKEY = (TSDBKEY *)TARRAY_DATA(pCompactor->aDelData);
        } else {
          pCompactor->aTSDBKEY = NULL;
        }
      }

      break;
    } else {
      // iter end, just break out
      break;
    }
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pCompactor->pTsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

static int32_t tsdbCompactGetRow(STsdbCompactor *pCompactor, SRowInfo **ppRowInfo, STSchema **ppTSchema) {
  int32_t code = 0;
  int32_t lino = 0;

  if (pCompactor->pIter == NULL) {
    code = tsdbCompactNextRow(pCompactor);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (pCompactor->pIter) {
    ASSERT(pCompactor->pIter->rowInfo.suid == pCompactor->tbSkm.suid);
    ASSERT(pCompactor->pIter->rowInfo.uid == pCompactor->tbSkm.uid);
    *ppRowInfo = &pCompactor->pIter->rowInfo;
    *ppTSchema = pCompactor->tbSkm.pTSchema;
  } else {
    *ppRowInfo = NULL;
    *ppTSchema = NULL;
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

  // reader
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
  tBlockDataReset(&pCompactor->bData);

  // writer
  SDFileSet wSet = {.diskId = (SDiskID){0},  // TODO
                    .fid = pCompactor->pDFileSet->fid,
                    .pHeadF = &(SHeadFile){.commitID = pCompactor->cid},
                    .pDataF = &(SDataFile){.commitID = pCompactor->cid},
                    .pSmaF = &(SSmaFile){.commitID = pCompactor->cid},
                    .nSttF = 1,
                    .aSttF = {&(SSttFile){.commitID = pCompactor->cid}}};
  code = tsdbDataFWriterOpen(&pCompactor->pWriter, pTsdb, &wSet);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (pCompactor->aBlockIdx == NULL) {
    pCompactor->aBlockIdx = taosArrayInit(0, sizeof(SBlockIdx));
    if (pCompactor->aBlockIdx == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  } else {
    taosArrayClear(pCompactor->aBlockIdx);
  }

  tMapDataReset(&pCompactor->mDataBlk);

  if (pCompactor->aSttBlk == NULL) {
    pCompactor->aSttBlk = taosArrayInit(0, sizeof(SSttBlk));
    if (pCompactor->aSttBlk == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  } else {
    taosArrayClear(pCompactor->aSttBlk);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s done", TD_VID(pTsdb->pVnode), __func__);
  }
  return code;
}

static void tsdbCloseCompactor(STsdbCompactor *pCompactor) {
  for (STsdbDataIter *pIter = pCompactor->iterList; pIter;) {
    STsdbDataIter *pIterNext = pIter->next;
    tsdbDataIterClose(pIter);
    pIter = pIterNext;
  }

  tDestroyTSchema(pCompactor->tbSkm.pTSchema);
  pCompactor->tbSkm.pTSchema = NULL;

  tsdbDataFReaderClose(&pCompactor->pReader);
}

extern int32_t tsdbWriteDataBlock(SDataFWriter *pWriter, SBlockData *pBlockData, SMapData *mDataBlk, int8_t cmprAlg);
extern int32_t tsdbWriteSttBlock(SDataFWriter *pWriter, SBlockData *pBlockData, SArray *aSttBlk, int8_t cmprAlg);
static int32_t tsdbCompactWriteBlockData(STsdbCompactor *pCompactor) {
  int32_t code = 0;
  int32_t lino = 0;

  SBlockData *pBData = &pCompactor->bData;

  if (pBData->nRow == 0) goto _exit;

  if (pBData->uid && pBData->nRow >= pCompactor->minRows) {  // write to .data file
    code = tsdbWriteDataBlock(pCompactor->pWriter, pBData, &pCompactor->mDataBlk, pCompactor->cmprAlg);
    TSDB_CHECK_CODE(code, lino, _exit);

    pCompactor->tableId.suid = pBData->suid;
    pCompactor->tableId.uid = pBData->uid;
  } else {  // write to .stt file
    code = tsdbWriteSttBlock(pCompactor->pWriter, pBData, pCompactor->aSttBlk, pCompactor->cmprAlg);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  tBlockDataClear(&pCompactor->bData);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pCompactor->pTsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

static int32_t tsdbCompactWriteDataBlk(STsdbCompactor *pCompactor) {
  int32_t code = 0;
  int32_t lino = 0;

  if (pCompactor->mDataBlk.nItem == 0) goto _exit;

  SBlockIdx *pBlockIdx = (SBlockIdx *)taosArrayReserve(pCompactor->aBlockIdx, 1);
  if (pBlockIdx == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  pBlockIdx->suid = pCompactor->tableId.suid;
  pBlockIdx->uid = pCompactor->tableId.uid;

  code = tsdbWriteDataBlk(pCompactor->pWriter, &pCompactor->mDataBlk, pBlockIdx);
  TSDB_CHECK_CODE(code, lino, _exit);

  tMapDataReset(&pCompactor->mDataBlk);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pCompactor->pTsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

int32_t tsdbCompact(STsdb *pTsdb, int32_t flag) {
  int32_t code = 0;
  int32_t lino = 0;

  // Check if can do compact (TODO)

  // Do compact
  STsdbCompactor *pCompactor = &(STsdbCompactor){0};

  code = tsdbBeginCompact(pTsdb, pCompactor);
  TSDB_CHECK_CODE(code, lino, _exit);

  while (true) {
    code = tsdbOpenCompactor(pCompactor);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (pCompactor->pDFileSet == NULL) break;

    // loop to merge row by row
    SRowInfo *pRowInfo = NULL;
    STSchema *pTSchema = NULL;
    int64_t   nRow = 0;
    for (;;) {
      code = tsdbCompactGetRow(pCompactor, &pRowInfo, &pTSchema);
      TSDB_CHECK_CODE(code, lino, _exit);

      if (pRowInfo == NULL) break;

      nRow++;

      if (pCompactor->bData.suid == 0 && pCompactor->bData.uid == 0) {  // init the block data if not initialized yet
        code = tBlockDataInit(&pCompactor->bData, &(TABLEID){.suid = pRowInfo->suid, .uid = pRowInfo->uid}, pTSchema,
                              NULL, 0);
        TSDB_CHECK_CODE(code, lino, _exit);
      } else {
        if ((pCompactor->tableId.suid != pRowInfo->suid) ||  // different super table
            (pCompactor->tableId.uid != pRowInfo->uid &&
             (pRowInfo->suid == 0 ||
              pCompactor->bData.uid && pCompactor->bData.nRow >= pCompactor->minRows))  // different table
        ) {
          code = tsdbCompactWriteBlockData(pCompactor);
          TSDB_CHECK_CODE(code, lino, _exit);

          code = tsdbCompactWriteDataBlk(pCompactor);
          TSDB_CHECK_CODE(code, lino, _exit);

          code = tBlockDataInit(&pCompactor->bData, &(TABLEID){.suid = pRowInfo->suid, .uid = pRowInfo->uid}, pTSchema,
                                NULL, 0);
          TSDB_CHECK_CODE(code, lino, _exit);
        }
      }

      // append row to block data
      code = tBlockDataAppendRowEx(&pCompactor->bData, &pRowInfo->row, pTSchema, pRowInfo->uid);
      TSDB_CHECK_CODE(code, lino, _exit);

      pCompactor->tableId.suid = pRowInfo->suid;
      pCompactor->tableId.uid = pRowInfo->uid;

      // check if block data is full
      if (pCompactor->bData.nRow >= pCompactor->maxRows) {
        code = tsdbCompactWriteBlockData(pCompactor);
        TSDB_CHECK_CODE(code, lino, _exit);
      }

      // iterate to next row
      code = tsdbCompactNextRow(pCompactor);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tsdbCompactWriteBlockData(pCompactor);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbCompactWriteDataBlk(pCompactor);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbWriteBlockIdx(pCompactor->pWriter, pCompactor->aBlockIdx);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbWriteSttBlk(pCompactor->pWriter, pCompactor->aSttBlk);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbUpdateDFileSetHeader(pCompactor->pWriter);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbFSUpsertFSet(&pCompactor->fs, &pCompactor->pWriter->wSet);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbDataFWriterClose(&pCompactor->pWriter, 1);
    TSDB_CHECK_CODE(code, lino, _exit);

    tsdbCloseCompactor(pCompactor);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
    tsdbAbortCompact(pCompactor);
  } else {
    tsdbCommitCompact(pCompactor);
  }
  tsdbEndCompact(pCompactor);
  return code;
}
