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
#include "vnodeInt.h"

#ifdef BUILD_NO_CALL
// STsdbDataIter2
/* open */
int32_t tsdbOpenDataFileDataIter(SDataFReader* pReader, STsdbDataIter2** ppIter) {
  int32_t code = 0;
  int32_t lino = 0;

  // create handle
  STsdbDataIter2* pIter = (STsdbDataIter2*)taosMemoryCalloc(1, sizeof(*pIter));
  if (pIter == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  pIter->type = TSDB_DATA_FILE_DATA_ITER;
  pIter->dIter.pReader = pReader;
  if ((pIter->dIter.aBlockIdx = taosArrayInit(0, sizeof(SBlockIdx))) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tBlockDataCreate(&pIter->dIter.bData);
  TSDB_CHECK_CODE(code, lino, _exit);

  pIter->dIter.iBlockIdx = 0;
  pIter->dIter.iDataBlk = 0;
  pIter->dIter.iRow = 0;

  // read data
  code = tsdbReadBlockIdx(pReader, pIter->dIter.aBlockIdx);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (taosArrayGetSize(pIter->dIter.aBlockIdx) == 0) goto _clear;

_exit:
  if (code) {
    if (pIter) {
    _clear:
      tBlockDataDestroy(&pIter->dIter.bData);
      taosArrayDestroy(pIter->dIter.aBlockIdx);
      taosMemoryFree(pIter);
      pIter = NULL;
    }
  }
  *ppIter = pIter;
  return code;
}

int32_t tsdbOpenSttFileDataIter(SDataFReader* pReader, int32_t iStt, STsdbDataIter2** ppIter) {
  int32_t code = 0;
  int32_t lino = 0;

  // create handle
  STsdbDataIter2* pIter = (STsdbDataIter2*)taosMemoryCalloc(1, sizeof(*pIter));
  if (pIter == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  pIter->type = TSDB_STT_FILE_DATA_ITER;
  pIter->sIter.pReader = pReader;
  pIter->sIter.iStt = iStt;
  pIter->sIter.aSttBlk = taosArrayInit(0, sizeof(SSttBlk));
  if (pIter->sIter.aSttBlk == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tBlockDataCreate(&pIter->sIter.bData);
  TSDB_CHECK_CODE(code, lino, _exit);

  pIter->sIter.iSttBlk = 0;
  pIter->sIter.iRow = 0;

  // read data
  code = tsdbReadSttBlk(pReader, iStt, pIter->sIter.aSttBlk);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (taosArrayGetSize(pIter->sIter.aSttBlk) == 0) goto _clear;

_exit:
  if (code) {
    if (pIter) {
    _clear:
      taosArrayDestroy(pIter->sIter.aSttBlk);
      tBlockDataDestroy(&pIter->sIter.bData);
      taosMemoryFree(pIter);
      pIter = NULL;
    }
  }
  *ppIter = pIter;
  return code;
}

int32_t tsdbOpenTombFileDataIter(SDelFReader* pReader, STsdbDataIter2** ppIter) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdbDataIter2* pIter = (STsdbDataIter2*)taosMemoryCalloc(1, sizeof(*pIter));
  if (pIter == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  pIter->type = TSDB_TOMB_FILE_DATA_ITER;

  pIter->tIter.pReader = pReader;
  if ((pIter->tIter.aDelIdx = taosArrayInit(0, sizeof(SDelIdx))) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  if ((pIter->tIter.aDelData = taosArrayInit(0, sizeof(SDelData))) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tsdbReadDelIdx(pReader, pIter->tIter.aDelIdx);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (taosArrayGetSize(pIter->tIter.aDelIdx) == 0) goto _clear;

  pIter->tIter.iDelIdx = 0;
  pIter->tIter.iDelData = 0;

_exit:
  if (code) {
    if (pIter) {
    _clear:
      taosArrayDestroy(pIter->tIter.aDelIdx);
      taosArrayDestroy(pIter->tIter.aDelData);
      taosMemoryFree(pIter);
      pIter = NULL;
    }
  }
  *ppIter = pIter;
  return code;
}

/* close */
static void tsdbCloseDataFileDataIter(STsdbDataIter2* pIter) {
  tBlockDataDestroy(&pIter->dIter.bData);
  tMapDataClear(&pIter->dIter.mDataBlk);
  taosArrayDestroy(pIter->dIter.aBlockIdx);
  taosMemoryFree(pIter);
}

static void tsdbCloseSttFileDataIter(STsdbDataIter2* pIter) {
  tBlockDataDestroy(&pIter->sIter.bData);
  taosArrayDestroy(pIter->sIter.aSttBlk);
  taosMemoryFree(pIter);
}

static void tsdbCloseTombFileDataIter(STsdbDataIter2* pIter) {
  taosArrayDestroy(pIter->tIter.aDelData);
  taosArrayDestroy(pIter->tIter.aDelIdx);
  taosMemoryFree(pIter);
}

void tsdbCloseDataIter2(STsdbDataIter2* pIter) {
  if (pIter->type == TSDB_MEM_TABLE_DATA_ITER) {
    ASSERT(0);
  } else if (pIter->type == TSDB_DATA_FILE_DATA_ITER) {
    tsdbCloseDataFileDataIter(pIter);
  } else if (pIter->type == TSDB_STT_FILE_DATA_ITER) {
    tsdbCloseSttFileDataIter(pIter);
  } else if (pIter->type == TSDB_TOMB_FILE_DATA_ITER) {
    tsdbCloseTombFileDataIter(pIter);
  } else {
    ASSERT(0);
  }
}

/* cmpr */
int32_t tsdbDataIterCmprFn(const SRBTreeNode* pNode1, const SRBTreeNode* pNode2) {
  STsdbDataIter2* pIter1 = TSDB_RBTN_TO_DATA_ITER(pNode1);
  STsdbDataIter2* pIter2 = TSDB_RBTN_TO_DATA_ITER(pNode2);
  return tRowInfoCmprFn(&pIter1->rowInfo, &pIter2->rowInfo);
}

/* seek */

/* iter next */
static int32_t tsdbDataFileDataIterNext(STsdbDataIter2* pIter, STsdbFilterInfo* pFilterInfo) {
  int32_t code = 0;
  int32_t lino = 0;

  for (;;) {
    while (pIter->dIter.iRow < pIter->dIter.bData.nRow) {
      if (pFilterInfo) {
        if (pFilterInfo->flag & TSDB_FILTER_FLAG_BY_VERSION) {
          if (pIter->dIter.bData.aVersion[pIter->dIter.iRow] < pFilterInfo->sver ||
              pIter->dIter.bData.aVersion[pIter->dIter.iRow] > pFilterInfo->ever) {
            pIter->dIter.iRow++;
            continue;
          }
        }
      }

      ASSERT(pIter->rowInfo.suid == pIter->dIter.bData.suid);
      ASSERT(pIter->rowInfo.uid == pIter->dIter.bData.uid);
      pIter->rowInfo.row = tsdbRowFromBlockData(&pIter->dIter.bData, pIter->dIter.iRow);
      pIter->dIter.iRow++;
      goto _exit;
    }

    for (;;) {
      while (pIter->dIter.iDataBlk < pIter->dIter.mDataBlk.nItem) {
        SDataBlk dataBlk;
        tMapDataGetItemByIdx(&pIter->dIter.mDataBlk, pIter->dIter.iDataBlk, &dataBlk, tGetDataBlk);

        // filter
        if (pFilterInfo) {
          if (pFilterInfo->flag & TSDB_FILTER_FLAG_BY_VERSION) {
            if (pFilterInfo->sver > dataBlk.maxVer || pFilterInfo->ever < dataBlk.minVer) {
              pIter->dIter.iDataBlk++;
              continue;
            }
          }
        }

        code = tsdbReadDataBlockEx(pIter->dIter.pReader, &dataBlk, &pIter->dIter.bData);
        TSDB_CHECK_CODE(code, lino, _exit);

        pIter->dIter.iDataBlk++;
        pIter->dIter.iRow = 0;

        break;
      }

      if (pIter->dIter.iRow < pIter->dIter.bData.nRow) break;

      for (;;) {
        if (pIter->dIter.iBlockIdx < taosArrayGetSize(pIter->dIter.aBlockIdx)) {
          SBlockIdx* pBlockIdx = taosArrayGet(pIter->dIter.aBlockIdx, pIter->dIter.iBlockIdx);

          if (pFilterInfo) {
            if (pFilterInfo->flag & TSDB_FILTER_FLAG_BY_TABLEID) {
              int32_t c = tTABLEIDCmprFn(pBlockIdx, &pFilterInfo->tbid);
              if (c == 0) {
                pIter->dIter.iBlockIdx++;
                continue;
              } else if (c < 0) {
                ASSERT(0);
              }
            }

            if (pFilterInfo->flag & TSDB_FILTER_FLAG_IGNORE_DROPPED_TABLE) {
              SMetaInfo info;
              if (metaGetInfo(pIter->dIter.pReader->pTsdb->pVnode->pMeta, pBlockIdx->uid, &info, NULL)) {
                pIter->dIter.iBlockIdx++;
                continue;
              }
            }
          }

          code = tsdbReadDataBlk(pIter->dIter.pReader, pBlockIdx, &pIter->dIter.mDataBlk);
          TSDB_CHECK_CODE(code, lino, _exit);

          pIter->rowInfo.suid = pBlockIdx->suid;
          pIter->rowInfo.uid = pBlockIdx->uid;

          pIter->dIter.iBlockIdx++;
          pIter->dIter.iDataBlk = 0;

          break;
        } else {
          pIter->rowInfo = (SRowInfo){0};
          goto _exit;
        }
      }
    }
  }

_exit:
  if (code) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbSttFileDataIterNext(STsdbDataIter2* pIter, STsdbFilterInfo* pFilterInfo) {
  int32_t code = 0;
  int32_t lino = 0;

  for (;;) {
    while (pIter->sIter.iRow < pIter->sIter.bData.nRow) {
      if (pFilterInfo) {
        int64_t uid = pIter->sIter.bData.uid ? pIter->sIter.bData.uid : pIter->sIter.bData.aUid[pIter->sIter.iRow];
        if (pFilterInfo->flag & TSDB_FILTER_FLAG_BY_TABLEID) {
          if (pFilterInfo->tbid.uid == uid) {
            pIter->sIter.iRow++;
            continue;
          }
        }

        if (pFilterInfo->flag & TSDB_FILTER_FLAG_IGNORE_DROPPED_TABLE) {
          if (pIter->rowInfo.uid != uid) {
            SMetaInfo info;
            if (metaGetInfo(pIter->sIter.pReader->pTsdb->pVnode->pMeta, uid, &info, NULL)) {
              pIter->sIter.iRow++;
              continue;
            }
          }
        }

        if (pFilterInfo->flag & TSDB_FILTER_FLAG_BY_VERSION) {
          if (pFilterInfo->sver > pIter->sIter.bData.aVersion[pIter->sIter.iRow] ||
              pFilterInfo->ever < pIter->sIter.bData.aVersion[pIter->sIter.iRow]) {
            pIter->sIter.iRow++;
            continue;
          }
        }
      }

      pIter->rowInfo.suid = pIter->sIter.bData.suid;
      pIter->rowInfo.uid = pIter->sIter.bData.uid ? pIter->sIter.bData.uid : pIter->sIter.bData.aUid[pIter->sIter.iRow];
      pIter->rowInfo.row = tsdbRowFromBlockData(&pIter->sIter.bData, pIter->sIter.iRow);
      pIter->sIter.iRow++;
      goto _exit;
    }

    for (;;) {
      if (pIter->sIter.iSttBlk < taosArrayGetSize(pIter->sIter.aSttBlk)) {
        SSttBlk* pSttBlk = taosArrayGet(pIter->sIter.aSttBlk, pIter->sIter.iSttBlk);

        if (pFilterInfo) {
          if (pFilterInfo->flag & TSDB_FILTER_FLAG_BY_TABLEID) {
            if (pSttBlk->suid == pFilterInfo->tbid.suid && pSttBlk->minUid == pFilterInfo->tbid.uid &&
                pSttBlk->maxUid == pFilterInfo->tbid.uid) {
              pIter->sIter.iSttBlk++;
              continue;
            }
          }

          if (pFilterInfo->flag & TSDB_FILTER_FLAG_BY_VERSION) {
            if (pFilterInfo->sver > pSttBlk->maxVer || pFilterInfo->ever < pSttBlk->minVer) {
              pIter->sIter.iSttBlk++;
              continue;
            }
          }
        }

        code = tsdbReadSttBlockEx(pIter->sIter.pReader, pIter->sIter.iStt, pSttBlk, &pIter->sIter.bData);
        TSDB_CHECK_CODE(code, lino, _exit);

        pIter->sIter.iRow = 0;
        pIter->sIter.iSttBlk++;
        break;
      } else {
        pIter->rowInfo = (SRowInfo){0};
        goto _exit;
      }
    }
  }

_exit:
  if (code) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbTombFileDataIterNext(STsdbDataIter2* pIter, STsdbFilterInfo* pFilterInfo) {
  int32_t code = 0;
  int32_t lino = 0;

  for (;;) {
    while (pIter->tIter.iDelData < taosArrayGetSize(pIter->tIter.aDelData)) {
      SDelData* pDelData = taosArrayGet(pIter->tIter.aDelData, pIter->tIter.iDelData);

      if (pFilterInfo) {
        if (pFilterInfo->flag & TSDB_FILTER_FLAG_BY_VERSION) {
          if (pFilterInfo->sver > pDelData->version || pFilterInfo->ever < pDelData->version) {
            pIter->tIter.iDelData++;
            continue;
          }
        }
      }

      pIter->delInfo.delData = *pDelData;
      pIter->tIter.iDelData++;
      goto _exit;
    }

    for (;;) {
      if (pIter->tIter.iDelIdx < taosArrayGetSize(pIter->tIter.aDelIdx)) {
        SDelIdx* pDelIdx = taosArrayGet(pIter->tIter.aDelIdx, pIter->tIter.iDelIdx);

        if (pFilterInfo) {
          if (pFilterInfo->flag & TSDB_FILTER_FLAG_IGNORE_DROPPED_TABLE) {
            SMetaInfo info;
            if (metaGetInfo(pIter->dIter.pReader->pTsdb->pVnode->pMeta, pDelIdx->uid, &info, NULL)) {
              pIter->tIter.iDelIdx++;
              continue;
            }
          }
        }

        code = tsdbReadDelDatav1(pIter->tIter.pReader, pDelIdx, pIter->tIter.aDelData, INT64_MAX);
        TSDB_CHECK_CODE(code, lino, _exit);

        pIter->delInfo.suid = pDelIdx->suid;
        pIter->delInfo.uid = pDelIdx->uid;
        pIter->tIter.iDelData = 0;
        pIter->tIter.iDelIdx++;
        break;
      } else {
        pIter->delInfo = (SDelInfo){0};
        goto _exit;
      }
    }
  }

_exit:
  if (code) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t tsdbDataIterNext2(STsdbDataIter2* pIter, STsdbFilterInfo* pFilterInfo) {
  int32_t code = 0;

  if (pIter->type == TSDB_MEM_TABLE_DATA_ITER) {
    ASSERT(0);
    return code;
  } else if (pIter->type == TSDB_DATA_FILE_DATA_ITER) {
    return tsdbDataFileDataIterNext(pIter, pFilterInfo);
  } else if (pIter->type == TSDB_STT_FILE_DATA_ITER) {
    return tsdbSttFileDataIterNext(pIter, pFilterInfo);
  } else if (pIter->type == TSDB_TOMB_FILE_DATA_ITER) {
    return tsdbTombFileDataIterNext(pIter, pFilterInfo);
  } else {
    ASSERT(0);
    return code;
  }
}
#endif

/* get */

// STsdbFSetIter
typedef struct STsdbFSetDataIter {
  STsdb*  pTsdb;
  int32_t flags;

  /* tombstone */
  SDelFReader* pDelFReader;
  SArray*      aDelIdx;    // SArray<SDelIdx>
  SArray*      aDelData;   // SArray<SDelData>
  SArray*      aSkeyLine;  // SArray<TABLEID>
  int32_t      iDelIdx;
  int32_t      iSkyLine;

  /* time-series data */
  SDataFReader*   pReader;
  STsdbDataIter2* iterList;
  STsdbDataIter2* pIter;
  SRBTree         rbt;
} STsdbFSetDataIter;
