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

extern int32_t tsdbReadDataBlockEx(SDataFReader* pReader, SDataBlk* pDataBlk, SBlockData* pBlockData);

// STsdbDataIter2 ========================================
#define TSDB_MEM_TABLE_DATA_ITER 0
#define TSDB_DATA_FILE_DATA_ITER 1
#define TSDB_STT_FILE_DATA_ITER  2

typedef struct STsdbDataIter2 STsdbDataIter2;
struct STsdbDataIter2 {
  STsdbDataIter2* next;
  SRBTreeNode     rbtn;

  int32_t  type;
  SRowInfo rowInfo;
  union {
    // TSDB_MEM_TABLE_DATA_ITER
    struct {
      SMemTable* pMemTable;
    } mIter;

    // TSDB_DATA_FILE_DATA_ITER
    struct {
      SDataFReader* pReader;
      SArray*       aBlockIdx;  // SArray<SBlockIdx>
      SMapData      mDataBlk;
      SBlockData    bData;
      int32_t       iBlockIdx;
      int32_t       iDataBlk;
      int32_t       iRow;
    } dIter;

    // TSDB_STT_FILE_DATA_ITER
    struct {
      SDataFReader* pReader;
      int32_t       iStt;
      SArray*       aSttBlk;
      SBlockData    bData;
      int32_t       iSttBlk;
      int32_t       iRow;
    } sIter;
  };
};

#define TSDB_RBTN_TO_DATA_ITER(pNode) ((STsdbDataIter2*)(((char*)pNode) - offsetof(STsdbDataIter2, rbtn)))

/* open */
static int32_t tsdbOpenDataFileDataIter(SDataFReader* pReader, STsdbDataIter2** ppIter) {
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
      tBlockDataDestroy(&pIter->dIter.bData, 1);
      taosArrayDestroy(pIter->dIter.aBlockIdx);
      taosMemoryFree(pIter);
      pIter = NULL;
    }
  }
  *ppIter = pIter;
  return code;
}

static int32_t tsdbOpenSttFileDataIter(SDataFReader* pReader, int32_t iStt, STsdbDataIter2** ppIter) {
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
      tBlockDataDestroy(&pIter->sIter.bData, 1);
      taosMemoryFree(pIter);
      pIter = NULL;
    }
  }
  *ppIter = pIter;
  return code;
}

/* close */
static void tsdbCloseDataFileDataIter(STsdbDataIter2* pIter) {
  tBlockDataDestroy(&pIter->dIter.bData, 1);
  tMapDataClear(&pIter->dIter.mDataBlk);
  taosArrayDestroy(pIter->dIter.aBlockIdx);
  taosMemoryFree(pIter);
}

static void tsdbCloseSttFileDataIter(STsdbDataIter2* pIter) {
  tBlockDataDestroy(&pIter->sIter.bData, 1);
  taosArrayDestroy(pIter->sIter.aSttBlk);
  taosMemoryFree(pIter);
}

static void tsdbCloseDataIter2(STsdbDataIter2* pIter) {
  if (pIter->type == TSDB_MEM_TABLE_DATA_ITER) {
    ASSERT(0);
  } else if (pIter->type == TSDB_DATA_FILE_DATA_ITER) {
    tsdbCloseDataFileDataIter(pIter);
  } else if (pIter->type == TSDB_STT_FILE_DATA_ITER) {
    tsdbCloseSttFileDataIter(pIter);
  } else {
    ASSERT(0);
  }
}

/* cmpr */
static int32_t tsdbDataIterCmprFn(const SRBTreeNode* pNode1, const SRBTreeNode* pNode2) {
  STsdbDataIter2* pIter1 = TSDB_RBTN_TO_DATA_ITER(pNode1);
  STsdbDataIter2* pIter2 = TSDB_RBTN_TO_DATA_ITER(pNode2);
  return tRowInfoCmprFn(&pIter1->rowInfo, &pIter2->rowInfo);
}

/* seek */

/* iter next */
static int32_t tsdbDataFileDataIterNext(STsdbDataIter2* pIter) {
  int32_t code = 0;
  int32_t lino = 0;

  for (;;) {
    if (pIter->dIter.iRow < pIter->dIter.bData.nRow) {
      pIter->rowInfo.suid = pIter->dIter.bData.suid;
      pIter->rowInfo.uid = pIter->dIter.bData.uid;
      pIter->rowInfo.row = tsdbRowFromBlockData(&pIter->dIter.bData, pIter->dIter.iRow);
      pIter->dIter.iRow++;
      break;
    }

    for (;;) {
      if (pIter->dIter.iDataBlk < pIter->dIter.mDataBlk.nItem) {
        SDataBlk dataBlk;
        tMapDataGetItemByIdx(&pIter->dIter.mDataBlk, pIter->dIter.iDataBlk, &dataBlk, tGetDataBlk);

        code = tsdbReadDataBlockEx(pIter->dIter.pReader, &dataBlk, &pIter->dIter.bData);
        TSDB_CHECK_CODE(code, lino, _exit);

        pIter->dIter.iDataBlk++;
        pIter->dIter.iRow = 0;

        break;
      }

      for (;;) {
        if (pIter->dIter.iBlockIdx < taosArrayGetSize(pIter->dIter.aBlockIdx)) {
          SBlockIdx* pBlockIdx = taosArrayGet(pIter->dIter.aBlockIdx, pIter->dIter.iBlockIdx);

          code = tsdbReadDataBlk(pIter->dIter.pReader, pBlockIdx, &pIter->dIter.mDataBlk);
          TSDB_CHECK_CODE(code, lino, _exit);

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

static int32_t tsdbSttFileDataIterNext(STsdbDataIter2* pIter) {
  int32_t code = 0;
  int32_t lino = 0;

  for (;;) {
    if (pIter->sIter.iRow < pIter->sIter.bData.nRow) {
      pIter->rowInfo.suid = pIter->sIter.bData.suid;
      pIter->rowInfo.uid = pIter->sIter.bData.uid ? pIter->sIter.bData.uid : pIter->sIter.bData.aUid[pIter->sIter.iRow];
      pIter->rowInfo.row = tsdbRowFromBlockData(&pIter->sIter.bData, pIter->sIter.iRow);
      pIter->sIter.iRow++;
      break;
    }

    if (pIter->sIter.iSttBlk < taosArrayGetSize(pIter->sIter.aSttBlk)) {
      SSttBlk* pSttBlk = taosArrayGet(pIter->sIter.aSttBlk, pIter->sIter.iSttBlk);

      code = tsdbReadSttBlockEx(pIter->sIter.pReader, pIter->sIter.iStt, pSttBlk, &pIter->sIter.bData);
      TSDB_CHECK_CODE(code, lino, _exit);

      pIter->sIter.iSttBlk++;

      pIter->sIter.iRow = 0;
    } else {
      pIter->rowInfo = (SRowInfo){0};
      break;
    }
  }

_exit:
  if (code) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbDataIterNext2(STsdbDataIter2* pIter) {
  int32_t code = 0;

  if (pIter->type == TSDB_MEM_TABLE_DATA_ITER) {
    ASSERT(0);
    return code;
  } else if (pIter->type == TSDB_DATA_FILE_DATA_ITER) {
    return tsdbDataFileDataIterNext(pIter);
  } else if (pIter->type == TSDB_STT_FILE_DATA_ITER) {
    return tsdbSttFileDataIterNext(pIter);
  } else {
    ASSERT(0);
    return code;
  }
}

/* get */

// STsdbSnapReader ========================================
typedef enum { SNAP_DATA_FILE_ITER = 0, SNAP_STT_FILE_ITER } EFIterT;
typedef struct {
  SRBTreeNode n;
  SRowInfo    rInfo;
  EFIterT     type;
  union {
    struct {
      SArray*    aBlockIdx;
      int32_t    iBlockIdx;
      SBlockIdx* pBlockIdx;
      SMapData   mBlock;
      int32_t    iBlock;
    };  // .data file
    struct {
      int32_t iStt;
      SArray* aSttBlk;
      int32_t iSttBlk;
    };  // .stt file
  };
  SBlockData bData;
  int32_t    iRow;
} SFDataIter;

struct STsdbSnapReader {
  STsdb*  pTsdb;
  int64_t sver;
  int64_t ever;
  STsdbFS fs;
  int8_t  type;
  // for data file
  int8_t        dataDone;
  int32_t       fid;
  SDataFReader* pDataFReader;
  SFDataIter*   pIter;
  SRBTree       rbt;
  SFDataIter    aFDataIter[TSDB_MAX_STT_TRIGGER + 1];
  SBlockData    bData;
  SSkmInfo      skmTable;
  // for del file
  int8_t       delDone;
  SDelFReader* pDelFReader;
  SArray*      aDelIdx;  // SArray<SDelIdx>
  int32_t      iDelIdx;
  SArray*      aDelData;  // SArray<SDelData>
  uint8_t*     aBuf[5];
};

extern int32_t tsdbUpdateTableSchema(SMeta* pMeta, int64_t suid, int64_t uid, SSkmInfo* pSkmInfo);

static int32_t tFDataIterCmprFn(const SRBTreeNode* pNode1, const SRBTreeNode* pNode2) {
  SFDataIter* pIter1 = (SFDataIter*)(((uint8_t*)pNode1) - offsetof(SFDataIter, n));
  SFDataIter* pIter2 = (SFDataIter*)(((uint8_t*)pNode2) - offsetof(SFDataIter, n));

  return tRowInfoCmprFn(&pIter1->rInfo, &pIter2->rInfo);
}

static int32_t tsdbSnapReadOpenFile(STsdbSnapReader* pReader) {
  int32_t code = 0;
  int32_t lino = 0;

  SDFileSet  dFileSet = {.fid = pReader->fid};
  SDFileSet* pSet = taosArraySearch(pReader->fs.aDFileSet, &dFileSet, tDFileSetCmprFn, TD_GT);
  if (pSet == NULL) return code;

  pReader->fid = pSet->fid;
  code = tsdbDataFReaderOpen(&pReader->pDataFReader, pReader->pTsdb, pSet);
  TSDB_CHECK_CODE(code, lino, _exit);

  pReader->pIter = NULL;
  tRBTreeCreate(&pReader->rbt, tFDataIterCmprFn);

  // .data file
  SFDataIter* pIter = &pReader->aFDataIter[0];
  pIter->type = SNAP_DATA_FILE_ITER;

  code = tsdbReadBlockIdx(pReader->pDataFReader, pIter->aBlockIdx);
  TSDB_CHECK_CODE(code, lino, _exit);

  for (pIter->iBlockIdx = 0; pIter->iBlockIdx < taosArrayGetSize(pIter->aBlockIdx); pIter->iBlockIdx++) {
    pIter->pBlockIdx = (SBlockIdx*)taosArrayGet(pIter->aBlockIdx, pIter->iBlockIdx);

    code = tsdbReadDataBlk(pReader->pDataFReader, pIter->pBlockIdx, &pIter->mBlock);
    TSDB_CHECK_CODE(code, lino, _exit);

    for (pIter->iBlock = 0; pIter->iBlock < pIter->mBlock.nItem; pIter->iBlock++) {
      SDataBlk dataBlk;
      tMapDataGetItemByIdx(&pIter->mBlock, pIter->iBlock, &dataBlk, tGetDataBlk);

      if (dataBlk.minVer > pReader->ever || dataBlk.maxVer < pReader->sver) continue;

      code = tsdbReadDataBlockEx(pReader->pDataFReader, &dataBlk, &pIter->bData);
      TSDB_CHECK_CODE(code, lino, _exit);

      ASSERT(pIter->pBlockIdx->suid == pIter->bData.suid);
      ASSERT(pIter->pBlockIdx->uid == pIter->bData.uid);

      for (pIter->iRow = 0; pIter->iRow < pIter->bData.nRow; pIter->iRow++) {
        int64_t rowVer = pIter->bData.aVersion[pIter->iRow];

        if (rowVer >= pReader->sver && rowVer <= pReader->ever) {
          pIter->rInfo.suid = pIter->pBlockIdx->suid;
          pIter->rInfo.uid = pIter->pBlockIdx->uid;
          pIter->rInfo.row = tsdbRowFromBlockData(&pIter->bData, pIter->iRow);
          goto _add_iter_and_break;
        }
      }
    }

    continue;

  _add_iter_and_break:
    tRBTreePut(&pReader->rbt, (SRBTreeNode*)pIter);
    break;
  }

  // .stt file
  pIter = &pReader->aFDataIter[1];
  for (int32_t iStt = 0; iStt < pSet->nSttF; iStt++) {
    pIter->type = SNAP_STT_FILE_ITER;
    pIter->iStt = iStt;

    code = tsdbReadSttBlk(pReader->pDataFReader, iStt, pIter->aSttBlk);
    TSDB_CHECK_CODE(code, lino, _exit);

    for (pIter->iSttBlk = 0; pIter->iSttBlk < taosArrayGetSize(pIter->aSttBlk); pIter->iSttBlk++) {
      SSttBlk* pSttBlk = (SSttBlk*)taosArrayGet(pIter->aSttBlk, pIter->iSttBlk);

      if (pSttBlk->minVer > pReader->ever) continue;
      if (pSttBlk->maxVer < pReader->sver) continue;

      code = tsdbReadSttBlockEx(pReader->pDataFReader, iStt, pSttBlk, &pIter->bData);
      TSDB_CHECK_CODE(code, lino, _exit);

      for (pIter->iRow = 0; pIter->iRow < pIter->bData.nRow; pIter->iRow++) {
        int64_t rowVer = pIter->bData.aVersion[pIter->iRow];

        if (rowVer >= pReader->sver && rowVer <= pReader->ever) {
          pIter->rInfo.suid = pIter->bData.suid;
          pIter->rInfo.uid = pIter->bData.uid ? pIter->bData.uid : pIter->bData.aUid[pIter->iRow];
          pIter->rInfo.row = tsdbRowFromBlockData(&pIter->bData, pIter->iRow);
          goto _add_iter;
        }
      }
    }

    continue;

  _add_iter:
    tRBTreePut(&pReader->rbt, (SRBTreeNode*)pIter);
    pIter++;
  }

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed since %s", TD_VID(pReader->pTsdb->pVnode), __func__, tstrerror(code));
  } else {
    tsdbInfo("vgId:%d, %s done, path:%s, fid:%d", TD_VID(pReader->pTsdb->pVnode), __func__, pReader->pTsdb->path,
             pReader->fid);
  }
  return code;
}

static int32_t tsdbSnapNextRow(STsdbSnapReader* pReader) {
  int32_t code = 0;

  if (pReader->pIter) {
    SFDataIter* pIter = NULL;
    while (true) {
    _find_row:
      pIter = pReader->pIter;
      for (pIter->iRow++; pIter->iRow < pIter->bData.nRow; pIter->iRow++) {
        int64_t rowVer = pIter->bData.aVersion[pIter->iRow];

        if (rowVer >= pReader->sver && rowVer <= pReader->ever) {
          pIter->rInfo.suid = pIter->bData.suid;
          pIter->rInfo.uid = pIter->bData.uid ? pIter->bData.uid : pIter->bData.aUid[pIter->iRow];
          pIter->rInfo.row = tsdbRowFromBlockData(&pIter->bData, pIter->iRow);
          goto _out;
        }
      }

      if (pIter->type == SNAP_DATA_FILE_ITER) {
        while (true) {
          for (pIter->iBlock++; pIter->iBlock < pIter->mBlock.nItem; pIter->iBlock++) {
            SDataBlk dataBlk;
            tMapDataGetItemByIdx(&pIter->mBlock, pIter->iBlock, &dataBlk, tGetDataBlk);

            if (dataBlk.minVer > pReader->ever || dataBlk.maxVer < pReader->sver) continue;

            code = tsdbReadDataBlockEx(pReader->pDataFReader, &dataBlk, &pIter->bData);
            if (code) goto _err;

            pIter->iRow = -1;
            goto _find_row;
          }

          pIter->iBlockIdx++;
          if (pIter->iBlockIdx >= taosArrayGetSize(pIter->aBlockIdx)) break;

          pIter->pBlockIdx = (SBlockIdx*)taosArrayGet(pIter->aBlockIdx, pIter->iBlockIdx);
          code = tsdbReadDataBlk(pReader->pDataFReader, pIter->pBlockIdx, &pIter->mBlock);
          if (code) goto _err;
          pIter->iBlock = -1;
        }

        pReader->pIter = NULL;
        break;
      } else if (pIter->type == SNAP_STT_FILE_ITER) {
        for (pIter->iSttBlk++; pIter->iSttBlk < taosArrayGetSize(pIter->aSttBlk); pIter->iSttBlk++) {
          SSttBlk* pSttBlk = (SSttBlk*)taosArrayGet(pIter->aSttBlk, pIter->iSttBlk);

          if (pSttBlk->minVer > pReader->ever || pSttBlk->maxVer < pReader->sver) continue;

          code = tsdbReadSttBlockEx(pReader->pDataFReader, pIter->iStt, pSttBlk, &pIter->bData);
          if (code) goto _err;

          pIter->iRow = -1;
          goto _find_row;
        }

        pReader->pIter = NULL;
        break;
      } else {
        ASSERT(0);
      }
    }

  _out:
    pIter = (SFDataIter*)tRBTreeMin(&pReader->rbt);
    if (pReader->pIter && pIter) {
      int32_t c = tRowInfoCmprFn(&pReader->pIter->rInfo, &pIter->rInfo);
      if (c > 0) {
        tRBTreePut(&pReader->rbt, (SRBTreeNode*)pReader->pIter);
        pReader->pIter = NULL;
      } else {
        ASSERT(c);
      }
    }
  }

  if (pReader->pIter == NULL) {
    pReader->pIter = (SFDataIter*)tRBTreeMin(&pReader->rbt);
    if (pReader->pIter) {
      tRBTreeDrop(&pReader->rbt, (SRBTreeNode*)pReader->pIter);
    }
  }

  return code;

_err:
  return code;
}

static SRowInfo* tsdbSnapGetRow(STsdbSnapReader* pReader) {
  if (pReader->pIter) {
    return &pReader->pIter->rInfo;
  } else {
    tsdbSnapNextRow(pReader);

    if (pReader->pIter) {
      return &pReader->pIter->rInfo;
    } else {
      return NULL;
    }
  }
}

static int32_t tsdbSnapCmprData(STsdbSnapReader* pReader, uint8_t** ppData) {
  int32_t code = 0;

  ASSERT(pReader->bData.nRow);

  int32_t aBufN[5] = {0};
  code = tCmprBlockData(&pReader->bData, TWO_STAGE_COMP, NULL, NULL, pReader->aBuf, aBufN);
  if (code) goto _exit;

  int32_t size = aBufN[0] + aBufN[1] + aBufN[2] + aBufN[3];
  *ppData = taosMemoryMalloc(sizeof(SSnapDataHdr) + size);
  if (*ppData == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  SSnapDataHdr* pHdr = (SSnapDataHdr*)*ppData;
  pHdr->type = SNAP_DATA_TSDB;
  pHdr->size = size;

  memcpy(pHdr->data, pReader->aBuf[3], aBufN[3]);
  memcpy(pHdr->data + aBufN[3], pReader->aBuf[2], aBufN[2]);
  if (aBufN[1]) {
    memcpy(pHdr->data + aBufN[3] + aBufN[2], pReader->aBuf[1], aBufN[1]);
  }
  if (aBufN[0]) {
    memcpy(pHdr->data + aBufN[3] + aBufN[2] + aBufN[1], pReader->aBuf[0], aBufN[0]);
  }

_exit:
  return code;
}

static int32_t tsdbSnapReadData(STsdbSnapReader* pReader, uint8_t** ppData) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdb* pTsdb = pReader->pTsdb;

  while (true) {
    if (pReader->pDataFReader == NULL) {
      code = tsdbSnapReadOpenFile(pReader);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    if (pReader->pDataFReader == NULL) break;

    SRowInfo* pRowInfo = tsdbSnapGetRow(pReader);
    if (pRowInfo == NULL) {
      tsdbDataFReaderClose(&pReader->pDataFReader);
      continue;
    }

    TABLEID     id = {.suid = pRowInfo->suid, .uid = pRowInfo->uid};
    SBlockData* pBlockData = &pReader->bData;

    code = tsdbUpdateTableSchema(pTsdb->pVnode->pMeta, id.suid, id.uid, &pReader->skmTable);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tBlockDataInit(pBlockData, &id, pReader->skmTable.pTSchema, NULL, 0);
    TSDB_CHECK_CODE(code, lino, _exit);

    while (pRowInfo->suid == id.suid && pRowInfo->uid == id.uid) {
      code = tBlockDataAppendRow(pBlockData, &pRowInfo->row, NULL, pRowInfo->uid);
      TSDB_CHECK_CODE(code, lino, _exit);

      code = tsdbSnapNextRow(pReader);
      TSDB_CHECK_CODE(code, lino, _exit);

      pRowInfo = tsdbSnapGetRow(pReader);
      if (pRowInfo == NULL) {
        tsdbDataFReaderClose(&pReader->pDataFReader);
        break;
      }

      if (pBlockData->nRow >= 4096) break;
    }

    code = tsdbSnapCmprData(pReader, ppData);
    TSDB_CHECK_CODE(code, lino, _exit);

    break;
  }

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed since %s, path:%s", TD_VID(pTsdb->pVnode), __func__, tstrerror(code), pTsdb->path);
  }
  return code;
}

static int32_t tsdbSnapReadDel(STsdbSnapReader* pReader, uint8_t** ppData) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdb*    pTsdb = pReader->pTsdb;
  SDelFile* pDelFile = pReader->fs.pDelFile;

  if (pReader->pDelFReader == NULL) {
    if (pDelFile == NULL) {
      goto _exit;
    }

    // open
    code = tsdbDelFReaderOpen(&pReader->pDelFReader, pDelFile, pTsdb);
    TSDB_CHECK_CODE(code, lino, _exit);

    // read index
    code = tsdbReadDelIdx(pReader->pDelFReader, pReader->aDelIdx);
    TSDB_CHECK_CODE(code, lino, _exit);

    pReader->iDelIdx = 0;
  }

  while (true) {
    if (pReader->iDelIdx >= taosArrayGetSize(pReader->aDelIdx)) {
      tsdbDelFReaderClose(&pReader->pDelFReader);
      break;
    }

    SDelIdx* pDelIdx = (SDelIdx*)taosArrayGet(pReader->aDelIdx, pReader->iDelIdx);

    pReader->iDelIdx++;

    code = tsdbReadDelData(pReader->pDelFReader, pDelIdx, pReader->aDelData);
    TSDB_CHECK_CODE(code, lino, _exit);

    int32_t size = 0;
    for (int32_t iDelData = 0; iDelData < taosArrayGetSize(pReader->aDelData); iDelData++) {
      SDelData* pDelData = (SDelData*)taosArrayGet(pReader->aDelData, iDelData);

      if (pDelData->version >= pReader->sver && pDelData->version <= pReader->ever) {
        size += tPutDelData(NULL, pDelData);
      }
    }
    if (size == 0) continue;

    // org data
    size = sizeof(TABLEID) + size;
    *ppData = taosMemoryMalloc(sizeof(SSnapDataHdr) + size);
    if (*ppData == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    SSnapDataHdr* pHdr = (SSnapDataHdr*)(*ppData);
    pHdr->type = SNAP_DATA_DEL;
    pHdr->size = size;

    TABLEID* pId = (TABLEID*)(&pHdr[1]);
    pId->suid = pDelIdx->suid;
    pId->uid = pDelIdx->uid;
    int32_t n = sizeof(SSnapDataHdr) + sizeof(TABLEID);
    for (int32_t iDelData = 0; iDelData < taosArrayGetSize(pReader->aDelData); iDelData++) {
      SDelData* pDelData = (SDelData*)taosArrayGet(pReader->aDelData, iDelData);

      if (pDelData->version < pReader->sver) continue;
      if (pDelData->version > pReader->ever) continue;

      n += tPutDelData((*ppData) + n, pDelData);
    }

    tsdbInfo("vgId:%d, vnode snapshot tsdb read del data for %s, suid:%" PRId64 " uid:%" PRId64 " size:%d",
             TD_VID(pTsdb->pVnode), pTsdb->path, pDelIdx->suid, pDelIdx->uid, size);

    break;
  }

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed since %s, path:%s", TD_VID(pTsdb->pVnode), __func__, tstrerror(code), pTsdb->path);
  }
  return code;
}

int32_t tsdbSnapReaderOpen(STsdb* pTsdb, int64_t sver, int64_t ever, int8_t type, STsdbSnapReader** ppReader) {
  int32_t          code = 0;
  int32_t          lino = 0;
  STsdbSnapReader* pReader = NULL;

  // alloc
  pReader = (STsdbSnapReader*)taosMemoryCalloc(1, sizeof(*pReader));
  if (pReader == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  pReader->pTsdb = pTsdb;
  pReader->sver = sver;
  pReader->ever = ever;
  pReader->type = type;

  code = taosThreadRwlockRdlock(&pTsdb->rwLock);
  if (code) {
    code = TAOS_SYSTEM_ERROR(code);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tsdbFSRef(pTsdb, &pReader->fs);
  if (code) {
    taosThreadRwlockUnlock(&pTsdb->rwLock);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = taosThreadRwlockUnlock(&pTsdb->rwLock);
  if (code) {
    code = TAOS_SYSTEM_ERROR(code);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // data
  pReader->fid = INT32_MIN;
  for (int32_t iIter = 0; iIter < sizeof(pReader->aFDataIter) / sizeof(pReader->aFDataIter[0]); iIter++) {
    SFDataIter* pIter = &pReader->aFDataIter[iIter];

    if (iIter == 0) {
      pIter->aBlockIdx = taosArrayInit(0, sizeof(SBlockIdx));
      if (pIter->aBlockIdx == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        TSDB_CHECK_CODE(code, lino, _exit);
      }
    } else {
      pIter->aSttBlk = taosArrayInit(0, sizeof(SSttBlk));
      if (pIter->aSttBlk == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        TSDB_CHECK_CODE(code, lino, _exit);
      }
    }

    code = tBlockDataCreate(&pIter->bData);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tBlockDataCreate(&pReader->bData);
  TSDB_CHECK_CODE(code, lino, _exit);

  // del
  pReader->aDelIdx = taosArrayInit(0, sizeof(SDelIdx));
  if (pReader->aDelIdx == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  pReader->aDelData = taosArrayInit(0, sizeof(SDelData));
  if (pReader->aDelData == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s, TSDB path: %s", TD_VID(pTsdb->pVnode), __func__, lino,
              tstrerror(code), pTsdb->path);
    *ppReader = NULL;

    if (pReader) {
      taosArrayDestroy(pReader->aDelData);
      taosArrayDestroy(pReader->aDelIdx);
      tBlockDataDestroy(&pReader->bData, 1);
      tsdbFSDestroy(&pReader->fs);
      taosMemoryFree(pReader);
    }
  } else {
    *ppReader = pReader;
    tsdbInfo("vgId:%d, vnode snapshot tsdb reader opened for %s", TD_VID(pTsdb->pVnode), pTsdb->path);
  }
  return code;
}

int32_t tsdbSnapReaderClose(STsdbSnapReader** ppReader) {
  int32_t          code = 0;
  STsdbSnapReader* pReader = *ppReader;

  // data
  if (pReader->pDataFReader) tsdbDataFReaderClose(&pReader->pDataFReader);
  for (int32_t iIter = 0; iIter < sizeof(pReader->aFDataIter) / sizeof(pReader->aFDataIter[0]); iIter++) {
    SFDataIter* pIter = &pReader->aFDataIter[iIter];

    if (iIter == 0) {
      taosArrayDestroy(pIter->aBlockIdx);
      tMapDataClear(&pIter->mBlock);
    } else {
      taosArrayDestroy(pIter->aSttBlk);
    }

    tBlockDataDestroy(&pIter->bData, 1);
  }

  tBlockDataDestroy(&pReader->bData, 1);
  tDestroyTSchema(pReader->skmTable.pTSchema);

  // del
  if (pReader->pDelFReader) tsdbDelFReaderClose(&pReader->pDelFReader);
  taosArrayDestroy(pReader->aDelIdx);
  taosArrayDestroy(pReader->aDelData);

  tsdbFSUnref(pReader->pTsdb, &pReader->fs);

  tsdbInfo("vgId:%d, vnode snapshot tsdb reader closed for %s", TD_VID(pReader->pTsdb->pVnode), pReader->pTsdb->path);

  for (int32_t iBuf = 0; iBuf < sizeof(pReader->aBuf) / sizeof(pReader->aBuf[0]); iBuf++) {
    tFree(pReader->aBuf[iBuf]);
  }

  taosMemoryFree(pReader);
  *ppReader = NULL;
  return code;
}

int32_t tsdbSnapRead(STsdbSnapReader* pReader, uint8_t** ppData) {
  int32_t code = 0;
  int32_t lino = 0;

  *ppData = NULL;

  // read data file
  if (!pReader->dataDone) {
    code = tsdbSnapReadData(pReader, ppData);
    TSDB_CHECK_CODE(code, lino, _exit);
    if (*ppData) {
      goto _exit;
    } else {
      pReader->dataDone = 1;
    }
  }

  // read del file
  if (!pReader->delDone) {
    code = tsdbSnapReadDel(pReader, ppData);
    TSDB_CHECK_CODE(code, lino, _exit);
    if (*ppData) {
      goto _exit;
    } else {
      pReader->delDone = 1;
    }
  }

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed since %s, path:%s", TD_VID(pReader->pTsdb->pVnode), __func__, tstrerror(code),
              pReader->pTsdb->path);
  } else {
    tsdbDebug("vgId:%d, %s done, path:%s", TD_VID(pReader->pTsdb->pVnode), __func__, pReader->pTsdb->path);
  }
  return code;
}

// STsdbSnapWriter ========================================
struct STsdbSnapWriter {
  STsdb*   pTsdb;
  int64_t  sver;
  int64_t  ever;
  int32_t  minutes;
  int8_t   precision;
  int32_t  minRow;
  int32_t  maxRow;
  int8_t   cmprAlg;
  int64_t  commitID;
  uint8_t* aBuf[5];

  STsdbFS fs;

  // time-series data
  SBlockData inData;

  int32_t  fid;
  TABLEID  tbid;
  SSkmInfo skmTable;

  /* reader */
  SDataFReader*   pDataFReader;
  STsdbDataIter2* iterList;
  STsdbDataIter2* pDIter;
  STsdbDataIter2* pIter;
  SRBTree         rbt;  // SRBTree<STsdbDataIter2>

  /* writer */
  SDataFWriter* pDataFWriter;
  SArray*       aBlockIdx;
  SMapData      mDataBlk;  // SMapData<SDataBlk>
  SArray*       aSttBlk;   // SArray<SSttBlk>
  SBlockData    bData;
  SBlockData    sData;

  // tombstone data
  SDelFReader* pDelFReader;
  SDelFWriter* pDelFWriter;
  int32_t      iDelIdx;
  SArray*      aDelIdxR;
  SArray*      aDelData;
  SArray*      aDelIdxW;
};

// SNAP_DATA_TSDB
extern int32_t tsdbWriteDataBlock(SDataFWriter* pWriter, SBlockData* pBlockData, SMapData* mDataBlk, int8_t cmprAlg);
extern int32_t tsdbWriteSttBlock(SDataFWriter* pWriter, SBlockData* pBlockData, SArray* aSttBlk, int8_t cmprAlg);

static int32_t tsdbSnapNextTableData(STsdbSnapWriter* pWriter) {
  int32_t code = 0;
  int32_t lino = 0;

#if 0
  ASSERT(pWriter->dReader.iRow >= pWriter->dReader.bData.nRow);

  if (pWriter->dReader.iBlockIdx < taosArrayGetSize(pWriter->dReader.aBlockIdx)) {
    pWriter->dReader.pBlockIdx = (SBlockIdx*)taosArrayGet(pWriter->dReader.aBlockIdx, pWriter->dReader.iBlockIdx);

    code = tsdbReadDataBlk(pWriter->dReader.pReader, pWriter->dReader.pBlockIdx, &pWriter->dReader.mDataBlk);
    if (code) goto _exit;

    pWriter->dReader.iBlockIdx++;
  } else {
    pWriter->dReader.pBlockIdx = NULL;
    tMapDataReset(&pWriter->dReader.mDataBlk);
  }
  pWriter->dReader.iDataBlk = 0;  // point to the next one
  tBlockDataReset(&pWriter->dReader.bData);
  pWriter->dReader.iRow = 0;
#endif

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pWriter->pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbSnapWriteCopyData(STsdbSnapWriter* pWriter, TABLEID* pId) {
  int32_t code = 0;
  int32_t lino = 0;

#if 0
  while (true) {
    if (pWriter->dReader.pBlockIdx == NULL) break;
    if (tTABLEIDCmprFn(pWriter->dReader.pBlockIdx, pId) >= 0) break;

    SBlockIdx blkIdx = *pWriter->dReader.pBlockIdx;
    code = tsdbWriteDataBlk(pWriter->dWriter.pWriter, &pWriter->dReader.mDataBlk, &blkIdx);
    if (code) goto _exit;

    if (taosArrayPush(pWriter->dWriter.aBlockIdx, &blkIdx) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }

    code = tsdbSnapNextTableData(pWriter);
    if (code) goto _exit;
  }
#endif

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pWriter->pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbSnapWriteTableDataStart(STsdbSnapWriter* pWriter, TABLEID* pId) {
  int32_t code = 0;
  int32_t lino = 0;

  if (pId) {
    pWriter->tbid = *pId;
  } else {
    pWriter->tbid = (TABLEID){INT64_MAX, INT64_MAX};
  }

  if (pWriter->pDIter) {
    STsdbDataIter2* pIter = pWriter->pDIter;

    // assert last table data end
    ASSERT(pIter->dIter.iRow >= pIter->dIter.bData.nRow);
    ASSERT(pIter->dIter.iDataBlk >= pIter->dIter.mDataBlk.nItem);

    for (;;) {
      if (pIter->dIter.iBlockIdx >= taosArrayGetSize(pIter->dIter.aBlockIdx)) {
        pWriter->pDIter = NULL;
        break;
      }

      SBlockIdx* pBlockIdx = (SBlockIdx*)taosArrayGet(pIter->dIter.aBlockIdx, pIter->dIter.iBlockIdx);

      int32_t c = tTABLEIDCmprFn(pBlockIdx, &pWriter->tbid);
      if (c < 0) {
        code = tsdbReadDataBlk(pIter->dIter.pReader, pBlockIdx, &pIter->dIter.mDataBlk);
        TSDB_CHECK_CODE(code, lino, _exit);

        SBlockIdx* pNewBlockIdx = taosArrayReserve(pWriter->aBlockIdx, 1);
        if (pNewBlockIdx == NULL) {
          code == TSDB_CODE_OUT_OF_MEMORY;
          TSDB_CHECK_CODE(code, lino, _exit);
        }

        pNewBlockIdx->suid = pBlockIdx->suid;
        pNewBlockIdx->uid = pBlockIdx->uid;

        code = tsdbWriteDataBlk(pWriter->pDataFWriter, &pIter->dIter.mDataBlk, pNewBlockIdx);
        TSDB_CHECK_CODE(code, lino, _exit);

        pIter->dIter.iBlockIdx++;
      } else if (c == 0) {
        code = tsdbReadDataBlk(pIter->dIter.pReader, pBlockIdx, &pIter->dIter.mDataBlk);
        TSDB_CHECK_CODE(code, lino, _exit);

        pIter->dIter.iDataBlk = 0;
        pIter->dIter.iBlockIdx++;

        break;
      } else {
        pIter->dIter.iDataBlk = pIter->dIter.mDataBlk.nItem;
        break;
      }
    }
  }

  if (pId == NULL) {
    if (pWriter->sData.nRow) {
      code = tsdbWriteSttBlock(pWriter->pDataFWriter, &pWriter->sData, pWriter->aSttBlk, pWriter->cmprAlg);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  } else {
    code = tsdbUpdateTableSchema(pWriter->pTsdb->pVnode->pMeta, pId->suid, pId->uid, &pWriter->skmTable);
    TSDB_CHECK_CODE(code, lino, _exit);

    tMapDataReset(&pWriter->mDataBlk);

    code = tBlockDataInit(&pWriter->bData, pId, pWriter->skmTable.pTSchema, NULL, 0);
    TSDB_CHECK_CODE(code, lino, _exit);

    // TODO: init pWriter->sData ??
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pWriter->pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbTrace("vgId:%d %s done, suid:%" PRId64 " uid:%" PRId64, TD_VID(pWriter->pTsdb->pVnode), __func__,
              pWriter->tbid.suid, pWriter->tbid.uid);
  }
  return code;
}

static int32_t tsdbSnapWriteTableDataEnd(STsdbSnapWriter* pWriter) {
  int32_t code = 0;
  int32_t lino = 0;

  // write a NULL row to end current table data write
  code = tsdbSnapWriteTableRow(pWriter, NULL);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (pWriter->bData.nRow > 0) {
    if (pWriter->bData.nRow < pWriter->minRow) {
      for (int32_t iRow = 0; iRow < pWriter->bData.nRow; iRow++) {
        code = tBlockDataAppendRow(&pWriter->sData, &tsdbRowFromBlockData(&pWriter->bData, iRow),
                                   pWriter->skmTable.pTSchema, pWriter->tbid.uid);
        TSDB_CHECK_CODE(code, lino, _exit);

        if (pWriter->sData.nRow >= pWriter->maxRow) {
          code = tsdbWriteSttBlock(pWriter->pDataFWriter, &pWriter->sData, pWriter->aSttBlk, pWriter->cmprAlg);
          TSDB_CHECK_CODE(code, lino, _exit);
        }
      }

      tBlockDataClear(&pWriter->bData);
    } else {
      code = tsdbWriteDataBlock(pWriter->pDataFWriter, &pWriter->bData, &pWriter->mDataBlk, pWriter->cmprAlg);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  if (pWriter->mDataBlk.nItem) {
    SBlockIdx* pBlockIdx = taosArrayReserve(pWriter->aBlockIdx, 1);
    if (pBlockIdx == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    pBlockIdx->suid = pWriter->tbid.suid;
    pBlockIdx->uid = pWriter->tbid.uid;

    code = tsdbWriteDataBlk(pWriter->pDataFWriter, &pWriter->mDataBlk, pBlockIdx);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pWriter->pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbSnapWriteFileDataStart(STsdbSnapWriter* pWriter, int32_t fid) {
  int32_t code = 0;
  int32_t lino = 0;

  ASSERT(pWriter->pDataFWriter == NULL && pWriter->fid < fid);

  STsdb* pTsdb = pWriter->pTsdb;

  pWriter->fid = fid;
  pWriter->tbid = (TABLEID){0};
  SDFileSet* pSet = taosArraySearch(pWriter->fs.aDFileSet, &(SDFileSet){.fid = fid}, tDFileSetCmprFn, TD_EQ);

  // open reader
  pWriter->pDataFReader = NULL;
  pWriter->iterList = NULL;
  pWriter->pDIter = NULL;
  pWriter->pIter = NULL;
  tRBTreeCreate(&pWriter->rbt, tsdbDataIterCmprFn);
  if (pSet) {
    code = tsdbDataFReaderOpen(&pWriter->pDataFReader, pTsdb, pSet);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbOpenDataFileDataIter(pWriter->pDataFReader, &pWriter->pDIter);
    TSDB_CHECK_CODE(code, lino, _exit);
    if (pWriter->pDIter) {
      pWriter->pDIter->next = pWriter->iterList;
      pWriter->iterList = pWriter->pDIter;
    }

    for (int32_t iStt = 0; iStt < pSet->nSttF; iStt++) {
      code = tsdbOpenSttFileDataIter(pWriter->pDataFReader, iStt, &pWriter->pIter);
      TSDB_CHECK_CODE(code, lino, _exit);

      if (pWriter->pIter) {
        code = tsdbSttFileDataIterNext(pWriter->pIter);
        TSDB_CHECK_CODE(code, lino, _exit);

        // add to tree
        tRBTreePut(&pWriter->rbt, &pWriter->pIter->rbtn);

        // add to list
        pWriter->pIter->next = pWriter->iterList;
        pWriter->iterList = pWriter->pIter;
      }
    }

    pWriter->pIter = NULL;
  }

  // open writer
  SDiskID diskId;
  if (pSet) {
    diskId = pSet->diskId;
  } else {
    tfsAllocDisk(pTsdb->pVnode->pTfs, 0 /*TODO*/, &diskId);
    tfsMkdirRecurAt(pTsdb->pVnode->pTfs, pTsdb->path, diskId);
  }
  SDFileSet wSet = {.diskId = diskId,
                    .fid = fid,
                    .pHeadF = &(SHeadFile){.commitID = pWriter->commitID},
                    .pDataF = (pSet) ? pSet->pDataF : &(SDataFile){.commitID = pWriter->commitID},
                    .pSmaF = (pSet) ? pSet->pSmaF : &(SSmaFile){.commitID = pWriter->commitID},
                    .nSttF = 1,
                    .aSttF = {&(SSttFile){.commitID = pWriter->commitID}}};
  code = tsdbDataFWriterOpen(&pWriter->pDataFWriter, pTsdb, &wSet);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (pWriter->aBlockIdx) {
    taosArrayClear(pWriter->aBlockIdx);
  } else if ((pWriter->aBlockIdx = taosArrayInit(0, sizeof(SBlockIdx))) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  tMapDataReset(&pWriter->mDataBlk);

  if (pWriter->aSttBlk) {
    taosArrayClear(pWriter->aSttBlk);
  } else if ((pWriter->aSttBlk = taosArrayInit(0, sizeof(SSttBlk))) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  tBlockDataReset(&pWriter->bData);
  tBlockDataReset(&pWriter->sData);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s, fid:%d", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code),
              fid);
  } else {
    tsdbDebug("vgId:%d %s done, fid:%d", TD_VID(pTsdb->pVnode), __func__, fid);
  }
  return code;
}

static int32_t tsdbSnapWriteFileDataEnd(STsdbSnapWriter* pWriter) {
  int32_t code = 0;
  int32_t lino = 0;

  ASSERT(pWriter->pDataFWriter);

  code = tsdbSnapWriteTableData(pWriter, NULL);
  TSDB_CHECK_CODE(code, lino, _exit);

  // TODO: ??

  if (pWriter->sData.nRow) {
    code = tsdbWriteSttBlock(pWriter->pDataFWriter, &pWriter->sData, pWriter->aSttBlk, pWriter->cmprAlg);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // do file-level updates
  code = tsdbWriteSttBlk(pWriter->pDataFWriter, pWriter->aSttBlk);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbWriteBlockIdx(pWriter->pDataFWriter, pWriter->aBlockIdx);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbUpdateDFileSetHeader(pWriter->pDataFWriter);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbFSUpsertFSet(&pWriter->fs, &pWriter->pDataFWriter->wSet);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbDataFWriterClose(&pWriter->pDataFWriter, 1);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (pWriter->pDataFReader) {
    code = tsdbDataFReaderClose(&pWriter->pDataFReader);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // clear sources
  while (pWriter->iterList) {
    STsdbDataIter2* pIter = pWriter->iterList;
    pWriter->iterList = pIter->next;
    tsdbCloseDataIter2(pIter);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed since %s", TD_VID(pWriter->pTsdb->pVnode), __func__, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s is done", TD_VID(pWriter->pTsdb->pVnode), __func__);
  }
  return code;
}

static int32_t tsdbSnapWriteNextRow(STsdbSnapWriter* pWriter, SRowInfo** ppRowInfo) {
  int32_t code = 0;
  int32_t lino = 0;

  if (pWriter->pIter) {
    code = tsdbDataIterNext2(pWriter->pIter);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (pWriter->pIter->rowInfo.suid == 0 && pWriter->pIter->rowInfo.uid == 0) {
      pWriter->pIter = NULL;
    } else {
      SRBTreeNode* pNode = tRBTreeMin(&pWriter->rbt);
      if (pNode) {
        int32_t c = tsdbDataIterCmprFn(&pWriter->pIter->rbtn, pNode);
        if (c > 0) {
          tRBTreePut(&pWriter->rbt, &pWriter->pIter->rbtn);
          pWriter->pIter = NULL;
        } else if (c == 0) {
          ASSERT(0);
        }
      }
    }
  }

  if (pWriter->pIter == NULL) {
    SRBTreeNode* pNode = tRBTreeMin(&pWriter->rbt);
    if (pNode) {
      tRBTreeDrop(&pWriter->rbt, pNode);
      pWriter->pIter = TSDB_RBTN_TO_DATA_ITER(pNode);
    }
  }

  if (ppRowInfo) {
    if (pWriter->pIter) {
      *ppRowInfo = &pWriter->pIter->rowInfo;
    } else {
      *ppRowInfo = NULL;
    }
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pWriter->pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbSnapWriteGetRow(STsdbSnapWriter* pWriter, SRowInfo** ppRowInfo) {
  int32_t code = 0;
  int32_t lino = 0;

  if (pWriter->pIter) {
    *ppRowInfo = &pWriter->pIter->rowInfo;
    goto _exit;
  }

  code = tsdbSnapWriteNextRow(pWriter, ppRowInfo);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pWriter->pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbSnapWriteTableRowImpl(STsdbSnapWriter* pWriter, TSDBROW* pRow) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tBlockDataAppendRow(&pWriter->bData, pRow, pWriter->skmTable.pTSchema, pWriter->tbid.uid);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (pWriter->bData.nRow >= pWriter->maxRow) {
    code = tsdbWriteDataBlock(pWriter->pDataFWriter, &pWriter->bData, &pWriter->mDataBlk, pWriter->cmprAlg);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pWriter->pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbSnapWriteTableRow(STsdbSnapWriter* pWriter, TSDBROW* pRow) {
  int32_t code = 0;
  int32_t lino = 0;

  TSDBKEY inKey = pRow ? TSDBROW_KEY(pRow) : TSDBKEY_MAX;

  if (pWriter->pDIter == NULL || (pWriter->pDIter->dIter.iRow >= pWriter->pDIter->dIter.bData.nRow &&
                                  pWriter->pDIter->dIter.iDataBlk >= pWriter->pDIter->dIter.mDataBlk.nItem)) {
    goto _write_row;
  } else {
    for (;;) {
      while (pWriter->pDIter->dIter.iRow < pWriter->pDIter->dIter.bData.nRow) {
        TSDBROW row = tsdbRowFromBlockData(&pWriter->pDIter->dIter.bData, pWriter->pDIter->dIter.iRow);

        int32_t c = tsdbKeyCmprFn(&inKey, &TSDBROW_KEY(&row));
        if (c < 0) {
          goto _write_row;
        } else if (c > 0) {
          code = tsdbSnapWriteTableRowImpl(pWriter, &row);
          TSDB_CHECK_CODE(code, lino, _exit);

          pWriter->pDIter->dIter.iRow++;
        } else {
          ASSERT(0);
        }
      }

      for (;;) {
        if (pWriter->pDIter->dIter.iDataBlk >= pWriter->pDIter->dIter.mDataBlk.nItem) goto _write_row;

        // FIXME: Here can be slow, use array instead
        SDataBlk dataBlk;
        tMapDataGetItemByIdx(&pWriter->pDIter->dIter.mDataBlk, pWriter->pDIter->dIter.iDataBlk, &dataBlk, tGetDataBlk);

        int32_t c = tDataBlkCmprFn(&dataBlk, &(SDataBlk){.minKey = inKey, .maxKey = inKey});
        if (c > 0) {
          goto _write_row;
        } else if (c < 0) {
          if (pWriter->bData.nRow > 0) {
            code = tsdbWriteDataBlock(pWriter->pDataFWriter, &pWriter->bData, &pWriter->mDataBlk, pWriter->cmprAlg);
            TSDB_CHECK_CODE(code, lino, _exit);
          }

          tMapDataPutItem(&pWriter->pDIter->dIter.mDataBlk, &dataBlk, tPutDataBlk);
          pWriter->pDIter->dIter.iDataBlk++;
        } else {
          code = tsdbReadDataBlockEx(pWriter->pDataFReader, &dataBlk, &pWriter->pDIter->dIter.bData);
          TSDB_CHECK_CODE(code, lino, _exit);

          pWriter->pDIter->dIter.iRow = 0;
          pWriter->pDIter->dIter.iDataBlk++;
          break;
        }
      }
    }
  }

_write_row:
  if (pRow) {
    code = tsdbSnapWriteTableRowImpl(pWriter, pRow);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pWriter->pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbSnapWriteTableData(STsdbSnapWriter* pWriter, SRowInfo* pRowInfo) {
  int32_t code = 0;
  int32_t lino = 0;

  // switch to new table if need
  if (pRowInfo == NULL || pRowInfo->uid != pWriter->tbid.uid) {
    if (pWriter->tbid.uid != 0) {
      code = tsdbSnapWriteTableDataEnd(pWriter);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tsdbSnapWriteTableDataStart(pWriter, (TABLEID*)pRowInfo);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // end with a NULL row
  if (pRowInfo) {
    code = tsdbSnapWriteTableRow(pWriter, &pRowInfo->row);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pWriter->pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbSnapWriteTimeSeriesData(STsdbSnapWriter* pWriter, SSnapDataHdr* pHdr) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tDecmprBlockData(pHdr->data, pHdr->size, &pWriter->inData, pWriter->aBuf);
  TSDB_CHECK_CODE(code, lino, _exit);

  ASSERT(pWriter->inData.nRow > 0);

  // switch to new data file if need
  int32_t fid = tsdbKeyFid(pWriter->inData.aTSKEY[0], pWriter->minutes, pWriter->precision);
  if (pWriter->fid != fid) {
    if (pWriter->pDataFWriter) {
      code = tsdbSnapWriteFileDataEnd(pWriter);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tsdbSnapWriteFileDataStart(pWriter, fid);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // loop write each row
  SRowInfo* pRowInfo;
  code = tsdbSnapWriteGetRow(pWriter, &pRowInfo);
  TSDB_CHECK_CODE(code, lino, _exit);
  for (int32_t iRow = 0; iRow < pWriter->inData.nRow; ++iRow) {
    SRowInfo rInfo = {.suid = pWriter->inData.suid,
                      .uid = pWriter->inData.uid ? pWriter->inData.uid : pWriter->inData.aUid[iRow],
                      .row = tsdbRowFromBlockData(&pWriter->inData, iRow)};

    for (;;) {
      if (pRowInfo == NULL) {
        code = tsdbSnapWriteTableData(pWriter, &rInfo);
        TSDB_CHECK_CODE(code, lino, _exit);
        break;
      } else {
        int32_t c = tRowInfoCmprFn(&rInfo, pRowInfo);
        if (c < 0) {
          code = tsdbSnapWriteTableData(pWriter, &rInfo);
          TSDB_CHECK_CODE(code, lino, _exit);
          break;
        } else if (c > 0) {
          code = tsdbSnapWriteTableData(pWriter, pRowInfo);
          TSDB_CHECK_CODE(code, lino, _exit);

          code = tsdbSnapWriteNextRow(pWriter, &pRowInfo);
          TSDB_CHECK_CODE(code, lino, _exit);
        } else {
          ASSERT(0);
        }
      }
    }
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pWriter->pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s done, suid:%" PRId64 " uid:%" PRId64 " nRow:%d", TD_VID(pWriter->pTsdb->pVnode), __func__,
              pWriter->inData.suid, pWriter->inData.uid, pWriter->inData.nRow);
  }
  return code;
}

// SNAP_DATA_DEL
static int32_t tsdbSnapMoveWriteDelData(STsdbSnapWriter* pWriter, TABLEID* pId) {
  int32_t code = 0;

  while (true) {
    if (pWriter->iDelIdx >= taosArrayGetSize(pWriter->aDelIdxR)) break;

    SDelIdx* pDelIdx = (SDelIdx*)taosArrayGet(pWriter->aDelIdxR, pWriter->iDelIdx);

    if (tTABLEIDCmprFn(pDelIdx, pId) >= 0) break;

    code = tsdbReadDelData(pWriter->pDelFReader, pDelIdx, pWriter->aDelData);
    if (code) goto _exit;

    SDelIdx delIdx = *pDelIdx;
    code = tsdbWriteDelData(pWriter->pDelFWriter, pWriter->aDelData, &delIdx);
    if (code) goto _exit;

    if (taosArrayPush(pWriter->aDelIdxW, &delIdx) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }

    pWriter->iDelIdx++;
  }

_exit:
  return code;
}

static int32_t tsdbSnapWriteDelData(STsdbSnapWriter* pWriter, SSnapDataHdr* pHdr) {
  int32_t code = 0;
  STsdb*  pTsdb = pWriter->pTsdb;

  // Open del file if not opened yet
  if (pWriter->pDelFWriter == NULL) {
    SDelFile* pDelFile = pWriter->fs.pDelFile;

    // reader
    if (pDelFile) {
      code = tsdbDelFReaderOpen(&pWriter->pDelFReader, pDelFile, pTsdb);
      if (code) goto _err;

      code = tsdbReadDelIdx(pWriter->pDelFReader, pWriter->aDelIdxR);
      if (code) goto _err;
    } else {
      taosArrayClear(pWriter->aDelIdxR);
    }
    pWriter->iDelIdx = 0;

    // writer
    SDelFile delFile = {.commitID = pWriter->commitID};
    code = tsdbDelFWriterOpen(&pWriter->pDelFWriter, &delFile, pTsdb);
    if (code) goto _err;
    taosArrayClear(pWriter->aDelIdxW);
  }

  TABLEID id = *(TABLEID*)pHdr->data;

  // Move write data < id
  code = tsdbSnapMoveWriteDelData(pWriter, &id);
  if (code) goto _err;

  // Merge incoming data with current
  if (pWriter->iDelIdx < taosArrayGetSize(pWriter->aDelIdxR) &&
      tTABLEIDCmprFn(taosArrayGet(pWriter->aDelIdxR, pWriter->iDelIdx), &id) == 0) {
    SDelIdx* pDelIdx = (SDelIdx*)taosArrayGet(pWriter->aDelIdxR, pWriter->iDelIdx);

    code = tsdbReadDelData(pWriter->pDelFReader, pDelIdx, pWriter->aDelData);
    if (code) goto _err;

    pWriter->iDelIdx++;
  } else {
    taosArrayClear(pWriter->aDelData);
  }

  int64_t n = sizeof(TABLEID);
  while (n < pHdr->size) {
    SDelData delData;

    n += tGetDelData(pHdr->data + n, &delData);

    if (taosArrayPush(pWriter->aDelData, &delData) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
  }

  SDelIdx delIdx = {.suid = id.suid, .uid = id.uid};
  code = tsdbWriteDelData(pWriter->pDelFWriter, pWriter->aDelData, &delIdx);
  if (code) goto _err;

  if (taosArrayPush(pWriter->aDelIdxW, &delIdx) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  return code;

_err:
  tsdbError("vgId:%d, vnode snapshot tsdb write del for %s failed since %s", TD_VID(pTsdb->pVnode), pTsdb->path,
            tstrerror(code));
  return code;
}

static int32_t tsdbSnapWriteDelEnd(STsdbSnapWriter* pWriter) {
  int32_t code = 0;
  STsdb*  pTsdb = pWriter->pTsdb;

  if (pWriter->pDelFWriter == NULL) return code;

  TABLEID id = {.suid = INT64_MAX, .uid = INT64_MAX};
  code = tsdbSnapMoveWriteDelData(pWriter, &id);
  if (code) goto _err;

  code = tsdbWriteDelIdx(pWriter->pDelFWriter, pWriter->aDelIdxW);
  if (code) goto _err;

  code = tsdbUpdateDelFileHdr(pWriter->pDelFWriter);
  if (code) goto _err;

  code = tsdbFSUpsertDelFile(&pWriter->fs, &pWriter->pDelFWriter->fDel);
  if (code) goto _err;

  code = tsdbDelFWriterClose(&pWriter->pDelFWriter, 1);
  if (code) goto _err;

  if (pWriter->pDelFReader) {
    code = tsdbDelFReaderClose(&pWriter->pDelFReader);
    if (code) goto _err;
  }

  tsdbInfo("vgId:%d, vnode snapshot tsdb write del for %s end", TD_VID(pTsdb->pVnode), pTsdb->path);
  return code;

_err:
  tsdbError("vgId:%d, vnode snapshot tsdb write del end for %s failed since %s", TD_VID(pTsdb->pVnode), pTsdb->path,
            tstrerror(code));
  return code;
}

// APIs
int32_t tsdbSnapWriterOpen(STsdb* pTsdb, int64_t sver, int64_t ever, STsdbSnapWriter** ppWriter) {
  int32_t code = 0;
  int32_t lino = 0;

  // alloc
  STsdbSnapWriter* pWriter = (STsdbSnapWriter*)taosMemoryCalloc(1, sizeof(*pWriter));
  if (pWriter == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  pWriter->pTsdb = pTsdb;
  pWriter->sver = sver;
  pWriter->ever = ever;
  pWriter->minutes = pTsdb->keepCfg.days;
  pWriter->precision = pTsdb->keepCfg.precision;
  pWriter->minRow = pTsdb->pVnode->config.tsdbCfg.minRows;
  pWriter->maxRow = pTsdb->pVnode->config.tsdbCfg.maxRows;
  pWriter->cmprAlg = pTsdb->pVnode->config.tsdbCfg.compression;
  pWriter->commitID = pTsdb->pVnode->state.commitID;

  code = tsdbFSCopy(pTsdb, &pWriter->fs);
  TSDB_CHECK_CODE(code, lino, _exit);

  // SNAP_DATA_TSDB
#if 1
  pWriter->fid = INT32_MIN;

  code = tBlockDataCreate(&pWriter->inData);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tBlockDataCreate(&pWriter->bData);
  TSDB_CHECK_CODE(code, lino, _exit);
#else
  code = tBlockDataCreate(&pWriter->bData);
  TSDB_CHECK_CODE(code, lino, _exit);

  pWriter->fid = INT32_MIN;
  pWriter->id = (TABLEID){0};
  // Reader
  pWriter->dReader.aBlockIdx = taosArrayInit(0, sizeof(SBlockIdx));
  if (pWriter->dReader.aBlockIdx == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  code = tBlockDataCreate(&pWriter->dReader.bData);
  TSDB_CHECK_CODE(code, lino, _exit);

  // Writer
  pWriter->dWriter.aBlockIdx = taosArrayInit(0, sizeof(SBlockIdx));
  if (pWriter->dWriter.aBlockIdx == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  pWriter->dWriter.aSttBlk = taosArrayInit(0, sizeof(SSttBlk));
  if (pWriter->dWriter.aSttBlk == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  code = tBlockDataCreate(&pWriter->dWriter.bData);
  TSDB_CHECK_CODE(code, lino, _exit);
  code = tBlockDataCreate(&pWriter->dWriter.sData);
  TSDB_CHECK_CODE(code, lino, _exit);
#endif

  // SNAP_DATA_DEL
  pWriter->aDelIdxR = taosArrayInit(0, sizeof(SDelIdx));
  if (pWriter->aDelIdxR == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  pWriter->aDelData = taosArrayInit(0, sizeof(SDelData));
  if (pWriter->aDelData == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  pWriter->aDelIdxW = taosArrayInit(0, sizeof(SDelIdx));
  if (pWriter->aDelIdxW == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
    *ppWriter = NULL;

    if (pWriter) {
#if 0
      if (pWriter->aDelIdxW) taosArrayDestroy(pWriter->aDelIdxW);
      if (pWriter->aDelData) taosArrayDestroy(pWriter->aDelData);
      if (pWriter->aDelIdxR) taosArrayDestroy(pWriter->aDelIdxR);
      tBlockDataDestroy(&pWriter->dWriter.sData, 1);
      tBlockDataDestroy(&pWriter->dWriter.bData, 1);
      if (pWriter->dWriter.aSttBlk) taosArrayDestroy(pWriter->dWriter.aSttBlk);
      if (pWriter->dWriter.aBlockIdx) taosArrayDestroy(pWriter->dWriter.aBlockIdx);
      tBlockDataDestroy(&pWriter->dReader.bData, 1);
      if (pWriter->dReader.aBlockIdx) taosArrayDestroy(pWriter->dReader.aBlockIdx);
      tBlockDataDestroy(&pWriter->bData, 1);
      tsdbFSDestroy(&pWriter->fs);
      taosMemoryFree(pWriter);
#endif
    }
  } else {
    tsdbInfo("vgId:%d, %s done", TD_VID(pTsdb->pVnode), __func__);
    *ppWriter = pWriter;
  }
  return code;
}

int32_t tsdbSnapWriterPrepareClose(STsdbSnapWriter* pWriter) {
  int32_t code = 0;
  if (pWriter->pDataFWriter) {
    code = tsdbSnapWriteFileDataEnd(pWriter);
    if (code) goto _exit;
  }

  code = tsdbSnapWriteDelEnd(pWriter);
  if (code) goto _exit;

  code = tsdbFSPrepareCommit(pWriter->pTsdb, &pWriter->fs);
  if (code) goto _exit;

_exit:
  if (code) {
    tsdbError("vgId:%d, %s failed since %s", TD_VID(pWriter->pTsdb->pVnode), __func__, tstrerror(code));
  }
  return code;
}

int32_t tsdbSnapWriterClose(STsdbSnapWriter** ppWriter, int8_t rollback) {
  int32_t          code = 0;
  STsdbSnapWriter* pWriter = *ppWriter;
  STsdb*           pTsdb = pWriter->pTsdb;

  if (rollback) {
    tsdbRollbackCommit(pWriter->pTsdb);
  } else {
    // lock
    taosThreadRwlockWrlock(&pTsdb->rwLock);

    code = tsdbFSCommit(pWriter->pTsdb);
    if (code) {
      taosThreadRwlockUnlock(&pTsdb->rwLock);
      goto _err;
    }

    // unlock
    taosThreadRwlockUnlock(&pTsdb->rwLock);
  }

  // SNAP_DATA_DEL
  taosArrayDestroy(pWriter->aDelIdxW);
  taosArrayDestroy(pWriter->aDelData);
  taosArrayDestroy(pWriter->aDelIdxR);

  // SNAP_DATA_TSDB

  // // Writer
  // tBlockDataDestroy(&pWriter->dWriter.sData, 1);
  // tBlockDataDestroy(&pWriter->dWriter.bData, 1);
  // taosArrayDestroy(pWriter->dWriter.aSttBlk);
  // tMapDataClear(&pWriter->dWriter.mDataBlk);
  // taosArrayDestroy(pWriter->dWriter.aBlockIdx);

  // // Reader
  // tBlockDataDestroy(&pWriter->dReader.bData, 1);
  // tMapDataClear(&pWriter->dReader.mDataBlk);
  // taosArrayDestroy(pWriter->dReader.aBlockIdx);

  tBlockDataDestroy(&pWriter->bData, 1);
  tDestroyTSchema(pWriter->skmTable.pTSchema);

  for (int32_t iBuf = 0; iBuf < sizeof(pWriter->aBuf) / sizeof(uint8_t*); iBuf++) {
    tFree(pWriter->aBuf[iBuf]);
  }
  tsdbInfo("vgId:%d, %s done", TD_VID(pWriter->pTsdb->pVnode), __func__);
  taosMemoryFree(pWriter);
  *ppWriter = NULL;
  return code;

_err:
  tsdbError("vgId:%d, vnode snapshot tsdb writer close for %s failed since %s", TD_VID(pWriter->pTsdb->pVnode),
            pWriter->pTsdb->path, tstrerror(code));
  taosMemoryFree(pWriter);
  *ppWriter = NULL;
  return code;
}

int32_t tsdbSnapWrite(STsdbSnapWriter* pWriter, SSnapDataHdr* pHdr) {
  int32_t code = 0;
  int32_t lino = 0;

  if (pHdr->type == SNAP_DATA_TSDB) {
    code = tsdbSnapWriteTimeSeriesData(pWriter, pHdr);
    TSDB_CHECK_CODE(code, lino, _exit);
    goto _exit;
  } else if (pWriter->pDataFWriter) {
    code = tsdbSnapWriteFileDataEnd(pWriter);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (pHdr->type == SNAP_DATA_DEL) {
    code = tsdbSnapWriteDelData(pWriter, pHdr);
    TSDB_CHECK_CODE(code, lino, _exit);
    goto _exit;
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s, type:%d index:%" PRId64 " size:%" PRId64,
              TD_VID(pWriter->pTsdb->pVnode), __func__, lino, tstrerror(code), pHdr->type, pHdr->index, pHdr->size);
  } else {
    tsdbDebug("vgId:%d %s done, type:%d index:%" PRId64 " size:%" PRId64, TD_VID(pWriter->pTsdb->pVnode), __func__,
              pHdr->type, pHdr->index, pHdr->size);
  }
  return code;
}
