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

extern int32_t tRowInfoCmprFn(const void* p1, const void* p2);
extern int32_t tsdbReadDataBlockEx(SDataFReader* pReader, SDataBlk* pDataBlk, SBlockData* pBlockData);
extern int32_t tsdbUpdateTableSchema(SMeta* pMeta, int64_t suid, int64_t uid, SSkmInfo* pSkmInfo);

static int32_t tFDataIterCmprFn(const SRBTreeNode* pNode1, const SRBTreeNode* pNode2) {
  SFDataIter* pIter1 = (SFDataIter*)(((uint8_t*)pNode1) - offsetof(SFDataIter, n));
  SFDataIter* pIter2 = (SFDataIter*)(((uint8_t*)pNode2) - offsetof(SFDataIter, n));

  return tRowInfoCmprFn(&pIter1->rInfo, &pIter2->rInfo);
}

static int32_t tsdbSnapReadOpenFile(STsdbSnapReader* pReader) {
  int32_t code = 0;

  SDFileSet  dFileSet = {.fid = pReader->fid};
  SDFileSet* pSet = taosArraySearch(pReader->fs.aDFileSet, &dFileSet, tDFileSetCmprFn, TD_GT);
  if (pSet == NULL) return code;

  pReader->fid = pSet->fid;
  code = tsdbDataFReaderOpen(&pReader->pDataFReader, pReader->pTsdb, pSet);
  if (code) goto _err;

  pReader->pIter = NULL;
  tRBTreeCreate(&pReader->rbt, tFDataIterCmprFn);

  // .data file
  SFDataIter* pIter = &pReader->aFDataIter[0];
  pIter->type = SNAP_DATA_FILE_ITER;

  code = tsdbReadBlockIdx(pReader->pDataFReader, pIter->aBlockIdx);
  if (code) goto _err;

  for (pIter->iBlockIdx = 0; pIter->iBlockIdx < taosArrayGetSize(pIter->aBlockIdx); pIter->iBlockIdx++) {
    pIter->pBlockIdx = (SBlockIdx*)taosArrayGet(pIter->aBlockIdx, pIter->iBlockIdx);

    code = tsdbReadDataBlk(pReader->pDataFReader, pIter->pBlockIdx, &pIter->mBlock);
    if (code) goto _err;

    for (pIter->iBlock = 0; pIter->iBlock < pIter->mBlock.nItem; pIter->iBlock++) {
      SDataBlk dataBlk;
      tMapDataGetItemByIdx(&pIter->mBlock, pIter->iBlock, &dataBlk, tGetDataBlk);

      if (dataBlk.minVer > pReader->ever || dataBlk.maxVer < pReader->sver) continue;

      code = tsdbReadDataBlockEx(pReader->pDataFReader, &dataBlk, &pIter->bData);
      if (code) goto _err;

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
    if (code) goto _err;

    for (pIter->iSttBlk = 0; pIter->iSttBlk < taosArrayGetSize(pIter->aSttBlk); pIter->iSttBlk++) {
      SSttBlk* pSttBlk = (SSttBlk*)taosArrayGet(pIter->aSttBlk, pIter->iSttBlk);

      if (pSttBlk->minVer > pReader->ever) continue;
      if (pSttBlk->maxVer < pReader->sver) continue;

      code = tsdbReadSttBlockEx(pReader->pDataFReader, iStt, pSttBlk, &pIter->bData);
      if (code) goto _err;

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

  tsdbInfo("vgId:%d, vnode snapshot tsdb open data file to read for %s, fid:%d", TD_VID(pReader->pTsdb->pVnode),
           pReader->pTsdb->path, pReader->fid);
  return code;

_err:
  tsdbError("vgId:%d, vnode snapshot tsdb snap read open file failed since %s", TD_VID(pReader->pTsdb->pVnode),
            tstrerror(code));
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
  STsdb*  pTsdb = pReader->pTsdb;

  while (true) {
    if (pReader->pDataFReader == NULL) {
      code = tsdbSnapReadOpenFile(pReader);
      if (code) goto _err;
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
    if (code) goto _err;

    code = tBlockDataInit(pBlockData, &id, pReader->skmTable.pTSchema, NULL, 0);
    if (code) goto _err;

    while (pRowInfo->suid == id.suid && pRowInfo->uid == id.uid) {
      code = tBlockDataAppendRow(pBlockData, &pRowInfo->row, NULL, pRowInfo->uid);
      if (code) goto _err;

      code = tsdbSnapNextRow(pReader);
      if (code) goto _err;

      pRowInfo = tsdbSnapGetRow(pReader);
      if (pRowInfo == NULL) {
        tsdbDataFReaderClose(&pReader->pDataFReader);
        break;
      }

      if (pBlockData->nRow >= 4096) break;
    }

    code = tsdbSnapCmprData(pReader, ppData);
    if (code) goto _err;

    break;
  }

  return code;

_err:
  tsdbError("vgId:%d, vnode snapshot tsdb read data for %s failed since %s", TD_VID(pTsdb->pVnode), pTsdb->path,
            tstrerror(code));
  return code;
}

static int32_t tsdbSnapReadDel(STsdbSnapReader* pReader, uint8_t** ppData) {
  int32_t   code = 0;
  STsdb*    pTsdb = pReader->pTsdb;
  SDelFile* pDelFile = pReader->fs.pDelFile;

  if (pReader->pDelFReader == NULL) {
    if (pDelFile == NULL) {
      goto _exit;
    }

    // open
    code = tsdbDelFReaderOpen(&pReader->pDelFReader, pDelFile, pTsdb);
    if (code) goto _err;

    // read index
    code = tsdbReadDelIdx(pReader->pDelFReader, pReader->aDelIdx);
    if (code) goto _err;

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
    if (code) goto _err;

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
      goto _err;
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
  return code;

_err:
  tsdbError("vgId:%d, vnode snapshot tsdb read del for %s failed since %s", TD_VID(pTsdb->pVnode), pTsdb->path,
            tstrerror(code));
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
      tBlockDataDestroy(&pReader->bData);
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

    tBlockDataDestroy(&pIter->bData);
  }

  tBlockDataDestroy(&pReader->bData);
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

  *ppData = NULL;

  // read data file
  if (!pReader->dataDone) {
    code = tsdbSnapReadData(pReader, ppData);
    if (code) {
      goto _err;
    } else {
      if (*ppData) {
        goto _exit;
      } else {
        pReader->dataDone = 1;
      }
    }
  }

  // read del file
  if (!pReader->delDone) {
    code = tsdbSnapReadDel(pReader, ppData);
    if (code) {
      goto _err;
    } else {
      if (*ppData) {
        goto _exit;
      } else {
        pReader->delDone = 1;
      }
    }
  }

_exit:
  tsdbDebug("vgId:%d, vnode snapshot tsdb read for %s", TD_VID(pReader->pTsdb->pVnode), pReader->pTsdb->path);
  return code;

_err:
  tsdbError("vgId:%d, vnode snapshot tsdb read for %s failed since %s", TD_VID(pReader->pTsdb->pVnode),
            pReader->pTsdb->path, tstrerror(code));
  return code;
}

// STsdbSnapWriter ========================================
struct STsdbSnapWriter {
  STsdb*  pTsdb;
  int64_t sver;
  int64_t ever;
  STsdbFS fs;

  // config
  int32_t  minutes;
  int8_t   precision;
  int32_t  minRow;
  int32_t  maxRow;
  int8_t   cmprAlg;
  int64_t  commitID;
  uint8_t* aBuf[5];

  // for data file
  SBlockData bData;
  int32_t    fid;
  TABLEID    id;
  SSkmInfo   skmTable;
  struct {
    SDataFReader* pReader;
    SArray*       aBlockIdx;
    int32_t       iBlockIdx;
    SBlockIdx*    pBlockIdx;
    SMapData      mDataBlk;
    int32_t       iDataBlk;
    SBlockData    bData;
    int32_t       iRow;
  } dReader;
  struct {
    SDataFWriter* pWriter;
    SArray*       aBlockIdx;
    SMapData      mDataBlk;
    SArray*       aSttBlk;
    SBlockData    bData;
    SBlockData    sData;
  } dWriter;

  // for del file
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

_exit:
  return code;
}

static int32_t tsdbSnapWriteCopyData(STsdbSnapWriter* pWriter, TABLEID* pId) {
  int32_t code = 0;

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

_exit:
  return code;
}

static int32_t tsdbSnapWriteTableDataStart(STsdbSnapWriter* pWriter, TABLEID* pId) {
  int32_t code = 0;

  code = tsdbSnapWriteCopyData(pWriter, pId);
  if (code) goto _err;

  pWriter->id.suid = pId->suid;
  pWriter->id.uid = pId->uid;

  code = tsdbUpdateTableSchema(pWriter->pTsdb->pVnode->pMeta, pId->suid, pId->uid, &pWriter->skmTable);
  if (code) goto _err;

  tMapDataReset(&pWriter->dWriter.mDataBlk);
  code = tBlockDataInit(&pWriter->dWriter.bData, pId, pWriter->skmTable.pTSchema, NULL, 0);
  if (code) goto _err;

  return code;

_err:
  tsdbError("vgId:%d, %s failed since %s", TD_VID(pWriter->pTsdb->pVnode), __func__, tstrerror(code));
  return code;
}

static int32_t tsdbSnapWriteTableDataEnd(STsdbSnapWriter* pWriter) {
  int32_t code = 0;

  if (pWriter->id.suid == 0 && pWriter->id.uid == 0) return code;

  int32_t c = 1;
  if (pWriter->dReader.pBlockIdx) {
    c = tTABLEIDCmprFn(pWriter->dReader.pBlockIdx, &pWriter->id);
    ASSERT(c >= 0);
  }

  if (c == 0) {
    SBlockData* pBData = &pWriter->dWriter.bData;

    for (; pWriter->dReader.iRow < pWriter->dReader.bData.nRow; pWriter->dReader.iRow++) {
      TSDBROW row = tsdbRowFromBlockData(&pWriter->dReader.bData, pWriter->dReader.iRow);

      code = tBlockDataAppendRow(pBData, &row, NULL, pWriter->id.uid);
      if (code) goto _err;

      if (pBData->nRow >= pWriter->maxRow) {
        code = tsdbWriteDataBlock(pWriter->dWriter.pWriter, pBData, &pWriter->dWriter.mDataBlk, pWriter->cmprAlg);
        if (code) goto _err;
      }
    }

    code = tsdbWriteDataBlock(pWriter->dWriter.pWriter, pBData, &pWriter->dWriter.mDataBlk, pWriter->cmprAlg);
    if (code) goto _err;

    for (; pWriter->dReader.iDataBlk < pWriter->dReader.mDataBlk.nItem; pWriter->dReader.iDataBlk++) {
      SDataBlk dataBlk;
      tMapDataGetItemByIdx(&pWriter->dReader.mDataBlk, pWriter->dReader.iDataBlk, &dataBlk, tGetDataBlk);

      code = tMapDataPutItem(&pWriter->dWriter.mDataBlk, &dataBlk, tPutDataBlk);
      if (code) goto _err;
    }

    code = tsdbSnapNextTableData(pWriter);
    if (code) goto _err;
  }

  if (pWriter->dWriter.mDataBlk.nItem) {
    SBlockIdx blockIdx = {.suid = pWriter->id.suid, .uid = pWriter->id.uid};
    code = tsdbWriteDataBlk(pWriter->dWriter.pWriter, &pWriter->dWriter.mDataBlk, &blockIdx);

    if (taosArrayPush(pWriter->dWriter.aBlockIdx, &blockIdx) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
  }

  pWriter->id.suid = 0;
  pWriter->id.uid = 0;

  return code;

_err:
  return code;
}

static int32_t tsdbSnapWriteOpenFile(STsdbSnapWriter* pWriter, int32_t fid) {
  int32_t code = 0;
  STsdb*  pTsdb = pWriter->pTsdb;

  ASSERT(pWriter->dWriter.pWriter == NULL);

  pWriter->fid = fid;
  pWriter->id = (TABLEID){0};
  SDFileSet* pSet = taosArraySearch(pWriter->fs.aDFileSet, &(SDFileSet){.fid = fid}, tDFileSetCmprFn, TD_EQ);

  // Reader
  if (pSet) {
    code = tsdbDataFReaderOpen(&pWriter->dReader.pReader, pWriter->pTsdb, pSet);
    if (code) goto _err;

    code = tsdbReadBlockIdx(pWriter->dReader.pReader, pWriter->dReader.aBlockIdx);
    if (code) goto _err;
  } else {
    ASSERT(pWriter->dReader.pReader == NULL);
    taosArrayClear(pWriter->dReader.aBlockIdx);
  }
  pWriter->dReader.iBlockIdx = 0;  // point to the next one
  code = tsdbSnapNextTableData(pWriter);
  if (code) goto _err;

  // Writer
  SHeadFile fHead = {.commitID = pWriter->commitID};
  SDataFile fData = {.commitID = pWriter->commitID};
  SSmaFile  fSma = {.commitID = pWriter->commitID};
  SSttFile  fStt = {.commitID = pWriter->commitID};
  SDFileSet wSet = {.fid = pWriter->fid, .pHeadF = &fHead, .pDataF = &fData, .pSmaF = &fSma};
  if (pSet) {
    wSet.diskId = pSet->diskId;
    fData = *pSet->pDataF;
    fSma = *pSet->pSmaF;
    for (int32_t iStt = 0; iStt < pSet->nSttF; iStt++) {
      wSet.aSttF[iStt] = pSet->aSttF[iStt];
    }
    wSet.nSttF = pSet->nSttF + 1;  // TODO: fix pSet->nSttF == pTsdb->maxFile
  } else {
    SDiskID did = {0};
    tfsAllocDisk(pTsdb->pVnode->pTfs, 0, &did);
    tfsMkdirRecurAt(pTsdb->pVnode->pTfs, pTsdb->path, did);
    wSet.diskId = did;
    wSet.nSttF = 1;
  }
  wSet.aSttF[wSet.nSttF - 1] = &fStt;

  code = tsdbDataFWriterOpen(&pWriter->dWriter.pWriter, pWriter->pTsdb, &wSet);
  if (code) goto _err;
  taosArrayClear(pWriter->dWriter.aBlockIdx);
  tMapDataReset(&pWriter->dWriter.mDataBlk);
  taosArrayClear(pWriter->dWriter.aSttBlk);
  tBlockDataReset(&pWriter->dWriter.bData);
  tBlockDataReset(&pWriter->dWriter.sData);

  return code;

_err:
  return code;
}

static int32_t tsdbSnapWriteCloseFile(STsdbSnapWriter* pWriter) {
  int32_t code = 0;

  ASSERT(pWriter->dWriter.pWriter);

  code = tsdbSnapWriteTableDataEnd(pWriter);
  if (code) goto _err;

  // copy remain table data
  TABLEID id = {.suid = INT64_MAX, .uid = INT64_MAX};
  code = tsdbSnapWriteCopyData(pWriter, &id);
  if (code) goto _err;

  code =
      tsdbWriteSttBlock(pWriter->dWriter.pWriter, &pWriter->dWriter.sData, pWriter->dWriter.aSttBlk, pWriter->cmprAlg);
  if (code) goto _err;

  // Indices
  code = tsdbWriteBlockIdx(pWriter->dWriter.pWriter, pWriter->dWriter.aBlockIdx);
  if (code) goto _err;

  code = tsdbWriteSttBlk(pWriter->dWriter.pWriter, pWriter->dWriter.aSttBlk);
  if (code) goto _err;

  code = tsdbUpdateDFileSetHeader(pWriter->dWriter.pWriter);
  if (code) goto _err;

  code = tsdbFSUpsertFSet(&pWriter->fs, &pWriter->dWriter.pWriter->wSet);
  if (code) goto _err;

  code = tsdbDataFWriterClose(&pWriter->dWriter.pWriter, 1);
  if (code) goto _err;

  if (pWriter->dReader.pReader) {
    code = tsdbDataFReaderClose(&pWriter->dReader.pReader);
    if (code) goto _err;
  }

_exit:
  return code;

_err:
  return code;
}

static int32_t tsdbSnapWriteToDataFile(STsdbSnapWriter* pWriter, int32_t iRow, int8_t* done) {
  int32_t code = 0;

  SBlockData* pBData = &pWriter->bData;
  TABLEID     id = {.suid = pBData->suid, .uid = pBData->uid ? pBData->uid : pBData->aUid[iRow]};
  TSDBROW     row = tsdbRowFromBlockData(pBData, iRow);
  TSDBKEY     key = TSDBROW_KEY(&row);

  *done = 0;
  while (pWriter->dReader.iRow < pWriter->dReader.bData.nRow ||
         pWriter->dReader.iDataBlk < pWriter->dReader.mDataBlk.nItem) {
    // Merge row by row
    for (; pWriter->dReader.iRow < pWriter->dReader.bData.nRow; pWriter->dReader.iRow++) {
      TSDBROW trow = tsdbRowFromBlockData(&pWriter->dReader.bData, pWriter->dReader.iRow);
      TSDBKEY tKey = TSDBROW_KEY(&trow);

      ASSERT(pWriter->dReader.bData.suid == id.suid && pWriter->dReader.bData.uid == id.uid);

      int32_t c = tsdbKeyCmprFn(&key, &tKey);
      if (c < 0) {
        code = tBlockDataAppendRow(&pWriter->dWriter.bData, &row, NULL, id.uid);
        if (code) goto _err;
      } else if (c > 0) {
        code = tBlockDataAppendRow(&pWriter->dWriter.bData, &trow, NULL, id.uid);
        if (code) goto _err;
      } else {
        ASSERT(0);
      }

      if (pWriter->dWriter.bData.nRow >= pWriter->maxRow) {
        code = tsdbWriteDataBlock(pWriter->dWriter.pWriter, &pWriter->dWriter.bData, &pWriter->dWriter.mDataBlk,
                                  pWriter->cmprAlg);
        if (code) goto _err;
      }

      if (c < 0) {
        *done = 1;
        goto _exit;
      }
    }

    // Merge row by block
    SDataBlk tDataBlk = {.minKey = key, .maxKey = key};
    for (; pWriter->dReader.iDataBlk < pWriter->dReader.mDataBlk.nItem; pWriter->dReader.iDataBlk++) {
      SDataBlk dataBlk;
      tMapDataGetItemByIdx(&pWriter->dReader.mDataBlk, pWriter->dReader.iDataBlk, &dataBlk, tGetDataBlk);

      int32_t c = tDataBlkCmprFn(&dataBlk, &tDataBlk);
      if (c < 0) {
        code = tsdbWriteDataBlock(pWriter->dWriter.pWriter, &pWriter->dWriter.bData, &pWriter->dWriter.mDataBlk,
                                  pWriter->cmprAlg);
        if (code) goto _err;

        code = tMapDataPutItem(&pWriter->dWriter.mDataBlk, &dataBlk, tPutDataBlk);
        if (code) goto _err;
      } else if (c > 0) {
        code = tBlockDataAppendRow(&pWriter->dWriter.bData, &row, NULL, id.uid);
        if (code) goto _err;

        if (pWriter->dWriter.bData.nRow >= pWriter->maxRow) {
          code = tsdbWriteDataBlock(pWriter->dWriter.pWriter, &pWriter->dWriter.bData, &pWriter->dWriter.mDataBlk,
                                    pWriter->cmprAlg);
          if (code) goto _err;
        }

        *done = 1;
        goto _exit;
      } else {
        code = tsdbReadDataBlockEx(pWriter->dReader.pReader, &dataBlk, &pWriter->dReader.bData);
        if (code) goto _err;
        pWriter->dReader.iRow = 0;

        pWriter->dReader.iDataBlk++;
        break;
      }
    }
  }

_exit:
  return code;

_err:
  tsdbError("vgId:%d, %s failed since %s", TD_VID(pWriter->pTsdb->pVnode), __func__, tstrerror(code));
  return code;
}

static int32_t tsdbSnapWriteToSttFile(STsdbSnapWriter* pWriter, int32_t iRow) {
  int32_t code = 0;

  TABLEID     id = {.suid = pWriter->bData.suid,
                    .uid = pWriter->bData.uid ? pWriter->bData.uid : pWriter->bData.aUid[iRow]};
  TSDBROW     row = tsdbRowFromBlockData(&pWriter->bData, iRow);
  SBlockData* pBData = &pWriter->dWriter.sData;

  if (pBData->suid || pBData->uid) {
    if (!TABLE_SAME_SCHEMA(pBData->suid, pBData->uid, id.suid, id.uid)) {
      code = tsdbWriteSttBlock(pWriter->dWriter.pWriter, pBData, pWriter->dWriter.aSttBlk, pWriter->cmprAlg);
      if (code) goto _err;

      pBData->suid = 0;
      pBData->uid = 0;
    }
  }

  if (pBData->suid == 0 && pBData->uid == 0) {
    code = tsdbUpdateTableSchema(pWriter->pTsdb->pVnode->pMeta, pWriter->id.suid, pWriter->id.uid, &pWriter->skmTable);
    if (code) goto _err;

    TABLEID tid = {.suid = pWriter->id.suid, .uid = pWriter->id.suid ? 0 : pWriter->id.uid};
    code = tBlockDataInit(pBData, &tid, pWriter->skmTable.pTSchema, NULL, 0);
    if (code) goto _err;
  }

  code = tBlockDataAppendRow(pBData, &row, NULL, id.uid);
  if (code) goto _err;

  if (pBData->nRow >= pWriter->maxRow) {
    code = tsdbWriteSttBlock(pWriter->dWriter.pWriter, pBData, pWriter->dWriter.aSttBlk, pWriter->cmprAlg);
    if (code) goto _err;
  }

_exit:
  return code;

_err:
  return code;
}

static int32_t tsdbSnapWriteRowData(STsdbSnapWriter* pWriter, int32_t iRow) {
  int32_t code = 0;

  SBlockData* pBlockData = &pWriter->bData;
  TABLEID     id = {.suid = pBlockData->suid, .uid = pBlockData->uid ? pBlockData->uid : pBlockData->aUid[iRow]};

  // End last table data write if need
  if (tTABLEIDCmprFn(&pWriter->id, &id) != 0) {
    code = tsdbSnapWriteTableDataEnd(pWriter);
    if (code) goto _err;
  }

  // Start new table data write if need
  if (pWriter->id.suid == 0 && pWriter->id.uid == 0) {
    code = tsdbSnapWriteTableDataStart(pWriter, &id);
    if (code) goto _err;
  }

  // Merge with .data file data
  int8_t done = 0;
  if (pWriter->dReader.pBlockIdx && tTABLEIDCmprFn(pWriter->dReader.pBlockIdx, &id) == 0) {
    code = tsdbSnapWriteToDataFile(pWriter, iRow, &done);
    if (code) goto _err;
  }

  // Append to the .stt data block (todo: check if need to set/reload sst block)
  if (!done) {
    code = tsdbSnapWriteToSttFile(pWriter, iRow);
    if (code) goto _err;
  }

_exit:
  return code;

_err:
  tsdbError("vgId:%d, %s failed since %s", TD_VID(pWriter->pTsdb->pVnode), __func__, tstrerror(code));
  return code;
}

static int32_t tsdbSnapWriteData(STsdbSnapWriter* pWriter, uint8_t* pData, uint32_t nData) {
  int32_t     code = 0;
  STsdb*      pTsdb = pWriter->pTsdb;
  SBlockData* pBlockData = &pWriter->bData;

  // Decode data
  SSnapDataHdr* pHdr = (SSnapDataHdr*)pData;
  code = tDecmprBlockData(pHdr->data, pHdr->size, pBlockData, pWriter->aBuf);
  if (code) goto _err;

  ASSERT(pBlockData->nRow > 0);

  // Loop to handle each row
  for (int32_t iRow = 0; iRow < pBlockData->nRow; iRow++) {
    TSKEY   ts = pBlockData->aTSKEY[iRow];
    int32_t fid = tsdbKeyFid(ts, pWriter->minutes, pWriter->precision);

    if (pWriter->dWriter.pWriter == NULL || pWriter->fid != fid) {
      if (pWriter->dWriter.pWriter) {
        // ASSERT(fid > pWriter->fid);

        code = tsdbSnapWriteCloseFile(pWriter);
        if (code) goto _err;
      }

      code = tsdbSnapWriteOpenFile(pWriter, fid);
      if (code) goto _err;
    }

    code = tsdbSnapWriteRowData(pWriter, iRow);
    if (code) goto _err;
  }

  return code;

_err:
  tsdbError("vgId:%d, vnode snapshot tsdb write data for %s failed since %s", TD_VID(pTsdb->pVnode), pTsdb->path,
            tstrerror(code));
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

static int32_t tsdbSnapWriteDel(STsdbSnapWriter* pWriter, uint8_t* pData, uint32_t nData) {
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

  SSnapDataHdr* pHdr = (SSnapDataHdr*)pData;
  TABLEID       id = *(TABLEID*)pHdr->data;

  ASSERT(pHdr->size + sizeof(SSnapDataHdr) == nData);

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

  int64_t n = sizeof(SSnapDataHdr) + sizeof(TABLEID);
  while (n < nData) {
    SDelData delData;

    n += tGetDelData(pData + n, &delData);

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
  int32_t          code = 0;
  int32_t          lino = 0;
  STsdbSnapWriter* pWriter = NULL;

  // alloc
  pWriter = (STsdbSnapWriter*)taosMemoryCalloc(1, sizeof(*pWriter));
  if (pWriter == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  pWriter->pTsdb = pTsdb;
  pWriter->sver = sver;
  pWriter->ever = ever;

  code = tsdbFSCopy(pTsdb, &pWriter->fs);
  TSDB_CHECK_CODE(code, lino, _exit);

  // config
  pWriter->minutes = pTsdb->keepCfg.days;
  pWriter->precision = pTsdb->keepCfg.precision;
  pWriter->minRow = pTsdb->pVnode->config.tsdbCfg.minRows;
  pWriter->maxRow = pTsdb->pVnode->config.tsdbCfg.maxRows;
  pWriter->cmprAlg = pTsdb->pVnode->config.tsdbCfg.compression;
  pWriter->commitID = pTsdb->pVnode->state.commitID;

  // SNAP_DATA_TSDB
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
      if (pWriter->aDelIdxW) taosArrayDestroy(pWriter->aDelIdxW);
      if (pWriter->aDelData) taosArrayDestroy(pWriter->aDelData);
      if (pWriter->aDelIdxR) taosArrayDestroy(pWriter->aDelIdxR);
      tBlockDataDestroy(&pWriter->dWriter.sData);
      tBlockDataDestroy(&pWriter->dWriter.bData);
      if (pWriter->dWriter.aSttBlk) taosArrayDestroy(pWriter->dWriter.aSttBlk);
      if (pWriter->dWriter.aBlockIdx) taosArrayDestroy(pWriter->dWriter.aBlockIdx);
      tBlockDataDestroy(&pWriter->dReader.bData);
      if (pWriter->dReader.aBlockIdx) taosArrayDestroy(pWriter->dReader.aBlockIdx);
      tBlockDataDestroy(&pWriter->bData);
      tsdbFSDestroy(&pWriter->fs);
      taosMemoryFree(pWriter);
    }
  } else {
    tsdbInfo("vgId:%d, %s done", TD_VID(pTsdb->pVnode), __func__);
    *ppWriter = pWriter;
  }
  return code;
}

int32_t tsdbSnapWriterPrepareClose(STsdbSnapWriter* pWriter) {
  int32_t code = 0;
  if (pWriter->dWriter.pWriter) {
    code = tsdbSnapWriteCloseFile(pWriter);
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

  // Writer
  tBlockDataDestroy(&pWriter->dWriter.sData);
  tBlockDataDestroy(&pWriter->dWriter.bData);
  taosArrayDestroy(pWriter->dWriter.aSttBlk);
  tMapDataClear(&pWriter->dWriter.mDataBlk);
  taosArrayDestroy(pWriter->dWriter.aBlockIdx);

  // Reader
  tBlockDataDestroy(&pWriter->dReader.bData);
  tMapDataClear(&pWriter->dReader.mDataBlk);
  taosArrayDestroy(pWriter->dReader.aBlockIdx);

  tBlockDataDestroy(&pWriter->bData);
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

int32_t tsdbSnapWrite(STsdbSnapWriter* pWriter, uint8_t* pData, uint32_t nData) {
  int32_t       code = 0;
  SSnapDataHdr* pHdr = (SSnapDataHdr*)pData;

  // ts data
  if (pHdr->type == SNAP_DATA_TSDB) {
    code = tsdbSnapWriteData(pWriter, pData, nData);
    if (code) goto _err;

    goto _exit;
  } else {
    if (pWriter->dWriter.pWriter) {
      code = tsdbSnapWriteCloseFile(pWriter);
      if (code) goto _err;
    }
  }

  // del data
  if (pHdr->type == SNAP_DATA_DEL) {
    code = tsdbSnapWriteDel(pWriter, pData, nData);
    if (code) goto _err;
  }

_exit:
  tsdbDebug("vgId:%d, tsdb snapshot write for %s succeed", TD_VID(pWriter->pTsdb->pVnode), pWriter->pTsdb->path);
  return code;

_err:
  tsdbError("vgId:%d, tsdb snapshot write for %s failed since %s", TD_VID(pWriter->pTsdb->pVnode), pWriter->pTsdb->path,
            tstrerror(code));
  return code;
}
