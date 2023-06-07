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

extern int32_t tsdbUpdateTableSchema(SMeta* pMeta, int64_t suid, int64_t uid, SSkmInfo* pSkmInfo);
extern int32_t tsdbWriteDataBlock(SDataFWriter* pWriter, SBlockData* pBlockData, SMapData* mDataBlk, int8_t cmprAlg);
extern int32_t tsdbWriteSttBlock(SDataFWriter* pWriter, SBlockData* pBlockData, SArray* aSttBlk, int8_t cmprAlg);

// STsdbSnapReader ========================================
struct STsdbSnapReader {
  STsdb*   pTsdb;
  int64_t  sver;
  int64_t  ever;
  int8_t   type;
  uint8_t* aBuf[5];

  STsdbFS  fs;
  TABLEID  tbid;
  SSkmInfo skmTable;

  // timeseries data
  int8_t  dataDone;
  int32_t fid;

  SDataFReader*   pDataFReader;
  STsdbDataIter2* iterList;
  STsdbDataIter2* pIter;
  SRBTree         rbt;
  SBlockData      bData;

  // tombstone data
  int8_t          delDone;
  SDelFReader*    pDelFReader;
  STsdbDataIter2* pTIter;
  SArray*         aDelData;
};

static int32_t tsdbSnapReadFileDataStart(STsdbSnapReader* pReader) {
  int32_t code = 0;
  int32_t lino = 0;

  SDFileSet* pSet = taosArraySearch(pReader->fs.aDFileSet, &(SDFileSet){.fid = pReader->fid}, tDFileSetCmprFn, TD_GT);
  if (pSet == NULL) {
    pReader->fid = INT32_MAX;
    goto _exit;
  }

  pReader->fid = pSet->fid;

  tRBTreeCreate(&pReader->rbt, tsdbDataIterCmprFn);

  code = tsdbDataFReaderOpen(&pReader->pDataFReader, pReader->pTsdb, pSet);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbOpenDataFileDataIter(pReader->pDataFReader, &pReader->pIter);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (pReader->pIter) {
    // iter to next with filter info (sver, ever)
    code = tsdbDataIterNext2(
        pReader->pIter,
        &(STsdbFilterInfo){.flag = TSDB_FILTER_FLAG_BY_VERSION | TSDB_FILTER_FLAG_IGNORE_DROPPED_TABLE,  // flag
                           .sver = pReader->sver,
                           .ever = pReader->ever});
    TSDB_CHECK_CODE(code, lino, _exit);

    if (pReader->pIter->rowInfo.suid || pReader->pIter->rowInfo.uid) {
      // add to rbtree
      tRBTreePut(&pReader->rbt, &pReader->pIter->rbtn);

      // add to iterList
      pReader->pIter->next = pReader->iterList;
      pReader->iterList = pReader->pIter;
    } else {
      tsdbCloseDataIter2(pReader->pIter);
    }
  }

  for (int32_t iStt = 0; iStt < pSet->nSttF; ++iStt) {
    code = tsdbOpenSttFileDataIter(pReader->pDataFReader, iStt, &pReader->pIter);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (pReader->pIter) {
      // iter to valid row
      code = tsdbDataIterNext2(
          pReader->pIter,
          &(STsdbFilterInfo){.flag = TSDB_FILTER_FLAG_BY_VERSION | TSDB_FILTER_FLAG_IGNORE_DROPPED_TABLE,  // flag
                             .sver = pReader->sver,
                             .ever = pReader->ever});
      TSDB_CHECK_CODE(code, lino, _exit);

      if (pReader->pIter->rowInfo.suid || pReader->pIter->rowInfo.uid) {
        // add to rbtree
        tRBTreePut(&pReader->rbt, &pReader->pIter->rbtn);

        // add to iterList
        pReader->pIter->next = pReader->iterList;
        pReader->iterList = pReader->pIter;
      } else {
        tsdbCloseDataIter2(pReader->pIter);
      }
    }
  }

  pReader->pIter = NULL;

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pReader->pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbInfo("vgId:%d %s done, fid:%d", TD_VID(pReader->pTsdb->pVnode), __func__, pReader->fid);
  }
  return code;
}

static void tsdbSnapReadFileDataEnd(STsdbSnapReader* pReader) {
  while (pReader->iterList) {
    STsdbDataIter2* pIter = pReader->iterList;
    pReader->iterList = pIter->next;
    tsdbCloseDataIter2(pIter);
  }

  tsdbDataFReaderClose(&pReader->pDataFReader);
}

static int32_t tsdbSnapReadNextRow(STsdbSnapReader* pReader, SRowInfo** ppRowInfo) {
  int32_t code = 0;
  int32_t lino = 0;

  if (pReader->pIter) {
    code = tsdbDataIterNext2(pReader->pIter, &(STsdbFilterInfo){.flag = TSDB_FILTER_FLAG_BY_VERSION |
                                                                        TSDB_FILTER_FLAG_IGNORE_DROPPED_TABLE,  // flag
                                                                .sver = pReader->sver,
                                                                .ever = pReader->ever});
    TSDB_CHECK_CODE(code, lino, _exit);

    if (pReader->pIter->rowInfo.suid == 0 && pReader->pIter->rowInfo.uid == 0) {
      pReader->pIter = NULL;
    } else {
      SRBTreeNode* pNode = tRBTreeMin(&pReader->rbt);
      if (pNode) {
        int32_t c = tsdbDataIterCmprFn(&pReader->pIter->rbtn, pNode);
        if (c > 0) {
          tRBTreePut(&pReader->rbt, &pReader->pIter->rbtn);
          pReader->pIter = NULL;
        } else if (c == 0) {
          ASSERT(0);
        }
      }
    }
  }

  if (pReader->pIter == NULL) {
    SRBTreeNode* pNode = tRBTreeMin(&pReader->rbt);
    if (pNode) {
      tRBTreeDrop(&pReader->rbt, pNode);
      pReader->pIter = TSDB_RBTN_TO_DATA_ITER(pNode);
    }
  }

  if (ppRowInfo) {
    if (pReader->pIter) {
      *ppRowInfo = &pReader->pIter->rowInfo;
    } else {
      *ppRowInfo = NULL;
    }
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pReader->pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbSnapReadGetRow(STsdbSnapReader* pReader, SRowInfo** ppRowInfo) {
  if (pReader->pIter) {
    *ppRowInfo = &pReader->pIter->rowInfo;
    return 0;
  }

  return tsdbSnapReadNextRow(pReader, ppRowInfo);
}

static int32_t tsdbSnapCmprData(STsdbSnapReader* pReader, uint8_t** ppData) {
  int32_t code = 0;

  ASSERT(pReader->bData.nRow);

  int32_t aBufN[5] = {0};
  code = tCmprBlockData(&pReader->bData, NO_COMPRESSION, NULL, NULL, pReader->aBuf, aBufN);
  if (code) goto _exit;

  int32_t size = aBufN[0] + aBufN[1] + aBufN[2] + aBufN[3];
  *ppData = taosMemoryMalloc(sizeof(SSnapDataHdr) + size);
  if (*ppData == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  SSnapDataHdr* pHdr = (SSnapDataHdr*)*ppData;
  pHdr->type = pReader->type;
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

static int32_t tsdbSnapReadTimeSeriesData(STsdbSnapReader* pReader, uint8_t** ppData) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdb* pTsdb = pReader->pTsdb;

  tBlockDataReset(&pReader->bData);

  for (;;) {
    // start a new file read if need
    if (pReader->pDataFReader == NULL) {
      code = tsdbSnapReadFileDataStart(pReader);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    if (pReader->pDataFReader == NULL) break;

    SRowInfo* pRowInfo;
    code = tsdbSnapReadGetRow(pReader, &pRowInfo);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (pRowInfo == NULL) {
      tsdbSnapReadFileDataEnd(pReader);
      continue;
    }

    code = tsdbUpdateTableSchema(pTsdb->pVnode->pMeta, pRowInfo->suid, pRowInfo->uid, &pReader->skmTable);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tBlockDataInit(&pReader->bData, (TABLEID*)pRowInfo, pReader->skmTable.pTSchema, NULL, 0);
    TSDB_CHECK_CODE(code, lino, _exit);

    do {
      if (!TABLE_SAME_SCHEMA(pReader->bData.suid, pReader->bData.uid, pRowInfo->suid, pRowInfo->uid)) break;

      if (pReader->bData.uid && pReader->bData.uid != pRowInfo->uid) {
        code = tRealloc((uint8_t**)&pReader->bData.aUid, sizeof(int64_t) * (pReader->bData.nRow + 1));
        TSDB_CHECK_CODE(code, lino, _exit);

        for (int32_t iRow = 0; iRow < pReader->bData.nRow; ++iRow) {
          pReader->bData.aUid[iRow] = pReader->bData.uid;
        }
        pReader->bData.uid = 0;
      }

      code = tBlockDataAppendRow(&pReader->bData, &pRowInfo->row, NULL, pRowInfo->uid);
      TSDB_CHECK_CODE(code, lino, _exit);

      code = tsdbSnapReadNextRow(pReader, &pRowInfo);
      TSDB_CHECK_CODE(code, lino, _exit);

      if (pReader->bData.nRow >= 81920) break;
    } while (pRowInfo);

    ASSERT(pReader->bData.nRow > 0);

    break;
  }

  if (pReader->bData.nRow > 0) {
    ASSERT(pReader->bData.suid || pReader->bData.uid);

    code = tsdbSnapCmprData(pReader, ppData);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbSnapCmprTombData(STsdbSnapReader* pReader, uint8_t** ppData) {
  int32_t code = 0;
  int32_t lino = 0;

  int64_t size = sizeof(TABLEID);
  for (int32_t iDelData = 0; iDelData < taosArrayGetSize(pReader->aDelData); ++iDelData) {
    size += tPutDelData(NULL, taosArrayGet(pReader->aDelData, iDelData));
  }

  uint8_t* pData = (uint8_t*)taosMemoryMalloc(sizeof(SSnapDataHdr) + size);
  if (pData == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  SSnapDataHdr* pHdr = (SSnapDataHdr*)pData;
  pHdr->type = SNAP_DATA_DEL;
  pHdr->size = size;

  TABLEID* pId = (TABLEID*)(pData + sizeof(SSnapDataHdr));
  *pId = pReader->tbid;

  size = sizeof(SSnapDataHdr) + sizeof(TABLEID);
  for (int32_t iDelData = 0; iDelData < taosArrayGetSize(pReader->aDelData); ++iDelData) {
    size += tPutDelData(pData + size, taosArrayGet(pReader->aDelData, iDelData));
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pReader->pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  *ppData = pData;
  return code;
}

static void tsdbSnapReadGetTombData(STsdbSnapReader* pReader, SDelInfo** ppDelInfo) {
  if (pReader->pTIter == NULL || (pReader->pTIter->delInfo.suid == 0 && pReader->pTIter->delInfo.uid == 0)) {
    *ppDelInfo = NULL;
  } else {
    *ppDelInfo = &pReader->pTIter->delInfo;
  }
}

static int32_t tsdbSnapReadNextTombData(STsdbSnapReader* pReader, SDelInfo** ppDelInfo) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tsdbDataIterNext2(
      pReader->pTIter, &(STsdbFilterInfo){.flag = TSDB_FILTER_FLAG_BY_VERSION | TSDB_FILTER_FLAG_IGNORE_DROPPED_TABLE,
                                          .sver = pReader->sver,
                                          .ever = pReader->ever});
  TSDB_CHECK_CODE(code, lino, _exit);

  if (ppDelInfo) {
    tsdbSnapReadGetTombData(pReader, ppDelInfo);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pReader->pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbSnapReadTombData(STsdbSnapReader* pReader, uint8_t** ppData) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdb* pTsdb = pReader->pTsdb;

  // open tombstone data iter if need
  if (pReader->pDelFReader == NULL) {
    if (pReader->fs.pDelFile == NULL) goto _exit;

    // open
    code = tsdbDelFReaderOpen(&pReader->pDelFReader, pReader->fs.pDelFile, pTsdb);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbOpenTombFileDataIter(pReader->pDelFReader, &pReader->pTIter);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (pReader->pTIter) {
      code = tsdbSnapReadNextTombData(pReader, NULL);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  // loop to get tombstone data
  SDelInfo* pDelInfo;
  tsdbSnapReadGetTombData(pReader, &pDelInfo);

  if (pDelInfo == NULL) goto _exit;

  pReader->tbid = *(TABLEID*)pDelInfo;

  if (pReader->aDelData) {
    taosArrayClear(pReader->aDelData);
  } else if ((pReader->aDelData = taosArrayInit(16, sizeof(SDelData))) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  while (pDelInfo && pDelInfo->suid == pReader->tbid.suid && pDelInfo->uid == pReader->tbid.uid) {
    if (taosArrayPush(pReader->aDelData, &pDelInfo->delData) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tsdbSnapReadNextTombData(pReader, &pDelInfo);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // encode tombstone data
  if (taosArrayGetSize(pReader->aDelData) > 0) {
    code = tsdbSnapCmprTombData(pReader, ppData);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s done", TD_VID(pTsdb->pVnode), __func__);
  }
  return code;
}

int32_t tsdbSnapReaderOpen(STsdb* pTsdb, int64_t sver, int64_t ever, int8_t type, STsdbSnapReader** ppReader) {
  int32_t code = 0;
  int32_t lino = 0;

  // alloc
  STsdbSnapReader* pReader = (STsdbSnapReader*)taosMemoryCalloc(1, sizeof(*pReader));
  if (pReader == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  pReader->pTsdb = pTsdb;
  pReader->sver = sver;
  pReader->ever = ever;
  pReader->type = type;

  taosThreadRwlockRdlock(&pTsdb->rwLock);
  code = tsdbFSRef(pTsdb, &pReader->fs);
  if (code) {
    taosThreadRwlockUnlock(&pTsdb->rwLock);
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  taosThreadRwlockUnlock(&pTsdb->rwLock);

  // init
  pReader->fid = INT32_MIN;

  code = tBlockDataCreate(&pReader->bData);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s, sver:%" PRId64 " ever:%" PRId64 " type:%d", TD_VID(pTsdb->pVnode),
              __func__, lino, tstrerror(code), sver, ever, type);
    if (pReader) {
      tBlockDataDestroy(&pReader->bData);
      tsdbFSUnref(pTsdb, &pReader->fs);
      taosMemoryFree(pReader);
      pReader = NULL;
    }
  } else {
    tsdbInfo("vgId:%d %s done, sver:%" PRId64 " ever:%" PRId64 " type:%d", TD_VID(pTsdb->pVnode), __func__, sver, ever,
             type);
  }
  *ppReader = pReader;
  return code;
}

int32_t tsdbSnapReaderClose(STsdbSnapReader** ppReader) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdbSnapReader* pReader = *ppReader;
  STsdb*           pTsdb = pReader->pTsdb;

  // tombstone
  if (pReader->pTIter) {
    tsdbCloseDataIter2(pReader->pTIter);
    pReader->pTIter = NULL;
  }
  if (pReader->pDelFReader) {
    tsdbDelFReaderClose(&pReader->pDelFReader);
  }
  taosArrayDestroy(pReader->aDelData);

  // timeseries
  while (pReader->iterList) {
    STsdbDataIter2* pIter = pReader->iterList;
    pReader->iterList = pIter->next;
    tsdbCloseDataIter2(pIter);
  }
  if (pReader->pDataFReader) {
    tsdbDataFReaderClose(&pReader->pDataFReader);
  }
  tBlockDataDestroy(&pReader->bData);

  // other
  tDestroyTSchema(pReader->skmTable.pTSchema);
  tsdbFSUnref(pReader->pTsdb, &pReader->fs);
  for (int32_t iBuf = 0; iBuf < sizeof(pReader->aBuf) / sizeof(pReader->aBuf[0]); iBuf++) {
    tFree(pReader->aBuf[iBuf]);
  }
  taosMemoryFree(pReader);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s done", TD_VID(pTsdb->pVnode), __func__);
  }
  *ppReader = NULL;
  return code;
}

int32_t tsdbSnapRead(STsdbSnapReader* pReader, uint8_t** ppData) {
  int32_t code = 0;
  int32_t lino = 0;

  *ppData = NULL;

  // read data file
  if (!pReader->dataDone) {
    code = tsdbSnapReadTimeSeriesData(pReader, ppData);
    TSDB_CHECK_CODE(code, lino, _exit);
    if (*ppData) {
      goto _exit;
    } else {
      pReader->dataDone = 1;
    }
  }

  // read del file
  if (!pReader->delDone) {
    code = tsdbSnapReadTombData(pReader, ppData);
    TSDB_CHECK_CODE(code, lino, _exit);
    if (*ppData) {
      goto _exit;
    } else {
      pReader->delDone = 1;
    }
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pReader->pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s done", TD_VID(pReader->pTsdb->pVnode), __func__);
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
  TABLEID tbid;

  // time-series data
  SBlockData inData;

  int32_t  fid;
  SSkmInfo skmTable;

  /* reader */
  SDataFReader*   pDataFReader;
  STsdbDataIter2* iterList;
  STsdbDataIter2* pDIter;
  STsdbDataIter2* pSIter;
  SRBTree         rbt;  // SRBTree<STsdbDataIter2>

  /* writer */
  SDataFWriter* pDataFWriter;
  SArray*       aBlockIdx;
  SMapData      mDataBlk;  // SMapData<SDataBlk>
  SArray*       aSttBlk;   // SArray<SSttBlk>
  SBlockData    bData;
  SBlockData    sData;

  // tombstone data
  /* reader */
  SDelFReader*    pDelFReader;
  STsdbDataIter2* pTIter;

  /* writer */
  SDelFWriter* pDelFWriter;
  SArray*      aDelIdx;
  SArray*      aDelData;
};

// SNAP_DATA_TSDB
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
          code = TSDB_CODE_OUT_OF_MEMORY;
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

  if (pId) {
    code = tsdbUpdateTableSchema(pWriter->pTsdb->pVnode->pMeta, pId->suid, pId->uid, &pWriter->skmTable);
    TSDB_CHECK_CODE(code, lino, _exit);

    tMapDataReset(&pWriter->mDataBlk);

    code = tBlockDataInit(&pWriter->bData, pId, pWriter->skmTable.pTSchema, NULL, 0);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (!TABLE_SAME_SCHEMA(pWriter->tbid.suid, pWriter->tbid.uid, pWriter->sData.suid, pWriter->sData.uid)) {
    if ((pWriter->sData.nRow > 0)) {
      code = tsdbWriteSttBlock(pWriter->pDataFWriter, &pWriter->sData, pWriter->aSttBlk, pWriter->cmprAlg);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    if (pId) {
      TABLEID id = {.suid = pWriter->tbid.suid, .uid = pWriter->tbid.suid ? 0 : pWriter->tbid.uid};
      code = tBlockDataInit(&pWriter->sData, &id, pWriter->skmTable.pTSchema, NULL, 0);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
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

          tMapDataPutItem(&pWriter->mDataBlk, &dataBlk, tPutDataBlk);
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

static int32_t tsdbSnapWriteTableDataEnd(STsdbSnapWriter* pWriter) {
  int32_t code = 0;
  int32_t lino = 0;

  // write a NULL row to end current table data write
  code = tsdbSnapWriteTableRow(pWriter, NULL);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (pWriter->bData.nRow > 0) {
    if (pWriter->bData.nRow < pWriter->minRow) {
      ASSERT(TABLE_SAME_SCHEMA(pWriter->sData.suid, pWriter->sData.uid, pWriter->tbid.suid, pWriter->tbid.uid));
      for (int32_t iRow = 0; iRow < pWriter->bData.nRow; iRow++) {
        code =
            tBlockDataAppendRow(&pWriter->sData, &tsdbRowFromBlockData(&pWriter->bData, iRow), NULL, pWriter->tbid.uid);
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
  pWriter->pSIter = NULL;
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
      code = tsdbOpenSttFileDataIter(pWriter->pDataFReader, iStt, &pWriter->pSIter);
      TSDB_CHECK_CODE(code, lino, _exit);

      if (pWriter->pSIter) {
        code = tsdbDataIterNext2(pWriter->pSIter, NULL);
        TSDB_CHECK_CODE(code, lino, _exit);

        // add to tree
        tRBTreePut(&pWriter->rbt, &pWriter->pSIter->rbtn);

        // add to list
        pWriter->pSIter->next = pWriter->iterList;
        pWriter->iterList = pWriter->pSIter;
      }
    }

    pWriter->pSIter = NULL;
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

static int32_t tsdbSnapWriteTableData(STsdbSnapWriter* pWriter, SRowInfo* pRowInfo) {
  int32_t code = 0;
  int32_t lino = 0;

  // switch to new table if need
  if (pRowInfo == NULL || pRowInfo->uid != pWriter->tbid.uid) {
    if (pWriter->tbid.uid) {
      code = tsdbSnapWriteTableDataEnd(pWriter);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tsdbSnapWriteTableDataStart(pWriter, (TABLEID*)pRowInfo);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (pRowInfo == NULL) goto _exit;

  code = tsdbSnapWriteTableRow(pWriter, &pRowInfo->row);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pWriter->pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbSnapWriteNextRow(STsdbSnapWriter* pWriter, SRowInfo** ppRowInfo) {
  int32_t code = 0;
  int32_t lino = 0;

  if (pWriter->pSIter) {
    code = tsdbDataIterNext2(pWriter->pSIter, NULL);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (pWriter->pSIter->rowInfo.suid == 0 && pWriter->pSIter->rowInfo.uid == 0) {
      pWriter->pSIter = NULL;
    } else {
      SRBTreeNode* pNode = tRBTreeMin(&pWriter->rbt);
      if (pNode) {
        int32_t c = tsdbDataIterCmprFn(&pWriter->pSIter->rbtn, pNode);
        if (c > 0) {
          tRBTreePut(&pWriter->rbt, &pWriter->pSIter->rbtn);
          pWriter->pSIter = NULL;
        } else if (c == 0) {
          ASSERT(0);
        }
      }
    }
  }

  if (pWriter->pSIter == NULL) {
    SRBTreeNode* pNode = tRBTreeMin(&pWriter->rbt);
    if (pNode) {
      tRBTreeDrop(&pWriter->rbt, pNode);
      pWriter->pSIter = TSDB_RBTN_TO_DATA_ITER(pNode);
    }
  }

  if (ppRowInfo) {
    if (pWriter->pSIter) {
      *ppRowInfo = &pWriter->pSIter->rowInfo;
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

  if (pWriter->pSIter) {
    *ppRowInfo = &pWriter->pSIter->rowInfo;
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

static int32_t tsdbSnapWriteFileDataEnd(STsdbSnapWriter* pWriter) {
  int32_t code = 0;
  int32_t lino = 0;

  ASSERT(pWriter->pDataFWriter);

  // consume remain data and end with a NULL table row
  SRowInfo* pRowInfo;
  code = tsdbSnapWriteGetRow(pWriter, &pRowInfo);
  TSDB_CHECK_CODE(code, lino, _exit);
  for (;;) {
    code = tsdbSnapWriteTableData(pWriter, pRowInfo);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (pRowInfo == NULL) break;

    code = tsdbSnapWriteNextRow(pWriter, &pRowInfo);
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
static int32_t tsdbSnapWriteDelTableDataStart(STsdbSnapWriter* pWriter, TABLEID* pId) {
  int32_t code = 0;
  int32_t lino = 0;

  if (pId) {
    pWriter->tbid = *pId;
  } else {
    pWriter->tbid = (TABLEID){.suid = INT64_MAX, .uid = INT64_MAX};
  }

  taosArrayClear(pWriter->aDelData);

  if (pWriter->pTIter) {
    while (pWriter->pTIter->tIter.iDelIdx < taosArrayGetSize(pWriter->pTIter->tIter.aDelIdx)) {
      SDelIdx* pDelIdx = taosArrayGet(pWriter->pTIter->tIter.aDelIdx, pWriter->pTIter->tIter.iDelIdx);

      int32_t c = tTABLEIDCmprFn(pDelIdx, &pWriter->tbid);
      if (c < 0) {
        code = tsdbReadDelDatav1(pWriter->pDelFReader, pDelIdx, pWriter->pTIter->tIter.aDelData, INT64_MAX);
        TSDB_CHECK_CODE(code, lino, _exit);

        SDelIdx* pDelIdxNew = taosArrayReserve(pWriter->aDelIdx, 1);
        if (pDelIdxNew == NULL) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          TSDB_CHECK_CODE(code, lino, _exit);
        }

        pDelIdxNew->suid = pDelIdx->suid;
        pDelIdxNew->uid = pDelIdx->uid;

        code = tsdbWriteDelData(pWriter->pDelFWriter, pWriter->pTIter->tIter.aDelData, pDelIdxNew);
        TSDB_CHECK_CODE(code, lino, _exit);

        pWriter->pTIter->tIter.iDelIdx++;
      } else if (c == 0) {
        code = tsdbReadDelDatav1(pWriter->pDelFReader, pDelIdx, pWriter->aDelData, INT64_MAX);
        TSDB_CHECK_CODE(code, lino, _exit);

        pWriter->pTIter->tIter.iDelIdx++;
        break;
      } else {
        break;
      }
    }
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

static int32_t tsdbSnapWriteDelTableDataEnd(STsdbSnapWriter* pWriter) {
  int32_t code = 0;
  int32_t lino = 0;

  if (taosArrayGetSize(pWriter->aDelData) > 0) {
    SDelIdx* pDelIdx = taosArrayReserve(pWriter->aDelIdx, 1);
    if (pDelIdx == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    pDelIdx->suid = pWriter->tbid.suid;
    pDelIdx->uid = pWriter->tbid.uid;

    code = tsdbWriteDelData(pWriter->pDelFWriter, pWriter->aDelData, pDelIdx);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pWriter->pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbTrace("vgId:%d %s done", TD_VID(pWriter->pTsdb->pVnode), __func__);
  }
  return code;
}

static int32_t tsdbSnapWriteDelTableData(STsdbSnapWriter* pWriter, TABLEID* pId, uint8_t* pData, int64_t size) {
  int32_t code = 0;
  int32_t lino = 0;

  if (pId == NULL || pId->uid != pWriter->tbid.uid) {
    if (pWriter->tbid.uid) {
      code = tsdbSnapWriteDelTableDataEnd(pWriter);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tsdbSnapWriteDelTableDataStart(pWriter, pId);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (pId == NULL) goto _exit;

  int64_t n = 0;
  while (n < size) {
    SDelData delData;
    n += tGetDelData(pData + n, &delData);

    if (taosArrayPush(pWriter->aDelData, &delData) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }
  ASSERT(n == size);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pWriter->pTsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbSnapWriteDelDataStart(STsdbSnapWriter* pWriter) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdb*    pTsdb = pWriter->pTsdb;
  SDelFile* pDelFile = pWriter->fs.pDelFile;

  pWriter->tbid = (TABLEID){0};

  // reader
  if (pDelFile) {
    code = tsdbDelFReaderOpen(&pWriter->pDelFReader, pDelFile, pTsdb);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbOpenTombFileDataIter(pWriter->pDelFReader, &pWriter->pTIter);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // writer
  code = tsdbDelFWriterOpen(&pWriter->pDelFWriter, &(SDelFile){.commitID = pWriter->commitID}, pTsdb);
  TSDB_CHECK_CODE(code, lino, _exit);

  if ((pWriter->aDelIdx = taosArrayInit(0, sizeof(SDelIdx))) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  if ((pWriter->aDelData = taosArrayInit(0, sizeof(SDelData))) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s done", TD_VID(pTsdb->pVnode), __func__);
  }
  return code;
}

static int32_t tsdbSnapWriteDelDataEnd(STsdbSnapWriter* pWriter) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdb* pTsdb = pWriter->pTsdb;

  // end remaining table with NULL data
  code = tsdbSnapWriteDelTableData(pWriter, NULL, NULL, 0);
  TSDB_CHECK_CODE(code, lino, _exit);

  // update file-level info
  code = tsdbWriteDelIdx(pWriter->pDelFWriter, pWriter->aDelIdx);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbUpdateDelFileHdr(pWriter->pDelFWriter);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbFSUpsertDelFile(&pWriter->fs, &pWriter->pDelFWriter->fDel);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbDelFWriterClose(&pWriter->pDelFWriter, 1);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (pWriter->pDelFReader) {
    code = tsdbDelFReaderClose(&pWriter->pDelFReader);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (pWriter->pTIter) {
    tsdbCloseDataIter2(pWriter->pTIter);
    pWriter->pTIter = NULL;
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbInfo("vgId:%d %s done", TD_VID(pTsdb->pVnode), __func__);
  }
  return code;
}

static int32_t tsdbSnapWriteDelData(STsdbSnapWriter* pWriter, SSnapDataHdr* pHdr) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdb* pTsdb = pWriter->pTsdb;

  // start to write del data if need
  if (pWriter->pDelFWriter == NULL) {
    code = tsdbSnapWriteDelDataStart(pWriter);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // do write del data
  code = tsdbSnapWriteDelTableData(pWriter, (TABLEID*)pHdr->data, pHdr->data + sizeof(TABLEID),
                                   pHdr->size - sizeof(TABLEID));
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed since %s", TD_VID(pTsdb->pVnode), __func__, tstrerror(code));
  } else {
    tsdbTrace("vgId:%d %s done", TD_VID(pTsdb->pVnode), __func__);
  }
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
  code = tBlockDataCreate(&pWriter->inData);
  TSDB_CHECK_CODE(code, lino, _exit);

  pWriter->fid = INT32_MIN;

  code = tBlockDataCreate(&pWriter->bData);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tBlockDataCreate(&pWriter->sData);
  TSDB_CHECK_CODE(code, lino, _exit);

  // SNAP_DATA_DEL

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
    if (pWriter) {
      tBlockDataDestroy(&pWriter->sData);
      tBlockDataDestroy(&pWriter->bData);
      tBlockDataDestroy(&pWriter->inData);
      tsdbFSDestroy(&pWriter->fs);
      taosMemoryFree(pWriter);
      pWriter = NULL;
    }
  } else {
    tsdbInfo("vgId:%d %s done, sver:%" PRId64 " ever:%" PRId64, TD_VID(pTsdb->pVnode), __func__, sver, ever);
  }
  *ppWriter = pWriter;
  return code;
}

int32_t tsdbSnapWriterPrepareClose(STsdbSnapWriter* pWriter) {
  int32_t code = 0;
  int32_t lino = 0;

  if (pWriter->pDataFWriter) {
    code = tsdbSnapWriteFileDataEnd(pWriter);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (pWriter->pDelFWriter) {
    code = tsdbSnapWriteDelDataEnd(pWriter);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tsdbFSPrepareCommit(pWriter->pTsdb, &pWriter->fs);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pWriter->pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s done", TD_VID(pWriter->pTsdb->pVnode), __func__);
  }
  return code;
}

int32_t tsdbSnapWriterClose(STsdbSnapWriter** ppWriter, int8_t rollback) {
  int32_t code = 0;
  int32_t lino = 0;

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
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    // unlock
    taosThreadRwlockUnlock(&pTsdb->rwLock);
  }

  // SNAP_DATA_DEL
  taosArrayDestroy(pWriter->aDelData);
  taosArrayDestroy(pWriter->aDelIdx);

  // SNAP_DATA_TSDB
  tBlockDataDestroy(&pWriter->sData);
  tBlockDataDestroy(&pWriter->bData);
  taosArrayDestroy(pWriter->aSttBlk);
  tMapDataClear(&pWriter->mDataBlk);
  taosArrayDestroy(pWriter->aBlockIdx);
  tDestroyTSchema(pWriter->skmTable.pTSchema);
  tBlockDataDestroy(&pWriter->inData);

  for (int32_t iBuf = 0; iBuf < sizeof(pWriter->aBuf) / sizeof(uint8_t*); iBuf++) {
    tFree(pWriter->aBuf[iBuf]);
  }
  tsdbFSDestroy(&pWriter->fs);
  taosMemoryFree(pWriter);
  *ppWriter = NULL;

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbInfo("vgId:%d %s done", TD_VID(pTsdb->pVnode), __func__);
  }
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
