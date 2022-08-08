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
  SArray*       aBlockIdx;  // SArray<SBlockIdx>
  int32_t       iBlockIdx;
  SBlockIdx*    pBlockIdx;
  SMapData      mBlock;  // SMapData<SBlock>
  int32_t       iBlock;
  SBlockData    oBlockData;
  SBlockData    nBlockData;
  // for del file
  int8_t       delDone;
  SDelFReader* pDelFReader;
  SArray*      aDelIdx;  // SArray<SDelIdx>
  int32_t      iDelIdx;
  SArray*      aDelData;  // SArray<SDelData>
};

#if 0
static int32_t tsdbSnapReadData(STsdbSnapReader* pReader, uint8_t** ppData) {
  int32_t code = 0;
  STsdb*  pTsdb = pReader->pTsdb;

  while (true) {
    if (pReader->pDataFReader == NULL) {
      SDFileSet* pSet =
          taosArraySearch(pReader->fs.aDFileSet, &(SDFileSet){.fid = pReader->fid}, tDFileSetCmprFn, TD_GT);

      if (pSet == NULL) goto _exit;

      pReader->fid = pSet->fid;
      code = tsdbDataFReaderOpen(&pReader->pDataFReader, pReader->pTsdb, pSet);
      if (code) goto _err;

      // SBlockIdx
      code = tsdbReadBlockIdx(pReader->pDataFReader, pReader->aBlockIdx);
      if (code) goto _err;

      pReader->iBlockIdx = 0;
      pReader->pBlockIdx = NULL;

      tsdbInfo("vgId:%d, vnode snapshot tsdb open data file to read for %s, fid:%d", TD_VID(pTsdb->pVnode), pTsdb->path,
               pReader->fid);
    }

    while (true) {
      if (pReader->pBlockIdx == NULL) {
        if (pReader->iBlockIdx >= taosArrayGetSize(pReader->aBlockIdx)) {
          tsdbDataFReaderClose(&pReader->pDataFReader);
          break;
        }

        pReader->pBlockIdx = (SBlockIdx*)taosArrayGet(pReader->aBlockIdx, pReader->iBlockIdx);
        pReader->iBlockIdx++;

        code = tsdbReadBlock(pReader->pDataFReader, pReader->pBlockIdx, &pReader->mBlock);
        if (code) goto _err;

        pReader->iBlock = 0;
      }

      SBlock  block;
      SBlock* pBlock = &block;
      while (true) {
        if (pReader->iBlock >= pReader->mBlock.nItem) {
          pReader->pBlockIdx = NULL;
          break;
        }

        tMapDataGetItemByIdx(&pReader->mBlock, pReader->iBlock, pBlock, tGetBlock);
        pReader->iBlock++;

        if (pBlock->minVersion > pReader->ever || pBlock->maxVersion < pReader->sver) continue;

        code = tsdbReadBlockData(pReader->pDataFReader, pReader->pBlockIdx, pBlock, &pReader->oBlockData, NULL, NULL);
        if (code) goto _err;

        // filter
        tBlockDataReset(&pReader->nBlockData);
        for (int32_t iColData = 0; iColData < taosArrayGetSize(pReader->oBlockData.aIdx); iColData++) {
          SColData* pColDataO = tBlockDataGetColDataByIdx(&pReader->oBlockData, iColData);
          SColData* pColDataN = NULL;

          code = tBlockDataAddColData(&pReader->nBlockData, taosArrayGetSize(pReader->nBlockData.aIdx), &pColDataN);
          if (code) goto _err;

          tColDataInit(pColDataN, pColDataO->cid, pColDataO->type, pColDataO->smaOn);
        }

        for (int32_t iRow = 0; iRow < pReader->oBlockData.nRow; iRow++) {
          TSDBROW row = tsdbRowFromBlockData(&pReader->oBlockData, iRow);
          int64_t version = TSDBROW_VERSION(&row);

          tsdbTrace("vgId:%d, vnode snapshot tsdb read for %s, %" PRId64 "(%" PRId64 " , %" PRId64 ")",
                    TD_VID(pReader->pTsdb->pVnode), pReader->pTsdb->path, version, pReader->sver, pReader->ever);

          if (version < pReader->sver || version > pReader->ever) continue;

          code = tBlockDataAppendRow(&pReader->nBlockData, &row, NULL);
          if (code) goto _err;
        }

        if (pReader->nBlockData.nRow <= 0) {
          continue;
        }

        // org data
        // compress data (todo)
        int32_t size = sizeof(TABLEID) + tPutBlockData(NULL, &pReader->nBlockData);

        *ppData = taosMemoryMalloc(sizeof(SSnapDataHdr) + size);
        if (*ppData == NULL) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          goto _err;
        }

        SSnapDataHdr* pHdr = (SSnapDataHdr*)(*ppData);
        pHdr->type = pReader->type;
        pHdr->size = size;

        TABLEID* pId = (TABLEID*)(&pHdr[1]);
        pId->suid = pReader->pBlockIdx->suid;
        pId->uid = pReader->pBlockIdx->uid;

        tPutBlockData((uint8_t*)(&pId[1]), &pReader->nBlockData);

        tsdbInfo("vgId:%d, vnode snapshot read data for %s, fid:%d suid:%" PRId64 " uid:%" PRId64
                 " iBlock:%d minVersion:%d maxVersion:%d nRow:%d out of %d size:%d",
                 TD_VID(pTsdb->pVnode), pTsdb->path, pReader->fid, pReader->pBlockIdx->suid, pReader->pBlockIdx->uid,
                 pReader->iBlock - 1, pBlock->minVersion, pBlock->maxVersion, pReader->nBlockData.nRow, pBlock->nRow,
                 size);

        goto _exit;
      }
    }
  }

_exit:
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
    code = tsdbDelFReaderOpen(&pReader->pDelFReader, pDelFile, pTsdb, NULL);
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

    tsdbInfo("vgId:%d, vnode snapshot tsdb read del data for %s, suid:%" PRId64 " uid:%d" PRId64 " size:%d",
             TD_VID(pTsdb->pVnode), pTsdb->path, pDelIdx->suid, pDelIdx->uid, size);

    break;
  }

_exit:
  return code;

_err:
  tsdbError("vgId:%d, vnode snapshot tsdb read del for %s failed since %s", TD_VID(pTsdb->pVnode), pTsdb->pVnode,
            tstrerror(code));
  return code;
}
#endif

int32_t tsdbSnapReaderOpen(STsdb* pTsdb, int64_t sver, int64_t ever, int8_t type, STsdbSnapReader** ppReader) {
  int32_t code = 0;
#if 0
  STsdbSnapReader* pReader = NULL;

  // alloc
  pReader = (STsdbSnapReader*)taosMemoryCalloc(1, sizeof(*pReader));
  if (pReader == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pReader->pTsdb = pTsdb;
  pReader->sver = sver;
  pReader->ever = ever;
  pReader->type = type;

  code = taosThreadRwlockRdlock(&pTsdb->rwLock);
  if (code) {
    code = TAOS_SYSTEM_ERROR(code);
    goto _err;
  }

  code = tsdbFSRef(pTsdb, &pReader->fs);
  if (code) {
    taosThreadRwlockUnlock(&pTsdb->rwLock);
    goto _err;
  }

  code = taosThreadRwlockUnlock(&pTsdb->rwLock);
  if (code) {
    code = TAOS_SYSTEM_ERROR(code);
    goto _err;
  }

  pReader->fid = INT32_MIN;
  pReader->aBlockIdx = taosArrayInit(0, sizeof(SBlockIdx));
  if (pReader->aBlockIdx == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pReader->mBlock = tMapDataInit();
  code = tBlockDataCreate(&pReader->oBlockData);
  if (code) goto _err;
  code = tBlockDataCreate(&pReader->nBlockData);
  if (code) goto _err;

  pReader->aDelIdx = taosArrayInit(0, sizeof(SDelIdx));
  if (pReader->aDelIdx == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pReader->aDelData = taosArrayInit(0, sizeof(SDelData));
  if (pReader->aDelData == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  tsdbInfo("vgId:%d, vnode snapshot tsdb reader opened for %s", TD_VID(pTsdb->pVnode), pTsdb->path);
  *ppReader = pReader;
  return code;

_err:
  tsdbError("vgId:%d, vnode snapshot tsdb reader open for %s failed since %s", TD_VID(pTsdb->pVnode), pTsdb->path,
            tstrerror(code));
  *ppReader = NULL;
#endif
  return code;
}

int32_t tsdbSnapReaderClose(STsdbSnapReader** ppReader) {
  int32_t code = 0;
#if 0
  STsdbSnapReader* pReader = *ppReader;

  if (pReader->pDataFReader) {
    tsdbDataFReaderClose(&pReader->pDataFReader);
  }
  taosArrayDestroy(pReader->aBlockIdx);
  tMapDataClear(&pReader->mBlock);
  tBlockDataDestroy(&pReader->oBlockData, 1);
  tBlockDataDestroy(&pReader->nBlockData, 1);

  if (pReader->pDelFReader) {
    tsdbDelFReaderClose(&pReader->pDelFReader);
  }
  taosArrayDestroy(pReader->aDelIdx);
  taosArrayDestroy(pReader->aDelData);

  tsdbFSUnref(pReader->pTsdb, &pReader->fs);

  tsdbInfo("vgId:%d, vnode snapshot tsdb reader closed for %s", TD_VID(pReader->pTsdb->pVnode), pReader->pTsdb->path);

  taosMemoryFree(pReader);
  *ppReader = NULL;
#endif
  return code;
}

int32_t tsdbSnapRead(STsdbSnapReader* pReader, uint8_t** ppData) {
  int32_t code = 0;
#if 0

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
#endif
  return code;
}

// STsdbSnapWriter ========================================
struct STsdbSnapWriter {
  STsdb*  pTsdb;
  int64_t sver;
  int64_t ever;
  STsdbFS fs;

  // config
  int32_t minutes;
  int8_t  precision;
  int32_t minRow;
  int32_t maxRow;
  int8_t  cmprAlg;
  int64_t commitID;

  // for data file
  SBlockData bData;

  int32_t       fid;
  SDataFReader* pDataFReader;
  SArray*       aBlockIdx;  // SArray<SBlockIdx>
  int32_t       iBlockIdx;
  SBlockIdx*    pBlockIdx;
  SMapData      mBlock;  // SMapData<SBlock>
  int32_t       iBlock;
  SBlockData*   pBlockData;
  int32_t       iRow;
  SBlockData    bDataR;

  SDataFWriter* pDataFWriter;
  SBlockIdx*    pBlockIdxW;  // NULL when no committing table
  SBlock        blockW;
  SBlockData    bDataW;
  SBlockIdx     blockIdxW;

  SMapData mBlockW;     // SMapData<SBlock>
  SArray*  aBlockIdxW;  // SArray<SBlockIdx>

  // for del file
  SDelFReader* pDelFReader;
  SDelFWriter* pDelFWriter;
  int32_t      iDelIdx;
  SArray*      aDelIdxR;
  SArray*      aDelData;
  SArray*      aDelIdxW;
};

#if 0
static int32_t tsdbSnapWriteAppendData(STsdbSnapWriter* pWriter, uint8_t* pData, uint32_t nData) {
  int32_t     code = 0;
  int32_t     iRow = 0;           // todo
  int32_t     nRow = 0;           // todo
  SBlockData* pBlockData = NULL;  // todo

  while (iRow < nRow) {
    code = tBlockDataAppendRow(&pWriter->bDataW, &tsdbRowFromBlockData(pBlockData, iRow), NULL);
    if (code) goto _err;
  }

  return code;

_err:
  tsdbError("vgId:%d, tsdb snapshot write append data for %s failed since %s", TD_VID(pWriter->pTsdb->pVnode),
            pWriter->pTsdb->path, tstrerror(code));
  return code;
}

static int32_t tsdbSnapWriteTableDataEnd(STsdbSnapWriter* pWriter) {
  int32_t code = 0;

  ASSERT(pWriter->pDataFWriter);

  if (pWriter->pBlockIdxW == NULL) goto _exit;

  // consume remain rows
  if (pWriter->pBlockData) {
    ASSERT(pWriter->iRow < pWriter->pBlockData->nRow);
    while (pWriter->iRow < pWriter->pBlockData->nRow) {
      code = tBlockDataAppendRow(&pWriter->bDataW, &tsdbRowFromBlockData(pWriter->pBlockData, pWriter->iRow), NULL);
      if (code) goto _err;

      if (pWriter->bDataW.nRow >= pWriter->maxRow * 4 / 5) {
        pWriter->blockW.last = 0;
        code = tsdbWriteBlockData(pWriter->pDataFWriter, &pWriter->bDataW, NULL, NULL, pWriter->pBlockIdxW,
                                  &pWriter->blockW, pWriter->cmprAlg);
        if (code) goto _err;

        code = tMapDataPutItem(&pWriter->mBlockW, &pWriter->blockW, tPutBlock);
        if (code) goto _err;

        tBlockReset(&pWriter->blockW);
        tBlockDataClear(&pWriter->bDataW);
      }

      pWriter->iRow++;
    }
  }

  // write remain data if has
  if (pWriter->bDataW.nRow > 0) {
    pWriter->blockW.last = 0;
    if (pWriter->bDataW.nRow < pWriter->minRow) {
      if (pWriter->iBlock > pWriter->mBlock.nItem) {
        pWriter->blockW.last = 1;
      }
    }

    code = tsdbWriteBlockData(pWriter->pDataFWriter, &pWriter->bDataW, NULL, NULL, pWriter->pBlockIdxW,
                              &pWriter->blockW, pWriter->cmprAlg);
    if (code) goto _err;

    code = tMapDataPutItem(&pWriter->mBlockW, &pWriter->blockW, tPutBlock);
    if (code) goto _err;
  }

  while (true) {
    if (pWriter->iBlock >= pWriter->mBlock.nItem) break;

    SBlock block;
    tMapDataGetItemByIdx(&pWriter->mBlock, pWriter->iBlock, &block, tGetBlock);

    if (block.last) {
      code = tsdbReadBlockData(pWriter->pDataFReader, pWriter->pBlockIdx, &block, &pWriter->bDataR, NULL, NULL);
      if (code) goto _err;

      tBlockReset(&block);
      block.last = 1;
      code = tsdbWriteBlockData(pWriter->pDataFWriter, &pWriter->bDataR, NULL, NULL, pWriter->pBlockIdxW, &block,
                                pWriter->cmprAlg);
      if (code) goto _err;
    }

    code = tMapDataPutItem(&pWriter->mBlockW, &block, tPutBlock);
    if (code) goto _err;

    pWriter->iBlock++;
  }

  // SBlock
  code = tsdbWriteBlock(pWriter->pDataFWriter, &pWriter->mBlockW, NULL, pWriter->pBlockIdxW);
  if (code) goto _err;

  // SBlockIdx
  if (taosArrayPush(pWriter->aBlockIdxW, pWriter->pBlockIdxW) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

_exit:
  tsdbInfo("vgId:%d, tsdb snapshot write table data end for %s", TD_VID(pWriter->pTsdb->pVnode), pWriter->pTsdb->path);
  return code;

_err:
  tsdbError("vgId:%d, tsdb snapshot write table data end for %s failed since %s", TD_VID(pWriter->pTsdb->pVnode),
            pWriter->pTsdb->path, tstrerror(code));
  return code;
}

static int32_t tsdbSnapMoveWriteTableData(STsdbSnapWriter* pWriter, SBlockIdx* pBlockIdx) {
  int32_t code = 0;

  code = tsdbReadBlock(pWriter->pDataFReader, pBlockIdx, &pWriter->mBlock);
  if (code) goto _err;

  // SBlockData
  SBlock block;
  tMapDataReset(&pWriter->mBlockW);
  for (int32_t iBlock = 0; iBlock < pWriter->mBlock.nItem; iBlock++) {
    tMapDataGetItemByIdx(&pWriter->mBlock, iBlock, &block, tGetBlock);

    if (block.last) {
      code = tsdbReadBlockData(pWriter->pDataFReader, pBlockIdx, &block, &pWriter->bDataR, NULL, NULL);
      if (code) goto _err;

      tBlockReset(&block);
      block.last = 1;
      code =
          tsdbWriteBlockData(pWriter->pDataFWriter, &pWriter->bDataR, NULL, NULL, pBlockIdx, &block, pWriter->cmprAlg);
      if (code) goto _err;
    }

    code = tMapDataPutItem(&pWriter->mBlockW, &block, tPutBlock);
    if (code) goto _err;
  }

  // SBlock
  SBlockIdx blockIdx = {.suid = pBlockIdx->suid, .uid = pBlockIdx->uid};
  code = tsdbWriteBlock(pWriter->pDataFWriter, &pWriter->mBlockW, NULL, &blockIdx);
  if (code) goto _err;

  // SBlockIdx
  if (taosArrayPush(pWriter->aBlockIdxW, &blockIdx) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

_exit:
  return code;

_err:
  tsdbError("vgId:%d, tsdb snapshot move write table data for %s failed since %s", TD_VID(pWriter->pTsdb->pVnode),
            pWriter->pTsdb->path, tstrerror(code));
  return code;
}

static int32_t tsdbSnapWriteTableDataImpl(STsdbSnapWriter* pWriter) {
  int32_t     code = 0;
  SBlockData* pBlockData = &pWriter->bData;
  int32_t     iRow = 0;
  TSDBROW     row;
  TSDBROW*    pRow = &row;

  // correct schema
  code = tBlockDataCorrectSchema(&pWriter->bDataW, pBlockData);
  if (code) goto _err;

  // loop to merge
  *pRow = tsdbRowFromBlockData(pBlockData, iRow);
  while (true) {
    if (pRow == NULL) break;

    if (pWriter->pBlockData) {
      ASSERT(pWriter->iRow < pWriter->pBlockData->nRow);

      int32_t c = tsdbRowCmprFn(pRow, &tsdbRowFromBlockData(pWriter->pBlockData, pWriter->iRow));

      ASSERT(c);

      if (c < 0) {
        code = tBlockDataAppendRow(&pWriter->bDataW, pRow, NULL);
        if (code) goto _err;

        iRow++;
        if (iRow < pWriter->pBlockData->nRow) {
          *pRow = tsdbRowFromBlockData(pBlockData, iRow);
        } else {
          pRow = NULL;
        }
      } else if (c > 0) {
        code = tBlockDataAppendRow(&pWriter->bDataW, &tsdbRowFromBlockData(pWriter->pBlockData, pWriter->iRow), NULL);
        if (code) goto _err;

        pWriter->iRow++;
        if (pWriter->iRow >= pWriter->pBlockData->nRow) {
          pWriter->pBlockData = NULL;
        }
      }
    } else {
      TSDBKEY key = TSDBROW_KEY(pRow);

      while (true) {
        if (pWriter->iBlock >= pWriter->mBlock.nItem) break;

        SBlock  block;
        int32_t c;

        tMapDataGetItemByIdx(&pWriter->mBlock, pWriter->iBlock, &block, tGetBlock);

        if (block.last) {
          pWriter->pBlockData = &pWriter->bDataR;

          code = tsdbReadBlockData(pWriter->pDataFReader, pWriter->pBlockIdx, &block, pWriter->pBlockData, NULL, NULL);
          if (code) goto _err;
          pWriter->iRow = 0;

          pWriter->iBlock++;
          break;
        }

        c = tsdbKeyCmprFn(&block.maxKey, &key);

        ASSERT(c);

        if (c < 0) {
          if (pWriter->bDataW.nRow) {
            pWriter->blockW.last = 0;
            code = tsdbWriteBlockData(pWriter->pDataFWriter, &pWriter->bDataW, NULL, NULL, pWriter->pBlockIdxW,
                                      &pWriter->blockW, pWriter->cmprAlg);
            if (code) goto _err;

            code = tMapDataPutItem(&pWriter->mBlockW, &pWriter->blockW, tPutBlock);
            if (code) goto _err;

            tBlockReset(&pWriter->blockW);
            tBlockDataClear(&pWriter->bDataW);
          }

          code = tMapDataPutItem(&pWriter->mBlockW, &block, tPutBlock);
          if (code) goto _err;

          pWriter->iBlock++;
        } else {
          c = tsdbKeyCmprFn(&tBlockDataLastKey(pBlockData), &block.minKey);

          ASSERT(c);

          if (c > 0) {
            pWriter->pBlockData = &pWriter->bDataR;
            code =
                tsdbReadBlockData(pWriter->pDataFReader, pWriter->pBlockIdx, &block, pWriter->pBlockData, NULL, NULL);
            if (code) goto _err;
            pWriter->iRow = 0;

            pWriter->iBlock++;
          }
          break;
        }
      }

      if (pWriter->pBlockData) continue;

      code = tBlockDataAppendRow(&pWriter->bDataW, pRow, NULL);
      if (code) goto _err;

      iRow++;
      if (iRow < pBlockData->nRow) {
        *pRow = tsdbRowFromBlockData(pBlockData, iRow);
      } else {
        pRow = NULL;
      }
    }

  _check_write:
    if (pWriter->bDataW.nRow < pWriter->maxRow * 4 / 5) continue;

  _write_block:
    code = tsdbWriteBlockData(pWriter->pDataFWriter, &pWriter->bDataW, NULL, NULL, pWriter->pBlockIdxW,
                              &pWriter->blockW, pWriter->cmprAlg);
    if (code) goto _err;

    code = tMapDataPutItem(&pWriter->mBlockW, &pWriter->blockW, tPutBlock);
    if (code) goto _err;

    tBlockReset(&pWriter->blockW);
    tBlockDataClear(&pWriter->bDataW);
  }

  return code;

_err:
  tsdbError("vgId:%d, vnode snapshot tsdb write table data impl for %s failed since %s", TD_VID(pWriter->pTsdb->pVnode),
            pWriter->pTsdb->path, tstrerror(code));
  return code;
}

static int32_t tsdbSnapWriteTableData(STsdbSnapWriter* pWriter, TABLEID id) {
  int32_t     code = 0;
  SBlockData* pBlockData = &pWriter->bData;
  TSDBKEY     keyFirst = tBlockDataFirstKey(pBlockData);
  TSDBKEY     keyLast = tBlockDataLastKey(pBlockData);

  // end last table write if should
  if (pWriter->pBlockIdxW) {
    int32_t c = tTABLEIDCmprFn(pWriter->pBlockIdxW, &id);
    if (c < 0) {
      // end
      code = tsdbSnapWriteTableDataEnd(pWriter);
      if (code) goto _err;

      // reset
      pWriter->pBlockIdxW = NULL;
    } else if (c > 0) {
      ASSERT(0);
    }
  }

  // start new table data write if need
  if (pWriter->pBlockIdxW == NULL) {
    // write table data ahead
    while (true) {
      if (pWriter->iBlockIdx >= taosArrayGetSize(pWriter->aBlockIdx)) break;

      SBlockIdx* pBlockIdx = (SBlockIdx*)taosArrayGet(pWriter->aBlockIdx, pWriter->iBlockIdx);
      int32_t    c = tTABLEIDCmprFn(pBlockIdx, &id);

      if (c >= 0) break;

      code = tsdbSnapMoveWriteTableData(pWriter, pBlockIdx);
      if (code) goto _err;

      pWriter->iBlockIdx++;
    }

    // reader
    pWriter->pBlockIdx = NULL;
    if (pWriter->iBlockIdx < taosArrayGetSize(pWriter->aBlockIdx)) {
      ASSERT(pWriter->pDataFReader);

      SBlockIdx* pBlockIdx = (SBlockIdx*)taosArrayGet(pWriter->aBlockIdx, pWriter->iBlockIdx);
      int32_t    c = tTABLEIDCmprFn(pBlockIdx, &id);

      ASSERT(c >= 0);

      if (c == 0) {
        pWriter->pBlockIdx = pBlockIdx;
        pWriter->iBlockIdx++;
      }
    }

    if (pWriter->pBlockIdx) {
      code = tsdbReadBlock(pWriter->pDataFReader, pWriter->pBlockIdx, &pWriter->mBlock);
      if (code) goto _err;
    } else {
      tMapDataReset(&pWriter->mBlock);
    }
    pWriter->iBlock = 0;
    pWriter->pBlockData = NULL;
    pWriter->iRow = 0;

    // writer
    pWriter->pBlockIdxW = &pWriter->blockIdxW;
    pWriter->pBlockIdxW->suid = id.suid;
    pWriter->pBlockIdxW->uid = id.uid;

    tBlockReset(&pWriter->blockW);
    tBlockDataReset(&pWriter->bDataW);
    tMapDataReset(&pWriter->mBlockW);
  }

  ASSERT(pWriter->pBlockIdxW && pWriter->pBlockIdxW->suid == id.suid && pWriter->pBlockIdxW->uid == id.uid);
  ASSERT(pWriter->pBlockIdx == NULL || (pWriter->pBlockIdx->suid == id.suid && pWriter->pBlockIdx->uid == id.uid));

  code = tsdbSnapWriteTableDataImpl(pWriter);
  if (code) goto _err;

_exit:
  tsdbDebug("vgId:%d, vnode snapshot tsdb write data impl for %s", TD_VID(pWriter->pTsdb->pVnode),
            pWriter->pTsdb->path);
  return code;

_err:
  tsdbError("vgId:%d, vnode snapshot tsdb write data impl for %s failed since %s", TD_VID(pWriter->pTsdb->pVnode),
            pWriter->pTsdb->path, tstrerror(code));
  return code;
}

static int32_t tsdbSnapWriteDataEnd(STsdbSnapWriter* pWriter) {
  int32_t code = 0;
  STsdb*  pTsdb = pWriter->pTsdb;

  if (pWriter->pDataFWriter == NULL) goto _exit;

  code = tsdbSnapWriteTableDataEnd(pWriter);
  if (code) goto _err;

  while (pWriter->iBlockIdx < taosArrayGetSize(pWriter->aBlockIdx)) {
    code = tsdbSnapMoveWriteTableData(pWriter, (SBlockIdx*)taosArrayGet(pWriter->aBlockIdx, pWriter->iBlockIdx));
    if (code) goto _err;

    pWriter->iBlockIdx++;
  }

  code = tsdbWriteBlockIdx(pWriter->pDataFWriter, pWriter->aBlockIdxW);
  if (code) goto _err;

  code = tsdbFSUpsertFSet(&pWriter->fs, &pWriter->pDataFWriter->wSet);
  if (code) goto _err;

  code = tsdbDataFWriterClose(&pWriter->pDataFWriter, 1);
  if (code) goto _err;

  if (pWriter->pDataFReader) {
    code = tsdbDataFReaderClose(&pWriter->pDataFReader);
    if (code) goto _err;
  }

_exit:
  tsdbInfo("vgId:%d, vnode snapshot tsdb writer data end for %s", TD_VID(pTsdb->pVnode), pTsdb->path);
  return code;

_err:
  tsdbError("vgId:%d, vnode snapshot tsdb writer data end for %s failed since %s", TD_VID(pTsdb->pVnode), pTsdb->path,
            tstrerror(code));
  return code;
}

static int32_t tsdbSnapWriteData(STsdbSnapWriter* pWriter, uint8_t* pData, uint32_t nData) {
  int32_t code = 0;
  STsdb*  pTsdb = pWriter->pTsdb;
  TABLEID id = *(TABLEID*)(pData + sizeof(SSnapDataHdr));
  int64_t n;

  // decode
  SBlockData* pBlockData = &pWriter->bData;
  n = tGetBlockData(pData + sizeof(SSnapDataHdr) + sizeof(TABLEID), pBlockData);
  ASSERT(n + sizeof(SSnapDataHdr) + sizeof(TABLEID) == nData);

  // open file
  TSDBKEY keyFirst = tBlockDataFirstKey(pBlockData);
  TSDBKEY keyLast = tBlockDataLastKey(pBlockData);

  int32_t fid = tsdbKeyFid(keyFirst.ts, pWriter->minutes, pWriter->precision);
  ASSERT(fid == tsdbKeyFid(keyLast.ts, pWriter->minutes, pWriter->precision));
  if (pWriter->pDataFWriter == NULL || pWriter->fid != fid) {
    // end last file data write if need
    code = tsdbSnapWriteDataEnd(pWriter);
    if (code) goto _err;

    pWriter->fid = fid;

    // read
    SDFileSet* pSet = taosArraySearch(pWriter->fs.aDFileSet, &(SDFileSet){.fid = fid}, tDFileSetCmprFn, TD_EQ);
    if (pSet) {
      code = tsdbDataFReaderOpen(&pWriter->pDataFReader, pTsdb, pSet);
      if (code) goto _err;

      code = tsdbReadBlockIdx(pWriter->pDataFReader, pWriter->aBlockIdx);
      if (code) goto _err;
    } else {
      ASSERT(pWriter->pDataFReader == NULL);
      taosArrayClear(pWriter->aBlockIdx);
    }
    pWriter->iBlockIdx = 0;
    pWriter->pBlockIdx = NULL;
    tMapDataReset(&pWriter->mBlock);
    pWriter->iBlock = 0;
    pWriter->pBlockData = NULL;
    pWriter->iRow = 0;
    tBlockDataReset(&pWriter->bDataR);

    // write
    SHeadFile fHead;
    SDataFile fData;
    SLastFile fLast;
    SSmaFile  fSma;
    SDFileSet wSet = {.pHeadF = &fHead, .pDataF = &fData, .pLastF = &fLast, .pSmaF = &fSma};

    if (pSet) {
      wSet.diskId = pSet->diskId;
      wSet.fid = fid;
      fHead = (SHeadFile){.commitID = pWriter->commitID, .offset = 0, .size = 0, .loffset = 0};
      fData = *pSet->pDataF;
      fLast = (SLastFile){.commitID = pWriter->commitID, .size = 0};
      fSma = *pSet->pSmaF;
    } else {
      wSet.diskId = (SDiskID){.level = 0, .id = 0};
      wSet.fid = fid;
      fHead = (SHeadFile){.commitID = pWriter->commitID, .offset = 0, .size = 0, .loffset = 0};
      fData = (SDataFile){.commitID = pWriter->commitID, .size = 0};
      fLast = (SLastFile){.commitID = pWriter->commitID, .size = 0};
      fSma = (SSmaFile){.commitID = pWriter->commitID, .size = 0};
    }

    code = tsdbDataFWriterOpen(&pWriter->pDataFWriter, pTsdb, &wSet);
    if (code) goto _err;

    taosArrayClear(pWriter->aBlockIdxW);
    tMapDataReset(&pWriter->mBlockW);
    pWriter->pBlockIdxW = NULL;
    tBlockDataReset(&pWriter->bDataW);
  }

  code = tsdbSnapWriteTableData(pWriter, id);
  if (code) goto _err;

  tsdbInfo("vgId:%d, vnode snapshot tsdb write data for %s, fid:%d suid:%" PRId64 " uid:%" PRId64 " nRow:%d",
           TD_VID(pTsdb->pVnode), pTsdb->path, fid, id.suid, id.suid, pBlockData->nRow);
  return code;

_err:
  tsdbError("vgId:%d, vnode snapshot tsdb write data for %s failed since %s", TD_VID(pTsdb->pVnode), pTsdb->path,
            tstrerror(code));
  return code;
}

static int32_t tsdbSnapWriteDel(STsdbSnapWriter* pWriter, uint8_t* pData, uint32_t nData) {
  int32_t code = 0;
  STsdb*  pTsdb = pWriter->pTsdb;

  if (pWriter->pDelFWriter == NULL) {
    SDelFile* pDelFile = pWriter->fs.pDelFile;

    // reader
    if (pDelFile) {
      code = tsdbDelFReaderOpen(&pWriter->pDelFReader, pDelFile, pTsdb, NULL);
      if (code) goto _err;

      code = tsdbReadDelIdx(pWriter->pDelFReader, pWriter->aDelIdxR);
      if (code) goto _err;
    }

    // writer
    SDelFile delFile = {.commitID = pWriter->commitID, .offset = 0, .size = 0};
    code = tsdbDelFWriterOpen(&pWriter->pDelFWriter, &delFile, pTsdb);
    if (code) goto _err;
  }

  // process the del data
  TABLEID id = *(TABLEID*)(pData + sizeof(SSnapDataHdr));

  while (true) {
    SDelIdx* pDelIdx = NULL;
    int64_t  n = sizeof(SSnapDataHdr) + sizeof(TABLEID);
    SDelData delData;
    SDelIdx  delIdx;
    int8_t   toBreak = 0;

    if (pWriter->iDelIdx < taosArrayGetSize(pWriter->aDelIdxR)) {
      pDelIdx = (SDelIdx*)taosArrayGet(pWriter->aDelIdxR, pWriter->iDelIdx);
    }

    if (pDelIdx) {
      int32_t c = tTABLEIDCmprFn(&id, pDelIdx);
      if (c < 0) {
        goto _new_del;
      } else {
        code = tsdbReadDelData(pWriter->pDelFReader, pDelIdx, pWriter->aDelData);
        if (code) goto _err;

        pWriter->iDelIdx++;
        if (c == 0) {
          toBreak = 1;
          delIdx = (SDelIdx){.suid = id.suid, .uid = id.uid};
          goto _merge_del;
        } else {
          delIdx = (SDelIdx){.suid = pDelIdx->suid, .uid = pDelIdx->uid};
          goto _write_del;
        }
      }
    }

  _new_del:
    toBreak = 1;
    delIdx = (SDelIdx){.suid = id.suid, .uid = id.uid};
    taosArrayClear(pWriter->aDelData);

  _merge_del:
    while (n < nData) {
      n += tGetDelData(pData + n, &delData);
      if (taosArrayPush(pWriter->aDelData, &delData) == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto _err;
      }
    }

  _write_del:
    code = tsdbWriteDelData(pWriter->pDelFWriter, pWriter->aDelData, &delIdx);
    if (code) goto _err;

    if (taosArrayPush(pWriter->aDelIdxW, &delIdx) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }

    if (toBreak) break;
  }

_exit:
  return code;

_err:
  tsdbError("vgId:%d, vnode snapshot tsdb write del for %s failed since %s", TD_VID(pTsdb->pVnode), pTsdb->path,
            tstrerror(code));
  return code;
}
#endif

#if 0
static int32_t tsdbSnapWriteDelEnd(STsdbSnapWriter* pWriter) {
  int32_t code = 0;
  STsdb*  pTsdb = pWriter->pTsdb;

  if (pWriter->pDelFWriter == NULL) goto _exit;

  for (; pWriter->iDelIdx < taosArrayGetSize(pWriter->aDelIdxR); pWriter->iDelIdx++) {
    SDelIdx* pDelIdx = (SDelIdx*)taosArrayGet(pWriter->aDelIdxR, pWriter->iDelIdx);

    code = tsdbReadDelData(pWriter->pDelFReader, pDelIdx, pWriter->aDelData);
    if (code) goto _err;

    SDelIdx delIdx = (SDelIdx){.suid = pDelIdx->suid, .uid = pDelIdx->uid};
    code = tsdbWriteDelData(pWriter->pDelFWriter, pWriter->aDelData, &delIdx);
    if (code) goto _err;

    if (taosArrayPush(pWriter->aDelIdxR, &delIdx) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
  }

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

_exit:
  tsdbInfo("vgId:%d, vnode snapshot tsdb write del for %s end", TD_VID(pTsdb->pVnode), pTsdb->path);
  return code;

_err:
  tsdbError("vgId:%d, vnode snapshot tsdb write del end for %s failed since %s", TD_VID(pTsdb->pVnode), pTsdb->path,
            tstrerror(code));
  return code;
}
#endif

int32_t tsdbSnapWriterOpen(STsdb* pTsdb, int64_t sver, int64_t ever, STsdbSnapWriter** ppWriter) {
  int32_t code = 0;
#if 0
  STsdbSnapWriter* pWriter = NULL;

  // alloc
  pWriter = (STsdbSnapWriter*)taosMemoryCalloc(1, sizeof(*pWriter));
  if (pWriter == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pWriter->pTsdb = pTsdb;
  pWriter->sver = sver;
  pWriter->ever = ever;

  code = tsdbFSCopy(pTsdb, &pWriter->fs);
  if (code) goto _err;

  // config
  pWriter->minutes = pTsdb->keepCfg.days;
  pWriter->precision = pTsdb->keepCfg.precision;
  pWriter->minRow = pTsdb->pVnode->config.tsdbCfg.minRows;
  pWriter->maxRow = pTsdb->pVnode->config.tsdbCfg.maxRows;
  pWriter->cmprAlg = pTsdb->pVnode->config.tsdbCfg.compression;
  pWriter->commitID = pTsdb->pVnode->state.commitID;

  // for data file
  code = tBlockDataCreate(&pWriter->bData);

  if (code) goto _err;
  pWriter->aBlockIdx = taosArrayInit(0, sizeof(SBlockIdx));
  if (pWriter->aBlockIdx == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  code = tBlockDataCreate(&pWriter->bDataR);
  if (code) goto _err;

  pWriter->aBlockIdxW = taosArrayInit(0, sizeof(SBlockIdx));
  if (pWriter->aBlockIdxW == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  code = tBlockDataCreate(&pWriter->bDataW);
  if (code) goto _err;

  // for del file
  pWriter->aDelIdxR = taosArrayInit(0, sizeof(SDelIdx));
  if (pWriter->aDelIdxR == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pWriter->aDelData = taosArrayInit(0, sizeof(SDelData));
  if (pWriter->aDelData == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  pWriter->aDelIdxW = taosArrayInit(0, sizeof(SDelIdx));
  if (pWriter->aDelIdxW == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  *ppWriter = pWriter;

  tsdbInfo("vgId:%d, tsdb snapshot writer open for %s succeed", TD_VID(pTsdb->pVnode), pTsdb->path);
  return code;
_err:
  tsdbError("vgId:%d, tsdb snapshot writer open for %s failed since %s", TD_VID(pTsdb->pVnode), pTsdb->path,
            tstrerror(code));
  *ppWriter = NULL;
#endif
  return code;
}

int32_t tsdbSnapWriterClose(STsdbSnapWriter** ppWriter, int8_t rollback) {
  int32_t code = 0;
#if 0
  STsdbSnapWriter* pWriter = *ppWriter;

  if (rollback) {
    ASSERT(0);
    // code = tsdbFSRollback(pWriter->pTsdb->pFS);
    // if (code) goto _err;
  } else {
    code = tsdbSnapWriteDataEnd(pWriter);
    if (code) goto _err;

    code = tsdbSnapWriteDelEnd(pWriter);
    if (code) goto _err;

    code = tsdbFSCommit1(pWriter->pTsdb, &pWriter->fs);
    if (code) goto _err;

    code = tsdbFSCommit2(pWriter->pTsdb, &pWriter->fs);
    if (code) goto _err;
  }

  tsdbInfo("vgId:%d, vnode snapshot tsdb writer close for %s", TD_VID(pWriter->pTsdb->pVnode), pWriter->pTsdb->path);
  taosMemoryFree(pWriter);
  *ppWriter = NULL;
  return code;

_err:
  tsdbError("vgId:%d, vnode snapshot tsdb writer close for %s failed since %s", TD_VID(pWriter->pTsdb->pVnode),
            pWriter->pTsdb->path, tstrerror(code));
  taosMemoryFree(pWriter);
  *ppWriter = NULL;
#endif
  return code;
}

int32_t tsdbSnapWrite(STsdbSnapWriter* pWriter, uint8_t* pData, uint32_t nData) {
  int32_t code = 0;
#if 0
  SSnapDataHdr* pHdr = (SSnapDataHdr*)pData;

  // ts data
  if (pHdr->type == SNAP_DATA_TSDB) {
    code = tsdbSnapWriteData(pWriter, pData, nData);
    if (code) goto _err;

    goto _exit;
  } else {
    if (pWriter->pDataFWriter) {
      code = tsdbSnapWriteDataEnd(pWriter);
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
#endif
  return code;
}
