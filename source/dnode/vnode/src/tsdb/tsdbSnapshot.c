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
#include "tsdbDataFileRW.h"
#include "tsdbFS2.h"
#include "tsdbIter.h"
#include "tsdbSttFileRW.h"

extern int32_t tsdbUpdateTableSchema(SMeta* pMeta, int64_t suid, int64_t uid, SSkmInfo* pSkmInfo);
extern int32_t tsdbWriteDataBlock(SDataFWriter* pWriter, SBlockData* pBlockData, SMapData* mDataBlk, int8_t cmprAlg);
extern int32_t tsdbWriteSttBlock(SDataFWriter* pWriter, SBlockData* pBlockData, SArray* aSttBlk, int8_t cmprAlg);

// STsdbSnapReader ========================================
struct STsdbSnapReader {
  STsdb*  tsdb;
  int64_t sver;
  int64_t ever;
  int8_t  type;

  uint8_t* aBuf[5];
  SSkmInfo skmTb[1];

  TFileSetArray* fsetArr;

  // context
  struct {
    int32_t    fsetArrIdx;
    STFileSet* fset;
    bool       isDataDone;
    bool       isTombDone;
  } ctx[1];

  // reader
  SDataFileReader*    dataReader;
  TSttFileReaderArray sttReaderArr[1];

  // iter
  TTsdbIterArray dataIterArr[1];
  SIterMerger*   dataIterMerger;
  TTsdbIterArray tombIterArr[1];
  SIterMerger*   tombIterMerger;

  // data
  SBlockData blockData[1];
  STombBlock tombBlock[1];
};

static int32_t tsdbSnapReadFileSetOpenReader(STsdbSnapReader* reader) {
  int32_t code = 0;
  int32_t lino = 0;

  ASSERT(reader->dataReader == NULL);
  ASSERT(TARRAY2_SIZE(reader->sttReaderArr) == 0);

  // data
  SDataFileReaderConfig config = {
      .tsdb = reader->tsdb,
      .szPage = reader->tsdb->pVnode->config.tsdbPageSize,
      .bufArr = reader->aBuf,
  };
  bool hasDataFile = false;
  for (int32_t ftype = 0; ftype < TSDB_FTYPE_MAX; ftype++) {
    if (reader->ctx->fset->farr[ftype] != NULL) {
      hasDataFile = true;
      config.files[ftype].exist = true;
      config.files[ftype].file = reader->ctx->fset->farr[ftype]->f[0];
    }
  }

  if (hasDataFile) {
    code = tsdbDataFileReaderOpen(NULL, &config, &reader->dataReader);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // stt
  SSttLvl* lvl;
  TARRAY2_FOREACH(reader->ctx->fset->lvlArr, lvl) {
    STFileObj* fobj;
    TARRAY2_FOREACH(lvl->fobjArr, fobj) {
      SSttFileReader*      sttReader;
      SSttFileReaderConfig config = {
          .tsdb = reader->tsdb,
          .szPage = reader->tsdb->pVnode->config.tsdbPageSize,
          .file = fobj->f[0],
          .bufArr = reader->aBuf,
      };

      code = tsdbSttFileReaderOpen(fobj->fname, &config, &sttReader);
      TSDB_CHECK_CODE(code, lino, _exit);

      code = TARRAY2_APPEND(reader->sttReaderArr, sttReader);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(reader->tsdb->pVnode), code, lino);
  }
  return code;
}

static int32_t tsdbSnapReadFileSetCloseReader(STsdbSnapReader* reader) {
  int32_t code = 0;
  int32_t lino = 0;

  TARRAY2_CLEAR(reader->sttReaderArr, tsdbSttFileReaderClose);
  tsdbDataFileReaderClose(&reader->dataReader);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(reader->tsdb->pVnode), code, lino);
  }
  return code;
}

static int32_t tsdbSnapReadFileSetOpenIter(STsdbSnapReader* reader) {
  int32_t code = 0;
  int32_t lino = 0;

  ASSERT(reader->dataIterMerger == NULL);
  ASSERT(reader->tombIterMerger == NULL);
  ASSERT(TARRAY2_SIZE(reader->dataIterArr) == 0);
  ASSERT(TARRAY2_SIZE(reader->tombIterArr) == 0);

  STsdbIter*      iter;
  STsdbIterConfig config = {
      .filterByVersion = true,
      .verRange[0] = reader->sver,
      .verRange[1] = reader->ever,
  };

  // data file
  if (reader->dataReader) {
    // data
    config.type = TSDB_ITER_TYPE_DATA;
    config.dataReader = reader->dataReader;

    code = tsdbIterOpen(&config, &iter);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = TARRAY2_APPEND(reader->dataIterArr, iter);
    TSDB_CHECK_CODE(code, lino, _exit);

    // tomb
    config.type = TSDB_ITER_TYPE_DATA_TOMB;
    config.dataReader = reader->dataReader;

    code = tsdbIterOpen(&config, &iter);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = TARRAY2_APPEND(reader->tombIterArr, iter);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // stt file
  SSttFileReader* sttReader;
  TARRAY2_FOREACH(reader->sttReaderArr, sttReader) {
    // data
    config.type = TSDB_ITER_TYPE_STT;
    config.sttReader = sttReader;

    code = tsdbIterOpen(&config, &iter);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = TARRAY2_APPEND(reader->dataIterArr, iter);
    TSDB_CHECK_CODE(code, lino, _exit);

    // tomb
    config.type = TSDB_ITER_TYPE_STT_TOMB;
    config.sttReader = sttReader;

    code = tsdbIterOpen(&config, &iter);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = TARRAY2_APPEND(reader->tombIterArr, iter);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // merger
  code = tsdbIterMergerOpen(reader->dataIterArr, &reader->dataIterMerger, false);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbIterMergerOpen(reader->tombIterArr, &reader->dataIterMerger, true);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(reader->tsdb->pVnode), code, lino);
  }
  return code;
}

static int32_t tsdbSnapReadFileSetCloseIter(STsdbSnapReader* reader) {
  tsdbIterMergerClose(&reader->dataIterMerger);
  tsdbIterMergerClose(&reader->tombIterMerger);
  TARRAY2_CLEAR(reader->dataIterArr, tsdbIterClose);
  TARRAY2_CLEAR(reader->tombIterArr, tsdbIterClose);
  return 0;
}

static int32_t tsdbSnapReadFileSetBegin(STsdbSnapReader* reader) {
  int32_t code = 0;
  int32_t lino = 0;

  if (reader->ctx->fsetArrIdx < TARRAY2_SIZE(reader->fsetArr)) {
    reader->ctx->fset = TARRAY2_GET(reader->fsetArr, reader->ctx->fsetArrIdx++);
    reader->ctx->isDataDone = false;
    reader->ctx->isTombDone = false;

    code = tsdbSnapReadFileSetOpenReader(reader);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbSnapReadFileSetOpenIter(reader);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(reader->tsdb->pVnode), code, lino);
  }
  return code;
}

static int32_t tsdbSnapReadFileSetEnd(STsdbSnapReader* reader) {
  tsdbSnapReadFileSetCloseIter(reader);
  tsdbSnapReadFileSetCloseReader(reader);
  reader->ctx->fset = NULL;
  return 0;
}

static int32_t tsdbSnapCmprData(STsdbSnapReader* reader, uint8_t** data) {
  int32_t code = 0;
  int32_t lino = 0;

  int32_t aBufN[5] = {0};
  code = tCmprBlockData(reader->blockData, NO_COMPRESSION, NULL, NULL, reader->aBuf, aBufN);
  TSDB_CHECK_CODE(code, lino, _exit);

  int32_t size = aBufN[0] + aBufN[1] + aBufN[2] + aBufN[3];
  *data = taosMemoryMalloc(sizeof(SSnapDataHdr) + size);
  if (*data == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  SSnapDataHdr* pHdr = (SSnapDataHdr*)*data;
  pHdr->type = reader->type;
  pHdr->size = size;

  memcpy(pHdr->data, reader->aBuf[3], aBufN[3]);
  memcpy(pHdr->data + aBufN[3], reader->aBuf[2], aBufN[2]);
  if (aBufN[1]) {
    memcpy(pHdr->data + aBufN[3] + aBufN[2], reader->aBuf[1], aBufN[1]);
  }
  if (aBufN[0]) {
    memcpy(pHdr->data + aBufN[3] + aBufN[2] + aBufN[1], reader->aBuf[0], aBufN[0]);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(reader->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbSnapReadTimeSeriesData(STsdbSnapReader* reader, uint8_t** data) {
  int32_t code = 0;
  int32_t lino = 0;

  tBlockDataReset(reader->blockData);

  for (SRowInfo* row; (row = tsdbIterMergerGetData(reader->dataIterMerger));) {
    if (reader->blockData->suid == 0 && reader->blockData->uid == 0) {
      code = tsdbUpdateSkmTb(reader->tsdb, (TABLEID*)row, reader->skmTb);
      TSDB_CHECK_CODE(code, lino, _exit);

      TABLEID tbid = {
          .suid = row->suid,
          .uid = row->suid ? 0 : row->uid,
      };
      code = tBlockDataInit(reader->blockData, &tbid, reader->skmTb->pTSchema, NULL, 0);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    if (!TABLE_SAME_SCHEMA(reader->blockData->suid, reader->blockData->uid, row->suid, row->uid)) {
      break;
    }

    code = tBlockDataAppendRow(reader->blockData, &row->row, NULL, row->uid);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbIterMergerNext(reader->dataIterMerger);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (reader->blockData->nRow >= 81920) {
      break;
    }
  }

  if (reader->blockData->nRow > 0) {
    ASSERT(reader->blockData->suid || reader->blockData->uid);
    code = tsdbSnapCmprData(reader, data);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(reader->tsdb->pVnode), code, lino);
  }
  return code;
}

static int32_t tsdbSnapCmprTombData(STsdbSnapReader* reader, uint8_t** data) {
  int32_t code = 0;
  int32_t lino = 0;

  int64_t size = sizeof(SSnapDataHdr);
  for (int32_t i = 0; i < ARRAY_SIZE(reader->tombBlock->dataArr); i++) {
    size += TARRAY2_DATA_LEN(reader->tombBlock->dataArr + i);
  }

  data[0] = taosMemoryMalloc(size);
  if (data[0] == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  SSnapDataHdr* hdr = (SSnapDataHdr*)data[0];
  hdr->type = SNAP_DATA_DEL;
  hdr->size = size;

  uint8_t* tdata = hdr->data;
  for (int32_t i = 0; i < TARRAY_SIZE(reader->tombBlock->dataArr); i++) {
    memcpy(tdata, TARRAY2_DATA(reader->tombBlock->dataArr + i), TARRAY2_DATA_LEN(reader->tombBlock->dataArr + i));
    tdata += TARRAY2_DATA_LEN(reader->tombBlock->dataArr + i);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(reader->tsdb->pVnode), code, lino);
  }
  return code;
}

static int32_t tsdbSnapReadTombData(STsdbSnapReader* reader, uint8_t** data) {
  int32_t code = 0;
  int32_t lino = 0;

  tTombBlockClear(reader->tombBlock);

  for (STombRecord* record; (record = tsdbIterMergerGetTombRecord(reader->tombIterMerger)) != NULL;) {
    code = tTombBlockPut(reader->tombBlock, record);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (TOMB_BLOCK_SIZE(reader->tombBlock) >= 81920) {
      break;
    }
  }

  if (TOMB_BLOCK_SIZE(reader->tombBlock) > 0) {
    code = tsdbSnapCmprTombData(reader, data);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(reader->tsdb->pVnode), code, lino);
  }
  return code;
}

int32_t tsdbSnapReaderOpen(STsdb* tsdb, int64_t sver, int64_t ever, int8_t type, STsdbSnapReader** reader) {
  int32_t code = 0;
  int32_t lino = 0;

  // alloc
  reader[0] = (STsdbSnapReader*)taosMemoryCalloc(1, sizeof(*reader[0]));
  if (reader[0] == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  reader[0]->tsdb = tsdb;
  reader[0]->sver = sver;
  reader[0]->ever = ever;
  reader[0]->type = type;

  code = tsdbFSCreateRefSnapshot(tsdb->pFS, &reader[0]->fsetArr);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s, sver:%" PRId64 " ever:%" PRId64 " type:%d", TD_VID(tsdb->pVnode),
              __func__, lino, tstrerror(code), sver, ever, type);
    tsdbFSDestroyRefSnapshot(&reader[0]->fsetArr);
    taosMemoryFree(reader[0]);
    reader[0] = NULL;
  } else {
    tsdbInfo("vgId:%d %s done, sver:%" PRId64 " ever:%" PRId64 " type:%d", TD_VID(tsdb->pVnode), __func__, sver, ever,
             type);
  }
  return code;
}

int32_t tsdbSnapReaderClose(STsdbSnapReader** reader) {
  if (reader[0] == NULL) return 0;

  int32_t code = 0;
  int32_t lino = 0;

  STsdb* tsdb = reader[0]->tsdb;

  tTombBlockDestroy(reader[0]->tombBlock);
  tBlockDataDestroy(reader[0]->blockData);

  tsdbIterMergerClose(&reader[0]->dataIterMerger);
  tsdbIterMergerClose(&reader[0]->tombIterMerger);
  TARRAY2_DESTROY(reader[0]->dataIterArr, tsdbIterClose);
  TARRAY2_DESTROY(reader[0]->tombIterArr, tsdbIterClose);
  TARRAY2_DESTROY(reader[0]->sttReaderArr, tsdbSttFileReaderClose);
  tsdbDataFileReaderClose(&reader[0]->dataReader);

  tsdbFSDestroyRefSnapshot(&reader[0]->fsetArr);
  tDestroyTSchema(reader[0]->skmTb->pTSchema);

  for (int32_t i = 0; i < ARRAY_SIZE(reader[0]->aBuf);) {
    tFree(reader[0]->aBuf[i]);
  }

  taosMemoryFree(reader[0]);
  reader[0] = NULL;

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(tsdb->pVnode), lino, code);
  } else {
    tsdbDebug("vgId:%d %s done", TD_VID(tsdb->pVnode), __func__);
  }
  return code;
}

int32_t tsdbSnapRead(STsdbSnapReader* reader, uint8_t** data) {
  int32_t code = 0;
  int32_t lino = 0;

  data[0] = NULL;

  for (;;) {
    if (reader->ctx->fset == NULL) {
      code = tsdbSnapReadFileSetBegin(reader);
      TSDB_CHECK_CODE(code, lino, _exit);

      if (reader->ctx->fset == NULL) {
        break;
      }
    }

    if (!reader->ctx->isDataDone) {
      code = tsdbSnapReadTimeSeriesData(reader, data);
      TSDB_CHECK_CODE(code, lino, _exit);
      if (data[0]) {
        goto _exit;
      } else {
        reader->ctx->isDataDone = true;
      }
    }

    if (!reader->ctx->isTombDone) {
      code = tsdbSnapReadTombData(reader, data);
      TSDB_CHECK_CODE(code, lino, _exit);
      if (data[0]) {
        goto _exit;
      } else {
        reader->ctx->isTombDone = true;
      }
    }

    code = tsdbSnapReadFileSetEnd(reader);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(reader->tsdb->pVnode), code, lino);
  } else {
    tsdbDebug("vgId:%d %s done", TD_VID(reader->tsdb->pVnode), __func__);
  }
  return code;
}

// STsdbSnapWriter ========================================
struct STsdbSnapWriter {
  STsdb*   tsdb;
  int64_t  sver;
  int64_t  ever;
  int32_t  minutes;
  int8_t   precision;
  int32_t  minRow;
  int32_t  maxRow;
  int8_t   cmprAlg;
  int64_t  commitID;
  uint8_t* aBuf[5];

  TFileSetArray* fsetArr;
  TFileOpArray   fopArr[1];

  struct {
    bool fsetWriteBegin;

    int32_t    fid;
    STFileSet* fset;

    bool hasData;
    bool hasTomb;

    // reader
    SDataFileReader*    dataReader;
    TSttFileReaderArray sttReaderArr[1];

    // iter/merger
    TTsdbIterArray dataIterArr[1];
    SIterMerger*   dataIterMerger;
    TTsdbIterArray tombIterArr[1];
    SIterMerger*   tombIterMerger;
  } ctx[1];

  SDataFileWriter* dataWriter;
  SSttFileWriter*  sttWriter;

  SBlockData blockData[1];
  STombBlock tombBlock[1];
};

#if 0
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
    code = tsdbUpdateTableSchema(pWriter->tsdb->pVnode->pMeta, pId->suid, pId->uid, &pWriter->skmTable);
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
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pWriter->tsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbTrace("vgId:%d %s done, suid:%" PRId64 " uid:%" PRId64, TD_VID(pWriter->tsdb->pVnode), __func__,
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
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pWriter->tsdb->pVnode), __func__, lino, tstrerror(code));
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
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pWriter->tsdb->pVnode), __func__, lino, tstrerror(code));
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
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pWriter->tsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbSnapWriteFileDataStart(STsdbSnapWriter* pWriter, int32_t fid) {
  int32_t code = 0;
  int32_t lino = 0;

  ASSERT(pWriter->pDataFWriter == NULL && pWriter->fid < fid);

  STsdb* pTsdb = pWriter->tsdb;

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
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pWriter->tsdb->pVnode), __func__, lino, tstrerror(code));
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
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pWriter->tsdb->pVnode), __func__, lino, tstrerror(code));
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
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pWriter->tsdb->pVnode), __func__, lino, tstrerror(code));
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
    tsdbError("vgId:%d %s failed since %s", TD_VID(pWriter->tsdb->pVnode), __func__, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s is done", TD_VID(pWriter->tsdb->pVnode), __func__);
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
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pWriter->tsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s done, suid:%" PRId64 " uid:%" PRId64 " nRow:%d", TD_VID(pWriter->tsdb->pVnode), __func__,
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
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pWriter->tsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbTrace("vgId:%d %s done, suid:%" PRId64 " uid:%" PRId64, TD_VID(pWriter->tsdb->pVnode), __func__,
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
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pWriter->tsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbTrace("vgId:%d %s done", TD_VID(pWriter->tsdb->pVnode), __func__);
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
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pWriter->tsdb->pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbSnapWriteDelDataStart(STsdbSnapWriter* pWriter) {
  int32_t code = 0;
  int32_t lino = 0;

  STsdb*    pTsdb = pWriter->tsdb;
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

  STsdb* pTsdb = pWriter->tsdb;

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

  STsdb* pTsdb = pWriter->tsdb;

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
#endif

// APIs
int32_t tsdbSnapWriterOpen(STsdb* pTsdb, int64_t sver, int64_t ever, STsdbSnapWriter** ppWriter) {
  int32_t code = 0;
  int32_t lino = 0;

#if 0
  // alloc
  STsdbSnapWriter* pWriter = (STsdbSnapWriter*)taosMemoryCalloc(1, sizeof(*pWriter));
  if (pWriter == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  pWriter->tsdb = pTsdb;
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
#endif

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
    // if (pWriter) {
    //   tBlockDataDestroy(&pWriter->sData);
    //   tBlockDataDestroy(&pWriter->bData);
    //   tBlockDataDestroy(&pWriter->inData);
    //   tsdbFSDestroy(&pWriter->fs);
    //   taosMemoryFree(pWriter);
    //   pWriter = NULL;
    // }
  } else {
    tsdbInfo("vgId:%d %s done, sver:%" PRId64 " ever:%" PRId64, TD_VID(pTsdb->pVnode), __func__, sver, ever);
  }
  // *ppWriter = pWriter;
  return code;
}

int32_t tsdbSnapWriterPrepareClose(STsdbSnapWriter* writer) {
  int32_t code = 0;
  int32_t lino = 0;

#if 0
  if (pWriter->pDataFWriter) {
    code = tsdbSnapWriteFileDataEnd(pWriter);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (pWriter->pDelFWriter) {
    code = tsdbSnapWriteDelDataEnd(pWriter);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tsdbFSPrepareCommit(pWriter->tsdb, &pWriter->fs);
  TSDB_CHECK_CODE(code, lino, _exit);
#endif

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(writer->tsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbDebug("vgId:%d %s done", TD_VID(writer->tsdb->pVnode), __func__);
  }
  return code;
}

int32_t tsdbSnapWriterClose(STsdbSnapWriter** writer, int8_t rollback) {
  int32_t code = 0;
  int32_t lino = 0;

#if 0
  STsdbSnapWriter* pWriter = *writer;
  STsdb*           pTsdb = pWriter->tsdb;

  if (rollback) {
    tsdbRollbackCommit(pWriter->tsdb);
  } else {
    // lock
    taosThreadRwlockWrlock(&pTsdb->rwLock);

    code = tsdbFSCommit(pWriter->tsdb);
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
  *writer = NULL;
#endif

_exit:
  if (code) {
    // tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    // tsdbInfo("vgId:%d %s done", TD_VID(pTsdb->pVnode), __func__);
  }
  return code;
}

static int32_t tsdbSnapWriteDoWriteTimeSeriesRow(STsdbSnapWriter* writer, const SRowInfo* row) {
  int32_t code = 0;
  int32_t lino = 0;

  // TODO

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbSnapWriteTimeSeriesRow(STsdbSnapWriter* writer, const SRowInfo* row) {
  int32_t code = 0;
  int32_t lino = 0;

  while (writer->ctx->hasData) {
    SRowInfo* row1 = tsdbIterMergerGetData(writer->ctx->dataIterMerger);
    if (row1 == NULL) {
      writer->ctx->hasData = false;
      break;
    }

    int32_t c = tRowInfoCmprFn(row1, row);
    if (c <= 0) {
      code = tsdbSnapWriteDoWriteTimeSeriesRow(writer, row1);
      TSDB_CHECK_CODE(code, lino, _exit);

      code = tsdbIterMergerNext(writer->ctx->dataIterMerger);
      TSDB_CHECK_CODE(code, lino, _exit);
    } else {
      break;
    }
  }

  if (row->suid == INT64_MAX) {
    ASSERT(writer->ctx->hasData == false);
    goto _exit;
  }

  code = tsdbSnapWriteDoWriteTimeSeriesRow(writer, row);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbSnapWriteFileSetBegin(STsdbSnapWriter* writer, int32_t fid) {
  int32_t code = 0;
  int32_t lino = 0;

  // TODO

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbSnapWriteTombRecord(STsdbSnapWriter* writer, const STombRecord* record) {
  int32_t code = 0;
  int32_t lino = 0;

  // TODO

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbSnapWriteFileSetEnd(STsdbSnapWriter* writer) {
  if (!writer->ctx->fsetWriteBegin) return 0;

  int32_t code = 0;
  int32_t lino = 0;

  // TODO
  SRowInfo row = {
      .suid = INT64_MAX,
      .uid = INT64_MAX,
  };

  code = tsdbSnapWriteTimeSeriesRow(writer, &row);
  TSDB_CHECK_CODE(code, lino, _exit);

  STombRecord record = {
      .suid = INT64_MAX,
      .uid = INT64_MAX,
  };

  code = tsdbSnapWriteTombRecord(writer, &record);
  TSDB_CHECK_CODE(code, lino, _exit);

  // close write
  code = tsdbSttFileWriterClose(&writer->sttWriter, 0, writer->fopArr);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbDataFileWriterClose(&writer->dataWriter, 0, writer->fopArr);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbSnapWriteTimeSeriesData(STsdbSnapWriter* writer, SSnapDataHdr* hdr) {
  int32_t code = 0;
  int32_t lino = 0;

  SBlockData blockData[1] = {0};

  code = tDecmprBlockData(hdr->data, hdr->size, blockData, writer->aBuf);
  TSDB_CHECK_CODE(code, lino, _exit);

  int32_t fid = tsdbKeyFid(blockData->aTSKEY[0], writer->minutes, writer->precision);
  if (fid != writer->ctx->fid) {
    code = tsdbSnapWriteFileSetEnd(writer);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbSnapWriteFileSetBegin(writer, fid);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  for (int32_t i = 0; i < blockData->nRow; ++i) {
    SRowInfo rowInfo = {
        .suid = blockData->suid,
        .uid = blockData->uid ? blockData->uid : blockData->aUid[i],
        .row = tsdbRowFromBlockData(blockData, i),
    };

    code = tsdbSnapWriteTimeSeriesRow(writer, &rowInfo);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->tsdb->pVnode), lino, code);
  } else {
    tsdbDebug("vgId:%d %s done, suid:%" PRId64 " uid:%" PRId64 " nRow:%d", TD_VID(writer->tsdb->pVnode), __func__,
              blockData->suid, blockData->uid, blockData->nRow);
  }
  tBlockDataDestroy(blockData);
  return code;
}

static int32_t tsdbSnapWriteDecmprTombBlock(SSnapDataHdr* hdr, STombBlock* tombBlock) {
  int32_t code = 0;
  int32_t lino = 0;

  // TODO

_exit:
  return code;
}

static int32_t tsdbSnapWriteTombData(STsdbSnapWriter* writer, SSnapDataHdr* hdr) {
  int32_t code = 0;
  int32_t lino = 0;

  STombBlock tombBlock[1] = {0};

  code = tsdbSnapWriteDecmprTombBlock(hdr, tombBlock);
  TSDB_CHECK_CODE(code, lino, _exit);

  for (int32_t i = 0; i < TOMB_BLOCK_SIZE(tombBlock); ++i) {
    STombRecord record;
    tTombBlockGet(tombBlock, i, &record);

    code = tsdbSnapWriteTombRecord(writer, &record);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  tTombBlockDestroy(tombBlock);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbSnapWrite(STsdbSnapWriter* writer, SSnapDataHdr* hdr) {
  int32_t code = 0;
  int32_t lino = 0;

  if (hdr->type == SNAP_DATA_TSDB) {
    code = tsdbSnapWriteTimeSeriesData(writer, hdr);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else if (hdr->type == SNAP_DATA_DEL) {
    code = tsdbSnapWriteTombData(writer, hdr);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    ASSERT(0);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s, type:%d index:%" PRId64 " size:%" PRId64,
              TD_VID(writer->tsdb->pVnode), __func__, lino, tstrerror(code), hdr->type, hdr->index, hdr->size);
  } else {
    tsdbDebug("vgId:%d %s done, type:%d index:%" PRId64 " size:%" PRId64, TD_VID(writer->tsdb->pVnode), __func__,
              hdr->type, hdr->index, hdr->size);
  }
  return code;
}
