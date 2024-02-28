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
#include "tsdbFSetRW.h"
#include "tsdbIter.h"
#include "tsdbSttFileRW.h"

extern int32_t tsdbUpdateTableSchema(SMeta* pMeta, int64_t suid, int64_t uid, SSkmInfo* pSkmInfo);

// STsdbSnapReader ========================================
struct STsdbSnapReader {
  STsdb*  tsdb;
  int64_t sver;
  int64_t ever;
  int8_t  type;

  uint8_t* aBuf[5];
  SSkmInfo skmTb[1];

  TFileSetRangeArray* fsrArr;

  // context
  struct {
    int32_t      fsrArrIdx;
    STFileSetRange* fsr;
    bool         isDataDone;
    bool         isTombDone;
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
    if (reader->ctx->fsr->fset->farr[ftype] != NULL) {
      hasDataFile = true;
      config.files[ftype].exist = true;
      config.files[ftype].file = reader->ctx->fsr->fset->farr[ftype]->f[0];
    }
  }

  if (hasDataFile) {
    code = tsdbDataFileReaderOpen(NULL, &config, &reader->dataReader);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // stt
  SSttLvl* lvl;
  TARRAY2_FOREACH(reader->ctx->fsr->fset->lvlArr, lvl) {
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
      .verRange[0] = reader->ctx->fsr->sver,
      .verRange[1] = reader->ctx->fsr->ever,
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

  code = tsdbIterMergerOpen(reader->tombIterArr, &reader->tombIterMerger, true);
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

static int32_t tsdbSnapReadRangeBegin(STsdbSnapReader* reader) {
  int32_t code = 0;
  int32_t lino = 0;

  ASSERT(reader->ctx->fsr == NULL);

  if (reader->ctx->fsrArrIdx < TARRAY2_SIZE(reader->fsrArr)) {
    reader->ctx->fsr = TARRAY2_GET(reader->fsrArr, reader->ctx->fsrArrIdx++);
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

static int32_t tsdbSnapReadRangeEnd(STsdbSnapReader* reader) {
  tsdbSnapReadFileSetCloseIter(reader);
  tsdbSnapReadFileSetCloseReader(reader);
  reader->ctx->fsr = NULL;
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

static int64_t tBlockDataSize(SBlockData* pBlockData) {
  int64_t nData = 0;
  for (int32_t iCol = 0; iCol < pBlockData->nColData; iCol++) {
    SColData* pColData = tBlockDataGetColDataByIdx(pBlockData, iCol);
    nData += pColData->nData;
  }
  return nData;
}

static int32_t tsdbSnapReadTimeSeriesData(STsdbSnapReader* reader, uint8_t** data) {
  int32_t   code = 0;
  int32_t   lino = 0;
  SMetaInfo info;

  tBlockDataReset(reader->blockData);

  TABLEID tbid[1] = {0};
  for (SRowInfo* row; (row = tsdbIterMergerGetData(reader->dataIterMerger));) {
    // skip dropped table
    if (row->uid != tbid->uid) {
      tbid->suid = row->suid;
      tbid->uid = row->uid;
      if (metaGetInfo(reader->tsdb->pVnode->pMeta, tbid->uid, &info, NULL) != 0) {
        code = tsdbIterMergerSkipTableData(reader->dataIterMerger, tbid);
        TSDB_CHECK_CODE(code, lino, _exit);
        continue;
      }
    }

    if (reader->blockData->suid == 0 && reader->blockData->uid == 0) {
      code = tsdbUpdateSkmTb(reader->tsdb, (TABLEID*)row, reader->skmTb);
      TSDB_CHECK_CODE(code, lino, _exit);

      TABLEID tbid1 = {
          .suid = row->suid,
          .uid = row->suid ? 0 : row->uid,
      };
      code = tBlockDataInit(reader->blockData, &tbid1, reader->skmTb->pTSchema, NULL, 0);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    if (!TABLE_SAME_SCHEMA(reader->blockData->suid, reader->blockData->uid, row->suid, row->uid)) {
      break;
    }

    code = tBlockDataAppendRow(reader->blockData, &row->row, NULL, row->uid);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbIterMergerNext(reader->dataIterMerger);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (!(reader->blockData->nRow % 16)) {
      int64_t nData = tBlockDataSize(reader->blockData);
      if (nData >= TSDB_SNAP_DATA_PAYLOAD_SIZE) {
        break;
      }
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

  int64_t size = 0;
  for (int32_t i = 0; i < ARRAY_SIZE(reader->tombBlock->dataArr); i++) {
    size += TARRAY2_DATA_LEN(reader->tombBlock->dataArr + i);
  }

  data[0] = taosMemoryMalloc(size + sizeof(SSnapDataHdr));
  if (data[0] == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  SSnapDataHdr* hdr = (SSnapDataHdr*)(data[0]);
  hdr->type = SNAP_DATA_DEL;
  hdr->size = size;

  uint8_t* tdata = hdr->data;
  for (int32_t i = 0; i < ARRAY_SIZE(reader->tombBlock->dataArr); i++) {
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
  int32_t   code = 0;
  int32_t   lino = 0;
  SMetaInfo info;

  tTombBlockClear(reader->tombBlock);

  TABLEID tbid[1] = {0};
  for (STombRecord* record; (record = tsdbIterMergerGetTombRecord(reader->tombIterMerger)) != NULL;) {
    if (record->uid != tbid->uid) {
      tbid->suid = record->suid;
      tbid->uid = record->uid;
      if (metaGetInfo(reader->tsdb->pVnode->pMeta, tbid->uid, &info, NULL) != 0) {
        code = tsdbIterMergerSkipTableData(reader->tombIterMerger, tbid);
        TSDB_CHECK_CODE(code, lino, _exit);
        continue;
      }
    }

    code = tTombBlockPut(reader->tombBlock, record);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbIterMergerNext(reader->tombIterMerger);
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

int32_t tsdbSnapReaderOpen(STsdb* tsdb, int64_t sver, int64_t ever, int8_t type, void* pRanges,
                           STsdbSnapReader** reader) {
  int32_t code = 0;
  int32_t lino = 0;

  reader[0] = (STsdbSnapReader*)taosMemoryCalloc(1, sizeof(*reader[0]));
  if (reader[0] == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  reader[0]->tsdb = tsdb;
  reader[0]->sver = sver;
  reader[0]->ever = ever;
  reader[0]->type = type;

  code = tsdbFSCreateRefRangedSnapshot(tsdb->pFS, sver, ever, (TFileSetRangeArray*)pRanges, &reader[0]->fsrArr);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s, sver:%" PRId64 " ever:%" PRId64 " type:%d", TD_VID(tsdb->pVnode),
              __func__, lino, tstrerror(code), sver, ever, type);
    tsdbTFileSetRangeArrayDestroy(&reader[0]->fsrArr);
    taosMemoryFree(reader[0]);
    reader[0] = NULL;
  } else {
    tsdbInfo("vgId:%d, tsdb snapshot incremental reader opened. sver:%" PRId64 " ever:%" PRId64 " type:%d",
             TD_VID(tsdb->pVnode), sver, ever, type);
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

  tsdbFSDestroyRefRangedSnapshot(&reader[0]->fsrArr);
  tDestroyTSchema(reader[0]->skmTb->pTSchema);

  for (int32_t i = 0; i < ARRAY_SIZE(reader[0]->aBuf); ++i) {
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
    if (reader->ctx->fsr == NULL) {
      code = tsdbSnapReadRangeBegin(reader);
      TSDB_CHECK_CODE(code, lino, _exit);

      if (reader->ctx->fsr == NULL) {
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

    code = tsdbSnapReadRangeEnd(reader);
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
  int32_t  szPage;
  int64_t  compactVersion;
  int64_t  now;
  uint8_t* aBuf[5];

  TFileSetArray* fsetArr;
  TFileOpArray   fopArr[1];

  struct {
    bool       fsetWriteBegin;
    int32_t    fid;
    STFileSet* fset;
    SDiskID    did;
    bool       hasData;  // if have time series data
    bool       hasTomb;  // if have tomb data

    // reader
    SDataFileReader*    dataReader;
    TSttFileReaderArray sttReaderArr[1];

    // iter/merger
    TTsdbIterArray dataIterArr[1];
    SIterMerger*   dataIterMerger;
    TTsdbIterArray tombIterArr[1];
    SIterMerger*   tombIterMerger;

    // writer
    SFSetWriter* fsetWriter;
  } ctx[1];
};

// APIs
static int32_t tsdbSnapWriteTimeSeriesRow(STsdbSnapWriter* writer, SRowInfo* row) {
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
      code = tsdbFSetWriteRow(writer->ctx->fsetWriter, row1);
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

  code = tsdbFSetWriteRow(writer->ctx->fsetWriter, row);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbSnapWriteFileSetOpenReader(STsdbSnapWriter* writer) {
  int32_t code = 0;
  int32_t lino = 0;

  ASSERT(writer->ctx->dataReader == NULL);
  ASSERT(TARRAY2_SIZE(writer->ctx->sttReaderArr) == 0);

  if (writer->ctx->fset) {
    // open data reader
    SDataFileReaderConfig dataFileReaderConfig = {
        .tsdb = writer->tsdb,
        .bufArr = writer->aBuf,
        .szPage = writer->szPage,
    };

    for (int32_t ftype = 0; ftype < TSDB_FTYPE_MAX; ++ftype) {
      if (writer->ctx->fset->farr[ftype] == NULL) {
        continue;
      }

      dataFileReaderConfig.files[ftype].exist = true;
      dataFileReaderConfig.files[ftype].file = writer->ctx->fset->farr[ftype]->f[0];

      STFileOp fileOp = {
          .optype = TSDB_FOP_REMOVE,
          .fid = writer->ctx->fset->fid,
          .of = writer->ctx->fset->farr[ftype]->f[0],
      };

      code = TARRAY2_APPEND(writer->fopArr, fileOp);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tsdbDataFileReaderOpen(NULL, &dataFileReaderConfig, &writer->ctx->dataReader);
    TSDB_CHECK_CODE(code, lino, _exit);

    // open stt reader array
    SSttLvl* lvl;
    TARRAY2_FOREACH(writer->ctx->fset->lvlArr, lvl) {
      STFileObj* fobj;
      TARRAY2_FOREACH(lvl->fobjArr, fobj) {
        SSttFileReader*      reader;
        SSttFileReaderConfig sttFileReaderConfig = {
            .tsdb = writer->tsdb,
            .szPage = writer->szPage,
            .bufArr = writer->aBuf,
            .file = fobj->f[0],
        };

        code = tsdbSttFileReaderOpen(fobj->fname, &sttFileReaderConfig, &reader);
        TSDB_CHECK_CODE(code, lino, _exit);

        code = TARRAY2_APPEND(writer->ctx->sttReaderArr, reader);
        TSDB_CHECK_CODE(code, lino, _exit);

        STFileOp fileOp = {
            .optype = TSDB_FOP_REMOVE,
            .fid = fobj->f->fid,
            .of = fobj->f[0],
        };

        code = TARRAY2_APPEND(writer->fopArr, fileOp);
        TSDB_CHECK_CODE(code, lino, _exit);
      }
    }
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbSnapWriteFileSetCloseReader(STsdbSnapWriter* writer) {
  TARRAY2_CLEAR(writer->ctx->sttReaderArr, tsdbSttFileReaderClose);
  tsdbDataFileReaderClose(&writer->ctx->dataReader);
  return 0;
}

static int32_t tsdbSnapWriteFileSetOpenIter(STsdbSnapWriter* writer) {
  int32_t code = 0;
  int32_t lino = 0;

  // data ieter
  if (writer->ctx->dataReader) {
    STsdbIter*      iter;
    STsdbIterConfig config = {0};

    // data
    config.type = TSDB_ITER_TYPE_DATA;
    config.dataReader = writer->ctx->dataReader;

    code = tsdbIterOpen(&config, &iter);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = TARRAY2_APPEND(writer->ctx->dataIterArr, iter);
    TSDB_CHECK_CODE(code, lino, _exit);

    // tome
    config.type = TSDB_ITER_TYPE_DATA_TOMB;
    config.dataReader = writer->ctx->dataReader;

    code = tsdbIterOpen(&config, &iter);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = TARRAY2_APPEND(writer->ctx->tombIterArr, iter);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // stt iter
  SSttFileReader* sttFileReader;
  TARRAY2_FOREACH(writer->ctx->sttReaderArr, sttFileReader) {
    STsdbIter*      iter;
    STsdbIterConfig config = {0};

    // data
    config.type = TSDB_ITER_TYPE_STT;
    config.sttReader = sttFileReader;

    code = tsdbIterOpen(&config, &iter);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = TARRAY2_APPEND(writer->ctx->dataIterArr, iter);
    TSDB_CHECK_CODE(code, lino, _exit);

    // tomb
    config.type = TSDB_ITER_TYPE_STT_TOMB;
    config.sttReader = sttFileReader;

    code = tsdbIterOpen(&config, &iter);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = TARRAY2_APPEND(writer->ctx->tombIterArr, iter);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // open merger
  code = tsdbIterMergerOpen(writer->ctx->dataIterArr, &writer->ctx->dataIterMerger, false);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbIterMergerOpen(writer->ctx->tombIterArr, &writer->ctx->tombIterMerger, true);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbSnapWriteFileSetCloseIter(STsdbSnapWriter* writer) {
  tsdbIterMergerClose(&writer->ctx->dataIterMerger);
  tsdbIterMergerClose(&writer->ctx->tombIterMerger);
  TARRAY2_CLEAR(writer->ctx->dataIterArr, tsdbIterClose);
  TARRAY2_CLEAR(writer->ctx->tombIterArr, tsdbIterClose);
  return 0;
}

static int32_t tsdbSnapWriteFileSetOpenWriter(STsdbSnapWriter* writer) {
  int32_t code = 0;
  int32_t lino = 0;

  SFSetWriterConfig config = {
      .tsdb = writer->tsdb,
      .toSttOnly = false,
      .compactVersion = writer->compactVersion,
      .minRow = writer->minRow,
      .maxRow = writer->maxRow,
      .szPage = writer->szPage,
      .cmprAlg = writer->cmprAlg,
      .fid = writer->ctx->fid,
      .cid = writer->commitID,
      .did = writer->ctx->did,
      .level = 0,
  };

  code = tsdbFSetWriterOpen(&config, &writer->ctx->fsetWriter);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbSnapWriteFileSetCloseWriter(STsdbSnapWriter* writer) {
  return tsdbFSetWriterClose(&writer->ctx->fsetWriter, 0, writer->fopArr);
}

static int32_t tsdbSnapWriteFileSetBegin(STsdbSnapWriter* writer, int32_t fid) {
  int32_t code = 0;
  int32_t lino = 0;

  ASSERT(writer->ctx->fsetWriteBegin == false);

  STFileSet* fset = &(STFileSet){.fid = fid};

  writer->ctx->fid = fid;
  STFileSet** fsetPtr = TARRAY2_SEARCH(writer->fsetArr, &fset, tsdbTFileSetCmprFn, TD_EQ);
  writer->ctx->fset = (fsetPtr == NULL) ? NULL : *fsetPtr;

  int32_t level = tsdbFidLevel(fid, &writer->tsdb->keepCfg, taosGetTimestampSec());
  if (tfsAllocDisk(writer->tsdb->pVnode->pTfs, level, &writer->ctx->did)) {
    code = TSDB_CODE_NO_AVAIL_DISK;
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  tfsMkdirRecurAt(writer->tsdb->pVnode->pTfs, writer->tsdb->path, writer->ctx->did);

  writer->ctx->hasData = true;
  writer->ctx->hasTomb = true;

  code = tsdbSnapWriteFileSetOpenReader(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbSnapWriteFileSetOpenIter(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbSnapWriteFileSetOpenWriter(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  writer->ctx->fsetWriteBegin = true;

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbSnapWriteTombRecord(STsdbSnapWriter* writer, const STombRecord* record) {
  int32_t code = 0;
  int32_t lino = 0;

  while (writer->ctx->hasTomb) {
    STombRecord* record1 = tsdbIterMergerGetTombRecord(writer->ctx->tombIterMerger);
    if (record1 == NULL) {
      writer->ctx->hasTomb = false;
      break;
    }

    int32_t c = tTombRecordCompare(record1, record);
    if (c <= 0) {
      code = tsdbFSetWriteTombRecord(writer->ctx->fsetWriter, record1);
      TSDB_CHECK_CODE(code, lino, _exit);
    } else {
      break;
    }

    code = tsdbIterMergerNext(writer->ctx->tombIterMerger);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (record->suid == INT64_MAX) {
    ASSERT(writer->ctx->hasTomb == false);
    goto _exit;
  }

  code = tsdbFSetWriteTombRecord(writer->ctx->fsetWriter, record);
  TSDB_CHECK_CODE(code, lino, _exit);

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

  // end timeseries data write
  SRowInfo row = {
      .suid = INT64_MAX,
      .uid = INT64_MAX,
  };

  code = tsdbSnapWriteTimeSeriesRow(writer, &row);
  TSDB_CHECK_CODE(code, lino, _exit);

  // end tombstone data write
  STombRecord record = {
      .suid = INT64_MAX,
      .uid = INT64_MAX,
  };

  code = tsdbSnapWriteTombRecord(writer, &record);
  TSDB_CHECK_CODE(code, lino, _exit);

  // close write
  code = tsdbSnapWriteFileSetCloseWriter(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbSnapWriteFileSetCloseIter(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbSnapWriteFileSetCloseReader(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  writer->ctx->fsetWriteBegin = false;

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

  code = tDecmprBlockData(hdr->data, hdr->size - sizeof(*hdr), blockData, writer->aBuf);
  TSDB_CHECK_CODE(code, lino, _exit);

  int32_t fid = tsdbKeyFid(blockData->aTSKEY[0], writer->minutes, writer->precision);
  if (!writer->ctx->fsetWriteBegin || fid != writer->ctx->fid) {
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

  int64_t size = hdr->size;
  ASSERT(size % TOMB_RECORD_ELEM_NUM == 0);
  size = size / TOMB_RECORD_ELEM_NUM;
  ASSERT(size % sizeof(int64_t) == 0);

  int64_t* data = (int64_t*)hdr->data;
  for (int32_t i = 0; i < TOMB_RECORD_ELEM_NUM; ++i) {
    code = TARRAY2_APPEND_BATCH(&tombBlock->dataArr[i], hdr->data + i * size, size / sizeof(int64_t));
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  return code;
}

static int32_t tsdbSnapWriteTombData(STsdbSnapWriter* writer, SSnapDataHdr* hdr) {
  int32_t code = 0;
  int32_t lino = 0;

  STombRecord record;
  STombBlock  tombBlock[1] = {0};

  code = tsdbSnapWriteDecmprTombBlock(hdr, tombBlock);
  TSDB_CHECK_CODE(code, lino, _exit);

  tTombBlockGet(tombBlock, 0, &record);
  int32_t fid = tsdbKeyFid(record.skey, writer->minutes, writer->precision);
  if (!writer->ctx->fsetWriteBegin || fid != writer->ctx->fid) {
    code = tsdbSnapWriteFileSetEnd(writer);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbSnapWriteFileSetBegin(writer, fid);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (writer->ctx->hasData) {
    SRowInfo row = {
        .suid = INT64_MAX,
        .uid = INT64_MAX,
    };

    code = tsdbSnapWriteTimeSeriesRow(writer, &row);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  ASSERT(writer->ctx->hasData == false);

  for (int32_t i = 0; i < TOMB_BLOCK_SIZE(tombBlock); ++i) {
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

int32_t tsdbSnapWriterOpen(STsdb* pTsdb, int64_t sver, int64_t ever, void* pRanges, STsdbSnapWriter** writer) {
  int32_t code = 0;
  int32_t lino = 0;

  // start to write
  writer[0] = taosMemoryCalloc(1, sizeof(*writer[0]));
  if (writer[0] == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  writer[0]->tsdb = pTsdb;
  writer[0]->sver = sver;
  writer[0]->ever = ever;
  writer[0]->minutes = pTsdb->keepCfg.days;
  writer[0]->precision = pTsdb->keepCfg.precision;
  writer[0]->minRow = pTsdb->pVnode->config.tsdbCfg.minRows;
  writer[0]->maxRow = pTsdb->pVnode->config.tsdbCfg.maxRows;
  writer[0]->cmprAlg = pTsdb->pVnode->config.tsdbCfg.compression;
  writer[0]->commitID = tsdbFSAllocEid(pTsdb->pFS);
  writer[0]->szPage = pTsdb->pVnode->config.tsdbPageSize;
  writer[0]->compactVersion = INT64_MAX;
  writer[0]->now = taosGetTimestampMs();

  code = tsdbFSCreateCopyRangedSnapshot(pTsdb->pFS, (TFileSetRangeArray*)pRanges, &writer[0]->fsetArr, writer[0]->fopArr);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(pTsdb->pVnode), __func__, lino, tstrerror(code));
  } else {
    tsdbInfo("vgId:%d %s done, sver:%" PRId64 " ever:%" PRId64, TD_VID(pTsdb->pVnode), __func__, sver, ever);
  }
  return code;
}

int32_t tsdbSnapWriterPrepareClose(STsdbSnapWriter* writer) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tsdbSnapWriteFileSetEnd(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbFSEditBegin(writer->tsdb->pFS, writer->fopArr, TSDB_FEDIT_COMMIT);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->tsdb->pVnode), lino, code);
  } else {
    tsdbDebug("vgId:%d %s done", TD_VID(writer->tsdb->pVnode), __func__);
  }
  return code;
}

int32_t tsdbSnapWriterClose(STsdbSnapWriter** writer, int8_t rollback) {
  if (writer[0] == NULL) return 0;

  int32_t code = 0;
  int32_t lino = 0;

  STsdb* tsdb = writer[0]->tsdb;

  if (rollback) {
    code = tsdbFSEditAbort(writer[0]->tsdb->pFS);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    taosThreadMutexLock(&writer[0]->tsdb->mutex);

    code = tsdbFSEditCommit(writer[0]->tsdb->pFS);
    if (code) {
      taosThreadMutexUnlock(&writer[0]->tsdb->mutex);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    writer[0]->tsdb->pFS->fsstate = TSDB_FS_STATE_NORMAL;

    taosThreadMutexUnlock(&writer[0]->tsdb->mutex);
  }

  tsdbIterMergerClose(&writer[0]->ctx->tombIterMerger);
  tsdbIterMergerClose(&writer[0]->ctx->dataIterMerger);
  TARRAY2_DESTROY(writer[0]->ctx->tombIterArr, tsdbIterClose);
  TARRAY2_DESTROY(writer[0]->ctx->dataIterArr, tsdbIterClose);
  TARRAY2_DESTROY(writer[0]->ctx->sttReaderArr, tsdbSttFileReaderClose);
  tsdbDataFileReaderClose(&writer[0]->ctx->dataReader);

  TARRAY2_DESTROY(writer[0]->fopArr, NULL);
  tsdbFSDestroyCopyRangedSnapshot(&writer[0]->fsetArr);

  for (int32_t i = 0; i < ARRAY_SIZE(writer[0]->aBuf); ++i) {
    tFree(writer[0]->aBuf[i]);
  }

  taosMemoryFree(writer[0]);
  writer[0] = NULL;

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(tsdb->pVnode), lino, code);
  } else {
    tsdbInfo("vgId:%d %s done", TD_VID(tsdb->pVnode), __func__);
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
