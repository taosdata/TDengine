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

  TSnapRangeArray* fsrArr;

  // context
  struct {
    int32_t      fsrArrIdx;
    STSnapRange* fsr;
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

  code = tsdbFSCreateRefRangedSnapshot(tsdb->pFS, sver, ever, (TSnapRangeArray*)pRanges, &reader[0]->fsrArr);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s, sver:%" PRId64 " ever:%" PRId64 " type:%d", TD_VID(tsdb->pVnode),
              __func__, lino, tstrerror(code), sver, ever, type);
    tsdbSnapRangeArrayDestroy(&reader[0]->fsrArr);
    taosMemoryFree(reader[0]);
    reader[0] = NULL;
  } else {
    tsdbInfo("vgId:%d tsdb snapshot reader opened. sver:%" PRId64 " ever:%" PRId64 " type:%d", TD_VID(tsdb->pVnode),
             sver, ever, type);
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

  tsdbSnapRangeArrayDestroy(&reader[0]->fsrArr);
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

  // disable background tasks
  tsdbFSDisableBgTask(pTsdb->pFS);

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

  code = tsdbFSCreateCopyRangedSnapshot(pTsdb->pFS, (TSnapRangeArray*)pRanges, &writer[0]->fsetArr, writer[0]->fopArr);
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
  tsdbFSEnableBgTask(tsdb->pFS);

  tsdbIterMergerClose(&writer[0]->ctx->tombIterMerger);
  tsdbIterMergerClose(&writer[0]->ctx->dataIterMerger);
  TARRAY2_DESTROY(writer[0]->ctx->tombIterArr, tsdbIterClose);
  TARRAY2_DESTROY(writer[0]->ctx->dataIterArr, tsdbIterClose);
  TARRAY2_DESTROY(writer[0]->ctx->sttReaderArr, tsdbSttFileReaderClose);
  tsdbDataFileReaderClose(&writer[0]->ctx->dataReader);

  TARRAY2_DESTROY(writer[0]->fopArr, NULL);
  tsdbFSDestroyCopySnapshot(&writer[0]->fsetArr);

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

// snap part
static int32_t tsdbSnapPartCmprFn(STsdbSnapPartition* x, STsdbSnapPartition* y) {
  if (x->fid < y->fid) return -1;
  if (x->fid > y->fid) return 1;
  return 0;
}

static int32_t tVersionRangeCmprFn(SVersionRange* x, SVersionRange* y) {
  if (x->minVer < y->minVer) return -1;
  if (x->minVer > y->minVer) return 1;
  if (x->maxVer < y->maxVer) return -1;
  if (x->maxVer > y->maxVer) return 1;
  return 0;
}

static int32_t tsdbSnapRangeCmprFn(STSnapRange* x, STSnapRange* y) {
  if (x->fid < y->fid) return -1;
  if (x->fid > y->fid) return 1;
  return 0;
}

STsdbSnapPartition* tsdbSnapPartitionCreate() {
  STsdbSnapPartition* pSP = taosMemoryCalloc(1, sizeof(STsdbSnapPartition));
  if (pSP == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  for (int32_t i = 0; i < TSDB_SNAP_RANGE_TYP_MAX; i++) {
    TARRAY2_INIT(&pSP->verRanges[i]);
  }
  return pSP;
}

void tsdbSnapPartitionClear(STsdbSnapPartition** ppSP) {
  if (ppSP == NULL || ppSP[0] == NULL) {
    return;
  }
  for (int32_t i = 0; i < TSDB_SNAP_RANGE_TYP_MAX; i++) {
    TARRAY2_DESTROY(&ppSP[0]->verRanges[i], NULL);
  }
  taosMemoryFree(ppSP[0]);
  ppSP[0] = NULL;
}

static int32_t tsdbFTypeToSRangeTyp(tsdb_ftype_t ftype) {
  switch (ftype) {
    case TSDB_FTYPE_HEAD:
      return TSDB_SNAP_RANGE_TYP_HEAD;
    case TSDB_FTYPE_DATA:
      return TSDB_SNAP_RANGE_TYP_DATA;
    case TSDB_FTYPE_SMA:
      return TSDB_SNAP_RANGE_TYP_SMA;
    case TSDB_FTYPE_TOMB:
      return TSDB_SNAP_RANGE_TYP_TOMB;
    case TSDB_FTYPE_STT:
      return TSDB_SNAP_RANGE_TYP_STT;
  }
  return TSDB_SNAP_RANGE_TYP_MAX;
}

static int32_t tsdbTFileSetToSnapPart(STFileSet* fset, STsdbSnapPartition** ppSP) {
  STsdbSnapPartition* p = tsdbSnapPartitionCreate();
  if (p == NULL) {
    goto _err;
  }

  p->fid = fset->fid;

  int32_t code = 0;
  int32_t typ = 0;
  int32_t corrupt = false;
  int32_t count = 0;
  for (int32_t ftype = TSDB_FTYPE_MIN; ftype < TSDB_FTYPE_MAX; ++ftype) {
    if (fset->farr[ftype] == NULL) continue;
    typ = tsdbFTypeToSRangeTyp(ftype);
    ASSERT(typ < TSDB_SNAP_RANGE_TYP_MAX);
    STFile* f = fset->farr[ftype]->f;
    if (f->maxVer > fset->maxVerValid) {
      corrupt = true;
      tsdbError("skip incomplete data file: fid:%d, maxVerValid:%" PRId64 ", minVer:%" PRId64 ", maxVer:%" PRId64
                ", ftype: %d",
                fset->fid, fset->maxVerValid, f->minVer, f->maxVer, ftype);
      continue;
    }
    count++;
    SVersionRange vr = {.minVer = f->minVer, .maxVer = f->maxVer};
    code = TARRAY2_SORT_INSERT(&p->verRanges[typ], vr, tVersionRangeCmprFn);
    ASSERT(code == 0);
  }

  typ = TSDB_SNAP_RANGE_TYP_STT;
  const SSttLvl* lvl;
  TARRAY2_FOREACH(fset->lvlArr, lvl) {
    STFileObj* fobj;
    TARRAY2_FOREACH(lvl->fobjArr, fobj) {
      STFile* f = fobj->f;
      if (f->maxVer > fset->maxVerValid) {
        corrupt = true;
        tsdbError("skip incomplete stt file.fid:%d, maxVerValid:%" PRId64 ", minVer:%" PRId64 ", maxVer:%" PRId64
                  ", ftype: %d",
                  fset->fid, fset->maxVerValid, f->minVer, f->maxVer, typ);
        continue;
      }
      count++;
      SVersionRange vr = {.minVer = f->minVer, .maxVer = f->maxVer};
      code = TARRAY2_SORT_INSERT(&p->verRanges[typ], vr, tVersionRangeCmprFn);
      ASSERT(code == 0);
    }
  }
  if (corrupt && count == 0) {
    SVersionRange vr = {.minVer = VERSION_MIN, .maxVer = fset->maxVerValid};
    code = TARRAY2_SORT_INSERT(&p->verRanges[typ], vr, tVersionRangeCmprFn);
    ASSERT(code == 0);
  }
  ppSP[0] = p;
  return 0;

_err:
  tsdbSnapPartitionClear(&p);
  return -1;
}

STsdbSnapPartList* tsdbSnapPartListCreate() {
  STsdbSnapPartList* pList = taosMemoryCalloc(1, sizeof(STsdbSnapPartList));
  if (pList == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  TARRAY2_INIT(pList);
  return pList;
}

static STsdbSnapPartList* tsdbGetSnapPartList(STFileSystem* fs) {
  STsdbSnapPartList* pList = tsdbSnapPartListCreate();
  if (pList == NULL) {
    return NULL;
  }

  int32_t code = 0;
  taosThreadMutexLock(&fs->tsdb->mutex);
  STFileSet* fset;
  TARRAY2_FOREACH(fs->fSetArr, fset) {
    STsdbSnapPartition* pItem = NULL;
    if (tsdbTFileSetToSnapPart(fset, &pItem) < 0) {
      code = -1;
      break;
    }
    ASSERT(pItem != NULL);
    code = TARRAY2_SORT_INSERT(pList, pItem, tsdbSnapPartCmprFn);
    ASSERT(code == 0);
  }
  taosThreadMutexUnlock(&fs->tsdb->mutex);

  if (code) {
    TARRAY2_DESTROY(pList, tsdbSnapPartitionClear);
    taosMemoryFree(pList);
    pList = NULL;
  }
  return pList;
}

int32_t tTsdbSnapPartListDataLenCalc(STsdbSnapPartList* pList) {
  int32_t hdrLen = sizeof(int32_t);
  int32_t datLen = 0;

  int8_t  msgVer = 1;
  int32_t len = TARRAY2_SIZE(pList);
  hdrLen += sizeof(msgVer);
  hdrLen += sizeof(len);
  datLen += hdrLen;

  for (int32_t u = 0; u < len; u++) {
    STsdbSnapPartition* p = TARRAY2_GET(pList, u);
    int32_t             typMax = TSDB_SNAP_RANGE_TYP_MAX;
    int32_t             uItem = 0;
    uItem += sizeof(STsdbSnapPartition);
    uItem += sizeof(typMax);

    for (int32_t i = 0; i < typMax; i++) {
      int32_t iLen = TARRAY2_SIZE(&p->verRanges[i]);
      int32_t jItem = 0;
      jItem += sizeof(SVersionRange);
      jItem += sizeof(int64_t);
      uItem += sizeof(iLen) + jItem * iLen;
    }
    datLen += uItem;
  }
  return datLen;
}

int32_t tSerializeTsdbSnapPartList(void* buf, int32_t bufLen, STsdbSnapPartList* pList) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  int8_t  reserved8 = 0;
  int16_t reserved16 = 0;
  int64_t reserved64 = 0;

  int8_t  msgVer = 1;
  int32_t len = TARRAY2_SIZE(pList);

  if (tStartEncode(&encoder) < 0) goto _err;
  if (tEncodeI8(&encoder, msgVer) < 0) goto _err;
  if (tEncodeI32(&encoder, len) < 0) goto _err;

  for (int32_t u = 0; u < len; u++) {
    STsdbSnapPartition* p = TARRAY2_GET(pList, u);
    if (tEncodeI64(&encoder, p->fid) < 0) goto _err;
    if (tEncodeI8(&encoder, p->stat) < 0) goto _err;
    if (tEncodeI8(&encoder, reserved8) < 0) goto _err;
    if (tEncodeI16(&encoder, reserved16) < 0) goto _err;

    int32_t typMax = TSDB_SNAP_RANGE_TYP_MAX;
    if (tEncodeI32(&encoder, typMax) < 0) goto _err;

    for (int32_t i = 0; i < typMax; i++) {
      SVerRangeList* iList = &p->verRanges[i];
      int32_t        iLen = TARRAY2_SIZE(iList);

      if (tEncodeI32(&encoder, iLen) < 0) goto _err;
      for (int32_t j = 0; j < iLen; j++) {
        SVersionRange r = TARRAY2_GET(iList, j);
        if (tEncodeI64(&encoder, r.minVer) < 0) goto _err;
        if (tEncodeI64(&encoder, r.maxVer) < 0) goto _err;
        if (tEncodeI64(&encoder, reserved64) < 0) goto _err;
      }
    }
  }

  tEndEncode(&encoder);
  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;

_err:
  tEncoderClear(&encoder);
  return -1;
}

int32_t tDeserializeTsdbSnapPartList(void* buf, int32_t bufLen, STsdbSnapPartList* pList) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  int8_t  reserved8 = 0;
  int16_t reserved16 = 0;
  int64_t reserved64 = 0;

  STsdbSnapPartition* p = NULL;

  int8_t  msgVer = 0;
  int32_t len = 0;
  if (tStartDecode(&decoder) < 0) goto _err;
  if (tDecodeI8(&decoder, &msgVer) < 0) goto _err;
  if (tDecodeI32(&decoder, &len) < 0) goto _err;

  for (int32_t u = 0; u < len; u++) {
    p = tsdbSnapPartitionCreate();
    if (p == NULL) goto _err;
    if (tDecodeI64(&decoder, &p->fid) < 0) goto _err;
    if (tDecodeI8(&decoder, &p->stat) < 0) goto _err;
    if (tDecodeI8(&decoder, &reserved8) < 0) goto _err;
    if (tDecodeI16(&decoder, &reserved16) < 0) goto _err;

    int32_t typMax = 0;
    if (tDecodeI32(&decoder, &typMax) < 0) goto _err;

    for (int32_t i = 0; i < typMax; i++) {
      SVerRangeList* iList = &p->verRanges[i];
      int32_t        iLen = 0;
      if (tDecodeI32(&decoder, &iLen) < 0) goto _err;
      for (int32_t j = 0; j < iLen; j++) {
        SVersionRange r = {0};
        if (tDecodeI64(&decoder, &r.minVer) < 0) goto _err;
        if (tDecodeI64(&decoder, &r.maxVer) < 0) goto _err;
        if (tDecodeI64(&decoder, &reserved64) < 0) goto _err;
        TARRAY2_APPEND(iList, r);
      }
    }
    TARRAY2_APPEND(pList, p);
    p = NULL;
  }

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;

_err:
  if (p) {
    tsdbSnapPartitionClear(&p);
  }
  tDecoderClear(&decoder);
  return -1;
}

int32_t tsdbSnapPartListToRangeDiff(STsdbSnapPartList* pList, TSnapRangeArray** ppRanges) {
  TSnapRangeArray* pDiff = taosMemoryCalloc(1, sizeof(TSnapRangeArray));
  if (pDiff == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }
  TARRAY2_INIT(pDiff);

  STsdbSnapPartition* part;
  TARRAY2_FOREACH(pList, part) {
    STSnapRange* r = taosMemoryCalloc(1, sizeof(STSnapRange));
    if (r == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
    }
    int64_t maxVerValid = -1;
    int32_t typMax = TSDB_SNAP_RANGE_TYP_MAX;
    for (int32_t i = 0; i < typMax; i++) {
      SVerRangeList* iList = &part->verRanges[i];
      SVersionRange  vr = {0};
      TARRAY2_FOREACH(iList, vr) {
        if (vr.maxVer < vr.minVer) {
          continue;
        }
        maxVerValid = TMAX(maxVerValid, vr.maxVer);
      }
    }
    r->fid = part->fid;
    r->sver = maxVerValid + 1;
    r->ever = VERSION_MAX;
    tsdbDebug("range diff fid:%" PRId64 ", sver:%" PRId64 ", ever:%" PRId64, part->fid, r->sver, r->ever);
    int32_t code = TARRAY2_SORT_INSERT(pDiff, r, tsdbSnapRangeCmprFn);
    ASSERT(code == 0);
  }
  ppRanges[0] = pDiff;

  tsdbInfo("pDiff size:%d", TARRAY2_SIZE(pDiff));
  return 0;

_err:
  if (pDiff) {
    tsdbSnapRangeArrayDestroy(&pDiff);
  }
  return -1;
}

void tsdbSnapRangeArrayDestroy(TSnapRangeArray** ppSnap) {
  if (ppSnap && ppSnap[0]) {
    TARRAY2_DESTROY(ppSnap[0], tsdbTSnapRangeClear);
    taosMemoryFree(ppSnap[0]);
    ppSnap[0] = NULL;
  }
}

void tsdbSnapPartListDestroy(STsdbSnapPartList** ppList) {
  if (ppList == NULL || ppList[0] == NULL) return;

  TARRAY2_DESTROY(ppList[0], tsdbSnapPartitionClear);
  taosMemoryFree(ppList[0]);
  ppList[0] = NULL;
}

ETsdbFsState tsdbSnapGetFsState(SVnode* pVnode) {
  if (!VND_IS_RSMA(pVnode)) {
    return pVnode->pTsdb->pFS->fsstate;
  }
  for (int32_t lvl = 0; lvl < TSDB_RETENTION_MAX; ++lvl) {
    if (SMA_RSMA_GET_TSDB(pVnode, lvl)->pFS->fsstate != TSDB_FS_STATE_NORMAL) {
      return TSDB_FS_STATE_INCOMPLETE;
    }
  }
  return TSDB_FS_STATE_NORMAL;
}

int32_t tsdbSnapGetDetails(SVnode* pVnode, SSnapshot* pSnap) {
  int                code = -1;
  int32_t            tsdbMaxCnt = (!VND_IS_RSMA(pVnode) ? 1 : TSDB_RETENTION_MAX);
  int32_t            subTyps[TSDB_RETENTION_MAX] = {SNAP_DATA_TSDB, SNAP_DATA_RSMA1, SNAP_DATA_RSMA2};
  STsdbSnapPartList* pLists[TSDB_RETENTION_MAX] = {0};

  for (int32_t j = 0; j < tsdbMaxCnt; ++j) {
    STsdb* pTsdb = SMA_RSMA_GET_TSDB(pVnode, j);
    pLists[j] = tsdbGetSnapPartList(pTsdb->pFS);
    if (pLists[j] == NULL) goto _out;
  }

  // estimate bufLen and prepare
  int32_t bufLen = sizeof(SSyncTLV);  // typ: TDMT_SYNC_PREP_SNAPSHOT or TDMT_SYNC_PREP_SNAPSOT_REPLY
  for (int32_t j = 0; j < tsdbMaxCnt; ++j) {
    bufLen += sizeof(SSyncTLV);  // subTyps[j]
    bufLen += tTsdbSnapPartListDataLenCalc(pLists[j]);
  }

  tsdbInfo("vgId:%d, allocate %d bytes for data of snapshot info.", TD_VID(pVnode), bufLen);

  void* data = taosMemoryRealloc(pSnap->data, bufLen);
  if (data == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    tsdbError("vgId:%d, failed to realloc memory for data of snapshot info. bytes:%d", TD_VID(pVnode), bufLen);
    goto _out;
  }
  pSnap->data = data;

  // header
  SSyncTLV* head = data;
  head->len = 0;
  head->typ = pSnap->type;
  int32_t offset = sizeof(SSyncTLV);
  int32_t tlen = 0;

  // fill snapshot info
  for (int32_t j = 0; j < tsdbMaxCnt; ++j) {
    if (pSnap->type == TDMT_SYNC_PREP_SNAPSHOT_REPLY) {
    }

    //  subHead
    SSyncTLV* subHead = (void*)((char*)data + offset);
    subHead->typ = subTyps[j];
    ASSERT(subHead->val == (char*)data + offset + sizeof(SSyncTLV));

    if ((tlen = tSerializeTsdbSnapPartList(subHead->val, bufLen - offset - sizeof(SSyncTLV), pLists[j])) < 0) {
      tsdbError("vgId:%d, failed to serialize snap partition list of tsdb %d since %s", TD_VID(pVnode), j, terrstr());
      goto _out;
    }
    subHead->len = tlen;
    offset += sizeof(SSyncTLV) + tlen;
  }

  head->len = offset - sizeof(SSyncTLV);
  ASSERT(offset <= bufLen);
  code = 0;

_out:
  for (int32_t j = 0; j < tsdbMaxCnt; ++j) {
    if (pLists[j] == NULL) continue;
    tsdbSnapPartListDestroy(&pLists[j]);
  }

  return code;
}
