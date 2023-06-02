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

#include "inc/tsdbDataFileRW.h"

typedef struct {
  SFDataPtr blockIdxPtr[1];
  SFDataPtr rsrvd[2];
} SDataFooter;

// SDataFileReader =============================================
struct SDataFileReader {
  struct SDataFileReaderConfig config[1];

  struct {
    bool blockIdxLoaded;
  } ctx[1];

  STsdbFD       *fd[TSDB_FTYPE_MAX];
  TBlockIdxArray blockIdxArray[1];
};

int32_t tsdbDataFileReaderOpen(const char *fname[], const SDataFileReaderConfig *config, SDataFileReader **reader) {
  int32_t code = 0;
  int32_t lino;
  int32_t vid = TD_VID(config->tsdb->pVnode);

  reader[0] = taosMemoryCalloc(1, sizeof(SDataFileReader));
  if (!reader[0]) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  reader[0]->config[0] = config[0];

  for (int32_t i = 0; i < TSDB_FTYPE_MAX; ++i) {
    if (fname[i]) {
      code = tsdbOpenFile(fname[i], config->szPage, TD_FILE_READ, &reader[0]->fd[i]);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  // TODO

_exit:
  if (code) {
    TSDB_ERROR_LOG(vid, lino, code);
  }
  return code;
}

int32_t tsdbDataFileReaderClose(SDataFileReader *reader) {
  // TODO
  return 0;
}

int32_t tsdbDataFileReadBlockIdx(SDataFileReader *reader, const TBlockIdxArray **blockIdxArray) {
  if (!reader->ctx->blockIdxLoaded) {
    // TODO
    reader->ctx->blockIdxLoaded = true;
  }
  blockIdxArray[0] = reader->blockIdxArray;
  return 0;
}

int32_t tsdbDataFileReadDataBlk(SDataFileReader *reader, const SBlockIdx *blockIdx,
                                const TDataBlkArray **dataBlkArray) {
  // TODO
  return 0;
}

int32_t tsdbDataFileReadDataBlock(SDataFileReader *reader, const SDataBlk *dataBlk, SBlockData *bData) {
  // TODO
  return 0;
}

// SDataFileWriter =============================================
struct SDataFileWriter {
  SDataFileWriterConfig config[1];

  SSkmInfo skmTb[1];
  SSkmInfo skmRow[1];
  uint8_t *bufArr[5];

  struct {
    bool                  opened;
    SDataFileReader      *reader;
    const TBlockIdxArray *blockIdxArray;
    int32_t               blockIdxArrayIdx;
    bool                  tbHasOldData;
    const TDataBlkArray  *dataBlkArray;
    int32_t               dataBlkArrayIdx;
    SBlockData            bData[1];
    int32_t               iRow;

    TABLEID tbid[1];
  } ctx[1];

  STFile   file[TSDB_FTYPE_MAX];
  STsdbFD *fd[TSDB_FTYPE_MAX];

  SDataFooter    footer[1];
  TBlockIdxArray blockIdxArray[1];
  TDataBlkArray  dataBlkArray[1];
  SBlockData     bData[1];
  SDelData       dData[1];
  STbStatisBlock sData[1];
};

static int32_t tsdbDataFileWriteBlockIdx(SDataFileWriter *writer) {
  int32_t code = 0;
  int32_t lino = 0;

  writer->footer->blockIdxPtr->offset = writer->file[TSDB_FTYPE_HEAD].size;
  writer->footer->blockIdxPtr->size = TARRAY2_DATA_LEN(writer->blockIdxArray);

  if (writer->footer->blockIdxPtr->size) {
    code = tsdbWriteFile(writer->fd[TSDB_FTYPE_HEAD], writer->footer->blockIdxPtr->offset,
                         (void *)TARRAY2_DATA(writer->blockIdxArray), writer->footer->blockIdxPtr->size);
    TSDB_CHECK_CODE(code, lino, _exit);
    writer->file[TSDB_FTYPE_HEAD].size += writer->footer->blockIdxPtr->size;
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbDataFileWriterCloseAbort(SDataFileWriter *writer) {
  ASSERT(0);
  return 0;
}

static int32_t tsdbDataFileWriterDoClose(SDataFileWriter *writer) {
  // TODO
  return 0;
}

static int32_t tsdbDataFileWriterDoOpen(SDataFileWriter *writer) {
  int32_t code = 0;
  int32_t lino = 0;

  if (!writer->config->skmTb) writer->config->skmTb = writer->skmTb;
  if (!writer->config->skmRow) writer->config->skmRow = writer->skmRow;
  if (!writer->config->bufArr) writer->config->bufArr = writer->bufArr;

  // open reader
  if (writer->config->hasOldFile) {
    // TODO
  }

  // open writer
  for (int32_t i = 0; i < TSDB_FTYPE_MAX; ++i) {
    char fname[TSDB_FILENAME_LEN];

    tsdbTFileName(writer->config->tsdb, writer->file + i, fname);
    int32_t flag = TD_FILE_WRITE;  // TODO

    code = tsdbOpenFile(fname, writer->config->szPage, flag, &writer->fd[i]);
    TSDB_CHECK_CODE(code, lino, _exit);

    // writer header
    if (0) {
      // TODO
    }
  }

  writer->ctx->opened = true;

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbDataFileWriteDataBlock(SDataFileWriter *writer, SBlockData *bData) {
  if (bData->nRow == 0) return 0;

  ASSERT(bData->uid);

  int32_t code = 0;
  int32_t lino = 0;

  SDataBlk dataBlk[1] = {{
      .minKey =
          {
              .ts = bData->aTSKEY[0],
              .version = bData->aVersion[0],
          },
      .maxKey =
          {
              .ts = bData->aTSKEY[bData->nRow - 1],
              .version = bData->aVersion[bData->nRow - 1],
          },
      .minVer = bData->aVersion[0],
      .maxVer = bData->aVersion[0],
      .nRow = bData->nRow,
      .hasDup = 0,
      .nSubBlock = 1,
  }};

  for (int32_t i = 1; i < bData->nRow; ++i) {
    if (bData->aTSKEY[i] == bData->aTSKEY[i - 1]) {
      dataBlk->hasDup = 1;
    }
    dataBlk->minVer = TMIN(dataBlk->minVer, bData->aVersion[i]);
    dataBlk->maxVer = TMAX(dataBlk->maxVer, bData->aVersion[i]);
  }

  int32_t sizeArr[5] = {0};

  // to .data
  code = tCmprBlockData(bData, writer->config->cmprAlg, NULL, NULL, writer->config->bufArr, sizeArr);
  TSDB_CHECK_CODE(code, lino, _exit);

  dataBlk->aSubBlock->offset = writer->file[TSDB_FTYPE_DATA].size;
  dataBlk->aSubBlock->szKey = sizeArr[3] + sizeArr[2];
  dataBlk->aSubBlock->szBlock = dataBlk->aSubBlock->szKey + sizeArr[1] + sizeArr[0];

  for (int32_t i = 3; i >= 0; --i) {
    if (sizeArr[i]) {
      code = tsdbWriteFile(writer->fd[TSDB_FTYPE_DATA], writer->file[TSDB_FTYPE_DATA].size, writer->config->bufArr[i],
                           sizeArr[i]);
      TSDB_CHECK_CODE(code, lino, _exit);
      writer->file[TSDB_FTYPE_DATA].size += sizeArr[i];
    }
  }

  // to .sma
  TColumnDataAggArray smaArr[1] = {0};

  for (int32_t i = 0; i < bData->nColData; ++i) {
    SColData *colData = bData->aColData + i;

    if ((!colData->smaOn)                      //
        || ((colData->flag & HAS_VALUE) == 0)  //
    ) {
      continue;
    }

    SColumnDataAgg sma[1] = {{.colId = colData->cid}};
    tColDataCalcSMA[colData->type](colData, &sma->sum, &sma->max, &sma->min, &sma->numOfNull);

    code = TARRAY2_APPEND_PTR(smaArr, sma);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  dataBlk->smaInfo.offset = writer->file[TSDB_FTYPE_SMA].size;
  dataBlk->smaInfo.size = TARRAY2_DATA_LEN(smaArr);

  if (dataBlk->smaInfo.size) {
    code = tsdbWriteFile(writer->fd[TSDB_FTYPE_SMA], dataBlk->smaInfo.offset, (const uint8_t *)TARRAY2_DATA(smaArr),
                         dataBlk->smaInfo.size);
    TSDB_CHECK_CODE(code, lino, _exit);
    writer->file[TSDB_FTYPE_SMA].size += dataBlk->smaInfo.size;
  }

  TARRAY2_FREE(smaArr);

  // to dataBlkArray
  code = TARRAY2_APPEND_PTR(writer->dataBlkArray, dataBlk);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (bData == writer->bData) {
    tBlockDataClear(writer->bData);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbDataFileWriteDataBlk(SDataFileWriter *writer, const TDataBlkArray *dataBlkArray) {
  if (TARRAY2_SIZE(dataBlkArray) == 0) return 0;

  int32_t code = 0;
  int32_t lino = 0;

  SBlockIdx blockIdx[1];
  blockIdx->suid = writer->ctx->tbid->suid;
  blockIdx->uid = writer->ctx->tbid->uid;
  blockIdx->offset = writer->file[TSDB_FTYPE_HEAD].size;
  blockIdx->size = TARRAY2_DATA_LEN(dataBlkArray);

  code = tsdbWriteFile(writer->fd[TSDB_FTYPE_HEAD], blockIdx->offset, (const uint8_t *)TARRAY2_DATA(dataBlkArray),
                       blockIdx->size);
  TSDB_CHECK_CODE(code, lino, _exit);
  writer->file[TSDB_FTYPE_HEAD].size += blockIdx->size;

  code = TARRAY2_APPEND_PTR(writer->blockIdxArray, blockIdx);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbDataFileDoWriteTSRow(SDataFileWriter *writer, TSDBROW *row) {
  int32_t code = 0;
  int32_t lino = 0;

  // update/append
  if (row->type == TSDBROW_ROW_FMT) {
    code = tsdbUpdateSkmRow(writer->config->tsdb, writer->ctx->tbid, TSDBROW_SVERSION(row), writer->config->skmRow);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  TSDBKEY key[1] = {TSDBROW_KEY(row)};
  if (key->version <= writer->config->compactVersion                //
      && writer->bData->nRow > 0                                    //
      && writer->bData->aTSKEY[writer->bData->nRow - 1] == key->ts  //
  ) {
    code = tBlockDataUpdateRow(writer->bData, row, writer->config->skmRow->pTSchema);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    if (writer->bData->nRow >= writer->config->maxRow) {
      code = tsdbDataFileWriteDataBlock(writer, writer->bData);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tBlockDataAppendRow(writer->bData, row, writer->config->skmRow->pTSchema, writer->ctx->tbid->uid);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbDataFileDoWriteTSData(SDataFileWriter *writer, TSDBROW *row) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(writer->config->tsdb->pVnode);

  while (writer->ctx->tbHasOldData) {
    for (; writer->ctx->iRow < writer->ctx->bData->nRow; writer->ctx->iRow++) {
      TSDBROW row1[1] = {tsdbRowFromBlockData(writer->ctx->bData, writer->ctx->iRow)};

      int32_t c = tsdbRowCmprFn(row, row1);
      ASSERT(c);
      if (c > 0) {
        code = tsdbDataFileDoWriteTSRow(writer, row1);
        TSDB_CHECK_CODE(code, lino, _exit);
      } else {
        goto _do_write;
      }
    }

    if (writer->ctx->dataBlkArrayIdx >= TARRAY2_SIZE(writer->ctx->dataBlkArray)) {
      writer->ctx->tbHasOldData = false;
      break;
    }

    for (; writer->ctx->dataBlkArrayIdx < TARRAY2_SIZE(writer->ctx->dataBlkArray); writer->ctx->dataBlkArrayIdx++) {
      const SDataBlk *dataBlk = TARRAY2_GET_PTR(writer->ctx->dataBlkArray, writer->ctx->dataBlkArrayIdx);
      TSDBKEY         key = TSDBROW_KEY(row);
      SDataBlk        dataBlk1[1] = {{
                 .minKey = key,
                 .maxKey = key,
      }};

      int32_t c = tDataBlkCmprFn(dataBlk, dataBlk1);
      if (c < 0) {
        code = tsdbDataFileWriteDataBlock(writer, writer->bData);
        TSDB_CHECK_CODE(code, lino, _exit);

        code = TARRAY2_APPEND_PTR(writer->dataBlkArray, dataBlk);
        TSDB_CHECK_CODE(code, lino, _exit);
      } else if (c > 0) {
        goto _do_write;
      } else {
        code = tsdbDataFileReadDataBlock(writer->ctx->reader, dataBlk, writer->ctx->bData);
        TSDB_CHECK_CODE(code, lino, _exit);

        writer->ctx->iRow = 0;
        writer->ctx->dataBlkArrayIdx++;
        break;
      }
    }
  }

_do_write:
  code = tsdbDataFileDoWriteTSRow(writer, row);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(vid, lino, code);
  }
  return code;
}

static int32_t tsdbDataFileWriteTableDataEnd(SDataFileWriter *writer) {
  if (!writer->ctx->tbid->uid) return 0;

  int32_t code = 0;
  int32_t lino = 0;

  // handle table remain data
  if (writer->ctx->tbHasOldData) {
    for (; writer->ctx->iRow < writer->ctx->bData->nRow; writer->ctx->iRow++) {
      TSDBROW row[1] = {tsdbRowFromBlockData(writer->ctx->bData, writer->ctx->iRow)};

      code = tsdbDataFileDoWriteTSRow(writer, row);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tsdbDataFileWriteDataBlock(writer, writer->bData);
    TSDB_CHECK_CODE(code, lino, _exit);

    for (; writer->ctx->dataBlkArrayIdx < TARRAY2_SIZE(writer->ctx->dataBlkArray); writer->ctx->dataBlkArrayIdx++) {
      code = TARRAY2_APPEND_PTR(writer->dataBlkArray,
                                TARRAY2_GET_PTR(writer->ctx->dataBlkArray, writer->ctx->dataBlkArrayIdx));
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    writer->ctx->tbHasOldData = false;
  }

  code = tsdbDataFileWriteDataBlock(writer, writer->bData);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbDataFileWriteDataBlk(writer, writer->dataBlkArray);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbDataFileWriteTableDataBegin(SDataFileWriter *writer, const TABLEID *tbid) {
  int32_t   code = 0;
  int32_t   lino = 0;
  SMetaInfo info;

  writer->ctx->tbHasOldData = false;

  // skip data of previous table
  if (writer->ctx->blockIdxArray) {
    for (; writer->ctx->blockIdxArrayIdx < TARRAY2_SIZE(writer->ctx->blockIdxArray); writer->ctx->blockIdxArrayIdx++) {
      const SBlockIdx *blockIdx = TARRAY2_GET_PTR(writer->ctx->blockIdxArray, writer->ctx->blockIdxArrayIdx);

      int32_t c = tTABLEIDCmprFn(blockIdx, tbid);
      if (c < 0) {
        if (metaGetInfo(writer->config->tsdb->pVnode->pMeta, blockIdx->uid, &info, NULL) == 0) {
          code = tsdbDataFileReadDataBlk(writer->ctx->reader, blockIdx, &writer->ctx->dataBlkArray);
          TSDB_CHECK_CODE(code, lino, _exit);

          writer->ctx->tbid->suid = blockIdx->suid;
          writer->ctx->tbid->uid = blockIdx->uid;

          code = tsdbDataFileWriteDataBlk(writer, writer->ctx->dataBlkArray);
          TSDB_CHECK_CODE(code, lino, _exit);
        } else {
          continue;
        }
      } else {
        if (c == 0) {
          writer->ctx->tbHasOldData = true;

          code = tsdbDataFileReadDataBlk(writer->ctx->reader, blockIdx, &writer->ctx->dataBlkArray);
          TSDB_CHECK_CODE(code, lino, _exit);

          writer->ctx->dataBlkArrayIdx = 0;

          tBlockDataReset(writer->ctx->bData);
          writer->ctx->iRow = 0;

          writer->ctx->blockIdxArrayIdx++;
        }
        break;
      }
    }
  }

  // make sure state is correct
  writer->ctx->tbid[0] = tbid[0];

  if (tbid->suid == INT64_MAX && tbid->uid == INT64_MAX) goto _exit;

  TARRAY2_CLEAR(writer->dataBlkArray, NULL);

  code = tsdbUpdateSkmTb(writer->config->tsdb, tbid, writer->config->skmTb);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tBlockDataInit(writer->bData, writer->ctx->tbid, writer->config->skmTb->pTSchema, NULL, 0);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbDataFileWriteFooter(SDataFileWriter *writer) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tsdbWriteFile(writer->fd[TSDB_FTYPE_HEAD], writer->file[TSDB_FTYPE_HEAD].size, (const uint8_t *)writer->footer,
                       sizeof(SDataFooter));
  TSDB_CHECK_CODE(code, lino, _exit);
  writer->file[TSDB_FTYPE_HEAD].size += sizeof(SDataFooter);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbDataFileWriterCloseCommit(SDataFileWriter *writer, STFileOp *op) {
  int32_t code = 0;
  int32_t lino = 0;
  TABLEID tbid[1] = {{INT64_MAX, INT64_MAX}};

  code = tsdbDataFileWriteTableDataEnd(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbDataFileWriteTableDataBegin(writer, tbid);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbDataFileWriteBlockIdx(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbDataFileWriteFooter(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  for (int32_t i = 0; i < TSDB_FTYPE_MAX; ++i) {
    if (writer->fd[i]) {
      code = tsdbFsyncFile(writer->fd[i]);
      TSDB_CHECK_CODE(code, lino, _exit);

      tsdbCloseFile(&writer->fd[i]);
    }
  }

  // .head

  // .data

  // .sma

  // .tomb

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbDataFileWriterOpen(const SDataFileWriterConfig *config, SDataFileWriter **writer) {
  writer[0] = taosMemoryCalloc(1, sizeof(SDataFileWriter));
  if (!writer[0]) return TSDB_CODE_OUT_OF_MEMORY;

  writer[0]->config[0] = config[0];
  writer[0]->ctx->opened = false;
  return 0;
}

int32_t tsdbDataFileWriterClose(SDataFileWriter **writer, bool abort, STFileOp op[/*TSDB_FTYPE_MAX*/]) {
  int32_t code = 0;
  int32_t lino = 0;

  if (!writer[0]->ctx->opened) {
    for (int32_t i = 0; i < TSDB_FTYPE_MAX; ++i) {
      op[i].optype = TSDB_FOP_NONE;
    }
  } else {
    if (abort) {
      code = tsdbDataFileWriterCloseAbort(writer[0]);
      TSDB_CHECK_CODE(code, lino, _exit);
    } else {
      code = tsdbDataFileWriterCloseCommit(writer[0], op);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    tsdbDataFileWriterDoClose(writer[0]);
  }
  taosMemoryFree(writer);
  writer[0] = NULL;

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer[0]->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbDataFileWriteTSData(SDataFileWriter *writer, SRowInfo *row) {
  int32_t code = 0;
  int32_t lino = 0;

  if (!writer->ctx->opened) {
    code = tsdbDataFileWriterDoOpen(writer);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (row->uid != writer->ctx->tbid->uid) {
    code = tsdbDataFileWriteTableDataEnd(writer);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbDataFileWriteTableDataBegin(writer, (TABLEID *)row);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tsdbDataFileDoWriteTSData(writer, &row->row);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbDataFileWriteTSDataBlock(SDataFileWriter *writer, SBlockData *bData) {
  if (bData->nRow == 0) return 0;

  int32_t code = 0;
  int32_t lino = 0;

  // ASSERT(bData->uid);

  // if (!writer->ctx->opened) {
  //   code = tsdbDataFileWriterDoOpen(writer);
  //   TSDB_CHECK_CODE(code, lino, _exit);
  // }

  // if (bData->uid != writer->ctx->tbid->uid) {
  //   code = tsdbDataFileWriteTableDataEnd(writer);
  //   TSDB_CHECK_CODE(code, lino, _exit);

  //   code = tsdbDataFileWriteTableDataBegin(writer, (TABLEID *)bData);
  //   TSDB_CHECK_CODE(code, lino, _exit);
  // }

  // code = tsdbDataFileDoWriteTableDataBlock(writer, bData);
  // TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbDataFileFLushTSDataBlock(SDataFileWriter *writer) {
  // if (writer->bData->nRow == 0) return 0;
  // return tsdbDataFileDoWriteTableDataBlock(writer, writer->bData);
  return 0;
}
