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

  uint8_t *bufArr[5];

  struct {
    bool    footerLoaded;
    bool    blockIdxLoaded;
    TABLEID tbid[1];
  } ctx[1];

  STsdbFD *fd[TSDB_FTYPE_MAX];

  SDataFooter    footer[1];
  TBlockIdxArray blockIdxArray[1];
  TDataBlkArray  dataBlkArray[1];
};

static int32_t tsdbDataFileReadFooter(SDataFileReader *reader) {
  if (!reader->config->files[TSDB_FTYPE_HEAD].exist  //
      || reader->ctx->footerLoaded) {
    return 0;
  }

  int32_t code = 0;
  int32_t lino = 0;

  code =
      tsdbReadFile(reader->fd[TSDB_FTYPE_HEAD], reader->config->files[TSDB_FTYPE_HEAD].file.size - sizeof(SDataFooter),
                   (uint8_t *)reader->footer, sizeof(SDataFooter));
  TSDB_CHECK_CODE(code, lino, _exit);
  reader->ctx->footerLoaded = true;

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(reader->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbDataFileReaderOpen(const char *fname[], const SDataFileReaderConfig *config, SDataFileReader **reader) {
  int32_t code = 0;
  int32_t lino = 0;

  reader[0] = taosMemoryCalloc(1, sizeof(**reader));
  if (!reader[0]) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  reader[0]->config[0] = config[0];
  if (!reader[0]->config->bufArr) {
    reader[0]->config->bufArr = reader[0]->bufArr;
  }

  if (fname) {
    for (int32_t i = 0; i < TSDB_FTYPE_MAX; ++i) {
      if (fname[i] == NULL) continue;

      code = tsdbOpenFile(fname[i], config->szPage, TD_FILE_READ, &reader[0]->fd[i]);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  } else {
    for (int32_t i = 0; i < TSDB_FTYPE_MAX; ++i) {
      if (!config->files[i].exist) continue;

      char fname1[TSDB_FILENAME_LEN];
      tsdbTFileName(config->tsdb, &config->files[i].file, fname1);
      code = tsdbOpenFile(fname1, config->szPage, TD_FILE_READ, &reader[0]->fd[i]);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbDataFileReaderClose(SDataFileReader **reader) {
  if (reader[0] == NULL) return 0;

  TARRAY2_FREE(reader[0]->dataBlkArray);
  TARRAY2_FREE(reader[0]->blockIdxArray);

  for (int32_t i = 0; i < TSDB_FTYPE_MAX; ++i) {
    tsdbCloseFile(&reader[0]->fd[i]);
  }

  for (int32_t i = 0; i < ARRAY_SIZE(reader[0]->bufArr); ++i) {
    tFree(reader[0]->bufArr[i]);
  }
  taosMemoryFree(reader[0]);
  reader[0] = NULL;

  return 0;
}

int32_t tsdbDataFileReadBlockIdx(SDataFileReader *reader, const TBlockIdxArray **blockIdxArray) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tsdbDataFileReadFooter(reader);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (!reader->ctx->blockIdxLoaded) {
    TARRAY2_CLEAR(reader->blockIdxArray, NULL);

    if (reader->config->files[TSDB_FTYPE_HEAD].exist  //
        && reader->footer->blockIdxPtr->size) {
      code = tRealloc(&reader->config->bufArr[0], reader->footer->blockIdxPtr->size);
      TSDB_CHECK_CODE(code, lino, _exit);

      code = tsdbReadFile(reader->fd[TSDB_FTYPE_HEAD], reader->footer->blockIdxPtr->offset, reader->config->bufArr[0],
                          reader->footer->blockIdxPtr->size);
      TSDB_CHECK_CODE(code, lino, _exit);

      int32_t size = reader->footer->blockIdxPtr->size / sizeof(SBlockIdx);
      for (int32_t i = 0; i < size; ++i) {
        code = TARRAY2_APPEND_PTR(reader->blockIdxArray, ((SBlockIdx *)reader->config->bufArr[0]) + i);
        TSDB_CHECK_CODE(code, lino, _exit);
      }
    }

    reader->ctx->blockIdxLoaded = true;
  }

  blockIdxArray[0] = reader->blockIdxArray;

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(reader->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbDataFileReadDataBlk(SDataFileReader *reader, const SBlockIdx *blockIdx,
                                const TDataBlkArray **dataBlkArray) {
  ASSERT(reader->ctx->footerLoaded);

  if (reader->ctx->tbid->suid == blockIdx->suid && reader->ctx->tbid->uid == blockIdx->uid) {
    dataBlkArray[0] = reader->dataBlkArray;
    return 0;
  }

  int32_t code = 0;
  int32_t lino = 0;

  reader->ctx->tbid->suid = blockIdx->suid;
  reader->ctx->tbid->uid = blockIdx->uid;

  TARRAY2_CLEAR(reader->dataBlkArray, NULL);

  code = tRealloc(&reader->config->bufArr[0], blockIdx->size);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbReadFile(reader->fd[TSDB_FTYPE_HEAD], blockIdx->offset, reader->config->bufArr[0], blockIdx->size);
  TSDB_CHECK_CODE(code, lino, _exit);

  int32_t size = blockIdx->size / sizeof(SDataBlk);
  for (int32_t i = 0; i < size; ++i) {
    code = TARRAY2_APPEND_PTR(reader->dataBlkArray, ((SDataBlk *)reader->config->bufArr[0]) + i);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  dataBlkArray[0] = reader->dataBlkArray;

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(reader->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbDataFileReadDataBlock(SDataFileReader *reader, const SDataBlk *dataBlk, SBlockData *bData) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tRealloc(&reader->config->bufArr[0], dataBlk->aSubBlock->szBlock);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbReadFile(reader->fd[TSDB_FTYPE_DATA], dataBlk->aSubBlock->offset, reader->config->bufArr[0],
                      dataBlk->aSubBlock->szBlock);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tDecmprBlockData(reader->config->bufArr[0], dataBlk->aSubBlock->szBlock, bData, &reader->config->bufArr[1]);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(reader->config->tsdb->pVnode), lino, code);
  }
  return code;
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
    TABLEID               tbid[1];
  } ctx[1];

  STFile   files[TSDB_FTYPE_MAX];
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

  writer->footer->blockIdxPtr->offset = writer->files[TSDB_FTYPE_HEAD].size;
  writer->footer->blockIdxPtr->size = TARRAY2_DATA_LEN(writer->blockIdxArray);

  if (writer->footer->blockIdxPtr->size) {
    code = tsdbWriteFile(writer->fd[TSDB_FTYPE_HEAD], writer->footer->blockIdxPtr->offset,
                         (void *)TARRAY2_DATA(writer->blockIdxArray), writer->footer->blockIdxPtr->size);
    TSDB_CHECK_CODE(code, lino, _exit);
    writer->files[TSDB_FTYPE_HEAD].size += writer->footer->blockIdxPtr->size;
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

static int32_t tsdbDataFileWriterDoOpenReader(SDataFileWriter *writer) {
  int32_t code = 0;
  int32_t lino = 0;

  for (int32_t i = 0; i < TSDB_FTYPE_MAX; ++i) {
    if (writer->config->files[i].exist) {
      SDataFileReaderConfig config[1] = {{
          .tsdb = writer->config->tsdb,
          .szPage = writer->config->szPage,
      }};

      for (int32_t i = 0; i < TSDB_FTYPE_MAX; ++i) {
        config->files[i].exist = writer->config->files[i].exist;
        config->files[i].file = writer->config->files[i].file;
      }

      code = tsdbDataFileReaderOpen(NULL, config, &writer->ctx->reader);
      TSDB_CHECK_CODE(code, lino, _exit);

      break;
    }
  }

  code = tsdbDataFileReadBlockIdx(writer->ctx->reader, &writer->ctx->blockIdxArray);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbDataFileWriterDoOpen(SDataFileWriter *writer) {
  int32_t code = 0;
  int32_t lino = 0;

  if (!writer->config->skmTb) writer->config->skmTb = writer->skmTb;
  if (!writer->config->skmRow) writer->config->skmRow = writer->skmRow;
  if (!writer->config->bufArr) writer->config->bufArr = writer->bufArr;

  // open reader
  code = tsdbDataFileWriterDoOpenReader(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  // .head
  writer->files[TSDB_FTYPE_HEAD] = (STFile){
      .type = TSDB_FTYPE_HEAD,
      .did = writer->config->did,
      .fid = writer->config->fid,
      .cid = writer->config->cid,
      .size = 0,
  };

  // .data
  if (writer->config->files[TSDB_FTYPE_DATA].exist) {
    writer->files[TSDB_FTYPE_DATA] = writer->config->files[TSDB_FTYPE_DATA].file;
  } else {
    writer->files[TSDB_FTYPE_DATA] = writer->files[TSDB_FTYPE_HEAD];
    writer->files[TSDB_FTYPE_DATA].type = TSDB_FTYPE_DATA;
  }

  // .sma
  if (writer->config->files[TSDB_FTYPE_SMA].exist) {
    writer->files[TSDB_FTYPE_SMA] = writer->config->files[TSDB_FTYPE_SMA].file;
  } else {
    writer->files[TSDB_FTYPE_SMA] = writer->files[TSDB_FTYPE_HEAD];
    writer->files[TSDB_FTYPE_SMA].type = TSDB_FTYPE_SMA;
  }

  // .tomb (todo)
  writer->files[TSDB_FTYPE_TOMB] = (STFile){
      .type = TSDB_FTYPE_TOMB,
      .did = writer->config->did,
      .fid = writer->config->fid,
      .cid = writer->config->cid,
      .size = 0,
  };

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  } else {
    writer->ctx->opened = true;
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

  dataBlk->aSubBlock->offset = writer->files[TSDB_FTYPE_DATA].size;
  dataBlk->aSubBlock->szKey = sizeArr[3] + sizeArr[2];
  dataBlk->aSubBlock->szBlock = dataBlk->aSubBlock->szKey + sizeArr[1] + sizeArr[0];

  for (int32_t i = 3; i >= 0; --i) {
    if (sizeArr[i]) {
      code = tsdbWriteFile(writer->fd[TSDB_FTYPE_DATA], writer->files[TSDB_FTYPE_DATA].size, writer->config->bufArr[i],
                           sizeArr[i]);
      TSDB_CHECK_CODE(code, lino, _exit);
      writer->files[TSDB_FTYPE_DATA].size += sizeArr[i];
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

  dataBlk->smaInfo.offset = writer->files[TSDB_FTYPE_SMA].size;
  dataBlk->smaInfo.size = TARRAY2_DATA_LEN(smaArr);

  if (dataBlk->smaInfo.size) {
    code = tsdbWriteFile(writer->fd[TSDB_FTYPE_SMA], dataBlk->smaInfo.offset, (const uint8_t *)TARRAY2_DATA(smaArr),
                         dataBlk->smaInfo.size);
    TSDB_CHECK_CODE(code, lino, _exit);
    writer->files[TSDB_FTYPE_SMA].size += dataBlk->smaInfo.size;
  }

  TARRAY2_FREE(smaArr);

  // to dataBlkArray
  code = TARRAY2_APPEND_PTR(writer->dataBlkArray, dataBlk);
  TSDB_CHECK_CODE(code, lino, _exit);

  tBlockDataClear(writer->bData);

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
  blockIdx->offset = writer->files[TSDB_FTYPE_HEAD].size;
  blockIdx->size = TARRAY2_DATA_LEN(dataBlkArray);

  code = tsdbWriteFile(writer->fd[TSDB_FTYPE_HEAD], blockIdx->offset, (const uint8_t *)TARRAY2_DATA(dataBlkArray),
                       blockIdx->size);
  TSDB_CHECK_CODE(code, lino, _exit);
  writer->files[TSDB_FTYPE_HEAD].size += blockIdx->size;

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
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
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

  code = tsdbWriteFile(writer->fd[TSDB_FTYPE_HEAD], writer->files[TSDB_FTYPE_HEAD].size,
                       (const uint8_t *)writer->footer, sizeof(SDataFooter));
  TSDB_CHECK_CODE(code, lino, _exit);
  writer->files[TSDB_FTYPE_HEAD].size += sizeof(SDataFooter);

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

  // .head
  int32_t ftype = TSDB_FTYPE_HEAD;
  op[ftype] = (STFileOp){
      .optype = TSDB_FOP_CREATE,
      .fid = writer->config->fid,
      .nf = writer->files[ftype],
  };

  // .data
  ftype = TSDB_FTYPE_DATA;
  if (writer->fd[ftype]) {
    if (!writer->config->files[ftype].exist) {
      op[ftype] = (STFileOp){
          .optype = TSDB_FOP_CREATE,
          .fid = writer->config->fid,
          .nf = writer->files[ftype],
      };
    } else if (writer->config->files[ftype].file.size == writer->files[ftype].size) {
      op[ftype].optype = TSDB_FOP_NONE;
    } else {
      op[ftype] = (STFileOp){
          .optype = TSDB_FOP_MODIFY,
          .fid = writer->config->fid,
          .of = writer->config->files[ftype].file,
          .nf = writer->files[ftype],
      };
    }
  } else {
    op[ftype].optype = TSDB_FOP_NONE;
  }

  // .sma
  ftype = TSDB_FTYPE_SMA;
  if (writer->fd[ftype]) {
    if (!writer->config->files[ftype].exist) {
      op[ftype] = (STFileOp){
          .optype = TSDB_FOP_CREATE,
          .fid = writer->config->fid,
          .nf = writer->files[ftype],
      };
    } else if (writer->config->files[ftype].file.size == writer->files[ftype].size) {
      op[ftype].optype = TSDB_FOP_NONE;
    } else {
      op[ftype] = (STFileOp){
          .optype = TSDB_FOP_MODIFY,
          .fid = writer->config->fid,
          .of = writer->config->files[ftype].file,
          .nf = writer->files[ftype],
      };
    }
  } else {
    op[ftype].optype = TSDB_FOP_NONE;
  }

  // .tomb
  op[TSDB_FTYPE_TOMB] = (STFileOp){
      .optype = TSDB_FOP_NONE,
  };

  for (int32_t i = 0; i < TSDB_FTYPE_MAX; ++i) {
    if (!writer->fd[i]) continue;
    code = tsdbFsyncFile(writer->fd[i]);
    TSDB_CHECK_CODE(code, lino, _exit);
    tsdbCloseFile(&writer->fd[i]);
  }

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

static int32_t tsdbDataFileWriterOpenDataFD(SDataFileWriter *writer) {
  int32_t code = 0;
  int32_t lino = 0;

  for (int32_t i = 0; i < TSDB_FTYPE_TOMB /* TODO */; ++i) {
    char    fname[TSDB_FILENAME_LEN];
    int32_t flag = TD_FILE_READ | TD_FILE_WRITE;

    if (writer->files[i].size == 0) {
      flag |= (TD_FILE_CREATE | TD_FILE_TRUNC);
    }

    tsdbTFileName(writer->config->tsdb, &writer->files[i], fname);
    code = tsdbOpenFile(fname, writer->config->szPage, flag, &writer->fd[i]);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (writer->files[i].size == 0) {
      uint8_t hdr[TSDB_FHDR_SIZE] = {0};

      code = tsdbWriteFile(writer->fd[i], 0, hdr, TSDB_FHDR_SIZE);
      TSDB_CHECK_CODE(code, lino, _exit);

      writer->files[i].size += TSDB_FHDR_SIZE;
    }
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
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

  // open FD
  if (!writer->fd[TSDB_FTYPE_DATA]) {
    code = tsdbDataFileWriterOpenDataFD(writer);
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

  ASSERT(bData->uid);

  if (!writer->ctx->opened) {
    code = tsdbDataFileWriterDoOpen(writer);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (!writer->fd[TSDB_FTYPE_DATA]) {
    code = tsdbDataFileWriterOpenDataFD(writer);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (bData->uid != writer->ctx->tbid->uid) {
    code = tsdbDataFileWriteTableDataEnd(writer);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbDataFileWriteTableDataBegin(writer, (TABLEID *)bData);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (!writer->ctx->tbHasOldData   //
      && writer->bData->nRow == 0  //
  ) {
    code = tsdbDataFileWriteDataBlock(writer, bData);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    for (int32_t i = 0; i < bData->nRow; ++i) {
      TSDBROW row[1] = {tsdbRowFromBlockData(bData, i)};
      code = tsdbDataFileDoWriteTSData(writer, row);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbDataFileFlushTSDataBlock(SDataFileWriter *writer) {
  ASSERT(writer->ctx->opened);

  if (writer->bData->nRow == 0) return 0;
  if (writer->ctx->tbHasOldData) return 0;

  return tsdbDataFileWriteDataBlock(writer, writer->bData);
}
