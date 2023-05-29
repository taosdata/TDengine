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

// SDataFileWriter =============================================
struct SDataFileWriter {
  SDataFileWriterConfig config[1];

  struct {
    bool                  opened;
    bool                  tbHasOldData;
    SDataFileReader      *reader;
    const TBlockIdxArray *blockIdxArray;
    int32_t               blockIdxArrayIdx;
    TABLEID               tbid[1];
    const TDataBlkArray  *dataBlkArray;
    int32_t               dataBlkArrayIdx;
    SBlockData            bData[1];
    int32_t               iRow;
  } ctx[1];

  STFile         f[TSDB_FTYPE_MAX];
  STsdbFD       *fd[TSDB_FTYPE_MAX];
  TBlockIdxArray blockIdxArray[1];
  TDataBlkArray  dataBlkArray[1];
  SBlockData     bData[1];
  SDelData       dData[1];
  STbStatisBlock sData[1];
};

int32_t tsdbDataFileWriterOpen(const SDataFileWriterConfig *config, SDataFileWriter **writer) {
  writer[0] = taosMemoryCalloc(1, sizeof(SDataFileWriter));
  if (!writer[0]) return TSDB_CODE_OUT_OF_MEMORY;
  writer[0]->ctx->opened = false;
  return 0;
}

static int32_t tsdbDataFileWriteRemainData(SDataFileWriter *writer) {
  // TODO
  return 0;
}
static int32_t tsdbDataFileWriteBlockIdx(SDataFileWriter *writer) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(writer->config->tsdb->pVnode);

  int64_t offset = writer->f[TSDB_FTYPE_HEAD].size;
  int64_t size = TARRAY2_DATA_LEN(writer->dataBlkArray);
  if (TARRAY2_SIZE(writer->blockIdxArray) > 0) {
    code = tsdbWriteFile(writer->fd[TSDB_FTYPE_HEAD], offset, (void *)TARRAY2_DATA(writer->blockIdxArray), size);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(vid, lino, code);
  }
  return code;
}
static int32_t tsdbDataFileWriterCloseCommit(SDataFileWriter *writer) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(writer->config->tsdb->pVnode);

  code = tsdbDataFileWriteRemainData(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbDataFileWriteBlockIdx(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  for (int32_t i = 0; i < TSDB_FTYPE_MAX; ++i) {
    if (writer->fd[i]) {
      code = tsdbFsyncFile(writer->fd[i]);
      TSDB_CHECK_CODE(code, lino, _exit);

      tsdbCloseFile(&writer->fd[i]);
    }
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(vid, lino, code);
  }
  return code;
}
static int32_t tsdbDataFileWriterCloseAbort(SDataFileWriter *writer) {
  // TODO
  return 0;
}
static int32_t tsdbDataFileWriterDoClose(SDataFileWriter *writer) {
  // TODO
  return 0;
}
int32_t tsdbDataFileWriterClose(SDataFileWriter **writer, bool abort, STFileOp op[/*TSDB_FTYPE_MAX*/]) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(writer[0]->config->tsdb->pVnode);

  if (!writer[0]->ctx->opened) {
    for (int32_t i = 0; i < TSDB_FTYPE_MAX; ++i) {
      op[i].optype = TSDB_FOP_NONE;
    }
  } else {
    if (abort) {
      code = tsdbDataFileWriterCloseAbort(writer[0]);
      TSDB_CHECK_CODE(code, lino, _exit);
    } else {
      code = tsdbDataFileWriterCloseCommit(writer[0]);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    tsdbDataFileWriterDoClose(writer[0]);
  }
  taosMemoryFree(writer);
  writer[0] = NULL;

_exit:
  if (code) {
    TSDB_ERROR_LOG(vid, lino, code);
  }
  return code;
}

static int32_t tsdbDataFileWriterDoOpen(SDataFileWriter *writer) {
  // TODO
  writer->ctx->opened = true;
  return 0;
}
static int32_t tsdbDataFileWriteBlockData(SDataFileWriter *writer, SBlockData *bData) {
  if (bData->nRow == 0) return 0;

  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(writer->config->tsdb->pVnode);

  SDataBlk dataBlk[1];

  // TODO: fill dataBlk

  // TODO: write data

  code = TARRAY2_APPEND_PTR(writer->dataBlkArray, dataBlk);
  TSDB_CHECK_CODE(code, lino, _exit);

  tBlockDataClear(bData);

_exit:
  if (code) {
    TSDB_ERROR_LOG(vid, lino, code);
  }
  return code;
}
static int32_t tsdbDataFileWriteDataBlk(SDataFileWriter *writer, const TDataBlkArray *dataBlkArray) {
  if (TARRAY2_SIZE(dataBlkArray) == 0) return 0;

  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(writer->config->tsdb->pVnode);

  SBlockIdx blockIdx[1];
  blockIdx->suid = writer->ctx->tbid->suid;
  blockIdx->uid = writer->ctx->tbid->uid;
  blockIdx->offset = writer->f[TSDB_FTYPE_HEAD].size;
  blockIdx->size = TARRAY2_DATA_LEN(dataBlkArray);

  code = tsdbWriteFile(writer->fd[TSDB_FTYPE_HEAD], blockIdx->offset, (const uint8_t *)TARRAY2_DATA(dataBlkArray),
                       blockIdx->size);
  TSDB_CHECK_CODE(code, lino, _exit);
  writer->f[TSDB_FTYPE_HEAD].size += blockIdx->size;

  code = TARRAY2_APPEND_PTR(writer->blockIdxArray, blockIdx);
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
  int32_t vid = TD_VID(writer->config->tsdb->pVnode);

  if (writer->ctx->tbHasOldData) {
    for (; writer->ctx->iRow < writer->ctx->bData->nRow; writer->ctx->iRow++) {
      TSDBROW row[1] = {tsdbRowFromBlockData(writer->ctx->bData, writer->ctx->iRow)};

      code = tBlockDataAppendRow(writer->bData, row, NULL, writer->ctx->tbid->uid);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tsdbDataFileWriteBlockData(writer, writer->bData);
    TSDB_CHECK_CODE(code, lino, _exit);

    for (; writer->ctx->dataBlkArrayIdx < TARRAY2_SIZE(writer->ctx->dataBlkArray); writer->ctx->dataBlkArrayIdx++) {
      code = TARRAY2_APPEND_PTR(writer->dataBlkArray,
                                TARRAY2_GET_PTR(writer->ctx->dataBlkArray, writer->ctx->dataBlkArrayIdx));
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  code = tsdbDataFileWriteBlockData(writer, writer->bData);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbDataFileWriteDataBlk(writer, writer->dataBlkArray);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(vid, lino, code);
  }
  return code;
}
static int32_t tsdbDataFileWriteTableDataBegin(SDataFileWriter *writer, const TABLEID *tbid) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(writer->config->tsdb->pVnode);

  for (; writer->ctx->blockIdxArrayIdx < TARRAY2_SIZE(writer->ctx->blockIdxArray); writer->ctx->blockIdxArrayIdx++) {
    const SBlockIdx *blockIdx = TARRAY2_GET_PTR(writer->ctx->blockIdxArray, writer->ctx->blockIdxArrayIdx);

    int32_t c = tTABLEIDCmprFn(blockIdx, tbid);
    if (c < 0) {
      SMetaInfo info;
      if (metaGetInfo(writer->config->tsdb->pVnode->pMeta, blockIdx->suid, &info, NULL) == 0) {
        code = tsdbDataFileReadDataBlk(writer->ctx->reader, blockIdx, &writer->ctx->dataBlkArray);
        TSDB_CHECK_CODE(code, lino, _exit);

        writer->ctx->tbid->suid = blockIdx->suid;
        writer->ctx->tbid->uid = blockIdx->uid;

        code = tsdbDataFileWriteDataBlk(writer, writer->ctx->dataBlkArray);
        TSDB_CHECK_CODE(code, lino, _exit);
      }
    } else {
      if (c == 0) {
        writer->ctx->tbHasOldData = true;
        code = tsdbDataFileReadDataBlk(writer->ctx->reader, blockIdx, &writer->ctx->dataBlkArray);
        TSDB_CHECK_CODE(code, lino, _exit);
        writer->ctx->dataBlkArrayIdx = 0;
      } else {
        writer->ctx->tbHasOldData = false;
      }
      break;
    }
  }
  writer->ctx->tbid[0] = tbid[0];

_exit:
  if (code) {
    TSDB_ERROR_LOG(vid, lino, code);
  }
  return code;
}
static int32_t tsdbDataFileDoWriteTableData(SDataFileWriter *writer, SBlockData *bData) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(writer->config->tsdb->pVnode);

  if (writer->ctx->tbHasOldData) {
    if (writer->ctx->dataBlkArrayIdx >= TARRAY2_SIZE(writer->ctx->dataBlkArray)) {
      // TODO
    }
    // TODO
  } else {
    // code = tsdbDataFileWriteBlockData(writer, bData);
    // TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(vid, lino, code);
  }
  return code;
}
int32_t tsdbDataFileWriteTSData(SDataFileWriter *writer, SBlockData *bData) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(writer->config->tsdb->pVnode);

  ASSERT(bData->uid);

  if (!writer->ctx->opened) {
    code = tsdbDataFileWriterDoOpen(writer);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (bData->uid != writer->ctx->tbid->uid) {
    code = tsdbDataFileWriteTableDataEnd(writer);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbDataFileWriteTableDataBegin(writer, (TABLEID *)bData);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tsdbDataFileDoWriteTableData(writer, bData);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(vid, lino, code);
  }
  return code;
}
