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

#include "inc/tsdbSttFileRW.h"

typedef struct {
  int64_t   prevFooter;
  SFDataPtr sttBlkPtr[1];
  SFDataPtr delBlkPtr[1];
  SFDataPtr statisBlkPtr[1];
  SFDataPtr rsrvd[2];
} SSttFooter;

// SSttFReader ============================================================
struct SSttFileReader {
  SSttFileReaderConfig config[1];
  TSttSegReaderArray   readerArray[1];
  STsdbFD             *fd;
};

struct SSttSegReader {
  SSttFileReader *reader;
  SSttFooter      footer[1];
  struct {
    bool sttBlkLoaded;
    bool delBlkLoaded;
    bool statisBlkLoaded;
  } ctx;
  TSttBlkArray    sttBlkArray[1];
  TDelBlkArray    delBlkArray[1];
  TStatisBlkArray statisBlkArray[1];
};

// SSttFileReader
static int32_t tsdbSttSegReaderOpen(SSttFileReader *reader, int64_t offset, SSttSegReader **segReader) {
  ASSERT(offset >= TSDB_FHDR_SIZE);

  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(reader->config->tsdb->pVnode);

  segReader[0] = taosMemoryCalloc(1, sizeof(*segReader[0]));
  if (!segReader[0]) return TSDB_CODE_OUT_OF_MEMORY;

  segReader[0]->reader = reader;
  code = tsdbReadFile(reader->fd, offset, (uint8_t *)(segReader[0]->footer), sizeof(SSttFooter));
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", vid, __func__, lino, tstrerror(code));
    taosMemoryFree(segReader[0]);
    segReader[0] = NULL;
  }
  return code;
}

static int32_t tsdbSttSegReaderClose(SSttSegReader **reader) {
  if (!reader[0]) return 0;

  if (reader[0]->ctx.sttBlkLoaded) {
    TARRAY2_FREE(reader[0]->sttBlkArray);
  }
  if (reader[0]->ctx.delBlkLoaded) {
    TARRAY2_FREE(reader[0]->delBlkArray);
  }
  if (reader[0]->ctx.statisBlkLoaded) {
    TARRAY2_FREE(reader[0]->statisBlkArray);
  }
  taosMemoryFree(reader[0]);
  reader[0] = NULL;
  return 0;
}

int32_t tsdbSttFReaderOpen(const char *fname, const SSttFileReaderConfig *config, SSttFileReader **reader) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(config->tsdb->pVnode);

  reader[0] = taosMemoryCalloc(1, sizeof(*reader[0]));
  if (reader[0] == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  reader[0]->config[0] = config[0];

  // open file
  code = tsdbOpenFile(fname, config->szPage, TD_FILE_READ, &reader[0]->fd);
  TSDB_CHECK_CODE(code, lino, _exit);

  // open each segment reader
  int64_t size = config->file->size;
  while (size > 0) {
    SSttSegReader *reader1;

    code = tsdbSttSegReaderOpen(reader[0], size - sizeof(SSttFooter), &reader1);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = TARRAY2_APPEND(reader[0]->readerArray, reader1);
    TSDB_CHECK_CODE(code, lino, _exit);

    size = reader1->footer->prevFooter;
  }

  ASSERT(TARRAY2_SIZE(reader[0]->readerArray) == config->file->stt->nseg);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", vid, __func__, lino, tstrerror(code));
    tsdbSttFReaderClose(reader);
  }
  return code;
}

int32_t tsdbSttFReaderClose(SSttFileReader **reader) {
  tsdbCloseFile(&reader[0]->fd);
  TARRAY2_CLEAR_FREE(reader[0]->readerArray, tsdbSttSegReaderClose);
  taosMemoryFree(reader[0]);
  reader[0] = NULL;
  return 0;
}

int32_t tsdbSttFReaderGetSegReader(SSttFileReader *reader, const TSttSegReaderArray **readerArray) {
  readerArray[0] = reader->readerArray;
  return 0;
}

// SSttFSegReader
int32_t tsdbSttFReadStatisBlk(SSttSegReader *reader, const TStatisBlkArray **statisBlkArray) {
  if (!reader->ctx.statisBlkLoaded) {
    if (reader->footer->statisBlkPtr->size > 0) {
      ASSERT(reader->footer->statisBlkPtr->size % sizeof(STbStatisBlk) == 0);

      int32_t size = reader->footer->statisBlkPtr->size / sizeof(STbStatisBlk);
      void   *data = taosMemoryMalloc(reader->footer->statisBlkPtr->size);
      if (!data) return TSDB_CODE_OUT_OF_MEMORY;

      int32_t code = tsdbReadFile(reader->reader->fd, reader->footer->statisBlkPtr->offset, data,
                                  reader->footer->statisBlkPtr->size);
      if (code) return code;

      TARRAY2_INIT_EX(reader->statisBlkArray, size, size, data);
    } else {
      TARRAY2_INIT(reader->statisBlkArray);
    }

    reader->ctx.statisBlkLoaded = true;
  }

  statisBlkArray[0] = reader->statisBlkArray;
  return 0;
}

int32_t tsdbSttFReadDelBlk(SSttSegReader *reader, const TDelBlkArray **delBlkArray) {
  if (!reader->ctx.delBlkLoaded) {
    if (reader->footer->delBlkPtr->size > 0) {
      ASSERT(reader->footer->delBlkPtr->size % sizeof(SDelBlk) == 0);

      int32_t size = reader->footer->delBlkPtr->size / sizeof(SDelBlk);
      void   *data = taosMemoryMalloc(reader->footer->delBlkPtr->size);
      if (!data) return TSDB_CODE_OUT_OF_MEMORY;

      int32_t code =
          tsdbReadFile(reader->reader->fd, reader->footer->delBlkPtr->offset, data, reader->footer->delBlkPtr->size);
      if (code) return code;

      TARRAY2_INIT_EX(reader->delBlkArray, size, size, data);
    } else {
      TARRAY2_INIT(reader->delBlkArray);
    }

    reader->ctx.delBlkLoaded = true;
  }

  delBlkArray[0] = reader->delBlkArray;
  return 0;
}

int32_t tsdbSttFReadSttBlk(SSttSegReader *reader, const TSttBlkArray **sttBlkArray) {
  if (!reader->ctx.sttBlkLoaded) {
    if (reader->footer->sttBlkPtr->size > 0) {
      ASSERT(reader->footer->sttBlkPtr->size % sizeof(SSttBlk) == 0);

      int32_t size = reader->footer->sttBlkPtr->size / sizeof(SSttBlk);
      void   *data = taosMemoryMalloc(reader->footer->sttBlkPtr->size);
      if (!data) return TSDB_CODE_OUT_OF_MEMORY;

      int32_t code =
          tsdbReadFile(reader->reader->fd, reader->footer->sttBlkPtr->offset, data, reader->footer->sttBlkPtr->size);
      if (code) return code;

      TARRAY2_INIT_EX(reader->sttBlkArray, size, size, data);
    } else {
      TARRAY2_INIT(reader->sttBlkArray);
    }

    reader->ctx.sttBlkLoaded = true;
  }

  sttBlkArray[0] = reader->sttBlkArray;
  return 0;
}

int32_t tsdbSttFReadSttBlock(SSttSegReader *reader, const SSttBlk *sttBlk, SBlockData *bData) {
  int32_t code = 0;
  // TODO
  return code;
}

int32_t tsdbSttFReadDelBlock(SSttSegReader *reader, const SDelBlk *delBlk, SDelBlock *dData) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(reader->reader->config->tsdb->pVnode);

  tDelBlockClear(dData);
  code = tRealloc(&reader->reader->config->aBuf[0], delBlk->dp->size);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbReadFile(reader->reader->fd, delBlk->dp->offset, reader->reader->config->aBuf[0], delBlk->dp->size);
  if (code) TSDB_CHECK_CODE(code, lino, _exit);

  int64_t size = 0;
  for (int32_t i = 0; i < ARRAY_SIZE(dData->aData); ++i) {
    code = tsdbDecmprData(reader->reader->config->aBuf[0] + size, delBlk->size[i], TSDB_DATA_TYPE_BIGINT,
                          TWO_STAGE_COMP, NULL, 0, NULL);  // TODO
    TSDB_CHECK_CODE(code, lino, _exit);

    size += delBlk->size[i];
  }

  ASSERT(size == delBlk->dp->size);
_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d, reason:%s", vid, __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t tsdbSttFReadStatisBlock(SSttSegReader *reader, const STbStatisBlk *statisBlk, STbStatisBlock *sData) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(reader->reader->config->tsdb->pVnode);

  tStatisBlockClear(sData);
  code = tRealloc(&reader->reader->config->aBuf[0], statisBlk->dp->size);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbReadFile(reader->reader->fd, statisBlk->dp->offset, reader->reader->config->aBuf[0], statisBlk->dp->size);
  if (code) TSDB_CHECK_CODE(code, lino, _exit);

  int64_t size = 0;
  for (int32_t i = 0; i < ARRAY_SIZE(sData->aData); ++i) {
    code = tsdbDecmprData(reader->reader->config->aBuf[0] + size, statisBlk->size[i], TSDB_DATA_TYPE_BIGINT,
                          TWO_STAGE_COMP, NULL, 0, NULL);  // TODO
    TSDB_CHECK_CODE(code, lino, _exit);

    size += statisBlk->size[i];
  }

  ASSERT(size == statisBlk->dp->size);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d, reason:%s", vid, __func__, lino, tstrerror(code));
  }
  return code;
}

// SSttFWriter ============================================================
struct SSttFileWriter {
  SSttFileWriterConfig config[1];
  struct {
    bool opened;
  } ctx[1];
  // file
  STFile file[1];
  // data
  TSttBlkArray    sttBlkArray[1];
  TDelBlkArray    delBlkArray[1];
  TStatisBlkArray statisBlkArray[1];
  SSttFooter      footer[1];
  SBlockData      bData[1];
  SDelBlock       dData[1];
  STbStatisBlock  sData[1];
  // helper data
  SSkmInfo skmTb[1];
  SSkmInfo skmRow[1];
  int32_t  aBufSize[5];
  uint8_t *aBuf[5];
  STsdbFD *fd;
};

static int32_t tsdbSttFileDoWriteTSDataBlock(SSttFileWriter *writer) {
  if (writer->bData->nRow == 0) return 0;

  int32_t code = 0;
  int32_t lino = 0;
  SSttBlk sttBlk[1];

  sttBlk->suid = writer->bData->suid;
  sttBlk->minUid = writer->bData->uid ? writer->bData->uid : writer->bData->aUid[0];
  sttBlk->maxUid = writer->bData->uid ? writer->bData->uid : writer->bData->aUid[writer->bData->nRow - 1];
  sttBlk->minKey = sttBlk->maxKey = writer->bData->aTSKEY[0];
  sttBlk->minVer = sttBlk->maxVer = writer->bData->aVersion[0];
  sttBlk->nRow = writer->bData->nRow;
  for (int32_t iRow = 1; iRow < writer->bData->nRow; iRow++) {
    if (sttBlk->minKey > writer->bData->aTSKEY[iRow]) sttBlk->minKey = writer->bData->aTSKEY[iRow];
    if (sttBlk->maxKey < writer->bData->aTSKEY[iRow]) sttBlk->maxKey = writer->bData->aTSKEY[iRow];
    if (sttBlk->minVer > writer->bData->aVersion[iRow]) sttBlk->minVer = writer->bData->aVersion[iRow];
    if (sttBlk->maxVer < writer->bData->aVersion[iRow]) sttBlk->maxVer = writer->bData->aVersion[iRow];
  }

  code = tCmprBlockData(writer->bData, writer->config->cmprAlg, NULL, NULL, writer->config->aBuf, writer->aBufSize);
  TSDB_CHECK_CODE(code, lino, _exit);

  sttBlk->bInfo.offset = writer->file->size;
  sttBlk->bInfo.szKey = writer->aBufSize[2] + writer->aBufSize[3];
  sttBlk->bInfo.szBlock = writer->aBufSize[0] + writer->aBufSize[1] + sttBlk->bInfo.szKey;

  for (int32_t i = 3; i >= 0; i--) {
    if (writer->aBufSize[i]) {
      code = tsdbWriteFile(writer->fd, writer->file->size, writer->config->aBuf[i], writer->aBufSize[i]);
      TSDB_CHECK_CODE(code, lino, _exit);
      writer->file->size += writer->aBufSize[i];
    }
  }
  tBlockDataClear(writer->bData);

  code = TARRAY2_APPEND_PTR(writer->sttBlkArray, sttBlk);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(writer->config->tsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

static int32_t tsdbSttFileDoWriteStatisBlock(SSttFileWriter *writer) {
  if (STATIS_BLOCK_SIZE(writer->sData)) return 0;

  int32_t code = 0;
  int32_t lino = 0;

  STbStatisBlk statisBlk[1] = {{
      .numRec = STATIS_BLOCK_SIZE(writer->sData),
      .minTid = {.suid = TARRAY2_FIRST(writer->sData->suid), .uid = TARRAY2_FIRST(writer->sData->uid)},
      .maxTid = {.suid = TARRAY2_LAST(writer->sData->suid), .uid = TARRAY2_LAST(writer->sData->uid)},
      .minVer = TARRAY2_FIRST(writer->sData->minVer),
      .maxVer = TARRAY2_FIRST(writer->sData->maxVer),
  }};

  for (int32_t i = 1; i < STATIS_BLOCK_SIZE(writer->sData); i++) {
    statisBlk->minVer = TMIN(statisBlk->minVer, TARRAY2_GET(writer->sData->minVer, i));
    statisBlk->maxVer = TMAX(statisBlk->maxVer, TARRAY2_GET(writer->sData->maxVer, i));
  }

  statisBlk->dp->offset = writer->file->size;
  statisBlk->dp->size = 0;

  for (int32_t i = 0; i < ARRAY_SIZE(writer->sData->aData); i++) {
    int32_t size;
    code = tsdbCmprData((uint8_t *)TARRAY2_DATA(&writer->sData->aData[i]), TARRAY2_DATA_LEN(&writer->sData->aData[i]),
                        TSDB_DATA_TYPE_BIGINT, TWO_STAGE_COMP, &writer->config->aBuf[0], 0, &size,
                        &writer->config->aBuf[1]);
    TSDB_CHECK_CODE(code, lino, _exit);
    statisBlk->size[i] = size;
    statisBlk->dp->size += size;
  }

  tStatisBlockClear(writer->sData);

  code = TARRAY2_APPEND_PTR(writer->statisBlkArray, statisBlk);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(writer->config->tsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

static int32_t tsdbSttFileDoWriteDelBlock(SSttFileWriter *writer) {
#if 0
  if (writer->dData->nRow == 0) return 0;

  int32_t code = 0;
  int32_t lino;

  SDelBlk delBlk[1];

  delBlk->nRow = writer->sData->nRow;
  delBlk->minTid.suid = writer->sData->aData[0][0];
  delBlk->minTid.uid = writer->sData->aData[1][0];
  delBlk->maxTid.suid = writer->sData->aData[0][writer->sData->nRow - 1];
  delBlk->maxTid.uid = writer->sData->aData[1][writer->sData->nRow - 1];
  delBlk->minVer = delBlk->maxVer = delBlk->maxVer = writer->sData->aData[2][0];
  for (int32_t iRow = 1; iRow < writer->sData->nRow; iRow++) {
    if (delBlk->minVer > writer->sData->aData[2][iRow]) delBlk->minVer = writer->sData->aData[2][iRow];
    if (delBlk->maxVer < writer->sData->aData[2][iRow]) delBlk->maxVer = writer->sData->aData[2][iRow];
  }

  delBlk->dp.offset = writer->file->size;
  delBlk->dp.size = 0;  // TODO

  int64_t tsize = sizeof(int64_t) * writer->dData->nRow;
  for (int32_t i = 0; i < ARRAY_SIZE(writer->dData->aData); i++) {
    code = tsdbWriteFile(writer->fd, writer->file->size, (const uint8_t *)writer->dData->aData[i], tsize);
    TSDB_CHECK_CODE(code, lino, _exit);

    delBlk->dp.size += tsize;
    writer->file->size += tsize;
  }
  tDelBlockDestroy(writer->dData);

  code = TARRAY2_APPEND_PTR(writer->delBlkArray, delBlk);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(writer->config->tsdb->pVnode), __func__, lino,
              tstrerror(code));
  } else {
    // tsdbTrace();
  }
  return code;
#endif
  return 0;
}

static int32_t tsdbSttFileDoWriteSttBlk(SSttFileWriter *writer) {
  int32_t code = 0;
  int32_t lino;

  writer->footer->sttBlkPtr->offset = writer->file->size;
  writer->footer->sttBlkPtr->size = TARRAY2_DATA_LEN(writer->sttBlkArray);

  if (writer->footer->sttBlkPtr->size) {
    code = tsdbWriteFile(writer->fd, writer->file->size, (const uint8_t *)TARRAY2_DATA(writer->sttBlkArray),
                         writer->footer->sttBlkPtr->size);
    TSDB_CHECK_CODE(code, lino, _exit);
    writer->file->size += writer->footer->sttBlkPtr->size;
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(writer->config->tsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

static int32_t tsdbSttFileDoWriteStatisBlk(SSttFileWriter *writer) {
  int32_t code = 0;
  int32_t lino;

  writer->footer->statisBlkPtr->offset = writer->file->size;
  writer->footer->statisBlkPtr->size = TARRAY2_DATA_LEN(writer->statisBlkArray);

  if (writer->footer->statisBlkPtr->size) {
    code = tsdbWriteFile(writer->fd, writer->file->size, (const uint8_t *)TARRAY2_DATA(writer->statisBlkArray),
                         writer->footer->statisBlkPtr->size);
    TSDB_CHECK_CODE(code, lino, _exit);
    writer->file->size += writer->footer->statisBlkPtr->size;
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(writer->config->tsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

static int32_t tsdbSttFileDoWriteDelBlk(SSttFileWriter *writer) {
  int32_t code = 0;
  int32_t lino;

  writer->footer->delBlkPtr->offset = writer->file->size;
  writer->footer->delBlkPtr->size = TARRAY2_DATA_LEN(writer->delBlkArray);

  if (writer->footer->delBlkPtr->size) {
    code = tsdbWriteFile(writer->fd, writer->file->size, (const uint8_t *)TARRAY2_DATA(writer->delBlkArray),
                         writer->footer->delBlkPtr->size);
    TSDB_CHECK_CODE(code, lino, _exit);
    writer->file->size += writer->footer->delBlkPtr->size;
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(writer->config->tsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

static int32_t tsdbSttFileDoWriteFooter(SSttFileWriter *writer) {
  int32_t code =
      tsdbWriteFile(writer->fd, writer->file->size, (const uint8_t *)&writer->footer, sizeof(writer->footer));
  writer->file->size += sizeof(writer->footer);
  return code;
}

static int32_t tsdbSttFWriterDoOpen(SSttFileWriter *writer) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(writer->config->tsdb->pVnode);

  // set
  writer->file[0] = writer->config->file;
  writer->file->stt->nseg++;
  if (!writer->config->skmTb) writer->config->skmTb = writer->skmTb;
  if (!writer->config->skmRow) writer->config->skmRow = writer->skmRow;
  if (!writer->config->aBuf) writer->config->aBuf = writer->aBuf;

  // open file
  int32_t flag;
  char    fname[TSDB_FILENAME_LEN];

  if (writer->file->size) {
    flag = TD_FILE_READ | TD_FILE_WRITE;
  } else {
    flag = TD_FILE_READ | TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC;
  }

  tsdbTFileName(writer->config->tsdb, writer->file, fname);
  code = tsdbOpenFile(fname, writer->config->szPage, flag, &writer->fd);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (!writer->file->size) {
    uint8_t hdr[TSDB_FHDR_SIZE] = {0};

    code = tsdbWriteFile(writer->fd, 0, hdr, sizeof(hdr));
    TSDB_CHECK_CODE(code, lino, _exit);
    writer->file->size += sizeof(hdr);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", vid, __func__, lino, tstrerror(code));
  } else {
    writer->ctx->opened = true;
  }
  return 0;
}

static void tsdbSttFWriterDoClose(SSttFileWriter *writer) {
  ASSERT(!writer->fd);

  for (int32_t i = 0; i < ARRAY_SIZE(writer->aBufSize); ++i) {
    tFree(writer->aBuf[i]);
  }
  tDestroyTSchema(writer->skmRow->pTSchema);
  tDestroyTSchema(writer->skmTb->pTSchema);
  tStatisBlockFree(writer->sData);
  tDelBlockFree(writer->dData);
  tBlockDataDestroy(writer->bData);
  TARRAY2_FREE(writer->statisBlkArray);
  TARRAY2_FREE(writer->delBlkArray);
  TARRAY2_FREE(writer->sttBlkArray);
}

static int32_t tsdbSttFileDoUpdateHeader(SSttFileWriter *writer) {
  // TODO
  return 0;
}

static int32_t tsdbSttFWriterCloseCommit(SSttFileWriter *writer, STFileOp *op) {
  int32_t lino;
  int32_t code;
  int32_t vid = TD_VID(writer->config->tsdb->pVnode);

  code = tsdbSttFileDoWriteTSDataBlock(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbSttFileDoWriteStatisBlock(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbSttFileDoWriteDelBlock(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbSttFileDoWriteSttBlk(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbSttFileDoWriteStatisBlk(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbSttFileDoWriteDelBlk(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbSttFileDoWriteFooter(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbSttFileDoUpdateHeader(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbFsyncFile(writer->fd);
  TSDB_CHECK_CODE(code, lino, _exit);

  tsdbCloseFile(&writer->fd);

  ASSERT(writer->config->file.size > writer->file->size);
  op->optype = writer->config->file.size ? TSDB_FOP_MODIFY : TSDB_FOP_CREATE;
  op->fid = writer->config->file.fid;
  op->of = writer->config->file;
  op->nf = writer->file[0];

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", vid, __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbSttFWriterCloseAbort(SSttFileWriter *writer) {
  if (writer->config->file.size) {  // truncate the file to the original size
    ASSERT(writer->config->file.size <= writer->file->size);
    if (writer->config->file.size < writer->file->size) {
      taosFtruncateFile(writer->fd->pFD, writer->config->file.size);
      tsdbCloseFile(&writer->fd);
    }
  } else {  // remove the file
    char fname[TSDB_FILENAME_LEN];
    tsdbTFileName(writer->config->tsdb, &writer->config->file, fname);
    tsdbCloseFile(&writer->fd);
    taosRemoveFile(fname);
  }

  return 0;
}

int32_t tsdbSttFWriterOpen(const SSttFileWriterConfig *config, SSttFileWriter **writer) {
  writer[0] = taosMemoryCalloc(1, sizeof(*writer[0]));
  if (writer[0] == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  writer[0]->config[0] = config[0];
  writer[0]->ctx->opened = false;
  return 0;
}

int32_t tsdbSttFWriterClose(SSttFileWriter **writer, int8_t abort, STFileOp *op) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(writer[0]->config->tsdb->pVnode);

  if (!writer[0]->ctx->opened) {
    op->optype = TSDB_FOP_NONE;
  } else {
    if (abort) {
      code = tsdbSttFWriterCloseAbort(writer[0]);
      TSDB_CHECK_CODE(code, lino, _exit);
    } else {
      code = tsdbSttFWriterCloseCommit(writer[0], op);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    tsdbSttFWriterDoClose(writer[0]);
  }
  taosMemoryFree(writer[0]);
  writer[0] = NULL;

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", vid, __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t tsdbSttFWriteTSData(SSttFileWriter *writer, SRowInfo *row) {
  int32_t code = 0;
  int32_t lino = 0;

  if (!writer->ctx->opened) {
    code = tsdbSttFWriterDoOpen(writer);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  TSDBROW *pRow = &row->row;
  TSDBKEY  key = TSDBROW_KEY(pRow);
  if (!TABLE_SAME_SCHEMA(writer->bData->suid, writer->bData->uid, row->suid, row->uid)) {
    code = tsdbSttFileDoWriteTSDataBlock(writer);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (STATIS_BLOCK_SIZE(writer->sData) >= writer->config->maxRow) {
      code = tsdbSttFileDoWriteStatisBlock(writer);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    STbStatisRecord record[1] = {{
        .suid = row->suid,
        .uid = row->uid,
        .firstKey = key.ts,
        .firstVer = key.version,
        .lastKey = key.ts,
        .lastVer = key.version,
        .minVer = key.version,
        .maxVer = key.version,
        .count = 1,
    }};
    code = tStatisBlockPut(writer->sData, record);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbUpdateSkmTb(writer->config->tsdb, (TABLEID *)row, writer->config->skmTb);
    TSDB_CHECK_CODE(code, lino, _exit);

    TABLEID id = {.suid = row->suid, .uid = row->suid ? 0 : row->uid};
    code = tBlockDataInit(writer->bData, &id, writer->config->skmTb->pTSchema, NULL, 0);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (row->row.type == TSDBROW_ROW_FMT) {
    code = tsdbUpdateSkmRow(writer->config->tsdb, (TABLEID *)row, TSDBROW_SVERSION(pRow), writer->config->skmRow);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tBlockDataAppendRow(writer->bData, pRow, writer->config->skmRow->pTSchema, row->uid);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (writer->bData->nRow >= writer->config->maxRow) {
    code = tsdbSttFileDoWriteTSDataBlock(writer);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  TARRAY2_LAST(writer->sData->minVer) = TMIN(TARRAY2_LAST(writer->sData->minVer), key.version);
  TARRAY2_LAST(writer->sData->maxVer) = TMAX(TARRAY2_LAST(writer->sData->maxVer), key.version);
  if (key.ts > TARRAY2_LAST(writer->sData->lastKey)) {
    TARRAY2_LAST(writer->sData->lastKey) = key.ts;
    TARRAY2_LAST(writer->sData->lastVer) = key.version;
    TARRAY2_LAST(writer->sData->aCount)++;
  } else if (key.ts == TARRAY2_LAST(writer->sData->lastKey)) {
    TARRAY2_LAST(writer->sData->lastVer) = key.version;
  } else {
    ASSERTS(0, "timestamp should be in ascending order");
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(writer->config->tsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return code;
}

int32_t tsdbSttFWriteTSDataBlock(SSttFileWriter *writer, SBlockData *bdata) {
  int32_t code = 0;
  int32_t lino = 0;

  SRowInfo row[1];
  row->suid = bdata->suid;
  for (int32_t i = 0; i < bdata->nRow; i++) {
    row->uid = bdata->uid ? bdata->uid : bdata->aUid[i];
    row->row = tsdbRowFromBlockData(bdata, i);

    code = tsdbSttFWriteTSData(writer, row);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at line %d since %s", TD_VID(writer->config->tsdb->pVnode), __func__, lino,
              tstrerror(code));
  }
  return 0;
}

int32_t tsdbSttFWriteDLData(SSttFileWriter *writer, TABLEID *tbid, SDelData *pDelData) {
  ASSERTS(0, "TODO: Not implemented yet");

  int32_t code;
  if (!writer->ctx->opened) {
    code = tsdbSttFWriterDoOpen(writer);
    return code;
  }

  // writer->dData[0].aData[0][writer->dData[0].nRow] = tbid->suid;         // suid
  // writer->dData[0].aData[1][writer->dData[0].nRow] = tbid->uid;          // uid
  // writer->dData[0].aData[2][writer->dData[0].nRow] = pDelData->version;  // version
  // writer->dData[0].aData[3][writer->dData[0].nRow] = pDelData->sKey;     // skey
  // writer->dData[0].aData[4][writer->dData[0].nRow] = pDelData->eKey;     // ekey
  // writer->dData[0].nRow++;

  // if (writer->dData[0].nRow >= writer->config->maxRow) {
  //   return tsdbSttFileDoWriteDelBlock(writer);
  // } else {
  //   return 0;
  // }
  return 0;
}