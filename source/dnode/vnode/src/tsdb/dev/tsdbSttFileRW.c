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
  SFDataPtr statisBlkPtr[1];
  SFDataPtr tombBlkPtr[1];
  SFDataPtr rsrvd[2];
} SSttFooter;

// SSttFReader ============================================================
struct SSttFileReader {
  SSttFileReaderConfig config[1];
  TSttSegReaderArray   readerArray[1];
  STsdbFD             *fd;
  uint8_t             *bufArr[5];
};

struct SSttSegReader {
  SSttFileReader *reader;
  SSttFooter      footer[1];
  struct {
    bool sttBlkLoaded;
    bool statisBlkLoaded;
    bool tombBlkLoaded;
  } ctx[1];
  TSttBlkArray    sttBlkArray[1];
  TStatisBlkArray statisBlkArray[1];
  TTombBlkArray   tombBlkArray[1];
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
    TSDB_ERROR_LOG(vid, lino, code);
    taosMemoryFree(segReader[0]);
    segReader[0] = NULL;
  }
  return code;
}

static int32_t tsdbSttSegReaderClose(SSttSegReader **reader) {
  if (reader[0]) {
    TARRAY2_FREE(reader[0]->sttBlkArray);
    TARRAY2_FREE(reader[0]->tombBlkArray);
    TARRAY2_FREE(reader[0]->statisBlkArray);
    taosMemoryFree(reader[0]);
    reader[0] = NULL;
  }
  return 0;
}

int32_t tsdbSttFileReaderOpen(const char *fname, const SSttFileReaderConfig *config, SSttFileReader **reader) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(config->tsdb->pVnode);

  reader[0] = taosMemoryCalloc(1, sizeof(*reader[0]));
  if (reader[0] == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  reader[0]->config[0] = config[0];
  if (!reader[0]->config->bufArr) reader[0]->config->bufArr = reader[0]->bufArr;

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
    TSDB_ERROR_LOG(vid, lino, code);
    tsdbSttFileReaderClose(reader);
  }
  return code;
}

int32_t tsdbSttFileReaderClose(SSttFileReader **reader) {
  if (reader[0]) {
    for (int32_t i = 0; i < ARRAY_SIZE(reader[0]->bufArr); ++i) {
      tFree(reader[0]->bufArr[i]);
    }
    tsdbCloseFile(&reader[0]->fd);
    TARRAY2_CLEAR_FREE(reader[0]->readerArray, tsdbSttSegReaderClose);
    taosMemoryFree(reader[0]);
    reader[0] = NULL;
  }
  return 0;
}

int32_t tsdbSttFileReaderGetSegReader(SSttFileReader *reader, const TSttSegReaderArray **readerArray) {
  readerArray[0] = reader->readerArray;
  return 0;
}

// SSttFSegReader
int32_t tsdbSttFileReadStatisBlk(SSttSegReader *reader, const TStatisBlkArray **statisBlkArray) {
  if (!reader->ctx->statisBlkLoaded) {
    if (reader->footer->statisBlkPtr->size > 0) {
      ASSERT(reader->footer->statisBlkPtr->size % sizeof(SStatisBlk) == 0);

      int32_t size = reader->footer->statisBlkPtr->size / sizeof(SStatisBlk);
      void   *data = taosMemoryMalloc(reader->footer->statisBlkPtr->size);
      if (!data) return TSDB_CODE_OUT_OF_MEMORY;

      int32_t code = tsdbReadFile(reader->reader->fd, reader->footer->statisBlkPtr->offset, data,
                                  reader->footer->statisBlkPtr->size);
      if (code) {
        taosMemoryFree(data);
        return code;
      }

      TARRAY2_INIT_EX(reader->statisBlkArray, size, size, data);
    } else {
      TARRAY2_INIT(reader->statisBlkArray);
    }

    reader->ctx->statisBlkLoaded = true;
  }

  statisBlkArray[0] = reader->statisBlkArray;
  return 0;
}

int32_t tsdbSttFileReadTombBlk(SSttSegReader *reader, const TTombBlkArray **tombBlkArray) {
  if (!reader->ctx->tombBlkLoaded) {
    if (reader->footer->tombBlkPtr->size > 0) {
      ASSERT(reader->footer->tombBlkPtr->size % sizeof(STombBlk) == 0);

      int32_t size = reader->footer->tombBlkPtr->size / sizeof(STombBlk);
      void   *data = taosMemoryMalloc(reader->footer->tombBlkPtr->size);
      if (!data) return TSDB_CODE_OUT_OF_MEMORY;

      int32_t code =
          tsdbReadFile(reader->reader->fd, reader->footer->tombBlkPtr->offset, data, reader->footer->tombBlkPtr->size);
      if (code) {
        taosMemoryFree(data);
        return code;
      }

      TARRAY2_INIT_EX(reader->tombBlkArray, size, size, data);
    } else {
      TARRAY2_INIT(reader->tombBlkArray);
    }

    reader->ctx->tombBlkLoaded = true;
  }

  tombBlkArray[0] = reader->tombBlkArray;
  return 0;
}

int32_t tsdbSttFileReadSttBlk(SSttSegReader *reader, const TSttBlkArray **sttBlkArray) {
  if (!reader->ctx->sttBlkLoaded) {
    if (reader->footer->sttBlkPtr->size > 0) {
      ASSERT(reader->footer->sttBlkPtr->size % sizeof(SSttBlk) == 0);

      int32_t size = reader->footer->sttBlkPtr->size / sizeof(SSttBlk);
      void   *data = taosMemoryMalloc(reader->footer->sttBlkPtr->size);
      if (!data) return TSDB_CODE_OUT_OF_MEMORY;

      int32_t code =
          tsdbReadFile(reader->reader->fd, reader->footer->sttBlkPtr->offset, data, reader->footer->sttBlkPtr->size);
      if (code) {
        taosMemoryFree(data);
        return code;
      }

      TARRAY2_INIT_EX(reader->sttBlkArray, size, size, data);
    } else {
      TARRAY2_INIT(reader->sttBlkArray);
    }

    reader->ctx->sttBlkLoaded = true;
  }

  sttBlkArray[0] = reader->sttBlkArray;
  return 0;
}

int32_t tsdbSttFileReadDataBlock(SSttSegReader *reader, const SSttBlk *sttBlk, SBlockData *bData) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tRealloc(&reader->reader->config->bufArr[0], sttBlk->bInfo.szBlock);
  TSDB_CHECK_CODE(code, lino, _exit);

  code =
      tsdbReadFile(reader->reader->fd, sttBlk->bInfo.offset, reader->reader->config->bufArr[0], sttBlk->bInfo.szBlock);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tDecmprBlockData(reader->reader->config->bufArr[0], sttBlk->bInfo.szBlock, bData,
                          &reader->reader->config->bufArr[1]);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(reader->reader->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbSttFileReadTombBlock(SSttSegReader *reader, const STombBlk *tombBlk, STombBlock *dData) {
  int32_t code = 0;
  int32_t lino = 0;

  tTombBlockClear(dData);

  code = tRealloc(&reader->reader->config->bufArr[0], tombBlk->dp->size);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbReadFile(reader->reader->fd, tombBlk->dp->offset, reader->reader->config->bufArr[0], tombBlk->dp->size);
  if (code) TSDB_CHECK_CODE(code, lino, _exit);

  int64_t size = 0;
  for (int32_t i = 0; i < ARRAY_SIZE(dData->dataArr); ++i) {
    code = tsdbDecmprData(reader->reader->config->bufArr[0] + size, tombBlk->size[i], TSDB_DATA_TYPE_BIGINT,
                          TWO_STAGE_COMP, &reader->reader->config->bufArr[1], sizeof(int64_t) * tombBlk->numRec,
                          &reader->reader->config->bufArr[2]);
    TSDB_CHECK_CODE(code, lino, _exit);

    for (int32_t j = 0; j < tombBlk->numRec; ++j) {
      code = TARRAY2_APPEND(&dData->dataArr[i], ((int64_t *)(reader->reader->config->bufArr[1]))[j]);
      continue;
    }

    size += tombBlk->size[i];
  }

  ASSERT(size == tombBlk->dp->size);
_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(reader->reader->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbSttFileReadStatisBlock(SSttSegReader *reader, const SStatisBlk *statisBlk, STbStatisBlock *sData) {
  int32_t code = 0;
  int32_t lino = 0;

  tStatisBlockClear(sData);

  code = tRealloc(&reader->reader->config->bufArr[0], statisBlk->dp->size);
  TSDB_CHECK_CODE(code, lino, _exit);

  code =
      tsdbReadFile(reader->reader->fd, statisBlk->dp->offset, reader->reader->config->bufArr[0], statisBlk->dp->size);
  if (code) TSDB_CHECK_CODE(code, lino, _exit);

  int64_t size = 0;
  for (int32_t i = 0; i < ARRAY_SIZE(sData->dataArr); ++i) {
    code = tsdbDecmprData(reader->reader->config->bufArr[0] + size, statisBlk->size[i], TSDB_DATA_TYPE_BIGINT,
                          TWO_STAGE_COMP, &reader->reader->config->bufArr[1], sizeof(int64_t) * statisBlk->numRec,
                          &reader->reader->config->bufArr[2]);
    TSDB_CHECK_CODE(code, lino, _exit);

    for (int32_t j = 0; j < statisBlk->numRec; ++j) {
      code = TARRAY2_APPEND(sData->dataArr + i, ((int64_t *)reader->reader->config->bufArr[1])[j]);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    size += statisBlk->size[i];
  }

  ASSERT(size == statisBlk->dp->size);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(reader->reader->config->tsdb->pVnode), lino, code);
  }
  return code;
}

// SSttFWriter ============================================================
struct SSttFileWriter {
  SSttFileWriterConfig config[1];
  struct {
    bool    opened;
    TABLEID tbid[1];
  } ctx[1];
  // file
  STFile file[1];
  // data
  TSttBlkArray    sttBlkArray[1];
  TStatisBlkArray statisBlkArray[1];
  TTombBlkArray   tombBlkArray[1];
  SSttFooter      footer[1];
  SBlockData      bData[1];
  STbStatisBlock  sData[1];
  STombBlock      dData[1];
  // helper data
  SSkmInfo skmTb[1];
  SSkmInfo skmRow[1];
  int32_t  sizeArr[5];
  uint8_t *bufArr[5];
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

  code = tCmprBlockData(writer->bData, writer->config->cmprAlg, NULL, NULL, writer->config->aBuf, writer->sizeArr);
  TSDB_CHECK_CODE(code, lino, _exit);

  sttBlk->bInfo.offset = writer->file->size;
  sttBlk->bInfo.szKey = writer->sizeArr[2] + writer->sizeArr[3];
  sttBlk->bInfo.szBlock = writer->sizeArr[0] + writer->sizeArr[1] + sttBlk->bInfo.szKey;

  for (int32_t i = 3; i >= 0; i--) {
    if (writer->sizeArr[i]) {
      code = tsdbWriteFile(writer->fd, writer->file->size, writer->config->aBuf[i], writer->sizeArr[i]);
      TSDB_CHECK_CODE(code, lino, _exit);
      writer->file->size += writer->sizeArr[i];
    }
  }

  code = TARRAY2_APPEND_PTR(writer->sttBlkArray, sttBlk);
  TSDB_CHECK_CODE(code, lino, _exit);

  tBlockDataClear(writer->bData);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbSttFileDoWriteStatisBlock(SSttFileWriter *writer) {
  if (STATIS_BLOCK_SIZE(writer->sData)) return 0;

  int32_t code = 0;
  int32_t lino = 0;

  SStatisBlk statisBlk[1] = {{
      .numRec = STATIS_BLOCK_SIZE(writer->sData),
      .minTbid =
          {
              .suid = TARRAY2_FIRST(writer->sData->suid),
              .uid = TARRAY2_FIRST(writer->sData->uid),
          },
      .maxTbid =
          {
              .suid = TARRAY2_LAST(writer->sData->suid),
              .uid = TARRAY2_LAST(writer->sData->uid),
          },
      .minVer = TARRAY2_FIRST(writer->sData->minVer),
      .maxVer = TARRAY2_FIRST(writer->sData->maxVer),
  }};

  for (int32_t i = 1; i < STATIS_BLOCK_SIZE(writer->sData); i++) {
    statisBlk->minVer = TMIN(statisBlk->minVer, TARRAY2_GET(writer->sData->minVer, i));
    statisBlk->maxVer = TMAX(statisBlk->maxVer, TARRAY2_GET(writer->sData->maxVer, i));
  }

  statisBlk->dp->offset = writer->file->size;
  statisBlk->dp->size = 0;

  for (int32_t i = 0; i < STATIS_RECORD_NUM_ELEM; i++) {
    int32_t size;
    code = tsdbCmprData((uint8_t *)TARRAY2_DATA(&writer->sData->dataArr[i]),
                        TARRAY2_DATA_LEN(&writer->sData->dataArr[i]), TSDB_DATA_TYPE_BIGINT, TWO_STAGE_COMP,
                        &writer->config->aBuf[0], 0, &size, &writer->config->aBuf[1]);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbWriteFile(writer->fd, writer->file->size, writer->config->aBuf[0], size);
    TSDB_CHECK_CODE(code, lino, _exit);

    statisBlk->size[i] = size;
    statisBlk->dp->size += size;
    writer->file->size += size;
  }

  code = TARRAY2_APPEND_PTR(writer->statisBlkArray, statisBlk);
  TSDB_CHECK_CODE(code, lino, _exit);

  tStatisBlockClear(writer->sData);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbSttFileDoWriteTombBlock(SSttFileWriter *writer) {
  if (TOMB_BLOCK_SIZE(writer->dData) == 0) return 0;

  int32_t code = 0;
  int32_t lino = 0;

  STombBlk tombBlk[1] = {{
      .numRec = TOMB_BLOCK_SIZE(writer->dData),
      .minTid =
          {
              .suid = TARRAY2_FIRST(writer->dData->suid),
              .uid = TARRAY2_FIRST(writer->dData->uid),
          },
      .maxTid =
          {
              .suid = TARRAY2_LAST(writer->dData->suid),
              .uid = TARRAY2_LAST(writer->dData->uid),
          },
      .minVer = TARRAY2_FIRST(writer->dData->version),
      .maxVer = TARRAY2_FIRST(writer->dData->version),
      .dp[0] =
          {
              .offset = writer->file->size,
              .size = 0,
          },
  }};

  for (int32_t i = 1; i < TOMB_BLOCK_SIZE(writer->dData); i++) {
    tombBlk->minVer = TMIN(tombBlk->minVer, TARRAY2_GET(writer->dData->version, i));
    tombBlk->maxVer = TMAX(tombBlk->maxVer, TARRAY2_GET(writer->dData->version, i));
  }

  for (int32_t i = 0; i < ARRAY_SIZE(writer->dData->dataArr); i++) {
    int32_t size;
    code = tsdbCmprData((uint8_t *)TARRAY2_DATA(&writer->dData->dataArr[i]),
                        TARRAY2_DATA_LEN(&writer->dData->dataArr[i]), TSDB_DATA_TYPE_BIGINT, TWO_STAGE_COMP,
                        &writer->config->aBuf[0], 0, &size, &writer->config->aBuf[1]);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbWriteFile(writer->fd, writer->file->size, writer->config->aBuf[0], size);
    TSDB_CHECK_CODE(code, lino, _exit);

    tombBlk->size[i] = size;
    tombBlk->dp[0].size += size;
    writer->file->size += size;
  }

  code = TARRAY2_APPEND_PTR(writer->tombBlkArray, tombBlk);
  TSDB_CHECK_CODE(code, lino, _exit);

  tTombBlockClear(writer->dData);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
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
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
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
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbSttFileDoWriteTombBlk(SSttFileWriter *writer) {
  int32_t code = 0;
  int32_t lino = 0;

  writer->footer->tombBlkPtr->offset = writer->file->size;
  writer->footer->tombBlkPtr->size = TARRAY2_DATA_LEN(writer->tombBlkArray);

  if (writer->footer->tombBlkPtr->size) {
    code = tsdbWriteFile(writer->fd, writer->file->size, (const uint8_t *)TARRAY2_DATA(writer->tombBlkArray),
                         writer->footer->tombBlkPtr->size);
    TSDB_CHECK_CODE(code, lino, _exit);
    writer->file->size += writer->footer->tombBlkPtr->size;
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbSttFileDoWriteFooter(SSttFileWriter *writer) {
  writer->footer->prevFooter = writer->config->file.size;
  int32_t code = tsdbWriteFile(writer->fd, writer->file->size, (const uint8_t *)writer->footer, sizeof(writer->footer));
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
  if (!writer->config->aBuf) writer->config->aBuf = writer->bufArr;

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
    TSDB_ERROR_LOG(vid, lino, code);
  } else {
    writer->ctx->opened = true;
  }
  return code;
}

static void tsdbSttFWriterDoClose(SSttFileWriter *writer) {
  ASSERT(!writer->fd);

  for (int32_t i = 0; i < ARRAY_SIZE(writer->sizeArr); ++i) {
    tFree(writer->bufArr[i]);
  }
  tDestroyTSchema(writer->skmRow->pTSchema);
  tDestroyTSchema(writer->skmTb->pTSchema);
  tStatisBlockFree(writer->sData);
  tTombBlockFree(writer->dData);
  tBlockDataDestroy(writer->bData);
  TARRAY2_FREE(writer->tombBlkArray);
  TARRAY2_FREE(writer->statisBlkArray);
  TARRAY2_FREE(writer->sttBlkArray);
}

static int32_t tsdbSttFileDoUpdateHeader(SSttFileWriter *writer) {
  // TODO
  return 0;
}

static int32_t tsdbSttFWriterCloseCommit(SSttFileWriter *writer, TFileOpArray *opArray) {
  int32_t lino;
  int32_t code;

  code = tsdbSttFileDoWriteTSDataBlock(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbSttFileDoWriteStatisBlock(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbSttFileDoWriteTombBlock(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbSttFileDoWriteSttBlk(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbSttFileDoWriteStatisBlk(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbSttFileDoWriteTombBlk(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbSttFileDoWriteFooter(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbSttFileDoUpdateHeader(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbFsyncFile(writer->fd);
  TSDB_CHECK_CODE(code, lino, _exit);

  tsdbCloseFile(&writer->fd);

  ASSERT(writer->config->file.size < writer->file->size);
  STFileOp op = {
      .optype = writer->config->file.size ? TSDB_FOP_MODIFY : TSDB_FOP_CREATE,
      .fid = writer->config->file.fid,
      .of = writer->config->file,
      .nf = writer->file[0],
  };

  code = TARRAY2_APPEND(opArray, op);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
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

int32_t tsdbSttFileWriterOpen(const SSttFileWriterConfig *config, SSttFileWriter **writer) {
  writer[0] = taosMemoryCalloc(1, sizeof(*writer[0]));
  if (writer[0] == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  writer[0]->config[0] = config[0];
  writer[0]->ctx->opened = false;
  return 0;
}

int32_t tsdbSttFileWriterClose(SSttFileWriter **writer, int8_t abort, TFileOpArray *opArray) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(writer[0]->config->tsdb->pVnode);

  if (writer[0]->ctx->opened) {
    if (abort) {
      code = tsdbSttFWriterCloseAbort(writer[0]);
      TSDB_CHECK_CODE(code, lino, _exit);
    } else {
      code = tsdbSttFWriterCloseCommit(writer[0], opArray);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    tsdbSttFWriterDoClose(writer[0]);
  }
  taosMemoryFree(writer[0]);
  writer[0] = NULL;

_exit:
  if (code) {
    TSDB_ERROR_LOG(vid, lino, code);
  }
  return code;
}

int32_t tsdbSttFileWriteTSData(SSttFileWriter *writer, SRowInfo *row) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t vid = TD_VID(writer->config->tsdb->pVnode);

  if (!writer->ctx->opened) {
    code = tsdbSttFWriterDoOpen(writer);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  TSDBKEY key[1] = {TSDBROW_KEY(&row->row)};
  if (!TABLE_SAME_SCHEMA(row->suid, row->uid, writer->ctx->tbid->suid, writer->ctx->tbid->uid)) {
    code = tsdbSttFileDoWriteTSDataBlock(writer);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbUpdateSkmTb(writer->config->tsdb, (TABLEID *)row, writer->config->skmTb);
    TSDB_CHECK_CODE(code, lino, _exit);

    TABLEID id = {.suid = row->suid, .uid = row->suid ? 0 : row->uid};
    code = tBlockDataInit(writer->bData, &id, writer->config->skmTb->pTSchema, NULL, 0);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (writer->ctx->tbid->uid != row->uid) {
    writer->ctx->tbid->suid = row->suid;
    writer->ctx->tbid->uid = row->uid;

    if (STATIS_BLOCK_SIZE(writer->sData) >= writer->config->maxRow) {
      code = tsdbSttFileDoWriteStatisBlock(writer);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    STbStatisRecord record[1] = {{
        .suid = row->suid,
        .uid = row->uid,
        .firstKey = key->ts,
        .firstKeyVer = key->version,
        .lastKey = key->ts,
        .lastKeyVer = key->version,
        .minVer = key->version,
        .maxVer = key->version,
        .count = 1,
    }};
    code = tStatisBlockPut(writer->sData, record);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (row->row.type == TSDBROW_ROW_FMT) {
    code = tsdbUpdateSkmRow(writer->config->tsdb, writer->ctx->tbid,  //
                            TSDBROW_SVERSION(&row->row), writer->config->skmRow);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // row to col conversion
  if (key->version <= writer->config->compactVersion                       //
      && writer->bData->nRow > 0                                           //
      && (writer->bData->uid                                               //
              ? writer->bData->uid                                         //
              : writer->bData->aUid[writer->bData->nRow - 1]) == row->uid  //
      && writer->bData->aTSKEY[writer->bData->nRow - 1] == key->ts         //
  ) {
    code = tBlockDataUpdateRow(writer->bData, &row->row, writer->config->skmRow->pTSchema);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    if (writer->bData->nRow >= writer->config->maxRow) {
      code = tsdbSttFileDoWriteTSDataBlock(writer);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tBlockDataAppendRow(writer->bData, &row->row, writer->config->skmRow->pTSchema, row->uid);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  TARRAY2_LAST(writer->sData->minVer) = TMIN(TARRAY2_LAST(writer->sData->minVer), key->version);
  TARRAY2_LAST(writer->sData->maxVer) = TMAX(TARRAY2_LAST(writer->sData->maxVer), key->version);
  if (key->ts > TARRAY2_LAST(writer->sData->lastKey)) {
    TARRAY2_LAST(writer->sData->lastKey) = key->ts;
    TARRAY2_LAST(writer->sData->lastKeyVer) = key->version;
    TARRAY2_LAST(writer->sData->count)++;
  } else if (key->ts == TARRAY2_LAST(writer->sData->lastKey)) {
    TARRAY2_LAST(writer->sData->lastKeyVer) = key->version;
  } else {
    ASSERTS(0, "timestamp should be in ascending order");
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(vid, lino, code);
  }
  return code;
}

int32_t tsdbSttFileWriteTSDataBlock(SSttFileWriter *writer, SBlockData *bdata) {
  int32_t code = 0;
  int32_t lino = 0;

  SRowInfo row[1];
  row->suid = bdata->suid;
  for (int32_t i = 0; i < bdata->nRow; i++) {
    row->uid = bdata->uid ? bdata->uid : bdata->aUid[i];
    row->row = tsdbRowFromBlockData(bdata, i);

    code = tsdbSttFileWriteTSData(writer, row);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbSttFileWriteTombRecord(SSttFileWriter *writer, const STombRecord *record) {
  int32_t code;
  int32_t lino;

  if (!writer->ctx->opened) {
    code = tsdbSttFWriterDoOpen(writer);
    return code;
  }

  // end time-series data write
  if (writer->bData->nRow > 0) {
    code = tsdbSttFileDoWriteTSDataBlock(writer);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (STATIS_BLOCK_SIZE(writer->sData) > 0) {
    code = tsdbSttFileDoWriteStatisBlock(writer);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // write SDelRecord
  code = tTombBlockPut(writer->dData, record);
  TSDB_CHECK_CODE(code, lino, _exit);

  // write SDelBlock if need
  if (TOMB_BLOCK_SIZE(writer->dData) >= writer->config->maxRow) {
    code = tsdbSttFileDoWriteTombBlock(writer);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}