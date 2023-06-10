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

  segReader[0] = taosMemoryCalloc(1, sizeof(*segReader[0]));
  if (!segReader[0]) return TSDB_CODE_OUT_OF_MEMORY;

  segReader[0]->reader = reader;
  code = tsdbReadFile(reader->fd, offset, (uint8_t *)(segReader[0]->footer), sizeof(SSttFooter));
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(reader->config->tsdb->pVnode), lino, code);
    taosMemoryFree(segReader[0]);
    segReader[0] = NULL;
  }
  return code;
}

static int32_t tsdbSttSegReaderClose(SSttSegReader **reader) {
  if (reader[0]) {
    TARRAY2_DESTROY(reader[0]->tombBlkArray, NULL);
    TARRAY2_DESTROY(reader[0]->statisBlkArray, NULL);
    TARRAY2_DESTROY(reader[0]->sttBlkArray, NULL);
    taosMemoryFree(reader[0]);
    reader[0] = NULL;
  }
  return 0;
}

int32_t tsdbSttFileReaderOpen(const char *fname, const SSttFileReaderConfig *config, SSttFileReader **reader) {
  int32_t code = 0;
  int32_t lino = 0;

  reader[0] = taosMemoryCalloc(1, sizeof(*reader[0]));
  if (reader[0] == NULL) return TSDB_CODE_OUT_OF_MEMORY;

  reader[0]->config[0] = config[0];
  if (reader[0]->config->bufArr == NULL) {
    reader[0]->config->bufArr = reader[0]->bufArr;
  }

  // open file
  if (fname) {
    code = tsdbOpenFile(fname, config->szPage, TD_FILE_READ, &reader[0]->fd);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    char fname1[TSDB_FILENAME_LEN];
    tsdbTFileName(config->tsdb, config->file, fname1);
    code = tsdbOpenFile(fname1, config->szPage, TD_FILE_READ, &reader[0]->fd);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

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
    TSDB_ERROR_LOG(TD_VID(config->tsdb->pVnode), lino, code);
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
    TARRAY2_DESTROY(reader[0]->readerArray, tsdbSttSegReaderClose);
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

int32_t tsdbSttFileReadBlockData(SSttSegReader *reader, const SSttBlk *sttBlk, SBlockData *bData) {
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

int32_t tsdbSttFileReadBlockDataByColumn(SSttSegReader *reader, const SSttBlk *sttBlk, SBlockData *bData,
                                         STSchema *pTSchema, int16_t cids[], int32_t ncid) {
  int32_t code = 0;
  int32_t lino = 0;

  TABLEID tbid = {.suid = sttBlk->suid, .uid = 0};
  code = tBlockDataInit(bData, &tbid, pTSchema, cids, ncid);
  TSDB_CHECK_CODE(code, lino, _exit);

  // uid + version + tskey
  code = tRealloc(&reader->reader->config->bufArr[0], sttBlk->bInfo.szKey);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbReadFile(reader->reader->fd, sttBlk->bInfo.offset, reader->reader->config->bufArr[0], sttBlk->bInfo.szKey);
  TSDB_CHECK_CODE(code, lino, _exit);

  // hdr
  SDiskDataHdr hdr[1];
  int32_t      size = 0;

  size += tGetDiskDataHdr(reader->reader->config->bufArr[0] + size, hdr);

  ASSERT(hdr->delimiter == TSDB_FILE_DLMT);

  bData->nRow = hdr->nRow;
  bData->uid = hdr->uid;

  // uid
  if (hdr->uid == 0) {
    ASSERT(hdr->szUid);
    code = tsdbDecmprData(reader->reader->config->bufArr[0] + size, hdr->szUid, TSDB_DATA_TYPE_BIGINT, hdr->cmprAlg,
                          (uint8_t **)&bData->aUid, sizeof(int64_t) * hdr->nRow, &reader->reader->config->bufArr[1]);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    ASSERT(hdr->szUid == 0);
  }
  size += hdr->szUid;

  // version
  code = tsdbDecmprData(reader->reader->config->bufArr[0] + size, hdr->szVer, TSDB_DATA_TYPE_BIGINT, hdr->cmprAlg,
                        (uint8_t **)&bData->aVersion, sizeof(int64_t) * hdr->nRow, &reader->reader->config->bufArr[1]);
  TSDB_CHECK_CODE(code, lino, _exit);
  size += hdr->szVer;

  // ts
  code = tsdbDecmprData(reader->reader->config->bufArr[0] + size, hdr->szKey, TSDB_DATA_TYPE_TIMESTAMP, hdr->cmprAlg,
                        (uint8_t **)&bData->aTSKEY, sizeof(TSKEY) * hdr->nRow, &reader->reader->config->bufArr[1]);
  TSDB_CHECK_CODE(code, lino, _exit);
  size += hdr->szKey;

  ASSERT(size == sttBlk->bInfo.szKey);

  // other columns
  if (bData->nColData > 0) {
    if (hdr->szBlkCol > 0) {
      code = tRealloc(&reader->reader->config->bufArr[0], hdr->szBlkCol);
      TSDB_CHECK_CODE(code, lino, _exit);

      code = tsdbReadFile(reader->reader->fd, sttBlk->bInfo.offset + sttBlk->bInfo.szKey,
                          reader->reader->config->bufArr[0], hdr->szBlkCol);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    SBlockCol  bc[1] = {{.cid = 0}};
    SBlockCol *blockCol = bc;

    size = 0;
    for (int32_t i = 0; i < bData->nColData; i++) {
      SColData *colData = tBlockDataGetColDataByIdx(bData, i);

      while (blockCol && blockCol->cid < colData->cid) {
        if (size < hdr->szBlkCol) {
          size += tGetBlockCol(reader->reader->config->bufArr[0] + size, blockCol);
        } else {
          ASSERT(size == hdr->szBlkCol);
          blockCol = NULL;
        }
      }

      if (blockCol == NULL || blockCol->cid > colData->cid) {
        for (int32_t iRow = 0; iRow < hdr->nRow; iRow++) {
          code = tColDataAppendValue(colData, &COL_VAL_NONE(colData->cid, colData->type));
          TSDB_CHECK_CODE(code, lino, _exit);
        }
      } else {
        ASSERT(blockCol->type == colData->type);
        ASSERT(blockCol->flag && blockCol->flag != HAS_NONE);

        if (blockCol->flag == HAS_NULL) {
          for (int32_t iRow = 0; iRow < hdr->nRow; iRow++) {
            code = tColDataAppendValue(colData, &COL_VAL_NULL(blockCol->cid, blockCol->type));
            TSDB_CHECK_CODE(code, lino, _exit);
          }
        } else {
          int32_t size1 = blockCol->szBitmap + blockCol->szOffset + blockCol->szValue;

          code = tRealloc(&reader->reader->config->bufArr[1], size1);
          TSDB_CHECK_CODE(code, lino, _exit);

          code = tsdbReadFile(reader->reader->fd,
                              sttBlk->bInfo.offset + sttBlk->bInfo.szKey + hdr->szBlkCol + blockCol->offset,
                              reader->reader->config->bufArr[1], size1);
          TSDB_CHECK_CODE(code, lino, _exit);

          code = tsdbDecmprColData(reader->reader->config->bufArr[1], blockCol, hdr->cmprAlg, hdr->nRow, colData,
                                   &reader->reader->config->bufArr[2]);
          TSDB_CHECK_CODE(code, lino, _exit);
        }
      }
    }
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(reader->reader->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbSttFileReadTombBlock(SSttSegReader *reader, const STombBlk *tombBlk, STombBlock *tombBlock) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tRealloc(&reader->reader->config->bufArr[0], tombBlk->dp->size);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbReadFile(reader->reader->fd, tombBlk->dp->offset, reader->reader->config->bufArr[0], tombBlk->dp->size);
  if (code) TSDB_CHECK_CODE(code, lino, _exit);

  int64_t size = 0;
  tTombBlockClear(tombBlock);
  for (int32_t i = 0; i < ARRAY_SIZE(tombBlock->dataArr); ++i) {
    code = tsdbDecmprData(reader->reader->config->bufArr[0] + size, tombBlk->size[i], TSDB_DATA_TYPE_BIGINT,
                          tombBlk->cmprAlg, &reader->reader->config->bufArr[1], sizeof(int64_t) * tombBlk->numRec,
                          &reader->reader->config->bufArr[2]);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = TARRAY2_APPEND_BATCH(&tombBlock->dataArr[i], reader->reader->config->bufArr[1], tombBlk->numRec);
    TSDB_CHECK_CODE(code, lino, _exit);

    size += tombBlk->size[i];
  }

  ASSERT(size == tombBlk->dp->size);
_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(reader->reader->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbSttFileReadStatisBlock(SSttSegReader *reader, const SStatisBlk *statisBlk, STbStatisBlock *statisBlock) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tRealloc(&reader->reader->config->bufArr[0], statisBlk->dp->size);
  TSDB_CHECK_CODE(code, lino, _exit);

  code =
      tsdbReadFile(reader->reader->fd, statisBlk->dp->offset, reader->reader->config->bufArr[0], statisBlk->dp->size);
  TSDB_CHECK_CODE(code, lino, _exit);

  int64_t size = 0;
  tStatisBlockClear(statisBlock);
  for (int32_t i = 0; i < ARRAY_SIZE(statisBlock->dataArr); ++i) {
    code = tsdbDecmprData(reader->reader->config->bufArr[0] + size, statisBlk->size[i], TSDB_DATA_TYPE_BIGINT,
                          statisBlk->cmprAlg, &reader->reader->config->bufArr[1], sizeof(int64_t) * statisBlk->numRec,
                          &reader->reader->config->bufArr[2]);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = TARRAY2_APPEND_BATCH(statisBlock->dataArr + i, reader->reader->config->bufArr[1], statisBlk->numRec);
    TSDB_CHECK_CODE(code, lino, _exit);

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
  STsdbFD *fd;
  STFile   file[1];
  // data
  SSttFooter      footer[1];
  TTombBlkArray   tombBlkArray[1];
  TSttBlkArray    sttBlkArray[1];
  TStatisBlkArray statisBlkArray[1];
  STombBlock      tombBlock[1];
  STbStatisBlock  staticBlock[1];
  SBlockData      blockData[1];
  // helper data
  SSkmInfo skmTb[1];
  SSkmInfo skmRow[1];
  uint8_t *bufArr[5];
};

static int32_t tsdbSttFileDoWriteBlockData(SSttFileWriter *writer) {
  if (writer->blockData->nRow == 0) return 0;

  int32_t code = 0;
  int32_t lino = 0;

  SSttBlk sttBlk[1] = {{
      .suid = writer->blockData->suid,
      .minUid = writer->blockData->uid ? writer->blockData->uid : writer->blockData->aUid[0],
      .maxUid = writer->blockData->uid ? writer->blockData->uid : writer->blockData->aUid[writer->blockData->nRow - 1],
      .minKey = writer->blockData->aTSKEY[0],
      .maxKey = writer->blockData->aTSKEY[0],
      .minVer = writer->blockData->aVersion[0],
      .maxVer = writer->blockData->aVersion[0],
      .nRow = writer->blockData->nRow,
  }};

  for (int32_t iRow = 1; iRow < writer->blockData->nRow; iRow++) {
    if (sttBlk->minKey > writer->blockData->aTSKEY[iRow]) sttBlk->minKey = writer->blockData->aTSKEY[iRow];
    if (sttBlk->maxKey < writer->blockData->aTSKEY[iRow]) sttBlk->maxKey = writer->blockData->aTSKEY[iRow];
    if (sttBlk->minVer > writer->blockData->aVersion[iRow]) sttBlk->minVer = writer->blockData->aVersion[iRow];
    if (sttBlk->maxVer < writer->blockData->aVersion[iRow]) sttBlk->maxVer = writer->blockData->aVersion[iRow];
  }

  int32_t sizeArr[5] = {0};
  code = tCmprBlockData(writer->blockData, writer->config->cmprAlg, NULL, NULL, writer->config->bufArr, sizeArr);
  TSDB_CHECK_CODE(code, lino, _exit);

  sttBlk->bInfo.offset = writer->file->size;
  sttBlk->bInfo.szKey = sizeArr[2] + sizeArr[3];
  sttBlk->bInfo.szBlock = sizeArr[0] + sizeArr[1] + sttBlk->bInfo.szKey;

  for (int32_t i = 3; i >= 0; i--) {
    if (sizeArr[i]) {
      code = tsdbWriteFile(writer->fd, writer->file->size, writer->config->bufArr[i], sizeArr[i]);
      TSDB_CHECK_CODE(code, lino, _exit);
      writer->file->size += sizeArr[i];
    }
  }

  code = TARRAY2_APPEND_PTR(writer->sttBlkArray, sttBlk);
  TSDB_CHECK_CODE(code, lino, _exit);

  tBlockDataClear(writer->blockData);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbSttFileDoWriteStatisBlock(SSttFileWriter *writer) {
  if (STATIS_BLOCK_SIZE(writer->staticBlock) == 0) return 0;

  int32_t code = 0;
  int32_t lino = 0;

  SStatisBlk statisBlk[1] = {{
      .dp[0] =
          {
              .offset = writer->file->size,
              .size = 0,
          },
      .minTbid =
          {
              .suid = TARRAY2_FIRST(writer->staticBlock->suid),
              .uid = TARRAY2_FIRST(writer->staticBlock->uid),
          },
      .maxTbid =
          {
              .suid = TARRAY2_LAST(writer->staticBlock->suid),
              .uid = TARRAY2_LAST(writer->staticBlock->uid),
          },
      .minVer = TARRAY2_FIRST(writer->staticBlock->minVer),
      .maxVer = TARRAY2_FIRST(writer->staticBlock->maxVer),
      .numRec = STATIS_BLOCK_SIZE(writer->staticBlock),
      .cmprAlg = writer->config->cmprAlg,
  }};

  for (int32_t i = 1; i < STATIS_BLOCK_SIZE(writer->staticBlock); i++) {
    if (statisBlk->minVer > TARRAY2_GET(writer->staticBlock->minVer, i)) {
      statisBlk->minVer = TARRAY2_GET(writer->staticBlock->minVer, i);
    }
    if (statisBlk->maxVer < TARRAY2_GET(writer->staticBlock->maxVer, i)) {
      statisBlk->maxVer = TARRAY2_GET(writer->staticBlock->maxVer, i);
    }
  }

  for (int32_t i = 0; i < STATIS_RECORD_NUM_ELEM; i++) {
    code = tsdbCmprData((uint8_t *)TARRAY2_DATA(writer->staticBlock->dataArr + i),
                        TARRAY2_DATA_LEN(&writer->staticBlock->dataArr[i]), TSDB_DATA_TYPE_BIGINT, statisBlk->cmprAlg,
                        &writer->config->bufArr[0], 0, &statisBlk->size[i], &writer->config->bufArr[1]);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbWriteFile(writer->fd, writer->file->size, writer->config->bufArr[0], statisBlk->size[i]);
    TSDB_CHECK_CODE(code, lino, _exit);

    statisBlk->dp->size += statisBlk->size[i];
    writer->file->size += statisBlk->size[i];
  }

  code = TARRAY2_APPEND_PTR(writer->statisBlkArray, statisBlk);
  TSDB_CHECK_CODE(code, lino, _exit);

  tStatisBlockClear(writer->staticBlock);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbSttFileDoWriteTombBlock(SSttFileWriter *writer) {
  if (TOMB_BLOCK_SIZE(writer->tombBlock) == 0) return 0;

  int32_t code = 0;
  int32_t lino = 0;

  STombBlk tombBlk[1] = {{
      .dp[0] =
          {
              .offset = writer->file->size,
              .size = 0,
          },
      .minTbid =
          {
              .suid = TARRAY2_FIRST(writer->tombBlock->suid),
              .uid = TARRAY2_FIRST(writer->tombBlock->uid),
          },
      .maxTbid =
          {
              .suid = TARRAY2_LAST(writer->tombBlock->suid),
              .uid = TARRAY2_LAST(writer->tombBlock->uid),
          },
      .minVer = TARRAY2_FIRST(writer->tombBlock->version),
      .maxVer = TARRAY2_FIRST(writer->tombBlock->version),
      .numRec = TOMB_BLOCK_SIZE(writer->tombBlock),
      .cmprAlg = writer->config->cmprAlg,
  }};

  for (int32_t i = 1; i < TOMB_BLOCK_SIZE(writer->tombBlock); i++) {
    if (tombBlk->minVer > TARRAY2_GET(writer->tombBlock->version, i)) {
      tombBlk->minVer = TARRAY2_GET(writer->tombBlock->version, i);
    }
    if (tombBlk->maxVer < TARRAY2_GET(writer->tombBlock->version, i)) {
      tombBlk->maxVer = TARRAY2_GET(writer->tombBlock->version, i);
    }
  }

  for (int32_t i = 0; i < ARRAY_SIZE(writer->tombBlock->dataArr); i++) {
    code = tsdbCmprData((uint8_t *)TARRAY2_DATA(&writer->tombBlock->dataArr[i]),
                        TARRAY2_DATA_LEN(&writer->tombBlock->dataArr[i]), TSDB_DATA_TYPE_BIGINT, tombBlk->cmprAlg,
                        &writer->config->bufArr[0], 0, &tombBlk->size[i], &writer->config->bufArr[1]);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbWriteFile(writer->fd, writer->file->size, writer->config->bufArr[0], tombBlk->size[i]);
    TSDB_CHECK_CODE(code, lino, _exit);

    tombBlk->dp->size += tombBlk->size[i];
    writer->file->size += tombBlk->size[i];
  }

  code = TARRAY2_APPEND_PTR(writer->tombBlkArray, tombBlk);
  TSDB_CHECK_CODE(code, lino, _exit);

  tTombBlockClear(writer->tombBlock);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbSttFileDoWriteSttBlk(SSttFileWriter *writer) {
  int32_t code = 0;
  int32_t lino;

  writer->footer->sttBlkPtr->size = TARRAY2_DATA_LEN(writer->sttBlkArray);
  if (writer->footer->sttBlkPtr->size) {
    writer->footer->sttBlkPtr->offset = writer->file->size;
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

  writer->footer->statisBlkPtr->size = TARRAY2_DATA_LEN(writer->statisBlkArray);
  if (writer->footer->statisBlkPtr->size) {
    writer->footer->statisBlkPtr->offset = writer->file->size;
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

  writer->footer->tombBlkPtr->size = TARRAY2_DATA_LEN(writer->tombBlkArray);
  if (writer->footer->tombBlkPtr->size) {
    writer->footer->tombBlkPtr->offset = writer->file->size;
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
  if (code) return code;
  writer->file->size += sizeof(writer->footer);
  return 0;
}

static int32_t tsdbSttFWriterDoOpen(SSttFileWriter *writer) {
  int32_t code = 0;
  int32_t lino = 0;

  // set
  writer->file[0] = writer->config->file;
  writer->file->stt->nseg++;
  if (!writer->config->skmTb) writer->config->skmTb = writer->skmTb;
  if (!writer->config->skmRow) writer->config->skmRow = writer->skmRow;
  if (!writer->config->bufArr) writer->config->bufArr = writer->bufArr;

  // open file
  int32_t flag;
  char    fname[TSDB_FILENAME_LEN];

  if (writer->file->size > 0) {
    flag = TD_FILE_READ | TD_FILE_WRITE;
  } else {
    flag = TD_FILE_READ | TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC;
  }

  tsdbTFileName(writer->config->tsdb, writer->file, fname);
  code = tsdbOpenFile(fname, writer->config->szPage, flag, &writer->fd);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (writer->file->size == 0) {
    uint8_t hdr[TSDB_FHDR_SIZE] = {0};
    code = tsdbWriteFile(writer->fd, 0, hdr, sizeof(hdr));
    TSDB_CHECK_CODE(code, lino, _exit);
    writer->file->size += sizeof(hdr);
  }

  writer->ctx->opened = true;

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

static void tsdbSttFWriterDoClose(SSttFileWriter *writer) {
  ASSERT(writer->fd == NULL);

  for (int32_t i = 0; i < ARRAY_SIZE(writer->bufArr); ++i) {
    tFree(writer->bufArr[i]);
  }
  tDestroyTSchema(writer->skmRow->pTSchema);
  tDestroyTSchema(writer->skmTb->pTSchema);
  tTombBlockDestroy(writer->tombBlock);
  tStatisBlockDestroy(writer->staticBlock);
  tBlockDataDestroy(writer->blockData);
  TARRAY2_DESTROY(writer->tombBlkArray, NULL);
  TARRAY2_DESTROY(writer->statisBlkArray, NULL);
  TARRAY2_DESTROY(writer->sttBlkArray, NULL);
}

static int32_t tsdbSttFileDoUpdateHeader(SSttFileWriter *writer) {
  // TODO
  return 0;
}

static int32_t tsdbSttFWriterCloseCommit(SSttFileWriter *writer, TFileOpArray *opArray) {
  int32_t lino;
  int32_t code;

  code = tsdbSttFileDoWriteBlockData(writer);
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
  STFileOp op;
  if (writer->config->file.size == 0) {
    op = (STFileOp){
        .optype = TSDB_FOP_CREATE,
        .fid = writer->config->file.fid,
        .nf = writer->file[0],
    };
  } else {
    op = (STFileOp){
        .optype = TSDB_FOP_MODIFY,
        .fid = writer->config->file.fid,
        .of = writer->config->file,
        .nf = writer->file[0],
    };
  }

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
    TSDB_ERROR_LOG(TD_VID(writer[0]->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbSttFileWriteRow(SSttFileWriter *writer, SRowInfo *row) {
  int32_t code = 0;
  int32_t lino = 0;

  if (!writer->ctx->opened) {
    code = tsdbSttFWriterDoOpen(writer);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (!TABLE_SAME_SCHEMA(row->suid, row->uid, writer->ctx->tbid->suid, writer->ctx->tbid->uid)) {
    code = tsdbSttFileDoWriteBlockData(writer);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbUpdateSkmTb(writer->config->tsdb, (TABLEID *)row, writer->config->skmTb);
    TSDB_CHECK_CODE(code, lino, _exit);

    TABLEID id = {.suid = row->suid, .uid = row->suid ? 0 : row->uid};
    code = tBlockDataInit(writer->blockData, &id, writer->config->skmTb->pTSchema, NULL, 0);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  TSDBKEY key[1];
  if (row->row.type == TSDBROW_ROW_FMT) {
    key->ts = row->row.pTSRow->ts;
    key->version = row->row.version;
  } else {
    key->ts = row->row.pBlockData->aTSKEY[row->row.iRow];
    key->version = row->row.pBlockData->aVersion[row->row.iRow];
  }

  if (writer->ctx->tbid->uid != row->uid) {
    writer->ctx->tbid->suid = row->suid;
    writer->ctx->tbid->uid = row->uid;

    if (STATIS_BLOCK_SIZE(writer->staticBlock) >= writer->config->maxRow) {
      code = tsdbSttFileDoWriteStatisBlock(writer);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    STbStatisRecord record = {
        .suid = row->suid,
        .uid = row->uid,
        .firstKey = key->ts,
        .lastKey = key->ts,
        .minVer = key->version,
        .maxVer = key->version,
        .count = 1,
    };
    code = tStatisBlockPut(writer->staticBlock, &record);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    ASSERT(key->ts >= TARRAY2_LAST(writer->staticBlock->lastKey));

    if (TARRAY2_LAST(writer->staticBlock->minVer) > key->version) {
      TARRAY2_LAST(writer->staticBlock->minVer) = key->version;
    }
    if (TARRAY2_LAST(writer->staticBlock->maxVer) < key->version) {
      TARRAY2_LAST(writer->staticBlock->maxVer) = key->version;
    }
    if (key->ts > TARRAY2_LAST(writer->staticBlock->lastKey)) {
      TARRAY2_LAST(writer->staticBlock->count)++;
      TARRAY2_LAST(writer->staticBlock->lastKey) = key->ts;
    }
  }

  if (row->row.type == TSDBROW_ROW_FMT) {
    code = tsdbUpdateSkmRow(writer->config->tsdb, writer->ctx->tbid,  //
                            TSDBROW_SVERSION(&row->row), writer->config->skmRow);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // row to col conversion
  if (key->version <= writer->config->compactVersion                               //
      && writer->blockData->nRow > 0                                               //
      && writer->blockData->aTSKEY[writer->blockData->nRow - 1] == key->ts         //
      && (writer->blockData->uid                                                   //
              ? writer->blockData->uid                                             //
              : writer->blockData->aUid[writer->blockData->nRow - 1]) == row->uid  //
  ) {
    code = tBlockDataUpdateRow(writer->blockData, &row->row, writer->config->skmRow->pTSchema);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    if (writer->blockData->nRow >= writer->config->maxRow) {
      code = tsdbSttFileDoWriteBlockData(writer);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tBlockDataAppendRow(writer->blockData, &row->row, writer->config->skmRow->pTSchema, row->uid);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbSttFileWriteBlockData(SSttFileWriter *writer, SBlockData *bdata) {
  int32_t code = 0;
  int32_t lino = 0;

  SRowInfo row[1];
  row->suid = bdata->suid;
  for (int32_t i = 0; i < bdata->nRow; i++) {
    row->uid = bdata->uid ? bdata->uid : bdata->aUid[i];
    row->row = tsdbRowFromBlockData(bdata, i);

    code = tsdbSttFileWriteRow(writer, row);
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
  } else {
    if (writer->blockData->nRow > 0) {
      code = tsdbSttFileDoWriteBlockData(writer);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    if (STATIS_BLOCK_SIZE(writer->staticBlock) > 0) {
      code = tsdbSttFileDoWriteStatisBlock(writer);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  code = tTombBlockPut(writer->tombBlock, record);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (TOMB_BLOCK_SIZE(writer->tombBlock) >= writer->config->maxRow) {
    code = tsdbSttFileDoWriteTombBlock(writer);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

bool tsdbSttFileWriterIsOpened(SSttFileWriter *writer) { return writer->ctx->opened; }