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

#include "tsdbSttFileRW.h"
#include "tsdbDataFileRW.h"

// SSttFReader ============================================================
struct SSttFileReader {
  SSttFileReaderConfig config[1];
  STsdbFD             *fd;
  SSttFooter           footer[1];
  struct {
    bool sttBlkLoaded;
    bool statisBlkLoaded;
    bool tombBlkLoaded;
  } ctx[1];
  TSttBlkArray    sttBlkArray[1];
  TStatisBlkArray statisBlkArray[1];
  TTombBlkArray   tombBlkArray[1];
  uint8_t        *bufArr[5];
};

// SSttFileReader
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
    code = tsdbOpenFile(fname, config->tsdb, TD_FILE_READ, &reader[0]->fd);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    char fname1[TSDB_FILENAME_LEN];
    tsdbTFileName(config->tsdb, config->file, fname1);
    code = tsdbOpenFile(fname1, config->tsdb, TD_FILE_READ, &reader[0]->fd);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // // open each segment reader
  int64_t offset = config->file->size - sizeof(SSttFooter);
  ASSERT(offset >= TSDB_FHDR_SIZE);

  code = tsdbReadFile(reader[0]->fd, offset, (uint8_t *)(reader[0]->footer), sizeof(SSttFooter), 0);
  TSDB_CHECK_CODE(code, lino, _exit);

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
    TARRAY2_DESTROY(reader[0]->tombBlkArray, NULL);
    TARRAY2_DESTROY(reader[0]->statisBlkArray, NULL);
    TARRAY2_DESTROY(reader[0]->sttBlkArray, NULL);
    taosMemoryFree(reader[0]);
    reader[0] = NULL;
  }
  return 0;
}

// SSttFSegReader
int32_t tsdbSttFileReadStatisBlk(SSttFileReader *reader, const TStatisBlkArray **statisBlkArray) {
  if (!reader->ctx->statisBlkLoaded) {
    if (reader->footer->statisBlkPtr->size > 0) {
      ASSERT(reader->footer->statisBlkPtr->size % sizeof(SStatisBlk) == 0);

      int32_t size = reader->footer->statisBlkPtr->size / sizeof(SStatisBlk);
      void   *data = taosMemoryMalloc(reader->footer->statisBlkPtr->size);
      if (!data) return TSDB_CODE_OUT_OF_MEMORY;

      int32_t code =
          tsdbReadFile(reader->fd, reader->footer->statisBlkPtr->offset, data, reader->footer->statisBlkPtr->size, 0);
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

int32_t tsdbSttFileReadTombBlk(SSttFileReader *reader, const TTombBlkArray **tombBlkArray) {
  if (!reader->ctx->tombBlkLoaded) {
    if (reader->footer->tombBlkPtr->size > 0) {
      ASSERT(reader->footer->tombBlkPtr->size % sizeof(STombBlk) == 0);

      int32_t size = reader->footer->tombBlkPtr->size / sizeof(STombBlk);
      void   *data = taosMemoryMalloc(reader->footer->tombBlkPtr->size);
      if (!data) return TSDB_CODE_OUT_OF_MEMORY;

      int32_t code =
          tsdbReadFile(reader->fd, reader->footer->tombBlkPtr->offset, data, reader->footer->tombBlkPtr->size, 0);
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

int32_t tsdbSttFileReadSttBlk(SSttFileReader *reader, const TSttBlkArray **sttBlkArray) {
  if (!reader->ctx->sttBlkLoaded) {
    if (reader->footer->sttBlkPtr->size > 0) {
      ASSERT(reader->footer->sttBlkPtr->size % sizeof(SSttBlk) == 0);

      int32_t size = reader->footer->sttBlkPtr->size / sizeof(SSttBlk);
      void   *data = taosMemoryMalloc(reader->footer->sttBlkPtr->size);
      if (!data) return TSDB_CODE_OUT_OF_MEMORY;

      int32_t code =
          tsdbReadFile(reader->fd, reader->footer->sttBlkPtr->offset, data, reader->footer->sttBlkPtr->size, 0);
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

int32_t tsdbSttFileReadBlockData(SSttFileReader *reader, const SSttBlk *sttBlk, SBlockData *bData) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tRealloc(&reader->config->bufArr[0], sttBlk->bInfo.szBlock);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbReadFile(reader->fd, sttBlk->bInfo.offset, reader->config->bufArr[0], sttBlk->bInfo.szBlock, 0);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tDecmprBlockData(reader->config->bufArr[0], sttBlk->bInfo.szBlock, bData, &reader->config->bufArr[1]);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(reader->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbSttFileReadBlockDataByColumn(SSttFileReader *reader, const SSttBlk *sttBlk, SBlockData *bData,
                                         STSchema *pTSchema, int16_t cids[], int32_t ncid) {
  int32_t code = 0;
  int32_t lino = 0;

  TABLEID tbid = {.suid = sttBlk->suid};
  if (tbid.suid == 0) {
    tbid.uid = sttBlk->minUid;
  } else {
    tbid.uid = 0;
  }

  code = tBlockDataInit(bData, &tbid, pTSchema, cids, ncid);
  TSDB_CHECK_CODE(code, lino, _exit);

  // uid + version + tskey
  code = tRealloc(&reader->config->bufArr[0], sttBlk->bInfo.szKey);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbReadFile(reader->fd, sttBlk->bInfo.offset, reader->config->bufArr[0], sttBlk->bInfo.szKey, 0);
  TSDB_CHECK_CODE(code, lino, _exit);

  // hdr
  SDiskDataHdr hdr[1];
  int32_t      size = 0;

  size += tGetDiskDataHdr(reader->config->bufArr[0] + size, hdr);

  ASSERT(hdr->delimiter == TSDB_FILE_DLMT);

  bData->nRow = hdr->nRow;
  bData->uid = hdr->uid;

  // uid
  if (hdr->uid == 0) {
    ASSERT(hdr->szUid);
    code = tsdbDecmprData(reader->config->bufArr[0] + size, hdr->szUid, TSDB_DATA_TYPE_BIGINT, hdr->cmprAlg,
                          (uint8_t **)&bData->aUid, sizeof(int64_t) * hdr->nRow, &reader->config->bufArr[1]);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    ASSERT(hdr->szUid == 0);
  }
  size += hdr->szUid;

  // version
  code = tsdbDecmprData(reader->config->bufArr[0] + size, hdr->szVer, TSDB_DATA_TYPE_BIGINT, hdr->cmprAlg,
                        (uint8_t **)&bData->aVersion, sizeof(int64_t) * hdr->nRow, &reader->config->bufArr[1]);
  TSDB_CHECK_CODE(code, lino, _exit);
  size += hdr->szVer;

  // ts
  code = tsdbDecmprData(reader->config->bufArr[0] + size, hdr->szKey, TSDB_DATA_TYPE_TIMESTAMP, hdr->cmprAlg,
                        (uint8_t **)&bData->aTSKEY, sizeof(TSKEY) * hdr->nRow, &reader->config->bufArr[1]);
  TSDB_CHECK_CODE(code, lino, _exit);
  size += hdr->szKey;

  ASSERT(size == sttBlk->bInfo.szKey);

  // other columns
  if (bData->nColData > 0) {
    if (hdr->szBlkCol > 0) {
      code = tRealloc(&reader->config->bufArr[0], hdr->szBlkCol);
      TSDB_CHECK_CODE(code, lino, _exit);

      code = tsdbReadFile(reader->fd, sttBlk->bInfo.offset + sttBlk->bInfo.szKey, reader->config->bufArr[0],
                          hdr->szBlkCol, 0);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    SBlockCol  bc[1] = {{.cid = 0}};
    SBlockCol *blockCol = bc;

    size = 0;
    for (int32_t i = 0; i < bData->nColData; i++) {
      SColData *colData = tBlockDataGetColDataByIdx(bData, i);

      while (blockCol && blockCol->cid < colData->cid) {
        if (size < hdr->szBlkCol) {
          size += tGetBlockCol(reader->config->bufArr[0] + size, blockCol);
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

          code = tRealloc(&reader->config->bufArr[1], size1);
          TSDB_CHECK_CODE(code, lino, _exit);

          code = tsdbReadFile(reader->fd, sttBlk->bInfo.offset + sttBlk->bInfo.szKey + hdr->szBlkCol + blockCol->offset,
                              reader->config->bufArr[1], size1, 0);
          TSDB_CHECK_CODE(code, lino, _exit);

          code = tsdbDecmprColData(reader->config->bufArr[1], blockCol, hdr->cmprAlg, hdr->nRow, colData,
                                   &reader->config->bufArr[2]);
          TSDB_CHECK_CODE(code, lino, _exit);
        }
      }
    }
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(reader->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbSttFileReadTombBlock(SSttFileReader *reader, const STombBlk *tombBlk, STombBlock *tombBlock) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tRealloc(&reader->config->bufArr[0], tombBlk->dp->size);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbReadFile(reader->fd, tombBlk->dp->offset, reader->config->bufArr[0], tombBlk->dp->size, 0);
  if (code) TSDB_CHECK_CODE(code, lino, _exit);

  int64_t size = 0;
  tTombBlockClear(tombBlock);
  for (int32_t i = 0; i < ARRAY_SIZE(tombBlock->dataArr); ++i) {
    code = tsdbDecmprData(reader->config->bufArr[0] + size, tombBlk->size[i], TSDB_DATA_TYPE_BIGINT, tombBlk->cmprAlg,
                          &reader->config->bufArr[1], sizeof(int64_t) * tombBlk->numRec, &reader->config->bufArr[2]);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = TARRAY2_APPEND_BATCH(&tombBlock->dataArr[i], reader->config->bufArr[1], tombBlk->numRec);
    TSDB_CHECK_CODE(code, lino, _exit);

    size += tombBlk->size[i];
  }

  ASSERT(size == tombBlk->dp->size);
_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(reader->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbSttFileReadStatisBlock(SSttFileReader *reader, const SStatisBlk *statisBlk, STbStatisBlock *statisBlock) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tRealloc(&reader->config->bufArr[0], statisBlk->dp->size);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbReadFile(reader->fd, statisBlk->dp->offset, reader->config->bufArr[0], statisBlk->dp->size, 0);
  TSDB_CHECK_CODE(code, lino, _exit);

  int64_t size = 0;
  tStatisBlockClear(statisBlock);
  for (int32_t i = 0; i < ARRAY_SIZE(statisBlock->dataArr); ++i) {
    code =
        tsdbDecmprData(reader->config->bufArr[0] + size, statisBlk->size[i], TSDB_DATA_TYPE_BIGINT, statisBlk->cmprAlg,
                       &reader->config->bufArr[1], sizeof(int64_t) * statisBlk->numRec, &reader->config->bufArr[2]);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = TARRAY2_APPEND_BATCH(statisBlock->dataArr + i, reader->config->bufArr[1], statisBlk->numRec);
    TSDB_CHECK_CODE(code, lino, _exit);

    size += statisBlk->size[i];
  }

  ASSERT(size == statisBlk->dp->size);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(reader->config->tsdb->pVnode), lino, code);
  }
  return code;
}

// SSttFWriter ============================================================
struct SSttFileWriter {
  SSttFileWriterConfig config[1];
  struct {
    bool    opened;
    TABLEID tbid[1];
    // range
    SVersionRange range;
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

static int32_t tsdbFileDoWriteSttBlockData(STsdbFD *fd, SBlockData *blockData, int8_t cmprAlg, int64_t *fileSize,
                                           TSttBlkArray *sttBlkArray, uint8_t **bufArr, SVersionRange *range) {
  if (blockData->nRow == 0) return 0;

  int32_t code = 0;

  SSttBlk sttBlk[1] = {{
      .suid = blockData->suid,
      .minUid = blockData->uid ? blockData->uid : blockData->aUid[0],
      .maxUid = blockData->uid ? blockData->uid : blockData->aUid[blockData->nRow - 1],
      .minKey = blockData->aTSKEY[0],
      .maxKey = blockData->aTSKEY[0],
      .minVer = blockData->aVersion[0],
      .maxVer = blockData->aVersion[0],
      .nRow = blockData->nRow,
  }};

  for (int32_t iRow = 1; iRow < blockData->nRow; iRow++) {
    if (sttBlk->minKey > blockData->aTSKEY[iRow]) sttBlk->minKey = blockData->aTSKEY[iRow];
    if (sttBlk->maxKey < blockData->aTSKEY[iRow]) sttBlk->maxKey = blockData->aTSKEY[iRow];
    if (sttBlk->minVer > blockData->aVersion[iRow]) sttBlk->minVer = blockData->aVersion[iRow];
    if (sttBlk->maxVer < blockData->aVersion[iRow]) sttBlk->maxVer = blockData->aVersion[iRow];
  }

  tsdbWriterUpdVerRange(range, sttBlk->minVer, sttBlk->maxVer);

  int32_t sizeArr[5] = {0};
  code = tCmprBlockData(blockData, cmprAlg, NULL, NULL, bufArr, sizeArr);
  if (code) return code;

  sttBlk->bInfo.offset = *fileSize;
  sttBlk->bInfo.szKey = sizeArr[2] + sizeArr[3];
  sttBlk->bInfo.szBlock = sizeArr[0] + sizeArr[1] + sttBlk->bInfo.szKey;

  for (int32_t i = 3; i >= 0; i--) {
    if (sizeArr[i]) {
      code = tsdbWriteFile(fd, *fileSize, bufArr[i], sizeArr[i]);
      if (code) return code;
      *fileSize += sizeArr[i];
    }
  }

  code = TARRAY2_APPEND_PTR(sttBlkArray, sttBlk);
  if (code) return code;

  tBlockDataClear(blockData);

  return 0;
}

static int32_t tsdbSttFileDoWriteBlockData(SSttFileWriter *writer) {
  if (writer->blockData->nRow == 0) return 0;

  int32_t code = 0;
  int32_t lino = 0;

  code = tsdbFileDoWriteSttBlockData(writer->fd, writer->blockData, writer->config->cmprAlg, &writer->file->size,
                                     writer->sttBlkArray, writer->config->bufArr, &writer->ctx->range);
  TSDB_CHECK_CODE(code, lino, _exit);

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
      .numRec = STATIS_BLOCK_SIZE(writer->staticBlock),
      .cmprAlg = writer->config->cmprAlg,
  }};

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

  code = tsdbFileWriteTombBlock(writer->fd, writer->tombBlock, writer->config->cmprAlg, &writer->file->size,
                                writer->tombBlkArray, writer->config->bufArr, &writer->ctx->range);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbFileWriteSttBlk(STsdbFD *fd, const TSttBlkArray *sttBlkArray, SFDataPtr *ptr, int64_t *fileSize) {
  ptr->size = TARRAY2_DATA_LEN(sttBlkArray);
  if (ptr->size > 0) {
    ptr->offset = *fileSize;

    int32_t code = tsdbWriteFile(fd, *fileSize, (const uint8_t *)TARRAY2_DATA(sttBlkArray), ptr->size);
    if (code) {
      return code;
    }

    *fileSize += ptr->size;
  }
  return 0;
}

static int32_t tsdbSttFileDoWriteSttBlk(SSttFileWriter *writer) {
  int32_t code = 0;
  int32_t lino;

  code = tsdbFileWriteSttBlk(writer->fd, writer->sttBlkArray, writer->footer->sttBlkPtr, &writer->file->size);
  TSDB_CHECK_CODE(code, lino, _exit);

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

  code = tsdbFileWriteTombBlk(writer->fd, writer->tombBlkArray, writer->footer->tombBlkPtr, &writer->file->size);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbFileWriteSttFooter(STsdbFD *fd, const SSttFooter *footer, int64_t *fileSize) {
  int32_t code = tsdbWriteFile(fd, *fileSize, (const uint8_t *)footer, sizeof(*footer));
  if (code) return code;
  *fileSize += sizeof(*footer);
  return 0;
}

static int32_t tsdbSttFileDoWriteFooter(SSttFileWriter *writer) {
  return tsdbFileWriteSttFooter(writer->fd, writer->footer, &writer->file->size);
}

static int32_t tsdbSttFWriterDoOpen(SSttFileWriter *writer) {
  int32_t code = 0;
  int32_t lino = 0;

  // set
  if (!writer->config->skmTb) writer->config->skmTb = writer->skmTb;
  if (!writer->config->skmRow) writer->config->skmRow = writer->skmRow;
  if (!writer->config->bufArr) writer->config->bufArr = writer->bufArr;

  writer->file[0] = (STFile){
      .type = TSDB_FTYPE_STT,
      .did = writer->config->did,
      .fid = writer->config->fid,
      .cid = writer->config->cid,
      .size = 0,
      .minVer = VERSION_MAX,
      .maxVer = VERSION_MIN,
      .stt[0] =
          {
              .level = writer->config->level,
          },
  };

  // open file
  int32_t flag = TD_FILE_READ | TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC;
  char    fname[TSDB_FILENAME_LEN];

  tsdbTFileName(writer->config->tsdb, writer->file, fname);
  code = tsdbOpenFile(fname, writer->config->tsdb, flag, &writer->fd);
  TSDB_CHECK_CODE(code, lino, _exit);

  uint8_t hdr[TSDB_FHDR_SIZE] = {0};
  code = tsdbWriteFile(writer->fd, 0, hdr, sizeof(hdr));
  TSDB_CHECK_CODE(code, lino, _exit);
  writer->file->size += sizeof(hdr);

  // range
  writer->ctx->range = (SVersionRange){.minVer = VERSION_MAX, .maxVer = VERSION_MIN};

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

  ASSERT(writer->file->size > 0);
  STFileOp op = (STFileOp){
      .optype = TSDB_FOP_CREATE,
      .fid = writer->config->fid,
      .nf = writer->file[0],
  };
  tsdbTFileUpdVerRange(&op.nf, writer->ctx->range);

  code = TARRAY2_APPEND(opArray, op);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbSttFWriterCloseAbort(SSttFileWriter *writer) {
  char fname[TSDB_FILENAME_LEN];
  tsdbTFileName(writer->config->tsdb, writer->file, fname);
  tsdbCloseFile(&writer->fd);
  taosRemoveFile(fname);
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
        .count = 1,
    };
    code = tStatisBlockPut(writer->staticBlock, &record);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    ASSERT(key->ts >= TARRAY2_LAST(writer->staticBlock->lastKey));

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
    TSDB_CHECK_CODE(code, lino, _exit);
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
  } else {
    tsdbTrace("vgId:%d write tomb record to stt file:%s, cid:%" PRId64 ", suid:%" PRId64 ", uid:%" PRId64
              ", version:%" PRId64,
              TD_VID(writer->config->tsdb->pVnode), writer->fd->path, writer->config->cid, record->suid, record->uid,
              record->version);
  }
  return code;
}

bool tsdbSttFileWriterIsOpened(SSttFileWriter *writer) { return writer->ctx->opened; }
