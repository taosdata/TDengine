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

#include "tsdbDataFileRW.h"

// SDataFileReader =============================================
struct SDataFileReader {
  SDataFileReaderConfig config[1];

  uint8_t *bufArr[5];

  struct {
    bool headFooterLoaded;
    bool tombFooterLoaded;
    bool brinBlkLoaded;
    bool tombBlkLoaded;
  } ctx[1];

  STsdbFD *fd[TSDB_FTYPE_MAX];

  SHeadFooter   headFooter[1];
  STombFooter   tombFooter[1];
  TBrinBlkArray brinBlkArray[1];
  TTombBlkArray tombBlkArray[1];
};

static int32_t tsdbDataFileReadHeadFooter(SDataFileReader *reader) {
  if (reader->ctx->headFooterLoaded) return 0;

  int32_t code = 0;
  int32_t lino = 0;

  int32_t ftype = TSDB_FTYPE_HEAD;
  if (reader->fd[ftype]) {
    code = tsdbReadFile(reader->fd[ftype], reader->config->files[ftype].file.size - sizeof(SHeadFooter),
                        (uint8_t *)reader->headFooter, sizeof(SHeadFooter), 0);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  reader->ctx->headFooterLoaded = true;

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(reader->config->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbDataFileReadTombFooter(SDataFileReader *reader) {
  if (reader->ctx->tombFooterLoaded) return 0;

  int32_t code = 0;
  int32_t lino = 0;

  int32_t ftype = TSDB_FTYPE_TOMB;
  if (reader->fd[ftype]) {
    code = tsdbReadFile(reader->fd[ftype], reader->config->files[ftype].file.size - sizeof(STombFooter),
                        (uint8_t *)reader->tombFooter, sizeof(STombFooter), 0);
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  reader->ctx->tombFooterLoaded = true;

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
  if (reader[0] == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  reader[0]->config[0] = config[0];
  if (reader[0]->config->bufArr == NULL) {
    reader[0]->config->bufArr = reader[0]->bufArr;
  }

  if (fname) {
    for (int32_t i = 0; i < TSDB_FTYPE_MAX; ++i) {
      if (fname[i]) {
        code = tsdbOpenFile(fname[i], config->tsdb, TD_FILE_READ, &reader[0]->fd[i]);
        TSDB_CHECK_CODE(code, lino, _exit);
      }
    }
  } else {
    for (int32_t i = 0; i < TSDB_FTYPE_MAX; ++i) {
      if (config->files[i].exist) {
        char fname1[TSDB_FILENAME_LEN];
        tsdbTFileName(config->tsdb, &config->files[i].file, fname1);
        code = tsdbOpenFile(fname1, config->tsdb, TD_FILE_READ, &reader[0]->fd[i]);
        TSDB_CHECK_CODE(code, lino, _exit);
      }
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

  TARRAY2_DESTROY(reader[0]->tombBlkArray, NULL);
  TARRAY2_DESTROY(reader[0]->brinBlkArray, NULL);

#if 0
  TARRAY2_DESTROY(reader[0]->dataBlkArray, NULL);
  TARRAY2_DESTROY(reader[0]->blockIdxArray, NULL);
#endif

  for (int32_t i = 0; i < TSDB_FTYPE_MAX; ++i) {
    if (reader[0]->fd[i]) {
      tsdbCloseFile(&reader[0]->fd[i]);
    }
  }

  for (int32_t i = 0; i < ARRAY_SIZE(reader[0]->bufArr); ++i) {
    tFree(reader[0]->bufArr[i]);
  }

  taosMemoryFree(reader[0]);
  reader[0] = NULL;
  return 0;
}

int32_t tsdbDataFileReadBrinBlk(SDataFileReader *reader, const TBrinBlkArray **brinBlkArray) {
  int32_t code = 0;
  int32_t lino = 0;

  if (!reader->ctx->brinBlkLoaded) {
    code = tsdbDataFileReadHeadFooter(reader);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (reader->headFooter->brinBlkPtr->size > 0) {
      void *data = taosMemoryMalloc(reader->headFooter->brinBlkPtr->size);
      if (data == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        TSDB_CHECK_CODE(code, lino, _exit);
      }

      code = tsdbReadFile(reader->fd[TSDB_FTYPE_HEAD], reader->headFooter->brinBlkPtr->offset, data,
                          reader->headFooter->brinBlkPtr->size, 0);
      if (code) {
        taosMemoryFree(data);
        TSDB_CHECK_CODE(code, lino, _exit);
      }

      int32_t size = reader->headFooter->brinBlkPtr->size / sizeof(SBrinBlk);
      TARRAY2_INIT_EX(reader->brinBlkArray, size, size, data);
    } else {
      TARRAY2_INIT(reader->brinBlkArray);
    }

    reader->ctx->brinBlkLoaded = true;
  }
  brinBlkArray[0] = reader->brinBlkArray;

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(reader->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbDataFileReadBrinBlock(SDataFileReader *reader, const SBrinBlk *brinBlk, SBrinBlock *brinBlock) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tRealloc(&reader->config->bufArr[0], brinBlk->dp->size);
  TSDB_CHECK_CODE(code, lino, _exit);

  code =
      tsdbReadFile(reader->fd[TSDB_FTYPE_HEAD], brinBlk->dp->offset, reader->config->bufArr[0], brinBlk->dp->size, 0);
  TSDB_CHECK_CODE(code, lino, _exit);

  int32_t size = 0;
  tBrinBlockClear(brinBlock);
  for (int32_t i = 0; i < ARRAY_SIZE(brinBlock->dataArr1); i++) {
    code = tsdbDecmprData(reader->config->bufArr[0] + size, brinBlk->size[i], TSDB_DATA_TYPE_BIGINT, brinBlk->cmprAlg,
                          &reader->config->bufArr[1], brinBlk->numRec * sizeof(int64_t), &reader->config->bufArr[2]);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = TARRAY2_APPEND_BATCH(&brinBlock->dataArr1[i], reader->config->bufArr[1], brinBlk->numRec);
    TSDB_CHECK_CODE(code, lino, _exit);

    size += brinBlk->size[i];
  }

  for (int32_t i = 0, j = ARRAY_SIZE(brinBlock->dataArr1); i < ARRAY_SIZE(brinBlock->dataArr2); i++, j++) {
    code = tsdbDecmprData(reader->config->bufArr[0] + size, brinBlk->size[j], TSDB_DATA_TYPE_INT, brinBlk->cmprAlg,
                          &reader->config->bufArr[1], brinBlk->numRec * sizeof(int32_t), &reader->config->bufArr[2]);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = TARRAY2_APPEND_BATCH(&brinBlock->dataArr2[i], reader->config->bufArr[1], brinBlk->numRec);
    TSDB_CHECK_CODE(code, lino, _exit);

    size += brinBlk->size[j];
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(reader->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbDataFileReadBlockData(SDataFileReader *reader, const SBrinRecord *record, SBlockData *bData) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tRealloc(&reader->config->bufArr[0], record->blockSize);
  TSDB_CHECK_CODE(code, lino, _exit);

  code =
      tsdbReadFile(reader->fd[TSDB_FTYPE_DATA], record->blockOffset, reader->config->bufArr[0], record->blockSize, 0);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tDecmprBlockData(reader->config->bufArr[0], record->blockSize, bData, &reader->config->bufArr[1]);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(reader->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbDataFileReadBlockDataByColumn(SDataFileReader *reader, const SBrinRecord *record, SBlockData *bData,
                                          STSchema *pTSchema, int16_t cids[], int32_t ncid) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tBlockDataInit(bData, (TABLEID *)record, pTSchema, cids, ncid);
  TSDB_CHECK_CODE(code, lino, _exit);

  // uid + version + tskey
  code = tRealloc(&reader->config->bufArr[0], record->blockKeySize);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbReadFile(reader->fd[TSDB_FTYPE_DATA], record->blockOffset, reader->config->bufArr[0], record->blockKeySize,
                      0);
  TSDB_CHECK_CODE(code, lino, _exit);

  // hdr
  SDiskDataHdr hdr[1];
  int32_t      size = 0;

  size += tGetDiskDataHdr(reader->config->bufArr[0] + size, hdr);

  ASSERT(hdr->delimiter == TSDB_FILE_DLMT);
  ASSERT(record->uid == hdr->uid);

  bData->nRow = hdr->nRow;

  // uid
  ASSERT(hdr->uid);

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

  ASSERT(size == record->blockKeySize);

  // other columns
  if (bData->nColData > 0) {
    if (hdr->szBlkCol > 0) {
      code = tRealloc(&reader->config->bufArr[0], hdr->szBlkCol);
      TSDB_CHECK_CODE(code, lino, _exit);

      code = tsdbReadFile(reader->fd[TSDB_FTYPE_DATA], record->blockOffset + record->blockKeySize,
                          reader->config->bufArr[0], hdr->szBlkCol, 0);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    int64_t szHint = 0;
    if (bData->nColData > 3) {
      int64_t    offset = 0;
      SBlockCol  bc = {.cid = 0};
      SBlockCol *blockCol = &bc;

      size = 0;
      SColData *colData = tBlockDataGetColDataByIdx(bData, 0);
      while (blockCol && blockCol->cid < colData->cid) {
        if (size < hdr->szBlkCol) {
          size += tGetBlockCol(reader->config->bufArr[0] + size, blockCol);
        } else {
          ASSERT(size == hdr->szBlkCol);
          blockCol = NULL;
        }
      }

      if (blockCol && blockCol->flag == HAS_VALUE) {
        offset = blockCol->offset;

        SColData *colDataEnd = tBlockDataGetColDataByIdx(bData, bData->nColData - 1);
        while (blockCol && blockCol->cid < colDataEnd->cid) {
          if (size < hdr->szBlkCol) {
            size += tGetBlockCol(reader->config->bufArr[0] + size, blockCol);
          } else {
            ASSERT(size == hdr->szBlkCol);
            blockCol = NULL;
          }
        }

        if (blockCol && blockCol->flag == HAS_VALUE) {
          szHint = blockCol->offset + blockCol->szBitmap + blockCol->szOffset + blockCol->szValue - offset;
        }
      }
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

          code = tsdbReadFile(reader->fd[TSDB_FTYPE_DATA],
                              record->blockOffset + record->blockKeySize + hdr->szBlkCol + blockCol->offset,
                              reader->config->bufArr[1], size1, i > 0 ? 0 : szHint);
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

int32_t tsdbDataFileReadBlockSma(SDataFileReader *reader, const SBrinRecord *record,
                                 TColumnDataAggArray *columnDataAggArray) {
  int32_t code = 0;
  int32_t lino = 0;

  TARRAY2_CLEAR(columnDataAggArray, NULL);
  if (record->smaSize > 0) {
    code = tRealloc(&reader->config->bufArr[0], record->smaSize);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbReadFile(reader->fd[TSDB_FTYPE_SMA], record->smaOffset, reader->config->bufArr[0], record->smaSize, 0);
    TSDB_CHECK_CODE(code, lino, _exit);

    // decode sma data
    int32_t size = 0;
    while (size < record->smaSize) {
      SColumnDataAgg sma[1];

      size += tGetColumnDataAgg(reader->config->bufArr[0] + size, sma);

      code = TARRAY2_APPEND_PTR(columnDataAggArray, sma);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    ASSERT(size == record->smaSize);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(reader->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbDataFileReadTombBlk(SDataFileReader *reader, const TTombBlkArray **tombBlkArray) {
  int32_t code = 0;
  int32_t lino = 0;

  if (!reader->ctx->tombBlkLoaded) {
    code = tsdbDataFileReadTombFooter(reader);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (reader->tombFooter->tombBlkPtr->size > 0) {
      void *data = taosMemoryMalloc(reader->tombFooter->tombBlkPtr->size);
      if (data == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        TSDB_CHECK_CODE(code, lino, _exit);
      }

      code = tsdbReadFile(reader->fd[TSDB_FTYPE_TOMB], reader->tombFooter->tombBlkPtr->offset, data,
                          reader->tombFooter->tombBlkPtr->size, 0);
      if (code) {
        taosMemoryFree(data);
        TSDB_CHECK_CODE(code, lino, _exit);
      }

      int32_t size = reader->tombFooter->tombBlkPtr->size / sizeof(STombBlk);
      TARRAY2_INIT_EX(reader->tombBlkArray, size, size, data);
    } else {
      TARRAY2_INIT(reader->tombBlkArray);
    }

    reader->ctx->tombBlkLoaded = true;
  }
  tombBlkArray[0] = reader->tombBlkArray;

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(reader->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbDataFileReadTombBlock(SDataFileReader *reader, const STombBlk *tombBlk, STombBlock *tData) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tRealloc(&reader->config->bufArr[0], tombBlk->dp->size);
  TSDB_CHECK_CODE(code, lino, _exit);

  code =
      tsdbReadFile(reader->fd[TSDB_FTYPE_TOMB], tombBlk->dp->offset, reader->config->bufArr[0], tombBlk->dp->size, 0);
  TSDB_CHECK_CODE(code, lino, _exit);

  int32_t size = 0;
  tTombBlockClear(tData);
  for (int32_t i = 0; i < ARRAY_SIZE(tData->dataArr); ++i) {
    code = tsdbDecmprData(reader->config->bufArr[0] + size, tombBlk->size[i], TSDB_DATA_TYPE_BIGINT, tombBlk->cmprAlg,
                          &reader->config->bufArr[1], sizeof(int64_t) * tombBlk->numRec, &reader->config->bufArr[2]);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = TARRAY2_APPEND_BATCH(&tData->dataArr[i], reader->config->bufArr[1], tombBlk->numRec);
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

// SDataFileWriter =============================================
struct SDataFileWriter {
  SDataFileWriterConfig config[1];

  SSkmInfo skmTb[1];
  SSkmInfo skmRow[1];
  uint8_t *bufArr[5];

  struct {
    bool             opened;
    SDataFileReader *reader;

    // for ts data
    TABLEID tbid[1];
    bool    tbHasOldData;

    const TBrinBlkArray *brinBlkArray;
    int32_t              brinBlkArrayIdx;
    SBrinBlock           brinBlock[1];
    int32_t              brinBlockIdx;
    SBlockData           blockData[1];
    int32_t              blockDataIdx;
    // for tomb data
    bool                 hasOldTomb;
    const TTombBlkArray *tombBlkArray;
    int32_t              tombBlkArrayIdx;
    STombBlock           tombBlock[1];
    int32_t              tombBlockIdx;
    // range
    SVersionRange range;
    SVersionRange tombRange;
  } ctx[1];

  STFile   files[TSDB_FTYPE_MAX];
  STsdbFD *fd[TSDB_FTYPE_MAX];

  SHeadFooter headFooter[1];
  STombFooter tombFooter[1];

  TBrinBlkArray brinBlkArray[1];
  SBrinBlock    brinBlock[1];
  SBlockData    blockData[1];

  TTombBlkArray tombBlkArray[1];
  STombBlock    tombBlock[1];
};

static int32_t tsdbDataFileWriterCloseAbort(SDataFileWriter *writer) {
  ASSERT(0);
  return 0;
}

static int32_t tsdbDataFileWriterDoClose(SDataFileWriter *writer) {
  if (writer->ctx->reader) {
    tsdbDataFileReaderClose(&writer->ctx->reader);
  }

  tTombBlockDestroy(writer->tombBlock);
  TARRAY2_DESTROY(writer->tombBlkArray, NULL);
  tBlockDataDestroy(writer->blockData);
  tBrinBlockDestroy(writer->brinBlock);
  TARRAY2_DESTROY(writer->brinBlkArray, NULL);

  tTombBlockDestroy(writer->ctx->tombBlock);
  tBlockDataDestroy(writer->ctx->blockData);
  tBrinBlockDestroy(writer->ctx->brinBlock);

  for (int32_t i = 0; i < ARRAY_SIZE(writer->bufArr); ++i) {
    tFree(writer->bufArr[i]);
  }

  tDestroyTSchema(writer->skmRow->pTSchema);
  tDestroyTSchema(writer->skmTb->pTSchema);
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
          .bufArr = writer->config->bufArr,
      }};

      for (int32_t i = 0; i < TSDB_FTYPE_MAX; ++i) {
        config->files[i].exist = writer->config->files[i].exist;
        if (config->files[i].exist) {
          config->files[i].file = writer->config->files[i].file;
        }
      }

      code = tsdbDataFileReaderOpen(NULL, config, &writer->ctx->reader);
      TSDB_CHECK_CODE(code, lino, _exit);
      break;
    }
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbDataFileWriterDoOpen(SDataFileWriter *writer) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t ftype;

  if (!writer->config->skmTb) writer->config->skmTb = writer->skmTb;
  if (!writer->config->skmRow) writer->config->skmRow = writer->skmRow;
  if (!writer->config->bufArr) writer->config->bufArr = writer->bufArr;

  // open reader
  code = tsdbDataFileWriterDoOpenReader(writer);
  TSDB_CHECK_CODE(code, lino, _exit);

  // .head
  ftype = TSDB_FTYPE_HEAD;
  writer->files[ftype] = (STFile){
      .type = ftype,
      .did = writer->config->did,
      .fid = writer->config->fid,
      .cid = writer->config->cid,
      .size = 0,
      .minVer = VERSION_MAX,
      .maxVer = VERSION_MIN,
  };

  // .data
  ftype = TSDB_FTYPE_DATA;
  if (writer->config->files[ftype].exist) {
    writer->files[ftype] = writer->config->files[ftype].file;
  } else {
    writer->files[ftype] = (STFile){
        .type = ftype,
        .did = writer->config->did,
        .fid = writer->config->fid,
        .cid = writer->config->cid,
        .size = 0,
        .minVer = VERSION_MAX,
        .maxVer = VERSION_MIN,
    };
  }

  // .sma
  ftype = TSDB_FTYPE_SMA;
  if (writer->config->files[ftype].exist) {
    writer->files[ftype] = writer->config->files[ftype].file;
  } else {
    writer->files[ftype] = (STFile){
        .type = ftype,
        .did = writer->config->did,
        .fid = writer->config->fid,
        .cid = writer->config->cid,
        .size = 0,
        .minVer = VERSION_MAX,
        .maxVer = VERSION_MIN,
    };
  }

  // .tomb
  ftype = TSDB_FTYPE_TOMB;
  writer->files[ftype] = (STFile){
      .type = ftype,
      .did = writer->config->did,
      .fid = writer->config->fid,
      .cid = writer->config->cid,
      .size = 0,
      .minVer = VERSION_MAX,
      .maxVer = VERSION_MIN,
  };

  // range
  writer->ctx->range = (SVersionRange){.minVer = VERSION_MAX, .maxVer = VERSION_MIN};
  writer->ctx->tombRange = (SVersionRange){.minVer = VERSION_MAX, .maxVer = VERSION_MIN};

  writer->ctx->opened = true;

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbWriterUpdVerRange(SVersionRange *range, int64_t minVer, int64_t maxVer) {
  range->minVer = TMIN(range->minVer, minVer);
  range->maxVer = TMAX(range->maxVer, maxVer);
  return 0;
}

int32_t tsdbFileWriteBrinBlock(STsdbFD *fd, SBrinBlock *brinBlock, int8_t cmprAlg, int64_t *fileSize,
                               TBrinBlkArray *brinBlkArray, uint8_t **bufArr, SVersionRange *range) {
  if (BRIN_BLOCK_SIZE(brinBlock) == 0) return 0;

  int32_t code;

  // get SBrinBlk
  SBrinBlk brinBlk[1] = {
      {
          .dp[0] =
              {
                  .offset = *fileSize,
                  .size = 0,
              },
          .minTbid =
              {
                  .suid = TARRAY2_FIRST(brinBlock->suid),
                  .uid = TARRAY2_FIRST(brinBlock->uid),
              },
          .maxTbid =
              {
                  .suid = TARRAY2_LAST(brinBlock->suid),
                  .uid = TARRAY2_LAST(brinBlock->uid),
              },
          .minVer = TARRAY2_FIRST(brinBlock->minVer),
          .maxVer = TARRAY2_FIRST(brinBlock->minVer),
          .numRec = BRIN_BLOCK_SIZE(brinBlock),
          .cmprAlg = cmprAlg,
      },
  };

  for (int32_t i = 1; i < BRIN_BLOCK_SIZE(brinBlock); i++) {
    if (brinBlk->minVer > TARRAY2_GET(brinBlock->minVer, i)) {
      brinBlk->minVer = TARRAY2_GET(brinBlock->minVer, i);
    }
    if (brinBlk->maxVer < TARRAY2_GET(brinBlock->maxVer, i)) {
      brinBlk->maxVer = TARRAY2_GET(brinBlock->maxVer, i);
    }
  }

  tsdbWriterUpdVerRange(range, brinBlk->minVer, brinBlk->maxVer);

  // write to file
  for (int32_t i = 0; i < ARRAY_SIZE(brinBlock->dataArr1); i++) {
    code = tsdbCmprData((uint8_t *)TARRAY2_DATA(brinBlock->dataArr1 + i), TARRAY2_DATA_LEN(brinBlock->dataArr1 + i),
                        TSDB_DATA_TYPE_BIGINT, brinBlk->cmprAlg, &bufArr[0], 0, &brinBlk->size[i], &bufArr[1]);
    if (code) return code;

    code = tsdbWriteFile(fd, *fileSize, bufArr[0], brinBlk->size[i]);
    if (code) return code;

    brinBlk->dp->size += brinBlk->size[i];
    *fileSize += brinBlk->size[i];
  }

  for (int32_t i = 0, j = ARRAY_SIZE(brinBlock->dataArr1); i < ARRAY_SIZE(brinBlock->dataArr2); i++, j++) {
    code = tsdbCmprData((uint8_t *)TARRAY2_DATA(brinBlock->dataArr2 + i), TARRAY2_DATA_LEN(brinBlock->dataArr2 + i),
                        TSDB_DATA_TYPE_INT, brinBlk->cmprAlg, &bufArr[0], 0, &brinBlk->size[j], &bufArr[1]);
    if (code) return code;

    code = tsdbWriteFile(fd, *fileSize, bufArr[0], brinBlk->size[j]);
    if (code) return code;

    brinBlk->dp->size += brinBlk->size[j];
    *fileSize += brinBlk->size[j];
  }

#if 0
  SBrinRecord record;
  for (int32_t i = 0; i < BRIN_BLOCK_SIZE(brinBlock); i++) {
    tBrinBlockGet(brinBlock, i, &record);
    tsdbInfo("write brin block, block num:%04d, idx:%04d suid:%ld, uid:%ld, offset:%ld, numRow:%d, count:%d",
             TARRAY2_SIZE(brinBlkArray), i, record.suid, record.uid, record.blockOffset, record.numRow, record.count);
  }
#endif

  // append to brinBlkArray
  code = TARRAY2_APPEND_PTR(brinBlkArray, brinBlk);
  if (code) return code;

  tBrinBlockClear(brinBlock);

  return 0;
}

static int32_t tsdbDataFileWriteBrinBlock(SDataFileWriter *writer) {
  if (BRIN_BLOCK_SIZE(writer->brinBlock) == 0) return 0;

  int32_t code = 0;
  int32_t lino = 0;

  code = tsdbFileWriteBrinBlock(writer->fd[TSDB_FTYPE_HEAD], writer->brinBlock, writer->config->cmprAlg,
                                &writer->files[TSDB_FTYPE_HEAD].size, writer->brinBlkArray, writer->config->bufArr,
                                &writer->ctx->range);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbDataFileWriteBrinRecord(SDataFileWriter *writer, const SBrinRecord *record) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tBrinBlockPut(writer->brinBlock, record);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (BRIN_BLOCK_SIZE(writer->brinBlock) >= writer->config->maxRow) {
    code = tsdbDataFileWriteBrinBlock(writer);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbDataFileDoWriteBlockData(SDataFileWriter *writer, SBlockData *bData) {
  if (bData->nRow == 0) return 0;

  ASSERT(bData->uid);

  int32_t code = 0;
  int32_t lino = 0;

  SBrinRecord record[1] = {{
      .suid = bData->suid,
      .uid = bData->uid,
      .firstKey = bData->aTSKEY[0],
      .firstKeyVer = bData->aVersion[0],
      .lastKey = bData->aTSKEY[bData->nRow - 1],
      .lastKeyVer = bData->aVersion[bData->nRow - 1],
      .minVer = bData->aVersion[0],
      .maxVer = bData->aVersion[0],
      .blockOffset = writer->files[TSDB_FTYPE_DATA].size,
      .smaOffset = writer->files[TSDB_FTYPE_SMA].size,
      .blockSize = 0,
      .blockKeySize = 0,
      .smaSize = 0,
      .numRow = bData->nRow,
      .count = 1,
  }};

  for (int32_t i = 1; i < bData->nRow; ++i) {
    if (bData->aTSKEY[i] != bData->aTSKEY[i - 1]) {
      record->count++;
    }
    if (bData->aVersion[i] < record->minVer) {
      record->minVer = bData->aVersion[i];
    }
    if (bData->aVersion[i] > record->maxVer) {
      record->maxVer = bData->aVersion[i];
    }
  }

  tsdbWriterUpdVerRange(&writer->ctx->range, record->minVer, record->maxVer);

  // to .data file
  int32_t sizeArr[5] = {0};

  code = tCmprBlockData(bData, writer->config->cmprAlg, NULL, NULL, writer->config->bufArr, sizeArr);
  TSDB_CHECK_CODE(code, lino, _exit);

  record->blockKeySize = sizeArr[3] + sizeArr[2];
  record->blockSize = sizeArr[0] + sizeArr[1] + record->blockKeySize;

  for (int32_t i = 3; i >= 0; --i) {
    if (sizeArr[i]) {
      code = tsdbWriteFile(writer->fd[TSDB_FTYPE_DATA], writer->files[TSDB_FTYPE_DATA].size, writer->config->bufArr[i],
                           sizeArr[i]);
      TSDB_CHECK_CODE(code, lino, _exit);
      writer->files[TSDB_FTYPE_DATA].size += sizeArr[i];
    }
  }

  // to .sma file
  for (int32_t i = 0; i < bData->nColData; ++i) {
    SColData *colData = bData->aColData + i;
    if ((!colData->smaOn) || ((colData->flag & HAS_VALUE) == 0)) continue;

    SColumnDataAgg sma[1] = {{.colId = colData->cid}};
    tColDataCalcSMA[colData->type](colData, &sma->sum, &sma->max, &sma->min, &sma->numOfNull);

    int32_t size = tPutColumnDataAgg(NULL, sma);

    code = tRealloc(&writer->config->bufArr[0], record->smaSize + size);
    TSDB_CHECK_CODE(code, lino, _exit);

    tPutColumnDataAgg(writer->config->bufArr[0] + record->smaSize, sma);
    record->smaSize += size;
  }

  if (record->smaSize > 0) {
    code = tsdbWriteFile(writer->fd[TSDB_FTYPE_SMA], record->smaOffset, writer->config->bufArr[0], record->smaSize);
    TSDB_CHECK_CODE(code, lino, _exit);
    writer->files[TSDB_FTYPE_SMA].size += record->smaSize;
  }

  // append SBrinRecord
  code = tsdbDataFileWriteBrinRecord(writer, record);
  TSDB_CHECK_CODE(code, lino, _exit);

  tBlockDataClear(bData);

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

  int32_t   ftype = TSDB_FTYPE_HEAD;
  SBlockIdx blockIdx[1] = {{
      .suid = writer->ctx->tbid->suid,
      .uid = writer->ctx->tbid->uid,
      .offset = writer->files[ftype].size,
      .size = TARRAY2_DATA_LEN(dataBlkArray),
  }};

  code =
      tsdbWriteFile(writer->fd[ftype], blockIdx->offset, (const uint8_t *)TARRAY2_DATA(dataBlkArray), blockIdx->size);
  TSDB_CHECK_CODE(code, lino, _exit);
  writer->files[ftype].size += blockIdx->size;

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

  TSDBKEY key[1];
  if (row->type == TSDBROW_ROW_FMT) {
    key->ts = row->pTSRow->ts;
    key->version = row->version;
  } else {
    key->ts = row->pBlockData->aTSKEY[row->iRow];
    key->version = row->pBlockData->aVersion[row->iRow];
  }
  if (key->version <= writer->config->compactVersion                        //
      && writer->blockData->nRow > 0                                        //
      && writer->blockData->aTSKEY[writer->blockData->nRow - 1] == key->ts  //
  ) {
    code = tBlockDataUpdateRow(writer->blockData, row, writer->config->skmRow->pTSchema);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    if (writer->blockData->nRow >= writer->config->maxRow) {
      code = tsdbDataFileDoWriteBlockData(writer, writer->blockData);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    code = tBlockDataAppendRow(writer->blockData, row, writer->config->skmRow->pTSchema, writer->ctx->tbid->uid);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbDataFileDoWriteTableOldData(SDataFileWriter *writer, const TSDBKEY *key) {
  if (writer->ctx->tbHasOldData == false) return 0;

  int32_t code = 0;
  int32_t lino = 0;

  for (;;) {
    for (;;) {
      // SBlockData
      for (; writer->ctx->blockDataIdx < writer->ctx->blockData->nRow; writer->ctx->blockDataIdx++) {
        if (key->ts < writer->ctx->blockData->aTSKEY[writer->ctx->blockDataIdx]  //
            || (key->ts == writer->ctx->blockData->aTSKEY[writer->ctx->blockDataIdx] &&
                key->version < writer->ctx->blockData->aVersion[writer->ctx->blockDataIdx])) {
          goto _exit;
        } else {
          TSDBROW row = tsdbRowFromBlockData(writer->ctx->blockData, writer->ctx->blockDataIdx);
          code = tsdbDataFileDoWriteTSRow(writer, &row);
          TSDB_CHECK_CODE(code, lino, _exit);
        }
      }

      // SBrinBlock
      if (writer->ctx->brinBlockIdx >= BRIN_BLOCK_SIZE(writer->ctx->brinBlock)) {
        break;
      }

      for (; writer->ctx->brinBlockIdx < BRIN_BLOCK_SIZE(writer->ctx->brinBlock); writer->ctx->brinBlockIdx++) {
        if (TARRAY2_GET(writer->ctx->brinBlock->uid, writer->ctx->brinBlockIdx) != writer->ctx->tbid->uid) {
          writer->ctx->tbHasOldData = false;
          goto _exit;
        }

        if (key->ts < TARRAY2_GET(writer->ctx->brinBlock->firstKey, writer->ctx->brinBlockIdx)  //
            || (key->ts == TARRAY2_GET(writer->ctx->brinBlock->firstKey, writer->ctx->brinBlockIdx) &&
                key->version < TARRAY2_GET(writer->ctx->brinBlock->firstKeyVer, writer->ctx->brinBlockIdx))) {
          goto _exit;
        } else {
          SBrinRecord record[1];
          tBrinBlockGet(writer->ctx->brinBlock, writer->ctx->brinBlockIdx, record);
          if (key->ts > record->lastKey || (key->ts == record->lastKey && key->version > record->maxVer)) {
            if (writer->blockData->nRow > 0) {
              code = tsdbDataFileDoWriteBlockData(writer, writer->blockData);
              TSDB_CHECK_CODE(code, lino, _exit);
            }

            code = tsdbDataFileWriteBrinRecord(writer, record);
            TSDB_CHECK_CODE(code, lino, _exit);
          } else {
            code = tsdbDataFileReadBlockData(writer->ctx->reader, record, writer->ctx->blockData);
            TSDB_CHECK_CODE(code, lino, _exit);

            writer->ctx->blockDataIdx = 0;
            writer->ctx->brinBlockIdx++;
            break;
          }
        }
      }
    }

    // SBrinBlk
    if (writer->ctx->brinBlkArrayIdx >= TARRAY2_SIZE(writer->ctx->brinBlkArray)) {
      writer->ctx->brinBlkArray = NULL;
      writer->ctx->tbHasOldData = false;
      goto _exit;
    } else {
      const SBrinBlk *brinBlk = TARRAY2_GET_PTR(writer->ctx->brinBlkArray, writer->ctx->brinBlkArrayIdx);

      if (brinBlk->minTbid.uid != writer->ctx->tbid->uid) {
        writer->ctx->tbHasOldData = false;
        goto _exit;
      }

      code = tsdbDataFileReadBrinBlock(writer->ctx->reader, brinBlk, writer->ctx->brinBlock);
      TSDB_CHECK_CODE(code, lino, _exit);

      writer->ctx->brinBlockIdx = 0;
      writer->ctx->brinBlkArrayIdx++;
    }
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

  if (writer->ctx->tbHasOldData) {
    TSDBKEY key[1];
    if (row->type == TSDBROW_ROW_FMT) {
      key->ts = row->pTSRow->ts;
      key->version = row->version;
    } else {
      key->ts = row->pBlockData->aTSKEY[row->iRow];
      key->version = row->pBlockData->aVersion[row->iRow];
    }

    code = tsdbDataFileDoWriteTableOldData(writer, key);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tsdbDataFileDoWriteTSRow(writer, row);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbDataFileWriteTableDataEnd(SDataFileWriter *writer) {
  if (writer->ctx->tbid->uid == 0) return 0;

  int32_t code = 0;
  int32_t lino = 0;

  if (writer->ctx->tbHasOldData) {
    TSDBKEY key = {
        .ts = TSKEY_MAX,
        .version = VERSION_MAX,
    };

    code = tsdbDataFileDoWriteTableOldData(writer, &key);
    TSDB_CHECK_CODE(code, lino, _exit);

    ASSERT(writer->ctx->tbHasOldData == false);
  }

  code = tsdbDataFileDoWriteBlockData(writer, writer->blockData);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbDataFileWriteTableDataBegin(SDataFileWriter *writer, const TABLEID *tbid) {
  int32_t code = 0;
  int32_t lino = 0;

  ASSERT(writer->ctx->blockDataIdx == writer->ctx->blockData->nRow);
  ASSERT(writer->blockData->nRow == 0);

  SMetaInfo info;
  bool      drop = false;
  TABLEID   tbid1[1];
  writer->ctx->tbHasOldData = false;
  while (writer->ctx->brinBlkArray) {  // skip data of previous table
    for (; writer->ctx->brinBlockIdx < BRIN_BLOCK_SIZE(writer->ctx->brinBlock); writer->ctx->brinBlockIdx++) {
      TABLEID tbid2[1] = {{
          .suid = TARRAY2_GET(writer->ctx->brinBlock->suid, writer->ctx->brinBlockIdx),
          .uid = TARRAY2_GET(writer->ctx->brinBlock->uid, writer->ctx->brinBlockIdx),
      }};

      if (tbid2->uid == tbid->uid) {
        writer->ctx->tbHasOldData = true;
        goto _begin;
      } else if (tbid2->suid > tbid->suid || (tbid2->suid == tbid->suid && tbid2->uid > tbid->uid)) {
        goto _begin;
      } else {
        if (tbid2->uid != writer->ctx->tbid->uid) {
          if (drop && tbid1->uid == tbid2->uid) {
            continue;
          } else if (metaGetInfo(writer->config->tsdb->pVnode->pMeta, tbid2->uid, &info, NULL) != 0) {
            drop = true;
            *tbid1 = *tbid2;
            continue;
          } else {
            drop = false;
            writer->ctx->tbid[0] = *tbid2;
          }
        }

        SBrinRecord record[1];
        tBrinBlockGet(writer->ctx->brinBlock, writer->ctx->brinBlockIdx, record);

        code = tsdbDataFileWriteBrinRecord(writer, record);
        TSDB_CHECK_CODE(code, lino, _exit);
      }
    }

    if (writer->ctx->brinBlkArrayIdx >= TARRAY2_SIZE(writer->ctx->brinBlkArray)) {
      writer->ctx->brinBlkArray = NULL;
      break;
    } else {
      const SBrinBlk *brinBlk = TARRAY2_GET_PTR(writer->ctx->brinBlkArray, writer->ctx->brinBlkArrayIdx);

      code = tsdbDataFileReadBrinBlock(writer->ctx->reader, brinBlk, writer->ctx->brinBlock);
      TSDB_CHECK_CODE(code, lino, _exit);

      writer->ctx->brinBlockIdx = 0;
      writer->ctx->brinBlkArrayIdx++;
    }
  }

_begin:
  writer->ctx->tbid[0] = *tbid;

  if (tbid->uid == INT64_MAX) goto _exit;

  code = tsdbUpdateSkmTb(writer->config->tsdb, tbid, writer->config->skmTb);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tBlockDataInit(writer->blockData, writer->ctx->tbid, writer->config->skmTb->pTSchema, NULL, 0);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbFileWriteHeadFooter(STsdbFD *fd, int64_t *fileSize, const SHeadFooter *footer) {
  int32_t code = tsdbWriteFile(fd, *fileSize, (const uint8_t *)footer, sizeof(*footer));
  if (code) return code;
  *fileSize += sizeof(*footer);
  return 0;
}

int32_t tsdbFileWriteTombBlock(STsdbFD *fd, STombBlock *tombBlock, int8_t cmprAlg, int64_t *fileSize,
                               TTombBlkArray *tombBlkArray, uint8_t **bufArr, SVersionRange *range) {
  int32_t code;

  if (TOMB_BLOCK_SIZE(tombBlock) == 0) return 0;

  STombBlk tombBlk[1] = {{
      .dp[0] =
          {
              .offset = *fileSize,
              .size = 0,
          },
      .minTbid =
          {
              .suid = TARRAY2_FIRST(tombBlock->suid),
              .uid = TARRAY2_FIRST(tombBlock->uid),
          },
      .maxTbid =
          {
              .suid = TARRAY2_LAST(tombBlock->suid),
              .uid = TARRAY2_LAST(tombBlock->uid),
          },
      .minVer = TARRAY2_FIRST(tombBlock->version),
      .maxVer = TARRAY2_FIRST(tombBlock->version),
      .numRec = TOMB_BLOCK_SIZE(tombBlock),
      .cmprAlg = cmprAlg,
  }};

  for (int32_t i = 1; i < TOMB_BLOCK_SIZE(tombBlock); i++) {
    if (tombBlk->minVer > TARRAY2_GET(tombBlock->version, i)) {
      tombBlk->minVer = TARRAY2_GET(tombBlock->version, i);
    }
    if (tombBlk->maxVer < TARRAY2_GET(tombBlock->version, i)) {
      tombBlk->maxVer = TARRAY2_GET(tombBlock->version, i);
    }
  }

  tsdbWriterUpdVerRange(range, tombBlk->minVer, tombBlk->maxVer);

  for (int32_t i = 0; i < ARRAY_SIZE(tombBlock->dataArr); i++) {
    code = tsdbCmprData((uint8_t *)TARRAY2_DATA(&tombBlock->dataArr[i]), TARRAY2_DATA_LEN(&tombBlock->dataArr[i]),
                        TSDB_DATA_TYPE_BIGINT, tombBlk->cmprAlg, &bufArr[0], 0, &tombBlk->size[i], &bufArr[1]);
    if (code) return code;

    code = tsdbWriteFile(fd, *fileSize, bufArr[0], tombBlk->size[i]);
    if (code) return code;

    tombBlk->dp->size += tombBlk->size[i];
    *fileSize += tombBlk->size[i];
  }

  code = TARRAY2_APPEND_PTR(tombBlkArray, tombBlk);
  if (code) return code;

  tTombBlockClear(tombBlock);
  return 0;
}

static int32_t tsdbDataFileWriteHeadFooter(SDataFileWriter *writer) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tsdbFileWriteHeadFooter(writer->fd[TSDB_FTYPE_HEAD], &writer->files[TSDB_FTYPE_HEAD].size, writer->headFooter);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbDataFileDoWriteTombBlock(SDataFileWriter *writer) {
  if (TOMB_BLOCK_SIZE(writer->tombBlock) == 0) return 0;

  int32_t code = 0;
  int32_t lino = 0;

  code = tsdbFileWriteTombBlock(writer->fd[TSDB_FTYPE_TOMB], writer->tombBlock, writer->config->cmprAlg,
                                &writer->files[TSDB_FTYPE_TOMB].size, writer->tombBlkArray, writer->config->bufArr,
                                &writer->ctx->tombRange);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbFileWriteTombBlk(STsdbFD *fd, const TTombBlkArray *tombBlkArray, SFDataPtr *ptr, int64_t *fileSize) {
  ptr->size = TARRAY2_DATA_LEN(tombBlkArray);
  if (ptr->size > 0) {
    ptr->offset = *fileSize;

    int32_t code = tsdbWriteFile(fd, *fileSize, (const uint8_t *)TARRAY2_DATA(tombBlkArray), ptr->size);
    if (code) {
      return code;
    }

    *fileSize += ptr->size;
  }
  return 0;
}

static int32_t tsdbDataFileDoWriteTombBlk(SDataFileWriter *writer) {
  ASSERT(TARRAY2_SIZE(writer->tombBlkArray) > 0);

  int32_t code = 0;
  int32_t lino = 0;

  code = tsdbFileWriteTombBlk(writer->fd[TSDB_FTYPE_TOMB], writer->tombBlkArray, writer->tombFooter->tombBlkPtr,
                              &writer->files[TSDB_FTYPE_TOMB].size);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbFileWriteTombFooter(STsdbFD *fd, const STombFooter *footer, int64_t *fileSize) {
  int32_t code = tsdbWriteFile(fd, *fileSize, (const uint8_t *)footer, sizeof(*footer));
  if (code) return code;
  *fileSize += sizeof(*footer);
  return 0;
}

static int32_t tsdbDataFileWriteTombFooter(SDataFileWriter *writer) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tsdbFileWriteTombFooter(writer->fd[TSDB_FTYPE_TOMB], writer->tombFooter, &writer->files[TSDB_FTYPE_TOMB].size);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbDataFileDoWriteTombRecord(SDataFileWriter *writer, const STombRecord *record) {
  int32_t code = 0;
  int32_t lino = 0;

  while (writer->ctx->hasOldTomb) {
    for (; writer->ctx->tombBlockIdx < TOMB_BLOCK_SIZE(writer->ctx->tombBlock); writer->ctx->tombBlockIdx++) {
      STombRecord record1[1];
      tTombBlockGet(writer->ctx->tombBlock, writer->ctx->tombBlockIdx, record1);

      int32_t c = tTombRecordCompare(record, record1);
      if (c < 0) {
        goto _write;
      } else if (c > 0) {
        code = tTombBlockPut(writer->tombBlock, record1);
        TSDB_CHECK_CODE(code, lino, _exit);

        tsdbTrace("vgId:%d write tomb record to tomb file:%s, cid:%" PRId64 ", suid:%" PRId64 ", uid:%" PRId64
                  ", version:%" PRId64,
                  TD_VID(writer->config->tsdb->pVnode), writer->fd[TSDB_FTYPE_TOMB]->path, writer->config->cid,
                  record1->suid, record1->uid, record1->version);

        if (TOMB_BLOCK_SIZE(writer->tombBlock) >= writer->config->maxRow) {
          code = tsdbDataFileDoWriteTombBlock(writer);
          TSDB_CHECK_CODE(code, lino, _exit);
        }
      } else {
        ASSERT(0);
      }
    }

    if (writer->ctx->tombBlkArrayIdx >= TARRAY2_SIZE(writer->ctx->tombBlkArray)) {
      writer->ctx->hasOldTomb = false;
      break;
    } else {
      const STombBlk *tombBlk = TARRAY2_GET_PTR(writer->ctx->tombBlkArray, writer->ctx->tombBlkArrayIdx);

      code = tsdbDataFileReadTombBlock(writer->ctx->reader, tombBlk, writer->ctx->tombBlock);
      TSDB_CHECK_CODE(code, lino, _exit);

      writer->ctx->tombBlockIdx = 0;
      writer->ctx->tombBlkArrayIdx++;
    }
  }

_write:
  if (record->suid == INT64_MAX) goto _exit;

  code = tTombBlockPut(writer->tombBlock, record);
  TSDB_CHECK_CODE(code, lino, _exit);

  tsdbTrace("vgId:%d write tomb record to tomb file:%s, cid:%" PRId64 ", suid:%" PRId64 ", uid:%" PRId64
            ", version:%" PRId64,
            TD_VID(writer->config->tsdb->pVnode), writer->fd[TSDB_FTYPE_TOMB]->path, writer->config->cid, record->suid,
            record->uid, record->version);

  if (TOMB_BLOCK_SIZE(writer->tombBlock) >= writer->config->maxRow) {
    code = tsdbDataFileDoWriteTombBlock(writer);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbFileWriteBrinBlk(STsdbFD *fd, TBrinBlkArray *brinBlkArray, SFDataPtr *ptr, int64_t *fileSize) {
  ASSERT(TARRAY2_SIZE(brinBlkArray) > 0);
  ptr->offset = *fileSize;
  ptr->size = TARRAY2_DATA_LEN(brinBlkArray);

  int32_t code = tsdbWriteFile(fd, ptr->offset, (uint8_t *)TARRAY2_DATA(brinBlkArray), ptr->size);
  if (code) return code;

  *fileSize += ptr->size;
  return 0;
}

static int32_t tsdbDataFileWriteBrinBlk(SDataFileWriter *writer) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tsdbFileWriteBrinBlk(writer->fd[TSDB_FTYPE_HEAD], writer->brinBlkArray, writer->headFooter->brinBlkPtr,
                              &writer->files[TSDB_FTYPE_HEAD].size);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbTFileUpdVerRange(STFile *f, SVersionRange range) {
  f->minVer = TMIN(f->minVer, range.minVer);
  f->maxVer = TMAX(f->maxVer, range.maxVer);
  return 0;
}

static int32_t tsdbDataFileWriterCloseCommit(SDataFileWriter *writer, TFileOpArray *opArr) {
  int32_t code = 0;
  int32_t lino = 0;

  int32_t  ftype;
  STFileOp op;

  if (writer->fd[TSDB_FTYPE_HEAD]) {
    TABLEID tbid[1] = {{
        .suid = INT64_MAX,
        .uid = INT64_MAX,
    }};

    code = tsdbDataFileWriteTableDataEnd(writer);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbDataFileWriteTableDataBegin(writer, tbid);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbDataFileWriteBrinBlock(writer);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbDataFileWriteBrinBlk(writer);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbDataFileWriteHeadFooter(writer);
    TSDB_CHECK_CODE(code, lino, _exit);

    SVersionRange ofRange = {.minVer = VERSION_MAX, .maxVer = VERSION_MIN};

    // .head
    ftype = TSDB_FTYPE_HEAD;
    if (writer->config->files[ftype].exist) {
      op = (STFileOp){
          .optype = TSDB_FOP_REMOVE,
          .fid = writer->config->fid,
          .of = writer->config->files[ftype].file,
      };
      ofRange = (SVersionRange){.minVer = op.of.minVer, .maxVer = op.of.maxVer};
      code = TARRAY2_APPEND(opArr, op);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    op = (STFileOp){
        .optype = TSDB_FOP_CREATE,
        .fid = writer->config->fid,
        .nf = writer->files[ftype],
    };
    tsdbTFileUpdVerRange(&op.nf, ofRange);
    tsdbTFileUpdVerRange(&op.nf, writer->ctx->range);
    code = TARRAY2_APPEND(opArr, op);
    TSDB_CHECK_CODE(code, lino, _exit);

    // .data
    ftype = TSDB_FTYPE_DATA;
    if (!writer->config->files[ftype].exist) {
      op = (STFileOp){
          .optype = TSDB_FOP_CREATE,
          .fid = writer->config->fid,
          .nf = writer->files[ftype],
      };
      tsdbTFileUpdVerRange(&op.nf, writer->ctx->range);
      code = TARRAY2_APPEND(opArr, op);
      TSDB_CHECK_CODE(code, lino, _exit);
    } else if (writer->config->files[ftype].file.size != writer->files[ftype].size) {
      op = (STFileOp){
          .optype = TSDB_FOP_MODIFY,
          .fid = writer->config->fid,
          .of = writer->config->files[ftype].file,
          .nf = writer->files[ftype],
      };
      tsdbTFileUpdVerRange(&op.nf, writer->ctx->range);
      code = TARRAY2_APPEND(opArr, op);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    // .sma
    ftype = TSDB_FTYPE_SMA;
    if (!writer->config->files[ftype].exist) {
      op = (STFileOp){
          .optype = TSDB_FOP_CREATE,
          .fid = writer->config->fid,
          .nf = writer->files[ftype],
      };
      tsdbTFileUpdVerRange(&op.nf, writer->ctx->range);
      code = TARRAY2_APPEND(opArr, op);
      TSDB_CHECK_CODE(code, lino, _exit);
    } else if (writer->config->files[ftype].file.size != writer->files[ftype].size) {
      op = (STFileOp){
          .optype = TSDB_FOP_MODIFY,
          .fid = writer->config->fid,
          .of = writer->config->files[ftype].file,
          .nf = writer->files[ftype],
      };
      tsdbTFileUpdVerRange(&op.nf, writer->ctx->range);
      code = TARRAY2_APPEND(opArr, op);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  if (writer->fd[TSDB_FTYPE_TOMB]) {
    STombRecord record[1] = {{
        .suid = INT64_MAX,
        .uid = INT64_MAX,
        .version = INT64_MAX,
    }};

    code = tsdbDataFileDoWriteTombRecord(writer, record);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbDataFileDoWriteTombBlock(writer);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbDataFileDoWriteTombBlk(writer);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbDataFileWriteTombFooter(writer);
    TSDB_CHECK_CODE(code, lino, _exit);

    SVersionRange ofRange = (SVersionRange){.minVer = VERSION_MAX, .maxVer = VERSION_MIN};

    ftype = TSDB_FTYPE_TOMB;
    if (writer->config->files[ftype].exist) {
      op = (STFileOp){
          .optype = TSDB_FOP_REMOVE,
          .fid = writer->config->fid,
          .of = writer->config->files[ftype].file,
      };
      ofRange = (SVersionRange){.minVer = op.of.minVer, .maxVer = op.of.maxVer};
      code = TARRAY2_APPEND(opArr, op);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    op = (STFileOp){
        .optype = TSDB_FOP_CREATE,
        .fid = writer->config->fid,
        .nf = writer->files[ftype],
    };
    tsdbTFileUpdVerRange(&op.nf, ofRange);
    tsdbTFileUpdVerRange(&op.nf, writer->ctx->tombRange);
    code = TARRAY2_APPEND(opArr, op);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  for (int32_t i = 0; i < TSDB_FTYPE_MAX; ++i) {
    if (writer->fd[i]) {
      code = tsdbFsyncFile(writer->fd[i]);
      TSDB_CHECK_CODE(code, lino, _exit);
      tsdbCloseFile(&writer->fd[i]);
    }
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbDataFileWriterOpenDataFD(SDataFileWriter *writer) {
  int32_t code = 0;
  int32_t lino = 0;

  int32_t ftypes[] = {TSDB_FTYPE_HEAD, TSDB_FTYPE_DATA, TSDB_FTYPE_SMA};

  for (int32_t i = 0; i < ARRAY_SIZE(ftypes); ++i) {
    int32_t ftype = ftypes[i];

    char    fname[TSDB_FILENAME_LEN];
    int32_t flag = TD_FILE_READ | TD_FILE_WRITE;

    if (writer->files[ftype].size == 0) {
      flag |= (TD_FILE_CREATE | TD_FILE_TRUNC);
    }

    tsdbTFileName(writer->config->tsdb, &writer->files[ftype], fname);
    code = tsdbOpenFile(fname, writer->config->tsdb, flag, &writer->fd[ftype]);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (writer->files[ftype].size == 0) {
      uint8_t hdr[TSDB_FHDR_SIZE] = {0};

      code = tsdbWriteFile(writer->fd[ftype], 0, hdr, TSDB_FHDR_SIZE);
      TSDB_CHECK_CODE(code, lino, _exit);

      writer->files[ftype].size += TSDB_FHDR_SIZE;
    }
  }

  if (writer->ctx->reader) {
    code = tsdbDataFileReadBrinBlk(writer->ctx->reader, &writer->ctx->brinBlkArray);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbDataFileWriterOpen(const SDataFileWriterConfig *config, SDataFileWriter **writer) {
  writer[0] = taosMemoryCalloc(1, sizeof(*writer[0]));
  if (!writer[0]) return TSDB_CODE_OUT_OF_MEMORY;

  writer[0]->config[0] = config[0];
  return 0;
}

int32_t tsdbDataFileWriterClose(SDataFileWriter **writer, bool abort, TFileOpArray *opArr) {
  if (writer[0] == NULL) return 0;

  int32_t code = 0;
  int32_t lino = 0;

  if (writer[0]->ctx->opened) {
    if (abort) {
      code = tsdbDataFileWriterCloseAbort(writer[0]);
      TSDB_CHECK_CODE(code, lino, _exit);
    } else {
      code = tsdbDataFileWriterCloseCommit(writer[0], opArr);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    tsdbDataFileWriterDoClose(writer[0]);
  }
  taosMemoryFree(writer[0]);
  writer[0] = NULL;

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer[0]->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbDataFileWriteRow(SDataFileWriter *writer, SRowInfo *row) {
  int32_t code = 0;
  int32_t lino = 0;

  if (!writer->ctx->opened) {
    code = tsdbDataFileWriterDoOpen(writer);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (writer->fd[TSDB_FTYPE_HEAD] == NULL) {
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

int32_t tsdbDataFileWriteBlockData(SDataFileWriter *writer, SBlockData *bData) {
  if (bData->nRow == 0) return 0;

  int32_t code = 0;
  int32_t lino = 0;

  ASSERT(bData->uid);

  if (!writer->ctx->opened) {
    code = tsdbDataFileWriterDoOpen(writer);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (writer->fd[TSDB_FTYPE_DATA] == NULL) {
    code = tsdbDataFileWriterOpenDataFD(writer);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (bData->uid != writer->ctx->tbid->uid) {
    code = tsdbDataFileWriteTableDataEnd(writer);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbDataFileWriteTableDataBegin(writer, (TABLEID *)bData);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (writer->ctx->tbHasOldData) {
    TSDBKEY key = {
        .ts = bData->aTSKEY[0],
        .version = bData->aVersion[0],
    };

    code = tsdbDataFileDoWriteTableOldData(writer, &key);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (!writer->ctx->tbHasOldData       //
      && writer->blockData->nRow == 0  //
  ) {
    code = tsdbDataFileDoWriteBlockData(writer, bData);
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

int32_t tsdbDataFileFlush(SDataFileWriter *writer) {
  ASSERT(writer->ctx->opened);

  if (writer->blockData->nRow == 0) return 0;
  if (writer->ctx->tbHasOldData) return 0;

  return tsdbDataFileDoWriteBlockData(writer, writer->blockData);
}

static int32_t tsdbDataFileWriterOpenTombFD(SDataFileWriter *writer) {
  int32_t code = 0;
  int32_t lino = 0;

  char    fname[TSDB_FILENAME_LEN];
  int32_t ftype = TSDB_FTYPE_TOMB;

  ASSERT(writer->files[ftype].size == 0);

  int32_t flag = (TD_FILE_READ | TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);

  tsdbTFileName(writer->config->tsdb, writer->files + ftype, fname);
  code = tsdbOpenFile(fname, writer->config->tsdb, flag, &writer->fd[ftype]);
  TSDB_CHECK_CODE(code, lino, _exit);

  uint8_t hdr[TSDB_FHDR_SIZE] = {0};
  code = tsdbWriteFile(writer->fd[ftype], 0, hdr, TSDB_FHDR_SIZE);
  TSDB_CHECK_CODE(code, lino, _exit);
  writer->files[ftype].size += TSDB_FHDR_SIZE;

  if (writer->ctx->reader) {
    code = tsdbDataFileReadTombBlk(writer->ctx->reader, &writer->ctx->tombBlkArray);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (TARRAY2_SIZE(writer->ctx->tombBlkArray) > 0) {
      writer->ctx->hasOldTomb = true;
    }

    writer->ctx->tombBlkArrayIdx = 0;
    tTombBlockClear(writer->ctx->tombBlock);
    writer->ctx->tombBlockIdx = 0;
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbDataFileWriteTombRecord(SDataFileWriter *writer, const STombRecord *record) {
  int32_t code = 0;
  int32_t lino = 0;

  if (!writer->ctx->opened) {
    code = tsdbDataFileWriterDoOpen(writer);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (writer->fd[TSDB_FTYPE_TOMB] == NULL) {
    code = tsdbDataFileWriterOpenTombFD(writer);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tsdbDataFileDoWriteTombRecord(writer, record);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}
