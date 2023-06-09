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
  SFDataPtr brinBlkPtr[1];
#if 1
  SFDataPtr blockIdxPtr[1];
#endif
  SFDataPtr rsrvd[2];
} SHeadFooter;

typedef struct {
  SFDataPtr tombBlkPtr[1];
  SFDataPtr rsrvd[2];
} STombFooter;

// SDataFileReader =============================================
struct SDataFileReader {
  SDataFileReaderConfig config[1];

  uint8_t *bufArr[5];

  struct {
    bool headFooterLoaded;
    bool tombFooterLoaded;
    bool brinBlkLoaded;
    bool tombBlkLoaded;

#if 1
    TABLEID tbid[1];
    bool    blockIdxLoaded;
#endif
  } ctx[1];

  STsdbFD *fd[TSDB_FTYPE_MAX];

  SHeadFooter   headFooter[1];
  STombFooter   tombFooter[1];
  TBrinBlkArray brinBlkArray[1];
  TTombBlkArray tombBlkArray[1];

#if 1
  TDataBlkArray  dataBlkArray[1];
  TBlockIdxArray blockIdxArray[1];
#endif
};

static int32_t tsdbDataFileReadHeadFooter(SDataFileReader *reader) {
  if (reader->ctx->headFooterLoaded) return 0;

  int32_t code = 0;
  int32_t lino = 0;

  int32_t ftype = TSDB_FTYPE_HEAD;
  if (reader->fd[ftype]) {
    code = tsdbReadFile(reader->fd[ftype], reader->config->files[ftype].file.size - sizeof(SHeadFooter),
                        (uint8_t *)reader->headFooter, sizeof(SHeadFooter));
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
                        (uint8_t *)reader->tombFooter, sizeof(STombFooter));
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
        code = tsdbOpenFile(fname[i], config->szPage, TD_FILE_READ, &reader[0]->fd[i]);
        TSDB_CHECK_CODE(code, lino, _exit);
      }
    }
  } else {
    for (int32_t i = 0; i < TSDB_FTYPE_MAX; ++i) {
      if (config->files[i].exist) {
        char fname1[TSDB_FILENAME_LEN];
        tsdbTFileName(config->tsdb, &config->files[i].file, fname1);
        code = tsdbOpenFile(fname1, config->szPage, TD_FILE_READ, &reader[0]->fd[i]);
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

#if 1
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
                          reader->headFooter->brinBlkPtr->size);
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

  code = tsdbReadFile(reader->fd[TSDB_FTYPE_HEAD], brinBlk->dp->offset, reader->config->bufArr[0], brinBlk->dp->size);
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

  code = tsdbReadFile(reader->fd[TSDB_FTYPE_DATA], record->blockOffset, reader->config->bufArr[0], record->blockSize);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tDecmprBlockData(reader->config->bufArr[0], record->blockSize, bData, &reader->config->bufArr[1]);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(reader->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbDataFileReadBlockDataByCol(SDataFileReader *reader, const SBrinRecord *record, SBlockData *bData,
                                       STSchema *pTSchema, int32_t cidArr[], int32_t numCid) {
  int32_t code = 0;
  int32_t lino = 0;

  // TODO
  ASSERT(0);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(reader->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbDataFileReadBlockSma(SDataFileReader *reader) {
  int32_t code = 0;
  int32_t lino = 0;

  // TODO
  ASSERT(0);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(reader->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbDataFileReadBlockIdx(SDataFileReader *reader, const TBlockIdxArray **blockIdxArray) {
  int32_t code = 0;
  int32_t lino = 0;

  if (!reader->ctx->blockIdxLoaded) {
    code = tsdbDataFileReadHeadFooter(reader);
    TSDB_CHECK_CODE(code, lino, _exit);

    TARRAY2_CLEAR(reader->blockIdxArray, NULL);
    if (reader->fd[TSDB_FTYPE_HEAD]  //
        && reader->headFooter->blockIdxPtr->size) {
      code = tRealloc(&reader->config->bufArr[0], reader->headFooter->blockIdxPtr->size);
      TSDB_CHECK_CODE(code, lino, _exit);

      code = tsdbReadFile(reader->fd[TSDB_FTYPE_HEAD], reader->headFooter->blockIdxPtr->offset,
                          reader->config->bufArr[0], reader->headFooter->blockIdxPtr->size);
      TSDB_CHECK_CODE(code, lino, _exit);

      int32_t size = reader->headFooter->blockIdxPtr->size / sizeof(SBlockIdx);
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
  ASSERT(reader->ctx->headFooterLoaded);

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
                          reader->tombFooter->tombBlkPtr->size);
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

  code = tsdbReadFile(reader->fd[TSDB_FTYPE_TOMB], tombBlk->dp->offset, reader->config->bufArr[0], tombBlk->dp->size);
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

#if 0
    const TBlockIdxArray *blockIdxArray;
    int32_t               blockIdxArrayIdx;
    const TDataBlkArray  *dataBlkArray;
    int32_t               dataBlkArrayIdx;
#endif
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

#if 0
  TBlockIdxArray blockIdxArray[1];
  TDataBlkArray  dataBlkArray[1];
#endif
};

#if 0
static int32_t tsdbDataFileWriteBlockIdx(SDataFileWriter *writer) {
  int32_t code = 0;
  int32_t lino = 0;

  writer->headFooter->blockIdxPtr->offset = writer->files[TSDB_FTYPE_HEAD].size;
  writer->headFooter->blockIdxPtr->size = TARRAY2_DATA_LEN(writer->blockIdxArray);

  if (writer->headFooter->blockIdxPtr->size) {
    code = tsdbWriteFile(writer->fd[TSDB_FTYPE_HEAD], writer->headFooter->blockIdxPtr->offset,
                         (void *)TARRAY2_DATA(writer->blockIdxArray), writer->headFooter->blockIdxPtr->size);
    TSDB_CHECK_CODE(code, lino, _exit);
    writer->files[TSDB_FTYPE_HEAD].size += writer->headFooter->blockIdxPtr->size;
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}
#endif

static int32_t tsdbDataFileWriterCloseAbort(SDataFileWriter *writer) {
  ASSERT(0);
  return 0;
}

static int32_t tsdbDataFileWriterDoClose(SDataFileWriter *writer) {
  if (writer->ctx->reader) {
    tsdbDataFileReaderClose(&writer->ctx->reader);
  }

  tTombBlockDestroy(writer->tombBlock);
  // tStatisBlockDestroy(writer->statisBlock);
  tBlockDataDestroy(writer->blockData);
  TARRAY2_DESTROY(writer->tombBlkArray, NULL);
#if 0
  TARRAY2_DESTROY(writer->dataBlkArray, NULL);
  TARRAY2_DESTROY(writer->blockIdxArray, NULL);
#endif
  tTombBlockDestroy(writer->ctx->tombBlock);

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
  };

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

  TARRAY2_DESTROY(smaArr, NULL);

#if 0
  // to dataBlkArray
  code = TARRAY2_APPEND_PTR(writer->dataBlkArray, dataBlk);
  TSDB_CHECK_CODE(code, lino, _exit);
#endif

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

#if 0
  code = TARRAY2_APPEND_PTR(writer->blockIdxArray, blockIdx);
  TSDB_CHECK_CODE(code, lino, _exit);
#endif

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
  if (key->version <= writer->config->compactVersion                        //
      && writer->blockData->nRow > 0                                        //
      && writer->blockData->aTSKEY[writer->blockData->nRow - 1] == key->ts  //
  ) {
    code = tBlockDataUpdateRow(writer->blockData, row, writer->config->skmRow->pTSchema);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    if (writer->blockData->nRow >= writer->config->maxRow) {
      code = tsdbDataFileWriteDataBlock(writer, writer->blockData);
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

static int32_t tsdbDataFileDoWriteTSData(SDataFileWriter *writer, TSDBROW *row) {
  int32_t code = 0;
  int32_t lino = 0;

  while (writer->ctx->tbHasOldData) {
    for (; writer->ctx->blockDataIdx < writer->ctx->blockData->nRow; writer->ctx->blockDataIdx++) {
      TSDBROW row1[1] = {tsdbRowFromBlockData(writer->ctx->blockData, writer->ctx->blockDataIdx)};

      int32_t c = tsdbRowCmprFn(row, row1);
      ASSERT(c);
      if (c > 0) {
        code = tsdbDataFileDoWriteTSRow(writer, row1);
        TSDB_CHECK_CODE(code, lino, _exit);
      } else {
        goto _do_write;
      }
    }

#if 0
    if (writer->ctx->dataBlkArrayIdx >= TARRAY2_SIZE(writer->ctx->dataBlkArray)) {
      writer->ctx->tbHasOldData = false;
      break;
    }

    for (; writer->ctx->dataBlkArrayIdx < TARRAY2_SIZE(writer->ctx->dataBlkArray); writer->ctx->dataBlkArrayIdx++) {
      const SDataBlk *dataBlk = TARRAY2_GET_PTR(writer->ctx->dataBlkArray, writer->ctx->dataBlkArrayIdx);

      TSDBKEY  key = TSDBROW_KEY(row);
      SDataBlk dataBlk1[1] = {{
          .minKey = key,
          .maxKey = key,
      }};

      int32_t c = tDataBlkCmprFn(dataBlk, dataBlk1);
      if (c < 0) {
        code = tsdbDataFileWriteDataBlock(writer, writer->blockData);
        TSDB_CHECK_CODE(code, lino, _exit);

        code = TARRAY2_APPEND_PTR(writer->dataBlkArray, dataBlk);
        TSDB_CHECK_CODE(code, lino, _exit);
      } else if (c > 0) {
        goto _do_write;
      } else {
        code = tsdbDataFileReadDataBlock(writer->ctx->reader, dataBlk, writer->ctx->blockData);
        TSDB_CHECK_CODE(code, lino, _exit);

        writer->ctx->blockDataIdx = 0;
        writer->ctx->dataBlkArrayIdx++;
        break;
      }
    }
#endif
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
  if (writer->ctx->tbid->uid == 0) return 0;

  int32_t code = 0;
  int32_t lino = 0;

  if (writer->ctx->tbHasOldData) {
    for (; writer->ctx->blockDataIdx < writer->ctx->blockData->nRow; writer->ctx->blockDataIdx++) {
      TSDBROW row = tsdbRowFromBlockData(writer->ctx->blockData, writer->ctx->blockDataIdx);
      code = tsdbDataFileDoWriteTSRow(writer, &row);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  code = tsdbDataFileWriteDataBlock(writer, writer->blockData);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbDataFileWriteBrinBlock(SDataFileWriter *writer) {
  if (BRIN_BLOCK_SIZE(writer->brinBlock) == 0) return 0;

  int32_t code = 0;
  int32_t lino = 0;

  // get SBrinBlk
  SBrinBlk brinBlk[1] = {
      {
          .dp[0] =
              {
                  .offset = writer->files[TSDB_FTYPE_HEAD].size,
                  .size = 0,
              },
          .minTbid =
              {
                  .suid = TARRAY2_FIRST(writer->brinBlock->suid),
                  .uid = TARRAY2_FIRST(writer->brinBlock->uid),
              },
          .maxTbid =
              {
                  .suid = TARRAY2_LAST(writer->brinBlock->suid),
                  .uid = TARRAY2_LAST(writer->brinBlock->uid),
              },
          .minVer = TARRAY2_FIRST(writer->brinBlock->minVer),
          .maxVer = TARRAY2_FIRST(writer->brinBlock->minVer),
          .numRec = BRIN_BLOCK_SIZE(writer->brinBlock),
          .cmprAlg = writer->config->cmprAlg,
      },
  };

  for (int32_t i = 1; i < BRIN_BLOCK_SIZE(writer->brinBlock); i++) {
    if (brinBlk->minVer > TARRAY2_GET(writer->brinBlock->minVer, i)) {
      brinBlk->minVer = TARRAY2_GET(writer->brinBlock->minVer, i);
    }
    if (brinBlk->maxVer < TARRAY2_GET(writer->brinBlock->maxVer, i)) {
      brinBlk->maxVer = TARRAY2_GET(writer->brinBlock->maxVer, i);
    }
  }

  // write to file
  for (int32_t i = 0; i < ARRAY_SIZE(writer->brinBlock->dataArr1); i++) {
    code = tsdbCmprData((uint8_t *)TARRAY2_DATA(writer->brinBlock->dataArr1 + i),
                        TARRAY2_DATA_LEN(writer->brinBlock->dataArr1 + i), TSDB_DATA_TYPE_BIGINT, brinBlk->cmprAlg,
                        &writer->config->bufArr[0], 0, &brinBlk->size[i], &writer->config->bufArr[1]);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbWriteFile(writer->fd[TSDB_FTYPE_HEAD], writer->files[TSDB_FTYPE_HEAD].size, writer->config->bufArr[0],
                         brinBlk->size[i]);
    TSDB_CHECK_CODE(code, lino, _exit);

    brinBlk->dp[i].size += brinBlk->size[i];
    writer->files[TSDB_FTYPE_HEAD].size += brinBlk->size[i];
  }

  for (int32_t i = 0, j = ARRAY_SIZE(writer->brinBlock->dataArr1); i < ARRAY_SIZE(writer->brinBlock->dataArr2);
       i++, j++) {
    code = tsdbCmprData((uint8_t *)TARRAY2_DATA(writer->brinBlock->dataArr2 + i),
                        TARRAY2_DATA_LEN(writer->brinBlock->dataArr2 + i), TSDB_DATA_TYPE_INT, brinBlk->cmprAlg,
                        &writer->config->bufArr[0], 0, &brinBlk->size[j], &writer->config->bufArr[1]);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbWriteFile(writer->fd[TSDB_FTYPE_HEAD], writer->files[TSDB_FTYPE_HEAD].size, writer->config->bufArr[0],
                         brinBlk->size[j]);
    TSDB_CHECK_CODE(code, lino, _exit);

    brinBlk->dp[i].size += brinBlk->size[j];
    writer->files[TSDB_FTYPE_HEAD].size += brinBlk->size[j];
  }

  // append to brinBlkArray
  code = TARRAY2_APPEND_PTR(writer->brinBlkArray, brinBlk);
  TSDB_CHECK_CODE(code, lino, _exit);

  tBrinBlockClear(writer->brinBlock);

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

static int32_t tsdbDataFileWriteTableDataBegin(SDataFileWriter *writer, const TABLEID *tbid) {
  int32_t   code = 0;
  int32_t   lino = 0;
  SMetaInfo info;

  ASSERT(writer->ctx->blockDataIdx == writer->ctx->blockData->nRow);
  ASSERT(writer->blockData->nRow == 0);

  writer->ctx->tbHasOldData = false;
  while (writer->ctx->brinBlkArray) {  // skip data of previous table
    for (; writer->ctx->brinBlockIdx < BRIN_BLOCK_SIZE(writer->ctx->brinBlock); writer->ctx->brinBlockIdx++) {
      // skip removed table
      int64_t uid = TARRAY2_GET(writer->ctx->brinBlock->uid, writer->ctx->brinBlockIdx);
      if (metaGetInfo(writer->config->tsdb->pVnode->pMeta, uid, &info, NULL) == TSDB_CODE_NOT_FOUND) {
        for (int32_t idx = writer->ctx->brinBlockIdx + 1;   //
             idx < BRIN_BLOCK_SIZE(writer->ctx->brinBlock)  //
             && uid == TARRAY2_GET(writer->ctx->brinBlock->uid, idx);
             idx++, writer->ctx->brinBlockIdx++) {
        }
        continue;
      }

      SBrinRecord record[1];
      tBrinBlockGet(writer->ctx->brinBlock, writer->ctx->brinBlockIdx, record);

      int32_t c = tTABLEIDCmprFn(record, tbid);
      if (c < 0) {
        code = tsdbDataFileWriteBrinRecord(writer, record);
        TSDB_CHECK_CODE(code, lino, _exit);
      } else {
        if (c == 0) {
          writer->ctx->tbHasOldData = true;
        }
        goto _begin;
      }
    }

    if (writer->ctx->brinBlkArrayIdx >= TARRAY2_SIZE(writer->ctx->brinBlkArray)) {
      writer->ctx->brinBlkArray = NULL;
      break;
    }

    for (; writer->ctx->brinBlkArrayIdx < TARRAY2_SIZE(writer->ctx->brinBlkArray); writer->ctx->brinBlkArrayIdx++) {
      const SBrinBlk *brinBlk = TARRAY2_GET_PTR(writer->ctx->brinBlkArray, writer->ctx->brinBlkArrayIdx);

      code = tsdbDataFileReadBrinBlock(writer->ctx->reader, brinBlk, writer->ctx->brinBlock);
      TSDB_CHECK_CODE(code, lino, _exit);

      writer->ctx->brinBlockIdx = 0;
      writer->ctx->brinBlkArrayIdx++;
      break;
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

static int32_t tsdbDataFileWriteHeadFooter(SDataFileWriter *writer) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tsdbWriteFile(writer->fd[TSDB_FTYPE_HEAD], writer->files[TSDB_FTYPE_HEAD].size,
                       (const uint8_t *)writer->headFooter, sizeof(SHeadFooter));
  TSDB_CHECK_CODE(code, lino, _exit);
  writer->files[TSDB_FTYPE_HEAD].size += sizeof(SHeadFooter);

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

  STombBlk tombBlk[1] = {{
      .numRec = TOMB_BLOCK_SIZE(writer->tombBlock),
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
      .dp[0] =
          {
              .offset = writer->files[TSDB_FTYPE_TOMB].size,
              .size = 0,
          },
  }};

  for (int32_t i = 1; i < TOMB_BLOCK_SIZE(writer->tombBlock); i++) {
    tombBlk->minVer = TMIN(tombBlk->minVer, TARRAY2_GET(writer->tombBlock->version, i));
    tombBlk->maxVer = TMAX(tombBlk->maxVer, TARRAY2_GET(writer->tombBlock->version, i));
  }

  for (int32_t i = 0; i < ARRAY_SIZE(writer->tombBlock->dataArr); i++) {
    int32_t size;
    code = tsdbCmprData((uint8_t *)TARRAY2_DATA(&writer->tombBlock->dataArr[i]),
                        TARRAY2_DATA_LEN(&writer->tombBlock->dataArr[i]), TSDB_DATA_TYPE_BIGINT, TWO_STAGE_COMP,
                        &writer->config->bufArr[0], 0, &size, &writer->config->bufArr[1]);
    TSDB_CHECK_CODE(code, lino, _exit);

    code = tsdbWriteFile(writer->fd[TSDB_FTYPE_TOMB], writer->files[TSDB_FTYPE_TOMB].size, writer->config->bufArr[0],
                         size);
    TSDB_CHECK_CODE(code, lino, _exit);

    tombBlk->size[i] = size;
    tombBlk->dp[0].size += size;
    writer->files[TSDB_FTYPE_TOMB].size += size;
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

static int32_t tsdbDataFileDoWriteTombBlk(SDataFileWriter *writer) {
  int32_t code = 0;
  int32_t lino = 0;

  int32_t ftype = TSDB_FTYPE_TOMB;
  writer->tombFooter->tombBlkPtr->offset = writer->files[ftype].size;
  writer->tombFooter->tombBlkPtr->size = TARRAY2_DATA_LEN(writer->tombBlkArray);

  if (writer->tombFooter->tombBlkPtr->size) {
    code = tsdbWriteFile(writer->fd[ftype], writer->tombFooter->tombBlkPtr->offset,
                         (const uint8_t *)TARRAY2_DATA(writer->tombBlkArray), writer->tombFooter->tombBlkPtr->size);
    TSDB_CHECK_CODE(code, lino, _exit);
    writer->files[ftype].size += writer->tombFooter->tombBlkPtr->size;
  }

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

static int32_t tsdbDataFileWriteTombFooter(SDataFileWriter *writer) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tsdbWriteFile(writer->fd[TSDB_FTYPE_TOMB], writer->files[TSDB_FTYPE_TOMB].size,
                       (const uint8_t *)writer->tombFooter, sizeof(STombFooter));
  TSDB_CHECK_CODE(code, lino, _exit);
  writer->files[TSDB_FTYPE_TOMB].size += sizeof(STombFooter);

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
      STombRecord record1[1] = {{
          .suid = TARRAY2_GET(writer->ctx->tombBlock->suid, writer->ctx->tombBlockIdx),
          .uid = TARRAY2_GET(writer->ctx->tombBlock->uid, writer->ctx->tombBlockIdx),
          .version = TARRAY2_GET(writer->ctx->tombBlock->version, writer->ctx->tombBlockIdx),
          .skey = TARRAY2_GET(writer->ctx->tombBlock->skey, writer->ctx->tombBlockIdx),
          .ekey = TARRAY2_GET(writer->ctx->tombBlock->ekey, writer->ctx->tombBlockIdx),
      }};

      int32_t c = tTombRecordCompare(record, record1);
      if (c < 0) {
        break;
      } else if (c > 0) {
        code = tTombBlockPut(writer->tombBlock, record1);
        TSDB_CHECK_CODE(code, lino, _exit);

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
    }

    for (; writer->ctx->tombBlkArrayIdx < TARRAY2_SIZE(writer->ctx->tombBlkArray); ++writer->ctx->tombBlkArrayIdx) {
      const STombBlk *tombBlk = TARRAY2_GET_PTR(writer->ctx->tombBlkArray, writer->ctx->tombBlkArrayIdx);

      code = tsdbDataFileReadTombBlock(writer->ctx->reader, tombBlk, writer->ctx->tombBlock);
      TSDB_CHECK_CODE(code, lino, _exit);

      writer->ctx->tombBlockIdx = 0;
      writer->ctx->tombBlkArrayIdx++;
      break;
    }
  }

_write:
  if (record->suid == INT64_MAX) goto _exit;

  code = tTombBlockPut(writer->tombBlock, record);
  TSDB_CHECK_CODE(code, lino, _exit);

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

#if 0
    code = tsdbDataFileWriteBlockIdx(writer);
    TSDB_CHECK_CODE(code, lino, _exit);
#endif

    code = tsdbDataFileWriteHeadFooter(writer);
    TSDB_CHECK_CODE(code, lino, _exit);

    // .head
    ftype = TSDB_FTYPE_HEAD;
    if (writer->config->files[ftype].exist) {
      op = (STFileOp){
          .optype = TSDB_FOP_REMOVE,
          .fid = writer->config->fid,
          .of = writer->config->files[ftype].file,
      };
      code = TARRAY2_APPEND(opArr, op);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    op = (STFileOp){
        .optype = TSDB_FOP_CREATE,
        .fid = writer->config->fid,
        .nf = writer->files[ftype],
    };
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
      code = TARRAY2_APPEND(opArr, op);
      TSDB_CHECK_CODE(code, lino, _exit);
    } else if (writer->config->files[ftype].file.size != writer->files[ftype].size) {
      op = (STFileOp){
          .optype = TSDB_FOP_MODIFY,
          .fid = writer->config->fid,
          .of = writer->config->files[ftype].file,
          .nf = writer->files[ftype],
      };
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
      code = TARRAY2_APPEND(opArr, op);
      TSDB_CHECK_CODE(code, lino, _exit);
    } else if (writer->config->files[ftype].file.size != writer->files[ftype].size) {
      op = (STFileOp){
          .optype = TSDB_FOP_MODIFY,
          .fid = writer->config->fid,
          .of = writer->config->files[ftype].file,
          .nf = writer->files[ftype],
      };
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

    ftype = TSDB_FTYPE_SMA;
    if (writer->config->files[ftype].exist) {
      op = (STFileOp){
          .optype = TSDB_FOP_REMOVE,
          .fid = writer->config->fid,
          .of = writer->config->files[ftype].file,
      };
      code = TARRAY2_APPEND(opArr, op);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    op = (STFileOp){
        .optype = TSDB_FOP_CREATE,
        .fid = writer->config->fid,
        .nf = writer->files[ftype],
    };
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
    code = tsdbOpenFile(fname, writer->config->szPage, flag, &writer->fd[ftype]);
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
#if 0
    code = tsdbDataFileReadBlockIdx(writer->ctx->reader, &writer->ctx->blockIdxArray);
    TSDB_CHECK_CODE(code, lino, _exit);
#endif
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

int32_t tsdbDataFileWriteTSData(SDataFileWriter *writer, SRowInfo *row) {
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

  if (!writer->ctx->tbHasOldData       //
      && writer->blockData->nRow == 0  //
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

  if (writer->blockData->nRow == 0) return 0;
  if (writer->ctx->tbHasOldData) return 0;

  return tsdbDataFileWriteDataBlock(writer, writer->blockData);
}

static int32_t tsdbDataFileWriterOpenTombFD(SDataFileWriter *writer) {
  int32_t code = 0;
  int32_t lino = 0;

  char    fname[TSDB_FILENAME_LEN];
  int32_t ftype = TSDB_FTYPE_TOMB;

  ASSERT(writer->files[ftype].size == 0);

  int32_t flag = (TD_FILE_READ | TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);

  tsdbTFileName(writer->config->tsdb, writer->files + ftype, fname);
  code = tsdbOpenFile(fname, writer->config->szPage, flag, &writer->fd[ftype]);
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