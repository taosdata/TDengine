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
#include "meta.h"

// SDataFileReader =============================================
struct SDataFileReader {
  SDataFileReaderConfig config[1];

  SBuffer  local[10];
  SBuffer *buffers;

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
    int32_t encryptAlgorithm = reader->config->tsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
    char* encryptKey = reader->config->tsdb->pVnode->config.tsdbCfg.encryptKey;
#if 1
    code = tsdbReadFile(reader->fd[ftype], reader->config->files[ftype].file.size - sizeof(SHeadFooter),
                        (uint8_t *)reader->headFooter, sizeof(SHeadFooter), 0, encryptAlgorithm, encryptKey);
    TSDB_CHECK_CODE(code, lino, _exit);
#else
    int64_t size = reader->config->files[ftype].file.size;
    for (; size > TSDB_FHDR_SIZE; size--) {
      code = tsdbReadFile(reader->fd[ftype], size - sizeof(SHeadFooter), (uint8_t *)reader->headFooter,
                          sizeof(SHeadFooter), 0, encryptAlgorithm, encryptKey);
      if (code) continue;
      if (reader->headFooter->brinBlkPtr->offset + reader->headFooter->brinBlkPtr->size + sizeof(SHeadFooter) == size) {
        break;
      }
    }
    if (size <= TSDB_FHDR_SIZE) {
      TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _exit);
    }
#endif
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
    int32_t encryptAlgorithm = reader->config->tsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
    char* encryptKey = reader->config->tsdb->pVnode->config.tsdbCfg.encryptKey;
    code = tsdbReadFile(reader->fd[ftype], reader->config->files[ftype].file.size - sizeof(STombFooter),
                        (uint8_t *)reader->tombFooter, sizeof(STombFooter), 0, encryptAlgorithm, encryptKey);
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

  for (int32_t i = 0; i < ARRAY_SIZE(reader[0]->local); i++) {
    tBufferInit(reader[0]->local + i);
  }

  reader[0]->config[0] = config[0];
  reader[0]->buffers = config->buffers;
  if (reader[0]->buffers == NULL) {
    reader[0]->buffers = reader[0]->local;
  }

  if (fname) {
    for (int32_t i = 0; i < TSDB_FTYPE_MAX; ++i) {
      if (fname[i]) {
        int32_t lcn = config->files[i].file.lcn;
        code = tsdbOpenFile(fname[i], config->tsdb, TD_FILE_READ, &reader[0]->fd[i], lcn);
        TSDB_CHECK_CODE(code, lino, _exit);
      }
    }
  } else {
    for (int32_t i = 0; i < TSDB_FTYPE_MAX; ++i) {
      if (config->files[i].exist) {
        char fname1[TSDB_FILENAME_LEN];
        tsdbTFileName(config->tsdb, &config->files[i].file, fname1);
        int32_t lcn = config->files[i].file.lcn;
        code = tsdbOpenFile(fname1, config->tsdb, TD_FILE_READ, &reader[0]->fd[i], lcn);
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

  for (int32_t i = 0; i < TSDB_FTYPE_MAX; ++i) {
    if (reader[0]->fd[i]) {
      tsdbCloseFile(&reader[0]->fd[i]);
    }
  }

  for (int32_t i = 0; i < ARRAY_SIZE(reader[0]->local); ++i) {
    tBufferDestroy(reader[0]->local + i);
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

      int32_t encryptAlgorithm = reader->config->tsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
      char* encryptKey = reader->config->tsdb->pVnode->config.tsdbCfg.encryptKey;
      code = tsdbReadFile(reader->fd[TSDB_FTYPE_HEAD], reader->headFooter->brinBlkPtr->offset, data,
                          reader->headFooter->brinBlkPtr->size, 0, encryptAlgorithm, encryptKey);
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

  SBuffer *buffer = reader->buffers + 0;
  SBuffer *assist = reader->buffers + 1;

  int32_t encryptAlgorithm = reader->config->tsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
  char* encryptKey = reader->config->tsdb->pVnode->config.tsdbCfg.encryptKey;
  // load data
  tBufferClear(buffer);
  code = tsdbReadFileToBuffer(reader->fd[TSDB_FTYPE_HEAD], brinBlk->dp->offset, brinBlk->dp->size, buffer, 0,
                             encryptAlgorithm, encryptKey);
  TSDB_CHECK_CODE(code, lino, _exit);

  // decode brin block
  SBufferReader br = BUFFER_READER_INITIALIZER(0, buffer);
  tBrinBlockClear(brinBlock);
  brinBlock->numOfPKs = brinBlk->numOfPKs;
  brinBlock->numOfRecords = brinBlk->numRec;
  for (int32_t i = 0; i < 10; i++) {  // int64_t

    SCompressInfo cinfo = {
        .cmprAlg = brinBlk->cmprAlg,
        .dataType = TSDB_DATA_TYPE_BIGINT,
        .compressedSize = brinBlk->size[i],
        .originalSize = brinBlk->numRec * sizeof(int64_t),
    };
    code = tDecompressDataToBuffer(BR_PTR(&br), &cinfo, brinBlock->buffers + i, assist);
    TSDB_CHECK_CODE(code, lino, _exit);
    br.offset += brinBlk->size[i];
  }

  for (int32_t i = 10; i < 15; i++) {  // int32_t
    SCompressInfo cinfo = {
        .cmprAlg = brinBlk->cmprAlg,
        .dataType = TSDB_DATA_TYPE_INT,
        .compressedSize = brinBlk->size[i],
        .originalSize = brinBlk->numRec * sizeof(int32_t),
    };
    code = tDecompressDataToBuffer(BR_PTR(&br), &cinfo, brinBlock->buffers + i, assist);
    TSDB_CHECK_CODE(code, lino, _exit);
    br.offset += brinBlk->size[i];
  }

  // primary keys
  if (brinBlk->numOfPKs > 0) {  // decode the primary keys
    SValueColumnCompressInfo firstInfos[TD_MAX_PK_COLS];
    SValueColumnCompressInfo lastInfos[TD_MAX_PK_COLS];

    for (int32_t i = 0; i < brinBlk->numOfPKs; i++) {
      code = tValueColumnCompressInfoDecode(&br, firstInfos + i);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    for (int32_t i = 0; i < brinBlk->numOfPKs; i++) {
      code = tValueColumnCompressInfoDecode(&br, lastInfos + i);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    for (int32_t i = 0; i < brinBlk->numOfPKs; i++) {
      SValueColumnCompressInfo *info = firstInfos + i;

      code = tValueColumnDecompress(BR_PTR(&br), info, brinBlock->firstKeyPKs + i, assist);
      TSDB_CHECK_CODE(code, lino, _exit);
      br.offset += (info->offsetCompressedSize + info->dataCompressedSize);
    }

    for (int32_t i = 0; i < brinBlk->numOfPKs; i++) {
      SValueColumnCompressInfo *info = lastInfos + i;

      code = tValueColumnDecompress(BR_PTR(&br), info, brinBlock->lastKeyPKs + i, assist);
      TSDB_CHECK_CODE(code, lino, _exit);
      br.offset += (info->offsetCompressedSize + info->dataCompressedSize);
    }
  }

  ASSERT(br.offset == br.buffer->size);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(reader->config->tsdb->pVnode), lino, code);
  }
  return code;
}

extern int32_t tBlockDataDecompress(SBufferReader *br, SBlockData *blockData, SBuffer *assist);

int32_t tsdbDataFileReadBlockData(SDataFileReader *reader, const SBrinRecord *record, SBlockData *bData) {
  int32_t code = 0;
  int32_t lino = 0;
  SHashObj *pEncryTable = NULL;
  SBuffer *buffer = reader->buffers + 0;
  SBuffer *assist = reader->buffers + 1;

  int32_t encryptAlgorithm = reader->config->tsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
  char* encryptKey = reader->config->tsdb->pVnode->config.tsdbCfg.encryptKey;

  // load data
  tBufferClear(buffer);
  code = tsdbReadFileToBuffer(reader->fd[TSDB_FTYPE_DATA], record->blockOffset, record->blockSize, buffer, 0,
                             encryptAlgorithm, encryptKey);
  TSDB_CHECK_CODE(code, lino, _exit);

  // decompress
  SBufferReader br = BUFFER_READER_INITIALIZER(0, buffer);
  code = tBlockDataDecompress(&br, bData, assist);
  TSDB_CHECK_CODE(code, lino, _exit);
  ASSERT(br.offset == buffer->size);

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

  SHashObj    *pEncryTable = NULL;
  SDiskDataHdr hdr;
  SBuffer     *buffer0 = reader->buffers + 0;
  SBuffer     *buffer1 = reader->buffers + 1;
  SBuffer     *assist = reader->buffers + 2;

  int32_t encryptAlgorithm = reader->config->tsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
  char* encryptKey = reader->config->tsdb->pVnode->config.tsdbCfg.encryptKey;
  // load key part
  tBufferClear(buffer0);

  code = tsdbReadFileToBuffer(reader->fd[TSDB_FTYPE_DATA], record->blockOffset, record->blockKeySize, buffer0, 0,
                              encryptAlgorithm, encryptKey);
  TSDB_CHECK_CODE(code, lino, _exit);

  // SDiskDataHdr
  SBufferReader br = BUFFER_READER_INITIALIZER(0, buffer0);
  code = tGetDiskDataHdr(&br, &hdr);
  TSDB_CHECK_CODE(code, lino, _exit);

  ASSERT(hdr.delimiter == TSDB_FILE_DLMT);

  tBlockDataReset(bData);
  bData->suid = hdr.suid;
  bData->uid = hdr.uid;
  bData->nRow = hdr.nRow;

  metaGetEncryParam(reader->config->tsdb->pVnode->pMeta, hdr.suid, &pEncryTable);

  // Key part
  code = tBlockDataDecompressKeyPart(&hdr, &br, bData, assist);
  TSDB_CHECK_CODE(code, lino, _exit);
  ASSERT(br.offset == buffer0->size);

  int extraColIdx = -1;
  for (int i = 0; i < ncid; i++) {
    if (tBlockDataGetColData(bData, cids[i]) == NULL) {
      extraColIdx = i;
      break;
    }
  }

  if (extraColIdx < 0) {
    goto _exit;
  }
  
  // load SBlockCol part
  tBufferClear(buffer0);
  code = tsdbReadFileToBuffer(reader->fd[TSDB_FTYPE_DATA], record->blockOffset + record->blockKeySize, hdr.szBlkCol,
                              buffer0, 0, encryptAlgorithm, encryptKey);
  TSDB_CHECK_CODE(code, lino, _exit);

  // calc szHint
  int64_t szHint = 0;
  int     extraCols = 1;
  for (int i = extraColIdx + 1; i < ncid; ++i) {
    if (tBlockDataGetColData(bData, cids[i]) == NULL) {
      ++extraCols;
      break;
    }
  }

  if (extraCols >= 2) {
    br = BUFFER_READER_INITIALIZER(0, buffer0);

    SBlockCol blockCol = {.cid = 0};
    for (int32_t i = extraColIdx; i < ncid; ++i) {
      int16_t extraColCid = cids[i];

      while (extraColCid > blockCol.cid) {
        if (br.offset >= buffer0->size) {
          blockCol.cid = INT16_MAX;
          break;
        }

        code = tGetBlockCol(&br, &blockCol, hdr.fmtVer, hdr.cmprAlg);
        TSDB_CHECK_CODE(code, lino, _exit);
      }

      if (extraColCid == blockCol.cid || blockCol.cid == INT16_MAX) {
        extraColIdx = i;
        break;
      }
    }

    if (blockCol.cid > 0 && blockCol.cid < INT16_MAX /*&& blockCol->flag == HAS_VALUE*/) {
      int64_t   offset = blockCol.offset;
      SBlockCol lastNonNoneBlockCol = {.cid = 0};

      for (int32_t i = extraColIdx; i < ncid; ++i) {
        int16_t extraColCid = cids[i];

        while (extraColCid > blockCol.cid) {
          if (br.offset >= buffer0->size) {
            blockCol.cid = INT16_MAX;
            break;
          }

          code = tGetBlockCol(&br, &blockCol, hdr.fmtVer, hdr.cmprAlg);
          TSDB_CHECK_CODE(code, lino, _exit);
        }

        if (extraColCid == blockCol.cid) {
          lastNonNoneBlockCol = blockCol;
          continue;
        }

        if (blockCol.cid == INT16_MAX) {
          break;
        }
      }

      if (lastNonNoneBlockCol.cid > 0) {
        szHint = lastNonNoneBlockCol.offset + lastNonNoneBlockCol.szBitmap + lastNonNoneBlockCol.szOffset +
                 lastNonNoneBlockCol.szValue - offset;
      }
    }
  }

  // load each column
  SBlockCol blockCol = {
      .cid = 0,
  };
  bool firstRead = true;
  br = BUFFER_READER_INITIALIZER(0, buffer0);
  for (int32_t i = 0; i < ncid; i++) {
    int16_t cid = cids[i];

    if (tBlockDataGetColData(bData, cid)) {  // already loaded
      continue;
    }

    while (cid > blockCol.cid) {
      if (br.offset >= buffer0->size) {
        blockCol.cid = INT16_MAX;
        break;
      }

      code = tGetBlockCol(&br, &blockCol, hdr.fmtVer, hdr.cmprAlg);
      TSDB_CHECK_CODE(code, lino, _exit);
    }

    if (cid < blockCol.cid) {
      const STColumn *tcol = tTSchemaSearchColumn(pTSchema, cid);
      ASSERT(tcol);
      SBlockCol none = {
          .cid = cid,
          .type = tcol->type,
          .cflag = tcol->flags,
          .flag = HAS_NONE,
          .szOrigin = 0,
          .szBitmap = 0,
          .szOffset = 0,
          .szValue = 0,
          .offset = 0,
      };
      code = tBlockDataDecompressColData(&hdr, &none, &br, bData, assist);
      TSDB_CHECK_CODE(code, lino, _exit);
    } else if (cid == blockCol.cid) {
      int32_t encryptAlgorithm = reader->config->tsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
      char* encryptKey = reader->config->tsdb->pVnode->config.tsdbCfg.encryptKey;
     // load from file
      tBufferClear(buffer1);
      code = tsdbReadFileToBuffer(
          reader->fd[TSDB_FTYPE_DATA], record->blockOffset + record->blockKeySize + hdr.szBlkCol + blockCol.offset,
          blockCol.szBitmap + blockCol.szOffset + blockCol.szValue, buffer1, firstRead ? szHint : 0,
          encryptAlgorithm, encryptKey);
      TSDB_CHECK_CODE(code, lino, _exit);

      firstRead = false;

      // decode the buffer
      SBufferReader br1 = BUFFER_READER_INITIALIZER(0, buffer1);
      code = tBlockDataDecompressColData(&hdr, &blockCol, &br1, bData, assist);
      TSDB_CHECK_CODE(code, lino, _exit);
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
  int32_t  code = 0;
  int32_t  lino = 0;
  SBuffer *buffer = reader->buffers + 0;

  TARRAY2_CLEAR(columnDataAggArray, NULL);
  if (record->smaSize > 0) {
    tBufferClear(buffer);
    int32_t encryptAlgorithm = reader->config->tsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
    char* encryptKey = reader->config->tsdb->pVnode->config.tsdbCfg.encryptKey;
    code = tsdbReadFileToBuffer(reader->fd[TSDB_FTYPE_SMA], record->smaOffset, record->smaSize, buffer, 0,
                               encryptAlgorithm, encryptKey);
    TSDB_CHECK_CODE(code, lino, _exit);

    // decode sma data
    SBufferReader br = BUFFER_READER_INITIALIZER(0, buffer);
    while (br.offset < record->smaSize) {
      SColumnDataAgg sma[1];

      code = tGetColumnDataAgg(&br, sma);
      TSDB_CHECK_CODE(code, lino, _exit);

      code = TARRAY2_APPEND_PTR(columnDataAggArray, sma);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    ASSERT(br.offset == record->smaSize);
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

      int32_t encryptAlgorithm = reader->config->tsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
      char* encryptKey = reader->config->tsdb->pVnode->config.tsdbCfg.encryptKey;
      code = tsdbReadFile(reader->fd[TSDB_FTYPE_TOMB], reader->tombFooter->tombBlkPtr->offset, data,
                          reader->tombFooter->tombBlkPtr->size, 0, encryptAlgorithm, encryptKey);
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

  SBuffer *buffer0 = reader->buffers + 0;
  SBuffer *assist = reader->buffers + 1;

  tBufferClear(buffer0);
  int32_t encryptAlgorithm = reader->config->tsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
  char* encryptKey = reader->config->tsdb->pVnode->config.tsdbCfg.encryptKey;
  code = tsdbReadFileToBuffer(reader->fd[TSDB_FTYPE_TOMB], tombBlk->dp->offset, tombBlk->dp->size, buffer0, 0,
                             encryptAlgorithm, encryptKey);
  TSDB_CHECK_CODE(code, lino, _exit);

  int32_t       size = 0;
  SBufferReader br = BUFFER_READER_INITIALIZER(0, buffer0);
  tTombBlockClear(tData);
  tData->numOfRecords = tombBlk->numRec;
  for (int32_t i = 0; i < ARRAY_SIZE(tData->buffers); ++i) {
    SCompressInfo cinfo = {
        .cmprAlg = tombBlk->cmprAlg,
        .dataType = TSDB_DATA_TYPE_BIGINT,
        .originalSize = tombBlk->numRec * sizeof(int64_t),
        .compressedSize = tombBlk->size[i],
    };
    code = tDecompressDataToBuffer(BR_PTR(&br), &cinfo, tData->buffers + i, assist);
    TSDB_CHECK_CODE(code, lino, _exit);
    br.offset += tombBlk->size[i];
  }

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
  SBuffer  local[10];
  SBuffer *buffers;

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

  for (int32_t i = 0; i < ARRAY_SIZE(writer->local); ++i) {
    tBufferDestroy(writer->local + i);
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
          .buffers = writer->buffers,
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
  writer->buffers = writer->config->buffers;
  if (writer->buffers == NULL) {
    writer->buffers = writer->local;
  }

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
        .lcn = writer->config->lcn == -1 ? 0 : -1,
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

int32_t tsdbFileWriteBrinBlock(STsdbFD *fd, SBrinBlock *brinBlock, uint32_t cmprAlg, int64_t *fileSize,
                               TBrinBlkArray *brinBlkArray, SBuffer *buffers, SVersionRange *range,
                              int32_t encryptAlgorithm, char* encryptKey) {
  if (brinBlock->numOfRecords == 0) return 0;

  int32_t  code;
  SBuffer *buffer0 = buffers + 0;
  SBuffer *buffer1 = buffers + 1;
  SBuffer *assist = buffers + 2;

  SBrinBlk brinBlk = {
      .dp[0] =
          {
              .offset = *fileSize,
              .size = 0,
          },
      .numRec = brinBlock->numOfRecords,
      .numOfPKs = brinBlock->numOfPKs,
      .cmprAlg = cmprAlg,
  };
  for (int i = 0; i < brinBlock->numOfRecords; i++) {
    SBrinRecord record;

    tBrinBlockGet(brinBlock, i, &record);
    if (i == 0) {
      brinBlk.minTbid.suid = record.suid;
      brinBlk.minTbid.uid = record.uid;
      brinBlk.minVer = record.minVer;
      brinBlk.maxVer = record.maxVer;
    }
    if (i == brinBlock->numOfRecords - 1) {
      brinBlk.maxTbid.suid = record.suid;
      brinBlk.maxTbid.uid = record.uid;
    }
    if (record.minVer < brinBlk.minVer) {
      brinBlk.minVer = record.minVer;
    }
    if (record.maxVer > brinBlk.maxVer) {
      brinBlk.maxVer = record.maxVer;
    }
  }

  tsdbWriterUpdVerRange(range, brinBlk.minVer, brinBlk.maxVer);

  // write to file
  for (int32_t i = 0; i < 10; ++i) {
    SCompressInfo info = {
        .cmprAlg = cmprAlg,
        .dataType = TSDB_DATA_TYPE_BIGINT,
        .originalSize = brinBlock->buffers[i].size,
    };

    tBufferClear(buffer0);
    code = tCompressDataToBuffer(brinBlock->buffers[i].data, &info, buffer0, assist);
    if (code) return code;
    code = tsdbWriteFile(fd, *fileSize, buffer0->data, buffer0->size, encryptAlgorithm, encryptKey);
    if (code) return code;
    brinBlk.size[i] = info.compressedSize;
    brinBlk.dp->size += info.compressedSize;
    *fileSize += info.compressedSize;
  }
  for (int32_t i = 10; i < 15; ++i) {
    SCompressInfo info = {
        .cmprAlg = cmprAlg,
        .dataType = TSDB_DATA_TYPE_INT,
        .originalSize = brinBlock->buffers[i].size,
    };

    tBufferClear(buffer0);
    code = tCompressDataToBuffer(brinBlock->buffers[i].data, &info, buffer0, assist);
    if (code) return code;
    code = tsdbWriteFile(fd, *fileSize, buffer0->data, buffer0->size, encryptAlgorithm, encryptKey);
    if (code) return code;
    brinBlk.size[i] = info.compressedSize;
    brinBlk.dp->size += info.compressedSize;
    *fileSize += info.compressedSize;
  }

  // write primary keys to file
  if (brinBlock->numOfPKs > 0) {
    tBufferClear(buffer0);
    tBufferClear(buffer1);

    // encode
    for (int i = 0; i < brinBlock->numOfPKs; i++) {
      SValueColumnCompressInfo info = {.cmprAlg = cmprAlg};
      if ((code = tValueColumnCompress(&brinBlock->firstKeyPKs[i], &info, buffer1, assist))) return code;
      if ((code = tValueColumnCompressInfoEncode(&info, buffer0))) return code;
    }
    for (int i = 0; i < brinBlock->numOfPKs; i++) {
      SValueColumnCompressInfo info = {.cmprAlg = cmprAlg};
      if ((code = tValueColumnCompress(&brinBlock->lastKeyPKs[i], &info, buffer1, assist))) return code;
      if ((code = tValueColumnCompressInfoEncode(&info, buffer0))) return code;
    }

    // write to file
    if ((code = tsdbWriteFile(fd, *fileSize, buffer0->data, buffer0->size, encryptAlgorithm, encryptKey))) return code;
    *fileSize += buffer0->size;
    brinBlk.dp->size += buffer0->size;
    if ((code = tsdbWriteFile(fd, *fileSize, buffer1->data, buffer1->size, encryptAlgorithm, encryptKey))) return code;
    *fileSize += buffer1->size;
    brinBlk.dp->size += buffer1->size;
  }

  // append to brinBlkArray
  code = TARRAY2_APPEND_PTR(brinBlkArray, &brinBlk);
  if (code) return code;

  tBrinBlockClear(brinBlock);

  return 0;
}

static int32_t tsdbDataFileWriteBrinBlock(SDataFileWriter *writer) {
  if (writer->brinBlock->numOfRecords == 0) return 0;

  int32_t code = 0;
  int32_t lino = 0;

  int32_t encryptAlgorithm = writer->config->tsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
  char* encryptKey = writer->config->tsdb->pVnode->config.tsdbCfg.encryptKey;

  code = tsdbFileWriteBrinBlock(writer->fd[TSDB_FTYPE_HEAD], writer->brinBlock, writer->config->cmprAlg,
                                &writer->files[TSDB_FTYPE_HEAD].size, writer->brinBlkArray, writer->buffers,
                                &writer->ctx->range, encryptAlgorithm, encryptKey);
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

  for (;;) {
    code = tBrinBlockPut(writer->brinBlock, record);
    if (code == TSDB_CODE_INVALID_PARA) {
      // different records with different primary keys
      code = tsdbDataFileWriteBrinBlock(writer);
      TSDB_CHECK_CODE(code, lino, _exit);
      continue;
    } else {
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    break;
  }

  if ((writer->brinBlock->numOfRecords) >= writer->config->maxRow) {
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

  int32_t  code = 0;
  int32_t  lino = 0;
  SBuffer *buffers = writer->buffers;
  SBuffer *assist = writer->buffers + 4;

  SColCompressInfo cmprInfo = {.pColCmpr = NULL, .defaultCmprAlg = writer->config->cmprAlg};

  SBrinRecord record[1] = {{
      .suid = bData->suid,
      .uid = bData->uid,
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

  tsdbRowGetKey(&tsdbRowFromBlockData(bData, 0), &record->firstKey);
  tsdbRowGetKey(&tsdbRowFromBlockData(bData, bData->nRow - 1), &record->lastKey);

  for (int32_t i = 1; i < bData->nRow; ++i) {
    if (tsdbRowCompareWithoutVersion(&tsdbRowFromBlockData(bData, i - 1), &tsdbRowFromBlockData(bData, i)) != 0) {
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

  code = metaGetColCmpr(writer->config->tsdb->pVnode->pMeta, bData->suid != 0 ? bData->suid : bData->uid,
                        &cmprInfo.pColCmpr);

  code = metaGetEncryParam(writer->config->tsdb->pVnode->pMeta, bData->suid != 0 ? bData->suid : bData->uid,
                           &cmprInfo.pEncryption);

  code = tBlockDataCompress(bData, &cmprInfo, buffers, assist);
  TSDB_CHECK_CODE(code, lino, _exit);

  

  record->blockKeySize = buffers[0].size + buffers[1].size;
  record->blockSize = record->blockKeySize + buffers[2].size + buffers[3].size;

  int32_t encryptAlgorithm = writer->config->tsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
  char* encryptKey = writer->config->tsdb->pVnode->config.tsdbCfg.encryptKey;
  for (int i = 0; i < 4; i++) {
    code = tsdbWriteFile(writer->fd[TSDB_FTYPE_DATA], writer->files[TSDB_FTYPE_DATA].size, buffers[i].data,
                          buffers[i].size, encryptAlgorithm, encryptKey);
    TSDB_CHECK_CODE(code, lino, _exit);
    writer->files[TSDB_FTYPE_DATA].size += buffers[i].size;
  }

  // to .sma file
  tBufferClear(&buffers[0]);
  for (int32_t i = 0; i < bData->nColData; ++i) {
    SColData *colData = bData->aColData + i;
    if ((colData->cflag & COL_SMA_ON) == 0 || ((colData->flag & HAS_VALUE) == 0)) continue;

    SColumnDataAgg sma[1] = {{.colId = colData->cid}};
    tColDataCalcSMA[colData->type](colData, &sma->sum, &sma->max, &sma->min, &sma->numOfNull);

    code = tPutColumnDataAgg(&buffers[0], sma);
    TSDB_CHECK_CODE(code, lino, _exit);
  }
  record->smaSize = buffers[0].size;

  if (record->smaSize > 0) {
    code = tsdbWriteFile(writer->fd[TSDB_FTYPE_SMA], record->smaOffset, buffers[0].data, record->smaSize,
                        encryptAlgorithm, encryptKey);
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
  taosHashCleanup(cmprInfo.pColCmpr);

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

  if (TSDBROW_VERSION(row) <= writer->config->compactVersion  //
      && writer->blockData->nRow > 0                          //
      &&
      tsdbRowCompareWithoutVersion(row, &tsdbRowFromBlockData(writer->blockData, writer->blockData->nRow - 1)) == 0  //
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

static FORCE_INLINE int32_t tsdbRowKeyCmprNullAsLargest(const STsdbRowKey *key1, const STsdbRowKey *key2) {
  if (key1 == NULL) {
    return 1;
  } else if (key2 == NULL) {
    return -1;
  } else {
    return tsdbRowKeyCmpr(key1, key2);
  }
}

static int32_t tsdbDataFileDoWriteTableOldData(SDataFileWriter *writer, const STsdbRowKey *key) {
  if (writer->ctx->tbHasOldData == false) return 0;

  int32_t     code = 0;
  int32_t     lino = 0;
  STsdbRowKey rowKey;

  for (;;) {
    for (;;) {
      // SBlockData
      for (; writer->ctx->blockDataIdx < writer->ctx->blockData->nRow; writer->ctx->blockDataIdx++) {
        TSDBROW row = tsdbRowFromBlockData(writer->ctx->blockData, writer->ctx->blockDataIdx);

        tsdbRowGetKey(&row, &rowKey);
        if (tsdbRowKeyCmprNullAsLargest(&rowKey, key) < 0) {  // key <= rowKey
          code = tsdbDataFileDoWriteTSRow(writer, &row);
          TSDB_CHECK_CODE(code, lino, _exit);
        } else {
          goto _exit;
        }
      }

      // SBrinBlock
      if (writer->ctx->brinBlockIdx >= writer->ctx->brinBlock->numOfRecords) {
        break;
      }

      for (; writer->ctx->brinBlockIdx < writer->ctx->brinBlock->numOfRecords; writer->ctx->brinBlockIdx++) {
        SBrinRecord record;
        tBrinBlockGet(writer->ctx->brinBlock, writer->ctx->brinBlockIdx, &record);
        if (record.uid != writer->ctx->tbid->uid) {
          writer->ctx->tbHasOldData = false;
          goto _exit;
        }

        if (tsdbRowKeyCmprNullAsLargest(key, &record.firstKey) < 0) {  // key < record->firstKey
          goto _exit;
        } else {
          SBrinRecord record[1];
          tBrinBlockGet(writer->ctx->brinBlock, writer->ctx->brinBlockIdx, record);
          if (tsdbRowKeyCmprNullAsLargest(key, &record->lastKey) > 0) {  // key > record->lastKey
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
    STsdbRowKey key;
    tsdbRowGetKey(row, &key);

    code = tsdbDataFileDoWriteTableOldData(writer, &key);
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
    code = tsdbDataFileDoWriteTableOldData(writer, NULL /* as the largest key */);
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
    for (; writer->ctx->brinBlockIdx < writer->ctx->brinBlock->numOfRecords; writer->ctx->brinBlockIdx++) {
      SBrinRecord record;
      tBrinBlockGet(writer->ctx->brinBlock, writer->ctx->brinBlockIdx, &record);

      if (record.uid == tbid->uid) {
        writer->ctx->tbHasOldData = true;
        goto _begin;
      } else if (record.suid > tbid->suid || (record.suid == tbid->suid && record.uid > tbid->uid)) {
        goto _begin;
      } else {
        if (record.uid != writer->ctx->tbid->uid) {
          if (drop && tbid1->uid == record.uid) {
            continue;
          } else if (metaGetInfo(writer->config->tsdb->pVnode->pMeta, record.uid, &info, NULL) != 0) {
            drop = true;
            tbid1->suid = record.suid;
            tbid1->uid = record.uid;
            continue;
          } else {
            drop = false;
            writer->ctx->tbid->suid = record.suid;
            writer->ctx->tbid->uid = record.uid;
          }
        }

        code = tsdbDataFileWriteBrinRecord(writer, &record);
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

int32_t tsdbFileWriteHeadFooter(STsdbFD *fd, int64_t *fileSize, const SHeadFooter *footer, int32_t encryptAlgorithm, 
                                char* encryptKey) {
  int32_t code = tsdbWriteFile(fd, *fileSize, (const uint8_t *)footer, sizeof(*footer), encryptAlgorithm, encryptKey);
  if (code) return code;
  *fileSize += sizeof(*footer);
  return 0;
}

int32_t tsdbFileWriteTombBlock(STsdbFD *fd, STombBlock *tombBlock, int8_t cmprAlg, int64_t *fileSize,
                               TTombBlkArray *tombBlkArray, SBuffer *buffers, SVersionRange *range,
                                int32_t encryptAlgorithm, char* encryptKey) {
  int32_t code;

  if (TOMB_BLOCK_SIZE(tombBlock) == 0) return 0;

  SBuffer *buffer0 = buffers + 0;
  SBuffer *assist = buffers + 1;

  STombBlk tombBlk = {
      .dp[0] =
          {
              .offset = *fileSize,
              .size = 0,
          },
      .numRec = TOMB_BLOCK_SIZE(tombBlock),
      .cmprAlg = cmprAlg,
  };
  for (int i = 0; i < TOMB_BLOCK_SIZE(tombBlock); i++) {
    STombRecord record;
    tTombBlockGet(tombBlock, i, &record);

    if (i == 0) {
      tombBlk.minTbid.suid = record.suid;
      tombBlk.minTbid.uid = record.uid;
      tombBlk.minVer = record.version;
      tombBlk.maxVer = record.version;
    }
    if (i == TOMB_BLOCK_SIZE(tombBlock) - 1) {
      tombBlk.maxTbid.suid = record.suid;
      tombBlk.maxTbid.uid = record.uid;
    }
    if (record.version < tombBlk.minVer) {
      tombBlk.minVer = record.version;
    }
    if (record.version > tombBlk.maxVer) {
      tombBlk.maxVer = record.version;
    }
  }

  tsdbWriterUpdVerRange(range, tombBlk.minVer, tombBlk.maxVer);

  for (int32_t i = 0; i < ARRAY_SIZE(tombBlock->buffers); i++) {
    tBufferClear(buffer0);

    SCompressInfo cinfo = {
        .cmprAlg = cmprAlg,
        .dataType = TSDB_DATA_TYPE_BIGINT,
        .originalSize = tombBlock->buffers[i].size,
    };
    code = tCompressDataToBuffer(tombBlock->buffers[i].data, &cinfo, buffer0, assist);
    if (code) return code;

    code = tsdbWriteFile(fd, *fileSize, buffer0->data, buffer0->size, encryptAlgorithm, encryptKey);
    if (code) return code;

    tombBlk.size[i] = cinfo.compressedSize;
    tombBlk.dp->size += tombBlk.size[i];
    *fileSize += tombBlk.size[i];
  }

  code = TARRAY2_APPEND_PTR(tombBlkArray, &tombBlk);
  if (code) return code;

  tTombBlockClear(tombBlock);
  return 0;
}

static int32_t tsdbDataFileWriteHeadFooter(SDataFileWriter *writer) {
  int32_t code = 0;
  int32_t lino = 0;

  int32_t encryptAlgorithm = writer->config->tsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
  char* encryptKey = writer->config->tsdb->pVnode->config.tsdbCfg.encryptKey;

  code = tsdbFileWriteHeadFooter(writer->fd[TSDB_FTYPE_HEAD], &writer->files[TSDB_FTYPE_HEAD].size, writer->headFooter,
                                  encryptAlgorithm, encryptKey);
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

  int32_t encryptAlgorithm = writer->config->tsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
  char* encryptKey = writer->config->tsdb->pVnode->config.tsdbCfg.encryptKey;

  code = tsdbFileWriteTombBlock(writer->fd[TSDB_FTYPE_TOMB], writer->tombBlock, writer->config->cmprAlg,
                                &writer->files[TSDB_FTYPE_TOMB].size, writer->tombBlkArray, writer->buffers,
                                &writer->ctx->tombRange, encryptAlgorithm, encryptKey);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbFileWriteTombBlk(STsdbFD *fd, const TTombBlkArray *tombBlkArray, SFDataPtr *ptr, int64_t *fileSize, 
                              int32_t encryptAlgorithm, char* encryptKey) {
  ptr->size = TARRAY2_DATA_LEN(tombBlkArray);
  if (ptr->size > 0) {
    ptr->offset = *fileSize;

    int32_t code = tsdbWriteFile(fd, *fileSize, (const uint8_t *)TARRAY2_DATA(tombBlkArray), ptr->size, 
                                  encryptAlgorithm, encryptKey);
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

  int32_t encryptAlgorithm = writer->config->tsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
  char* encryptKey = writer->config->tsdb->pVnode->config.tsdbCfg.encryptKey;

  code = tsdbFileWriteTombBlk(writer->fd[TSDB_FTYPE_TOMB], writer->tombBlkArray, writer->tombFooter->tombBlkPtr,
                              &writer->files[TSDB_FTYPE_TOMB].size, encryptAlgorithm, encryptKey);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    TSDB_ERROR_LOG(TD_VID(writer->config->tsdb->pVnode), lino, code);
  }
  return code;
}

int32_t tsdbFileWriteTombFooter(STsdbFD *fd, const STombFooter *footer, int64_t *fileSize, int32_t encryptAlgorithm,
                                char* encryptKey) {
  int32_t code = tsdbWriteFile(fd, *fileSize, (const uint8_t *)footer, sizeof(*footer), encryptAlgorithm, encryptKey);
  if (code) return code;
  *fileSize += sizeof(*footer);
  return 0;
}

static int32_t tsdbDataFileWriteTombFooter(SDataFileWriter *writer) {
  int32_t code = 0;
  int32_t lino = 0;

  int32_t encryptAlgorithm = writer->config->tsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
  char* encryptKey = writer->config->tsdb->pVnode->config.tsdbCfg.encryptKey;

  code = tsdbFileWriteTombFooter(writer->fd[TSDB_FTYPE_TOMB], writer->tombFooter, &writer->files[TSDB_FTYPE_TOMB].size,
                                  encryptAlgorithm, encryptKey);
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

int32_t tsdbFileWriteBrinBlk(STsdbFD *fd, TBrinBlkArray *brinBlkArray, SFDataPtr *ptr, int64_t *fileSize, 
                            int32_t encryptAlgorithm, char* encryptKey) {
  ASSERT(TARRAY2_SIZE(brinBlkArray) > 0);
  ptr->offset = *fileSize;
  ptr->size = TARRAY2_DATA_LEN(brinBlkArray);

  int32_t code = tsdbWriteFile(fd, ptr->offset, (uint8_t *)TARRAY2_DATA(brinBlkArray), ptr->size, encryptAlgorithm,
                                encryptKey);
  if (code) return code;

  *fileSize += ptr->size;
  return 0;
}

static int32_t tsdbDataFileWriteBrinBlk(SDataFileWriter *writer) {
  int32_t code = 0;
  int32_t lino = 0;

  int32_t encryptAlgorithm = writer->config->tsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
  char* encryptKey = writer->config->tsdb->pVnode->config.tsdbCfg.encryptKey;

  code = tsdbFileWriteBrinBlk(writer->fd[TSDB_FTYPE_HEAD], writer->brinBlkArray, writer->headFooter->brinBlkPtr,
                              &writer->files[TSDB_FTYPE_HEAD].size, encryptAlgorithm, encryptKey);
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
  int32_t encryptAlgorithm = writer->config->tsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
  char* encryptKey = writer->config->tsdb->pVnode->config.tsdbCfg.encryptKey;
  for (int32_t i = 0; i < TSDB_FTYPE_MAX; ++i) {
    if (writer->fd[i]) {
      code = tsdbFsyncFile(writer->fd[i], encryptAlgorithm, encryptKey);
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

    int32_t lcn = writer->files[ftype].lcn;
    tsdbTFileName(writer->config->tsdb, &writer->files[ftype], fname);
    code = tsdbOpenFile(fname, writer->config->tsdb, flag, &writer->fd[ftype], lcn);
    TSDB_CHECK_CODE(code, lino, _exit);

    if (writer->files[ftype].size == 0) {
      uint8_t hdr[TSDB_FHDR_SIZE] = {0};

      int32_t encryptAlgorithm = writer->config->tsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
      char* encryptKey = writer->config->tsdb->pVnode->config.tsdbCfg.encryptKey;

      code = tsdbWriteFile(writer->fd[ftype], 0, hdr, TSDB_FHDR_SIZE, encryptAlgorithm, encryptKey);
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
    STsdbRowKey key;

    tsdbRowGetKey(&tsdbRowFromBlockData(bData, 0), &key);
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

  int32_t lcn = writer->files[ftype].lcn;
  tsdbTFileName(writer->config->tsdb, writer->files + ftype, fname);
  code = tsdbOpenFile(fname, writer->config->tsdb, flag, &writer->fd[ftype], lcn);
  TSDB_CHECK_CODE(code, lino, _exit);

  uint8_t hdr[TSDB_FHDR_SIZE] = {0};
  int32_t encryptAlgorithm = writer->config->tsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
  char* encryptKey = writer->config->tsdb->pVnode->config.tsdbCfg.encryptKey;
  code = tsdbWriteFile(writer->fd[ftype], 0, hdr, TSDB_FHDR_SIZE, encryptAlgorithm, encryptKey);
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
