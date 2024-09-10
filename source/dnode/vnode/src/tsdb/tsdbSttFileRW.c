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
#include "meta.h"
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
  SBuffer         local[10];
  SBuffer        *buffers;
};

// SSttFileReader
int32_t tsdbSttFileReaderOpen(const char *fname, const SSttFileReaderConfig *config, SSttFileReader **reader) {
  int32_t code = 0;
  int32_t lino = 0;

  reader[0] = taosMemoryCalloc(1, sizeof(*reader[0]));
  if (reader[0] == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _exit);
  }

  reader[0]->config[0] = config[0];
  reader[0]->buffers = config->buffers;
  if (reader[0]->buffers == NULL) {
    reader[0]->buffers = reader[0]->local;
  }

  // open file
  if (fname) {
    TAOS_CHECK_GOTO(tsdbOpenFile(fname, config->tsdb, TD_FILE_READ, &reader[0]->fd, 0), &lino, _exit);
  } else {
    char fname1[TSDB_FILENAME_LEN];
    tsdbTFileName(config->tsdb, config->file, fname1);
    TAOS_CHECK_GOTO(tsdbOpenFile(fname1, config->tsdb, TD_FILE_READ, &reader[0]->fd, 0), &lino, _exit);
  }

  // // open each segment reader
  int64_t offset = config->file->size - sizeof(SSttFooter);
  if (offset < TSDB_FHDR_SIZE) {
    TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _exit);
  }

  int32_t encryptAlgoirthm = config->tsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
  char   *encryptKey = config->tsdb->pVnode->config.tsdbCfg.encryptKey;
#if 1
  TAOS_CHECK_GOTO(tsdbReadFile(reader[0]->fd, offset, (uint8_t *)(reader[0]->footer), sizeof(SSttFooter), 0,
                               encryptAlgoirthm, encryptKey),
                  &lino, _exit);
#else
  int64_t size = config->file->size;

  for (; size > TSDB_FHDR_SIZE; size--) {
    code = tsdbReadFile(reader[0]->fd, size - sizeof(SSttFooter), (uint8_t *)(reader[0]->footer), sizeof(SSttFooter), 0,
                        encryptAlgoirthm, encryptKey);
    if (code) continue;
    if ((*reader)->footer->sttBlkPtr->offset + (*reader)->footer->sttBlkPtr->size + sizeof(SSttFooter) == size ||
        (*reader)->footer->statisBlkPtr->offset + (*reader)->footer->statisBlkPtr->size + sizeof(SSttFooter) == size ||
        (*reader)->footer->tombBlkPtr->offset + (*reader)->footer->tombBlkPtr->size + sizeof(SSttFooter) == size) {
      break;
    }
  }
  if (size <= TSDB_FHDR_SIZE) {
    TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _exit);
  }
#endif

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(config->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
    (void)tsdbSttFileReaderClose(reader);
  }
  return code;
}

int32_t tsdbSttFileReaderClose(SSttFileReader **reader) {
  if (reader[0]) {
    for (int32_t i = 0; i < ARRAY_SIZE(reader[0]->local); ++i) {
      tBufferDestroy(reader[0]->local + i);
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
      if (reader->footer->statisBlkPtr->size % sizeof(SStatisBlk) != 0) {
        return TSDB_CODE_FILE_CORRUPTED;
      }

      int32_t size = reader->footer->statisBlkPtr->size / sizeof(SStatisBlk);
      void   *data = taosMemoryMalloc(reader->footer->statisBlkPtr->size);
      if (!data) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }

      int32_t encryptAlgorithm = reader->config->tsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
      char   *encryptKey = reader->config->tsdb->pVnode->config.tsdbCfg.encryptKey;
      int32_t code = tsdbReadFile(reader->fd, reader->footer->statisBlkPtr->offset, data,
                                  reader->footer->statisBlkPtr->size, 0, encryptAlgorithm, encryptKey);
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
      if (reader->footer->tombBlkPtr->size % sizeof(STombBlk) != 0) {
        return TSDB_CODE_FILE_CORRUPTED;
      }

      int32_t size = reader->footer->tombBlkPtr->size / sizeof(STombBlk);
      void   *data = taosMemoryMalloc(reader->footer->tombBlkPtr->size);
      if (!data) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }

      int32_t encryptAlgorithm = reader->config->tsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
      char   *encryptKey = reader->config->tsdb->pVnode->config.tsdbCfg.encryptKey;
      int32_t code = tsdbReadFile(reader->fd, reader->footer->tombBlkPtr->offset, data,
                                  reader->footer->tombBlkPtr->size, 0, encryptAlgorithm, encryptKey);
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
      if (reader->footer->sttBlkPtr->size % sizeof(SSttBlk) != 0) {
        return TSDB_CODE_FILE_CORRUPTED;
      }

      int32_t size = reader->footer->sttBlkPtr->size / sizeof(SSttBlk);
      void   *data = taosMemoryMalloc(reader->footer->sttBlkPtr->size);
      if (!data) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }

      int32_t encryptAlgorithm = reader->config->tsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
      char   *encryptKey = reader->config->tsdb->pVnode->config.tsdbCfg.encryptKey;
      int32_t code = tsdbReadFile(reader->fd, reader->footer->sttBlkPtr->offset, data, reader->footer->sttBlkPtr->size,
                                  0, encryptAlgorithm, encryptKey);
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

  SBuffer *buffer0 = reader->buffers + 0;
  SBuffer *assist = reader->buffers + 1;

  int32_t encryptAlgorithm = reader->config->tsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
  char   *encryptKey = reader->config->tsdb->pVnode->config.tsdbCfg.encryptKey;
  // load data
  tBufferClear(buffer0);
  TAOS_CHECK_GOTO(tsdbReadFileToBuffer(reader->fd, sttBlk->bInfo.offset, sttBlk->bInfo.szBlock, buffer0, 0,
                                       encryptAlgorithm, encryptKey),
                  &lino, _exit);

  SBufferReader br = BUFFER_READER_INITIALIZER(0, buffer0);
  TAOS_CHECK_GOTO(tBlockDataDecompress(&br, bData, assist), &lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(reader->config->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
  }
  return code;
}

int32_t tsdbSttFileReadBlockDataByColumn(SSttFileReader *reader, const SSttBlk *sttBlk, SBlockData *bData,
                                         STSchema *pTSchema, int16_t cids[], int32_t ncid) {
  int32_t code = 0;
  int32_t lino = 0;

  SDiskDataHdr hdr;
  SBuffer     *buffer0 = reader->buffers + 0;
  SBuffer     *buffer1 = reader->buffers + 1;
  SBuffer     *assist = reader->buffers + 2;

  int32_t encryptAlgorithm = reader->config->tsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
  char   *encryptKey = reader->config->tsdb->pVnode->config.tsdbCfg.encryptKey;
  // load key part
  tBufferClear(buffer0);
  TAOS_CHECK_GOTO(tsdbReadFileToBuffer(reader->fd, sttBlk->bInfo.offset, sttBlk->bInfo.szKey, buffer0, 0,
                                       encryptAlgorithm, encryptKey),
                  &lino, _exit);

  // decode header
  SBufferReader br = BUFFER_READER_INITIALIZER(0, buffer0);
  TAOS_CHECK_GOTO(tGetDiskDataHdr(&br, &hdr), &lino, _exit);

  if (hdr.delimiter != TSDB_FILE_DLMT) {
    TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _exit);
  }

  // set data container
  tBlockDataReset(bData);
  bData->suid = hdr.suid;
  bData->uid = (sttBlk->suid == 0) ? sttBlk->minUid : 0;
  bData->nRow = hdr.nRow;

  // key part
  TAOS_CHECK_GOTO(tBlockDataDecompressKeyPart(&hdr, &br, bData, assist), &lino, _exit);
  if (br.offset != buffer0->size) {
    TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _exit);
  }

  bool loadExtra = false;
  for (int i = 0; i < ncid; i++) {
    if (tBlockDataGetColData(bData, cids[i]) == NULL) {
      loadExtra = true;
      break;
    }
  }

  if (!loadExtra) {
    goto _exit;
  }

  // load SBlockCol part
  tBufferClear(buffer0);
  TAOS_CHECK_GOTO(tsdbReadFileToBuffer(reader->fd, sttBlk->bInfo.offset + sttBlk->bInfo.szKey, hdr.szBlkCol, buffer0, 0,
                                       encryptAlgorithm, encryptKey),
                  &lino, _exit);

  // load each column
  SBlockCol blockCol = {
      .cid = 0,
  };
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

      TAOS_CHECK_GOTO(tGetBlockCol(&br, &blockCol, hdr.fmtVer, hdr.cmprAlg), &lino, _exit);
    }

    if (cid < blockCol.cid) {
      const STColumn *tcol = tTSchemaSearchColumn(pTSchema, cid);
      TSDB_CHECK_NULL(tcol, code, lino, _exit, TSDB_CODE_TDB_INVALID_TABLE_SCHEMA_VER);
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
      TAOS_CHECK_GOTO(tBlockDataDecompressColData(&hdr, &none, &br, bData, assist), &lino, _exit);
    } else if (cid == blockCol.cid) {
      // load from file
      tBufferClear(buffer1);
      TAOS_CHECK_GOTO(
          tsdbReadFileToBuffer(reader->fd, sttBlk->bInfo.offset + sttBlk->bInfo.szKey + hdr.szBlkCol + blockCol.offset,
                               blockCol.szBitmap + blockCol.szOffset + blockCol.szValue, buffer1, 0, encryptAlgorithm,
                               encryptKey),
          &lino, _exit);

      // decode the buffer
      SBufferReader br1 = BUFFER_READER_INITIALIZER(0, buffer1);
      TAOS_CHECK_GOTO(tBlockDataDecompressColData(&hdr, &blockCol, &br1, bData, assist), &lino, _exit);
    }
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(reader->config->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
  }
  return code;
}

int32_t tsdbSttFileReadTombBlock(SSttFileReader *reader, const STombBlk *tombBlk, STombBlock *tombBlock) {
  int32_t code = 0;
  int32_t lino = 0;

  SBuffer *buffer0 = reader->buffers + 0;
  SBuffer *assist = reader->buffers + 1;

  int32_t encryptAlgorithm = reader->config->tsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
  char   *encryptKey = reader->config->tsdb->pVnode->config.tsdbCfg.encryptKey;
  // load
  tBufferClear(buffer0);
  TAOS_CHECK_GOTO(tsdbReadFileToBuffer(reader->fd, tombBlk->dp->offset, tombBlk->dp->size, buffer0, 0, encryptAlgorithm,
                                       encryptKey),
                  &lino, _exit);

  // decode
  int32_t       size = 0;
  SBufferReader br = BUFFER_READER_INITIALIZER(0, buffer0);
  tTombBlockClear(tombBlock);
  tombBlock->numOfRecords = tombBlk->numRec;
  for (int32_t i = 0; i < ARRAY_SIZE(tombBlock->buffers); ++i) {
    SCompressInfo cinfo = {
        .cmprAlg = tombBlk->cmprAlg,
        .dataType = TSDB_DATA_TYPE_BIGINT,
        .originalSize = tombBlk->numRec * sizeof(int64_t),
        .compressedSize = tombBlk->size[i],
    };
    TAOS_CHECK_GOTO(tDecompressDataToBuffer(BR_PTR(&br), &cinfo, tombBlock->buffers + i, assist), &lino, _exit);
    br.offset += tombBlk->size[i];
  }

  if (br.offset != tombBlk->dp->size) {
    TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(reader->config->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
  }
  return code;
}

int32_t tsdbSttFileReadStatisBlock(SSttFileReader *reader, const SStatisBlk *statisBlk, STbStatisBlock *statisBlock) {
  int32_t code = 0;
  int32_t lino = 0;

  SBuffer *buffer0 = reader->buffers + 0;
  SBuffer *assist = reader->buffers + 1;

  int32_t encryptAlgorithm = reader->config->tsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
  char   *encryptKey = reader->config->tsdb->pVnode->config.tsdbCfg.encryptKey;
  // load data
  tBufferClear(buffer0);
  TAOS_CHECK_GOTO(tsdbReadFileToBuffer(reader->fd, statisBlk->dp->offset, statisBlk->dp->size, buffer0, 0,
                                       encryptAlgorithm, encryptKey),
                  &lino, _exit);

  // decode data
  TAOS_UNUSED(tStatisBlockClear(statisBlock));
  statisBlock->numOfPKs = statisBlk->numOfPKs;
  statisBlock->numOfRecords = statisBlk->numRec;
  SBufferReader br = BUFFER_READER_INITIALIZER(0, buffer0);
  for (int32_t i = 0; i < ARRAY_SIZE(statisBlock->buffers); ++i) {
    SCompressInfo info = {
        .dataType = TSDB_DATA_TYPE_BIGINT,
        .cmprAlg = statisBlk->cmprAlg,
        .compressedSize = statisBlk->size[i],
        .originalSize = statisBlk->numRec * sizeof(int64_t),
    };

    TAOS_CHECK_GOTO(tDecompressDataToBuffer(BR_PTR(&br), &info, &statisBlock->buffers[i], assist), &lino, _exit);
    br.offset += statisBlk->size[i];
  }

  if (statisBlk->numOfPKs > 0) {
    SValueColumnCompressInfo firstKeyInfos[TD_MAX_PK_COLS];
    SValueColumnCompressInfo lastKeyInfos[TD_MAX_PK_COLS];

    // decode compress info
    for (int32_t i = 0; i < statisBlk->numOfPKs; i++) {
      TAOS_CHECK_GOTO(tValueColumnCompressInfoDecode(&br, &firstKeyInfos[i]), &lino, _exit);
    }

    for (int32_t i = 0; i < statisBlk->numOfPKs; i++) {
      TAOS_CHECK_GOTO(tValueColumnCompressInfoDecode(&br, &lastKeyInfos[i]), &lino, _exit);
    }

    // decode value columns
    for (int32_t i = 0; i < statisBlk->numOfPKs; i++) {
      TAOS_CHECK_GOTO(tValueColumnDecompress(BR_PTR(&br), firstKeyInfos + i, &statisBlock->firstKeyPKs[i], assist),
                      &lino, _exit);
      br.offset += (firstKeyInfos[i].dataCompressedSize + firstKeyInfos[i].offsetCompressedSize);
    }

    for (int32_t i = 0; i < statisBlk->numOfPKs; i++) {
      TAOS_CHECK_GOTO(tValueColumnDecompress(BR_PTR(&br), &lastKeyInfos[i], &statisBlock->lastKeyPKs[i], assist), &lino,
                      _exit);
      br.offset += (lastKeyInfos[i].dataCompressedSize + lastKeyInfos[i].offsetCompressedSize);
    }
  }

  if (br.offset != buffer0->size) {
    TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(reader->config->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
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
  SBuffer  local[10];
  SBuffer *buffers;
  // SColCompressInfo2 pInfo;
};

static int32_t tsdbFileDoWriteSttBlockData(STsdbFD *fd, SBlockData *blockData, SColCompressInfo *info,
                                           int64_t *fileSize, TSttBlkArray *sttBlkArray, SBuffer *buffers,
                                           SVersionRange *range, int32_t encryptAlgorithm, char *encryptKey) {
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
  TAOS_CHECK_RETURN(tBlockDataCompress(blockData, info, buffers, buffers + 4));

  sttBlk->bInfo.offset = *fileSize;
  sttBlk->bInfo.szKey = buffers[0].size + buffers[1].size;
  sttBlk->bInfo.szBlock = buffers[2].size + buffers[3].size + sttBlk->bInfo.szKey;
  for (int i = 0; i < 4; i++) {
    if (buffers[i].size) {
      TAOS_CHECK_RETURN(tsdbWriteFile(fd, *fileSize, buffers[i].data, buffers[i].size, encryptAlgorithm, encryptKey));
      *fileSize += buffers[i].size;
    }
  }

  TAOS_CHECK_RETURN(TARRAY2_APPEND_PTR(sttBlkArray, sttBlk));

  tBlockDataClear(blockData);
  return 0;
}

static int32_t tsdbSttFileDoWriteBlockData(SSttFileWriter *writer) {
  if (writer->blockData->nRow == 0) return 0;

  int32_t code = 0;
  int32_t lino = 0;

  tb_uid_t         uid = writer->blockData->suid == 0 ? writer->blockData->uid : writer->blockData->suid;
  SColCompressInfo info = {.defaultCmprAlg = writer->config->cmprAlg, .pColCmpr = NULL};
  code = metaGetColCmpr(writer->config->tsdb->pVnode->pMeta, uid, &(info.pColCmpr));

  int32_t encryptAlgorithm = writer->config->tsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
  char   *encryptKey = writer->config->tsdb->pVnode->config.tsdbCfg.encryptKey;
  TAOS_CHECK_GOTO(
      tsdbFileDoWriteSttBlockData(writer->fd, writer->blockData, &info, &writer->file->size, writer->sttBlkArray,
                                  writer->buffers, &writer->ctx->range, encryptAlgorithm, encryptKey),
      &lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(writer->config->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
  }
  taosHashCleanup(info.pColCmpr);
  return code;
}

static int32_t tsdbSttFileDoWriteStatisBlock(SSttFileWriter *writer) {
  if (writer->staticBlock->numOfRecords == 0) {
    return 0;
  }

  int32_t code = 0;
  int32_t lino = 0;

  SBuffer *buffer0 = writer->buffers + 0;
  SBuffer *buffer1 = writer->buffers + 1;
  SBuffer *assist = writer->buffers + 2;

  STbStatisRecord record;
  STbStatisBlock *statisBlock = writer->staticBlock;
  SStatisBlk      statisBlk = {0};

  statisBlk.dp->offset = writer->file->size;
  statisBlk.dp->size = 0;
  statisBlk.numRec = statisBlock->numOfRecords;
  statisBlk.cmprAlg = writer->config->cmprAlg;
  statisBlk.numOfPKs = statisBlock->numOfPKs;

  (void)tStatisBlockGet(statisBlock, 0, &record);
  statisBlk.minTbid.suid = record.suid;
  statisBlk.minTbid.uid = record.uid;

  (void)tStatisBlockGet(statisBlock, statisBlock->numOfRecords - 1, &record);
  statisBlk.maxTbid.suid = record.suid;
  statisBlk.maxTbid.uid = record.uid;

  int32_t encryptAlgorithm = writer->config->tsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
  char   *encryptKey = writer->config->tsdb->pVnode->config.tsdbCfg.encryptKey;

  // compress each column
  for (int32_t i = 0; i < ARRAY_SIZE(statisBlk.size); i++) {
    SCompressInfo info = {
        .dataType = TSDB_DATA_TYPE_BIGINT,
        .cmprAlg = statisBlk.cmprAlg,
        .originalSize = statisBlock->buffers[i].size,
    };

    tBufferClear(buffer0);
    TAOS_CHECK_GOTO(tCompressDataToBuffer(statisBlock->buffers[i].data, &info, buffer0, assist), &lino, _exit);
    TAOS_CHECK_GOTO(
        tsdbWriteFile(writer->fd, writer->file->size, buffer0->data, info.compressedSize, encryptAlgorithm, encryptKey),
        &lino, _exit);

    statisBlk.size[i] = info.compressedSize;
    statisBlk.dp->size += info.compressedSize;
    writer->file->size += info.compressedSize;
  }

  // compress primary keys
  if (statisBlk.numOfPKs > 0) {
    SValueColumnCompressInfo compressInfo = {.cmprAlg = statisBlk.cmprAlg};

    tBufferClear(buffer0);
    tBufferClear(buffer1);

    for (int32_t i = 0; i < statisBlk.numOfPKs; i++) {
      TAOS_CHECK_GOTO(tValueColumnCompress(&statisBlock->firstKeyPKs[i], &compressInfo, buffer1, assist), &lino, _exit);
      TAOS_CHECK_GOTO(tValueColumnCompressInfoEncode(&compressInfo, buffer0), &lino, _exit);
    }

    for (int32_t i = 0; i < statisBlk.numOfPKs; i++) {
      TAOS_CHECK_GOTO(tValueColumnCompress(&statisBlock->lastKeyPKs[i], &compressInfo, buffer1, assist), &lino, _exit);
      TAOS_CHECK_GOTO(tValueColumnCompressInfoEncode(&compressInfo, buffer0), &lino, _exit);
    }

    TAOS_CHECK_GOTO(
        tsdbWriteFile(writer->fd, writer->file->size, buffer0->data, buffer0->size, encryptAlgorithm, encryptKey),
        &lino, _exit);
    writer->file->size += buffer0->size;
    statisBlk.dp->size += buffer0->size;

    TAOS_CHECK_GOTO(
        tsdbWriteFile(writer->fd, writer->file->size, buffer1->data, buffer1->size, encryptAlgorithm, encryptKey),
        &lino, _exit);
    writer->file->size += buffer1->size;
    statisBlk.dp->size += buffer1->size;
  }

  TAOS_CHECK_GOTO(TARRAY2_APPEND_PTR(writer->statisBlkArray, &statisBlk), &lino, _exit);

  TAOS_UNUSED(tStatisBlockClear(writer->staticBlock));

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(writer->config->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
  }
  return code;
}

static int32_t tsdbSttFileDoWriteTombBlock(SSttFileWriter *writer) {
  if (TOMB_BLOCK_SIZE(writer->tombBlock) == 0) {
    return 0;
  }

  int32_t code = 0;
  int32_t lino = 0;

  int32_t encryptAlgorithm = writer->config->tsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
  char   *encryptKey = writer->config->tsdb->pVnode->config.tsdbCfg.encryptKey;

  TAOS_CHECK_GOTO(
      tsdbFileWriteTombBlock(writer->fd, writer->tombBlock, writer->config->cmprAlg, &writer->file->size,
                             writer->tombBlkArray, writer->buffers, &writer->ctx->range, encryptAlgorithm, encryptKey),
      &lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(writer->config->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
  }
  return code;
}

int32_t tsdbFileWriteSttBlk(STsdbFD *fd, const TSttBlkArray *sttBlkArray, SFDataPtr *ptr, int64_t *fileSize,
                            int32_t encryptAlgorithm, char *encryptKey) {
  ptr->size = TARRAY2_DATA_LEN(sttBlkArray);
  if (ptr->size > 0) {
    ptr->offset = *fileSize;

    TAOS_CHECK_RETURN(tsdbWriteFile(fd, *fileSize, (const uint8_t *)TARRAY2_DATA(sttBlkArray), ptr->size,
                                    encryptAlgorithm, encryptKey));

    *fileSize += ptr->size;
  }
  return 0;
}

static int32_t tsdbSttFileDoWriteSttBlk(SSttFileWriter *writer) {
  int32_t code = 0;
  int32_t lino;

  int32_t encryptAlgorithm = writer->config->tsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
  char   *encryptKey = writer->config->tsdb->pVnode->config.tsdbCfg.encryptKey;

  TAOS_CHECK_GOTO(tsdbFileWriteSttBlk(writer->fd, writer->sttBlkArray, writer->footer->sttBlkPtr, &writer->file->size,
                                      encryptAlgorithm, encryptKey),
                  &lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(writer->config->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
  }
  return code;
}

static int32_t tsdbSttFileDoWriteStatisBlk(SSttFileWriter *writer) {
  int32_t code = 0;
  int32_t lino;
  int32_t encryptAlgorithm = writer->config->tsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
  char   *encryptKey = writer->config->tsdb->pVnode->config.tsdbCfg.encryptKey;

  writer->footer->statisBlkPtr->size = TARRAY2_DATA_LEN(writer->statisBlkArray);
  if (writer->footer->statisBlkPtr->size) {
    writer->footer->statisBlkPtr->offset = writer->file->size;
    TAOS_CHECK_GOTO(tsdbWriteFile(writer->fd, writer->file->size, (const uint8_t *)TARRAY2_DATA(writer->statisBlkArray),
                                  writer->footer->statisBlkPtr->size, encryptAlgorithm, encryptKey),
                    &lino, _exit);
    writer->file->size += writer->footer->statisBlkPtr->size;
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(writer->config->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
  }
  return code;
}

static int32_t tsdbSttFileDoWriteTombBlk(SSttFileWriter *writer) {
  int32_t code = 0;
  int32_t lino = 0;

  int32_t encryptAlgorithm = writer->config->tsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
  char   *encryptKey = writer->config->tsdb->pVnode->config.tsdbCfg.encryptKey;

  TAOS_CHECK_GOTO(tsdbFileWriteTombBlk(writer->fd, writer->tombBlkArray, writer->footer->tombBlkPtr,
                                       &writer->file->size, encryptAlgorithm, encryptKey),
                  &lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(writer->config->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
  }
  return code;
}

int32_t tsdbFileWriteSttFooter(STsdbFD *fd, const SSttFooter *footer, int64_t *fileSize, int32_t encryptAlgorithm,
                               char *encryptKey) {
  TAOS_CHECK_RETURN(
      tsdbWriteFile(fd, *fileSize, (const uint8_t *)footer, sizeof(*footer), encryptAlgorithm, encryptKey));
  *fileSize += sizeof(*footer);
  return 0;
}

static int32_t tsdbSttFileDoWriteFooter(SSttFileWriter *writer) {
  int32_t encryptAlgorithm = writer->config->tsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
  char   *encryptKey = writer->config->tsdb->pVnode->config.tsdbCfg.encryptKey;

  return tsdbFileWriteSttFooter(writer->fd, writer->footer, &writer->file->size, encryptAlgorithm, encryptKey);
}

static int32_t tsdbSttFWriterDoOpen(SSttFileWriter *writer) {
  int32_t code = 0;
  int32_t lino = 0;

  // set
  if (!writer->config->skmTb) writer->config->skmTb = writer->skmTb;
  if (!writer->config->skmRow) writer->config->skmRow = writer->skmRow;
  writer->buffers = writer->config->buffers;
  if (writer->buffers == NULL) {
    writer->buffers = writer->local;
  }

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
  TAOS_CHECK_GOTO(tsdbOpenFile(fname, writer->config->tsdb, flag, &writer->fd, 0), &lino, _exit);

  uint8_t hdr[TSDB_FHDR_SIZE] = {0};
  int32_t encryptAlgorithm = writer->config->tsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
  char   *encryptKey = writer->config->tsdb->pVnode->config.tsdbCfg.encryptKey;

  TAOS_CHECK_GOTO(tsdbWriteFile(writer->fd, 0, hdr, sizeof(hdr), encryptAlgorithm, encryptKey), &lino, _exit);
  writer->file->size += sizeof(hdr);

  // range
  writer->ctx->range = (SVersionRange){.minVer = VERSION_MAX, .maxVer = VERSION_MIN};

  writer->ctx->opened = true;

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(writer->config->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
  }
  return code;
}

static void tsdbSttFWriterDoClose(SSttFileWriter *writer) {
  for (int32_t i = 0; i < ARRAY_SIZE(writer->local); ++i) {
    tBufferDestroy(writer->local + i);
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

  TAOS_CHECK_GOTO(tsdbSttFileDoWriteBlockData(writer), &lino, _exit);
  TAOS_CHECK_GOTO(tsdbSttFileDoWriteStatisBlock(writer), &lino, _exit);
  TAOS_CHECK_GOTO(tsdbSttFileDoWriteTombBlock(writer), &lino, _exit);
  TAOS_CHECK_GOTO(tsdbSttFileDoWriteSttBlk(writer), &lino, _exit);
  TAOS_CHECK_GOTO(tsdbSttFileDoWriteStatisBlk(writer), &lino, _exit);
  TAOS_CHECK_GOTO(tsdbSttFileDoWriteTombBlk(writer), &lino, _exit);
  TAOS_CHECK_GOTO(tsdbSttFileDoWriteFooter(writer), &lino, _exit);
  TAOS_CHECK_GOTO(tsdbSttFileDoUpdateHeader(writer), &lino, _exit);

  int32_t encryptAlgorithm = writer->config->tsdb->pVnode->config.tsdbCfg.encryptAlgorithm;
  char   *encryptKey = writer->config->tsdb->pVnode->config.tsdbCfg.encryptKey;

  TAOS_CHECK_GOTO(tsdbFsyncFile(writer->fd, encryptAlgorithm, encryptKey), &lino, _exit);

  tsdbCloseFile(&writer->fd);

  STFileOp op = (STFileOp){
      .optype = TSDB_FOP_CREATE,
      .fid = writer->config->fid,
      .nf = writer->file[0],
  };
  tsdbTFileUpdVerRange(&op.nf, writer->ctx->range);

  TAOS_CHECK_GOTO(TARRAY2_APPEND(opArray, op), &lino, _exit);

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(writer->config->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
  }
  return code;
}

static int32_t tsdbSttFWriterCloseAbort(SSttFileWriter *writer) {
  char fname[TSDB_FILENAME_LEN];
  tsdbTFileName(writer->config->tsdb, writer->file, fname);
  tsdbCloseFile(&writer->fd);
  tsdbRemoveFile(fname);
  return 0;
}

int32_t tsdbSttFileWriterOpen(const SSttFileWriterConfig *config, SSttFileWriter **writer) {
  writer[0] = taosMemoryCalloc(1, sizeof(*writer[0]));
  if (writer[0] == NULL) {
    return terrno;
  }

  writer[0]->config[0] = config[0];
  writer[0]->ctx->opened = false;
  return 0;
}

int32_t tsdbSttFileWriterClose(SSttFileWriter **writer, int8_t abort, TFileOpArray *opArray) {
  int32_t code = 0;
  int32_t lino = 0;

  if (writer[0]->ctx->opened) {
    if (abort) {
      TAOS_CHECK_GOTO(tsdbSttFWriterCloseAbort(writer[0]), &lino, _exit);
    } else {
      TAOS_CHECK_GOTO(tsdbSttFWriterCloseCommit(writer[0], opArray), &lino, _exit);
    }
    tsdbSttFWriterDoClose(writer[0]);
  }
  taosMemoryFree(writer[0]);
  writer[0] = NULL;

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(writer[0]->config->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
  }
  return code;
}

int32_t tsdbSttFileWriteRow(SSttFileWriter *writer, SRowInfo *row) {
  int32_t code = 0;
  int32_t lino = 0;

  if (!writer->ctx->opened) {
    TAOS_CHECK_GOTO(tsdbSttFWriterDoOpen(writer), &lino, _exit);
  }

  if (!TABLE_SAME_SCHEMA(row->suid, row->uid, writer->ctx->tbid->suid, writer->ctx->tbid->uid)) {
    TAOS_CHECK_GOTO(tsdbSttFileDoWriteBlockData(writer), &lino, _exit);

    TAOS_CHECK_GOTO(tsdbUpdateSkmTb(writer->config->tsdb, (TABLEID *)row, writer->config->skmTb), &lino, _exit);

    TABLEID id = {
        .suid = row->suid,
        .uid = row->suid ? 0 : row->uid,
    };
    TAOS_CHECK_GOTO(tBlockDataInit(writer->blockData, &id, writer->config->skmTb->pTSchema, NULL, 0), &lino, _exit);
  }

  if (writer->ctx->tbid->uid != row->uid) {
    writer->ctx->tbid->suid = row->suid;
    writer->ctx->tbid->uid = row->uid;
  }

  STsdbRowKey key;
  tsdbRowGetKey(&row->row, &key);

  for (;;) {
    code = tStatisBlockPut(writer->staticBlock, row, writer->config->maxRow);
    if (code == TSDB_CODE_INVALID_PARA) {
      TAOS_CHECK_GOTO(tsdbSttFileDoWriteStatisBlock(writer), &lino, _exit);
      continue;
    } else {
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    break;
  }

  if (row->row.type == TSDBROW_ROW_FMT) {
    TAOS_CHECK_GOTO(tsdbUpdateSkmRow(writer->config->tsdb, writer->ctx->tbid,  //
                                     TSDBROW_SVERSION(&row->row), writer->config->skmRow),
                    &lino, _exit);
  }

  // row to col conversion
  if (key.version <= writer->config->compactVersion                                //
      && writer->blockData->nRow > 0                                               //
      && (writer->blockData->uid                                                   //
              ? writer->blockData->uid                                             //
              : writer->blockData->aUid[writer->blockData->nRow - 1]) == row->uid  //
      && tsdbRowCompareWithoutVersion(&row->row,
                                      &tsdbRowFromBlockData(writer->blockData, writer->blockData->nRow - 1)) == 0  //
  ) {
    TAOS_CHECK_GOTO(tBlockDataUpdateRow(writer->blockData, &row->row, writer->config->skmRow->pTSchema), &lino, _exit);
  } else {
    if (writer->blockData->nRow >= writer->config->maxRow) {
      TAOS_CHECK_GOTO(tsdbSttFileDoWriteBlockData(writer), &lino, _exit);
    }

    TAOS_CHECK_GOTO(tBlockDataAppendRow(writer->blockData, &row->row, writer->config->skmRow->pTSchema, row->uid),
                    &lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(writer->config->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
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

    TAOS_CHECK_GOTO(tsdbSttFileWriteRow(writer, row), &lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(writer->config->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
  }
  return code;
}

int32_t tsdbSttFileWriteTombRecord(SSttFileWriter *writer, const STombRecord *record) {
  int32_t code;
  int32_t lino;

  if (!writer->ctx->opened) {
    TAOS_CHECK_GOTO(tsdbSttFWriterDoOpen(writer), &lino, _exit);
  } else {
    if (writer->blockData->nRow > 0) {
      TAOS_CHECK_GOTO(tsdbSttFileDoWriteBlockData(writer), &lino, _exit);
    }

    if (STATIS_BLOCK_SIZE(writer->staticBlock) > 0) {
      TAOS_CHECK_GOTO(tsdbSttFileDoWriteStatisBlock(writer), &lino, _exit);
    }
  }

  TAOS_CHECK_GOTO(tTombBlockPut(writer->tombBlock, record), &lino, _exit);

  if (TOMB_BLOCK_SIZE(writer->tombBlock) >= writer->config->maxRow) {
    TAOS_CHECK_GOTO(tsdbSttFileDoWriteTombBlock(writer), &lino, _exit);
  }

_exit:
  if (code) {
    tsdbError("vgId:%d %s failed at %s:%d since %s", TD_VID(writer->config->tsdb->pVnode), __func__, __FILE__, lino,
              tstrerror(code));
  } else {
    tsdbTrace("vgId:%d write tomb record to stt file:%s, cid:%" PRId64 ", suid:%" PRId64 ", uid:%" PRId64
              ", version:%" PRId64,
              TD_VID(writer->config->tsdb->pVnode), writer->fd->path, writer->config->cid, record->suid, record->uid,
              record->version);
  }
  return code;
}

bool tsdbSttFileWriterIsOpened(SSttFileWriter *writer) { return writer->ctx->opened; }