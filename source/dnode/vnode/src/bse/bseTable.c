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

#include "bseTable.h"
#include "bseTableMgt.h"

// block handle func
static int32_t blkHandleEncode(SBlkHandle *pHandle, char *buf);
static int32_t blkHandleDecode(SBlkHandle *pHandle, char *buf);

// table footer func
static int32_t footerEncode(STableFooter *pFooter, char *buf);
static int32_t footerDecode(STableFooter *pFooter, char *buf);

// block handle func
static int32_t blkHandleEncode(SBlkHandle *pHandle, char *buf);
static int32_t blkHandleDecode(SBlkHandle *pHandle, char *buf);

// table footer func
static int32_t footerEncode(STableFooter *pFooter, char *buf);
static int32_t footerDecode(STableFooter *pFooter, char *buf);

// block func
static int32_t blockCreate(int32_t cap, SBlock **pBlock);
static void    blockDestroy(SBlock *pBlock);
static int32_t blockPut(SBlock *pBlock, int64_t seq, uint8_t *value, int32_t len);
static int32_t blockAppendBatch(SBlock *p, uint8_t *value, int32_t len);
static int32_t blockEsimateSize(SBlock *pBlock, int32_t extra);
static void    blockClear(SBlock *pBlock);
static int32_t blockSeek(SBlock *p, int64_t seq, uint8_t **pValue, int32_t *len);
static int8_t  blockGetType(SBlock *p);

static int32_t metaBlockAdd(SBlock *p, SBlkHandle *pInfo);

typedef struct {
  int64_t seq;
  int32_t offset;
} blockIndexMeta;

// STable builder func
static int32_t tableBuilderGetBlockSize(STableBuilder *p);
static void    tableBuilderUpdateBlockRange(STableBuilder *p, SBlockItemInfo *pInfo);
static void    tableBuilderResetBlockRange(STableBuilder *p);
static void    tableBuilderResetRange(STableBuilder *p);
static int32_t tableBuilderLoadBlock(STableBuilder *p, SBlkHandle *pHandle, SBlockWrapper *pBlkWrapper);
static int32_t tableBuilderSeekData(STableBuilder *p, SBlkHandle *pHandle, int64_t seq, uint8_t **pValue, int32_t *len);

// STable pReaderMgt func

static int32_t tableReadOpenImpl(STableReader *p);
static int32_t tableReaderLoadBlock(STableReader *p, SBlkHandle *pHandle, SBlockWrapper *pBlkWrapper);
static int32_t tableReaderSeekData(STableReader *p, SBlkHandle *pHandle, int64_t seq, uint8_t **pValue, int32_t *len);
static int32_t tableReaderInitRange(STableReader *p);
static int32_t tableReaderInitMeta(STableReader *p, SBlock *pBlock);

static int32_t tableReaderLoadRawBlock(STableReader *p, SBlkHandle *pHandle, SBlockWrapper *pBlkWrapper);
static int32_t tableReaderLoadRawMeta(STableReader *p, SBlockWrapper *blkWrapper);
static int32_t tableReaderLoadRawFooter(STableReader *p, SBlockWrapper *blkWrapper);

static int32_t tableLoadBlock(TdFilePtr pFile, SBlkHandle *pHandle, SBlockWrapper *pBlk);
static int32_t tableLoadRawBlock(TdFilePtr pFile, SBlkHandle *pHandle, SBlockWrapper *pBlk, int8_t checkSum);

/*---block formate----*/
//---datatype--|---len---|--data---|--rawdatasize---|--compressType---|---checksum---|
#define BLOCK_ROW_SIZE_OFFSET(p)      (sizeof(SBlock) + (p)->len)
#define BLOCK_ROW_SIZE(p)             BLOCK_ROW_SIZE_OFFSET(p)
#define BLOCK_COMPRESS_TYPE_OFFSET(p) (BLOCK_ROW_SIZE_OFFSET(p) + sizeof(int32_t))
#define BLOCK_CHECKSUM_OFFSET(p)      (BLOCK_COMPRESS_TYPE_OFFSET(p) + sizeof(int8_t))
#define BLOCK_TOTAL_SIZE(p)           (BLOCK_CHECKSUM_OFFSET(p) + sizeof(TSCKSUM))

#define BLOCK_SET_ROW_SIZE(p, size) *(int32_t *)((char *)(p) + BLOCK_ROW_SIZE_OFFSET(p)) = (size)
#define BLOCK_GET_ROW_SIZE(p)       *(int32_t *)((char *)(p) + BLOCK_ROW_SIZE_OFFSET(p))

#define BLOCK_SET_COMPRESS_TYPE(p, type) *(int8_t *)((char *)(p) + BLOCK_COMPRESS_TYPE_OFFSET(p)) = (type)
#define BLOCK_GET_COMPRESS_TYPE(p)       *(int8_t *)((char *)(p) + BLOCK_COMPRESS_TYPE_OFFSET(p))

#define BLOCK_TAIL_LEN (sizeof(int32_t) + sizeof(int8_t) + sizeof(TSCKSUM))

#define COMREPSS_DATA_SET_TYPE_AND_RAWLEN(p, len, type, rawLen) \
  do {                                                          \
    *(int32_t *)((char *)(p) + len) = (rawLen);                 \
    *(int8_t *)((char *)(p) + len + sizeof(int32_t)) = (type);  \
  } while (0);
#define COMPRESS_DATA_GET_TYPE_AND_RAWLEN(p, len, type, rawLen)                 \
  do {                                                                          \
    (rawLen) = *(int32_t *)((char *)(p) + len - BLOCK_TAIL_LEN);                \
    (type) = *(int8_t *)((char *)(p) + len - BLOCK_TAIL_LEN + sizeof(int32_t)); \
  } while (0);

int32_t tableBuilderSeekData(STableBuilder *p, SBlkHandle *pHandle, int64_t seq, uint8_t **pValue, int32_t *len) {
  int32_t code = 0;
  int32_t lino = 0;

  SBlockWrapper blockWrapper = {0};

  code = tableBuilderLoadBlock(p, pHandle, &blockWrapper);
  TSDB_CHECK_CODE(code, lino, _error);

  code = blockSeek(blockWrapper.data, seq, pValue, len);
  TSDB_CHECK_CODE(code, lino, _error);

  blockWrapperCleanup(&blockWrapper);
_error:
  if (code != 0) {
    bseError("failed to seek data from table builder since %s at line %d", tstrerror(code), lino);
  }
  return code;
}

int32_t tableBuilderLoadBlock(STableBuilder *p, SBlkHandle *pHandle, SBlockWrapper *pBlkWrapper) {
  int32_t code = 0;
  int32_t lino = 0;
  code = blockWrapperInit(pBlkWrapper, pHandle->size);
  TSDB_CHECK_CODE(code, lino, _error);

  code = tableLoadBlock(p->pDataFile, pHandle, pBlkWrapper);
_error:
  if (code != 0) {
    bseError("failed to load block from table builder since %s at line %d", tstrerror(code), lino);
  }
  return code;
}

int32_t tableBuilderOpen(char *path, STableBuilder **pBuilder, SBse *pBse) {
  int32_t code = 0;
  int32_t lino = 0;

  STableBuilder *p = taosMemoryCalloc(1, sizeof(STableBuilder));
  if (p == NULL) {
    TSDB_CHECK_CODE(terrno, lino, _error);
  }

  memcpy(p->name, path, strlen(path));

  p->pSeqToBlock = taosArrayInit(128, sizeof(SSeqToBlk));
  if (p->pSeqToBlock == NULL) {
    TSDB_CHECK_CODE(terrno, lino, _error);
  }

  p->pMetaHandle = taosArrayInit(128, sizeof(SBlkHandle));
  if (p->pMetaHandle == NULL) {
    TSDB_CHECK_CODE(terrno, lino, _error);
  }

  p->blockCap = BSE_GET_BLOCK_SIZE(pBse);

  code = blockWrapperInit(&p->pBlockWrapper, p->blockCap);
  TSDB_CHECK_CODE(code, lino, _error);

  p->compressType = BSE_GET_COMPRESS_TYPE(pBse);
  TSDB_CHECK_CODE(code, lino, _error);

  tableBuilderResetRange(p);

  tableBuilderResetBlockRange(p);
  *pBuilder = p;

  return code;
_error:
  if (code != 0) {
    (void)tableBuilderClose(p, 0);
    bseError("failed to open table builder since %s at line %d", tstrerror(code), lino);
  }
  return code;
}

// int32_t tableBuildShouldOpenFile(STableBuilder *p) {
//   if (p->pDataFile == NULL) {
//     char name[TSDB_FILENAME_LEN];
//     bseBuildDataFullName(p->pBse, p->tableRange.sseq, name);
//     p->pDataFile = taosOpenFile(p->name, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_READ | TD_FILE_APPEND);
//     if (p->pDataFile == NULL) {
//       return terrno;
//     }
//     bseBuildDataName(NULL, p->tableRange.sseq, p->name);
//   }
//   return 0;
// }
int32_t tableBuilderOpenFile(STableBuilder *p) {
  int32_t code = 0;
  int32_t lino = 0;

  char path[TSDB_FILENAME_LEN] = {0};
  bseBuildDataFullName(p->pBse, p->tableRange.sseq, path);

  bseBuildDataName(p->pBse, p->tableRange.sseq, p->name);

  p->pDataFile = taosOpenFile(path, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_READ | TD_FILE_APPEND);
  if (p->pDataFile == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }

_error:
  if (code != 0) {
    bseError("failed to reinit table builder since %s at line %d", tstrerror(code), lino);
  }
  return code;
}
int32_t tableBuildAddFooter(STableBuilder *p) {
  char buf[kEncodeLen];

  int32_t code = 0;
  int32_t lino = 0;

  code = footerEncode(&p->footer, buf);
  TSDB_CHECK_CODE(code, lino, _error);

  p->offset += sizeof(buf);

  int32_t nwrite = taosWriteFile(p->pDataFile, buf, sizeof(buf));
  if (nwrite != sizeof(buf)) {
    code = terrno;
    TSDB_CHECK_CODE(code, lino, _error);
  }

_error:
  if (code != 0) {
    bseError("failed to add footer to table builder since %s at line %d", tstrerror(code), lino);
  }
  return code;
}

int32_t tableBuilderFlush(STableBuilder *p, int8_t type) {
  int32_t code = 0;
  int32_t lino = 0;

  SBlock *pBlk = p->pBlockWrapper.data;
  if (pBlk->len == 0) {
    return 0;
  }
  int8_t compressType = BSE_GET_COMPRESS_TYPE(p->pBse);

  SBlockWrapper wrapper = {0};

  uint8_t *pWrite = (uint8_t *)pBlk;
  int32_t  len = BLOCK_TOTAL_SIZE(pBlk);

  pBlk->type = type;

  BLOCK_SET_COMPRESS_TYPE(pBlk, compressType);
  BLOCK_SET_ROW_SIZE(pBlk, BLOCK_ROW_SIZE(pBlk));

  if (compressType != kNoCompres) {
    code = blockWrapperInit(&wrapper, len + 4);
    TSDB_CHECK_CODE(code, lino, _error);

    int32_t compressSize = wrapper.cap;
    code = bseCompressData(compressType, pWrite, BLOCK_ROW_SIZE(pBlk), wrapper.data, &compressSize);
    if (code != 0) {
      bseError("failed to compress data since %s at line %d, set no compress", tstrerror(code), lino);
      blockWrapperCleanup(&wrapper);
      BLOCK_SET_COMPRESS_TYPE(pBlk, kNoCompres);
      BLOCK_SET_ROW_SIZE(pBlk, BLOCK_ROW_SIZE(pBlk));
    } else {
      int32_t rawSize = BLOCK_ROW_SIZE(pBlk);
      COMREPSS_DATA_SET_TYPE_AND_RAWLEN(wrapper.data, compressSize, compressType, rawSize);
      len = compressSize + BLOCK_TAIL_LEN;

      pWrite = (uint8_t *)wrapper.data;
    }
  }

  code = taosCalcChecksumAppend(0, (uint8_t *)pWrite, len);
  TSDB_CHECK_CODE(code, lino, _error);

  SBlkHandle handle = {.size = len, .offset = p->offset};
  handle.range = p->blockRange;
  bseDebug("bse block range sseq:%ld, eseq:%ld", p->blockRange.sseq, p->blockRange.eseq);

  (void)taosLSeekFile(p->pDataFile, handle.offset, SEEK_SET);

  int32_t nwrite = taosWriteFile(p->pDataFile, (uint8_t *)pWrite, len);
  if (nwrite != len) {
    code = terrno;
    TSDB_CHECK_CODE(code, lino, _error);
  }
  p->offset += len;
  if (type == BSE_TABLE_DATA_TYPE) {
    taosArrayPush(p->pMetaHandle, &handle);
    tableBuilderResetBlockRange(p);
  } else if (type == BSE_TABLE_META_TYPE) {
    p->footer.metaHandle[0] = handle;
  } else if (type == BSE_TABLE_INDEX_TYPE) {
  } else if (type == BSE_TABLE_FOOTER_TYPE) {
  }

  blockWrapperClear(&p->pBlockWrapper);
  blockWrapperCleanup(&wrapper);
_error:
  if (code != 0) {
    bseError("failed to flush table builder since %s at line %d", tstrerror(code), lino);
  }
  return code;
}

int32_t tableBuildUpdateRange(STableBuilder *p, SBlockItemInfo *pInfo) {
  int32_t    code = 0;
  SSeqRange *pRange = &p->tableRange;
  if (pRange->sseq == -1) {
    pRange->sseq = pInfo->seq;
    code = tableBuilderOpenFile(p);
    if (code != 0) {
      return code;
    }
  }
  pRange->eseq = pInfo->seq;
  return code;
}
void tableBuilderResetRange(STableBuilder *p) {
  p->tableRange.sseq = -1;
  p->tableRange.eseq = -1;
}

void tableBuilderUpdateBlockRange(STableBuilder *p, SBlockItemInfo *pInfo) {
  SSeqRange *pRange = &p->blockRange;
  if (pRange->sseq == -1) {
    pRange->sseq = pInfo->seq;
  }
  pRange->eseq = pInfo->seq;
}

void tableBuilderResetBlockRange(STableBuilder *p) {
  p->blockRange.sseq = -1;
  p->blockRange.eseq = -1;
}

/*|seq len value|seq len value| seq len value| seq len value|*/
int32_t tableBuilderPutBatch(STableBuilder *p, SBseBatch *pBatch) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t len = 0, offset = 0;

  for (int32_t i = 0; i < taosArrayGetSize(pBatch->pSeq);) {
    SBlockItemInfo *pInfo = taosArrayGet(pBatch->pSeq, i);
    if (i == 0 || i == taosArrayGetSize(pBatch->pSeq) - 1) {
      code = tableBuildUpdateRange(p, pInfo);
      TSDB_CHECK_CODE(code, lino, _error);
    }

    if (blockEsimateSize(p->pBlockWrapper.data, len + pInfo->size) <= tableBuilderGetBlockSize(p)) {
      i++;
      len += pInfo->size;
      tableBuilderUpdateBlockRange(p, pInfo);
      continue;
    } else {
      if (len > 0) {
        offset += blockAppendBatch(p->pBlockWrapper.data, pBatch->buf + offset, len);
      }
      code = tableBuilderFlush(p, BSE_TABLE_DATA_TYPE);
      TSDB_CHECK_CODE(code, lino, _error);
      len = 0;
    }
  }
  if (offset < pBatch->len) {
    blockAppendBatch(p->pBlockWrapper.data, pBatch->buf + offset, pBatch->len - offset);
  }
_error:
  if (code != 0) {
    bseError("failed to append batch since %s", tstrerror(code));
  }
  return code;
}

int32_t tableBuilderPut(STableBuilder *p, int64_t *seq, uint8_t *value, int32_t len) {
  int32_t        code = 0;
  int32_t        lino = 0;
  SBlockItemInfo info = {.size = len, .seq = *seq};
  code = tableBuildUpdateRange(p, &info);
  TSDB_CHECK_CODE(code, lino, _error);

  // seqlen + valuelen + value

  int32_t extra = sizeof(*seq) + len + sizeof(len);
  if (blockEsimateSize(p->pBlockWrapper.data, extra) >= tableBuilderGetBlockSize(p)) {
    code = tableBuilderFlush(p, BSE_TABLE_DATA_TYPE);
    TSDB_CHECK_CODE(code, lino, _error);
  }

  code = blockPut(p->pBlockWrapper.data, *seq, value, len);
  TSDB_CHECK_CODE(code, lino, _error);

_error:
  if (code != 0) {
    bseError("failed to put value by seq %" PRId64 " since %s at lino %d", *seq, tstrerror(code), lino);
  }
  return code;
}

int32_t compareFunc(const void *pLeft, const void *pRight) {
  SBlkHandle *p1 = (SBlkHandle *)pLeft;
  SBlkHandle *p2 = (SBlkHandle *)pRight;
  if (p1->range.sseq > p2->range.sseq) {
    return 1;
  } else if (p1->range.sseq < p2->range.sseq) {
    return -1;
  }
  return 0;
}
int32_t findTargetBlock(SArray *pMetaHandle, int64_t seq) {
  SBlkHandle handle = {.range = {.sseq = seq, .eseq = seq}};
  return taosArraySearchIdx(pMetaHandle, &handle, compareFunc, TD_LE);
}
int32_t tableBuilderGet(STableBuilder *p, int64_t seq, uint8_t **value, int32_t *len) {
  if (p == NULL) {
    return TSDB_CODE_NOT_FOUND;
  }
  SBlkHandle *pHandle = NULL;
  if (taosArrayGetSize(p->pMetaHandle) > 0) {
    pHandle = taosArrayGetLast(p->pMetaHandle);
    if (isGreaterSeqRange(&pHandle->range, seq)) {
      return blockSeek(p->pBlockWrapper.data, seq, value, len);
    } else {
      int32_t idx = findTargetBlock(p->pMetaHandle, seq);
      if (idx < 0) {
        return TSDB_CODE_NOT_FOUND;
      }
      pHandle = taosArrayGet(p->pMetaHandle, idx);
      return tableBuilderSeekData(p, pHandle, seq, value, len);
    }
  } else {
    return blockSeek(p->pBlockWrapper.data, seq, value, len);
  }
  return TSDB_CODE_NOT_FOUND;
}

int32_t tableBuildResizeBuf(STableBuilder *p, int32_t size) {
  int32_t code = 0;
  int32_t lino = 0;

  code = blockWrapperResize(&p->pBlockWrapper, size);
  return code;
}
int32_t tableBuildAddMetaBlock(STableBuilder *p) {
  int32_t code = 0;
  int32_t lino = 0;

  int32_t dataEndOffset, metaEndOffset = 0;
  int64_t offset = 0;

  dataEndOffset = p->offset;
  code = tableBuildResizeBuf(p, taosArrayGetSize(p->pMetaHandle) * sizeof(SBlkHandle));
  if (code != 0) {
    return code;
  }

  // kbseInfo("pMeta handle size %d", taosArrayGetSize(p->pMetaHandle));
  for (int32_t i = 0; i < taosArrayGetSize(p->pMetaHandle); i++) {
    SBlkHandle *pHandle = taosArrayGet(p->pMetaHandle, i);
    offset += metaBlockAdd(p->pBlockWrapper.data, pHandle);
  }

  metaEndOffset = p->offset;

  SBlkHandle metaHandle = {.offset = dataEndOffset, .size = offset};
  metaHandle.range = p->tableRange;

  SBlkHandle indexHandle = {.offset = metaEndOffset, .size = 0};
  indexHandle.range = p->tableRange;

  memcpy(p->footer.metaHandle, &metaHandle, sizeof(metaHandle));
  memcpy(p->footer.indexHandle, &metaHandle, sizeof(indexHandle));

_error:
  if (code != 0) {
    bseError("failed to add meta block to table builder since %s at line %d", tstrerror(code), 0);
  }
  return code;
}

int32_t tableBuildGenCommitInfo(STableBuilder *p, SBseLiveFileInfo *pInfo) {
  int32_t code = 0;
  char    name[TSDB_FILENAME_LEN];
  sprintf(pInfo->name, "%s", p->name);

  pInfo->sseq = p->tableRange.sseq;
  pInfo->eseq = p->tableRange.eseq;
  pInfo->size = p->offset;
  pInfo->level = 0;

  return code;
}
int32_t tableBuilderCommit(STableBuilder *p, SBseLiveFileInfo *pInfo) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tableBuilderFlush(p, BSE_TABLE_DATA_TYPE);
  TSDB_CHECK_CODE(code, lino, _error);

  code = tableBuildAddMetaBlock(p);
  TSDB_CHECK_CODE(code, lino, _error);

  code = tableBuilderFlush(p, BSE_TABLE_META_TYPE);
  TSDB_CHECK_CODE(code, lino, _error);

  code = tableBuildAddFooter(p);
  TSDB_CHECK_CODE(code, lino, _error);

  tableBuildGenCommitInfo(p, pInfo);
  tableBuilderClear(p);

  return code;
_error:
  if (code != 0) {
    bseError("failed to commit table builder since %s at line %d", tstrerror(code), lino);
  } else {
    bseInfo("succ to commit table %s", p->name);
  }
  return code;
}

int32_t tableBuilderGetBlockSize(STableBuilder *p) { return p->blockCap; }

int32_t tableBuilderClose(STableBuilder *p, int8_t commited) {
  int32_t code = 0;
  blockWrapperCleanup(&p->pBlockWrapper);
  taosArrayDestroy(p->pSeqToBlock);
  taosCloseFile(&p->pDataFile);
  taosArrayDestroy(p->pMetaHandle);
  taosMemoryFree(p);
  return code;
}
void tableBuilderClear(STableBuilder *p) {
  blockWrapperClear(&p->pBlockWrapper);
  taosCloseFile(&p->pDataFile);
  p->tableRange.sseq = -1;
  p->tableRange.eseq = -1;
  p->offset = 0;
  p->blockId = 0;
  taosArrayClear(p->pSeqToBlock);
  taosArrayClear(p->pMetaHandle);
  memset(&p->footer, 0, sizeof(p->footer));
  p->name[0] = 0;
}

int32_t tableReadLoadFooter(STableReader *p) {
  int32_t code = 0;
  int32_t lino = 0;

  char footer[kEncodeLen];
  if (p->fileSize <= sizeof(footer)) {
    TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _error);
  }

  (void)taosLSeekFile(p->pDataFile, p->fileSize - sizeof(footer), SEEK_SET);
  int32_t nread = taosReadFile(p->pDataFile, footer, sizeof(footer));
  if (nread != sizeof(footer)) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }

  code = footerDecode(&p->footer, footer);
  TSDB_CHECK_CODE(code, lino, _error);

_error:
  if (code != 0) {
    bseError("failed to load table footer since %s at lino", tstrerror(code), lino);
  }
  return code;
}

int32_t tableReaderInitMeta(STableReader *pReader, SBlock *p) {
  int32_t  code = 0;
  int32_t  lino = 0;
  SBlock  *pBlk = p;
  uint8_t *data = (uint8_t *)pBlk->data;
  int32_t  offset = 0;
  do {
    SBlkHandle handle = {0};
    offset += blkHandleDecode(&handle, (char *)data + offset);
    if (taosArrayPush(pReader->pMetaHandle, &handle) == NULL) {
      TSDB_CHECK_CODE(terrno, lino, _error);
    }
  } while (offset < pBlk->len);

_error:
  if (code != 0) {
    bseError("failed to load meta from table pReaderMgt since %s at line %d", tstrerror(code), lino);
  }
  return code;
}

int32_t tableReaderInitRange(STableReader *p) {
  int32_t code = 0;
  if (taosArrayGetSize(p->pMetaHandle) <= 0) {
    return TSDB_CODE_FILE_CORRUPTED;
  }
  SBlkHandle *pHandle = taosArrayGet(p->pMetaHandle, 0);
  p->range.sseq = pHandle->range.sseq;

  pHandle = taosArrayGetLast(p->pMetaHandle);
  p->range.eseq = pHandle->range.eseq;

  return code;
}
int32_t tableReaderLoadMeta(STableReader *p) {
  int32_t code = 0;
  int32_t lino = 0;

  SBlockWrapper wrapper = {0};
  wrapper.type = BSE_TABLE_META_TYPE;
  code = tableReaderLoadBlock(p, p->footer.metaHandle, &wrapper);

  TSDB_CHECK_CODE(code, lino, _error);

  if (blockGetType(wrapper.data) != BSE_TABLE_META_TYPE) {
    TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _error);
  }
  code = tableReaderInitMeta(p, wrapper.data);
  TSDB_CHECK_CODE(code, lino, _error);

  code = tableReaderInitRange(p);
  TSDB_CHECK_CODE(code, lino, _error);

_error:
  blockWrapperCleanup(&wrapper);
  return code;
}
static void addSnapmetaToBlock(SBlockWrapper *pBlkWrapper, SSeqRange range, int8_t fileType, int8_t blockType,
                               int32_t keepDays) {
  SBseSnapMeta *pSnapMeta = pBlkWrapper->data;
  pSnapMeta->range = range;
  pSnapMeta->fileType = fileType;
  pSnapMeta->blockType = blockType;
  pSnapMeta->keepDays = keepDays;
  return;
}

int32_t tableReaderLoadRawBlock(STableReader *p, SBlkHandle *pHandle, SBlockWrapper *blkWrapper) {
  int32_t code = 0;
  int32_t lino = 0;

  code = blockWrapperResize(blkWrapper, pHandle->size + sizeof(SBseSnapMeta));
  TSDB_CHECK_CODE(code, lino, _error);

  code = tableLoadRawBlock(p->pDataFile, pHandle, blkWrapper, 1);
  TSDB_CHECK_CODE(code, lino, _error);

  addSnapmetaToBlock(blkWrapper, p->range, BSE_TABLE_SNAP, BSE_TABLE_DATA_TYPE, 365);

_error:
  if (code != 0) {
    bseError("table reader failed to load block since %s at line %d", tstrerror(code), lino);
  }
  return code;
}

int32_t tableReaderLoadRawMeta(STableReader *p, SBlockWrapper *blkWrapper) {
  int32_t code = 0;
  int32_t lino = 0;

  SBlkHandle *pHandle = p->footer.metaHandle;

  code = blockWrapperResize(blkWrapper, pHandle->size + sizeof(SBseSnapMeta));
  TSDB_CHECK_CODE(code, lino, _error);

  code = tableLoadRawBlock(p->pDataFile, pHandle, blkWrapper, 1);
  TSDB_CHECK_CODE(code, lino, _error);

  addSnapmetaToBlock(blkWrapper, p->range, BSE_TABLE_SNAP, BSE_TABLE_META_TYPE, 365);
_error:
  if (code != 0) {
    bseError("failed to load raw meta from table pReaderMgt since %s at line %d", tstrerror(code), lino);
  }
  return code;
}

int32_t tableReaderLoadRawFooter(STableReader *p, SBlockWrapper *blkWrapper) {
  int32_t code = 0;
  int32_t lino = 0;

  SBlkHandle *pHandle = p->footer.metaHandle;

  int64_t    footerOffset = pHandle->offset + pHandle->size;
  SBlkHandle footerHandle = {.offset = footerOffset, .size = p->fileSize - footerOffset};

  code = blockWrapperResize(blkWrapper, pHandle->size + sizeof(SBseSnapMeta));
  TSDB_CHECK_CODE(code, lino, _error);

  code = tableLoadRawBlock(p->pDataFile, pHandle, blkWrapper, 0);
  TSDB_CHECK_CODE(code, lino, _error);

  addSnapmetaToBlock(blkWrapper, p->range, BSE_TABLE_SNAP, BSE_TABLE_FOOTER_TYPE, 365);
_error:
  if (code != 0) {
    bseError("failed to load raw footer from table pReaderMgt since %s at line %d", tstrerror(code), lino);
  }
  return code;
}

int32_t tableReadOpenImpl(STableReader *p) {
  int32_t code = 0;
  int32_t lino = 0;
  int64_t size = 0;

  char footer[kEncodeLen] = {0};

  code = taosStatFile(p->name, &size, NULL, NULL);
  TSDB_CHECK_CODE(code, lino, _error);

  if (size <= 0) {
    TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _error);
  }
  p->fileSize = size;

  p->pDataFile = taosOpenFile(p->name, TD_FILE_READ);
  if (p->pDataFile == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }

  code = tableReadLoadFooter(p);
  TSDB_CHECK_CODE(code, lino, _error);

  code = tableReaderLoadMeta(p);
  TSDB_CHECK_CODE(code, lino, _error);

_error:
  if (code != 0) {
    bseError("failed to init table pReaderMgt name %s since %s at line %d", p->name, tstrerror(code), lino);
  }
  return code;
}
int32_t tableReaderOpen(char *name, STableReader **pReader, void *pReaderMgt) {
  int32_t code = 0;
  int32_t lino = 0;

  STableReader *p = taosMemCalloc(1, sizeof(STableReader));
  if (p == NULL) {
    TSDB_CHECK_CODE(terrno, lino, _error);
  }

  p->pSeqToBlock = taosArrayInit(128, sizeof(SSeqToBlk));
  if (p->pSeqToBlock == NULL) {
    TSDB_CHECK_CODE(terrno, lino, _error);
  }

  p->pMetaHandle = taosArrayInit(128, sizeof(SBlkHandle));
  if (p->pMetaHandle == NULL) {
    TSDB_CHECK_CODE(terrno, lino, _error);
  }

  p->blockCap = 1024;

  memcpy(p->name, name, strlen(name));
  p->pReaderMgt = pReaderMgt;

  code = tableReadOpenImpl(p);
  TSDB_CHECK_CODE(code, lino, _error);

  p->putInCache = 1;
  *pReader = p;

_error:
  if (code != 0) {
    tableReaderClose(p);
    bseError("failed to open table pReaderMgt file %s since %s at line %d", name, tstrerror(code), lino);
  }
  return code;
}
void    tableReaderShouldPutToCache(STableReader *p, int8_t cache) { p->putInCache = cache; }
int32_t tableReaderGet(STableReader *p, int64_t seq, uint8_t **pValue, int32_t *len) {
  int32_t     code = 0;
  SBlkHandle *pHandle = NULL;
  // opt later
  int32_t idx = findTargetBlock(p->pMetaHandle, seq);
  if (idx < 0) {
    return TSDB_CODE_NOT_FOUND;
  }

  pHandle = taosArrayGet(p->pMetaHandle, idx);
  return tableReaderSeekData(p, pHandle, seq, pValue, len);
}
int32_t tableReaderGetMeta(STableReader *p, SArray **pMeta) {
  int32_t code = 0;
  int32_t lino = 0;

  SArray *pMetaHandle = taosArrayInit(128, sizeof(SBlkHandle));
  if (pMetaHandle == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }

  if (taosArrayAddAll(pMetaHandle, p->pMetaHandle) == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }

  *pMeta = pMetaHandle;

_error:
  if (code != 0) {
    bseError("failed to get table reader meta since %s at line %d", tstrerror(code), lino);
  }
  return code;
}

int32_t tableReaderLoadBlock(STableReader *p, SBlkHandle *pHandle, SBlockWrapper *blkWrapper) {
  int32_t code = 0;
  int32_t lino = 0;

  STableReaderMgt *pRdMgt = p->pReaderMgt;
  SSeqRange        range = pHandle->range;

  if (p->putInCache == 1 && blkWrapper->type == BSE_TABLE_DATA_TYPE) {
    SBlock *pBlock = NULL;
    code = blockCacheGet(pRdMgt->pBlockCache, &range, (void **)&pBlock);
    if (code == TSDB_CODE_NOT_FOUND) {
      code = blockWrapperInit(blkWrapper, pHandle->size);
      TSDB_CHECK_CODE(code, lino, _error);

      code = tableLoadBlock(p->pDataFile, pHandle, blkWrapper);
      TSDB_CHECK_CODE(code, lino, _error);

      pBlock = blkWrapper->data;
      code = blockCachePut(pRdMgt->pBlockCache, &range, pBlock);
      TSDB_CHECK_CODE(code, lino, _error);

    } else if (code == TSDB_CODE_SUCCESS) {
      blkWrapper->data = pBlock;
      blkWrapper->cap = pHandle->size;
    }
  } else {
    code = blockWrapperInit(blkWrapper, pHandle->size);
    TSDB_CHECK_CODE(code, lino, _error);

    code = tableLoadBlock(p->pDataFile, pHandle, blkWrapper);
    TSDB_CHECK_CODE(code, lino, _error);
  }

_error:
  if (code != 0) {
    bseError("table reader failed to load block since %s at line %d", tstrerror(code), lino);
  }
  return code;
}

int32_t tableReaderSeekData(STableReader *p, SBlkHandle *pHandle, int64_t seq, uint8_t **pValue, int32_t *len) {
  int32_t lino = 0;
  int32_t code = 0;

  SBlockWrapper wrapper = {0};
  wrapper.type = BSE_TABLE_DATA_TYPE;

  code = tableReaderLoadBlock(p, pHandle, &wrapper);
  TSDB_CHECK_CODE(code, lino, _error);

  code = blockSeek(wrapper.data, seq, pValue, len);
  TSDB_CHECK_CODE(code, lino, _error);

  wrapper.data = NULL;

_error:
  if (code != 0) {
    bseError("failed to seek data from table reader since %s at line %d", tstrerror(code), lino);
  }
  blockWrapperCleanup(&wrapper);
  return code;
}
int32_t tableReaderClose(STableReader *p) {
  if (p == NULL) return 0;
  int32_t code = 0;

  taosArrayDestroy(p->pMetaHandle);
  taosArrayDestroy(p->pSeqToBlock);

  taosCloseFile(&p->pDataFile);

  taosMemoryFree(p);
  return code;
}

int32_t blockCreate(int32_t cap, SBlock **p) {
  int32_t code = 0;
  SBlock *t = taosMemCalloc(1, cap);
  if (t == NULL) {
    return terrno;
  }
  *p = t;
  return code;
}

int32_t blockEsimateSize(SBlock *p, int32_t extra) {
  // block len + TSCHSUM + len + type;
  return BLOCK_TOTAL_SIZE(p) + extra;
}

int32_t blockAppendBatch(SBlock *p, uint8_t *value, int32_t len) {
  int32_t  code = 0;
  int32_t  offset = 0;
  uint8_t *data = (uint8_t *)p->data + p->len;
  memcpy(data, value, len);
  p->len += len;
  return len;
}
int32_t blockPut(SBlock *p, int64_t seq, uint8_t *value, int32_t len) {
  int32_t  code = 0;
  uint8_t *data = (uint8_t *)p->data + p->len;

  int32_t offset = taosEncodeVariantI64((void **)&data, seq);
  offset += taosEncodeVariantI32((void **)&data, len);
  offset += taosEncodeBinary((void **)&data, value, len);
  p->len += len;
  return offset;
}
void blockClear(SBlock *p) {
  p->len = 0;
  p->type = 0;
  p->data[0] = 0;
}

int32_t blockSeek(SBlock *p, int64_t seq, uint8_t **pValue, int32_t *len) {
  int8_t  found = 0;
  int32_t code = 0;
  int32_t offset = 0;

  uint8_t *p1 = (uint8_t *)p->data;
  uint8_t *p2 = p1;
  while (p2 - p1 < p->len) {
    int64_t k;
    int32_t v;
    p2 = taosDecodeVariantI64(p2, &k);
    p2 = taosDecodeVariantI32(p2, &v);
    if (seq == k) {
      *pValue = taosMemCalloc(1, v);
      memcpy(*pValue, p2, v);
      *len = v;
      found = 1;
      break;
    }

    p2 += v;
  }
  if (found == 0) {
    code = TSDB_CODE_NOT_FOUND;
  }

  return code;
}

int8_t blockGetType(SBlock *p) { return p->type; }
void   blockDestroy(SBlock *pBlock) { taosMemoryFree(pBlock); }

int32_t metaBlockAdd(SBlock *p, SBlkHandle *pInfo) {
  int32_t  code = 0;
  uint8_t *data = (uint8_t *)p->data + p->len;
  int32_t  offset = blkHandleEncode(pInfo, (char *)data);
  p->len += offset;
  return offset;
}

int32_t blkHandleEncode(SBlkHandle *pHandle, char *buf) {
  char   *p = buf;
  int32_t tlen = 0;
  tlen += taosEncodeVariantU64((void **)&p, pHandle->offset);
  tlen += taosEncodeVariantU64((void **)&p, pHandle->size);
  tlen += taosEncodeVariantI64((void **)&p, pHandle->range.sseq);
  tlen += taosEncodeVariantI64((void **)&p, pHandle->range.eseq);
  return tlen;
}
int32_t blkHandleDecode(SBlkHandle *pHandle, char *buf) {
  char *p = buf;
  p = taosDecodeVariantU64(p, &pHandle->offset);
  p = taosDecodeVariantU64(p, &pHandle->size);
  p = taosDecodeVariantI64(p, &pHandle->range.sseq);
  p = taosDecodeVariantI64(p, &pHandle->range.eseq);
  return p - buf;
}

// | meta handle | index handle | padding | magic number high | magic number low |
int32_t footerEncode(STableFooter *pFooter, char *buf) {
  char   *p = buf;
  int32_t len = 0;
  len += blkHandleEncode(pFooter->metaHandle, p + len);
  len += blkHandleEncode(pFooter->indexHandle, p + len);

  p = buf + kEncodeLen - 8;
  taosEncodeFixedU32((void **)&p, kMagicNum);
  taosEncodeFixedU32((void **)&p, kMagicNum);
  return 0;
}
int32_t footerDecode(STableFooter *pFooter, char *buf) {
  int32_t  code = 0;
  char    *p = buf;
  char    *mp = buf + kEncodeLen - 8;
  uint32_t ml, mh;

  taosDecodeFixedU32(mp, &ml);
  taosDecodeFixedU32(mp + 4, &mh);
  if (ml != kMagicNum || mh != kMagicNum) {
    return TSDB_CODE_FILE_CORRUPTED;
  }

  int32_t len = blkHandleDecode(pFooter->metaHandle, buf);
  if (len < 0) {
    return TSDB_CODE_FILE_CORRUPTED;
  }

  len = blkHandleDecode(pFooter->indexHandle, buf + len);
  if (len < 0) {
    return TSDB_CODE_FILE_CORRUPTED;
  }
  return code;
}

int32_t tableLoadBlock(TdFilePtr pFile, SBlkHandle *pHandle, SBlockWrapper *pBlkW) {
  int32_t  code = 0;
  int32_t  lino = 0;
  SBlock  *pBlk = pBlkW->data;
  uint8_t *pRead = (uint8_t *)pBlk;

  SBlockWrapper pHelp = {0};

  (void)taosLSeekFile(pFile, pHandle->offset, SEEK_SET);
  int32_t nr = taosReadFile(pFile, pBlk, pHandle->size);
  if (nr != pHandle->size) {
    TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _error);
  }

  if (taosCheckChecksumWhole((uint8_t *)pRead, pHandle->size) != 1) {
    TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _error);
  }
  uint8_t compressType = 0;
  int32_t rawSize = 0;

  COMPRESS_DATA_GET_TYPE_AND_RAWLEN(pRead, pHandle->size, compressType, rawSize);

  if (compressType != kNoCompres) {
    code = blockWrapperInit(&pHelp, rawSize);
    TSDB_CHECK_CODE(code, lino, _error);

    int32_t unCompressSize = pHelp.cap;
    code = bseDecompressData(compressType, pRead, pHandle->size - BLOCK_TAIL_LEN, pHelp.data, &unCompressSize);
    if (code != 0) {
      TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _error);
    }

    SBlock *p = pHelp.data;
    if (BLOCK_ROW_SIZE_OFFSET(p) != unCompressSize) {
      TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _error);
    }
    blockWrapperCleanup(pBlkW);

    blockWrapperTransfer(pBlkW, &pHelp);

  } else {
    if (pBlk->len != (pHandle->size - BLOCK_TAIL_LEN - sizeof(SBlock))) {
      TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _error);
    }
  }
_error:
  if (code != 0) {
    bseError("failed to load block since %s at line %d", tstrerror(code), lino);
  }
  blockWrapperCleanup(&pHelp);
  return code;
}

int32_t tableLoadRawBlock(TdFilePtr pFile, SBlkHandle *pHandle, SBlockWrapper *pBlkW, int8_t checkSum) {
  int32_t code = 0;
  int32_t lino = 0;

  SBlock  *pBlk = pBlkW->data;
  uint8_t *pRead = (uint8_t *)pBlk + sizeof(SBseSnapMeta);

  (void)taosLSeekFile(pFile, pHandle->offset, SEEK_SET);
  int32_t nr = taosReadFile(pFile, pRead, pHandle->size);
  if (nr != pHandle->size) {
    TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _error);
  }

  if (checkSum) {
    if (taosCheckChecksumWhole((uint8_t *)pRead, pHandle->size) != 1) {
      TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _error);
    }
  }

  pBlkW->size = pHandle->size + sizeof(SBseSnapMeta);
_error:
  if (code != 0) {
    bseError("failed to load block since %s at line %d", tstrerror(code), lino);
  }
  return code;
}

int8_t inSeqRange(SSeqRange *p, int64_t seq) { return seq >= p->sseq && seq <= p->eseq; }

int8_t isGreaterSeqRange(SSeqRange *p, int64_t seq) { return seq > p->eseq; }

int32_t blockWrapperInit(SBlockWrapper *p, int32_t cap) {
  p->data = taosMemoryCalloc(1, cap);
  if (p->data == NULL) {
    return terrno;
  }
  p->cap = cap;
  return 0;
}

void blockWrapperCleanup(SBlockWrapper *p) {
  if (p->data != NULL) {
    taosMemoryFree(p->data);
    p->data = NULL;
  }
  p->cap = 0;
}

void blockWrapperTransfer(SBlockWrapper *dst, SBlockWrapper *src) {
  if (dst == NULL || src == NULL) {
    return;
  }
  dst->data = src->data;
  dst->cap = src->cap;

  src->data = NULL;
  src->cap = 0;
}

int32_t blockWrapperResize(SBlockWrapper *p, int32_t newCap) {
  if (p->cap < newCap) {
    int32_t cap = p->cap;
    while (cap < newCap) {
      cap = cap * 2;
    }
    void *data = taosMemoryRealloc(p->data, cap);
    if (data == NULL) {
      return terrno;
    }
    p->data = data;
    p->cap = cap;
  }
  return 0;
}

void blockWrapperClear(SBlockWrapper *p) {
  SBlock *block = (SBlock *)p->data;
  blockClear(block);
}

int32_t tableReaderIterInit(char *name, STableReaderIter **ppIter, SBse *pBse) {
  int32_t    code = 0;
  int32_t    lino = 0;
  STableMgt *pTableMgt = pBse->pTableMgt;

  STableReaderIter *p = taosMemCalloc(1, sizeof(STableReaderIter));
  if (p == NULL) {
    return terrno;
  }

  code = tableReaderOpen(name, &p->pReader, pTableMgt->pReaderMgt);
  TSDB_CHECK_CODE(code, lino, _error);

  tableReaderShouldPutToCache(p->pReader, 0);

  code = tableReaderGetMeta(p->pReader, &p->pMetaHandle);
  TSDB_CHECK_CODE(code, lino, _error);

  p->blockIndex = 0;
  p->blockType = BSE_TABLE_DATA_TYPE;

  *ppIter = p;

_error:
  if (code != 0) {
    bseError("failed to init table reader iter since %s", tstrerror(code));
    tableReaderIterDestroy(p);
  }
  return code;
}

int32_t tableReaderIterNext(STableReaderIter *pIter, uint8_t **pValue, int32_t *len) {
  int32_t code = 0;
  int32_t lino = 0;

  if (pIter->blockType == BSE_TABLE_DATA_TYPE) {
    SBlkHandle *pHandle = NULL;
    if (pIter->blockIndex >= taosArrayGetSize(pIter->pMetaHandle)) {
      pIter->isOver = 1;
      TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _error);
    }

    pHandle = taosArrayGet(pIter->pMetaHandle, pIter->blockIndex);
    code = tableReaderLoadRawBlock(pIter->pTableReader, pHandle, &pIter->blockWrapper);
    TSDB_CHECK_CODE(code, lino, _error);

    pIter->blockIndex++;
    if (pIter->blockIndex >= taosArrayGetSize(pIter->pMetaHandle)) {
      pIter->blockType = BSE_TABLE_META_TYPE;
      pIter->blockIndex = 0;
    }
  } else if (pIter->blockType == BSE_TABLE_META_TYPE) {
    code = tableReaderLoadRawMeta(pIter->pTableReader, &pIter->blockWrapper);
    TSDB_CHECK_CODE(code, lino, _error);

    pIter->blockType = BSE_TABLE_FOOTER_TYPE;
  } else if (pIter->blockType == BSE_TABLE_FOOTER_TYPE) {
    code = tableReaderLoadRawFooter(pIter->pTableReader, &pIter->blockWrapper);
    TSDB_CHECK_CODE(code, lino, _error);

    pIter->blockType = BSE_TABLE_END_TYPE;
  } else if (pIter->blockType == BSE_TABLE_END_TYPE) {
    pIter->isOver = 1;
    return code;
  }
  *pValue = (uint8_t *)pIter->blockWrapper.data;
  *len = pIter->blockWrapper.size;

_error:
  if (code != 0) {
    bseError("failed to load block since %s", tstrerror(code));
    pIter->isOver = 1;
  }
  return code;
}

int8_t tableReaderIterValid(STableReaderIter *pIter) { return pIter->isOver == 0; }

void tableReaderIterDestroy(STableReaderIter *pIter) {
  if (pIter == NULL) return;

  taosArrayDestroy(pIter->pMetaHandle);
  tableReaderClose(pIter->pReader);
  blockWrapperCleanup(&pIter->blockWrapper);
  taosMemoryFree(pIter);
}

int32_t blockWithMetaInit(SBlock *pBlock, SBlockWithMeta **pMeta) {
  int32_t code = 0;
  int32_t lino = 0;

  SBlockWithMeta *p = taosMemCalloc(1, sizeof(SBlockWithMeta));
  if (p == NULL) {
    return terrno;
  }
  p->pBlock = pBlock;
  p->pMeta = taosArrayInit(8, sizeof(blockIndexMeta));
  if (p->pMeta == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }

  uint8_t *p1 = (uint8_t *)pBlock->data;
  uint8_t *p2 = (uint8_t *)p1;
  while (p2 - p1 < pBlock->len) {
    int64_t        k;
    int32_t        vlen = 0;
    blockIndexMeta meta = {0};
    int32_t        offset = 0;
    p2 = taosDecodeVariantI64((void **)p2, &k);
    offset = p2 - p1;
    p2 = taosDecodeVariantI32((void **)p2, &vlen);

    meta.seq = k;
    meta.offset = offset;
    if (taosArrayPush(p->pMeta, &meta) == NULL) {
      TSDB_CHECK_CODE(code = terrno, lino, _error);
    }
    p2 += vlen;
  }

  *pMeta = p;
_error:
  if (code != 0) {
    bseError("failed to init block with meta since %s", tstrerror(code));
    blockWithMetaCleanup(p);
  }
  return code;
}

int32_t blockWithMetaCleanup(SBlockWithMeta *p) {
  if (p == NULL) return 0;
  taosArrayDestroy(p->pMeta);
  taosMemoryFree(p);
  return 0;
}

int comprareFunc(const void *pLeft, const void *pRight) {
  blockIndexMeta *p1 = (blockIndexMeta *)pLeft;
  blockIndexMeta *p2 = (blockIndexMeta *)pRight;
  if (p1->seq > p2->seq) {
    return 1;
  } else if (p1->seq < p2->seq) {
    return -1;
  }
  return 0;
}

int32_t blockWithMetaSeek(SBlockWithMeta *p, int64_t seq, uint8_t **pValue, int32_t *len) {
  int32_t        code = 0;
  blockIndexMeta key = {.seq = seq, .offset = 0};
  int32_t        idx = taosArraySearchIdx(p->pMeta, &seq, comprareFunc, TD_EQ);
  if (idx < 0) {
    return TSDB_CODE_NOT_FOUND;
  }
  blockIndexMeta *pMeta = taosArrayGet(p->pMeta, idx);
  if (pMeta == NULL) {
    return TSDB_CODE_NOT_FOUND;
  }

  uint8_t *data = (uint8_t *)p->pBlock->data + pMeta->offset;

  data = taosDecodeVariantI32((void *)data, len);
  if (*len <= 0) {
    return TSDB_CODE_NOT_FOUND;
  }
  *pValue = taosMemCalloc(1, *len);
  if (*pValue == NULL) {
    return terrno;
  }
  memcpy(*pValue, data, *len);

  return code;
}