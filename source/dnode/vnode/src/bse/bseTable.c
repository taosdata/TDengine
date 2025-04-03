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
static int32_t blockClear(SBlock *pBlock);
static int32_t blockSeek(SBlock *p, int64_t seq, uint8_t **pValue, int32_t *len);
static int8_t  blockGetType(SBlock *p);

static int32_t blockWrapperInit(SBlockWrapper *p, int32_t cap);
static void    blockWrapperCleanup(SBlockWrapper *p);
static int32_t blockWrapperResize(SBlockWrapper *p, int32_t cap);
static int32_t blockWrapperClear(SBlockWrapper *p);

static int32_t metaBlockAdd(SBlock *p, SBlkHandle *pInfo);

// STable builder func
static FORCE_INLINE int32_t tableBuildGetBlockSize(STableBuilder *p);

static void    tableBuildUpdateBlockRange(STableBuilder *p, SBlockItemInfo *pInfo);
static void    tableBuildResetBlockRange(STableBuilder *p);
static void    tableBuildResetRange(STableBuilder *p);
static int32_t tableBuildLoadBlock(STableBuilder *p, SBlkHandle *pHandle);
static int32_t tabldBuildSeekData(STableBuilder *p, SBlkHandle *pHandle, int64_t seq, uint8_t **pValue, int32_t *len);
// STable pReaderMgt func
static int32_t tableReadLoadBlock(STableReader *p, SBlkHandle *pHandle);
static int32_t tableReadSeekData(STableReader *p, SBlkHandle *pHandle, int64_t seq, uint8_t **pValue, int32_t *len);
static int32_t tableReadMayResizeLoadBuf(STableReader *p, int32_t size);

static int32_t tableLoadBlock(TdFilePtr pFile, SBlkHandle *pHandle, SBlock *pBlk);

static int32_t tabldBuildSeekData(STableBuilder *p, SBlkHandle *pHandle, int64_t seq, uint8_t **pValue, int32_t *len) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tableBuildLoadBlock(p, pHandle);
  TSDB_CHECK_CODE(code, lino, _error);

  code = blockSeek(p->pHBlockWrapper.data, seq, pValue, len);
  TSDB_CHECK_CODE(code, lino, _error);
_error:
  if (code != 0) {
    bseError("failed to seek data from table builder since %s at line %d", tstrerror(code), lino);
  }
  return code;
}
static int32_t tableBuildResizeLoadBuf(STableBuilder *p, int32_t size) {
  return blockWrapperResize(&p->pHBlockWrapper, size);
}
static int32_t tableBuildResizeAllBuf(STableBuilder *p, int32_t size) {
  int32_t code = 0;

  code = blockWrapperResize(&p->pBlockWrapper, size);
  if (code != 0) {
    return code;
  }

  code = blockWrapperResize(&p->pHBlockWrapper, size);
  return code;
}

int32_t tableBuildLoadBlock(STableBuilder *p, SBlkHandle *pHandle) {
  int32_t code = 0;
  code = tableBuildResizeLoadBuf(p, pHandle->size);
  if (code != 0) {
    return code;
  }
  code = tableLoadBlock(p->pDataFile, pHandle, p->pHBlockWrapper.data);
  if (code != 0) {
    return code;
  }

  return code;
}

int32_t tableBuildOpen(char *path, STableBuilder **pBuilder) {
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

  p->blockCap = 1024;

  code = blockWrapperInit(&p->pBlockWrapper, p->blockCap);
  TSDB_CHECK_CODE(code, lino, _error);

  code = blockWrapperInit(&p->pHBlockWrapper, p->blockCap);
  TSDB_CHECK_CODE(code, lino, _error);

  p->compressType = 0;
  TSDB_CHECK_CODE(code, lino, _error);

  tableBuildResetRange(p);

  tableBuildResetBlockRange(p);
  *pBuilder = p;

  return code;
_error:
  if (code != 0) {
    (void)tableBuildClose(p, 0);
    bseError("failed to open table builder since %s at line %d", tstrerror(code), lino);
  }
  return code;
}

int32_t tableBuildShouldOpenFile(STableBuilder *p) {
  if (p->pDataFile == NULL) {
    char name[TSDB_FILENAME_LEN];
    bseBuildDataFullName((SBse *)p->bse, p->tableRange.sseq, name);
    p->pDataFile = taosOpenFile(p->name, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_READ | TD_FILE_APPEND);
    if (p->pDataFile == NULL) {
      return terrno;
    }
    bseBuildDataName(NULL, p->tableRange.sseq, p->name);
  }
  return 0;
}
int32_t tableBuildOpenFile(STableBuilder *p) {
  int32_t code = 0;
  int32_t lino = 0;

  char path[TSDB_FILENAME_LEN] = {0};
  bseBuildDataFullName(p->bse, p->tableRange.sseq, path);

  bseBuildDataName(p->bse, p->tableRange.sseq, p->name);

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

int32_t tableBuildFlush(STableBuilder *p, int8_t type) {
  int32_t code = 0;
  int32_t lino = 0;

  SBlock *pBlk = p->pBlockWrapper.data;
  if (pBlk->len == 0) {
    return 0;
  }

  int32_t len = sizeof(SBlock) + pBlk->len + sizeof(int8_t) + sizeof(TSCKSUM);

  pBlk->type = type;
  uint8_t *cmprType = (uint8_t *)pBlk->data + pBlk->len;
  memcpy(cmprType, &p->compressType, sizeof(p->compressType));
  // do compress

  code = taosCalcChecksumAppend(0, (uint8_t *)pBlk, len);
  TSDB_CHECK_CODE(code, lino, _error);

  SBlkHandle handle = {.size = len, .offset = p->offset};
  handle.range = p->blockRange;
  bseDebug("bse block range sseq:%ld, eseq:%ld", p->blockRange.sseq, p->blockRange.eseq);

  (void)taosLSeekFile(p->pDataFile, handle.offset, SEEK_SET);

  int32_t nwrite = taosWriteFile(p->pDataFile, (uint8_t *)pBlk, len);
  if (nwrite != len) {
    code = terrno;
    TSDB_CHECK_CODE(code, lino, _error);
  }
  p->offset += len;
  if (type == BSE_TABLE_DATA_TYPE) {
    taosArrayPush(p->pMetaHandle, &handle);
    tableBuildResetBlockRange(p);
  } else if (type == BSE_TABLE_META_TYPE) {
    p->footer.metaHandle[0] = handle;
    // taosArrayPush(p->pMetaHandle, &handle);
  } else if (type == BSE_TABLE_INDEX_TYPE) {
    // taosArrayPush(p->pMetaHandle, &handle);
  } else if (type == BSE_TABLE_FOOTER_TYPE) {
    // p->footer.indexHandle[0] = handle;
  }

  blockWrapperClear(&p->pBlockWrapper);
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
    code = tableBuildOpenFile(p);
    if (code != 0) {
      return code;
    }
  }
  pRange->eseq = pInfo->seq;
  return code;
}
static void tableBuildResetRange(STableBuilder *p) {
  p->tableRange.sseq = -1;
  p->tableRange.eseq = -1;
}

static void tableBuildUpdateBlockRange(STableBuilder *p, SBlockItemInfo *pInfo) {
  SSeqRange *pRange = &p->blockRange;
  if (pRange->sseq == -1) {
    pRange->sseq = pInfo->seq;
  }
  pRange->eseq = pInfo->seq;
}
static void tableBuildResetBlockRange(STableBuilder *p) {
  p->blockRange.sseq = -1;
  p->blockRange.eseq = -1;
}

/*|seq len value|seq len value| seq len value| seq len value|*/
int32_t tableBuildPutBatch(STableBuilder *p, SBseBatch *pBatch) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t len = 0, offset = 0;

  for (int32_t i = 0; i < taosArrayGetSize(pBatch->pSeq);) {
    SBlockItemInfo *pInfo = taosArrayGet(pBatch->pSeq, i);
    if (i == 0 || i == taosArrayGetSize(pBatch->pSeq) - 1) {
      code = tableBuildUpdateRange(p, pInfo);
      TSDB_CHECK_CODE(code, lino, _error);
    }

    if (blockEsimateSize(p->pBlockWrapper.data, len + pInfo->size) <= tableBuildGetBlockSize(p)) {
      i++;
      len += pInfo->size;
      tableBuildUpdateBlockRange(p, pInfo);
      continue;
    } else {
      if (len > 0) {
        offset += blockAppendBatch(p->pBlockWrapper.data, pBatch->buf + offset, len);
      }
      code = tableBuildFlush(p, BSE_TABLE_DATA_TYPE);
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
int32_t tableBuildPut(STableBuilder *p, int64_t *seq, uint8_t *value, int32_t len) {
  int32_t        code = 0;
  int32_t        lino = 0;
  SBlockItemInfo info = {.size = len, .seq = *seq};
  code = tableBuildUpdateRange(p, &info);
  TSDB_CHECK_CODE(code, lino, _error);

  // seqlen + valuelen + value

  int32_t extra = sizeof(*seq) + len + sizeof(len);
  if (blockEsimateSize(p->pBlockWrapper.data, extra) >= tableBuildGetBlockSize(p)) {
    code = tableBuildFlush(p, BSE_TABLE_DATA_TYPE);
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
static int32_t findHandleBySeq(SArray *pMetaHandle, int64_t seq) {
  SBlkHandle handle = {.range = {.sseq = seq, .eseq = seq}};
  return taosArraySearchIdx(pMetaHandle, &handle, compareFunc, TD_LE);
}
int32_t tableBuildGet(STableBuilder *p, int64_t seq, uint8_t **value, int32_t *len) {
  if (p == NULL) {
    return TSDB_CODE_NOT_FOUND;
  }
  SBlkHandle *pHandle = NULL;
  if (taosArrayGetSize(p->pMetaHandle) > 0) {
    pHandle = taosArrayGetLast(p->pMetaHandle);
    if (isGreaterSeqRange(&pHandle->range, seq)) {
      return blockSeek(p->pBlockWrapper.data, seq, value, len);
    } else {
      int32_t idx = findHandleBySeq(p->pMetaHandle, seq);
      if (idx < 0) {
        return TSDB_CODE_NOT_FOUND;
      }
      pHandle = taosArrayGet(p->pMetaHandle, idx);
      return tabldBuildSeekData(p, pHandle, seq, value, len);
    }
  } else {
    return blockSeek(p->pBlockWrapper.data, seq, value, len);
  }
  return TSDB_CODE_NOT_FOUND;
}

int32_t tableBuildAddMetaBlock(STableBuilder *p) {
  int32_t code = 0;
  int32_t lino = 0;

  int32_t dataEndOffset, metaEndOffset = 0;
  int64_t offset = 0;

  dataEndOffset = p->offset;
  code = tableBuildResizeAllBuf(p, taosArrayGetSize(p->pMetaHandle) * sizeof(SBlkHandle));
  if (code != 0) {
    return code;
  }

  bseInfo("pMeta handle size %d", taosArrayGetSize(p->pMetaHandle));
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
int32_t tableBuildCommit(STableBuilder *p, SBseLiveFileInfo *pInfo) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tableBuildFlush(p, BSE_TABLE_DATA_TYPE);
  TSDB_CHECK_CODE(code, lino, _error);

  code = tableBuildAddMetaBlock(p);
  TSDB_CHECK_CODE(code, lino, _error);

  code = tableBuildFlush(p, BSE_TABLE_META_TYPE);
  TSDB_CHECK_CODE(code, lino, _error);

  code = tableBuildAddFooter(p);
  TSDB_CHECK_CODE(code, lino, _error);

  tableBuildGenCommitInfo(p, pInfo);
  tableBuildClear(p);

  return code;
_error:
  if (code != 0) {
    bseError("failed to commit table builder since %s at line %d", tstrerror(code), lino);
  } else {
    bseInfo("succ to commit table %s", p->name);
  }
  return code;
}

static FORCE_INLINE int32_t tableBuildGetBlockSize(STableBuilder *p) { return p->blockCap; }

int32_t tableBuildClose(STableBuilder *p, int8_t commited) {
  int32_t code = 0;
  blockWrapperCleanup(&p->pBlockWrapper);
  blockWrapperCleanup(&p->pHBlockWrapper);
  taosArrayDestroy(p->pSeqToBlock);
  taosCloseFile(&p->pDataFile);
  taosArrayDestroy(p->pMetaHandle);
  taosMemFree(p);
  return code;
}
void tableBuildClear(STableBuilder *p) {
  blockWrapperClear(&p->pBlockWrapper);
  blockWrapperClear(&p->pHBlockWrapper);
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

static int32_t tableReadLoadFooter(STableReader *p) {
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

static int32_t tableReadMetaBuild(SBlock *p, SArray *pMetaHandle) {
  int32_t  code = 0;
  int32_t  lino = 0;
  SBlock  *pBlk = p;
  uint8_t *data = (uint8_t *)pBlk->data;
  int32_t  offset = 0;
  do {
    SBlkHandle handle = {0};
    offset += blkHandleDecode(&handle, (char *)data + offset);
    if (taosArrayPush(pMetaHandle, &handle) == NULL) {
      TSDB_CHECK_CODE(terrno, lino, _error);
    }
  } while (offset < pBlk->len);

_error:
  if (code != 0) {
    bseError("failed to load meta from table pReaderMgt since %s at line %d", tstrerror(code), lino);
  }
  return code;
}
static int32_t tableReadLoadMeta(STableReader *p) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tableReadLoadBlock(p, p->footer.metaHandle);
  TSDB_CHECK_CODE(code, lino, _error);

  if (blockGetType(p->pBlockWrapper.data) != BSE_TABLE_META_TYPE) {
    TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _error);
  }
  code = tableReadMetaBuild(p->pBlockWrapper.data, p->pMetaHandle);
  TSDB_CHECK_CODE(code, lino, _error);

_error:
  return code;
}
static int32_t tableReadOpenImpl(STableReader *p) {
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

  code = tableReadLoadMeta(p);
  TSDB_CHECK_CODE(code, lino, _error);

_error:
  if (code != 0) {
    bseError("failed to init table pReaderMgt name %s since %s at line %d", p->name, tstrerror(code), lino);
  }
  return code;
}
int32_t tableReadOpen(char *name, STableReader **pReader) {
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

  code = blockWrapperInit(&p->pBlockWrapper, p->blockCap);
  TSDB_CHECK_CODE(code, lino, _error);

  code = blockWrapperInit(&p->pHBlockWrapper, p->blockCap);
  TSDB_CHECK_CODE(code, lino, _error);

  memcpy(p->name, name, strlen(name));

  code = tableReadOpenImpl(p);
  TSDB_CHECK_CODE(code, lino, _error);

  *pReader = p;

_error:
  if (code != 0) {
    tableReadClose(p);
    bseError("failed to open table pReaderMgt file %s since %s at line %d", name, tstrerror(code), lino);
  }
  return code;
}

int32_t tableReadGet(STableReader *p, int64_t seq, uint8_t **pValue, int32_t *len) {
  int32_t     code = 0;
  SBlkHandle *pHandle = NULL;
  // opt later
  int32_t idx = findHandleBySeq(p->pMetaHandle, seq);
  if (idx < 0) {
    return TSDB_CODE_NOT_FOUND;
  }

  pHandle = taosArrayGet(p->pMetaHandle, idx);
  return tableReadSeekData(p, pHandle, seq, pValue, len);
}

static int32_t tableReadMayResizeLoadBuf(STableReader *p, int32_t size) {
  int32_t code = 0;
  int32_t lino = 0;
  code = blockWrapperResize(&p->pBlockWrapper, size);
  if (code != 0) {
    return code;
  }

  code = blockWrapperResize(&p->pHBlockWrapper, size);
  return code;
}

static int32_t tableReadDataBlock(STableReader *p, SBlkHandle *pHandle) {
  int32_t code = tableReadLoadBlock(p, pHandle);
  if (blockGetType(p->pBlockWrapper.data) != BSE_TABLE_DATA_TYPE) {
    return TSDB_CODE_FILE_CORRUPTED;
  }
  return code;
}

int32_t tableReadLoadBlock(STableReader *p, SBlkHandle *pHandle) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tableReadMayResizeLoadBuf(p, pHandle->size);
  TSDB_CHECK_CODE(code, lino, _error);

  code = tableLoadBlock(p->pDataFile, pHandle, p->pBlockWrapper.data);
  TSDB_CHECK_CODE(code, lino, _error);

_error:
  if (code != 0) {
    bseError("table reader failed to load block since %s at line %d", tstrerror(code), lino);
  }

  return code;
}

int32_t tableReadSeekData(STableReader *p, SBlkHandle *pHandle, int64_t seq, uint8_t **pValue, int32_t *len) {
  int32_t lino = 0;
  int32_t code = 0;

  code = tableReadLoadBlock(p, pHandle);
  if (code != 0) {
    return code;
  }

  return blockSeek(p->pBlockWrapper.data, seq, pValue, len);
}
int32_t tableReadClose(STableReader *p) {
  if (p == NULL) return 0;
  int32_t code = 0;

  taosArrayDestroy(p->pMetaHandle);
  taosArrayDestroy(p->pSeqToBlock);

  blockWrapperCleanup(&p->pBlockWrapper);
  blockWrapperCleanup(&p->pHBlockWrapper);

  taosCloseFile(&p->pDataFile);

  taosMemFree(p);
  return code;
}

static int32_t blockCreate(int32_t cap, SBlock **p) {
  int32_t code = 0;
  SBlock *t = taosMemCalloc(1, cap);
  if (t == NULL) {
    return terrno;
  }
  *p = t;
  return code;
}

static int32_t blockEsimateSize(SBlock *p, int32_t extra) {
  // block len + TSCHSUM + len + type;
  return sizeof(*p) + p->len + sizeof(TSCKSUM) + sizeof(int8_t) + extra;
}

static int32_t blockAppendBatch(SBlock *p, uint8_t *value, int32_t len) {
  int32_t  code = 0;
  int32_t  offset = 0;
  uint8_t *data = (uint8_t *)p->data + p->len;
  memcpy(data, value, len);
  p->len += len;
  return len;
}
static int32_t blockPut(SBlock *p, int64_t seq, uint8_t *value, int32_t len) {
  int32_t  code = 0;
  uint8_t *data = (uint8_t *)p->data + p->len;

  int32_t offset = taosEncodeVariantI64((void **)&data, seq);
  offset += taosEncodeVariantI32((void **)&data, len);
  offset += taosEncodeBinary((void **)&data, value, len);
  p->len += len;
  return offset;
}
static int32_t blockClear(SBlock *p) {
  int32_t code = 0;
  p->len = 0;
  p->type = 0;
  p->data[0] = 0;
  return code;
}

static int32_t blockSeek(SBlock *p, int64_t seq, uint8_t **pValue, int32_t *len) {
  int8_t  found = 0;
  int32_t code = 0;
  int32_t offset = 0;
  // 1. seq + len + value
  // opt read later

  uint8_t *p1 = (uint8_t *)p->data;
  uint8_t *p2 = p1;
  while (p2 - p1 < p->len) {
    int64_t k, v;
    p2 = taosDecodeVariantI64(p2, &k);
    p2 = taosDecodeVariantI64(p2, &v);
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

static int8_t blockGetType(SBlock *p) { return p->type; }
static void   blockDestroy(SBlock *pBlock) { taosMemFree(pBlock); }

static int32_t metaBlockAdd(SBlock *p, SBlkHandle *pInfo) {
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

static int32_t tableLoadBlock(TdFilePtr pFile, SBlkHandle *pHandle, SBlock *pBlk) {
  int32_t code = 0;
  int32_t lino = 0;

  (void)taosLSeekFile(pFile, pHandle->offset, SEEK_SET);

  int32_t nr = taosReadFile(pFile, pBlk, pHandle->size);
  if (nr != pHandle->size) {
    TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _error);
  }

  if (taosCheckChecksumWhole((uint8_t *)pBlk, pHandle->size) != 1) {
    TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _error);
  }

  if (pBlk->len != (pHandle->size - sizeof(TSCKSUM) - 1 - sizeof(SBlock))) {
    TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _error);
  }

  // uint8_t compresType = *(uint8_t *)(data + pHandle->size - sizeof(TSCKSUM) - 1);
  //  handle compress
_error:
  if (code != 0) {
    bseError("failed to load block since %s at line %d", tstrerror(code), lino);
  }
  return code;
}
int8_t inSeqRange(SSeqRange *p, int64_t seq) { return seq >= p->sseq && seq <= p->eseq; }

int8_t isGreaterSeqRange(SSeqRange *p, int64_t seq) { return seq > p->eseq; }

static int32_t blockWrapperInit(SBlockWrapper *p, int32_t cap) {
  p->data = taosMemoryCalloc(1, cap);
  if (p->data == NULL) {
    return terrno;
  }
  p->cap = cap;
  return 0;
}

static void blockWrapperCleanup(SBlockWrapper *p) {
  if (p->data != NULL) {
    taosMemoryFree(p->data);
    p->data = NULL;
  }
  p->cap = 0;
}

static int32_t blockWrapperResize(SBlockWrapper *p, int32_t newCap) {
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

static int32_t blockWrapperClear(SBlockWrapper *p) {
  SBlock *block = (SBlock *)p->data;

  blockClear(block);
  return 0;
}