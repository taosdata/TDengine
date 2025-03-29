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

#include "bse.h"
#include "cJSON.h"
#include "lz4.h"
#include "os.h"
#include "tchecksum.h"
#include "tcompare.h"
#include "tlog.h"
#include "tmsg.h"
#include "tutil.h"

#define kMaxEncodeLen 64
#define kEncodeLen    (2 * (kMaxEncodeLen) + 8)

#define kMagicNumber 0xdb4775248b80fb57ull;
#define kMagicNum    0x123456

enum type {
  BSE_TABLE_DATA_TYPE = 0x1,
  BSE_TABLE_META_TYPE = 0x2,
  BSE_FOOTER_TYPE = 0x4,
};

typedef enum {
  kNoCompres = 0,
  kLZ4Compres = 1,
  kZSTDCompres = 2,
  kZLibCompres = 4,
  kZxCompress = 8,
} SBseTableCompress;

typedef struct {
  uint64_t offset;
  uint64_t size;
  uint32_t blockId;
  int64_t  seq;
} SBlkHandle;

typedef struct {
  SBlkHandle metaHandle[1];
  SBlkHandle indexHandle[1];
} STableFooter;

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

int32_t blkHandleEncode(SBlkHandle *pHandle, char *buf) {
  char   *p = buf;
  int32_t tlen = 0;
  tlen += taosEncodeVariantU64((void **)&p, pHandle->offset);
  tlen += taosEncodeVariantU64((void **)&p, pHandle->size);
  tlen += taosEncodeVariantU32((void **)&p, pHandle->blockId);
  tlen += taosEncodeVariantI64((void **)&p, pHandle->seq);
  return tlen;
}
int32_t blkHandleDecode(SBlkHandle *pHandle, char *buf) {
  char *p = buf;
  p = taosDecodeVariantU64(p, &pHandle->offset);
  p = taosDecodeVariantU64(p, &pHandle->size);
  p = taosDecodeVariantU32(p, &pHandle->blockId);
  p = taosDecodeVariantI64(p, &pHandle->seq);
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
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t len = blkHandleDecode(pFooter->metaHandle, buf);
  if (len < 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  len = blkHandleDecode(pFooter->indexHandle, buf + len);
  if (len < 0) {
    return TSDB_CODE_INVALID_PARA;
  }
  return code;
}

typedef struct {
  int64_t seq;

} SSeqToBlk;
typedef struct {
  int32_t type;
  int32_t len;
  char    data[0];
} SBlock;

static int32_t blockCreate(int32_t cap, SBlock **pBlock);
static int32_t blockPut(SBlock *pBlock, int64_t seq, uint8_t *value, int32_t len);
static int32_t blockEsimateSize(SBlock *pBlock, int32_t extra);
static int32_t blockClear(SBlock *pBlock);
static void    blockDestroy(SBlock *pBlock);
static int32_t blockAddMeta(SBlock *p, SBlkHandle *pInfo);
static int32_t blockSeek(SBlock *p, int64_t seq, uint8_t **pValue, int32_t *len);

typedef struct {
  char         name[TSDB_FILENAME_LEN];
  TdFilePtr    pDataFile;
  STableFooter footer;
  SArray      *pSeqToBlock;
  SArray      *pMetaHandle;
  SBlock      *pData;
  SBlock      *pHdata;
  void        *bse;
  int32_t      blockCap;
  int8_t       compressType;
  int32_t      offset;
  int32_t      blockId;
  int64_t      lastSeq;
} STableBuilder;

int32_t tableBuildOpen(char *path, STableBuilder **pBuilder);
int32_t tableBuildPut(STableBuilder *p, uint64_t *seq, uint8_t *value, int32_t len);
int32_t tableBuildFlush(STableBuilder *p, int8_t type);
int32_t tableBuildCommit(STableBuilder *p);
int32_t tableBuildClose(STableBuilder *p);
int32_t tableBuildGetBlockSize(STableBuilder *p);

typedef struct {
  char         name[TSDB_FILENAME_LEN];
  TdFilePtr    pDataFile;
  STableFooter footer;
  SArray      *pSeqToBlock;
  SArray      *pMetaHandle;
  SBlock      *pData;
  SBlock      *pHdata;
  int32_t      blockCap;
  int32_t      fileSize;
} STableReader;

int32_t tableReadOpen(char *name, STableReader *pReader);
int32_t tableReadClose(STableReader *p);

int32_t tableReadGet(STableReader *p, int64_t seq, uint8_t **pValue, int32_t *len);
int32_t tableReadLoad(STableReader *p, SBlkHandle *pHandle);

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

  p->pDataFile = taosOpenFile(p->name, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_READ | TD_FILE_APPEND);
  if (p->pDataFile == NULL) {
    TSDB_CHECK_CODE(terrno, lino, _error);
  }

  p->blockCap = 4 * 1024 * 1024;
  p->compressType = 0;
  code = blockCreate(tableBuildGetBlockSize(p), &p->pData);
  TSDB_CHECK_CODE(code, lino, _error);

  code = blockCreate(tableBuildGetBlockSize(p), &p->pHdata);
  TSDB_CHECK_CODE(code, lino, _error);

  *pBuilder = p;

  return code;
_error:
  if (code != 0) {
    (void)tableBuildClose(p);
    bseError("failed to open table builder since %s at line %d", tstrerror(code), lino);
  }
  return code;
}

int32_t tableBuildAddFooter(STableBuilder *p) {
  char buf[kEncodeLen];

  int32_t code = 0;
  int32_t lino = 0;

  code = footerEncode(&p->footer, buf);
  TSDB_CHECK_CODE(code, lino, _error);

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

  if (p->pData->len == 0) {
    return 0;
  }
  SBlock *pBlk = p->pData;
  int32_t len = sizeof(SBlock) + pBlk->len + sizeof(int8_t) + sizeof(TSCKSUM);

  pBlk->type = type;
  uint8_t *head = (uint8_t *)p->pData;

  uint8_t *cmprType = (uint8_t *)pBlk->data + pBlk->len;
  memcpy(cmprType, &p->compressType, sizeof(p->compressType));
  // do compress

  code = taosCalcChecksumAppend(0, (uint8_t *)pBlk, len);
  TSDB_CHECK_CODE(code, lino, _error);

  SBlkHandle handle = {.size = len, .offset = p->offset, .blockId = p->blockId, .seq = p->lastSeq};

  int32_t nwrite = taosWriteFile(p->pDataFile, (uint8_t *)pBlk, len);
  if (nwrite != len) {
    code = terrno;
    TSDB_CHECK_CODE(code, lino, _error);
  }
  p->offset += len;
  if (type == BSE_TABLE_DATA_TYPE) {
    taosArrayPush(p->pMetaHandle, &handle);
  }

  blockClear(p->pData);
_error:
  if (code != 0) {
    bseError("failed to flush table builder since %s at line %d", tstrerror(code), lino);
  }
  return code;
}
int32_t tableBuildPut(STableBuilder *p, uint64_t *seq, uint8_t *value, int32_t len) {
  int32_t code = 0;
  int32_t lino = 0;
  // seqlen + valuelen + value
  int32_t extra = sizeof(*seq) + len + sizeof(len);
  if (blockEsimateSize(p->pData, extra) >= tableBuildGetBlockSize(p)) {
    code = tableBuildFlush(p, BSE_TABLE_DATA_TYPE);
    TSDB_CHECK_CODE(code, lino, _error);
  }

  p->lastSeq = *seq;
  code = blockPut(p->pData, *seq, value, len);
  TSDB_CHECK_CODE(code, lino, _error);

_error:
  if (code != 0) {
    bseError("failed to put value by seq %" PRId64 " since %s at lino %d", *seq, tstrerror(code), lino);
  }
  return code;
}

int32_t tableBuildAddMetaBlock(STableBuilder *p) {
  int32_t code = 0;
  int32_t lino = 0;

  int32_t dataEndOffset, metaEndOffset = 0;

  dataEndOffset = p->offset;
  for (int32_t i = 0; i < taosArrayGetSize(p->pMetaHandle); i++) {
    SBlkHandle *pHandle = taosArrayGet(p->pMetaHandle, i);
    code = blockAddMeta(p->pData, pHandle);
    TSDB_CHECK_CODE(code, lino, _error);
  }

  metaEndOffset = p->offset;

  SBlkHandle metaHandle = {
      .offset = dataEndOffset, .size = metaEndOffset - dataEndOffset, .blockId = p->blockId, .seq = p->lastSeq};

  SBlkHandle indexHandle = {
      .offset = metaEndOffset, .size = p->offset - metaEndOffset, .blockId = p->blockId, .seq = p->lastSeq};

  memcpy(p->footer.metaHandle, &metaHandle, sizeof(metaHandle));
  memcpy(p->footer.indexHandle, &metaHandle, sizeof(indexHandle));

_error:
  if (code != 0) {
    bseError("failed to add meta block to table builder since %s at line %d", tstrerror(code), 0);
  }
  return code;
}
int32_t tableBuildCommit(STableBuilder *p) {
  int32_t code = 0;
  int32_t lino = 0;

  code = tableBuildFlush(p, BSE_TABLE_DATA_TYPE);
  TSDB_CHECK_CODE(code, lino, _error);

  code = tableBuildAddMetaBlock(p);
  TSDB_CHECK_CODE(code, lino, _error);

  code = tableBuildFlush(p, BSE_TABLE_META_TYPE);

  code = tableBuildAddFooter(p);
_error:
  if (code != 0) {
    bseError("failed to commit table builder since %s at line %d", tstrerror(code), lino);
  }
  return code;
}

int32_t tableBuildClose(STableBuilder *p) {
  int32_t code = 0;
  taosArrayDestroy(p->pSeqToBlock);
  taosCloseFile(&p->pDataFile);
  blockDestroy(p->pData);
  blockDestroy(p->pHdata);
  taosMemFree(p);
  return code;
}

static int32_t tableReadLoadFooter(STableReader *p) {
  int32_t code = 0;
  int32_t lino = 0;

  char footer[kEncodeLen];
  if (p->fileSize <= sizeof(footer)) {
    TSDB_CHECK_CODE(TSDB_CODE_FILE_CORRUPTED, lino, _error);
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

static int32_t tableReadLoadMeta(STableReader *p) {
  int32_t code = 0;
  int32_t lino = 0;

  SBlkHandle *pHandle = p->footer.metaHandle;
  code = tableReadLoad(p, pHandle);
  TSDB_CHECK_CODE(code, lino, _error);

  SBlock  *pBlk = p->pData;
  uint8_t *data = (uint8_t *)pBlk->data;

  if (pBlk->type != BSE_TABLE_META_TYPE) {
    TSDB_CHECK_CODE(TSDB_CODE_FILE_CORRUPTED, lino, _error);
  }

  int32_t offset = 0;
  do {
    SBlkHandle handle = {0};
    offset += blkHandleDecode(&handle, (char *)data + offset);
    if (taosArrayPush(p->pMetaHandle, &handle) == NULL) {
      TSDB_CHECK_CODE(terrno, lino, _error);
    }
  } while (offset < pBlk->len);

_error:
  if (code != 0) {
    bseError("failed to load meta from table reader since %s at line %d", tstrerror(code), lino);
  }
  return code;
}
static int32_t tableReadInitFile(STableReader *p) {
  int32_t code = 0;
  int32_t lino = 0;
  int64_t size = 0;

  char footer[kEncodeLen];

  code = taosStatFile(p->name, &size, NULL, NULL);
  TSDB_CHECK_CODE(code, lino, _error);

  if (size <= 0) {
    TSDB_CHECK_CODE(TSDB_CODE_INVALID_PARA, lino, _error);
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
    bseError("failed to init table reader since %s at line %d", tstrerror(code), lino);
  }
  return code;
}
int32_t tableReadOpen(char *name, STableReader *pReader) {
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

  p->blockCap = 4 * 1024 * 1024;

  code = blockCreate(p->blockCap, &p->pData);
  TSDB_CHECK_CODE(code, lino, _error);

  code = blockCreate(p->blockCap, &p->pHdata);
  TSDB_CHECK_CODE(code, lino, _error);

  memcpy(pReader->name, name, strlen(name));

  code = tableReadInitFile(p);
  TSDB_CHECK_CODE(code, lino, _error);

_error:
  if (code != 0) {
    tableReadClose(p);
    bseError("failed to open table reader file %s since %s at line %d", name, tstrerror(code), lino);
  }
  return code;
}

int32_t tableReadGet(STableReader *p, int64_t seq, uint8_t **pValue, int32_t *len) {
  int32_t     code = 0;
  SBlkHandle *pHandle = NULL;
  for (int32_t i = 0; i < taosArrayGetSize(p->pMetaHandle); i++) {
    pHandle = taosArrayGet(p->pMetaHandle, i);
    if (seq <= pHandle->seq) {
      break;
    } else {
      pHandle = NULL;
    }
  }

  if (pHandle == NULL) {
    return TSDB_CODE_NOT_FOUND;
  }

  return code;
}

int32_t tableReadLoad(STableReader *p, SBlkHandle *pHandle) {
  int32_t code = 0;
  int32_t lino = 0;

  (void)taosLSeekFile(p->pDataFile, pHandle->offset, SEEK_SET);
  if (p->blockCap < pHandle->size) {
    int32_t cap = p->blockCap;
    while (cap < pHandle->size) {
      cap = cap * 2;
    }
    (void)blockDestroy(p->pData);
    (void)blockDestroy(p->pHdata);

    code = blockCreate(cap, &p->pData);
    code = blockCreate(cap, &p->pHdata);

    p->blockCap = cap;
  }
  SBlock *pBlk = p->pData;

  int32_t nr = taosReadFile(p->pDataFile, p->pData, pHandle->size);
  if (nr != pHandle->size) {
    TSDB_CHECK_CODE(TSDB_CODE_FILE_CORRUPTED, lino, _error);
  }

  uint8_t *data = (uint8_t *)pBlk->data;
  if (taosCheckChecksumWhole(data, pHandle->size) != 1) {
    TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _error);
  }

  if (pBlk->len != pHandle->size - sizeof(TSCKSUM) - 1) {
    TSDB_CHECK_CODE(code = TSDB_CODE_FILE_CORRUPTED, lino, _error);
  }

  // uint8_t compresType = *(uint8_t *)(data + pHandle->size - sizeof(TSCKSUM) - 1);
  //  handle compress

_error:

  return code;
}

int32_t tableLoadData(STableReader *p, SBlkHandle *pHandle, int64_t seq, uint8_t **pValue, int32_t *len) {
  int32_t lino = 0;
  int32_t code = 0;

  code = tableReadLoad(p, pHandle);
  if (code != 0) {
    return code;
  }

  code = blockSeek(p->pData, seq, pValue, len);

  return code;
}
int32_t tableReadClose(STableReader *p) {
  if (p == NULL) return 0;
  int32_t code = 0;

  taosArrayDestroy(p->pMetaHandle);
  taosArrayDestroy(p->pSeqToBlock);

  blockDestroy(p->pData);
  blockDestroy(p->pHdata);

  taosCloseFile(&p->pDataFile);

  taosMemFree(p);
  return code;
}

// static int32_t blockCreateByHandle(, SBlock **pBlock) {
//   int32_t code = 0;
// }
static int32_t blockCreate(int32_t cap, SBlock **pBlock) {
  int32_t code = 0;
  SBlock *p = taosMemCalloc(1, sizeof(SBlock) + cap);
  if (p == NULL) {
    return terrno;
  }
  p->len = 0;
  *pBlock = p;
  return code;
}
static void blockDestroy(SBlock *pBlock) { taosMemFree(pBlock); }

static int32_t blockEsimateSize(SBlock *p, int32_t extra) {
  // block len + TSCHSUM + len + type;
  return sizeof(*p) + p->len + sizeof(TSCKSUM) + sizeof(int8_t);
}

static int32_t blockPut(SBlock *p, int64_t seq, uint8_t *value, int32_t len) {
  int32_t  code = 0;
  uint8_t *data = (uint8_t *)p->data + p->len;

  int32_t offset = taosEncodeVariantI64((void **)&data, seq);
  offset += taosEncodeVariantI32((void **)&data, len);
  offset += taosEncodeBinary((void **)&data, value, len);
  p->len += len;
  return code;
}
static int32_t blockClear(SBlock *p) {
  int32_t code = 0;
  p->len = 0;
  p->type = 0;
  p->data[0] = 0;
  return code;
}

static int32_t blockAddMeta(SBlock *p, SBlkHandle *pInfo) {
  int32_t  code = 0;
  uint8_t *data = (uint8_t *)p->data + p->len;
  int32_t  offset = blkHandleEncode(pInfo, (char *)data);
  p->len += offset;
  return code;
}

static int32_t blockFillData(SBlock *p, SBlkHandle *pInfo) {
  int32_t code = 0;

  return code;
}

static int32_t blockSeek(SBlock *p, int64_t seq, uint8_t **pValue, int32_t *len) {
  int8_t   found = 0;
  int32_t  code = 0;
  int32_t  offset = 0;
  uint8_t *p1 = (uint8_t *)p->data;
  uint8_t *p2 = p1;
  while (p2 - p1 < p->len) {
    int64_t k, v;
    p2 = taosDecodeVariantI64(p2, &k);
    p2 = taosDecodeVariantI64(p2, &v);
    if (seq == v) {
      *pValue = taosMemCalloc(1, v);
      memcpy(*pValue, p2, v);
      *len = v;
      found = 1;
      break;
    }

    p2 += v;
  }
  if (found) {
    code = TSDB_CODE_NOT_FOUND;
  }

  return code;
}

static int8_t blockGetType(SBlock *p) { return p->type; }
