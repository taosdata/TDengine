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

#ifndef BSE_TABLE_H_
#define BSE_TABLE_H_

#include "bse.h"
#include "bseInc.h"
#include "bseUtil.h"
#include "cJSON.h"
#include "os.h"
#include "tchecksum.h"
#include "tcompare.h"
#include "tlog.h"
#include "tmsg.h"
#include "tutil.h"

#ifdef __cplusplus
extern "C" {
#endif

#define kMaxEncodeLen 64
#define kEncodeLen    (2 * (kMaxEncodeLen) + 8)

#define kMagicNumber 0xdb4775248b80fb57ull;
#define kMagicNum    0x123456

enum {
  BSE_TABLE_DATA_TYPE = 0x1,
  BSE_TABLE_META_TYPE = 0x2,
  BSE_TABLE_META_INDEX_TYPE = 0x4,
  BSE_TABLE_FOOTER_TYPE = 0x8,
  BSE_TABLE_END_TYPE = 0x10,
};

typedef struct {
  int64_t seq;
  int64_t offset;
} SBlockIndexMeta;

typedef struct {
  uint64_t  offset;
  uint64_t  size;
  struct SSeqRange range;
} SBlkHandle;
typedef struct {
  SBlkHandle metaHandle[1];
  SBlkHandle indexHandle[1];
} STableFooter;

typedef struct {
  int32_t type;
  int32_t len;
  int32_t version;
  int64_t offset;
  char    data[0];
} SBlock;

typedef struct {
  int8_t    type;
  int8_t    version;
  int16_t   reserve;
  int64_t   offset;
  int64_t   size;
  struct SSeqRange range;
} SMetaBlock;

typedef struct {
  SBlock *pBlock;
  SArray *pMeta;
} SBlockWithMeta;

typedef struct {
  char          name[TSDB_FILENAME_LEN];
  TdFilePtr     pFile;
  STableFooter  footer;
  SArray       *pBlkHandle;
  SArray       *pLastBlkHandle;
  int64_t       offset;
  int64_t       size;
  int32_t       blockCap;
  SArray       *pBlock;
  SBlockWrapper blockWrapper;

  void *pTableMeta;
} SBtableMetaReader, SBtableMetaWriter;

typedef struct {
  int32_t            blkIdx;
  int8_t             isOver;
  SBtableMetaReader *pReader;
  SBlockWrapper      pBlockWrapper;

} SBtableMetaReaderIter;

typedef struct {
  SSeqRange range;
  int64_t   dataSize;
  int64_t   retionTs;
  int8_t    level;
} STableCommitInfo;
typedef struct {
  char    name[TSDB_FILENAME_LEN];
  int32_t blockCap;

  SBtableMetaWriter *pWriter;
  SBtableMetaReader *pReader;

  SSeqRange range;
  int64_t   timestamp;
  SBse     *pBse;
} SBTableMeta;

typedef struct {
  char             name[TSDB_FILENAME_LEN];
  TdFilePtr        pDataFile;

  int32_t       blockCap;
  int8_t        compressType;
  int64_t       offset;
  SSeqRange     tableRange;
  SSeqRange     blockRange;

  STableMemTable *pMemTable;
  STableMemTable *pImmuMemTable;
  int8_t          hasImmuMemTable;

  int32_t nRef;

  SBTableMeta *pTableMeta;
  int64_t      timestamp;
  void        *pBuilderMgt;

  SBse *pBse;
} STableBuilder;

typedef struct {
  char         name[TSDB_FILENAME_LEN];
  TdFilePtr    pDataFile;
  STableFooter footer;
  SArray      *pMetaHandle;

  int32_t blockCap;
  int64_t fileSize;
  void   *pReaderMgt;
  int8_t  putInCache;

  SBtableMetaReader *pMetaReader;
  SBlockWrapper      blockWrapper;
  SSeqRange range;

  int64_t timestamp;
} STableReader;

typedef struct {
  SSeqRange range;
  int64_t size;
  int32_t level;
  int64_t   timestamp;
  char    name[TSDB_FILENAME_LEN];
} SBseLiveFileInfo;

int32_t tableBuilderOpen(int64_t timestamp, STableBuilder **pBuilder, SBse *pBse);
int32_t tableBuilderPut(STableBuilder *p, SBseBatch *pBatch);
int32_t tableBuilderGet(STableBuilder *p, int64_t seq, uint8_t **value, int32_t *len);
int32_t tableBuilderFlush(STableBuilder *p, int8_t type, int8_t immuTable);
int32_t tableBuilderCommit(STableBuilder *p, SBseLiveFileInfo *pInfo);
void    tableBuilderClose(STableBuilder *p, int8_t commited);
int32_t tableBuilderTruncFile(STableBuilder *p, int64_t size);

int32_t tableReaderOpen(int64_t timestamp, STableReader **pReader, void *pReaderMgt);
void    tableReaderShouldPutToCache(STableReader *pReader, int8_t putInCache);
int32_t tableReaderGet(STableReader *p, int64_t seq, uint8_t **pValue, int32_t *len);
void    tableReaderClose(STableReader *p);
int32_t tableReaderGetMeta(STableReader *p, SArray **pMeta);

int32_t tableMetaOpen(char *name, SBTableMeta **pMeta, void *pMetaMgt);
int32_t tableMetaCommit(SBTableMeta *pMeta, SArray *pBlock);
int32_t tableMetaAppend(SBTableMeta *pMeta, SMetaBlock *pBlock);
int32_t tableMetaWriterAppendBlock(SBtableMetaWriter *pMeta, SArray *pBlock);
int32_t tableMetaReaderLoadBlockMeta(SBtableMetaReader *pMeta, int64_t seq, SMetaBlock *pBlock);

int32_t tableMetaReaderLoadAllDataHandle(SBtableMetaReader *p, SArray *dataHandle);
int32_t tableMetaReaderLoadMetaHandle(SBtableMetaReader *p, SArray *metaHandle);

void    tableMetaClose(SBTableMeta *p);

typedef struct {
  char          name[TSDB_FILENAME_LEN];
  int8_t        isOver;
  SSeqRange     range;
  STableReader *pTableReader;
  SArray       *pMetaHandle;
  int32_t       blockIndex;
  int64_t       offset;
  SBlockWrapper blockWrapper;
  int8_t        blockType;  // BSE_TABLE_DATA_TYPE, BSE_TABLE_META_TYPE, BSE_TABLE_FOOTER_TYPE
  int8_t        fileType;
  int64_t       timestamp;

  void *pSubMgt;
} STableReaderIter;

int32_t tableReaderIterInit(int64_t timestamp, int8_t type, STableReaderIter **ppIter, SBse *pBse);

int32_t tableReaderIterNext(STableReaderIter *pIter, uint8_t **pValue, int32_t *len);

void tableReaderIterDestroy(STableReaderIter *pIter);

int8_t tableReaderIterValid(STableReaderIter *pIter);

int32_t bseReadCurrentSnap(SBse *pBse, uint8_t **pValue, int32_t *len);

#ifdef __cplusplus
}
#endif
#endif