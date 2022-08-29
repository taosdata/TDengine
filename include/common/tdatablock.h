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

#ifndef _TD_COMMON_EP_H_
#define _TD_COMMON_EP_H_

#include "tcommon.h"
#include "tcompression.h"
#include "tmsg.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SCorEpSet {
  int32_t version;
  SEpSet  epSet;
} SCorEpSet;

typedef struct SBlockOrderInfo {
  bool             nullFirst;
  int32_t          order;
  int32_t          slotId;
  SColumnInfoData* pColData;
} SBlockOrderInfo;

int32_t taosGetFqdnPortFromEp(const char* ep, SEp* pEp);
void    addEpIntoEpSet(SEpSet* pEpSet, const char* fqdn, uint16_t port);

bool isEpsetEqual(const SEpSet* s1, const SEpSet* s2);

void   updateEpSet_s(SCorEpSet* pEpSet, SEpSet* pNewEpSet);
SEpSet getEpSet_s(SCorEpSet* pEpSet);

#define NBIT                     (3u)
#define BitPos(_n)               ((_n) & ((1 << NBIT) - 1))
#define BMCharPos(bm_, r_)       ((bm_)[(r_) >> NBIT])
#define colDataIsNull_f(bm_, r_) ((BMCharPos(bm_, r_) & (1u << (7u - BitPos(r_)))) == (1u << (7u - BitPos(r_))))

#define colDataSetNull_f(bm_, r_)                    \
  do {                                               \
    BMCharPos(bm_, r_) |= (1u << (7u - BitPos(r_))); \
  } while (0)

#define colDataSetNotNull_f(bm_, r_)                  \
  do {                                                \
    BMCharPos(bm_, r_) &= ~(1u << (7u - BitPos(r_))); \
  } while (0)

#define colDataIsNull_var(pColumnInfoData, row)  (pColumnInfoData->varmeta.offset[row] == -1)
#define colDataSetNull_var(pColumnInfoData, row) (pColumnInfoData->varmeta.offset[row] = -1)

#define BitmapLen(_n) (((_n) + ((1 << NBIT) - 1)) >> NBIT)

#define colDataGetVarData(p1_, r_) ((p1_)->pData + (p1_)->varmeta.offset[(r_)])

#define colDataGetNumData(p1_, r_) ((p1_)->pData + ((r_) * (p1_)->info.bytes))
// SColumnInfoData, rowNumber
#define colDataGetData(p1_, r_) \
  ((IS_VAR_DATA_TYPE((p1_)->info.type)) ? colDataGetVarData(p1_, r_) : colDataGetNumData(p1_, r_))

#define IS_JSON_NULL(type, data) \
  ((type) == TSDB_DATA_TYPE_JSON && (*(data) == TSDB_DATA_TYPE_NULL || tTagIsJsonNull(data)))

static FORCE_INLINE bool colDataIsNull_s(const SColumnInfoData* pColumnInfoData, uint32_t row) {
  if (!pColumnInfoData->hasNull) {
    return false;
  }

  if (IS_VAR_DATA_TYPE(pColumnInfoData->info.type)) {
    return colDataIsNull_var(pColumnInfoData, row);
  } else {
    if (pColumnInfoData->nullbitmap == NULL) {
      return false;
    }

    return colDataIsNull_f(pColumnInfoData->nullbitmap, row);
  }
}

static FORCE_INLINE bool colDataIsNull(const SColumnInfoData* pColumnInfoData, uint32_t totalRows, uint32_t row,
                                       SColumnDataAgg* pColAgg) {
  if (!pColumnInfoData->hasNull) {
    return false;
  }

  if (pColAgg != NULL) {
    if (pColAgg->numOfNull == totalRows) {
      ASSERT(pColumnInfoData->nullbitmap == NULL);
      return true;
    } else if (pColAgg->numOfNull == 0) {
      ASSERT(pColumnInfoData->nullbitmap == NULL);
      return false;
    }
  }

  if (IS_VAR_DATA_TYPE(pColumnInfoData->info.type)) {
    return colDataIsNull_var(pColumnInfoData, row);
  } else {
    if (pColumnInfoData->nullbitmap == NULL) {
      return false;
    }

    return colDataIsNull_f(pColumnInfoData->nullbitmap, row);
  }
}

static FORCE_INLINE void colDataAppendNULL(SColumnInfoData* pColumnInfoData, uint32_t currentRow) {
  // There is a placehold for each NULL value of binary or nchar type.
  if (IS_VAR_DATA_TYPE(pColumnInfoData->info.type)) {
    colDataSetNull_var(pColumnInfoData, currentRow);  // it is a null value of VAR type.
  } else {
    colDataSetNull_f(pColumnInfoData->nullbitmap, currentRow);
  }

  pColumnInfoData->hasNull = true;
}

static FORCE_INLINE void colDataAppendNNULL(SColumnInfoData* pColumnInfoData, uint32_t start, size_t nRows) {
  if (IS_VAR_DATA_TYPE(pColumnInfoData->info.type)) {
    for (int32_t i = start; i < start + nRows; ++i) {
      colDataSetNull_var(pColumnInfoData, i);  // it is a null value of VAR type.
    }
  } else {
    for (int32_t i = start; i < start + nRows; ++i) {
      colDataSetNull_f(pColumnInfoData->nullbitmap, i);
    }
  }

  pColumnInfoData->hasNull = true;
}

static FORCE_INLINE void colDataAppendInt8(SColumnInfoData* pColumnInfoData, uint32_t currentRow, int8_t* v) {
  ASSERT(pColumnInfoData->info.type == TSDB_DATA_TYPE_TINYINT ||
         pColumnInfoData->info.type == TSDB_DATA_TYPE_UTINYINT || pColumnInfoData->info.type == TSDB_DATA_TYPE_BOOL);
  char* p = pColumnInfoData->pData + pColumnInfoData->info.bytes * currentRow;
  *(int8_t*)p = *(int8_t*)v;
}

static FORCE_INLINE void colDataAppendInt16(SColumnInfoData* pColumnInfoData, uint32_t currentRow, int16_t* v) {
  ASSERT(pColumnInfoData->info.type == TSDB_DATA_TYPE_SMALLINT ||
         pColumnInfoData->info.type == TSDB_DATA_TYPE_USMALLINT);
  char* p = pColumnInfoData->pData + pColumnInfoData->info.bytes * currentRow;
  *(int16_t*)p = *(int16_t*)v;
}

static FORCE_INLINE void colDataAppendInt32(SColumnInfoData* pColumnInfoData, uint32_t currentRow, int32_t* v) {
  ASSERT(pColumnInfoData->info.type == TSDB_DATA_TYPE_INT || pColumnInfoData->info.type == TSDB_DATA_TYPE_UINT);
  char* p = pColumnInfoData->pData + pColumnInfoData->info.bytes * currentRow;
  *(int32_t*)p = *(int32_t*)v;
}

static FORCE_INLINE void colDataAppendInt64(SColumnInfoData* pColumnInfoData, uint32_t currentRow, int64_t* v) {
  int32_t type = pColumnInfoData->info.type;
  ASSERT(type == TSDB_DATA_TYPE_BIGINT || type == TSDB_DATA_TYPE_UBIGINT || type == TSDB_DATA_TYPE_TIMESTAMP);
  char* p = pColumnInfoData->pData + pColumnInfoData->info.bytes * currentRow;
  *(int64_t*)p = *(int64_t*)v;
}

static FORCE_INLINE void colDataAppendFloat(SColumnInfoData* pColumnInfoData, uint32_t currentRow, float* v) {
  ASSERT(pColumnInfoData->info.type == TSDB_DATA_TYPE_FLOAT);
  char* p = pColumnInfoData->pData + pColumnInfoData->info.bytes * currentRow;
  *(float*)p = *(float*)v;
}

static FORCE_INLINE void colDataAppendDouble(SColumnInfoData* pColumnInfoData, uint32_t currentRow, double* v) {
  ASSERT(pColumnInfoData->info.type == TSDB_DATA_TYPE_DOUBLE);
  char* p = pColumnInfoData->pData + pColumnInfoData->info.bytes * currentRow;
  *(double*)p = *(double*)v;
}

int32_t getJsonValueLen(const char* data);

int32_t colDataAppend(SColumnInfoData* pColumnInfoData, uint32_t currentRow, const char* pData, bool isNull);
int32_t colDataAppendNItems(SColumnInfoData* pColumnInfoData, uint32_t currentRow, const char* pData, uint32_t numOfRows);
int32_t colDataMergeCol(SColumnInfoData* pColumnInfoData, int32_t numOfRow1, int32_t* capacity,
                        const SColumnInfoData* pSource, int32_t numOfRow2);
int32_t colDataAssign(SColumnInfoData* pColumnInfoData, const SColumnInfoData* pSource, int32_t numOfRows,
                      const SDataBlockInfo* pBlockInfo);
int32_t blockDataUpdateTsWindow(SSDataBlock* pDataBlock, int32_t tsColumnIndex);

int32_t colDataGetLength(const SColumnInfoData* pColumnInfoData, int32_t numOfRows);
void    colDataTrim(SColumnInfoData* pColumnInfoData);

size_t blockDataGetNumOfCols(const SSDataBlock* pBlock);
size_t blockDataGetNumOfRows(const SSDataBlock* pBlock);

int32_t blockDataMerge(SSDataBlock* pDest, const SSDataBlock* pSrc);
int32_t blockDataSplitRows(SSDataBlock* pBlock, bool hasVarCol, int32_t startIndex, int32_t* stopIndex,
                           int32_t pageSize);
int32_t blockDataToBuf(char* buf, const SSDataBlock* pBlock);
int32_t blockDataFromBuf(SSDataBlock* pBlock, const char* buf);
int32_t blockDataFromBuf1(SSDataBlock* pBlock, const char* buf, size_t capacity);

SSDataBlock* blockDataExtractBlock(SSDataBlock* pBlock, int32_t startIndex, int32_t rowCount);

size_t blockDataGetSize(const SSDataBlock* pBlock);
size_t blockDataGetRowSize(SSDataBlock* pBlock);
double blockDataGetSerialRowSize(const SSDataBlock* pBlock);
size_t blockDataGetSerialMetaSize(uint32_t numOfCols);

int32_t blockDataSort(SSDataBlock* pDataBlock, SArray* pOrderInfo);
int32_t blockDataSort_rv(SSDataBlock* pDataBlock, SArray* pOrderInfo, bool nullFirst);

int32_t colInfoDataEnsureCapacity(SColumnInfoData* pColumn, uint32_t numOfRows);
int32_t blockDataEnsureCapacity(SSDataBlock* pDataBlock, uint32_t numOfRows);

void colInfoDataCleanup(SColumnInfoData* pColumn, uint32_t numOfRows);
void blockDataCleanup(SSDataBlock* pDataBlock);

size_t blockDataGetCapacityInRow(const SSDataBlock* pBlock, size_t pageSize);

int32_t blockDataTrimFirstNRows(SSDataBlock* pBlock, size_t n);
int32_t blockDataKeepFirstNRows(SSDataBlock* pBlock, size_t n);

int32_t      assignOneDataBlock(SSDataBlock* dst, const SSDataBlock* src);
int32_t      copyDataBlock(SSDataBlock* dst, const SSDataBlock* src);

SSDataBlock* createDataBlock();
void*        blockDataDestroy(SSDataBlock* pBlock);
void         blockDataFreeRes(SSDataBlock* pBlock);
SSDataBlock* createOneDataBlock(const SSDataBlock* pDataBlock, bool copyData);

int32_t      blockDataAppendColInfo(SSDataBlock* pBlock, SColumnInfoData* pColInfoData);

SColumnInfoData  createColumnInfoData(int16_t type, int32_t bytes, int16_t colId);
SColumnInfoData* bdGetColumnInfoData(const SSDataBlock* pBlock, int32_t index);

void blockEncode(const SSDataBlock* pBlock, char* data, int32_t* dataLen, int32_t numOfCols, int8_t needCompress);
const char* blockDecode(SSDataBlock* pBlock, const char* pData);

void blockDebugShowDataBlock(SSDataBlock* pBlock, const char* flag);
void blockDebugShowDataBlocks(const SArray* dataBlocks, const char* flag);
// for debug
char* dumpBlockData(SSDataBlock* pDataBlock, const char* flag, char** dumpBuf);

int32_t buildSubmitReqFromDataBlock(SSubmitReq** pReq, const SSDataBlock* pDataBlocks, STSchema* pTSchema, int32_t vgId,
                                    tb_uid_t suid);


char* buildCtbNameByGroupId(const char* stbName, uint64_t groupId);

static FORCE_INLINE int32_t blockGetEncodeSize(const SSDataBlock* pBlock) {
  return blockDataGetSerialMetaSize(taosArrayGetSize(pBlock->pDataBlock)) + blockDataGetSize(pBlock);
}

static FORCE_INLINE int32_t blockCompressColData(SColumnInfoData* pColRes, int32_t numOfRows, char* data,
                                                 int8_t compressed) {
  int32_t colSize = colDataGetLength(pColRes, numOfRows);
  return (*(tDataTypes[pColRes->info.type].compFunc))(pColRes->pData, colSize, numOfRows, data,
                                                      colSize + COMP_OVERFLOW_BYTES, compressed, NULL, 0);
}

#ifdef __cplusplus
}
#endif

#endif /*_TD_COMMON_EP_H_*/
