/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

#include <stdint.h>
#include "tcommon.h"
#include "tcompression.h"
#include "tmsg.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SBlockOrderInfo {
  bool             nullFirst;
  int32_t          order;
  int32_t          slotId;
  void*            compFn;
  SColumnInfoData* pColData;
} SBlockOrderInfo;

#define BLOCK_VERSION_1 1
#define BLOCK_VERSION_2 2
#define BLOCK_RECYCLE_TIMEOUT_MS 60000

#define NBIT                     (3u)
#define BitPos(_n)               ((_n) & ((1 << NBIT) - 1))
#define CharPos(r_)              ((r_) >> NBIT)
#define BMCharPos(bm_, r_)       ((bm_)[(r_) >> NBIT])
#define colDataIsNull_f(c_, r_) (((c_)->nullbitmap && (BMIsNull((c_)->nullbitmap, r_))) || (c_)->pData == NULL)
#define BMIsNull(bm_, r_) ((BMCharPos(bm_, r_) & (1u << (7u - BitPos(r_)))) == (1u << (7u - BitPos(r_))))


#define QRY_PARAM_CHECK(_o)          \
  do {                               \
    if ((_o) == NULL) {              \
      return TSDB_CODE_INVALID_PARA; \
    } else {                         \
      *(_o) = NULL;                  \
    }                                \
  } while (0)

#define colDataSetNull_f(bm_, r_)                    \
  do {                                               \
    BMCharPos(bm_, r_) |= (1u << (7u - BitPos(r_))); \
  } while (0)

#define colDataSetNull_f_s(c_, r_)                                                     \
  do {                                                                                 \
    colDataSetNull_f((c_)->nullbitmap, r_);                                            \
    (void)memset(((char*)(c_)->pData) + (c_)->info.bytes * (r_), 0, (c_)->info.bytes); \
  } while (0)

#define colDataClearNull_f(bm_, r_)                             \
  do {                                                          \
    BMCharPos(bm_, r_) &= ((char)(~(1u << (7u - BitPos(r_))))); \
  } while (0)

#define colDataIsNull_var(pColumnInfoData, row)  (pColumnInfoData->varmeta.offset[row] == -1 || pColumnInfoData->pData == NULL)
#define colDataSetNull_var(pColumnInfoData, row) (pColumnInfoData->varmeta.offset[row] = -1)

#define BitmapLen(_n) (((_n) + ((1 << NBIT) - 1)) >> NBIT)

#define colDataGetVarData(p1_, r_) ((p1_)->pData + (p1_)->varmeta.offset[(r_)])
#define colDataGetNumData(p1_, r_) ((p1_)->pData + ((r_) * (p1_)->info.bytes))
// SColumnInfoData, rowNumber
#define colDataGetData(p1_, r_) \
  ((IS_VAR_DATA_TYPE((p1_)->info.type)) ? colDataGetVarData(p1_, r_) : colDataGetNumData(p1_, r_))

#define IS_JSON_NULL(type, data) \
  ((type) == TSDB_DATA_TYPE_JSON && (*(data) == TSDB_DATA_TYPE_NULL || tTagIsJsonNull(data)))

#define GET_COL_DATA_TYPE(col) \
  { .type = (col).type, .precision = (col).precision, .bytes = (col).bytes, .scale = (col).scale }

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

    return colDataIsNull_f(pColumnInfoData, row);
  }
}

static FORCE_INLINE bool colDataIsNull_t(const SColumnInfoData* pColumnInfoData, uint32_t row, bool isVarType) {
  if (!pColumnInfoData->hasNull) return false;
  if (isVarType) {
    return colDataIsNull_var(pColumnInfoData, row);
  } else {
    return pColumnInfoData->nullbitmap ? colDataIsNull_f(pColumnInfoData, row) : false;
  }
}

static FORCE_INLINE bool colDataIsNull(const SColumnInfoData* pColumnInfoData, uint32_t totalRows, uint32_t row,
                                       SColumnDataAgg* pColAgg) {
  if (!pColumnInfoData->hasNull) {
    return false;
  }

  if (pColAgg != NULL && pColAgg->colId != -1) {
    if (pColAgg->numOfNull == totalRows) {
      return true;
    } else if (pColAgg->numOfNull == 0) {
      return false;
    }
  }

  if (IS_VAR_DATA_TYPE(pColumnInfoData->info.type)) {
    return colDataIsNull_var(pColumnInfoData, row);
  } else {
    if (pColumnInfoData->nullbitmap == NULL) {
      return false;
    }

    return colDataIsNull_f(pColumnInfoData, row);
  }
}

static FORCE_INLINE void colDataSetNULL(SColumnInfoData* pColumnInfoData, uint32_t rowIndex) {
  // There is a placehold for each NULL value of binary or nchar type.
  if (IS_VAR_DATA_TYPE(pColumnInfoData->info.type)) {
    colDataSetNull_var(pColumnInfoData, rowIndex);  // it is a null value of VAR type.
  } else {
    colDataSetNull_f_s(pColumnInfoData, rowIndex);
  }

  pColumnInfoData->hasNull = true;
}

static FORCE_INLINE void colDataSetNNULL(SColumnInfoData* pColumnInfoData, uint32_t start, size_t nRows) {
  if (IS_VAR_DATA_TYPE(pColumnInfoData->info.type)) {
    for (int32_t i = start; i < start + nRows; ++i) {
      colDataSetNull_var(pColumnInfoData, i);  // it is a null value of VAR type.
    }
  } else {
    for (int32_t i = start; i < start + nRows; ++i) {
      colDataSetNull_f(pColumnInfoData->nullbitmap, i);
    }
    (void)memset(pColumnInfoData->pData + start * pColumnInfoData->info.bytes, 0, pColumnInfoData->info.bytes * nRows);
  }

  pColumnInfoData->hasNull = true;
}

static FORCE_INLINE void colDataSetInt8(SColumnInfoData* pColumnInfoData, uint32_t rowIndex, int8_t* v) {
  char* p = pColumnInfoData->pData + pColumnInfoData->info.bytes * rowIndex;
  *(int8_t*)p = *(int8_t*)v;
}

static FORCE_INLINE void colDataSetInt16(SColumnInfoData* pColumnInfoData, uint32_t rowIndex, int16_t* v) {
  char* p = pColumnInfoData->pData + pColumnInfoData->info.bytes * rowIndex;
  *(int16_t*)p = *(int16_t*)v;
}

static FORCE_INLINE void colDataSetInt32(SColumnInfoData* pColumnInfoData, uint32_t rowIndex, int32_t* v) {
  char* p = pColumnInfoData->pData + pColumnInfoData->info.bytes * rowIndex;
  *(int32_t*)p = *(int32_t*)v;
}

static FORCE_INLINE void colDataSetInt64(SColumnInfoData* pColumnInfoData, uint32_t rowIndex, int64_t* v) {
  int32_t type = pColumnInfoData->info.type;
  char*   p = pColumnInfoData->pData + pColumnInfoData->info.bytes * rowIndex;
  taosSetPInt64Aligned((int64_t*)p, v);
}

static FORCE_INLINE void colDataSetFloat(SColumnInfoData* pColumnInfoData, uint32_t rowIndex, float* v) {
  char* p = pColumnInfoData->pData + pColumnInfoData->info.bytes * rowIndex;
  taosSetPFloatAligned((float*)p, v);
}

static FORCE_INLINE void colDataSetDouble(SColumnInfoData* pColumnInfoData, uint32_t rowIndex, double* v) {
  char* p = pColumnInfoData->pData + pColumnInfoData->info.bytes * rowIndex;
  taosSetPDoubleAligned((double*)p, v);
}

int32_t getJsonValueLen(const char* data);

// For the VAR_DATA_TYPE type, new data is inserted strictly according to the position of SVarColAttr.length.
// If the same row is inserted repeatedly, data holes will result.
int32_t colDataSetVal(SColumnInfoData* pColumnInfoData, uint32_t rowIndex, const char* pData, bool isNull);
// For the VAR_DATA_TYPE type, if a row already has data before inserting it (judged by offset != -1),
// it will be inserted at the original position and the old data will be overwritten.
int32_t colDataSetValOrCover(SColumnInfoData* pColumnInfoData, uint32_t rowIndex, const char* pData, bool isNull);
int32_t varColSetVarData(SColumnInfoData* pColumnInfoData, uint32_t rowIndex, const char* pVarData, int32_t varDataLen,
                         bool isNull);
int32_t colDataReassignVal(SColumnInfoData* pColumnInfoData, uint32_t dstRowIdx, uint32_t srcRowIdx, const char* pData);
int32_t colDataSetNItems(SColumnInfoData* pColumnInfoData, uint32_t rowIndex, const char* pData, uint32_t numOfRows, uint32_t capacity,
                         bool trimValue);
int32_t doCopyNItems(struct SColumnInfoData* pColumnInfoData, int32_t currentRow, const char* pData,
                            int32_t itemLen, int32_t numOfRows, bool trimValue);                         
void    colDataSetNItemsNull(SColumnInfoData* pColumnInfoData, uint32_t currentRow, uint32_t numOfRows);
int32_t colDataCopyNItems(SColumnInfoData* pColumnInfoData, uint32_t currentRow, const char* pData, uint32_t numOfRows,
                          bool isNull);
int32_t colDataMergeCol(SColumnInfoData* pColumnInfoData, int32_t numOfRow1, int32_t* capacity,
                        const SColumnInfoData* pSource, int32_t numOfRow2);
int32_t colDataAssign(SColumnInfoData* pColumnInfoData, const SColumnInfoData* pSource, int32_t numOfRows,
                      const SDataBlockInfo* pBlockInfo);
int32_t colDataAssignNRows(SColumnInfoData* pDst, int32_t dstIdx, const SColumnInfoData* pSrc, int32_t srcIdx,
                           int32_t numOfRows);
void    blockDataUpdateHasNull(SSDataBlock* pDataBlock);
int32_t blockDataUpdateTsWindow(SSDataBlock* pDataBlock, int32_t tsColumnIndex);
int32_t blockDataUpdatePkRange(SSDataBlock* pDataBlock, int32_t pkColumnIndex, bool asc);

// Recomputes hasNull from the column's null metadata - the offset array
// for var-length types, the null bitmap otherwise. Any decoder that loads
// that metadata from a serialized buffer must call this: the target block
// is reused across blocks, so its hasNull is whatever the previous block
// left (blockDataCleanup clears it), and a stale false makes
// colDataIsNull_s report every row as non-NULL without ever reading the
// metadata. Touches only the metadata, so it may be called before the
// payload is allocated.
void    colDataUpdateHasNull(SColumnInfoData* pColumnInfoData,
                             int32_t numOfRows);
int32_t colDataGetLength(const SColumnInfoData* pColumnInfoData, int32_t numOfRows);

int32_t colDataGetRowLength(const SColumnInfoData* pColumnInfoData, int32_t rowIdx);
void    colDataTrim(SColumnInfoData* pColumnInfoData);

size_t blockDataGetNumOfCols(const SSDataBlock* pBlock);
size_t blockDataGetNumOfRows(const SSDataBlock* pBlock);

void blockDataTransform(SSDataBlock* pDest, const SSDataBlock* pSrc);
int32_t blockDataMerge(SSDataBlock* pDest, const SSDataBlock* pSrc);
int32_t blockDataMergeNRows(SSDataBlock* pDest, const SSDataBlock* pSrc, int32_t srcIdx, int32_t numOfRows);
bool    blockDataBlobExceedLimit(const SSDataBlock* pBlock);
bool    blockDataMergeExceedLimit(const SSDataBlock* pDest, const SSDataBlock* pSrc);
void    blockDataShrinkNRows(SSDataBlock* pBlock, int32_t numOfRows);
int32_t blockDataSplitRows(SSDataBlock* pBlock, bool hasVarCol, int32_t startIndex, int32_t* stopIndex,
                           int32_t pageSize);
int32_t blockDataToBuf(char* buf, const SSDataBlock* pBlock);
// Exact number of bytes blockDataToBuf writes for pBlock; sizing a buffer
// with anything else risks drifting from the serialized layout.
size_t blockDataGetBufSize(const SSDataBlock* pBlock);
// Reads back the compact layout blockDataToBuf writes: a row count
// followed by per-column regions packed to that row count with no slack.
// Sizes pBlock itself.
int32_t blockDataFromBuf(SSDataBlock* pBlock, const char* buf);
// Decode numOfCols blockDataToBuf columns while verifying that every read stays within capacity.
int32_t blockDataFromBufWithCapacity(SSDataBlock* pBlock, const char* buf, size_t capacity, int32_t numOfCols);
// Bounded variant for deserializing from untrusted storage (e.g. spill
// files): validates the full serialized layout against bufSize before any
// allocation or copy, returning TSDB_CODE_FILE_CORRUPTED on violations.
// bufSize must be the exact serialized length; trailing bytes are rejected.
// Schemas containing noData columns are rejected with TSDB_CODE_INVALID_PARA
// (blockDataToBuf serializes them but blockDataFromBuf skips them, so no
// consistent bounded parse exists).
int32_t blockDataFromBufBounded(SSDataBlock* pBlock, const char* buf,
                                int32_t bufSize);
int32_t blockDataExtractBlock(SSDataBlock* pBlock, int32_t startIndex, int32_t rowCount, SSDataBlock** pResBlock);

size_t blockDataGetSize(const SSDataBlock* pBlock);
size_t blockDataGetMemorySize(const SSDataBlock* pBlock);
size_t blockDataGetRowSize(SSDataBlock* pBlock);
double blockDataGetSerialRowSize(const SSDataBlock* pBlock);
size_t blockDataGetSerialMetaSize(uint32_t numOfCols);

int32_t blockDataSort(SSDataBlock* pDataBlock, SArray* pOrderInfo);
/**
 * @brief find how many rows already in order start from first row
 */
int32_t blockDataGetSortedRows(SSDataBlock* pDataBlock, SArray* pOrderInfo);
int32_t blockDataCheck(const SSDataBlock* pDataBlock);

int32_t colInfoDataEnsureCapacity(SColumnInfoData* pColumn, uint32_t numOfRows, bool clearPayload);
int32_t blockDataEnsureCapacity(SSDataBlock* pDataBlock, uint32_t numOfRows);

void    colInfoDataCleanup(SColumnInfoData* pColumn, uint32_t numOfRows);
void    blockDataCleanup(SSDataBlock* pDataBlock);
void    blockDataReset(SSDataBlock* pDataBlock);
void    blockDataEmpty(SSDataBlock* pDataBlock);
int32_t doEnsureCapacity(SColumnInfoData* pColumn, const SDataBlockInfo* pBlockInfo, uint32_t numOfRows,
                         bool clearPayload);

size_t blockDataGetCapacityInRow(const SSDataBlock* pBlock, size_t pageSize, int32_t extraSize);

int32_t blockDataTrimFirstRows(SSDataBlock* pBlock, size_t n);
void    blockDataKeepFirstNRows(SSDataBlock* pBlock, size_t n);

int32_t assignOneDataBlock(SSDataBlock* dst, const SSDataBlock* src);
int32_t copyDataBlock(SSDataBlock* pDst, const SSDataBlock* pSrc);

int32_t createDataBlock(SSDataBlock** pResBlock);
void    blockDataDestroy(SSDataBlock* pBlock);
// Recycle a completed block for later reuse. The block must no longer be referenced by any operator.
void    blockDataRecycle(SSDataBlock* pBlock);
// Updates the coarse clock used when a completed block enters the recycle list.
void    blockDataRecycleSetCurrentTime(int64_t currentTime);
// Releases recycle-list blocks that have been idle for at least BLOCK_RECYCLE_TIMEOUT_MS.
void    blockDataRecycleTrimExpired(int64_t currentTime);
// Releases every recycle-list block after query execution has stopped during process shutdown.
void    blockDataRecycleCleanup(void);
void    blockDataFreeRes(SSDataBlock* pBlock);
void    blockDataFreeCols(SSDataBlock* pBlock);
int32_t createOneDataBlock(const SSDataBlock* pDataBlock, bool copyData, SSDataBlock** pResBlock);
// Acquire an empty block with the template schema, reusing retained column capacity when available.
int32_t createOneDataBlockFromRecycle(const SSDataBlock* pDataBlock, SSDataBlock** pResBlock, bool* pReused);
int32_t createOneDataBlockWithColArray(const SSDataBlock* pDataBlock, SArray* pColArray, SSDataBlock** pResBlock);
int32_t createOneDataBlockWithTwoBlock(const SSDataBlock* pSrcBlock, const SSDataBlock* pTemplateBlock, SArray* pColMap,
                                       SSDataBlock** pResBlock);

int32_t blockCopyOneRow(const SSDataBlock* pDataBlock, int32_t rowIdx, SSDataBlock** pResBlock);
int32_t blockDataAppendColInfo(SSDataBlock* pBlock, SColumnInfoData* pColInfoData);

SColumnInfoData createColumnInfoData(int16_t type, int32_t bytes, int16_t colId);
int32_t         bdGetColumnInfoData(const SSDataBlock* pBlock, int32_t index, SColumnInfoData** pColInfoData);

int32_t blockGetEncodeSize(const SSDataBlock* pBlock);
int32_t blockGetInternalEncodeSize(const SSDataBlock* pBlock);
int32_t blockEncodeInternal(const SSDataBlock* pBlock, char* data, size_t dataBuflen, int32_t numOfCols);
int32_t blockEncode(const SSDataBlock* pBlock, char* data, size_t dataLen, int32_t numOfCols);
int32_t blockDecodeInternal(SSDataBlock* pBlock, const char* pData, const char** pEndPos);
int32_t blockDecodeInternalBounded(SSDataBlock* pBlock, const char* pData, size_t dataLen, const char** pEndPos);
int32_t blockIsSingleNullColumnEncoded(const char* pData, size_t dataLen, bool* pIsNull);
int32_t blockDecode(SSDataBlock* pBlock, const char* pData, const char** pEndPos);
// Decodes one normal block from pData[0, dataLen). On success, pEndPos, when non-NULL, points to the byte after the
// encoded block; callers retain ownership of pBlock and must release it with blockDataDestroy.
int32_t blockDecodeBounded(SSDataBlock* pBlock, const char* pData, size_t dataLen, const char** pEndPos);

// for debug
int32_t dumpBlockData(SSDataBlock* pDataBlock, const char* flag, char** dumpBuf, const char* taskIdStr, int64_t qId);

int32_t buildSubmitReqFromDataBlock(SSubmitReq2** pReq, const SSDataBlock* pDataBlocks, const STSchema* pTSchema,
                                    int64_t uid, int32_t vgId, tb_uid_t suid);

bool    alreadyAddGroupId(char* ctbName, int64_t groupId);
bool    isAutoTableName(char* ctbName);
int32_t buildCtbNameAddGroupId(const char* stbName, char* ctbName, uint64_t groupId, size_t cap);
int32_t buildCtbNameByGroupId(const char* stbName, uint64_t groupId, char** pName);
int32_t buildCtbNameByGroupIdImpl(const char* stbName, uint64_t groupId, char* pBuf);
int32_t buildSinkDestTableName(char* parTbName, const char* stbFullName, uint64_t gid, bool newSubTableRule,
                               char** dstTableName);

int32_t trimDataBlock(SSDataBlock* pBlock, int32_t totalRows, const bool* pBoolList);
int32_t copyPkVal(SDataBlockInfo* pDst, const SDataBlockInfo* pSrc);

int32_t calcStrBytesByType(int8_t type, char* data);
int32_t blockGetEncodeSizeOfRows(const SSDataBlock* pBlock, int32_t startIndex, int32_t endIndex);
int32_t blockEncodeAsRows(const SSDataBlock* pBlock, char* data, size_t dataLen, int32_t numOfCols, int32_t startIndex,
                          int32_t endIndex, int32_t* pLen);
int32_t blockSpecialDecodeLaterPart(SSDataBlock* pBlock, const char* pData, int32_t tsColSlotId, TSKEY start, TSKEY end);
// Decodes rows whose timestamp is within [start, end] from one normal block in pData[0, dataLen).
// pEndPos is optional and has the same meaning as blockDecodeBounded; callers retain ownership of pBlock and must
// release it with blockDataDestroy.
int32_t blockSpecialDecodeLaterPartBounded(SSDataBlock* pBlock, const char* pData, size_t dataLen, int32_t tsColSlotId,
                                           TSKEY start, TSKEY end, const char** pEndPos);
int32_t getStreamBlockTS(SSDataBlock* pBlock, int32_t tsColSlotId, int32_t row, TSKEY* ts);

#ifdef __cplusplus
}
#endif

#endif /*_TD_COMMON_EP_H_*/
