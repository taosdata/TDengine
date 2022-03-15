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
#include "tmsg.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SCorEpSet {
  int32_t version;
  SEpSet  epSet;
} SCorEpSet;

typedef struct SBlockOrderInfo {
  int32_t          order;
  int32_t          colIndex;
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
    return pColumnInfoData->varmeta.offset[row] == -1;
  } else {
    if (pColumnInfoData->nullbitmap == NULL) {
      return false;
    }

    return colDataIsNull_f(pColumnInfoData->nullbitmap, row);
  }
}

#define BitmapLen(_n)     (((_n) + ((1<<NBIT)-1)) >> NBIT)


#define colDataGetData(p1_, r_)                                                          \
  ((IS_VAR_DATA_TYPE((p1_)->info.type)) ? ((p1_)->pData + (p1_)->varmeta.offset[(r_)]) \
                                        : ((p1_)->pData + ((r_) * (p1_)->info.bytes)))

int32_t colDataAppend(SColumnInfoData* pColumnInfoData, uint32_t currentRow, const char* pData, bool isNull);
int32_t colDataMergeCol(SColumnInfoData* pColumnInfoData, uint32_t numOfRow1, const SColumnInfoData* pSource,
                        uint32_t numOfRow2);
int32_t blockDataUpdateTsWindow(SSDataBlock* pDataBlock);

int32_t colDataGetLength(const SColumnInfoData* pColumnInfoData, int32_t numOfRows);
void    colDataTrim(SColumnInfoData* pColumnInfoData);

size_t blockDataGetNumOfCols(const SSDataBlock* pBlock);
size_t blockDataGetNumOfRows(const SSDataBlock* pBlock);

int32_t      blockDataMerge(SSDataBlock* pDest, const SSDataBlock* pSrc);
int32_t      blockDataSplitRows(SSDataBlock* pBlock, bool hasVarCol, int32_t startIndex, int32_t* stopIndex,
                                int32_t pageSize);
SSDataBlock* blockDataExtractBlock(SSDataBlock* pBlock, int32_t startIndex, int32_t rowCount);

int32_t blockDataToBuf(char* buf, const SSDataBlock* pBlock);
int32_t blockDataFromBuf(SSDataBlock* pBlock, const char* buf);

size_t blockDataGetSize(const SSDataBlock* pBlock);
size_t blockDataGetRowSize(const SSDataBlock* pBlock);
double blockDataGetSerialRowSize(const SSDataBlock* pBlock);
size_t blockDataGetSerialMetaSize(const SSDataBlock* pBlock);

SSchema* blockDataExtractSchema(const SSDataBlock* pBlock, int32_t* numOfCols);

int32_t blockDataSort(SSDataBlock* pDataBlock, SArray* pOrderInfo, bool nullFirst);
int32_t blockDataSort_rv(SSDataBlock* pDataBlock, SArray* pOrderInfo, bool nullFirst);

int32_t      blockDataEnsureColumnCapacity(SColumnInfoData* pColumn, uint32_t numOfRows);
int32_t      blockDataEnsureCapacity(SSDataBlock* pDataBlock, uint32_t numOfRows);
void         blockDataClearup(SSDataBlock* pDataBlock);
SSDataBlock* createOneDataBlock(const SSDataBlock* pDataBlock);
size_t       blockDataGetCapacityInRow(const SSDataBlock* pBlock, size_t pageSize);
void*        blockDataDestroy(SSDataBlock* pBlock);

#ifdef __cplusplus
}
#endif

#endif  /*_TD_COMMON_EP_H_*/
