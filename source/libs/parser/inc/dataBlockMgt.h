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

#ifndef TDENGINE_DATABLOCKMGT_H
#define TDENGINE_DATABLOCKMGT_H

#include "catalog.h"
#include "os.h"
#include "ttypes.h"
#include "tname.h"

#define IS_DATA_COL_ORDERED(spd) ((spd->orderStatus) == (int8_t)ORDER_STATUS_ORDERED)

typedef enum EOrderStatus {
  ORDER_STATUS_UNKNOWN = 0,
  ORDER_STATUS_ORDERED = 1,
  ORDER_STATUS_DISORDERED = 2,
} EOrderStatus;

typedef enum EValStat {
  VAL_STAT_HAS = 0x0,    // 0 means has val
  VAL_STAT_NONE = 0x01,  // 1 means no val
} EValStat;

typedef enum ERowCompareStat {
  ROW_COMPARE_NO_NEED = 0,
  ROW_COMPARE_NEED = 1,
} ERowCompareStat;

typedef struct SBoundColumn {
  int32_t offset;   // all column offset value
  int32_t toffset;  // first part offset for SDataRow TODO: get offset from STSchema on future
  uint8_t valStat;  // EValStat. denote if current column bound or not(0 means has val, 1 means no val)
} SBoundColumn;

typedef struct {
  uint16_t schemaColIdx;
  uint16_t boundIdx;
  uint16_t finalIdx;
} SBoundIdxInfo;

typedef struct SParsedDataColInfo {
  int16_t        numOfCols;
  int16_t        numOfBound;
  uint16_t       flen;        // TODO: get from STSchema
  uint16_t       allNullLen;  // TODO: get from STSchema
  uint16_t       extendedVarLen;
  int32_t        *boundedColumns;  // bound column idx according to schema
  SBoundColumn   *cols;
  SBoundIdxInfo  *colIdxInfo;
  int8_t         orderStatus;  // bound columns
} SParsedDataColInfo;

typedef struct SMemRowInfo {
  int32_t dataLen;  // len of SDataRow
  int32_t kvLen;    // len of SKVRow
} SMemRowInfo;

typedef struct {
  uint8_t      memRowType;   // default is 0, that is SDataRow 
  uint8_t      compareStat;  // 0 no need, 1 need compare
  TDRowTLenT   kvRowInitLen;
  SMemRowInfo *rowInfo;
} SMemRowBuilder;

typedef struct STableDataBlocks {
  int8_t      tsSource;     // where does the UNIX timestamp come from, server or client
  bool        ordered;      // if current rows are ordered or not
  int32_t     vgId;         // virtual group id
  int64_t     prevTS;       // previous timestamp, recorded to decide if the records array is ts ascending
  int32_t     numOfTables;  // number of tables in current submit block
  int32_t     rowSize;      // row size for current table
  uint32_t    nAllocSize;
  uint32_t    headerSize;   // header for table info (uid, tid, submit metadata)
  uint32_t    size;
  STableMeta *pTableMeta;   // the tableMeta of current table, the table meta will be used during submit, keep a ref to avoid to be removed from cache
  char       *pData;
  bool        cloned;
  STagData    tagData; 
  
  SParsedDataColInfo boundColumnInfo;
  SMemRowBuilder rowBuilder;
} STableDataBlocks;

static FORCE_INLINE void initSMemRow(SMemRow row, uint8_t memRowType, STableDataBlocks *pBlock, int16_t nBoundCols) {
  memRowSetType(row, memRowType);
  if (isDataRowT(memRowType)) {
    dataRowSetVersion(memRowDataBody(row), pBlock->pTableMeta->sversion);
    dataRowSetLen(memRowDataBody(row), (TDRowLenT)(TD_DATA_ROW_HEAD_SIZE + pBlock->boundColumnInfo.flen));
  } else {
    ASSERT(nBoundCols > 0);
    memRowSetKvVersion(row, pBlock->pTableMeta->sversion);
    kvRowSetNCols(memRowKvBody(row), nBoundCols);
    kvRowSetLen(memRowKvBody(row), (TDRowLenT)(TD_KV_ROW_HEAD_SIZE + sizeof(SColIdx) * nBoundCols));
  }
}

static FORCE_INLINE int32_t getExtendedRowSize(STableDataBlocks *pBlock) {
  ASSERT(pBlock->rowSize == pBlock->pTableMeta->tableInfo.rowSize);
  return pBlock->rowSize + TD_MEM_ROW_DATA_HEAD_SIZE + pBlock->boundColumnInfo.extendedVarLen;
}

// Applicable to consume by one row
static FORCE_INLINE void appendMemRowColValEx(SMemRow row, const void *value, bool isCopyVarData, int16_t colId,
                                                 int8_t colType, int32_t toffset, int32_t *dataLen, int32_t *kvLen,
                                                 uint8_t compareStat) {
  tdAppendMemRowColVal(row, value, isCopyVarData, colId, colType, toffset);
  if (compareStat == ROW_COMPARE_NEED) {
    tdGetColAppendDeltaLen(value, colType, dataLen, kvLen);
  }
}

static FORCE_INLINE void getMemRowAppendInfo(SSchema *pSchema, uint8_t memRowType, SParsedDataColInfo *spd,
                                                int32_t idx, int32_t *toffset) {
  int32_t schemaIdx = 0;
  if (IS_DATA_COL_ORDERED(spd)) {
    schemaIdx = spd->boundedColumns[idx] - 1;
    if (isDataRowT(memRowType)) {
      *toffset = (spd->cols + schemaIdx)->toffset;  // the offset of firstPart
    } else {
      *toffset = idx * sizeof(SColIdx);  // the offset of SColIdx
    }
  } else {
    ASSERT(idx == (spd->colIdxInfo + idx)->boundIdx);
    schemaIdx = (spd->colIdxInfo + idx)->schemaColIdx;
    if (isDataRowT(memRowType)) {
      *toffset = (spd->cols + schemaIdx)->toffset;
    } else {
      *toffset = ((spd->colIdxInfo + idx)->finalIdx) * sizeof(SColIdx);
    }
  }
}

static FORCE_INLINE void convertMemRow(SMemRow row, int32_t dataLen, int32_t kvLen) {
  if (isDataRow(row)) {
    if (kvLen < (dataLen * KVRatioConvert)) {
      memRowSetConvert(row);
    }
  } else if (kvLen > dataLen) {
    memRowSetConvert(row);
  }
}

static FORCE_INLINE int32_t setBlockInfo(SSubmitBlk *pBlocks, const STableMeta *pTableMeta, int32_t numOfRows) {
  pBlocks->tid = pTableMeta->suid;
  pBlocks->uid = pTableMeta->uid;
  pBlocks->sversion = pTableMeta->sversion;

  if (pBlocks->numOfRows + numOfRows >= INT16_MAX) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  } else {
    pBlocks->numOfRows += numOfRows;
    return TSDB_CODE_SUCCESS;
  }
}

int32_t schemaIdxCompar(const void *lhs, const void *rhs);
int32_t boundIdxCompar(const void *lhs, const void *rhs);
void setBoundColumnInfo(SParsedDataColInfo* pColList, SSchema* pSchema, int32_t numOfCols);
void destroyBoundColumnInfo(SParsedDataColInfo* pColList);
void destroyBlockArrayList(SArray* pDataBlockList);
int32_t initMemRowBuilder(SMemRowBuilder *pBuilder, uint32_t nRows, uint32_t nCols, uint32_t nBoundCols, int32_t allNullLen);
int32_t allocateMemIfNeed(STableDataBlocks *pDataBlock, int32_t rowSize, int32_t * numOfRows);
int32_t getDataBlockFromList(SHashObj* pHashList, int64_t id, int32_t size, int32_t startOffset, int32_t rowSize,
    const STableMeta* pTableMeta, STableDataBlocks** dataBlocks, SArray* pBlockList);
int32_t mergeTableDataBlocks(SHashObj* pHashObj, int8_t schemaAttached, uint8_t payloadType, SArray** pVgDataBlocks);

#endif  // TDENGINE_DATABLOCKMGT_H
