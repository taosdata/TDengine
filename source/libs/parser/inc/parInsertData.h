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
  uint16_t       allNullLen;  // TODO: get from STSchema(base on SDataRow)
  uint16_t       extendedVarLen;
  uint16_t       boundNullLen;    // bound column len with all NULL value(without VarDataOffsetT/SColIdx part)
  int32_t *      boundedColumns;  // bound column idx according to schema
  SBoundColumn * cols;
  SBoundIdxInfo *colIdxInfo;
  int8_t         orderStatus;  // bound columns
} SParsedDataColInfo;

typedef struct {
  uint8_t memRowType;  // default is 0, that is SDataRow
  int32_t rowSize;
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
  char        tableName[TSDB_TABLE_NAME_LEN];
  char        dbFName[TSDB_DB_FNAME_LEN];
  
  SParsedDataColInfo boundColumnInfo;
  SRowBuilder        rowBuilder;
} STableDataBlocks;

static FORCE_INLINE int32_t getExtendedRowSize(STableDataBlocks *pBlock) {
  STableComInfo *pTableInfo = &pBlock->pTableMeta->tableInfo;
  ASSERT(pBlock->rowSize == pTableInfo->rowSize);
  return pBlock->rowSize + TD_ROW_HEAD_LEN - sizeof(TSKEY) + pBlock->boundColumnInfo.extendedVarLen +
         (int32_t)TD_BITMAP_BYTES(pTableInfo->numOfColumns - 1);
}

static FORCE_INLINE void getMemRowAppendInfo(SSchema *pSchema, uint8_t rowType, SParsedDataColInfo *spd,
                                                int32_t idx, int32_t *toffset, int32_t *colIdx) {
  int32_t schemaIdx = 0;
  if (IS_DATA_COL_ORDERED(spd)) {
    schemaIdx = spd->boundedColumns[idx] - PRIMARYKEY_TIMESTAMP_COL_ID;
    if (TD_IS_TP_ROW_T(rowType)) {
      *toffset = (spd->cols + schemaIdx)->toffset;  // the offset of firstPart
      *colIdx = schemaIdx;
    } else {
      *toffset = idx * sizeof(SColIdx);  // the offset of SColIdx
      *colIdx = idx;
    }
  } else {
    ASSERT(idx == (spd->colIdxInfo + idx)->boundIdx);
    schemaIdx = (spd->colIdxInfo + idx)->schemaColIdx - PRIMARYKEY_TIMESTAMP_COL_ID;
    if (TD_IS_TP_ROW_T(rowType)) {
      *toffset = (spd->cols + schemaIdx)->toffset;
      *colIdx = schemaIdx;
    } else {
      *toffset = ((spd->colIdxInfo + idx)->finalIdx) * sizeof(SColIdx);
      *colIdx = (spd->colIdxInfo + idx)->finalIdx;
    }
  }
}

static FORCE_INLINE int32_t setBlockInfo(SSubmitBlk *pBlocks, STableDataBlocks* dataBuf, int32_t numOfRows) {
  pBlocks->tid = dataBuf->pTableMeta->suid;
  pBlocks->uid = dataBuf->pTableMeta->uid;
  pBlocks->sversion = dataBuf->pTableMeta->sversion;

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
void destroyBlockHashmap(SHashObj* pDataBlockHash);
int  initRowBuilder(SRowBuilder *pBuilder, int16_t schemaVer, SParsedDataColInfo *pColInfo);
int32_t allocateMemIfNeed(STableDataBlocks *pDataBlock, int32_t rowSize, int32_t * numOfRows);
int32_t getDataBlockFromList(SHashObj* pHashList, int64_t id, int32_t size, int32_t startOffset, int32_t rowSize,
    const STableMeta* pTableMeta, STableDataBlocks** dataBlocks, SArray* pBlockList);
int32_t mergeTableDataBlocks(SHashObj* pHashObj, int8_t schemaAttached, uint8_t payloadType, SArray** pVgDataBlocks);

#endif  // TDENGINE_DATABLOCKMGT_H
