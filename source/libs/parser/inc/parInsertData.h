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
#include "tname.h"
#include "ttypes.h"
#include "query.h"

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
  col_id_t schemaColIdx;
  col_id_t boundIdx;
  col_id_t finalIdx;
} SBoundIdxInfo;

typedef struct SParsedDataColInfo {
  col_id_t       numOfCols;
  col_id_t       numOfBound;
  uint16_t       flen;        // TODO: get from STSchema
  uint16_t       allNullLen;  // TODO: get from STSchema(base on SDataRow)
  uint16_t       extendedVarLen;
  uint16_t       boundNullLen;  // bound column len with all NULL value(without VarDataOffsetT/SColIdx part)
  col_id_t      *boundColumns;  // bound column idx according to schema
  SBoundColumn  *cols;
  SBoundIdxInfo *colIdxInfo;
  int8_t         orderStatus;  // bound columns
} SParsedDataColInfo;

typedef struct {
  uint8_t rowType;  // default is 0, that is SDataRow
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
  uint32_t    headerSize;  // header for table info (uid, tid, submit metadata)
  uint32_t    size;
  STableMeta *pTableMeta;  // the tableMeta of current table, the table meta will be used during submit, keep a ref to
                           // avoid to be removed from cache
  char              *pData;
  bool               cloned;
  int32_t            createTbReqLen;
  SParsedDataColInfo boundColumnInfo;
  SRowBuilder        rowBuilder;
} STableDataBlocks;

static FORCE_INLINE int32_t getExtendedRowSize(STableDataBlocks *pBlock) {
  STableComInfo *pTableInfo = &pBlock->pTableMeta->tableInfo;
  ASSERT(pBlock->rowSize == pTableInfo->rowSize);
  return pBlock->rowSize + TD_ROW_HEAD_LEN - sizeof(TSKEY) + pBlock->boundColumnInfo.extendedVarLen +
         (int32_t)TD_BITMAP_BYTES(pTableInfo->numOfColumns - 1);
}

static FORCE_INLINE void getSTSRowAppendInfo(uint8_t rowType, SParsedDataColInfo *spd, col_id_t idx, int32_t *toffset,
                                             col_id_t *colIdx) {
  col_id_t schemaIdx = 0;
  if (IS_DATA_COL_ORDERED(spd)) {
    schemaIdx = spd->boundColumns[idx];
    if (TD_IS_TP_ROW_T(rowType)) {
      *toffset = (spd->cols + schemaIdx)->toffset;  // the offset of firstPart
      *colIdx = schemaIdx;
    } else {
      *toffset = idx * sizeof(SKvRowIdx);  // the offset of SKvRowIdx
      *colIdx = idx;
    }
  } else {
    ASSERT(idx == (spd->colIdxInfo + idx)->boundIdx);
    schemaIdx = (spd->colIdxInfo + idx)->schemaColIdx;
    if (TD_IS_TP_ROW_T(rowType)) {
      *toffset = (spd->cols + schemaIdx)->toffset;
      *colIdx = schemaIdx;
    } else {
      *toffset = ((spd->colIdxInfo + idx)->finalIdx) * sizeof(SKvRowIdx);
      *colIdx = (spd->colIdxInfo + idx)->finalIdx;
    }
  }
}

static FORCE_INLINE int32_t setBlockInfo(SSubmitBlk *pBlocks, STableDataBlocks *dataBuf, int32_t numOfRows) {
  pBlocks->suid = (TSDB_NORMAL_TABLE == dataBuf->pTableMeta->tableType ? 0 : dataBuf->pTableMeta->suid);
  pBlocks->uid = dataBuf->pTableMeta->uid;
  pBlocks->sversion = dataBuf->pTableMeta->sversion;
  pBlocks->schemaLen = dataBuf->createTbReqLen;

  if (pBlocks->numOfRows + numOfRows >= INT16_MAX) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  } else {
    pBlocks->numOfRows += numOfRows;
    return TSDB_CODE_SUCCESS;
  }
}

int32_t schemaIdxCompar(const void *lhs, const void *rhs);
int32_t boundIdxCompar(const void *lhs, const void *rhs);
void    setBoundColumnInfo(SParsedDataColInfo *pColList, SSchema *pSchema, col_id_t numOfCols);
void    destroyBlockArrayList(SArray *pDataBlockList);
void    destroyBlockHashmap(SHashObj *pDataBlockHash);
int     initRowBuilder(SRowBuilder *pBuilder, int16_t schemaVer, SParsedDataColInfo *pColInfo);
int32_t allocateMemIfNeed(STableDataBlocks *pDataBlock, int32_t rowSize, int32_t *numOfRows);
int32_t getDataBlockFromList(SHashObj *pHashList, void *id, int32_t idLen, int32_t size, int32_t startOffset,
                             int32_t rowSize, STableMeta *pTableMeta, STableDataBlocks **dataBlocks, SArray *pBlockList,
                             SVCreateTbReq *pCreateTbReq);
int32_t mergeTableDataBlocks(SHashObj *pHashObj, uint8_t payloadType, SArray **pVgDataBlocks);
int32_t buildCreateTbMsg(STableDataBlocks *pBlocks, SVCreateTbReq *pCreateTbReq);

int32_t allocateMemForSize(STableDataBlocks *pDataBlock, int32_t allSize);

#endif  // TDENGINE_DATABLOCKMGT_H
