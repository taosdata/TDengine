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

#ifndef TDENGINE_PAR_INSERT_UTIL_H
#define TDENGINE_PAR_INSERT_UTIL_H

#include "parUtil.h"

struct SToken;

#define IS_DATA_COL_ORDERED(spd) ((spd->orderStatus) == (int8_t)ORDER_STATUS_ORDERED)

#define NEXT_TOKEN(pSql, sToken)                      \
  do {                                                \
    int32_t index = 0;                                \
    sToken = tStrGetToken(pSql, &index, false, NULL); \
    pSql += index;                                    \
  } while (0)

#define CHECK_CODE(expr)             \
  do {                               \
    int32_t code = expr;             \
    if (TSDB_CODE_SUCCESS != code) { \
      return code;                   \
    }                                \
  } while (0)

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

typedef struct SInsertParseBaseContext {
  SParseContext *pComCxt;
  char          *pSql;
  SMsgBuf        msg;
} SInsertParseBaseContext;

typedef struct SInsertParseSyntaxCxt {
  SParseContext   *pComCxt;
  char            *pSql;
  SMsgBuf          msg;
  SParseMetaCache *pMetaCache;
} SInsertParseSyntaxCxt;

typedef struct SMemParam {
  SRowBuilder *rb;
  SSchema     *schema;
  int32_t      toffset;
  col_id_t     colIdx;
} SMemParam;

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

int32_t insGetExtendedRowSize(STableDataBlocks *pBlock);
void insGetSTSRowAppendInfo(uint8_t rowType, SParsedDataColInfo *spd, col_id_t idx, int32_t *toffset, col_id_t *colIdx);
int32_t insSetBlockInfo(SSubmitBlk *pBlocks, STableDataBlocks *dataBuf, int32_t numOfRows, SMsgBuf *pMsg);
int32_t insSchemaIdxCompar(const void *lhs, const void *rhs);
int32_t insBoundIdxCompar(const void *lhs, const void *rhs);
void    insSetBoundColumnInfo(SParsedDataColInfo *pColList, SSchema *pSchema, col_id_t numOfCols);
void    insDestroyBlockArrayList(SArray *pDataBlockList);
void    insDestroyBlockHashmap(SHashObj *pDataBlockHash);
int32_t insInitRowBuilder(SRowBuilder *pBuilder, int16_t schemaVer, SParsedDataColInfo *pColInfo);
int32_t insGetDataBlockFromList(SHashObj *pHashList, void *id, int32_t idLen, int32_t size, int32_t startOffset,
                                int32_t rowSize, STableMeta *pTableMeta, STableDataBlocks **dataBlocks,
                                SArray *pBlockList, SVCreateTbReq *pCreateTbReq);
int32_t insMergeTableDataBlocks(SHashObj *pHashObj, SArray **pVgDataBlocks);
int32_t insBuildCreateTbMsg(STableDataBlocks *pBlocks, SVCreateTbReq *pCreateTbReq);
int32_t insAllocateMemForSize(STableDataBlocks *pDataBlock, int32_t allSize);
int32_t insCreateSName(SName *pName, struct SToken *pTableName, int32_t acctId, const char *dbName, SMsgBuf *pMsgBuf);
int32_t insFindCol(struct SToken *pColname, int32_t start, int32_t end, SSchema *pSchema);
void    insBuildCreateTbReq(SVCreateTbReq *pTbReq, const char *tname, STag *pTag, int64_t suid, const char *sname,
                            SArray *tagName, uint8_t tagNum, int32_t ttl);
int32_t insMemRowAppend(SMsgBuf *pMsgBuf, const void *value, int32_t len, void *param);
int32_t insCheckTimestamp(STableDataBlocks *pDataBlocks, const char *start);
int32_t insBuildOutput(SHashObj *pVgroupsHashObj, SArray *pVgDataBlocks, SArray **pDataBlocks);
void    insDestroyDataBlock(STableDataBlocks *pDataBlock);

#endif  // TDENGINE_PAR_INSERT_UTIL_H
