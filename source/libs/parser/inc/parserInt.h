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

#ifndef _TD_PARSER_INT_H_
#define _TD_PARSER_INT_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "catalog.h"
#include "tname.h"
#include "astGenerator.h"

struct SSqlNode;

typedef struct SColumnIndex {
  int16_t tableIndex;
  int16_t columnIndex;
} SColumnIndex;

typedef struct SInsertStmtInfo {
  SHashObj *pTableBlockHashList;     // data block for each table
  SArray   *pDataBlocks;             // SArray<STableDataBlocks*>. Merged submit block for each vgroup
  int8_t    schemaAttached;          // denote if submit block is built with table schema or not
  uint8_t   payloadType;             // EPayloadType. 0: K-V payload for non-prepare insert, 1: rawPayload for prepare insert
  uint32_t  insertType;              // insert data from [file|sql statement| bound statement]
  char     *sql;                     // current sql statement position
} SInsertStmtInfo;

// the structure for sql function in select clause
typedef struct SSqlExpr {
  char      aliasName[TSDB_COL_NAME_LEN];  // as aliasName
  char      token[TSDB_COL_NAME_LEN];      // original token
  SColIndex colInfo;
  uint64_t  uid;            // table uid, todo refactor use the pointer

  int16_t   functionId;     // function id in aAgg array

  int16_t   resType;        // return value type
  int16_t   resBytes;       // length of return value
  int32_t   interBytes;     // inter result buffer size

  int16_t   colType;        // table column type
  int16_t   colBytes;       // table column bytes

  int16_t   numOfParams;    // argument value of each function
  SVariant  param[3];       // parameters are not more than 3
  int32_t   offset;         // sub result column value of arithmetic expression.
  int16_t   resColId;       // result column id

  SColumnFilterList flist;
} SSqlExpr;

typedef struct SExprInfo {
  SSqlExpr           base;
  struct tExprNode  *pExpr;
} SExprInfo;

typedef struct SColumn {
  uint64_t     tableUid;
  int32_t      columnIndex;
  SColumnInfo  info;
} SColumn;

typedef struct SInternalField {
  TAOS_FIELD      field;
  bool            visible;
  SExprInfo      *pExpr;
} SInternalField;

void clearTableMetaInfo(STableMetaInfo* pTableMetaInfo);

void clearAllTableMetaInfo(SQueryStmtInfo* pQueryInfo, bool removeMeta, uint64_t id);

/**
 * Validate the sql info, according to the corresponding metadata info from catalog.
 * @param pCatalog
 * @param pSqlInfo
 * @param pQueryInfo a bounded AST with essential meta data from local buffer or mgmt node
 * @param id
 * @param msg
 * @return
 */
int32_t qParserValidateSqlNode(struct SCatalog* pCatalog, SSqlInfo* pSqlInfo, SQueryStmtInfo* pQueryInfo, int64_t id, char* msg, int32_t msgLen);

/**
 * Evaluate the numeric and timestamp arithmetic expression in the WHERE clause.
 * @param pNode
 * @param tsPrecision
 * @param msg
 * @param msgBufLen
 * @return
 */
int32_t evaluateSqlNode(SSqlNode* pNode, int32_t tsPrecision, char* msg, int32_t msgBufLen);

/**
 * Extract request meta info from the sql statement
 * @param pSqlInfo
 * @param pMetaInfo
 * @param msg
 * @param msgBufLen
 * @return
 */
int32_t qParserExtractRequestedMetaInfo(const SSqlInfo* pSqlInfo, SMetaReq* pMetaInfo, char* msg, int32_t msgBufLen);

#ifdef __cplusplus
}
#endif

#endif /*_TD_PARSER_INT_H_*/