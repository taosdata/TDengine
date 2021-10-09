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

typedef struct SInsertStmtInfo {
  SHashObj *pTableBlockHashList;     // data block for each table
  SArray   *pDataBlocks;             // SArray<STableDataBlocks*>. Merged submit block for each vgroup
  int8_t    schemaAttached;          // denote if submit block is built with table schema or not
  uint8_t   payloadType;             // EPayloadType. 0: K-V payload for non-prepare insert, 1: rawPayload for prepare insert
  uint32_t  insertType;              // insert data from [file|sql statement| bound statement]
  char     *sql;                     // current sql statement position
} SInsertStmtInfo;

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
 *
 * @param pSqlNode
 * @param pMetaInfo
 * @return
 */
int32_t qParserExtractRequestedMetaInfo(const SArray* pSqlNodeList, SMetaReq* pMetaInfo);

#ifdef __cplusplus
}
#endif

#endif /*_TD_PARSER_INT_H_*/