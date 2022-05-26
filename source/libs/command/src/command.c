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

#include "command.h"
#include "tdatablock.h"

static int32_t getSchemaBytes(const SSchema* pSchema) {
  switch (pSchema->type) {
    case TSDB_DATA_TYPE_BINARY:
      return (pSchema->bytes - VARSTR_HEADER_SIZE);
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_JSON:
      return (pSchema->bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE;
    default:
      return pSchema->bytes;
  }
}

// todo : to convert data according to SSDatablock
static void buildRspData(const STableMeta* pMeta, char* pData) {
  int32_t* payloadLen = (int32_t*) pData;
  uint64_t* groupId = (uint64_t*)(pData + sizeof(int32_t));

  int32_t* pColSizes = (int32_t*)(pData + sizeof(int32_t) + sizeof(uint64_t));
  pData = (char*) pColSizes + DESCRIBE_RESULT_COLS * sizeof(int32_t);

  int32_t numOfRows = TABLE_TOTAL_COL_NUM(pMeta);

  // Field
  int32_t* pOffset = (int32_t*)pData;
  pData += numOfRows * sizeof(int32_t);
  for (int32_t i = 0; i < numOfRows; ++i) {
    STR_TO_VARSTR(pData, pMeta->schema[i].name);
    int16_t len = varDataTLen(pData);
    pData += len;
    *pOffset = pColSizes[0];
    pOffset += 1;
    pColSizes[0] += len;
  }
  
  // Type
  pOffset = (int32_t*)pData;
  pData += numOfRows * sizeof(int32_t);
  for (int32_t i = 0; i < numOfRows; ++i) {
    STR_TO_VARSTR(pData, tDataTypes[pMeta->schema[i].type].name);
    int16_t len = varDataTLen(pData);
    pData += len;
    *pOffset = pColSizes[1];
    pOffset += 1;
    pColSizes[1] += len;
  }

  // Length
  pData += BitmapLen(numOfRows);
  for (int32_t i = 0; i < numOfRows; ++i) {
    *(int32_t*)pData = getSchemaBytes(pMeta->schema + i);
    pData += sizeof(int32_t);
  }
  pColSizes[2] = sizeof(int32_t) * numOfRows;

  // Note
  pOffset = (int32_t*)pData;
  pData += numOfRows * sizeof(int32_t);
  for (int32_t i = 0; i < numOfRows; ++i) {
    STR_TO_VARSTR(pData, i >= pMeta->tableInfo.numOfColumns ? "TAG" : "");
    int16_t len = varDataTLen(pData);
    pData += len;
    *pOffset = pColSizes[3];
    pOffset += 1;
    pColSizes[3] += len;
  }

  for (int32_t i = 0; i < DESCRIBE_RESULT_COLS; ++i) {
    pColSizes[i] = htonl(pColSizes[i]);
  }


  *payloadLen = (int32_t)(pData - (char*)payloadLen);
}

static int32_t calcRspSize(const STableMeta* pMeta) {
  int32_t numOfRows = TABLE_TOTAL_COL_NUM(pMeta);
  return sizeof(SRetrieveTableRsp) + 
      (numOfRows * sizeof(int32_t) + numOfRows * DESCRIBE_RESULT_FIELD_LEN) + 
      (numOfRows * sizeof(int32_t) + numOfRows * DESCRIBE_RESULT_TYPE_LEN) +
      (BitmapLen(numOfRows) + numOfRows * sizeof(int32_t)) + 
      (numOfRows * sizeof(int32_t) + numOfRows * DESCRIBE_RESULT_NOTE_LEN) +
      sizeof(int32_t) + sizeof(uint64_t);
}

static int32_t execDescribe(SNode* pStmt, SRetrieveTableRsp** pRsp) {
  SDescribeStmt* pDesc = (SDescribeStmt*)pStmt;
  *pRsp = taosMemoryCalloc(1, calcRspSize(pDesc->pMeta));
  if (NULL == *pRsp) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  (*pRsp)->useconds = 0;
  (*pRsp)->completed = 1;
  (*pRsp)->precision = 0;
  (*pRsp)->compressed = 0;
  (*pRsp)->compLen = 0;
  (*pRsp)->numOfRows = htonl(TABLE_TOTAL_COL_NUM(pDesc->pMeta));
  buildRspData(pDesc->pMeta, (*pRsp)->data);
  return TSDB_CODE_SUCCESS;
}

static int32_t execResetQueryCache() {
  // todo
  return TSDB_CODE_SUCCESS;
}

int32_t qExecCommand(SNode* pStmt, SRetrieveTableRsp** pRsp) {
  switch (nodeType(pStmt)) {
    case QUERY_NODE_DESCRIBE_STMT:
      return execDescribe(pStmt, pRsp);
    case QUERY_NODE_RESET_QUERY_CACHE_STMT:
      return execResetQueryCache();
    default:
      break;
  }
  return TSDB_CODE_FAILED;
}
