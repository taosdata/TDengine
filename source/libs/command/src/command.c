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

static SSDataBlock* buildDescResultDataBlock() {
  SSDataBlock* pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
  pBlock->info.numOfCols = DESCRIBE_RESULT_COLS;
  pBlock->info.hasVarCol = true;

  pBlock->pDataBlock = taosArrayInit(4, sizeof(SColumnInfoData));

  SColumnInfoData infoData = {0};
  infoData.info.type = TSDB_DATA_TYPE_VARCHAR;
  infoData.info.bytes = DESCRIBE_RESULT_FIELD_LEN;

  taosArrayPush(pBlock->pDataBlock, &infoData);

  infoData.info.type = TSDB_DATA_TYPE_VARCHAR;
  infoData.info.bytes = DESCRIBE_RESULT_TYPE_LEN;
  taosArrayPush(pBlock->pDataBlock, &infoData);

  infoData.info.type = TSDB_DATA_TYPE_INT;
  infoData.info.bytes = tDataTypes[TSDB_DATA_TYPE_INT].bytes;;
  taosArrayPush(pBlock->pDataBlock, &infoData);

  infoData.info.type = TSDB_DATA_TYPE_VARCHAR;
  infoData.info.bytes = DESCRIBE_RESULT_NOTE_LEN;
  taosArrayPush(pBlock->pDataBlock, &infoData);

  return pBlock;
}

static void setDescResultIntoDataBlock(SSDataBlock* pBlock, int32_t numOfRows, STableMeta* pMeta) {
  blockDataEnsureCapacity(pBlock, numOfRows);
  pBlock->info.rows = numOfRows;

  // field
  SColumnInfoData* pCol1 = taosArrayGet(pBlock->pDataBlock, 0);
  char buf[DESCRIBE_RESULT_FIELD_LEN] = {0};
  for (int32_t i = 0; i < numOfRows; ++i) {
    STR_TO_VARSTR(buf, pMeta->schema[i].name);
    colDataAppend(pCol1, i, buf, false);
  }

  // Type
  SColumnInfoData* pCol2 = taosArrayGet(pBlock->pDataBlock, 1);
  for (int32_t i = 0; i < numOfRows; ++i) {
    STR_TO_VARSTR(buf, tDataTypes[pMeta->schema[i].type].name);
    colDataAppend(pCol2, i, buf, false);
  }

  // Length
  SColumnInfoData* pCol3 = taosArrayGet(pBlock->pDataBlock, 2);
  for (int32_t i = 0; i < numOfRows; ++i) {
    int32_t bytes = getSchemaBytes(pMeta->schema + i);
    colDataAppend(pCol3, i, (const char*)&bytes, false);
  }

  // Note
  SColumnInfoData* pCol4 = taosArrayGet(pBlock->pDataBlock, 3);
  for (int32_t i = 0; i < numOfRows; ++i) {
    STR_TO_VARSTR(buf, i >= pMeta->tableInfo.numOfColumns ? "TAG" : "");
    colDataAppend(pCol4, i, buf, false);
  }
}

static int32_t execDescribe(SNode* pStmt, SRetrieveTableRsp** pRsp) {
  SDescribeStmt* pDesc = (SDescribeStmt*) pStmt;
  int32_t numOfRows = TABLE_TOTAL_COL_NUM(pDesc->pMeta);

  SSDataBlock* pBlock = buildDescResultDataBlock();
  setDescResultIntoDataBlock(pBlock, numOfRows, pDesc->pMeta);

  size_t rspSize = sizeof(SRetrieveTableRsp) + blockGetEncodeSize(pBlock);
  *pRsp = taosMemoryCalloc(1, rspSize);
  if (NULL == *pRsp) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  (*pRsp)->useconds = 0;
  (*pRsp)->completed = 1;
  (*pRsp)->precision = 0;
  (*pRsp)->compressed = 0;
  (*pRsp)->compLen = 0;
  (*pRsp)->numOfRows = htonl(numOfRows);
  (*pRsp)->numOfCols = htonl(DESCRIBE_RESULT_COLS);

  int32_t len = 0;
  blockCompressEncode(pBlock, (*pRsp)->data, &len, DESCRIBE_RESULT_COLS, false);
  ASSERT(len == rspSize - sizeof(SRetrieveTableRsp));

  blockDataDestroy(pBlock);
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
