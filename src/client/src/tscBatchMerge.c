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

#include "tscBatchMerge.h"

/**
 * A util function to compare two SName.
 */
static int32_t compareSName(const void *left, const void *right) {
  if (left == right) {
    return 0;
  }
  SName* x = *(SName** ) left;
  SName* y = *(SName** ) right;
  return memcmp(x, y, sizeof(SName));
}

/**
 * A util function to sort SArray<SMemRow> by key.
 */
static int32_t compareSMemRow(const void *x, const void *y) {
  TSKEY left = memRowKey(*(void **) x);
  TSKEY right = memRowKey(*(void **) y);
  if (left == right) {
    return 0;
  } else {
    return left > right ? 1 : -1;
  }
}

/**
 * If the SSubmitBlkBuilder of pBlock->uid is present, returns it. Otherwise, build a new SSubmitBlkBuilder.
 * 
 * @param builder the SSubmitMsgBlocksBuilder.
 * @param pBlock  the SSubmitBlk.
 * @return        the SSubmitBlkBuilder (NULL means failure).
 */
inline static SSubmitBlkBuilder* computeIfAbsentSSubmitBlkBuilder(SSubmitMsgBlocksBuilder* builder, SSubmitBlk* pBlock) {
  SSubmitBlkBuilder** iter = taosHashGet(builder->blockBuilders, &pBlock->uid, sizeof(pBlock->uid));
  SSubmitBlkBuilder* blocksBuilder = NULL;
  if (iter) {
    return *iter;
  }
  
  blocksBuilder = createSSubmitBlkBuilder(pBlock);
  if (!blocksBuilder) {
    return NULL;
  }
    
  if (taosHashPut(builder->blockBuilders, &pBlock->uid, sizeof(pBlock->uid), &blocksBuilder, sizeof(SArray*))) {
    destroySSubmitBlkBuilder(blocksBuilder);
    return NULL;
  }
  
  return blocksBuilder;
}

SName** buildSTableNameListBuilder(STableNameListBuilder* builder, size_t* nTables) {
  if (!taosArrayGetSize(builder->pTableNameList)) {
    *nTables = 0;
    return NULL;
  }

  // sort and unique.
  taosArraySort(builder->pTableNameList, compareSName);
  size_t tail = 0;
  size_t nNames = taosArrayGetSize(builder->pTableNameList);
  for (size_t i = 1; i < nNames; ++i) {
    SName* last = taosArrayGetP(builder->pTableNameList, tail);
    SName* current = taosArrayGetP(builder->pTableNameList, i);
    if (memcmp(last, current, sizeof(SName)) != 0) {
      ++tail;
      taosArraySet(builder->pTableNameList, tail, &current);
    }
  }

  // build table names list.
  SName** tableNames = calloc(tail + 1, sizeof(SName*));
  if (!tableNames) {
    return NULL;
  }
  
  // clone data.
  for (size_t i = 0; i <= tail; ++i) {
    SName* clone = malloc(sizeof(SName));
    if (!clone) {
      goto error;
    }
    memcpy(clone, taosArrayGetP(builder->pTableNameList, i), sizeof(SName));
    tableNames[i] = clone;
  }

  *nTables = tail + 1;
  return tableNames;

error:
  for (size_t i = 0; i <= tail; ++i) {
    if (tableNames[i]) {
      free(tableNames[i]);
    }
  }
  free(tableNames);
  return NULL;
}

SSubmitBlkBuilder* createSSubmitBlkBuilder(SSubmitBlk* metadata) {
  SSubmitBlkBuilder* builder = calloc(1, sizeof(SSubmitBlkBuilder));
  if (!builder) {
    return NULL;
  }

  builder->rows = taosArrayInit(1, sizeof(SMemRow));
  if (!builder->rows) {
    free(builder);
    return NULL;
  }

  builder->metadata = calloc(1, sizeof(SSubmitBlk));
  if (!builder->metadata) {
    taosArrayDestroy(&builder->rows);
    free(builder);
    return NULL;
  }
  memcpy(builder->metadata, metadata, sizeof(SSubmitBlk));

  return builder;
}

void destroySSubmitBlkBuilder(SSubmitBlkBuilder* builder) {
  if (!builder) {
    return;
  }
  taosArrayDestroy(&builder->rows);
  free(builder->metadata);
  free(builder);
}

bool appendSSubmitBlkBuilder(SSubmitBlkBuilder* builder, SSubmitBlk* pBlock) {
  assert(pBlock->uid == builder->metadata->uid);
  assert(pBlock->schemaLen == 0);
  
  // shadow copy all the SMemRow to SSubmitBlkBuilder::rows.
  char* pRow = pBlock->data;
  char* pEnd = pBlock->data + htonl(pBlock->dataLen);
  while (pRow < pEnd) {
    if (!taosArrayPush(builder->rows, &pRow)) {
      return false;
    }
    pRow += memRowTLen(pRow);
  }

  return true;
}

size_t writeSSubmitBlkBuilder(SSubmitBlkBuilder* builder, SSubmitBlk* target, size_t* nRows) {
  memcpy(target, builder->metadata, sizeof(SSubmitBlk));

  // sort SSubmitBlkBuilder::rows by timestamp.
  uint32_t dataLen = 0;
  taosArraySort(builder->rows, compareSMemRow);
  
  // deep copy all the SMemRow to target.
  size_t nMemRows = taosArrayGetSize(builder->rows);
  for (int i = 0; i < nMemRows; ++i) {
    char* pRow = taosArrayGetP(builder->rows, i);
    memcpy(POINTER_SHIFT(target->data, dataLen), pRow, memRowTLen(pRow));
    dataLen += memRowTLen(pRow);
  }

  *nRows = nMemRows;

  target->schemaLen = 0;
  target->dataLen = (int32_t) htonl(dataLen);
  target->numOfRows = (int16_t) htons(*nRows);

  return dataLen + sizeof(SSubmitBlk);
}

size_t nWriteSSubmitBlkBuilder(SSubmitBlkBuilder* builder) {
  size_t dataLen = 0;
  size_t nRows = taosArrayGetSize(builder->rows);
  for (int i = 0; i < nRows; ++i) {
    char* pRow = taosArrayGetP(builder->rows, i);
    dataLen += memRowTLen(pRow);
  }
  return dataLen + sizeof(SSubmitBlk);
}

SSubmitMsgBlocksBuilder* createSSubmitMsgBuilder(int64_t vgId) {
  SSubmitMsgBlocksBuilder* builder = calloc(1, sizeof(SSubmitMsgBlocksBuilder));
  if (!builder) {
    return NULL;
  }
  builder->vgId = vgId;

  builder->blockBuilders = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, false);
  if (!builder->blockBuilders) {
    free(builder);
    return NULL;
  }
  return builder;
}

size_t nWriteSSubmitMsgBuilder(SSubmitMsgBlocksBuilder* builder) {
  size_t nWrite = 0;
  SSubmitBlkBuilder** iter = taosHashIterate(builder->blockBuilders, NULL);
  while (iter) {
    SSubmitBlkBuilder* blocksBuilder = *iter;
    nWrite += nWriteSSubmitBlkBuilder(blocksBuilder);
    iter = taosHashIterate(builder->blockBuilders, iter);
  }
  return nWrite;
}

size_t writeSSubmitMsgBlocksBuilder(SSubmitMsgBlocksBuilder* builder, SSubmitBlk* pBlocks, size_t* nRows) {
  size_t nWrite = 0;
  SSubmitBlkBuilder** iter = taosHashIterate(builder->blockBuilders, NULL);
  
  // copy all the SSubmitBlk to pBlocks.
  while (iter) {
    size_t nSubRows = 0;
    SSubmitBlkBuilder* blocksBuilder = *iter;
    SSubmitBlk* pBlock = POINTER_SHIFT(pBlocks, nWrite);
    nWrite += writeSSubmitBlkBuilder(blocksBuilder, pBlock, &nSubRows);
    *nRows += nSubRows;
    iter = taosHashIterate(builder->blockBuilders, iter);
  }
  return nWrite;
}

size_t nBlockSSubmitMsgBlocksBuilder(SSubmitMsgBlocksBuilder* builder) {
  return taosHashGetSize(builder->blockBuilders);
}

void destroySSubmitMsgBuilder(SSubmitMsgBlocksBuilder* builder) {
  if (!builder) {
    return;
  }

  SSubmitBlkBuilder** iter = taosHashIterate(builder->blockBuilders, NULL);
  while (iter) {
    destroySSubmitBlkBuilder(*iter);
    iter = taosHashIterate(builder->blockBuilders, iter);
  }
  taosHashCleanup(builder->blockBuilders);
  free(builder);
}

bool appendSSubmitMsgBlocks(SSubmitMsgBlocksBuilder* builder, SSubmitBlk* pBlocks, size_t nBlocks) {
  SSubmitBlk* pBlock = pBlocks;
  for (size_t i = 0; i < nBlocks; ++i) {
    // not support SSubmitBlk with schema.
    assert(pBlock->schemaLen == 0);
    
    // get the builder of specific table (by pBlock->uid).
    SSubmitBlkBuilder* blocksBuilder = computeIfAbsentSSubmitBlkBuilder(builder, pBlock);
    if (!blocksBuilder) {
      return false;
    }

    if (!appendSSubmitBlkBuilder(blocksBuilder, pBlock)) {
      return false;
    }
    
    // go to next block.
    size_t blockSize = sizeof (SSubmitBlk) + htonl(pBlock->dataLen);
    pBlock = POINTER_SHIFT(pBlock, blockSize);
  }
  return true;
}

STableDataBlocksBuilder* createSTableDataBlocksBuilder(int64_t vgId) {
  STableDataBlocksBuilder* builder = calloc(1, sizeof(STableDataBlocksBuilder));
  if (!builder) {
    return NULL;
  }

  builder->blocksBuilder = createSSubmitMsgBuilder(vgId);
  if (!builder->blocksBuilder) {
    free(builder);
    return NULL;
  }

  builder->vgId = vgId;
  builder->firstBlock = NULL;
  return builder;
}

void destroySTableDataBlocksBuilder(STableDataBlocksBuilder* builder) {
  if (!builder) {
    return;
  }

  destroySSubmitMsgBuilder(builder->blocksBuilder);
  free(builder);
}

bool appendSTableDataBlocksBuilder(STableDataBlocksBuilder* builder, STableDataBlocks* dataBlocks) {
  // the data blocks vgId must be same with builder vgId.
  if (!dataBlocks || dataBlocks->vgId != builder->vgId) {
    return false;
  }

  if (!builder->firstBlock) {
    builder->firstBlock = dataBlocks;
  }

  SSubmitBlk* pBlocks = (SSubmitBlk *)(dataBlocks->pData + dataBlocks->headerSize);
  return appendSSubmitMsgBlocks(builder->blocksBuilder, pBlocks, dataBlocks->numOfTables);
}

STableDataBlocks* buildSTableDataBlocksBuilder(STableDataBlocksBuilder* builder, size_t* nRows) {
  SSubmitMsgBlocksBuilder* blocksBuilder = builder->blocksBuilder;
  STableDataBlocks *firstBlock = builder->firstBlock;
  if (!firstBlock) {
    return NULL;
  }

  size_t nWriteSize = nWriteSSubmitMsgBuilder(builder->blocksBuilder);
  size_t nHeaderSize = firstBlock->headerSize;
  size_t nAllocSize = nWriteSize + nHeaderSize;
  
  // allocate data blocks.
  STableDataBlocks* dataBlocks = NULL;
  int32_t code = tscCreateDataBlock(nAllocSize, 0, (int32_t) nHeaderSize, &firstBlock->tableName, firstBlock->pTableMeta, &dataBlocks);
  if (code != TSDB_CODE_SUCCESS) {
    return NULL;
  }
  
  // build the header (using first block).
  dataBlocks->size = nHeaderSize;
  memcpy(dataBlocks->pData, firstBlock->pData, nHeaderSize);
  
  // build the SSubmitMsg::blocks.
  dataBlocks->size += writeSSubmitMsgBlocksBuilder(blocksBuilder, (SSubmitBlk *) (dataBlocks->pData + nHeaderSize), nRows);
  dataBlocks->numOfTables = (int32_t) nBlockSSubmitMsgBlocksBuilder(blocksBuilder);
  return dataBlocks;
}

STableDataBlocksListBuilder* createSTableDataBlocksListBuilder() {
  STableDataBlocksListBuilder* builder = calloc(1, sizeof(STableDataBlocksListBuilder));
  if (!builder) {
    return NULL;
  }

  builder->dataBlocksBuilders = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, false);
  if (!builder->dataBlocksBuilders) {
    free(builder);
    return NULL;
  }

  return builder;
}

void destroySTableDataBlocksListBuilder(STableDataBlocksListBuilder* builder) {
  if (!builder) {
    return;
  }
  
  STableDataBlocksBuilder** iter = taosHashIterate(builder->dataBlocksBuilders, NULL);
  while (iter) {
    destroySTableDataBlocksBuilder(*iter);
    iter = taosHashIterate(builder->dataBlocksBuilders, iter);
  }
  taosHashCleanup(builder->dataBlocksBuilders);
  free(builder);
}

bool appendSTableDataBlocksListBuilder(STableDataBlocksListBuilder* builder, STableDataBlocks* dataBlocks) {
  // get the data blocks builder of specific vgId.
  STableDataBlocksBuilder** item = taosHashGet(builder->dataBlocksBuilders, &dataBlocks->vgId, sizeof(dataBlocks->vgId));
  STableDataBlocksBuilder* blocksBuilder = NULL;
  if (item) {
    blocksBuilder = *item;
  } else {
    blocksBuilder = createSTableDataBlocksBuilder(dataBlocks->vgId);
    if (!blocksBuilder) {
      return false;
    }

    if (taosHashPut(builder->dataBlocksBuilders, &dataBlocks->vgId, sizeof(dataBlocks->vgId), &blocksBuilder, sizeof(STableDataBlocksBuilder*))) {
      destroySTableDataBlocksBuilder(blocksBuilder);
      return false;
    }
  }
  
  // append to this builder.
  return appendSTableDataBlocksBuilder(blocksBuilder, dataBlocks);
}

SArray* buildSTableDataBlocksListBuilder(STableDataBlocksListBuilder* builder, size_t* nTables, size_t* nRows) {
  SArray* pVnodeDataBlockList = taosArrayInit(taosHashGetSize(builder->dataBlocksBuilders), sizeof(STableDataBlocks*));
  if (!pVnodeDataBlockList) {
    return NULL;
  }
  
  // build data blocks of each vgId.
  STableDataBlocksBuilder** iter = taosHashIterate(builder->dataBlocksBuilders, NULL);
  while (iter) {
    size_t nSubRows = 0;
    STableDataBlocksBuilder* dataBlocksBuilder = *iter;
    STableDataBlocks* dataBlocks = buildSTableDataBlocksBuilder(dataBlocksBuilder, &nSubRows);
    if (!dataBlocks) {
      goto error;
    }
    *nTables += dataBlocks->numOfTables;
    *nRows += nSubRows;

    taosArrayPush(pVnodeDataBlockList, &dataBlocks);
    iter = taosHashIterate(builder->dataBlocksBuilders, iter);
  }
  return pVnodeDataBlockList;

error:
  for (int i = 0; i < taosArrayGetSize(pVnodeDataBlockList); ++i) {
    STableDataBlocks* dataBlocks = taosArrayGetP(pVnodeDataBlockList, i);
    tscDestroyDataBlock(NULL, dataBlocks, false);
  }
  taosArrayDestroy(&pVnodeDataBlockList);
  return NULL;
}

STableNameListBuilder* createSTableNameListBuilder() {
  STableNameListBuilder* builder = calloc(1, sizeof(STableNameListBuilder));
  if (!builder) {
    return NULL;
  }

  builder->pTableNameList = taosArrayInit(1, sizeof(SName*));
  if (!builder->pTableNameList) {
    free(builder);
    return NULL;
  }

  return builder;
}

void destroySTableNameListBuilder(STableNameListBuilder* builder) {
  if (!builder) {
    return;
  }
  taosArrayDestroy(&builder->pTableNameList);
  free(builder);
}

bool insertSTableNameListBuilder(STableNameListBuilder* builder, SName* name) {
  return taosArrayPush(builder->pTableNameList, &name);
}

int32_t tscMergeSSqlObjs(SSqlObj** polls, size_t nPolls, SSqlObj* result) {
  // statement array is empty.
  if (!polls || !nPolls) {
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  STableDataBlocksListBuilder* builder = createSTableDataBlocksListBuilder();
  if (!builder) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  STableNameListBuilder* nameListBuilder = createSTableNameListBuilder();
  if (!nameListBuilder) {
    destroySTableDataBlocksListBuilder(builder);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  // append the existing data blocks to builder.
  for (size_t i = 0; i < nPolls; ++i) {
    SSqlObj *pSql = polls[i];
    SInsertStatementParam* pInsertParam = &pSql->cmd.insertParam;
    if (!pInsertParam->pDataBlocks) {
      continue;
    }

    assert(pInsertParam->payloadType == PAYLOAD_TYPE_KV);
    assert(!pInsertParam->schemaAttached);

    // append each vnode data block to the builder.
    size_t nBlocks = taosArrayGetSize(pInsertParam->pDataBlocks);
    for (size_t j = 0; j < nBlocks; ++j) {
      STableDataBlocks* tableBlock = taosArrayGetP(pInsertParam->pDataBlocks, j);
      if (!appendSTableDataBlocksListBuilder(builder, tableBlock)) {
        destroySTableDataBlocksListBuilder(builder);
        destroySTableNameListBuilder(nameListBuilder);
        return TSDB_CODE_TSC_OUT_OF_MEMORY;
      }

      for (int k = 0; k < pInsertParam->numOfTables; ++k) {
        if (!insertSTableNameListBuilder(nameListBuilder, pInsertParam->pTableNameList[k])) {
          destroySTableDataBlocksListBuilder(builder);
          destroySTableNameListBuilder(nameListBuilder);
          return TSDB_CODE_TSC_OUT_OF_MEMORY;
        }
      }
    }
  }

  // build the vnode data blocks.
  size_t nBlocks = 0;
  size_t nRows = 0;
  SInsertStatementParam* pInsertParam = &result->cmd.insertParam;
  SArray* pVnodeDataBlocksList = buildSTableDataBlocksListBuilder(builder, &nBlocks, &nRows);
  if (!pVnodeDataBlocksList) {
    destroySTableDataBlocksListBuilder(builder);
    destroySTableNameListBuilder(nameListBuilder);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  // build the table name list.
  size_t nTables = 0;
  SName** pTableNameList = buildSTableNameListBuilder(nameListBuilder, &nTables);
  if (!pTableNameList) {
    destroySTableDataBlocksListBuilder(builder);
    destroySTableNameListBuilder(nameListBuilder);
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }
  
  if (nTables != nBlocks) {
    destroySTableDataBlocksListBuilder(builder);
    destroySTableNameListBuilder(nameListBuilder);
    return TSDB_CODE_TSC_INVALID_OPERATION;
  }

  // replace table name list.
  if (pInsertParam->pTableNameList) {
    destroyTableNameList(pInsertParam);
  }
  pInsertParam->pTableNameList = pTableNameList;
  pInsertParam->numOfTables = (int32_t) nTables;

  // replace vnode data blocks.
  if (pInsertParam->pDataBlocks) {
    tscDestroyBlockArrayList(result, pInsertParam->pDataBlocks);
  }
  pInsertParam->pDataBlocks = pVnodeDataBlocksList;
  pInsertParam->numOfRows = (int32_t) nRows;

  // clean up.
  destroySTableDataBlocksListBuilder(builder);
  destroySTableNameListBuilder(nameListBuilder);
  return TSDB_CODE_SUCCESS;
}
