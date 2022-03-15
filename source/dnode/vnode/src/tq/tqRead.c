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

#include "vnode.h"

STqReadHandle* tqInitSubmitMsgScanner(SMeta* pMeta) {
  STqReadHandle* pReadHandle = malloc(sizeof(STqReadHandle));
  if (pReadHandle == NULL) {
    return NULL;
  }
  pReadHandle->pVnodeMeta = pMeta;
  pReadHandle->pMsg = NULL;
  pReadHandle->ver = -1;
  pReadHandle->pColIdList = NULL;
  pReadHandle->sver = -1;
  pReadHandle->pSchema = NULL;
  pReadHandle->pSchemaWrapper = NULL;
  pReadHandle->tbIdHash = NULL;
  return pReadHandle;
}

int32_t tqReadHandleSetMsg(STqReadHandle* pReadHandle, SSubmitReq* pMsg, int64_t ver) {
  pReadHandle->pMsg = pMsg;
  pMsg->length = htonl(pMsg->length);
  pMsg->numOfBlocks = htonl(pMsg->numOfBlocks);

  // iterate and convert
  if (tInitSubmitMsgIter(pMsg, &pReadHandle->msgIter) < 0) return -1;
  while (true) {
    if (tGetSubmitMsgNext(&pReadHandle->msgIter, &pReadHandle->pBlock) < 0) return -1;
    if (pReadHandle->pBlock == NULL) break;

    pReadHandle->pBlock->uid = htobe64(pReadHandle->pBlock->uid);
    pReadHandle->pBlock->tid = htonl(pReadHandle->pBlock->tid);
    pReadHandle->pBlock->sversion = htonl(pReadHandle->pBlock->sversion);
    pReadHandle->pBlock->dataLen = htonl(pReadHandle->pBlock->dataLen);
    pReadHandle->pBlock->schemaLen = htonl(pReadHandle->pBlock->schemaLen);
    pReadHandle->pBlock->numOfRows = htons(pReadHandle->pBlock->numOfRows);
  }

  if (tInitSubmitMsgIter(pMsg, &pReadHandle->msgIter) < 0) return -1;
  pReadHandle->ver = ver;
  memset(&pReadHandle->blkIter, 0, sizeof(SSubmitBlkIter));
  return 0;
}

bool tqNextDataBlock(STqReadHandle* pHandle) {
  while (1) {
    if (tGetSubmitMsgNext(&pHandle->msgIter, &pHandle->pBlock) < 0) {
      return false;
    }
    if (pHandle->pBlock == NULL) return false;

    /*pHandle->pBlock->uid = htobe64(pHandle->pBlock->uid);*/
    /*if (pHandle->tbUid == pHandle->pBlock->uid) {*/
    ASSERT(pHandle->tbIdHash);
    void* ret = taosHashGet(pHandle->tbIdHash, &pHandle->pBlock->uid, sizeof(int64_t));
    if (ret != NULL) {
      /*printf("retrieve one tb %ld\n", pHandle->pBlock->uid);*/
      /*pHandle->pBlock->tid = htonl(pHandle->pBlock->tid);*/
      /*pHandle->pBlock->sversion = htonl(pHandle->pBlock->sversion);*/
      /*pHandle->pBlock->dataLen = htonl(pHandle->pBlock->dataLen);*/
      /*pHandle->pBlock->schemaLen = htonl(pHandle->pBlock->schemaLen);*/
      /*pHandle->pBlock->numOfRows = htons(pHandle->pBlock->numOfRows);*/
      return true;
      /*} else {*/
      /*printf("skip one tb %ld\n", pHandle->pBlock->uid);*/
    }
  }
  return false;
}

int tqRetrieveDataBlockInfo(STqReadHandle* pHandle, SDataBlockInfo* pBlockInfo) {
  // currently only rows are used

  pBlockInfo->numOfCols = taosArrayGetSize(pHandle->pColIdList);
  pBlockInfo->rows = pHandle->pBlock->numOfRows;
  pBlockInfo->uid = pHandle->pBlock->uid;
  return 0;
}

SArray* tqRetrieveDataBlock(STqReadHandle* pHandle) {
  /*int32_t         sversion = pHandle->pBlock->sversion;*/
  // TODO set to real sversion
  int32_t sversion = 0;
  if (pHandle->sver != sversion) {
    pHandle->pSchema = metaGetTbTSchema(pHandle->pVnodeMeta, pHandle->pBlock->uid, sversion);

    tb_uid_t quid;
    STbCfg*  pTbCfg = metaGetTbInfoByUid(pHandle->pVnodeMeta, pHandle->pBlock->uid);
    if (pTbCfg->type == META_CHILD_TABLE) {
      quid = pTbCfg->ctbCfg.suid;
    } else {
      quid = pHandle->pBlock->uid;
    }
    pHandle->pSchemaWrapper = metaGetTableSchema(pHandle->pVnodeMeta, quid, sversion, true);
    pHandle->sver = sversion;
  }

  STSchema*       pTschema = pHandle->pSchema;
  SSchemaWrapper* pSchemaWrapper = pHandle->pSchemaWrapper;

  int32_t numOfRows = pHandle->pBlock->numOfRows;
  int32_t numOfCols = pHandle->pSchema->numOfCols;
  int32_t colNumNeed = taosArrayGetSize(pHandle->pColIdList);

  // TODO: stable case
  if (colNumNeed > pSchemaWrapper->nCols) {
    colNumNeed = pSchemaWrapper->nCols;
  }

  SArray* pArray = taosArrayInit(colNumNeed, sizeof(SColumnInfoData));
  if (pArray == NULL) {
    return NULL;
  }

  int j = 0;
  for (int32_t i = 0; i < colNumNeed; i++) {
    int32_t colId = *(int32_t*)taosArrayGet(pHandle->pColIdList, i);
    while (j < pSchemaWrapper->nCols && pSchemaWrapper->pSchema[j].colId < colId) {
      j++;
    }
    SSchema*        pColSchema = &pSchemaWrapper->pSchema[j];
    SColumnInfoData colInfo = {0};
    int             sz = numOfRows * pColSchema->bytes;
    colInfo.info.bytes = pColSchema->bytes;
    colInfo.info.colId = colId;
    colInfo.info.type = pColSchema->type;

    colInfo.pData = calloc(1, sz);
    if (colInfo.pData == NULL) {
      // TODO free
      taosArrayDestroy(pArray);
      return NULL;
    }
    taosArrayPush(pArray, &colInfo);
  }

  STSRowIter iter = {0};
  tdSTSRowIterInit(&iter, pTschema);
  STSRow* row;
  // int32_t kvIdx = 0;
  int32_t curRow = 0;
  tInitSubmitBlkIter(pHandle->pBlock, &pHandle->blkIter);
  while ((row = tGetSubmitBlkNext(&pHandle->blkIter)) != NULL) {
    tdSTSRowIterReset(&iter, row);
    // get all wanted col of that block
    for (int32_t i = 0; i < colNumNeed; i++) {
      SColumnInfoData* pColData = taosArrayGet(pArray, i);
      STColumn*        pCol = schemaColAt(pTschema, i);
      // TODO
      ASSERT(pCol->colId == pColData->info.colId);
      // void* val = tdGetMemRowDataOfColEx(row, pCol->colId, pCol->type, TD_DATA_ROW_HEAD_SIZE + pCol->offset, &kvIdx);
      SCellVal sVal = {0};
      if (!tdSTSRowIterNext(&iter, pCol->colId, pCol->type, &sVal)) {
        // TODO: reach end
        break;
      }
      memcpy(POINTER_SHIFT(pColData->pData, curRow * pCol->bytes), sVal.val, pCol->bytes);
    }
    curRow++;
  }
  return pArray;
}
