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

#include "vnodeInt.h"

STqReadHandle* tqInitSubmitMsgScanner(SMeta* pMeta) {
  STqReadHandle* pReadHandle = taosMemoryMalloc(sizeof(STqReadHandle));
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
  // pMsg->length = htonl(pMsg->length);
  // pMsg->numOfBlocks = htonl(pMsg->numOfBlocks);

  // iterate and convert
  if (tInitSubmitMsgIterEx(pMsg, &pReadHandle->msgIter) < 0) return -1;
  while (true) {
    if (tGetSubmitMsgNextEx(&pReadHandle->msgIter, &pReadHandle->pBlock) < 0) return -1;
    if (pReadHandle->pBlock == NULL) break;

    // pReadHandle->pBlock->uid = htobe64(pReadHandle->pBlock->uid);
    // pReadHandle->pBlock->suid = htobe64(pReadHandle->pBlock->suid);
    // pReadHandle->pBlock->sversion = htonl(pReadHandle->pBlock->sversion);
    // pReadHandle->pBlock->dataLen = htonl(pReadHandle->pBlock->dataLen);
    // pReadHandle->pBlock->schemaLen = htonl(pReadHandle->pBlock->schemaLen);
    // pReadHandle->pBlock->numOfRows = htons(pReadHandle->pBlock->numOfRows);
  }

  if (tInitSubmitMsgIterEx(pMsg, &pReadHandle->msgIter) < 0) return -1;
  pReadHandle->ver = ver;
  memset(&pReadHandle->blkIter, 0, sizeof(SSubmitBlkIter));
  return 0;
}

bool tqNextDataBlock(STqReadHandle* pHandle) {
  while (1) {
    if (tGetSubmitMsgNextEx(&pHandle->msgIter, &pHandle->pBlock) < 0) {
      return false;
    }
    if (pHandle->pBlock == NULL) return false;

    /*pHandle->pBlock->uid = htobe64(pHandle->pBlock->uid);*/
    /*if (pHandle->tbUid == pHandle->pBlock->uid) {*/
    if (pHandle->tbIdHash == NULL) {
      return true;
    }
    void* ret = taosHashGet(pHandle->tbIdHash, &pHandle->msgIter.uid, sizeof(int64_t));
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

int32_t tqRetrieveDataBlock(SArray** ppCols, STqReadHandle* pHandle, uint64_t* pGroupId, int32_t* pNumOfRows,
                            int16_t* pNumOfCols) {
  /*int32_t         sversion = pHandle->pBlock->sversion;*/
  // TODO set to real sversion
  int32_t sversion = 0;
  if (pHandle->sver != sversion) {
    pHandle->pSchema = metaGetTbTSchema(pHandle->pVnodeMeta, pHandle->msgIter.uid, sversion);
#if 0
    tb_uid_t quid;
    STbCfg*  pTbCfg = metaGetTbInfoByUid(pHandle->pVnodeMeta, pHandle->msgIter.uid);
    if (pTbCfg->type == META_CHILD_TABLE) {
      quid = pTbCfg->ctbCfg.suid;
    } else {
      quid = pHandle->msgIter.uid;
    }
    pHandle->pSchemaWrapper = metaGetTableSchema(pHandle->pVnodeMeta, quid, sversion, true);
#endif
    pHandle->pSchemaWrapper = metaGetTableSchema(pHandle->pVnodeMeta, pHandle->msgIter.suid, sversion, true);
    pHandle->sver = sversion;
  }

  STSchema*       pTschema = pHandle->pSchema;
  SSchemaWrapper* pSchemaWrapper = pHandle->pSchemaWrapper;

  *pNumOfRows = pHandle->msgIter.numOfRows;
  int32_t colNumNeed = taosArrayGetSize(pHandle->pColIdList);

  if (colNumNeed == 0) {
    *ppCols = taosArrayInit(pSchemaWrapper->nCols, sizeof(SColumnInfoData));
    if (*ppCols == NULL) {
      return -1;
    }

    int32_t colMeta = 0;
    while (colMeta < pSchemaWrapper->nCols) {
      SSchema*        pColSchema = &pSchemaWrapper->pSchema[colMeta];
      SColumnInfoData colInfo = {0};
      colInfo.info.bytes = pColSchema->bytes;
      colInfo.info.colId = pColSchema->colId;
      colInfo.info.type = pColSchema->type;

      if (colInfoDataEnsureCapacity(&colInfo, 0, *pNumOfRows) < 0) {
        goto FAIL;
      }
      taosArrayPush(*ppCols, &colInfo);
      colMeta++;
    }
  } else {
    if (colNumNeed > pSchemaWrapper->nCols) {
      colNumNeed = pSchemaWrapper->nCols;
    }

    *ppCols = taosArrayInit(colNumNeed, sizeof(SColumnInfoData));
    if (*ppCols == NULL) {
      return -1;
    }

    int32_t colMeta = 0;
    int32_t colNeed = 0;
    while (colMeta < pSchemaWrapper->nCols && colNeed < colNumNeed) {
      SSchema* pColSchema = &pSchemaWrapper->pSchema[colMeta];
      col_id_t colIdSchema = pColSchema->colId;
      col_id_t colIdNeed = *(col_id_t*)taosArrayGet(pHandle->pColIdList, colNeed);
      if (colIdSchema < colIdNeed) {
        colMeta++;
      } else if (colIdSchema > colIdNeed) {
        colNeed++;
      } else {
        SColumnInfoData colInfo = {0};
        colInfo.info.bytes = pColSchema->bytes;
        colInfo.info.colId = pColSchema->colId;
        colInfo.info.type = pColSchema->type;

        if (colInfoDataEnsureCapacity(&colInfo, 0, *pNumOfRows) < 0) {
          goto FAIL;
        }
        taosArrayPush(*ppCols, &colInfo);
        colMeta++;
        colNeed++;
      }
    }
  }

  int32_t colActual = taosArrayGetSize(*ppCols);
  *pNumOfCols = colActual;

  // TODO in stream shuffle case, fetch groupId
  *pGroupId = 0;

  STSRowIter iter = {0};
  tdSTSRowIterInit(&iter, pTschema);
  STSRow* row;
  int32_t curRow = 0;
  tInitSubmitBlkIterEx(&pHandle->msgIter, pHandle->pBlock, &pHandle->blkIter);
  while ((row = tGetSubmitBlkNextEx(&pHandle->blkIter)) != NULL) {
    tdSTSRowIterReset(&iter, row);
    // get all wanted col of that block
    for (int32_t i = 0; i < colActual; i++) {
      SColumnInfoData* pColData = taosArrayGet(*ppCols, i);
      SCellVal         sVal = {0};
      if (!tdSTSRowIterNext(&iter, pColData->info.colId, pColData->info.type, &sVal)) {
        break;
      }
      if (colDataAppend(pColData, curRow, sVal.val, sVal.valType == TD_VTYPE_NULL) < 0) {
        goto FAIL;
      }
    }
    curRow++;
  }
  return 0;
FAIL:
  taosArrayDestroy(*ppCols);
  return -1;
}

void tqReadHandleSetColIdList(STqReadHandle* pReadHandle, SArray* pColIdList) { pReadHandle->pColIdList = pColIdList; }

int tqReadHandleSetTbUidList(STqReadHandle* pHandle, const SArray* tbUidList) {
  if (pHandle->tbIdHash) {
    taosHashClear(pHandle->tbIdHash);
  }

  pHandle->tbIdHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  if (pHandle->tbIdHash == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  for (int i = 0; i < taosArrayGetSize(tbUidList); i++) {
    int64_t* pKey = (int64_t*)taosArrayGet(tbUidList, i);
    taosHashPut(pHandle->tbIdHash, pKey, sizeof(int64_t), NULL, 0);
  }

  return 0;
}

int tqReadHandleAddTbUidList(STqReadHandle* pHandle, const SArray* tbUidList) {
  if (pHandle->tbIdHash == NULL) {
    pHandle->tbIdHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
    if (pHandle->tbIdHash == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
  }

  for (int i = 0; i < taosArrayGetSize(tbUidList); i++) {
    int64_t* pKey = (int64_t*)taosArrayGet(tbUidList, i);
    taosHashPut(pHandle->tbIdHash, pKey, sizeof(int64_t), NULL, 0);
  }

  return 0;
}
