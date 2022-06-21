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

#include "sma.h"

// functions for external invocation

// TODO: Who is responsible for resource allocate and release?
int32_t tdProcessTSmaInsert(SSma* pSma, int64_t indexUid, const char* msg) {
  int32_t code = TSDB_CODE_SUCCESS;

  if ((code = tdProcessTSmaInsertImpl(pSma, indexUid, msg)) < 0) {
    smaWarn("vgId:%d, insert tsma data failed since %s", SMA_VID(pSma), tstrerror(terrno));
  }
  // TODO: destroy SSDataBlocks(msg)
  return code;
}

int32_t tdProcessTSmaCreate(SSma* pSma, int64_t version, const char* msg) {
  int32_t code = TSDB_CODE_SUCCESS;

  if ((code = tdProcessTSmaCreateImpl(pSma, version, msg)) < 0) {
    smaWarn("vgId:%d, create tsma failed since %s", SMA_VID(pSma), tstrerror(terrno));
  }
  // TODO: destroy SSDataBlocks(msg)
  return code;
}

int32_t smaGetTSmaDays(SVnodeCfg* pCfg, void* pCont, uint32_t contLen, int32_t* days) {
  int32_t code = TSDB_CODE_SUCCESS;
  if ((code = tdProcessTSmaGetDaysImpl(pCfg, pCont, contLen, days)) < 0) {
    smaWarn("vgId:%d, get tsma days failed since %s", pCfg->vgId, tstrerror(terrno));
  }
  smaDebug("vgId:%d, get tsma days %d", pCfg->vgId, *days);
  return code;
}


// functions for internal invocation

#if 0

/**
 * @brief TODO: Assume that the final generated result it less than 3M
 *
 * @param pReq
 * @param pDataBlocks
 * @param vgId
 * @param suid  // TODO: check with Liao whether suid response is reasonable
 *
 * TODO: colId should be set
 */
int32_t buildSubmitReqFromDataBlock(SSubmitReq** pReq, const SArray* pDataBlocks, STSchema* pTSchema, int32_t vgId,
                                    tb_uid_t suid, const char* stbName, bool isCreateCtb) {
  int32_t sz = taosArrayGetSize(pDataBlocks);
  int32_t bufSize = sizeof(SSubmitReq);
  for (int32_t i = 0; i < sz; ++i) {
    SDataBlockInfo* pBlkInfo = &((SSDataBlock*)taosArrayGet(pDataBlocks, i))->info;
    bufSize += pBlkInfo->rows * (TD_ROW_HEAD_LEN + pBlkInfo->rowSize + BitmapLen(pBlkInfo->numOfCols));
    bufSize += sizeof(SSubmitBlk);
  }

  *pReq = taosMemoryCalloc(1, bufSize);
  if (!(*pReq)) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return TSDB_CODE_FAILED;
  }
  void* pDataBuf = *pReq;

  SArray*     pTagArray = NULL;
  int32_t     msgLen = sizeof(SSubmitReq);
  int32_t     numOfBlks = 0;
  int32_t     schemaLen = 0;
  SRowBuilder rb = {0};
  tdSRowInit(&rb, pTSchema->version);

  for (int32_t i = 0; i < sz; ++i) {
    SSDataBlock*    pDataBlock = taosArrayGet(pDataBlocks, i);
    SDataBlockInfo* pDataBlkInfo = &pDataBlock->info;
    int32_t         colNum = pDataBlkInfo->numOfCols;
    int32_t         rows = pDataBlkInfo->rows;
    int32_t         rowSize = pDataBlkInfo->rowSize;
    int64_t         groupId = pDataBlkInfo->groupId;

    if (rb.nCols != colNum) {
      tdSRowSetTpInfo(&rb, colNum, pTSchema->flen);
    }

   if(isCreateCtb) {
     SMetaReader  mr = {0};
     const char*  ctbName = buildCtbNameByGroupId(stbName, pDataBlock->info.groupId);
     if (metaGetTableEntryByName(&mr, ctbName) != 0) {
       smaDebug("vgId:%d, no tsma ctb %s exists", vgId, ctbName);
     }
     SVCreateTbReq ctbReq = {0};
     ctbReq.name = ctbName;
     ctbReq.type = TSDB_CHILD_TABLE;
     ctbReq.ctb.suid = suid;

     STagVal tagVal = {.cid = colNum + PRIMARYKEY_TIMESTAMP_COL_ID,
                       .type = TSDB_DATA_TYPE_BIGINT,
                       .i64 = groupId};
     STag*   pTag = NULL;
     if(!pTagArray) {
       pTagArray = taosArrayInit(1, sizeof(STagVal));
       if (!pTagArray) goto _err;
     }
     taosArrayClear(pTagArray);
     taosArrayPush(pTagArray, &tagVal);
     tTagNew(pTagArray, 1, false, &pTag);
     if (pTag == NULL) {
       tdDestroySVCreateTbReq(&ctbReq);
       goto _err;
      }
      ctbReq.ctb.pTag = (uint8_t*)pTag;

      int32_t code;
      tEncodeSize(tEncodeSVCreateTbReq, &ctbReq, schemaLen, code);

      tdDestroySVCreateTbReq(&ctbReq);
      if (code < 0) {
        goto _err;
      }
   }



    SSubmitBlk* pSubmitBlk = POINTER_SHIFT(pDataBuf, msgLen);
    pSubmitBlk->suid = suid;
    pSubmitBlk->uid = groupId;
    pSubmitBlk->numOfRows = rows;

    msgLen += sizeof(SSubmitBlk);
    int32_t dataLen = 0;
    for (int32_t j = 0; j < rows; ++j) {                     // iterate by row
      tdSRowResetBuf(&rb, POINTER_SHIFT(pDataBuf, msgLen));  // set row buf
      bool    isStartKey = false;
      int32_t offset = 0;
      for (int32_t k = 0; k < colNum; ++k) {  // iterate by column
        SColumnInfoData* pColInfoData = taosArrayGet(pDataBlock->pDataBlock, k);
        STColumn*        pCol = &pTSchema->columns[k];
        void*            var = POINTER_SHIFT(pColInfoData->pData, j * pColInfoData->info.bytes);
        switch (pColInfoData->info.type) {
          case TSDB_DATA_TYPE_TIMESTAMP:
            if (!isStartKey) {
              isStartKey = true;
              tdAppendColValToRow(&rb, PRIMARYKEY_TIMESTAMP_COL_ID, TSDB_DATA_TYPE_TIMESTAMP, TD_VTYPE_NORM, var, true,
                                  offset, k);

            } else {
              tdAppendColValToRow(&rb, PRIMARYKEY_TIMESTAMP_COL_ID + k, TSDB_DATA_TYPE_TIMESTAMP, TD_VTYPE_NORM, var,
                                  true, offset, k);
            }
            break;
          case TSDB_DATA_TYPE_NCHAR: {
            tdAppendColValToRow(&rb, PRIMARYKEY_TIMESTAMP_COL_ID + k, TSDB_DATA_TYPE_NCHAR, TD_VTYPE_NORM, var, true,
                                offset, k);
            break;
          }
          case TSDB_DATA_TYPE_VARCHAR: {  // TSDB_DATA_TYPE_BINARY
            tdAppendColValToRow(&rb, PRIMARYKEY_TIMESTAMP_COL_ID + k, TSDB_DATA_TYPE_VARCHAR, TD_VTYPE_NORM, var, true,
                                offset, k);
            break;
          }
          case TSDB_DATA_TYPE_VARBINARY:
          case TSDB_DATA_TYPE_DECIMAL:
          case TSDB_DATA_TYPE_BLOB:
          case TSDB_DATA_TYPE_JSON:
          case TSDB_DATA_TYPE_MEDIUMBLOB:
            uError("the column type %" PRIi16 " is defined but not implemented yet", pColInfoData->info.type);
            TASSERT(0);
            break;
          default:
            if (pColInfoData->info.type < TSDB_DATA_TYPE_MAX && pColInfoData->info.type > TSDB_DATA_TYPE_NULL) {
              if (pCol->type == pColInfoData->info.type) {
                tdAppendColValToRow(&rb, PRIMARYKEY_TIMESTAMP_COL_ID + k, pCol->type, TD_VTYPE_NORM, var, true, offset,
                                    k);
              } else {
                char tv[8] = {0};
                if (pColInfoData->info.type == TSDB_DATA_TYPE_FLOAT) {
                  float v = 0;
                  GET_TYPED_DATA(v, float, pColInfoData->info.type, var);
                  SET_TYPED_DATA(&tv, pCol->type, v);
                } else if (pColInfoData->info.type == TSDB_DATA_TYPE_DOUBLE) {
                  double v = 0;
                  GET_TYPED_DATA(v, double, pColInfoData->info.type, var);
                  SET_TYPED_DATA(&tv, pCol->type, v);
                } else if (IS_SIGNED_NUMERIC_TYPE(pColInfoData->info.type)) {
                  int64_t v = 0;
                  GET_TYPED_DATA(v, int64_t, pColInfoData->info.type, var);
                  SET_TYPED_DATA(&tv, pCol->type, v);
                } else {
                  uint64_t v = 0;
                  GET_TYPED_DATA(v, uint64_t, pColInfoData->info.type, var);
                  SET_TYPED_DATA(&tv, pCol->type, v);
                }
                tdAppendColValToRow(&rb, PRIMARYKEY_TIMESTAMP_COL_ID + k, pCol->type, TD_VTYPE_NORM, tv, true, offset,
                                    k);
              }
            } else {
              uError("the column type %" PRIi16 " is undefined\n", pColInfoData->info.type);
              TASSERT(0);
            }
            break;
        }
        offset += TYPE_BYTES[pCol->type];  // sum/avg would convert to int64_t/uint64_t/double during aggregation
      }
      dataLen += TD_ROW_LEN(rb.pBuf);
#ifdef TD_DEBUG_PRINT_ROW
      tdSRowPrint(rb.pBuf, pTSchema, __func__);
#endif
    }

    ++numOfBlks;

    pSubmitBlk->dataLen = dataLen;
    msgLen += pSubmitBlk->dataLen;
  }

  (*pReq)->length = msgLen;

  (*pReq)->header.vgId = htonl(vgId);
  (*pReq)->header.contLen = htonl(msgLen);
  (*pReq)->length = (*pReq)->header.contLen;
  (*pReq)->numOfBlocks = htonl(numOfBlks);
  SSubmitBlk* blk = (SSubmitBlk*)((*pReq) + 1);
  while (numOfBlks--) {
    int32_t dataLen = blk->dataLen;
    blk->uid = htobe64(blk->uid);
    blk->suid = htobe64(blk->suid);
    blk->padding = htonl(blk->padding);
    blk->sversion = htonl(blk->sversion);
    blk->dataLen = htonl(blk->dataLen);
    blk->schemaLen = htonl(blk->schemaLen);
    blk->numOfRows = htons(blk->numOfRows);
    blk = (SSubmitBlk*)(blk->data + dataLen);
  }
  return TSDB_CODE_SUCCESS;
_err:
  taosMemoryFreeClear(*pReq);
  taosArrayDestroy(pTagArray);

  return TSDB_CODE_FAILED;
}
#endif
