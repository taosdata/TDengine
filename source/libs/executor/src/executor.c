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

#include "executor.h"
#include "executorimpl.h"
#include "planner.h"
#include "vnode.h"

static int32_t doSetStreamBlock(SOperatorInfo* pOperator, void* input, int32_t type, char* id) {
  ASSERT(pOperator != NULL);
  if (pOperator->operatorType != QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN) {
    if (pOperator->numOfDownstream == 0) {
      qError("failed to find stream scan operator to set the input data block, %s" PRIx64, id);
      return TSDB_CODE_QRY_APP_ERROR;
    }

    if (pOperator->numOfDownstream > 1) {  // not handle this in join query
      qError("join not supported for stream block scan, %s" PRIx64, id);
      return TSDB_CODE_QRY_APP_ERROR;
    }
    pOperator->status = OP_NOT_OPENED;
    return doSetStreamBlock(pOperator->pDownstream[0], input, type, id);
  } else {
    SStreamBlockScanInfo* pInfo = pOperator->info;

    // the block type can not be changed in the streamscan operators
    if (pInfo->blockType == 0) {
      pInfo->blockType = type;
    } else if (pInfo->blockType != type) {
      return TSDB_CODE_QRY_APP_ERROR;
    }

    if (type == STREAM_DATA_TYPE_SUBMIT_BLOCK) {
      if (tqReadHandleSetMsg(pInfo->readerHandle, input, 0) < 0) {
        qError("submit msg messed up when initing stream block, %s" PRIx64, id);
        return TSDB_CODE_QRY_APP_ERROR;
      }
    } else {
      ASSERT(!pInfo->blockValid);

      SSDataBlock* pDataBlock = input;
      pInfo->pRes->info = pDataBlock->info;
      taosArrayClear(pInfo->pRes->pDataBlock);
      taosArrayAddAll(pInfo->pRes->pDataBlock, pDataBlock->pDataBlock);

      // set current block valid.
      pInfo->blockValid = true;
    }

    return TSDB_CODE_SUCCESS;
  }
}

int32_t qSetStreamInput(qTaskInfo_t tinfo, const void* input, int32_t type) {
  if (tinfo == NULL) {
    return TSDB_CODE_QRY_APP_ERROR;
  }

  if (input == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tinfo;

  int32_t code = doSetStreamBlock(pTaskInfo->pRoot, (void*)input, type, GET_TASKID(pTaskInfo));
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed to set the stream block data", GET_TASKID(pTaskInfo));
  } else {
    qDebug("%s set the stream block successfully", GET_TASKID(pTaskInfo));
  }

  return code;
}

qTaskInfo_t qCreateStreamExecTaskInfo(void* msg, void* streamReadHandle) {
  if (msg == NULL || streamReadHandle == NULL) {
    return NULL;
  }

  // print those info into log
#if 0
  pMsg->sId = pMsg->sId;
  pMsg->queryId = pMsg->queryId;
  pMsg->taskId = pMsg->taskId;
  pMsg->contentLen = pMsg->contentLen;
#endif

  struct SSubplan* plan = NULL;
  int32_t          code = qStringToSubplan(msg, &plan);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    return NULL;
  }

  qTaskInfo_t pTaskInfo = NULL;
  code = qCreateExecTask(streamReadHandle, 0, 0, plan, &pTaskInfo, NULL);
  if (code != TSDB_CODE_SUCCESS) {
    // TODO: destroy SSubplan & pTaskInfo
    terrno = code;
    return NULL;
  }

  return pTaskInfo;
}

int32_t qUpdateQualifiedTableId(qTaskInfo_t tinfo, SArray* tableIdList, bool isAdd) {
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tinfo;

  // traverse to the streamscan node to add this table id
  SOperatorInfo* pInfo = pTaskInfo->pRoot;
  while (pInfo->operatorType != QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN) {
    pInfo = pInfo->pDownstream[0];
  }

  SStreamBlockScanInfo* pScanInfo = pInfo->info;
  if (isAdd) {
    int32_t code = tqReadHandleAddTbUidList(pScanInfo->readerHandle, tableIdList);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  } else {
    assert(0);
  }

  return TSDB_CODE_SUCCESS;
}
