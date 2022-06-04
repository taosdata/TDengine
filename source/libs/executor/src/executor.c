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
#include "tdatablock.h"
#include "vnode.h"

static int32_t doSetStreamBlock(SOperatorInfo* pOperator, void* input, size_t numOfBlocks, int32_t type, bool assignUid, char* id) {
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
    return doSetStreamBlock(pOperator->pDownstream[0], input, numOfBlocks, type, assignUid, id);
  } else {
    pOperator->status = OP_NOT_OPENED;

    SStreamBlockScanInfo* pInfo = pOperator->info;
    pInfo->assignBlockUid = assignUid;

    // the block type can not be changed in the streamscan operators
    if (pInfo->blockType == 0) {
      pInfo->blockType = type;
    } else if (pInfo->blockType != type) {
      return TSDB_CODE_QRY_APP_ERROR;
    }

    if (type == STREAM_DATA_TYPE_SUBMIT_BLOCK) {
      if (tqReadHandleSetMsg(pInfo->streamBlockReader, input, 0) < 0) {
        qError("submit msg messed up when initing stream block, %s" PRIx64, id);
        return TSDB_CODE_QRY_APP_ERROR;
      }
    } else {
      for (int32_t i = 0; i < numOfBlocks; ++i) {
        SSDataBlock* pDataBlock = &((SSDataBlock*)input)[i];

        SSDataBlock* p = createOneDataBlock(pDataBlock, false);
        p->info = pDataBlock->info;

        taosArrayClear(p->pDataBlock);
        taosArrayAddAll(p->pDataBlock, pDataBlock->pDataBlock);
        taosArrayPush(pInfo->pBlockLists, &p);
      }
    }

    return TSDB_CODE_SUCCESS;
  }
}

int32_t qSetStreamInput(qTaskInfo_t tinfo, const void* input, int32_t type, bool assignUid) {
  return qSetMultiStreamInput(tinfo, input, 1, type, assignUid);
}

int32_t qSetMultiStreamInput(qTaskInfo_t tinfo, const void* pBlocks, size_t numOfBlocks, int32_t type, bool assignUid) {
  if (tinfo == NULL) {
    return TSDB_CODE_QRY_APP_ERROR;
  }

  if (pBlocks == NULL || numOfBlocks == 0) {
    return TSDB_CODE_SUCCESS;
  }

  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tinfo;

  int32_t code = doSetStreamBlock(pTaskInfo->pRoot, (void**)pBlocks, numOfBlocks, type, assignUid, GET_TASKID(pTaskInfo));
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

  /*qDebugL("stream task string %s", (const char*)msg);*/

  struct SSubplan* plan = NULL;
  int32_t          code = qStringToSubplan(msg, &plan);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    return NULL;
  }

  qTaskInfo_t pTaskInfo = NULL;
  code = qCreateExecTask(streamReadHandle, 0, 0, plan, &pTaskInfo, NULL, OPTR_EXEC_MODEL_STREAM);
  if (code != TSDB_CODE_SUCCESS) {
    // TODO: destroy SSubplan & pTaskInfo
    terrno = code;
    return NULL;
  }

  return pTaskInfo;
}

static SArray* filterQualifiedChildTables(const SStreamBlockScanInfo* pScanInfo, const SArray* tableIdList) {
  SArray* qa = taosArrayInit(4, sizeof(tb_uid_t));

  // let's discard the tables those are not created according to the queried super table.
  SMetaReader mr = {0};
  metaReaderInit(&mr, pScanInfo->readHandle.meta, 0);
  for (int32_t i = 0; i < taosArrayGetSize(tableIdList); ++i) {
    int64_t* id = (int64_t*)taosArrayGet(tableIdList, i);

    int32_t code = metaGetTableEntryByUid(&mr, *id);
    if (code != TSDB_CODE_SUCCESS) {
      qError("failed to get table meta, uid:%" PRIu64 " code:%s", *id, tstrerror(terrno));
      continue;
    }

    ASSERT(mr.me.type == TSDB_CHILD_TABLE);
    if (mr.me.ctbEntry.suid != pScanInfo->tableUid) {
      continue;
    }

    taosArrayPush(qa, id);
  }

  metaReaderClear(&mr);
  return qa;
}

int32_t qUpdateQualifiedTableId(qTaskInfo_t tinfo, const SArray* tableIdList, bool isAdd) {
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*)tinfo;

  // traverse to the stream scanner node to add this table id
  SOperatorInfo* pInfo = pTaskInfo->pRoot;
  while (pInfo->operatorType != QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN) {
    pInfo = pInfo->pDownstream[0];
  }

  int32_t code = 0;
  SStreamBlockScanInfo* pScanInfo = pInfo->info;
  if (isAdd) {  // add new table id
    SArray* qa = filterQualifiedChildTables(pScanInfo, tableIdList);

    qDebug(" %d qualified child tables added into stream scanner", (int32_t)taosArrayGetSize(qa));
    code = tqReadHandleAddTbUidList(pScanInfo->streamBlockReader, qa);
    taosArrayDestroy(qa);
  } else {  // remove the table id in current list
    qDebug(" %d remove child tables from the stream scanner", (int32_t)taosArrayGetSize(tableIdList));
    code = tqReadHandleRemoveTbUidList(pScanInfo->streamBlockReader, tableIdList);
  }

  return code;
}

int32_t qGetQueriedTableSchemaVersion(qTaskInfo_t tinfo, char* dbName, char* tableName, int32_t* sversion, int32_t* tversion) {
  ASSERT(tinfo != NULL && dbName != NULL && tableName != NULL);
  SExecTaskInfo* pTaskInfo = (SExecTaskInfo*) tinfo;

  *sversion = pTaskInfo->schemaVer.sversion;
  *tversion = pTaskInfo->schemaVer.tversion;
  if (pTaskInfo->schemaVer.dbname) {
    strcpy(dbName, pTaskInfo->schemaVer.dbname);
  } else {
    dbName[0] = 0;
  }
  if (pTaskInfo->schemaVer.tablename) {
    strcpy(tableName, pTaskInfo->schemaVer.tablename);
  } else {
    tableName[0] = 0;
  }

  return 0;
}