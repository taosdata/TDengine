/*
 * Copyright (c) 2024 TAOS Data, Inc. <jhtao@taosdata.com>
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
#include "mndJson.h"
#include "tjson.h"
#include "tvariant.h"
#include "querynodes.h"

static void sstreamTriggerOptionsToJson(SJson* pJson, const SStreamTriggerOptions* pOpts) {
  if (pOpts == NULL) {
    return;
  }
  tjsonAddIntegerToObject(pJson, "pEventType", pOpts->pEventType);
  tjsonAddIntegerToObject(pJson, "fillHistoryStartTime", pOpts->fillHistoryStartTime);
  tjsonAddBoolToObject(pJson, "ignoreDisorder", pOpts->ignoreDisorder);
  tjsonAddBoolToObject(pJson, "deleteRecalc", pOpts->deleteRecalc);
  tjsonAddBoolToObject(pJson, "deleteOutputTable", pOpts->deleteOutputTable);
  tjsonAddBoolToObject(pJson, "fillHistory", pOpts->fillHistory);
  tjsonAddBoolToObject(pJson, "fillHistoryFirst", pOpts->fillHistoryFirst);
  tjsonAddBoolToObject(pJson, "calcNotifyOnly", pOpts->calcNotifyOnly);
  tjsonAddBoolToObject(pJson, "lowLatencyCalc", pOpts->lowLatencyCalc);
  tjsonAddBoolToObject(pJson, "forceOutput", pOpts->forceOutput);
  tjsonAddBoolToObject(pJson, "ignoreNoDataTrigger", pOpts->ignoreNoDataTrigger);
}

static void sstreamTriggerNodeToJson(SJson* pJson, const SStreamTriggerNode* pNode) {
  if (pNode == NULL) {
    return;
  }
  SJson* pOptions = tjsonCreateObject();
  sstreamTriggerOptionsToJson(pOptions, (SStreamTriggerOptions*)pNode->pOptions);
  tjsonAddItemToObject(pJson, "options", pOptions);
}

static void sstreamOutTableNodeToJson(SJson* pJson, const SStreamOutTableNode* pNode) {
  if (pNode == NULL) {
    return;
  }
  tjsonAddStringToObject(pJson, "outTable", ((STableNode*)pNode->pOutTable)->tableName);
  if (pNode->pSubtable) {
    tjsonAddStringToObject(pJson, "subTable", ((STableNode*)pNode->pSubtable)->tableName);
  }
}

static void sstreamCalcRangeNodeToJson(SJson* pJson, const SStreamCalcRangeNode* pNode) {
  if (pNode == NULL) {
    return;
  }
  tjsonAddBoolToObject(pJson, "calcAll", pNode->calcAll);
}

static const char* jkCreateStreamReqName      = "name";
static const char* jkCreateStreamReqStreamId      = "streamId";
static const char* jkCreateStreamReqSql      = "sql";

static const char* jkCreateStreamReqStreamDB      = "streamDB";
static const char* jkCreateStreamReqTriggerDB      = "triggerDB";
static const char* jkCreateStreamReqOutDB      = "outDB";
static const char* jkCreateStreamReqCalcDB      = "calcDB";

static const char* jkCreateStreamReqTriggerTblName      = "triggerTblName";
static const char* jkCreateStreamReqOutTblName      = "outTblName";

static const char* jkCreateStreamReqIgExists      = "igExists";
static const char* jkCreateStreamReqTriggerType      = "triggerType";
static const char* jkCreateStreamReqIgDisorder      = "igDisorder";
static const char* jkCreateStreamReqDeleteReCalc      = "deleteReCalc";
static const char* jkCreateStreamReqDeleteOutTbl      = "deleteOutTbl";
static const char* jkCreateStreamReqFillHistory      = "fillHistory";
static const char* jkCreateStreamReqFillHistoryFirst      = "fillHistoryFirst";
static const char* jkCreateStreamReqCalcNotifyOnly      = "calcNotifyOnly";
static const char* jkCreateStreamReqLowLatencyCalc      = "lowLatencyCalc";
static const char* jkCreateStreamReqIgNoDataTrigger      = "igNoDataTrigger";

static const char* jkCreateStreamReqPNotifyAddrUrls      = "pNotifyAddrUrls";
static const char* jkCreateStreamReqNotifyEventTypes      = "notifyEventTypes";
static const char* jkCreateStreamReqAddOptions      = "addOptions";
static const char* jkCreateStreamReqNotifyHistory      = "notifyHistory";

static const char* jkCreateStreamReqTriggerFilterCols      = "triggerFilterCols";
static const char* jkCreateStreamReqTriggerCols      = "triggerCols";
static const char* jkCreateStreamReqPartitionCols      = "partitionCols";
static const char* jkCreateStreamReqOutCols      = "outCols";
static const char* jkCreateStreamReqOutTags      = "outTags";
static const char* jkCreateStreamReqMaxDelay      = "maxDelay";
static const char* jkCreateStreamReqFillHistoryStartTime      = "fillHistoryStartTime";
static const char* jkCreateStreamReqWatermark      = "watermark";
static const char* jkCreateStreamReqExpiredTime      = "expiredTime";
static const char* jkCreateStreamReqTrigger      = "trigger";

static const char* jkCreateStreamReqTriggerTblType      = "triggerTblType";
static const char* jkCreateStreamReqTriggerTblUid      = "triggerTblUid";
static const char* jkCreateStreamReqTriggerTblSuid      = "triggerTblSuid";
static const char* jkCreateStreamReqVtableCalc      = "vtableCalc";
static const char* jkCreateStreamReqOutTblType      = "outTblType";
static const char* jkCreateStreamReqOutStbExists      = "outStbExists";
static const char* jkCreateStreamReqOutStbUid      = "outStbUid";
static const char* jkCreateStreamReqOutStbSversion      = "outStbSversion";
static const char* jkCreateStreamReqEventTypes      = "eventTypes";
static const char* jkCreateStreamReqFlags      = "flags";
static const char* jkCreateStreamReqTsmaId      = "tsmaId";
static const char* jkCreateStreamReqPlaceHolderBitmap      = "placeHolderBitmap";
static const char* jkCreateStreamReqCalcTsSlotId      = "calcTsSlotId";
static const char* jkCreateStreamReqTriTsSlotId      = "triTsSlotId";

static const char* jkCreateStreamReqTriggerTblVgId      = "triggerTblVgId";
static const char* jkCreateStreamReqOutTblVgId      = "outTblVgId";

static const char* jkCreateStreamReqTriggerScanPlan      = "triggerScanPlan";
static const char* jkCreateStreamReqCalcScanPlanList      = "calcScanPlanList";

static const char* jkCreateStreamReqTriggerHasPF      = "triggerHasPF";
static const char* jkCreateStreamReqTriggerPrevFilter      = "triggerPrevFilter";

static const char* jkCreateStreamReqNumOfCalcSubplan      = "numOfCalcSubplan";
static const char* jkCreateStreamReqCalcPlan      = "calcPlan";
static const char* jkCreateStreamReqSubTblNameExpr      = "subTblNameExpr";
static const char* jkCreateStreamReqTagValueExpr      = "tagValueExpr";
static const char* jkCreateStreamReqForceOutCols      = "forceOutCols";

static int32_t scmCreateStreamReqToJson(const void* pObj, SJson* pJson) {
  const SCMCreateStreamReq* pReq = (const SCMCreateStreamReq*)pObj;

  int32_t code = tjsonAddStringToObject(pJson, jkCreateStreamReqName, pReq->name);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkCreateStreamReqStreamId, pReq->streamId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkCreateStreamReqSql, pReq->sql);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkCreateStreamReqStreamDB, pReq->streamDB);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkCreateStreamReqTriggerDB, pReq->triggerDB);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkCreateStreamReqOutDB, pReq->outDB);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddTArray(pJson, jkCreateStreamReqCalcDB, tjsonAddStringToObject, pReq->calcDB);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkCreateStreamReqTriggerTblName, pReq->triggerTblName);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddStringToObject(pJson, jkCreateStreamReqOutTblName, pReq->outTblName);
  }
  // trigger contol part
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkCreateStreamReqIgExists, pReq->igExists);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkCreateStreamReqTriggerType, pReq->triggerType);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkCreateStreamReqIgDisorder, pReq->igDisorder);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkCreateStreamReqDeleteReCalc, pReq->deleteReCalc);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkCreateStreamReqDeleteOutTbl, pReq->deleteOutTbl);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkCreateStreamReqFillHistory, pReq->fillHistory);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkCreateStreamReqFillHistoryFirst, pReq->fillHistoryFirst);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkCreateStreamReqCalcNotifyOnly, pReq->calcNotifyOnly);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkCreateStreamReqLowLatencyCalc, pReq->lowLatencyCalc);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkCreateStreamReqIgNoDataTrigger, pReq->igNoDataTrigger);
  }
  
  // notify part
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddTArray(pJson, jkCreateStreamReqPNotifyAddrUrls, tjsonAddStringToObject, pReq->pNotifyAddrUrls);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkCreateStreamReqNotifyEventTypes, pReq->notifyEventTypes);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkCreateStreamReqAddOptions, pReq->addOptions);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkCreateStreamReqNotifyHistory, pReq->notifyHistory);
  }

  // out table part

  // trigger cols and partition cols

  // out cols

  // out tags


  
  return code;
}

static const char* jkStreamObjName      = "name";
static const char* jkStreamObjMainSnode = "mainSnodeId";
static const char* jkStreamObjUserDrop  = "userDropped";
static const char* jkStreamObjUserStop  = "userStopped";
static const char* jkStreamObjCreateTime = "createTime";
static const char* jkStreamObjUpdateTime = "updateTime";
static const char* jkStreamObjCreate    = "create";

int32_t mndStreamObjToJson(const SStreamObj* pObj, bool format, char** pStr, int32_t* pStrLen) {
  if (NULL == pObj || NULL == pStr || NULL == pStrLen) {
    terrno = TSDB_CODE_FAILED;
    return terrno;
  }

  SJson* pJson = tjsonCreateObject();
  if (NULL == pJson) {
    return terrno;
  }

  int32_t code = tjsonAddStringToObject(pJson, jkStreamObjName, pObj->name);
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkStreamObjMainSnode, pObj->mainSnodeId);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkStreamObjUserDrop, pObj->userDropped);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkStreamObjUserStop, pObj->userStopped);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkStreamObjCreateTime, pObj->createTime);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = tjsonAddIntegerToObject(pJson, jkStreamObjUpdateTime, pObj->updateTime);
  }

  if (TSDB_CODE_SUCCESS == code) {
    SJson* pCreate = tjsonCreateObject();
    scmCreateStreamReqToJson(pCreate, pObj->pCreate);
    code = tjsonAddItemToObject(pJson, jkStreamObjCreate, pCreate);
  }

  if (code == TSDB_CODE_SUCCESS) {
    *pStr = format ? tjsonToString(pJson) : tjsonToUnformattedString(pJson);
    if (*pStr == NULL) {
      code = terrno;
    } else {
      *pStrLen = strlen(*pStr);
    }
  }

  tjsonDelete(pJson);

  return code;
}
