#include "streamMsg.h"
#include "tjson.h"

static const char* jkFieldName     = "name";
static const char* jkFieldType     = "type";
static const char* jkFieldFlags    = "flags";
static const char* jkFieldBytes    = "bytes";
static const char* jkFieldCompress = "compress";
static const char* jkFieldTypeMod  = "typeMod";
static int32_t sfieldWithOptionsToJson(const void* pObj, SJson* pJson) {
  const SFieldWithOptions* pField = (const SFieldWithOptions*)pObj;
  if (NULL != pField->name) {
    TAOS_CHECK_RETURN(tjsonAddStringToObject(pJson, jkFieldName, pField->name));
  }
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, jkFieldType, pField->type));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkFieldFlags, pField->flags));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkFieldBytes, pField->bytes));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkFieldCompress, pField->compress));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkFieldTypeMod, pField->typeMod));
  return TSDB_CODE_SUCCESS;
}

static int32_t jsonToSFieldWithOptions(const SJson* pJson, void* pObj) {
  SFieldWithOptions* pField = (SFieldWithOptions*)pObj;
  TAOS_CHECK_RETURN(tjsonGetStringValue(pJson, jkFieldName, pField->name));
  TAOS_CHECK_RETURN(tjsonGetUTinyIntValue(pJson, jkFieldType, &pField->type));
  TAOS_CHECK_RETURN(tjsonGetTinyIntValue(pJson, jkFieldFlags, &pField->flags));
  TAOS_CHECK_RETURN(tjsonGetIntValue(pJson, jkFieldBytes, &pField->bytes));
  TAOS_CHECK_RETURN(tjsonGetUIntValue(
    pJson, jkFieldCompress, &pField->compress));
  TAOS_CHECK_RETURN(tjsonGetIntValue(pJson, jkFieldTypeMod, &pField->typeMod));
  return TSDB_CODE_SUCCESS;
}

static int32_t stagFieldWithOptionsToJson(const void* pObj, SJson* pJson) {
  const SFieldWithOptions* pField = (const SFieldWithOptions*)pObj;
  if (NULL != pField->name) {
    TAOS_CHECK_RETURN(tjsonAddStringToObject(pJson, jkFieldName, pField->name));
  }
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, jkFieldType, pField->type));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkFieldFlags, pField->flags));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkFieldBytes, pField->bytes));
  // TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
  //   pJson, jkFieldCompress, pField->compress));
  // TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
  //   pJson, jkFieldTypeMod, pField->typeMod));
  return TSDB_CODE_SUCCESS;
}

static int32_t jsonToSTagFieldWithOptions(const SJson* pJson, void* pObj) {
  SFieldWithOptions* pField = (SFieldWithOptions*)pObj;
  TAOS_CHECK_RETURN(tjsonGetStringValue(pJson, jkFieldName, pField->name));
  TAOS_CHECK_RETURN(tjsonGetUTinyIntValue(pJson, jkFieldType, &pField->type));
  TAOS_CHECK_RETURN(tjsonGetTinyIntValue(pJson, jkFieldFlags, &pField->flags));
  TAOS_CHECK_RETURN(tjsonGetIntValue(pJson, jkFieldBytes, &pField->bytes));
  // TAOS_CHECK_RETURN(tjsonGetUIntValue(
  //   pJson, jkFieldCompress, &pField->compress));
  // TAOS_CHECK_RETURN(tjsonGetIntValue(pJson, jkFieldTypeMod, &pField->typeMod));
  return TSDB_CODE_SUCCESS;
}

static const char* jkSessionTriggerSlotId     = "slotId";
static const char* jkSessionTriggerSessionVal = "sessionVal";
static int32_t sessionTriggerToJson(const void* pObj, SJson* pJson) {
  const SSessionTrigger* pTrigger = (const SSessionTrigger*)pObj;
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkSessionTriggerSlotId, pTrigger->slotId));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkSessionTriggerSessionVal, pTrigger->sessionVal));
  return TSDB_CODE_SUCCESS;
}

static int32_t jsonToSessionTrigger(const SJson* pJson, void* pObj) {
  SSessionTrigger* pTrigger = (SSessionTrigger*)pObj;
  TAOS_CHECK_RETURN(tjsonGetSmallIntValue(
    pJson, jkSessionTriggerSlotId, &pTrigger->slotId));
  TAOS_CHECK_RETURN(tjsonGetBigIntValue(
    pJson, jkSessionTriggerSessionVal, &pTrigger->sessionVal));
  return TSDB_CODE_SUCCESS;
}

static const char* jkStateTriggerSlotId           = "slotId";
static const char* jkStateTriggerExtend           = "extend";
static const char* jkStateTriggerTrueForDuration  = "trueForDuration";
static const char* jkStateTriggerExpr             = "expr";
static int32_t stateTriggerToJson(const void* pObj, SJson* pJson) {
  const SStateWinTrigger* pTrigger = (const SStateWinTrigger*)pObj;
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkStateTriggerSlotId, pTrigger->slotId));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkStateTriggerExtend, pTrigger->extend));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkStateTriggerTrueForDuration, pTrigger->trueForDuration));
  if (NULL != pTrigger->expr) {
    TAOS_CHECK_RETURN(tjsonAddStringToObject(
      pJson, jkStateTriggerExpr, (const char*)pTrigger->expr));
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t jsonToStateTrigger(const SJson* pJson, void* pObj) {
  SStateWinTrigger* pTrigger = (SStateWinTrigger*)pObj;
  TAOS_CHECK_RETURN(
    tjsonGetSmallIntValue(pJson, jkStateTriggerSlotId, &pTrigger->slotId));
  TAOS_CHECK_RETURN(
    tjsonGetSmallIntValue(pJson, jkStateTriggerExtend, &pTrigger->extend));
  TAOS_CHECK_RETURN(tjsonGetBigIntValue(
    pJson, jkStateTriggerTrueForDuration, &pTrigger->trueForDuration));
  TAOS_CHECK_RETURN(tjsonDupStringValue(
    pJson, jkStateTriggerExpr, (char**)&pTrigger->expr));
  return TSDB_CODE_SUCCESS;
}

static const char* jkSlidingTriggerIntervalUnit = "intervalUnit";
static const char* jkSlidingTriggerSlidingUnit  = "slidingUnit";
static const char* jkSlidingTriggerOffsetUnit   = "offsetUnit";
static const char* jkSlidingTriggerSoffsetUnit  = "soffsetUnit";
static const char* jkSlidingTriggerPrecision    = "precision";
static const char* jkSlidingTriggerInterval     = "interval";
static const char* jkSlidingTriggerOffset       = "offset";
static const char* jkSlidingTriggerSliding      = "sliding";
static const char* jkSlidingTriggerSoffset      = "soffset";
static const char* jkSlidingTriggerOverlap      = "overlap";
static int32_t slidingTriggerToJson(const void* pObj, SJson* pJson) {
  const SSlidingTrigger* pTrigger = (const SSlidingTrigger*)pObj;
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkSlidingTriggerIntervalUnit, pTrigger->intervalUnit));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkSlidingTriggerSlidingUnit, pTrigger->slidingUnit));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkSlidingTriggerOffsetUnit, pTrigger->offsetUnit));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkSlidingTriggerSoffsetUnit, pTrigger->soffsetUnit));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkSlidingTriggerPrecision, pTrigger->precision));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkSlidingTriggerInterval, pTrigger->interval));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkSlidingTriggerSliding, pTrigger->sliding));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkSlidingTriggerOffset, pTrigger->offset));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkSlidingTriggerSoffset, pTrigger->soffset));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkSlidingTriggerOverlap, pTrigger->overlap));
  return TSDB_CODE_SUCCESS;
}

static int32_t jsonToSlidingTrigger(const SJson* pJson, void* pObj) {
  SSlidingTrigger* pTrigger = (SSlidingTrigger*)pObj;
  TAOS_CHECK_RETURN(tjsonGetTinyIntValue(
    pJson, jkSlidingTriggerIntervalUnit, &pTrigger->intervalUnit));
  TAOS_CHECK_RETURN(tjsonGetTinyIntValue(
    pJson, jkSlidingTriggerSlidingUnit, &pTrigger->slidingUnit));
  TAOS_CHECK_RETURN(tjsonGetTinyIntValue(
    pJson, jkSlidingTriggerOffsetUnit, &pTrigger->offsetUnit));
  TAOS_CHECK_RETURN(tjsonGetTinyIntValue(
    pJson, jkSlidingTriggerSoffsetUnit, &pTrigger->soffsetUnit));
  TAOS_CHECK_RETURN(tjsonGetTinyIntValue(
    pJson, jkSlidingTriggerPrecision, &pTrigger->precision));
  TAOS_CHECK_RETURN(tjsonGetBigIntValue(
    pJson, jkSlidingTriggerInterval, &pTrigger->interval));
  TAOS_CHECK_RETURN(tjsonGetBigIntValue(
    pJson, jkSlidingTriggerSliding, &pTrigger->sliding));
  TAOS_CHECK_RETURN(tjsonGetBigIntValue(
    pJson, jkSlidingTriggerOffset, &pTrigger->offset));
  TAOS_CHECK_RETURN(tjsonGetBigIntValue(
    pJson, jkSlidingTriggerSoffset, &pTrigger->soffset));
  TAOS_CHECK_RETURN(tjsonGetTinyIntValue(
    pJson, jkSlidingTriggerOverlap, &pTrigger->overlap));
  return TSDB_CODE_SUCCESS;
}

static const char* jkEventTriggerStartCond       = "startCond";
static const char* jkEventTriggerEndCond         = "endCond";
static const char* jkEventTriggerTrueForDuration = "trueForDuration";
static int32_t eventTriggerToJson(const void* pObj, SJson* pJson) {
  const SEventTrigger* pTrigger = (const SEventTrigger*)pObj;
  if (NULL != pTrigger->startCond) {
    TAOS_CHECK_RETURN(tjsonAddStringToObject(
      pJson, jkEventTriggerStartCond, (const char*)pTrigger->startCond));
  }
  if (NULL != pTrigger->endCond) {
    TAOS_CHECK_RETURN(tjsonAddStringToObject(
      pJson, jkEventTriggerEndCond, (const char*)pTrigger->endCond));
  }
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkEventTriggerTrueForDuration, pTrigger->trueForDuration));
  return TSDB_CODE_SUCCESS;
}

static int32_t jsonToEventTrigger(const SJson* pJson, void* pObj) {
  SEventTrigger* pTrigger = (SEventTrigger*)pObj;
  TAOS_CHECK_RETURN(tjsonDupStringValue(
    pJson, jkEventTriggerStartCond, (char**)&pTrigger->startCond));
  TAOS_CHECK_RETURN(tjsonDupStringValue(
    pJson, jkEventTriggerEndCond, (char**)&pTrigger->endCond));
  TAOS_CHECK_RETURN(tjsonGetBigIntValue(
    pJson, jkEventTriggerTrueForDuration, &pTrigger->trueForDuration));
  return TSDB_CODE_SUCCESS;
}

static const char* jkCountTriggerCountVal = "countVal";
static const char* jkCountTriggerSliding  = "sliding";
static const char* jkCountTriggerCondCols = "condCols";
static int32_t countTriggerToJson(const void* pObj, SJson* pJson) {
  const SCountTrigger* pTrigger = (const SCountTrigger*)pObj;
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, jkCountTriggerCountVal, pTrigger->countVal));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, jkCountTriggerSliding, pTrigger->sliding));
  if (NULL != pTrigger->condCols) {
    TAOS_CHECK_RETURN(tjsonAddStringToObject(
      pJson, jkCountTriggerCondCols, (const char*)pTrigger->condCols));
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t jsonToCountTrigger(const SJson* pJson, void* pObj) {
  SCountTrigger* pTrigger = (SCountTrigger*)pObj;
  TAOS_CHECK_RETURN(tjsonGetBigIntValue(
    pJson, jkCountTriggerCountVal, &pTrigger->countVal));
  TAOS_CHECK_RETURN(tjsonGetBigIntValue(
    pJson, jkCountTriggerSliding, &pTrigger->sliding));
  TAOS_CHECK_RETURN(tjsonDupStringValue(
    pJson, jkCountTriggerCondCols, (char**)&pTrigger->condCols));
  return TSDB_CODE_SUCCESS;
}

static const char* jkPeriodTriggerPeriodUnit = "periodUnit";
static const char* jkPeriodTriggerOffsetUnit = "offsetUnit";
static const char* jkPeriodTriggerPrecision  = "precision";
static const char* jkPeriodTriggerPeriod     = "period";
static const char* jkPeriodTriggerOffset     = "offset";
static int32_t periodTriggerToJson(const void* pObj, SJson* pJson) {
  const SPeriodTrigger* pTrigger = (const SPeriodTrigger*)pObj;
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkPeriodTriggerPeriodUnit, pTrigger->periodUnit));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkPeriodTriggerOffsetUnit, pTrigger->offsetUnit));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkPeriodTriggerPrecision, pTrigger->precision));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkPeriodTriggerPeriod, pTrigger->period));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkPeriodTriggerOffset, pTrigger->offset));
  return TSDB_CODE_SUCCESS;
}

static int32_t jsonToPeriodTrigger(const SJson* pJson, void* pObj) {
  SPeriodTrigger* pTrigger = (SPeriodTrigger*)pObj;
  TAOS_CHECK_RETURN(tjsonGetTinyIntValue(
    pJson, jkPeriodTriggerPeriodUnit, &pTrigger->periodUnit));
  TAOS_CHECK_RETURN(tjsonGetTinyIntValue(
    pJson, jkPeriodTriggerOffsetUnit, &pTrigger->offsetUnit));
  TAOS_CHECK_RETURN(tjsonGetTinyIntValue(
    pJson, jkPeriodTriggerPrecision, &pTrigger->precision));
  TAOS_CHECK_RETURN(tjsonGetBigIntValue(
    pJson, jkPeriodTriggerPeriod, &pTrigger->period));
  TAOS_CHECK_RETURN(tjsonGetBigIntValue(
    pJson, jkPeriodTriggerOffset, &pTrigger->offset));
  return TSDB_CODE_SUCCESS;
}

static int32_t int32ToJson(const void* pObj, SJson* pJson) {
  const int32_t* pInt = (const int32_t*)pObj;
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "value", *pInt));
  return TSDB_CODE_SUCCESS;
}

static int32_t jsonToInt32(const SJson* pJson, void* pObj) {
  int32_t* pInt = (int32_t*)pObj;
  TAOS_CHECK_RETURN(tjsonGetIntValue(pJson, "value", pInt));
  return TSDB_CODE_SUCCESS;
}

static const char* jkSstreamCalcScanVgList        = "vgList";
static const char* jkSstreamCalcScanReadFromCache = "readFromCache";
static const char* jkSstreamCalcScanScanPlan      = "scanPlan";
static int32_t calcScanPlanToJson(const void* pObj, SJson* pJson) {
  const SStreamCalcScan* pPlan = (const SStreamCalcScan*)pObj;
  TAOS_CHECK_RETURN(tjsonAddArray(
    pJson, jkSstreamCalcScanVgList, int32ToJson,
    pPlan->vgList ? TARRAY_GET_ELEM(pPlan->vgList, 0) : NULL, sizeof(int32_t),
    pPlan->vgList ? pPlan->vgList->size : 0));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkSstreamCalcScanReadFromCache, pPlan->readFromCache));
  if (NULL != pPlan->scanPlan) {
    TAOS_CHECK_RETURN(tjsonAddStringToObject(
      pJson, jkSstreamCalcScanScanPlan, (const char*)pPlan->scanPlan));
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t jsonToCalcScanPlan(const SJson* pJson, void* pObj) {
  SStreamCalcScan* pPlan = (SStreamCalcScan*)pObj;
  TAOS_CHECK_RETURN(tjsonToTArray(
    pJson, jkSstreamCalcScanVgList, jsonToInt32,
    &pPlan->vgList, sizeof(int32_t)));
  TAOS_CHECK_RETURN(tjsonGetTinyIntValue(
    pJson, jkSstreamCalcScanReadFromCache, &pPlan->readFromCache));
  TAOS_CHECK_RETURN(tjsonDupStringValue(
    pJson, jkSstreamCalcScanScanPlan, (char**)&pPlan->scanPlan));
  return TSDB_CODE_SUCCESS;
}

static const char* jkSDataTypeType      = "type";
static const char* jkSDataTypePrecision = "precision";
static const char* jkSDataTypeScale     = "scale";
static const char* jkSDataTypeBytes     = "bytes";
static int32_t sDataTypeToJson(const void* pObj, SJson* pJson) {
  const SDataType* pType = (const SDataType*)pObj;
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkSDataTypeType, pType->type));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkSDataTypePrecision, pType->precision));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkSDataTypeScale, pType->scale));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkSDataTypeBytes, pType->bytes));
  return TSDB_CODE_SUCCESS;
}

static int32_t jsonToSDataType(const SJson* pJson, void* pObj) {
  SDataType* pType = (SDataType*)pObj;
  TAOS_CHECK_RETURN(tjsonGetTinyIntValue(
    pJson, jkSDataTypeType, &pType->type));
  TAOS_CHECK_RETURN(tjsonGetTinyIntValue(
    pJson, jkSDataTypePrecision, &pType->precision));
  TAOS_CHECK_RETURN(tjsonGetTinyIntValue(
    pJson, jkSDataTypeScale, &pType->scale));
  TAOS_CHECK_RETURN(tjsonGetIntValue(
    pJson, jkSDataTypeBytes, &pType->bytes));
  return TSDB_CODE_SUCCESS;
}

static const char* jkSStreamOutColExpr = "expr";
static const char* jkSStreamOutColType = "type";
static int32_t sStreamOutColToJson(const void* pObj, SJson* pJson) {
  const SStreamOutCol* pCol = (const SStreamOutCol*)pObj;
  if (NULL != pCol->expr) {
    TAOS_CHECK_RETURN(tjsonAddStringToObject(
      pJson, jkSStreamOutColExpr, (const char*)pCol->expr));
  }
  TAOS_CHECK_RETURN(tjsonAddObject(
    pJson, jkSStreamOutColType, sDataTypeToJson, &pCol->type));
  return TSDB_CODE_SUCCESS;
}

static int32_t jsonToSStreamOutCol(const SJson* pJson, void* pObj) {
  SStreamOutCol* pCol = (SStreamOutCol*)pObj;
  TAOS_CHECK_RETURN(tjsonDupStringValue(
    pJson, jkSStreamOutColExpr, (char**)&pCol->expr));
  TAOS_CHECK_RETURN(tjsonToObject(
    pJson, jkSStreamOutColType, jsonToSDataType, &pCol->type));
  return TSDB_CODE_SUCCESS;
}

static int32_t stringToJson(const void* pObj, SJson* pJson) {
  const char** pStr = (const char**)pObj;
  TAOS_CHECK_RETURN(tjsonAddStringToObject(pJson, "value", *pStr));
  return TSDB_CODE_SUCCESS;
}

static int32_t jsonToString(const SJson* pJson, void* pObj) {
  char** pStr = (char**)pObj;
  TAOS_CHECK_RETURN(tjsonDupStringValue(pJson, "value", pStr));
  return TSDB_CODE_SUCCESS;
}

static const char* jkCreateStreamReqName                 = "name";
static const char* jkCreateStreamReqStreamId             = "streamId";
static const char* jkCreateStreamReqSql                  = "sql";

static const char* jkCreateStreamReqStreamDB             = "streamDB";
static const char* jkCreateStreamReqTriggerDB            = "triggerDB";
static const char* jkCreateStreamReqOutDB                = "outDB";
static const char* jkCreateStreamReqCalcDB               = "calcDB";

static const char* jkCreateStreamReqTriggerTblName       = "triggerTblName";
static const char* jkCreateStreamReqOutTblName           = "outTblName";

static const char* jkCreateStreamReqIgExists             = "igExists";
static const char* jkCreateStreamReqTriggerType          = "triggerType";
static const char* jkCreateStreamReqIgDisorder           = "igDisorder";
static const char* jkCreateStreamReqDeleteReCalc         = "deleteReCalc";
static const char* jkCreateStreamReqDeleteOutTbl         = "deleteOutTbl";
static const char* jkCreateStreamReqFillHistory          = "fillHistory";
static const char* jkCreateStreamReqFillHistoryFirst     = "fillHistoryFirst";
static const char* jkCreateStreamReqCalcNotifyOnly       = "calcNotifyOnly";
static const char* jkCreateStreamReqLowLatencyCalc       = "lowLatencyCalc";
static const char* jkCreateStreamReqIgNoDataTrigger      = "igNoDataTrigger";

static const char* jkCreateStreamReqPNotifyAddrUrls      = "pNotifyAddrUrls";
static const char* jkCreateStreamReqNotifyEventTypes     = "notifyEventTypes";
static const char* jkCreateStreamReqAddOptions           = "addOptions";
static const char* jkCreateStreamReqNotifyHistory        = "notifyHistory";

static const char* jkCreateStreamReqTriggerFilterCols    = "triggerFilterCols";
static const char* jkCreateStreamReqTriggerCols          = "triggerCols";
static const char* jkCreateStreamReqPartitionCols        = "partitionCols";
static const char* jkCreateStreamReqOutCols              = "outCols";
static const char* jkCreateStreamReqOutTags              = "outTags";
static const char* jkCreateStreamReqMaxDelay             = "maxDelay";
static const char* jkCreateStreamReqFillHistoryStartTime = 
  "fillHistoryStartTime";
static const char* jkCreateStreamReqWatermark            = "watermark";
static const char* jkCreateStreamReqExpiredTime          = "expiredTime";
static const char* jkCreateStreamReqTrigger              = "trigger";

static const char* jkCreateStreamReqTriggerTblType       = "triggerTblType";
static const char* jkCreateStreamReqTriggerTblUid        = "triggerTblUid";
static const char* jkCreateStreamReqTriggerTblSuid       = "triggerTblSuid";
static const char* jkCreateStreamReqTriggerPrec          = "triggerPrec";
static const char* jkCreateStreamReqVtableCalc           = "vtableCalc";
static const char* jkCreateStreamReqOutTblType           = "outTblType";
static const char* jkCreateStreamReqOutStbExists         = "outStbExists";
static const char* jkCreateStreamReqOutStbUid            = "outStbUid";
static const char* jkCreateStreamReqOutStbSversion       = "outStbSversion";
static const char* jkCreateStreamReqEventTypes           = "eventTypes";
static const char* jkCreateStreamReqFlags                = "flags";
static const char* jkCreateStreamReqTsmaId               = "tsmaId";
static const char* jkCreateStreamReqPlaceHolderBitmap    = "placeHolderBitmap";
static const char* jkCreateStreamReqCalcTsSlotId         = "calcTsSlotId";
static const char* jkCreateStreamReqTriTsSlotId          = "triTsSlotId";

static const char* jkCreateStreamReqTriggerTblVgId       = "triggerTblVgId";
static const char* jkCreateStreamReqOutTblVgId           = "outTblVgId";

static const char* jkCreateStreamReqTriggerScanPlan      = "triggerScanPlan";
static const char* jkCreateStreamReqCalcScanPlanList     = "calcScanPlanList";

static const char* jkCreateStreamReqTriggerHasPF         = "triggerHasPF";
static const char* jkCreateStreamReqTriggerPrevFilter    = "triggerPrevFilter";

static const char* jkCreateStreamReqNumOfCalcSubplan     = "numOfCalcSubplan";
static const char* jkCreateStreamReqCalcPlan             = "calcPlan";
static const char* jkCreateStreamReqSubTblNameExpr       = "subTblNameExpr";
static const char* jkCreateStreamReqTagValueExpr         = "tagValueExpr";
static const char* jkCreateStreamReqForceOutCols         = "forceOutCols";

static int32_t scmCreateStreamReqToJsonImpl(const void* pObj, void* pJson) {
  const SCMCreateStreamReq* pReq = (const SCMCreateStreamReq*)pObj;
  if (NULL != pReq->name) {
    TAOS_CHECK_RETURN(tjsonAddStringToObject(
      pJson, jkCreateStreamReqName, pReq->name));
  }
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkCreateStreamReqStreamId, pReq->streamId));
  if (NULL != pReq->sql) {
    TAOS_CHECK_RETURN(tjsonAddStringToObject(
      pJson, jkCreateStreamReqSql, pReq->sql));
  }
  if (NULL != pReq->streamDB) {
    TAOS_CHECK_RETURN(tjsonAddStringToObject(
      pJson, jkCreateStreamReqStreamDB, pReq->streamDB));
  }
  if (NULL != pReq->triggerDB) {
    TAOS_CHECK_RETURN(tjsonAddStringToObject(
      pJson, jkCreateStreamReqTriggerDB, pReq->triggerDB));
  }
  if (NULL != pReq->outDB) {
    TAOS_CHECK_RETURN(tjsonAddStringToObject(
      pJson, jkCreateStreamReqOutDB, pReq->outDB));
  }
  TAOS_CHECK_RETURN(tjsonAddArray(
    pJson, jkCreateStreamReqCalcDB, stringToJson,
    pReq->calcDB ? TARRAY_GET_ELEM(pReq->calcDB, 0) : NULL,
    pReq->calcDB ? pReq->calcDB->elemSize : 0,
    pReq->calcDB ? pReq->calcDB->size : 0));
  if (NULL != pReq->triggerTblName) {
    TAOS_CHECK_RETURN(tjsonAddStringToObject(
      pJson, jkCreateStreamReqTriggerTblName, pReq->triggerTblName));
  }
  if (NULL != pReq->outTblName) {
    TAOS_CHECK_RETURN(tjsonAddStringToObject(
      pJson, jkCreateStreamReqOutTblName, pReq->outTblName));
  }
  // trigger contol part
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkCreateStreamReqIgExists, pReq->igExists));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkCreateStreamReqTriggerType, pReq->triggerType));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkCreateStreamReqIgDisorder, pReq->igDisorder));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkCreateStreamReqDeleteReCalc, pReq->deleteReCalc));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkCreateStreamReqDeleteOutTbl, pReq->deleteOutTbl));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkCreateStreamReqFillHistory, pReq->fillHistory));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkCreateStreamReqFillHistoryFirst, pReq->fillHistoryFirst));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkCreateStreamReqCalcNotifyOnly, pReq->calcNotifyOnly));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkCreateStreamReqLowLatencyCalc, pReq->lowLatencyCalc));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkCreateStreamReqIgNoDataTrigger, pReq->igNoDataTrigger));

  // notify part
  TAOS_CHECK_RETURN(tjsonAddArray(
    pJson, jkCreateStreamReqPNotifyAddrUrls, stringToJson,
    pReq->pNotifyAddrUrls ? TARRAY_GET_ELEM(pReq->pNotifyAddrUrls, 0) : NULL,
    pReq->pNotifyAddrUrls ? pReq->pNotifyAddrUrls->elemSize : 0,
    pReq->pNotifyAddrUrls ? pReq->pNotifyAddrUrls->size : 0));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkCreateStreamReqNotifyEventTypes, pReq->notifyEventTypes));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkCreateStreamReqAddOptions, pReq->addOptions));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkCreateStreamReqNotifyHistory, pReq->notifyHistory));

  // out table part
  // trigger cols and partition cols
  if (NULL != pReq->triggerFilterCols) {
    TAOS_CHECK_RETURN(tjsonAddStringToObject(
      pJson, jkCreateStreamReqTriggerFilterCols,
      (const char*)pReq->triggerFilterCols));
  }
  if (NULL != pReq->triggerCols) {
    TAOS_CHECK_RETURN(tjsonAddStringToObject(
      pJson, jkCreateStreamReqTriggerCols, (const char*)pReq->triggerCols));
  }
  if (NULL != pReq->partitionCols) {
    TAOS_CHECK_RETURN(tjsonAddStringToObject(
      pJson, jkCreateStreamReqPartitionCols, (const char*)pReq->partitionCols));
  }

  // out cols
  TAOS_CHECK_RETURN(tjsonAddArray(
    pJson, jkCreateStreamReqOutCols, sfieldWithOptionsToJson,
    pReq->outCols ? TARRAY_GET_ELEM(pReq->outCols, 0) : NULL,
    pReq->outCols ? pReq->outCols->elemSize : 0,
    pReq->outCols ? pReq->outCols->size : 0));
  // out tags
  TAOS_CHECK_RETURN(tjsonAddArray(
    pJson, jkCreateStreamReqOutTags, stagFieldWithOptionsToJson,
    pReq->outTags ? TARRAY_GET_ELEM(pReq->outTags, 0) : NULL,
    pReq->outTags ? pReq->outTags->elemSize : 0,
    pReq->outTags ? pReq->outTags->size : 0));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkCreateStreamReqMaxDelay, pReq->maxDelay));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkCreateStreamReqFillHistoryStartTime, pReq->fillHistoryStartTime));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkCreateStreamReqWatermark, pReq->watermark));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkCreateStreamReqExpiredTime, pReq->expiredTime));
  // trigger
  switch (pReq->triggerType) {
    case WINDOW_TYPE_SESSION:
      TAOS_CHECK_RETURN(tjsonAddObject(pJson, jkCreateStreamReqTrigger,
        sessionTriggerToJson, &pReq->trigger));
      break;

    case WINDOW_TYPE_STATE:
      TAOS_CHECK_RETURN(tjsonAddObject(pJson, jkCreateStreamReqTrigger,
        stateTriggerToJson, &pReq->trigger));
      break;

    case WINDOW_TYPE_INTERVAL:
      TAOS_CHECK_RETURN(tjsonAddObject(pJson, jkCreateStreamReqTrigger,
        slidingTriggerToJson, &pReq->trigger));
      break;

    case WINDOW_TYPE_EVENT:
      TAOS_CHECK_RETURN(tjsonAddObject(pJson, jkCreateStreamReqTrigger,
        eventTriggerToJson, &pReq->trigger));
      break;

    case WINDOW_TYPE_COUNT:
      TAOS_CHECK_RETURN(tjsonAddObject(pJson, jkCreateStreamReqTrigger,
        countTriggerToJson, &pReq->trigger));
      break;

    case WINDOW_TYPE_PERIOD:
      TAOS_CHECK_RETURN(tjsonAddObject(pJson, jkCreateStreamReqTrigger,
        periodTriggerToJson, &pReq->trigger));
      break;

  default:
    return TSDB_CODE_STREAM_INVALID_TRIGGER;
  }

  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkCreateStreamReqTriggerTblType, pReq->triggerTblType));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkCreateStreamReqTriggerTblUid, pReq->triggerTblUid));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkCreateStreamReqTriggerTblSuid, pReq->triggerTblSuid));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkCreateStreamReqTriggerPrec, pReq->triggerPrec));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkCreateStreamReqVtableCalc, pReq->vtableCalc));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkCreateStreamReqOutTblType, pReq->outTblType));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkCreateStreamReqOutStbExists, pReq->outStbExists));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkCreateStreamReqOutStbUid, pReq->outStbUid));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkCreateStreamReqOutStbSversion, pReq->outStbSversion));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkCreateStreamReqEventTypes, pReq->eventTypes));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkCreateStreamReqFlags, pReq->flags));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkCreateStreamReqTsmaId, pReq->tsmaId));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkCreateStreamReqPlaceHolderBitmap, pReq->placeHolderBitmap));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkCreateStreamReqCalcTsSlotId, pReq->calcTsSlotId));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkCreateStreamReqTriTsSlotId, pReq->triTsSlotId));
  
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkCreateStreamReqTriggerTblVgId, pReq->triggerTblVgId));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkCreateStreamReqOutTblVgId, pReq->outTblVgId));

  if (NULL != pReq->triggerScanPlan) {
    TAOS_CHECK_RETURN(tjsonAddStringToObject(
      pJson, jkCreateStreamReqTriggerScanPlan, (const char*)pReq->triggerScanPlan));
  }
  TAOS_CHECK_RETURN(tjsonAddArray(
    pJson, jkCreateStreamReqCalcScanPlanList, calcScanPlanToJson,
    pReq->calcScanPlanList ? TARRAY_GET_ELEM(pReq->calcScanPlanList, 0) : NULL,
    pReq->calcScanPlanList ? pReq->calcScanPlanList->elemSize : 0,
    pReq->calcScanPlanList ? pReq->calcScanPlanList->size : 0));

  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkCreateStreamReqTriggerHasPF, pReq->triggerHasPF));
  if (NULL != pReq->triggerPrevFilter) {
    TAOS_CHECK_RETURN(tjsonAddStringToObject(
      pJson, jkCreateStreamReqTriggerPrevFilter,
      (const char*)pReq->triggerPrevFilter));
  }

  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkCreateStreamReqNumOfCalcSubplan, pReq->numOfCalcSubplan));
  if (NULL != pReq->calcPlan) {
    TAOS_CHECK_RETURN(tjsonAddStringToObject(
      pJson, jkCreateStreamReqCalcPlan, (const char*)pReq->calcPlan));
  }
  if (NULL != pReq->subTblNameExpr) {
    TAOS_CHECK_RETURN(tjsonAddStringToObject(pJson,
      jkCreateStreamReqSubTblNameExpr, (const char*)pReq->subTblNameExpr));
  }
  if (NULL != pReq->tagValueExpr) {
    TAOS_CHECK_RETURN(tjsonAddStringToObject(
      pJson, jkCreateStreamReqTagValueExpr, (const char*)pReq->tagValueExpr));
  }
  TAOS_CHECK_RETURN(tjsonAddArray(
    pJson, jkCreateStreamReqForceOutCols, sStreamOutColToJson,
    pReq->forceOutCols ? TARRAY_GET_ELEM(pReq->forceOutCols, 0) : NULL,
    pReq->forceOutCols ? pReq->forceOutCols->elemSize : 0,
    pReq->forceOutCols ? pReq->forceOutCols->size : 0));

  return TSDB_CODE_SUCCESS;
}

int32_t scmCreateStreamReqToJson(
  const SCMCreateStreamReq* pReq, bool format, char** ppStr, int32_t* pStrLen) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t streamId = pReq ? pReq->streamId : -1;
  TSDB_CHECK_NULL(pReq, code, lino, _end, TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
  TSDB_CHECK_NULL(ppStr, code, lino, _end, TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
  TSDB_CHECK_NULL(
    pStrLen, code, lino, _end, TSDB_CODE_MND_STREAM_INTERNAL_ERROR);

  SJson* pJson = tjsonCreateObject();
  TSDB_CHECK_NULL(pJson, code, lino, _end, terrno);
  TSDB_CHECK_CODE(scmCreateStreamReqToJsonImpl(pReq, pJson), lino, _end);

  if (TSDB_CODE_SUCCESS == code) {
    *ppStr = format ? tjsonToString(pJson) : tjsonToUnformattedString(pJson);
    if (*ppStr == NULL) {
      code = terrno;
    } else {
      *pStrLen = strlen(*ppStr);
    }
  }

_end:
  if (TSDB_CODE_SUCCESS != code) {
    uError(
      "failed to convert SCMCreateStreamReq to json, lino: %d, since %s",
      lino, tstrerror(code));
  }
  tjsonDelete(pJson);
  return code;
}

int32_t jsonToSCMCreateStreamReq(const void* pJson, void* pObj) {
  SCMCreateStreamReq* pReq = (SCMCreateStreamReq*)pObj;
  TAOS_CHECK_RETURN(tjsonDupStringValue(
    pJson, jkCreateStreamReqName, (char**)&pReq->name));
  TAOS_CHECK_RETURN(tjsonGetBigIntValue(
    pJson, jkCreateStreamReqStreamId, &pReq->streamId));
  TAOS_CHECK_RETURN(tjsonDupStringValue(
    pJson, jkCreateStreamReqSql, (char**)&pReq->sql));

  TAOS_CHECK_RETURN(tjsonDupStringValue(
    pJson, jkCreateStreamReqStreamDB, (char**)&pReq->streamDB));
  TAOS_CHECK_RETURN(tjsonDupStringValue(
    pJson, jkCreateStreamReqTriggerDB, (char**)&pReq->triggerDB));
  TAOS_CHECK_RETURN(tjsonDupStringValue(
    pJson, jkCreateStreamReqOutDB, (char**)&pReq->outDB));
  TAOS_CHECK_RETURN(tjsonToTArray(
    pJson, jkCreateStreamReqCalcDB, jsonToString,
    &pReq->calcDB, POINTER_BYTES));
  TAOS_CHECK_RETURN(tjsonDupStringValue(
    pJson, jkCreateStreamReqTriggerTblName, (char**)&pReq->triggerTblName));
  TAOS_CHECK_RETURN(tjsonDupStringValue(
    pJson, jkCreateStreamReqOutTblName, (char**)&pReq->outTblName));

  // trigger control part
  TAOS_CHECK_RETURN(tjsonGetTinyIntValue(
    pJson, jkCreateStreamReqIgExists, &pReq->igExists));
  TAOS_CHECK_RETURN(tjsonGetTinyIntValue(
    pJson, jkCreateStreamReqTriggerType, &pReq->triggerType));
  TAOS_CHECK_RETURN(tjsonGetTinyIntValue(
    pJson, jkCreateStreamReqIgDisorder, &pReq->igDisorder));
  TAOS_CHECK_RETURN(tjsonGetTinyIntValue(
    pJson, jkCreateStreamReqDeleteReCalc, &pReq->deleteReCalc));
  TAOS_CHECK_RETURN(tjsonGetTinyIntValue(
    pJson, jkCreateStreamReqDeleteOutTbl, &pReq->deleteOutTbl));
  TAOS_CHECK_RETURN(tjsonGetTinyIntValue(
    pJson, jkCreateStreamReqFillHistory, &pReq->fillHistory));
  TAOS_CHECK_RETURN(tjsonGetTinyIntValue(
    pJson, jkCreateStreamReqFillHistoryFirst, &pReq->fillHistoryFirst));
  TAOS_CHECK_RETURN(tjsonGetTinyIntValue(
    pJson, jkCreateStreamReqCalcNotifyOnly, &pReq->calcNotifyOnly));
  TAOS_CHECK_RETURN(tjsonGetTinyIntValue(
    pJson, jkCreateStreamReqLowLatencyCalc, &pReq->lowLatencyCalc));
  TAOS_CHECK_RETURN(tjsonGetTinyIntValue(
    pJson, jkCreateStreamReqIgNoDataTrigger, &pReq->igNoDataTrigger));

  // notify part
  TAOS_CHECK_RETURN(tjsonToTArray(
    pJson, jkCreateStreamReqPNotifyAddrUrls, jsonToString,
    &pReq->pNotifyAddrUrls, POINTER_BYTES));
  TAOS_CHECK_RETURN(tjsonGetIntValue(
    pJson, jkCreateStreamReqNotifyEventTypes, &pReq->notifyEventTypes));
  TAOS_CHECK_RETURN(tjsonGetIntValue(
    pJson, jkCreateStreamReqAddOptions, &pReq->addOptions));
  TAOS_CHECK_RETURN(tjsonGetTinyIntValue(
    pJson, jkCreateStreamReqNotifyHistory, &pReq->notifyHistory));

  TAOS_CHECK_RETURN(tjsonDupStringValue(
    pJson, jkCreateStreamReqTriggerFilterCols,
    (char**)&pReq->triggerFilterCols));
  TAOS_CHECK_RETURN(tjsonDupStringValue(
    pJson, jkCreateStreamReqTriggerCols, (char**)&pReq->triggerCols));
  TAOS_CHECK_RETURN(tjsonDupStringValue(
    pJson, jkCreateStreamReqPartitionCols, (char**)&pReq->partitionCols));
  // out cols
  TAOS_CHECK_RETURN(tjsonToTArray(
    pJson, jkCreateStreamReqOutCols, jsonToSFieldWithOptions,
    &pReq->outCols, sizeof(SFieldWithOptions)));
  // out tags
  TAOS_CHECK_RETURN(tjsonToTArray(
    pJson, jkCreateStreamReqOutTags, jsonToSTagFieldWithOptions,
    &pReq->outTags, sizeof(SFieldWithOptions)));
  TAOS_CHECK_RETURN(tjsonGetBigIntValue(
    pJson, jkCreateStreamReqMaxDelay, &pReq->maxDelay));
  TAOS_CHECK_RETURN(tjsonGetBigIntValue(
    pJson, jkCreateStreamReqFillHistoryStartTime, &pReq->fillHistoryStartTime));
  TAOS_CHECK_RETURN(tjsonGetBigIntValue(
    pJson, jkCreateStreamReqWatermark, &pReq->watermark));
  TAOS_CHECK_RETURN(tjsonGetBigIntValue(
    pJson, jkCreateStreamReqExpiredTime, &pReq->expiredTime));
  // trigger
  switch (pReq->triggerType) {
    case WINDOW_TYPE_SESSION:
      TAOS_CHECK_RETURN(tjsonToObject(
        pJson, jkCreateStreamReqTrigger, jsonToSessionTrigger, &pReq->trigger));
      break;
    
    case WINDOW_TYPE_STATE:
      TAOS_CHECK_RETURN(tjsonToObject(
        pJson, jkCreateStreamReqTrigger, jsonToStateTrigger, &pReq->trigger));
      break;

    case WINDOW_TYPE_INTERVAL:
      TAOS_CHECK_RETURN(tjsonToObject(
        pJson, jkCreateStreamReqTrigger, jsonToSlidingTrigger, &pReq->trigger));
      break;
    
    case WINDOW_TYPE_EVENT:
      TAOS_CHECK_RETURN(tjsonToObject(
        pJson, jkCreateStreamReqTrigger, jsonToEventTrigger, &pReq->trigger));
      break;
    
    case WINDOW_TYPE_COUNT:
      TAOS_CHECK_RETURN(tjsonToObject(
        pJson, jkCreateStreamReqTrigger, jsonToCountTrigger, &pReq->trigger));
      break;
    
    case WINDOW_TYPE_PERIOD:
      TAOS_CHECK_RETURN(tjsonToObject(
        pJson, jkCreateStreamReqTrigger, jsonToPeriodTrigger, &pReq->trigger));
      break;
    
    default:
      return TSDB_CODE_STREAM_INVALID_TRIGGER;
  }

  TAOS_CHECK_RETURN(tjsonGetTinyIntValue(
    pJson, jkCreateStreamReqTriggerTblType, &pReq->triggerTblType));
  TAOS_CHECK_RETURN(tjsonGetUBigIntValue(
    pJson, jkCreateStreamReqTriggerTblUid, &pReq->triggerTblUid));
  TAOS_CHECK_RETURN(tjsonGetUBigIntValue(
    pJson, jkCreateStreamReqTriggerTblSuid, &pReq->triggerTblSuid));
  TAOS_CHECK_RETURN(tjsonGetUTinyIntValue(
    pJson, jkCreateStreamReqTriggerPrec, &pReq->triggerPrec));
  TAOS_CHECK_RETURN(tjsonGetTinyIntValue(
    pJson, jkCreateStreamReqVtableCalc, &pReq->vtableCalc));
  TAOS_CHECK_RETURN(tjsonGetTinyIntValue(
    pJson, jkCreateStreamReqOutTblType, &pReq->outTblType));
  TAOS_CHECK_RETURN(tjsonGetTinyIntValue(
    pJson, jkCreateStreamReqOutStbExists, &pReq->outStbExists));
  TAOS_CHECK_RETURN(tjsonGetUBigIntValue(
    pJson, jkCreateStreamReqOutStbUid, &pReq->outStbUid));
  TAOS_CHECK_RETURN(tjsonGetIntValue(
    pJson, jkCreateStreamReqOutStbSversion, &pReq->outStbSversion));
  TAOS_CHECK_RETURN(tjsonGetBigIntValue(
    pJson, jkCreateStreamReqEventTypes, &pReq->eventTypes));
  TAOS_CHECK_RETURN(tjsonGetBigIntValue(
    pJson, jkCreateStreamReqFlags, &pReq->flags));
  TAOS_CHECK_RETURN(tjsonGetBigIntValue(
    pJson, jkCreateStreamReqTsmaId, &pReq->tsmaId));
  TAOS_CHECK_RETURN(tjsonGetBigIntValue(
    pJson, jkCreateStreamReqPlaceHolderBitmap, &pReq->placeHolderBitmap));
  TAOS_CHECK_RETURN(tjsonGetSmallIntValue(
    pJson, jkCreateStreamReqCalcTsSlotId, &pReq->calcTsSlotId));
  TAOS_CHECK_RETURN(tjsonGetSmallIntValue(
    pJson, jkCreateStreamReqTriTsSlotId, &pReq->triTsSlotId));

  TAOS_CHECK_RETURN(tjsonGetIntValue(
    pJson, jkCreateStreamReqTriggerTblVgId, &pReq->triggerTblVgId));
  TAOS_CHECK_RETURN(tjsonGetIntValue(
    pJson, jkCreateStreamReqOutTblVgId, &pReq->outTblVgId));

  TAOS_CHECK_RETURN(tjsonDupStringValue(
    pJson, jkCreateStreamReqTriggerScanPlan, (char**)&pReq->triggerScanPlan));
  TAOS_CHECK_RETURN(tjsonToTArray(
    pJson, jkCreateStreamReqCalcScanPlanList, jsonToCalcScanPlan,
    &pReq->calcScanPlanList, sizeof(SStreamCalcScan)));

  TAOS_CHECK_RETURN(tjsonGetTinyIntValue(
    pJson, jkCreateStreamReqTriggerHasPF, &pReq->triggerHasPF));
  TAOS_CHECK_RETURN(tjsonDupStringValue(
    pJson, jkCreateStreamReqTriggerPrevFilter,
    (char**)&pReq->triggerPrevFilter));
  TAOS_CHECK_RETURN(tjsonGetIntValue(
    pJson, jkCreateStreamReqNumOfCalcSubplan, &pReq->numOfCalcSubplan));
  TAOS_CHECK_RETURN(tjsonDupStringValue(
    pJson, jkCreateStreamReqCalcPlan, (char**)&pReq->calcPlan));
  TAOS_CHECK_RETURN(tjsonDupStringValue(
    pJson, jkCreateStreamReqSubTblNameExpr, (char**)&pReq->subTblNameExpr));
  TAOS_CHECK_RETURN(tjsonDupStringValue(
    pJson, jkCreateStreamReqTagValueExpr, (char**)&pReq->tagValueExpr));
  TAOS_CHECK_RETURN(tjsonToTArray(
    pJson, jkCreateStreamReqForceOutCols,
    jsonToSStreamOutCol, &pReq->forceOutCols, sizeof(SStreamOutCol)));

  return TSDB_CODE_SUCCESS;
}
