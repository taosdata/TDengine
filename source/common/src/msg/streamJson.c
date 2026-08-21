#include "streamMsg.h"
#include "tbase64.h"
#include "tjson.h"

static int32_t int16ToJson(const void* pObj, SJson* pJson);
static int32_t jsonToInt16(const SJson* pJson, void* pObj);

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
  TAOS_CHECK_RETURN(tjsonGetStringValue1(pJson, jkFieldName, pField->name, sizeof(pField->name)));
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
  TAOS_CHECK_RETURN(tjsonAddStringToObject(pJson, jkFieldName, pField->name));
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
  TAOS_CHECK_RETURN(tjsonGetStringValue1(pJson, jkFieldName, pField->name, sizeof(pField->name)));
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

/* forward compat: decode old "slotId" single-value payloads */
static const char* jkStateTriggerSlotId           = "slotId";
static const char* jkStateTriggerSlotIds          = "slotIds";
static const char* jkStateTriggerExtend           = "extend";
static const char* jkStateTriggerZeroth           = "zeroth";
static const char* jkStateTriggerTrueForType      = "trueForType";
static const char* jkStateTriggerTrueForCount     = "trueForCount";
static const char* jkStateTriggerTrueForDuration  = "trueForDuration";
static const char* jkStateTriggerExpr             = "expr";
static int32_t stateTriggerToJson(const void* pObj, SJson* pJson) {
  const SStateWinTrigger* pTrigger = (const SStateWinTrigger*)pObj;
  TAOS_CHECK_RETURN(tjsonAddTArray(
    pJson, jkStateTriggerSlotIds,
    int16ToJson, pTrigger->pSlotIds));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkStateTriggerExtend, pTrigger->extend));
  if (NULL != pTrigger->zeroth) {
    TAOS_CHECK_RETURN(tjsonAddStringToObject(
      pJson, jkStateTriggerZeroth, (const char*)pTrigger->zeroth));
  }
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, jkStateTriggerTrueForType, pTrigger->trueForType));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, jkStateTriggerTrueForCount, pTrigger->trueForCount));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, jkStateTriggerTrueForDuration, pTrigger->trueForDuration));
  if (NULL != pTrigger->expr) {
    TAOS_CHECK_RETURN(tjsonAddStringToObject(
      pJson, jkStateTriggerExpr, (const char*)pTrigger->expr));
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t jsonToStateTrigger(const SJson* pJson, void* pObj) {
  SStateWinTrigger* pTrigger = (SStateWinTrigger*)pObj;
  SJson* pSlotIds = tjsonGetObjectItem(pJson, jkStateTriggerSlotIds);
  if (pSlotIds != NULL) {
    TAOS_CHECK_RETURN(tjsonToTArray(
      pJson, jkStateTriggerSlotIds, jsonToInt16, &pTrigger->pSlotIds, sizeof(int16_t)));
  } else if (tjsonGetObjectItem(pJson, jkStateTriggerSlotId) != NULL) {
    int16_t slotId = -1;
    TAOS_CHECK_RETURN(tjsonGetSmallIntValue(pJson, jkStateTriggerSlotId, &slotId));
    pTrigger->pSlotIds = taosArrayInit(1, sizeof(int16_t));
    if (pTrigger->pSlotIds == NULL) {
      return terrno;
    }
    if (taosArrayPush(pTrigger->pSlotIds, &slotId) == NULL) {
      return terrno;
    }
  }
  TAOS_CHECK_RETURN(
    tjsonGetSmallIntValue(pJson, jkStateTriggerExtend, &pTrigger->extend));
  TAOS_CHECK_RETURN(tjsonDupStringValue(
    pJson, jkStateTriggerZeroth, (char**)&pTrigger->zeroth));
  TAOS_CHECK_RETURN(tjsonGetIntValue(pJson, jkStateTriggerTrueForType, &pTrigger->trueForType));
  TAOS_CHECK_RETURN(tjsonGetIntValue(pJson, jkStateTriggerTrueForCount, &pTrigger->trueForCount));
  TAOS_CHECK_RETURN(tjsonGetBigIntValue(pJson, jkStateTriggerTrueForDuration, &pTrigger->trueForDuration));
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

static const char* jkEventTriggerStartCond            = "startCond";
static const char* jkEventTriggerEndCond              = "endCond";
static const char* jkEventTriggerTrueForType          = "trueForType";
static const char* jkEventTriggerTrueForCount         = "trueForCount";
static const char* jkEventTriggerTrueForDuration      = "trueForDuration";
static const char* jkEventTriggerStartTrueForType     = "startTrueForType";
static const char* jkEventTriggerStartTrueForCount    = "startTrueForCount";
static const char* jkEventTriggerStartTrueForDuration = "startTrueForDuration";
static const char* jkEventTriggerEndTrueForType       = "endTrueForType";
static const char* jkEventTriggerEndTrueForCount      = "endTrueForCount";
static const char* jkEventTriggerEndTrueForDuration   = "endTrueForDuration";
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
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, jkEventTriggerTrueForType, pTrigger->trueForType));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, jkEventTriggerTrueForCount, pTrigger->trueForCount));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, jkEventTriggerTrueForDuration, pTrigger->trueForDuration));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, jkEventTriggerStartTrueForType, pTrigger->startTrueForType));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, jkEventTriggerStartTrueForCount, pTrigger->startTrueForCount));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, jkEventTriggerStartTrueForDuration, pTrigger->startTrueForDuration));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, jkEventTriggerEndTrueForType, pTrigger->endTrueForType));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, jkEventTriggerEndTrueForCount, pTrigger->endTrueForCount));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, jkEventTriggerEndTrueForDuration, pTrigger->endTrueForDuration));
  return TSDB_CODE_SUCCESS;
}

static int32_t jsonToEventTrigger(const SJson* pJson, void* pObj) {
  SEventTrigger* pTrigger = (SEventTrigger*)pObj;
  TAOS_CHECK_RETURN(tjsonDupStringValue(
    pJson, jkEventTriggerStartCond, (char**)&pTrigger->startCond));
  TAOS_CHECK_RETURN(tjsonDupStringValue(
    pJson, jkEventTriggerEndCond, (char**)&pTrigger->endCond));
  TAOS_CHECK_RETURN(tjsonGetIntValue(pJson, jkEventTriggerTrueForType, &pTrigger->trueForType));
  TAOS_CHECK_RETURN(tjsonGetIntValue(pJson, jkEventTriggerTrueForCount, &pTrigger->trueForCount));
  TAOS_CHECK_RETURN(tjsonGetBigIntValue(pJson, jkEventTriggerTrueForDuration, &pTrigger->trueForDuration));
  // New fields are optional for backward compatibility. Missing keys are
  // treated as zero here explicitly instead of relying on caller-side
  // zero-initialization or tjson helper defaults.
  pTrigger->startTrueForType = 0;
  pTrigger->startTrueForCount = 0;
  pTrigger->startTrueForDuration = 0;
  pTrigger->endTrueForType = 0;
  pTrigger->endTrueForCount = 0;
  pTrigger->endTrueForDuration = 0;
  (void)tjsonGetIntValue(pJson, jkEventTriggerStartTrueForType, &pTrigger->startTrueForType);
  (void)tjsonGetIntValue(pJson, jkEventTriggerStartTrueForCount, &pTrigger->startTrueForCount);
  (void)tjsonGetBigIntValue(pJson, jkEventTriggerStartTrueForDuration, &pTrigger->startTrueForDuration);
  (void)tjsonGetIntValue(pJson, jkEventTriggerEndTrueForType, &pTrigger->endTrueForType);
  (void)tjsonGetIntValue(pJson, jkEventTriggerEndTrueForCount, &pTrigger->endTrueForCount);
  (void)tjsonGetBigIntValue(pJson, jkEventTriggerEndTrueForDuration, &pTrigger->endTrueForDuration);
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

static int32_t int16ToJson(const void* pObj, SJson* pJson) {
  const int16_t* pInt = (const int16_t*)pObj;
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "value", *pInt));
  return TSDB_CODE_SUCCESS;
}

static int32_t jsonToInt16(const SJson* pJson, void* pObj) {
  int16_t* pInt = (int16_t*)pObj;
  TAOS_CHECK_RETURN(tjsonGetSmallIntValue(pJson, "value", pInt));
  return TSDB_CODE_SUCCESS;
}

static const char* jkSstreamCalcScanVgList        = "vgList";
static const char* jkSstreamCalcScanReadFromCache = "readFromCache";
static const char* jkSstreamCalcScanScanPlan      = "scanPlan";
static const char* jkSstreamCalcScanSourceName    = "sourceName";
static const char* jkSstreamCalcScanExtTable      = "extTable";
static const char* jkSstreamCalcScanTsColumn      = "tsColumn";
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
  /* Per-scan ext source identity (federated multi-source calc). Only written
   * when set, so non-ext streams stay unchanged and old readers ignore them. */
  if (pPlan->sourceName[0] != '\0') {
    TAOS_CHECK_RETURN(tjsonAddStringToObject(pJson, jkSstreamCalcScanSourceName, pPlan->sourceName));
  }
  if (pPlan->extTable[0] != '\0') {
    TAOS_CHECK_RETURN(tjsonAddStringToObject(pJson, jkSstreamCalcScanExtTable, pPlan->extTable));
  }
  if (pPlan->tsColumn[0] != '\0') {
    TAOS_CHECK_RETURN(tjsonAddStringToObject(pJson, jkSstreamCalcScanTsColumn, pPlan->tsColumn));
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
  /* Missing keys leave the fields empty (old streams) — tjsonGetStringValue1
   * returns SUCCESS without touching the buffer, so single-source fallback holds. */
  TAOS_CHECK_RETURN(tjsonGetStringValue1(pJson, jkSstreamCalcScanSourceName, pPlan->sourceName,
                                         sizeof(pPlan->sourceName)));
  TAOS_CHECK_RETURN(tjsonGetStringValue1(pJson, jkSstreamCalcScanExtTable, pPlan->extTable,
                                         sizeof(pPlan->extTable)));
  TAOS_CHECK_RETURN(tjsonGetStringValue1(pJson, jkSstreamCalcScanTsColumn, pPlan->tsColumn,
                                         sizeof(pPlan->tsColumn)));
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
static const char* jkCreateStreamReqMultiGroupCalc       = "multiGroupCalc";

static const char* jkCreateStreamReqPNotifyAddrUrls      = "pNotifyAddrUrls";
static const char* jkCreateStreamReqNotifyEventTypes     = "notifyEventTypes";
static const char* jkCreateStreamReqAddOptions           = "addOptions";
static const char* jkCreateStreamReqNotifyHistory        = "notifyHistory";

static const char* jkCreateStreamReqTriggerFilterCols    = "triggerFilterCols";
static const char* jkCreateStreamReqTriggerCols          = "triggerCols";
static const char* jkCreateStreamReqPartitionCols        = "partitionCols";
static const char* jkCreateStreamReqRollupTagCols        = "rollupTagCols";
static const char* jkCreateStreamReqOutCols              = "outCols";
static const char* jkCreateStreamReqOutTags              = "outTags";
static const char* jkCreateStreamReqMaxDelay             = "maxDelay";
static const char* jkCreateStreamReqFillHistoryStartTime = 
  "fillHistoryStartTime";
static const char* jkCreateStreamReqWatermark            = "watermark";
static const char* jkCreateStreamReqExpiredTime          = "expiredTime";
static const char* jkCreateStreamReqIdleTimeoutMs        = "idleTimeoutMs";
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
static const char* jkCreateStreamReqCalcPkSlotId         = "calcPkSlotId";
static const char* jkCreateStreamReqTriPkSlotId          = "triPkSlotId";

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
static const char* jkCreateStreamReqWindowPlan = "WindowPlan";
static const char* jkStreamWindowPlanVersion = "Version";
static const char* jkStreamWindowPlanLayers = "Layers";
static const char* jkStreamWindowLayerName = "name";
static const char* jkStreamWindowLayerTriggerType = "triggerType";
static const char* jkStreamWindowLayerPlaceholderMask = "placeholderMask";
static const char* jkStreamWindowLayerInput = "input";
static const char* jkStreamWindowLayerTrigger = "trigger";
static const char* jkStreamWindowLayerInputTsSlotId = "tsSlotId";
static const char* jkStreamWindowLayerInputPkSlotId = "pkSlotId";
static const char* jkStreamWindowLayerInputEventStartSlotId = "eventStartSlotId";
static const char* jkStreamWindowLayerInputEventEndSlotId = "eventEndSlotId";
static const char* jkStreamWindowLayerInputConditionSlotIds = "conditionSlotIds";

static const char* jkCreateStreamReqColCids = "colCids";
static const char* jkCreateStreamReqTagCids = "tagCids";
static const char* jkCreateStreamReqNodelayCreateSubtable = "nodelayCreateSubtable";

/* === Federated query: extSpecs JSON keys (Pt A6) ===
 * Wire format mirrors SStreamExtTriggerSpec (streamMsg.h). This is the sole
 * persisted form of SStreamExtTriggerSpec (SStreamObj no longer has its own
 * extSpecs field/binary codec; SCMCreateStreamReq.extSpecs is authoritative
 * for the whole lifetime of the stream object, mnode-filled fields included).
 * encryptedPassword travels as base64 (see extSpecEncryptedPasswordToJson);
 * encryptedPasswordLen is also written as a sanity/length flag. Old mnodes
 * silently ignore the unknown jkCreateStreamReqExtSpecs key. */
static const char* jkCreateStreamReqExtSpecs           = "extSpecs";
static const char* jkExtSpecSourceName                  = "sourceName";
static const char* jkExtSpecSourceType                  = "sourceType";
static const char* jkExtSpecExtDb                       = "extDb";
static const char* jkExtSpecExtSchema                   = "extSchema";
static const char* jkExtSpecExtTable                    = "extTable";
static const char* jkExtSpecTsColumn                    = "tsColumn";
static const char* jkExtSpecTriggerColumns               = "triggerColumns";
static const char* jkExtSpecHost                        = "host";
static const char* jkExtSpecPort                        = "port";
static const char* jkExtSpecUser                        = "user";
static const char* jkExtSpecEncryptedPassword           = "encryptedPassword";
static const char* jkExtSpecConnCfgVersion              = "connCfgVersion";
static const char* jkExtSpecOptions                     = "options";
static const char* jkExtSpecPrefilter                   = "prefilter";
static const char* jkExtSpecTriggerPrefilter             = "triggerPrefilter";
static const char* jkExtSpecPartitionByTag               = "partitionByTag";
static const char* jkExtSpecPartitionByTbname             = "partitionByTbname";
static const char* jkExtSpecPartitionTagCols             = "partitionTagCols";
static const char* jkExtSpecPartitionTagExprs             = "partitionTagExprs";

/* partitionTagCols element codec: SArray<char[TSDB_COL_NAME_LEN]> — the
 * element IS the fixed-size name buffer itself (not a char*), unlike
 * stringToJson/jsonToString above which operate on SArray<char*>. */
static int32_t extSpecTagColToJson(const void* pObj, SJson* pJson) {
  TAOS_CHECK_RETURN(tjsonAddStringToObject(pJson, "value", (const char*)pObj));
  return TSDB_CODE_SUCCESS;
}

static int32_t jsonToExtSpecTagCol(const SJson* pJson, void* pObj) {
  TAOS_CHECK_RETURN(tjsonGetStringValue(pJson, "value", (char*)pObj));
  return TSDB_CODE_SUCCESS;
}

static int32_t extTriggerSpecToJson(const void* pObj, SJson* pJson) {
  const SStreamExtTriggerSpec* pSpec = (const SStreamExtTriggerSpec*)pObj;
  TAOS_CHECK_RETURN(tjsonAddStringToObject(pJson, jkExtSpecSourceName, pSpec->sourceName));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, jkExtSpecSourceType, pSpec->sourceType));
  TAOS_CHECK_RETURN(tjsonAddStringToObject(pJson, jkExtSpecExtDb, pSpec->extDb));
  TAOS_CHECK_RETURN(tjsonAddStringToObject(pJson, jkExtSpecExtSchema, pSpec->extSchema));
  TAOS_CHECK_RETURN(tjsonAddStringToObject(pJson, jkExtSpecExtTable, pSpec->extTable));
  TAOS_CHECK_RETURN(tjsonAddStringToObject(pJson, jkExtSpecTsColumn, pSpec->tsColumn));
  /* triggerColumns: col names referenced by the trigger. Currently always
   * empty (attached at reader-task deploy time, not by the parser), but
   * round-tripped for completeness since SCMCreateStreamReq.extSpecs is now
   * the sole persisted home for SStreamExtTriggerSpec. */
  if (pSpec->triggerColumns != NULL && taosArrayGetSize(pSpec->triggerColumns) > 0) {
    TAOS_CHECK_RETURN(tjsonAddTArray(pJson, jkExtSpecTriggerColumns, extSpecTagColToJson, pSpec->triggerColumns));
  }
  TAOS_CHECK_RETURN(tjsonAddStringToObject(pJson, jkExtSpecHost, pSpec->host));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, jkExtSpecPort, pSpec->port));
  TAOS_CHECK_RETURN(tjsonAddStringToObject(pJson, jkExtSpecUser, pSpec->user));
  /* encryptedPassword: base64-encoded AES-CBC ciphertext. Absent until mnode
   * fills it in from sdb (P1 B2); encryptedPasswordLen is written either way
   * as a sanity/length flag. */
  char*   pB64    = NULL;
  int32_t b64Code = base64_encode(pSpec->encryptedPassword, TSDB_EXT_SOURCE_ENC_PASSWORD_LEN, &pB64);
  if (b64Code != TSDB_CODE_SUCCESS) {
    return b64Code;
  }
  b64Code = tjsonAddStringToObject(pJson, jkExtSpecEncryptedPassword, pB64);
  taosMemoryFree(pB64);
  TAOS_CHECK_RETURN(b64Code);
  
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, jkExtSpecConnCfgVersion,
                                            (int64_t)pSpec->connCfgVersion));
  TAOS_CHECK_RETURN(tjsonAddStringToObject(pJson, jkExtSpecOptions, pSpec->options));
  /* prefilter: optional WHERE clause fragment for calc (aggregate) reader queries. */
  if (pSpec->prefilter != NULL && pSpec->prefilter[0] != '\0') {
    TAOS_CHECK_RETURN(tjsonAddStringToObject(pJson, jkExtSpecPrefilter, pSpec->prefilter));
  }
  /* triggerPrefilter: optional WHERE clause fragment for trigger reader queries (PRE_FILTER). */
  if (pSpec->triggerPrefilter != NULL && pSpec->triggerPrefilter[0] != '\0') {
    TAOS_CHECK_RETURN(tjsonAddStringToObject(pJson, jkExtSpecTriggerPrefilter, pSpec->triggerPrefilter));
  }
  /* partitionByTag / partitionTagCols: PARTITION BY groupId derivation (see
   * streamReaderExt.c). Must cross the wire here — unlike calcColumns
   * (deferred to reader-task deploy time, derivable from the already-
   * transmitted scan plan), the PARTITION BY tag subset is a parse-time-only
   * fact the mnode cannot re-derive later. */
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, jkExtSpecPartitionByTag, pSpec->partitionByTag));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, jkExtSpecPartitionByTbname, pSpec->partitionByTbname));
  if (pSpec->partitionTagCols != NULL && taosArrayGetSize(pSpec->partitionTagCols) > 0) {
    TAOS_CHECK_RETURN(tjsonAddTArray(pJson, jkExtSpecPartitionTagCols, extSpecTagColToJson, pSpec->partitionTagCols));
  }
  /* partitionTagExprs: parallel to partitionTagCols -- see
   * SStreamExtTriggerSpec.partitionTagExprs in streamMsg.h. */
  if (pSpec->partitionTagExprs != NULL && taosArrayGetSize(pSpec->partitionTagExprs) > 0) {
    TAOS_CHECK_RETURN(tjsonAddTArray(pJson, jkExtSpecPartitionTagExprs, stringToJson, pSpec->partitionTagExprs));
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t jsonToExtTriggerSpec(const SJson* pJson, void* pObj) {
  /* tjsonToTArray decodes into a calloc'd buffer of size elemSize. We pass
   * sizeof(SStreamExtTriggerSpec*) and decode INTO a stack copy first, then
   * allocate a real heap spec and store its pointer. This matches the
   * SArray<SStreamExtTriggerSpec*> shape used by SCMCreateStreamReq.extSpecs. */
  SStreamExtTriggerSpec** ppOut = (SStreamExtTriggerSpec**)pObj;
  SStreamExtTriggerSpec*  pSpec = taosMemoryCalloc(1, sizeof(SStreamExtTriggerSpec));
  if (pSpec == NULL) return terrno;

  int32_t code = TSDB_CODE_SUCCESS;
  if ((code = tjsonGetStringValue(pJson, jkExtSpecSourceName, pSpec->sourceName)) != 0) goto _err;
  if ((code = tjsonGetTinyIntValue(pJson, jkExtSpecSourceType, &pSpec->sourceType)) != 0) goto _err;
  if ((code = tjsonGetStringValue(pJson, jkExtSpecExtDb, pSpec->extDb)) != 0) goto _err;
  /* extSchema is optional in the JSON (absent in streams created by older taosc);
   * tolerate missing key by ignoring TSDB_CODE_SUCCESS with empty result. */
  if ((code = tjsonGetStringValue(pJson, jkExtSpecExtSchema, pSpec->extSchema)) != 0) goto _err;
  if ((code = tjsonGetStringValue(pJson, jkExtSpecExtTable, pSpec->extTable)) != 0) goto _err;
  if ((code = tjsonGetStringValue(pJson, jkExtSpecTsColumn, pSpec->tsColumn)) != 0) goto _err;
  /* triggerColumns: absent means empty (currently always the case). */
  if ((code = tjsonToTArray(pJson, jkExtSpecTriggerColumns, jsonToExtSpecTagCol,
                            &pSpec->triggerColumns, TSDB_COL_NAME_LEN)) != 0) {
    goto _err;
  }
  if ((code = tjsonGetStringValue(pJson, jkExtSpecHost, pSpec->host)) != 0) goto _err;
  {
    int32_t p32 = 0;
    if ((code = tjsonGetIntValue(pJson, jkExtSpecPort, &p32)) != 0) goto _err;
    pSpec->port = (uint16_t)p32;
  }
  if ((code = tjsonGetStringValue(pJson, jkExtSpecUser, pSpec->user)) != 0) goto _err;
  {
    int64_t v = 0;
    if ((code = tjsonGetBigIntValue(pJson, jkExtSpecConnCfgVersion, &v)) != 0) goto _err;
    pSpec->connCfgVersion = (uint64_t)v;
  }
  /* encryptedPassword: base64-encoded ciphertext. Absent means the spec has
   * no credential yet (e.g. freshly parsed by taosc, not yet filled by mnode
   * (P1 B2), or refreshed by msmRefreshExtSpecPasswords on redeploy). */
  {
    char b64Buf[TSDB_EXT_SOURCE_ENC_PASSWORD_LEN * 2] = {0};
    int32_t b64Code = tjsonGetStringValue(pJson, jkExtSpecEncryptedPassword, b64Buf);
    if (b64Code == TSDB_CODE_SUCCESS && b64Buf[0] != '\0') {
      uint8_t* pRaw   = NULL;
      int32_t  rawLen = 0;
      if ((code = base64_decode(b64Buf, (int32_t)strlen(b64Buf), &rawLen, &pRaw)) != 0) {
        taosMemoryFree(pRaw);
        goto _err;
      }
      if (rawLen > TSDB_EXT_SOURCE_ENC_PASSWORD_LEN) {
        taosMemoryFree(pRaw);
        code = TSDB_CODE_OUT_OF_RANGE;
        goto _err;
      }
      (void)memcpy(pSpec->encryptedPassword, pRaw, rawLen);
      taosMemoryFree(pRaw);
    }
  }
  /* options: connection options JSON string (e.g. api_token, protocol).
   * Absent means no options were configured on the ext source. */
  if ((code = tjsonGetStringValue(pJson, jkExtSpecOptions, pSpec->options)) != 0) goto _err;

  /* prefilter: optional calc reader WHERE fragment.  Absent on streams created
   * by older taosc or when no static WHERE was present in the calc query. */
  {
    char pfBuf[4096] = {0};
    int32_t pfCode = tjsonGetStringValue(pJson, jkExtSpecPrefilter, pfBuf);
    if (pfCode == TSDB_CODE_SUCCESS && pfBuf[0] != '\0') {
      pSpec->prefilter    = tstrdup(pfBuf);
      if (pSpec->prefilter == NULL) { code = terrno; goto _err; }
    }
  }
  /* triggerPrefilter: optional trigger reader PRE_FILTER fragment. */
  {
    char pfBuf[4096] = {0};
    int32_t pfCode = tjsonGetStringValue(pJson, jkExtSpecTriggerPrefilter, pfBuf);
    if (pfCode == TSDB_CODE_SUCCESS && pfBuf[0] != '\0') {
      pSpec->triggerPrefilter    = tstrdup(pfBuf);
      if (pSpec->triggerPrefilter == NULL) { code = terrno; goto _err; }
    }
  }
  /* partitionByTag: absent on streams created by older taosc; defaults to 0
   * (calloc'd) when the key is missing, matching "no PARTITION BY". */
  (void)tjsonGetTinyIntValue(pJson, jkExtSpecPartitionByTag, &pSpec->partitionByTag);
  /* partitionByTbname: same backward-compat default (0) as partitionByTag. */
  (void)tjsonGetTinyIntValue(pJson, jkExtSpecPartitionByTbname, &pSpec->partitionByTbname);
  /* partitionTagCols is absent only when there is no PARTITION BY list.
   * Every list item otherwise owns one positional column/expression slot. */
  if ((code = tjsonToTArray(pJson, jkExtSpecPartitionTagCols, jsonToExtSpecTagCol,
                            &pSpec->partitionTagCols, TSDB_COL_NAME_LEN)) != 0) {
    goto _err;
  }
  /* partitionTagExprs: parallel to partitionTagCols -- see
   * SStreamExtTriggerSpec.partitionTagExprs in streamMsg.h. */
  if ((code = tjsonToTArray(pJson, jkExtSpecPartitionTagExprs, jsonToString,
                            &pSpec->partitionTagExprs, POINTER_BYTES)) != 0) {
    goto _err;
  }
  *ppOut = pSpec;
  return TSDB_CODE_SUCCESS;

_err:
  tFreeSStreamExtTriggerSpec(pSpec);
  return code;
}

static int32_t streamWindowLayerInputToJson(const void* pObj, SJson* pJson) {
  const SStreamWindowLayerInputSpec* pInput = (const SStreamWindowLayerInputSpec*)pObj;
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, jkStreamWindowLayerInputTsSlotId, pInput->tsSlotId));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, jkStreamWindowLayerInputPkSlotId, pInput->pkSlotId));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, jkStreamWindowLayerInputEventStartSlotId, pInput->eventStartSlotId));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, jkStreamWindowLayerInputEventEndSlotId, pInput->eventEndSlotId));
  if (taosArrayGetSize(pInput->pConditionSlotIds) == 0) {
    return tjsonAddArrayToObject(pJson, jkStreamWindowLayerInputConditionSlotIds) == NULL ? terrno : TSDB_CODE_SUCCESS;
  }
  TAOS_CHECK_RETURN(
      tjsonAddTArray(pJson, jkStreamWindowLayerInputConditionSlotIds, int16ToJson, pInput->pConditionSlotIds));
  return TSDB_CODE_SUCCESS;
}

static int32_t jsonToStreamWindowLayerInput(const SJson* pJson, void* pObj) {
  SStreamWindowLayerInputSpec* pInput = (SStreamWindowLayerInputSpec*)pObj;
  int32_t                      code = TSDB_CODE_SUCCESS;
  if (tjsonGetObjectItem(pJson, jkStreamWindowLayerInputTsSlotId) == NULL ||
      tjsonGetObjectItem(pJson, jkStreamWindowLayerInputPkSlotId) == NULL ||
      tjsonGetObjectItem(pJson, jkStreamWindowLayerInputEventStartSlotId) == NULL ||
      tjsonGetObjectItem(pJson, jkStreamWindowLayerInputEventEndSlotId) == NULL ||
      tjsonGetObjectItem(pJson, jkStreamWindowLayerInputConditionSlotIds) == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  TAOS_CHECK_RETURN(tjsonGetSmallIntValue(pJson, jkStreamWindowLayerInputTsSlotId, &pInput->tsSlotId));
  TAOS_CHECK_RETURN(tjsonGetSmallIntValue(pJson, jkStreamWindowLayerInputPkSlotId, &pInput->pkSlotId));
  TAOS_CHECK_RETURN(tjsonGetSmallIntValue(pJson, jkStreamWindowLayerInputEventStartSlotId, &pInput->eventStartSlotId));
  TAOS_CHECK_RETURN(tjsonGetSmallIntValue(pJson, jkStreamWindowLayerInputEventEndSlotId, &pInput->eventEndSlotId));
  code = tjsonToTArray(pJson, jkStreamWindowLayerInputConditionSlotIds, jsonToInt16, &pInput->pConditionSlotIds,
                       sizeof(int16_t));
  if (code == TSDB_CODE_SUCCESS && pInput->pConditionSlotIds == NULL) {
    pInput->pConditionSlotIds = taosArrayInit(0, sizeof(int16_t));
    if (pInput->pConditionSlotIds == NULL) code = terrno;
  }
  return code;
}

static int32_t streamWindowLayerTriggerToJson(const SStreamWindowLayerSpec* pLayer, SJson* pJson) {
  switch (pLayer->triggerType) {
    case WINDOW_TYPE_SESSION:
      return tjsonAddObject(pJson, jkStreamWindowLayerTrigger, sessionTriggerToJson, &pLayer->trigger);
    case WINDOW_TYPE_STATE:
      return tjsonAddObject(pJson, jkStreamWindowLayerTrigger, stateTriggerToJson, &pLayer->trigger);
    case WINDOW_TYPE_INTERVAL:
      return tjsonAddObject(pJson, jkStreamWindowLayerTrigger, slidingTriggerToJson, &pLayer->trigger);
    case WINDOW_TYPE_EVENT:
      return tjsonAddObject(pJson, jkStreamWindowLayerTrigger, eventTriggerToJson, &pLayer->trigger);
    case WINDOW_TYPE_COUNT:
      return tjsonAddObject(pJson, jkStreamWindowLayerTrigger, countTriggerToJson, &pLayer->trigger);
    case WINDOW_TYPE_PERIOD:
      return tjsonAddObject(pJson, jkStreamWindowLayerTrigger, periodTriggerToJson, &pLayer->trigger);
    default:
      return TSDB_CODE_STREAM_INVALID_TRIGGER;
  }
}

static int32_t jsonToStreamWindowLayerTrigger(const SJson* pJson, SStreamWindowLayerSpec* pLayer) {
  SJson* pTrigger = tjsonGetObjectItem(pJson, jkStreamWindowLayerTrigger);
  if (pTrigger == NULL) return TSDB_CODE_INVALID_PARA;
  switch (pLayer->triggerType) {
    case WINDOW_TYPE_SESSION:
      return jsonToSessionTrigger(pTrigger, &pLayer->trigger);
    case WINDOW_TYPE_STATE:
      return jsonToStateTrigger(pTrigger, &pLayer->trigger);
    case WINDOW_TYPE_INTERVAL:
      return jsonToSlidingTrigger(pTrigger, &pLayer->trigger);
    case WINDOW_TYPE_EVENT:
      return jsonToEventTrigger(pTrigger, &pLayer->trigger);
    case WINDOW_TYPE_COUNT:
      return jsonToCountTrigger(pTrigger, &pLayer->trigger);
    case WINDOW_TYPE_PERIOD:
      return jsonToPeriodTrigger(pTrigger, &pLayer->trigger);
    default:
      return TSDB_CODE_STREAM_INVALID_TRIGGER;
  }
}

static int32_t streamWindowLayerToJson(const void* pObj, SJson* pJson) {
  const SStreamWindowLayerSpec* pLayer = (const SStreamWindowLayerSpec*)pObj;
  TAOS_CHECK_RETURN(tjsonAddStringToObject(pJson, jkStreamWindowLayerName, pLayer->name));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, jkStreamWindowLayerTriggerType, pLayer->triggerType));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, jkStreamWindowLayerPlaceholderMask, pLayer->placeholderMask));
  TAOS_CHECK_RETURN(tjsonAddObject(pJson, jkStreamWindowLayerInput, streamWindowLayerInputToJson, &pLayer->input));
  return streamWindowLayerTriggerToJson(pLayer, pJson);
}

static int32_t jsonToStreamWindowLayer(const SJson* pJson, void* pObj) {
  SStreamWindowLayerSpec* pLayer = (SStreamWindowLayerSpec*)pObj;
  int32_t                 code = TSDB_CODE_SUCCESS;
  SJson*                  pInput = tjsonGetObjectItem(pJson, jkStreamWindowLayerInput);
  if (tjsonGetObjectItem(pJson, jkStreamWindowLayerName) == NULL ||
      tjsonGetObjectItem(pJson, jkStreamWindowLayerTriggerType) == NULL ||
      tjsonGetObjectItem(pJson, jkStreamWindowLayerPlaceholderMask) == NULL || pInput == NULL ||
      tjsonGetObjectItem(pJson, jkStreamWindowLayerTrigger) == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  if ((code = tjsonGetStringValue1(pJson, jkStreamWindowLayerName, pLayer->name, sizeof(pLayer->name))) != 0) {
    goto _exit;
  }
  if ((code = tjsonGetTinyIntValue(pJson, jkStreamWindowLayerTriggerType, &pLayer->triggerType)) != 0) goto _exit;
  if ((code = tjsonGetBigIntValue(pJson, jkStreamWindowLayerPlaceholderMask, &pLayer->placeholderMask)) != 0) {
    goto _exit;
  }
  if ((code = jsonToStreamWindowLayerInput(pInput, &pLayer->input)) != 0) goto _exit;
  if ((code = jsonToStreamWindowLayerTrigger(pJson, pLayer)) != 0) goto _exit;
  return TSDB_CODE_SUCCESS;

_exit:
  return code;
}

static int32_t streamWindowPlanToJson(const void* pObj, SJson* pJson) {
  const SStreamWindowPlan* pPlan = (const SStreamWindowPlan*)pObj;
  if (pPlan->pLayers == NULL || pPlan->pLayers->elemSize != sizeof(SStreamWindowLayerSpec)) {
    return TSDB_CODE_INVALID_PARA;
  }
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, jkStreamWindowPlanVersion, pPlan->version));
  return tjsonAddTArray(pJson, jkStreamWindowPlanLayers, streamWindowLayerToJson, pPlan->pLayers);
}

static int32_t jsonToStreamWindowPlan(const SJson* pJson, SCMCreateStreamReq* pReq) {
  SJson*     pPlanJson = tjsonGetObjectItem(pJson, jkCreateStreamReqWindowPlan);
  const bool nested = BIT_FLAG_TEST_MASK(pReq->addOptions, STREAM_OPTION_NESTED_WINDOW_PLAN);
  const bool flushOnOuterClose = BIT_FLAG_TEST_MASK(pReq->addOptions, STREAM_OPTION_FLUSH_ON_OUTER_CLOSE);
  int32_t    code = TSDB_CODE_SUCCESS;
  int32_t    numLayers = 0;
  if (pReq->pWindowPlan != NULL) return TSDB_CODE_INVALID_PARA;
  if (pPlanJson == NULL) return nested || flushOnOuterClose ? TSDB_CODE_INVALID_PARA : TSDB_CODE_SUCCESS;
  if (!nested || tjsonGetObjectItem(pPlanJson, jkStreamWindowPlanVersion) == NULL ||
      tjsonGetObjectItem(pPlanJson, jkStreamWindowPlanLayers) == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  SJson* pLayers = tjsonGetObjectItem(pPlanJson, jkStreamWindowPlanLayers);
  if (!tjsonIsArray(pLayers) || (numLayers = tjsonGetArraySize(pLayers)) < 2 || numLayers > STREAM_WINDOW_MAX_LAYERS) {
    return TSDB_CODE_INVALID_PARA;
  }

  SStreamWindowPlan* pPlan = taosMemoryCalloc(1, sizeof(*pPlan));
  if (pPlan == NULL) return terrno;
  if ((code = tjsonGetIntValue(pPlanJson, jkStreamWindowPlanVersion, &pPlan->version)) != 0) goto _exit;
  pPlan->pLayers = taosArrayInit(numLayers, sizeof(SStreamWindowLayerSpec));
  if (pPlan->pLayers == NULL) {
    code = terrno;
    goto _exit;
  }
  for (int32_t i = 0; i < numLayers; ++i) {
    SStreamWindowLayerSpec emptyLayer = {};
    SJson*                 pLayerJson = tjsonGetArrayItem(pLayers, i);
    if (pLayerJson == NULL) {
      code = TSDB_CODE_INVALID_PARA;
      goto _exit;
    }
    SStreamWindowLayerSpec* pLayer = taosArrayPush(pPlan->pLayers, &emptyLayer);
    if (pLayer == NULL) {
      code = terrno;
      goto _exit;
    }
    if ((code = jsonToStreamWindowLayer(pLayerJson, pLayer)) != 0) goto _exit;
  }

  SStreamWindowPlanValidationCtx intrinsicCtx = {0};
  intrinsicCtx.deleteRecalc = pReq->deleteReCalc;
  intrinsicCtx.ignoreNoDataTrigger = pReq->igNoDataTrigger;
  intrinsicCtx.flushOnOuterClose = flushOnOuterClose;
  intrinsicCtx.eventTypes = pReq->eventTypes;
  if ((code = tValidateStreamWindowPlan(pPlan, &intrinsicCtx)) != 0) goto _exit;
  if ((code = tValidateStreamWindowPlanLeafProjection(pPlan, pReq->triggerType, &pReq->trigger)) != 0) goto _exit;

  pReq->pWindowPlan = pPlan;
  return TSDB_CODE_SUCCESS;

_exit:
  tDestroyStreamWindowPlan(&pPlan);
  return code;
}

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
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkCreateStreamReqMultiGroupCalc, pReq->enableMultiGroupCalc));

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
  if (NULL != pReq->rollupTagCols) {
    TAOS_CHECK_RETURN(tjsonAddStringToObject(pJson, jkCreateStreamReqRollupTagCols, (const char*)pReq->rollupTagCols));
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
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkCreateStreamReqIdleTimeoutMs, pReq->idleTimeoutMs));
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
    pJson, jkCreateStreamReqCalcPkSlotId, pReq->calcPkSlotId));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(
    pJson, jkCreateStreamReqTriPkSlotId, pReq->triPkSlotId));
  
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
  TAOS_CHECK_RETURN(tjsonAddArray(
      pJson, jkCreateStreamReqColCids, int16ToJson,
      pReq->colCids ? TARRAY_GET_ELEM(pReq->colCids, 0) : NULL,
      pReq->colCids ? pReq->colCids->elemSize : 0,
      pReq->colCids ? pReq->colCids->size : 0));
  TAOS_CHECK_RETURN(tjsonAddArray(
      pJson, jkCreateStreamReqTagCids, int16ToJson,
      pReq->tagCids ? TARRAY_GET_ELEM(pReq->tagCids, 0) : NULL,
      pReq->tagCids ? pReq->tagCids->elemSize : 0,
      pReq->tagCids ? pReq->tagCids->size : 0));
  TAOS_CHECK_RETURN(
      tjsonAddIntegerToObject(pJson, jkCreateStreamReqNodelayCreateSubtable, pReq->nodelayCreateSubtable));

  if (pReq->pWindowPlan != NULL) {
    TAOS_CHECK_RETURN(tjsonAddObject(pJson, jkCreateStreamReqWindowPlan, streamWindowPlanToJson, pReq->pWindowPlan));
  }

  /* Pt A6: federated query extSpecs. Encode as a JSON array of spec objects.
   * Each element is SStreamExtTriggerSpec*; iterate and tjsonAddItemToArray.
   * Skipped entirely when no ext sources referenced (numOfExtSpecs == 0). */
  if (pReq->extSpecs != NULL && pReq->numOfExtSpecs > 0) {
    SJson* pArr = tjsonCreateArray();
    if (pArr == NULL) return terrno;
    int32_t n = (int32_t)taosArrayGetSize(pReq->extSpecs);
    for (int32_t i = 0; i < n; ++i) {
      SStreamExtTriggerSpec* pSpec = *(SStreamExtTriggerSpec**)taosArrayGet(pReq->extSpecs, i);
      if (pSpec == NULL) continue;
      SJson* pItem = tjsonCreateObject();
      if (pItem == NULL) { tjsonDelete(pArr); return terrno; }
      int32_t c = extTriggerSpecToJson(pSpec, pItem);
      if (c != TSDB_CODE_SUCCESS) { tjsonDelete(pItem); tjsonDelete(pArr); return c; }
      if ((c = tjsonAddItemToArray(pArr, pItem)) != 0) { tjsonDelete(pItem); tjsonDelete(pArr); return c; }
    }
    if (tjsonAddItemToObject(pJson, jkCreateStreamReqExtSpecs, pArr) != 0) {
      tjsonDelete(pArr);
      return terrno;
    }
  }

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
  pReq->calcTsSlotId = -1;
  pReq->triTsSlotId = -1;
  pReq->calcPkSlotId = -1;
  pReq->triPkSlotId = -1;
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
  TAOS_CHECK_RETURN(tjsonGetTinyIntValue(
    pJson, jkCreateStreamReqMultiGroupCalc, &pReq->enableMultiGroupCalc));

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
  TAOS_CHECK_RETURN(tjsonDupStringValue(pJson, jkCreateStreamReqRollupTagCols, (char**)&pReq->rollupTagCols));
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
  TAOS_CHECK_RETURN(tjsonGetBigIntValue(
    pJson, jkCreateStreamReqIdleTimeoutMs, &pReq->idleTimeoutMs));
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
  TAOS_CHECK_RETURN(tjsonGetSmallIntValue(
    pJson, jkCreateStreamReqCalcPkSlotId, &pReq->calcPkSlotId));
  TAOS_CHECK_RETURN(tjsonGetSmallIntValue(
    pJson, jkCreateStreamReqTriPkSlotId, &pReq->triPkSlotId));

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
  TAOS_CHECK_RETURN(tjsonToTArray(pJson, jkCreateStreamReqColCids, jsonToInt16, &pReq->colCids, sizeof(int16_t)));
  TAOS_CHECK_RETURN(tjsonToTArray(pJson, jkCreateStreamReqTagCids, jsonToInt16, &pReq->tagCids, sizeof(int16_t)));
  (void)tjsonGetTinyIntValue(pJson, jkCreateStreamReqNodelayCreateSubtable, &pReq->nodelayCreateSubtable);

  /* Pt A6: decode extSpecs array (optional — absent on streams without
   * EXTERNAL sources and on messages from older taosc). */
  SJson* pExtArr = tjsonGetObjectItem(pJson, jkCreateStreamReqExtSpecs);
  if (pExtArr != NULL) {
    int32_t n = tjsonGetArraySize(pExtArr);
    if (n > 0) {
      pReq->extSpecs = taosArrayInit(n, POINTER_BYTES);
      if (pReq->extSpecs == NULL) return terrno;
      for (int32_t i = 0; i < n; ++i) {
        SJson* pItem = tjsonGetArrayItem(pExtArr, i);
        if (pItem == NULL) continue;
        SStreamExtTriggerSpec* pSpec = NULL;
        int32_t c = jsonToExtTriggerSpec(pItem, &pSpec);
        if (c != TSDB_CODE_SUCCESS) return c;
        if (taosArrayPush(pReq->extSpecs, &pSpec) == NULL) {
          tFreeSStreamExtTriggerSpec(pSpec);
          return terrno;
        }
      }
      pReq->numOfExtSpecs = (int32_t)taosArrayGetSize(pReq->extSpecs);
    }
  }

  TAOS_CHECK_RETURN(jsonToStreamWindowPlan(pJson, pReq));

  return TSDB_CODE_SUCCESS;
}
