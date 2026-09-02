#include "streamMsg.h"

#include <limits.h>
#include <string.h>

#include "taoserror.h"

static void destroyWindowLayer(void* pParam);

static bool sameString(const char* left, const char* right) {
  if (left == NULL || right == NULL) return left == right;
  return strcmp(left, right) == 0;
}

static bool sameI16Array(const SArray* left, const SArray* right) {
  if ((left != NULL && left->elemSize != sizeof(int16_t)) || (right != NULL && right->elemSize != sizeof(int16_t))) {
    return false;
  }
  const int32_t leftSize = left == NULL ? 0 : taosArrayGetSize(left);
  const int32_t rightSize = right == NULL ? 0 : taosArrayGetSize(right);
  if (leftSize != rightSize) return false;
  for (int32_t i = 0; i < leftSize; ++i) {
    if (*(const int16_t*)taosArrayGet(left, i) != *(const int16_t*)taosArrayGet(right, i)) return false;
  }
  return true;
}

static void destroyTrigger(int8_t triggerType, SStreamTrigger* pTrigger) {
  if (pTrigger == NULL) return;
  switch (triggerType) {
    case WINDOW_TYPE_STATE:
      taosArrayDestroy(pTrigger->stateWin.pSlotIds);
      taosMemoryFree(pTrigger->stateWin.zeroth);
      taosMemoryFree(pTrigger->stateWin.expr);
      break;
    case WINDOW_TYPE_EVENT:
      taosMemoryFree(pTrigger->event.startCond);
      taosMemoryFree(pTrigger->event.endCond);
      break;
    case WINDOW_TYPE_COUNT:
      taosMemoryFree(pTrigger->count.condCols);
      break;
    default:
      break;
  }
  memset(pTrigger, 0, sizeof(*pTrigger));
}

static void destroyWindowLayer(void* pParam) {
  SStreamWindowLayerSpec* pLayer = (SStreamWindowLayerSpec*)pParam;
  if (pLayer == NULL) return;
  taosArrayDestroy(pLayer->input.pConditionSlotIds);
  destroyTrigger(pLayer->triggerType, &pLayer->trigger);
  memset(pLayer, 0, sizeof(*pLayer));
}

void tDestroyStreamWindowPlan(SStreamWindowPlan** ppPlan) {
  if (ppPlan == NULL || *ppPlan == NULL) return;
  SStreamWindowPlan* pPlan = *ppPlan;
  taosArrayDestroyEx(pPlan->pLayers, destroyWindowLayer);
  taosMemoryFree(pPlan);
  *ppPlan = NULL;
}

static int32_t cloneTrigger(int8_t triggerType, const SStreamTrigger* pSrc, SStreamTrigger* pDst) {
  int32_t code = TSDB_CODE_SUCCESS;
  memset(pDst, 0, sizeof(*pDst));
  switch (triggerType) {
    case WINDOW_TYPE_SESSION:
      pDst->session = pSrc->session;
      break;
    case WINDOW_TYPE_INTERVAL:
      pDst->sliding = pSrc->sliding;
      break;
    case WINDOW_TYPE_PERIOD:
      pDst->period = pSrc->period;
      break;
    case WINDOW_TYPE_STATE:
      pDst->stateWin.extend = pSrc->stateWin.extend;
      pDst->stateWin.trueForType = pSrc->stateWin.trueForType;
      pDst->stateWin.trueForCount = pSrc->stateWin.trueForCount;
      pDst->stateWin.trueForDuration = pSrc->stateWin.trueForDuration;
      if (pSrc->stateWin.pSlotIds != NULL) {
        if (pSrc->stateWin.pSlotIds->elemSize != sizeof(int16_t)) return TSDB_CODE_INVALID_PARA;
        pDst->stateWin.pSlotIds = taosArrayDup(pSrc->stateWin.pSlotIds, NULL);
        if (pDst->stateWin.pSlotIds == NULL) return terrno;
      }
      if (pSrc->stateWin.zeroth != NULL) {
        pDst->stateWin.zeroth = taosStrdup((const char*)pSrc->stateWin.zeroth);
        if (pDst->stateWin.zeroth == NULL) return terrno;
      }
      if (pSrc->stateWin.expr != NULL) {
        pDst->stateWin.expr = taosStrdup((const char*)pSrc->stateWin.expr);
        if (pDst->stateWin.expr == NULL) return terrno;
      }
      break;
    case WINDOW_TYPE_EVENT:
      pDst->event.trueForType = pSrc->event.trueForType;
      pDst->event.trueForCount = pSrc->event.trueForCount;
      pDst->event.trueForDuration = pSrc->event.trueForDuration;
      pDst->event.startTrueForType = pSrc->event.startTrueForType;
      pDst->event.startTrueForCount = pSrc->event.startTrueForCount;
      pDst->event.startTrueForDuration = pSrc->event.startTrueForDuration;
      pDst->event.endTrueForType = pSrc->event.endTrueForType;
      pDst->event.endTrueForCount = pSrc->event.endTrueForCount;
      pDst->event.endTrueForDuration = pSrc->event.endTrueForDuration;
      if (pSrc->event.startCond != NULL) {
        pDst->event.startCond = taosStrdup((const char*)pSrc->event.startCond);
        if (pDst->event.startCond == NULL) return terrno;
      }
      if (pSrc->event.endCond != NULL) {
        pDst->event.endCond = taosStrdup((const char*)pSrc->event.endCond);
        if (pDst->event.endCond == NULL) return terrno;
      }
      break;
    case WINDOW_TYPE_COUNT:
      pDst->count.countVal = pSrc->count.countVal;
      pDst->count.sliding = pSrc->count.sliding;
      if (pSrc->count.condCols != NULL) {
        pDst->count.condCols = taosStrdup((const char*)pSrc->count.condCols);
        if (pDst->count.condCols == NULL) return terrno;
      }
      break;
    default:
      return TSDB_CODE_INVALID_PARA;
  }
  return code;
}

int32_t tCloneStreamWindowPlan(const SStreamWindowPlan* pSrc, SStreamWindowPlan** ppDst) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (pSrc == NULL || ppDst == NULL) return TSDB_CODE_INVALID_PARA;
  *ppDst = NULL;
  SStreamWindowPlan* pDst = taosMemoryCalloc(1, sizeof(*pDst));
  if (pDst == NULL) return terrno;
  pDst->version = pSrc->version;
  if (pSrc->pLayers != NULL) {
    if (pSrc->pLayers->elemSize != sizeof(SStreamWindowLayerSpec)) {
      code = TSDB_CODE_INVALID_PARA;
      goto _exit;
    }
    const int32_t num = taosArrayGetSize(pSrc->pLayers);
    pDst->pLayers = taosArrayInit(num, sizeof(SStreamWindowLayerSpec));
    if (pDst->pLayers == NULL) {
      code = terrno;
      goto _exit;
    }
    for (int32_t i = 0; i < num; ++i) {
      const SStreamWindowLayerSpec* pSrcLayer = taosArrayGet(pSrc->pLayers, i);
      SStreamWindowLayerSpec        dstLayer = {};
      dstLayer.triggerType = pSrcLayer->triggerType;
      memcpy(dstLayer.name, pSrcLayer->name, sizeof(dstLayer.name));
      dstLayer.placeholderMask = pSrcLayer->placeholderMask;
      dstLayer.input.tsSlotId = pSrcLayer->input.tsSlotId;
      dstLayer.input.pkSlotId = pSrcLayer->input.pkSlotId;
      dstLayer.input.eventStartSlotId = pSrcLayer->input.eventStartSlotId;
      dstLayer.input.eventEndSlotId = pSrcLayer->input.eventEndSlotId;
      if (pSrcLayer->input.pConditionSlotIds != NULL) {
        if (pSrcLayer->input.pConditionSlotIds->elemSize != sizeof(int16_t)) {
          code = TSDB_CODE_INVALID_PARA;
          destroyWindowLayer(&dstLayer);
          goto _exit;
        }
        dstLayer.input.pConditionSlotIds = taosArrayDup(pSrcLayer->input.pConditionSlotIds, NULL);
        if (dstLayer.input.pConditionSlotIds == NULL) {
          code = terrno;
          destroyWindowLayer(&dstLayer);
          goto _exit;
        }
      }
      code = cloneTrigger(dstLayer.triggerType, &pSrcLayer->trigger, &dstLayer.trigger);
      if (code != TSDB_CODE_SUCCESS) {
        destroyWindowLayer(&dstLayer);
        goto _exit;
      }
      if (taosArrayPush(pDst->pLayers, &dstLayer) == NULL) {
        code = terrno;
        destroyWindowLayer(&dstLayer);
        goto _exit;
      }
    }
  }
  *ppDst = pDst;
  return TSDB_CODE_SUCCESS;

_exit:
  tDestroyStreamWindowPlan(&pDst);
  return code;
}

static bool isKnownWindowType(int8_t type) {
  return type == WINDOW_TYPE_INTERVAL || type == WINDOW_TYPE_SESSION || type == WINDOW_TYPE_STATE ||
         type == WINDOW_TYPE_EVENT || type == WINDOW_TYPE_COUNT;
}

static bool asciiEqualIgnoreCase(const char* left, const char* right) {
  while (*left != '\0' && *right != '\0') {
    char a = *left++;
    char b = *right++;
    if (a >= 'A' && a <= 'Z') a = (char)(a - 'A' + 'a');
    if (b >= 'A' && b <= 'Z') b = (char)(b - 'A' + 'a');
    if (a != b) return false;
  }
  return *left == '\0' && *right == '\0';
}

static int32_t parseNodeTypeString(const char* value, int32_t* pNodeType) {
  if (value == NULL || pNodeType == NULL || *value == '\0') return TSDB_CODE_INVALID_PARA;
  const bool  negative = *value == '-';
  const char* cursor = value + (negative ? 1 : 0);
  if (*cursor == '\0') return TSDB_CODE_INVALID_PARA;
  const int64_t limit = negative ? -(int64_t)INT32_MIN : INT32_MAX;
  int64_t       parsed = 0;
  for (; *cursor != '\0'; ++cursor) {
    if (*cursor < '0' || *cursor > '9') return TSDB_CODE_INVALID_PARA;
    const int32_t digit = *cursor - '0';
    if (parsed > (limit - digit) / 10) return TSDB_CODE_INVALID_PARA;
    parsed = parsed * 10 + digit;
  }
  *pNodeType = (int32_t)(negative ? -parsed : parsed);
  return TSDB_CODE_SUCCESS;
}

static int32_t eventStartConditionKind(const char* condition, bool* pMultiple) {
  if (condition == NULL || pMultiple == NULL) return TSDB_CODE_INVALID_PARA;
  SJson* pJson = tjsonParse(condition);
  if (pJson == NULL) return TSDB_CODE_INVALID_PARA;
  SJson* pNodeType = tjsonGetObjectItem(pJson, "NodeType");
  if (pNodeType == NULL) {
    tjsonDelete(pJson);
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t     nodeType = 0;
  int32_t     code = TSDB_CODE_SUCCESS;
  const char* nodeTypeString = tjsonGetStringPointer(pJson, "NodeType");
  if (nodeTypeString != NULL) {
    code = parseNodeTypeString(nodeTypeString, &nodeType);
  } else {
    double number = 0;
    code = tjsonGetDoubleValue(pJson, "NodeType", &number);
    if (code == TSDB_CODE_SUCCESS &&
        (number != number || number < INT32_MIN || number > INT32_MAX || (double)(int32_t)number != number)) {
      code = TSDB_CODE_INVALID_PARA;
    }
    if (code == TSDB_CODE_SUCCESS) nodeType = (int32_t)number;
  }
  tjsonDelete(pJson);
  if (code != TSDB_CODE_SUCCESS) return code;
  if (nodeType < QUERY_NODE_COLUMN) return TSDB_CODE_INVALID_PARA;
  *pMultiple = nodeType == QUERY_NODE_NODE_LIST;
  return TSDB_CODE_SUCCESS;
}

static bool hasTrueFor(const SStateWinTrigger* pTrigger) {
  return pTrigger->trueForType != 0 || pTrigger->trueForCount != 0 || pTrigger->trueForDuration != 0;
}

static bool hasEventTrueFor(const SEventTrigger* pTrigger) {
  return pTrigger->trueForType != 0 || pTrigger->trueForCount != 0 || pTrigger->trueForDuration != 0 ||
         pTrigger->startTrueForType != 0 || pTrigger->startTrueForCount != 0 || pTrigger->startTrueForDuration != 0 ||
         pTrigger->endTrueForType != 0 || pTrigger->endTrueForCount != 0 || pTrigger->endTrueForDuration != 0;
}

static bool allTimeOrSession(const SStreamWindowPlan* pPlan) {
  const int32_t num = taosArrayGetSize(pPlan->pLayers);
  for (int32_t i = 0; i < num; ++i) {
    const int8_t type = ((const SStreamWindowLayerSpec*)taosArrayGet(pPlan->pLayers, i))->triggerType;
    if (type != WINDOW_TYPE_INTERVAL && type != WINDOW_TYPE_SESSION) return false;
  }
  return true;
}

static bool layerAllowsPlaceholderMask(const SStreamWindowLayerSpec* pLayer) {
  const int64_t allowed = pLayer->triggerType == WINDOW_TYPE_INTERVAL && pLayer->trigger.sliding.interval == 0
                              ? PLACE_HOLDER_PREV_TS | PLACE_HOLDER_CURRENT_TS | PLACE_HOLDER_NEXT_TS
                              : PLACE_HOLDER_WSTART | PLACE_HOLDER_WEND | PLACE_HOLDER_WDURATION | PLACE_HOLDER_WROWNUM;
  return (pLayer->placeholderMask & ~allowed) == 0;
}

static bool hasValidIntrinsicLayerParameters(const SStreamWindowLayerSpec* pLayer) {
  switch (pLayer->triggerType) {
    case WINDOW_TYPE_SESSION:
      return pLayer->trigger.session.sessionVal >= 0;
    case WINDOW_TYPE_INTERVAL:
      return pLayer->trigger.sliding.interval == 0
                 ? pLayer->trigger.sliding.sliding > 0
                 : pLayer->trigger.sliding.interval > 0 && pLayer->trigger.sliding.sliding > 0;
    case WINDOW_TYPE_STATE:
      return pLayer->input.pConditionSlotIds != NULL && pLayer->input.pConditionSlotIds->elemSize == sizeof(int16_t) &&
             taosArrayGetSize(pLayer->input.pConditionSlotIds) > 0 && pLayer->trigger.stateWin.extend >= 0 &&
             pLayer->trigger.stateWin.extend <= 2;
    case WINDOW_TYPE_COUNT:
      return pLayer->trigger.count.countVal > 0 && pLayer->trigger.count.sliding > 0;
    case WINDOW_TYPE_EVENT:
      return pLayer->trigger.event.startCond != NULL && pLayer->input.eventStartSlotId >= 0;
    default:
      return false;
  }
}

int32_t tValidateStreamWindowPlan(const SStreamWindowPlan* pPlan, const SStreamWindowPlanValidationCtx* pCtx) {
  if (pPlan == NULL || pCtx == NULL || pPlan->version != STREAM_WINDOW_PLAN_VERSION || pPlan->pLayers == NULL ||
      pPlan->pLayers->elemSize != sizeof(SStreamWindowLayerSpec)) {
    return TSDB_CODE_INVALID_PARA;
  }
  const int32_t num = taosArrayGetSize(pPlan->pLayers);
  if (num < 2 || num > STREAM_WINDOW_MAX_LAYERS) return TSDB_CODE_INVALID_PARA;
  bool hasStateCountEvent = false;
  for (int32_t i = 0; i < num; ++i) {
    const SStreamWindowLayerSpec* pLayer = taosArrayGet(pPlan->pLayers, i);
    if (pLayer == NULL || !hasValidIntrinsicLayerParameters(pLayer) ||
        (pLayer->input.pConditionSlotIds != NULL && pLayer->input.pConditionSlotIds->elemSize != sizeof(int16_t)) ||
        (pLayer->triggerType == WINDOW_TYPE_STATE && pLayer->trigger.stateWin.pSlotIds != NULL &&
         pLayer->trigger.stateWin.pSlotIds->elemSize != sizeof(int16_t)) ||
        !layerAllowsPlaceholderMask(pLayer)) {
      return TSDB_CODE_INVALID_PARA;
    }
    const bool   isLeaf = i == num - 1;
    const void*  nul = memchr(pLayer->name, '\0', sizeof(pLayer->name));
    const size_t nameLen = nul == NULL ? sizeof(pLayer->name) : strlen(pLayer->name);
    if (nul == NULL || (!isLeaf && nameLen == 0) || (nameLen > 192)) return TSDB_CODE_INVALID_PARA;
    if (nameLen > 0) {
      for (int32_t j = 0; j < i; ++j) {
        const SStreamWindowLayerSpec* prior = taosArrayGet(pPlan->pLayers, j);
        if (strlen(prior->name) > 0 && asciiEqualIgnoreCase(prior->name, pLayer->name)) {
          return TSDB_CODE_INVALID_PARA;
        }
      }
    }
    switch (pLayer->triggerType) {
      case WINDOW_TYPE_INTERVAL: {
        const int64_t sliding =
            pLayer->trigger.sliding.sliding > 0 ? pLayer->trigger.sliding.sliding : pLayer->trigger.sliding.interval;
        if (!isLeaf && (pLayer->trigger.sliding.overlap ||
                        (pLayer->trigger.sliding.interval != 0 && pLayer->trigger.sliding.interval > sliding))) {
          return TSDB_CODE_INVALID_PARA;
        }
        break;
      }
      case WINDOW_TYPE_SESSION:
        break;
      case WINDOW_TYPE_STATE:
        hasStateCountEvent = true;
        if (!isLeaf && (pLayer->trigger.stateWin.extend != 1 || hasTrueFor(&pLayer->trigger.stateWin) ||
                        pLayer->trigger.stateWin.zeroth != NULL)) {
          return TSDB_CODE_INVALID_PARA;
        }
        break;
      case WINDOW_TYPE_COUNT:
        hasStateCountEvent = true;
        if (!isLeaf && pLayer->trigger.count.countVal > pLayer->trigger.count.sliding) return TSDB_CODE_INVALID_PARA;
        break;
      case WINDOW_TYPE_EVENT:
        hasStateCountEvent = true;
        if (pLayer->trigger.event.startCond == NULL) return TSDB_CODE_INVALID_PARA;
        if (!isLeaf && hasEventTrueFor(&pLayer->trigger.event)) return TSDB_CODE_INVALID_PARA;
        bool          multipleStart = false;
        const int32_t eventCode = eventStartConditionKind((const char*)pLayer->trigger.event.startCond, &multipleStart);
        if (eventCode != TSDB_CODE_SUCCESS || (!isLeaf && multipleStart)) {
          return TSDB_CODE_INVALID_PARA;
        }
        break;
      default:
        return TSDB_CODE_INVALID_PARA;
    }
    if (pCtx->eventTypes & (BIT_FLAG_MASK(2) | BIT_FLAG_MASK(3)) && pLayer->placeholderMask != 0) {
      return TSDB_CODE_INVALID_PARA;
    }
  }
  if (pCtx->isExtTrigger || pCtx->hasCompositePrimaryKey) return TSDB_CODE_INVALID_PARA;
  if (pCtx->isSuperTable && hasStateCountEvent && !pCtx->partitionByTbname) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (pCtx->hasRollup && !allTimeOrSession(pPlan)) return TSDB_CODE_INVALID_PARA;
  if (pCtx->deleteRecalc) {
    for (int32_t i = 0; i < num; ++i) {
      const SStreamWindowLayerSpec* pLayer = taosArrayGet(pPlan->pLayers, i);
      if (pLayer->triggerType == WINDOW_TYPE_COUNT && pLayer->trigger.count.sliding != 1) return TSDB_CODE_INVALID_PARA;
    }
  }
  const SStreamWindowLayerSpec* pLeaf = taosArrayGet(pPlan->pLayers, num - 1);
  if (pCtx->ignoreNoDataTrigger && pLeaf->triggerType != WINDOW_TYPE_INTERVAL) return TSDB_CODE_INVALID_PARA;
  return TSDB_CODE_SUCCESS;
}

static bool sameStateTrigger(const SStateWinTrigger* left, const SStateWinTrigger* right) {
  return left->extend == right->extend && left->trueForType == right->trueForType &&
         left->trueForCount == right->trueForCount && left->trueForDuration == right->trueForDuration &&
         sameI16Array(left->pSlotIds, right->pSlotIds) && sameString(left->zeroth, right->zeroth) &&
         sameString(left->expr, right->expr);
}

static bool sameEventTrigger(const SEventTrigger* left, const SEventTrigger* right) {
  return left->trueForType == right->trueForType && left->trueForCount == right->trueForCount &&
         left->trueForDuration == right->trueForDuration && left->startTrueForType == right->startTrueForType &&
         left->startTrueForCount == right->startTrueForCount &&
         left->startTrueForDuration == right->startTrueForDuration && left->endTrueForType == right->endTrueForType &&
         left->endTrueForCount == right->endTrueForCount && left->endTrueForDuration == right->endTrueForDuration &&
         sameString(left->startCond, right->startCond) && sameString(left->endCond, right->endCond);
}

static bool sameSlidingTrigger(const SSlidingTrigger* left, const SSlidingTrigger* right) {
  return left->intervalUnit == right->intervalUnit && left->slidingUnit == right->slidingUnit &&
         left->offsetUnit == right->offsetUnit && left->soffsetUnit == right->soffsetUnit &&
         left->precision == right->precision && left->interval == right->interval && left->offset == right->offset &&
         left->sliding == right->sliding && left->soffset == right->soffset;
}

int32_t tValidateStreamWindowPlanLeafProjection(const SStreamWindowPlan* pPlan, int8_t leafWindowType,
                                                const SStreamTrigger* pLeafTrigger) {
  if (pPlan == NULL || pPlan->pLayers == NULL || pPlan->pLayers->elemSize != sizeof(SStreamWindowLayerSpec) ||
      pLeafTrigger == NULL || !isKnownWindowType(leafWindowType)) {
    return TSDB_CODE_INVALID_PARA;
  }
  const int32_t num = taosArrayGetSize(pPlan->pLayers);
  if (num <= 0) return TSDB_CODE_INVALID_PARA;
  const SStreamWindowLayerSpec* pLeaf = taosArrayGet(pPlan->pLayers, num - 1);
  if (pLeaf->triggerType != leafWindowType) return TSDB_CODE_INVALID_PARA;
  bool equal = false;
  switch (leafWindowType) {
    case WINDOW_TYPE_INTERVAL:
      equal = sameSlidingTrigger(&pLeaf->trigger.sliding, &pLeafTrigger->sliding);
      break;
    case WINDOW_TYPE_SESSION:
      equal = pLeaf->trigger.session.slotId == pLeafTrigger->session.slotId &&
              pLeaf->trigger.session.sessionVal == pLeafTrigger->session.sessionVal;
      break;
    case WINDOW_TYPE_STATE:
      equal = sameStateTrigger(&pLeaf->trigger.stateWin, &pLeafTrigger->stateWin);
      break;
    case WINDOW_TYPE_EVENT:
      equal = sameEventTrigger(&pLeaf->trigger.event, &pLeafTrigger->event);
      break;
    case WINDOW_TYPE_COUNT:
      equal = pLeaf->trigger.count.countVal == pLeafTrigger->count.countVal &&
              pLeaf->trigger.count.sliding == pLeafTrigger->count.sliding &&
              sameString(pLeaf->trigger.count.condCols, pLeafTrigger->count.condCols);
      break;
    default:
      break;
  }
  return equal ? TSDB_CODE_SUCCESS : TSDB_CODE_INVALID_PARA;
}

static int32_t encodeString(SEncoder* pEncoder, const char* pString) {
  const int32_t len = pString == NULL ? 0 : (int32_t)strlen(pString) + 1;
  if (len < 0) return TSDB_CODE_INVALID_PARA;
  int32_t code = tEncodeI32(pEncoder, len);
  if (code == TSDB_CODE_SUCCESS && len > 0) code = tEncodeFixed(pEncoder, pString, len);
  return code;
}

static int32_t decodeString(SDecoder* pDecoder, char** ppString) {
  int32_t len = 0;
  int32_t code = tDecodeI32(pDecoder, &len);
  if (code != TSDB_CODE_SUCCESS || len < 0 || (uint32_t)len > pDecoder->size - pDecoder->pos) {
    return code == TSDB_CODE_SUCCESS ? TSDB_CODE_OUT_OF_RANGE : code;
  }
  *ppString = NULL;
  if (len == 0) return TSDB_CODE_SUCCESS;
  if (len < 1 || ((uint8_t*)pDecoder->data)[pDecoder->pos + len - 1] != '\0') return TSDB_CODE_INVALID_PARA;
  *ppString = taosMemoryMalloc(len);
  if (*ppString == NULL) return terrno;
  code = tDecodeFixed(pDecoder, *ppString, len);
  if (code != TSDB_CODE_SUCCESS) taosMemoryFreeClear(*ppString);
  return code;
}

static int32_t encodeTrigger(SEncoder* pEncoder, int8_t type, const SStreamTrigger* pTrigger) {
  int32_t code = TSDB_CODE_SUCCESS;
  switch (type) {
    case WINDOW_TYPE_SESSION:
      code = tEncodeI16(pEncoder, pTrigger->session.slotId);
      if (code == 0) code = tEncodeI64(pEncoder, pTrigger->session.sessionVal);
      break;
    case WINDOW_TYPE_INTERVAL:
      code = tEncodeI8(pEncoder, pTrigger->sliding.intervalUnit);
      if (code == 0) code = tEncodeI8(pEncoder, pTrigger->sliding.slidingUnit);
      if (code == 0) code = tEncodeI8(pEncoder, pTrigger->sliding.offsetUnit);
      if (code == 0) code = tEncodeI8(pEncoder, pTrigger->sliding.soffsetUnit);
      if (code == 0) code = tEncodeI8(pEncoder, pTrigger->sliding.precision);
      if (code == 0) code = tEncodeI64(pEncoder, pTrigger->sliding.interval);
      if (code == 0) code = tEncodeI64(pEncoder, pTrigger->sliding.offset);
      if (code == 0) code = tEncodeI64(pEncoder, pTrigger->sliding.sliding);
      if (code == 0) code = tEncodeI64(pEncoder, pTrigger->sliding.soffset);
      if (code == 0) code = tEncodeI8(pEncoder, pTrigger->sliding.overlap);
      break;
    case WINDOW_TYPE_STATE: {
      const int32_t num = pTrigger->stateWin.pSlotIds == NULL ? 0 : taosArrayGetSize(pTrigger->stateWin.pSlotIds);
      code = tEncodeI32(pEncoder, num);
      for (int32_t i = 0; code == 0 && i < num; ++i)
        code = tEncodeI16(pEncoder, *(int16_t*)taosArrayGet(pTrigger->stateWin.pSlotIds, i));
      if (code == 0) code = tEncodeI16(pEncoder, pTrigger->stateWin.extend);
      if (code == 0) code = tEncodeI32(pEncoder, pTrigger->stateWin.trueForType);
      if (code == 0) code = tEncodeI32(pEncoder, pTrigger->stateWin.trueForCount);
      if (code == 0) code = tEncodeI64(pEncoder, pTrigger->stateWin.trueForDuration);
      if (code == 0) code = encodeString(pEncoder, pTrigger->stateWin.zeroth);
      if (code == 0) code = encodeString(pEncoder, pTrigger->stateWin.expr);
      break;
    }
    case WINDOW_TYPE_EVENT:
      code = encodeString(pEncoder, pTrigger->event.startCond);
      if (code == 0) code = encodeString(pEncoder, pTrigger->event.endCond);
      if (code == 0) code = tEncodeI32(pEncoder, pTrigger->event.trueForType);
      if (code == 0) code = tEncodeI32(pEncoder, pTrigger->event.trueForCount);
      if (code == 0) code = tEncodeI64(pEncoder, pTrigger->event.trueForDuration);
      if (code == 0) code = tEncodeI32(pEncoder, pTrigger->event.startTrueForType);
      if (code == 0) code = tEncodeI32(pEncoder, pTrigger->event.startTrueForCount);
      if (code == 0) code = tEncodeI64(pEncoder, pTrigger->event.startTrueForDuration);
      if (code == 0) code = tEncodeI32(pEncoder, pTrigger->event.endTrueForType);
      if (code == 0) code = tEncodeI32(pEncoder, pTrigger->event.endTrueForCount);
      if (code == 0) code = tEncodeI64(pEncoder, pTrigger->event.endTrueForDuration);
      break;
    case WINDOW_TYPE_COUNT:
      code = encodeString(pEncoder, pTrigger->count.condCols);
      if (code == 0) code = tEncodeI64(pEncoder, pTrigger->count.countVal);
      if (code == 0) code = tEncodeI64(pEncoder, pTrigger->count.sliding);
      break;
    case WINDOW_TYPE_PERIOD:
      code = tEncodeI8(pEncoder, pTrigger->period.periodUnit);
      if (code == 0) code = tEncodeI8(pEncoder, pTrigger->period.offsetUnit);
      if (code == 0) code = tEncodeI8(pEncoder, pTrigger->period.precision);
      if (code == 0) code = tEncodeI64(pEncoder, pTrigger->period.period);
      if (code == 0) code = tEncodeI64(pEncoder, pTrigger->period.offset);
      break;
    default:
      code = TSDB_CODE_INVALID_PARA;
      break;
  }
  return code;
}

int32_t tEncodeStreamWindowPlan(SEncoder* pEncoder, const SStreamWindowPlan* pPlan) {
  if (pEncoder == NULL || pPlan == NULL) return TSDB_CODE_INVALID_PARA;
  if (pPlan->pLayers != NULL && pPlan->pLayers->elemSize != sizeof(SStreamWindowLayerSpec)) {
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t       code = tEncodeI32(pEncoder, pPlan->version);
  const int32_t num = pPlan->pLayers == NULL ? 0 : taosArrayGetSize(pPlan->pLayers);
  if (code == 0) code = tEncodeI32(pEncoder, num);
  for (int32_t i = 0; code == 0 && i < num; ++i) {
    const SStreamWindowLayerSpec* pLayer = taosArrayGet(pPlan->pLayers, i);
    if ((pLayer->input.pConditionSlotIds != NULL && pLayer->input.pConditionSlotIds->elemSize != sizeof(int16_t)) ||
        (pLayer->triggerType == WINDOW_TYPE_STATE && pLayer->trigger.stateWin.pSlotIds != NULL &&
         pLayer->trigger.stateWin.pSlotIds->elemSize != sizeof(int16_t))) {
      return TSDB_CODE_INVALID_PARA;
    }
    code = tEncodeI8(pEncoder, pLayer->triggerType);
    if (code == 0) code = tEncodeFixed(pEncoder, pLayer->name, sizeof(pLayer->name));
    if (code == 0) code = tEncodeI64(pEncoder, pLayer->placeholderMask);
    if (code == 0) code = tEncodeI16(pEncoder, pLayer->input.tsSlotId);
    if (code == 0) code = tEncodeI16(pEncoder, pLayer->input.pkSlotId);
    if (code == 0) code = tEncodeI16(pEncoder, pLayer->input.eventStartSlotId);
    if (code == 0) code = tEncodeI16(pEncoder, pLayer->input.eventEndSlotId);
    const int32_t conditionNum =
        pLayer->input.pConditionSlotIds == NULL ? 0 : taosArrayGetSize(pLayer->input.pConditionSlotIds);
    if (code == 0) code = tEncodeI32(pEncoder, conditionNum);
    for (int32_t j = 0; code == 0 && j < conditionNum; ++j)
      code = tEncodeI16(pEncoder, *(int16_t*)taosArrayGet(pLayer->input.pConditionSlotIds, j));
    if (code == 0) code = encodeTrigger(pEncoder, pLayer->triggerType, &pLayer->trigger);
  }
  return code;
}

static int32_t decodeTrigger(SDecoder* pDecoder, int8_t type, SStreamTrigger* pTrigger) {
  int32_t code = TSDB_CODE_SUCCESS;
  memset(pTrigger, 0, sizeof(*pTrigger));
  switch (type) {
    case WINDOW_TYPE_SESSION:
      code = tDecodeI16(pDecoder, &pTrigger->session.slotId);
      if (code == 0) code = tDecodeI64(pDecoder, &pTrigger->session.sessionVal);
      break;
    case WINDOW_TYPE_INTERVAL:
      code = tDecodeI8(pDecoder, &pTrigger->sliding.intervalUnit);
      if (code == 0) code = tDecodeI8(pDecoder, &pTrigger->sliding.slidingUnit);
      if (code == 0) code = tDecodeI8(pDecoder, &pTrigger->sliding.offsetUnit);
      if (code == 0) code = tDecodeI8(pDecoder, &pTrigger->sliding.soffsetUnit);
      if (code == 0) code = tDecodeI8(pDecoder, &pTrigger->sliding.precision);
      if (code == 0) code = tDecodeI64(pDecoder, &pTrigger->sliding.interval);
      if (code == 0) code = tDecodeI64(pDecoder, &pTrigger->sliding.offset);
      if (code == 0) code = tDecodeI64(pDecoder, &pTrigger->sliding.sliding);
      if (code == 0) code = tDecodeI64(pDecoder, &pTrigger->sliding.soffset);
      if (code == 0) code = tDecodeI8(pDecoder, &pTrigger->sliding.overlap);
      break;
    case WINDOW_TYPE_STATE: {
      int32_t num = 0;
      code = tDecodeI32(pDecoder, &num);
      if (code != 0 || num < 0 || (uint32_t)num > (pDecoder->size - pDecoder->pos) / sizeof(int16_t))
        return code == 0 ? TSDB_CODE_OUT_OF_RANGE : code;
      if (num > 0) {
        pTrigger->stateWin.pSlotIds = taosArrayInit(num, sizeof(int16_t));
        if (pTrigger->stateWin.pSlotIds == NULL) return terrno;
      }
      for (int32_t i = 0; i < num; ++i) {
        int16_t slot = 0;
        code = tDecodeI16(pDecoder, &slot);
        if (code != 0 || taosArrayPush(pTrigger->stateWin.pSlotIds, &slot) == NULL) return code == 0 ? terrno : code;
      }
      code = tDecodeI16(pDecoder, &pTrigger->stateWin.extend);
      if (code == 0) code = tDecodeI32(pDecoder, &pTrigger->stateWin.trueForType);
      if (code == 0) code = tDecodeI32(pDecoder, &pTrigger->stateWin.trueForCount);
      if (code == 0) code = tDecodeI64(pDecoder, &pTrigger->stateWin.trueForDuration);
      if (code == 0) code = decodeString(pDecoder, (char**)&pTrigger->stateWin.zeroth);
      if (code == 0) code = decodeString(pDecoder, (char**)&pTrigger->stateWin.expr);
      break;
    }
    case WINDOW_TYPE_EVENT:
      code = decodeString(pDecoder, (char**)&pTrigger->event.startCond);
      if (code == 0) code = decodeString(pDecoder, (char**)&pTrigger->event.endCond);
      if (code == 0) code = tDecodeI32(pDecoder, &pTrigger->event.trueForType);
      if (code == 0) code = tDecodeI32(pDecoder, &pTrigger->event.trueForCount);
      if (code == 0) code = tDecodeI64(pDecoder, &pTrigger->event.trueForDuration);
      if (code == 0) code = tDecodeI32(pDecoder, &pTrigger->event.startTrueForType);
      if (code == 0) code = tDecodeI32(pDecoder, &pTrigger->event.startTrueForCount);
      if (code == 0) code = tDecodeI64(pDecoder, &pTrigger->event.startTrueForDuration);
      if (code == 0) code = tDecodeI32(pDecoder, &pTrigger->event.endTrueForType);
      if (code == 0) code = tDecodeI32(pDecoder, &pTrigger->event.endTrueForCount);
      if (code == 0) code = tDecodeI64(pDecoder, &pTrigger->event.endTrueForDuration);
      break;
    case WINDOW_TYPE_COUNT:
      code = decodeString(pDecoder, (char**)&pTrigger->count.condCols);
      if (code == 0) code = tDecodeI64(pDecoder, &pTrigger->count.countVal);
      if (code == 0) code = tDecodeI64(pDecoder, &pTrigger->count.sliding);
      break;
    case WINDOW_TYPE_PERIOD:
      code = tDecodeI8(pDecoder, &pTrigger->period.periodUnit);
      if (code == 0) code = tDecodeI8(pDecoder, &pTrigger->period.offsetUnit);
      if (code == 0) code = tDecodeI8(pDecoder, &pTrigger->period.precision);
      if (code == 0) code = tDecodeI64(pDecoder, &pTrigger->period.period);
      if (code == 0) code = tDecodeI64(pDecoder, &pTrigger->period.offset);
      break;
    default:
      code = TSDB_CODE_INVALID_PARA;
      break;
  }
  return code;
}

static int32_t preflightSkipBytes(SDecoder* pDecoder, uint32_t bytes) {
  if (pDecoder->pos > pDecoder->size || bytes > pDecoder->size - pDecoder->pos) {
    return TSDB_CODE_OUT_OF_RANGE;
  }
  pDecoder->pos += bytes;
  return TSDB_CODE_SUCCESS;
}

static int32_t preflightDecodeI8(SDecoder* pDecoder, int8_t* pValue) {
  if (pDecoder->pos > pDecoder->size || sizeof(*pValue) > pDecoder->size - pDecoder->pos) {
    return TSDB_CODE_OUT_OF_RANGE;
  }
  return tDecodeI8(pDecoder, pValue);
}

static int32_t preflightDecodeI32(SDecoder* pDecoder, int32_t* pValue) {
  if (pDecoder->pos > pDecoder->size || sizeof(*pValue) > pDecoder->size - pDecoder->pos) {
    return TSDB_CODE_OUT_OF_RANGE;
  }
  return tDecodeI32(pDecoder, pValue);
}

static int32_t preflightString(SDecoder* pDecoder) {
  int32_t len = 0;
  int32_t code = preflightDecodeI32(pDecoder, &len);
  if (code != TSDB_CODE_SUCCESS) return code;
  if (len < 0) return TSDB_CODE_INVALID_PARA;
  if (len == 0) return TSDB_CODE_SUCCESS;
  if (pDecoder->pos > pDecoder->size || (uint32_t)len > pDecoder->size - pDecoder->pos) {
    return TSDB_CODE_OUT_OF_RANGE;
  }
  if (pDecoder->data[pDecoder->pos + (uint32_t)len - 1] != '\0') return TSDB_CODE_INVALID_PARA;
  return preflightSkipBytes(pDecoder, (uint32_t)len);
}

static int32_t preflightI16Array(SDecoder* pDecoder) {
  int32_t num = 0;
  int32_t code = preflightDecodeI32(pDecoder, &num);
  if (code != TSDB_CODE_SUCCESS) return code;
  if (num < 0) return TSDB_CODE_INVALID_PARA;
  if (pDecoder->pos > pDecoder->size || (uint32_t)num > (pDecoder->size - pDecoder->pos) / sizeof(int16_t)) {
    return TSDB_CODE_OUT_OF_RANGE;
  }
  return preflightSkipBytes(pDecoder, (uint32_t)num * sizeof(int16_t));
}

static int32_t preflightTrigger(SDecoder* pDecoder, int8_t type) {
  int32_t code = TSDB_CODE_SUCCESS;
  switch (type) {
    case WINDOW_TYPE_SESSION:
      return preflightSkipBytes(pDecoder, sizeof(int16_t) + sizeof(int64_t));
    case WINDOW_TYPE_INTERVAL:
      return preflightSkipBytes(pDecoder, sizeof(int8_t) * 6 + sizeof(int64_t) * 4);
    case WINDOW_TYPE_STATE:
      code = preflightI16Array(pDecoder);
      if (code == TSDB_CODE_SUCCESS) {
        code = preflightSkipBytes(pDecoder, sizeof(int16_t) + sizeof(int32_t) * 2 + sizeof(int64_t));
      }
      if (code == TSDB_CODE_SUCCESS) code = preflightString(pDecoder);
      if (code == TSDB_CODE_SUCCESS) code = preflightString(pDecoder);
      return code;
    case WINDOW_TYPE_EVENT:
      code = preflightString(pDecoder);
      if (code == TSDB_CODE_SUCCESS) code = preflightString(pDecoder);
      if (code == TSDB_CODE_SUCCESS) {
        code = preflightSkipBytes(pDecoder, (sizeof(int32_t) * 2 + sizeof(int64_t)) * 3);
      }
      return code;
    case WINDOW_TYPE_COUNT:
      code = preflightString(pDecoder);
      if (code == TSDB_CODE_SUCCESS) code = preflightSkipBytes(pDecoder, sizeof(int64_t) * 2);
      return code;
    case WINDOW_TYPE_PERIOD:
      return preflightSkipBytes(pDecoder, sizeof(int8_t) * 3 + sizeof(int64_t) * 2);
    default:
      return TSDB_CODE_INVALID_PARA;
  }
}

static int32_t preflightStreamWindowPlan(const SDecoder* pSource) {
  if (pSource->pos > pSource->size || (pSource->data == NULL && pSource->pos < pSource->size)) {
    return TSDB_CODE_INVALID_PARA;
  }
  SDecoder decoder = *pSource;
  int32_t  version = 0;
  int32_t  num = 0;
  int32_t  code = preflightDecodeI32(&decoder, &version);
  if (code == TSDB_CODE_SUCCESS) code = preflightDecodeI32(&decoder, &num);
  if (code != TSDB_CODE_SUCCESS) return code;
  if (version != STREAM_WINDOW_PLAN_VERSION || num < 2 || num > STREAM_WINDOW_MAX_LAYERS) {
    return TSDB_CODE_INVALID_PARA;
  }

  for (int32_t i = 0; i < num; ++i) {
    int8_t type = 0;
    code = preflightDecodeI8(&decoder, &type);
    if (code != TSDB_CODE_SUCCESS) return code;
    if (decoder.pos > decoder.size || sizeof(((SStreamWindowLayerSpec*)0)->name) > decoder.size - decoder.pos) {
      return TSDB_CODE_OUT_OF_RANGE;
    }
    if (memchr(decoder.data + decoder.pos, '\0', sizeof(((SStreamWindowLayerSpec*)0)->name)) == NULL) {
      return TSDB_CODE_INVALID_PARA;
    }
    code = preflightSkipBytes(&decoder, sizeof(((SStreamWindowLayerSpec*)0)->name));
    if (code == TSDB_CODE_SUCCESS) {
      code = preflightSkipBytes(&decoder, sizeof(int64_t) + sizeof(int16_t) * 4);
    }
    if (code == TSDB_CODE_SUCCESS) code = preflightI16Array(&decoder);
    if (code == TSDB_CODE_SUCCESS) code = preflightTrigger(&decoder, type);
    if (code != TSDB_CODE_SUCCESS) return code;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t tDecodeStreamWindowPlan(SDecoder* pDecoder, SStreamWindowPlan** ppPlan) {
  if (pDecoder == NULL || ppPlan == NULL) return TSDB_CODE_INVALID_PARA;
  *ppPlan = NULL;
  int32_t code = preflightStreamWindowPlan(pDecoder);
  if (code != TSDB_CODE_SUCCESS) return code;
  int32_t version = 0;
  int32_t num = 0;
  code = tDecodeI32(pDecoder, &version);
  if (code == 0) code = tDecodeI32(pDecoder, &num);
  if (code != 0) return code;
  if (version != STREAM_WINDOW_PLAN_VERSION || num < 0 || num > STREAM_WINDOW_MAX_LAYERS) return TSDB_CODE_INVALID_PARA;
  if (num < 2) return TSDB_CODE_INVALID_PARA;
  SStreamWindowPlan* pPlan = taosMemoryCalloc(1, sizeof(*pPlan));
  if (pPlan == NULL) return terrno;
  pPlan->version = version;
  pPlan->pLayers = taosArrayInit(num, sizeof(SStreamWindowLayerSpec));
  if (pPlan->pLayers == NULL) {
    code = terrno;
    goto _exit;
  }
  for (int32_t i = 0; i < num; ++i) {
    SStreamWindowLayerSpec layer = {};
    code = tDecodeI8(pDecoder, &layer.triggerType);
    if (code == 0) code = tDecodeFixed(pDecoder, layer.name, sizeof(layer.name));
    if (code == 0 && memchr(layer.name, '\0', sizeof(layer.name)) == NULL) code = TSDB_CODE_INVALID_PARA;
    if (code == 0) code = tDecodeI64(pDecoder, &layer.placeholderMask);
    if (code == 0) code = tDecodeI16(pDecoder, &layer.input.tsSlotId);
    if (code == 0) code = tDecodeI16(pDecoder, &layer.input.pkSlotId);
    if (code == 0) code = tDecodeI16(pDecoder, &layer.input.eventStartSlotId);
    if (code == 0) code = tDecodeI16(pDecoder, &layer.input.eventEndSlotId);
    int32_t conditionNum = 0;
    if (code == 0) code = tDecodeI32(pDecoder, &conditionNum);
    if (code == 0 &&
        (conditionNum < 0 || (uint32_t)conditionNum > (pDecoder->size - pDecoder->pos) / sizeof(int16_t))) {
      code = TSDB_CODE_OUT_OF_RANGE;
    }
    if (code == 0 && conditionNum > 0) {
      layer.input.pConditionSlotIds = taosArrayInit(conditionNum, sizeof(int16_t));
      if (layer.input.pConditionSlotIds == NULL) code = terrno;
    }
    for (int32_t j = 0; code == 0 && j < conditionNum; ++j) {
      int16_t slot = 0;
      code = tDecodeI16(pDecoder, &slot);
      if (code == 0 && taosArrayPush(layer.input.pConditionSlotIds, &slot) == NULL) code = terrno;
    }
    if (code == 0) code = decodeTrigger(pDecoder, layer.triggerType, &layer.trigger);
    if (code == 0 && taosArrayPush(pPlan->pLayers, &layer) == NULL) code = terrno;
    if (code != 0) destroyWindowLayer(&layer);
    if (code != 0) goto _exit;
  }
  *ppPlan = pPlan;
  return TSDB_CODE_SUCCESS;

_exit:
  tDestroyStreamWindowPlan(&pPlan);
  return code;
}
