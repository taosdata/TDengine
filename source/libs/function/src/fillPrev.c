/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#include "fillPrev.h"

#include "tdatablock.h"
#include "ttime.h"

static int32_t fixedRangeTicks(int64_t amount, int8_t unit, int8_t precision, uint64_t* ticks) {
  int64_t perUnit;
  TAOS_CHECK_RETURN(getDuration(1, unit, &perUnit, precision));
  if (perUnit == 0) {
    // The unit is finer than a tick. Divide the whole amount before truncating.
    int64_t truncated;
    TAOS_CHECK_RETURN(getDuration(amount, unit, &truncated, precision));
    *ticks = truncated;
  } else {
    *ticks = (uint64_t)amount > UINT64_MAX / perUnit ? UINT64_MAX : (uint64_t)amount * perUnit;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t fillPrevRangeInit(SFillPrevRange* pRange, bool hasSpan, int64_t amount, int8_t unit, int8_t precision,
                          const char* tzName) {
  timezone_t tz = NULL;
  TAOS_CHECK_RETURN(fillPrevRangeInitShared(pRange, hasSpan, amount, unit, precision, tzName, &tz));
  pRange->ownsTz = (tz != NULL);
  return TSDB_CODE_SUCCESS;
}

int32_t fillPrevRangeInitShared(SFillPrevRange* pRange, bool hasSpan, int64_t amount, int8_t unit, int8_t precision,
                                const char* tzName, timezone_t* tz) {
  SFillPrevRange range = {.hasSpan = hasSpan, .unit = unit, .precision = precision, .amount = amount};
  if (hasSpan) {
    if (amount < 0) return TSDB_CODE_OUT_OF_RANGE;
    int32_t code = TSDB_CODE_SUCCESS;
    if (IS_CALENDAR_TIME_DURATION(unit)) {
      if (*tz == NULL && tzName != NULL && tzName[0] != '\0') {
        code = taosValidateTimezone(tzName, tz);
      }
      range.tz = *tz;
      if (amount == 0) {
        range.unit = TIME_UNIT_SECOND;
      } else {
        // Month-end clamping cannot lengthen civil time beyond 31 days/month or 366 days/year.
        // Two extra days cover the difference between IANA UTC offsets, including date-line jumps.
        uint64_t day = (uint64_t)86400 * TSDB_TICK_PER_SECOND(precision);
        uint64_t perUnit = (unit == 'y' || unit == 'Y' ? 366 : 31) * day;
        range.ticks = (uint64_t)amount > (UINT64_MAX - 2 * day) / perUnit ? UINT64_MAX : amount * perUnit + 2 * day;
      }
    } else {
      code = fixedRangeTicks(amount, unit, precision, &range.ticks);
    }
    if (code != TSDB_CODE_SUCCESS) return code;
  }
  fillPrevRangeDestroy(pRange);
  *pRange = range;
  return TSDB_CODE_SUCCESS;
}

int32_t fillPrevCalendarEnd(const SFillPrevRange* pRange, int64_t earlier, int64_t* end) {
  int32_t code = taosTimeAddCalendarMonths(earlier, pRange->amount, pRange->unit, pRange->precision, pRange->tz, end);
  if (code == TSDB_CODE_OUT_OF_RANGE) {
    *end = INT64_MAX;
    return TSDB_CODE_SUCCESS;
  }
  return code;
}

int32_t fillPrevRangeContains(const SFillPrevRange* pRange, int64_t earlier, int64_t later, bool* contains) {
  if (earlier >= later || !pRange->hasSpan) {
    *contains = earlier <= later;
  } else if (IS_CALENDAR_TIME_DURATION(pRange->unit)) {
    int64_t end;
    TAOS_CHECK_RETURN(fillPrevCalendarEnd(pRange, earlier, &end));
    *contains = later <= end;
  } else {
    *contains = (uint64_t)later - (uint64_t)earlier <= pRange->ticks;
  }
  return TSDB_CODE_SUCCESS;
}

void fillPrevRangeDestroy(SFillPrevRange* pRange) {
  if (pRange->ownsTz) tzfree(pRange->tz);
  memset(pRange, 0, sizeof(*pRange));
}

int32_t fillPrevValueSet(SFillPrevValue* pValue, const SDataType* pType, const void* pData, int64_t sourceTs) {
  if (pData == NULL) return TSDB_CODE_SUCCESS;
  if (IS_MATHABLE_TYPE(pType->type)) {
    if (pType->type == TSDB_DATA_TYPE_BOOL) {
      *(char*)pValue->fixed = *(const char*)pData != 0;
    } else {
      memcpy(pValue->fixed, pData, tDataTypes[pType->type].bytes);
    }
  } else if (IS_VAR_DATA_TYPE(pType->type) && pType->type != TSDB_DATA_TYPE_JSON) {
    int64_t total;
    if (IS_STR_DATA_BLOB(pType->type)) {
      BlobDataLenT length;
      memcpy(&length, pData, sizeof(length));
      total = (int64_t)length + sizeof(length);
    } else {
      VarDataLenT length;
      memcpy(&length, pData, sizeof(length));
      total = (int64_t)length + sizeof(length);
    }
    if (total > pType->bytes) return TSDB_CODE_QRY_NOT_ENOUGH_BUFFER;
    int32_t bytes = (int32_t)total;
    if (bytes > pValue->capacity) {
      int32_t capacity = (int32_t)TMIN(pType->bytes, TMAX(bytes, (int64_t)pValue->capacity * 2));
      char*   buffer = taosMemoryRealloc(pValue->data, capacity);
      if (buffer == NULL) return terrno;
      pValue->data = buffer;
      pValue->capacity = capacity;
    }
    memcpy(pValue->data, pData, bytes);
  } else {
    return TSDB_CODE_FUNC_FUNTION_PARA_TYPE;
  }
  pValue->type = *pType;
  pValue->nonnull = true;
  pValue->sourceTs = sourceTs;
  return TSDB_CODE_SUCCESS;
}

int32_t fillPrevValueWrite(const SFillPrevValue* pValue, bool valid, SColumnInfoData* pOut, int32_t row) {
  if (!valid || !pValue->nonnull) {
    colDataSetNULL(pOut, row);
    return TSDB_CODE_SUCCESS;
  }
  const char* data = IS_VAR_DATA_TYPE(pValue->type.type) ? pValue->data : (const char*)pValue->fixed;
  if (IS_FLOAT_TYPE(pValue->type.type) && pOut->nullbitmap == NULL) {
    memcpy(colDataGetNumData(pOut, row), data, tDataTypes[pValue->type.type].bytes);
    return TSDB_CODE_SUCCESS;
  }
  return colDataSetVal(pOut, row, data, false);
}

void fillPrevValueReset(SFillPrevValue* pValue) {
  taosMemoryFree(pValue->data);
  memset(pValue, 0, sizeof(*pValue));
}
