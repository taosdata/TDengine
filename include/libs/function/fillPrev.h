/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#ifndef TDENGINE_FILLPREV_H
#define TDENGINE_FILLPREV_H

#include "tcommon.h"
#include "ttypes.h"

#ifdef __cplusplus
extern "C" {
#endif

/** Zero-initialize each candidate. Reset releases its owned variable-length storage. */
typedef struct SFillPrevValue {
  SDataType type;
  bool      nonnull;
  int64_t   sourceTs;
  int64_t   fixed[2];
  char*     data;
  int32_t   capacity;
} SFillPrevValue;

/** Zero-initialize before first use. Only standalone ranges own their calendar timezone. */
typedef struct SFillPrevRange {
  bool       hasSpan;
  bool       ownsTz;
  int8_t     unit;
  int8_t     precision;
  int64_t    amount;
  uint64_t   ticks;  // Upper bound in ticks, exact for fixed spans and saturated at UINT64_MAX.
  timezone_t tz;
} SFillPrevRange;

int32_t fillPrevRangeInit(SFillPrevRange* pRange, bool hasSpan, int64_t amount, int8_t unit, int8_t precision,
                          const char* tzName);
/** Each initialized range borrows *tz; its caller owns the handle and uses the same name for all ranges. */
int32_t fillPrevRangeInitShared(SFillPrevRange* pRange, bool hasSpan, int64_t amount, int8_t unit, int8_t precision,
                                const char* tzName, timezone_t* tz);
/** Absolute inclusive deadline for a calendar range, saturated at the largest timestamp. */
int32_t fillPrevCalendarEnd(const SFillPrevRange* pRange, int64_t earlier, int64_t* end);
int32_t fillPrevRangeContains(const SFillPrevRange* pRange, int64_t earlier, int64_t later, bool* contains);
void    fillPrevRangeDestroy(SFillPrevRange* pRange);
int32_t fillPrevValueSet(SFillPrevValue* pValue, const SDataType* pType, const void* pData, int64_t sourceTs);
int32_t fillPrevValueWrite(const SFillPrevValue* pValue, bool valid, SColumnInfoData* pOut, int32_t row);
void    fillPrevValueReset(SFillPrevValue* pValue);

#ifdef __cplusplus
}
#endif
#endif
