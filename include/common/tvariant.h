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

#ifndef _TD_COMMON_VARIANT_H_
#define _TD_COMMON_VARIANT_H_

#include "tarray.h"

#ifdef __cplusplus
extern "C" {
#endif

// variant, each number/string/field_id has a corresponding struct during parsing sql
typedef struct SVariant {
  uint32_t nType;
  int32_t  nLen;  // only used for string, for number, it is useless
  union {
    int64_t  i;
    uint64_t u;
    double   d;
    char    *pz;
    TdUcs4  *ucs4;
    SArray  *arr;  // only for 'in' query to hold value list, not value for a field
  };
} SVariant;

int32_t toInteger(const char *z, int32_t n, int32_t base, int64_t *value, bool *issigned);

bool taosVariantIsValid(SVariant *pVar);

void taosVariantCreate(SVariant *pVar, const char *z, int32_t n, int32_t type);

void taosVariantCreateFromBinary(SVariant *pVar, const char *pz, size_t len, uint32_t type);

void taosVariantDestroy(SVariant *pV);

void taosVariantAssign(SVariant *pDst, const SVariant *pSrc);

int32_t taosVariantCompare(const SVariant *p1, const SVariant *p2);

int32_t taosVariantToString(SVariant *pVar, char *dst);

int32_t taosVariantDump(SVariant *pVariant, char *payload, int16_t type, bool includeLengthPrefix);

#if 0
int32_t taosVariantDumpEx(SVariant *pVariant, char *payload, int16_t type, bool includeLengthPrefix, bool *converted, char *extInfo);
#endif

int32_t taosVariantTypeSetType(SVariant *pVariant, char type);

#ifdef __cplusplus
}
#endif

#endif  /*_TD_COMMON_VARIANT_H_*/
