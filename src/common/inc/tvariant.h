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

#ifndef TDENGINE_TVARIANT_H
#define TDENGINE_TVARIANT_H

#include "tstoken.h"
#include "tarray.h"

#ifdef __cplusplus
extern "C" {
#endif

// variant, each number/string/field_id has a corresponding struct during parsing sql
typedef struct tVariant {
  uint32_t nType;
  int32_t  nLen;  // only used for string, for number, it is useless
  union {
    int64_t  i64;
    uint64_t u64;
    double   dKey;
    char *   pz;
    wchar_t *wpz;
    SArray  *arr; // only for 'in' query to hold value list, not value for a field
  };
} tVariant;

bool tVariantIsValid(tVariant *pVar);

void tVariantCreate(tVariant *pVar, SStrToken *token);

void tVariantCreateFromBinary(tVariant *pVar, const char *pz, size_t len, uint32_t type);

void tVariantDestroy(tVariant *pV);

void tVariantAssign(tVariant *pDst, const tVariant *pSrc);

int32_t tVariantCompare(const tVariant* p1, const tVariant* p2);

int32_t tVariantToString(tVariant *pVar, char *dst);

int32_t tVariantDump(tVariant *pVariant, char *payload, int16_t type, bool includeLengthPrefix);

int32_t tVariantTypeSetType(tVariant *pVariant, char type);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TVARIANT_H
