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

#ifndef TDENGINE_TTYPES_H
#define TDENGINE_TTYPES_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include "tstoken.h"

// Bytes for each type.
#define CHAR_BYTES   sizeof(char)
#define SHORT_BYTES  sizeof(short)
#define INT_BYTES    sizeof(int)
#define LONG_BYTES   sizeof(int64_t)
#define FLOAT_BYTES  sizeof(float)
#define DOUBLE_BYTES sizeof(double)

#define POINTER_BYTES sizeof(void *)  // 8 by default  assert(sizeof(ptrdiff_t) == sizseof(void*)

typedef struct tDataDescriptor {
  int16_t nType;
  int16_t nameLen;
  int32_t nSize;
  char *  aName;
} tDataDescriptor;

extern tDataDescriptor tDataTypeDesc[11];

bool isValidDataType(int32_t type, int32_t length);
bool isNull(const char *val, int32_t type);

void setNull(char *val, int32_t type, int32_t bytes);
void setNullN(char *val, int32_t type, int32_t bytes, int32_t numOfElems);

void assignVal(char *val, char *src, int32_t len, int32_t type);
void tsDataSwap(void *pLeft, void *pRight, int32_t type, int32_t size);

// variant, each number/string/field_id has a corresponding struct during
// parsing sql
typedef struct tVariant {
  uint32_t nType;
  int32_t  nLen;  // only used for string, for number, it is useless
  union {
    int64_t  i64Key;
    double   dKey;
    char *   pz;
    wchar_t *wpz;
  };
} tVariant;

void tVariantCreate(tVariant *pVar, SSQLToken *token);

void tVariantCreateN(tVariant *pVar, char *pz, uint32_t len, uint32_t type);

void tVariantCreateB(tVariant *pVar, char *pz, uint32_t len, uint32_t type);

void tVariantDestroy(tVariant *pV);

void tVariantAssign(tVariant *pDst, tVariant *pSrc);

int32_t tVariantToString(tVariant *pVar, char *dst);

int32_t tVariantDump(tVariant *pVariant, char *payload, char type);

int32_t tVariantTypeSetType(tVariant *pVariant, char type);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TTYPES_H
