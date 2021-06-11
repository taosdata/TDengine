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

#ifndef TDENGINE_QFILTER_H
#define TDENGINE_QFILTER_H

#ifdef __cplusplus
extern "C" {
#endif

#include "texpr.h"

enum {
  F_FIELD_COLUMN = 0,
  F_FIELD_VALUE,
  F_FIELD_MAX
};

typedef struct OptrStr {
  uint16_t optr;
  char    *str;
} OptrStr;


typedef struct SFilterField {
  uint16_t type;
  void*    desc;
  void*    data;
} SFilterField;

typedef struct SFilterFields {
  uint16_t num;
  SFilterField *fields;
} SFilterFields;

typedef struct SFilterGroup {
  uint16_t  unitNum;
  uint16_t *unitIdxs;
  uint8_t  *unitFlags;  // !unit result
} SFilterGroup;

typedef struct SFilterCompare {
  __compar_fn_t pCompareFunc;
  uint8_t       optr;
} SFilterCompare;

typedef struct SFilterUnit {
  SFilterCompare compare;
  SFilterField  *left;
  SFilterField  *right;
} SFilterUnit;

typedef struct SFilterInfo {
  uint16_t      unitNum;
  uint16_t      groupNum;
  SFilterFields fileds[F_FIELD_MAX];
  SFilterGroup *groups;
  SFilterUnit  *units;
  uint8_t      *unitRes;    // result
  uint8_t      *unitFlags;  // got result
} SFilterInfo;

#define ERR_RET(c) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { return _code; } } while (0)
#define ERR_LRET(c,...) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { qError(__VA_ARGS__); return _code; } } while (0)
#define ERR_JRET(c) do { code = c; if (code != TSDB_CODE_SUCCESS) { goto _err_return; } } while (0)

#define CHK_RETV(c) do { if (c) { return; } } while (0)
#define CHK_RET(c, r) do { if (c) { return r; } } while (0)
#define CHK_LRETV(c,...) do { if (c) { qError(__VA_ARGS__); return; } } while (0)
#define CHK_LRET(c, r,...) do { if (c) { qError(__VA_ARGS__); return r; } } while (0)

typedef int32_t(*filter_desc_compare_func)(const void *, const void *);


extern int32_t filterInitFromTree(tExprNode* tree, SFilterInfo *info, int32_t colSize);


#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_QFILTER_H
