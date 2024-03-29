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
#include <tlog.h>
#include "os.h"
#include "tglobal.h"
#include "thash.h"
// #include "queryLog.h"
#include "filter.h"
#include "filterInt.h"
#include "functionMgt.h"
#include "sclInt.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "tsimplehash.h"
#include "ttime.h"

bool filterRangeCompGi(const void *minv, const void *maxv, const void *minr, const void *maxr, __compar_fn_t cfunc) {
  int32_t result = cfunc(maxv, minr);
  return result >= 0;
}
bool filterRangeCompGe(const void *minv, const void *maxv, const void *minr, const void *maxr, __compar_fn_t cfunc) {
  int32_t result = cfunc(maxv, minr);
  return result > 0;
}
bool filterRangeCompLi(const void *minv, const void *maxv, const void *minr, const void *maxr, __compar_fn_t cfunc) {
  int32_t result = cfunc(minv, maxr);
  return result <= 0;
}
bool filterRangeCompLe(const void *minv, const void *maxv, const void *minr, const void *maxr, __compar_fn_t cfunc) {
  int32_t result = cfunc(minv, maxr);
  return result < 0;
}
bool filterRangeCompii(const void *minv, const void *maxv, const void *minr, const void *maxr, __compar_fn_t cfunc) {
  return cfunc(maxv, minr) >= 0 && cfunc(minv, maxr) <= 0;
}
bool filterRangeCompee(const void *minv, const void *maxv, const void *minr, const void *maxr, __compar_fn_t cfunc) {
  return cfunc(maxv, minr) > 0 && cfunc(minv, maxr) < 0;
}
bool filterRangeCompei(const void *minv, const void *maxv, const void *minr, const void *maxr, __compar_fn_t cfunc) {
  return cfunc(maxv, minr) > 0 && cfunc(minv, maxr) <= 0;
}
bool filterRangeCompie(const void *minv, const void *maxv, const void *minr, const void *maxr, __compar_fn_t cfunc) {
  return cfunc(maxv, minr) >= 0 && cfunc(minv, maxr) < 0;
}

rangeCompFunc filterGetRangeCompFunc(char sflag, char eflag) {
  if (FILTER_GET_FLAG(sflag, RANGE_FLG_NULL)) {
    if (FILTER_GET_FLAG(eflag, RANGE_FLG_EXCLUDE)) {
      return filterRangeCompLe;
    }

    return filterRangeCompLi;
  }

  if (FILTER_GET_FLAG(eflag, RANGE_FLG_NULL)) {
    if (FILTER_GET_FLAG(sflag, RANGE_FLG_EXCLUDE)) {
      return filterRangeCompGe;
    }

    return filterRangeCompGi;
  }

  if (FILTER_GET_FLAG(sflag, RANGE_FLG_EXCLUDE)) {
    if (FILTER_GET_FLAG(eflag, RANGE_FLG_EXCLUDE)) {
      return filterRangeCompee;
    }

    return filterRangeCompei;
  }

  if (FILTER_GET_FLAG(eflag, RANGE_FLG_EXCLUDE)) {
    return filterRangeCompie;
  }

  return filterRangeCompii;
}

rangeCompFunc gRangeCompare[] = {filterRangeCompee, filterRangeCompei, filterRangeCompie, filterRangeCompii,
                                 filterRangeCompGe, filterRangeCompGi, filterRangeCompLe, filterRangeCompLi};

int8_t filterGetRangeCompFuncFromOptrs(uint8_t optr, uint8_t optr2) {
  if (optr2) {
    ASSERT(optr2 == OP_TYPE_LOWER_THAN || optr2 == OP_TYPE_LOWER_EQUAL);

    if (optr == OP_TYPE_GREATER_THAN) {
      if (optr2 == OP_TYPE_LOWER_THAN) {
        return 0;
      }

      return 1;
    }

    if (optr2 == OP_TYPE_LOWER_THAN) {
      return 2;
    }

    return 3;
  } else {
    switch (optr) {
      case OP_TYPE_GREATER_THAN:
        return 4;
      case OP_TYPE_GREATER_EQUAL:
        return 5;
      case OP_TYPE_LOWER_THAN:
        return 6;
      case OP_TYPE_LOWER_EQUAL:
        return 7;
      default:
        break;
    }
  }

  return -1;
}

__compar_fn_t gDataCompare[] = {
    compareInt32Val,       compareInt8Val,         compareInt16Val,         compareInt64Val,
    compareFloatVal,       compareDoubleVal,       compareLenPrefixedStr,   comparestrPatternMatch,
    compareChkInString,    comparewcsPatternMatch, compareLenPrefixedWStr,  compareUint8Val,
    compareUint16Val,      compareUint32Val,       compareUint64Val,        setChkInBytes1,
    setChkInBytes2,        setChkInBytes4,         setChkInBytes8,          comparestrRegexMatch,
    comparestrRegexNMatch, setChkNotInBytes1,      setChkNotInBytes2,       setChkNotInBytes4,
    setChkNotInBytes8,     compareChkNotInString,  comparestrPatternNMatch, comparewcsPatternNMatch,
    comparewcsRegexMatch,  comparewcsRegexNMatch,  compareLenBinaryVal
};

__compar_fn_t gInt8SignCompare[] = {compareInt8Val,   compareInt8Int16, compareInt8Int32,
                                    compareInt8Int64, compareInt8Float, compareInt8Double};
__compar_fn_t gInt8UsignCompare[] = {compareInt8Uint8, compareInt8Uint16, compareInt8Uint32, compareInt8Uint64};

__compar_fn_t gInt16SignCompare[] = {compareInt16Int8,  compareInt16Val,   compareInt16Int32,
                                     compareInt16Int64, compareInt16Float, compareInt16Double};
__compar_fn_t gInt16UsignCompare[] = {compareInt16Uint8, compareInt16Uint16, compareInt16Uint32, compareInt16Uint64};

__compar_fn_t gInt32SignCompare[] = {compareInt32Int8,  compareInt32Int16, compareInt32Val,
                                     compareInt32Int64, compareInt32Float, compareInt32Double};
__compar_fn_t gInt32UsignCompare[] = {compareInt32Uint8, compareInt32Uint16, compareInt32Uint32, compareInt32Uint64};

__compar_fn_t gInt64SignCompare[] = {compareInt64Int8, compareInt64Int16, compareInt64Int32,
                                     compareInt64Val,  compareInt64Float, compareInt64Double};
__compar_fn_t gInt64UsignCompare[] = {compareInt64Uint8, compareInt64Uint16, compareInt64Uint32, compareInt64Uint64};

__compar_fn_t gFloatSignCompare[] = {compareFloatInt8,  compareFloatInt16, compareFloatInt32,
                                     compareFloatInt64, compareFloatVal,   compareFloatDouble};
__compar_fn_t gFloatUsignCompare[] = {compareFloatUint8, compareFloatUint16, compareFloatUint32, compareFloatUint64};

__compar_fn_t gDoubleSignCompare[] = {compareDoubleInt8,  compareDoubleInt16, compareDoubleInt32,
                                      compareDoubleInt64, compareDoubleFloat, compareDoubleVal};
__compar_fn_t gDoubleUsignCompare[] = {compareDoubleUint8, compareDoubleUint16, compareDoubleUint32,
                                       compareDoubleUint64};

__compar_fn_t gUint8SignCompare[] = {compareUint8Int8,  compareUint8Int16, compareUint8Int32,
                                     compareUint8Int64, compareUint8Float, compareUint8Double};
__compar_fn_t gUint8UsignCompare[] = {compareUint8Val, compareUint8Uint16, compareUint8Uint32, compareUint8Uint64};

__compar_fn_t gUint16SignCompare[] = {compareUint16Int8,  compareUint16Int16, compareUint16Int32,
                                      compareUint16Int64, compareUint16Float, compareUint16Double};
__compar_fn_t gUint16UsignCompare[] = {compareUint16Uint8, compareUint16Val, compareUint16Uint32, compareUint16Uint64};

__compar_fn_t gUint32SignCompare[] = {compareUint32Int8,  compareUint32Int16, compareUint32Int32,
                                      compareUint32Int64, compareUint32Float, compareUint32Double};
__compar_fn_t gUint32UsignCompare[] = {compareUint32Uint8, compareUint32Uint16, compareUint32Val, compareUint32Uint64};

__compar_fn_t gUint64SignCompare[] = {compareUint64Int8,  compareUint64Int16, compareUint64Int32,
                                      compareUint64Int64, compareUint64Float, compareUint64Double};
__compar_fn_t gUint64UsignCompare[] = {compareUint64Uint8, compareUint64Uint16, compareUint64Uint32, compareUint64Val};

int8_t filterGetCompFuncIdx(int32_t type, int32_t optr) {
  int8_t comparFn = 0;

  if (optr == OP_TYPE_IN && (type != TSDB_DATA_TYPE_BINARY && type != TSDB_DATA_TYPE_VARBINARY &&
                             type != TSDB_DATA_TYPE_NCHAR && type != TSDB_DATA_TYPE_GEOMETRY)) {
    switch (type) {
      case TSDB_DATA_TYPE_BOOL:
      case TSDB_DATA_TYPE_TINYINT:
      case TSDB_DATA_TYPE_UTINYINT:
        return 15;
      case TSDB_DATA_TYPE_SMALLINT:
      case TSDB_DATA_TYPE_USMALLINT:
        return 16;
      case TSDB_DATA_TYPE_INT:
      case TSDB_DATA_TYPE_UINT:
      case TSDB_DATA_TYPE_FLOAT:
        return 17;
      case TSDB_DATA_TYPE_BIGINT:
      case TSDB_DATA_TYPE_UBIGINT:
      case TSDB_DATA_TYPE_DOUBLE:
      case TSDB_DATA_TYPE_TIMESTAMP:
        return 18;
      case TSDB_DATA_TYPE_JSON:
        terrno = TSDB_CODE_QRY_JSON_IN_ERROR;
        return 0;
      default:
        return 0;
    }
  }

  if (optr == OP_TYPE_NOT_IN && (type != TSDB_DATA_TYPE_BINARY && type != TSDB_DATA_TYPE_VARBINARY && type != TSDB_DATA_TYPE_NCHAR && type != TSDB_DATA_TYPE_GEOMETRY)) {
    switch (type) {
      case TSDB_DATA_TYPE_BOOL:
      case TSDB_DATA_TYPE_TINYINT:
      case TSDB_DATA_TYPE_UTINYINT:
        return 21;
      case TSDB_DATA_TYPE_SMALLINT:
      case TSDB_DATA_TYPE_USMALLINT:
        return 22;
      case TSDB_DATA_TYPE_INT:
      case TSDB_DATA_TYPE_UINT:
      case TSDB_DATA_TYPE_FLOAT:
        return 23;
      case TSDB_DATA_TYPE_BIGINT:
      case TSDB_DATA_TYPE_UBIGINT:
      case TSDB_DATA_TYPE_DOUBLE:
      case TSDB_DATA_TYPE_TIMESTAMP:
        return 24;
      case TSDB_DATA_TYPE_JSON:
        terrno = TSDB_CODE_QRY_JSON_IN_ERROR;
        return 0;
      default:
        return 0;
    }
  }

  //  if (optr == OP_TYPE_JSON_CONTAINS && type == TSDB_DATA_TYPE_JSON) {
  //    return 28;
  //  }

  switch (type) {
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT:
      comparFn = 1;
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      comparFn = 2;
      break;
    case TSDB_DATA_TYPE_INT:
      comparFn = 0;
      break;
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP:
      comparFn = 3;
      break;
    case TSDB_DATA_TYPE_FLOAT:
      comparFn = 4;
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      comparFn = 5;
      break;
    case TSDB_DATA_TYPE_VARBINARY:{
      if (optr == OP_TYPE_IN) {
        comparFn = 8;
      } else if (optr == OP_TYPE_NOT_IN) {
        comparFn = 25;
      } else { /* normal relational comparFn */
        comparFn = 30;
      }
      break;
    }
    case TSDB_DATA_TYPE_BINARY: {
      if (optr == OP_TYPE_MATCH) {
        comparFn = 19;
      } else if (optr == OP_TYPE_NMATCH) {
        comparFn = 20;
      } else if (optr == OP_TYPE_LIKE) {     /* wildcard query using like operator */
        comparFn = 7;
      } else if (optr == OP_TYPE_NOT_LIKE) { /* wildcard query using like operator */
        comparFn = 26;
      } else if (optr == OP_TYPE_IN) {
        comparFn = 8;
      } else if (optr == OP_TYPE_NOT_IN) {
        comparFn = 25;
      } else { /* normal relational comparFn */
        comparFn = 6;
      }

      break;
    }

    case TSDB_DATA_TYPE_NCHAR: {
      if (optr == OP_TYPE_MATCH) {
        comparFn = 28;
      } else if (optr == OP_TYPE_NMATCH) {
        comparFn = 29;
      } else if (optr == OP_TYPE_LIKE) {
        comparFn = 9;
      } else if (optr == OP_TYPE_NOT_LIKE) {
        comparFn = 27;
      } else if (optr == OP_TYPE_IN) {
        comparFn = 8;
      } else if (optr == OP_TYPE_NOT_IN) {
        comparFn = 25;
      } else {
        comparFn = 10;
      }
      break;
    }

    case TSDB_DATA_TYPE_GEOMETRY: {
      if (optr == OP_TYPE_EQUAL || optr == OP_TYPE_NOT_EQUAL || optr == OP_TYPE_IS_NULL ||
          optr == OP_TYPE_IS_NOT_NULL) {
        comparFn = 30;
      } else if (optr == OP_TYPE_IN) {
        comparFn = 8;
      } else if (optr == OP_TYPE_NOT_IN) {
        comparFn = 25;
      } else {
        terrno = TSDB_CODE_QRY_GEO_NOT_SUPPORT_ERROR;
        return 0;
      }
      break;
    }

    case TSDB_DATA_TYPE_UTINYINT:
      comparFn = 11;
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      comparFn = 12;
      break;
    case TSDB_DATA_TYPE_UINT:
      comparFn = 13;
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      comparFn = 14;
      break;

    default:
      comparFn = 0;
      break;
  }

  return comparFn;
}

__compar_fn_t filterGetCompFunc(int32_t type, int32_t optr) { return gDataCompare[filterGetCompFuncIdx(type, optr)]; }

__compar_fn_t filterGetCompFuncEx(int32_t lType, int32_t rType, int32_t optr) {
  if (TSDB_DATA_TYPE_NULL == rType || TSDB_DATA_TYPE_JSON == rType) {
    return NULL;
  }

  switch (lType) {
    case TSDB_DATA_TYPE_TINYINT: {
      if (IS_SIGNED_NUMERIC_TYPE(rType) || IS_FLOAT_TYPE(rType)) {
        return gInt8SignCompare[rType - TSDB_DATA_TYPE_TINYINT];
      } else {
        return gInt8UsignCompare[rType - TSDB_DATA_TYPE_UTINYINT];
      }
      break;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      if (IS_SIGNED_NUMERIC_TYPE(rType) || IS_FLOAT_TYPE(rType)) {
        return gInt16SignCompare[rType - TSDB_DATA_TYPE_TINYINT];
      } else {
        return gInt16UsignCompare[rType - TSDB_DATA_TYPE_UTINYINT];
      }
      break;
    }
    case TSDB_DATA_TYPE_INT: {
      if (IS_SIGNED_NUMERIC_TYPE(rType) || IS_FLOAT_TYPE(rType)) {
        return gInt32SignCompare[rType - TSDB_DATA_TYPE_TINYINT];
      } else {
        return gInt32UsignCompare[rType - TSDB_DATA_TYPE_UTINYINT];
      }
      break;
    }
    case TSDB_DATA_TYPE_BIGINT: {
      if (IS_SIGNED_NUMERIC_TYPE(rType) || IS_FLOAT_TYPE(rType)) {
        return gInt64SignCompare[rType - TSDB_DATA_TYPE_TINYINT];
      } else {
        return gInt64UsignCompare[rType - TSDB_DATA_TYPE_UTINYINT];
      }
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      if (IS_SIGNED_NUMERIC_TYPE(rType) || IS_FLOAT_TYPE(rType)) {
        return gFloatSignCompare[rType - TSDB_DATA_TYPE_TINYINT];
      } else {
        return gFloatUsignCompare[rType - TSDB_DATA_TYPE_UTINYINT];
      }
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      if (IS_SIGNED_NUMERIC_TYPE(rType) || IS_FLOAT_TYPE(rType)) {
        return gDoubleSignCompare[rType - TSDB_DATA_TYPE_TINYINT];
      } else {
        return gDoubleUsignCompare[rType - TSDB_DATA_TYPE_UTINYINT];
      }
      break;
    }
    case TSDB_DATA_TYPE_UTINYINT: {
      if (IS_SIGNED_NUMERIC_TYPE(rType) || IS_FLOAT_TYPE(rType)) {
        return gUint8SignCompare[rType - TSDB_DATA_TYPE_TINYINT];
      } else {
        return gUint8UsignCompare[rType - TSDB_DATA_TYPE_UTINYINT];
      }
      break;
    }
    case TSDB_DATA_TYPE_USMALLINT: {
      if (IS_SIGNED_NUMERIC_TYPE(rType) || IS_FLOAT_TYPE(rType)) {
        return gUint16SignCompare[rType - TSDB_DATA_TYPE_TINYINT];
      } else {
        return gUint16UsignCompare[rType - TSDB_DATA_TYPE_UTINYINT];
      }
      break;
    }
    case TSDB_DATA_TYPE_UINT: {
      if (IS_SIGNED_NUMERIC_TYPE(rType) || IS_FLOAT_TYPE(rType)) {
        return gUint32SignCompare[rType - TSDB_DATA_TYPE_TINYINT];
      } else {
        return gUint32UsignCompare[rType - TSDB_DATA_TYPE_UTINYINT];
      }
      break;
    }
    case TSDB_DATA_TYPE_UBIGINT: {
      if (IS_SIGNED_NUMERIC_TYPE(rType) || IS_FLOAT_TYPE(rType)) {
        return gUint64SignCompare[rType - TSDB_DATA_TYPE_TINYINT];
      } else {
        return gUint64UsignCompare[rType - TSDB_DATA_TYPE_UTINYINT];
      }
      break;
    }
    default:
      break;
  }
  return NULL;
}

static FORCE_INLINE int32_t filterCompareGroupCtx(const void *pLeft, const void *pRight) {
  SFilterGroupCtx *left = *((SFilterGroupCtx **)pLeft), *right = *((SFilterGroupCtx **)pRight);
  if (left->colNum > right->colNum) return 1;
  if (left->colNum < right->colNum) return -1;
  return 0;
}

int32_t filterInitUnitsFields(SFilterInfo *info) {
  info->unitSize = FILTER_DEFAULT_UNIT_SIZE;
  info->units = taosMemoryCalloc(info->unitSize, sizeof(SFilterUnit));

  info->fields[FLD_TYPE_COLUMN].num = 0;
  info->fields[FLD_TYPE_COLUMN].size = FILTER_DEFAULT_FIELD_SIZE;
  info->fields[FLD_TYPE_COLUMN].fields = taosMemoryCalloc(info->fields[FLD_TYPE_COLUMN].size, sizeof(SFilterField));
  info->fields[FLD_TYPE_VALUE].num = 0;
  info->fields[FLD_TYPE_VALUE].size = FILTER_DEFAULT_FIELD_SIZE;
  info->fields[FLD_TYPE_VALUE].fields = taosMemoryCalloc(info->fields[FLD_TYPE_VALUE].size, sizeof(SFilterField));

  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE SFilterRangeNode *filterNewRange(SFilterRangeCtx *ctx, SFilterRange *ra) {
  SFilterRangeNode *r = NULL;

  if (ctx->rf) {
    r = ctx->rf;
    ctx->rf = ctx->rf->next;
    r->prev = NULL;
    r->next = NULL;
  } else {
    r = taosMemoryCalloc(1, sizeof(SFilterRangeNode));
  }

  FILTER_COPY_RA(&r->ra, ra);

  return r;
}

void *filterInitRangeCtx(int32_t type, int32_t options) {
  if (type > TSDB_DATA_TYPE_UBIGINT || type < TSDB_DATA_TYPE_BOOL ||
      type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_VARBINARY ||
      type == TSDB_DATA_TYPE_NCHAR || type == TSDB_DATA_TYPE_GEOMETRY) {
    qError("not supported range type:%d", type);
    return NULL;
  }

  SFilterRangeCtx *ctx = taosMemoryCalloc(1, sizeof(SFilterRangeCtx));

  ctx->type = type;
  ctx->options = options;
  ctx->pCompareFunc = getComparFunc(type, 0);

  return ctx;
}

int32_t filterResetRangeCtx(SFilterRangeCtx *ctx) {
  ctx->status = 0;

  if (ctx->rf == NULL) {
    ctx->rf = ctx->rs;
    ctx->rs = NULL;
    return TSDB_CODE_SUCCESS;
  }

  ctx->isnull = false;
  ctx->notnull = false;
  ctx->isrange = false;

  SFilterRangeNode *r = ctx->rf;

  while (r && r->next) {
    r = r->next;
  }

  r->next = ctx->rs;
  ctx->rs = NULL;
  return TSDB_CODE_SUCCESS;
}

int32_t filterReuseRangeCtx(SFilterRangeCtx *ctx, int32_t type, int32_t options) {
  filterResetRangeCtx(ctx);

  ctx->type = type;
  ctx->options = options;
  ctx->pCompareFunc = getComparFunc(type, 0);

  return TSDB_CODE_SUCCESS;
}

int32_t filterConvertRange(SFilterRangeCtx *cur, SFilterRange *ra, bool *notNull) {
  int64_t tmp = 0;

  if (!FILTER_GET_FLAG(ra->sflag, RANGE_FLG_NULL)) {
    int32_t sr = cur->pCompareFunc(&ra->s, getDataMin(cur->type, &tmp));
    if (sr == 0) {
      FILTER_SET_FLAG(ra->sflag, RANGE_FLG_NULL);
    }
  }

  if (!FILTER_GET_FLAG(ra->eflag, RANGE_FLG_NULL)) {
    int32_t er = cur->pCompareFunc(&ra->e, getDataMax(cur->type, &tmp));
    if (er == 0) {
      FILTER_SET_FLAG(ra->eflag, RANGE_FLG_NULL);
    }
  }

  if (FILTER_GET_FLAG(ra->sflag, RANGE_FLG_NULL) && FILTER_GET_FLAG(ra->eflag, RANGE_FLG_NULL)) {
    *notNull = true;
  } else {
    *notNull = false;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t filterAddRangeOptr(void *h, uint8_t raOptr, int32_t optr, bool *empty, bool *all) {
  SFilterRangeCtx *ctx = (SFilterRangeCtx *)h;

  if (optr == LOGIC_COND_TYPE_AND) {
    SET_AND_OPTR(ctx, raOptr);
    if (CHK_AND_OPTR(ctx) || (raOptr == FILTER_DUMMY_EMPTY_OPTR)) {
      FILTER_SET_FLAG(ctx->status, MR_ST_EMPTY);
      *empty = true;
    }
  } else {
    SET_OR_OPTR(ctx, raOptr);
    if (CHK_OR_OPTR(ctx)) {
      FILTER_SET_FLAG(ctx->status, MR_ST_ALL);
      *all = true;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t filterAddRangeImpl(void *h, SFilterRange *ra, int32_t optr) {
  SFilterRangeCtx *ctx = (SFilterRangeCtx *)h;

  if (ctx->rs == NULL) {
    if ((FILTER_GET_FLAG(ctx->status, MR_ST_START) == 0) ||
        (FILTER_GET_FLAG(ctx->status, MR_ST_ALL) && (optr == LOGIC_COND_TYPE_AND)) ||
        ((!FILTER_GET_FLAG(ctx->status, MR_ST_ALL)) && (optr == LOGIC_COND_TYPE_OR))) {
      APPEND_RANGE(ctx, ctx->rs, ra);
      FILTER_SET_FLAG(ctx->status, MR_ST_START);
    }

    return TSDB_CODE_SUCCESS;
  }

  SFilterRangeNode *r = ctx->rs;
  SFilterRangeNode *rn = NULL;
  int32_t           cr = 0;

  if (optr == LOGIC_COND_TYPE_AND) {
    while (r != NULL) {
      cr = ctx->pCompareFunc(&r->ra.s, &ra->e);
      if (FILTER_GREATER(cr, r->ra.sflag, ra->eflag)) {
        FREE_FROM_RANGE(ctx, r);
        break;
      }

      cr = ctx->pCompareFunc(&ra->s, &r->ra.e);
      if (FILTER_GREATER(cr, ra->sflag, r->ra.eflag)) {
        rn = r->next;
        FREE_RANGE(ctx, r);
        r = rn;
        continue;
      }

      cr = ctx->pCompareFunc(&ra->s, &r->ra.s);
      if (FILTER_GREATER(cr, ra->sflag, r->ra.sflag)) {
        SIMPLE_COPY_VALUES((char *)&r->ra.s, &ra->s);
        cr == 0 ? (r->ra.sflag |= ra->sflag) : (r->ra.sflag = ra->sflag);
      }

      cr = ctx->pCompareFunc(&r->ra.e, &ra->e);
      if (FILTER_GREATER(cr, r->ra.eflag, ra->eflag)) {
        SIMPLE_COPY_VALUES((char *)&r->ra.e, &ra->e);
        cr == 0 ? (r->ra.eflag |= ra->eflag) : (r->ra.eflag = ra->eflag);
        break;
      }

      r = r->next;
    }

    return TSDB_CODE_SUCCESS;
  }

  // TSDB_RELATION_OR

  bool smerged = false;
  bool emerged = false;

  while (r != NULL) {
    cr = ctx->pCompareFunc(&r->ra.s, &ra->e);
    if (FILTER_GREATER(cr, r->ra.sflag, ra->eflag)) {
      if (emerged == false) {
        INSERT_RANGE(ctx, r, ra);
      }

      break;
    }

    if (smerged == false) {
      cr = ctx->pCompareFunc(&ra->s, &r->ra.e);
      if (FILTER_GREATER(cr, ra->sflag, r->ra.eflag)) {
        if (r->next) {
          r = r->next;
          continue;
        }

        APPEND_RANGE(ctx, r, ra);
        break;
      }

      cr = ctx->pCompareFunc(&r->ra.s, &ra->s);
      if (FILTER_GREATER(cr, r->ra.sflag, ra->sflag)) {
        SIMPLE_COPY_VALUES((char *)&r->ra.s, &ra->s);
        cr == 0 ? (r->ra.sflag &= ra->sflag) : (r->ra.sflag = ra->sflag);
      }

      smerged = true;
    }

    if (emerged == false) {
      cr = ctx->pCompareFunc(&ra->e, &r->ra.e);
      if (FILTER_GREATER(cr, ra->eflag, r->ra.eflag)) {
        SIMPLE_COPY_VALUES((char *)&r->ra.e, &ra->e);
        if (cr == 0) {
          r->ra.eflag &= ra->eflag;
          break;
        }

        r->ra.eflag = ra->eflag;
        emerged = true;
        r = r->next;
        continue;
      }

      break;
    }

    cr = ctx->pCompareFunc(&ra->e, &r->ra.e);
    if (FILTER_GREATER(cr, ra->eflag, r->ra.eflag)) {
      rn = r->next;
      FREE_RANGE(ctx, r);
      r = rn;

      continue;
    } else {
      SIMPLE_COPY_VALUES(&r->prev->ra.e, (char *)&r->ra.e);
      cr == 0 ? (r->prev->ra.eflag &= r->ra.eflag) : (r->prev->ra.eflag = r->ra.eflag);
      FREE_RANGE(ctx, r);

      break;
    }
  }

  if (ctx->rs && ctx->rs->next == NULL) {
    bool notnull;
    filterConvertRange(ctx, &ctx->rs->ra, &notnull);
    if (notnull) {
      bool all = false;
      FREE_FROM_RANGE(ctx, ctx->rs);
      filterAddRangeOptr(h, OP_TYPE_IS_NOT_NULL, optr, NULL, &all);
      if (all) {
        FILTER_SET_FLAG(ctx->status, MR_ST_ALL);
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t filterAddRange(void *h, SFilterRange *ra, int32_t optr) {
  SFilterRangeCtx *ctx = (SFilterRangeCtx *)h;
  int64_t          tmp = 0;

  if (FILTER_GET_FLAG(ra->sflag, RANGE_FLG_NULL)) {
    SIMPLE_COPY_VALUES(&ra->s, getDataMin(ctx->type, &tmp));
    // FILTER_CLR_FLAG(ra->sflag, RA_NULL);
  }

  if (FILTER_GET_FLAG(ra->eflag, RANGE_FLG_NULL)) {
    SIMPLE_COPY_VALUES(&ra->e, getDataMax(ctx->type, &tmp));
    // FILTER_CLR_FLAG(ra->eflag, RA_NULL);
  }

  return filterAddRangeImpl(h, ra, optr);
}

int32_t filterAddRangeCtx(void *dst, void *src, int32_t optr) {
  SFilterRangeCtx *dctx = (SFilterRangeCtx *)dst;
  SFilterRangeCtx *sctx = (SFilterRangeCtx *)src;

  ASSERT(optr == LOGIC_COND_TYPE_OR);

  if (sctx->rs == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  SFilterRangeNode *r = sctx->rs;

  while (r) {
    filterAddRange(dctx, &r->ra, optr);
    r = r->next;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t filterCopyRangeCtx(void *dst, void *src) {
  SFilterRangeCtx *dctx = (SFilterRangeCtx *)dst;
  SFilterRangeCtx *sctx = (SFilterRangeCtx *)src;

  dctx->status = sctx->status;

  dctx->isnull = sctx->isnull;
  dctx->notnull = sctx->notnull;
  dctx->isrange = sctx->isrange;

  SFilterRangeNode *r = sctx->rs;
  SFilterRangeNode *dr = dctx->rs;

  while (r) {
    APPEND_RANGE(dctx, dr, &r->ra);
    if (dr == NULL) {
      dr = dctx->rs;
    } else {
      dr = dr->next;
    }
    r = r->next;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t filterFinishRange(void *h) {
  SFilterRangeCtx *ctx = (SFilterRangeCtx *)h;

  if (FILTER_GET_FLAG(ctx->status, MR_ST_FIN)) {
    return TSDB_CODE_SUCCESS;
  }

  if (FILTER_GET_FLAG(ctx->options, FLT_OPTION_TIMESTAMP)) {
    SFilterRangeNode *r = ctx->rs;
    SFilterRangeNode *rn = NULL;

    while (r && r->next) {
      int64_t tmp = 1;
      int32_t code = operateVal(&tmp, &r->ra.e, &tmp, OP_TYPE_ADD, ctx->type);
      if (code != 0) {
        return TSDB_CODE_APP_ERROR;
      }
      if (ctx->pCompareFunc(&tmp, &r->next->ra.s) == 0) {
        rn = r->next;
        SIMPLE_COPY_VALUES((char *)&r->next->ra.s, (char *)&r->ra.s);
        FREE_RANGE(ctx, r);
        r = rn;

        continue;
      }

      r = r->next;
    }
  }

  FILTER_SET_FLAG(ctx->status, MR_ST_FIN);

  return TSDB_CODE_SUCCESS;
}

int32_t filterGetRangeNum(void *h, int32_t *num) {
  filterFinishRange(h);

  SFilterRangeCtx *ctx = (SFilterRangeCtx *)h;

  *num = 0;

  SFilterRangeNode *r = ctx->rs;

  while (r) {
    ++(*num);
    r = r->next;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t filterGetRangeRes(void *h, SFilterRange *ra) {
  filterFinishRange(h);

  SFilterRangeCtx  *ctx = (SFilterRangeCtx *)h;
  uint32_t          num = 0;
  SFilterRangeNode *r = ctx->rs;

  while (r) {
    if (num) {
      ra->e = r->ra.e;
      ra->eflag = r->ra.eflag;
    } else {
      FILTER_COPY_RA(ra, &r->ra);
    }

    ++num;
    r = r->next;
  }

  if (num == 0) {
    qError("no range result");
    return TSDB_CODE_APP_ERROR;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t filterSourceRangeFromCtx(SFilterRangeCtx *ctx, void *sctx, int32_t optr, bool *empty, bool *all) {
  SFilterRangeCtx *src = (SFilterRangeCtx *)sctx;

  if (src->isnull) {
    filterAddRangeOptr(ctx, OP_TYPE_IS_NULL, optr, empty, all);
    if (FILTER_GET_FLAG(ctx->status, MR_ST_ALL)) {
      *all = true;
    }
  }

  if (src->notnull) {
    filterAddRangeOptr(ctx, OP_TYPE_IS_NOT_NULL, optr, empty, all);
    if (FILTER_GET_FLAG(ctx->status, MR_ST_ALL)) {
      *all = true;
    }
  }

  if (src->isrange) {
    filterAddRangeOptr(ctx, 0, optr, empty, all);

    if (!(optr == LOGIC_COND_TYPE_OR && ctx->notnull)) {
      filterAddRangeCtx(ctx, src, optr);
    }

    if (FILTER_GET_FLAG(ctx->status, MR_ST_ALL)) {
      *all = true;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t filterFreeRangeCtx(void *h) {
  if (h == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  SFilterRangeCtx  *ctx = (SFilterRangeCtx *)h;
  SFilterRangeNode *r = ctx->rs;
  SFilterRangeNode *rn = NULL;

  while (r) {
    rn = r->next;
    taosMemoryFree(r);
    r = rn;
  }

  r = ctx->rf;
  while (r) {
    rn = r->next;
    taosMemoryFree(r);
    r = rn;
  }

  taosMemoryFree(ctx);

  return TSDB_CODE_SUCCESS;
}

int32_t filterDetachCnfGroup(SFilterGroup *gp1, SFilterGroup *gp2, SArray *group) {
  SFilterGroup gp = {0};

  gp.unitNum = gp1->unitNum + gp2->unitNum;
  gp.unitIdxs = taosMemoryCalloc(gp.unitNum, sizeof(*gp.unitIdxs));
  memcpy(gp.unitIdxs, gp1->unitIdxs, gp1->unitNum * sizeof(*gp.unitIdxs));
  memcpy(gp.unitIdxs + gp1->unitNum, gp2->unitIdxs, gp2->unitNum * sizeof(*gp.unitIdxs));

  gp.unitFlags = NULL;

  taosArrayPush(group, &gp);

  return TSDB_CODE_SUCCESS;
}

int32_t filterDetachCnfGroups(SArray *group, SArray *left, SArray *right) {
  int32_t leftSize = (int32_t)taosArrayGetSize(left);
  int32_t rightSize = (int32_t)taosArrayGetSize(right);

  if (taosArrayGetSize(left) <= 0) {
    if (taosArrayGetSize(right) <= 0) {
      fltDebug("both groups are empty");
      return TSDB_CODE_SUCCESS;
    }

    SFilterGroup *gp = NULL;
    while ((gp = (SFilterGroup *)taosArrayPop(right)) != NULL) {
      taosArrayPush(group, gp);
    }

    return TSDB_CODE_SUCCESS;
  }

  if (taosArrayGetSize(right) <= 0) {
    SFilterGroup *gp = NULL;
    while ((gp = (SFilterGroup *)taosArrayPop(left)) != NULL) {
      taosArrayPush(group, gp);
    }

    return TSDB_CODE_SUCCESS;
  }

  for (int32_t l = 0; l < leftSize; ++l) {
    SFilterGroup *gp1 = taosArrayGet(left, l);

    for (int32_t r = 0; r < rightSize; ++r) {
      SFilterGroup *gp2 = taosArrayGet(right, r);

      filterDetachCnfGroup(gp1, gp2, group);
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t filterGetFiledByDesc(SFilterFields *fields, int32_t type, void *v) {
  for (uint32_t i = 0; i < fields->num; ++i) {
    if (nodesEqualNode(fields->fields[i].desc, v)) {
      return i;
    }
  }

  return -1;
}

int32_t filterGetFiledByData(SFilterInfo *info, int32_t type, void *v, int32_t dataLen, bool *sameBuf) {
  if (type == FLD_TYPE_VALUE) {
    if (info->pctx.valHash == false) {
      qError("value hash is empty");
      return -1;
    }

    SFilterDataInfo *dInfo = taosHashGet(info->pctx.valHash, v, dataLen);
    if (dInfo) {
      *sameBuf = (dInfo->addr == v);
      return dInfo->idx;
    }
  }

  return -1;
}

// In the params, we should use void *data instead of void **data, there is no need to use taosMemoryFreeClear(*data) to
// set *data = 0 Besides, fields data value is a pointer, so dataLen should be POINTER_BYTES for better.
int32_t filterAddField(SFilterInfo *info, void *desc, void **data, int32_t type, SFilterFieldId *fid, int32_t dataLen,
                       bool freeIfExists, int16_t *srcFlag) {
  int32_t   idx = -1;
  uint32_t *num;
  bool      sameBuf = false;

  num = &info->fields[type].num;

  if (*num > 0) {
    if (type == FLD_TYPE_COLUMN) {
      idx = filterGetFiledByDesc(&info->fields[type], type, desc);
    } else if (data && (*data) && dataLen > 0 && FILTER_GET_FLAG(info->options, FLT_OPTION_NEED_UNIQE)) {
      idx = filterGetFiledByData(info, type, *data, dataLen, &sameBuf);
    }
  }

  if (idx < 0) {
    idx = *num;
    if (idx >= info->fields[type].size) {
      info->fields[type].size += FILTER_DEFAULT_FIELD_SIZE;
      info->fields[type].fields =
          taosMemoryRealloc(info->fields[type].fields, info->fields[type].size * sizeof(SFilterField));
    }

    info->fields[type].fields[idx].flag = type;
    info->fields[type].fields[idx].desc = desc;
    info->fields[type].fields[idx].data = data ? *data : NULL;

    if (type == FLD_TYPE_COLUMN) {
      FILTER_SET_FLAG(info->fields[type].fields[idx].flag, FLD_DATA_NO_FREE);
    }

    ++(*num);

    if (data && (*data) && dataLen > 0 && FILTER_GET_FLAG(info->options, FLT_OPTION_NEED_UNIQE)) {
      if (info->pctx.valHash == NULL) {
        info->pctx.valHash = taosHashInit(FILTER_DEFAULT_GROUP_SIZE * FILTER_DEFAULT_VALUE_SIZE,
                                          taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, false);
      }

      SFilterDataInfo dInfo = {idx, *data};
      taosHashPut(info->pctx.valHash, *data, dataLen, &dInfo, sizeof(dInfo));
      if (srcFlag) {
        FILTER_SET_FLAG(*srcFlag, FLD_DATA_NO_FREE);
      }
    }
  } else if (type != FLD_TYPE_COLUMN && data) {
    if (freeIfExists) {
      taosMemoryFreeClear(*data);
    } else if (sameBuf) {
      FILTER_SET_FLAG(*srcFlag, FLD_DATA_NO_FREE);
    }
  }

  fid->type = type;
  fid->idx = idx;

  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE int32_t filterAddColFieldFromField(SFilterInfo *info, SFilterField *field, SFilterFieldId *fid) {
  filterAddField(info, field->desc, &field->data, FILTER_GET_TYPE(field->flag), fid, 0, false, NULL);

  FILTER_SET_FLAG(field->flag, FLD_DATA_NO_FREE);

  return TSDB_CODE_SUCCESS;
}

int32_t filterAddFieldFromNode(SFilterInfo *info, SNode *node, SFilterFieldId *fid) {
  if (node == NULL) {
    fltDebug("empty node");
    goto _return;
  }

  if (nodeType(node) != QUERY_NODE_COLUMN && nodeType(node) != QUERY_NODE_VALUE &&
      nodeType(node) != QUERY_NODE_NODE_LIST) {
    goto _return;
  }

  int32_t type;
  void   *v;

  if (nodeType(node) == QUERY_NODE_COLUMN) {
    type = FLD_TYPE_COLUMN;
    v = node;
  } else {
    type = FLD_TYPE_VALUE;
    v = node;
  }

  filterAddField(info, v, NULL, type, fid, 0, true, NULL);

_return:
  return TSDB_CODE_SUCCESS;
}

int32_t filterAddUnitImpl(SFilterInfo *info, uint8_t optr, SFilterFieldId *left, SFilterFieldId *right, uint8_t optr2,
                          SFilterFieldId *right2, uint32_t *uidx) {
  if (FILTER_GET_FLAG(info->options, FLT_OPTION_NEED_UNIQE)) {
    if (info->pctx.unitHash == NULL) {
      info->pctx.unitHash = taosHashInit(FILTER_DEFAULT_GROUP_SIZE * FILTER_DEFAULT_UNIT_SIZE,
                                         taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, false);
    } else {
      char v[14] = {0};
      FLT_PACKAGE_UNIT_HASH_KEY(&v, optr, optr2, left->idx, (right ? right->idx : -1), (right2 ? right2->idx : -1));
      void *hu = taosHashGet(info->pctx.unitHash, v, sizeof(v));
      if (hu) {
        *uidx = *(uint32_t *)hu;
        return TSDB_CODE_SUCCESS;
      }
    }
  }

  if (info->unitNum >= info->unitSize) {
    uint32_t psize = info->unitSize;
    info->unitSize += FILTER_DEFAULT_UNIT_SIZE;

    void *tmp = taosMemoryRealloc(info->units, info->unitSize * sizeof(SFilterUnit));
    if (tmp == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    info->units = (SFilterUnit *)tmp;
    memset(info->units + psize, 0, sizeof(*info->units) * FILTER_DEFAULT_UNIT_SIZE);
  }

  SFilterUnit *u = &info->units[info->unitNum];

  u->compare.optr = optr;
  u->left = *left;
  if (right) {
    u->right = *right;
  }
  u->compare.optr2 = optr2;
  if (right2) {
    u->right2 = *right2;
  }

  if (u->right.type == FLD_TYPE_VALUE) {
    SFilterField *val = FILTER_UNIT_RIGHT_FIELD(info, u);
    ASSERT(FILTER_GET_FLAG(val->flag, FLD_TYPE_VALUE));
  } else {
    int32_t paramNum = scalarGetOperatorParamNum(optr);
    if (1 != paramNum) {
      fltError("invalid right field in unit, operator:%s, rightType:%d", operatorTypeStr(optr), u->right.type);
      return TSDB_CODE_APP_ERROR;
    }
  }

  SFilterField *col = FILTER_UNIT_LEFT_FIELD(info, u);
  ASSERT(FILTER_GET_FLAG(col->flag, FLD_TYPE_COLUMN));

  info->units[info->unitNum].compare.type = FILTER_GET_COL_FIELD_TYPE(col);
  info->units[info->unitNum].compare.precision = FILTER_GET_COL_FIELD_PRECISION(col);

  *uidx = info->unitNum;

  if (FILTER_GET_FLAG(info->options, FLT_OPTION_NEED_UNIQE)) {
    char v[14] = {0};
    FLT_PACKAGE_UNIT_HASH_KEY(&v, optr, optr2, left->idx, (right ? right->idx : -1), (right2 ? right2->idx : -1));
    taosHashPut(info->pctx.unitHash, v, sizeof(v), uidx, sizeof(*uidx));
  }

  ++info->unitNum;

  return TSDB_CODE_SUCCESS;
}

int32_t filterAddUnit(SFilterInfo *info, uint8_t optr, SFilterFieldId *left, SFilterFieldId *right, uint32_t *uidx) {
  return filterAddUnitImpl(info, optr, left, right, 0, NULL, uidx);
}

int32_t filterAddUnitToGroup(SFilterGroup *group, uint32_t unitIdx) {
  if (group->unitNum >= group->unitSize) {
    group->unitSize += FILTER_DEFAULT_UNIT_SIZE;

    void *tmp = taosMemoryRealloc(group->unitIdxs, group->unitSize * sizeof(*group->unitIdxs));
    if (tmp == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    group->unitIdxs = tmp;
  }

  group->unitIdxs[group->unitNum++] = unitIdx;

  return TSDB_CODE_SUCCESS;
}

int32_t fltAddGroupUnitFromNode(SFilterInfo *info, SNode *tree, SArray *group) {
  SOperatorNode *node = (SOperatorNode *)tree;
  int32_t        ret = TSDB_CODE_SUCCESS;
  SFilterFieldId left = {0}, right = {0};
  filterAddFieldFromNode(info, node->pLeft, &left);
  uint8_t  type = FILTER_GET_COL_FIELD_TYPE(FILTER_GET_FIELD(info, left));
  int32_t  len = 0;
  uint32_t uidx = 0;
  int32_t  code = 0;

  if (node->opType == OP_TYPE_IN && (!IS_VAR_DATA_TYPE(type))) {
    SNodeListNode *listNode = (SNodeListNode *)node->pRight;
    SListCell     *cell = listNode->pNodeList->pHead;

    SScalarParam out = {.columnData = taosMemoryCalloc(1, sizeof(SColumnInfoData))};
    out.columnData->info.type = type;
    out.columnData->info.bytes = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes;  // reserved space for simple_copy

    for (int32_t i = 0; i < listNode->pNodeList->length; ++i) {
      SValueNode *valueNode = (SValueNode *)cell->pNode;
      if (valueNode->node.resType.type != type) {
        int32_t overflow = 0;
        code = sclConvertValueToSclParam(valueNode, &out, &overflow);
        if (code) {
          //        fltError("convert from %d to %d failed", in.type, out.type);
          FLT_ERR_RET(code);
        }

        if (overflow) {
          cell = cell->pNext;
          continue;
        }

        len = tDataTypes[type].bytes;

        filterAddField(info, NULL, (void **)&out.columnData->pData, FLD_TYPE_VALUE, &right, len, true, NULL);
        out.columnData->pData = NULL;
      } else {
        void *data = taosMemoryCalloc(1, tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes);  // reserved space for simple_copy
        if (NULL == data) {
          FLT_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
        }
        memcpy(data, nodesGetValueFromNode(valueNode), tDataTypes[type].bytes);
        filterAddField(info, NULL, (void **)&data, FLD_TYPE_VALUE, &right, len, true, NULL);
      }
      filterAddUnit(info, OP_TYPE_EQUAL, &left, &right, &uidx);

      SFilterGroup fgroup = {0};
      filterAddUnitToGroup(&fgroup, uidx);

      taosArrayPush(group, &fgroup);

      cell = cell->pNext;
    }
    colDataDestroy(out.columnData);
    taosMemoryFree(out.columnData);
  } else {
    filterAddFieldFromNode(info, node->pRight, &right);

    FLT_ERR_RET(filterAddUnit(info, node->opType, &left, &right, &uidx));
    SFilterGroup fgroup = {0};
    filterAddUnitToGroup(&fgroup, uidx);

    taosArrayPush(group, &fgroup);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t filterAddUnitFromUnit(SFilterInfo *dst, SFilterInfo *src, SFilterUnit *u, uint32_t *uidx) {
  SFilterFieldId left, right, *pright = &right;
  uint8_t        type = FILTER_UNIT_DATA_TYPE(u);
  uint16_t       flag = 0;

  filterAddField(dst, FILTER_UNIT_COL_DESC(src, u), NULL, FLD_TYPE_COLUMN, &left, 0, false, NULL);
  SFilterField *t = FILTER_UNIT_LEFT_FIELD(src, u);

  if (u->right.type == FLD_TYPE_VALUE) {
    void         *data = FILTER_UNIT_VAL_DATA(src, u);
    SFilterField *rField = FILTER_UNIT_RIGHT_FIELD(src, u);

    if (IS_VAR_DATA_TYPE(type)) {
      if (FILTER_UNIT_OPTR(u) == OP_TYPE_IN) {
        filterAddField(dst, NULL, &data, FLD_TYPE_VALUE, &right, POINTER_BYTES, false,
                       &rField->flag);  // POINTER_BYTES should be sizeof(SHashObj), but POINTER_BYTES is also right.

        t = FILTER_GET_FIELD(dst, right);
        FILTER_SET_FLAG(t->flag, FLD_DATA_IS_HASH);
      } else {
        filterAddField(dst, NULL, &data, FLD_TYPE_VALUE, &right, varDataTLen(data), false, &rField->flag);
      }
    } else {
      filterAddField(dst, NULL, &data, FLD_TYPE_VALUE, &right, tDataTypes[type].bytes, false, &rField->flag);
    }
  } else {
    pright = NULL;
  }

  return filterAddUnit(dst, FILTER_UNIT_OPTR(u), &left, pright, uidx);
}

int32_t filterAddUnitRight(SFilterInfo *info, uint8_t optr, SFilterFieldId *right, uint32_t uidx) {
  SFilterUnit *u = &info->units[uidx];
  u->compare.optr2 = optr;
  u->right2 = *right;

  return TSDB_CODE_SUCCESS;
}

int32_t filterAddGroupUnitFromCtx(SFilterInfo *dst, SFilterInfo *src, SFilterRangeCtx *ctx, uint32_t cidx,
                                  SFilterGroup *g, int32_t optr, SArray *res) {
  SFilterFieldId left, right, right2;
  uint32_t       uidx = 0;

  SFilterField *col = FILTER_GET_COL_FIELD(src, cidx);

  filterAddColFieldFromField(dst, col, &left);

  int32_t type = FILTER_GET_COL_FIELD_TYPE(FILTER_GET_FIELD(dst, left));

  if (optr == LOGIC_COND_TYPE_AND) {
    if (ctx->isnull) {
      ASSERT(ctx->notnull == false && ctx->isrange == false);
      filterAddUnit(dst, OP_TYPE_IS_NULL, &left, NULL, &uidx);
      filterAddUnitToGroup(g, uidx);
      return TSDB_CODE_SUCCESS;
    }

    if (ctx->notnull) {
      ASSERT(ctx->isnull == false && ctx->isrange == false);
      filterAddUnit(dst, OP_TYPE_IS_NOT_NULL, &left, NULL, &uidx);
      filterAddUnitToGroup(g, uidx);
      return TSDB_CODE_SUCCESS;
    }

    if (!ctx->isrange) {
      ASSERT(ctx->isnull || ctx->notnull);
      return TSDB_CODE_SUCCESS;
    }

    ASSERT(ctx->rs && ctx->rs->next == NULL);

    SFilterRange *ra = &ctx->rs->ra;

    ASSERT(!((FILTER_GET_FLAG(ra->sflag, RANGE_FLG_NULL)) && (FILTER_GET_FLAG(ra->eflag, RANGE_FLG_NULL))));

    if ((!FILTER_GET_FLAG(ra->sflag, RANGE_FLG_NULL)) && (!FILTER_GET_FLAG(ra->eflag, RANGE_FLG_NULL))) {
      __compar_fn_t func = getComparFunc(type, 0);
      if (func(&ra->s, &ra->e) == 0) {
        void *data = taosMemoryMalloc(sizeof(int64_t));
        SIMPLE_COPY_VALUES(data, &ra->s);
        filterAddField(dst, NULL, &data, FLD_TYPE_VALUE, &right, tDataTypes[type].bytes, true, NULL);
        filterAddUnit(dst, OP_TYPE_EQUAL, &left, &right, &uidx);
        filterAddUnitToGroup(g, uidx);
        return TSDB_CODE_SUCCESS;
      } else {
        void *data = taosMemoryMalloc(sizeof(int64_t));
        SIMPLE_COPY_VALUES(data, &ra->s);
        filterAddField(dst, NULL, &data, FLD_TYPE_VALUE, &right, tDataTypes[type].bytes, true, NULL);
        void *data2 = taosMemoryMalloc(sizeof(int64_t));
        SIMPLE_COPY_VALUES(data2, &ra->e);
        filterAddField(dst, NULL, &data2, FLD_TYPE_VALUE, &right2, tDataTypes[type].bytes, true, NULL);

        filterAddUnitImpl(
            dst, FILTER_GET_FLAG(ra->sflag, RANGE_FLG_EXCLUDE) ? OP_TYPE_GREATER_THAN : OP_TYPE_GREATER_EQUAL, &left,
            &right, FILTER_GET_FLAG(ra->eflag, RANGE_FLG_EXCLUDE) ? OP_TYPE_LOWER_THAN : OP_TYPE_LOWER_EQUAL, &right2,
            &uidx);
        filterAddUnitToGroup(g, uidx);
        return TSDB_CODE_SUCCESS;
      }
    }

    if (!FILTER_GET_FLAG(ra->sflag, RANGE_FLG_NULL)) {
      void *data = taosMemoryMalloc(sizeof(int64_t));
      SIMPLE_COPY_VALUES(data, &ra->s);
      filterAddField(dst, NULL, &data, FLD_TYPE_VALUE, &right, tDataTypes[type].bytes, true, NULL);
      filterAddUnit(dst, FILTER_GET_FLAG(ra->sflag, RANGE_FLG_EXCLUDE) ? OP_TYPE_GREATER_THAN : OP_TYPE_GREATER_EQUAL,
                    &left, &right, &uidx);
      filterAddUnitToGroup(g, uidx);
    }

    if (!FILTER_GET_FLAG(ra->eflag, RANGE_FLG_NULL)) {
      void *data = taosMemoryMalloc(sizeof(int64_t));
      SIMPLE_COPY_VALUES(data, &ra->e);
      filterAddField(dst, NULL, &data, FLD_TYPE_VALUE, &right, tDataTypes[type].bytes, true, NULL);
      filterAddUnit(dst, FILTER_GET_FLAG(ra->eflag, RANGE_FLG_EXCLUDE) ? OP_TYPE_LOWER_THAN : OP_TYPE_LOWER_EQUAL,
                    &left, &right, &uidx);
      filterAddUnitToGroup(g, uidx);
    }

    return TSDB_CODE_SUCCESS;
  }

  // OR PROCESS

  SFilterGroup ng = {0};
  g = &ng;

  ASSERT(ctx->isnull || ctx->notnull || ctx->isrange);

  if (ctx->isnull) {
    filterAddUnit(dst, OP_TYPE_IS_NULL, &left, NULL, &uidx);
    filterAddUnitToGroup(g, uidx);
    taosArrayPush(res, g);
  }

  if (ctx->notnull) {
    ASSERT(!ctx->isrange);
    memset(g, 0, sizeof(*g));

    filterAddUnit(dst, OP_TYPE_IS_NOT_NULL, &left, NULL, &uidx);
    filterAddUnitToGroup(g, uidx);
    taosArrayPush(res, g);
  }

  if (!ctx->isrange) {
    ASSERT(ctx->isnull || ctx->notnull);
    g->unitNum = 0;
    return TSDB_CODE_SUCCESS;
  }

  SFilterRangeNode *r = ctx->rs;

  while (r) {
    memset(g, 0, sizeof(*g));

    if ((!FILTER_GET_FLAG(r->ra.sflag, RANGE_FLG_NULL)) && (!FILTER_GET_FLAG(r->ra.eflag, RANGE_FLG_NULL))) {
      __compar_fn_t func = getComparFunc(type, 0);
      if (func(&r->ra.s, &r->ra.e) == 0) {
        void *data = taosMemoryMalloc(sizeof(int64_t));
        SIMPLE_COPY_VALUES(data, &r->ra.s);
        filterAddField(dst, NULL, &data, FLD_TYPE_VALUE, &right, tDataTypes[type].bytes, true, NULL);
        filterAddUnit(dst, OP_TYPE_EQUAL, &left, &right, &uidx);
        filterAddUnitToGroup(g, uidx);
      } else {
        void *data = taosMemoryMalloc(sizeof(int64_t));
        SIMPLE_COPY_VALUES(data, &r->ra.s);
        filterAddField(dst, NULL, &data, FLD_TYPE_VALUE, &right, tDataTypes[type].bytes, true, NULL);
        void *data2 = taosMemoryMalloc(sizeof(int64_t));
        SIMPLE_COPY_VALUES(data2, &r->ra.e);
        filterAddField(dst, NULL, &data2, FLD_TYPE_VALUE, &right2, tDataTypes[type].bytes, true, NULL);

        filterAddUnitImpl(
            dst, FILTER_GET_FLAG(r->ra.sflag, RANGE_FLG_EXCLUDE) ? OP_TYPE_GREATER_THAN : OP_TYPE_GREATER_EQUAL, &left,
            &right, FILTER_GET_FLAG(r->ra.eflag, RANGE_FLG_EXCLUDE) ? OP_TYPE_LOWER_THAN : OP_TYPE_LOWER_EQUAL, &right2,
            &uidx);
        filterAddUnitToGroup(g, uidx);
      }

      taosArrayPush(res, g);

      r = r->next;

      continue;
    }

    if (!FILTER_GET_FLAG(r->ra.sflag, RANGE_FLG_NULL)) {
      void *data = taosMemoryMalloc(sizeof(int64_t));
      SIMPLE_COPY_VALUES(data, &r->ra.s);
      filterAddField(dst, NULL, &data, FLD_TYPE_VALUE, &right, tDataTypes[type].bytes, true, NULL);
      filterAddUnit(dst, FILTER_GET_FLAG(r->ra.sflag, RANGE_FLG_EXCLUDE) ? OP_TYPE_GREATER_THAN : OP_TYPE_GREATER_EQUAL,
                    &left, &right, &uidx);
      filterAddUnitToGroup(g, uidx);
    }

    if (!FILTER_GET_FLAG(r->ra.eflag, RANGE_FLG_NULL)) {
      void *data = taosMemoryMalloc(sizeof(int64_t));
      SIMPLE_COPY_VALUES(data, &r->ra.e);
      filterAddField(dst, NULL, &data, FLD_TYPE_VALUE, &right, tDataTypes[type].bytes, true, NULL);
      filterAddUnit(dst, FILTER_GET_FLAG(r->ra.eflag, RANGE_FLG_EXCLUDE) ? OP_TYPE_LOWER_THAN : OP_TYPE_LOWER_EQUAL,
                    &left, &right, &uidx);
      filterAddUnitToGroup(g, uidx);
    }

    ASSERT(g->unitNum > 0);

    taosArrayPush(res, g);

    r = r->next;
  }

  g->unitNum = 0;

  return TSDB_CODE_SUCCESS;
}

static void filterFreeGroup(void *pItem) {
  if (pItem == NULL) {
    return;
  }

  SFilterGroup *p = (SFilterGroup *)pItem;
  taosMemoryFreeClear(p->unitIdxs);
  taosMemoryFreeClear(p->unitFlags);
}

EDealRes fltTreeToGroup(SNode *pNode, void *pContext) {
  int32_t            code = TSDB_CODE_SUCCESS;
  SArray            *preGroup = NULL;
  SArray            *newGroup = NULL;
  SArray            *resGroup = NULL;
  ENodeType          nType = nodeType(pNode);
  SFltBuildGroupCtx *ctx = (SFltBuildGroupCtx *)pContext;

  if (QUERY_NODE_LOGIC_CONDITION == nodeType(pNode)) {
    SLogicConditionNode *node = (SLogicConditionNode *)pNode;
    if (LOGIC_COND_TYPE_AND == node->condType) {
      SListCell *cell = node->pParameterList->pHead;
      for (int32_t i = 0; i < node->pParameterList->length; ++i) {
        newGroup = taosArrayInit(4, sizeof(SFilterGroup));
        resGroup = taosArrayInit(4, sizeof(SFilterGroup));

        SFltBuildGroupCtx tctx = {.info = ctx->info, .group = newGroup};
        nodesWalkExpr(cell->pNode, fltTreeToGroup, (void *)&tctx);
        FLT_ERR_JRET(tctx.code);

        FLT_ERR_JRET(filterDetachCnfGroups(resGroup, preGroup, newGroup));

        taosArrayDestroyEx(newGroup, filterFreeGroup);
        newGroup = NULL;
        taosArrayDestroyEx(preGroup, filterFreeGroup);

        preGroup = resGroup;
        resGroup = NULL;

        cell = cell->pNext;
      }

      taosArrayAddAll(ctx->group, preGroup);

      taosArrayDestroy(preGroup);

      return DEAL_RES_IGNORE_CHILD;
    }

    if (LOGIC_COND_TYPE_OR == node->condType) {
      SListCell *cell = node->pParameterList->pHead;
      for (int32_t i = 0; i < node->pParameterList->length; ++i) {
        nodesWalkExpr(cell->pNode, fltTreeToGroup, (void *)pContext);
        FLT_ERR_JRET(ctx->code);

        cell = cell->pNext;
      }

      return DEAL_RES_IGNORE_CHILD;
    }

    ctx->code = TSDB_CODE_APP_ERROR;

    fltError("invalid condition type, type:%d", node->condType);

    return DEAL_RES_ERROR;
  }

  if (QUERY_NODE_OPERATOR == nType) {
    FLT_ERR_JRET(fltAddGroupUnitFromNode(ctx->info, pNode, ctx->group));

    return DEAL_RES_IGNORE_CHILD;
  }

  if (QUERY_NODE_VALUE == nType && ((SValueNode*)pNode)->node.resType.type == TSDB_DATA_TYPE_BOOL) {
    if (((SValueNode*)pNode)->datum.b) {
      FILTER_SET_FLAG(ctx->info->status, FI_STATUS_ALL);
    } else {
      FILTER_SET_FLAG(ctx->info->status, FI_STATUS_EMPTY);
    }
    return DEAL_RES_END;
  }
  
  fltError("invalid node type for filter, type:%d", nodeType(pNode));

  code = TSDB_CODE_QRY_INVALID_INPUT;

_return:

  taosArrayDestroyEx(newGroup, filterFreeGroup);
  taosArrayDestroyEx(preGroup, filterFreeGroup);
  taosArrayDestroyEx(resGroup, filterFreeGroup);

  ctx->code = code;

  return DEAL_RES_ERROR;
}

int32_t fltConverToStr(char *str, int type, void *buf, int32_t bufSize, int32_t *len) {
  int32_t n = 0;

  switch (type) {
    case TSDB_DATA_TYPE_NULL:
      n = sprintf(str, "null");
      break;

    case TSDB_DATA_TYPE_BOOL:
      n = sprintf(str, (*(int8_t *)buf) ? "true" : "false");
      break;

    case TSDB_DATA_TYPE_TINYINT:
      n = sprintf(str, "%d", *(int8_t *)buf);
      break;

    case TSDB_DATA_TYPE_SMALLINT:
      n = sprintf(str, "%d", *(int16_t *)buf);
      break;

    case TSDB_DATA_TYPE_INT:
      n = sprintf(str, "%d", *(int32_t *)buf);
      break;

    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP:
      n = sprintf(str, "%" PRId64, *(int64_t *)buf);
      break;

    case TSDB_DATA_TYPE_FLOAT:
      n = sprintf(str, "%e", GET_FLOAT_VAL(buf));
      break;

    case TSDB_DATA_TYPE_DOUBLE:
      n = sprintf(str, "%e", GET_DOUBLE_VAL(buf));
      break;

    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_VARBINARY:
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_GEOMETRY:
      if (bufSize < 0) {
        //        tscError("invalid buf size");
        return TSDB_CODE_TSC_INVALID_VALUE;
      }

      *str = '"';
      memcpy(str + 1, buf, bufSize);
      *(str + bufSize + 1) = '"';
      n = bufSize + 2;
      break;

    case TSDB_DATA_TYPE_UTINYINT:
      n = sprintf(str, "%d", *(uint8_t *)buf);
      break;

    case TSDB_DATA_TYPE_USMALLINT:
      n = sprintf(str, "%d", *(uint16_t *)buf);
      break;

    case TSDB_DATA_TYPE_UINT:
      n = sprintf(str, "%u", *(uint32_t *)buf);
      break;

    case TSDB_DATA_TYPE_UBIGINT:
      n = sprintf(str, "%" PRIu64, *(uint64_t *)buf);
      break;

    default:
      //      tscError("unsupported type:%d", type);
      return TSDB_CODE_TSC_INVALID_VALUE;
  }

  *len = n;

  return TSDB_CODE_SUCCESS;
}

void filterDumpInfoToString(SFilterInfo *info, const char *msg, int32_t options) {
  if (qDebugFlag & DEBUG_DEBUG) {
    if (info == NULL) {
      fltDebug("%s - FilterInfo: EMPTY", msg);
      return;
    }

    if (options == 0) {
      qDebug("%s - FilterInfo:", msg);
      qDebug("COLUMN Field Num:%u", info->fields[FLD_TYPE_COLUMN].num);
      for (uint32_t i = 0; i < info->fields[FLD_TYPE_COLUMN].num; ++i) {
        SFilterField *field = &info->fields[FLD_TYPE_COLUMN].fields[i];
        SColumnNode  *refNode = (SColumnNode *)field->desc;
        qDebug("COL%d => [%d][%d]", i, refNode->dataBlockId, refNode->slotId);
      }

      qDebug("VALUE Field Num:%u", info->fields[FLD_TYPE_VALUE].num);
      for (uint32_t i = 0; i < info->fields[FLD_TYPE_VALUE].num; ++i) {
        SFilterField *field = &info->fields[FLD_TYPE_VALUE].fields[i];
        if (field->desc) {
          if (QUERY_NODE_VALUE != nodeType(field->desc)) {
            qDebug("VAL%d => [type:not value node][val:NIL]", i);  // TODO
            continue;
          }

          SValueNode *var = (SValueNode *)field->desc;
          SDataType  *dType = &var->node.resType;
          qDebug("VAL%d => [type:%d][val:%" PRIx64 "]", i, dType->type, var->datum.i);  // TODO
        } else if (field->data) {
          qDebug("VAL%d => [type:NIL][val:NIL]", i);                                    // TODO
        }
      }

      qDebug("UNIT  Num:%u", info->unitNum);
      for (uint32_t i = 0; i < info->unitNum; ++i) {
        SFilterUnit *unit = &info->units[i];
        int32_t      type = FILTER_UNIT_DATA_TYPE(unit);
        int32_t      len = 0;
        int32_t      tlen = 0;
        char         str[512] = {0};

        SFilterField *left = FILTER_UNIT_LEFT_FIELD(info, unit);
        SColumnNode  *refNode = (SColumnNode *)left->desc;
        if (unit->compare.optr <= OP_TYPE_JSON_CONTAINS) {
          len = sprintf(str, "UNIT[%d] => [%d][%d]  %s  [", i, refNode->dataBlockId, refNode->slotId,
                        operatorTypeStr(unit->compare.optr));
        }

        if (unit->right.type == FLD_TYPE_VALUE && FILTER_UNIT_OPTR(unit) != OP_TYPE_IN) {
          SFilterField *right = FILTER_UNIT_RIGHT_FIELD(info, unit);
          char         *data = right->data;
          if (IS_VAR_DATA_TYPE(type)) {
            tlen = varDataLen(data);
            data += VARSTR_HEADER_SIZE;
          }
          if (data) fltConverToStr(str + len, type, data, tlen > 32 ? 32 : tlen, &tlen);
        } else {
          strcat(str, "NULL");
        }
        strcat(str, "]");

        if (unit->compare.optr2) {
          strcat(str, " && ");
          if (unit->compare.optr2 <= OP_TYPE_JSON_CONTAINS) {
            sprintf(str + strlen(str), "[%d][%d]  %s  [", refNode->dataBlockId, refNode->slotId,
                    operatorTypeStr(unit->compare.optr2));
          }

          if (unit->right2.type == FLD_TYPE_VALUE && FILTER_UNIT_OPTR(unit) != OP_TYPE_IN) {
            SFilterField *right = FILTER_UNIT_RIGHT2_FIELD(info, unit);
            char         *data = right->data;
            if (IS_VAR_DATA_TYPE(type)) {
              tlen = varDataLen(data);
              data += VARSTR_HEADER_SIZE;
            }
            fltConverToStr(str + strlen(str), type, data, tlen > 32 ? 32 : tlen, &tlen);
          } else {
            strcat(str, "NULL");
          }
          strcat(str, "]");
        }

        qDebug("%s", str);  // TODO
      }

      qDebug("GROUP Num:%u", info->groupNum);
      uint32_t maxDbgGrpNum = TMIN(info->groupNum, 1000);
      for (uint32_t i = 0; i < maxDbgGrpNum; ++i) {
        SFilterGroup *group = &info->groups[i];
        qDebug("Group%d : unit num[%u]", i, group->unitNum);

        for (uint32_t u = 0; u < group->unitNum; ++u) {
          qDebug("unit id:%u", group->unitIdxs[u]);
        }
      }

      return;
    }

    if (options == 1) {
      qDebug("%s - RANGE info:", msg);

      qDebug("RANGE Num:%u", info->colRangeNum);
      for (uint32_t i = 0; i < info->colRangeNum; ++i) {
        SFilterRangeCtx *ctx = info->colRange[i];
        qDebug("Column ID[%d] RANGE: isnull[%d],notnull[%d],range[%d]", ctx->colId, ctx->isnull, ctx->notnull,
               ctx->isrange);
        if (ctx->isrange) {
          SFilterRangeNode *r = ctx->rs;
          int32_t           tlen = 0;
          while (r) {
            char str[256] = {0};
            if (FILTER_GET_FLAG(r->ra.sflag, RANGE_FLG_NULL)) {
              strcat(str, "(NULL)");
            } else {
              FILTER_GET_FLAG(r->ra.sflag, RANGE_FLG_EXCLUDE) ? strcat(str, "(") : strcat(str, "[");
              fltConverToStr(str + strlen(str), ctx->type, &r->ra.s, tlen > 32 ? 32 : tlen, &tlen);
              FILTER_GET_FLAG(r->ra.sflag, RANGE_FLG_EXCLUDE) ? strcat(str, ")") : strcat(str, "]");
            }
            strcat(str, " - ");
            if (FILTER_GET_FLAG(r->ra.eflag, RANGE_FLG_NULL)) {
              strcat(str, "(NULL)");
            } else {
              FILTER_GET_FLAG(r->ra.eflag, RANGE_FLG_EXCLUDE) ? strcat(str, "(") : strcat(str, "[");
              fltConverToStr(str + strlen(str), ctx->type, &r->ra.e, tlen > 32 ? 32 : tlen, &tlen);
              FILTER_GET_FLAG(r->ra.eflag, RANGE_FLG_EXCLUDE) ? strcat(str, ")") : strcat(str, "]");
            }
            qDebug("range: %s", str);

            r = r->next;
          }
        }
      }

      return;
    }

    qDebug("%s - Block Filter info:", msg);

    if (FILTER_GET_FLAG(info->blkFlag, FI_STATUS_BLK_ALL)) {
      qDebug("Flag:%s", "ALL");
      return;
    } else if (FILTER_GET_FLAG(info->blkFlag, FI_STATUS_BLK_EMPTY)) {
      qDebug("Flag:%s", "EMPTY");
      return;
    } else if (FILTER_GET_FLAG(info->blkFlag, FI_STATUS_BLK_ACTIVE)) {
      qDebug("Flag:%s", "ACTIVE");
    }

    qDebug("GroupNum:%d", info->blkGroupNum);
    uint32_t *unitIdx = info->blkUnits;
    for (uint32_t i = 0; i < info->blkGroupNum; ++i) {
      qDebug("Group[%d] UnitNum: %d:", i, *unitIdx);
      uint32_t unitNum = *(unitIdx++);
      for (uint32_t m = 0; m < unitNum; ++m) {
        qDebug("uidx[%d]", *(unitIdx++));
      }
    }
  }
}

void filterFreeColInfo(void *data) {
  SFilterColInfo *info = (SFilterColInfo *)data;

  if (info->info == NULL) {
    return;
  }

  if (info->type == RANGE_TYPE_VAR_HASH) {
    // TODO
  } else if (info->type == RANGE_TYPE_MR_CTX) {
    filterFreeRangeCtx(info->info);
  } else if (info->type == RANGE_TYPE_UNIT) {
    taosArrayDestroy((SArray *)info->info);
  }

  // NO NEED TO FREE UNIT

  info->type = 0;
  info->info = NULL;
}

void filterFreeColCtx(void *data) {
  SFilterColCtx *ctx = (SFilterColCtx *)data;

  if (ctx->ctx) {
    filterFreeRangeCtx(ctx->ctx);
  }
}

void filterFreeGroupCtx(SFilterGroupCtx *gRes) {
  if (gRes == NULL) {
    return;
  }

  taosMemoryFreeClear(gRes->colIdx);

  int16_t i = 0, j = 0;

  while (i < gRes->colNum) {
    if (gRes->colInfo[j].info) {
      filterFreeColInfo(&gRes->colInfo[j]);
      ++i;
    }

    ++j;
  }

  taosMemoryFreeClear(gRes->colInfo);
  taosMemoryFreeClear(gRes);
}

void filterFreeField(SFilterField *field, int32_t type) {
  if (field == NULL) {
    return;
  }

  if (!FILTER_GET_FLAG(field->flag, FLD_DATA_NO_FREE)) {
    if (FILTER_GET_FLAG(field->flag, FLD_DATA_IS_HASH)) {
      taosHashCleanup(field->data);
    } else {
      taosMemoryFreeClear(field->data);
    }
  }
}

void filterFreePCtx(SFilterPCtx *pctx) {
  taosHashCleanup(pctx->valHash);
  taosHashCleanup(pctx->unitHash);
}

void filterFreeInfo(SFilterInfo *info) {
  if (info == NULL) {
    return;
  }

  for (int32_t i = 0; i < taosArrayGetSize(info->sclCtx.fltSclRange); ++i) {
    SFltSclColumnRange *colRange = taosArrayGet(info->sclCtx.fltSclRange, i);
    nodesDestroyNode((SNode *)colRange->colNode);
    taosArrayDestroy(colRange->points);
  }
  taosArrayDestroy(info->sclCtx.fltSclRange);

  taosMemoryFreeClear(info->cunits);
  taosMemoryFreeClear(info->blkUnitRes);
  taosMemoryFreeClear(info->blkUnits);

  for (int32_t i = 0; i < FLD_TYPE_MAX; ++i) {
    for (uint32_t f = 0; f < info->fields[i].num; ++f) {
      filterFreeField(&info->fields[i].fields[f], i);
    }

    taosMemoryFreeClear(info->fields[i].fields);
  }

  for (uint32_t i = 0; i < info->groupNum; ++i) {
    filterFreeGroup(&info->groups[i]);
  }

  taosMemoryFreeClear(info->groups);

  taosMemoryFreeClear(info->units);

  taosMemoryFreeClear(info->unitRes);

  taosMemoryFreeClear(info->unitFlags);

  for (uint32_t i = 0; i < info->colRangeNum; ++i) {
    filterFreeRangeCtx(info->colRange[i]);
  }

  taosMemoryFreeClear(info->colRange);

  filterFreePCtx(&info->pctx);

  if (!FILTER_GET_FLAG(info->status, FI_STATUS_CLONED)) {
    taosMemoryFreeClear(info);
  }
}

int32_t filterHandleValueExtInfo(SFilterUnit *unit, char extInfo) {
  ASSERT(extInfo > 0 || extInfo < 0);

  uint8_t optr = FILTER_UNIT_OPTR(unit);
  switch (optr) {
    case OP_TYPE_GREATER_THAN:
    case OP_TYPE_GREATER_EQUAL:
      unit->compare.optr = (extInfo > 0) ? FILTER_DUMMY_EMPTY_OPTR : OP_TYPE_IS_NOT_NULL;
      break;
    case OP_TYPE_LOWER_THAN:
    case OP_TYPE_LOWER_EQUAL:
      unit->compare.optr = (extInfo > 0) ? OP_TYPE_IS_NOT_NULL : FILTER_DUMMY_EMPTY_OPTR;
      break;
    case OP_TYPE_EQUAL:
      unit->compare.optr = FILTER_DUMMY_EMPTY_OPTR;
      break;
    default:
      fltError("unsupported operator type");
      return TSDB_CODE_APP_ERROR;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t fltInitValFieldData(SFilterInfo *info) {
  for (uint32_t i = 0; i < info->unitNum; ++i) {
    SFilterUnit *unit = &info->units[i];
    if (unit->right.type != FLD_TYPE_VALUE) {
      ASSERT(unit->compare.optr == FILTER_DUMMY_EMPTY_OPTR || scalarGetOperatorParamNum(unit->compare.optr) == 1);
      continue;
    }

    SFilterField *right = FILTER_UNIT_RIGHT_FIELD(info, unit);

    ASSERT(FILTER_GET_FLAG(right->flag, FLD_TYPE_VALUE));

    uint32_t      type = FILTER_UNIT_DATA_TYPE(unit);
    int8_t        precision = FILTER_UNIT_DATA_PRECISION(unit);
    SFilterField *fi = right;

    SValueNode *var = (SValueNode *)fi->desc;
    if (var == NULL) {
      ASSERT(fi->data != NULL);
      continue;
    }

    if (unit->compare.optr == OP_TYPE_IN) {
      FLT_ERR_RET(scalarGenerateSetFromList((void **)&fi->data, fi->desc, type));
      if (fi->data == NULL) {
        fltError("failed to convert in param");
        FLT_ERR_RET(TSDB_CODE_APP_ERROR);
      }

      FILTER_SET_FLAG(fi->flag, FLD_DATA_IS_HASH);

      continue;
    }

    SDataType *dType = &var->node.resType;
    if (dType->type == type) {
      size_t bufBytes = TMAX(dType->bytes, sizeof(int64_t));
      fi->data = taosMemoryCalloc(1, bufBytes);
      assignVal(fi->data, nodesGetValueFromNode(var), dType->bytes, type);
    } else {
      SScalarParam out = {.columnData = taosMemoryCalloc(1, sizeof(SColumnInfoData))};
      out.columnData->info.type = type;
      out.columnData->info.precision = precision;
      if (!IS_VAR_DATA_TYPE(type)) {
        out.columnData->info.bytes = tDataTypes[type].bytes;
      }

      // todo refactor the convert
      int32_t code = sclConvertValueToSclParam(var, &out, NULL);
      if (code != TSDB_CODE_SUCCESS) {
        colDataDestroy(out.columnData);
        taosMemoryFree(out.columnData);
        qError("convert value to type[%d] failed", type);
        return code;
      }

      size_t bufBytes = IS_VAR_DATA_TYPE(type) ? varDataTLen(out.columnData->pData)
                                            : TMAX(out.columnData->info.bytes, sizeof(int64_t));
      fi->data = taosMemoryCalloc(1, bufBytes);

      size_t valBytes = IS_VAR_DATA_TYPE(type) ? varDataTLen(out.columnData->pData) : out.columnData->info.bytes;
      memcpy(fi->data, out.columnData->pData, valBytes);

      colDataDestroy(out.columnData);
      taosMemoryFree(out.columnData);
    }

    // match/nmatch for nchar type need convert from ucs4 to mbs
    if (type == TSDB_DATA_TYPE_NCHAR && (unit->compare.optr == OP_TYPE_MATCH || unit->compare.optr == OP_TYPE_NMATCH)) {
      char    newValData[TSDB_REGEX_STRING_DEFAULT_LEN * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE] = {0};
      int32_t len = taosUcs4ToMbs((TdUcs4 *)varDataVal(fi->data), varDataLen(fi->data), varDataVal(newValData));
      if (len < 0) {
        qError("filterInitValFieldData taosUcs4ToMbs error 1");
        return TSDB_CODE_APP_ERROR;
      }
      varDataSetLen(newValData, len);
      varDataCopy(fi->data, newValData);
    }
  }

  return TSDB_CODE_SUCCESS;
}

bool filterDoCompare(__compar_fn_t func, uint8_t optr, void *left, void *right) {
  int32_t ret = func(left, right);

  switch (optr) {
    case OP_TYPE_EQUAL: {
      return ret == 0;
    }
    case OP_TYPE_NOT_EQUAL: {
      return ret != 0;
    }
    case OP_TYPE_GREATER_EQUAL: {
      return ret >= 0;
    }
    case OP_TYPE_GREATER_THAN: {
      return ret > 0;
    }
    case OP_TYPE_LOWER_EQUAL: {
      return ret <= 0;
    }
    case OP_TYPE_LOWER_THAN: {
      return ret < 0;
    }
    case OP_TYPE_LIKE: {
      return ret == 0;
    }
    case OP_TYPE_NOT_LIKE: {
      return ret == 0;
    }
    case OP_TYPE_MATCH: {
      return ret == 0;
    }
    case OP_TYPE_NMATCH: {
      return ret == 0;
    }
    case OP_TYPE_IN: {
      return ret == 1;
    }
    case OP_TYPE_NOT_IN: {
      return ret == 1;
    }

    default:
      fltError("unsupported operator type");
      return false;
  }

  return true;
}

int32_t filterAddUnitRange(SFilterInfo *info, SFilterUnit *u, SFilterRangeCtx *ctx, int32_t optr) {
  int32_t      type = FILTER_UNIT_DATA_TYPE(u);
  uint8_t      uoptr = FILTER_UNIT_OPTR(u);
  void        *val = FILTER_UNIT_VAL_DATA(info, u);
  SFilterRange ra = {0};
  int64_t      tmp = 0;

  switch (uoptr) {
    case OP_TYPE_GREATER_THAN:
      SIMPLE_COPY_VALUES(&ra.s, val);
      FILTER_SET_FLAG(ra.sflag, RANGE_FLG_EXCLUDE);
      FILTER_SET_FLAG(ra.eflag, RANGE_FLG_NULL);
      break;
    case OP_TYPE_GREATER_EQUAL:
      SIMPLE_COPY_VALUES(&ra.s, val);
      FILTER_SET_FLAG(ra.eflag, RANGE_FLG_NULL);
      break;
    case OP_TYPE_LOWER_THAN:
      SIMPLE_COPY_VALUES(&ra.e, val);
      FILTER_SET_FLAG(ra.eflag, RANGE_FLG_EXCLUDE);
      FILTER_SET_FLAG(ra.sflag, RANGE_FLG_NULL);
      break;
    case OP_TYPE_LOWER_EQUAL:
      SIMPLE_COPY_VALUES(&ra.e, val);
      FILTER_SET_FLAG(ra.sflag, RANGE_FLG_NULL);
      break;
    case OP_TYPE_NOT_EQUAL:
      ASSERT(type == TSDB_DATA_TYPE_BOOL);
      if (GET_INT8_VAL(val)) {
        SIMPLE_COPY_VALUES(&ra.s, &tmp);
        SIMPLE_COPY_VALUES(&ra.e, &tmp);
      } else {
        *(bool *)&tmp = true;
        SIMPLE_COPY_VALUES(&ra.s, &tmp);
        SIMPLE_COPY_VALUES(&ra.e, &tmp);
      }
      break;
    case OP_TYPE_EQUAL:
      SIMPLE_COPY_VALUES(&ra.s, val);
      SIMPLE_COPY_VALUES(&ra.e, val);
      break;
    default:
      fltError("unsupported operator type");
      return TSDB_CODE_APP_ERROR;
  }

  filterAddRange(ctx, &ra, optr);

  return TSDB_CODE_SUCCESS;
}

int32_t filterCompareRangeCtx(SFilterRangeCtx *ctx1, SFilterRangeCtx *ctx2, bool *equal) {
  FLT_CHK_JMP(ctx1->status != ctx2->status);
  FLT_CHK_JMP(ctx1->isnull != ctx2->isnull);
  FLT_CHK_JMP(ctx1->notnull != ctx2->notnull);
  FLT_CHK_JMP(ctx1->isrange != ctx2->isrange);

  SFilterRangeNode *r1 = ctx1->rs;
  SFilterRangeNode *r2 = ctx2->rs;

  while (r1 && r2) {
    FLT_CHK_JMP(r1->ra.sflag != r2->ra.sflag);
    FLT_CHK_JMP(r1->ra.eflag != r2->ra.eflag);
    FLT_CHK_JMP(r1->ra.s != r2->ra.s);
    FLT_CHK_JMP(r1->ra.e != r2->ra.e);

    r1 = r1->next;
    r2 = r2->next;
  }

  FLT_CHK_JMP(r1 != r2);

  *equal = true;

  return TSDB_CODE_SUCCESS;

_return:
  *equal = false;
  return TSDB_CODE_SUCCESS;
}

int32_t filterMergeUnits(SFilterInfo *info, SFilterGroupCtx *gRes, uint32_t colIdx, bool *empty) {
  SArray          *colArray = (SArray *)gRes->colInfo[colIdx].info;
  int32_t          size = (int32_t)taosArrayGetSize(colArray);
  int32_t          type = gRes->colInfo[colIdx].dataType;
  SFilterRangeCtx *ctx = filterInitRangeCtx(type, 0);

  for (uint32_t i = 0; i < size; ++i) {
    SFilterUnit *u = taosArrayGetP(colArray, i);
    uint8_t      optr = FILTER_UNIT_OPTR(u);

    filterAddRangeOptr(ctx, optr, LOGIC_COND_TYPE_AND, empty, NULL);
    FLT_CHK_JMP(*empty);

    if (!FILTER_NO_MERGE_OPTR(optr)) {
      filterAddUnitRange(info, u, ctx, LOGIC_COND_TYPE_AND);
      FLT_CHK_JMP(MR_EMPTY_RES(ctx));
    }
    if (FILTER_UNIT_OPTR(u) == OP_TYPE_EQUAL && !FILTER_NO_MERGE_DATA_TYPE(FILTER_UNIT_DATA_TYPE(u))) {
      gRes->colInfo[colIdx].optr = OP_TYPE_EQUAL;
      SIMPLE_COPY_VALUES(&gRes->colInfo[colIdx].value, FILTER_UNIT_VAL_DATA(info, u));
    }
  }

  taosArrayDestroy(colArray);

  FILTER_PUSH_CTX(gRes->colInfo[colIdx], ctx);

  return TSDB_CODE_SUCCESS;

_return:

  *empty = true;

  filterFreeRangeCtx(ctx);

  return TSDB_CODE_SUCCESS;
}

int32_t filterMergeGroupUnits(SFilterInfo *info, SFilterGroupCtx **gRes, int32_t *gResNum) {
  bool      empty = false;
  uint32_t *colIdx = taosMemoryMalloc(info->fields[FLD_TYPE_COLUMN].num * sizeof(uint32_t));
  uint32_t  colIdxi = 0;
  uint32_t  gResIdx = 0;

  for (uint32_t i = 0; i < info->groupNum; ++i) {
    SFilterGroup *g = info->groups + i;

    gRes[gResIdx] = taosMemoryCalloc(1, sizeof(SFilterGroupCtx));
    gRes[gResIdx]->colInfo = taosMemoryCalloc(info->fields[FLD_TYPE_COLUMN].num, sizeof(SFilterColInfo));
    colIdxi = 0;
    empty = false;

    for (uint32_t j = 0; j < g->unitNum; ++j) {
      SFilterUnit *u = FILTER_GROUP_UNIT(info, g, j);
      uint32_t     cidx = FILTER_UNIT_COL_IDX(u);

      if (gRes[gResIdx]->colInfo[cidx].info == NULL) {
        gRes[gResIdx]->colInfo[cidx].info = (SArray *)taosArrayInit(4, POINTER_BYTES);
        colIdx[colIdxi++] = cidx;
        ++gRes[gResIdx]->colNum;
      } else {
        if (!FILTER_NO_MERGE_DATA_TYPE(FILTER_UNIT_DATA_TYPE(u))) {
          FILTER_SET_FLAG(info->status, FI_STATUS_REWRITE);
        }
      }

      FILTER_PUSH_UNIT(gRes[gResIdx]->colInfo[cidx], u);
    }

    if (colIdxi > 1) {
      taosSort(colIdx, colIdxi, sizeof(uint32_t), getComparFunc(TSDB_DATA_TYPE_USMALLINT, 0));
    }

    for (uint32_t l = 0; l < colIdxi; ++l) {
      int32_t type = gRes[gResIdx]->colInfo[colIdx[l]].dataType;

      if (FILTER_NO_MERGE_DATA_TYPE(type)) {
        continue;
      }

      filterMergeUnits(info, gRes[gResIdx], colIdx[l], &empty);

      if (empty) {
        break;
      }
    }

    if (empty) {
      FILTER_SET_FLAG(info->status, FI_STATUS_REWRITE);
      filterFreeGroupCtx(gRes[gResIdx]);
      gRes[gResIdx] = NULL;

      continue;
    }

    gRes[gResIdx]->colNum = colIdxi;
    FILTER_COPY_IDX(&gRes[gResIdx]->colIdx, colIdx, colIdxi);
    ++gResIdx;
  }

  taosMemoryFreeClear(colIdx);

  *gResNum = gResIdx;

  if (gResIdx == 0) {
    FILTER_SET_FLAG(info->status, FI_STATUS_EMPTY);
  }

  return TSDB_CODE_SUCCESS;
}

bool filterIsSameUnits(SFilterColInfo *pCol1, SFilterColInfo *pCol2) {
  if (pCol1->type != pCol2->type) {
    return false;
  }

  if (RANGE_TYPE_MR_CTX == pCol1->type) {
    SFilterRangeCtx *pCtx1 = (SFilterRangeCtx *)pCol1->info;
    SFilterRangeCtx *pCtx2 = (SFilterRangeCtx *)pCol2->info;

    if ((pCtx1->isnull != pCtx2->isnull) || (pCtx1->notnull != pCtx2->notnull) || (pCtx1->isrange != pCtx2->isrange)) {
      return false;
    }

    SFilterRangeNode *pNode1 = pCtx1->rs;
    SFilterRangeNode *pNode2 = pCtx2->rs;

    while (true) {
      if (NULL == pNode1 && NULL == pNode2) {
        break;
      }

      if (NULL == pNode1 || NULL == pNode2) {
        return false;
      }

      if (pNode1->ra.s != pNode2->ra.s || pNode1->ra.e != pNode2->ra.e || pNode1->ra.sflag != pNode2->ra.sflag ||
          pNode1->ra.eflag != pNode2->ra.eflag) {
        return false;
      }

      pNode1 = pNode1->next;
      pNode2 = pNode2->next;
    }
  }

  return true;
}

void filterCheckColConflict(SFilterGroupCtx *gRes1, SFilterGroupCtx *gRes2, bool *conflict) {
  uint32_t idx1 = 0, idx2 = 0, m = 0, n = 0;
  bool     equal = false;

  for (; m < gRes1->colNum; ++m) {
    idx1 = gRes1->colIdx[m];

    equal = false;

    for (; n < gRes2->colNum; ++n) {
      idx2 = gRes2->colIdx[n];
      if (idx1 < idx2) {
        *conflict = true;
        return;
      }

      if (idx1 > idx2) {
        continue;
      }

      if (FILTER_NO_MERGE_DATA_TYPE(gRes1->colInfo[idx1].dataType)) {
        *conflict = true;
        return;
      }

      if (!filterIsSameUnits(&gRes1->colInfo[idx1], &gRes2->colInfo[idx2])) {
        *conflict = true;
        return;
      }

      // for long in operation
      if (gRes1->colInfo[idx1].optr == OP_TYPE_EQUAL && gRes2->colInfo[idx2].optr == OP_TYPE_EQUAL) {
        SFilterRangeCtx *ctx = gRes1->colInfo[idx1].info;
        if (ctx->pCompareFunc(&gRes1->colInfo[idx1].value, &gRes2->colInfo[idx2].value)) {
          *conflict = true;
          return;
        }
      }

      ++n;
      equal = true;
      break;
    }

    if (!equal) {
      *conflict = true;
      return;
    }
  }

  *conflict = false;
  return;
}

int32_t filterMergeTwoGroupsImpl(SFilterInfo *info, SFilterRangeCtx **ctx, int32_t optr, uint32_t cidx,
                                 SFilterGroupCtx *gRes1, SFilterGroupCtx *gRes2, bool *empty, bool *all) {
  SFilterField *fi = FILTER_GET_COL_FIELD(info, cidx);
  int32_t       type = FILTER_GET_COL_FIELD_TYPE(fi);

  if ((*ctx) == NULL) {
    *ctx = filterInitRangeCtx(type, 0);
  } else {
    filterReuseRangeCtx(*ctx, type, 0);
  }

  ASSERT(gRes2->colInfo[cidx].type == RANGE_TYPE_MR_CTX);
  ASSERT(gRes1->colInfo[cidx].type == RANGE_TYPE_MR_CTX);

  filterCopyRangeCtx(*ctx, gRes2->colInfo[cidx].info);
  filterSourceRangeFromCtx(*ctx, gRes1->colInfo[cidx].info, optr, empty, all);

  return TSDB_CODE_SUCCESS;
}

int32_t filterMergeTwoGroups(SFilterInfo *info, SFilterGroupCtx **gRes1, SFilterGroupCtx **gRes2, bool *all) {
  bool conflict = false;

  filterCheckColConflict(*gRes1, *gRes2, &conflict);
  if (conflict) {
    return TSDB_CODE_SUCCESS;
  }

  FILTER_SET_FLAG(info->status, FI_STATUS_REWRITE);

  uint32_t         idx1 = 0, idx2 = 0, m = 0, n = 0;
  bool             numEqual = (*gRes1)->colNum == (*gRes2)->colNum;
  bool             equal = false;
  uint32_t         equal1 = 0, equal2 = 0, merNum = 0;
  SFilterRangeCtx *ctx = NULL;
  SFilterColCtx    colCtx = {0};
  SArray          *colCtxs = taosArrayInit((*gRes2)->colNum, sizeof(SFilterColCtx));

  for (; m < (*gRes1)->colNum; ++m) {
    idx1 = (*gRes1)->colIdx[m];

    for (; n < (*gRes2)->colNum; ++n) {
      idx2 = (*gRes2)->colIdx[n];

      if (idx1 > idx2) {
        continue;
      }

      ASSERT(idx1 == idx2);

      ++merNum;

      filterMergeTwoGroupsImpl(info, &ctx, LOGIC_COND_TYPE_OR, idx1, *gRes1, *gRes2, NULL, all);

      FLT_CHK_JMP(*all);

      if (numEqual) {
        if ((*gRes1)->colNum == 1) {
          ++equal1;
          colCtx.colIdx = idx1;
          colCtx.ctx = ctx;
          taosArrayPush(colCtxs, &colCtx);
          break;
        } else {
          filterCompareRangeCtx(ctx, (*gRes1)->colInfo[idx1].info, &equal);
          if (equal) {
            ++equal1;
          }

          filterCompareRangeCtx(ctx, (*gRes2)->colInfo[idx2].info, &equal);
          if (equal) {
            ++equal2;
          }

          FLT_CHK_JMP(equal1 != merNum && equal2 != merNum);
          colCtx.colIdx = idx1;
          colCtx.ctx = ctx;
          ctx = NULL;
          taosArrayPush(colCtxs, &colCtx);
        }
      } else {
        filterCompareRangeCtx(ctx, (*gRes1)->colInfo[idx1].info, &equal);
        if (equal) {
          ++equal1;
        }

        FLT_CHK_JMP(equal1 != merNum);
        colCtx.colIdx = idx1;
        colCtx.ctx = ctx;
        ctx = NULL;
        taosArrayPush(colCtxs, &colCtx);
      }

      ++n;
      break;
    }
  }

  ASSERT(merNum > 0);

  SFilterColInfo *colInfo = NULL;
  ASSERT(merNum == equal1 || merNum == equal2);

  filterFreeGroupCtx(*gRes2);
  *gRes2 = NULL;

  ASSERT(colCtxs && taosArrayGetSize(colCtxs) > 0);

  int32_t        ctxSize = (int32_t)taosArrayGetSize(colCtxs);
  SFilterColCtx *pctx = NULL;

  for (int32_t i = 0; i < ctxSize; ++i) {
    pctx = taosArrayGet(colCtxs, i);
    colInfo = &(*gRes1)->colInfo[pctx->colIdx];

    filterFreeColInfo(colInfo);
    FILTER_PUSH_CTX((*gRes1)->colInfo[pctx->colIdx], pctx->ctx);
  }

  taosArrayDestroy(colCtxs);

  return TSDB_CODE_SUCCESS;

_return:

  if (colCtxs) {
    if (taosArrayGetSize(colCtxs) > 0) {
      taosArrayDestroyEx(colCtxs, filterFreeColCtx);
    } else {
      taosArrayDestroy(colCtxs);
    }
  }

  filterFreeRangeCtx(ctx);

  return TSDB_CODE_SUCCESS;
}

int32_t filterMergeGroups(SFilterInfo *info, SFilterGroupCtx **gRes, int32_t *gResNum) {
  if (*gResNum <= 1) {
    return TSDB_CODE_SUCCESS;
  }

  taosSort(gRes, *gResNum, POINTER_BYTES, filterCompareGroupCtx);

  int32_t  pEnd = 0, cStart = 0, cEnd = 0;
  uint32_t pColNum = 0, cColNum = 0;
  int32_t  movedNum = 0;
  bool     all = false;

  cColNum = gRes[0]->colNum;

  for (int32_t i = 1; i <= *gResNum; ++i) {
    if (i < (*gResNum) && gRes[i]->colNum == cColNum) {
      continue;
    }

    cEnd = i - 1;

    movedNum = 0;
    if (pColNum > 0) {
      for (int32_t m = 0; m <= pEnd; ++m) {
        for (int32_t n = cStart; n <= cEnd; ++n) {
          ASSERT(m < n);
          filterMergeTwoGroups(info, &gRes[m], &gRes[n], &all);

          FLT_CHK_JMP(all);

          if (gRes[n] == NULL) {
            if (n < ((*gResNum) - 1)) {
              memmove(&gRes[n], &gRes[n + 1], (*gResNum - n - 1) * POINTER_BYTES);
            }

            --cEnd;
            --(*gResNum);
            ++movedNum;
            --n;
          }
        }
      }
    }

    for (int32_t m = cStart; m < cEnd; ++m) {
      for (int32_t n = m + 1; n <= cEnd; ++n) {
        ASSERT(m < n);
        filterMergeTwoGroups(info, &gRes[m], &gRes[n], &all);

        FLT_CHK_JMP(all);

        if (gRes[n] == NULL) {
          if (n < ((*gResNum) - 1)) {
            memmove(&gRes[n], &gRes[n + 1], (*gResNum - n - 1) * POINTER_BYTES);
          }

          --cEnd;
          --(*gResNum);
          ++movedNum;
          --n;
        }
      }
    }

    pColNum = cColNum;
    pEnd = cEnd;

    i -= movedNum;

    if (i >= (*gResNum)) {
      break;
    }

    cStart = i;
    cColNum = gRes[i]->colNum;
  }

  return TSDB_CODE_SUCCESS;

_return:

  FILTER_SET_FLAG(info->status, FI_STATUS_ALL);

  return TSDB_CODE_SUCCESS;
}

int32_t filterConvertGroupFromArray(SFilterInfo *info, SArray *group) {
  size_t groupSize = taosArrayGetSize(group);

  info->groupNum = (uint32_t)groupSize;

  if (info->groupNum > 0) {
    info->groups = taosMemoryCalloc(info->groupNum, sizeof(*info->groups));
  }

  for (size_t i = 0; i < groupSize; ++i) {
    SFilterGroup *pg = taosArrayGet(group, i);
    pg->unitFlags = taosMemoryCalloc(pg->unitNum, sizeof(*pg->unitFlags));
    info->groups[i] = *pg;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t filterRewrite(SFilterInfo *info, SFilterGroupCtx **gRes, int32_t gResNum) {
  if (!FILTER_GET_FLAG(info->status, FI_STATUS_REWRITE)) {
    qDebug("no need rewrite");
    return TSDB_CODE_SUCCESS;
  }

  SFilterInfo oinfo = *info;

  FILTER_SET_FLAG(oinfo.status, FI_STATUS_CLONED);

  SArray          *group = taosArrayInit(FILTER_DEFAULT_GROUP_SIZE, sizeof(SFilterGroup));
  SFilterGroupCtx *res = NULL;
  SFilterColInfo  *colInfo = NULL;
  int32_t          optr = 0;
  uint32_t         uidx = 0;

  memset(info, 0, sizeof(*info));

  info->colRangeNum = oinfo.colRangeNum;
  info->colRange = oinfo.colRange;
  oinfo.colRangeNum = 0;
  oinfo.colRange = NULL;

  FILTER_SET_FLAG(info->options, FLT_OPTION_NEED_UNIQE);

  filterInitUnitsFields(info);

  for (int32_t i = 0; i < gResNum; ++i) {
    res = gRes[i];

    optr = (res->colNum > 1) ? LOGIC_COND_TYPE_AND : LOGIC_COND_TYPE_OR;

    SFilterGroup ng = {0};

    for (uint32_t m = 0; m < res->colNum; ++m) {
      colInfo = &res->colInfo[res->colIdx[m]];
      if (FILTER_NO_MERGE_DATA_TYPE(colInfo->dataType)) {
        ASSERT(colInfo->type == RANGE_TYPE_UNIT);
        int32_t usize = (int32_t)taosArrayGetSize((SArray *)colInfo->info);

        for (int32_t n = 0; n < usize; ++n) {
          SFilterUnit *u = (SFilterUnit *)taosArrayGetP((SArray *)colInfo->info, n);

          filterAddUnitFromUnit(info, &oinfo, u, &uidx);
          filterAddUnitToGroup(&ng, uidx);
        }

        continue;
      }

      ASSERT(colInfo->type == RANGE_TYPE_MR_CTX);

      filterAddGroupUnitFromCtx(info, &oinfo, colInfo->info, res->colIdx[m], &ng, optr, group);
    }

    if (ng.unitNum > 0) {
      taosArrayPush(group, &ng);
    }
  }

  filterConvertGroupFromArray(info, group);

  taosArrayDestroy(group);

  filterFreeInfo(&oinfo);

  return TSDB_CODE_SUCCESS;
}

int32_t filterGenerateColRange(SFilterInfo *info, SFilterGroupCtx **gRes, int32_t gResNum) {
  uint32_t        *idxs = NULL;
  uint32_t         colNum = 0;
  SFilterGroupCtx *res = NULL;
  uint32_t        *idxNum = taosMemoryCalloc(info->fields[FLD_TYPE_COLUMN].num, sizeof(*idxNum));

  for (int32_t i = 0; i < gResNum; ++i) {
    for (uint32_t m = 0; m < gRes[i]->colNum; ++m) {
      SFilterColInfo *colInfo = &gRes[i]->colInfo[gRes[i]->colIdx[m]];
      if (FILTER_NO_MERGE_DATA_TYPE(colInfo->dataType)) {
        continue;
      }

      ++idxNum[gRes[i]->colIdx[m]];
    }
  }

  for (uint32_t i = 0; i < info->fields[FLD_TYPE_COLUMN].num; ++i) {
    if (idxNum[i] < gResNum) {
      continue;
    }

    ASSERT(idxNum[i] == gResNum);

    if (idxs == NULL) {
      idxs = taosMemoryCalloc(info->fields[FLD_TYPE_COLUMN].num, sizeof(*idxs));
    }

    idxs[colNum++] = i;
  }

  FLT_CHK_JMP(colNum <= 0);

  info->colRangeNum = colNum;
  info->colRange = taosMemoryCalloc(colNum, POINTER_BYTES);

  for (int32_t i = 0; i < gResNum; ++i) {
    res = gRes[i];
    uint32_t n = 0;

    for (uint32_t m = 0; m < info->colRangeNum; ++m) {
      for (; n < res->colNum; ++n) {
        if (res->colIdx[n] < idxs[m]) {
          continue;
        }

        ASSERT(res->colIdx[n] == idxs[m]);

        SFilterColInfo *colInfo = &res->colInfo[res->colIdx[n]];
        if (info->colRange[m] == NULL) {
          info->colRange[m] = filterInitRangeCtx(colInfo->dataType, 0);
          SFilterField *fi = FILTER_GET_COL_FIELD(info, res->colIdx[n]);
          info->colRange[m]->colId = FILTER_GET_COL_FIELD_ID(fi);
        }

        ASSERT(colInfo->type == RANGE_TYPE_MR_CTX);

        bool all = false;
        filterSourceRangeFromCtx(info->colRange[m], colInfo->info, LOGIC_COND_TYPE_OR, NULL, &all);
        if (all) {
          filterFreeRangeCtx(info->colRange[m]);
          info->colRange[m] = NULL;

          if (m < (info->colRangeNum - 1)) {
            memmove(&info->colRange[m], &info->colRange[m + 1], (info->colRangeNum - m - 1) * POINTER_BYTES);
            memmove(&idxs[m], &idxs[m + 1], (info->colRangeNum - m - 1) * sizeof(*idxs));
          }

          --info->colRangeNum;
          --m;

          FLT_CHK_JMP(info->colRangeNum <= 0);
        }

        ++n;
        break;
      }
    }
  }

_return:
  taosMemoryFreeClear(idxNum);
  taosMemoryFreeClear(idxs);

  return TSDB_CODE_SUCCESS;
}

int32_t filterPostProcessRange(SFilterInfo *info) {
  for (uint32_t i = 0; i < info->colRangeNum; ++i) {
    SFilterRangeCtx  *ctx = info->colRange[i];
    SFilterRangeNode *r = ctx->rs;
    while (r) {
      r->rc.func = filterGetRangeCompFunc(r->ra.sflag, r->ra.eflag);
      r = r->next;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t filterGenerateComInfo(SFilterInfo *info) {
  terrno = 0;
  info->cunits = taosMemoryMalloc(info->unitNum * sizeof(*info->cunits));
  info->blkUnitRes = taosMemoryMalloc(sizeof(*info->blkUnitRes) * info->unitNum);
  info->blkUnits = taosMemoryMalloc(sizeof(*info->blkUnits) * (info->unitNum + 1) * info->groupNum);

  for (uint32_t i = 0; i < info->unitNum; ++i) {
    SFilterUnit *unit = &info->units[i];

    info->cunits[i].func = filterGetCompFuncIdx(FILTER_UNIT_DATA_TYPE(unit), unit->compare.optr); // set terrno if err
    info->cunits[i].rfunc = filterGetRangeCompFuncFromOptrs(unit->compare.optr, unit->compare.optr2);
    info->cunits[i].optr = FILTER_UNIT_OPTR(unit);
    info->cunits[i].colData = NULL;
    info->cunits[i].colId = FILTER_UNIT_COL_ID(info, unit);

    if (unit->right.type == FLD_TYPE_VALUE) {
      info->cunits[i].valData = FILTER_UNIT_VAL_DATA(info, unit);
    } else {
      info->cunits[i].valData = NULL;
    }
    if (unit->right2.type == FLD_TYPE_VALUE) {
      info->cunits[i].valData2 = FILTER_GET_VAL_FIELD_DATA(FILTER_GET_FIELD(info, unit->right2));
    } else {
      info->cunits[i].valData2 = info->cunits[i].valData;
    }

    info->cunits[i].dataSize = FILTER_UNIT_COL_SIZE(info, unit);
    info->cunits[i].dataType = FILTER_UNIT_DATA_TYPE(unit);
  }

  return terrno;
}

int32_t filterUpdateComUnits(SFilterInfo *info) {
  for (uint32_t i = 0; i < info->unitNum; ++i) {
    SFilterUnit *unit = &info->units[i];

    SFilterField *col = FILTER_UNIT_LEFT_FIELD(info, unit);
    info->cunits[i].colData = col->data;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t filterRmUnitByRange(SFilterInfo *info, SColumnDataAgg *pDataStatis, int32_t numOfCols, int32_t numOfRows) {
  int32_t rmUnit = 0;

  memset(info->blkUnitRes, 0, sizeof(*info->blkUnitRes) * info->unitNum);

  for (uint32_t k = 0; k < info->unitNum; ++k) {
    int32_t         index = -1;
    SFilterComUnit *cunit = &info->cunits[k];

    if (FILTER_NO_MERGE_DATA_TYPE(cunit->dataType)) {
      continue;
    }

    for (int32_t i = 0; i < numOfCols; ++i) {
      if (pDataStatis[i].colId == cunit->colId) {
        index = i;
        break;
      }
    }

    if (index == -1) {
      continue;
    }

    if (pDataStatis[index].numOfNull <= 0) {
      if (cunit->optr == OP_TYPE_IS_NULL) {
        info->blkUnitRes[k] = -1;
        rmUnit = 1;
        continue;
      }

      if (cunit->optr == OP_TYPE_IS_NOT_NULL) {
        info->blkUnitRes[k] = 1;
        rmUnit = 1;
        continue;
      }
    } else {
      if (pDataStatis[index].numOfNull == numOfRows) {
        if (cunit->optr == OP_TYPE_IS_NULL) {
          info->blkUnitRes[k] = 1;
          rmUnit = 1;
          continue;
        }

        info->blkUnitRes[k] = -1;
        rmUnit = 1;
        continue;
      }
    }

    if (cunit->optr == OP_TYPE_IS_NULL || cunit->optr == OP_TYPE_IS_NOT_NULL || cunit->optr == OP_TYPE_IN ||
        cunit->optr == OP_TYPE_LIKE || cunit->optr == OP_TYPE_MATCH || cunit->optr == OP_TYPE_NOT_EQUAL) {
      continue;
    }

    SColumnDataAgg *pDataBlockst = &pDataStatis[index];
    void           *minVal, *maxVal;
    float           minv = 0;
    float           maxv = 0;

    if (cunit->dataType == TSDB_DATA_TYPE_FLOAT) {
      minv = (float)(*(double *)(&pDataBlockst->min));
      maxv = (float)(*(double *)(&pDataBlockst->max));

      minVal = &minv;
      maxVal = &maxv;
    } else {
      minVal = &pDataBlockst->min;
      maxVal = &pDataBlockst->max;
    }

    bool minRes = false, maxRes = false;

    if (cunit->rfunc >= 0) {
      minRes =
          (*gRangeCompare[cunit->rfunc])(minVal, minVal, cunit->valData, cunit->valData2, gDataCompare[cunit->func]);
      maxRes =
          (*gRangeCompare[cunit->rfunc])(maxVal, maxVal, cunit->valData, cunit->valData2, gDataCompare[cunit->func]);

      if (minRes && maxRes) {
        info->blkUnitRes[k] = 1;
        rmUnit = 1;
      } else if ((!minRes) && (!maxRes)) {
        minRes = filterDoCompare(gDataCompare[cunit->func], OP_TYPE_LOWER_EQUAL, minVal, cunit->valData);
        maxRes = filterDoCompare(gDataCompare[cunit->func], OP_TYPE_GREATER_EQUAL, maxVal, cunit->valData2);

        if (minRes && maxRes) {
          continue;
        }

        info->blkUnitRes[k] = -1;
        rmUnit = 1;
      }
    } else {
      minRes = filterDoCompare(gDataCompare[cunit->func], cunit->optr, minVal, cunit->valData);
      maxRes = filterDoCompare(gDataCompare[cunit->func], cunit->optr, maxVal, cunit->valData);

      if (minRes && maxRes) {
        info->blkUnitRes[k] = 1;
        rmUnit = 1;
      } else if ((!minRes) && (!maxRes)) {
        if (cunit->optr == OP_TYPE_EQUAL) {
          minRes = filterDoCompare(gDataCompare[cunit->func], OP_TYPE_GREATER_THAN, minVal, cunit->valData);
          maxRes = filterDoCompare(gDataCompare[cunit->func], OP_TYPE_LOWER_THAN, maxVal, cunit->valData);
          if (minRes || maxRes) {
            info->blkUnitRes[k] = -1;
            rmUnit = 1;
          }

          continue;
        }

        info->blkUnitRes[k] = -1;
        rmUnit = 1;
      }
    }
  }

  if (rmUnit == 0) {
    fltDebug("NO Block Filter APPLY");
    FLT_RET(TSDB_CODE_SUCCESS);
  }

  info->blkGroupNum = info->groupNum;

  uint32_t *unitNum = info->blkUnits;
  uint32_t *unitIdx = unitNum + 1;
  int32_t   all = 0, empty = 0;

  for (uint32_t g = 0; g < info->groupNum; ++g) {
    SFilterGroup *group = &info->groups[g];
    // first is block unint num for a group, following append unitNum blkUnitIdx for this group
    *unitNum = group->unitNum;
    all = 0;
    empty = 0;

    // save group idx start pointer
    uint32_t *pGroupIdx = unitIdx;
    for (uint32_t u = 0; u < group->unitNum; ++u) {
      uint32_t uidx = group->unitIdxs[u];
      if (info->blkUnitRes[uidx] == 1) {
        // blkUnitRes == 1 is always true, so need not compare every time, delete this unit from group
        --(*unitNum);
        all = 1;
        continue;
      } else if (info->blkUnitRes[uidx] == -1) {
        // blkUnitRes == -1 is alwary false, so in group is alwary false, need delete this group from blkGroupNum
        *unitNum = 0;
        empty = 1;
        break;
      }

      *(unitIdx++) = uidx;
    }

    if (*unitNum == 0) {
      // if unit num is zero, reset unitIdx to start on this group
      unitIdx = pGroupIdx;

      --info->blkGroupNum;
      ASSERT(empty || all);

      if (empty) {
        FILTER_SET_FLAG(info->blkFlag, FI_STATUS_BLK_EMPTY);
      } else {
        FILTER_SET_FLAG(info->blkFlag, FI_STATUS_BLK_ALL);
        goto _return;
      }

      continue;
    }

    unitNum = unitIdx;
    ++unitIdx;
  }

  if (info->blkGroupNum) {
    FILTER_CLR_FLAG(info->blkFlag, FI_STATUS_BLK_EMPTY);
    FILTER_SET_FLAG(info->blkFlag, FI_STATUS_BLK_ACTIVE);
  }

_return:

  filterDumpInfoToString(info, "Block Filter", 2);

  return TSDB_CODE_SUCCESS;
}

bool filterExecuteBasedOnStatisImpl(void *pinfo, int32_t numOfRows, SColumnInfoData *pRes, SColumnDataAgg *statis,
                                    int16_t numOfCols) {
  SFilterInfo *info = (SFilterInfo *)pinfo;
  bool         all = true;
  uint32_t    *unitIdx = NULL;

  int8_t *p = (int8_t *)pRes->pData;

  for (int32_t i = 0; i < numOfRows; ++i) {
    // FILTER_UNIT_CLR_F(info);

    unitIdx = info->blkUnits;

    for (uint32_t g = 0; g < info->blkGroupNum; ++g) {
      uint32_t unitNum = *(unitIdx++);
      for (uint32_t u = 0; u < unitNum; ++u) {
        SFilterComUnit *cunit = &info->cunits[*(unitIdx + u)];
        void           *colData = colDataGetData((SColumnInfoData *)cunit->colData, i);

        // if (FILTER_UNIT_GET_F(info, uidx)) {
        //   p[i] = FILTER_UNIT_GET_R(info, uidx);
        // } else {
        uint8_t optr = cunit->optr;

        if (colDataIsNull((SColumnInfoData *)(cunit->colData), 0, i, NULL)) {
          p[i] = (optr == OP_TYPE_IS_NULL) ? true : false;
        } else {
          if (optr == OP_TYPE_IS_NOT_NULL) {
            p[i] = 1;
          } else if (optr == OP_TYPE_IS_NULL) {
            p[i] = 0;
          } else if (cunit->rfunc >= 0) {
            p[i] = (*gRangeCompare[cunit->rfunc])(colData, colData, cunit->valData, cunit->valData2,
                                                  gDataCompare[cunit->func]);
          } else {
            p[i] = filterDoCompare(gDataCompare[cunit->func], cunit->optr, colData, cunit->valData);
          }

          // FILTER_UNIT_SET_R(info, uidx, p[i]);
          // FILTER_UNIT_SET_F(info, uidx);
        }

        if (p[i] == 0) {
          break;
        }
      }

      if (p[i]) {
        break;
      }

      unitIdx += unitNum;
    }

    if (p[i] == 0) {
      all = false;
    }
  }

  return all;
}

int32_t filterExecuteBasedOnStatis(SFilterInfo *info, int32_t numOfRows, SColumnInfoData *p, SColumnDataAgg *statis,
                                   int16_t numOfCols, bool *all) {
  if (statis && numOfRows >= FILTER_RM_UNIT_MIN_ROWS) {
    info->blkFlag = 0;

    filterRmUnitByRange(info, statis, numOfCols, numOfRows);

    if (info->blkFlag) {
      if (FILTER_GET_FLAG(info->blkFlag, FI_STATUS_BLK_ALL)) {
        *all = true;
        goto _return;
      } else if (FILTER_GET_FLAG(info->blkFlag, FI_STATUS_BLK_EMPTY)) {
        *all = false;
        goto _return;
      }

      ASSERT(info->unitNum > 1);

      *all = filterExecuteBasedOnStatisImpl(info, numOfRows, p, statis, numOfCols);
      goto _return;
    }
  }

  return 1;

_return:
  info->blkFlag = 0;
  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE bool filterExecuteImplAll(void *info, int32_t numOfRows, SColumnInfoData *p, SColumnDataAgg *statis,
                                              int16_t numOfCols, int32_t *numOfQualified) {
  return true;
}

static FORCE_INLINE bool filterExecuteImplEmpty(void *info, int32_t numOfRows, SColumnInfoData *p,
                                                SColumnDataAgg *statis, int16_t numOfCols, int32_t *numOfQualified) {
  return false;
}

static FORCE_INLINE bool filterExecuteImplIsNull(void *pinfo, int32_t numOfRows, SColumnInfoData *pRes,
                                                 SColumnDataAgg *statis, int16_t numOfCols, int32_t *numOfQualified) {
  SFilterInfo *info = (SFilterInfo *)pinfo;
  bool         all = true;

  int8_t *p = (int8_t *)pRes->pData;

  if (filterExecuteBasedOnStatis(info, numOfRows, pRes, statis, numOfCols, &all) == 0) {
    return all;
  }

  for (int32_t i = 0; i < numOfRows; ++i) {
    uint32_t uidx = info->groups[0].unitIdxs[0];

    p[i] = colDataIsNull((SColumnInfoData *)info->cunits[uidx].colData, 0, i, NULL);
    if (p[i] == 0) {
      all = false;
    } else {
      (*numOfQualified) += 1;
    }
  }

  return all;
}

static FORCE_INLINE bool filterExecuteImplNotNull(void *pinfo, int32_t numOfRows, SColumnInfoData *pRes,
                                                  SColumnDataAgg *statis, int16_t numOfCols, int32_t *numOfQualified) {
  SFilterInfo *info = (SFilterInfo *)pinfo;
  bool         all = true;

  if (filterExecuteBasedOnStatis(info, numOfRows, pRes, statis, numOfCols, &all) == 0) {
    return all;
  }

  int8_t *p = (int8_t *)pRes->pData;

  for (int32_t i = 0; i < numOfRows; ++i) {
    uint32_t uidx = info->groups[0].unitIdxs[0];

    p[i] = !colDataIsNull((SColumnInfoData *)info->cunits[uidx].colData, 0, i, NULL);
    if (p[i] == 0) {
      all = false;
    } else {
      (*numOfQualified) += 1;
    }
  }

  return all;
}

bool filterExecuteImplRange(void *pinfo, int32_t numOfRows, SColumnInfoData *pRes, SColumnDataAgg *statis,
                            int16_t numOfCols, int32_t *numOfQualified) {
  SFilterInfo  *info = (SFilterInfo *)pinfo;
  bool          all = true;
  uint16_t      dataSize = info->cunits[0].dataSize;
  rangeCompFunc rfunc = gRangeCompare[info->cunits[0].rfunc];
  void         *valData = info->cunits[0].valData;
  void         *valData2 = info->cunits[0].valData2;
  __compar_fn_t func = gDataCompare[info->cunits[0].func];

  if (filterExecuteBasedOnStatis(info, numOfRows, pRes, statis, numOfCols, &all) == 0) {
    return all;
  }

  int8_t *p = (int8_t *)pRes->pData;

  for (int32_t i = 0; i < numOfRows; ++i) {
    SColumnInfoData *pData = info->cunits[0].colData;

    if (colDataIsNull_s(pData, i)) {
      all = false;
      p[i] = 0;
      continue;
    }

    void *colData = colDataGetData(pData, i);
    p[i] = (*rfunc)(colData, colData, valData, valData2, func);

    if (p[i] == 0) {
      all = false;
    } else {
      (*numOfQualified)++;
    }
  }

  return all;
}

bool filterExecuteImplMisc(void *pinfo, int32_t numOfRows, SColumnInfoData *pRes, SColumnDataAgg *statis,
                           int16_t numOfCols, int32_t *numOfQualified) {
  SFilterInfo *info = (SFilterInfo *)pinfo;
  bool         all = true;

  if (filterExecuteBasedOnStatis(info, numOfRows, pRes, statis, numOfCols, &all) == 0) {
    return all;
  }

  int8_t *p = (int8_t *)pRes->pData;

  for (int32_t i = 0; i < numOfRows; ++i) {
    uint32_t uidx = info->groups[0].unitIdxs[0];
    if (colDataIsNull_s((SColumnInfoData *)info->cunits[uidx].colData, i)) {
      p[i] = 0;
      all = false;
      continue;
    }

    void *colData = colDataGetData((SColumnInfoData *)info->cunits[uidx].colData, i);
    // match/nmatch for nchar type need convert from ucs4 to mbs
    if (info->cunits[uidx].dataType == TSDB_DATA_TYPE_NCHAR &&
        (info->cunits[uidx].optr == OP_TYPE_MATCH || info->cunits[uidx].optr == OP_TYPE_NMATCH)) {
      char   *newColData = taosMemoryCalloc(info->cunits[uidx].dataSize * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE, 1);
      int32_t len = taosUcs4ToMbs((TdUcs4 *)varDataVal(colData), varDataLen(colData), varDataVal(newColData));
      if (len < 0) {
        qError("castConvert1 taosUcs4ToMbs error");
      } else {
        varDataSetLen(newColData, len);
        p[i] = filterDoCompare(gDataCompare[info->cunits[uidx].func], info->cunits[uidx].optr, newColData,
                               info->cunits[uidx].valData);
      }
      taosMemoryFreeClear(newColData);
    } else {
      p[i] = filterDoCompare(gDataCompare[info->cunits[uidx].func], info->cunits[uidx].optr, colData,
                             info->cunits[uidx].valData);
    }

    if (p[i] == 0) {
      all = false;
    } else {
      (*numOfQualified) += 1;
    }
  }

  return all;
}

bool filterExecuteImpl(void *pinfo, int32_t numOfRows, SColumnInfoData *pRes, SColumnDataAgg *statis, int16_t numOfCols,
                       int32_t *numOfQualified) {
  SFilterInfo *info = (SFilterInfo *)pinfo;
  bool         all = true;

  if (filterExecuteBasedOnStatis(info, numOfRows, pRes, statis, numOfCols, &all) == 0) {
    return all;
  }

  int8_t *p = (int8_t *)pRes->pData;

  for (int32_t i = 0; i < numOfRows; ++i) {
    // FILTER_UNIT_CLR_F(info);

    for (uint32_t g = 0; g < info->groupNum; ++g) {
      SFilterGroup *group = &info->groups[g];
      for (uint32_t u = 0; u < group->unitNum; ++u) {
        uint32_t        uidx = group->unitIdxs[u];
        SFilterComUnit *cunit = &info->cunits[uidx];
        void           *colData = NULL;
        bool            isNull = colDataIsNull((SColumnInfoData *)(cunit->colData), 0, i, NULL);
        // if (FILTER_UNIT_GET_F(info, uidx)) {
        //   p[i] = FILTER_UNIT_GET_R(info, uidx);
        // } else {
        uint8_t optr = cunit->optr;

        if (!isNull) {
          colData = colDataGetData((SColumnInfoData *)(cunit->colData), i);
        }

        if (colData == NULL || isNull) {
          p[i] = optr == OP_TYPE_IS_NULL ? true : false;
        } else {
          if (optr == OP_TYPE_IS_NOT_NULL) {
            p[i] = 1;
          } else if (optr == OP_TYPE_IS_NULL) {
            p[i] = 0;
          } else if (cunit->rfunc >= 0) {
            p[i] = (*gRangeCompare[cunit->rfunc])(colData, colData, cunit->valData, cunit->valData2,
                                                  gDataCompare[cunit->func]);
          } else {
            if (cunit->dataType == TSDB_DATA_TYPE_NCHAR &&
                (cunit->optr == OP_TYPE_MATCH || cunit->optr == OP_TYPE_NMATCH)) {
              char   *newColData = taosMemoryCalloc(cunit->dataSize * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE, 1);
              int32_t len = taosUcs4ToMbs((TdUcs4 *)varDataVal(colData), varDataLen(colData), varDataVal(newColData));
              if (len < 0) {
                qError("castConvert1 taosUcs4ToMbs error");
              } else {
                varDataSetLen(newColData, len);
                p[i] = filterDoCompare(gDataCompare[cunit->func], cunit->optr, newColData, cunit->valData);
              }
              taosMemoryFreeClear(newColData);
            } else {
              p[i] = filterDoCompare(gDataCompare[cunit->func], cunit->optr, colData, cunit->valData);
            }
          }

          // FILTER_UNIT_SET_R(info, uidx, p[i]);
          // FILTER_UNIT_SET_F(info, uidx);
        }

        if (p[i] == 0) {
          break;
        }
      }

      if (p[i]) {
        break;
      }
    }

    if (p[i] == 0) {
      all = false;
    } else {
      (*numOfQualified) += 1;
    }
  }

  return all;
}

int32_t filterSetExecFunc(SFilterInfo *info) {
  if (FILTER_ALL_RES(info)) {
    info->func = filterExecuteImplAll;
    return TSDB_CODE_SUCCESS;
  }

  if (FILTER_EMPTY_RES(info)) {
    info->func = filterExecuteImplEmpty;
    return TSDB_CODE_SUCCESS;
  }

  if (info->unitNum > 1) {
    info->func = filterExecuteImpl;
    return TSDB_CODE_SUCCESS;
  }

  if (info->units[0].compare.optr == OP_TYPE_IS_NULL) {
    info->func = filterExecuteImplIsNull;
    return TSDB_CODE_SUCCESS;
  }

  if (info->units[0].compare.optr == OP_TYPE_IS_NOT_NULL) {
    info->func = filterExecuteImplNotNull;
    return TSDB_CODE_SUCCESS;
  }

  if (info->cunits[0].rfunc >= 0) {
    info->func = filterExecuteImplRange;
    return TSDB_CODE_SUCCESS;
  }

  info->func = filterExecuteImplMisc;
  return TSDB_CODE_SUCCESS;
}

int32_t filterPreprocess(SFilterInfo *info) {
  int32_t code = TSDB_CODE_SUCCESS;
  SFilterGroupCtx **gRes = taosMemoryCalloc(info->groupNum, sizeof(SFilterGroupCtx *));
  int32_t           gResNum = 0;

  filterMergeGroupUnits(info, gRes, &gResNum);

  filterMergeGroups(info, gRes, &gResNum);

  if (FILTER_GET_FLAG(info->status, FI_STATUS_ALL)) {
    fltInfo("Final - FilterInfo: [ALL]");
    goto _return;
  }

  if (FILTER_GET_FLAG(info->status, FI_STATUS_EMPTY)) {
    fltInfo("Final - FilterInfo: [EMPTY]");
    goto _return;
  }

  filterGenerateColRange(info, gRes, gResNum);

  filterDumpInfoToString(info, "Final", 1);

  filterPostProcessRange(info);

  filterRewrite(info, gRes, gResNum);

  FLT_ERR_JRET(filterGenerateComInfo(info));

_return:

  filterSetExecFunc(info);

  for (int32_t i = 0; i < gResNum; ++i) {
    filterFreeGroupCtx(gRes[i]);
  }

  taosMemoryFreeClear(gRes);

  return code;
}

int32_t fltSetColFieldDataImpl(SFilterInfo *info, void *param, filer_get_col_from_id fp, bool fromColId) {
  if (FILTER_ALL_RES(info) || FILTER_EMPTY_RES(info)) {
    return TSDB_CODE_SUCCESS;
  }

  for (uint32_t i = 0; i < info->fields[FLD_TYPE_COLUMN].num; ++i) {
    SFilterField *fi = &info->fields[FLD_TYPE_COLUMN].fields[i];

    if (fromColId) {
      (*fp)(param, FILTER_GET_COL_FIELD_ID(fi), &fi->data);
    } else {
      (*fp)(param, FILTER_GET_COL_FIELD_SLOT_ID(fi), &fi->data);
    }
  }

  filterUpdateComUnits(info);

  return TSDB_CODE_SUCCESS;
}

int32_t fltInitFromNode(SNode *tree, SFilterInfo *info, uint32_t options) {
  int32_t code = TSDB_CODE_SUCCESS;

  SArray *group = taosArrayInit(FILTER_DEFAULT_GROUP_SIZE, sizeof(SFilterGroup));

  filterInitUnitsFields(info);

  SFltBuildGroupCtx tctx = {.info = info, .group = group};
  nodesWalkExpr(tree, fltTreeToGroup, (void *)&tctx);
  FLT_ERR_JRET(tctx.code);

  filterConvertGroupFromArray(info, group);
  taosArrayDestroy(group);

  FLT_ERR_JRET(fltInitValFieldData(info));

  if (!FILTER_GET_FLAG(info->options, FLT_OPTION_NO_REWRITE)) {
    filterDumpInfoToString(info, "Before preprocess", 0);

    FLT_ERR_JRET(filterPreprocess(info));

    FLT_CHK_JMP(FILTER_GET_FLAG(info->status, FI_STATUS_ALL));

    if (FILTER_GET_FLAG(info->status, FI_STATUS_EMPTY)) {
      return code;
    }
  }

  info->unitRes = taosMemoryMalloc(info->unitNum * sizeof(*info->unitRes));
  info->unitFlags = taosMemoryMalloc(info->unitNum * sizeof(*info->unitFlags));

  filterDumpInfoToString(info, "Final", 0);
  return code;

_return:
  if (code) {
    qInfo("init from node failed, code:%d", code);
  }
  return code;
}

// compare ranges, null < min < val < max. null=null, min=min, max=max
typedef enum {
  FLT_SCL_DATUM_KIND_NULL,
  FLT_SCL_DATUM_KIND_MIN,
  FLT_SCL_DATUM_KIND_INT64,
  FLT_SCL_DATUM_KIND_UINT64,
  FLT_SCL_DATUM_KIND_FLOAT64,
  FLT_SCL_DATUM_KIND_VARCHAR,
  FLT_SCL_DATUM_KIND_NCHAR,
  FLT_SCL_DATUM_KIND_MAX,
} SFltSclDatumKind;

typedef struct {
  SFltSclDatumKind kind;
  union {
    int64_t  i;      // for int and bool (1 true, 0 false) and ts
    uint64_t u;      // for uint
    double   d;      // for double
    uint8_t *pData;  // for varchar, nchar, len prefixed
  };
  SDataType type;    // TODO: original data type, may not be used?
} SFltSclDatum;

typedef struct {
  SFltSclDatum val;
  bool         excl;
  bool         start;
} SFltSclPoint;

int32_t fltSclCompareWithFloat64(SFltSclDatum *val1, SFltSclDatum *val2) {
  // val2->kind == float64
  switch (val1->kind) {
    case FLT_SCL_DATUM_KIND_UINT64:
      return compareUint64Double(&val1->u, &val2->d);
    case FLT_SCL_DATUM_KIND_INT64:
      return compareInt64Double(&val1->i, &val2->d);
    case FLT_SCL_DATUM_KIND_FLOAT64: {
      return compareDoubleVal(&val1->d, &val2->d);
    }
    // TODO: varchar, nchar
    default:
      qError("not supported comparsion. kind1 %d, kind2 %d", val1->kind, val2->kind);
      return (val1->kind - val2->kind);
  }
}

int32_t fltSclCompareWithInt64(SFltSclDatum *val1, SFltSclDatum *val2) {
  // val2->kind == int64
  switch (val1->kind) {
    case FLT_SCL_DATUM_KIND_UINT64:
      return compareUint64Int64(&val1->u, &val2->i);
    case FLT_SCL_DATUM_KIND_INT64:
      return compareInt64Val(&val1->i, &val2->i);
    case FLT_SCL_DATUM_KIND_FLOAT64: {
      return compareDoubleInt64(&val1->d, &val2->i);
    }
    // TODO: varchar, nchar
    default:
      qError("not supported comparsion. kind1 %d, kind2 %d", val1->kind, val2->kind);
      return (val1->kind - val2->kind);
  }
}

int32_t fltSclCompareWithUInt64(SFltSclDatum *val1, SFltSclDatum *val2) {
  // val2 kind == uint64
  switch (val1->kind) {
    case FLT_SCL_DATUM_KIND_UINT64:
      return compareUint64Val(&val1->u, &val2->u);
    case FLT_SCL_DATUM_KIND_INT64:
      return compareInt64Uint64(&val1->i, &val2->u);
    case FLT_SCL_DATUM_KIND_FLOAT64: {
      return compareDoubleUint64(&val1->d, &val2->u);
    }
    // TODO: varchar, nchar
    default:
      qError("not supported comparsion. kind1 %d, kind2 %d", val1->kind, val2->kind);
      return (val1->kind - val2->kind);
  }
}

int32_t fltSclCompareDatum(SFltSclDatum *val1, SFltSclDatum *val2) {
  if (val2->kind == FLT_SCL_DATUM_KIND_NULL || val2->kind == FLT_SCL_DATUM_KIND_MIN ||
      val2->kind == FLT_SCL_DATUM_KIND_MAX) {
    return (val1->kind < val2->kind) ? -1 : ((val1->kind > val2->kind) ? 1 : 0);
  }

  switch (val2->kind) {
    case FLT_SCL_DATUM_KIND_UINT64: {
      return fltSclCompareWithUInt64(val1, val2);
    }
    case FLT_SCL_DATUM_KIND_INT64: {
      return fltSclCompareWithInt64(val1, val2);
    }
    case FLT_SCL_DATUM_KIND_FLOAT64: {
      return fltSclCompareWithFloat64(val1, val2);
    }
    // TODO: varchar/nchar
    default:
      qError("not supported kind when compare datum. kind2 : %d", val2->kind);
      return 0;
      break;
  }
  return 0;
}

bool fltSclLessPoint(SFltSclPoint *pt1, SFltSclPoint *pt2) {
  // first value compare
  int32_t cmp = fltSclCompareDatum(&pt1->val, &pt2->val);
  if (cmp != 0) {
    return cmp < 0;
  }

  if (pt1->start && pt2->start) {
    return !pt1->excl && pt2->excl;
  } else if (pt1->start) {
    return !pt1->excl && !pt2->excl;
  } else if (pt2->start) {
    return pt1->excl || pt2->excl;
  }
  return pt1->excl && !pt2->excl;
}

int32_t fltSclMergeSort(SArray *pts1, SArray *pts2, SArray *result) {
  size_t len1 = taosArrayGetSize(pts1);
  size_t len2 = taosArrayGetSize(pts2);
  size_t i = 0;
  size_t j = 0;
  while (i < len1 && j < len2) {
    SFltSclPoint *pt1 = taosArrayGet(pts1, i);
    SFltSclPoint *pt2 = taosArrayGet(pts2, j);
    bool          less = fltSclLessPoint(pt1, pt2);
    if (less) {
      taosArrayPush(result, pt1);
      ++i;
    } else {
      taosArrayPush(result, pt2);
      ++j;
    }
  }
  if (i < len1) {
    for (; i < len1; ++i) {
      SFltSclPoint *pt1 = taosArrayGet(pts1, i);
      taosArrayPush(result, pt1);
    }
  }
  if (j < len2) {
    for (; j < len2; ++j) {
      SFltSclPoint *pt2 = taosArrayGet(pts2, j);
      taosArrayPush(result, pt2);
    }
  }
  return 0;
}

int32_t fltSclMerge(SArray *pts1, SArray *pts2, bool isUnion, SArray *merged) {
  size_t len1 = taosArrayGetSize(pts1);
  size_t len2 = taosArrayGetSize(pts2);
  // first merge sort pts1 and pts2
  SArray *all = taosArrayInit(len1 + len2, sizeof(SFltSclPoint));
  fltSclMergeSort(pts1, pts2, all);
  int32_t countRequired = (isUnion) ? 1 : 2;
  int32_t count = 0;
  for (int32_t i = 0; i < taosArrayGetSize(all); ++i) {
    SFltSclPoint *pt = taosArrayGet(all, i);
    if (pt->start) {
      ++count;
      if (count == countRequired) {
        taosArrayPush(merged, pt);
      }
    } else {
      if (count == countRequired) {
        taosArrayPush(merged, pt);
      }
      --count;
    }
  }
  taosArrayDestroy(all);
  return 0;
}

int32_t fltSclIntersect(SArray *pts1, SArray *pts2, SArray *merged) { return fltSclMerge(pts1, pts2, false, merged); }

int32_t fltSclUnion(SArray *pts1, SArray *pts2, SArray *merged) { return fltSclMerge(pts1, pts2, true, merged); }

typedef struct {
  SColumnNode  *colNode;
  SValueNode   *valNode;
  EOperatorType type;
} SFltSclOperator;

SFltSclColumnRange *fltSclGetOrCreateColumnRange(SColumnNode *colNode, SArray *colRangeList) {
  for (int32_t i = 0; i < taosArrayGetSize(colRangeList); ++i) {
    SFltSclColumnRange *colRange = taosArrayGet(colRangeList, i);
    if (nodesEqualNode((SNode *)colRange->colNode, (SNode *)colNode)) {
      return colRange;
    }
  }
  SColumnNode       *pColumnNode = (SColumnNode *)nodesCloneNode((SNode *)colNode);
  SFltSclColumnRange newColRange = {.colNode = pColumnNode, .points = taosArrayInit(4, sizeof(SFltSclPoint))};
  taosArrayPush(colRangeList, &newColRange);
  return taosArrayGetLast(colRangeList);
}

int32_t fltSclBuildDatumFromValueNode(SFltSclDatum *datum, SValueNode *valNode) {
  datum->type = valNode->node.resType;

  if (valNode->isNull) {
    datum->kind = FLT_SCL_DATUM_KIND_NULL;
  } else {
    switch (valNode->node.resType.type) {
      case TSDB_DATA_TYPE_NULL: {
        datum->kind = FLT_SCL_DATUM_KIND_NULL;
        break;
      }
      case TSDB_DATA_TYPE_BOOL: {
        datum->kind = FLT_SCL_DATUM_KIND_INT64;
        datum->i = (valNode->datum.b) ? 0 : 1;
        break;
      }
      case TSDB_DATA_TYPE_TINYINT:
      case TSDB_DATA_TYPE_SMALLINT:
      case TSDB_DATA_TYPE_INT:
      case TSDB_DATA_TYPE_BIGINT:
      case TSDB_DATA_TYPE_TIMESTAMP: {
        datum->kind = FLT_SCL_DATUM_KIND_INT64;
        datum->i = valNode->datum.i;
        break;
      }
      case TSDB_DATA_TYPE_UTINYINT:
      case TSDB_DATA_TYPE_USMALLINT:
      case TSDB_DATA_TYPE_UINT:
      case TSDB_DATA_TYPE_UBIGINT: {
        datum->kind = FLT_SCL_DATUM_KIND_UINT64;
        datum->u = valNode->datum.u;
        break;
      }
      case TSDB_DATA_TYPE_FLOAT:
      case TSDB_DATA_TYPE_DOUBLE: {
        datum->kind = FLT_SCL_DATUM_KIND_FLOAT64;
        datum->d = valNode->datum.d;
        break;
      }
      // TODO:varchar/nchar/json
      default: {
        qError("not supported type %d when build datum from value node", valNode->node.resType.type);
        break;
      }
    }
  }
  return TSDB_CODE_SUCCESS;
}

int32_t fltSclBuildDatumFromBlockSmaValue(SFltSclDatum *datum, uint8_t type, int64_t val) {
  switch (type) {
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP: {
      datum->kind = FLT_SCL_DATUM_KIND_INT64;
      datum->i = val;
      break;
    }
    case TSDB_DATA_TYPE_UTINYINT:
    case TSDB_DATA_TYPE_USMALLINT:
    case TSDB_DATA_TYPE_UINT:
    case TSDB_DATA_TYPE_UBIGINT: {
      datum->kind = FLT_SCL_DATUM_KIND_UINT64;
      datum->u = *(uint64_t *)&val;
      break;
    }
    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE: {
      datum->kind = FLT_SCL_DATUM_KIND_FLOAT64;
      datum->d = *(double *)&val;
      break;
    }
    // TODO:varchar/nchar/json
    default: {
      datum->kind = FLT_SCL_DATUM_KIND_NULL;
      qError("not supported type %d when build datum from block sma value", type);
      break;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t fltSclBuildRangeFromBlockSma(SFltSclColumnRange *colRange, SColumnDataAgg *pAgg, int32_t numOfRows,
                                     SArray *points) {
  if (pAgg->numOfNull == numOfRows) {
    SFltSclDatum datum = {.kind = FLT_SCL_DATUM_KIND_NULL};
    SFltSclPoint startPt = {.start = true, .excl = false, .val = datum};
    SFltSclPoint endPt = {.start = false, .excl = false, .val = datum};
    taosArrayPush(points, &startPt);
    taosArrayPush(points, &endPt);
    return TSDB_CODE_SUCCESS;
  }
  if (pAgg->numOfNull > 0) {
    SFltSclDatum nullDatum = {.kind = FLT_SCL_DATUM_KIND_NULL};
    SFltSclPoint startPt = {.start = true, .excl = false, .val = nullDatum};
    SFltSclPoint endPt = {.start = false, .excl = false, .val = nullDatum};
    taosArrayPush(points, &startPt);
    taosArrayPush(points, &endPt);
  }
  SFltSclDatum min = {0};
  fltSclBuildDatumFromBlockSmaValue(&min, colRange->colNode->node.resType.type, pAgg->min);
  SFltSclPoint minPt = {.excl = false, .start = true, .val = min};
  SFltSclDatum max = {0};
  fltSclBuildDatumFromBlockSmaValue(&max, colRange->colNode->node.resType.type, pAgg->max);
  SFltSclPoint maxPt = {.excl = false, .start = false, .val = max};
  taosArrayPush(points, &minPt);
  taosArrayPush(points, &maxPt);
  return TSDB_CODE_SUCCESS;
}

bool filterRangeExecute(SFilterInfo *info, SColumnDataAgg **pDataStatis, int32_t numOfCols, int32_t numOfRows) {
  if (info->scalarMode) {
    SArray *colRanges = info->sclCtx.fltSclRange;
    for (int32_t i = 0; i < taosArrayGetSize(colRanges); ++i) {
      SFltSclColumnRange *colRange = taosArrayGet(colRanges, i);
      bool                foundCol = false;
      int32_t             j = 0;
      for (; j < numOfCols; ++j) {
        if (pDataStatis[j] != NULL && pDataStatis[j]->colId == colRange->colNode->colId) {
          foundCol = true;
          break;
        }
      }
      if (foundCol) {
        SColumnDataAgg *pAgg = pDataStatis[j];
        SArray         *points = taosArrayInit(2, sizeof(SFltSclPoint));
        fltSclBuildRangeFromBlockSma(colRange, pAgg, numOfRows, points);
        qDebug("column data agg: nulls %d, rows %d, max %" PRId64 " min %" PRId64, pAgg->numOfNull, numOfRows,
               pAgg->max, pAgg->min);

        SArray *merged = taosArrayInit(8, sizeof(SFltSclPoint));
        fltSclIntersect(points, colRange->points, merged);
        bool isIntersect = taosArrayGetSize(merged) != 0;
        qDebug("filter range execute, scalar mode, column range found. colId: %d colName: %s has overlap: %d",
               colRange->colNode->colId, colRange->colNode->colName, isIntersect);

        taosArrayDestroy(merged);
        taosArrayDestroy(points);
        if (!isIntersect) {
          return false;
        }
      }
    }
    return true;
  }

  if (FILTER_EMPTY_RES(info)) {
    return false;
  }

  if (FILTER_ALL_RES(info)) {
    return true;
  }

  bool  ret = true;
  void *minVal, *maxVal;

  for (uint32_t k = 0; k < info->colRangeNum; ++k) {
    int32_t          index = -1;
    SFilterRangeCtx *ctx = info->colRange[k];
    for (int32_t i = 0; i < numOfCols; ++i) {
      if (pDataStatis[i] != NULL && pDataStatis[i]->colId == ctx->colId) {
        index = i;
        break;
      }
    }

    // no statistics data, load the true data block
    if (index == -1) {
      break;
    }

    // not support pre-filter operation on binary/nchar data type
    if (FILTER_NO_MERGE_DATA_TYPE(ctx->type)) {
      break;
    }

    if (pDataStatis[index]->numOfNull <= 0) {
      if (ctx->isnull && !ctx->notnull && !ctx->isrange) {
        ret = false;
        break;
      }
    } else if (pDataStatis[index]->numOfNull > 0) {
      if (pDataStatis[index]->numOfNull == numOfRows) {
        if ((ctx->notnull || ctx->isrange) && (!ctx->isnull)) {
          ret = false;
          break;
        }

        continue;
      } else {
        if (ctx->isnull) {
          continue;
        }
      }
    }

    SColumnDataAgg *pDataBlockst = pDataStatis[index];

    SFilterRangeNode *r = ctx->rs;
    float             minv = 0;
    float             maxv = 0;

    if (ctx->type == TSDB_DATA_TYPE_FLOAT) {
      minv = (float)(*(double *)(&pDataBlockst->min));
      maxv = (float)(*(double *)(&pDataBlockst->max));

      minVal = &minv;
      maxVal = &maxv;
    } else {
      minVal = &pDataBlockst->min;
      maxVal = &pDataBlockst->max;
    }

    while (r) {
      ret = r->rc.func(minVal, maxVal, &r->rc.s, &r->rc.e, ctx->pCompareFunc);
      if (ret) {
        break;
      }
      r = r->next;
    }

    if (!ret) {
      return ret;
    }
  }

  return ret;
}

int32_t filterGetTimeRangeImpl(SFilterInfo *info, STimeWindow *win, bool *isStrict) {
  SFilterRange     ra = {0};
  SFilterRangeCtx *prev = filterInitRangeCtx(TSDB_DATA_TYPE_TIMESTAMP, FLT_OPTION_TIMESTAMP);
  SFilterRangeCtx *tmpc = filterInitRangeCtx(TSDB_DATA_TYPE_TIMESTAMP, FLT_OPTION_TIMESTAMP);
  SFilterRangeCtx *cur = NULL;
  int32_t          num = 0;
  int32_t          optr = 0;
  int32_t          code = 0;
  bool             empty = false, all = false;

  for (uint32_t i = 0; i < info->groupNum; ++i) {
    SFilterGroup *group = &info->groups[i];
    if (group->unitNum > 1) {
      cur = tmpc;
      optr = LOGIC_COND_TYPE_AND;
    } else {
      cur = prev;
      optr = LOGIC_COND_TYPE_OR;
    }

    for (uint32_t u = 0; u < group->unitNum; ++u) {
      uint32_t     uidx = group->unitIdxs[u];
      SFilterUnit *unit = &info->units[uidx];

      uint8_t raOptr = FILTER_UNIT_OPTR(unit);

      filterAddRangeOptr(cur, raOptr, LOGIC_COND_TYPE_AND, &empty, NULL);
      FLT_CHK_JMP(empty);

      if (FILTER_NO_MERGE_OPTR(raOptr)) {
        continue;
      }

      filterAddUnitRange(info, unit, cur, optr);
    }

    if (cur->notnull) {
      prev->notnull = true;
      break;
    }

    if (group->unitNum > 1) {
      filterSourceRangeFromCtx(prev, cur, LOGIC_COND_TYPE_OR, &empty, &all);
      filterResetRangeCtx(cur);
      if (all) {
        break;
      }
    }
  }

  if (prev->notnull) {
    *win = TSWINDOW_INITIALIZER;
  } else {
    filterGetRangeNum(prev, &num);

    FLT_CHK_JMP(num < 1);

    if (num > 1) {
      *isStrict = false;
      qDebug("more than one time range, num:%d", num);
    }

    SFilterRange tra;
    filterGetRangeRes(prev, &tra);
    win->skey = tra.s;
    win->ekey = tra.e;
    if (FILTER_GET_FLAG(tra.sflag, RANGE_FLG_EXCLUDE)) {
      win->skey++;
    }
    if (FILTER_GET_FLAG(tra.eflag, RANGE_FLG_EXCLUDE)) {
      win->ekey--;
    }
  }

  filterFreeRangeCtx(prev);
  filterFreeRangeCtx(tmpc);

  qDebug("qFilter time range:[%" PRId64 "]-[%" PRId64 "]", win->skey, win->ekey);
  return TSDB_CODE_SUCCESS;

_return:

  *win = TSWINDOW_DESC_INITIALIZER;

  filterFreeRangeCtx(prev);
  filterFreeRangeCtx(tmpc);

  qDebug("qFilter time range:[%" PRId64 "]-[%" PRId64 "]", win->skey, win->ekey);

  return code;
}

static int32_t fltSclGetTimeStampDatum(SFltSclPoint *point, SFltSclDatum *d) {
  *d = point->val;
  d->kind = FLT_SCL_DATUM_KIND_INT64;

  if (point->val.kind == FLT_SCL_DATUM_KIND_MAX) {
    getDataMax(point->val.type.type, &(d->i));
  } else if (point->val.kind == FLT_SCL_DATUM_KIND_MIN) {
    getDataMin(point->val.type.type, &(d->i));
  } else if (point->val.kind == FLT_SCL_DATUM_KIND_INT64) {
    if (point->excl) {
      if (point->start) {
        ++d->i;
      } else {
        --d->i;
      }
    }
  } else if (point->val.kind == FLT_SCL_DATUM_KIND_FLOAT64) {
    double v = d->d;
    if (point->excl) {
      if (point->start) {
        d->i = v + 1;
      }  else {
        d->i = v - 1;
      }
    } else {
      d->i = v;
    }
  } else if (point->val.kind == FLT_SCL_DATUM_KIND_UINT64) {
    uint64_t v = d->u;
    if (point->excl) {
      if (point->start) {
        d->i = v + 1;
      }  else {
        d->i = v - 1;
      }
    } else {
      d->i = v;
    }
  } else {
    qError("not supported type %d when get datum from point", d->type.type);
  }
  return TSDB_CODE_SUCCESS;
}

int32_t filterGetTimeRange(SNode *pNode, STimeWindow *win, bool *isStrict) {
  SFilterInfo *info = NULL;
  int32_t      code = 0;

  *isStrict = true;

  FLT_ERR_RET(filterInitFromNode(pNode, &info, FLT_OPTION_NO_REWRITE | FLT_OPTION_TIMESTAMP));

  if (info->scalarMode) {
    SArray *colRanges = info->sclCtx.fltSclRange;
    if (taosArrayGetSize(colRanges) == 1) {
      SFltSclColumnRange *colRange = taosArrayGet(colRanges, 0);
      SArray             *points = colRange->points;
      if (taosArrayGetSize(points) == 2) {
        *win = TSWINDOW_DESC_INITIALIZER;
        SFltSclPoint *startPt = taosArrayGet(points, 0);
        SFltSclPoint *endPt = taosArrayGet(points, 1);
        SFltSclDatum  start;
        SFltSclDatum  end;
        fltSclGetTimeStampDatum(startPt, &start);
        fltSclGetTimeStampDatum(endPt, &end);
        win->skey = start.i;
        win->ekey = end.i;
        *isStrict = true;
        goto _return;
      } else if (taosArrayGetSize(points) == 0) {
        *win = TSWINDOW_DESC_INITIALIZER;
        goto _return;
      }
    }
    *win = TSWINDOW_INITIALIZER;
    *isStrict = false;
    goto _return;
  }

  FLT_ERR_JRET(filterGetTimeRangeImpl(info, win, isStrict));

_return:

  filterFreeInfo(info);

  FLT_RET(code);
}

int32_t filterConverNcharColumns(SFilterInfo *info, int32_t rows, bool *gotNchar) {
  if (FILTER_EMPTY_RES(info) || FILTER_ALL_RES(info)) {
    return TSDB_CODE_SUCCESS;
  }

  for (uint32_t i = 0; i < info->fields[FLD_TYPE_COLUMN].num; ++i) {
    SFilterField *fi = &info->fields[FLD_TYPE_COLUMN].fields[i];
    int32_t       type = FILTER_GET_COL_FIELD_TYPE(fi);
    if (type == TSDB_DATA_TYPE_NCHAR) {
      SFilterField nfi = {0};
      nfi.desc = fi->desc;
      int32_t bytes = FILTER_GET_COL_FIELD_SIZE(fi);
      nfi.data = taosMemoryMalloc(rows * bytes);
      int32_t bufSize = bytes - VARSTR_HEADER_SIZE;
      for (int32_t j = 0; j < rows; ++j) {
        char   *src = FILTER_GET_COL_FIELD_DATA(fi, j);
        char   *dst = FILTER_GET_COL_FIELD_DATA(&nfi, j);
        int32_t len = 0;
        char   *varSrc = varDataVal(src);
        size_t  k = 0, varSrcLen = varDataLen(src);
        while (k < varSrcLen && varSrc[k++] == -1) {
        }
        if (k == varSrcLen) {
          /* NULL */
          varDataLen(dst) = (VarDataLenT)varSrcLen;
          varDataCopy(dst, src);
          continue;
        }
        bool ret = taosMbsToUcs4(varDataVal(src), varDataLen(src), (TdUcs4 *)varDataVal(dst), bufSize, &len);
        if (!ret) {
          qError("filterConverNcharColumns taosMbsToUcs4 error");
          return TSDB_CODE_FAILED;
        }
        varDataLen(dst) = len;
      }

      fi->data = nfi.data;

      *gotNchar = true;
    }
  }

  if (*gotNchar) {
    filterUpdateComUnits(info);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t filterFreeNcharColumns(SFilterInfo *info) {
  for (uint32_t i = 0; i < info->fields[FLD_TYPE_COLUMN].num; ++i) {
    SFilterField *fi = &info->fields[FLD_TYPE_COLUMN].fields[i];
    int32_t       type = FILTER_GET_COL_FIELD_TYPE(fi);
    if (type == TSDB_DATA_TYPE_NCHAR) {
      taosMemoryFreeClear(fi->data);
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t fltAddValueNodeToConverList(SFltTreeStat *stat, SValueNode *pNode) {
  if (NULL == stat->nodeList) {
    stat->nodeList = taosArrayInit(10, POINTER_BYTES);
    if (NULL == stat->nodeList) {
      FLT_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
  }

  if (NULL == taosArrayPush(stat->nodeList, &pNode)) {
    FLT_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  return TSDB_CODE_SUCCESS;
}

EDealRes fltReviseRewriter(SNode **pNode, void *pContext) {
  SFltTreeStat *stat = (SFltTreeStat *)pContext;

  if (QUERY_NODE_LOGIC_CONDITION == nodeType(*pNode)) {
    SLogicConditionNode *node = (SLogicConditionNode *)*pNode;
    SListCell           *cell = node->pParameterList->pHead;
    for (int32_t i = 0; i < node->pParameterList->length; ++i) {
      if (NULL == cell || NULL == cell->pNode) {
        fltError("invalid cell");
        stat->code = TSDB_CODE_QRY_INVALID_INPUT;
        return DEAL_RES_ERROR;
      }

      if ((QUERY_NODE_OPERATOR != nodeType(cell->pNode)) && (QUERY_NODE_LOGIC_CONDITION != nodeType(cell->pNode))) {
        stat->scalarMode = true;
      }

      cell = cell->pNext;
    }

    return DEAL_RES_CONTINUE;
  }

  if (QUERY_NODE_VALUE == nodeType(*pNode)) {
    SValueNode *valueNode = (SValueNode *)*pNode;
    if (valueNode->placeholderNo >= 1) {
      stat->scalarMode = true;
      return DEAL_RES_CONTINUE;
    }

    /*
        if (!FILTER_GET_FLAG(stat->info->options, FLT_OPTION_TIMESTAMP)) {
          return DEAL_RES_CONTINUE;
        }

        if (TSDB_DATA_TYPE_BINARY != valueNode->node.resType.type && TSDB_DATA_TYPE_NCHAR !=
       valueNode->node.resType.type &&
        TSDB_DATA_TYPE_GEOMETRY != valueNode->node.resType.type) { return DEAL_RES_CONTINUE;
        }

        if (stat->precision < 0) {
          int32_t code = fltAddValueNodeToConverList(stat, valueNode);
          if (code) {
            stat->code = code;
            return DEAL_RES_ERROR;
          }

          return DEAL_RES_CONTINUE;
        }

        int32_t code = sclConvertToTsValueNode(stat->precision, valueNode);
        if (code) {
          stat->code = code;
          return DEAL_RES_ERROR;
        }
    */
    return DEAL_RES_CONTINUE;
  }

  if (QUERY_NODE_COLUMN == nodeType(*pNode)) {
    SColumnNode *colNode = (SColumnNode *)*pNode;
    stat->precision = colNode->node.resType.precision;
    return DEAL_RES_CONTINUE;
  }

  if (QUERY_NODE_NODE_LIST == nodeType(*pNode)) {
    SNodeListNode *listNode = (SNodeListNode *)*pNode;
    if (QUERY_NODE_VALUE != nodeType(listNode->pNodeList->pHead->pNode)) {
      stat->scalarMode = true;
      return DEAL_RES_CONTINUE;
    }

    SValueNode *valueNode = (SValueNode *)listNode->pNodeList->pHead->pNode;
    uint8_t     type = valueNode->node.resType.type;
    SNode      *node = NULL;
    FOREACH(node, listNode->pNodeList) {
      if (type != ((SValueNode *)node)->node.resType.type) {
        stat->scalarMode = true;
        return DEAL_RES_CONTINUE;
      }
    }

    return DEAL_RES_CONTINUE;
  }

  if (QUERY_NODE_FUNCTION == nodeType(*pNode)) {
    stat->scalarMode = true;
    return DEAL_RES_CONTINUE;
  }

  if (QUERY_NODE_CASE_WHEN == nodeType(*pNode) || QUERY_NODE_WHEN_THEN == nodeType(*pNode)) {
    stat->scalarMode = true;
    return DEAL_RES_CONTINUE;
  }

  if (QUERY_NODE_OPERATOR == nodeType(*pNode)) {
    SOperatorNode *node = (SOperatorNode *)*pNode;
    if (!FLT_IS_COMPARISON_OPERATOR(node->opType)) {
      stat->scalarMode = true;
      return DEAL_RES_CONTINUE;
    }

    if (node->opType == OP_TYPE_NOT_IN || node->opType == OP_TYPE_NOT_LIKE || node->opType > OP_TYPE_IS_NOT_NULL ||
        node->opType == OP_TYPE_NOT_EQUAL) {
      stat->scalarMode = true;
      return DEAL_RES_CONTINUE;
    }

    if (FILTER_GET_FLAG(stat->info->options, FLT_OPTION_TIMESTAMP) && (node->opType >= OP_TYPE_NOT_EQUAL) &&
        (node->opType != OP_TYPE_IS_NULL && node->opType != OP_TYPE_IS_NOT_NULL)) {
      stat->scalarMode = true;
      return DEAL_RES_CONTINUE;
    }

    if (NULL == node->pRight) {
      if (scalarGetOperatorParamNum(node->opType) > 1) {
        fltError("invalid operator, pRight:%p, nodeType:%d, opType:%d", node->pRight, nodeType(node), node->opType);
        stat->code = TSDB_CODE_APP_ERROR;
        return DEAL_RES_ERROR;
      }

      if (QUERY_NODE_COLUMN != nodeType(node->pLeft)) {
        stat->scalarMode = true;
        return DEAL_RES_CONTINUE;
      }

      if (OP_TYPE_IS_TRUE == node->opType || OP_TYPE_IS_FALSE == node->opType || OP_TYPE_IS_UNKNOWN == node->opType ||
          OP_TYPE_IS_NOT_TRUE == node->opType || OP_TYPE_IS_NOT_FALSE == node->opType ||
          OP_TYPE_IS_NOT_UNKNOWN == node->opType) {
        stat->scalarMode = true;
        return DEAL_RES_CONTINUE;
      }
    } else {
      if ((QUERY_NODE_COLUMN != nodeType(node->pLeft)) && (QUERY_NODE_VALUE != nodeType(node->pLeft))) {
        stat->scalarMode = true;
        return DEAL_RES_CONTINUE;
      }

      if ((QUERY_NODE_COLUMN != nodeType(node->pRight)) && (QUERY_NODE_VALUE != nodeType(node->pRight)) &&
          (QUERY_NODE_NODE_LIST != nodeType(node->pRight))) {
        stat->scalarMode = true;
        return DEAL_RES_CONTINUE;
      }

      if (nodeType(node->pLeft) == nodeType(node->pRight)) {
        stat->scalarMode = true;
        return DEAL_RES_CONTINUE;
      }

      if (OP_TYPE_JSON_CONTAINS == node->opType) {
        stat->scalarMode = true;
        return DEAL_RES_CONTINUE;
      }

      if (QUERY_NODE_COLUMN != nodeType(node->pLeft)) {
        SNode *t = node->pLeft;
        node->pLeft = node->pRight;
        node->pRight = t;
        switch (node->opType) {
          case OP_TYPE_GREATER_THAN:
            node->opType = OP_TYPE_LOWER_THAN;
            break;
          case OP_TYPE_LOWER_THAN:
            node->opType = OP_TYPE_GREATER_THAN;
            break;
          case OP_TYPE_GREATER_EQUAL:
            node->opType = OP_TYPE_LOWER_EQUAL;
            break;
          case OP_TYPE_LOWER_EQUAL:
            node->opType = OP_TYPE_GREATER_EQUAL;
            break;
          default:
            break;
        }
      }

      if (OP_TYPE_IN == node->opType && QUERY_NODE_NODE_LIST != nodeType(node->pRight)) {
        fltError("invalid IN operator node, rightType:%d", nodeType(node->pRight));
        stat->code = TSDB_CODE_APP_ERROR;
        return DEAL_RES_ERROR;
      }

      SColumnNode *refNode = (SColumnNode *)node->pLeft;
      SExprNode   *exprNode = NULL;
      if (OP_TYPE_IN != node->opType) {
        SValueNode  *valueNode = (SValueNode *)node->pRight;
        if (FILTER_GET_FLAG(stat->info->options, FLT_OPTION_TIMESTAMP) &&
            TSDB_DATA_TYPE_UBIGINT == valueNode->node.resType.type && valueNode->datum.u <= INT64_MAX) {
          valueNode->node.resType.type = TSDB_DATA_TYPE_BIGINT;
        }
        exprNode = &valueNode->node;
      } else {
        SNodeListNode *listNode = (SNodeListNode *)node->pRight;
        if (LIST_LENGTH(listNode->pNodeList) > 10) {
          stat->scalarMode = true;
          return DEAL_RES_CONTINUE;
        }
        exprNode = &listNode->node;
      }
      int32_t type = vectorGetConvertType(refNode->node.resType.type, exprNode->resType.type);
      if (0 != type && type != refNode->node.resType.type) {
        stat->scalarMode = true;
        return DEAL_RES_CONTINUE;
      }
    }

    return DEAL_RES_CONTINUE;
  }

  fltError("invalid node type for filter, type:%d", nodeType(*pNode));

  stat->code = TSDB_CODE_QRY_INVALID_INPUT;

  return DEAL_RES_ERROR;
}

int32_t fltReviseNodes(SFilterInfo *pInfo, SNode **pNode, SFltTreeStat *pStat) {
  int32_t code = 0;
  nodesRewriteExprPostOrder(pNode, fltReviseRewriter, (void *)pStat);

  FLT_ERR_JRET(pStat->code);

  /*
    int32_t nodeNum = taosArrayGetSize(pStat->nodeList);
    for (int32_t i = 0; i < nodeNum; ++i) {
      SValueNode *valueNode = *(SValueNode **)taosArrayGet(pStat->nodeList, i);

      FLT_ERR_JRET(sclConvertToTsValueNode(pStat->precision, valueNode));
    }
  */

_return:

  taosArrayDestroy(pStat->nodeList);
  FLT_RET(code);
}

int32_t fltSclBuildRangePoints(SFltSclOperator *oper, SArray *points) {
  switch (oper->type) {
    case OP_TYPE_GREATER_THAN: {
      SFltSclDatum start;
      fltSclBuildDatumFromValueNode(&start, oper->valNode);
      SFltSclPoint startPt = {.start = true, .excl = true, .val = start};
      SFltSclDatum end = {.kind = FLT_SCL_DATUM_KIND_MAX, .type = oper->colNode->node.resType};
      SFltSclPoint endPt = {.start = false, .excl = false, .val = end};
      taosArrayPush(points, &startPt);
      taosArrayPush(points, &endPt);
      break;
    }
    case OP_TYPE_GREATER_EQUAL: {
      SFltSclDatum start;
      fltSclBuildDatumFromValueNode(&start, oper->valNode);
      SFltSclPoint startPt = {.start = true, .excl = false, .val = start};
      SFltSclDatum end = {.kind = FLT_SCL_DATUM_KIND_MAX, .type = oper->colNode->node.resType};
      SFltSclPoint endPt = {.start = false, .excl = false, .val = end};
      taosArrayPush(points, &startPt);
      taosArrayPush(points, &endPt);
      break;
    }
    case OP_TYPE_LOWER_THAN: {
      SFltSclDatum end;
      fltSclBuildDatumFromValueNode(&end, oper->valNode);
      SFltSclPoint endPt = {.start = false, .excl = true, .val = end};
      SFltSclDatum start = {.kind = FLT_SCL_DATUM_KIND_MIN, .type = oper->colNode->node.resType};
      SFltSclPoint startPt = {.start = true, .excl = false, .val = start};
      taosArrayPush(points, &startPt);
      taosArrayPush(points, &endPt);
      break;
    }
    case OP_TYPE_LOWER_EQUAL: {
      SFltSclDatum end;
      fltSclBuildDatumFromValueNode(&end, oper->valNode);
      SFltSclPoint endPt = {.start = false, .excl = false, .val = end};
      SFltSclDatum start = {.kind = FLT_SCL_DATUM_KIND_MIN, .type = oper->colNode->node.resType};
      SFltSclPoint startPt = {.start = true, .excl = false, .val = start};
      taosArrayPush(points, &startPt);
      taosArrayPush(points, &endPt);
      break;
    }
    case OP_TYPE_EQUAL: {
      SFltSclDatum valDatum;
      fltSclBuildDatumFromValueNode(&valDatum, oper->valNode);
      SFltSclPoint startPt = {.start = true, .excl = false, .val = valDatum};
      SFltSclPoint endPt = {.start = false, .excl = false, .val = valDatum};
      taosArrayPush(points, &startPt);
      taosArrayPush(points, &endPt);
      break;
    }
    case OP_TYPE_NOT_EQUAL: {
      SFltSclDatum valDatum;
      fltSclBuildDatumFromValueNode(&valDatum, oper->valNode);
      {
        SFltSclDatum start = {.kind = FLT_SCL_DATUM_KIND_MIN, .type = oper->colNode->node.resType};
        SFltSclPoint startPt = {.start = true, .excl = false, .val = start};
        SFltSclPoint endPt = {.start = false, .excl = true, .val = valDatum};
        taosArrayPush(points, &startPt);
        taosArrayPush(points, &endPt);
      }
      {
        SFltSclPoint startPt = {.start = true, .excl = true, .val = valDatum};
        SFltSclDatum end = {.kind = FLT_SCL_DATUM_KIND_MAX, .type = oper->colNode->node.resType};
        SFltSclPoint endPt = {.start = false, .excl = false, .val = end};
        taosArrayPush(points, &startPt);
        taosArrayPush(points, &endPt);
      }
      break;
    }
    case OP_TYPE_IS_NULL: {
      SFltSclDatum nullDatum = {.kind = FLT_SCL_DATUM_KIND_NULL};
      SFltSclPoint startPt = {.start = true, .excl = false, .val = nullDatum};
      SFltSclPoint endPt = {.start = false, .excl = false, .val = nullDatum};
      taosArrayPush(points, &startPt);
      taosArrayPush(points, &endPt);
      break;
    }
    case OP_TYPE_IS_NOT_NULL: {
      SFltSclDatum minDatum = {.kind = FLT_SCL_DATUM_KIND_MIN, .type = oper->colNode->node.resType};
      SFltSclPoint startPt = {.start = true, .excl = false, .val = minDatum};
      SFltSclDatum maxDatum = {.kind = FLT_SCL_DATUM_KIND_MAX, .type = oper->colNode->node.resType};
      SFltSclPoint endPt = {.start = false, .excl = false, .val = maxDatum};
      taosArrayPush(points, &startPt);
      taosArrayPush(points, &endPt);
      break;
    }
    default: {
      qError("not supported operator type : %d when build range points", oper->type);
      break;
    }
  }
  return TSDB_CODE_SUCCESS;
}

// TODO: process DNF composed of CNF
int32_t fltSclProcessCNF(SArray *sclOpListCNF, SArray *colRangeList) {
  size_t sz = taosArrayGetSize(sclOpListCNF);
  for (int32_t i = 0; i < sz; ++i) {
    SFltSclOperator    *sclOper = taosArrayGet(sclOpListCNF, i);
    SFltSclColumnRange *colRange = fltSclGetOrCreateColumnRange(sclOper->colNode, colRangeList);
    SArray             *points = taosArrayInit(4, sizeof(SFltSclPoint));
    fltSclBuildRangePoints(sclOper, points);
    if (taosArrayGetSize(colRange->points) != 0) {
      SArray *merged = taosArrayInit(4, sizeof(SFltSclPoint));
      int32_t code = fltSclIntersect(colRange->points, points, merged);
      taosArrayDestroy(colRange->points);
      taosArrayDestroy(points);
      colRange->points = merged;
    } else {
      taosArrayDestroy(colRange->points);
      colRange->points = points;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static bool fltSclIsCollectableNode(SNode *pNode) {
  if (nodeType(pNode) != QUERY_NODE_OPERATOR) {
    return false;
  }

  SOperatorNode *pOper = (SOperatorNode *)pNode;
  if (pOper->pLeft == NULL || pOper->pRight == NULL) {
    return false;
  }

  if (!(pOper->opType == OP_TYPE_GREATER_THAN || pOper->opType == OP_TYPE_GREATER_EQUAL ||
        pOper->opType == OP_TYPE_LOWER_THAN || pOper->opType == OP_TYPE_LOWER_EQUAL ||
        pOper->opType == OP_TYPE_NOT_EQUAL || pOper->opType == OP_TYPE_EQUAL)) {
    return false;
  }

  if (!(nodeType(pOper->pLeft) == QUERY_NODE_COLUMN && nodeType(pOper->pRight) == QUERY_NODE_VALUE)) {
    return false;
  }
  return true;
}

static int32_t fltSclCollectOperatorFromNode(SNode *pNode, SArray *sclOpList) {
  if (!fltSclIsCollectableNode(pNode)) {
    return TSDB_CODE_SUCCESS;
  }

  SOperatorNode *pOper = (SOperatorNode *)pNode;

  SValueNode *valNode = (SValueNode *)pOper->pRight;
  if (IS_NUMERIC_TYPE(valNode->node.resType.type) || valNode->node.resType.type == TSDB_DATA_TYPE_TIMESTAMP) {
    SFltSclOperator sclOp = {.colNode = (SColumnNode *)nodesCloneNode(pOper->pLeft),
                             .valNode = (SValueNode *)nodesCloneNode(pOper->pRight),
                             .type = pOper->opType};
    taosArrayPush(sclOpList, &sclOp);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t fltSclCollectOperatorsFromLogicCond(SNode *pNode, SArray *sclOpList) {
  if (nodeType(pNode) != QUERY_NODE_LOGIC_CONDITION) {
    return TSDB_CODE_SUCCESS;
  }
  SLogicConditionNode *pLogicCond = (SLogicConditionNode *)pNode;
  // TODO: support LOGIC_COND_TYPE_OR
  if (pLogicCond->condType != LOGIC_COND_TYPE_AND) {
    return TSDB_CODE_SUCCESS;
  }
  SNode *pExpr = NULL;
  FOREACH(pExpr, pLogicCond->pParameterList) {
    if (!fltSclIsCollectableNode(pExpr)) {
      return TSDB_CODE_SUCCESS;
    }
  }
  FOREACH(pExpr, pLogicCond->pParameterList) { fltSclCollectOperatorFromNode(pExpr, sclOpList); }
  return TSDB_CODE_SUCCESS;
}

static int32_t fltSclCollectOperators(SNode *pNode, SArray *sclOpList) {
  if (nodeType(pNode) == QUERY_NODE_OPERATOR) {
    fltSclCollectOperatorFromNode(pNode, sclOpList);
  } else if (nodeType(pNode) == QUERY_NODE_LOGIC_CONDITION) {
    fltSclCollectOperatorsFromLogicCond(pNode, sclOpList);
  }
  return TSDB_CODE_SUCCESS;
}

int32_t fltOptimizeNodes(SFilterInfo *pInfo, SNode **pNode, SFltTreeStat *pStat) {
  SArray *sclOpList = taosArrayInit(16, sizeof(SFltSclOperator));
  fltSclCollectOperators(*pNode, sclOpList);
  SArray *colRangeList = taosArrayInit(16, sizeof(SFltSclColumnRange));
  fltSclProcessCNF(sclOpList, colRangeList);
  pInfo->sclCtx.fltSclRange = colRangeList;

  for (int32_t i = 0; i < taosArrayGetSize(sclOpList); ++i) {
    SFltSclOperator *sclOp = taosArrayGet(sclOpList, i);
    nodesDestroyNode((SNode *)sclOp->colNode);
    nodesDestroyNode((SNode *)sclOp->valNode);
  }
  taosArrayDestroy(sclOpList);
  return TSDB_CODE_SUCCESS;
}

int32_t fltGetDataFromColId(void *param, int32_t id, void **data) {
  int32_t numOfCols = ((SFilterColumnParam *)param)->numOfCols;
  SArray *pDataBlock = ((SFilterColumnParam *)param)->pDataBlock;

  for (int32_t j = 0; j < numOfCols; ++j) {
    SColumnInfoData *pColInfo = taosArrayGet(pDataBlock, j);
    if (id == pColInfo->info.colId) {
      *data = pColInfo;
      break;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t fltGetDataFromSlotId(void *param, int32_t id, void **data) {
  int32_t numOfCols = ((SFilterColumnParam *)param)->numOfCols;
  SArray *pDataBlock = ((SFilterColumnParam *)param)->pDataBlock;
  if (id < 0 || id >= numOfCols || id >= taosArrayGetSize(pDataBlock)) {
    fltError("invalid slot id, id:%d, numOfCols:%d, arraySize:%d", id, numOfCols,
             (int32_t)taosArrayGetSize(pDataBlock));
    return TSDB_CODE_APP_ERROR;
  }

  SColumnInfoData *pColInfo = taosArrayGet(pDataBlock, id);
  *data = pColInfo;

  return TSDB_CODE_SUCCESS;
}

int32_t filterSetDataFromSlotId(SFilterInfo *info, void *param) {
  if (NULL == info) {
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  return fltSetColFieldDataImpl(info, param, fltGetDataFromSlotId, false);
}

int32_t filterSetDataFromColId(SFilterInfo *info, void *param) {
  return fltSetColFieldDataImpl(info, param, fltGetDataFromColId, true);
}

int32_t filterInitFromNode(SNode *pNode, SFilterInfo **pInfo, uint32_t options) {
  SFilterInfo *info = NULL;
  if (pNode == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = 0;
  if (pNode == NULL || pInfo == NULL) {
    fltError("invalid param");
    FLT_ERR_RET(TSDB_CODE_APP_ERROR);
  }

  if (*pInfo == NULL) {
    *pInfo = taosMemoryCalloc(1, sizeof(SFilterInfo));
    if (NULL == *pInfo) {
      fltError("taosMemoryCalloc %d failed", (int32_t)sizeof(SFilterInfo));
      FLT_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
  }

  info = *pInfo;
  info->options = options;

  SFltTreeStat stat = {0};
  stat.precision = -1;
  stat.info = info;

  FLT_ERR_JRET(fltReviseNodes(info, &pNode, &stat));
  if (tsFilterScalarMode) {
    info->scalarMode = true;
  } else {
    info->scalarMode = stat.scalarMode;
  }
  fltDebug("scalar mode: %d", info->scalarMode);

  if (!info->scalarMode) {
    FLT_ERR_JRET(fltInitFromNode(pNode, info, options));
  } else {
    info->sclCtx.node = pNode;
    FLT_ERR_JRET(fltOptimizeNodes(info, &info->sclCtx.node, &stat));
  }

  return code;

_return:

  filterFreeInfo(*pInfo);
  *pInfo = NULL;
  FLT_RET(code);
}

int32_t filterExecute(SFilterInfo *info, SSDataBlock *pSrc, SColumnInfoData **p, SColumnDataAgg *statis,
                      int16_t numOfCols, int32_t *pResultStatus) {
  if (NULL == info) {
    *pResultStatus = FILTER_RESULT_ALL_QUALIFIED;
    return TSDB_CODE_SUCCESS;
  }

  SScalarParam output = {0};
  SDataType    type = {.type = TSDB_DATA_TYPE_BOOL, .bytes = sizeof(bool)};

  int32_t code = sclCreateColumnInfoData(&type, pSrc->info.rows, &output);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (info->scalarMode) {
    SArray *pList = taosArrayInit(1, POINTER_BYTES);
    taosArrayPush(pList, &pSrc);

    code = scalarCalculate(info->sclCtx.node, pList, &output);
    taosArrayDestroy(pList);

    *p = output.columnData;

    FLT_ERR_RET(code);

    if (output.numOfQualified == output.numOfRows) {
      *pResultStatus = FILTER_RESULT_ALL_QUALIFIED;
    } else if (output.numOfQualified == 0) {
      *pResultStatus = FILTER_RESULT_NONE_QUALIFIED;
    } else {
      *pResultStatus = FILTER_RESULT_PARTIAL_QUALIFIED;
    }
    return TSDB_CODE_SUCCESS;
  }

  ASSERT(false == info->scalarMode);
  *p = output.columnData;
  output.numOfRows = pSrc->info.rows;

  if (*p == NULL) {
    return TSDB_CODE_APP_ERROR;
  }

  bool keepAll = (*info->func)(info, pSrc->info.rows, *p, statis, numOfCols, &output.numOfQualified);

  // todo this should be return during filter procedure
  if (keepAll) {
    *pResultStatus = FILTER_RESULT_ALL_QUALIFIED;
  } else {
    int32_t num = 0;
    for (int32_t i = 0; i < output.numOfRows; ++i) {
      if (((int8_t *)((*p)->pData))[i] == 1) {
        ++num;
      }
    }

    if (num == output.numOfRows) {
      *pResultStatus = FILTER_RESULT_ALL_QUALIFIED;
    } else if (num == 0) {
      *pResultStatus = FILTER_RESULT_NONE_QUALIFIED;
    } else {
      *pResultStatus = FILTER_RESULT_PARTIAL_QUALIFIED;
    }
  }

  return TSDB_CODE_SUCCESS;
}

typedef struct SClassifyConditionCxt {
  bool hasPrimaryKey;
  bool hasTagIndexCol;
  bool hasTagCol;
  bool hasOtherCol;
} SClassifyConditionCxt;

static EDealRes classifyConditionImpl(SNode *pNode, void *pContext) {
  SClassifyConditionCxt *pCxt = (SClassifyConditionCxt *)pContext;
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    SColumnNode *pCol = (SColumnNode *)pNode;
    if (PRIMARYKEY_TIMESTAMP_COL_ID == pCol->colId && TSDB_SYSTEM_TABLE != pCol->tableType) {
      pCxt->hasPrimaryKey = true;
    } else if (pCol->hasIndex) {
      pCxt->hasTagIndexCol = true;
      pCxt->hasTagCol = true;
    } else if (COLUMN_TYPE_TAG == pCol->colType || COLUMN_TYPE_TBNAME == pCol->colType) {
      pCxt->hasTagCol = true;
    } else {
      pCxt->hasOtherCol = true;
    }
  } else if (QUERY_NODE_FUNCTION == nodeType(pNode)) {
    SFunctionNode *pFunc = (SFunctionNode *)pNode;
    if (fmIsPseudoColumnFunc(pFunc->funcId)) {
      if (FUNCTION_TYPE_TBNAME == pFunc->funcType) {
        pCxt->hasTagCol = true;
      } else {
        pCxt->hasOtherCol = true;
      }
    }
  }
  return DEAL_RES_CONTINUE;
}

typedef enum EConditionType {
  COND_TYPE_PRIMARY_KEY = 1,
  COND_TYPE_TAG_INDEX,
  COND_TYPE_TAG,
  COND_TYPE_NORMAL
} EConditionType;

static EConditionType classifyCondition(SNode *pNode) {
  SClassifyConditionCxt cxt = {.hasPrimaryKey = false, .hasTagIndexCol = false, .hasOtherCol = false};
  nodesWalkExpr(pNode, classifyConditionImpl, &cxt);
  return cxt.hasOtherCol ? COND_TYPE_NORMAL
                         : (cxt.hasPrimaryKey && cxt.hasTagCol
                                ? COND_TYPE_NORMAL
                                : (cxt.hasPrimaryKey ? COND_TYPE_PRIMARY_KEY
                                                     : (cxt.hasTagIndexCol ? COND_TYPE_TAG_INDEX : COND_TYPE_TAG)));
}

static bool isCondColumnsFromMultiTable(SNode *pCond) {
  SNodeList *pCondCols = nodesMakeList();
  int32_t    code = nodesCollectColumnsFromNode(pCond, NULL, COLLECT_COL_TYPE_ALL, &pCondCols);
  if (code == TSDB_CODE_SUCCESS) {
    if (LIST_LENGTH(pCondCols) >= 2) {
      SColumnNode *pFirstCol = (SColumnNode *)nodesListGetNode(pCondCols, 0);
      SNode       *pColNode = NULL;
      FOREACH(pColNode, pCondCols) {
        if (strcmp(((SColumnNode *)pColNode)->dbName, pFirstCol->dbName) != 0 ||
            strcmp(((SColumnNode *)pColNode)->tableAlias, pFirstCol->tableAlias) != 0) {
          nodesDestroyList(pCondCols);
          return true;
        }
      }
    }
    nodesDestroyList(pCondCols);
  }
  return false;
}

static int32_t partitionLogicCond(SNode **pCondition, SNode **pPrimaryKeyCond, SNode **pTagIndexCond, SNode **pTagCond,
                                  SNode **pOtherCond) {
  SLogicConditionNode *pLogicCond = (SLogicConditionNode *)(*pCondition);

  int32_t code = TSDB_CODE_SUCCESS;

  SNodeList *pPrimaryKeyConds = NULL;
  SNodeList *pTagIndexConds = NULL;
  SNodeList *pTagConds = NULL;
  SNodeList *pOtherConds = NULL;
  SNode     *pCond = NULL;
  FOREACH(pCond, pLogicCond->pParameterList) {
    if (isCondColumnsFromMultiTable(pCond)) {
      if (NULL != pOtherCond) {
        code = nodesListMakeAppend(&pOtherConds, nodesCloneNode(pCond));
      }
    } else {
      switch (classifyCondition(pCond)) {
        case COND_TYPE_PRIMARY_KEY:
          if (NULL != pPrimaryKeyCond) {
            code = nodesListMakeAppend(&pPrimaryKeyConds, nodesCloneNode(pCond));
          }
          break;
        case COND_TYPE_TAG_INDEX:
          if (NULL != pTagIndexCond) {
            code = nodesListMakeAppend(&pTagIndexConds, nodesCloneNode(pCond));
          }
          if (NULL != pTagCond) {
            code = nodesListMakeAppend(&pTagConds, nodesCloneNode(pCond));
          }
          break;
        case COND_TYPE_TAG:
          if (NULL != pTagCond) {
            code = nodesListMakeAppend(&pTagConds, nodesCloneNode(pCond));
          }
          break;
        case COND_TYPE_NORMAL:
        default:
          if (NULL != pOtherCond) {
            code = nodesListMakeAppend(&pOtherConds, nodesCloneNode(pCond));
          }
          break;
      }
    }
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }

  SNode *pTempPrimaryKeyCond = NULL;
  SNode *pTempTagIndexCond = NULL;
  SNode *pTempTagCond = NULL;
  SNode *pTempOtherCond = NULL;
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesMergeConds(&pTempPrimaryKeyCond, &pPrimaryKeyConds);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesMergeConds(&pTempTagIndexCond, &pTagIndexConds);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesMergeConds(&pTempTagCond, &pTagConds);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = nodesMergeConds(&pTempOtherCond, &pOtherConds);
  }

  if (TSDB_CODE_SUCCESS == code) {
    if (NULL != pPrimaryKeyCond) {
      *pPrimaryKeyCond = pTempPrimaryKeyCond;
    }
    if (NULL != pTagIndexCond) {
      *pTagIndexCond = pTempTagIndexCond;
    }
    if (NULL != pTagCond) {
      *pTagCond = pTempTagCond;
    }
    if (NULL != pOtherCond) {
      *pOtherCond = pTempOtherCond;
    }
    nodesDestroyNode(*pCondition);
    *pCondition = NULL;
  } else {
    nodesDestroyList(pPrimaryKeyConds);
    nodesDestroyList(pTagIndexConds);
    nodesDestroyList(pTagConds);
    nodesDestroyList(pOtherConds);
    nodesDestroyNode(pTempPrimaryKeyCond);
    nodesDestroyNode(pTempTagIndexCond);
    nodesDestroyNode(pTempTagCond);
    nodesDestroyNode(pTempOtherCond);
  }

  return code;
}

int32_t filterPartitionCond(SNode **pCondition, SNode **pPrimaryKeyCond, SNode **pTagIndexCond, SNode **pTagCond,
                            SNode **pOtherCond) {
  if (QUERY_NODE_LOGIC_CONDITION == nodeType(*pCondition) &&
      LOGIC_COND_TYPE_AND == ((SLogicConditionNode *)*pCondition)->condType) {
    return partitionLogicCond(pCondition, pPrimaryKeyCond, pTagIndexCond, pTagCond, pOtherCond);
  }

  bool needOutput = false;
  if (isCondColumnsFromMultiTable(*pCondition)) {
    if (NULL != pOtherCond) {
      *pOtherCond = *pCondition;
      needOutput = true;
    }
  } else {
    switch (classifyCondition(*pCondition)) {
      case COND_TYPE_PRIMARY_KEY:
        if (NULL != pPrimaryKeyCond) {
          *pPrimaryKeyCond = *pCondition;
          needOutput = true;
        }
        break;
      case COND_TYPE_TAG_INDEX:
        if (NULL != pTagIndexCond) {
          *pTagIndexCond = *pCondition;
          needOutput = true;
        }
        if (NULL != pTagCond) {
          SNode *pTempCond = *pCondition;
          if (NULL != pTagIndexCond) {
            pTempCond = nodesCloneNode(*pCondition);
            if (NULL == pTempCond) {
              return TSDB_CODE_OUT_OF_MEMORY;
            }
          }
          *pTagCond = pTempCond;
          needOutput = true;
        }
        break;
      case COND_TYPE_TAG:
        if (NULL != pTagCond) {
          *pTagCond = *pCondition;
          needOutput = true;
        }
        break;
      case COND_TYPE_NORMAL:
      default:
        if (NULL != pOtherCond) {
          *pOtherCond = *pCondition;
          needOutput = true;
        }
        break;
    }
  }
  if (needOutput) {
    *pCondition = NULL;
  }

  return TSDB_CODE_SUCCESS;
}
