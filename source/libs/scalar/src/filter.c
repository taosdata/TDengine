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
#include "os.h"
#include <tlog.h>
#include "thash.h"
//#include "queryLog.h"
#include "filter.h"
#include "filterInt.h"
#include "sclInt.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "ttime.h"

OptrStr gOptrStr[] = {
  {0,                                      "invalid"},
  {OP_TYPE_ADD,                            "+"},
  {OP_TYPE_SUB,                            "-"},
  {OP_TYPE_MULTI,                          "*"},
  {OP_TYPE_DIV,                            "/"},
  {OP_TYPE_MOD,                            "%"},

  // bit operator
  {OP_TYPE_BIT_AND,                        "&"},
  {OP_TYPE_BIT_OR,                         "|"},

  // comparison operator
  {OP_TYPE_GREATER_THAN,                   ">"},
  {OP_TYPE_GREATER_EQUAL,                  ">="},
  {OP_TYPE_LOWER_THAN,                     "<"},
  {OP_TYPE_LOWER_EQUAL,                    "<="},
  {OP_TYPE_EQUAL,                          "=="},
  {OP_TYPE_NOT_EQUAL,                      "!="},
  {OP_TYPE_IN,                             "in"},
  {OP_TYPE_NOT_IN,                         "not in"},
  {OP_TYPE_LIKE,                           "like"},
  {OP_TYPE_NOT_LIKE,                       "not like"},
  {OP_TYPE_MATCH,                          "match"},
  {OP_TYPE_NMATCH,                         "nmatch"},
  {OP_TYPE_IS_NULL,                        "is null"},
  {OP_TYPE_IS_NOT_NULL,                    "not null"},
  {OP_TYPE_IS_TRUE,                        "is true"},
  {OP_TYPE_IS_FALSE,                       "is false"},
  {OP_TYPE_IS_UNKNOWN,                     "is unknown"},
  {OP_TYPE_IS_NOT_TRUE,                    "not true"},
  {OP_TYPE_IS_NOT_FALSE,                   "not false"},
  {OP_TYPE_IS_NOT_UNKNOWN,                 "not unknown"},

  // json operator
  {OP_TYPE_JSON_GET_VALUE,                 "->"},
  {OP_TYPE_JSON_CONTAINS,                  "json contains"}
};

bool filterRangeCompGi (const void *minv, const void *maxv, const void *minr, const void *maxr, __compar_fn_t cfunc) {
  int32_t result = cfunc(maxv, minr);
  return result >= 0;
}
bool filterRangeCompGe (const void *minv, const void *maxv, const void *minr, const void *maxr, __compar_fn_t cfunc) {
  int32_t result = cfunc(maxv, minr);
  return result > 0;
}
bool filterRangeCompLi (const void *minv, const void *maxv, const void *minr, const void *maxr, __compar_fn_t cfunc) {
  int32_t result = cfunc(minv, maxr);
  return result <= 0;
}
bool filterRangeCompLe (const void *minv, const void *maxv, const void *minr, const void *maxr, __compar_fn_t cfunc) {
  int32_t result = cfunc(minv, maxr);
  return result < 0;
}
bool filterRangeCompii (const void *minv, const void *maxv, const void *minr, const void *maxr, __compar_fn_t cfunc) {
  return cfunc(maxv, minr) >= 0 && cfunc(minv, maxr) <= 0;
}
bool filterRangeCompee (const void *minv, const void *maxv, const void *minr, const void *maxr, __compar_fn_t cfunc) {
  return cfunc(maxv, minr) > 0 && cfunc(minv, maxr) < 0;
}
bool filterRangeCompei (const void *minv, const void *maxv, const void *minr, const void *maxr, __compar_fn_t cfunc) {
  return cfunc(maxv, minr) > 0 && cfunc(minv, maxr) <= 0;
}
bool filterRangeCompie (const void *minv, const void *maxv, const void *minr, const void *maxr, __compar_fn_t cfunc) {
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

rangeCompFunc gRangeCompare[] = {filterRangeCompee, filterRangeCompei, filterRangeCompie, filterRangeCompii, filterRangeCompGe,
 filterRangeCompGi, filterRangeCompLe, filterRangeCompLi};


int8_t filterGetRangeCompFuncFromOptrs(uint8_t optr, uint8_t optr2) {
  if (optr2) {
    assert(optr2 == OP_TYPE_LOWER_THAN || optr2 == OP_TYPE_LOWER_EQUAL);  

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

__compar_fn_t gDataCompare[] = {compareInt32Val, compareInt8Val, compareInt16Val, compareInt64Val, compareFloatVal,
  compareDoubleVal, compareLenPrefixedStr, compareStrPatternMatch, compareChkInString, compareWStrPatternMatch, 
  compareLenPrefixedWStr, compareUint8Val, compareUint16Val, compareUint32Val, compareUint64Val,
  setChkInBytes1, setChkInBytes2, setChkInBytes4, setChkInBytes8, compareStrRegexCompMatch, 
  compareStrRegexCompNMatch, setChkNotInBytes1, setChkNotInBytes2, setChkNotInBytes4, setChkNotInBytes8,
  compareChkNotInString, compareStrPatternNotMatch, compareWStrPatternNotMatch, compareJsonContainsKey
};

int8_t filterGetCompFuncIdx(int32_t type, int32_t optr) {
  int8_t comparFn = 0;

  if (optr == OP_TYPE_IN && (type != TSDB_DATA_TYPE_BINARY && type != TSDB_DATA_TYPE_NCHAR)) {
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
      default:
        assert(0);
    }
  }

  if (optr == OP_TYPE_NOT_IN && (type != TSDB_DATA_TYPE_BINARY && type != TSDB_DATA_TYPE_NCHAR)) {
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
      default:
        assert(0);
    }
  }

  if (optr == OP_TYPE_JSON_CONTAINS && type == TSDB_DATA_TYPE_JSON) {
    return 28;
  }


  switch (type) {
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT:   comparFn = 1;   break;
    case TSDB_DATA_TYPE_SMALLINT:  comparFn = 2;  break;
    case TSDB_DATA_TYPE_INT:       comparFn = 0;  break;
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP: comparFn = 3;  break;
    case TSDB_DATA_TYPE_FLOAT:     comparFn = 4;  break;
    case TSDB_DATA_TYPE_DOUBLE:    comparFn = 5; break;
    case TSDB_DATA_TYPE_BINARY: {
      if (optr == OP_TYPE_MATCH) {
        comparFn = 19;
      } else if (optr == OP_TYPE_NMATCH) {
        comparFn = 20;
      } else if (optr == OP_TYPE_LIKE) { /* wildcard query using like operator */
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
        comparFn = 19;
      } else if (optr == OP_TYPE_NMATCH) {
        comparFn = 20;
      } else if (optr == OP_TYPE_LIKE) {
        comparFn = 9;
      } else if (optr == OP_TYPE_LIKE) {
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

    case TSDB_DATA_TYPE_UTINYINT:  comparFn = 11; break;
    case TSDB_DATA_TYPE_USMALLINT: comparFn = 12;break;
    case TSDB_DATA_TYPE_UINT:      comparFn = 13;break;
    case TSDB_DATA_TYPE_UBIGINT:   comparFn = 14;break;

    default:
      comparFn = 0;
      break;
  }
  
  return comparFn;
}

__compar_fn_t filterGetCompFunc(int32_t type, int32_t optr) {
  return gDataCompare[filterGetCompFuncIdx(type, optr)];
}


static FORCE_INLINE int32_t filterCompareGroupCtx(const void *pLeft, const void *pRight) {
  SFilterGroupCtx *left = *((SFilterGroupCtx**)pLeft), *right = *((SFilterGroupCtx**)pRight);
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

static FORCE_INLINE SFilterRangeNode* filterNewRange(SFilterRangeCtx *ctx, SFilterRange* ra) {
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

void* filterInitRangeCtx(int32_t type, int32_t options) {
  if (type > TSDB_DATA_TYPE_UBIGINT || type < TSDB_DATA_TYPE_BOOL || type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_NCHAR) {
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
  if (!FILTER_GET_FLAG(ra->sflag, RANGE_FLG_NULL)) {
    int32_t sr = cur->pCompareFunc(&ra->s, getDataMin(cur->type));
    if (sr == 0) {
      FILTER_SET_FLAG(ra->sflag, RANGE_FLG_NULL);
    }
  }

  if (!FILTER_GET_FLAG(ra->eflag, RANGE_FLG_NULL)) {
    int32_t er = cur->pCompareFunc(&ra->e, getDataMax(cur->type));
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

int32_t filterAddRangeOptr(void* h, uint8_t raOptr, int32_t optr, bool *empty, bool *all) {
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



int32_t filterAddRangeImpl(void* h, SFilterRange* ra, int32_t optr) {
  SFilterRangeCtx *ctx = (SFilterRangeCtx *)h;

  if (ctx->rs == NULL) {
    if ((FILTER_GET_FLAG(ctx->status, MR_ST_START) == 0) 
      || (FILTER_GET_FLAG(ctx->status, MR_ST_ALL) && (optr == LOGIC_COND_TYPE_AND))
      || ((!FILTER_GET_FLAG(ctx->status, MR_ST_ALL)) && (optr == LOGIC_COND_TYPE_OR))) {
      APPEND_RANGE(ctx, ctx->rs, ra);
      FILTER_SET_FLAG(ctx->status, MR_ST_START);
    }

    return TSDB_CODE_SUCCESS;
  }

  SFilterRangeNode *r = ctx->rs;
  SFilterRangeNode *rn = NULL;
  int32_t cr = 0;

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


  //TSDB_RELATION_OR
  
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
          r= r->next;
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

int32_t filterAddRange(void* h, SFilterRange* ra, int32_t optr) {
  SFilterRangeCtx *ctx = (SFilterRangeCtx *)h;
  
  if (FILTER_GET_FLAG(ra->sflag, RANGE_FLG_NULL)) {
    SIMPLE_COPY_VALUES(&ra->s, getDataMin(ctx->type));
    //FILTER_CLR_FLAG(ra->sflag, RA_NULL);
  }

  if (FILTER_GET_FLAG(ra->eflag, RANGE_FLG_NULL)) {
    SIMPLE_COPY_VALUES(&ra->e, getDataMax(ctx->type));
    //FILTER_CLR_FLAG(ra->eflag, RA_NULL);
  }

  return filterAddRangeImpl(h, ra, optr);
}


int32_t filterAddRangeCtx(void *dst, void *src, int32_t optr) {
  SFilterRangeCtx *dctx = (SFilterRangeCtx *)dst;
  SFilterRangeCtx *sctx = (SFilterRangeCtx *)src;

  assert(optr == LOGIC_COND_TYPE_OR);

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



int32_t filterFinishRange(void* h) {
  SFilterRangeCtx *ctx = (SFilterRangeCtx *)h;

  if (FILTER_GET_FLAG(ctx->status, MR_ST_FIN)) {
    return TSDB_CODE_SUCCESS;
  }

  if (FILTER_GET_FLAG(ctx->options, FLT_OPTION_TIMESTAMP)) {
    SFilterRangeNode *r = ctx->rs;
    SFilterRangeNode *rn = NULL;
    
    while (r && r->next) {
      int64_t tmp = 1;
      operateVal(&tmp, &r->ra.e, &tmp, OP_TYPE_ADD, ctx->type);
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

int32_t filterGetRangeNum(void* h, int32_t* num) {
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


int32_t filterGetRangeRes(void* h, SFilterRange *ra) {
  filterFinishRange(h);

  SFilterRangeCtx *ctx = (SFilterRangeCtx *)h;
  uint32_t num = 0;
  SFilterRangeNode* r = ctx->rs;
  
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
    return TSDB_CODE_QRY_APP_ERROR;
  }
  
  return TSDB_CODE_SUCCESS;
}


int32_t filterSourceRangeFromCtx(SFilterRangeCtx *ctx, void *sctx, int32_t optr, bool *empty, bool *all) {
  SFilterRangeCtx *src = (SFilterRangeCtx *)sctx;

  if (src->isnull){
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



int32_t filterFreeRangeCtx(void* h) {
  if (h == NULL) {
    return TSDB_CODE_SUCCESS;
  }
  
  SFilterRangeCtx *ctx = (SFilterRangeCtx *)h;
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


int32_t filterDetachCnfGroup(SFilterGroup *gp1, SFilterGroup *gp2, SArray* group) {
  SFilterGroup gp = {0};

  gp.unitNum = gp1->unitNum + gp2->unitNum;
  gp.unitIdxs = taosMemoryCalloc(gp.unitNum, sizeof(*gp.unitIdxs));
  memcpy(gp.unitIdxs, gp1->unitIdxs, gp1->unitNum * sizeof(*gp.unitIdxs));
  memcpy(gp.unitIdxs + gp1->unitNum, gp2->unitIdxs, gp2->unitNum * sizeof(*gp.unitIdxs));    

  gp.unitFlags = NULL;
  
  taosArrayPush(group, &gp);

  return TSDB_CODE_SUCCESS;
}


int32_t filterDetachCnfGroups(SArray* group, SArray* left, SArray* right) {
  int32_t leftSize = (int32_t)taosArrayGetSize(left);
  int32_t rightSize = (int32_t)taosArrayGetSize(right);

  if (taosArrayGetSize(left) <= 0) {
    if (taosArrayGetSize(right) <= 0) {
      fltError("both groups are empty");
      FLT_ERR_RET(TSDB_CODE_QRY_APP_ERROR);      
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

int32_t filterGetFiledByDesc(SFilterFields* fields, int32_t type, void *v) {
  for (uint32_t i = 0; i < fields->num; ++i) {
    if (nodesEqualNode(fields->fields[i].desc, v)) {
      return i;
    }
  }

  return -1;
}


int32_t filterGetFiledByData(SFilterInfo *info, int32_t type, void *v, int32_t dataLen) {
  if (type == FLD_TYPE_VALUE) {
    if (info->pctx.valHash == false) {
      qError("value hash is empty");
      return -1;
    }

    void *hv = taosHashGet(info->pctx.valHash, v, dataLen);
    if (hv) {
      return *(int32_t *)hv;
    }
  }

  return -1;
}

// In the params, we should use void *data instead of void **data, there is no need to use taosMemoryFreeClear(*data) to set *data = 0
// Besides, fields data value is a pointer, so dataLen should be POINTER_BYTES for better.
int32_t filterAddField(SFilterInfo *info, void *desc, void **data, int32_t type, SFilterFieldId *fid, int32_t dataLen, bool freeIfExists) {
  int32_t idx = -1;
  uint32_t *num;

  num = &info->fields[type].num;

  if (*num > 0) {
    if (type == FLD_TYPE_COLUMN) {
      idx = filterGetFiledByDesc(&info->fields[type], type, desc);
    } else if (data && (*data) && dataLen > 0 && FILTER_GET_FLAG(info->options, FLT_OPTION_NEED_UNIQE)) {
      idx = filterGetFiledByData(info, type, *data, dataLen);
    }
  }
  
  if (idx < 0) {
    idx = *num;
    if (idx >= info->fields[type].size) {
      info->fields[type].size += FILTER_DEFAULT_FIELD_SIZE;
      info->fields[type].fields = taosMemoryRealloc(info->fields[type].fields, info->fields[type].size * sizeof(SFilterField));
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
        info->pctx.valHash = taosHashInit(FILTER_DEFAULT_GROUP_SIZE * FILTER_DEFAULT_VALUE_SIZE, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, false);
      }
      
      taosHashPut(info->pctx.valHash, *data, dataLen, &idx, sizeof(idx));
    }
  } else {
    if (data && freeIfExists) {
      taosMemoryFreeClear(*data);
    }
  }

  fid->type = type;
  fid->idx = idx;
  
  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE int32_t filterAddColFieldFromField(SFilterInfo *info, SFilterField *field, SFilterFieldId *fid) {
  filterAddField(info, field->desc, &field->data, FILTER_GET_TYPE(field->flag), fid, 0, false);

  FILTER_SET_FLAG(field->flag, FLD_DATA_NO_FREE);

  return TSDB_CODE_SUCCESS;
}


int32_t filterAddFieldFromNode(SFilterInfo *info, SNode *node, SFilterFieldId *fid) {
  if (node == NULL) {
    fltError("empty node");
    FLT_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
  }
  
  if (nodeType(node) != QUERY_NODE_COLUMN && nodeType(node) != QUERY_NODE_VALUE && nodeType(node) != QUERY_NODE_NODE_LIST) {
    FLT_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
  }
  
  int32_t type;
  void *v;

  if (nodeType(node) == QUERY_NODE_COLUMN) {
    type = FLD_TYPE_COLUMN;
    v = node;
  } else {
    type = FLD_TYPE_VALUE;
    v = node;
  }

  filterAddField(info, v, NULL, type, fid, 0, true);
  
  return TSDB_CODE_SUCCESS;
}

int32_t filterAddUnit(SFilterInfo *info, uint8_t optr, SFilterFieldId *left, SFilterFieldId *right, uint32_t *uidx) {
  if (FILTER_GET_FLAG(info->options, FLT_OPTION_NEED_UNIQE)) {
    if (info->pctx.unitHash == NULL) {
      info->pctx.unitHash = taosHashInit(FILTER_DEFAULT_GROUP_SIZE * FILTER_DEFAULT_UNIT_SIZE, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, false);
    } else {
      int64_t v = 0;
      FILTER_PACKAGE_UNIT_HASH_KEY(&v, optr, left->idx, right ? right->idx : -1);
      void *hu = taosHashGet(info->pctx.unitHash, &v, sizeof(v));
      if (hu) {
        *uidx = *(uint32_t *)hu;
        return TSDB_CODE_SUCCESS;
      }
    }
  }

  if (info->unitNum >= info->unitSize) {
    uint32_t psize = info->unitSize;
    info->unitSize += FILTER_DEFAULT_UNIT_SIZE;
    info->units = taosMemoryRealloc(info->units, info->unitSize * sizeof(SFilterUnit));
    memset(info->units + psize, 0, sizeof(*info->units) * FILTER_DEFAULT_UNIT_SIZE);
  }

  SFilterUnit *u = &info->units[info->unitNum];
  
  u->compare.optr = optr;
  u->left = *left;
  if (right) {
    u->right = *right;
  }

  if (u->right.type == FLD_TYPE_VALUE) {
    SFilterField *val = FILTER_UNIT_RIGHT_FIELD(info, u);  
    assert(FILTER_GET_FLAG(val->flag, FLD_TYPE_VALUE));
  } else {
    int32_t paramNum = scalarGetOperatorParamNum(optr);
    if (1 != paramNum) {
      fltError("invalid right field in unit, operator:%s, rightType:%d", gOptrStr[optr].str, u->right.type);
      return TSDB_CODE_QRY_APP_ERROR;
    }
  }
  
  SFilterField *col = FILTER_UNIT_LEFT_FIELD(info, u);
  assert(FILTER_GET_FLAG(col->flag, FLD_TYPE_COLUMN));
  
  info->units[info->unitNum].compare.type = FILTER_GET_COL_FIELD_TYPE(col);
  info->units[info->unitNum].compare.precision = FILTER_GET_COL_FIELD_PRECISION(col);

  *uidx = info->unitNum;

  if (FILTER_GET_FLAG(info->options, FLT_OPTION_NEED_UNIQE)) {
    int64_t v = 0;
    FILTER_PACKAGE_UNIT_HASH_KEY(&v, optr, left->idx, right ? right->idx : -1);  
    taosHashPut(info->pctx.unitHash, &v, sizeof(v), uidx, sizeof(*uidx));
  }
  
  ++info->unitNum;
  
  return TSDB_CODE_SUCCESS;
}



int32_t filterAddUnitToGroup(SFilterGroup *group, uint32_t unitIdx) {
  if (group->unitNum >= group->unitSize) {
    group->unitSize += FILTER_DEFAULT_UNIT_SIZE;
    group->unitIdxs = taosMemoryRealloc(group->unitIdxs, group->unitSize * sizeof(*group->unitIdxs));
  }
  
  group->unitIdxs[group->unitNum++] = unitIdx;

  return TSDB_CODE_SUCCESS;
}

int32_t fltAddGroupUnitFromNode(SFilterInfo *info, SNode* tree, SArray *group) {
  SOperatorNode *node = (SOperatorNode *)tree;
  int32_t ret = TSDB_CODE_SUCCESS;
  SFilterFieldId left = {0}, right = {0};
  filterAddFieldFromNode(info, node->pLeft, &left);
  uint8_t type = FILTER_GET_COL_FIELD_TYPE(FILTER_GET_FIELD(info, left));
  int32_t len = 0;
  uint32_t uidx = 0;
  int32_t code = 0;

  if (node->opType == OP_TYPE_IN && (!IS_VAR_DATA_TYPE(type))) {
    SNodeListNode *listNode = (SNodeListNode *)node->pRight;
    SListCell *cell = listNode->pNodeList->pHead;

    SScalarParam out = {.columnData = taosMemoryCalloc(1, sizeof(SColumnInfoData))};
    out.columnData->info.type = type;
    
    for (int32_t i = 0; i < listNode->pNodeList->length; ++i) {
      SValueNode *valueNode = (SValueNode *)cell->pNode;
      code = doConvertDataType(valueNode, &out);
      if (code) {
//        fltError("convert from %d to %d failed", in.type, out.type);
        FLT_ERR_RET(code);
      }
      
      len = tDataTypes[type].bytes;

      filterAddField(info, NULL, (void**) &out.columnData->pData, FLD_TYPE_VALUE, &right, len, true);
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


int32_t filterAddUnitFromUnit(SFilterInfo *dst, SFilterInfo *src, SFilterUnit* u, uint32_t *uidx) {
  SFilterFieldId left, right, *pright = &right;
  int32_t type = FILTER_UNIT_DATA_TYPE(u);
  uint16_t flag = 0;

  filterAddField(dst, FILTER_UNIT_COL_DESC(src, u), NULL, FLD_TYPE_COLUMN, &left, 0, false);
  SFilterField *t = FILTER_UNIT_LEFT_FIELD(src, u);
  
  if (u->right.type == FLD_TYPE_VALUE) {
    void *data = FILTER_UNIT_VAL_DATA(src, u);
    if (IS_VAR_DATA_TYPE(type)) {
      if (FILTER_UNIT_OPTR(u) ==  OP_TYPE_IN) {
        filterAddField(dst, NULL, &data, FLD_TYPE_VALUE, &right, POINTER_BYTES, false); // POINTER_BYTES should be sizeof(SHashObj), but POINTER_BYTES is also right.

        t = FILTER_GET_FIELD(dst, right);
        FILTER_SET_FLAG(t->flag, FLD_DATA_IS_HASH);
      } else {
        filterAddField(dst, NULL, &data, FLD_TYPE_VALUE, &right, varDataTLen(data), false);
      }
    } else {
      filterAddField(dst, NULL, &data, FLD_TYPE_VALUE, &right, tDataTypes[type].bytes, false);
    }

    flag = FLD_DATA_NO_FREE;    
    t = FILTER_UNIT_RIGHT_FIELD(src, u);
    FILTER_SET_FLAG(t->flag, flag);
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

int32_t filterAddGroupUnitFromCtx(SFilterInfo *dst, SFilterInfo *src, SFilterRangeCtx *ctx, uint32_t cidx, SFilterGroup *g, int32_t optr, SArray *res) {
  SFilterFieldId left, right, right2;
  uint32_t uidx = 0;

  SFilterField *col = FILTER_GET_COL_FIELD(src, cidx);

  filterAddColFieldFromField(dst, col, &left);

  int32_t type = FILTER_GET_COL_FIELD_TYPE(FILTER_GET_FIELD(dst, left));

  if (optr == LOGIC_COND_TYPE_AND) {
    if (ctx->isnull) {
      assert(ctx->notnull == false && ctx->isrange == false);
      filterAddUnit(dst, OP_TYPE_IS_NULL, &left, NULL, &uidx);
      filterAddUnitToGroup(g, uidx);
      return TSDB_CODE_SUCCESS;
    }

    if (ctx->notnull) {      
      assert(ctx->isnull == false && ctx->isrange == false);
      filterAddUnit(dst, OP_TYPE_IS_NOT_NULL, &left, NULL, &uidx);
      filterAddUnitToGroup(g, uidx);
      return TSDB_CODE_SUCCESS;
    }

    if (!ctx->isrange) {
      assert(ctx->isnull || ctx->notnull);
      return TSDB_CODE_SUCCESS;
    }

    assert(ctx->rs && ctx->rs->next == NULL);

    SFilterRange *ra = &ctx->rs->ra;
    
    assert(!((FILTER_GET_FLAG(ra->sflag, RANGE_FLG_NULL)) && (FILTER_GET_FLAG(ra->eflag, RANGE_FLG_NULL))));

    if ((!FILTER_GET_FLAG(ra->sflag, RANGE_FLG_NULL)) && (!FILTER_GET_FLAG(ra->eflag, RANGE_FLG_NULL))) {
      __compar_fn_t func = getComparFunc(type, 0);
      if (func(&ra->s, &ra->e) == 0) {
        void *data = taosMemoryMalloc(sizeof(int64_t));
        SIMPLE_COPY_VALUES(data, &ra->s);
        filterAddField(dst, NULL, &data, FLD_TYPE_VALUE, &right, tDataTypes[type].bytes, true);
        filterAddUnit(dst, OP_TYPE_EQUAL, &left, &right, &uidx);
        filterAddUnitToGroup(g, uidx);
        return TSDB_CODE_SUCCESS;              
      } else {
        void *data = taosMemoryMalloc(sizeof(int64_t));
        SIMPLE_COPY_VALUES(data, &ra->s);
        filterAddField(dst, NULL, &data, FLD_TYPE_VALUE, &right, tDataTypes[type].bytes, true);
        void *data2 = taosMemoryMalloc(sizeof(int64_t));
        SIMPLE_COPY_VALUES(data2, &ra->e);
        filterAddField(dst, NULL, &data2, FLD_TYPE_VALUE, &right2, tDataTypes[type].bytes, true);
        
        filterAddUnit(dst, FILTER_GET_FLAG(ra->sflag, RANGE_FLG_EXCLUDE) ? OP_TYPE_GREATER_THAN : OP_TYPE_GREATER_EQUAL, &left, &right, &uidx);
        filterAddUnitRight(dst, FILTER_GET_FLAG(ra->eflag, RANGE_FLG_EXCLUDE) ? OP_TYPE_LOWER_THAN : OP_TYPE_LOWER_EQUAL, &right2, uidx);
        filterAddUnitToGroup(g, uidx);
        return TSDB_CODE_SUCCESS;              
      }
    }
    
    if (!FILTER_GET_FLAG(ra->sflag, RANGE_FLG_NULL)) {
      void *data = taosMemoryMalloc(sizeof(int64_t));
      SIMPLE_COPY_VALUES(data, &ra->s);
      filterAddField(dst, NULL, &data, FLD_TYPE_VALUE, &right, tDataTypes[type].bytes, true);
      filterAddUnit(dst, FILTER_GET_FLAG(ra->sflag, RANGE_FLG_EXCLUDE) ? OP_TYPE_GREATER_THAN : OP_TYPE_GREATER_EQUAL, &left, &right, &uidx);
      filterAddUnitToGroup(g, uidx);
    }

    if (!FILTER_GET_FLAG(ra->eflag, RANGE_FLG_NULL)) {
      void *data = taosMemoryMalloc(sizeof(int64_t));
      SIMPLE_COPY_VALUES(data, &ra->e);
      filterAddField(dst, NULL, &data, FLD_TYPE_VALUE, &right, tDataTypes[type].bytes, true);
      filterAddUnit(dst, FILTER_GET_FLAG(ra->eflag, RANGE_FLG_EXCLUDE) ? OP_TYPE_LOWER_THAN : OP_TYPE_LOWER_EQUAL, &left, &right, &uidx);
      filterAddUnitToGroup(g, uidx);
    }    

    return TSDB_CODE_SUCCESS;      
  } 

  // OR PROCESS
  
  SFilterGroup ng = {0};
  g = &ng;

  assert(ctx->isnull || ctx->notnull || ctx->isrange);
  
  if (ctx->isnull) {
    filterAddUnit(dst, OP_TYPE_IS_NULL, &left, NULL, &uidx);
    filterAddUnitToGroup(g, uidx);    
    taosArrayPush(res, g);
  }
  
  if (ctx->notnull) {
    assert(!ctx->isrange);
    memset(g, 0, sizeof(*g));
    
    filterAddUnit(dst, OP_TYPE_IS_NOT_NULL, &left, NULL, &uidx);
    filterAddUnitToGroup(g, uidx);
    taosArrayPush(res, g);
  }

  if (!ctx->isrange) {
    assert(ctx->isnull || ctx->notnull);
    g->unitNum = 0;
    return TSDB_CODE_SUCCESS;
  }

  SFilterRangeNode *r = ctx->rs;
  
  while (r) {
    memset(g, 0, sizeof(*g));

    if ((!FILTER_GET_FLAG(r->ra.sflag, RANGE_FLG_NULL)) &&(!FILTER_GET_FLAG(r->ra.eflag, RANGE_FLG_NULL))) {
      __compar_fn_t func = getComparFunc(type, 0);
      if (func(&r->ra.s, &r->ra.e) == 0) {
        void *data = taosMemoryMalloc(sizeof(int64_t));
        SIMPLE_COPY_VALUES(data, &r->ra.s);
        filterAddField(dst, NULL, &data, FLD_TYPE_VALUE, &right, tDataTypes[type].bytes, true);
        filterAddUnit(dst, OP_TYPE_EQUAL, &left, &right, &uidx);
        filterAddUnitToGroup(g, uidx);
      } else {
        void *data = taosMemoryMalloc(sizeof(int64_t));
        SIMPLE_COPY_VALUES(data, &r->ra.s);
        filterAddField(dst, NULL, &data, FLD_TYPE_VALUE, &right, tDataTypes[type].bytes, true);
        void *data2 = taosMemoryMalloc(sizeof(int64_t));
        SIMPLE_COPY_VALUES(data2, &r->ra.e);
        filterAddField(dst, NULL, &data2, FLD_TYPE_VALUE, &right2, tDataTypes[type].bytes, true);
        
        filterAddUnit(dst, FILTER_GET_FLAG(r->ra.sflag, RANGE_FLG_EXCLUDE) ? OP_TYPE_GREATER_THAN : OP_TYPE_GREATER_EQUAL, &left, &right, &uidx);
        filterAddUnitRight(dst, FILTER_GET_FLAG(r->ra.eflag, RANGE_FLG_EXCLUDE) ? OP_TYPE_LOWER_THAN : OP_TYPE_LOWER_EQUAL, &right2, uidx);
        filterAddUnitToGroup(g, uidx);
      }

      taosArrayPush(res, g);
      
      r = r->next;
      
      continue;
    }
    
    if (!FILTER_GET_FLAG(r->ra.sflag, RANGE_FLG_NULL)) {
      void *data = taosMemoryMalloc(sizeof(int64_t));
      SIMPLE_COPY_VALUES(data, &r->ra.s);
      filterAddField(dst, NULL, &data, FLD_TYPE_VALUE, &right, tDataTypes[type].bytes, true);
      filterAddUnit(dst, FILTER_GET_FLAG(r->ra.sflag, RANGE_FLG_EXCLUDE) ? OP_TYPE_GREATER_THAN : OP_TYPE_GREATER_EQUAL, &left, &right, &uidx);
      filterAddUnitToGroup(g, uidx);
    }
    
    if (!FILTER_GET_FLAG(r->ra.eflag, RANGE_FLG_NULL)) {
      void *data = taosMemoryMalloc(sizeof(int64_t));
      SIMPLE_COPY_VALUES(data, &r->ra.e);    
      filterAddField(dst, NULL, &data, FLD_TYPE_VALUE, &right, tDataTypes[type].bytes, true);
      filterAddUnit(dst, FILTER_GET_FLAG(r->ra.eflag, RANGE_FLG_EXCLUDE) ? OP_TYPE_LOWER_THAN : OP_TYPE_LOWER_EQUAL, &left, &right, &uidx);
      filterAddUnitToGroup(g, uidx);
    }

    assert (g->unitNum > 0);

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
  
  SFilterGroup* p = (SFilterGroup*) pItem;
  taosMemoryFreeClear(p->unitIdxs);
  taosMemoryFreeClear(p->unitFlags);
}


EDealRes fltTreeToGroup(SNode* pNode, void* pContext) {
  int32_t code = TSDB_CODE_SUCCESS;
  SArray* preGroup = NULL;
  SArray* newGroup = NULL;
  SArray* resGroup = NULL;
  ENodeType nType = nodeType(pNode);
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

    ctx->code = TSDB_CODE_QRY_APP_ERROR;
    
    fltError("invalid condition type, type:%d", node->condType);

    return DEAL_RES_ERROR;
  }

  if (QUERY_NODE_OPERATOR == nType) {
    FLT_ERR_JRET(fltAddGroupUnitFromNode(ctx->info, pNode, ctx->group));  
    
    return DEAL_RES_IGNORE_CHILD;
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
      n = sprintf(str, (*(int8_t*)buf) ? "true" : "false");
      break;

    case TSDB_DATA_TYPE_TINYINT:
      n = sprintf(str, "%d", *(int8_t*)buf);
      break;

    case TSDB_DATA_TYPE_SMALLINT:
      n = sprintf(str, "%d", *(int16_t*)buf);
      break;

    case TSDB_DATA_TYPE_INT:
      n = sprintf(str, "%d", *(int32_t*)buf);
      break;

    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP:
      n = sprintf(str, "%" PRId64, *(int64_t*)buf);
      break;

    case TSDB_DATA_TYPE_FLOAT:
      n = sprintf(str, "%e", GET_FLOAT_VAL(buf));
      break;

    case TSDB_DATA_TYPE_DOUBLE:
      n = sprintf(str, "%e", GET_DOUBLE_VAL(buf));
      break;

    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_NCHAR:
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
      n = sprintf(str, "%d", *(uint8_t*)buf);
      break;

    case TSDB_DATA_TYPE_USMALLINT:
      n = sprintf(str, "%d", *(uint16_t*)buf);
      break;

    case TSDB_DATA_TYPE_UINT:
      n = sprintf(str, "%u", *(uint32_t*)buf);
      break;

    case TSDB_DATA_TYPE_UBIGINT:
      n = sprintf(str, "%" PRIu64, *(uint64_t*)buf);
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
        SColumnNode *refNode = (SColumnNode *)field->desc;
        qDebug("COL%d => [%d][%d]", i, refNode->dataBlockId, refNode->slotId);
      }

      qDebug("VALUE Field Num:%u", info->fields[FLD_TYPE_VALUE].num);
      for (uint32_t i = 0; i < info->fields[FLD_TYPE_VALUE].num; ++i) {
        SFilterField *field = &info->fields[FLD_TYPE_VALUE].fields[i];
        if (field->desc) {
          SValueNode *var = (SValueNode *)field->desc;
          SDataType *dType = &var->node.resType;
          if (dType->type == TSDB_DATA_TYPE_VALUE_ARRAY) {
            qDebug("VAL%d => [type:TS][val:[%" PRIi64"] - [%" PRId64 "]]", i, *(int64_t *)field->data, *(((int64_t *)field->data) + 1)); 
          } else {
            qDebug("VAL%d => [type:%d][val:%" PRIx64"]", i, dType->type, var->datum.i); //TODO
          }
        } else if (field->data) {
          qDebug("VAL%d => [type:NIL][val:NIL]", i); //TODO
        }
      }

      qDebug("UNIT  Num:%u", info->unitNum);
      for (uint32_t i = 0; i < info->unitNum; ++i) {
        SFilterUnit *unit = &info->units[i];
        int32_t type = FILTER_UNIT_DATA_TYPE(unit);
        int32_t len = 0;
        int32_t tlen = 0;
        char str[512] = {0};
        
        SFilterField *left = FILTER_UNIT_LEFT_FIELD(info, unit);
        SColumnNode *refNode = (SColumnNode *)left->desc;
        if (unit->compare.optr >= 0 && unit->compare.optr <= OP_TYPE_JSON_CONTAINS){
          len = sprintf(str, "UNIT[%d] => [%d][%d]  %s  [", i, refNode->dataBlockId, refNode->slotId, gOptrStr[unit->compare.optr].str);
        }

        if (unit->right.type == FLD_TYPE_VALUE && FILTER_UNIT_OPTR(unit) != OP_TYPE_IN) {
          SFilterField *right = FILTER_UNIT_RIGHT_FIELD(info, unit);
          char *data = right->data;
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
          if (unit->compare.optr2 >= 0 && unit->compare.optr2 <= OP_TYPE_JSON_CONTAINS){
            sprintf(str + strlen(str), "[%d][%d]  %s  [", refNode->dataBlockId, refNode->slotId, gOptrStr[unit->compare.optr2].str);
          }
          
          if (unit->right2.type == FLD_TYPE_VALUE && FILTER_UNIT_OPTR(unit) != OP_TYPE_IN) {
            SFilterField *right = FILTER_UNIT_RIGHT2_FIELD(info, unit);
            char *data = right->data;
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
        
        qDebug("%s", str); //TODO
      }

      qDebug("GROUP Num:%u", info->groupNum);
      for (uint32_t i = 0; i < info->groupNum; ++i) {
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
        qDebug("Column ID[%d] RANGE: isnull[%d],notnull[%d],range[%d]", ctx->colId, ctx->isnull, ctx->notnull, ctx->isrange);
        if (ctx->isrange) {      
          SFilterRangeNode *r = ctx->rs;
          while (r) {
            char str[256] = {0};        
            int32_t tlen = 0;
            if (FILTER_GET_FLAG(r->ra.sflag, RANGE_FLG_NULL)) {
              strcat(str,"(NULL)");
            } else {
              FILTER_GET_FLAG(r->ra.sflag, RANGE_FLG_EXCLUDE) ? strcat(str,"(") : strcat(str,"[");
              fltConverToStr(str + strlen(str), ctx->type, &r->ra.s, tlen > 32 ? 32 : tlen, &tlen);
              FILTER_GET_FLAG(r->ra.sflag, RANGE_FLG_EXCLUDE) ? strcat(str,")") : strcat(str,"]");
            }
            strcat(str, " - ");
            if (FILTER_GET_FLAG(r->ra.eflag, RANGE_FLG_NULL)) {
              strcat(str, "(NULL)");
            } else {
              FILTER_GET_FLAG(r->ra.eflag, RANGE_FLG_EXCLUDE) ? strcat(str,"(") : strcat(str,"[");
              fltConverToStr(str + strlen(str), ctx->type, &r->ra.e, tlen > 32 ? 32 : tlen, &tlen);
              FILTER_GET_FLAG(r->ra.eflag, RANGE_FLG_EXCLUDE) ? strcat(str,")") : strcat(str,"]");
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
    } else if (FILTER_GET_FLAG(info->blkFlag, FI_STATUS_BLK_ACTIVE)){
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
  SFilterColInfo* info = (SFilterColInfo *)data;

  if (info->info == NULL) {
    return;
  }

  if (info->type == RANGE_TYPE_VAR_HASH) {
    //TODO
  } else if (info->type == RANGE_TYPE_MR_CTX) {
    filterFreeRangeCtx(info->info);  
  } else if (info->type == RANGE_TYPE_UNIT) {
    taosArrayDestroy((SArray *)info->info);
  }

  //NO NEED TO FREE UNIT

  info->type = 0;
  info->info = NULL;
}

void filterFreeColCtx(void *data) {
  SFilterColCtx* ctx = (SFilterColCtx *)data;

  if (ctx->ctx) {
    filterFreeRangeCtx(ctx->ctx);
  }
}


void filterFreeGroupCtx(SFilterGroupCtx* gRes) {
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

void filterFreeField(SFilterField* field, int32_t type) {
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

int32_t filterHandleValueExtInfo(SFilterUnit* unit, char extInfo) {
  assert(extInfo > 0 || extInfo < 0);
  
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
      assert(0);
  }

  return TSDB_CODE_SUCCESS;
}


int32_t fltInitValFieldData(SFilterInfo *info) {
  for (uint32_t i = 0; i < info->unitNum; ++i) {
    SFilterUnit* unit = &info->units[i];
    if (unit->right.type != FLD_TYPE_VALUE) {
      assert(unit->compare.optr == FILTER_DUMMY_EMPTY_OPTR || scalarGetOperatorParamNum(unit->compare.optr) == 1);
      continue;
    }
    
    SFilterField* right = FILTER_UNIT_RIGHT_FIELD(info, unit);

    assert(FILTER_GET_FLAG(right->flag, FLD_TYPE_VALUE));

    uint32_t type = FILTER_UNIT_DATA_TYPE(unit);
    int8_t precision = FILTER_UNIT_DATA_PRECISION(unit);
    SFilterField* fi = right;
    
    SValueNode* var = (SValueNode *)fi->desc;
    if (var == NULL) {
      assert(fi->data != NULL);
      continue;
    }

    if (unit->compare.optr == OP_TYPE_IN) {
      FLT_ERR_RET(scalarGenerateSetFromList((void **)&fi->data, fi->desc, type));
      if (fi->data == NULL) {
        fltError("failed to convert in param");
        FLT_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
      }

      FILTER_SET_FLAG(fi->flag, FLD_DATA_IS_HASH);
      
      continue;
    }

    SDataType *dType = &var->node.resType;
    size_t bytes = 0;

    if (type == TSDB_DATA_TYPE_BINARY) {
      size_t len = (dType->type == TSDB_DATA_TYPE_BINARY || dType->type == TSDB_DATA_TYPE_NCHAR) ? dType->bytes : MAX_NUM_STR_SIZE;
      bytes = len + 1 + VARSTR_HEADER_SIZE;

      fi->data = taosMemoryCalloc(1, bytes);
    } else if (type == TSDB_DATA_TYPE_NCHAR) {
      size_t len = (dType->type == TSDB_DATA_TYPE_BINARY || dType->type == TSDB_DATA_TYPE_NCHAR) ? dType->bytes : MAX_NUM_STR_SIZE;
      bytes = (len + 1) * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE;

      fi->data = taosMemoryCalloc(1, bytes);
    } else{
      if (dType->type == TSDB_DATA_TYPE_VALUE_ARRAY) {  //TIME RANGE
/*
        fi->data = taosMemoryCalloc(dType->bytes, tDataTypes[type].bytes);
        for (int32_t a = 0; a < dType->bytes; ++a) {
          int64_t *v = taosArrayGet(var->arr, a);
          assignVal((char *)fi->data + a * tDataTypes[type].bytes, (char *)v, 0, type);
        }
*/
        continue;
      } else {
        fi->data = taosMemoryCalloc(1, sizeof(int64_t));
      }
    }

    if (dType->type == type) {
      assignVal(fi->data, nodesGetValueFromNode(var), dType->bytes, type);
    } else {
      SScalarParam out = {.columnData = taosMemoryCalloc(1, sizeof(SColumnInfoData))};
      out.columnData->info.type = type;
      out.columnData->info.precision = precision;
      if (IS_VAR_DATA_TYPE(type)) {
        out.columnData->info.bytes = bytes;
      } else {
        out.columnData->info.bytes = tDataTypes[type].bytes;
      }

      // todo refactor the convert
      int32_t code = doConvertDataType(var, &out);
      if (code != TSDB_CODE_SUCCESS) {
        qError("convert value to type[%d] failed", type);
        return TSDB_CODE_TSC_INVALID_OPERATION;
      }

      memcpy(fi->data, out.columnData->pData, out.columnData->info.bytes);
      colDataDestroy(out.columnData);
      taosMemoryFree(out.columnData);
    }

    // match/nmatch for nchar type need convert from ucs4 to mbs
    if(type == TSDB_DATA_TYPE_NCHAR &&
        (unit->compare.optr == OP_TYPE_MATCH || unit->compare.optr == OP_TYPE_NMATCH)){
      char newValData[TSDB_REGEX_STRING_DEFAULT_LEN * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE] = {0};
      int32_t len = taosUcs4ToMbs((TdUcs4*)varDataVal(fi->data), varDataLen(fi->data), varDataVal(newValData));
      if (len < 0){
        qError("filterInitValFieldData taosUcs4ToMbs error 1");
        return TSDB_CODE_QRY_APP_ERROR;
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
      assert(false);
  }

  return true;
}


int32_t filterAddUnitRange(SFilterInfo *info, SFilterUnit* u, SFilterRangeCtx *ctx, int32_t optr) {
  int32_t type = FILTER_UNIT_DATA_TYPE(u);
  uint8_t uoptr = FILTER_UNIT_OPTR(u);
  void *val = FILTER_UNIT_VAL_DATA(info, u);
  SFilterRange ra = {0};
  int64_t tmp = 0;

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
      assert(type == TSDB_DATA_TYPE_BOOL);
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
      assert(0);
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


int32_t filterMergeUnits(SFilterInfo *info, SFilterGroupCtx* gRes, uint32_t colIdx, bool *empty) {
  SArray* colArray = (SArray *)gRes->colInfo[colIdx].info;
  int32_t size = (int32_t)taosArrayGetSize(colArray);
  int32_t type = gRes->colInfo[colIdx].dataType;
  SFilterRangeCtx* ctx = filterInitRangeCtx(type, 0);
  
  for (uint32_t i = 0; i < size; ++i) {
    SFilterUnit* u = taosArrayGetP(colArray, i);
    uint8_t optr = FILTER_UNIT_OPTR(u);

    filterAddRangeOptr(ctx, optr, LOGIC_COND_TYPE_AND, empty, NULL);
    FLT_CHK_JMP(*empty);

    if (!FILTER_NO_MERGE_OPTR(optr)) {
      filterAddUnitRange(info, u, ctx, LOGIC_COND_TYPE_AND);
      FLT_CHK_JMP(MR_EMPTY_RES(ctx));
    }
    if(FILTER_UNIT_OPTR(u) == OP_TYPE_EQUAL && !FILTER_NO_MERGE_DATA_TYPE(FILTER_UNIT_DATA_TYPE(u))){
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


int32_t filterMergeGroupUnits(SFilterInfo *info, SFilterGroupCtx** gRes, int32_t* gResNum) {
  bool empty = false;
  uint32_t *colIdx = taosMemoryMalloc(info->fields[FLD_TYPE_COLUMN].num * sizeof(uint32_t));
  uint32_t colIdxi = 0;
  uint32_t gResIdx = 0;
  
  for (uint32_t i = 0; i < info->groupNum; ++i) {
    SFilterGroup* g = info->groups + i;

    gRes[gResIdx] = taosMemoryCalloc(1, sizeof(SFilterGroupCtx));
    gRes[gResIdx]->colInfo = taosMemoryCalloc(info->fields[FLD_TYPE_COLUMN].num, sizeof(SFilterColInfo));
    colIdxi = 0;
    empty = false;
    
    for (uint32_t j = 0; j < g->unitNum; ++j) {
      SFilterUnit* u = FILTER_GROUP_UNIT(info, g, j);
      uint32_t cidx = FILTER_UNIT_COL_IDX(u);

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
      qsort(colIdx, colIdxi, sizeof(uint32_t), getComparFunc(TSDB_DATA_TYPE_USMALLINT, 0));
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

void filterCheckColConflict(SFilterGroupCtx* gRes1, SFilterGroupCtx* gRes2, bool *conflict) {
  uint32_t idx1 = 0, idx2 = 0, m = 0, n = 0;
  bool equal = false;
  
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

      // for long in operation
      if (gRes1->colInfo[idx1].optr == OP_TYPE_EQUAL && gRes2->colInfo[idx2].optr == OP_TYPE_EQUAL) {
        SFilterRangeCtx* ctx = gRes1->colInfo[idx1].info;
        if (ctx->pCompareFunc(&gRes1->colInfo[idx1].value, &gRes2->colInfo[idx2].value)){
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


int32_t filterMergeTwoGroupsImpl(SFilterInfo *info, SFilterRangeCtx **ctx, int32_t optr, uint32_t cidx, SFilterGroupCtx* gRes1, SFilterGroupCtx* gRes2, bool *empty, bool *all) {
  SFilterField *fi = FILTER_GET_COL_FIELD(info, cidx);
  int32_t type = FILTER_GET_COL_FIELD_TYPE(fi);

  if ((*ctx) == NULL) {
    *ctx = filterInitRangeCtx(type, 0);
  } else {
    filterReuseRangeCtx(*ctx, type, 0);
  }

  assert(gRes2->colInfo[cidx].type == RANGE_TYPE_MR_CTX);
  assert(gRes1->colInfo[cidx].type == RANGE_TYPE_MR_CTX);

  filterCopyRangeCtx(*ctx, gRes2->colInfo[cidx].info);
  filterSourceRangeFromCtx(*ctx, gRes1->colInfo[cidx].info, optr, empty, all);

  return TSDB_CODE_SUCCESS;
}


int32_t filterMergeTwoGroups(SFilterInfo *info, SFilterGroupCtx** gRes1, SFilterGroupCtx** gRes2, bool *all) {
  bool conflict = false;
  
  filterCheckColConflict(*gRes1, *gRes2, &conflict);
  if (conflict) {
    return TSDB_CODE_SUCCESS;
  }

  FILTER_SET_FLAG(info->status, FI_STATUS_REWRITE);

  uint32_t idx1 = 0, idx2 = 0, m = 0, n = 0;
  bool numEqual = (*gRes1)->colNum == (*gRes2)->colNum;
  bool equal = false;
  uint32_t equal1 = 0, equal2 = 0, merNum = 0;
  SFilterRangeCtx *ctx = NULL;
  SFilterColCtx colCtx = {0};
  SArray* colCtxs = taosArrayInit((*gRes2)->colNum, sizeof(SFilterColCtx));

  for (; m < (*gRes1)->colNum; ++m) {
    idx1 = (*gRes1)->colIdx[m];
  
    for (; n < (*gRes2)->colNum; ++n) {
      idx2 = (*gRes2)->colIdx[n];

      if (idx1 > idx2) {
        continue;
      }

      assert(idx1 == idx2);
      
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

  assert(merNum > 0);

  SFilterColInfo *colInfo = NULL;
  assert (merNum == equal1 || merNum == equal2);

  filterFreeGroupCtx(*gRes2);
  *gRes2 = NULL;

  assert(colCtxs && taosArrayGetSize(colCtxs) > 0);

  int32_t ctxSize = (int32_t)taosArrayGetSize(colCtxs);
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


int32_t filterMergeGroups(SFilterInfo *info, SFilterGroupCtx** gRes, int32_t *gResNum) {
  if (*gResNum <= 1) {
    return TSDB_CODE_SUCCESS;
  }

  qsort(gRes, *gResNum, POINTER_BYTES, filterCompareGroupCtx);

  int32_t pEnd = 0, cStart = 0, cEnd = 0;
  uint32_t pColNum = 0, cColNum = 0; 
  int32_t movedNum = 0;
  bool all = false;

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
          assert(m < n);
          filterMergeTwoGroups(info, &gRes[m], &gRes[n], &all);

          FLT_CHK_JMP(all);

          if (gRes[n] == NULL) {
            if (n < ((*gResNum) - 1)) {
              memmove(&gRes[n], &gRes[n+1], (*gResNum-n-1) * POINTER_BYTES);
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
        assert(m < n);
        filterMergeTwoGroups(info, &gRes[m], &gRes[n], &all);

        FLT_CHK_JMP(all);
        
        if (gRes[n] == NULL) {
          if (n < ((*gResNum) - 1)) {
            memmove(&gRes[n], &gRes[n+1], (*gResNum-n-1) * POINTER_BYTES);
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

int32_t filterConvertGroupFromArray(SFilterInfo *info, SArray* group) {
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

int32_t filterRewrite(SFilterInfo *info, SFilterGroupCtx** gRes, int32_t gResNum) {
  if (!FILTER_GET_FLAG(info->status, FI_STATUS_REWRITE)) {
    qDebug("no need rewrite");
    return TSDB_CODE_SUCCESS;
  }
  
  SFilterInfo oinfo = *info;

  FILTER_SET_FLAG(oinfo.status, FI_STATUS_CLONED);
  
  SArray* group = taosArrayInit(FILTER_DEFAULT_GROUP_SIZE, sizeof(SFilterGroup));
  SFilterGroupCtx *res = NULL;
  SFilterColInfo *colInfo = NULL;
  int32_t optr = 0;
  uint32_t uidx = 0;

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
        assert(colInfo->type == RANGE_TYPE_UNIT);
        int32_t usize = (int32_t)taosArrayGetSize((SArray *)colInfo->info);
        
        for (int32_t n = 0; n < usize; ++n) {
          SFilterUnit* u = taosArrayGetP((SArray *)colInfo->info, n);
          
          filterAddUnitFromUnit(info, &oinfo, u, &uidx);
          filterAddUnitToGroup(&ng, uidx);
        }
        
        continue;
      }
      
      assert(colInfo->type == RANGE_TYPE_MR_CTX);
      
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

int32_t filterGenerateColRange(SFilterInfo *info, SFilterGroupCtx** gRes, int32_t gResNum) {
  uint32_t *idxs = NULL;
  uint32_t colNum = 0;
  SFilterGroupCtx *res = NULL;
  uint32_t *idxNum = taosMemoryCalloc(info->fields[FLD_TYPE_COLUMN].num, sizeof(*idxNum));

  for (int32_t i = 0; i < gResNum; ++i) {
    for (uint32_t m = 0; m < gRes[i]->colNum; ++m) {
      SFilterColInfo  *colInfo = &gRes[i]->colInfo[gRes[i]->colIdx[m]];
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

    assert(idxNum[i] == gResNum);
    
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

        assert(res->colIdx[n] == idxs[m]);

        SFilterColInfo * colInfo = &res->colInfo[res->colIdx[n]];
        if (info->colRange[m] == NULL) {
          info->colRange[m] = filterInitRangeCtx(colInfo->dataType, 0);
          SFilterField* fi = FILTER_GET_COL_FIELD(info, res->colIdx[n]);
          info->colRange[m]->colId = FILTER_GET_COL_FIELD_ID(fi);
        }

        assert(colInfo->type == RANGE_TYPE_MR_CTX);

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
    SFilterRangeCtx* ctx = info->colRange[i];
    SFilterRangeNode *r = ctx->rs;
    while (r) {
      r->rc.func = filterGetRangeCompFunc(r->ra.sflag, r->ra.eflag);
      r = r->next;
    }
  }

  return TSDB_CODE_SUCCESS;
}


int32_t filterGenerateComInfo(SFilterInfo *info) {
  info->cunits = taosMemoryMalloc(info->unitNum * sizeof(*info->cunits));
  info->blkUnitRes = taosMemoryMalloc(sizeof(*info->blkUnitRes) * info->unitNum);
  info->blkUnits = taosMemoryMalloc(sizeof(*info->blkUnits) * (info->unitNum + 1) * info->groupNum);

  for (uint32_t i = 0; i < info->unitNum; ++i) {
    SFilterUnit *unit = &info->units[i];

    info->cunits[i].func = filterGetCompFuncIdx(FILTER_UNIT_DATA_TYPE(unit), unit->compare.optr);
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
  
  return TSDB_CODE_SUCCESS;
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
    int32_t index = -1;
    SFilterComUnit *cunit = &info->cunits[k];

    if (FILTER_NO_MERGE_DATA_TYPE(cunit->dataType)) {
      continue;
    }

    for(int32_t i = 0; i < numOfCols; ++i) {
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

    if (cunit->optr == OP_TYPE_IS_NULL || cunit->optr == OP_TYPE_IS_NOT_NULL
     || cunit->optr == OP_TYPE_IN || cunit->optr == OP_TYPE_LIKE || cunit->optr == OP_TYPE_MATCH
     || cunit->optr == OP_TYPE_NOT_EQUAL) {
      continue;
    }

    SColumnDataAgg* pDataBlockst = &pDataStatis[index];
    void *minVal, *maxVal;
    float minv = 0;
    float maxv = 0;

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
      minRes = (*gRangeCompare[cunit->rfunc])(minVal, minVal, cunit->valData, cunit->valData2, gDataCompare[cunit->func]);
      maxRes = (*gRangeCompare[cunit->rfunc])(maxVal, maxVal, cunit->valData, cunit->valData2, gDataCompare[cunit->func]);

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
  int32_t all = 0, empty = 0;
  
  for (uint32_t g = 0; g < info->groupNum; ++g) {
    SFilterGroup *group = &info->groups[g];
    *unitNum = group->unitNum;
    all = 0; 
    empty = 0;
    
    for (uint32_t u = 0; u < group->unitNum; ++u) {
      uint32_t uidx = group->unitIdxs[u];
      if (info->blkUnitRes[uidx] == 1) {
        --(*unitNum);
        all = 1;
        continue;
      } else if (info->blkUnitRes[uidx] == -1) {
        *unitNum = 0;
        empty = 1;
        break;
      }

      *(unitIdx++) = uidx;
    }

    if (*unitNum == 0) {
      --info->blkGroupNum;
      assert(empty || all);
      
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

bool filterExecuteBasedOnStatisImpl(void *pinfo, int32_t numOfRows, int8_t** p, SColumnDataAgg *statis, int16_t numOfCols) {
  SFilterInfo *info = (SFilterInfo *)pinfo;
  bool all = true;
  uint32_t *unitIdx = NULL;

  if (*p == NULL) {
    *p = taosMemoryCalloc(numOfRows, sizeof(int8_t));
  }
  
  for (int32_t i = 0; i < numOfRows; ++i) {
    //FILTER_UNIT_CLR_F(info);

    unitIdx = info->blkUnits;
    
    for (uint32_t g = 0; g < info->blkGroupNum; ++g) {
      uint32_t unitNum = *(unitIdx++);
      for (uint32_t u = 0; u < unitNum; ++u) {
        SFilterComUnit *cunit = &info->cunits[*(unitIdx + u)];
        void *colData = colDataGetData((SColumnInfoData *)cunit->colData, i);
      
        //if (FILTER_UNIT_GET_F(info, uidx)) {
        //  p[i] = FILTER_UNIT_GET_R(info, uidx);
        //} else {
          uint8_t optr = cunit->optr;

          if (colDataIsNull((SColumnInfoData *)(cunit->colData), 0, i, NULL)) {
            (*p)[i] = optr == OP_TYPE_IS_NULL ? true : false;
          } else {
            if (optr == OP_TYPE_IS_NOT_NULL) {
              (*p)[i] = 1;
            } else if (optr == OP_TYPE_IS_NULL) {
              (*p)[i] = 0;
            } else if (cunit->rfunc >= 0) {
              (*p)[i] = (*gRangeCompare[cunit->rfunc])(colData, colData, cunit->valData, cunit->valData2, gDataCompare[cunit->func]);
            } else {
              (*p)[i] = filterDoCompare(gDataCompare[cunit->func], cunit->optr, colData, cunit->valData);
            }
          
          //FILTER_UNIT_SET_R(info, uidx, p[i]);
          //FILTER_UNIT_SET_F(info, uidx);
          }

        if ((*p)[i] == 0) {
          break;
        }
      }

      if ((*p)[i]) {
        break;
      }

      unitIdx += unitNum;
    }

    if ((*p)[i] == 0) {
      all = false;
    }    
  }

  return all;
}



int32_t filterExecuteBasedOnStatis(SFilterInfo *info, int32_t numOfRows, int8_t** p, SColumnDataAgg *statis, int16_t numOfCols, bool* all) {
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

      assert(info->unitNum > 1);
      
      *all = filterExecuteBasedOnStatisImpl(info, numOfRows, p, statis, numOfCols);

      goto _return;
    }
  }

  return 1;

_return:
  info->blkFlag = 0;
  
  return TSDB_CODE_SUCCESS;
}


static FORCE_INLINE bool filterExecuteImplAll(void *info, int32_t numOfRows, int8_t** p, SColumnDataAgg *statis, int16_t numOfCols) {
  return true;
}
static FORCE_INLINE bool filterExecuteImplEmpty(void *info, int32_t numOfRows, int8_t** p, SColumnDataAgg *statis, int16_t numOfCols) {
  return false;
}
static FORCE_INLINE bool filterExecuteImplIsNull(void *pinfo, int32_t numOfRows, int8_t** p, SColumnDataAgg *statis, int16_t numOfCols) {
  SFilterInfo *info = (SFilterInfo *)pinfo;
  bool all = true;

  if (filterExecuteBasedOnStatis(info, numOfRows, p, statis, numOfCols, &all) == 0) {
    return all;
  }

  if (*p == NULL) {
    *p = taosMemoryCalloc(numOfRows, sizeof(int8_t));
  }
  
  for (int32_t i = 0; i < numOfRows; ++i) {
    uint32_t uidx = info->groups[0].unitIdxs[0];
    void *colData = colDataGetData((SColumnInfoData *)info->cunits[uidx].colData, i);
    (*p)[i] = ((colData == NULL) || colDataIsNull((SColumnInfoData *)info->cunits[uidx].colData, 0, i, NULL));

    if ((*p)[i] == 0) {
      all = false;
    }    
  }

  return all;
}
static FORCE_INLINE bool filterExecuteImplNotNull(void *pinfo, int32_t numOfRows, int8_t** p, SColumnDataAgg *statis, int16_t numOfCols) {
  SFilterInfo *info = (SFilterInfo *)pinfo;
  bool all = true;

  if (filterExecuteBasedOnStatis(info, numOfRows, p, statis, numOfCols, &all) == 0) {
    return all;
  }

  if (*p == NULL) {
    *p = taosMemoryCalloc(numOfRows, sizeof(int8_t));
  }
  
  for (int32_t i = 0; i < numOfRows; ++i) {
    uint32_t uidx = info->groups[0].unitIdxs[0];
    void *colData = colDataGetData((SColumnInfoData *)info->cunits[uidx].colData, i);

    (*p)[i] = ((colData != NULL) && !colDataIsNull((SColumnInfoData *)info->cunits[uidx].colData, 0, i, NULL));
    if ((*p)[i] == 0) {
      all = false;
    }
  }

  return all;
}

bool filterExecuteImplRange(void *pinfo, int32_t numOfRows, int8_t** p, SColumnDataAgg *statis, int16_t numOfCols) {
  SFilterInfo *info = (SFilterInfo *)pinfo;
  bool all = true;
  uint16_t dataSize = info->cunits[0].dataSize;
  rangeCompFunc rfunc = gRangeCompare[info->cunits[0].rfunc];
  void *valData = info->cunits[0].valData;
  void *valData2 = info->cunits[0].valData2;
  __compar_fn_t func = gDataCompare[info->cunits[0].func];

  if (filterExecuteBasedOnStatis(info, numOfRows, p, statis, numOfCols, &all) == 0) {
    return all;
  }

  if (*p == NULL) {
    *p = taosMemoryCalloc(numOfRows, sizeof(int8_t));
  }
  
  for (int32_t i = 0; i < numOfRows; ++i) {    
    void *colData = colDataGetData((SColumnInfoData *)info->cunits[0].colData, i);
    SColumnInfoData* pData = info->cunits[0].colData;
    if (colData == NULL || colDataIsNull_s(pData, i)) {
      all = false;
      continue;
    }

    (*p)[i] = (*rfunc)(colData, colData, valData, valData2, func);
            
    if ((*p)[i] == 0) {
      all = false;
    }
  }

  return all;
}

bool filterExecuteImplMisc(void *pinfo, int32_t numOfRows, int8_t** p, SColumnDataAgg *statis, int16_t numOfCols) {
  SFilterInfo *info = (SFilterInfo *)pinfo;
  bool all = true;

  if (filterExecuteBasedOnStatis(info, numOfRows, p, statis, numOfCols, &all) == 0) {
    return all;
  }
  
  if (*p == NULL) {
    *p = taosMemoryCalloc(numOfRows, sizeof(int8_t));
  }
  
  for (int32_t i = 0; i < numOfRows; ++i) {
    uint32_t uidx = info->groups[0].unitIdxs[0];
    void *colData = colDataGetData((SColumnInfoData *)info->cunits[uidx].colData, i);
    if (colData == NULL || colDataIsNull_s((SColumnInfoData *)info->cunits[uidx].colData, i)) {
      (*p)[i] = 0;
      all = false;
      continue;
    }

    // match/nmatch for nchar type need convert from ucs4 to mbs
    if(info->cunits[uidx].dataType == TSDB_DATA_TYPE_NCHAR && (info->cunits[uidx].optr == OP_TYPE_MATCH || info->cunits[uidx].optr == OP_TYPE_NMATCH)){
      char *newColData = taosMemoryCalloc(info->cunits[uidx].dataSize * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE, 1);
      int32_t len = taosUcs4ToMbs((TdUcs4*)varDataVal(colData), varDataLen(colData), varDataVal(newColData));
      if (len < 0){
        qError("castConvert1 taosUcs4ToMbs error");
      }else{
        varDataSetLen(newColData, len);
        (*p)[i] = filterDoCompare(gDataCompare[info->cunits[uidx].func], info->cunits[uidx].optr, newColData, info->cunits[uidx].valData);
      }
      taosMemoryFreeClear(newColData);
    }else{
      (*p)[i] = filterDoCompare(gDataCompare[info->cunits[uidx].func], info->cunits[uidx].optr, colData, info->cunits[uidx].valData);
    }

    if ((*p)[i] == 0) {
      all = false;
    }
  }

  return all;
}


bool filterExecuteImpl(void *pinfo, int32_t numOfRows, int8_t** p, SColumnDataAgg *statis, int16_t numOfCols) {
  SFilterInfo *info = (SFilterInfo *)pinfo;
  bool all = true;

  if (filterExecuteBasedOnStatis(info, numOfRows, p, statis, numOfCols, &all) == 0) {
    return all;
  }

  if (*p == NULL) {
    *p = taosMemoryCalloc(numOfRows, sizeof(int8_t));
  }
  
  for (int32_t i = 0; i < numOfRows; ++i) {
    //FILTER_UNIT_CLR_F(info);
  
    for (uint32_t g = 0; g < info->groupNum; ++g) {
      SFilterGroup *group = &info->groups[g];
      for (uint32_t u = 0; u < group->unitNum; ++u) {
        uint32_t uidx = group->unitIdxs[u];
        SFilterComUnit *cunit = &info->cunits[uidx];
        void *colData = colDataGetData((SColumnInfoData *)(cunit->colData), i);
      
        //if (FILTER_UNIT_GET_F(info, uidx)) {
        //  p[i] = FILTER_UNIT_GET_R(info, uidx);
        //} else {
          uint8_t optr = cunit->optr;

          if (colData == NULL || colDataIsNull((SColumnInfoData *)(cunit->colData), 0, i, NULL)) {
            (*p)[i] = optr == OP_TYPE_IS_NULL ? true : false;
          } else {
            if (optr == OP_TYPE_IS_NOT_NULL) {
              (*p)[i] = 1;
            } else if (optr == OP_TYPE_IS_NULL) {
              (*p)[i] = 0;
            } else if (cunit->rfunc >= 0) {
              (*p)[i] = (*gRangeCompare[cunit->rfunc])(colData, colData, cunit->valData, cunit->valData2, gDataCompare[cunit->func]);
            } else {
              if(cunit->dataType == TSDB_DATA_TYPE_NCHAR && (cunit->optr == OP_TYPE_MATCH || cunit->optr == OP_TYPE_NMATCH)){
                char *newColData = taosMemoryCalloc(cunit->dataSize * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE, 1);
                int32_t len = taosUcs4ToMbs((TdUcs4*)varDataVal(colData), varDataLen(colData), varDataVal(newColData));
                if (len < 0){
                  qError("castConvert1 taosUcs4ToMbs error");
                }else{
                  varDataSetLen(newColData, len);
                  (*p)[i] = filterDoCompare(gDataCompare[cunit->func], cunit->optr, newColData, cunit->valData);
                }
                taosMemoryFreeClear(newColData);
              }else{
                (*p)[i] = filterDoCompare(gDataCompare[cunit->func], cunit->optr, colData, cunit->valData);
              }
            }
          
          //FILTER_UNIT_SET_R(info, uidx, p[i]);
          //FILTER_UNIT_SET_F(info, uidx);
          }

        if ((*p)[i] == 0) {
          break;
        }
      }

      if ((*p)[i]) {
        break;
      }
    }

    if ((*p)[i] == 0) {
      all = false;
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
  SFilterGroupCtx** gRes = taosMemoryCalloc(info->groupNum, sizeof(SFilterGroupCtx *));
  int32_t gResNum = 0;
  
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

  filterGenerateComInfo(info);

_return:

  filterSetExecFunc(info);

  for (int32_t i = 0; i < gResNum; ++i) {
    filterFreeGroupCtx(gRes[i]);
  }

  taosMemoryFreeClear(gRes);
  
  return TSDB_CODE_SUCCESS;
}


int32_t fltSetColFieldDataImpl(SFilterInfo *info, void *param, filer_get_col_from_id fp, bool fromColId) {
  if (FILTER_ALL_RES(info) || FILTER_EMPTY_RES(info)) {
    return TSDB_CODE_SUCCESS;
  }

  for (uint32_t i = 0; i < info->fields[FLD_TYPE_COLUMN].num; ++i) {
    SFilterField* fi = &info->fields[FLD_TYPE_COLUMN].fields[i];

    if (fromColId) {
      (*fp)(param, FILTER_GET_COL_FIELD_ID(fi), &fi->data);
    } else {
      (*fp)(param, FILTER_GET_COL_FIELD_SLOT_ID(fi), &fi->data);
    }
  }

  filterUpdateComUnits(info);

  return TSDB_CODE_SUCCESS;
}


int32_t fltInitFromNode(SNode* tree, SFilterInfo *info, uint32_t options) {
  int32_t code = TSDB_CODE_SUCCESS;
  
  SArray* group = taosArrayInit(FILTER_DEFAULT_GROUP_SIZE, sizeof(SFilterGroup));

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
  qInfo("init from node failed, code:%d", code);
  return code;
}

bool filterRangeExecute(SFilterInfo *info, SColumnDataAgg *pDataStatis, int32_t numOfCols, int32_t numOfRows) {
  if (FILTER_EMPTY_RES(info)) {
    return false;
  }

  if (FILTER_ALL_RES(info)) {
    return true;
  }
  
  bool ret = true;
  void *minVal, *maxVal;
  
  for (uint32_t k = 0; k < info->colRangeNum; ++k) {
    int32_t index = -1;
    SFilterRangeCtx *ctx = info->colRange[k];
    for(int32_t i = 0; i < numOfCols; ++i) {
      if (pDataStatis[i].colId == ctx->colId) {
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

    if (pDataStatis[index].numOfNull <= 0) {
      if (ctx->isnull && !ctx->notnull && !ctx->isrange) {
        ret = false;
        break;
      }
    } else if (pDataStatis[index].numOfNull > 0) {
      if (pDataStatis[index].numOfNull == numOfRows) {
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

    SColumnDataAgg* pDataBlockst = &pDataStatis[index];

    SFilterRangeNode *r = ctx->rs;
    float minv = 0;
    float maxv = 0;

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



int32_t filterGetTimeRangeImpl(SFilterInfo *info, STimeWindow       *win, bool *isStrict) {
  SFilterRange ra = {0};
  SFilterRangeCtx *prev = filterInitRangeCtx(TSDB_DATA_TYPE_TIMESTAMP, FLT_OPTION_TIMESTAMP);
  SFilterRangeCtx *tmpc = filterInitRangeCtx(TSDB_DATA_TYPE_TIMESTAMP, FLT_OPTION_TIMESTAMP);
  SFilterRangeCtx *cur = NULL;
  int32_t num = 0;
  int32_t optr = 0;
  int32_t code = 0;
  bool empty = false, all = false;

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
      uint32_t uidx = group->unitIdxs[u];
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

  qDebug("qFilter time range:[%"PRId64 "]-[%"PRId64 "]", win->skey, win->ekey);
  return TSDB_CODE_SUCCESS;

_return:

  *win = TSWINDOW_DESC_INITIALIZER;

  filterFreeRangeCtx(prev);
  filterFreeRangeCtx(tmpc);

  qDebug("qFilter time range:[%"PRId64 "]-[%"PRId64 "]", win->skey, win->ekey);

  return code;
}


int32_t filterGetTimeRange(SNode *pNode, STimeWindow *win, bool *isStrict) {
  SFilterInfo *info = NULL;
  int32_t code = 0;
  
  *isStrict = true;

  FLT_ERR_RET(filterInitFromNode(pNode, &info, FLT_OPTION_NO_REWRITE|FLT_OPTION_TIMESTAMP));

  if (info->scalarMode) {
    *win = TSWINDOW_INITIALIZER;
    *isStrict = false;
    goto _return;
  }

  FLT_ERR_JRET(filterGetTimeRangeImpl(info, win, isStrict));

_return:

  filterFreeInfo(info);

  FLT_RET(code);
}


int32_t filterConverNcharColumns(SFilterInfo* info, int32_t rows, bool *gotNchar) {
  if (FILTER_EMPTY_RES(info) || FILTER_ALL_RES(info)) {
    return TSDB_CODE_SUCCESS;
  }
 
  for (uint32_t i = 0; i < info->fields[FLD_TYPE_COLUMN].num; ++i) {
    SFilterField* fi = &info->fields[FLD_TYPE_COLUMN].fields[i];
    int32_t type = FILTER_GET_COL_FIELD_TYPE(fi);
    if (type == TSDB_DATA_TYPE_NCHAR) {
      SFilterField nfi = {0};
      nfi.desc = fi->desc;
      int32_t bytes = FILTER_GET_COL_FIELD_SIZE(fi);
      nfi.data = taosMemoryMalloc(rows * bytes);
      int32_t bufSize = bytes - VARSTR_HEADER_SIZE;
      for (int32_t j = 0; j < rows; ++j) {
        char *src = FILTER_GET_COL_FIELD_DATA(fi, j);
        char *dst = FILTER_GET_COL_FIELD_DATA(&nfi, j);
        int32_t len = 0;
        char *varSrc = varDataVal(src);
        size_t k = 0, varSrcLen = varDataLen(src);
        while (k < varSrcLen && varSrc[k++] == -1) {}
        if (k == varSrcLen) {
          /* NULL */
          varDataLen(dst) = (VarDataLenT) varSrcLen;
          varDataCopy(dst, src);
          continue;
        }
        bool ret = taosMbsToUcs4(varDataVal(src), varDataLen(src), (TdUcs4*)varDataVal(dst), bufSize, &len);
        if(!ret) {
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

int32_t filterFreeNcharColumns(SFilterInfo* info) {
  for (uint32_t i = 0; i < info->fields[FLD_TYPE_COLUMN].num; ++i) {
    SFilterField* fi = &info->fields[FLD_TYPE_COLUMN].fields[i];
    int32_t type = FILTER_GET_COL_FIELD_TYPE(fi);
    if (type == TSDB_DATA_TYPE_NCHAR) {
      taosMemoryFreeClear(fi->data);
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t fltAddValueNodeToConverList(SFltTreeStat *stat, SValueNode* pNode) {
  if (NULL == stat->nodeList) {
    stat->nodeList = taosArrayInit(10, POINTER_BYTES);
    if (NULL == stat->nodeList) {
      FLT_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
    }
  }

  if (NULL == taosArrayPush(stat->nodeList, &pNode)) {
    FLT_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  return TSDB_CODE_SUCCESS;
}

void fltConvertToTsValueNode(SFltTreeStat *stat, SValueNode* valueNode) {
  char *timeStr = valueNode->datum.p;
  if (convertStringToTimestamp(valueNode->node.resType.type, valueNode->datum.p, stat->precision, &valueNode->datum.i) !=
      TSDB_CODE_SUCCESS) {
    valueNode->datum.i = 0;
  }
  taosMemoryFree(timeStr);
  
  valueNode->node.resType.type = TSDB_DATA_TYPE_TIMESTAMP;
  valueNode->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_TIMESTAMP].bytes;
}

EDealRes fltReviseRewriter(SNode** pNode, void* pContext) {
  SFltTreeStat *stat = (SFltTreeStat *)pContext;

  if (QUERY_NODE_LOGIC_CONDITION == nodeType(*pNode)) {
    SLogicConditionNode *node = (SLogicConditionNode *)*pNode;
    SListCell *cell = node->pParameterList->pHead;
    for (int32_t i = 0; i < node->pParameterList->length; ++i) {
      if (NULL == cell || NULL == cell->pNode) {
        fltError("invalid cell, cell:%p, pNode:%p", cell, cell->pNode);
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
    if (!FILTER_GET_FLAG(stat->info->options, FLT_OPTION_TIMESTAMP)) {
      return DEAL_RES_CONTINUE;
    }
    
    SValueNode *valueNode = (SValueNode *)*pNode;
    if (TSDB_DATA_TYPE_BINARY != valueNode->node.resType.type && TSDB_DATA_TYPE_NCHAR != valueNode->node.resType.type) {
      return DEAL_RES_CONTINUE;
    }

    if (stat->precision < 0) {
      int32_t code = fltAddValueNodeToConverList(stat, valueNode);
      if (code) {
        stat->code = code;
        return DEAL_RES_ERROR;
      }
      
      return DEAL_RES_CONTINUE;
    }

    fltConvertToTsValueNode(stat, valueNode);

    return DEAL_RES_CONTINUE;
  }

  if (QUERY_NODE_COLUMN == nodeType(*pNode)) {
    SColumnNode *colNode = (SColumnNode *)*pNode;
    stat->precision = colNode->node.resType.precision;
    return DEAL_RES_CONTINUE;
  }
  
  if (QUERY_NODE_NODE_LIST == nodeType(*pNode)) {
    return DEAL_RES_CONTINUE;
  }

  if (QUERY_NODE_FUNCTION == nodeType(*pNode)) {
    stat->scalarMode = true;
    return DEAL_RES_CONTINUE;
  }

  if (QUERY_NODE_OPERATOR == nodeType(*pNode)) {
    SOperatorNode *node = (SOperatorNode *)*pNode;
    if (!FLT_IS_COMPARISON_OPERATOR(node->opType)) {
      stat->scalarMode = true;
      return DEAL_RES_CONTINUE;
    }

    if (NULL == node->pRight) {
      if (scalarGetOperatorParamNum(node->opType) > 1) {
        fltError("invalid operator, pRight:%p, nodeType:%d, opType:%d", node->pRight, nodeType(node), node->opType);
        stat->code = TSDB_CODE_QRY_APP_ERROR;
        return DEAL_RES_ERROR;
      }
      
      if (QUERY_NODE_COLUMN != nodeType(node->pLeft)) {
        stat->scalarMode = true;
        return DEAL_RES_CONTINUE;
      }

      if (OP_TYPE_IS_TRUE == node->opType || OP_TYPE_IS_FALSE == node->opType || OP_TYPE_IS_UNKNOWN == node->opType
       || OP_TYPE_IS_NOT_TRUE == node->opType || OP_TYPE_IS_NOT_FALSE == node->opType || OP_TYPE_IS_NOT_UNKNOWN == node->opType) {
        stat->scalarMode = true;
        return DEAL_RES_CONTINUE;
      }
    } else {
      if ((QUERY_NODE_COLUMN != nodeType(node->pLeft)) && (QUERY_NODE_VALUE != nodeType(node->pLeft))) {
        stat->scalarMode = true;
        return DEAL_RES_CONTINUE;
      }

      if ((QUERY_NODE_COLUMN != nodeType(node->pRight)) && (QUERY_NODE_VALUE != nodeType(node->pRight))) {
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
      }

      if (OP_TYPE_IN == node->opType && QUERY_NODE_NODE_LIST != nodeType(node->pRight)) {
        fltError("invalid IN operator node, rightType:%d", nodeType(node->pRight));
        stat->code = TSDB_CODE_QRY_APP_ERROR;
        return DEAL_RES_ERROR;
      }

      if (OP_TYPE_IN != node->opType) {
        SColumnNode *refNode = (SColumnNode *)node->pLeft;
        SValueNode *valueNode = (SValueNode *)node->pRight;
        int32_t type = vectorGetConvertType(refNode->node.resType.type, valueNode->node.resType.type);
        if (0 != type && type != refNode->node.resType.type) {
          stat->scalarMode = true;
          return DEAL_RES_CONTINUE;
        }
      }
    }

    return DEAL_RES_CONTINUE;
  }  
  
  fltError("invalid node type for filter, type:%d", nodeType(*pNode));
  
  stat->code = TSDB_CODE_QRY_INVALID_INPUT;
  
  return DEAL_RES_ERROR;
}

int32_t fltReviseNodes(SFilterInfo *pInfo, SNode** pNode, SFltTreeStat *pStat) {
  int32_t code = 0;
  nodesRewriteExprPostOrder(pNode, fltReviseRewriter, (void *)pStat);

  FLT_ERR_JRET(pStat->code);

  int32_t nodeNum = taosArrayGetSize(pStat->nodeList);
  for (int32_t i = 0; i < nodeNum; ++i) {
    SValueNode *valueNode = *(SValueNode **)taosArrayGet(pStat->nodeList, i);
    
    fltConvertToTsValueNode(pStat, valueNode);
  }

_return:

  taosArrayDestroy(pStat->nodeList);
  FLT_RET(code);
}

int32_t fltOptimizeNodes(SFilterInfo *pInfo, SNode** pNode, SFltTreeStat *pStat) {
  //TODO
  return TSDB_CODE_SUCCESS;
}


int32_t fltGetDataFromColId(void *param, int32_t id, void **data) {
  int32_t numOfCols = ((SFilterColumnParam *)param)->numOfCols;
  SArray* pDataBlock = ((SFilterColumnParam *)param)->pDataBlock;
  
  for (int32_t j = 0; j < numOfCols; ++j) {
    SColumnInfoData* pColInfo = taosArrayGet(pDataBlock, j);
    if (id == pColInfo->info.colId) {
      *data = pColInfo;
      break;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t fltGetDataFromSlotId(void *param, int32_t id, void **data) {
  int32_t numOfCols = ((SFilterColumnParam *)param)->numOfCols;
  SArray* pDataBlock = ((SFilterColumnParam *)param)->pDataBlock;
  if (id < 0 || id >= numOfCols || id >= taosArrayGetSize(pDataBlock)) {
    fltError("invalid slot id, id:%d, numOfCols:%d, arraySize:%d", id, numOfCols, (int32_t)taosArrayGetSize(pDataBlock));
    return TSDB_CODE_QRY_APP_ERROR;
  }
  
  SColumnInfoData* pColInfo = taosArrayGet(pDataBlock, id);
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



int32_t filterInitFromNode(SNode* pNode, SFilterInfo **pInfo, uint32_t options) {
  int32_t code = 0;
  SFilterInfo *info = NULL;
  
  if (pNode == NULL || pInfo == NULL) {
    fltError("invalid param");
    FLT_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
  }

  if (*pInfo == NULL) {
    *pInfo = taosMemoryCalloc(1, sizeof(SFilterInfo));
    if (NULL == *pInfo) {
      fltError("taosMemoryCalloc %d failed", (int32_t)sizeof(SFilterInfo));
      FLT_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
    }
  }

  info = *pInfo;
  info->options = options;

  SFltTreeStat stat = {0};
  stat.precision = -1;
  stat.info = info;
  
  FLT_ERR_JRET(fltReviseNodes(info, &pNode, &stat));

  info->scalarMode = stat.scalarMode;

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

bool filterExecute(SFilterInfo *info, SSDataBlock *pSrc, int8_t** p, SColumnDataAgg *statis, int16_t numOfCols) {
  if (NULL == info) {
    return false;
  }

  if (info->scalarMode) {
    SScalarParam output = {0};

    SDataType type = {.type = TSDB_DATA_TYPE_BOOL, .bytes = sizeof(bool)};
    output.columnData = createColumnInfoData(&type, pSrc->info.rows);

    SArray *pList = taosArrayInit(1, POINTER_BYTES);
    taosArrayPush(pList, &pSrc);

    FLT_ERR_RET(scalarCalculate(info->sclCtx.node, pList, &output));
    *p = (int8_t *)output.columnData->pData;

    taosArrayDestroy(pList);
    return false;
  }

  return (*info->func)(info, pSrc->info.rows, p, statis, numOfCols);
}



