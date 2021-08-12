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
#include "queryLog.h"
#include "qFilter.h"
#include "tcompare.h"
#include "hash.h"
#include "tscUtil.h"

OptrStr gOptrStr[] = {
  {TSDB_RELATION_INVALID,                  "invalid"},
  {TSDB_RELATION_LESS,                     "<"},
  {TSDB_RELATION_GREATER,                  ">"},
  {TSDB_RELATION_EQUAL,                    "="},
  {TSDB_RELATION_LESS_EQUAL,               "<="},
  {TSDB_RELATION_GREATER_EQUAL,            ">="},
  {TSDB_RELATION_NOT_EQUAL,                "!="},
  {TSDB_RELATION_LIKE,                     "like"},
  {TSDB_RELATION_ISNULL,                   "is null"},
  {TSDB_RELATION_NOTNULL,                  "not null"},
  {TSDB_RELATION_IN,                       "in"},
  {TSDB_RELATION_AND,                      "and"},
  {TSDB_RELATION_OR,                       "or"},
  {TSDB_RELATION_NOT,                      "not"}
};

static FORCE_INLINE int32_t filterFieldColDescCompare(const void *desc1, const void *desc2) {
  const SSchema *sch1 = desc1;
  const SSchema *sch2 = desc2;

  return sch1->colId != sch2->colId;
}

static FORCE_INLINE int32_t filterFieldValDescCompare(const void *desc1, const void *desc2) {
  const tVariant *val1 = desc1;
  const tVariant *val2 = desc2;

  return tVariantCompare(val1, val2);
}


filter_desc_compare_func gDescCompare [FLD_TYPE_MAX] = {
  NULL,
  filterFieldColDescCompare,
  filterFieldValDescCompare
};

bool filterRangeCompGi (const void *minv, const void *maxv, const void *minr, const void *maxr, __compar_fn_t cfunc) {
  return cfunc(maxv, minr) >= 0;
}
bool filterRangeCompGe (const void *minv, const void *maxv, const void *minr, const void *maxr, __compar_fn_t cfunc) {
  return cfunc(maxv, minr) > 0;
}
bool filterRangeCompLi (const void *minv, const void *maxv, const void *minr, const void *maxr, __compar_fn_t cfunc) {
  return cfunc(minv, maxr) <= 0;
}
bool filterRangeCompLe (const void *minv, const void *maxv, const void *minr, const void *maxr, __compar_fn_t cfunc) {
  return cfunc(minv, maxr) < 0;
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
    assert(optr2 == TSDB_RELATION_LESS || optr2 == TSDB_RELATION_LESS_EQUAL);  

    if (optr == TSDB_RELATION_GREATER) {
      if (optr2 == TSDB_RELATION_LESS) {
        return 0;
      }

      return 1;
    }

    if (optr2 == TSDB_RELATION_LESS) {
      return 2;
    }

    return 3;
  } else {
    switch (optr) {
     case TSDB_RELATION_GREATER:
      return 4;
     case TSDB_RELATION_GREATER_EQUAL:
      return 5;
     case TSDB_RELATION_LESS:
      return 6;
     case TSDB_RELATION_LESS_EQUAL:
      return 7;
     default:
      break;
    }
  }

  return -1;
}

__compar_fn_t gDataCompare[] = {compareInt32Val, compareInt8Val, compareInt16Val, compareInt64Val, compareFloatVal,
  compareDoubleVal, compareLenPrefixedStr, compareStrPatternComp, compareFindItemInSet, compareWStrPatternComp, 
  compareLenPrefixedWStr, compareUint8Val, compareUint16Val, compareUint32Val, compareUint64Val,
  setCompareBytes1, setCompareBytes2, setCompareBytes4, setCompareBytes8
};

int8_t filterGetCompFuncIdx(int32_t type, int32_t optr) {
  int8_t comparFn = 0;

  if (optr == TSDB_RELATION_IN && (type != TSDB_DATA_TYPE_BINARY && type != TSDB_DATA_TYPE_NCHAR)) {
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
      if (optr == TSDB_RELATION_LIKE) { /* wildcard query using like operator */
        comparFn = 7;
      } else if (optr == TSDB_RELATION_IN) {
        comparFn = 8;
      } else { /* normal relational comparFn */
        comparFn = 6;
      }
    
      break;
    }
  
    case TSDB_DATA_TYPE_NCHAR: {
      if (optr == TSDB_RELATION_LIKE) {
        comparFn = 9;
      } else if (optr == TSDB_RELATION_IN) {
        comparFn = 8;
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


static FORCE_INLINE int32_t filterCompareGroupCtx(const void *pLeft, const void *pRight) {
  SFilterGroupCtx *left = *((SFilterGroupCtx**)pLeft), *right = *((SFilterGroupCtx**)pRight);
  if (left->colNum > right->colNum) return 1;
  if (left->colNum < right->colNum) return -1;
  return 0;
}

int32_t filterInitUnitsFields(SFilterInfo *info) {
  info->unitSize = FILTER_DEFAULT_UNIT_SIZE;
  info->units = calloc(info->unitSize, sizeof(SFilterUnit));
  
  info->fields[FLD_TYPE_COLUMN].num = 0;
  info->fields[FLD_TYPE_COLUMN].size = FILTER_DEFAULT_FIELD_SIZE;
  info->fields[FLD_TYPE_COLUMN].fields = calloc(info->fields[FLD_TYPE_COLUMN].size, COL_FIELD_SIZE);
  info->fields[FLD_TYPE_VALUE].num = 0;
  info->fields[FLD_TYPE_VALUE].size = FILTER_DEFAULT_FIELD_SIZE;
  info->fields[FLD_TYPE_VALUE].fields = calloc(info->fields[FLD_TYPE_VALUE].size, sizeof(SFilterField));

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
    r = calloc(1, sizeof(SFilterRangeNode)); 
  }

  FILTER_COPY_RA(&r->ra, ra);

  return r;
}

void* filterInitRangeCtx(int32_t type, int32_t options) {
  if (type > TSDB_DATA_TYPE_UBIGINT || type < TSDB_DATA_TYPE_BOOL || type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_NCHAR) {
    qError("not supported range type:%d", type);
    return NULL;
  }
  
  SFilterRangeCtx *ctx = calloc(1, sizeof(SFilterRangeCtx));

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

  if (optr == TSDB_RELATION_AND) {
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
      || (FILTER_GET_FLAG(ctx->status, MR_ST_ALL) && (optr == TSDB_RELATION_AND))
      || ((!FILTER_GET_FLAG(ctx->status, MR_ST_ALL)) && (optr == TSDB_RELATION_OR))) {
      APPEND_RANGE(ctx, ctx->rs, ra);
      FILTER_SET_FLAG(ctx->status, MR_ST_START);
    }

    return TSDB_CODE_SUCCESS;
  }

  SFilterRangeNode *r = ctx->rs;
  SFilterRangeNode *rn = NULL;
  int32_t cr = 0;

  if (optr == TSDB_RELATION_AND) {
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
      filterAddRangeOptr(h, TSDB_RELATION_NOTNULL, optr, NULL, &all);
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

  assert(optr == TSDB_RELATION_OR);

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

  if (FILTER_GET_FLAG(ctx->options, FI_OPTION_TIMESTAMP)) {
    SFilterRangeNode *r = ctx->rs;
    SFilterRangeNode *rn = NULL;
    
    while (r && r->next) {
      int64_t tmp = 1;
      operateVal(&tmp, &r->ra.e, &tmp, TSDB_BINARY_OP_ADD, ctx->type);
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
    FILTER_COPY_RA(ra, &r->ra);

    ++num;
    r = r->next;
    ++ra;
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
    filterAddRangeOptr(ctx, TSDB_RELATION_ISNULL, optr, empty, all);
    if (FILTER_GET_FLAG(ctx->status, MR_ST_ALL)) {
      *all = true;
    }
  }

  if (src->notnull) {
    filterAddRangeOptr(ctx, TSDB_RELATION_NOTNULL, optr, empty, all);
    if (FILTER_GET_FLAG(ctx->status, MR_ST_ALL)) {
      *all = true;
    }
  }

  if (src->isrange) {
    filterAddRangeOptr(ctx, 0, optr, empty, all);

    if (!(optr == TSDB_RELATION_OR && ctx->notnull)) {
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
    free(r);
    r = rn;
  }

  r = ctx->rf;
  while (r) {
    rn = r->next;
    free(r);
    r = rn;
  }

  free(ctx);

  return TSDB_CODE_SUCCESS;
}


int32_t filterDetachCnfGroup(SFilterGroup *gp1, SFilterGroup *gp2, SArray* group) {
  SFilterGroup gp = {0};

  gp.unitNum = gp1->unitNum + gp2->unitNum;
  gp.unitIdxs = calloc(gp.unitNum, sizeof(*gp.unitIdxs));
  memcpy(gp.unitIdxs, gp1->unitIdxs, gp1->unitNum * sizeof(*gp.unitIdxs));
  memcpy(gp.unitIdxs + gp1->unitNum, gp2->unitIdxs, gp2->unitNum * sizeof(*gp.unitIdxs));    

  gp.unitFlags = NULL;
  
  taosArrayPush(group, &gp);

  return TSDB_CODE_SUCCESS;
}


int32_t filterDetachCnfGroups(SArray* group, SArray* left, SArray* right) {
  int32_t leftSize = (int32_t)taosArrayGetSize(left);
  int32_t rightSize = (int32_t)taosArrayGetSize(right);

  CHK_LRET(taosArrayGetSize(left) <= 0, TSDB_CODE_QRY_APP_ERROR, "empty group");
  CHK_LRET(taosArrayGetSize(right) <= 0, TSDB_CODE_QRY_APP_ERROR, "empty group");  
  
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
  for (uint16_t i = 0; i < fields->num; ++i) {
    if (0 == gDescCompare[type](fields->fields[i].desc, v)) {
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


int32_t filterAddField(SFilterInfo *info, void *desc, void **data, int32_t type, SFilterFieldId *fid, int32_t dataLen, bool freeIfExists) {
  int32_t idx = -1;
  uint16_t *num;

  num = &info->fields[type].num;

  if (*num > 0) {
    if (type == FLD_TYPE_COLUMN) {
      idx = filterGetFiledByDesc(&info->fields[type], type, desc);
    } else if (data && (*data) && dataLen > 0 && FILTER_GET_FLAG(info->options, FI_OPTION_NEED_UNIQE)) {
      idx = filterGetFiledByData(info, type, *data, dataLen);
    }
  }
  
  if (idx < 0) {
    idx = *num;
    if (idx >= info->fields[type].size) {
      info->fields[type].size += FILTER_DEFAULT_FIELD_SIZE;
      info->fields[type].fields = realloc(info->fields[type].fields, info->fields[type].size * sizeof(SFilterField));
    }
    
    info->fields[type].fields[idx].flag = type;  
    info->fields[type].fields[idx].desc = desc;
    info->fields[type].fields[idx].data = data ? *data : NULL;

    if (type == FLD_TYPE_COLUMN) {
      FILTER_SET_FLAG(info->fields[type].fields[idx].flag, FLD_DATA_NO_FREE);
    }

    ++(*num);

    if (data && (*data) && dataLen > 0 && FILTER_GET_FLAG(info->options, FI_OPTION_NEED_UNIQE)) {
      if (info->pctx.valHash == NULL) {
        info->pctx.valHash = taosHashInit(FILTER_DEFAULT_GROUP_SIZE * FILTER_DEFAULT_VALUE_SIZE, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, false);
      }
      
      taosHashPut(info->pctx.valHash, *data, dataLen, &idx, sizeof(idx));
    }
  } else {
    if (freeIfExists) {
      tfree(desc);
    }

    if (data && freeIfExists) {
      tfree(*data);
    }
  }

  fid->type = type;
  fid->idx = idx;
  
  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE int32_t filterAddColFieldFromField(SFilterInfo *info, SFilterField *field, SFilterFieldId *fid) {
  filterAddField(info, field->desc, &field->data, FILTER_GET_TYPE(field->flag), fid, 0, false);

  FILTER_SET_FLAG(field->flag, FLD_DESC_NO_FREE);
  FILTER_SET_FLAG(field->flag, FLD_DATA_NO_FREE);

  return TSDB_CODE_SUCCESS;
}


int32_t filterAddFieldFromNode(SFilterInfo *info, tExprNode *node, SFilterFieldId *fid) {
  CHK_LRET(node == NULL, TSDB_CODE_QRY_APP_ERROR, "empty node");
  CHK_RET(node->nodeType != TSQL_NODE_COL && node->nodeType != TSQL_NODE_VALUE, TSDB_CODE_QRY_APP_ERROR);
  
  int32_t type;
  void *v;

  if (node->nodeType == TSQL_NODE_COL) {
    type = FLD_TYPE_COLUMN;
    v = node->pSchema;
    node->pSchema = NULL;
  } else {
    type = FLD_TYPE_VALUE;
    v = node->pVal;
    node->pVal = NULL;
  }

  filterAddField(info, v, NULL, type, fid, 0, true);
  
  return TSDB_CODE_SUCCESS;
}

int32_t filterAddUnit(SFilterInfo *info, uint8_t optr, SFilterFieldId *left, SFilterFieldId *right, uint16_t *uidx) {
  if (FILTER_GET_FLAG(info->options, FI_OPTION_NEED_UNIQE)) {
    if (info->pctx.unitHash == NULL) {
      info->pctx.unitHash = taosHashInit(FILTER_DEFAULT_GROUP_SIZE * FILTER_DEFAULT_UNIT_SIZE, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, false);
    } else {
      int64_t v = 0;
      FILTER_PACKAGE_UNIT_HASH_KEY(&v, optr, left->idx, right ? right->idx : -1);
      void *hu = taosHashGet(info->pctx.unitHash, &v, sizeof(v));
      if (hu) {
        *uidx = *(uint16_t *)hu;
        return TSDB_CODE_SUCCESS;
      }
    }
  }

  if (info->unitNum >= info->unitSize) {
    uint16_t psize = info->unitSize;
    info->unitSize += FILTER_DEFAULT_UNIT_SIZE;
    info->units = realloc(info->units, info->unitSize * sizeof(SFilterUnit));
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
    assert(optr == TSDB_RELATION_ISNULL || optr == TSDB_RELATION_NOTNULL || optr == FILTER_DUMMY_EMPTY_OPTR);
  }
  
  SFilterField *col = FILTER_UNIT_LEFT_FIELD(info, u);
  assert(FILTER_GET_FLAG(col->flag, FLD_TYPE_COLUMN));
  
  info->units[info->unitNum].compare.type = FILTER_GET_COL_FIELD_TYPE(col);

  *uidx = info->unitNum;

  if (FILTER_GET_FLAG(info->options, FI_OPTION_NEED_UNIQE)) {
    int64_t v = 0;
    FILTER_PACKAGE_UNIT_HASH_KEY(&v, optr, left->idx, right ? right->idx : -1);  
    taosHashPut(info->pctx.unitHash, &v, sizeof(v), uidx, sizeof(*uidx));
  }
  
  ++info->unitNum;
  
  return TSDB_CODE_SUCCESS;
}



int32_t filterAddUnitToGroup(SFilterGroup *group, uint16_t unitIdx) {
  if (group->unitNum >= group->unitSize) {
    group->unitSize += FILTER_DEFAULT_UNIT_SIZE;
    group->unitIdxs = realloc(group->unitIdxs, group->unitSize * sizeof(*group->unitIdxs));
  }
  
  group->unitIdxs[group->unitNum++] = unitIdx;

  return TSDB_CODE_SUCCESS;
}

int32_t filterConvertSetFromBinary(void **q, const char *buf, int32_t len, uint32_t tType) {
  SBufferReader br = tbufInitReader(buf, len, false); 
  uint32_t sType  = tbufReadUint32(&br);     
  SHashObj *pObj = taosHashInit(256, taosGetDefaultHashFunction(tType), true, false);
  int32_t code = 0;
  
  taosHashSetEqualFp(pObj, taosGetDefaultEqualFunction(tType)); 
  
  int dummy = -1;
  tVariant tmpVar = {0};  
  size_t  t = 0;
  int32_t sz = tbufReadInt32(&br);
  void *pvar = NULL;  
  int64_t val = 0;
  int32_t bufLen = 0;
  if (IS_NUMERIC_TYPE(sType)) {
    bufLen = 60;  // The maximum length of string that a number is converted to.
  } else {
    bufLen = 128;
  }

  char *tmp = calloc(1, bufLen * TSDB_NCHAR_SIZE);
    
  for (int32_t i = 0; i < sz; i++) {
    switch (sType) {
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_UTINYINT:
    case TSDB_DATA_TYPE_TINYINT: {
      *(uint8_t *)&val = (uint8_t)tbufReadInt64(&br); 
      t = sizeof(val);
      pvar = &val;
      break;
    }
    case TSDB_DATA_TYPE_USMALLINT:
    case TSDB_DATA_TYPE_SMALLINT: {
      *(uint16_t *)&val = (uint16_t)tbufReadInt64(&br); 
      t = sizeof(val);
      pvar = &val;
      break;
    }
    case TSDB_DATA_TYPE_UINT:
    case TSDB_DATA_TYPE_INT: {
      *(uint32_t *)&val = (uint32_t)tbufReadInt64(&br); 
      t = sizeof(val);
      pvar = &val;
      break;
    }
    case TSDB_DATA_TYPE_TIMESTAMP:
    case TSDB_DATA_TYPE_UBIGINT:
    case TSDB_DATA_TYPE_BIGINT: {
      *(uint64_t *)&val = (uint64_t)tbufReadInt64(&br); 
      t = sizeof(val);
      pvar = &val;
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      *(double *)&val = tbufReadDouble(&br);
      t = sizeof(val);
      pvar = &val;
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      *(float *)&val = (float)tbufReadDouble(&br);
      t = sizeof(val);
      pvar = &val;
      break;
    }
    case TSDB_DATA_TYPE_BINARY: {
      pvar = (char *)tbufReadBinary(&br, &t);
      break;
    }
    case TSDB_DATA_TYPE_NCHAR: {
      pvar = (char *)tbufReadBinary(&br, &t);      
      break;
    }
    default:
      taosHashCleanup(pObj);
      *q = NULL;
      assert(0);
    }
    
    tVariantCreateFromBinary(&tmpVar, (char *)pvar, t, sType);

    if (bufLen < t) {
      tmp = realloc(tmp, t * TSDB_NCHAR_SIZE);
      bufLen = (int32_t)t;
    }

    bool converted = false;
    char extInfo = 0;

    switch (tType) {
      case TSDB_DATA_TYPE_BOOL:
      case TSDB_DATA_TYPE_UTINYINT:
      case TSDB_DATA_TYPE_TINYINT: {
        if (tVariantDumpEx(&tmpVar, (char *)&val, tType, false, &converted, &extInfo)) {
          if (converted) {
            tVariantDestroy(&tmpVar);
            memset(&tmpVar, 0, sizeof(tmpVar));
            continue;
          }
                  
          goto _return;
        }
        pvar = &val;
        t = sizeof(val);
        break;
      }
      case TSDB_DATA_TYPE_USMALLINT:
      case TSDB_DATA_TYPE_SMALLINT: {
        if (tVariantDumpEx(&tmpVar, (char *)&val, tType, false, &converted, &extInfo)) {
          if (converted) {
            tVariantDestroy(&tmpVar);
            memset(&tmpVar, 0, sizeof(tmpVar));
            continue;
          }
                  
          goto _return;
        }
        pvar = &val;
        t = sizeof(val);
        break;
      }
      case TSDB_DATA_TYPE_UINT:
      case TSDB_DATA_TYPE_INT: {
        if (tVariantDumpEx(&tmpVar, (char *)&val, tType, false, &converted, &extInfo)) {
          if (converted) {
            tVariantDestroy(&tmpVar);
            memset(&tmpVar, 0, sizeof(tmpVar));
            continue;
          }
                  
          goto _return;
        }
        pvar = &val;
        t = sizeof(val);
        break;
      }
      case TSDB_DATA_TYPE_TIMESTAMP:
      case TSDB_DATA_TYPE_UBIGINT:
      case TSDB_DATA_TYPE_BIGINT: {
        if (tVariantDump(&tmpVar, (char *)&val, tType, false)) {
          goto _return;
        }
        pvar = &val;
        t = sizeof(val);
        break;
      }
      case TSDB_DATA_TYPE_DOUBLE: {
        if (tVariantDump(&tmpVar, (char *)&val, tType, false)) {
          goto _return;
        }
        pvar = &val;
        t = sizeof(val);
        break;
      }
      case TSDB_DATA_TYPE_FLOAT: {
        if (tVariantDumpEx(&tmpVar, (char *)&val, tType, false, &converted, &extInfo)) {
          if (converted) {
            tVariantDestroy(&tmpVar);
            memset(&tmpVar, 0, sizeof(tmpVar));
            continue;
          }
          
          goto _return;
        }
        pvar = &val;
        t = sizeof(val);
        break;
      }
      case TSDB_DATA_TYPE_BINARY: {
        if (tVariantDump(&tmpVar, tmp, tType, true)) {
          goto _return;
        }
        t = varDataLen(tmp);
        pvar = varDataVal(tmp);
        break;
      }
      case TSDB_DATA_TYPE_NCHAR: {
        if (tVariantDump(&tmpVar, tmp, tType, true)) {
          goto _return;
        }
        t = varDataLen(tmp);
        pvar = varDataVal(tmp);        
        break;
      }
      default:
        goto _return;
    }
    
    taosHashPut(pObj, (char *)pvar, t,  &dummy, sizeof(dummy));
    tVariantDestroy(&tmpVar);
    memset(&tmpVar, 0, sizeof(tmpVar));
  }

  *q = (void *)pObj;
  pObj = NULL;
  
_return:  
  tVariantDestroy(&tmpVar);
  taosHashCleanup(pObj);
  tfree(tmp);
  
  return code;
}



int32_t filterAddGroupUnitFromNode(SFilterInfo *info, tExprNode* tree, SArray *group) {
  SFilterFieldId left = {0}, right = {0};

  filterAddFieldFromNode(info, tree->_node.pLeft, &left);  

  tVariant* var = tree->_node.pRight->pVal;
  int32_t type = FILTER_GET_COL_FIELD_TYPE(FILTER_GET_FIELD(info, left));
  int32_t len = 0;
  uint16_t uidx = 0;

  if (tree->_node.optr == TSDB_RELATION_IN && (!IS_VAR_DATA_TYPE(type))) {
    void *data = NULL;
    filterConvertSetFromBinary((void **)&data, var->pz, var->nLen, type);
    CHK_LRET(data == NULL, TSDB_CODE_QRY_APP_ERROR, "failed to convert in param");

    if (taosHashGetSize((SHashObj *)data) <= 0) {
      filterAddUnit(info, FILTER_DUMMY_EMPTY_OPTR, &left, NULL, &uidx);
      
      SFilterGroup fgroup = {0};
      filterAddUnitToGroup(&fgroup, uidx);
      
      taosArrayPush(group, &fgroup);
      taosHashCleanup(data);

      return TSDB_CODE_SUCCESS;
    }

    void *p = taosHashIterate((SHashObj *)data, NULL);
    while(p) {
      void *key = taosHashGetDataKey((SHashObj *)data, p);
      void *fdata = NULL;
    
      if (IS_VAR_DATA_TYPE(type)) {
        len = (int32_t)taosHashGetDataKeyLen((SHashObj *)data, p);
        fdata = malloc(len + VARSTR_HEADER_SIZE);
        varDataLen(fdata) = len;
        memcpy(varDataVal(fdata), key, len);
        len += VARSTR_HEADER_SIZE;
      } else {
        fdata = malloc(sizeof(int64_t));
        SIMPLE_COPY_VALUES(fdata, key);
        len = tDataTypes[type].bytes;
      }
      
      filterAddField(info, NULL, &fdata, FLD_TYPE_VALUE, &right, len, true);

      filterAddUnit(info, TSDB_RELATION_EQUAL, &left, &right, &uidx);
      
      SFilterGroup fgroup = {0};
      filterAddUnitToGroup(&fgroup, uidx);
      
      taosArrayPush(group, &fgroup);
      
      p = taosHashIterate((SHashObj *)data, p);
    }

    taosHashCleanup(data);
  } else {
    filterAddFieldFromNode(info, tree->_node.pRight, &right);  
    
    filterAddUnit(info, tree->_node.optr, &left, &right, &uidx);  

    SFilterGroup fgroup = {0};
    filterAddUnitToGroup(&fgroup, uidx);
    
    taosArrayPush(group, &fgroup);
  }
  
  return TSDB_CODE_SUCCESS;
}


int32_t filterAddUnitFromUnit(SFilterInfo *dst, SFilterInfo *src, SFilterUnit* u, uint16_t *uidx) {
  SFilterFieldId left, right, *pright = &right;
  int32_t type = FILTER_UNIT_DATA_TYPE(u);
  uint16_t flag = FLD_DESC_NO_FREE;

  filterAddField(dst, FILTER_UNIT_COL_DESC(src, u), NULL, FLD_TYPE_COLUMN, &left, 0, false);
  SFilterField *t = FILTER_UNIT_LEFT_FIELD(src, u);
  FILTER_SET_FLAG(t->flag, flag);
  
  if (u->right.type == FLD_TYPE_VALUE) {
    void *data = FILTER_UNIT_VAL_DATA(src, u);
    if (IS_VAR_DATA_TYPE(type)) {
      if (FILTER_UNIT_OPTR(u) ==  TSDB_RELATION_IN) {
        filterAddField(dst, NULL, &data, FLD_TYPE_VALUE, &right, 0, false);

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

int32_t filterAddUnitRight(SFilterInfo *info, uint8_t optr, SFilterFieldId *right, uint16_t uidx) {
  SFilterUnit *u = &info->units[uidx];
  
  u->compare.optr2 = optr;
  u->right2 = *right;

  return TSDB_CODE_SUCCESS;
}


int32_t filterAddGroupUnitFromCtx(SFilterInfo *dst, SFilterInfo *src, SFilterRangeCtx *ctx, uint16_t cidx, SFilterGroup *g, int32_t optr, SArray *res) {
  SFilterFieldId left, right, right2;
  uint16_t uidx = 0;

  SFilterField *col = FILTER_GET_COL_FIELD(src, cidx);

  filterAddColFieldFromField(dst, col, &left);

  int32_t type = FILTER_GET_COL_FIELD_TYPE(FILTER_GET_FIELD(dst, left));

  if (optr == TSDB_RELATION_AND) {
    if (ctx->isnull) {
      assert(ctx->notnull == false && ctx->isrange == false);
      filterAddUnit(dst, TSDB_RELATION_ISNULL, &left, NULL, &uidx);
      filterAddUnitToGroup(g, uidx);
      return TSDB_CODE_SUCCESS;
    }

    if (ctx->notnull) {      
      assert(ctx->isnull == false && ctx->isrange == false);
      filterAddUnit(dst, TSDB_RELATION_NOTNULL, &left, NULL, &uidx);
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
        void *data = malloc(sizeof(int64_t));
        SIMPLE_COPY_VALUES(data, &ra->s);
        filterAddField(dst, NULL, &data, FLD_TYPE_VALUE, &right, tDataTypes[type].bytes, true);
        filterAddUnit(dst, TSDB_RELATION_EQUAL, &left, &right, &uidx);
        filterAddUnitToGroup(g, uidx);
        return TSDB_CODE_SUCCESS;              
      } else {
        void *data = malloc(sizeof(int64_t));
        SIMPLE_COPY_VALUES(data, &ra->s);
        filterAddField(dst, NULL, &data, FLD_TYPE_VALUE, &right, tDataTypes[type].bytes, true);
        void *data2 = malloc(sizeof(int64_t));
        SIMPLE_COPY_VALUES(data2, &ra->e);
        filterAddField(dst, NULL, &data2, FLD_TYPE_VALUE, &right2, tDataTypes[type].bytes, true);
        
        filterAddUnit(dst, FILTER_GET_FLAG(ra->sflag, RANGE_FLG_EXCLUDE) ? TSDB_RELATION_GREATER : TSDB_RELATION_GREATER_EQUAL, &left, &right, &uidx);
        filterAddUnitRight(dst, FILTER_GET_FLAG(ra->eflag, RANGE_FLG_EXCLUDE) ? TSDB_RELATION_LESS : TSDB_RELATION_LESS_EQUAL, &right2, uidx);
        filterAddUnitToGroup(g, uidx);
        return TSDB_CODE_SUCCESS;              
      }
    }
    
    if (!FILTER_GET_FLAG(ra->sflag, RANGE_FLG_NULL)) {
      void *data = malloc(sizeof(int64_t));
      SIMPLE_COPY_VALUES(data, &ra->s);
      filterAddField(dst, NULL, &data, FLD_TYPE_VALUE, &right, tDataTypes[type].bytes, true);
      filterAddUnit(dst, FILTER_GET_FLAG(ra->sflag, RANGE_FLG_EXCLUDE) ? TSDB_RELATION_GREATER : TSDB_RELATION_GREATER_EQUAL, &left, &right, &uidx);
      filterAddUnitToGroup(g, uidx);
    }

    if (!FILTER_GET_FLAG(ra->eflag, RANGE_FLG_NULL)) {
      void *data = malloc(sizeof(int64_t));
      SIMPLE_COPY_VALUES(data, &ra->e);
      filterAddField(dst, NULL, &data, FLD_TYPE_VALUE, &right, tDataTypes[type].bytes, true);
      filterAddUnit(dst, FILTER_GET_FLAG(ra->eflag, RANGE_FLG_EXCLUDE) ? TSDB_RELATION_LESS : TSDB_RELATION_LESS_EQUAL, &left, &right, &uidx);
      filterAddUnitToGroup(g, uidx);
    }    

    return TSDB_CODE_SUCCESS;      
  } 

  // OR PROCESS
  
  SFilterGroup ng = {0};
  g = &ng;

  assert(ctx->isnull || ctx->notnull || ctx->isrange);
  
  if (ctx->isnull) {
    filterAddUnit(dst, TSDB_RELATION_ISNULL, &left, NULL, &uidx);
    filterAddUnitToGroup(g, uidx);    
    taosArrayPush(res, g);
  }
  
  if (ctx->notnull) {
    assert(!ctx->isrange);
    memset(g, 0, sizeof(*g));
    
    filterAddUnit(dst, TSDB_RELATION_NOTNULL, &left, NULL, &uidx);
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
        void *data = malloc(sizeof(int64_t));
        SIMPLE_COPY_VALUES(data, &r->ra.s);
        filterAddField(dst, NULL, &data, FLD_TYPE_VALUE, &right, tDataTypes[type].bytes, true);
        filterAddUnit(dst, TSDB_RELATION_EQUAL, &left, &right, &uidx);
        filterAddUnitToGroup(g, uidx);
      } else {
        void *data = malloc(sizeof(int64_t));
        SIMPLE_COPY_VALUES(data, &r->ra.s);
        filterAddField(dst, NULL, &data, FLD_TYPE_VALUE, &right, tDataTypes[type].bytes, true);
        void *data2 = malloc(sizeof(int64_t));
        SIMPLE_COPY_VALUES(data2, &r->ra.e);
        filterAddField(dst, NULL, &data2, FLD_TYPE_VALUE, &right2, tDataTypes[type].bytes, true);
        
        filterAddUnit(dst, FILTER_GET_FLAG(r->ra.sflag, RANGE_FLG_EXCLUDE) ? TSDB_RELATION_GREATER : TSDB_RELATION_GREATER_EQUAL, &left, &right, &uidx);
        filterAddUnitRight(dst, FILTER_GET_FLAG(r->ra.eflag, RANGE_FLG_EXCLUDE) ? TSDB_RELATION_LESS : TSDB_RELATION_LESS_EQUAL, &right2, uidx);
        filterAddUnitToGroup(g, uidx);
      }

      taosArrayPush(res, g);
      
      r = r->next;
      
      continue;
    }
    
    if (!FILTER_GET_FLAG(r->ra.sflag, RANGE_FLG_NULL)) {
      void *data = malloc(sizeof(int64_t));
      SIMPLE_COPY_VALUES(data, &r->ra.s);
      filterAddField(dst, NULL, &data, FLD_TYPE_VALUE, &right, tDataTypes[type].bytes, true);
      filterAddUnit(dst, FILTER_GET_FLAG(r->ra.sflag, RANGE_FLG_EXCLUDE) ? TSDB_RELATION_GREATER : TSDB_RELATION_GREATER_EQUAL, &left, &right, &uidx);
      filterAddUnitToGroup(g, uidx);
    }
    
    if (!FILTER_GET_FLAG(r->ra.eflag, RANGE_FLG_NULL)) {
      void *data = malloc(sizeof(int64_t));
      SIMPLE_COPY_VALUES(data, &r->ra.e);    
      filterAddField(dst, NULL, &data, FLD_TYPE_VALUE, &right, tDataTypes[type].bytes, true);
      filterAddUnit(dst, FILTER_GET_FLAG(r->ra.eflag, RANGE_FLG_EXCLUDE) ? TSDB_RELATION_LESS : TSDB_RELATION_LESS_EQUAL, &left, &right, &uidx);
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
  tfree(p->unitIdxs);
  tfree(p->unitFlags);
}


int32_t filterTreeToGroup(tExprNode* tree, SFilterInfo *info, SArray* group) {
  int32_t code = TSDB_CODE_SUCCESS;
  SArray* leftGroup = NULL;
  SArray* rightGroup = NULL;
  
  if (tree->nodeType != TSQL_NODE_EXPR) {
    qError("invalid nodeType:%d", tree->nodeType); 
    return TSDB_CODE_QRY_APP_ERROR;
  }
  
  if (tree->_node.optr == TSDB_RELATION_AND) {
    leftGroup = taosArrayInit(4, sizeof(SFilterGroup));
    rightGroup = taosArrayInit(4, sizeof(SFilterGroup));
    ERR_JRET(filterTreeToGroup(tree->_node.pLeft, info, leftGroup));
    ERR_JRET(filterTreeToGroup(tree->_node.pRight, info, rightGroup));

    ERR_JRET(filterDetachCnfGroups(group, leftGroup, rightGroup));

    taosArrayDestroyEx(leftGroup, filterFreeGroup);
    taosArrayDestroyEx(rightGroup, filterFreeGroup);
    
    return TSDB_CODE_SUCCESS;
  }

  if (tree->_node.optr == TSDB_RELATION_OR) {
    ERR_RET(filterTreeToGroup(tree->_node.pLeft, info, group));
    ERR_RET(filterTreeToGroup(tree->_node.pRight, info, group));

    return TSDB_CODE_SUCCESS;
  }

  code = filterAddGroupUnitFromNode(info, tree, group);  


_return:

  taosArrayDestroyEx(leftGroup, filterFreeGroup);
  taosArrayDestroyEx(rightGroup, filterFreeGroup);
  
  return code;
}

#if 0
int32_t filterInitUnitFunc(SFilterInfo *info) {
  for (uint16_t i = 0; i < info->unitNum; ++i) {
    SFilterUnit* unit = &info->units[i];
    
    info->cunits[i].func = getComparFunc(FILTER_UNIT_DATA_TYPE(unit), unit->compare.optr);
  }

  return TSDB_CODE_SUCCESS;
}
#endif


void filterDumpInfoToString(SFilterInfo *info, const char *msg, int32_t options) {
  if (qDebugFlag & DEBUG_DEBUG) {
    CHK_LRETV(info == NULL, "%s - FilterInfo: EMPTY", msg);

    if (options == 0) {
      qDebug("%s - FilterInfo:", msg);
      qDebug("COLUMN Field Num:%u", info->fields[FLD_TYPE_COLUMN].num);
      for (uint16_t i = 0; i < info->fields[FLD_TYPE_COLUMN].num; ++i) {
        SFilterField *field = &info->fields[FLD_TYPE_COLUMN].fields[i];
        SSchema *sch = field->desc;
        qDebug("COL%d => [%d][%s]", i, sch->colId, sch->name);
      }

      qDebug("VALUE Field Num:%u", info->fields[FLD_TYPE_VALUE].num);
      for (uint16_t i = 0; i < info->fields[FLD_TYPE_VALUE].num; ++i) {
        SFilterField *field = &info->fields[FLD_TYPE_VALUE].fields[i];
        if (field->desc) {
          tVariant *var = field->desc;
          if (var->nType == TSDB_DATA_TYPE_VALUE_ARRAY) {
            qDebug("VAL%d => [type:TS][val:[%" PRIi64"] - [%" PRId64 "]]", i, *(int64_t *)field->data, *(((int64_t *)field->data) + 1)); 
          } else {
            qDebug("VAL%d => [type:%d][val:%" PRIx64"]", i, var->nType, var->i64); //TODO
          }
        } else if (field->data) {
          qDebug("VAL%d => [type:NIL][val:NIL]", i); //TODO
        }
      }

      qDebug("UNIT  Num:%u", info->unitNum);
      for (uint16_t i = 0; i < info->unitNum; ++i) {
        SFilterUnit *unit = &info->units[i];
        int32_t type = FILTER_UNIT_DATA_TYPE(unit);
        int32_t len = 0;
        int32_t tlen = 0;
        char str[256] = {0};
        
        SFilterField *left = FILTER_UNIT_LEFT_FIELD(info, unit);
        SSchema *sch = left->desc;
        len = sprintf(str, "UNIT[%d] => [%d][%s]  %s  [", i, sch->colId, sch->name, gOptrStr[unit->compare.optr].str);

        if (unit->right.type == FLD_TYPE_VALUE && FILTER_UNIT_OPTR(unit) != TSDB_RELATION_IN) {
          SFilterField *right = FILTER_UNIT_RIGHT_FIELD(info, unit);
          char *data = right->data;
          if (IS_VAR_DATA_TYPE(type)) {
            tlen = varDataLen(data);
            data += VARSTR_HEADER_SIZE;
          }
          converToStr(str + len, type, data, tlen > 32 ? 32 : tlen, &tlen);
        } else {
          strcat(str, "NULL");
        }
        strcat(str, "]");
        
        qDebug("%s", str); //TODO
      }

      qDebug("GROUP Num:%u", info->groupNum);
      for (uint16_t i = 0; i < info->groupNum; ++i) {
        SFilterGroup *group = &info->groups[i];
        qDebug("Group%d : unit num[%u]", i, group->unitNum);

        for (uint16_t u = 0; u < group->unitNum; ++u) {
          qDebug("unit id:%u", group->unitIdxs[u]);
        }
      }

      return;
    }

    qDebug("%s - RANGE info:", msg);

    qDebug("RANGE Num:%u", info->colRangeNum);
    for (uint16_t i = 0; i < info->colRangeNum; ++i) {
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
            converToStr(str + strlen(str), ctx->type, &r->ra.s, tlen > 32 ? 32 : tlen, &tlen);
            FILTER_GET_FLAG(r->ra.sflag, RANGE_FLG_EXCLUDE) ? strcat(str,")") : strcat(str,"]");
          }
          strcat(str, " - ");
          if (FILTER_GET_FLAG(r->ra.eflag, RANGE_FLG_NULL)) {
            strcat(str, "(NULL)");
          } else {
            FILTER_GET_FLAG(r->ra.eflag, RANGE_FLG_EXCLUDE) ? strcat(str,"(") : strcat(str,"[");
            converToStr(str + strlen(str), ctx->type, &r->ra.e, tlen > 32 ? 32 : tlen, &tlen);
            FILTER_GET_FLAG(r->ra.eflag, RANGE_FLG_EXCLUDE) ? strcat(str,")") : strcat(str,"]");
          }
          qDebug("range: %s", str);        
          
          r = r->next;
        }
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

  tfree(gRes->colIdx);

  int16_t i = 0, j = 0;

  while (i < gRes->colNum) {
    if (gRes->colInfo[j].info) {
      filterFreeColInfo(&gRes->colInfo[j]);
      ++i;
    }

    ++j;
  }

  tfree(gRes->colInfo);
  tfree(gRes);
}

void filterFreeField(SFilterField* field, int32_t type) {
  if (field == NULL) {
    return;
  }

  if (!FILTER_GET_FLAG(field->flag, FLD_DESC_NO_FREE)) {
    if (type == FLD_TYPE_VALUE) {
      tVariantDestroy(field->desc);
    }
    
    tfree(field->desc);
  }

  if (!FILTER_GET_FLAG(field->flag, FLD_DATA_NO_FREE)) {
    if (FILTER_GET_FLAG(field->flag, FLD_DATA_IS_HASH)) {
      taosHashCleanup(field->data);
    } else {
      tfree(field->data);
    }
  }
}

void filterFreePCtx(SFilterPCtx *pctx) {
  taosHashCleanup(pctx->valHash);
  taosHashCleanup(pctx->unitHash);
}

void filterFreeInfo(SFilterInfo *info) {
  CHK_RETV(info == NULL);

  tfree(info->cunits);

  for (int32_t i = 0; i < FLD_TYPE_MAX; ++i) {
    for (uint16_t f = 0; f < info->fields[i].num; ++f) {
      filterFreeField(&info->fields[i].fields[f], i);
    }
    
    tfree(info->fields[i].fields);
  }

  for (int32_t i = 0; i < info->groupNum; ++i) {
    filterFreeGroup(&info->groups[i]);    
  }
  
  tfree(info->groups);

  tfree(info->units);

  tfree(info->unitRes);

  tfree(info->unitFlags);

  for (uint16_t i = 0; i < info->colRangeNum; ++i) {
    filterFreeRangeCtx(info->colRange[i]);
  }

  tfree(info->colRange);

  filterFreePCtx(&info->pctx);

  if (!FILTER_GET_FLAG(info->status, FI_STATUS_CLONED)) {
    tfree(info);
  }
}

int32_t filterHandleValueExtInfo(SFilterUnit* unit, char extInfo) {
  assert(extInfo > 0 || extInfo < 0);
  
  uint8_t optr = FILTER_UNIT_OPTR(unit);
  switch (optr) {
    case TSDB_RELATION_GREATER:
    case TSDB_RELATION_GREATER_EQUAL:
      unit->compare.optr = (extInfo > 0) ? FILTER_DUMMY_EMPTY_OPTR : TSDB_RELATION_NOTNULL;
      break;
    case TSDB_RELATION_LESS:
    case TSDB_RELATION_LESS_EQUAL:
      unit->compare.optr = (extInfo > 0) ? TSDB_RELATION_NOTNULL : FILTER_DUMMY_EMPTY_OPTR;
      break;
    case TSDB_RELATION_EQUAL:
      unit->compare.optr = FILTER_DUMMY_EMPTY_OPTR;
      break;
    default:
      assert(0);
  }

  return TSDB_CODE_SUCCESS;
}


int32_t filterInitValFieldData(SFilterInfo *info) {
  for (uint16_t i = 0; i < info->unitNum; ++i) {
    SFilterUnit* unit = &info->units[i];
    if (unit->right.type != FLD_TYPE_VALUE) {
      assert(unit->compare.optr == TSDB_RELATION_ISNULL || unit->compare.optr == TSDB_RELATION_NOTNULL || unit->compare.optr == FILTER_DUMMY_EMPTY_OPTR);
      continue;
    }
    
    SFilterField* right = FILTER_UNIT_RIGHT_FIELD(info, unit);

    assert(FILTER_GET_FLAG(right->flag, FLD_TYPE_VALUE));

    uint32_t type = FILTER_UNIT_DATA_TYPE(unit);
    SFilterField* fi = right;
    
    tVariant* var = fi->desc;

    if (var == NULL) {
      assert(fi->data != NULL);
      continue;
    }

    if (unit->compare.optr == TSDB_RELATION_IN) {
      filterConvertSetFromBinary((void **)&fi->data, var->pz, var->nLen, type);
      CHK_LRET(fi->data == NULL, TSDB_CODE_QRY_APP_ERROR, "failed to convert in param");

      FILTER_SET_FLAG(fi->flag, FLD_DATA_IS_HASH);
      
      continue;
    }

    if (type == TSDB_DATA_TYPE_BINARY) {
      size_t len = (var->nType == TSDB_DATA_TYPE_BINARY || var->nType == TSDB_DATA_TYPE_NCHAR) ? var->nLen : MAX_NUM_STR_SIZE;
      fi->data = calloc(1, len + 1 + VARSTR_HEADER_SIZE);
    } else if (type == TSDB_DATA_TYPE_NCHAR) {
      size_t len = (var->nType == TSDB_DATA_TYPE_BINARY || var->nType == TSDB_DATA_TYPE_NCHAR) ? var->nLen : MAX_NUM_STR_SIZE;    
      fi->data = calloc(1, (len + 1) * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE);
    } else {
      if (var->nType == TSDB_DATA_TYPE_VALUE_ARRAY) {  //TIME RANGE
        fi->data = calloc(var->nLen, tDataTypes[type].bytes);
        for (int32_t a = 0; a < var->nLen; ++a) {
          int64_t *v = taosArrayGet(var->arr, a);
          assignVal((char *)fi->data + a * tDataTypes[type].bytes, (char *)v, 0, type);
        }

        continue;
      } else {
        fi->data = calloc(1, sizeof(int64_t));
      }
    }

    bool converted = false;
    char extInfo = 0;
    if (tVariantDumpEx(var, (char*)fi->data, type, true, &converted, &extInfo)) {
      if (converted) {
        filterHandleValueExtInfo(unit, extInfo);
        
        continue;
      }
      qError("dump value to type[%d] failed", type);
      return TSDB_CODE_TSC_INVALID_OPERATION;
    }
  }

  return TSDB_CODE_SUCCESS;
}


bool filterDoCompare(__compar_fn_t func, uint8_t optr, void *left, void *right) {
  int32_t ret = func(left, right);

  switch (optr) {
    case TSDB_RELATION_EQUAL: {
      return ret == 0;
    }
    case TSDB_RELATION_NOT_EQUAL: {
      return ret != 0;
    }
    case TSDB_RELATION_GREATER_EQUAL: {
      return ret >= 0;
    }
    case TSDB_RELATION_GREATER: {
      return ret > 0;
    }
    case TSDB_RELATION_LESS_EQUAL: {
      return ret <= 0;
    }
    case TSDB_RELATION_LESS: {
      return ret < 0;
    }
    case TSDB_RELATION_LIKE: {
      return ret == 0;
    }
    case TSDB_RELATION_IN: {
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
    case TSDB_RELATION_GREATER:
      SIMPLE_COPY_VALUES(&ra.s, val);
      FILTER_SET_FLAG(ra.sflag, RANGE_FLG_EXCLUDE);
      FILTER_SET_FLAG(ra.eflag, RANGE_FLG_NULL);
      break;
    case TSDB_RELATION_GREATER_EQUAL:
      SIMPLE_COPY_VALUES(&ra.s, val);
      FILTER_SET_FLAG(ra.eflag, RANGE_FLG_NULL);
      break;
    case TSDB_RELATION_LESS:
      SIMPLE_COPY_VALUES(&ra.e, val);
      FILTER_SET_FLAG(ra.eflag, RANGE_FLG_EXCLUDE);
      FILTER_SET_FLAG(ra.sflag, RANGE_FLG_NULL);
      break;
    case TSDB_RELATION_LESS_EQUAL:
      SIMPLE_COPY_VALUES(&ra.e, val);
      FILTER_SET_FLAG(ra.sflag, RANGE_FLG_NULL);
      break;
    case TSDB_RELATION_NOT_EQUAL:
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
    case TSDB_RELATION_EQUAL:
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
  CHK_JMP(ctx1->status != ctx2->status);
  CHK_JMP(ctx1->isnull != ctx2->isnull);
  CHK_JMP(ctx1->notnull != ctx2->notnull);
  CHK_JMP(ctx1->isrange != ctx2->isrange);

  SFilterRangeNode *r1 = ctx1->rs;
  SFilterRangeNode *r2 = ctx2->rs;
  
  while (r1 && r2) {
    CHK_JMP(r1->ra.sflag != r2->ra.sflag);
    CHK_JMP(r1->ra.eflag != r2->ra.eflag);
    CHK_JMP(r1->ra.s != r2->ra.s);
    CHK_JMP(r1->ra.e != r2->ra.e);
    
    r1 = r1->next;
    r2 = r2->next;
  }

  CHK_JMP(r1 != r2);

  *equal = true;

  return TSDB_CODE_SUCCESS;

_return:
  *equal = false;
  return TSDB_CODE_SUCCESS;
}


int32_t filterMergeUnits(SFilterInfo *info, SFilterGroupCtx* gRes, uint16_t colIdx, bool *empty) {
  SArray* colArray = (SArray *)gRes->colInfo[colIdx].info;
  int32_t size = (int32_t)taosArrayGetSize(colArray);
  int32_t type = gRes->colInfo[colIdx].dataType;
  SFilterRangeCtx* ctx = filterInitRangeCtx(type, 0);
  
  for (uint32_t i = 0; i < size; ++i) {
    SFilterUnit* u = taosArrayGetP(colArray, i);
    uint8_t optr = FILTER_UNIT_OPTR(u);

    filterAddRangeOptr(ctx, optr, TSDB_RELATION_AND, empty, NULL);
    CHK_JMP(*empty);

    if (!FILTER_NO_MERGE_OPTR(optr)) {
      filterAddUnitRange(info, u, ctx, TSDB_RELATION_AND);
      CHK_JMP(MR_EMPTY_RES(ctx));
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
  uint16_t *colIdx = malloc(info->fields[FLD_TYPE_COLUMN].num * sizeof(uint16_t));
  uint16_t colIdxi = 0;
  uint16_t gResIdx = 0;
  
  for (uint16_t i = 0; i < info->groupNum; ++i) {
    SFilterGroup* g = info->groups + i;

    gRes[gResIdx] = calloc(1, sizeof(SFilterGroupCtx));
    gRes[gResIdx]->colInfo = calloc(info->fields[FLD_TYPE_COLUMN].num, sizeof(SFilterColInfo));
    colIdxi = 0;
    empty = false;
    
    for (uint16_t j = 0; j < g->unitNum; ++j) {
      SFilterUnit* u = FILTER_GROUP_UNIT(info, g, j);
      uint16_t cidx = FILTER_UNIT_COL_IDX(u);

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
      qsort(colIdx, colIdxi, sizeof(uint16_t), getComparFunc(TSDB_DATA_TYPE_USMALLINT, 0));
    }

    for (uint16_t l = 0; l < colIdxi; ++l) {
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

  tfree(colIdx);

  *gResNum = gResIdx;
  
  if (gResIdx == 0) {
    FILTER_SET_FLAG(info->status, FI_STATUS_EMPTY);
  }

  return TSDB_CODE_SUCCESS;
}

void filterCheckColConflict(SFilterGroupCtx* gRes1, SFilterGroupCtx* gRes2, bool *conflict) {
  uint16_t idx1 = 0, idx2 = 0, m = 0, n = 0;
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


int32_t filterMergeTwoGroupsImpl(SFilterInfo *info, SFilterRangeCtx **ctx, int32_t optr, uint16_t cidx, SFilterGroupCtx* gRes1, SFilterGroupCtx* gRes2, bool *empty, bool *all) {
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

  uint16_t idx1 = 0, idx2 = 0, m = 0, n = 0;
  bool numEqual = (*gRes1)->colNum == (*gRes2)->colNum;
  bool equal = false;
  uint16_t equal1 = 0, equal2 = 0, merNum = 0;
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
      
      filterMergeTwoGroupsImpl(info, &ctx, TSDB_RELATION_OR, idx1, *gRes1, *gRes2, NULL, all);

      CHK_JMP(*all);

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

          CHK_JMP(equal1 != merNum && equal2 != merNum);
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

        CHK_JMP(equal1 != merNum);
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
  uint16_t pColNum = 0, cColNum = 0; 
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

          CHK_JMP(all);

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

        CHK_JMP(all);
        
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
    cEnd = i;
    cColNum = gRes[i]->colNum;    
  }

  return TSDB_CODE_SUCCESS;
  
_return:

  FILTER_SET_FLAG(info->status, FI_STATUS_ALL);

  return TSDB_CODE_SUCCESS;
}

int32_t filterConvertGroupFromArray(SFilterInfo *info, SArray* group) {
  size_t groupSize = taosArrayGetSize(group);

  info->groupNum = (uint16_t)groupSize;

  if (info->groupNum > 0) {
    info->groups = calloc(info->groupNum, sizeof(*info->groups));
  }

  for (size_t i = 0; i < groupSize; ++i) {
    SFilterGroup *pg = taosArrayGet(group, i);
    pg->unitFlags = calloc(pg->unitNum, sizeof(*pg->unitFlags));
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
  uint16_t uidx = 0;

  memset(info, 0, sizeof(*info));
  
  info->colRangeNum = oinfo.colRangeNum;
  info->colRange = oinfo.colRange;
  oinfo.colRangeNum = 0;
  oinfo.colRange = NULL;

  FILTER_SET_FLAG(info->options, FI_OPTION_NEED_UNIQE);

  filterInitUnitsFields(info);

  for (int32_t i = 0; i < gResNum; ++i) {
    res = gRes[i];

    optr = (res->colNum > 1) ? TSDB_RELATION_AND : TSDB_RELATION_OR;

    SFilterGroup ng = {0};
    
    for (uint16_t m = 0; m < res->colNum; ++m) {
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
  uint16_t *idxs = NULL;
  uint16_t colNum = 0;
  SFilterGroupCtx *res = NULL;
  uint16_t *idxNum = calloc(info->fields[FLD_TYPE_COLUMN].num, sizeof(*idxNum));

  for (int32_t i = 0; i < gResNum; ++i) {
    for (uint16_t m = 0; m < gRes[i]->colNum; ++m) {
      SFilterColInfo  *colInfo = &gRes[i]->colInfo[gRes[i]->colIdx[m]];
      if (FILTER_NO_MERGE_DATA_TYPE(colInfo->dataType)) {
        continue;
      }

      ++idxNum[gRes[i]->colIdx[m]];
    }
  }

  for (uint16_t i = 0; i < info->fields[FLD_TYPE_COLUMN].num; ++i) {
    if (idxNum[i] < gResNum) {
      continue;
    }

    assert(idxNum[i] == gResNum);
    
    if (idxs == NULL) {
      idxs = calloc(info->fields[FLD_TYPE_COLUMN].num, sizeof(*idxs));
    }

    idxs[colNum++] = i;
  }

  CHK_JMP(colNum <= 0);

  info->colRangeNum = colNum;
  info->colRange = calloc(colNum, POINTER_BYTES);

  for (int32_t i = 0; i < gResNum; ++i) {
    res = gRes[i];
    uint16_t n = 0;

    for (uint16_t m = 0; m < info->colRangeNum; ++m) {
      for (; n < res->colNum; ++n) {
        if (res->colIdx[n] < idxs[m]) {
          continue;
        }

        assert(res->colIdx[n] == idxs[m]);

        SFilterColInfo * colInfo = &res->colInfo[res->colIdx[n]];
        if (info->colRange[m] == NULL) {
          info->colRange[m] = filterInitRangeCtx(colInfo->dataType, 0);
          SFilterField* fi = FILTER_GET_COL_FIELD(info, res->colIdx[n]);
          info->colRange[m]->colId = ((SSchema*)fi->desc)->colId;
        }

        assert(colInfo->type == RANGE_TYPE_MR_CTX);

        bool all = false;
        filterSourceRangeFromCtx(info->colRange[m], colInfo->info, TSDB_RELATION_OR, NULL, &all);
        if (all) {
          filterFreeRangeCtx(info->colRange[m]);
          info->colRange[m] = NULL;
          
          if (m < (info->colRangeNum - 1)) {
            memmove(&info->colRange[m], &info->colRange[m + 1], (info->colRangeNum - m - 1) * POINTER_BYTES);
            memmove(&idxs[m], &idxs[m + 1], (info->colRangeNum - m - 1) * sizeof(*idxs));
          }

          --info->colRangeNum;
          --m;

          CHK_JMP(info->colRangeNum <= 0);          
        }

        ++n; 
        break;
      }
    }
  }

_return:
  tfree(idxNum);
  tfree(idxs);

  return TSDB_CODE_SUCCESS;
}

int32_t filterPostProcessRange(SFilterInfo *info) {
  for (uint16_t i = 0; i < info->colRangeNum; ++i) {
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
  uint16_t n = 0;
  
  info->cunits = malloc(info->unitNum * sizeof(*info->cunits));
  
  for (uint16_t i = 0; i < info->unitNum; ++i) {
    SFilterUnit *unit = &info->units[i];

    info->cunits[i].func = filterGetCompFuncIdx(FILTER_UNIT_DATA_TYPE(unit), unit->compare.optr);
    info->cunits[i].rfunc = filterGetRangeCompFuncFromOptrs(unit->compare.optr, unit->compare.optr2);
    info->cunits[i].optr = FILTER_UNIT_OPTR(unit);
    info->cunits[i].colData = NULL;
    
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

  uint16_t cgroupNum = info->groupNum + 1;
  
  for (uint16_t i = 0; i < info->groupNum; ++i) {
    cgroupNum += info->groups[i].unitNum;
  }

  info->cgroups = malloc(cgroupNum * sizeof(*info->cgroups));
  
  for (uint16_t i = 0; i < info->groupNum; ++i) {
    info->cgroups[n++] = info->groups[i].unitNum;

    for (uint16_t m = 0; m < info->groups[i].unitNum; ++m) {
      info->cgroups[n++] = info->groups[i].unitIdxs[m];
    }
  }

  info->cgroups[n] = 0;
  
  return TSDB_CODE_SUCCESS;
}

int32_t filterUpdateComUnits(SFilterInfo *info) {
  for (uint16_t i = 0; i < info->unitNum; ++i) {
    SFilterUnit *unit = &info->units[i];

    info->cunits[i].colData = FILTER_UNIT_COL_DATA(info, unit, 0);
  }

  return TSDB_CODE_SUCCESS;
}


static FORCE_INLINE bool filterExecuteImplAll(void *info, int32_t numOfRows, int8_t* p) {
  return true;
}
static FORCE_INLINE bool filterExecuteImplEmpty(void *info, int32_t numOfRows, int8_t* p) {
  return false;
}
static FORCE_INLINE bool filterExecuteImplIsNull(void *pinfo, int32_t numOfRows, int8_t* p) {
  SFilterInfo *info = (SFilterInfo *)pinfo;
  bool all = true;
  
  for (int32_t i = 0; i < numOfRows; ++i) {
    uint16_t uidx = info->groups[0].unitIdxs[0];
    void *colData = (char *)info->cunits[uidx].colData + info->cunits[uidx].dataSize * i;
    p[i] = isNull(colData, info->cunits[uidx].dataType);
    if (p[i] == 0) {
      all = false;
    }    
  }

  return all;
}
static FORCE_INLINE bool filterExecuteImplNotNull(void *pinfo, int32_t numOfRows, int8_t* p) {
  SFilterInfo *info = (SFilterInfo *)pinfo;
  bool all = true;
  
  for (int32_t i = 0; i < numOfRows; ++i) {
    uint16_t uidx = info->groups[0].unitIdxs[0];
    void *colData = (char *)info->cunits[uidx].colData + info->cunits[uidx].dataSize * i;
    p[i] = !isNull(colData, info->cunits[uidx].dataType);
    if (p[i] == 0) {
      all = false;
    }
  }

  return all;
}

bool filterExecuteImplRange(void *pinfo, int32_t numOfRows, int8_t* p) {
  SFilterInfo *info = (SFilterInfo *)pinfo;
  bool all = true;
  uint16_t dataSize = info->cunits[0].dataSize;
  char *colData = (char *)info->cunits[0].colData;
  rangeCompFunc rfunc = gRangeCompare[info->cunits[0].rfunc];
  void *valData = info->cunits[0].valData;
  void *valData2 = info->cunits[0].valData2;
  __compar_fn_t func = gDataCompare[info->cunits[0].func];
  
  for (int32_t i = 0; i < numOfRows; ++i) {
    if (isNull(colData, info->cunits[0].dataType)) {
      all = false;
      colData += dataSize;
      continue;
    }

    p[i] = (*rfunc)(colData, colData, valData, valData2, func);
            
    if (p[i] == 0) {
      all = false;
    }
    
    colData += dataSize;
  }

  return all;
}

bool filterExecuteImplMisc(void *pinfo, int32_t numOfRows, int8_t* p) {
  SFilterInfo *info = (SFilterInfo *)pinfo;
  bool all = true;
  
  for (int32_t i = 0; i < numOfRows; ++i) {
    uint16_t uidx = info->groups[0].unitIdxs[0];
    void *colData = (char *)info->cunits[uidx].colData + info->cunits[uidx].dataSize * i;
    if (isNull(colData, info->cunits[uidx].dataType)) {
      all = false;
      continue;
    }

    p[i] = filterDoCompare(gDataCompare[info->cunits[uidx].func], info->cunits[uidx].optr, colData, info->cunits[uidx].valData);
            
    if (p[i] == 0) {
      all = false;
    }
  }

  return all;
}


bool filterExecuteImpl(void *pinfo, int32_t numOfRows, int8_t* p) {
  SFilterInfo *info = (SFilterInfo *)pinfo;
  bool all = true;

  for (int32_t i = 0; i < numOfRows; ++i) {
    //FILTER_UNIT_CLR_F(info);
  
    for (uint32_t g = 0; g < info->groupNum; ++g) {
      SFilterGroup *group = &info->groups[g];
      for (uint32_t u = 0; u < group->unitNum; ++u) {
        uint16_t uidx = group->unitIdxs[u];
        SFilterComUnit *cunit = &info->cunits[uidx];
        void *colData = (char *)cunit->colData + cunit->dataSize * i;
      
        //if (FILTER_UNIT_GET_F(info, uidx)) {
        //  p[i] = FILTER_UNIT_GET_R(info, uidx);
        //} else {
          uint8_t optr = cunit->optr;

          if (isNull(colData, cunit->dataType)) {
            p[i] = optr == TSDB_RELATION_ISNULL ? true : false;
          } else {
            if (optr == TSDB_RELATION_NOTNULL) {
              p[i] = 1;
            } else if (optr == TSDB_RELATION_ISNULL) {
              p[i] = 0;
            } else if (cunit->rfunc >= 0) {
              p[i] = (*gRangeCompare[cunit->rfunc])(colData, colData, cunit->valData, cunit->valData2, gDataCompare[cunit->func]);
            } else {
              p[i] = filterDoCompare(gDataCompare[cunit->func], cunit->optr, colData, cunit->valData);
            }
          
          //FILTER_UNIT_SET_R(info, uidx, p[i]);
          //FILTER_UNIT_SET_F(info, uidx);
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
    }    
  }

  return all;
}

FORCE_INLINE bool filterExecute(SFilterInfo *info, int32_t numOfRows, int8_t* p) {
  return (*info->func)(info, numOfRows, p);
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

  if (info->units[0].compare.optr == TSDB_RELATION_ISNULL) {
    info->func = filterExecuteImplIsNull;
    return TSDB_CODE_SUCCESS;
  }

  if (info->units[0].compare.optr == TSDB_RELATION_NOTNULL) {
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
  SFilterGroupCtx** gRes = calloc(info->groupNum, sizeof(SFilterGroupCtx *));
  int32_t gResNum = 0;
  
  filterMergeGroupUnits(info, gRes, &gResNum);

  filterMergeGroups(info, gRes, &gResNum);

  if (FILTER_GET_FLAG(info->status, FI_STATUS_ALL)) {
    qInfo("Final - FilterInfo: [ALL]");
    goto _return;
  }

  
  if (FILTER_GET_FLAG(info->status, FI_STATUS_EMPTY)) {
    qInfo("Final - FilterInfo: [EMPTY]");
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

  tfree(gRes);
  
  return TSDB_CODE_SUCCESS;
}

int32_t filterSetColFieldData(SFilterInfo *info, int16_t colId, void *data) {
  CHK_LRET(info == NULL, TSDB_CODE_QRY_APP_ERROR, "info NULL");
  CHK_LRET(info->fields[FLD_TYPE_COLUMN].num <= 0, TSDB_CODE_QRY_APP_ERROR, "no column fileds");

  if (FILTER_ALL_RES(info) || FILTER_EMPTY_RES(info)) {
    return TSDB_CODE_SUCCESS;
  }

  for (uint16_t i = 0; i < info->fields[FLD_TYPE_COLUMN].num; ++i) {
    SFilterField* fi = &info->fields[FLD_TYPE_COLUMN].fields[i];
    SSchema* sch = fi->desc;
    if (sch->colId == colId) {
      fi->data = data;

      break;
    }
  }

  filterUpdateComUnits(info);

  return TSDB_CODE_SUCCESS;
}


int32_t filterInitFromTree(tExprNode* tree, SFilterInfo **pinfo, uint32_t options) {
  int32_t code = TSDB_CODE_SUCCESS;
  SFilterInfo *info = NULL;
  
  CHK_LRET(tree == NULL || pinfo == NULL, TSDB_CODE_QRY_APP_ERROR, "invalid param");

  if (*pinfo == NULL) {
    *pinfo = calloc(1, sizeof(SFilterInfo));
  }

  info = *pinfo;

  info->options = options;
  
  SArray* group = taosArrayInit(FILTER_DEFAULT_GROUP_SIZE, sizeof(SFilterGroup));

  filterInitUnitsFields(info);

  code = filterTreeToGroup(tree, info, group);

  ERR_JRET(code);

  filterConvertGroupFromArray(info, group);

  ERR_JRET(filterInitValFieldData(info));

  if (!FILTER_GET_FLAG(info->options, FI_OPTION_NO_REWRITE)) {
    filterDumpInfoToString(info, "Before preprocess", 0);

    ERR_JRET(filterPreprocess(info));
    
    CHK_JMP(FILTER_GET_FLAG(info->status, FI_STATUS_ALL));

    if (FILTER_GET_FLAG(info->status, FI_STATUS_EMPTY)) {
      taosArrayDestroy(group);
      return code;
    }

    //ERR_JRET(filterInitUnitFunc(info));
  }  

  info->unitRes = malloc(info->unitNum * sizeof(*info->unitRes));
  info->unitFlags = malloc(info->unitNum * sizeof(*info->unitFlags));

  filterDumpInfoToString(info, "Final", 0);

  taosArrayDestroy(group);

  return code;

_return:
  qInfo("No filter, code:%d", code);
  
  taosArrayDestroy(group);

  filterFreeInfo(*pinfo);

  *pinfo = NULL;

  return code;
}




bool filterRangeExecute(SFilterInfo *info, SDataStatis *pDataStatis, int32_t numOfCols, int32_t numOfRows) {
  if (FILTER_EMPTY_RES(info)) {
    return false;
  }

  if (FILTER_ALL_RES(info)) {
    return true;
  }
  
  bool ret = true;
  void *minVal, *maxVal;
  
  for (int32_t k = 0; k < info->colRangeNum; ++k) {
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
      return true;
    }

    // not support pre-filter operation on binary/nchar data type
    if (FILTER_NO_MERGE_DATA_TYPE(ctx->type)) {
      return true;
    }

    if ((pDataStatis[index].numOfNull <= 0) && (ctx->isnull && !ctx->notnull && !ctx->isrange)) {
      return false;
    }
    
    // all data in current column are NULL, no need to check its boundary value
    if (pDataStatis[index].numOfNull == numOfRows) {

      // if isNULL query exists, load the null data column
      if ((ctx->notnull || ctx->isrange) && (!ctx->isnull)) {
        return false;
      }

      continue;
    }

    SDataStatis* pDataBlockst = &pDataStatis[index];

    SFilterRangeNode *r = ctx->rs;

    if (ctx->type == TSDB_DATA_TYPE_FLOAT) {
      float minv = (float)(*(double *)(&pDataBlockst->min));
      float maxv = (float)(*(double *)(&pDataBlockst->max));
       
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
    
    CHK_RET(!ret, ret);
  }

  return ret;
}



int32_t filterGetTimeRange(SFilterInfo *info, STimeWindow       *win) {
  SFilterRange ra = {0};
  SFilterRangeCtx *prev = filterInitRangeCtx(TSDB_DATA_TYPE_TIMESTAMP, FI_OPTION_TIMESTAMP);
  SFilterRangeCtx *tmpc = filterInitRangeCtx(TSDB_DATA_TYPE_TIMESTAMP, FI_OPTION_TIMESTAMP);
  SFilterRangeCtx *cur = NULL;
  int32_t num = 0;
  int32_t optr = 0;
  int32_t code = 0;
  bool empty = false, all = false;

  for (int32_t i = 0; i < info->groupNum; ++i) {
    SFilterGroup *group = &info->groups[i];
    if (group->unitNum > 1) {
      cur = tmpc;
      optr = TSDB_RELATION_AND;
    } else {
      cur = prev;
      optr = TSDB_RELATION_OR;
    }

    for (int32_t u = 0; u < group->unitNum; ++u) {
      uint16_t uidx = group->unitIdxs[u];
      SFilterUnit *unit = &info->units[uidx];

      uint8_t raOptr = FILTER_UNIT_OPTR(unit);
      
      filterAddRangeOptr(cur, raOptr, TSDB_RELATION_AND, &empty, NULL);
      CHK_JMP(empty);
      
      if (FILTER_NO_MERGE_OPTR(raOptr)) {
        continue;
      }
      
      SFilterField *right = FILTER_UNIT_RIGHT_FIELD(info, unit);
      void *s = FILTER_GET_VAL_FIELD_DATA(right);
      void *e = FILTER_GET_VAL_FIELD_DATA(right) + tDataTypes[TSDB_DATA_TYPE_TIMESTAMP].bytes;

      SIMPLE_COPY_VALUES(&ra.s, s);
      SIMPLE_COPY_VALUES(&ra.e, e);
      
      filterAddRange(cur, &ra, optr);
    }

    if (cur->notnull) {
      prev->notnull = true;
      break;
    }

    if (group->unitNum > 1) {
      filterSourceRangeFromCtx(prev, cur, TSDB_RELATION_OR, &empty, &all);
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
    if (num > 1) {
      qError("only one time range accepted, num:%d", num);
      ERR_JRET(TSDB_CODE_QRY_INVALID_TIME_CONDITION);
    }

    CHK_JMP(num < 1);

    SFilterRange tra;
    filterGetRangeRes(prev, &tra);
    win->skey = tra.s; 
    win->ekey = tra.e;
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


int32_t filterConverNcharColumns(SFilterInfo* info, int32_t rows, bool *gotNchar) {
  for (uint16_t i = 0; i < info->fields[FLD_TYPE_COLUMN].num; ++i) {
    SFilterField* fi = &info->fields[FLD_TYPE_COLUMN].fields[i];
    int32_t type = FILTER_GET_COL_FIELD_TYPE(fi);
    if (type == TSDB_DATA_TYPE_NCHAR) {
      SFilterField nfi = {0};
      nfi.desc = fi->desc;
      int32_t bytes = FILTER_GET_COL_FIELD_SIZE(fi);
      nfi.data = malloc(rows * bytes);
      int32_t bufSize = bytes - VARSTR_HEADER_SIZE;
      for (int32_t j = 0; j < rows; ++j) {
        char *src = FILTER_GET_COL_FIELD_DATA(fi, j);
        char *dst = FILTER_GET_COL_FIELD_DATA(&nfi, j);
        int32_t len = 0;
        taosMbsToUcs4(varDataVal(src), varDataLen(src), varDataVal(dst), bufSize, &len);
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
  for (uint16_t i = 0; i < info->fields[FLD_TYPE_COLUMN].num; ++i) {
    SFilterField* fi = &info->fields[FLD_TYPE_COLUMN].fields[i];
    int32_t type = FILTER_GET_COL_FIELD_TYPE(fi);
    if (type == TSDB_DATA_TYPE_NCHAR) {
      tfree(fi->data);
    }
  }

  return TSDB_CODE_SUCCESS;
}





