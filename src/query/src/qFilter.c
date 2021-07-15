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

static FORCE_INLINE SFilterRangeNode* filterNewRange(SFilterRMCtx *ctx, SFilterRange* ra) {
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

void* filterInitMergeRange(int32_t type, int32_t options) {
  if (type > TSDB_DATA_TYPE_UBIGINT || type < TSDB_DATA_TYPE_BOOL || type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_NCHAR) {
    qError("not supported range type:%d", type);
    return NULL;
  }
  
  SFilterRMCtx *ctx = calloc(1, sizeof(SFilterRMCtx));

  ctx->type = type;
  ctx->options = options;
  ctx->pCompareFunc = getComparFunc(type, 0);

  return ctx;
}


int32_t filterResetMergeRangeCtx(SFilterRMCtx *ctx) {
  ctx->status = 0;

  if (ctx->rf == NULL) {
    ctx->rf = ctx->rs;
    ctx->rs = NULL;
    return TSDB_CODE_SUCCESS;
  }

  SFilterRangeNode *r = ctx->rf;
  
  while (r && r->next) {
    r = r->next;
  }

  r->next = ctx->rs;
  ctx->rs = NULL;
  return TSDB_CODE_SUCCESS;
}

int32_t filterReuseMergeRangeCtx(SFilterRMCtx *ctx, int32_t type, int32_t options) {
  filterResetMergeRangeCtx(ctx);

  ctx->type = type;
  ctx->options = options;
  ctx->pCompareFunc = getComparFunc(type, 0);

  return TSDB_CODE_SUCCESS;
}


int32_t filterPostProcessRange(SFilterRMCtx *cur, SFilterRange *ra, bool *notNull) {
  if (!FILTER_GET_FLAG(ra->sflag, RA_NULL)) {
    int32_t sr = cur->pCompareFunc(&ra->s, getDataMin(cur->type));
    if (sr == 0) {
      FILTER_SET_FLAG(ra->sflag, RA_NULL);
    }
  }

  if (!FILTER_GET_FLAG(ra->eflag, RA_NULL)) {
    int32_t er = cur->pCompareFunc(&ra->e, getDataMax(cur->type));
    if (er == 0) {
      FILTER_SET_FLAG(ra->eflag, RA_NULL);
    }
  }

  
  if (FILTER_GET_FLAG(ra->sflag, RA_NULL) && FILTER_GET_FLAG(ra->eflag, RA_NULL)) {
    *notNull = true;
  } else {
    *notNull = false;
  }

  return TSDB_CODE_SUCCESS;
}


int32_t filterAddMergeRangeImpl(void* h, SFilterRange* ra, int32_t optr) {
  SFilterRMCtx *ctx = (SFilterRMCtx *)h;

  if (ctx->rs == NULL) {
    if ((FILTER_GET_FLAG(ctx->status, MR_ST_START) == 0) 
      || (FILTER_GET_FLAG(ctx->status, MR_ALL) && (optr == TSDB_RELATION_AND))
      || ((!FILTER_GET_FLAG(ctx->status, MR_ALL)) && (optr == TSDB_RELATION_OR))) {
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
    filterPostProcessRange(ctx, &ctx->rs->ra, &notnull);
    if (notnull) {
      FREE_FROM_RANGE(ctx, ctx->rs);
      FILTER_SET_FLAG(ctx->status, MR_ALL);
    }
  }

  return TSDB_CODE_SUCCESS;  
}

int32_t filterAddMergeRange(void* h, SFilterRange* ra, int32_t optr) {
  SFilterRMCtx *ctx = (SFilterRMCtx *)h;
  
  if (FILTER_GET_FLAG(ra->sflag, RA_NULL)) {
    SIMPLE_COPY_VALUES(&ra->s, getDataMin(ctx->type));
    //FILTER_CLR_FLAG(ra->sflag, RA_NULL);
  }

  if (FILTER_GET_FLAG(ra->eflag, RA_NULL)) {
    SIMPLE_COPY_VALUES(&ra->e, getDataMax(ctx->type));
    //FILTER_CLR_FLAG(ra->eflag, RA_NULL);
  }

  return filterAddMergeRangeImpl(h, ra, optr);
}

int32_t filterAddMergeRangeCtx(void *dst, void *src, int32_t optr) {
  SFilterRMCtx *dctx = (SFilterRMCtx *)dst;
  SFilterRMCtx *sctx = (SFilterRMCtx *)src;

  if (sctx->rs == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  SFilterRangeNode *r = sctx->rs;
  
  while (r) {
    filterAddMergeRange(dctx, &r->ra, optr);
    r = r->next;
  }

  return TSDB_CODE_SUCCESS;
}


int32_t filterFinMergeRange(void* h) {
  SFilterRMCtx *ctx = (SFilterRMCtx *)h;

  if (FILTER_GET_FLAG(ctx->status, MR_ST_FIN)) {
    return TSDB_CODE_SUCCESS;
  }

  if (FILTER_GET_FLAG(ctx->options, MR_OPT_TS)) {
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

int32_t filterGetMergeRangeNum(void* h, int32_t* num) {
  filterFinMergeRange(h);
  
  SFilterRMCtx *ctx = (SFilterRMCtx *)h;

  *num = 0;

  SFilterRangeNode *r = ctx->rs;
  
  while (r) {
    ++(*num);
    r = r->next;
  }

  return TSDB_CODE_SUCCESS;
}


int32_t filterGetMergeRangeRes(void* h, SFilterRange *ra) {
  filterFinMergeRange(h);

  SFilterRMCtx *ctx = (SFilterRMCtx *)h;
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

int32_t filterFreeMergeRange(void* h) {
  if (h == NULL) {
    return TSDB_CODE_SUCCESS;
  }
  
  SFilterRMCtx *ctx = (SFilterRMCtx *)h;
  SFilterRangeNode *r = ctx->rs;
  SFilterRangeNode *rn = NULL;
  
  while (r) {
    rn = r->next;
    free(r);
    r = rn;
  }

  free(ctx);

  return TSDB_CODE_SUCCESS;
}


int32_t filterMergeGroup(SFilterGroup *gp1, SFilterGroup *gp2, SArray* group) {
  SFilterGroup gp = {0};

  //TODO CHECK DUP
  
  gp.unitNum = gp1->unitNum + gp2->unitNum;
  gp.unitIdxs = calloc(gp.unitNum, sizeof(*gp.unitIdxs));
  memcpy(gp.unitIdxs, gp1->unitIdxs, gp1->unitNum * sizeof(*gp.unitIdxs));
  memcpy(gp.unitIdxs + gp1->unitNum, gp2->unitIdxs, gp2->unitNum * sizeof(*gp.unitIdxs));    

  gp.unitFlags = NULL;
  
  taosArrayPush(group, &gp);

  return TSDB_CODE_SUCCESS;
}


int32_t filterMergeGroups(SArray* group, SArray* left, SArray* right) {
  int32_t leftSize = (int32_t)taosArrayGetSize(left);
  int32_t rightSize = (int32_t)taosArrayGetSize(right);

  CHK_LRET(taosArrayGetSize(left) <= 0, TSDB_CODE_QRY_APP_ERROR, "empty group");
  CHK_LRET(taosArrayGetSize(right) <= 0, TSDB_CODE_QRY_APP_ERROR, "empty group");  
  
  for (int32_t l = 0; l < leftSize; ++l) {
    SFilterGroup *gp1 = taosArrayGet(left, l);
    
    for (int32_t r = 0; r < rightSize; ++r) {
      SFilterGroup *gp2 = taosArrayGet(right, r);

      filterMergeGroup(gp1, gp2, group);
    }
  }


  return TSDB_CODE_SUCCESS;
}

int32_t filterGetFiled(SFilterFields* fields, int32_t type, void *v) {
  for (uint16_t i = 0; i < fields->num; ++i) {
    if (0 == gDescCompare[type](fields->fields[i].desc, v)) {
      return i;
    }
  }

  return -1;
}

int32_t filterAddField(SFilterInfo *info, void *desc, void *data, int32_t type, SFilterFieldId *fid) {
  int32_t idx = -1;
  uint16_t *num;

  num = &info->fields[type].num;

  if (*num > 0 && type != FLD_TYPE_VALUE) {
    idx = filterGetFiled(&info->fields[type], type, desc);
  }
  
  if (idx < 0) {
    idx = *num;
    if (idx >= info->fields[type].size) {
      info->fields[type].size += FILTER_DEFAULT_FIELD_SIZE;
      info->fields[type].fields = realloc(info->fields[type].fields, info->fields[type].size * sizeof(SFilterField));
    }
    
    info->fields[type].fields[idx].flag = type;  
    info->fields[type].fields[idx].desc = desc;
    info->fields[type].fields[idx].data = data;
    ++(*num);
  }

  fid->type = type;
  fid->idx = idx;
  
  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE int32_t filterAddFieldFromField(SFilterInfo *info, SFilterField *field, SFilterFieldId *fid) {
  filterAddField(info, field->desc, field->data, FILTER_GET_TYPE(field->flag), fid);

  FILTER_SET_FLAG(field->flag, FLD_DESC_NO_FREE);
  FILTER_SET_FLAG(field->flag, FLD_DATA_NO_FREE);

  return TSDB_CODE_SUCCESS;
}


int32_t filterAddFieldFromNode(SFilterInfo *info, tExprNode *node, SFilterFieldId *fid) {
  CHK_LRET(node == NULL, TSDB_CODE_QRY_APP_ERROR, "empty node");
  CHK_LRET(node->nodeType != TSQL_NODE_COL && node->nodeType != TSQL_NODE_VALUE, TSDB_CODE_QRY_APP_ERROR, "invalid nodeType:%d", node->nodeType);
  
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

  filterAddField(info, v, NULL, type, fid);
  
  return TSDB_CODE_SUCCESS;
}

int32_t filterAddUnit(SFilterInfo *info, uint8_t optr, SFilterFieldId *left, SFilterFieldId *right) {
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
    assert(optr == TSDB_RELATION_ISNULL || optr == TSDB_RELATION_NOTNULL);
  }
  
  SFilterField *col = FILTER_UNIT_LEFT_FIELD(info, u);
  assert(FILTER_GET_FLAG(col->flag, FLD_TYPE_COLUMN));
  
  info->units[info->unitNum].compare.type = FILTER_GET_COL_FIELD_TYPE(col);
  
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



int32_t filterAddGroupUnitFromNode(SFilterInfo *info, tExprNode* tree, SArray *group) {
  SFilterFieldId left = {0}, right = {0};

  filterAddFieldFromNode(info, tree->_node.pLeft, &left);  

  tVariant* var = tree->_node.pRight->pVal;
  int32_t type = FILTER_GET_COL_FIELD_TYPE(FILTER_GET_FIELD(info, left));

  if (tree->_node.optr == TSDB_RELATION_IN && (!IS_VAR_DATA_TYPE(type))) {    
    void *data = NULL;
    convertFilterSetFromBinary((void **)&data, var->pz, var->nLen, type);
    CHK_LRET(data == NULL, TSDB_CODE_QRY_APP_ERROR, "failed to convert in param");

    void *p = taosHashIterate((SHashObj *)data, NULL);
    while(p) {
      void *key = taosHashGetDataKey((SHashObj *)data, p);
      void *fdata = NULL;
    
      if (IS_VAR_DATA_TYPE(type)) {
        uint32_t len = taosHashGetDataKeyLen((SHashObj *)data, p);
        fdata = malloc(len + VARSTR_HEADER_SIZE);
        varDataLen(fdata) = len;
        memcpy(varDataVal(fdata), key, len);
      } else {
        fdata = malloc(sizeof(int64_t));
        SIMPLE_COPY_VALUES(fdata, key);
      }
      
      filterAddField(info, NULL, fdata, FLD_TYPE_VALUE, &right);

      filterAddUnit(info, TSDB_RELATION_EQUAL, &left, &right);
      
      SFilterGroup fgroup = {0};
      filterAddUnitToGroup(&fgroup, info->unitNum - 1);
      
      taosArrayPush(group, &fgroup);
      
      p = taosHashIterate((SHashObj *)data, p);
    }
  } else {
    filterAddFieldFromNode(info, tree->_node.pRight, &right);  
    
    filterAddUnit(info, tree->_node.optr, &left, &right);  

    SFilterGroup fgroup = {0};
    filterAddUnitToGroup(&fgroup, info->unitNum - 1);
    
    taosArrayPush(group, &fgroup);
  }
  
  return TSDB_CODE_SUCCESS;
}


int32_t filterAddUnitFromUnit(SFilterInfo *dst, SFilterInfo *src, SFilterUnit* u) {
  SFilterFieldId left, right;

  filterAddField(dst, FILTER_UNIT_COL_DESC(src, u), NULL, FLD_TYPE_COLUMN, &left);
  filterAddField(dst, NULL, FILTER_UNIT_VAL_DATA(src, u), FLD_TYPE_VALUE, &right);

  SFilterField *t = FILTER_UNIT_LEFT_FIELD(src, u);
  FILTER_SET_FLAG(t->flag, FLD_DESC_NO_FREE);
  t = FILTER_UNIT_RIGHT_FIELD(src, u);
  FILTER_SET_FLAG(t->flag, FLD_DATA_NO_FREE);

  return filterAddUnit(dst, FILTER_UNIT_OPTR(u), &left, &right);
}


int32_t filterAddGroupUnitFromRange(SFilterInfo *dst, SFilterInfo *src, SFilterColRange *cra, SFilterGroup *g, int32_t optr, SArray *res) {
  SFilterFieldId left, right;

  SFilterField *col = FILTER_GET_COL_FIELD(src, cra->idx);

  filterAddFieldFromField(dst, col, &left);

  if (optr == TSDB_RELATION_AND) {
    if (cra->isNull) {
      assert(cra->notNull == false);
      filterAddUnit(dst, TSDB_RELATION_ISNULL, &left, NULL);
      return filterAddUnitToGroup(g, dst->unitNum - 1);
    }

    if (cra->notNull) {      
      assert(cra->isNull == false);
      filterAddUnit(dst, TSDB_RELATION_NOTNULL, &left, NULL);
      return filterAddUnitToGroup(g, dst->unitNum - 1);
    }

    assert(!((FILTER_GET_FLAG(cra->ra.sflag, RA_NULL)) && (FILTER_GET_FLAG(cra->ra.eflag, RA_NULL))));

    if ((!FILTER_GET_FLAG(cra->ra.sflag, RA_NULL)) &&(!FILTER_GET_FLAG(cra->ra.eflag, RA_NULL))) {
      int32_t type = FILTER_GET_COL_FIELD_TYPE(FILTER_GET_FIELD(dst, left));
      __compar_fn_t func = getComparFunc(type, 0);
      if (func(&cra->ra.s, &cra->ra.e) == 0) {
        void *data = malloc(sizeof(int64_t));
        SIMPLE_COPY_VALUES(data, &cra->ra.s);
        filterAddField(dst, NULL, data, FLD_TYPE_VALUE, &right);
        filterAddUnit(dst, TSDB_RELATION_EQUAL, &left, &right);
        return filterAddUnitToGroup(g, dst->unitNum - 1);
      }
    }
    
    if (!FILTER_GET_FLAG(cra->ra.sflag, RA_NULL)) {
      void *data = malloc(sizeof(int64_t));
      SIMPLE_COPY_VALUES(data, &cra->ra.s);
      filterAddField(dst, NULL, data, FLD_TYPE_VALUE, &right);
      filterAddUnit(dst, FILTER_GET_FLAG(cra->ra.sflag, RA_EXCLUDE) ? TSDB_RELATION_GREATER : TSDB_RELATION_GREATER_EQUAL, &left, &right);
      filterAddUnitToGroup(g, dst->unitNum - 1);
    }

    if (!FILTER_GET_FLAG(cra->ra.eflag, RA_NULL)) {
      void *data = malloc(sizeof(int64_t));
      SIMPLE_COPY_VALUES(data, &cra->ra.e);
      filterAddField(dst, NULL, data, FLD_TYPE_VALUE, &right);
      filterAddUnit(dst, FILTER_GET_FLAG(cra->ra.eflag, RA_EXCLUDE) ? TSDB_RELATION_LESS : TSDB_RELATION_LESS_EQUAL, &left, &right);
      filterAddUnitToGroup(g, dst->unitNum - 1);
    }    

    return TSDB_CODE_SUCCESS;
  } 

  // OR PROCESS
  
  SFilterGroup ng = {0};
  g = &ng;
  
  if (cra->isNull) {
    filterAddUnit(dst, TSDB_RELATION_ISNULL, &left, NULL);
    filterAddUnitToGroup(g, dst->unitNum - 1);    
    taosArrayPush(res, g);
  }
  
  if (cra->notNull) {
    memset(g, 0, sizeof(*g));
    
    filterAddUnit(dst, TSDB_RELATION_NOTNULL, &left, NULL);
    filterAddUnitToGroup(g, dst->unitNum - 1);
    taosArrayPush(res, g);
  }

  memset(g, 0, sizeof(*g));

  if ((!FILTER_GET_FLAG(cra->ra.sflag, RA_NULL)) &&(!FILTER_GET_FLAG(cra->ra.eflag, RA_NULL))) {
    int32_t type = FILTER_GET_COL_FIELD_TYPE(FILTER_GET_FIELD(dst, left));
    __compar_fn_t func = getComparFunc(type, 0);
    if (func(&cra->ra.s, &cra->ra.e) == 0) {
      void *data = malloc(sizeof(int64_t));
      SIMPLE_COPY_VALUES(data, &cra->ra.s);
      filterAddField(dst, NULL, data, FLD_TYPE_VALUE, &right);
      filterAddUnit(dst, TSDB_RELATION_EQUAL, &left, &right);
      filterAddUnitToGroup(g, dst->unitNum - 1);
      
      taosArrayPush(res, g);
      return TSDB_CODE_SUCCESS;
    }
  }
  
  if (!FILTER_GET_FLAG(cra->ra.sflag, RA_NULL)) {
    filterAddField(dst, NULL, &cra->ra.s, FLD_TYPE_VALUE, &right);
    filterAddUnit(dst, FILTER_GET_FLAG(cra->ra.sflag, RA_EXCLUDE) ? TSDB_RELATION_GREATER : TSDB_RELATION_GREATER_EQUAL, &left, &right);
    filterAddUnitToGroup(g, dst->unitNum - 1);
  }
  
  if (!FILTER_GET_FLAG(cra->ra.eflag, RA_NULL)) {
    filterAddField(dst, NULL, &cra->ra.e, FLD_TYPE_VALUE, &right);
    filterAddUnit(dst, FILTER_GET_FLAG(cra->ra.eflag, RA_EXCLUDE) ? TSDB_RELATION_LESS : TSDB_RELATION_LESS_EQUAL, &left, &right);
    filterAddUnitToGroup(g, dst->unitNum - 1);
  }

  if (g->unitNum > 0) {
    taosArrayPush(res, g);
  }

  return TSDB_CODE_SUCCESS;
}


static void filterFreeGroup(void *pItem) {
  SFilterGroup* p = (SFilterGroup*) pItem;
  if (p) {
    tfree(p->unitIdxs);
    tfree(p->unitFlags);
  }
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

    ERR_JRET(filterMergeGroups(group, leftGroup, rightGroup));

    taosArrayDestroyEx(leftGroup, filterFreeGroup);
    taosArrayDestroyEx(rightGroup, filterFreeGroup);
    
    return TSDB_CODE_SUCCESS;
  }

  if (tree->_node.optr == TSDB_RELATION_OR) {
    ERR_RET(filterTreeToGroup(tree->_node.pLeft, info, group));
    ERR_RET(filterTreeToGroup(tree->_node.pRight, info, group));

    return TSDB_CODE_SUCCESS;
  }

  filterAddGroupUnitFromNode(info, tree, group);  


_err_return:

  taosArrayDestroyEx(leftGroup, filterFreeGroup);
  taosArrayDestroyEx(rightGroup, filterFreeGroup);
  
  return code;
}

int32_t filterInitUnitFunc(SFilterInfo *info) {
  for (uint16_t i = 0; i < info->unitNum; ++i) {
    SFilterUnit* unit = &info->units[i];
    unit->compare.pCompareFunc = getComparFunc(FILTER_UNIT_DATA_TYPE(unit), unit->compare.optr);
  }

  return TSDB_CODE_SUCCESS;
}



void filterDumpInfoToString(SFilterInfo *info, const char *msg) {
  CHK_LRETV(info == NULL, "%s - FilterInfo: empty", msg);

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
      qDebug("VAL%d => [type:%d][val:%" PRIi64"]", i, var->nType, var->i64); //TODO
    } else {
      qDebug("VAL%d => [type:NIL][val:%" PRIi64" or %f]", i, *(int64_t *)field->data, *(double *)field->data); //TODO
    }
  }

  qDebug("Unit  Num:%u", info->unitNum);
  for (uint16_t i = 0; i < info->unitNum; ++i) {
    SFilterUnit *unit = &info->units[i];
    int32_t type = FILTER_UNIT_DATA_TYPE(unit);
    int32_t len = 0;
    int32_t tlen = 0;
    char str[128] = {0};
    
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
      converToStr(str + len, type, data, tlen, &tlen);
    } else {
      strcat(str, "NULL");
    }
    strcat(str, "]");
    
    qDebug("%s", str); //TODO
  }

  qDebug("Group Num:%u", info->groupNum);
  for (uint16_t i = 0; i < info->groupNum; ++i) {
    SFilterGroup *group = &info->groups[i];
    qDebug("Group%d : unit num[%u]", i, group->unitNum);

    for (uint16_t u = 0; u < group->unitNum; ++u) {
      qDebug("unit id:%u", group->unitIdxs[u]);
    }
  }
}

void filterFreeInfo(SFilterInfo *info) {
  CHK_RETV(info == NULL);

  //TODO
}



int32_t filterInitValFieldData(SFilterInfo *info) {
  for (uint16_t i = 0; i < info->unitNum; ++i) {
    SFilterUnit* unit = &info->units[i];
    if (unit->right.type != FLD_TYPE_VALUE) {
      assert(unit->compare.optr == TSDB_RELATION_ISNULL || unit->compare.optr == TSDB_RELATION_NOTNULL);
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
      convertFilterSetFromBinary((void **)&fi->data, var->pz, var->nLen, type);
      CHK_LRET(fi->data == NULL, TSDB_CODE_QRY_APP_ERROR, "failed to convert in param");
    
      continue;
    }

    if (type == TSDB_DATA_TYPE_BINARY) {
      fi->data = calloc(1, (var->nLen + 1) * TSDB_NCHAR_SIZE);
    } else if (type == TSDB_DATA_TYPE_NCHAR) {
      fi->data = calloc(1, (var->nLen + 1) * TSDB_NCHAR_SIZE);
    } else {
      if (var->nType == TSDB_DATA_TYPE_VALUE_ARRAY) {  //TIME RANGE
        fi->data = calloc(var->nLen, tDataTypes[type].bytes);
        for (int32_t a = 0; a < var->nLen; ++a) {
          int64_t *v = taosArrayGet(var->arr, a);
          assignVal(fi->data + a * tDataTypes[type].bytes, (char *)v, 0, type);
        }

        continue;
      } else {
        fi->data = calloc(1, sizeof(int64_t));
      }
    }

    ERR_LRET(tVariantDump(var, (char*)fi->data, type, true), "dump type[%d] failed", type);
  }

  return TSDB_CODE_SUCCESS;
}



bool filterDoCompare(SFilterUnit *unit, void *left, void *right) {
  int32_t ret = unit->compare.pCompareFunc(left, right);

  switch (unit->compare.optr) {
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


int32_t filterAddUnitRange(SFilterInfo *info, SFilterUnit* u, SFilterRMCtx *ctx, int32_t optr) {
  int32_t type = FILTER_UNIT_DATA_TYPE(u);
  uint8_t uoptr = FILTER_UNIT_OPTR(u);
  void *val = FILTER_UNIT_VAL_DATA(info, u);
  SFilterRange ra = {0};
  int64_t tmp = 0;

  switch (uoptr) {
    case TSDB_RELATION_GREATER:
      SIMPLE_COPY_VALUES(&ra.s, val);
      FILTER_SET_FLAG(ra.sflag, RA_EXCLUDE);
      FILTER_SET_FLAG(ra.eflag, RA_NULL);
      break;
    case TSDB_RELATION_GREATER_EQUAL:
      SIMPLE_COPY_VALUES(&ra.s, val);
      FILTER_SET_FLAG(ra.eflag, RA_NULL);
      break;
    case TSDB_RELATION_LESS:
      SIMPLE_COPY_VALUES(&ra.e, val);
      FILTER_SET_FLAG(ra.eflag, RA_EXCLUDE);
      FILTER_SET_FLAG(ra.sflag, RA_NULL);
      break;
    case TSDB_RELATION_LESS_EQUAL:
      SIMPLE_COPY_VALUES(&ra.e, val);
      FILTER_SET_FLAG(ra.sflag, RA_NULL);
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
  
  filterAddMergeRange(ctx, &ra, optr);

  return TSDB_CODE_SUCCESS;
}



int32_t filterProcessUnitsInOneGroup(SFilterInfo *info, SFilterGroup* g, uint16_t id1, uint16_t id2, SArray* res, uint16_t *removedNum) {
  bool isnull = false, notnull = false, isrange = false;
  int32_t num = 0;
  SFilterRMCtx *cur = NULL;
  SFilterUnit* u1 = FILTER_GROUP_UNIT(info, g, id1);
  SFilterUnit* u2 = FILTER_GROUP_UNIT(info, g, id2);
  uint8_t optr1 = FILTER_UNIT_OPTR(u1);
  uint8_t optr2 = FILTER_UNIT_OPTR(u2);
  uint16_t cidx = FILTER_UNIT_COL_IDX(u1);

  int32_t type = FILTER_UNIT_DATA_TYPE(u1);

  SET_AND_OPTR(optr1);
  SET_AND_OPTR(optr2);
  
  CHK_JMP(CHK_AND_OPTR());

  cur = filterInitMergeRange(type, 0);

  if (!FILTER_NO_MERGE_OPTR(optr1)) {
    filterAddUnitRange(info, u1, cur, TSDB_RELATION_AND);
  }

  if (!FILTER_NO_MERGE_OPTR(optr2)) {
    filterAddUnitRange(info, u2, cur, TSDB_RELATION_AND);
    CHK_JMP(MR_EMPTY_RES(cur));
  }
  
  
  for (int32_t i = id2 + 1; i < g->unitNum; ++i) {
    SFilterUnit* u = FILTER_GROUP_UNIT(info, g, i);
    if (cidx != FILTER_UNIT_COL_IDX(u)) {
      continue;
    }

    ++(*removedNum);
    optr2 = FILTER_UNIT_OPTR(u);
    SET_AND_OPTR(optr2);
    CHK_JMP(CHK_AND_OPTR());
    
    if (!FILTER_NO_MERGE_OPTR(optr2)) {
      filterAddUnitRange(info, u, cur, TSDB_RELATION_AND);
      CHK_JMP(MR_EMPTY_RES(cur));
    }
  }

  SFilterColRange cra = {0};
  cra.idx = cidx;

  filterGetMergeRangeNum(cur, &num);
  assert(num == 1 || num == 0);

  if (num == 1) {
    filterGetMergeRangeRes(cur, &cra.ra);
    filterPostProcessRange(cur, &cra.ra, &cra.notNull);
  } else {
    if (isnull) {
      cra.isNull = true;
    }

    if (notnull) {
      cra.notNull = true;
    }
  }

  taosArrayPush(res, &cra);

  filterFreeMergeRange(cur);

  return TSDB_CODE_SUCCESS;

_err_return:

  g->unitNum = 0;
  
  *removedNum = g->unitNum;

  filterFreeMergeRange(cur);

  return TSDB_CODE_SUCCESS;

}


int32_t filterProcessUnits(SFilterInfo *info, SFilterGroupCtx*** res) {
  int32_t *col = NULL;
  SFilterGroupCtx *gctx = NULL;
  SArray *colRange = NULL;
  bool gresUsed = false;
  bool colInit = false;
  uint16_t removedNum = 0;
  
  for (uint16_t i = 0; i < info->groupNum; ++i) {
    SFilterGroup* g = info->groups + i;

    if (g->unitNum <= 1) {
      continue;
    }
        
    gresUsed = false;
    colInit = false;

    removedNum = 0;
    
    for (uint16_t j = 0; j < g->unitNum; ++j) {
      SFilterUnit* u = FILTER_GROUP_UNIT(info, g, j);
      int32_t type = FILTER_UNIT_DATA_TYPE(u);
      if (FILTER_NO_MERGE_DATA_TYPE(type)) {
        continue;
      }

      if (colInit == false) {
        if (col == NULL) {
          col = calloc(info->fields[FLD_TYPE_COLUMN].num, sizeof(int32_t));
        } else {
          memset(col, 0, info->fields[FLD_TYPE_COLUMN].num * sizeof(int32_t));
        }
        
        colInit = true;
      }
      
      uint16_t cidx = FILTER_UNIT_COL_IDX(u);
    
      if (col[cidx] == 0) {
        col[cidx] = j + 1;
      } else if (col[cidx] == -1) {
        continue;
      } else {
        if (colRange == NULL) {
          colRange = taosArrayInit(4, sizeof(SFilterColRange));
        }

        removedNum += 2;

        filterProcessUnitsInOneGroup(info, g, col[cidx] - 1, j, colRange, &removedNum);

        col[cidx] = -1;

        if (g->unitNum == 0) {
          break;
        } else {
          gresUsed = true;
        }
      }
    }

    if (g->unitNum == 0) {
      if (gresUsed) {
        taosArrayClear(colRange);
      }

      continue;
    }

    if (gresUsed) {
      gctx = malloc(sizeof(*gctx));
      gctx->num = g->unitNum - removedNum + 1;
      gctx->col = col;
      gctx->colRange = colRange;

      col = NULL;
      colRange = NULL;
      
      if (*res == NULL) {
        *res = calloc(info->groupNum, sizeof(SFilterGroupCtx *));
      }

      (*res)[i] = gctx;
    }
  }

  tfree(col);
  if (colRange) {
    taosArrayDestroy(colRange);
  }

  return TSDB_CODE_SUCCESS;
}




int32_t filterProcessGroupsSameColumn(SFilterInfo *info, uint16_t id1, uint16_t id2, SArray* res, uint16_t *removedNum, SFilterGroupCtx** unitRes) {
  bool isnull = false, notnull = false;
  int32_t num = 0;
  SFilterColRange *cra = NULL;
  SFilterGroup *g = NULL;
  
  SFilterUnit* u1 = FILTER_GROUP_UNIT(info, info->groups + id1, 0);
  uint8_t optr = FILTER_UNIT_OPTR(u1);
  uint16_t cidx = FILTER_UNIT_COL_IDX(u1);
  uint16_t cidx2 = 0;

  int32_t type = FILTER_UNIT_DATA_TYPE(u1);
  SFilterRMCtx *cur = filterInitMergeRange(type, 0);  
  
  for (int32_t i = 0; i < info->groupNum; ++i) {
    g = info->groups + i;
    uint16_t unitNum = (unitRes && unitRes[i]) ? unitRes[i]->num : g->unitNum;

    if (unitNum == 0) {      
      continue;
    }

    if (unitNum > 1) {
      continue;
    }

    if (unitRes && unitRes[i]) {
      assert(taosArrayGetSize(unitRes[i]->colRange) == 1);
      if (notnull) {
        continue;
      }
      
      cra = taosArrayGet(unitRes[i]->colRange, 0);

      if (cra->notNull) {
        notnull = true;        
        CHK_JMP(CHK_OR_OPTR());
      }
      
      cidx2 = cra->idx;
    } else {
      u1 = FILTER_GROUP_UNIT(info, g, 0);
      type = FILTER_UNIT_DATA_TYPE(u1);
      if (FILTER_NO_MERGE_DATA_TYPE(type)) {
        continue;
      }

      optr = FILTER_UNIT_OPTR(u1);
      
      cidx2 = FILTER_UNIT_COL_IDX(u1);
    }
    
    if (cidx != cidx2) {
      continue;
    }

    g->unitNum = 0;
    
    if (unitRes && unitRes[i]) {
      unitRes[i]->num = 0;
      if (notnull) {
        continue;
      }
      
      filterAddMergeRange(cur, &cra->ra, TSDB_RELATION_OR);
      if (MR_EMPTY_RES(cur)) {
        notnull = true;
        CHK_JMP(CHK_OR_OPTR());
      }

      continue;
    }

    optr = FILTER_UNIT_OPTR(u1);
    SET_OR_OPTR(optr);
    CHK_JMP(CHK_OR_OPTR());
    
    if (!FILTER_NO_MERGE_OPTR(optr) && notnull == false) {
      filterAddUnitRange(info, u1, cur, TSDB_RELATION_OR);
      if (MR_EMPTY_RES(cur)) {
        notnull = true;
        CHK_JMP(CHK_OR_OPTR());
      }      
    }
  }

  SFilterColRange ncra;
  SFilterRange *ra;
  ncra.idx = cidx;

  ncra.isNull = isnull;
  ncra.notNull = notnull;

  if (isnull) {
    --removedNum;
  }

  if (!notnull) {
    filterGetMergeRangeNum(cur, &num);
    if (num > 0) {
      if (num > 1) {
        ra = calloc(num, sizeof(*ra));
      } else {
        ra = &ncra.ra;
      }

      filterGetMergeRangeRes(cur, ra);

      SFilterRange *tra = ra;
      for (int32_t i = 0; i < num; ++i) {
        filterPostProcessRange(cur, tra, &ncra.notNull);
        assert(ncra.notNull == false);
        ++tra;
      }

      if (num > 1) {
        for (int32_t i = 0; i < num; ++i) {
          FILTER_COPY_RA(&ncra.ra, ra);
          
          taosArrayPush(res, &ncra);

          ++ra;
        }
      } else {
        taosArrayPush(res, &ncra);      
      }
    } else {
      ncra.ra.sflag = RA_NULL;
      ncra.ra.eflag = RA_NULL;      
      taosArrayPush(res, &ncra);      
    }
  } else {
    ncra.ra.sflag = RA_NULL;
    ncra.ra.eflag = RA_NULL;
    taosArrayPush(res, &ncra);
  }

  filterFreeMergeRange(cur);

  return TSDB_CODE_SUCCESS;

_err_return:

  FILTER_SET_FLAG(info->flags, FILTER_ALL);
  
  filterFreeMergeRange(cur);

  return TSDB_CODE_SUCCESS;

}


int32_t filterProcessGroups(SFilterInfo *info, SFilterGroupCtx** unitRes, SFilterGroupCtx* groupRes) {
  int32_t *col = NULL;
  uint16_t cidx;
  uint16_t removedNum = 0;
  bool merged = false;
  SArray *colRange = NULL;

  for (uint16_t i = 0; i < info->groupNum; ++i) {
    SFilterGroup* g = info->groups + i;
    uint16_t unitNum = g->unitNum;
  
    if (unitRes && unitRes[i]) {
      unitNum = unitRes[i]->num;
      merged = true;
    }

    if (unitNum == 0) {
      ++removedNum;
      continue;
    }

    if (unitNum > 1) {
      continue;
    }

    if (unitRes && unitRes[i]) {
      assert(taosArrayGetSize(unitRes[i]->colRange) == 1);
      SFilterColRange *ra = taosArrayGet(unitRes[i]->colRange, 0);

      cidx = ra->idx;
    } else {
      SFilterUnit* u = FILTER_GROUP_UNIT(info, g, 0);
      int32_t type = FILTER_UNIT_DATA_TYPE(u);
      if (FILTER_NO_MERGE_DATA_TYPE(type)) {
        continue;
      }
      
      cidx = FILTER_UNIT_COL_IDX(u);
    }

    if (col == NULL) {
      if (i < (info->groupNum - 1)) {
        col = calloc(info->fields[FLD_TYPE_COLUMN].num, sizeof(int32_t));
      } else {
        break;
      }
    }
    
    if (col[cidx] == 0) {
      col[cidx] = i + 1;
      continue;
    } else if (col[cidx] == -1) {
      continue;
    } else {
      if (colRange == NULL) {
        colRange = taosArrayInit(4, sizeof(SFilterColRange));
      }

      removedNum += 2;
      filterProcessGroupsSameColumn(info, col[cidx] - 1, i, colRange, &removedNum, unitRes);
      
      CHK_JMP(FILTER_GET_FLAG(info->flags, FILTER_ALL));
      
      col[cidx] = -1;
    }
  }

  uint16_t num = info->groupNum - removedNum;
  assert(num >= 0);
  
  if (colRange) {
    num += taosArrayGetSize(colRange);
  }

  if (num == 0) {
    FILTER_SET_FLAG(info->flags, FILTER_NONE);
    goto _err_return;
  }

  if (colRange || removedNum > 0 || merged) {
    groupRes->num = num;
    groupRes->col = col;
    groupRes->colRange = colRange;
  } else {
    tfree(col);
  }
  
  return TSDB_CODE_SUCCESS;

_err_return:
  tfree(col);
  if (colRange) {
    taosArrayDestroy(colRange);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t filterGenerateGroupFromArray(SFilterInfo *info, SArray* group) {
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


int32_t filterRewrite(SFilterInfo *info, SFilterGroupCtx* gctx, SFilterGroupCtx** uctx) {
  if (gctx->num == 0) {
    return TSDB_CODE_SUCCESS;
  }

  SFilterInfo oinfo = *info;
  uint16_t gNum = 0;
  SArray* group = taosArrayInit(FILTER_DEFAULT_GROUP_SIZE, sizeof(SFilterGroup));

  memset(info, 0, sizeof(*info));

  filterInitUnitsFields(info);
  
  for (uint16_t i = 0; i < oinfo.groupNum; ++i) {
    SFilterGroup* g = oinfo.groups + i;
    SFilterGroup ng = {0};
    uint16_t unitNum = (uctx && uctx[i]) ? uctx[i]->num : g->unitNum;

    if (unitNum == 0) {
      continue;
    }

    ++gNum;

    if ((uctx == NULL) || (uctx[i] == NULL)) {
      for (uint16_t n = 0; n < g->unitNum; ++n) {
        SFilterUnit* u = FILTER_GROUP_UNIT(&oinfo, g, n);
        filterAddUnitFromUnit(info, &oinfo, u);      
        filterAddUnitToGroup(&ng, info->unitNum - 1);
      }
      
      taosArrayPush(group, &ng);

      continue;
    }
    
    SFilterGroupCtx* ctx = uctx[i];
    for (uint16_t n = 0; n < g->unitNum; ++n) {
      SFilterUnit* u = FILTER_GROUP_UNIT(&oinfo, g, n);
      int32_t type = FILTER_UNIT_DATA_TYPE(u);
      if (FILTER_NO_MERGE_DATA_TYPE(type)) {
        filterAddUnitFromUnit(info, &oinfo, u);
        filterAddUnitToGroup(&ng, info->unitNum - 1);
        continue;
      }

      uint16_t cidx = FILTER_UNIT_COL_IDX(u);

      assert(ctx->col[cidx] > 0 || ctx->col[cidx] == -1);
      
      if (ctx->col[cidx] != -1) {
        filterAddUnitFromUnit(info, &oinfo, u);        
        filterAddUnitToGroup(&ng, info->unitNum - 1);
      }
    }

    if (ctx->colRange && taosArrayGetSize(ctx->colRange) > 0) {
      int32_t size = (int32_t)taosArrayGetSize(ctx->colRange);
      for (int32_t m = 0; m < size; ++m) {
        SFilterColRange *cra = taosArrayGet(ctx->colRange, m);
        filterAddGroupUnitFromRange(info, &oinfo, cra, &ng, TSDB_RELATION_AND, NULL);
      }
    }

    taosArrayPush(group, &ng);
  }

  if (gctx->colRange && taosArrayGetSize(gctx->colRange) > 0) {
    int32_t size = (int32_t)taosArrayGetSize(gctx->colRange);
    for (int32_t i = 0; i < size; ++i) {
      SFilterColRange *cra = taosArrayGet(gctx->colRange, i);
      filterAddGroupUnitFromRange(info, &oinfo, cra, NULL, TSDB_RELATION_OR, group);
    }
  }

  filterGenerateGroupFromArray(info, group);

  taosArrayDestroy(group);

  return TSDB_CODE_SUCCESS;
}


int32_t filterPreprocess(SFilterInfo *info) {
  SFilterGroupCtx** unitsRes = NULL;
  SFilterGroupCtx groupRes = {0};
  
  filterProcessUnits(info, &unitsRes);

  filterProcessGroups(info, unitsRes, &groupRes);

  if (FILTER_GET_FLAG(info->flags, FILTER_ALL)) {
    qInfo("Final - FilterInfo: [ALL]");
    return TSDB_CODE_SUCCESS;
  }

  if (FILTER_GET_FLAG(info->flags, FILTER_NONE)) {
    qInfo("Final - FilterInfo: [NONE]");
    return TSDB_CODE_SUCCESS;
  }  

  //TODO GET COLUMN RANGE

  filterRewrite(info, &groupRes, unitsRes);
  
  return TSDB_CODE_SUCCESS;
}


int32_t filterSetColFieldData(SFilterInfo *info, int16_t colId, void *data) {
  CHK_LRET(info == NULL, TSDB_CODE_QRY_APP_ERROR, "info NULL");
  CHK_LRET(info->fields[FLD_TYPE_COLUMN].num <= 0, TSDB_CODE_QRY_APP_ERROR, "no column fileds");

  for (uint16_t i = 0; i < info->fields[FLD_TYPE_COLUMN].num; ++i) {
    SFilterField* fi = &info->fields[FLD_TYPE_COLUMN].fields[i];
    SSchema* sch = fi->desc;
    if (sch->colId == colId) {
      fi->data = data;
      break;
    }
  }

  return TSDB_CODE_SUCCESS;
}


bool filterExecute(SFilterInfo *info, int32_t numOfRows, int8_t* p) {
  bool all = true;

  if (FILTER_GET_FLAG(info->flags, FILTER_NONE)) {
    return false;
  }

  for (int32_t i = 0; i < numOfRows; ++i) {
    FILTER_UNIT_CLR_F(info);

    p[i] = 0;
  
    for (uint16_t g = 0; g < info->groupNum; ++g) {
      SFilterGroup* group = &info->groups[g];
      bool qualified = true;
      
      for (uint16_t u = 0; u < group->unitNum; ++u) {
        uint16_t uidx = group->unitIdxs[u];
        uint8_t ures = 0;
      
        if (FILTER_UNIT_GET_F(info, uidx)) {
          ures = FILTER_UNIT_GET_R(info, uidx);
        } else {
          SFilterUnit *unit = &info->units[uidx];

          if (isNull(FILTER_UNIT_COL_DATA(info, unit, i), FILTER_UNIT_DATA_TYPE(unit))) {
            ures = unit->compare.optr == TSDB_RELATION_ISNULL ? true : false;
          } else {
            if (unit->compare.optr == TSDB_RELATION_NOTNULL) {
              ures = true;
            } else if (unit->compare.optr == TSDB_RELATION_ISNULL) {
              ures = false;
            } else {
              ures = filterDoCompare(unit, FILTER_UNIT_COL_DATA(info, unit, i), FILTER_UNIT_VAL_DATA(info, unit));
            }
          }
          
          FILTER_UNIT_SET_R(info, uidx, ures);
          FILTER_UNIT_SET_F(info, uidx);
        }

        if (!ures) {
          qualified = ures;
          break;
        }
      }

      if (qualified) {
        p[i] = 1;
        break;
      }
    }

    if (p[i] != 1) {
      all = false;
    }    
  }

  return all;
}


int32_t filterInitFromTree(tExprNode* tree, SFilterInfo **pinfo, uint32_t options) {
  int32_t code = TSDB_CODE_SUCCESS;
  SFilterInfo *info = NULL;
  
  CHK_LRET(tree == NULL || pinfo == NULL, TSDB_CODE_QRY_APP_ERROR, "invalid param");

  if (*pinfo == NULL) {
    *pinfo = calloc(1, sizeof(SFilterInfo));
  }

  info = *pinfo;

  info->flags = options;
  
  SArray* group = taosArrayInit(FILTER_DEFAULT_GROUP_SIZE, sizeof(SFilterGroup));

  filterInitUnitsFields(info);

  code = filterTreeToGroup(tree, info, group);

  ERR_JRET(code);

  filterGenerateGroupFromArray(info, group);

  ERR_JRET(filterInitValFieldData(info));

  if (!FILTER_GET_FLAG(info->flags, FILTER_NO_REWRITE)) {
    filterDumpInfoToString(info, "Before preprocess");

    ERR_JRET(filterPreprocess(info));
    
    CHK_JMP(FILTER_GET_FLAG(info->flags, FILTER_ALL));

    if (FILTER_GET_FLAG(info->flags, FILTER_NONE)) {
      taosArrayDestroy(group);
      return code;
    }
  }
  
  ERR_JRET(filterInitUnitFunc(info));

  info->unitRes = malloc(info->unitNum * sizeof(*info->unitRes));
  info->unitFlags = malloc(info->unitNum * sizeof(*info->unitFlags));

  filterDumpInfoToString(info, "Final");

  taosArrayDestroy(group);

  return code;

_err_return:
  qInfo("No filter, code:%d", code);
  
  taosArrayDestroy(group);

  filterFreeInfo(*pinfo);

  *pinfo = NULL;

  return code;
}

int32_t filterGetTimeRange(SFilterInfo *info, STimeWindow       *win) {
  SFilterRange ra = {0};
  SFilterRMCtx *prev = filterInitMergeRange(TSDB_DATA_TYPE_TIMESTAMP, MR_OPT_TS);
  SFilterRMCtx *tmpc = filterInitMergeRange(TSDB_DATA_TYPE_TIMESTAMP, MR_OPT_TS);
  SFilterRMCtx *cur = NULL;
  int32_t num = 0;
  int32_t optr = 0;
  int32_t code = 0;

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
      SFilterField *right = FILTER_UNIT_RIGHT_FIELD(info, unit);
      void *s = FILTER_GET_VAL_FIELD_DATA(right);
      void *e = FILTER_GET_VAL_FIELD_DATA(right) + tDataTypes[TSDB_DATA_TYPE_TIMESTAMP].bytes;

      SIMPLE_COPY_VALUES(&ra.s, s);
      SIMPLE_COPY_VALUES(&ra.e, e);
      
      code = filterAddMergeRange(cur, &ra, optr);
      if (code) {
        break;
      }
    }

    if (code != TSDB_CODE_SUCCESS) {
      break;
    }

    if (FILTER_GET_FLAG(cur->status, MR_ALL)) {
      FILTER_SET_FLAG(prev->status, MR_ALL);
      break;
    }

    if (group->unitNum > 1) {
      filterAddMergeRangeCtx(prev, cur, TSDB_RELATION_OR);      
      filterResetMergeRangeCtx(cur);
    }
  }

  if (code == TSDB_CODE_SUCCESS) {
    if (FILTER_GET_FLAG(prev->status, MR_ALL)) {
      *win = TSWINDOW_INITIALIZER;
    } else {
      filterGetMergeRangeNum(prev, &num);
      if (num != 1) {
        qError("only one time range accepted, num:%d", num);
        ERR_JRET(TSDB_CODE_QRY_INVALID_TIME_CONDITION);
      }

      SFilterRange tra;
      filterGetMergeRangeRes(prev, &tra);
      win->skey = tra.s; 
      win->ekey = tra.e;
    }
  }

_err_return:

  filterFreeMergeRange(prev);
  filterFreeMergeRange(tmpc);

  return code;
}


int32_t filterConverNcharColumns(SFilterInfo* pFilterInfo, int32_t rows, bool *gotNchar) {
#if 0
  for (int32_t i = 0; i < numOfFilterCols; ++i) {
    if (pFilterInfo[i].info.type == TSDB_DATA_TYPE_NCHAR) {
      pFilterInfo[i].pData2 = pFilterInfo[i].pData;
      pFilterInfo[i].pData = malloc(rows * pFilterInfo[i].info.bytes);
      int32_t bufSize = pFilterInfo[i].info.bytes - VARSTR_HEADER_SIZE;
      for (int32_t j = 0; j < rows; ++j) {
        char* dst = (char *)pFilterInfo[i].pData + j * pFilterInfo[i].info.bytes;
        char* src = (char *)pFilterInfo[i].pData2 + j * pFilterInfo[i].info.bytes;
        int32_t len = 0;
        taosMbsToUcs4(varDataVal(src), varDataLen(src), varDataVal(dst), bufSize, &len);
        varDataLen(dst) = len;
      }
      *gotNchar = true;
    }
  }
#endif

  return TSDB_CODE_SUCCESS;
}

int32_t filterFreeNcharColumns(SFilterInfo* pFilterInfo) {
#if 0
  for (int32_t i = 0; i < numOfFilterCols; ++i) {
    if (pFilterInfo[i].info.type == TSDB_DATA_TYPE_NCHAR) {
      if (pFilterInfo[i].pData2) {
        tfree(pFilterInfo[i].pData);
        pFilterInfo[i].pData = pFilterInfo[i].pData2;
        pFilterInfo[i].pData2 = NULL;
      }
    }
  }
#endif

  return TSDB_CODE_SUCCESS;
}





