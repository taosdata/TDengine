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
#include "hash.h"
#include "tname.h"

#define FILTER_DEFAULT_GROUP_SIZE 4
#define FILTER_DEFAULT_UNIT_SIZE 4
#define FILTER_DEFAULT_FIELD_SIZE 4
#define FILTER_DEFAULT_VALUE_SIZE 4
#define FILTER_DEFAULT_GROUP_UNIT_SIZE 2

#define FILTER_DUMMY_EMPTY_OPTR  127
#define FILTER_DUMMY_RANGE_OPTR  126

#define MAX_NUM_STR_SIZE 40

enum {
  FLD_TYPE_COLUMN = 1,
  FLD_TYPE_VALUE = 2,  
  FLD_TYPE_MAX = 3,
  FLD_DESC_NO_FREE = 4,
  FLD_DATA_NO_FREE = 8,
  FLD_DATA_IS_HASH = 16,
};

enum {
  MR_ST_START = 1,
  MR_ST_FIN = 2,
  MR_ST_ALL = 4,
  MR_ST_EMPTY = 8,
};

enum {
  RANGE_FLG_EXCLUDE = 1,
  RANGE_FLG_INCLUDE = 2,
  RANGE_FLG_NULL    = 4,
};

enum {
  FI_OPTION_NO_REWRITE = 1,
  FI_OPTION_TIMESTAMP = 2,
  FI_OPTION_NEED_UNIQE = 4,
};

enum {
  FI_STATUS_ALL = 1,
  FI_STATUS_EMPTY = 2,
  FI_STATUS_REWRITE = 4,
  FI_STATUS_CLONED = 8,
};

enum {
  RANGE_TYPE_UNIT = 1,
  RANGE_TYPE_VAR_HASH = 2,
  RANGE_TYPE_MR_CTX = 3,
};

typedef struct OptrStr {
  uint16_t optr;
  char    *str;
} OptrStr;

typedef struct SFilterRange {
  int64_t s;
  int64_t e;
  char sflag;
  char eflag;
} SFilterRange;

typedef struct SFilterColRange {  
  uint16_t idx;  //column field idx
  bool isNull;
  bool notNull;
  bool isRange;
  SFilterRange ra;
} SFilterColRange;

typedef bool (*rangeCompFunc) (const void *, const void *, const void *, const void *, __compar_fn_t);
typedef int32_t(*filter_desc_compare_func)(const void *, const void *);
typedef bool(*filter_exec_func)(void *, int32_t, int8_t*);

typedef struct SFilterRangeCompare {
  int64_t s;
  int64_t e;
  rangeCompFunc func;
} SFilterRangeCompare;

typedef struct SFilterRangeNode {
  struct SFilterRangeNode*   prev;
  struct SFilterRangeNode*   next;
  union {
    SFilterRange ra;
    SFilterRangeCompare rc;
  };
} SFilterRangeNode;

typedef struct SFilterRangeCtx {
  int32_t type;
  int32_t options;
  int8_t  status;  
  bool isnull;
  bool notnull;
  bool isrange;
  int16_t colId;
  __compar_fn_t pCompareFunc;
  SFilterRangeNode *rf;        //freed
  SFilterRangeNode *rs;
} SFilterRangeCtx ;

typedef struct SFilterVarCtx {
  int32_t type;
  int32_t options;
  int8_t  status;  
  bool isnull;
  bool notnull;
  bool isrange;
  SHashObj *wild;
  SHashObj *value;
} SFilterVarCtx;

typedef struct SFilterField {
  uint16_t flag;
  void*    desc;
  void*    data;
} SFilterField;

typedef struct SFilterFields {
  uint16_t size;
  uint16_t num;
  SFilterField *fields;
} SFilterFields;

typedef struct SFilterFieldId {
  uint16_t type;
  uint16_t idx;
} SFilterFieldId;

typedef struct SFilterGroup {
  uint16_t  unitSize;
  uint16_t  unitNum;
  uint16_t *unitIdxs;
  uint8_t  *unitFlags;  // !unit result
} SFilterGroup;

typedef struct SFilterColInfo {
  uint8_t type;
  int32_t dataType;
  void   *info;
} SFilterColInfo;

typedef struct SFilterGroupCtx {
  uint16_t         colNum;
  uint16_t        *colIdx;
  SFilterColInfo  *colInfo;
} SFilterGroupCtx;

typedef struct SFilterColCtx {
  uint16_t  colIdx;
  void*     ctx;
} SFilterColCtx;

typedef struct SFilterCompare {
  uint8_t       type;
  uint8_t       optr;
  uint8_t       optr2;
} SFilterCompare;

typedef struct SFilterUnit {
  SFilterCompare  compare;
  SFilterFieldId  left;
  SFilterFieldId  right;
  SFilterFieldId  right2;
} SFilterUnit;

typedef struct SFilterComUnit {
  void *colData;
  void *valData;
  void *valData2;
  uint16_t dataSize;
  uint8_t dataType;
  uint8_t optr;
  int8_t func;
  int8_t rfunc;
} SFilterComUnit;

typedef struct SFilterPCtx {
  SHashObj *valHash;
  SHashObj *unitHash;
} SFilterPCtx;

typedef struct SFilterInfo {
  uint32_t          options;
  uint32_t          status;  
  uint16_t          unitSize;
  uint16_t          unitNum;
  uint16_t          groupNum;
  uint16_t          colRangeNum;
  SFilterFields     fields[FLD_TYPE_MAX];
  SFilterGroup     *groups;
  uint16_t         *cgroups;
  SFilterUnit      *units;
  SFilterComUnit   *cunits;
  uint8_t          *unitRes;    // result
  uint8_t          *unitFlags;  // got result
  SFilterRangeCtx **colRange;
  filter_exec_func  func;
  
  SFilterPCtx       pctx;
} SFilterInfo;

#define COL_FIELD_SIZE (sizeof(SFilterField) + 2 * sizeof(int64_t))

#define FILTER_NO_MERGE_DATA_TYPE(t) ((t) == TSDB_DATA_TYPE_BINARY || (t) == TSDB_DATA_TYPE_NCHAR)
#define FILTER_NO_MERGE_OPTR(o) ((o) == TSDB_RELATION_ISNULL || (o) == TSDB_RELATION_NOTNULL || (o) == FILTER_DUMMY_EMPTY_OPTR)

#define MR_EMPTY_RES(ctx) (ctx->rs == NULL)

#define SET_AND_OPTR(ctx, o) do {if (o == TSDB_RELATION_ISNULL) { (ctx)->isnull = true; } else if (o == TSDB_RELATION_NOTNULL) { if (!(ctx)->isrange) { (ctx)->notnull = true; } } else if (o != FILTER_DUMMY_EMPTY_OPTR) { (ctx)->isrange = true; (ctx)->notnull = false; }  } while (0)
#define SET_OR_OPTR(ctx,o) do {if (o == TSDB_RELATION_ISNULL) { (ctx)->isnull = true; } else if (o == TSDB_RELATION_NOTNULL) { (ctx)->notnull = true; (ctx)->isrange = false; } else if (o != FILTER_DUMMY_EMPTY_OPTR) { if (!(ctx)->notnull) { (ctx)->isrange = true; } } } while (0)
#define CHK_OR_OPTR(ctx)  ((ctx)->isnull == true && (ctx)->notnull == true)
#define CHK_AND_OPTR(ctx)  ((ctx)->isnull == true && (((ctx)->notnull == true) || ((ctx)->isrange == true)))


#define FILTER_GET_FLAG(st, f) (st & f)
#define FILTER_SET_FLAG(st, f) st |= (f)
#define FILTER_CLR_FLAG(st, f) st &= (~f)

#define SIMPLE_COPY_VALUES(dst, src) *((int64_t *)dst) = *((int64_t *)src)
#define FILTER_PACKAGE_UNIT_HASH_KEY(v, optr, idx1, idx2) do { char *_t = (char *)v; _t[0] = optr; *(uint16_t *)(_t + 1) = idx1; *(uint16_t *)(_t + 3) = idx2; } while (0)
#define FILTER_GREATER(cr,sflag,eflag) ((cr > 0) || ((cr == 0) && (FILTER_GET_FLAG(sflag,RANGE_FLG_EXCLUDE) || FILTER_GET_FLAG(eflag,RANGE_FLG_EXCLUDE))))
#define FILTER_COPY_RA(dst, src) do { (dst)->sflag = (src)->sflag; (dst)->eflag = (src)->eflag; (dst)->s = (src)->s; (dst)->e = (src)->e; } while (0)

#define RESET_RANGE(ctx, r) do { (r)->next = (ctx)->rf; (ctx)->rf = r; } while (0)
#define FREE_RANGE(ctx, r) do { if ((r)->prev) { (r)->prev->next = (r)->next; } else { (ctx)->rs = (r)->next;} if ((r)->next) { (r)->next->prev = (r)->prev; } RESET_RANGE(ctx, r); } while (0)
#define FREE_FROM_RANGE(ctx, r) do { SFilterRangeNode *_r = r; if ((_r)->prev) { (_r)->prev->next = NULL; } else { (ctx)->rs = NULL;} while (_r) {SFilterRangeNode *n = (_r)->next; RESET_RANGE(ctx, _r); _r = n; } } while (0)
#define INSERT_RANGE(ctx, r, ra) do { SFilterRangeNode *n = filterNewRange(ctx, ra); n->prev = (r)->prev; if ((r)->prev) { (r)->prev->next = n; } else { (ctx)->rs = n; } (r)->prev = n; n->next = r; } while (0)
#define APPEND_RANGE(ctx, r, ra) do { SFilterRangeNode *n = filterNewRange(ctx, ra); n->prev = (r); if (r) { (r)->next = n; } else { (ctx)->rs = n; } } while (0)

#define ERR_RET(c) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { return _code; } } while (0)
#define ERR_LRET(c,...) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { qError(__VA_ARGS__); return _code; } } while (0)
#define ERR_JRET(c) do { code = c; if (code != TSDB_CODE_SUCCESS) { goto _return; } } while (0)

#define CHK_RETV(c) do { if (c) { return; } } while (0)
#define CHK_RET(c, r) do { if (c) { return r; } } while (0)
#define CHK_JMP(c) do { if (c) { goto _return; } } while (0)
#define CHK_LRETV(c,...) do { if (c) { qError(__VA_ARGS__); return; } } while (0)
#define CHK_LRET(c, r,...) do { if (c) { qError(__VA_ARGS__); return r; } } while (0)

#define FILTER_GET_FIELD(i, id) (&((i)->fields[(id).type].fields[(id).idx]))
#define FILTER_GET_COL_FIELD(i, idx) (&((i)->fields[FLD_TYPE_COLUMN].fields[idx]))
#define FILTER_GET_COL_FIELD_TYPE(fi) (((SSchema *)((fi)->desc))->type)
#define FILTER_GET_COL_FIELD_SIZE(fi) (((SSchema *)((fi)->desc))->bytes)
#define FILTER_GET_COL_FIELD_DESC(fi) ((SSchema *)((fi)->desc))
#define FILTER_GET_COL_FIELD_DATA(fi, ri) ((char *)(fi)->data + ((SSchema *)((fi)->desc))->bytes * (ri))
#define FILTER_GET_VAL_FIELD_TYPE(fi) (((tVariant *)((fi)->desc))->nType)
#define FILTER_GET_VAL_FIELD_DATA(fi) ((char *)(fi)->data)
#define FILTER_GET_TYPE(fl) ((fl) & FLD_TYPE_MAX)

#define FILTER_GROUP_UNIT(i, g, uid) ((i)->units + (g)->unitIdxs[uid])
#define FILTER_UNIT_LEFT_FIELD(i, u) FILTER_GET_FIELD(i, (u)->left)
#define FILTER_UNIT_RIGHT_FIELD(i, u) FILTER_GET_FIELD(i, (u)->right)
#define FILTER_UNIT_DATA_TYPE(u) ((u)->compare.type)
#define FILTER_UNIT_COL_DESC(i, u) FILTER_GET_COL_FIELD_DESC(FILTER_UNIT_LEFT_FIELD(i, u))
#define FILTER_UNIT_COL_DATA(i, u, ri) FILTER_GET_COL_FIELD_DATA(FILTER_UNIT_LEFT_FIELD(i, u), ri)
#define FILTER_UNIT_COL_SIZE(i, u) FILTER_GET_COL_FIELD_SIZE(FILTER_UNIT_LEFT_FIELD(i, u))
#define FILTER_UNIT_VAL_DATA(i, u) FILTER_GET_VAL_FIELD_DATA(FILTER_UNIT_RIGHT_FIELD(i, u))
#define FILTER_UNIT_COL_IDX(u) ((u)->left.idx)
#define FILTER_UNIT_OPTR(u) ((u)->compare.optr)
#define FILTER_UNIT_COMP_FUNC(u) ((u)->compare.func)

#define FILTER_UNIT_CLR_F(i) memset((i)->unitFlags, 0, (i)->unitNum * sizeof(*info->unitFlags)) 
#define FILTER_UNIT_SET_F(i, idx) (i)->unitFlags[idx] = 1
#define FILTER_UNIT_GET_F(i, idx) ((i)->unitFlags[idx])
#define FILTER_UNIT_GET_R(i, idx) ((i)->unitRes[idx])
#define FILTER_UNIT_SET_R(i, idx, v) (i)->unitRes[idx] = (v)

#define FILTER_PUSH_UNIT(colInfo, u) do { (colInfo).type = RANGE_TYPE_UNIT; (colInfo).dataType = FILTER_UNIT_DATA_TYPE(u);taosArrayPush((SArray *)((colInfo).info), &u);} while (0)
#define FILTER_PUSH_VAR_HASH(colInfo, ha) do { (colInfo).type = RANGE_TYPE_VAR_HASH; (colInfo).info = ha;} while (0)
#define FILTER_PUSH_CTX(colInfo, ctx) do { (colInfo).type = RANGE_TYPE_MR_CTX; (colInfo).info = ctx;} while (0)

#define FILTER_COPY_IDX(dst, src, n) do { *(dst) = malloc(sizeof(uint16_t) * n); memcpy(*(dst), src, sizeof(uint16_t) * n);} while (0)

#define FILTER_ADD_CTX_TO_GRES(gres, idx, ctx) do { if ((gres)->colCtxs == NULL) { (gres)->colCtxs = taosArrayInit(gres->colNum, sizeof(SFilterColCtx)); } SFilterColCtx cCtx = {idx, ctx}; taosArrayPush((gres)->colCtxs, &cCtx); } while (0) 


#define FILTER_ALL_RES(i) FILTER_GET_FLAG((i)->status, FI_STATUS_ALL)
#define FILTER_EMPTY_RES(i) FILTER_GET_FLAG((i)->status, FI_STATUS_EMPTY)


extern int32_t filterInitFromTree(tExprNode* tree, SFilterInfo **pinfo, uint32_t options);
extern bool filterExecute(SFilterInfo *info, int32_t numOfRows, int8_t* p);
extern int32_t filterSetColFieldData(SFilterInfo *info, int16_t colId, void *data);
extern int32_t filterGetTimeRange(SFilterInfo *info, STimeWindow *win);
extern int32_t filterConverNcharColumns(SFilterInfo* pFilterInfo, int32_t rows, bool *gotNchar);
extern int32_t filterFreeNcharColumns(SFilterInfo* pFilterInfo);
extern void filterFreeInfo(SFilterInfo *info);
extern bool filterRangeExecute(SFilterInfo *info, SDataStatis *pDataStatis, int32_t numOfCols, int32_t numOfRows);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_QFILTER_H
