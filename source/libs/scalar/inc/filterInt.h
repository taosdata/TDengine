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

#ifndef TDENGINE_FILTER_INT_H
#define TDENGINE_FILTER_INT_H

#ifdef __cplusplus
extern "C" {
#endif

#include "query.h"
#include "querynodes.h"
#include "scalar.h"
#include "tcommon.h"
#include "tdatablock.h"
#include "thash.h"
#include "tname.h"

#define FILTER_DEFAULT_GROUP_SIZE      4
#define FILTER_DEFAULT_UNIT_SIZE       4
#define FILTER_DEFAULT_FIELD_SIZE      4
#define FILTER_DEFAULT_VALUE_SIZE      4
#define FILTER_DEFAULT_GROUP_UNIT_SIZE 2

#define FILTER_DUMMY_EMPTY_OPTR 127

#define FILTER_RM_UNIT_MIN_ROWS 100

enum {
  FLD_TYPE_COLUMN = 1,
  FLD_TYPE_VALUE = 2,
  FLD_TYPE_MAX = 3,
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
  RANGE_FLG_NULL = 4,
};

enum {
  FI_STATUS_ALL = 1,
  FI_STATUS_EMPTY = 2,
  FI_STATUS_REWRITE = 4,
  FI_STATUS_CLONED = 8,
};

enum {
  FI_STATUS_BLK_ALL = 1,
  FI_STATUS_BLK_EMPTY = 2,
  FI_STATUS_BLK_ACTIVE = 4,
};

enum {
  RANGE_TYPE_UNIT = 1,
  RANGE_TYPE_VAR_HASH = 2,
  RANGE_TYPE_MR_CTX = 3,
};

enum {
  FI_ACTION_NO_NEED = 1,
  FI_ACTION_CONTINUE,
  FI_ACTION_STOP,
};

typedef struct OptrStr {
  uint16_t optr;
  char    *str;
} OptrStr;

typedef struct SFilterRange {
  int64_t s;
  int64_t e;
  char    sflag;
  char    eflag;
} SFilterRange;

typedef bool (*rangeCompFunc)(const void *, const void *, const void *, const void *, __compar_fn_t);
typedef int32_t (*filter_desc_compare_func)(const void *, const void *);
typedef bool (*filter_exec_func)(void *, int32_t, SColumnInfoData *, SColumnDataAgg *, int16_t, int32_t *);
typedef int32_t (*filer_get_col_from_name)(void *, int32_t, char *, void **);

typedef struct SFilterDataInfo {
  int32_t idx;
  void*   addr;
} SFilterDataInfo;

typedef struct SFilterRangeCompare {
  int64_t       s;
  int64_t       e;
  rangeCompFunc func;
} SFilterRangeCompare;

typedef struct SFilterRangeNode {
  struct SFilterRangeNode *prev;
  struct SFilterRangeNode *next;
  union {
    SFilterRange        ra;
    SFilterRangeCompare rc;
  };
} SFilterRangeNode;

typedef struct SFilterRangeCtx {
  int32_t           type;
  int32_t           options;
  int8_t            status;
  bool              isnull;
  bool              notnull;
  bool              isrange;
  int16_t           colId;
  __compar_fn_t     pCompareFunc;
  SFilterRangeNode *rf;  // freed
  SFilterRangeNode *rs;
} SFilterRangeCtx;

typedef struct SFilterVarCtx {
  int32_t   type;
  int32_t   options;
  int8_t    status;
  bool      isnull;
  bool      notnull;
  bool      isrange;
  SHashObj *wild;
  SHashObj *value;
} SFilterVarCtx;

typedef struct SFilterField {
  uint16_t flag;
  void    *desc;
  void    *data;
} SFilterField;

typedef struct SFilterFields {
  uint32_t      size;
  uint32_t      num;
  SFilterField *fields;
} SFilterFields;

typedef struct SFilterFieldId {
  uint16_t type;
  uint32_t idx;
} SFilterFieldId;

typedef struct SFilterGroup {
  uint32_t  unitSize;
  uint32_t  unitNum;
  uint32_t *unitIdxs;
  uint8_t  *unitFlags;  // !unit result
} SFilterGroup;

typedef struct SFilterColInfo {
  uint8_t type;
  int32_t dataType;
  uint8_t optr;   // for equal operation in the relation of RELATION_IN
  int64_t value;  // for equal operation in the relation of RELATION_IN
  void   *info;
} SFilterColInfo;

typedef struct SFilterGroupCtx {
  uint32_t        colNum;
  uint32_t       *colIdx;
  SFilterColInfo *colInfo;
} SFilterGroupCtx;

typedef struct SFilterColCtx {
  uint32_t colIdx;
  void    *ctx;
} SFilterColCtx;

typedef struct SFilterCompare {
  uint8_t type;
  int8_t  precision;
  uint8_t optr;
  uint8_t optr2;
} SFilterCompare;

typedef struct SFilterUnit {
  SFilterCompare compare;
  SFilterFieldId left;
  SFilterFieldId right;
  SFilterFieldId right2;
} SFilterUnit;

typedef struct SFilterComUnit {
  void    *colData;  // pointer to SColumnInfoData
  void    *valData;
  void    *valData2;
  uint16_t colId;
  uint16_t dataSize;
  uint8_t  dataType;
  uint8_t  optr;
  int8_t   func;
  int8_t   rfunc;
} SFilterComUnit;

typedef struct SFilterPCtx {
  SHashObj *valHash;
  SHashObj *unitHash;
} SFilterPCtx;

typedef struct SFltTreeStat {
  int32_t      code;
  int8_t       precision;
  bool         scalarMode;
  SArray      *nodeList;
  SFilterInfo *info;
} SFltTreeStat;


typedef struct SFltScalarCtx {
  SNode *node;
  SArray* fltSclRange;
} SFltScalarCtx;

typedef struct SFltBuildGroupCtx {
  SFilterInfo *info;
  SArray      *group;
  int32_t      code;
} SFltBuildGroupCtx;

typedef struct {
  SColumnNode *colNode;
  SArray      *points;
} SFltSclColumnRange;

struct SFilterInfo {
  bool              scalarMode;
  SFltScalarCtx     sclCtx;
  uint32_t          options;
  uint32_t          status;
  uint32_t          unitSize;
  uint32_t          unitNum;
  uint32_t          groupNum;
  uint32_t          colRangeNum;
  SFilterFields     fields[FLD_TYPE_MAX];
  SFilterGroup     *groups;
  SFilterUnit      *units;
  SFilterComUnit   *cunits;
  uint8_t          *unitRes;    // result
  uint8_t          *unitFlags;  // got result
  SFilterRangeCtx **colRange;
  filter_exec_func  func;
  uint8_t           blkFlag;
  uint32_t          blkGroupNum;
  uint32_t         *blkUnits;
  int8_t           *blkUnitRes;
  void             *pTable;
  SArray           *blkList;

  SFilterPCtx pctx;
};

#define FILTER_NO_MERGE_DATA_TYPE(t)                                                            \
  ((t) == TSDB_DATA_TYPE_BINARY || (t) == TSDB_DATA_TYPE_VARBINARY || (t) == TSDB_DATA_TYPE_NCHAR || (t) == TSDB_DATA_TYPE_JSON || \
   (t) == TSDB_DATA_TYPE_GEOMETRY)
#define FILTER_NO_MERGE_OPTR(o) ((o) == OP_TYPE_IS_NULL || (o) == OP_TYPE_IS_NOT_NULL || (o) == FILTER_DUMMY_EMPTY_OPTR)

#define MR_EMPTY_RES(ctx) (ctx->rs == NULL)

#define SET_AND_OPTR(ctx, o)                   \
  do {                                         \
    if (o == OP_TYPE_IS_NULL) {                \
      (ctx)->isnull = true;                    \
    } else if (o == OP_TYPE_IS_NOT_NULL) {     \
      if (!(ctx)->isrange) {                   \
        (ctx)->notnull = true;                 \
      }                                        \
    } else if (o != FILTER_DUMMY_EMPTY_OPTR) { \
      (ctx)->isrange = true;                   \
      (ctx)->notnull = false;                  \
    }                                          \
  } while (0)
#define SET_OR_OPTR(ctx, o)                    \
  do {                                         \
    if (o == OP_TYPE_IS_NULL) {                \
      (ctx)->isnull = true;                    \
    } else if (o == OP_TYPE_IS_NOT_NULL) {     \
      (ctx)->notnull = true;                   \
      (ctx)->isrange = false;                  \
    } else if (o != FILTER_DUMMY_EMPTY_OPTR) { \
      if (!(ctx)->notnull) {                   \
        (ctx)->isrange = true;                 \
      }                                        \
    }                                          \
  } while (0)
#define CHK_OR_OPTR(ctx)  ((ctx)->isnull == true && (ctx)->notnull == true)
#define CHK_AND_OPTR(ctx) ((ctx)->isnull == true && (((ctx)->notnull == true) || ((ctx)->isrange == true)))

#define FILTER_GET_FLAG(st, f) ((st) & (f))
#define FILTER_SET_FLAG(st, f) (st) |= (f)
#define FILTER_CLR_FLAG(st, f) (st) &= (~f)

#define SIMPLE_COPY_VALUES(dst, src) *((int64_t *)dst) = *((int64_t *)src)
#define FLT_PACKAGE_UNIT_HASH_KEY(v, op1, op2, lidx, ridx, ridx2) \
  do {                                                            \
    char *_t = (char *)(v);                                       \
    _t[0] = (op1);                                                \
    _t[1] = (op2);                                                \
    *(uint32_t *)(_t + 2) = (lidx);                               \
    *(uint32_t *)(_t + 2 + sizeof(uint32_t)) = (ridx);            \
  } while (0)
#define FILTER_GREATER(cr, sflag, eflag) \
  ((cr > 0) || ((cr == 0) && (FILTER_GET_FLAG(sflag, RANGE_FLG_EXCLUDE) || FILTER_GET_FLAG(eflag, RANGE_FLG_EXCLUDE))))
#define FILTER_COPY_RA(dst, src) \
  do {                           \
    (dst)->sflag = (src)->sflag; \
    (dst)->eflag = (src)->eflag; \
    (dst)->s = (src)->s;         \
    (dst)->e = (src)->e;         \
  } while (0)

#define RESET_RANGE(ctx, r) \
  do {                      \
    (r)->next = (ctx)->rf;  \
    (ctx)->rf = r;          \
  } while (0)
#define FREE_RANGE(ctx, r)         \
  do {                             \
    if ((r)->prev) {               \
      (r)->prev->next = (r)->next; \
    } else {                       \
      (ctx)->rs = (r)->next;       \
    }                              \
    if ((r)->next) {               \
      (r)->next->prev = (r)->prev; \
    }                              \
    RESET_RANGE(ctx, r);           \
  } while (0)
#define FREE_FROM_RANGE(ctx, r)         \
  do {                                  \
    SFilterRangeNode *_r = r;           \
    if ((_r)->prev) {                   \
      (_r)->prev->next = NULL;          \
    } else {                            \
      (ctx)->rs = NULL;                 \
    }                                   \
    while (_r) {                        \
      SFilterRangeNode *n = (_r)->next; \
      RESET_RANGE(ctx, _r);             \
      _r = n;                           \
    }                                   \
  } while (0)
#define INSERT_RANGE(ctx, r, ra)                   \
  do {                                             \
    SFilterRangeNode *n = filterNewRange(ctx, ra); \
    n->prev = (r)->prev;                           \
    if ((r)->prev) {                               \
      (r)->prev->next = n;                         \
    } else {                                       \
      (ctx)->rs = n;                               \
    }                                              \
    (r)->prev = n;                                 \
    n->next = r;                                   \
  } while (0)
#define APPEND_RANGE(ctx, r, ra)                   \
  do {                                             \
    SFilterRangeNode *n = filterNewRange(ctx, ra); \
    n->prev = (r);                                 \
    if (r) {                                       \
      (r)->next = n;                               \
    } else {                                       \
      (ctx)->rs = n;                               \
    }                                              \
  } while (0)

#define FLT_IS_COMPARISON_OPERATOR(_op) ((_op) >= OP_TYPE_GREATER_THAN && (_op) < OP_TYPE_IS_NOT_UNKNOWN)

#define fltFatal(...) qFatal(__VA_ARGS__)
#define fltError(...) qError(__VA_ARGS__)
#define fltWarn(...)  qWarn(__VA_ARGS__)
#define fltInfo(...)  qInfo(__VA_ARGS__)
#define fltDebug(...) qDebug(__VA_ARGS__)
#define fltTrace(...) qTrace(__VA_ARGS__)

#define FLT_CHK_JMP(c) \
  do {                 \
    if (c) {           \
      goto _return;    \
    }                  \
  } while (0)
#define FLT_ERR_RET(c)                \
  do {                                \
    int32_t _code = c;                \
    if (_code != TSDB_CODE_SUCCESS) { \
      terrno = _code;                 \
      return _code;                   \
    }                                 \
  } while (0)
#define FLT_RET(c)                    \
  do {                                \
    int32_t _code = c;                \
    if (_code != TSDB_CODE_SUCCESS) { \
      terrno = _code;                 \
    }                                 \
    return _code;                     \
  } while (0)
#define FLT_ERR_JRET(c)              \
  do {                               \
    code = c;                        \
    if (code != TSDB_CODE_SUCCESS) { \
      terrno = code;                 \
      goto _return;                  \
    }                                \
  } while (0)

#define FILTER_GET_FIELD(i, id)            (&((i)->fields[(id).type].fields[(id).idx]))
#define FILTER_GET_COL_FIELD(i, idx)       (&((i)->fields[FLD_TYPE_COLUMN].fields[idx]))
#define FILTER_GET_COL_FIELD_TYPE(fi)      (((SColumnNode *)((fi)->desc))->node.resType.type)
#define FILTER_GET_COL_FIELD_PRECISION(fi) (((SColumnNode *)((fi)->desc))->node.resType.precision)
#define FILTER_GET_COL_FIELD_SIZE(fi)      (((SColumnNode *)((fi)->desc))->node.resType.bytes)
#define FILTER_GET_COL_FIELD_ID(fi)        (((SColumnNode *)((fi)->desc))->colId)
#define FILTER_GET_COL_FIELD_SLOT_ID(fi)   (((SColumnNode *)((fi)->desc))->slotId)
#define FILTER_GET_COL_FIELD_DESC(fi)      ((SColumnNode *)((fi)->desc))
#define FILTER_GET_COL_FIELD_DATA(fi, ri)  (colDataGetData(((SColumnInfoData *)(fi)->data), (ri)))
#define FILTER_GET_VAL_FIELD_TYPE(fi)      (((SValueNode *)((fi)->desc))->node.resType.type)
#define FILTER_GET_VAL_FIELD_DATA(fi)      ((char *)(fi)->data)
#define FILTER_GET_TYPE(fl)                ((fl)&FLD_TYPE_MAX)

#define FILTER_GROUP_UNIT(i, g, uid)   ((i)->units + (g)->unitIdxs[uid])
#define FILTER_UNIT_LEFT_FIELD(i, u)   FILTER_GET_FIELD(i, (u)->left)
#define FILTER_UNIT_RIGHT_FIELD(i, u)  FILTER_GET_FIELD(i, (u)->right)
#define FILTER_UNIT_RIGHT2_FIELD(i, u) FILTER_GET_FIELD(i, (u)->right2)
#define FILTER_UNIT_DATA_TYPE(u)       ((u)->compare.type)
#define FILTER_UNIT_DATA_PRECISION(u)  ((u)->compare.precision)
#define FILTER_UNIT_COL_DESC(i, u)     FILTER_GET_COL_FIELD_DESC(FILTER_UNIT_LEFT_FIELD(i, u))
#define FILTER_UNIT_COL_DATA(i, u, ri) FILTER_GET_COL_FIELD_DATA(FILTER_UNIT_LEFT_FIELD(i, u), ri)
#define FILTER_UNIT_COL_SIZE(i, u)     FILTER_GET_COL_FIELD_SIZE(FILTER_UNIT_LEFT_FIELD(i, u))
#define FILTER_UNIT_COL_ID(i, u)       FILTER_GET_COL_FIELD_ID(FILTER_UNIT_LEFT_FIELD(i, u))
#define FILTER_UNIT_VAL_DATA(i, u)     FILTER_GET_VAL_FIELD_DATA(FILTER_UNIT_RIGHT_FIELD(i, u))
#define FILTER_UNIT_COL_IDX(u)         ((u)->left.idx)
#define FILTER_UNIT_OPTR(u)            ((u)->compare.optr)
#define FILTER_UNIT_COMP_FUNC(u)       ((u)->compare.func)

#define FILTER_UNIT_CLR_F(i)         memset((i)->unitFlags, 0, (i)->unitNum * sizeof(*info->unitFlags))
#define FILTER_UNIT_SET_F(i, idx)    (i)->unitFlags[idx] = 1
#define FILTER_UNIT_GET_F(i, idx)    ((i)->unitFlags[idx])
#define FILTER_UNIT_GET_R(i, idx)    ((i)->unitRes[idx])
#define FILTER_UNIT_SET_R(i, idx, v) (i)->unitRes[idx] = (v)

#define FILTER_PUSH_UNIT(colInfo, u)               \
  do {                                             \
    (colInfo).type = RANGE_TYPE_UNIT;              \
    (colInfo).dataType = FILTER_UNIT_DATA_TYPE(u); \
    taosArrayPush((SArray *)((colInfo).info), &u); \
  } while (0)
#define FILTER_PUSH_VAR_HASH(colInfo, ha) \
  do {                                    \
    (colInfo).type = RANGE_TYPE_VAR_HASH; \
    (colInfo).info = ha;                  \
  } while (0)
#define FILTER_PUSH_CTX(colInfo, ctx)   \
  do {                                  \
    (colInfo).type = RANGE_TYPE_MR_CTX; \
    (colInfo).info = ctx;               \
  } while (0)

#define FILTER_COPY_IDX(dst, src, n)                 \
  do {                                               \
    *(dst) = taosMemoryMalloc(sizeof(uint32_t) * n); \
    memcpy(*(dst), src, sizeof(uint32_t) * n);       \
  } while (0)

#define FILTER_ADD_CTX_TO_GRES(gres, idx, ctx)                              \
  do {                                                                      \
    if ((gres)->colCtxs == NULL) {                                          \
      (gres)->colCtxs = taosArrayInit(gres->colNum, sizeof(SFilterColCtx)); \
    }                                                                       \
    SFilterColCtx cCtx = {idx, ctx};                                        \
    taosArrayPush((gres)->colCtxs, &cCtx);                                  \
  } while (0)

#define FILTER_ALL_RES(i)   FILTER_GET_FLAG((i)->status, FI_STATUS_ALL)
#define FILTER_EMPTY_RES(i) FILTER_GET_FLAG((i)->status, FI_STATUS_EMPTY)

extern bool          filterDoCompare(__compar_fn_t func, uint8_t optr, void *left, void *right);
extern __compar_fn_t filterGetCompFunc(int32_t type, int32_t optr);
extern __compar_fn_t filterGetCompFuncEx(int32_t lType, int32_t rType, int32_t optr);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_FILTER_INT_H
