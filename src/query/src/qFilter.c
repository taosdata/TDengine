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


filter_desc_compare_func gDescCompare [F_FIELD_MAX] = {
  filterFieldColDescCompare,
  filterFieldValDescCompare
};


int32_t filterMergeGroup(SArray* group, SArray* left, SArray* right) {
  int32_t leftSize = (int32_t)taosArrayGetSize(left);
  int32_t rightSize = (int32_t)taosArrayGetSize(right);

  CHK_LRET(taosArrayGetSize(left) <= 0, TSDB_CODE_QRY_APP_ERROR, "empty group");
  CHK_LRET(taosArrayGetSize(right) <= 0, TSDB_CODE_QRY_APP_ERROR, "empty group");  

  SFilterGroup gp = {0};
  
  for (int32_t l = 0; l < leftSize; ++l) {
    SFilterGroup *gp1 = taosArrayGet(left, l);
    
    for (int32_t r = 0; r < rightSize; ++r) {
      SFilterGroup *gp2 = taosArrayGet(right, r);
      
      gp.unitNum = gp1->unitNum + gp2->unitNum;
      gp.unitIdxs = calloc(gp.unitNum, sizeof(*gp.unitIdxs));
      memcpy(gp.unitIdxs, gp1->unitIdxs, gp1->unitNum * sizeof(*gp.unitIdxs));
      memcpy(gp.unitIdxs + gp1->unitNum, gp2->unitIdxs, gp2->unitNum * sizeof(*gp.unitIdxs));

      gp.unitFlags = NULL;
      
      taosArrayPush(group, &gp);
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


SFilterField* filterAddField(SFilterInfo *info, tExprNode *node) {
  CHK_LRET(node == NULL, NULL, "empty node");
  CHK_LRET(node->nodeType != TSQL_NODE_COL && node->nodeType != TSQL_NODE_VALUE, NULL, "invalid nodeType");
  int32_t type, idx = -1;
  uint16_t *num;
  void *v;

  if (node->nodeType == TSQL_NODE_COL) {
    type = F_FIELD_COLUMN;
    v = node->pSchema;
  } else {
    type = F_FIELD_VALUE;
    v = node->pVal;
  }

  num = &info->fileds[type].num;

  if (num > 0) {
    idx = filterGetFiled(&info->fileds[type], type, v);
  }
  
  if (idx < 0) {
    idx = *num;
    info->fileds[type].fields[idx].type = type;  
    info->fileds[type].fields[idx].desc = v;
    ++(*num);
  }
  
  return &info->fileds[type].fields[idx];
}

int32_t filterAddUnit(SFilterInfo *info, uint8_t optr, SFilterField *left, SFilterField *right) {
  info->units[info->unitNum].compare.optr = optr;
  info->units[info->unitNum].left = left;
  info->units[info->unitNum].right = right;

  ++info->unitNum;
  
  return TSDB_CODE_SUCCESS;
}

int32_t filterAddGroup(SFilterGroup *group, uint16_t unitIdx) {
  group->unitNum = 1;
  group->unitIdxs= calloc(1, sizeof(*group->unitIdxs));
  group->unitIdxs[0] = unitIdx;

  return TSDB_CODE_SUCCESS;
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

    ERR_JRET(filterMergeGroup(group, leftGroup, rightGroup));
    
    return TSDB_CODE_SUCCESS;
  }

  if (tree->_node.optr == TSDB_RELATION_OR) {
    ERR_RET(filterTreeToGroup(tree->_node.pLeft, info, group));
    ERR_RET(filterTreeToGroup(tree->_node.pRight, info, group));

    return TSDB_CODE_SUCCESS;
  }

  SFilterField *left = filterAddField(info, tree->_node.pLeft);  
  SFilterField *right = filterAddField(info, tree->_node.pRight);  
  
  filterAddUnit(info, tree->_node.optr, left, right);  

  SFilterGroup fgroup = {0};
  filterAddGroup(&fgroup, info->unitNum - 1);
  
  taosArrayPush(group, &fgroup);

_err_return:

  taosArrayDestroy(leftGroup);
  taosArrayDestroy(rightGroup);
  
  return code;
}

void filterDumpInfoToString(SFilterInfo *info) {
  CHK_LRETV(info == NULL, "FilterInfo: empty");

  qDebug("FilterInfo:");
  qDebug("Col F Num:%u", info->fileds[F_FIELD_COLUMN].num);
  for (uint16_t i = 0; i < info->fileds[F_FIELD_COLUMN].num; ++i) {
    SFilterField *field = &info->fileds[F_FIELD_COLUMN].fields[i];
    SSchema *sch = field->desc;
    qDebug("COL%d => [%d][%s]", i, sch->colId, sch->name);
  }

  qDebug("Unit  Num:%u", info->unitNum);
  for (uint16_t i = 0; i < info->unitNum; ++i) {
    SFilterUnit *unit = &info->units[i];
    SFilterField *left = unit->left;
    SFilterField *right = unit->right;

    SSchema *sch = left->desc;
    tVariant *var = right->desc;
    qDebug("UNIT%d => [%d][%s]  %s  %" PRId64, i, sch->colId, sch->name, gOptrStr[unit->compare.optr].str, IS_NUMERIC_TYPE(var->nType) ? var->i64 : -1);
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

int32_t filterInitFromTree(tExprNode* tree, SFilterInfo *info, int32_t colSize) {
  int32_t code = TSDB_CODE_SUCCESS;

  CHK_RET(colSize <= 0, code);
  CHK_LRET(tree == NULL || info == NULL, TSDB_CODE_QRY_APP_ERROR, "invalid param");

  SArray* group = taosArrayInit(4, sizeof(SFilterGroup));
  
  info->units = calloc(colSize, sizeof(SFilterUnit));
  
  info->fileds[F_FIELD_COLUMN].num = 0;
  info->fileds[F_FIELD_COLUMN].fields = calloc(colSize, sizeof(SFilterField));
  info->fileds[F_FIELD_VALUE].num = 0;
  info->fileds[F_FIELD_VALUE].fields = calloc(colSize, sizeof(SFilterField));

  code = filterTreeToGroup(tree, info, group);

  ERR_RET(code);

  size_t groupSize = taosArrayGetSize(group);

  info->groupNum = (uint16_t)groupSize;

  if (info->groupNum > 0) {
    info->groups = calloc(info->groupNum, sizeof(*info->groups));
  }

  for (size_t i = 0; i < groupSize; ++i) {
    SFilterGroup *pg = taosArrayGet(group, i);
    info->groups[i].unitNum = pg->unitNum;
    info->groups[i].unitIdxs = pg->unitIdxs;
    info->groups[i].unitFlags = pg->unitFlags;
  }

  filterDumpInfoToString(info);
  
  return code;
}

void filterFreeInfo(SFilterInfo *info) {
  CHK_RETV(info == NULL);

  //TODO
}


