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


int32_t filterAddField(SFilterInfo *info, tExprNode *node, SFilterFieldId *fid) {
  CHK_LRET(node == NULL, TSDB_CODE_QRY_APP_ERROR, "empty node");
  CHK_LRET(node->nodeType != TSQL_NODE_COL && node->nodeType != TSQL_NODE_VALUE, TSDB_CODE_QRY_APP_ERROR, "invalid nodeType");
  
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

  num = &info->fields[type].num;

  if (*num > 0) {
    idx = filterGetFiled(&info->fields[type], type, v);
  }
  
  if (idx < 0) {
    idx = *num;
    if (idx >= info->fields[type].size) {
      info->fields[type].size += FILTER_DEFAULT_FIELD_SIZE;
      info->fields[type].fields = realloc(info->fields[type].fields, info->fields[type].size * sizeof(SFilterField));
    }
    
    info->fields[type].fields[idx].type = type;  
    info->fields[type].fields[idx].desc = v;
    ++(*num);
  }

  fid->type = type;
  fid->idx = idx;
  
  return TSDB_CODE_SUCCESS;
}

int32_t filterAddUnit(SFilterInfo *info, uint8_t optr, SFilterFieldId *left, SFilterFieldId *right) {
  if (info->unitNum >= info->unitSize) {
    info->unitSize += FILTER_DEFAULT_UNIT_SIZE;
    info->units = realloc(info->units, info->unitSize * sizeof(SFilterUnit));
  }
  
  info->units[info->unitNum].compare.optr = optr;
  info->units[info->unitNum].left = *left;
  info->units[info->unitNum].right = *right;

  ++info->unitNum;
  
  return TSDB_CODE_SUCCESS;
}

int32_t filterAddGroup(SFilterGroup *group, uint16_t unitIdx) {
  group->unitNum = 1;
  group->unitIdxs= calloc(group->unitNum, sizeof(*group->unitIdxs));
  group->unitIdxs[0] = unitIdx;

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

  SFilterFieldId left, right;
  filterAddField(info, tree->_node.pLeft, &left);  
  filterAddField(info, tree->_node.pRight, &right);  
  
  filterAddUnit(info, tree->_node.optr, &left, &right);  

  SFilterGroup fgroup = {0};
  filterAddGroup(&fgroup, info->unitNum - 1);
  
  taosArrayPush(group, &fgroup);

_err_return:

  taosArrayDestroyEx(leftGroup, filterFreeGroup);
  taosArrayDestroyEx(rightGroup, filterFreeGroup);
  
  return code;
}

int32_t filterInitUnitFunc(SFilterInfo *info) {
  for (uint16_t i = 0; i < info->unitNum; ++i) {
    SFilterUnit* unit = &info->units[i];
    SFilterField *left = FILTER_GET_FIELD(info, unit->left);
  
    unit->compare.pCompareFunc = getComparFunc(left->type, unit->compare.optr);
  }

  return TSDB_CODE_SUCCESS;
}



void filterDumpInfoToString(SFilterInfo *info) {
  CHK_LRETV(info == NULL, "FilterInfo: empty");

  qDebug("FilterInfo:");
  qDebug("Field Col Num:%u", info->fields[F_FIELD_COLUMN].num);
  for (uint16_t i = 0; i < info->fields[F_FIELD_COLUMN].num; ++i) {
    SFilterField *field = &info->fields[F_FIELD_COLUMN].fields[i];
    SSchema *sch = field->desc;
    qDebug("COL%d => [%d][%s]", i, sch->colId, sch->name);
  }

  qDebug("Field Val Num:%u", info->fields[F_FIELD_VALUE].num);
  for (uint16_t i = 0; i < info->fields[F_FIELD_VALUE].num; ++i) {
    SFilterField *field = &info->fields[F_FIELD_VALUE].fields[i];
    tVariant *var = field->desc;
    qDebug("VAL%d => [type:%d][val:%" PRIu64"]", i, var->nType, var->u64); //TODO
  }

  qDebug("Unit  Num:%u", info->unitNum);
  for (uint16_t i = 0; i < info->unitNum; ++i) {
    SFilterUnit *unit = &info->units[i];
    SFilterField *left = FILTER_GET_FIELD(info, unit->left);
    SFilterField *right = FILTER_GET_FIELD(info, unit->right);

    SSchema *sch = left->desc;
    tVariant *var = right->desc;
    qDebug("UNIT%d => [%d][%s]  %s  %" PRId64, i, sch->colId, sch->name, gOptrStr[unit->compare.optr].str, IS_NUMERIC_TYPE(var->nType) ? var->i64 : -1); //TODO
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

int32_t filterInitFromTree(tExprNode* tree, SFilterInfo **pinfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  SFilterInfo *info = NULL;
  
  CHK_LRET(tree == NULL || pinfo == NULL, TSDB_CODE_QRY_APP_ERROR, "invalid param");

  if (*pinfo == NULL) {
    *pinfo = calloc(1, sizeof(SFilterInfo));
  }

  info = *pinfo;

  SArray* group = taosArrayInit(4, sizeof(SFilterGroup));

  info->unitSize = FILTER_DEFAULT_UNIT_SIZE;
  info->units = calloc(info->unitSize, sizeof(SFilterUnit));
  
  info->fields[F_FIELD_COLUMN].num = 0;
  info->fields[F_FIELD_COLUMN].size = FILTER_DEFAULT_FIELD_SIZE;
  info->fields[F_FIELD_COLUMN].fields = calloc(info->fields[F_FIELD_COLUMN].size, sizeof(SFilterField));
  info->fields[F_FIELD_VALUE].num = 0;
  info->fields[F_FIELD_VALUE].size = FILTER_DEFAULT_FIELD_SIZE;
  info->fields[F_FIELD_VALUE].fields = calloc(info->fields[F_FIELD_VALUE].size, sizeof(SFilterField));

  code = filterTreeToGroup(tree, info, group);

  ERR_JRET(code);

  size_t groupSize = taosArrayGetSize(group);

  info->groupNum = (uint16_t)groupSize;

  if (info->groupNum > 0) {
    info->groups = calloc(info->groupNum, sizeof(*info->groups));
  }

  for (size_t i = 0; i < groupSize; ++i) {
    SFilterGroup *pg = taosArrayGet(group, i);
    info->groups[i] = *pg;
  }

  ERR_JRET(filterInitUnitFunc(info));

  info->unitRes = malloc(info->unitNum * sizeof(*info->unitRes));
  info->unitFlags = malloc(info->unitNum * sizeof(*info->unitFlags));

  filterDumpInfoToString(info);

_err_return:
  
  taosArrayDestroy(group);

  return code;
}

void filterFreeInfo(SFilterInfo *info) {
  CHK_RETV(info == NULL);

  //TODO
}

int32_t filterSetColData(SFilterInfo *info, int16_t colId, void *data) {
  CHK_LRET(info == NULL, TSDB_CODE_QRY_APP_ERROR, "info NULL");
  CHK_LRET(info->fields[F_FIELD_COLUMN].num <= 0, TSDB_CODE_QRY_APP_ERROR, "no column fileds");

  for (uint16_t i = 0; i < info->fields[F_FIELD_COLUMN].num; ++i) {
    SFilterField* fi = &info->fields[F_FIELD_COLUMN].fields[i];
    SSchema* sch = fi->desc;
    if (sch->colId == colId) {
      fi->data = data;
      break;
    }
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


bool filterExecute(SFilterInfo *info, int32_t numOfRows, int8_t* p) {
  bool all = true;

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
          SFilterField *left = FILTER_GET_FIELD(info, unit->left);
          SFilterField *right = FILTER_GET_FIELD(info, unit->right);

          ures = filterDoCompare(unit, FILTER_GET_COL_FIELD_DATA(left, i), FILTER_GET_VAL_FIELD_DATA(right));

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




