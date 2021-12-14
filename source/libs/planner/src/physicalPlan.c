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

#include "plannerInt.h"

// typedef struct SQueryPlanNode {
//   void               *pExtInfo;     // additional information
//   SArray             *pPrevNodes;   // children
//   struct SQueryPlanNode  *nextNode; // parent 
// } SQueryPlanNode;

// typedef struct SSubplan {
//   int32_t   type;               // QUERY_TYPE_MERGE|QUERY_TYPE_PARTIAL|QUERY_TYPE_SCAN
//   SArray   *pDatasource;          // the datasource subplan,from which to fetch the result
//   struct SPhyNode *pNode;  // physical plan of current subplan
// } SSubplan;

// typedef struct SQueryDag {
//   SArray  **pSubplans;
// } SQueryDag;

// typedef struct SScanPhyNode {
//   SPhyNode    node;
//   STimeWindow window;
//   uint64_t    uid;  // unique id of the table
// } SScanPhyNode;

// typedef SScanPhyNode STagScanPhyNode;

void fillDataBlockSchema(SQueryPlanNode* pPlanNode, SDataBlockSchema* dataBlockSchema) {
  dataBlockSchema->index = 0; // todo
  SWAP(dataBlockSchema->pSchema, pPlanNode->pSchema, SSchema*);
  dataBlockSchema->numOfCols = pPlanNode->numOfCols;
}

void fillPhyNode(SQueryPlanNode* pPlanNode, int32_t type, const char* name, SPhyNode* node) {
  node->info.type = type;
  node->info.name = name;
  SWAP(node->pTargets, pPlanNode->pExpr, SArray*);
  fillDataBlockSchema(pPlanNode, &(node->targetSchema));
}

SPhyNode* createTagScanNode(SQueryPlanNode* pPlanNode) {
  STagScanPhyNode* node = calloc(1, sizeof(STagScanPhyNode));
  fillPhyNode(pPlanNode, OP_TagScan, "TagScan", (SPhyNode*)node);
  return (SPhyNode*)node;
}

SPhyNode* createScanNode(SQueryPlanNode* pPlanNode) {
  STagScanPhyNode* node = calloc(1, sizeof(STagScanPhyNode));
  fillPhyNode(pPlanNode, OP_TableScan, "SingleTableScan", (SPhyNode*)node);
  return (SPhyNode*)node;
}

SPhyNode* createPhyNode(SQueryPlanNode* pPlanNode) {
  SPhyNode* node = NULL;
  switch (pPlanNode->info.type) {
    case QNODE_TAGSCAN:
      node = createTagScanNode(pPlanNode);
      break;
    case QNODE_TABLESCAN:
      node = createScanNode(pPlanNode);
      break;
    default:
      assert(false);
  }
  if (pPlanNode->pChildren != NULL && taosArrayGetSize(pPlanNode->pChildren) > 0) {
    node->pChildren = taosArrayInit(4, POINTER_BYTES);
    size_t size = taosArrayGetSize(pPlanNode->pChildren);
    for(int32_t i = 0; i < size; ++i) {
      SPhyNode* child = createPhyNode(taosArrayGet(pPlanNode->pChildren, i));
      child->pParent = node;
      taosArrayPush(node->pChildren, &child);
    }
  }
  return node;
}

SSubplan* createSubplan(SQueryPlanNode* pSubquery) {
  SSubplan* subplan = calloc(1, sizeof(SSubplan));
  subplan->pNode = createPhyNode(pSubquery);
  // todo
  return subplan;
}

int32_t createDag(struct SQueryPlanNode* pQueryNode, struct SEpSet* pQnode, struct SQueryDag** pDag) {
  return 0;
}
