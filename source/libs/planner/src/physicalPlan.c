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

SPhyNode* createScanNode(SQueryPlanNode* pPlanNode) {
  return NULL;
}

SPhyNode* createPhyNode(SQueryPlanNode* node) {
  switch (node->info.type) {
    case LP_SCAN:
      return createScanNode(node);
  }
  return NULL;
}

SPhyNode* createSubplan(SQueryPlanNode* pSubquery) {
  return NULL;
}

int32_t createDag(struct SQueryPlanNode* pQueryNode, struct SEpSet* pQnode, struct SQueryDag** pDag) {
  return 0;
}
