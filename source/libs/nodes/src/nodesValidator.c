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

#include "nodesValidator.h"

int32_t doValidateBasePhysiNode(SPhysiNode* pNode) {
  if (pNode->inputTsOrder == 0 || pNode->outputTsOrder == 0) return TSDB_CODE_PLAN_INTERNAL_ERROR;
  if (!pNode->pOutputDataBlockDesc || !pNode->pOutputDataBlockDesc->pSlots) return TSDB_CODE_PLAN_INTERNAL_ERROR;
  // check conditions
  return TSDB_CODE_SUCCESS;
}

int32_t doValidateScanPhysiNode(SScanPhysiNode* pNode) {
  int32_t code = TSDB_CODE_SUCCESS;
  code = doValidateBasePhysiNode(&pNode->node);
  return code;
}

int32_t doValidateTableScanPhysiNode(STableScanPhysiNode* pNode) {
  int32_t code = TSDB_CODE_SUCCESS;
  code = doValidateScanPhysiNode(&pNode->scan);
  return code;
}

