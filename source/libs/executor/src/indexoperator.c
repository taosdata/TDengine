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

#include "indexoperator.h"
#include "executorimpl.h"
// construct tag filter operator later
static void destroyTagFilterOperatorInfo(void *param) {
  STagFilterOperatorInfo *pInfo = (STagFilterOperatorInfo *)param;
}
int32_t doFilterTag(const SNode *pFilterNode, SArray *resutl) {
  if (pFilterNode == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  SFilterInfo *filter = NULL;

  // todo move to the initialization function
  int32_t code = filterInitFromNode((SNode *)pFilterNode, &filter, 0);
  return code;
}
