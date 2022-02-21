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

#ifndef _TD_PLANN_NODES_H_
#define _TD_PLANN_NODES_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "querynodes.h"

typedef struct SLogicNode {
  ENodeType type;
  int32_t id;
  SNodeList* pTargets; // SColumnNode
  SNode* pConditions;
  SNodeList* pChildren;
  struct SLogicNode* pParent;
} SLogicNode;

typedef struct SScanLogicNode {
  SLogicNode node;
  SNodeList* pScanCols;
  struct STableMeta* pMeta;
} SScanLogicNode;

typedef struct SJoinLogicNode {
  SLogicNode node;
  EJoinType joinType;
  SNode* pOnConditions;
} SJoinLogicNode;

typedef struct SFilterLogicNode {
  SLogicNode node;
} SFilterLogicNode;

typedef struct SAggLogicNode {
  SLogicNode node;
  SNodeList* pGroupKeys;
  SNodeList* pAggFuncs;
} SAggLogicNode;

typedef struct SProjectLogicNode {
  SLogicNode node;
  SNodeList* pProjections;
} SProjectLogicNode;

#ifdef __cplusplus
}
#endif

#endif /*_TD_PLANN_NODES_H_*/
