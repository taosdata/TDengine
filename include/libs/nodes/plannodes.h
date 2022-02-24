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
#include "tmsg.h"

typedef struct SLogicNode {
  ENodeType type;
  int32_t id;
  SNodeList* pTargets; // SColumnNode
  SNode* pConditions;
  SNodeList* pChildren;
  struct SLogicNode* pParent;
} SLogicNode;

typedef enum EScanType {
  SCAN_TYPE_TAG,
  SCAN_TYPE_TABLE,
  SCAN_TYPE_STABLE,
  SCAN_TYPE_STREAM
} EScanType;

typedef struct SScanLogicNode {
  SLogicNode node;
  SNodeList* pScanCols;
  struct STableMeta* pMeta;
  EScanType scanType;
  uint8_t scanFlag;         // denotes reversed scan of data or not
  STimeWindow scanRange;
} SScanLogicNode;

typedef struct SJoinLogicNode {
  SLogicNode node;
  EJoinType joinType;
  SNode* pOnConditions;
} SJoinLogicNode;

typedef struct SAggLogicNode {
  SLogicNode node;
  SNodeList* pGroupKeys;
  SNodeList* pAggFuncs;
} SAggLogicNode;

typedef struct SProjectLogicNode {
  SLogicNode node;
  SNodeList* pProjections;
} SProjectLogicNode;

typedef struct SSlotDescNode {
  ENodeType type;
  int16_t slotId;
  SDataType dataType;
  int16_t srcTupleId;
  int16_t srcSlotId;
  bool reserve;
  bool output;
} SSlotDescNode;

typedef struct STupleDescNode {
  ENodeType type;
  int16_t tupleId;
  SNodeList* pSlots;
} STupleDescNode;

typedef struct SPhysiNode {
  ENodeType type;
  STupleDescNode outputTuple;
  SNode* pConditions;
  SNodeList* pChildren;
  struct SPhysiNode* pParent;
} SPhysiNode;

typedef struct SScanPhysiNode {
  SPhysiNode  node;
  SNodeList* pScanCols;
  uint64_t uid;           // unique id of the table
  int8_t tableType;
  int32_t order;         // scan order: TSDB_ORDER_ASC|TSDB_ORDER_DESC
  int32_t count;         // repeat count
  int32_t reverse;       // reverse scan count
} SScanPhysiNode;

typedef SScanPhysiNode SSystemTableScanPhysiNode;
typedef SScanPhysiNode STagScanPhysiNode;

typedef struct STableScanPhysiNode {
  SScanPhysiNode scan;
  uint8_t scanFlag;         // denotes reversed scan of data or not
  STimeWindow scanRange;
} STableScanPhysiNode;

typedef STableScanPhysiNode STableSeqScanPhysiNode;

typedef struct SProjectPhysiNode {
  SPhysiNode node;
  SNodeList* pProjections;
} SProjectPhysiNode;

#ifdef __cplusplus
}
#endif

#endif /*_TD_PLANN_NODES_H_*/
