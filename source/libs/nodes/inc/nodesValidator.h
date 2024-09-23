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

#include "plannodes.h"

int32_t doValidateBasePhysiNode(SPhysiNode* pNode);

int32_t doValidateScanPhysiNode(SScanPhysiNode* pNode);
int32_t doValidateTableScanPhysiNode(STableScanPhysiNode* pNode);
int32_t doValidateTagScanPhysiNode(STagScanPhysiNode* pNode);
int32_t doValidateLastRowScanPhysiNode(SLastRowScanPhysiNode* pNode);
int32_t doValidateSystemTableScanPhysiNode(SSystemTableScanPhysiNode* pNode);

int32_t doValidateProjectPhysiNode(SProjectPhysiNode* pNode);

int32_t doValidateIndefRowsFuncPhysiNode(SIndefRowsFuncPhysiNode* pNode);
int32_t doValidateInterpFuncPhysiNode(SInterpFuncPhysiNode* pNode);

int32_t doValidateSortMergeJoinPhysiNode(SSortMergeJoinPhysiNode* pNode);
int32_t doValidateHashJoinPhysiNode(SHashJoinPhysiNode* pNode);

int32_t doValidateGroupCachePhysiNode(SGroupCachePhysiNode* pNode);

int32_t doValidateDynQueryCtrlPhysiNode(SDynQueryCtrlPhysiNode* pNode);

int32_t doValidateAggPhysiNode(SAggPhysiNode* pNode);

int32_t doValidateExchangePhysiNode(SExchangePhysiNode* pNode);

int32_t doValidateMergePhysiNode(SMergePhysiNode* pNode);

int32_t doValidateWindowPhysiNode(SWindowPhysiNode* pNode);
int32_t doValidateIntervalPhysiNode(SIntervalPhysiNode* pNode);

int32_t doValidateFillPhysiNode(SFillPhysiNode* pNode);

int32_t doValidateMultiTableIntervalPhysiNode(SMultiTableIntervalPhysiNode* pNode);

int32_t doValidateSessionWindowPhysiNode(SSessionWindowPhysiNode* pNode);

int32_t doValidateStateWindowPhysiNode(SStateWindowPhysiNode* pNode);

int32_t doValidateEventWindowPhysiNode(SEventWindowPhysiNode* pNode);

int32_t doValidateCountWindowPhysiNode(SCountWindowPhysiNode* pNode);

int32_t doValidateSortPhysiNode(SSortPhysiNode* pNode);

int32_t doValidatePartitionPhysiNode(SPartitionPhysiNode* pNode);

