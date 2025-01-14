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

#include "executorInt.h"
#include "cmdnodes.h"

#define UPDATE_OPERATOR_INFO       BIT_FLAG_MASK(0)
#define FILL_HISTORY_OPERATOR      BIT_FLAG_MASK(1)
#define RECALCULATE_OPERATOR       BIT_FLAG_MASK(2)
#define SEMI_OPERATOR              BIT_FLAG_MASK(3)
#define FINAL_OPERATOR             BIT_FLAG_MASK(4)
#define SINGLE_OPERATOR            BIT_FLAG_MASK(5)

void setStreamOperatorState(SSteamOpBasicInfo* pBasicInfo, EStreamType type) {
  if (type != STREAM_GET_ALL && type != STREAM_CHECKPOINT) {
    BIT_FLAG_SET_MASK(pBasicInfo->operatorFlag, UPDATE_OPERATOR_INFO);
  }
}

bool needSaveStreamOperatorInfo(SSteamOpBasicInfo* pBasicInfo) {
  return BIT_FLAG_TEST_MASK(pBasicInfo->operatorFlag, UPDATE_OPERATOR_INFO);
}

void saveStreamOperatorStateComplete(SSteamOpBasicInfo* pBasicInfo) {
  BIT_FLAG_UNSET_MASK(pBasicInfo->operatorFlag, UPDATE_OPERATOR_INFO);
}

void initStreamBasicInfo(SSteamOpBasicInfo* pBasicInfo) {
  pBasicInfo->primaryPkIndex = -1;
  pBasicInfo->operatorFlag = 0;
}

void setFillHistoryOperatorFlag(SSteamOpBasicInfo* pBasicInfo) {
  BIT_FLAG_SET_MASK(pBasicInfo->operatorFlag, FILL_HISTORY_OPERATOR);
}

bool isHistoryOperator(SSteamOpBasicInfo* pBasicInfo) {
  return BIT_FLAG_TEST_MASK(pBasicInfo->operatorFlag, FILL_HISTORY_OPERATOR);
}

bool needBuildAllResult(SSteamOpBasicInfo* pBasicInfo) {
  return BIT_FLAG_TEST_MASK(pBasicInfo->operatorFlag, FILL_HISTORY_OPERATOR) || BIT_FLAG_TEST_MASK(pBasicInfo->operatorFlag, SEMI_OPERATOR);
}

void setSemiOperatorFlag(SSteamOpBasicInfo* pBasicInfo) {
  BIT_FLAG_SET_MASK(pBasicInfo->operatorFlag, SEMI_OPERATOR);
}

bool isSemiOperator(SSteamOpBasicInfo* pBasicInfo) {
  return BIT_FLAG_TEST_MASK(pBasicInfo->operatorFlag, SEMI_OPERATOR);
}

void setFinalOperatorFlag(SSteamOpBasicInfo* pBasicInfo) {
  BIT_FLAG_SET_MASK(pBasicInfo->operatorFlag, FINAL_OPERATOR);
}

bool isFinalOperator(SSteamOpBasicInfo* pBasicInfo) {
  return BIT_FLAG_TEST_MASK(pBasicInfo->operatorFlag, FINAL_OPERATOR);
}

bool isRecalculateOperator(SSteamOpBasicInfo* pBasicInfo) {
  return BIT_FLAG_TEST_MASK(pBasicInfo->operatorFlag, RECALCULATE_OPERATOR);
}


void setSingleOperatorFlag(SSteamOpBasicInfo* pBasicInfo) {
  BIT_FLAG_SET_MASK(pBasicInfo->operatorFlag, SINGLE_OPERATOR);
}

bool isSingleOperator(SSteamOpBasicInfo* pBasicInfo) {
  return BIT_FLAG_TEST_MASK(pBasicInfo->operatorFlag, SINGLE_OPERATOR);
}

