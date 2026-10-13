/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

#include <gtest/gtest.h>

#include <utility>

#include "executorInt.h"
#include "mergejoin.h"
#include "operator.h"

extern "C" int32_t mJoinSetImplFp(SMJoinOperatorInfo* pJoin);

TEST(MergeJoinValidation, RejectInvalidEnumsBeforeInitialization) {
  for (const auto& enums : {std::make_pair(-1, int(JOIN_STYPE_OUTER)),
                           std::make_pair(int(JOIN_TYPE_MAX_VALUE), int(JOIN_STYPE_OUTER)),
                           std::make_pair(int(JOIN_TYPE_LEFT), -1),
                           std::make_pair(int(JOIN_TYPE_LEFT), int(JOIN_STYPE_MAX_VALUE)),
                           std::make_pair(int(JOIN_TYPE_INNER), int(JOIN_STYPE_SEMI)),
                           std::make_pair(int(JOIN_TYPE_FULL), int(JOIN_STYPE_ANTI)),
                           std::make_pair(int(JOIN_TYPE_LEFT), int(JOIN_STYPE_NONE))}) {
    SSortMergeJoinPhysiNode node = {};
    node.joinType = static_cast<EJoinType>(enums.first);
    node.subType = static_cast<EJoinSubType>(enums.second);
    SOperatorInfo* pOperator = reinterpret_cast<SOperatorInfo*>(1);
    EXPECT_EQ(TSDB_CODE_OPS_NOT_SUPPORT, createMergeJoinOperatorInfo(nullptr, 0, &node, nullptr, &pOperator));
    EXPECT_EQ(nullptr, pOperator);
    SMJoinOperatorInfo info = {};
    info.joinType = node.joinType;
    info.subType = node.subType;
    EXPECT_EQ(TSDB_CODE_OPS_NOT_SUPPORT, mJoinSetImplFp(&info));
    EXPECT_EQ(nullptr, info.joinFp);
  }
}

TEST(MergeJoinValidation, RejectMissingAsofImplementation) {
  SMJoinOperatorInfo info = {};
  info.joinType = JOIN_TYPE_LEFT;
  info.subType = JOIN_STYPE_ASOF;
  EXPECT_EQ(TSDB_CODE_OPS_NOT_SUPPORT, mJoinSetImplFp(&info));
}

TEST(MergeJoinValidation, AcceptOrdinaryAndWindowImplementations) {
  for (auto type : {JOIN_TYPE_INNER, JOIN_TYPE_LEFT, JOIN_TYPE_RIGHT, JOIN_TYPE_FULL}) {
    for (auto subType : {JOIN_STYPE_NONE, JOIN_STYPE_OUTER, JOIN_STYPE_SEMI, JOIN_STYPE_ANTI, JOIN_STYPE_WIN}) {
      if ((type == JOIN_TYPE_INNER && subType != JOIN_STYPE_NONE) ||
          (type == JOIN_TYPE_FULL && subType != JOIN_STYPE_OUTER) ||
          ((type == JOIN_TYPE_LEFT || type == JOIN_TYPE_RIGHT) && subType == JOIN_STYPE_NONE)) continue;
      SMJoinOperatorInfo info = {};
      info.joinType = type;
      info.subType = subType;
      EXPECT_EQ(TSDB_CODE_SUCCESS, mJoinSetImplFp(&info));
      EXPECT_NE(nullptr, info.joinFp);
    }
  }
}

TEST(MergeJoinValidation, PreserveInitializedAsofImplementation) {
  SMJoinOperatorInfo info = {};
  info.joinType = JOIN_TYPE_LEFT;
  info.subType = JOIN_STYPE_ASOF;
  joinImplFp initialized = [](SOperatorInfo*) -> SSDataBlock* { return nullptr; };
  info.joinFp = initialized;
  EXPECT_EQ(TSDB_CODE_SUCCESS, mJoinSetImplFp(&info));
  EXPECT_EQ(initialized, info.joinFp);
}
