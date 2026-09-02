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

#include "planTestUtil.h"
#include "planner.h"

using namespace std;

class PlanLimitTest : public PlannerTestBase {};

static SNode* makeValueNode(int64_t value) {
  SValueNode* pNode = NULL;
  int32_t     code = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&pNode);
  EXPECT_EQ(TSDB_CODE_SUCCESS, code);
  pNode->node.resType.type = TSDB_DATA_TYPE_BIGINT;
  pNode->translate = true;
  code = nodesSetValueNodeValue(pNode, &value);
  EXPECT_EQ(TSDB_CODE_SUCCESS, code);
  return (SNode*)pNode;
}

static SNode* makeLimitNode(bool hasLimit, int64_t limit, bool hasOffset, int64_t offset) {
  SLimitNode* pLimit = NULL;
  int32_t     code = nodesMakeNode(QUERY_NODE_LIMIT, (SNode**)&pLimit);
  EXPECT_EQ(TSDB_CODE_SUCCESS, code);
  if (hasLimit) {
    pLimit->limit = (SValueNode*)makeValueNode(limit);
  }
  if (hasOffset) {
    pLimit->offset = (SValueNode*)makeValueNode(offset);
  }
  return (SNode*)pLimit;
}

TEST_F(PlanLimitTest, limit) {
  useDb("root", "test");

  run("select * from t1 limit 2");

  run("select * from t1 limit 5 offset 2");

  run("select * from t1 limit 2, 5");
}

TEST_F(PlanLimitTest, offsetWithoutLimit) {
  useDb("root", "test");

  run("select * from t1 offset 2");

  run("select * from t1 order by ts offset 2");

  run("select * from t1 order by c1 offset 2");

  run("select * from st1 offset 2");

  run("select * from st1 order by ts offset 2");

  run("select * from st1 order by c1 offset 2");
}

TEST_F(PlanLimitTest, cloneLimitSkipsOffsetWithoutLimit) {
  SLogicNode parent;
  SLogicNode child;
  bool       cloned = true;
  memset(&parent, 0, sizeof(parent));
  memset(&child, 0, sizeof(child));

  parent.pLimit = makeLimitNode(false, 0, true, 2);
  int32_t code = cloneLimit(&parent, &child, CLONE_LIMIT, &cloned);

  ASSERT_EQ(TSDB_CODE_SUCCESS, code);
  ASSERT_FALSE(cloned);
  ASSERT_EQ(child.pLimit, nullptr);

  nodesDestroyNode(parent.pLimit);
}

TEST_F(PlanLimitTest, cloneLimitPushesFiniteLimitWithOffset) {
  SLogicNode parent;
  SLogicNode child;
  bool       cloned = false;
  memset(&parent, 0, sizeof(parent));
  memset(&child, 0, sizeof(child));

  parent.pLimit = makeLimitNode(true, 5, true, 2);
  int32_t code = cloneLimit(&parent, &child, CLONE_LIMIT, &cloned);

  ASSERT_EQ(TSDB_CODE_SUCCESS, code);
  ASSERT_TRUE(cloned);
  ASSERT_NE(child.pLimit, nullptr);
  ASSERT_EQ(((SLimitNode*)child.pLimit)->limit->datum.i, 7);
  ASSERT_EQ(((SLimitNode*)child.pLimit)->offset->datum.i, 0);

  nodesDestroyNode(parent.pLimit);
  nodesDestroyNode(child.pLimit);
}

TEST_F(PlanLimitTest, slimit) {
  useDb("root", "test");

  run("select * from t1 partition by c1 slimit 2");

  run("select * from t1 partition by c1 slimit 5 soffset 2");

  run("select * from t1 partition by c1 slimit 2, 5");
}
