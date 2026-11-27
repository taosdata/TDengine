// Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
//
// This program is free software: you can use, redistribute, and/or modify
// it under the terms of the GNU Affero General Public License, version 3
// or later ("AGPL"), as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
// FITNESS FOR A PARTICULAR PURPOSE.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

#include "parTestUtil.h"
#include "parAst.h"
#include "stub.h"

#include <cstdlib>
#include <set>
#include <string>

namespace ParserTest {
namespace {

std::multiset<SNode*> g_listDestroyedNodes;

void recordListNodeDestroy(SNode* pNode) {
  if (pNode != nullptr) {
    g_listDestroyedNodes.insert(pNode);
  }
}

int32_t failListAppend(SNodeList*, SNode*) {
  return TSDB_CODE_OUT_OF_MEMORY;
}

int32_t failMakeList(SNodeList**) {
  return TSDB_CODE_OUT_OF_MEMORY;
}

Stub* g_astStub = nullptr;

int32_t failCaseWhenNode(ENodeType type, SNode** ppNode) {
  if (type == QUERY_NODE_CASE_WHEN) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  g_astStub->reset(nodesMakeNode);
  int32_t code = nodesMakeNode(type, ppNode);
  g_astStub->set(nodesMakeNode, failCaseWhenNode);
  return code;
}

// The conditional constructors differ in arity, so each gets a trampoline
// with a uniform signature; unused parameters are ignored by design.
SNode* invokeNullIf(SAstCreateContext* pCxt, SNode* first, SNode* second,
                    SNode*, SNodeList*) {
  return createNullIfNode(pCxt, first, second);
}

SNode* invokeIf(SAstCreateContext* pCxt, SNode* first, SNode* second,
                SNode* third, SNodeList*) {
  return createIfNode(pCxt, first, second, third);
}

SNode* invokeNvl(SAstCreateContext* pCxt, SNode* first, SNode* second, SNode*,
                 SNodeList*) {
  return createNvlNode(pCxt, first, second);
}

SNode* invokeNvl2(SAstCreateContext* pCxt, SNode* first, SNode* second,
                  SNode* third, SNodeList*) {
  return createNvl2Node(pCxt, first, second, third);
}

SNode* invokeCoalesce(SAstCreateContext* pCxt, SNode*, SNode*, SNode*,
                      SNodeList* pParams) {
  return createCoalesceNode(pCxt, pParams);
}

struct AstCase {
  const char* name;
  bool        needsThird;
  bool        usesParamList;
  SNode* (*invoke)(SAstCreateContext*, SNode*, SNode*, SNode*, SNodeList*);
};

const AstCase kAstCases[] = {
    {"createNullIfNode", false, false, invokeNullIf},
    {"createIfNode", true, false, invokeIf},
    {"createNvlNode", false, false, invokeNvl},
    {"createNvl2Node", true, false, invokeNvl2},
    {"createCoalesceNode", false, true, invokeCoalesce},
};

struct AstFailure {
  const char* name;
  void (*arm)(Stub& stub);
};

const AstFailure kAstFailures[] = {
    {"nodesListAppend", [](Stub& stub) { stub.set(nodesListAppend,
                                                  failListAppend); }},
    {"nodesMakeList", [](Stub& stub) { stub.set(nodesMakeList,
                                                failMakeList); }},
    {"nodesMakeNode/CASE_WHEN", [](Stub& stub) { stub.set(nodesMakeNode,
                                                          failCaseWhenNode); }},
};

SNodeList* g_colsParamList = nullptr;
std::multiset<void*> g_colsFreedMemory;

int32_t supplyColsParamList(SNodeList** ppList) {
  *ppList = g_colsParamList;
  return TSDB_CODE_SUCCESS;
}

void recordColsMemoryFree(void* ptr) {
  g_colsFreedMemory.insert(ptr);
}

int32_t failListAppendList(SNodeList*, SNodeList*) {
  return TSDB_CODE_FAILED;
}

}  // namespace

TEST(ParserAstTest, colsFunctionParametersReleaseOwnedMemory) {
#ifdef _WIN32
  GTEST_SKIP() << "cppstub patching is unreliable in Windows release builds";
#else
  struct TestCase {
    const char* name;
    int32_t error;
    void (*arm)(Stub&);
  };
  const TestCase cases[] = {
      {"success", TSDB_CODE_SUCCESS, [](Stub&) {}},
      {"nodesMakeList", TSDB_CODE_OUT_OF_MEMORY,
       [](Stub& stub) { stub.set(nodesMakeList, failMakeList); }},
      {"nodesListAppend", TSDB_CODE_OUT_OF_MEMORY,
       [](Stub& stub) { stub.set(nodesListAppend, failListAppend); }},
      {"nodesListAppendList", TSDB_CODE_FAILED,
       [](Stub& stub) { stub.set(nodesListAppendList, failListAppendList); }},
  };
  for (const auto& test : cases) {
    SCOPED_TRACE(test.name);
    SParseContext query = {0};
    SAstCreateContext context;
    initAstCreateContext(&query, &context);
    SNode* raw = nullptr;
    SNode* func = nullptr;
    SNode* column = nullptr;
    ASSERT_EQ(nodesMakeNode(QUERY_NODE_RAW_EXPR, &raw), TSDB_CODE_SUCCESS);
    ASSERT_EQ(nodesMakeNode(QUERY_NODE_FUNCTION, &func), TSDB_CODE_SUCCESS);
    ASSERT_EQ(nodesMakeNode(QUERY_NODE_COLUMN, &column), TSDB_CODE_SUCCESS);
    ((SRawExprNode*)raw)->pNode = func;
    SNodeList* params = createNodeList(&context, column);
    ASSERT_NE(params, nullptr);
    ASSERT_EQ(nodesMakeList(&g_colsParamList), TSDB_CODE_SUCCESS);
    std::set<void*> allocations = {
        raw, func, column, params, params->pHead, g_colsParamList};
    g_colsFreedMemory.clear();
    {
      Stub stub;
      stub.set(nodesMakeList, supplyColsParamList);
      stub.set(nodesFree, recordColsMemoryFree);
      test.arm(stub);
      SNodeList* result =
          createColsFuncParamNodeList(&context, raw, params, nullptr);
      EXPECT_EQ(context.errCode, test.error);
      if (test.error == TSDB_CODE_SUCCESS) {
        EXPECT_EQ(result, g_colsParamList);
        EXPECT_EQ(LIST_LENGTH(result), 2);
        EXPECT_EQ(nodesListGetNode(result, 0), func);
        EXPECT_EQ(nodesListGetNode(result, 1), column);
        EXPECT_EQ(g_colsFreedMemory.count(func), 0);
        EXPECT_EQ(g_colsFreedMemory.count(column), 0);
      } else {
        EXPECT_EQ(result, nullptr);
      }
      nodesDestroyList(result);
      EXPECT_EQ(g_colsFreedMemory.count(raw), 1);
      EXPECT_EQ(g_colsFreedMemory.count(func), 1);
      EXPECT_EQ(g_colsFreedMemory.count(column), 1);
      EXPECT_EQ(g_colsFreedMemory.count(params), 1);
      EXPECT_EQ(g_colsFreedMemory.count(g_colsParamList),
                std::string(test.name) == "nodesMakeList" ? 0 : 1);
      // Keep failed implementations leak-free too; nodesFree is only recorded.
      for (SListCell* cell = g_colsParamList->pHead; cell != nullptr;
           cell = cell->pNext) {
        allocations.insert(cell);
      }
      for (void* ptr : g_colsFreedMemory) {
        EXPECT_EQ(g_colsFreedMemory.count(ptr), 1);
        allocations.insert(ptr);
      }
    }
    for (void* ptr : allocations) {
      nodesFree(ptr);
    }
    g_colsParamList = nullptr;
    g_colsFreedMemory.clear();
  }
#endif
}

TEST(ParserAstTest, conditionalConstructorsReleaseInputsOnFailure) {
#ifdef _WIN32
  GTEST_SKIP() << "cppstub patching is unreliable in Windows release builds";
#else
  testing::GTEST_FLAG(death_test_style) = "threadsafe";
  for (const auto& astCase : kAstCases) {
    for (const auto& failure : kAstFailures) {
      SCOPED_TRACE(std::string(astCase.name) + " / " + failure.name);
      EXPECT_EXIT(
          {
            SParseContext query = {0};
            SAstCreateContext context;
            initAstCreateContext(&query, &context);
            SNode* first = nullptr;
            SNode* second = nullptr;
            SNode* third = nullptr;
            if (nodesMakeNode(QUERY_NODE_COLUMN, &first) != 0 ||
                nodesMakeNode(QUERY_NODE_COLUMN, &second) != 0) {
              std::exit(2);
            }
            if (astCase.needsThird &&
                nodesMakeNode(QUERY_NODE_COLUMN, &third) != 0) {
              std::exit(2);
            }
            SNodeList* params = nullptr;
            if (astCase.usesParamList) {
              params = createNodeList(&context, first);
              params = addNodeToList(&context, params, second);
            }
            Stub stub;
            g_astStub = &stub;
            failure.arm(stub);
            SNode* result =
                astCase.invoke(&context, first, second, third, params);
            std::exit(result == nullptr &&
                              context.errCode == TSDB_CODE_OUT_OF_MEMORY
                          ? 0
                          : 1);
          },
          testing::ExitedWithCode(0), "");
    }
  }
#endif
}

namespace {

SNodeList* parseJoinAlgoHintWithParams(SAstCreateContext* pCxt, int32_t paramCount) {
  std::string hint = "/*+ JOIN_ALGO(";
  for (int32_t i = 0; i < paramCount; ++i) {
    if (i != 0) {
      hint += ',';
    }
    hint += "THETA";
  }
  hint += ") */";

  SToken literal = {0};
  literal.z = &hint[0];
  literal.n = static_cast<uint32_t>(hint.size());
  return createHintNodeList(pCxt, &literal);
}

}  // namespace

// Positive control for the two tests below. Without the keyword table,
// tGetToken() reports JOIN_ALGO as TK_NK_ILLEGAL, createHintNodeList() bails
// on its first token and returns NULL -- which is also what a rejected hint
// returns. Any test that only asserts NULL would then pass while exercising
// none of the hint parsing. Assert the accepting case first so that a silently
// dead harness cannot masquerade as a passing guard.
TEST(ParserAstTest, joinAlgoHintAcceptsSingleParameter) {
  char          message[TSDB_ERROR_MSG_LEN] = {0};
  SParseContext query = {0};
  query.pMsg = message;
  query.msgLen = sizeof(message);

  initKeywordsTableOnce();
  SAstCreateContext context;
  initAstCreateContext(&query, &context);

  SNodeList* result = parseJoinAlgoHintWithParams(&context, 1);

  ASSERT_NE(result, nullptr);
  EXPECT_EQ(LIST_LENGTH(result), 1);
  EXPECT_EQ(context.errCode, TSDB_CODE_SUCCESS);
  nodesDestroyList(result);
}

// Guards the bound on the fixed-size SToken paramList[10] in
// createHintNodeList().
//
// The bound changes nothing the parser can report: every hint rejects a
// multi-parameter list anyway, so both the return value and errCode are the
// same with or without it. The only difference is the out-of-bounds write.
// That write is still observable in an ordinary build -- 11 parameters put a
// full SToken past the end of the array, over the frame's stack canary, so an
// unbounded createHintNodeList() aborts with "stack smashing detected"
// instead of returning. Under BUILD_SANITIZER the same input trips ASan's
// stack redzone first.
//
// So the real assertion here is that the call returns at all. Keep the
// parameter count above 10, and keep the positive control above green -- a
// dead harness turns this into a no-op.
TEST(ParserAstTest, joinAlgoHintDoesNotOverflowParamList) {
  char          message[TSDB_ERROR_MSG_LEN] = {0};
  SParseContext query = {0};
  query.pMsg = message;
  query.msgLen = sizeof(message);

  initKeywordsTableOnce();
  SAstCreateContext context;
  initAstCreateContext(&query, &context);

  SNodeList* result = parseJoinAlgoHintWithParams(&context, 11);

  EXPECT_EQ(result, nullptr);
  EXPECT_EQ(context.errCode, TSDB_CODE_SUCCESS);
  nodesDestroyList(result);
}

TEST(ParserAstTest, listAppendFailureReleasesInputs) {
#ifdef _WIN32
  GTEST_SKIP() << "cppstub patching is unreliable in Windows release builds";
#else
  SParseContext query = {0};
  initKeywordsTableOnce();
  SAstCreateContext context;
  initAstCreateContext(&query, &context);
  SNode* first = nullptr;
  SNode* second = nullptr;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_COLUMN, &first), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_COLUMN, &second), TSDB_CODE_SUCCESS);
  SNodeList* list = createNodeList(&context, first);
  ASSERT_NE(list, nullptr);

  g_listDestroyedNodes.clear();
  {
    Stub stub;
    stub.set(nodesListAppend, failListAppend);
    stub.set(nodesDestroyNode, recordListNodeDestroy);
    SNodeList* result = createNodeList(&context, second);
    EXPECT_EQ(result, nullptr);
    EXPECT_EQ(context.errCode, TSDB_CODE_OUT_OF_MEMORY);
    EXPECT_EQ(g_listDestroyedNodes, std::multiset<SNode*>({second}));

    context.errCode = TSDB_CODE_SUCCESS;
    g_listDestroyedNodes.clear();
    result = addNodeToList(&context, list, second);
    EXPECT_EQ(result, nullptr);
    EXPECT_EQ(context.errCode, TSDB_CODE_OUT_OF_MEMORY);
    EXPECT_EQ(g_listDestroyedNodes,
              std::multiset<SNode*>({second, first}));
    if (result != nullptr) {
      nodesDestroyList(result);
    }
  }
  g_listDestroyedNodes.clear();
  nodesDestroyNode(first);
  nodesDestroyNode(second);
#endif
}

}  // namespace ParserTest
