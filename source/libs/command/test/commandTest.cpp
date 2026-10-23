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

#include <algorithm>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

extern "C" {
#include "command.h"
#include "parInt.h"
#include "plannodes.h"
#include "tdatablock.h"
#include "tglobal.h"
}

TEST(ExplainCorrelationTest, StaticPlanShowsCallUnderOwningOperator) {
  SQueryPlan* pPlan = nullptr;
  SSubplan* pSubplan = nullptr;
  SNodeListNode* pLevel = nullptr;
  SProjectPhysiNode* pProject = nullptr;
  SDataBlockDescNode* pDesc = nullptr;
  SCorrelatedSubqueryNode* pCall = nullptr;
  SCorrelationParamBindingNode* pBinding = nullptr;
  SCorrelationTemplateNode* pTemplate = nullptr;
  SColumnNode* pSource = nullptr;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN, reinterpret_cast<SNode**>(&pPlan)), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN, reinterpret_cast<SNode**>(&pSubplan)), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_NODE_LIST, reinterpret_cast<SNode**>(&pLevel)), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_PROJECT, reinterpret_cast<SNode**>(&pProject)), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_DATABLOCK_DESC, reinterpret_cast<SNode**>(&pDesc)), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_CORRELATED_SUBQUERY, reinterpret_cast<SNode**>(&pCall)), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_CORRELATION_PARAM_BINDING, reinterpret_cast<SNode**>(&pBinding)),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_CORRELATION_TEMPLATE, reinterpret_cast<SNode**>(&pTemplate)),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_COLUMN, reinterpret_cast<SNode**>(&pSource)), TSDB_CODE_SUCCESS);
  pPlan->numOfSubplans = 1;
  pPlan->explainInfo.mode = EXPLAIN_MODE_STATIC;
  pSubplan->id.groupId = 1;
  pProject->node.pOutputDataBlockDesc = pDesc;
  pDesc->totalRowSize = sizeof(int32_t);
  pDesc->outputRowSize = sizeof(int32_t);
  pCall->templateId = 3;
  pCall->exprId = 1;
  pCall->scopeId = 2;
  pCall->parentScopeId = 1;
  pCall->node.resType.type = TSDB_DATA_TYPE_INT;
  pCall->node.resType.bytes = sizeof(int32_t);
  pBinding->paramId = 0;
  pBinding->dataType = pCall->node.resType;
  pBinding->scopeId = 1;
  pBinding->bindingId = 1;
  pBinding->tableRefId = 1;
  pBinding->sourceKind = CORRELATION_SOURCE_BLOCK;
  pBinding->sourceBlockId = 7;
  pBinding->sourceSlotId = 2;
  pBinding->inheritedParamId = -1;
  pSource->node.resType = pBinding->dataType;
  pSource->dataBlockId = pBinding->sourceBlockId;
  pSource->slotId = pBinding->sourceSlotId;
  pBinding->pSource = reinterpret_cast<SNode*>(pSource);
  ASSERT_EQ(nodesListMakeStrictAppend(&pCall->pParamBindings, reinterpret_cast<SNode*>(pBinding)),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesListMakeStrictAppend(&pProject->pProjections, reinterpret_cast<SNode*>(pCall)),
            TSDB_CODE_SUCCESS);
  pSubplan->pNode = reinterpret_cast<SPhysiNode*>(pProject);
  ASSERT_EQ(nodesListMakeStrictAppend(&pLevel->pNodeList, reinterpret_cast<SNode*>(pSubplan)), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesListMakeStrictAppend(&pPlan->pSubplans, reinterpret_cast<SNode*>(pLevel)), TSDB_CODE_SUCCESS);

  SQueryPlan* pTemplatePlan = nullptr;
  SSubplan* pTemplateSubplan = nullptr;
  SNodeListNode* pTemplateLevel = nullptr;
  SProjectPhysiNode* pTemplateProject = nullptr;
  SDataBlockDescNode* pTemplateDesc = nullptr;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN, reinterpret_cast<SNode**>(&pTemplatePlan)), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN, reinterpret_cast<SNode**>(&pTemplateSubplan)),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_NODE_LIST, reinterpret_cast<SNode**>(&pTemplateLevel)), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_PROJECT, reinterpret_cast<SNode**>(&pTemplateProject)),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_DATABLOCK_DESC, reinterpret_cast<SNode**>(&pTemplateDesc)),
            TSDB_CODE_SUCCESS);
  pTemplatePlan->numOfSubplans = 1;
  pTemplateSubplan->id.groupId = 1;
  pTemplateProject->node.pOutputDataBlockDesc = pTemplateDesc;
  pTemplateDesc->totalRowSize = sizeof(int32_t);
  pTemplateDesc->outputRowSize = sizeof(int32_t);
  ASSERT_EQ(nodesMakeList(&pTemplateProject->pProjections), TSDB_CODE_SUCCESS);
  pTemplateSubplan->pNode = reinterpret_cast<SPhysiNode*>(pTemplateProject);
  ASSERT_EQ(nodesListMakeStrictAppend(&pTemplateLevel->pNodeList,
                                      reinterpret_cast<SNode*>(pTemplateSubplan)),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesListMakeStrictAppend(&pTemplatePlan->pSubplans,
                                      reinterpret_cast<SNode*>(pTemplateLevel)),
            TSDB_CODE_SUCCESS);
  pTemplate->templateId = pCall->templateId;
  pTemplate->scopeId = pCall->scopeId;
  pTemplate->resultType = pCall->node.resType;
  ASSERT_EQ(nodesNodeToMsg(reinterpret_cast<SNode*>(pTemplatePlan), &pTemplate->pPlan,
                           &pTemplate->planLen),
            TSDB_CODE_SUCCESS);
  nodesDestroyNode(reinterpret_cast<SNode*>(pTemplatePlan));
  ASSERT_EQ(nodesListMakeStrictAppend(&pPlan->pCorrelationTemplates,
                                      reinterpret_cast<SNode*>(pTemplate)),
            TSDB_CODE_SUCCESS);

  SRetrieveTableRsp* pRsp = nullptr;
  ASSERT_EQ(qExecStaticExplain(pPlan, &pRsp), TSDB_CODE_SUCCESS);
  ASSERT_NE(pRsp, nullptr);
  SSDataBlock block = {};
  const char* pEnd = nullptr;
  ASSERT_EQ(blockDecode(&block, pRsp->data + PAYLOAD_PREFIX_LEN, &pEnd), TSDB_CODE_SUCCESS);
  auto* pColumn = static_cast<SColumnInfoData*>(taosArrayGet(block.pDataBlock, 0));
  std::string result;
  for (int32_t i = 0; i < block.info.rows; ++i) {
    char* pValue = colDataGetData(pColumn, i);
    result.append(varDataVal(pValue), varDataLen(pValue));
    result.push_back('\n');
  }
  EXPECT_NE(result.find("Projection"), std::string::npos);
  EXPECT_NE(result.find("Correlated Scalar Subquery (template: 3, stage: Projection, type: INT)"),
            std::string::npos);
  EXPECT_NE(result.find("Param 0: scope 1, block 7, slot 2"), std::string::npos);
  EXPECT_NE(result.find("Subquery Plan"), std::string::npos);
  EXPECT_EQ(result.find("Input Rows:"), std::string::npos);
  blockDataFreeRes(&block);
  taosMemoryFree(pRsp);

  pPlan->explainInfo.mode = EXPLAIN_MODE_ANALYZE;
  SExplainCtx* pCtx = nullptr;
  ASSERT_EQ(qExecExplainBegin(pPlan, &pCtx, taosGetTimestampUs()), TSDB_CODE_SUCCESS);
  SExplainRsp statsRsp = {};
  statsRsp.numOfPlans = 1;
  statsRsp.subplanInfo = static_cast<SExplainExecInfo*>(taosMemoryCalloc(1, sizeof(SExplainExecInfo)));
  ASSERT_NE(statsRsp.subplanInfo, nullptr);
  statsRsp.subplanInfo[0].correlationStatNum = 1;
  statsRsp.subplanInfo[0].pCorrelationStats =
      static_cast<SExplainCorrelationStat*>(taosMemoryCalloc(1, sizeof(SExplainCorrelationStat)));
  ASSERT_NE(statsRsp.subplanInfo[0].pCorrelationStats, nullptr);
  statsRsp.subplanInfo[0].pCorrelationStats[0].templateId = 3;
  statsRsp.subplanInfo[0].pCorrelationStats[0].inputRows = 5;
  statsRsp.subplanInfo[0].pCorrelationStats[0].calls = 4;
  statsRsp.subplanInfo[0].pCorrelationStats[0].elapsedUs = 12345;
  pRsp = nullptr;
  ASSERT_EQ(qExplainUpdateExecInfo(pCtx, qExplainGetCurrPlan(pCtx, -1), &statsRsp, 1, &pRsp),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(qExecExplainEnd(pCtx, &pRsp), TSDB_CODE_SUCCESS);
  ASSERT_NE(pRsp, nullptr);
  SSDataBlock analyzed = {};
  ASSERT_EQ(blockDecode(&analyzed, pRsp->data + PAYLOAD_PREFIX_LEN, &pEnd), TSDB_CODE_SUCCESS);
  pColumn = static_cast<SColumnInfoData*>(taosArrayGet(analyzed.pDataBlock, 0));
  result.clear();
  for (int32_t i = 0; i < analyzed.info.rows; ++i) {
    char* pValue = colDataGetData(pColumn, i);
    result.append(varDataVal(pValue), varDataLen(pValue));
    result.push_back('\n');
  }
  EXPECT_NE(result.find("Input Rows: 5, Calls: 4, Time: 12.345 ms"), std::string::npos);
  blockDataFreeRes(&analyzed);
  taosMemoryFree(pRsp);
  qExplainFreeCtx(pCtx);
  nodesDestroyNode(reinterpret_cast<SNode*>(pPlan));
}

TEST(ExplainVerboseInfoTest, SortRejectsShortBlobAndAcceptsCompleteBlob) {
  for (int32_t length : {0, 1, static_cast<int32_t>(sizeof(SSortExecInfo))}) {
    SQueryPlan* plan = nullptr;
    SSubplan* subplan = nullptr;
    SNodeListNode* level = nullptr;
    SSortPhysiNode* sort = nullptr;
    SDataBlockDescNode* desc = nullptr;
    ASSERT_EQ(nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN, reinterpret_cast<SNode**>(&plan)), TSDB_CODE_SUCCESS);
    ASSERT_EQ(nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN, reinterpret_cast<SNode**>(&subplan)), TSDB_CODE_SUCCESS);
    ASSERT_EQ(nodesMakeNode(QUERY_NODE_NODE_LIST, reinterpret_cast<SNode**>(&level)), TSDB_CODE_SUCCESS);
    ASSERT_EQ(nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_SORT, reinterpret_cast<SNode**>(&sort)), TSDB_CODE_SUCCESS);
    ASSERT_EQ(nodesMakeNode(QUERY_NODE_DATABLOCK_DESC, reinterpret_cast<SNode**>(&desc)), TSDB_CODE_SUCCESS);
    plan->numOfSubplans = 1;
    plan->explainInfo.mode = EXPLAIN_MODE_ANALYZE;
    subplan->id.groupId = 1;
    sort->node.pOutputDataBlockDesc = desc;
    desc->totalRowSize = sizeof(int32_t);
    desc->outputRowSize = sizeof(int32_t);
    subplan->pNode = reinterpret_cast<SPhysiNode*>(sort);
    ASSERT_EQ(nodesListMakeStrictAppend(&level->pNodeList, reinterpret_cast<SNode*>(subplan)), TSDB_CODE_SUCCESS);
    ASSERT_EQ(nodesListMakeStrictAppend(&plan->pSubplans, reinterpret_cast<SNode*>(level)), TSDB_CODE_SUCCESS);

    SExplainCtx* ctx = nullptr;
    ASSERT_EQ(qExecExplainBegin(plan, &ctx, taosGetTimestampUs()), TSDB_CODE_SUCCESS);
    SExplainRsp stats = {};
    stats.numOfPlans = 1;
    stats.subplanInfo = static_cast<SExplainExecInfo*>(taosMemoryCalloc(1, sizeof(SExplainExecInfo)));
    ASSERT_NE(stats.subplanInfo, nullptr);
    stats.subplanInfo[0].verboseLen = length;
    if (length > 0) {
      stats.subplanInfo[0].verboseInfo = taosMemoryCalloc(1, length);
      ASSERT_NE(stats.subplanInfo[0].verboseInfo, nullptr);
    }
    SRetrieveTableRsp* response = nullptr;
    ASSERT_EQ(qExplainUpdateExecInfo(ctx, qExplainGetCurrPlan(ctx, -1), &stats, 1, &response), TSDB_CODE_SUCCESS);
    int32_t code = qExecExplainEnd(ctx, &response);
    if (length < sizeof(SSortExecInfo)) {
      EXPECT_EQ(code, TSDB_CODE_INVALID_MSG);
      EXPECT_EQ(response, nullptr);
    } else {
      EXPECT_EQ(code, TSDB_CODE_SUCCESS);
      EXPECT_NE(response, nullptr);
    }
    taosMemoryFree(response);
    qExplainFreeCtx(ctx);
    nodesDestroyNode(reinterpret_cast<SNode*>(plan));
  }
}

TEST(ExplainVerboseInfoTest, TableScanRejectsShortBlobAndAcceptsCompleteBlob) {
  for (int32_t length : {0, 1, static_cast<int32_t>(sizeof(STableScanAnalyzeInfo))}) {
    SQueryPlan* plan = nullptr;
    SSubplan* subplan = nullptr;
    SNodeListNode* level = nullptr;
    STableScanPhysiNode* scan = nullptr;
    SDataBlockDescNode* desc = nullptr;
    ASSERT_EQ(nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN, reinterpret_cast<SNode**>(&plan)), TSDB_CODE_SUCCESS);
    ASSERT_EQ(nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN, reinterpret_cast<SNode**>(&subplan)), TSDB_CODE_SUCCESS);
    ASSERT_EQ(nodesMakeNode(QUERY_NODE_NODE_LIST, reinterpret_cast<SNode**>(&level)), TSDB_CODE_SUCCESS);
    ASSERT_EQ(nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN, reinterpret_cast<SNode**>(&scan)), TSDB_CODE_SUCCESS);
    ASSERT_EQ(nodesMakeNode(QUERY_NODE_DATABLOCK_DESC, reinterpret_cast<SNode**>(&desc)), TSDB_CODE_SUCCESS);
    plan->numOfSubplans = 1;
    plan->explainInfo.mode = EXPLAIN_MODE_ANALYZE;
    plan->explainInfo.verbose = true;
    subplan->id.groupId = 1;
    scan->scan.node.pOutputDataBlockDesc = desc;
    ASSERT_EQ(nodesMakeList(&scan->scan.pScanCols), TSDB_CODE_SUCCESS);
    desc->totalRowSize = sizeof(int32_t);
    desc->outputRowSize = sizeof(int32_t);
    subplan->pNode = reinterpret_cast<SPhysiNode*>(scan);
    ASSERT_EQ(nodesListMakeStrictAppend(&level->pNodeList, reinterpret_cast<SNode*>(subplan)), TSDB_CODE_SUCCESS);
    ASSERT_EQ(nodesListMakeStrictAppend(&plan->pSubplans, reinterpret_cast<SNode*>(level)), TSDB_CODE_SUCCESS);

    SExplainCtx* ctx = nullptr;
    ASSERT_EQ(qExecExplainBegin(plan, &ctx, taosGetTimestampUs()), TSDB_CODE_SUCCESS);
    SExplainRsp stats = {};
    stats.numOfPlans = 1;
    stats.subplanInfo = static_cast<SExplainExecInfo*>(taosMemoryCalloc(1, sizeof(SExplainExecInfo)));
    ASSERT_NE(stats.subplanInfo, nullptr);
    stats.subplanInfo[0].verboseLen = length;
    if (length > 0) {
      stats.subplanInfo[0].verboseInfo = taosMemoryCalloc(1, length);
      ASSERT_NE(stats.subplanInfo[0].verboseInfo, nullptr);
    }
    SRetrieveTableRsp* response = nullptr;
    ASSERT_EQ(qExplainUpdateExecInfo(ctx, qExplainGetCurrPlan(ctx, -1), &stats, 1, &response), TSDB_CODE_SUCCESS);
    int32_t code = qExecExplainEnd(ctx, &response);
    if (length < sizeof(STableScanAnalyzeInfo)) {
      EXPECT_EQ(code, TSDB_CODE_INVALID_PARA);
      EXPECT_EQ(response, nullptr);
    } else {
      EXPECT_EQ(code, TSDB_CODE_SUCCESS);
      EXPECT_NE(response, nullptr);
    }
    taosMemoryFree(response);
    qExplainFreeCtx(ctx);
    nodesDestroyNode(reinterpret_cast<SNode*>(plan));
  }
}

TEST(ExplainVerboseInfoTest, MergeAndGroupSortRejectShortBlobAndAcceptCompleteBlob) {
  for (ENodeType nodeType : {QUERY_NODE_PHYSICAL_PLAN_MERGE, QUERY_NODE_PHYSICAL_PLAN_GROUP_SORT}) {
    for (int32_t length : {0, 1, static_cast<int32_t>(sizeof(SSortExecInfo))}) {
      SQueryPlan* plan = nullptr;
      SSubplan* subplan = nullptr;
      SNodeListNode* level = nullptr;
      SPhysiNode* sort = nullptr;
      SDataBlockDescNode* desc = nullptr;
      ASSERT_EQ(nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN, reinterpret_cast<SNode**>(&plan)), TSDB_CODE_SUCCESS);
      ASSERT_EQ(nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN, reinterpret_cast<SNode**>(&subplan)), TSDB_CODE_SUCCESS);
      ASSERT_EQ(nodesMakeNode(QUERY_NODE_NODE_LIST, reinterpret_cast<SNode**>(&level)), TSDB_CODE_SUCCESS);
      ASSERT_EQ(nodesMakeNode(nodeType, reinterpret_cast<SNode**>(&sort)), TSDB_CODE_SUCCESS);
      ASSERT_EQ(nodesMakeNode(QUERY_NODE_DATABLOCK_DESC, reinterpret_cast<SNode**>(&desc)), TSDB_CODE_SUCCESS);
      plan->numOfSubplans = 1;
      plan->explainInfo.mode = EXPLAIN_MODE_ANALYZE;
      subplan->id.groupId = 1;
      sort->pOutputDataBlockDesc = desc;
      desc->totalRowSize = sizeof(int32_t);
      desc->outputRowSize = sizeof(int32_t);
      if (nodeType == QUERY_NODE_PHYSICAL_PLAN_MERGE) {
        reinterpret_cast<SMergePhysiNode*>(sort)->type = MERGE_TYPE_SORT;
      }
      subplan->pNode = sort;
      ASSERT_EQ(nodesListMakeStrictAppend(&level->pNodeList, reinterpret_cast<SNode*>(subplan)), TSDB_CODE_SUCCESS);
      ASSERT_EQ(nodesListMakeStrictAppend(&plan->pSubplans, reinterpret_cast<SNode*>(level)), TSDB_CODE_SUCCESS);

      SExplainCtx* ctx = nullptr;
      ASSERT_EQ(qExecExplainBegin(plan, &ctx, taosGetTimestampUs()), TSDB_CODE_SUCCESS);
      SExplainRsp stats = {};
      stats.numOfPlans = 1;
      stats.subplanInfo = static_cast<SExplainExecInfo*>(taosMemoryCalloc(1, sizeof(SExplainExecInfo)));
      ASSERT_NE(stats.subplanInfo, nullptr);
      stats.subplanInfo[0].verboseLen = length;
      if (length > 0) {
        stats.subplanInfo[0].verboseInfo = taosMemoryCalloc(1, length);
        ASSERT_NE(stats.subplanInfo[0].verboseInfo, nullptr);
      }
      SRetrieveTableRsp* response = nullptr;
      ASSERT_EQ(qExplainUpdateExecInfo(ctx, qExplainGetCurrPlan(ctx, -1), &stats, 1, &response), TSDB_CODE_SUCCESS);
      int32_t code = qExecExplainEnd(ctx, &response);
      SCOPED_TRACE(static_cast<int>(nodeType));
      SCOPED_TRACE(length);
      if (length < sizeof(SSortExecInfo)) {
        EXPECT_EQ(code, TSDB_CODE_INVALID_MSG);
        EXPECT_EQ(response, nullptr);
      } else {
        EXPECT_EQ(code, TSDB_CODE_SUCCESS);
        EXPECT_NE(response, nullptr);
      }
      taosMemoryFree(response);
      qExplainFreeCtx(ctx);
      nodesDestroyNode(reinterpret_cast<SNode*>(plan));
    }
  }
}

namespace {

using QueryPtr = std::unique_ptr<SQuery, decltype(&qDestroyQuery)>;
using FilterCase = std::tuple<int8_t, EExtSourceType>;

TEST(ShowCreateTableTest, ReturnsTruncatedWideTableDefinition) {
  constexpr int32_t kColumnCount = 2500;
  std::vector<SSchema> schemas(kColumnCount);
  for (int32_t i = 0; i < kColumnCount; ++i) {
    schemas[i].colId = i + 1;
    schemas[i].type = i == 0 ? TSDB_DATA_TYPE_TIMESTAMP : TSDB_DATA_TYPE_INT;
    schemas[i].bytes = tDataTypes[schemas[i].type].bytes;
    std::string name = "column_" + std::to_string(i) + std::string(20, 'x');
    tstrncpy(schemas[i].name, name.c_str(), sizeof(schemas[i].name));
  }

  STableCfg cfg = {};
  cfg.tableType = TSDB_NORMAL_TABLE;
  cfg.numOfColumns = kColumnCount;
  cfg.pSchemas = schemas.data();
  SDbCfgInfo dbCfg = {};
  SShowCreateTableStmt stmt = {};
  stmt.type = QUERY_NODE_SHOW_CREATE_TABLE_STMT;
  tstrncpy(stmt.tableName, "wide", sizeof(stmt.tableName));
  stmt.pTableCfg = &cfg;
  stmt.pDbCfg = &dbCfg;

  SRetrieveTableRsp* rsp = nullptr;
  ASSERT_EQ(qExecCommand(nullptr, true, 0, reinterpret_cast<SNode*>(&stmt), &rsp, 0, nullptr), TSDB_CODE_SUCCESS);
  ASSERT_NE(rsp, nullptr);
  SSDataBlock block = {};
  const char* end = nullptr;
  ASSERT_EQ(blockDecode(&block, rsp->data + PAYLOAD_PREFIX_LEN, &end), TSDB_CODE_SUCCESS);
  auto* column = static_cast<SColumnInfoData*>(taosArrayGet(block.pDataBlock, 1));
  char* value = colDataGetData(column, 0);
  std::string sql(varDataVal(value), varDataLen(value));
  EXPECT_EQ(sql.size(), static_cast<size_t>(UINT16_MAX));
  EXPECT_EQ(sql.find("CREATE TABLE `wide` (`column_0"), 0U);
  EXPECT_NE(sql.find("`column_1000"), std::string::npos);
  EXPECT_EQ(sql.find("`column_2200"), std::string::npos);
  blockDataFreeRes(&block);
  taosMemoryFree(rsp);
}

class ShowCreateFilterTest : public testing::TestWithParam<FilterCase> {
 protected:
  static void SetUpTestSuite() { ASSERT_EQ(qInitKeywordsTable(), TSDB_CODE_SUCCESS); }
  static void TearDownTestSuite() { taosCleanupKeywordsTable(); }

  void SetUp() override {
    cfg_.tableType = std::get<0>(GetParam());
    cfg_.numOfColumns = 3;
    cfg_.numOfTags = 1;
    cfg_.pSchemas = schemas_;
    cfg_.pColRefs = refs_;
    tstrncpy(cfg_.stbName, "stb", sizeof(cfg_.stbName));
    const char* names[] = {"ts", "v1", "v2", "group_id"};
    for (int32_t i = 0; i < 4; ++i) {
      schemas_[i].colId = i + 1;
      schemas_[i].type = i == 0 ? TSDB_DATA_TYPE_TIMESTAMP : TSDB_DATA_TYPE_INT;
      schemas_[i].bytes = tDataTypes[schemas_[i].type].bytes;
      tstrncpy(schemas_[i].name, names[i], sizeof(schemas_[i].name));
    }
    for (int32_t i = 1; i < 3; ++i) {
      refs_[i].id = schemas_[i].colId;
      refs_[i].hasRef = true;
      refs_[i].refType = 1;
      tstrncpy(refs_[i].refSourceName, sourceName(), sizeof(refs_[i].refSourceName));
      tstrncpy(refs_[i].refDbName, "db", sizeof(refs_[i].refDbName));
      tstrncpy(refs_[i].refTableName, "t", sizeof(refs_[i].refTableName));
      tstrncpy(refs_[i].refColName, "value", sizeof(refs_[i].refColName));
    }
    SArray* pTags = taosArrayInit(1, sizeof(STagVal));
    ASSERT_NE(pTags, nullptr);
    STagVal tag = {};
    tag.cid = schemas_[3].colId;
    tag.type = TSDB_DATA_TYPE_INT;
    tag.i64 = 7;
    ASSERT_NE(taosArrayPush(pTags, &tag), nullptr);
    int32_t code = tTagNew(pTags, 1, false, reinterpret_cast<STag**>(&cfg_.pTags));
    taosArrayDestroy(pTags);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);

    meta_.numOfCols = 7;
    meta_.pCols = columns_;
    const char* colNames[] = {"ts", "device", "value", "enabled", "price", "ratio", "dev`ice"};
    const char* types[] = {"TIMESTAMP", "VARCHAR(60000)", "INT", "BOOL", "DECIMAL(38,8)", "DOUBLE", "VARCHAR(32)"};
    for (int32_t i = 0; i < meta_.numOfCols; ++i) {
      tstrncpy(columns_[i].colName, colNames[i], sizeof(columns_[i].colName));
      tstrncpy(columns_[i].extTypeName, types[i], sizeof(columns_[i].extTypeName));
      tstrncpy(columns_[i].extCharsetName, "ascii", sizeof(columns_[i].extCharsetName));
      columns_[i].isRemotePrimaryKey = i < 2;
    }
    if (sourceType() == EXT_SOURCE_MYSQL) {
      tstrncpy(columns_[0].extTypeName, "DATETIME", sizeof(columns_[0].extTypeName));
    } else {
      tstrncpy(columns_[5].extTypeName, "DOUBLE PRECISION", sizeof(columns_[5].extTypeName));
    }
  }

  void TearDown() override {
    for (auto& ref : refs_) {
      taosMemoryFree(ref.filterCondJson);
      taosMemoryFree(ref.tagCondJson);
    }
    taosMemoryFree(series_.tagCondJson);
    taosMemoryFree(cfg_.pTags);
    tsMaxSQLLength = maxSqlLength_;
  }

  EExtSourceType sourceType() const { return std::get<1>(GetParam()); }
  const char*    sourceName() const { return sourceType() == EXT_SOURCE_MYSQL ? "mysql_src" : "pg_src"; }

  QueryPtr parseSQL(const std::string& sql) {
    SParseContext context = {};
    context.db = "test";
    context.pSql = sql.c_str();
    context.sqlLen = sql.size();
    context.pMsg = message_;
    context.msgLen = sizeof(message_);
    SQuery* pQuery = nullptr;
    int32_t code = parse(&context, &pQuery);
    QueryPtr query(pQuery, qDestroyQuery);
    EXPECT_EQ(code, TSDB_CODE_SUCCESS) << message_ << ": " << sql.substr(0, 256);
    if (TSDB_CODE_SUCCESS != code) {
      return QueryPtr(nullptr, qDestroyQuery);
    }
    return query;
  }

  SNode* filterOf(SQuery* pQuery, int32_t colIndex) {
    if (nodeType(pQuery->pRoot) == QUERY_NODE_CREATE_VIRTUAL_TABLE_STMT) {
      auto* pStmt = reinterpret_cast<SCreateVTableStmt*>(pQuery->pRoot);
      auto* pCol = reinterpret_cast<SColumnDefNode*>(nodesListGetNode(pStmt->pCols, colIndex));
      return reinterpret_cast<SColumnOptions*>(pCol->pOptions)->pFilterCond;
    }
    auto* pStmt = reinterpret_cast<SCreateVSubTableStmt*>(pQuery->pRoot);
    auto* pRef = reinterpret_cast<SColumnRefNode*>(nodesListGetNode(pStmt->pSpecificColRefs, colIndex - 1));
    return pRef->pFilterCond;
  }

  int32_t validate(SNode* pFilter) {
    int32_t tsIndex = -1;
    return nodesValidateExternalFilter(sourceType(), &meta_, pFilter, nullptr, nullptr, message_, sizeof(message_),
                                       &tsIndex);
  }

  void setFilter(int32_t colIndex, const std::string& condition) {
    auto query = parseSQL(std::string("CREATE VTABLE vt (ts TIMESTAMP, v INT FROM ") + sourceName() +
                          ".db.t.value WHERE " + condition + ")");
    ASSERT_NE(query, nullptr);
    SNode* pFilter = filterOf(query.get(), 1);
    ASSERT_NE(pFilter, nullptr);
    ASSERT_EQ(validate(pFilter), TSDB_CODE_SUCCESS) << message_;
    taosMemoryFreeClear(refs_[colIndex].filterCondJson);
    int32_t len = 0;
    ASSERT_EQ(nodesNodeToString(pFilter, false, &refs_[colIndex].filterCondJson, &len), TSDB_CODE_SUCCESS);
    refs_[colIndex].filterCondLen = strlen(refs_[colIndex].filterCondJson);
  }

  int32_t showCreate(std::string* pSQL) {
    SShowCreateTableStmt stmt = {};
    stmt.type = QUERY_NODE_SHOW_CREATE_VTABLE_STMT;
    tstrncpy(stmt.tableName, "vt", sizeof(stmt.tableName));
    stmt.pTableCfg = &cfg_;
    stmt.pDbCfg = &dbCfg_;
    SRetrieveTableRsp* pRsp = nullptr;
    int32_t            code = qExecCommand(nullptr, true, 0, reinterpret_cast<SNode*>(&stmt), &pRsp, 0, nullptr);
    if (code == TSDB_CODE_SUCCESS) {
      SSDataBlock block = {};
      const char* pEnd = nullptr;
      code = blockDecode(&block, pRsp->data + PAYLOAD_PREFIX_LEN, &pEnd);
      if (code == TSDB_CODE_SUCCESS) {
        auto* pCol = static_cast<SColumnInfoData*>(taosArrayGet(block.pDataBlock, 1));
        char* pValue = colDataGetData(pCol, 0);
        pSQL->assign(varDataVal(pValue), varDataLen(pValue));
      }
      blockDataFreeRes(&block);
    }
    taosMemoryFree(pRsp);
    return code;
  }

  void expectReplay(const std::string& sql, int32_t colIndex) {
    auto query = parseSQL(sql);
    ASSERT_NE(query, nullptr);
    SNode* pFilter = filterOf(query.get(), colIndex);
    ASSERT_NE(pFilter, nullptr);
    ASSERT_EQ(validate(pFilter), TSDB_CODE_SUCCESS) << message_;
    SNode* pSaved = nullptr;
    int32_t code = nodesStringToNode(refs_[colIndex].filterCondJson, &pSaved);
    std::unique_ptr<SNode, decltype(&nodesDestroyNode)> saved(pSaved, nodesDestroyNode);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    // Restore both sides to literal nodes so typed values (including DECIMAL)
    // compare consistently while retaining operators, columns, and tree shape.
    ASSERT_EQ(nodesRestoreExternalFilterLiterals(&pSaved, true), TSDB_CODE_SUCCESS);
    ASSERT_EQ(nodesRestoreExternalFilterLiterals(&pFilter, true), TSDB_CODE_SUCCESS);
    EXPECT_TRUE(nodesEqualNode(pSaved, pFilter));
  }

  STableCfg     cfg_ = {};
  SDbCfgInfo    dbCfg_ = {};
  SSchema       schemas_[4] = {};
  SColRef       refs_[3] = {};
  SSeriesEntry  series_ = {};
  SExtTableMeta meta_ = {};
  SExtColumnDef columns_[7] = {};
  char          message_[512] = {};
  int32_t       maxSqlLength_ = tsMaxSQLLength;
};

TEST_P(ShowCreateFilterTest, EmitsEachColumnsOwnCondition) {
  ASSERT_NO_FATAL_FAILURE(setFilter(1, "device = 'd1'"));
  ASSERT_NO_FATAL_FAILURE(setFilter(2, "device = 'd2'"));
  std::string saved = refs_[1].filterCondJson;
  std::string sql;
  ASSERT_EQ(showCreate(&sql), TSDB_CODE_SUCCESS);
  EXPECT_NE(sql.find(".`value` WHERE (`device` = 'd1')"), std::string::npos);
  EXPECT_NE(sql.find(".`value` WHERE (`device` = 'd2')"), std::string::npos);
  EXPECT_EQ(saved, refs_[1].filterCondJson);
  expectReplay(sql, 1);
  expectReplay(sql, 2);
}

TEST_P(ShowCreateFilterTest, EscapesIdentifiersQuotesAndBackslashes) {
  ASSERT_NO_FATAL_FAILURE(setFilter(1, R"(device = 'O''Reilly\\path' AND `dev``ice` != 'a"b')"));
  std::string sql;
  ASSERT_EQ(showCreate(&sql), TSDB_CODE_SUCCESS);
  EXPECT_NE(sql.find("`dev``ice`"), std::string::npos);
  expectReplay(sql, 1);
}

TEST_P(ShowCreateFilterTest, PreservesTimestampAndExactNumericLiterals) {
  ASSERT_NO_FATAL_FAILURE(
      setFilter(1,
                "device = 'd1' AND ts >= '2026-09-01 12:34:56.123' AND value < -42 "
                "AND enabled = true AND price <= '12345678901234567890.12345678' AND `ratio` > 1.23456789012345"));
  std::string sql;
  ASSERT_EQ(showCreate(&sql), TSDB_CODE_SUCCESS);
  EXPECT_NE(sql.find("'2026-09-01 12:34:56.123'"), std::string::npos);
  EXPECT_NE(sql.find("'12345678901234567890.12345678'"), std::string::npos);
  expectReplay(sql, 1);
}

TEST_P(ShowCreateFilterTest, ReplaysEmptyString) {
  ASSERT_NO_FATAL_FAILURE(setFilter(1, "device = ''"));
  std::string sql;
  ASSERT_EQ(showCreate(&sql), TSDB_CODE_SUCCESS);
  EXPECT_NE(sql.find("`device` = ''"), std::string::npos);
  expectReplay(sql, 1);
}

TEST_P(ShowCreateFilterTest, ReplaysUnicodeLiteral) {
  tstrncpy(columns_[1].extCharsetName, sourceType() == EXT_SOURCE_MYSQL ? "utf8mb4" : "UTF8",
           sizeof(columns_[1].extCharsetName));
  ASSERT_NO_FATAL_FAILURE(setFilter(1, "device = '设备一'"));
  std::string sql;
  ASSERT_EQ(showCreate(&sql), TSDB_CODE_SUCCESS);
  EXPECT_NE(sql.find("'设备一'"), std::string::npos);
  expectReplay(sql, 1);
}

TEST_P(ShowCreateFilterTest, GrowsBufferAndKeepsTrailingTags) {
  std::string value(12000, 'x');
  ASSERT_NO_FATAL_FAILURE(setFilter(1, "device = '" + value + "'"));
  ASSERT_NO_FATAL_FAILURE(setFilter(2, "device = 'd2'"));
  tsMaxSQLLength = 128;
  std::string sql;
  ASSERT_EQ(showCreate(&sql), TSDB_CODE_SUCCESS);
  EXPECT_GT(sql.size(), static_cast<size_t>(SHOW_CREATE_TB_RESULT_FIELD2_LEN));
  EXPECT_NE(sql.find(value), std::string::npos);
  EXPECT_NE(sql.find("`group_id`"), std::string::npos);
  tsMaxSQLLength = maxSqlLength_;
  expectReplay(sql, 1);
  expectReplay(sql, 2);
}

TEST_P(ShowCreateFilterTest, RejectsExpandedReferenceThatExceedsTypeBuffer) {
  memset(refs_[1].refTableName, '`', sizeof(refs_[1].refTableName) - 1);
  refs_[1].refTableName[sizeof(refs_[1].refTableName) - 1] = '\0';
  memset(refs_[1].refColName, '`', sizeof(refs_[1].refColName) - 1);
  refs_[1].refColName[sizeof(refs_[1].refColName) - 1] = '\0';

  std::string sql;
  EXPECT_EQ(showCreate(&sql), TSDB_CODE_OUT_OF_BUFFER);
  EXPECT_TRUE(sql.empty());
}

TEST_P(ShowCreateFilterTest, RejectsCommentThatExceedsResultBuffer) {
  cfg_.tableType = TSDB_NORMAL_TABLE;
  cfg_.numOfTags = 0;
  std::string comment(static_cast<size_t>(tsMaxSQLLength) * 3, 'x');
  cfg_.commentLen = comment.size();
  cfg_.pComment = const_cast<char*>(comment.data());

  std::string sql;
  EXPECT_EQ(showCreate(&sql), TSDB_CODE_OUT_OF_BUFFER);
  EXPECT_TRUE(sql.empty());
}

TEST_P(ShowCreateFilterTest, SetNullDoesNotEmitReferenceOrFilter) {
  ASSERT_NO_FATAL_FAILURE(setFilter(1, "device = 'd1'"));
  refs_[1].hasRef = false;
  taosMemoryFreeClear(refs_[1].filterCondJson);
  refs_[1].filterCondLen = 0;
  std::string sql;
  ASSERT_EQ(showCreate(&sql), TSDB_CODE_SUCCESS);
  EXPECT_EQ(sql.find(" WHERE "), std::string::npos);
  EXPECT_EQ(sql.find("`v1` FROM"), std::string::npos);
  if (cfg_.tableType == TSDB_VIRTUAL_NORMAL_TABLE) {
    EXPECT_NE(sql.find("`v1` INT,"), std::string::npos);
  }
  ASSERT_NE(parseSQL(sql), nullptr);
}

TEST_P(ShowCreateFilterTest, DoesNotEmitFiltersOnIndirectReferences) {
  ASSERT_NO_FATAL_FAILURE(setFilter(1, "device = 'd1'"));
  refs_[1].refType = 0;
  refs_[1].refSourceName[0] = '\0';
  std::string sql;
  ASSERT_EQ(showCreate(&sql), TSDB_CODE_SUCCESS);
  EXPECT_EQ(sql.find(" WHERE "), std::string::npos);
}

TEST_P(ShowCreateFilterTest, RejectsInvalidSavedJson) {
  refs_[1].filterCondJson = taosStrdup("invalid-json");
  refs_[1].filterCondLen = strlen(refs_[1].filterCondJson);
  std::string sql;
  EXPECT_EQ(showCreate(&sql), TSDB_CODE_VTABLE_INVALID_EXT_FILTER);
  EXPECT_TRUE(sql.empty());
}

TEST_P(ShowCreateFilterTest, RejectsOversizeResultInsteadOfTruncating) {
  std::string value(33000, 'x');
  ASSERT_NO_FATAL_FAILURE(setFilter(1, "device = '" + value + "'"));
  ASSERT_NO_FATAL_FAILURE(setFilter(2, "device = '" + value + "'"));
  std::string sql;
  EXPECT_EQ(showCreate(&sql), TSDB_CODE_OUT_OF_BUFFER);
  EXPECT_TRUE(sql.empty());
}

TEST_P(ShowCreateFilterTest, RejectsInconsistentFilterLength) {
  ASSERT_NO_FATAL_FAILURE(setFilter(1, "device = 'd1'"));
  refs_[1].filterCondLen = 0;
  std::string sql;
  EXPECT_EQ(showCreate(&sql), TSDB_CODE_VTABLE_INVALID_EXT_FILTER);
  EXPECT_TRUE(sql.empty());
}

TEST_P(ShowCreateFilterTest, PreservesInfluxSeriesCondition) {
  ASSERT_NO_FATAL_FAILURE(setFilter(1, "device = 'd1'"));
  auto seriesQuery = parseSQL("CREATE VTABLE vseries (ts TIMESTAMP, v INT FROM series1.value) "
                              "SERIES series1 AS influx_src.db.measurement (device = 'influx-tag')");
  ASSERT_NE(seriesQuery, nullptr);
  auto* stmt = reinterpret_cast<SCreateVTableStmt*>(seriesQuery->pRoot);
  ASSERT_EQ(LIST_LENGTH(stmt->pSeriesList), 1);
  auto* decl = reinterpret_cast<SSeriesDeclNode*>(nodesListGetNode(stmt->pSeriesList, 0));
  tstrncpy(series_.alias, decl->alias, sizeof(series_.alias));
  tstrncpy(series_.sourceName, decl->sourceName, sizeof(series_.sourceName));
  tstrncpy(series_.dbName, decl->dbName, sizeof(series_.dbName));
  tstrncpy(series_.measurementName, decl->measurementName, sizeof(series_.measurementName));
  ASSERT_EQ(nodesNodeToString(decl->pTagCond, false, &series_.tagCondJson, &series_.tagCondLen), TSDB_CODE_SUCCESS);
  --series_.tagCondLen;
  tstrncpy(refs_[2].refSourceName, series_.sourceName, sizeof(refs_[2].refSourceName));
  tstrncpy(refs_[2].refTableName, series_.measurementName, sizeof(refs_[2].refTableName));
  cfg_.numOfSeries = 1;
  cfg_.pSeries = &series_;
  std::string sql;
  ASSERT_EQ(showCreate(&sql), TSDB_CODE_SUCCESS);
  EXPECT_NE(sql.find("SERIES `series1` AS influx_src.db.measurement (device='influx-tag')"), std::string::npos);
  EXPECT_NE(sql.find("WHERE (`device` = 'd1')"), std::string::npos);
}

TEST_P(ShowCreateFilterTest, ParsesAlterFilterAndSetNull) {
  std::string reference = std::string(sourceName()) + ".db.t.value WHERE (device = 'd1' AND value >= 1)";
  auto        add = parseSQL("ALTER VTABLE vt ADD COLUMN v3 INT FROM " + reference);
  ASSERT_NE(add, nullptr);
  auto* pAdd = reinterpret_cast<SAlterTableStmt*>(add->pRoot);
  ASSERT_NE(pAdd->pColOptions, nullptr);
  ASSERT_NE(pAdd->pColOptions->pFilterCond, nullptr);
  EXPECT_EQ(validate(pAdd->pColOptions->pFilterCond), TSDB_CODE_SUCCESS) << message_;

  auto set = parseSQL("ALTER VTABLE vt ALTER COLUMN v1 SET " + reference);
  ASSERT_NE(set, nullptr);
  auto* pSet = reinterpret_cast<SAlterTableStmt*>(set->pRoot);
  ASSERT_NE(pSet->pFilterCond, nullptr);
  EXPECT_EQ(validate(pSet->pFilterCond), TSDB_CODE_SUCCESS) << message_;

  auto clear = parseSQL("ALTER VTABLE vt ALTER COLUMN v1 SET NULL");
  ASSERT_NE(clear, nullptr);
  EXPECT_EQ(reinterpret_cast<SAlterTableStmt*>(clear->pRoot)->pFilterCond, nullptr);
}

TEST(ShowCreateDatabaseTest, RetentionsUseBoundedOutput) {
  SDbCfgInfo cfg = {};
  cfg.pRetensions = taosArrayInit(TSDB_RETENTION_MAX + 1, sizeof(SRetention));
  ASSERT_NE(cfg.pRetensions, nullptr);
  SRetention retention = {};
  retention.freq = INT64_MAX;
  retention.keep = INT64_MAX;
  retention.freqUnit = 'm';
  retention.keepUnit = 'm';
  for (int32_t i = 0; i < TSDB_RETENTION_MAX; ++i) {
    ASSERT_NE(taosArrayPush(cfg.pRetensions, &retention), nullptr);
  }

  SShowCreateDatabaseStmt stmt = {};
  stmt.type = QUERY_NODE_SHOW_CREATE_DATABASE_STMT;
  tstrncpy(stmt.dbName, "db", sizeof(stmt.dbName));
  tstrncpy(stmt.dbFName, "1.db", sizeof(stmt.dbFName));
  stmt.pCfg = &cfg;
  SRetrieveTableRsp* rsp = nullptr;
  ASSERT_EQ(qExecCommand(nullptr, true, 0, reinterpret_cast<SNode*>(&stmt), &rsp, 0, nullptr), TSDB_CODE_SUCCESS);
  ASSERT_NE(rsp, nullptr);
  SSDataBlock block = {};
  const char* end = nullptr;
  ASSERT_EQ(blockDecode(&block, rsp->data + PAYLOAD_PREFIX_LEN, &end), TSDB_CODE_SUCCESS);
  auto* col = static_cast<SColumnInfoData*>(taosArrayGet(block.pDataBlock, 1));
  char* value = colDataGetData(col, 0);
  std::string sql(varDataVal(value), varDataLen(value));
  EXPECT_NE(sql.find(" RETENTIONS -:"), std::string::npos);
  EXPECT_EQ(std::count(sql.begin(), sql.end(), ':'), TSDB_RETENTION_MAX);
  blockDataFreeRes(&block);
  taosMemoryFree(rsp);

  ASSERT_NE(taosArrayPush(cfg.pRetensions, &retention), nullptr);
  rsp = nullptr;
  EXPECT_EQ(qExecCommand(nullptr, true, 0, reinterpret_cast<SNode*>(&stmt), &rsp, 0, nullptr),
            TSDB_CODE_TSC_INVALID_VALUE);
  EXPECT_EQ(rsp, nullptr);
  taosArrayDestroy(cfg.pRetensions);
}

INSTANTIATE_TEST_SUITE_P(VirtualTables, ShowCreateFilterTest,
                         testing::Combine(testing::Values(TSDB_VIRTUAL_NORMAL_TABLE, TSDB_VIRTUAL_CHILD_TABLE),
                                          testing::Values(EXT_SOURCE_MYSQL, EXT_SOURCE_POSTGRESQL)));

}  // namespace

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
