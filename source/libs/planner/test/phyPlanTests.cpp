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

#include <gtest/gtest.h>

#include "plannerInt.h"
#include "mockCatalogService.h"

using namespace std;
using namespace testing;

void* myCalloc(size_t nmemb, size_t size) {
  if (void* p = calloc(nmemb, size)) {
    return p;
  }
  throw bad_alloc();
}

class PhyPlanTest : public Test {
protected:
  void pushAgg(int32_t aggOp) {
    unique_ptr<SQueryPlanNode> agg((SQueryPlanNode*)myCalloc(1, sizeof(SQueryPlanNode)));
    agg->info.type = aggOp;
    agg->pExpr = taosArrayInit(TARRAY_MIN_SIZE, POINTER_BYTES);
    unique_ptr<SExprInfo> expr((SExprInfo*)myCalloc(1, sizeof(SExprInfo)));
    expr->base.resSchema.type = TSDB_DATA_TYPE_INT;
    expr->base.resSchema.bytes = tDataTypes[TSDB_DATA_TYPE_INT].bytes;
    expr->pExpr = (tExprNode*)myCalloc(1, sizeof(tExprNode));
    expr->pExpr->nodeType = TEXPR_FUNCTION_NODE;
    strcpy(expr->pExpr->_function.functionName, "Count");
    SExprInfo* item = expr.release();
    taosArrayPush(agg->pExpr, &item);
    pushNode(agg.release());
  }

  void pushScan(const string& db, const string& table, int32_t scanOp) {
    shared_ptr<MockTableMeta> meta = mockCatalogService->getTableMeta(db, table);
    EXPECT_TRUE(meta);
    unique_ptr<SQueryPlanNode> scan((SQueryPlanNode*)myCalloc(1, sizeof(SQueryPlanNode)));
    scan->info.type = scanOp;
    scan->numOfCols = meta->schema->tableInfo.numOfColumns;
    scan->pSchema = (SSchema*)myCalloc(1, sizeof(SSchema) * scan->numOfCols);
    memcpy(scan->pSchema, meta->schema->schema, sizeof(SSchema) * scan->numOfCols);
    //todo 'pExpr' 'numOfExpr'
    scan->pExtInfo = createScanExtInfo(meta);
    pushNode(scan.release());
  }

  int32_t run() {
    SQueryDag* dag = nullptr;
    uint64_t requestId = 20;
    int32_t code = createDag(logicPlan_.get(), nullptr, &dag, NULL, requestId);
    dag_.reset(dag);
    return code;
  }

  int32_t run(const string& db, const string& sql) {
    SParseContext cxt;
    buildParseContext(db, sql, &cxt);
    SQueryNode* query;
    int32_t code = qParseQuerySql(&cxt, &query);
    if (TSDB_CODE_SUCCESS != code) {
      cout << "error no:" << code << ", msg:" << cxt.pMsg << endl;
      return code;
    }
    SQueryDag* dag = nullptr;
    uint64_t requestId = 20;
    SSchema *schema = NULL;
    int32_t numOfOutput = 0;

    code = qCreateQueryDag(query, &dag, &schema, &numOfOutput, nullptr, requestId);
    dag_.reset(dag);
    return code;
  }

  void explain() {
    size_t level = taosArrayGetSize(dag_->pSubplans);
    for (size_t i = 0; i < level; ++i) {
      std::cout << "level " << i  << ":" << std::endl;
      const SArray* subplans = (const SArray*)taosArrayGetP(dag_->pSubplans, i);
      size_t num = taosArrayGetSize(subplans);
      for (size_t j = 0; j < num; ++j) {
        std::cout << "no " << j << ":" << std::endl;
        int32_t len = 0;
        char* str = nullptr;
        ASSERT_EQ(TSDB_CODE_SUCCESS, qSubPlanToString((const SSubplan*)taosArrayGetP(subplans, j), &str, &len));
        std::cout << "len:" << len << std::endl;
        std::cout << str << std::endl;
        free(str);
      }
    }
  }

  SQueryDag* result() {
    return dag_.get();
  }

private:
  void pushNode(SQueryPlanNode* node) {
    if (logicPlan_) {
      node->pChildren = taosArrayInit(TARRAY_MIN_SIZE, POINTER_BYTES);
      SQueryPlanNode* child = logicPlan_.release();
      taosArrayPush(node->pChildren, &child);
    }
    logicPlan_.reset(node);
  }

  void copySchemaMeta(STableMeta** dst, const STableMeta* src) {
    int32_t size = sizeof(STableMeta) + sizeof(SSchema) * (src->tableInfo.numOfTags + src->tableInfo.numOfColumns);
    *dst = (STableMeta*)myCalloc(1, size);
    memcpy(*dst, src, size);
  }

  void copyStorageMeta(SVgroupsInfo** dst, const std::vector<SVgroupInfo>& src) {
    *dst = (SVgroupsInfo*)myCalloc(1, sizeof(SVgroupsInfo) + sizeof(SVgroupInfo) * src.size());
    (*dst)->numOfVgroups = src.size();
    for (int32_t i = 0; i < src.size(); ++i) {
      (*dst)->vgroups[i] = src[i];
    }
  }

  SQueryTableInfo* createScanExtInfo(shared_ptr<MockTableMeta>& meta) {
    SQueryTableInfo* info = (SQueryTableInfo*)myCalloc(1, sizeof(SQueryTableInfo));
    info->pMeta = (STableMetaInfo*)myCalloc(1, sizeof(STableMetaInfo));
    copySchemaMeta(&info->pMeta->pTableMeta, meta->schema.get());
    copyStorageMeta(&info->pMeta->vgroupList, meta->vgs);
    return info;
  }

  void buildParseContext(const string& db, const string& sql, SParseContext* pCxt) {
    static string _db;
    static string _sql;
    static const int32_t _msgMaxLen = 4096;
    static char _msg[_msgMaxLen];
  
    _db = db;
    _sql = sql;
    memset(_msg, 0, _msgMaxLen);
  
    pCxt->acctId = 1;
    pCxt->db = _db.c_str();
    pCxt->requestId = 1;
    pCxt->pSql = _sql.c_str();
    pCxt->sqlLen = _sql.length();
    pCxt->pMsg = _msg;
    pCxt->msgLen = _msgMaxLen;
  }

  shared_ptr<MockTableMeta> meta_;
  unique_ptr<SQueryPlanNode> logicPlan_;
  unique_ptr<SQueryDag> dag_;
};

// select * from table
TEST_F(PhyPlanTest, tableScanTest) {
  pushScan("test", "t1", QNODE_TABLESCAN);
  ASSERT_EQ(run(), TSDB_CODE_SUCCESS);
  explain();
  SQueryDag* dag = result();
  // todo check
}

TEST_F(PhyPlanTest, serializeTest) {
  pushScan("test", "t1", QNODE_TABLESCAN);
  ASSERT_EQ(run(), TSDB_CODE_SUCCESS);
  SQueryDag* dag = result();
  cout << qDagToString(dag) << endl;
}

// select * from supertable
TEST_F(PhyPlanTest, superTableScanTest) {
  pushScan("test", "st1", QNODE_TABLESCAN);
  ASSERT_EQ(run(), TSDB_CODE_SUCCESS);
  explain();
  SQueryDag* dag = result();
  // todo check
}

// select count(*) from table
TEST_F(PhyPlanTest, simpleAggTest) {
  pushScan("test", "t1", QNODE_TABLESCAN);
  pushAgg(QNODE_AGGREGATE);
  ASSERT_EQ(run(), TSDB_CODE_SUCCESS);
  explain();
  SQueryDag* dag = result();
  // todo check
}

// insert into t values(...)
TEST_F(PhyPlanTest, insertTest) {
  ASSERT_EQ(run("test", "insert into t1 values (now, 1, \"beijing\")"), TSDB_CODE_SUCCESS);
  explain();
  SQueryDag* dag = result();
  // todo check
}
