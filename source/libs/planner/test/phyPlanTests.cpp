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
  void pushScan(const string& db, const string& table, int32_t scanOp) {
    shared_ptr<MockTableMeta> meta = mockCatalogService->getTableMeta(db, table);
    EXPECT_TRUE(meta);
// typedef struct SQueryPlanNode {
//   SArray             *pExpr;        // the query functions or sql aggregations
//   int32_t             numOfExpr;  // number of result columns, which is also the number of pExprs
// } SQueryPlanNode;
    unique_ptr<SQueryPlanNode> scan((SQueryPlanNode*)calloc(1, sizeof(SQueryPlanNode)));
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
    int32_t code = createDag(logicPlan_.get(), nullptr, &dag);
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
        ASSERT_EQ (TSDB_CODE_SUCCESS, qSubPlanToString((const SSubplan*)taosArrayGetP(subplans, j), &str, &len));
        std::cout << str << std::endl;
        free(str);
      }
    }
  }

  SQueryDag* reslut() {
    return dag_.get();
  }

private:
  void pushNode(SQueryPlanNode* node) {
    if (logicPlan_) {
      // todo
    } else {
      logicPlan_.reset(node);
    }
  }

  void copySchemaMeta(STableMeta** dst, const STableMeta* src) {
    int32_t size = sizeof(STableMeta) + sizeof(SSchema) * (src->tableInfo.numOfTags + src->tableInfo.numOfColumns);
    *dst = (STableMeta*)myCalloc(1, size);
    memcpy(*dst, src, size);
  }

  void copyStorageMeta(SVgroupsInfo** dst, const std::vector<SVgroupInfo>& src) {
    *dst = (SVgroupsInfo*)myCalloc(1, sizeof(SVgroupsInfo) + sizeof(SVgroupMsg) * src.size());
    (*dst)->numOfVgroups = src.size();
    for (int32_t i = 0; i < src.size(); ++i) {
      (*dst)->vgroups[i].vgId = src[i].vgId;
      (*dst)->vgroups[i].numOfEps = src[i].numOfEps;
      memcpy((*dst)->vgroups[i].epAddr, src[i].epAddr, src[i].numOfEps);
    }
  }

  SQueryTableInfo* createScanExtInfo(shared_ptr<MockTableMeta>& meta) {
    SQueryTableInfo* info = (SQueryTableInfo*)myCalloc(1, sizeof(SQueryTableInfo));
    info->pMeta = (STableMetaInfo*)myCalloc(1, sizeof(STableMetaInfo));
    copySchemaMeta(&info->pMeta->pTableMeta, meta->schema.get());
    copyStorageMeta(&info->pMeta->vgroupList, meta->vgs);
    return info;
  }

  shared_ptr<MockTableMeta> meta_;
  unique_ptr<SQueryPlanNode> logicPlan_;
  unique_ptr<SQueryDag> dag_;
};

// select * from table
TEST_F(PhyPlanTest, tableScanTest) {
  pushScan("root.test", "t1", QNODE_TABLESCAN);
  ASSERT_EQ(run(), TSDB_CODE_SUCCESS);
  explain();
  SQueryDag* dag = reslut();
  // todo check
}

// select * from supertable
TEST_F(PhyPlanTest, superTableScanTest) {
  pushScan("root.test", "st1", QNODE_TABLESCAN);
  ASSERT_EQ(run(), TSDB_CODE_SUCCESS);
  explain();
  SQueryDag* dag = reslut();
  // todo check
}
