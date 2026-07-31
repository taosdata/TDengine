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

#include <string>
#include <vector>

#include "planInt.h"

namespace {

class ExtMetaFixture {
 public:
  ExtMetaFixture(int32_t numCols, int32_t numTags) : columns_(numCols) {
    EXPECT_GE(numCols, numTags);
    memset(&meta_, 0, sizeof(meta_));
    meta_.pCols = columns_.data();
    meta_.numOfCols = numCols;

    memset(&node_, 0, sizeof(node_));
    node_.pExtMeta = &meta_;
    tstrncpy(node_.sourceName, "fake_source", sizeof(node_.sourceName));
    tstrncpy(node_.table.tableName, "fake_table", sizeof(node_.table.tableName));

    for (int32_t i = 0; i < numCols; ++i) {
      std::string name = "c" + std::to_string(i);
      tstrncpy(columns_[i].colName, name.c_str(), sizeof(columns_[i].colName));
      columns_[i].isTag = i >= numCols - numTags;
    }
  }

  ~ExtMetaFixture() { nodesDestroyList(scanCols_); }

  void addReference(int32_t columnIndex) {
    ASSERT_GE(columnIndex, 0);
    ASSERT_LT(columnIndex, meta_.numOfCols);

    SColumnNode* column = NULL;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&column));
    tstrncpy(column->colName, columns_[columnIndex].colName, sizeof(column->colName));
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&scanCols_, (SNode*)column));
  }

  int32_t check() const { return planCheckRequiredExtColumns(&node_, scanCols_); }

 private:
  std::vector<SExtColumnDef> columns_;
  SExtTableMeta              meta_;
  SExtTableNode              node_;
  SNodeList*                 scanCols_ = NULL;
};

void addReferences(ExtMetaFixture* fixture, int32_t begin, int32_t end) {
  for (int32_t i = begin; i < end; ++i) {
    fixture->addReference(i);
  }
}

}  // namespace

TEST(PlanFederatedRequiredColumnsTest, AllowsFiniteProjectionFromWideMetadata) {
  for (int32_t width : {4097, 32768}) {
    ExtMetaFixture fixture(width, 0);
    fixture.addReference(0);
    fixture.addReference(0);
    EXPECT_EQ(TSDB_CODE_SUCCESS, fixture.check()) << "width=" << width;
  }
}

TEST(PlanFederatedRequiredColumnsTest, CountsUniqueReferencesAtTheColumnLimit) {
  ExtMetaFixture fixture(4097, 0);
  addReferences(&fixture, 0, TSDB_MAX_COLUMNS_NON_VIRTUAL);
  fixture.addReference(0);
  EXPECT_EQ(TSDB_CODE_SUCCESS, fixture.check());
}

TEST(PlanFederatedRequiredColumnsTest, RejectsOnlyActualColumnReferenceOverflow) {
  ExtMetaFixture fixture(4097, 0);
  addReferences(&fixture, 0, TSDB_MAX_COLUMNS_NON_VIRTUAL + 1);
  EXPECT_EQ(TSDB_CODE_EXT_REQUIRED_COLUMNS_TOO_MANY, fixture.check());
}

TEST(PlanFederatedRequiredColumnsTest, CountsTagReferencesSeparately) {
  ExtMetaFixture allowed(129, 129);
  addReferences(&allowed, 0, TSDB_MAX_TAGS);
  EXPECT_EQ(TSDB_CODE_SUCCESS, allowed.check());

  ExtMetaFixture rejected(129, 129);
  addReferences(&rejected, 0, TSDB_MAX_TAGS + 1);
  EXPECT_EQ(TSDB_CODE_EXT_REQUIRED_TAGS_TOO_MANY, rejected.check());
}
