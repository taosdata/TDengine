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

/**
 * Tests for vtable-ref-vtable resolution logic.
 *
 * Since resolveVtableOTableInfo is static in vnodeStream.c, we test the
 * resolution algorithm by re-implementing the core logic in a testable form.
 * This validates the chain-following algorithm, depth limits, cycle detection,
 * column-not-found, and forward-to-remote behavior.
 */

#include <gtest/gtest.h>
#include <cstring>
#include <map>
#include <string>
#include <vector>

extern "C" {
#include "streamMsg.h"
#include "taoserror.h"
#include "taosdef.h"
#include "tutil.h"
}

// Simplified mock table entry for testing resolution logic
struct MockColRef {
  std::string colName;
  std::string refDbName;
  std::string refTableName;
  std::string refColName;
  bool        hasRef;
};

struct MockSchema {
  std::string name;
  col_id_t    colId;
};

struct MockTableEntry {
  int8_t                  type;  // TD_NORMAL_TABLE, TD_CHILD_TABLE, TD_VIRTUAL_NORMAL_TABLE, etc.
  int64_t                 uid;
  int64_t                 suid;  // for child tables
  std::vector<MockColRef> colRefs;
  std::vector<MockSchema> schemas;  // for physical tables
};

// Mock table registry (simulates what VNode metadata would contain)
static std::map<std::string, MockTableEntry> g_mockTables;

// Mirror of resolveVtableOTableInfo logic for testing
static int32_t testResolveVtableOTableInfo(
    const std::string& startTableName,
    const char*        refColName,
    OTableInfoRsp*     rsp) {
  int32_t     depth = 0;
  const int32_t maxDepth = 32;
  char        curRefCol[TSDB_COL_NAME_LEN];
  tstrncpy(curRefCol, refColName, TSDB_COL_NAME_LEN);

  std::string curTable = startTableName;

  while (depth < maxDepth) {
    auto it = g_mockTables.find(curTable);
    if (it == g_mockTables.end()) {
      return TSDB_CODE_PAR_TABLE_NOT_EXIST;
    }
    MockTableEntry& entry = it->second;

    if (entry.type != TD_VIRTUAL_NORMAL_TABLE && entry.type != TD_VIRTUAL_CHILD_TABLE) {
      return TSDB_CODE_INVALID_PARA;
    }

    // Find matching colRef
    MockColRef* pFound = nullptr;
    for (auto& cr : entry.colRefs) {
      if (cr.hasRef && strcmp(cr.colName.c_str(), curRefCol) == 0) {
        pFound = &cr;
        break;
      }
    }
    if (pFound == nullptr) {
      return TSDB_CODE_PAR_INVALID_COLUMN;
    }

    std::string nextDb    = pFound->refDbName;
    std::string nextTable = pFound->refTableName;
    std::string nextCol   = pFound->refColName;

    // Try to look up next-hop locally
    auto nextIt = g_mockTables.find(nextTable);
    if (nextIt == g_mockTables.end()) {
      // Not local: forward
      rsp->resolved = 0;
      rsp->suid = 0;
      rsp->uid = 0;
      rsp->cid = 0;
      tstrncpy(rsp->nextRefDbName, nextDb.c_str(), TSDB_DB_NAME_LEN);
      tstrncpy(rsp->nextRefTableName, nextTable.c_str(), TSDB_TABLE_NAME_LEN);
      tstrncpy(rsp->nextRefColName, nextCol.c_str(), TSDB_COL_NAME_LEN);
      return 0;
    }

    MockTableEntry& nextEntry = nextIt->second;
    if (nextEntry.type == TD_NORMAL_TABLE || nextEntry.type == TD_CHILD_TABLE) {
      // Physical table found: resolve
      rsp->resolved = 1;
      rsp->uid = nextEntry.uid;
      rsp->suid = nextEntry.suid;
      rsp->cid = 0;
      for (auto& s : nextEntry.schemas) {
        if (s.name == nextCol) {
          rsp->cid = s.colId;
          break;
        }
      }
      return 0;
    }

    if (nextEntry.type != TD_VIRTUAL_NORMAL_TABLE && nextEntry.type != TD_VIRTUAL_CHILD_TABLE) {
      return TSDB_CODE_INVALID_PARA;
    }

    // Continue chasing
    curTable = nextTable;
    tstrncpy(curRefCol, nextCol.c_str(), TSDB_COL_NAME_LEN);
    depth++;
  }

  return TSDB_CODE_STREAM_VTB_REF_TOO_DEEP;
}

class VtableResolveTest : public ::testing::Test {
 protected:
  void SetUp() override { g_mockTables.clear(); }
  void TearDown() override { g_mockTables.clear(); }
};

// Test 1: Single-hop vtable → physical_tb
TEST_F(VtableResolveTest, SingleHopToPhysical) {
  g_mockTables["vtable1"] = {
      .type = TD_VIRTUAL_NORMAL_TABLE,
      .uid = 1001,
      .suid = 0,
      .colRefs = {{"col_a", "db1", "physical_tb", "real_col", true}},
      .schemas = {}};

  g_mockTables["physical_tb"] = {
      .type = TD_NORMAL_TABLE,
      .uid = 2001,
      .suid = 0,
      .colRefs = {},
      .schemas = {{"real_col", 42}, {"other_col", 43}}};

  OTableInfoRsp rsp = {0};
  int32_t code = testResolveVtableOTableInfo("vtable1", "col_a", &rsp);
  EXPECT_EQ(code, 0);
  EXPECT_EQ(rsp.resolved, 1);
  EXPECT_EQ(rsp.uid, 2001);
  EXPECT_EQ(rsp.suid, 0);
  EXPECT_EQ(rsp.cid, 42);
}

// Test 2: Multi-hop local: vtable2 → vtable1 → physical_tb
TEST_F(VtableResolveTest, MultiHopLocal) {
  g_mockTables["vtable2"] = {
      .type = TD_VIRTUAL_NORMAL_TABLE,
      .uid = 1002,
      .suid = 0,
      .colRefs = {{"col_b", "db1", "vtable1", "col_a", true}},
      .schemas = {}};

  g_mockTables["vtable1"] = {
      .type = TD_VIRTUAL_NORMAL_TABLE,
      .uid = 1001,
      .suid = 0,
      .colRefs = {{"col_a", "db1", "physical_tb", "real_col", true}},
      .schemas = {}};

  g_mockTables["physical_tb"] = {
      .type = TD_NORMAL_TABLE,
      .uid = 2001,
      .suid = 0,
      .colRefs = {},
      .schemas = {{"real_col", 42}}};

  OTableInfoRsp rsp = {0};
  int32_t code = testResolveVtableOTableInfo("vtable2", "col_b", &rsp);
  EXPECT_EQ(code, 0);
  EXPECT_EQ(rsp.resolved, 1);
  EXPECT_EQ(rsp.uid, 2001);
  EXPECT_EQ(rsp.cid, 42);
}

// Test 3: Forward to remote (next-hop table not found locally)
TEST_F(VtableResolveTest, ForwardToRemote) {
  g_mockTables["vtable1"] = {
      .type = TD_VIRTUAL_NORMAL_TABLE,
      .uid = 1001,
      .suid = 0,
      .colRefs = {{"col_a", "remote_db", "remote_table", "remote_col", true}},
      .schemas = {}};
  // remote_table is NOT in g_mockTables (not local)

  OTableInfoRsp rsp = {0};
  int32_t code = testResolveVtableOTableInfo("vtable1", "col_a", &rsp);
  EXPECT_EQ(code, 0);
  EXPECT_EQ(rsp.resolved, 0);
  EXPECT_STREQ(rsp.nextRefDbName, "remote_db");
  EXPECT_STREQ(rsp.nextRefTableName, "remote_table");
  EXPECT_STREQ(rsp.nextRefColName, "remote_col");
}

// Test 4: Column not found in vtable's colRef
TEST_F(VtableResolveTest, ColumnNotFound) {
  g_mockTables["vtable1"] = {
      .type = TD_VIRTUAL_NORMAL_TABLE,
      .uid = 1001,
      .suid = 0,
      .colRefs = {{"col_a", "db1", "physical_tb", "real_col", true}},
      .schemas = {}};

  OTableInfoRsp rsp = {0};
  int32_t code = testResolveVtableOTableInfo("vtable1", "nonexistent_col", &rsp);
  EXPECT_EQ(code, TSDB_CODE_PAR_INVALID_COLUMN);
}

// Test 5: Depth limit exceeded (long chain)
TEST_F(VtableResolveTest, DepthLimitExceeded) {
  // Build a chain of 34 virtual tables: vt_0 → vt_1 → ... → vt_33
  for (int i = 0; i < 34; i++) {
    std::string name = "vt_" + std::to_string(i);
    std::string nextName = "vt_" + std::to_string(i + 1);
    g_mockTables[name] = {
        .type = TD_VIRTUAL_NORMAL_TABLE,
        .uid = (int64_t)(1000 + i),
        .suid = 0,
        .colRefs = {{"col", "db", nextName, "col", true}},
        .schemas = {}};
  }
  // vt_34 doesn't exist — but we should hit depth limit before reaching it

  OTableInfoRsp rsp = {0};
  int32_t code = testResolveVtableOTableInfo("vt_0", "col", &rsp);
  EXPECT_EQ(code, TSDB_CODE_STREAM_VTB_REF_TOO_DEEP);
}

// Test 6: Cycle detection (A → B → A) — bounded by depth limit
TEST_F(VtableResolveTest, CycleDetection) {
  g_mockTables["vt_a"] = {
      .type = TD_VIRTUAL_NORMAL_TABLE,
      .uid = 1001,
      .suid = 0,
      .colRefs = {{"col", "db", "vt_b", "col", true}},
      .schemas = {}};

  g_mockTables["vt_b"] = {
      .type = TD_VIRTUAL_NORMAL_TABLE,
      .uid = 1002,
      .suid = 0,
      .colRefs = {{"col", "db", "vt_a", "col", true}},
      .schemas = {}};

  OTableInfoRsp rsp = {0};
  int32_t code = testResolveVtableOTableInfo("vt_a", "col", &rsp);
  EXPECT_EQ(code, TSDB_CODE_STREAM_VTB_REF_TOO_DEEP);
}

// Test 7: Physical child table resolution (suid from parent)
TEST_F(VtableResolveTest, PhysicalChildTable) {
  g_mockTables["vtable1"] = {
      .type = TD_VIRTUAL_NORMAL_TABLE,
      .uid = 1001,
      .suid = 0,
      .colRefs = {{"col_a", "db1", "child_tb", "ts_col", true}},
      .schemas = {}};

  g_mockTables["child_tb"] = {
      .type = TD_CHILD_TABLE,
      .uid = 3001,
      .suid = 5000,  // parent super table
      .colRefs = {},
      .schemas = {{"ts_col", 1}, {"val_col", 2}}};

  OTableInfoRsp rsp = {0};
  int32_t code = testResolveVtableOTableInfo("vtable1", "col_a", &rsp);
  EXPECT_EQ(code, 0);
  EXPECT_EQ(rsp.resolved, 1);
  EXPECT_EQ(rsp.uid, 3001);
  EXPECT_EQ(rsp.suid, 5000);
  EXPECT_EQ(rsp.cid, 1);
}

// Test 8: Terminal physical column not found (cid stays 0)
TEST_F(VtableResolveTest, TerminalColumnNotFound) {
  g_mockTables["vtable1"] = {
      .type = TD_VIRTUAL_NORMAL_TABLE,
      .uid = 1001,
      .suid = 0,
      .colRefs = {{"col_a", "db1", "physical_tb", "missing_col", true}},
      .schemas = {}};

  g_mockTables["physical_tb"] = {
      .type = TD_NORMAL_TABLE,
      .uid = 2001,
      .suid = 0,
      .colRefs = {},
      .schemas = {{"real_col", 42}}};  // "missing_col" not here

  OTableInfoRsp rsp = {0};
  int32_t code = testResolveVtableOTableInfo("vtable1", "col_a", &rsp);
  EXPECT_EQ(code, 0);
  EXPECT_EQ(rsp.resolved, 1);
  EXPECT_EQ(rsp.uid, 2001);
  EXPECT_EQ(rsp.cid, 0);  // column not found → cid stays 0
}

// Test 9: colRef with hasRef=false should be skipped
TEST_F(VtableResolveTest, SkipsNonRefColumns) {
  g_mockTables["vtable1"] = {
      .type = TD_VIRTUAL_NORMAL_TABLE,
      .uid = 1001,
      .suid = 0,
      .colRefs = {
          {"col_a", "", "", "", false},  // no ref (computed column)
          {"col_b", "db1", "physical_tb", "real_col", true}},
      .schemas = {}};

  g_mockTables["physical_tb"] = {
      .type = TD_NORMAL_TABLE,
      .uid = 2001,
      .suid = 0,
      .colRefs = {},
      .schemas = {{"real_col", 42}}};

  // col_a has hasRef=false, should return column not found
  OTableInfoRsp rsp1 = {0};
  int32_t code = testResolveVtableOTableInfo("vtable1", "col_a", &rsp1);
  EXPECT_EQ(code, TSDB_CODE_PAR_INVALID_COLUMN);

  // col_b has hasRef=true, should resolve
  OTableInfoRsp rsp2 = {0};
  code = testResolveVtableOTableInfo("vtable1", "col_b", &rsp2);
  EXPECT_EQ(code, 0);
  EXPECT_EQ(rsp2.resolved, 1);
  EXPECT_EQ(rsp2.cid, 42);
}

// Test 10: Three-hop: vtable3 → vtable2 → vtable1 → physical_tb
TEST_F(VtableResolveTest, ThreeHopChain) {
  g_mockTables["vtable3"] = {
      .type = TD_VIRTUAL_NORMAL_TABLE,
      .uid = 1003,
      .suid = 0,
      .colRefs = {{"c", "db", "vtable2", "b", true}},
      .schemas = {}};

  g_mockTables["vtable2"] = {
      .type = TD_VIRTUAL_NORMAL_TABLE,
      .uid = 1002,
      .suid = 0,
      .colRefs = {{"b", "db", "vtable1", "a", true}},
      .schemas = {}};

  g_mockTables["vtable1"] = {
      .type = TD_VIRTUAL_NORMAL_TABLE,
      .uid = 1001,
      .suid = 0,
      .colRefs = {{"a", "db", "physical_tb", "real", true}},
      .schemas = {}};

  g_mockTables["physical_tb"] = {
      .type = TD_NORMAL_TABLE,
      .uid = 2001,
      .suid = 0,
      .colRefs = {},
      .schemas = {{"real", 99}}};

  OTableInfoRsp rsp = {0};
  int32_t code = testResolveVtableOTableInfo("vtable3", "c", &rsp);
  EXPECT_EQ(code, 0);
  EXPECT_EQ(rsp.resolved, 1);
  EXPECT_EQ(rsp.uid, 2001);
  EXPECT_EQ(rsp.cid, 99);
}

// Test 11: Mixed chain — local vtable hops then forwards to remote
TEST_F(VtableResolveTest, LocalHopsThenForward) {
  g_mockTables["vtable2"] = {
      .type = TD_VIRTUAL_NORMAL_TABLE,
      .uid = 1002,
      .suid = 0,
      .colRefs = {{"col", "db", "vtable1", "col", true}},
      .schemas = {}};

  g_mockTables["vtable1"] = {
      .type = TD_VIRTUAL_NORMAL_TABLE,
      .uid = 1001,
      .suid = 0,
      .colRefs = {{"col", "remote_db", "remote_tb", "remote_col", true}},
      .schemas = {}};
  // remote_tb not in local tables

  OTableInfoRsp rsp = {0};
  int32_t code = testResolveVtableOTableInfo("vtable2", "col", &rsp);
  EXPECT_EQ(code, 0);
  EXPECT_EQ(rsp.resolved, 0);
  EXPECT_STREQ(rsp.nextRefDbName, "remote_db");
  EXPECT_STREQ(rsp.nextRefTableName, "remote_tb");
  EXPECT_STREQ(rsp.nextRefColName, "remote_col");
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
