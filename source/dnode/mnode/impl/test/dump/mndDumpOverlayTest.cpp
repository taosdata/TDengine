/**
 * Unit tests for the sdb.json import field-overlay logic in mndDump.c
 * (FS "支持修改手动修改 sdb"), covering the pure per-type mapping helpers
 * without booting a full mnode. See mndDump.h for why these are exposed.
 */

#include <gtest/gtest.h>

#include "mndDef.h"
#include "mndDump.h"
#include "sdb.h"
#include "tjson.h"

// sdbTypeFromName: every named sdb table round-trips through sdbTableName,
// and an unrecognized name maps to SDB_MAX ("unknown" sentinel used by
// mndImportRecord to reject a record).
TEST(mndDumpOverlay, sdbTypeFromName_roundtrip) {
  int32_t matched = 0;
  for (int32_t t = 0; t < SDB_MAX; ++t) {
    const char *name = sdbTableName((ESdbType)t);
    if (name == nullptr || strcmp(name, "undefine") == 0) continue;
    EXPECT_EQ(sdbTypeFromName(name), (ESdbType)t) << "type index " << t << " name " << name;
    matched++;
  }
  EXPECT_GT(matched, 0);

  EXPECT_EQ(sdbTypeFromName("no-such-sdb-type"), SDB_MAX);
  EXPECT_EQ(sdbTypeFromName(""), SDB_MAX);
}

// isOverlayType: locks in the FS 4.4 table-type classification. dnode/vgroup/
// db/cluster/config/compact support scalar overlay; mnode/user are rawData-only
// (privileges and sync-managed role must never be settable via `fields`).
TEST(mndDumpOverlay, isOverlayType_classification) {
  EXPECT_TRUE(isOverlayType(SDB_DNODE));
  EXPECT_TRUE(isOverlayType(SDB_VGROUP));
  EXPECT_TRUE(isOverlayType(SDB_DB));
  EXPECT_TRUE(isOverlayType(SDB_CLUSTER));
  EXPECT_TRUE(isOverlayType(SDB_CFG));
  EXPECT_TRUE(isOverlayType(SDB_COMPACT));

  EXPECT_FALSE(isOverlayType(SDB_MNODE));
  EXPECT_FALSE(isOverlayType(SDB_USER));
  EXPECT_FALSE(isOverlayType(SDB_TRANS));
  EXPECT_FALSE(isOverlayType(SDB_STB));
}

TEST(mndDumpOverlay, overlayDnode_fqdnAndPort) {
  SDnodeObj obj = {0};
  tstrncpy(obj.fqdn, "old.example.com", sizeof(obj.fqdn));
  obj.port = 6030;

  SJson *f = tjsonCreateObject();
  ASSERT_EQ(tjsonAddStringToObject(f, "fqdn", "new.example.com"), 0);
  ASSERT_EQ(tjsonAddStringToObject(f, "port", "6040"), 0);
  overlayDnode(&obj, f);
  EXPECT_STREQ(obj.fqdn, "new.example.com");
  EXPECT_EQ(obj.port, 6040);
  tjsonDelete(f);

  // Keys absent from `fields` must leave the decoded value untouched.
  SJson *f2 = tjsonCreateObject();
  overlayDnode(&obj, f2);
  EXPECT_STREQ(obj.fqdn, "new.example.com");
  EXPECT_EQ(obj.port, 6040);
  tjsonDelete(f2);
}

TEST(mndDumpOverlay, overlayVgroup_replicaAndDnodeIds) {
  SVgObj obj = {0};
  obj.replica = 1;
  obj.vnodeGid[0].dnodeId = 1;

  SJson *f = tjsonCreateObject();
  ASSERT_EQ(tjsonAddStringToObject(f, "replica", "2"), 0);
  SJson *reps = tjsonAddArrayToObject(f, "replicas");
  SJson *r0 = tjsonCreateObject();
  ASSERT_EQ(tjsonAddStringToObject(r0, "dnodeId", "3"), 0);
  ASSERT_EQ(tjsonAddItemToArray(reps, r0), 0);
  SJson *r1 = tjsonCreateObject();
  ASSERT_EQ(tjsonAddStringToObject(r1, "dnodeId", "4"), 0);
  ASSERT_EQ(tjsonAddItemToArray(reps, r1), 0);

  overlayVgroup(&obj, f);
  EXPECT_EQ(obj.replica, 2);
  EXPECT_EQ(obj.vnodeGid[0].dnodeId, 3);
  EXPECT_EQ(obj.vnodeGid[1].dnodeId, 4);
  tjsonDelete(f);

  // Absent `replicas` key: existing dnode mapping must survive untouched.
  SJson *f2 = tjsonCreateObject();
  overlayVgroup(&obj, f2);
  EXPECT_EQ(obj.vnodeGid[0].dnodeId, 3);
  EXPECT_EQ(obj.vnodeGid[1].dnodeId, 4);
  tjsonDelete(f2);
}

TEST(mndDumpOverlay, overlayCluster_name) {
  SClusterObj obj = {0};
  tstrncpy(obj.name, "old-cluster-id", sizeof(obj.name));

  SJson *f = tjsonCreateObject();
  ASSERT_EQ(tjsonAddStringToObject(f, "name", "new-cluster-id"), 0);
  overlayCluster(&obj, f);
  EXPECT_STREQ(obj.name, "new-cluster-id");
  tjsonDelete(f);
}

TEST(mndDumpOverlay, overlayDb_replicationsBufferWalLevel) {
  SDbObj obj = {0};
  obj.cfg.replications = 1;
  obj.cfg.buffer = 96;
  obj.cfg.walLevel = 1;
  // A field not in the FS's editable list for `db` must never be touched by overlayDb.
  obj.cfg.pages = 256;

  SJson *f = tjsonCreateObject();
  ASSERT_EQ(tjsonAddStringToObject(f, "replications", "3"), 0);
  ASSERT_EQ(tjsonAddStringToObject(f, "buffer", "256"), 0);
  ASSERT_EQ(tjsonAddStringToObject(f, "walLevel", "2"), 0);
  overlayDb(&obj, f);
  EXPECT_EQ(obj.cfg.replications, 3);
  EXPECT_EQ(obj.cfg.buffer, 256);
  EXPECT_EQ(obj.cfg.walLevel, 2);
  EXPECT_EQ(obj.cfg.pages, 256);  // untouched: not in the FS 4.4 db overlay list
  tjsonDelete(f);
}

// overlayConfig: the FS's most subtle rule -- `fields.value` overlay only takes
// effect for numeric dtypes (bool/int32/int64/float/double); a string-like dtype
// must be left decoding straight from rawData, even if `fields.value` is present.
TEST(mndDumpOverlay, overlayConfig_numericDtypesOverlay) {
  {
    SConfigObj obj = {0};
    obj.dtype = CFG_DTYPE_BOOL;
    obj.bval = false;
    SJson *f = tjsonCreateObject();
    ASSERT_EQ(tjsonAddStringToObject(f, "value", "1"), 0);
    overlayConfig(&obj, f);
    EXPECT_EQ(obj.bval, true);
    tjsonDelete(f);
  }
  {
    SConfigObj obj = {0};
    obj.dtype = CFG_DTYPE_INT32;
    obj.i32 = 10;
    SJson *f = tjsonCreateObject();
    ASSERT_EQ(tjsonAddStringToObject(f, "value", "42"), 0);
    overlayConfig(&obj, f);
    EXPECT_EQ(obj.i32, 42);
    tjsonDelete(f);
  }
  {
    SConfigObj obj = {0};
    obj.dtype = CFG_DTYPE_INT64;
    obj.i64 = 10;
    SJson *f = tjsonCreateObject();
    ASSERT_EQ(tjsonAddStringToObject(f, "value", "4200000000"), 0);
    overlayConfig(&obj, f);
    EXPECT_EQ(obj.i64, 4200000000LL);
    tjsonDelete(f);
  }
  {
    SConfigObj obj = {0};
    obj.dtype = CFG_DTYPE_DOUBLE;
    obj.fval = 1.0f;
    SJson *f = tjsonCreateObject();
    ASSERT_EQ(tjsonAddStringToObject(f, "value", "2.5"), 0);
    overlayConfig(&obj, f);
    EXPECT_FLOAT_EQ(obj.fval, 2.5f);
    tjsonDelete(f);
  }
}

TEST(mndDumpOverlay, overlayConfig_stringDtypeNotOverlaid) {
  SConfigObj      obj = {0};
  static char origStr[] = "/var/lib/original";
  obj.dtype = CFG_DTYPE_STRING;
  obj.str = origStr;

  SJson *f = tjsonCreateObject();
  ASSERT_EQ(tjsonAddStringToObject(f, "value", "/tmp/injected-should-not-apply"), 0);
  overlayConfig(&obj, f);
  // Per FS 4.4: string-typed config value overlay is intentionally a no-op;
  // fixing it requires editing rawData directly.
  EXPECT_STREQ(obj.str, "/var/lib/original");
  tjsonDelete(f);
}

TEST(mndDumpOverlay, overlayConfig_missingValueKeyIsNoop) {
  SConfigObj obj = {0};
  obj.dtype = CFG_DTYPE_INT32;
  obj.i32 = 7;

  SJson *f = tjsonCreateObject();
  overlayConfig(&obj, f);
  EXPECT_EQ(obj.i32, 7);
  tjsonDelete(f);
}

TEST(mndDumpOverlay, overlayCompact_compactIdStartTimeDbname) {
  SCompactObj obj = {0};
  obj.compactId = 1;
  obj.startTime = 1000;
  tstrncpy(obj.dbname, "db1", sizeof(obj.dbname));
  obj.compactDetail = nullptr;  // no detail array in this fixture

  SJson *f = tjsonCreateObject();
  ASSERT_EQ(tjsonAddStringToObject(f, "compactId", "2"), 0);
  ASSERT_EQ(tjsonAddStringToObject(f, "startTime", "2000"), 0);
  ASSERT_EQ(tjsonAddStringToObject(f, "dbname", "db2"), 0);
  overlayCompact(&obj, f);
  EXPECT_EQ(obj.compactId, 2);
  EXPECT_EQ(obj.startTime, 2000);
  EXPECT_STREQ(obj.dbname, "db2");
  // overlayCompact must never touch compactDetail: per FS 4.4 the nested
  // array does not participate in scalar overlay and stays as decoded.
  EXPECT_EQ(obj.compactDetail, nullptr);
  tjsonDelete(f);
}
