/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <gtest/gtest.h>

#include "catalogInt.h"
#include "parser.h"
#include "taoserror.h"
#include "thash.h"

// Unit tests for the batch-meta txn "touched table" check that decides whether a table meta
// response may be written to the shared (committed-view) catalog cache.

namespace {

const char* kDb = "1.db1";

struct TxnFixture {
  SHashObj* pTableMeta = nullptr;
  SHashObj* pSuidMap = nullptr;

  TxnFixture() {
    pTableMeta = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
    pSuidMap = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK);
  }
  ~TxnFixture() {
    taosHashCleanup(pTableMeta);
    taosHashCleanup(pSuidMap);
  }
  // NULL value is the "dropped in txn" sentinel
  void touchName(const char* fullName, STableMeta* v = nullptr) {
    ASSERT_EQ(0, taosHashPut(pTableMeta, fullName, strlen(fullName), &v, sizeof(v)));
  }
  // stbChanged: the txn created / altered / dropped the super table, which leaves an entry under its name in
  // pTxnTableMeta. false: the suid is only mapped because the txn created a child table of it.
  void touchSuid(uint64_t suid, const char* stbFullName, bool stbChanged = true) {
    char name[TSDB_TABLE_FNAME_LEN] = {0};
    snprintf(name, sizeof(name), "%s", stbFullName);
    ASSERT_EQ(0, taosHashPut(pSuidMap, &suid, sizeof(suid), name, TSDB_TABLE_FNAME_LEN));
    if (stbChanged) touchName(stbFullName);
  }
  SRequestConnInfo conn(int64_t txnId) const {
    SRequestConnInfo c = {};
    c.txnId = txnId;
    c.pTxnTableMeta = pTableMeta;
    c.pTxnSuidMap = pSuidMap;
    return c;
  }
};

void initOut(STableMetaOutput* o, int32_t type, const char* tb, const char* ctb) {
  memset(o, 0, sizeof(*o));
  o->metaType = type;
  snprintf(o->dbFName, sizeof(o->dbFName), "%s", kDb);
  snprintf(o->tbName, sizeof(o->tbName), "%s", tb);
  snprintf(o->ctbName, sizeof(o->ctbName), "%s", ctb);
}

}  // namespace

TEST(ctgTxnTouched, notInTxn) {
  TxnFixture f;
  f.touchName("1.db1.t1");
  SRequestConnInfo c = f.conn(0);
  STableMetaOutput o;
  initOut(&o, META_TYPE_TABLE, "t1", "");
  ASSERT_FALSE(ctgIsMetaTouchedByTxn(&c, &o));
}

TEST(ctgTxnTouched, txnTouchedNothing) {
  // inside a txn the hashes exist but are empty: nothing is touched
  TxnFixture f;
  SRequestConnInfo c = f.conn(7);
  STableMetaOutput o;
  initOut(&o, META_TYPE_TABLE, "t1", "");
  ASSERT_FALSE(ctgIsMetaTouchedByTxn(&c, &o));
  initOut(&o, META_TYPE_CTABLE, "stb", "ct1");
  o.ctbMeta.suid = 1001;
  ASSERT_FALSE(ctgIsMetaTouchedByTxn(&c, &o));
}

// txnId filled by hand without the txn-local hashes: the touched set is unknown, so be conservative
TEST(ctgTxnTouched, txnWithoutHashesIsConservative) {
  SRequestConnInfo c = {};
  c.txnId = 7;
  STableMetaOutput o;
  initOut(&o, META_TYPE_TABLE, "t1", "");
  ASSERT_TRUE(ctgIsMetaTouchedByTxn(&c, &o));
  initOut(&o, META_TYPE_CTABLE, "stb", "ct1");
  o.ctbMeta.suid = 1001;
  ASSERT_TRUE(ctgIsMetaTouchedByTxn(&c, &o));

  SName      name = {};
  STableMeta m;
  memset(&m, 0, sizeof(m));
  ASSERT_EQ(0, tNameFromString(&name, "1.db1.ct1", T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE));
  ASSERT_TRUE(ctgIsCachedMetaTouchedByTxn(&c, &name, &m));

  // a request that is not in a txn is never affected
  c.txnId = 0;
  ASSERT_FALSE(ctgIsMetaTouchedByTxn(&c, &o));
}

TEST(ctgTxnTouched, byName) {
  TxnFixture f;
  f.touchName("1.db1.t1");
  SRequestConnInfo c = f.conn(7);
  STableMetaOutput o;
  initOut(&o, META_TYPE_TABLE, "t1", "");
  ASSERT_TRUE(ctgIsMetaTouchedByTxn(&c, &o));
  initOut(&o, META_TYPE_TABLE, "t2", "");
  ASSERT_FALSE(ctgIsMetaTouchedByTxn(&c, &o));
  // same table name in another db is a different table
  initOut(&o, META_TYPE_TABLE, "t1", "");
  snprintf(o.dbFName, sizeof(o.dbFName), "%s", "1.db2");
  ASSERT_FALSE(ctgIsMetaTouchedByTxn(&c, &o));
}

TEST(ctgTxnTouched, droppedSentinel) {
  TxnFixture f;
  f.touchName("1.db1.t1", nullptr);
  SRequestConnInfo c = f.conn(7);
  STableMetaOutput o;
  initOut(&o, META_TYPE_TABLE, "t1", "");
  ASSERT_TRUE(ctgIsMetaTouchedByTxn(&c, &o));
}

TEST(ctgTxnTouched, childByOwnName) {
  TxnFixture f;
  f.touchName("1.db1.ct1");
  SRequestConnInfo c = f.conn(7);
  STableMetaOutput o;
  initOut(&o, META_TYPE_CTABLE, "stb", "ct1");
  ASSERT_TRUE(ctgIsMetaTouchedByTxn(&c, &o));
  initOut(&o, META_TYPE_CTABLE, "stb", "ct2");
  ASSERT_FALSE(ctgIsMetaTouchedByTxn(&c, &o));
}

TEST(ctgTxnTouched, childByStbSuid) {
  TxnFixture f;
  f.touchSuid(1001, "1.db1.stb_by_suid");  // judged by suid: the name differs from the tbName below
  SRequestConnInfo c = f.conn(7);
  STableMetaOutput o;

  initOut(&o, META_TYPE_CTABLE, "stb", "ct1");
  o.ctbMeta.suid = 1001;
  ASSERT_TRUE(ctgIsMetaTouchedByTxn(&c, &o));
  o.ctbMeta.suid = 1002;
  ASSERT_FALSE(ctgIsMetaTouchedByTxn(&c, &o));

  initOut(&o, META_TYPE_BOTH_TABLE, "stb", "ct1");
  o.ctbMeta.suid = 1001;
  ASSERT_TRUE(ctgIsMetaTouchedByTxn(&c, &o));
}

TEST(ctgTxnTouched, virtualChildByStbSuid) {
  TxnFixture f;
  f.touchSuid(2001, "1.db1.vstb_by_suid");  // judged by suid: the name differs from the tbName below
  SRequestConnInfo c = f.conn(7);
  STableMetaOutput o;
  SVCTableMeta     vc;
  memset(&vc, 0, sizeof(vc));

  initOut(&o, META_TYPE_VCTABLE, "vstb", "vct1");
  o.vctbMeta = &vc;
  vc.suid = 2001;
  ASSERT_TRUE(ctgIsMetaTouchedByTxn(&c, &o));
  vc.suid = 2002;
  ASSERT_FALSE(ctgIsMetaTouchedByTxn(&c, &o));

  initOut(&o, META_TYPE_BOTH_VTABLE, "vstb", "vct1");
  o.vctbMeta = &vc;
  vc.suid = 2001;
  ASSERT_TRUE(ctgIsMetaTouchedByTxn(&c, &o));

  // VCTABLE without vctbMeta must not crash and is judged by name only
  initOut(&o, META_TYPE_VCTABLE, "vstb", "vct1");
  ASSERT_FALSE(ctgIsMetaTouchedByTxn(&c, &o));
}

TEST(ctgTxnTouched, stbMetaBySuid) {
  TxnFixture f;
  f.touchSuid(3001, "1.db1.stb");
  SRequestConnInfo c = f.conn(7);
  STableMetaOutput o;
  STableMeta       m;
  memset(&m, 0, sizeof(m));

  // super table itself: its uid is the suid
  initOut(&o, META_TYPE_TABLE, "other", "");
  o.tbMeta = &m;
  m.tableType = TSDB_SUPER_TABLE;
  m.uid = 3001;
  ASSERT_TRUE(ctgIsMetaTouchedByTxn(&c, &o));
  m.uid = 3002;
  ASSERT_FALSE(ctgIsMetaTouchedByTxn(&c, &o));

  // child table meta whose suid is touched
  m.tableType = TSDB_CHILD_TABLE;
  m.uid = 9;
  m.suid = 3001;
  ASSERT_TRUE(ctgIsMetaTouchedByTxn(&c, &o));
}

TEST(ctgTxnTouched, parseCxtToConnInfoCopiesAllTxnFields) {
  TxnFixture f;
  SParseContext cxt = {};
  cxt.requestId = 11;
  cxt.requestRid = 12;
  cxt.txnId = 13;
  cxt.pTxnTableMeta = f.pTableMeta;
  cxt.pTxnSuidMap = f.pSuidMap;
  cxt.pTransporter = (void*)0x10;

  SRequestConnInfo c = {};
  parseCxtToConnInfo(&cxt, &c);
  ASSERT_EQ(c.requestId, 11u);
  ASSERT_EQ(c.requestObjRefId, 12);
  ASSERT_EQ(c.txnId, 13);
  ASSERT_EQ(c.pTxnTableMeta, f.pTableMeta);
  ASSERT_EQ(c.pTxnSuidMap, f.pSuidMap);
  ASSERT_EQ(c.pTrans, (void*)0x10);
}

// ctgIsCachedMetaTouchedByTxn decides whether a meta found in the shared (committed-view) cache
// must be ignored by the txn connection.
TEST(ctgTxnTouched, cachedMetaNotInTxn) {
  TxnFixture f;
  f.touchName("1.db1.t1");
  f.touchSuid(1001, "1.db1.stb");
  SRequestConnInfo c = f.conn(0);
  SName            name = {};
  ASSERT_EQ(0, tNameFromString(&name, "1.db1.t1", T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE));
  STableMeta m;
  memset(&m, 0, sizeof(m));
  m.suid = 1001;
  ASSERT_FALSE(ctgIsCachedMetaTouchedByTxn(&c, &name, &m));
  ASSERT_FALSE(ctgIsCachedMetaTouchedByTxn(nullptr, &name, &m));
}

TEST(ctgTxnTouched, cachedMetaByNameAndSuid) {
  TxnFixture f;
  f.touchName("1.db1.t1");
  f.touchSuid(1001, "1.db1.stb");
  SRequestConnInfo c = f.conn(7);
  SName            name = {};
  STableMeta       m;
  memset(&m, 0, sizeof(m));

  ASSERT_EQ(0, tNameFromString(&name, "1.db1.t1", T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE));
  ASSERT_TRUE(ctgIsCachedMetaTouchedByTxn(&c, &name, &m));     // touched by name
  ASSERT_TRUE(ctgIsCachedMetaTouchedByTxn(&c, &name, nullptr));

  ASSERT_EQ(0, tNameFromString(&name, "1.db1.ct1", T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE));
  ASSERT_FALSE(ctgIsCachedMetaTouchedByTxn(&c, &name, nullptr));  // untouched, no meta to inspect
  m.suid = 1001;                                                   // child of a touched super table
  m.tableType = TSDB_CHILD_TABLE;
  ASSERT_TRUE(ctgIsCachedMetaTouchedByTxn(&c, &name, &m));
  m.suid = 1002;
  ASSERT_FALSE(ctgIsCachedMetaTouchedByTxn(&c, &name, &m));

  m.tableType = TSDB_SUPER_TABLE;                                  // the super table itself: uid is the suid
  m.suid = 0;
  m.uid = 1001;
  ASSERT_TRUE(ctgIsCachedMetaTouchedByTxn(&c, &name, &m));
  m.uid = 1002;
  ASSERT_FALSE(ctgIsCachedMetaTouchedByTxn(&c, &name, &m));

  m.tableType = TSDB_VIRTUAL_CHILD_TABLE;                          // virtual child of a touched virtual stable
  m.suid = 1001;
  ASSERT_TRUE(ctgIsCachedMetaTouchedByTxn(&c, &name, &m));
}

TEST(ctgTxnTouched, nullArguments) {
  TxnFixture f;
  f.touchName("1.db1.t1");
  SRequestConnInfo c = f.conn(7);
  STableMetaOutput o;
  initOut(&o, META_TYPE_TABLE, "t1", "");
  ASSERT_FALSE(ctgIsMetaTouchedByTxn(nullptr, &o));
  ASSERT_FALSE(ctgIsMetaTouchedByTxn(&c, nullptr));
  SName name = {};
  ASSERT_EQ(0, tNameFromString(&name, "1.db1.t1", T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE));
  ASSERT_FALSE(ctgIsCachedMetaTouchedByTxn(nullptr, &name, nullptr));
}

// A super table whose suid is mapped only because the txn created a child table of it is not touched: creating a
// child changes neither the super table nor its other children, so both stay cacheable. The child itself is
// touched (by its name), and so is everything once the txn really changes the super table.
TEST(ctgTxnTouched, childCreationDoesNotTouchItsSuperTable) {
  TxnFixture f;
  f.touchSuid(4001, "1.db1.stb", false);
  f.touchName("1.db1.ct_new");
  SRequestConnInfo c = f.conn(7);
  STableMetaOutput o;
  STableMeta       m;
  memset(&m, 0, sizeof(m));

  // the super table itself
  initOut(&o, META_TYPE_TABLE, "stb", "");
  o.tbMeta = &m;
  m.tableType = TSDB_SUPER_TABLE;
  m.uid = 4001;
  ASSERT_FALSE(ctgIsMetaTouchedByTxn(&c, &o));

  // an existing child of it
  initOut(&o, META_TYPE_CTABLE, "stb", "ct_old");
  o.ctbMeta.suid = 4001;
  ASSERT_FALSE(ctgIsMetaTouchedByTxn(&c, &o));

  // the child the txn created
  initOut(&o, META_TYPE_CTABLE, "stb", "ct_new");
  o.ctbMeta.suid = 4001;
  ASSERT_TRUE(ctgIsMetaTouchedByTxn(&c, &o));

  // cached lookups
  SName name = {};
  ASSERT_EQ(0, tNameFromString(&name, "1.db1.stb", T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE));
  ASSERT_FALSE(ctgIsCachedMetaTouchedByTxn(&c, &name, &m));
  m.tableType = TSDB_CHILD_TABLE;
  m.uid = 9;
  m.suid = 4001;
  ASSERT_EQ(0, tNameFromString(&name, "1.db1.ct_old", T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE));
  ASSERT_FALSE(ctgIsCachedMetaTouchedByTxn(&c, &name, &m));

  // once the txn alters (or drops) the super table, it and all its children are touched
  f.touchName("1.db1.stb");
  ASSERT_TRUE(ctgIsCachedMetaTouchedByTxn(&c, &name, &m));
  initOut(&o, META_TYPE_CTABLE, "stb", "ct_old");
  o.ctbMeta.suid = 4001;
  ASSERT_TRUE(ctgIsMetaTouchedByTxn(&c, &o));
}

// Only one of the two txn-local hashes present: which super tables were changed cannot be told, so be conservative.
TEST(ctgTxnTouched, suidCheckWithoutTableMetaHashIsConservative) {
  TxnFixture f;
  f.touchSuid(5001, "1.db1.stb", false);
  SRequestConnInfo c = f.conn(7);
  c.pTxnTableMeta = nullptr;
  STableMetaOutput o;
  initOut(&o, META_TYPE_CTABLE, "", "ct1");
  o.ctbMeta.suid = 5001;
  ASSERT_TRUE(ctgIsMetaTouchedByTxn(&c, &o));
}
