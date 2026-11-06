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

/**
 * Unit tests for the idempotence guard on the virtual-table column-ref ALTER path
 * (metaTable2.c metaAlterTableColumnRef).
 *
 * IDMP re-binds every `ans*` virtual table on a 6-hour timer without diffing against the
 * current state first. Before the guard, every one of those ALTERs overwrote the SColRef,
 * bumped pSchema->version and pEntry->colRef.version and wrote the entry back, which makes
 * the mnode broadcast a new super-table version and every client drop its cached meta for a
 * vstb with tens of thousands of children.
 *
 * metaColRefMatchesAlterReq() is the decision: true means applying the request would leave
 * every persisted field byte-identical, so the write and both version bumps are skipped.
 * metaResolveColRefTagCond() feeds it the series tag condition the request would install,
 * resolved from the entry without copying so the comparison stays allocation-free.
 *
 * Both helpers are declared in meta.h so this binary can exercise them without standing up
 * an SMeta/SVnode.
 */

#include <cstring>

#include "gtest/gtest.h"

extern "C" {
#include "meta.h"
#include "taoserror.h"
#include "tmsg.h"
#include "vnodeInt.h"
}

// Referenced by the meta audit-notify path pulled in via libvnode (state stays 0 => no-op).
SDmNotifyHandle dmNotifyHdl = {};

namespace {

constexpr col_id_t kColId = 7;

// A stored reference that matches `baseReq()` exactly.
SColRef baseColRef() {
  SColRef colRef = {};
  colRef.hasRef = true;
  colRef.id = kColId;
  colRef.refType = 0;
  tstrncpy(colRef.refSourceName, "", TSDB_EXT_SOURCE_NAME_LEN);
  tstrncpy(colRef.refSchemaName, "", TSDB_EXT_SOURCE_SCHEMA_LEN);
  tstrncpy(colRef.refDbName, "idmp", TSDB_DB_NAME_LEN);
  tstrncpy(colRef.refTableName, "org_ctb_1", TSDB_TABLE_NAME_LEN);
  tstrncpy(colRef.refColName, "current", TSDB_COL_NAME_LEN);
  return colRef;
}

SVAlterTbReq baseReq() {
  SVAlterTbReq req = {};
  req.action = TSDB_ALTER_TABLE_ALTER_COLUMN_REF;
  req.tbName = (char *)"vst_child_1";
  req.colName = (char *)"vcol";
  req.colId = kColId;
  req.refType = 0;
  req.refDbName = (char *)"idmp";
  req.refTbName = (char *)"org_ctb_1";
  req.refColName = (char *)"current";
  return req;
}

bool matches(const SColRef &colRef, const SVAlterTbReq &req, const char *tagCondJson = nullptr,
             int32_t tagCondLen = 0) {
  return metaColRefMatchesAlterReq(&colRef, &req, kColId, tagCondJson, tagCondLen);
}

SSeriesEntry makeSeries(const char *alias, const char *sourceName, const char *dbName, const char *measurementName,
                        const char *tagCondJson) {
  SSeriesEntry series = {};
  tstrncpy(series.alias, alias, TSDB_COL_NAME_LEN);
  tstrncpy(series.sourceName, sourceName, TSDB_EXT_SOURCE_NAME_LEN);
  tstrncpy(series.dbName, dbName, TSDB_DB_NAME_LEN);
  tstrncpy(series.measurementName, measurementName, TSDB_TABLE_NAME_LEN);
  if (tagCondJson != nullptr) {
    series.tagCondJson = (char *)tagCondJson;
    series.tagCondLen = (int32_t)strlen(tagCondJson) + 1;
  }
  return series;
}

}  // namespace

// ---------------------------------------------------------------------------
// metaColRefMatchesAlterReq: the no-op case the 6-hour IDMP rebind produces
// ---------------------------------------------------------------------------

TEST(metaColRefIdempotentTest, identicalLocalRefIsIdempotent) {
  SColRef      colRef = baseColRef();
  SVAlterTbReq req = baseReq();
  ASSERT_TRUE(matches(colRef, req));
}

TEST(metaColRefIdempotentTest, absentSourceAndSchemaNamesNormaliseToEmpty) {
  SColRef      colRef = baseColRef();
  SVAlterTbReq req = baseReq();

  // The write path stores "" for a NULL/empty source or schema name, so a request that omits
  // them must still compare equal to an entry that was written from such a request.
  req.refSourceName = nullptr;
  req.refSchemaName = nullptr;
  ASSERT_TRUE(matches(colRef, req));

  req.refSourceName = (char *)"";
  req.refSchemaName = (char *)"";
  ASSERT_TRUE(matches(colRef, req));
}

TEST(metaColRefIdempotentTest, columnWithoutRefIsNotIdempotent) {
  SColRef      colRef = baseColRef();
  SVAlterTbReq req = baseReq();

  // `SET NULL` cleared the ref; re-binding it is a real change.
  colRef.hasRef = false;
  ASSERT_FALSE(matches(colRef, req));
}

TEST(metaColRefIdempotentTest, differentColumnIdIsNotIdempotent) {
  SColRef      colRef = baseColRef();
  SVAlterTbReq req = baseReq();

  // A stale/unset id on the stored ref must be rewritten even if every name agrees.
  colRef.id = kColId + 1;
  ASSERT_FALSE(matches(colRef, req));
}

TEST(metaColRefIdempotentTest, anyChangedRefNameIsNotIdempotent) {
  SColRef base = baseColRef();

  {
    SColRef      colRef = base;
    SVAlterTbReq req = baseReq();
    req.refDbName = (char *)"idmp_other";
    ASSERT_FALSE(matches(colRef, req));
  }
  {
    SColRef      colRef = base;
    SVAlterTbReq req = baseReq();
    req.refTbName = (char *)"org_ctb_2";
    ASSERT_FALSE(matches(colRef, req));
  }
  {
    SColRef      colRef = base;
    SVAlterTbReq req = baseReq();
    req.refColName = (char *)"voltage";
    ASSERT_FALSE(matches(colRef, req));
  }
  {
    SColRef      colRef = base;
    SVAlterTbReq req = baseReq();
    req.refType = 1;
    ASSERT_FALSE(matches(colRef, req));
  }
}

TEST(metaColRefIdempotentTest, externalSourceAndSchemaParticipate) {
  SColRef colRef = baseColRef();
  colRef.refType = 1;
  tstrncpy(colRef.refSourceName, "pg_src", TSDB_EXT_SOURCE_NAME_LEN);
  tstrncpy(colRef.refSchemaName, "public", TSDB_EXT_SOURCE_SCHEMA_LEN);

  SVAlterTbReq req = baseReq();
  req.refType = 1;
  req.refSourceName = (char *)"pg_src";
  req.refSchemaName = (char *)"public";
  ASSERT_TRUE(matches(colRef, req));

  req.refSourceName = (char *)"pg_src_2";
  ASSERT_FALSE(matches(colRef, req));

  req.refSourceName = (char *)"pg_src";
  req.refSchemaName = (char *)"dbo";
  ASSERT_FALSE(matches(colRef, req));

  // Dropping the schema segment from a ref that has one is a change, not a no-op.
  req.refSchemaName = nullptr;
  ASSERT_FALSE(matches(colRef, req));
}

TEST(metaColRefIdempotentTest, missingRequestNamesAreNeverIdempotent) {
  SColRef base = baseColRef();

  {
    SColRef      colRef = base;
    SVAlterTbReq req = baseReq();
    req.refDbName = nullptr;
    ASSERT_FALSE(matches(colRef, req));
  }
  {
    SColRef      colRef = base;
    SVAlterTbReq req = baseReq();
    req.refTbName = nullptr;
    ASSERT_FALSE(matches(colRef, req));
  }
  {
    SColRef      colRef = base;
    SVAlterTbReq req = baseReq();
    req.refColName = nullptr;
    ASSERT_FALSE(matches(colRef, req));
  }

  SVAlterTbReq req = baseReq();
  ASSERT_FALSE(metaColRefMatchesAlterReq(nullptr, &req, kColId, nullptr, 0));
  ASSERT_FALSE(metaColRefMatchesAlterReq(&base, nullptr, kColId, nullptr, 0));
}

// ---------------------------------------------------------------------------
// filterCondJson takes part in the comparison
// ---------------------------------------------------------------------------

TEST(metaColRefIdempotentTest, identicalFilterConditionIsIdempotent) {
  const char *kFilter = "{\"op\":\">\",\"val\":10}";

  SColRef colRef = baseColRef();
  colRef.filterCondJson = (char *)kFilter;
  colRef.filterCondLen = (int32_t)strlen(kFilter) + 1;

  SVAlterTbReq req = baseReq();
  req.filterCondJson = (char *)kFilter;
  req.filterCondLen = colRef.filterCondLen;

  ASSERT_TRUE(matches(colRef, req));
}

TEST(metaColRefIdempotentTest, changedFilterConditionIsNotIdempotent) {
  const char *kFilter = "{\"op\":\">\",\"val\":10}";
  const char *kOther = "{\"op\":\">\",\"val\":20}";

  SColRef colRef = baseColRef();
  colRef.filterCondJson = (char *)kFilter;
  colRef.filterCondLen = (int32_t)strlen(kFilter) + 1;

  // Same length, different payload: a length-only check would wrongly call this a no-op.
  ASSERT_EQ(strlen(kFilter), strlen(kOther));
  SVAlterTbReq req = baseReq();
  req.filterCondJson = (char *)kOther;
  req.filterCondLen = colRef.filterCondLen;
  ASSERT_FALSE(matches(colRef, req));

  // Adding a filter to an unfiltered ref, and dropping it again, are both real changes.
  SColRef unfiltered = baseColRef();
  ASSERT_FALSE(matches(unfiltered, req));

  SVAlterTbReq noFilter = baseReq();
  ASSERT_FALSE(matches(colRef, noFilter));
}

// ---------------------------------------------------------------------------
// metaResolveColRefTagCond + tagCondJson in the comparison
// ---------------------------------------------------------------------------

TEST(metaColRefIdempotentTest, tagCondOnlyResolvedForExternalRefs) {
  const char  *kTagCond = "{\"tag\":\"line\",\"eq\":\"L1\"}";
  SSeriesEntry series[] = {makeSeries("s1", "pg_src", "idmp", "org_ctb_1", kTagCond)};

  SMetaEntry entry = {};
  entry.series.nSeries = 1;
  entry.series.pSeries = series;

  const char *tagCondJson = (const char *)0x1;
  int32_t     tagCondLen = -1;

  // refType 0 (local ref): no series lookup at all, outputs are cleared.
  SVAlterTbReq localReq = baseReq();
  metaResolveColRefTagCond(&entry, &localReq, &tagCondJson, &tagCondLen);
  ASSERT_TRUE(tagCondJson == nullptr);
  ASSERT_EQ(0, tagCondLen);

  // refType 1 with a matching series: the entry's own buffer is handed back, not a copy.
  SVAlterTbReq extReq = baseReq();
  extReq.refType = 1;
  extReq.refSourceName = (char *)"pg_src";
  metaResolveColRefTagCond(&entry, &extReq, &tagCondJson, &tagCondLen);
  ASSERT_EQ((const char *)series[0].tagCondJson, tagCondJson);
  ASSERT_EQ(series[0].tagCondLen, tagCondLen);
}

TEST(metaColRefIdempotentTest, tagCondMatchesSeriesByAliasOrByPathCaseInsensitively) {
  const char  *kAliasCond = "{\"tag\":\"alias\"}";
  const char  *kPathCond = "{\"tag\":\"path\"}";
  SSeriesEntry series[] = {makeSeries("sAlias", "other_src", "other_db", "other_tb", kAliasCond),
                           makeSeries("sPath", "pg_src", "idmp", "org_ctb_1", kPathCond)};

  SMetaEntry entry = {};
  entry.series.nSeries = 2;
  entry.series.pSeries = series;

  const char *tagCondJson = nullptr;
  int32_t     tagCondLen = 0;

  // An explicit alias wins over the source/db/measurement path.
  SVAlterTbReq aliasReq = baseReq();
  aliasReq.refType = 1;
  aliasReq.refSourceName = (char *)"pg_src";
  aliasReq.seriesAlias = (char *)"salias";  // alias match is case-insensitive
  metaResolveColRefTagCond(&entry, &aliasReq, &tagCondJson, &tagCondLen);
  ASSERT_EQ((const char *)series[0].tagCondJson, tagCondJson);

  // Without an alias, the source/db/measurement triple selects the series, also case-insensitively.
  SVAlterTbReq pathReq = baseReq();
  pathReq.refType = 1;
  pathReq.refSourceName = (char *)"PG_SRC";
  pathReq.refDbName = (char *)"IDMP";
  pathReq.refTbName = (char *)"ORG_CTB_1";
  metaResolveColRefTagCond(&entry, &pathReq, &tagCondJson, &tagCondLen);
  ASSERT_EQ((const char *)series[1].tagCondJson, tagCondJson);

  // No series matches: nothing to install.
  SVAlterTbReq missReq = baseReq();
  missReq.refType = 1;
  missReq.refSourceName = (char *)"pg_src";
  missReq.refTbName = (char *)"org_ctb_9";
  metaResolveColRefTagCond(&entry, &missReq, &tagCondJson, &tagCondLen);
  ASSERT_TRUE(tagCondJson == nullptr);
  ASSERT_EQ(0, tagCondLen);
}

// metaAddVNtbColumn (the ADD COLUMN ... WITH REF path) resolves the series tag condition with the
// same helper. If the two ever diverged, a column created there would store a tag condition the
// ALTER path does not reproduce, and the first IDMP rebind of it would be misjudged as a real
// change - the exact churn this guard exists to remove. Pin the shared contract.
TEST(metaColRefIdempotentTest, createPathResolutionAgreesWithTheAlterGuard) {
  const char  *kTagCond = "{\"tag\":\"line\",\"eq\":\"L1\"}";
  SSeriesEntry series[] = {makeSeries("s1", "pg_src", "idmp", "org_ctb_1", kTagCond)};

  SMetaEntry entry = {};
  entry.series.nSeries = 1;
  entry.series.pSeries = series;

  SVAlterTbReq req = baseReq();
  req.refType = 1;
  req.refSourceName = (char *)"pg_src";

  // What metaAddVNtbColumn stores when it creates the column.
  const char *pCreateTagCondJson = NULL;
  int32_t     createTagCondLen = 0;
  metaResolveColRefTagCond(&entry, &req, &pCreateTagCondJson, &createTagCondLen);
  ASSERT_TRUE(pCreateTagCondJson != nullptr);

  SColRef created = baseColRef();
  created.refType = 1;
  tstrncpy(created.refSourceName, "pg_src", TSDB_EXT_SOURCE_NAME_LEN);
  created.tagCondJson = (char *)pCreateTagCondJson;
  created.tagCondLen = createTagCondLen;

  // The very first rebind of that column must already be a no-op.
  const char *pAlterTagCondJson = NULL;
  int32_t     alterTagCondLen = 0;
  metaResolveColRefTagCond(&entry, &req, &pAlterTagCondJson, &alterTagCondLen);
  ASSERT_EQ(pCreateTagCondJson, pAlterTagCondJson);
  ASSERT_EQ(createTagCondLen, alterTagCondLen);
  ASSERT_TRUE(metaColRefMatchesAlterReq(&created, &req, kColId, pAlterTagCondJson, alterTagCondLen));
}

TEST(metaColRefIdempotentTest, identicalTagCondIsIdempotentAndChangedOneIsNot) {
  const char *kTagCond = "{\"tag\":\"line\",\"eq\":\"L1\"}";
  const char *kOtherCond = "{\"tag\":\"line\",\"eq\":\"L2\"}";

  SColRef colRef = baseColRef();
  colRef.refType = 1;
  tstrncpy(colRef.refSourceName, "pg_src", TSDB_EXT_SOURCE_NAME_LEN);
  colRef.tagCondJson = (char *)kTagCond;
  colRef.tagCondLen = (int32_t)strlen(kTagCond) + 1;

  SVAlterTbReq req = baseReq();
  req.refType = 1;
  req.refSourceName = (char *)"pg_src";

  ASSERT_TRUE(matches(colRef, req, kTagCond, colRef.tagCondLen));

  // Same length, different condition: the series this column binds to has been re-tagged.
  ASSERT_EQ(strlen(kTagCond), strlen(kOtherCond));
  ASSERT_FALSE(matches(colRef, req, kOtherCond, colRef.tagCondLen));

  // The series lost its tag condition - the stored one must be cleared, so not a no-op.
  ASSERT_FALSE(matches(colRef, req, nullptr, 0));

  // ... and the mirror case: a ref with no stored condition gaining one.
  SColRef untagged = baseColRef();
  untagged.refType = 1;
  tstrncpy(untagged.refSourceName, "pg_src", TSDB_EXT_SOURCE_NAME_LEN);
  ASSERT_FALSE(matches(untagged, req, kTagCond, (int32_t)strlen(kTagCond) + 1));
  ASSERT_TRUE(matches(untagged, req, nullptr, 0));
}

// ---------------------------------------------------------------------------
// The end-to-end decision an IDMP rebind round makes
// ---------------------------------------------------------------------------

TEST(metaColRefIdempotentTest, repeatedExternalRebindResolvesToTheSameDecision) {
  const char  *kTagCond = "{\"tag\":\"line\",\"eq\":\"L1\"}";
  SSeriesEntry series[] = {makeSeries("s1", "pg_src", "idmp", "org_ctb_1", kTagCond)};

  SMetaEntry entry = {};
  entry.series.nSeries = 1;
  entry.series.pSeries = series;

  SVAlterTbReq req = baseReq();
  req.refType = 1;
  req.refSourceName = (char *)"pg_src";

  // First bind: the column has no ref yet, so the ALTER must be applied.
  SColRef colRef = baseColRef();
  colRef.hasRef = false;

  const char *tagCondJson = nullptr;
  int32_t     tagCondLen = 0;
  metaResolveColRefTagCond(&entry, &req, &tagCondJson, &tagCondLen);
  ASSERT_FALSE(metaColRefMatchesAlterReq(&colRef, &req, kColId, tagCondJson, tagCondLen));

  // Apply it the way metaAlterTableColumnRef does.
  colRef.hasRef = true;
  colRef.refType = req.refType;
  tstrncpy(colRef.refSourceName, req.refSourceName, TSDB_EXT_SOURCE_NAME_LEN);
  colRef.refSchemaName[0] = '\0';
  tstrncpy(colRef.refDbName, req.refDbName, TSDB_DB_NAME_LEN);
  tstrncpy(colRef.refTableName, req.refTbName, TSDB_TABLE_NAME_LEN);
  tstrncpy(colRef.refColName, req.refColName, TSDB_COL_NAME_LEN);
  colRef.tagCondJson = (char *)tagCondJson;
  colRef.tagCondLen = tagCondLen;

  // Second round of the 6-hour rebind, byte-identical request: now a no-op.
  tagCondJson = nullptr;
  tagCondLen = 0;
  metaResolveColRefTagCond(&entry, &req, &tagCondJson, &tagCondLen);
  ASSERT_TRUE(metaColRefMatchesAlterReq(&colRef, &req, kColId, tagCondJson, tagCondLen));

  // A genuine re-point still goes through.
  req.refColName = (char *)"voltage";
  metaResolveColRefTagCond(&entry, &req, &tagCondJson, &tagCondLen);
  ASSERT_FALSE(metaColRefMatchesAlterReq(&colRef, &req, kColId, tagCondJson, tagCondLen));
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
