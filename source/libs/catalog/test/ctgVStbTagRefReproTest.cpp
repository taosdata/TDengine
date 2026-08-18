/*
 * Regression test for TD-7050063599.
 *
 * ctgBuildVStbFirstLayerRefs() merges each virtual child table's tag-ref info
 * into a single per-STB SRefColInfo array (pTagRefCols), keyed by tag colId.
 * Children of the same virtual STB can have different LIVE tag-ref sets: an
 * `ALTER VTABLE ... SET TAG <col> = <literal>` converts that one child's tag
 * from a ref to a literal, shrinking its numOfTagRefs while other children
 * keep the full set. The merge must not assume every child reports the same
 * count/shape - otherwise a tag column's ref can be silently dropped
 * depending on which child happens to be processed first (which itself
 * depends on inter-vgroup response arrival order, not something callers
 * control), and a later lookup of that column via
 * ctgSetResolvedVStbTagRef() fails with TSDB_CODE_CTG_INTERNAL_ERROR even
 * though the ref genuinely exists on another child.
 *
 * These tests drive the two (otherwise-static) functions directly - via the
 * ctgdTest* wrappers exposed under BUILD_TEST - with synthetic "vgroup
 * responses" so the merge order dependency is reproduced deterministically,
 * without needing a live cluster or real vgroup timing.
 */

#include <gtest/gtest.h>

#include "catalog.h"
#include "catalogInt.h"
#include "taoserror.h"

extern "C" int32_t ctgdTestBuildVStbFirstLayerRefs(SArray* pSubTablesList, SArray** ppLayerRefs, SHashObj** ppRefDbs,
                                                    SHashObj** ppRefExtSources, int32_t* pNumOfColRefs,
                                                    SRefColInfo** ppColRefCols, int32_t* pNumOfTagRefs,
                                                    SRefColInfo** ppTagRefCols);

extern "C" int32_t ctgdTestSetResolvedVStbTagRef(int32_t numOfTagRefs, SRefColInfo* pTagRefCols, col_id_t rootColId,
                                                  const char* pDbName, const char* pTbName, const char* pColName,
                                                  const char* pSourceName);

namespace {

const int16_t REF_CITY_COLID = 2;  // tag col ids on vstb: name=1, ref_city=2, ref_code=3
const int16_t REF_CODE_COLID = 3;

SRefColInfo makeRef(int16_t colId, const char* db, const char* tb, const char* col) {
  SRefColInfo r = {0};
  r.colId = colId;
  snprintf(r.refDbName, sizeof(r.refDbName), "%s", db);
  snprintf(r.refTableName, sizeof(r.refTableName), "%s", tb);
  snprintf(r.refColName, sizeof(r.refColName), "%s", col);
  return r;
}

// One vgroup response containing a single child table with the given tagRefs.
SVSubTablesRsp makeVgRsp(int32_t vgId, uint64_t uid, SRefColInfo* tagRefs, int32_t numTagRefs) {
  SVSubTablesRsp rsp = {0};
  rsp.vgId = vgId;
  rsp.pTables = taosArrayInit(1, sizeof(SVCTableRefCols*));

  int32_t totalRefs = numTagRefs;
  SVCTableRefCols* pTb =
      (SVCTableRefCols*)taosMemoryCalloc(1, sizeof(SVCTableRefCols) + totalRefs * sizeof(SRefColInfo));
  pTb->uid = uid;
  pTb->numOfColRefs = 0;
  pTb->refCols = NULL;
  pTb->numOfTagRefs = numTagRefs;
  if (numTagRefs > 0) {
    pTb->tagRefCols = (SRefColInfo*)(pTb + 1);
    memcpy(pTb->tagRefCols, tagRefs, numTagRefs * sizeof(SRefColInfo));
  } else {
    pTb->tagRefCols = NULL;
  }

  taosArrayPush(rsp.pTables, &pTb);
  return rsp;
}

void runMergeAndResolve(bool literalChildFirst, int32_t* pCode, int32_t* pNumOfTagRefs) {
  SRefColInfo litChildRefs[1] = {makeRef(REF_CITY_COLID, "d0", "src_1", "city")};
  SRefColInfo refChildRefs[2] = {makeRef(REF_CITY_COLID, "d0", "src_0", "city"),
                                  makeRef(REF_CODE_COLID, "d0", "src_0", "code")};

  SVSubTablesRsp litRsp = makeVgRsp(1, 1001, litChildRefs, 1);  // vc_1/vc_3-like: ref_code now literal
  SVSubTablesRsp refRsp = makeVgRsp(2, 1002, refChildRefs, 2);  // vc_0/vc_2-like: both still refs

  SArray* pSubTablesList = taosArrayInit(2, sizeof(SVSubTablesRsp));
  if (literalChildFirst) {
    taosArrayPush(pSubTablesList, &litRsp);
    taosArrayPush(pSubTablesList, &refRsp);
  } else {
    taosArrayPush(pSubTablesList, &refRsp);
    taosArrayPush(pSubTablesList, &litRsp);
  }

  SArray*      pLayerRefs = NULL;
  SHashObj*    pRefDbs = NULL;
  SHashObj*    pRefExtSources = NULL;
  int32_t      numOfColRefs = 0;
  SRefColInfo* pColRefCols = NULL;
  int32_t      numOfTagRefs = 0;
  SRefColInfo* pTagRefCols = NULL;

  int32_t code = ctgdTestBuildVStbFirstLayerRefs(pSubTablesList, &pLayerRefs, &pRefDbs, &pRefExtSources,
                                                  &numOfColRefs, &pColRefCols, &numOfTagRefs, &pTagRefCols);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  // ref_code must always end up with a slot regardless of arrival order.
  code = ctgdTestSetResolvedVStbTagRef(numOfTagRefs, pTagRefCols, REF_CODE_COLID, "d0", "src_0", "code", NULL);

  *pCode = code;
  *pNumOfTagRefs = numOfTagRefs;

  taosArrayDestroy(pLayerRefs);
  taosHashCleanup(pRefDbs);
  taosHashCleanup(pRefExtSources);
  taosMemoryFree(pColRefCols);
  taosMemoryFree(pTagRefCols);
  taosArrayDestroyEx(pSubTablesList, tDestroySVSubTablesRsp);
}

}  // namespace

// Literal-only child (numOfTagRefs=1) processed before the full-ref child
// (numOfTagRefs=2): this is the ordering the bug depended on.
TEST(ctgVStbTagRefMerge, literalChildFirst_refCodeStillResolves) {
  int32_t code = 0;
  int32_t numOfTagRefs = 0;
  runMergeAndResolve(/*literalChildFirst=*/true, &code, &numOfTagRefs);

  EXPECT_EQ(numOfTagRefs, 2) << "both ref_city and ref_code must be present regardless of processing order";
  EXPECT_EQ(code, TSDB_CODE_SUCCESS) << "ref_code lookup must not fail just because a literal-only child arrived first";
}

// Full-ref child processed before the literal-only child: this ordering
// never triggered the bug, kept here so both orderings are covered.
TEST(ctgVStbTagRefMerge, refChildFirst_refCodeResolves) {
  int32_t code = 0;
  int32_t numOfTagRefs = 0;
  runMergeAndResolve(/*literalChildFirst=*/false, &code, &numOfTagRefs);

  EXPECT_EQ(numOfTagRefs, 2);
  EXPECT_EQ(code, TSDB_CODE_SUCCESS);
}
