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

#ifdef LINUX

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"

#include <vnodeInt.h>
#include "tsdbFS2.h"
#include "tsdbFSet2.h"

#pragma GCC diagnostic pop

// Required stub for builds that reference this symbol via dmRepair linkage.
extern "C" SDmNotifyHandle dmNotifyHdl = {};

// ---------------------------------------------------------------------------
// Test fixture
// ---------------------------------------------------------------------------
//
// Regression test for the ASAN-reported memory leak in tsdbFSEditBegin.
//
// Root cause: when edit_fs (tsdbFSDupState) succeeds but save_fs fails, the
// error path in tsdbFSEditBegin previously did not:
//   1. Clear fSetArrTmp  -> leaked every STFileSet* copy allocated by
//      tsdbFSDupState (ultimately traced to taosMemMalloc in tsdbTFileObjInit).
//   2. Post canEdit back -> any subsequent tsdbFSEditBegin call deadlocked.
//
// The test triggers the failure by pointing the tsdb path to a directory that
// does not exist, so taosWriteCfgFile (called from save_fs) returns an error.
//
class TsdbFSEditBeginTest : public ::testing::Test {
 protected:
  SVnode        vnode_{};
  STsdb         tsdb_{};
  STFileSystem  fs_{};

  void SetUp() override {
    // pTfs = NULL + mounted = false -> vnodeGetPrimaryDir just copies path as-is.
    // Use a path whose tsdb subdir is guaranteed not to exist.
    vnode_.path    = const_cast<char *>("/nonexistent_vnode_base_tsdbFSEditTest");
    vnode_.pTfs    = nullptr;
    vnode_.mounted = false;

    tsdb_.pVnode = &vnode_;
    // name is used as the final path component; the combined path will not
    // exist on any real filesystem.
    snprintf(tsdb_.name, sizeof(tsdb_.name), "tsdb");

    fs_.tsdb = &tsdb_;
    ASSERT_EQ(tsem_init(&fs_.canEdit, 0, 1), 0);
    TARRAY2_INIT(fs_.fSetArr);
    TARRAY2_INIT(fs_.fSetArrTmp);
  }

  void TearDown() override {
    TARRAY2_DESTROY(fs_.fSetArr,    tsdbTFileSetClear);
    TARRAY2_DESTROY(fs_.fSetArrTmp, tsdbTFileSetClear);
    (void)tsem_destroy(&fs_.canEdit);
  }
};

// ---------------------------------------------------------------------------
// Test: failed tsdbFSEditBegin must clear fSetArrTmp and release semaphore
// ---------------------------------------------------------------------------
//
// Scenario:
//   - fSetArr has one STFileSet (no files, no stt levels).
//   - tsdbFSDupState copies it into fSetArrTmp (allocates an STFileSet*).
//   - save_fs fails because the target directory does not exist.
//   - tsdbFSEditBegin must:
//       (a) clear fSetArrTmp (freeing the copied STFileSet*)
//       (b) post canEdit so the next caller is not blocked forever
//
TEST_F(TsdbFSEditBeginTest, FailedSaveFSClearsTmpAndReleaseSemaphore) {
  // Populate fSetArr so tsdbFSDupState has something to copy.
  STFileSet *fset = nullptr;
  ASSERT_EQ(tsdbTFileSetInit(1, &fset), 0);
  ASSERT_EQ(TARRAY2_SORT_INSERT(fs_.fSetArr, fset, tsdbTFileSetCmprFn), 0);

  TFileOpArray opArr;
  TARRAY2_INIT(&opArr);

  // This call goes through edit_fs (tsdbFSDupState succeeds, fSetArrTmp gets
  // one entry) then hits save_fs which fails because the path does not exist.
  int32_t code = tsdbFSEditBegin(&fs_, &opArr, TSDB_FEDIT_COMMIT);

  TARRAY2_DESTROY(&opArr, nullptr);

  // (a) The call must have failed.
  ASSERT_NE(0, code);

  // (b) fSetArrTmp must be empty — no leaked STFileSet* copies.
  ASSERT_EQ(0, TARRAY2_SIZE(fs_.fSetArrTmp));

  // (c) canEdit semaphore must be available again (posted back on failure).
  //     tsem_timewait returns 0 on success (decrements the count).
  ASSERT_EQ(0, tsem_timewait(&fs_.canEdit, 200));

  // Restore semaphore count so TearDown's tsem_destroy works cleanly.
  (void)tsem_post(&fs_.canEdit);
}

// ---------------------------------------------------------------------------
// Test: a second tsdbFSEditBegin call is not blocked after a prior failure
// ---------------------------------------------------------------------------
//
// Without the semaphore fix, canEdit would be stuck at 0 after the first
// failure and the second call would block forever (or deadlock in tests).
//
TEST_F(TsdbFSEditBeginTest, SubsequentEditBeginIsNotBlockedAfterFailure) {
  TFileOpArray opArr;
  TARRAY2_INIT(&opArr);

  // First call — fails (no directory).
  int32_t code1 = tsdbFSEditBegin(&fs_, &opArr, TSDB_FEDIT_COMMIT);
  ASSERT_NE(0, code1);

  // Second call — must also return promptly (not block on the semaphore).
  int32_t code2 = tsdbFSEditBegin(&fs_, &opArr, TSDB_FEDIT_COMMIT);
  ASSERT_NE(0, code2);

  TARRAY2_DESTROY(&opArr, nullptr);

  // Semaphore must still be available after both failures.
  ASSERT_EQ(0, tsem_timewait(&fs_.canEdit, 200));
  (void)tsem_post(&fs_.canEdit);
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else

int main(int argc, char **argv) { return 0; }

#endif  // LINUX
