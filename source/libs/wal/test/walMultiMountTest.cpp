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
#include <cstring>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "tfs.h"
#include "tglobal.h"
#include "walInt.h"

static const char* kRanStr = "wmmt02tcp";

// Multi-mount-point write test fixture: builds a real 3-disk level-0 tfs instance (same
// pattern as TfsTest::05_MultiDisk in source/libs/tfs/test/tfsTest.cpp), opens a WAL
// against the primary mount point with pTfs/relDir already set on SWalCfg (mirroring
// vnodeOpen()'s corrected sequence: bind BEFORE walOpen(), not after -- walOpen() runs a
// one-time repair pass over historical segments that needs the binding in place already,
// see the SWalCfg.pTfs comment in wal.h), and forces every write to roll into a new
// segment so distribution across disks can be observed quickly.
class WalMultiMountEnv : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    int32_t code = walInit(NULL);
    ASSERT_EQ(code, 0);
  }

  static void TearDownTestCase() { walCleanUp(); }

  void SetUp() override {
    buildTfs();
    pWal = openBoundWal();
    ASSERT_NE(pWal, nullptr);
  }

  // Build an SWalCfg with pTfs/relDir already bound and open a WAL against pathName.
  // Reused by tests that close and reopen the WAL to mimic a taosd restart.
  SWal* openBoundWal() {
    SWalCfg cfg = {0};
    cfg.rollPeriod = -1;
    cfg.segSize = -1;
    cfg.committed = -1;
    // retentionPeriod == 0 makes walEndSnapshot eligible to recycle every segment below
    // the snapshot boundary immediately; -1 (the default here) disables recycling
    // entirely. The deletion-path tests flip this to 0 and reopen.
    cfg.retentionPeriod = retentionPeriod;
    cfg.retentionSize = 0;
    // Off by default, as for a non-vnode WAL. vnodeOpen() forces this on (see
    // vnodeOpen.c), and it is what gates walTxnFilesTrim; the .txn trim test turns it on.
    cfg.enableTxnFile = enableTxnFile;
    cfg.vgId = 0;
    cfg.level = TAOS_WAL_FSYNC;
    cfg.pTfs = pTfs;
    tstrncpy(cfg.relDir, relDir, sizeof(cfg.relDir));
    return walOpen(pathName, &cfg);
  }

  void TearDown() override {
    if (pWal != NULL) {
      walClose(pWal);
      pWal = NULL;
    }
    if (pTfs != NULL) {
      tfsClose(pTfs);
      pTfs = NULL;
    }
  }

  // Collect the basenames of every file with the given suffix across all three mount
  // points, keyed by the mount index that holds it. The deletion-path tests need this
  // because checking only pathName (the primary mount) is exactly the blind spot that
  // let the leak through: a stale segment on mount 1 or 2 is invisible from there.
  std::map<std::string, int> collectSegFiles(const char* suffix) {
    std::map<std::string, int> found;
    for (int i = 0; i < 3; i++) {
      char dir[TSDB_FILENAME_LEN * 2];
      snprintf(dir, sizeof(dir), "%s%s%s", mountDirs[i], TD_DIRSEP, relDir);
      TdDirPtr pDir = taosOpenDir(dir);
      if (pDir == NULL) continue;
      TdDirEntryPtr pEntry;
      while ((pEntry = taosReadDir(pDir)) != NULL) {
        if (taosDirEntryIsDir(pEntry)) continue;
        std::string name(taosGetDirEntryName(pEntry));
        size_t      pos = name.rfind(suffix);
        if (pos != std::string::npos && pos + strlen(suffix) == name.size()) {
          found[name] = i;
        }
      }
      (void)taosCloseDir(&pDir);
    }
    return found;
  }

  // Append one record at version `ver` and force a roll, mirroring the write+roll
  // pattern used by WalRetentionEnv's corruptedDirDelete* tests in walMetaTest.cpp.
  void appendAndRoll(int64_t ver) {
    char newStr[64];
    snprintf(newStr, sizeof(newStr), "%s-%" PRId64, kRanStr, ver);
    int32_t code = walAppendLog(pWal, ver, 0, syncMeta, newStr, (int32_t)strlen(newStr), 0, NULL);
    ASSERT_EQ(code, 0);
    code = walRollImpl(pWal);
    ASSERT_EQ(code, 0);
  }

  void buildTfs() {
    for (int i = 0; i < 3; i++) {
      snprintf(mountDirs[i], sizeof(mountDirs[i]), TD_TMP_DIR_PATH "walMultiMount%d", i);
      taosRemoveDir(mountDirs[i]);
      taosMkDir(mountDirs[i]);
    }

    SDiskCfg dCfg[3] = {0};
    for (int i = 0; i < 3; i++) {
      tstrncpy(dCfg[i].dir, mountDirs[i], TSDB_FILENAME_LEN);
      dCfg[i].level = 0;
      dCfg[i].primary = (i == 0) ? 1 : 0;
      dCfg[i].disable = 0;
    }

    pTfs = NULL;
    int32_t code = tfsOpen(dCfg, 3, &pTfs);
    ASSERT_EQ(code, 0);
    ASSERT_NE(pTfs, nullptr);

    snprintf(pathName, sizeof(pathName), "%s%s%s", mountDirs[0], TD_DIRSEP, relDir);
  }

  STfs*       pTfs = NULL;
  SWal*       pWal = NULL;
  int32_t     retentionPeriod = -1;
  int8_t      enableTxnFile = 0;
  char        mountDirs[3][TSDB_FILENAME_LEN] = {{0}};
  char        pathName[TSDB_FILENAME_LEN * 2] = {0};
  const char* relDir = "wal";
  SWalSyncInfo syncMeta = {0};
};

// Case 1: with tfs bound and walMultiMountEnable on (the default), consecutive rolls
// must spread segments across more than just the primary disk, and every segment
// recorded in fileInfoSet must physically exist under the disk tfsGetDiskPath resolves
// its diskId to.
TEST_F(WalMultiMountEnv, segmentsSpreadAcrossDisks) {
  const int kRolls = 9;
  for (int64_t ver = 0; ver < kRolls; ver++) {
    appendAndRoll(ver);
  }

  int32_t sz = (int32_t)taosArrayGetSize(pWal->fileInfoSet);
  ASSERT_GE(sz, kRolls);

  std::set<int32_t> usedDiskIds;
  for (int32_t i = 0; i < sz; i++) {
    SWalFileInfo* pInfo = (SWalFileInfo*)taosArrayGet(pWal->fileInfoSet, i);
    ASSERT_GE(pInfo->diskId.id, 0);
    ASSERT_EQ(pInfo->diskId.level, 0);
    usedDiskIds.insert(pInfo->diskId.id);

    const char* diskPath = tfsGetDiskPath(pTfs, pInfo->diskId);
    ASSERT_NE(diskPath, nullptr);
    char logFile[WAL_FILE_LEN];
    snprintf(logFile, sizeof(logFile), "%s%s%s%s%020" PRId64 ".log", diskPath, TD_DIRSEP, relDir, TD_DIRSEP,
             pInfo->firstVer);
    ASSERT_TRUE(taosCheckExistFile(logFile)) << logFile;

    // walBuildLogName (the untouched public-facing name builder) must agree.
    char viaBuilder[WAL_FILE_LEN];
    walBuildLogName(pWal, pInfo->firstVer, viaBuilder);
    ASSERT_STREQ(viaBuilder, logFile);
  }

  // The whole point of the feature: more than one disk actually got used.
  ASSERT_GT((int)usedDiskIds.size(), 1);
}

// Case 2: toggling walMultiMountEnable off mid-run must (a) stop placing *new* segments
// on non-primary disks, while (b) leaving segments already created on other disks intact
// and still resolvable.
TEST_F(WalMultiMountEnv, toggleOffFallsBackToPrimary) {
  bool oldVal = tsWalMultiMountEnable;
  tsWalMultiMountEnable = true;

  int64_t ver = 0;
  for (int i = 0; i < 4; i++) appendAndRoll(ver++);

  int32_t szBeforeToggle = (int32_t)taosArrayGetSize(pWal->fileInfoSet);
  bool    sawNonPrimary = false;
  for (int32_t i = 0; i < szBeforeToggle; i++) {
    SWalFileInfo* pInfo = (SWalFileInfo*)taosArrayGet(pWal->fileInfoSet, i);
    if (pInfo->diskId.id > 0) sawNonPrimary = true;
  }
  ASSERT_TRUE(sawNonPrimary);

  tsWalMultiMountEnable = false;
  for (int i = 0; i < 4; i++) appendAndRoll(ver++);

  int32_t szAfterToggle = (int32_t)taosArrayGetSize(pWal->fileInfoSet);
  for (int32_t i = szBeforeToggle; i < szAfterToggle; i++) {
    SWalFileInfo* pInfo = (SWalFileInfo*)taosArrayGet(pWal->fileInfoSet, i);
    ASSERT_EQ(pInfo->diskId.id, -1);  // sentinel: resolved through pWal->path

    char fnameStr[WAL_FILE_LEN];
    walBuildLogName(pWal, pInfo->firstVer, fnameStr);
    ASSERT_NE(strstr(fnameStr, pathName), nullptr) << fnameStr;
  }

  // Segments created before the toggle remain untouched and readable at their original
  // (possibly non-primary) location.
  for (int32_t i = 0; i < szBeforeToggle; i++) {
    SWalFileInfo* pInfo = (SWalFileInfo*)taosArrayGet(pWal->fileInfoSet, i);
    char fnameStr[WAL_FILE_LEN];
    walBuildLogName(pWal, pInfo->firstVer, fnameStr);
    ASSERT_TRUE(taosCheckExistFile(fnameStr)) << fnameStr;
  }

  tsWalMultiMountEnable = oldVal;
}

// Case 3: closing and reopening the WAL (mirroring a taosd restart) must restore every
// segment's diskId from the persisted meta file and keep resolving each historical
// segment to the disk it actually lives on -- meta stays centralized, segment bodies
// stay wherever they were placed.
TEST_F(WalMultiMountEnv, reopenResolvesOldSegmentsAcrossDisks) {
  int64_t ver = 0;
  for (int i = 0; i < 6; i++) appendAndRoll(ver++);

  int32_t szBefore = (int32_t)taosArrayGetSize(pWal->fileInfoSet);
  ASSERT_GT(szBefore, 0);

  std::map<int64_t, SDiskID> expected;
  bool                       hadNonPrimary = false;
  for (int32_t i = 0; i < szBefore; i++) {
    SWalFileInfo* pInfo = (SWalFileInfo*)taosArrayGet(pWal->fileInfoSet, i);
    expected[pInfo->firstVer] = pInfo->diskId;
    if (pInfo->diskId.id > 0) hadNonPrimary = true;
  }
  ASSERT_TRUE(hadNonPrimary);

  walClose(pWal);
  pWal = NULL;

  // Reopen exactly like the corrected vnodeOpen() does on restart: pTfs/relDir are set
  // on SWalCfg *before* walOpen() is called, so the one-time repair pass walOpen() runs
  // internally (walCheckAndRepairMeta/walCheckAndRepairIdx) already knows to look on every
  // mount point. This is the regression test for the bug where binding tfs only *after*
  // walOpen() returned caused that repair pass to see every non-primary segment as
  // missing and silently drop it from fileInfoSet (and then persist that loss via
  // walSaveMeta) -- if that regresses, either walOpen() below returns NULL or szAfter
  // != szBefore.
  pWal = openBoundWal();
  ASSERT_NE(pWal, nullptr);

  int32_t szAfter = (int32_t)taosArrayGetSize(pWal->fileInfoSet);
  ASSERT_EQ(szAfter, szBefore);

  for (int32_t i = 0; i < szAfter; i++) {
    SWalFileInfo* pInfo = (SWalFileInfo*)taosArrayGet(pWal->fileInfoSet, i);
    auto          it = expected.find(pInfo->firstVer);
    ASSERT_NE(it, expected.end());
    ASSERT_EQ(pInfo->diskId.level, it->second.level);
    ASSERT_EQ(pInfo->diskId.id, it->second.id);

    char fnameStr[WAL_FILE_LEN];
    walBuildLogName(pWal, pInfo->firstVer, fnameStr);
    ASSERT_TRUE(taosCheckExistFile(fnameStr)) << fnameStr;
  }
}

// Case 4: retention must physically unlink segments that live on non-primary mount
// points. Regression test for the bug where walEndSnapshot popped the entries off
// fileInfoSet *before* building the file names: walBuildLogName's reverse lookup then
// found nothing and silently fell back to pWal->path, so every unlink targeted the
// primary mount. ENOENT is explicitly tolerated there, so the failure was completely
// silent -- "wal log files recycled" was still logged. Assert on the filesystem across
// all three mounts rather than on fileInfoSet, which was always correct.
TEST_F(WalMultiMountEnv, endSnapshotRemovesSegmentsOnAllDisks) {
  // Reopen with recycling enabled; SetUp's WAL has it disabled.
  walClose(pWal);
  retentionPeriod = 0;
  pWal = openBoundWal();
  ASSERT_NE(pWal, nullptr);

  int64_t ver = 0;
  for (int i = 0; i < 8; i++) appendAndRoll(ver++);

  // Confirm the precondition the bug needed: segments exist off the primary mount.
  auto before = collectSegFiles(".log");
  bool sawNonPrimary = false;
  for (const auto& kv : before) {
    if (kv.second != 0) sawNonPrimary = true;
  }
  ASSERT_TRUE(sawNonPrimary) << "multi-mount placement did not spread; test is vacuous";

  // Retire everything except the current write segment.
  SWalFileInfo* pLast = (SWalFileInfo*)taosArrayGetLast(pWal->fileInfoSet);
  int64_t       keepFrom = pLast->firstVer;

  for (int64_t v = 0; v < ver; v++) {
    ASSERT_EQ(walCommit(pWal, v), 0);
  }
  walApplyVer(pWal, ver - 1);
  ASSERT_EQ(walBeginSnapshot(pWal, ver - 1, 0), 0);
  ASSERT_EQ(walEndSnapshot(pWal, false), 0);

  // Every retired segment must be gone from whichever mount it lived on. Before the fix
  // the non-primary ones survived here.
  auto after = collectSegFiles(".log");
  auto afterIdx = collectSegFiles(".idx");
  for (const auto& kv : before) {
    int64_t firstVer = -1;
    ASSERT_EQ(sscanf(kv.first.c_str(), "%" PRId64 ".log", &firstVer), 1);
    if (firstVer >= keepFrom) continue;
    EXPECT_EQ(after.count(kv.first), 0u) << "stale .log left on mount " << kv.second << ": " << kv.first;

    char idxName[64];
    snprintf(idxName, sizeof(idxName), "%020" PRId64 ".idx", firstVer);
    EXPECT_EQ(afterIdx.count(std::string(idxName)), 0u) << "stale .idx left behind: " << idxName;
  }

  // And the retained segment is still intact and resolvable.
  char fnameStr[WAL_FILE_LEN];
  walBuildLogName(pWal, keepFrom, fnameStr);
  ASSERT_TRUE(taosCheckExistFile(fnameStr)) << fnameStr;
}

// Case 6: walRollback must unlink the discarded segments on whichever mount they live
// on. The bug here was subtler than Case 4's: taosArrayPop only decrements the array
// size and returns the element in place, so the popped segment is already invisible to
// walResolveFileDir (which iterates up to taosArrayGetSize) and the name fell back to
// the primary mount. A rolled-back segment surviving on another mount gets rescanned
// into fileInfoSet on the next restart -- i.e. discarded Raft entries coming back.
TEST_F(WalMultiMountEnv, rollbackRemovesSegmentsOnAllDisks) {
  int64_t ver = 0;
  for (int i = 0; i < 8; i++) appendAndRoll(ver++);

  auto before = collectSegFiles(".log");
  bool sawNonPrimary = false;
  for (const auto& kv : before) {
    if (kv.second != 0) sawNonPrimary = true;
  }
  ASSERT_TRUE(sawNonPrimary) << "multi-mount placement did not spread; test is vacuous";

  // Roll back into the middle of the segment list, leaving several segments to discard.
  // walRollback requires ver > commitVer && ver > snapshotVer, so commit nothing here.
  int32_t       sz = (int32_t)taosArrayGetSize(pWal->fileInfoSet);
  ASSERT_GE(sz, 4);
  SWalFileInfo* pTarget = (SWalFileInfo*)taosArrayGet(pWal->fileInfoSet, sz / 2);
  int64_t       rollbackTo = pTarget->firstVer;

  ASSERT_EQ(walRollback(pWal, rollbackTo), 0);

  // Segments at or after the rollback point must be gone from every mount.
  auto after = collectSegFiles(".log");
  auto afterIdx = collectSegFiles(".idx");
  for (const auto& kv : before) {
    int64_t firstVer = -1;
    ASSERT_EQ(sscanf(kv.first.c_str(), "%" PRId64 ".log", &firstVer), 1);
    if (firstVer <= rollbackTo) continue;
    EXPECT_EQ(after.count(kv.first), 0u) << "stale .log left on mount " << kv.second << ": " << kv.first;

    char idxName[64];
    snprintf(idxName, sizeof(idxName), "%020" PRId64 ".idx", firstVer);
    EXPECT_EQ(afterIdx.count(std::string(idxName)), 0u) << "stale .idx left behind: " << idxName;
  }

  // Reopening must not resurrect them either.
  int32_t szAfterRollback = (int32_t)taosArrayGetSize(pWal->fileInfoSet);
  int64_t lastVerAfter = pWal->vers.lastVer;
  walClose(pWal);
  pWal = NULL;
  pWal = openBoundWal();
  ASSERT_NE(pWal, nullptr);
  EXPECT_EQ((int32_t)taosArrayGetSize(pWal->fileInfoSet), szAfterRollback);
  EXPECT_EQ(pWal->vers.lastVer, lastVerAfter);
}

// Case 7: the second half of the walMultiMountEnable toggle requirement. Case 2 covers
// *loading* segments written on either side of the switch; this covers *deleting* them.
// An operator who turns the switch off mid-incident must still get the segments that were
// placed on non-primary mounts while it was on reclaimed by retention -- otherwise the
// disk leak simply freezes in place. This is why walResolveFileDirAt keys off the
// recorded diskId alone and never consults tsWalMultiMountEnable.
TEST_F(WalMultiMountEnv, retentionDeletesSegmentsWrittenBeforeToggleOff) {
  walClose(pWal);
  retentionPeriod = 0;
  pWal = openBoundWal();
  ASSERT_NE(pWal, nullptr);

  bool oldVal = tsWalMultiMountEnable;
  tsWalMultiMountEnable = true;

  int64_t ver = 0;
  for (int i = 0; i < 8; i++) appendAndRoll(ver++);

  // Resolve the non-primary segments' paths now, while their fileInfoSet entries (and so
  // the reverse lookup) are still intact.
  std::vector<std::string> onDiskBeforeToggle;
  int32_t                  szOn = (int32_t)taosArrayGetSize(pWal->fileInfoSet);
  for (int32_t i = 0; i < szOn; i++) {
    SWalFileInfo* pInfo = (SWalFileInfo*)taosArrayGet(pWal->fileInfoSet, i);
    if (pInfo->diskId.id <= 0) continue;
    char fnameStr[WAL_FILE_LEN];
    walBuildLogName(pWal, pInfo->firstVer, fnameStr);
    onDiskBeforeToggle.push_back(std::string(fnameStr));
  }
  ASSERT_FALSE(onDiskBeforeToggle.empty()) << "no segment landed on a non-primary mount; test is vacuous";

  // Flip the switch off and keep writing, exactly as an operator would mid-incident.
  tsWalMultiMountEnable = false;
  for (int i = 0; i < 8; i++) appendAndRoll(ver++);
  tsWalMultiMountEnable = oldVal;

  // Retire everything up to the current write segment.
  SWalFileInfo* pLast = (SWalFileInfo*)taosArrayGetLast(pWal->fileInfoSet);
  int64_t       keepFrom = pLast->firstVer;
  ASSERT_GT(keepFrom, 0);

  for (int64_t v = 0; v < ver; v++) {
    ASSERT_EQ(walCommit(pWal, v), 0);
  }
  walApplyVer(pWal, ver - 1);
  ASSERT_EQ(walBeginSnapshot(pWal, ver - 1, 0), 0);
  ASSERT_EQ(walEndSnapshot(pWal, false), 0);

  for (const auto& f : onDiskBeforeToggle) {
    EXPECT_FALSE(taosCheckExistFile(f.c_str()))
        << "segment written while multi-mount was on survived retention after toggling off: " << f;
  }
}

// Case 8: the upgrade path, and the reason the cleanup has to happen during repair rather
// than at the next retention cycle.
//
// A cluster that ran the buggy build has retired segments still sitting on non-primary
// mounts. Placement rotates across mounts, so the survivors are *not* contiguous -- the
// ones that happened to land on the primary mount really were deleted. On restart
// walScanActualLogFiles walks every mount and finds the survivors again; without the
// purge, walRebuildFileInfoSet reinserts them, walLogEntriesComplete sees the hole, and
// with tsWalDeleteOnCorruption on by default the entire WAL directory is renamed away --
// so walOpen never reaches a retention cycle that could have cleaned them up.
//
// This plants a non-contiguous set of orphans below meta's first index and asserts that
// reopening cleans them up, keeps the live segment, and does not drag firstVer backwards.
TEST_F(WalMultiMountEnv, reopenPurgesNonContiguousOrphansBelowMetaFirstVer) {
  walClose(pWal);
  retentionPeriod = 0;
  pWal = openBoundWal();
  ASSERT_NE(pWal, nullptr);

  int64_t ver = 0;
  for (int i = 0; i < 16; i++) appendAndRoll(ver++);

  int32_t sz = (int32_t)taosArrayGetSize(pWal->fileInfoSet);
  ASSERT_GT(sz, 3);
  SWalFileInfo* pLast = (SWalFileInfo*)taosArrayGetLast(pWal->fileInfoSet);
  int64_t       keepFrom = pLast->firstVer;
  ASSERT_GT(keepFrom, 10) << "need headroom below the retained segment to plant orphans";

  // Retire everything below the last segment, the normal way.
  for (int64_t v = 0; v < keepFrom; v++) {
    ASSERT_EQ(walCommit(pWal, v), 0);
  }
  walApplyVer(pWal, keepFrom - 1);
  ASSERT_EQ(walBeginSnapshot(pWal, keepFrom - 1, 0), 0);
  ASSERT_EQ(walEndSnapshot(pWal, false), 0);
  ASSERT_EQ(pWal->vers.firstVer, keepFrom);

  // The retained segment, which must survive the reopen untouched.
  char liveLog[WAL_FILE_LEN];
  walBuildLogName(pWal, keepFrom, liveLog);
  ASSERT_TRUE(taosCheckExistFile(liveLog)) << liveLog;

  // Plant orphans on the two non-primary mounts, leaving a deliberate hole between them
  // so the resulting version range is discontinuous -- exactly the pre-upgrade shape.
  int64_t                  orphanVers[2] = {keepFrom - 9, keepFrom - 5};
  std::vector<std::string> plantedFiles;
  for (int k = 0; k < 2; k++) {
    char dir[TSDB_FILENAME_LEN * 2];
    snprintf(dir, sizeof(dir), "%s%s%s", mountDirs[k + 1], TD_DIRSEP, relDir);
    ASSERT_EQ(taosMulMkDir(dir), 0);

    for (const char* suffix : {"log", "idx"}) {
      char p[WAL_FILE_LEN];
      snprintf(p, sizeof(p), "%s%s%020" PRId64 ".%s", dir, TD_DIRSEP, orphanVers[k], suffix);
      TdFilePtr fp = taosOpenFile(p, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
      ASSERT_NE(fp, nullptr) << p;
      ASSERT_GT(taosWriteFile(fp, kRanStr, (int32_t)strlen(kRanStr)), 0);
      ASSERT_EQ(taosCloseFile(&fp), 0);
      ASSERT_TRUE(taosCheckExistFile(p)) << "failed to plant orphan " << p;
      plantedFiles.push_back(std::string(p));
    }
  }

  // Restart.
  walClose(pWal);
  pWal = NULL;
  pWal = openBoundWal();
  ASSERT_NE(pWal, nullptr) << "walOpen failed after restart -- the orphans were treated as a gap";

  // Every planted orphan is gone.
  for (const auto& f : plantedFiles) {
    EXPECT_FALSE(taosCheckExistFile(f.c_str())) << "orphan survived the reopen: " << f;
  }

  // The live segment is still there: the WAL directory was not wiped as "corrupted".
  EXPECT_TRUE(taosCheckExistFile(liveLog))
      << "live segment lost -- the WAL directory was most likely renamed away: " << liveLog;

  // firstVer did not regress into the purged range.
  EXPECT_GE(pWal->vers.firstVer, keepFrom)
      << "firstVer regressed to " << pWal->vers.firstVer << ", expected >= " << keepFrom;

  // And the surviving set is contiguous, so the completeness check is satisfied.
  int32_t szAfter = (int32_t)taosArrayGetSize(pWal->fileInfoSet);
  for (int32_t i = 1; i < szAfter; i++) {
    SWalFileInfo* pPrev = (SWalFileInfo*)taosArrayGet(pWal->fileInfoSet, i - 1);
    SWalFileInfo* pCur = (SWalFileInfo*)taosArrayGet(pWal->fileInfoSet, i);
    EXPECT_EQ(pPrev->lastVer + 1, pCur->firstVer)
        << "hole between segments " << pPrev->firstVer << " and " << pCur->firstVer;
  }
}

// Case 10: walTxnFilesTrim must count only real .txn files.
//
// Its directory scan used sscanf("%<PRId64>.txn", &ver) and tested only the return value.
// For "00000000000000000304.log" the "%" PRId64 conversion completes (so sscanf returns 1)
// before the literal "txn" fails against "log" and scanning stops -- a failed literal match
// does not undo an already-completed conversion. Every .log and .idx file in the directory
// was therefore counted as a .txn file, roughly tripling txnCount and clearing Condition B
// ("txnCount > logCount + WAL_TXN_TRIM_EXTRA_FILES") when there was no real excess at all.
//
// Setup: logCount live segments and one .txn per segment except the first. A correct
// txnCount is logCount - 1, far below the logCount + WAL_TXN_TRIM_EXTRA_FILES threshold, so
// a correct trim returns at Condition B without unlinking anything. The miscount reaches
// roughly 3 * logCount, clears the threshold, and proceeds to unlink.
//
// Condition A has to be satisfiable for the miscount to become observable, otherwise the
// loop skips every candidate and the test would pass either way. Nothing is retired here,
// so every .txn is inside the live range and does go through the age guard: it compares the
// createTs of the .log segment with the same firstVer against
// (fileInfoSet[0]->createTs - WAL_TXN_TRIM_EXTRA_HOURS) -- index 0 specifically, not the
// minimum. Segment 0 therefore keeps a current createTs to set a cutoff the rest fall below,
// and every later segment is backdated well past it.
TEST_F(WalMultiMountEnv, txnTrimCountsOnlyTxnFiles) {
  walClose(pWal);
  enableTxnFile = 1;
  pWal = openBoundWal();
  ASSERT_NE(pWal, nullptr);

  const int kRolls = 12;
  int64_t   ver = 0;
  for (int i = 0; i < kRolls; i++) appendAndRoll(ver++);

  int32_t logCount = (int32_t)taosArrayGetSize(pWal->fileInfoSet);
  ASSERT_GE(logCount, kRolls);

  // Build each .txn next to its own segment, via the same diskId-based resolution the trim
  // uses, so a segment on a non-primary mount gets its .txn on that mount too.
  std::vector<std::string> txnFiles;
  for (int32_t i = 1; i < logCount; i++) {
    SWalFileInfo* pInfo = (SWalFileInfo*)taosArrayGet(pWal->fileInfoSet, i);
    char          segDir[WAL_PATH_LEN];
    char          fnameStr[WAL_FILE_LEN];
    walResolveFileDirAt(pWal, pInfo->diskId, segDir, sizeof(segDir));
    walBuildTxnNameAt(segDir, pInfo->firstVer, fnameStr);
    TdFilePtr fp = taosOpenFile(fnameStr, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
    ASSERT_NE(fp, nullptr) << fnameStr;
    ASSERT_GT(taosWriteFile(fp, kRanStr, (int32_t)strlen(kRanStr)), 0);
    ASSERT_EQ(taosCloseFile(&fp), 0);
    txnFiles.push_back(std::string(fnameStr));
  }
  ASSERT_FALSE(txnFiles.empty());

  // createTs is in seconds (walRollFileInfo uses taosGetTimestampSec), same unit as the
  // mtime fallback, so mixing the two in the comparison below is sound.
  int64_t nowSec = taosGetTimestampSec();
  ((SWalFileInfo*)taosArrayGet(pWal->fileInfoSet, 0))->createTs = nowSec;
  for (int32_t i = 1; i < logCount; i++) {
    ((SWalFileInfo*)taosArrayGet(pWal->fileInfoSet, i))->createTs = nowSec - 48 * 3600;
  }

  walTxnFilesTrim(pWal);

  for (const auto& f : txnFiles) {
    EXPECT_TRUE(taosCheckExistFile(f.c_str()))
        << ".txn file was trimmed even though txnCount never exceeded logCount + "
           "WAL_TXN_TRIM_EXTRA_FILES; .log/.idx names are being miscounted as .txn files: "
        << f;
  }

  // Trim touches .txn files only; the segments themselves must be untouched.
  for (int32_t i = 0; i < logCount; i++) {
    SWalFileInfo* pInfo = (SWalFileInfo*)taosArrayGet(pWal->fileInfoSet, i);
    char          segDir[WAL_PATH_LEN];
    char          fnameStr[WAL_FILE_LEN];
    walResolveFileDirAt(pWal, pInfo->diskId, segDir, sizeof(segDir));
    walBuildLogNameAt(segDir, pInfo->firstVer, fnameStr);
    EXPECT_TRUE(taosCheckExistFile(fnameStr)) << fnameStr;
  }
}

// Case 11: retention must actually reclaim the .txn files it orphans.
//
// Reproduces the field state directly: dozens of 0-byte .txn files piled up in a vnode WAL
// while the .log/.idx segments beside them were being recycled normally. Condition A was
// the blocker. It compared each .txn file's timestamp against
// (fileInfoSet[0]->createTs - WAL_TXN_TRIM_EXTRA_HOURS), falling back to the file's own
// mtime for an orphan whose segment was already gone -- but after retention runs, the
// oldest *surviving* segment is the newest thing on disk, so that cutoff sits hours in the
// future relative to every orphan. The first iteration failed A, the `break` ended the
// loop, and nothing was ever removed. With wal_retention_period 0 the condition could
// never be satisfied at all, so the leak was unbounded.
//
// Everything here goes through walEndSnapshot, which calls walTxnFilesTrim itself -- the
// production path, and no second explicit trim to confuse the arithmetic.
TEST_F(WalMultiMountEnv, retentionReclaimsOrphanedTxnFiles) {
  walClose(pWal);
  retentionPeriod = 0;
  enableTxnFile = 1;
  pWal = openBoundWal();
  ASSERT_NE(pWal, nullptr);

  const int kRolls = 20;
  int64_t   ver = 0;
  for (int i = 0; i < kRolls; i++) appendAndRoll(ver++);

  int32_t total = (int32_t)taosArrayGetSize(pWal->fileInfoSet);
  ASSERT_GE(total, kRolls);

  // A .txn beside every segment, on whichever mount that segment lives on -- same spread
  // as the field data, where the orphans were scattered over data3/data4/data5.
  for (int32_t i = 0; i < total; i++) {
    SWalFileInfo* pInfo = (SWalFileInfo*)taosArrayGet(pWal->fileInfoSet, i);
    char          segDir[WAL_PATH_LEN];
    char          fnameStr[WAL_FILE_LEN];
    walResolveFileDirAt(pWal, pInfo->diskId, segDir, sizeof(segDir));
    walBuildTxnNameAt(segDir, pInfo->firstVer, fnameStr);
    TdFilePtr fp = taosOpenFile(fnameStr, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
    ASSERT_NE(fp, nullptr) << fnameStr;
    ASSERT_EQ(taosCloseFile(&fp), 0);
  }

  auto txnBefore = collectSegFiles(".txn");
  ASSERT_EQ((int32_t)txnBefore.size(), total) << "setup did not place one .txn per segment";
  bool sawNonPrimaryTxn = false;
  for (const auto& kv : txnBefore) {
    if (kv.second != 0) sawNonPrimaryTxn = true;
  }
  ASSERT_TRUE(sawNonPrimaryTxn) << "no .txn landed off the primary mount; test is vacuous";

  // Retire all but the last few segments, leaving their .txn files orphaned.
  const int32_t kLive = 3;
  int64_t keepFrom = ((SWalFileInfo*)taosArrayGet(pWal->fileInfoSet, total - kLive))->firstVer;
  ASSERT_GT(keepFrom, 0);

  for (int64_t v = 0; v < ver; v++) {
    ASSERT_EQ(walCommit(pWal, v), 0);
  }
  walApplyVer(pWal, ver - 1);
  ASSERT_EQ(walBeginSnapshot(pWal, keepFrom - 1, 0), 0);
  ASSERT_EQ(walEndSnapshot(pWal, false), 0);

  int32_t liveCount = (int32_t)taosArrayGetSize(pWal->fileInfoSet);
  ASSERT_GT(liveCount, 0);
  ASSERT_LT(liveCount, total) << "retention did not recycle anything; nothing was orphaned";

  // Condition B keeps liveCount + WAL_TXN_TRIM_EXTRA_FILES .txn files as the deliberate
  // grace margin for a lagging CDC consumer, and trims the oldest beyond that. The
  // surviving count is therefore exact, not just "fewer than before".
  int32_t expected = liveCount + WAL_TXN_TRIM_EXTRA_FILES;
  ASSERT_LT(expected, total) << "not enough excess to exercise trim";

  auto txnAfter = collectSegFiles(".txn");
  EXPECT_EQ((int32_t)txnAfter.size(), expected)
      << "retention orphaned " << (total - liveCount) << " .txn file(s) and kept "
      << txnAfter.size() << " of " << total << "; expected " << expected
      << " (liveCount + WAL_TXN_TRIM_EXTRA_FILES)";

  // Every live segment's own .txn must be among the survivors -- trim must never take the
  // .txn of a segment whose .log is still there.
  for (int32_t i = 0; i < liveCount; i++) {
    SWalFileInfo* pInfo = (SWalFileInfo*)taosArrayGet(pWal->fileInfoSet, i);
    char          nameBuf[64];
    snprintf(nameBuf, sizeof(nameBuf), "%020" PRId64 ".txn", pInfo->firstVer);
    EXPECT_EQ(txnAfter.count(std::string(nameBuf)), 1u)
        << "trimmed the .txn of a segment whose .log is still live: " << nameBuf;
  }

  // And the survivors must be the newest ones: trim works oldest-first, so the set that
  // remains is exactly the `expected` highest firstVers of the original set. collectSegFiles
  // returns a std::map keyed by basename, and the names are zero-padded to a fixed width, so
  // iteration order is already ascending by firstVer.
  std::vector<std::string> beforeNames;
  for (const auto& kv : txnBefore) beforeNames.push_back(kv.first);
  std::vector<std::string> expectedNames(beforeNames.end() - expected, beforeNames.end());
  std::vector<std::string> afterNames;
  for (const auto& kv : txnAfter) afterNames.push_back(kv.first);
  EXPECT_EQ(afterNames, expectedNames) << "surviving .txn files are not the newest ones";
}

// Case 9: a meta whose file list is not in ascending order must not cause the orphan
// purge to delete live segments.
//
// fileInfoSet is ascending at runtime, but walMetaDeserialize pushes entries in the order
// the meta file lists them without sorting. If the purge bound were read from index 0, a
// reordered meta file would yield a bound above the real oldest segment and live data
// would be unlinked. This asserts the safety property -- every live segment survives --
// which the bound now gets from two independent sources (the scanned minimum over the
// file list, and vers.firstVer from the meta object). Either one alone is enough here, so
// this guards the outcome rather than one specific mechanism.
TEST_F(WalMultiMountEnv, purgeBoundIsMinFirstVerEvenIfMetaUnordered) {
  int64_t ver = 0;
  for (int i = 0; i < 16; i++) appendAndRoll(ver++);

  int32_t sz = (int32_t)taosArrayGetSize(pWal->fileInfoSet);
  ASSERT_GT(sz, 3);

  // Record every live segment's resolved path while the meta is still well-ordered.
  std::vector<std::string> liveFiles;
  for (int32_t i = 0; i < sz; i++) {
    SWalFileInfo* pInfo = (SWalFileInfo*)taosArrayGet(pWal->fileInfoSet, i);
    char          fnameStr[WAL_FILE_LEN];
    walBuildLogName(pWal, pInfo->firstVer, fnameStr);
    liveFiles.push_back(std::string(fnameStr));
  }

  // Rotate the array so the oldest segment is no longer at index 0 -- the shape a
  // reordered meta file would produce after deserialization.
  std::vector<SWalFileInfo> rotated;
  for (int32_t i = 0; i < sz; i++) {
    rotated.push_back(*(SWalFileInfo*)taosArrayGet(pWal->fileInfoSet, (i + sz / 2) % sz));
  }
  for (int32_t i = 0; i < sz; i++) {
    *(SWalFileInfo*)taosArrayGet(pWal->fileInfoSet, i) = rotated[i];
  }
  SWalFileInfo* pAtZero = (SWalFileInfo*)taosArrayGet(pWal->fileInfoSet, 0);
  ASSERT_GT(pAtZero->firstVer, 0) << "rotation did not move a later segment to index 0";

  // Repair must not treat the segments below pAtZero->firstVer as orphans.
  ASSERT_EQ(walCheckAndRepairMeta(pWal), 0);

  for (const auto& f : liveFiles) {
    EXPECT_TRUE(taosCheckExistFile(f.c_str()))
        << "live segment deleted because the purge bound was read from index 0: " << f;
  }
}
