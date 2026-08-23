#include <gtest/gtest.h>
#include <cstring>
#include <map>
#include <set>

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
    cfg.retentionPeriod = -1;
    cfg.retentionSize = 0;
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
