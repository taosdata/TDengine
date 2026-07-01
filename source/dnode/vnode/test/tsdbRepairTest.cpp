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

#include <sys/syscall.h>
#include <unistd.h>

#include <cstring>

extern "C" {
#include "dmRepair.h"
#include "vnodeInt.h"

typedef struct STFileObj STFileObj;
typedef struct SDataFileWriter SDataFileWriter;
typedef struct SSttFileWriter SSttFileWriter;
typedef struct STFileSet STFileSet;
typedef struct SBlockData SBlockData;

typedef struct {
  int32_t    size;
  int32_t    capacity;
  STFileSet **data;
} TFileSetArray;

typedef struct {
  SRowKey key;
  int64_t version;
} STsdbRowKey;

typedef struct {
  int64_t     suid;
  int64_t     uid;
  STsdbRowKey firstKey;
  STsdbRowKey lastKey;
  int64_t     minVer;
  int64_t     maxVer;
  int64_t     blockOffset;
  int64_t     smaOffset;
  int32_t     blockSize;
  int32_t     blockKeySize;
  int32_t     smaSize;
  int32_t     numRow;
  int32_t     count;
} SBrinRecord;

struct SBlockData {
  int64_t  suid;
  int64_t  uid;
  int32_t  nRow;
  int64_t *aUid;
  int64_t *aVersion;
  TSKEY   *aTSKEY;
};

enum {
  TSDB_REPAIR_ACTION_KEEP = 0,
  TSDB_REPAIR_ACTION_DROP = 1,
  TSDB_REPAIR_ACTION_REBUILD = 2,
};

typedef enum {
  TSDB_FEDIT_COMMIT = 1,
  TSDB_FEDIT_MERGE,
  TSDB_FEDIT_COMPACT,
  TSDB_FEDIT_RETENTION,
  TSDB_FEDIT_SSMIGRATE,
  TSDB_FEDIT_ROLLUP,
  TSDB_FEDIT_FORCE_REPAIR,
} EFEditT;

typedef enum {
  TSDB_FCURRENT = 1,
  TSDB_FCURRENT_C,
  TSDB_FCURRENT_M,
} EFCurrentT;

struct STsdb {
  char   *path;
  SVnode *pVnode;
  char    name[VNODE_TSDB_NAME_LEN];
};

typedef struct STFileSystem {
  STsdb        *tsdb;
  tsem_t        canEdit;
  int32_t       fsstate;
  int32_t       rollupLevel;
  int64_t       neid;
  EFEditT       etype;
  TFileSetArray fSetArr[1];
  TFileSetArray fSetArrTmp[1];
} STFileSystem;

bool tsdbRepairDataBlockLooksValid(const SBlockData *blockData, const SBrinRecord *record);
bool tsdbRepairSttBlockLooksValid(const SBlockData *blockData);
int32_t tsdbRepairResolveCoreAction(int32_t keptBlocks, int32_t droppedBlocks);
int32_t tsdbRepairResolveSttAction(int32_t keptDataBlocks, int32_t keptTombBlocks, int32_t droppedDataBlocks,
                                   int32_t droppedTombBlocks);
bool        tsdbRepairShouldProcessFileSet(int32_t vnodeId, int32_t fid);
EDmRepairStrategy tsdbRepairNormalizeStrategy(EDmRepairStrategy strategy);
const char *tsdbRepairStrategyName(EDmRepairStrategy strategy);
int32_t     tsdbRepairResolveMode(EDmRepairStrategy strategy);
void        tsdbRepairBuildHeadOnlyBrinRecord(const SBrinRecord *src, bool keepSma, SBrinRecord *dst);
int32_t     tsdbRepairDescribeHeadOnlyOps(bool hasHead, bool hasSma, bool dropSma);
bool        tsdbRepairFileAffected(const STFileObj *fobj);
const char *tsdbRepairFileIssue(const STFileObj *fobj);
int32_t     tsdbDataFileWriterClose(SDataFileWriter **writer, bool abort, void *opArray);
int32_t     tsdbSttFileWriterClose(SSttFileWriter **writer, int8_t abort, void *opArray);
void        current_fname(STsdb *pTsdb, char *fname, EFCurrentT ftype);
int32_t     tsdbFSEditCommit(STFileSystem *fs);
}

namespace {
bool              g_hasTsdbRepairTarget = false;
int32_t           g_targetVnodeId = 0;
int32_t           g_targetFileId = 0;
SRepairTsdbFileOpt g_targetTsdbFileOpt = {.strategy = DM_REPAIR_STRATEGY_TSDB_DROP_INVALID_ONLY};
}  // namespace

extern "C" {
SDmNotifyHandle dmNotifyHdl = {.state = 0};
}

extern "C" const SRepairTsdbFileOpt *dmRepairGetTsdbFileOpt(int32_t vnodeId, int32_t fileId) {
  if (g_hasTsdbRepairTarget && vnodeId == g_targetVnodeId && fileId == g_targetFileId) {
    return &g_targetTsdbFileOpt;
  }

  return nullptr;
}

struct TestFileMeta {
  int32_t type;
  SDiskID did;
  int32_t fid;
  int32_t lcn;
  int32_t mid;
  int64_t cid;
  int64_t size;
  int64_t minVer;
  int64_t maxVer;
  union {
    struct {
      int32_t level;
    } stt[1];
  };
};

struct TestFileObj {
  TdThreadMutex mutex;
  TestFileMeta  f[1];
  int32_t       state;
  int32_t       ref;
  int32_t       nlevel;
  char          fname[TSDB_FILENAME_LEN];
};

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

class TsdbRepairValidationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::memset(&block_, 0, sizeof(block_));
    std::memset(&record_, 0, sizeof(record_));

    block_.suid = 42;
    block_.uid = 7;
    block_.nRow = 2;
    block_.aTSKEY = ts_;
    block_.aVersion = versions_;

    record_.suid = 42;
    record_.uid = 7;
  }

  SBlockData  block_ = {};
  SBrinRecord record_ = {};
  int64_t     versions_[2] = {10, 11};
  TSKEY       ts_[2] = {100, 101};
  int64_t     uids_[2] = {7001, 7002};
};

TEST_F(TsdbRepairValidationTest, AcceptsMatchingCoreBlock) {
  EXPECT_TRUE(tsdbRepairDataBlockLooksValid(&block_, &record_));
}

TEST_F(TsdbRepairValidationTest, RejectsCoreBlockWithMismatchedUid) {
  record_.uid = 9;
  EXPECT_FALSE(tsdbRepairDataBlockLooksValid(&block_, &record_));
}

TEST_F(TsdbRepairValidationTest, RejectsCoreBlockWithoutDecodedArrays) {
  block_.aTSKEY = nullptr;
  EXPECT_FALSE(tsdbRepairDataBlockLooksValid(&block_, &record_));
}

TEST_F(TsdbRepairValidationTest, AcceptsSttBlockWithDecodedRows) {
  block_.uid = 0;
  block_.aUid = uids_;
  EXPECT_TRUE(tsdbRepairSttBlockLooksValid(&block_));
}

TEST_F(TsdbRepairValidationTest, RejectsSttBlockWithoutUidArrayWhenUidIsZero) {
  block_.uid = 0;
  block_.aUid = nullptr;
  EXPECT_FALSE(tsdbRepairSttBlockLooksValid(&block_));
}

TEST(TsdbRepairDecisionTest, KeepsHealthyCore) {
  EXPECT_EQ(tsdbRepairResolveCoreAction(3, 0), TSDB_REPAIR_ACTION_KEEP);
}

TEST(TsdbRepairDecisionTest, RebuildsPartiallyDamagedCore) {
  EXPECT_EQ(tsdbRepairResolveCoreAction(2, 1), TSDB_REPAIR_ACTION_REBUILD);
}

TEST(TsdbRepairDecisionTest, DropsFullyDamagedCore) {
  EXPECT_EQ(tsdbRepairResolveCoreAction(0, 2), TSDB_REPAIR_ACTION_DROP);
}

TEST(TsdbRepairDecisionTest, KeepsHealthySttFile) {
  EXPECT_EQ(tsdbRepairResolveSttAction(2, 1, 0, 0), TSDB_REPAIR_ACTION_KEEP);
}

TEST(TsdbRepairDecisionTest, RebuildsPartiallyDamagedSttFile) {
  EXPECT_EQ(tsdbRepairResolveSttAction(1, 0, 0, 1), TSDB_REPAIR_ACTION_REBUILD);
}

TEST(TsdbRepairDecisionTest, DropsFullyDamagedSttFile) {
  EXPECT_EQ(tsdbRepairResolveSttAction(0, 0, 2, 1), TSDB_REPAIR_ACTION_DROP);
}

enum {
  TSDB_REPAIR_MODE_DROP_INVALID_ONLY = 0,
  TSDB_REPAIR_MODE_HEAD_ONLY_REBUILD = 1,
  TSDB_REPAIR_MODE_FULL_REBUILD = 2,
};

enum {
  TSDB_REPAIR_HEAD_OP_REMOVE_HEAD = 1 << 0,
  TSDB_REPAIR_HEAD_OP_CREATE_HEAD = 1 << 1,
  TSDB_REPAIR_HEAD_OP_REMOVE_DATA = 1 << 2,
  TSDB_REPAIR_HEAD_OP_CREATE_DATA = 1 << 3,
  TSDB_REPAIR_HEAD_OP_REMOVE_SMA = 1 << 4,
  TSDB_REPAIR_HEAD_OP_CREATE_SMA = 1 << 5,
};

TEST(TsdbRepairStrategyTest, UsesPublicTsdbStrategyNames) {
  EXPECT_STREQ(tsdbRepairStrategyName(DM_REPAIR_STRATEGY_TSDB_DROP_INVALID_ONLY), "drop_invalid_only");
  EXPECT_STREQ(tsdbRepairStrategyName(DM_REPAIR_STRATEGY_TSDB_HEAD_ONLY_REBUILD), "head_only_rebuild");
  EXPECT_STREQ(tsdbRepairStrategyName(DM_REPAIR_STRATEGY_TSDB_FULL_REBUILD), "full_rebuild");
}

TEST(TsdbRepairModeTest, MapsDropOnlyStrategyToBadFileOnlyMode) {
  EXPECT_EQ(tsdbRepairResolveMode(DM_REPAIR_STRATEGY_TSDB_DROP_INVALID_ONLY), TSDB_REPAIR_MODE_DROP_INVALID_ONLY);
}

TEST(TsdbRepairModeTest, MapsHeadOnlyStrategyToHeadOnlyMode) {
  EXPECT_EQ(tsdbRepairResolveMode(DM_REPAIR_STRATEGY_TSDB_HEAD_ONLY_REBUILD), TSDB_REPAIR_MODE_HEAD_ONLY_REBUILD);
}

TEST(TsdbRepairModeTest, MapsFullRebuildStrategyToFullRebuildMode) {
  EXPECT_EQ(tsdbRepairResolveMode(DM_REPAIR_STRATEGY_TSDB_FULL_REBUILD), TSDB_REPAIR_MODE_FULL_REBUILD);
}

TEST(TsdbRepairDefaultStrategyTest, NormalizesMissingStrategyToDropInvalidOnly) {
  EXPECT_EQ(tsdbRepairNormalizeStrategy(DM_REPAIR_STRATEGY_NONE), DM_REPAIR_STRATEGY_TSDB_DROP_INVALID_ONLY);
}

class TsdbRepairScopeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    g_hasTsdbRepairTarget = false;
    g_targetVnodeId = 0;
    g_targetFileId = 0;
    g_targetTsdbFileOpt.strategy = DM_REPAIR_STRATEGY_TSDB_DROP_INVALID_ONLY;
  }

  void TearDown() override {
    g_hasTsdbRepairTarget = false;
    g_targetVnodeId = 0;
    g_targetFileId = 0;
  }
};

TEST_F(TsdbRepairScopeTest, SkipsFileSetWithoutExplicitTarget) {
  EXPECT_FALSE(tsdbRepairShouldProcessFileSet(7, 101));
}

TEST_F(TsdbRepairScopeTest, ProcessesExplicitlyTargetedFileSet) {
  g_hasTsdbRepairTarget = true;
  g_targetVnodeId = 7;
  g_targetFileId = 101;

  EXPECT_TRUE(tsdbRepairShouldProcessFileSet(7, 101));
}

TEST_F(TsdbRepairScopeTest, SkipsDifferentFileIdWithinSameVnode) {
  g_hasTsdbRepairTarget = true;
  g_targetVnodeId = 7;
  g_targetFileId = 101;

  EXPECT_FALSE(tsdbRepairShouldProcessFileSet(7, 102));
}

TEST(TsdbRepairBrinRecordTest, KeepsSmaOffsetsForHealthyHeadOnlyRewrite) {
  SBrinRecord src = {
      .suid = 11,
      .uid = 12,
      .minVer = 13,
      .maxVer = 14,
      .blockOffset = 15,
      .smaOffset = 16,
      .blockSize = 17,
      .blockKeySize = 18,
      .smaSize = 19,
      .numRow = 20,
      .count = 21,
  };
  SBrinRecord dst = {};

  tsdbRepairBuildHeadOnlyBrinRecord(&src, true, &dst);

  EXPECT_EQ(dst.suid, src.suid);
  EXPECT_EQ(dst.uid, src.uid);
  EXPECT_EQ(dst.minVer, src.minVer);
  EXPECT_EQ(dst.maxVer, src.maxVer);
  EXPECT_EQ(dst.blockOffset, src.blockOffset);
  EXPECT_EQ(dst.blockSize, src.blockSize);
  EXPECT_EQ(dst.blockKeySize, src.blockKeySize);
  EXPECT_EQ(dst.smaOffset, src.smaOffset);
  EXPECT_EQ(dst.smaSize, src.smaSize);
  EXPECT_EQ(dst.numRow, src.numRow);
  EXPECT_EQ(dst.count, src.count);
}

TEST(TsdbRepairBrinRecordTest, ClearsSmaOffsetsForDamagedHeadOnlyRewrite) {
  SBrinRecord src = {
      .suid = 21,
      .uid = 22,
      .minVer = 23,
      .maxVer = 24,
      .blockOffset = 25,
      .smaOffset = 26,
      .blockSize = 27,
      .blockKeySize = 28,
      .smaSize = 29,
      .numRow = 30,
      .count = 31,
  };
  SBrinRecord dst = {};

  tsdbRepairBuildHeadOnlyBrinRecord(&src, false, &dst);

  EXPECT_EQ(dst.suid, src.suid);
  EXPECT_EQ(dst.uid, src.uid);
  EXPECT_EQ(dst.blockOffset, src.blockOffset);
  EXPECT_EQ(dst.blockSize, src.blockSize);
  EXPECT_EQ(dst.blockKeySize, src.blockKeySize);
  EXPECT_EQ(dst.numRow, src.numRow);
  EXPECT_EQ(dst.count, src.count);
  EXPECT_EQ(dst.smaOffset, 0);
  EXPECT_EQ(dst.smaSize, 0);
}

TEST(TsdbRepairHeadOnlyRebuildTest, ReplacesOnlyHeadWhenSmaStaysHealthy) {
  int32_t opMask = tsdbRepairDescribeHeadOnlyOps(true, true, false);

  EXPECT_NE(opMask & TSDB_REPAIR_HEAD_OP_REMOVE_HEAD, 0);
  EXPECT_NE(opMask & TSDB_REPAIR_HEAD_OP_CREATE_HEAD, 0);
  EXPECT_EQ(opMask & TSDB_REPAIR_HEAD_OP_REMOVE_DATA, 0);
  EXPECT_EQ(opMask & TSDB_REPAIR_HEAD_OP_CREATE_DATA, 0);
  EXPECT_EQ(opMask & TSDB_REPAIR_HEAD_OP_REMOVE_SMA, 0);
  EXPECT_EQ(opMask & TSDB_REPAIR_HEAD_OP_CREATE_SMA, 0);
}

TEST(TsdbRepairHeadOnlyRebuildTest, RemovesSmaWhenOriginalSmaIsBad) {
  int32_t opMask = tsdbRepairDescribeHeadOnlyOps(true, true, true);

  EXPECT_NE(opMask & TSDB_REPAIR_HEAD_OP_REMOVE_HEAD, 0);
  EXPECT_NE(opMask & TSDB_REPAIR_HEAD_OP_CREATE_HEAD, 0);
  EXPECT_EQ(opMask & TSDB_REPAIR_HEAD_OP_REMOVE_DATA, 0);
  EXPECT_EQ(opMask & TSDB_REPAIR_HEAD_OP_CREATE_DATA, 0);
  EXPECT_NE(opMask & TSDB_REPAIR_HEAD_OP_REMOVE_SMA, 0);
  EXPECT_EQ(opMask & TSDB_REPAIR_HEAD_OP_CREATE_SMA, 0);
}

class TsdbRepairFileIssueTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::snprintf(path_, sizeof(path_), "/tmp/tsdb-repair-test-XXXXXX");
    int fd = mkstemp(path_);
    ASSERT_GE(fd, 0);

    static const char payload[] = "0123456789abcdef";
    ssize_t           written = write(fd, payload, sizeof(payload) - 1);
    ASSERT_EQ(written, static_cast<ssize_t>(sizeof(payload) - 1));
    ASSERT_EQ(syscall(SYS_close, fd), 0);

    std::memset(&fobj_, 0, sizeof(fobj_));
    std::snprintf(fobj_.fname, sizeof(fobj_.fname), "%s", path_);
    fobj_.f->size = 8;
  }

  void TearDown() override { taosRemoveFile(path_); }

  char        path_[TSDB_FILENAME_LEN] = {0};
  TestFileObj fobj_ = {};
};

TEST_F(TsdbRepairFileIssueTest, SizeMismatchDoesNotMarkFileAsAffected) {
  const STFileObj *fobj = reinterpret_cast<const STFileObj *>(&fobj_);

  EXPECT_FALSE(tsdbRepairFileAffected(fobj));
  EXPECT_EQ(tsdbRepairFileIssue(fobj), nullptr);
}

TEST(TsdbSttFileWriterCloseTest, NullWriterHandleDoesNotCrash) {
  ASSERT_EXIT(
      {
        SSttFileWriter *writer = nullptr;
        int32_t         code = tsdbSttFileWriterClose(&writer, 1, nullptr);
        if (code != 0) {
          _exit(1);
        }
        _exit(0);
      },
      ::testing::ExitedWithCode(0), "");
}

TEST(TsdbDataFileWriterCloseTest, NullWriterPointerDoesNotCrash) {
  ASSERT_EXIT(
      {
        SDataFileWriter **writer = nullptr;
        int32_t           code = tsdbDataFileWriterClose(writer, true, nullptr);
        if (code != 0) {
          _exit(1);
        }
        _exit(0);
      },
      ::testing::ExitedWithCode(0), "");
}

class TsdbForceRepairCommitTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::snprintf(rootPath_, sizeof(rootPath_), "/tmp/tsdb-force-repair-commit-XXXXXX");
    ASSERT_NE(mkdtemp(rootPath_), nullptr);

    std::snprintf(tsdbPath_, sizeof(tsdbPath_), "%s/testtsdb", rootPath_);
    ASSERT_EQ(taosMkDir(tsdbPath_), 0);

    std::memset(&vnode_, 0, sizeof(vnode_));
    std::memset(&tsdb_, 0, sizeof(tsdb_));
    std::memset(&fs_, 0, sizeof(fs_));

    vnode_.path = rootPath_;
    vnode_.config.vgId = 7;
    std::snprintf(tsdb_.name, sizeof(tsdb_.name), "testtsdb");
    tsdb_.pVnode = &vnode_;
    fs_.tsdb = &tsdb_;
    fs_.etype = TSDB_FEDIT_FORCE_REPAIR;
    fs_.fSetArr->size = 0;
    fs_.fSetArr->capacity = 0;
    fs_.fSetArr->data = nullptr;
    fs_.fSetArrTmp->size = 0;
    fs_.fSetArrTmp->capacity = 0;
    fs_.fSetArrTmp->data = nullptr;

    ASSERT_EQ(tsem_init(&fs_.canEdit, 0, 1), 0);

    char currentTmp[TSDB_FILENAME_LEN] = {0};
    current_fname(&tsdb_, currentTmp, TSDB_FCURRENT_M);

    TdFilePtr fp = taosCreateFile(currentTmp, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
    ASSERT_NE(fp, nullptr);
    ASSERT_EQ(taosWriteFile(fp, "{}", 2), 2);
    ASSERT_EQ(taosCloseFile(&fp), 0);
  }

  void TearDown() override {
    ASSERT_EQ(tsem_destroy(&fs_.canEdit), 0);
    taosRemoveDir(rootPath_);
  }

  char        rootPath_[TSDB_FILENAME_LEN] = {0};
  char        tsdbPath_[TSDB_FILENAME_LEN] = {0};
  SVnode      vnode_ = {};
  STsdb       tsdb_ = {};
  STFileSystem fs_ = {};
};

TEST_F(TsdbForceRepairCommitTest, ForceRepairCommitReleasesCanEditSemaphore) {
  ASSERT_EQ(tsem_wait(&fs_.canEdit), 0);

  ASSERT_EQ(tsdbFSEditCommit(&fs_), 0);
  EXPECT_EQ(tsem_timewait(&fs_.canEdit, 10), 0);
}
