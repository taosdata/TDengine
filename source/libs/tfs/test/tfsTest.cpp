/**
 * @file tfsTest.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief TFS module tests
 * @version 1.0
 * @date 2022-01-20
 *
 * @copyright Copyright (c) 2022
 *
 */

#include <gtest/gtest.h>
#include "os.h"

#include "tfs.h"

class TfsTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() { root = "/tmp/tfsTest"; }
  static void TearDownTestSuite() {}

 public:
  void SetUp() override {}
  void TearDown() override {}

  static const char *root;
};

const char *TfsTest::root;

TEST_F(TfsTest, 01_Open_Close) {
  SDiskCfg dCfg = {0};
  tstrncpy(dCfg.dir, root, TSDB_FILENAME_LEN);
  dCfg.level = 0;
  dCfg.primary = 1;

  taosRemoveDir(root);
  STfs *pTfs = tfsOpen(&dCfg, 1);
  ASSERT_EQ(pTfs, nullptr);

  taosMkDir(root);
  pTfs = tfsOpen(&dCfg, 1);
  ASSERT_NE(pTfs, nullptr);

  tfsUpdateSize(pTfs);
  SDiskSize size = tfsGetSize(pTfs);

  EXPECT_GT(size.avail, 0);
  EXPECT_GT(size.used, 0);
  EXPECT_GT(size.total, size.avail);
  EXPECT_GT(size.total, size.used);

  tfsClose(pTfs);
}

TEST_F(TfsTest, 02_AllocDisk) {
  int32_t  code = 0;
  SDiskCfg dCfg = {0};
  tstrncpy(dCfg.dir, root, TSDB_FILENAME_LEN);
  dCfg.level = 0;
  dCfg.primary = 1;

  taosRemoveDir(root);
  taosMkDir(root);
  STfs *pTfs = tfsOpen(&dCfg, 1);
  ASSERT_NE(pTfs, nullptr);

  SDiskID did;
  did.id = 0;
  did.level = 0;

  code = tfsAllocDisk(pTfs, 0, &did);
  EXPECT_EQ(code, 0);
  EXPECT_EQ(did.id, 0);
  EXPECT_EQ(did.level, 0);

  did.id = 1;
  did.level = 1;
  code = tfsAllocDisk(pTfs, 0, &did);
  EXPECT_EQ(code, 0);
  EXPECT_EQ(did.id, 0);
  EXPECT_EQ(did.level, 0);

  did.id = 1;
  did.level = 2;
  code = tfsAllocDisk(pTfs, 0, &did);
  EXPECT_EQ(code, 0);
  EXPECT_EQ(did.id, 0);
  EXPECT_EQ(did.level, 0);

  did.id = 1;
  did.level = 3;
  code = tfsAllocDisk(pTfs, 0, &did);
  EXPECT_EQ(code, 0);
  EXPECT_EQ(did.id, 0);
  EXPECT_EQ(did.level, 0);

  const char *primary = tfsGetPrimaryPath(pTfs);
  EXPECT_STREQ(primary, root);

  const char *path = tfsGetDiskPath(pTfs, did);
  EXPECT_STREQ(path, root);

  tfsClose(pTfs);
}

TEST_F(TfsTest, 03_Dir) {
  int32_t  code = 0;
  SDiskCfg dCfg = {0};
  tstrncpy(dCfg.dir, root, TSDB_FILENAME_LEN);
  dCfg.level = 0;
  dCfg.primary = 1;

  taosRemoveDir(root);
  taosMkDir(root);
  STfs *pTfs = tfsOpen(&dCfg, 1);
  ASSERT_NE(pTfs, nullptr);

  char p1[] = "p1";
  char ap1[128] = {0};
  snprintf(ap1, 128, "%s%s%s", root, TD_DIRSEP, p1);
  EXPECT_NE(taosDirExist(ap1), 0);
  EXPECT_EQ(tfsMkdir(pTfs, p1), 0);
  EXPECT_EQ(taosDirExist(ap1), 0);

  char p2[] = "p2";
  char ap2[128] = {0};
  snprintf(ap2, 128, "%s%s%s", root, TD_DIRSEP, p2);
  SDiskID did = {0};
  EXPECT_NE(taosDirExist(ap2), 0);
  EXPECT_EQ(tfsMkdirAt(pTfs, p2, did), 0);
  EXPECT_EQ(taosDirExist(ap2), 0);

  char p3[] = "p3/p2/p1/p0";
  char ap3[128] = {0};
  snprintf(ap3, 128, "%s%s%s", root, TD_DIRSEP, p3);
  EXPECT_NE(taosDirExist(ap3), 0);
  EXPECT_NE(tfsMkdir(pTfs, p3), 0);
  EXPECT_NE(tfsMkdirAt(pTfs, p3, did), 0);
  EXPECT_EQ(tfsMkdirRecurAt(pTfs, p3, did), 0);
  EXPECT_EQ(taosDirExist(ap3), 0);

  EXPECT_EQ(tfsRmdir(pTfs, p3), 0);
  EXPECT_NE(taosDirExist(ap3), 0);

  char p45[] = "p5";
  char p44[] = "p4";
  char p4[] = "p4/p2/p1/p0";
  char ap4[128] = {0};
  snprintf(ap4, 128, "%s%s%s", root, TD_DIRSEP, p4);

  EXPECT_NE(taosDirExist(ap4), 0);
  EXPECT_EQ(tfsMkdirRecurAt(pTfs, p4, did), 0);
  EXPECT_EQ(taosDirExist(ap4), 0);
  EXPECT_EQ(tfsRename(pTfs, p44, p45), 0);
  EXPECT_EQ(tfsRmdir(pTfs, p4), 0);
  EXPECT_NE(taosDirExist(ap4), 0);

  tfsClose(pTfs);
}
#if 0
TEST_F(TfsTest, 04_File) {
  int32_t  code = 0;
  SDiskCfg dCfg = {0};
  tstrncpy(dCfg.dir, root, TSDB_FILENAME_LEN);
  dCfg.level = 0;
  dCfg.primary = 1;

  taosRemoveDir(root);
  taosMkDir(root);
  STfs *pTfs = tfsOpen(&dCfg, 1);
  ASSERT_NE(pTfs, nullptr);

  STfsFile file0;
  STfsFile file1;
  STfsFile file2;
  STfsFile file3;
  STfsFile file4;
  SDiskID  did0 = {0};
  SDiskID  did1 = {0};
  SDiskID  did2 = {0};
  SDiskID  did3 = {0};
  SDiskID  did4 = {0};
  did3.id = 1;
  did4.level = 1;
  tfsInitFile(pTfs, &file0, did0, "fname");
  tfsInitFile(pTfs, &file1, did1, "fname");
  tfsInitFile(pTfs, &file2, did2, "fnamex");
  tfsInitFile(pTfs, &file3, did3, "fname");
  tfsInitFile(pTfs, &file4, did4, "fname");

  EXPECT_TRUE(tfsIsSameFile(&file0, &file1));
  EXPECT_FALSE(tfsIsSameFile(&file0, &file2));
  EXPECT_FALSE(tfsIsSameFile(&file0, &file3));
  EXPECT_FALSE(tfsIsSameFile(&file0, &file4));

  tfsClose(pTfs);
}
#endif