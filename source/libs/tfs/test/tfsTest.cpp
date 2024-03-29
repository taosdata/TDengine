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
#ifdef _TD_DARWIN_64
  static void SetUpTestSuite() { root = "/private" TD_TMP_DIR_PATH "tfsTest"; }
#else
  static void SetUpTestSuite() { root = TD_TMP_DIR_PATH "tfsTest"; }
#endif
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

  taosMulMkDir(root);
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
  EXPECT_NE(taosDirExist(ap1), 1);
  EXPECT_EQ(tfsMkdir(pTfs, p1), 0);
  EXPECT_EQ(taosDirExist(ap1), 1);

  char p2[] = "p2";
  char ap2[128] = {0};
  snprintf(ap2, 128, "%s%s%s", root, TD_DIRSEP, p2);
  SDiskID did = {0};
  EXPECT_NE(taosDirExist(ap2), 1);
  EXPECT_EQ(tfsMkdirAt(pTfs, p2, did), 0);
  EXPECT_EQ(taosDirExist(ap2), 1);

  char p3[] = "p3/p2/p1/p0";
  char ap3[128] = {0};
  snprintf(ap3, 128, "%s%s%s", root, TD_DIRSEP, p3);
  EXPECT_NE(taosDirExist(ap3), 1);
  EXPECT_NE(tfsMkdir(pTfs, p3), 0);
  EXPECT_NE(tfsMkdirAt(pTfs, p3, did), 0);
  EXPECT_EQ(tfsMkdirRecurAt(pTfs, p3, did), 0);
  EXPECT_EQ(taosDirExist(ap3), 1);

  EXPECT_EQ(tfsRmdir(pTfs, p3), 0);
  EXPECT_NE(taosDirExist(ap3), 1);

  char p45[] = "p5";
  char p44[] = "p4";
  char p4[] = "p4/p2/p1/p0";
  char ap4[128] = {0};
  snprintf(ap4, 128, "%s%s%s", root, TD_DIRSEP, p4);

  EXPECT_NE(taosDirExist(ap4), 1);
  EXPECT_EQ(tfsMkdirRecurAt(pTfs, p4, did), 0);
  EXPECT_EQ(taosDirExist(ap4), 1);
  EXPECT_EQ(tfsRename(pTfs, 0, p44, p45), 0);
  EXPECT_EQ(tfsRmdir(pTfs, p4), 0);
  EXPECT_NE(taosDirExist(ap4), 1);

  tfsClose(pTfs);
}

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

  {
    int32_t size = 1024;
    void   *ret = taosMemoryMalloc(size + sizeof(size_t));
    *(size_t *)ret = size;
    void *buf = (void *)((char *)ret + sizeof(size_t));

    file0.did.id = 0;
    file0.did.level = 0;
    int32_t len = tfsEncodeFile((void **)&buf, &file0);
    EXPECT_EQ(len, 8);

    STfsFile outfile = {0};
    char    *outbuf = (char *)tfsDecodeFile(pTfs, (void *)((char *)buf - len), &outfile);
    int32_t  decodeLen = (outbuf - (char *)buf);

    EXPECT_EQ(outfile.did.id, 0);
    EXPECT_EQ(outfile.did.level, 0);
    EXPECT_STREQ(outfile.aname, file0.aname);
    EXPECT_STREQ(outfile.rname, "fname");
    EXPECT_EQ(outfile.pTfs, pTfs);
    taosMemoryFree(ret);
  }

  {
    char     n1[] = "t3/t1.json";
    char     n2[] = "t3/t2.json";
    STfsFile f1 = {0};
    STfsFile f2 = {0};
    SDiskID  did;
    did.id = 0;
    did.level = 0;

    tfsInitFile(pTfs, &f1, did, n1);
    tfsInitFile(pTfs, &f2, did, n2);

    EXPECT_EQ(tfsMkdir(pTfs, "t3"), 0);

    // FILE *fp = fopen(f1.aname, "w");
    TdFilePtr pFile = taosOpenFile(f1.aname, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
    ASSERT_NE(pFile, nullptr);
    taosWriteFile(pFile, "12345678", 5);
    taosCloseFile(&pFile);

    char base[128] = {0};
    tfsBasename(&f1, base);
    char dir[128] = {0};
    tfsDirname(&f1, dir);

    EXPECT_STREQ(base, "t1.json");

    char fulldir[128];
    snprintf(fulldir, 128, "%s%s%s", root, TD_DIRSEP, "t3");
    EXPECT_STREQ(dir, fulldir);

    EXPECT_GT(tfsCopyFile(&f1, &f2), 0);

    char af2[128] = {0};
    snprintf(af2, 128, "%s%s%s", root, TD_DIRSEP, n2);
    EXPECT_EQ(taosDirExist(af2), 1);
    tfsRemoveFile(&f2);
    EXPECT_NE(taosDirExist(af2), 1);

    {
      STfsDir *pDir = tfsOpendir(pTfs, "t3");

      const STfsFile *pf1 = tfsReaddir(pDir);
      EXPECT_NE(pf1, nullptr);
      EXPECT_EQ(pf1->did.id, 0);
      EXPECT_EQ(pf1->did.level, 0);
      EXPECT_EQ(pf1->pTfs, pTfs);

      const STfsFile *pf2 = tfsReaddir(pDir);
      EXPECT_EQ(pf2, nullptr);

      tfsClosedir(pDir);
    }

    EXPECT_GT(tfsCopyFile(&f1, &f2), 0);

    {
      STfsDir *pDir = tfsOpendir(pTfs, "t3");

      const STfsFile *pf1 = tfsReaddir(pDir);
      EXPECT_NE(pf1, nullptr);
      EXPECT_EQ(pf1->did.id, 0);
      EXPECT_EQ(pf1->did.level, 0);
      EXPECT_EQ(pf1->pTfs, pTfs);

      const STfsFile *pf2 = tfsReaddir(pDir);
      EXPECT_EQ(pf2->did.id, 0);
      EXPECT_EQ(pf2->did.level, 0);
      EXPECT_EQ(pf2->pTfs, pTfs);

      const STfsFile *pf3 = tfsReaddir(pDir);
      EXPECT_EQ(pf3, nullptr);

      tfsClosedir(pDir);
    }
  }

  tfsClose(pTfs);
}

TEST_F(TfsTest, 05_MultiDisk) {
  int32_t code = 0;

#ifdef _TD_DARWIN_64
  const char *root00 = "/private" TD_TMP_DIR_PATH "tfsTest00";
  const char *root01 = "/private" TD_TMP_DIR_PATH "tfsTest01";
  const char *root10 = "/private" TD_TMP_DIR_PATH "tfsTest10";
  const char *root11 = "/private" TD_TMP_DIR_PATH "tfsTest11";
  const char *root12 = "/private" TD_TMP_DIR_PATH "tfsTest12";
  const char *root20 = "/private" TD_TMP_DIR_PATH "tfsTest20";
  const char *root21 = "/private" TD_TMP_DIR_PATH "tfsTest21";
  const char *root22 = "/private" TD_TMP_DIR_PATH "tfsTest22";
  const char *root23 = "/private" TD_TMP_DIR_PATH "tfsTest23";
#else
  const char *root00 = TD_TMP_DIR_PATH "tfsTest00";
  const char *root01 = TD_TMP_DIR_PATH "tfsTest01";
  const char *root10 = TD_TMP_DIR_PATH "tfsTest10";
  const char *root11 = TD_TMP_DIR_PATH "tfsTest11";
  const char *root12 = TD_TMP_DIR_PATH "tfsTest12";
  const char *root20 = TD_TMP_DIR_PATH "tfsTest20";
  const char *root21 = TD_TMP_DIR_PATH "tfsTest21";
  const char *root22 = TD_TMP_DIR_PATH "tfsTest22";
  const char *root23 = TD_TMP_DIR_PATH "tfsTest23";
#endif

  SDiskCfg dCfg[9] = {0};
  tstrncpy(dCfg[0].dir, root01, TSDB_FILENAME_LEN);
  dCfg[0].level = 0;
  dCfg[0].primary = 0;
  tstrncpy(dCfg[1].dir, root00, TSDB_FILENAME_LEN);
  dCfg[1].level = 0;
  dCfg[1].primary = 0;
  tstrncpy(dCfg[2].dir, root20, TSDB_FILENAME_LEN);
  dCfg[2].level = 2;
  dCfg[2].primary = 0;
  tstrncpy(dCfg[3].dir, root21, TSDB_FILENAME_LEN);
  dCfg[3].level = 2;
  dCfg[3].primary = 0;
  tstrncpy(dCfg[4].dir, root22, TSDB_FILENAME_LEN);
  dCfg[4].level = 2;
  dCfg[4].primary = 0;
  tstrncpy(dCfg[5].dir, root23, TSDB_FILENAME_LEN);
  dCfg[5].level = 2;
  dCfg[5].primary = 0;
  tstrncpy(dCfg[6].dir, root10, TSDB_FILENAME_LEN);
  dCfg[6].level = 1;
  dCfg[6].primary = 0;
  tstrncpy(dCfg[7].dir, root11, TSDB_FILENAME_LEN);
  dCfg[7].level = 1;
  dCfg[7].primary = 0;
  tstrncpy(dCfg[8].dir, root12, TSDB_FILENAME_LEN);
  dCfg[8].level = 1;
  dCfg[8].primary = 0;

  taosRemoveDir(root00);
  taosRemoveDir(root01);
  taosRemoveDir(root10);
  taosRemoveDir(root11);
  taosRemoveDir(root12);
  taosRemoveDir(root20);
  taosRemoveDir(root21);
  taosRemoveDir(root22);
  taosRemoveDir(root23);
  taosMkDir(root00);
  taosMkDir(root01);
  taosMkDir(root10);
  taosMkDir(root11);
  taosMkDir(root12);
  taosMkDir(root20);
  taosMkDir(root21);
  taosMkDir(root22);
  taosMkDir(root23);

  STfs *pTfs = tfsOpen(dCfg, 9);
  ASSERT_EQ(pTfs, nullptr);

  dCfg[0].primary = 1;
  dCfg[1].primary = 1;
  pTfs = tfsOpen(dCfg, 9);
  ASSERT_EQ(pTfs, nullptr);

  dCfg[0].primary = 0;
  dCfg[1].primary = 1;
  pTfs = tfsOpen(dCfg, 9);
  ASSERT_NE(pTfs, nullptr);

  tfsUpdateSize(pTfs);
  SDiskSize size = tfsGetSize(pTfs);

  EXPECT_GT(size.avail, 0);
  EXPECT_GT(size.used, 0);
  EXPECT_GT(size.total, size.avail);
  EXPECT_GT(size.total, size.used);

  //------------- AllocDisk -----------------//
  {
    const char *path = NULL;
    SDiskID     did;
    did.id = 0;
    did.level = 0;

    code = tfsAllocDisk(pTfs, 0, &did);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(did.id, 0);
    EXPECT_EQ(did.level, 0);
    path = tfsGetDiskPath(pTfs, did);
    EXPECT_STREQ(path, root00);

    code = tfsAllocDisk(pTfs, 0, &did);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(did.id, 1);
    EXPECT_EQ(did.level, 0);
    path = tfsGetDiskPath(pTfs, did);
    EXPECT_STREQ(path, root01);

    code = tfsAllocDisk(pTfs, 0, &did);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(did.id, 0);
    EXPECT_EQ(did.level, 0);
    path = tfsGetDiskPath(pTfs, did);
    EXPECT_STREQ(path, root00);

    code = tfsAllocDisk(pTfs, 0, &did);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(did.id, 1);
    EXPECT_EQ(did.level, 0);
    path = tfsGetDiskPath(pTfs, did);
    EXPECT_STREQ(path, root01);

    code = tfsAllocDisk(pTfs, 0, &did);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(did.id, 0);
    EXPECT_EQ(did.level, 0);
    path = tfsGetDiskPath(pTfs, did);
    EXPECT_STREQ(path, root00);

    code = tfsAllocDisk(pTfs, 0, &did);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(did.id, 1);
    EXPECT_EQ(did.level, 0);
    path = tfsGetDiskPath(pTfs, did);
    EXPECT_STREQ(path, root01);

    code = tfsAllocDisk(pTfs, 1, &did);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(did.id, 0);
    EXPECT_EQ(did.level, 1);
    path = tfsGetDiskPath(pTfs, did);
    EXPECT_STREQ(path, root10);

    code = tfsAllocDisk(pTfs, 1, &did);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(did.id, 1);
    EXPECT_EQ(did.level, 1);
    path = tfsGetDiskPath(pTfs, did);
    EXPECT_STREQ(path, root11);

    code = tfsAllocDisk(pTfs, 1, &did);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(did.id, 2);
    EXPECT_EQ(did.level, 1);
    path = tfsGetDiskPath(pTfs, did);
    EXPECT_STREQ(path, root12);

    code = tfsAllocDisk(pTfs, 1, &did);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(did.id, 0);
    EXPECT_EQ(did.level, 1);
    path = tfsGetDiskPath(pTfs, did);
    EXPECT_STREQ(path, root10);

    code = tfsAllocDisk(pTfs, 2, &did);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(did.id, 0);
    EXPECT_EQ(did.level, 2);
    path = tfsGetDiskPath(pTfs, did);
    EXPECT_STREQ(path, root20);

    code = tfsAllocDisk(pTfs, 2, &did);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(did.id, 1);
    EXPECT_EQ(did.level, 2);
    path = tfsGetDiskPath(pTfs, did);
    EXPECT_STREQ(path, root21);

    code = tfsAllocDisk(pTfs, 2, &did);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(did.id, 2);
    EXPECT_EQ(did.level, 2);
    path = tfsGetDiskPath(pTfs, did);
    EXPECT_STREQ(path, root22);

    code = tfsAllocDisk(pTfs, 2, &did);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(did.id, 3);
    EXPECT_EQ(did.level, 2);
    path = tfsGetDiskPath(pTfs, did);
    EXPECT_STREQ(path, root23);

    code = tfsAllocDisk(pTfs, 2, &did);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(did.id, 0);
    EXPECT_EQ(did.level, 2);
    path = tfsGetDiskPath(pTfs, did);
    EXPECT_STREQ(path, root20);

    code = tfsAllocDisk(pTfs, 3, &did);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(did.id, 1);
    EXPECT_EQ(did.level, 2);
    path = tfsGetDiskPath(pTfs, did);
    EXPECT_STREQ(path, root21);

    code = tfsAllocDisk(pTfs, 4, &did);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(did.id, 2);
    EXPECT_EQ(did.level, 2);
    path = tfsGetDiskPath(pTfs, did);
    EXPECT_STREQ(path, root22);

    const char *primary = tfsGetPrimaryPath(pTfs);
    EXPECT_STREQ(primary, root00);
  }

  //------------- Dir -----------------//
  {
    char p1[] = "p1";
    char ap00[128] = {0};
    snprintf(ap00, 128, "%s%s%s", root00, TD_DIRSEP, p1);
    char ap01[128] = {0};
    snprintf(ap01, 128, "%s%s%s", root01, TD_DIRSEP, p1);
    char ap10[128] = {0};
    snprintf(ap10, 128, "%s%s%s", root10, TD_DIRSEP, p1);
    char ap11[128] = {0};
    snprintf(ap11, 128, "%s%s%s", root11, TD_DIRSEP, p1);
    char ap12[128] = {0};
    snprintf(ap12, 128, "%s%s%s", root12, TD_DIRSEP, p1);
    char ap20[128] = {0};
    snprintf(ap20, 128, "%s%s%s", root20, TD_DIRSEP, p1);
    char ap21[128] = {0};
    snprintf(ap21, 128, "%s%s%s", root21, TD_DIRSEP, p1);
    char ap22[128] = {0};
    snprintf(ap22, 128, "%s%s%s", root22, TD_DIRSEP, p1);
    char ap23[128] = {0};
    snprintf(ap23, 128, "%s%s%s", root23, TD_DIRSEP, p1);
    EXPECT_NE(taosDirExist(ap00), 1);
    EXPECT_NE(taosDirExist(ap01), 1);
    EXPECT_NE(taosDirExist(ap10), 1);
    EXPECT_NE(taosDirExist(ap11), 1);
    EXPECT_NE(taosDirExist(ap12), 1);
    EXPECT_NE(taosDirExist(ap20), 1);
    EXPECT_NE(taosDirExist(ap21), 1);
    EXPECT_NE(taosDirExist(ap22), 1);
    EXPECT_NE(taosDirExist(ap23), 1);
    EXPECT_EQ(tfsMkdir(pTfs, p1), 0);
    EXPECT_EQ(taosDirExist(ap00), 1);
    EXPECT_EQ(taosDirExist(ap01), 1);
    EXPECT_EQ(taosDirExist(ap10), 1);
    EXPECT_EQ(taosDirExist(ap11), 1);
    EXPECT_EQ(taosDirExist(ap12), 1);
    EXPECT_EQ(taosDirExist(ap20), 1);
    EXPECT_EQ(taosDirExist(ap21), 1);
    EXPECT_EQ(taosDirExist(ap22), 1);
    EXPECT_EQ(taosDirExist(ap23), 1);
    EXPECT_EQ(tfsRmdir(pTfs, p1), 0);
    EXPECT_NE(taosDirExist(ap00), 1);
    EXPECT_NE(taosDirExist(ap01), 1);
    EXPECT_NE(taosDirExist(ap10), 1);
    EXPECT_NE(taosDirExist(ap11), 1);
    EXPECT_NE(taosDirExist(ap12), 1);
    EXPECT_NE(taosDirExist(ap20), 1);
    EXPECT_NE(taosDirExist(ap21), 1);
    EXPECT_NE(taosDirExist(ap22), 1);
    EXPECT_NE(taosDirExist(ap23), 1);

    char p2[] = "p2";
    char _ap21[128] = {0};
    snprintf(_ap21, 128, "%s%s%s", root21, TD_DIRSEP, p2);
    SDiskID did = {0};
    did.level = 2;
    did.id = 1;
    EXPECT_NE(taosDirExist(_ap21), 1);
    EXPECT_EQ(tfsMkdirAt(pTfs, p2, did), 0);
    EXPECT_EQ(taosDirExist(_ap21), 1);

    char p3[] = "p3/p2/p1/p0";
    char _ap12[128] = {0};
    snprintf(_ap12, 128, "%s%s%s", root12, TD_DIRSEP, p3);
    did.level = 1;
    did.id = 2;
    EXPECT_NE(taosDirExist(_ap12), 1);
    EXPECT_NE(tfsMkdir(pTfs, p3), 0);
    EXPECT_NE(tfsMkdirAt(pTfs, p3, did), 0);
    EXPECT_EQ(tfsMkdirRecurAt(pTfs, p3, did), 0);
    EXPECT_EQ(taosDirExist(_ap12), 1);
    EXPECT_EQ(tfsRmdir(pTfs, p3), 0);
    EXPECT_NE(taosDirExist(_ap12), 1);

    char p45[] = "p5";
    char p44[] = "p4";
    char p4[] = "p4/p2/p1/p0";
    char _ap22[128] = {0};
    snprintf(_ap22, 128, "%s%s%s", root22, TD_DIRSEP, p4);
    did.level = 2;
    did.id = 2;

    EXPECT_NE(taosDirExist(_ap22), 1);
    EXPECT_EQ(tfsMkdirRecurAt(pTfs, p4, did), 0);
    EXPECT_EQ(taosDirExist(_ap22), 1);
    EXPECT_EQ(tfsRename(pTfs, 0, p44, p45), 0);
    EXPECT_EQ(tfsRmdir(pTfs, p4), 0);
    EXPECT_NE(taosDirExist(_ap22), 1);
  }

  //------------- File -----------------//
  {
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

    {
      char     n1[] = "t3/t1.json";
      char     n2[] = "t3/t2.json";
      STfsFile f1 = {0};
      STfsFile f2 = {0};
      SDiskID  did;
      did1.level = 1;
      did1.id = 2;
      did2.level = 2;
      did2.id = 3;

      tfsInitFile(pTfs, &f1, did1, n1);
      tfsInitFile(pTfs, &f2, did2, n2);

      EXPECT_EQ(tfsMkdir(pTfs, "t3"), 0);

      // FILE *fp = fopen(f1.aname, "w");
      TdFilePtr pFile = taosOpenFile(f1.aname, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
      ASSERT_NE(pFile, nullptr);
      taosWriteFile(pFile, "12345678", 5);
      taosCloseFile(&pFile);

      char base[128] = {0};
      tfsBasename(&f1, base);
      char dir[128] = {0};
      tfsDirname(&f1, dir);

      EXPECT_STREQ(base, "t1.json");

      char fulldir[128];
      snprintf(fulldir, 128, "%s%s%s", root12, TD_DIRSEP, "t3");
      EXPECT_STREQ(dir, fulldir);

      EXPECT_GT(tfsCopyFile(&f1, &f2), 0);

      char af2[128] = {0};
      snprintf(af2, 128, "%s%s%s", root23, TD_DIRSEP, n2);
      EXPECT_EQ(taosDirExist(af2), 1);
      tfsRemoveFile(&f2);

      {
        STfsDir *pDir = tfsOpendir(pTfs, "t3");

        const STfsFile *pf1 = tfsReaddir(pDir);
        EXPECT_NE(pf1, nullptr);
        EXPECT_EQ(pf1->did.level, 1);
        EXPECT_EQ(pf1->did.id, 2);
        EXPECT_EQ(pf1->pTfs, pTfs);

        const STfsFile *pf2 = tfsReaddir(pDir);
        EXPECT_EQ(pf2, nullptr);

        tfsClosedir(pDir);
      }

      EXPECT_NE(taosDirExist(af2), 1);
      EXPECT_GT(tfsCopyFile(&f1, &f2), 0);

      {
        STfsDir *pDir = tfsOpendir(pTfs, "t3");

        const STfsFile *pf1 = tfsReaddir(pDir);
        EXPECT_NE(pf1, nullptr);
        EXPECT_GT(pf1->did.level, 0);
        EXPECT_GT(pf1->did.id, 0);
        EXPECT_EQ(pf1->pTfs, pTfs);

        const STfsFile *pf2 = tfsReaddir(pDir);
        EXPECT_NE(pf1, nullptr);
        EXPECT_GT(pf1->did.level, 0);
        EXPECT_GT(pf1->did.id, 0);
        EXPECT_EQ(pf1->pTfs, pTfs);

        const STfsFile *pf3 = tfsReaddir(pDir);
        EXPECT_EQ(pf3, nullptr);

        tfsClosedir(pDir);
      }
    }
  }

  tfsClose(pTfs);
}
