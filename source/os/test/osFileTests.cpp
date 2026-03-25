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
#include <iostream>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wformat"
#pragma GCC diagnostic ignored "-Wint-to-pointer-cast"
#pragma GCC diagnostic ignored "-Wpointer-arith"

#include "os.h"
#include "tlog.h"

static void buildTmpPath(char* out, size_t n, const char* fileName) {
  (void)snprintf(out, n, "%s%s", TD_TMP_DIR_PATH, fileName);
}

TEST(osFileTests, taosGetTmpfilePath) {
  char inputTmpDir[100] = "/tmp";
  char fileNamePrefix[100] = "txt";
  char dstPath[100] = {0};

  taosGetTmpfilePath(NULL, fileNamePrefix, dstPath);
  taosGetTmpfilePath(inputTmpDir, NULL, dstPath);
  taosGetTmpfilePath(inputTmpDir, fileNamePrefix, dstPath);

  int32_t ret = taosRemoveFile(NULL);
  EXPECT_NE(ret, 0);

  ret = taosCloseFile(NULL);
  EXPECT_EQ(ret, 0);

  ret = taosRenameFile(NULL, "");
  EXPECT_NE(ret, 0);
  ret = taosRenameFile("", NULL);
  EXPECT_NE(ret, 0);

  int64_t stDev = 0;
  int64_t stIno = 0;
  ret = taosDevInoFile(NULL, &stDev, &stIno);
  EXPECT_NE(ret, 0);
}

TEST(osFileTests, taosCopyFile) {
  char    from[100] = {0};
  char    to[100] = {0};
  int64_t ret = taosCopyFile(from, NULL);
  EXPECT_EQ(ret, -1);

  ret = taosCopyFile(NULL, to);
  EXPECT_EQ(ret, -1);

  ret = taosCopyFile(from, to);
  EXPECT_EQ(ret, -1);

  tstrncpy(from, "/tmp/tdengine-test-file", sizeof(from));
  TdFilePtr testFilePtr = taosCreateFile(from, TD_FILE_CREATE);
  taosWriteFile(testFilePtr, "abcdefg", 9);

  int64_t ret64 = taosReadFile(testFilePtr, NULL, 0);
  EXPECT_NE(ret64, 0);
  ret64 = taosReadFile(NULL, to, 100);
  EXPECT_NE(ret64, 0);
  ret64 = taosWriteFile(testFilePtr, NULL, 0);
  EXPECT_EQ(ret64, 0);
  ret64 = taosWriteFile(NULL, to, 100);
  EXPECT_EQ(ret64, 0);
  ret64 = taosPWriteFile(testFilePtr, NULL, 0, 0);
  EXPECT_EQ(ret64, 0);
  ret64 = taosPWriteFile(NULL, to, 100, 0);
  EXPECT_EQ(ret64, 0);
  ret64 = taosLSeekFile(NULL, 0, 0);
  EXPECT_EQ(ret64, -1);

  ret64 = taosPReadFile(NULL, NULL, 0, 0);
  EXPECT_EQ(ret64, -1);

  bool retb = taosValidFile(testFilePtr);
  EXPECT_TRUE(retb);
  retb = taosValidFile(NULL);
  EXPECT_FALSE(retb);

  retb = taosCheckAccessFile(NULL, 0);
  EXPECT_FALSE(retb);

  int32_t ret32 = taosFStatFile(NULL, NULL, NULL);
  EXPECT_NE(ret32, 0);

  ret32 = taosLockFile(NULL);
  EXPECT_NE(ret32, 0);
  ret32 = taosUnLockFile(NULL);
  EXPECT_NE(ret32, 0);
  ret32 = taosFtruncateFile(NULL, 0);
  EXPECT_NE(ret32, 0);
  ret64 = taosFSendFile(NULL, testFilePtr, NULL, 0);
  EXPECT_NE(ret64, 0);
  ret64 = taosFSendFile(testFilePtr, NULL, NULL, 0);
  EXPECT_NE(ret64, 0);

  char buf[100] = {0};
  ret64 = taosGetLineFile(NULL, (char**)&buf);
  EXPECT_EQ(ret64, -1);
  ret64 = taosGetLineFile(testFilePtr, NULL);
  EXPECT_EQ(ret64, -1);

  ret64 = taosGetsFile(testFilePtr, 0, NULL);
  EXPECT_NE(ret64, -1);
  ret64 = taosGetsFile(NULL, 0, buf);
  EXPECT_NE(ret64, -1);

  ret32 = taosEOFFile(NULL);
  EXPECT_NE(ret64, -1);

  taosCloseFile(&testFilePtr);
  ret32 = taosFStatFile(testFilePtr, NULL, NULL);
  EXPECT_NE(ret32, 0);
  ret32 = taosLockFile(testFilePtr);
  EXPECT_NE(ret32, 0);
  ret32 = taosUnLockFile(testFilePtr);
  EXPECT_NE(ret32, 0);
  ret32 = taosFtruncateFile(testFilePtr, 0);
  EXPECT_NE(ret32, 0);
  ret64 = taosFSendFile(testFilePtr, testFilePtr, NULL, 0);
  EXPECT_NE(ret64, 0);
  ret64 = taosGetLineFile(testFilePtr, NULL);
  EXPECT_EQ(ret64, -1);
  ret64 = taosGetsFile(testFilePtr, 0, NULL);
  EXPECT_NE(ret64, -1);
  ret32 = taosEOFFile(testFilePtr);
  EXPECT_NE(ret64, -1);

  retb = taosValidFile(testFilePtr);
  EXPECT_FALSE(retb);

  ret = taosCopyFile(from, to);
  EXPECT_EQ(ret, -1);

  int64_t size = 0;
  int64_t mtime = 0;
  int64_t atime = 0;
  ret = taosStatFile(NULL, &size, &mtime, &atime);
  EXPECT_NE(ret, 0);

  ret = taosStatFile(from, &size, &mtime, NULL);
  EXPECT_EQ(ret, 0);

  int64_t diskid = 0;
  ret = taosGetFileDiskID(NULL, &diskid);
  EXPECT_NE(ret, 0);

  ret = taosGetFileDiskID("", &diskid);
  EXPECT_NE(ret, 0);

  ret = taosGetFileDiskID(from, NULL);
  EXPECT_EQ(ret, 0);

  ret32 = taosCompressFile(NULL, "");
  EXPECT_NE(ret32, 0);
  ret32 = taosCompressFile("", NULL);
  EXPECT_NE(ret32, 0);
  ret32 = taosCompressFile("", "");
  EXPECT_NE(ret32, 0);
  ret32 = taosCompressFile("/tmp/tdengine-test-file", "");
  EXPECT_NE(ret32, 0);

  ret32 = taosLinkFile("", "");
  EXPECT_NE(ret32, 0);

  char  mod[8] = {0};
  FILE* retptr = taosOpenCFile(NULL, "");
  EXPECT_EQ(retptr, nullptr);
  retptr = taosOpenCFile("", NULL);
  EXPECT_EQ(retptr, nullptr);
  retptr = taosOpenCFile("", mod);
  EXPECT_EQ(retptr, nullptr);

  ret32 = taosSeekCFile(NULL, 0, 0);
  EXPECT_NE(ret32, 0);

  size_t retsize = taosReadFromCFile(buf, 0, 0, NULL);
  EXPECT_EQ(retsize, 0);
  retsize = taosReadFromCFile(NULL, 0, 0, NULL);
  EXPECT_EQ(retsize, 0);

  taosRemoveFile(from);
}

TEST(osFileTests, taosCreateFile) {
  char    path[100] = {0};
  int32_t tdFileOptions = 0;

  TdFilePtr ret = taosCreateFile(NULL, 0);
  EXPECT_EQ(ret, nullptr);

  ret = taosCreateFile(path, 0);
  EXPECT_EQ(ret, nullptr);

  FILE* retptr = taosOpenFileForStream(NULL, 0);
  EXPECT_EQ(retptr, nullptr);

  TdFilePtr retptr2 = taosOpenFile(NULL, 0);
  EXPECT_EQ(retptr2, nullptr);
}

TEST(osFileTests, taosFsyncFile) {
  char testFile[64] = "/tmp/test_fsync.txt";
  TdFilePtr pFile = taosOpenFile(testFile, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  ASSERT_NE(pFile, nullptr);

  const char* data = "test data\n";
  taosWriteFile(pFile, data, strlen(data));

  // Test fsync
  int32_t ret = taosFsyncFile(pFile);
  EXPECT_EQ(ret, 0);

  // Test with NULL
  ret = taosFsyncFile(NULL);
  EXPECT_EQ(ret, 0);

  taosCloseFile(&pFile);
  taosRemoveFile(testFile);
}

TEST(osFileTests, taosFprintfFile) {
  char testFile[64] = "/tmp/test_fprintf.txt";
  TdFilePtr pFile = taosOpenFile(testFile, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_STREAM);
  ASSERT_NE(pFile, nullptr);

  // Test fprintf
  taosFprintfFile(pFile, "Hello %s %d\n", "world", 123);

  // Test with NULL
  taosFprintfFile(NULL, "test");

  taosCloseFile(&pFile);
  taosRemoveFile(testFile);
}

TEST(osFileTests, taosUmaskFile) {
#ifndef WINDOWS
  int32_t oldMask = taosUmaskFile(0022);
  EXPECT_GE(oldMask, 0);
  taosUmaskFile(oldMask);
#else
  int32_t ret = taosUmaskFile(0022);
  EXPECT_EQ(ret, 0);
#endif
}

TEST(osFileTests, taosSymLink) {
  char targetFile[64] = "/tmp/test_symlink_target.txt";
  char linkFile[64] = "/tmp/test_symlink_link.txt";

  taosRemoveFile(linkFile);
  taosRemoveFile(targetFile);

  TdFilePtr pFile = taosOpenFile(targetFile, TD_FILE_CREATE | TD_FILE_WRITE);
  ASSERT_NE(pFile, nullptr);
  taosWriteFile(pFile, "target", 6);
  taosCloseFile(&pFile);

  int32_t ret = taosSymLink(targetFile, linkFile);
#ifndef WINDOWS
  EXPECT_EQ(ret, 0);
  bool exists = taosCheckExistFile(linkFile);
  EXPECT_TRUE(exists);
#endif

  taosRemoveFile(linkFile);
  taosRemoveFile(targetFile);
}

TEST(osFileTests, taosSetFileHandlesLimit) {
  int32_t ret = taosSetFileHandlesLimit();
  EXPECT_EQ(ret, 0);
}

TEST(osFileTests, taosCloseCFile) {
  char testFile[64] = "/tmp/test_cfile.txt";
  FILE* fp = taosOpenCFile(testFile, "w");
  ASSERT_NE(fp, nullptr);

  fprintf(fp, "test\n");

  int ret = taosCloseCFile(fp);
  EXPECT_EQ(ret, 0);

  taosRemoveFile(testFile);
}

TEST(osFileTests, taosSetAutoDelFile) {
  char testFile[64] = "/tmp/test_autodel.txt";

  TdFilePtr pFile = taosOpenFile(testFile, TD_FILE_CREATE | TD_FILE_WRITE);
  ASSERT_NE(pFile, nullptr);
  taosWriteFile(pFile, "test", 4);
  taosCloseFile(&pFile);

  int ret = taosSetAutoDelFile(testFile);

#ifndef WINDOWS
  EXPECT_EQ(ret, 0);
#endif
}

TEST(osFileTests, taosWritevFile) {
  char testFile[64] = "/tmp/test_writev.txt";

  TdFilePtr pFile = taosOpenFile(testFile, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  ASSERT_NE(pFile, nullptr);

  TaosIOVec iov[3];
  char buf1[] = "Hello ";
  char buf2[] = "World ";
  char buf3[] = "!\n";

  iov[0].iov_base = buf1;
  iov[0].iov_len = strlen(buf1);
  iov[1].iov_base = buf2;
  iov[1].iov_len = strlen(buf2);
  iov[2].iov_base = buf3;
  iov[2].iov_len = strlen(buf3);

  int64_t written = taosWritevFile(pFile, iov, 3);
  EXPECT_GT(written, 0);

  // Test error cases
  int64_t ret = taosWritevFile(NULL, iov, 3);
  EXPECT_EQ(ret, -1);

  ret = taosWritevFile(pFile, NULL, 3);
  EXPECT_EQ(ret, -1);

  ret = taosWritevFile(pFile, iov, 0);
  EXPECT_EQ(ret, -1);

  taosCloseFile(&pFile);
  taosRemoveFile(testFile);
}

TEST(osFileTests, lastErrorIsFileNotExist) {
  char nonExistentFile[64] = "/tmp/this_file_does_not_exist_xyz123.txt";

  TdFilePtr pFile = taosOpenFile(nonExistentFile, TD_FILE_READ);
  EXPECT_EQ(pFile, nullptr);

  bool isNotExist = lastErrorIsFileNotExist();
  EXPECT_TRUE(isNotExist);
}

TEST(osFileTests, taosCopyFileSuccessPath) {
  char src[256] = {0};
  char dst[256] = {0};
  buildTmpPath(src, sizeof(src), "td_osfile_copy_src.bin");
  buildTmpPath(dst, sizeof(dst), "td_osfile_copy_dst.bin");

  taosRemoveFile(src);
  taosRemoveFile(dst);

  TdFilePtr pSrc = taosOpenFile(src, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  ASSERT_NE(pSrc, nullptr);

  char payload[5000];
  (void)memset(payload, 'a', sizeof(payload));
  int64_t w = taosWriteFile(pSrc, payload, sizeof(payload));
  EXPECT_EQ(w, (int64_t)sizeof(payload));
  EXPECT_EQ(taosCloseFile(&pSrc), 0);

  int64_t copied = taosCopyFile(src, dst);
  EXPECT_EQ(copied, (int64_t)sizeof(payload));

  int64_t size = 0;
  EXPECT_EQ(taosStatFile(dst, &size, NULL, NULL), 0);
  EXPECT_EQ(size, (int64_t)sizeof(payload));

  taosRemoveFile(src);
  taosRemoveFile(dst);
}

TEST(osFileTests, taosCreateFileAutoMkDirAndRenameSuccess) {
  char dir[256] = {0};
  char src[256] = {0};
  char dst[256] = {0};
  buildTmpPath(dir, sizeof(dir), "td_osfile_nested_a");
  buildTmpPath(src, sizeof(src), "td_osfile_nested_a/td_osfile_src.txt");
  buildTmpPath(dst, sizeof(dst), "td_osfile_nested_a/td_osfile_dst.txt");

  taosRemoveFile(dst);
  taosRemoveFile(src);
  taosRemoveDir(dir);

  TdFilePtr p = taosCreateFile(src, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  ASSERT_NE(p, nullptr);
  EXPECT_EQ(taosWriteFile(p, "abc", 3), 3);
  EXPECT_EQ(taosCloseFile(&p), 0);

  EXPECT_EQ(taosRenameFile(src, dst), 0);

  int64_t size = 0, mtime = 0, atime = 0;
  EXPECT_EQ(taosStatFile(dst, &size, &mtime, &atime), 0);
  EXPECT_EQ(size, 3);
  EXPECT_GT(mtime, 0);
  EXPECT_GT(atime, 0);

  int64_t diskid = 0;
  EXPECT_EQ(taosGetFileDiskID(dst, &diskid), 0);

  taosRemoveFile(dst);
  taosRemoveDir(dir);
}

TEST(osFileTests, taosOpenFileForStreamAndCFileHappyPath) {
  char path[256] = {0};
  buildTmpPath(path, sizeof(path), "td_osfile_stream.txt");
  taosRemoveFile(path);

  FILE* f = taosOpenFileForStream(path, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_TEXT);
  ASSERT_NE(f, nullptr);
  (void)fprintf(f, "hello\n");
  EXPECT_EQ(taosCloseCFile(f), 0);

  f = taosOpenCFile(path, "r");
  ASSERT_NE(f, nullptr);
  EXPECT_EQ(taosSeekCFile(f, 0, SEEK_SET), 0);
  char buf[32] = {0};
  EXPECT_EQ(taosReadFromCFile(buf, 1, sizeof(buf) - 1, f) > 0, true);
  EXPECT_EQ(taosCloseCFile(f), 0);

  taosRemoveFile(path);
}

TEST(osFileTests, taosPosixReadWriteSeekStatLockUnlock) {
  char path[256] = {0};
  buildTmpPath(path, sizeof(path), "td_osfile_rw_ops.bin");
  taosRemoveFile(path);

  TdFilePtr p = taosOpenFile(path, TD_FILE_CREATE | TD_FILE_READ | TD_FILE_WRITE | TD_FILE_TRUNC);
  ASSERT_NE(p, nullptr);

  const char* s1 = "AAAA";
  const char* s2 = "BBBB";
  EXPECT_EQ(taosPWriteFile(p, s1, 4, 0), 4);
  EXPECT_EQ(taosPWriteFile(p, s2, 4, 4), 4);

  char out[9] = {0};
  EXPECT_EQ(taosPReadFile(p, out, 8, 0), 8);
  EXPECT_STREQ(out, "AAAABBBB");

  EXPECT_EQ(taosLSeekFile(p, 0, SEEK_SET), 0);

  int64_t size = 0, mtime = 0;
  EXPECT_EQ(taosFStatFile(p, &size, &mtime), 0);
  EXPECT_EQ(size, 8);

  EXPECT_EQ(taosLockFile(p), 0);
  EXPECT_EQ(taosUnLockFile(p), 0);

  EXPECT_EQ(taosFtruncateFile(p, 3), 0);
  EXPECT_EQ(taosFStatFile(p, &size, NULL), 0);
  EXPECT_EQ(size, 3);

  EXPECT_EQ(taosCloseFile(&p), 0);
  taosRemoveFile(path);
}

TEST(osFileTests, taosFSendFileSuccessLinuxPath) {
  char src[256] = {0};
  char dst[256] = {0};
  buildTmpPath(src, sizeof(src), "td_osfile_send_src.bin");
  buildTmpPath(dst, sizeof(dst), "td_osfile_send_dst.bin");

  taosRemoveFile(src);
  taosRemoveFile(dst);

  TdFilePtr in = taosOpenFile(src, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  ASSERT_NE(in, nullptr);
  char payload[2048];
  (void)memset(payload, 'z', sizeof(payload));
  EXPECT_EQ(taosWriteFile(in, payload, sizeof(payload)), (int64_t)sizeof(payload));
  EXPECT_EQ(taosCloseFile(&in), 0);

  in = taosOpenFile(src, TD_FILE_READ);
  TdFilePtr out = taosOpenFile(dst, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  ASSERT_NE(in, nullptr);
  ASSERT_NE(out, nullptr);

  int64_t off = 0;
  int64_t sent = taosFSendFile(out, in, &off, sizeof(payload));
  EXPECT_EQ(sent, (int64_t)sizeof(payload));

  EXPECT_EQ(taosCloseFile(&in), 0);
  EXPECT_EQ(taosCloseFile(&out), 0);

  int64_t size = 0;
  EXPECT_EQ(taosStatFile(dst, &size, NULL, NULL), 0);
  EXPECT_EQ(size, (int64_t)sizeof(payload));

  taosRemoveFile(src);
  taosRemoveFile(dst);
}

TEST(osFileTests, taosSetAutoDelFileErrorPath) {
  char path[256] = {0};
  buildTmpPath(path, sizeof(path), "td_osfile_autodel_missing.bin");
  taosRemoveFile(path);

  int ret = taosSetAutoDelFile(path);
#ifndef WINDOWS
  EXPECT_NE(ret, 0);
#endif
}