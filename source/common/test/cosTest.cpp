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

#include <cos.h>
#include <taoserror.h>
#include <tglobal.h>
#include <iostream>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}

int32_t cosInitEnv() {
  int32_t code = 0;
  bool    isBlob = false;

  extern int8_t tsS3Ablob;
  extern char   tsS3Hostname[][TSDB_FQDN_LEN];
  extern char   tsS3AccessKeyId[][TSDB_FQDN_LEN];
  extern char   tsS3AccessKeySecret[][TSDB_FQDN_LEN];
  extern char   tsS3BucketName[TSDB_FQDN_LEN];

  tsS3Ablob = isBlob;
  /*
  const char *hostname = "endpoint/<account-name>.blob.core.windows.net";
  const char *accessKeyId = "<access-key-id/account-name>";
  const char *accessKeySecret = "<access-key-secret/account-key>";
  const char *bucketName = "<bucket/container-name>";
  */

  // const char *hostname = "http://192.168.1.52:9000";
  // const char *accessKeyId = "zOgllR6bSnw2Ah3mCNel";
  // const char *accessKeySecret = "cdO7oXAu3Cqdb1rUdevFgJMi0LtRwCXdWKQx4bhX";
  // const char *bucketName = "test-bucket";
  const char *hostname = "192.168.1.52:9000";
  const char *accessKeyId = "zOgllR6bSnw2Ah3mCNel";
  const char *accessKeySecret = "cdO7oXAu3Cqdb1rUdevFgJMi0LtRwCXdWKQx4bhX";
  const char *bucketName = "ci-bucket19";

  tstrncpy(&tsS3Hostname[0][0], hostname, TSDB_FQDN_LEN);
  tstrncpy(&tsS3AccessKeyId[0][0], accessKeyId, TSDB_FQDN_LEN);
  tstrncpy(&tsS3AccessKeySecret[0][0], accessKeySecret, TSDB_FQDN_LEN);
  tstrncpy(tsS3BucketName, bucketName, TSDB_FQDN_LEN);

  // setup s3 env
  extern int8_t tsS3EpNum;
  extern int8_t tsS3Https[TSDB_MAX_EP_NUM];

  tsS3EpNum = 1;
  tsS3Https[0] = false;

  tstrncpy(tsTempDir, "/tmp/", PATH_MAX);

  tsSsEnabled = true;

  return code;
}

TEST(testCase, cosCpPutError) {
  int32_t code = 0, lino = 0;

  char const *objectName = "testObject";

  EXPECT_EQ(cosInitEnv(), TSDB_CODE_SUCCESS);
  EXPECT_EQ(s3Begin(), TSDB_CODE_SUCCESS);

#if defined(USE_S3)
  EXPECT_EQ(s3Size(objectName), -1);
#else
  EXPECT_EQ(s3Size(objectName), 0);
#endif

  s3EvictCache("", 0);

  s3End();

  return;

_exit:
  std::cout << "code: " << code << std::endl;
}

TEST(testCase, cosCpPut) {
  int32_t code = 0, lino = 0;

  int8_t with_cp = 0;
  char  *data = nullptr;

  const long  objectSize = 65 * 1024 * 1024;
  char const *objectName = "cosut.bin";
  const char  object_name[] = "cosut.bin";

  EXPECT_EQ(std::string(object_name), objectName);

  EXPECT_EQ(cosInitEnv(), TSDB_CODE_SUCCESS);
  EXPECT_EQ(s3Begin(), TSDB_CODE_SUCCESS);

  {
    data = (char *)taosMemoryCalloc(1, objectSize);
    if (!data) {
      TAOS_CHECK_EXIT(terrno);
    }

    for (int i = 0; i < objectSize / 2; ++i) {
      data[i * 2 + 1] = 1;
    }

    char path[PATH_MAX] = {0};
    char path_download[PATH_MAX] = {0};
    int  ds_len = strlen(TD_DIRSEP);
    int  tmp_len = strlen(tsTempDir);

    (void)snprintf(path, PATH_MAX, "%s", tsTempDir);
    if (strncmp(tsTempDir + tmp_len - ds_len, TD_DIRSEP, ds_len) != 0) {
      (void)snprintf(path + tmp_len, PATH_MAX - tmp_len, "%s", TD_DIRSEP);
      (void)snprintf(path + tmp_len + ds_len, PATH_MAX - tmp_len - ds_len, "%s", object_name);
    } else {
      (void)snprintf(path + tmp_len, PATH_MAX - tmp_len, "%s", object_name);
    }

    tstrncpy(path_download, path, strlen(path) + 1);
    tstrncpy(path_download + strlen(path), ".download", strlen(".download") + 1);

    TdFilePtr fp = taosOpenFile(path, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_WRITE_THROUGH);
    GTEST_ASSERT_NE(fp, nullptr);

    int n = taosWriteFile(fp, data, objectSize);
    GTEST_ASSERT_EQ(n, objectSize);

    code = taosCloseFile(&fp);
    GTEST_ASSERT_EQ(code, 0);

    code = s3PutObjectFromFile2(path, objectName, with_cp);
    GTEST_ASSERT_EQ(code, 0);

    with_cp = 1;
    code = s3PutObjectFromFile2(path, objectName, with_cp);
    GTEST_ASSERT_EQ(code, 0);

#if defined(USE_S3)
    EXPECT_EQ(s3Size(objectName), objectSize);
#else
    EXPECT_EQ(s3Size(objectName), 0);
#endif

    s3End();
    s3EvictCache("", 0);

    taosMemoryFree(data);

    EXPECT_EQ(taosRemoveFile(path), TSDB_CODE_SUCCESS);
  }

  return;

_exit:
  if (data) {
    taosMemoryFree(data);
    s3End();
  }

  std::cout << "code: " << code << std::endl;
}
