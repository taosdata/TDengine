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
  const char *accessKeyId = "fGPPyYjzytw05nw44ViA";
  const char *accessKeySecret = "vK1VcwxgSOykicx6hk8fL1x15uEtyDSFU3w4hTaZ";

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

  tsS3Enabled = true;

  return code;
}

TEST(testCase, cosCpPutError) {
  int32_t code = 0, lino = 0;

  long        objectSize = 128 * 1024 * 1024;
  char const *objectName = "testObject";

  EXPECT_EQ(cosInitEnv(), TSDB_CODE_SUCCESS);
  EXPECT_EQ(s3Size(objectName), -1);
  s3EvictCache("", 0);

  return;

_exit:
  std::cout << "code: " << code << std::endl;
}

TEST(testCase, cosCpPut) {
  int32_t code = 0, lino = 0;

  long        objectSize = 128 * 1024 * 1024;
  char const *objectName = "testObject";

  EXPECT_EQ(cosInitEnv(), TSDB_CODE_SUCCESS);
  EXPECT_EQ(s3Size(objectName), -1);
  s3EvictCache("", 0);

  return;

_exit:
  std::cout << "code: " << code << std::endl;
}

TEST(testCase, cosCpPutSize) {
  int32_t code = 0, lino = 0;

  long        objectSize = 128 * 1024 * 1024;
  char const *objectName = "testObject";

  EXPECT_EQ(cosInitEnv(), TSDB_CODE_SUCCESS);
  EXPECT_EQ(s3Size(objectName), -1);
  s3EvictCache("", 0);

  return;

_exit:
  std::cout << "code: " << code << std::endl;
}
