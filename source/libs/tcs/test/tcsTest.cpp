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

#include <cstring>
#include <iostream>
#include <queue>

#include "tcs.h"
#include "tcsInt.h"

int32_t tcsInitEnv(int8_t isBlob) {
  int32_t code = 0;

  extern char tsS3Hostname[][TSDB_FQDN_LEN];
  extern char tsS3AccessKeyId[][TSDB_FQDN_LEN];
  extern char tsS3AccessKeySecret[][TSDB_FQDN_LEN];
  extern char tsS3BucketName[TSDB_FQDN_LEN];

  /* TCS parameter format
  tsS3Hostname[0] = "<endpoint>/<account-name>.blob.core.windows.net";
  tsS3AccessKeyId[0] = "<access-key-id/account-name>";
  tsS3AccessKeySecret[0] = "<access-key-secret/account-key>";
  tsS3BucketName = "<bucket/container-name>";
  */

  tsS3Ablob = isBlob;
  if (isBlob) {
    const char *hostname = "<endpoint>/<account-name>.blob.core.windows.net";
    const char *accessKeyId = "<access-key-id/account-name>";
    const char *accessKeySecret = "<access-key-secret/account-key>";
    const char *bucketName = "<bucket/container-name>";

    if (hostname[0] != '<') {
      tstrncpy(&tsS3Hostname[0][0], hostname, TSDB_FQDN_LEN);
      tstrncpy(&tsS3AccessKeyId[0][0], accessKeyId, TSDB_FQDN_LEN);
      tstrncpy(&tsS3AccessKeySecret[0][0], accessKeySecret, TSDB_FQDN_LEN);
      tstrncpy(tsS3BucketName, bucketName, TSDB_FQDN_LEN);
    } else {
      const char *accountId = getenv("ablob_account_id");
      if (!accountId) {
        return -1;
      }

      const char *accountSecret = getenv("ablob_account_secret");
      if (!accountSecret) {
        return -1;
      }

      const char *containerName = getenv("ablob_container");
      if (!containerName) {
        return -1;
      }

      TAOS_STRCPY(&tsS3Hostname[0][0], accountId);
      TAOS_STRCAT(&tsS3Hostname[0][0], ".blob.core.windows.net");
      TAOS_STRCPY(&tsS3AccessKeyId[0][0], accountId);
      TAOS_STRCPY(&tsS3AccessKeySecret[0][0], accountSecret);
      TAOS_STRCPY(tsS3BucketName, containerName);
    }
  } else {
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
  }

  tstrncpy(tsTempDir, "/tmp/", PATH_MAX);

  tsS3Enabled = true;

  return code;
}

// TEST(TcsTest, DISABLED_InterfaceTest) {
TEST(TcsTest, InterfaceTest) {
  int  code = 0;
  bool check = false;
  bool withcp = false;

  code = tcsInitEnv(true);
  if (code) {
    std::cout << "ablob env init failed with: " << code << std::endl;
    return;
  }

  GTEST_ASSERT_EQ(code, 0);
  GTEST_ASSERT_EQ(tsS3Enabled, 1);
  GTEST_ASSERT_EQ(tsS3Ablob, 1);

  code = tcsInit();
  GTEST_ASSERT_EQ(code, 0);

  code = tcsCheckCfg();
  GTEST_ASSERT_EQ(code, 0);

  const int size = 4096;
  char      data[size] = {0};
  for (int i = 0; i < size / 2; ++i) {
    data[i * 2 + 1] = 1;
  }

  const char object_name[] = "tcsut.bin";
  char       path[PATH_MAX] = {0};
  char       path_download[PATH_MAX] = {0};
  int        ds_len = strlen(TD_DIRSEP);
  int        tmp_len = strlen(tsTempDir);

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

  int n = taosWriteFile(fp, data, size);
  GTEST_ASSERT_EQ(n, size);

  code = taosCloseFile(&fp);
  GTEST_ASSERT_EQ(code, 0);

  code = tcsPutObjectFromFileOffset(path, object_name, 0, size);
  GTEST_ASSERT_EQ(code, 0);

  uint8_t *pBlock = NULL;
  code = tcsGetObjectBlock(object_name, 0, size, check, &pBlock);
  GTEST_ASSERT_EQ(code, 0);

  for (int i = 0; i < size / 2; ++i) {
    GTEST_ASSERT_EQ(pBlock[i * 2], 0);
    GTEST_ASSERT_EQ(pBlock[i * 2 + 1], 1);
  }

  taosMemoryFree(pBlock);

  code = tcsGetObjectToFile(object_name, path_download);
  GTEST_ASSERT_EQ(code, 0);

  {
    TdFilePtr fp = taosOpenFile(path, TD_FILE_READ);
    GTEST_ASSERT_NE(fp, nullptr);

    (void)memset(data, 0, size);

    int64_t n = taosReadFile(fp, data, size);
    GTEST_ASSERT_EQ(n, size);

    code = taosCloseFile(&fp);
    GTEST_ASSERT_EQ(code, 0);

    for (int i = 0; i < size / 2; ++i) {
      GTEST_ASSERT_EQ(data[i * 2], 0);
      GTEST_ASSERT_EQ(data[i * 2 + 1], 1);
    }
  }

  tcsDeleteObjectsByPrefix(object_name);
  // list object to check

  code = tcsPutObjectFromFile2(path, object_name, withcp);
  GTEST_ASSERT_EQ(code, 0);

  code = tcsGetObjectsByPrefix(object_name, tsTempDir);
  GTEST_ASSERT_EQ(code, 0);

  {
    TdFilePtr fp = taosOpenFile(path, TD_FILE_READ);
    GTEST_ASSERT_NE(fp, nullptr);

    (void)memset(data, 0, size);

    int64_t n = taosReadFile(fp, data, size);
    GTEST_ASSERT_EQ(n, size);

    code = taosCloseFile(&fp);
    GTEST_ASSERT_EQ(code, 0);

    for (int i = 0; i < size / 2; ++i) {
      GTEST_ASSERT_EQ(data[i * 2], 0);
      GTEST_ASSERT_EQ(data[i * 2 + 1], 1);
    }
  }

  const char *object_name_arr[] = {object_name};
  code = tcsDeleteObjects(object_name_arr, 1);
  GTEST_ASSERT_EQ(code, 0);

  tcsUninit();
}

// TEST(TcsTest, DISABLED_InterfaceNonBlobTest) {
TEST(TcsTest, InterfaceNonBlobTest) {
#ifndef TD_ENTERPRISE
  // NOTE: this test case will coredump for community edition of taos
  //       thus we bypass this test case for the moment
  // code = tcsGetObjectBlock(object_name, 0, size, check, &pBlock);
  // tcsGetObjectBlock succeeded but pBlock is nullptr
  // which results in nullptr-access-coredump shortly after
#else
  int  code = 0;
  bool check = false;
  bool withcp = false;

  code = tcsInitEnv(false);
  GTEST_ASSERT_EQ(code, 0);
  GTEST_ASSERT_EQ(tsS3Enabled, 1);
  GTEST_ASSERT_EQ(tsS3Ablob, 0);

  code = tcsInit();
  GTEST_ASSERT_EQ(code, 0);

  code = tcsCheckCfg();
  GTEST_ASSERT_EQ(code, 0);

  const int size = 4096;
  char      data[size] = {0};
  for (int i = 0; i < size / 2; ++i) {
    data[i * 2 + 1] = 1;
  }

  const char object_name[] = "tcsut.bin";
  char       path[PATH_MAX] = {0};
  char       path_download[PATH_MAX] = {0};
  int        ds_len = strlen(TD_DIRSEP);
  int        tmp_len = strlen(tsTempDir);

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

  int n = taosWriteFile(fp, data, size);
  GTEST_ASSERT_EQ(n, size);

  code = taosCloseFile(&fp);
  GTEST_ASSERT_EQ(code, 0);

  code = tcsPutObjectFromFileOffset(path, object_name, 0, size);
  GTEST_ASSERT_EQ(code, 0);

  uint8_t *pBlock = NULL;
  code = tcsGetObjectBlock(object_name, 0, size, check, &pBlock);
  GTEST_ASSERT_EQ(code, 0);
  if (pBlock) {
    for (int i = 0; i < size / 2; ++i) {
      GTEST_ASSERT_EQ(pBlock[i * 2], 0);
      GTEST_ASSERT_EQ(pBlock[i * 2 + 1], 1);
    }
  }
  taosMemoryFree(pBlock);

  code = tcsGetObjectToFile(object_name, path_download);
  GTEST_ASSERT_EQ(code, 0);

  {
    TdFilePtr fp = taosOpenFile(path, TD_FILE_READ);
    GTEST_ASSERT_NE(fp, nullptr);

    (void)memset(data, 0, size);

    int64_t n = taosReadFile(fp, data, size);
    GTEST_ASSERT_EQ(n, size);

    code = taosCloseFile(&fp);
    GTEST_ASSERT_EQ(code, 0);

    for (int i = 0; i < size / 2; ++i) {
      GTEST_ASSERT_EQ(data[i * 2], 0);
      GTEST_ASSERT_EQ(data[i * 2 + 1], 1);
    }
  }

  tcsDeleteObjectsByPrefix(object_name);
  // list object to check

  code = tcsPutObjectFromFile2(path, object_name, withcp);
  GTEST_ASSERT_EQ(code, 0);

  code = tcsGetObjectsByPrefix(object_name, tsTempDir);
  GTEST_ASSERT_EQ(code, 0);

  {
    TdFilePtr fp = taosOpenFile(path, TD_FILE_READ);
    GTEST_ASSERT_NE(fp, nullptr);

    (void)memset(data, 0, size);

    int64_t n = taosReadFile(fp, data, size);
    GTEST_ASSERT_EQ(n, size);

    code = taosCloseFile(&fp);
    GTEST_ASSERT_EQ(code, 0);

    for (int i = 0; i < size / 2; ++i) {
      GTEST_ASSERT_EQ(data[i * 2], 0);
      GTEST_ASSERT_EQ(data[i * 2 + 1], 1);
    }
  }

  const char *object_name_arr[] = {object_name};
  code = tcsDeleteObjects(object_name_arr, 1);
  GTEST_ASSERT_EQ(code, 0);

  tcsUninit();
#endif
}
