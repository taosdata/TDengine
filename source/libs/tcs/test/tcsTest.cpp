#include <gtest/gtest.h>
#include <cstring>
#include <iostream>
#include <queue>

#include "tcs.h"
#include "tcsInt.h"

int32_t tcsInitEnv(int8_t isBlob) {
  int32_t code = 0;

  extern int8_t tsS3EpNum;

  extern char tsS3Hostname[][TSDB_FQDN_LEN];
  extern char tsS3AccessKeyId[][TSDB_FQDN_LEN];
  extern char tsS3AccessKeySecret[][TSDB_FQDN_LEN];
  extern char tsS3BucketName[TSDB_FQDN_LEN];

  /* TCS parameter format
  tsS3Hostname[0] = "endpoint/<account-name>.blob.core.windows.net";
  tsS3AccessKeyId[0] = "<access-key-id/account-name>";
  tsS3AccessKeySecret[0] = "<access-key-secret/account-key>";
  tsS3BucketName = "<bucket/container-name>";
  */

  tsS3Ablob = isBlob;
  if (isBlob) {
    const char *hostname = "endpoint/<account-name>.blob.core.windows.net";
    const char *accessKeyId = "<access-key-id/account-name>";
    const char *accessKeySecret = "<access-key-secret/account-key>";
    const char *bucketName = "<bucket/container-name>";

    tstrncpy(&tsS3Hostname[0][0], hostname, TSDB_FQDN_LEN);
    tstrncpy(&tsS3AccessKeyId[0][0], accessKeyId, TSDB_FQDN_LEN);
    tstrncpy(&tsS3AccessKeySecret[0][0], accessKeySecret, TSDB_FQDN_LEN);
    tstrncpy(tsS3BucketName, bucketName, TSDB_FQDN_LEN);

  } else {
    const char *hostname = "endpoint/<account-name>.blob.core.windows.net";
    const char *accessKeyId = "<access-key-id/account-name>";
    const char *accessKeySecret = "<access-key-secret/account-key>";
    const char *bucketName = "<bucket/container-name>";

    tstrncpy(&tsS3Hostname[0][0], hostname, TSDB_FQDN_LEN);
    tstrncpy(&tsS3AccessKeyId[0][0], accessKeyId, TSDB_FQDN_LEN);
    tstrncpy(&tsS3AccessKeySecret[0][0], accessKeySecret, TSDB_FQDN_LEN);
    tstrncpy(tsS3BucketName, bucketName, TSDB_FQDN_LEN);

    // setup s3 env
    tsS3EpNum = 1;
  }

  tstrncpy(tsTempDir, "/tmp/", PATH_MAX);

  tsS3Enabled = true;
  if (!tsS3Ablob) {
  }

  return code;
}

TEST(TcsTest, DISABLED_InterfaceTest) {
  // TEST(TcsTest, InterfaceTest) {
  int code = 0;

  code = tcsInitEnv(true);
  GTEST_ASSERT_EQ(code, 0);
  GTEST_ASSERT_EQ(tsS3Enabled, 1);
  GTEST_ASSERT_EQ(tsS3Ablob, 1);

  code = tcsInit();
  GTEST_ASSERT_EQ(code, 0);

  code = tcsCheckCfg();
  GTEST_ASSERT_EQ(code, 0);
  /*
  code = tcsPutObjectFromFileOffset(file, object_name, offset, size);
  GTEST_ASSERT_EQ(code, 0);
  code = tcsGetObjectBlock(object_name, offset, size, check, ppBlock);
  GTEST_ASSERT_EQ(code, 0);

  tcsDeleteObjectsByPrefix(prefix);
  // list object to check

  code = tcsPutObjectFromFile2(file, object, withcp);
  GTEST_ASSERT_EQ(code, 0);
  code = tcsGetObjectsByPrefix(prefix, path);
  GTEST_ASSERT_EQ(code, 0);
  code = tcsDeleteObjects(object_name, nobject);
  GTEST_ASSERT_EQ(code, 0);
  code = tcsGetObjectToFile(object_name, fileName);
  GTEST_ASSERT_EQ(code, 0);

  // GTEST_ASSERT_NE(pEnv, nullptr);
  */

  tcsUninit();
}

TEST(TcsTest, DISABLED_InterfaceNonBlobTest) {
  // TEST(TcsTest, InterfaceNonBlobTest) {
  int code = 0;

  code = tcsInitEnv(false);
  GTEST_ASSERT_EQ(code, 0);
  GTEST_ASSERT_EQ(tsS3Enabled, 1);
  GTEST_ASSERT_EQ(tsS3Ablob, 0);

  code = tcsInit();
  GTEST_ASSERT_EQ(code, 0);

  code = tcsCheckCfg();
  GTEST_ASSERT_EQ(code, 0);
  /*
  code = tcsPutObjectFromFileOffset(file, object_name, offset, size);
  GTEST_ASSERT_EQ(code, 0);
  code = tcsGetObjectBlock(object_name, offset, size, check, ppBlock);
  GTEST_ASSERT_EQ(code, 0);

  tcsDeleteObjectsByPrefix(prefix);
  // list object to check

  code = tcsPutObjectFromFile2(file, object, withcp);
  GTEST_ASSERT_EQ(code, 0);
  code = tcsGetObjectsByPrefix(prefix, path);
  GTEST_ASSERT_EQ(code, 0);
  code = tcsDeleteObjects(object_name, nobject);
  GTEST_ASSERT_EQ(code, 0);
  code = tcsGetObjectToFile(object_name, fileName);
  GTEST_ASSERT_EQ(code, 0);

  // GTEST_ASSERT_NE(pEnv, nullptr);
  */

  tcsUninit();
}
