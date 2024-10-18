#include <gtest/gtest.h>
#include <cstring>
#include <iostream>
#include <queue>

#include "az.h"

extern int8_t tsS3Enabled;

int32_t azInitEnv() {
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

  const char *hostname = "endpoint/<account-name>.blob.core.windows.net";
  const char *accessKeyId = "<access-key-id/account-name>";
  const char *accessKeySecret = "<access-key-secret/account-key>";
  const char *bucketName = "<bucket/container-name>";

  tstrncpy(&tsS3Hostname[0][0], hostname, TSDB_FQDN_LEN);
  tstrncpy(&tsS3AccessKeyId[0][0], accessKeyId, TSDB_FQDN_LEN);
  tstrncpy(&tsS3AccessKeySecret[0][0], accessKeySecret, TSDB_FQDN_LEN);
  tstrncpy(tsS3BucketName, bucketName, TSDB_FQDN_LEN);

  tstrncpy(tsTempDir, "/tmp/", PATH_MAX);

  tsS3Enabled = true;

  return code;
}

TEST(AzTest, DISABLED_InterfaceTest) {
  // TEST(AzTest, InterfaceTest) {
  int code = 0;

  code = azInitEnv();
  GTEST_ASSERT_EQ(code, 0);
  GTEST_ASSERT_EQ(tsS3Enabled, 1);

  code = azBegin();
  GTEST_ASSERT_EQ(code, 0);

  code = azCheckCfg();
  GTEST_ASSERT_EQ(code, 0);
  /*
  code = azPutObjectFromFileOffset(file, object_name, offset, size);
  GTEST_ASSERT_EQ(code, 0);
  code = azGetObjectBlock(object_name, offset, size, check, ppBlock);
  GTEST_ASSERT_EQ(code, 0);

  azDeleteObjectsByPrefix(prefix);
  // list object to check

  code = azPutObjectFromFile2(file, object, withcp);
  GTEST_ASSERT_EQ(code, 0);
  code = azGetObjectsByPrefix(prefix, path);
  GTEST_ASSERT_EQ(code, 0);
  code = azDeleteObjects(object_name, nobject);
  GTEST_ASSERT_EQ(code, 0);
  code = azGetObjectToFile(object_name, fileName);
  GTEST_ASSERT_EQ(code, 0);

  // GTEST_ASSERT_NE(pEnv, nullptr);
  */

  azEnd();
}
