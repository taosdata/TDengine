#include <gtest/gtest.h>
#include <cJSON.h>
#include "tjson.h"

TEST(CheckJsonTemplateTest, ArraySizeNotOne) {
  cJSON *json = cJSON_CreateArray();
  cJSON_AddItemToArray(json, cJSON_CreateString("test1"));
  cJSON_AddItemToArray(json, cJSON_CreateString("test2"));

  int32_t result = checkJsonTemplate(json);

  EXPECT_EQ(result, TSDB_CODE_TEMPLATE_ARRAY_ONLY_ONE_TYPE);

  cJSON_Delete(json);
}

TEST(CheckJsonTemplateTest, ArrayValid) {
  cJSON *json = cJSON_CreateArray();
  cJSON_AddItemToArray(json, cJSON_CreateString("string"));

  int32_t result = checkJsonTemplate(json);

  EXPECT_EQ(result, TSDB_CODE_SUCCESS);

  cJSON_Delete(json);
}

TEST(CheckJsonTemplateTest, ObjectValid) {
  cJSON *json = cJSON_CreateObject();
  cJSON_AddStringToObject(json, "key1", "double");
  cJSON_AddStringToObject(json, "key2", "long");

  int32_t result = checkJsonTemplate(json);

  EXPECT_EQ(result, TSDB_CODE_SUCCESS);

  cJSON_Delete(json);
}

TEST(CheckJsonTemplateTest, ValueInvalid) {
  cJSON *json = cJSON_CreateString("invalid");

  int32_t result = checkJsonTemplate(json);

  EXPECT_EQ(result, TSDB_CODE_TEMPLATE_VALUE_INVALIDATE);

  cJSON_Delete(json);
}

TEST(CheckJsonTemplateTest, ValueValid) {
  cJSON *json = cJSON_CreateString("boolean");

  int32_t result = checkJsonTemplate(json);

  EXPECT_EQ(result, TSDB_CODE_SUCCESS);

  cJSON_Delete(json);
}

TEST(CheckJsonTemplateTest, Value1) {
  char *tmp = "{\n"
      "            \"k1\": \"string\",\n"
      "            \"k2\": \"long\",\n"
      "            \"k3\": [\"double\"],\n"
      "            \"k4\": \"long\",\n"
      "            \"k5\": {\n"
      "                \"k6\": \"boolean\",\n"
      "                \"k7\": \"double\"\n"
      "                },\n"
      "            \"k8\": \"string\"\n"
      "        }";
  cJSON *json = cJSON_Parse(tmp);

  int32_t result = checkJsonTemplate(json);

  EXPECT_EQ(result, TSDB_CODE_SUCCESS);

  cJSON_Delete(json);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}