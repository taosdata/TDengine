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

// Include the header file containing the function declaration
#include "thash.h"
#include "ttypes.h"
#include "tmsg.h"
#include "commandInt.h"

// Define a fixture class for the unit tests
class BuildJsonTemplateTest : public ::testing::Test {
 protected:
  // Set up the test fixture
  void SetUp() override {
    // Initialize any required objects or variables before each test case
  }

  // Clean up the test fixture
  void TearDown() override {
    // Clean up any objects or variables created in the SetUp() function
  }
};

// Define test cases
TEST_F(BuildJsonTemplateTest, TemplateArrayIsNull) {
  // Arrange
  SHashObj* pHashJsonTemplate = nullptr;
  col_id_t colId = 0;
  char jsonBuf[100];
  int jsonBufLen = 100;

  // Act
  buildJsonTemplate(pHashJsonTemplate, colId, jsonBuf, jsonBufLen);

  // Assert
  // Check if the function returns without doing anything
  // Assert if necessary
}

TEST_F(BuildJsonTemplateTest, TemplateArrayIsEmpty) {
  // Arrange
  SHashObj* pHashJsonTemplate = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  col_id_t colId = 0;
  char jsonBuf[100];
  int jsonBufLen = 100;

  // Act
  buildJsonTemplate(pHashJsonTemplate, colId, jsonBuf, jsonBufLen);

  // Assert
  // Check if the function returns without doing anything
  // Assert if necessary
}

TEST_F(BuildJsonTemplateTest, TemplateArrayHasElements) {
  // Arrange
  SHashObj* pHashJsonTemplate = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_SMALLINT), true, HASH_ENTRY_LOCK);;
  col_id_t colId = 0;
  char jsonBuf[20];
  int jsonBufLen = 20;

  // Create and add template elements to the templateArray
  SArray* templateArray = taosArrayInit(1, sizeof(SJsonTemplate));
  SJsonTemplate pTemplate1 = {0};
  pTemplate1.isValidate = true;
  pTemplate1.templateId = 1;
  pTemplate1.templateJsonString = "Template1";
  SJsonTemplate pTemplate2 = {0};
  pTemplate2.isValidate = false;
  pTemplate2.templateId = 2;
  pTemplate2.templateJsonString = "Template2";
  SJsonTemplate pTemplate3 = {0};
  pTemplate2.isValidate = true;
  pTemplate2.templateId = 3;
  pTemplate2.templateJsonString = "Template3";
  taosArrayPush(templateArray, &pTemplate1);
  taosArrayPush(templateArray, &pTemplate2);
  taosArrayPush(templateArray, &pTemplate3);
  taosHashPut(pHashJsonTemplate, &colId, sizeof(col_id_t), &templateArray, POINTER_BYTES);

  // Act
  buildJsonTemplate(pHashJsonTemplate, colId, jsonBuf, jsonBufLen);
  printf("jsonBuf: %s\n", jsonBuf);
  ASSERT_EQ(jsonBuf, "1:Template1\n3:Templ");

  char jsonBuf2[100];
  int jsonBufLen2 = 100;
  buildJsonTemplate(pHashJsonTemplate, colId, jsonBuf2, jsonBufLen2);
  printf("jsonBuf: %s\n", jsonBuf2);
  ASSERT_EQ(jsonBuf2, "1:Template1\n3:Template3");
}

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
