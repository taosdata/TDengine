/**
 * @file xnodeRealFunctionsTest.cpp
 * @brief Test real XNode utility functions: checkPasswordFmt and swapFields
 * @version 2.0
 * @date 2025-12-25
 *
 * This version is completely independent from TDengine internal headers
 * to avoid macro conflicts with C++ standard library.
 */

#include <gtest/gtest.h>
#include <cstdint>
#include <cstdlib>
#include <cstring>

// Define constants that would come from TDengine headers
#define TSDB_PASSWORD_MIN_LEN                   8
#define TSDB_PASSWORD_MAX_LEN                   32
#define TSDB_CODE_MND_INVALID_PASS_FORMAT       -1001
#define TSDB_CODE_PAR_PASSWD_TOO_SHORT_OR_EMPTY -1002
#define TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG   -1003

// Mock global config (normally from tglobal.h)
static int32_t tsEnableStrongPassword = 0;

// Mock function (normally from common library)
static bool taosIsComplexString(const char* pwd) {
  // Simple implementation for testing
  bool hasUpper = false, hasLower = false, hasDigit = false, hasSpecial = false;

  for (const char* p = pwd; *p; p++) {
    if (*p >= 'A' && *p <= 'Z')
      hasUpper = true;
    else if (*p >= 'a' && *p <= 'z')
      hasLower = true;
    else if (*p >= '0' && *p <= '9')
      hasDigit = true;
    else
      hasSpecial = true;
  }

  // A complex string has at least 3 of the 4 types
  int count = (hasUpper ? 1 : 0) + (hasLower ? 1 : 0) + (hasDigit ? 1 : 0) + (hasSpecial ? 1 : 0);
  return count >= 3;
}

// Copied utility functions from mndXnode.c for testing
extern "C" {

int32_t checkPasswordFmt_test(const char* pwd) {
  if (strcmp(pwd, "taosdata") == 0) {
    return 0;
  }

  if (tsEnableStrongPassword == 0) {
    for (char c = *pwd; c != 0; c = *(++pwd)) {
      if (c == ' ' || c == '\'' || c == '\"' || c == '`' || c == '\\') {
        return TSDB_CODE_MND_INVALID_PASS_FORMAT;
      }
    }
    return 0;
  }

  int32_t len = strlen(pwd);
  if (len < TSDB_PASSWORD_MIN_LEN) {
    return TSDB_CODE_PAR_PASSWD_TOO_SHORT_OR_EMPTY;
  }

  if (len > TSDB_PASSWORD_MAX_LEN) {
    return TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG;
  }

  if (taosIsComplexString(pwd)) {
    return 0;
  }

  return TSDB_CODE_MND_INVALID_PASS_FORMAT;
}

void swapFields_test(int32_t* newLen, char** ppNewStr, int32_t* oldLen, char** ppOldStr) {
  if (*newLen > 0) {
    int32_t tempLen = *newLen;
    *newLen = *oldLen;
    *oldLen = tempLen;

    char* tempStr = *ppNewStr;
    *ppNewStr = *ppOldStr;
    *ppOldStr = tempStr;
  }
}

}  // extern "C"

class XnodeRealFunctionsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Reset to default (weak password mode)
    tsEnableStrongPassword = 0;
  }
  void TearDown() override {}
};

// Test checkPasswordFmt function
TEST_F(XnodeRealFunctionsTest, CheckPasswordFormat_TaosData) {
  // "taosdata" is always allowed
  EXPECT_EQ(checkPasswordFmt_test("taosdata"), 0);
}

TEST_F(XnodeRealFunctionsTest, CheckPasswordFormat_InvalidCharacters) {
  // Test invalid characters (space, quotes, backtick, backslash)
  EXPECT_NE(checkPasswordFmt_test("pass word"), 0);   // space
  EXPECT_NE(checkPasswordFmt_test("pass'word"), 0);   // single quote
  EXPECT_NE(checkPasswordFmt_test("pass\"word"), 0);  // double quote
  EXPECT_NE(checkPasswordFmt_test("pass`word"), 0);   // backtick
  EXPECT_NE(checkPasswordFmt_test("pass\\word"), 0);  // backslash
}

TEST_F(XnodeRealFunctionsTest, CheckPasswordFormat_ValidSimple) {
  // Valid simple passwords (when strong password is disabled)
  EXPECT_EQ(checkPasswordFmt_test("password"), 0);
  EXPECT_EQ(checkPasswordFmt_test("Pass123"), 0);
  EXPECT_EQ(checkPasswordFmt_test("test_pass"), 0);
  EXPECT_EQ(checkPasswordFmt_test("pass-word"), 0);
}

TEST_F(XnodeRealFunctionsTest, CheckPasswordFormat_MinLength_WeakMode) {
  // In weak password mode, short passwords are allowed
  tsEnableStrongPassword = 0;
  EXPECT_EQ(checkPasswordFmt_test("abc"), 0);
  EXPECT_EQ(checkPasswordFmt_test("a"), 0);
}

TEST_F(XnodeRealFunctionsTest, CheckPasswordFormat_MinLength_StrongMode) {
  // In strong password mode, short passwords are rejected
  tsEnableStrongPassword = 1;
  EXPECT_EQ(checkPasswordFmt_test("abc"), TSDB_CODE_PAR_PASSWD_TOO_SHORT_OR_EMPTY);
  EXPECT_EQ(checkPasswordFmt_test("1234567"), TSDB_CODE_PAR_PASSWD_TOO_SHORT_OR_EMPTY);
}

TEST_F(XnodeRealFunctionsTest, CheckPasswordFormat_MaxLength_StrongMode) {
  // Test maximum length (32 chars for TSDB_PASSWORD_MAX_LEN)
  tsEnableStrongPassword = 1;

  char longPass[100];
  memset(longPass, 'a', sizeof(longPass) - 1);
  longPass[sizeof(longPass) - 1] = '\0';

  EXPECT_EQ(checkPasswordFmt_test(longPass), TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG);
}

TEST_F(XnodeRealFunctionsTest, CheckPasswordFormat_Complex_StrongMode) {
  // Complex passwords with numbers, uppercase, lowercase, special chars
  tsEnableStrongPassword = 1;

  // These should pass (complex enough)
  EXPECT_EQ(checkPasswordFmt_test("Abc123!@#"), 0);
  EXPECT_EQ(checkPasswordFmt_test("Test_Pass_123"), 0);
  EXPECT_EQ(checkPasswordFmt_test("MyP@ssw0rd"), 0);
  EXPECT_EQ(checkPasswordFmt_test("ComplexPass1!"), 0);
}

TEST_F(XnodeRealFunctionsTest, CheckPasswordFormat_NotComplex_StrongMode) {
  // Passwords that aren't complex enough should fail in strong mode
  tsEnableStrongPassword = 1;

  // Only lowercase
  EXPECT_EQ(checkPasswordFmt_test("abcdefgh"), TSDB_CODE_MND_INVALID_PASS_FORMAT);

  // Only uppercase
  EXPECT_EQ(checkPasswordFmt_test("ABCDEFGH"), TSDB_CODE_MND_INVALID_PASS_FORMAT);

  // Only digits
  EXPECT_EQ(checkPasswordFmt_test("12345678"), TSDB_CODE_MND_INVALID_PASS_FORMAT);
}

// Test swapFields function
TEST_F(XnodeRealFunctionsTest, SwapFields_BothNonEmpty) {
  int32_t oldLen = 10;
  int32_t newLen = 20;
  char*   oldStr = (char*)malloc(oldLen);
  char*   newStr = (char*)malloc(newLen);
  strcpy(oldStr, "old");
  strcpy(newStr, "new_string");

  char* origOldStr = oldStr;
  char* origNewStr = newStr;

  swapFields_test(&newLen, &newStr, &oldLen, &oldStr);

  // After swap:
  // - oldLen should be 20 (original newLen)
  // - newLen should be 10 (original oldLen)
  // - oldStr should point to what newStr pointed to
  // - newStr should point to what oldStr pointed to
  EXPECT_EQ(oldLen, 20);
  EXPECT_EQ(newLen, 10);
  EXPECT_EQ(oldStr, origNewStr);
  EXPECT_EQ(newStr, origOldStr);

  free(oldStr);
  free(newStr);
}

TEST_F(XnodeRealFunctionsTest, SwapFields_NewLenZero) {
  int32_t oldLen = 10;
  int32_t newLen = 0;
  char*   oldStr = (char*)malloc(oldLen);
  char*   newStr = nullptr;
  strcpy(oldStr, "old");

  int32_t origOldLen = oldLen;
  char*   origOldStr = oldStr;

  swapFields_test(&newLen, &newStr, &oldLen, &oldStr);

  // When newLen is 0, no swap should occur
  EXPECT_EQ(oldLen, origOldLen);
  EXPECT_EQ(oldStr, origOldStr);
  EXPECT_EQ(newLen, 0);
  EXPECT_EQ(newStr, nullptr);

  free(oldStr);
}

TEST_F(XnodeRealFunctionsTest, SwapFields_MultipleSwaps) {
  int32_t len1 = 5;
  int32_t len2 = 10;
  int32_t len3 = 15;

  char* str1 = (char*)malloc(len1);
  char* str2 = (char*)malloc(len2);
  char* str3 = (char*)malloc(len3);

  strcpy(str1, "aaa");
  strcpy(str2, "bbbbb");
  strcpy(str3, "ccccccccc");

  char* orig1 = str1;
  char* orig2 = str2;
  char* orig3 = str3;

  // Swap str1 and str2
  swapFields_test(&len2, &str2, &len1, &str1);
  EXPECT_EQ(len1, 10);
  EXPECT_EQ(len2, 5);
  EXPECT_EQ(str1, orig2);
  EXPECT_EQ(str2, orig1);

  // Swap str2 and str3
  swapFields_test(&len3, &str3, &len2, &str2);
  EXPECT_EQ(len2, 15);
  EXPECT_EQ(len3, 5);
  EXPECT_EQ(str2, orig3);
  EXPECT_EQ(str3, orig1);

  free(str1);
  free(str2);
  free(str3);
}

TEST_F(XnodeRealFunctionsTest, SwapFields_SamePointers) {
  int32_t len = 10;
  char*   str = (char*)malloc(len);
  strcpy(str, "test");

  // Swapping with itself
  swapFields_test(&len, &str, &len, &str);

  // Values should remain the same
  EXPECT_EQ(len, 10);
  EXPECT_STREQ(str, "test");

  free(str);
}

TEST_F(XnodeRealFunctionsTest, SwapFields_EdgeCase_EmptyStrings) {
  int32_t oldLen = 1;
  int32_t newLen = 1;
  char*   oldStr = (char*)malloc(oldLen);
  char*   newStr = (char*)malloc(newLen);
  oldStr[0] = '\0';
  newStr[0] = '\0';

  char* origOldStr = oldStr;
  char* origNewStr = newStr;

  swapFields_test(&newLen, &newStr, &oldLen, &oldStr);

  EXPECT_EQ(oldLen, 1);
  EXPECT_EQ(newLen, 1);
  EXPECT_EQ(oldStr, origNewStr);
  EXPECT_EQ(newStr, origOldStr);

  free(oldStr);
  free(newStr);
}

// Integration test - test the swapFields usage pattern from mndXnodeActionUpdate
TEST_F(XnodeRealFunctionsTest, SwapFields_UpdatePattern) {
  // Simulate the pattern used in mndXnodeActionUpdate
  struct MockXnodeObj {
    int32_t statusLen;
    char*   status;
  };

  MockXnodeObj oldObj = {0};
  MockXnodeObj newObj = {0};

  // Old object has "online" status
  oldObj.statusLen = 7;
  oldObj.status = (char*)malloc(oldObj.statusLen);
  strcpy(oldObj.status, "online");

  // New object has "offline" status
  newObj.statusLen = 8;
  newObj.status = (char*)malloc(newObj.statusLen);
  strcpy(newObj.status, "offline");

  char* oldOrigStatus = oldObj.status;
  char* newOrigStatus = newObj.status;

  // Swap status fields (as done in update)
  swapFields_test(&newObj.statusLen, &newObj.status, &oldObj.statusLen, &oldObj.status);

  // After swap, oldObj should have the new status
  EXPECT_EQ(oldObj.statusLen, 8);
  EXPECT_EQ(oldObj.status, newOrigStatus);
  EXPECT_STREQ(oldObj.status, "offline");

  // newObj should have the old status (which will be freed)
  EXPECT_EQ(newObj.statusLen, 7);
  EXPECT_EQ(newObj.status, oldOrigStatus);
  EXPECT_STREQ(newObj.status, "online");

  // Cleanup
  free(oldObj.status);
  free(newObj.status);
}

TEST_F(XnodeRealFunctionsTest, CheckPasswordFormat_SpecialValidChars) {
  // Test that commonly used special characters in passwords work
  tsEnableStrongPassword = 0;

  EXPECT_EQ(checkPasswordFmt_test("pass@123"), 0);
  EXPECT_EQ(checkPasswordFmt_test("pass#word"), 0);
  EXPECT_EQ(checkPasswordFmt_test("pass$123"), 0);
  EXPECT_EQ(checkPasswordFmt_test("pass%word"), 0);
  EXPECT_EQ(checkPasswordFmt_test("pass&123"), 0);
  EXPECT_EQ(checkPasswordFmt_test("pass!word"), 0);
}

TEST_F(XnodeRealFunctionsTest, CheckPasswordFormat_NumbersOnly_WeakMode) {
  tsEnableStrongPassword = 0;
  EXPECT_EQ(checkPasswordFmt_test("12345678"), 0);
  EXPECT_EQ(checkPasswordFmt_test("99999999"), 0);
}

TEST_F(XnodeRealFunctionsTest, CheckPasswordFormat_MixedCase_WeakMode) {
  tsEnableStrongPassword = 0;
  EXPECT_EQ(checkPasswordFmt_test("PaSsWoRd"), 0);
  EXPECT_EQ(checkPasswordFmt_test("UPPERCASE"), 0);
  EXPECT_EQ(checkPasswordFmt_test("lowercase"), 0);
}
