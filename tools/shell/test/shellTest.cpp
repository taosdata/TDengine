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
#include <cstring> 
#include <cctype>
#include <string>
#include "shellAuto.h"
#include "shellInt.h"

TEST(fieldOptionsArea, autoTabTest) {
  printf("hello world  SHELL tab test\n");

  // str false
  const char *s0[] = {
      "create table st(ts ",
      "create table st(ts timestamp, age ",
      "create table st(ts timestamp,  age",
      "create table st(ts timestamp, age int ,  name ",
      "create table st(ts timestamp, age int ,  name  binary(16)",
      "create table st(ts timestamp, age int ,  name  binary(16) ) tags( ",
      "create table st(ts timestamp, age int ,  name  binary(16) ) tags( area int, addr ",
      "create table st(ts timestamp, age int ,  name  binary(16) ) tags( area int,addr varbinary",
      "create table st(ts timestamp, age int, name binary(16)) tags(area int  ,  addr varbinary(32)",
      "create table st( ts timestamp, age int, name binary(16)) tags( area int,    addr",
      "create table st  (ts   timestamp ,  age int, name binary(16) , area int,",
      "create table st  (ts   timestamp ,  age int, name binary(16) ) tags ( area int ,addr varbinary",
      "create table st  (ts   timestamp ,  age int, name binary(16) ) tags ( area int , addr varbinary(32) level "
      "'high' , no i",
      "create table st  (ts   timestamp ,  age int, name binary(16) ) tags ( area int , addr varbinary(32) encode "
      "'simple8b' level 'high', no  in",
  };

  // str true
  const char *s1[] = {
      "create table st(ts timestamp ",
      "create table st(ts timestamp, age int ",
      "create table st(ts timestamp,  age  int  ",
      "create table st(ts timestamp, age int ,  name binary(16) ",
      "create table st(ts timestamp, age int ,  name  binary(16)  ",
      "create table st(ts timestamp, age int ,  name  binary(16) , addr varbinary( 32 ) ",
      "create table st(ts timestamp, age int ,  name  binary(16) ,area int, addr varbinary(32) ",
      "create table st(ts timestamp, age int ,  name  binary(16), area int,addr varbinary(32) ",
      "create table st(ts timestamp, age int, name binary(16) , area int,addr varbinary(32) ",
      "create table st( ts timestamp, age int, name binary(16) ,area int,addr varbinary(32) ",
      "create table st  (ts   timestamp ,  age int, name binary(16), area int,addr varbinary(32) ",
      "create table st  (ts   timestamp ,  age int, name binary(16), area int , addr varbinary(32) compress 'zlib' ",
      "create table st  (ts   timestamp ,  age int, name binary(16),  area int , addr varbinary(32) level 'high' ",
      "create table st  (ts   timestamp ,  age int, name binary(16) , area int , addr varbinary(32) encode 'simple8b' "
      "level 'high'   ",
  };

  // s0 is false
  for (int32_t i = 0; i < sizeof(s0) / sizeof(char *); i++) {
    printf("s0 i=%d fieldOptionsArea %s expect false \n", i, s0[i]);
    ASSERT(fieldOptionsArea((char *)s0[i]) == false);
  }

  // s1 is true
  for (int32_t i = 0; i < sizeof(s1) / sizeof(char *); i++) {
    printf("s1 i=%d fieldOptionsArea %s expect true \n", i, s1[i]);
    ASSERT(fieldOptionsArea((char *)s1[i]) == true);
  }
}

TEST(isCreateFieldsArea, autoTabTest) {
  printf("hellow world  SHELL tab test\n");

  // str false
  const char *s0[] = {
      "create table st(ts )",
      "create table st(ts timestamp, age) ",
      "create table st(ts timestamp,  age)",
      "create table st(ts timestamp, age int ,  name binary(16) )",
      "create table st(ts timestamp, age int ,  name  binary(16))",
      "create table st(ts timestamp, age int ,  name  binary(16) ) tags( )",
      "create table st(ts timestamp, age int ,  name  binary(16) ) tags( area int, addr )",
      "create table st(ts timestamp, age int ,  name  binary(16) ) tags( area int,addr varbinary)",
      "create table st(ts timestamp, age int, name binary(16)) tags(area int  ,  addr varbinary(32))",
      "create table st( ts timestamp, age int, name binary(16)) tags( area int,    addr int)",
      "create table st  (ts   timestamp ,  age int, name binary(16) ) tags ( area int,addr varbinary(32) )",
      "create table st  (ts   timestamp ,  age int, name binary(16) ) tags ( area int ,addr varbinary(14))",
      "create table st  (ts   timestamp ,  age int, name binary(16) ) tags ( area int , addr varbinary(32) level "
      "'high' )",
      "create table st  (ts   timestamp ,  age int, name binary(16) ) tags ( area int , addr varbinary(32) encode "
      "'simple8b' level 'high' )  ",
  };

  // str true
  const char *s1[] = {
      "create table st(ts timestamp ",
      "create table st(ts timestamp, age int ",
      "create table st(ts timestamp,  age  int  ,",
      "create table st(ts timestamp, age int ,  name binary(16), ",
      "create table st(ts timestamp, age int ,  name  binary(16)  ",
      "create table st(ts timestamp, age int ,  name  binary(16) ) tags( area int ",
      "create table st(ts timestamp, age int ,  name  binary(16) ) tags( area int, addr varbinary(32) ",
      "create table st(ts timestamp, age int ,  name  binary(16) ) tags( area int,addr varbinary(32)",
      "create table st(ts timestamp, age int, name binary(16)) tags(area int,addr varbinary(32) ",
      "create table st( ts timestamp, age int, name binary(16)) tags(area int,addr varbinary(32) ",
      "create table st  (ts   timestamp ,  age int, name binary(16) ) tags ( area int, addr varbinary(32) ",
      "create table st  (ts   timestamp ,  age int, name binary(16) ) tags ( area int , addr varbinary(32) compress "
      "'zlib' ",
      "create table st  (ts   timestamp ,  age int, name binary(16) ) tags ( area int , addr varbinary(32) level "
      "'high' ",
      "create table st  (ts   timestamp ,  age int, name binary(16) ) tags ( area int , addr varbinary(32) encode "
      "'simple8b' level 'high' ",
  };

  // s0 is false
  for (int32_t i = 0; i < sizeof(s0) / sizeof(char *); i++) {
    printf("s0 i=%d isCreateFieldsArea %s expect false. \n", i, s0[i]);
    ASSERT(isCreateFieldsArea((char *)s0[i]) == false);
  }

  // s1 is true
  for (int32_t i = 0; i < sizeof(s1) / sizeof(char *); i++) {
    printf("s1 i=%d isCreateFieldsArea %s expect true. \n", i, s1[i]);
    ASSERT(isCreateFieldsArea((char *)s1[i]) == true);
  }
}

TEST(shellCountPrefixOnes, checkUtf8) {
  // test ASCII
  assert(shellCountPrefixOnes(0x00) == 1);   // 00000000 → 0 leading 1s → Return 1
  assert(shellCountPrefixOnes(0x41) == 1);   // 01000001 → 0 leading 1s → Return 1
  assert(shellCountPrefixOnes(0x7F) == 1);   // 01111111 → 0 leading 1s → Return 1

  // test two byte characters (such as Latin extended characters)
  assert(shellCountPrefixOnes(0xC2) == 2);   // 11000010 → 2 leading 1s → Return 2
  assert(shellCountPrefixOnes(0xDF) == 2);   // 11011111 → 2 leading 1s → Return 2

  // test three byte characters (such as Chinese)
  assert(shellCountPrefixOnes(0xE0) == 3);   // 11100000 → 3 leading 1s → Return 3
  assert(shellCountPrefixOnes(0xEF) == 3);   // 11101111 → 3 leading 1s → Return 3

  // test four byte characters (such as emojis)
  assert(shellCountPrefixOnes(0xF0) == 4);   // 11110000 → 4 leading 1s → Return 4
  assert(shellCountPrefixOnes(0xF7) == 4);   // 11110111 → 4 leading 1s → Return 4

  // test for illegal UTF-8 first byte (only verifying leading 1 count)
  assert(shellCountPrefixOnes(0xF8) == 5);   // 11111000 → 5 leading 1s → Return 5
  assert(shellCountPrefixOnes(0xFF) == 8);   // 11111111 → 8个 leading 1s → Return 8
}

TEST(shellUtil, trimStr) {
    struct Case { const char* in; const char* expect; } cases[] = {
      {"3.0.8.0 trial ", "3.0.8.0"},    // remove "trial" and trailing spaces
      {"3.0.8.0 trial",  "3.0.8.0"},    // remove "trial"
      {"3.0.8.0   ",     "3.0.8.0"},   // remove trailing spaces
      {"3.0.8.0",        "3.0.8.0"},   // no change
      {"trial",          ""},           // exact match "trial" -> empty
      {"trial ",         ""},           // "trial" followed by a space
      {" trial",         ""},           // leading space + "trial"
      {"3.0.8.0,trial",  "3.0.8.0,"},   // remove ",trial"
    };

    for (const auto& c : cases) {
        char buf[64] = {0};
        strncpy(buf, c.in, sizeof(buf) - 1);
        trimStr(buf, "trial");
        EXPECT_STREQ(c.expect, buf) << "input: '" << c.in << "'";
    }
}


// create table / stable / vtable share the same field / tags completion
static bool createTableCompletion(const char* sql) {
  char        buffer[512] = {0};
  char        command[512] = {0};
  SShellCmd   cmd;

  memset(&cmd, 0, sizeof(cmd));
  strncpy(command, sql, sizeof(command) - 1);
  strncpy(buffer, command, sizeof(buffer) - 1);
  cmd.buffer = buffer;
  cmd.bufferSize = (uint32_t)strlen(buffer);
  cmd.command = command;
  cmd.commandSize = (uint32_t)strlen(command);
  cmd.cursorOffset = cmd.commandSize;
  cmd.screenOffset = cmd.commandSize;
  cmd.endOffset = cmd.commandSize;

  // reset the global auto completion state, it is otherwise shared between calls
  pressOtherKey(0);
  return matchCreateTable(NULL, &cmd);
}

TEST(createTableCompletion, fieldsOfTableStableAndVtable) {
  shellAutoInit();

  // the field data types are completed for table, stable and vtable
  EXPECT_TRUE(createTableCompletion("create table st(ts ti"));
  EXPECT_TRUE(createTableCompletion("create stable st(ts ti"));
  EXPECT_TRUE(createTableCompletion("create vtable vt(ts ti"));

  // other statements must not be handled by the create table matching
  EXPECT_FALSE(createTableCompletion("create stream s1 as select "));
  EXPECT_FALSE(createTableCompletion("create topic tp1 as select "));
  EXPECT_FALSE(createTableCompletion("drop stable st;"));
  EXPECT_FALSE(createTableCompletion("alter stable st add column c1 int;"));
}

// A hostile or intercepted server answers with column names, error text and
// completion words of its own choosing. Control characters must be stripped
// before such text reaches the user's terminal.
TEST(shellUtil, stripControlChars) {
  struct Case {
    const char *in;
    const char *expect;
  } cases[] = {
      {"show grants", "show grants"},                     // plain text is kept
      {"col\tname", "col\tname"},                         // tab is kept
      {"\x1b]0;evil\x07" "col", "]0;evilcol"},            // OSC, window title
      {"\x1b[31mred\x1b[0m", "[31mred[0m"},               // SGR, colour
      {"a\x7f" "b", "ab"},                                // DEL
      {"\x01\x02\x03", ""},                               // C0 controls
      {"\xc2\x9b" "31m", "31m"},                          // C1 CSI (U+009B)
      {"\xe6\x95\xb0\xe6\x8d\xae\xe5\xba\x93", "\xe6\x95\xb0\xe6\x8d\xae\xe5\xba\x93"},  // Chinese is kept
      {"\xe4\xb8\x80", "\xe4\xb8\x80"},                   // 0x80 continuation is not a C1 control
  };

  for (const auto &c : cases) {
    char buf[64] = {0};
    strncpy(buf, c.in, sizeof(buf) - 1);
    shellStripControlChars(buf);
    EXPECT_STREQ(c.expect, buf) << "input: '" << c.in << "'";
  }

  shellStripControlChars(NULL);  // must not crash
}

// Statements that assign a credential must not be recorded to the history file,
// whatever the leading / repeated blanks and the quoting look like; the filter
// used to only match "alter user ... pass " exactly.
TEST(shellUtil, statementHoldsCredential) {
  const char *redacted[] = {
      "alter user root pass 'S3cret'",
      "  alter user root pass 'S3cret'",
      "alter  user root pass 'S3cret'",
      "alter user root pass'S3cret'",
      "ALTER USER ROOT PASS 'S3cret';",
      "create user u pass 'x'",
      "create xnode ep user u token 'tk'",
      "alter xnode set token tk",
      "create totp_secret for user u",
  };

  for (const char *stmt : redacted) {
    EXPECT_TRUE(shellStatementHoldsCredential(stmt)) << "must not be recorded: " << stmt;
  }

  const char *recorded[] = {
      "",
      "show users;",
      "show tokens;",
      "drop user u",
      "create database db",
      "alter table t add column password varchar(20)",
      "select * from ins_users where pass = 1",
  };

  for (const char *stmt : recorded) {
    EXPECT_FALSE(shellStatementHoldsCredential(stmt)) << "should be recorded: " << stmt;
  }

  // a credential far behind the head of a long statement must not be missed
  const char *head = "create user u comment '";
  std::string longStatement = std::string(head) + std::string(4096, 'a') + std::string("' pass 'S3cret'");
  EXPECT_TRUE(shellStatementHoldsCredential(longStatement.c_str()));
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

