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

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "taos.h"
#include "types.h"
#include "tlog.h"

#define GET_ROW_NUM \
  numRows = 0;\
  while(1){\
    row = taos_fetch_row(pRes);\
    if (row != NULL){\
      numRows++;\
    }else{\
      break;\
    }\
  }
int varbinary_test() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);

  TAOS_RES *pRes = taos_query(taos, "drop database if exists varbinary_db");
  taos_free_result(pRes);

  pRes = taos_query(taos, "create database if not exists varbinary_db");
  taos_free_result(pRes);

  pRes = taos_query(taos, "use varbinary_db");
  int code = taos_errno(pRes);
  taos_free_result(pRes);
  ASSERT(code == 0);

  pRes = taos_query(taos, "create stable stb (ts timestamp, c1 nchar(32), c2 varbinary(16), c3 float) tags (t1 int, t2 binary(8), t3 varbinary(8))");
  taos_free_result(pRes);

  pRes = taos_query(taos, "desc stb");

  TAOS_ROW row = NULL;
  int32_t  rowIndex = 0;
  while ((row = taos_fetch_row(pRes)) != NULL) {
    char* type  = row[1];
    int32_t length = *(int32_t *)row[2];

    if (rowIndex == 2) {
      ASSERT(strncmp(type, "VARBINARY", sizeof("VARBINARY") - 1) == 0);
      ASSERT(length == 16);
    }

    if (rowIndex == 6) {
      ASSERT(strncmp(type, "VARBINARY", sizeof("VARBINARY") - 1) == 0);
      ASSERT(length == 8);
    }
    rowIndex++;
  }

  pRes = taos_query(taos, "insert into tb1 using stb tags (1, 'tb1_bin1', 'vart1') values (now, 'nchar1', 'varc1', 0.3)");
  taos_free_result(pRes);
  pRes = taos_query(taos, "insert into tb1 values (now + 1s, 'nchar2', null, 0.4);");
  taos_free_result(pRes);
  pRes = taos_query(taos, "insert into tb3 using stb tags (3, 'tb3_bin1', '\\x7f8290') values (now + 2s, 'nchar1', '\\x7f8290', 0.3)");
  taos_free_result(pRes);
  pRes = taos_query(taos, "insert into tb3 values (now + 3s, 'nchar1', '\\x7f829000', 0.3)");
  taos_free_result(pRes);
  pRes = taos_query(taos, "insert into tb2 using stb tags (2, 'tb2_bin1', '\\x') values (now + 4s, 'nchar1', '\\x', 0.3)");
  taos_free_result(pRes);
  pRes = taos_query(taos, "insert into tb2 values (now + 5s, 'nchar1', '\\x00000000', 0.3)");
  taos_free_result(pRes);

  // test insert
  pRes = taos_query(taos, "insert into tb2 using stb tags (2, 'tb2_bin1', 093) values (now + 2s, 'nchar1', 892, 0.3)");
  ASSERT(taos_errno(pRes) != 0);

  pRes = taos_query(taos, "insert into tb3 using stb tags (3, 'tb3_bin1', 0x7f829) values (now + 3s, 'nchar1', 0x7f829, 0.3)");
  ASSERT(taos_errno(pRes) != 0);

  pRes = taos_query(taos, "insert into tb3 using stb tags (3, 'tb3_bin1', '\\x7f829') values (now + 3s, 'nchar1', '\\x7f829', 0.3)");
  ASSERT(taos_errno(pRes) != 0);

  pRes = taos_query(taos, "insert into tb4 using stb tags (4, 'tb4_bin1', 0b100000010) values (now + 4s, 'nchar1', 0b110000001, 0.3)");
  ASSERT(taos_errno(pRes) != 0);
  taos_free_result(pRes);


  // test error
  pRes = taos_query(taos, "select * from tb1 where c2 >= 0x8de6");
  ASSERT(taos_errno(pRes) != 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select * from tb1 where c2 >= 0");
  ASSERT(taos_errno(pRes) != 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select * from stb where c2 > '\\x7F82900'");
  ASSERT(taos_errno(pRes) != 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select c2+2 from stb");
  ASSERT(taos_errno(pRes) != 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select c2|2 from stb");
  ASSERT(taos_errno(pRes) != 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select * from stb where c2 not like 's%'");
  ASSERT(taos_errno(pRes) != 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select * from stb where c2 like 's%'");
  ASSERT(taos_errno(pRes) != 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select * from stb where c2 match 'ssd'");
  ASSERT(taos_errno(pRes) != 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select * from stb where c2 nmatch 'ssd'");
  ASSERT(taos_errno(pRes) != 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select * from stb where c2->'ssd' = 1");
  ASSERT(taos_errno(pRes) != 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select * from stb where c2 contains 'ssd'");
  ASSERT(taos_errno(pRes) != 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select * from stb where sum(c2) = 2");
  ASSERT(taos_errno(pRes) != 0);
  taos_free_result(pRes);

//  pRes = taos_query(taos, "select * from stb where c2 contains 'ssd'");
//  ASSERT(taos_errno(pRes) != 0);
//  taos_free_result(pRes);
//
//  pRes = taos_query(taos, "select * from stb where c2 contains 'ssd'");
//  ASSERT(taos_errno(pRes) != 0);
//  taos_free_result(pRes);

  int numRows = 0;

  pRes = taos_query(taos, "select * from stb where c2 > 'varc1'");
  GET_ROW_NUM
  ASSERT(numRows == 2);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select * from stb where c2 > '\\x7F8290'");
  GET_ROW_NUM
  ASSERT(numRows == 1);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select * from stb where c2 in ('\\x7F829000','\\x00000000')");
  GET_ROW_NUM
  ASSERT(numRows == 2);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select * from stb where c2 not in ('\\x00000000')");
  GET_ROW_NUM
  ASSERT(numRows == 4);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select * from stb where c2 is null");
  GET_ROW_NUM
  ASSERT(numRows == 1);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select * from stb where c2 is not null");
  GET_ROW_NUM
  ASSERT(numRows == 5);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select * from stb where c2 between '\\x3e' and '\\x7F8290'");
  GET_ROW_NUM
  ASSERT(numRows == 2);
  taos_free_result(pRes);


  pRes = taos_query(taos, "select * from stb where c2 not between '\\x3e' and '\\x7F8290'");
  GET_ROW_NUM
  ASSERT(numRows == 3);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select ts,c2 from stb order by c2");
  rowIndex = 0;
  while ((row = taos_fetch_row(pRes)) != NULL) {
//    int64_t ts = *(int64_t *)row[0];
    if (rowIndex == 0) {
      ASSERT(row[1] == NULL);
      rowIndex++;
      continue;
    }
    int32_t*    length = taos_fetch_lengths(pRes);
    void*         data = NULL;
    uint32_t      size = 0;
    if(taosAscii2Hex(row[1], length[1], &data, &size) < 0){
      ASSERT(0);
    }

    if (rowIndex == 1) {
//      ASSERT(ts == 1661943960000);
      ASSERT(memcmp(data, "\\x", size) == 0);
    }
    if (rowIndex == 2) {
//      ASSERT(ts == 1661943960000);
      ASSERT(memcmp(data, "\\x00000000", size) == 0);
    }
    if (rowIndex == 3) {
//      ASSERT(ts == 1661943960000);
      ASSERT(memcmp(data, "\\x7661726331", size) == 0);
    }
    if (rowIndex == 4) {
//      ASSERT(ts == 1661943960000);
      ASSERT(memcmp(data, "\\x7F8290", size) == 0);
    }
    if (rowIndex == 5) {
//      ASSERT(ts == 1661943960000);
      ASSERT(memcmp(data, "\\x7F829000", size) == 0);
    }
    taosMemoryFree(data);

    rowIndex++;
  }
  printf("%s result1:%s\n", __FUNCTION__, taos_errstr(pRes));
  taos_free_result(pRes);

  taos_close(taos);

  return code;
}

int main(int argc, char *argv[]) {
  int ret = 0;
  ret = varbinary_test();
  ASSERT(!ret);
  return ret;
}
