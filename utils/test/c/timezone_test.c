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

char* timezone_name = "America/New_York";
void check_timezone(const char* tzName){
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT(pConn != NULL);
  TAOS_RES *pRes = taos_query(pConn, "show local variables");
  ASSERT(taos_errno(pRes) == 0);
  TAOS_ROW row = NULL;
  while ((row = taos_fetch_row(pRes)) != NULL) {
    if (strcmp(row[0], "timezone") == 0){
      ASSERT(strstr(row[1], tzName) != NULL);
    }
  }
  taos_free_result(pRes);
  taos_close(pConn);
}

void timezone_set_options_test(){
  taos_options(TSDB_OPTION_TIMEZONE, timezone_name);
  check_timezone(timezone_name);
}

void timezone_insert_test(){
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);

  TAOS_RES *pRes = taos_query(taos, "drop database if exists tz_db");
  taos_free_result(pRes);

  pRes = taos_query(taos, "create database if not exists tz_db");
  taos_free_result(pRes);

  pRes = taos_query(taos, "use tz_db");
  int code = taos_errno(pRes);
  taos_free_result(pRes);
  ASSERT(code == 0);

  pRes = taos_query(taos, "create stable stb (ts timestamp, c1 int) tags (t1 timestamp, t2 binary(32))");
  taos_free_result(pRes);

  pRes = taos_query(taos, "create table ntb (ts timestamp, c1 int)");
  taos_free_result(pRes);

  pRes = taos_query(taos, "insert into tb1 using stb tags (1, 'test') values (1, 2)");
  taos_free_result(pRes);

  pRes = taos_query(taos, "insert into tb1 using stb tags ('2013-04-12T10:52:01', 'test') values ('2013-04-12T10:52:01', 2)");
  taos_free_result(pRes);

  pRes = taos_query(taos, "insert into ntb values ('2013-04-12T10:52:01', 2)");
  taos_free_result(pRes);

  pRes = taos_query(taos, "insert into ntb values (1, 2)");
  taos_free_result(pRes);


  /*
   select today();
   tsConver.sim
   "COMPACT DATABASE test START WITH '2023-03-07 14:01:23' END WITH '2023-03-08 14:01:23'"
   TK_TIMEZONE

   ts > c1('2013-04-12T10:52:01')
   in ('2013-04-12T10:52:01')

   offsetFromTz  hash join
   */
}

int main(int argc, char *argv[]) {
  int ret = 0;
  timezone_set_options_test();
  return ret;
}
