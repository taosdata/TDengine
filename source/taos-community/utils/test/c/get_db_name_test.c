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

int get_db_test() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);

  TAOS_RES *pRes = taos_query(taos, "create database if not exists sml_db vgroups 2");
  taos_free_result(pRes);

  pRes = taos_query(taos, "use sml_db");
  int code = taos_errno(pRes);
  taos_free_result(pRes);
  ASSERT(code == 0);

  code = taos_get_current_db(taos, NULL, 0, NULL);
  ASSERT(code != 0);

  int required = 0;
  code = taos_get_current_db(taos, NULL, 0, &required);
  ASSERT(code != 0);
  ASSERT(required == 7);

  char database[10] = {0};
  code = taos_get_current_db(taos, database, 3, &required);
  ASSERT(code != 0);
  ASSERT(required == 7);
  ASSERT(strcpy(database, "sm"));

  char database1[10] = {0};
  code = taos_get_current_db(taos, database1, 10, &required);
  ASSERT(code == 0);
  ASSERT(strcpy(database1, "sml_db"));

  taos_close(taos);

  return code;
}

int main(int argc, char *argv[]) {
  int ret = 0;
  ret = get_db_test();
  ASSERT(!ret);
  return ret;
}
