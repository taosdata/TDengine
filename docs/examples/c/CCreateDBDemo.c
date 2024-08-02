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

// TAOS standard API example. The same syntax as MySQL, but only a subset
// to compile: gcc -o CCreateDBDemo CCreateDBDemo.c -ltaos

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "taos.h"


static int DemoCreateDB() {
// ANCHOR: create_db_and_table
  int         ret_code  = -1;
  const char *ip        = "localhost";
  const char *user      = "root";
  const char *password  = "taosdata";

  // connect
  TAOS *taos = taos_connect(ip, user, password, NULL, 0);
  if (taos == NULL) {
    printf("failed to connect to server, reason: %s\n", taos_errstr(NULL));
    goto end;
  }

  // create database
  TAOS_RES *result = taos_query(taos, "CREATE DATABASE IF NOT EXISTS power");
  int code = taos_errno(result);
  if (code != 0) {
    printf("failed to create database, reason: %s\n", taos_errstr(result));
    goto end;
  }
  taos_free_result(result);
  printf("success to create database\n");

  // use database
  result = taos_query(taos, "USE power");
  taos_free_result(result);

  // create table
  const char* sql = "CREATE STABLE IF NOT EXISTS meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))";
  result = taos_query(taos, sql);
  code = taos_errno(result);
  if (code != 0) {
    printf("failed to create table, reason: %s\n", taos_errstr(result));
    goto end;
  }
  taos_free_result(result);
  printf("success to create table\n");
  ret_code = 0;

end:
  // close & clean
  taos_close(taos);
  taos_cleanup();
  return ret_code;
// ANCHOR_END: create_db_and_table
}

int main(int argc, char *argv[]) {
  return DemoCreateDB();
}
