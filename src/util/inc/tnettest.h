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

#ifndef TDENGINE_TNETTEST_H
#define TDENGINE_TNETTEST_H

#ifdef __cplusplus
extern "C" {
#endif

typedef struct CmdArguments {
  char* host;
  char* password;
  char* user;
  char* auth;
  char* database;
  char* timezone;
  bool  is_raw_time;
  bool  is_use_passwd;
  char  file[TSDB_FILENAME_LEN];
  char  dir[TSDB_FILENAME_LEN];
  int   threadNum;
  char* commands;
  int   abort;
  int   port;
  int   endPort;
  int   pktLen;
  char* netTestRole;
} CmdArguments;

void taosNetTest(CmdArguments* args);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TNETTEST_H
