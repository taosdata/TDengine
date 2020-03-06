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
#define _DEFAULT_SOURCE
#include "sdbint.h"

int32_t (*mpeerInitMnodesFp)(char *directory) = NULL;
void    (*mpeerCleanUpMnodesFp)() = NULL;
int32_t (*mpeerForwardRequestFp)(SSdbTable *pTable, char type, void *cont, int32_t contLen) = NULL;

char *sdbStatusStr[] = {
  "offline",
  "unsynced",
  "syncing",
  "serving",
  "null"
};

char *sdbRoleStr[] = {
  "unauthed",
  "undecided",
  "master",
  "slave",
  "null"
};

int32_t sdbForwardDbReqToPeer(SSdbTable *pTable, char type, char *data, int32_t dataLen) {
  if (mpeerForwardRequestFp) {
    return mpeerForwardRequestFp(pTable, type, data, dataLen);
  } else {
    return 0;
  }
}

int32_t sdbInitPeers(char *directory) {
  if (mpeerInitMnodesFp) {
    return (*mpeerInitMnodesFp)(directory);
  } else {
    return 0;
  }
}

void sdbCleanUpPeers() {
  if (mpeerCleanUpMnodesFp) {
    (*mpeerCleanUpMnodesFp)();
  }
}
