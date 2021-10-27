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
#include "os.h"
#include "mnodeAuth.h"

int32_t mnodeInitAuth() { return 0; }
void    mnodeCleanupAuth() {}

int32_t mnodeRetriveAuth(char *user, char *spi, char *encrypt, char *secret, char *ckey) {
  if (strcmp(user, TSDB_NETTEST_USER) == 0) {
    char pass[32] = {0};
    taosEncryptPass((uint8_t *)user, strlen(user), pass);
    *spi = 0;
    *encrypt = 0;
    *ckey = 0;
    memcpy(secret, pass, TSDB_KEY_LEN);
    mDebug("nettest user is authorized");
    return 0;
  }

  return 0;
}