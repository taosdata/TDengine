/*
 * Copyright (c) 2022 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
#include "syncTest.h"

void usage(char* exe) {
  printf("Usage: %s host port \n", exe);
  printf("Usage: %s u64 \n", exe);
}

int main(int argc, char** argv) {
  if (argc == 2) {
    uint64_t u64 = atoll(argv[1]);
    char     host[128];
    uint16_t port;
    syncUtilU642Addr(u64, host, sizeof(host), &port);
    printf("%" PRIu64 " -> %s:%d \n", u64, host, port);

  } else if (argc == 3) {
    uint64_t u64;
    char*    host = argv[1];
    uint16_t port = atoi(argv[2]);
    u64 = syncUtilAddr2U64(host, port);
    printf("%s:%d ->: %" PRIu64 " \n", host, port, u64);
  } else {
    usage(argv[0]);
    exit(-1);
  }

  return 0;
}
