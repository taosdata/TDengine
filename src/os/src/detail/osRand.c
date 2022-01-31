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

uint32_t taosRand(void) { return rand(); }

uint32_t taosSafeRand(void) {
  int fd;
  int seed;

  fd = open("/dev/urandom", 0 | O_BINARY);
  if (fd < 0) {
    seed = (int)time(0);
  } else {
    int len = read(fd, &seed, sizeof(seed));
    if (len < 0) {
      seed = (int)time(0);
    }
    close(fd);
  }

  return (uint32_t)seed;
}

void taosRandStr(char* str, int32_t size) {
  const char* set = "abcdefghijklmnopqrstuvwxyz0123456789-_.";
  int32_t     len = 39;

  for (int32_t i = 0; i < size; ++i) {
    str[i] = set[taosRand() % len];
  }
}