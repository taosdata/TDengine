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

#include "tuuid.h"

static int64_t hashId = 0;
static int32_t SerialNo = 0;

int32_t tGenIdPI32(void) {
  if (hashId == 0) {
    char    uid[64];
    int32_t code = taosGetSystemUUID(uid, tListLen(uid));
    if (code != TSDB_CODE_SUCCESS) {
      terrno = TAOS_SYSTEM_ERROR(errno);
    } else {
      hashId = MurmurHash3_32(uid, strlen(uid));
    }
  }

  int64_t  ts = taosGetTimestampMs();
  uint64_t pid = taosGetPId();
  int32_t  val = atomic_add_fetch_32(&SerialNo, 1);

  int32_t id = ((hashId & 0x1F) << 26) | ((pid & 0x3F) << 20) | ((ts & 0xFFF) << 8) | (val & 0xFF);
  return id;
}

int64_t tGenIdPI64(void) {
  if (hashId == 0) {
    char    uid[64];
    int32_t code = taosGetSystemUUID(uid, tListLen(uid));
    if (code != TSDB_CODE_SUCCESS) {
      terrno = TAOS_SYSTEM_ERROR(errno);
    } else {
      hashId = MurmurHash3_32(uid, strlen(uid));
    }
  }

  int64_t  ts = taosGetTimestampMs();
  uint64_t pid = taosGetPId();
  int32_t  val = atomic_add_fetch_32(&SerialNo, 1);

  int64_t id = ((hashId & 0x07FF) << 52) | ((pid & 0x0FFF) << 40) | ((ts & 0xFFFFFF) << 16) | (val & 0xFFFF);
  return id;
}
