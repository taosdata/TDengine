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

static int64_t tUUIDHashId = 0;
static int32_t tUUIDSerialNo = 0;

int32_t tGenIdPI32(void) {
  if (tUUIDHashId == 0) {
    char    uid[65] = {0};
    int32_t code = taosGetSystemUUID(uid, sizeof(uid));
    uid[64] = 0;

    if (code != TSDB_CODE_SUCCESS) {
      terrno = TAOS_SYSTEM_ERROR(errno);
    } else {
      tUUIDHashId = MurmurHash3_32(uid, strlen(uid));
    }
  }

  int64_t  ts = taosGetTimestampMs();
  uint64_t pid = taosGetPId();
  int32_t  val = atomic_add_fetch_32(&tUUIDSerialNo, 1);

  int32_t id = ((tUUIDHashId & 0x1F) << 26) | ((pid & 0x3F) << 20) | ((ts & 0xFFF) << 8) | (val & 0xFF);
  return id;
}

int64_t tGenIdPI64(void) {
  if (tUUIDHashId == 0) {
    char    uid[65] = {0};
    int32_t code = taosGetSystemUUID(uid, 64);
    if (code != TSDB_CODE_SUCCESS) {
      terrno = TAOS_SYSTEM_ERROR(errno);
    } else {
      tUUIDHashId = MurmurHash3_32(uid, strlen(uid));
    }
  }

  int64_t id;

  while (true) {
    int64_t  ts = taosGetTimestampMs() >> 8;
    uint64_t pid = taosGetPId();
    int32_t  val = atomic_add_fetch_32(&tUUIDSerialNo, 1);

    id = ((tUUIDHashId & 0x07FF) << 52) | ((pid & 0x0F) << 48) | ((ts & 0x3FFFFFF) << 20) | (val & 0xFFFFF);
    if (id) {
      break;
    }
  }

  return id;
}
