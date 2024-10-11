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

static uint32_t tUUIDHashId = 0;
static int32_t tUUIDSerialNo = 0;

int32_t taosGetSystemUUID32(uint32_t *uuid) {
  if (uuid == NULL) return TSDB_CODE_APP_ERROR;
  char    uid[65] = {0};
  int32_t code = taosGetSystemUUID(uid, sizeof(uid));
  uid[64] = 0;

  if (code != TSDB_CODE_SUCCESS) {
    return code;
  } else {
    *uuid = MurmurHash3_32(uid, strlen(uid));
  }
  return TSDB_CODE_SUCCESS;
}

int32_t taosGetSystemUUID64(uint64_t *uuid) {
  if (uuid == NULL) return TSDB_CODE_APP_ERROR;
  char    uid[65] = {0};
  int32_t code = taosGetSystemUUID(uid, sizeof(uid));
  uid[64] = 0;

  if (code != TSDB_CODE_SUCCESS) {
    return code;
  } else {
    *uuid = MurmurHash3_64(uid, strlen(uid));
  }
  return TSDB_CODE_SUCCESS;
}

int32_t tGenIdPI32(void) {
  if (tUUIDHashId == 0) {
    int32_t code = taosGetSystemUUID32(&tUUIDHashId);
    if (code != TSDB_CODE_SUCCESS) {
      terrno = code;
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
    int32_t code = taosGetSystemUUID32(&tUUIDHashId);
    if (code != TSDB_CODE_SUCCESS) {
      terrno = code;
    }
  }

  int64_t id;

  while (true) {
    int64_t  ts = taosGetTimestampMs() >> 8;
    uint64_t pid = taosGetPId();
    int32_t  val = atomic_add_fetch_32(&tUUIDSerialNo, 1);

    id = (((uint64_t)(tUUIDHashId & 0x07FF)) << 52) | ((pid & 0x0F) << 48) | ((ts & 0x3FFFFFF) << 20) | (val & 0xFFFFF);
    if (id) {
      break;
    }
  }

  return id;
}

int64_t tGenQid64(int8_t dnodeId) {
  int64_t id = dnodeId;

  while (true) {
    int32_t val = atomic_add_fetch_32(&tUUIDSerialNo, 1);

    id = (id << 56) | (val & 0xFFFFF) << 8;
    if (id) {
      break;
    }
  }

  return id;
}