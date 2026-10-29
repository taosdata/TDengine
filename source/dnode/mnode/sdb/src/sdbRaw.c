/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

#define _DEFAULT_SOURCE
#include "sdb.h"

int32_t sdbGetIdFromRaw(SSdb *pSdb, SSdbRaw *pRaw) {
  // [Security fix for finding item 268] pRaw comes from an untrusted source on the mnode
  // raft apply path (mndProcessWriteMsg casts the payload of a committed entry to SSdbRaw
  // directly), so the int8 header fields cannot be trusted here:
  // 1. pRaw->type is used as an index into the fixed-size pSdb->keyTypes[SDB_MAX] array.
  //    An out-of-range value (negative, or >= SDB_MAX) reads neighbouring SSdb fields and
  //    yields a bogus key type, so it must be range-checked before the array is indexed.
  // 2. pRaw->dataLen is only checked by the sdbGetRaw* getters, not by this function. The
  //    INT32 key read below is the one read that bypasses dataLen, so require the raw to
  //    actually carry a full int32 key before touching pData.
  // Callers only treat the result as "a valid id" when it is > 0: -1 means malformed raw,
  // -2 (unchanged) means a table whose key is not an INT32 and has no id in its header.
  if (pRaw == NULL || pRaw->type < 0 || pRaw->type >= SDB_MAX) {
    terrno = TSDB_CODE_SDB_INVALID_TABLE_TYPE;
    return -1;
  }

  EKeyType keytype = pSdb->keyTypes[pRaw->type];
  if (keytype == SDB_KEY_INT32) {
    if (pRaw->dataLen < (int32_t)sizeof(int32_t)) {
      terrno = TSDB_CODE_SDB_INVALID_DATA_LEN;
      return -1;
    }
    int32_t id = *((int32_t *)(pRaw->pData));
    return id;
  } else {
    return -2;
  }
}

SSdbRaw *sdbAllocRaw(ESdbType type, int8_t sver, int32_t dataLen) {
  SSdbRaw *pRaw = taosMemoryCalloc(1, dataLen + sizeof(SSdbRaw));
  if (pRaw == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pRaw->type = type;
  pRaw->sver = sver;
  pRaw->dataLen = dataLen;

#if 1
  mTrace("raw:%p, is created, len:%d table:%s", pRaw, dataLen, sdbTableName(type));
#endif
  return pRaw;
}

void sdbFreeRaw(SSdbRaw *pRaw) {
  if (pRaw != NULL) {
#if 1
    mTrace("raw:%p, is freed, len:%d, table:%s", pRaw, pRaw->dataLen, sdbTableName(pRaw->type));
#endif
    taosMemoryFree(pRaw);
  }
}

int32_t sdbSetRawInt8(SSdbRaw *pRaw, int32_t dataPos, int8_t val) {
  int32_t code = 0;
  if (pRaw == NULL) {
    code = TSDB_CODE_INVALID_PTR;
    TAOS_RETURN(code);
  }

  if (dataPos + sizeof(int8_t) > pRaw->dataLen) {
    code = TSDB_CODE_SDB_INVALID_DATA_LEN;
    TAOS_RETURN(code);
  }

  *(int8_t *)(pRaw->pData + dataPos) = val;
  return 0;
}

int32_t sdbSetRawUInt8(SSdbRaw *pRaw, int32_t dataPos, uint8_t val) {
  int32_t code = 0;
  if (pRaw == NULL) {
    code = TSDB_CODE_INVALID_PTR;
    TAOS_RETURN(code);
  }

  if (dataPos + sizeof(uint8_t) > pRaw->dataLen) {
    code = TSDB_CODE_SDB_INVALID_DATA_LEN;
    TAOS_RETURN(code);
  }

  *(uint8_t *)(pRaw->pData + dataPos) = val;
  return 0;
}

int32_t sdbSetRawBool(SSdbRaw *pRaw, int32_t dataPos, bool val) {
  if (val) {
    return sdbSetRawUInt8(pRaw, dataPos, 1);
  } else {
    return sdbSetRawUInt8(pRaw, dataPos, 0);
  }
}

int32_t sdbSetRawInt32(SSdbRaw *pRaw, int32_t dataPos, int32_t val) {
  int32_t code = 0;
  if (pRaw == NULL) {
    code = TSDB_CODE_INVALID_PTR;
    TAOS_RETURN(code);
  }

  if (dataPos + sizeof(int32_t) > pRaw->dataLen) {
    code = TSDB_CODE_SDB_INVALID_DATA_LEN;
    TAOS_RETURN(code);
  }

  *(int32_t *)(pRaw->pData + dataPos) = val;
  return 0;
}

int32_t sdbSetRawUInt32(SSdbRaw *pRaw, int32_t dataPos, uint32_t val) {
  int32_t code = 0;
  if (pRaw == NULL) {
    code = TSDB_CODE_INVALID_PTR;
    TAOS_RETURN(code);
  }

  if (dataPos + sizeof(uint32_t) > pRaw->dataLen) {
    code = TSDB_CODE_SDB_INVALID_DATA_LEN;
    TAOS_RETURN(code);
  }

  *(uint32_t *)(pRaw->pData + dataPos) = val;
  return 0;
}

int32_t sdbSetRawInt16(SSdbRaw *pRaw, int32_t dataPos, int16_t val) {
  int32_t code = 0;
  if (pRaw == NULL) {
    code = TSDB_CODE_INVALID_PTR;
    TAOS_RETURN(code);
  }

  if (dataPos + sizeof(int16_t) > pRaw->dataLen) {
    code = TSDB_CODE_SDB_INVALID_DATA_LEN;
    TAOS_RETURN(code);
  }

  *(int16_t *)(pRaw->pData + dataPos) = val;
  return 0;
}

int32_t sdbSetRawInt64(SSdbRaw *pRaw, int32_t dataPos, int64_t val) {
  int32_t code = 0;
  if (pRaw == NULL) {
    code = TSDB_CODE_INVALID_PTR;
    TAOS_RETURN(code);
  }

  if (dataPos + sizeof(int64_t) > pRaw->dataLen) {
    code = TSDB_CODE_SDB_INVALID_DATA_LEN;
    TAOS_RETURN(code);
  }

  taosSetInt64Aligned((int64_t *)(pRaw->pData + dataPos), val);
  return 0;
}

int32_t sdbSetRawFloat(SSdbRaw *pRaw, int32_t dataPos, float val) {
  int32_t code = 0;
  if (pRaw == NULL) {
    code = TSDB_CODE_INVALID_PTR;
    TAOS_RETURN(code);
  }

  if (dataPos + sizeof(float) > pRaw->dataLen) {
    code = TSDB_CODE_SDB_INVALID_DATA_LEN;
    TAOS_RETURN(code);
  }

  *(int64_t *)(pRaw->pData + dataPos) = val;
  return 0;
}

int32_t sdbSetRawBinary(SSdbRaw *pRaw, int32_t dataPos, const char *pVal, int32_t valLen) {
  int32_t code = 0;
  if (pRaw == NULL) {
    code = TSDB_CODE_INVALID_PTR;
    TAOS_RETURN(code);
  }

  // Overflow-safe bounds check (see security finding item 267).
  // dataPos / valLen / pRaw->dataLen are all int32, so the original expression
  // `dataPos + valLen > pRaw->dataLen` is evaluated in int32 arithmetic: an attacker-chosen
  // valLen near INT32_MAX (e.g. 0x7FFFFFF0) makes the sum wrap negative and slip past the
  // guard, after which memcpy() reads gigabytes from a tiny raw and crashes the mnode.
  // We therefore reject negative operands and compare the length against the *remaining*
  // raw bytes without any intermediate addition that can overflow.
  if (dataPos < 0 || valLen < 0 || dataPos > pRaw->dataLen ||
      valLen > pRaw->dataLen - dataPos) {
    code = TSDB_CODE_SDB_INVALID_DATA_LEN;
    TAOS_RETURN(code);
  }

  if (pVal != NULL) {
    memcpy(pRaw->pData + dataPos, pVal, valLen);
  }
  return 0;
}

int32_t sdbSetRawDataLen(SSdbRaw *pRaw, int32_t dataLen) {
  int32_t code = 0;
  if (pRaw == NULL) {
    code = TSDB_CODE_INVALID_PTR;
    TAOS_RETURN(code);
  }

  if (dataLen > pRaw->dataLen) {
    code = TSDB_CODE_SDB_INVALID_DATA_LEN;
    TAOS_RETURN(code);
  }

  pRaw->dataLen = dataLen;
  return 0;
}

int32_t sdbSetRawStatus(SSdbRaw *pRaw, ESdbStatus status) {
  int32_t code = 0;
  if (pRaw == NULL) {
    code = TSDB_CODE_INVALID_PTR;
    TAOS_RETURN(code);
  }

  if (status == SDB_STATUS_INIT) {
    code = TSDB_CODE_INVALID_PARA;
    TAOS_RETURN(code);
  }

  pRaw->status = status;
  return 0;
}

int32_t sdbGetRawInt8(SSdbRaw *pRaw, int32_t dataPos, int8_t *val) {
  int32_t code = 0;
  if (pRaw == NULL) {
    code = TSDB_CODE_INVALID_PTR;
    TAOS_RETURN(code);
  }

  if (dataPos + sizeof(int8_t) > pRaw->dataLen) {
    code = TSDB_CODE_SDB_INVALID_DATA_LEN;
    TAOS_RETURN(code);
  }

  *val = *(int8_t *)(pRaw->pData + dataPos);
  return 0;
}

int32_t sdbGetRawUInt8(SSdbRaw *pRaw, int32_t dataPos, uint8_t *val) {
  int32_t code = 0;
  if (pRaw == NULL) {
    code = TSDB_CODE_INVALID_PTR;
    TAOS_RETURN(code);
  }

  if (dataPos + sizeof(uint8_t) > pRaw->dataLen) {
    code = TSDB_CODE_SDB_INVALID_DATA_LEN;
    TAOS_RETURN(code);
  }

  *val = *(uint8_t *)(pRaw->pData + dataPos);
  return 0;
}

int32_t sdbGetRawBool(SSdbRaw *pRaw, int32_t dataPos, bool *val) {
  int32_t code = 0;
  uint8_t v = 0;
  code = sdbGetRawUInt8(pRaw, dataPos, &v);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  if (v) {
    *val = true;
  } else {
    *val = false;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t sdbGetRawInt32(SSdbRaw *pRaw, int32_t dataPos, int32_t *val) {
  int32_t code = 0;
  if (pRaw == NULL) {
    code = TSDB_CODE_INVALID_PTR;
    TAOS_RETURN(code);
  }

  if (dataPos + sizeof(int32_t) > pRaw->dataLen) {
    code = TSDB_CODE_SDB_INVALID_DATA_LEN;
    TAOS_RETURN(code);
  }

  *val = *(int32_t *)(pRaw->pData + dataPos);
  return 0;
}

int32_t sdbGetRawUInt32(SSdbRaw *pRaw, int32_t dataPos, uint32_t *val) {
  int32_t code = 0;
  if (pRaw == NULL) {
    code = TSDB_CODE_INVALID_PTR;
    TAOS_RETURN(code);
  }

  if (dataPos + sizeof(uint32_t) > pRaw->dataLen) {
    code = TSDB_CODE_SDB_INVALID_DATA_LEN;
    TAOS_RETURN(code);
  }

  *val = *(uint32_t *)(pRaw->pData + dataPos);
  return 0;
}

int32_t sdbGetRawInt16(SSdbRaw *pRaw, int32_t dataPos, int16_t *val) {
  int32_t code = 0;
  if (pRaw == NULL) {
    code = TSDB_CODE_INVALID_PTR;
    TAOS_RETURN(code);
  }

  if (dataPos + sizeof(int16_t) > pRaw->dataLen) {
    code = TSDB_CODE_SDB_INVALID_DATA_LEN;
    TAOS_RETURN(code);
  }

  *val = *(int16_t *)(pRaw->pData + dataPos);
  return 0;
}

int32_t sdbGetRawInt64(SSdbRaw *pRaw, int32_t dataPos, int64_t *val) {
  int32_t code = 0;
  if (pRaw == NULL) {
    code = TSDB_CODE_INVALID_PTR;
    TAOS_RETURN(code);
  }

  if (dataPos + sizeof(int64_t) > pRaw->dataLen) {
    code = TSDB_CODE_SDB_INVALID_DATA_LEN;
    TAOS_RETURN(code);
  }

  taosSetPInt64Aligned(val, (int64_t *)(pRaw->pData + dataPos));
  return 0;
}

int32_t sdbGetRawFloat(SSdbRaw *pRaw, int32_t dataPos, float *val) {
  int32_t code = 0;
  if (pRaw == NULL) {
    code = TSDB_CODE_INVALID_PTR;
    TAOS_RETURN(code);
  }

  if (dataPos + sizeof(float) > pRaw->dataLen) {
    code = TSDB_CODE_SDB_INVALID_DATA_LEN;
    TAOS_RETURN(code);
  }

  *val = *(int64_t *)(pRaw->pData + dataPos);
  return 0;
}

int32_t sdbGetRawBinary(SSdbRaw *pRaw, int32_t dataPos, char *pVal, int32_t valLen) {
  int32_t code = 0;
  if (pRaw == NULL) {
    code = TSDB_CODE_INVALID_PTR;
    TAOS_RETURN(code);
  }

  // Overflow-safe bounds check (see security finding item 267).
  // dataPos / valLen / pRaw->dataLen are all int32, so the original expression
  // `dataPos + valLen > pRaw->dataLen` is evaluated in int32 arithmetic: an attacker-chosen
  // valLen near INT32_MAX (e.g. 0x7FFFFFF0) makes the sum wrap negative and slip past the
  // guard, after which memcpy() reads gigabytes from a tiny raw and crashes the mnode.
  // We therefore reject negative operands and compare the length against the *remaining*
  // raw bytes without any intermediate addition that can overflow.
  if (dataPos < 0 || valLen < 0 || dataPos > pRaw->dataLen ||
      valLen > pRaw->dataLen - dataPos) {
    code = TSDB_CODE_SDB_INVALID_DATA_LEN;
    TAOS_RETURN(code);
  }
  if (pVal != NULL) {
    memcpy(pVal, pRaw->pData + dataPos, valLen);
  }
  return 0;
}

int32_t sdbGetRawSoftVer(SSdbRaw *pRaw, int8_t *sver) {
  int32_t code = 0;
  if (pRaw == NULL) {
    code = TSDB_CODE_INVALID_PTR;
    TAOS_RETURN(code);
  }

  *sver = pRaw->sver;
  return 0;
}

int32_t sdbGetRawTotalSize(SSdbRaw *pRaw) {
  if (pRaw == NULL) {
    terrno = TSDB_CODE_INVALID_PTR;
    return -1;
  }

  return sizeof(SSdbRaw) + pRaw->dataLen;
}
