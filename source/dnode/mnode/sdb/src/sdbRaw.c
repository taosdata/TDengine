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
#include "sdb.h"

int32_t sdbGetIdFromRaw(SSdb *pSdb, SSdbRaw *pRaw) {
  EKeyType keytype = pSdb->keyTypes[pRaw->type];
  if (keytype == SDB_KEY_INT32) {
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
  if (pRaw == NULL) {
    terrno = TSDB_CODE_INVALID_PTR;
    return -1;
  }

  if (dataPos + sizeof(int8_t) > pRaw->dataLen) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_LEN;
    return -1;
  }

  *(int8_t *)(pRaw->pData + dataPos) = val;
  return 0;
}

int32_t sdbSetRawInt32(SSdbRaw *pRaw, int32_t dataPos, int32_t val) {
  if (pRaw == NULL) {
    terrno = TSDB_CODE_INVALID_PTR;
    return -1;
  }

  if (dataPos + sizeof(int32_t) > pRaw->dataLen) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_LEN;
    return -1;
  }

  *(int32_t *)(pRaw->pData + dataPos) = val;
  return 0;
}

int32_t sdbSetRawInt16(SSdbRaw *pRaw, int32_t dataPos, int16_t val) {
  if (pRaw == NULL) {
    terrno = TSDB_CODE_INVALID_PTR;
    return -1;
  }

  if (dataPos + sizeof(int16_t) > pRaw->dataLen) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_LEN;
    return -1;
  }

  *(int16_t *)(pRaw->pData + dataPos) = val;
  return 0;
}

int32_t sdbSetRawInt64(SSdbRaw *pRaw, int32_t dataPos, int64_t val) {
  if (pRaw == NULL) {
    terrno = TSDB_CODE_INVALID_PTR;
    return -1;
  }

  if (dataPos + sizeof(int64_t) > pRaw->dataLen) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_LEN;
    return -1;
  }

  *(int64_t *)(pRaw->pData + dataPos) = val;
  return 0;
}

int32_t sdbSetRawBinary(SSdbRaw *pRaw, int32_t dataPos, const char *pVal, int32_t valLen) {
  if (pRaw == NULL) {
    terrno = TSDB_CODE_INVALID_PTR;
    return -1;
  }

  if (dataPos + valLen > pRaw->dataLen) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_LEN;
    return -1;
  }

  if (pVal != NULL) {
    memcpy(pRaw->pData + dataPos, pVal, valLen);
  }
  return 0;
}

int32_t sdbSetRawDataLen(SSdbRaw *pRaw, int32_t dataLen) {
  if (pRaw == NULL) {
    terrno = TSDB_CODE_INVALID_PTR;
    return -1;
  }

  if (dataLen > pRaw->dataLen) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_LEN;
    return -1;
  }

  pRaw->dataLen = dataLen;
  return 0;
}

int32_t sdbSetRawStatus(SSdbRaw *pRaw, ESdbStatus status) {
  if (pRaw == NULL) {
    terrno = TSDB_CODE_INVALID_PTR;
    return -1;
  }

  if (status == SDB_STATUS_INIT) {
    terrno = TSDB_CODE_INVALID_PARA;
    return -1;
  }

  pRaw->status = status;
  return 0;
}

int32_t sdbGetRawInt8(SSdbRaw *pRaw, int32_t dataPos, int8_t *val) {
  if (pRaw == NULL) {
    terrno = TSDB_CODE_INVALID_PTR;
    return -1;
  }

  if (dataPos + sizeof(int8_t) > pRaw->dataLen) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_LEN;
    return -1;
  }

  *val = *(int8_t *)(pRaw->pData + dataPos);
  return 0;
}

int32_t sdbGetRawInt32(SSdbRaw *pRaw, int32_t dataPos, int32_t *val) {
  if (pRaw == NULL) {
    terrno = TSDB_CODE_INVALID_PTR;
    return -1;
  }

  if (dataPos + sizeof(int32_t) > pRaw->dataLen) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_LEN;
    return -1;
  }

  *val = *(int32_t *)(pRaw->pData + dataPos);
  return 0;
}

int32_t sdbGetRawInt16(SSdbRaw *pRaw, int32_t dataPos, int16_t *val) {
  if (pRaw == NULL) {
    terrno = TSDB_CODE_INVALID_PTR;
    return -1;
  }

  if (dataPos + sizeof(int16_t) > pRaw->dataLen) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_LEN;
    return -1;
  }

  *val = *(int16_t *)(pRaw->pData + dataPos);
  return 0;
}

int32_t sdbGetRawInt64(SSdbRaw *pRaw, int32_t dataPos, int64_t *val) {
  if (pRaw == NULL) {
    terrno = TSDB_CODE_INVALID_PTR;
    return -1;
  }

  if (dataPos + sizeof(int64_t) > pRaw->dataLen) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_LEN;
    return -1;
  }

  *val = *(int64_t *)(pRaw->pData + dataPos);
  return 0;
}

int32_t sdbGetRawBinary(SSdbRaw *pRaw, int32_t dataPos, char *pVal, int32_t valLen) {
  if (pRaw == NULL) {
    terrno = TSDB_CODE_INVALID_PTR;
    return -1;
  }

  if (dataPos + valLen > pRaw->dataLen) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_LEN;
    return -1;
  }
  if (pVal != NULL) {
    memcpy(pVal, pRaw->pData + dataPos, valLen);
  }
  return 0;
}

int32_t sdbGetRawSoftVer(SSdbRaw *pRaw, int8_t *sver) {
  if (pRaw == NULL) {
    terrno = TSDB_CODE_INVALID_PTR;
    return -1;
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
