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
 * along with this program. If not, see <http:www.gnu.org/licenses/>.
 */

#define _DEFAULT_SOURCE
#include "mmInt.h"
#include "sdb.h"
#include "tencrypt.h"
#include "tjson.h"

static int32_t mmDecodeOption(SJson *pJson, SMnodeOpt *pOption) {
  int32_t code = 0;

  tjsonGetInt32ValueFromDouble(pJson, "deployed", pOption->deploy, code);
  if (code < 0) return code;
  tjsonGetInt32ValueFromDouble(pJson, "version", pOption->version, code);
  if (code < 0) return code;
  tjsonGetInt32ValueFromDouble(pJson, "selfIndex", pOption->selfIndex, code);
  if (code < 0) return code;
  tjsonGetInt32ValueFromDouble(pJson, "lastIndex", pOption->lastIndex, code);
  if (code < 0) return code;

  // Read encrypted flag (optional, defaults to false for backward compatibility)
  int32_t encrypted = 0;
  tjsonGetInt32ValueFromDouble(pJson, "encrypted", encrypted, code);
  pOption->encrypted = (encrypted != 0);
  // Reset code to 0 if encrypted field not found (backward compatibility)
  code = 0;

  SJson *replicas = tjsonGetObjectItem(pJson, "replicas");
  if (replicas == NULL) return 0;
  pOption->numOfTotalReplicas = tjsonGetArraySize(replicas);

  pOption->numOfReplicas = 0;

  for (int32_t i = 0; i < pOption->numOfTotalReplicas; ++i) {
    SJson *replica = tjsonGetArrayItem(replicas, i);
    if (replica == NULL) return TSDB_CODE_INVALID_JSON_FORMAT;

    SReplica *pReplica = pOption->replicas + i;
    tjsonGetInt32ValueFromDouble(replica, "id", pReplica->id, code);
    if (code < 0) return code;
    code = tjsonGetStringValue(replica, "fqdn", pReplica->fqdn, sizeof(pReplica->fqdn));
    if (code < 0) return code;
    tjsonGetUInt16ValueFromDouble(replica, "port", pReplica->port, code);
    if (code < 0) return code;
    tjsonGetInt32ValueFromDouble(replica, "role", pOption->nodeRoles[i], code);
    if (code < 0) return code;
    if (pOption->nodeRoles[i] == TAOS_SYNC_ROLE_VOTER) {
      pOption->numOfReplicas++;
    }
  }

  for (int32_t i = 0; i < pOption->numOfTotalReplicas; ++i) {
  }

  return 0;
}

int32_t mmReadFile(const char *path, SMnodeOpt *pOption) {
  int32_t   code = -1;
  char     *pData = NULL;
  int32_t   dataLen = 0;
  SJson    *pJson = NULL;
  char      file[PATH_MAX] = {0};

  int32_t nBytes = snprintf(file, sizeof(file), "%s%smnode.json", path, TD_DIRSEP);
  if (nBytes <= 0 || nBytes >= sizeof(file)) {
    code = TSDB_CODE_OUT_OF_BUFFER;
    goto _OVER;
  }

  if (taosStatFile(file, NULL, NULL, NULL) < 0) {
    dInfo("mnode file:%s not exist, reason:%s", file, tstrerror(terrno));
    return 0;
  }

  // Read file with potential decryption
  // First try to read with taosReadCfgFile (supports encrypted files)
  code = taosReadCfgFile(file, &pData, &dataLen);
  if (code != 0) {
    dError("failed to read mnode file:%s since %s", file, tstrerror(code));
    goto _OVER;
  }

  pJson = tjsonParse(pData);
  if (pJson == NULL) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _OVER;
  }

  if ((code = mmDecodeOption(pJson, pOption)) < 0) {
    goto _OVER;
  }

  code = 0;
  dInfo("succeed to read mnode file %s, sdb.data encrypted flag:%d", file, pOption->encrypted);

_OVER:
  if (pData != NULL) taosMemoryFree(pData);
  if (pJson != NULL) cJSON_Delete(pJson);

  if (code != 0) {
    dError("failed to read mnode file:%s since %s", file, tstrerror(code));
  }
  return code;
}

static int32_t mmEncodeOption(SJson *pJson, const SMnodeOpt *pOption) {
  int32_t code = 0;
  if (pOption->deploy && pOption->numOfTotalReplicas > 0) {
    if ((code = tjsonAddDoubleToObject(pJson, "selfIndex", pOption->selfIndex)) < 0) return code;

    SJson *replicas = tjsonCreateArray();
    if (replicas == NULL) {
      return terrno;
    }
    if ((code = tjsonAddItemToObject(pJson, "replicas", replicas)) < 0) return code;

    for (int32_t i = 0; i < pOption->numOfTotalReplicas; ++i) {
      SJson *replica = tjsonCreateObject();
      if (replica == NULL) {
        return terrno;
      }

      const SReplica *pReplica = pOption->replicas + i;
      if ((code = tjsonAddDoubleToObject(replica, "id", pReplica->id)) < 0) return code;
      if ((code = tjsonAddStringToObject(replica, "fqdn", pReplica->fqdn)) < 0) return code;
      if ((code = tjsonAddDoubleToObject(replica, "port", pReplica->port)) < 0) return code;
      if ((code = tjsonAddDoubleToObject(replica, "role", pOption->nodeRoles[i])) < 0) return code;
      if ((code = tjsonAddItemToArray(replicas, replica)) < 0) return code;
    }
  }

  if ((code = tjsonAddDoubleToObject(pJson, "lastIndex", pOption->lastIndex)) < 0) return code;

  if ((code = tjsonAddDoubleToObject(pJson, "deployed", pOption->deploy)) < 0) return code;

  if ((code = tjsonAddDoubleToObject(pJson, "version", pOption->version)) < 0) return code;

  // Add encrypted flag
  if ((code = tjsonAddDoubleToObject(pJson, "encrypted", pOption->encrypted ? 1 : 0)) < 0) return code;

  return code;
}

int32_t mmWriteFile(const char *path, const SMnodeOpt *pOption) {
  int32_t   code = -1;
  char     *buffer = NULL;
  SJson    *pJson = NULL;
  char      realfile[PATH_MAX] = {0};

  int32_t nBytes = snprintf(realfile, sizeof(realfile), "%s%smnode.json", path, TD_DIRSEP);
  if (nBytes <= 0 || nBytes >= sizeof(realfile)) {
    code = TSDB_CODE_OUT_OF_BUFFER;
    goto _OVER;
  }

  // terrno = TSDB_CODE_OUT_OF_MEMORY;
  pJson = tjsonCreateObject();
  if (pJson == NULL) {
    code = terrno;
    goto _OVER;
  }

  TAOS_CHECK_GOTO(mmEncodeOption(pJson, pOption), NULL, _OVER);

  buffer = tjsonToString(pJson);
  if (buffer == NULL) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _OVER;
  }

  int32_t len = strlen(buffer);

  // mnode.json itself is not encrypted, just write as plain JSON
  // The encrypted flag indicates whether sdb.data is encrypted
  code = taosWriteCfgFile(realfile, buffer, len);
  if (code != 0) {
    goto _OVER;
  }

  dInfo("succeed to write mnode file:%s, deployed:%d, sdb.data encrypted:%d", realfile, pOption->deploy,
        pOption->encrypted);

_OVER:
  if (pJson != NULL) tjsonDelete(pJson);
  if (buffer != NULL) taosMemoryFree(buffer);

  if (code != 0) {
    dError("failed to write mnode file:%s since %s, deloyed:%d", realfile, tstrerror(code), pOption->deploy);
  }
  return code;
}

// Update and persist encrypted flag (exposed via mnode.h for sdb module)
int32_t mndSetEncryptedFlag(SSdb *pSdb) {
  int32_t   code = 0;
  SMnodeOpt option = {0};

  if (pSdb == NULL || pSdb->mnodePath[0] == '\0') {
    dError("invalid parameters, pSdb:%p", pSdb);
    return TSDB_CODE_INVALID_PARA;
  }

  // Read current mnode.json
  code = mmReadFile(pSdb->mnodePath, &option);
  if (code != 0) {
    dError("failed to read mnode.json for setting encrypted flag since %s", tstrerror(code));
    return code;
  }

  // Update encrypted flag
  option.encrypted = true;
  pSdb->encrypted = true;

  // Write back to mnode.json
  code = mmWriteFile(pSdb->mnodePath, &option);
  if (code != 0) {
    dError("failed to persist encrypted flag to mnode.json since %s", tstrerror(code));
    // Rollback in-memory flag
    pSdb->encrypted = false;
    return code;
  }

  dInfo("successfully set and persisted encrypted flag in mnode.json");
  return 0;
}
