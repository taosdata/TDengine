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
#include "cfgInt.h"

SConfig *cfgInit() {
  SConfig *pConfig = calloc(1, sizeof(SConfig));
  if (pConfig == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pConfig->hash = taosHashInit(64, MurmurHash3_32, false, HASH_NO_LOCK);
  if (pConfig->hash == NULL) {
    free(pConfig);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  return pConfig;
}

int32_t cfgLoad(SConfig *pConfig, ECfgSrcType cfgType, const char *sourceStr) {
  switch (cfgType) {
    case CFG_STYPE_CFG_FILE:
      return cfgLoadFromCfgFile(pConfig, sourceStr);
    case CFG_STYPE_ENV_FILE:
      return cfgLoadFromEnvFile(pConfig, sourceStr);
    case CFG_STYPE_ENV_VAR:
      return cfgLoadFromEnvVar(pConfig);
    case CFG_STYPE_APOLLO_URL:
      return cfgLoadFromApollUrl(pConfig, sourceStr);
    default:
      return -1;
  }
}

void cfgCleanup(SConfig *pConfig) {
  if (pConfig != NULL) {
    if (pConfig->hash != NULL) {
      taosHashCleanup(pConfig->hash);
      pConfig->hash == NULL;
    }
    free(pConfig);
  }
}

int32_t cfgGetSize(SConfig *pConfig) { return taosHashGetSize(pConfig->hash); }

SConfigItem *cfgIterate(SConfig *pConfig, SConfigItem *pIter) { return taosHashIterate(pConfig->hash, pIter); }

void cfgCancelIterate(SConfig *pConfig, SConfigItem *pIter) { return taosHashCancelIterate(pConfig->hash, pIter); }


int32_t cfgSetBool(SConfigItem *pItem, const char *value, ECfgSrcType stype) {
  bool tmp = false;
  if (strcasecmp(value, "true") == 0) {
    tmp = true;
  }
  if (atoi(value) > 0) {
    tmp = true;
  }
  pItem->boolVal = tmp;
  pItem->stype = stype;
  return 0;
}

int32_t cfgSetInt8(SConfigItem *pItem, const char *value, ECfgSrcType stype) {
  int8_t ival = (int8_t)atoi(value);
  if (ival < pItem->minIntVal || ival > pItem->maxIntVal) {
    uError("cfg:%s, type:%s src:%s value:%d out of range[%" PRId64 ", %" PRId64 "], use last src:%s value:%d",
           pItem->name, cfgDtypeStr(pItem->dtype), cfgStypeStr(stype), ival, pItem->minIntVal, pItem->maxIntVal,
           cfgStypeStr(pItem->stype), pItem->int8Val);
    terrno = TSDB_CODE_OUT_OF_RANGE;
    return -1;
  }
  pItem->int8Val = ival;
  pItem->stype = stype;
  return 0;
}

int32_t cfgSetUInt16(SConfigItem *pItem, const char *value, ECfgSrcType stype) {
  uint16_t ival = (uint16_t)atoi(value);
  if (ival < pItem->minIntVal || ival > pItem->maxIntVal) {
    uError("cfg:%s, type:%s src:%s value:%d out of range[%" PRId64 ", %" PRId64 "], use last src:%s value:%d",
           pItem->name, cfgDtypeStr(pItem->dtype), cfgStypeStr(stype), ival, pItem->minIntVal, pItem->maxIntVal,
           cfgStypeStr(pItem->stype), pItem->uint16Val);
    terrno = TSDB_CODE_OUT_OF_RANGE;
    return -1;
  }
  pItem->uint16Val = ival;
  pItem->stype = stype;
  return 0;
}

int32_t cfgSetInt32(SConfigItem *pItem, const char *value, ECfgSrcType stype) {
  int32_t ival = (int32_t)atoi(value);
  if (ival < pItem->minIntVal || ival > pItem->maxIntVal) {
    uError("cfg:%s, type:%s src:%s value:%d out of range[%" PRId64 ", %" PRId64 "], use last src:%s value:%d",
           pItem->name, cfgDtypeStr(pItem->dtype), cfgStypeStr(stype), ival, pItem->minIntVal, pItem->maxIntVal,
           cfgStypeStr(pItem->stype), pItem->int32Val);
    terrno = TSDB_CODE_OUT_OF_RANGE;
    return -1;
  }
  pItem->int32Val = ival;
  pItem->stype = stype;
  return 0;
}

int32_t cfgSetInt64(SConfigItem *pItem, const char *value, ECfgSrcType stype) {
  int64_t ival = (int64_t)atoi(value);
  if (ival < pItem->minIntVal || ival > pItem->maxIntVal) {
    uError("cfg:%s, type:%s src:%s value:%d out of range[%" PRId64 ", %" PRId64 "], use last src:%s value:%d",
           pItem->name, cfgDtypeStr(pItem->dtype), cfgStypeStr(stype), ival, pItem->minIntVal, pItem->maxIntVal,
           cfgStypeStr(pItem->stype), pItem->int64Val);
    terrno = TSDB_CODE_OUT_OF_RANGE;
    return -1;
  }
  pItem->int64Val = ival;
  pItem->stype = stype;
  return 0;
}

int32_t cfgSetFloat(SConfigItem *pItem, const char *value, ECfgSrcType stype) {
  float fval = (float)atof(value);
  if (fval < pItem->minFloatVal || fval > pItem->maxFloatVal) {
    uError("cfg:%s, type:%s src:%s value:%f out of range[%f, %f], use last src:%s value:%f", pItem->name,
           cfgDtypeStr(pItem->dtype), cfgStypeStr(stype), fval, pItem->minFloatVal, pItem->maxFloatVal,
           cfgStypeStr(pItem->stype), pItem->floatVal);
    terrno = TSDB_CODE_OUT_OF_RANGE;
    return -1;
  }
  pItem->floatVal = fval;
  pItem->stype = stype;
  return 0;
}

int32_t cfgSetString(SConfigItem *pItem, const char *value, ECfgSrcType stype) {
  char *tmp = strdup(value);
  if (tmp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    uError("cfg:%s, type:%s src:%s value:%s failed to dup since %s, use last src:%s value:%s", pItem->name,
           cfgDtypeStr(pItem->dtype), cfgStypeStr(stype), terrstr(), cfgStypeStr(pItem->stype), pItem->floatVal);
    return -1;
  }
  free(pItem->strVal);
  pItem->strVal = tmp;
  pItem->stype = stype;
  return 0;
}

int32_t cfgSetIpStr(SConfigItem *pItem, const char *value, ECfgSrcType stype) {
  char *tmp = strdup(value);
  if (tmp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    uError("cfg:%s, type:%s src:%s value:%s failed to dup since %s, use last src:%s value:%s", pItem->name,
           cfgDtypeStr(pItem->dtype), cfgStypeStr(stype), terrstr(), cfgStypeStr(pItem->stype), pItem->floatVal);
    return -1;
  }
  free(pItem->strVal);
  pItem->strVal = tmp;
  pItem->stype = stype;
  return 0;
}

int32_t cfgSetDir(SConfigItem *pItem, const char *value, ECfgSrcType stype) {
  char *tmp = strdup(value);
  if (tmp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    uError("cfg:%s, type:%s src:%s value:%s failed to dup since %s, use last src:%s value:%s", pItem->name,
           cfgDtypeStr(pItem->dtype), cfgStypeStr(stype), terrstr(), cfgStypeStr(pItem->stype), pItem->floatVal);
    return -1;
  }
  free(pItem->strVal);
  pItem->strVal = tmp;
  pItem->stype = stype;
  return 0;
}

int32_t cfgSetItem(SConfig *pConfig, const char *name, const char *value, ECfgSrcType stype) {
  SConfigItem *pItem = cfgGetItem(pConfig, name);
  if (pItem == NULL) {
    return -1;
  }

  switch (pItem->dtype) {
    case CFG_DTYPE_BOOL:
      return cfgSetBool(pItem, value, stype);
    case CFG_DTYPE_INT8:
      return cfgSetInt8(pItem, value, stype);
    case CFG_DTYPE_UINT16:
      return cfgSetUInt16(pItem, value, stype);
    case CFG_DTYPE_INT32:
      return cfgSetInt32(pItem, value, stype);
    case CFG_DTYPE_INT64:
      return cfgSetInt64(pItem, value, stype);
    case CFG_DTYPE_FLOAT:
      return cfgSetFloat(pItem, value, stype);
    case CFG_DTYPE_STRING:
      return cfgSetString(pItem, value, stype);
    case CFG_DTYPE_IPSTR:
      return cfgSetIpStr(pItem, value, stype);
    case CFG_DTYPE_DIR:
      return cfgSetFqdn(pItem, value, stype);
    case CFG_DTYPE_NONE:
    default:
      break;
  }

  terrno = TSDB_CODE_INVALID_CFG;
  return -1;
}

SConfigItem *cfgGetItem(SConfig *pConfig, const char *name) {
  char lowcaseName[128] = 0;
  memcpy(lowcaseName, name, 127);

  SConfigItem *pItem = taosHashGet(pConfig->hash, lowcaseName, strlen(lowcaseName) + 1);
  if (pItem == NULL) {
    terrno = TSDB_CODE_CFG_NOT_FOUND;
  }

  return pItem;
}

static int32_t cfgAddItem(SConfig *pConfig, SConfigItem *pItem, const char *name, ECfgUnitType utype) {
  pItem->stype = CFG_STYPE_DEFAULT;
  pItem->utype = utype;
  pItem->name = strdup(name);
  if (pItem->name == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  char lowcaseName[128] = 0;
  memcpy(lowcaseName, name, 127);
  if (taosHashPut(pConfig->hash, lowcaseName, strlen(lowcaseName) + 1, pItem, sizeof(SConfigItem)) != 0) {
    if (pItem->dtype == CFG_DTYPE_STRING) {
      free(pItem->strVal);
    } else if (pItem->dtype == CFG_DTYPE_IPSTR) {
      free(pItem->ipstrVal);
    } else if (pItem->dtype == CFG_DTYPE_DIR) {
      free(pItem->dirVal);
    }
    free(pItem->name);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

int32_t cfgAddBool(SConfig *pConfig, const char *name, bool defaultVal, ECfgUnitType utype) {
  SConfigItem item = {.dtype = CFG_DTYPE_BOOL, .boolVal = defaultVal};
  return cfgAddItem(pConfig, &item, name, utype);
}

int32_t cfgAddInt8(SConfig *pConfig, const char *name, int8_t defaultVal, int64_t minval, int64_t maxval,
                   ECfgUnitType utype) {
  if (defaultVal < minval || defaultVal > maxval) {
    terrno = TSDB_CODE_OUT_OF_RANGE;
    return -1;
  }

  SConfigItem item = {.dtype = CFG_DTYPE_INT8, .int8Val = defaultVal, .minIntVal = minval, .maxIntVal = maxval};
  return cfgAddItem(pConfig, &item, name, utype);
}

int32_t cfgAddUInt16(SConfig *pConfig, const char *name, uint16_t defaultVal, int64_t minval, int64_t maxval,
                     ECfgUnitType utype) {
  if (defaultVal < minval || defaultVal > maxval) {
    terrno = TSDB_CODE_OUT_OF_RANGE;
    return -1;
  }

  SConfigItem item = {.dtype = CFG_DTYPE_UINT16, .uint16Val = defaultVal, .minIntVal = minval, .maxIntVal = maxval};
  return cfgAddItem(pConfig, &item, name, utype);
}

int32_t cfgAddInt32(SConfig *pConfig, const char *name, int32_t defaultVal, int64_t minval, int64_t maxval,
                    ECfgUnitType utype) {
  if (defaultVal < minval || defaultVal > maxval) {
    terrno = TSDB_CODE_OUT_OF_RANGE;
    return -1;
  }

  SConfigItem item = {.dtype = CFG_DTYPE_INT32, .int32Val = defaultVal, .minIntVal = minval, .maxIntVal = maxval};
  return cfgAddItem(pConfig, &item, name, utype);
}

int32_t cfgAddInt64(SConfig *pConfig, const char *name, int64_t defaultVal, int64_t minval, int64_t maxval,
                    ECfgUnitType utype) {
  if (defaultVal < minval || defaultVal > maxval) {
    terrno = TSDB_CODE_OUT_OF_RANGE;
    return -1;
  }

  SConfigItem item = {.dtype = CFG_DTYPE_INT64, .int64Val = defaultVal, .minIntVal = minval, .maxIntVal = maxval};
  return cfgAddItem(pConfig, &item, name, utype);
}

int32_t cfgAddFloat(SConfig *pConfig, const char *name, float defaultVal, double minval, double maxval,
                    ECfgUnitType utype) {
  if (defaultVal < minval || defaultVal > maxval) {
    terrno = TSDB_CODE_OUT_OF_RANGE;
    return -1;
  }

  SConfigItem item = {.dtype = CFG_DTYPE_FLOAT, .floatVal = defaultVal, .minFloatVal = minval, .maxFloatVal = maxval};
  return cfgAddItem(pConfig, &item, name, utype);
}

int32_t cfgAddString(SConfig *pConfig, const char *name, const char *defaultVal, ECfgUnitType utype) {
  if (defaultVal == NULL) {
    terrno = TSDB_CODE_OUT_OF_RANGE;
    return -1;
  }

  SConfigItem item = {.dtype = CFG_DTYPE_STRING};
  item.strVal = strdup(defaultVal);
  if (item.strVal == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  return cfgAddItem(pConfig, &item, name, utype);
}

static int32_t cfgCheckIpStr(const char *ip) {
  uint32_t value = taosInetAddr(ip);
  if (value == INADDR_NONE) {
    uError("ip:%s is not a valid ip address", ip);
    return -1;
  }

  return 0;
}

int32_t cfgAddIpStr(SConfig *pConfig, const char *name, const char *defaultVal, ECfgUnitType utype) {
  if (cfgCheckIpStr(defaultVal) != 0) {
    terrno = TSDB_CODE_OUT_OF_RANGE;
    return -1;
  }

  SConfigItem item = {.dtype = CFG_DTYPE_IPSTR};
  item.ipstrVal = strdup(defaultVal);
  if (item.ipstrVal == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  return cfgAddItem(pConfig, &item, name, utype);
}

static int32_t cfgCheckAndSetDir(SConfigItem *pItem, const char *inputDir) {
  char fullDir[PATH_MAX] = {0};
  if (taosExpandDir(inputDir, fullDir, PATH_MAX) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    uError("failed to expand dir:%s since %s", inputDir, terrstr());
    return -1;
  }

  if (taosRealPath(fullDir, PATH_MAX) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    uError("failed to get realpath of dir:%s since %s", inputDir, terrstr());
    return -1;
  }

  if (taosMkDir(fullDir) != 0) {
    uError("failed to create dir:%s realpath:%s since %s", inputDir, fullDir, terrstr());
    return -1;
  }

  tfree(pItem->dirVal);
  pItem->dirVal = strdup(fullDir);
  if (pItem->dirVal == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

int32_t cfgAddDir(SConfig *pConfig, const char *name, const char *defaultVal, ECfgUnitType utype) {
  SConfigItem item = {.dtype = CFG_DTYPE_DIR};
  if (cfgCheckAndSetDir(&item, defaultVal) != 0) {
    return -1;
  }
  return cfgAddItem(pConfig, &item, name, utype);
}

const char *cfgStypeStr(ECfgSrcType type) {
  switch (type) {
    case CFG_STYPE_DEFAULT:
      return "default";
    case CFG_STYPE_CFG_FILE:
      return "cfg_file";
    case CFG_STYPE_ENV_FILE:
      return "env_file";
    case CFG_STYPE_ENV_VAR:
      return "env_var";
    case CFG_STYPE_APOLLO_URL:
      return "apollo_url";
    default:
      return "invalid";
  }
}

const char *cfgDtypeStr(ECfgDataType type) {
  switch (type) {
    case CFG_DTYPE_NONE:
      return "none";
    case CFG_DTYPE_BOOL:
      return "bool";
    case CFG_DTYPE_INT8:
      return "int8";
    case CFG_DTYPE_UINT16:
      return "uint16";
    case CFG_DTYPE_INT32:
      return "int32";
    case CFG_DTYPE_INT64:
      return "int64";
    case CFG_DTYPE_FLOAT:
      return "float";
    case CFG_DTYPE_STRING:
      return "string";
    case CFG_DTYPE_IPSTR:
      return "ipstr";
    case CFG_DTYPE_DIR:
      return "dir";
    default:
      return "invalid";
  }
}

const char *cfgUtypeStr(ECfgUnitType type) {
  switch (type) {
    case CFG_UTYPE_NONE:
      return "";
    case CFG_UTYPE_PERCENT:
      return "(%)";
    case CFG_UTYPE_GB:
      return "(GB)";
    case CFG_UTYPE_MB:
      return "(Mb)";
    case CFG_UTYPE_BYTE:
      return "(byte)";
    case CFG_UTYPE_SECOND:
      return "(s)";
    case CFG_UTYPE_MS:
      return "(ms)";
    default:
      return "invalid";
  }
}