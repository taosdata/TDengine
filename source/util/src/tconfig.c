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
#include "tconfig.h"
#include "cJSON.h"
#include "taoserror.h"
#include "tenv.h"
#include "tgrant.h"
#include "tjson.h"
#include "tlog.h"
#include "tutil.h"
#include "tunit.h"

#define CFG_NAME_PRINT_LEN 24
#define CFG_SRC_PRINT_LEN  12

int32_t cfgLoadFromCfgFile(SConfig *pConfig, const char *filepath);
int32_t cfgLoadFromEnvFile(SConfig *pConfig, const char *filepath);
int32_t cfgLoadFromEnvVar(SConfig *pConfig);
int32_t cfgLoadFromEnvCmd(SConfig *pConfig, const char **envCmd);
int32_t cfgLoadFromApollUrl(SConfig *pConfig, const char *url);
int32_t cfgSetItem(SConfig *pConfig, const char *name, const char *value, ECfgSrcType stype);

extern char **environ;

SConfig *cfgInit() {
  SConfig *pCfg = taosMemoryCalloc(1, sizeof(SConfig));
  if (pCfg == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pCfg->array = taosArrayInit(32, sizeof(SConfigItem));
  if (pCfg->array == NULL) {
    taosMemoryFree(pCfg);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  return pCfg;
}

int32_t cfgLoad(SConfig *pCfg, ECfgSrcType cfgType, const void *sourceStr) {
  switch (cfgType) {
    case CFG_STYPE_CFG_FILE:
      return cfgLoadFromCfgFile(pCfg, sourceStr);
    case CFG_STYPE_ENV_FILE:
      return cfgLoadFromEnvFile(pCfg, sourceStr);
    case CFG_STYPE_ENV_VAR:
      return cfgLoadFromEnvVar(pCfg);
    case CFG_STYPE_APOLLO_URL:
      return cfgLoadFromApollUrl(pCfg, sourceStr);
    case CFG_STYPE_ENV_CMD:
      return cfgLoadFromEnvCmd(pCfg, (const char **)sourceStr);
    default:
      return -1;
  }
}

int32_t cfgLoadFromArray(SConfig *pCfg, SArray *pArgs) {
  int32_t size = taosArrayGetSize(pArgs);
  for (int32_t i = 0; i < size; ++i) {
    SConfigPair *pPair = taosArrayGet(pArgs, i);
    if (cfgSetItem(pCfg, pPair->name, pPair->value, CFG_STYPE_ARG_LIST) != 0) {
      return -1;
    }
  }

  return 0;
}

static void cfgFreeItem(SConfigItem *pItem) {
  if (pItem->dtype == CFG_DTYPE_STRING || pItem->dtype == CFG_DTYPE_DIR || pItem->dtype == CFG_DTYPE_LOCALE ||
      pItem->dtype == CFG_DTYPE_CHARSET || pItem->dtype == CFG_DTYPE_TIMEZONE) {
    taosMemoryFreeClear(pItem->str);
  }
  if (pItem->array) {
    taosArrayDestroy(pItem->array);
    pItem->array = NULL;
  }
}

void cfgCleanup(SConfig *pCfg) {
  if (pCfg != NULL) {
    int32_t size = taosArrayGetSize(pCfg->array);
    for (int32_t i = 0; i < size; ++i) {
      SConfigItem *pItem = taosArrayGet(pCfg->array, i);
      cfgFreeItem(pItem);
      taosMemoryFreeClear(pItem->name);
    }
    taosArrayDestroy(pCfg->array);
    taosMemoryFree(pCfg);
  }
}

int32_t cfgGetSize(SConfig *pCfg) { return taosArrayGetSize(pCfg->array); }

static int32_t cfgCheckAndSetTimezone(SConfigItem *pItem, const char *timezone) {
  cfgFreeItem(pItem);
  pItem->str = taosStrdup(timezone);
  if (pItem->str == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

static int32_t cfgCheckAndSetCharset(SConfigItem *pItem, const char *charset) {
  cfgFreeItem(pItem);
  pItem->str = taosStrdup(charset);
  if (pItem->str == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

static int32_t cfgCheckAndSetLocale(SConfigItem *pItem, const char *locale) {
  cfgFreeItem(pItem);
  pItem->str = taosStrdup(locale);
  if (pItem->str == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

static int32_t cfgCheckAndSetDir(SConfigItem *pItem, const char *inputDir) {
  char fullDir[PATH_MAX] = {0};
  if (taosExpandDir(inputDir, fullDir, PATH_MAX) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    uError("failed to expand dir:%s since %s", inputDir, terrstr());
    return -1;
  }

  taosMemoryFreeClear(pItem->str);
  pItem->str = taosStrdup(fullDir);
  if (pItem->str == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

static int32_t cfgSetBool(SConfigItem *pItem, const char *value, ECfgSrcType stype) {
  bool tmp = false;
  if (strcasecmp(value, "true") == 0) {
    tmp = true;
  }
  if (atoi(value) > 0) {
    tmp = true;
  }

  pItem->bval = tmp;
  pItem->stype = stype;
  return 0;
}

static int32_t cfgSetInt32(SConfigItem *pItem, const char *value, ECfgSrcType stype) {
  int32_t ival = taosStrHumanToInt32(value);
  if (ival < pItem->imin || ival > pItem->imax) {
    uError("cfg:%s, type:%s src:%s value:%d out of range[%" PRId64 ", %" PRId64 "]", pItem->name,
           cfgDtypeStr(pItem->dtype), cfgStypeStr(stype), ival, pItem->imin, pItem->imax);
    terrno = TSDB_CODE_OUT_OF_RANGE;
    return -1;
  }

  pItem->i32 = ival;
  pItem->stype = stype;
  return 0;
}

static int32_t cfgSetInt64(SConfigItem *pItem, const char *value, ECfgSrcType stype) {
  int64_t ival = taosStrHumanToInt64(value);
  if (ival < pItem->imin || ival > pItem->imax) {
    uError("cfg:%s, type:%s src:%s value:%" PRId64 " out of range[%" PRId64 ", %" PRId64 "]", pItem->name,
           cfgDtypeStr(pItem->dtype), cfgStypeStr(stype), ival, pItem->imin, pItem->imax);
    terrno = TSDB_CODE_OUT_OF_RANGE;
    return -1;
  }

  pItem->i64 = ival;
  pItem->stype = stype;
  return 0;
}

static int32_t cfgSetFloat(SConfigItem *pItem, const char *value, ECfgSrcType stype) {
  float fval = (float)atof(value);
  if (fval < pItem->fmin || fval > pItem->fmax) {
    uError("cfg:%s, type:%s src:%s value:%f out of range[%f, %f]", pItem->name, cfgDtypeStr(pItem->dtype),
           cfgStypeStr(stype), fval, pItem->fmin, pItem->fmax);
    terrno = TSDB_CODE_OUT_OF_RANGE;
    return -1;
  }

  pItem->fval = fval;
  pItem->stype = stype;
  return 0;
}

static int32_t cfgSetString(SConfigItem *pItem, const char *value, ECfgSrcType stype) {
  char *tmp = taosStrdup(value);
  if (tmp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    uError("cfg:%s, type:%s src:%s value:%s failed to dup since %s", pItem->name, cfgDtypeStr(pItem->dtype),
           cfgStypeStr(stype), value, terrstr());
    return -1;
  }

  taosMemoryFree(pItem->str);
  pItem->str = tmp;
  pItem->stype = stype;
  return 0;
}

static int32_t cfgSetDir(SConfigItem *pItem, const char *value, ECfgSrcType stype) {
  if (cfgCheckAndSetDir(pItem, value) != 0) {
    uError("cfg:%s, type:%s src:%s value:%s failed to dup since %s", pItem->name, cfgDtypeStr(pItem->dtype),
           cfgStypeStr(stype), value, terrstr());
    return -1;
  }

  pItem->stype = stype;
  return 0;
}

static int32_t cfgSetLocale(SConfigItem *pItem, const char *value, ECfgSrcType stype) {
  if (cfgCheckAndSetLocale(pItem, value) != 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    uError("cfg:%s, type:%s src:%s value:%s failed to dup since %s", pItem->name, cfgDtypeStr(pItem->dtype),
           cfgStypeStr(stype), value, terrstr());
    return -1;
  }

  pItem->stype = stype;
  return 0;
}

static int32_t cfgSetCharset(SConfigItem *pItem, const char *value, ECfgSrcType stype) {
  if (cfgCheckAndSetCharset(pItem, value) != 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    uError("cfg:%s, type:%s src:%s value:%s failed to dup since %s", pItem->name, cfgDtypeStr(pItem->dtype),
           cfgStypeStr(stype), value, terrstr());
    return -1;
  }

  pItem->stype = stype;
  return 0;
}

static int32_t cfgSetTimezone(SConfigItem *pItem, const char *value, ECfgSrcType stype) {
  if (cfgCheckAndSetTimezone(pItem, value) != 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    uError("cfg:%s, type:%s src:%s value:%s failed to dup since %s", pItem->name, cfgDtypeStr(pItem->dtype),
           cfgStypeStr(stype), value, terrstr());
    return -1;
  }
  pItem->stype = stype;

  // apply new timezone
  osSetTimezone(value);

  return 0;
}

static int32_t cfgSetTfsItem(SConfig *pCfg, const char *name, const char *value, const char *level, const char *primary,
                             ECfgSrcType stype) {
  SConfigItem *pItem = cfgGetItem(pCfg, name);
  if (pItem == NULL) return -1;

  if (pItem->array == NULL) {
    pItem->array = taosArrayInit(16, sizeof(SDiskCfg));
    if (pItem->array == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
  }

  SDiskCfg cfg = {0};
  tstrncpy(cfg.dir, pItem->str, sizeof(cfg.dir));
  cfg.level = level ? atoi(level) : 0;
  cfg.primary = primary ? atoi(primary) : 1;
  void *ret = taosArrayPush(pItem->array, &cfg);
  if (ret == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pItem->stype = stype;
  return 0;
}

static int32_t cfgUpdateDebugFlagItem(SConfig *pCfg, const char *name, bool resetArray) {
  SConfigItem *pDebugFlagItem = cfgGetItem(pCfg, "debugFlag");
  if (resetArray) {
      // reset
      if (pDebugFlagItem == NULL) return -1;

      // logflag names that should 'not' be set by 'debugFlag'
      if (pDebugFlagItem->array == NULL) {
        pDebugFlagItem->array = taosArrayInit(16, sizeof(SLogVar));
        if (pDebugFlagItem->array == NULL) {
          terrno = TSDB_CODE_OUT_OF_MEMORY;
          return -1;
        }
      }
      taosArrayClear(pDebugFlagItem->array);
      return 0;
  }

  // update
  if (pDebugFlagItem == NULL) return -1;
  if (pDebugFlagItem->array != NULL) {
    SLogVar logVar = {0};
    strncpy(logVar.name, name, TSDB_LOG_VAR_LEN - 1);
    taosArrayPush(pDebugFlagItem->array, &logVar);
  }
  return 0;
}

int32_t cfgSetItem(SConfig *pCfg, const char *name, const char *value, ECfgSrcType stype) {
  // GRANT_CFG_SET;
  SConfigItem *pItem = cfgGetItem(pCfg, name);
  if (pItem == NULL) {
    terrno = TSDB_CODE_CFG_NOT_FOUND;
    return -1;
  }

  switch (pItem->dtype) {
    case CFG_DTYPE_BOOL:
      return cfgSetBool(pItem, value, stype);
    case CFG_DTYPE_INT32:
      return cfgSetInt32(pItem, value, stype);
    case CFG_DTYPE_INT64:
      return cfgSetInt64(pItem, value, stype);
    case CFG_DTYPE_FLOAT:
    case CFG_DTYPE_DOUBLE:
      return cfgSetFloat(pItem, value, stype);
    case CFG_DTYPE_STRING:
      return cfgSetString(pItem, value, stype);
    case CFG_DTYPE_DIR:
      return cfgSetDir(pItem, value, stype);
    case CFG_DTYPE_TIMEZONE:
      return cfgSetTimezone(pItem, value, stype);
    case CFG_DTYPE_CHARSET:
      return cfgSetCharset(pItem, value, stype);
    case CFG_DTYPE_LOCALE:
      return cfgSetLocale(pItem, value, stype);
    case CFG_DTYPE_NONE:
    default:
      break;
  }

_err_out:
  terrno = TSDB_CODE_INVALID_CFG;
  return -1;
}

SConfigItem *cfgGetItem(SConfig *pCfg, const char *name) {
  if (pCfg == NULL) return NULL;
  int32_t size = taosArrayGetSize(pCfg->array);
  for (int32_t i = 0; i < size; ++i) {
    SConfigItem *pItem = taosArrayGet(pCfg->array, i);
    if (strcasecmp(pItem->name, name) == 0) {
      return pItem;
    }
  }

  // uError("name:%s, cfg not found", name);
  terrno = TSDB_CODE_CFG_NOT_FOUND;
  return NULL;
}

int32_t cfgCheckRangeForDynUpdate(SConfig *pCfg, const char *name, const char *pVal, bool isServer) {
  ECfgDynType  dynType = isServer ? CFG_DYN_SERVER : CFG_DYN_CLIENT;
  SConfigItem *pItem = cfgGetItem(pCfg, name);
  if (!pItem || (pItem->dynScope & dynType) == 0) {
    uError("failed to config:%s, not support", name);
    terrno = TSDB_CODE_INVALID_CFG;
    return -1;
  }

  switch (pItem->dtype) {
    case CFG_DTYPE_BOOL: {
      int32_t ival = (int32_t)atoi(pVal);
      if (ival != 0 && ival != 1) {
        uError("cfg:%s, type:%s value:%d out of range[0, 1]", pItem->name,
               cfgDtypeStr(pItem->dtype), ival);
        terrno = TSDB_CODE_OUT_OF_RANGE;
        return -1;
      }
    } break;
    case CFG_DTYPE_INT32: {
      int32_t ival = (int32_t)taosStrHumanToInt32(pVal);
      if (ival < pItem->imin || ival > pItem->imax) {
        uError("cfg:%s, type:%s value:%d out of range[%" PRId64 ", %" PRId64 "]", pItem->name,
               cfgDtypeStr(pItem->dtype), ival, pItem->imin, pItem->imax);
        terrno = TSDB_CODE_OUT_OF_RANGE;
        return -1;
      }
    } break;
    case CFG_DTYPE_INT64: {
      int64_t ival = (int64_t)taosStrHumanToInt64(pVal);
      if (ival < pItem->imin || ival > pItem->imax) {
        uError("cfg:%s, type:%s value:%" PRId64 " out of range[%" PRId64 ", %" PRId64 "]", pItem->name,
               cfgDtypeStr(pItem->dtype), ival, pItem->imin, pItem->imax);
        terrno = TSDB_CODE_OUT_OF_RANGE;
        return -1;
      }
    } break;
    case CFG_DTYPE_FLOAT:
    case CFG_DTYPE_DOUBLE: {
      float fval = (float)atof(pVal);
      if (fval < pItem->fmin || fval > pItem->fmax) {
        uError("cfg:%s, type:%s value:%f out of range[%f, %f]", pItem->name, cfgDtypeStr(pItem->dtype), fval,
               pItem->fmin, pItem->fmax);
        terrno = TSDB_CODE_OUT_OF_RANGE;
        return -1;
      }
    } break;
    default:
      break;
  }
  return 0;
}

static int32_t cfgAddItem(SConfig *pCfg, SConfigItem *pItem, const char *name) {
  pItem->stype = CFG_STYPE_DEFAULT;
  pItem->name = taosStrdup(name);
  if (pItem->name == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  int32_t len = strlen(name);
  char    lowcaseName[CFG_NAME_MAX_LEN + 1] = {0};
  strntolower(lowcaseName, name, TMIN(CFG_NAME_MAX_LEN, len));

  if (taosArrayPush(pCfg->array, pItem) == NULL) {
    if (pItem->dtype == CFG_DTYPE_STRING) {
      taosMemoryFree(pItem->str);
    }
    taosMemoryFree(pItem->name);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

int32_t cfgAddBool(SConfig *pCfg, const char *name, bool defaultVal, int8_t scope, int8_t dynScope) {
  SConfigItem item = {.dtype = CFG_DTYPE_BOOL, .bval = defaultVal, .scope = scope, .dynScope = dynScope};
  return cfgAddItem(pCfg, &item, name);
}

int32_t cfgAddInt32(SConfig *pCfg, const char *name, int32_t defaultVal, int64_t minval, int64_t maxval, int8_t scope,
                    int8_t dynScope) {
  if (defaultVal < minval || defaultVal > maxval) {
    terrno = TSDB_CODE_OUT_OF_RANGE;
    return -1;
  }

  SConfigItem item = {.dtype = CFG_DTYPE_INT32,
                      .i32 = defaultVal,
                      .imin = minval,
                      .imax = maxval,
                      .scope = scope,
                      .dynScope = dynScope};
  return cfgAddItem(pCfg, &item, name);
}

int32_t cfgAddInt64(SConfig *pCfg, const char *name, int64_t defaultVal, int64_t minval, int64_t maxval, int8_t scope,
                    int8_t dynScope) {
  if (defaultVal < minval || defaultVal > maxval) {
    terrno = TSDB_CODE_OUT_OF_RANGE;
    return -1;
  }

  SConfigItem item = {.dtype = CFG_DTYPE_INT64,
                      .i64 = defaultVal,
                      .imin = minval,
                      .imax = maxval,
                      .scope = scope,
                      .dynScope = dynScope};
  return cfgAddItem(pCfg, &item, name);
}

int32_t cfgAddFloat(SConfig *pCfg, const char *name, float defaultVal, float minval, float maxval, int8_t scope,
                    int8_t dynScope) {
  if (defaultVal < minval || defaultVal > maxval) {
    terrno = TSDB_CODE_OUT_OF_RANGE;
    return -1;
  }

  SConfigItem item = {.dtype = CFG_DTYPE_FLOAT,
                      .fval = defaultVal,
                      .fmin = minval,
                      .fmax = maxval,
                      .scope = scope,
                      .dynScope = dynScope};
  return cfgAddItem(pCfg, &item, name);
}

int32_t cfgAddString(SConfig *pCfg, const char *name, const char *defaultVal, int8_t scope, int8_t dynScope) {
  SConfigItem item = {.dtype = CFG_DTYPE_STRING, .scope = scope, .dynScope = dynScope};
  item.str = taosStrdup(defaultVal);
  if (item.str == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  return cfgAddItem(pCfg, &item, name);
}

int32_t cfgAddDir(SConfig *pCfg, const char *name, const char *defaultVal, int8_t scope, int8_t dynScope) {
  SConfigItem item = {.dtype = CFG_DTYPE_DIR, .scope = scope, .dynScope = dynScope};
  if (cfgCheckAndSetDir(&item, defaultVal) != 0) {
    return -1;
  }

  return cfgAddItem(pCfg, &item, name);
}

int32_t cfgAddLocale(SConfig *pCfg, const char *name, const char *defaultVal, int8_t scope, int8_t dynScope) {
  SConfigItem item = {.dtype = CFG_DTYPE_LOCALE, .scope = scope, .dynScope = dynScope};
  if (cfgCheckAndSetLocale(&item, defaultVal) != 0) {
    return -1;
  }

  return cfgAddItem(pCfg, &item, name);
}

int32_t cfgAddCharset(SConfig *pCfg, const char *name, const char *defaultVal, int8_t scope, int8_t dynScope) {
  SConfigItem item = {.dtype = CFG_DTYPE_CHARSET, .scope = scope, .dynScope = dynScope};
  if (cfgCheckAndSetCharset(&item, defaultVal) != 0) {
    return -1;
  }

  return cfgAddItem(pCfg, &item, name);
}

int32_t cfgAddTimezone(SConfig *pCfg, const char *name, const char *defaultVal, int8_t scope, int8_t dynScope) {
  SConfigItem item = {.dtype = CFG_DTYPE_TIMEZONE, .scope = scope, .dynScope = dynScope};
  if (cfgCheckAndSetTimezone(&item, defaultVal) != 0) {
    return -1;
  }

  return cfgAddItem(pCfg, &item, name);
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
    case CFG_STYPE_ARG_LIST:
      return "arg_list";
    case CFG_STYPE_TAOS_OPTIONS:
      return "taos_options";
    case CFG_STYPE_ENV_CMD:
      return "env_cmd";
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
    case CFG_DTYPE_INT32:
      return "int32";
    case CFG_DTYPE_INT64:
      return "int64";
    case CFG_DTYPE_FLOAT:
      return "float";
    case CFG_DTYPE_DOUBLE:
      return "double";
    case CFG_DTYPE_STRING:
      return "string";
    case CFG_DTYPE_DIR:
      return "dir";
    case CFG_DTYPE_LOCALE:
      return "locale";
    case CFG_DTYPE_CHARSET:
      return "charset";
    case CFG_DTYPE_TIMEZONE:
      return "timezone";
    default:
      return "invalid";
  }
}

void cfgDumpItemValue(SConfigItem *pItem, char *buf, int32_t bufSize, int32_t *pLen) {
  int32_t len = 0;
  switch (pItem->dtype) {
    case CFG_DTYPE_BOOL:
      len = snprintf(buf, bufSize, "%u", pItem->bval);
      break;
    case CFG_DTYPE_INT32:
      len = snprintf(buf, bufSize, "%d", pItem->i32);
      break;
    case CFG_DTYPE_INT64:
      len = snprintf(buf, bufSize, "%" PRId64, pItem->i64);
      break;
    case CFG_DTYPE_FLOAT:
    case CFG_DTYPE_DOUBLE:
      len = snprintf(buf, bufSize, "%f", pItem->fval);
      break;
    case CFG_DTYPE_STRING:
    case CFG_DTYPE_DIR:
    case CFG_DTYPE_LOCALE:
    case CFG_DTYPE_CHARSET:
    case CFG_DTYPE_TIMEZONE:
    case CFG_DTYPE_NONE:
      len = snprintf(buf, bufSize, "%s", pItem->str);
      break;
  }

  if (len > bufSize) {
    len = bufSize;
  }

  *pLen = len;
}

void cfgDumpItemScope(SConfigItem *pItem, char *buf, int32_t bufSize, int32_t *pLen) {
  int32_t len = 0;
  switch (pItem->scope) {
    case CFG_SCOPE_SERVER:
      len = snprintf(buf, bufSize, "server");
      break;
    case CFG_SCOPE_CLIENT:
      len = snprintf(buf, bufSize, "client");
      break;
    case CFG_SCOPE_BOTH:
      len = snprintf(buf, bufSize, "both");
      break;
  }

  if (len > bufSize) {
    len = bufSize;
  }

  *pLen = len;
}

void cfgDumpCfg(SConfig *pCfg, bool tsc, bool dump) {
  if (dump) {
    printf("                     global config");
    printf("\n");
    printf("=================================================================");
    printf("\n");
  } else {
    uInfo("                     global config");
    uInfo("=================================================================");
  }

  char src[CFG_SRC_PRINT_LEN + 1] = {0};
  char name[CFG_NAME_PRINT_LEN + 1] = {0};

  int32_t size = taosArrayGetSize(pCfg->array);
  for (int32_t i = 0; i < size; ++i) {
    SConfigItem *pItem = taosArrayGet(pCfg->array, i);
    if (tsc && pItem->scope == CFG_SCOPE_SERVER) continue;
    if (dump && strcmp(pItem->name, "scriptDir") == 0) continue;
    tstrncpy(src, cfgStypeStr(pItem->stype), CFG_SRC_PRINT_LEN);
    for (int32_t j = 0; j < CFG_SRC_PRINT_LEN; ++j) {
      if (src[j] == 0) src[j] = ' ';
    }

    tstrncpy(name, pItem->name, CFG_NAME_PRINT_LEN);
    for (int32_t j = 0; j < CFG_NAME_PRINT_LEN; ++j) {
      if (name[j] == 0) name[j] = ' ';
    }

    switch (pItem->dtype) {
      case CFG_DTYPE_BOOL:
        if (dump) {
          printf("%s %s %u\n", src, name, pItem->bval);
        } else {
          uInfo("%s %s %u", src, name, pItem->bval);
        }

        break;
      case CFG_DTYPE_INT32:
        if (dump) {
          printf("%s %s %d\n", src, name, pItem->i32);
        } else {
          uInfo("%s %s %d", src, name, pItem->i32);
        }
        break;
      case CFG_DTYPE_INT64:
        if (dump) {
          printf("%s %s %" PRId64"\n", src, name, pItem->i64);
        } else {
          uInfo("%s %s %" PRId64, src, name, pItem->i64);
        }
        break;
      case CFG_DTYPE_DOUBLE:
      case CFG_DTYPE_FLOAT:
        if (dump) {
          printf("%s %s %.2f\n", src, name, pItem->fval);
        } else {
          uInfo("%s %s %.2f", src, name, pItem->fval);
        }
        break;
      case CFG_DTYPE_STRING:
      case CFG_DTYPE_DIR:
      case CFG_DTYPE_LOCALE:
      case CFG_DTYPE_CHARSET:
      case CFG_DTYPE_TIMEZONE:
      case CFG_DTYPE_NONE:
        if (dump) {
          printf("%s %s %s\n", src, name, pItem->str);
        } else {
          uInfo("%s %s %s", src, name, pItem->str);
        }
        break;
    }
  }

  if (dump) {
    printf("=================================================================\n");
  } else {
    uInfo("=================================================================");
  }
}

int32_t cfgLoadFromEnvVar(SConfig *pConfig) {
  char    line[1024], *name, *value, *value2, *value3;
  int32_t olen, vlen, vlen2, vlen3;
  int32_t code = 0;
  char  **pEnv = environ;
  line[1023] = 0;

  if (pEnv == NULL) return 0;
  while (*pEnv != NULL) {
    name = value = value2 = value3 = NULL;
    olen = vlen = vlen2 = vlen3 = 0;

    strncpy(line, *pEnv, sizeof(line) - 1);
    pEnv++;
    taosEnvToCfg(line, line);

    paGetToken(line, &name, &olen);
    if (olen == 0) continue;
    name[olen] = 0;

    paGetToken(name + olen + 1, &value, &vlen);
    if (vlen == 0) continue;
    value[vlen] = 0;

    paGetToken(value + vlen + 1, &value2, &vlen2);
    if (vlen2 != 0) {
      value2[vlen2] = 0;
      paGetToken(value2 + vlen2 + 1, &value3, &vlen3);
      if (vlen3 != 0) value3[vlen3] = 0;
    }

    code = cfgSetItem(pConfig, name, value, CFG_STYPE_ENV_VAR);
    if (code != 0 && terrno != TSDB_CODE_CFG_NOT_FOUND) break;

    if (strcasecmp(name, "dataDir") == 0) {
      code = cfgSetTfsItem(pConfig, name, value, value2, value3, CFG_STYPE_ENV_VAR);
      if (code != 0 && terrno != TSDB_CODE_CFG_NOT_FOUND) break;
    }
  }

  uInfo("load from env variables cfg success");
  return 0;
}

int32_t cfgLoadFromEnvCmd(SConfig *pConfig, const char **envCmd) {
  char    buf[1024], *name, *value, *value2, *value3;
  int32_t olen, vlen, vlen2, vlen3;
  int32_t code = 0;
  int32_t index = 0;
  if (envCmd == NULL) return 0;
  while (envCmd[index] != NULL) {
    strncpy(buf, envCmd[index], sizeof(buf) - 1);
    buf[sizeof(buf) - 1] = 0;
    taosEnvToCfg(buf, buf);
    index++;

    name = value = value2 = value3 = NULL;
    olen = vlen = vlen2 = vlen3 = 0;

    paGetToken(buf, &name, &olen);
    if (olen == 0) continue;
    name[olen] = 0;

    paGetToken(name + olen + 1, &value, &vlen);
    if (vlen == 0) continue;
    value[vlen] = 0;

    paGetToken(value + vlen + 1, &value2, &vlen2);
    if (vlen2 != 0) {
      value2[vlen2] = 0;
      paGetToken(value2 + vlen2 + 1, &value3, &vlen3);
      if (vlen3 != 0) value3[vlen3] = 0;
    }

    code = cfgSetItem(pConfig, name, value, CFG_STYPE_ENV_CMD);
    if (code != 0 && terrno != TSDB_CODE_CFG_NOT_FOUND) break;

    if (strcasecmp(name, "dataDir") == 0) {
      code = cfgSetTfsItem(pConfig, name, value, value2, value3, CFG_STYPE_ENV_CMD);
      if (code != 0 && terrno != TSDB_CODE_CFG_NOT_FOUND) break;
    }
  }

  uInfo("load from env cmd cfg success");
  return 0;
}

int32_t cfgLoadFromEnvFile(SConfig *pConfig, const char *envFile) {
  char    line[1024], *name, *value, *value2, *value3;
  int32_t olen, vlen, vlen2, vlen3;
  int32_t code = 0;
  ssize_t _bytes = 0;

  const char *filepath = ".env";
  if (envFile != NULL && strlen(envFile) > 0) {
    if (!taosCheckExistFile(envFile)) {
      uError("failed to load env file:%s", envFile);
      return -1;
    }
    filepath = envFile;
  } else {
    if (!taosCheckExistFile(filepath)) {
      uInfo("env file:%s not load", filepath);
      return 0;
    }
  }

  TdFilePtr pFile = taosOpenFile(filepath, TD_FILE_READ | TD_FILE_STREAM);
  if (pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  while (!taosEOFFile(pFile)) {
    name = value = value2 = value3 = NULL;
    olen = vlen = vlen2 = vlen3 = 0;

    _bytes = taosGetsFile(pFile, sizeof(line), line);
    if (_bytes <= 0) {
      break;
    }
    if (line[_bytes - 1] == '\n') line[_bytes - 1] = 0;
    taosEnvToCfg(line, line);

    paGetToken(line, &name, &olen);
    if (olen == 0) continue;
    name[olen] = 0;

    paGetToken(name + olen + 1, &value, &vlen);
    if (vlen == 0) continue;
    value[vlen] = 0;

    paGetToken(value + vlen + 1, &value2, &vlen2);
    if (vlen2 != 0) {
      value2[vlen2] = 0;
      paGetToken(value2 + vlen2 + 1, &value3, &vlen3);
      if (vlen3 != 0) value3[vlen3] = 0;
    }

    code = cfgSetItem(pConfig, name, value, CFG_STYPE_ENV_FILE);
    if (code != 0 && terrno != TSDB_CODE_CFG_NOT_FOUND) break;

    if (strcasecmp(name, "dataDir") == 0) {
      code = cfgSetTfsItem(pConfig, name, value, value2, value3, CFG_STYPE_ENV_FILE);
      if (code != 0 && terrno != TSDB_CODE_CFG_NOT_FOUND) break;
    }
  }

  taosCloseFile(&pFile);

  uInfo("load from env cfg file %s success", filepath);
  return 0;
}

int32_t cfgLoadFromCfgFile(SConfig *pConfig, const char *filepath) {
  char    line[1024], *name, *value, *value2, *value3;
  int32_t olen, vlen, vlen2, vlen3;
  ssize_t _bytes = 0;
  int32_t code = 0;

  TdFilePtr pFile = taosOpenFile(filepath, TD_FILE_READ | TD_FILE_STREAM);
  if (pFile == NULL) {
    // success when the file does not exist
    if (errno == ENOENT) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      uInfo("failed to load from cfg file %s since %s, use default parameters", filepath, terrstr());
      return 0;
    } else {
      uError("failed to load from cfg file %s since %s", filepath, terrstr());
      return -1;
    }
  }

  while (!taosEOFFile(pFile)) {
    name = value = value2 = value3 = NULL;
    olen = vlen = vlen2 = vlen3 = 0;

    _bytes = taosGetsFile(pFile, sizeof(line), line);
    if (_bytes <= 0) {
      break;
    }

    if (line[_bytes - 1] == '\n') line[_bytes - 1] = 0;

    paGetToken(line, &name, &olen);
    if (olen == 0) continue;
    name[olen] = 0;

    paGetToken(name + olen + 1, &value, &vlen);
    if (vlen == 0) continue;
    value[vlen] = 0;

    paGetToken(value + vlen + 1, &value2, &vlen2);
    if (vlen2 != 0) {
      value2[vlen2] = 0;
      paGetToken(value2 + vlen2 + 1, &value3, &vlen3);
      if (vlen3 != 0) value3[vlen3] = 0;
    }

    code = cfgSetItem(pConfig, name, value, CFG_STYPE_CFG_FILE);
    if (code != 0 && terrno != TSDB_CODE_CFG_NOT_FOUND) break;

    if (strcasecmp(name, "dataDir") == 0) {
      code = cfgSetTfsItem(pConfig, name, value, value2, value3, CFG_STYPE_CFG_FILE);
      if (code != 0 && terrno != TSDB_CODE_CFG_NOT_FOUND) break;
    }

    size_t       len = strlen(name);
    const char  *debugFlagStr = "debugFlag";
    const size_t debugFlagLen = strlen(debugFlagStr);
    if (len >= debugFlagLen && strcasecmp(name + len - debugFlagLen, debugFlagStr) == 0) {
      code = cfgUpdateDebugFlagItem(pConfig, name, len == debugFlagLen);
      if (code != 0 && terrno != TSDB_CODE_CFG_NOT_FOUND) break;
    }
  }

  taosCloseFile(&pFile);

  if (code == 0 || (code != 0 && terrno == TSDB_CODE_CFG_NOT_FOUND)) {
    uInfo("load from cfg file %s success", filepath);
    return 0;
  } else {
    uError("failed to load from cfg file %s since %s", filepath, terrstr());
    return -1;
  }
}

// int32_t cfgLoadFromCfgText(SConfig *pConfig, const char *configText) {
//   char   *line = NULL, *name, *value, *value2, *value3;
//   int32_t olen, vlen, vlen2, vlen3;
//   ssize_t _bytes = 0;
//   int32_t code = 0;

//   TdFilePtr pFile = taosOpenFile(filepath, TD_FILE_READ | TD_FILE_STREAM);
//   if (pFile == NULL) {
//     // success when the file does not exist
//     if (errno == ENOENT) {
//       terrno = TAOS_SYSTEM_ERROR(errno);
//       uInfo("failed to load from cfg file %s since %s, use default parameters", filepath, terrstr());
//       return 0;
//     } else {
//       uError("failed to load from cfg file %s since %s", filepath, terrstr());
//       return -1;
//     }
//   }

//   while (!taosEOFFile(pFile)) {
//     name = value = value2 = value3 = NULL;
//     olen = vlen = vlen2 = vlen3 = 0;

//     _bytes = taosGetLineFile(pFile, &line);
//     if (_bytes <= 0) {
//       break;
//     }

//     if(line[_bytes - 1] == '\n') line[_bytes - 1] = 0;

//     paGetToken(line, &name, &olen);
//     if (olen == 0) continue;
//     name[olen] = 0;

//     paGetToken(name + olen + 1, &value, &vlen);
//     if (vlen == 0) continue;
//     value[vlen] = 0;

//     paGetToken(value + vlen + 1, &value2, &vlen2);
//     if (vlen2 != 0) {
//       value2[vlen2] = 0;
//       paGetToken(value2 + vlen2 + 1, &value3, &vlen3);
//       if (vlen3 != 0) value3[vlen3] = 0;
//     }

//     code = cfgSetItem(pConfig, name, value, CFG_STYPE_CFG_FILE);
//     if (code != 0 && terrno != TSDB_CODE_CFG_NOT_FOUND) break;
//     if (strcasecmp(name, "dataDir") == 0) {
//       code = cfgSetTfsItem(pConfig, name, value, value2, value3, CFG_STYPE_CFG_FILE);
//       if (code != 0 && terrno != TSDB_CODE_CFG_NOT_FOUND) break;
//     }
//   }

//   taosCloseFile(&pFile);
//   if (line != NULL) taosMemoryFreeClear(line);

//   if (code == 0 || (code != 0 && terrno == TSDB_CODE_CFG_NOT_FOUND)) {
//     uInfo("load from cfg file %s success", filepath);
//     return 0;
//   } else {
//     uError("failed to load from cfg file %s since %s", filepath, terrstr());
//     return -1;
//   }
// }

int32_t cfgLoadFromApollUrl(SConfig *pConfig, const char *url) {
  char   *cfgLineBuf = NULL, *name, *value, *value2, *value3;
  int32_t olen, vlen, vlen2, vlen3;
  int32_t code = 0;
  if (url == NULL || strlen(url) == 0) {
    uInfo("apoll url not load");
    return 0;
  }

  char *p = strchr(url, ':');
  if (p == NULL) {
    uError("fail to load apoll url: %s, unknown format", url);
    return -1;
  }
  p++;

  if (strncmp(url, "jsonFile", 8) == 0) {
    char *filepath = p;
    if (!taosCheckExistFile(filepath)) {
      uError("failed to load json file:%s", filepath);
      return -1;
    }

    TdFilePtr pFile = taosOpenFile(filepath, TD_FILE_READ);
    if (pFile == NULL) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      return -1;
    }
    size_t fileSize = taosLSeekFile(pFile, 0, SEEK_END);
    char  *buf = taosMemoryMalloc(fileSize);
    taosLSeekFile(pFile, 0, SEEK_SET);
    if (taosReadFile(pFile, buf, fileSize) <= 0) {
      taosCloseFile(&pFile);
      uError("load json file error: %s", filepath);
      taosMemoryFreeClear(buf);
      return -1;
    }
    taosCloseFile(&pFile);
    SJson *pJson = tjsonParse(buf);
    if (NULL == pJson) {
      const char *jsonParseError = tjsonGetError();
      if (jsonParseError != NULL) {
        uError("load json file parse error: %s", jsonParseError);
      }
      taosMemoryFreeClear(buf);
      return -1;
    }
    taosMemoryFreeClear(buf);

    int32_t jsonArraySize = tjsonGetArraySize(pJson);
    for (int32_t i = 0; i < jsonArraySize; i++) {
      cJSON *item = tjsonGetArrayItem(pJson, i);
      if (item == NULL) break;
      char *itemName = NULL, *itemValueString = NULL;
      tjsonGetObjectName(item, &itemName);
      tjsonGetObjectName(item, &itemName);
      tjsonGetObjectValueString(item, &itemValueString);
      if (itemValueString != NULL && itemName != NULL) {
        size_t itemNameLen = strlen(itemName);
        size_t itemValueStringLen = strlen(itemValueString);
        cfgLineBuf = taosMemoryMalloc(itemNameLen + itemValueStringLen + 2);
        memcpy(cfgLineBuf, itemName, itemNameLen);
        cfgLineBuf[itemNameLen] = ' ';
        memcpy(&cfgLineBuf[itemNameLen + 1], itemValueString, itemValueStringLen);
        cfgLineBuf[itemNameLen + itemValueStringLen + 1] = '\0';

        paGetToken(cfgLineBuf, &name, &olen);
        if (olen == 0) continue;
        name[olen] = 0;

        paGetToken(name + olen + 1, &value, &vlen);
        if (vlen == 0) continue;
        value[vlen] = 0;

        paGetToken(value + vlen + 1, &value2, &vlen2);
        if (vlen2 != 0) {
          value2[vlen2] = 0;
          paGetToken(value2 + vlen2 + 1, &value3, &vlen3);
          if (vlen3 != 0) value3[vlen3] = 0;
        }

        code = cfgSetItem(pConfig, name, value, CFG_STYPE_APOLLO_URL);
        if (code != 0 && terrno != TSDB_CODE_CFG_NOT_FOUND) break;

        if (strcasecmp(name, "dataDir") == 0) {
          code = cfgSetTfsItem(pConfig, name, value, value2, value3, CFG_STYPE_APOLLO_URL);
          if (code != 0 && terrno != TSDB_CODE_CFG_NOT_FOUND) break;
        }
      }
    }
    tjsonDelete(pJson);

    // } else if (strncmp(url, "jsonUrl", 7) == 0) {
    // } else if (strncmp(url, "etcdUrl", 7) == 0) {
  } else {
    uError("Unsupported url: %s", url);
    return -1;
  }

  uInfo("load from apoll url not implemented yet");
  return 0;
}

int32_t cfgGetApollUrl(const char **envCmd, const char *envFile, char *apolloUrl) {
  int32_t index = 0;
  if (envCmd == NULL) return 0;
  while (envCmd[index] != NULL) {
    if (strncmp(envCmd[index], "TAOS_APOLLO_URL", 14) == 0) {
      char *p = strchr(envCmd[index], '=');
      if (p != NULL) {
        p++;
        if (*p == '\'') {
          p++;
          p[strlen(p) - 1] = '\0';
        }
        memcpy(apolloUrl, p, TMIN(strlen(p) + 1, PATH_MAX));
        uInfo("get apollo url from env cmd success");
        return 0;
      }
    }
    index++;
  }

  char   line[1024];
  char **pEnv = environ;
  line[1023] = 0;
  while (*pEnv != NULL) {
    strncpy(line, *pEnv, sizeof(line) - 1);
    pEnv++;
    if (strncmp(line, "TAOS_APOLLO_URL", 14) == 0) {
      char *p = strchr(line, '=');
      if (p != NULL) {
        p++;
        if (*p == '\'') {
          p++;
          p[strlen(p) - 1] = '\0';
        }
        memcpy(apolloUrl, p, TMIN(strlen(p) + 1, PATH_MAX));
        uInfo("get apollo url from env variables success, apolloUrl=%s", apolloUrl);
        return 0;
      }
    }
  }

  const char *filepath = ".env";
  if (envFile != NULL && strlen(envFile) > 0) {
    if (!taosCheckExistFile(envFile)) {
      uError("failed to load env file:%s", envFile);
      return -1;
    }
    filepath = envFile;
  } else {
    if (!taosCheckExistFile(filepath)) {
      uInfo("env file:%s not load", filepath);
      return 0;
    }
  }
  int64_t   _bytes;
  TdFilePtr pFile = taosOpenFile(filepath, TD_FILE_READ | TD_FILE_STREAM);
  if (pFile != NULL) {
    while (!taosEOFFile(pFile)) {
      _bytes = taosGetsFile(pFile, sizeof(line) - 1, line);
      if (_bytes <= 0) {
        break;
      }
      if (line[_bytes - 1] == '\n') line[_bytes - 1] = 0;
      if (strncmp(line, "TAOS_APOLLO_URL", 14) == 0) {
        char *p = strchr(line, '=');
        if (p != NULL) {
          p++;
          if (*p == '\'') {
            p++;
            p[strlen(p) - 1] = '\0';
          }
          memcpy(apolloUrl, p, TMIN(strlen(p) + 1, PATH_MAX));
          taosCloseFile(&pFile);
          uInfo("get apollo url from env file success");
          return 0;
        }
      }
    }
    taosCloseFile(&pFile);
  }

  uInfo("fail get apollo url from cmd env file");
  return -1;
}
