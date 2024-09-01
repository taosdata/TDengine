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
#include "tglobal.h"
#include "tgrant.h"
#include "tjson.h"
#include "tlog.h"
#include "tunit.h"
#include "tutil.h"

#define CFG_NAME_PRINT_LEN 24
#define CFG_SRC_PRINT_LEN  12

struct SConfig {
  ECfgSrcType   stype;
  SArray       *array;
  TdThreadMutex lock;
};

int32_t cfgLoadFromCfgFile(SConfig *pConfig, const char *filepath);
int32_t cfgLoadFromEnvFile(SConfig *pConfig, const char *envFile);
int32_t cfgLoadFromEnvVar(SConfig *pConfig);
int32_t cfgLoadFromEnvCmd(SConfig *pConfig, const char **envCmd);
int32_t cfgLoadFromApollUrl(SConfig *pConfig, const char *url);

extern char **environ;

int32_t cfgInit(SConfig **ppCfg) {
  SConfig *pCfg = taosMemoryCalloc(1, sizeof(SConfig));
  if (pCfg == NULL) {
    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
  }

  pCfg->array = taosArrayInit(32, sizeof(SConfigItem));
  if (pCfg->array == NULL) {
    taosMemoryFree(pCfg);
    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
  }

  TAOS_CHECK_RETURN(taosThreadMutexInit(&pCfg->lock, NULL));
  *ppCfg = pCfg;
  TAOS_RETURN(TSDB_CODE_SUCCESS);
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
      return TSDB_CODE_INVALID_PARA;
  }
}

int32_t cfgLoadFromArray(SConfig *pCfg, SArray *pArgs) {
  int32_t size = taosArrayGetSize(pArgs);
  for (int32_t i = 0; i < size; ++i) {
    SConfigPair *pPair = taosArrayGet(pArgs, i);
    if (cfgSetItem(pCfg, pPair->name, pPair->value, CFG_STYPE_ARG_LIST, true) != 0) {
      return TSDB_CODE_INVALID_PARA;
    }
  }

  return TSDB_CODE_SUCCESS;
}

void cfgItemFreeVal(SConfigItem *pItem) {
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
  if (pCfg == NULL) {
    return;
  }

  int32_t size = taosArrayGetSize(pCfg->array);
  for (int32_t i = 0; i < size; ++i) {
    SConfigItem *pItem = taosArrayGet(pCfg->array, i);
    cfgItemFreeVal(pItem);
    taosMemoryFreeClear(pItem->name);
  }

  taosArrayDestroy(pCfg->array);
  (void)taosThreadMutexDestroy(&pCfg->lock);
  taosMemoryFree(pCfg);
}

int32_t cfgGetSize(SConfig *pCfg) { return taosArrayGetSize(pCfg->array); }

static int32_t cfgCheckAndSetConf(SConfigItem *pItem, const char *conf) {
  cfgItemFreeVal(pItem);
  if (!(pItem->str == NULL)) {
    return TSDB_CODE_INVALID_PARA;
  }

  pItem->str = taosStrdup(conf);
  if (pItem->str == NULL) {
    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
  }

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static int32_t cfgCheckAndSetDir(SConfigItem *pItem, const char *inputDir) {
  char fullDir[PATH_MAX] = {0};
  if (taosExpandDir(inputDir, fullDir, PATH_MAX) != 0) {
    int32_t code = TAOS_SYSTEM_ERROR(errno);
    uError("failed to expand dir:%s since %s", inputDir, tstrerror(code));
    TAOS_RETURN(code);
  }

  taosMemoryFreeClear(pItem->str);
  pItem->str = taosStrdup(fullDir);
  if (pItem->str == NULL) {
    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
  }

  TAOS_RETURN(TSDB_CODE_SUCCESS);
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
  int32_t ival;
  TAOS_CHECK_RETURN(taosStrHumanToInt32(value, &ival));
  if (ival < pItem->imin || ival > pItem->imax) {
    uError("cfg:%s, type:%s src:%s value:%d out of range[%" PRId64 ", %" PRId64 "]", pItem->name,
           cfgDtypeStr(pItem->dtype), cfgStypeStr(stype), ival, pItem->imin, pItem->imax);
    TAOS_RETURN(TSDB_CODE_OUT_OF_RANGE);
  }

  pItem->i32 = ival;
  pItem->stype = stype;
  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static int32_t cfgSetInt64(SConfigItem *pItem, const char *value, ECfgSrcType stype) {
  int64_t ival;
  TAOS_CHECK_RETURN(taosStrHumanToInt64(value, &ival));
  if (ival < pItem->imin || ival > pItem->imax) {
    uError("cfg:%s, type:%s src:%s value:%" PRId64 " out of range[%" PRId64 ", %" PRId64 "]", pItem->name,
           cfgDtypeStr(pItem->dtype), cfgStypeStr(stype), ival, pItem->imin, pItem->imax);
    TAOS_RETURN(TSDB_CODE_OUT_OF_RANGE);
  }

  pItem->i64 = ival;
  pItem->stype = stype;
  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static int32_t cfgSetFloat(SConfigItem *pItem, const char *value, ECfgSrcType stype) {
  float dval = 0;
  TAOS_CHECK_RETURN(parseCfgReal(value, &dval));
  if (dval < pItem->fmin || dval > pItem->fmax) {
    uError("cfg:%s, type:%s src:%s value:%g out of range[%g, %g]", pItem->name, cfgDtypeStr(pItem->dtype),
           cfgStypeStr(stype), dval, pItem->fmin, pItem->fmax);
    TAOS_RETURN(TSDB_CODE_OUT_OF_RANGE);
  }

  pItem->fval = dval;
  pItem->stype = stype;
  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static int32_t cfgSetString(SConfigItem *pItem, const char *value, ECfgSrcType stype) {
  char *tmp = taosStrdup(value);
  if (tmp == NULL) {
    uError("cfg:%s, type:%s src:%s value:%s failed to dup since %s", pItem->name, cfgDtypeStr(pItem->dtype),
           cfgStypeStr(stype), value, tstrerror(TSDB_CODE_OUT_OF_MEMORY));
    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
  }

  taosMemoryFreeClear(pItem->str);
  pItem->str = tmp;
  pItem->stype = stype;
  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static int32_t cfgSetDir(SConfigItem *pItem, const char *value, ECfgSrcType stype) {
  int32_t code = cfgCheckAndSetDir(pItem, value);
  if (TSDB_CODE_SUCCESS != code) {
    uError("cfg:%s, type:%s src:%s value:%s failed to dup since %s", pItem->name, cfgDtypeStr(pItem->dtype),
           cfgStypeStr(stype), value, tstrerror(code));
    TAOS_RETURN(code);
  }

  pItem->stype = stype;
  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static int32_t doSetConf(SConfigItem *pItem, const char *value, ECfgSrcType stype) {
  int32_t code = cfgCheckAndSetConf(pItem, value);
  if (TSDB_CODE_SUCCESS != code) {
    uError("cfg:%s, type:%s src:%s value:%s failed to dup since %s", pItem->name, cfgDtypeStr(pItem->dtype),
           cfgStypeStr(stype), value, tstrerror(TSDB_CODE_OUT_OF_MEMORY));
    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
  }

  pItem->stype = stype;
  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static int32_t cfgSetTimezone(SConfigItem *pItem, const char *value, ECfgSrcType stype) {
  TAOS_CHECK_RETURN(doSetConf(pItem, value, stype));
  if (strlen(value) == 0) {
    uError("cfg:%s, type:%s src:%s, value:%s, skip to set timezone", pItem->name, cfgDtypeStr(pItem->dtype),
           cfgStypeStr(stype), value);
    TAOS_RETURN(TSDB_CODE_SUCCESS);
  }

  TAOS_CHECK_RETURN(osSetTimezone(value));
  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static int32_t cfgSetTfsItem(SConfig *pCfg, const char *name, const char *value, const char *level, const char *primary,
                             const char *disable, ECfgSrcType stype) {
  (void)taosThreadMutexLock(&pCfg->lock);

  SConfigItem *pItem = cfgGetItem(pCfg, name);
  if (pItem == NULL) {
    (void)taosThreadMutexUnlock(&pCfg->lock);

    TAOS_RETURN(TSDB_CODE_CFG_NOT_FOUND);
  }

  if (pItem->array == NULL) {
    pItem->array = taosArrayInit(16, sizeof(SDiskCfg));
    if (pItem->array == NULL) {
      (void)taosThreadMutexUnlock(&pCfg->lock);

      TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
    }
  }

  SDiskCfg cfg = {0};
  tstrncpy(cfg.dir, pItem->str, sizeof(cfg.dir));
  cfg.level = level ? atoi(level) : 0;
  cfg.primary = primary ? atoi(primary) : 1;
  cfg.disable = disable ? atoi(disable) : 0;
  void *ret = taosArrayPush(pItem->array, &cfg);
  if (ret == NULL) {
    (void)taosThreadMutexUnlock(&pCfg->lock);

    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
  }

  pItem->stype = stype;
  (void)taosThreadMutexUnlock(&pCfg->lock);

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static int32_t cfgUpdateDebugFlagItem(SConfig *pCfg, const char *name, bool resetArray) {
  SConfigItem *pDebugFlagItem = cfgGetItem(pCfg, "debugFlag");
  if (resetArray) {
    // reset
    if (pDebugFlagItem == NULL) TAOS_RETURN(TSDB_CODE_CFG_NOT_FOUND);

    // logflag names that should 'not' be set by 'debugFlag'
    if (pDebugFlagItem->array == NULL) {
      pDebugFlagItem->array = taosArrayInit(16, sizeof(SLogVar));
      if (pDebugFlagItem->array == NULL) {
        TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
      }
    }
    taosArrayClear(pDebugFlagItem->array);
    TAOS_RETURN(TSDB_CODE_SUCCESS);
  }

  // update
  if (pDebugFlagItem == NULL) return -1;
  if (pDebugFlagItem->array != NULL) {
    SLogVar logVar = {0};
    (void)strncpy(logVar.name, name, TSDB_LOG_VAR_LEN - 1);
    if (NULL == taosArrayPush(pDebugFlagItem->array, &logVar)) {
      TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
    }
  }
  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

int32_t cfgSetItem(SConfig *pCfg, const char *name, const char *value, ECfgSrcType stype, bool lock) {
  // GRANT_CFG_SET;
  int32_t code = TSDB_CODE_SUCCESS;

  if (lock) {
    (void)taosThreadMutexLock(&pCfg->lock);
  }

  SConfigItem *pItem = cfgGetItem(pCfg, name);
  if (pItem == NULL) {
    (void)taosThreadMutexUnlock(&pCfg->lock);
    TAOS_RETURN(TSDB_CODE_CFG_NOT_FOUND);
  }

  switch (pItem->dtype) {
    case CFG_DTYPE_BOOL: {
      code = cfgSetBool(pItem, value, stype);
      break;
    }
    case CFG_DTYPE_INT32: {
      code = cfgSetInt32(pItem, value, stype);
      break;
    }
    case CFG_DTYPE_INT64: {
      code = cfgSetInt64(pItem, value, stype);
      break;
    }
    case CFG_DTYPE_FLOAT:
    case CFG_DTYPE_DOUBLE: {
      code = cfgSetFloat(pItem, value, stype);
      break;
    }
    case CFG_DTYPE_STRING: {
      code = cfgSetString(pItem, value, stype);
      break;
    }
    case CFG_DTYPE_DIR: {
      code = cfgSetDir(pItem, value, stype);
      break;
    }
    case CFG_DTYPE_TIMEZONE: {
      code = cfgSetTimezone(pItem, value, stype);
      break;
    }
    case CFG_DTYPE_CHARSET: {
      code = doSetConf(pItem, value, stype);
      break;
    }
    case CFG_DTYPE_LOCALE: {
      code = doSetConf(pItem, value, stype);
      break;
    }
    case CFG_DTYPE_NONE:
    default:
      code = TSDB_CODE_INVALID_CFG;
      break;
  }

  if (lock) {
    (void)taosThreadMutexUnlock(&pCfg->lock);
  }

  TAOS_RETURN(code);
}

SConfigItem *cfgGetItem(SConfig *pCfg, const char *pName) {
  if (pCfg == NULL) return NULL;
  int32_t size = taosArrayGetSize(pCfg->array);
  for (int32_t i = 0; i < size; ++i) {
    SConfigItem *pItem = taosArrayGet(pCfg->array, i);
    if (strcasecmp(pItem->name, pName) == 0) {
      return pItem;
    }
  }

  return NULL;
}

void cfgLock(SConfig *pCfg) {
  if (pCfg == NULL) {
    return;
  }

  (void)taosThreadMutexLock(&pCfg->lock);
}

void cfgUnLock(SConfig *pCfg) { (void)taosThreadMutexUnlock(&pCfg->lock); }

int32_t cfgCheckRangeForDynUpdate(SConfig *pCfg, const char *name, const char *pVal, bool isServer) {
  ECfgDynType dynType = isServer ? CFG_DYN_SERVER : CFG_DYN_CLIENT;

  cfgLock(pCfg);

  SConfigItem *pItem = cfgGetItem(pCfg, name);
  if (!pItem || (pItem->dynScope & dynType) == 0) {
    uError("failed to config:%s, not support update this config", name);
    cfgUnLock(pCfg);
    TAOS_RETURN(TSDB_CODE_INVALID_CFG);
  }

  switch (pItem->dtype) {
    case CFG_DTYPE_STRING: {
      if (strcasecmp(name, "slowLogScope") == 0) {
        char   *tmp = taosStrdup(pVal);
        int32_t scope = 0;
        int32_t code = taosSetSlowLogScope(tmp, &scope);
        if (TSDB_CODE_SUCCESS != code) {
          cfgUnLock(pCfg);
          taosMemoryFree(tmp);
          TAOS_RETURN(code);
        }
        taosMemoryFree(tmp);
      }
    } break;
    case CFG_DTYPE_BOOL: {
      int32_t ival = (int32_t)atoi(pVal);
      if (ival != 0 && ival != 1) {
        uError("cfg:%s, type:%s value:%d out of range[0, 1]", pItem->name, cfgDtypeStr(pItem->dtype), ival);
        cfgUnLock(pCfg);
        TAOS_RETURN(TSDB_CODE_OUT_OF_RANGE);
      }
    } break;
    case CFG_DTYPE_INT32: {
      int32_t ival;
      int32_t code = (int32_t)taosStrHumanToInt32(pVal, &ival);
      if (code != TSDB_CODE_SUCCESS) {
        cfgUnLock(pCfg);
        return code;
      }
      if (ival < pItem->imin || ival > pItem->imax) {
        uError("cfg:%s, type:%s value:%d out of range[%" PRId64 ", %" PRId64 "]", pItem->name,
               cfgDtypeStr(pItem->dtype), ival, pItem->imin, pItem->imax);
        cfgUnLock(pCfg);
        TAOS_RETURN(TSDB_CODE_OUT_OF_RANGE);
      }
    } break;
    case CFG_DTYPE_INT64: {
      int64_t ival;
      int32_t code = taosStrHumanToInt64(pVal, &ival);
      if (code != TSDB_CODE_SUCCESS) {
        cfgUnLock(pCfg);
        TAOS_RETURN(code);
      }
      if (ival < pItem->imin || ival > pItem->imax) {
        uError("cfg:%s, type:%s value:%" PRId64 " out of range[%" PRId64 ", %" PRId64 "]", pItem->name,
               cfgDtypeStr(pItem->dtype), ival, pItem->imin, pItem->imax);
        cfgUnLock(pCfg);
        TAOS_RETURN(TSDB_CODE_OUT_OF_RANGE);
      }
    } break;
    case CFG_DTYPE_FLOAT:
    case CFG_DTYPE_DOUBLE: {
      float  dval = 0;
      int32_t code = parseCfgReal(pVal, &dval);
      if (code != TSDB_CODE_SUCCESS) {
        cfgUnLock(pCfg);
        TAOS_RETURN(code);
      }
      if (dval < pItem->fmin || dval > pItem->fmax) {
        uError("cfg:%s, type:%s value:%g out of range[%g, %g]", pItem->name, cfgDtypeStr(pItem->dtype), dval,
               pItem->fmin, pItem->fmax);
        cfgUnLock(pCfg);
        TAOS_RETURN(TSDB_CODE_OUT_OF_RANGE);
      }
    } break;
    default:
      break;
  }

  cfgUnLock(pCfg);
  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static int32_t cfgAddItem(SConfig *pCfg, SConfigItem *pItem, const char *name) {
  pItem->stype = CFG_STYPE_DEFAULT;
  pItem->name = taosStrdup(name);
  if (pItem->name == NULL) {
    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
  }

  int32_t size = taosArrayGetSize(pCfg->array);
  for (int32_t i = 0; i < size; ++i) {
    SConfigItem *existItem = taosArrayGet(pCfg->array, i);
    if (existItem != NULL && strcmp(existItem->name, pItem->name) == 0) {
      taosMemoryFree(pItem->name);
      TAOS_RETURN(TSDB_CODE_INVALID_CFG);
    }
  }

  int32_t len = strlen(name);
  char    lowcaseName[CFG_NAME_MAX_LEN + 1] = {0};
  (void)strntolower(lowcaseName, name, TMIN(CFG_NAME_MAX_LEN, len));

  if (taosArrayPush(pCfg->array, pItem) == NULL) {
    if (pItem->dtype == CFG_DTYPE_STRING) {
      taosMemoryFree(pItem->str);
    }

    taosMemoryFree(pItem->name);
    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
  }

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

int32_t cfgAddBool(SConfig *pCfg, const char *name, bool defaultVal, int8_t scope, int8_t dynScope) {
  SConfigItem item = {.dtype = CFG_DTYPE_BOOL, .bval = defaultVal, .scope = scope, .dynScope = dynScope};
  return cfgAddItem(pCfg, &item, name);
}

int32_t cfgAddInt32(SConfig *pCfg, const char *name, int32_t defaultVal, int64_t minval, int64_t maxval, int8_t scope,
                    int8_t dynScope) {
  if (defaultVal < minval || defaultVal > maxval) {
    TAOS_RETURN(TSDB_CODE_OUT_OF_RANGE);
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
    TAOS_RETURN(TSDB_CODE_OUT_OF_RANGE);
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
    TAOS_RETURN(TSDB_CODE_OUT_OF_RANGE);
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
    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
  }
  return cfgAddItem(pCfg, &item, name);
}

int32_t cfgAddDir(SConfig *pCfg, const char *name, const char *defaultVal, int8_t scope, int8_t dynScope) {
  SConfigItem item = {.dtype = CFG_DTYPE_DIR, .scope = scope, .dynScope = dynScope};
  TAOS_CHECK_RETURN(cfgCheckAndSetDir(&item, defaultVal));
  return cfgAddItem(pCfg, &item, name);
}

int32_t cfgAddLocale(SConfig *pCfg, const char *name, const char *defaultVal, int8_t scope, int8_t dynScope) {
  SConfigItem item = {.dtype = CFG_DTYPE_LOCALE, .scope = scope, .dynScope = dynScope};
  TAOS_CHECK_RETURN(cfgCheckAndSetConf(&item, defaultVal));
  return cfgAddItem(pCfg, &item, name);
}

int32_t cfgAddCharset(SConfig *pCfg, const char *name, const char *defaultVal, int8_t scope, int8_t dynScope) {
  SConfigItem item = {.dtype = CFG_DTYPE_CHARSET, .scope = scope, .dynScope = dynScope};
  TAOS_CHECK_RETURN(cfgCheckAndSetConf(&item, defaultVal));
  return cfgAddItem(pCfg, &item, name);
}

int32_t cfgAddTimezone(SConfig *pCfg, const char *name, const char *defaultVal, int8_t scope, int8_t dynScope) {
  SConfigItem item = {.dtype = CFG_DTYPE_TIMEZONE, .scope = scope, .dynScope = dynScope};
  TAOS_CHECK_RETURN(cfgCheckAndSetConf(&item, defaultVal));
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

int32_t cfgDumpItemValue(SConfigItem *pItem, char *buf, int32_t bufSize, int32_t *pLen) {
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

  if (len < 0) {
    TAOS_RETURN(TAOS_SYSTEM_ERROR(errno));
  }

  if (len > bufSize) {
    len = bufSize;
  }

  *pLen = len;
  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

int32_t cfgDumpItemScope(SConfigItem *pItem, char *buf, int32_t bufSize, int32_t *pLen) {
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

  if (len < 0) {
    TAOS_RETURN(TAOS_SYSTEM_ERROR(errno));
  }

  if (len > bufSize) {
    len = bufSize;
  }

  *pLen = len;
  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

void cfgDumpCfgS3(SConfig *pCfg, bool tsc, bool dump) {
  if (dump) {
    (void)printf("                     s3 config");
    (void)printf("\n");
    (void)printf("=================================================================");
    (void)printf("\n");
  } else {
    uInfo("                     s3 config");
    uInfo("=================================================================");
  }

  char src[CFG_SRC_PRINT_LEN + 1] = {0};
  char name[CFG_NAME_PRINT_LEN + 1] = {0};

  int32_t size = taosArrayGetSize(pCfg->array);
  for (int32_t i = 0; i < size; ++i) {
    SConfigItem *pItem = taosArrayGet(pCfg->array, i);
    if (tsc && pItem->scope == CFG_SCOPE_SERVER) continue;
    if (dump && strcmp(pItem->name, "scriptDir") == 0) continue;
    if (dump && strncmp(pItem->name, "s3", 2) != 0) continue;
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
          (void)printf("%s %s %u\n", src, name, pItem->bval);
        } else {
          uInfo("%s %s %u", src, name, pItem->bval);
        }

        break;
      case CFG_DTYPE_INT32:
        if (dump) {
          (void)printf("%s %s %d\n", src, name, pItem->i32);
        } else {
          uInfo("%s %s %d", src, name, pItem->i32);
        }
        break;
      case CFG_DTYPE_INT64:
        if (dump) {
          (void)printf("%s %s %" PRId64 "\n", src, name, pItem->i64);
        } else {
          uInfo("%s %s %" PRId64, src, name, pItem->i64);
        }
        break;
      case CFG_DTYPE_DOUBLE:
      case CFG_DTYPE_FLOAT:
        if (dump) {
          (void)printf("%s %s %.2f\n", src, name, pItem->fval);
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
          (void)printf("%s %s %s\n", src, name, pItem->str);
        } else {
          uInfo("%s %s %s", src, name, pItem->str);
        }
        break;
    }
  }

  if (dump) {
    (void)printf("=================================================================\n");
  } else {
    uInfo("=================================================================");
  }
}

void cfgDumpCfg(SConfig *pCfg, bool tsc, bool dump) {
  if (dump) {
    (void)printf("                     global config");
    (void)printf("\n");
    (void)printf("=================================================================");
    (void)printf("\n");
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
          (void)printf("%s %s %u\n", src, name, pItem->bval);
        } else {
          uInfo("%s %s %u", src, name, pItem->bval);
        }

        break;
      case CFG_DTYPE_INT32:
        if (dump) {
          (void)printf("%s %s %d\n", src, name, pItem->i32);
        } else {
          uInfo("%s %s %d", src, name, pItem->i32);
        }
        break;
      case CFG_DTYPE_INT64:
        if (dump) {
          (void)printf("%s %s %" PRId64 "\n", src, name, pItem->i64);
        } else {
          uInfo("%s %s %" PRId64, src, name, pItem->i64);
        }
        break;
      case CFG_DTYPE_DOUBLE:
      case CFG_DTYPE_FLOAT:
        if (dump) {
          (void)printf("%s %s %.2f\n", src, name, pItem->fval);
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
          (void)printf("%s %s %s\n", src, name, pItem->str);
        } else {
          uInfo("%s %s %s", src, name, pItem->str);
        }
        break;
    }
  }

  if (dump) {
    (void)printf("=================================================================\n");
  } else {
    uInfo("=================================================================");
  }
}

int32_t cfgLoadFromEnvVar(SConfig *pConfig) {
  char    line[1024], *name, *value, *value2, *value3, *value4;
  int32_t olen, vlen, vlen2, vlen3, vlen4;
  int32_t code = 0;
  char  **pEnv = environ;
  line[1023] = 0;

  if (pEnv == NULL) TAOS_RETURN(TSDB_CODE_SUCCESS);
  while (*pEnv != NULL) {
    name = value = value2 = value3 = value4 = NULL;
    olen = vlen = vlen2 = vlen3 = vlen4 = 0;

    strncpy(line, *pEnv, sizeof(line) - 1);
    pEnv++;
    (void)taosEnvToCfg(line, line);

    (void)paGetToken(line, &name, &olen);
    if (olen == 0) continue;
    name[olen] = 0;

    (void)paGetToken(name + olen + 1, &value, &vlen);
    if (vlen == 0) continue;
    value[vlen] = 0;

    (void)paGetToken(value + vlen + 1, &value2, &vlen2);
    if (vlen2 != 0) {
      value2[vlen2] = 0;
      (void)paGetToken(value2 + vlen2 + 1, &value3, &vlen3);
      if (vlen3 != 0) {
        value3[vlen3] = 0;
        (void)paGetToken(value3 + vlen3 + 1, &value4, &vlen4);
        if (vlen4 != 0) value4[vlen4] = 0;
      }
    }

    code = cfgSetItem(pConfig, name, value, CFG_STYPE_ENV_VAR, true);
    if (TSDB_CODE_SUCCESS != code && TSDB_CODE_CFG_NOT_FOUND != code) break;

    if (strcasecmp(name, "dataDir") == 0) {
      code = cfgSetTfsItem(pConfig, name, value, value2, value3, value4, CFG_STYPE_ENV_VAR);
      if (TSDB_CODE_SUCCESS != code && TSDB_CODE_CFG_NOT_FOUND != code) break;
    }
  }

  uInfo("load from env variables cfg success");
  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

int32_t cfgLoadFromEnvCmd(SConfig *pConfig, const char **envCmd) {
  char    buf[1024], *name, *value, *value2, *value3, *value4;
  int32_t olen, vlen, vlen2, vlen3, vlen4;
  int32_t code = 0;
  int32_t index = 0;
  if (envCmd == NULL) TAOS_RETURN(TSDB_CODE_SUCCESS);
  while (envCmd[index] != NULL) {
    strncpy(buf, envCmd[index], sizeof(buf) - 1);
    buf[sizeof(buf) - 1] = 0;
    (void)taosEnvToCfg(buf, buf);
    index++;

    name = value = value2 = value3 = value4 = NULL;
    olen = vlen = vlen2 = vlen3 = vlen4 = 0;

    (void)paGetToken(buf, &name, &olen);
    if (olen == 0) continue;
    name[olen] = 0;

    (void)paGetToken(name + olen + 1, &value, &vlen);
    if (vlen == 0) continue;
    value[vlen] = 0;

    (void)paGetToken(value + vlen + 1, &value2, &vlen2);
    if (vlen2 != 0) {
      value2[vlen2] = 0;
      (void)paGetToken(value2 + vlen2 + 1, &value3, &vlen3);
      if (vlen3 != 0) {
        value3[vlen3] = 0;
        (void)paGetToken(value3 + vlen3 + 1, &value4, &vlen4);
        if (vlen4 != 0) value4[vlen4] = 0;
      }
    }

    code = cfgSetItem(pConfig, name, value, CFG_STYPE_ENV_CMD, true);
    if (TSDB_CODE_SUCCESS != code && TSDB_CODE_CFG_NOT_FOUND != code) break;

    if (strcasecmp(name, "dataDir") == 0) {
      code = cfgSetTfsItem(pConfig, name, value, value2, value3, value4, CFG_STYPE_ENV_CMD);
      if (TSDB_CODE_SUCCESS != code && TSDB_CODE_CFG_NOT_FOUND != code) break;
    }
  }

  uInfo("load from env cmd cfg success");
  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

int32_t cfgLoadFromEnvFile(SConfig *pConfig, const char *envFile) {
  char    line[1024], *name, *value, *value2, *value3, *value4;
  int32_t olen, vlen, vlen2, vlen3, vlen4;
  int32_t code = 0;
  ssize_t _bytes = 0;

  const char *filepath = ".env";
  if (envFile != NULL && strlen(envFile) > 0) {
    if (!taosCheckExistFile(envFile)) {
      (void)printf("failed to load env file:%s\n", envFile);
      TAOS_RETURN(TSDB_CODE_NOT_FOUND);
    }
    filepath = envFile;
  } else {
    if (!taosCheckExistFile(filepath)) {
      uInfo("env file:%s not load", filepath);
      TAOS_RETURN(TSDB_CODE_SUCCESS);
    }
  }

  TdFilePtr pFile = taosOpenFile(filepath, TD_FILE_READ | TD_FILE_STREAM);
  if (pFile == NULL) {
    TAOS_RETURN(TAOS_SYSTEM_ERROR(errno));
  }

  while (!taosEOFFile(pFile)) {
    name = value = value2 = value3 = value4 = NULL;
    olen = vlen = vlen2 = vlen3 = vlen4 = 0;

    _bytes = taosGetsFile(pFile, sizeof(line), line);
    if (_bytes <= 0) {
      break;
    }
    if (line[_bytes - 1] == '\n') line[_bytes - 1] = 0;
    (void)taosEnvToCfg(line, line);

    (void)paGetToken(line, &name, &olen);
    if (olen == 0) continue;
    name[olen] = 0;

    (void)paGetToken(name + olen + 1, &value, &vlen);
    if (vlen == 0) continue;
    value[vlen] = 0;

    (void)paGetToken(value + vlen + 1, &value2, &vlen2);
    if (vlen2 != 0) {
      value2[vlen2] = 0;
      (void)paGetToken(value2 + vlen2 + 1, &value3, &vlen3);
      if (vlen3 != 0) {
        value3[vlen3] = 0;
        (void)paGetToken(value3 + vlen3 + 1, &value4, &vlen4);
        if (vlen4 != 0) value4[vlen4] = 0;
      }
    }

    code = cfgSetItem(pConfig, name, value, CFG_STYPE_ENV_FILE, true);
    if (TSDB_CODE_SUCCESS != code && TSDB_CODE_CFG_NOT_FOUND != code) break;

    if (strcasecmp(name, "dataDir") == 0) {
      code = cfgSetTfsItem(pConfig, name, value, value2, value3, value4, CFG_STYPE_ENV_FILE);
      if (TSDB_CODE_SUCCESS != code && TSDB_CODE_CFG_NOT_FOUND != code) break;
    }
  }

  (void)taosCloseFile(&pFile);

  uInfo("load from env cfg file %s success", filepath);
  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

int32_t cfgLoadFromCfgFile(SConfig *pConfig, const char *filepath) {
  char    line[1024], *name, *value, *value2, *value3, *value4;
  int32_t olen, vlen, vlen2, vlen3, vlen4;
  ssize_t _bytes = 0;
  int32_t code = 0;

  TdFilePtr pFile = taosOpenFile(filepath, TD_FILE_READ | TD_FILE_STREAM);
  if (pFile == NULL) {
    // success when the file does not exist
    code = TAOS_SYSTEM_ERROR(errno);
    if (errno == ENOENT) {
      uInfo("failed to load from cfg file %s since %s, use default parameters", filepath, tstrerror(code));
      TAOS_RETURN(TSDB_CODE_SUCCESS);
    } else {
      (void)printf("failed to load from cfg file %s since %s\n", filepath, tstrerror(code));
      TAOS_RETURN(code);
    }
  }

  while (!taosEOFFile(pFile)) {
    name = value = value2 = value3 = value4 = NULL;
    olen = vlen = vlen2 = vlen3 = vlen4 = 0;

    _bytes = taosGetsFile(pFile, sizeof(line), line);
    if (_bytes <= 0) {
      break;
    }

    if (line[_bytes - 1] == '\n') line[_bytes - 1] = 0;

    (void)paGetToken(line, &name, &olen);
    if (olen == 0) continue;
    name[olen] = 0;

    (void)paGetToken(name + olen + 1, &value, &vlen);
    if (vlen == 0) continue;
    value[vlen] = 0;

    if (strcasecmp(name, "encryptScope") == 0) {
      char   *tmp = NULL;
      int32_t len = 0;
      char    newValue[1024] = {0};

      strcpy(newValue, value);

      int32_t count = 1;
      while (vlen < 1024) {
        (void)paGetToken(value + vlen + 1 * count, &tmp, &len);
        if (len == 0) break;
        tmp[len] = 0;
        strcpy(newValue + vlen, tmp);
        vlen += len;
        count++;
      }

      code = cfgSetItem(pConfig, name, newValue, CFG_STYPE_CFG_FILE, true);
      if (TSDB_CODE_SUCCESS != code && TSDB_CODE_CFG_NOT_FOUND != code) {
        (void)printf("cfg:%s, value:%s failed since %s\n", name,newValue, tstrerror(code));
        break;
      }
    } else {
      (void)paGetToken(value + vlen + 1, &value2, &vlen2);
      if (vlen2 != 0) {
        value2[vlen2] = 0;
        (void)paGetToken(value2 + vlen2 + 1, &value3, &vlen3);
        if (vlen3 != 0) {
          value3[vlen3] = 0;
          (void)paGetToken(value3 + vlen3 + 1, &value4, &vlen4);
          if (vlen4 != 0) value4[vlen4] = 0;
        }
      }

      code = cfgSetItem(pConfig, name, value, CFG_STYPE_CFG_FILE, true);
      if (TSDB_CODE_SUCCESS != code && TSDB_CODE_CFG_NOT_FOUND != code) {
        (void)printf("cfg:%s, value:%s failed since %s\n", name, value, tstrerror(code));
        break;
      }
    }

    if (strcasecmp(name, "dataDir") == 0) {
      code = cfgSetTfsItem(pConfig, name, value, value2, value3, value4, CFG_STYPE_CFG_FILE);
      if (TSDB_CODE_SUCCESS != code && TSDB_CODE_CFG_NOT_FOUND != code) break;
    }

    size_t       len = strlen(name);
    const char  *debugFlagStr = "debugFlag";
    const size_t debugFlagLen = strlen(debugFlagStr);
    if (len >= debugFlagLen && strcasecmp(name + len - debugFlagLen, debugFlagStr) == 0) {
      code = cfgUpdateDebugFlagItem(pConfig, name, len == debugFlagLen);
      if (TSDB_CODE_SUCCESS != code && TSDB_CODE_CFG_NOT_FOUND != code) break;
    }
  }

  (void)taosCloseFile(&pFile);

  if (TSDB_CODE_SUCCESS == code || TSDB_CODE_CFG_NOT_FOUND == code) {
    uInfo("load from cfg file %s success", filepath);
    TAOS_RETURN(TSDB_CODE_SUCCESS);
  } else {
    (void)printf("failed to load from cfg file %s since %s\n", filepath, tstrerror(code));
    TAOS_RETURN(code);
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

//     (void)paGetToken(line, &name, &olen);
//     if (olen == 0) continue;
//     name[olen] = 0;

//     (void)paGetToken(name + olen + 1, &value, &vlen);
//     if (vlen == 0) continue;
//     value[vlen] = 0;

//     (void)paGetToken(value + vlen + 1, &value2, &vlen2);
//     if (vlen2 != 0) {
//       value2[vlen2] = 0;
//       (void)paGetToken(value2 + vlen2 + 1, &value3, &vlen3);
//       if (vlen3 != 0) value3[vlen3] = 0;
//     }

//     code = cfgSetItem(pConfig, name, value, CFG_STYPE_CFG_FILE);
//     if (code != 0 && terrno != TSDB_CODE_CFG_NOT_FOUND) break;
//     if (strcasecmp(name, "dataDir") == 0) {
//       code = cfgSetTfsItem(pConfig, name, value, value2, value3, CFG_STYPE_CFG_FILE);
//       if (code != 0 && terrno != TSDB_CODE_CFG_NOT_FOUND) break;
//     }
//   }

//   (void)taosCloseFile(&pFile);
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
  char   *cfgLineBuf = NULL, *name, *value, *value2, *value3, *value4;
  SJson  *pJson = NULL;
  int32_t olen, vlen, vlen2, vlen3, vlen4;
  int32_t code = 0, lino = 0;
  if (url == NULL || strlen(url) == 0) {
    uInfo("apoll url not load");
    TAOS_RETURN(TSDB_CODE_SUCCESS);
  }

  char *p = strchr(url, ':');
  if (p == NULL) {
    (void)printf("fail to load apoll url: %s, unknown format\n", url);
    TAOS_RETURN(TSDB_CODE_INVALID_PARA);
  }
  p++;

  if (strncmp(url, "jsonFile", 8) == 0) {
    char *filepath = p;
    if (!taosCheckExistFile(filepath)) {
      (void)printf("failed to load json file:%s\n", filepath);
      TAOS_RETURN(TSDB_CODE_NOT_FOUND);
    }

    TdFilePtr pFile = taosOpenFile(filepath, TD_FILE_READ);
    if (pFile == NULL) {
      TAOS_CHECK_EXIT(TAOS_SYSTEM_ERROR(errno));
    }
    size_t fileSize = taosLSeekFile(pFile, 0, SEEK_END);
    char  *buf = taosMemoryMalloc(fileSize + 1);
    if (!buf) {
      (void)taosCloseFile(&pFile);
      (void)printf("load json file error: %s, failed to alloc memory\n", filepath);
      TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
    }

    buf[fileSize] = 0;
    (void)taosLSeekFile(pFile, 0, SEEK_SET);
    if (taosReadFile(pFile, buf, fileSize) <= 0) {
      (void)taosCloseFile(&pFile);
      (void)printf("load json file error: %s\n", filepath);
      taosMemoryFreeClear(buf);
      TAOS_RETURN(TSDB_CODE_INVALID_DATA_FMT);
    }
    (void)taosCloseFile(&pFile);
    pJson = tjsonParse(buf);
    if (NULL == pJson) {
      const char *jsonParseError = tjsonGetError();
      if (jsonParseError != NULL) {
        (void)printf("load json file parse error: %s\n", jsonParseError);
      }
      taosMemoryFreeClear(buf);
      TAOS_CHECK_EXIT(TSDB_CODE_INVALID_DATA_FMT);
    }
    taosMemoryFreeClear(buf);

    int32_t jsonArraySize = tjsonGetArraySize(pJson);
    for (int32_t i = 0; i < jsonArraySize; i++) {
      cJSON *item = tjsonGetArrayItem(pJson, i);
      if (item == NULL) break;
      char *itemName = NULL, *itemValueString = NULL;
      if (tjsonGetObjectName(item, &itemName) != 0) {
        TAOS_CHECK_EXIT(TSDB_CODE_INVALID_DATA_FMT);
      }
      if (tjsonGetObjectValueString(item, &itemValueString) != 0) {
        TAOS_CHECK_EXIT(TSDB_CODE_INVALID_DATA_FMT);
      }

      if (itemValueString != NULL && itemName != NULL) {
        size_t itemNameLen = strlen(itemName);
        size_t itemValueStringLen = strlen(itemValueString);
        void  *px = taosMemoryRealloc(cfgLineBuf, itemNameLen + itemValueStringLen + 3);
        if (NULL == px) {
          TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
        }

        cfgLineBuf = px;
        (void)memset(cfgLineBuf, 0, itemNameLen + itemValueStringLen + 3);

        (void)memcpy(cfgLineBuf, itemName, itemNameLen);
        cfgLineBuf[itemNameLen] = ' ';
        (void)memcpy(&cfgLineBuf[itemNameLen + 1], itemValueString, itemValueStringLen);

        (void)paGetToken(cfgLineBuf, &name, &olen);
        if (olen == 0) continue;
        name[olen] = 0;

        (void)paGetToken(name + olen + 1, &value, &vlen);
        if (vlen == 0) continue;
        value[vlen] = 0;

        (void)paGetToken(value + vlen + 1, &value2, &vlen2);
        if (vlen2 != 0) {
          value2[vlen2] = 0;
          (void)paGetToken(value2 + vlen2 + 1, &value3, &vlen3);
          if (vlen3 != 0) {
            value3[vlen3] = 0;
            (void)paGetToken(value3 + vlen3 + 1, &value4, &vlen4);
            if (vlen4 != 0) value4[vlen4] = 0;
          }
        }

        code = cfgSetItem(pConfig, name, value, CFG_STYPE_APOLLO_URL, true);
        if (TSDB_CODE_SUCCESS != code && TSDB_CODE_CFG_NOT_FOUND != code) break;

        if (strcasecmp(name, "dataDir") == 0) {
          code = cfgSetTfsItem(pConfig, name, value, value2, value3, value4, CFG_STYPE_APOLLO_URL);
          if (TSDB_CODE_SUCCESS != code && TSDB_CODE_CFG_NOT_FOUND != code) break;
        }
      }
    }
    tjsonDelete(pJson);
    pJson = NULL;

    // } else if (strncmp(url, "jsonUrl", 7) == 0) {
    // } else if (strncmp(url, "etcdUrl", 7) == 0) {
  } else {
    (void)printf("Unsupported url: %s\n", url);
    TAOS_RETURN(TSDB_CODE_INVALID_PARA);
  }

  taosMemoryFree(cfgLineBuf);
  uInfo("load from apoll url not implemented yet");
  TAOS_RETURN(TSDB_CODE_SUCCESS);

_exit:
  taosMemoryFree(cfgLineBuf);
  tjsonDelete(pJson);
  if (code != 0) {
    (void)printf("failed to load from apollo url:%s at line %d since %s\n", url, lino, tstrerror(code));
  }
  TAOS_RETURN(code);
}

int32_t cfgGetApollUrl(const char **envCmd, const char *envFile, char *apolloUrl) {
  int32_t index = 0;
  if (envCmd == NULL) TAOS_RETURN(TSDB_CODE_SUCCESS);
  while (envCmd[index] != NULL) {
    if (strncmp(envCmd[index], "TAOS_APOLLO_URL", 14) == 0) {
      char *p = strchr(envCmd[index], '=');
      if (p != NULL) {
        p++;
        if (*p == '\'') {
          p++;
          p[strlen(p) - 1] = '\0';
        }
        (void)memcpy(apolloUrl, p, TMIN(strlen(p) + 1, PATH_MAX));
        uInfo("get apollo url from env cmd success");
        TAOS_RETURN(TSDB_CODE_SUCCESS);
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
        (void)memcpy(apolloUrl, p, TMIN(strlen(p) + 1, PATH_MAX));
        uInfo("get apollo url from env variables success, apolloUrl=%s", apolloUrl);
        TAOS_RETURN(TSDB_CODE_SUCCESS);
      }
    }
  }

  const char *filepath = ".env";
  if (envFile != NULL && strlen(envFile) > 0) {
    if (!taosCheckExistFile(envFile)) {
      uError("failed to load env file:%s", envFile);
      TAOS_RETURN(TSDB_CODE_NOT_FOUND);
    }
    filepath = envFile;
  } else {
    if (!taosCheckExistFile(filepath)) {
      uInfo("env file:%s not load", filepath);
      TAOS_RETURN(TSDB_CODE_SUCCESS);
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
          (void)memcpy(apolloUrl, p, TMIN(strlen(p) + 1, PATH_MAX));
          (void)taosCloseFile(&pFile);
          uInfo("get apollo url from env file success");
          TAOS_RETURN(TSDB_CODE_SUCCESS);
        }
      }
    }
    (void)taosCloseFile(&pFile);
  }

  uInfo("fail get apollo url from cmd env file");
  TAOS_RETURN(TSDB_CODE_NOT_FOUND);
}

struct SConfigIter {
  int32_t  index;
  SConfig *pConf;
};

int32_t cfgCreateIter(SConfig *pConf, SConfigIter **ppIter) {
  SConfigIter *pIter = taosMemoryCalloc(1, sizeof(SConfigIter));
  if (pIter == NULL) {
    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
  }

  pIter->pConf = pConf;

  *ppIter = pIter;
  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

SConfigItem *cfgNextIter(SConfigIter *pIter) {
  if (pIter->index < cfgGetSize(pIter->pConf)) {
    return taosArrayGet(pIter->pConf->array, pIter->index++);
  }

  return NULL;
}

void cfgDestroyIter(SConfigIter *pIter) {
  if (pIter == NULL) {
    return;
  }

  taosMemoryFree(pIter);
}
