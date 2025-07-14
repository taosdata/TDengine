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
#include "cJSON.h"
#include "taoserror.h"
#include "tconfig.h"
#include "tconv.h"
#include "tenv.h"
#include "tglobal.h"
#include "tgrant.h"
#include "tjson.h"
#include "tlog.h"
#include "tunit.h"
#include "tutil.h"

#define CFG_NAME_PRINT_LEN 32
#define CFG_SRC_PRINT_LEN  12

struct SConfig {
  ECfgSrcType   stype;
  SArray       *localArray;
  SArray       *globalArray;
  TdThreadMutex lock;
};

int32_t cfgLoadFromCfgFile(SConfig *pConfig, const char *filepath);
int32_t cfgLoadFromEnvFile(SConfig *pConfig, const char *envFile);
int32_t cfgLoadFromEnvVar(SConfig *pConfig);
int32_t cfgLoadFromEnvCmd(SConfig *pConfig, const char **envCmd);
int32_t cfgLoadFromApollUrl(SConfig *pConfig, const char *url);
int32_t cfgSetItemVal(SConfigItem *pItem, const char *name, const char *value, ECfgSrcType stype);

extern char **environ;

int32_t cfgInit(SConfig **ppCfg) {
  int32_t  code = 0;
  int32_t  lino = 0;
  SConfig *pCfg = NULL;

  pCfg = taosMemoryCalloc(1, sizeof(SConfig));
  if (pCfg == NULL) return terrno;

  pCfg->localArray = NULL, pCfg->globalArray = NULL;
  pCfg->localArray = taosArrayInit(64, sizeof(SConfigItem));
  TSDB_CHECK_NULL(pCfg->localArray, code, lino, _exit, terrno);

  pCfg->globalArray = taosArrayInit(64, sizeof(SConfigItem));
  TSDB_CHECK_NULL(pCfg->globalArray, code, lino, _exit, terrno);

  TAOS_CHECK_RETURN(taosThreadMutexInit(&pCfg->lock, NULL));
  *ppCfg = pCfg;

_exit:
  if (code != 0) {
    uError("failed to init config, since %s ,at line %d", tstrerror(code), lino);
    cfgCleanup(pCfg);
  }

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

int32_t cfgUpdateFromArray(SConfig *pCfg, SArray *pArgs) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t size = taosArrayGetSize(pArgs);
  for (int32_t i = 0; i < size; ++i) {
    SConfigItem *pItemNew = taosArrayGet(pArgs, i);

    (void)taosThreadMutexLock(&pCfg->lock);

    SConfigItem *pItemOld = cfgGetItem(pCfg, pItemNew->name);
    if (pItemOld == NULL) {
      uInfo("cfg:%s, type:%s src:%s, not found, skip to update", pItemNew->name, cfgDtypeStr(pItemNew->dtype),
            cfgStypeStr(pItemNew->stype));
      (void)taosThreadMutexUnlock(&pCfg->lock);
      continue;
    }
    switch (pItemNew->dtype) {
      case CFG_DTYPE_BOOL:
        pItemOld->bval = pItemNew->bval;
        break;
      case CFG_DTYPE_INT32:
        pItemOld->i32 = pItemNew->i32;
        break;
      case CFG_DTYPE_INT64:
        pItemOld->i64 = pItemNew->i64;
        break;
      case CFG_DTYPE_FLOAT:
      case CFG_DTYPE_DOUBLE:
        pItemOld->fval = pItemNew->fval;
        break;
      case CFG_DTYPE_STRING:
      case CFG_DTYPE_DIR:
        taosMemoryFree(pItemOld->str);
        pItemOld->str = taosStrdup(pItemNew->str);
        if (pItemOld->str == NULL) {
          (void)taosThreadMutexUnlock(&pCfg->lock);
          TAOS_RETURN(terrno);
        }
        break;
      case CFG_DTYPE_LOCALE:
      case CFG_DTYPE_CHARSET:
        code = cfgSetItemVal(pItemOld, pItemNew->name, pItemNew->str, pItemNew->stype);
        if (code != TSDB_CODE_SUCCESS) {
          (void)taosThreadMutexUnlock(&pCfg->lock);
          TAOS_RETURN(code);
        }
        break;
      case CFG_DTYPE_TIMEZONE:
        truncateTimezoneString(pItemNew->str);
        code = cfgSetItemVal(pItemOld, pItemNew->name, pItemNew->str, pItemNew->stype);
        if (code != TSDB_CODE_SUCCESS) {
          (void)taosThreadMutexUnlock(&pCfg->lock);
          TAOS_RETURN(code);
        }
        break;
      default:
        break;
    }

    (void)taosThreadMutexUnlock(&pCfg->lock);
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

  int32_t size = taosArrayGetSize(pCfg->localArray);
  for (int32_t i = 0; i < size; ++i) {
    SConfigItem *pItem = taosArrayGet(pCfg->localArray, i);
    cfgItemFreeVal(pItem);
    taosMemoryFreeClear(pItem->name);
  }

  size = taosArrayGetSize(pCfg->globalArray);
  for (int32_t i = 0; i < size; ++i) {
    SConfigItem *pItem = taosArrayGet(pCfg->globalArray, i);
    cfgItemFreeVal(pItem);
    taosMemoryFreeClear(pItem->name);
  }

  taosArrayDestroy(pCfg->localArray);
  taosArrayDestroy(pCfg->globalArray);
  (void)taosThreadMutexDestroy(&pCfg->lock);
  taosMemoryFree(pCfg);
}

int32_t cfgGetSize(SConfig *pCfg) { return taosArrayGetSize(pCfg->localArray) + taosArrayGetSize(pCfg->globalArray); }
int32_t cfgGetLocalSize(SConfig *pCfg) { return taosArrayGetSize(pCfg->localArray); }
int32_t cfgGetGlobalSize(SConfig *pCfg) { return taosArrayGetSize(pCfg->globalArray); }

static int32_t cfgCheckAndSetConf(SConfigItem *pItem, const char *conf) {
  cfgItemFreeVal(pItem);
  if (!(pItem->str == NULL)) return TSDB_CODE_INVALID_PARA;

  pItem->str = taosStrdup(conf);

  if (pItem->str == NULL) return terrno;

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static int32_t cfgCheckAndSetDir(SConfigItem *pItem, const char *inputDir) {
  char fullDir[PATH_MAX] = {0};
  if (taosExpandDir(inputDir, fullDir, PATH_MAX) != 0) {
    int32_t code = terrno;
    uError("failed to expand dir:%s since %s", inputDir, tstrerror(code));
    TAOS_RETURN(code);
  }

  taosMemoryFreeClear(pItem->str);
  pItem->str = taosStrdup(fullDir);

  if (pItem->str == NULL) return terrno;

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static int32_t cfgSetBool(SConfigItem *pItem, const char *value, ECfgSrcType stype) {
  int32_t code = 0;
  bool    tmp = false;
  if (strcasecmp(value, "true") == 0) tmp = true;

  int32_t val = 0;
  if ((code = taosStr2int32(value, &val)) == 0 && val > 0) {
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
    TAOS_RETURN(terrno);
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
  if (value == NULL) {
    uError("cfg:%s, type:%s src:%s, value is null, skip to set timezone", pItem->name, cfgDtypeStr(pItem->dtype),
           cfgStypeStr(stype));
    TAOS_RETURN(TSDB_CODE_INVALID_CFG);
  }
  TAOS_CHECK_RETURN(osSetTimezone(value));

  TAOS_CHECK_RETURN(doSetConf(pItem, tsTimezoneStr, stype));

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static int32_t cfgSetCharset(SConfigItem *pItem, const char *value, ECfgSrcType stype) {
  if (stype == CFG_STYPE_ALTER_SERVER_CMD || stype == CFG_STYPE_ALTER_CLIENT_CMD) {
    uError("failed to config charset, not support");
    TAOS_RETURN(TSDB_CODE_INVALID_CFG);
  }

  if (value == NULL || strlen(value) == 0) {
    uError("cfg:%s, type:%s src:%s, value:%s, skip to set charset", pItem->name, cfgDtypeStr(pItem->dtype),
           cfgStypeStr(stype), value);
    TAOS_RETURN(TSDB_CODE_INVALID_CFG);
  }
#ifndef DISALLOW_NCHAR_WITHOUT_ICONV
  if (!taosValidateEncodec(value)) {
    uError("invalid charset:%s", value);
    TAOS_RETURN(terrno);
  }

  if ((tsCharsetCxt = taosConvInit(value)) == NULL) {
    TAOS_RETURN(terrno);
  }
  (void)memcpy(tsCharset, value, strlen(value) + 1);
  TAOS_CHECK_RETURN(doSetConf(pItem, value, stype));
#endif
  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static int32_t cfgSetLocale(SConfigItem *pItem, const char *value, ECfgSrcType stype) {
  if (stype == CFG_STYPE_ALTER_SERVER_CMD || (pItem->dynScope & CFG_DYN_CLIENT) == 0) {
    uError("failed to config locale, not support");
    TAOS_RETURN(TSDB_CODE_INVALID_CFG);
  }

  if (value == NULL || strlen(value) == 0 || taosSetSystemLocale(value) != 0) {
    uError("cfg:%s, type:%s src:%s, value:%s, skip to set locale", pItem->name, cfgDtypeStr(pItem->dtype),
           cfgStypeStr(stype), value);
    TAOS_RETURN(TSDB_CODE_INVALID_CFG);
  }

  TAOS_CHECK_RETURN(doSetConf(pItem, value, stype));

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static int32_t cfgSetTfsItem(SConfig *pCfg, const char *name, const char *value, const char *level, const char *primary,
                             const char *disable, ECfgSrcType stype) {
  int32_t code = 0;
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

      TAOS_RETURN(terrno);
    }
  }

  SDiskCfg cfg = {0};
  tstrncpy(cfg.dir, pItem->str, sizeof(cfg.dir));

  if (level == NULL || strlen(level) == 0) {
    cfg.level = 0;
  } else {
    code = taosStr2int32(level, &cfg.level);
    TAOS_CHECK_GOTO(code, NULL, _err);
  }

  if (primary == NULL || strlen(primary) == 0) {
    cfg.primary = 1;
  } else {
    code = taosStr2int32(primary, &cfg.primary);
    TAOS_CHECK_GOTO(code, NULL, _err);
  }

  if (disable == NULL || strlen(disable) == 0) {
    cfg.disable = 0;
  } else {
    code = taosStr2int8(disable, &cfg.disable);
    TAOS_CHECK_GOTO(code, NULL, _err);
  }
  void *ret = taosArrayPush(pItem->array, &cfg);
  if (ret == NULL) {
    code = terrno;
    TAOS_CHECK_GOTO(code, NULL, _err);
  }

  pItem->stype = stype;
  (void)taosThreadMutexUnlock(&pCfg->lock);

  TAOS_RETURN(TSDB_CODE_SUCCESS);
_err:
  (void)taosThreadMutexUnlock(&pCfg->lock);
  TAOS_RETURN(code);
}

static int32_t cfgUpdateDebugFlagItem(SConfig *pCfg, const char *name, bool resetArray) {
  SConfigItem *pDebugFlagItem = cfgGetItem(pCfg, "debugFlag");
  if (resetArray) {
    // reset
    if (pDebugFlagItem == NULL) TAOS_RETURN(TSDB_CODE_CFG_NOT_FOUND);

    // logflag names that should 'not' be set by 'debugFlag'
    if (pDebugFlagItem->array == NULL) {
      pDebugFlagItem->array = taosArrayInit(16, sizeof(SLogVar));
      if (pDebugFlagItem->array == NULL) return terrno;
    }
    taosArrayClear(pDebugFlagItem->array);
    TAOS_RETURN(TSDB_CODE_SUCCESS);
  }

  // update
  if (pDebugFlagItem == NULL) return -1;
  if (pDebugFlagItem->array != NULL) {
    SLogVar logVar = {0};
    tstrncpy(logVar.name, name, TSDB_LOG_VAR_LEN);
    if (NULL == taosArrayPush(pDebugFlagItem->array, &logVar)) return terrno;
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

  code = cfgSetItemVal(pItem, name, value, stype);
  if (code != TSDB_CODE_SUCCESS) {
    if (lock) {
      (void)taosThreadMutexUnlock(&pCfg->lock);
    }
    TAOS_RETURN(code);
  }

  if (lock) {
    (void)taosThreadMutexUnlock(&pCfg->lock);
  }

  TAOS_RETURN(code);
}

int32_t cfgGetAndSetItem(SConfig *pCfg, SConfigItem **pItem, const char *name, const char *value, ECfgSrcType stype,
                         bool lock) {
  // GRANT_CFG_SET;
  int32_t code = TSDB_CODE_SUCCESS;

  if (lock) {
    (void)taosThreadMutexLock(&pCfg->lock);
  }

  *pItem = cfgGetItem(pCfg, name);
  if (*pItem == NULL) {
    code = TSDB_CODE_CFG_NOT_FOUND;
    goto _exit;
  }

  TAOS_CHECK_GOTO(cfgSetItemVal(*pItem, name, value, stype), NULL, _exit);

_exit:
  if (lock) {
    (void)taosThreadMutexUnlock(&pCfg->lock);
  }

  TAOS_RETURN(code);
}

int32_t cfgSetItemVal(SConfigItem *pItem, const char *name, const char *value, ECfgSrcType stype) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (pItem == NULL) return TSDB_CODE_CFG_NOT_FOUND;

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
      code = cfgSetCharset(pItem, value, stype);
      break;
    }
    case CFG_DTYPE_LOCALE: {
      code = cfgSetLocale(pItem, value, stype);
      break;
    }
    case CFG_DTYPE_NONE:
    default:
      code = TSDB_CODE_INVALID_CFG;
      break;
  }

  TAOS_RETURN(code);
}

SConfigItem *cfgGetItem(SConfig *pCfg, const char *pName) {
  if (pCfg == NULL) return NULL;
  int32_t size = taosArrayGetSize(pCfg->localArray);
  for (int32_t i = 0; i < size; ++i) {
    SConfigItem *pItem = taosArrayGet(pCfg->localArray, i);
    if (taosStrcasecmp(pItem->name, pName) == 0) {
      return pItem;
    }
  }
  size = taosArrayGetSize(pCfg->globalArray);
  for (int32_t i = 0; i < size; ++i) {
    SConfigItem *pItem = taosArrayGet(pCfg->globalArray, i);
    if (taosStrcasecmp(pItem->name, pName) == 0) {
      return pItem;
    }
  }

  return NULL;
}

void cfgLock(SConfig *pCfg) {
  if (pCfg == NULL) return;
  (void)taosThreadMutexLock(&pCfg->lock);
}

void cfgUnLock(SConfig *pCfg) { (void)taosThreadMutexUnlock(&pCfg->lock); }

int32_t checkItemDyn(SConfigItem *pItem, bool isServer) {
  if (pItem->dynScope == CFG_DYN_NONE) {
    return TSDB_CODE_INVALID_CFG;
  }
  if (isServer) {
    if (pItem->dynScope == CFG_DYN_CLIENT || pItem->dynScope == CFG_DYN_CLIENT_LAZY) {
      return TSDB_CODE_INVALID_CFG;
    }
  } else {
    if (pItem->dynScope == CFG_DYN_SERVER || pItem->dynScope == CFG_DYN_SERVER_LAZY) {
      return TSDB_CODE_INVALID_CFG;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t cfgCheckRangeForDynUpdate(SConfig *pCfg, const char *name, const char *pVal, bool isServer,
                                  CfgAlterType alterType) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  cfgLock(pCfg);

  SConfigItem *pItem = cfgGetItem(pCfg, name);
  TSDB_CHECK_NULL(pItem, code, lino, _exit, TSDB_CODE_CFG_NOT_FOUND);

  TAOS_CHECK_EXIT(checkItemDyn(pItem, isServer));

  if ((pItem->category == CFG_CATEGORY_GLOBAL) && alterType == CFG_ALTER_DNODE) {
    uError("failed to config:%s, not support update global config on only one dnode", name);
    code = TSDB_CODE_INVALID_CFG;
    goto _exit;
  }
  switch (pItem->dtype) {
    case CFG_DTYPE_STRING: {
      if (strcasecmp(name, "slowLogScope") == 0) {
        char *tmp = taosStrdup(pVal);
        if (!tmp) {
          code = terrno;
          goto _exit;
        }
        int32_t scope = 0;
        code = taosSetSlowLogScope(tmp, &scope);
        if (TSDB_CODE_SUCCESS != code) {
          taosMemoryFree(tmp);
          goto _exit;
        }
        taosMemoryFree(tmp);
      }
    } break;
    case CFG_DTYPE_BOOL: {
      int32_t ival = 0;
      code = taosStr2int32(pVal, &ival);
      if (code != 0 || (ival != 0 && ival != 1)) {
        uError("cfg:%s, type:%s value:%d out of range[0, 1]", pItem->name, cfgDtypeStr(pItem->dtype), ival);
        code = TSDB_CODE_OUT_OF_RANGE;
        goto _exit;
      }
    } break;
    case CFG_DTYPE_INT32: {
      int32_t ival;
      code = (int32_t)taosStrHumanToInt32(pVal, &ival);
      if (code != TSDB_CODE_SUCCESS) {
        cfgUnLock(pCfg);
        return code;
      }
      if (ival < pItem->imin || ival > pItem->imax) {
        uError("cfg:%s, type:%s value:%d out of range[%" PRId64 ", %" PRId64 "]", pItem->name,
               cfgDtypeStr(pItem->dtype), ival, pItem->imin, pItem->imax);
        code = TSDB_CODE_OUT_OF_RANGE;
        goto _exit;
      }
    } break;
    case CFG_DTYPE_INT64: {
      int64_t ival;
      code = taosStrHumanToInt64(pVal, &ival);
      if (code != TSDB_CODE_SUCCESS) {
        cfgUnLock(pCfg);
        TAOS_RETURN(code);
      }
      if (ival < pItem->imin || ival > pItem->imax) {
        uError("cfg:%s, type:%s value:%" PRId64 " out of range[%" PRId64 ", %" PRId64 "]", pItem->name,
               cfgDtypeStr(pItem->dtype), ival, pItem->imin, pItem->imax);
        code = TSDB_CODE_OUT_OF_RANGE;
        goto _exit;
      }
    } break;
    case CFG_DTYPE_FLOAT:
    case CFG_DTYPE_DOUBLE: {
      float dval = 0;
      TAOS_CHECK_EXIT(parseCfgReal(pVal, &dval));

      if (dval < pItem->fmin || dval > pItem->fmax) {
        uError("cfg:%s, type:%s value:%g out of range[%g, %g]", pItem->name, cfgDtypeStr(pItem->dtype), dval,
               pItem->fmin, pItem->fmax);
        code = TSDB_CODE_OUT_OF_RANGE;
        goto _exit;
      }
    } break;
    default:
      break;
  }

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    uError("failed to check range for cfg:%s, value:%s, since %s at line:%d", name, pVal, tstrerror(code), __LINE__);
  }
  cfgUnLock(pCfg);
  TAOS_RETURN(code);
}

static int32_t cfgAddItem(SConfig *pCfg, SConfigItem *pItem, const char *name) {
  SArray *array = pCfg->globalArray;
  if (pItem->category == CFG_CATEGORY_LOCAL) array = pCfg->localArray;

  pItem->stype = CFG_STYPE_DEFAULT;
  pItem->name = taosStrdup(name);
  if (pItem->name == NULL) return terrno;

  int32_t size = taosArrayGetSize(array);
  for (int32_t i = 0; i < size; ++i) {
    SConfigItem *existItem = taosArrayGet(array, i);
    if (existItem != NULL && strcmp(existItem->name, pItem->name) == 0) {
      taosMemoryFree(pItem->name);
      TAOS_RETURN(TSDB_CODE_INVALID_CFG);
    }
  }

  int32_t len = strlen(name);
  char    lowcaseName[CFG_NAME_MAX_LEN + 1] = {0};
  (void)strntolower(lowcaseName, name, TMIN(CFG_NAME_MAX_LEN, len));

  if (taosArrayPush(array, pItem) == NULL) {
    if (pItem->dtype == CFG_DTYPE_STRING) {
      taosMemoryFree(pItem->str);
    }

    taosMemoryFree(pItem->name);
    TAOS_RETURN(terrno);
  }

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

int32_t cfgAddBool(SConfig *pCfg, const char *name, bool defaultVal, int8_t scope, int8_t dynScope, int8_t category) {
  SConfigItem item = {
      .dtype = CFG_DTYPE_BOOL, .bval = defaultVal, .scope = scope, .dynScope = dynScope, .category = category};
  return cfgAddItem(pCfg, &item, name);
}

int32_t cfgAddInt32Ex(SConfig *pCfg, const char *name, int32_t defaultVal, int64_t minval, int64_t maxval, int8_t scope,
                      int8_t dynScope, int8_t category) {
  SConfigItem item = {.dtype = CFG_DTYPE_INT32,
                      .i32 = defaultVal,
                      .imin = minval,
                      .imax = maxval,
                      .scope = scope,
                      .dynScope = dynScope,
                      .category = category};
  return cfgAddItem(pCfg, &item, name);
}

int32_t cfgAddInt32(SConfig *pCfg, const char *name, int32_t defaultVal, int64_t minval, int64_t maxval, int8_t scope,
                    int8_t dynScope, int8_t category) {
  if (defaultVal < minval || defaultVal > maxval) {
    TAOS_RETURN(TSDB_CODE_OUT_OF_RANGE);
  }

  SConfigItem item = {.dtype = CFG_DTYPE_INT32,
                      .i32 = defaultVal,
                      .imin = minval,
                      .imax = maxval,
                      .scope = scope,
                      .dynScope = dynScope,
                      .category = category};
  return cfgAddItem(pCfg, &item, name);
}

int32_t cfgAddInt64(SConfig *pCfg, const char *name, int64_t defaultVal, int64_t minval, int64_t maxval, int8_t scope,
                    int8_t dynScope, int8_t category) {
  if (defaultVal < minval || defaultVal > maxval) {
    TAOS_RETURN(TSDB_CODE_OUT_OF_RANGE);
  }

  SConfigItem item = {.dtype = CFG_DTYPE_INT64,
                      .i64 = defaultVal,
                      .imin = minval,
                      .imax = maxval,
                      .scope = scope,
                      .dynScope = dynScope,
                      .category = category};
  return cfgAddItem(pCfg, &item, name);
}

int32_t cfgAddFloat(SConfig *pCfg, const char *name, float defaultVal, float minval, float maxval, int8_t scope,
                    int8_t dynScope, int8_t category) {
  if (defaultVal < minval || defaultVal > maxval) {
    TAOS_RETURN(TSDB_CODE_OUT_OF_RANGE);
  }

  SConfigItem item = {.dtype = CFG_DTYPE_FLOAT,
                      .fval = defaultVal,
                      .fmin = minval,
                      .fmax = maxval,
                      .scope = scope,
                      .dynScope = dynScope,
                      .category = category};
  return cfgAddItem(pCfg, &item, name);
}

int32_t cfgAddString(SConfig *pCfg, const char *name, const char *defaultVal, int8_t scope, int8_t dynScope,
                     int8_t category) {
  SConfigItem item = {.dtype = CFG_DTYPE_STRING, .scope = scope, .dynScope = dynScope, .category = category};
  item.str = taosStrdup(defaultVal);
  if (item.str == NULL) return terrno;

  return cfgAddItem(pCfg, &item, name);
}

int32_t cfgAddDir(SConfig *pCfg, const char *name, const char *defaultVal, int8_t scope, int8_t dynScope,
                  int8_t category) {
  SConfigItem item = {.dtype = CFG_DTYPE_DIR, .scope = scope, .dynScope = dynScope, .category = category};
  TAOS_CHECK_RETURN(cfgCheckAndSetDir(&item, defaultVal));
  return cfgAddItem(pCfg, &item, name);
}

int32_t cfgAddLocale(SConfig *pCfg, const char *name, const char *defaultVal, int8_t scope, int8_t dynScope,
                     int8_t category) {
  SConfigItem item = {.dtype = CFG_DTYPE_LOCALE, .scope = scope, .dynScope = dynScope, .category = category};
  TAOS_CHECK_RETURN(cfgCheckAndSetConf(&item, defaultVal));
  return cfgAddItem(pCfg, &item, name);
}

int32_t cfgAddCharset(SConfig *pCfg, const char *name, const char *defaultVal, int8_t scope, int8_t dynScope,
                      int8_t category) {
  SConfigItem item = {.dtype = CFG_DTYPE_CHARSET, .scope = scope, .dynScope = dynScope, .category = category};
  TAOS_CHECK_RETURN(cfgCheckAndSetConf(&item, defaultVal));
  return cfgAddItem(pCfg, &item, name);
}

int32_t cfgAddTimezone(SConfig *pCfg, const char *name, const char *defaultVal, int8_t scope, int8_t dynScope,
                       int8_t category) {
  SConfigItem item = {.dtype = CFG_DTYPE_TIMEZONE, .scope = scope, .dynScope = dynScope, .category = category};
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
    case CFG_STYPE_ALTER_CLIENT_CMD:
      return "alter_client_cmd";
    case CFG_STYPE_ALTER_SERVER_CMD:
      return "alter_server_cmd";
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
      len = tsnprintf(buf, bufSize, "%u", pItem->bval);
      break;
    case CFG_DTYPE_INT32:
      len = tsnprintf(buf, bufSize, "%d", pItem->i32);
      break;
    case CFG_DTYPE_INT64:
      len = tsnprintf(buf, bufSize, "%" PRId64, pItem->i64);
      break;
    case CFG_DTYPE_FLOAT:
    case CFG_DTYPE_DOUBLE:
      len = tsnprintf(buf, bufSize, "%f", pItem->fval);
      break;
    case CFG_DTYPE_TIMEZONE: {
      //      char str1[TD_TIMEZONE_LEN] = {0};
      //      time_t    tx1 = taosGetTimestampSec();
      //      if (taosFormatTimezoneStr(tx1, buf, NULL, str1) != 0) {
      //        tstrncpy(str1, "tz error", sizeof(str1));
      //      }
      //      len = tsnprintf(buf, bufSize, "%s", str1);
      //      break;
    }
    case CFG_DTYPE_STRING:
    case CFG_DTYPE_DIR:
    case CFG_DTYPE_LOCALE:
    case CFG_DTYPE_CHARSET:
    case CFG_DTYPE_NONE:
      len = tsnprintf(buf, bufSize, "%s", pItem->str);
      break;
  }

  if (len < 0) return terrno;

  if (len > bufSize) len = bufSize;

  *pLen = len;
  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

int32_t cfgDumpItemScope(SConfigItem *pItem, char *buf, int32_t bufSize, int32_t *pLen) {
  int32_t len = 0;
  switch (pItem->scope) {
    case CFG_SCOPE_SERVER:
      len = tsnprintf(buf, bufSize, "server");
      break;
    case CFG_SCOPE_CLIENT:
      len = tsnprintf(buf, bufSize, "client");
      break;
    case CFG_SCOPE_BOTH:
      len = tsnprintf(buf, bufSize, "both");
      break;
  }

  if (len < 0) {
    TAOS_RETURN(TAOS_SYSTEM_ERROR(ERRNO));
  }

  if (len > bufSize) {
    len = bufSize;
  }

  *pLen = len;
  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

int32_t cfgDumpItemCategory(SConfigItem *pItem, char *buf, int32_t bufSize, int32_t *pLen) {
  int32_t len = 0;
  switch (pItem->category) {
    case CFG_CATEGORY_LOCAL:
      len = tsnprintf(buf, bufSize, "local");
      break;
    case CFG_CATEGORY_GLOBAL:
      len = tsnprintf(buf, bufSize, "global");
      break;
    default:
      uError("invalid category:%d", pItem->category);
      TAOS_RETURN(TSDB_CODE_INVALID_CFG);
  }

  if (len < 0) {
    TAOS_RETURN(TAOS_SYSTEM_ERROR(ERRNO));
  }

  if (len > bufSize) {
    len = bufSize;
  }

  *pLen = len;
  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

void cfgDumpCfgImpl(SArray *array, bool tsc, bool dump) {
  char src[CFG_SRC_PRINT_LEN + 1] = {0};
  char name[CFG_NAME_PRINT_LEN + 1] = {0};

  int32_t size = taosArrayGetSize(array);
  for (int32_t i = 0; i < size; ++i) {
    SConfigItem *pItem = taosArrayGet(array, i);
    if (tsc && pItem->scope == CFG_SCOPE_SERVER) continue;
    if (dump && strcmp(pItem->name, "scriptDir") == 0) continue;
    if (strcmp(pItem->name, "ssAccessString") == 0) continue; // contains sensitive information

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
        if (strcasecmp(pItem->name, "dataDir") == 0) {
          size_t sz = taosArrayGetSize(pItem->array);
          if (sz > 1) {
            for (size_t j = 0; j < sz; ++j) {
              SDiskCfg *pCfg = taosArrayGet(pItem->array, j);
              if (dump) {
                (void)printf("%s %s %s l:%d p:%d d:%" PRIi8 "\n", src, name, pCfg->dir, pCfg->level, pCfg->primary,
                             pCfg->disable);
              } else {
                uInfo("%s %s %s l:%d p:%d d:%" PRIi8, src, name, pCfg->dir, pCfg->level, pCfg->primary, pCfg->disable);
              }
            }
            break;
          }
        }
        if (dump) {
          (void)printf("%s %s %s\n", src, name, pItem->str);
        } else {
          uInfo("%s %s %s", src, name, pItem->str);
        }

        break;
    }
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
  cfgDumpCfgImpl(pCfg->localArray, tsc, dump);
  cfgDumpCfgImpl(pCfg->globalArray, tsc, dump);
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

    tstrncpy(line, *pEnv, sizeof(line));
    pEnv++;
    if (taosEnvToCfg(line, line, 1024) < 0) {
      uTrace("failed to convert env to cfg:%s", line);
    }

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
    tstrncpy(buf, envCmd[index], sizeof(buf));
    buf[sizeof(buf) - 1] = 0;
    if (taosEnvToCfg(buf, buf, 1024) < 0) {
      uTrace("failed to convert env to cfg:%s", buf);
    }
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
  if (pFile == NULL) return terrno;

  while (!taosEOFFile(pFile)) {
    name = value = value2 = value3 = value4 = NULL;
    olen = vlen = vlen2 = vlen3 = vlen4 = 0;

    _bytes = taosGetsFile(pFile, sizeof(line), line);
    if (_bytes <= 0) {
      break;
    }
    if (line[_bytes - 1] == '\n') line[_bytes - 1] = 0;
    if (taosEnvToCfg(line, line, 1024) < 0) {
      uTrace("failed to convert env to cfg:%s", line);
    }

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
    code = terrno;
    if (ERRNO == ENOENT) {
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

      tstrncpy(newValue, value, sizeof(newValue));

      int32_t count = 1;
      while (vlen < 1024) {
        (void)paGetToken(value + vlen + 1 * count, &tmp, &len);
        if (len == 0) break;
        tmp[len] = 0;
        tstrncpy(newValue + vlen, tmp, sizeof(newValue) - vlen);
        vlen += len;
        count++;
      }

      code = cfgSetItem(pConfig, name, newValue, CFG_STYPE_CFG_FILE, true);
      if (TSDB_CODE_SUCCESS != code && TSDB_CODE_CFG_NOT_FOUND != code) {
        (void)printf("cfg:%s, value:%s failed since %s\n", name, newValue, tstrerror(code));
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
    if (len >= debugFlagLen && taosStrcasecmp(name + len - debugFlagLen, debugFlagStr) == 0) {
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

int32_t cfgLoadFromApollUrl(SConfig *pConfig, const char *url) {
  char     *cfgLineBuf = NULL, *buf = NULL, *name, *value, *value2, *value3, *value4;
  SJson    *pJson = NULL;
  TdFilePtr pFile = NULL;

  int32_t olen, vlen, vlen2, vlen3, vlen4;
  int32_t code = 0, lino = 0;
  if (url == NULL || strlen(url) == 0) {
    uTrace("apoll url not load");
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
    TSDB_CHECK_NULL(pFile, code, lino, _exit, terrno);

    size_t fileSize = taosLSeekFile(pFile, 0, SEEK_END);
    if (fileSize <= 0) {
      code = terrno;
      goto _exit;
    }

    buf = taosMemoryMalloc(fileSize + 1);
    TSDB_CHECK_NULL(buf, code, lino, _exit, terrno);

    buf[fileSize] = 0;
    if (taosLSeekFile(pFile, 0, SEEK_SET) < 0) {
      code = terrno;
      goto _exit;
    }

    if (taosReadFile(pFile, buf, fileSize) <= 0) {
      code = TSDB_CODE_INVALID_DATA_FMT;
      goto _exit;
    }

    pJson = tjsonParse(buf);
    if (NULL == pJson) {
      const char *jsonParseError = tjsonGetError();
      if (jsonParseError != NULL) {
        (void)printf("load json file parse error: %s\n", jsonParseError);
      }
      taosMemoryFreeClear(buf);
      TAOS_CHECK_EXIT(TSDB_CODE_INVALID_DATA_FMT);
    }

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
          TAOS_CHECK_EXIT(terrno);
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

  uInfo("load from apoll url not implemented yet");

_exit:
  taosMemoryFree(cfgLineBuf);
  taosMemoryFree(buf);
  (void)taosCloseFile(&pFile);
  tjsonDelete(pJson);
  if (code == TSDB_CODE_CFG_NOT_FOUND) {
    uTrace("load from apoll url success");
    TAOS_RETURN(TSDB_CODE_SUCCESS);
  } else {
    (void)printf("failed to load from apoll url:%s at line %d since %s\n", url, lino, tstrerror(code));
    TAOS_RETURN(code);
  }
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
    tstrncpy(line, *pEnv, sizeof(line));
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
  if (pIter == NULL) return terrno;

  pIter->pConf = pConf;

  *ppIter = pIter;
  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

SConfigItem *cfgNextIter(SConfigIter *pIter) {
  if (pIter->index < cfgGetGlobalSize(pIter->pConf)) {
    return taosArrayGet(pIter->pConf->globalArray, pIter->index++);
  } else if (pIter->index < cfgGetGlobalSize(pIter->pConf) + cfgGetLocalSize(pIter->pConf)) {
    return taosArrayGet(pIter->pConf->localArray, pIter->index++ - cfgGetGlobalSize(pIter->pConf));
  }
  return NULL;
}

void cfgDestroyIter(SConfigIter *pIter) {
  if (pIter == NULL) return;

  taosMemoryFree(pIter);
}

SArray *taosGetLocalCfg(SConfig *pCfg) { return pCfg->localArray; }
SArray *taosGetGlobalCfg(SConfig *pCfg) { return pCfg->globalArray; }