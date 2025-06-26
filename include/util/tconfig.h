
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

#ifndef _TD_CONFIG_H_
#define _TD_CONFIG_H_

#include "tarray.h"

#ifdef __cplusplus
extern "C" {
#endif

#define CFG_NAME_MAX_LEN 128

typedef enum {
  CFG_STYPE_DEFAULT,
  CFG_STYPE_CFG_FILE,
  CFG_STYPE_ENV_FILE,
  CFG_STYPE_ENV_VAR,
  CFG_STYPE_ENV_CMD,
  CFG_STYPE_APOLLO_URL,
  CFG_STYPE_ARG_LIST,
  CFG_STYPE_TAOS_OPTIONS,
  CFG_STYPE_ALTER_CLIENT_CMD,
  CFG_STYPE_ALTER_SERVER_CMD,
} ECfgSrcType;

typedef enum {
  CFG_DTYPE_NONE,
  CFG_DTYPE_BOOL,
  CFG_DTYPE_INT32,
  CFG_DTYPE_INT64,
  CFG_DTYPE_FLOAT,
  CFG_DTYPE_DOUBLE,
  CFG_DTYPE_STRING,
  CFG_DTYPE_DIR,
  CFG_DTYPE_LOCALE,
  CFG_DTYPE_CHARSET,
  CFG_DTYPE_TIMEZONE
} ECfgDataType;

typedef enum { CFG_SCOPE_SERVER, CFG_SCOPE_CLIENT, CFG_SCOPE_BOTH } ECfgScopeType;
typedef enum { CFG_CATEGORY_GLOBAL, CFG_CATEGORY_LOCAL } ECfgCategoryType;
typedef enum { CFG_ALTER_LOCAL, CFG_ALTER_DNODE, CFG_ALTER_ALL_DNODES } CfgAlterType;

typedef enum {
  CFG_DYN_NONE = 0,
  CFG_DYN_SERVER = 1,
  CFG_DYN_CLIENT = 2,
  CFG_DYN_SERVER_LAZY = 3,
  CFG_DYN_CLIENT_LAZY = 4,
  CFG_DYN_BOTH_LAZY = 5,
  CFG_DYN_BOTH = 6,
#ifdef TD_ENTERPRISE
  CFG_DYN_ENT_SERVER = CFG_DYN_SERVER,
  CFG_DYN_ENT_CLIENT = CFG_DYN_CLIENT,
  CFG_DYN_ENT_SERVER_LAZY = CFG_DYN_SERVER_LAZY,
  CFG_DYN_ENT_CLIENT_LAZY = CFG_DYN_CLIENT_LAZY,
  CFG_DYN_ENT_BOTH_LAZY = CFG_DYN_BOTH_LAZY,
  CFG_DYN_ENT_BOTH = CFG_DYN_BOTH,
#else
  CFG_DYN_ENT_SERVER = CFG_DYN_NONE,
  CFG_DYN_ENT_CLIENT = CFG_DYN_NONE,
  CFG_DYN_ENT_SERVER_LAZY = CFG_DYN_NONE,
  CFG_DYN_ENT_CLIENT_LAZY = CFG_DYN_NONE,
  CFG_DYN_ENT_BOTH_LAZY = CFG_DYN_NONE,
  CFG_DYN_ENT_BOTH = CFG_DYN_NONE,
#endif
} ECfgDynType;

typedef struct SConfigItem {
  ECfgSrcType  stype;
  ECfgDataType dtype;
  int8_t       scope;
  int8_t       dynScope;
  int8_t       category;
  char        *name;
  union {
    bool    bval;
    float   fval;
    int32_t i32;
    int64_t i64;
    char   *str;
  };
  union {
    int64_t imin;
    float   fmin;
  };
  union {
    int64_t imax;
    float   fmax;
  };
  SArray *array;  // SDiskCfg/SLogVar
} SConfigItem;

typedef struct {
  const char *name;
  const char *value;
} SConfigPair;

typedef struct SConfig     SConfig;
typedef struct SConfigIter SConfigIter;

int32_t      cfgInit(SConfig **ppCfg);
int32_t      cfgLoad(SConfig *pCfg, ECfgSrcType cfgType, const void *sourceStr);
int32_t      cfgLoadFromArray(SConfig *pCfg, SArray *pArgs);    // SConfigPair
int32_t      cfgUpdateFromArray(SConfig *pCfg, SArray *pArgs);  // SConfigItem
void         cfgCleanup(SConfig *pCfg);
int32_t      cfgGetSize(SConfig *pCfg);
SConfigItem *cfgGetItem(SConfig *pCfg, const char *pName);
int32_t      cfgSetItem(SConfig *pCfg, const char *name, const char *value, ECfgSrcType stype, bool lock);
int32_t cfgGetAndSetItem(SConfig *pCfg, SConfigItem **ppItem, const char *name, const char *value, ECfgSrcType stype,
                         bool lock);
int32_t cfgCheckRangeForDynUpdate(SConfig *pCfg, const char *name, const char *pVal, bool isServer,
                                  CfgAlterType alterType);

int32_t      cfgCreateIter(SConfig *pConf, SConfigIter **ppIter);
SConfigItem *cfgNextIter(SConfigIter *pIter);
void         cfgDestroyIter(SConfigIter *pIter);
void         cfgLock(SConfig *pCfg);
void         cfgUnLock(SConfig *pCfg);

// clang-format off
int32_t cfgAddBool(SConfig *pCfg, const char *name, bool defaultVal, int8_t scope, int8_t dynScope,int8_t category);
int32_t cfgAddInt32(SConfig *pCfg, const char *name, int32_t defaultVal, int64_t minval, int64_t maxval, int8_t scope, int8_t dynScope,int8_t category);
int32_t cfgAddInt32Ex(SConfig *pCfg, const char *name, int32_t defaultVal, int64_t minval, int64_t maxval, int8_t scope, int8_t dynScope,int8_t category);
int32_t cfgAddInt64(SConfig *pCfg, const char *name, int64_t defaultVal, int64_t minval, int64_t maxval, int8_t scope, int8_t dynScope,int8_t category);
int32_t cfgAddFloat(SConfig *pCfg, const char *name, float defaultVal, float minval, float maxval, int8_t scope, int8_t dynScope,int8_t category);
int32_t cfgAddString(SConfig *pCfg, const char *name, const char *defaultVal, int8_t scope, int8_t dynScope,int8_t category);
int32_t cfgAddDir(SConfig *pCfg, const char *name, const char *defaultVal, int8_t scope, int8_t dynScope,int8_t category);
int32_t cfgAddLocale(SConfig *pCfg, const char *name, const char *defaultVal, int8_t scope, int8_t dynScope,int8_t category);
int32_t cfgAddCharset(SConfig *pCfg, const char *name, const char *defaultVal, int8_t scope, int8_t dynScope,int8_t category);
int32_t cfgAddTimezone(SConfig *pCfg, const char *name, const char *defaultVal, int8_t scope, int8_t dynScope,int8_t category);
// clang-format on

const char *cfgStypeStr(ECfgSrcType type);
const char *cfgDtypeStr(ECfgDataType type);

int32_t cfgDumpItemValue(SConfigItem *pItem, char *buf, int32_t bufSize, int32_t *pLen);
int32_t cfgDumpItemScope(SConfigItem *pItem, char *buf, int32_t bufSize, int32_t *pLen);
int32_t cfgDumpItemCategory(SConfigItem *pItem, char *buf, int32_t bufSize, int32_t *pLen);

void cfgDumpCfg(SConfig *pCfg, bool tsc, bool dump);
void cfgDumpCfgSs(SConfig *pCfg, bool tsc, bool dump);

int32_t cfgGetApollUrl(const char **envCmd, const char *envFile, char *apolloUrl);
SArray *taosGetLocalCfg(SConfig *pCfg);
SArray *taosGetGlobalCfg(SConfig *pCfg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_CONFIG_H_*/
