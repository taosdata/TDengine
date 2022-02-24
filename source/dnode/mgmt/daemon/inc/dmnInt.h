
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

#ifndef _TD_DMN_INT_H_
#define _TD_DMN_INT_H_

#include "tconfig.h"
#include "dnode.h"
#include "taoserror.h"
#include "tglobal.h"
#include "ulog.h"
#include "version.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t dmnAddLogCfg(SConfig *pCfg);
int32_t dmnInitLog(const char *cfgDir, const char *envFile, const char *apolloUrl);
int32_t dmnLoadCfg(SConfig *pConfig, const char *inputCfgDir, const char *envFile, const char *apolloUrl);

SConfig     *dmnReadCfg(const char *cfgDir, const char *envFile, const char *apolloUrl);
SDnodeEnvCfg dmnGetEnvCfg(SConfig *pCfg);
SDnodeObjCfg dmnGetObjCfg(SConfig *pCfg);

void dmnDumpCfg(SConfig *pCfg);
void dmnPrintVersion();
void dmnGenerateGrant();

#ifdef __cplusplus
}
#endif

#endif /*_TD_DMN_INT_H_*/
