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

#ifndef _TD_COMMON_GLOBAL_H_
#define _TD_COMMON_GLOBAL_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "tdef.h"

// common
extern int8_t   tsDaylight;
extern int32_t  tsCompressMsgSize;
extern int32_t  tsCompressColData;
extern int32_t  tsMaxNumOfDistinctResults;
extern char     tsTempDir[];
extern int      tsCompatibleModel;  // 2.0 compatible model
extern int8_t   tsEnableSlaveQuery;
extern int8_t   tsEnableAdjustMaster;
extern int8_t   tsPrintAuth;
extern int64_t  tsTickPerDay[3];

// query buffer management
extern int32_t tsQueryBufferSize;  // maximum allowed usage buffer size in MB for each data node during query processing
extern int64_t tsQueryBufferSizeBytes;   // maximum allowed usage buffer size in byte for each data node
extern int32_t tsRetrieveBlockingModel;  // retrieve threads will be blocked
extern int8_t  tsKeepOriginalColumnName;
extern int8_t  tsDeadLockKillQuery;

// client
extern int32_t tsMaxWildCardsLen;
extern int32_t tsMaxRegexStringLen;
extern int8_t  tsTscEnableRecordSql;
extern int32_t tsMaxNumOfOrderedResults;
extern int32_t tsMinSlidingTime;
extern int32_t tsMinIntervalTime;
extern int32_t tsMaxStreamComputDelay;
extern int32_t tsStreamCompStartDelay;
extern int32_t tsRetryStreamCompDelay;
extern float   tsStreamComputDelayRatio;  // the delayed computing ration of the whole time window
extern int32_t tsProjectExecInterval;
extern int64_t tsMaxRetentWindow;

// system info

extern uint32_t tsVersion;

// build info
extern char version[];
extern char compatible_version[];
extern char gitinfo[];
extern char gitinfoOfInternal[];
extern char buildinfo[];

// lossy
extern char     tsLossyColumns[];
extern double   tsFPrecision;
extern double   tsDPrecision;
extern uint32_t tsMaxRange;
extern uint32_t tsCurRange;
extern char     tsCompressor[];


#define NEEDTO_COMPRESSS_MSG(size) (tsCompressMsgSize != -1 && (size) > tsCompressMsgSize)

void    taosInitGlobalCfg();
int32_t taosCfgDynamicOptions(char *msg);
bool    taosCheckBalanceCfgOptions(const char *option, int32_t *vnodeId, int32_t *dnodeId);
void    taosAddDataDir(int index, char *v1, int level, int primary);
void    taosReadDataDirCfg(char *v1, char *v2, char *v3);
void    taosPrintDataDirCfg();

#ifdef __cplusplus
}
#endif

#endif /*_TD_COMMON_GLOBAL_H_*/
