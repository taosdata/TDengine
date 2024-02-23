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

#ifndef _TD_MND_SCHEDULER_H_
#define _TD_MND_SCHEDULER_H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif


int32_t mndSchedInitSubEp(SMnode* pMnode, const SMqTopicObj* pTopic, SMqSubscribeObj* pSub);
int32_t mndConvertRsmaTask(char** pDst, int32_t* pDstLen, const char* ast, int64_t uid, int8_t triggerType,
                           int64_t watermark, int64_t deleteMark);

int32_t mndScheduleStream(SMnode* pMnode, SStreamObj* pStream, int64_t skey, SArray* pVerList);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_SCHEDULER_H_ */
