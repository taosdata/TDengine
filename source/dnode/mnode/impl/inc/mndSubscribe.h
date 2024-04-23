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

#ifndef _TD_MND_SUBSCRIBE_H_
#define _TD_MND_SUBSCRIBE_H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t mndInitSubscribe(SMnode *pMnode);
void    mndCleanupSubscribe(SMnode *pMnode);

int32_t          mndGetGroupNumByTopic(SMnode *pMnode, const char *topicName);
SMqSubscribeObj *mndAcquireSubscribe(SMnode *pMnode, const char *CGroup, const char *topicName);
SMqSubscribeObj *mndAcquireSubscribeByKey(SMnode *pMnode, const char *key);
void             mndReleaseSubscribe(SMnode *pMnode, SMqSubscribeObj *pSub);

int32_t mndMakeSubscribeKey(char *key, const char *cgroup, const char *topicName);

int32_t mndDropSubByTopic(SMnode *pMnode, STrans *pTrans, const char *topic);
int32_t mndSetDropSubCommitLogs(SMnode *pMnode, STrans *pTrans, SMqSubscribeObj *pSub);

bool mndRebTryStart();
void mndRebCntInc();
void mndRebCntDec();

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_SUBSCRIBE_H_*/
