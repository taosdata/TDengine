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

#ifndef _TD_MND_CONSUMER_H_
#define _TD_MND_CONSUMER_H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

enum {
  MQ_CONSUMER_STATUS__MODIFY = 1,
  MQ_CONSUMER_STATUS__MODIFY_IN_REB,
  MQ_CONSUMER_STATUS__READY,
  MQ_CONSUMER_STATUS__LOST,
  MQ_CONSUMER_STATUS__LOST_IN_REB,
  MQ_CONSUMER_STATUS__LOST_REBD,
  MQ_CONSUMER_STATUS__REMOVED,
};

int32_t mndInitConsumer(SMnode *pMnode);
void    mndCleanupConsumer(SMnode *pMnode);

SMqConsumerObj *mndAcquireConsumer(SMnode *pMnode, int64_t consumerId);
void            mndReleaseConsumer(SMnode *pMnode, SMqConsumerObj *pConsumer);

SMqConsumerObj *mndCreateConsumer(int64_t consumerId, const char *cgroup);

SSdbRaw *mndConsumerActionEncode(SMqConsumerObj *pConsumer);
SSdbRow *mndConsumerActionDecode(SSdbRaw *pRaw);

int32_t mndSetConsumerCommitLogs(SMnode *pMnode, STrans *pTrans, SMqConsumerObj *pConsumer);

bool mndRebTryStart();
void mndRebEnd();
void mndRebCntInc();
void mndRebCntDec();

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_CONSUMER_H_*/
