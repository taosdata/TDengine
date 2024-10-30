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
  MQ_CONSUMER_STATUS_REBALANCE = 1,
  MQ_CONSUMER_STATUS_READY,
//  MQ_CONSUMER_STATUS_LOST,
};

int32_t mndInitConsumer(SMnode *pMnode);
void    mndCleanupConsumer(SMnode *pMnode);
int32_t mndSendConsumerMsg(SMnode *pMnode, int64_t consumerId, uint16_t msgType, SRpcHandleInfo* info);

int32_t mndAcquireConsumer(SMnode *pMnode, int64_t consumerId, SMqConsumerObj** pConsumer);
void    mndReleaseConsumer(SMnode *pMnode, SMqConsumerObj *pConsumer);

SSdbRaw *mndConsumerActionEncode(SMqConsumerObj *pConsumer);
SSdbRow *mndConsumerActionDecode(SSdbRaw *pRaw);

int32_t mndSetConsumerCommitLogs(STrans *pTrans, SMqConsumerObj *pConsumer);
int32_t mndSetConsumerDropLogs(STrans *pTrans, SMqConsumerObj *pConsumer);

const char *mndConsumerStatusName(int status);

#define MND_TMQ_NULL_CHECK(c)                \
  do {                                   \
    if (c == NULL) {                     \
      code = TAOS_GET_TERRNO(TSDB_CODE_OUT_OF_MEMORY);     \
      goto END;                          \
    }                                    \
  } while (0)

#define MND_TMQ_RETURN_CHECK(c)                \
  do {                                     \
    code = c;                            \
    if (code != 0) {                     \
      goto END;                          \
    }                                    \
  } while (0)

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_CONSUMER_H_*/
