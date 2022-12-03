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

#ifndef _TD_MND_TOPIC_H_
#define _TD_MND_TOPIC_H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t mndInitTopic(SMnode *pMnode);
void    mndCleanupTopic(SMnode *pMnode);

SMqTopicObj *mndAcquireTopic(SMnode *pMnode, const char *topicName);
void         mndReleaseTopic(SMnode *pMnode, SMqTopicObj *pTopic);

SSdbRaw *mndTopicActionEncode(SMqTopicObj *pTopic);
SSdbRow *mndTopicActionDecode(SSdbRaw *pRaw);

int32_t mndDropTopicByDB(SMnode *pMnode, STrans *pTrans, SDbObj *pDb);
int32_t mndCheckTopicExist(SMnode *pMnode, SDbObj *pDb);

const char *mndTopicGetShowName(const char topic[TSDB_TOPIC_FNAME_LEN]);

int32_t mndSetTopicCommitLogs(SMnode *pMnode, STrans *pTrans, SMqTopicObj *pTopic);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_TOPIC_H_*/
