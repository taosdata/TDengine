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

#ifndef _TD_MND_USER_TXN__H_
#define _TD_MND_USER_TXN__H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

// 用户事务状态
typedef enum {
  UTXN_STATUS_BEGUN = 1,
  UTXN_STATUS_COMMITTING = 2,
} EUtxnStatus;

// MNode 侧的用户事务上下文
typedef struct SUserTxn {
  utxn_id_t txnId;      // 64位 ID
  uint32_t  connId;     // 关联的 RPC 连接 ID
  int64_t   startTime;  // 用于简单超时参考（虽然你说暂时不考虑）
  int8_t    status;     // EUtxnStatus

  // 暂存该事务内的 DDL 操作列表
  // 每一个 STransAction 将在 COMMIT 时被转化为内部 STrans 的 Action
  SArray* pActions;  // Array of STransAction

  // 涉及到的 VNode 列表，COMMIT 时需要通知它们
  SArray* pVgList;  // Array of int32_t (vgId)
} SUserTxn;

// 用户事务管理器（存放在 SMnode 中）
typedef struct {
  SHashObj*       pTxnHash;  // txnId -> SUserTxn*
  pthread_mutex_t lock;
} SUserTxnMgr;

int32_t mndInitTxn(SMnode* pMnode);
void    mndCleanupTxn(SMnode* pMnode);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_USER_TXN__H_*/
