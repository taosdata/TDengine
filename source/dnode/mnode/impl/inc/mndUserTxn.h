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

#define UTXN_ID_MASK   0xFFFFFFFF00000000ULL
#define IS_UTXN_ID(id) (((id) & UTXN_ID_MASK) != 0)
#define TRANS_ID(id)   ((int32_t)((id) & 0xFFFFFFFF))

// 用户事务状态
typedef enum {
  UTXN_STATUS_NONE = 0,
  UTXN_STATUS_BEGUN = 1,        // 已经开始事务，正在收集操作，但还未通知 VNode 创建影子数据
  UTXN_STATUS_PREPARING = 2,    // 正在通知 VNode 创建影子数据
  UTXN_STATUS_PREPARED = 3,     // 所有 VNode 影子数据创建完成，准备持久化决策
  UTXN_STATUS_COMMITTING = 4,   // MNode 已下达 Commit 决策，正在等待全员 ACK
  UTXN_STATUS_COMMITTED = 5,    // 【终态】全员 ACK 已收齐，事务即将从内存销毁
  UTXN_STATUS_ROLLBACKING = 6,  // 正在通知 VNode 回滚（清理影子数据）
  UTXN_STATUS_ROLLBACKED = 7,   // 【终态】全员已回滚，事务即将从内存销毁
  UTXN_STATUS_ZOMBIE = 8,       // 【异常态】长时间未收齐 ACK，等待运维手工处理
} EUtxnStatus;

// MNode 侧的用户事务上下文
typedef struct SUserTxn {
  utxn_id_t      txnId;      // 64位 ID
  uint32_t       connId;     // 关联的 RPC 连接 ID
  int64_t        startTime;  // 用于简单超时参考（虽然你说暂时不考虑）
  int64_t        lastActive;
  int8_t         status;  // EUtxnStatus
  TdThreadRwlock lock;    // 保护本事务上下文的并发访问
  // 暂存该事务内的 DDL 操作列表
  // 每一个 STransAction 将在 COMMIT 时被转化为内部 STrans 的 Action
  SArray* pActions;  // Array of STransAction

  // 涉及到的 VNode 列表，COMMIT 时需要通知它们
  SArray* pVgList;       // Array of int32_t (vgId)
  int64_t lastWarnTime;  // 上次打印警告的时间戳
} SUserTxn;

int32_t mndInitTxn(SMnode* pMnode);
void    mndCleanupTxn(SMnode* pMnode);
const char* mndTxnStr(ETrnStage stage);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_USER_TXN__H_*/
