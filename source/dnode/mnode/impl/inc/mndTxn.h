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

// EUtxnStage / EVtxnStage 定义已移至 tdef.h（公共头文件），此处不再重复定义。
// 设计说明：
//   DECIDING 是 2PC 的"决断点"（Point of No Return）。MNode 将 COMMIT 决定写入 Raft WAL 后
//   即进入此阶段，此后无论发生任何故障（包括切主），新 Leader 都必须继续推进 COMMIT，不得回滚。
//   VNode 侧不需要区分"提交完成"和"回滚完成"——两者都意味着影子数据已处理完毕，
//   事务上下文可以销毁。因此用 FINISHING 统一表示，由 VNode 内部记录具体操作类型。

// MNode 侧的用户事务运行时上下文（纯内存，不持久化到 SDB）
// 持久化部分见 STxnObj（mndDef.h），切主后由新 Leader 从 SDB 重建此结构
typedef struct SUserTxn {
  utxn_id_t      txnId;      // 全局唯一 64 位事务 ID（高 32 位为 seq 段，低 32 位为 range 内偏移）
  uint32_t       connId;     // 关联的客户端 RPC 连接 ID，用于检测客户端断连后自动触发回滚
  SyncTerm       term;       // 事务创建时的 Raft 任期，用于切主后的 Fencing 校验
  int64_t        startTime;  // 事务创建时间戳（ms），用于超时检测
  int64_t        lastActive; // 最近一次活跃时间戳（ms），客户端心跳或 DDL 操作时更新
  int8_t         stage;      // EUtxnStage，当前事务阶段
  TdThreadRwlock lock;       // 保护本事务上下文的并发访问（读多写少场景）

  // 参与该事务的 VGroup 列表，PREPARE/COMMIT/ROLLBACK 时向其广播指令
  // 由 VNode 首次收到带 txnId 的 DDL 时，主动向 MNode 注册（MSG_MND_TXN_REG）
  SArray* pVgList;  // Array of int32_t (vgId)，有序，用于位图索引

  // ACK 位图：记录哪些 VGroup 已响应当前阶段的指令（PREPARING 阶段记录 PREPARE ACK，
  // COMMITTING 阶段记录 COMMIT ACK）。位图大小 = pVgList 长度，按 vgId 在 pVgList 中的下标索引。
  // 使用 void* 以支持动态大小（vgList 长度在运行时确定）
  void*   pVgAckBitmap;  // 动态分配，大小 = (taosArrayGetSize(pVgList) + 7) / 8 字节

  int64_t lastWarnTime;  // 上次打印超时警告的时间戳，避免日志刷屏
} SUserTxn;

int32_t     mndInitTxn(SMnode* pMnode);
void        mndCleanupTxn(SMnode* pMnode);
int32_t     mndStartTxnTimer(SMnode* pMnode);      // 启动超时扫描（mnode 完全启动后调用）
void        mndTxnDoTimeoutScan(SMnode* pMnode);   // 手动触发超时扫描（供定期任务调用）
const char* mndUtxnStageStr(EUtxnStage stage);
const char* mndVtxnStageStr(EVtxnStage stage);
const char* mndTxnStr(EUtxnStage stage);   // mndUtxnStageStr 的别名，供 mndTrans.c 等调用

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_USER_TXN__H_*/
