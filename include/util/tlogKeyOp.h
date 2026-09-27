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

#ifndef _TD_UTIL_TLOG_KEYOP_H_
#define _TD_UTIL_TLOG_KEYOP_H_

#ifdef __cplusplus
extern "C" {
#endif

// Marks a log line that belongs to a "key operation" - an operation that is either
// high-impact on overall cluster performance or high-risk (data migration, mnode
// transactions/leader election/rebalance, stream processing, subscription, virtual
// table DDL, etc). The literal "[KEYOP]" token lets operators grep every such event
// across every module's log lines (regardless of module tag or debug flag), then
// narrow down with "op=<name>" and/or "phase=<phase>".
//
// Compile-time variant: op / phase must be string literals (relies on adjacent
// string-literal concatenation, so it costs nothing at runtime). Place it as the
// first characters of the fmt argument at any xInfo/xWarn/xError/xDebug call site:
//   mInfo(KEYOP("split-vgroup", "START") "vgId:%d, start to split", vgId);
#define KEYOP(op, phase) "[KEYOP] op=" op " phase=" phase " "

// Runtime variant: op (and/or phase) is only known at runtime, e.g. pTrans->opername.
// Usage: mInfo(KEYOP_TAG_FMT "trans:%d, ...", opNameString, phaseString, transId, ...);
#define KEYOP_TAG_FMT "[KEYOP] op=%s phase=%s "

// Canonical phase values used with KEYOP()/KEYOP_TAG_FMT. Keep these stable - they
// are documented in docs/unplanned/engine/ (key-operation log search guide) for operators to grep.
#define KEYOP_PHASE_START  "START"
#define KEYOP_PHASE_STEP   "STEP"
#define KEYOP_PHASE_END    "END"
#define KEYOP_PHASE_FAIL   "FAIL"
#define KEYOP_PHASE_KILLED "KILLED"

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_TLOG_KEYOP_H_*/
