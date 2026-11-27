/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

// Compile the implementation in this test-only translation unit to use its private state without duplicating layouts.
#ifndef TD_ASTRA_RPC
#include "../src/transSvr.c"

/*
 * Test-only entry point for the TD-SEC-2026-001 security regression tests.
 * Builds a minimal connection plus rpc message head and exercises the real
 * uvConnMayGetUserInfo() boundary check without a live socket.
 * Return 1 when the user info is accepted, 0 when the packet is rejected,
 * and a negative value when the test allocation fails.
 */
int32_t testUvConnMayGetUserInfo(int32_t msgLen, int8_t withUserInfo, int8_t isToken) {
  SSvrConn conn = {0};

  int32_t        allocLen = msgLen > (int32_t)sizeof(STransMsgHead) ? msgLen : (int32_t)sizeof(STransMsgHead);
  STransMsgHead* pHead = (STransMsgHead*)taosMemoryCalloc(1, allocLen);
  if (pHead == NULL) {
    return -1;
  }

  pHead->version = TRANS_VER;
  pHead->withUserInfo = withUserInfo;
  pHead->isToken = isToken;
  pHead->msgLen = (int32_t)htonl((uint32_t)msgLen);

  int32_t len = msgLen;
  bool    ret = uvConnMayGetUserInfo(&conn, &pHead, &len);
  taosMemoryFree(pHead);
  return ret ? 1 : 0;
}
#endif  // !TD_ASTRA_RPC
