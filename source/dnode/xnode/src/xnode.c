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

#include "xndInt.h"

// clang-format off
#define xndError(...) do {  if (xndDebugFlag & DEBUG_ERROR) { taosPrintLog("XND ERROR ", DEBUG_ERROR, xndDebugFlag, __VA_ARGS__);}} while (0)
#define xndInfo(...)  do {  if (xndDebugFlag & DEBUG_INFO)  { taosPrintLog("XND INFO  ", DEBUG_INFO,  xndDebugFlag, __VA_ARGS__);}} while (0)
#define xndDebug(...) do {  if (xndDebugFlag & DEBUG_DEBUG) { taosPrintLog("XND DEBUG ", DEBUG_DEBUG, xndDebugFlag, __VA_ARGS__);}} while (0)
// clang-format on

int32_t xndOpen(const SXnodeOpt *pOption, SXnode **pXnode) {
  *pXnode = taosMemoryCalloc(1, sizeof(SXnode));
  if (NULL == *pXnode) {
    xndError("calloc SXnode failed");
    return terrno;
  }
  /*
  int32_t code = qWorkerInit(NODE_TYPE_QNODE, (*pXnode)->xndId, (void **)&(*pXnode)->pQuery, &pOption->msgCb);
  if (TSDB_CODE_SUCCESS != code) {
    taosMemoryFreeClear(pXnode);
    return code;
  }
  */
  (*pXnode)->msgCb = pOption->msgCb;

  // TODO: read config & start taosmqtt

  xndInfo("Xnode opened.");

  return TSDB_CODE_SUCCESS;
}

void xndClose(SXnode *pXnode) {
  // TODO: stop taosmqtt

  taosMemoryFree(pXnode);

  xndInfo("Xnode closed.");
}
