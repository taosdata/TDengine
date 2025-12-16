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

#ifndef _TD_XNODE_H_
#define _TD_XNODE_H_

/** cflags: -L../../common/ */
#include "common/tmsgcb.h"
#include "tglobal.h"

#ifdef __cplusplus
extern "C" {
#endif

// clang-format off

#define xndFatal(...) do {  if (xndDebugFlag & DEBUG_FATAL) { taosPrintLog("xnd FATAL ", DEBUG_FATAL, xndDebugFlag, __VA_ARGS__);}} while (0)
#define xndError(...) do {  if (xndDebugFlag & DEBUG_ERROR) { taosPrintLog("xnd ERROR ", DEBUG_ERROR, xndDebugFlag, __VA_ARGS__);}} while (0)
#define xndWarn(...)  do {  if (xndDebugFlag & DEBUG_WARN)  { taosPrintLog("xnd WARN  ", DEBUG_WARN,  xndDebugFlag, __VA_ARGS__);}} while (0)
#define xndInfo(...)  do {  if (xndDebugFlag & DEBUG_INFO)  { taosPrintLog("xnd INFO  ", DEBUG_INFO,  xndDebugFlag, __VA_ARGS__);}} while (0)
#define xndDebug(...) do {  if (xndDebugFlag & DEBUG_DEBUG) { taosPrintLog("xnd DEBUG ", DEBUG_DEBUG, xndDebugFlag, __VA_ARGS__);}} while (0)
#define xndTrace(...) do {  if (xndDebugFlag & DEBUG_TRACE) { taosPrintLog("xnd TRACE ", DEBUG_TRACE, xndDebugFlag, __VA_ARGS__);}} while (0)

// clang-format on

/* ------------------------ TYPES EXPOSED ------------------------ */
#ifndef XNODE_USER_PASS_LEN
#define XNODE_USER_PASS_LEN (TSDB_USER_LEN + TSDB_USET_PASSWORD_LONGLEN + 16)
#endif

typedef struct SXnode SXnode;

typedef struct {
  SMsgCb  msgCb;
  int32_t dnodeId;
  int64_t clusterId;
  int32_t proto;
  int32_t upLen;
  char    userPass[XNODE_USER_PASS_LEN];
  SEp     ep;
} SXnodeOpt;

struct SXnode {
  SMsgCb  msgCb;
  int8_t  protocol;
  int32_t dnodeId;
  int64_t clusterId;
  int32_t proto;
  int32_t upLen;
  char    userPass[XNODE_USER_PASS_LEN];
  SEp     ep;
};

/* ------------------------ SXnode ------------------------ */
/**
 * @brief Start one Xnode in Dnode.
 *
 * @param pOption Option of the qnode.
 * @param pXnode The qnode object.
 * @return int32_t The error code.
 */
int32_t xndOpen(const SXnodeOpt *pOption, SXnode **pXnode);

/**
 * @brief Stop Xnode in Dnode.
 *
 * @param pXnode The qnode object to close.
 */
void xndClose(SXnode *pXnode);

#ifdef __cplusplus
}
#endif

int32_t mndOpenXnd(const SXnodeOpt *pOption);
void mndCloseXnd();

#endif /*_TD_BNODE_H_*/
