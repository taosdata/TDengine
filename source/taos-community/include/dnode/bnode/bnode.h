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

#ifndef _TD_BNODE_H_
#define _TD_BNODE_H_

#include "tmsgcb.h"

#ifdef __cplusplus
extern "C" {
#endif

// clang-format off

#define bndFatal(...) do {  if (bndDebugFlag & DEBUG_FATAL) { taosPrintLog("BND FATAL ", DEBUG_FATAL, bndDebugFlag, __VA_ARGS__);}} while (0)
#define bndError(...) do {  if (bndDebugFlag & DEBUG_ERROR) { taosPrintLog("BND ERROR ", DEBUG_ERROR, bndDebugFlag, __VA_ARGS__);}} while (0)
#define bndWarn(...)  do {  if (bndDebugFlag & DEBUG_WARN)  { taosPrintLog("BND WARN  ", DEBUG_WARN,  bndDebugFlag, __VA_ARGS__);}} while (0)
#define bndInfo(...)  do {  if (bndDebugFlag & DEBUG_INFO)  { taosPrintLog("BND INFO  ", DEBUG_INFO,  bndDebugFlag, __VA_ARGS__);}} while (0)
#define bndDebug(...) do {  if (bndDebugFlag & DEBUG_DEBUG) { taosPrintLog("BND DEBUG ", DEBUG_DEBUG, bndDebugFlag, __VA_ARGS__);}} while (0)
#define bndTrace(...) do {  if (bndDebugFlag & DEBUG_TRACE) { taosPrintLog("BND TRACE ", DEBUG_TRACE, bndDebugFlag, __VA_ARGS__);}} while (0)

// clang-format on

/* ------------------------ TYPES EXPOSED ------------------------ */
typedef struct SBnode SBnode;

typedef struct {
  SMsgCb  msgCb;
  int32_t dnodeId;
  int32_t proto;
} SBnodeOpt;

/* ------------------------ SBnode ------------------------ */
/**
 * @brief Start one Bnode in Dnode.
 *
 * @param pOption Option of the qnode.
 * @param pBnode The qnode object.
 * @return int32_t The error code.
 */
int32_t bndOpen(const SBnodeOpt *pOption, SBnode **pBnode);

/**
 * @brief Stop Bnode in Dnode.
 *
 * @param pBnode The qnode object to close.
 */
void bndClose(SBnode *pBnode);

#ifdef __cplusplus
}
#endif

#endif /*_TD_BNODE_H_*/
