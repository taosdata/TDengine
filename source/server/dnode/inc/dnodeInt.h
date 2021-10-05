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

#ifndef _TD_DNODE_INT_H_
#define _TD_DNODE_INT_H_

#ifdef __cplusplus
extern "C" {
#endif
#include "taoserror.h"
#include "taosmsg.h"
#include "tglobal.h"
#include "tlog.h"
#include "trpc.h"
#include "tstep.h"
#include "dnode.h"

struct DnCfg;
struct DnCheck;
struct DnEps;
struct DnMnEps;
struct DnStatus;
struct DnTelem;
struct DnTrans;
struct DnMain;
struct Mnode;
struct Vnode;

typedef struct Dnode {
  struct SSteps *  steps;
  struct DnCfg *   cfg;
  struct DnCheck * check;
  struct DnEps *   eps;
  struct DnMnEps * meps;
  struct DnStatus *status;
  struct DnTelem * telem;
  struct DnTrans * trans;
  struct DnMain *  main;
  struct Mnode *   mnode;
  struct Vnode *   vnode;
} Dnode;

Dnode* dnodeInst();

#define dFatal(...) { if (dDebugFlag & DEBUG_FATAL) { taosPrintLog("DND FATAL ", 255, __VA_ARGS__); }}
#define dError(...) { if (dDebugFlag & DEBUG_ERROR) { taosPrintLog("DND ERROR ", 255, __VA_ARGS__); }}
#define dWarn(...)  { if (dDebugFlag & DEBUG_WARN)  { taosPrintLog("DND WARN ", 255, __VA_ARGS__); }}
#define dInfo(...)  { if (dDebugFlag & DEBUG_INFO)  { taosPrintLog("DND ", 255, __VA_ARGS__); }}
#define dDebug(...) { if (dDebugFlag & DEBUG_DEBUG) { taosPrintLog("DND ", dDebugFlag, __VA_ARGS__); }}
#define dTrace(...) { if (dDebugFlag & DEBUG_TRACE) { taosPrintLog("DND ", dDebugFlag, __VA_ARGS__); }}

#ifdef __cplusplus
}
#endif

#endif /*_TD_DNODE_INT_H_*/