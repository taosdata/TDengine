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

#define _DEFAULT_SOURCE
#include "dmMgmt.h"
#include "dmNodes.h"
#include "audit.h"
#include "metrics.h"

static void collectDnodeMetricsInfo(SDnode *pDnode);
static void collectWriteMetricsInfo(SDnode *pDnode);
static void collectQueryMetricsInfo(SDnode *pDnode);
static void collectStreamMetricsInfo(SDnode *pDnode);

void dmSendMetricsReport() {
  SDnode *pDnode = dmInstance();
  if (pDnode == NULL) {
    return;
  }

  collectDnodeMetricsInfo(pDnode);
  collectWriteMetricsInfo(pDnode);
  collectQueryMetricsInfo(pDnode);
  collectStreamMetricsInfo(pDnode);
}

static void collectDnodeMetricsInfo(SDnode *pDnode) {
  // TODO: collect dnode metrics info
  return;
}

static void collectWriteMetricsInfo(SDnode *pDnode) { return; }

static void collectQueryMetricsInfo(SDnode *pDnode) {
  // TODO: collect query metrics info
  return;
}

static void collectStreamMetricsInfo(SDnode *pDnode) {
  // TODO: collect stream metrics info
  return;
}
