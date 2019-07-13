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

#include <arpa/inet.h>
#include <endian.h>
#include <stdbool.h>

#include "dnodeSystem.h"
#include "mgmt.h"
#include "tschemautil.h"
#include "tstatus.h"
#pragma GCC diagnostic ignored "-Wunused-variable"

SDnodeObj dnodeObj;

void mgmtSetDnodeMaxVnodes(SDnodeObj *pDnode) {
  int maxVnodes = pDnode->numOfCores * tsNumOfVnodesPerCore;
  maxVnodes = maxVnodes > TSDB_MAX_VNODES ? TSDB_MAX_VNODES : maxVnodes;
  maxVnodes = maxVnodes < TSDB_MIN_VNODES ? TSDB_MIN_VNODES : maxVnodes;
  if (tsNumOfTotalVnodes != 0) {
    maxVnodes = tsNumOfTotalVnodes;
  }
  if (pDnode->alternativeRole == TSDB_DNODE_ROLE_MGMT) {
    maxVnodes = 0;
  }

  pDnode->numOfVnodes = maxVnodes;
  pDnode->numOfFreeVnodes = maxVnodes;
  pDnode->openVnodes = 0;
}

void mgmtCalcNumOfFreeVnodes(SDnodeObj *pDnode) {
  int totalVnodes = 0;

  for (int i = 0; i < pDnode->numOfVnodes; ++i) {
    SVnodeLoad *pVload = pDnode->vload + i;
    if (pVload->vgId != 0) {
      totalVnodes++;
    }
  }

  pDnode->numOfFreeVnodes = pDnode->numOfVnodes - totalVnodes;
}

void mgmtSetDnodeVgid(int vnode, int vgId) {
  SDnodeObj *pDnode = &dnodeObj;

  SVnodeLoad *pVload = pDnode->vload + vnode;
  memset(pVload, 0, sizeof(SVnodeLoad));
  pVload->vnode = vnode;
  pVload->vgId = vgId;
  mgmtCalcNumOfFreeVnodes(pDnode);
}

void mgmtUnSetDnodeVgid(int vnode) {
  SDnodeObj *pDnode = &dnodeObj;

  SVnodeLoad *pVload = pDnode->vload + vnode;
  memset(pVload, 0, sizeof(SVnodeLoad));
  mgmtCalcNumOfFreeVnodes(pDnode);
}

int mgmtGetDnodeMeta(SMeterMeta *pMeta, SShowObj *pShow, SConnObj *pConn) {
  int cols = 0;

  SSchema *pSchema = tsGetSchema(pMeta);

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "open vnodes");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "free vnodes");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int i = 1; i < cols; ++i) pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];

  pShow->numOfRows = 1;
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  pShow->pNode = NULL;

  return 0;
}

int mgmtRetrieveDnodes(SShowObj *pShow, char *data, int rows, SConnObj *pConn) {
  int        numOfRows = 0;
  SDnodeObj *pDnode = &dnodeObj;
  char *     pWrite;
  int        cols = 0;
  char       ipstr[20];

  if (pShow->numOfReads > 0) return 0;

  pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
  *(int16_t *)pWrite = pDnode->openVnodes;
  cols++;

  pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
  *(int16_t *)pWrite = pDnode->numOfFreeVnodes;
  cols++;

  pShow->numOfReads += 1;
  return 1;
}