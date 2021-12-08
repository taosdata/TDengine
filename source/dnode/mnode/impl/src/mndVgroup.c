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
#include "mndVgroup.h"
#include "mndDnode.h"
#include "mndShow.h"
#include "mndTrans.h"
#include "ttime.h"

static char *syncRole[] = {"unsynced", "slave", "master"};

static int32_t mndGetVnodeMeta(SMnodeMsg *pMsg, SShowObj *pShow, STableMetaMsg *pMeta);
static int32_t mndRetrieveVnodes(SMnodeMsg *pMsg, SShowObj *pShow, char *data, int32_t rows);
static void    mndCancelGetNextVnode(SMnode *pMnode, void *pIter);

int32_t mndInitVgroup(SMnode *pMnode) {
  mndAddShowMetaHandle(pMnode, TSDB_MGMT_TABLE_VNODES, mndGetVnodeMeta);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_VNODES, mndRetrieveVnodes);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_VNODES, mndCancelGetNextVnode);

  return 0;
}

void mndCleanupVgroup(SMnode *pMnode) {}

int32_t mndGetVnodesNum(SMnode *pMnode, int32_t dnodeId) {
  if (dnodeId == 0) {
    return 0;
  }

  return 0;
}

static int32_t mndGetVnodeMeta(SMnodeMsg *pMsg, SShowObj *pShow, STableMetaMsg *pMeta) {
  SMnode *pMnode = pMsg->pMnode;
  SSdb   *pSdb = pMnode->pSdb;

  int32_t  cols = 0;
  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "vgId");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 12 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "status");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  int32_t dnodeId = 0;
  if (pShow->payloadLen > 0) {
    dnodeId = atoi(pShow->payload);
  }

  pShow->replica = dnodeId;
  pShow->numOfRows = mndGetVnodesNum(pMnode, dnodeId);
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  strcpy(pMeta->tableFname, mndShowStr(pShow->type));

  return 0;
}

static int32_t mndRetrieveVnodes(SMnodeMsg *pMsg, SShowObj *pShow, char *data, int32_t rows) {
  SMnode *pMnode = pMsg->pMnode;
  SSdb   *pSdb = pMnode->pSdb;
  int32_t numOfRows = 0;
  SVgObj *pVgroup = NULL;
  char   *pWrite;
  int32_t cols = 0;
  int32_t dnodeId = pShow->replica;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_VGROUP, pShow->pIter, (void **)&pVgroup);
    if (pShow->pIter == NULL) break;

    for (int32_t i = 0; i < pVgroup->numOfVnodes && numOfRows < rows; ++i) {
      SVnodeGid *pVgid = &pVgroup->vnodeGid[i];
      if (pVgid->dnodeId != dnodeId) continue;

      cols = 0;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(uint32_t *)pWrite = pVgroup->vgId;
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      STR_TO_VARSTR(pWrite, syncRole[pVgid->role]);
      cols++;
      numOfRows++;
    }

    sdbRelease(pSdb, pVgroup);
  }

  mnodeVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);
  pShow->numOfReads += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextVnode(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}