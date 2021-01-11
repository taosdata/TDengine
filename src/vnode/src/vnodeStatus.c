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
#include "os.h"
#include "taosmsg.h"
#include "query.h"
#include "vnodeStatus.h"
#include "vnodeRead.h"
#include "vnodeWrite.h"

char* vnodeStatus[] = {
  "init",
  "ready",
  "closing",
  "updating",
  "reset"
};

bool vnodeSetInitStatus(SVnodeObj* pVnode) {
  pthread_mutex_lock(&pVnode->statusMutex);
  pVnode->status = TAOS_VN_STATUS_INIT;
  pthread_mutex_unlock(&pVnode->statusMutex);
  return true;
}

bool vnodeSetReadyStatus(SVnodeObj* pVnode) {
  bool set = false;
  pthread_mutex_lock(&pVnode->statusMutex);

  if (pVnode->status == TAOS_VN_STATUS_INIT || pVnode->status == TAOS_VN_STATUS_READY ||
      pVnode->status == TAOS_VN_STATUS_UPDATING || pVnode->status == TAOS_VN_STATUS_RESET) {
    pVnode->status = TAOS_VN_STATUS_READY;
    set = true;
  }

  qQueryMgmtReOpen(pVnode->qMgmt);

  pthread_mutex_unlock(&pVnode->statusMutex);
  return set;
}

static bool vnodeSetClosingStatusImp(SVnodeObj* pVnode) {
  bool set = false;
  pthread_mutex_lock(&pVnode->statusMutex);

  if (pVnode->status == TAOS_VN_STATUS_READY || pVnode->status == TAOS_VN_STATUS_INIT) {
    pVnode->status = TAOS_VN_STATUS_CLOSING;
    set = true;
  }

  pthread_mutex_unlock(&pVnode->statusMutex);
  return set;
}

bool vnodeSetClosingStatus(SVnodeObj* pVnode) {
  while (!vnodeSetClosingStatusImp(pVnode)) {
    taosMsleep(1);
  }

  // release local resources only after cutting off outside connections
  qQueryMgmtNotifyClosed(pVnode->qMgmt);
  vnodeWaitReadCompleted(pVnode);
  vnodeWaitWriteCompleted(pVnode);

  return true;
}

bool vnodeSetUpdatingStatus(SVnodeObj* pVnode) {
  bool set = false;
  pthread_mutex_lock(&pVnode->statusMutex);

  if (pVnode->status == TAOS_VN_STATUS_READY) {
    pVnode->status = TAOS_VN_STATUS_UPDATING;
    set = true;
  }

  pthread_mutex_unlock(&pVnode->statusMutex);
  return set;
}

static bool vnodeSetResetStatusImp(SVnodeObj* pVnode) {
  bool set = false;
  pthread_mutex_lock(&pVnode->statusMutex);

  if (pVnode->status == TAOS_VN_STATUS_READY || pVnode->status == TAOS_VN_STATUS_INIT) {
    pVnode->status = TAOS_VN_STATUS_RESET;
    set = true;
  }

  pthread_mutex_unlock(&pVnode->statusMutex);
  return set;
}

bool vnodeSetResetStatus(SVnodeObj* pVnode) {
  while (!vnodeSetResetStatusImp(pVnode)) {
    taosMsleep(1);
  }

  // release local resources only after cutting off outside connections
  qQueryMgmtNotifyClosed(pVnode->qMgmt);
  vnodeWaitReadCompleted(pVnode);
  vnodeWaitWriteCompleted(pVnode);

  return true;
}

bool vnodeInInitStatus(SVnodeObj* pVnode) {
  bool in = false;
  pthread_mutex_lock(&pVnode->statusMutex);

  if (pVnode->status == TAOS_VN_STATUS_INIT) {
    in = true;
  }

  pthread_mutex_unlock(&pVnode->statusMutex);
  return in;
}

bool vnodeInReadyStatus(SVnodeObj* pVnode) {
  bool in = false;
  pthread_mutex_lock(&pVnode->statusMutex);

  if (pVnode->status == TAOS_VN_STATUS_READY) {
    in = true;
  }

  pthread_mutex_unlock(&pVnode->statusMutex);
  return in;
}

bool vnodeInReadyOrUpdatingStatus(SVnodeObj* pVnode) {
  bool in = false;
  pthread_mutex_lock(&pVnode->statusMutex);

  if (pVnode->status == TAOS_VN_STATUS_READY || pVnode->status == TAOS_VN_STATUS_UPDATING) {
    in = true;
  }

  pthread_mutex_unlock(&pVnode->statusMutex);
  return in;
}

bool vnodeInResetStatus(SVnodeObj* pVnode) {
  bool in = false;
  pthread_mutex_lock(&pVnode->statusMutex);

  if (pVnode->status == TAOS_VN_STATUS_RESET) {
    in = true;
  }

  pthread_mutex_unlock(&pVnode->statusMutex);
  return in;
}
