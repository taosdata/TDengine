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
// #include "query.h"
#include "vnodeRead.h"
#include "vnodeStatus.h"
#include "vnodeWrite.h"

char* vnodeStatus[] = {
  "init",
  "ready",
  "closing",
  "updating",
  "reset"
};

bool vnodeSetInitStatus(SVnode* pVnode) {
  pthread_mutex_lock(&pVnode->statusMutex);
  pVnode->status = TAOS_VN_STATUS_INIT;
  pthread_mutex_unlock(&pVnode->statusMutex);
  return true;
}

bool vnodeSetReadyStatus(SVnode* pVnode) {
  bool set = false;
  pthread_mutex_lock(&pVnode->statusMutex);

  if (pVnode->status == TAOS_VN_STATUS_INIT || pVnode->status == TAOS_VN_STATUS_READY ||
      pVnode->status == TAOS_VN_STATUS_UPDATING) {
    pVnode->status = TAOS_VN_STATUS_READY;
    set = true;
  }

#if 0
  qQueryMgmtReOpen(pVnode->qMgmt);
#endif   

  pthread_mutex_unlock(&pVnode->statusMutex);
  return set;
}

static bool vnodeSetClosingStatusImp(SVnode* pVnode) {
  bool set = false;
  pthread_mutex_lock(&pVnode->statusMutex);

  if (pVnode->status == TAOS_VN_STATUS_READY || pVnode->status == TAOS_VN_STATUS_INIT) {
    pVnode->status = TAOS_VN_STATUS_CLOSING;
    set = true;
  }

  pthread_mutex_unlock(&pVnode->statusMutex);
  return set;
}

bool vnodeSetClosingStatus(SVnode* pVnode) {
  if (pVnode->status == TAOS_VN_STATUS_CLOSING)
    return true;

  while (!vnodeSetClosingStatusImp(pVnode)) {
    taosMsleep(1);
  }

#if 0
  // release local resources only after cutting off outside connections
  qQueryMgmtNotifyClosed(pVnode->qMgmt);
#endif   
  vnodeWaitReadCompleted(pVnode);
  vnodeWaitWriteCompleted(pVnode);

  return true;
}

bool vnodeSetUpdatingStatus(SVnode* pVnode) {
  bool set = false;
  pthread_mutex_lock(&pVnode->statusMutex);

  if (pVnode->status == TAOS_VN_STATUS_READY) {
    pVnode->status = TAOS_VN_STATUS_UPDATING;
    set = true;
  }

  pthread_mutex_unlock(&pVnode->statusMutex);
  return set;
}

bool vnodeInInitStatus(SVnode* pVnode) {
  bool in = false;
  pthread_mutex_lock(&pVnode->statusMutex);

  if (pVnode->status == TAOS_VN_STATUS_INIT) {
    in = true;
  }

  pthread_mutex_unlock(&pVnode->statusMutex);
  return in;
}

bool vnodeInReadyStatus(SVnode* pVnode) {
  bool in = false;
  pthread_mutex_lock(&pVnode->statusMutex);

  if (pVnode->status == TAOS_VN_STATUS_READY) {
    in = true;
  }

  pthread_mutex_unlock(&pVnode->statusMutex);
  return in;
}

bool vnodeInClosingStatus(SVnode* pVnode) {
  bool in = false;
  pthread_mutex_lock(&pVnode->statusMutex);

  if (pVnode->status == TAOS_VN_STATUS_CLOSING) {
    in = true;
  }

  pthread_mutex_unlock(&pVnode->statusMutex);
  return in;
}

