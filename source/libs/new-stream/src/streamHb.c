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

#include "executor.h"
#include "stream.h"
#include "streamInt.h"
#include "tdTestSeam.h"
#include "tmisce.h"
#include "tref.h"
#include "ttimer.h"
#include "tuuid.h"
#include "wal.h"

static int32_t streamHbSendRequestMsg(SStreamHbMsg* pMsg, SEpSet* pEpset) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t tlen = 0;
  SEncoder encoder = {0};
  void* buf = NULL;

  tEncodeSize(tEncodeStreamHbMsg, pMsg, tlen, code);
  TAOS_CHECK_EXIT(code);

  buf = rpcMallocCont(tlen + sizeof(SStreamMsgGrpHeader));
  TSDB_CHECK_NULL(buf, code, lino, _exit, terrno);

  ((SStreamMsgGrpHeader *)buf)->streamGid = pMsg->streamGId;
  void *abuf = POINTER_SHIFT(buf, sizeof(SStreamMsgGrpHeader));

  tEncoderInit(&encoder, abuf, tlen);
  TAOS_CHECK_EXIT(tEncodeStreamHbMsg(&encoder, pMsg));
  tEncoderClear(&encoder);

  stDebug("try to send stream hb to mnode, gid:%d, snodeId:%d, vgLeaders:%d, streamStatus:%d", 
      pMsg->streamGId, pMsg->snodeId, (int32_t)taosArrayGetSize(pMsg->pVgLeaders), (int32_t)taosArrayGetSize(pMsg->pStreamStatus));

  SRpcMsg msg = {.msgType = TDMT_MND_STREAM_HEARTBEAT, .pCont = buf, .contLen = tlen + sizeof(SStreamMsgGrpHeader)};

  buf = NULL;
  
  TAOS_CHECK_EXIT(tmsgSendReq(pEpset, &msg));

  stTrace("stream hb sent");

  return code;

_exit:

  rpcFreeCont(buf);
  tEncoderClear(&encoder);

  if (code != TSDB_CODE_APP_IS_STOPPING) {
    stError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  } else {
    stWarn("%s failed at line %d, error:%s, ignore the error", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}

static void streamHbDestroyReaderPollWindow(SReaderPollWindow* window) {
  tSimpleHashCleanup(window->polls);
  tSimpleHashCleanup(window->retired);
}

static void streamHbResetReaderState(SStreamHbInfo* hb, const bool* newRole) {
  SReaderPollWindow expired[2];
  taosWLockLatch(&hb->readerDeployLock);
  if (newRole) atomic_store_8(&gStreamMgmt.snodeEnabled, *newRole);
  hb->readerRuntime = (SStreamReaderRuntime){.id = atomic_load_8(&gStreamMgmt.snodeEnabled) ? tGenIdPI64() : 0};
  SSHashObj* installs = hb->readerInstalls;
  hb->readerInstalls = NULL;
  memcpy(expired, hb->readerPollWindows, sizeof(expired));
  memset(hb->readerPollWindows, 0, sizeof(expired));
  hb->readerPollSerial = 0;
  taosWUnLockLatch(&hb->readerDeployLock);
  for (size_t i = 0; i < 2; ++i) streamHbDestroyReaderPollWindow(&expired[i]);
  tSimpleHashCleanup(installs);
}

void streamHbResetReaderRole(SStreamHbInfo* hb, bool enabled) { streamHbResetReaderState(hb, &enabled); }

void streamHbRenewReaderRuntime(SStreamHbInfo* hb) { streamHbResetReaderState(hb, NULL); }

int32_t streamHbIssueReaderPoll(SStreamHbInfo* hb, int64_t now, SStreamHbMsg* msg) {
  // Retirements must outlive every request that could still deploy the retired instance.
  // Retain the current and previous heartbeat timeout intervals, then forget both together.
  const int64_t     period = now / STREAM_HB_ERR_HANDLE_MAX_DELAY;
  SReaderPollWindow expired[2] = {0};
  taosWLockLatch(&hb->readerDeployLock);
  msg->readerRuntime = hb->readerRuntime;
  msg->readerDeployEpoch = 0;
  msg->snodeId = hb->readerRuntime.id ? msg->dnodeId : 0;
  if (!msg->snodeId) {
    taosWUnLockLatch(&hb->readerDeployLock);
    return TSDB_CODE_SUCCESS;
  }
  int64_t token = tGenIdPI64();
  for (size_t i = 0; i < 2; ++i) {
    SReaderPollWindow* window = &hb->readerPollWindows[i];
    if (window->period + 1 < period) {
      expired[i] = *window;
      *window = (SReaderPollWindow){0};
    }
  }
  SReaderPollWindow* window = &hb->readerPollWindows[period % 2];
  window->period = period;
  if (!window->polls) window->polls = tSimpleHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  int64_t serial = hb->readerPollSerial + 1;
  int32_t code = window->polls ? tSimpleHashPut(window->polls, &token, sizeof(token), &serial, sizeof(serial)) : terrno;
  if (!code) {
    hb->readerPollSerial = serial;
    msg->readerDeployEpoch = token;
  }
  taosWUnLockLatch(&hb->readerDeployLock);
  for (size_t i = 0; i < 2; ++i) streamHbDestroyReaderPollWindow(&expired[i]);
  return code;
}

int32_t streamHbBuildRequestMsg(SStreamHbMsg* pMsg, bool* skipHb) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  pMsg->dnodeId = (*gStreamMgmt.getDnode)(gStreamMgmt.dnode);
  pMsg->runnerThreadNum = tsNumOfStreamRunnerThreads;
  pMsg->streamGId = stmAddFetchStreamGid();

  TAOS_CHECK_EXIT(stmBuildHbStreamsStatusReq(pMsg));

  if (NULL == pMsg->pStreamStatus) {
    if (0 != pMsg->streamGId || gStreamMgmt.hbReported) {
      *skipHb = true;
      
      stTrace("no stream in streamGid %d, skip hb", pMsg->streamGId);
      if (0 == pMsg->streamGId) {
        gStreamMgmt.hbReported = false;
      }
      
      return code;
    }
    // Group zero also polls for work when the entire node is idle.
    TAOS_CHECK_EXIT(streamHbIssueReaderPoll(&gStreamMgmt.hb, taosGetMonoTimestampMs(), pMsg));
  } else {
    // A role change while collecting later streams invalidates this group's snapshot.
    taosRLockLatch(&gStreamMgmt.hb.readerDeployLock);
    bool current = pMsg->readerRuntime.id == gStreamMgmt.hb.readerRuntime.id;
    taosRUnLockLatch(&gStreamMgmt.hb.readerDeployLock);
    if (!current) {
      *skipHb = true;
      return code;
    }
    gStreamMgmt.hbReported = true;
  }

  taosRLockLatch(&gStreamMgmt.vgLeadersLock);
  pMsg->pVgLeaders = taosArrayDup(gStreamMgmt.vgLeaders, NULL);
  taosRUnLockLatch(&gStreamMgmt.vgLeadersLock);
  
  return code;
  
_exit:

  stError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));

  return code;
}

void streamHbStart(void* param, void* tmrId) {
  int32_t code = 0;
  int32_t lino = 0;
  bool    skipHb = false;
  SStreamHbMsg reqMsg = {0};
  SEpSet epSet = {0};

  STREAM_TMR_CB_ENTER("stream hb");

  stTrace("stream hb begin");

  TAOS_CHECK_EXIT(streamHbBuildRequestMsg(&reqMsg, &skipHb));
  if (skipHb) {
    stTrace("stream hb skipped");
    goto _exit;
  }
  
  (*gStreamMgmt.getMnode)(gStreamMgmt.dnode, &epSet);
  TAOS_CHECK_EXIT(streamHbSendRequestMsg(&reqMsg, &epSet));

_exit:

  streamTmrStart(streamHbStart, STREAM_HB_INTERVAL_MS, NULL, gStreamMgmt.timer, &gStreamMgmt.hb.hbTmr, "stream-hb");

  tCleanupStreamHbMsg(&reqMsg, true);

  if (code && code != TSDB_CODE_APP_IS_STOPPING) {
    stError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  } else {
    stTrace("stream hb end");
  }

  STREAM_TMR_CB_LEAVE();
}

int32_t streamHbInit(SStreamHbInfo* pHb) {
  streamHbRenewReaderRuntime(pHb);
  if (tsDisableStream) {
    stInfo("stream disabled");
    return TSDB_CODE_SUCCESS;
  }

  terrno = TSDB_CODE_SUCCESS;
  streamTmrStart(streamHbStart, STREAM_HB_INTERVAL_MS, NULL, gStreamMgmt.timer, &pHb->hbTmr, "stream-hb");

  if (pHb->hbTmr == NULL) {
    return terrno != TSDB_CODE_SUCCESS ? terrno : TSDB_CODE_FAILED;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t streamHbHandleRspErr(int32_t errCode, int64_t currTs) {
  TD_SEAM_RET(streamHbHandleRspErr, int32_t (*)(int32_t, int64_t), (errCode, currTs));
  if (gStreamMgmt.hb.lastErrCode) {
    int64_t errTime = currTs - gStreamMgmt.hb.lastErrTs;
    if (errTime >= STREAM_HB_ERR_HANDLE_MAX_DELAY) {
      stError("stream hb error(current:%s, last:%s) last for %" PRId64 "ms, try to undeploy all tasks", 
          tstrerror(errCode), tstrerror(gStreamMgmt.hb.lastErrCode), errTime);
          
      smUndeployAllTasks();
    }
    
    stError("stream hb got error:%s, lastError:%s, lastErrTs:%" PRId64, tstrerror(errCode), 
        tstrerror(gStreamMgmt.hb.lastErrCode), gStreamMgmt.hb.lastErrTs);
        
    gStreamMgmt.hb.lastErrCode = errCode;

    return TSDB_CODE_SUCCESS;
  }
  
  gStreamMgmt.hb.lastErrCode = errCode;
  gStreamMgmt.hb.lastErrTs = currTs;
  stError("stream hb got error:%s, currTs:%" PRId64, tstrerror(errCode), currTs);

  return TSDB_CODE_SUCCESS;
}

// A group worker handles heartbeat replies serially, but an older reply can arrive after
// a later cleanup. Validate the trigger request before applying its reader deployments.
// Caller holds the Trigger's request latch.
static SStreamMgmtReq* streamHbPendingReaderRequest(SStreamTask* trigger, const SStreamMgmtRsp* reply) {
  SStreamMgmtReq* req = atomic_load_ptr(&trigger->pMgmtReq);
  return trigger->type == STREAM_TRIGGER_TASK && trigger->seriousId == reply->task.seriousId && req &&
                 req->type == STREAM_MGMT_REQ_TRIGGER_MIXED_ORIGTBL_READER && req->reqId == reply->reqId
             ? req
             : NULL;
}

static bool streamHbHasPendingReaderRequest(const SStreamMgmtRsp* reply) {
  SStreamTask* trigger = NULL;
  void*        ref = NULL;
  if (streamAcquireTask(reply->task.streamId, reply->task.taskId, &trigger, &ref)) return false;
  taosRLockLatch(&trigger->mgmtReqLock);
  bool current = streamHbPendingReaderRequest(trigger, reply) != NULL;
  taosRUnLockLatch(&trigger->mgmtReqLock);
  streamReleaseTask(ref);
  return current;
}

static bool streamHbLegacyReaderWaitExpired(const SStreamMgmtRsp* reply) {
  SStreamTask* trigger = NULL;
  void*        ref = NULL;
  if (streamAcquireTask(reply->task.streamId, reply->task.taskId, &trigger, &ref)) return false;
  taosWLockLatch(&trigger->mgmtReqLock);
  SStreamMgmtReq* req = streamHbPendingReaderRequest(trigger, reply);
  bool            expired = false;
  if (req) {
    int64_t now = taosGetMonoTimestampMs();
    if (!req->legacyReaderDeadline) req->legacyReaderDeadline = now + STREAM_HB_ERR_HANDLE_MAX_DELAY;
    expired = now >= req->legacyReaderDeadline;
  }
  taosWUnLockLatch(&trigger->mgmtReqLock);
  streamReleaseTask(ref);
  return expired;
}

static bool streamHbReaderUpsert(const SStreamExtOrigTblReaderRsp* ext) {
  // A PATCH with no upsert flags or binding results is a standalone cleanup.
  return !ext->code && ext->readerTaskAddr.taskId &&
         (!(ext->capabilities & STREAM_EXT_ORIGTBL_READER_CAP_PATCH) ||
          (ext->capabilities & (STREAM_EXT_ORIGTBL_READER_CAP_CREATE | STREAM_EXT_ORIGTBL_READER_CAP_START)) ||
          taosArrayGetSize(ext->results));
}

typedef struct {
  int64_t seriousId;
  bool    discarded;
} SLocalReaderInstall;

static int32_t streamHbMatchReaderDeploys(SMStreamHbRspMsg* rsp, SSHashObj** localReaders) {
  SSHashObj* stale = NULL;
  int32_t    code = 0, lino = 0;
  taosRLockLatch(&gStreamMgmt.hb.readerDeployLock);
  int64_t role = gStreamMgmt.hb.readerRuntime.id;
  taosRUnLockLatch(&gStreamMgmt.hb.readerDeployLock);
  for (int32_t i = 0; i < taosArrayGetSize(rsp->rsps.rspList); ++i) {
    SStreamMgmtRsp* reply = taosArrayGet(rsp->rsps.rspList, i);
    if (reply->header.msgType != STREAM_MSG_MIXED_ORIGTBL_READER_INFO ||
        !taosArrayGetSize(reply->cont.pExtOrigTblReaders))
      continue;
    bool current = streamHbHasPendingReaderRequest(reply);
    for (int32_t j = 0; j < taosArrayGetSize(reply->cont.pExtOrigTblReaders); ++j) {
      SStreamExtOrigTblReaderRsp* ext = taosArrayGet(reply->cont.pExtOrigTblReaders, j);
      if (!ext->readerTaskAddr.taskId) continue;
      if (current) {
        if (reply->code || ext->readerTaskAddr.nodeId != reply->task.nodeId || !streamHbReaderUpsert(ext)) continue;
        if (!*localReaders) *localReaders = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
        TSDB_CHECK_NULL(*localReaders, code, lino, _exit, terrno);
        const int64_t       key[] = {reply->task.streamId, ext->readerTaskAddr.taskId, ext->readerTaskAddr.nodeId};
        SLocalReaderInstall instance = {0};
        TAOS_CHECK_EXIT(tSimpleHashPut(*localReaders, key, sizeof(key), &instance, sizeof(instance)));
        continue;
      }
      if (!stale) stale = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
      TSDB_CHECK_NULL(stale, code, lino, _exit, terrno);
      int64_t key[] = {reply->task.streamId, ext->readerTaskAddr.taskId};
      bool    patch = (ext->capabilities & STREAM_EXT_ORIGTBL_READER_CAP_PATCH) && streamHbReaderUpsert(ext);
      bool*   previous = tSimpleHashGet(stale, key, sizeof(key));
      if (previous) {
        *previous |= patch;
        continue;
      }
      code = tSimpleHashPut(stale, key, sizeof(key), &patch, sizeof(patch));
      if (code) goto _exit;
    }
  }

  if (stale || *localReaders) {
    for (int32_t i = 0; i < taosArrayGetSize(rsp->deploy.streamList); ++i) {
      SStmStreamDeploy* stream = taosArrayGet(rsp->deploy.streamList, i);
      SArray*           readers = stream->readerTasks;
      size_t            count = taosArrayGetSize(readers), kept = 0;
      for (size_t j = 0; j < count; ++j) {
        SStmTaskDeploy* reader = taosArrayGet(readers, j);
        int64_t         key[] = {stream->streamId, reader->task.taskId};
        bool*           patch = tSimpleHashGet(stale, key, sizeof(key));
        if (*localReaders && STREAM_IS_TRIGGER_READER(reader->task.flags) &&
            STREAM_IS_REF_EXT_SOURCE(reader->task.flags)) {
          const int64_t        address[] = {stream->streamId, reader->task.taskId, reader->task.nodeId};
          SLocalReaderInstall* install = tSimpleHashGet(*localReaders, address, sizeof(address));
          if (install) {
            install->seriousId = reader->task.seriousId;
            install->discarded |= patch != NULL;
            if (!patch && !(reader->task.flags & STREAM_FLAG_REMOTE_READER_DEPLOY)) {
              // Older mnodes may omit the action flag or the poll token.
              if (rsp->readerDeployEpoch)
                reader->task.flags |= STREAM_FLAG_REMOTE_READER_DEPLOY;
              else
                reader->colocatedReaderRuntime = role ? role : -1;
            }
          }
        }
        if (STREAM_IS_REF_EXT_SOURCE(reader->task.flags) && patch) {
          if (*patch && rsp->undeploy.taskList && !code) {
            // Older mnodes PATCH a reader in place. Each discarded replacement has one
            // cleanup for the same incarnation, mixed with any independent cleanups.
            int64_t  instance[] = {stream->streamId, reader->task.taskId, reader->task.seriousId};
            int32_t* paired = tSimpleHashGet(stale, instance, sizeof(instance));
            if (paired) {
              ++*paired;
            } else {
              int32_t one = 1;
              code = tSimpleHashPut(stale, instance, sizeof(instance), &one, sizeof(one));
            }
          }
          tFreeSStmTaskDeploy(reader);
          continue;
        }
        if (kept != j) taosArraySet(readers, kept, reader);
        ++kept;
      }
      if (kept != count) taosArrayPopTailBatch(readers, count - kept);
      if (code) goto _exit;
    }
  }
  if (stale) {
    SArray* actions = rsp->undeploy.taskList;
    size_t  count = taosArrayGetSize(actions), kept = 0;
    for (size_t j = 0; j < count; ++j) {
      SStreamTaskUndeploy* action = taosArrayGet(actions, j);
      int64_t              instance[] = {action->task.streamId, action->task.taskId, action->task.seriousId};
      int32_t*             paired = tSimpleHashGet(stale, instance, sizeof(instance));
      if (paired && *paired && action->task.type == STREAM_READER_TASK &&
          STREAM_IS_REF_EXT_SOURCE(action->task.flags) && action->undeployMsg.doCleanup &&
          !action->undeployMsg.doCheckpoint) {
        --*paired;
        continue;
      }
      if (kept != j) taosArraySet(actions, kept, action);
      ++kept;
    }
    if (kept != count) taosArrayPopTailBatch(actions, count - kept);
  }
_exit:
  if (code) stError("%s failed at line %d, error:%s", __func__, lino, tstrerror(code));
  tSimpleHashCleanup(stale);
  return code;
}

// An exact duplicate is ready when its already installed instance survived. A failed open,
// a retired instance, or an older Reader at the same address leaves this request pending.
static void streamHbDeferMissingReaderReplies(SArray* replies, SSHashObj* localReaders, bool legacy) {
  size_t count = taosArrayGetSize(replies), kept = 0;
  for (size_t i = 0; i < count; ++i) {
    SStreamMgmtRsp* reply = taosArrayGet(replies, i);
    bool            ready = true;
    SStreamExtOrigTblReaderRsp* missingCreate = NULL;
    size_t          readers = !reply->code && reply->header.msgType == STREAM_MSG_MIXED_ORIGTBL_READER_INFO
                                  ? taosArrayGetSize(reply->cont.pExtOrigTblReaders)
                                  : 0;
    for (size_t j = 0; j < readers; ++j) {
      SStreamExtOrigTblReaderRsp* ext = taosArrayGet(reply->cont.pExtOrigTblReaders, j);
      if (ext->code) {
        // The Trigger must consume the schema rejection even if another Reader failed to open.
        ready = true;
        break;
      }
      if ((!ready && (!legacy || missingCreate)) || ext->readerTaskAddr.nodeId != reply->task.nodeId ||
          !streamHbReaderUpsert(ext))
        continue;
      const int64_t              key[] = {reply->task.streamId, ext->readerTaskAddr.taskId, ext->readerTaskAddr.nodeId};
      const SLocalReaderInstall* install = tSimpleHashGet(localReaders, key, sizeof(key));
      SStreamTask*               reader = NULL;
      void*                      ref = NULL;
      bool installed = (!install || !install->discarded) && !streamAcquireTask(key[0], key[1], &reader, &ref) &&
                       reader->type == STREAM_READER_TASK && STREAM_IS_TRIGGER_READER(reader->flags) &&
                       STREAM_IS_REF_EXT_SOURCE(reader->flags) && reader->nodeId == key[2] &&
                       (!install || !install->seriousId || reader->seriousId == install->seriousId) &&
                       !atomic_load_8(&reader->deployed) &&
                       (reader->status == STREAM_STATUS_INIT || reader->status == STREAM_STATUS_RUNNING);
      streamReleaseTask(ref);
      if (!installed) {
        ready = false;
        // A legacy CREATE retry returns only the registered address. Allow time for an
        // out-of-order install, then report the missing task and retain the cleanup address.
        if (legacy && !(ext->capabilities & STREAM_EXT_ORIGTBL_READER_CAP_PATCH) &&
            (!install || (!install->seriousId && !install->discarded)))
          missingCreate = ext;
      }
    }
    if (!ready && missingCreate && streamHbLegacyReaderWaitExpired(reply)) {
      missingCreate->code = TSDB_CODE_STREAM_TASK_NOT_EXIST;
      ready = true;
    }
    if (!ready) {
      tFreeSStreamMgmtRsp(reply);
      continue;
    }
    if (kept != i) taosArraySet(replies, kept, reply);
    ++kept;
  }
  if (kept != count) taosArrayPopTailBatch(replies, count - kept);
}

int32_t streamHbProcessRspMsg(SMStreamHbRspMsg* pRsp) {
  TD_SEAM_RET(streamHbProcessRspMsg, int32_t (*)(SMStreamHbRspMsg*), (pRsp));
  int32_t      code = 0;
  int32_t      lino = 0;

  SSHashObj* localReaders = NULL;

  stDebug("start to process stream hb rsp msg, gid:%d", pRsp->streamGId);

  gStreamMgmt.hb.lastErrCode = 0;
  if (pRsp->readerRuntime.id) {
    taosWLockLatch(&gStreamMgmt.hb.readerDeployLock);
    if (pRsp->readerRuntime.id == gStreamMgmt.hb.readerRuntime.id) {
      gStreamMgmt.hb.readerRuntime.challenge = pRsp->readerRuntime.challenge;
    }
    taosWUnLockLatch(&gStreamMgmt.hb.readerDeployLock);
  }

  if (pRsp->deploy.streamList && pRsp->rsps.rspList) {
    TAOS_CHECK_EXIT(streamHbMatchReaderDeploys(pRsp, &localReaders));
  }
  if (pRsp->undeploy.undeployAll) {
    //STREAMTODO
  } else if (pRsp->undeploy.taskList) {
    pRsp->undeploy.readerDeployEpoch = pRsp->readerDeployEpoch;
    TAOS_CHECK_EXIT(smUndeployTasks(&pRsp->undeploy));
  }

  if (pRsp->deploy.streamList) {
    TAOS_CHECK_EXIT(smDeployStreams(&pRsp->deploy, pRsp->readerDeployEpoch));
  }

  if (pRsp->rsps.rspList) {
    streamHbDeferMissingReaderReplies(pRsp->rsps.rspList, localReaders,
                                      !pRsp->readerDeployEpoch && !pRsp->readerRuntime.id);
    TAOS_CHECK_EXIT(smHandleMgmtRsp(&pRsp->rsps));
  }

  if (pRsp->start.taskList) {
    TAOS_CHECK_EXIT(smStartTasks(&pRsp->start));
  }

_exit:

  if (code) {
    stError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  } else {
    stDebug("end to process stream hb rsp msg");
  }

  tSimpleHashCleanup(localReaders);
  tDeepFreeSMStreamHbRspMsg(pRsp);
  
  return code;
}
