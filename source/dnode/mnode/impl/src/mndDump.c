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
#include "mndDb.h"
#include "mndDump.h"
#include "mndInt.h"
#include "mndShow.h"
#include "mndStb.h"
#include "mndSync.h"
#include "mndVgroup.h"
#include "sdb.h"
#include "tconfig.h"
#include "tjson.h"
#include "tbase64.h"
#include "ttypes.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-result"

void reportStartup(const char *name, const char *desc) {}
void sendRsp(SRpcMsg *pMsg) { rpcFreeCont(pMsg->pCont); }

int32_t sendReq(const SEpSet *pEpSet, SRpcMsg *pMsg) {
  int32_t code = 0;
  code = TSDB_CODE_INVALID_PTR;
  TAOS_RETURN(code);
}
int32_t sendSyncReq(const SEpSet *pEpSet, SRpcMsg *pMsg) {
  int32_t code = 0;
  code = TSDB_CODE_INVALID_PTR;
  TAOS_RETURN(code);
}

char *i642str(int64_t val) {
  static threadlocal char str[24] = {0};
  (void)snprintf(str, sizeof(str), "%" PRId64, val);
  return str;
}

static void dumpDnodeFields(SDnodeObj *p, SJson *fields) {
  (void)tjsonAddStringToObject(fields, "id", i642str(p->id));
  (void)tjsonAddStringToObject(fields, "createdTime", i642str(p->createdTime));
  (void)tjsonAddStringToObject(fields, "updateTime", i642str(p->updateTime));
  (void)tjsonAddStringToObject(fields, "port", i642str(p->port));
  (void)tjsonAddStringToObject(fields, "fqdn", p->fqdn);
}

static void dumpMnodeFields(SMnodeObj *p, SJson *fields) {
  (void)tjsonAddStringToObject(fields, "id", i642str(p->id));
  (void)tjsonAddStringToObject(fields, "createdTime", i642str(p->createdTime));
  (void)tjsonAddStringToObject(fields, "updateTime", i642str(p->updateTime));
  (void)tjsonAddStringToObject(fields, "role", i642str(p->role));
}

static void dumpClusterFields(SClusterObj *p, SJson *fields) {
  (void)tjsonAddStringToObject(fields, "id", i642str(p->id));
  (void)tjsonAddStringToObject(fields, "createdTime", i642str(p->createdTime));
  (void)tjsonAddStringToObject(fields, "updateTime", i642str(p->updateTime));
  (void)tjsonAddStringToObject(fields, "name", p->name);
  (void)tjsonAddStringToObject(fields, "upTime", i642str(p->upTime));
}

static void dumpVgroupFields(SVgObj *p, SJson *fields) {
  (void)tjsonAddStringToObject(fields, "vgId", i642str(p->vgId));
  (void)tjsonAddStringToObject(fields, "createdTime", i642str(p->createdTime));
  (void)tjsonAddStringToObject(fields, "updateTime", i642str(p->updateTime));
  (void)tjsonAddStringToObject(fields, "version", i642str(p->version));
  (void)tjsonAddStringToObject(fields, "hashBegin", i642str(p->hashBegin));
  (void)tjsonAddStringToObject(fields, "hashEnd", i642str(p->hashEnd));
  (void)tjsonAddStringToObject(fields, "db", p->dbName);
  (void)tjsonAddStringToObject(fields, "dbUid", i642str(p->dbUid));
  (void)tjsonAddStringToObject(fields, "isTsma", i642str(p->isTsma));
  (void)tjsonAddStringToObject(fields, "replica", i642str(p->replica));
  SJson *replicas = tjsonAddArrayToObject(fields, "replicas");
  for (int32_t i = 0; i < p->replica; ++i) {
    SJson *r = tjsonCreateObject();
    (void)tjsonAddItemToArray(replicas, r);
    (void)tjsonAddStringToObject(r, "dnodeId", i642str(p->vnodeGid[i].dnodeId));
  }
  (void)tjsonAddStringToObject(fields, "syncConfChangeVer", i642str(p->syncConfChangeVer));
}

static void dumpConfigFields(SConfigObj *p, SJson *fields) {
  (void)tjsonAddStringToObject(fields, "name", p->name);
  (void)tjsonAddStringToObject(fields, "dtype", i642str(p->dtype));
  switch (p->dtype) {
    case CFG_DTYPE_BOOL:   (void)tjsonAddStringToObject(fields, "value", i642str(p->bval)); break;
    case CFG_DTYPE_INT32:  (void)tjsonAddStringToObject(fields, "value", i642str(p->i32));  break;
    case CFG_DTYPE_INT64:  (void)tjsonAddStringToObject(fields, "value", i642str(p->i64));  break;
    case CFG_DTYPE_FLOAT:
    case CFG_DTYPE_DOUBLE: {
      char buf[32] = {0};
      (void)snprintf(buf, sizeof(buf), "%f", p->fval);
      (void)tjsonAddStringToObject(fields, "value", buf);
      break;
    }
    default: /* STRING/DIR/LOCALE/CHARSET/TIMEZONE */
      (void)tjsonAddStringToObject(fields, "value", p->str ? p->str : "");
      break;
  }
}

static void dumpCompactFields(SCompactObj *p, SJson *fields) {
  (void)tjsonAddStringToObject(fields, "compactId", i642str(p->compactId));
  (void)tjsonAddStringToObject(fields, "dbname", p->dbname);
  (void)tjsonAddStringToObject(fields, "startTime", i642str(p->startTime));
  SJson *details = tjsonAddArrayToObject(fields, "compactDetail");
  int32_t dn = (p->compactDetail != NULL) ? (int32_t)taosArrayGetSize(p->compactDetail) : 0;
  for (int32_t i = 0; i < dn; ++i) {
    SCompactDetailObj *d = taosArrayGet(p->compactDetail, i);
    SJson *dj = tjsonCreateObject();
    (void)tjsonAddItemToArray(details, dj);
    (void)tjsonAddStringToObject(dj, "compactDetailId", i642str(d->compactDetailId));
    (void)tjsonAddStringToObject(dj, "compactId", i642str(d->compactId));
    (void)tjsonAddStringToObject(dj, "vgId", i642str(d->vgId));
    (void)tjsonAddStringToObject(dj, "dnodeId", i642str(d->dnodeId));
    (void)tjsonAddStringToObject(dj, "numberFileset", i642str(d->numberFileset));
    (void)tjsonAddStringToObject(dj, "finished", i642str(d->finished));
    (void)tjsonAddStringToObject(dj, "startTime", i642str(d->startTime));
    (void)tjsonAddStringToObject(dj, "newNumberFileset", i642str(d->newNumberFileset));
    (void)tjsonAddStringToObject(dj, "newFinished", i642str(d->newFinished));
    (void)tjsonAddStringToObject(dj, "progress", i642str(d->progress));
    (void)tjsonAddStringToObject(dj, "remainingTime", i642str(d->remainingTime));
  }
}

static void dumpUserFields(SUserObj *p, SJson *fields) {
  // Scalar, human-readable view ONLY. Privileges live in an opaque encoded
  // blob inside the raw record and are preserved losslessly via rawData;
  // they are intentionally NOT reconstructed from these fields on import.
  (void)tjsonAddStringToObject(fields, "name", p->user);
  (void)tjsonAddStringToObject(fields, "acct", p->acct);
  (void)tjsonAddStringToObject(fields, "createdTime", i642str(p->createdTime));
  (void)tjsonAddStringToObject(fields, "updateTime", i642str(p->updateTime));
  (void)tjsonAddStringToObject(fields, "superUser", i642str(p->superUser));
  (void)tjsonAddStringToObject(fields, "authVersion", i642str(p->authVersion));
  (void)tjsonAddStringToObject(fields, "passVersion", i642str(p->passVersion));
}

static void dumpDbFields(SDbObj *p, SJson *fields) {
  (void)tjsonAddStringToObject(fields, "name", mndGetDbStr(p->name));
  (void)tjsonAddStringToObject(fields, "acct", p->acct);
  (void)tjsonAddStringToObject(fields, "createUser", p->createUser);
  (void)tjsonAddStringToObject(fields, "createdTime", i642str(p->createdTime));
  (void)tjsonAddStringToObject(fields, "updateTime", i642str(p->updateTime));
  (void)tjsonAddStringToObject(fields, "uid", i642str(p->uid));
  (void)tjsonAddStringToObject(fields, "cfgVersion", i642str(p->cfgVersion));
  (void)tjsonAddStringToObject(fields, "vgVersion", i642str(p->vgVersion));
  (void)tjsonAddStringToObject(fields, "numOfVgroups", i642str(p->cfg.numOfVgroups));
  (void)tjsonAddStringToObject(fields, "numOfStables", i642str(p->cfg.numOfStables));
  (void)tjsonAddStringToObject(fields, "buffer", i642str(p->cfg.buffer));
  (void)tjsonAddStringToObject(fields, "pageSize", i642str(p->cfg.pageSize));
  (void)tjsonAddStringToObject(fields, "pages", i642str(p->cfg.pages));
  (void)tjsonAddStringToObject(fields, "cacheLastSize", i642str(p->cfg.cacheLastSize));
  (void)tjsonAddStringToObject(fields, "daysPerFile", i642str(p->cfg.daysPerFile));
  (void)tjsonAddStringToObject(fields, "daysToKeep0", i642str(p->cfg.daysToKeep0));
  (void)tjsonAddStringToObject(fields, "daysToKeep1", i642str(p->cfg.daysToKeep1));
  (void)tjsonAddStringToObject(fields, "daysToKeep2", i642str(p->cfg.daysToKeep2));
  (void)tjsonAddStringToObject(fields, "minRows", i642str(p->cfg.minRows));
  (void)tjsonAddStringToObject(fields, "maxRows", i642str(p->cfg.maxRows));
  (void)tjsonAddStringToObject(fields, "precision", i642str(p->cfg.precision));
  (void)tjsonAddStringToObject(fields, "compression", i642str(p->cfg.compression));
  (void)tjsonAddStringToObject(fields, "encryptAlgorithm", i642str(p->cfg.encryptAlgorithm));
  (void)tjsonAddStringToObject(fields, "replications", i642str(p->cfg.replications));
  (void)tjsonAddStringToObject(fields, "strict", i642str(p->cfg.strict));
  (void)tjsonAddStringToObject(fields, "cacheLast", i642str(p->cfg.cacheLast));
  (void)tjsonAddStringToObject(fields, "hashMethod", i642str(p->cfg.hashMethod));
  (void)tjsonAddStringToObject(fields, "hashPrefix", i642str(p->cfg.hashPrefix));
  (void)tjsonAddStringToObject(fields, "hashSuffix", i642str(p->cfg.hashSuffix));
  (void)tjsonAddStringToObject(fields, "sstTrigger", i642str(p->cfg.sstTrigger));
  (void)tjsonAddStringToObject(fields, "tsdbPageSize", i642str(p->cfg.tsdbPageSize));
  (void)tjsonAddStringToObject(fields, "schemaless", i642str(p->cfg.schemaless));
  (void)tjsonAddStringToObject(fields, "walLevel", i642str(p->cfg.walLevel));
  (void)tjsonAddStringToObject(fields, "walFsyncPeriod", i642str(p->cfg.walFsyncPeriod));
  (void)tjsonAddStringToObject(fields, "walRetentionPeriod", i642str(p->cfg.walRetentionPeriod));
  (void)tjsonAddStringToObject(fields, "walRetentionSize", i642str(p->cfg.walRetentionSize));
  (void)tjsonAddStringToObject(fields, "walRollPeriod", i642str(p->cfg.walRollPeriod));
  (void)tjsonAddStringToObject(fields, "walSegmentSize", i642str(p->cfg.walSegmentSize));
}

static void dumpRecordFields(ESdbType type, void *pObj, SJson *fields) {
  switch (type) {
    case SDB_DNODE:   dumpDnodeFields((SDnodeObj *)pObj, fields); break;
    case SDB_MNODE:   dumpMnodeFields((SMnodeObj *)pObj, fields); break;
    case SDB_CLUSTER: dumpClusterFields((SClusterObj *)pObj, fields); break;
    case SDB_VGROUP:  dumpVgroupFields((SVgObj *)pObj, fields); break;
    case SDB_USER:    dumpUserFields((SUserObj *)pObj, fields); break;
    case SDB_DB:      dumpDbFields((SDbObj *)pObj, fields); break;
    case SDB_CFG:     dumpConfigFields((SConfigObj *)pObj, fields); break;
    case SDB_COMPACT: dumpCompactFields((SCompactObj *)pObj, fields); break;
    default: break;  // other types: rawData only
  }
}

void dumpHeader(SSdb *pSdb, SJson *json) {
  int32_t code = 0;
  int32_t lino = 0;
  TAOS_CHECK_GOTO(tjsonAddStringToObject(json, "sver", i642str(1)), &lino, _OVER);
  TAOS_CHECK_GOTO(tjsonAddStringToObject(json, "applyIndex", i642str(pSdb->applyIndex)), &lino, _OVER);
  TAOS_CHECK_GOTO(tjsonAddStringToObject(json, "applyTerm", i642str(pSdb->applyTerm)), &lino, _OVER);
  TAOS_CHECK_GOTO(tjsonAddStringToObject(json, "applyConfig", i642str(pSdb->applyConfig)), &lino, _OVER);

  SJson *maxIdsJson = tjsonCreateObject();
  TAOS_CHECK_GOTO(tjsonAddItemToObject(json, "maxIds", maxIdsJson), &lino, _OVER);
  for (int32_t i = 0; i < SDB_MAX; ++i) {
    if (i == 5) continue;
    int64_t maxId = 0;
    if (i < SDB_MAX) {
      maxId = pSdb->maxId[i];
    }
    TAOS_CHECK_GOTO(tjsonAddStringToObject(maxIdsJson, sdbTableName(i), i642str(maxId)), &lino, _OVER);
  }

  SJson *tableVersJson = tjsonCreateObject();
  TAOS_CHECK_GOTO(tjsonAddItemToObject(json, "tableVers", tableVersJson), &lino, _OVER);
  for (int32_t i = 0; i < SDB_MAX; ++i) {
    int64_t tableVer = 0;
    if (i < SDB_MAX) {
      tableVer = pSdb->tableVer[i];
    }
    TAOS_CHECK_GOTO(tjsonAddStringToObject(tableVersJson, sdbTableName(i), i642str(tableVer)), &lino, _OVER);
  }

_OVER:
  if (code != 0) mError("failed to dump sdb info at line:%d since %s", lino, tstrerror(code));
}

static SMnode *mndPrepareMnode();

int32_t mndDumpSdb() {
  mInfo("start to dump sdb info to sdb.json");

  SMnode *pMnode = mndPrepareMnode();
  if (pMnode == NULL) return terrno;

  SSdb  *pSdb = pMnode->pSdb;
  SJson *json = tjsonCreateObject();
  dumpHeader(pSdb, json);

  SJson *records = tjsonAddArrayToObject(json, "records");
  for (int32_t t = 0; t < SDB_MAX; ++t) {
    if (pSdb->hashObjs[t] == NULL || pSdb->encodeFps[t] == NULL) continue;

    void *pIter = NULL;
    while (1) {
      void      *pObj = NULL;
      ESdbStatus status;
      pIter = sdbFetchAll(pSdb, t, pIter, &pObj, &status, false);
      if (pIter == NULL) break;

      SSdbRaw *pRaw = pSdb->encodeFps[t](pObj);
      if (pRaw == NULL) {
        sdbRelease(pSdb, pObj);
        continue;
      }

      SJson *rec = tjsonCreateObject();
      (void)tjsonAddItemToArray(records, rec);
      (void)tjsonAddStringToObject(rec, "type", sdbTableName(t));
      (void)tjsonAddStringToObject(rec, "status", i642str(status));
      (void)tjsonAddStringToObject(rec, "sver", i642str(pRaw->sver));

      SJson *fields = tjsonCreateObject();
      (void)tjsonAddItemToObject(rec, "fields", fields);
      dumpRecordFields(t, pObj, fields);

      char *b64 = NULL;
      if (pRaw->dataLen > 0 && base64_encode((const uint8_t *)pRaw->pData, pRaw->dataLen, &b64) == 0) {
        (void)tjsonAddStringToObject(rec, "rawData", b64);
        taosMemoryFree(b64);
      } else {
        (void)tjsonAddStringToObject(rec, "rawData", "");
      }

      sdbFreeRaw(pRaw);
      sdbRelease(pSdb, pObj);
    }
  }

  char     *pCont = tjsonToString(json);
  int32_t   contLen = strlen(pCont);
  char      file[] = "sdb.json";
  TdFilePtr pFile = taosOpenFile(file, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_WRITE_THROUGH);
  if (pFile == NULL) {
    mError("failed to write %s since %s", file, terrstr());
    return terrno;
  }
  // taosWriteFile returns the number of bytes written (or -1), NOT a TSDB_CODE error code, so it
  // must never be passed through TAOS_CHECK_RETURN -- that treats any non-zero return (i.e. every
  // successful non-empty write) as a failure and aborts before fsync/close, silently leaving the
  // exit code as a byte-count leftover instead of 0 even though the write itself succeeded.
  if (taosWriteFile(pFile, pCont, contLen) != contLen) {
    int32_t code = terrno;
    mError("failed to write %s since %s", file, tstrerror(code));
    (void)taosCloseFile(&pFile);
    return code;
  }
  if (taosWriteFile(pFile, "\n", 1) != 1) {
    int32_t code = terrno;
    mError("failed to write %s since %s", file, tstrerror(code));
    (void)taosCloseFile(&pFile);
    return code;
  }
  TAOS_CHECK_RETURN(taosFsyncFile(pFile));
  TAOS_CHECK_RETURN(taosCloseFile(&pFile));
  tjsonDelete(json);
  taosMemoryFree(pCont);

  mInfo("dump sdb info success");
  return 0;
}

static SMnode *mndPrepareMnode() {
  int32_t code = 0;
  char    path[PATH_MAX * 2] = {0};
  (void)snprintf(path, sizeof(path), "%s%smnode", tsDataDir, TD_DIRSEP);

  SMsgCb msgCb = {0};
  msgCb.reportStartupFp = reportStartup;
  msgCb.sendReqFp = sendReq;
  msgCb.sendSyncReqFp = sendSyncReq;
  msgCb.sendRspFp = sendRsp;
  msgCb.mgmt = (SMgmtWrapper *)(&msgCb);  // hack
  tmsgSetDefault(&msgCb);

  if ((code = walInit(NULL)) != 0) {
    terrno = code;
    return NULL;
  }
  if ((code = syncInit()) != 0) {
    terrno = code;
    return NULL;
  }

  SMnodeOpt opt = {.msgCb = msgCb};
  SMnode   *pMnode = mndOpen(path, &opt);

  return pMnode;
}

// mndOpen() above only loads the last periodic sdb.data snapshot (sdbReadFile); any raft log
// entries written to the wal after that snapshot -- including ones already committed before
// taosd was killed -- are not yet reflected in pMnode->pSdb. This replays them in, the same
// way a normal online mnode restart catches up before serving traffic. Deliberately NOT folded
// into mndPrepareMnode() itself: for a single-replica mnode (the common case), becoming leader
// to drive the replay appends a new raft noop log entry to the wal as a side effect (standard
// raft "commit an entry from the current term" rule) -- harmless for mndFlushMnode(), which
// immediately snapshots+force-trims the wal right after, but for mndDumpSdb()/mndDeleteTrans()/
// mndModifySdb(), which don't, that stray entry would never get cleaned up and pile up across
// repeated invocations; a second such call was observed to fail wal log-buffer validation
// ("unexpected wal log, index:N, read request index:N+1") and crash. Those three tools are only
// meant to run after the operator has already flushed (FLUSH MNODE/-fMnode), at which point the
// wal is already trimmed and there is nothing left to replay anyway, so they stay on the plain
// mndOpen()-only path above.
static void mndReplayMnodeWal(SMnode *pMnode) {
  // Becoming leader fires the sync FSM's become-leader callback, which reaches into stream
  // management (msmHandleBecomeLeader -> streamAddVnodeLeader) and touches the gStreamMgmt
  // global. That's only allocated by the full streamInit(), which this offline one-shot
  // mini-mnode never calls -- so left enabled, this segfaults on a NULL gStreamMgmt.vgLeaders
  // (verified via gdb backtrace). The callback already gates on tsDisableStream and returns
  // immediately when set, so disable it here instead of standing up the whole (thread-spawning)
  // stream subsystem just to satisfy it; this process exits right after, so there's no need to
  // restore the previous value.
  tsDisableStream = true;

  // The sync FSM's restore-finish callback (mndRestoreFinish) normally calls mndTransPullup()
  // once replay catches up, to resume any transaction (e.g. an interrupted CREATE DATABASE)
  // still mid-flight -- by re-dispatching its pending redo actions over pMnode->msgCb.sendReqFp.
  // That's the sendReq() stub above, which unconditionally returns TSDB_CODE_INVALID_PTR. Trans
  // execution treats that as a hard failure (not one of the retry-later codes) and, per
  // TRN_POLICY_ROLLBACK, moves the transaction to TRN_STAGE_ROLLBACK -- undoing it, i.e. actively
  // deleting whatever the transaction was creating (verified: a CREATE DATABASE interrupted
  // mid-redo-action came back missing after a plain -fMnode run). Preset pMnode->restored = true
  // so mndRestoreFinish takes its "repeat call" branch and skips mndTransPullup() entirely --
  // this offline tool has no real transport to safely resume pending transactions over anyway;
  // any such transaction is left exactly as the wal committed it, to be properly resumed the
  // next time a real online mnode starts.
  mndSetRestored(pMnode, true);

  // For a single-replica mnode (the common case for -fMnode) syncNodeStart()'s "one replica
  // start" path synchronously becomes leader and replays every wal entry up to the local log's
  // tail before returning -- no wait needed. A multi-replica mnode can only catch up by reaching
  // consensus with peer nodes, which by design are expected to be stopped while -fMnode runs, so
  // there is nothing productive to wait for locally either way.
  mndSyncStart(pMnode);

  // sync is only needed to replay the wal above; stop it now so its timer threads can't race
  // with the wal writes (sdbWriteFileForDump/walBeginSnapshot/walEndSnapshot) mndFlushMnode()
  // performs right after this returns.
  mndCleanupSync(pMnode);
}

int32_t mndDeleteTrans() {
  mInfo("start to dump sdb info to sdb.json");

  SMnode *pMnode = mndPrepareMnode();
  if (pMnode == NULL) return terrno;

  TAOS_CHECK_RETURN(sdbWriteFileForDump(pMnode->pSdb, 0));

  mInfo("dump sdb info success");

  return 0;
}

int32_t mndFlushMnode() {
  mWarn("-fMnode: ensure ALL mnode instances are stopped before proceeding");

  SMnode *pMnode = mndPrepareMnode();
  if (pMnode == NULL) return terrno;
  mndReplayMnodeWal(pMnode);

  SSdb   *pSdb = pMnode->pSdb;
  int32_t code = sdbWriteFileForDump(pSdb, -1);
  if (code == 0 && pSdb->pWal != NULL) {
    code = walBeginSnapshot(pSdb->pWal, pSdb->applyIndex, 0);
    if (code == 0) code = walEndSnapshot(pSdb->pWal, true);  // forceTrim
  }

  if (code != 0) {
    mError("-fMnode failed at applyIndex:%" PRId64 " since %s", pSdb->applyIndex, tstrerror(code));
  } else {
    mInfo("-fMnode success, applyIndex:%" PRId64, pSdb->applyIndex);
  }

  // NOTE: deliberately not calling mndClose(pMnode) here, matching mndDeleteTrans()/mndModifySdb()
  // above: pMnode was created via mndPrepareMnode(), which never calls the global streamInit(), so
  // gStreamMgmt.vgLeaders etc. are left NULL. mndClose() -> mndCleanupSteps() -> mndCleanupStream()
  // -> msmHandleBecomeNotLeader() -> streamRemoveVnodeLeader() dereferences that NULL array and
  // segfaults (verified via gdb backtrace). The flush (sdb file write + WAL snapshot/trim) is
  // already fsync'd to disk above; this is a one-shot CLI process about to exit via return 0/exit
  // code, so skipping the in-memory teardown is safe and avoids the crash.
  return code;
}

static SJson *mndLoadSdbJson(char *path) {
  int32_t   code = 0;
  TdFilePtr pFile = NULL;
  char     *pData = NULL;
  SJson    *pJson = NULL;

  mInfo("start to load sdb info from sdb.json");
  pFile = taosOpenFile(path, TD_FILE_READ);
  if (pFile == NULL) {
    code = terrno;
    mError("failed to open file:%s since %s", path, tstrerror(code));
    goto _OVER;
  }

  int64_t size = 0;
  code = taosFStatFile(pFile, &size, NULL);
  if (code != 0) {
    mError("failed to fstat file:%s since %s", path, tstrerror(code));
    goto _OVER;
  }

  pData = taosMemoryMalloc(size + 1);
  if (pData == NULL) {
    code = terrno;
    goto _OVER;
  }

  if (taosReadFile(pFile, pData, size) != size) {
    code = terrno;
    mError("failed to read file:%s since %s", path, tstrerror(code));
    goto _OVER;
  }

  pData[size] = '\0';

  pJson = tjsonParse(pData);
  if (pJson == NULL) {
    code = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _OVER;
  }

  terrno = code;
  return pJson;

_OVER:
  if (pFile != NULL) taosCloseFile(&pFile);
  if (pData != NULL) taosMemoryFree(pData);
  mError("failed to load sdb json, since %s", tstrerror(code));
  terrno = code;
  return NULL;
}

ESdbType sdbTypeFromName(const char *name) {
  for (int32_t t = 0; t < SDB_MAX; ++t) {
    const char *n = sdbTableName(t);
    if (n != NULL && strcmp(n, name) == 0) return (ESdbType)t;
  }
  return SDB_MAX;  // unknown
}

static int32_t importRawData(SSdb *pSdb, ESdbType type, int8_t status, int8_t sver, SJson *rec) {
  int32_t  code = 0;
  uint8_t *pData = NULL;
  SSdbRaw *pRaw = NULL;

  SJson      *itemRaw = tjsonGetObjectItem(rec, "rawData");
  const char *b64 = (itemRaw != NULL) ? cJSON_GetStringValue(itemRaw) : NULL;
  if (b64 == NULL || b64[0] == '\0') {
    mError("import: record type:%s has empty rawData", sdbTableName(type));
    return TSDB_CODE_INVALID_JSON_FORMAT;
  }

  int32_t dataLen = 0;
  code = base64_decode(b64, (int32_t)strlen(b64), &dataLen, &pData);
  if (code != 0) return code;

  pRaw = sdbAllocRaw(type, sver, dataLen);
  if (pRaw == NULL) {
    taosMemoryFree(pData);
    return terrno;
  }

  memcpy(pRaw->pData, pData, dataLen);
  if ((code = sdbSetRawDataLen(pRaw, dataLen)) != 0) goto _over;
  if ((code = sdbSetRawStatus(pRaw, (ESdbStatus)status)) != 0) goto _over;

  code = sdbWrite(pSdb, pRaw);  // sdbWrite takes ownership of pRaw
  pRaw = NULL;

_over:
  taosMemoryFree(pData);
  if (pRaw != NULL) sdbFreeRaw(pRaw);
  return code;
}

bool isOverlayType(ESdbType t) {
  // NOTE: kept in exact 1:1 sync with applyOverlay()'s switch below -- every type here must have
  // a corresponding overlayXxx()/case, and vice versa, or records would silently decode+re-encode
  // with no overlay effect (harmless but pointless) or worse, be missing an intended overlay.
  return t == SDB_DNODE || t == SDB_CLUSTER || t == SDB_VGROUP || t == SDB_DB || t == SDB_CFG ||
         t == SDB_COMPACT;
  // SDB_USER intentionally excluded: privileges live in an opaque blob; rawData round-trip only.
  // SDB_MNODE intentionally excluded: no editable scalar fields (role is sync-managed); rawData round-trip only.
}

// helpers: only overwrite when the JSON key is present
static void ovStr(SJson *f, const char *k, char *dst, int32_t cap) {
  if (tjsonGetObjectItem(f, k) == NULL) return;
  int32_t code = tjsonGetStringValue1(f, k, dst, cap);
  if (code != 0) {
    mError("import: failed to overlay field:%s since %s", k, tstrerror(code));
  }
}
#define OV_NUM(f, k, lval)                          \
  do {                                              \
    if (tjsonGetObjectItem((f), (k)) != NULL) {     \
      int32_t _c = 0;                               \
      tjsonGetNumberValue((f), (k), (lval), _c);    \
      (void)_c;                                     \
    }                                               \
  } while (0)

void overlayDnode(SDnodeObj *p, SJson *f) {
  ovStr(f, "fqdn", p->fqdn, sizeof(p->fqdn));
  OV_NUM(f, "port", p->port);
}

void overlayVgroup(SVgObj *p, SJson *f) {
  OV_NUM(f, "replica", p->replica);
  SJson *reps = tjsonGetObjectItem(f, "replicas");
  if (reps != NULL) {
    int32_t nr = tjsonGetArraySize(reps);
    for (int32_t i = 0; i < nr && i < TSDB_MAX_REPLICA; ++i) {
      SJson *r = tjsonGetArrayItem(reps, i);
      OV_NUM(r, "dnodeId", p->vnodeGid[i].dnodeId);
    }
  }
}

void overlayCluster(SClusterObj *p, SJson *f) { ovStr(f, "name", p->name, sizeof(p->name)); }

void overlayDb(SDbObj *p, SJson *f) {
  OV_NUM(f, "replications", p->cfg.replications);
  OV_NUM(f, "buffer", p->cfg.buffer);
  OV_NUM(f, "walLevel", p->cfg.walLevel);
}

void overlayConfig(SConfigObj *p, SJson *f) {
  // dtype is fixed by the record; overwrite the value union according to it
  SJson *v = tjsonGetObjectItem(f, "value");
  if (v == NULL) return;
  const char *s = cJSON_GetStringValue(v);
  if (s == NULL) return;
  switch (p->dtype) {
    case CFG_DTYPE_BOOL:  p->bval = (bool)atoll(s); break;
    case CFG_DTYPE_INT32: p->i32 = (int32_t)atoll(s); break;
    case CFG_DTYPE_INT64: p->i64 = (int64_t)atoll(s); break;
    case CFG_DTYPE_FLOAT:
    case CFG_DTYPE_DOUBLE: p->fval = (float)atof(s); break;
    default: /* string-like: point str at a copy owned by the decoded object's arena is unsafe;
                for string configs prefer editing rawData. Skip to stay lossless. */
      break;
  }
}

void overlayCompact(SCompactObj *p, SJson *f) {
  OV_NUM(f, "compactId", p->compactId);
  OV_NUM(f, "startTime", p->startTime);
  ovStr(f, "dbname", p->dbname, sizeof(p->dbname));
  // compactDetail edits: rare; leave nested array from decoded raw untouched for losslessness.
}

static void applyOverlay(ESdbType type, void *pObj, SJson *fields) {
  switch (type) {
    case SDB_DNODE:   overlayDnode((SDnodeObj *)pObj, fields); break;
    case SDB_VGROUP:  overlayVgroup((SVgObj *)pObj, fields); break;
    case SDB_CLUSTER: overlayCluster((SClusterObj *)pObj, fields); break;
    case SDB_DB:      overlayDb((SDbObj *)pObj, fields); break;
    case SDB_CFG:     overlayConfig((SConfigObj *)pObj, fields); break;
    case SDB_COMPACT: overlayCompact((SCompactObj *)pObj, fields); break;
    default: break;
  }
}

static int32_t overlayImport(SSdb *pSdb, ESdbType type, int8_t status, int8_t sver, SJson *rec) {
  int32_t  code = 0;
  uint8_t *pData = NULL;
  SSdbRaw *pDecRaw = NULL, *pNewRaw = NULL;
  SSdbRow *pRow = NULL;

  if (pSdb->decodeFps[type] == NULL || pSdb->encodeFps[type] == NULL) {
    return importRawData(pSdb, type, status, sver, rec);  // fall back
  }

  SJson      *itemRaw = tjsonGetObjectItem(rec, "rawData");
  const char *b64 = (itemRaw != NULL) ? cJSON_GetStringValue(itemRaw) : NULL;
  if (b64 == NULL || b64[0] == '\0') {
    mError("import: record type:%s has empty rawData", sdbTableName(type));
    return TSDB_CODE_INVALID_JSON_FORMAT;
  }

  int32_t dataLen = 0;
  if ((code = base64_decode(b64, (int32_t)strlen(b64), &dataLen, &pData)) != 0) return code;

  pDecRaw = sdbAllocRaw(type, sver, dataLen);
  if (pDecRaw == NULL) {
    taosMemoryFree(pData);
    return terrno;
  }
  memcpy(pDecRaw->pData, pData, dataLen);
  if ((code = sdbSetRawDataLen(pDecRaw, dataLen)) != 0) goto _over;
  if ((code = sdbSetRawStatus(pDecRaw, (ESdbStatus)status)) != 0) goto _over;

  pRow = pSdb->decodeFps[type](pDecRaw);
  if (pRow == NULL) {
    code = terrno ? terrno : TSDB_CODE_INVALID_DATA_FMT;
    goto _over;
  }
  // decodeFps[type] alone does not set pRow->type (normally done by the sdbWriteWithoutFree
  // wrapper right after decoding, see sdbHash.c). Since we call the decode fp directly here and
  // later free pRow via sdbFreeRow(), which dispatches deleteFps[pRow->type], leaving this unset
  // would misroute to deleteFps[0] (SDB_TRANS) and crash on an unrelated object type.
  pRow->type = type;

  void *pObj = sdbGetRowObj(pRow);
  if (pObj == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _over;
  }
  SJson *fields = tjsonGetObjectItem(rec, "fields");
  if (fields != NULL) applyOverlay(type, pObj, fields);

  pNewRaw = pSdb->encodeFps[type](pObj);
  if (pNewRaw == NULL) {
    code = terrno;
    goto _over;
  }
  if ((code = sdbSetRawStatus(pNewRaw, (ESdbStatus)status)) != 0) goto _over;

  code = sdbWrite(pSdb, pNewRaw);  // takes ownership
  pNewRaw = NULL;

_over:
  taosMemoryFree(pData);
  if (pDecRaw != NULL) sdbFreeRaw(pDecRaw);
  if (pRow != NULL) sdbFreeRow(pSdb, pRow, false);
  if (pNewRaw != NULL) sdbFreeRaw(pNewRaw);
  return code;
}

static int32_t mndImportRecord(SMnode *pMnode, SJson *rec) {
  char    typeName[TSDB_TABLE_NAME_LEN] = {0};
  int32_t code = tjsonGetStringValue1(rec, "type", typeName, sizeof(typeName));
  if (code != 0) return code;

  ESdbType type = sdbTypeFromName(typeName);
  if (type == SDB_MAX) {
    mError("import: unknown record type:%s", typeName);
    return TSDB_CODE_INVALID_JSON_FORMAT;
  }

  int64_t status = 0, sver = 0;
  tjsonGetNumberValue(rec, "status", status, code);
  tjsonGetNumberValue(rec, "sver", sver, code);

  if (isOverlayType(type)) {
    return overlayImport(pMnode->pSdb, type, (int8_t)status, (int8_t)sver, rec);
  }
  return importRawData(pMnode->pSdb, type, (int8_t)status, (int8_t)sver, rec);
}

int32_t mndModifySdb(char *path) {
  int32_t code = 0;
  int32_t lino = 0;
  SJson  *pJson = NULL;

  mInfo("start to modify sdb from json:%s", path);

  SMnode *pMnode = mndPrepareMnode();
  if (pMnode == NULL) return terrno;
  SSdb *pSdb = pMnode->pSdb;

  pJson = mndLoadSdbJson(path);
  if (pJson == NULL) return terrno;

  // capture header (JSON is authoritative for cluster version state)
  int64_t applyIndex = pSdb->applyIndex, applyTerm = pSdb->applyTerm, applyConfig = pSdb->applyConfig;
  (void)tjsonGetBigIntValue(pJson, "applyIndex", &applyIndex);
  (void)tjsonGetBigIntValue(pJson, "applyTerm", &applyTerm);
  (void)tjsonGetBigIntValue(pJson, "applyConfig", &applyConfig);

  int64_t maxId[SDB_MAX];
  int64_t tableVer[SDB_MAX];
  for (int32_t t = 0; t < SDB_MAX; ++t) {
    maxId[t] = pSdb->maxId[t];
    tableVer[t] = pSdb->tableVer[t];
  }
  SJson *maxIds = tjsonGetObjectItem(pJson, "maxIds");
  SJson *tableVers = tjsonGetObjectItem(pJson, "tableVers");
  for (int32_t t = 0; t < SDB_MAX; ++t) {
    const char *n = sdbTableName(t);
    if (n == NULL) continue;
    if (maxIds != NULL) (void)tjsonGetBigIntValue(maxIds, n, &maxId[t]);
    if (tableVers != NULL) (void)tjsonGetBigIntValue(tableVers, n, &tableVer[t]);
  }

  // reset: JSON becomes the full desired state; omitted records are dropped
  sdbResetData(pSdb);

  SJson  *records = tjsonGetObjectItem(pJson, "records");
  int32_t n = tjsonGetArraySize(records);
  mInfo("import %d records from json", n);

  // Import grouped by type, in descending ESdbType order. This mirrors the
  // on-disk sdb.data record order (see sdbWriteFileImp/sdbReadFileImp, which
  // iterate types from SDB_MAX-1 down to 0), so that a row's insert callback
  // can find types it depends on (e.g. SDB_MNODE looks up its SDB_DNODE)
  // already present, regardless of the record's position in the JSON array.
  for (int32_t t = SDB_MAX - 1; t >= 0; --t) {
    const char *typeName = sdbTableName(t);
    if (typeName == NULL) continue;
    for (int32_t i = 0; i < n; ++i) {
      SJson *rec = tjsonGetArrayItem(records, i);
      char   recType[TSDB_TABLE_NAME_LEN] = {0};
      if (tjsonGetStringValue1(rec, "type", recType, sizeof(recType)) != 0) continue;
      if (strcmp(recType, typeName) != 0) continue;
      TAOS_CHECK_GOTO(mndImportRecord(pMnode, rec), &lino, _OVER);
    }
  }

  // restore header (sdbResetData zeroed these to -1/0)
  pSdb->applyIndex = applyIndex;
  pSdb->applyTerm = applyTerm;
  pSdb->applyConfig = applyConfig;
  pSdb->commitIndex = applyIndex;
  pSdb->commitTerm = applyTerm;
  pSdb->commitConfig = applyConfig;
  for (int32_t t = 0; t < SDB_MAX; ++t) {
    pSdb->maxId[t] = maxId[t];
    pSdb->tableVer[t] = tableVer[t];
  }

  mInfo("write back to sdb file");
  TAOS_CHECK_GOTO(sdbWriteFileForDump(pSdb, -1), &lino, _OVER);

  mInfo("modify sdb success");

_OVER:
  if (pJson != NULL) cJSON_Delete(pJson);
  if (code != 0) mError("failed to modify sdb, file:%s at line:%d since %s", path, lino, tstrerror(code));
  TAOS_RETURN(code);
}

#pragma GCC diagnostic pop
