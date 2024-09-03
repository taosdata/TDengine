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
#include "mndInt.h"
#include "mndStb.h"
#include "sdb.h"
#include "tconfig.h"
#include "tjson.h"
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
  static char str[24] = {0};
  (void)snprintf(str, sizeof(str), "%" PRId64, val);
  return str;
}

void dumpFunc(SSdb *pSdb, SJson *json) {
  void  *pIter = NULL;
  SJson *items = tjsonAddArrayToObject(json, "funcs");

  while (1) {
    SFuncObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_FUNC, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    (void)tjsonAddItemToArray(items, item);
    (void)tjsonAddStringToObject(item, "name", pObj->name);
    (void)tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime));
    (void)tjsonAddStringToObject(item, "funcType", i642str(pObj->funcType));
    (void)tjsonAddStringToObject(item, "scriptType", i642str(pObj->scriptType));
    (void)tjsonAddStringToObject(item, "align", i642str(pObj->align));
    (void)tjsonAddStringToObject(item, "outputType", i642str(pObj->outputType));
    (void)tjsonAddStringToObject(item, "outputLen", i642str(pObj->outputLen));
    (void)tjsonAddStringToObject(item, "bufSize", i642str(pObj->bufSize));
    (void)tjsonAddStringToObject(item, "signature", i642str(pObj->signature));
    (void)tjsonAddStringToObject(item, "commentSize", i642str(pObj->commentSize));
    (void)tjsonAddStringToObject(item, "codeSize", i642str(pObj->codeSize));
    sdbRelease(pSdb, pObj);
  }
}

void dumpDb(SSdb *pSdb, SJson *json) {
  void  *pIter = NULL;
  SJson *items = tjsonCreateObject();
  (void)tjsonAddItemToObject(json, "dbs", items);

  while (1) {
    SDbObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_DB, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    (void)tjsonAddItemToObject(items, "db", item);

    (void)tjsonAddStringToObject(item, "name", mndGetDbStr(pObj->name));
    (void)tjsonAddStringToObject(item, "acct", pObj->acct);
    (void)tjsonAddStringToObject(item, "createUser", pObj->createUser);
    (void)tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime));
    (void)tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime));
    (void)tjsonAddStringToObject(item, "uid", i642str(pObj->uid));
    (void)tjsonAddStringToObject(item, "cfgVersion", i642str(pObj->cfgVersion));
    (void)tjsonAddStringToObject(item, "vgVersion", i642str(pObj->vgVersion));
    (void)tjsonAddStringToObject(item, "numOfVgroups", i642str(pObj->cfg.numOfVgroups));
    (void)tjsonAddStringToObject(item, "numOfStables", i642str(pObj->cfg.numOfStables));
    (void)tjsonAddStringToObject(item, "buffer", i642str(pObj->cfg.buffer));
    (void)tjsonAddStringToObject(item, "pageSize", i642str(pObj->cfg.pageSize));
    (void)tjsonAddStringToObject(item, "pages", i642str(pObj->cfg.pages));
    (void)tjsonAddStringToObject(item, "cacheLastSize", i642str(pObj->cfg.cacheLastSize));
    (void)tjsonAddStringToObject(item, "daysPerFile", i642str(pObj->cfg.daysPerFile));
    (void)tjsonAddStringToObject(item, "daysToKeep0", i642str(pObj->cfg.daysToKeep0));
    (void)tjsonAddStringToObject(item, "daysToKeep1", i642str(pObj->cfg.daysToKeep1));
    (void)tjsonAddStringToObject(item, "daysToKeep2", i642str(pObj->cfg.daysToKeep2));
    (void)tjsonAddStringToObject(item, "minRows", i642str(pObj->cfg.minRows));
    (void)tjsonAddStringToObject(item, "maxRows", i642str(pObj->cfg.maxRows));
    (void)tjsonAddStringToObject(item, "precision", i642str(pObj->cfg.precision));
    (void)tjsonAddStringToObject(item, "compression", i642str(pObj->cfg.compression));
    (void)tjsonAddStringToObject(item, "encryptAlgorithm", i642str(pObj->cfg.encryptAlgorithm));
    (void)tjsonAddStringToObject(item, "replications", i642str(pObj->cfg.replications));
    (void)tjsonAddStringToObject(item, "strict", i642str(pObj->cfg.strict));
    (void)tjsonAddStringToObject(item, "cacheLast", i642str(pObj->cfg.cacheLast));
    (void)tjsonAddStringToObject(item, "hashMethod", i642str(pObj->cfg.hashMethod));
    (void)tjsonAddStringToObject(item, "hashPrefix", i642str(pObj->cfg.hashPrefix));
    (void)tjsonAddStringToObject(item, "hashSuffix", i642str(pObj->cfg.hashSuffix));
    (void)tjsonAddStringToObject(item, "sstTrigger", i642str(pObj->cfg.sstTrigger));
    (void)tjsonAddStringToObject(item, "tsdbPageSize", i642str(pObj->cfg.tsdbPageSize));
    (void)tjsonAddStringToObject(item, "schemaless", i642str(pObj->cfg.schemaless));
    (void)tjsonAddStringToObject(item, "walLevel", i642str(pObj->cfg.walLevel));
    (void)tjsonAddStringToObject(item, "walFsyncPeriod", i642str(pObj->cfg.walFsyncPeriod));
    (void)tjsonAddStringToObject(item, "walRetentionPeriod", i642str(pObj->cfg.walRetentionPeriod));
    (void)tjsonAddStringToObject(item, "walRetentionSize", i642str(pObj->cfg.walRetentionSize));
    (void)tjsonAddStringToObject(item, "walRollPeriod", i642str(pObj->cfg.walRollPeriod));
    (void)tjsonAddStringToObject(item, "walSegmentSize", i642str(pObj->cfg.walSegmentSize));

    (void)tjsonAddStringToObject(item, "numOfRetensions", i642str(pObj->cfg.numOfRetensions));
    for (int32_t i = 0; i < pObj->cfg.numOfRetensions; ++i) {
      SJson *rentensions = tjsonAddArrayToObject(item, "rentensions");
      SJson *rentension = tjsonCreateObject();
      (void)tjsonAddItemToArray(rentensions, rentension);

      SRetention *pRetension = taosArrayGet(pObj->cfg.pRetensions, i);
      (void)tjsonAddStringToObject(item, "freq", i642str(pRetension->freq));
      (void)tjsonAddStringToObject(item, "freqUnit", i642str(pRetension->freqUnit));
      (void)tjsonAddStringToObject(item, "keep", i642str(pRetension->keep));
      (void)tjsonAddStringToObject(item, "keepUnit", i642str(pRetension->keepUnit));
    }

    sdbRelease(pSdb, pObj);
  }
}

void dumpStb(SSdb *pSdb, SJson *json) {
  void  *pIter = NULL;
  SJson *items = tjsonAddArrayToObject(json, "stbs");

  while (1) {
    SStbObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_STB, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    (void)tjsonAddItemToArray(items, item);
    (void)tjsonAddStringToObject(item, "name", mndGetStbStr(pObj->name));
    (void)tjsonAddStringToObject(item, "db", mndGetDbStr(pObj->db));
    (void)tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime));
    (void)tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime));
    (void)tjsonAddStringToObject(item, "uid", i642str(pObj->uid));
    (void)tjsonAddStringToObject(item, "dbUid", i642str(pObj->dbUid));
    (void)tjsonAddStringToObject(item, "tagVer", i642str(pObj->tagVer));
    (void)tjsonAddStringToObject(item, "colVer", i642str(pObj->colVer));
    (void)tjsonAddStringToObject(item, "smaVer", i642str(pObj->smaVer));
    (void)tjsonAddStringToObject(item, "nextColId", i642str(pObj->nextColId));
    (void)tjsonAddStringToObject(item, "watermark1", i642str(pObj->watermark[0]));
    (void)tjsonAddStringToObject(item, "watermark2", i642str(pObj->watermark[1]));
    (void)tjsonAddStringToObject(item, "maxdelay0", i642str(pObj->maxdelay[0]));
    (void)tjsonAddStringToObject(item, "maxdelay1", i642str(pObj->maxdelay[1]));
    (void)tjsonAddStringToObject(item, "ttl", i642str(pObj->ttl));
    (void)tjsonAddStringToObject(item, "numOfFuncs", i642str(pObj->numOfFuncs));
    (void)tjsonAddStringToObject(item, "commentLen", i642str(pObj->commentLen));
    (void)tjsonAddStringToObject(item, "ast1Len", i642str(pObj->ast1Len));
    (void)tjsonAddStringToObject(item, "ast2Len", i642str(pObj->ast2Len));

    (void)tjsonAddStringToObject(item, "numOfColumns", i642str(pObj->numOfColumns));
    SJson *columns = tjsonAddArrayToObject(item, "columns");
    for (int32_t i = 0; i < pObj->numOfColumns; ++i) {
      SJson *column = tjsonCreateObject();
      (void)tjsonAddItemToArray(columns, column);

      SSchema *pColumn = &pObj->pColumns[i];
      (void)tjsonAddStringToObject(column, "type", i642str(pColumn->type));
      (void)tjsonAddStringToObject(column, "typestr", tDataTypes[pColumn->type].name);
      (void)tjsonAddStringToObject(column, "flags", i642str(pColumn->flags));
      (void)tjsonAddStringToObject(column, "colId", i642str(pColumn->colId));
      (void)tjsonAddStringToObject(column, "bytes", i642str(pColumn->bytes));
      (void)tjsonAddStringToObject(column, "name", pColumn->name);
    }

    (void)tjsonAddStringToObject(item, "numOfTags", i642str(pObj->numOfTags));
    SJson *tags = tjsonAddArrayToObject(item, "tags");
    for (int32_t i = 0; i < pObj->numOfTags; ++i) {
      SJson *tag = tjsonCreateObject();
      (void)tjsonAddItemToArray(tags, tag);

      SSchema *pTag = &pObj->pTags[i];
      (void)tjsonAddStringToObject(tag, "type", i642str(pTag->type));
      (void)tjsonAddStringToObject(tag, "typestr", tDataTypes[pTag->type].name);
      (void)tjsonAddStringToObject(tag, "flags", i642str(pTag->flags));
      (void)tjsonAddStringToObject(tag, "colId", i642str(pTag->colId));
      (void)tjsonAddStringToObject(tag, "bytes", i642str(pTag->bytes));
      (void)tjsonAddStringToObject(tag, "name", pTag->name);
    }

    sdbRelease(pSdb, pObj);
  }
}

void dumpSma(SSdb *pSdb, SJson *json) {
  void  *pIter = NULL;
  SJson *items = tjsonAddArrayToObject(json, "smas");

  while (1) {
    SSmaObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_SMA, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    (void)tjsonAddItemToArray(items, item);
    (void)tjsonAddStringToObject(item, "name", mndGetStbStr(pObj->name));
    (void)tjsonAddStringToObject(item, "stb", mndGetStbStr(pObj->stb));
    (void)tjsonAddStringToObject(item, "db", mndGetDbStr(pObj->db));
    (void)tjsonAddStringToObject(item, "dstTbName", mndGetStbStr(pObj->dstTbName));
    (void)tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime));
    (void)tjsonAddStringToObject(item, "uid", i642str(pObj->uid));
    (void)tjsonAddStringToObject(item, "stbUid", i642str(pObj->stbUid));
    (void)tjsonAddStringToObject(item, "dbUid", i642str(pObj->dbUid));
    (void)tjsonAddStringToObject(item, "dstTbUid", i642str(pObj->dstTbUid));
    (void)tjsonAddStringToObject(item, "intervalUnit", i642str(pObj->intervalUnit));
    (void)tjsonAddStringToObject(item, "slidingUnit", i642str(pObj->slidingUnit));
    (void)tjsonAddStringToObject(item, "timezone", i642str(pObj->timezone));
    (void)tjsonAddStringToObject(item, "dstVgId", i642str(pObj->dstVgId));
    (void)tjsonAddStringToObject(item, "interval", i642str(pObj->interval));
    (void)tjsonAddStringToObject(item, "offset", i642str(pObj->offset));
    (void)tjsonAddStringToObject(item, "sliding", i642str(pObj->sliding));
    (void)tjsonAddStringToObject(item, "exprLen", i642str(pObj->exprLen));
    (void)tjsonAddStringToObject(item, "tagsFilterLen", i642str(pObj->tagsFilterLen));
    (void)tjsonAddStringToObject(item, "sqlLen", i642str(pObj->sqlLen));
    (void)tjsonAddStringToObject(item, "astLen", i642str(pObj->astLen));
    sdbRelease(pSdb, pObj);
  }
}

void dumpVgroup(SSdb *pSdb, SJson *json) {
  void  *pIter = NULL;
  SJson *items = tjsonAddArrayToObject(json, "vgroups");

  while (1) {
    SVgObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    (void)tjsonAddItemToArray(items, item);
    (void)tjsonAddStringToObject(item, "vgId", i642str(pObj->vgId));
    (void)tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime));
    (void)tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime));
    (void)tjsonAddStringToObject(item, "version", i642str(pObj->version));
    (void)tjsonAddStringToObject(item, "hashBegin", i642str(pObj->hashBegin));
    (void)tjsonAddStringToObject(item, "hashEnd", i642str(pObj->hashEnd));
    (void)tjsonAddStringToObject(item, "db", mndGetDbStr(pObj->dbName));
    (void)tjsonAddStringToObject(item, "dbUid", i642str(pObj->dbUid));
    (void)tjsonAddStringToObject(item, "isTsma", i642str(pObj->isTsma));
    (void)tjsonAddStringToObject(item, "replica", i642str(pObj->replica));
    for (int32_t i = 0; i < pObj->replica; ++i) {
      SJson *replicas = tjsonAddArrayToObject(item, "replicas");
      SJson *replica = tjsonCreateObject();
      (void)tjsonAddItemToArray(replicas, replica);
      (void)tjsonAddStringToObject(replica, "dnodeId", i642str(pObj->vnodeGid[i].dnodeId));
    }
    sdbRelease(pSdb, pObj);
  }
}

void dumpTopic(SSdb *pSdb, SJson *json) {
  void  *pIter = NULL;
  SJson *items = tjsonAddArrayToObject(json, "topics");

  while (1) {
    SMqTopicObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_TOPIC, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    (void)tjsonAddItemToArray(items, item);
    (void)tjsonAddStringToObject(item, "name", mndGetDbStr(pObj->name));
    (void)tjsonAddStringToObject(item, "name", mndGetDbStr(pObj->db));
    (void)tjsonAddStringToObject(item, "createTime", i642str(pObj->createTime));
    (void)tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime));
    (void)tjsonAddStringToObject(item, "uid", i642str(pObj->uid));
    (void)tjsonAddStringToObject(item, "dbUid", i642str(pObj->dbUid));
    (void)tjsonAddStringToObject(item, "version", i642str(pObj->version));
    (void)tjsonAddStringToObject(item, "subType", i642str(pObj->subType));
    (void)tjsonAddStringToObject(item, "withMeta", i642str(pObj->withMeta));
    (void)tjsonAddStringToObject(item, "stbUid", i642str(pObj->stbUid));
    (void)tjsonAddStringToObject(item, "stbName", mndGetStableStr(pObj->stbName));
    (void)tjsonAddStringToObject(item, "sqlLen", i642str(pObj->sqlLen));
    (void)tjsonAddStringToObject(item, "astLen", i642str(pObj->astLen));
    (void)tjsonAddStringToObject(item, "sqlLen", i642str(pObj->sqlLen));
    (void)tjsonAddStringToObject(item, "ntbUid", i642str(pObj->ntbUid));
    (void)tjsonAddStringToObject(item, "ctbStbUid", i642str(pObj->ctbStbUid));
    sdbRelease(pSdb, pObj);
  }
}

void dumpConsumer(SSdb *pSdb, SJson *json) {
  void  *pIter = NULL;
  SJson *items = tjsonAddArrayToObject(json, "consumers");

  while (1) {
    SMqConsumerObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_CONSUMER, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    (void)tjsonAddItemToArray(items, item);
    (void)tjsonAddStringToObject(item, "consumerId", i642str(pObj->consumerId));
    (void)tjsonAddStringToObject(item, "cgroup", pObj->cgroup);
    sdbRelease(pSdb, pObj);
  }
}

void dumpSubscribe(SSdb *pSdb, SJson *json) {
  void  *pIter = NULL;
  SJson *items = tjsonAddArrayToObject(json, "subscribes");

  while (1) {
    SMqSubscribeObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_SUBSCRIBE, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    (void)tjsonAddItemToArray(items, item);
    (void)tjsonAddStringToObject(item, "key", pObj->key);
    (void)tjsonAddStringToObject(item, "dbUid", i642str(pObj->dbUid));
    (void)tjsonAddStringToObject(item, "stbUid", i642str(pObj->stbUid));
    sdbRelease(pSdb, pObj);
  }
}

void dumpStream(SSdb *pSdb, SJson *json) {
  void  *pIter = NULL;
  SJson *items = tjsonAddArrayToObject(json, "streams");

  while (1) {
    SStreamObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_STREAM, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    (void)tjsonAddItemToArray(items, item);
    (void)tjsonAddStringToObject(item, "name", mndGetDbStr(pObj->name));
    (void)tjsonAddStringToObject(item, "createTime", i642str(pObj->createTime));
    (void)tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime));
    (void)tjsonAddStringToObject(item, "version", i642str(pObj->version));
    (void)tjsonAddStringToObject(item, "totalLevel", i642str(pObj->totalLevel));
    (void)tjsonAddStringToObject(item, "smaId", i642str(pObj->smaId));
    (void)tjsonAddStringToObject(item, "uid", i642str(pObj->uid));
    (void)tjsonAddStringToObject(item, "status", i642str(pObj->status));
    (void)tjsonAddStringToObject(item, "igExpired", i642str(pObj->conf.igExpired));
    (void)tjsonAddStringToObject(item, "trigger", i642str(pObj->conf.trigger));
    (void)tjsonAddStringToObject(item, "triggerParam", i642str(pObj->conf.triggerParam));
    (void)tjsonAddStringToObject(item, "watermark", i642str(pObj->conf.watermark));
    (void)tjsonAddStringToObject(item, "sourceDbUid", i642str(pObj->sourceDbUid));
    (void)tjsonAddStringToObject(item, "targetDbUid", i642str(pObj->targetDbUid));
    (void)tjsonAddStringToObject(item, "sourceDb", mndGetDbStr(pObj->sourceDb));
    (void)tjsonAddStringToObject(item, "targetDb", mndGetDbStr(pObj->targetDb));
    (void)tjsonAddStringToObject(item, "targetSTbName", mndGetStbStr(pObj->targetSTbName));
    (void)tjsonAddStringToObject(item, "targetStbUid", i642str(pObj->targetStbUid));
    (void)tjsonAddStringToObject(item, "fixedSinkVgId", i642str(pObj->fixedSinkVgId));
    sdbRelease(pSdb, pObj);
  }
}

void dumpAcct(SSdb *pSdb, SJson *json) {
  void  *pIter = NULL;
  SJson *items = tjsonAddArrayToObject(json, "accts");

  while (1) {
    SAcctObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_ACCT, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    (void)tjsonAddItemToArray(items, item);
    (void)tjsonAddStringToObject(item, "acct", pObj->acct);
    (void)tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime));
    (void)tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime));
    (void)tjsonAddStringToObject(item, "acctId", i642str(pObj->acctId));
    sdbRelease(pSdb, pObj);
  }
}

void dumpAuth(SSdb *pSdb, SJson *json) {
  // todo
}

void dumpUser(SSdb *pSdb, SJson *json) {
  void  *pIter = NULL;
  SJson *items = tjsonAddArrayToObject(json, "users");

  while (1) {
    SUserObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_USER, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    (void)tjsonAddItemToArray(items, item);
    (void)tjsonAddStringToObject(item, "name", pObj->user);
    (void)tjsonAddStringToObject(item, "acct", pObj->acct);
    (void)tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime));
    (void)tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime));
    (void)tjsonAddStringToObject(item, "superUser", i642str(pObj->superUser));
    (void)tjsonAddStringToObject(item, "authVersion", i642str(pObj->authVersion));
    (void)tjsonAddStringToObject(item, "passVersion", i642str(pObj->passVersion));
    (void)tjsonAddStringToObject(item, "numOfReadDbs", i642str(taosHashGetSize(pObj->readDbs)));
    (void)tjsonAddStringToObject(item, "numOfWriteDbs", i642str(taosHashGetSize(pObj->writeDbs)));
    sdbRelease(pSdb, pObj);
  }
}

void dumpDnode(SSdb *pSdb, SJson *json) {
  void  *pIter = NULL;
  SJson *items = tjsonAddArrayToObject(json, "dnodes");

  while (1) {
    SDnodeObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_DNODE, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    (void)tjsonAddItemToArray(items, item);
    (void)tjsonAddStringToObject(item, "id", i642str(pObj->id));
    (void)tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime));
    (void)tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime));
    (void)tjsonAddStringToObject(item, "port", i642str(pObj->port));
    (void)tjsonAddStringToObject(item, "fqdn", pObj->fqdn);
    sdbRelease(pSdb, pObj);
  }
}

void dumpSnode(SSdb *pSdb, SJson *json) {
  void  *pIter = NULL;
  SJson *items = tjsonAddArrayToObject(json, "snodes");

  while (1) {
    SSnodeObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_QNODE, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    (void)tjsonAddItemToArray(items, item);
    (void)tjsonAddStringToObject(item, "id", i642str(pObj->id));
    (void)tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime));
    (void)tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime));
    sdbRelease(pSdb, pObj);
  }
}

void dumpQnode(SSdb *pSdb, SJson *json) {
  void  *pIter = NULL;
  SJson *items = tjsonAddArrayToObject(json, "qnodes");

  while (1) {
    SQnodeObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_QNODE, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    (void)tjsonAddItemToArray(items, item);
    (void)tjsonAddStringToObject(item, "id", i642str(pObj->id));
    (void)tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime));
    (void)tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime));
    sdbRelease(pSdb, pObj);
  }
}

void dumpMnode(SSdb *pSdb, SJson *json) {
  void  *pIter = NULL;
  SJson *items = tjsonAddArrayToObject(json, "mnodes");

  while (1) {
    SMnodeObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_MNODE, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    (void)tjsonAddItemToArray(items, item);
    (void)tjsonAddStringToObject(item, "id", i642str(pObj->id));
    (void)tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime));
    (void)tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime));
    sdbRelease(pSdb, pObj);
  }
}

void dumpCluster(SSdb *pSdb, SJson *json) {
  void  *pIter = NULL;
  SJson *items = tjsonAddArrayToObject(json, "clusters");

  while (1) {
    SClusterObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_CLUSTER, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    (void)tjsonAddItemToArray(items, item);
    (void)tjsonAddStringToObject(item, "id", i642str(pObj->id));
    (void)tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime));
    (void)tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime));
    (void)tjsonAddStringToObject(item, "name", pObj->name);
    sdbRelease(pSdb, pObj);
  }
}

void dumpTrans(SSdb *pSdb, SJson *json) {
  void  *pIter = NULL;
  SJson *items = tjsonAddArrayToObject(json, "transactions");

  while (1) {
    STrans *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_TRANS, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    (void)tjsonAddItemToArray(items, item);
    (void)tjsonAddStringToObject(item, "id", i642str(pObj->id));
    (void)tjsonAddStringToObject(item, "stage", i642str(pObj->stage));
    (void)tjsonAddStringToObject(item, "policy", i642str(pObj->policy));
    (void)tjsonAddStringToObject(item, "conflict", i642str(pObj->conflict));
    (void)tjsonAddStringToObject(item, "exec", i642str(pObj->exec));
    (void)tjsonAddStringToObject(item, "oper", i642str(pObj->oper));
    (void)tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime));
    (void)tjsonAddStringToObject(item, "dbname", pObj->dbname);
    (void)tjsonAddStringToObject(item, "stbname", pObj->stbname);
    (void)tjsonAddStringToObject(item, "opername", pObj->opername);
    (void)tjsonAddStringToObject(item, "commitLogNum", i642str(taosArrayGetSize(pObj->commitActions)));
    (void)tjsonAddStringToObject(item, "redoActionNum", i642str(taosArrayGetSize(pObj->redoActions)));
    (void)tjsonAddStringToObject(item, "undoActionNum", i642str(taosArrayGetSize(pObj->undoActions)));
    sdbRelease(pSdb, pObj);
  }
}

void dumpHeader(SSdb *pSdb, SJson *json) {
  (void)tjsonAddStringToObject(json, "sver", i642str(1));
  (void)tjsonAddStringToObject(json, "applyIndex", i642str(pSdb->applyIndex));
  (void)tjsonAddStringToObject(json, "applyTerm", i642str(pSdb->applyTerm));
  (void)tjsonAddStringToObject(json, "applyConfig", i642str(pSdb->applyConfig));

  SJson *maxIdsJson = tjsonCreateObject();
  (void)tjsonAddItemToObject(json, "maxIds", maxIdsJson);
  for (int32_t i = 0; i < SDB_MAX; ++i) {
    if(i == 5) continue;
    int64_t maxId = 0;
    if (i < SDB_MAX) {
      maxId = pSdb->maxId[i];
    }
    (void)tjsonAddStringToObject(maxIdsJson, sdbTableName(i), i642str(maxId));
  }

  SJson *tableVersJson = tjsonCreateObject();
  (void)tjsonAddItemToObject(json, "tableVers", tableVersJson);
  for (int32_t i = 0; i < SDB_MAX; ++i) {
    int64_t tableVer = 0;
    if (i < SDB_MAX) {
      tableVer = pSdb->tableVer[i];
    }
    (void)tjsonAddStringToObject(tableVersJson, sdbTableName(i), i642str(tableVer));
  }
}

void mndDumpSdb() {
  mInfo("start to dump sdb info to sdb.json");

  char path[PATH_MAX * 2] = {0};
  (void)snprintf(path, sizeof(path), "%s%smnode", tsDataDir, TD_DIRSEP);

  SMsgCb msgCb = {0};
  msgCb.reportStartupFp = reportStartup;
  msgCb.sendReqFp = sendReq;
  msgCb.sendSyncReqFp = sendSyncReq;
  msgCb.sendRspFp = sendRsp;
  msgCb.mgmt = (SMgmtWrapper *)(&msgCb);  // hack
  tmsgSetDefault(&msgCb);

  (void)walInit(NULL);
  (void)syncInit();

  SMnodeOpt opt = {.msgCb = msgCb};
  SMnode   *pMnode = mndOpen(path, &opt);
  if (pMnode == NULL) return;

  SSdb  *pSdb = pMnode->pSdb;
  SJson *json = tjsonCreateObject();
  dumpHeader(pSdb, json);
  dumpFunc(pSdb, json);
  dumpDb(pSdb, json);
  dumpStb(pSdb, json);
  dumpSma(pSdb, json);
  dumpVgroup(pSdb, json);
  dumpTopic(pSdb, json);
  dumpConsumer(pSdb, json);
  dumpSubscribe(pSdb, json);
  //  dumpOffset(pSdb, json);
  dumpStream(pSdb, json);
  dumpAcct(pSdb, json);
  dumpAuth(pSdb, json);
  dumpUser(pSdb, json);
  dumpDnode(pSdb, json);
  dumpSnode(pSdb, json);
  dumpQnode(pSdb, json);
  dumpMnode(pSdb, json);
  dumpCluster(pSdb, json);
  dumpTrans(pSdb, json);

  char     *pCont = tjsonToString(json);
  int32_t   contLen = strlen(pCont);
  char      file[] = "sdb.json";
  TdFilePtr pFile = taosOpenFile(file, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_WRITE_THROUGH);
  if (pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    mError("failed to write %s since %s", file, terrstr());
    return;
  }
  (void)taosWriteFile(pFile, pCont, contLen);
  (void)taosWriteFile(pFile, "\n", 1);
  UNUSED(taosFsyncFile(pFile));
  (void)taosCloseFile(&pFile);
  tjsonDelete(json);
  taosMemoryFree(pCont);

  mInfo("dump sdb info success");
}

void mndDeleteTrans() {
  mInfo("start to dump sdb info to sdb.json");

  char path[PATH_MAX * 2] = {0};
  (void)snprintf(path, sizeof(path), "%s%smnode", tsDataDir, TD_DIRSEP);

  SMsgCb msgCb = {0};
  msgCb.reportStartupFp = reportStartup;
  msgCb.sendReqFp = sendReq;
  msgCb.sendSyncReqFp = sendSyncReq;
  msgCb.sendRspFp = sendRsp;
  msgCb.mgmt = (SMgmtWrapper *)(&msgCb);  // hack
  tmsgSetDefault(&msgCb);

  (void)walInit(NULL);
  (void)syncInit();

  SMnodeOpt opt = {.msgCb = msgCb};
  SMnode   *pMnode = mndOpen(path, &opt);
  if (pMnode == NULL) return;

  (void)sdbWriteFileForDump(pMnode->pSdb);

  mInfo("dump sdb info success");
}

#pragma GCC diagnostic pop
