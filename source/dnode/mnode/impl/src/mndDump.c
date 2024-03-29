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
  terrno = TSDB_CODE_INVALID_PTR;
  return -1;
}
int32_t sendSyncReq(const SEpSet *pEpSet, SRpcMsg *pMsg) {
  terrno = TSDB_CODE_INVALID_PTR;
  return -1;
}

char *i642str(int64_t val) {
  static char str[24] = {0};
  snprintf(str, sizeof(str), "%" PRId64, val);
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
    tjsonAddItemToArray(items, item);
    tjsonAddStringToObject(item, "name", pObj->name);
    tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime));
    tjsonAddStringToObject(item, "funcType", i642str(pObj->funcType));
    tjsonAddStringToObject(item, "scriptType", i642str(pObj->scriptType));
    tjsonAddStringToObject(item, "align", i642str(pObj->align));
    tjsonAddStringToObject(item, "outputType", i642str(pObj->outputType));
    tjsonAddStringToObject(item, "outputLen", i642str(pObj->outputLen));
    tjsonAddStringToObject(item, "bufSize", i642str(pObj->bufSize));
    tjsonAddStringToObject(item, "signature", i642str(pObj->signature));
    tjsonAddStringToObject(item, "commentSize", i642str(pObj->commentSize));
    tjsonAddStringToObject(item, "codeSize", i642str(pObj->codeSize));
    sdbRelease(pSdb, pObj);
  }
}

void dumpDb(SSdb *pSdb, SJson *json) {
  void  *pIter = NULL;
  SJson *items = tjsonCreateObject();
  tjsonAddItemToObject(json, "dbs", items);

  while (1) {
    SDbObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_DB, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    tjsonAddItemToObject(items, "db", item);

    tjsonAddStringToObject(item, "name", mndGetDbStr(pObj->name));
    tjsonAddStringToObject(item, "acct", pObj->acct);
    tjsonAddStringToObject(item, "createUser", pObj->createUser);
    tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime));
    tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime));
    tjsonAddStringToObject(item, "uid", i642str(pObj->uid));
    tjsonAddStringToObject(item, "cfgVersion", i642str(pObj->cfgVersion));
    tjsonAddStringToObject(item, "vgVersion", i642str(pObj->vgVersion));
    tjsonAddStringToObject(item, "numOfVgroups", i642str(pObj->cfg.numOfVgroups));
    tjsonAddStringToObject(item, "numOfStables", i642str(pObj->cfg.numOfStables));
    tjsonAddStringToObject(item, "buffer", i642str(pObj->cfg.buffer));
    tjsonAddStringToObject(item, "pageSize", i642str(pObj->cfg.pageSize));
    tjsonAddStringToObject(item, "pages", i642str(pObj->cfg.pages));
    tjsonAddStringToObject(item, "cacheLastSize", i642str(pObj->cfg.cacheLastSize));
    tjsonAddStringToObject(item, "daysPerFile", i642str(pObj->cfg.daysPerFile));
    tjsonAddStringToObject(item, "daysToKeep0", i642str(pObj->cfg.daysToKeep0));
    tjsonAddStringToObject(item, "daysToKeep1", i642str(pObj->cfg.daysToKeep1));
    tjsonAddStringToObject(item, "daysToKeep2", i642str(pObj->cfg.daysToKeep2));
    tjsonAddStringToObject(item, "minRows", i642str(pObj->cfg.minRows));
    tjsonAddStringToObject(item, "maxRows", i642str(pObj->cfg.maxRows));
    tjsonAddStringToObject(item, "precision", i642str(pObj->cfg.precision));
    tjsonAddStringToObject(item, "compression", i642str(pObj->cfg.compression));
    tjsonAddStringToObject(item, "replications", i642str(pObj->cfg.replications));
    tjsonAddStringToObject(item, "strict", i642str(pObj->cfg.strict));
    tjsonAddStringToObject(item, "cacheLast", i642str(pObj->cfg.cacheLast));
    tjsonAddStringToObject(item, "hashMethod", i642str(pObj->cfg.hashMethod));
    tjsonAddStringToObject(item, "hashPrefix", i642str(pObj->cfg.hashPrefix));
    tjsonAddStringToObject(item, "hashSuffix", i642str(pObj->cfg.hashSuffix));
    tjsonAddStringToObject(item, "sstTrigger", i642str(pObj->cfg.sstTrigger));
    tjsonAddStringToObject(item, "tsdbPageSize", i642str(pObj->cfg.tsdbPageSize));
    tjsonAddStringToObject(item, "schemaless", i642str(pObj->cfg.schemaless));
    tjsonAddStringToObject(item, "walLevel", i642str(pObj->cfg.walLevel));
    tjsonAddStringToObject(item, "walFsyncPeriod", i642str(pObj->cfg.walFsyncPeriod));
    tjsonAddStringToObject(item, "walRetentionPeriod", i642str(pObj->cfg.walRetentionPeriod));
    tjsonAddStringToObject(item, "walRetentionSize", i642str(pObj->cfg.walRetentionSize));
    tjsonAddStringToObject(item, "walRollPeriod", i642str(pObj->cfg.walRollPeriod));
    tjsonAddStringToObject(item, "walSegmentSize", i642str(pObj->cfg.walSegmentSize));

    tjsonAddStringToObject(item, "numOfRetensions", i642str(pObj->cfg.numOfRetensions));
    for (int32_t i = 0; i < pObj->cfg.numOfRetensions; ++i) {
      SJson *rentensions = tjsonAddArrayToObject(item, "rentensions");
      SJson *rentension = tjsonCreateObject();
      tjsonAddItemToArray(rentensions, rentension);

      SRetention *pRetension = taosArrayGet(pObj->cfg.pRetensions, i);
      tjsonAddStringToObject(item, "freq", i642str(pRetension->freq));
      tjsonAddStringToObject(item, "freqUnit", i642str(pRetension->freqUnit));
      tjsonAddStringToObject(item, "keep", i642str(pRetension->keep));
      tjsonAddStringToObject(item, "keepUnit", i642str(pRetension->keepUnit));
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
    tjsonAddItemToArray(items, item);
    tjsonAddStringToObject(item, "name", mndGetStbStr(pObj->name));
    tjsonAddStringToObject(item, "db", mndGetDbStr(pObj->db));
    tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime));
    tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime));
    tjsonAddStringToObject(item, "uid", i642str(pObj->uid));
    tjsonAddStringToObject(item, "dbUid", i642str(pObj->dbUid));
    tjsonAddStringToObject(item, "tagVer", i642str(pObj->tagVer));
    tjsonAddStringToObject(item, "colVer", i642str(pObj->colVer));
    tjsonAddStringToObject(item, "smaVer", i642str(pObj->smaVer));
    tjsonAddStringToObject(item, "nextColId", i642str(pObj->nextColId));
    tjsonAddStringToObject(item, "watermark1", i642str(pObj->watermark[0]));
    tjsonAddStringToObject(item, "watermark2", i642str(pObj->watermark[1]));
    tjsonAddStringToObject(item, "maxdelay0", i642str(pObj->maxdelay[0]));
    tjsonAddStringToObject(item, "maxdelay1", i642str(pObj->maxdelay[1]));
    tjsonAddStringToObject(item, "ttl", i642str(pObj->ttl));
    tjsonAddStringToObject(item, "numOfFuncs", i642str(pObj->numOfFuncs));
    tjsonAddStringToObject(item, "commentLen", i642str(pObj->commentLen));
    tjsonAddStringToObject(item, "ast1Len", i642str(pObj->ast1Len));
    tjsonAddStringToObject(item, "ast2Len", i642str(pObj->ast2Len));

    tjsonAddStringToObject(item, "numOfColumns", i642str(pObj->numOfColumns));
    SJson *columns = tjsonAddArrayToObject(item, "columns");
    for (int32_t i = 0; i < pObj->numOfColumns; ++i) {
      SJson *column = tjsonCreateObject();
      tjsonAddItemToArray(columns, column);

      SSchema *pColumn = &pObj->pColumns[i];
      tjsonAddStringToObject(column, "type", i642str(pColumn->type));
      tjsonAddStringToObject(column, "typestr", tDataTypes[pColumn->type].name);
      tjsonAddStringToObject(column, "flags", i642str(pColumn->flags));
      tjsonAddStringToObject(column, "colId", i642str(pColumn->colId));
      tjsonAddStringToObject(column, "bytes", i642str(pColumn->bytes));
      tjsonAddStringToObject(column, "name", pColumn->name);
    }

    tjsonAddStringToObject(item, "numOfTags", i642str(pObj->numOfTags));
    SJson *tags = tjsonAddArrayToObject(item, "tags");
    for (int32_t i = 0; i < pObj->numOfTags; ++i) {
      SJson *tag = tjsonCreateObject();
      tjsonAddItemToArray(tags, tag);

      SSchema *pTag = &pObj->pTags[i];
      tjsonAddStringToObject(tag, "type", i642str(pTag->type));
      tjsonAddStringToObject(tag, "typestr", tDataTypes[pTag->type].name);
      tjsonAddStringToObject(tag, "flags", i642str(pTag->flags));
      tjsonAddStringToObject(tag, "colId", i642str(pTag->colId));
      tjsonAddStringToObject(tag, "bytes", i642str(pTag->bytes));
      tjsonAddStringToObject(tag, "name", pTag->name);
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
    tjsonAddItemToArray(items, item);
    tjsonAddStringToObject(item, "name", mndGetStbStr(pObj->name));
    tjsonAddStringToObject(item, "stb", mndGetStbStr(pObj->stb));
    tjsonAddStringToObject(item, "db", mndGetDbStr(pObj->db));
    tjsonAddStringToObject(item, "dstTbName", mndGetStbStr(pObj->dstTbName));
    tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime));
    tjsonAddStringToObject(item, "uid", i642str(pObj->uid));
    tjsonAddStringToObject(item, "stbUid", i642str(pObj->stbUid));
    tjsonAddStringToObject(item, "dbUid", i642str(pObj->dbUid));
    tjsonAddStringToObject(item, "dstTbUid", i642str(pObj->dstTbUid));
    tjsonAddStringToObject(item, "intervalUnit", i642str(pObj->intervalUnit));
    tjsonAddStringToObject(item, "slidingUnit", i642str(pObj->slidingUnit));
    tjsonAddStringToObject(item, "timezone", i642str(pObj->timezone));
    tjsonAddStringToObject(item, "dstVgId", i642str(pObj->dstVgId));
    tjsonAddStringToObject(item, "interval", i642str(pObj->interval));
    tjsonAddStringToObject(item, "offset", i642str(pObj->offset));
    tjsonAddStringToObject(item, "sliding", i642str(pObj->sliding));
    tjsonAddStringToObject(item, "exprLen", i642str(pObj->exprLen));
    tjsonAddStringToObject(item, "tagsFilterLen", i642str(pObj->tagsFilterLen));
    tjsonAddStringToObject(item, "sqlLen", i642str(pObj->sqlLen));
    tjsonAddStringToObject(item, "astLen", i642str(pObj->astLen));
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
    tjsonAddItemToArray(items, item);
    tjsonAddStringToObject(item, "vgId", i642str(pObj->vgId));
    tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime));
    tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime));
    tjsonAddStringToObject(item, "version", i642str(pObj->version));
    tjsonAddStringToObject(item, "hashBegin", i642str(pObj->hashBegin));
    tjsonAddStringToObject(item, "hashEnd", i642str(pObj->hashEnd));
    tjsonAddStringToObject(item, "db", mndGetDbStr(pObj->dbName));
    tjsonAddStringToObject(item, "dbUid", i642str(pObj->dbUid));
    tjsonAddStringToObject(item, "isTsma", i642str(pObj->isTsma));
    tjsonAddStringToObject(item, "replica", i642str(pObj->replica));
    for (int32_t i = 0; i < pObj->replica; ++i) {
      SJson *replicas = tjsonAddArrayToObject(item, "replicas");
      SJson *replica = tjsonCreateObject();
      tjsonAddItemToArray(replicas, replica);
      tjsonAddStringToObject(replica, "dnodeId", i642str(pObj->vnodeGid[i].dnodeId));
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
    tjsonAddItemToArray(items, item);
    tjsonAddStringToObject(item, "name", mndGetDbStr(pObj->name));
    tjsonAddStringToObject(item, "name", mndGetDbStr(pObj->db));
    tjsonAddStringToObject(item, "createTime", i642str(pObj->createTime));
    tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime));
    tjsonAddStringToObject(item, "uid", i642str(pObj->uid));
    tjsonAddStringToObject(item, "dbUid", i642str(pObj->dbUid));
    tjsonAddStringToObject(item, "version", i642str(pObj->version));
    tjsonAddStringToObject(item, "subType", i642str(pObj->subType));
    tjsonAddStringToObject(item, "withMeta", i642str(pObj->withMeta));
    tjsonAddStringToObject(item, "stbUid", i642str(pObj->stbUid));
    tjsonAddStringToObject(item, "stbName", mndGetStableStr(pObj->stbName));
    tjsonAddStringToObject(item, "sqlLen", i642str(pObj->sqlLen));
    tjsonAddStringToObject(item, "astLen", i642str(pObj->astLen));
    tjsonAddStringToObject(item, "sqlLen", i642str(pObj->sqlLen));
    tjsonAddStringToObject(item, "ntbUid", i642str(pObj->ntbUid));
    tjsonAddStringToObject(item, "ctbStbUid", i642str(pObj->ctbStbUid));
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
    tjsonAddItemToArray(items, item);
    tjsonAddStringToObject(item, "consumerId", i642str(pObj->consumerId));
    tjsonAddStringToObject(item, "cgroup", pObj->cgroup);
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
    tjsonAddItemToArray(items, item);
    tjsonAddStringToObject(item, "key", pObj->key);
    tjsonAddStringToObject(item, "dbUid", i642str(pObj->dbUid));
    tjsonAddStringToObject(item, "stbUid", i642str(pObj->stbUid));
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
    tjsonAddItemToArray(items, item);
    tjsonAddStringToObject(item, "name", mndGetDbStr(pObj->name));
    tjsonAddStringToObject(item, "createTime", i642str(pObj->createTime));
    tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime));
    tjsonAddStringToObject(item, "version", i642str(pObj->version));
    tjsonAddStringToObject(item, "totalLevel", i642str(pObj->totalLevel));
    tjsonAddStringToObject(item, "smaId", i642str(pObj->smaId));
    tjsonAddStringToObject(item, "uid", i642str(pObj->uid));
    tjsonAddStringToObject(item, "status", i642str(pObj->status));
    tjsonAddStringToObject(item, "igExpired", i642str(pObj->conf.igExpired));
    tjsonAddStringToObject(item, "trigger", i642str(pObj->conf.trigger));
    tjsonAddStringToObject(item, "triggerParam", i642str(pObj->conf.triggerParam));
    tjsonAddStringToObject(item, "watermark", i642str(pObj->conf.watermark));
    tjsonAddStringToObject(item, "sourceDbUid", i642str(pObj->sourceDbUid));
    tjsonAddStringToObject(item, "targetDbUid", i642str(pObj->targetDbUid));
    tjsonAddStringToObject(item, "sourceDb", mndGetDbStr(pObj->sourceDb));
    tjsonAddStringToObject(item, "targetDb", mndGetDbStr(pObj->targetDb));
    tjsonAddStringToObject(item, "targetSTbName", mndGetStbStr(pObj->targetSTbName));
    tjsonAddStringToObject(item, "targetStbUid", i642str(pObj->targetStbUid));
    tjsonAddStringToObject(item, "fixedSinkVgId", i642str(pObj->fixedSinkVgId));
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
    tjsonAddItemToArray(items, item);
    tjsonAddStringToObject(item, "acct", pObj->acct);
    tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime));
    tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime));
    tjsonAddStringToObject(item, "acctId", i642str(pObj->acctId));
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
    tjsonAddItemToArray(items, item);
    tjsonAddStringToObject(item, "name", pObj->user);
    tjsonAddStringToObject(item, "acct", pObj->acct);
    tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime));
    tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime));
    tjsonAddStringToObject(item, "superUser", i642str(pObj->superUser));
    tjsonAddStringToObject(item, "authVersion", i642str(pObj->authVersion));
    tjsonAddStringToObject(item, "passVersion", i642str(pObj->passVersion));
    tjsonAddStringToObject(item, "numOfReadDbs", i642str(taosHashGetSize(pObj->readDbs)));
    tjsonAddStringToObject(item, "numOfWriteDbs", i642str(taosHashGetSize(pObj->writeDbs)));
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
    tjsonAddItemToArray(items, item);
    tjsonAddStringToObject(item, "id", i642str(pObj->id));
    tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime));
    tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime));
    tjsonAddStringToObject(item, "port", i642str(pObj->port));
    tjsonAddStringToObject(item, "fqdn", pObj->fqdn);
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
    tjsonAddItemToArray(items, item);
    tjsonAddStringToObject(item, "id", i642str(pObj->id));
    tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime));
    tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime));
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
    tjsonAddItemToArray(items, item);
    tjsonAddStringToObject(item, "id", i642str(pObj->id));
    tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime));
    tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime));
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
    tjsonAddItemToArray(items, item);
    tjsonAddStringToObject(item, "id", i642str(pObj->id));
    tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime));
    tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime));
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
    tjsonAddItemToArray(items, item);
    tjsonAddStringToObject(item, "id", i642str(pObj->id));
    tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime));
    tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime));
    tjsonAddStringToObject(item, "name", pObj->name);
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
    tjsonAddItemToArray(items, item);
    tjsonAddStringToObject(item, "id", i642str(pObj->id));
    tjsonAddStringToObject(item, "stage", i642str(pObj->stage));
    tjsonAddStringToObject(item, "policy", i642str(pObj->policy));
    tjsonAddStringToObject(item, "conflict", i642str(pObj->conflict));
    tjsonAddStringToObject(item, "exec", i642str(pObj->exec));
    tjsonAddStringToObject(item, "oper", i642str(pObj->oper));
    tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime));
    tjsonAddStringToObject(item, "dbname", pObj->dbname);
    tjsonAddStringToObject(item, "stbname", pObj->stbname);
    tjsonAddStringToObject(item, "opername", pObj->opername);
    tjsonAddStringToObject(item, "commitLogNum", i642str(taosArrayGetSize(pObj->commitActions)));
    tjsonAddStringToObject(item, "redoActionNum", i642str(taosArrayGetSize(pObj->redoActions)));
    tjsonAddStringToObject(item, "undoActionNum", i642str(taosArrayGetSize(pObj->undoActions)));
    sdbRelease(pSdb, pObj);
  }
}

void dumpHeader(SSdb *pSdb, SJson *json) {
  tjsonAddStringToObject(json, "sver", i642str(1));
  tjsonAddStringToObject(json, "applyIndex", i642str(pSdb->applyIndex));
  tjsonAddStringToObject(json, "applyTerm", i642str(pSdb->applyTerm));
  tjsonAddStringToObject(json, "applyConfig", i642str(pSdb->applyConfig));

  SJson *maxIdsJson = tjsonCreateObject();
  tjsonAddItemToObject(json, "maxIds", maxIdsJson);
  for (int32_t i = 0; i < SDB_MAX; ++i) {
    if(i == 5) continue;
    int64_t maxId = 0;
    if (i < SDB_MAX) {
      maxId = pSdb->maxId[i];
    }
    tjsonAddStringToObject(maxIdsJson, sdbTableName(i), i642str(maxId));
  }

  SJson *tableVersJson = tjsonCreateObject();
  tjsonAddItemToObject(json, "tableVers", tableVersJson);
  for (int32_t i = 0; i < SDB_MAX; ++i) {
    int64_t tableVer = 0;
    if (i < SDB_MAX) {
      tableVer = pSdb->tableVer[i];
    }
    tjsonAddStringToObject(tableVersJson, sdbTableName(i), i642str(tableVer));
  }
}

void mndDumpSdb() {
  mInfo("start to dump sdb info to sdb.json");

  char path[PATH_MAX * 2] = {0};
  snprintf(path, sizeof(path), "%s%smnode", tsDataDir, TD_DIRSEP);

  SMsgCb msgCb = {0};
  msgCb.reportStartupFp = reportStartup;
  msgCb.sendReqFp = sendReq;
  msgCb.sendSyncReqFp = sendSyncReq;
  msgCb.sendRspFp = sendRsp;
  msgCb.mgmt = (SMgmtWrapper *)(&msgCb);  // hack
  tmsgSetDefault(&msgCb);

  walInit();
  syncInit();

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
  taosWriteFile(pFile, pCont, contLen);
  taosWriteFile(pFile, "\n", 1);
  UNUSED(taosFsyncFile(pFile));
  taosCloseFile(&pFile);
  tjsonDelete(json);
  taosMemoryFree(pCont);

  mInfo("dump sdb info success");
}

#pragma GCC diagnostic pop
