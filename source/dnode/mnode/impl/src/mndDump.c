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
    tjsonAddIntegerToObject(item, "funcType", pObj->funcType);
    tjsonAddIntegerToObject(item, "scriptType", pObj->scriptType);
    tjsonAddIntegerToObject(item, "align", pObj->align);
    tjsonAddIntegerToObject(item, "outputType", pObj->outputType);
    tjsonAddIntegerToObject(item, "outputLen", pObj->outputLen);
    tjsonAddIntegerToObject(item, "bufSize", pObj->bufSize);
    tjsonAddStringToObject(item, "signature", i642str(pObj->signature));
    tjsonAddIntegerToObject(item, "commentSize", pObj->commentSize);
    tjsonAddIntegerToObject(item, "codeSize", pObj->codeSize);
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
    tjsonAddIntegerToObject(item, "cfgVersion", pObj->cfgVersion);
    tjsonAddIntegerToObject(item, "vgVersion", pObj->vgVersion);
    tjsonAddIntegerToObject(item, "numOfVgroups", pObj->cfg.numOfVgroups);
    tjsonAddIntegerToObject(item, "numOfStables", pObj->cfg.numOfStables);
    tjsonAddIntegerToObject(item, "buffer", pObj->cfg.buffer);
    tjsonAddIntegerToObject(item, "pageSize", pObj->cfg.pageSize);
    tjsonAddIntegerToObject(item, "pages", pObj->cfg.pages);
    tjsonAddIntegerToObject(item, "cacheLastSize", pObj->cfg.cacheLastSize);
    tjsonAddIntegerToObject(item, "daysPerFile", pObj->cfg.daysPerFile);
    tjsonAddIntegerToObject(item, "daysToKeep0", pObj->cfg.daysToKeep0);
    tjsonAddIntegerToObject(item, "daysToKeep1", pObj->cfg.daysToKeep1);
    tjsonAddIntegerToObject(item, "daysToKeep2", pObj->cfg.daysToKeep2);
    tjsonAddIntegerToObject(item, "minRows", pObj->cfg.minRows);
    tjsonAddIntegerToObject(item, "maxRows", pObj->cfg.maxRows);
    tjsonAddIntegerToObject(item, "precision", pObj->cfg.precision);
    tjsonAddIntegerToObject(item, "compression", pObj->cfg.compression);
    tjsonAddIntegerToObject(item, "replications", pObj->cfg.replications);
    tjsonAddIntegerToObject(item, "strict", pObj->cfg.strict);
    tjsonAddIntegerToObject(item, "cacheLast", pObj->cfg.cacheLast);
    tjsonAddIntegerToObject(item, "hashMethod", pObj->cfg.hashMethod);
    tjsonAddIntegerToObject(item, "hashPrefix", pObj->cfg.hashPrefix);
    tjsonAddIntegerToObject(item, "hashSuffix", pObj->cfg.hashSuffix);
    tjsonAddIntegerToObject(item, "sstTrigger", pObj->cfg.sstTrigger);
    tjsonAddIntegerToObject(item, "tsdbPageSize", pObj->cfg.tsdbPageSize);
    tjsonAddIntegerToObject(item, "schemaless", pObj->cfg.schemaless);
    tjsonAddIntegerToObject(item, "walLevel", pObj->cfg.walLevel);
    tjsonAddIntegerToObject(item, "walFsyncPeriod", pObj->cfg.walFsyncPeriod);
    tjsonAddIntegerToObject(item, "walRetentionPeriod", pObj->cfg.walRetentionPeriod);
    tjsonAddIntegerToObject(item, "walRetentionSize", pObj->cfg.walRetentionSize);
    tjsonAddIntegerToObject(item, "walRollPeriod", pObj->cfg.walRollPeriod);
    tjsonAddIntegerToObject(item, "walSegmentSize", pObj->cfg.walSegmentSize);

    tjsonAddIntegerToObject(item, "numOfRetensions", pObj->cfg.numOfRetensions);
    for (int32_t i = 0; i < pObj->cfg.numOfRetensions; ++i) {
      SJson *rentensions = tjsonAddArrayToObject(item, "rentensions");
      SJson *rentension = tjsonCreateObject();
      tjsonAddItemToArray(rentensions, rentension);

      SRetention *pRetension = taosArrayGet(pObj->cfg.pRetensions, i);
      tjsonAddStringToObject(item, "freq", i642str(pRetension->freq));
      tjsonAddIntegerToObject(item, "freqUnit", pRetension->freqUnit);
      tjsonAddStringToObject(item, "keep", i642str(pRetension->keep));
      tjsonAddIntegerToObject(item, "keepUnit", pRetension->keepUnit);
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
    tjsonAddIntegerToObject(item, "tagVer", pObj->tagVer);
    tjsonAddIntegerToObject(item, "colVer", pObj->colVer);
    tjsonAddIntegerToObject(item, "smaVer", pObj->smaVer);
    tjsonAddIntegerToObject(item, "nextColId", pObj->nextColId);
    tjsonAddIntegerToObject(item, "watermark1", pObj->watermark[0]);
    tjsonAddIntegerToObject(item, "watermark2", pObj->watermark[1]);
    tjsonAddIntegerToObject(item, "maxdelay0", pObj->maxdelay[0]);
    tjsonAddIntegerToObject(item, "maxdelay1", pObj->maxdelay[1]);
    tjsonAddIntegerToObject(item, "ttl", pObj->ttl);
    tjsonAddIntegerToObject(item, "numOfFuncs", pObj->numOfFuncs);
    tjsonAddIntegerToObject(item, "commentLen", pObj->commentLen);
    tjsonAddIntegerToObject(item, "ast1Len", pObj->ast1Len);
    tjsonAddIntegerToObject(item, "ast2Len", pObj->ast2Len);

    tjsonAddIntegerToObject(item, "numOfColumns", pObj->numOfColumns);
    SJson *columns = tjsonAddArrayToObject(item, "columns");
    for (int32_t i = 0; i < pObj->numOfColumns; ++i) {
      SJson *column = tjsonCreateObject();
      tjsonAddItemToArray(columns, column);

      SSchema *pColumn = &pObj->pColumns[i];
      tjsonAddIntegerToObject(column, "type", pColumn->type);
      tjsonAddStringToObject(column, "typestr", tDataTypes[pColumn->type].name);
      tjsonAddIntegerToObject(column, "flags", pColumn->flags);
      tjsonAddIntegerToObject(column, "colId", pColumn->colId);
      tjsonAddIntegerToObject(column, "bytes", pColumn->bytes);
      tjsonAddStringToObject(column, "name", pColumn->name);
    }

    tjsonAddIntegerToObject(item, "numOfTags", pObj->numOfTags);
    SJson *tags = tjsonAddArrayToObject(item, "tags");
    for (int32_t i = 0; i < pObj->numOfTags; ++i) {
      SJson *tag = tjsonCreateObject();
      tjsonAddItemToArray(tags, tag);

      SSchema *pTag = &pObj->pTags[i];
      tjsonAddIntegerToObject(tag, "type", pTag->type);
      tjsonAddStringToObject(tag, "typestr", tDataTypes[pTag->type].name);
      tjsonAddIntegerToObject(tag, "flags", pTag->flags);
      tjsonAddIntegerToObject(tag, "colId", pTag->colId);
      tjsonAddIntegerToObject(tag, "bytes", pTag->bytes);
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
    tjsonAddIntegerToObject(item, "intervalUnit", pObj->intervalUnit);
    tjsonAddIntegerToObject(item, "slidingUnit", pObj->slidingUnit);
    tjsonAddIntegerToObject(item, "timezone", pObj->timezone);
    tjsonAddIntegerToObject(item, "dstVgId", pObj->dstVgId);
    tjsonAddStringToObject(item, "interval", i642str(pObj->interval));
    tjsonAddStringToObject(item, "offset", i642str(pObj->offset));
    tjsonAddStringToObject(item, "sliding", i642str(pObj->sliding));
    tjsonAddIntegerToObject(item, "exprLen", pObj->exprLen);
    tjsonAddIntegerToObject(item, "tagsFilterLen", pObj->tagsFilterLen);
    tjsonAddIntegerToObject(item, "sqlLen", pObj->sqlLen);
    tjsonAddIntegerToObject(item, "astLen", pObj->astLen);
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
    tjsonAddIntegerToObject(item, "vgId", pObj->vgId);
    tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime));
    tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime));
    tjsonAddIntegerToObject(item, "version", pObj->version);
    tjsonAddIntegerToObject(item, "hashBegin", pObj->hashBegin);
    tjsonAddIntegerToObject(item, "hashEnd", pObj->hashEnd);
    tjsonAddStringToObject(item, "db", mndGetDbStr(pObj->dbName));
    tjsonAddStringToObject(item, "dbUid", i642str(pObj->dbUid));
    tjsonAddIntegerToObject(item, "isTsma", pObj->isTsma);
    tjsonAddIntegerToObject(item, "replica", pObj->replica);
    for (int32_t i = 0; i < pObj->replica; ++i) {
      SJson *replicas = tjsonAddArrayToObject(item, "replicas");
      SJson *replica = tjsonCreateObject();
      tjsonAddItemToArray(replicas, replica);
      tjsonAddIntegerToObject(replica, "dnodeId", pObj->vnodeGid[i].dnodeId);
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
    tjsonAddIntegerToObject(item, "version", pObj->version);
    tjsonAddIntegerToObject(item, "subType", pObj->subType);
    tjsonAddIntegerToObject(item, "withMeta", pObj->withMeta);
    tjsonAddStringToObject(item, "stbUid", i642str(pObj->stbUid));
    tjsonAddIntegerToObject(item, "sqlLen", pObj->sqlLen);
    tjsonAddIntegerToObject(item, "astLen", pObj->astLen);
    tjsonAddIntegerToObject(item, "sqlLen", pObj->sqlLen);
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

void dumpOffset(SSdb *pSdb, SJson *json) {
  void  *pIter = NULL;
  SJson *items = tjsonAddArrayToObject(json, "offsets");

  while (1) {
    SMqOffsetObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_OFFSET, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    tjsonAddItemToArray(items, item);
    tjsonAddStringToObject(item, "key", pObj->key);
    tjsonAddStringToObject(item, "dbUid", i642str(pObj->dbUid));
    tjsonAddStringToObject(item, "offset", i642str(pObj->offset));
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
    tjsonAddIntegerToObject(item, "version", pObj->version);
    tjsonAddIntegerToObject(item, "totalLevel", pObj->totalLevel);
    tjsonAddStringToObject(item, "smaId", i642str(pObj->smaId));
    tjsonAddStringToObject(item, "uid", i642str(pObj->uid));
    tjsonAddIntegerToObject(item, "status", pObj->status);
    tjsonAddIntegerToObject(item, "igExpired", pObj->igExpired);
    tjsonAddIntegerToObject(item, "trigger", pObj->trigger);
    tjsonAddStringToObject(item, "triggerParam", i642str(pObj->triggerParam));
    tjsonAddStringToObject(item, "watermark", i642str(pObj->watermark));
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
    tjsonAddIntegerToObject(item, "acctId", pObj->acctId);
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
    tjsonAddIntegerToObject(item, "superUser", pObj->superUser);
    tjsonAddIntegerToObject(item, "authVersion", pObj->authVersion);
    tjsonAddIntegerToObject(item, "numOfReadDbs", taosHashGetSize(pObj->readDbs));
    tjsonAddIntegerToObject(item, "numOfWriteDbs", taosHashGetSize(pObj->writeDbs));
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
    tjsonAddIntegerToObject(item, "id", pObj->id);
    tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime));
    tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime));
    tjsonAddIntegerToObject(item, "port", pObj->port);
    tjsonAddStringToObject(item, "fqdn", pObj->fqdn);
    sdbRelease(pSdb, pObj);
  }
}

void dumpBnode(SSdb *pSdb, SJson *json) {
  // not implemented yet
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
    tjsonAddIntegerToObject(item, "id", pObj->id);
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
    tjsonAddIntegerToObject(item, "id", pObj->id);
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
    tjsonAddIntegerToObject(item, "id", pObj->id);
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
    tjsonAddIntegerToObject(item, "id", pObj->id);
    tjsonAddIntegerToObject(item, "stage", pObj->stage);
    tjsonAddIntegerToObject(item, "policy", pObj->policy);
    tjsonAddIntegerToObject(item, "conflict", pObj->conflict);
    tjsonAddIntegerToObject(item, "exec", pObj->exec);
    tjsonAddIntegerToObject(item, "oper", pObj->oper);
    tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime));
    tjsonAddStringToObject(item, "dbname", pObj->dbname);
    tjsonAddStringToObject(item, "stbname", pObj->stbname);
    tjsonAddStringToObject(item, "opername", pObj->opername);
    tjsonAddIntegerToObject(item, "commitLogNum", taosArrayGetSize(pObj->commitActions));
    tjsonAddIntegerToObject(item, "redoActionNum", taosArrayGetSize(pObj->redoActions));
    tjsonAddIntegerToObject(item, "undoActionNum", taosArrayGetSize(pObj->undoActions));
    sdbRelease(pSdb, pObj);
  }
}

void dumpHeader(SSdb *pSdb, SJson *json) {
  tjsonAddIntegerToObject(json, "sver", 1);
  tjsonAddStringToObject(json, "applyIndex", i642str(pSdb->applyIndex));
  tjsonAddStringToObject(json, "applyTerm", i642str(pSdb->applyTerm));
  tjsonAddStringToObject(json, "applyConfig", i642str(pSdb->applyConfig));

  SJson *maxIdsJson = tjsonCreateObject();
  tjsonAddItemToObject(json, "maxIds", maxIdsJson);
  for (int32_t i = 0; i < SDB_MAX; ++i) {
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
  dumpOffset(pSdb, json);
  dumpStream(pSdb, json);
  dumpAcct(pSdb, json);
  dumpAuth(pSdb, json);
  dumpUser(pSdb, json);
  dumpDnode(pSdb, json);
  dumpBnode(pSdb, json);
  dumpSnode(pSdb, json);
  dumpQnode(pSdb, json);
  dumpMnode(pSdb, json);
  dumpCluster(pSdb, json);
  dumpTrans(pSdb, json);

  char     *pCont = tjsonToString(json);
  int32_t   contLen = strlen(pCont);
  char      file[] = "sdb.json";
  TdFilePtr pFile = taosOpenFile(file, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    mError("failed to write %s since %s", file, terrstr());
    return;
  }
  taosWriteFile(pFile, pCont, contLen);
  taosWriteFile(pFile, "\n", 1);
  taosFsyncFile(pFile);
  taosCloseFile(&pFile);
  tjsonDelete(json);
  taosMemoryFree(pCont);

  mInfo("dump sdb info success");
}

#pragma GCC diagnostic pop
