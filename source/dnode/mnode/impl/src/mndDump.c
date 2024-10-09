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
#include "mndShow.h"
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
  int32_t code = 0;
  int32_t lino = 0;
  void   *pIter = NULL;
  SJson *items = tjsonAddArrayToObject(json, "funcs");

  while (1) {
    SFuncObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_FUNC, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    RETRIEVE_CHECK_GOTO(tjsonAddItemToArray(items, item), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "name", pObj->name), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "funcType", i642str(pObj->funcType)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "scriptType", i642str(pObj->scriptType)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "align", i642str(pObj->align)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "outputType", i642str(pObj->outputType)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "outputLen", i642str(pObj->outputLen)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "bufSize", i642str(pObj->bufSize)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "signature", i642str(pObj->signature)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "commentSize", i642str(pObj->commentSize)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "codeSize", i642str(pObj->codeSize)), pObj, &lino, _OVER);
    sdbRelease(pSdb, pObj);
  }
_OVER:
  if (code != 0) mError("failed to dump func info at line:%d since %s", lino, tstrerror(code));
}

void dumpDb(SSdb *pSdb, SJson *json) {
  int32_t code = 0;
  int32_t lino = 0;
  void  *pIter = NULL;
  SJson *items = tjsonCreateObject();
  TAOS_CHECK_GOTO(tjsonAddItemToObject(json, "dbs", items), &lino, _OVER);

  while (1) {
    SDbObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_DB, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    RETRIEVE_CHECK_GOTO(tjsonAddItemToObject(items, "db", item), pObj, &lino, _OVER);

    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "name", mndGetDbStr(pObj->name)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "acct", pObj->acct), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "createUser", pObj->createUser), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "uid", i642str(pObj->uid)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "cfgVersion", i642str(pObj->cfgVersion)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "vgVersion", i642str(pObj->vgVersion)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "numOfVgroups", i642str(pObj->cfg.numOfVgroups)), pObj, &lino,
                        _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "numOfStables", i642str(pObj->cfg.numOfStables)), pObj, &lino,
                        _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "buffer", i642str(pObj->cfg.buffer)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "pageSize", i642str(pObj->cfg.pageSize)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "pages", i642str(pObj->cfg.pages)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "cacheLastSize", i642str(pObj->cfg.cacheLastSize)), pObj, &lino,
                        _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "daysPerFile", i642str(pObj->cfg.daysPerFile)), pObj, &lino,
                        _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "daysToKeep0", i642str(pObj->cfg.daysToKeep0)), pObj, &lino,
                        _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "daysToKeep1", i642str(pObj->cfg.daysToKeep1)), pObj, &lino,
                        _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "daysToKeep2", i642str(pObj->cfg.daysToKeep2)), pObj, &lino,
                        _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "minRows", i642str(pObj->cfg.minRows)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "maxRows", i642str(pObj->cfg.maxRows)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "precision", i642str(pObj->cfg.precision)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "compression", i642str(pObj->cfg.compression)), pObj, &lino,
                        _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "encryptAlgorithm", i642str(pObj->cfg.encryptAlgorithm)), pObj,
                        &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "replications", i642str(pObj->cfg.replications)), pObj, &lino,
                        _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "strict", i642str(pObj->cfg.strict)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "cacheLast", i642str(pObj->cfg.cacheLast)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "hashMethod", i642str(pObj->cfg.hashMethod)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "hashPrefix", i642str(pObj->cfg.hashPrefix)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "hashSuffix", i642str(pObj->cfg.hashSuffix)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "sstTrigger", i642str(pObj->cfg.sstTrigger)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "tsdbPageSize", i642str(pObj->cfg.tsdbPageSize)), pObj, &lino,
                        _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "schemaless", i642str(pObj->cfg.schemaless)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "walLevel", i642str(pObj->cfg.walLevel)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "walFsyncPeriod", i642str(pObj->cfg.walFsyncPeriod)), pObj, &lino,
                        _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "walRetentionPeriod", i642str(pObj->cfg.walRetentionPeriod)), pObj,
                        &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "walRetentionSize", i642str(pObj->cfg.walRetentionSize)), pObj,
                        &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "walRollPeriod", i642str(pObj->cfg.walRollPeriod)), pObj, &lino,
                        _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "walSegmentSize", i642str(pObj->cfg.walSegmentSize)), pObj, &lino,
                        _OVER);

    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "numOfRetensions", i642str(pObj->cfg.numOfRetensions)), pObj,
                        &lino, _OVER);
    for (int32_t i = 0; i < pObj->cfg.numOfRetensions; ++i) {
      SJson *rentensions = tjsonAddArrayToObject(item, "rentensions");
      SJson *rentension = tjsonCreateObject();
      RETRIEVE_CHECK_GOTO(tjsonAddItemToArray(rentensions, rentension), pObj, &lino, _OVER);

      SRetention *pRetension = taosArrayGet(pObj->cfg.pRetensions, i);
      RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "freq", i642str(pRetension->freq)), pObj, &lino, _OVER);
      RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "freqUnit", i642str(pRetension->freqUnit)), pObj, &lino, _OVER);
      RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "keep", i642str(pRetension->keep)), pObj, &lino, _OVER);
      RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "keepUnit", i642str(pRetension->keepUnit)), pObj, &lino, _OVER);
    }

    sdbRelease(pSdb, pObj);
  }
_OVER:
  if (code != 0) mError("failed to dump db info at line:%d since %s", lino, tstrerror(code));
}

void dumpStb(SSdb *pSdb, SJson *json) {
  int32_t code = 0;
  int32_t lino = 0;
  void  *pIter = NULL;
  SJson *items = tjsonAddArrayToObject(json, "stbs");

  while (1) {
    SStbObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_STB, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    RETRIEVE_CHECK_GOTO(tjsonAddItemToArray(items, item), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "name", mndGetStbStr(pObj->name)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "db", mndGetDbStr(pObj->db)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "uid", i642str(pObj->uid)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "dbUid", i642str(pObj->dbUid)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "tagVer", i642str(pObj->tagVer)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "colVer", i642str(pObj->colVer)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "smaVer", i642str(pObj->smaVer)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "nextColId", i642str(pObj->nextColId)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "watermark1", i642str(pObj->watermark[0])), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "watermark2", i642str(pObj->watermark[1])), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "maxdelay0", i642str(pObj->maxdelay[0])), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "maxdelay1", i642str(pObj->maxdelay[1])), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "ttl", i642str(pObj->ttl)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "numOfFuncs", i642str(pObj->numOfFuncs)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "commentLen", i642str(pObj->commentLen)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "ast1Len", i642str(pObj->ast1Len)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "ast2Len", i642str(pObj->ast2Len)), pObj, &lino, _OVER);

    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "numOfColumns", i642str(pObj->numOfColumns)), pObj, &lino, _OVER);
    SJson *columns = tjsonAddArrayToObject(item, "columns");
    for (int32_t i = 0; i < pObj->numOfColumns; ++i) {
      SJson *column = tjsonCreateObject();
      RETRIEVE_CHECK_GOTO(tjsonAddItemToArray(columns, column), pObj, &lino, _OVER);

      SSchema *pColumn = &pObj->pColumns[i];
      RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(column, "type", i642str(pColumn->type)), pObj, &lino, _OVER);
      RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(column, "typestr", tDataTypes[pColumn->type].name), pObj, &lino,
                          _OVER);
      RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(column, "flags", i642str(pColumn->flags)), pObj, &lino, _OVER);
      RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(column, "colId", i642str(pColumn->colId)), pObj, &lino, _OVER);
      RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(column, "bytes", i642str(pColumn->bytes)), pObj, &lino, _OVER);
      RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(column, "name", pColumn->name), pObj, &lino, _OVER);
    }

    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "numOfTags", i642str(pObj->numOfTags)), pObj, &lino, _OVER);
    SJson *tags = tjsonAddArrayToObject(item, "tags");
    for (int32_t i = 0; i < pObj->numOfTags; ++i) {
      SJson *tag = tjsonCreateObject();
      RETRIEVE_CHECK_GOTO(tjsonAddItemToArray(tags, tag), pObj, &lino, _OVER);

      SSchema *pTag = &pObj->pTags[i];
      RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(tag, "type", i642str(pTag->type)), pObj, &lino, _OVER);
      RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(tag, "typestr", tDataTypes[pTag->type].name), pObj, &lino, _OVER);
      RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(tag, "flags", i642str(pTag->flags)), pObj, &lino, _OVER);
      RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(tag, "colId", i642str(pTag->colId)), pObj, &lino, _OVER);
      RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(tag, "bytes", i642str(pTag->bytes)), pObj, &lino, _OVER);
      RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(tag, "name", pTag->name), pObj, &lino, _OVER);
    }

    sdbRelease(pSdb, pObj);
  }
_OVER:
  if (code != 0) mError("failed to dump stable info at line:%d since %s", lino, tstrerror(code));
}

void dumpSma(SSdb *pSdb, SJson *json) {
  int32_t code = 0;
  int32_t lino = 0;
  void  *pIter = NULL;
  SJson *items = tjsonAddArrayToObject(json, "smas");
  ESdbStatus objStatus = 0;

  while (1) {
    SSmaObj *pObj = NULL;
    pIter = sdbFetchAll(pSdb, SDB_SMA, pIter, (void **)&pObj, &objStatus, false);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    RETRIEVE_CHECK_GOTO(tjsonAddItemToArray(items, item), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "name", mndGetStbStr(pObj->name)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "stb", mndGetStbStr(pObj->stb)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "db", mndGetDbStr(pObj->db)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "dstTbName", mndGetStbStr(pObj->dstTbName)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "uid", i642str(pObj->uid)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "stbUid", i642str(pObj->stbUid)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "dbUid", i642str(pObj->dbUid)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "dstTbUid", i642str(pObj->dstTbUid)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "intervalUnit", i642str(pObj->intervalUnit)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "slidingUnit", i642str(pObj->slidingUnit)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "timezone", i642str(pObj->timezone)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "dstVgId", i642str(pObj->dstVgId)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "interval", i642str(pObj->interval)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "offset", i642str(pObj->offset)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "sliding", i642str(pObj->sliding)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "exprLen", i642str(pObj->exprLen)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "tagsFilterLen", i642str(pObj->tagsFilterLen)), pObj, &lino,
                        _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "sqlLen", i642str(pObj->sqlLen)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "astLen", i642str(pObj->astLen)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "status", i642str(objStatus)), pObj, &lino, _OVER);
    sdbRelease(pSdb, pObj);
  }
_OVER:
  if (code != 0) mError("failed to dump sma info at line:%d since %s", lino, tstrerror(code));
}

void dumpVgroup(SSdb *pSdb, SJson *json) {
  int32_t code = 0;
  int32_t lino = 0;
  void  *pIter = NULL;
  SJson *items = tjsonAddArrayToObject(json, "vgroups");

  while (1) {
    SVgObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    RETRIEVE_CHECK_GOTO(tjsonAddItemToArray(items, item), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "vgId", i642str(pObj->vgId)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "version", i642str(pObj->version)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "hashBegin", i642str(pObj->hashBegin)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "hashEnd", i642str(pObj->hashEnd)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "db", mndGetDbStr(pObj->dbName)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "dbUid", i642str(pObj->dbUid)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "isTsma", i642str(pObj->isTsma)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "replica", i642str(pObj->replica)), pObj, &lino, _OVER);
    for (int32_t i = 0; i < pObj->replica; ++i) {
      SJson *replicas = tjsonAddArrayToObject(item, "replicas");
      SJson *replica = tjsonCreateObject();
      RETRIEVE_CHECK_GOTO(tjsonAddItemToArray(replicas, replica), pObj, &lino, _OVER);
      RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(replica, "dnodeId", i642str(pObj->vnodeGid[i].dnodeId)), pObj, &lino,
                          _OVER);
    }
    sdbRelease(pSdb, pObj);
  }
_OVER:
  if (code != 0) mError("failed to dump vgroup info at line:%d since %s", lino, tstrerror(code));
}

void dumpTopic(SSdb *pSdb, SJson *json) {
  int32_t code = 0;
  int32_t lino = 0;
  void  *pIter = NULL;
  SJson *items = tjsonAddArrayToObject(json, "topics");

  while (1) {
    SMqTopicObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_TOPIC, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    RETRIEVE_CHECK_GOTO(tjsonAddItemToArray(items, item), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "name", mndGetDbStr(pObj->name)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "name", mndGetDbStr(pObj->db)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "createTime", i642str(pObj->createTime)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "uid", i642str(pObj->uid)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "dbUid", i642str(pObj->dbUid)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "version", i642str(pObj->version)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "subType", i642str(pObj->subType)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "withMeta", i642str(pObj->withMeta)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "stbUid", i642str(pObj->stbUid)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "stbName", mndGetStableStr(pObj->stbName)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "sqlLen", i642str(pObj->sqlLen)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "astLen", i642str(pObj->astLen)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "sqlLen", i642str(pObj->sqlLen)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "ntbUid", i642str(pObj->ntbUid)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "ctbStbUid", i642str(pObj->ctbStbUid)), pObj, &lino, _OVER);
    sdbRelease(pSdb, pObj);
  }
_OVER:
  if (code != 0) mError("failed to dump topic info at line:%d since %s", lino, tstrerror(code));
}

void dumpConsumer(SSdb *pSdb, SJson *json) {
  int32_t code = 0;
  int32_t lino = 0;
  void  *pIter = NULL;
  SJson *items = tjsonAddArrayToObject(json, "consumers");

  while (1) {
    SMqConsumerObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_CONSUMER, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    RETRIEVE_CHECK_GOTO(tjsonAddItemToArray(items, item), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "consumerId", i642str(pObj->consumerId)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "cgroup", pObj->cgroup), pObj, &lino, _OVER);
    sdbRelease(pSdb, pObj);
  }
_OVER:
  if (code != 0) mError("failed to dump consumer info at line:%d since %s", lino, tstrerror(code));
}

void dumpSubscribe(SSdb *pSdb, SJson *json) {
  int32_t code = 0;
  int32_t lino = 0;
  void  *pIter = NULL;
  SJson *items = tjsonAddArrayToObject(json, "subscribes");

  while (1) {
    SMqSubscribeObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_SUBSCRIBE, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    RETRIEVE_CHECK_GOTO(tjsonAddItemToArray(items, item), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "key", pObj->key), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "dbUid", i642str(pObj->dbUid)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "stbUid", i642str(pObj->stbUid)), pObj, &lino, _OVER);
    sdbRelease(pSdb, pObj);
  }
_OVER:
  if (code != 0) mError("failed to dump subscribe info at line:%d since %s", lino, tstrerror(code));
}

void dumpStream(SSdb *pSdb, SJson *json) {
  int32_t code = 0;
  int32_t lino = 0;
  void   *pIter = NULL;
  SJson *items = tjsonAddArrayToObject(json, "streams");

  while (1) {
    SStreamObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_STREAM, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    RETRIEVE_CHECK_GOTO(tjsonAddItemToArray(items, item), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "name", mndGetDbStr(pObj->name)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "createTime", i642str(pObj->createTime)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "version", i642str(pObj->version)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "totalLevel", i642str(pObj->totalLevel)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "smaId", i642str(pObj->smaId)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "uid", i642str(pObj->uid)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "status", i642str(pObj->status)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "igExpired", i642str(pObj->conf.igExpired)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "trigger", i642str(pObj->conf.trigger)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "triggerParam", i642str(pObj->conf.triggerParam)), pObj, &lino,
                        _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "watermark", i642str(pObj->conf.watermark)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "sourceDbUid", i642str(pObj->sourceDbUid)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "targetDbUid", i642str(pObj->targetDbUid)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "sourceDb", mndGetDbStr(pObj->sourceDb)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "targetDb", mndGetDbStr(pObj->targetDb)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "targetSTbName", mndGetStbStr(pObj->targetSTbName)), pObj, &lino,
                        _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "targetStbUid", i642str(pObj->targetStbUid)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "fixedSinkVgId", i642str(pObj->fixedSinkVgId)), pObj, &lino,
                        _OVER);
    sdbRelease(pSdb, pObj);
  }
_OVER:
  if (code != 0) mError("failed to dump stream info at line:%d since %s", lino, tstrerror(code));
}

void dumpAcct(SSdb *pSdb, SJson *json) {
  int32_t code = 0;
  int32_t lino = 0;
  void  *pIter = NULL;
  SJson *items = tjsonAddArrayToObject(json, "accts");

  while (1) {
    SAcctObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_ACCT, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    RETRIEVE_CHECK_GOTO(tjsonAddItemToArray(items, item), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "acct", pObj->acct), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "acctId", i642str(pObj->acctId)), pObj, &lino, _OVER);
    sdbRelease(pSdb, pObj);
  }
_OVER:
  if (code != 0) mError("failed to dump acct info at line:%d since %s", lino, tstrerror(code));
}

void dumpAuth(SSdb *pSdb, SJson *json) {
  // todo
}

void dumpUser(SSdb *pSdb, SJson *json) {
  int32_t code = 0;
  int32_t lino = 0;
  void  *pIter = NULL;
  SJson *items = tjsonAddArrayToObject(json, "users");

  while (1) {
    SUserObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_USER, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    RETRIEVE_CHECK_GOTO(tjsonAddItemToArray(items, item), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "name", pObj->user), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "acct", pObj->acct), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "superUser", i642str(pObj->superUser)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "authVersion", i642str(pObj->authVersion)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "passVersion", i642str(pObj->passVersion)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "numOfReadDbs", i642str(taosHashGetSize(pObj->readDbs))), pObj,
                        &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "numOfWriteDbs", i642str(taosHashGetSize(pObj->writeDbs))), pObj,
                        &lino, _OVER);
    sdbRelease(pSdb, pObj);
  }
_OVER:
  if (code != 0) mError("failed to dump user info at line:%d since %s", lino, tstrerror(code));
}

void dumpDnode(SSdb *pSdb, SJson *json) {
  int32_t code = 0;
  int32_t lino = 0;
  void  *pIter = NULL;
  SJson *items = tjsonAddArrayToObject(json, "dnodes");

  while (1) {
    SDnodeObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_DNODE, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    RETRIEVE_CHECK_GOTO(tjsonAddItemToArray(items, item), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "id", i642str(pObj->id)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "port", i642str(pObj->port)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "fqdn", pObj->fqdn), pObj, &lino, _OVER);
    sdbRelease(pSdb, pObj);
  }
_OVER:
  if (code != 0) mError("failed to dump dnode info at line:%d since %s", lino, tstrerror(code));
}

void dumpSnode(SSdb *pSdb, SJson *json) {
  int32_t code = 0;
  int32_t lino = 0;
  void  *pIter = NULL;
  SJson *items = tjsonAddArrayToObject(json, "snodes");

  while (1) {
    SSnodeObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_QNODE, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    RETRIEVE_CHECK_GOTO(tjsonAddItemToArray(items, item), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "id", i642str(pObj->id)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime)), pObj, &lino, _OVER);
    sdbRelease(pSdb, pObj);
  }
_OVER:
  if (code != 0) mError("failed to dump snode info at line:%d since %s", lino, tstrerror(code));
}

void dumpQnode(SSdb *pSdb, SJson *json) {
  int32_t code = 0;
  int32_t lino = 0;
  void  *pIter = NULL;
  SJson *items = tjsonAddArrayToObject(json, "qnodes");

  while (1) {
    SQnodeObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_QNODE, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    RETRIEVE_CHECK_GOTO(tjsonAddItemToArray(items, item), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "id", i642str(pObj->id)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime)), pObj, &lino, _OVER);
    sdbRelease(pSdb, pObj);
  }
_OVER:
  if (code != 0) mError("failed to dump qnode info at line:%d since %s", lino, tstrerror(code));
}

void dumpMnode(SSdb *pSdb, SJson *json) {
  int32_t code = 0;
  int32_t lino = 0;
  void  *pIter = NULL;
  SJson *items = tjsonAddArrayToObject(json, "mnodes");

  while (1) {
    SMnodeObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_MNODE, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    RETRIEVE_CHECK_GOTO(tjsonAddItemToArray(items, item), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "id", i642str(pObj->id)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime)), pObj, &lino, _OVER);
    sdbRelease(pSdb, pObj);
  }
_OVER:
  if (code != 0) mError("failed to dump mnode info at line:%d since %s", lino, tstrerror(code));
}

void dumpCluster(SSdb *pSdb, SJson *json) {
  int32_t code = 0;
  int32_t lino = 0;

  void  *pIter = NULL;
  SJson *items = tjsonAddArrayToObject(json, "clusters");

  while (1) {
    SClusterObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_CLUSTER, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    RETRIEVE_CHECK_GOTO(tjsonAddItemToArray(items, item), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "id", i642str(pObj->id)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "name", pObj->name), pObj, &lino, _OVER);
    sdbRelease(pSdb, pObj);
  }

_OVER:
  if (code != 0) mError("failed to dump cluster info at line:%d since %s", lino, tstrerror(code));
}

void dumpTrans(SSdb *pSdb, SJson *json) {
  int32_t code = 0;
  int32_t lino = 0;
  void  *pIter = NULL;
  SJson *items = tjsonAddArrayToObject(json, "transactions");

  while (1) {
    STrans *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_TRANS, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    RETRIEVE_CHECK_GOTO(tjsonAddItemToArray(items, item), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "id", i642str(pObj->id)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "stage", i642str(pObj->stage)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "policy", i642str(pObj->policy)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "conflict", i642str(pObj->conflict)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "exec", i642str(pObj->exec)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "oper", i642str(pObj->oper)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime)), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "dbname", pObj->dbname), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "stbname", pObj->stbname), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "opername", pObj->opername), pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "commitLogNum", i642str(taosArrayGetSize(pObj->commitActions))),
                        pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "redoActionNum", i642str(taosArrayGetSize(pObj->redoActions))),
                        pObj, &lino, _OVER);
    RETRIEVE_CHECK_GOTO(tjsonAddStringToObject(item, "undoActionNum", i642str(taosArrayGetSize(pObj->undoActions))),
                        pObj, &lino, _OVER);
    sdbRelease(pSdb, pObj);
  }

_OVER:
  if (code != 0) mError("failed to dump trans info at line:%d since %s", lino, tstrerror(code));
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
    if(i == 5) continue;
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

int32_t mndDumpSdb() {
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

  TAOS_CHECK_RETURN(walInit(NULL));
  TAOS_CHECK_RETURN(syncInit());

  SMnodeOpt opt = {.msgCb = msgCb};
  SMnode   *pMnode = mndOpen(path, &opt);
  if (pMnode == NULL) return -1;

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
    mError("failed to write %s since %s", file, terrstr());
    return terrno;
  }
  TAOS_CHECK_RETURN(taosWriteFile(pFile, pCont, contLen));
  TAOS_CHECK_RETURN(taosWriteFile(pFile, "\n", 1));
  TAOS_CHECK_RETURN(taosFsyncFile(pFile));
  TAOS_CHECK_RETURN(taosCloseFile(&pFile));
  tjsonDelete(json);
  taosMemoryFree(pCont);

  mInfo("dump sdb info success");
  return 0;
}

int32_t mndDeleteTrans() {
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

  TAOS_CHECK_RETURN(walInit(NULL));
  TAOS_CHECK_RETURN(syncInit());

  SMnodeOpt opt = {.msgCb = msgCb};
  SMnode   *pMnode = mndOpen(path, &opt);
  if (pMnode == NULL) return terrno;

  TAOS_CHECK_RETURN(sdbWriteFileForDump(pMnode->pSdb));

  mInfo("dump sdb info success");

  return 0;
}

#pragma GCC diagnostic pop
