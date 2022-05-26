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
#include "dmMgmt.h"
#include "mndInt.h"
#include "sdb.h"
#include "tconfig.h"
#include "tjson.h"

#define TMP_SDB_DATA_DIR  "/tmp/dumpsdb"
#define TMP_SDB_MNODE_DIR "/tmp/dumpsdb/mnode"
#define TMP_SDB_FILE      "/tmp/dumpsdb/mnode/data/sdb.data"
#define TMP_SDB_PATH      "/tmp/dumpsdb/mnode/data"

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

void dumpFunc(SSdb *pSdb, SJson *json) {}

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

    tjsonAddStringToObject(item, "name", pObj->name);
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
    tjsonAddIntegerToObject(item, "daysPerFile", pObj->cfg.daysPerFile);
    tjsonAddIntegerToObject(item, "daysToKeep0", pObj->cfg.daysToKeep0);
    tjsonAddIntegerToObject(item, "daysToKeep1", pObj->cfg.daysToKeep1);
    tjsonAddIntegerToObject(item, "daysToKeep2", pObj->cfg.daysToKeep2);
    tjsonAddIntegerToObject(item, "minRows", pObj->cfg.minRows);
    tjsonAddIntegerToObject(item, "maxRows", pObj->cfg.maxRows);
    tjsonAddIntegerToObject(item, "fsyncPeriod", pObj->cfg.fsyncPeriod);
    tjsonAddIntegerToObject(item, "walLevel", pObj->cfg.walLevel);
    tjsonAddIntegerToObject(item, "precision", pObj->cfg.precision);
    tjsonAddIntegerToObject(item, "compression", pObj->cfg.compression);
    tjsonAddIntegerToObject(item, "replications", pObj->cfg.replications);
    tjsonAddIntegerToObject(item, "strict", pObj->cfg.strict);
    tjsonAddIntegerToObject(item, "cacheLastRow", pObj->cfg.cacheLastRow);
    tjsonAddIntegerToObject(item, "hashMethod", pObj->cfg.hashMethod);
    tjsonAddIntegerToObject(item, "numOfRetensions", pObj->cfg.numOfRetensions);

    sdbRelease(pSdb, pObj);
  }
}

void dumpStb(SSdb *pSdb, SJson *json) {
  void  *pIter = NULL;
  SJson *items = tjsonCreateObject();
  tjsonAddItemToObject(json, "stbs", items);

  while (1) {
    SStbObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_STB, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    tjsonAddItemToObject(items, "stb", item);

    tjsonAddStringToObject(item, "name", pObj->name);
    tjsonAddStringToObject(item, "db", pObj->db);
    tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime));
    tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime));
    tjsonAddStringToObject(item, "uid", i642str(pObj->uid));
    tjsonAddStringToObject(item, "dbUid", i642str(pObj->dbUid));
    tjsonAddIntegerToObject(item, "version", pObj->version);
    tjsonAddIntegerToObject(item, "tagVer", pObj->tagVer);
    tjsonAddIntegerToObject(item, "colVer", pObj->colVer);
    tjsonAddIntegerToObject(item, "nextColId", pObj->nextColId);
    tjsonAddIntegerToObject(item, "xFilesFactor", pObj->xFilesFactor * 10000);
    tjsonAddIntegerToObject(item, "delay", pObj->delay);
    tjsonAddIntegerToObject(item, "ttl", pObj->ttl);
    tjsonAddIntegerToObject(item, "numOfColumns", pObj->numOfColumns);
    tjsonAddIntegerToObject(item, "numOfTags", pObj->numOfTags);
    tjsonAddIntegerToObject(item, "commentLen", pObj->commentLen);
    tjsonAddIntegerToObject(item, "ast1Len", pObj->ast1Len);
    tjsonAddIntegerToObject(item, "ast2Len", pObj->ast2Len);

    sdbRelease(pSdb, pObj);
  }
}

void dumpSma(SSdb *pSdb, SJson *json) {}

void dumpVgroup(SSdb *pSdb, SJson *json) {}

void dumpTopic(SSdb *pSdb, SJson *json) {}

void dumpConsumber(SSdb *pSdb, SJson *json) {}

void dumpSubscribe(SSdb *pSdb, SJson *json) {}

void dumpOffset(SSdb *pSdb, SJson *json) {}

void dumpStream(SSdb *pSdb, SJson *json) {}

void dumpAcct(SSdb *pSdb, SJson *json) {
  void  *pIter = NULL;
  SJson *items = tjsonCreateObject();
  tjsonAddItemToObject(json, "accts", items);

  while (1) {
    SAcctObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_ACCT, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    tjsonAddItemToObject(items, "acct", item);

    tjsonAddStringToObject(item, "acct", pObj->acct);
    tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime));
    tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime));
    tjsonAddIntegerToObject(item, "acctId", pObj->acctId);

    sdbRelease(pSdb, pObj);
  }
}

void dumpAuth(SSdb *pSdb, SJson *json) {}

void dumpUser(SSdb *pSdb, SJson *json) {
  void  *pIter = NULL;
  SJson *items = tjsonCreateObject();
  tjsonAddItemToObject(json, "users", items);

  while (1) {
    SUserObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_USER, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    tjsonAddItemToObject(items, "user", item);

    tjsonAddStringToObject(item, "name", pObj->user);
    tjsonAddStringToObject(item, "pass", pObj->pass);
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
  SJson *items = tjsonCreateObject();
  tjsonAddItemToObject(json, "dnodes", items);

  while (1) {
    SDnodeObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_DNODE, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    tjsonAddItemToObject(items, "dnode", item);

    tjsonAddIntegerToObject(item, "id", pObj->id);
    tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime));
    tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime));
    tjsonAddIntegerToObject(item, "port", pObj->port);
    tjsonAddStringToObject(item, "fqdn", pObj->fqdn);

    sdbRelease(pSdb, pObj);
  }
}

void dumpBnode(SSdb *pSdb, SJson *json) {}

void dumpSnode(SSdb *pSdb, SJson *json) {}

void dumpQnode(SSdb *pSdb, SJson *json) {}

void dumpMnode(SSdb *pSdb, SJson *json) {
  void  *pIter = NULL;
  SJson *items = tjsonCreateObject();
  tjsonAddItemToObject(json, "mnodes", items);

  while (1) {
    SMnodeObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_MNODE, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    tjsonAddItemToObject(items, "mnode", item);

    tjsonAddIntegerToObject(item, "id", pObj->id);
    tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime));
    tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime));

    sdbRelease(pSdb, pObj);
  }
}

void dumpCluster(SSdb *pSdb, SJson *json) {
  void  *pIter = NULL;
  SJson *items = tjsonCreateObject();
  tjsonAddItemToObject(json, "clusters", items);

  while (1) {
    SClusterObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_CLUSTER, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    tjsonAddItemToObject(items, "cluster", item);

    tjsonAddStringToObject(item, "id", i642str(pObj->id));
    tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime));
    tjsonAddStringToObject(item, "updateTime", i642str(pObj->updateTime));
    tjsonAddStringToObject(item, "name", pObj->name);

    sdbRelease(pSdb, pObj);
  }
}

void dumpTrans(SSdb *pSdb, SJson *json) {
  void  *pIter = NULL;
  SJson *items = tjsonCreateObject();
  tjsonAddItemToObject(json, "transactions", items);

  while (1) {
    STrans *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_TRANS, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SJson *item = tjsonCreateObject();
    tjsonAddItemToObject(items, "trans", item);

    tjsonAddIntegerToObject(item, "id", pObj->id);
    tjsonAddIntegerToObject(item, "stage", pObj->stage);
    tjsonAddIntegerToObject(item, "policy", pObj->policy);
    tjsonAddIntegerToObject(item, "type", pObj->type);
    tjsonAddStringToObject(item, "createdTime", i642str(pObj->createdTime));
    tjsonAddStringToObject(item, "dbUid", i642str(pObj->dbUid));
    tjsonAddStringToObject(item, "dbname", pObj->dbname);
    tjsonAddIntegerToObject(item, "redoLogNum", taosArrayGetSize(pObj->redoLogs));
    tjsonAddIntegerToObject(item, "undoLogNum", taosArrayGetSize(pObj->undoLogs));
    tjsonAddIntegerToObject(item, "commitLogNum", taosArrayGetSize(pObj->commitLogs));
    tjsonAddIntegerToObject(item, "redoActionNum", taosArrayGetSize(pObj->redoActions));
    tjsonAddIntegerToObject(item, "undoActionNum", taosArrayGetSize(pObj->undoActions));

    sdbRelease(pSdb, pObj);
  }
}

void dumpHeader(SSdb *pSdb, SJson *json) {
  tjsonAddIntegerToObject(json, "sver", 1);
  tjsonAddStringToObject(json, "curVer", i642str(pSdb->curVer));
  tjsonAddStringToObject(json, "curTerm", i642str(pSdb->curTerm));

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

int32_t dumpSdb() {
  SMsgCb msgCb = {0};
  msgCb.reportStartupFp = reportStartup;
  msgCb.sendReqFp = sendReq;
  msgCb.sendRspFp = sendRsp;
  msgCb.mgmt = (SMgmtWrapper *)(&msgCb);  // hack
  tmsgSetDefault(&msgCb);
  walInit();

  SMnodeOpt opt = {.msgCb = msgCb};
  SMnode   *pMnode = mndOpen(TMP_SDB_MNODE_DIR, &opt);
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
  dumpConsumber(pSdb, json);
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
    dError("failed to write %s since %s", file, terrstr());
    return -1;
  }
  taosWriteFile(pFile, pCont, contLen);
  taosWriteFile(pFile, "\n", 1);
  taosFsyncFile(pFile);
  taosCloseFile(&pFile);
  tjsonDelete(json);
  taosMemoryFree(pCont);
  taosRemoveDir(TMP_SDB_DATA_DIR);
  return 0;
}

int32_t parseArgs(int32_t argc, char *argv[]) {
  char file[PATH_MAX] = {0};

  for (int32_t i = 1; i < argc; ++i) {
    if (strcmp(argv[i], "-c") == 0) {
      if (i < argc - 1) {
        if (strlen(argv[++i]) >= PATH_MAX) {
          printf("config file path overflow");
          return -1;
        }
        tstrncpy(configDir, argv[i], PATH_MAX);
      } else {
        printf("'-c' requires a parameter, default is %s\n", configDir);
        return -1;
      }
    } else if (strcmp(argv[i], "-f") == 0) {
      if (i < argc - 1) {
        if (strlen(argv[++i]) >= PATH_MAX) {
          printf("file path overflow");
          return -1;
        }
        tstrncpy(file, argv[i], PATH_MAX);
      } else {
        printf("'-f' requires a parameter, default is %s\n", configDir);
        return -1;
      }
    } else {
      printf("-c Configuration directory. \n");
      printf("-f Input sdb.data file. \n");
      return -1;
    }
  }

  if (taosCreateLog("dumplog", 1, configDir, NULL, NULL, NULL, NULL, 1) != 0) {
    printf("failed to dump since init log error\n");
    return -1;
  }

  if (taosInitCfg(configDir, NULL, NULL, NULL, NULL, 0) != 0) {
    printf("failed to dump since read config error\n");
    return -1;
  }

  if (file[0] == 0) {
    snprintf(file, PATH_MAX, "%s/mnode/data/sdb.data", tsDataDir);
  }

  strcpy(tsDataDir, TMP_SDB_DATA_DIR);
  taosMulMkDir(TMP_SDB_PATH);
  taosCopyFile(file, TMP_SDB_FILE);
  return 0;
}

int32_t main(int32_t argc, char *argv[]) {
  if (parseArgs(argc, argv) != 0) {
    return -1;
  }

  return dumpSdb();
}
